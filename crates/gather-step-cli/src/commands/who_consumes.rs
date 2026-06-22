use anyhow::Result;
use clap::Args;
use gather_step_mcp::tools::who_consumes::{
    WhoConsumesData, WhoConsumesRequest, who_consumes_tool,
};
use serde_json::json;

use crate::command_render::RenderedCommand;
use crate::daemon_protocol::DaemonRequest;
use crate::storage_context::StorageContext;
use crate::{app::AppContext, daemon_proxy};

#[derive(Debug, Args)]
pub struct WhoConsumesArgs {
    #[arg(help = "Symbol name to search for; reports the repos that consume what it produces")]
    pub symbol: String,
}

pub fn run(app: &AppContext, args: &WhoConsumesArgs) -> Result<()> {
    daemon_proxy::run_read_only_command(
        app,
        &DaemonRequest::WhoConsumes {
            symbol: args.symbol.clone(),
            repo_filter: app.repo_filter.clone(),
        },
        |app| run_rendered(app, &StorageContext::workspace_read_only(app), args),
    )
}

pub(crate) fn run_rendered(
    app: &AppContext,
    ctx: &StorageContext,
    args: &WhoConsumesArgs,
) -> Result<RenderedCommand> {
    let mcp = gather_step_mcp::McpContext::open(ctx.mcp_server_config())?;
    execute(&mcp, &args.symbol, app.repo_filter.as_deref())
}

pub(crate) fn execute(
    ctx: &gather_step_mcp::McpContext,
    symbol: &str,
    repo: Option<&str>,
) -> Result<RenderedCommand> {
    let response = who_consumes_tool(
        ctx,
        WhoConsumesRequest {
            symbol: symbol.to_owned(),
            repo: repo.map(str::to_owned),
        },
    )?;
    let data: WhoConsumesData = response.data;

    let mut lines = Vec::new();
    if data.consumers.is_empty() {
        lines.push(format!("No repos consume `{}`.", data.symbol));
    } else {
        lines.push(format!("Repos consuming `{}`:", data.symbol));
        for consumer in &data.consumers {
            lines.push(format!(
                "  {} (via {})",
                consumer.repo,
                consumer.linking_symbols.join(", ")
            ));
        }
    }

    let payload = json!({
        "event": "who_consumes_completed",
        "symbol": data.symbol,
        "consumers": data.consumers,
    });
    Ok(RenderedCommand::success(payload, lines))
}

#[cfg(test)]
mod tests {
    use std::{env, fs};

    use gather_step_core::{
        EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeId, NodeKind, SourceSpan, Visibility,
        node_id, virtual_node,
    };
    use gather_step_mcp::{McpContext, McpServerConfig};
    use gather_step_storage::{GraphStore, StorageCoordinator};

    use super::execute;

    /// A per-process-unique temp dir. A bare `pid + nanos` path collides when two
    /// tests open in the same instant (identical pid, coarse clock) → redb
    /// `DatabaseAlreadyOpen` under the parallel runner. The atomic counter makes
    /// every call's path distinct.
    fn unique_storage_root(prefix: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let root = env::temp_dir().join(format!(
            "{prefix}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&root).expect("storage dir");
        root
    }

    fn file(repo: &str, file_path: &str) -> NodeData {
        NodeData {
            id: node_id(repo, file_path, NodeKind::File, file_path),
            kind: NodeKind::File,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: file_path.to_owned(),
            qualified_name: Some(format!("{repo}::{file_path}")),
            external_id: None,
            signature: None,
            visibility: None,
            span: None,
            is_virtual: false,
            ai_role: None,
        }
    }

    fn symbol(repo: &str, file_path: &str, name: &str) -> NodeData {
        NodeData {
            id: node_id(repo, file_path, NodeKind::Function, name),
            kind: NodeKind::Function,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(format!("{repo}::{name}")),
            external_id: None,
            signature: None,
            visibility: Some(Visibility::Public),
            span: Some(SourceSpan {
                line_start: 1,
                line_len: 1,
                column_start: 0,
                column_len: 0,
            }),
            is_virtual: false,
            ai_role: None,
        }
    }

    fn defines(owner: NodeId, target: NodeId) -> EdgeData {
        EdgeData {
            source: owner,
            target,
            kind: EdgeKind::Defines,
            metadata: EdgeMetadata::default(),
            owner_file: owner,
            is_cross_file: false,
        }
    }

    fn edge(owner: NodeId, source: NodeId, target: NodeId, kind: EdgeKind) -> EdgeData {
        EdgeData {
            source,
            target,
            kind,
            metadata: EdgeMetadata::default(),
            owner_file: owner,
            is_cross_file: true,
        }
    }

    /// Route-mediated fixture: `service-api` serves `GET /credits`, whose handler
    /// references `CREDIT_AGENT_CONFIGS`; `service-ui` consumes that route.
    /// `who-consumes CREDIT_AGENT_CONFIGS` payload `consumers` must name
    /// `service-ui`.
    #[test]
    fn who_consumes_payload_lists_route_mediated_consumer() {
        let storage_root = unique_storage_root("gather-step-cli-who-consumes");

        let config_file = file("service-api", "src/config/credit.ts");
        let config_sym = symbol(
            "service-api",
            "src/config/credit.ts",
            "CREDIT_AGENT_CONFIGS",
        );
        let handler_file = file("service-api", "src/handlers/credit.ts");
        let handler_sym = symbol("service-api", "src/handlers/credit.ts", "getCredits");
        let caller_file = file("service-ui", "src/caller.ts");
        let caller_sym = symbol("service-ui", "src/caller.ts", "callCredits");
        let route = virtual_node(
            NodeKind::Route,
            "service-api",
            "src/handlers/credit.ts",
            "GET /credits",
            "__route__GET__/credits",
        );

        {
            let storage = StorageCoordinator::open(&storage_root).expect("coordinator opens");
            storage
                .graph()
                .bulk_insert(
                    &[
                        config_file.clone(),
                        config_sym.clone(),
                        handler_file.clone(),
                        handler_sym.clone(),
                        caller_file.clone(),
                        caller_sym.clone(),
                        route.clone(),
                    ],
                    &[
                        defines(config_file.id, config_sym.id),
                        defines(handler_file.id, handler_sym.id),
                        defines(caller_file.id, caller_sym.id),
                        edge(
                            handler_file.id,
                            handler_sym.id,
                            config_sym.id,
                            EdgeKind::References,
                        ),
                        edge(handler_file.id, handler_sym.id, route.id, EdgeKind::Serves),
                        edge(
                            caller_file.id,
                            caller_sym.id,
                            route.id,
                            EdgeKind::ConsumesApiFrom,
                        ),
                    ],
                )
                .expect("fixture insert");
            storage.reconcile_search("service-api");
            storage.reconcile_search("service-ui");
        }

        let registry_path = storage_root.join("registry.json");
        let graph_path = storage_root.join("graph.redb");
        let ctx = McpContext::open(McpServerConfig::new(registry_path, graph_path))
            .expect("context should open");

        let rendered =
            execute(&ctx, "CREDIT_AGENT_CONFIGS", None).expect("who_consumes should succeed");
        let payload = rendered.payload.expect("payload should be present");
        let consumers = payload
            .get("consumers")
            .and_then(|value| value.as_array())
            .expect("consumers array");
        assert!(
            consumers
                .iter()
                .filter_map(|consumer| consumer.get("repo").and_then(|repo| repo.as_str()))
                .any(|repo| repo == "service-ui"),
            "consumers payload must contain service-ui: {consumers:?}"
        );

        let _ = fs::remove_dir_all(&storage_root);
    }

    /// CLI parity with the MCP layer: a symbol produced by two distinct repos is
    /// rejected as ambiguous when no `--repo` scope is supplied.
    #[test]
    fn who_consumes_cli_rejects_ambiguous_targets() {
        let storage_root = unique_storage_root("gather-step-cli-who-consumes-ambig");

        let build = |producer: &str, route_path: &str, consumer: &str| {
            let config_file = file(producer, "src/config/shared.ts");
            let config_sym = symbol(producer, "src/config/shared.ts", "SHARED_CONFIG");
            let handler_file = file(producer, "src/handlers/shared.ts");
            let handler_sym = symbol(producer, "src/handlers/shared.ts", "getShared");
            let caller_file = file(consumer, "src/caller.ts");
            let caller_sym = symbol(consumer, "src/caller.ts", "callShared");
            let route = virtual_node(
                NodeKind::Route,
                producer,
                "src/handlers/shared.ts",
                format!("GET {route_path}"),
                format!("__route__GET__{route_path}"),
            );
            let nodes = vec![
                config_file.clone(),
                config_sym.clone(),
                handler_file.clone(),
                handler_sym.clone(),
                caller_file.clone(),
                caller_sym.clone(),
                route.clone(),
            ];
            let edges = vec![
                defines(config_file.id, config_sym.id),
                defines(handler_file.id, handler_sym.id),
                defines(caller_file.id, caller_sym.id),
                edge(
                    handler_file.id,
                    handler_sym.id,
                    config_sym.id,
                    EdgeKind::References,
                ),
                edge(handler_file.id, handler_sym.id, route.id, EdgeKind::Serves),
                edge(
                    caller_file.id,
                    caller_sym.id,
                    route.id,
                    EdgeKind::ConsumesApiFrom,
                ),
            ];
            (nodes, edges)
        };

        let (mut nodes, mut edges) = build("service-api", "/shared", "service-ui");
        let (nodes_b, edges_b) = build("billing", "/billing-shared", "service-review");
        nodes.extend(nodes_b);
        edges.extend(edges_b);

        {
            let storage = StorageCoordinator::open(&storage_root).expect("coordinator opens");
            storage
                .graph()
                .bulk_insert(&nodes, &edges)
                .expect("fixture insert");
            storage.reconcile_search("service-api");
            storage.reconcile_search("billing");
            storage.reconcile_search("service-ui");
            storage.reconcile_search("service-review");
        }

        let registry_path = storage_root.join("registry.json");
        let graph_path = storage_root.join("graph.redb");
        let ctx = McpContext::open(McpServerConfig::new(registry_path, graph_path))
            .expect("context should open");

        let error = execute(&ctx, "SHARED_CONFIG", None).expect_err("ambiguous target should fail");
        let message = error.to_string();
        assert!(
            message.contains("`SHARED_CONFIG` is ambiguous"),
            "error must flag ambiguity: {message}"
        );

        let rendered = execute(&ctx, "SHARED_CONFIG", Some("service-api"))
            .expect("repo-scoped query should resolve");
        let payload = rendered.payload.expect("payload should be present");
        let repos: Vec<String> = payload
            .get("consumers")
            .and_then(|value| value.as_array())
            .expect("consumers array")
            .iter()
            .filter_map(|consumer| {
                consumer
                    .get("repo")
                    .and_then(|repo| repo.as_str())
                    .map(str::to_owned)
            })
            .collect();
        assert!(
            repos.iter().any(|repo| repo == "service-ui"),
            "service-api scope must surface service-ui: {repos:?}"
        );
        assert!(
            !repos.iter().any(|repo| repo == "service-review"),
            "service-api scope must not surface billing's consumer: {repos:?}"
        );

        let _ = fs::remove_dir_all(&storage_root);
    }
}
