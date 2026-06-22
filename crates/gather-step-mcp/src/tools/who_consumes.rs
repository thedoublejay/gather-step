use std::collections::BTreeMap;

use gather_step_analysis::CrossRepoConsumerLookup;
use rmcp::schemars;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    config::{McpContext, validate_input_length},
    error::McpServerError,
    tools::search::{SearchRequest, search_symbols},
};

const WHO_CONSUMES_SEARCH_LIMIT: usize = 25;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct WhoConsumesRequest {
    pub symbol: String,
    /// Scope the search to a single producing repo. When omitted and the symbol
    /// name is produced by more than one repo, the query is rejected as
    /// ambiguous rather than silently unioning every producer's consumers.
    #[serde(default)]
    pub repo: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct WhoConsumesResponse {
    pub data: WhoConsumesData,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct WhoConsumesData {
    pub consumers: Vec<ConsumerRepo>,
    pub symbol: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ConsumerRepo {
    /// The symbols matched by the query whose files link this consumer repo.
    pub linking_symbols: Vec<String>,
    /// The foreign repo that consumes what one of the matched symbols produces.
    pub repo: String,
}

/// Find the symbols matching `symbol`, group their hit files by producing repo,
/// and report the foreign repos that consume what those files produce, together
/// with which matched symbols link each consumer.
///
/// The cross-repo participation primitive is computed once per *distinct*
/// producing repo via [`CrossRepoConsumerLookup`], so a query that returns many
/// hits in the same repo pays a single graph scan for that repo.
pub fn who_consumes_tool(
    ctx: &McpContext,
    request: WhoConsumesRequest,
) -> Result<WhoConsumesResponse, McpServerError> {
    validate_input_length("symbol", &request.symbol)?;
    if let Some(repo) = &request.repo {
        validate_input_length("repo", repo)?;
    }

    let search = search_symbols(
        ctx,
        SearchRequest {
            budget_bytes: None,
            cursor: None,
            kind: None,
            language: None,
            limit: Some(WHO_CONSUMES_SEARCH_LIMIT),
            query: request.symbol.clone(),
            repo: request.repo.clone(),
        },
    )?;

    if request.repo.is_none() {
        let producing_repos = search
            .data
            .results
            .iter()
            .map(|hit| hit.repo.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        if producing_repos.len() > 1 {
            let choices = search
                .data
                .results
                .iter()
                .map(|hit| format!("{}:{} ({})", hit.repo, hit.file_path, hit.symbol_name))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(McpServerError::InvalidInput(format!(
                "symbol `{}` is ambiguous; refine the symbol or scope it to a repo: {choices}",
                request.symbol
            )));
        }
    }

    let graph = ctx.graph();
    let mut lookup = CrossRepoConsumerLookup::new();
    // consumer repo -> set of matched symbol names that link it.
    let mut consumers: BTreeMap<String, std::collections::BTreeSet<String>> = BTreeMap::new();

    for hit in &search.data.results {
        let consumer_repos = lookup.consumer_repos(graph, &hit.repo, &hit.file_path)?;
        for consumer_repo in consumer_repos {
            consumers
                .entry(consumer_repo)
                .or_default()
                .insert(hit.symbol_name.clone());
        }
    }

    let consumers = consumers
        .into_iter()
        .map(|(repo, linking_symbols)| ConsumerRepo {
            linking_symbols: linking_symbols.into_iter().collect(),
            repo,
        })
        .collect();

    Ok(WhoConsumesResponse {
        data: WhoConsumesData {
            consumers,
            symbol: request.symbol,
        },
    })
}

#[cfg(test)]
mod tests {
    use std::{env, fs};

    use gather_step_core::{
        EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeId, NodeKind, SourceSpan, Visibility,
        node_id, virtual_node,
    };
    use gather_step_storage::{GraphStore, StorageCoordinator};

    use crate::{McpServerConfig, config::McpContext};

    use super::{WhoConsumesRequest, who_consumes_tool};

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
    /// references `CREDIT_AGENT_CONFIGS`; `service-ui` consumes that route. So
    /// `who-consumes CREDIT_AGENT_CONFIGS` must surface `service-ui`.
    #[test]
    fn who_consumes_surfaces_route_mediated_consumer() {
        let storage_root = unique_storage_root("gather-step-mcp-who-consumes");

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

        let response = who_consumes_tool(
            &ctx,
            WhoConsumesRequest {
                symbol: "CREDIT_AGENT_CONFIGS".to_owned(),
                repo: None,
            },
        )
        .expect("who_consumes should succeed");

        assert!(
            response
                .data
                .consumers
                .iter()
                .any(|consumer| consumer.repo == "service-ui"),
            "consumers must contain service-ui: {:?}",
            response.data.consumers
        );

        let _ = fs::remove_dir_all(&storage_root);
    }

    /// Two distinct producing repos define a symbol with the same name and each
    /// has its own foreign consumer. With no `repo` scope the query is ambiguous
    /// and must be rejected (mirrors F4 `resolve_single_event_target`), rather
    /// than silently unioning both producers' consumers.
    #[test]
    fn who_consumes_rejects_ambiguous_cross_repo_targets() {
        let storage_root = ambiguous_fixture();

        let registry_path = storage_root.join("registry.json");
        let graph_path = storage_root.join("graph.redb");
        let ctx = McpContext::open(McpServerConfig::new(registry_path, graph_path))
            .expect("context should open");

        let error = who_consumes_tool(
            &ctx,
            WhoConsumesRequest {
                symbol: "SHARED_CONFIG".to_owned(),
                repo: None,
            },
        )
        .expect_err("ambiguous target should fail");

        let message = error.to_string();
        assert!(
            message.contains("`SHARED_CONFIG` is ambiguous"),
            "error must flag ambiguity: {message}"
        );
        assert!(
            message.contains("service-api") && message.contains("billing"),
            "error must list both producing repos as candidates: {message}"
        );

        let _ = fs::remove_dir_all(&storage_root);
    }

    /// Scoping the same ambiguous query to one producing repo resolves it: only
    /// that producer's foreign consumer is reported.
    #[test]
    fn who_consumes_repo_scope_selects_single_producer() {
        let storage_root = ambiguous_fixture();

        let registry_path = storage_root.join("registry.json");
        let graph_path = storage_root.join("graph.redb");
        let ctx = McpContext::open(McpServerConfig::new(registry_path, graph_path))
            .expect("context should open");

        let response = who_consumes_tool(
            &ctx,
            WhoConsumesRequest {
                symbol: "SHARED_CONFIG".to_owned(),
                repo: Some("service-api".to_owned()),
            },
        )
        .expect("repo-scoped query should resolve");

        let repos: Vec<&str> = response
            .data
            .consumers
            .iter()
            .map(|consumer| consumer.repo.as_str())
            .collect();
        assert!(
            repos.contains(&"service-ui"),
            "service-api scope must surface service-ui: {repos:?}"
        );
        assert!(
            !repos.contains(&"service-review"),
            "service-api scope must not surface billing's consumer: {repos:?}"
        );

        let _ = fs::remove_dir_all(&storage_root);
    }

    /// Builds a graph where `SHARED_CONFIG` is produced by two distinct repos
    /// (`service-api` consumed by `service-ui`, `billing` consumed by
    /// `service-review`), each via a route boundary.
    fn ambiguous_fixture() -> std::path::PathBuf {
        let storage_root = unique_storage_root("gather-step-mcp-who-consumes-ambig");

        let nodes_edges = |producer: &str, route_path: &str, consumer: &str| {
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

        let (mut nodes, mut edges) = nodes_edges("service-api", "/shared", "service-ui");
        let (nodes_b, edges_b) = nodes_edges("billing", "/billing-shared", "service-review");
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

        storage_root
    }
}
