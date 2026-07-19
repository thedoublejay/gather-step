use anyhow::{Context, Result};
use clap::Args;
use comfy_table::{Cell, ContentArrangement, Table, presets::UTF8_BORDERS_ONLY};
use console::style;
use gather_step_analysis::{CrossRepoConsumerLookup, cross_repo_consumers_for_symbol};
use gather_step_core::{NodeId, NodeKind};
use gather_step_storage::{GraphStore, SearchFilters, SearchStore, StorageCoordinator};
use serde::Serialize;
use serde_json::json;

use crate::command_render::RenderedCommand;
use crate::daemon_protocol::DaemonRequest;
use crate::storage_context::StorageContext;
use crate::{app::AppContext, daemon_proxy};

#[derive(Debug, Args)]
pub struct SearchArgs {
    pub query: String,
    #[arg(long, default_value_t = 20, help = "Maximum number of hits to return")]
    pub limit: usize,
    #[arg(
        long,
        value_parser = parse_kind_value,
        help = "Optional node kind filter, e.g. function, class, route, topic"
    )]
    pub kind: Option<String>,
}

#[derive(Debug, Serialize)]
struct SearchOutput {
    event: &'static str,
    query: String,
    total_hits: usize,
    hits: Vec<SearchHitOutput>,
}

#[derive(Debug, Serialize)]
struct SearchHitOutput {
    #[serde(skip)]
    node_id: NodeId,
    repo: String,
    file_path: String,
    line: Option<u32>,
    symbol_name: String,
    qualified_name: Option<String>,
    node_kind: String,
    match_mode: MatchMode,
    matched_field: &'static str,
    symbol_name_exact: bool,
    adjusted_score: f32,
    cross_repo: CrossRepoHit,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum MatchMode {
    Exact,
    Lexical,
    Fuzzy,
}

/// Cross-repo participation annotation for a search hit: which *other* repos
/// consume the exact symbol (directly or via a transport boundary). File and
/// module hits retain file-level participation.
#[derive(Debug, Serialize)]
struct CrossRepoHit {
    participates: bool,
    consumer_repos: Vec<String>,
}

pub fn run(app: &AppContext, args: SearchArgs) -> Result<()> {
    daemon_proxy::run_read_only_command(
        app,
        &DaemonRequest::Search {
            query: args.query.clone(),
            limit: args.limit,
            kind: args.kind.clone(),
            repo_filter: app.repo_filter.clone(),
        },
        move |app| run_rendered(app, &StorageContext::workspace_read_only(app), args),
    )
}

pub(crate) fn run_rendered(
    app: &AppContext,
    ctx: &StorageContext,
    args: SearchArgs,
) -> Result<RenderedCommand> {
    let storage = ctx
        .open_storage_coordinator()
        .context("opening workspace-local storage")?;
    execute(&storage, app.repo_filter.as_deref(), args)
}

pub(crate) fn execute(
    storage: &StorageCoordinator,
    repo_filter: Option<&str>,
    args: SearchArgs,
) -> Result<RenderedCommand> {
    let requested_limit = args.limit.max(1);
    let kind_filter = args.kind.as_deref().map(parse_kind).transpose()?;
    let hits = storage
        .search()
        .search_filtered(
            &args.query,
            requested_limit,
            SearchFilters {
                repo: repo_filter,
                node_kind: kind_filter,
                lang: None,
            },
        )
        .with_context(|| format!("running search for `{}`", args.query))?;

    let mut payload_hits = hits
        .into_iter()
        .filter_map(|hit| {
            // `repo` and `file_path` are not stored in Tantivy (S6); rehydrate
            // from the graph store using the `node_id` that is always present.
            let node = match storage
                .graph()
                .get_node(hit.node_id)
                .with_context(|| format!("loading node {:?}", hit.node_id))
            {
                Ok(n) => n,
                Err(e) => return Some(Err(e)),
            };
            let (repo, file_path, line, qualified_name) = match node {
                Some(n) => (
                    n.repo.clone(),
                    n.file_path.clone(),
                    n.span.as_ref().map(|s| s.line_start),
                    n.qualified_name,
                ),
                None => return None,
            };
            // Defensive: Tantivy already filtered by repo via a MUST clause,
            // but guard against any stale index entries that slipped through.
            if repo_filter.is_some_and(|r| repo.as_str() != r) {
                return None;
            }
            let symbol_name_exact = hit.symbol_name.eq_ignore_ascii_case(args.query.trim());
            let qualified_name_exact = qualified_name
                .as_deref()
                .is_some_and(|name| name.eq_ignore_ascii_case(args.query.trim()));
            let matched_field = if symbol_name_exact {
                "symbol_name"
            } else if qualified_name_exact {
                "qualified_name"
            } else {
                "lexical_index"
            };
            Some(Ok(SearchHitOutput {
                node_id: hit.node_id,
                repo,
                file_path,
                line,
                symbol_name: hit.symbol_name,
                qualified_name,
                node_kind: hit.node_kind.to_string(),
                match_mode: if symbol_name_exact || qualified_name_exact {
                    MatchMode::Exact
                } else if hit.exact_match {
                    MatchMode::Lexical
                } else {
                    MatchMode::Fuzzy
                },
                matched_field,
                symbol_name_exact,
                adjusted_score: hit.adjusted_score,
                cross_repo: CrossRepoHit {
                    participates: false,
                    consumer_repos: Vec::new(),
                },
            }))
        })
        .take(requested_limit)
        .collect::<Result<Vec<_>>>()?;

    annotate_cross_repo(storage, &mut payload_hits);

    let payload = SearchOutput {
        event: "search_completed",
        query: args.query,
        total_hits: payload_hits.len(),
        hits: payload_hits,
    };
    let mut lines = Vec::new();
    if payload.hits.is_empty() {
        lines.push("No matches found.".to_owned());
    } else {
        lines.push(format!(
            "Search results for {} ({})",
            style(&payload.query).cyan().bold(),
            payload.total_hits
        ));
        let mut table = Table::new();
        table.load_preset(UTF8_BORDERS_ONLY);
        table.set_content_arrangement(ContentArrangement::Dynamic);
        table.set_header(vec!["Symbol", "Kind", "Repo", "Location", "Cross-repo"]);
        for hit in &payload.hits {
            let line_suffix = hit.line.map_or_else(String::new, |line| format!(":{line}"));
            let cross_repo = if hit.cross_repo.consumer_repos.is_empty() {
                "-".to_owned()
            } else {
                hit.cross_repo.consumer_repos.join(", ")
            };
            table.add_row(vec![
                Cell::new(&hit.symbol_name),
                Cell::new(&hit.node_kind),
                Cell::new(&hit.repo),
                Cell::new(format!("{}{}", hit.file_path, line_suffix)),
                Cell::new(cross_repo),
            ]);
            if let Some(qualified_name) = &hit.qualified_name {
                table.add_row(vec![
                    Cell::new(style("  qn").dim().to_string()),
                    Cell::new(""),
                    Cell::new(""),
                    Cell::new(style(qualified_name).dim().to_string()),
                    Cell::new(""),
                ]);
            }
        }
        lines.push(table.to_string());
    }

    Ok(RenderedCommand::success(json!(payload), lines))
}

/// Annotate symbol hits with exact consumer reachability. File and module hits
/// retain the broader file-participation projection because they do not name a
/// single declaration.
///
/// Best-effort: a graph error while computing a repo's projection leaves the
/// affected hits with an empty (non-participating) annotation rather than
/// failing the whole search.
fn annotate_cross_repo(storage: &StorageCoordinator, hits: &mut [SearchHitOutput]) {
    let mut file_lookup = CrossRepoConsumerLookup::new();
    for hit in hits.iter_mut() {
        let consumer_repos = if matches!(hit.node_kind.as_str(), "file" | "module") {
            file_lookup.consumer_repos(storage.graph(), &hit.repo, &hit.file_path)
        } else {
            cross_repo_consumers_for_symbol(storage.graph(), hit.node_id)
        };
        if let Ok(consumer_repos) = consumer_repos {
            hit.cross_repo.participates = !consumer_repos.is_empty();
            hit.cross_repo.consumer_repos = consumer_repos;
        }
    }
}

fn parse_kind(value: &str) -> Result<NodeKind> {
    let mut normalized = value.trim().to_owned();
    normalized.make_ascii_lowercase();
    let kind = match normalized.as_str() {
        "file" => NodeKind::File,
        "function" => NodeKind::Function,
        "class" => NodeKind::Class,
        "type" => NodeKind::Type,
        "module" => NodeKind::Module,
        "entity" => NodeKind::Entity,
        "route" => NodeKind::Route,
        "topic" => NodeKind::Topic,
        "queue" => NodeKind::Queue,
        "subject" => NodeKind::Subject,
        "stream" => NodeKind::Stream,
        "event" => NodeKind::Event,
        "sharedsymbol" | "shared_symbol" | "shared-symbol" => NodeKind::SharedSymbol,
        "payloadcontract" | "payload_contract" | "payload-contract" => NodeKind::PayloadContract,
        "repo" => NodeKind::Repo,
        "convention" => NodeKind::Convention,
        "service" => NodeKind::Service,
        "agentgraph" | "agent_graph" | "agent-graph" => NodeKind::AgentGraph,
        "prompt" => NodeKind::Prompt,
        "aicontract" | "ai_contract" | "ai-contract" => NodeKind::AiContract,
        "vectorindex" | "vector_index" | "vector-index" => NodeKind::VectorIndex,
        "mcpserver" | "mcp_server" | "mcp-server" => NodeKind::McpServer,
        "mcptool" | "mcp_tool" | "mcp-tool" => NodeKind::McpTool,
        "llmmodel" | "llm_model" | "llm-model" => NodeKind::LlmModel,
        _ => anyhow::bail!(
            "unsupported search kind `{value}`; expected one of: file, function, class, type, module, entity, route, topic, queue, subject, stream, event, shared_symbol, payload_contract, repo, convention, service, agent_graph, prompt, ai_contract, vector_index, mcp_server, mcp_tool, llm_model"
        ),
    };
    Ok(kind)
}

fn parse_kind_value(value: &str) -> std::result::Result<String, String> {
    parse_kind(value)
        .map(|_| value.to_owned())
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `run_rendered` consults the injected `StorageContext` rather than the
    /// app's `workspace_paths()` default.  A successful search response with
    /// `event=search_completed` proves context injection is honoured.
    #[test]
    fn search_runs_against_explicit_storage_context() {
        let (ctx, _workspace) =
            crate::test_helpers::indexed_fixture("search-ctx-inject", "pr-test-search");
        let app = crate::test_helpers::test_app(ctx.workspace_root().to_path_buf());

        let rendered = run_rendered(
            &app,
            &ctx,
            SearchArgs {
                query: "OrderService".to_owned(),
                limit: 5,
                kind: None,
            },
        )
        .expect("search::run_rendered should succeed");

        let payload = rendered
            .payload
            .as_ref()
            .expect("search should produce a JSON payload");

        assert_eq!(
            payload["event"].as_str(),
            Some("search_completed"),
            "search payload should have event=search_completed, got: {payload}"
        );
        let exact_hit = payload["hits"]
            .as_array()
            .expect("hits array")
            .iter()
            .find(|hit| hit["symbol_name"].as_str() == Some("OrderService"))
            .unwrap_or_else(|| panic!("exact symbol hit should be present: {payload}"));
        assert_eq!(exact_hit["match_mode"], "exact");
        assert_eq!(exact_hit["matched_field"], "symbol_name");
        assert_eq!(exact_hit["symbol_name_exact"], true);
        assert!(
            exact_hit.get("exact_match").is_none(),
            "legacy ambiguous exact_match field must not be emitted: {payload}"
        );
    }

    #[test]
    fn unsupported_kind_is_rejected() {
        let error = parse_kind_value("import").expect_err("unsupported kind must fail");
        assert!(error.contains("unsupported search kind `import`"));
        assert!(error.contains("shared_symbol"));
        assert_eq!(parse_kind_value("route").as_deref(), Ok("route"));
    }

    /// A payload hit whose file transitively feeds a foreign consumer must
    /// carry that consumer in `cross_repo.consumer_repos`.
    #[test]
    fn search_surfaces_cross_repo_consumers() {
        use std::{env, fs};

        use gather_step_core::{
            EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeId, NodeKind, SourceSpan, Visibility,
            node_id, virtual_node,
        };
        use gather_step_storage::GraphStore;

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

        let storage_root = env::temp_dir().join(format!(
            "gather-step-cli-xrepo-search-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&storage_root).expect("storage dir");
        let storage = StorageCoordinator::open(&storage_root).expect("coordinator opens");

        let config_file = file("service-api", "src/config/credit.ts");
        let config_sym = symbol(
            "service-api",
            "src/config/credit.ts",
            "CREDIT_AGENT_CONFIGS",
        );
        let unrelated_sym = symbol("service-api", "src/config/credit.ts", "PRIVATE_HELPER");
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

        storage
            .graph()
            .bulk_insert(
                &[
                    config_file.clone(),
                    config_sym.clone(),
                    unrelated_sym.clone(),
                    handler_file.clone(),
                    handler_sym.clone(),
                    caller_file.clone(),
                    caller_sym.clone(),
                    route.clone(),
                ],
                &[
                    defines(config_file.id, config_sym.id),
                    defines(config_file.id, unrelated_sym.id),
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

        let rendered = execute(
            &storage,
            None,
            SearchArgs {
                query: "CREDIT_AGENT_CONFIGS".to_owned(),
                limit: 10,
                kind: None,
            },
        )
        .expect("search should succeed");
        let payload = rendered.payload.as_ref().expect("payload");

        let hits = payload["hits"].as_array().expect("hits array");
        let config_hit = hits
            .iter()
            .find(|hit| hit["file_path"].as_str() == Some("src/config/credit.ts"))
            .unwrap_or_else(|| panic!("config hit must be present, got: {payload}"));
        let consumers = config_hit["cross_repo"]["consumer_repos"]
            .as_array()
            .expect("consumer_repos array");
        assert!(
            consumers
                .iter()
                .any(|repo| repo.as_str() == Some("service-ui")),
            "config hit must carry service-ui as a cross-repo consumer, got: {payload}"
        );

        let unrelated = execute(
            &storage,
            Some("service-api"),
            SearchArgs {
                query: "PRIVATE_HELPER".to_owned(),
                limit: 10,
                kind: None,
            },
        )
        .expect("search should succeed");
        let unrelated_payload = unrelated.payload.as_ref().expect("payload");
        let unrelated_hit = unrelated_payload["hits"]
            .as_array()
            .expect("hits array")
            .iter()
            .find(|hit| hit["symbol_name"].as_str() == Some("PRIVATE_HELPER"))
            .unwrap_or_else(|| panic!("private helper hit must be present: {unrelated_payload}"));
        assert_eq!(
            unrelated_hit["cross_repo"]["consumer_repos"],
            serde_json::json!([]),
            "unrelated symbol must not inherit its file's consumers: {unrelated_payload}"
        );

        let _ = fs::remove_dir_all(&storage_root);
    }
}
