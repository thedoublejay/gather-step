use anyhow::{Context, Result};
use clap::Args;
use comfy_table::{Cell, ContentArrangement, Table, presets::UTF8_BORDERS_ONLY};
use console::style;
use gather_step_core::NodeKind;
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
    repo: String,
    file_path: String,
    line: Option<u32>,
    symbol_name: String,
    qualified_name: Option<String>,
    node_kind: String,
    exact_match: bool,
    adjusted_score: f32,
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
    let kind_filter = args.kind.as_deref().and_then(parse_kind);
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

    let payload_hits = hits
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
            Some(Ok(SearchHitOutput {
                repo,
                file_path,
                line,
                symbol_name: hit.symbol_name,
                qualified_name,
                node_kind: hit.node_kind.to_string(),
                exact_match: hit.exact_match,
                adjusted_score: hit.adjusted_score,
            }))
        })
        .take(requested_limit)
        .collect::<Result<Vec<_>>>()?;

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
        table.set_header(vec!["Symbol", "Kind", "Repo", "Location"]);
        for hit in &payload.hits {
            let line_suffix = hit.line.map_or_else(String::new, |line| format!(":{line}"));
            table.add_row(vec![
                Cell::new(&hit.symbol_name),
                Cell::new(&hit.node_kind),
                Cell::new(&hit.repo),
                Cell::new(format!("{}{}", hit.file_path, line_suffix)),
            ]);
            if let Some(qualified_name) = &hit.qualified_name {
                table.add_row(vec![
                    Cell::new(style("  qn").dim().to_string()),
                    Cell::new(""),
                    Cell::new(""),
                    Cell::new(style(qualified_name).dim().to_string()),
                ]);
            }
        }
        lines.push(table.to_string());
    }

    Ok(RenderedCommand::success(json!(payload), lines))
}

fn parse_kind(value: &str) -> Option<NodeKind> {
    let mut normalized = value.trim().to_owned();
    normalized.make_ascii_lowercase();
    match normalized.as_str() {
        "file" => Some(NodeKind::File),
        "function" => Some(NodeKind::Function),
        "class" => Some(NodeKind::Class),
        "type" => Some(NodeKind::Type),
        "module" => Some(NodeKind::Module),
        "entity" => Some(NodeKind::Entity),
        "route" => Some(NodeKind::Route),
        "topic" => Some(NodeKind::Topic),
        "queue" => Some(NodeKind::Queue),
        "subject" => Some(NodeKind::Subject),
        "stream" => Some(NodeKind::Stream),
        "event" => Some(NodeKind::Event),
        "sharedsymbol" | "shared_symbol" | "shared-symbol" => Some(NodeKind::SharedSymbol),
        "payloadcontract" | "payload_contract" | "payload-contract" => {
            Some(NodeKind::PayloadContract)
        }
        "repo" => Some(NodeKind::Repo),
        "convention" => Some(NodeKind::Convention),
        "service" => Some(NodeKind::Service),
        _ => None,
    }
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
    }
}
