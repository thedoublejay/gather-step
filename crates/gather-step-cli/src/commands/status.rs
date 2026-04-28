use anyhow::{Context, Result, bail};
use clap::Args;
use comfy_table::{Cell, ContentArrangement, Table, presets::UTF8_BORDERS_ONLY};
use console::style;
use gather_step_analysis::{
    GraphQuery, SemanticHealthReport, semantic_health_for_repo, semantic_health_for_workspace,
};
use gather_step_core::{DepthLevel, RegistryStore};
use gather_step_mcp::output::redact::relativize_to_workspace;
use gather_step_storage::{ContextPackStats, GraphStore, StorageCoordinator};
use serde::Serialize;
use serde_json::{Value, json};

use crate::command_render::RenderedCommand;
use crate::daemon_protocol::DaemonRequest;
use crate::{app::AppContext, daemon_proxy};

#[derive(Debug, Args, Default)]
pub struct StatusArgs {}

#[derive(Debug, Serialize)]
struct StatusOutput {
    event: &'static str,
    workspace: String,
    registry_path: String,
    storage_root: String,
    pack_cache: PackCacheStatusOutput,
    repos: Vec<RepoStatusOutput>,
    graph: GraphStatusOutput,
}

#[derive(Debug, Serialize)]
struct RepoStatusOutput {
    repo: String,
    path: String,
    path_exists: bool,
    depth_level: String,
    last_indexed_at: Option<String>,
    registry_file_count: u64,
    registry_symbol_count: u64,
    graph_node_count: usize,
    metadata_file_count: usize,
    unresolved_inputs: usize,
    frameworks: Vec<String>,
    semantic_health: SemanticHealthReport,
}

#[derive(Debug, Serialize)]
struct GraphStatusOutput {
    node_kinds: Vec<KindCountOutput>,
    edge_kinds: Vec<KindCountOutput>,
    semantic_health: SemanticHealthReport,
}

#[derive(Debug, Serialize)]
struct PackCacheStatusOutput {
    total_packs: usize,
    total_bytes: i64,
    total_hits: i64,
    truncated_packs: usize,
    unresolved_packs: usize,
}

#[derive(Debug, Serialize)]
struct KindCountOutput {
    kind: String,
    count: usize,
}

pub fn run(app: &AppContext, _args: StatusArgs) -> Result<()> {
    daemon_proxy::run_read_only_command(
        app,
        &DaemonRequest::Status {
            repo_filter: app.repo_filter.clone(),
        },
        run_rendered,
    )
}

pub fn run_default(app: &AppContext) -> Result<()> {
    if !app.workspace_paths().registry_path.exists() {
        render_unindexed_summary(app);
        return Ok(());
    }

    run(app, StatusArgs::default())
}

fn render_unindexed_summary(app: &AppContext) {
    let output = app.output();
    output.line("");
    output.line(format!(
        "  {}",
        style(format!("gather-step v{}", env!("CARGO_PKG_VERSION"))).bold()
    ));
    output.line("");
    output.line(format!("  Workspace:   {}", app.workspace_path.display()));
    output.line("  Index:       not indexed yet");
    output.line(format!("  Watch:       {}", watch_state()));
    output.line(format!("  MCP:         {}", mcp_state(app)));
    output.line("");
    output.line(format!("  Next: {}", style("gather-step index").cyan()));
    output.line("");
}

fn mcp_state(app: &AppContext) -> &'static str {
    let local = app.workspace_path.join(".claude/settings.json");
    if json_has_gather_step(&local) {
        return "configured: local";
    }

    if let Some(home) = std::env::var_os("HOME") {
        let global = std::path::PathBuf::from(home).join(".claude/settings.json");
        if json_has_gather_step(&global) {
            return "configured: global";
        }
    }

    "not configured"
}

fn json_has_gather_step(path: &std::path::Path) -> bool {
    let Ok(body) = std::fs::read_to_string(path) else {
        return false;
    };
    let Ok(value) = serde_json::from_str::<Value>(&body) else {
        return false;
    };
    value.pointer("/mcpServers/gather-step").is_some()
}

fn watch_state() -> &'static str {
    "not running"
}

pub(crate) fn run_rendered(app: &AppContext) -> Result<RenderedCommand> {
    let paths = app.workspace_paths();
    let registry = RegistryStore::open(&paths.registry_path)
        .with_context(|| format!("opening {}", paths.registry_path.display()))?;
    let storage = StorageCoordinator::open(&paths.storage_root)
        .with_context(|| format!("opening {}", paths.storage_root.display()))?;
    execute(
        &app.workspace_path,
        &paths.registry_path,
        &paths.storage_root,
        &registry,
        &storage,
        app.repo_filter.as_deref(),
    )
}

pub(crate) fn execute(
    workspace_path: &std::path::Path,
    registry_path: &std::path::Path,
    storage_root: &std::path::Path,
    registry: &RegistryStore,
    storage: &StorageCoordinator,
    repo_filter: Option<&str>,
) -> Result<RenderedCommand> {
    let repos = registry
        .registry()
        .repos
        .iter()
        .filter(|(repo, _)| repo_filter.is_none_or(|wanted| repo.as_str() == wanted))
        .map(|(repo, registered)| {
            let metadata_rows = storage
                .metadata()
                .file_index_states_by_repo(repo)
                .with_context(|| format!("loading metadata file state for `{repo}`"))?;
            let unresolved_inputs = storage
                .metadata()
                .unresolved_resolution_input_count_by_repo(repo)
                .with_context(|| format!("loading unresolved calls for `{repo}`"))?;
            let graph_node_count = storage
                .graph()
                .count_nodes_by_repo(repo)
                .with_context(|| format!("counting graph nodes for `{repo}`"))?;
            let semantic_health = semantic_health_for_repo(
                storage.graph(),
                storage.metadata(),
                repo,
                unresolved_inputs,
            )
            .with_context(|| format!("computing semantic health for `{repo}`"))?;

            Ok(RepoStatusOutput {
                repo: repo.clone(),
                path: relativize_to_workspace(&registered.path, workspace_path),
                path_exists: registered.path.exists(),
                depth_level: depth_label(registered.depth_level).to_owned(),
                last_indexed_at: registered.last_indexed_at.clone(),
                registry_file_count: registered.file_count,
                registry_symbol_count: registered.symbol_count,
                graph_node_count,
                metadata_file_count: metadata_rows.len(),
                unresolved_inputs,
                frameworks: registered.frameworks.clone(),
                semantic_health,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    if repos.is_empty()
        && let Some(repo) = repo_filter
    {
        bail!("repo `{repo}` is not present in the workspace registry");
    }

    let query = GraphQuery::new(storage.graph());
    let node_kinds = query
        .count_by_kind()?
        .into_iter()
        .filter(|(_, count)| *count > 0)
        .map(|(kind, count)| KindCountOutput {
            kind: kind.to_string(),
            count,
        })
        .collect();
    let edge_kinds = query
        .count_edges_by_kind()?
        .into_iter()
        .map(|(kind, count)| KindCountOutput {
            kind: kind.to_string(),
            count,
        })
        .collect();

    let payload = StatusOutput {
        event: "status_completed",
        workspace: relativize_to_workspace(workspace_path, workspace_path),
        registry_path: relativize_to_workspace(registry_path, workspace_path),
        storage_root: relativize_to_workspace(storage_root, workspace_path),
        pack_cache: pack_cache_status(storage.metadata())
            .context("computing context-pack cache statistics")?,
        repos,
        graph: GraphStatusOutput {
            node_kinds,
            edge_kinds,
            semantic_health: semantic_health_for_workspace(storage.graph(), storage.metadata())
                .context("computing workspace semantic health")?,
        },
    };

    let mut lines = vec![
        format!("Workspace: {}", payload.workspace),
        format!("Registry: {}", payload.registry_path),
        format!("Storage: {}", payload.storage_root),
        format!(
            "Pack cache: packs={} bytes={} hits={} truncated={} unresolved={}",
            payload.pack_cache.total_packs,
            payload.pack_cache.total_bytes,
            payload.pack_cache.total_hits,
            payload.pack_cache.truncated_packs,
            payload.pack_cache.unresolved_packs
        ),
    ];
    let mut repo_table = Table::new();
    repo_table.load_preset(UTF8_BORDERS_ONLY);
    repo_table.set_content_arrangement(ContentArrangement::Dynamic);
    repo_table.set_header(vec![
        "Repo",
        "Depth",
        "Indexed",
        "Files",
        "Symbols",
        "Graph",
        "Unresolved",
        "Semantic",
        "Frameworks",
    ]);
    for repo in &payload.repos {
        repo_table.add_row(vec![
            Cell::new(&repo.repo),
            Cell::new(&repo.depth_level),
            Cell::new(repo.last_indexed_at.as_deref().unwrap_or("never")),
            Cell::new(format!(
                "{}/{}",
                repo.registry_file_count, repo.metadata_file_count
            )),
            Cell::new(repo.registry_symbol_count),
            Cell::new(repo.graph_node_count),
            Cell::new(repo.unresolved_inputs),
            Cell::new(format_semantic_summary(&repo.semantic_health)),
            Cell::new(if repo.frameworks.is_empty() {
                "-".to_owned()
            } else {
                repo.frameworks.join(",")
            }),
        ]);
    }
    lines.push(repo_table.to_string());

    let mut graph_table = Table::new();
    graph_table.load_preset(UTF8_BORDERS_ONLY);
    graph_table.set_content_arrangement(ContentArrangement::Dynamic);
    graph_table.set_header(vec!["Node Kind", "Count"]);
    for item in &payload.graph.node_kinds {
        graph_table.add_row(vec![Cell::new(&item.kind), Cell::new(item.count)]);
    }
    lines.push("Graph nodes:".to_owned());
    lines.push(graph_table.to_string());
    lines.push(format!(
        "Semantic health: {}",
        format_semantic_summary(&payload.graph.semantic_health)
    ));

    Ok(RenderedCommand::success(json!(payload), lines))
}

fn format_semantic_summary(health: &SemanticHealthReport) -> String {
    format!(
        "routes {}/{} events {}/{} shared {}/{} contracts {}/{} orphan {}",
        health.route_links.linked_targets,
        health.route_links.total_targets,
        health.event_links.linked_targets,
        health.event_links.total_targets,
        health.shared_symbol_links.linked_targets,
        health.shared_symbol_links.total_targets,
        health.payload_contract_links.linked_targets,
        health.payload_contract_links.total_targets,
        health.orphan_topics
    )
}

fn depth_label(depth: DepthLevel) -> &'static str {
    match depth {
        DepthLevel::Level1 => "level1",
        DepthLevel::Level2 => "level2",
        DepthLevel::Level3 => "level3",
        DepthLevel::Full => "full",
        _ => "unknown",
    }
}

fn pack_cache_status(
    metadata: &gather_step_storage::MetadataStoreDb,
) -> Result<PackCacheStatusOutput> {
    let ContextPackStats {
        total_packs,
        total_bytes,
        total_hits,
    } = metadata.context_pack_stats()?;
    let records = metadata.list_context_packs()?;
    let mut truncated_packs = 0_usize;
    let mut unresolved_packs = 0_usize;
    for record in records {
        let value: serde_json::Value = serde_json::from_slice(&record.response)
            .with_context(|| format!("deserializing context pack `{}`", record.pack_key))?;
        if value
            .get("meta")
            .and_then(|meta| meta.get("budget"))
            .and_then(|budget| budget.get("truncated"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            truncated_packs += 1;
        }
        if value
            .get("data")
            .and_then(|data| data.get("found"))
            .and_then(serde_json::Value::as_bool)
            .is_some_and(|found| !found)
        {
            unresolved_packs += 1;
        }
    }
    Ok(PackCacheStatusOutput {
        total_packs,
        total_bytes,
        total_hits,
        truncated_packs,
        unresolved_packs,
    })
}
