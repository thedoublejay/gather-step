use anyhow::{Context, Result, bail};
use clap::Args;
use comfy_table::{Cell, ContentArrangement, Table, presets::UTF8_BORDERS_ONLY};
use gather_step_analysis::{SemanticHealthReport, semantic_health_for_repo};
use gather_step_core::NodeKind;
use gather_step_core::RegistryStore;
use gather_step_parser::resolve::{ResolutionInput, is_non_actionable_unresolved_call};
use gather_step_storage::{GraphStore, SearchStore, StorageCoordinator};
use serde::Serialize;
use serde_json::json;

use crate::command_render::RenderedCommand;
use crate::daemon_protocol::DaemonRequest;
use crate::storage_context::StorageContext;
use crate::{app::AppContext, daemon_proxy};

#[derive(Debug, Args, Default)]
pub struct DoctorArgs {}

#[derive(Debug, Serialize)]
struct DoctorOutput {
    event: &'static str,
    ok: bool,
    issue_count: usize,
    pack_metrics: PackDoctorOutput,
    repos: Vec<RepoDoctorOutput>,
}

#[derive(Debug, Serialize)]
struct RepoDoctorOutput {
    repo: String,
    ok: bool,
    issues: Vec<String>,
    unresolved_inputs: usize,
    dangling_edges: usize,
    semantic_health: SemanticHealthReport,
}

// Every field intentionally ends in `_packs` because each describes a count of
// context packs in a different state. Renaming to drop the postfix would make
// the serialized JSON ambiguous ("total" of what?).
#[expect(
    clippy::struct_field_names,
    reason = "serialized field names describe which kind of pack count; postfix is load-bearing"
)]
#[derive(Debug, Serialize)]
struct PackDoctorOutput {
    total_packs: usize,
    truncated_packs: usize,
    unresolved_packs: usize,
}

pub fn run(app: &AppContext, _args: DoctorArgs) -> Result<()> {
    daemon_proxy::run_read_only_command(
        app,
        &DaemonRequest::Doctor {
            repo_filter: app.repo_filter.clone(),
        },
        |app| run_rendered(app, &StorageContext::workspace_read_only(app)),
    )
}

pub(crate) fn run_rendered(app: &AppContext, ctx: &StorageContext) -> Result<RenderedCommand> {
    let registry = RegistryStore::open(ctx.registry_path())
        .with_context(|| format!("opening {}", ctx.registry_path().display()))?;
    let storage = ctx
        .open_storage_coordinator()
        .with_context(|| format!("opening {}", ctx.storage_root().display()))?;
    execute(&registry, &storage, app.repo_filter.as_deref())
}

pub(crate) fn execute(
    registry: &RegistryStore,
    storage: &StorageCoordinator,
    repo_filter: Option<&str>,
) -> Result<RenderedCommand> {
    let repos = registry
        .registry()
        .repos
        .iter()
        .filter(|(repo, _)| repo_filter.is_none_or(|wanted| repo.as_str() == wanted))
        .map(|(repo, registered)| inspect_repo(repo, registered, storage))
        .collect::<Result<Vec<_>>>()?;

    if repos.is_empty()
        && let Some(repo) = repo_filter
    {
        bail!("repo `{repo}` is not present in the workspace registry");
    }

    let issue_count = repos.iter().map(|repo| repo.issues.len()).sum();
    let pack_metrics = pack_metrics(storage.metadata()).context("computing pack diagnostics")?;
    let payload = DoctorOutput {
        event: "doctor_completed",
        ok: issue_count == 0,
        issue_count,
        pack_metrics,
        repos,
    };

    let mut lines = Vec::new();
    if payload.ok {
        lines.push("Doctor checks passed.".to_owned());
    } else {
        lines.push(format!("Doctor found {} issue(s).", payload.issue_count));
        let mut table = Table::new();
        table.load_preset(UTF8_BORDERS_ONLY);
        table.set_content_arrangement(ContentArrangement::Dynamic);
        table.set_header(vec![
            "Repo",
            "Status",
            "Unresolved",
            "Dangling",
            "Semantic",
            "Issues",
        ]);
        for repo in &payload.repos {
            table.add_row(vec![
                Cell::new(&repo.repo),
                Cell::new(if repo.ok { "ok" } else { "issues" }),
                Cell::new(repo.unresolved_inputs),
                Cell::new(repo.dangling_edges),
                Cell::new(format_semantic_summary(&repo.semantic_health)),
                Cell::new(if repo.issues.is_empty() {
                    "-".to_owned()
                } else {
                    repo.issues.join("; ")
                }),
            ]);
        }
        lines.push(table.to_string());
        lines.push(format!(
            "Pack diagnostics: packs={} truncated={} unresolved={}",
            payload.pack_metrics.total_packs,
            payload.pack_metrics.truncated_packs,
            payload.pack_metrics.unresolved_packs
        ));
    }
    let payload_json = json!(payload);
    Ok(if issue_count == 0 {
        RenderedCommand::success(payload_json, lines)
    } else {
        RenderedCommand::failure(
            Some(payload_json),
            lines,
            format!("doctor found {issue_count} issue(s)"),
        )
    })
}

fn inspect_repo(
    repo: &str,
    registered: &gather_step_core::RegisteredRepo,
    storage: &StorageCoordinator,
) -> Result<RepoDoctorOutput> {
    let mut issues = Vec::new();

    if !registered.path.exists() {
        issues.push(format!(
            "registered path does not exist: {}",
            registered.path.display()
        ));
    }

    let metadata_rows = storage
        .metadata()
        .file_index_states_by_repo(repo)
        .with_context(|| format!("loading metadata file state for `{repo}`"))?;
    let registered_file_count = usize::try_from(registered.file_count).unwrap_or(usize::MAX);
    if metadata_rows.len() < registered_file_count {
        issues.push(format!(
            "registry file_count={} but metadata has only {} indexed files",
            registered_file_count,
            metadata_rows.len()
        ));
    }

    let nodes = storage
        .graph()
        .nodes_by_repo(repo)
        .with_context(|| format!("loading graph nodes for `{repo}`"))?;
    if registered.symbol_count > 0 && nodes.is_empty() {
        issues.push("registry reports symbols but graph has no nodes".to_owned());
    }

    let unresolved_inputs = storage
        .metadata()
        .unresolved_resolution_inputs_by_repo(repo)
        .with_context(|| format!("loading unresolved calls for `{repo}`"))?;
    let actionable_unresolved_inputs = count_actionable_unresolved_inputs(&unresolved_inputs);
    if actionable_unresolved_inputs > 0 {
        issues.push(format!(
            "{actionable_unresolved_inputs} unresolved call input(s) remain"
        ));
    }

    let dangling_edges = count_dangling_edges(repo, storage, &nodes)
        .with_context(|| format!("checking dangling edges for `{repo}`"))?;
    if dangling_edges > 0 {
        issues.push(format!(
            "{dangling_edges} dangling edge(s) owned by repo files"
        ));
    }

    if let Some(sample) = nodes.iter().find(|node| node.kind.is_search_indexable()) {
        let search_hits = storage
            .search()
            .search(&sample.name, 10)
            .with_context(|| format!("checking search projection for `{repo}`"))?;
        if !search_hits.iter().any(|hit| hit.node_id == sample.id) {
            issues.push(format!(
                "search projection did not return sample indexed symbol `{}`",
                sample.name
            ));
        }
    } else if registered.symbol_count > 0 {
        issues.push("no searchable nodes found for repo despite indexed symbols".to_owned());
    }
    let semantic_health = semantic_health_for_repo(
        storage.graph(),
        storage.metadata(),
        repo,
        actionable_unresolved_inputs,
    )
    .with_context(|| format!("computing semantic health for `{repo}`"))?;
    issues.extend(semantic_issues(&semantic_health));

    Ok(RepoDoctorOutput {
        repo: repo.to_owned(),
        ok: issues.is_empty(),
        issues,
        unresolved_inputs: actionable_unresolved_inputs,
        dangling_edges,
        semantic_health,
    })
}

pub(crate) fn count_actionable_unresolved_inputs(inputs: &[ResolutionInput]) -> usize {
    inputs
        .iter()
        .flat_map(|input| input.call_sites.iter())
        .filter(|call_site| !is_non_actionable_unresolved_call(call_site))
        .count()
}

fn count_dangling_edges(
    repo: &str,
    storage: &StorageCoordinator,
    nodes: &[gather_step_core::NodeData],
) -> Result<usize> {
    let mut dangling_edges = 0_usize;

    for node in nodes.iter().filter(|node| node.kind == NodeKind::File) {
        for edge in storage
            .graph()
            .edges_by_owner(node.id)
            .with_context(|| format!("loading edges for owner {:?}", node.id))?
        {
            if storage.graph().get_node(edge.target)?.is_none() {
                let _ = repo;
                dangling_edges += 1;
            }
        }
    }

    Ok(dangling_edges)
}

fn semantic_issues(health: &SemanticHealthReport) -> Vec<String> {
    let mut issues = Vec::new();
    if health.route_links.unlinked_targets > 0 || health.route_links.partially_linked_targets > 0 {
        issues.push(format!(
            "route links incomplete: linked={} partial={} unlinked={} ambiguous={}",
            health.route_links.linked_targets,
            health.route_links.partially_linked_targets,
            health.route_links.unlinked_targets,
            health.route_links.ambiguous_targets
        ));
    }
    if health.event_links.unlinked_targets > 0 || health.event_links.partially_linked_targets > 0 {
        issues.push(format!(
            "event links incomplete: linked={} partial={} unlinked={} ambiguous={}",
            health.event_links.linked_targets,
            health.event_links.partially_linked_targets,
            health.event_links.unlinked_targets,
            health.event_links.ambiguous_targets
        ));
    }
    if health.shared_symbol_links.unlinked_targets > 0
        || health.shared_symbol_links.partially_linked_targets > 0
    {
        issues.push(format!(
            "shared symbol links incomplete: linked={} partial={} unlinked={} ambiguous={}",
            health.shared_symbol_links.linked_targets,
            health.shared_symbol_links.partially_linked_targets,
            health.shared_symbol_links.unlinked_targets,
            health.shared_symbol_links.ambiguous_targets
        ));
    }
    if health.payload_contract_links.partially_linked_targets > 0
        || health.payload_contract_links.ambiguous_targets > 0
    {
        issues.push(format!(
            "payload contracts incomplete: linked={} partial={} ambiguous={}",
            health.payload_contract_links.linked_targets,
            health.payload_contract_links.partially_linked_targets,
            health.payload_contract_links.ambiguous_targets
        ));
    }
    // Orphan event topics are surfaced in `semantic_health` and via
    // `events orphans`. They are not a hard doctor failure because real
    // workspaces often index only one side of an event boundary: producer-only
    // topics can have external consumers, and consumer-only topics can be
    // driven by infrastructure or services outside the configured repo set.
    issues
}

fn format_semantic_summary(health: &SemanticHealthReport) -> String {
    format!(
        "r {}/{} e {}/{} s {}/{} c {}/{} o {}",
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

fn pack_metrics(metadata: &gather_step_storage::MetadataStoreDb) -> Result<PackDoctorOutput> {
    let total_packs = metadata.context_pack_stats()?.total_packs;
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
    Ok(PackDoctorOutput {
        total_packs,
        truncated_packs,
        unresolved_packs,
    })
}

#[cfg(test)]
mod tests {
    use super::semantic_issues;

    fn healthy_link() -> gather_step_analysis::SemanticLinkHealth {
        gather_step_analysis::SemanticLinkHealth {
            total_targets: 0,
            linked_targets: 0,
            partially_linked_targets: 0,
            unlinked_targets: 0,
            ambiguous_targets: 0,
            coverage_ratio: 1.0,
        }
    }

    #[test]
    fn orphan_topics_remain_diagnostic_not_hard_failures() {
        let health = gather_step_analysis::SemanticHealthReport {
            route_links: healthy_link(),
            event_links: healthy_link(),
            shared_symbol_links: healthy_link(),
            payload_contract_links: healthy_link(),
            orphan_topics: 1,
            unresolved_call_inputs: 0,
        };

        assert!(semantic_issues(&health).is_empty());
    }
}
