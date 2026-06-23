use anyhow::{Context, Result, bail};
use clap::Args;
use comfy_table::{Cell, ContentArrangement, Table, presets::UTF8_BORDERS_ONLY};
use gather_step_analysis::{
    SemanticHealthReport, analyze_shared_component_reuse, find_cycles, find_mock_leakage,
    semantic_health_for_repo,
};
use gather_step_core::EdgeKind;
use gather_step_core::NodeKind;
use gather_step_core::RegistryStore;
use gather_step_parser::resolve::{ResolutionInput, is_non_actionable_unresolved_call};
use gather_step_storage::{GraphStore, SearchStore, StorageCoordinator};
use serde::Serialize;
use serde_json::json;

use crate::command_render::RenderedCommand;
use crate::daemon_protocol::DaemonRequest;
use crate::freshness::{RepoFreshness, workspace_freshness};
use crate::storage_context::StorageContext;
use crate::{app::AppContext, daemon_proxy};

#[derive(Debug, Args, Default)]
pub struct DoctorArgs {}

#[derive(Debug, Serialize)]
struct DoctorOutput {
    event: &'static str,
    ok: bool,
    issue_count: usize,
    graph_health: GraphHealthOutput,
    pack_metrics: PackDoctorOutput,
    repos: Vec<RepoDoctorOutput>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    quality_advisories: Vec<QualityAdvisory>,
    locks: Vec<LockOutput>,
}

#[derive(Debug, Serialize)]
struct LockOutput {
    label: String,
    age_secs: u64,
    pid: Option<u32>,
    hostname: Option<String>,
    owner_alive: Option<bool>,
}

#[derive(Debug, Serialize)]
struct QualityAdvisory {
    rule_id: &'static str,
    confidence: f64,
    message: String,
}

#[derive(Debug, Serialize)]
struct GraphHealthOutput {
    degraded: bool,
    total_repos: usize,
    fresh_repos: usize,
    stale_repos: Vec<String>,
    unknown_repos: Vec<String>,
    never_indexed_repos: Vec<String>,
    truncated_packs: usize,
    reasons: Vec<String>,
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
        bail!("Repo `{repo}` is not present in the workspace registry.");
    }

    let pack_metrics = pack_metrics(storage.metadata()).context("computing pack diagnostics")?;
    let graph_health = summarize_graph_health(
        workspace_freshness(registry, storage.metadata()),
        repo_filter,
        pack_metrics.truncated_packs,
    );
    let issue_count = repos.iter().map(|repo| repo.issues.len()).sum::<usize>()
        + usize::from(graph_health.degraded);
    let repo_names: Vec<String> = repos.iter().map(|repo| repo.repo.clone()).collect();
    let quality_advisories = collect_quality_advisories(storage, &repo_names)
        .context("collecting code-quality advisories")?;
    let locks = collect_locks(storage, registry);
    let payload = DoctorOutput {
        event: "doctor_completed",
        ok: issue_count == 0,
        issue_count,
        graph_health,
        pack_metrics,
        repos,
        quality_advisories,
        locks,
    };

    let mut lines = Vec::new();
    if payload.ok {
        lines.push("Doctor checks passed.".to_owned());
        lines.push(format_graph_health_line(&payload.graph_health));
    } else {
        lines.push(format!("Doctor found {} issue(s).", payload.issue_count));
        lines.push(format_graph_health_line(&payload.graph_health));
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
    if !payload.quality_advisories.is_empty() {
        lines.push(format!(
            "Code-quality advisories ({}):",
            payload.quality_advisories.len()
        ));
        for advisory in &payload.quality_advisories {
            lines.push(format!(
                "  - [{}] ({:.2}) {}",
                advisory.rule_id, advisory.confidence, advisory.message
            ));
        }
    }
    if payload.locks.is_empty() {
        lines.push("Locks: none".to_owned());
    } else {
        for lock in &payload.locks {
            lines.push(format!("Locks: {}", format_lock_line(lock)));
        }
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

const MAX_ADVISORIES_PER_CATEGORY: usize = 50;

fn summarize_graph_health(
    freshness: Vec<RepoFreshness>,
    repo_filter: Option<&str>,
    truncated_packs: usize,
) -> GraphHealthOutput {
    let mut total_repos = 0_usize;
    let mut fresh_repos = 0_usize;
    let mut stale_repos = Vec::new();
    let mut unknown_repos = Vec::new();
    let mut never_indexed_repos = Vec::new();

    for entry in freshness
        .into_iter()
        .filter(|entry| repo_filter.is_none_or(|wanted| entry.repo == wanted))
    {
        total_repos += 1;
        match entry.freshness.as_str() {
            "fresh" => fresh_repos += 1,
            "stale" => stale_repos.push(entry.repo),
            "never_indexed" => never_indexed_repos.push(entry.repo),
            _ => unknown_repos.push(entry.repo),
        }
    }

    // Unknown freshness means git freshness could not be computed (e.g. the
    // registered path is not a git repository). That is informational, not a
    // graph degradation, so it is reported in `unknown_repos` but never flips
    // `degraded` / fails `doctor`.
    let degraded = !stale_repos.is_empty()
        || !never_indexed_repos.is_empty()
        || truncated_packs > 0;
    let mut reasons = Vec::new();
    if !stale_repos.is_empty() {
        reasons.push(format!(
            "{} repo(s) are stale relative to git HEAD",
            stale_repos.len()
        ));
    }
    if !never_indexed_repos.is_empty() {
        reasons.push(format!(
            "{} repo(s) have never been indexed",
            never_indexed_repos.len()
        ));
    }
    if truncated_packs > 0 {
        reasons.push(format!("{truncated_packs} context pack(s) are truncated"));
    }

    GraphHealthOutput {
        degraded,
        total_repos,
        fresh_repos,
        stale_repos,
        unknown_repos,
        never_indexed_repos,
        truncated_packs,
        reasons,
    }
}

fn format_graph_health_line(health: &GraphHealthOutput) -> String {
    if health.degraded {
        format!("Graph health: degraded ({})", health.reasons.join("; "))
    } else {
        format!(
            "Graph health: fresh ({} of {} repo(s))",
            health.fresh_repos, health.total_repos
        )
    }
}

fn collect_quality_advisories(
    storage: &StorageCoordinator,
    repos: &[String],
) -> Result<Vec<QualityAdvisory>> {
    let graph = storage.graph();
    let repo_set: std::collections::BTreeSet<&str> = repos.iter().map(String::as_str).collect();
    let mut advisories = Vec::new();

    let cycles = find_cycles(graph, Some(&[EdgeKind::Imports, EdgeKind::Calls]))
        .context("detecting dependency cycles")?;
    push_capped(
        &mut advisories,
        "GS-GRAPH-DEPENDENCY-CYCLE",
        0.9,
        cycles
            .iter()
            // Scope to the repos under inspection so `--repo X` does not report
            // cycles that do not involve `X`.
            .filter(|cycle| {
                cycle
                    .repos
                    .iter()
                    .any(|repo| repo_set.contains(repo.as_str()))
            })
            .map(|cycle| {
                let scope = if cycle.cross_repo {
                    " (cross-repo)"
                } else {
                    ""
                };
                format!("Dependency cycle{scope}: {}", cycle.nodes.join(" -> "))
            }),
        "dependency cycle",
    );

    for repo in repos {
        let leaks = find_mock_leakage(graph, repo)
            .with_context(|| format!("detecting mock leakage in `{repo}`"))?;
        push_capped(
            &mut advisories,
            "GS-FE-MOCK-IN-PRODUCTION",
            0.8,
            leaks.iter().map(|leak| {
                format!(
                    "Mock import in production (`{repo}`): {} imports {}",
                    leak.importer_file, leak.mock_file
                )
            }),
            "mock import in production",
        );

        let forks = analyze_shared_component_reuse(graph, repo)
            .with_context(|| format!("auditing shared-component reuse in `{repo}`"))?;
        push_capped(
            &mut advisories,
            "GS-FE-SHARED-COMPONENT-FORK",
            0.5,
            forks.iter().map(|fork| {
                format!(
                    "Reuse opportunity (`{repo}`): {} duplicates shared `{}` ({})",
                    fork.local_file, fork.shared_symbol, fork.shared_file
                )
            }),
            "reuse opportunity",
        );
    }

    Ok(advisories)
}

fn push_capped(
    advisories: &mut Vec<QualityAdvisory>,
    rule_id: &'static str,
    confidence: f64,
    items: impl Iterator<Item = String>,
    label: &str,
) {
    let collected: Vec<String> = items.collect();
    let total = collected.len();
    for message in collected.into_iter().take(MAX_ADVISORIES_PER_CATEGORY) {
        advisories.push(QualityAdvisory {
            rule_id,
            confidence,
            message,
        });
    }
    if total > MAX_ADVISORIES_PER_CATEGORY {
        advisories.push(QualityAdvisory {
            rule_id,
            confidence,
            message: format!(
                "... and {} more {label} finding(s) not shown",
                total - MAX_ADVISORIES_PER_CATEGORY
            ),
        });
    }
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

fn collect_locks(storage: &StorageCoordinator, registry: &RegistryStore) -> Vec<LockOutput> {
    let locks_dir = gather_step_storage::lock::lock_dir(storage.root());
    let repo_names: Vec<String> = registry.registry().repos.keys().cloned().collect();
    gather_step_storage::lock::scan_locks(&locks_dir, &repo_names)
        .into_iter()
        .map(|report| LockOutput {
            label: report
                .repo
                .unwrap_or_else(|| report.hash.chars().take(12).collect()),
            age_secs: report.age.as_secs(),
            pid: report.owner.as_ref().map(|owner| owner.pid),
            hostname: report.owner.as_ref().map(|owner| owner.hostname.clone()),
            owner_alive: report.owner_alive,
        })
        .collect()
}

fn format_lock_age(secs: u64) -> String {
    if secs >= 60 {
        format!("{}m{:02}s", secs / 60, secs % 60)
    } else {
        format!("{secs}s")
    }
}

fn format_lock_line(lock: &LockOutput) -> String {
    let owner = match (lock.pid, &lock.hostname) {
        (Some(pid), Some(host)) => format!("pid {pid} on {host}"),
        (Some(pid), None) => format!("pid {pid}"),
        _ => "unknown owner".to_owned(),
    };
    let alive = match lock.owner_alive {
        Some(true) => "alive",
        Some(false) => "dead",
        None => "unknown",
    };
    format!(
        "`{}` held {} by {owner} ({alive})",
        lock.label,
        format_lock_age(lock.age_secs)
    )
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
    fn doctor_payload_always_carries_locks_array() {
        let (ctx, _workspace) =
            crate::test_helpers::indexed_fixture("doctor-locks", "pr-test-doctor");
        let app = crate::test_helpers::test_app(ctx.workspace_root().to_path_buf());

        let rendered =
            super::run_rendered(&app, &ctx).expect("doctor::run_rendered should succeed");
        let payload = rendered
            .payload
            .as_ref()
            .expect("doctor should produce a JSON payload");

        assert!(
            payload["locks"].is_array(),
            "doctor payload should always carry a locks array, got {:?}",
            payload["locks"]
        );
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

    fn freshness(repo: &str, kind: &str) -> crate::freshness::RepoFreshness {
        crate::freshness::RepoFreshness {
            repo: repo.to_owned(),
            freshness: kind.to_owned(),
        }
    }

    #[test]
    fn unknown_freshness_is_informational_not_degraded() {
        // Non-git workspaces report "unknown" freshness because git freshness
        // cannot be computed. That must not flip the graph to degraded or fail
        // `doctor`; otherwise indexing any non-git workspace reports unhealthy.
        let health = super::summarize_graph_health(
            vec![
                freshness("backend", "unknown"),
                freshness("frontend", "unknown"),
            ],
            None,
            0,
        );

        assert!(
            !health.degraded,
            "unknown-only freshness should not be degraded"
        );
        assert_eq!(health.unknown_repos.len(), 2); // still reported for visibility
        assert!(health.reasons.is_empty()); // reasons list only degradation causes
    }

    #[test]
    fn stale_repo_is_degraded() {
        let health = super::summarize_graph_health(vec![freshness("backend", "stale")], None, 0);
        assert!(health.degraded);
        assert_eq!(health.stale_repos, vec!["backend".to_owned()]);
    }

    #[test]
    fn never_indexed_repo_is_degraded() {
        let health =
            super::summarize_graph_health(vec![freshness("backend", "never_indexed")], None, 0);
        assert!(health.degraded);
    }

    #[test]
    fn truncated_packs_are_degraded() {
        let health = super::summarize_graph_health(vec![freshness("backend", "fresh")], None, 3);
        assert!(health.degraded);
        assert_eq!(health.truncated_packs, 3);
    }
}
