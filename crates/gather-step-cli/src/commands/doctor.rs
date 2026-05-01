use anyhow::{Context, Result, bail};
use clap::Args;
use comfy_table::{Cell, ContentArrangement, Table, presets::UTF8_BORDERS_ONLY};
use gather_step_analysis::{SemanticHealthReport, semantic_health_for_repo};
use gather_step_core::NodeKind;
use gather_step_core::RegistryStore;
use gather_step_parser::resolve::ResolutionInput;
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

fn count_actionable_unresolved_inputs(inputs: &[ResolutionInput]) -> usize {
    inputs
        .iter()
        .flat_map(|input| input.call_sites.iter())
        .filter(|call_site| !looks_like_non_actionable_runtime_call(call_site))
        .count()
}

fn looks_like_non_actionable_runtime_call(
    call_site: &gather_step_parser::resolve::CallSite,
) -> bool {
    if call_site.callee_name.eq_ignore_ascii_case("fetch") {
        return true;
    }

    if is_non_actionable_test_file(call_site.source_path.as_path()) {
        return true;
    }

    if is_global_runtime_function(call_site.callee_name.as_str()) {
        return true;
    }

    if is_setter_like_name(call_site.callee_name.as_str()) || call_site.callee_name == "navigate" {
        return true;
    }

    let Some(hint) = call_site.callee_qualified_hint.as_deref() else {
        return false;
    };
    let mut hint = hint.to_owned();
    hint.make_ascii_lowercase();
    let mut name = call_site.callee_name.clone();
    name.make_ascii_lowercase();

    if matches!(
        name.as_str(),
        "get" | "post" | "put" | "patch" | "delete" | "head" | "options" | "request"
    ) && ["http", "api", "client", "fetch", "request"]
        .iter()
        .any(|segment| hint.contains(segment))
    {
        return true;
    }

    // `sendmessage` lowercased matches `sendMessage`, the NestJS messaging
    // client shape. Without it here, `this.bus.sendMessage(...)` on a
    // workspace where the bus is injected without a resolvable type
    // declaration (existing fixture pattern) trips doctor's unresolved-call
    // counter and masks genuine resolution gaps.
    if matches!(
        name.as_str(),
        "emit" | "send" | "sendmessage" | "publish" | "produce"
    ) && ["bus", "client", "event", "kafka", "producer", "publisher"]
        .iter()
        .any(|segment| hint.contains(segment))
    {
        return true;
    }

    // ORM / persistence framework method calls: Mongoose / TypeORM / Prisma /
    // Sequelize expose repository- or model-like instances whose method names
    // collide with very common identifiers (`create`, `save`, `find`, …).
    // These rarely resolve to in-workspace symbols because the method comes
    // from an external dependency, so counting them as "unresolved" drowns
    // out real parser-resolution gaps in doctor output.
    if matches!(
        name.as_str(),
        "create"
            | "save"
            | "find"
            | "findone"
            | "findall"
            | "findmany"
            | "findoneandupdate"
            | "findoneanddelete"
            | "findbyidandupdate"
            | "findbyidanddelete"
            | "updateone"
            | "updatemany"
            | "deleteone"
            | "deletemany"
            | "insertmany"
            | "count"
            | "aggregate"
            | "exec"
    ) && [
        "model",
        "repository",
        "orm",
        "mongoose",
        "prisma",
        "typeorm",
        "sequelize",
    ]
    .iter()
    .any(|segment| hint.contains(segment))
    {
        return true;
    }

    // NestJS Mongoose `SchemaFactory.createForClass(Entity)` — a framework
    // constructor, not a workspace call.
    if name == "createforclass" && hint.contains("schemafactory") {
        return true;
    }

    if is_non_actionable_runtime_hint(hint.as_str(), name.as_str()) {
        return true;
    }

    false
}

fn is_non_actionable_test_file(path: &std::path::Path) -> bool {
    let path = path.to_string_lossy();
    path.contains("/cypress/")
        || path.contains("/__tests__/")
        || path.contains("/__mocks__/")
        || path.ends_with(".cy.ts")
        || path.ends_with(".cy.tsx")
        || path.ends_with(".cy.js")
        || path.ends_with(".cy.jsx")
        || path.ends_with(".stories.ts")
        || path.ends_with(".stories.tsx")
        || path.ends_with(".stories.js")
        || path.ends_with(".stories.jsx")
}

fn is_global_runtime_function(name: &str) -> bool {
    matches!(
        name,
        "setTimeout"
            | "clearTimeout"
            | "setInterval"
            | "clearInterval"
            | "requestAnimationFrame"
            | "cancelAnimationFrame"
            | "parseInt"
            | "parseFloat"
            | "encodeURIComponent"
            | "decodeURIComponent"
            | "isNaN"
    )
}

fn is_setter_like_name(name: &str) -> bool {
    name.strip_prefix("set")
        .and_then(|suffix| suffix.chars().next())
        .is_some_and(char::is_uppercase)
}

fn is_non_actionable_runtime_hint(hint: &str, name: &str) -> bool {
    if matches!(
        hint,
        h if h.starts_with("cy.")
            || h.starts_with("console.")
            || h.starts_with("snackbar.")
            || h.starts_with("document.")
            || h.starts_with("window.location.")
            || h.starts_with("observer.")
            || h.starts_with("intl.datetimeformat")
            || h.starts_with("object.")
            || h.starts_with("array.")
            || h.starts_with("json.")
            || h.starts_with("math.")
            || h.starts_with("date.")
            || h.starts_with("promise.")
            || h.starts_with("queryclient.")
            || h.starts_with("theme.")
            || h.starts_with("urlsearchparams")
    ) {
        return true;
    }

    let receiver = hint.rsplit_once('.').map_or("", |(recv, _)| recv);
    if receiver.eq_ignore_ascii_case("this") {
        return false;
    }

    matches!(
        name,
        "map"
            | "includes"
            | "find"
            | "push"
            | "foreach"
            | "keys"
            | "replace"
            | "tolowercase"
            | "some"
            | "slice"
            | "trim"
            | "reduce"
            | "has"
            | "sort"
            | "from"
            | "split"
            | "tostring"
            | "stoppropagation"
            | "indexof"
            | "localecompare"
            | "parse"
            | "addeventlistener"
            | "removeeventlistener"
            | "touppercase"
            | "startswith"
            | "max"
            | "now"
            | "then"
            | "add"
            | "gettime"
            | "fill"
            | "flatmap"
            | "random"
            | "click"
            | "bind"
            | "entries"
            | "isarray"
    )
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
    if health.orphan_topics > 0 {
        issues.push(format!("{} orphan topic(s) remain", health.orphan_topics));
    }
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
    use std::path::PathBuf;

    use gather_step_core::{NodeId, SourceSpan};
    use gather_step_parser::resolve::CallSite;

    use super::looks_like_non_actionable_runtime_call;

    fn call_site(name: &str, hint: Option<&str>, source_path: &str) -> CallSite {
        CallSite {
            owner_id: NodeId([0; 16]),
            owner_file: NodeId([1; 16]),
            source_path: PathBuf::from(source_path),
            callee_name: name.to_owned(),
            callee_qualified_hint: hint.map(ToOwned::to_owned),
            span: Some(SourceSpan {
                line_start: 1,
                line_len: 0,
                column_start: 0,
                column_len: 1,
            }),
        }
    }

    #[test]
    fn suppresses_frontend_runtime_noise() {
        assert!(looks_like_non_actionable_runtime_call(&call_site(
            "setOpen",
            Some("setOpen"),
            "app/src/components/Modal.tsx",
        )));
        assert!(looks_like_non_actionable_runtime_call(&call_site(
            "toLowerCase",
            Some("value.toLowerCase"),
            "app/src/utils/string.ts",
        )));
        assert!(looks_like_non_actionable_runtime_call(&call_site(
            "intercept",
            Some("cy.intercept"),
            "app/cypress/support/mock.ts",
        )));
        assert!(looks_like_non_actionable_runtime_call(&call_site(
            "setTimeout",
            None,
            "app/src/views/Dashboard.tsx",
        )));
        assert!(looks_like_non_actionable_runtime_call(&call_site(
            "success",
            Some("snackbar.success"),
            "app/src/entities/details.tsx",
        )));
        assert!(looks_like_non_actionable_runtime_call(&call_site(
            "getElementById",
            Some("document.getElementById"),
            "app/src/utils/dom.ts",
        )));
        assert!(looks_like_non_actionable_runtime_call(&call_site(
            "fetchQuery",
            Some("queryClient.fetchQuery"),
            "app/src/hooks/query.ts",
        )));
        assert!(looks_like_non_actionable_runtime_call(&call_site(
            "encodeURIComponent",
            None,
            "app/src/utils/url.ts",
        )));
    }

    #[test]
    fn keeps_this_calls_and_app_symbols_actionable() {
        assert!(!looks_like_non_actionable_runtime_call(&call_site(
            "handleSortChange",
            Some("this.handleSortChange"),
            "app/src/views/containers/Dashboard.jsx",
        )));
        assert!(!looks_like_non_actionable_runtime_call(&call_site(
            "translate",
            Some("translate"),
            "app/src/entities/form/validation.ts",
        )));
    }
}
