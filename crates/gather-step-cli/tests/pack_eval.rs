use std::{
    env, fs,
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, Ordering},
};

use gather_step_mcp::{
    McpContext, McpServerConfig, McpServerError,
    tools::{
        events::{TraceEventRequest, TraceRouteRequest, trace_event_tool, trace_route_tool},
        packs::{
            ContextPackRequest, ContextPackResponse, ModePackRequest, change_impact_pack_tool,
            context_pack_tool, debug_pack_tool, fix_pack_tool, planning_pack_tool,
            review_pack_tool,
        },
    },
};
use gather_step_storage::MetadataStoreDb;
use rusqlite::Connection;
use serde_json::to_vec;
use std::time::{Duration, Instant};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(name: &str) -> Self {
        let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!(
            "gather-step-pack-eval-{name}-{}-{id}",
            process::id()
        ));
        fs::create_dir_all(&path).expect("temp dir should exist");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn gather_step() -> process::Command {
    process::Command::new(env!("CARGO_BIN_EXE_gather-step"))
}

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures")
        .canonicalize()
        .expect("fixture root should resolve")
}

fn copy_dir_all(from: &Path, to: &Path) {
    fs::create_dir_all(to).expect("destination directory should exist");
    for entry in fs::read_dir(from).expect("source directory should be readable") {
        let entry = entry.expect("directory entry should load");
        if entry.file_name() == ".gather-step" {
            continue;
        }
        let file_type = entry.file_type().expect("file type should load");
        assert!(
            !file_type.is_symlink(),
            "fixture must not contain symlinks (found: {})",
            entry.path().display()
        );
        let from_path = entry.path();
        let to_path = to.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_all(&from_path, &to_path);
        } else {
            fs::copy(&from_path, &to_path).expect("fixture file should copy");
        }
    }
}

fn stage_fixture_workspace(name: &str) -> TempDir {
    let temp = TempDir::new(name);
    copy_dir_all(&fixture_root(), temp.path());
    temp
}

fn run_ok_json(workspace: &Path, args: &[&str]) -> serde_json::Value {
    let output = gather_step()
        .arg("--workspace")
        .arg(workspace)
        .arg("--json")
        .args(args)
        .output()
        .expect("command should run");

    assert!(
        output.status.success(),
        "command failed: {}\nstdout:\n{}\nstderr:\n{}",
        args.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    serde_json::from_slice(&output.stdout).expect("stdout should contain valid json")
}

fn mcp_context(workspace: &Path) -> McpContext {
    let registry = workspace.join(".gather-step/registry.json");
    let graph = workspace.join(".gather-step/storage/graph.redb");
    McpContext::open(McpServerConfig::new(registry, graph)).expect("mcp context should open")
}

#[derive(Debug)]
struct EvalMetrics {
    output_size: usize,
    item_count: usize,
    next_steps: usize,
    unresolved_gaps: usize,
}

fn metrics(response: &ContextPackResponse) -> EvalMetrics {
    EvalMetrics {
        output_size: to_vec(response).expect("response should serialize").len(),
        item_count: response.data.items.len(),
        next_steps: response.data.next_steps.len(),
        unresolved_gaps: response.data.unresolved_gaps.len(),
    }
}

fn count_precomputed_packs(workspace: &Path) -> usize {
    let metadata_path = workspace.join(".gather-step/storage/metadata.sqlite");
    let connection = Connection::open(metadata_path).expect("metadata sqlite should open");
    let count = connection
        .query_row("SELECT COUNT(*) FROM context_packs", [], |row| {
            row.get::<_, i64>(0)
        })
        .expect("context pack count should load");
    usize::try_from(count).expect("context pack count should fit usize")
}

fn count_precomputed_modes(workspace: &Path) -> usize {
    let metadata_path = workspace.join(".gather-step/storage/metadata.sqlite");
    let connection = Connection::open(metadata_path).expect("metadata sqlite should open");
    let count = connection
        .query_row(
            "SELECT COUNT(DISTINCT mode) FROM context_packs",
            [],
            |row| row.get::<_, i64>(0),
        )
        .expect("context pack mode count should load");
    usize::try_from(count).expect("context pack mode count should fit usize")
}

fn cache_hit_count_for_pack(
    workspace: &Path,
    mode: &str,
    target: &str,
    depth: usize,
    limit: usize,
    budget_bytes: usize,
) -> i64 {
    let metadata_path = workspace.join(".gather-step/storage/metadata.sqlite");
    let connection = Connection::open(metadata_path).expect("metadata sqlite should open");
    let pattern = format!("%:depth={depth}:limit={limit}:budget={budget_bytes}");
    let mut statement = connection
        .prepare(
            "SELECT hit_count FROM context_packs
             WHERE mode = ?1 AND target = ?2 AND pack_key LIKE ?3",
        )
        .expect("context pack hit-count query should prepare");
    let hits = statement
        .query_map([mode, target, pattern.as_str()], |row| row.get::<_, i64>(0))
        .expect("context pack hit-count query should run")
        .collect::<Result<Vec<_>, _>>()
        .expect("context pack hit-count rows should decode");
    assert_eq!(
        hits.len(),
        1,
        "expected exactly one context-pack row for mode={mode} target={target} depth={depth} limit={limit} budget={budget_bytes}; got {hits:?}"
    );
    hits[0]
}

fn assert_context_pack_generation_identity(workspace: &Path) {
    let metadata_path = workspace.join(".gather-step/storage/metadata.sqlite");
    let store = MetadataStoreDb::open(metadata_path).expect("metadata sqlite should open");
    for record in store
        .list_context_packs()
        .expect("context packs should list")
    {
        let deps = store
            .context_pack_files_for_key(&record.pack_key)
            .expect("context pack deps should load");
        for (repo, file_path) in &deps {
            assert!(
                !repo.is_empty()
                    && repo != "__virtual__"
                    && !repo.starts_with("__")
                    && !file_path.is_empty()
                    && !file_path.starts_with("__"),
                "cache deps must be real files; pack_key={} dep={repo}:{file_path}",
                record.pack_key
            );
        }
        let current_generation = store
            .latest_indexed_at_for_files(&deps)
            .expect("cache dep generation should compute");
        assert_eq!(
            current_generation, record.generation,
            "context pack generation must be computed from persisted deps; pack_key={} deps={deps:?}",
            record.pack_key
        );
    }
}

fn assert_confirmed_repos_have_planning_proofs(response: &ContextPackResponse) {
    let proof_targets = response
        .data
        .planning_proofs
        .iter()
        .filter_map(|proof| proof.get("target_repo").and_then(serde_json::Value::as_str))
        .collect::<std::collections::BTreeSet<_>>();
    for repo in &response.data.change_impact.confirmed_downstream_repos {
        assert!(
            proof_targets.contains(repo.as_str()),
            "confirmed repo `{repo}` has no planning_proofs target; proof_targets={proof_targets:?}"
        );
    }
}

fn run_pack_eval_suite(workspace: &Path) {
    let index = run_ok_json(workspace, &["index"]);
    assert_eq!(index["event"], "index_completed");
    assert!(
        count_precomputed_packs(workspace) > 0,
        "index should precompute and persist context packs"
    );
    assert_eq!(count_precomputed_modes(workspace), 5);
    assert_context_pack_generation_identity(workspace);

    let ctx = mcp_context(workspace);
    let planning = planning_pack_tool(
        &ctx,
        ModePackRequest {
            budget_bytes: Some(18_000),
            depth: Some(2),
            limit: Some(6),
            repo: None,
            target: "listOrders".to_owned(),
        },
    )
    .expect("planning pack should succeed");
    let planning_metrics = metrics(&planning);
    assert!(planning.data.found);
    assert!(planning_metrics.item_count >= 1);
    assert!(planning.data.items.len() <= 6);
    assert_eq!(planning.data.items[0].category, "target");
    assert_eq!(planning.data.items[0].symbol_name, "listOrders");
    assert_eq!(planning.data.items[0].repo, "backend_standard");
    assert!(planning_metrics.next_steps >= 2);
    assert!(planning_metrics.output_size < 18_000);
    assert_eq!(
        planning
            .meta
            .as_ref()
            .map(|meta| meta.completeness.as_str()),
        Some("partial")
    );
    assert_eq!(
        planning.meta.as_ref().map(|meta| meta.candidate_count),
        Some(1)
    );
    assert_eq!(
        planning
            .meta
            .as_ref()
            .map(|meta| meta.response_schema_version),
        Some(gather_step_mcp::budget::response_schema_version())
    );
    assert!(
        planning
            .meta
            .as_ref()
            .is_some_and(|meta| !meta.warnings.is_empty())
    );
    assert!(
        planning
            .data
            .items
            .windows(2)
            .all(|pair| pair[0].score >= pair[1].score)
    );
    let planning_cached = planning_pack_tool(
        &ctx,
        ModePackRequest {
            budget_bytes: Some(18_000),
            depth: Some(2),
            limit: Some(6),
            repo: None,
            target: "listOrders".to_owned(),
        },
    )
    .expect("cached planning pack should succeed");
    // Verify second call returns the same items — the primary cache-correctness
    // invariant.  Hit-count verification is covered by the dedicated
    // `warm_cache_hit_increments_hit_count_for_event_trace_target` test.
    assert_eq!(planning_cached.data.items, planning.data.items);

    let planning_tight_limit = planning_pack_tool(
        &ctx,
        ModePackRequest {
            budget_bytes: Some(18_000),
            depth: Some(2),
            limit: Some(1),
            repo: None,
            target: "listOrders".to_owned(),
        },
    )
    .expect("planning pack with tight limit should succeed");
    assert_eq!(planning_tight_limit.data.items.len(), 1);

    let planning_repo_scoped = planning_pack_tool(
        &ctx,
        ModePackRequest {
            budget_bytes: Some(18_000),
            depth: Some(2),
            limit: Some(6),
            repo: Some("backend_standard".to_owned()),
            target: "listOrders".to_owned(),
        },
    )
    .expect("repo-scoped planning pack should succeed");
    assert!(planning_repo_scoped.data.found);
    assert!(
        planning_repo_scoped
            .data
            .items
            .iter()
            .all(|item| item.repo == "backend_standard")
    );
    assert!(
        planning_repo_scoped
            .data
            .semantic_bridges
            .iter()
            .all(|bridge| bridge.repo == "backend_standard")
    );

    let planning_ranked_repo_scoped = planning_pack_tool(
        &ctx,
        ModePackRequest {
            budget_bytes: Some(18_000),
            depth: Some(2),
            limit: Some(6),
            repo: Some("frontend_standard".to_owned()),
            target: "useAuthentication".to_owned(),
        },
    )
    .expect("repo-scoped ranked planning pack should succeed");
    assert!(planning_ranked_repo_scoped.data.found);
    assert_eq!(
        planning_ranked_repo_scoped
            .meta
            .as_ref()
            .map(|meta| meta.resolution.as_str()),
        Some("search_ranked_resolved")
    );
    assert_eq!(
        planning_ranked_repo_scoped
            .meta
            .as_ref()
            .and_then(|meta| meta.confidence_model_version.as_deref()),
        Some("v1.0")
    );
    assert!(
        planning_ranked_repo_scoped
            .meta
            .as_ref()
            .and_then(|meta| meta.winner_margin)
            .is_some_and(|margin| margin > 0)
    );
    assert_eq!(
        planning_ranked_repo_scoped.data.items[0].symbol_name,
        "useAuthentication"
    );
    assert_eq!(
        planning_ranked_repo_scoped.data.items[0].repo,
        "frontend_standard"
    );
    assert_eq!(
        planning_ranked_repo_scoped.data.items[0].file_path,
        "src/auth_api.ts"
    );
    assert!(
        planning_ranked_repo_scoped
            .data
            .planning_rescue
            .as_ref()
            .is_some_and(|rescue| !rescue.alternate_anchors.is_empty())
    );
    assert!(
        planning_ranked_repo_scoped
            .data
            .planning_rescue
            .as_ref()
            .and_then(|rescue| rescue.alternate_anchors.first())
            .and_then(|anchor| anchor.rationale.as_deref())
            .is_some()
    );
    assert!(
        planning_ranked_repo_scoped
            .data
            .planning_rescue
            .as_ref()
            .and_then(|rescue| rescue.alternate_anchors.first())
            .and_then(|anchor| anchor.score_delta)
            .is_some_and(|delta| delta > 0)
    );

    let planning_repo_miss = planning_pack_tool(
        &ctx,
        ModePackRequest {
            budget_bytes: Some(18_000),
            depth: Some(2),
            limit: Some(6),
            repo: Some("frontend_standard".to_owned()),
            target: "listOrders".to_owned(),
        },
    )
    .expect("repo-miss planning pack should succeed");
    assert!(!planning_repo_miss.data.found);
    assert!(planning_repo_miss.data.items.is_empty());
    assert_eq!(
        planning_repo_miss
            .meta
            .as_ref()
            .map(|meta| meta.resolution.as_str()),
        Some("unresolved")
    );

    let route = trace_route_tool(
        &ctx,
        TraceRouteRequest {
            budget_bytes: None,
            limit: Some(10),
            method: "POST".to_owned(),
            path: "/orders".to_owned(),
        },
    )
    .expect("route trace should succeed");
    let route_target = route
        .data
        .target_id
        .clone()
        .expect("route should resolve to target id");
    let route_debug = debug_pack_tool(
        &ctx,
        ModePackRequest {
            budget_bytes: Some(18_000),
            depth: Some(2),
            limit: Some(6),
            repo: None,
            target: route_target,
        },
    )
    .expect("route debug pack should succeed");
    let route_metrics = metrics(&route_debug);
    assert!(route_debug.data.found);
    assert_eq!(
        route_debug
            .meta
            .as_ref()
            .map(|meta| meta.response_schema_version),
        Some(gather_step_mcp::budget::response_schema_version())
    );
    assert!(route_metrics.item_count >= 1);
    assert!(
        route_debug
            .data
            .next_steps
            .iter()
            .any(|step| step == "crud_trace")
    );
    assert!(route_metrics.output_size < 18_000);

    let event = trace_event_tool(
        &ctx,
        TraceEventRequest {
            budget_bytes: None,
            limit: Some(10),
            target: "order.created".to_owned(),
        },
    )
    .expect("event trace should succeed");
    let event_target = event.data.matches[0].target_id.clone();
    let event_debug = debug_pack_tool(
        &ctx,
        ModePackRequest {
            budget_bytes: Some(18_000),
            depth: Some(2),
            limit: Some(6),
            repo: None,
            target: event_target,
        },
    )
    .expect("event debug pack should succeed");
    let event_metrics = metrics(&event_debug);
    assert!(event_debug.data.found);
    assert_eq!(
        event_debug
            .meta
            .as_ref()
            .map(|meta| meta.response_schema_version),
        Some(gather_step_mcp::budget::response_schema_version())
    );
    assert!(event_metrics.item_count >= 1);
    assert!(
        event_debug
            .data
            .next_steps
            .iter()
            .any(|step| step == "trace_event")
    );
    assert!(event_metrics.output_size < 18_000);

    let fix = fix_pack_tool(
        &ctx,
        ModePackRequest {
            budget_bytes: Some(18_000),
            depth: Some(2),
            limit: Some(6),
            repo: None,
            target: "listOrders".to_owned(),
        },
    )
    .expect("fix pack should succeed");
    let fix_metrics = metrics(&fix);
    assert!(fix.data.found);
    assert_eq!(
        fix.meta.as_ref().map(|meta| meta.response_schema_version),
        Some(gather_step_mcp::budget::response_schema_version())
    );
    assert!(fix_metrics.item_count >= 1);
    assert!(fix.data.next_steps.iter().any(|step| step == "get_callees"));
    assert!(fix_metrics.output_size < 18_000);

    let review = review_pack_tool(
        &ctx,
        ModePackRequest {
            budget_bytes: Some(18_000),
            depth: Some(2),
            limit: Some(6),
            repo: None,
            target: "listOrders".to_owned(),
        },
    )
    .expect("review pack should succeed");
    let review_metrics = metrics(&review);
    assert!(review.data.found);
    assert_eq!(
        review
            .meta
            .as_ref()
            .map(|meta| meta.response_schema_version),
        Some(gather_step_mcp::budget::response_schema_version())
    );
    assert!(review_metrics.item_count >= 1);
    assert!(
        review
            .data
            .next_steps
            .iter()
            .any(|step| step == "trace_impact")
    );
    assert!(review_metrics.output_size < 18_000);

    let impact = change_impact_pack_tool(
        &ctx,
        ModePackRequest {
            budget_bytes: Some(18_000),
            depth: Some(2),
            limit: Some(6),
            repo: None,
            target: "listOrders".to_owned(),
        },
    )
    .expect("change impact pack should succeed");
    let impact_metrics = metrics(&impact);
    assert!(impact.data.found);
    assert_eq!(
        impact
            .meta
            .as_ref()
            .map(|meta| meta.response_schema_version),
        Some(gather_step_mcp::budget::response_schema_version())
    );
    assert!(!impact.data.change_impact.direct_repos.is_empty());
    assert!(
        impact
            .data
            .change_impact
            .downstream_repos
            .contains(&"frontend_standard".to_owned())
    );
    assert!(impact.data.change_impact.unresolved_possible.is_empty());
    assert!(impact_metrics.next_steps >= 1);
    assert!(impact_metrics.output_size < 18_000);

    let unresolved = context_pack_tool(
        &ctx,
        ContextPackRequest {
            budget_bytes: Some(64),
            depth: Some(2),
            limit: Some(6),
            repo: None,
            mode: "planning".to_owned(),
            target: "missingSymbol".to_owned(),
        },
    )
    .expect("unresolved context pack should succeed");
    assert!(!unresolved.data.found);
    assert_eq!(
        unresolved
            .meta
            .as_ref()
            .map(|meta| meta.resolution.as_str()),
        Some("unresolved")
    );
    assert_eq!(
        unresolved
            .meta
            .as_ref()
            .map(|meta| meta.budget.budget_bytes),
        Some(64)
    );
    assert_eq!(
        unresolved
            .meta
            .as_ref()
            .map(|meta| meta.completeness.as_str()),
        Some("unresolved")
    );
    assert_eq!(
        unresolved
            .meta
            .as_ref()
            .map(|meta| meta.response_schema_version),
        Some(gather_step_mcp::budget::response_schema_version())
    );
    assert!(
        unresolved
            .meta
            .as_ref()
            .is_some_and(|meta| !meta.warnings.is_empty())
    );

    let ambiguous = context_pack_tool(
        &ctx,
        ContextPackRequest {
            budget_bytes: Some(18_000),
            depth: Some(2),
            limit: Some(6),
            repo: None,
            mode: "planning".to_owned(),
            target: "order".to_owned(),
        },
    )
    .expect("ambiguous context pack should succeed");
    assert!(!ambiguous.data.found);
    assert_eq!(
        ambiguous.meta.as_ref().map(|meta| meta.resolution.as_str()),
        Some("ambiguous_event_anchor")
    );
    assert!(
        ambiguous
            .meta
            .as_ref()
            .is_some_and(|meta| meta.candidate_count > 1)
    );
    assert_eq!(
        ambiguous
            .meta
            .as_ref()
            .map(|meta| meta.completeness.as_str()),
        Some("unresolved")
    );
    assert_eq!(
        ambiguous
            .meta
            .as_ref()
            .map(|meta| meta.response_schema_version),
        Some(gather_step_mcp::budget::response_schema_version())
    );
    assert!(ambiguous.meta.as_ref().is_some_and(|meta| {
        meta.warnings
            .iter()
            .any(|warning| warning.contains("narrow the query"))
    }));

    let invalid_mode = context_pack_tool(
        &ctx,
        ContextPackRequest {
            budget_bytes: Some(18_000),
            depth: Some(2),
            limit: Some(6),
            repo: None,
            mode: "invalid".to_owned(),
            target: "listOrders".to_owned(),
        },
    )
    .expect_err("invalid mode should fail");
    assert!(matches!(invalid_mode, McpServerError::InvalidInput(_)));

    // Ambiguity / incompleteness must be surfaced explicitly instead of hidden.
    // A fully complete fixture is acceptable as long as the explicit ambiguous
    // query above still reports ambiguity rather than silently picking a target.
    assert!(
        planning_metrics.unresolved_gaps > 0
            || route_metrics.unresolved_gaps > 0
            || event_metrics.unresolved_gaps > 0
            || review_metrics.unresolved_gaps > 0
            || impact_metrics.unresolved_gaps > 0
            || ambiguous.meta.as_ref().is_some_and(|meta| {
                meta.resolution == "ambiguous_event_anchor"
                    || meta.resolution == "ambiguous_search_match"
            })
    );
}

#[test]
fn pack_eval_suite_proves_pack_quality_on_fixture_workspace() {
    let temp = stage_fixture_workspace("suite");
    run_pack_eval_suite(temp.path());
}

#[test]
fn pack_eval_suite_runs_under_two_minutes() {
    let temp = stage_fixture_workspace("timing");
    let started = Instant::now();
    run_pack_eval_suite(temp.path());
    assert!(
        started.elapsed() < Duration::from_secs(120),
        "pack eval suite took {:?}, target < 2 minutes",
        started.elapsed()
    );
}

#[test]
fn precomputed_context_pack_generations_match_persisted_dependencies() {
    let temp = stage_fixture_workspace("cache-generation-identity");
    let index = run_ok_json(temp.path(), &["index"]);
    assert_eq!(index["event"], "index_completed");

    assert_context_pack_generation_identity(temp.path());
}

/// Verifies the contract invariant for proof coverage: every proof emitted in
/// `planning_proofs` is a well-formed JSON object with required fields and a
/// valid `strength` value.
///
/// Uses `change_impact_pack_tool("SharedAuditRecord")` as the anchor because it
/// has direct cross-repo edges (`UsesTypeFrom` / `ImplementsContractFrom`) that
/// the proof builder covers — a well-defined case for structural proof coverage.
#[test]
fn planning_proofs_are_well_formed_for_shared_contract_target() {
    let temp = stage_fixture_workspace("contract-invariant");
    let index = run_ok_json(temp.path(), &["index"]);
    assert_eq!(index["event"], "index_completed");

    let ctx = mcp_context(temp.path());
    let response = change_impact_pack_tool(
        &ctx,
        ModePackRequest {
            budget_bytes: Some(32_000),
            depth: Some(2),
            limit: Some(6),
            repo: None,
            target: "SharedAuditRecord".to_owned(),
        },
    )
    .expect("change impact pack for SharedAuditRecord should succeed");

    // Every proof entry must be a JSON object with required fields.
    for proof in &response.data.planning_proofs {
        let obj = proof.as_object().expect("proof must be a JSON object");
        assert!(
            obj.contains_key("kind"),
            "proof missing 'kind' field: {proof}"
        );
        assert!(
            obj.contains_key("source_repo"),
            "proof missing 'source_repo' field: {proof}"
        );
        assert!(
            obj.contains_key("target_repo"),
            "proof missing 'target_repo' field: {proof}"
        );
        assert!(
            obj.contains_key("strength"),
            "proof missing 'strength' field: {proof}"
        );
        let strength = obj["strength"].as_u64().expect("strength must be a number");
        assert!(strength <= 100, "strength must be <= 100; got {strength}");
        let src = obj["source_repo"].as_str().unwrap_or("");
        let tgt = obj["target_repo"].as_str().unwrap_or("");
        assert_ne!(
            src, tgt,
            "proof source_repo and target_repo must differ; got src={src} tgt={tgt}"
        );
        assert!(!src.is_empty(), "proof source_repo must not be empty");
        assert!(!tgt.is_empty(), "proof target_repo must not be empty");
    }
}

/// Verifies that event-trace proof emission produces well-formed entries when
/// the pack anchor is itself a virtual event node (`order.created`).
///
/// The planning pack for a virtual event node anchor will not have proofs from
/// the standard proof builder (which skips virtual nodes), so its structural
/// downstream repos are explained by synthetic event-trace proofs emitted during
/// normal pack assembly.
#[test]
fn event_trace_proofs_are_well_formed_on_event_anchor() {
    let temp = stage_fixture_workspace("event-trace-proofs");
    let index = run_ok_json(temp.path(), &["index"]);
    assert_eq!(index["event"], "index_completed");

    let ctx = mcp_context(temp.path());

    // Resolve the virtual event node ID for 'order.created'.
    let event = trace_event_tool(
        &ctx,
        TraceEventRequest {
            budget_bytes: None,
            limit: Some(10),
            target: "order.created".to_owned(),
        },
    )
    .expect("event trace should succeed");
    let event_target_id = event
        .data
        .matches
        .first()
        .map(|m| m.target_id.clone())
        .expect("event trace should find order.created");

    // Query a planning pack using the event node as anchor.
    let response = planning_pack_tool(
        &ctx,
        ModePackRequest {
            budget_bytes: Some(32_000),
            depth: Some(2),
            limit: Some(6),
            repo: None,
            target: event_target_id,
        },
    )
    .expect("planning pack for event anchor should succeed");
    assert!(
        !response
            .data
            .change_impact
            .confirmed_downstream_repos
            .is_empty(),
        "event anchor should produce confirmed downstream repos"
    );
    assert_confirmed_repos_have_planning_proofs(&response);

    // All proof entries must be well-formed JSON objects.
    for proof in &response.data.planning_proofs {
        let obj = proof.as_object().expect("proof must be a JSON object");
        assert!(
            obj.contains_key("kind"),
            "event-anchor proof missing 'kind': {proof}"
        );
        assert!(
            obj.contains_key("source_repo"),
            "event-anchor proof missing 'source_repo': {proof}"
        );
        assert!(
            obj.contains_key("target_repo"),
            "event-anchor proof missing 'target_repo': {proof}"
        );
        let src = obj["source_repo"].as_str().unwrap_or("");
        let tgt = obj["target_repo"].as_str().unwrap_or("");
        assert_ne!(
            src, tgt,
            "proof source_repo and target_repo must differ; got src={src} tgt={tgt}"
        );
    }
}

/// Verifies that a route-anchor planning pack emits at least one
/// `RouteClientServer` proof and that every confirmed downstream repo produced
/// by that pack has a matching proof entry (derivation invariant).
///
/// Uses `POST /orders` as the anchor — the fixture has `frontend_standard/src/api.ts`
/// calling that route and `backend_standard/src/controller.ts` serving it, so
/// the route topology traversal is guaranteed to discover cross-repo participants.
#[test]
fn route_anchor_emits_route_client_server_planning_proof() {
    let temp = stage_fixture_workspace("route-proof-emission");
    let index = run_ok_json(temp.path(), &["index"]);
    assert_eq!(index["event"], "index_completed");

    let ctx = mcp_context(temp.path());

    // Resolve the route node ID for POST /orders via trace_route.
    let route = trace_route_tool(
        &ctx,
        TraceRouteRequest {
            budget_bytes: None,
            limit: Some(10),
            method: "POST".to_owned(),
            path: "/orders".to_owned(),
        },
    )
    .expect("route trace should succeed");
    let route_target_id = route
        .data
        .target_id
        .clone()
        .expect("route trace should resolve to a target_id for POST /orders");

    // Query a planning pack using the route node as anchor.
    let response = planning_pack_tool(
        &ctx,
        ModePackRequest {
            budget_bytes: Some(32_000),
            depth: Some(2),
            limit: Some(6),
            repo: None,
            target: route_target_id,
        },
    )
    .expect("planning pack for route anchor should succeed");

    assert!(
        response.data.found,
        "route-anchor planning pack must resolve successfully"
    );

    // At least one RouteClientServer proof must be present.
    let has_route_proof = response.data.planning_proofs.iter().any(|proof| {
        proof.get("kind").and_then(serde_json::Value::as_str) == Some("RouteClientServer")
    });
    assert!(
        has_route_proof,
        "planning_proofs must contain at least one RouteClientServer proof for a route anchor; \
         got proofs: {:?}",
        response
            .data
            .planning_proofs
            .iter()
            .map(|p| p.get("kind").and_then(|k| k.as_str()).unwrap_or("?"))
            .collect::<Vec<_>>()
    );

    // Every RouteClientServer proof must be well-formed.
    for proof in &response.data.planning_proofs {
        if proof.get("kind").and_then(serde_json::Value::as_str) == Some("RouteClientServer") {
            let obj = proof
                .as_object()
                .expect("RouteClientServer proof must be a JSON object");
            assert!(
                obj.contains_key("source_repo"),
                "missing source_repo: {proof}"
            );
            assert!(
                obj.contains_key("target_repo"),
                "missing target_repo: {proof}"
            );
            assert!(obj.contains_key("strength"), "missing strength: {proof}");
            let strength = obj["strength"].as_u64().unwrap_or(0);
            assert!(
                strength >= 67,
                "RouteClientServer strength must be >= 67 (structural); got {strength}"
            );
            let src = obj["source_repo"].as_str().unwrap_or("");
            let tgt = obj["target_repo"].as_str().unwrap_or("");
            assert_ne!(src, tgt, "source_repo and target_repo must differ");
            assert!(!src.is_empty(), "source_repo must not be empty");
            assert!(!tgt.is_empty(), "target_repo must not be empty");
        }
    }

    // Derivation invariant: every confirmed repo must have a proof entry.
    assert_confirmed_repos_have_planning_proofs(&response);
}

/// Verifies that a second call with identical parameters is a real warm-cache
/// hit. Response equality is not enough here: a recompute can return the same
/// payload while leaving `hit_count` unchanged.
///
/// Uses parameters that do not match the cold-workspace precompute set, so the
/// first call writes one row and the second identical call must touch it.
#[test]
fn warm_cache_hit_increments_hit_count_for_event_trace_target() {
    let temp = stage_fixture_workspace("cache-hit-event-trace");
    let index = run_ok_json(temp.path(), &["index"]);
    assert_eq!(index["event"], "index_completed");

    let ctx = mcp_context(temp.path());
    let budget_bytes = 31_000;
    let depth = 2;
    let limit = 11;
    let req = || ModePackRequest {
        budget_bytes: Some(budget_bytes),
        depth: Some(depth),
        limit: Some(limit),
        repo: None,
        target: "listOrders".to_owned(),
    };

    let first = planning_pack_tool(&ctx, req()).expect("first planning pack should succeed");
    let before = cache_hit_count_for_pack(
        temp.path(),
        "planning",
        "listOrders",
        depth,
        limit,
        budget_bytes,
    );

    let second = planning_pack_tool(&ctx, req()).expect("second planning pack should succeed");
    let after = cache_hit_count_for_pack(
        temp.path(),
        "planning",
        "listOrders",
        depth,
        limit,
        budget_bytes,
    );

    assert_eq!(
        second.data.items, first.data.items,
        "second call must return the same items as the first"
    );
    assert!(
        !second
            .data
            .change_impact
            .confirmed_downstream_repos
            .is_empty(),
        "listOrders should produce confirmed downstream repos"
    );
    assert_confirmed_repos_have_planning_proofs(&second);
    assert_eq!(
        after,
        before + 1,
        "second identical call should increment hit_count exactly once (before={before}, after={after})"
    );
    assert_context_pack_generation_identity(temp.path());
}
