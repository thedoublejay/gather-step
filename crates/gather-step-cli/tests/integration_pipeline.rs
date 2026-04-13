use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{self, Command},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use gather_step_mcp::{
    McpContext, McpServerConfig,
    tools::{
        contract::{ContractDriftRequest, contract_drift_tool},
        cross_repo::{CrossRepoDepsRequest, cross_repo_deps_tool},
        crud_trace::{CrudTraceRequest, crud_trace_tool},
        events::{TraceEventRequest, TraceRouteRequest, trace_event_tool, trace_route_tool},
        packs::{ContextPackRequest, context_pack_tool},
    },
};
use gather_step_storage::{IndexingOptions, RepoIndexer};
use serde_json::Value;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Scoped temp directory for fixture staging.
///
/// `Drop` panics when cleanup fails so stale state (e.g. a held file handle on
/// Windows) is surfaced instead of silently accumulating in `/tmp`.
struct TempDir {
    path: PathBuf,
    leaked: bool,
}

impl TempDir {
    fn new(name: &str) -> Self {
        let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!(
            "gather-step-integration-pipeline-{name}-{}-{id}",
            process::id()
        ));
        fs::create_dir_all(&path).expect("temp dir should exist");
        Self {
            path,
            leaked: false,
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        if self.leaked || std::thread::panicking() {
            // Best-effort: don't escalate into a double panic during unwinding.
            let _ = fs::remove_dir_all(&self.path);
            return;
        }
        fs::remove_dir_all(&self.path).unwrap_or_else(|err| {
            panic!("failed to remove temp dir {}: {err}", self.path.display())
        });
    }
}

fn gather_step() -> Command {
    Command::new(env!("CARGO_BIN_EXE_gather-step"))
}

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures")
        .canonicalize()
        .expect("fixture root should resolve")
}

fn stage_fixture_workspace(name: &str) -> TempDir {
    let temp = TempDir::new(name);
    copy_dir_all(&fixture_root(), temp.path());
    temp
}

/// Copy a directory tree. Refuses symlinks and skips the `.gather-step/`
/// generated-state directory so fixture staging is always "cold".
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

fn run_ok_json(workspace: &Path, args: &[&str]) -> Value {
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

fn run_ok_text(workspace: &Path, args: &[&str]) -> String {
    let output = gather_step()
        .arg("--workspace")
        .arg(workspace)
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

    String::from_utf8(output.stdout).expect("stdout should be valid utf8")
}

fn workspace_paths(workspace: &Path) -> (PathBuf, PathBuf, PathBuf) {
    let registry = workspace.join(".gather-step/registry.json");
    let storage = workspace.join(".gather-step/storage");
    let graph = storage.join("graph.redb");
    (registry, storage, graph)
}

fn mcp_context(workspace: &Path) -> McpContext {
    let (registry, _storage, graph) = workspace_paths(workspace);
    McpContext::open(McpServerConfig::new(registry, graph)).expect("mcp context should open")
}

#[test]
fn integration_pipeline_runs_on_committed_fixture_workspace() {
    let temp = stage_fixture_workspace("pipeline");

    let index = run_ok_json(temp.path(), &["index"]);
    assert_eq!(index["event"], "index_completed");
    assert_eq!(index["stats"]["indexed_repos"], 10);
    let cross_repo_edges = index["stats"]["cross_repo_edges"]
        .as_u64()
        .expect("cross_repo_edges should be numeric");
    assert!(cross_repo_edges > 0);
    let total_edges = index["stats"]["total_edges"]
        .as_u64()
        .expect("total_edges should be numeric");
    let total_files = index["stats"]["total_files"]
        .as_u64()
        .expect("total_files should be numeric");
    let total_symbols = index["stats"]["total_symbols"]
        .as_u64()
        .expect("total_symbols should be numeric");
    if total_edges > 0 {
        assert!(
            total_files > 0,
            "total_files must be > 0 when total_edges > 0"
        );
        assert!(
            total_symbols > 0,
            "total_symbols must be > 0 when total_edges > 0"
        );
    }

    // Split-metric fields must be present and reconcile with the total.
    let true_cross = index["stats"]["true_cross_repo_edges"]
        .as_u64()
        .expect("true_cross_repo_edges should be numeric");
    let history_own = index["stats"]["history_ownership_edges"]
        .as_u64()
        .expect("history_ownership_edges should be numeric");
    let virt_other = index["stats"]["virtual_other_cross_repo_edges"]
        .as_u64()
        .expect("virtual_other_cross_repo_edges should be numeric");
    assert_eq!(
        true_cross + history_own + virt_other,
        cross_repo_edges,
        "split metrics must sum to cross_repo_edges"
    );

    let status = run_ok_json(temp.path(), &["status"]);
    assert_eq!(status["event"], "status_completed");
    let repos = status["repos"]
        .as_array()
        .expect("repos should be an array");
    assert!(repos.iter().any(|repo| repo["repo"] == "backend_standard"));
    assert!(repos.iter().any(|repo| repo["repo"] == "frontend_standard"));
    assert!(repos.iter().any(|repo| repo["repo"] == "shared_contracts"));

    // Doctor must exit zero on the committed fixture. If this regresses, it
    // means either a legitimate indexer problem (real unresolved call or
    // broken semantic link) or a classification bug in the health report —
    // both should be investigated rather than worked around.
    let doctor = run_ok_json(temp.path(), &["doctor"]);
    assert_eq!(doctor["event"], "doctor_completed");
    assert_eq!(
        doctor["ok"], true,
        "fixture workspace should produce a clean doctor report; full output: {doctor}"
    );
    assert_eq!(doctor["issue_count"], 0);
    assert!(
        doctor["repos"]
            .as_array()
            .expect("doctor repos should be an array")
            .iter()
            .any(|repo| repo["repo"] == "backend_standard")
    );

    let trace = run_ok_json(temp.path(), &["events", "trace", "order.created"]);
    assert_eq!(trace["event"], "events_trace_completed");
    assert!(
        trace["producers"]
            .as_array()
            .expect("producers should be an array")
            .iter()
            .any(|item| item["repo"] == "backend_standard")
    );
    assert!(
        trace["consumers"]
            .as_array()
            .expect("consumers should be an array")
            .iter()
            .any(|item| item["repo"] == "frontend_standard")
    );

    let family_trace = run_ok_json(temp.path(), &["events", "trace", "order"]);
    assert_eq!(family_trace["event"], "events_trace_completed");
    assert_eq!(
        family_trace["target"]["name"],
        "__event__kafka__order.created"
    );
    // Messaging decorators (`@MessagePattern` included) emit canonical
    // `NodeKind::Event` nodes with `__event__kafka__` QNs, so `order.sync`
    // surfaces as the Event alternate — not the old Topic-kinded
    // `__topic__kafka__order.sync`.
    assert!(
        family_trace["alternates"]
            .as_array()
            .expect("alternates should be an array")
            .iter()
            .any(|item| item["name"] == "__event__kafka__order.sync")
    );

    let generate = run_ok_json(temp.path(), &["generate", "claude-md"]);
    assert_eq!(generate["event"], "generate_claude_md_completed");
    let events_rule = temp.path().join(".claude/rules/gather-step-events.md");
    let routes_rule = temp.path().join(".claude/rules/gather-step-routes.md");
    assert!(events_rule.exists());
    assert!(routes_rule.exists());

    let events_md = fs::read_to_string(&events_rule).expect("events rule should read");
    let routes_md = fs::read_to_string(&routes_rule).expect("routes rule should read");
    assert!(events_md.contains("order.created"));
    assert!(routes_md.contains("GET /orders"));

    let crud = run_ok_json(
        temp.path(),
        &["trace", "crud", "--method", "POST", "--path", "/orders"],
    );
    assert_eq!(crud["event"], "trace_crud_completed");
    assert!(
        crud["handlers"]
            .as_array()
            .expect("handlers should be an array")
            .iter()
            .any(|item| item["repo"] == "backend_standard")
    );
    assert_eq!(
        crud["callers"]
            .as_array()
            .expect("callers should be an array")
            .len(),
        1
    );
    assert!(
        crud["callers"]
            .as_array()
            .expect("callers should be an array")
            .iter()
            .all(|item| item["symbol_name"] == "createOrder")
    );
    assert!(
        crud["callers"]
            .as_array()
            .expect("callers should be an array")
            .iter()
            .any(|item| item["evidence_kind"] == "imported_constant")
    );
    assert!(
        crud["callers"]
            .as_array()
            .expect("callers should be an array")
            .iter()
            .any(|item| item["resolver"] == "frontend_constant")
    );
    assert!(
        crud["continuation"]
            .as_array()
            .expect("continuation should be an array")
            .iter()
            .any(|item| item["role"] == "entity" || item["role"] == "repository")
    );

    let crud_text = run_ok_text(
        temp.path(),
        &["trace", "crud", "--method", "POST", "--path", "/orders"],
    );
    assert!(crud_text.contains("Entities:"));
    assert!(crud_text.contains("Order"));

    let pack = run_ok_json(
        temp.path(),
        &[
            "pack",
            "listOrders",
            "--mode",
            "change_impact",
            "--limit",
            "6",
        ],
    );
    assert_eq!(pack["event"], "context_pack_completed");
    assert_eq!(pack["data"]["mode"], "change_impact");
    assert!(
        !pack["data"]["items"]
            .as_array()
            .expect("pack items should be an array")
            .is_empty()
    );
    assert!(
        pack["data"]["next_steps"]
            .as_array()
            .expect("pack next_steps should be an array")
            .iter()
            .any(|item| item == "trace_impact")
    );

    let ctx = mcp_context(temp.path());
    let event_response = trace_event_tool(
        &ctx,
        TraceEventRequest {
            budget_bytes: None,
            limit: None,
            target: "order.created".to_owned(),
        },
    )
    .expect("trace_event should succeed");
    assert_eq!(event_response.data.returned, 1);
    assert!(
        event_response.data.matches[0]
            .producers
            .iter()
            .any(|item| item.repo == "backend_standard")
    );
    assert!(
        event_response.data.matches[0]
            .consumers
            .iter()
            .any(|item| item.repo == "frontend_standard")
    );

    let pack_response = context_pack_tool(
        &ctx,
        ContextPackRequest {
            budget_bytes: None,
            depth: Some(2),
            limit: Some(6),
            repo: None,
            mode: "planning".to_owned(),
            target: "listOrders".to_owned(),
        },
    )
    .expect("context_pack should succeed");
    assert!(pack_response.data.found);
    assert!(!pack_response.data.items.is_empty());
    assert!(
        pack_response
            .data
            .next_steps
            .iter()
            .any(|step| step == "context")
    );

    let route_response = trace_route_tool(
        &ctx,
        TraceRouteRequest {
            budget_bytes: None,
            limit: None,
            method: "GET".to_owned(),
            path: "/orders".to_owned(),
        },
    )
    .expect("trace_route should succeed");
    assert!(
        route_response
            .data
            .handlers
            .iter()
            .any(|item| item.repo == "backend_standard")
    );
    assert!(
        route_response
            .data
            .callers
            .iter()
            .any(|item| item.repo == "frontend_standard")
    );

    let crud_response = crud_trace_tool(
        &ctx,
        CrudTraceRequest {
            budget_bytes: None,
            limit: None,
            method: Some("POST".to_owned()),
            path: Some("/orders".to_owned()),
            symbol_id: None,
        },
    )
    .expect("crud_trace should succeed");
    assert!(
        crud_response
            .data
            .handlers
            .iter()
            .any(|item| item.repo == "backend_standard")
    );
    assert!(
        crud_response
            .data
            .entities
            .iter()
            .any(|item| item.symbol_name == "Order")
    );
    assert!(
        crud_response
            .data
            .callers
            .iter()
            .any(|item| item.evidence_kind == "imported_constant")
    );
    assert!(
        crud_response
            .data
            .callers
            .iter()
            .any(|item| item.resolver.as_deref() == Some("frontend_constant"))
    );

    let frontend_symbol_id = crud_response
        .data
        .callers
        .iter()
        .find(|item| item.symbol_name == "createOrder")
        .map(|item| item.symbol_id.clone())
        .expect("frontend caller symbol should exist");
    let crud_by_symbol = crud_trace_tool(
        &ctx,
        CrudTraceRequest {
            budget_bytes: None,
            limit: None,
            method: None,
            path: None,
            symbol_id: Some(frontend_symbol_id),
        },
    )
    .expect("crud_trace by symbol should succeed");
    assert_eq!(crud_by_symbol.data.callers.len(), 1);
    assert_eq!(crud_by_symbol.data.callers[0].symbol_name, "createOrder");
    assert!(
        crud_by_symbol
            .data
            .handlers
            .iter()
            .any(|item| item.repo == "backend_standard")
    );

    let drift = contract_drift_tool(
        &ctx,
        ContractDriftRequest {
            budget_bytes: None,
            include_weak: None,
            target: "order.created".to_owned(),
        },
    )
    .expect("contract_drift should succeed");
    assert!(
        drift
            .data
            .drifts
            .iter()
            .any(|item| item.field_name == "orderId" && item.drift_kind == "type")
    );

    let dependencies = cross_repo_deps_tool(
        &ctx,
        CrossRepoDepsRequest {
            repo: "frontend_standard".to_owned(),
        },
    )
    .expect("cross_repo_deps should succeed");
    assert_eq!(dependencies.data.repo, "frontend_standard");
}

#[test]
fn incremental_reindex_updates_contract_drift() {
    let temp = stage_fixture_workspace("incremental");
    run_ok_json(temp.path(), &["index"]);

    let initial = contract_drift_tool(
        &mcp_context(temp.path()),
        ContractDriftRequest {
            budget_bytes: None,
            include_weak: None,
            target: "order.created".to_owned(),
        },
    )
    .expect("initial contract drift should succeed");
    assert!(
        initial
            .data
            .drifts
            .iter()
            .any(|item| item.field_name == "orderId" && item.drift_kind == "type")
    );

    fs::write(
        temp.path()
            .join("workspace/frontend_standard/src/consumer.ts"),
        r"import { EventPattern } from '@nestjs/microservices';

type OrderFeedDto = {
  orderId: string;
  email: string;
  status: string;
};

export class OrderFeedConsumer {
  @EventPattern('order.created')
  handleOrderCreated(data: OrderFeedDto) {
    return data.status;
  }
}
",
    )
    .expect("updated fixture should write");

    let (_registry, storage, _graph) = workspace_paths(temp.path());
    let indexer = RepoIndexer::open(&storage, IndexingOptions::default()).expect("indexer");
    let changed = vec!["src/consumer.ts".to_owned()];
    let incremental_started = Instant::now();
    let (delta, _stats) = indexer
        .index_repo_incremental_with_hint(
            "frontend_standard",
            temp.path().join("workspace/frontend_standard"),
            Some(&changed),
            None,
        )
        .expect("incremental reindex should succeed");
    let incremental_elapsed = incremental_started.elapsed();
    assert_eq!(delta.modified.len(), 1);
    drop(indexer);

    // performance gate: isolated local runs should stay comfortably below 500ms,
    // but the full `nextest --all-features` matrix now runs this under heavier
    // parallel fixture load. Keep a wider CI ceiling here so the gate catches
    // real regressions instead of scheduler noise from concurrent indexing and
    // oracle tests.
    assert!(
        incremental_elapsed < Duration::from_millis(2_250),
        "incremental re-index exceeded 2.25s CI ceiling: {incremental_elapsed:?}"
    );

    let updated = contract_drift_tool(
        &mcp_context(temp.path()),
        ContractDriftRequest {
            budget_bytes: None,
            include_weak: None,
            target: "order.created".to_owned(),
        },
    )
    .expect("updated contract drift should succeed");
    assert!(
        updated
            .data
            .drifts
            .iter()
            .all(|item| item.field_name != "orderId")
    );
}

/// query-latency regression gate.
///
/// Runs `trace_event` and `trace_route` against the committed fixture and
/// asserts each stays under its target. This is the CI counterpart to
/// the Criterion benches — the benches generate repeatable baselines, this
/// test fails the build when the budgets are broken.
#[test]
fn query_latency_budget() {
    let temp = stage_fixture_workspace("query-latency");
    run_ok_json(temp.path(), &["index"]);
    let ctx = mcp_context(temp.path());

    // Target: trace_event <1ms (3-hop cross-repo trace).
    // Ceiling of 50ms absorbs CI noise while still catching 10x+ regressions.
    let started = Instant::now();
    let _ = trace_event_tool(
        &ctx,
        TraceEventRequest {
            budget_bytes: None,
            limit: None,
            target: "order.created".to_owned(),
        },
    )
    .expect("trace_event should succeed");
    let trace_event_elapsed = started.elapsed();
    assert!(
        trace_event_elapsed < Duration::from_millis(50),
        "trace_event exceeded 50ms ceiling: {trace_event_elapsed:?}"
    );

    // Target: trace_route <1ms. Same ceiling.
    let started = Instant::now();
    let _ = trace_route_tool(
        &ctx,
        TraceRouteRequest {
            budget_bytes: None,
            limit: None,
            method: "GET".to_owned(),
            path: "/orders".to_owned(),
        },
    )
    .expect("trace_route should succeed");
    let trace_route_elapsed = started.elapsed();
    assert!(
        trace_route_elapsed < Duration::from_millis(50),
        "trace_route exceeded 50ms ceiling: {trace_route_elapsed:?}"
    );

    // Target: contract_drift <5ms. Ceiling of 100ms.
    let started = Instant::now();
    let _ = contract_drift_tool(
        &ctx,
        ContractDriftRequest {
            budget_bytes: None,
            include_weak: None,
            target: "order.created".to_owned(),
        },
    )
    .expect("contract_drift should succeed");
    let contract_drift_elapsed = started.elapsed();
    assert!(
        contract_drift_elapsed < Duration::from_millis(100),
        "contract_drift exceeded 100ms ceiling: {contract_drift_elapsed:?}"
    );
    drop(ctx);
}
