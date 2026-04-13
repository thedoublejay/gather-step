use std::{
    collections::BTreeSet,
    env, fs,
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use gather_step_core::high_contract::{
    HIGH_SCENARIO_CONTRACTS, MIN_HIGH_REAL_WORKSPACE_INDEXED_REPOS,
    MIN_HIGH_REAL_WORKSPACE_TOTAL_FILES,
};
use gather_step_mcp::{
    McpContext, McpServerConfig,
    tools::{
        events::{TraceEventRequest, TraceRouteRequest, trace_event_tool, trace_route_tool},
        packs::{
            ContextPackResponse, ModePackRequest, change_impact_pack_tool, debug_pack_tool,
            fix_pack_tool, planning_pack_tool, review_pack_tool,
        },
    },
};
use serde::Deserialize;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(name: &str) -> Self {
        let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!(
            "gather-step-pack-oracle-{name}-{}-{id}",
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

#[derive(Debug, Deserialize)]
struct OracleScenario {
    name: String,
    mode: String,
    #[serde(default)]
    repo: Option<String>,
    target: OracleTarget,
    oracle: OracleExpectations,
    #[serde(skip)]
    dir: PathBuf,
    /// When `true`, oracle assertions for this scenario are skipped unless the
    /// environment variable `GATHER_STEP_EXPECTED_FAIL` equals this scenario's
    /// name.  Used to track regressions that are known to fail and need a fix.
    #[serde(default)]
    expected_fail: bool,
}

#[derive(Debug, Deserialize)]
struct OracleTarget {
    kind: String,
    qn: String,
}

#[derive(Debug, Deserialize)]
struct OracleExpectations {
    expected_files: Vec<String>,
    forbidden_files: Vec<String>,
    max_follow_ups: usize,
    min_confidence: u16,
    #[serde(default)]
    required_ambiguity_codes: Vec<String>,
    #[serde(default)]
    expected_primary_symbol_name: Option<String>,
    #[serde(default)]
    expected_primary_symbol_kind: Option<String>,
    #[serde(default)]
    expected_primary_repo: Option<String>,
    #[serde(default)]
    expected_primary_file: Option<String>,
    #[serde(default)]
    expected_resolved_symbol_kind: Option<String>,
    #[serde(default)]
    expected_confirmed_downstream_repos: Vec<String>,
    #[serde(default)]
    expected_cross_repo_caller_repos: Vec<String>,
    #[serde(default)]
    forbidden_cross_repo_caller_repos: Vec<String>,
    #[serde(default)]
    forbidden_confirmed_downstream_repos: Vec<String>,
    #[serde(default)]
    max_probable_downstream_repos: Option<usize>,
    #[serde(default)]
    forbidden_warnings: Vec<String>,
    #[serde(default)]
    expected_resolution: Option<String>,
    #[serde(default)]
    expected_confidence_model_version: Option<String>,
    #[serde(default)]
    expected_impact_repos: Vec<String>,
    #[serde(default)]
    expected_primary_strategy: Option<String>,
    #[serde(default)]
    required_primary_edge_kinds: Vec<String>,
    #[serde(default)]
    forbidden_primary_edge_kinds: Vec<String>,
    max_response_bytes: usize,
    /// The `repo:file` pair that must rank as the top-1 canonical primary
    /// entry. Format: `"<repo>:<file>"`. When unset, the assert is skipped.
    #[serde(default)]
    require_top1_canonical: Option<String>,
    /// Repos that must appear in the structural evidence section.
    #[serde(default)]
    expected_structural_repos: Vec<String>,
    /// Repos that must not appear as structural primary evidence. They may
    /// still appear in advisory/co-change-only output.
    #[serde(default)]
    forbidden_advisory_in_primary: Vec<String>,
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

fn oracle_root() -> PathBuf {
    fixture_root().join("oracle")
}

fn copy_dir_all(from: &Path, to: &Path) {
    fs::create_dir_all(to).expect("destination directory should exist");
    for entry in fs::read_dir(from).expect("source directory should be readable") {
        let entry = entry.expect("directory entry should load");
        if entry.file_name() == ".gather-step" {
            continue;
        }
        let file_type = entry.file_type().expect("file type should load");
        assert!(!file_type.is_symlink(), "fixture must not contain symlinks");
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
    run_ok_json_with(workspace, &[], args)
}

fn run_ok_json_with(workspace: &Path, global_args: &[&str], args: &[&str]) -> serde_json::Value {
    let output = gather_step()
        .arg("--workspace")
        .arg(workspace)
        .args(global_args)
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

fn assert_release_gate_index_timings(index: &serde_json::Value) {
    let timings = index
        .get("timings")
        .and_then(serde_json::Value::as_object)
        .expect("release-gate index must emit timing splits");
    for key in [
        "total_wall_ms",
        "graph_build_ms",
        "parser_augment_ms",
        "pack_precompute_ms",
        "metadata_persist_ms",
        "prepare_total_ms",
        "writer_storage_commit_total_ms",
        "durable_sync_ms",
        "search_flush_ms",
        "context_pack_cache_clear_ms",
        "context_pack_cache_rows_removed",
        "precompute_ms",
    ] {
        assert!(
            timings
                .get(key)
                .and_then(serde_json::Value::as_u64)
                .is_some(),
            "release-gate index timings.{key} must be numeric"
        );
    }
}

fn mcp_context(workspace: &Path) -> McpContext {
    mcp_context_with_paths(
        workspace.join(".gather-step/registry.json"),
        workspace.join(".gather-step/storage/graph.redb"),
    )
}

fn mcp_context_with_paths(registry: PathBuf, graph: PathBuf) -> McpContext {
    McpContext::open(McpServerConfig::new(registry, graph)).expect("mcp context should open")
}

struct IsolatedIndexPaths {
    _root: TempDir,
    registry: PathBuf,
    storage: PathBuf,
}

impl IsolatedIndexPaths {
    fn new(name: &str) -> Self {
        let root = TempDir::new(name);
        let generated = root.path().join(".gather-step");
        let registry = generated.join("registry.json");
        let storage = generated.join("storage");
        fs::create_dir_all(&storage).expect("isolated storage dir should exist");
        Self {
            _root: root,
            registry,
            storage,
        }
    }

    fn global_args(&self) -> Vec<String> {
        vec![
            "--registry".to_owned(),
            self.registry.display().to_string(),
            "--storage".to_owned(),
            self.storage.display().to_string(),
        ]
    }
}

fn load_scenarios() -> Vec<OracleScenario> {
    let mut scenarios = fs::read_dir(oracle_root())
        .expect("oracle root should exist")
        .map(|entry| entry.expect("oracle entry should load").path())
        .filter(|path| path.is_dir())
        .map(|path| {
            let raw =
                fs::read_to_string(path.join("scenario.toml")).expect("scenario.toml should load");
            let mut scenario =
                toml::from_str::<OracleScenario>(&raw).expect("scenario should parse");
            scenario.dir = path;
            scenario
        })
        .collect::<Vec<_>>();
    scenarios.sort_by(|left, right| left.name.cmp(&right.name));
    scenarios
}

fn resolve_target(ctx: &McpContext, target: &OracleTarget) -> String {
    match target.kind.as_str() {
        "symbol" => target.qn.clone(),
        "route" => {
            let (method, path) = target
                .qn
                .split_once(' ')
                .expect("route qn should be `METHOD /path`");
            let response = trace_route_tool(
                ctx,
                TraceRouteRequest {
                    budget_bytes: None,
                    limit: Some(10),
                    method: method.to_owned(),
                    path: path.to_owned(),
                },
            )
            .expect("route trace should succeed");
            response
                .data
                .target_id
                .expect("route target should resolve to a target id")
        }
        "event" => {
            let response = trace_event_tool(
                ctx,
                TraceEventRequest {
                    budget_bytes: None,
                    limit: Some(10),
                    target: target.qn.clone(),
                },
            )
            .expect("event trace should succeed");
            assert_eq!(
                response.data.matches.len(),
                1,
                "event target should resolve to exactly one canonical target"
            );
            response
                .data
                .matches
                .first()
                .expect("event target should resolve")
                .target_id
                .clone()
        }
        kind => panic!("unsupported oracle target kind `{kind}`"),
    }
}

fn observed_cross_repo_caller_repos(response: &ContextPackResponse) -> BTreeSet<String> {
    response
        .data
        .change_impact
        .cross_repo_callers
        .iter()
        .map(|caller| caller.repo.clone())
        .collect()
}

fn run_pack_for_scenario(ctx: &McpContext, scenario: &OracleScenario) -> ContextPackResponse {
    let request = ModePackRequest {
        budget_bytes: Some(scenario.oracle.max_response_bytes),
        depth: Some(2),
        limit: Some(6),
        repo: scenario.repo.clone(),
        target: resolve_target(ctx, &scenario.target),
    };

    match scenario.mode.as_str() {
        "planning" => planning_pack_tool(ctx, request).expect("planning pack should succeed"),
        "debug" => debug_pack_tool(ctx, request).expect("debug pack should succeed"),
        "fix" => fix_pack_tool(ctx, request).expect("fix pack should succeed"),
        "review" => review_pack_tool(ctx, request).expect("review pack should succeed"),
        "change_impact" => {
            change_impact_pack_tool(ctx, request).expect("change impact pack should succeed")
        }
        mode => panic!("unsupported oracle mode `{mode}`"),
    }
}

fn observed_files(response: &ContextPackResponse) -> BTreeSet<String> {
    response
        .data
        .items
        .iter()
        .map(|item| item.file_path.clone())
        .collect()
}

fn observed_ambiguity_codes(response: &ContextPackResponse) -> BTreeSet<String> {
    response
        .meta
        .as_ref()
        .and_then(|meta| meta.ambiguity.as_ref())
        .map(|ambiguity| ambiguity.reason_codes.iter().cloned().collect())
        .unwrap_or_default()
}

fn observed_resolved_symbol_kind(response: &ContextPackResponse) -> Option<&str> {
    let resolved_symbol_id = response.meta.as_ref()?.resolved_symbol_id.as_deref()?;
    response
        .data
        .items
        .iter()
        .find(|item| item.symbol_id == resolved_symbol_id)
        .map(|item| item.symbol_kind.as_str())
}

fn assert_oracle_scenario(response: &ContextPackResponse, scenario: &OracleScenario) {
    let files = observed_files(response);
    let primary = response.data.items.first();
    for expected in &scenario.oracle.expected_files {
        assert!(
            files.contains(expected),
            "scenario `{}` missing expected file `{expected}`; observed={files:?}",
            scenario.name
        );
    }
    for forbidden in &scenario.oracle.forbidden_files {
        assert!(
            !files.contains(forbidden),
            "scenario `{}` unexpectedly included forbidden file `{forbidden}`",
            scenario.name
        );
    }

    assert!(
        response.data.next_steps.len() <= scenario.oracle.max_follow_ups,
        "scenario `{}` exceeded follow-up budget: {} > {}",
        scenario.name,
        response.data.next_steps.len(),
        scenario.oracle.max_follow_ups
    );

    let output_size = serde_json::to_vec(response)
        .expect("pack response should serialize")
        .len();
    assert!(
        output_size <= scenario.oracle.max_response_bytes,
        "scenario `{}` exceeded response budget: {} > {}",
        scenario.name,
        output_size,
        scenario.oracle.max_response_bytes
    );

    assert!(
        response
            .data
            .items
            .iter()
            .any(|item| item.score >= scenario.oracle.min_confidence),
        "scenario `{}` did not meet minimum score threshold {}",
        scenario.name,
        scenario.oracle.min_confidence
    );

    let observed_codes = observed_ambiguity_codes(response);
    for code in &scenario.oracle.required_ambiguity_codes {
        assert!(
            observed_codes.contains(code),
            "scenario `{}` missing ambiguity code `{code}`; observed={observed_codes:?}",
            scenario.name
        );
    }

    if let Some(expected) = &scenario.oracle.expected_primary_symbol_name {
        assert_eq!(
            primary.map(|item| item.symbol_name.as_str()),
            Some(expected.as_str()),
            "scenario `{}` primary symbol name mismatch",
            scenario.name
        );
    }
    if let Some(expected) = &scenario.oracle.expected_primary_symbol_kind {
        assert_eq!(
            primary.map(|item| item.symbol_kind.as_str()),
            Some(expected.as_str()),
            "scenario `{}` primary symbol kind mismatch",
            scenario.name
        );
    }
    if let Some(expected) = &scenario.oracle.expected_primary_repo {
        assert_eq!(
            primary.map(|item| item.repo.as_str()),
            Some(expected.as_str()),
            "scenario `{}` primary repo mismatch",
            scenario.name
        );
    }
    if let Some(expected) = &scenario.oracle.expected_primary_file {
        assert_eq!(
            primary.map(|item| item.file_path.as_str()),
            Some(expected.as_str()),
            "scenario `{}` primary file mismatch",
            scenario.name
        );
    }
    if let Some(expected) = &scenario.oracle.expected_resolved_symbol_kind {
        assert_eq!(
            observed_resolved_symbol_kind(response),
            Some(expected.as_str()),
            "scenario `{}` resolved symbol kind mismatch",
            scenario.name
        );
    }
    for expected_repo in &scenario.oracle.expected_confirmed_downstream_repos {
        assert!(
            response
                .data
                .change_impact
                .confirmed_downstream_repos
                .contains(expected_repo),
            "scenario `{}` missing confirmed downstream repo `{expected_repo}`; observed={:?}",
            scenario.name,
            response.data.change_impact.confirmed_downstream_repos
        );
    }
    let observed_callers = observed_cross_repo_caller_repos(response);
    for expected_repo in &scenario.oracle.expected_cross_repo_caller_repos {
        assert!(
            observed_callers.contains(expected_repo),
            "scenario `{}` missing cross-repo caller repo `{expected_repo}`; observed={observed_callers:?}",
            scenario.name
        );
    }
    for forbidden_repo in &scenario.oracle.forbidden_cross_repo_caller_repos {
        assert!(
            !observed_callers.contains(forbidden_repo),
            "scenario `{}` unexpectedly contained forbidden cross-repo caller repo `{forbidden_repo}`; observed={observed_callers:?}",
            scenario.name
        );
    }
    for forbidden_repo in &scenario.oracle.forbidden_confirmed_downstream_repos {
        assert!(
            !response
                .data
                .change_impact
                .confirmed_downstream_repos
                .contains(forbidden_repo),
            "scenario `{}` unexpectedly contained forbidden confirmed downstream repo `{forbidden_repo}`; observed={:?}",
            scenario.name,
            response.data.change_impact.confirmed_downstream_repos
        );
    }
    if let Some(max_probable) = scenario.oracle.max_probable_downstream_repos {
        assert!(
            response.data.change_impact.probable_downstream_repos.len() <= max_probable,
            "scenario `{}` probable downstream repo count {} exceeded max {}; observed={:?}",
            scenario.name,
            response.data.change_impact.probable_downstream_repos.len(),
            max_probable,
            response.data.change_impact.probable_downstream_repos
        );
    }
    for expected_repo in &scenario.oracle.expected_structural_repos {
        assert!(
            response
                .data
                .change_impact
                .confirmed_downstream_repos
                .contains(expected_repo),
            "scenario `{}` missing structural downstream repo `{expected_repo}`; observed={:?}",
            scenario.name,
            response.data.change_impact.confirmed_downstream_repos
        );
    }
    let structural_item_repos = response
        .data
        .items
        .iter()
        .filter(|item| item.category != "advisory_co_change_files")
        .map(|item| item.repo.clone())
        .collect::<BTreeSet<_>>();
    for forbidden_repo in &scenario.oracle.forbidden_advisory_in_primary {
        assert!(
            !structural_item_repos.contains(forbidden_repo),
            "scenario `{}` unexpectedly placed advisory repo `{forbidden_repo}` in structural pack items; observed={structural_item_repos:?}",
            scenario.name
        );
    }
    let warnings: &[String] = response
        .meta
        .as_ref()
        .map_or(&[], |meta| meta.warnings.as_slice());
    for forbidden_warning in &scenario.oracle.forbidden_warnings {
        assert!(
            !warnings
                .iter()
                .any(|warning| warning.contains(forbidden_warning)),
            "scenario `{}` unexpectedly contained warning matching `{forbidden_warning}`; observed={warnings:?}",
            scenario.name
        );
    }
    if let Some(expected) = &scenario.oracle.expected_resolution {
        assert_eq!(
            response.meta.as_ref().map(|meta| meta.resolution.as_str()),
            Some(expected.as_str()),
            "scenario `{}` resolution mismatch",
            scenario.name
        );
    }
    if let Some(expected) = &scenario.oracle.expected_confidence_model_version {
        assert_eq!(
            response
                .meta
                .as_ref()
                .and_then(|meta| meta.confidence_model_version.as_deref()),
            Some(expected.as_str()),
            "scenario `{}` confidence model version mismatch",
            scenario.name
        );
    }

    assert_eq!(
        response
            .meta
            .as_ref()
            .map(|meta| meta.response_schema_version),
        Some(gather_step_mcp::budget::response_schema_version()),
        "scenario `{}` should emit the current response schema version",
        scenario.name
    );

    if let Some(expected_canonical) = &scenario.oracle.require_top1_canonical {
        // Format: "<repo>:<file>" — split on the first colon.
        let (expected_repo, expected_file) = expected_canonical
            .split_once(':')
            .expect("require_top1_canonical must be in `repo:file` format");
        let actual_repo = primary.map_or("", |item| item.repo.as_str());
        let actual_file = primary.map_or("", |item| item.file_path.as_str());
        assert_eq!(
            actual_repo, expected_repo,
            "scenario `{}` top-1 canonical repo mismatch (require_top1_canonical)",
            scenario.name
        );
        assert_eq!(
            actual_file, expected_file,
            "scenario `{}` top-1 canonical file mismatch (require_top1_canonical)",
            scenario.name
        );
    }
}

fn primary_impact_match(json: &serde_json::Value) -> Option<&serde_json::Value> {
    let matches = json.get("matches")?.as_array()?;
    matches
        .iter()
        .find(|item| item.get("primary").and_then(serde_json::Value::as_bool) == Some(true))
        .or_else(|| matches.first())
}

fn observed_impact_repos(json: &serde_json::Value) -> BTreeSet<String> {
    let mut repos = BTreeSet::new();
    let Some(matches) = json.get("matches").and_then(serde_json::Value::as_array) else {
        return repos;
    };
    for item in matches {
        if let Some(impacted_files) = item
            .get("impacted_files")
            .and_then(serde_json::Value::as_array)
        {
            for repo in impacted_files {
                if let Some(name) = repo.get("repo").and_then(serde_json::Value::as_str) {
                    repos.insert(name.to_owned());
                }
            }
        }
        if let Some(virtual_targets) = item
            .get("virtual_targets")
            .and_then(serde_json::Value::as_array)
        {
            for target in virtual_targets {
                if let Some(target_repos) =
                    target.get("repos").and_then(serde_json::Value::as_array)
                {
                    for repo in target_repos {
                        if let Some(name) = repo.as_str() {
                            repos.insert(name.to_owned());
                        }
                    }
                }
            }
        }
    }
    repos
}

fn primary_impact_edge_kinds(json: &serde_json::Value) -> BTreeSet<String> {
    let mut kinds = BTreeSet::new();
    let Some(primary) = primary_impact_match(json) else {
        return kinds;
    };
    let Some(repos) = primary
        .get("impacted_files")
        .and_then(serde_json::Value::as_array)
    else {
        return kinds;
    };
    for repo in repos {
        let Some(files) = repo.get("files").and_then(serde_json::Value::as_array) else {
            continue;
        };
        for file in files {
            let Some(edge_kinds) = file.get("edge_kinds").and_then(serde_json::Value::as_array)
            else {
                continue;
            };
            for edge_kind in edge_kinds {
                if let Some(edge_kind) = edge_kind.as_str() {
                    kinds.insert(edge_kind.to_owned());
                }
            }
        }
    }
    kinds
}

fn primary_impact_structural_repos(json: &serde_json::Value) -> BTreeSet<String> {
    let mut repos = BTreeSet::new();
    let Some(primary) = primary_impact_match(json) else {
        return repos;
    };
    let Some(impacted_files) = primary
        .get("impacted_files")
        .and_then(serde_json::Value::as_array)
    else {
        return repos;
    };
    for repo in impacted_files {
        if let Some(name) = repo.get("repo").and_then(serde_json::Value::as_str) {
            repos.insert(name.to_owned());
        }
    }
    repos
}

fn assert_impact_scenario(json: &serde_json::Value, scenario: &OracleScenario) {
    assert_eq!(
        json.get("event").and_then(serde_json::Value::as_str),
        Some("impact_completed"),
        "scenario `{}` should emit impact_completed",
        scenario.name
    );
    let output_size = serde_json::to_vec(json)
        .expect("impact response should serialize")
        .len();
    assert!(
        output_size <= scenario.oracle.max_response_bytes,
        "scenario `{}` exceeded response budget: {} > {}",
        scenario.name,
        output_size,
        scenario.oracle.max_response_bytes
    );
    let primary =
        primary_impact_match(json).expect("impact scenario should have at least one match");
    if let Some(expected) = &scenario.oracle.expected_primary_symbol_name {
        assert_eq!(
            primary
                .get("source_symbol")
                .and_then(serde_json::Value::as_str),
            Some(expected.as_str()),
            "scenario `{}` primary impact symbol mismatch",
            scenario.name
        );
    }
    if let Some(expected) = &scenario.oracle.expected_primary_repo {
        assert_eq!(
            primary
                .get("source_repo")
                .and_then(serde_json::Value::as_str),
            Some(expected.as_str()),
            "scenario `{}` primary impact repo mismatch",
            scenario.name
        );
    }
    if let Some(expected) = &scenario.oracle.expected_primary_file {
        assert_eq!(
            primary
                .get("source_file")
                .and_then(serde_json::Value::as_str),
            Some(expected.as_str()),
            "scenario `{}` primary impact file mismatch",
            scenario.name
        );
    }
    if let Some(expected) = &scenario.oracle.expected_primary_strategy {
        assert_eq!(
            primary.get("strategy").and_then(serde_json::Value::as_str),
            Some(expected.as_str()),
            "scenario `{}` primary impact strategy mismatch",
            scenario.name
        );
    }
    let primary_edge_kinds = primary_impact_edge_kinds(json);
    for required in &scenario.oracle.required_primary_edge_kinds {
        assert!(
            primary_edge_kinds.contains(required),
            "scenario `{}` missing required primary edge kind `{required}`; observed={primary_edge_kinds:?}",
            scenario.name
        );
    }
    for forbidden in &scenario.oracle.forbidden_primary_edge_kinds {
        assert!(
            !primary_edge_kinds.contains(forbidden),
            "scenario `{}` unexpectedly contained forbidden primary edge kind `{forbidden}`; observed={primary_edge_kinds:?}",
            scenario.name
        );
    }
    let observed_repos = observed_impact_repos(json);
    for expected_repo in &scenario.oracle.expected_impact_repos {
        assert!(
            observed_repos.contains(expected_repo),
            "scenario `{}` missing impacted repo `{expected_repo}`; observed={observed_repos:?}",
            scenario.name
        );
    }
    let primary_structural_repos = primary_impact_structural_repos(json);
    for expected_repo in &scenario.oracle.expected_structural_repos {
        assert!(
            primary_structural_repos.contains(expected_repo),
            "scenario `{}` missing structural impact repo `{expected_repo}`; observed={primary_structural_repos:?}",
            scenario.name
        );
    }
    for forbidden_repo in &scenario.oracle.forbidden_advisory_in_primary {
        assert!(
            !primary_structural_repos.contains(forbidden_repo),
            "scenario `{}` unexpectedly placed advisory repo `{forbidden_repo}` in primary structural impact; observed={primary_structural_repos:?}",
            scenario.name
        );
    }
}

fn assert_json_contains_subset(
    actual: &serde_json::Value,
    expected: &serde_json::Value,
    path: &str,
) {
    match expected {
        serde_json::Value::Object(expected_map) => {
            let actual_map = actual
                .as_object()
                .unwrap_or_else(|| panic!("expected object at {path}, found {actual}"));
            for (key, expected_value) in expected_map {
                let child_path = if path == "$" {
                    format!("$.{key}")
                } else {
                    format!("{path}.{key}")
                };
                let actual_value = actual_map
                    .get(key)
                    .unwrap_or_else(|| panic!("missing key `{key}` at {path}"));
                assert_json_contains_subset(actual_value, expected_value, &child_path);
            }
        }
        serde_json::Value::Array(expected_items) => {
            let actual_items = actual
                .as_array()
                .unwrap_or_else(|| panic!("expected array at {path}, found {actual}"));
            assert!(
                actual_items.len() >= expected_items.len(),
                "array at {path} shorter than expected: {} < {}",
                actual_items.len(),
                expected_items.len()
            );
            for (index, expected_item) in expected_items.iter().enumerate() {
                let child_path = format!("{path}[{index}]");
                assert_json_contains_subset(&actual_items[index], expected_item, &child_path);
            }
        }
        _ => assert_eq!(actual, expected, "value mismatch at {path}"),
    }
}

fn maybe_assert_golden_fragment(actual: &serde_json::Value, scenario: &OracleScenario) {
    let golden_path = scenario.dir.join("golden.json");
    if !golden_path.exists() {
        return;
    }
    let raw = fs::read_to_string(&golden_path).expect("golden.json should load");
    let expected =
        serde_json::from_str::<serde_json::Value>(&raw).expect("golden.json should parse");
    assert_json_contains_subset(actual, &expected, "$");
}

fn expected_cli_wrapper(response: &ContextPackResponse) -> serde_json::Value {
    serde_json::json!({
        "event": "context_pack_completed",
        "response_schema_version": response.meta.as_ref().map_or_else(
            gather_step_mcp::budget::response_schema_version,
            |meta| meta.response_schema_version,
        ),
        "data": response.data,
        "meta": response.meta,
    })
}

fn run_cli_pack_for_scenario(workspace: &Path, scenario: &OracleScenario) -> serde_json::Value {
    let budget_bytes = scenario.oracle.max_response_bytes.to_string();
    let repo_filter = scenario.repo.as_deref();
    if scenario.mode == "impact" {
        return run_ok_json(
            workspace,
            &pack_cli_args(
                repo_filter,
                &["impact", &scenario.target.qn, "--limit", "20"],
            ),
        );
    }
    match scenario.target.kind.as_str() {
        "symbol" => run_ok_json(
            workspace,
            &pack_cli_args(
                repo_filter,
                &[
                    "pack",
                    &scenario.target.qn,
                    "--mode",
                    &scenario.mode,
                    "--budget-bytes",
                    &budget_bytes,
                ],
            ),
        ),
        "route" => {
            let (method, path) = scenario
                .target
                .qn
                .split_once(' ')
                .expect("route qn should be `METHOD /path`");
            run_ok_json(
                workspace,
                &pack_cli_args(
                    repo_filter,
                    &[
                        "pack",
                        "--mode",
                        &scenario.mode,
                        "--budget-bytes",
                        &budget_bytes,
                        "--route-method",
                        method,
                        "--route-path",
                        path,
                    ],
                ),
            )
        }
        "event" => run_ok_json(
            workspace,
            &pack_cli_args(
                repo_filter,
                &[
                    "pack",
                    "--mode",
                    &scenario.mode,
                    "--budget-bytes",
                    &budget_bytes,
                    "--event-target",
                    &scenario.target.qn,
                ],
            ),
        ),
        kind => panic!("unsupported oracle target kind `{kind}`"),
    }
}

fn pack_cli_args<'a>(repo: Option<&'a str>, args: &[&'a str]) -> Vec<&'a str> {
    let mut full = Vec::new();
    if let Some(repo) = repo {
        full.extend(["--repo", repo]);
    }
    full.extend(args.iter().copied());
    full
}

/// Return `true` when the scenario is expected to fail and the caller has NOT
/// opted in to enforcing it via `GATHER_STEP_EXPECTED_FAIL=<scenario_name>`.
///
/// A scenario with `expected_fail = true` in its TOML is skipped (oracle
/// assertions are suppressed) unless the environment variable
/// `GATHER_STEP_EXPECTED_FAIL` is set to that scenario's name.  This lets the
/// CI harness mark a known-bad heuristic without blocking the full suite, while
/// still providing a mechanism to run the failing assertion locally.
fn should_skip_expected_fail(scenario: &OracleScenario) -> bool {
    if !scenario.expected_fail {
        return false;
    }
    let enforced = std::env::var("GATHER_STEP_EXPECTED_FAIL")
        .ok()
        .is_some_and(|v| v == scenario.name);
    !enforced
}

fn run_pack_oracle_suite(workspace: &Path) {
    let index = run_ok_json(workspace, &["index"]);
    assert_eq!(index["event"], "index_completed");

    for scenario in load_scenarios() {
        // Scenarios marked `expected_fail = true` are skipped unless the
        // caller has set GATHER_STEP_EXPECTED_FAIL=<scenario_name>.
        if should_skip_expected_fail(&scenario) {
            continue;
        }

        let cli_json = run_cli_pack_for_scenario(workspace, &scenario);
        if scenario.mode == "impact" {
            assert_impact_scenario(&cli_json, &scenario);
            maybe_assert_golden_fragment(&cli_json, &scenario);
            continue;
        }

        let response = {
            let ctx = mcp_context(workspace);
            run_pack_for_scenario(&ctx, &scenario)
        };
        assert_oracle_scenario(&response, &scenario);

        let expected = expected_cli_wrapper(&response);
        assert_eq!(
            serde_json::to_vec(&cli_json).expect("cli json should serialize"),
            serde_json::to_vec(&expected).expect("expected json should serialize"),
            "CLI/MCP parity mismatch for scenario `{}`",
            scenario.name
        );
        maybe_assert_golden_fragment(&cli_json, &scenario);
    }
}

#[test]
fn pack_oracle_suite_proves_pack_quality_on_fixture_workspace() {
    let temp = stage_fixture_workspace("suite");
    run_pack_oracle_suite(temp.path());
}

#[test]
fn pack_oracle_suite_runs_under_two_minutes() {
    let temp = stage_fixture_workspace("timing");
    let started = Instant::now();
    run_pack_oracle_suite(temp.path());
    assert!(started.elapsed() < Duration::from_secs(120));
}

#[test]
#[ignore = "requires GATHER_STEP_REAL_WORKSPACE to point at a real workspace root"]
#[expect(
    clippy::too_many_lines,
    reason = "Operator-driven probe that exercises four distinct planning-quality assertions; \
              splitting them across helpers obscures the single end-to-end flow"
)]
fn pack_oracle_suite_runs_on_real_workspace() {
    let workspace_root = std::env::var("GATHER_STEP_REAL_WORKSPACE")
        .expect("GATHER_STEP_REAL_WORKSPACE must point at a real workspace root");
    let event_target = std::env::var("GATHER_STEP_REAL_EVENT_TARGET").expect(
        "GATHER_STEP_REAL_EVENT_TARGET must name a canonical event with at least one \
         in-workspace producer and one in-workspace consumer",
    );
    let hook_target = std::env::var("GATHER_STEP_REAL_HOOK_TARGET").expect(
        "GATHER_STEP_REAL_HOOK_TARGET must name a hook/session symbol with cross-repo consumers",
    );
    let impact_target = std::env::var("GATHER_STEP_REAL_IMPACT_TARGET").expect(
        "GATHER_STEP_REAL_IMPACT_TARGET must name a shared-library API consumed cross-repo",
    );
    assert_ne!(
        hook_target, impact_target,
        "real-workspace Task 1 hook/session target must be distinct from the Task 3 \
         shared-library impact target"
    );

    let workspace = PathBuf::from(workspace_root);
    let isolated = IsolatedIndexPaths::new("real-workspace");
    let global_args = isolated.global_args();
    let mut index_global_args = global_args.clone();
    let default_config = workspace.join("gather-step.full-bench.tmp.yaml");
    if default_config.exists() {
        index_global_args.push("--config".to_owned());
        index_global_args.push(default_config.display().to_string());
    }
    let index_global_arg_refs = index_global_args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let global_arg_refs = global_args.iter().map(String::as_str).collect::<Vec<_>>();

    let index = run_ok_json_with(&workspace, &index_global_arg_refs, &["index"]);
    assert_eq!(index["event"], "index_completed");
    assert_release_gate_index_timings(&index);
    let total_edges = index["stats"]["total_edges"]
        .as_u64()
        .expect("total_edges should be numeric");
    let total_files = index["stats"]["total_files"]
        .as_u64()
        .expect("total_files should be numeric");
    let total_symbols = index["stats"]["total_symbols"]
        .as_u64()
        .expect("total_symbols should be numeric");
    let indexed_repos = index["stats"]["indexed_repos"].as_u64().unwrap_or(0);
    if total_edges > 0 {
        assert!(
            total_files > 0,
            "release-gate index must report total_files > 0 when total_edges > 0"
        );
        assert!(
            total_symbols > 0,
            "release-gate index must report total_symbols > 0 when total_edges > 0"
        );
    }
    // Scope sanity: mirror the real release-gate floors so this ignored oracle
    // cannot pass on a partial two-repo workspace that release-gate would reject.
    assert!(
        indexed_repos >= MIN_HIGH_REAL_WORKSPACE_INDEXED_REPOS,
        "real-workspace probe requires indexed_repos >= {MIN_HIGH_REAL_WORKSPACE_INDEXED_REPOS} \
         to exercise cross-repo evidence; \
         indexed_repos={indexed_repos}. Did the wrong config get picked up?"
    );
    assert!(
        total_files >= MIN_HIGH_REAL_WORKSPACE_TOTAL_FILES,
        "real-workspace probe requires total_files >= {MIN_HIGH_REAL_WORKSPACE_TOTAL_FILES}; \
         total_files={total_files}. Did the wrong config get picked up?"
    );

    let status = run_ok_json_with(&workspace, &global_arg_refs, &["status"]);
    assert_eq!(status["event"], "status_completed");

    let doctor = run_ok_json_with(&workspace, &global_arg_refs, &["doctor"]);
    assert_eq!(doctor["event"], "doctor_completed");

    let ctx = mcp_context_with_paths(
        isolated.registry.clone(),
        isolated.storage.join("graph.redb"),
    );

    // ── Task 1: hook anchor must produce cross-repo planning evidence ───────
    let planning_json = run_ok_json_with(
        &workspace,
        &global_arg_refs,
        &[
            "pack",
            &hook_target,
            "--mode",
            "planning",
            "--budget-bytes",
            "18000",
        ],
    );
    assert_eq!(planning_json["event"], "context_pack_completed");
    let planning = planning_pack_tool(
        &ctx,
        ModePackRequest {
            budget_bytes: Some(18_000),
            depth: Some(2),
            limit: Some(6),
            repo: None,
            target: hook_target.clone(),
        },
    )
    .expect("real-workspace planning pack should succeed");
    assert_eq!(
        planning
            .meta
            .as_ref()
            .map(|meta| meta.response_schema_version),
        Some(2)
    );
    assert!(
        planning
            .meta
            .as_ref()
            .is_some_and(|meta| !meta.completeness.is_empty())
    );
    let confirmed_repos = planning
        .data
        .change_impact
        .confirmed_downstream_repos
        .iter()
        .filter(|repo| repo.as_str() != "frontend_standard")
        .collect::<Vec<_>>();
    let caller_repos = planning
        .data
        .change_impact
        .cross_repo_callers
        .iter()
        .map(|caller| caller.repo.as_str())
        .collect::<BTreeSet<_>>();
    assert!(
        !caller_repos.is_empty(),
        "Task 1 quality gap: planning pack for `{hook_target}` resolved a primary anchor but \
         emitted no cross_repo_callers. Cross-package hook/shared-peer consumers should surface \
         as callers without weak references being upgraded into confirmed downstream evidence. \
         confirmed_downstream_repos beyond anchor={confirmed_repos:?}"
    );

    // ── Task 2: one canonical event must surface producer and consumer ──────
    let event_trace = run_ok_json_with(
        &workspace,
        &global_arg_refs,
        &["events", "trace", &event_target, "--limit", "128"],
    );
    assert_eq!(event_trace["event"], "events_trace_completed");
    let producers = event_trace["producers"].as_array().map_or(0, Vec::len);
    let consumers = event_trace["consumers"].as_array().map_or(0, Vec::len);
    assert!(
        producers > 0 && consumers > 0,
        "Task 2 quality gap: events trace for `{event_target}` returned producers={producers} \
         consumers={consumers}. The real-workspace HIGH probe requires a single canonical event \
         that resolves both sides; set GATHER_STEP_REAL_EVENT_TARGET to a compatible event."
    );
    let event_pack = run_ok_json_with(
        &workspace,
        &global_arg_refs,
        &[
            "pack",
            "--event-target",
            &event_target,
            "--mode",
            "change_impact",
            "--budget-bytes",
            "18000",
        ],
    );
    assert_eq!(event_pack["event"], "context_pack_completed");
    let event_confirmed = event_pack["data"]["change_impact"]["confirmed_downstream_repos"]
        .as_array()
        .map_or(0, Vec::len);
    let event_proofs = event_pack["data"]["planning_proofs"]
        .as_array()
        .map_or(0, Vec::len);
    assert!(
        event_confirmed > 0 && event_proofs > 0,
        "Task 2 quality gap: event pack for `{event_target}` returned \
         confirmed_downstream_repos={event_confirmed} planning_proofs={event_proofs}. \
         The pack resolver/cache path must stay proof-backed, not just the lower-level trace."
    );

    // ── Task 3: pack and impact must agree on the canonical primary repo ────
    let pack_change_impact = run_ok_json_with(
        &workspace,
        &global_arg_refs,
        &[
            "pack",
            &impact_target,
            "--mode",
            "change_impact",
            "--budget-bytes",
            "18000",
        ],
    );
    assert_eq!(pack_change_impact["event"], "context_pack_completed");
    // Resolution lives under `meta.resolution`, not `data.resolution` —
    // see the matching fix in `pack_change_impact_check`. Reading from the
    // wrong path silently turns every alternates-only response into a pass.
    let pack_resolution = pack_change_impact["meta"]["resolution"]
        .as_str()
        .unwrap_or("");
    assert_ne!(
        pack_resolution, "search_ranked_alternates",
        "Task 3 quality gap: pack `{impact_target} --mode change_impact` returned only \
         `search_ranked_alternates`. The shared-library candidate should beat consumer-side \
         re-implementations on score; verify repo_shared_library_bonus is firing for the \
         canonical repo and that select_pack_target is reaching the medium-confidence path."
    );
    let pack_primary_repo = pack_change_impact["data"]["items"]
        .as_array()
        .and_then(|items| items.first())
        .and_then(|item| item["repo"].as_str())
        .map(str::to_owned);

    let impact = run_ok_json_with(
        &workspace,
        &global_arg_refs,
        &["impact", &impact_target, "--limit", "20"],
    );
    assert_eq!(impact["event"], "impact_completed");
    let impact_primary_repo = impact["data"]["primary"]["repo"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| impact["data"]["primary_repo"].as_str().map(str::to_owned));
    if let (Some(pack_repo), Some(impact_repo)) = (pack_primary_repo, impact_primary_repo) {
        assert_eq!(
            pack_repo, impact_repo,
            "Task 3 parity gap: pack primary repo (`{pack_repo}`) disagrees with impact \
             primary repo (`{impact_repo}`) for target `{impact_target}`. The pack and \
             impact resolvers must converge on the same canonical declaration."
        );
    }
}

/// Real-workspace producer/consumer convergence probe.
///
/// Confirms that producer and consumer for a specific event name terminate
/// on the same canonical virtual node on a real workspace, and classifies
/// any misalignment found so the operator can route it to the right fix
/// category. The probe is gated on two env vars so it can be pointed at
/// whichever real codebase the operator is validating:
///
/// - `GATHER_STEP_REAL_WORKSPACE` — workspace root (shared with the suite
///   above).
/// - `GATHER_STEP_REAL_EVENT_TARGET` — event name to probe, in the same
///   format `events trace` accepts (e.g. a dotted topic name like
///   `document.reg-genius-report-generation.queued`).
///
/// Outcome classifications (printed on failure; the test itself panics so
/// the operator sees the misalignment):
///
/// - **identity gap** — the resolver found no virtual event node for the
///   target. Parser did not extract the name on either side. Fix category:
///   parser gap (extend extraction rule) or identity normalization gap
///   (resolver didn't resolve the same string on both sides).
/// - **producer gap** — target resolved but `producers` is empty. Parser
///   extracted the consumer side but not any producer. Fix category:
///   parser gap on the producer path (e.g. new `sendMessage` shape), or
///   identity normalization gap.
/// - **consumer gap** — target resolved but `consumers` is empty. Parser
///   extracted the producer side but not any consumer. Fix category:
///   parser gap on the consumer path (e.g. decorator arg shape), or
///   identity normalization gap.
/// - **store serialization gap** — node exists but `trace_event` returns
///   unexpected `node_kind` or dangling edges. Investigate the bulk-insert
///   / canonicalize path.
///
/// The test only validates the cross-repo pair case (at least one producer
/// AND at least one consumer).
#[test]
#[ignore = "requires GATHER_STEP_REAL_WORKSPACE and GATHER_STEP_REAL_EVENT_TARGET"]
#[expect(
    clippy::print_stdout,
    reason = "operator-invoked real-workspace probe; the summary line is the payoff"
)]
fn pack_report_flow_event_probe_on_real_workspace() {
    let workspace_root = std::env::var("GATHER_STEP_REAL_WORKSPACE")
        .expect("GATHER_STEP_REAL_WORKSPACE must point at a real workspace root");
    let target_name = std::env::var("GATHER_STEP_REAL_EVENT_TARGET").expect(
        "GATHER_STEP_REAL_EVENT_TARGET must name an event to probe (e.g. the dotted topic name)",
    );
    let workspace = PathBuf::from(workspace_root);
    let isolated = IsolatedIndexPaths::new("real-event-probe");
    let global_args = isolated.global_args();
    let mut index_global_args = global_args.clone();
    let default_config = workspace.join("gather-step.full-bench.tmp.yaml");
    if default_config.exists() {
        index_global_args.push("--config".to_owned());
        index_global_args.push(default_config.display().to_string());
    }
    let index_global_arg_refs = index_global_args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();

    let index = run_ok_json_with(&workspace, &index_global_arg_refs, &["index"]);
    assert_eq!(index["event"], "index_completed");

    let ctx = mcp_context_with_paths(
        isolated.registry.clone(),
        isolated.storage.join("graph.redb"),
    );

    let trace = trace_event_tool(
        &ctx,
        TraceEventRequest {
            budget_bytes: None,
            limit: Some(128),
            target: target_name.clone(),
        },
    )
    .expect("trace_event_tool should return a response");

    assert!(
        !trace.data.matches.is_empty(),
        "identity gap: no virtual event node for `{target_name}` — parser did not extract this event name on either side. \
         Fix: extend parser rule to emit the eventType from the report-flow's producer/consumer shape, \
         or verify `resolve_topic_decorator_argument` resolves the same string on both sides."
    );

    assert_eq!(
        trace.data.matches.len(),
        1,
        "identity gap: `{target_name}` should resolve to exactly one canonical event target; observed={}",
        trace.data.matches.len()
    );
    let joined = &trace.data.matches[0];
    let producers = joined.producers.len();
    let consumers = joined.consumers.len();
    if producers == 0 || consumers == 0 {
        let class = match (producers, consumers) {
            (0, c) if c > 0 => "producer gap",
            (p, 0) if p > 0 => "consumer gap",
            _ => "store serialization gap",
        };
        panic!(
            "{class}: `{target_name}` resolved to `{target}` (kind={kind}) but the canonical node has {producers} producer(s) and {consumers} consumer(s). \
             Fix category: see the classification matrix in the test's doc comment.",
            target = joined.target_name,
            kind = joined.event_kind,
        );
    }

    println!(
        "real-workspace event probe for `{target_name}` passed: target=`{target}` kind={kind} producers={producers} consumers={consumers}",
        target = joined.target_name,
        kind = joined.event_kind,
        producers = joined.producers.len(),
        consumers = joined.consumers.len(),
    );
}

/// A snapshot of the per-scenario outputs that must be stable across
/// independent fresh-index runs (i.e., deterministic regardless of which
/// temp dir or iteration order the indexer uses).
#[derive(Debug, PartialEq, Eq)]
struct ScenarioStableFingerprint {
    name: String,
    top1_primary_repo: Option<String>,
    top1_primary_file: Option<String>,
    confirmed_downstream_repos: BTreeSet<String>,
    probable_downstream_repos_len: usize,
    cross_repo_caller_repos: BTreeSet<String>,
}

fn collect_stable_fingerprints(workspace: &Path) -> Vec<ScenarioStableFingerprint> {
    let index = run_ok_json(workspace, &["index"]);
    assert_eq!(
        index["event"], "index_completed",
        "stability-gate index must complete successfully"
    );

    // Only collect fingerprints for the three HIGH-bar scenarios. These are
    // the scenarios whose oracle assertions encode the scenario quality bar,
    // so they are the most important to verify for cross-run stability.
    let high_bar_names: BTreeSet<&str> = HIGH_SCENARIO_CONTRACTS
        .iter()
        .map(|contract| contract.scenario_name)
        .collect();

    let ctx = mcp_context(workspace);
    let mut fingerprints: Vec<ScenarioStableFingerprint> = load_scenarios()
        .into_iter()
        .filter(|scenario| high_bar_names.contains(scenario.name.as_str()))
        // Impact-mode scenarios use the CLI path, not the MCP path; skip them
        // for the MCP fingerprint collection (they have no primary item in the
        // pack response shape).
        .filter(|scenario| scenario.mode != "impact")
        .map(|scenario| {
            let response = run_pack_for_scenario(&ctx, &scenario);
            let primary = response.data.items.first();
            ScenarioStableFingerprint {
                name: scenario.name.clone(),
                top1_primary_repo: primary.map(|item| item.repo.clone()),
                top1_primary_file: primary.map(|item| item.file_path.clone()),
                confirmed_downstream_repos: response
                    .data
                    .change_impact
                    .confirmed_downstream_repos
                    .iter()
                    .cloned()
                    .collect(),
                probable_downstream_repos_len: response
                    .data
                    .change_impact
                    .probable_downstream_repos
                    .len(),
                cross_repo_caller_repos: observed_cross_repo_caller_repos(&response),
            }
        })
        .collect();

    // Sort by name so the two runs compare in a stable order regardless of
    // the filesystem iteration order in `load_scenarios`.
    fingerprints.sort_by(|a, b| a.name.cmp(&b.name));
    fingerprints
}

/// Double-run stability gate for the HIGH-bar oracle scenarios.
///
/// Stages the fixture workspace into two independent temp dirs, indexes each
/// from scratch, then compares the per-scenario fingerprints. The key
/// outputs that must be identical across both runs are:
///
/// - `top1_primary_repo` and `top1_primary_file` — the pack resolver must
///   always surface the same canonical primary declaration.
/// - `confirmed_downstream_repos` — structural downstream evidence must be
///   deterministic (no hash-order or race-condition variation).
/// - `probable_downstream_repos_len` — the count of probable hits must be
///   stable (individual repo names in this bucket are also stable, but
///   counting is sufficient to catch regressions without over-specifying
///   ordering).
/// - `cross_repo_caller_repos` — the set of repos promoted to cross-repo
///   callers via the planning-mode upstream widening must be deterministic.
///
/// Wall-time budget: the test must finish within 3 minutes. Each individual
/// index pass is already gated to 2 minutes by `pack_oracle_suite_runs_under_two_minutes`;
/// running two passes in sequence with a lighter scenario subset stays well
/// within the 3-minute envelope on the fixture workspace.
#[test]
#[expect(
    clippy::similar_names,
    reason = "run_a and run_b are intentionally parallel names for two independent index runs in a stability comparison"
)]
fn oracle_high_bar_scenarios_are_stable_across_fresh_index_runs() {
    let started = Instant::now();

    let run_a_temp = stage_fixture_workspace("stability-a");
    let run_a = collect_stable_fingerprints(run_a_temp.path());

    let run_b_temp = stage_fixture_workspace("stability-b");
    let run_b = collect_stable_fingerprints(run_b_temp.path());

    assert_eq!(
        run_a, run_b,
        "oracle output must be stable across fresh-index runs: \
         run-a={run_a:?} run-b={run_b:?}"
    );

    assert!(
        started.elapsed() < Duration::from_secs(180),
        "stability gate must finish within 3 minutes; elapsed={:?}",
        started.elapsed()
    );
}
