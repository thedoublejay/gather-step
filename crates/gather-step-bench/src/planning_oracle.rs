#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Path, PathBuf},
    process,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use gather_step::commands::{
    impact::{self, ImpactArgs},
    projection_impact::{self, EvidenceVerbosityArg, ProjectionImpactArgs},
};
use gather_step_core::{GatherStepConfig, RegistryStore};
use gather_step_mcp::{
    McpContext, McpServerConfig,
    budget::{BudgetedTool, ResponseBudget, response_schema_version},
    tools::{
        events::{TraceEventRequest, TraceRouteRequest, trace_event_tool, trace_route_tool},
        packs::{
            ChangeImpactSummary, ContextPackData, ContextPackMeta, ContextPackResponse,
            ModePackRequest, PackItem, change_impact_pack_tool, debug_pack_tool, fix_pack_tool,
            planning_pack_tool, review_pack_tool,
        },
    },
};
use gather_step_storage::{
    IndexingOptions, StorageCoordinator, WorkspaceStores, index_workspace_with_storage,
};
use serde::{Deserialize, Serialize};

use crate::threshold::Thresholds;

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(label: &str) -> anyhow::Result<Self> {
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!(
            "gather-step-bench-planning-oracle-{label}-{}-{id}",
            process::id()
        ));
        fs::create_dir_all(&path)?;
        Ok(Self { path })
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

#[derive(Debug, Clone, Deserialize)]
pub struct OracleScenario {
    pub name: String,
    pub mode: String,
    #[serde(default)]
    pub repo: Option<String>,
    pub target: OracleTarget,
    pub oracle: OracleExpectations,
    #[serde(default)]
    pub python_oracle: Option<PythonOracleExpectations>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OracleTarget {
    pub kind: String,
    pub qn: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OracleExpectations {
    pub expected_files: Vec<String>,
    pub forbidden_files: Vec<String>,
    pub max_follow_ups: usize,
    pub min_confidence: u16,
    #[serde(default)]
    pub required_ambiguity_codes: Vec<String>,
    #[serde(default)]
    pub expected_primary_symbol_name: Option<String>,
    #[serde(default)]
    pub expected_primary_symbol_kind: Option<String>,
    #[serde(default)]
    pub expected_primary_repo: Option<String>,
    #[serde(default)]
    pub expected_primary_file: Option<String>,
    #[serde(default)]
    pub expected_resolved_symbol_kind: Option<String>,
    #[serde(default)]
    pub expected_confirmed_downstream_repos: Vec<String>,
    #[serde(default)]
    pub expected_cross_repo_caller_repos: Vec<String>,
    #[serde(default)]
    pub forbidden_cross_repo_caller_repos: Vec<String>,
    #[serde(default)]
    pub forbidden_confirmed_downstream_repos: Vec<String>,
    #[serde(default)]
    pub max_probable_downstream_repos: Option<usize>,
    #[serde(default)]
    pub forbidden_warnings: Vec<String>,
    #[serde(default)]
    pub expected_resolution: Option<String>,
    #[serde(default)]
    pub expected_confidence_model_version: Option<String>,
    #[serde(default)]
    pub expected_impact_repos: Vec<String>,
    #[serde(default)]
    pub expected_primary_strategy: Option<String>,
    #[serde(default)]
    pub required_primary_edge_kinds: Vec<String>,
    #[serde(default)]
    pub forbidden_primary_edge_kinds: Vec<String>,
    #[serde(default)]
    pub expected_structural_repos: Vec<String>,
    #[serde(default)]
    pub forbidden_advisory_in_primary: Vec<String>,
    #[serde(default)]
    pub expected_projection_resolved: Option<bool>,
    #[serde(default)]
    pub expected_projection_ambiguity: Option<String>,
    #[serde(default)]
    pub expected_projection_risks: Vec<String>,
    #[serde(default)]
    pub forbidden_projection_risks: Vec<String>,
    #[serde(default)]
    pub expected_projection_fields: Vec<String>,
    #[serde(default)]
    pub expected_source_fields: Vec<String>,
    #[serde(default)]
    pub expected_backfill_files: Vec<String>,
    #[serde(default)]
    pub expected_index_files: Vec<String>,
    #[serde(default)]
    pub forbidden_focus_only_files: Vec<String>,
    pub max_response_bytes: usize,
    /// When set, the scenario specifies which anchor (by file path) should rank
    /// first.  Used to compute `anchor_top1` across scenarios.
    #[serde(default)]
    pub expected_canonical_anchor: Option<String>,
    /// When set, asserts that the top-1 primary item resolves to this
    /// `repo:file` (or `repo:virtual_qn`) value.  Distinct from
    /// `expected_canonical_anchor` because it pins the full target identity
    /// (including a virtual-node sentinel for shared-symbol anchors) rather
    /// than just the file path.  Used as a fallback signal for
    /// `anchor_top1` when `expected_canonical_anchor` is absent: the
    /// portion after the first `:` is treated as the expected primary file.
    #[serde(default)]
    pub require_top1_canonical: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PythonOracleExpectations {
    #[serde(default)]
    pub expected_repos: Vec<String>,
    #[serde(default)]
    pub expected_bridges: Vec<String>,
    #[serde(default)]
    pub required_top_rank: Option<usize>,
    #[serde(default)]
    pub max_unresolved_gaps: Option<usize>,
    #[serde(default)]
    pub expected_resolution: Option<String>,
    #[serde(default)]
    pub expected_completeness: Option<String>,
    #[serde(default)]
    pub required_warning_substrings: Vec<String>,
}

fn is_shared_contract_impact_scenario(scenario: &OracleScenario) -> bool {
    matches!(scenario.mode.as_str(), "impact" | "change_impact") && scenario.target.kind == "symbol"
}

fn scenario_has_cross_repo_expectation(scenario: &OracleScenario) -> bool {
    !scenario.oracle.expected_cross_repo_caller_repos.is_empty()
        || !scenario
            .oracle
            .expected_confirmed_downstream_repos
            .is_empty()
        || !scenario.oracle.expected_impact_repos.is_empty()
        || !scenario.oracle.expected_structural_repos.is_empty()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "benchmark reports intentionally expose several independent boolean quality flags"
)]
pub struct PlanningOracleScenarioReport {
    pub name: String,
    pub passed: bool,
    pub latency_ms: u64,
    pub completeness: String,
    pub resolution: String,
    pub candidate_count: usize,
    pub output_bytes: usize,
    pub top1_correct: bool,
    pub top3_correct: bool,
    pub reciprocal_rank: f64,
    pub expected_file_recall: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_repo_recall: Option<f64>,
    pub forbidden_hit_count: usize,
    pub empty_result: bool,
    pub unresolved_gap_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_target_resolved: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ranking_kendall_tau: Option<f64>,
    pub findings: Vec<String>,
    /// Number of confirmed downstream repos emitted for this scenario.
    /// Used by the release-gate to check cross-repo evidence.
    #[serde(default)]
    pub confirmed_downstream_count: usize,
    /// True when this scenario expects at least one cross-repo caller repo.
    /// Derived from `expected_cross_repo_caller_repos` being non-empty.
    #[serde(default)]
    pub cross_repo_expected: bool,
    /// True when this scenario is classified as a shared-contract impact task.
    /// Set when mode == "impact", target.kind == "symbol", and the scenario
    /// carries a shared-contract pattern.
    #[serde(default)]
    pub is_shared_contract_task: bool,
    /// Fraction of this scenario's confirmed downstream repos that are backed
    /// only by advisory (co-change) proofs.  `None` when not applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub advisory_only_repo_fraction: Option<f64>,
}

/// Split retrieval-vs-planning scalar metrics.
///
/// All fields are `Option` so that historical JSON result files that pre-date
/// this struct deserialize without error; missing fields are simply `None`.
/// Fields are omitted from serialized output when `None` so that result files
/// written before this struct existed remain valid.
///
/// Advisory targets (not hard gates):
/// - `anchor_top1`:          1.0 once measurement is solid
/// - `proof_recall`:         ≥ 0.70 once real proof data exists
/// - `proof_precision`:      ≥ 0.80
/// - `structural_ratio`:     ≥ 0.67
/// - `advisory_leak_rate`:   0.0
/// - `operator_actionability`: ≥ 0.75
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplitMetrics {
    /// Fraction of scenarios where the top-1 result is the canonical anchor
    /// file specified by `expected_canonical_anchor`.  `None` when no scenario
    /// in the run declares that field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub anchor_top1: Option<f32>,
    /// Fraction of expected cross-repo proof paths actually emitted.
    /// Computed from `planning_proofs` when present; otherwise `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proof_recall: Option<f32>,
    /// Fraction of emitted proof paths that match an expected or allowed kind.
    /// Computed from `planning_proofs` when present; otherwise `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proof_precision: Option<f32>,
    /// Structural proofs divided by all proofs.
    /// Computed from `planning_proofs` when present; otherwise `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub structural_ratio: Option<f32>,
    /// Fraction of advisory proofs that appear in the primary answer.
    /// Should be 0.0.  Computed from `planning_proofs` when present; otherwise `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub advisory_leak_rate: Option<f32>,
    /// Fraction of scenarios where all expected next repos/files are present
    /// AND no forbidden file is included.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operator_actionability: Option<f32>,
    /// Maximum across shared-contract scenarios of (confirmed repos that
    /// have NO `planning_proofs` entry / total confirmed repos).  Counts
    /// unexplained risk separately from advisory leak: a confirmed repo
    /// without any proof is not "non-advisory" — it's unverified.  The
    /// release gate consults this so that deferring proof emission for
    /// some confirmed-repo paths does not silently turn into a green
    /// `no_advisory_domination` result.  `None` when no shared-contract
    /// scenarios in the run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unexplained_confirmed_repo_fraction: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanningOracleReport {
    pub passed: bool,
    pub total_scenarios: usize,
    pub passed_scenarios: usize,
    pub coverage: f64,
    pub latency_p50_ms: u64,
    pub latency_p95_ms: u64,
    pub latency_p99_ms: u64,
    pub top1_accuracy: f64,
    pub top3_accuracy: f64,
    pub mrr: f64,
    pub expected_file_recall: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_repo_recall: Option<f64>,
    pub forbidden_hit_rate: f64,
    pub empty_result_rate: f64,
    pub unresolved_gap_rate: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_resolution_success_rate: Option<f64>,
    pub stability_kendall_tau: f64,
    /// Split retrieval-vs-planning scalar metrics.  All fields are optional;
    /// see [`SplitMetrics`] for details.
    pub split: SplitMetrics,
    pub scenarios: Vec<PlanningOracleScenarioReport>,
}

#[derive(Debug, Clone)]
pub struct OracleRun {
    pub error: Option<String>,
    pub event_target_resolved: Option<bool>,
    pub impact_response: Option<serde_json::Value>,
    pub latency_ms: u64,
    pub response: Option<ContextPackResponse>,
}

#[derive(Debug, Clone)]
struct WorkspaceFileIndex {
    unique_file_to_repo: BTreeMap<String, String>,
}

pub fn load_oracle_scenarios(root: &Path) -> anyhow::Result<Vec<OracleScenario>> {
    let mut scenarios = fs::read_dir(root)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter_map(|path| {
            if path.is_dir() {
                Some(path.join("scenario.toml"))
            } else if path
                .extension()
                .is_some_and(|extension| extension == "toml")
            {
                Some(path)
            } else {
                None
            }
        })
        .map(|path| {
            let raw = fs::read_to_string(path)?;
            let scenario = toml::from_str::<OracleScenario>(&raw)?;
            Ok(scenario)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    scenarios.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(scenarios)
}

pub fn run_planning_oracle_benchmark(
    fixture_root: &Path,
    scenarios_root: &Path,
    thresholds: &Thresholds,
) -> anyhow::Result<PlanningOracleReport> {
    let scenarios = load_oracle_scenarios(scenarios_root)?;
    if scenarios.is_empty() {
        anyhow::bail!("no oracle scenarios found in {}", scenarios_root.display());
    }

    let config_path = fixture_root.join("gather-step.config.yaml");
    let config_root = if config_path.exists() {
        fixture_root.to_path_buf()
    } else {
        fixture_root
            .parent()
            .map(Path::to_path_buf)
            .filter(|parent| parent.join("gather-step.config.yaml").exists())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "failed to find gather-step.config.yaml in {} or its parent",
                    fixture_root.display()
                )
            })?
    };
    let config_path = config_root.join("gather-step.config.yaml");
    let config = GatherStepConfig::from_yaml_file(&config_path)?;
    let workspace_index = WorkspaceFileIndex::build(&config, &config_root)?;
    let primary_run = execute_oracle_run(&config, &config_root, &scenarios)?;
    let stability_run = execute_oracle_run(&config, &config_root, &scenarios)?;
    let mut stability_by_name = BTreeMap::new();
    for (scenario, run) in scenarios.iter().zip(&stability_run) {
        stability_by_name.insert(scenario.name.as_str(), run);
    }
    let mut reports = Vec::new();
    let mut latencies = Vec::new();
    for (scenario, run) in scenarios.iter().zip(&primary_run) {
        latencies.push(run.latency_ms);
        reports.push(build_scenario_report(
            &workspace_index,
            scenario,
            run,
            stability_by_name.get(scenario.name.as_str()).copied(),
        ));
    }

    latencies.sort_unstable();
    let passed_scenarios = reports.iter().filter(|report| report.passed).count();
    let coverage = ratio_usize(passed_scenarios, reports.len());
    let top1_accuracy = average_bool(reports.iter().map(|report| report.top1_correct));
    let top3_accuracy = average_bool(reports.iter().map(|report| report.top3_correct));
    let mrr = average_f64(reports.iter().map(|report| report.reciprocal_rank));
    let expected_file_recall =
        average_f64(reports.iter().map(|report| report.expected_file_recall));
    let expected_repo_recall =
        average_option_f64(reports.iter().map(|report| report.expected_repo_recall));
    let forbidden_hit_rate = ratio_usize(
        reports
            .iter()
            .filter(|report| report.forbidden_hit_count > 0)
            .count(),
        reports.len(),
    );
    let empty_result_rate = ratio_usize(
        reports.iter().filter(|report| report.empty_result).count(),
        reports.len(),
    );
    let unresolved_gap_rate = ratio_usize(
        reports
            .iter()
            .filter(|report| report.unresolved_gap_count > 0)
            .count(),
        reports.len(),
    );
    let event_resolution_success_rate =
        average_option_bool(reports.iter().map(|report| report.event_target_resolved));
    let stability_kendall_tau =
        average_option_f64(reports.iter().map(|report| report.ranking_kendall_tau)).unwrap_or(1.0);
    let split = compute_split_metrics(&scenarios, &primary_run);
    let passed = passed_scenarios == reports.len()
        && percentile(&latencies, 50.0) <= thresholds.latency.p50_ms_max
        && percentile(&latencies, 95.0) <= thresholds.latency.p95_ms_max
        && percentile(&latencies, 99.0) <= thresholds.latency.p99_ms_max
        && coverage >= thresholds.planning_oracle.coverage_min
        && top1_accuracy >= thresholds.planning_oracle.top1_accuracy_min
        && top3_accuracy >= thresholds.planning_oracle.top3_accuracy_min
        && mrr >= thresholds.planning_oracle.mrr_min
        && expected_file_recall >= thresholds.planning_oracle.expected_file_recall_min
        && expected_repo_recall
            .is_none_or(|value| value >= thresholds.planning_oracle.expected_repo_recall_min)
        && forbidden_hit_rate <= thresholds.planning_oracle.forbidden_hit_rate_max
        && empty_result_rate <= thresholds.planning_oracle.empty_result_rate_max
        && unresolved_gap_rate <= thresholds.planning_oracle.unresolved_gap_rate_max
        && event_resolution_success_rate.is_none_or(|value| {
            value >= thresholds.planning_oracle.event_resolution_success_rate_min
        })
        && stability_kendall_tau >= thresholds.planning_oracle.stability_kendall_tau_min;

    Ok(PlanningOracleReport {
        passed,
        total_scenarios: reports.len(),
        passed_scenarios,
        coverage,
        latency_p50_ms: percentile(&latencies, 50.0),
        latency_p95_ms: percentile(&latencies, 95.0),
        latency_p99_ms: percentile(&latencies, 99.0),
        top1_accuracy,
        top3_accuracy,
        mrr,
        expected_file_recall,
        expected_repo_recall,
        forbidden_hit_rate,
        empty_result_rate,
        unresolved_gap_rate,
        event_resolution_success_rate,
        stability_kendall_tau,
        split,
        scenarios: reports,
    })
}

/// Compute all six [`SplitMetrics`] scalars from the full scenario/run pairs.
///
/// Each metric is `None` when the fixture suite has no scenarios that can
/// contribute data for it (e.g. no scenario declares `expected_canonical_anchor`
/// → `anchor_top1` is `None`).
fn compute_split_metrics(scenarios: &[OracleScenario], runs: &[OracleRun]) -> SplitMetrics {
    // Accumulator state for each metric.
    let mut anchor_matches = 0usize;
    let mut anchor_total = 0usize;

    let mut recall_sum = 0.0f64;
    let mut recall_count = 0usize;

    let mut precision_sum = 0.0f64;
    let mut precision_count = 0usize;

    let mut all_proofs_total = 0usize;
    let mut all_proofs_structural = 0usize;

    let mut advisory_leak_max: Option<f64> = None;
    let mut unexplained_confirmed_max: Option<f64> = None;

    let mut actionability_sum = 0.0f64;
    let mut actionability_count = 0usize;

    for (scenario, run) in scenarios.iter().zip(runs.iter()) {
        let Some(response) = &run.response else {
            continue;
        };

        let views = parse_proofs(&response.data.planning_proofs);

        // ── anchor_top1 ──────────────────────────────────────────────────────
        // Fall back to `require_top1_canonical` when `expected_canonical_anchor`
        // is absent.  `require_top1_canonical` encodes `repo:file` (or
        // `repo:virtual_qn`); compare on the portion AFTER the first colon so
        // both schemas contribute to the metric.
        let expected_anchor_file: Option<String> = scenario
            .oracle
            .expected_canonical_anchor
            .clone()
            .or_else(|| {
                scenario
                    .oracle
                    .require_top1_canonical
                    .as_deref()
                    .and_then(|s| s.split_once(':').map(|(_, file)| file.to_owned()))
            });
        if let Some(expected_anchor) = &expected_anchor_file {
            anchor_total += 1;
            if response
                .data
                .items
                .first()
                .is_some_and(|item| item.file_path == *expected_anchor)
            {
                anchor_matches += 1;
            }
        }

        // ── proof_recall ─────────────────────────────────────────────────────
        // Expected: at least one proof targeting each repo in
        // `expected_confirmed_downstream_repos`.
        let expected_repos = &scenario.oracle.expected_confirmed_downstream_repos;
        if !expected_repos.is_empty() {
            let emitted_target_repos: BTreeSet<&str> =
                views.iter().map(|p| p.target_repo).collect();
            let matched = expected_repos
                .iter()
                .filter(|repo| emitted_target_repos.contains(repo.as_str()))
                .count();
            recall_sum += ratio_usize(matched, expected_repos.len());
            recall_count += 1;
        }

        // ── proof_precision ──────────────────────────────────────────────────
        // Allowed target repos = expected_confirmed_downstream_repos ∪
        //                        expected_cross_repo_caller_repos.
        if !views.is_empty() {
            let allowed_repos: BTreeSet<&str> = scenario
                .oracle
                .expected_confirmed_downstream_repos
                .iter()
                .chain(scenario.oracle.expected_cross_repo_caller_repos.iter())
                .chain(scenario.oracle.expected_impact_repos.iter())
                .chain(scenario.oracle.expected_structural_repos.iter())
                .map(String::as_str)
                .collect();
            let on_target = views
                .iter()
                .filter(|p| allowed_repos.contains(p.target_repo))
                .count();
            precision_sum += ratio_usize(on_target, views.len());
            precision_count += 1;
        }

        // ── structural_ratio (across all scenarios) ──────────────────────────
        all_proofs_total += views.len();
        all_proofs_structural += views
            .iter()
            .filter(|p| p.kind != "CoChangeAdvisory" && p.strength >= 67)
            .count();

        // ── advisory_leak_rate (max over shared-contract scenarios) ──────────
        // Tracks: of the confirmed repos that HAVE a proof, how many are
        // backed only by advisory edges?
        let is_shared_contract = is_shared_contract_impact_scenario(scenario);
        if is_shared_contract {
            let confirmed = &response.data.change_impact.confirmed_downstream_repos;
            let frac = advisory_only_repo_fraction(&views, confirmed);
            if let Some(f) = frac {
                advisory_leak_max = Some(advisory_leak_max.map_or(f, |prev: f64| prev.max(f)));
            }
            if scenario.mode != "impact" {
                // Separately track confirmed repos that have NO proof at all.
                // These are unverified — NOT advisory and NOT structural.  The
                // release gate must consult this metric so deferred proof
                // emission cannot trivially turn into a passing
                // `no_advisory_domination` result.
                let unexplained = unexplained_confirmed_repo_fraction(&views, confirmed);
                if let Some(f) = unexplained {
                    unexplained_confirmed_max =
                        Some(unexplained_confirmed_max.map_or(f, |prev: f64| prev.max(f)));
                }
            }
        }

        // ── operator_actionability ───────────────────────────────────────────
        // 1.0 when expected_file_recall == 1.0 AND forbidden_hit_count == 0.
        let files: BTreeSet<&str> = response
            .data
            .items
            .iter()
            .map(|item| item.file_path.as_str())
            .collect();
        let recall_perfect = scenario.oracle.expected_files.is_empty()
            || scenario
                .oracle
                .expected_files
                .iter()
                .all(|f| files.contains(f.as_str()));
        let no_forbidden = scenario
            .oracle
            .forbidden_files
            .iter()
            .all(|f| !files.contains(f.as_str()));
        actionability_sum += if recall_perfect && no_forbidden {
            1.0
        } else {
            0.0
        };
        actionability_count += 1;
    }

    // Finalise each metric.
    // All casts here intentionally narrow to f32 for the public metric type.
    // Values are ratios in [0, 1] or small benchmark counts; truncation is
    // acceptable for summary display purposes.
    #[expect(
        clippy::cast_precision_loss,
        reason = "benchmark counts are tiny; ratio narrowed to f32 for the public metric type"
    )]
    let anchor_top1 = if anchor_total > 0 {
        Some(anchor_matches as f32 / anchor_total as f32)
    } else {
        None
    };

    #[expect(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        reason = "benchmark counts are tiny; ratio narrowed to f32 for the public metric type"
    )]
    let proof_recall = if recall_count > 0 {
        Some((recall_sum / recall_count as f64) as f32)
    } else {
        None
    };

    #[expect(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        reason = "benchmark counts are tiny; ratio narrowed to f32 for the public metric type"
    )]
    let proof_precision = if precision_count > 0 {
        Some((precision_sum / precision_count as f64) as f32)
    } else {
        None
    };

    #[expect(
        clippy::cast_precision_loss,
        reason = "benchmark counts are tiny; ratio narrowed to f32 for the public metric type"
    )]
    let structural_ratio = if all_proofs_total > 0 {
        Some(all_proofs_structural as f32 / all_proofs_total as f32)
    } else {
        None
    };

    #[expect(
        clippy::cast_possible_truncation,
        reason = "advisory_leak_rate is a fraction in [0,1]; narrowing to f32 is intentional"
    )]
    let advisory_leak_rate = advisory_leak_max.map(|v| v as f32);

    #[expect(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        reason = "benchmark counts are tiny; ratio narrowed to f32 for the public metric type"
    )]
    let operator_actionability = if actionability_count > 0 {
        Some(actionability_sum as f32 / actionability_count as f32)
    } else {
        None
    };

    #[expect(
        clippy::cast_possible_truncation,
        reason = "f64 fraction is in [0.0, 1.0]; truncation to f32 is negligible for metric display"
    )]
    let unexplained_confirmed_repo_fraction = unexplained_confirmed_max.map(|f| f as f32);

    SplitMetrics {
        anchor_top1,
        proof_recall,
        proof_precision,
        structural_ratio,
        advisory_leak_rate,
        operator_actionability,
        unexplained_confirmed_repo_fraction,
    }
}

fn resolve_target(ctx: &McpContext, target: &OracleTarget) -> anyhow::Result<String> {
    match target.kind.as_str() {
        "symbol" => Ok(target.qn.clone()),
        "route" => {
            let (method, path) = target
                .qn
                .split_once(' ')
                .ok_or_else(|| anyhow::anyhow!("route qn must be `METHOD /path`"))?;
            let response = trace_route_tool(
                ctx,
                TraceRouteRequest {
                    budget_bytes: None,
                    limit: Some(10),
                    method: method.to_owned(),
                    path: path.to_owned(),
                },
            )?;
            response
                .data
                .target_id
                .ok_or_else(|| anyhow::anyhow!("route target did not resolve to a target id"))
        }
        "event" => {
            let response = trace_event_tool(
                ctx,
                TraceEventRequest {
                    budget_bytes: None,
                    limit: Some(10),
                    target: target.qn.clone(),
                },
            )?;
            anyhow::ensure!(
                response.data.matches.len() == 1,
                "event target should resolve to exactly one canonical target"
            );
            response
                .data
                .matches
                .first()
                .map(|item| item.target_id.clone())
                .ok_or_else(|| anyhow::anyhow!("event target should resolve"))
        }
        other => anyhow::bail!("unsupported oracle target kind `{other}`"),
    }
}

fn run_pack_for_scenario(
    ctx: &McpContext,
    scenario: &OracleScenario,
) -> anyhow::Result<ContextPackResponse> {
    let request = ModePackRequest {
        budget_bytes: Some(scenario.oracle.max_response_bytes),
        depth: Some(2),
        limit: Some(6),
        repo: scenario.repo.clone(),
        target: resolve_target(ctx, &scenario.target)?,
    };

    match scenario.mode.as_str() {
        "planning" => Ok(planning_pack_tool(ctx, request)?),
        "debug" => Ok(debug_pack_tool(ctx, request)?),
        "fix" => Ok(fix_pack_tool(ctx, request)?),
        "review" => Ok(review_pack_tool(ctx, request)?),
        "impact" | "change_impact" => Ok(change_impact_pack_tool(ctx, request)?),
        other => anyhow::bail!("unsupported oracle mode `{other}`"),
    }
}

fn run_impact_for_scenario(
    storage: &StorageCoordinator,
    scenario: &OracleScenario,
) -> anyhow::Result<(ContextPackResponse, serde_json::Value)> {
    let rendered = impact::execute(
        storage,
        scenario.repo.as_deref(),
        ImpactArgs {
            registry: None,
            storage: None,
            symbol: scenario.target.qn.clone(),
            limit: 20,
        },
    )?;
    let payload = rendered
        .payload
        .ok_or_else(|| anyhow::anyhow!("impact command did not return a JSON payload"))?;
    let response = impact_payload_as_context_pack_response(&payload, scenario);
    Ok((response, payload))
}

fn run_projection_impact_for_scenario(
    storage: &StorageCoordinator,
    scenario: &OracleScenario,
) -> anyhow::Result<(ContextPackResponse, serde_json::Value)> {
    anyhow::ensure!(
        scenario.target.kind == "field",
        "projection_impact oracle target kind must be `field`"
    );
    let rendered = projection_impact::execute(
        storage,
        scenario.repo.as_deref(),
        ProjectionImpactArgs {
            target: scenario.target.qn.clone(),
            limit: 20,
            evidence_verbosity: EvidenceVerbosityArg::Full,
        },
    )?;
    let payload = rendered.payload.ok_or_else(|| {
        anyhow::anyhow!("projection-impact command did not return a JSON payload")
    })?;
    let response = projection_impact_payload_as_context_pack_response(&payload, scenario);
    Ok((response, payload))
}

fn impact_payload_as_context_pack_response(
    payload: &serde_json::Value,
    scenario: &OracleScenario,
) -> ContextPackResponse {
    let matches = payload
        .get("matches")
        .and_then(serde_json::Value::as_array)
        .map_or(&[] as &[serde_json::Value], Vec::as_slice);
    let primary = primary_impact_match(payload);
    let direct_repos = primary
        .and_then(|item| item.get("source_repo"))
        .and_then(serde_json::Value::as_str)
        .map(|repo| vec![repo.to_owned()])
        .unwrap_or_default();
    let confirmed_downstream_repos = observed_impact_repos(payload)
        .into_iter()
        .collect::<Vec<_>>();
    let output_bytes = serde_json::to_vec(payload).map_or(0, |bytes| bytes.len());

    ContextPackResponse {
        data: ContextPackData {
            mode: "impact".to_owned(),
            target: scenario.target.qn.clone(),
            found: !matches.is_empty(),
            items: matches
                .iter()
                .enumerate()
                .map(|(index, item)| impact_match_as_pack_item(index, item))
                .collect(),
            semantic_bridges: Vec::new(),
            next_steps: Vec::new(),
            unresolved_gaps: Vec::new(),
            change_impact: ChangeImpactSummary {
                direct_repos,
                cross_repo_callers: Vec::new(),
                confirmed_downstream_repos: confirmed_downstream_repos.clone(),
                probable_downstream_repos: Vec::new(),
                downstream_repos: confirmed_downstream_repos,
                unresolved_possible: Vec::new(),
                truncated_repos: None,
            },
            transport_links: None,
            planning_rescue: None,
            planning_proofs: Vec::new(),
            migration_siblings: None,
        },
        meta: Some(ContextPackMeta {
            response_schema_version: response_schema_version(),
            generation: 0,
            ambiguity: None,
            budget: ResponseBudget::not_truncated(
                BudgetedTool::ChangeImpact,
                scenario.oracle.max_response_bytes,
                output_bytes,
            ),
            candidate_count: matches.len(),
            completeness: if matches.is_empty() {
                "unresolved".to_owned()
            } else {
                "complete".to_owned()
            },
            resolution: "impact".to_owned(),
            resolution_details: None,
            confidence_model_version: None,
            resolution_confidence: None,
            resolved_symbol_id: None,
            winner_margin: None,
            warnings: Vec::new(),
        }),
    }
}

fn projection_impact_payload_as_context_pack_response(
    payload: &serde_json::Value,
    scenario: &OracleScenario,
) -> ContextPackResponse {
    let candidates = payload
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .map_or(&[] as &[serde_json::Value], Vec::as_slice);
    let output_bytes = serde_json::to_vec(payload).map_or(0, |bytes| bytes.len());
    let found = payload
        .get("resolved")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);

    ContextPackResponse {
        data: ContextPackData {
            mode: "projection_impact".to_owned(),
            target: scenario.target.qn.clone(),
            found,
            items: candidates
                .iter()
                .enumerate()
                .map(|(index, item)| projection_field_as_pack_item(index, item))
                .collect(),
            semantic_bridges: Vec::new(),
            next_steps: Vec::new(),
            unresolved_gaps: unexpected_projection_missing_evidence(payload, scenario),
            change_impact: ChangeImpactSummary {
                direct_repos: Vec::new(),
                cross_repo_callers: Vec::new(),
                confirmed_downstream_repos: Vec::new(),
                probable_downstream_repos: Vec::new(),
                downstream_repos: Vec::new(),
                unresolved_possible: Vec::new(),
                truncated_repos: None,
            },
            transport_links: None,
            planning_rescue: None,
            planning_proofs: Vec::new(),
            migration_siblings: None,
        },
        meta: Some(ContextPackMeta {
            response_schema_version: response_schema_version(),
            generation: 0,
            ambiguity: None,
            budget: ResponseBudget::not_truncated(
                BudgetedTool::ChangeImpact,
                scenario.oracle.max_response_bytes,
                output_bytes,
            ),
            candidate_count: candidates.len(),
            completeness: if found {
                "complete".to_owned()
            } else {
                "unresolved".to_owned()
            },
            resolution: "projection_impact".to_owned(),
            resolution_details: None,
            confidence_model_version: None,
            resolution_confidence: payload
                .get("confidence")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned),
            resolved_symbol_id: None,
            winner_margin: None,
            warnings: Vec::new(),
        }),
    }
}

fn impact_match_as_pack_item(index: usize, item: &serde_json::Value) -> PackItem {
    let is_primary = item
        .get("primary")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let score = if is_primary {
        1000
    } else {
        900u16.saturating_sub(u16::try_from(index).unwrap_or(u16::MAX))
    };
    PackItem {
        category: if is_primary {
            "impact_primary".to_owned()
        } else {
            "impact_candidate".to_owned()
        },
        file_path: item
            .get("source_file")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        line_start: None,
        reason: item
            .get("strategy")
            .and_then(serde_json::Value::as_str)
            .map_or_else(|| "impact candidate".to_owned(), ToOwned::to_owned),
        repo: item
            .get("source_repo")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        score,
        symbol_id: format!("impact:{index}"),
        symbol_kind: "impact_match".to_owned(),
        symbol_name: item
            .get("source_symbol")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        evidence_chain: None,
    }
}

fn projection_field_as_pack_item(index: usize, item: &serde_json::Value) -> PackItem {
    PackItem {
        category: "projection_candidate".to_owned(),
        file_path: item
            .get("field_path")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        line_start: None,
        reason: "projection field candidate".to_owned(),
        repo: item
            .get("repo")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        score: 900u16.saturating_sub(u16::try_from(index).unwrap_or(u16::MAX)),
        symbol_id: format!("projection-field:{index}"),
        symbol_kind: "data_field".to_owned(),
        symbol_name: item
            .get("field_path")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        evidence_chain: None,
    }
}

fn expected_projection_missing_evidence(scenario: &OracleScenario) -> BTreeSet<&'static str> {
    scenario
        .oracle
        .expected_projection_risks
        .iter()
        .filter_map(|risk| match risk.as_str() {
            "field_candidate_not_found" => Some("data_field"),
            "projection_chain_unproven" => Some("derivation_edge"),
            "projection_writer_missing" => Some("writer"),
            "backfill_unproven" => Some("backfill"),
            "index_or_search_mapping_unproven" => Some("index_or_search_mapping"),
            _ => None,
        })
        .collect()
}

fn unexpected_projection_missing_evidence(
    payload: &serde_json::Value,
    scenario: &OracleScenario,
) -> Vec<String> {
    let expected = expected_projection_missing_evidence(scenario);
    payload
        .get("missing_evidence")
        .and_then(serde_json::Value::as_array)
        .map_or_else(Vec::new, |items| {
            items
                .iter()
                .filter_map(serde_json::Value::as_str)
                .filter(|missing| !expected.contains(missing))
                .map(ToOwned::to_owned)
                .collect()
        })
}

fn scenario_expects_empty_projection_result(
    scenario: &OracleScenario,
    response: &ContextPackResponse,
) -> bool {
    scenario.mode == "projection_impact"
        && scenario.oracle.expected_projection_resolved == Some(false)
        && !response.data.found
        && scenario
            .oracle
            .expected_projection_risks
            .iter()
            .any(|risk| risk == "field_candidate_not_found")
}

fn primary_impact_match(payload: &serde_json::Value) -> Option<&serde_json::Value> {
    let matches = payload.get("matches")?.as_array()?;
    matches
        .iter()
        .find(|item| item.get("primary").and_then(serde_json::Value::as_bool) == Some(true))
        .or_else(|| matches.first())
}

fn observed_impact_repos(payload: &serde_json::Value) -> BTreeSet<String> {
    let mut repos = BTreeSet::new();
    let Some(matches) = payload.get("matches").and_then(serde_json::Value::as_array) else {
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

fn primary_impact_edge_kinds(payload: &serde_json::Value) -> BTreeSet<String> {
    let mut kinds = BTreeSet::new();
    let Some(primary) = primary_impact_match(payload) else {
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

fn primary_impact_structural_repos(payload: &serde_json::Value) -> BTreeSet<String> {
    let mut repos = BTreeSet::new();
    let Some(primary) = primary_impact_match(payload) else {
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

/// Execute one full oracle run against a fresh index.
///
/// This is the public entry point used by the reliability check.  Each call
/// creates a new temporary storage directory, indexes the workspace from
/// scratch, and runs every scenario against that fresh index.
///
/// # In-process limitation
///
/// Any process-global registries (e.g. in-flight pack tracking backed by
/// `LazyLock<Mutex<...>>`) persist across calls within the same process.  The
/// index itself is isolated (fresh temp dir per call), but callers should be
/// aware that cached state from a prior call may influence subsequent calls.
///
/// # Errors
///
/// Returns an error when the temporary directory cannot be created or when the
/// workspace indexing step fails.
pub fn execute_oracle_run_with_config(
    config: &GatherStepConfig,
    config_root: &Path,
    scenarios: &[OracleScenario],
) -> anyhow::Result<Vec<OracleRun>> {
    execute_oracle_run(config, config_root, scenarios)
}

fn execute_oracle_run(
    config: &GatherStepConfig,
    config_root: &Path,
    scenarios: &[OracleScenario],
) -> anyhow::Result<Vec<OracleRun>> {
    let storage_dir = TempDir::new("storage")?;
    let registry_path = storage_dir.path().join("registry.json");
    let graph_path = storage_dir.path().join("graph.redb");
    let mut registry = RegistryStore::open(&registry_path)?;
    index_workspace_with_storage(
        config,
        config_root,
        &mut registry,
        storage_dir.path(),
        IndexingOptions::default(),
    )?;

    let stores = Arc::new(WorkspaceStores::open_read_only_search(storage_dir.path())?);
    let ctx = McpContext::from_workspace_stores(
        McpServerConfig::new(registry_path, graph_path),
        Arc::clone(&stores),
    );
    let storage = StorageCoordinator::from_stores(stores.as_ref().clone());
    let mut runs = Vec::with_capacity(scenarios.len());
    for scenario in scenarios {
        let started = Instant::now();
        let result = match scenario.mode.as_str() {
            "impact" => run_impact_for_scenario(&storage, scenario)
                .map(|(response, impact_response)| (response, Some(impact_response))),
            "projection_impact" => run_projection_impact_for_scenario(&storage, scenario)
                .map(|(response, impact_response)| (response, Some(impact_response))),
            _ => run_pack_for_scenario(&ctx, scenario).map(|response| (response, None)),
        };
        match result {
            Ok((response, impact_response)) => runs.push(OracleRun {
                error: None,
                event_target_resolved: (scenario.target.kind == "event").then_some(true),
                impact_response,
                latency_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                response: Some(response),
            }),
            Err(error) => runs.push(OracleRun {
                error: Some(error.to_string()),
                event_target_resolved: (scenario.target.kind == "event").then_some(false),
                impact_response: None,
                latency_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                response: None,
            }),
        }
    }
    Ok(runs)
}

fn ranking_expected_files(scenario: &OracleScenario) -> Vec<&str> {
    if scenario.oracle.expected_files.is_empty() {
        scenario
            .oracle
            .expected_primary_file
            .iter()
            .map(String::as_str)
            .collect()
    } else {
        scenario
            .oracle
            .expected_files
            .iter()
            .map(String::as_str)
            .collect()
    }
}

fn add_impact_response_findings(
    findings: &mut Vec<String>,
    payload: &serde_json::Value,
    scenario: &OracleScenario,
) {
    let Some(primary) = primary_impact_match(payload) else {
        findings.push("impact response contained no primary match".to_owned());
        return;
    };
    if let Some(expected) = &scenario.oracle.expected_primary_strategy
        && primary
            .get("strategy")
            .and_then(serde_json::Value::as_str)
            .is_none_or(|strategy| strategy != expected)
    {
        findings.push(format!(
            "primary impact strategy mismatch; expected `{expected}`"
        ));
    }

    let primary_edge_kinds = primary_impact_edge_kinds(payload);
    for required in &scenario.oracle.required_primary_edge_kinds {
        if !primary_edge_kinds.contains(required) {
            findings.push(format!("missing required primary edge kind `{required}`"));
        }
    }
    for forbidden in &scenario.oracle.forbidden_primary_edge_kinds {
        if primary_edge_kinds.contains(forbidden) {
            findings.push(format!(
                "forbidden primary edge kind `{forbidden}` was present"
            ));
        }
    }

    let primary_structural_repos = primary_impact_structural_repos(payload);
    for expected_repo in &scenario.oracle.expected_structural_repos {
        if !primary_structural_repos.contains(expected_repo) {
            findings.push(format!("missing structural impact repo `{expected_repo}`"));
        }
    }
    for forbidden_repo in &scenario.oracle.forbidden_advisory_in_primary {
        if primary_structural_repos.contains(forbidden_repo) {
            findings.push(format!(
                "advisory repo `{forbidden_repo}` appeared in primary structural impact"
            ));
        }
    }
}

fn projection_string_set(payload: &serde_json::Value, key: &str) -> BTreeSet<String> {
    payload
        .get(key)
        .and_then(serde_json::Value::as_array)
        .map_or_else(BTreeSet::new, |items| {
            items
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(ToOwned::to_owned)
                .collect()
        })
}

fn projection_field_paths(payload: &serde_json::Value, key: &str) -> BTreeSet<String> {
    payload
        .get(key)
        .and_then(serde_json::Value::as_array)
        .map_or_else(BTreeSet::new, |items| {
            items
                .iter()
                .filter_map(|item| {
                    item.get("field_path")
                        .and_then(serde_json::Value::as_str)
                        .map(ToOwned::to_owned)
                })
                .collect()
        })
}

fn projection_evidence_files(payload: &serde_json::Value, key: &str) -> BTreeSet<String> {
    payload
        .get(key)
        .and_then(serde_json::Value::as_array)
        .map_or_else(BTreeSet::new, |items| {
            items
                .iter()
                .filter_map(|item| {
                    item.get("file_path")
                        .and_then(serde_json::Value::as_str)
                        .map(ToOwned::to_owned)
                })
                .collect()
        })
}

fn add_projection_response_findings(
    findings: &mut Vec<String>,
    payload: &serde_json::Value,
    scenario: &OracleScenario,
) {
    if let Some(expected) = scenario.oracle.expected_projection_resolved {
        let observed = payload
            .get("resolved")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        if observed != expected {
            findings.push(format!(
                "projection resolved mismatch; expected `{expected}`"
            ));
        }
    }

    if let Some(expected) = &scenario.oracle.expected_projection_ambiguity {
        let observed = payload.get("ambiguity").and_then(serde_json::Value::as_str);
        if observed != Some(expected.as_str()) {
            findings.push(format!(
                "projection ambiguity mismatch; expected `{expected}`"
            ));
        }
    }

    let risks = projection_string_set(payload, "risk_hints");
    for expected in &scenario.oracle.expected_projection_risks {
        if !risks.contains(expected) {
            findings.push(format!("missing projection risk `{expected}`"));
        }
    }
    for forbidden in &scenario.oracle.forbidden_projection_risks {
        if risks.contains(forbidden) {
            findings.push(format!(
                "forbidden projection risk `{forbidden}` was present"
            ));
        }
    }

    let projected_fields = projection_field_paths(payload, "projected_fields");
    for expected in &scenario.oracle.expected_projection_fields {
        if !projected_fields.contains(expected) {
            findings.push(format!("missing projected field `{expected}`"));
        }
    }

    let source_fields = projection_field_paths(payload, "source_fields");
    for expected in &scenario.oracle.expected_source_fields {
        if !source_fields.contains(expected) {
            findings.push(format!("missing source field `{expected}`"));
        }
    }

    let backfill_files = projection_evidence_files(payload, "backfills");
    for expected in &scenario.oracle.expected_backfill_files {
        if !backfill_files.contains(expected) {
            findings.push(format!("missing projection backfill file `{expected}`"));
        }
    }

    let index_files = projection_evidence_files(payload, "indexes");
    for expected in &scenario.oracle.expected_index_files {
        if !index_files.contains(expected) {
            findings.push(format!("missing projection index file `{expected}`"));
        }
    }

    let observed_evidence_files = ["readers", "writers", "filters", "indexes", "backfills"]
        .into_iter()
        .flat_map(|key| projection_evidence_files(payload, key))
        .collect::<BTreeSet<_>>();
    for forbidden in &scenario.oracle.forbidden_focus_only_files {
        if observed_evidence_files.contains(forbidden) {
            findings.push(format!(
                "projection included forbidden focus-only file `{forbidden}`"
            ));
        }
    }
}

fn build_scenario_report(
    workspace_index: &WorkspaceFileIndex,
    scenario: &OracleScenario,
    run: &OracleRun,
    stability_run: Option<&OracleRun>,
) -> PlanningOracleScenarioReport {
    let Some(response) = &run.response else {
        return PlanningOracleScenarioReport {
            name: scenario.name.clone(),
            passed: false,
            latency_ms: run.latency_ms,
            completeness: "error".to_owned(),
            resolution: "error".to_owned(),
            candidate_count: 0,
            output_bytes: 0,
            top1_correct: false,
            top3_correct: false,
            reciprocal_rank: 0.0,
            expected_file_recall: 0.0,
            expected_repo_recall: workspace_index
                .expected_repo_recall(&scenario.oracle.expected_files, &BTreeSet::new()),
            forbidden_hit_count: 0,
            empty_result: true,
            unresolved_gap_count: 0,
            event_target_resolved: run.event_target_resolved,
            ranking_kendall_tau: stability_run.and_then(|other| {
                kendall_tau(
                    &[],
                    &other
                        .response
                        .as_ref()
                        .map_or_else(Vec::new, ranked_symbol_ids),
                )
            }),
            findings: vec![format!(
                "scenario execution failed: {}",
                run.error
                    .clone()
                    .unwrap_or_else(|| "unknown error".to_owned())
            )],
            confirmed_downstream_count: 0,
            cross_repo_expected: scenario_has_cross_repo_expectation(scenario),
            is_shared_contract_task: is_shared_contract_impact_scenario(scenario),
            advisory_only_repo_fraction: None,
        };
    };
    let files = response
        .data
        .items
        .iter()
        .map(|item| item.file_path.clone())
        .collect::<BTreeSet<_>>();
    let primary = response.data.items.first();
    let repos = response
        .data
        .items
        .iter()
        .map(|item| item.repo.clone())
        .collect::<BTreeSet<_>>();
    let ambiguity_codes = response
        .meta
        .as_ref()
        .and_then(|meta| meta.ambiguity.as_ref())
        .map(|ambiguity| {
            ambiguity
                .reason_codes
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    let output_bytes = serde_json::to_vec(response).map_or(usize::MAX, |json| json.len());
    let ranked_ids = ranked_symbol_ids(response);
    let ranking_expected_files = ranking_expected_files(scenario);
    let first_expected_rank = response
        .data
        .items
        .iter()
        .position(|item| ranking_expected_files.contains(&item.file_path.as_str()));
    let top1_correct = if ranking_expected_files.is_empty() {
        true
    } else {
        first_expected_rank == Some(0)
    };
    let top3_correct = if ranking_expected_files.is_empty() {
        true
    } else {
        first_expected_rank.is_some_and(|rank| rank < 3)
    };
    let reciprocal_rank = if ranking_expected_files.is_empty() {
        1.0
    } else {
        first_expected_rank.map_or(0.0, reciprocal_rank)
    };
    let matched_expected_files = scenario
        .oracle
        .expected_files
        .iter()
        .filter(|expected| files.contains(expected.as_str()))
        .count();
    let matched_ranking_files = ranking_expected_files
        .iter()
        .filter(|expected| files.contains(**expected))
        .count();
    let expected_file_recall = if scenario.oracle.expected_files.is_empty() {
        if ranking_expected_files.is_empty() {
            1.0
        } else {
            ratio_usize(matched_ranking_files, ranking_expected_files.len())
        }
    } else {
        ratio_usize(matched_expected_files, scenario.oracle.expected_files.len())
    };
    let forbidden_hit_count = scenario
        .oracle
        .forbidden_files
        .iter()
        .filter(|forbidden| files.contains(forbidden.as_str()))
        .count();
    let expected_repo_recall =
        workspace_index.expected_repo_recall(&scenario.oracle.expected_files, &repos);
    let mut findings = Vec::new();
    let observed_callers = response
        .data
        .change_impact
        .cross_repo_callers
        .iter()
        .map(|caller| caller.repo.clone())
        .collect::<BTreeSet<_>>();

    for expected in &scenario.oracle.expected_files {
        if !files.contains(expected) {
            findings.push(format!("missing expected file `{expected}`"));
        }
    }
    for forbidden in &scenario.oracle.forbidden_files {
        if files.contains(forbidden) {
            findings.push(format!("included forbidden file `{forbidden}`"));
        }
    }
    if response.data.next_steps.len() > scenario.oracle.max_follow_ups {
        findings.push(format!(
            "follow-ups {} exceed max {}",
            response.data.next_steps.len(),
            scenario.oracle.max_follow_ups
        ));
    }
    if !response.data.items.is_empty()
        && response
            .data
            .items
            .iter()
            .all(|item| item.score < scenario.oracle.min_confidence)
    {
        findings.push(format!(
            "no item met minimum confidence {}",
            scenario.oracle.min_confidence
        ));
    }
    for code in &scenario.oracle.required_ambiguity_codes {
        if !ambiguity_codes.contains(code) {
            findings.push(format!("missing ambiguity code `{code}`"));
        }
    }
    if output_bytes > scenario.oracle.max_response_bytes {
        findings.push(format!(
            "response size {} exceeds max {}",
            output_bytes, scenario.oracle.max_response_bytes
        ));
    }
    if let Some(expected) = &scenario.oracle.expected_primary_symbol_name
        && primary.is_none_or(|item| item.symbol_name != *expected)
    {
        findings.push(format!(
            "primary symbol name mismatch; expected `{expected}`"
        ));
    }
    if let Some(expected) = &scenario.oracle.expected_primary_symbol_kind
        && primary.is_none_or(|item| item.symbol_kind != *expected)
    {
        findings.push(format!(
            "primary symbol kind mismatch; expected `{expected}`"
        ));
    }
    if let Some(expected) = &scenario.oracle.expected_primary_repo
        && primary.is_none_or(|item| item.repo != *expected)
    {
        findings.push(format!("primary repo mismatch; expected `{expected}`"));
    }
    if let Some(expected) = &scenario.oracle.expected_primary_file
        && primary.is_none_or(|item| item.file_path != *expected)
    {
        findings.push(format!("primary file mismatch; expected `{expected}`"));
    }
    if let Some(impact_response) = &run.impact_response {
        if scenario.mode == "projection_impact" {
            add_projection_response_findings(&mut findings, impact_response, scenario);
        } else {
            add_impact_response_findings(&mut findings, impact_response, scenario);
        }
    }
    if let Some(expected) = &scenario.oracle.expected_resolved_symbol_kind
        && observed_resolved_symbol_kind(response).is_none_or(|kind| kind != expected.as_str())
    {
        findings.push(format!(
            "resolved symbol kind mismatch; expected `{expected}`"
        ));
    }
    for expected_repo in &scenario.oracle.expected_confirmed_downstream_repos {
        if !response
            .data
            .change_impact
            .confirmed_downstream_repos
            .contains(expected_repo)
        {
            findings.push(format!(
                "missing confirmed downstream repo `{expected_repo}`"
            ));
        }
    }
    for expected_repo in &scenario.oracle.expected_impact_repos {
        if !response
            .data
            .change_impact
            .confirmed_downstream_repos
            .contains(expected_repo)
        {
            findings.push(format!("missing impacted repo `{expected_repo}`"));
        }
    }
    for expected_repo in &scenario.oracle.expected_structural_repos {
        if !response
            .data
            .change_impact
            .confirmed_downstream_repos
            .contains(expected_repo)
        {
            findings.push(format!(
                "missing structural downstream repo `{expected_repo}`"
            ));
        }
    }
    let structural_item_repos = response
        .data
        .items
        .iter()
        .filter(|item| item.category != "advisory_co_change_files")
        .map(|item| item.repo.clone())
        .collect::<BTreeSet<_>>();
    for forbidden_repo in &scenario.oracle.forbidden_advisory_in_primary {
        if structural_item_repos.contains(forbidden_repo) {
            findings.push(format!(
                "advisory repo `{forbidden_repo}` appeared in structural pack items"
            ));
        }
    }
    for expected_repo in &scenario.oracle.expected_cross_repo_caller_repos {
        if !observed_callers.contains(expected_repo) {
            findings.push(format!("missing cross-repo caller repo `{expected_repo}`"));
        }
    }
    for forbidden_repo in &scenario.oracle.forbidden_cross_repo_caller_repos {
        if observed_callers.contains(forbidden_repo) {
            findings.push(format!(
                "forbidden cross-repo caller repo `{forbidden_repo}` was present"
            ));
        }
    }
    for forbidden_repo in &scenario.oracle.forbidden_confirmed_downstream_repos {
        if response
            .data
            .change_impact
            .confirmed_downstream_repos
            .contains(forbidden_repo)
        {
            findings.push(format!(
                "forbidden confirmed downstream repo `{forbidden_repo}` was present"
            ));
        }
    }
    if let Some(max_probable) = scenario.oracle.max_probable_downstream_repos {
        let probable_count = response.data.change_impact.probable_downstream_repos.len();
        if probable_count > max_probable {
            findings.push(format!(
                "probable downstream repo count {probable_count} exceeded max {max_probable}"
            ));
        }
    }
    let warnings: &[String] = response
        .meta
        .as_ref()
        .map_or(&[], |meta| meta.warnings.as_slice());
    for forbidden_warning in &scenario.oracle.forbidden_warnings {
        if warnings
            .iter()
            .any(|warning| warning.contains(forbidden_warning))
        {
            findings.push(format!(
                "forbidden warning `{forbidden_warning}` was present"
            ));
        }
    }
    if let Some(expected) = &scenario.oracle.expected_resolution
        && response
            .meta
            .as_ref()
            .is_none_or(|meta| meta.resolution != *expected)
    {
        findings.push(format!("resolution mismatch; expected `{expected}`"));
    }
    if let Some(expected) = &scenario.oracle.expected_confidence_model_version
        && response
            .meta
            .as_ref()
            .and_then(|meta| meta.confidence_model_version.as_deref())
            != Some(expected.as_str())
    {
        findings.push(format!(
            "confidence model version mismatch; expected `{expected}`"
        ));
    }
    if let Some(python_oracle) = &scenario.python_oracle {
        add_python_oracle_findings(
            &mut findings,
            python_oracle,
            response,
            first_expected_rank,
            warnings,
        );
    }
    PlanningOracleScenarioReport {
        name: scenario.name.clone(),
        passed: findings.is_empty(),
        latency_ms: run.latency_ms,
        completeness: response
            .meta
            .as_ref()
            .map_or_else(|| "unknown".to_owned(), |meta| meta.completeness.clone()),
        resolution: response
            .meta
            .as_ref()
            .map_or_else(|| "unknown".to_owned(), |meta| meta.resolution.clone()),
        candidate_count: response
            .meta
            .as_ref()
            .map_or(0, |meta| meta.candidate_count),
        output_bytes,
        top1_correct,
        top3_correct,
        reciprocal_rank,
        expected_file_recall,
        expected_repo_recall,
        forbidden_hit_count,
        empty_result: response.data.items.is_empty()
            && !scenario_expects_empty_projection_result(scenario, response),
        unresolved_gap_count: response.data.unresolved_gaps.len(),
        event_target_resolved: run.event_target_resolved,
        ranking_kendall_tau: stability_run.and_then(|other| {
            other.response.as_ref().and_then(|other_response| {
                kendall_tau(&ranked_ids, &ranked_symbol_ids(other_response))
            })
        }),
        findings,
        confirmed_downstream_count: response.data.change_impact.confirmed_downstream_repos.len(),
        cross_repo_expected: scenario_has_cross_repo_expectation(scenario),
        is_shared_contract_task: is_shared_contract_impact_scenario(scenario),
        advisory_only_repo_fraction: {
            let views = parse_proofs(&response.data.planning_proofs);
            advisory_only_repo_fraction(
                &views,
                &response.data.change_impact.confirmed_downstream_repos,
            )
        },
    }
}

fn add_python_oracle_findings(
    findings: &mut Vec<String>,
    oracle: &PythonOracleExpectations,
    response: &ContextPackResponse,
    first_expected_rank: Option<usize>,
    warnings: &[String],
) {
    let observed_repos = python_oracle_observed_repos(response);
    for expected_repo in &oracle.expected_repos {
        if !observed_repos.contains(expected_repo) {
            findings.push(format!("missing Python repo `{expected_repo}`"));
        }
    }

    let observed_bridges = python_oracle_observed_bridges(response);
    for expected_bridge in &oracle.expected_bridges {
        if !observed_bridges.contains(expected_bridge) {
            findings.push(format!("missing Python bridge `{expected_bridge}`"));
        }
    }

    if let Some(required_top_rank) = oracle.required_top_rank {
        if required_top_rank == 0 {
            findings.push("Python required top rank must be at least 1".to_owned());
        } else if first_expected_rank.is_none_or(|rank| rank + 1 > required_top_rank) {
            findings.push(format!(
                "Python top rank exceeded {required_top_rank}; observed {}",
                first_expected_rank
                    .map_or_else(|| "none".to_owned(), |rank| (rank + 1).to_string())
            ));
        }
    }

    if let Some(max_unresolved_gaps) = oracle.max_unresolved_gaps {
        let unresolved_gap_count = response.data.unresolved_gaps.len();
        if unresolved_gap_count > max_unresolved_gaps {
            findings.push(format!(
                "Python unresolved gaps {unresolved_gap_count} exceeded max {max_unresolved_gaps}"
            ));
        }
    }

    if let Some(expected) = &oracle.expected_resolution
        && response
            .meta
            .as_ref()
            .is_none_or(|meta| meta.resolution != *expected)
    {
        findings.push(format!("Python resolution mismatch; expected `{expected}`"));
    }

    if let Some(expected) = &oracle.expected_completeness
        && response
            .meta
            .as_ref()
            .is_none_or(|meta| meta.completeness != *expected)
    {
        findings.push(format!(
            "Python completeness mismatch; expected `{expected}`"
        ));
    }

    for required_warning in &oracle.required_warning_substrings {
        if !warnings
            .iter()
            .any(|warning| warning.contains(required_warning))
        {
            findings.push(format!(
                "missing Python warning containing `{required_warning}`"
            ));
        }
    }
}

fn python_oracle_observed_repos(response: &ContextPackResponse) -> BTreeSet<String> {
    let mut repos = response
        .data
        .items
        .iter()
        .map(|item| item.repo.clone())
        .collect::<BTreeSet<_>>();
    repos.extend(
        response
            .data
            .semantic_bridges
            .iter()
            .map(|bridge| bridge.repo.clone()),
    );
    repos.extend(response.data.change_impact.direct_repos.iter().cloned());
    repos.extend(
        response
            .data
            .change_impact
            .cross_repo_callers
            .iter()
            .map(|caller| caller.repo.clone()),
    );
    repos.extend(
        response
            .data
            .change_impact
            .confirmed_downstream_repos
            .iter()
            .cloned(),
    );
    repos.extend(
        response
            .data
            .change_impact
            .probable_downstream_repos
            .iter()
            .cloned(),
    );
    repos.extend(response.data.change_impact.downstream_repos.iter().cloned());
    for proof in &response.data.planning_proofs {
        if let Some(repo) = proof.get("source_repo").and_then(serde_json::Value::as_str) {
            repos.insert(repo.to_owned());
        }
        if let Some(repo) = proof.get("target_repo").and_then(serde_json::Value::as_str) {
            repos.insert(repo.to_owned());
        }
    }
    repos
}

fn python_oracle_observed_bridges(response: &ContextPackResponse) -> BTreeSet<String> {
    let mut bridges = BTreeSet::new();
    for bridge in &response.data.semantic_bridges {
        bridges.insert(format!("{}:{}", bridge.repo, bridge.name));
        bridges.insert(format!("{}:{}", bridge.repo, bridge.symbol_id));
    }
    for caller in &response.data.change_impact.cross_repo_callers {
        bridges.insert(format!("{}:{}", caller.repo, caller.symbol_name));
        bridges.insert(format!("{}:{}", caller.repo, caller.symbol_id));
        bridges.insert(format!("{}:{}", caller.repo, caller.file_path));
    }
    for item in &response.data.items {
        bridges.insert(format!("{}:{}", item.repo, item.symbol_name));
        bridges.insert(format!("{}:{}", item.repo, item.symbol_id));
        bridges.insert(format!("{}:{}", item.repo, item.file_path));
    }
    for proof in &response.data.planning_proofs {
        let source_repo = proof.get("source_repo").and_then(serde_json::Value::as_str);
        let target_repo = proof.get("target_repo").and_then(serde_json::Value::as_str);
        let source_file = proof.get("source_file").and_then(serde_json::Value::as_str);
        let target_file = proof.get("target_file").and_then(serde_json::Value::as_str);
        let kind = proof.get("kind").and_then(serde_json::Value::as_str);
        if let (Some(repo), Some(file)) = (source_repo, source_file) {
            bridges.insert(format!("{repo}:{file}"));
        }
        if let (Some(repo), Some(file)) = (target_repo, target_file) {
            bridges.insert(format!("{repo}:{file}"));
        }
        if let (Some(source), Some(target)) = (source_repo, target_repo) {
            bridges.insert(format!("{source}:{target}"));
            if let Some(kind) = kind {
                bridges.insert(format!("{source}:{kind}:{target}"));
            }
        }
    }
    bridges
}

impl WorkspaceFileIndex {
    fn build(config: &GatherStepConfig, config_root: &Path) -> anyhow::Result<Self> {
        let mut seen = BTreeMap::<String, Option<String>>::new();
        for repo in &config.repos {
            let repo_root = config_root.join(&repo.path);
            collect_repo_files(&repo_root, &repo_root, &repo.name, &mut seen)?;
        }
        let unique_file_to_repo = seen
            .into_iter()
            .filter_map(|(file, repo)| repo.map(|repo| (file, repo)))
            .collect();
        Ok(Self {
            unique_file_to_repo,
        })
    }

    fn expected_repo_recall(
        &self,
        expected_files: &[String],
        observed_repos: &BTreeSet<String>,
    ) -> Option<f64> {
        let expected_repos = expected_files
            .iter()
            .filter_map(|file| self.unique_file_to_repo.get(file))
            .cloned()
            .collect::<BTreeSet<_>>();
        if expected_repos.is_empty() {
            return None;
        }
        let matched = expected_repos
            .iter()
            .filter(|repo| observed_repos.contains(repo.as_str()))
            .count();
        Some(ratio_usize(matched, expected_repos.len()))
    }
}

fn collect_repo_files(
    repo_root: &Path,
    current: &Path,
    repo_name: &str,
    seen: &mut BTreeMap<String, Option<String>>,
) -> anyhow::Result<()> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_repo_files(repo_root, &path, repo_name, seen)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }
        let relative = path
            .strip_prefix(repo_root)
            .map_err(|error| anyhow::anyhow!("failed to strip repo prefix: {error}"))?;
        let relative = normalize_relative_path(relative);
        match seen.get(&relative) {
            None => {
                seen.insert(relative, Some(repo_name.to_owned()));
            }
            Some(Some(existing)) if existing != repo_name => {
                seen.insert(relative, None);
            }
            Some(_) => {}
        }
    }
    Ok(())
}

fn normalize_relative_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

// ─── Proof-parsing helpers ────────────────────────────────────────────────────

/// A lightweight view into a single `PlanningProof` serialized as
/// [`serde_json::Value`].
///
/// Borrows directly from the `Value` so no allocation is needed for the string
/// fields.  Fields that are missing or non-string in the JSON produce `""` /
/// `0` defaults so callers can tolerate schema gaps without panicking.
struct ProofView<'a> {
    kind: &'a str,
    strength: u8,
    target_repo: &'a str,
}

/// Extract a [`ProofView`] slice from `planning_proofs` field of a response.
///
/// Entries that are not JSON objects or that are missing required fields are
/// silently skipped.
fn parse_proofs(proofs: &[serde_json::Value]) -> Vec<ProofView<'_>> {
    proofs
        .iter()
        .filter_map(|v| {
            let obj = v.as_object()?;
            let kind = obj.get("kind")?.as_str().unwrap_or("");
            let strength = proof_strength_from_json(obj);
            let target_repo = obj.get("target_repo")?.as_str().unwrap_or("");
            Some(ProofView {
                kind,
                strength,
                target_repo,
            })
        })
        .collect()
}

/// Extract the proof `strength` field as `u8` from a JSON object map.
///
/// Values are clamped to `u8::MAX` before the cast so the narrowing is safe.
/// Absent or non-numeric fields yield 0.
fn proof_strength_from_json(obj: &serde_json::Map<String, serde_json::Value>) -> u8 {
    obj.get("strength")
        .and_then(serde_json::Value::as_u64)
        .map_or(0, |s| {
            #[expect(
                clippy::cast_possible_truncation,
                reason = "value is clamped to u8::MAX before the cast; truncation cannot occur at runtime"
            )]
            let v = s.min(u64::from(u8::MAX)) as u8;
            v
        })
}

/// Compute the fraction of `confirmed_repos` that have ONLY advisory-strength
/// proofs (`strength < 33` or `kind == "CoChangeAdvisory"`).
///
/// Returns `None` when `confirmed_repos` is empty.
fn advisory_only_repo_fraction(
    proofs: &[ProofView<'_>],
    confirmed_repos: &[String],
) -> Option<f64> {
    if confirmed_repos.is_empty() {
        return None;
    }
    let advisory_count = confirmed_repos
        .iter()
        .filter(|repo| {
            let repo_proofs: Vec<_> = proofs
                .iter()
                .filter(|p| p.target_repo == repo.as_str())
                .collect();
            // A repo is "advisory-only" when every proof targeting it is advisory.
            !repo_proofs.is_empty()
                && repo_proofs
                    .iter()
                    .all(|p| p.kind == "CoChangeAdvisory" || p.strength < 33)
        })
        .count();
    #[expect(
        clippy::cast_precision_loss,
        reason = "confirmed_repos count is tiny and only used for benchmark metrics"
    )]
    Some(advisory_count as f64 / confirmed_repos.len() as f64)
}

/// Fraction of confirmed repos that have NO proof entry at all.  Distinct
/// from advisory leak: an unexplained repo is neither advisory nor
/// structural — it's unverified.  The release gate consults this so that
/// deferred proof emission cannot trivially turn into a passing
/// `no_advisory_domination` result.
///
/// Returns `None` when `confirmed_repos` is empty.
fn unexplained_confirmed_repo_fraction(
    proofs: &[ProofView<'_>],
    confirmed_repos: &[String],
) -> Option<f64> {
    if confirmed_repos.is_empty() {
        return None;
    }
    // A confirmed downstream repo is "explained" only when at least one proof
    // **targets** it: the proof builder derives confirmed_downstream_repos
    // from proof.target_repo (see crates/gather-step-mcp/src/tools/proof_builder.rs).
    // Counting source_repo matches would mask the gap by treating the anchor's
    // own repo as proof for unrelated downstream repos.
    let unexplained = confirmed_repos
        .iter()
        .filter(|repo| !proofs.iter().any(|p| p.target_repo == repo.as_str()))
        .count();
    #[expect(
        clippy::cast_precision_loss,
        reason = "confirmed_repos count is tiny and only used for benchmark metrics"
    )]
    Some(unexplained as f64 / confirmed_repos.len() as f64)
}

fn ranked_symbol_ids(response: &ContextPackResponse) -> Vec<String> {
    response
        .data
        .items
        .iter()
        .map(|item| item.symbol_id.clone())
        .collect()
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

fn reciprocal_rank(rank: usize) -> f64 {
    #[expect(
        clippy::cast_precision_loss,
        reason = "rank values are tiny and only used for benchmark aggregation"
    )]
    {
        1.0 / (rank + 1) as f64
    }
}

fn kendall_tau(left: &[String], right: &[String]) -> Option<f64> {
    let right_positions = right
        .iter()
        .enumerate()
        .map(|(index, symbol_id)| (symbol_id.as_str(), index))
        .collect::<BTreeMap<_, _>>();
    let common = left
        .iter()
        .filter(|symbol_id| right_positions.contains_key(symbol_id.as_str()))
        .collect::<Vec<_>>();
    if common.is_empty() {
        return None;
    }
    if common.len() < 2 {
        return Some(1.0);
    }

    let mut concordant: usize = 0;
    let mut discordant: usize = 0;
    for left_index in 0..common.len() {
        for right_index in (left_index + 1)..common.len() {
            let first = common[left_index];
            let second = common[right_index];
            if right_positions[first.as_str()] < right_positions[second.as_str()] {
                concordant += 1;
            } else {
                discordant += 1;
            }
        }
    }

    let total_pairs = concordant + discordant;
    if total_pairs == 0 {
        return Some(1.0);
    }
    #[expect(
        clippy::cast_precision_loss,
        reason = "pair counts are tiny and only used for benchmark aggregation"
    )]
    Some((concordant as f64 - discordant as f64) / total_pairs as f64)
}

fn ratio_usize(numerator: usize, denominator: usize) -> f64 {
    if denominator == 0 {
        return 0.0;
    }
    #[expect(
        clippy::cast_precision_loss,
        reason = "benchmark counts are tiny and only used for summary metrics"
    )]
    {
        numerator as f64 / denominator as f64
    }
}

fn average_bool(values: impl Iterator<Item = bool>) -> f64 {
    let mut total = 0usize;
    let mut positives = 0usize;
    for value in values {
        total += 1;
        if value {
            positives += 1;
        }
    }
    ratio_usize(positives, total)
}

fn average_f64(values: impl Iterator<Item = f64>) -> f64 {
    let mut total = 0usize;
    let mut sum = 0.0;
    for value in values {
        total += 1;
        sum += value;
    }
    if total == 0 {
        0.0
    } else {
        #[expect(
            clippy::cast_precision_loss,
            reason = "benchmark counts are tiny and only used for summary metrics"
        )]
        {
            sum / total as f64
        }
    }
}

fn average_option_f64(values: impl Iterator<Item = Option<f64>>) -> Option<f64> {
    let mut total = 0usize;
    let mut sum = 0.0;
    for value in values.flatten() {
        total += 1;
        sum += value;
    }
    if total == 0 {
        None
    } else {
        #[expect(
            clippy::cast_precision_loss,
            reason = "benchmark counts are tiny and only used for summary metrics"
        )]
        {
            Some(sum / total as f64)
        }
    }
}

fn average_option_bool(values: impl Iterator<Item = Option<bool>>) -> Option<f64> {
    let mut total = 0usize;
    let mut positives = 0usize;
    for value in values.flatten() {
        total += 1;
        if value {
            positives += 1;
        }
    }
    if total == 0 {
        None
    } else {
        Some(ratio_usize(positives, total))
    }
}

fn percentile(samples: &[u64], percentile: f64) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    let max_index = samples.len().saturating_sub(1);
    #[expect(
        clippy::cast_possible_truncation,
        reason = "bounded percentile index is clamped to the sample range"
    )]
    #[expect(
        clippy::cast_precision_loss,
        reason = "sample counts in this benchmark are small and only used for percentile indexing"
    )]
    #[expect(
        clippy::cast_sign_loss,
        reason = "the rounded percentile index is non-negative before conversion"
    )]
    let raw_index = (((percentile / 100.0) * max_index as f64).round() as usize).min(max_index);
    samples[raw_index]
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs};

    use gather_step_mcp::{
        budget::{BudgetedTool, ResponseBudget, response_schema_version},
        tools::packs::{
            ChangeImpactSummary, ContextPackData, ContextPackMeta, ContextPackResponse,
            CrossRepoCaller, PackBridge, PackItem,
        },
    };
    use serde_json::json;

    use super::{
        WorkspaceFileIndex, advisory_only_repo_fraction, build_scenario_report,
        compute_split_metrics, kendall_tau, load_oracle_scenarios, parse_proofs, percentile,
        projection_impact_payload_as_context_pack_response, unexplained_confirmed_repo_fraction,
    };
    use crate::planning_oracle::{
        OracleExpectations, OracleRun, OracleScenario, OracleTarget, PythonOracleExpectations,
    };

    // ── percentile / kendall_tau ─────────────────────────────────────────────

    #[test]
    fn percentile_picks_expected_bucket() {
        let samples = vec![10, 20, 30, 40, 50];
        assert_eq!(percentile(&samples, 50.0), 30);
        assert_eq!(percentile(&samples, 95.0), 50);
        assert_eq!(percentile(&samples, 99.0), 50);
    }

    #[test]
    fn kendall_tau_is_one_for_identical_rankings() {
        let left = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
        let right = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
        assert_eq!(kendall_tau(&left, &right), Some(1.0));
    }

    #[test]
    fn kendall_tau_detects_reordered_pairs() {
        let left = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
        let right = vec!["b".to_owned(), "a".to_owned(), "c".to_owned()];
        assert_eq!(kendall_tau(&left, &right), Some(1.0 / 3.0));
    }

    // ── advisory_only_repo_fraction ──────────────────────────────────────────

    fn proof_json(kind: &str, strength: u8, target_repo: &str) -> serde_json::Value {
        json!({
            "kind": kind,
            "strength": strength,
            "source_repo": "source_repo",
            "target_repo": target_repo,
        })
    }

    #[test]
    fn advisory_only_repo_fraction_returns_none_when_no_confirmed_repos() {
        let proofs = vec![proof_json("CoChangeAdvisory", 25, "repo_a")];
        let views = parse_proofs(&proofs);
        let result = advisory_only_repo_fraction(&views, &[]);
        assert!(
            result.is_none(),
            "should be None when confirmed_repos is empty"
        );
    }

    #[test]
    fn advisory_only_repo_fraction_returns_zero_when_all_repos_are_structural() {
        // Both repos are backed by structural proofs (strength >= 67).
        let proofs = vec![
            proof_json("SharedContractConsumer", 75, "repo_a"),
            proof_json("DirectCall", 85, "repo_b"),
        ];
        let views = parse_proofs(&proofs);
        let confirmed = vec!["repo_a".to_owned(), "repo_b".to_owned()];
        let result = advisory_only_repo_fraction(&views, &confirmed);
        assert_eq!(result, Some(0.0), "all structural → fraction should be 0.0");
    }

    #[test]
    fn advisory_only_repo_fraction_returns_one_when_all_repos_are_co_change_only() {
        let proofs = vec![
            proof_json("CoChangeAdvisory", 25, "repo_a"),
            proof_json("CoChangeAdvisory", 25, "repo_b"),
        ];
        let views = parse_proofs(&proofs);
        let confirmed = vec!["repo_a".to_owned(), "repo_b".to_owned()];
        let result = advisory_only_repo_fraction(&views, &confirmed);
        assert_eq!(result, Some(1.0), "all advisory → fraction should be 1.0");
    }

    #[test]
    fn advisory_only_repo_fraction_mixes_correctly_for_partial_advisory() {
        // repo_a: only advisory; repo_b: has a structural proof.
        let proofs = vec![
            proof_json("CoChangeAdvisory", 25, "repo_a"),
            proof_json("DirectCall", 85, "repo_b"),
        ];
        let views = parse_proofs(&proofs);
        let confirmed = vec!["repo_a".to_owned(), "repo_b".to_owned()];
        let result = advisory_only_repo_fraction(&views, &confirmed);
        // 1 of 2 repos is advisory-only → 0.5
        assert!(
            (result.unwrap() - 0.5).abs() < f64::EPSILON,
            "1/2 advisory → fraction should be 0.5; got {result:?}"
        );
    }

    // ── unexplained_confirmed_repo_fraction tests ────────────────────────────
    //
    // The proof builder derives `confirmed_downstream_repos` from
    // `proof.target_repo`.  A confirmed repo is "explained" only when at
    // least one proof TARGETS it; appearance as a `source_repo` does not
    // count.  These tests lock that invariant down so a future refactor
    // cannot silently revert to the broader OR check that masked the gap.

    #[test]
    fn unexplained_confirmed_repo_fraction_returns_none_when_no_confirmed_repos() {
        let proofs = vec![proof_json("DirectCall", 85, "repo_a")];
        let views = parse_proofs(&proofs);
        assert_eq!(unexplained_confirmed_repo_fraction(&views, &[]), None);
    }

    #[test]
    fn unexplained_confirmed_repo_fraction_counts_repo_with_no_targeting_proof() {
        // repo_a is targeted; repo_b is NOT targeted by any proof.
        let proofs = vec![proof_json("DirectCall", 85, "repo_a")];
        let views = parse_proofs(&proofs);
        let confirmed = vec!["repo_a".to_owned(), "repo_b".to_owned()];
        let result = unexplained_confirmed_repo_fraction(&views, &confirmed);
        // 1 of 2 repos lacks any targeting proof → 0.5
        assert!(
            (result.unwrap() - 0.5).abs() < f64::EPSILON,
            "1/2 unexplained → fraction should be 0.5; got {result:?}"
        );
    }

    #[test]
    fn unexplained_confirmed_repo_fraction_does_not_credit_source_repo_match() {
        // The `proof_json` helper sets `source_repo` to the literal string
        // "source_repo". Build a confirmed list that includes "source_repo"
        // — which appears as a SOURCE in every proof but is never a TARGET.
        // The metric must count it as unexplained, not credit it because it
        // matches `source_repo`.
        let proofs = vec![
            proof_json("DirectCall", 85, "real_target_a"),
            proof_json("DirectCall", 85, "real_target_b"),
        ];
        let views = parse_proofs(&proofs);
        let confirmed = vec![
            "real_target_a".to_owned(),
            "source_repo".to_owned(),
            "real_target_b".to_owned(),
        ];
        let result = unexplained_confirmed_repo_fraction(&views, &confirmed);
        // 1 of 3 repos is unexplained ("source_repo" — appears as a source
        // but never as a target) → 1/3
        let expected = 1.0_f64 / 3.0_f64;
        assert!(
            (result.unwrap() - expected).abs() < 1e-9,
            "source-only repo must count as unexplained; got {result:?}"
        );
    }

    #[test]
    fn unexplained_confirmed_repo_fraction_returns_zero_when_every_repo_is_targeted() {
        let proofs = vec![
            proof_json("DirectCall", 85, "repo_a"),
            proof_json("ImportBridge", 55, "repo_b"),
        ];
        let views = parse_proofs(&proofs);
        let confirmed = vec!["repo_a".to_owned(), "repo_b".to_owned()];
        let result = unexplained_confirmed_repo_fraction(&views, &confirmed);
        assert!(
            result.unwrap() < f64::EPSILON,
            "every confirmed repo targeted → fraction should be 0.0; got {result:?}"
        );
    }

    // ── helper builders for compute_split_metrics unit tests ─────────────────

    fn minimal_response(
        anchor_file: &str,
        confirmed_downstream: &[String],
        proofs: &[serde_json::Value],
    ) -> ContextPackResponse {
        // Build a minimal valid response by round-tripping through JSON so that
        // optional/defaulted fields are correctly handled without needing to
        // maintain field-level parity with every struct change.
        let value = json!({
            "data": {
                "mode": "planning",
                "target": "some::Target",
                "found": true,
                "items": [
                    {
                        "category": "primary",
                        "file_path": anchor_file,
                        "reason": "top match",
                        "repo": "source_repo",
                        "score": 90,
                        "symbol_id": "sym_1",
                        "symbol_kind": "Function",
                        "symbol_name": "Symbol",
                    }
                ],
                "semantic_bridges": [],
                "next_steps": [],
                "unresolved_gaps": [],
                "change_impact": {
                    "direct_repos": [],
                    "downstream_repos": confirmed_downstream,
                    "confirmed_downstream_repos": confirmed_downstream,
                    "unresolved_possible": [],
                },
                "planning_proofs": proofs,
            }
        });
        serde_json::from_value(value).expect("minimal response JSON must deserialize correctly")
    }

    fn minimal_scenario(
        mode: &str,
        target_kind: &str,
        expected_files: Vec<String>,
        forbidden_files: Vec<String>,
        expected_canonical_anchor: Option<String>,
        expected_confirmed_downstream_repos: Vec<String>,
        expected_cross_repo_caller_repos: Vec<String>,
    ) -> OracleScenario {
        OracleScenario {
            name: "test_scenario".to_owned(),
            mode: mode.to_owned(),
            repo: None,
            target: OracleTarget {
                kind: target_kind.to_owned(),
                qn: "some::QN".to_owned(),
            },
            oracle: OracleExpectations {
                expected_files,
                forbidden_files,
                max_follow_ups: 10,
                min_confidence: 0,
                required_ambiguity_codes: vec![],
                expected_primary_symbol_name: None,
                expected_primary_symbol_kind: None,
                expected_primary_repo: None,
                expected_primary_file: None,
                expected_resolved_symbol_kind: None,
                expected_confirmed_downstream_repos,
                expected_cross_repo_caller_repos,
                forbidden_cross_repo_caller_repos: vec![],
                forbidden_confirmed_downstream_repos: vec![],
                max_probable_downstream_repos: None,
                forbidden_warnings: vec![],
                expected_resolution: None,
                expected_confidence_model_version: None,
                expected_impact_repos: vec![],
                expected_primary_strategy: None,
                required_primary_edge_kinds: vec![],
                forbidden_primary_edge_kinds: vec![],
                expected_structural_repos: vec![],
                forbidden_advisory_in_primary: vec![],
                expected_projection_resolved: None,
                expected_projection_ambiguity: None,
                expected_projection_risks: vec![],
                forbidden_projection_risks: vec![],
                expected_projection_fields: vec![],
                expected_source_fields: vec![],
                expected_backfill_files: vec![],
                expected_index_files: vec![],
                forbidden_focus_only_files: vec![],
                max_response_bytes: 1_000_000,
                expected_canonical_anchor,
                require_top1_canonical: None,
            },
            python_oracle: None,
        }
    }

    fn run_with(response: ContextPackResponse) -> OracleRun {
        OracleRun {
            error: None,
            event_target_resolved: None,
            impact_response: None,
            latency_ms: 5,
            response: Some(response),
        }
    }

    fn python_oracle_response() -> ContextPackResponse {
        ContextPackResponse {
            data: ContextPackData {
                mode: "planning".to_owned(),
                target: "transform_batch".to_owned(),
                found: true,
                items: vec![
                    PackItem {
                        category: "primary".to_owned(),
                        file_path: "src/transform_service/pipeline.py".to_owned(),
                        line_start: Some(12),
                        reason: "top match".to_owned(),
                        repo: "py_transform_service".to_owned(),
                        score: 950,
                        symbol_id: "py-transform-symbol".to_owned(),
                        symbol_kind: "function".to_owned(),
                        symbol_name: "transform_batch".to_owned(),
                        evidence_chain: None,
                    },
                    PackItem {
                        category: "structural_neighbor".to_owned(),
                        file_path: "src/shared_models/records.py".to_owned(),
                        line_start: Some(5),
                        reason: "type dependency".to_owned(),
                        repo: "py_shared_models".to_owned(),
                        score: 870,
                        symbol_id: "py-shared-symbol".to_owned(),
                        symbol_kind: "class".to_owned(),
                        symbol_name: "ParsedDocument".to_owned(),
                        evidence_chain: None,
                    },
                ],
                semantic_bridges: vec![PackBridge {
                    kind: "sharedsymbol".to_owned(),
                    name: "shared_models.records.ParsedDocument".to_owned(),
                    repo: "py_transform_service".to_owned(),
                    symbol_id: "py-shared-symbol".to_owned(),
                }],
                next_steps: Vec::new(),
                unresolved_gaps: vec!["optional dynamic import not resolved".to_owned()],
                change_impact: ChangeImpactSummary {
                    direct_repos: vec!["py_transform_service".to_owned()],
                    cross_repo_callers: vec![CrossRepoCaller {
                        file_path: "src/api_service/routes.py".to_owned(),
                        line_start: Some(24),
                        repo: "py_api_service".to_owned(),
                        symbol_id: "py-api-caller".to_owned(),
                        symbol_kind: "function".to_owned(),
                        symbol_name: "transform_service.pipeline.transform_batch".to_owned(),
                    }],
                    confirmed_downstream_repos: vec!["py_shared_models".to_owned()],
                    probable_downstream_repos: Vec::new(),
                    downstream_repos: vec!["py_shared_models".to_owned()],
                    unresolved_possible: Vec::new(),
                    truncated_repos: None,
                },
                transport_links: None,
                planning_rescue: None,
                planning_proofs: vec![json!({
                    "kind": "ImportBridge",
                    "strength": 55,
                    "source_repo": "py_transform_service",
                    "target_repo": "py_shared_models",
                    "source_file": "src/transform_service/pipeline.py",
                    "target_file": "src/shared_models/records.py",
                })],
                migration_siblings: None,
            },
            meta: Some(ContextPackMeta {
                response_schema_version: response_schema_version(),
                generation: 0,
                ambiguity: None,
                budget: ResponseBudget::not_truncated(BudgetedTool::ContextPack, 22_000, 1_000),
                candidate_count: 2,
                completeness: "primary_and_structural_neighbors".to_owned(),
                resolution: "search_ranked_resolved".to_owned(),
                resolution_details: None,
                confidence_model_version: Some("v1.0".to_owned()),
                resolution_confidence: None,
                resolved_symbol_id: Some("py-transform-symbol".to_owned()),
                winner_margin: None,
                warnings: vec!["namespace widened through import bridge".to_owned()],
            }),
        }
    }

    // ── anchor_top1 ──────────────────────────────────────────────────────────

    #[test]
    fn anchor_top1_skips_scenarios_without_expected_canonical_anchor() {
        // No scenario declares expected_canonical_anchor → anchor_top1 must be None.
        let scenario = minimal_scenario(
            "planning",
            "symbol",
            vec![],
            vec![],
            None, // no anchor expectation
            vec![],
            vec![],
        );
        let response = minimal_response("src/whatever.ts", &[], &[]);
        let split = compute_split_metrics(&[scenario], &[run_with(response)]);
        assert!(
            split.anchor_top1.is_none(),
            "anchor_top1 should be None when no scenario declares the field"
        );
    }

    #[test]
    fn anchor_top1_counts_correct_top1_match() {
        let scenario = minimal_scenario(
            "planning",
            "symbol",
            vec![],
            vec![],
            Some("src/target_file.ts".to_owned()),
            vec![],
            vec![],
        );
        // Response has the expected file as top-1.
        let response = minimal_response("src/target_file.ts", &[], &[]);
        let split = compute_split_metrics(&[scenario], &[run_with(response)]);
        assert_eq!(
            split.anchor_top1,
            Some(1.0),
            "top-1 matches expected anchor → score should be 1.0"
        );
    }

    // ── proof_recall ─────────────────────────────────────────────────────────

    #[test]
    fn proof_recall_counts_each_expected_repo_once() {
        let scenario = minimal_scenario(
            "planning",
            "symbol",
            vec![],
            vec![],
            None,
            vec!["repo_a".to_owned(), "repo_b".to_owned()],
            vec![],
        );
        // Only repo_a has a proof emitted; repo_b does not.
        let proofs = vec![proof_json("DirectCall", 85, "repo_a")];
        let confirmed = vec!["repo_a".to_owned()];
        let response = minimal_response("src/anchor.ts", &confirmed, &proofs);
        let split = compute_split_metrics(&[scenario], &[run_with(response)]);
        // 1 of 2 expected repos hit → recall = 0.5
        let recall = split.proof_recall.expect("proof_recall should be Some");
        assert!(
            (recall - 0.5).abs() < 1e-5,
            "recall should be 0.5; got {recall}"
        );
    }

    // ── structural_ratio ─────────────────────────────────────────────────────

    #[test]
    fn structural_ratio_excludes_co_change_advisory_proofs() {
        let scenario = minimal_scenario("planning", "symbol", vec![], vec![], None, vec![], vec![]);
        let proofs = vec![
            proof_json("DirectCall", 85, "repo_a"),       // structural
            proof_json("CoChangeAdvisory", 25, "repo_b"), // advisory
        ];
        let response = minimal_response("src/anchor.ts", &[], &proofs);
        let split = compute_split_metrics(&[scenario], &[run_with(response)]);
        let ratio = split
            .structural_ratio
            .expect("structural_ratio should be Some");
        // 1 structural out of 2 total → 0.5
        assert!(
            (ratio - 0.5).abs() < 1e-5,
            "structural_ratio should be 0.5; got {ratio}"
        );
    }

    // ── advisory_leak_rate ────────────────────────────────────────────────────

    #[test]
    fn advisory_leak_rate_takes_max_across_shared_contract_scenarios() {
        // Two shared-contract scenarios with different advisory fractions.
        // Scenario 1: repo_a advisory-only → fraction = 1.0
        // Scenario 2: repo_b structural → fraction = 0.0
        // advisory_leak_rate should be max = 1.0.
        let s1 = minimal_scenario("impact", "symbol", vec![], vec![], None, vec![], vec![]);
        let proofs1 = vec![proof_json("CoChangeAdvisory", 25, "repo_a")];
        let confirmed1 = vec!["repo_a".to_owned()];
        let r1 = minimal_response("src/anchor.ts", &confirmed1, &proofs1);

        let s2 = minimal_scenario("impact", "symbol", vec![], vec![], None, vec![], vec![]);
        let proofs2 = vec![proof_json("DirectCall", 85, "repo_b")];
        let confirmed2 = vec!["repo_b".to_owned()];
        let r2 = minimal_response("src/anchor.ts", &confirmed2, &proofs2);

        let split = compute_split_metrics(&[s1, s2], &[run_with(r1), run_with(r2)]);
        let leak = split
            .advisory_leak_rate
            .expect("advisory_leak_rate should be Some when shared-contract scenarios present");
        assert!(
            (leak - 1.0).abs() < 1e-5,
            "max advisory fraction is 1.0; got {leak}"
        );
    }

    #[test]
    fn advisory_leak_rate_includes_change_impact_shared_contract_scenarios() {
        let scenario = minimal_scenario(
            "change_impact",
            "symbol",
            vec![],
            vec![],
            None,
            vec![],
            vec![],
        );
        let proofs = vec![proof_json("CoChangeAdvisory", 25, "repo_a")];
        let confirmed = vec!["repo_a".to_owned()];
        let response = minimal_response("src/anchor.ts", &confirmed, &proofs);

        let split = compute_split_metrics(&[scenario], &[run_with(response)]);

        assert_eq!(
            split.advisory_leak_rate,
            Some(1.0),
            "change_impact symbol scenarios must contribute to advisory leak metrics"
        );
    }

    #[test]
    fn build_scenario_report_enforces_structural_repo_expectations() {
        let mut scenario = minimal_scenario(
            "change_impact",
            "symbol",
            vec![],
            vec![],
            None,
            vec![],
            vec![],
        );
        scenario.oracle.expected_structural_repos = vec!["repo_b".to_owned()];
        let response = minimal_response("src/anchor.ts", &["repo_a".to_owned()], &[]);
        let workspace_index = WorkspaceFileIndex {
            unique_file_to_repo: BTreeMap::new(),
        };

        let report = build_scenario_report(&workspace_index, &scenario, &run_with(response), None);

        assert!(!report.passed);
        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.contains("missing structural downstream repo `repo_b`")),
            "expected missing structural repo finding; got {:?}",
            report.findings
        );
    }

    #[test]
    fn build_scenario_report_enforces_expected_impact_repos() {
        let mut scenario = minimal_scenario(
            "change_impact",
            "symbol",
            vec![],
            vec![],
            None,
            vec![],
            vec![],
        );
        scenario.oracle.expected_impact_repos = vec!["repo_b".to_owned()];
        let response = minimal_response("src/anchor.ts", &["repo_a".to_owned()], &[]);
        let workspace_index = WorkspaceFileIndex {
            unique_file_to_repo: BTreeMap::new(),
        };

        let report = build_scenario_report(&workspace_index, &scenario, &run_with(response), None);

        assert!(!report.passed);
        assert!(report.cross_repo_expected);
        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.contains("missing impacted repo `repo_b`")),
            "expected missing impact repo finding; got {:?}",
            report.findings
        );
    }

    #[test]
    fn build_scenario_report_rejects_forbidden_advisory_primary_repo() {
        let mut scenario = minimal_scenario(
            "change_impact",
            "symbol",
            vec![],
            vec![],
            None,
            vec![],
            vec![],
        );
        scenario.oracle.forbidden_advisory_in_primary = vec!["source_repo".to_owned()];
        let response = minimal_response("src/anchor.ts", &[], &[]);
        let workspace_index = WorkspaceFileIndex {
            unique_file_to_repo: BTreeMap::new(),
        };

        let report = build_scenario_report(&workspace_index, &scenario, &run_with(response), None);

        assert!(!report.passed);
        assert!(
            report.findings.iter().any(|finding| {
                finding.contains("advisory repo `source_repo` appeared in structural pack items")
            }),
            "expected forbidden advisory primary finding; got {:?}",
            report.findings
        );
    }

    #[test]
    fn load_oracle_scenarios_accepts_direct_toml_files() {
        let temp = tempfile::tempdir().expect("temp dir should be created");
        fs::write(
            temp.path().join("direct.toml"),
            r#"
name = "direct_python"
mode = "planning"

[target]
kind = "symbol"
qn = "Target"

[oracle]
expected_files = []
forbidden_files = []
max_follow_ups = 1
min_confidence = 0
max_response_bytes = 1000
"#,
        )
        .expect("direct scenario should be written");
        let nested = temp.path().join("nested_python");
        fs::create_dir(&nested).expect("nested scenario dir should be created");
        fs::write(
            nested.join("scenario.toml"),
            r#"
name = "nested_python"
mode = "planning"

[target]
kind = "symbol"
qn = "Target"

[oracle]
expected_files = []
forbidden_files = []
max_follow_ups = 1
min_confidence = 0
max_response_bytes = 1000
"#,
        )
        .expect("nested scenario should be written");

        let scenarios = load_oracle_scenarios(temp.path()).expect("scenarios should load");

        let names = scenarios
            .iter()
            .map(|scenario| scenario.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["direct_python", "nested_python"]);
    }

    #[test]
    fn build_scenario_report_enforces_python_oracle_expectations() {
        let mut scenario = minimal_scenario(
            "planning",
            "symbol",
            vec!["src/transform_service/pipeline.py".to_owned()],
            vec![],
            None,
            vec![],
            vec![],
        );
        scenario.python_oracle = Some(PythonOracleExpectations {
            expected_repos: vec![
                "py_api_service".to_owned(),
                "py_transform_service".to_owned(),
                "py_shared_models".to_owned(),
            ],
            expected_bridges: vec![
                "py_api_service:transform_service.pipeline.transform_batch".to_owned(),
                "py_transform_service:shared_models.records.ParsedDocument".to_owned(),
            ],
            required_top_rank: Some(1),
            max_unresolved_gaps: Some(1),
            expected_resolution: Some("search_ranked_resolved".to_owned()),
            expected_completeness: Some("primary_and_structural_neighbors".to_owned()),
            required_warning_substrings: vec!["namespace widened".to_owned()],
        });
        let response = python_oracle_response();
        let workspace_index = WorkspaceFileIndex {
            unique_file_to_repo: BTreeMap::new(),
        };

        let report = build_scenario_report(&workspace_index, &scenario, &run_with(response), None);

        assert!(
            report.passed,
            "expected Python oracle scenario to pass; got {:?}",
            report.findings
        );
    }

    #[test]
    fn build_scenario_report_flags_missing_python_oracle_bridge() {
        let mut scenario = minimal_scenario(
            "planning",
            "symbol",
            vec!["src/transform_service/pipeline.py".to_owned()],
            vec![],
            None,
            vec![],
            vec![],
        );
        scenario.python_oracle = Some(PythonOracleExpectations {
            expected_bridges: vec!["py_api_service:missing.bridge".to_owned()],
            ..PythonOracleExpectations::default()
        });
        let response = python_oracle_response();
        let workspace_index = WorkspaceFileIndex {
            unique_file_to_repo: BTreeMap::new(),
        };

        let report = build_scenario_report(&workspace_index, &scenario, &run_with(response), None);

        assert!(!report.passed);
        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding
                    .contains("missing Python bridge `py_api_service:missing.bridge`")),
            "expected missing Python bridge finding; got {:?}",
            report.findings
        );
    }

    #[test]
    fn oracle_expectations_accept_projection_impact_fields() {
        let raw = r#"
name = "projection_impact_contract"
mode = "projection_impact"
repo = "backend_standard"

[target]
kind = "field"
qn = "legacySeatIds"

[oracle]
expected_files = []
forbidden_files = []
max_follow_ups = 6
min_confidence = 0
expected_projection_resolved = true
expected_projection_ambiguity = "multiple_field_candidates"
expected_projection_risks = ["source_field_unreviewed"]
forbidden_projection_risks = ["backfill_unproven"]
expected_projection_fields = ["legacySeatIds"]
expected_source_fields = ["seats"]
expected_backfill_files = []
expected_index_files = []
forbidden_focus_only_files = ["src/projection_fixed.ts"]
max_response_bytes = 12000
"#;

        let scenario =
            toml::from_str::<OracleScenario>(raw).expect("projection oracle keys should parse");

        assert_eq!(scenario.oracle.expected_projection_resolved, Some(true));
        assert_eq!(
            scenario.oracle.expected_projection_ambiguity.as_deref(),
            Some("multiple_field_candidates")
        );
        assert_eq!(
            scenario.oracle.expected_projection_risks,
            vec!["source_field_unreviewed".to_owned()]
        );
    }

    #[test]
    fn projection_expected_not_found_does_not_count_as_empty_regression() {
        let mut scenario = minimal_scenario(
            "projection_impact",
            "field",
            vec![],
            vec![],
            None,
            vec![],
            vec![],
        );
        scenario.oracle.expected_projection_resolved = Some(false);
        scenario.oracle.expected_projection_risks = vec!["field_candidate_not_found".to_owned()];

        let payload = json!({
            "resolved": false,
            "confidence": "low",
            "candidates": [],
            "risk_hints": ["field_candidate_not_found"],
            "missing_evidence": ["data_field"],
        });
        let response = projection_impact_payload_as_context_pack_response(&payload, &scenario);
        let mut run = run_with(response);
        run.impact_response = Some(payload);
        let workspace_index = WorkspaceFileIndex {
            unique_file_to_repo: BTreeMap::new(),
        };

        let report = build_scenario_report(&workspace_index, &scenario, &run, None);

        assert!(
            report.passed,
            "expected not-found projection scenario to pass; got {:?}",
            report.findings
        );
        assert!(!report.empty_result);
        assert_eq!(report.unresolved_gap_count, 0);
    }

    #[test]
    fn projection_expected_missing_evidence_is_not_a_generic_unresolved_gap() {
        let mut scenario = minimal_scenario(
            "projection_impact",
            "field",
            vec![],
            vec![],
            None,
            vec![],
            vec![],
        );
        scenario.oracle.expected_projection_risks = vec![
            "backfill_unproven".to_owned(),
            "index_or_search_mapping_unproven".to_owned(),
        ];

        let payload = json!({
            "resolved": true,
            "confidence": "high",
            "candidates": [],
            "risk_hints": [
                "backfill_unproven",
                "index_or_search_mapping_unproven",
                "projection_writer_missing"
            ],
            "missing_evidence": ["backfill", "index_or_search_mapping", "writer"],
        });

        let response = projection_impact_payload_as_context_pack_response(&payload, &scenario);

        assert_eq!(response.data.unresolved_gaps, vec!["writer".to_owned()]);
    }

    #[test]
    fn oracle_expectations_reject_unknown_fields() {
        let raw = r#"
name = "unknown_field"
mode = "planning"

[target]
kind = "symbol"
qn = "Target"

[oracle]
expected_files = []
forbidden_files = []
max_follow_ups = 1
min_confidence = 0
max_response_bytes = 1000
unknown_oracle_key = true
"#;

        let error = toml::from_str::<OracleScenario>(raw)
            .expect_err("unknown oracle keys must be rejected");
        assert!(
            error.to_string().contains("unknown field"),
            "expected unknown-field parse error; got {error}"
        );
    }
}
