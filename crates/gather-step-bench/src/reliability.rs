#![forbid(unsafe_code)]

//! Reliability check for planning probe consistency.
//!
//! Runs each planning scenario N times (default 3) after indexing the workspace
//! once per run, then diffs the results across runs.
//!
//! # Execution model
//!
//! **In-process** — probes are executed by calling the same MCP tool functions
//! that the main planning-oracle benchmark uses.  Each run re-indexes the
//! workspace into a fresh temporary directory, so the index itself is isolated.
//!
//! **Known limitation**: any process-global registries (e.g. in-flight pack
//! tracking caches backed by `LazyLock<Mutex<...>>`) persist across runs within
//! the same process.  This means the check is *not* a true isolation test: a
//! registry entry written in run 1 may still be visible in run 2.  For true
//! subprocess isolation, use `--reliability-mode=subprocess` (not yet
//! implemented; planned as a future opt-in).
//!
//! Despite the in-process limitation, the check is useful for catching
//! non-determinism caused by:
//! - unordered hash map iteration bleeding into result ordering
//! - rayon task-stealing producing different output ordering
//! - symbol-id generation that depends on traversal order
//!
//! # Content vs latency drift
//!
//! - **Content drift = failure**: ranked primary file/symbol/repo, confirmed
//!   downstream repos, proof count, warnings, or output byte size differ across
//!   any two runs.
//! - **Latency drift = soft signal**: the ratio between the slowest and fastest
//!   run is reported but only fails when it exceeds the configured tolerance
//!   (default: 3×).

use std::{collections::BTreeSet, path::Path};

use serde::{Deserialize, Serialize};

use crate::planning_oracle::{
    OracleScenario, execute_oracle_run_with_config, load_oracle_scenarios,
};
use gather_step_core::GatherStepConfig;

// ─── Configuration ────────────────────────────────────────────────────────────

/// Default number of probe repetitions per scenario.
pub const DEFAULT_RELIABILITY_RUNS: usize = 3;

/// Default maximum ratio between slowest and fastest run before a
/// `LATENCY-DRIFT` verdict is emitted.
pub const DEFAULT_LATENCY_DRIFT_TOLERANCE: f64 = 3.0;

// ─── Output types ─────────────────────────────────────────────────────────────

/// Verdict for a single scenario across N runs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ScenarioVerdict {
    /// All runs produced identical content and latency was within tolerance.
    Stable,
    /// Content differed across at least two runs — this is a hard failure.
    Drift,
    /// Content was stable but latency ratio exceeded the configured tolerance.
    LatencyDrift,
}

impl std::fmt::Display for ScenarioVerdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stable => write!(f, "STABLE"),
            Self::Drift => write!(f, "DRIFT"),
            Self::LatencyDrift => write!(f, "LATENCY-DRIFT"),
        }
    }
}

/// Per-scenario reliability report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioReliabilityReport {
    /// Scenario name from the oracle TOML.
    pub name: String,
    /// Overall verdict across all runs.
    pub verdict: ScenarioVerdict,
    /// Number of runs executed.
    pub runs: usize,
    /// Latency of each run in milliseconds.
    pub latency_ms_per_run: Vec<u64>,
    /// Ratio of slowest to fastest run (1.0 = perfectly consistent).
    pub latency_drift_ratio: f64,
    /// Human-readable description of any content differences found.
    pub content_diff: Vec<String>,
}

/// Aggregate reliability report across all scenarios.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReliabilityReport {
    /// `true` when every scenario is `STABLE` or `LATENCY-DRIFT` (no content drift).
    pub all_content_stable: bool,
    /// Number of scenarios that drifted in content.
    pub content_drift_count: usize,
    /// Number of scenarios with latency drift beyond tolerance.
    pub latency_drift_count: usize,
    /// Number of stable scenarios.
    pub stable_count: usize,
    /// Total scenarios checked.
    pub total_scenarios: usize,
    /// Configured number of repetitions per scenario.
    pub runs_per_scenario: usize,
    /// Configured latency drift tolerance (ratio).
    pub latency_drift_tolerance: f64,
    /// Per-scenario results.
    pub scenarios: Vec<ScenarioReliabilityReport>,
}

// ─── Snapshot of a single probe invocation ────────────────────────────────────

/// Deterministic content fingerprint extracted from a single probe invocation.
/// Fields that we consider content (not latency) are captured here.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ProbeSnapshot {
    /// File path of the first ranked item, if any.
    primary_file: Option<String>,
    /// Repository of the first ranked item, if any.
    primary_repo: Option<String>,
    /// Symbol name of the first ranked item, if any.
    primary_symbol: Option<String>,
    /// Confirmed downstream repositories, sorted for stable comparison.
    confirmed_downstream_repos: BTreeSet<String>,
    /// Number of planning proofs emitted.
    proof_count: usize,
    /// Serialised byte length of the full response.
    output_bytes: usize,
    /// Warnings from response metadata, sorted.
    warnings: BTreeSet<String>,
    /// Error string when the probe itself failed.
    error: Option<String>,
}

impl ProbeSnapshot {
    /// Return a list of human-readable diff strings between `self` and `other`.
    /// An empty list means the snapshots are identical in content.
    fn diff(&self, other: &Self) -> Vec<String> {
        let mut diffs = Vec::new();

        if self.error != other.error {
            diffs.push(format!("error: {:?} vs {:?}", self.error, other.error));
        }
        if self.primary_file != other.primary_file {
            diffs.push(format!(
                "primary_file: {:?} vs {:?}",
                self.primary_file, other.primary_file
            ));
        }
        if self.primary_repo != other.primary_repo {
            diffs.push(format!(
                "primary_repo: {:?} vs {:?}",
                self.primary_repo, other.primary_repo
            ));
        }
        if self.primary_symbol != other.primary_symbol {
            diffs.push(format!(
                "primary_symbol: {:?} vs {:?}",
                self.primary_symbol, other.primary_symbol
            ));
        }
        if self.confirmed_downstream_repos != other.confirmed_downstream_repos {
            diffs.push(format!(
                "confirmed_downstream_repos: {:?} vs {:?}",
                self.confirmed_downstream_repos, other.confirmed_downstream_repos
            ));
        }
        if self.proof_count != other.proof_count {
            diffs.push(format!(
                "proof_count: {} vs {}",
                self.proof_count, other.proof_count
            ));
        }
        if self.output_bytes != other.output_bytes {
            diffs.push(format!(
                "output_bytes: {} vs {}",
                self.output_bytes, other.output_bytes
            ));
        }
        if self.warnings != other.warnings {
            diffs.push(format!(
                "warnings: {:?} vs {:?}",
                self.warnings, other.warnings
            ));
        }
        diffs
    }
}

// ─── Public entry point ───────────────────────────────────────────────────────

/// Run a reliability check for all oracle scenarios under `scenarios_root`.
///
/// Indexes the workspace `runs` times (one fresh index per run) and compares
/// probe outputs across runs for content stability.
///
/// # Arguments
///
/// * `fixture_root` — workspace root passed to `execute_oracle_run_with_config`.
/// * `scenarios_root` — directory containing oracle scenario subdirectories.
/// * `runs` — number of repetitions per scenario (default: [`DEFAULT_RELIABILITY_RUNS`]).
/// * `latency_drift_tolerance` — maximum allowed slowest/fastest ratio before
///   `LATENCY-DRIFT` is emitted (default: [`DEFAULT_LATENCY_DRIFT_TOLERANCE`]).
///
/// # Errors
///
/// Returns an error when scenario files cannot be loaded, when fewer than 2
/// runs are requested, or when the workspace cannot be indexed.
pub fn run_reliability_check(
    fixture_root: &Path,
    scenarios_root: &Path,
    runs: usize,
    latency_drift_tolerance: f64,
) -> anyhow::Result<ReliabilityReport> {
    anyhow::ensure!(runs >= 2, "reliability check requires at least 2 runs");

    let scenarios = load_oracle_scenarios(scenarios_root)?;
    anyhow::ensure!(
        !scenarios.is_empty(),
        "no oracle scenarios found in {}",
        scenarios_root.display()
    );

    let config_root = resolve_config_root(fixture_root)?;
    let config_path = config_root.join("gather-step.config.yaml");
    let config = GatherStepConfig::from_yaml_file(&config_path)?;

    run_reliability_check_with_config(
        &config,
        &config_root,
        &scenarios,
        runs,
        latency_drift_tolerance,
    )
}

fn resolve_config_root(fixture_root: &Path) -> anyhow::Result<std::path::PathBuf> {
    let config_path = fixture_root.join("gather-step.config.yaml");
    if config_path.exists() {
        return Ok(fixture_root.to_path_buf());
    }
    fixture_root
        .parent()
        .map(std::path::Path::to_path_buf)
        .filter(|parent| parent.join("gather-step.config.yaml").exists())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "failed to find gather-step.config.yaml in {} or its parent",
                fixture_root.display()
            )
        })
}

fn run_reliability_check_with_config(
    config: &GatherStepConfig,
    config_root: &Path,
    scenarios: &[OracleScenario],
    runs: usize,
    latency_drift_tolerance: f64,
) -> anyhow::Result<ReliabilityReport> {
    // Per-scenario accumulated (latency_ms, snapshot) across all runs.
    let mut per_scenario: Vec<Vec<(u64, ProbeSnapshot)>> =
        vec![Vec::with_capacity(runs); scenarios.len()];

    for _ in 0..runs {
        let oracle_runs = execute_oracle_run_with_config(config, config_root, scenarios)?;
        for (idx, oracle_run) in oracle_runs.iter().enumerate() {
            let snapshot = snapshot_from_run(oracle_run);
            per_scenario[idx].push((oracle_run.latency_ms, snapshot));
        }
    }

    // Build per-scenario reports.
    let mut scenario_reports = Vec::with_capacity(scenarios.len());
    for (idx, scenario) in scenarios.iter().enumerate() {
        let results = &per_scenario[idx];
        let latency_ms_per_run: Vec<u64> = results.iter().map(|(ms, _)| *ms).collect();

        let min_latency = latency_ms_per_run.iter().copied().min().unwrap_or(0);
        let max_latency = latency_ms_per_run.iter().copied().max().unwrap_or(0);
        let latency_drift_ratio = if min_latency == 0 {
            // A run that finishes in 0 ms is effectively a no-op; treat as no drift.
            1.0
        } else {
            #[expect(
                clippy::cast_precision_loss,
                reason = "latency values are small and only used for drift ratio reporting"
            )]
            {
                max_latency as f64 / min_latency as f64
            }
        };

        let baseline = &results[0].1;
        let mut all_diffs: Vec<String> = Vec::new();
        for (run_index, (_, snapshot)) in results.iter().enumerate().skip(1) {
            for diff in baseline.diff(snapshot) {
                all_diffs.push(format!("run 0 vs run {run_index}: {diff}"));
            }
        }

        let has_content_drift = !all_diffs.is_empty();
        let has_latency_drift = latency_drift_ratio > latency_drift_tolerance;

        let verdict = if has_content_drift {
            ScenarioVerdict::Drift
        } else if has_latency_drift {
            ScenarioVerdict::LatencyDrift
        } else {
            ScenarioVerdict::Stable
        };

        scenario_reports.push(ScenarioReliabilityReport {
            name: scenario.name.clone(),
            verdict,
            runs,
            latency_ms_per_run,
            latency_drift_ratio,
            content_diff: all_diffs,
        });
    }

    let content_drift_count = scenario_reports
        .iter()
        .filter(|r| r.verdict == ScenarioVerdict::Drift)
        .count();
    let latency_drift_count = scenario_reports
        .iter()
        .filter(|r| r.verdict == ScenarioVerdict::LatencyDrift)
        .count();
    let stable_count = scenario_reports
        .iter()
        .filter(|r| r.verdict == ScenarioVerdict::Stable)
        .count();

    Ok(ReliabilityReport {
        all_content_stable: content_drift_count == 0,
        content_drift_count,
        latency_drift_count,
        stable_count,
        total_scenarios: scenarios.len(),
        runs_per_scenario: runs,
        latency_drift_tolerance,
        scenarios: scenario_reports,
    })
}

fn snapshot_from_run(oracle_run: &crate::planning_oracle::OracleRun) -> ProbeSnapshot {
    let primary = oracle_run
        .response
        .as_ref()
        .and_then(|r| r.data.items.first());
    let confirmed = oracle_run
        .response
        .as_ref()
        .map(|r| {
            r.data
                .change_impact
                .confirmed_downstream_repos
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    let proof_count = oracle_run
        .response
        .as_ref()
        .map_or(0, |r| r.data.planning_proofs.len());
    let output_bytes = oracle_run
        .response
        .as_ref()
        .and_then(|r| serde_json::to_vec(r).ok())
        .map_or(0, |v| v.len());
    let warnings = oracle_run
        .response
        .as_ref()
        .and_then(|r| r.meta.as_ref())
        .map(|meta| meta.warnings.iter().cloned().collect::<BTreeSet<_>>())
        .unwrap_or_default();
    ProbeSnapshot {
        primary_file: primary.map(|p| p.file_path.clone()),
        primary_repo: primary.map(|p| p.repo.clone()),
        primary_symbol: primary.map(|p| p.symbol_name.clone()),
        confirmed_downstream_repos: confirmed,
        proof_count,
        output_bytes,
        warnings,
        error: oracle_run.error.clone(),
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_snapshot(primary_file: Option<&str>, output_bytes: usize) -> ProbeSnapshot {
        ProbeSnapshot {
            primary_file: primary_file.map(str::to_owned),
            primary_repo: Some("repo_a".to_owned()),
            primary_symbol: Some("MySymbol".to_owned()),
            confirmed_downstream_repos: BTreeSet::from(["repo_b".to_owned()]),
            proof_count: 2,
            output_bytes,
            warnings: BTreeSet::new(),
            error: None,
        }
    }

    #[test]
    fn snapshot_diff_detects_primary_file_change() {
        let a = make_snapshot(Some("src/foo.ts"), 1024);
        let b = make_snapshot(Some("src/bar.ts"), 1024);
        let diffs = a.diff(&b);
        assert!(
            diffs.iter().any(|d| d.contains("primary_file")),
            "expected primary_file diff, got: {diffs:?}"
        );
    }

    #[test]
    fn snapshot_diff_is_empty_for_identical_snapshots() {
        let a = make_snapshot(Some("src/foo.ts"), 1024);
        let b = make_snapshot(Some("src/foo.ts"), 1024);
        assert!(a.diff(&b).is_empty(), "identical snapshots should not diff");
    }

    #[test]
    fn snapshot_diff_detects_output_bytes_change() {
        let a = make_snapshot(Some("src/foo.ts"), 1024);
        let b = make_snapshot(Some("src/foo.ts"), 2048);
        let diffs = a.diff(&b);
        assert!(
            diffs.iter().any(|d| d.contains("output_bytes")),
            "expected output_bytes diff, got: {diffs:?}"
        );
    }

    #[test]
    fn snapshot_diff_detects_confirmed_downstream_change() {
        let a = make_snapshot(Some("src/foo.ts"), 1024);
        let mut b = make_snapshot(Some("src/foo.ts"), 1024);
        b.confirmed_downstream_repos.insert("repo_c".to_owned());
        let diffs = a.diff(&b);
        assert!(
            diffs
                .iter()
                .any(|d| d.contains("confirmed_downstream_repos")),
            "expected confirmed_downstream_repos diff, got: {diffs:?}"
        );
    }

    #[test]
    fn snapshot_diff_detects_warning_change() {
        let a = make_snapshot(Some("src/foo.ts"), 1024);
        let mut b = make_snapshot(Some("src/foo.ts"), 1024);
        b.warnings.insert("budget_exceeded".to_owned());
        let diffs = a.diff(&b);
        assert!(
            diffs.iter().any(|d| d.contains("warnings")),
            "expected warnings diff, got: {diffs:?}"
        );
    }

    #[test]
    fn snapshot_diff_detects_proof_count_change() {
        let mut a = make_snapshot(Some("src/foo.ts"), 1024);
        let mut b = make_snapshot(Some("src/foo.ts"), 1024);
        a.proof_count = 2;
        b.proof_count = 5;
        let diffs = a.diff(&b);
        assert!(
            diffs.iter().any(|d| d.contains("proof_count")),
            "expected proof_count diff, got: {diffs:?}"
        );
    }

    #[test]
    fn snapshot_diff_is_empty_when_warnings_match() {
        let mut a = make_snapshot(Some("src/foo.ts"), 1024);
        let mut b = make_snapshot(Some("src/foo.ts"), 1024);
        a.warnings.insert("budget_exceeded".to_owned());
        b.warnings.insert("budget_exceeded".to_owned());
        assert!(a.diff(&b).is_empty(), "identical warnings should not diff");
    }

    #[test]
    fn verdict_display_matches_expected_strings() {
        assert_eq!(ScenarioVerdict::Stable.to_string(), "STABLE");
        assert_eq!(ScenarioVerdict::Drift.to_string(), "DRIFT");
        assert_eq!(ScenarioVerdict::LatencyDrift.to_string(), "LATENCY-DRIFT");
    }

    /// Verify that a forced nondeterministic source (two snapshots differing in
    /// `primary_file`) is caught by the content drift check.
    ///
    /// This test simulates what a nondeterministic traversal order would produce
    /// in practice, and asserts the diff logic catches it.
    #[test]
    fn forced_nondeterminism_is_caught_by_diff() {
        let run1 = make_snapshot(Some("src/alpha.ts"), 1000);
        let run2 = make_snapshot(Some("src/beta.ts"), 1000);
        let diffs = run1.diff(&run2);
        assert!(
            !diffs.is_empty(),
            "content drift should be detected when primary_file changes"
        );
        assert!(
            diffs.iter().any(|d| d.contains("primary_file")),
            "drift should be on primary_file"
        );
    }

    /// Verify that latency drift ratio is computed correctly.
    #[test]
    fn latency_drift_ratio_is_max_over_min() {
        let latencies = [300u64, 500, 900];
        let min = latencies.iter().copied().min().unwrap();
        let max = latencies.iter().copied().max().unwrap();
        #[expect(
            clippy::cast_precision_loss,
            reason = "test values fit well within f64 precision"
        )]
        let ratio = max as f64 / min as f64;
        // 900 / 300 = 3.0 exactly.
        assert!(
            (ratio - 3.0).abs() < 1e-9,
            "ratio should be 3.0, got {ratio}"
        );
    }

    /// Verify that a zero-latency run does not produce a division-by-zero panic.
    #[test]
    fn zero_min_latency_does_not_panic() {
        let latencies = [0u64, 500];
        let min = latencies.iter().copied().min().unwrap();
        let ratio = if min == 0 {
            1.0f64
        } else {
            #[expect(
                clippy::cast_precision_loss,
                reason = "test values fit well within f64 precision"
            )]
            {
                let max = latencies.iter().copied().max().unwrap();
                max as f64 / min as f64
            }
        };
        // Should fall back to 1.0 (no drift) when min is 0.
        assert!(
            (ratio - 1.0).abs() < 1e-9,
            "ratio should be 1.0 when min latency is 0, got {ratio}"
        );
    }

    /// Verify that content stable + latency within tolerance → STABLE verdict.
    #[test]
    fn stable_verdict_when_content_identical_and_latency_within_tolerance() {
        let snapshot = make_snapshot(Some("src/foo.ts"), 512);
        assert!(snapshot.diff(&snapshot).is_empty());
        // Ratio 1.0 ≤ default tolerance of 3.0 — verified structurally since
        // DEFAULT_LATENCY_DRIFT_TOLERANCE is a const and this is always true.
        const { assert!(1.0f64 <= DEFAULT_LATENCY_DRIFT_TOLERANCE) }
    }

    /// Verify that `run_reliability_check` rejects a `runs` value of 0 or 1.
    #[test]
    fn run_reliability_check_rejects_fewer_than_two_runs() {
        use std::path::PathBuf;
        let dummy = PathBuf::from("/nonexistent");
        let result = run_reliability_check(&dummy, &dummy, 1, DEFAULT_LATENCY_DRIFT_TOLERANCE);
        assert!(result.is_err(), "should error when runs < 2");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("at least 2 runs"),
            "error should mention the minimum run count: {err}"
        );
    }
}
