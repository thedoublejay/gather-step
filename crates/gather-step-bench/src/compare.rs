#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use serde::{Deserialize, Serialize};

use crate::environment::{EnvironmentCapture, IndexSummary};

/// Full metadata snapshot written to `benchmark/results/<datetime>/<bench>.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    /// Execution environment at the time of the run.  `None` when loading a
    /// result produced before environment capture was introduced; new results
    /// always populate this field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub environment: Option<Environment>,
    /// ISO-8601 timestamp of when this result was recorded.
    pub date: String,
    /// Map from benchmark name to the number of samples collected.
    pub sample_sizes: BTreeMap<String, usize>,
    /// Optional Git ref or directory name of the comparison baseline.
    pub comparison_window: Option<String>,
    /// Benchmark-specific metric values as a JSON blob.
    pub metrics: serde_json::Value,
    /// Human-readable list of threshold rules that were evaluated.
    pub thresholds_applied: Vec<String>,
}

/// Reproducibility metadata describing the execution environment.
///
/// This struct is the serialised form written into result JSON files.  New
/// fields are always optional so that result files written before the field
/// existed continue to deserialise without error.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Environment {
    /// Operating system family, e.g. `"darwin"`, `"linux"`.
    pub os: String,
    /// Kernel release string from `uname -r` (e.g. `"25.4.0"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub os_version: Option<String>,
    /// CPU architecture, e.g. `"aarch64"`, `"x86_64"`.
    pub arch: String,
    /// Human-readable CPU model string (best effort).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_model: Option<String>,
    /// Number of logical CPU cores available to the process.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logical_cpus: Option<usize>,
    /// Number of physical CPU cores.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub physical_cpus: Option<usize>,
    /// Total system memory in bytes (best effort).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_memory_bytes: Option<u64>,
    /// Toolchain version string from `rustc -V`.
    pub rust_version: String,
    /// Git commit SHA of the bench binary's build source; set via the
    /// `GIT_COMMIT_SHA` environment variable in CI.  Falls back to `None` in
    /// local runs.
    pub commit_sha: Option<String>,
    /// HEAD commit SHA of the workspace at run time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_commit: Option<String>,
    /// `true` when the workspace had uncommitted changes at run time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_dirty: Option<bool>,
    /// First ten lines of `git status --porcelain` when dirty.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_dirty_summary: Option<Vec<String>>,
    /// `argv` of the bench process joined with spaces.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_line: Option<String>,
    /// High-level index counts when the run builds an index.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_summary: Option<IndexSummary>,
}

impl Environment {
    /// Build an [`Environment`] from a fully-populated [`EnvironmentCapture`].
    ///
    /// The `GIT_COMMIT_SHA` environment variable is read here to populate the
    /// `commit_sha` field used by CI provenance checks.
    #[must_use]
    pub fn from_capture(capture: EnvironmentCapture) -> Self {
        Self {
            os: capture.os,
            os_version: capture.os_version,
            arch: capture.arch,
            cpu_model: capture.cpu_model,
            logical_cpus: capture.logical_cpus,
            physical_cpus: capture.physical_cpus,
            total_memory_bytes: capture.total_memory_bytes,
            rust_version: capture.rust_version,
            commit_sha: std::env::var("GIT_COMMIT_SHA").ok(),
            workspace_commit: capture.workspace_commit,
            workspace_dirty: capture.workspace_dirty,
            workspace_dirty_summary: capture.workspace_dirty_summary,
            command_line: Some(redact_local_paths(&capture.command_line)),
            index_summary: capture.index_summary,
        }
    }

    /// Capture the current execution environment without a workspace root.
    ///
    /// This is a convenience wrapper over [`crate::environment::capture`] for
    /// call sites that do not have a workspace path readily available.  Git
    /// workspace fields will be `None`.
    #[must_use]
    pub fn current() -> Self {
        Self::from_capture(crate::environment::capture(None))
    }
}

/// Redact machine-local path prefixes before writing benchmark artifacts.
///
/// Benchmark JSON is often committed as release evidence. Keep enough shape to
/// reproduce the command while avoiding raw home and temp-directory paths.
#[must_use]
pub(crate) fn redact_local_paths(value: &str) -> String {
    let mut redacted = value.to_owned();
    redacted = redacted.replace("/private/tmp", "<tmp>");
    redacted = redacted.replace("/tmp", "<tmp>");
    let temp_dir = std::env::temp_dir().display().to_string();
    let temp_dir = temp_dir.trim_end_matches('/');
    if !temp_dir.is_empty() {
        redacted = redacted.replace(temp_dir, "<tmp>");
    }
    if let Ok(home) = std::env::var("HOME") {
        let home = home.trim_end_matches('/');
        if !home.is_empty() {
            redacted = redacted.replace(home, "<home>");
        }
    }
    redacted
}

/// Summary produced by [`compare_result_dirs`].
#[derive(Debug)]
pub struct ComparisonSummary {
    /// `true` when no regressions were detected.
    pub passed: bool,
    /// Human-readable lines to present to the user.
    pub lines: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct PlanningOracleScenarioSnapshot {
    name: String,
    passed: bool,
    completeness: String,
    resolution: String,
    findings: Vec<String>,
}

/// Load every `*.json` file directly inside `dir` and return them together
/// with their file stems.
///
/// # Errors
///
/// Returns an error when the directory cannot be read or a JSON file is
/// malformed.
fn load_results_from_dir(dir: &Path) -> anyhow::Result<Vec<(String, BenchmarkResult)>> {
    let mut results = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_owned();
        let raw = std::fs::read_to_string(&path)?;
        let result: BenchmarkResult = serde_json::from_str(&raw)?;
        results.push((stem, result));
    }
    Ok(results)
}

/// Compare two result directories and return a [`ComparisonSummary`].
///
/// Matches JSON files by stem (benchmark name). Reports missing files and
/// metric deltas. Comparison is currently qualitative (environment metadata
/// and metric key presence); numeric regression detection is delegated to the
/// CI step which compares against threshold gates.
///
/// # Errors
///
/// Returns an error when either directory cannot be read or a JSON file is
/// malformed.
pub fn compare_result_dirs(from: &Path, to: &Path) -> anyhow::Result<ComparisonSummary> {
    let from_results = load_results_from_dir(from)?;
    let to_results = load_results_from_dir(to)?;

    let from_map: BTreeMap<_, _> = from_results.into_iter().collect();
    let to_map: BTreeMap<_, _> = to_results.into_iter().collect();

    let mut all_passed = true;
    let mut lines = Vec::new();

    for (name, to_result) in &to_map {
        match from_map.get(name) {
            None => {
                lines.push(format!("[NEW]  {name}: no baseline to compare against"));
            }
            Some(from_result) => {
                let from_env = from_result.environment.as_ref();
                let to_env = to_result.environment.as_ref();
                let env_same = match (from_env, to_env) {
                    (Some(f), Some(t)) => f.os == t.os && f.arch == t.arch,
                    // If either side lacks captured environment metadata, treat
                    // it as compatible rather than flagging a spurious mismatch.
                    _ => true,
                };
                if !env_same {
                    let (f, t) = (from_env.unwrap(), to_env.unwrap());
                    lines.push(format!(
                        "[WARN] {name}: environment mismatch — \
                         baseline={}/{} current={}/{}",
                        f.os, f.arch, t.os, t.arch,
                    ));
                    all_passed = false;
                }

                // Compare metric keys to detect regressions in structure.
                if let (Some(from_obj), Some(to_obj)) = (
                    from_result.metrics.as_object(),
                    to_result.metrics.as_object(),
                ) {
                    for key in from_obj.keys() {
                        let baseline_value = from_obj.get(key);
                        if !to_obj.contains_key(key)
                            && !baseline_value.is_some_and(serde_json::Value::is_null)
                        {
                            lines.push(format!(
                                "[FAIL] {name}: metric `{key}` present in baseline but missing in current"
                            ));
                            all_passed = false;
                        }
                    }
                    compare_numeric_metrics(name, from_obj, to_obj, &mut lines, &mut all_passed);
                    compare_scenario_metrics(name, from_obj, to_obj, &mut lines, &mut all_passed);
                }

                if env_same {
                    lines.push(format!("[OK]   {name}: environments match"));
                }
            }
        }
    }

    for name in from_map.keys() {
        if !to_map.contains_key(name) {
            lines.push(format!(
                "[WARN] {name}: present in baseline but missing from current results"
            ));
        }
    }

    if all_passed {
        lines.push("Comparison complete: all checks passed.".to_owned());
    }

    Ok(ComparisonSummary {
        passed: all_passed,
        lines,
    })
}

fn compare_numeric_metrics(
    bench_name: &str,
    from_metrics: &serde_json::Map<String, serde_json::Value>,
    to_metrics: &serde_json::Map<String, serde_json::Value>,
    lines: &mut Vec<String>,
    all_passed: &mut bool,
) {
    for key in [
        "coverage",
        "top1_accuracy",
        "top3_accuracy",
        "mrr",
        "expected_file_recall",
        "expected_repo_recall",
        "event_resolution_success_rate",
        "stability_kendall_tau",
    ] {
        compare_lower_is_regression(bench_name, key, from_metrics, to_metrics, lines, all_passed);
    }
    for key in [
        "latency_p50_ms",
        "latency_p95_ms",
        "latency_p99_ms",
        "forbidden_hit_rate",
        "empty_result_rate",
        "unresolved_gap_rate",
    ] {
        compare_higher_is_regression(bench_name, key, from_metrics, to_metrics, lines, all_passed);
    }
}

fn compare_lower_is_regression(
    bench_name: &str,
    key: &str,
    from_metrics: &serde_json::Map<String, serde_json::Value>,
    to_metrics: &serde_json::Map<String, serde_json::Value>,
    lines: &mut Vec<String>,
    all_passed: &mut bool,
) {
    let Some(from_value) = metric_as_f64(from_metrics, key) else {
        return;
    };
    let Some(to_value) = metric_as_f64(to_metrics, key) else {
        return;
    };
    if to_value + f64::EPSILON < from_value {
        lines.push(format!(
            "[FAIL] {bench_name}: metric `{key}` regressed from {from_value:.3} to {to_value:.3}"
        ));
        *all_passed = false;
    }
}

fn compare_higher_is_regression(
    bench_name: &str,
    key: &str,
    from_metrics: &serde_json::Map<String, serde_json::Value>,
    to_metrics: &serde_json::Map<String, serde_json::Value>,
    lines: &mut Vec<String>,
    all_passed: &mut bool,
) {
    let Some(from_value) = metric_as_f64(from_metrics, key) else {
        return;
    };
    let Some(to_value) = metric_as_f64(to_metrics, key) else {
        return;
    };
    if to_value > from_value + f64::EPSILON {
        lines.push(format!(
            "[FAIL] {bench_name}: metric `{key}` regressed from {from_value:.3} to {to_value:.3}"
        ));
        *all_passed = false;
    }
}

fn metric_as_f64(metrics: &serde_json::Map<String, serde_json::Value>, key: &str) -> Option<f64> {
    metrics.get(key)?.as_f64()
}

fn compare_scenario_metrics(
    bench_name: &str,
    from_metrics: &serde_json::Map<String, serde_json::Value>,
    to_metrics: &serde_json::Map<String, serde_json::Value>,
    lines: &mut Vec<String>,
    all_passed: &mut bool,
) {
    let Some(from_scenarios) = scenario_snapshots(from_metrics) else {
        return;
    };
    let Some(to_scenarios) = scenario_snapshots(to_metrics) else {
        return;
    };

    let from_map: BTreeMap<_, _> = from_scenarios
        .into_iter()
        .map(|scenario| (scenario.name.clone(), scenario))
        .collect();
    let to_map: BTreeMap<_, _> = to_scenarios
        .into_iter()
        .map(|scenario| (scenario.name.clone(), scenario))
        .collect();

    for (scenario_name, to_scenario) in &to_map {
        match from_map.get(scenario_name) {
            None => {
                lines.push(format!(
                    "[NEW]  {bench_name}/{scenario_name}: no baseline scenario to compare against"
                ));
            }
            Some(from_scenario) => {
                compare_scenario_passed(
                    bench_name,
                    scenario_name,
                    from_scenario,
                    to_scenario,
                    lines,
                    all_passed,
                );
                compare_scenario_field_drift(
                    bench_name,
                    scenario_name,
                    "resolution",
                    &from_scenario.resolution,
                    &to_scenario.resolution,
                    lines,
                    all_passed,
                );
                compare_scenario_field_drift(
                    bench_name,
                    scenario_name,
                    "completeness",
                    &from_scenario.completeness,
                    &to_scenario.completeness,
                    lines,
                    all_passed,
                );
                compare_scenario_findings(
                    bench_name,
                    scenario_name,
                    &from_scenario.findings,
                    &to_scenario.findings,
                    lines,
                    all_passed,
                );
            }
        }
    }

    for scenario_name in from_map.keys() {
        if !to_map.contains_key(scenario_name) {
            lines.push(format!(
                "[FAIL] {bench_name}/{scenario_name}: baseline scenario missing from current results"
            ));
            *all_passed = false;
        }
    }
}

fn scenario_snapshots(
    metrics: &serde_json::Map<String, serde_json::Value>,
) -> Option<Vec<PlanningOracleScenarioSnapshot>> {
    serde_json::from_value(metrics.get("scenarios")?.clone()).ok()
}

fn compare_scenario_passed(
    bench_name: &str,
    scenario_name: &str,
    from_scenario: &PlanningOracleScenarioSnapshot,
    to_scenario: &PlanningOracleScenarioSnapshot,
    lines: &mut Vec<String>,
    all_passed: &mut bool,
) {
    match (from_scenario.passed, to_scenario.passed) {
        (true, false) => {
            lines.push(format!(
                "[FAIL] {bench_name}/{scenario_name}: scenario regressed from passed to failed"
            ));
            *all_passed = false;
        }
        (false, true) => {
            lines.push(format!(
                "[OK]   {bench_name}/{scenario_name}: scenario improved from failed to passed"
            ));
        }
        _ => {}
    }
}

fn compare_scenario_field_drift(
    bench_name: &str,
    scenario_name: &str,
    field_name: &str,
    from_value: &str,
    to_value: &str,
    lines: &mut Vec<String>,
    all_passed: &mut bool,
) {
    if from_value == to_value {
        return;
    }
    lines.push(format!(
        "[FAIL] {bench_name}/{scenario_name}: {field_name} drifted from `{from_value}` to `{to_value}`"
    ));
    *all_passed = false;
}

fn compare_scenario_findings(
    bench_name: &str,
    scenario_name: &str,
    from_findings: &[String],
    to_findings: &[String],
    lines: &mut Vec<String>,
    all_passed: &mut bool,
) {
    let from_set: BTreeSet<_> = from_findings.iter().map(String::as_str).collect();
    let new_findings: Vec<_> = to_findings
        .iter()
        .filter(|finding| !from_set.contains(finding.as_str()))
        .collect();

    for finding in new_findings {
        lines.push(format!(
            "[FAIL] {bench_name}/{scenario_name}: new finding `{finding}`"
        ));
        *all_passed = false;
    }
}

#[cfg(test)]
mod tests {
    use super::{BenchmarkResult, Environment, compare_result_dirs, redact_local_paths};
    use std::{fs, path::Path};

    fn write_result(dir: &Path, name: &str, metrics: serde_json::Value) {
        let result = BenchmarkResult {
            environment: Some(Environment {
                os: "macos".to_owned(),
                os_version: None,
                arch: "aarch64".to_owned(),
                cpu_model: None,
                logical_cpus: None,
                physical_cpus: None,
                total_memory_bytes: None,
                rust_version: "1.90".to_owned(),
                commit_sha: None,
                workspace_commit: None,
                workspace_dirty: None,
                workspace_dirty_summary: None,
                command_line: None,
                index_summary: None,
            }),
            date: "2026-04-20T00:00:00Z".to_owned(),
            sample_sizes: [("planning_oracle".to_owned(), 2)].into_iter().collect(),
            comparison_window: None,
            metrics,
            thresholds_applied: Vec::new(),
        };
        let raw = serde_json::to_string_pretty(&result).expect("serialize benchmark result");
        fs::write(dir.join(format!("{name}.json")), raw).expect("write benchmark result");
    }

    fn temp_result_dir(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "gather-step-bench-compare-{label}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[test]
    fn redact_local_paths_replaces_temp_and_home_prefixes() {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/home/example".to_owned());
        let value = format!("{home}/repo/bin --sample /tmp/gather-step/sample.json");
        let redacted = redact_local_paths(&value);
        assert!(!redacted.contains(&home));
        assert!(!redacted.contains("/tmp/gather-step"));
        assert!(redacted.contains("<home>/repo/bin"));
        assert!(redacted.contains("<tmp>/gather-step/sample.json"));
    }

    #[test]
    fn compare_result_dirs_flags_planning_oracle_scenario_regressions() {
        let from_dir = temp_result_dir("from");
        let to_dir = temp_result_dir("to");

        write_result(
            &from_dir,
            "planning_oracle",
            serde_json::json!({
                "coverage": 1.0,
                "top1_accuracy": 1.0,
                "top3_accuracy": 1.0,
                "mrr": 1.0,
                "expected_file_recall": 1.0,
                "expected_repo_recall": 1.0,
                "event_resolution_success_rate": 1.0,
                "stability_kendall_tau": 1.0,
                "latency_p50_ms": 10.0,
                "latency_p95_ms": 15.0,
                "latency_p99_ms": 20.0,
                "forbidden_hit_rate": 0.0,
                "empty_result_rate": 0.0,
                "unresolved_gap_rate": 0.0,
                "scenarios": [
                    {
                        "name": "stable",
                        "passed": true,
                        "completeness": "complete",
                        "resolution": "symbol_id",
                        "findings": []
                    },
                    {
                        "name": "removed",
                        "passed": true,
                        "completeness": "complete",
                        "resolution": "route_anchor",
                        "findings": []
                    }
                ]
            }),
        );

        write_result(
            &to_dir,
            "planning_oracle",
            serde_json::json!({
                "coverage": 1.0,
                "top1_accuracy": 1.0,
                "top3_accuracy": 1.0,
                "mrr": 1.0,
                "expected_file_recall": 1.0,
                "expected_repo_recall": 1.0,
                "event_resolution_success_rate": 1.0,
                "stability_kendall_tau": 1.0,
                "latency_p50_ms": 10.0,
                "latency_p95_ms": 15.0,
                "latency_p99_ms": 20.0,
                "forbidden_hit_rate": 0.0,
                "empty_result_rate": 0.0,
                "unresolved_gap_rate": 0.0,
                "scenarios": [
                    {
                        "name": "stable",
                        "passed": false,
                        "completeness": "partial",
                        "resolution": "search_resolved",
                        "findings": ["missing expected file `src/lib.rs`"]
                    },
                    {
                        "name": "new",
                        "passed": true,
                        "completeness": "complete",
                        "resolution": "symbol_id",
                        "findings": []
                    }
                ]
            }),
        );

        let summary = compare_result_dirs(&from_dir, &to_dir).expect("comparison succeeds");

        assert!(!summary.passed);
        assert!(summary.lines.iter().any(|line| {
            line.contains("planning_oracle/stable: scenario regressed from passed to failed")
        }));
        assert!(summary.lines.iter().any(|line| line.contains(
            "planning_oracle/stable: resolution drifted from `symbol_id` to `search_resolved`"
        )));
        assert!(summary.lines.iter().any(|line| line.contains(
            "planning_oracle/stable: completeness drifted from `complete` to `partial`"
        )));
        assert!(summary.lines.iter().any(|line| {
            line.contains(
                "planning_oracle/stable: new finding `missing expected file `src/lib.rs``",
            )
        }));
        assert!(summary.lines.iter().any(|line| {
            line.contains("planning_oracle/removed: baseline scenario missing from current results")
        }));
        assert!(summary.lines.iter().any(|line| {
            line.contains("planning_oracle/new: no baseline scenario to compare against")
        }));

        let _ = fs::remove_dir_all(from_dir);
        let _ = fs::remove_dir_all(to_dir);
    }

    #[test]
    fn compare_result_dirs_allows_scenario_improvement_without_regression() {
        let from_dir = temp_result_dir("improve-from");
        let to_dir = temp_result_dir("improve-to");

        write_result(
            &from_dir,
            "planning_oracle",
            serde_json::json!({
                "coverage": 1.0,
                "top1_accuracy": 1.0,
                "top3_accuracy": 1.0,
                "mrr": 1.0,
                "expected_file_recall": 1.0,
                "expected_repo_recall": 1.0,
                "event_resolution_success_rate": 1.0,
                "stability_kendall_tau": 1.0,
                "latency_p50_ms": 10.0,
                "latency_p95_ms": 15.0,
                "latency_p99_ms": 20.0,
                "forbidden_hit_rate": 0.0,
                "empty_result_rate": 0.0,
                "unresolved_gap_rate": 0.0,
                "scenarios": [
                    {
                        "name": "rescued",
                        "passed": false,
                        "completeness": "complete",
                        "resolution": "symbol_id",
                        "findings": ["transient failure"]
                    }
                ]
            }),
        );

        write_result(
            &to_dir,
            "planning_oracle",
            serde_json::json!({
                "coverage": 1.0,
                "top1_accuracy": 1.0,
                "top3_accuracy": 1.0,
                "mrr": 1.0,
                "expected_file_recall": 1.0,
                "expected_repo_recall": 1.0,
                "event_resolution_success_rate": 1.0,
                "stability_kendall_tau": 1.0,
                "latency_p50_ms": 10.0,
                "latency_p95_ms": 15.0,
                "latency_p99_ms": 20.0,
                "forbidden_hit_rate": 0.0,
                "empty_result_rate": 0.0,
                "unresolved_gap_rate": 0.0,
                "scenarios": [
                    {
                        "name": "rescued",
                        "passed": true,
                        "completeness": "complete",
                        "resolution": "symbol_id",
                        "findings": ["transient failure"]
                    }
                ]
            }),
        );

        let summary = compare_result_dirs(&from_dir, &to_dir).expect("comparison succeeds");

        assert!(summary.passed);
        assert!(summary.lines.iter().any(|line| {
            line.contains("planning_oracle/rescued: scenario improved from failed to passed")
        }));

        let _ = fs::remove_dir_all(from_dir);
        let _ = fs::remove_dir_all(to_dir);
    }

    #[test]
    fn compare_result_dirs_allows_missing_metric_when_baseline_value_was_null() {
        let from_dir = temp_result_dir("null-metric-from");
        let to_dir = temp_result_dir("null-metric-to");

        write_result(
            &from_dir,
            "index_pass",
            serde_json::json!({
                "parse_ms": 100,
                "graph_nodes": 10,
                "graph_edges": 20,
                "memory_rss_peak_bytes": null
            }),
        );
        write_result(
            &to_dir,
            "index_pass",
            serde_json::json!({
                "parse_ms": 100,
                "graph_nodes": 10,
                "graph_edges": 20,
                "memory_rss_growth_bytes": null
            }),
        );

        let summary = compare_result_dirs(&from_dir, &to_dir).expect("comparison succeeds");

        assert!(
            summary.passed,
            "missing unavailable baseline metric should not fail: {:?}",
            summary.lines
        );

        let _ = fs::remove_dir_all(from_dir);
        let _ = fs::remove_dir_all(to_dir);
    }
}
