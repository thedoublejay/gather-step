#![forbid(unsafe_code)]

//! Machine-executable release gate for benchmark quality criteria.
//!
//! The gate reads a [`PlanningOracleReport`] JSON artifact and an optional
//! PR-oracle score artifact, then evaluates four criteria:
//!
//! 1. **Anchor resolution** — every planning anchor resolves via `exact`,
//!    `ranked`, or `rescue`; never `ambiguous_search_match` or absent.
//! 2. **Cross-repo evidence** — scenarios that declare cross-repo expectations
//!    (`cross_repo_expected == true`) each confirm at least one downstream repo.
//! 3. **No advisory domination** — for shared-contract impact scenarios the
//!    fraction of confirmed repos backed only by co-change advisory proofs is
//!    below 50 %.
//! 4. **PR-oracle F1 threshold** — median F1 ≥ 0.75 and median recall ≥ 0.70
//!    (skipped gracefully when no result artifact is supplied).

use std::path::Path;

use anyhow::Context;
use gather_step_core::high_contract::{
    HIGH_SCENARIO_CONTRACTS, MAX_ADVISORY_ONLY_FRACTION, MIN_PR_ORACLE_MEDIAN_F1,
    MIN_PR_ORACLE_MEDIAN_RECALL, MIN_PROOF_PRECISION, MIN_PROOF_RECALL,
    VALID_HIGH_RESOLUTION_STRATEGIES, normalize_high_contract_name,
};
use serde::{Deserialize, Serialize};

use crate::{planning_oracle::PlanningOracleReport, pr_oracle::ScoreArtifact};

// ─── Output types ─────────────────────────────────────────────────────────────

/// Verdict for a single gate criterion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CriterionVerdict {
    /// Short identifier for the criterion.
    pub name: String,
    /// Whether the criterion passed.
    pub passed: bool,
    /// One-line human-readable explanation of the verdict with relevant numbers.
    pub message: String,
}

/// Aggregate result of all gate criteria.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateResult {
    /// True when every criterion passed.
    pub all_passed: bool,
    /// Individual criterion verdicts, in evaluation order.
    pub criteria: Vec<CriterionVerdict>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct GateOptions {
    /// When true, missing PR-oracle score artifacts fail criterion 4 instead of
    /// being treated as a graceful skip.
    pub require_pr_oracle: bool,
}

// ─── Public evaluation entry point ────────────────────────────────────────────

/// Evaluate all gate criteria against the supplied `report` and optional
/// `pr_oracle_result`.
///
/// Returns a [`GateResult`] summarising every criterion's verdict.  The caller
/// is responsible for printing the verdicts and choosing the process exit code.
///
/// # Errors
///
/// Returns an error only when the report JSON cannot be loaded or parsed.
pub fn evaluate_report_path(
    report_path: &Path,
    pr_oracle_result_path: Option<&Path>,
) -> anyhow::Result<GateResult> {
    evaluate_report_path_with_options(report_path, pr_oracle_result_path, GateOptions::default())
}

pub fn evaluate_report_path_with_options(
    report_path: &Path,
    pr_oracle_result_path: Option<&Path>,
    options: GateOptions,
) -> anyhow::Result<GateResult> {
    let report = load_planning_oracle_report(report_path)?;
    let pr_oracle = pr_oracle_result_path.map(load_score_artifact).transpose()?;
    Ok(evaluate_with_options(&report, pr_oracle.as_ref(), options))
}

/// Evaluate all gate criteria against an already-loaded report.
///
/// `pr_oracle` may be `None`; in that case criterion 4 is skipped and marked
/// as passing (graceful degradation when no score artifact has been produced
/// yet).
#[must_use]
pub fn evaluate(report: &PlanningOracleReport, pr_oracle: Option<&ScoreArtifact>) -> GateResult {
    evaluate_with_options(report, pr_oracle, GateOptions::default())
}

#[must_use]
pub fn evaluate_with_options(
    report: &PlanningOracleReport,
    pr_oracle: Option<&ScoreArtifact>,
    options: GateOptions,
) -> GateResult {
    let criteria = vec![
        check_high_contract_coverage(report),
        check_scenario_oracles(report),
        check_anchor_resolution(report),
        check_cross_repo_evidence(report),
        check_advisory_domination(report),
        check_proof_precision(report),
        check_proof_recall(report),
        check_pr_oracle_f1(pr_oracle, options.require_pr_oracle),
    ];

    let all_passed = criteria.iter().all(|c| c.passed);
    GateResult {
        all_passed,
        criteria,
    }
}

// ─── Criterion 0: required HIGH scenario coverage ────────────────────────────

fn check_high_contract_coverage(report: &PlanningOracleReport) -> CriterionVerdict {
    let observed_raw = report
        .scenarios
        .iter()
        .map(|scenario| scenario.name.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let observed = observed_raw
        .iter()
        .map(|name| normalize_high_contract_name(name))
        .collect::<std::collections::BTreeSet<_>>();
    let missing = HIGH_SCENARIO_CONTRACTS
        .iter()
        .filter(|contract| {
            !observed.contains(&normalize_high_contract_name(contract.scenario_name))
        })
        .map(|contract| contract.scenario_name)
        .collect::<Vec<_>>();

    if missing.is_empty() {
        return CriterionVerdict {
            name: "high_contract_coverage".to_owned(),
            passed: true,
            message: format!(
                "[PASS] high_contract_coverage: all {} required HIGH scenarios are present",
                HIGH_SCENARIO_CONTRACTS.len()
            ),
        };
    }

    CriterionVerdict {
        name: "high_contract_coverage".to_owned(),
        passed: false,
        message: format!(
            "[FAIL] high_contract_coverage: missing required HIGH scenario(s): {}; observed={observed_raw:?}",
            missing.join(", "),
        ),
    }
}

// ─── Criterion 1: scenario-level oracle assertions ───────────────────────────

fn check_scenario_oracles(report: &PlanningOracleReport) -> CriterionVerdict {
    let failing = report
        .scenarios
        .iter()
        .filter(|scenario| !scenario.passed || !scenario.findings.is_empty())
        .collect::<Vec<_>>();

    if failing.is_empty() {
        return CriterionVerdict {
            name: "scenario_oracles".to_owned(),
            passed: true,
            message: format!(
                "[PASS] scenario_oracles: all {} scenario-level oracle assertions passed",
                report.scenarios.len()
            ),
        };
    }

    let names = failing
        .iter()
        .map(|scenario| scenario.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let message = format!(
        "[FAIL] scenario_oracles: {}/{} scenario(s) failed oracle assertions: {}",
        failing.len(),
        report.scenarios.len(),
        names
    );

    CriterionVerdict {
        name: "scenario_oracles".to_owned(),
        passed: false,
        message,
    }
}

// ─── Criterion 2: anchor resolution ──────────────────────────────────────────

fn check_anchor_resolution(report: &PlanningOracleReport) -> CriterionVerdict {
    let mut bad: Vec<&str> = Vec::new();

    for scenario in &report.scenarios {
        let strategy = scenario.resolution.as_str();
        if !VALID_HIGH_RESOLUTION_STRATEGIES.contains(&strategy) {
            bad.push(scenario.name.as_str());
        }
    }

    let total = report.scenarios.len();
    let passed = bad.is_empty();
    let message = if passed {
        format!(
            "[PASS] anchor_resolution: all {total} scenarios resolved via \
             exact/ranked/rescue"
        )
    } else {
        format!(
            "[FAIL] anchor_resolution: {}/{total} scenarios have invalid resolution \
             strategy: {}",
            bad.len(),
            bad.join(", ")
        )
    };

    CriterionVerdict {
        name: "anchor_resolution".to_owned(),
        passed,
        message,
    }
}

// ─── Criterion 3: cross-repo evidence ────────────────────────────────────────

fn check_cross_repo_evidence(report: &PlanningOracleReport) -> CriterionVerdict {
    // Scenarios that declare cross-repo expectations but have zero confirmed
    // downstream repos.
    let mut failing: Vec<&str> = Vec::new();
    let mut total_expected = 0usize;

    for scenario in &report.scenarios {
        if !scenario.cross_repo_expected {
            continue;
        }
        total_expected += 1;
        if scenario.confirmed_downstream_count == 0 {
            failing.push(scenario.name.as_str());
        }
    }

    if total_expected == 0 {
        return CriterionVerdict {
            name: "cross_repo_evidence".to_owned(),
            passed: true,
            message: "[PASS] cross_repo_evidence: no scenarios declare cross-repo expectations \
                      (criterion not applicable)"
                .to_owned(),
        };
    }

    let passed = failing.is_empty();
    let message = if passed {
        format!(
            "[PASS] cross_repo_evidence: all {total_expected} cross-repo scenarios \
             have confirmed downstream repos"
        )
    } else {
        format!(
            "[FAIL] cross_repo_evidence: {}/{total_expected} cross-repo scenarios \
             have zero confirmed downstream repos: {}",
            failing.len(),
            failing.join(", ")
        )
    };

    CriterionVerdict {
        name: "cross_repo_evidence".to_owned(),
        passed,
        message,
    }
}

// ─── Criterion 4: no advisory domination for shared-contract tasks ──────────

fn check_advisory_domination(report: &PlanningOracleReport) -> CriterionVerdict {
    // Collect shared-contract scenarios that supply an advisory_only_repo_fraction.
    let mut dominated: Vec<(&str, f64)> = Vec::new();
    let mut evaluated = 0usize;

    for scenario in &report.scenarios {
        if !scenario.is_shared_contract_task {
            continue;
        }
        let Some(fraction) = scenario.advisory_only_repo_fraction else {
            // No fraction data — skip this scenario (cannot evaluate).
            continue;
        };
        evaluated += 1;
        if fraction >= MAX_ADVISORY_ONLY_FRACTION {
            dominated.push((scenario.name.as_str(), fraction));
        }
    }

    // Also check the aggregate SplitMetrics.advisory_leak_rate when present.
    // advisory_leak_rate >= MAX_ADVISORY_ONLY_FRACTION is a fail even if
    // per-scenario fractions are absent.
    let aggregate_fail = report
        .split
        .advisory_leak_rate
        .is_some_and(|rate| f64::from(rate) >= MAX_ADVISORY_ONLY_FRACTION);

    // Confirmed repos that lack any planning_proofs entry are not
    // "non-advisory" — they are unverified.  The same threshold applies:
    // when the unexplained-confirmed fraction crosses 50%, treat the gate
    // as failing regardless of advisory leak.  This stops a deferred-proof
    // state from silently turning into a green criterion.
    let unexplained_fail = report
        .split
        .unexplained_confirmed_repo_fraction
        .is_some_and(|rate| f64::from(rate) >= MAX_ADVISORY_ONLY_FRACTION);

    if evaluated == 0 && !aggregate_fail && !unexplained_fail {
        return CriterionVerdict {
            name: "no_advisory_domination".to_owned(),
            passed: true,
            message: "[PASS] no_advisory_domination: no shared-contract scenarios with \
                      advisory fraction data (criterion not applicable)"
                .to_owned(),
        };
    }

    let passed = dominated.is_empty() && !aggregate_fail && !unexplained_fail;
    let message = if passed {
        format!(
            "[PASS] no_advisory_domination: {evaluated} shared-contract scenario(s) \
             all below advisory threshold ({:.0}%)",
            MAX_ADVISORY_ONLY_FRACTION * 100.0
        )
    } else {
        let mut parts = Vec::new();
        if !dominated.is_empty() {
            let detail: Vec<String> = dominated
                .iter()
                .map(|(name, frac)| format!("{name}={:.0}%", frac * 100.0))
                .collect();
            parts.push(format!(
                "{} scenario(s) advisory-dominated: {}",
                dominated.len(),
                detail.join(", ")
            ));
        }
        if aggregate_fail {
            let rate = report.split.advisory_leak_rate.map_or(0.0, f64::from);
            parts.push(format!(
                "aggregate advisory_leak_rate={:.1}% >= threshold {:.0}%",
                rate * 100.0,
                MAX_ADVISORY_ONLY_FRACTION * 100.0
            ));
        }
        if unexplained_fail {
            let rate = report
                .split
                .unexplained_confirmed_repo_fraction
                .map_or(0.0, f64::from);
            parts.push(format!(
                "unexplained_confirmed_repo_fraction={:.1}% >= threshold {:.0}% \
                 (confirmed repos without planning_proofs)",
                rate * 100.0,
                MAX_ADVISORY_ONLY_FRACTION * 100.0
            ));
        }
        format!("[FAIL] no_advisory_domination: {}", parts.join("; "))
    };

    CriterionVerdict {
        name: "no_advisory_domination".to_owned(),
        passed,
        message,
    }
}

// ─── Criterion 5: aggregate proof_precision ─────────────────────────────────

fn check_proof_precision(report: &PlanningOracleReport) -> CriterionVerdict {
    let Some(precision) = report.split.proof_precision else {
        return CriterionVerdict {
            name: "proof_precision".to_owned(),
            passed: true,
            message: "[PASS] proof_precision: not reported by aggregate (criterion not applicable)"
                .to_owned(),
        };
    };
    let value = f64::from(precision);
    let passed = value >= MIN_PROOF_PRECISION;
    let message = if passed {
        format!(
            "[PASS] proof_precision: aggregate {value:.3} >= threshold {MIN_PROOF_PRECISION:.2}"
        )
    } else {
        format!("[FAIL] proof_precision: aggregate {value:.3} < threshold {MIN_PROOF_PRECISION:.2}")
    };
    CriterionVerdict {
        name: "proof_precision".to_owned(),
        passed,
        message,
    }
}

// ─── Criterion 6: aggregate proof_recall ─────────────────────────────────────

fn check_proof_recall(report: &PlanningOracleReport) -> CriterionVerdict {
    let Some(recall) = report.split.proof_recall else {
        return CriterionVerdict {
            name: "proof_recall".to_owned(),
            passed: true,
            message: "[PASS] proof_recall: not reported by aggregate (criterion not applicable)"
                .to_owned(),
        };
    };
    let value = f64::from(recall);
    let passed = value >= MIN_PROOF_RECALL;
    let message = if passed {
        format!("[PASS] proof_recall: aggregate {value:.3} >= threshold {MIN_PROOF_RECALL:.2}")
    } else {
        format!("[FAIL] proof_recall: aggregate {value:.3} < threshold {MIN_PROOF_RECALL:.2}")
    };
    CriterionVerdict {
        name: "proof_recall".to_owned(),
        passed,
        message,
    }
}

// ─── Criterion 7: PR-oracle F1 threshold ─────────────────────────────────────

fn check_pr_oracle_f1(pr_oracle: Option<&ScoreArtifact>, required: bool) -> CriterionVerdict {
    let Some(artifact) = pr_oracle else {
        if required {
            return CriterionVerdict {
                name: "pr_oracle_f1".to_owned(),
                passed: false,
                message:
                    "[FAIL] pr_oracle_f1: no score artifact supplied; rerun with --pr-oracle-result"
                        .to_owned(),
            };
        }
        return CriterionVerdict {
            name: "pr_oracle_f1".to_owned(),
            passed: true,
            message: "[PASS] pr_oracle_f1: no score artifact supplied — criterion skipped"
                .to_owned(),
        };
    };

    let f1_ok = artifact.median_f1 >= MIN_PR_ORACLE_MEDIAN_F1;
    let recall_ok = artifact.median_recall >= MIN_PR_ORACLE_MEDIAN_RECALL;
    let passed = f1_ok && recall_ok;

    let message = if passed {
        format!(
            "[PASS] pr_oracle_f1: median_f1={:.3} >= {:.2}, median_recall={:.3} >= {:.2}",
            artifact.median_f1,
            MIN_PR_ORACLE_MEDIAN_F1,
            artifact.median_recall,
            MIN_PR_ORACLE_MEDIAN_RECALL
        )
    } else {
        let mut failures = Vec::new();
        if !f1_ok {
            failures.push(format!(
                "median_f1={:.3} < {:.2}",
                artifact.median_f1, MIN_PR_ORACLE_MEDIAN_F1
            ));
        }
        if !recall_ok {
            failures.push(format!(
                "median_recall={:.3} < {:.2}",
                artifact.median_recall, MIN_PR_ORACLE_MEDIAN_RECALL
            ));
        }
        format!("[FAIL] pr_oracle_f1: {}", failures.join("; "))
    };

    CriterionVerdict {
        name: "pr_oracle_f1".to_owned(),
        passed,
        message,
    }
}

// ─── I/O helpers ─────────────────────────────────────────────────────────────

fn load_planning_oracle_report(path: &Path) -> anyhow::Result<PlanningOracleReport> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading planning oracle report from {}", path.display()))?;
    parse_planning_oracle_report(&raw)
}

fn parse_planning_oracle_report(raw: &str) -> anyhow::Result<PlanningOracleReport> {
    match serde_json::from_str::<PlanningOracleReport>(raw) {
        Ok(report) => Ok(report),
        Err(raw_report_error) => {
            let value = serde_json::from_str::<serde_json::Value>(raw)
                .context("parsing planning oracle report JSON")?;
            let Some(metrics) = value.get("metrics") else {
                return Err(raw_report_error).context("parsing planning oracle report JSON");
            };
            serde_json::from_value::<PlanningOracleReport>(metrics.clone())
                .context("parsing planning oracle report from benchmark metrics")
        }
    }
}

fn load_score_artifact(path: &Path) -> anyhow::Result<ScoreArtifact> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading score artifact from {}", path.display()))?;
    serde_json::from_str(&raw).context("parsing score artifact JSON")
}

/// Public helper that loads a PR-oracle score artifact and runs the F1 /
/// recall criterion against it.
///
/// Used by the release-gate orchestrator to incorporate the PR-oracle
/// criterion into the multi-probe gate report. Returns the loaded artifact
/// alongside the verdict so callers can surface the underlying numbers in
/// their structured output.
///
/// # Errors
///
/// Returns an error only when the artifact path is provided but cannot be
/// read or parsed. A missing path with `required = false` returns a
/// PASS-skipped verdict; a missing path with `required = true` returns a
/// FAIL verdict.
pub fn run_pr_oracle_criterion(
    pr_oracle_result_path: Option<&Path>,
    required: bool,
) -> anyhow::Result<(CriterionVerdict, Option<ScoreArtifact>)> {
    let artifact = pr_oracle_result_path.map(load_score_artifact).transpose()?;
    let verdict = check_pr_oracle_f1(artifact.as_ref(), required);
    Ok((verdict, artifact))
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        planning_oracle::{PlanningOracleScenarioReport, SplitMetrics},
        pr_oracle::{ChangeCategoryLabel, DiffSizeBucket, PrScore, StratumAggregate},
    };

    // ── helpers ──────────────────────────────────────────────────────────────

    fn passing_scenario(name: &str) -> PlanningOracleScenarioReport {
        PlanningOracleScenarioReport {
            name: name.to_owned(),
            passed: true,
            latency_ms: 10,
            completeness: "full".to_owned(),
            resolution: "exact".to_owned(),
            candidate_count: 1,
            output_bytes: 512,
            top1_correct: true,
            top3_correct: true,
            reciprocal_rank: 1.0,
            expected_file_recall: 1.0,
            expected_repo_recall: None,
            forbidden_hit_count: 0,
            empty_result: false,
            unresolved_gap_count: 0,
            event_target_resolved: None,
            ranking_kendall_tau: None,
            findings: vec![],
            confirmed_downstream_count: 2,
            cross_repo_expected: false,
            is_shared_contract_task: false,
            advisory_only_repo_fraction: None,
        }
    }

    fn report_from_scenarios(scenarios: Vec<PlanningOracleScenarioReport>) -> PlanningOracleReport {
        PlanningOracleReport {
            passed: true,
            total_scenarios: scenarios.len(),
            passed_scenarios: scenarios.len(),
            coverage: 1.0,
            latency_p50_ms: 10,
            latency_p95_ms: 20,
            latency_p99_ms: 30,
            top1_accuracy: 1.0,
            top3_accuracy: 1.0,
            mrr: 1.0,
            expected_file_recall: 1.0,
            expected_repo_recall: None,
            forbidden_hit_rate: 0.0,
            empty_result_rate: 0.0,
            unresolved_gap_rate: 0.0,
            event_resolution_success_rate: None,
            stability_kendall_tau: 1.0,
            split: SplitMetrics {
                anchor_top1: None,
                proof_recall: None,
                proof_precision: None,
                structural_ratio: None,
                advisory_leak_rate: None,
                operator_actionability: None,
                unexplained_confirmed_repo_fraction: None,
            },
            scenarios,
        }
    }

    fn base_report(mut scenarios: Vec<PlanningOracleScenarioReport>) -> PlanningOracleReport {
        for contract in HIGH_SCENARIO_CONTRACTS {
            if scenarios
                .iter()
                .all(|scenario| scenario.name != contract.scenario_name)
            {
                scenarios.push(passing_scenario(contract.scenario_name));
            }
        }
        report_from_scenarios(scenarios)
    }

    fn passing_score_artifact() -> ScoreArtifact {
        ScoreArtifact {
            scored_at: "2026-01-01T00:00:00Z".to_owned(),
            sample_path: "sample.json".to_owned(),
            gather_step_bin: "gather-step".to_owned(),
            median_f1: 0.80,
            median_precision: 0.85,
            median_recall: 0.75,
            gate_high_passed: true,
            pr_scores: vec![PrScore {
                pr_id: "1".to_owned(),
                merge_commit: "abc".to_owned(),
                pre_merge_commit: "def".to_owned(),
                change_category: ChangeCategoryLabel::Other,
                diff_size_bucket: DiffSizeBucket::Small,
                repo_count: 1,
                gate_evaluable: true,
                suggested_files: vec![],
                precision: 0.85,
                recall: 0.75,
                f1: 0.80,
                error: None,
            }],
            strata: vec![StratumAggregate {
                stratum: "all".to_owned(),
                count: 1,
                median_f1: 0.80,
                median_precision: 0.85,
                median_recall: 0.75,
            }],
        }
    }

    // ── scenario-level oracle assertions ─────────────────────────────────────

    #[test]
    fn scenario_oracles_fails_when_any_scenario_oracle_failed() {
        let mut scenario = passing_scenario("frontend_hook_rollout");
        scenario.passed = false;
        scenario
            .findings
            .push("resolution mismatch; expected `search_ranked_resolved`".to_owned());
        let mut report = base_report(vec![scenario]);
        report.passed = false;
        report.passed_scenarios = 0;
        report.coverage = 0.0;

        let result = evaluate(&report, None);
        let c = result
            .criteria
            .iter()
            .find(|c| c.name == "scenario_oracles")
            .unwrap();
        assert!(!c.passed, "expected scenario_oracles failure");
        assert!(c.message.contains("frontend_hook_rollout"));
        assert!(!result.all_passed);
    }

    #[test]
    fn high_contract_coverage_fails_when_required_scenario_is_missing() {
        let report = report_from_scenarios(vec![
            passing_scenario("frontend_hook_rollout"),
            passing_scenario("shared_api_rollout_split"),
        ]);

        let result = evaluate(&report, None);
        let c = result
            .criteria
            .iter()
            .find(|c| c.name == "high_contract_coverage")
            .unwrap();

        assert!(!c.passed, "expected missing HIGH scenario to fail");
        assert!(c.message.contains("event_producer_consumer_rollout"));
        assert!(!result.all_passed);
    }

    #[test]
    fn high_contract_coverage_passes_for_authoritative_set() {
        let report = base_report(Vec::new());

        let result = evaluate(&report, None);
        let c = result
            .criteria
            .iter()
            .find(|c| c.name == "high_contract_coverage")
            .unwrap();

        assert!(c.passed, "expected HIGH contract coverage pass");
    }

    #[test]
    fn high_contract_coverage_normalizes_required_scenario_names() {
        let report = report_from_scenarios(vec![
            passing_scenario(" FRONTEND_HOOK_ROLLOUT "),
            passing_scenario(" event_producer_consumer_rollout "),
            passing_scenario(" SHARED_API_ROLLOUT_SPLIT "),
        ]);

        let result = evaluate(&report, None);
        let c = result
            .criteria
            .iter()
            .find(|c| c.name == "high_contract_coverage")
            .unwrap();

        assert!(c.passed, "expected normalized HIGH contract coverage pass");
    }

    #[test]
    fn scenario_oracles_ignores_aggregate_report_failure_without_scenario_findings() {
        let mut report = base_report(vec![passing_scenario("threshold_only_failure")]);
        report.passed = false;

        let result = evaluate(&report, None);
        let c = result
            .criteria
            .iter()
            .find(|c| c.name == "scenario_oracles")
            .unwrap();

        assert!(
            c.passed,
            "scenario_oracles should only evaluate scenario assertions: {}",
            c.message
        );
    }

    // ── criterion 1 ──────────────────────────────────────────────────────────

    #[test]
    fn criterion1_passes_when_all_resolutions_are_valid() {
        let mut s1 = passing_scenario("s1");
        s1.resolution = "exact".to_owned();
        let mut s2 = passing_scenario("s2");
        s2.resolution = "ranked".to_owned();
        let mut s3 = passing_scenario("s3");
        s3.resolution = "rescue".to_owned();
        let report = base_report(vec![s1, s2, s3]);

        let result = evaluate(&report, None);
        let c = result
            .criteria
            .iter()
            .find(|c| c.name == "anchor_resolution")
            .unwrap();
        assert!(c.passed, "expected pass but got: {}", c.message);
        assert!(c.message.starts_with("[PASS]"));
        assert!(result.all_passed);
    }

    #[test]
    fn criterion1_passes_for_search_ranked_alternates() {
        let mut s = passing_scenario("s1");
        s.resolution = "search_ranked_alternates".to_owned();
        let report = base_report(vec![s]);

        let result = evaluate(&report, None);
        let c = result
            .criteria
            .iter()
            .find(|c| c.name == "anchor_resolution")
            .unwrap();
        assert!(
            c.passed,
            "search_ranked_alternates should be accepted: {}",
            c.message
        );
        assert!(c.message.starts_with("[PASS]"));
    }

    #[test]
    fn criterion1_fails_on_ambiguous_search_match() {
        let mut s = passing_scenario("s1");
        s.resolution = "ambiguous_search_match".to_owned();
        let report = base_report(vec![s]);

        let result = evaluate(&report, None);
        let c = result
            .criteria
            .iter()
            .find(|c| c.name == "anchor_resolution")
            .unwrap();
        assert!(!c.passed, "expected fail but got: {}", c.message);
        assert!(c.message.starts_with("[FAIL]"));
        assert!(!result.all_passed);
    }

    #[test]
    fn criterion1_fails_on_absent_resolution() {
        let mut s = passing_scenario("s1");
        s.resolution = String::new(); // absent / unknown
        let report = base_report(vec![s]);

        let result = evaluate(&report, None);
        let c = result
            .criteria
            .iter()
            .find(|c| c.name == "anchor_resolution")
            .unwrap();
        assert!(!c.passed, "expected fail but got: {}", c.message);
    }

    // ── criterion 2 ──────────────────────────────────────────────────────────

    #[test]
    fn criterion2_passes_when_cross_repo_scenarios_have_confirmed_repos() {
        let mut s = passing_scenario("cross_repo_task");
        s.cross_repo_expected = true;
        s.confirmed_downstream_count = 3;
        let report = base_report(vec![s]);

        let result = evaluate(&report, None);
        let c = result
            .criteria
            .iter()
            .find(|c| c.name == "cross_repo_evidence")
            .unwrap();
        assert!(c.passed, "expected pass but got: {}", c.message);
        assert!(c.message.starts_with("[PASS]"));
    }

    #[test]
    fn criterion2_passes_when_no_cross_repo_expectations() {
        let s = passing_scenario("solo_task");
        let report = base_report(vec![s]);

        let result = evaluate(&report, None);
        let c = result
            .criteria
            .iter()
            .find(|c| c.name == "cross_repo_evidence")
            .unwrap();
        assert!(c.passed, "criterion not applicable — should pass");
    }

    #[test]
    fn criterion2_fails_when_cross_repo_scenario_has_no_confirmed_repos() {
        let mut s = passing_scenario("cross_repo_task");
        s.cross_repo_expected = true;
        s.confirmed_downstream_count = 0;
        let report = base_report(vec![s]);

        let result = evaluate(&report, None);
        let c = result
            .criteria
            .iter()
            .find(|c| c.name == "cross_repo_evidence")
            .unwrap();
        assert!(!c.passed, "expected fail but got: {}", c.message);
        assert!(c.message.starts_with("[FAIL]"));
        assert!(!result.all_passed);
    }

    // ── criterion 3 ──────────────────────────────────────────────────────────

    #[test]
    fn criterion3_passes_when_advisory_fraction_is_below_threshold() {
        let mut s = passing_scenario("shared_contract_task");
        s.is_shared_contract_task = true;
        s.advisory_only_repo_fraction = Some(0.33);
        let report = base_report(vec![s]);

        let result = evaluate(&report, None);
        let c = result
            .criteria
            .iter()
            .find(|c| c.name == "no_advisory_domination")
            .unwrap();
        assert!(c.passed, "expected pass but got: {}", c.message);
        assert!(c.message.starts_with("[PASS]"));
    }

    #[test]
    fn criterion3_fails_when_advisory_fraction_at_or_above_50_percent() {
        let mut s = passing_scenario("shared_contract_task");
        s.is_shared_contract_task = true;
        s.advisory_only_repo_fraction = Some(0.60);
        let report = base_report(vec![s]);

        let result = evaluate(&report, None);
        let c = result
            .criteria
            .iter()
            .find(|c| c.name == "no_advisory_domination")
            .unwrap();
        assert!(!c.passed, "expected fail but got: {}", c.message);
        assert!(c.message.starts_with("[FAIL]"));
        assert!(!result.all_passed);
    }

    #[test]
    fn criterion3_fails_on_aggregate_advisory_leak_rate_above_threshold() {
        let mut report = base_report(vec![passing_scenario("s1")]);
        // No per-scenario fraction, but aggregate leak rate is above threshold.
        report.split.advisory_leak_rate = Some(0.55);

        let result = evaluate(&report, None);
        let c = result
            .criteria
            .iter()
            .find(|c| c.name == "no_advisory_domination")
            .unwrap();
        assert!(!c.passed, "expected fail but got: {}", c.message);
        assert!(!result.all_passed);
    }

    #[test]
    fn criterion3_skips_when_no_shared_contract_scenarios_and_no_aggregate_rate() {
        let report = base_report(vec![passing_scenario("plain_scenario")]);

        let result = evaluate(&report, None);
        let c = result
            .criteria
            .iter()
            .find(|c| c.name == "no_advisory_domination")
            .unwrap();
        assert!(c.passed, "not applicable — should pass gracefully");
    }

    // ── criterion 4 ──────────────────────────────────────────────────────────

    #[test]
    fn criterion4_skips_when_no_pr_oracle_result_supplied() {
        let report = base_report(vec![passing_scenario("s1")]);

        let result = evaluate(&report, None);
        let c = result
            .criteria
            .iter()
            .find(|c| c.name == "pr_oracle_f1")
            .unwrap();
        assert!(c.passed, "should pass (skipped) with no artifact");
        assert!(
            c.message.contains("skipped"),
            "message should mention skip: {}",
            c.message
        );
    }

    #[test]
    fn criterion4_fails_when_pr_oracle_is_required_and_missing() {
        let report = base_report(vec![passing_scenario("s1")]);

        let result = evaluate_with_options(
            &report,
            None,
            GateOptions {
                require_pr_oracle: true,
            },
        );
        let c = result
            .criteria
            .iter()
            .find(|c| c.name == "pr_oracle_f1")
            .unwrap();
        assert!(!c.passed, "required PR-oracle artifact should fail");
        assert!(
            c.message.contains("--pr-oracle-result"),
            "message should name the missing artifact flag: {}",
            c.message
        );
        assert!(!result.all_passed);
    }

    #[test]
    fn parses_planning_oracle_report_from_benchmark_result_wrapper() {
        let report = report_from_scenarios(vec![passing_scenario("s1")]);
        let raw = serde_json::json!({
            "date": "2026-04-26T00:00:00Z",
            "sample_sizes": {"planning_oracle": 1},
            "metrics": report,
        })
        .to_string();

        let parsed = parse_planning_oracle_report(&raw).expect("wrapped report should parse");

        assert_eq!(parsed.total_scenarios, 1);
        assert_eq!(parsed.passed_scenarios, 1);
    }

    #[test]
    fn criterion4_passes_when_f1_and_recall_meet_thresholds() {
        let report = base_report(vec![passing_scenario("s1")]);
        let artifact = passing_score_artifact();

        let result = evaluate(&report, Some(&artifact));
        let c = result
            .criteria
            .iter()
            .find(|c| c.name == "pr_oracle_f1")
            .unwrap();
        assert!(c.passed, "expected pass but got: {}", c.message);
        assert!(c.message.starts_with("[PASS]"));
    }

    #[test]
    fn criterion4_fails_when_f1_below_threshold() {
        let report = base_report(vec![passing_scenario("s1")]);
        let mut artifact = passing_score_artifact();
        artifact.median_f1 = 0.60; // below 0.75

        let result = evaluate(&report, Some(&artifact));
        let c = result
            .criteria
            .iter()
            .find(|c| c.name == "pr_oracle_f1")
            .unwrap();
        assert!(!c.passed, "expected fail but got: {}", c.message);
        assert!(c.message.starts_with("[FAIL]"));
        assert!(!result.all_passed);
    }

    #[test]
    fn criterion4_fails_when_recall_below_threshold() {
        let report = base_report(vec![passing_scenario("s1")]);
        let mut artifact = passing_score_artifact();
        artifact.median_recall = 0.60; // below 0.70

        let result = evaluate(&report, Some(&artifact));
        let c = result
            .criteria
            .iter()
            .find(|c| c.name == "pr_oracle_f1")
            .unwrap();
        assert!(!c.passed, "expected fail but got: {}", c.message);
        assert!(!result.all_passed);
    }

    // ── all-criteria pass ─────────────────────────────────────────────────────

    #[test]
    fn all_criteria_pass_on_clean_report() {
        let mut s1 = passing_scenario("anchor_task");
        s1.resolution = "ranked".to_owned();
        let mut s2 = passing_scenario("cross_repo_task");
        s2.cross_repo_expected = true;
        s2.confirmed_downstream_count = 2;
        let mut s3 = passing_scenario("shared_contract_task");
        s3.is_shared_contract_task = true;
        s3.advisory_only_repo_fraction = Some(0.20);
        let report = base_report(vec![s1, s2, s3]);
        let artifact = passing_score_artifact();

        let result = evaluate(&report, Some(&artifact));
        assert!(result.all_passed, "all criteria should pass");
        // 8 criteria: high_contract_coverage, scenario_oracles,
        // anchor_resolution, cross_repo_evidence, no_advisory_domination,
        // proof_precision, proof_recall, pr_oracle_f1.
        assert_eq!(result.criteria.len(), 8);
        for c in &result.criteria {
            assert!(c.passed, "criterion {} failed: {}", c.name, c.message);
        }
    }

    // ── file-backed fixture integration ───────────────────────────────────────

    #[test]
    fn fixture_fail_shaped_fails_gate() {
        let fixture_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../benchmark/release_gate_fixtures/fail_shaped.json");
        let result = evaluate_report_path(&fixture_path, None).expect("fixture must load");
        assert!(
            !result.all_passed,
            "fail-shaped fixture should fail the gate"
        );
        // Criterion 3 should be the specific failure for the advisory-domination fixture.
        let c3 = result
            .criteria
            .iter()
            .find(|c| c.name == "no_advisory_domination")
            .unwrap();
        assert!(!c3.passed, "advisory domination criterion should fail");
    }

    #[test]
    fn fixture_pass_shaped_passes_gate() {
        let fixture_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../benchmark/release_gate_fixtures/pass_shaped.json");
        let result = evaluate_report_path(&fixture_path, None).expect("fixture must load");
        assert!(
            result.all_passed,
            "pass-shaped fixture should pass the gate"
        );
    }

    #[test]
    fn fixture_pass_shaped_with_pr_oracle_passes_gate() {
        let fixture_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../benchmark/release_gate_fixtures/pass_shaped.json");
        let pr_oracle_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../benchmark/release_gate_fixtures/pr_oracle_pass.json");
        let result =
            evaluate_report_path(&fixture_path, Some(&pr_oracle_path)).expect("fixture must load");
        assert!(result.all_passed, "should pass with passing PR-oracle");
    }

    #[test]
    fn fixture_pass_shaped_with_failing_pr_oracle_fails_criterion4() {
        let fixture_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../benchmark/release_gate_fixtures/pass_shaped.json");
        let pr_oracle_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../benchmark/release_gate_fixtures/pr_oracle_fail.json");
        let result =
            evaluate_report_path(&fixture_path, Some(&pr_oracle_path)).expect("fixture must load");
        assert!(
            !result.all_passed,
            "should fail when PR-oracle artifact fails"
        );
        let c4 = result
            .criteria
            .iter()
            .find(|c| c.name == "pr_oracle_f1")
            .unwrap();
        assert!(!c4.passed, "criterion 4 should fail");
    }
}
