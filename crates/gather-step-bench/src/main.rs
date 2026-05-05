#![forbid(unsafe_code)]

use std::{
    collections::BTreeSet,
    fmt::Write as _,
    path::{Path, PathBuf},
    process::Command as ProcessCommand,
};

use anyhow::Context;
use clap::{Parser, Subcommand};
use gather_step_bench::{
    compare::{BenchmarkResult, Environment, compare_result_dirs},
    harness::{StorageMetrics, index_workspace_fixture, run_index_pass, run_workspace_index_pass},
    link_quality::{
        load_link_quality_task, render_link_quality_report, run_link_quality_benchmark,
    },
    planning_oracle::run_planning_oracle_benchmark,
    pr_oracle::{PrOracleArgs, run as run_pr_oracle},
    release_gate::{
        GateOptions, evaluate_report_path, evaluate_report_path_with_options,
        run_pr_oracle_criterion,
    },
    reliability::{
        DEFAULT_LATENCY_DRIFT_TOLERANCE, DEFAULT_RELIABILITY_RUNS, run_reliability_check,
    },
    threshold::Thresholds,
    tool_trace::{
        GroundTruth, aggregate_report, expand_glob, group_by_session, parse_trace_file,
        render_aggregate_report,
    },
};
use gather_step_core::high_contract::{
    HIGH_SCENARIO_CONTRACTS, HighContractKind, MIN_HIGH_REAL_WORKSPACE_INDEXED_REPOS,
    MIN_HIGH_REAL_WORKSPACE_TOTAL_FILES, normalize_high_contract_name,
};
use serde::Serialize;

/// Subcommands for `tool-trace`.
#[derive(Debug, clap::Subcommand)]
enum ToolTraceSubcommand {
    /// Read tool-call JSONL trace files and print an aggregate report.
    Report {
        /// Glob pattern matching one or more JSONL trace files.
        ///
        /// Example: `traces/*.jsonl`
        #[arg(long)]
        inputs: String,
        /// Optional path to a ground-truth JSON file containing a `must_find`
        /// array of tool names.  When supplied, `time_to_first_correct_anchor_ms`
        /// is computed for each session.
        ///
        /// Format: `{ "must_find": ["search", "planning_pack"] }`
        #[arg(long)]
        ground_truth: Option<PathBuf>,
        /// Maximum number of modal call sequences to include in the report.
        #[arg(long, default_value_t = 5)]
        top_n: usize,
        /// When set, write the aggregate report as JSON to this path in
        /// addition to printing the human-readable summary.
        #[arg(long)]
        output_dir: Option<PathBuf>,
    },
}

/// Gather Step benchmark quality harness.
#[derive(Debug, Parser)]
#[command(name = "gather-step-bench", author, version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Index a fixture directory and write benchmark results.
    Run {
        /// Path to the fixture directory to index.
        fixture_path: PathBuf,
        /// Repository name to assign to the indexed fixture.
        #[arg(long, default_value = "bench-fixture")]
        repo: String,
        /// Path to the thresholds YAML file.
        #[arg(long, default_value = "benchmark/thresholds.yaml")]
        thresholds: PathBuf,
        /// Directory where JSON results are written.
        #[arg(long, default_value = "benchmark/results")]
        output_dir: PathBuf,
    },
    /// Index a configured fixture workspace and write benchmark results.
    WorkspaceRun {
        /// Path to the fixture workspace containing gather-step.config.yaml.
        fixture_path: PathBuf,
        /// Path to the thresholds YAML file.
        #[arg(long, default_value = "benchmark/thresholds.yaml")]
        thresholds: PathBuf,
        /// Directory where JSON results are written.
        #[arg(long, default_value = "benchmark/results")]
        output_dir: PathBuf,
    },
    /// Compare two result directories and report regressions.
    Compare {
        /// Baseline results directory.
        from: PathBuf,
        /// Current results directory to compare against the baseline.
        to: PathBuf,
    },
    /// Run link-quality benchmark tasks against an indexed fixture.
    LinkQuality {
        /// Path to the fixture directory to index.
        #[arg(long)]
        fixture: PathBuf,
        /// Directory containing task YAML files (*.yaml).
        #[arg(long)]
        tasks: PathBuf,
        /// Path to the thresholds YAML file.
        #[arg(long, default_value = "benchmark/thresholds.yaml")]
        thresholds: PathBuf,
    },
    /// Run planning-oracle scenarios against a fixture workspace.
    PlanningOracle {
        /// Path to the fixture workspace to index.
        #[arg(long)]
        fixture: PathBuf,
        /// Directory containing oracle scenario subdirectories.
        #[arg(long)]
        scenarios: PathBuf,
        /// Path to the thresholds YAML file.
        #[arg(long, default_value = "benchmark/thresholds.yaml")]
        thresholds: PathBuf,
        /// Directory where JSON results are written.
        #[arg(long, default_value = "benchmark/results")]
        output_dir: PathBuf,
    },
    /// Measure indexer precision and recall against real merged-PR file sets.
    PrOracle(PrOracleArgs),
    /// Evaluate release-gate criteria against a planning-oracle report artifact.
    ///
    /// Reads a `PlanningOracleReport` JSON and evaluates machine-executable
    /// gate criteria.  Exits non-zero when any criterion fails.
    AnalyzeReport {
        /// Path to the `PlanningOracleReport` JSON artifact to evaluate.
        #[arg(long)]
        report: PathBuf,
        /// Optional path to a PR-oracle score artifact (from `pr-oracle score`).
        /// When absent, criterion 4 (F1 threshold) is skipped.
        #[arg(long)]
        pr_oracle_result: Option<PathBuf>,
        /// Fail criterion 4 when --pr-oracle-result is absent.
        #[arg(long)]
        require_pr_oracle: bool,
        /// Directory where the gate result JSON is written.  Omit to skip.
        #[arg(long)]
        output_dir: Option<PathBuf>,
    },
    /// Run each planning probe N times and check for content drift across runs.
    ///
    /// Indexes the workspace once per run and compares probe outputs across runs.
    /// Content drift (different primary file, repo, symbol, confirmed downstream
    /// repos, proof count, output size, or warnings) is a hard failure.
    /// Latency drift beyond the configured tolerance is reported as a soft signal
    /// but does not fail the check.
    ///
    /// NOTE: This check runs in-process.  Each run creates a fresh temporary
    /// index, so the index data is isolated.  However, any process-global
    /// registries (e.g. in-flight pack caches backed by `LazyLock<Mutex<...>>`)
    /// persist across runs within the same process.  This means the check is
    /// not a true isolation test.  A future --reliability-mode=subprocess flag
    /// will provide full isolation at the cost of higher overhead.
    ReliabilityCheck {
        /// Path to the fixture workspace to index.
        #[arg(long)]
        fixture: PathBuf,
        /// Directory containing oracle scenario subdirectories.
        #[arg(long)]
        scenarios: PathBuf,
        /// Number of probe repetitions per scenario.
        #[arg(long, default_value_t = DEFAULT_RELIABILITY_RUNS)]
        reliability_runs: usize,
        /// Maximum ratio between slowest and fastest run before LATENCY-DRIFT
        /// is emitted.  Set to a large value (e.g. 100) to suppress latency
        /// drift reporting.
        #[arg(long, default_value_t = DEFAULT_LATENCY_DRIFT_TOLERANCE)]
        latency_drift_tolerance: f64,
        /// Directory where the reliability report JSON is written.  Omit to
        /// skip writing a file artifact.
        #[arg(long)]
        output_dir: Option<PathBuf>,
    },
    /// Analyse tool-call trace JSONL files and report wrong-path-rate metrics.
    ToolTrace {
        #[command(subcommand)]
        subcommand: ToolTraceSubcommand,
    },
    /// Run the real-workspace release gate and persist a JSON artifact.
    ReleaseGate {
        /// Real workspace root to index and probe.
        #[arg(long)]
        workspace: PathBuf,
        /// Path to the gather-step binary to invoke.
        #[arg(long, default_value = "target/release/gather-step")]
        gather_step_bin: PathBuf,
        /// Optional config file to pass through to gather-step.
        #[arg(long)]
        config: Option<PathBuf>,
        /// Directory where JSON results are written.
        #[arg(long, default_value = "benchmark/results")]
        output_dir: PathBuf,
        /// Planning pack target for the Task 1 HIGH probe. Must be a
        /// workspace-local hook/session symbol with cross-repo consumers.
        #[arg(long)]
        planning_target: String,
        /// Canonical event subject for the Task 2 HIGH probe. Must surface at
        /// least one producer and one consumer in `events trace`.
        #[arg(long, aliases = ["producer-target", "consumer-target"])]
        event_target: String,
        /// Impact target for the Task 3 HIGH probe. Must be a shared-library
        /// API consumed cross-repo.
        #[arg(long)]
        impact_target: String,
        /// Path to a PR-oracle score artifact (from `gather-step-bench
        /// pr-oracle build-sample` followed by `gather-step-bench
        /// pr-oracle score`). The release gate runs the median F1 /
        /// recall criterion against it. The flag is required: without a
        /// score artifact the gate cannot observe planning-quality wins
        /// or regressions, so architectural changes would be invisible
        /// at the release boundary. Operators must build the sample
        /// once per quarter and pass the scored artifact on every
        /// release-bearing run.
        #[arg(long)]
        pr_oracle_result: PathBuf,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Run {
            fixture_path,
            repo,
            thresholds,
            output_dir,
        } => run_command(&fixture_path, &repo, &thresholds, &output_dir),
        Command::WorkspaceRun {
            fixture_path,
            thresholds,
            output_dir,
        } => workspace_run_command(&fixture_path, &thresholds, &output_dir),
        Command::Compare { from, to } => {
            let summary = compare_result_dirs(&from, &to)?;
            for line in &summary.lines {
                print_status(line);
            }
            if !summary.passed {
                anyhow::bail!("comparison found one or more regressions");
            }
            Ok(())
        }
        Command::LinkQuality {
            fixture,
            tasks,
            thresholds,
        } => link_quality_command(&fixture, &tasks, &thresholds),
        Command::PlanningOracle {
            fixture,
            scenarios,
            thresholds,
            output_dir,
        } => planning_oracle_command(&fixture, &scenarios, &thresholds, &output_dir),
        Command::PrOracle(args) => run_pr_oracle(&args),
        Command::ToolTrace { subcommand } => match subcommand {
            ToolTraceSubcommand::Report {
                inputs,
                ground_truth,
                top_n,
                output_dir,
            } => tool_trace_report_command(
                &inputs,
                ground_truth.as_deref(),
                top_n,
                output_dir.as_deref(),
            ),
        },
        Command::AnalyzeReport {
            report,
            pr_oracle_result,
            require_pr_oracle,
            output_dir,
        } => analyze_report_command(
            &report,
            pr_oracle_result.as_deref(),
            require_pr_oracle,
            output_dir.as_deref(),
        ),
        Command::ReliabilityCheck {
            fixture,
            scenarios,
            reliability_runs,
            latency_drift_tolerance,
            output_dir,
        } => reliability_check_command(
            &fixture,
            &scenarios,
            reliability_runs,
            latency_drift_tolerance,
            output_dir.as_deref(),
        ),
        Command::ReleaseGate {
            workspace,
            gather_step_bin,
            config,
            output_dir,
            planning_target,
            event_target,
            impact_target,
            pr_oracle_result,
        } => release_gate_command(
            &workspace,
            &gather_step_bin,
            config.as_deref(),
            &output_dir,
            &planning_target,
            &event_target,
            &impact_target,
            &pr_oracle_result,
        ),
    }
}

#[derive(Debug, Serialize)]
struct ReleaseGateReport {
    workspace: String,
    gather_step_bin: String,
    provenance: ReleaseGateProvenance,
    index: serde_json::Value,
    /// Ensures the operator-facing release gate exercised every HIGH contract
    /// scenario, not only whichever probes happened to be wired into this run.
    high_contract: ReleaseGateCheck,
    planning: ReleaseGateCheck,
    /// Single canonical event-subject probe (Task 2). Replaces the
    /// previous split producer / consumer probes. The trace must surface
    /// at least one producer AND at least one consumer for the same
    /// canonical event node — split probes could not prove this pairing.
    event_trace: ReleaseGateCheck,
    /// Canonical event context-pack probe (Task 2). Keeps the pack resolver,
    /// downstream repo assembly, and pack cache path under the real release
    /// gate instead of validating only the lower-level trace.
    event_pack: ReleaseGateCheck,
    /// `pack <impact_target> --mode change_impact` probe (Task 3). Fails
    /// when the resolver degrades to ranked alternates instead of picking
    /// the canonical shared-library primary.
    pack_change_impact: ReleaseGateCheck,
    impact: ReleaseGateCheck,
    /// Parity gate: pack and impact must agree on the canonical primary
    /// repo for the impact target. Disagreement is a hard fail because
    /// the two resolvers are meant to converge.
    pack_impact_parity: ReleaseGateCheck,
    /// PR-oracle median F1 / recall criterion result. Required for every
    /// release-bearing run — without a score artifact, planning-quality
    /// wins or regressions are invisible at the release boundary.
    pr_oracle: ReleaseGateCheck,
    all_passed: bool,
}

#[derive(Debug, Serialize)]
struct ReleaseGateProvenance {
    checkout_root: String,
    checkout_head: String,
    binary_commit_sha: String,
}

#[derive(Debug, Serialize)]
struct ReleaseGateCheck {
    passed: bool,
    summary: String,
    output: serde_json::Value,
}

/// Verdict for a probe that also surfaces the primary-repo string. Used by
/// the pack/impact parity gate to compare the two resolvers without
/// re-parsing the underlying JSON.
#[derive(Debug)]
struct ProbeWithPrimary {
    check: ReleaseGateCheck,
    primary_repo: Option<String>,
}

/// Write a status line to stderr.  Printing is intentional in this binary.
#[expect(
    clippy::print_stderr,
    reason = "benchmark binary writes structured status to stderr by design"
)]
fn print_status(line: &str) {
    eprintln!("{line}");
}

fn analyze_report_command(
    report_path: &Path,
    pr_oracle_result_path: Option<&Path>,
    require_pr_oracle: bool,
    output_dir: Option<&Path>,
) -> anyhow::Result<()> {
    let gate_result = if require_pr_oracle {
        evaluate_report_path_with_options(
            report_path,
            pr_oracle_result_path,
            GateOptions {
                require_pr_oracle: true,
            },
        )?
    } else {
        evaluate_report_path(report_path, pr_oracle_result_path)?
    };

    for criterion in &gate_result.criteria {
        print_status(&criterion.message);
    }

    if gate_result.all_passed {
        print_status("release-gate: ALL CRITERIA PASSED");
    } else {
        print_status("release-gate: ONE OR MORE CRITERIA FAILED");
    }

    if let Some(dir) = output_dir {
        let date = chrono::Utc::now().to_rfc3339();
        let result = BenchmarkResult {
            environment: Some(Environment::current()),
            date: date.clone(),
            sample_sizes: [("release_gate_analyze".to_owned(), 1)]
                .into_iter()
                .collect(),
            comparison_window: None,
            metrics: serde_json::to_value(&gate_result)?,
            thresholds_applied: gate_result
                .criteria
                .iter()
                .map(|c| c.message.clone())
                .collect(),
        };
        write_result(dir, "release_gate_analyze", &date, &result)?;
    }

    if !gate_result.all_passed {
        anyhow::bail!("release gate failed one or more criteria");
    }
    Ok(())
}

fn run_command(
    fixture_path: &Path,
    repo: &str,
    thresholds_path: &Path,
    output_dir: &Path,
) -> anyhow::Result<()> {
    let thresholds = if thresholds_path.exists() {
        Thresholds::load(thresholds_path)?
    } else {
        print_status(&format!(
            "warning: thresholds file not found at {}; using defaults",
            thresholds_path.display()
        ));
        Thresholds::default_thresholds()
    };

    print_status(&format!("Indexing fixture: {}", fixture_path.display()));
    let metrics = run_index_pass(fixture_path, repo)?;
    print_status(&format!(
        "Done: parse_ms={} nodes={} edges={} storage={}B",
        metrics.parse_ms, metrics.graph_nodes, metrics.graph_edges, metrics.storage.total_bytes
    ));

    let mut checks: Vec<String> = Vec::new();
    let mut failures = Vec::new();

    // Check memory threshold.
    if let Some(rss_growth) = metrics.memory_rss_growth_bytes {
        checks.push(format!(
            "memory.rss_absolute_max_bytes: {} <= {}",
            rss_growth, thresholds.memory.rss_absolute_max_bytes
        ));
        if rss_growth > thresholds.memory.rss_absolute_max_bytes {
            let message = format!(
                "FAIL: RSS growth {} exceeds absolute max {}",
                rss_growth, thresholds.memory.rss_absolute_max_bytes
            );
            print_status(&message);
            failures.push(message);
        }
    }
    collect_storage_threshold_checks(&metrics.storage, &thresholds, &mut checks, &mut failures);

    let env = Environment::current();
    let date = chrono::Utc::now().to_rfc3339();

    let result = BenchmarkResult {
        environment: Some(env),
        date: date.clone(),
        sample_sizes: [("index_pass".to_owned(), 1)].into_iter().collect(),
        comparison_window: None,
        metrics: serde_json::json!({
            "parse_ms": metrics.parse_ms,
            "graph_nodes": metrics.graph_nodes,
            "graph_edges": metrics.graph_edges,
            "memory_rss_growth_bytes": metrics.memory_rss_growth_bytes,
            "storage": metrics.storage,
        }),
        thresholds_applied: checks,
    };

    write_result(output_dir, "index_pass", &date, &result)?;
    if !failures.is_empty() {
        anyhow::bail!(failures.join("; "));
    }
    Ok(())
}

fn workspace_run_command(
    fixture_path: &Path,
    thresholds_path: &Path,
    output_dir: &Path,
) -> anyhow::Result<()> {
    let thresholds = if thresholds_path.exists() {
        Thresholds::load(thresholds_path)?
    } else {
        print_status(&format!(
            "warning: thresholds file not found at {}; using defaults",
            thresholds_path.display()
        ));
        Thresholds::default_thresholds()
    };

    print_status(&format!(
        "Indexing fixture workspace: {}",
        fixture_path.display()
    ));
    let metrics = run_workspace_index_pass(fixture_path)?;
    print_status(&format!(
        "Done: parse_ms={} repos={} files={} nodes={} edges={} cross_repo_edges={} storage={}B",
        metrics.parse_ms,
        metrics.indexed_repos.unwrap_or_default(),
        metrics.indexed_files.unwrap_or_default(),
        metrics.graph_nodes,
        metrics.graph_edges,
        metrics.cross_repo_edges.unwrap_or_default(),
        metrics.storage.total_bytes
    ));

    let mut checks: Vec<String> = Vec::new();
    let mut failures = Vec::new();
    if let Some(rss_growth) = metrics.memory_rss_growth_bytes {
        checks.push(format!(
            "memory.rss_absolute_max_bytes: {} <= {}",
            rss_growth, thresholds.memory.rss_absolute_max_bytes
        ));
        if rss_growth > thresholds.memory.rss_absolute_max_bytes {
            let message = format!(
                "FAIL: RSS growth {} exceeds absolute max {}",
                rss_growth, thresholds.memory.rss_absolute_max_bytes
            );
            print_status(&message);
            failures.push(message);
        }
    }
    collect_storage_threshold_checks(&metrics.storage, &thresholds, &mut checks, &mut failures);

    let env = Environment::current();
    let date = chrono::Utc::now().to_rfc3339();
    let result = BenchmarkResult {
        environment: Some(env),
        date: date.clone(),
        sample_sizes: [("workspace_index_pass".to_owned(), 1)]
            .into_iter()
            .collect(),
        comparison_window: None,
        metrics: serde_json::json!({
            "parse_ms": metrics.parse_ms,
            "graph_nodes": metrics.graph_nodes,
            "graph_edges": metrics.graph_edges,
            "memory_rss_growth_bytes": metrics.memory_rss_growth_bytes,
            "storage": metrics.storage,
            "indexed_repos": metrics.indexed_repos,
            "indexed_files": metrics.indexed_files,
            "cross_repo_edges": metrics.cross_repo_edges,
        }),
        thresholds_applied: checks,
    };

    write_result(output_dir, "workspace_index_pass", &date, &result)?;
    if !failures.is_empty() {
        anyhow::bail!(failures.join("; "));
    }
    Ok(())
}

fn collect_storage_threshold_checks(
    storage: &StorageMetrics,
    thresholds: &Thresholds,
    checks: &mut Vec<String>,
    failures: &mut Vec<String>,
) {
    check_storage_bytes(
        "storage.graph_bytes_max",
        storage.graph_bytes,
        thresholds.storage.graph_bytes_max,
        checks,
        failures,
    );
    check_storage_bytes(
        "storage.metadata_bytes_max",
        storage.metadata_bytes,
        thresholds.storage.metadata_bytes_max,
        checks,
        failures,
    );
    check_storage_bytes(
        "storage.search_bytes_max",
        storage.search_bytes,
        thresholds.storage.search_bytes_max,
        checks,
        failures,
    );
    check_storage_bytes(
        "storage.total_bytes_max",
        storage.total_bytes,
        thresholds.storage.total_bytes_max,
        checks,
        failures,
    );
}

fn check_storage_bytes(
    label: &str,
    actual: u64,
    max: u64,
    checks: &mut Vec<String>,
    failures: &mut Vec<String>,
) {
    checks.push(format!("{label}: {actual} <= {max}"));
    if actual > max {
        let message = format!("FAIL: {label} exceeded: {actual} > {max}.");
        print_status(&message);
        failures.push(message);
    }
}

fn link_quality_command(
    fixture: &Path,
    tasks_dir: &Path,
    _thresholds_path: &Path,
) -> anyhow::Result<()> {
    print_status(&format!("Indexing fixture: {}", fixture.display()));
    let (storage, _guard) = index_workspace_fixture(fixture)?;
    let graph = storage.graph();
    print_status("Fixture indexed.");

    let mut task_files: Vec<std::path::PathBuf> = std::fs::read_dir(tasks_dir)?
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            if path.extension().and_then(|e| e.to_str()) == Some("yaml") {
                Some(path)
            } else {
                None
            }
        })
        .collect();
    task_files.sort();

    if task_files.is_empty() {
        anyhow::bail!("no *.yaml task files found in {}", tasks_dir.display());
    }

    let mut reports = Vec::new();
    for task_path in &task_files {
        let task = load_link_quality_task(task_path)?;
        print_status(&format!("Running task: {}", task.name));
        let report = run_link_quality_benchmark(&task, graph);
        if report.passed {
            print_status(&format!("  PASS — {}", task.name));
        } else {
            print_status(&format!("  FAIL — {}", task.name));
            for finding in &report.findings {
                print_status(&format!("    {finding}"));
            }
        }
        reports.push(report);
    }

    let summary = render_link_quality_report(&reports);
    print_status(&summary);

    let any_failed = reports.iter().any(|r| !r.passed);
    if any_failed {
        anyhow::bail!("one or more link-quality tasks failed their thresholds");
    }
    Ok(())
}

fn planning_oracle_command(
    fixture: &Path,
    scenarios_dir: &Path,
    thresholds_path: &Path,
    output_dir: &Path,
) -> anyhow::Result<()> {
    let thresholds = if thresholds_path.exists() {
        Thresholds::load(thresholds_path)?
    } else {
        print_status(&format!(
            "warning: thresholds file not found at {}; using defaults",
            thresholds_path.display()
        ));
        Thresholds::default_thresholds()
    };

    print_status(&format!("Indexing fixture: {}", fixture.display()));
    print_status(&format!(
        "Running planning oracle scenarios from {}",
        scenarios_dir.display()
    ));
    let report = run_planning_oracle_benchmark(fixture, scenarios_dir, &thresholds)?;
    for scenario in &report.scenarios {
        if scenario.passed {
            print_status(&format!(
                "  PASS — {} ({})",
                scenario.name, scenario.resolution
            ));
        } else {
            print_status(&format!(
                "  FAIL — {} ({})",
                scenario.name, scenario.resolution
            ));
            for finding in &scenario.findings {
                print_status(&format!("    {finding}"));
            }
        }
    }

    print_status(&format!(
        "Planning oracle summary: {}/{} passed, coverage={:.3}, top1={:.3}, top3={:.3}, mrr={:.3}, file_recall={:.3}, stability_tau={:.3}, p50={}ms p95={}ms p99={}ms",
        report.passed_scenarios,
        report.total_scenarios,
        report.coverage,
        report.top1_accuracy,
        report.top3_accuracy,
        report.mrr,
        report.expected_file_recall,
        report.stability_kendall_tau,
        report.latency_p50_ms,
        report.latency_p95_ms,
        report.latency_p99_ms
    ));

    let date = chrono::Utc::now().to_rfc3339();
    let result = BenchmarkResult {
        environment: Some(Environment::current()),
        date: date.clone(),
        sample_sizes: [("planning_oracle".to_owned(), report.total_scenarios)]
            .into_iter()
            .collect(),
        comparison_window: None,
        metrics: serde_json::to_value(&report)?,
        thresholds_applied: vec![
            format!(
                "latency.p50_ms_max: {} <= {}",
                report.latency_p50_ms, thresholds.latency.p50_ms_max
            ),
            format!(
                "latency.p95_ms_max: {} <= {}",
                report.latency_p95_ms, thresholds.latency.p95_ms_max
            ),
            format!(
                "latency.p99_ms_max: {} <= {}",
                report.latency_p99_ms, thresholds.latency.p99_ms_max
            ),
            format!(
                "planning_oracle.coverage_min: {:.3} >= {:.3}",
                report.coverage, thresholds.planning_oracle.coverage_min
            ),
            format!(
                "planning_oracle.top1_accuracy_min: {:.3} >= {:.3}",
                report.top1_accuracy, thresholds.planning_oracle.top1_accuracy_min
            ),
            format!(
                "planning_oracle.top3_accuracy_min: {:.3} >= {:.3}",
                report.top3_accuracy, thresholds.planning_oracle.top3_accuracy_min
            ),
            format!(
                "planning_oracle.mrr_min: {:.3} >= {:.3}",
                report.mrr, thresholds.planning_oracle.mrr_min
            ),
            format!(
                "planning_oracle.expected_file_recall_min: {:.3} >= {:.3}",
                report.expected_file_recall, thresholds.planning_oracle.expected_file_recall_min
            ),
            format!(
                "planning_oracle.expected_repo_recall_min: {} >= {:.3}",
                report
                    .expected_repo_recall
                    .map_or_else(|| "n/a".to_owned(), |value| format!("{value:.3}")),
                thresholds.planning_oracle.expected_repo_recall_min
            ),
            format!(
                "planning_oracle.forbidden_hit_rate_max: {:.3} <= {:.3}",
                report.forbidden_hit_rate, thresholds.planning_oracle.forbidden_hit_rate_max
            ),
            format!(
                "planning_oracle.empty_result_rate_max: {:.3} <= {:.3}",
                report.empty_result_rate, thresholds.planning_oracle.empty_result_rate_max
            ),
            format!(
                "planning_oracle.unresolved_gap_rate_max: {:.3} <= {:.3}",
                report.unresolved_gap_rate, thresholds.planning_oracle.unresolved_gap_rate_max
            ),
            format!(
                "planning_oracle.event_resolution_success_rate_min: {} >= {:.3}",
                report
                    .event_resolution_success_rate
                    .map_or_else(|| "n/a".to_owned(), |value| format!("{value:.3}")),
                thresholds.planning_oracle.event_resolution_success_rate_min
            ),
            format!(
                "planning_oracle.stability_kendall_tau_min: {:.3} >= {:.3}",
                report.stability_kendall_tau, thresholds.planning_oracle.stability_kendall_tau_min
            ),
        ],
    };
    write_result(output_dir, "planning_oracle", &date, &result)?;

    if !report.passed {
        anyhow::bail!("planning oracle benchmark failed one or more thresholds");
    }
    Ok(())
}

fn reliability_check_command(
    fixture: &Path,
    scenarios_dir: &Path,
    reliability_runs: usize,
    latency_drift_tolerance: f64,
    output_dir: Option<&Path>,
) -> anyhow::Result<()> {
    print_status(&format!(
        "Reliability check: fixture={} scenarios={} runs={} latency_tolerance={:.1}x",
        fixture.display(),
        scenarios_dir.display(),
        reliability_runs,
        latency_drift_tolerance,
    ));
    print_status(
        "NOTE: running in-process — index is isolated per run, but process-global \
         registries persist across runs (not a true subprocess isolation test).",
    );

    let report = run_reliability_check(
        fixture,
        scenarios_dir,
        reliability_runs,
        latency_drift_tolerance,
    )?;

    for scenario in &report.scenarios {
        let latency_summary = scenario
            .latency_ms_per_run
            .iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>()
            .join("/");
        print_status(&format!(
            "  [{}] {} — latency=[{}]ms drift_ratio={:.2}x",
            scenario.verdict, scenario.name, latency_summary, scenario.latency_drift_ratio,
        ));
        for diff in &scenario.content_diff {
            print_status(&format!("      DRIFT: {diff}"));
        }
    }

    print_status(&format!(
        "Reliability summary: {}/{} stable, {} content-drift, {} latency-drift",
        report.stable_count,
        report.total_scenarios,
        report.content_drift_count,
        report.latency_drift_count,
    ));

    if let Some(dir) = output_dir {
        let date = chrono::Utc::now().to_rfc3339();
        let result = BenchmarkResult {
            environment: Some(Environment::current()),
            date: date.clone(),
            sample_sizes: [("reliability_check".to_owned(), report.total_scenarios)]
                .into_iter()
                .collect(),
            comparison_window: None,
            metrics: serde_json::to_value(&report)?,
            thresholds_applied: vec![
                format!(
                    "content_drift: 0 of {} scenarios drifted",
                    report.total_scenarios
                ),
                format!("latency_drift_tolerance: {:.1}x", latency_drift_tolerance),
            ],
        };
        write_result(dir, "reliability_check", &date, &result)?;
    }

    if !report.all_content_stable {
        anyhow::bail!(
            "reliability check: {} scenario(s) showed content drift across runs",
            report.content_drift_count
        );
    }
    Ok(())
}

fn release_gate_command(
    workspace: &Path,
    gather_step_bin: &Path,
    config: Option<&Path>,
    output_dir: &Path,
    planning_target: &str,
    event_target: &str,
    impact_target: &str,
    pr_oracle_result_path: &Path,
) -> anyhow::Result<()> {
    let checkout_root = checkout_root();
    let checkout_head = git_stdout(&checkout_root, &["rev-parse", "HEAD"])
        .context("reading gather-step checkout HEAD")?;
    let binary_commit_sha = std::env::var("GIT_COMMIT_SHA")
        .context("GIT_COMMIT_SHA must be set for release-gate runs")?;
    if binary_commit_sha != checkout_head {
        anyhow::bail!(
            "release-gate provenance mismatch: binary reports {binary_commit_sha} but checkout HEAD is {checkout_head}"
        );
    }
    ensure_clean_git_checkout(&checkout_root)?;

    let artifact_path = output_dir.join("release-gate-index-artifact.json");
    let index = run_gather_step_json(
        gather_step_bin,
        workspace,
        config,
        &[
            "index",
            "--release-gate",
            "--artifact-path",
            artifact_path
                .to_str()
                .context("artifact path must be utf-8")?,
        ],
    )?;
    let index_check = index_summary_check(&index)?;
    print_status(&format!(
        "Release gate index summary: {}",
        index_check.summary
    ));

    let planning = run_gather_step_json(
        gather_step_bin,
        workspace,
        config,
        &[
            "pack",
            planning_target,
            "--mode",
            "planning",
            "--budget-bytes",
            "18000",
        ],
    )?;
    let planning_check = planning_check(&planning, planning_target, impact_target)?;
    print_status(&format!(
        "Release gate planning: {}",
        planning_check.summary
    ));

    // Task 2 (event flow): a single canonical-event probe whose response
    // surfaces BOTH producers and consumers. This target is required operator
    // input because generic defaults can legitimately resolve to empty sides on
    // a real workspace.
    let event_subject = event_target;
    let event_trace = run_gather_step_json(
        gather_step_bin,
        workspace,
        config,
        &["events", "trace", event_subject, "--limit", "128"],
    )?;
    let event_check = event_trace_check(&event_trace, event_subject)?;
    print_status(&format!(
        "Release gate event trace: {}",
        event_check.summary
    ));

    let event_pack = run_gather_step_json(
        gather_step_bin,
        workspace,
        config,
        &[
            "pack",
            "--event-target",
            event_subject,
            "--mode",
            "change_impact",
            "--budget-bytes",
            "18000",
        ],
    )?;
    let event_pack_check = event_pack_check(&event_pack, event_subject)?;
    print_status(&format!(
        "Release gate event pack: {}",
        event_pack_check.summary
    ));

    let impact = run_gather_step_json(
        gather_step_bin,
        workspace,
        config,
        &["impact", impact_target, "--limit", "20"],
    )?;
    let impact_probe = impact_check(&impact)?;
    print_status(&format!(
        "Release gate impact: {}",
        impact_probe.check.summary
    ));

    let pack_change_impact = run_gather_step_json(
        gather_step_bin,
        workspace,
        config,
        &[
            "pack",
            impact_target,
            "--mode",
            "change_impact",
            "--budget-bytes",
            "18000",
        ],
    )?;
    let pack_probe = pack_change_impact_check(&pack_change_impact, impact_target)?;
    print_status(&format!(
        "Release gate pack change-impact: {}",
        pack_probe.check.summary
    ));

    let parity_check = pack_impact_parity_check(
        pack_probe.primary_repo.as_deref(),
        impact_probe.primary_repo.as_deref(),
        impact_target,
    );
    print_status(&format!(
        "Release gate pack/impact parity: {}",
        parity_check.summary
    ));

    // PR-oracle is mandatory for the release gate: without a scored sample
    // artifact, the gate cannot observe planning-quality wins or regressions,
    // and architectural changes land invisibly to the release boundary.
    let (pr_oracle_verdict, pr_oracle_artifact) =
        run_pr_oracle_criterion(Some(pr_oracle_result_path), true)?;
    let pr_oracle_output = pr_oracle_artifact
        .as_ref()
        .map(serde_json::to_value)
        .transpose()?
        .unwrap_or(serde_json::Value::Null);
    let pr_oracle_check = ReleaseGateCheck {
        passed: pr_oracle_verdict.passed,
        summary: pr_oracle_verdict.message.clone(),
        output: pr_oracle_output,
    };
    print_status(&format!(
        "Release gate PR oracle: {}",
        pr_oracle_check.summary
    ));

    let high_contract_check = release_gate_high_contract_check([
        HighContractKind::FrontendHookSession.release_probe_name(),
        HighContractKind::ProducerConsumerEvent.release_probe_name(),
        HighContractKind::SharedApiRollout.release_probe_name(),
    ]);
    print_status(&format!(
        "Release gate HIGH contract: {}",
        high_contract_check.summary
    ));

    let all_passed = index_check.passed
        && high_contract_check.passed
        && planning_check.passed
        && event_check.passed
        && event_pack_check.passed
        && impact_probe.check.passed
        && pack_probe.check.passed
        && parity_check.passed
        && pr_oracle_check.passed;

    let report = ReleaseGateReport {
        workspace: workspace.display().to_string(),
        gather_step_bin: gather_step_bin.display().to_string(),
        provenance: ReleaseGateProvenance {
            checkout_root: checkout_root.display().to_string(),
            checkout_head: checkout_head.clone(),
            binary_commit_sha,
        },
        index,
        high_contract: high_contract_check,
        planning: planning_check,
        event_trace: event_check,
        event_pack: event_pack_check,
        pack_change_impact: pack_probe.check,
        impact: impact_probe.check,
        pack_impact_parity: parity_check,
        pr_oracle: pr_oracle_check,
        all_passed,
    };

    let date = chrono::Utc::now().to_rfc3339();
    let result = BenchmarkResult {
        environment: Some(Environment::current()),
        date: date.clone(),
        sample_sizes: [("release_gate".to_owned(), 1)].into_iter().collect(),
        comparison_window: None,
        metrics: serde_json::to_value(&report)?,
        thresholds_applied: vec![
            format!(
                "high_contract: required release probes = {}, {}, {}; operator targets are explicit",
                HighContractKind::FrontendHookSession.release_probe_name(),
                HighContractKind::ProducerConsumerEvent.release_probe_name(),
                HighContractKind::SharedApiRollout.release_probe_name(),
            ),
            format!(
                "summary_invariant: total_edges > 0 => total_files > 0 && total_symbols > 0; \
                 indexed_repos >= {MIN_HIGH_REAL_WORKSPACE_INDEXED_REPOS}; \
                 total_files >= {MIN_HIGH_REAL_WORKSPACE_TOTAL_FILES}; \
                 virtual_other / true_cross_repo edge ratio <= {MAX_VIRTUAL_TO_TRUE_EDGE_RATIO}"
            ),
            "planning_cross_repo_proof: explicit Task 1 planning target is distinct from the Task 3 impact target and has cross-repo callers or confirmed_downstream_repos beyond frontend_standard".to_owned(),
            "event_trace_pairing: canonical event subject surfaces at least one producer AND one consumer in a single trace response".to_owned(),
            "event_pack_proofs: canonical event pack has proof-backed confirmed_downstream_repos".to_owned(),
            "pack_change_impact: target resolves a primary item without falling to search_ranked_alternates".to_owned(),
            "canonical_guard_anchor: impact primary stays on shared_contracts equivalent and excludes CoChangesWith from primary evidence".to_owned(),
            "pack_impact_parity: pack and impact agree on the canonical primary repo for the shared-API target".to_owned(),
            "pr_oracle_f1: median_f1 / median_recall threshold (required)".to_owned(),
            "index_timing_split: index artifact must include total/prepare/write/durable-sync/search/cache-clear/precompute timings for cold-start fsync diagnosis".to_owned(),
            format!("provenance.checkout_head: {checkout_head}"),
        ],
    };
    write_result(output_dir, "release_gate", &date, &result)?;

    if !report.all_passed {
        anyhow::bail!("release gate failed one or more cut criteria");
    }
    Ok(())
}

fn checkout_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate should live under gather-step/crates/<name>")
        .to_path_buf()
}

fn git_stdout(cwd: &Path, args: &[&str]) -> anyhow::Result<String> {
    let output = ProcessCommand::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .with_context(|| format!("running git {}", args.join(" ")))?;
    if !output.status.success() {
        anyhow::bail!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn ensure_clean_git_checkout(checkout_root: &Path) -> anyhow::Result<()> {
    let status = git_stdout(checkout_root, &["status", "--porcelain"])?;
    if status.is_empty() {
        return Ok(());
    }
    let sample = status.lines().take(3).collect::<Vec<_>>().join(", ");
    anyhow::bail!(
        "release-gate requires a clean gather-step checkout; sample dirty paths: {sample}"
    );
}

fn run_gather_step_json(
    gather_step_bin: &Path,
    workspace: &Path,
    config: Option<&Path>,
    args: &[&str],
) -> anyhow::Result<serde_json::Value> {
    let mut command = ProcessCommand::new(gather_step_bin);
    command.arg("--workspace").arg(workspace).arg("--json");
    command.args(args);
    if let (Some("index"), Some(config)) = (args.first().copied(), config) {
        command.arg("--config").arg(config);
    }
    let output = command
        .output()
        .with_context(|| format!("running {}", gather_step_bin.display()))?;
    if !output.status.success() {
        anyhow::bail!(
            "command failed: {} --workspace {} --json {}\nstdout:\n{}\nstderr:\n{}",
            gather_step_bin.display(),
            workspace.display(),
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    serde_json::from_slice(&output.stdout).context("parsing gather-step JSON output")
}

// Minimum scope thresholds for a real-workspace release-gate run live in
// `gather_step_core::high_contract`, where the ignored real-workspace oracle
// can share them. A release gate that exercises a tiny workspace cannot
// validate cross-repo planning evidence.

/// Maximum allowed ratio of `virtual_other_cross_repo_edges` to
/// `true_cross_repo_edges`. Catches framework-augmenter blow-ups
/// (`FrontendHooks`, `NestJS` helper-event extraction, etc.) that emit
/// virtual cross-repo edges per consumer file. A ratio above this
/// threshold means the index is dominated by synthetic edges rather than
/// real graph evidence — almost always a regression in an augmenter.
///
/// Calibrated against the April 26 measurement (12,978 / 21,433 ≈ 0.61);
/// 2.0 leaves headroom for legitimate growth while catching obvious
/// regressions.
const MAX_VIRTUAL_TO_TRUE_EDGE_RATIO: f64 = 2.0;

fn release_gate_high_contract_check<I, S>(observed_probe_names: I) -> ReleaseGateCheck
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let observed_raw = observed_probe_names
        .into_iter()
        .map(|name| name.as_ref().to_owned())
        .collect::<BTreeSet<_>>();
    let observed = observed_raw
        .iter()
        .map(|name| normalize_high_contract_name(name))
        .collect::<BTreeSet<_>>();
    let required_raw = HIGH_SCENARIO_CONTRACTS
        .iter()
        .map(|contract| contract.release_probe_name.to_owned())
        .collect::<BTreeSet<_>>();
    let missing = required_raw
        .iter()
        .filter(|name| !observed.contains(&normalize_high_contract_name(name)))
        .cloned()
        .collect::<Vec<_>>();
    let passed = missing.is_empty();
    let summary = if passed {
        format!(
            "PASS high_contract: all {} required HIGH release probes are present",
            required_raw.len()
        )
    } else {
        format!(
            "FAIL high_contract: missing required HIGH release probe(s): {}; observed={observed_raw:?}",
            missing.join(", ")
        )
    };
    ReleaseGateCheck {
        passed,
        summary,
        output: serde_json::json!({
            "required_release_probes": required_raw,
            "observed_release_probes": observed_raw,
            "missing_release_probes": missing,
        }),
    }
}

fn index_summary_check(index: &serde_json::Value) -> anyhow::Result<ReleaseGateCheck> {
    anyhow::ensure!(
        index.get("event").and_then(serde_json::Value::as_str) == Some("index_completed"),
        "index command did not complete successfully"
    );
    let total_edges = index["stats"]["total_edges"]
        .as_u64()
        .context("index stats.total_edges missing")?;
    let total_files = index["stats"]["total_files"]
        .as_u64()
        .context("index stats.total_files missing")?;
    let total_symbols = index["stats"]["total_symbols"]
        .as_u64()
        .context("index stats.total_symbols missing")?;
    let indexed_repos = index["stats"]["indexed_repos"].as_u64().unwrap_or(0);
    let true_cross_repo_edges = index["stats"]["true_cross_repo_edges"]
        .as_u64()
        .unwrap_or(0);
    let virtual_other_cross_repo_edges = index["stats"]["virtual_other_cross_repo_edges"]
        .as_u64()
        .unwrap_or(0);
    let timings = index
        .get("timings")
        .and_then(serde_json::Value::as_object)
        .context("index timings missing; rerun with a binary that emits index timing splits")?;
    let total_wall_ms = timings
        .get("total_wall_ms")
        .and_then(serde_json::Value::as_u64)
        .context("index timings.total_wall_ms missing")?;
    let graph_build_ms = timings
        .get("graph_build_ms")
        .and_then(serde_json::Value::as_u64)
        .context("index timings.graph_build_ms missing")?;
    let parser_augment_ms = timings
        .get("parser_augment_ms")
        .and_then(serde_json::Value::as_u64)
        .context("index timings.parser_augment_ms missing")?;
    let pack_precompute_ms = timings
        .get("pack_precompute_ms")
        .and_then(serde_json::Value::as_u64)
        .context("index timings.pack_precompute_ms missing")?;
    let metadata_persist_ms = timings
        .get("metadata_persist_ms")
        .and_then(serde_json::Value::as_u64)
        .context("index timings.metadata_persist_ms missing")?;
    let prepare_total_ms = timings
        .get("prepare_total_ms")
        .and_then(serde_json::Value::as_u64)
        .context("index timings.prepare_total_ms missing")?;
    let writer_storage_commit_total_ms = timings
        .get("writer_storage_commit_total_ms")
        .and_then(serde_json::Value::as_u64)
        .context("index timings.writer_storage_commit_total_ms missing")?;
    let durable_sync_ms = timings
        .get("durable_sync_ms")
        .and_then(serde_json::Value::as_u64)
        .context("index timings.durable_sync_ms missing")?;
    let search_flush_ms = timings
        .get("search_flush_ms")
        .and_then(serde_json::Value::as_u64)
        .context("index timings.search_flush_ms missing")?;
    let precompute_ms = timings
        .get("precompute_ms")
        .and_then(serde_json::Value::as_u64)
        .context("index timings.precompute_ms missing")?;
    let context_pack_cache_clear_ms = timings
        .get("context_pack_cache_clear_ms")
        .and_then(serde_json::Value::as_u64)
        .context("index timings.context_pack_cache_clear_ms missing")?;
    let context_pack_cache_rows_removed = timings
        .get("context_pack_cache_rows_removed")
        .and_then(serde_json::Value::as_u64)
        .context("index timings.context_pack_cache_rows_removed missing")?;
    let invariant_ok = total_edges == 0 || (total_files > 0 && total_symbols > 0);
    // Scope guards: catch the "indexed only one repo" / "indexed only a
    // handful of files" misroute before any of the planning probes run.
    // A passing release gate without these guards happily certifies a
    // single-repo run as if it had exercised the full workspace.
    let scope_repos_ok = indexed_repos >= MIN_HIGH_REAL_WORKSPACE_INDEXED_REPOS;
    let scope_files_ok = total_files >= MIN_HIGH_REAL_WORKSPACE_TOTAL_FILES;
    // Edge-budget guard: catch framework-augmenter blow-ups (FrontendHooks
    // overmatch, NestJS cross-file helper extraction, etc.) before they
    // mask a regression as a green index. We only evaluate the ratio when
    // there is at least one true cross-repo edge — otherwise the workspace
    // is intra-repo only and the ratio is undefined.
    #[expect(
        clippy::cast_precision_loss,
        reason = "edge counts are bounded; precision loss in the ratio is acceptable"
    )]
    let edge_ratio = if true_cross_repo_edges > 0 {
        virtual_other_cross_repo_edges as f64 / true_cross_repo_edges as f64
    } else {
        0.0
    };
    let edge_budget_ok = true_cross_repo_edges == 0 || edge_ratio <= MAX_VIRTUAL_TO_TRUE_EDGE_RATIO;
    #[expect(
        clippy::cast_precision_loss,
        reason = "duration values are bounded; precision loss in diagnostic ratios is acceptable"
    )]
    let durable_sync_ratio = if total_wall_ms > 0 {
        durable_sync_ms as f64 / total_wall_ms as f64
    } else {
        0.0
    };
    let passed = invariant_ok && scope_repos_ok && scope_files_ok && edge_budget_ok;
    let mut summary = format!(
        "indexed_repos={indexed_repos} total_files={total_files} \
         total_symbols={total_symbols} total_edges={total_edges} \
         true_cross_repo_edges={true_cross_repo_edges} \
         virtual_other_cross_repo_edges={virtual_other_cross_repo_edges} \
         edge_ratio={edge_ratio:.2} total_wall_ms={total_wall_ms} \
         graph_build_ms={graph_build_ms} parser_augment_ms={parser_augment_ms} \
         pack_precompute_ms={pack_precompute_ms} metadata_persist_ms={metadata_persist_ms} \
         prepare_total_ms={prepare_total_ms} \
         writer_storage_commit_total_ms={writer_storage_commit_total_ms} \
         durable_sync_ms={durable_sync_ms} durable_sync_ratio={durable_sync_ratio:.2} \
         search_flush_ms={search_flush_ms} \
         context_pack_cache_clear_ms={context_pack_cache_clear_ms} \
         context_pack_cache_rows_removed={context_pack_cache_rows_removed} \
         precompute_ms={precompute_ms}"
    );
    if !scope_repos_ok {
        let _ = write!(
            &mut summary,
            " [scope FAIL: indexed_repos < {MIN_HIGH_REAL_WORKSPACE_INDEXED_REPOS} — likely a \
             single-repo config got picked up; rerun with the full benchmark config]"
        );
    }
    if !scope_files_ok {
        let _ = write!(
            &mut summary,
            " [scope FAIL: total_files < {MIN_HIGH_REAL_WORKSPACE_TOTAL_FILES} — workspace \
             too small to exercise planning quality]"
        );
    }
    if !edge_budget_ok {
        let _ = write!(
            &mut summary,
            " [edge-budget FAIL: virtual_other / true_cross_repo edge ratio \
             {edge_ratio:.2} > cap {MAX_VIRTUAL_TO_TRUE_EDGE_RATIO:.2} — \
             augmenter regression suspected (FrontendHooks, helper-event \
             extraction, RouteClientServer, etc.)]"
        );
    }
    Ok(ReleaseGateCheck {
        passed,
        summary,
        output: index.clone(),
    })
}

fn planning_check(
    json: &serde_json::Value,
    planning_target: &str,
    impact_target: &str,
) -> anyhow::Result<ReleaseGateCheck> {
    anyhow::ensure!(
        json.get("event").and_then(serde_json::Value::as_str) == Some("context_pack_completed"),
        "planning pack did not complete successfully"
    );
    let callers = json["data"]["change_impact"]["cross_repo_callers"]
        .as_array()
        .context("planning cross_repo_callers missing")?;
    let confirmed = json["data"]["change_impact"]["confirmed_downstream_repos"]
        .as_array()
        .context("planning confirmed_downstream_repos missing")?;
    let caller_repos = callers
        .iter()
        .filter_map(|item| item.get("repo").and_then(serde_json::Value::as_str))
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    let confirmed_repos = confirmed
        .iter()
        .filter_map(serde_json::Value::as_str)
        .filter(|repo| *repo != "frontend_standard")
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    let distinct_target = planning_target != impact_target;
    let has_cross_repo_evidence = !caller_repos.is_empty() || !confirmed_repos.is_empty();
    let passed = distinct_target && has_cross_repo_evidence;
    Ok(ReleaseGateCheck {
        passed,
        summary: format!(
            "planning_target={planning_target} impact_target={impact_target} \
             distinct_target={distinct_target} cross_repo_callers={caller_repos:?} \
             confirmed_downstream_repos={confirmed_repos:?}"
        ),
        output: json.clone(),
    })
}

/// Validate the canonical-event probe used for Task 2: a single
/// `events trace <subject>` response that must surface at least one
/// producer and at least one consumer attached to the same canonical
/// virtual event node. Replaces the prior two-probe shape that resolved
/// independent symbols on either side of the flow.
fn event_trace_check(json: &serde_json::Value, subject: &str) -> anyhow::Result<ReleaseGateCheck> {
    anyhow::ensure!(
        json.get("event").and_then(serde_json::Value::as_str) == Some("events_trace_completed"),
        "events trace for `{subject}` did not complete successfully"
    );
    let producers = json["producers"].as_array().map_or(0, Vec::len);
    let consumers = json["consumers"].as_array().map_or(0, Vec::len);
    let producer_repos = json["producers"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.get("repo").and_then(serde_json::Value::as_str))
                .map(str::to_owned)
                .collect::<BTreeSet<String>>()
        })
        .unwrap_or_default();
    let consumer_repos = json["consumers"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.get("repo").and_then(serde_json::Value::as_str))
                .map(str::to_owned)
                .collect::<BTreeSet<String>>()
        })
        .unwrap_or_default();
    let passed = producers > 0 && consumers > 0;
    let summary = if passed {
        format!(
            "subject={subject} producers={producers} consumers={consumers} \
             producer_repos={producer_repos:?} consumer_repos={consumer_repos:?}"
        )
    } else {
        format!(
            "subject={subject} producers={producers} consumers={consumers} \
             [FAIL: a canonical event must surface both a producer and a \
             consumer in one response — split-direction gap detected]"
        )
    };
    Ok(ReleaseGateCheck {
        passed,
        summary,
        output: json.clone(),
    })
}

fn event_pack_check(json: &serde_json::Value, subject: &str) -> anyhow::Result<ReleaseGateCheck> {
    anyhow::ensure!(
        json.get("event").and_then(serde_json::Value::as_str) == Some("context_pack_completed"),
        "event pack for `{subject}` did not complete successfully"
    );
    let confirmed = json["data"]["change_impact"]["confirmed_downstream_repos"]
        .as_array()
        .context("event pack confirmed_downstream_repos missing")?;
    let proof_count = json["data"]["planning_proofs"]
        .as_array()
        .map_or(0, Vec::len);
    let confirmed_repos = confirmed
        .iter()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    let passed = !confirmed_repos.is_empty() && proof_count > 0;
    let summary = if passed {
        format!(
            "subject={subject} confirmed_downstream_repos={confirmed_repos:?} \
             planning_proofs={proof_count}"
        )
    } else {
        format!(
            "subject={subject} confirmed_downstream_repos={confirmed_repos:?} \
             planning_proofs={proof_count} [FAIL: canonical event pack must \
             produce proof-backed downstream repos]"
        )
    };
    Ok(ReleaseGateCheck {
        passed,
        summary,
        output: json.clone(),
    })
}

/// Conventional shared-library tokens used across internal monorepos.
/// Used by `repo_is_shared_library` and kept in sync with the pack
/// resolver's `repo_shared_library_bonus` so the gate's notion of
/// "canonical" matches the scorer's tiebreaker.
const SHARED_LIBRARY_REPO_TOKENS: &[&str] =
    &["shared", "common", "contracts", "core", "types", "lib"];

/// Heuristic: does the repo name look like a shared-library boundary?
///
/// Tokenises the repo name on `-`, `_`, `/`, `.` separators and checks for
/// exact membership in [`SHARED_LIBRARY_REPO_TOKENS`]. Substring matching
/// was rejected because it false-positives on names like `score-service`
/// (matched `core`), `prototypes` (matched `types`), and `notification-core`
/// (matched `core`). Tokenisation gives `["score", "service"]` which has
/// no shared-library token, while `shared-lib` → `["shared", "lib"]`
/// correctly matches.
fn repo_is_shared_library(repo: &str) -> bool {
    let mut normalized = repo.to_owned();
    normalized.make_ascii_lowercase();
    if normalized.is_empty() {
        return false;
    }
    normalized
        .split(['-', '_', '/', '.'])
        .any(|token| SHARED_LIBRARY_REPO_TOKENS.contains(&token))
}

fn impact_check(json: &serde_json::Value) -> anyhow::Result<ProbeWithPrimary> {
    anyhow::ensure!(
        json.get("event").and_then(serde_json::Value::as_str) == Some("impact_completed"),
        "impact command did not complete successfully"
    );
    let matches = json["matches"]
        .as_array()
        .context("impact matches missing")?;
    let Some(primary) = matches
        .iter()
        .find(|item| item.get("primary").and_then(serde_json::Value::as_bool) == Some(true))
    else {
        anyhow::bail!("impact output did not contain a primary match");
    };
    let primary_repo = primary
        .get("source_repo")
        .and_then(serde_json::Value::as_str)
        .context("impact primary source_repo missing")?;
    let primary_strategy = primary
        .get("strategy")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let mut edge_kinds = BTreeSet::new();
    if let Some(repos) = primary
        .get("impacted_files")
        .and_then(serde_json::Value::as_array)
    {
        for repo in repos {
            if let Some(files) = repo.get("files").and_then(serde_json::Value::as_array) {
                for file in files {
                    if let Some(kinds) =
                        file.get("edge_kinds").and_then(serde_json::Value::as_array)
                    {
                        for kind in kinds {
                            if let Some(kind) = kind.as_str() {
                                edge_kinds.insert(kind.to_owned());
                            }
                        }
                    }
                }
            }
        }
    }
    // Generic shared-library detection: any repo whose name contains a
    // canonical token (`shared`, `common`, `contracts`, `core`, `types`, `lib`)
    // counts as a shared-library boundary. Hardcoding `shared_contracts`
    // worked for the fixture but rejected real workspaces whose canonical
    // boundary is `shared-lib`, `contracts-core`, etc.
    let canonical_repo = repo_is_shared_library(primary_repo);
    let structural = primary_strategy == "shared_contract"
        && edge_kinds.iter().any(|kind| kind != "CoChangesWith")
        && !edge_kinds.contains("CoChangesWith");
    Ok(ProbeWithPrimary {
        check: ReleaseGateCheck {
            passed: canonical_repo && structural,
            summary: format!(
                "primary_repo={primary_repo} strategy={primary_strategy} edge_kinds={edge_kinds:?}"
            ),
            output: json.clone(),
        },
        primary_repo: Some(primary_repo.to_owned()),
    })
}

/// Validate `pack <target> --mode change_impact` output and surface the
/// resolved primary repo for the parity gate. Fails when the resolver
/// degrades to `search_ranked_alternates` (a sign that the canonical
/// shared-library candidate failed to clear the medium-confidence margin
/// against same-name re-implementations) or when no primary item exists.
///
/// Note: the resolution string lives at `meta.resolution`, NOT
/// `data.resolution`. The earlier draft of this check read `data.resolution`
/// and silently passed every alternates-only response — a real release-
/// gate hole. The check now reads from `meta.resolution` to match the
/// actual response shape.
fn pack_change_impact_check(
    json: &serde_json::Value,
    target: &str,
) -> anyhow::Result<ProbeWithPrimary> {
    anyhow::ensure!(
        json.get("event").and_then(serde_json::Value::as_str) == Some("context_pack_completed"),
        "pack --mode change_impact for `{target}` did not complete successfully"
    );
    let resolution = json["meta"]["resolution"].as_str().unwrap_or("");
    let primary_repo = json["data"]["items"]
        .as_array()
        .and_then(|items| items.first())
        .and_then(|item| item["repo"].as_str())
        .map(str::to_owned);
    let alternates_only = resolution == "search_ranked_alternates";
    let passed = !alternates_only && primary_repo.is_some();
    let summary = if alternates_only {
        format!(
            "target={target} resolution=search_ranked_alternates [FAIL: pack \
             could not resolve canonical primary; expected the shared-library \
             candidate to win on score]"
        )
    } else {
        format!(
            "target={target} resolution={resolution} primary_repo={}",
            primary_repo.as_deref().unwrap_or("<none>")
        )
    };
    Ok(ProbeWithPrimary {
        check: ReleaseGateCheck {
            passed,
            summary,
            output: json.clone(),
        },
        primary_repo,
    })
}

/// Parity gate: pack and impact must agree on the primary repo for the
/// shared-API impact target. Disagreement is a hard fail because the two
/// resolvers should converge on the same canonical declaration.
fn pack_impact_parity_check(
    pack_primary: Option<&str>,
    impact_primary: Option<&str>,
    target: &str,
) -> ReleaseGateCheck {
    let (passed, summary) = match (pack_primary, impact_primary) {
        (Some(pack), Some(imp)) if pack == imp => {
            (true, format!("target={target} pack=impact={pack} [PASS]"))
        }
        (Some(pack), Some(imp)) => (
            false,
            format!(
                "target={target} pack_primary={pack} impact_primary={imp} \
                 [FAIL: pack and impact disagree on the canonical primary repo]"
            ),
        ),
        _ => (
            false,
            format!(
                "target={target} pack_primary={pack_primary:?} impact_primary={impact_primary:?} \
                 [FAIL: parity check requires both probes to expose a primary repo]"
            ),
        ),
    };
    ReleaseGateCheck {
        passed,
        summary,
        output: serde_json::json!({
            "pack_primary": pack_primary,
            "impact_primary": impact_primary,
        }),
    }
}

fn tool_trace_report_command(
    inputs_glob: &str,
    ground_truth_path: Option<&Path>,
    top_n: usize,
    output_dir: Option<&Path>,
) -> anyhow::Result<()> {
    let must_find = if let Some(gt_path) = ground_truth_path {
        let gt = GroundTruth::load(gt_path)?;
        gt.into_must_find_set()
    } else {
        rustc_hash::FxHashSet::default()
    };

    let paths = expand_glob(inputs_glob)?;
    if paths.is_empty() {
        print_status(&format!(
            "tool-trace: no files matched pattern `{inputs_glob}`"
        ));
        return Ok(());
    }

    let mut all_records = Vec::new();
    for path in &paths {
        let records = parse_trace_file(path)?;
        print_status(&format!(
            "tool-trace: loaded {} records from {}",
            records.len(),
            path.display()
        ));
        all_records.extend(records);
    }

    let sessions = group_by_session(all_records);
    let report = aggregate_report(&sessions, &must_find, top_n);
    let summary = render_aggregate_report(&report);
    print_status(&summary);

    if let Some(dir) = output_dir {
        let date = chrono::Utc::now().to_rfc3339();
        let result = BenchmarkResult {
            environment: Some(Environment::current()),
            date: date.clone(),
            sample_sizes: [("tool_trace".to_owned(), report.session_count)]
                .into_iter()
                .collect(),
            comparison_window: None,
            metrics: serde_json::to_value(&report)?,
            thresholds_applied: vec![],
        };
        write_result(dir, "tool_trace", &date, &result)?;
    }

    Ok(())
}

fn write_result(
    output_dir: &Path,
    bench_name: &str,
    date: &str,
    result: &BenchmarkResult,
) -> anyhow::Result<()> {
    // Sanitise date string for use as a directory name component.
    let dir_name: String = date
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let result_dir = output_dir.join(&dir_name);
    std::fs::create_dir_all(&result_dir)?;

    let result_path = result_dir.join(format!("{bench_name}.json"));
    let json = serde_json::to_string_pretty(result)?;
    std::fs::write(&result_path, json)?;
    print_status(&format!("Results written to {}", result_path.display()));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{planning_check, release_gate_high_contract_check};
    use gather_step_core::high_contract::HighContractKind;
    use serde_json::json;

    fn planning_pack() -> serde_json::Value {
        json!({
            "event": "context_pack_completed",
            "data": {
                "change_impact": {
                    "cross_repo_callers": [
                        {"repo": "identity"}
                    ],
                    "confirmed_downstream_repos": [
                        "identity"
                    ]
                }
            }
        })
    }

    #[test]
    fn planning_check_rejects_reused_impact_target() {
        let check = planning_check(
            &planning_pack(),
            "UserAuthGuard.canActivate",
            "UserAuthGuard.canActivate",
        )
        .expect("planning check should parse");
        assert!(!check.passed);
        assert!(check.summary.contains("distinct_target=false"));
    }

    #[test]
    fn planning_check_accepts_distinct_cross_repo_target() {
        let check = planning_check(
            &planning_pack(),
            "UserAuthGuard",
            "UserAuthGuard.canActivate",
        )
        .expect("planning check should parse");
        assert!(check.passed);
        assert!(check.summary.contains("distinct_target=true"));
    }

    #[test]
    fn release_gate_high_contract_check_requires_all_release_probes() {
        let check = release_gate_high_contract_check([
            HighContractKind::FrontendHookSession.release_probe_name(),
            HighContractKind::SharedApiRollout.release_probe_name(),
        ]);

        assert!(!check.passed);
        assert!(
            check
                .summary
                .contains(HighContractKind::ProducerConsumerEvent.release_probe_name())
        );
        assert!(check.summary.contains("observed="));
    }

    #[test]
    fn release_gate_high_contract_check_normalizes_probe_names() {
        let check = release_gate_high_contract_check([
            " PLANNING_FRONTEND_HOOK_SESSION ",
            " CANONICAL_EVENT_PRODUCER_CONSUMER ",
            " SHARED_API_ROLLOUT ",
        ]);

        assert!(check.passed, "{}", check.summary);
    }
}
