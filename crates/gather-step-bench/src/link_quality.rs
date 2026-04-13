#![forbid(unsafe_code)]

use std::{fmt::Write as _, path::Path};

use gather_step_core::NodeKind;
use gather_step_storage::GraphStore;
use serde::{Deserialize, Serialize};

/// A single link-quality evaluation task loaded from a YAML file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkQualityTask {
    /// Short identifier for this task.
    pub name: String,
    /// Human-readable description of what the task exercises.
    pub description: String,
    /// Anchor file path used as the starting point for the trace.
    pub anchor: String,
    /// Repository names expected to appear in the result set.
    pub expected_repos: Vec<String>,
    /// File paths expected to appear in the result set.
    pub expected_files: Vec<String>,
    /// Edge kinds expected in the result.
    pub expected_edges: Vec<ExpectedEdge>,
    /// Per-task threshold override for missed repos.
    pub missed_repos_max: usize,
    /// Per-task threshold override for missed files.
    pub missed_files_max: usize,
    /// Per-task threshold override for false positives.
    pub false_positives_max: usize,
    /// Per-task threshold override for cross-boundary precision.
    pub cross_boundary_precision_min: f64,
}

/// An edge kind requirement inside a link-quality task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpectedEdge {
    pub kind: String,
}

/// Metrics computed for a single link-quality evaluation task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkQualityReport {
    /// Name of the task that produced this report.
    pub task_name: String,
    /// Whether all per-task thresholds were satisfied.
    pub passed: bool,
    /// Repos that were expected but not found.
    pub missed_repos: Vec<String>,
    /// Files that were expected but not found.
    pub missed_files: Vec<String>,
    /// Result entries that are likely false positives (repos not in expected
    /// set and not the anchor repo).
    pub false_positive_repos: Vec<String>,
    /// Fraction of cross-boundary edges that match an expected edge kind.
    pub cross_boundary_precision: f64,
    /// Human-readable findings for failed checks.
    pub findings: Vec<String>,
}

/// Load a link-quality task from a YAML file.
///
/// # Errors
///
/// Returns an error when the file cannot be read or the YAML is malformed.
pub fn load_link_quality_task(task_yaml: &Path) -> anyhow::Result<LinkQualityTask> {
    let raw = std::fs::read_to_string(task_yaml)?;
    let task = serde_yaml_ng::from_str(&raw)?;
    Ok(task)
}

/// Evaluate a link-quality task against the indexed graph.
///
/// The evaluation walks from all nodes whose `file_path` matches the anchor
/// and collects the repos and file paths reachable via outgoing edges.  The
/// result is compared to the task's expected sets to produce a
/// [`LinkQualityReport`].
pub fn run_link_quality_benchmark<S: GraphStore>(
    task: &LinkQualityTask,
    store: &S,
) -> LinkQualityReport {
    // Collect all nodes whose file_path starts with the anchor prefix.
    let anchor_nodes = collect_anchor_nodes(store, &task.anchor);

    // Gather repos reachable via any outgoing edge from anchor nodes.
    let mut reached_repos: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut reached_files: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut total_cross_boundary: usize = 0;
    let mut matching_cross_boundary: usize = 0;
    let expected_edge_kinds: std::collections::BTreeSet<String> =
        task.expected_edges.iter().map(|e| e.kind.clone()).collect();

    for node in &anchor_nodes {
        let Ok(outgoing) = store.get_outgoing(node.id) else {
            continue;
        };
        for edge in outgoing {
            if let Ok(Some(target_node)) = store.get_node(edge.target) {
                if target_node.is_virtual {
                    continue;
                }
                reached_repos.insert(target_node.repo.clone());
                reached_files.insert(target_node.file_path.clone());
                if target_node.repo != node.repo {
                    total_cross_boundary += 1;
                    let edge_kind_str = format!("{:?}", edge.kind);
                    if expected_edge_kinds.contains(&edge_kind_str) {
                        matching_cross_boundary += 1;
                    }
                }
            }
        }
    }

    let missed_repos: Vec<String> = task
        .expected_repos
        .iter()
        .filter(|r| !reached_repos.contains(*r))
        .cloned()
        .collect();

    let missed_files: Vec<String> = task
        .expected_files
        .iter()
        .filter(|f| !reached_files.contains(*f))
        .cloned()
        .collect();

    let anchor_repo = anchor_nodes.first().map_or("", |n| n.repo.as_str());
    let false_positive_repos: Vec<String> = reached_repos
        .iter()
        .filter(|r| r.as_str() != anchor_repo && !task.expected_repos.contains(r))
        .cloned()
        .collect();

    #[expect(
        clippy::cast_precision_loss,
        reason = "counts are small enough that f64 precision is acceptable for benchmark reporting"
    )]
    let cross_boundary_precision = if total_cross_boundary == 0 {
        1.0_f64
    } else {
        matching_cross_boundary as f64 / total_cross_boundary as f64
    };

    let mut findings = Vec::new();

    if missed_repos.len() > task.missed_repos_max {
        findings.push(format!(
            "missed repos ({}) exceeds threshold ({}): {:?}",
            missed_repos.len(),
            task.missed_repos_max,
            missed_repos,
        ));
    }

    if missed_files.len() > task.missed_files_max {
        findings.push(format!(
            "missed files ({}) exceeds threshold ({}): {:?}",
            missed_files.len(),
            task.missed_files_max,
            missed_files,
        ));
    }

    if false_positive_repos.len() > task.false_positives_max {
        findings.push(format!(
            "false positive repos ({}) exceeds threshold ({}): {:?}",
            false_positive_repos.len(),
            task.false_positives_max,
            false_positive_repos,
        ));
    }

    if cross_boundary_precision < task.cross_boundary_precision_min {
        findings.push(format!(
            "cross-boundary precision ({:.3}) below threshold ({:.3})",
            cross_boundary_precision, task.cross_boundary_precision_min,
        ));
    }

    let passed = findings.is_empty();

    LinkQualityReport {
        task_name: task.name.clone(),
        passed,
        missed_repos,
        missed_files,
        false_positive_repos,
        cross_boundary_precision,
        findings,
    }
}

fn collect_anchor_nodes<S: GraphStore>(store: &S, anchor: &str) -> Vec<gather_step_core::NodeData> {
    // Walk all File nodes and match by file_path or name.
    store
        .nodes_by_type(NodeKind::File)
        .unwrap_or_default()
        .into_iter()
        .filter(|n| n.file_path == anchor || n.file_path.ends_with(anchor))
        .collect()
}

/// Render a Markdown summary of multiple link-quality reports.
#[must_use]
pub fn render_link_quality_report(reports: &[LinkQualityReport]) -> String {
    let mut out = String::new();
    out.push_str("# Link-Quality Benchmark Results\n\n");
    for report in reports {
        let status = if report.passed { "PASS" } else { "FAIL" };
        // `writeln!` / `write!` to `String` never fails; results are intentionally discarded.
        let _ = write!(out, "## {} — {}\n\n", report.task_name, status);
        let _ = writeln!(
            out,
            "- Cross-boundary precision: {:.3}",
            report.cross_boundary_precision
        );
        let _ = writeln!(out, "- Missed repos: {}", report.missed_repos.len());
        let _ = writeln!(out, "- Missed files: {}", report.missed_files.len());
        let _ = writeln!(
            out,
            "- False positive repos: {}",
            report.false_positive_repos.len()
        );
        if !report.findings.is_empty() {
            out.push_str("\n### Findings\n\n");
            for finding in &report.findings {
                let _ = writeln!(out, "- {finding}");
            }
        }
        out.push('\n');
    }
    out
}
