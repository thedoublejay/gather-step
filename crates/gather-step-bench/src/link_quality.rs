#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt::Write as _,
    path::Path,
};

use gather_step_core::{NodeData, NodeId, NodeKind};
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
/// The evaluation walks from all nodes whose `file_path` matches the anchor and
/// collects nearby repos and file paths through outbound, inbound, and virtual
/// bridge edges. The result is compared to the task's expected sets to produce a
/// [`LinkQualityReport`].
pub fn run_link_quality_benchmark<S: GraphStore>(
    task: &LinkQualityTask,
    store: &S,
) -> LinkQualityReport {
    // Collect all nodes whose file_path starts with the anchor prefix.
    let anchor_nodes = collect_anchor_nodes(store, &task.anchor);

    let expected_edge_kinds: BTreeSet<String> =
        task.expected_edges.iter().map(|e| e.kind.clone()).collect();
    let anchor_repos = anchor_nodes
        .iter()
        .map(|node| node.repo.clone())
        .collect::<BTreeSet<_>>();
    let reached_nodes = collect_reachable_nodes(store, &anchor_nodes, &expected_edge_kinds);
    let reached_repos = reached_nodes
        .values()
        .filter(|(node, _)| !node.is_virtual)
        .map(|(node, _)| node.repo.clone())
        .collect::<BTreeSet<_>>();
    let reached_files = reached_nodes
        .values()
        .filter(|(node, _)| !node.is_virtual)
        .map(|(node, _)| node.file_path.clone())
        .collect::<BTreeSet<_>>();
    let cross_boundary_repos = reached_nodes
        .values()
        .filter(|(node, _)| !node.is_virtual && !anchor_repos.contains(&node.repo))
        .map(|(node, _)| node.repo.clone())
        .collect::<BTreeSet<_>>();
    let matching_cross_boundary_repos = reached_nodes
        .values()
        .filter(|(node, matched)| {
            !node.is_virtual && !anchor_repos.contains(&node.repo) && *matched
        })
        .map(|(node, _)| node.repo.clone())
        .collect::<BTreeSet<_>>();
    let total_cross_boundary = cross_boundary_repos.len();
    let matching_cross_boundary = matching_cross_boundary_repos.len();

    let missed_repos: Vec<String> = task
        .expected_repos
        .iter()
        .filter(|r| !reached_repos.contains(*r))
        .cloned()
        .collect();

    let missed_files: Vec<String> = task
        .expected_files
        .iter()
        .filter(|expected| {
            !reached_files
                .iter()
                .any(|reached| file_matches(expected, reached))
        })
        .cloned()
        .collect();

    let false_positive_repos: Vec<String> = reached_repos
        .iter()
        .filter(|r| !anchor_repos.contains(*r) && !task.expected_repos.contains(r))
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

const MAX_LINK_QUALITY_DEPTH: usize = 4;

fn collect_reachable_nodes<S: GraphStore>(
    store: &S,
    anchor_nodes: &[NodeData],
    expected_edge_kinds: &BTreeSet<String>,
) -> BTreeMap<NodeId, (NodeData, bool)> {
    let mut queue = VecDeque::new();
    let mut visited = BTreeSet::new();
    let mut reached = BTreeMap::new();

    for node in anchor_nodes {
        queue.push_back((node.id, 0usize, false));
        visited.insert((node.id, false));
        reached.insert(node.id, (node.clone(), false));
    }
    let shared_symbols = store
        .nodes_by_type(NodeKind::SharedSymbol)
        .unwrap_or_default()
        .into_iter()
        .filter(|node| !node.name.is_empty())
        .fold(
            BTreeMap::<String, Vec<NodeData>>::new(),
            |mut symbols, node| {
                symbols.entry(node.name.clone()).or_default().push(node);
                symbols
            },
        );

    while let Some((node_id, depth, matched_expected_edge)) = queue.pop_front() {
        if depth >= MAX_LINK_QUALITY_DEPTH {
            continue;
        }

        let Some((current_node, _)) = reached.get(&node_id) else {
            continue;
        };
        let mut adjacent = Vec::new();
        if let Ok(outgoing) = store.get_outgoing(node_id) {
            adjacent.extend(
                outgoing
                    .into_iter()
                    .map(|edge| (edge.target, format!("{:?}", edge.kind))),
            );
        }
        if let Ok(incoming) = store.get_incoming(node_id) {
            adjacent.extend(
                incoming
                    .into_iter()
                    .map(|edge| (edge.source, format!("{:?}", edge.kind))),
            );
        }
        if !current_node.is_virtual
            && let Some(symbols) = shared_symbols.get(&current_node.name)
        {
            adjacent.extend(
                symbols
                    .iter()
                    .map(|node| (node.id, "SharedSymbolNameBridge".to_owned())),
            );
        }

        for (next_id, edge_kind) in adjacent {
            let Ok(Some(next_node)) = store.get_node(next_id) else {
                continue;
            };
            let next_matched =
                matched_expected_edge || expected_edge_kinds.contains(edge_kind.as_str());
            reached
                .entry(next_id)
                .and_modify(|(_, matched)| *matched |= next_matched)
                .or_insert_with(|| (next_node.clone(), next_matched));

            if visited.insert((next_id, next_matched)) {
                queue.push_back((next_id, depth + 1, next_matched));
            }
        }
    }

    reached
}

fn file_matches(expected: &str, reached: &str) -> bool {
    expected == reached || expected.ends_with(reached) || reached.ends_with(expected)
}

fn collect_anchor_nodes<S: GraphStore>(store: &S, anchor: &str) -> Vec<gather_step_core::NodeData> {
    // Walk all File nodes and match by file_path or name.
    store
        .nodes_by_type(NodeKind::File)
        .unwrap_or_default()
        .into_iter()
        .filter(|n| {
            n.file_path == anchor
                || n.file_path.ends_with(anchor)
                || (anchor.ends_with(&n.file_path) && anchor.contains(&n.repo))
        })
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
