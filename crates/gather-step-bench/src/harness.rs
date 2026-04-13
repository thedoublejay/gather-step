#![forbid(unsafe_code)]

use std::{path::Path, time::Instant};

use gather_step_storage::{GraphStoreError, IndexingOptions, RepoIndexer, RepoIndexerError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::metrics::capture_rss;

/// Metrics collected from a single indexing pass over a fixture directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetrics {
    /// Wall-clock duration of the full index pass in milliseconds.
    pub parse_ms: u64,
    /// Total node count stored in the graph after indexing.
    pub graph_nodes: usize,
    /// Total edge count stored in the graph after indexing.
    pub graph_edges: usize,
    /// RSS growth in bytes during the index pass (after minus before), when
    /// measurement is available.
    pub memory_rss_growth_bytes: Option<u64>,
}

/// Errors that can arise during a benchmark index pass.
#[derive(Debug, Error)]
pub enum HarnessError {
    #[error("indexer error: {0}")]
    Indexer(#[from] RepoIndexerError),
    #[error("graph store error: {0}")]
    Graph(#[from] GraphStoreError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Run a full indexing pass over `fixture_path` and collect metrics.
///
/// This function uses a temporary storage directory so successive calls start
/// from a clean state.  The storage directory is removed when the function
/// returns.
///
/// `repo_name` is used as the logical repository name inside the graph store.
///
/// # Errors
///
/// Returns a [`HarnessError`] when the indexer fails to open, the index pass
/// fails, or the graph counts cannot be read.
pub fn run_index_pass(fixture_path: &Path, repo_name: &str) -> Result<IndexMetrics, HarnessError> {
    let storage_dir = tempdir_for_pass(fixture_path)?;
    let _guard = StorageDirGuard(storage_dir.clone());

    let rss_before = capture_rss();

    let t0 = Instant::now();
    let indexer = RepoIndexer::open(&storage_dir, IndexingOptions::default())?;
    indexer.index_repo(repo_name, fixture_path, None)?;
    let elapsed_ms = u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX);

    let rss_after = capture_rss();

    let graph = indexer.storage().graph();
    let graph_nodes = graph.count_nodes()?;
    let graph_edges = graph.count_edges()?;

    let memory_rss_growth_bytes = match (rss_before, rss_after) {
        (Some(before), Some(after)) => Some(after.saturating_sub(before)),
        _ => None,
    };

    Ok(IndexMetrics {
        parse_ms: elapsed_ms,
        graph_nodes,
        graph_edges,
        memory_rss_growth_bytes,
    })
}

/// Index `fixture_path` and return the live [`RepoIndexer`] together with a
/// storage guard.
///
/// The caller must hold `_guard` for as long as it needs the indexer's graph
/// store — dropping the guard removes the temporary storage directory.
///
/// # Errors
///
/// Returns a [`HarnessError`] when the indexer fails to open or the index pass
/// fails.
pub fn index_fixture(
    fixture_path: &Path,
    repo_name: &str,
) -> Result<(RepoIndexer, StorageDirGuard), HarnessError> {
    let storage_dir = tempdir_for_pass(fixture_path)?;
    let guard = StorageDirGuard(storage_dir.clone());
    let indexer = RepoIndexer::open(&storage_dir, IndexingOptions::default())?;
    indexer.index_repo(repo_name, fixture_path, None)?;
    Ok((indexer, guard))
}

/// Summary produced by comparing actual metrics against a recorded snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonReport {
    /// `true` when no regressions were detected.
    pub passed: bool,
    /// Human-readable descriptions of any findings (warnings or failures).
    pub findings: Vec<String>,
}

/// Compare `actual` metrics against a JSON snapshot file at `expected_path`.
///
/// Returns a `ComparisonReport` describing any deviations.  If `expected_path`
/// does not exist, the report is considered passing so that the first run can
/// establish the baseline.
pub fn compare_against_expected(actual: &IndexMetrics, expected_path: &Path) -> ComparisonReport {
    let raw = match std::fs::read_to_string(expected_path) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return ComparisonReport {
                passed: true,
                findings: vec![format!(
                    "no baseline found at {}; this run establishes the baseline",
                    expected_path.display()
                )],
            };
        }
        Err(e) => {
            return ComparisonReport {
                passed: false,
                findings: vec![format!(
                    "could not read expected file {}: {e}",
                    expected_path.display()
                )],
            };
        }
    };

    let expected: IndexMetrics = match serde_json::from_str(&raw) {
        Ok(m) => m,
        Err(e) => {
            return ComparisonReport {
                passed: false,
                findings: vec![format!("could not parse expected JSON: {e}")],
            };
        }
    };

    let mut findings = Vec::new();

    if actual.graph_nodes < expected.graph_nodes {
        findings.push(format!(
            "graph_nodes regression: expected >= {}, got {}",
            expected.graph_nodes, actual.graph_nodes
        ));
    }

    if actual.graph_edges < expected.graph_edges {
        findings.push(format!(
            "graph_edges regression: expected >= {}, got {}",
            expected.graph_edges, actual.graph_edges
        ));
    }

    ComparisonReport {
        passed: findings.is_empty(),
        findings,
    }
}

fn tempdir_for_pass(fixture_path: &Path) -> Result<std::path::PathBuf, HarnessError> {
    use std::{
        env,
        sync::atomic::{AtomicU64, Ordering},
    };
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let name = fixture_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("fixture");
    let dir = env::temp_dir().join(format!(
        "gather-step-bench-{name}-{}-{id}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

/// RAII guard that removes the storage directory on drop.
pub struct StorageDirGuard(std::path::PathBuf);

impl Drop for StorageDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
