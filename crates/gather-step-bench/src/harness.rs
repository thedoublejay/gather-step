#![forbid(unsafe_code)]

use std::{fs, path::Path, time::Instant};

use gather_step_core::{ConfigError, GatherStepConfig, RegistryError, RegistryStore};
use gather_step_storage::{
    GraphStoreError, IndexingOptions, RepoIndexer, RepoIndexerError, index_workspace_with_storage,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::metrics::capture_rss;

/// Byte-size breakdown of the on-disk storage written by an index pass.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StorageMetrics {
    pub graph_bytes: u64,
    pub metadata_bytes: u64,
    pub metadata_wal_bytes: u64,
    pub search_bytes: u64,
    pub total_bytes: u64,
}

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
    /// On-disk storage size written during the pass.
    #[serde(default)]
    pub storage: StorageMetrics,
    /// Number of repos indexed for workspace runs.
    #[serde(default)]
    pub indexed_repos: Option<usize>,
    /// Number of source files indexed for workspace runs.
    #[serde(default)]
    pub indexed_files: Option<u64>,
    /// Number of cross-repo edges counted after workspace finalization.
    #[serde(default)]
    pub cross_repo_edges: Option<u64>,
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
    #[error("config error: {0}")]
    Config(#[from] ConfigError),
    #[error("registry error: {0}")]
    Registry(#[from] RegistryError),
    #[error("workspace index error: {0}")]
    Workspace(String),
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
    indexer.storage().metadata().finalize();
    let storage = collect_storage_metrics(&storage_dir)?;

    let memory_rss_growth_bytes = match (rss_before, rss_after) {
        (Some(before), Some(after)) => Some(after.saturating_sub(before)),
        _ => None,
    };

    Ok(IndexMetrics {
        parse_ms: elapsed_ms,
        graph_nodes,
        graph_edges,
        memory_rss_growth_bytes,
        storage,
        indexed_repos: None,
        indexed_files: None,
        cross_repo_edges: None,
    })
}

/// Run a full indexing pass over every repo declared in a fixture workspace.
///
/// `fixture_path` must contain a `gather-step.config.yaml` file. The workspace
/// is indexed into a temporary storage directory and the resulting graph and
/// storage metrics are returned.
pub fn run_workspace_index_pass(fixture_path: &Path) -> Result<IndexMetrics, HarnessError> {
    let storage_dir = tempdir_for_pass(fixture_path)?;
    let _guard = StorageDirGuard(storage_dir.clone());
    let config = GatherStepConfig::from_yaml_file(fixture_path.join("gather-step.config.yaml"))?;
    let mut registry = RegistryStore::open(storage_dir.join("registry.json"))?;

    let rss_before = capture_rss();

    let t0 = Instant::now();
    let stats = index_workspace_with_storage(
        &config,
        fixture_path,
        &mut registry,
        &storage_dir,
        IndexingOptions::default(),
    )
    .map_err(|error| HarnessError::Workspace(error.to_string()))?;
    let elapsed_ms = u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX);

    let rss_after = capture_rss();

    let indexer = RepoIndexer::open(&storage_dir, IndexingOptions::default())?;
    let graph = indexer.storage().graph();
    let graph_nodes = graph.count_nodes()?;
    let graph_edges = graph.count_edges()?;
    indexer.storage().metadata().finalize();
    let storage = collect_storage_metrics(&storage_dir)?;

    let memory_rss_growth_bytes = match (rss_before, rss_after) {
        (Some(before), Some(after)) => Some(after.saturating_sub(before)),
        _ => None,
    };

    Ok(IndexMetrics {
        parse_ms: elapsed_ms,
        graph_nodes,
        graph_edges,
        memory_rss_growth_bytes,
        storage,
        indexed_repos: Some(stats.indexed_repos),
        indexed_files: Some(stats.total_files),
        cross_repo_edges: Some(stats.cross_repo_edges),
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
    fs::create_dir_all(&dir)?;
    Ok(dir)
}

fn collect_storage_metrics(storage_dir: &Path) -> Result<StorageMetrics, HarnessError> {
    let graph_bytes = file_size(storage_dir.join("graph.redb"))?;
    let metadata_bytes = file_size(storage_dir.join("metadata.sqlite"))?;
    let metadata_wal_bytes = file_size(storage_dir.join("metadata.sqlite-wal"))?
        + file_size(storage_dir.join("metadata.sqlite-shm"))?;
    let search_bytes = path_size(storage_dir.join("search"))?;
    let total_bytes = path_size(storage_dir)?;

    Ok(StorageMetrics {
        graph_bytes,
        metadata_bytes,
        metadata_wal_bytes,
        search_bytes,
        total_bytes,
    })
}

fn file_size(path: impl AsRef<Path>) -> Result<u64, HarnessError> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
    }
}

fn path_size(path: impl AsRef<Path>) -> Result<u64, HarnessError> {
    let path = path.as_ref();
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() {
        return Ok(0);
    }
    if metadata.is_file() {
        return Ok(metadata.len());
    }

    let mut total = 0_u64;
    for entry in fs::read_dir(path)? {
        total = total.saturating_add(path_size(entry?.path())?);
    }
    Ok(total)
}

/// RAII guard that removes the storage directory on drop.
pub struct StorageDirGuard(std::path::PathBuf);

impl Drop for StorageDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
