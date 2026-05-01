use std::{
    fs,
    fs::OpenOptions,
    path::Path,
    sync::Arc,
    thread,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use globset::{Glob, GlobSet, GlobSetBuilder};
use rustc_hash::FxHashMap;

use crossbeam_channel::bounded;
use gather_step_core::{
    DeploymentConfig, EdgeData, EdgeKind, EdgeMetadata, GatherStepConfig, NodeData, NodeKind,
    node_id, normalize_path_separators, ref_node_id,
};
use gather_step_deploy::{
    DeploymentArtifactKind, DeploymentParseOutput, detect_artifact_kind, parse_deployment_artifact,
    parse_deployment_artifact_with_kind,
};
use gather_step_parser::{
    CallSite, FileEntry as SourceFileEntry, FileStat, ManifestError, ParseError, ParsedFile,
    TraverseConfig, TraverseError, collect_repo_files, extract_package_manifest,
    frameworks::{Framework, detect_frameworks, local_config::LocalConfig},
    infer_payload_contracts, parse_file_with_context, parse_file_with_packs,
    resolve::ResolutionInput,
    resolve_calls_with_unresolved,
    tsconfig::PathAliases,
    workspace_manifest::{discover_workspace_packages, find_workspace_root},
};
use rayon::prelude::*;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    ChangedSet, FileBatch, GraphStore, GraphStoreDb, GraphStoreError, IncrementalError, RepoBatch,
    RepoBatchHooks, StorageCoordinator, StorageCoordinatorError, StorageReconcileError,
    WorkspaceStores, classify_changes, classify_selected_changes, compute_affected_set,
    incremental::{TrackedPath, snapshot_repo_files, snapshot_selected_repo_files},
    metadata::{FileIndexState, MetadataStoreError, PayloadContractStoreRecord},
    reconcile_changed_files_with_mode,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexingOptions {
    pub traverse: TraverseConfig,
    pub batch_size: usize,
    pub deployment: DeploymentIndexingOptions,
}

impl Default for IndexingOptions {
    fn default() -> Self {
        Self {
            traverse: TraverseConfig::default(),
            batch_size: 500,
            deployment: DeploymentIndexingOptions::default(),
        }
    }
}

impl IndexingOptions {
    #[must_use]
    pub fn from_config(config: &GatherStepConfig) -> Self {
        Self {
            deployment: DeploymentIndexingOptions::from(&config.deployment),
            ..Self::default()
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeploymentIndexingOptions {
    pub include: Vec<String>,
    pub gitops_roots: Vec<String>,
    pub env_files: Vec<String>,
}

impl From<&DeploymentConfig> for DeploymentIndexingOptions {
    fn from(config: &DeploymentConfig) -> Self {
        Self {
            include: config.include.clone(),
            gitops_roots: config.gitops_roots.clone(),
            env_files: config.env_files.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexProgress {
    pub phase: &'static str,
    pub processed: usize,
    pub total: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IndexingStats {
    pub files_parsed: usize,
    pub nodes_created: usize,
    pub edges_created: usize,
    pub duration_ms: u128,
}

fn millis_u64(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn nonnegative_i64_to_u64(value: i64) -> u64 {
    value.max(0).cast_unsigned()
}

const MAX_DEPLOYMENT_ARTIFACT_BYTES: u64 = 1024 * 1024;

pub struct RepoIndexer {
    storage: StorageCoordinator,
    options: IndexingOptions,
}

#[derive(Debug, Error)]
pub enum RepoIndexerError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Incremental(#[from] IncrementalError),
    #[error(transparent)]
    Traverse(#[from] TraverseError),
    #[error(transparent)]
    Parse(#[from] ParseError),
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error(transparent)]
    Metadata(#[from] MetadataStoreError),
    #[error(transparent)]
    Storage(#[from] StorageCoordinatorError),
    #[error(transparent)]
    Graph(#[from] GraphStoreError),
    #[error(transparent)]
    Reconcile(#[from] StorageReconcileError),
    #[error("invalid deployment include glob `{pattern}`: {source}")]
    DeploymentGlob {
        pattern: String,
        source: globset::Error,
    },
    #[error("parse worker channel closed unexpectedly")]
    ChannelClosed,
    #[error("writer thread panicked")]
    WriterPanicked,
    #[error("incremental indexing worker panicked")]
    IncrementalWorkerPanicked,
    #[error("indexing cancelled")]
    Cancelled,
}

enum WriteMessage {
    Batch(RepoBatch),
    CrossFileEdgeBatch(Vec<EdgeData>),
}

/// RAII guard that enables bulk mode on the graph store for the duration of a
/// batch write session and reliably disables it when dropped — even if the
/// caller panics.
///
/// Construct via [`BulkModeGuard::new`] and let the value drop at the end of
/// the bulk region.  Do not call [`GraphStoreDb::set_bulk_mode`] directly
/// inside a scope that uses this guard.
///
/// # Nesting
///
/// The guard is backed by an [`AtomicBool`][std::sync::atomic::AtomicBool] —
/// **not** a depth counter.  Creating an inner guard while an outer guard is
/// live is therefore unsafe: the inner guard's `Drop` would prematurely
/// disable bulk mode on the outer scope.  Call sites avoid this by checking
/// [`GraphStoreDb::is_bulk_mode`] before constructing a nested guard and
/// binding `None` when a workspace-level guard is already active (see
/// `commit_repo_payload` for the pattern).
///
/// # Durability on drop
///
/// While bulk mode is active [`GraphStoreDb::begin_write_txn`] uses
/// `Durability::None`, so per-commit pages remain unsynced in the OS page
/// cache.  On drop this guard (1) flips the atomic flag so any subsequent
/// transactions default back to `Durability::Immediate`, then (2) commits
/// an empty write transaction to force a fsync of everything written under
/// None durability.  Without step 2 an `index` run that performs no further
/// graph writes after the bulk session (e.g. a cold index on a workspace
/// with no new git commits) would leave recent pages on disk but unsynced —
/// a crash before the kernel flushed them would lose the index.
pub struct BulkModeGuard<'a> {
    graph: &'a GraphStoreDb,
}

impl<'a> BulkModeGuard<'a> {
    pub(crate) fn new(graph: &'a GraphStoreDb) -> Self {
        graph.set_bulk_mode(true);
        Self { graph }
    }
}

impl Drop for BulkModeGuard<'_> {
    fn drop(&mut self) {
        // Step 1: flip the flag so the empty commit below (and any caller
        // work that happens after Drop returns) uses Immediate durability.
        self.graph.set_bulk_mode(false);
        // Step 2: force a fsync of accumulated None-durability pages by
        // committing an empty Immediate-durability transaction.  Drop cannot
        // propagate errors via `?`; log and continue — the alternative is
        // silent data-loss risk if the caller never writes again before
        // process exit.
        if let Err(error) = self.graph.commit_durable_marker() {
            tracing::warn!(
                %error,
                "BulkModeGuard drop: durable marker commit failed; \
                 bulk-mode writes may not be fsynced until the next commit",
            );
        }
    }
}

/// Everything produced by the parse+resolve phase for one repo. Fully owned,
/// no borrows — safe to send through channels or collect across rayon workers.
pub struct RepoIndexPayload {
    pub repo: String,
    pub files: Vec<FileBatch>,
    pub deferred_cross_file_edges: Vec<EdgeData>,
    pub unresolved_inputs: Vec<gather_step_parser::resolve::ResolutionInput>,
    pub payload_records: Vec<PayloadContractStoreRecord>,
    pub file_states: Vec<FileIndexState>,
    /// Display strings for the indexed files.  Used by the reconcile path
    /// (graph-store lookups, dependency writes) which keys on the lossy
    /// UTF-8 form.  For identity keys in `SQLite` `BLOB` columns use
    /// `indexed_path_ids` instead.
    pub indexed_file_paths: Vec<String>,
    /// Raw `OsStr` bytes for each indexed file — the authoritative identity
    /// key for `SQLite` `BLOB` comparisons.  Populated from
    /// `PathId::from_path(&file.path).as_bytes()` at parse time so
    /// byte-distinct non-UTF-8 filenames are never collapsed.
    pub indexed_path_ids: Vec<Vec<u8>>,
    pub is_cold_index: bool,
    pub stats: IndexingStats,
    pub synthetic_file_count: usize,
}

struct RepoIndexLockGuard {
    file: std::fs::File,
}

impl Drop for RepoIndexLockGuard {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

impl RepoIndexer {
    pub fn open(
        storage_root: impl AsRef<Path>,
        options: IndexingOptions,
    ) -> Result<Self, RepoIndexerError> {
        let stores = WorkspaceStores::open(storage_root).map_err(StorageCoordinatorError::from)?;
        Self::open_with_stores(stores, options)
    }

    pub fn open_with_stores(
        stores: WorkspaceStores,
        options: IndexingOptions,
    ) -> Result<Self, RepoIndexerError> {
        Ok(Self {
            storage: StorageCoordinator::from_stores(stores),
            options,
        })
    }

    #[must_use]
    pub fn storage(&self) -> &StorageCoordinator {
        &self.storage
    }

    /// Compact the graph database to reclaim dead space. Call after a full
    /// cold-index run completes. No-op if the database is already compact.
    pub fn compact_graph(&mut self) -> Result<bool, RepoIndexerError> {
        Ok(self.storage.compact_graph()?)
    }

    /// Begin a workspace-level bulk write session that spans multiple
    /// [`commit_repo_payload`] calls.
    ///
    /// Holding the returned [`BulkModeGuard`] for the lifetime of a serial
    /// commit loop keeps `Durability::None` active across all repos, replacing
    /// O(n-repos) redb syncs with a single sync when the guard drops.  Call
    /// [`commit_repo_payload`] normally inside the loop — it detects the active
    /// bulk session and skips creating its own inner guard.
    ///
    /// # Panics
    ///
    /// Panics if called while a bulk session is already active (i.e. this
    /// method must not be nested).
    pub fn begin_workspace_bulk_session(&self) -> BulkModeGuard<'_> {
        assert!(
            !self.storage.graph().is_bulk_mode(),
            "begin_workspace_bulk_session called while a bulk session is already active"
        );
        BulkModeGuard::new(self.storage.graph())
    }

    /// Parse and resolve a repo without writing to storage. Returns a payload
    /// that can be committed later via [`commit_repo_payload`].
    pub fn prepare_repo_payload(
        &self,
        repo: &str,
        repo_root: impl AsRef<Path>,
    ) -> Result<RepoIndexPayload, RepoIndexerError> {
        let repo_root = repo_root.as_ref();
        let frameworks: Vec<Framework> = detect_frameworks(repo_root).into_iter().collect();
        self.prepare_repo_payload_with_frameworks(repo, repo_root, &frameworks)
    }

    pub fn prepare_repo_payload_with_frameworks(
        &self,
        repo: &str,
        repo_root: impl AsRef<Path>,
        detected_frameworks: &[Framework],
    ) -> Result<RepoIndexPayload, RepoIndexerError> {
        let _repo_lock = self.acquire_repo_lock(repo, None)?;
        let repo_root = repo_root.as_ref().to_path_buf();
        let traversal = collect_repo_files(&repo_root, &self.options.traverse)?;
        let include_manifest_batch = has_indexable_manifest(&repo_root);
        Self::prepare_repo_files(
            repo,
            &repo_root,
            &traversal.files,
            &traversal.file_stats,
            include_manifest_batch,
            false,
            detected_frameworks,
        )
    }

    /// Write a prepared payload to storage, reconcile, and update metadata.
    pub fn commit_repo_payload(
        &self,
        mut payload: RepoIndexPayload,
    ) -> Result<IndexingStats, RepoIndexerError> {
        let started_at = Instant::now();
        let repo = payload.repo.clone();
        let repo = repo.as_str();

        self.storage
            .metadata()
            .clear_semantic_metadata_for_files(repo, &payload.indexed_path_ids)?;

        let pre_write_node_count = self.storage.graph().count_nodes_by_repo(repo).unwrap_or(0);

        let write_start = Instant::now();
        let is_cold_index_pre = pre_write_node_count == 0;
        let batch_size = self.options.batch_size.max(1);
        let mut stats = {
            let mut stats = IndexingStats::default();
            // BulkModeGuard enables Durability::None for the duration of this
            // block and restores normal durability on drop — even if the block
            // exits via panic or early `?` return.
            // If a workspace-level bulk session is already active (see
            // `begin_workspace_bulk_session`), skip creating an inner guard so
            // the workspace-level guard's lifetime controls when durability is
            // restored.
            let _bulk_guard = if self.storage.graph().is_bulk_mode() {
                None
            } else {
                Some(BulkModeGuard::new(self.storage.graph()))
            };
            // Ship owned FileBatch values without deep-cloning their
            // Vec<NodeData>/Vec<EdgeData>.  Consuming with `by_ref().take()`
            // avoids the O(n) memmove that `Vec::drain(..take)` from the
            // front would require on every iteration.
            let mut files_iter = std::mem::take(&mut payload.files).into_iter();
            loop {
                let files: Vec<FileBatch> = files_iter.by_ref().take(batch_size).collect();
                if files.is_empty() {
                    break;
                }
                let batch = RepoBatch {
                    repo: repo.to_owned(),
                    files,
                    test_hooks: RepoBatchHooks::default(),
                };
                let result = self
                    .storage
                    .index_repo_batch_without_file_states_cold(&batch, is_cold_index_pre)?;
                stats.files_parsed += result.files_indexed;
                stats.nodes_created += result.nodes_written;
                stats.edges_created += result.edges_written;
            }
            if !payload.deferred_cross_file_edges.is_empty() {
                let edge_count = payload.deferred_cross_file_edges.len();
                self.storage.graph().with_write_txn(|write_txn| {
                    GraphStoreDb::bulk_insert_edges_in_txn(
                        write_txn,
                        &payload.deferred_cross_file_edges,
                    )
                })?;
                stats.edges_created += edge_count;
            }
            stats
            // `_bulk` drops here, restoring normal durability.
        };
        let write_elapsed = write_start.elapsed();
        info!(
            repo,
            write_ms = millis_u64(write_elapsed),
            nodes = stats.nodes_created,
            edges = stats.edges_created,
            "stage timing: graph write complete",
        );

        let reconcile_start = Instant::now();
        let is_cold_index = pre_write_node_count == 0 && stats.nodes_created > 0;
        let changed_files = payload
            .indexed_file_paths
            .iter()
            .cloned()
            .zip(payload.indexed_path_ids.iter().cloned())
            .map(|(path, path_id_bytes)| TrackedPath {
                path,
                path_id_bytes,
            })
            .collect::<Vec<_>>();
        reconcile_changed_files_with_mode(&self.storage, repo, &changed_files, is_cold_index)
            .map_err(|error| {
                warn!(repo, error = %error, "reconciliation failed after indexing");
                error
            })?;
        info!(
            repo,
            reconcile_ms = millis_u64(reconcile_start.elapsed()),
            "stage timing: reconcile complete",
        );

        self.storage.metadata().replace_index_metadata_for_files(
            repo,
            &payload.indexed_path_ids,
            &payload.unresolved_inputs,
            &payload.payload_records,
            &payload.file_states,
        )?;

        stats.files_parsed = stats
            .files_parsed
            .saturating_sub(payload.synthetic_file_count);
        stats.duration_ms = started_at.elapsed().as_millis();
        Ok(stats)
    }

    pub fn index_repo(
        &self,
        repo: &str,
        repo_root: impl AsRef<Path>,
        progress: Option<&dyn Fn(IndexProgress)>,
    ) -> Result<IndexingStats, RepoIndexerError> {
        self.index_repo_cancellable(repo, repo_root, None, progress)
    }

    pub fn index_repo_with_frameworks(
        &self,
        repo: &str,
        repo_root: impl AsRef<Path>,
        detected_frameworks: &[Framework],
        progress: Option<&dyn Fn(IndexProgress)>,
    ) -> Result<IndexingStats, RepoIndexerError> {
        self.index_repo_cancellable_with_frameworks(
            repo,
            repo_root,
            detected_frameworks,
            None,
            progress,
        )
    }

    pub fn index_repo_cancellable(
        &self,
        repo: &str,
        repo_root: impl AsRef<Path>,
        cancel: Option<&CancellationToken>,
        progress: Option<&dyn Fn(IndexProgress)>,
    ) -> Result<IndexingStats, RepoIndexerError> {
        let repo_root = repo_root.as_ref();
        let frameworks: Vec<Framework> = detect_frameworks(repo_root).into_iter().collect();
        self.index_repo_cancellable_with_frameworks(repo, repo_root, &frameworks, cancel, progress)
    }

    fn index_repo_cancellable_with_frameworks(
        &self,
        repo: &str,
        repo_root: impl AsRef<Path>,
        detected_frameworks: &[Framework],
        cancel: Option<&CancellationToken>,
        progress: Option<&dyn Fn(IndexProgress)>,
    ) -> Result<IndexingStats, RepoIndexerError> {
        let _repo_lock = self.acquire_repo_lock(repo, cancel)?;
        let repo_root = repo_root.as_ref().to_path_buf();
        let traversal = collect_repo_files(&repo_root, &self.options.traverse)?;
        Self::check_cancel(cancel)?;
        let include_manifest_batch = has_indexable_manifest(&repo_root);
        let mut stats = self.index_repo_files(
            repo,
            &repo_root,
            &traversal.files,
            &traversal.file_stats,
            include_manifest_batch,
            false,
            detected_frameworks,
            cancel,
            progress,
        )?;
        let deployment_stats = self.index_deployment_artifacts(repo, &repo_root, progress)?;
        stats.files_parsed = stats
            .files_parsed
            .saturating_add(deployment_stats.files_parsed);
        stats.nodes_created = stats
            .nodes_created
            .saturating_add(deployment_stats.nodes_created);
        stats.edges_created = stats
            .edges_created
            .saturating_add(deployment_stats.edges_created);
        stats.duration_ms = stats
            .duration_ms
            .saturating_add(deployment_stats.duration_ms);
        Ok(stats)
    }

    fn index_deployment_artifacts(
        &self,
        repo: &str,
        repo_root: &Path,
        progress: Option<&dyn Fn(IndexProgress)>,
    ) -> Result<IndexingStats, RepoIndexerError> {
        let started_at = Instant::now();
        let indexed_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .try_into()
            .unwrap_or(i64::MAX);
        let candidates = collect_deployment_artifact_paths(repo_root, &self.options.deployment)?;
        if let Some(progress) = progress {
            progress(IndexProgress {
                phase: "deployment",
                processed: 0,
                total: candidates.len(),
            });
        }

        let mut stats = IndexingStats::default();
        for (index, relative_path) in candidates.iter().enumerate() {
            if let Some(progress) = progress {
                progress(IndexProgress {
                    phase: "deployment",
                    processed: index,
                    total: candidates.len(),
                });
            }

            let full_path = repo_root.join(relative_path);
            let metadata = file_metadata_stamp(&full_path)?;
            if u64::try_from(metadata.size_bytes).unwrap_or(u64::MAX)
                > MAX_DEPLOYMENT_ARTIFACT_BYTES
            {
                warn!(
                    repo,
                    path = %relative_path.display(),
                    size_bytes = metadata.size_bytes,
                    "skipping oversized deployment artifact"
                );
                continue;
            }
            let bytes = fs::read(&full_path)?;
            let content = String::from_utf8_lossy(&bytes);
            let file_path =
                normalize_path_separators(&relative_path.to_string_lossy()).into_owned();
            let forced_artifact_kind = self
                .options
                .deployment
                .forced_artifact_kind_for_path(&file_path);
            let output = match forced_artifact_kind.map_or_else(
                || parse_deployment_artifact(repo, &file_path, &content),
                |artifact_kind| {
                    parse_deployment_artifact_with_kind(repo, &file_path, &content, artifact_kind)
                },
            ) {
                Ok(output) => output,
                Err(error) => {
                    warn!(repo, path = %file_path, error = %error, "skipping malformed deployment artifact");
                    continue;
                }
            };
            if output.artifact_kind == DeploymentArtifactKind::Unknown {
                continue;
            }

            let batch = deployment_output_to_batch(
                repo, &file_path, &bytes, metadata, indexed_at, output, false,
            );
            let result = self.storage.index_repo_batch(&RepoBatch {
                repo: repo.to_owned(),
                files: vec![batch],
                test_hooks: RepoBatchHooks::default(),
            })?;
            stats.files_parsed = stats.files_parsed.saturating_add(result.files_indexed);
            stats.nodes_created = stats.nodes_created.saturating_add(result.nodes_written);
            stats.edges_created = stats.edges_created.saturating_add(result.edges_written);
        }

        if let Some(progress) = progress {
            progress(IndexProgress {
                phase: "deployment",
                processed: candidates.len(),
                total: candidates.len(),
            });
        }
        stats.duration_ms = started_at.elapsed().as_millis();
        Ok(stats)
    }

    pub fn index_repo_incremental(
        &self,
        repo: &str,
        repo_root: impl AsRef<Path>,
        progress: Option<&dyn Fn(IndexProgress)>,
    ) -> Result<(ChangedSet, IndexingStats), RepoIndexerError> {
        self.index_repo_incremental_with_hint(repo, repo_root, None, progress)
    }

    pub fn index_repo_incremental_with_hint(
        &self,
        repo: &str,
        repo_root: impl AsRef<Path>,
        changed_paths_hint: Option<&[String]>,
        progress: Option<&dyn Fn(IndexProgress)>,
    ) -> Result<(ChangedSet, IndexingStats), RepoIndexerError> {
        self.index_repo_incremental_with_hint_cancellable(
            repo,
            repo_root,
            changed_paths_hint,
            None,
            progress,
        )
    }

    pub fn index_repo_incremental_with_hint_cancellable(
        &self,
        repo: &str,
        repo_root: impl AsRef<Path>,
        changed_paths_hint: Option<&[String]>,
        cancel: Option<&CancellationToken>,
        progress: Option<&dyn Fn(IndexProgress)>,
    ) -> Result<(ChangedSet, IndexingStats), RepoIndexerError> {
        let _repo_lock = self.acquire_repo_lock(repo, cancel)?;
        let started_at = Instant::now();
        let repo_root = repo_root.as_ref().to_path_buf();
        let snapshot = match changed_paths_hint {
            Some(paths) if !paths.is_empty() => {
                snapshot_selected_repo_files(&repo_root, paths, &self.options.traverse)?
            }
            _ => snapshot_repo_files(
                self.storage.metadata(),
                repo,
                &repo_root,
                &self.options.traverse,
            )?,
        };
        Self::check_cancel(cancel)?;
        let changed = match changed_paths_hint {
            Some(paths) if !paths.is_empty() => {
                classify_selected_changes(self.storage.metadata(), repo, &snapshot, paths)?
            }
            _ => classify_changes(self.storage.metadata(), repo, &snapshot)?,
        };
        let changed_paths = changed
            .added
            .iter()
            .map(|file| TrackedPath {
                path: file.path.clone(),
                path_id_bytes: file.path_id_bytes.clone(),
            })
            .chain(changed.modified.iter().map(|file| TrackedPath {
                path: file.path.clone(),
                path_id_bytes: file.path_id_bytes.clone(),
            }))
            .chain(changed.deleted.iter().map(|file| TrackedPath {
                path: file.path.clone(),
                path_id_bytes: file.path_id_bytes.clone(),
            }))
            .collect::<Vec<_>>();
        if changed_paths.is_empty() {
            return Ok((
                changed,
                IndexingStats {
                    duration_ms: started_at.elapsed().as_millis(),
                    ..IndexingStats::default()
                },
            ));
        }

        let affected_paths = compute_affected_set(self.storage.metadata(), repo, &changed_paths)?;
        Self::check_cancel(cancel)?;
        let affected_path_strings = affected_paths
            .iter()
            .map(|path| path.path.clone())
            .collect::<Vec<_>>();
        let indexing_snapshot = match changed_paths_hint {
            Some(_) => snapshot_selected_repo_files(
                &repo_root,
                &affected_path_strings,
                &self.options.traverse,
            )?,
            None => snapshot,
        };
        let source_files_by_path = indexing_snapshot
            .source_files
            .into_iter()
            // Key on raw OsStr bytes so two byte-distinct non-UTF-8 filenames
            // are never collapsed to the same entry under to_string_lossy.
            .map(|file| {
                let key = gather_step_core::PathId::from_path(&file.path)
                    .as_bytes()
                    .to_vec();
                (key, file)
            })
            .collect::<rustc_hash::FxHashMap<_, _>>();
        let files_to_index = affected_paths
            .iter()
            .filter_map(|path| {
                source_files_by_path
                    .get(path.path_id_bytes.as_slice())
                    .cloned()
            })
            .collect::<Vec<_>>();
        let include_manifest_batch = affected_paths
            .iter()
            .any(|path| path.path == "package.json")
            && indexing_snapshot
                .files_by_path
                .contains_key(b"package.json".as_ref());

        let deleted_paths = changed
            .deleted
            .iter()
            .map(|file| file.path.clone())
            .collect::<Vec<_>>();
        self.storage.purge_deleted_files(repo, &deleted_paths)?;
        let detected_frameworks = detect_frameworks(&repo_root)
            .into_iter()
            .collect::<Vec<_>>();
        let stats = self.index_repo_files(
            repo,
            &repo_root,
            &files_to_index,
            &indexing_snapshot.file_stats,
            include_manifest_batch,
            true,
            &detected_frameworks,
            cancel,
            progress,
        )?;
        Ok((changed, stats))
    }

    /// Acquire the per-repo filesystem lock, checking for cancellation while
    /// waiting so that `watch --cancel` and `serve --shutdown` are not blocked
    /// indefinitely when another process holds the lock.
    ///
    /// Uses `try_lock()` + exponential back-off starting at 10 ms and doubling
    /// on each retry up to a 500 ms ceiling.  Returns
    /// [`RepoIndexerError::Cancelled`] as soon as the token is set.
    fn acquire_repo_lock(
        &self,
        repo: &str,
        cancel: Option<&CancellationToken>,
    ) -> Result<RepoIndexLockGuard, RepoIndexerError> {
        const BACKOFF_INITIAL_MS: u64 = 10;
        const BACKOFF_MAX_MS: u64 = 500;

        let lock_dir = self.storage.root().join("locks");
        fs::create_dir_all(&lock_dir)?;
        let lock_name = format!("{}.lock", blake3::hash(repo.as_bytes()).to_hex());
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(lock_dir.join(lock_name))?;
        let wait_started = Instant::now();
        let mut backoff_ms = BACKOFF_INITIAL_MS;
        loop {
            if cancel.is_some_and(CancellationToken::is_cancelled) {
                return Err(RepoIndexerError::Cancelled);
            }
            match file.try_lock() {
                Ok(()) => break,
                Err(std::fs::TryLockError::WouldBlock) => {
                    // Lock is held by another process — wait with back-off.
                    std::thread::sleep(std::time::Duration::from_millis(backoff_ms));
                    backoff_ms = (backoff_ms * 2).min(BACKOFF_MAX_MS);
                }
                Err(std::fs::TryLockError::Error(error)) => {
                    return Err(RepoIndexerError::Io(error));
                }
            }
        }
        let wait_ms = wait_started.elapsed().as_millis();
        if wait_ms > 0 {
            info!(repo, wait_ms, "acquired repo index lock after waiting");
        }
        Ok(RepoIndexLockGuard { file })
    }

    fn prepare_repo_files(
        repo: &str,
        repo_root: &Path,
        files_to_index: &[SourceFileEntry],
        traversal_stats: &FxHashMap<Vec<u8>, FileStat>,
        include_manifest_batch: bool,
        force_rewrite: bool,
        detected_frameworks: &[Framework],
    ) -> Result<RepoIndexPayload, RepoIndexerError> {
        let started_at = Instant::now();
        let indexed_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| i64::try_from(duration.as_secs()).unwrap_or(i64::MAX))
            .unwrap_or_default();

        let path_aliases = Arc::new(build_path_aliases(repo_root));
        let repo_path_str = repo_root.to_string_lossy();
        let local_config = LocalConfig::load(repo_root);

        let (sender, receiver) = bounded::<Result<ParsedFile, ParseError>>(64);
        let repo_name = Arc::new(repo.to_owned());
        let root = Arc::new(repo_root.to_path_buf());

        let mut files_parsed = 0_usize;
        // Accumulated for the whole repo so the resolver can build its SymbolIndex across all files.
        let mut symbol_nodes = Vec::new();
        // Accumulated for the whole repo so the resolver can build its SymbolIndex across all files.
        let mut resolution_inputs = Vec::new();
        let mut payload_records = Vec::<PayloadContractStoreRecord>::new();
        let mut files = Vec::new();
        // Keyed by `Arc<[u8]>` of PathId bytes so two byte-distinct non-UTF-8
        // filenames are never collapsed to the same map entry.
        let mut file_positions: rustc_hash::FxHashMap<Arc<[u8]>, usize> =
            rustc_hash::FxHashMap::default();
        let mut node_to_file: rustc_hash::FxHashMap<gather_step_core::NodeId, Arc<[u8]>> =
            rustc_hash::FxHashMap::default();
        thread::scope(|scope| -> Result<(), RepoIndexerError> {
            let producer = scope.spawn(|| {
                if let Some(packs) = local_config
                    .as_ref()
                    .and_then(|cfg| cfg.packs_for_repo(repo_path_str.as_ref()))
                {
                    let packs = Arc::<[_]>::from(packs);
                    let _: Result<(), ()> = files_to_index.par_iter().try_for_each_with(
                        sender.clone(),
                        |sender, file| {
                            let parsed = parse_file_with_packs(
                                repo_name.as_str(),
                                root.as_path(),
                                file,
                                packs.as_ref(),
                                path_aliases.as_ref(),
                            );
                            sender.send(parsed).map_err(|_| ())?;
                            Ok(())
                        },
                    );
                } else {
                    let frameworks = Arc::<[Framework]>::from(detected_frameworks.to_vec());
                    let _: Result<(), ()> = files_to_index.par_iter().try_for_each_with(
                        sender.clone(),
                        |sender, file| {
                            let parsed = parse_file_with_context(
                                repo_name.as_str(),
                                root.as_path(),
                                file,
                                frameworks.as_ref(),
                                path_aliases.as_ref(),
                            );
                            sender.send(parsed).map_err(|_| ())?;
                            Ok(())
                        },
                    );
                }
                drop(sender);
            });

            for message in receiver {
                let parsed = message?;
                files_parsed += 1;
                let inferred_payloads = infer_payload_contracts(&parsed);

                let ParsedFile {
                    file,
                    file_node,
                    nodes,
                    edges,
                    symbols,
                    call_sites,
                    import_bindings,
                    parse_ms,
                    ..
                } = parsed;

                // Display string for the FileBatch.file_path field and logging.
                // Lossy UTF-8 is acceptable here because this string is only
                // used for rendering and the reconcile graph-store path (which
                // also keys on display strings).
                let file_path_str =
                    normalize_path_separators(&file.path.to_string_lossy()).into_owned();
                // Lossless identity bytes for the SQLite BLOB column.
                let path_id_bytes = gather_step_core::PathId::from_path(&file.path)
                    .as_bytes()
                    .to_vec();
                // Arc<[u8]> backed by PathId bytes: used as the identity key in
                // file_positions and node_to_file.  Two byte-distinct non-UTF-8
                // filenames that are equal under to_string_lossy are distinct here.
                let file_path_arc: Arc<[u8]> = Arc::from(path_id_bytes.clone().into_boxed_slice());
                // Use the original PathBuf (not a lossy round-trip through String)
                // for ResolutionInput and CallSite so imports/call resolution
                // and the unresolved_call_candidates BLOB keys are byte-exact.
                let source_path = file.path.clone();
                let call_sites = call_sites
                    .into_iter()
                    .map(|call| CallSite {
                        owner_id: call.owner_id,
                        owner_file: call.owner_file,
                        source_path: source_path.clone(),
                        callee_name: call.callee_name,
                        callee_qualified_hint: call.callee_qualified_hint,
                        span: call.span,
                    })
                    .collect();
                let payload_nodes = inferred_payloads
                    .iter()
                    .map(|contract| contract.node.clone())
                    .collect::<Vec<_>>();
                let payload_edges = inferred_payloads
                    .iter()
                    .map(|contract| contract.edge.clone())
                    .collect::<Vec<_>>();
                payload_records.extend(inferred_payloads.into_iter().map(|contract| {
                    PayloadContractStoreRecord {
                        record: contract.record,
                    }
                }));

                symbol_nodes.extend(symbols.into_iter().map(|symbol| symbol.node));
                resolution_inputs.push(gather_step_parser::resolve::ResolutionInput {
                    file_node: file_node.id,
                    file_path: file.path.clone(),
                    import_bindings,
                    call_sites,
                });
                node_to_file.extend(
                    resolution_inputs
                        .last()
                        .into_iter()
                        .flat_map(|input| input.call_sites.iter())
                        .map(|call_site| (call_site.owner_id, Arc::clone(&file_path_arc))),
                );
                file_positions.insert(Arc::clone(&file_path_arc), files.len());
                let file_stat = traversal_stats
                    .get(path_id_bytes.as_slice())
                    .copied()
                    .unwrap_or_else(|| {
                        file_metadata_stamp(root.join(&file.path)).unwrap_or_default()
                    });
                files.push(FileBatch {
                    repo: repo.to_owned(),
                    file_path: file_path_str,
                    path_id_bytes,
                    nodes: nodes.into_iter().chain(payload_nodes).collect(),
                    edges: edges.into_iter().chain(payload_edges).collect(),
                    content_hash: file.content_hash.to_vec(),
                    size_bytes: file_stat.size_bytes,
                    mtime_ns: file_stat.mtime_ns,
                    indexed_at,
                    parse_ms: Some(parse_ms),
                    force: force_rewrite,
                });
            }

            producer
                .join()
                .map_err(|_| RepoIndexerError::ChannelClosed)?;
            Ok(())
        })?;

        let resolution =
            resolve_calls_with_unresolved(repo_root, &symbol_nodes, &resolution_inputs);

        let mut deferred_cross_file_edges = Vec::new();
        for resolved_call in resolution.resolved {
            let owner_path = node_to_file
                .get(&resolved_call.edge.source)
                .ok_or(RepoIndexerError::ChannelClosed)?;
            if resolved_call.edge.is_cross_file {
                deferred_cross_file_edges.push(resolved_call.edge);
            } else if let Some(index) = file_positions.get(owner_path.as_ref()).copied() {
                files[index].edges.push(resolved_call.edge);
            }
        }

        let mut synthetic_file_count = 0_usize;
        if include_manifest_batch
            && let Some(mut manifest_batch) = build_manifest_batch(repo, repo_root, indexed_at)
        {
            manifest_batch.force = force_rewrite;
            files.push(manifest_batch);
            synthetic_file_count += 1;
        }

        files.sort_by(|left, right| left.file_path.cmp(&right.file_path));
        // Collapse the three sequential projections into a single iterator pass.
        let mut indexed_file_paths = Vec::with_capacity(files.len());
        let mut indexed_path_ids = Vec::with_capacity(files.len());
        let mut file_states = Vec::with_capacity(files.len());
        for file in &files {
            indexed_file_paths.push(file.file_path.clone());
            indexed_path_ids.push(file.path_id_bytes.clone());
            file_states.push(FileIndexState {
                repo: file.repo.clone(),
                file_path: file.file_path.clone(),
                path_id_bytes: file.path_id_bytes.clone(),
                content_hash: file.content_hash.clone(),
                size_bytes: file.size_bytes,
                mtime_ns: file.mtime_ns,
                node_count: i64::try_from(file.nodes.len()).unwrap_or(i64::MAX),
                edge_count: i64::try_from(file.edges.len()).unwrap_or(i64::MAX),
                indexed_at: file.indexed_at,
                parse_ms: file.parse_ms,
            });
        }

        let stats = IndexingStats {
            files_parsed: files_parsed.saturating_sub(synthetic_file_count),
            nodes_created: files.iter().map(|f| f.nodes.len()).sum(),
            edges_created: files.iter().map(|f| f.edges.len()).sum::<usize>()
                + deferred_cross_file_edges.len(),
            duration_ms: started_at.elapsed().as_millis(),
        };

        Ok(RepoIndexPayload {
            repo: repo.to_owned(),
            files,
            deferred_cross_file_edges,
            unresolved_inputs: resolution.unresolved,
            payload_records,
            file_states,
            indexed_file_paths,
            indexed_path_ids,
            is_cold_index: false, // determined at commit time
            stats,
            synthetic_file_count,
        })
    }

    #[expect(clippy::too_many_lines)]
    fn index_repo_files(
        &self,
        repo: &str,
        repo_root: &Path,
        files_to_index: &[SourceFileEntry],
        traversal_stats: &FxHashMap<Vec<u8>, FileStat>,
        include_manifest_batch: bool,
        force_rewrite: bool,
        detected_frameworks: &[Framework],
        cancel: Option<&CancellationToken>,
        progress: Option<&dyn Fn(IndexProgress)>,
    ) -> Result<IndexingStats, RepoIndexerError> {
        let started_at = Instant::now();
        let indexed_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| i64::try_from(duration.as_secs()).unwrap_or(i64::MAX))
            .unwrap_or_default();
        let total_files = files_to_index.len();

        if let Some(progress) = progress {
            progress(IndexProgress {
                phase: "traverse",
                processed: total_files,
                total: total_files,
            });
        }

        let path_aliases = Arc::new(build_path_aliases(repo_root));
        let repo_path_str = repo_root.to_string_lossy();
        let local_config = LocalConfig::load(repo_root);
        let cancel = cancel.cloned();

        let (parse_sender, parse_receiver) = bounded::<Result<ParsedFile, ParseError>>(64);
        let repo_name = Arc::new(repo.to_owned());
        let root = Arc::new(repo_root.to_path_buf());
        let batch_size = self.options.batch_size.max(1);

        // ── Pass 1 (parse) bookkeeping ──────────────────────────────────────
        // symbol_nodes and resolution_inputs accumulate for the whole repo so
        // the resolver can build its SymbolIndex across all files.
        // FileBatch bodies (nodes/edges — the large allocations) are forwarded
        // to the writer thread immediately so they are freed from the
        // receiver's stack before the next file parses, capping peak RSS.
        let mut files_parsed = 0_usize;
        let mut symbol_nodes: Vec<gather_step_core::NodeData> = Vec::new();
        let mut resolution_inputs: Vec<ResolutionInput> = Vec::new();
        let mut payload_records = Vec::<PayloadContractStoreRecord>::new();
        // Slim per-file metadata retained for reconcile and the metadata store.
        let mut file_states: Vec<FileIndexState> = Vec::new();
        let mut indexed_file_paths: Vec<String> = Vec::new();
        let mut indexed_path_ids: Vec<Vec<u8>> = Vec::new();
        let mut synthetic_file_count = 0_usize;
        let mut parse_ms_total = 0_u64;
        let mut call_sites_total = 0_usize;
        let mut import_bindings_total = 0_usize;
        let mut payload_contracts_total = 0_usize;
        let mut max_file_parse_ms = 0_i64;
        let mut max_file_parse_path = String::new();

        // Snapshot the pre-write node count before bulk mode is enabled and
        // before any files are written to storage.
        let pre_write_node_count = self.storage.graph().count_nodes_by_repo(repo).unwrap_or(0);
        let is_cold_index_pre = pre_write_node_count == 0;

        // Clear stale semantic metadata for all previously-indexed files in
        // this repo.  This runs before the write thread opens to avoid a write
        // transaction conflict.
        self.storage
            .metadata()
            .clear_semantic_metadata_for_files(repo, &[])?;

        let write_start = Instant::now();
        // BulkModeGuard enables Durability::None for the entire parse+write
        // scope (Pass 1 + Pass 2) and restores normal durability on drop.
        let bulk_guard = BulkModeGuard::new(self.storage.graph());

        // ── Combined parse + write scope ────────────────────────────────────
        // Returns (writer_stats, unresolved_inputs) from a single thread::scope
        // so both are available after the scope exits.
        let (write_stats, unresolved_inputs) = thread::scope(
            |scope| -> Result<(IndexingStats, Vec<ResolutionInput>), RepoIndexerError> {
                let (write_sender, write_receiver) = bounded::<WriteMessage>(8);
                let storage = &self.storage;

                // Writer thread: drains write_receiver and commits to the graph.
                let writer = scope.spawn(move || -> Result<IndexingStats, RepoIndexerError> {
                    let mut stats = IndexingStats::default();
                    while let Ok(message) = write_receiver.recv() {
                        match message {
                            WriteMessage::Batch(batch) => {
                                let result = storage.index_repo_batch_without_file_states_cold(
                                    &batch,
                                    is_cold_index_pre,
                                )?;
                                stats.files_parsed += result.files_indexed;
                                stats.nodes_created += result.nodes_written;
                                stats.edges_created += result.edges_written;
                            }
                            WriteMessage::CrossFileEdgeBatch(edges) => {
                                let edge_count = edges.len();
                                storage.graph().with_write_txn(|write_txn| {
                                    GraphStoreDb::bulk_insert_edges_in_txn(write_txn, &edges)
                                })?;
                                stats.edges_created += edge_count;
                            }
                        }
                    }
                    Ok(stats)
                });

                // ── Parse sub-scope ────────────────────────────────────────
                // Produces parsed files into parse_receiver; the main thread
                // (below) consumes them and immediately forwards each FileBatch
                // to the writer via write_sender.
                thread::scope(|parse_scope| -> Result<(), RepoIndexerError> {
                    let producer = parse_scope.spawn(|| {
                        if let Some(packs) = local_config
                            .as_ref()
                            .and_then(|cfg| cfg.packs_for_repo(repo_path_str.as_ref()))
                        {
                            let packs = Arc::<[_]>::from(packs);
                            let _ = files_to_index.par_iter().try_for_each_with(
                                parse_sender.clone(),
                                |sender, file| {
                                    if cancel.as_ref().is_some_and(CancellationToken::is_cancelled)
                                    {
                                        return Err(());
                                    }
                                    let parsed = parse_file_with_packs(
                                        repo_name.as_str(),
                                        root.as_path(),
                                        file,
                                        packs.as_ref(),
                                        path_aliases.as_ref(),
                                    );
                                    sender.send(parsed).map_err(|_| ())?;
                                    Ok(())
                                },
                            );
                        } else {
                            let frameworks = Arc::<[Framework]>::from(detected_frameworks.to_vec());
                            let _ = files_to_index.par_iter().try_for_each_with(
                                parse_sender.clone(),
                                |sender, file| {
                                    if cancel.as_ref().is_some_and(CancellationToken::is_cancelled)
                                    {
                                        return Err(());
                                    }
                                    let parsed = parse_file_with_context(
                                        repo_name.as_str(),
                                        root.as_path(),
                                        file,
                                        frameworks.as_ref(),
                                        path_aliases.as_ref(),
                                    );
                                    sender.send(parsed).map_err(|_| ())?;
                                    Ok(())
                                },
                            );
                        }
                        drop(parse_sender);
                    });

                    // Accumulator for the current write batch.
                    let mut pending: Vec<FileBatch> = Vec::with_capacity(batch_size);

                    for message in &parse_receiver {
                        let parsed = message?;
                        files_parsed += 1;
                        let inferred_payloads = infer_payload_contracts(&parsed);

                        let ParsedFile {
                            file,
                            file_node,
                            nodes,
                            edges,
                            symbols,
                            call_sites,
                            import_bindings,
                            parse_ms,
                            ..
                        } = parsed;

                        // Display string for FileBatch field and logging.
                        // Lossy UTF-8 acceptable here — used for rendering
                        // and the reconcile graph-store path only.
                        let file_path_str =
                            normalize_path_separators(&file.path.to_string_lossy()).into_owned();
                        parse_ms_total =
                            parse_ms_total.saturating_add(nonnegative_i64_to_u64(parse_ms));
                        if parse_ms > max_file_parse_ms {
                            max_file_parse_ms = parse_ms;
                            max_file_parse_path.clone_from(&file_path_str);
                        }
                        // Lossless identity bytes for SQLite BLOB column.
                        let path_id_bytes = gather_step_core::PathId::from_path(&file.path)
                            .as_bytes()
                            .to_vec();
                        let source_path = file.path.clone();
                        call_sites_total += call_sites.len();
                        import_bindings_total += import_bindings.len();
                        payload_contracts_total += inferred_payloads.len();
                        let call_sites_normalized = call_sites
                            .into_iter()
                            .map(|call| CallSite {
                                owner_id: call.owner_id,
                                owner_file: call.owner_file,
                                source_path: source_path.clone(),
                                callee_name: call.callee_name,
                                callee_qualified_hint: call.callee_qualified_hint,
                                span: call.span,
                            })
                            .collect::<Vec<_>>();
                        let payload_nodes = inferred_payloads
                            .iter()
                            .map(|contract| contract.node.clone())
                            .collect::<Vec<_>>();
                        let payload_edges = inferred_payloads
                            .iter()
                            .map(|contract| contract.edge.clone())
                            .collect::<Vec<_>>();
                        payload_records.extend(inferred_payloads.into_iter().map(|contract| {
                            PayloadContractStoreRecord {
                                record: contract.record,
                            }
                        }));

                        symbol_nodes.extend(symbols.into_iter().map(|symbol| symbol.node));
                        resolution_inputs.push(ResolutionInput {
                            file_node: file_node.id,
                            file_path: file.path.clone(),
                            import_bindings,
                            call_sites: call_sites_normalized,
                        });

                        let file_stat = traversal_stats
                            .get(path_id_bytes.as_slice())
                            .copied()
                            .unwrap_or_else(|| {
                                file_metadata_stamp(root.join(&file.path)).unwrap_or_default()
                            });
                        let all_nodes: Vec<gather_step_core::NodeData> =
                            nodes.into_iter().chain(payload_nodes).collect();
                        let all_edges: Vec<gather_step_core::EdgeData> =
                            edges.into_iter().chain(payload_edges).collect();

                        // Retain slim metadata for the FileIndexState.
                        file_states.push(FileIndexState {
                            repo: repo.to_owned(),
                            file_path: file_path_str.clone(),
                            path_id_bytes: path_id_bytes.clone(),
                            content_hash: file.content_hash.to_vec(),
                            size_bytes: file_stat.size_bytes,
                            mtime_ns: file_stat.mtime_ns,
                            node_count: i64::try_from(all_nodes.len()).unwrap_or(i64::MAX),
                            edge_count: i64::try_from(all_edges.len()).unwrap_or(i64::MAX),
                            indexed_at,
                            parse_ms: Some(parse_ms),
                        });
                        indexed_file_paths.push(file_path_str.clone());
                        indexed_path_ids.push(path_id_bytes.clone());

                        // Forward the FileBatch to the writer immediately (Pass 1
                        // streaming write) so node/edge allocations are freed
                        // before the next file arrives.
                        pending.push(FileBatch {
                            repo: repo.to_owned(),
                            file_path: file_path_str,
                            path_id_bytes,
                            nodes: all_nodes,
                            edges: all_edges,
                            content_hash: file.content_hash.to_vec(),
                            size_bytes: file_stat.size_bytes,
                            mtime_ns: file_stat.mtime_ns,
                            indexed_at,
                            parse_ms: Some(parse_ms),
                            force: force_rewrite,
                        });

                        if pending.len() >= batch_size {
                            let batch = RepoBatch {
                                repo: repo.to_owned(),
                                files: std::mem::take(&mut pending),
                                test_hooks: RepoBatchHooks::default(),
                            };
                            if write_sender.send(WriteMessage::Batch(batch)).is_err() {
                                break; // writer closed; join will surface error
                            }
                        }
                    }

                    // Flush any remaining files.
                    if !pending.is_empty() {
                        let batch = RepoBatch {
                            repo: repo.to_owned(),
                            files: pending,
                            test_hooks: RepoBatchHooks::default(),
                        };
                        let _ = write_sender.send(WriteMessage::Batch(batch));
                    }

                    producer
                        .join()
                        .map_err(|_| RepoIndexerError::ChannelClosed)?;
                    Ok(())
                })?;

                // manifest batch: written as part of Pass 1 after source files.
                if include_manifest_batch
                    && let Some(mut manifest_batch) =
                        build_manifest_batch(repo, repo_root, indexed_at)
                {
                    manifest_batch.force = force_rewrite;
                    file_states.push(FileIndexState {
                        repo: repo.to_owned(),
                        file_path: manifest_batch.file_path.clone(),
                        path_id_bytes: manifest_batch.path_id_bytes.clone(),
                        content_hash: manifest_batch.content_hash.clone(),
                        size_bytes: manifest_batch.size_bytes,
                        mtime_ns: manifest_batch.mtime_ns,
                        node_count: i64::try_from(manifest_batch.nodes.len()).unwrap_or(i64::MAX),
                        edge_count: i64::try_from(manifest_batch.edges.len()).unwrap_or(i64::MAX),
                        indexed_at,
                        parse_ms: manifest_batch.parse_ms,
                    });
                    indexed_file_paths.push(manifest_batch.file_path.clone());
                    indexed_path_ids.push(manifest_batch.path_id_bytes.clone());
                    synthetic_file_count += 1;
                    let _ = write_sender.send(WriteMessage::Batch(RepoBatch {
                        repo: repo.to_owned(),
                        files: vec![manifest_batch],
                        test_hooks: RepoBatchHooks::default(),
                    }));
                }

                // ── Pass 2: resolve cross-file calls, write resolved edges ──
                // All source file nodes are now in the graph store (written by
                // the writer thread above).  Build the resolver index and emit
                // all resolved call edges — both intra-file and cross-file —
                // as a single CrossFileEdgeBatch.
                //
                // Intra-file resolved edges are written here rather than being
                // appended to their FileBatch because those batches have already
                // been consumed.  The graph store accepts edges whose source and
                // target nodes were committed in a prior write transaction.
                Self::check_cancel(cancel.as_ref())?;

                if let Some(progress) = progress {
                    progress(IndexProgress {
                        phase: "parse",
                        processed: files_parsed,
                        total: total_files,
                    });
                }

                let parse_elapsed = started_at.elapsed();
                info!(
                    repo,
                    files = files_parsed,
                    parse_ms = millis_u64(parse_elapsed),
                    parse_ms_sum = parse_ms_total,
                    call_sites = call_sites_total,
                    import_bindings = import_bindings_total,
                    symbol_nodes = symbol_nodes.len(),
                    resolution_inputs = resolution_inputs.len(),
                    payload_contracts = payload_contracts_total,
                    max_file_parse_ms = nonnegative_i64_to_u64(max_file_parse_ms),
                    max_file_parse_path = %max_file_parse_path,
                    "stage timing: parse + write (pass 1) complete",
                );

                let resolve_start = Instant::now();
                let resolution =
                    resolve_calls_with_unresolved(repo_root, &symbol_nodes, &resolution_inputs);
                let resolve_elapsed = resolve_start.elapsed();
                let unresolved_files = resolution.unresolved.len();
                let in_file_resolved_edges = resolution
                    .resolved
                    .iter()
                    .filter(|c| !c.edge.is_cross_file)
                    .count();
                let cross_file_resolved_edges = resolution
                    .resolved
                    .iter()
                    .filter(|c| c.edge.is_cross_file)
                    .count();
                info!(
                    repo,
                    resolve_ms = millis_u64(resolve_elapsed),
                    in_file_resolved_edges,
                    cross_file_resolved_edges,
                    unresolved_files,
                    "stage timing: call resolution (pass 2) complete",
                );

                // Collect all resolved edges into one batch; cross-file and
                // intra-file are both written via the CrossFileEdgeBatch path
                // because file batches for Pass 1 have already been sent.
                let all_resolved_edges: Vec<gather_step_core::EdgeData> =
                    resolution.resolved.into_iter().map(|c| c.edge).collect();
                if !all_resolved_edges.is_empty() {
                    let _ = write_sender.send(WriteMessage::CrossFileEdgeBatch(all_resolved_edges));
                }
                drop(write_sender); // signal writer to drain and exit

                // Join the writer and retrieve its stats.
                let writer_stats = writer
                    .join()
                    .map_err(|_| RepoIndexerError::WriterPanicked)??;
                Ok((writer_stats, resolution.unresolved))
            },
        )?;

        drop(bulk_guard); // Restore normal durability before reconcile.
        let write_elapsed = write_start.elapsed();
        info!(
            repo,
            write_ms = millis_u64(write_elapsed),
            nodes = write_stats.nodes_created,
            edges = write_stats.edges_created,
            "stage timing: graph write complete",
        );

        let reconcile_start = Instant::now();
        let changed_files = indexed_file_paths
            .iter()
            .cloned()
            .zip(indexed_path_ids.iter().cloned())
            .map(|(path, path_id_bytes)| TrackedPath {
                path,
                path_id_bytes,
            })
            .collect::<Vec<_>>();
        // Detect cold full-index: the repo had zero nodes before this run.
        // On a cold build there is no prior state to reconcile against, so
        // we skip dangling-edge cleanup, unresolved-call reconciliation,
        // semantic bridge traversal, and pack invalidation.
        let is_cold_index = pre_write_node_count == 0 && write_stats.nodes_created > 0;
        let reconcile_stats =
            reconcile_changed_files_with_mode(&self.storage, repo, &changed_files, is_cold_index)
                .map_err(|error| {
                warn!(repo, error = %error, "reconciliation failed after indexing");
                error
            })?;
        let reconcile_elapsed = reconcile_start.elapsed();
        info!(
            repo,
            reconcile_ms = millis_u64(reconcile_elapsed),
            files = changed_files.len(),
            reconcile_files = reconcile_stats.files_processed,
            dependency_rows = reconcile_stats.dependency_rows_written,
            semantic_peer_files = reconcile_stats.semantic_peer_files_affected,
            dangling_edges_removed = reconcile_stats.dangling_edges_removed,
            unresolved_inputs_scanned = reconcile_stats.unresolved_inputs_scanned,
            unresolved_calls_resolved = reconcile_stats.unresolved_calls_resolved,
            unresolved_calls_remaining = reconcile_stats.unresolved_calls_remaining,
            "stage timing: reconcile complete",
        );

        // Now that we know indexed_path_ids, clear stale metadata for this
        // specific set of files and replace with the fresh data.
        self.storage
            .metadata()
            .clear_semantic_metadata_for_files(repo, &indexed_path_ids)?;
        let metadata_start = Instant::now();
        self.storage.metadata().replace_index_metadata_for_files(
            repo,
            &indexed_path_ids,
            &unresolved_inputs,
            &payload_records,
            &file_states,
        )?;
        let metadata_elapsed = metadata_start.elapsed();
        info!(
            repo,
            metadata_ms = millis_u64(metadata_elapsed),
            "stage timing: metadata replacement complete",
        );

        info!(
            repo,
            graph_file_bytes = self.storage.graph().file_size_bytes(),
            "redb file size after indexing",
        );

        let mut stats = write_stats;
        stats.files_parsed = stats.files_parsed.saturating_sub(synthetic_file_count);
        stats.duration_ms = started_at.elapsed().as_millis();
        if let Some(progress) = progress {
            progress(IndexProgress {
                phase: "write",
                processed: stats.files_parsed,
                total: stats.files_parsed,
            });
        }

        Ok(stats)
    }

    fn check_cancel(cancel: Option<&CancellationToken>) -> Result<(), RepoIndexerError> {
        if cancel.is_some_and(CancellationToken::is_cancelled) {
            return Err(RepoIndexerError::Cancelled);
        }
        Ok(())
    }
}

fn build_manifest_batch(repo: &str, repo_root: &Path, indexed_at: i64) -> Option<FileBatch> {
    let raw = read_indexable_manifest(repo_root)?;
    let manifest_meta = file_metadata_stamp(repo_root.join("package.json")).ok();

    let file_path = "package.json".to_owned();
    let file_node = NodeData {
        id: node_id(repo, &file_path, NodeKind::File, &file_path),
        kind: NodeKind::File,
        repo: repo.to_owned(),
        file_path: file_path.clone(),
        name: file_path.clone(),
        qualified_name: Some(format!("{repo}::{file_path}")),
        external_id: None,
        signature: None,
        visibility: None,
        span: None,
        is_virtual: false,
    };
    let repo_node = NodeData {
        id: node_id(repo, "__repo__", NodeKind::Repo, repo),
        kind: NodeKind::Repo,
        repo: repo.to_owned(),
        file_path: "__repo__".to_owned(),
        name: repo.to_owned(),
        qualified_name: Some(format!("{repo}::__repo__")),
        external_id: None,
        signature: None,
        visibility: None,
        span: None,
        is_virtual: false,
    };

    let extraction =
        match extract_package_manifest(repo, &file_path, file_node.id, repo_node.id, &raw) {
            Ok(extraction) => extraction,
            Err(error) => {
                warn!(repo, error = %error, "skipping malformed package manifest during indexing");
                return None;
            }
        };

    let mut nodes = vec![file_node.clone(), repo_node.clone()];
    nodes.extend(extraction.nodes);

    let mut edges = vec![EdgeData {
        source: file_node.id,
        target: repo_node.id,
        kind: EdgeKind::Defines,
        metadata: EdgeMetadata::default(),
        owner_file: file_node.id,
        is_cross_file: false,
    }];
    edges.extend(extraction.edges);

    Some(FileBatch {
        repo: repo.to_owned(),
        file_path,
        path_id_bytes: vec![], // package.json is always ASCII — fallback is correct
        nodes,
        edges,
        content_hash: blake3::hash(raw.as_bytes()).as_bytes().to_vec(),
        size_bytes: manifest_meta
            .as_ref()
            .map_or(i64::try_from(raw.len()).unwrap_or(i64::MAX), |meta| {
                meta.size_bytes
            }),
        mtime_ns: manifest_meta.as_ref().map_or(0, |meta| meta.mtime_ns),
        indexed_at,
        parse_ms: None,
        force: false,
    })
}

fn file_metadata_stamp(path: impl AsRef<Path>) -> Result<FileStat, std::io::Error> {
    let metadata = fs::metadata(path)?;
    let mtime_ns = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or_default();
    Ok(FileStat {
        size_bytes: i64::try_from(metadata.len()).unwrap_or(i64::MAX),
        mtime_ns,
    })
}

fn collect_deployment_artifact_paths(
    repo_root: &Path,
    deployment: &DeploymentIndexingOptions,
) -> Result<Vec<std::path::PathBuf>, RepoIndexerError> {
    let include_globs = deployment.include_globs()?;
    let walker = ignore::WalkBuilder::new(repo_root)
        .hidden(false)
        .parents(false)
        .filter_entry(|entry| {
            if entry.depth() == 0 {
                return true;
            }
            let Some(name) = entry.file_name().to_str() else {
                return true;
            };
            !matches!(name, ".git" | "node_modules" | "target" | "dist")
        })
        .build();
    let mut paths = Vec::new();
    for entry in walker {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                warn!(error = %error, "skipping deployment artifact walk entry");
                continue;
            }
        };
        if !entry.file_type().is_some_and(|kind| kind.is_file()) {
            continue;
        }
        let relative_path = match entry.path().strip_prefix(repo_root) {
            Ok(path) => path,
            Err(_) => continue,
        };
        let file_path = normalize_path_separators(&relative_path.to_string_lossy()).into_owned();
        let path_kind = detect_artifact_kind(&file_path, "");
        let extension = relative_path
            .extension()
            .and_then(std::ffi::OsStr::to_str)
            .unwrap_or_default();
        let could_be_yaml_deploy = matches!(extension, "yaml" | "yml");
        if path_kind != DeploymentArtifactKind::Unknown
            || could_be_yaml_deploy
            || deployment.matches_configured_artifact(&file_path, extension, &include_globs)
        {
            paths.push(relative_path.to_path_buf());
        }
    }
    paths.sort();
    Ok(paths)
}

impl DeploymentIndexingOptions {
    fn include_globs(&self) -> Result<GlobSet, RepoIndexerError> {
        let mut builder = GlobSetBuilder::new();
        for pattern in &self.include {
            builder.add(
                Glob::new(pattern).map_err(|source| RepoIndexerError::DeploymentGlob {
                    pattern: pattern.clone(),
                    source,
                })?,
            );
        }
        builder
            .build()
            .map_err(|source| RepoIndexerError::DeploymentGlob {
                pattern: self.include.join(", "),
                source,
            })
    }

    fn matches_configured_artifact(
        &self,
        file_path: &str,
        extension: &str,
        include_globs: &GlobSet,
    ) -> bool {
        include_globs.is_match(file_path)
            || self
                .env_files
                .iter()
                .any(|path| normalized_config_path(path) == file_path)
            || self.gitops_roots.iter().any(|root| {
                path_is_under_root(file_path, &normalized_config_path(root))
                    && matches!(extension, "yaml" | "yml" | "json" | "tpl")
            })
    }

    fn forced_artifact_kind_for_path(&self, file_path: &str) -> Option<DeploymentArtifactKind> {
        self.env_files
            .iter()
            .any(|path| normalized_config_path(path) == file_path)
            .then_some(DeploymentArtifactKind::EnvFile)
    }
}

fn normalized_config_path(path: &str) -> String {
    normalize_path_separators(path).trim_matches('/').to_owned()
}

fn path_is_under_root(path: &str, root: &str) -> bool {
    !root.is_empty() && (path == root || path.starts_with(&format!("{root}/")))
}

fn deployment_output_to_batch(
    repo: &str,
    file_path: &str,
    raw: &[u8],
    file_stat: FileStat,
    indexed_at: i64,
    output: DeploymentParseOutput,
    force: bool,
) -> FileBatch {
    let file_node = NodeData {
        id: node_id(repo, file_path, NodeKind::File, file_path),
        kind: NodeKind::File,
        repo: repo.to_owned(),
        file_path: file_path.to_owned(),
        name: file_path.to_owned(),
        qualified_name: Some(format!("{repo}::{file_path}")),
        external_id: None,
        signature: None,
        visibility: None,
        span: None,
        is_virtual: false,
    };
    let repo_node = NodeData {
        id: node_id(repo, "__repo__", NodeKind::Repo, repo),
        kind: NodeKind::Repo,
        repo: repo.to_owned(),
        file_path: "__repo__".to_owned(),
        name: repo.to_owned(),
        qualified_name: Some(format!("{repo}::__repo__")),
        external_id: None,
        signature: None,
        visibility: None,
        span: None,
        is_virtual: false,
    };

    let mut nodes = vec![file_node.clone(), repo_node.clone()];
    nodes.extend(output.nodes.into_iter().map(|node| NodeData {
        id: ref_node_id(node.kind, &node.qualified_name),
        kind: node.kind,
        repo: repo.to_owned(),
        file_path: file_path.to_owned(),
        name: node.name,
        qualified_name: Some(node.qualified_name.clone()),
        external_id: Some(node.qualified_name),
        signature: None,
        visibility: None,
        span: None,
        is_virtual: true,
    }));

    let mut edges = vec![EdgeData {
        source: file_node.id,
        target: repo_node.id,
        kind: EdgeKind::Defines,
        metadata: EdgeMetadata::default(),
        owner_file: file_node.id,
        is_cross_file: false,
    }];
    edges.extend(output.edges.into_iter().map(|edge| EdgeData {
        source: ref_node_id(edge.source_kind, &edge.source_qualified_name),
        target: ref_node_id(edge.target_kind, &edge.target_qualified_name),
        kind: edge.kind,
        metadata: EdgeMetadata {
            confidence: Some(edge.confidence),
            ..EdgeMetadata::default()
        },
        owner_file: file_node.id,
        is_cross_file: false,
    }));

    FileBatch {
        repo: repo.to_owned(),
        file_path: file_path.to_owned(),
        path_id_bytes: gather_step_core::PathId::from_path(Path::new(file_path))
            .as_bytes()
            .to_vec(),
        nodes,
        edges,
        content_hash: blake3::hash(raw).as_bytes().to_vec(),
        size_bytes: file_stat.size_bytes,
        mtime_ns: file_stat.mtime_ns,
        indexed_at,
        parse_ms: None,
        force,
    }
}

/// Build `PathAliases` for a repo, injecting any workspace-local package
/// aliases discovered by walking up from `repo_root` to find a monorepo root.
///
/// This is the canonical construction point used by both `prepare_repo_files`
/// and `index_repo_files`. The workspace root walk is capped at 3 parent
/// levels; failures at any stage are silent (the function returns plain
/// tsconfig aliases).
fn build_path_aliases(repo_root: &Path) -> PathAliases {
    let mut aliases = PathAliases::from_repo_root(repo_root);
    if let Some(workspace_root) = find_workspace_root(repo_root, 3) {
        let packages = discover_workspace_packages(&workspace_root);
        if !packages.is_empty() {
            aliases.add_workspace_packages(&packages);
        }
    }
    aliases
}

fn has_indexable_manifest(repo_root: &Path) -> bool {
    let manifest_path = repo_root.join("package.json");
    let Ok(metadata) = fs::symlink_metadata(&manifest_path) else {
        return false;
    };
    !metadata.file_type().is_symlink() && metadata.is_file()
}

fn read_indexable_manifest(repo_root: &Path) -> Option<String> {
    if !has_indexable_manifest(repo_root) {
        return None;
    }
    fs::read_to_string(repo_root.join("package.json")).ok()
}

#[cfg(test)]
mod tests {
    #![expect(clippy::needless_raw_string_hashes)]

    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
        sync::{Arc, Mutex},
        thread,
    };

    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    use gather_step_core::{EdgeKind, NodeKind};
    use gather_step_parser::{
        FileEntry, Language, collect_repo_files,
        frameworks::{Framework, detect_frameworks},
        parse_file_with_context,
        tsconfig::PathAliases,
    };
    use pretty_assertions::assert_eq;

    use crate::{GraphStore, MetadataStore, SearchStore};

    use super::{DeploymentIndexingOptions, IndexingOptions, RepoIndexer};

    static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> Self {
            let counter = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-orchestrator-{name}-{}-{counter}",
                process::id()
            ));
            fs::create_dir_all(&path).expect("test directory should be created");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn indexes_repo_end_to_end_with_calls_routes_topics_search_and_metadata() {
        let repo_root = TestDir::new("repo");
        let storage_root = TestDir::new("storage");
        fs::create_dir_all(repo_root.path().join("src")).expect("src dir should exist");
        fs::write(
            repo_root.path().join("package.json"),
            r#"{ "name": "sample-service", "dependencies": { "@nestjs/core": "^11.0.0", "@workspace/shared-contracts": "2.3.1" } }"#,
        )
        .expect("package.json fixture should write");
        fs::write(
            repo_root.path().join("src/helper.ts"),
            "export function helper() { return 1; }\n",
        )
        .expect("helper fixture should write");
        fs::write(
            repo_root.path().join("src/caller.ts"),
            "import { helper } from './helper';\nexport function caller() { return helper(); }\n",
        )
        .expect("caller fixture should write");
        fs::write(
            repo_root.path().join("src/controller.ts"),
            r#"
import { Controller, Get } from '@nestjs/common';

@Controller('items')
export class ItemController {
  @Get('list')
  list() {
    return [];
  }
}
"#,
        )
        .expect("controller fixture should write");
        fs::write(
            repo_root.path().join("src/events.ts"),
            r#"
import { Controller } from '@nestjs/common';
import { MessagePattern } from '@nestjs/microservices';

@Controller()
export class EventController {
  @MessagePattern(['item.created'])
  handleCreated() {
    return {};
  }
}
"#,
        )
        .expect("event fixture should write");

        let indexer =
            RepoIndexer::open(storage_root.path(), IndexingOptions::default()).expect("indexer");
        let stats = indexer
            .index_repo("sample-service", repo_root.path(), None)
            .expect("indexing should succeed");

        assert_eq!(stats.files_parsed, 4);
        assert!(stats.nodes_created >= 8);
        assert!(stats.edges_created >= 8);

        let graph = indexer.storage().graph();
        let caller_nodes = graph
            .nodes_by_file("sample-service", "src/caller.ts")
            .expect("caller nodes should load");
        let helper_nodes = graph
            .nodes_by_file("sample-service", "src/helper.ts")
            .expect("helper nodes should load");
        let caller_function = caller_nodes
            .iter()
            .find(|node| node.kind == NodeKind::Function && node.name == "caller")
            .expect("caller function should exist");
        let helper_function = helper_nodes
            .iter()
            .find(|node| node.kind == NodeKind::Function && node.name == "helper")
            .expect("helper function should exist");
        let route_nodes = graph
            .nodes_by_type(NodeKind::Route)
            .expect("route nodes should load");
        // NestJS messaging nodes converge on `NodeKind::Event` (canonical
        // messaging identity), so the assertion targets Event kind rather
        // than the old `__topic__kafka__…` form.
        let event_nodes = graph
            .nodes_by_type(NodeKind::Event)
            .expect("event nodes should load");
        let shared_nodes = graph
            .nodes_by_type(NodeKind::SharedSymbol)
            .expect("shared symbol nodes should load");
        let repo_nodes = graph
            .nodes_by_type(NodeKind::Repo)
            .expect("repo nodes should load");
        let outgoing = graph
            .get_outgoing(caller_function.id)
            .expect("outgoing edges should load");

        assert!(
            outgoing
                .iter()
                .any(|edge| edge.kind == EdgeKind::Calls && edge.target == helper_function.id)
        );
        assert!(
            route_nodes
                .iter()
                .any(|node| node.external_id.as_deref() == Some("__route__GET__/items/list"))
        );
        assert!(
            event_nodes
                .iter()
                .any(|node| node.external_id.as_deref() == Some("__event__kafka__item.created"))
        );
        assert!(shared_nodes.iter().any(|node| {
            node.external_id.as_deref()
                == Some("__shared__@workspace/shared-contracts@2.3.1__package")
        }));
        assert!(
            repo_nodes
                .iter()
                .any(|node| !node.is_virtual && node.name == "sample-service")
        );

        let search_hits = indexer
            .storage()
            .search()
            .search("helper", 10)
            .expect("search should succeed");
        assert!(search_hits.iter().any(|hit| hit.symbol_name == "helper"));
        let traversal = collect_repo_files(repo_root.path(), &IndexingOptions::default().traverse)
            .expect("traversal should still work");
        let caller_hash = traversal
            .files
            .iter()
            .find(|file| file.path == std::path::Path::new("src/caller.ts"))
            .expect("caller file should exist")
            .content_hash;
        let should_reindex = indexer
            .storage()
            .metadata()
            .should_reindex("sample-service", "src/caller.ts", &caller_hash)
            .expect("metadata query should succeed");
        assert!(!should_reindex);
    }

    #[test]
    fn indexes_deployment_topology_artifacts() {
        let repo_root = TestDir::new("deployment-topology-repo");
        let storage_root = TestDir::new("deployment-topology-storage");
        fs::create_dir_all(repo_root.path().join("src")).expect("src dir should exist");
        fs::create_dir_all(repo_root.path().join(".github/workflows"))
            .expect("workflow dir should exist");
        fs::write(
            repo_root.path().join("src/app.ts"),
            "export function run() { return true; }\n",
        )
        .expect("source fixture should write");
        fs::write(
            repo_root.path().join("compose.yaml"),
            r#"
services:
  api:
    image: sample-api
    environment:
      DATABASE_URL: postgres://redacted
    depends_on:
      - postgres
  postgres:
    image: postgres:16
"#,
        )
        .expect("compose fixture should write");
        fs::write(
            repo_root.path().join(".github/workflows/deploy.yml"),
            r#"
name: Deploy
on:
  push:
jobs:
  deploy:
    steps:
      - run: helm upgrade api ./charts/api
"#,
        )
        .expect("workflow fixture should write");

        let indexer =
            RepoIndexer::open(storage_root.path(), IndexingOptions::default()).expect("indexer");
        let stats = indexer
            .index_repo("sample-service", repo_root.path(), None)
            .expect("indexing should succeed");

        assert!(stats.files_parsed >= 3);

        let graph = indexer.storage().graph();
        let deployments = graph
            .nodes_by_type(NodeKind::Deployment)
            .expect("deployment nodes should load");
        let env_vars = graph
            .nodes_by_type(NodeKind::EnvVar)
            .expect("env var nodes should load");
        let workflow_jobs = graph
            .nodes_by_type(NodeKind::WorkflowJob)
            .expect("workflow job nodes should load");
        let databases = graph
            .nodes_by_type(NodeKind::Database)
            .expect("database nodes should load");

        assert!(deployments.iter().any(|node| {
            node.qualified_name.as_deref() == Some("__deployment__sample-service__api")
        }));
        assert!(
            env_vars
                .iter()
                .any(|node| node.qualified_name.as_deref() == Some("__env_var__database_url"))
        );
        assert!(workflow_jobs.iter().any(|node| node.name == "deploy"));
        assert!(databases.iter().any(|node| node.kind == NodeKind::Database));

        let compose_file = graph
            .nodes_by_file("sample-service", "compose.yaml")
            .expect("compose file nodes should load")
            .into_iter()
            .find(|node| node.kind == NodeKind::File)
            .expect("compose file node should exist");
        let compose_edges = graph
            .edges_by_owner(compose_file.id)
            .expect("compose edges should load");
        assert!(
            compose_edges
                .iter()
                .any(|edge| edge.kind == EdgeKind::ReadsEnv)
        );
        assert!(
            compose_edges
                .iter()
                .any(|edge| edge.kind == EdgeKind::UsesDatabase)
        );
    }

    #[test]
    fn configured_env_files_are_indexed_as_deployment_artifacts() {
        let repo_root = TestDir::new("deployment-configured-env-repo");
        let storage_root = TestDir::new("deployment-configured-env-storage");
        fs::create_dir_all(repo_root.path().join("config")).expect("config dir should exist");
        fs::write(
            repo_root.path().join("config/runtime.vars"),
            "DATABASE_URL=redacted\nAPI_TOKEN=redacted\n",
        )
        .expect("env fixture should write");

        let options = IndexingOptions {
            deployment: DeploymentIndexingOptions {
                env_files: vec!["config/runtime.vars".to_owned()],
                ..DeploymentIndexingOptions::default()
            },
            ..IndexingOptions::default()
        };
        let indexer = RepoIndexer::open(storage_root.path(), options).expect("indexer");
        indexer
            .index_repo("sample-service", repo_root.path(), None)
            .expect("indexing should succeed");

        let env_vars = indexer
            .storage()
            .graph()
            .nodes_by_type(NodeKind::EnvVar)
            .expect("env var nodes should load");
        assert!(
            env_vars
                .iter()
                .any(|node| node.qualified_name.as_deref() == Some("__env_var__database_url"))
        );
        assert!(
            env_vars
                .iter()
                .any(|node| node.qualified_name.as_deref() == Some("__env_var__api_token"))
        );
    }

    #[test]
    fn indexes_helper_built_event_producer_edges_into_graph() {
        let repo_root = TestDir::new("event-producer-repo");
        let storage_root = TestDir::new("event-producer-storage");
        fs::create_dir_all(repo_root.path().join("src")).expect("src dir should exist");
        fs::write(
            repo_root.path().join("package.json"),
            r#"{ "name": "producer-service", "dependencies": { "@nestjs/core": "^11.0.0" } }"#,
        )
        .expect("package.json fixture should write");
        fs::write(
            repo_root.path().join("src/topics.ts"),
            r#"
export enum EventTopic {
  PlatformEvents = 'platform-events',
}
"#,
        )
        .expect("topics fixture should write");
        fs::write(
            repo_root.path().join("src/event-types.ts"),
            r#"
export enum EventType {
  CsvGenerationQueued = 'csv.generation.queued',
  PdfGenerationQueued = 'pdf.generation.queued',
}
"#,
        )
        .expect("event types fixture should write");
        fs::write(
            repo_root.path().join("src/report-events.concern.ts"),
            r#"
import { EventTopic } from './topics';
import { EventType } from './event-types';

export class ReportEventsConcern {
  emitReportQueued(input: { fileType: 'csv' | 'pdf' }) {
    const eventType =
      input.fileType === 'csv'
        ? EventType.CsvGenerationQueued
        : EventType.PdfGenerationQueued;

    return this.kafkaProducer.sendMessage({
      topic: EventTopic.PlatformEvents,
      message: this.#buildEventPayload(input, eventType),
    });
  }

  #buildEventPayload(input: unknown, eventType: EventType) {
    return {
      payload: input,
      eventType,
    };
  }
}
"#,
        )
        .expect("producer fixture should write");

        let indexer =
            RepoIndexer::open(storage_root.path(), IndexingOptions::default()).expect("indexer");
        let frameworks = detect_frameworks(repo_root.path());
        assert!(
            frameworks.contains(&Framework::NestJs),
            "fixture repo must detect as NestJS"
        );
        let parsed = parse_file_with_context(
            "producer-service",
            repo_root.path(),
            &FileEntry {
                path: "src/report-events.concern.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &frameworks.iter().copied().collect::<Vec<_>>(),
            &PathAliases::from_repo_root(repo_root.path()),
        )
        .expect("producer fixture should parse with detected frameworks");
        assert!(
            parsed
                .nodes
                .iter()
                .any(|node| node.external_id.as_deref()
                    == Some("__event__kafka__csv.generation.queued")),
            "parser must emit csv event node before indexing"
        );
        indexer
            .index_repo("producer-service", repo_root.path(), None)
            .expect("indexing should succeed");

        let graph = indexer.storage().graph();
        let producer_nodes = graph
            .nodes_by_file("producer-service", "src/report-events.concern.ts")
            .expect("producer file nodes should load");
        let producer = producer_nodes
            .iter()
            .find(|node| node.kind == NodeKind::Function && node.name == "emitReportQueued")
            .expect("producer function should exist");

        for event_qn in [
            "__event__kafka__csv.generation.queued",
            "__event__kafka__pdf.generation.queued",
        ] {
            let all_events = graph
                .nodes_by_type(NodeKind::Event)
                .expect("all event nodes should load")
                .into_iter()
                .filter_map(|node| node.external_id)
                .collect::<Vec<_>>();
            let event_nodes = graph
                .nodes_by_external_id(NodeKind::Event, event_qn)
                .expect("event nodes should load");
            assert_eq!(
                event_nodes.len(),
                1,
                "expected one stored node for {event_qn}; available events: {all_events:?}"
            );
            let incoming = graph
                .get_incoming(event_nodes[0].id)
                .expect("incoming edges should load");
            assert!(
                incoming.iter().any(|edge| {
                    edge.kind == EdgeKind::ProducesEventFor && edge.source == producer.id
                }),
                "expected ProducesEventFor edge from emitReportQueued to {event_qn}, got: {incoming:?}"
            );
        }
    }

    #[test]
    fn indexes_imported_helper_built_event_producer_edges_into_graph() {
        let repo_root = TestDir::new("imported-helper-event-producer-repo");
        let storage_root = TestDir::new("imported-helper-event-producer-storage");
        fs::create_dir_all(repo_root.path().join("src")).expect("src dir should exist");
        fs::write(
            repo_root.path().join("package.json"),
            r#"{ "name": "producer-service", "dependencies": { "@nestjs/core": "^11.0.0" } }"#,
        )
        .expect("package.json fixture should write");
        fs::write(
            repo_root.path().join("src/topics.ts"),
            r#"
export enum EventTopic {
  PlatformEvents = 'platform-events',
}
"#,
        )
        .expect("topics fixture should write");
        fs::write(
            repo_root.path().join("src/event-types.ts"),
            r#"
export enum EventType {
  CsvGenerationQueued = 'csv.generation.queued',
  PdfGenerationQueued = 'pdf.generation.queued',
}
"#,
        )
        .expect("event types fixture should write");
        fs::write(
            repo_root.path().join("src/payload.ts"),
            r#"
import { EventType } from './event-types';

export function buildEventPayload(input: unknown, eventType: EventType) {
  return {
    payload: input,
    eventType,
  };
}
"#,
        )
        .expect("helper fixture should write");
        fs::write(
            repo_root.path().join("src/report-events.concern.ts"),
            r#"
import { EventTopic } from './topics';
import { EventType } from './event-types';
import { buildEventPayload } from './payload';

export class ReportEventsConcern {
  emitReportQueued(input: { fileType: 'csv' | 'pdf' }) {
    const eventType =
      input.fileType === 'csv'
        ? EventType.CsvGenerationQueued
        : EventType.PdfGenerationQueued;

    return this.kafkaProducer.sendMessage({
      topic: EventTopic.PlatformEvents,
      message: buildEventPayload(input, eventType),
    });
  }
}
"#,
        )
        .expect("producer fixture should write");

        let indexer =
            RepoIndexer::open(storage_root.path(), IndexingOptions::default()).expect("indexer");
        indexer
            .index_repo("producer-service", repo_root.path(), None)
            .expect("indexing should succeed");

        let graph = indexer.storage().graph();
        let producer_nodes = graph
            .nodes_by_file("producer-service", "src/report-events.concern.ts")
            .expect("producer file nodes should load");
        let producer = producer_nodes
            .iter()
            .find(|node| node.kind == NodeKind::Function && node.name == "emitReportQueued")
            .expect("producer function should exist");

        for event_qn in [
            "__event__kafka__csv.generation.queued",
            "__event__kafka__pdf.generation.queued",
        ] {
            let event_nodes = graph
                .nodes_by_external_id(NodeKind::Event, event_qn)
                .expect("event nodes should load");
            assert_eq!(
                event_nodes.len(),
                1,
                "expected one stored node for {event_qn}"
            );
            let incoming = graph
                .get_incoming(event_nodes[0].id)
                .expect("incoming edges should load");
            assert!(
                incoming.iter().any(|edge| {
                    edge.kind == EdgeKind::ProducesEventFor && edge.source == producer.id
                }),
                "expected ProducesEventFor edge from emitReportQueued to {event_qn}, got: {incoming:?}"
            );
        }
    }

    #[test]
    fn indexes_cross_package_frontend_hook_boundary_edges_into_graph() {
        let repo_root = TestDir::new("frontend-hook-boundary-repo");
        let storage_root = TestDir::new("frontend-hook-boundary-storage");
        fs::create_dir_all(repo_root.path().join("src")).expect("src dir should exist");
        fs::write(
            repo_root.path().join("package.json"),
            r#"{ "name": "frontend-service", "dependencies": { "react": "^19.0.0" } }"#,
        )
        .expect("package.json fixture should write");
        fs::write(
            repo_root.path().join("src/session.ts"),
            r#"
import { useAuthentication } from '@workspace/shared-hooks';

export function SessionPanel() {
  return useAuthentication();
}
"#,
        )
        .expect("consumer fixture should write");

        let indexer =
            RepoIndexer::open(storage_root.path(), IndexingOptions::default()).expect("indexer");
        indexer
            .index_repo("frontend-service", repo_root.path(), None)
            .expect("indexing should succeed");

        let graph = indexer.storage().graph();
        let hook_nodes = graph
            .nodes_by_external_id(
                NodeKind::SharedSymbol,
                "__hook__@workspace/shared-hooks::useAuthentication",
            )
            .expect("hook nodes should load");
        assert_eq!(
            hook_nodes.len(),
            1,
            "expected one virtual hook node for cross-package hook import"
        );
        let incoming = graph
            .get_incoming(hook_nodes[0].id)
            .expect("incoming edges should load");
        assert!(
            incoming.iter().any(|edge| {
                edge.kind == EdgeKind::ConsumesHookFrom
                    && graph
                        .get_node(edge.source)
                        .ok()
                        .flatten()
                        .is_some_and(|node| {
                            node.repo == "frontend-service" && node.file_path == "src/session.ts"
                        })
            }),
            "expected ConsumesHookFrom edge from frontend consumer file to virtual hook node; incoming={incoming:?}"
        );
    }

    #[test]
    fn indexes_dispatcher_event_consumer_edges_into_graph() {
        let repo_root = TestDir::new("event-consumer-repo");
        let storage_root = TestDir::new("event-consumer-storage");
        fs::create_dir_all(repo_root.path().join("src")).expect("src dir should exist");
        fs::write(
            repo_root.path().join("package.json"),
            r#"{ "name": "consumer-service", "dependencies": { "@nestjs/core": "^11.0.0" } }"#,
        )
        .expect("package.json fixture should write");
        fs::write(
            repo_root.path().join("src/event-types.ts"),
            r#"
export enum EventType {
  CsvGenerationQueued = 'csv.generation.queued',
  PdfGenerationQueued = 'pdf.generation.queued',
  DocumentReportQueued = 'document.reg-genius-report-generation.queued',
}
"#,
        )
        .expect("event types fixture should write");
        fs::write(
            repo_root.path().join("src/event-handlers.service.ts"),
            r#"
import { CustomEventPattern } from '@nestjs/microservices';
import { EventType } from './event-types';

export class EventHandlersService {
  @CustomEventPattern('generation-events')
  handleEvent(event: { eventType: EventType }) {
    switch (event.eventType) {
      case EventType.CsvGenerationQueued:
        return 'csv';
      case EventType.PdfGenerationQueued:
        return 'pdf';
      case EventType.DocumentReportQueued:
        return 'report';
      default:
        return 'noop';
    }
  }
}
"#,
        )
        .expect("consumer fixture should write");

        let indexer =
            RepoIndexer::open(storage_root.path(), IndexingOptions::default()).expect("indexer");
        let frameworks = detect_frameworks(repo_root.path());
        assert!(
            frameworks.contains(&Framework::NestJs),
            "fixture repo must detect as NestJS"
        );
        let parsed = parse_file_with_context(
            "consumer-service",
            repo_root.path(),
            &FileEntry {
                path: "src/event-handlers.service.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &frameworks.iter().copied().collect::<Vec<_>>(),
            &PathAliases::from_repo_root(repo_root.path()),
        )
        .expect("consumer fixture should parse with detected frameworks");
        assert!(
            parsed
                .nodes
                .iter()
                .any(|node| node.external_id.as_deref()
                    == Some("__event__kafka__csv.generation.queued")),
            "parser must emit csv event node before indexing"
        );
        indexer
            .index_repo("consumer-service", repo_root.path(), None)
            .expect("indexing should succeed");

        let graph = indexer.storage().graph();
        let consumer_nodes = graph
            .nodes_by_file("consumer-service", "src/event-handlers.service.ts")
            .expect("consumer file nodes should load");
        let consumer = consumer_nodes
            .iter()
            .find(|node| node.kind == NodeKind::Function && node.name == "handleEvent")
            .expect("consumer function should exist");

        for event_qn in [
            "__event__kafka__csv.generation.queued",
            "__event__kafka__pdf.generation.queued",
            "__event__kafka__document.reg-genius-report-generation.queued",
        ] {
            let all_events = graph
                .nodes_by_type(NodeKind::Event)
                .expect("all event nodes should load")
                .into_iter()
                .filter_map(|node| node.external_id)
                .collect::<Vec<_>>();
            let event_nodes = graph
                .nodes_by_external_id(NodeKind::Event, event_qn)
                .expect("event nodes should load");
            assert_eq!(
                event_nodes.len(),
                1,
                "expected one stored node for {event_qn}; available events: {all_events:?}"
            );
            let incoming = graph
                .get_incoming(event_nodes[0].id)
                .expect("incoming edges should load");
            assert!(
                incoming
                    .iter()
                    .any(|edge| edge.kind == EdgeKind::UsesEventFrom && edge.source == consumer.id),
                "expected UsesEventFrom edge from handleEvent to {event_qn}, got: {incoming:?}"
            );
        }
    }

    #[test]
    #[ignore = "requires GATHER_STEP_REAL_WORKSPACE to point at a real multi-repo workspace"]
    fn real_workspace_report_flow_event_edges_survive_indexing() {
        let workspace = std::env::var("GATHER_STEP_REAL_WORKSPACE")
            .expect("GATHER_STEP_REAL_WORKSPACE must be set for the real-workspace storage probe");
        let workspace = PathBuf::from(workspace);
        let storage_root = TestDir::new("real-workspace-storage-probe");
        let indexer =
            RepoIndexer::open(storage_root.path(), IndexingOptions::default()).expect("indexer");

        for (repo, path) in [
            ("shared_contracts", workspace.join("shared_contracts")),
            ("report", workspace.join("report")),
            ("backend_standard", workspace.join("backend_standard")),
        ] {
            indexer
                .index_repo(repo, &path, None)
                .unwrap_or_else(|error| panic!("indexing {repo} should succeed: {error}"));
        }

        let graph = indexer.storage().graph();

        let report_nodes = graph
            .nodes_by_repo("report")
            .expect("report nodes should load");
        let report_producer = report_nodes
            .iter()
            .find(|node| node.kind == NodeKind::Function && node.name == "emitReportQueued")
            .expect("real report repo should include emitReportQueued");
        for event_qn in [
            "__event__kafka__csv.generation.queued",
            "__event__kafka__pdf.generation.queued",
        ] {
            let event_nodes = graph
                .nodes_by_external_id(NodeKind::Event, event_qn)
                .expect("event nodes should load");
            assert!(
                !event_nodes.is_empty(),
                "real workspace graph should store {event_qn}"
            );
            let incoming = graph
                .get_incoming(event_nodes[0].id)
                .expect("incoming edges should load");
            assert!(
                incoming.iter().any(|edge| {
                    edge.kind == EdgeKind::ProducesEventFor && edge.source == report_producer.id
                }),
                "real workspace graph should attach ProducesEventFor from emitReportQueued to {event_qn}; got {incoming:?}"
            );
        }

        let document_nodes = graph
            .nodes_by_repo("backend_standard")
            .expect("backend_standard nodes should load");
        let service_dispatcher = document_nodes
            .iter()
            .find(|node| {
                node.kind == NodeKind::Function
                    && node.name == "handleEvent"
                    && node.file_path.ends_with("event-handlers.service.ts")
            })
            .expect("real backend_standard repo should include handleEvent");
        for event_qn in [
            "__event__kafka__csv.generation.queued",
            "__event__kafka__pdf.generation.queued",
            "__event__kafka__document.report-generation.queued",
        ] {
            let event_nodes = graph
                .nodes_by_external_id(NodeKind::Event, event_qn)
                .expect("event nodes should load");
            assert!(
                !event_nodes.is_empty(),
                "real workspace graph should store {event_qn}"
            );
            let incoming = graph
                .get_incoming(event_nodes[0].id)
                .expect("incoming edges should load");
            assert!(
                incoming.iter().any(|edge| {
                    edge.kind == EdgeKind::UsesEventFrom && edge.source == service_dispatcher.id
                }),
                "real workspace graph should attach UsesEventFrom from handleEvent to {event_qn}; got {incoming:?}"
            );
        }
    }

    #[test]
    #[ignore = "requires GATHER_STEP_REAL_WORKSPACE to point at a real workspace with an existing .gather-step index"]
    fn real_workspace_local_storage_contains_report_flow_event_edges() {
        let workspace = std::env::var("GATHER_STEP_REAL_WORKSPACE")
            .expect("GATHER_STEP_REAL_WORKSPACE must be set for the local-storage probe");
        let workspace = PathBuf::from(workspace);
        let graph = crate::GraphStoreDb::open(workspace.join(".gather-step/storage/graph.redb"))
            .expect("workspace-local graph should open");

        let report_nodes = graph
            .nodes_by_repo("report")
            .expect("report nodes should load");
        let report_producer = report_nodes
            .iter()
            .find(|node| {
                node.kind == NodeKind::Function
                    && node.name == "emitReportQueued"
                    && node.file_path.ends_with("report-events.concern.ts")
            })
            .expect("workspace-local graph should include report emitReportQueued");
        for event_qn in [
            "__event__kafka__csv.generation.queued",
            "__event__kafka__pdf.generation.queued",
        ] {
            let event_nodes = graph
                .nodes_by_external_id(NodeKind::Event, event_qn)
                .expect("event nodes should load");
            assert!(
                !event_nodes.is_empty(),
                "workspace-local graph should store {event_qn}"
            );
            let incoming = graph
                .get_incoming(event_nodes[0].id)
                .expect("incoming edges should load");
            assert!(
                incoming.iter().any(|edge| {
                    edge.kind == EdgeKind::ProducesEventFor && edge.source == report_producer.id
                }),
                "workspace-local graph should attach ProducesEventFor from emitReportQueued to {event_qn}; got {incoming:?}"
            );
        }

        let document_nodes = graph
            .nodes_by_repo("backend_standard")
            .expect("backend_standard nodes should load");
        let service_dispatcher = document_nodes
            .iter()
            .find(|node| {
                node.kind == NodeKind::Function
                    && node.name == "handleEvent"
                    && node.file_path.ends_with("event-handlers.service.ts")
            })
            .expect("workspace-local graph should include service handleEvent");
        for event_qn in [
            "__event__kafka__csv.generation.queued",
            "__event__kafka__pdf.generation.queued",
            "__event__kafka__document.report-generation.queued",
        ] {
            let event_nodes = graph
                .nodes_by_external_id(NodeKind::Event, event_qn)
                .expect("event nodes should load");
            assert!(
                !event_nodes.is_empty(),
                "workspace-local graph should store {event_qn}"
            );
            let incoming = graph
                .get_incoming(event_nodes[0].id)
                .expect("incoming edges should load");
            assert!(
                incoming.iter().any(|edge| {
                    edge.kind == EdgeKind::UsesEventFrom && edge.source == service_dispatcher.id
                }),
                "workspace-local graph should attach UsesEventFrom from service handleEvent to {event_qn}; got {incoming:?}"
            );
        }
    }

    #[test]
    fn orchestrator_completes_when_output_spans_multiple_writer_batches() {
        let repo_root = TestDir::new("multi-batch-repo");
        let storage_root = TestDir::new("multi-batch-storage");
        fs::create_dir_all(repo_root.path().join("src")).expect("src dir should exist");

        for index in 0..5 {
            fs::write(
                repo_root.path().join(format!("src/file{index}.ts")),
                format!("export function fn{index}() {{ return {index}; }}\n"),
            )
            .expect("fixture file should write");
        }

        let options = IndexingOptions {
            batch_size: 2,
            ..IndexingOptions::default()
        };
        let indexer = RepoIndexer::open(storage_root.path(), options).expect("indexer");
        let stats = indexer
            .index_repo("multi-batch", repo_root.path(), None)
            .expect("indexing should succeed");

        assert_eq!(stats.files_parsed, 5);
        assert!(stats.nodes_created >= 10);

        let graph = indexer.storage().graph();
        let nodes = graph
            .nodes_by_repo("multi-batch")
            .expect("repo nodes should load");
        let function_count = nodes
            .iter()
            .filter(|node| node.kind == NodeKind::Function)
            .count();
        assert_eq!(function_count, 5);
    }

    #[test]
    fn orchestrator_resolves_calls_through_custom_tsconfig_alias() {
        let repo_root = TestDir::new("alias-repo");
        let storage_root = TestDir::new("alias-storage");
        fs::create_dir_all(repo_root.path().join("src/lib")).expect("src/lib dir should exist");

        fs::write(
            repo_root.path().join("tsconfig.json"),
            r#"
{
  "compilerOptions": {
    "baseUrl": ".",
    "paths": {
      "@lib/*": ["src/lib/*"]
    }
  }
}
"#,
        )
        .expect("tsconfig fixture should write");
        fs::write(
            repo_root.path().join("src/lib/helper.ts"),
            "export function helper() { return 1; }\n",
        )
        .expect("helper fixture should write");
        fs::write(
            repo_root.path().join("src/caller.ts"),
            "import { helper } from '@lib/helper';\nexport function caller() { return helper(); }\n",
        )
        .expect("caller fixture should write");

        let indexer =
            RepoIndexer::open(storage_root.path(), IndexingOptions::default()).expect("indexer");
        let stats = indexer
            .index_repo("alias-service", repo_root.path(), None)
            .expect("indexing should succeed");

        assert_eq!(stats.files_parsed, 2);

        let graph = indexer.storage().graph();
        let caller_nodes = graph
            .nodes_by_file("alias-service", "src/caller.ts")
            .expect("caller nodes should load");
        let helper_nodes = graph
            .nodes_by_file("alias-service", "src/lib/helper.ts")
            .expect("helper nodes should load");
        let caller_function = caller_nodes
            .iter()
            .find(|node| node.kind == NodeKind::Function && node.name == "caller")
            .expect("caller function should exist");
        let helper_function = helper_nodes
            .iter()
            .find(|node| node.kind == NodeKind::Function && node.name == "helper")
            .expect("helper function should exist");
        let outgoing = graph
            .get_outgoing(caller_function.id)
            .expect("outgoing edges should load");

        assert!(
            outgoing
                .iter()
                .any(|edge| edge.kind == EdgeKind::Calls && edge.target == helper_function.id)
        );
    }

    #[test]
    fn incremental_index_reparses_changed_file_and_direct_dependents_only() {
        let repo_root = TestDir::new("incremental-repo");
        let storage_root = TestDir::new("incremental-storage");
        fs::create_dir_all(repo_root.path().join("src")).expect("src dir should exist");
        fs::write(
            repo_root.path().join("src/helper.ts"),
            "export function helper() { return 1; }\n",
        )
        .expect("helper fixture should write");
        fs::write(
            repo_root.path().join("src/caller.ts"),
            "import { helper } from './helper';\nexport function caller() { return helper(); }\n",
        )
        .expect("caller fixture should write");
        fs::write(
            repo_root.path().join("src/unrelated.ts"),
            "export function unrelated() { return 3; }\n",
        )
        .expect("unrelated fixture should write");

        let indexer =
            RepoIndexer::open(storage_root.path(), IndexingOptions::default()).expect("indexer");
        indexer
            .index_repo("incremental-service", repo_root.path(), None)
            .expect("initial indexing should succeed");

        fs::write(
            repo_root.path().join("src/helper.ts"),
            "export function helper() { return 2; }\n",
        )
        .expect("helper update should write");

        let (changed, stats) = indexer
            .index_repo_incremental("incremental-service", repo_root.path(), None)
            .expect("incremental indexing should succeed");

        assert_eq!(changed.added.len(), 0);
        assert_eq!(
            changed
                .modified
                .iter()
                .map(|file| file.path.as_str())
                .collect::<Vec<_>>(),
            vec!["src/helper.ts"]
        );
        assert_eq!(changed.deleted.len(), 0);
        assert_eq!(stats.files_parsed, 2);

        let graph = indexer.storage().graph();
        let caller_nodes = graph
            .nodes_by_file("incremental-service", "src/caller.ts")
            .expect("caller nodes should load");
        let helper_nodes = graph
            .nodes_by_file("incremental-service", "src/helper.ts")
            .expect("helper nodes should load");
        let caller_function = caller_nodes
            .iter()
            .find(|node| node.kind == NodeKind::Function && node.name == "caller")
            .expect("caller function should exist");
        let helper_function = helper_nodes
            .iter()
            .find(|node| node.kind == NodeKind::Function && node.name == "helper")
            .expect("helper function should exist");
        let outgoing = graph
            .get_outgoing(caller_function.id)
            .expect("outgoing edges should load");

        assert!(
            outgoing
                .iter()
                .any(|edge| edge.kind == EdgeKind::Calls && edge.target == helper_function.id)
        );
        assert_eq!(
            graph
                .nodes_by_file("incremental-service", "src/unrelated.ts")
                .expect("unrelated nodes should load")
                .iter()
                .filter(|node| node.kind == NodeKind::Function)
                .count(),
            1
        );
    }

    #[test]
    fn hinted_incremental_index_should_reparse_reverse_dependents_too() {
        let repo_root = TestDir::new("hinted-incremental-repo");
        let storage_root = TestDir::new("hinted-incremental-storage");
        fs::create_dir_all(repo_root.path().join("src")).expect("src dir should exist");
        fs::write(
            repo_root.path().join("src/helper.ts"),
            "export function helper() { return 1; }\n",
        )
        .expect("helper fixture should write");
        fs::write(
            repo_root.path().join("src/caller.ts"),
            "import { helper } from './helper';\nexport function caller() { return helper(); }\n",
        )
        .expect("caller fixture should write");

        let indexer =
            RepoIndexer::open(storage_root.path(), IndexingOptions::default()).expect("indexer");
        indexer
            .index_repo("hinted-service", repo_root.path(), None)
            .expect("initial indexing should succeed");

        fs::write(
            repo_root.path().join("src/helper.ts"),
            "export function helper() { return 2; }\n",
        )
        .expect("helper update should write");

        let hint = vec!["src/helper.ts".to_owned()];
        let (changed, stats) = indexer
            .index_repo_incremental_with_hint("hinted-service", repo_root.path(), Some(&hint), None)
            .expect("hinted incremental indexing should succeed");

        assert_eq!(
            changed
                .modified
                .iter()
                .map(|file| file.path.as_str())
                .collect::<Vec<_>>(),
            vec!["src/helper.ts"]
        );
        assert_eq!(
            stats.files_parsed, 2,
            "hinted incremental indexing should still reparse reverse dependents"
        );
    }

    #[test]
    #[cfg(unix)]
    fn symlinked_package_manifest_is_not_indexed() {
        let repo_root = TestDir::new("symlink-manifest-repo");
        let storage_root = TestDir::new("symlink-manifest-storage");
        fs::create_dir_all(repo_root.path().join("src")).expect("src dir should exist");
        fs::write(
            repo_root.path().join("external.json"),
            r#"{ "dependencies": { "@nestjs/core": "^11.0.0", "@workspace/shared-contracts": "2.3.1" } }"#,
        )
        .expect("external manifest");
        symlink(
            repo_root.path().join("external.json"),
            repo_root.path().join("package.json"),
        )
        .expect("manifest symlink");
        fs::write(
            repo_root.path().join("src/app.ts"),
            "export function app() { return 1; }\n",
        )
        .expect("source file");

        let indexer =
            RepoIndexer::open(storage_root.path(), IndexingOptions::default()).expect("indexer");
        let stats = indexer
            .index_repo("symlink-manifest", repo_root.path(), None)
            .expect("indexing should succeed");

        assert_eq!(stats.files_parsed, 1);
        assert!(
            indexer
                .storage()
                .graph()
                .nodes_by_file("symlink-manifest", "package.json")
                .expect("package.json nodes should load")
                .is_empty()
        );
        assert!(
            indexer
                .storage()
                .graph()
                .nodes_by_type(NodeKind::SharedSymbol)
                .expect("shared symbols should load")
                .is_empty()
        );
    }

    #[test]
    fn orchestrator_skips_nestjs_extractor_when_package_json_is_absent() {
        let repo_root = TestDir::new("plain-repo");
        let storage_root = TestDir::new("plain-storage");
        fs::create_dir_all(repo_root.path().join("src")).expect("src dir should exist");
        fs::write(
            repo_root.path().join("src/controller.ts"),
            r#"
import { Controller, Get } from '@nestjs/common';

@Controller('items')
export class ItemController {
  @Get('list')
  list() {
    return [];
  }
}
"#,
        )
        .expect("controller fixture should write");

        let indexer =
            RepoIndexer::open(storage_root.path(), IndexingOptions::default()).expect("indexer");
        let stats = indexer
            .index_repo("plain-service", repo_root.path(), None)
            .expect("indexing should succeed");

        assert_eq!(stats.files_parsed, 1);

        let route_nodes = indexer
            .storage()
            .graph()
            .nodes_by_type(NodeKind::Route)
            .expect("route nodes should load");
        assert!(route_nodes.is_empty());
    }

    #[test]
    fn unresolved_cross_file_calls_are_persisted_and_resolved_on_later_index() {
        let repo_root = TestDir::new("unresolved-repo");
        let storage_root = TestDir::new("unresolved-storage");
        fs::create_dir_all(repo_root.path().join("src")).expect("src dir should exist");
        fs::write(
            repo_root.path().join("src/caller.ts"),
            "export function caller() { return helper(); }\n",
        )
        .expect("caller fixture should write");

        let indexer =
            RepoIndexer::open(storage_root.path(), IndexingOptions::default()).expect("indexer");
        let first = indexer
            .index_repo("unresolved-service", repo_root.path(), None)
            .expect("initial indexing should succeed");
        assert_eq!(first.files_parsed, 1);

        let caller_nodes = indexer
            .storage()
            .graph()
            .nodes_by_file("unresolved-service", "src/caller.ts")
            .expect("caller nodes should load");
        let caller_function = caller_nodes
            .iter()
            .find(|node| node.kind == NodeKind::Function && node.name == "caller")
            .expect("caller function should exist");
        let outgoing = indexer
            .storage()
            .graph()
            .get_outgoing(caller_function.id)
            .expect("outgoing edges should load");
        assert!(!outgoing.iter().any(|edge| edge.kind == EdgeKind::Calls));

        let unresolved = indexer
            .storage()
            .metadata()
            .unresolved_resolution_inputs_by_repo("unresolved-service")
            .expect("unresolved inputs should load");
        assert_eq!(unresolved.len(), 1);
        assert_eq!(unresolved[0].call_sites.len(), 1);
        assert_eq!(unresolved[0].call_sites[0].callee_name, "helper");

        fs::write(
            repo_root.path().join("src/helper.ts"),
            "export function helper() { return 1; }\n",
        )
        .expect("helper fixture should write");

        let second = indexer
            .index_repo("unresolved-service", repo_root.path(), None)
            .expect("second indexing should succeed");
        assert_eq!(second.files_parsed, 1);

        let helper_nodes = indexer
            .storage()
            .graph()
            .nodes_by_file("unresolved-service", "src/helper.ts")
            .expect("helper nodes should load");
        let helper_function = helper_nodes
            .iter()
            .find(|node| node.kind == NodeKind::Function && node.name == "helper")
            .expect("helper function should exist");
        let outgoing = indexer
            .storage()
            .graph()
            .get_outgoing(caller_function.id)
            .expect("outgoing edges should load after resolution");
        assert!(
            outgoing
                .iter()
                .any(|edge| edge.kind == EdgeKind::Calls && edge.target == helper_function.id)
        );

        let unresolved = indexer
            .storage()
            .metadata()
            .unresolved_resolution_inputs_by_repo("unresolved-service")
            .expect("unresolved inputs should load after resolution");
        assert!(unresolved.is_empty());
    }

    #[test]
    fn parse_tolerates_invalid_utf8_without_panicking_workers() {
        // Bytes 0x80 and 0xff are not valid UTF-8 continuation sequences.
        // The parser must substitute U+FFFD and continue rather than aborting
        // the entire repo index run.
        let repo_root = TestDir::new("parse-utf8-tolerate");
        let storage_root = TestDir::new("parse-utf8-tolerate-storage");
        fs::create_dir_all(repo_root.path().join("src")).expect("src dir should exist");
        fs::write(
            repo_root.path().join("src/good.ts"),
            "export function ok() { return 1; }\n",
        )
        .expect("good fixture should write");
        // Mix of valid ASCII and invalid UTF-8 high bytes (Latin-1 / Windows-1252
        // style), e.g. `f o <0x80> o` — a common pattern in copyright headers.
        fs::write(
            repo_root.path().join("src/latin1.ts"),
            b"// Copyright \xa9 ACME\nexport function latin() { return 42; }\n".as_slice(),
        )
        .expect("latin-1 fixture should write");

        let indexer =
            RepoIndexer::open(storage_root.path(), IndexingOptions::default()).expect("indexer");
        let stats = indexer
            .index_repo("utf8-tolerate-service", repo_root.path(), None)
            .expect("indexing must succeed even with non-UTF-8 bytes in source files");

        // Both files must have been visited; the file with non-UTF-8 bytes
        // is still parsed (replacement chars are substituted) and counts
        // toward parsed output.
        assert_eq!(stats.files_parsed, 2, "both files should be parsed");
    }

    #[test]
    fn empty_repo_indexes_to_zero_stats() {
        let repo_root = TestDir::new("empty-repo");
        let storage_root = TestDir::new("empty-storage");

        let indexer =
            RepoIndexer::open(storage_root.path(), IndexingOptions::default()).expect("indexer");
        let stats = indexer
            .index_repo("empty-service", repo_root.path(), None)
            .expect("indexing should succeed");

        assert_eq!(stats.files_parsed, 0);
        assert_eq!(stats.nodes_created, 0);
        assert_eq!(stats.edges_created, 0);
    }

    #[test]
    fn progress_callback_reports_traverse_parse_and_write() {
        let repo_root = TestDir::new("progress-repo");
        let storage_root = TestDir::new("progress-storage");
        fs::create_dir_all(repo_root.path().join("src")).expect("src dir should exist");
        fs::write(
            repo_root.path().join("src/file.ts"),
            "export function run() { return 1; }\n",
        )
        .expect("fixture should write");

        let indexer =
            RepoIndexer::open(storage_root.path(), IndexingOptions::default()).expect("indexer");
        let progress_events = Arc::new(Mutex::new(Vec::new()));
        let progress_sink = Arc::clone(&progress_events);
        indexer
            .index_repo(
                "progress-service",
                repo_root.path(),
                Some(&move |progress| {
                    progress_sink
                        .lock()
                        .expect("progress mutex should not poison")
                        .push((progress.phase, progress.processed, progress.total));
                }),
            )
            .expect("indexing should succeed");

        let progress_events = progress_events
            .lock()
            .expect("progress mutex should not poison");
        assert_eq!(progress_events.len(), 3);
        assert_eq!(progress_events[0].0, "traverse");
        assert_eq!(progress_events[1].0, "parse");
        assert_eq!(progress_events[2].0, "write");
    }

    #[test]
    fn reindex_unchanged_repo_skips_storage_writes() {
        let repo_root = TestDir::new("reindex-unchanged-repo");
        let storage_root = TestDir::new("reindex-unchanged-storage");
        fs::create_dir_all(repo_root.path().join("src")).expect("src dir should exist");

        fs::write(
            repo_root.path().join("src/alpha.ts"),
            "export function alpha() { return 1; }\n",
        )
        .expect("alpha fixture should write");
        fs::write(
            repo_root.path().join("src/beta.ts"),
            "export function beta() { return 2; }\n",
        )
        .expect("beta fixture should write");
        fs::write(
            repo_root.path().join("src/gamma.ts"),
            "export function gamma() { return 3; }\n",
        )
        .expect("gamma fixture should write");

        let indexer =
            RepoIndexer::open(storage_root.path(), IndexingOptions::default()).expect("indexer");

        let first_stats = indexer
            .index_repo("reindex-service", repo_root.path(), None)
            .expect("first indexing should succeed");
        assert_eq!(first_stats.files_parsed, 3);
        assert!(
            first_stats.nodes_created > 0,
            "first index should create nodes"
        );
        assert!(
            first_stats.edges_created > 0,
            "first index should create edges"
        );

        let second_stats = indexer
            .index_repo("reindex-service", repo_root.path(), None)
            .expect("second indexing should succeed");

        // Storage writes are skipped because content hashes match — no new nodes or edges
        assert_eq!(second_stats.nodes_created, 0);
        assert_eq!(second_stats.edges_created, 0);
    }

    #[test]
    #[expect(
        clippy::similar_names,
        reason = "repo_a_* and repo_b_* are deliberately parallel test names"
    )]
    fn concurrent_indexing_of_two_repos_does_not_corrupt_data() {
        let repo_a_root = TestDir::new("concurrent-repo-a");
        let repo_b_root = TestDir::new("concurrent-repo-b");
        let storage_root = TestDir::new("concurrent-storage");
        fs::create_dir_all(repo_a_root.path().join("src")).expect("repo-a src dir should exist");
        fs::create_dir_all(repo_b_root.path().join("src")).expect("repo-b src dir should exist");

        fs::write(
            repo_a_root.path().join("src/service_a.ts"),
            "export function serviceA() { return 'a'; }\n",
        )
        .expect("repo-a fixture should write");
        fs::write(
            repo_a_root.path().join("src/util_a.ts"),
            "export function utilA() { return 'a_util'; }\n",
        )
        .expect("repo-a util fixture should write");

        fs::write(
            repo_b_root.path().join("src/service_b.ts"),
            "export function serviceB() { return 'b'; }\n",
        )
        .expect("repo-b fixture should write");
        fs::write(
            repo_b_root.path().join("src/util_b.ts"),
            "export function utilB() { return 'b_util'; }\n",
        )
        .expect("repo-b util fixture should write");

        let indexer =
            RepoIndexer::open(storage_root.path(), IndexingOptions::default()).expect("indexer");

        thread::scope(|scope| {
            let handle_a = scope.spawn(|| {
                indexer
                    .index_repo("repo-a", repo_a_root.path(), None)
                    .expect("repo-a indexing should succeed")
            });
            let handle_b = scope.spawn(|| {
                indexer
                    .index_repo("repo-b", repo_b_root.path(), None)
                    .expect("repo-b indexing should succeed")
            });

            let stats_a = handle_a.join().expect("repo-a thread should join");
            let stats_b = handle_b.join().expect("repo-b thread should join");

            assert_eq!(stats_a.files_parsed, 2);
            assert_eq!(stats_b.files_parsed, 2);
        });

        let graph = indexer.storage().graph();

        // Verify repo-a nodes exist and belong to repo-a
        let repo_a_nodes = graph
            .nodes_by_repo("repo-a")
            .expect("repo-a nodes should load");
        assert!(
            !repo_a_nodes.is_empty(),
            "repo-a should have nodes in the graph"
        );
        assert!(
            repo_a_nodes.iter().all(|node| node.repo == "repo-a"),
            "all repo-a nodes should belong to repo-a"
        );
        assert!(
            repo_a_nodes
                .iter()
                .any(|node| node.kind == NodeKind::Function && node.name == "serviceA"),
            "repo-a should contain serviceA"
        );
        assert!(
            repo_a_nodes
                .iter()
                .any(|node| node.kind == NodeKind::Function && node.name == "utilA"),
            "repo-a should contain utilA"
        );

        // Verify repo-b nodes exist and belong to repo-b
        let repo_b_nodes = graph
            .nodes_by_repo("repo-b")
            .expect("repo-b nodes should load");
        assert!(
            !repo_b_nodes.is_empty(),
            "repo-b should have nodes in the graph"
        );
        assert!(
            repo_b_nodes.iter().all(|node| node.repo == "repo-b"),
            "all repo-b nodes should belong to repo-b"
        );
        assert!(
            repo_b_nodes
                .iter()
                .any(|node| node.kind == NodeKind::Function && node.name == "serviceB"),
            "repo-b should contain serviceB"
        );
        assert!(
            repo_b_nodes
                .iter()
                .any(|node| node.kind == NodeKind::Function && node.name == "utilB"),
            "repo-b should contain utilB"
        );

        // Verify no cross-contamination: repo-a files should not appear under repo-b
        let repo_a_file_paths: Vec<&str> = repo_a_nodes
            .iter()
            .filter(|node| node.kind == NodeKind::File)
            .map(|node| node.file_path.as_str())
            .collect();
        let repo_b_file_paths: Vec<&str> = repo_b_nodes
            .iter()
            .filter(|node| node.kind == NodeKind::File)
            .map(|node| node.file_path.as_str())
            .collect();
        for path in &repo_a_file_paths {
            assert!(
                !repo_b_file_paths.contains(path),
                "repo-a file {path} should not appear in repo-b"
            );
        }
        for path in &repo_b_file_paths {
            assert!(
                !repo_a_file_paths.contains(path),
                "repo-b file {path} should not appear in repo-a"
            );
        }
    }

    #[test]
    fn orchestrator_indexes_mixed_language_repo() {
        let repo_root = TestDir::new("mixed-repo");
        let storage_root = TestDir::new("mixed-storage");
        fs::create_dir_all(repo_root.path().join("src")).expect("src dir should exist");
        fs::write(
            repo_root.path().join("src/caller.ts"),
            "export function caller() { return helper(); }\n",
        )
        .expect("typescript fixture should write");
        fs::write(
            repo_root.path().join("src/helpers.py"),
            "def helper():\n    return 1\n",
        )
        .expect("python fixture should write");

        let indexer =
            RepoIndexer::open(storage_root.path(), IndexingOptions::default()).expect("indexer");
        let stats = indexer
            .index_repo("mixed-service", repo_root.path(), None)
            .expect("indexing should succeed");

        assert_eq!(stats.files_parsed, 2);
        let graph = indexer.storage().graph();
        assert!(
            graph
                .nodes_by_file("mixed-service", "src/caller.ts")
                .expect("typescript nodes should load")
                .iter()
                .any(|node| node.kind == NodeKind::Function && node.name == "caller")
        );
        assert!(
            graph
                .nodes_by_file("mixed-service", "src/helpers.py")
                .expect("python nodes should load")
                .iter()
                .any(|node| node.kind == NodeKind::Function && node.name == "helper")
        );
    }

    #[test]
    fn type_only_import_produces_uses_type_from_edge() {
        let repo_root = TestDir::new("type-import-repo");
        let storage_root = TestDir::new("type-import-storage");
        fs::create_dir_all(repo_root.path().join("src")).expect("src dir should exist");

        // Target file that will be referenced by type-only and regular imports.
        fs::write(
            repo_root.path().join("src/dto.ts"),
            "export interface IAlertDto { id: string; }\n",
        )
        .expect("dto fixture should write");

        // consumer.ts: one type-only import (should produce UsesTypeFrom) and one
        // regular import (must NOT produce UsesTypeFrom).
        fs::write(
            repo_root.path().join("src/consumer.ts"),
            "import type { IAlertDto } from './dto';\nimport { IAlertDto as _Unused } from './dto';\nexport function consume(x: IAlertDto) { return x; }\n",
        )
        .expect("consumer fixture should write");

        let indexer =
            RepoIndexer::open(storage_root.path(), IndexingOptions::default()).expect("indexer");
        indexer
            .index_repo("type-import-service", repo_root.path(), None)
            .expect("indexing should succeed");

        let graph = indexer.storage().graph();

        // Collect all UsesTypeFrom edges across the whole repo.
        let consumer_file_nodes = graph
            .nodes_by_file("type-import-service", "src/consumer.ts")
            .expect("consumer file nodes should load");
        let consumer_file_node = consumer_file_nodes
            .iter()
            .find(|node| node.kind == NodeKind::File)
            .expect("consumer file node should exist");

        let outgoing = graph
            .get_outgoing(consumer_file_node.id)
            .expect("outgoing edges for consumer should load");

        let uses_type_edges: Vec<_> = outgoing
            .iter()
            .filter(|edge| edge.kind == EdgeKind::UsesTypeFrom)
            .collect();

        // Exactly one UsesTypeFrom edge: from consumer.ts to dto.ts (type-only import).
        // The regular import of the same binding must NOT contribute an additional edge.
        assert_eq!(
            uses_type_edges.len(),
            1,
            "expected exactly one UsesTypeFrom edge from consumer.ts; got {}: {:?}",
            uses_type_edges.len(),
            uses_type_edges,
        );

        // Verify the edge targets the dto file node.
        let dto_file_nodes = graph
            .nodes_by_file("type-import-service", "src/dto.ts")
            .expect("dto file nodes should load");
        let dto_file_node = dto_file_nodes
            .iter()
            .find(|node| node.kind == NodeKind::File)
            .expect("dto file node should exist");

        assert_eq!(
            uses_type_edges[0].target, dto_file_node.id,
            "UsesTypeFrom edge should target the dto.ts file node",
        );
    }

    #[test]
    fn bulk_mode_guard_disables_bulk_on_drop_after_panic() {
        // A panic inside the bulk region must not leave the graph store in bulk
        // mode permanently.  Use `catch_unwind` to simulate the panic path.
        let storage_root = TestDir::new("bulk-guard");
        let indexer = RepoIndexer::open(&storage_root.path, IndexingOptions::default())
            .expect("indexer should open");
        let graph = indexer.storage().graph();

        // Verify the initial state is non-bulk.
        assert!(
            !graph.is_bulk_mode(),
            "graph should not be in bulk mode before the guard"
        );

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = super::BulkModeGuard::new(graph);
            assert!(
                graph.is_bulk_mode(),
                "graph should be in bulk mode while guard is alive"
            );
            panic!("simulated bulk-region panic");
        }));

        assert!(result.is_err(), "the inner closure should have panicked");
        assert!(
            !graph.is_bulk_mode(),
            "bulk mode must be off after the guard drops on panic"
        );
    }
}
