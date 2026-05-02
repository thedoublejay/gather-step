#![forbid(unsafe_code)]

use std::{collections::BTreeSet, path::Path, thread, time::Duration};

use gather_step_core::{EdgeData, NodeData};
use redb::SavepointError;
use thiserror::Error;
use tracing::{error, warn};

pub mod daemon_metadata;
pub mod daemon_state;
pub mod fs_mode;
pub mod graph_store;
pub mod incremental;
pub mod indexer;
pub mod metadata;
pub mod pack_store;
pub mod reconcile;
pub mod search_store;
pub mod stores;
pub mod watcher;
pub mod workspace_indexer;

pub use daemon_metadata::{
    StorageDaemonMetadata, StorageDaemonMetadataError, StorageDaemonMetadataGuard,
    daemon_pid_path_for_graph_path, daemon_pid_path_for_storage_root,
};
pub use graph_store::{EdgeCountSummary, GraphStore, GraphStoreDb, GraphStoreError};
pub use incremental::{
    ChangedSet, IncrementalError, IncrementalFileEntry, RepoSnapshot, TrackedPath,
    classify_changes, classify_selected_changes, compute_affected_set, snapshot_repo_files,
    snapshot_selected_repo_files,
};
pub use indexer::{
    BulkModeGuard, DeploymentIndexingOptions, IndexProgress, IndexingOptions, IndexingStats,
    RepoIndexPayload, RepoIndexer, RepoIndexerError,
};
pub use metadata::{
    CoChangePairRecord, CommitFileChangeKind, CommitFileDeltaRecord, CommitRecord,
    ContextPackRecord, ContextPackStats, FileAnalytics, FileIndexState, MetadataStore,
    MetadataStoreDb, MetadataStoreError, PackCallLogEntry, PayloadContractQuery,
    PayloadContractStoreRecord,
};
pub use pack_store::{PackBlob, PackStore};
pub use reconcile::{
    DanglingCleanup, ReconcileStats, StorageReconcileError, cleanup_dangling_edges_classified,
    cleanup_dangling_edges_for_files, reconcile_changed_files, reconcile_changed_files_with_mode,
};

/// Outcome returned by [`StorageCoordinator::reconcile_search`].
///
/// `Full` means all search documents were rebuilt from the graph successfully.
/// `Partial` means the search index was updated as much as possible but a
/// tantivy error prevented full completion; the caller should treat the search
/// index as potentially stale and log the error.
#[derive(Debug)]
pub enum ReconcileOutcome {
    /// The search index was rebuilt completely.
    Full,
    /// Reconcile could not complete; the search index may be stale.
    Partial {
        /// Human-readable description of the tantivy error.
        search_error: String,
    },
}
pub use search_store::{
    SearchDocument, SearchFilters, SearchHit, SearchStore, SearchStoreError, SearchWorkload,
    TantivySearchStore,
};
pub use stores::{WorkspaceStores, WorkspaceStoresError};
pub use watcher::{
    WatchCause, WatchEvent, Watcher, WatcherConfig, WatcherError, WatcherStatus, WorkspaceWatcher,
};
pub use workspace_indexer::{StorageWorkspaceIndexDelegate, index_workspace_with_storage};

pub struct StorageCoordinator {
    stores: WorkspaceStores,
}

#[derive(Clone, Debug)]
pub struct FileBatch {
    pub repo: String,
    pub file_path: String,
    /// Raw `OsStr` bytes of the file path for lossless BLOB identity in `SQLite`.
    ///
    /// When non-empty this is used instead of `file_path.as_bytes()` when
    /// writing the `file_index_state.file_path` BLOB column, preserving
    /// non-UTF-8 path bytes that would otherwise be mangled by the lossy
    /// `String` conversion.  Callers that do not populate this field (empty
    /// `Vec`) fall back to `file_path.as_bytes()`, which is correct for
    /// ASCII / valid-UTF-8 paths.
    pub path_id_bytes: Vec<u8>,
    pub nodes: Vec<NodeData>,
    pub edges: Vec<EdgeData>,
    pub content_hash: Vec<u8>,
    pub size_bytes: i64,
    pub mtime_ns: i64,
    pub indexed_at: i64,
    pub parse_ms: Option<i64>,
    pub force: bool,
}

impl FileBatch {
    fn effective_path_bytes(&self) -> &[u8] {
        if self.path_id_bytes.is_empty() {
            self.file_path.as_bytes()
        } else {
            &self.path_id_bytes
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct RepoBatch {
    pub repo: String,
    pub files: Vec<FileBatch>,
    pub test_hooks: RepoBatchHooks,
}

#[derive(Clone, Debug, Default)]
pub struct RepoBatchHooks {
    pub fail_after_graph_files: Option<usize>,
    pub pause_after_graph_stage: Option<Duration>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RepoBatchResult {
    pub files_indexed: usize,
    pub nodes_written: usize,
    pub edges_written: usize,
}

#[derive(Debug, Error)]
pub enum StorageCoordinatorError {
    #[error("graph store error: {0}")]
    Graph(#[from] GraphStoreError),
    #[error("search store error: {0}")]
    Search(#[from] SearchStoreError),
    #[error("metadata store error: {0}")]
    Metadata(#[from] MetadataStoreError),
    #[error(transparent)]
    Stores(#[from] WorkspaceStoresError),
    #[error("savepoint error: {0}")]
    Savepoint(#[from] SavepointError),
    #[error("file batch repo `{actual}` did not match coordinator repo `{expected}`")]
    MismatchedRepo { expected: String, actual: String },
    #[error("test hook injected batch failure after {processed} files")]
    InjectedFailure { processed: usize },
    #[error("rollback cleanup failed after `{original}`: {cleanup}")]
    RollbackCleanup { original: String, cleanup: String },
}

impl StorageCoordinator {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, StorageCoordinatorError> {
        Ok(Self::from_stores(WorkspaceStores::open(root)?))
    }

    #[must_use]
    pub fn from_stores(stores: WorkspaceStores) -> Self {
        Self { stores }
    }

    /// Open a coordinator with a **read-only** Tantivy search store.
    ///
    /// The graph and metadata stores are opened read-write. Any attempt to
    /// write to the search index (e.g. inside `reconcile_search`) will fail
    /// with [`SearchStoreError::ReadOnly`], which surfaces as
    /// [`ReconcileOutcome::Partial`].  Use this in tests to verify that
    /// `reconcile_search` correctly returns `Partial` when Tantivy is broken.
    #[cfg(any(test, feature = "test-support"))]
    pub fn open_with_broken_search(
        root: impl AsRef<Path>,
    ) -> Result<Self, StorageCoordinatorError> {
        Ok(Self::from_stores(WorkspaceStores::open_with_broken_search(
            root,
        )?))
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        self.stores.root()
    }

    #[must_use]
    pub fn graph(&self) -> &GraphStoreDb {
        self.stores.graph()
    }

    #[must_use]
    pub fn search(&self) -> &TantivySearchStore {
        self.stores.search()
    }

    #[must_use]
    pub fn metadata(&self) -> &MetadataStoreDb {
        self.stores.metadata()
    }

    #[must_use]
    pub fn stores(&self) -> &WorkspaceStores {
        &self.stores
    }

    /// Compact the graph database to reclaim dead space after bulk writes.
    /// Should only be called when no read or write transactions are open.
    pub fn compact_graph(&mut self) -> Result<bool, GraphStoreError> {
        let path = self.graph().path().to_path_buf();
        self.stores
            .graph_mut()
            .ok_or(GraphStoreError::CompactionRequiresExclusiveHandle { path })
            .and_then(GraphStoreDb::compact)
    }

    /// Rebuild the Tantivy search index for `repo` from canonical graph state.
    ///
    /// Returns [`ReconcileOutcome::Full`] when the search index is consistent
    /// with the graph after this call.  Returns
    /// [`ReconcileOutcome::Partial`] when a Tantivy error prevented full
    /// completion; in that case a `tracing::warn!` is emitted and the caller
    /// can decide whether to surface the error or continue with a stale index.
    ///
    /// This method never silently returns success when the index is broken.
    pub fn reconcile_search(&self, repo: &str) -> ReconcileOutcome {
        let reconcile_result = (|| -> Result<(), StorageCoordinatorError> {
            self.search().delete_by_repo(repo)?;
            let documents = self
                .graph()
                .nodes_by_repo(repo)?
                .into_iter()
                .filter(|node| node.kind.is_search_indexable())
                .map(|node| {
                    // The graph does not persist search recency timestamps; recovery prefers a
                    // correct repo projection over preserving prior Tantivy-specific ordering.
                    SearchDocument::from_node(&node, 0)
                })
                .collect::<Vec<_>>();
            self.search().index_symbols(&documents)?;
            Ok(())
        })();

        match reconcile_result {
            Ok(()) => ReconcileOutcome::Full,
            Err(error) => {
                let _ = self.search().rollback();
                let msg = error.to_string();
                warn!(repo, error = %error, "reconcile_search could not rebuild search index");
                ReconcileOutcome::Partial { search_error: msg }
            }
        }
    }

    pub fn index_repo_batch(
        &self,
        batch: &RepoBatch,
    ) -> Result<RepoBatchResult, StorageCoordinatorError> {
        self.index_repo_batch_impl(batch, true, false)
    }

    /// Run a repo batch without committing file-state rows. When
    /// `cold_index` is true, skip the semantic-bridge read walk (always
    /// empty on a cold index) and the per-file graph delete (nothing to
    /// delete), saving one read transaction per batch and ~N table opens
    /// per file.
    pub(crate) fn index_repo_batch_without_file_states_cold(
        &self,
        batch: &RepoBatch,
        cold_index: bool,
    ) -> Result<RepoBatchResult, StorageCoordinatorError> {
        self.index_repo_batch_impl(batch, false, cold_index)
    }

    fn index_repo_batch_impl(
        &self,
        batch: &RepoBatch,
        commit_file_states: bool,
        cold_index: bool,
    ) -> Result<RepoBatchResult, StorageCoordinatorError> {
        let mut changed_files = Vec::new();
        let reindex_candidates = batch
            .files
            .iter()
            .filter(|file| file.repo == batch.repo && !file.force)
            .map(|file| (file.effective_path_bytes(), file.content_hash.as_slice()))
            .collect::<Vec<_>>();
        let reindex_required = self
            .metadata()
            .should_reindex_batch_by_path_bytes(&batch.repo, &reindex_candidates)?;
        for file in &batch.files {
            if file.repo != batch.repo {
                return Err(StorageCoordinatorError::MismatchedRepo {
                    expected: batch.repo.clone(),
                    actual: file.repo.clone(),
                });
            }
            if file.force || reindex_required.contains(file.effective_path_bytes()) {
                changed_files.push(file);
            }
        }

        if changed_files.is_empty() {
            return Ok(RepoBatchResult::default());
        }

        let changed_file_paths = changed_files
            .iter()
            .map(|file| file.file_path.clone())
            .collect::<Vec<_>>();
        let changed_file_path_ids = changed_files
            .iter()
            .map(|file| file.effective_path_bytes().to_vec())
            .collect::<Vec<_>>();
        // On cold index there are no prior semantic bridges to discover and
        // no context packs to invalidate, so skip the read-transaction walk
        // that would always return empty.
        if !cold_index {
            let invalidation_targets = crate::reconcile::semantic_bridge_related_files(
                self.graph(),
                &batch.repo,
                &changed_file_paths,
            )?
            .into_iter()
            .chain(
                changed_file_paths
                    .iter()
                    .cloned()
                    .map(|file_path| (batch.repo.clone(), file_path)),
            )
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
            let _ = self
                .metadata()
                .invalidate_context_packs_for_targets(&invalidation_targets)?;
        }
        self.metadata().with_write_txn(|tx| {
            for file_path_id in &changed_file_path_ids {
                tx.execute(
                    "DELETE FROM file_index_state WHERE repo = ?1 AND file_path = ?2",
                    rusqlite::params![&batch.repo, file_path_id],
                )?;
                tx.execute(
                    "DELETE FROM file_dependencies WHERE source_repo = ?1 AND source_path = ?2",
                    rusqlite::params![&batch.repo, file_path_id],
                )?;
                tx.execute(
                    "DELETE FROM file_dependencies WHERE target_repo = ?1 AND target_path = ?2",
                    rusqlite::params![&batch.repo, file_path_id],
                )?;
            }
            Ok(())
        })?;

        let mut write_txn = self.graph().begin_write_txn()?;
        // In bulk mode (Durability::None) persistent savepoints are not
        // supported by redb, so use ephemeral savepoints instead. They
        // still allow rollback within the session but don't survive crashes
        // — which is fine since bulk mode already forgoes crash safety.
        let use_ephemeral = self.graph().is_bulk_mode();
        let savepoint_id = if use_ephemeral {
            None
        } else {
            Some(write_txn.persistent_savepoint()?)
        };
        let savepoint = if let Some(id) = savepoint_id {
            Some(write_txn.get_persistent_savepoint(id)?)
        } else {
            Some(write_txn.ephemeral_savepoint()?)
        };

        let graph_result = (|| -> Result<RepoBatchResult, StorageCoordinatorError> {
            let mut result = RepoBatchResult::default();

            for (index, file) in changed_files.iter().enumerate() {
                // On cold index there is nothing to delete: repo has zero
                // prior nodes by definition. The repo-level filesystem lock
                // prevents concurrent writers from another process, so we can
                // safely skip the delete path.
                if !cold_index {
                    GraphStoreDb::delete_file_nodes_in_txn(
                        &write_txn,
                        &file.repo,
                        &file.file_path,
                    )?;
                }
                // Edges produced here come from the same ParsedFile as the
                // nodes, so their source/target/owner_file references are
                // guaranteed to exist — skip the per-edge node validation.
                GraphStoreDb::bulk_insert_in_txn_trusted(&write_txn, &file.nodes, &file.edges)?;

                result.files_indexed += 1;
                result.nodes_written += file.nodes.len();
                result.edges_written += file.edges.len();

                if batch.test_hooks.fail_after_graph_files == Some(index + 1) {
                    return Err(StorageCoordinatorError::InjectedFailure {
                        processed: index + 1,
                    });
                }
            }

            if let Some(pause) = batch.test_hooks.pause_after_graph_stage {
                thread::sleep(pause);
            }

            Ok(result)
        })();

        let result = match graph_result {
            Ok(result) => result,
            Err(error) => {
                if let Some(sp) = savepoint
                    && let Err(restore_error) = write_txn.restore_savepoint(&sp)
                {
                    if let Some(id) = savepoint_id
                        && let Err(delete_error) = write_txn.delete_persistent_savepoint(id)
                    {
                        error!(
                            savepoint_id = id,
                            original = %error,
                            cleanup = %delete_error,
                            "failed to delete redb savepoint after restore failure",
                        );
                    }
                    error!(
                        savepoint_id = ?savepoint_id,
                        original = %error,
                        cleanup = %restore_error,
                        "failed to restore redb savepoint after graph write error",
                    );
                    return Err(StorageCoordinatorError::RollbackCleanup {
                        original: error.to_string(),
                        cleanup: format!(
                            "savepoint_id={savepoint_id:?}; restore_error={restore_error}"
                        ),
                    });
                }
                if let Some(id) = savepoint_id
                    && let Err(delete_error) = write_txn.delete_persistent_savepoint(id)
                {
                    error!(
                        savepoint_id = id,
                        original = %error,
                        cleanup = %delete_error,
                        "failed to delete redb savepoint after restore",
                    );
                }
                if let Err(abort_error) = write_txn.abort().map_err(GraphStoreError::storage) {
                    error!(
                        original = %error,
                        cleanup = %abort_error,
                        "failed to abort redb transaction after restore",
                    );
                    return Err(StorageCoordinatorError::RollbackCleanup {
                        original: error.to_string(),
                        cleanup: abort_error.to_string(),
                    });
                }
                return Err(error);
            }
        };

        if let Some(id) = savepoint_id {
            write_txn.delete_persistent_savepoint(id)?;
        }
        write_txn.commit().map_err(GraphStoreError::storage)?;

        let search_result = (|| -> Result<(), StorageCoordinatorError> {
            let files = changed_files
                .iter()
                .map(|file| (file.repo.as_str(), file.file_path.as_str()))
                .collect::<Vec<_>>();
            let documents = changed_files
                .iter()
                .flat_map(|file| {
                    file.nodes
                        .iter()
                        .filter(|node| node.kind.is_search_indexable())
                        .map(|node| {
                            SearchDocument::from_node(
                                node,
                                u64::try_from(file.indexed_at).unwrap_or_default(),
                            )
                        })
                })
                .collect::<Vec<_>>();
            self.search().replace_by_files(&files, &documents)?;
            Ok(())
        })();

        if let Err(error) = search_result {
            // redb is already committed here, so a Tantivy failure leaves search behind the graph.
            // `reconcile_search(repo)` can rebuild the repo's search projection from canonical graph
            // state without re-running extraction.
            let _ = self.search().rollback();
            if let ReconcileOutcome::Partial { search_error } = self.reconcile_search(&batch.repo) {
                warn!(
                    repo = batch.repo.as_str(),
                    %search_error,
                    "search reconcile also failed after batch search error; index may be stale",
                );
            }
            return Err(error);
        }

        if commit_file_states {
            let file_states = changed_files
                .iter()
                .map(|file| FileIndexState {
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
                })
                .collect::<Vec<_>>();
            self.metadata().upsert_file_states(&file_states)?;
        }

        Ok(result)
    }

    pub fn purge_deleted_files(
        &self,
        repo: &str,
        file_paths: &[String],
    ) -> Result<(), StorageCoordinatorError> {
        self.purge_deleted_files_impl(repo, file_paths, false)
    }

    /// Like [`purge_deleted_files`] but injects a simulated crash immediately
    /// after the redb commit and before the `SQLite` delete. Returns
    /// `Err(StorageCoordinatorError::InjectedFailure { processed: 0 })` at
    /// that point so callers can test crash-recovery paths.
    ///
    /// Only available in test and `test-support` builds.
    #[cfg(any(test, feature = "test-support"))]
    pub fn purge_deleted_files_crash_after_redb(
        &self,
        repo: &str,
        file_paths: &[String],
    ) -> Result<(), StorageCoordinatorError> {
        self.purge_deleted_files_impl(repo, file_paths, true)
    }

    fn purge_deleted_files_impl(
        &self,
        repo: &str,
        file_paths: &[String],
        crash_after_redb: bool,
    ) -> Result<(), StorageCoordinatorError> {
        if file_paths.is_empty() {
            return Ok(());
        }

        let invalidation_targets =
            crate::reconcile::semantic_bridge_related_files(self.graph(), repo, file_paths)?
                .into_iter()
                .chain(
                    file_paths
                        .iter()
                        .cloned()
                        .map(|file_path| (repo.to_owned(), file_path)),
                )
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
        let _ = self
            .metadata()
            .invalidate_context_packs_for_targets(&invalidation_targets)?;

        // Step 1: write redb entries — delete file nodes from the graph store.
        // Step 2: commit redb — the graph is now the durable source of truth.
        // Step 3: apply SQLite deletes — if this crashes, redb is complete and
        //         SQLite has reconcilable stale rows that a startup pass can purge.
        // Step 4: delete from the search index.
        //
        // Ordering guarantee: a crash between steps 2 and 3 leaves redb complete
        // (file nodes are gone) and SQLite with stale rows.  A reconcile pass at
        // startup can detect this by comparing the two stores and finish the purge.

        // Step 1+2: write and commit redb.
        let write_txn = self.graph().begin_write_txn()?;
        for file_path in file_paths {
            GraphStoreDb::delete_file_nodes_in_txn(&write_txn, repo, file_path)?;
        }
        write_txn.commit().map_err(GraphStoreError::storage)?;

        // Test-only crash injection point: simulate a crash between redb commit
        // and SQLite delete so integration tests can verify crash-recovery.
        if crash_after_redb {
            return Err(StorageCoordinatorError::InjectedFailure { processed: 0 });
        }

        // Step 3: apply SQLite deletes now that redb is durable.
        // Convert display strings to byte slices.  For the deleted-file path
        // these bytes come from `String::from_utf8_lossy` of the BLOB stored at
        // index time, so for ASCII paths they are identical; the remaining delta
        // for non-UTF-8 filenames is tracked separately (see
        // `classify_changes` / `FileIndexState::path_id_bytes`).
        let file_path_ids: Vec<Vec<u8>> =
            file_paths.iter().map(|s| s.as_bytes().to_vec()).collect();
        self.metadata()
            .clear_index_metadata_for_files(repo, &file_path_ids)?;

        // Step 4: delete from the search index.
        let search_files = file_paths
            .iter()
            .map(|file_path| (repo, file_path.as_str()))
            .collect::<Vec<_>>();
        if let Err(error) = self.search().delete_by_files(&search_files) {
            let _ = self.search().rollback();
            if let ReconcileOutcome::Partial { search_error } = self.reconcile_search(repo) {
                warn!(
                    %repo,
                    %search_error,
                    "search reconcile failed after purge search error; index may be stale",
                );
            }
            return Err(StorageCoordinatorError::Search(error));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        time::{SystemTime, UNIX_EPOCH},
    };

    use gather_step_core::{
        EdgeKind, EdgeMetadata, NodeId, NodeKind, SourceSpan, Visibility, node_id,
    };
    use pretty_assertions::assert_eq;
    use rusqlite::params;

    use super::{
        FileBatch, GraphStore, MetadataStore, RepoBatch, RepoBatchHooks, SearchStore,
        StorageCoordinator, StorageCoordinatorError,
    };

    struct TestRoot {
        path: PathBuf,
    }

    impl TestRoot {
        fn new(name: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock should be monotonic enough for tests")
                .as_nanos();
            let path = env::temp_dir().join(format!(
                "gather-step-coordinator-{name}-{}-{nanos}",
                process::id()
            ));
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn file_node(repo: &str, file_path: &str) -> gather_step_core::NodeData {
        gather_step_core::NodeData {
            id: node_id(repo, file_path, NodeKind::File, file_path),
            kind: NodeKind::File,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: file_path.to_owned(),
            qualified_name: Some(format!("{repo}::{file_path}")),
            external_id: None,
            signature: None,
            visibility: None,
            span: Some(SourceSpan {
                line_start: 1,
                line_len: 99,
                column_start: 0,
                column_len: 0,
            }),
            is_virtual: false,
        }
    }

    fn function_node(
        repo: &str,
        file_path: &str,
        name: &str,
        _ordinal: u16,
    ) -> gather_step_core::NodeData {
        gather_step_core::NodeData {
            id: node_id(repo, file_path, NodeKind::Function, name),
            kind: NodeKind::Function,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(format!("{repo}::{name}")),
            external_id: None,
            signature: Some(format!("{name}()")),
            visibility: Some(Visibility::Public),
            span: Some(SourceSpan {
                line_start: 2,
                line_len: 1,
                column_start: 0,
                column_len: 0,
            }),
            is_virtual: false,
        }
    }

    fn search_ignored_node(repo: &str, file_path: &str) -> gather_step_core::NodeData {
        gather_step_core::NodeData {
            id: node_id(repo, file_path, NodeKind::Import, "sharedImport"),
            kind: NodeKind::Import,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: "sharedImport".to_owned(),
            qualified_name: None,
            external_id: None,
            signature: None,
            visibility: None,
            span: Some(SourceSpan {
                line_start: 1,
                line_len: 0,
                column_start: 0,
                column_len: 1,
            }),
            is_virtual: false,
        }
    }

    fn defines_edge(file_id: NodeId, symbol_id: NodeId) -> gather_step_core::EdgeData {
        gather_step_core::EdgeData {
            source: file_id,
            target: symbol_id,
            kind: EdgeKind::Defines,
            metadata: EdgeMetadata::default(),
            owner_file: file_id,
            is_cross_file: false,
        }
    }

    fn batch(repo: &str, file_path: &str, symbol_names: &[&str], hash: &[u8]) -> RepoBatch {
        let file = file_node(repo, file_path);
        let mut nodes = vec![file.clone()];
        let mut edges = Vec::new();
        for (index, symbol_name) in symbol_names.iter().enumerate() {
            let ordinal = u16::try_from(index).expect("test symbol ordinal should fit in u16");
            let symbol = function_node(repo, file_path, symbol_name, ordinal);
            edges.push(defines_edge(file.id, symbol.id));
            nodes.push(symbol);
        }

        RepoBatch {
            repo: repo.to_owned(),
            files: vec![FileBatch {
                repo: repo.to_owned(),
                file_path: file_path.to_owned(),
                path_id_bytes: vec![],
                nodes,
                edges,
                content_hash: hash.to_vec(),
                size_bytes: 0,
                mtime_ns: 0,
                indexed_at: 1_713_000_000,
                parse_ms: Some(10),
                force: false,
            }],
            test_hooks: RepoBatchHooks::default(),
        }
    }

    fn batch_with_non_searchable_node(repo: &str, file_path: &str, hash: &[u8]) -> RepoBatch {
        let file = file_node(repo, file_path);
        let function = function_node(repo, file_path, "createOrder", 0);
        let import = search_ignored_node(repo, file_path);
        let import_id = import.id;

        RepoBatch {
            repo: repo.to_owned(),
            files: vec![FileBatch {
                repo: repo.to_owned(),
                file_path: file_path.to_owned(),
                path_id_bytes: vec![],
                nodes: vec![file.clone(), function.clone(), import],
                edges: vec![
                    defines_edge(file.id, function.id),
                    defines_edge(file.id, import_id),
                ],
                content_hash: hash.to_vec(),
                size_bytes: 0,
                mtime_ns: 0,
                indexed_at: 1_713_000_000,
                parse_ms: Some(10),
                force: false,
            }],
            test_hooks: RepoBatchHooks::default(),
        }
    }

    fn route_node(name: &str, qualified_name: &str) -> gather_step_core::NodeData {
        gather_step_core::NodeData {
            id: node_id("__virtual__", qualified_name, NodeKind::Route, name),
            kind: NodeKind::Route,
            repo: "__virtual__".to_owned(),
            file_path: qualified_name.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(qualified_name.to_owned()),
            external_id: Some(qualified_name.to_owned()),
            signature: None,
            visibility: Some(Visibility::Public),
            span: None,
            is_virtual: true,
        }
    }

    fn semantic_batch(
        repo: &str,
        file_path: &str,
        symbol_name: &str,
        route: &gather_step_core::NodeData,
        hash: &[u8],
    ) -> RepoBatch {
        let file = file_node(repo, file_path);
        let function = function_node(repo, file_path, symbol_name, 0);
        RepoBatch {
            repo: repo.to_owned(),
            files: vec![FileBatch {
                repo: repo.to_owned(),
                file_path: file_path.to_owned(),
                nodes: vec![file.clone(), function.clone(), route.clone()],
                edges: vec![
                    defines_edge(file.id, function.id),
                    gather_step_core::EdgeData {
                        source: function.id,
                        target: route.id,
                        kind: EdgeKind::Serves,
                        metadata: EdgeMetadata::default(),
                        owner_file: file.id,
                        is_cross_file: true,
                    },
                ],
                content_hash: hash.to_vec(),
                size_bytes: 0,
                mtime_ns: 0,
                indexed_at: 1_713_000_000,
                parse_ms: Some(10),
                path_id_bytes: vec![],
                force: false,
            }],
            test_hooks: RepoBatchHooks::default(),
        }
    }

    fn delete_file_state_row(
        coordinator: &StorageCoordinator,
        repo: &str,
        file_path: &str,
    ) -> rusqlite::Result<usize> {
        let connection = rusqlite::Connection::open(coordinator.metadata().path())?;
        connection.execute(
            "DELETE FROM file_index_state WHERE repo = ?1 AND file_path = ?2",
            params![repo, file_path.as_bytes()],
        )
    }

    #[test]
    fn coordinator_writes_all_three_stores() {
        let root = TestRoot::new("write-all");
        let coordinator = StorageCoordinator::open(root.path()).expect("coordinator should open");
        let batch = batch("service-a", "src/foo.ts", &["createOrder"], &[1, 2, 3]);

        let result = coordinator
            .index_repo_batch(&batch)
            .expect("batch should index");

        assert_eq!(result.files_indexed, 1);
        assert_eq!(
            coordinator
                .graph()
                .nodes_by_file("service-a", "src/foo.ts")
                .expect("graph lookup should succeed")
                .len(),
            2
        );
        assert_eq!(
            coordinator
                .search()
                .search("service-a", 10)
                .expect("search should succeed")
                .len(),
            2
        );
        assert!(
            !coordinator
                .metadata()
                .should_reindex("service-a", "src/foo.ts", &[1, 2, 3])
                .expect("metadata check should succeed")
        );
    }

    #[test]
    fn reindex_replaces_stale_graph_and_search_entries() {
        let root = TestRoot::new("reindex");
        let coordinator = StorageCoordinator::open(root.path()).expect("coordinator should open");

        coordinator
            .index_repo_batch(&batch("service-a", "src/foo.ts", &["oldSymbol"], &[1]))
            .expect("first batch should index");
        coordinator
            .index_repo_batch(&batch("service-a", "src/foo.ts", &["newSymbol"], &[2]))
            .expect("second batch should index");

        let graph_nodes = coordinator
            .graph()
            .nodes_by_file("service-a", "src/foo.ts")
            .expect("graph lookup should succeed");
        let mut graph_names = graph_nodes
            .iter()
            .map(|node| node.name.as_str())
            .collect::<Vec<_>>();
        graph_names.sort_unstable();
        assert_eq!(graph_names, vec!["newSymbol", "src/foo.ts"]);
        assert!(
            coordinator
                .search()
                .search("oldsymbol", 10)
                .expect("search should succeed")
                .is_empty()
        );
        assert_eq!(
            coordinator
                .search()
                .search("newsymbol", 10)
                .expect("search should succeed")
                .len(),
            1
        );
    }

    #[test]
    fn search_only_indexes_query_worthy_node_kinds() {
        let root = TestRoot::new("search-filter");
        let coordinator = StorageCoordinator::open(root.path()).expect("coordinator should open");

        coordinator
            .index_repo_batch(&batch_with_non_searchable_node(
                "service-a",
                "src/foo.ts",
                &[9],
            ))
            .expect("batch should index");

        let results = coordinator
            .search()
            .search("sharedimport", 10)
            .expect("search should succeed");
        assert!(results.is_empty());

        let function_results = coordinator
            .search()
            .search("createorder", 10)
            .expect("search should succeed");
        assert_eq!(function_results.len(), 1);
        assert_eq!(function_results[0].node_kind, NodeKind::Function);
    }

    #[test]
    fn missing_file_index_state_row_triggers_self_healing_reindex() {
        let root = TestRoot::new("self-heal");
        let coordinator = StorageCoordinator::open(root.path()).expect("coordinator should open");
        let batch = batch("service-a", "src/foo.ts", &["createOrder"], &[1, 2, 3]);

        coordinator
            .index_repo_batch(&batch)
            .expect("seed batch should index");
        let original_nodes = coordinator
            .graph()
            .count_nodes()
            .expect("node count should work");
        let original_edges = coordinator
            .graph()
            .count_edges()
            .expect("edge count should work");

        assert_eq!(
            delete_file_state_row(&coordinator, "service-a", "src/foo.ts")
                .expect("sqlite row delete should work"),
            1
        );
        assert!(
            coordinator
                .metadata()
                .should_reindex("service-a", "src/foo.ts", &[1, 2, 3])
                .expect("metadata should treat missing row as dirty")
        );

        let result = coordinator
            .index_repo_batch(&batch)
            .expect("recovery reindex should succeed");

        assert_eq!(result.files_indexed, 1);
        assert_eq!(
            coordinator
                .graph()
                .count_nodes()
                .expect("node count should work"),
            original_nodes
        );
        assert_eq!(
            coordinator
                .graph()
                .count_edges()
                .expect("edge count should work"),
            original_edges
        );
        assert_eq!(
            coordinator
                .graph()
                .nodes_by_file("service-a", "src/foo.ts")
                .expect("graph lookup should succeed")
                .len(),
            2
        );
        assert_eq!(
            coordinator
                .search()
                .search("createorder", 10)
                .expect("search should succeed")
                .len(),
            1
        );
    }

    #[test]
    fn reconcile_search_rebuilds_repo_projection_from_graph() {
        let root = TestRoot::new("reconcile-search");
        let coordinator = StorageCoordinator::open(root.path()).expect("coordinator should open");
        let batch = batch("service-a", "src/foo.ts", &["createOrder"], &[1, 2, 3]);

        coordinator
            .index_repo_batch(&batch)
            .expect("seed batch should index");
        coordinator
            .search()
            .delete_by_repo("service-a")
            .expect("repo delete should succeed");
        assert!(
            coordinator
                .search()
                .search("createorder", 10)
                .expect("search should succeed")
                .is_empty()
        );

        let outcome = coordinator.reconcile_search("service-a");
        assert!(
            matches!(outcome, crate::ReconcileOutcome::Full),
            "search reconciliation should complete fully"
        );

        let results = coordinator
            .search()
            .search("createorder", 10)
            .expect("search should succeed");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].symbol_name, "createOrder");
    }

    #[test]
    fn reindex_invalidates_packs_for_cross_repo_semantic_peers() {
        let root = TestRoot::new("semantic-pack-invalidation");
        let coordinator = StorageCoordinator::open(root.path()).expect("coordinator should open");
        let route = route_node("GET /orders", "route::GET::/orders");

        coordinator
            .index_repo_batch(&semantic_batch(
                "backend_standard",
                "src/controller.ts",
                "handle_order",
                &route,
                &[1],
            ))
            .expect("backend batch should index");
        coordinator
            .index_repo_batch(&semantic_batch(
                "frontend_standard",
                "src/api.ts",
                "call_order_api",
                &route,
                &[2],
            ))
            .expect("frontend batch should index");

        coordinator
            .metadata()
            .put_context_pack(
                &crate::ContextPackRecord {
                    pack_key: "pack:frontend".to_owned(),
                    mode: "planning".to_owned(),
                    target: "call_order_api".to_owned(),
                    generation: 5,
                    response: br#"{"ok":true}"#.to_vec(),
                    created_at: 10,
                    last_read_at: 10,
                    byte_size: 11,
                    hit_count: 0,
                },
                &[("frontend_standard".to_owned(), "src/api.ts".to_owned())],
            )
            .expect("pack should persist");

        coordinator
            .index_repo_batch(&semantic_batch(
                "backend_standard",
                "src/controller.ts",
                "handle_order",
                &route,
                &[9],
            ))
            .expect("backend reindex should succeed");

        assert!(
            coordinator
                .metadata()
                .get_context_pack("pack:frontend")
                .expect("pack lookup should work")
                .is_none()
        );
    }

    #[test]
    fn savepoint_rollback_restores_prebatch_snapshot() {
        let root = TestRoot::new("rollback");
        let coordinator = StorageCoordinator::open(root.path()).expect("coordinator should open");
        coordinator
            .index_repo_batch(&batch("repo-a", "src/a.ts", &["alpha"], &[1]))
            .expect("seed batch should index");
        let before_nodes = coordinator
            .graph()
            .count_nodes()
            .expect("node count should work");
        let before_edges = coordinator
            .graph()
            .count_edges()
            .expect("edge count should work");

        let mut failing = RepoBatch {
            repo: "repo-b".to_owned(),
            files: vec![
                batch("repo-b", "src/one.ts", &["one"], &[2])
                    .files
                    .remove(0),
                batch("repo-b", "src/two.ts", &["two"], &[3])
                    .files
                    .remove(0),
            ],
            test_hooks: RepoBatchHooks {
                fail_after_graph_files: Some(1),
                pause_after_graph_stage: None,
            },
        };

        let error = coordinator
            .index_repo_batch(&failing)
            .expect_err("batch should fail");
        assert!(matches!(
            error,
            StorageCoordinatorError::InjectedFailure { processed: 1 }
        ));
        assert_eq!(
            coordinator
                .graph()
                .count_nodes()
                .expect("node count should work"),
            before_nodes
        );
        assert_eq!(
            coordinator
                .graph()
                .count_edges()
                .expect("edge count should work"),
            before_edges
        );
        failing.files.clear();
    }

    #[test]
    fn cross_repo_failure_does_not_remove_successful_repo() {
        let root = TestRoot::new("cross-repo");
        let coordinator = StorageCoordinator::open(root.path()).expect("coordinator should open");
        coordinator
            .index_repo_batch(&batch("repo-a", "src/a.ts", &["alpha"], &[1]))
            .expect("repo a should index");

        let mut repo_b = batch("repo-b", "src/b.ts", &["beta"], &[2]);
        repo_b.test_hooks.fail_after_graph_files = Some(1);
        let _ = coordinator.index_repo_batch(&repo_b);

        assert_eq!(
            coordinator
                .graph()
                .nodes_by_file("repo-a", "src/a.ts")
                .expect("repo a lookup should work")
                .len(),
            2
        );
        assert!(
            coordinator
                .graph()
                .nodes_by_file("repo-b", "src/b.ts")
                .expect("repo b lookup should work")
                .is_empty()
        );
    }

    #[test]
    fn creates_expected_workspace_files() {
        let root = TestRoot::new("files");
        let coordinator = StorageCoordinator::open(root.path()).expect("coordinator should open");

        assert!(coordinator.root().join("graph.redb").exists());
        assert!(coordinator.root().join("search").exists());
        assert!(coordinator.root().join("metadata.sqlite").exists());
    }
}
