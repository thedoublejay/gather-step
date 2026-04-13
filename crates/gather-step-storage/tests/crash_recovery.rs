//! Crash-recovery integration tests for the `SQLite`+redb ordering fix (Task 4.1)
//! and the `reconcile_search` error propagation fix (Task 4.2).
//!
//! Task 4.1 — reorder `SQLite` delete vs redb commit
//! ─────────────────────────────────────────────────
//! Simulates a crash between the "redb commit" and the "`SQLite` delete"
//! steps in `purge_deleted_files` by using the `purge_deleted_files_crash_after_redb`
//! seam.  After the crash, reopens the store and verifies that:
//!   - redb no longer contains nodes for the deleted file (durable commit succeeded).
//!   - `SQLite` still has a stale row (delete was not yet applied).
//!
//! A reconcile pass then removes the stale `SQLite` row so both stores agree.
//!
//! Task 4.2 — `reconcile_search` must not swallow errors
//! ────────────────────────────────────────────────────
//! Opens a coordinator with a read-only (broken) search store and asserts
//! that `reconcile_search` returns `ReconcileOutcome::Partial`, not `Full`.

use std::{
    env, fs,
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use gather_step_core::{NodeData, NodeKind, SourceSpan, Visibility, node_id};
use gather_step_storage::{
    FileBatch, GraphStore, MetadataStore, ReconcileOutcome, RepoBatch, StorageCoordinator,
};

// ── helpers ──────────────────────────────────────────────────────────────────

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
            "gather-step-crash-recovery-{name}-{}-{nanos}",
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

fn file_node(repo: &str, file_path: &str) -> NodeData {
    NodeData {
        id: node_id(repo, file_path, NodeKind::File, file_path),
        kind: NodeKind::File,
        repo: repo.to_owned(),
        file_path: file_path.to_owned(),
        name: file_path.to_owned(),
        qualified_name: Some(format!("{repo}::{file_path}")),
        external_id: None,
        signature: None,
        visibility: Some(Visibility::Public),
        span: Some(SourceSpan {
            line_start: 1,
            line_len: 9,
            column_start: 0,
            column_len: 0,
        }),
        is_virtual: false,
    }
}

fn single_file_batch(repo: &str, file_path: &str, hash: &[u8]) -> RepoBatch {
    let node = file_node(repo, file_path);
    RepoBatch {
        repo: repo.to_owned(),
        files: vec![FileBatch {
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            path_id_bytes: vec![],
            nodes: vec![node],
            edges: vec![],
            content_hash: hash.to_vec(),
            size_bytes: 0,
            mtime_ns: 0,
            indexed_at: 1_713_000_000,
            parse_ms: None,
            force: false,
        }],
        test_hooks: gather_step_storage::RepoBatchHooks::default(),
    }
}

// ── Task 4.1 ─────────────────────────────────────────────────────────────────

/// A crash between the redb commit and the `SQLite` delete leaves:
///   - redb: file nodes are gone (the commit completed before the crash).
///   - `SQLite`: `file_index_state` row still present (delete did not run).
///
/// After reopening the store and calling `clear_index_metadata_for_files`
/// (the reconcile pass), both stores agree that the file is gone.
#[test]
fn crash_between_redb_commit_and_sqlite_delete_leaves_reconcilable_state() {
    const REPO: &str = "crash-recovery-repo";
    const FILE: &str = "src/doomed.ts";
    const HASH: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];

    let root = TestRoot::new("crash-redb-sqlite");

    // Step 1: index a file so both redb and SQLite have state for it.
    {
        let coordinator = StorageCoordinator::open(root.path()).expect("coordinator should open");
        coordinator
            .index_repo_batch(&single_file_batch(REPO, FILE, HASH))
            .expect("initial batch should index");

        // Verify the file is present in both stores before we crash.
        let nodes = coordinator
            .graph()
            .nodes_by_file(REPO, FILE)
            .expect("graph lookup should succeed");
        assert!(!nodes.is_empty(), "graph should have nodes for the file");
        assert!(
            !coordinator
                .metadata()
                .should_reindex(REPO, FILE, HASH)
                .expect("metadata query should succeed"),
            "SQLite should have a file_index_state row"
        );
    }

    // Step 2: simulate a crash between redb commit and SQLite delete.
    // `purge_deleted_files_crash_after_redb` writes and commits the redb
    // delete, then returns Err before applying the SQLite delete.
    {
        let coordinator = StorageCoordinator::open(root.path()).expect("coordinator should reopen");
        let err = coordinator
            .purge_deleted_files_crash_after_redb(REPO, &[FILE.to_owned()])
            .expect_err("crash injection must return an error");
        assert!(
            matches!(
                err,
                gather_step_storage::StorageCoordinatorError::InjectedFailure { .. }
            ),
            "expected InjectedFailure, got: {err:?}"
        );
    }

    // Step 3: reopen and inspect the inconsistent state.
    //   - redb should have NO nodes (commit happened before the crash).
    //   - SQLite should still have a row (delete was skipped by the crash).
    {
        let coordinator = StorageCoordinator::open(root.path()).expect("coordinator should reopen");
        let nodes = coordinator
            .graph()
            .nodes_by_file(REPO, FILE)
            .expect("graph lookup should succeed after crash");
        assert!(
            nodes.is_empty(),
            "redb must have no nodes for the deleted file after crash \
             (commit completed before crash)"
        );

        // SQLite still has the stale row — should_reindex returns false because
        // the hash in the stale row still matches.
        let still_indexed = !coordinator
            .metadata()
            .should_reindex(REPO, FILE, HASH)
            .expect("metadata query should succeed");
        assert!(
            still_indexed,
            "SQLite must still have a stale file_index_state row after the crash \
             (reconcilable inconsistency)"
        );
    }

    // Step 4: reconcile — purge the stale SQLite row now that redb says the
    // file is gone.  In production this would be done by a startup reconcile
    // pass.  Here we call `clear_index_metadata_for_files` directly.
    {
        let coordinator = StorageCoordinator::open(root.path()).expect("coordinator should reopen");
        coordinator
            .metadata()
            .clear_index_metadata_for_files(REPO, &[FILE.as_bytes().to_vec()])
            .expect("reconcile purge should succeed");

        // After reconcile, both stores agree: the file is gone.
        let nodes = coordinator
            .graph()
            .nodes_by_file(REPO, FILE)
            .expect("graph lookup should succeed after reconcile");
        assert!(
            nodes.is_empty(),
            "graph should still have no nodes after reconcile"
        );
        assert!(
            coordinator
                .metadata()
                .should_reindex(REPO, FILE, HASH)
                .expect("metadata query should succeed after reconcile"),
            "SQLite should have no row after reconcile — file is fully purged"
        );
    }
}

// ── Task 4.2 ─────────────────────────────────────────────────────────────────

/// `reconcile_search` must return `Partial` when the Tantivy search store is
/// broken (read-only), not silently return success.
#[test]
fn reconcile_search_returns_partial_when_search_store_is_broken() {
    const REPO: &str = "broken-search-repo";
    const FILE: &str = "src/service.ts";
    const HASH: &[u8] = &[0x01, 0x02];

    let root = TestRoot::new("broken-search");

    // First, seed data using a normal coordinator.
    {
        let coordinator = StorageCoordinator::open(root.path()).expect("coordinator should open");
        coordinator
            .index_repo_batch(&single_file_batch(REPO, FILE, HASH))
            .expect("seed batch should index");
    }

    // Reopen with a read-only (broken) search store.  Writes to Tantivy will
    // fail with SearchStoreError::ReadOnly, which surfaces as Partial.
    let broken = StorageCoordinator::open_with_broken_search(root.path())
        .expect("broken coordinator should open");

    let outcome = broken.reconcile_search(REPO);
    assert!(
        matches!(outcome, ReconcileOutcome::Partial { .. }),
        "reconcile_search must return Partial when the search store is read-only, \
         not silently succeed; got: {outcome:?}"
    );
}

/// `reconcile_search` returns `Full` when the search store is healthy.
#[test]
fn reconcile_search_returns_full_when_store_is_healthy() {
    const REPO: &str = "healthy-search-repo";
    const FILE: &str = "src/app.ts";
    const HASH: &[u8] = &[0xAA, 0xBB];

    let root = TestRoot::new("healthy-search");
    let coordinator = StorageCoordinator::open(root.path()).expect("coordinator should open");
    coordinator
        .index_repo_batch(&single_file_batch(REPO, FILE, HASH))
        .expect("seed batch should index");

    let outcome = coordinator.reconcile_search(REPO);
    assert!(
        matches!(outcome, ReconcileOutcome::Full),
        "reconcile_search must return Full on a healthy store; got: {outcome:?}"
    );
}
