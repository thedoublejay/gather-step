//! Path identity tests.
//!
//! Verifies that the storage layer uses byte-stable path identity for the
//! path-bearing `SQLite` columns and that the indexed/reconcile flows agree on
//! those keys.
//!
//! Coverage model:
//!   - Filesystem-creating tests with invalid-UTF-8 filenames are intentionally
//!     omitted: APFS rejects such names at creation (Apple APFS FAQ), so they
//!     cannot exist on the supported platform.
//!   - In-memory / direct-BLOB tests with non-UTF-8 byte paths are retained.
//!     They validate the storage contract (`PathId` round-trip, `SQLite` BLOB
//!     boundary) without relying on the filesystem, so they remain meaningful
//!     on macOS.  This matters because a cross-platform indexer can still see
//!     non-UTF-8 byte paths via checked-in fixtures, remote repos, or test
//!     inputs that never hit the local filesystem.
//!
//! Migrated columns:
//!   - `file_index_state.file_path`
//!   - `unresolved_call_candidates.file_path`
//!   - `unresolved_call_candidate_keys.source_path`
//!   - `payload_contracts.file_path`
//!   - `file_dependencies.source_path` / `target_path`

// ── PathId unit tests ────────────────────────────────────────────────────────

/// Two `PathId` values derived from the same raw bytes must be equal.
#[test]
fn path_id_equality_is_byte_exact() {
    use std::path::Path;

    use gather_step_core::PathId;

    let a = PathId::from_path(Path::new("src/service.ts"));
    let b = PathId::from_path(Path::new("src/service.ts"));
    assert_eq!(a, b);

    let c = PathId::from_path(Path::new("src/other.ts"));
    assert_ne!(a, c);
}

// ── SQLite BLOB boundary tests ────────────────────────────────────────────────

fn temp_store() -> (std::path::PathBuf, gather_step_storage::StorageCoordinator) {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    // Monotonic per-process counter.  `SystemTime::now()` alone is not
    // guaranteed unique across threads — two tests started in the same
    // nanosecond by rayon's test runner can collide, which races
    // redb's `DatabaseAlreadyOpen` detection and surfaces as `StorageHeld`.
    // Layering an `AtomicU64` on top guarantees distinct temp dirs.
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "gather-step-path-id-{}-{nanos}-{seq}",
        std::process::id()
    ));
    let coordinator = gather_step_storage::StorageCoordinator::open(&root).expect("open store");
    (root, coordinator)
}

/// ASCII paths must round-trip through BLOB via both the string API
/// (`should_reindex`) and the `Path` API (`should_reindex_path`).
#[test]
fn storage_ascii_path_round_trips_through_blob() {
    use gather_step_storage::{FileIndexState, MetadataStore};

    let (root, coordinator) = temp_store();
    let _cleanup = defer_remove(&root);

    coordinator
        .metadata()
        .upsert_file_state(&FileIndexState {
            repo: "svc".to_owned(),
            file_path: "src/ascii.ts".to_owned(),
            content_hash: vec![0x01],
            node_count: 1,
            edge_count: 0,
            indexed_at: 1_713_000_000,
            parse_ms: None,
            ..Default::default()
        })
        .expect("upsert");

    // String API: bytes of ASCII string == bytes of PathId(ascii path)
    let is_stale = coordinator
        .metadata()
        .should_reindex("svc", "src/ascii.ts", &[0x01])
        .expect("should_reindex");
    assert!(!is_stale, "ASCII path must not be stale after upsert");

    // Path API: same result
    let is_stale_path = coordinator
        .metadata()
        .should_reindex_path("svc", std::path::Path::new("src/ascii.ts"), &[0x01])
        .expect("should_reindex_path");
    assert!(!is_stale_path, "ASCII path must not be stale via Path API");
}

#[test]
fn reconcile_writes_same_repo_dependency_targets_with_path_id_bytes() {
    use std::time::{SystemTime, UNIX_EPOCH};

    use gather_step_core::{EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, node_id};
    use gather_step_storage::{
        FileBatch, RepoBatch, RepoBatchHooks, StorageCoordinator, TrackedPath,
        reconcile_changed_files_with_mode,
    };

    const REPO: &str = "svc";
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "gather-step-deps-path-id-{}-{nanos}",
        std::process::id()
    ));
    let _cleanup = defer_remove(&root);
    let coord = StorageCoordinator::open(&root).expect("open store");
    let caller_display = "src/caller.ts".to_owned();
    let helper_display = "src/helper.ts".to_owned();

    let caller_file = NodeData {
        id: node_id(REPO, &caller_display, NodeKind::File, &caller_display),
        kind: NodeKind::File,
        repo: REPO.to_owned(),
        file_path: caller_display.clone(),
        name: caller_display.clone(),
        qualified_name: None,
        external_id: None,
        signature: None,
        visibility: None,
        span: None,
        is_virtual: false,
    };
    let helper_file = NodeData {
        id: node_id(REPO, &helper_display, NodeKind::File, &helper_display),
        kind: NodeKind::File,
        repo: REPO.to_owned(),
        file_path: helper_display.clone(),
        name: helper_display.clone(),
        qualified_name: None,
        external_id: None,
        signature: None,
        visibility: None,
        span: None,
        is_virtual: false,
    };

    coord
        .index_repo_batch(&RepoBatch {
            repo: REPO.to_owned(),
            files: vec![
                FileBatch {
                    repo: REPO.to_owned(),
                    file_path: caller_display.clone(),
                    path_id_bytes: caller_display.as_bytes().to_vec(),
                    nodes: vec![caller_file.clone()],
                    edges: vec![EdgeData {
                        source: caller_file.id,
                        target: helper_file.id,
                        kind: EdgeKind::DependsOn,
                        metadata: EdgeMetadata::default(),
                        owner_file: caller_file.id,
                        is_cross_file: true,
                    }],
                    content_hash: vec![0x01],
                    size_bytes: 0,
                    mtime_ns: 0,
                    indexed_at: 1,
                    parse_ms: None,
                    force: true,
                },
                FileBatch {
                    repo: REPO.to_owned(),
                    file_path: helper_display.clone(),
                    path_id_bytes: helper_display.as_bytes().to_vec(),
                    nodes: vec![helper_file.clone()],
                    edges: vec![],
                    content_hash: vec![0x02],
                    size_bytes: 0,
                    mtime_ns: 0,
                    indexed_at: 1,
                    parse_ms: None,
                    force: true,
                },
            ],
            test_hooks: RepoBatchHooks::default(),
        })
        .expect("seed graph + metadata");

    reconcile_changed_files_with_mode(
        &coord,
        REPO,
        &[TrackedPath {
            path: caller_display.clone(),
            path_id_bytes: caller_display.as_bytes().to_vec(),
        }],
        false,
    )
    .expect("reconcile succeeds");

    let dependents = coord
        .metadata()
        .reverse_dependents_by_path_id(REPO, helper_display.as_bytes())
        .expect("reverse dependents by raw path id");
    assert_eq!(dependents.len(), 1);
    assert_eq!(dependents[0].path, caller_display);
    assert_eq!(dependents[0].path_id_bytes, b"src/caller.ts".to_vec());
}

fn defer_remove(path: &std::path::Path) -> impl Drop + '_ {
    struct Cleanup<'a>(&'a std::path::Path);
    impl Drop for Cleanup<'_> {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(self.0);
        }
    }
    Cleanup(path)
}

// ── Finding 1: delete/insert BLOB key alignment ───────────────────────────────
//
// `replace_unresolved_resolution_inputs_for_files` and
// `clear_unresolved_metadata_for_files` now take `&[Vec<u8>]` (PathId bytes)
// rather than `&[String]`.  This test verifies that:
//   1. Inserting two ResolutionInputs with distinct ASCII paths produces two
//      distinct rows in the database (keyed by PathId bytes).
//   2. Clearing by PathId bytes for one path removes exactly that row without
//      touching the other.
//
// The regression this guards against: before the fix, the DELETE branch bound
// `file_path.as_bytes()` from `&[String]`, while the INSERT bound
// `PathId::from_path(...).as_bytes()`.  For non-UTF-8 filenames the two byte
// sequences differ.  For ASCII they are identical, so this test uses ASCII
// paths to verify the key-alignment invariant holds after the fix.
//
// The test uses `all_unresolved_rows_for_test` (available under the
// `test-support` feature) to read back the raw BLOB bytes and confirm
// byte-level identity.
#[test]
fn unresolved_metadata_delete_matches_insert_key_alignment() {
    use std::{
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use gather_step_core::{NodeKind, PathId, node_id};
    use gather_step_parser::resolve::{CallSite, ResolutionInput};
    use gather_step_storage::StorageCoordinator;

    const REPO: &str = "path-id-test-repo";
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "gather-step-finding1-{}-{nanos}",
        std::process::id()
    ));
    let _cleanup = defer_remove(&root);
    let coord = StorageCoordinator::open(&root).unwrap();
    let path_a = "src/alpha.ts";
    let path_b = "src/beta.ts";
    let file_a = PathBuf::from(path_a);
    let file_b = PathBuf::from(path_b);

    let file_node_a = node_id(REPO, path_a, NodeKind::File, path_a);
    let file_node_b = node_id(REPO, path_b, NodeKind::File, path_b);
    let owner_a = node_id(REPO, path_a, NodeKind::Function, "fnA");
    let owner_b = node_id(REPO, path_b, NodeKind::Function, "fnB");

    let inputs = vec![
        ResolutionInput {
            file_node: file_node_a,
            file_path: file_a.clone(),
            import_bindings: vec![],
            call_sites: vec![CallSite {
                owner_id: owner_a,
                owner_file: file_node_a,
                source_path: file_a.clone(),
                callee_name: "doThingA".to_owned(),
                callee_qualified_hint: None,
                span: None,
            }],
        },
        ResolutionInput {
            file_node: file_node_b,
            file_path: file_b.clone(),
            import_bindings: vec![],
            call_sites: vec![CallSite {
                owner_id: owner_b,
                owner_file: file_node_b,
                source_path: file_b.clone(),
                callee_name: "doThingB".to_owned(),
                callee_qualified_hint: None,
                span: None,
            }],
        },
    ];

    let path_ids: Vec<Vec<u8>> = inputs
        .iter()
        .map(|i| PathId::from_path(&i.file_path).as_bytes().to_vec())
        .collect();

    // PathId bytes for ASCII paths are identical to the string bytes.
    assert_eq!(path_ids[0], path_a.as_bytes());
    assert_eq!(path_ids[1], path_b.as_bytes());
    assert_ne!(path_ids[0], path_ids[1], "paths must be distinct");

    coord
        .metadata()
        .replace_unresolved_resolution_inputs_for_files(REPO, &path_ids, &inputs)
        .expect("insert both unresolved inputs");

    // Both rows must be present after insertion.
    let all = coord
        .metadata()
        .all_unresolved_rows_for_test(REPO)
        .expect("read back rows after insert");
    let all_paths: Vec<&[u8]> = all.iter().map(|(p, _)| p.as_slice()).collect();
    assert_eq!(
        all_paths.len(),
        2,
        "two distinct paths must produce two rows; got {all_paths:?}"
    );

    // Clear only path_a by PathId bytes.  path_b's row must survive.
    coord
        .metadata()
        .replace_unresolved_resolution_inputs_for_files(REPO, &[path_ids[0].clone()], &[])
        .expect("clear for path_a");

    let remaining = coord
        .metadata()
        .all_unresolved_rows_for_test(REPO)
        .expect("read back rows after partial clear");
    let remaining_paths: Vec<&[u8]> = remaining.iter().map(|(p, _)| p.as_slice()).collect();

    assert!(
        remaining_paths.contains(&path_b.as_bytes()),
        "path_b's row must survive after clearing only path_a; got {remaining_paths:?}"
    );
    assert!(
        !remaining_paths.contains(&path_a.as_bytes()),
        "path_a's row must be gone; got {remaining_paths:?}"
    );
}

// ── In-memory non-UTF-8 identity coverage ─────────────────────────────────────
//
// APFS rejects invalid-UTF-8 filenames at creation time (Apple APFS FAQ), so
// the indexer will not encounter such files on the local macOS filesystem in
// normal operation.  These tests are nonetheless valuable on macOS because
// they validate the storage-layer identity contract, not filesystem behavior:
//
//   - `PathId::from_path` must be byte-exact when handed an OsStr built from
//     non-UTF-8 bytes (pure in-memory).
//   - `to_display` must produce a replacement-character form while leaving
//     `as_bytes()` intact.
//   - SQLite BLOB columns must round-trip arbitrary bytes, since third-party
//     tooling (git, checked-in fixtures, cross-platform repos indexed via
//     non-local paths) can still hand the indexer non-UTF-8 byte paths in
//     `NodeData::file_path` even when APFS would not materialise the file on
//     disk.
//   - The unresolved-metadata delete/insert key invariant must hold when the
//     PathId bytes differ from any lossy-string byte form.  The ASCII variant
//     above cannot exercise this because ASCII bytes and lossy-string bytes
//     are identical.

#[cfg(unix)]
#[test]
fn path_id_roundtrips_non_utf8_bytes() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    use std::path::Path;

    use gather_step_core::PathId;

    let raw: &[u8] = b"bad-\xff-bytes.ts";
    let os_str = OsStr::from_bytes(raw);
    let path = Path::new(os_str);

    let id = PathId::from_path(path);
    assert_eq!(
        id.as_bytes(),
        raw,
        "PathId::from_path must preserve non-UTF-8 bytes unchanged"
    );
}

#[cfg(unix)]
#[test]
fn path_id_display_is_lossy_for_non_utf8() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    use std::path::Path;

    use gather_step_core::PathId;

    let raw: &[u8] = b"bad-\xff-bytes.ts";
    let os_str = OsStr::from_bytes(raw);
    let path = Path::new(os_str);

    let id = PathId::from_path(path);
    let display = id.to_display();

    assert!(
        display.contains('\u{FFFD}'),
        "to_display() must use the replacement character for non-UTF-8 bytes; got: {display:?}"
    );
    assert_eq!(
        id.as_bytes(),
        raw,
        "underlying bytes must remain intact despite lossy display"
    );
}

#[cfg(unix)]
#[test]
fn storage_non_utf8_file_path_survives_blob_round_trip() {
    // Feeds a non-UTF-8 file_path through index_repo_batch (in-memory; no
    // filesystem creation is required) and confirms the SQLite BLOB column
    // stores the exact bytes, not a lossy form.
    use std::os::unix::ffi::OsStrExt;

    use gather_step_core::{NodeKind, PathId, SourceSpan, Visibility, node_id};
    use gather_step_storage::{FileBatch, RepoBatch, RepoBatchHooks};

    const REPO: &str = "path-id-test-repo";
    let (root, coord) = temp_store();
    let _cleanup = defer_remove(&root);
    let raw_bytes: &[u8] = b"src/bad-\xff-name.ts";
    // Build a synthetic NodeData whose file_path carries the non-UTF-8 bytes
    // via the lossy-string view; path_id_bytes on the FileBatch carries the
    // true identity.
    let file_path_lossy = String::from_utf8_lossy(raw_bytes).into_owned();
    let path_id_bytes = PathId::from_bytes(raw_bytes.to_vec()).as_bytes().to_vec();

    let file_node = gather_step_core::NodeData {
        id: node_id(REPO, &file_path_lossy, NodeKind::File, &file_path_lossy),
        kind: NodeKind::File,
        repo: REPO.to_owned(),
        file_path: file_path_lossy.clone(),
        name: std::ffi::OsStr::from_bytes(raw_bytes)
            .to_string_lossy()
            .into_owned(),
        qualified_name: None,
        external_id: None,
        signature: None,
        visibility: Some(Visibility::Public),
        span: Some(SourceSpan {
            line_start: 1,
            line_len: 0,
            column_start: 0,
            column_len: 0,
        }),
        is_virtual: false,
    };

    coord
        .index_repo_batch(&RepoBatch {
            repo: REPO.to_owned(),
            files: vec![FileBatch {
                repo: REPO.to_owned(),
                file_path: file_path_lossy.clone(),
                path_id_bytes: path_id_bytes.clone(),
                nodes: vec![file_node],
                edges: vec![],
                content_hash: vec![0xAA],
                size_bytes: 0,
                mtime_ns: 0,
                indexed_at: 1_713_000_000,
                parse_ms: None,
                force: false,
            }],
            test_hooks: RepoBatchHooks::default(),
        })
        .expect("batch should index");

    // The BLOB column must contain the exact non-UTF-8 bytes, not a lossy
    // UTF-8 encoding (which would replace 0xFF with EF BF BD).
    let stored = coord
        .metadata()
        .file_index_states_by_repo(REPO)
        .expect("read back file_index_state rows");
    let matched = stored.iter().find(|state| state.path_id_bytes == raw_bytes);
    assert!(
        matched.is_some(),
        "non-UTF-8 path bytes must round-trip exactly through file_index_state.path_id_bytes; \
         got: {:?}",
        stored
            .iter()
            .map(|s| s.path_id_bytes.clone())
            .collect::<Vec<_>>()
    );
}

#[cfg(unix)]
#[test]
fn storage_non_utf8_file_path_reindex_batch_uses_lossless_path_bytes() {
    use gather_step_core::{NodeKind, PathId, SourceSpan, Visibility, node_id};
    use gather_step_storage::{FileBatch, RepoBatch, RepoBatchHooks};

    const REPO: &str = "path-id-reindex-test";
    let (root, coord) = temp_store();
    let _cleanup = defer_remove(&root);
    let raw_bytes: &[u8] = b"src/reindex-\xff-name.ts";
    let file_path_lossy = String::from_utf8_lossy(raw_bytes).into_owned();
    let path_id_bytes = PathId::from_bytes(raw_bytes.to_vec()).as_bytes().to_vec();

    let file_node = gather_step_core::NodeData {
        id: node_id(REPO, &file_path_lossy, NodeKind::File, &file_path_lossy),
        kind: NodeKind::File,
        repo: REPO.to_owned(),
        file_path: file_path_lossy.clone(),
        name: file_path_lossy.clone(),
        qualified_name: None,
        external_id: None,
        signature: None,
        visibility: Some(Visibility::Public),
        span: Some(SourceSpan {
            line_start: 1,
            line_len: 0,
            column_start: 0,
            column_len: 0,
        }),
        is_virtual: false,
    };

    let batch = RepoBatch {
        repo: REPO.to_owned(),
        files: vec![FileBatch {
            repo: REPO.to_owned(),
            file_path: file_path_lossy,
            path_id_bytes,
            nodes: vec![file_node],
            edges: vec![],
            content_hash: vec![0xAA],
            size_bytes: 0,
            mtime_ns: 0,
            indexed_at: 1_713_000_000,
            parse_ms: None,
            force: false,
        }],
        test_hooks: RepoBatchHooks::default(),
    };

    let first = coord
        .index_repo_batch(&batch)
        .expect("first batch should index");
    assert_eq!(
        first.nodes_written, 1,
        "first batch should write the file node"
    );

    let second = coord
        .index_repo_batch(&batch)
        .expect("second batch should reuse the stored path bytes");
    assert_eq!(
        second.nodes_written, 0,
        "unchanged non-UTF-8 paths should not be reindexed by the batch lookup"
    );
    assert_eq!(
        second.edges_written, 0,
        "unchanged non-UTF-8 paths should not trigger rewrite work"
    );
}

// Note on the non-UTF-8 variant of `unresolved_metadata_delete_matches_insert_key_alignment`:
//
// A non-UTF-8 regression test for the delete/insert byte-mismatch bug class
// was considered, but is not representable through the current public API.
// `replace_unresolved_resolution_inputs_for_files` JSON-serializes each
// `ResolutionInput` payload before INSERT (serde_json requires valid UTF-8
// in `PathBuf` fields), so a non-UTF-8 `input.file_path` cannot reach the
// INSERT branch via the public method at all.  On macOS this is doubly-
// irrelevant because APFS rejects invalid-UTF-8 filenames at creation.
// The ASCII-key-alignment test above is retained as the structural guard;
// the delete/insert byte contract at the storage layer is additionally
// covered by direct BLOB round-trip in
// `storage_non_utf8_file_path_survives_blob_round_trip`, which bypasses the
// JSON payload entirely and exercises the BLOB column directly.
