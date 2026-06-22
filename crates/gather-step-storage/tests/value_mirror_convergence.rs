//! Indexer integration test for value-mirror convergence (v5.1, Task 4).
//!
//! Indexes a two-repo fixture — an authoritative string-literal union in one
//! repo and a frontend string-array mirror of the same value in another — into
//! a shared store, then asserts the persisted graph carries the shared
//! `ValueMirror` node and the `MirrorsValueFrom` / `Defines` edges. This proves
//! the `owner_file` threading satisfies the cross-file `OwnerNotAFile`
//! validation end-to-end.

use std::fs;

use gather_step_core::{EdgeKind, NodeData, NodeKind, value_mirror_qn, virtual_node_id};
use gather_step_storage::{GraphStore, IndexingOptions, RepoIndexer};

fn write_fixture(root: &std::path::Path, relative: &str, contents: &str) {
    let path = root.join(relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("fixture parent should create");
    }
    fs::write(path, contents).expect("fixture should write");
}

#[test]
fn two_repo_value_mirror_converges_in_persisted_graph() {
    let backend_root = tempfile::tempdir().expect("backend tempdir should create");
    let frontend_root = tempfile::tempdir().expect("frontend tempdir should create");
    let storage_root = tempfile::tempdir().expect("storage tempdir should create");

    // Backend: authoritative union type carrying the canonical event value.
    // A multi-member union (not a single-literal alias) so the parser captures
    // the literal members as authoritative value-mirror candidates.
    write_fixture(
        backend_root.path(),
        "src/events.ts",
        r#"
export type EventType =
  | "orders.statusCheck.triggered"
  | "orders.review.completed";
"#,
    );
    // Frontend: a string array mirroring the same value (non-authoritative).
    write_fixture(
        frontend_root.path(),
        "src/triggers.ts",
        r#"
export const TRIGGERS = [
  "orders.statusCheck.triggered",
  "orders.review.completed",
];
"#,
    );

    let indexer =
        RepoIndexer::open(storage_root.path(), IndexingOptions::default()).expect("indexer");
    indexer
        .index_repo("backend", backend_root.path(), None)
        .expect("backend indexing should succeed");
    indexer
        .index_repo("frontend", frontend_root.path(), None)
        .expect("frontend indexing should succeed");

    let graph = indexer.storage().graph();

    // The shared ValueMirror node has a deterministic id keyed on the value qn.
    let qn = value_mirror_qn("orders.statusCheck.triggered");
    let value_node_id = virtual_node_id(NodeKind::ValueMirror, &qn);

    let incoming = graph
        .get_incoming(value_node_id)
        .expect("incoming edges to the value node should load");

    assert!(
        incoming.iter().any(|edge| edge.kind == EdgeKind::Defines),
        "backend union should Define the shared value node, got: {:?}",
        incoming.iter().map(|e| e.kind).collect::<Vec<_>>()
    );
    assert!(
        incoming
            .iter()
            .any(|edge| edge.kind == EdgeKind::MirrorsValueFrom),
        "frontend array should MirrorValueFrom the shared value node, got: {:?}",
        incoming.iter().map(|e| e.kind).collect::<Vec<_>>()
    );

    // Sanity: the value node itself is persisted as a ValueMirror virtual node.
    let nodes: Vec<NodeData> = graph
        .nodes_by_repo(gather_step_core::VIRTUAL_NODE_REPO)
        .expect("virtual nodes should load");
    assert!(
        nodes
            .iter()
            .any(|n| n.id == value_node_id && n.kind == NodeKind::ValueMirror),
        "the shared ValueMirror node should be persisted"
    );
}
