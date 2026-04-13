// Probe: canonical event identity invariant.
//
// Verifies that a producer and a consumer for the same Kafka event
// resolve to a single shared virtual `NodeKind::Event` node whose
// `qualified_name` follows the `__event__kafka__<name>` convention,
// and that no separate `NodeKind::Topic` sibling node is required
// for the join to succeed.
//
// Test name: `producer_and_consumer_share_canonical_event_identity_for_kafka_event_type`
//
// Result: determined at runtime — see the assertion messages for the verdict.

use std::{
    env,
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use gather_step_core::{
    EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, Visibility, node_id, ref_node_id,
};
use gather_step_storage::{FileBatch, GraphStore, GraphStoreDb, RepoBatch, RepoBatchHooks};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDb {
    path: PathBuf,
}

impl TempDb {
    fn new(name: &str) -> Self {
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = env::temp_dir().join(format!(
            "gather-step-event-identity-{name}-{}-{nanos}-{seq}.redb",
            process::id()
        ));
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn open_store(name: &str) -> (GraphStoreDb, TempDb) {
    let db = TempDb::new(name);
    let store = GraphStoreDb::open(db.path()).expect("store should open");
    (store, db)
}

/// File node for a real (non-virtual) source file.
fn file_node(repo: &str, path: &str) -> NodeData {
    NodeData {
        id: node_id(repo, path, NodeKind::File, path),
        kind: NodeKind::File,
        repo: repo.to_owned(),
        file_path: path.to_owned(),
        name: path.to_owned(),
        qualified_name: Some(format!("{repo}::{path}")),
        external_id: None,
        signature: None,
        visibility: Some(Visibility::Public),
        span: None,
        is_virtual: false,
    }
}

/// Symbolic function node in a real file.
fn sym_node(repo: &str, path: &str, name: &str) -> NodeData {
    NodeData {
        id: node_id(repo, path, NodeKind::Function, name),
        kind: NodeKind::Function,
        repo: repo.to_owned(),
        file_path: path.to_owned(),
        name: name.to_owned(),
        qualified_name: Some(format!("{repo}::{name}")),
        external_id: None,
        signature: None,
        visibility: Some(Visibility::Public),
        span: None,
        is_virtual: false,
    }
}

/// Build the canonical virtual `NodeKind::Event` node.
///
/// The `id` is computed via `ref_node_id` so it is stable regardless of which
/// repo creates the node — exactly matching the parser's behaviour.
fn canonical_event_node(event_qn: &str) -> NodeData {
    NodeData {
        id: ref_node_id(NodeKind::Event, event_qn),
        kind: NodeKind::Event,
        // Virtual nodes live in the shared virtual repo.
        repo: gather_step_core::VIRTUAL_NODE_REPO.to_owned(),
        file_path: String::new(),
        name: event_qn
            .strip_prefix("__event__kafka__")
            .unwrap_or(event_qn)
            .to_owned(),
        qualified_name: Some(event_qn.to_owned()),
        external_id: Some(event_qn.to_owned()),
        signature: None,
        visibility: None,
        span: None,
        is_virtual: true,
    }
}

fn make_edge(
    source: gather_step_core::NodeId,
    target: gather_step_core::NodeId,
    kind: EdgeKind,
    owner: gather_step_core::NodeId,
) -> EdgeData {
    EdgeData {
        source,
        target,
        kind,
        metadata: EdgeMetadata::default(),
        owner_file: owner,
        is_cross_file: true,
    }
}

// ---------------------------------------------------------------------------
// Probe 2.1
// ---------------------------------------------------------------------------

/// Verify that producer and consumer for the same Kafka event type converge on
/// a single canonical `NodeKind::Event` virtual node and that no `NodeKind::Topic`
/// sibling node is needed to bridge them.
///
/// Graph shape:
///
/// ```text
/// producer_repo::sendOrder  --ProducesEventFor-->  __event__kafka__OrderCreated (virtual)
/// consumer_repo::handleOrder --UsesEventFrom-->    __event__kafka__OrderCreated (virtual)
/// ```
///
/// Assertions:
/// - Exactly one `NodeKind::Event` node with the canonical qualified name exists.
/// - The producer node has an outgoing `ProducesEventFor` edge to that node.
/// - The consumer node has an outgoing `UsesEventFrom` edge to that node.
/// - No `NodeKind::Topic` nodes exist (legacy sibling fallback is absent).
#[test]
fn producer_and_consumer_share_canonical_event_identity_for_kafka_event_type() {
    let event_qn = "__event__kafka__OrderCreated";

    // Build the single shared virtual event node.
    let event_node = canonical_event_node(event_qn);
    let event_id = event_node.id;

    // Producer side.
    let producer_file = file_node("producer_repo", "src/order_service.ts");
    let producer_sym = sym_node("producer_repo", "src/order_service.ts", "sendOrder");

    // Consumer side.
    let consumer_file = file_node("consumer_repo", "src/order_handler.ts");
    let consumer_sym = sym_node("consumer_repo", "src/order_handler.ts", "handleOrder");

    let (store, _db) = open_store("event-identity");

    store
        .bulk_insert(
            &[
                event_node.clone(),
                producer_file.clone(),
                producer_sym.clone(),
                consumer_file.clone(),
                consumer_sym.clone(),
            ],
            &[
                make_edge(
                    producer_sym.id,
                    event_id,
                    EdgeKind::ProducesEventFor,
                    producer_file.id,
                ),
                make_edge(
                    consumer_sym.id,
                    event_id,
                    EdgeKind::UsesEventFrom,
                    consumer_file.id,
                ),
            ],
        )
        .expect("bulk insert must succeed");

    // ── Assert 1: exactly one NodeKind::Event node with the canonical QN ──────
    let event_nodes = store
        .nodes_by_type(NodeKind::Event)
        .expect("nodes_by_type must succeed");
    let canonical_events: Vec<_> = event_nodes
        .iter()
        .filter(|n| n.qualified_name.as_deref() == Some(event_qn))
        .collect();
    assert_eq!(
        canonical_events.len(),
        1,
        "exactly one canonical Event node must exist with QN `{event_qn}`; \
         found {}: {canonical_events:#?}",
        canonical_events.len()
    );

    // ── Assert 2: producer has an outgoing ProducesEventFor edge to that node ─
    let producer_outgoing = store
        .get_outgoing(producer_sym.id)
        .expect("outgoing edges must be readable");
    let produces_edge = producer_outgoing
        .iter()
        .find(|e| e.kind == EdgeKind::ProducesEventFor && e.target == event_id);
    assert!(
        produces_edge.is_some(),
        "producer must have a ProducesEventFor edge to the canonical event node; \
         outgoing: {producer_outgoing:#?}"
    );

    // ── Assert 3: consumer has an outgoing UsesEventFrom edge to that node ────
    let consumer_outgoing = store
        .get_outgoing(consumer_sym.id)
        .expect("outgoing edges must be readable");
    let uses_edge = consumer_outgoing
        .iter()
        .find(|e| e.kind == EdgeKind::UsesEventFrom && e.target == event_id);
    assert!(
        uses_edge.is_some(),
        "consumer must have a UsesEventFrom edge to the canonical event node; \
         outgoing: {consumer_outgoing:#?}"
    );

    // ── Assert 4: no NodeKind::Topic sibling node exists ──────────────────────
    let topic_nodes = store
        .nodes_by_type(NodeKind::Topic)
        .expect("nodes_by_type must succeed");
    assert!(
        topic_nodes.is_empty(),
        "no NodeKind::Topic sibling node must exist — the canonical Event node \
         is the sole bridge for this event; topics found: {topic_nodes:#?}"
    );

    // ── Assert 5: both edges target the same node id ──────────────────────────
    let retrieved = store
        .get_node(event_id)
        .expect("get_node must succeed")
        .expect("canonical event node must be retrievable");
    assert_eq!(
        retrieved.id, event_id,
        "retrieved event node id must match the inserted id"
    );
    assert_eq!(
        retrieved.qualified_name.as_deref(),
        Some(event_qn),
        "retrieved event node must have the canonical qualified name"
    );
    assert!(
        retrieved.is_virtual,
        "canonical event node must be marked virtual"
    );
}

// ---------------------------------------------------------------------------
// Supplementary: bulk_insert + index_repo_batch path
//
// Verifies the same invariant when nodes and edges are written through the
// standard `index_repo_batch` + `FileBatch` pathway used by the real indexer.
// This confirms the storage layer persists and indexes virtual Event nodes
// correctly when they arrive via the normal indexing pipeline.
// ---------------------------------------------------------------------------

#[test]
fn canonical_event_node_survives_index_repo_batch_round_trip() {
    use std::time::{SystemTime, UNIX_EPOCH};

    use gather_step_storage::StorageCoordinator;

    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let root = env::temp_dir().join(format!(
        "gather-step-event-identity-coord-{}-{nanos}-{seq}",
        process::id()
    ));
    let cleanup = DeferRemove(&root);
    let coord = StorageCoordinator::open(&root).expect("coordinator must open");

    let event_qn = "__event__kafka__OrderCreated";
    let event_node = canonical_event_node(event_qn);
    let event_id = event_node.id;

    let producer_file_path = "src/order_service.ts".to_owned();
    let producer_file = file_node("producer_repo", &producer_file_path);
    let producer_sym = sym_node("producer_repo", &producer_file_path, "sendOrder");

    let consumer_file_path = "src/order_handler.ts".to_owned();
    let consumer_file = file_node("consumer_repo", &consumer_file_path);
    let consumer_sym = sym_node("consumer_repo", &consumer_file_path, "handleOrder");

    // Index producer repo.
    coord
        .index_repo_batch(&RepoBatch {
            repo: "producer_repo".to_owned(),
            files: vec![FileBatch {
                repo: "producer_repo".to_owned(),
                file_path: producer_file_path.clone(),
                path_id_bytes: producer_file_path.as_bytes().to_vec(),
                nodes: vec![
                    producer_file.clone(),
                    producer_sym.clone(),
                    event_node.clone(),
                ],
                edges: vec![make_edge(
                    producer_sym.id,
                    event_id,
                    EdgeKind::ProducesEventFor,
                    producer_file.id,
                )],
                content_hash: vec![0x01],
                size_bytes: 0,
                mtime_ns: 0,
                indexed_at: 1_713_000_000,
                parse_ms: None,
                force: true,
            }],
            test_hooks: RepoBatchHooks::default(),
        })
        .expect("producer batch must index");

    // Index consumer repo.
    coord
        .index_repo_batch(&RepoBatch {
            repo: "consumer_repo".to_owned(),
            files: vec![FileBatch {
                repo: "consumer_repo".to_owned(),
                file_path: consumer_file_path.clone(),
                path_id_bytes: consumer_file_path.as_bytes().to_vec(),
                nodes: vec![
                    consumer_file.clone(),
                    consumer_sym.clone(),
                    event_node.clone(),
                ],
                edges: vec![make_edge(
                    consumer_sym.id,
                    event_id,
                    EdgeKind::UsesEventFrom,
                    consumer_file.id,
                )],
                content_hash: vec![0x02],
                size_bytes: 0,
                mtime_ns: 0,
                indexed_at: 1_713_000_001,
                parse_ms: None,
                force: true,
            }],
            test_hooks: RepoBatchHooks::default(),
        })
        .expect("consumer batch must index");

    let graph = coord.graph();

    // The canonical event node must be retrievable by its stable id.
    let retrieved = graph
        .get_node(event_id)
        .expect("get_node must succeed")
        .expect("canonical event node must persist after both batches");

    assert_eq!(
        retrieved.qualified_name.as_deref(),
        Some(event_qn),
        "qualified name must survive round-trip"
    );
    assert!(retrieved.is_virtual, "virtual flag must survive round-trip");

    // Producer outgoing edge must be readable.
    let producer_out = graph
        .get_outgoing(producer_sym.id)
        .expect("outgoing edges must read");
    assert!(
        producer_out
            .iter()
            .any(|e| e.kind == EdgeKind::ProducesEventFor && e.target == event_id),
        "ProducesEventFor edge must persist after producer batch; edges: {producer_out:#?}"
    );

    // Consumer outgoing edge must be readable.
    let consumer_out = graph
        .get_outgoing(consumer_sym.id)
        .expect("outgoing edges must read");
    assert!(
        consumer_out
            .iter()
            .any(|e| e.kind == EdgeKind::UsesEventFrom && e.target == event_id),
        "UsesEventFrom edge must persist after consumer batch; edges: {consumer_out:#?}"
    );

    // No Topic sibling node should exist.
    let topics = graph
        .nodes_by_type(NodeKind::Topic)
        .expect("nodes_by_type must succeed");
    assert!(
        topics.is_empty(),
        "no Topic sibling node must appear after canonical Event indexing; topics: {topics:#?}"
    );

    drop(cleanup);
}

// ---------------------------------------------------------------------------

struct DeferRemove<'a>(&'a std::path::Path);
impl Drop for DeferRemove<'_> {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(self.0);
    }
}
