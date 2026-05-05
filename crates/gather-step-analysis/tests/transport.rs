/// Integration tests for transport-boundary linking.
///
/// These tests exercise `transport_links_for` via the public crate API and
/// verify that route and queue boundary links are correctly derived from the
/// graph without being persisted.
use gather_step_analysis::transport::{Confidence, transport_links_for};
use gather_step_core::{
    EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, SourceSpan, Visibility, node_id,
    queue_qn, route_qn, virtual_node,
};
use gather_step_storage::{GraphStore, GraphStoreDb};
use std::sync::atomic::{AtomicU64, Ordering};
use std::{env, fs, path::PathBuf, process};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDb {
    path: PathBuf,
}

impl TempDb {
    fn new(name: &str) -> Self {
        let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!(
            "gather-step-transport-test-{name}-{}-{id}.redb",
            process::id()
        ));
        Self { path }
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
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
        visibility: None,
        span: None,
        is_virtual: false,
    }
}

fn symbol_node(repo: &str, file_path: &str, name: &str, ordinal: u16) -> NodeData {
    NodeData {
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
            line_start: u32::from(ordinal) + 1,
            line_len: 0,
            column_start: 0,
            column_len: 4,
        }),
        is_virtual: false,
    }
}

#[test]
fn route_link_resolves_frontend_to_backend_via_virtual_node() {
    let temp = TempDb::new("route");
    let store = GraphStoreDb::open(&temp.path).expect("store should open");

    let fe_file = file_node("frontend_standard", "src/hooks/useAuth.ts");
    let be_file = file_node("identity_service", "src/auth/auth.controller.ts");
    let frontend = symbol_node(
        "frontend_standard",
        "src/hooks/useAuth.ts",
        "useAuthentication",
        0,
    );
    let backend = symbol_node(
        "identity_service",
        "src/auth/auth.controller.ts",
        "renewAuthSession",
        0,
    );
    let route = virtual_node(
        NodeKind::Route,
        "identity_service",
        "src/auth/auth.controller.ts",
        "POST /auth/refresh",
        route_qn("POST", "/auth/refresh"),
    );

    store
        .bulk_insert(
            &[
                fe_file.clone(),
                be_file.clone(),
                frontend.clone(),
                backend.clone(),
                route.clone(),
            ],
            &[
                EdgeData {
                    source: frontend.id,
                    target: route.id,
                    kind: EdgeKind::ConsumesApiFrom,
                    metadata: EdgeMetadata {
                        confidence: Some(920),
                        ..EdgeMetadata::default()
                    },
                    owner_file: fe_file.id,
                    is_cross_file: true,
                },
                EdgeData {
                    source: backend.id,
                    target: route.id,
                    kind: EdgeKind::Serves,
                    metadata: EdgeMetadata {
                        confidence: Some(990),
                        ..EdgeMetadata::default()
                    },
                    owner_file: be_file.id,
                    is_cross_file: true,
                },
            ],
        )
        .expect("graph write should succeed");

    let links = transport_links_for(&store, None, 100).expect("transport_links_for should succeed");
    assert_eq!(links.len(), 1);
    let link = &links[0];
    assert_eq!(link.frontend_node, frontend.id);
    assert_eq!(link.backend_node, backend.id);
    assert_eq!(link.method, "POST");
    assert_eq!(link.canonical_path, "/auth/refresh");
    assert_eq!(link.confidence, Confidence::Exact);
}

#[test]
fn queue_producer_consumer_link_resolves_correctly() {
    let temp = TempDb::new("queue");
    let store = GraphStoreDb::open(&temp.path).expect("store should open");

    let producer_file = file_node("report_service", "src/report.service.ts");
    let consumer_file = file_node("report_worker", "src/report.processor.ts");
    let producer = symbol_node(
        "report_service",
        "src/report.service.ts",
        "scheduleReport",
        0,
    );
    let consumer = symbol_node(
        "report_worker",
        "src/report.processor.ts",
        "processReport",
        0,
    );
    let queue = virtual_node(
        NodeKind::Queue,
        "report_service",
        "src/report.service.ts",
        "report-generation",
        queue_qn("bull", "report-generation"),
    );

    store
        .bulk_insert(
            &[
                producer_file.clone(),
                consumer_file.clone(),
                producer.clone(),
                consumer.clone(),
                queue.clone(),
            ],
            &[
                EdgeData {
                    source: producer.id,
                    target: queue.id,
                    kind: EdgeKind::Publishes,
                    metadata: EdgeMetadata::default(),
                    owner_file: producer_file.id,
                    is_cross_file: false,
                },
                EdgeData {
                    source: consumer.id,
                    target: queue.id,
                    kind: EdgeKind::Consumes,
                    metadata: EdgeMetadata::default(),
                    owner_file: consumer_file.id,
                    is_cross_file: true,
                },
            ],
        )
        .expect("graph write should succeed");

    let links = transport_links_for(&store, None, 100).expect("transport_links_for should succeed");
    assert_eq!(links.len(), 1);
    let link = &links[0];
    assert_eq!(link.frontend_node, producer.id);
    assert_eq!(link.backend_node, consumer.id);
    assert_eq!(link.method, "queue");
    assert_eq!(link.canonical_path, "report-generation");
    assert_eq!(link.confidence, Confidence::Exact);
}

#[test]
fn empty_graph_returns_no_links() {
    let temp = TempDb::new("empty");
    let store = GraphStoreDb::open(&temp.path).expect("store should open");
    store.bulk_insert(&[], &[]).expect("empty insert");

    let links = transport_links_for(&store, None, 100).expect("should succeed on empty graph");
    assert!(links.is_empty());
}
