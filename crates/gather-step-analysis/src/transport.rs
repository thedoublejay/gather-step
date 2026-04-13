/// Transport-boundary linking: query-time derivation of [`TransportLink`]
/// structs for HTTP route and Bull queue boundaries.
///
/// All links are computed on demand from existing virtual nodes and edges in
/// the graph — nothing is persisted.  The matching strategy uses the same
/// virtual-node traversal pattern as [`crate::event_topology::trace_route`]
/// so there is no second bridge layer.
///
/// # Token / session linker
///
/// Token/session boundary extraction is not yet supported at the parser level.
/// When that support is added, a new linker variant can be added here
/// alongside `transport_links_for`.
use gather_step_core::{EdgeKind, NodeId, NodeKind};
use gather_step_storage::{GraphStore, GraphStoreError};

/// Confidence of a [`TransportLink`] match.
///
/// Only [`Confidence::Exact`] is currently assigned — `route_qn` normalises
/// all virtual-node QNs at emit time so every match is exact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Confidence {
    /// The frontend and backend QNs matched exactly via the same canonical
    /// route QN produced by [`gather_step_core::route_qn`].
    Exact,
}

/// A derived (non-persisted) link between a frontend caller and a backend
/// handler that share a transport boundary virtual node.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TransportLink {
    /// The frontend node that has a `ConsumesApiFrom` or `Consumes` edge to
    /// the virtual transport node.
    pub frontend_node: NodeId,
    /// The backend node that has a `Serves` or `Consumes` edge to the same
    /// virtual transport node.
    pub backend_node: NodeId,
    /// HTTP method or `"queue"` for queue links.
    pub method: String,
    /// Canonical path (e.g. `/orders/:id`) or queue name.
    pub canonical_path: String,
    /// How confident the match is.
    pub confidence: Confidence,
}

/// Derive transport links by walking all virtual transport nodes.
///
/// For **Route** virtual nodes:
/// - Find incoming `ConsumesApiFrom` edges → frontend callers.
/// - Find incoming `Serves` edges → backend handlers.
/// - Build a `TransportLink` for every frontend × backend pair.
///
/// For **Queue** virtual nodes:
/// - Find incoming `Publishes` edges → producers.
/// - Find incoming `Consumes` edges → consumers.
/// - Build a `TransportLink` for every producer × consumer pair.
///
/// All links are query-time only (not persisted).
///
/// # Errors
///
/// Returns [`GraphStoreError`] on storage read failure.
pub fn transport_links_for<S: GraphStore>(
    store: &S,
    repo: Option<&str>,
    limit: usize,
) -> Result<Vec<TransportLink>, GraphStoreError> {
    let mut links = Vec::new();

    // --- Route boundary ---
    for route_node in store.nodes_by_type(NodeKind::Route)? {
        if !route_node.is_virtual {
            continue;
        }
        let (method, canonical_path) = parse_route_qn(&route_node);

        let incoming = store.get_incoming(route_node.id)?;
        // The repo filter is applied to the consumer side only (the caller /
        // frontend). The server side is always included so that a filtered call
        // site still resolves to its backend handler.
        let frontend_ids: Vec<NodeId> = incoming
            .iter()
            .filter(|edge| edge.kind == EdgeKind::ConsumesApiFrom)
            .filter(|edge| node_matches_repo(store, edge.source, repo))
            .map(|edge| edge.source)
            .collect();
        let backend_ids: Vec<NodeId> = incoming
            .iter()
            .filter(|edge| edge.kind == EdgeKind::Serves)
            .map(|edge| edge.source)
            .collect();

        for &frontend_node in &frontend_ids {
            for &backend_node in &backend_ids {
                links.push(TransportLink {
                    frontend_node,
                    backend_node,
                    method: method.clone(),
                    canonical_path: canonical_path.clone(),
                    confidence: Confidence::Exact,
                });
                if links.len() >= limit {
                    return Ok(links);
                }
            }
        }
    }

    // --- Queue boundary ---
    for queue_node in store.nodes_by_type(NodeKind::Queue)? {
        if !queue_node.is_virtual {
            continue;
        }
        let queue_path = parse_queue_qn(&queue_node);

        let incoming = store.get_incoming(queue_node.id)?;
        // The repo filter applies to the producer side (the caller / publisher).
        // Consumers are always included so a filtered producer still resolves to
        // its queue handler.
        let producer_ids: Vec<NodeId> = incoming
            .iter()
            .filter(|edge| edge.kind == EdgeKind::Publishes)
            .filter(|edge| node_matches_repo(store, edge.source, repo))
            .map(|edge| edge.source)
            .collect();
        let consumer_ids: Vec<NodeId> = incoming
            .iter()
            .filter(|edge| edge.kind == EdgeKind::Consumes)
            .map(|edge| edge.source)
            .collect();

        for &frontend_node in &producer_ids {
            for &backend_node in &consumer_ids {
                links.push(TransportLink {
                    frontend_node,
                    backend_node,
                    method: "queue".to_owned(),
                    canonical_path: queue_path.clone(),
                    confidence: Confidence::Exact,
                });
                if links.len() >= limit {
                    return Ok(links);
                }
            }
        }
    }

    Ok(links)
}

/// Extract `(method, canonical_path)` from a Route virtual node's QN.
///
/// Route QNs follow the form `__route__<METHOD>__<path>`.
fn parse_route_qn(node: &gather_step_core::NodeData) -> (String, String) {
    let qn = node
        .qualified_name
        .as_deref()
        .or(node.external_id.as_deref())
        .unwrap_or("");
    if let Some(suffix) = qn.strip_prefix("__route__")
        && let Some((method, path)) = suffix.split_once("__")
    {
        return (method.to_owned(), path.to_owned());
    }
    // Fallback: use the node name as-is.
    ("UNKNOWN".to_owned(), node.name.clone())
}

/// Extract the queue name from a Queue virtual node's QN.
///
/// Queue QNs follow the form `__queue__<protocol>__<name>`.
fn parse_queue_qn(node: &gather_step_core::NodeData) -> String {
    let qn = node
        .qualified_name
        .as_deref()
        .or(node.external_id.as_deref())
        .unwrap_or("");
    if let Some(suffix) = qn.strip_prefix("__queue__")
        && let Some((_protocol, name)) = suffix.split_once("__")
    {
        return name.to_owned();
    }
    node.name.clone()
}

/// Return `true` when the node identified by `id` belongs to `repo`, or when
/// no repo filter is active.
fn node_matches_repo<S: GraphStore>(store: &S, id: NodeId, repo: Option<&str>) -> bool {
    let Some(filter) = repo else {
        return true;
    };
    store
        .get_node(id)
        .ok()
        .flatten()
        .is_some_and(|node| node.repo == filter)
}

#[cfg(test)]
mod tests {
    use gather_step_core::{
        EdgeData, EdgeKind, EdgeMetadata, NodeKind, queue_qn, route_qn, virtual_node,
    };
    use gather_step_storage::GraphStore;

    use crate::test_utils::{TempDb, file_node, symbol_node};

    use super::transport_links_for;

    #[test]
    fn route_transport_link_found_via_serves_and_consumes_api_from() {
        let temp = TempDb::new("transport", "route-link");
        let store = temp.open();

        let frontend_file = file_node("frontend_standard", "src/api.ts");
        let backend_file = file_node("backend_standard", "src/controller.ts");
        let frontend = symbol_node("frontend_standard", "src/api.ts", "fetchOrders", 0);
        let backend = symbol_node("backend_standard", "src/controller.ts", "listOrders", 0);
        let route = virtual_node(
            NodeKind::Route,
            "backend_standard",
            "src/controller.ts",
            "GET /orders",
            route_qn("GET", "/orders"),
        );

        store
            .bulk_insert(
                &[
                    frontend_file.clone(),
                    backend_file.clone(),
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
                            confidence: Some(900),
                            ..EdgeMetadata::default()
                        },
                        owner_file: frontend_file.id,
                        is_cross_file: true,
                    },
                    EdgeData {
                        source: backend.id,
                        target: route.id,
                        kind: EdgeKind::Serves,
                        metadata: EdgeMetadata {
                            confidence: Some(980),
                            ..EdgeMetadata::default()
                        },
                        owner_file: backend_file.id,
                        is_cross_file: true,
                    },
                ],
            )
            .expect("bulk_insert should succeed");

        let links = transport_links_for(&store, None, 100).expect("links should resolve");
        assert_eq!(links.len(), 1, "expected exactly one route link");
        let link = &links[0];
        assert_eq!(link.frontend_node, frontend.id);
        assert_eq!(link.backend_node, backend.id);
        assert_eq!(link.method, "GET");
        assert_eq!(link.canonical_path, "/orders");
    }

    #[test]
    fn queue_transport_link_found_via_publishes_and_consumes() {
        let temp = TempDb::new("transport", "queue-link");
        let store = temp.open();

        let producer_file = file_node("backend_standard", "src/service.ts");
        let consumer_file = file_node("worker_standard", "src/processor.ts");
        let producer = symbol_node("backend_standard", "src/service.ts", "enqueueReport", 0);
        let consumer = symbol_node("worker_standard", "src/processor.ts", "handleReport", 0);
        let queue = virtual_node(
            NodeKind::Queue,
            "backend_standard",
            "src/service.ts",
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
            .expect("bulk_insert should succeed");

        let links = transport_links_for(&store, None, 100).expect("links should resolve");
        assert_eq!(links.len(), 1, "expected exactly one queue link");
        let link = &links[0];
        assert_eq!(link.frontend_node, producer.id);
        assert_eq!(link.backend_node, consumer.id);
        assert_eq!(link.method, "queue");
        assert_eq!(link.canonical_path, "report-generation");
    }

    #[test]
    fn repo_filter_restricts_links() {
        let temp = TempDb::new("transport", "repo-filter");
        let store = temp.open();

        let fe_file = file_node("frontend_standard", "src/api.ts");
        let be_file = file_node("backend_standard", "src/controller.ts");
        let other_file = file_node("other_repo", "src/caller.ts");
        let frontend = symbol_node("frontend_standard", "src/api.ts", "call", 0);
        let backend = symbol_node("backend_standard", "src/controller.ts", "handle", 0);
        let other = symbol_node("other_repo", "src/caller.ts", "otherCall", 0);
        let route = virtual_node(
            NodeKind::Route,
            "backend_standard",
            "src/controller.ts",
            "POST /orders",
            route_qn("POST", "/orders"),
        );

        store
            .bulk_insert(
                &[
                    fe_file.clone(),
                    be_file.clone(),
                    other_file.clone(),
                    frontend.clone(),
                    backend.clone(),
                    other.clone(),
                    route.clone(),
                ],
                &[
                    EdgeData {
                        source: frontend.id,
                        target: route.id,
                        kind: EdgeKind::ConsumesApiFrom,
                        metadata: EdgeMetadata::default(),
                        owner_file: fe_file.id,
                        is_cross_file: true,
                    },
                    EdgeData {
                        source: other.id,
                        target: route.id,
                        kind: EdgeKind::ConsumesApiFrom,
                        metadata: EdgeMetadata::default(),
                        owner_file: other_file.id,
                        is_cross_file: true,
                    },
                    EdgeData {
                        source: backend.id,
                        target: route.id,
                        kind: EdgeKind::Serves,
                        metadata: EdgeMetadata::default(),
                        owner_file: be_file.id,
                        is_cross_file: true,
                    },
                ],
            )
            .expect("bulk_insert should succeed");

        // Without filter: both frontend and other repo get links (2 links).
        let all_links = transport_links_for(&store, None, 100).expect("ok");
        assert_eq!(all_links.len(), 2);

        // With filter: only frontend_standard is included.
        let filtered = transport_links_for(&store, Some("frontend_standard"), 100).expect("ok");
        assert_eq!(filtered.len(), 1);
        assert_eq!(
            store
                .get_node(filtered[0].frontend_node)
                .unwrap()
                .unwrap()
                .repo,
            "frontend_standard"
        );
    }
}
