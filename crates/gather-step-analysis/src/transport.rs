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
use gather_step_storage::{GraphReadSession, GraphStore, GraphStoreError};

use crate::confidence::ConfidenceBand;

/// Confidence of a [`TransportLink`] match.
///
/// This is a query-time-only concept; it is never persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Confidence {
    /// The frontend and backend QNs matched exactly via the same canonical
    /// route QN produced by [`gather_step_core::route_qn`].
    Exact,
    /// The consumer's canonical path segment-suffix-extends a unique
    /// server-only route (e.g. a gateway rewrite `/api/v1/items` → `/items`).
    Suffix,
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
    /// The confidence tier in the shared cross-surface vocabulary
    /// (`extracted` / `inferred`): [`Confidence::Exact`] maps to
    /// [`ConfidenceBand::Extracted`], [`Confidence::Suffix`] to
    /// [`ConfidenceBand::Inferred`].
    pub confidence_band: ConfidenceBand,
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
    let session = store.read_session()?;
    let mut links = Vec::new();

    // --- Route boundary ---
    for route_node in session.nodes_by_type(NodeKind::Route)? {
        if !route_node.is_virtual {
            continue;
        }
        let (method, canonical_path) = parse_route_qn(&route_node);

        let incoming = session.incoming(route_node.id)?;
        // The repo filter is applied to the consumer side only (the caller /
        // frontend). The server side is always included so that a filtered call
        // site still resolves to its backend handler.
        let frontend_ids: Vec<NodeId> = incoming
            .iter()
            .filter(|edge| edge.kind == EdgeKind::ConsumesApiFrom)
            .filter(|edge| node_matches_repo(session.as_ref(), edge.source, repo))
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
                    confidence_band: ConfidenceBand::Extracted,
                });
                if links.len() >= limit {
                    return Ok(links);
                }
            }
        }
    }

    // --- Queue boundary ---
    for queue_node in session.nodes_by_type(NodeKind::Queue)? {
        if !queue_node.is_virtual {
            continue;
        }
        let queue_path = parse_queue_qn(&queue_node);

        let incoming = session.incoming(queue_node.id)?;
        // The repo filter applies to the producer side (the caller / publisher).
        // Consumers are always included so a filtered producer still resolves to
        // its queue handler.
        let producer_ids: Vec<NodeId> = incoming
            .iter()
            .filter(|edge| edge.kind == EdgeKind::Publishes)
            .filter(|edge| node_matches_repo(session.as_ref(), edge.source, repo))
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
                    confidence_band: ConfidenceBand::Extracted,
                });
                if links.len() >= limit {
                    return Ok(links);
                }
            }
        }
    }

    // --- Suffix-tolerant route linking (query-time only, additive) ---
    append_suffix_route_links(session.as_ref(), repo, limit, &mut links)?;

    Ok(links)
}

/// A consumes-only route eligible to suffix-extend a server route.
struct SuffixConsumer {
    method: String,
    segments: Vec<String>,
    frontends: Vec<NodeId>,
}

/// A serves-only route that a consumer route may suffix-extend.
struct SuffixServer {
    method: String,
    path: String,
    segments: Vec<String>,
    backends: Vec<NodeId>,
}

/// Append query-time suffix links for gateway-style path rewrites.
///
/// A consumes-only route whose canonical path segment-suffix-extends a unique
/// serves-only route (same method, ≥2 aligned trailing segments) yields a
/// [`Confidence::Suffix`] link. Both sides are exclusive (a route that also
/// serves is never a consumer; a route that also consumes is never a server),
/// so these links never coincide with an [`Confidence::Exact`] pairing.
fn append_suffix_route_links(
    session: &dyn GraphReadSession,
    repo: Option<&str>,
    limit: usize,
    links: &mut Vec<TransportLink>,
) -> Result<(), GraphStoreError> {
    let mut consumers: Vec<SuffixConsumer> = Vec::new();
    let mut servers: Vec<SuffixServer> = Vec::new();

    for route_node in session.nodes_by_type(NodeKind::Route)? {
        if !route_node.is_virtual {
            continue;
        }
        let incoming = session.incoming(route_node.id)?;
        let has_serve = incoming.iter().any(|edge| edge.kind == EdgeKind::Serves);
        let has_consume = incoming
            .iter()
            .any(|edge| edge.kind == EdgeKind::ConsumesApiFrom);

        let (method, path) = parse_route_qn(&route_node);
        let segments = path_segments(&path);

        if has_consume && !has_serve {
            let frontends: Vec<NodeId> = incoming
                .iter()
                .filter(|edge| edge.kind == EdgeKind::ConsumesApiFrom)
                .filter(|edge| node_matches_repo(session, edge.source, repo))
                .map(|edge| edge.source)
                .collect();
            if !frontends.is_empty() {
                consumers.push(SuffixConsumer {
                    method,
                    segments,
                    frontends,
                });
            }
        } else if has_serve && !has_consume {
            let backends: Vec<NodeId> = incoming
                .iter()
                .filter(|edge| edge.kind == EdgeKind::Serves)
                .map(|edge| edge.source)
                .collect();
            servers.push(SuffixServer {
                method,
                path,
                segments,
                backends,
            });
        }
    }

    for consumer in &consumers {
        let mut matched: Option<&SuffixServer> = None;
        let mut ambiguous = false;
        for server in &servers {
            if server.method != consumer.method {
                continue;
            }
            if is_segment_suffix(&consumer.segments, &server.segments) {
                if matched.is_some() {
                    ambiguous = true;
                    break;
                }
                matched = Some(server);
            }
        }
        if ambiguous {
            continue;
        }
        let Some(server) = matched else {
            continue;
        };
        for &frontend_node in &consumer.frontends {
            for &backend_node in &server.backends {
                links.push(TransportLink {
                    frontend_node,
                    backend_node,
                    method: consumer.method.clone(),
                    canonical_path: server.path.clone(),
                    confidence: Confidence::Suffix,
                    confidence_band: ConfidenceBand::Inferred,
                });
                if links.len() >= limit {
                    return Ok(());
                }
            }
        }
    }

    Ok(())
}

/// Split a canonical route path into non-empty segments.
fn path_segments(path: &str) -> Vec<String> {
    path.split('/')
        .filter(|segment| !segment.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Return `true` when `consumer` strictly suffix-extends `server` on aligned
/// path segments: `server` has ≥2 segments, `consumer` has strictly more, and
/// `consumer`'s trailing segments equal `server` exactly. Parameter segments
/// (`:id`) only match identical parameter segments, never literals, because
/// canonicalization already normalises them and equality is byte-wise.
fn is_segment_suffix(consumer: &[String], server: &[String]) -> bool {
    let server_len = server.len();
    server_len >= 2
        && consumer.len() > server_len
        && consumer[consumer.len() - server_len..] == *server
}

/// Extract `(method, canonical_path)` from a Route virtual node's QN.
///
/// Route identities use the structured `__route__` or `__api_call__` forms.
fn parse_route_qn(node: &gather_step_core::NodeData) -> (String, String) {
    let qn = node
        .qualified_name
        .as_deref()
        .or(node.external_id.as_deref())
        .unwrap_or("");
    if let Some(route) = gather_step_core::parse_route_qn(qn) {
        return route;
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
fn node_matches_repo(session: &dyn GraphReadSession, id: NodeId, repo: Option<&str>) -> bool {
    let Some(filter) = repo else {
        return true;
    };
    session
        .node(id)
        .ok()
        .flatten()
        .is_some_and(|node| node.repo == filter)
}

#[cfg(test)]
mod tests {
    use gather_step_core::{
        EdgeData, EdgeKind, EdgeMetadata, NodeId, NodeKind, queue_qn, route_qn, virtual_node,
    };
    use gather_step_storage::GraphStore;

    use crate::test_utils::{TempDb, file_node, symbol_node};

    use super::{Confidence, ConfidenceBand, transport_links_for};

    fn route_vnode(method: &str, path: &str) -> gather_step_core::NodeData {
        virtual_node(
            NodeKind::Route,
            "svc",
            "src/routes.ts",
            format!("{method} {path}"),
            route_qn(method, path),
        )
    }

    fn consumes_edge(source: NodeId, route: NodeId, owner: NodeId) -> EdgeData {
        EdgeData {
            source,
            target: route,
            kind: EdgeKind::ConsumesApiFrom,
            metadata: EdgeMetadata::default(),
            owner_file: owner,
            is_cross_file: true,
        }
    }

    fn serves_edge(source: NodeId, route: NodeId, owner: NodeId) -> EdgeData {
        EdgeData {
            source,
            target: route,
            kind: EdgeKind::Serves,
            metadata: EdgeMetadata::default(),
            owner_file: owner,
            is_cross_file: true,
        }
    }

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
        assert_eq!(link.confidence, Confidence::Exact);
        assert_eq!(link.confidence_band, ConfidenceBand::Extracted);
    }

    #[test]
    fn gateway_bridge_does_not_emit_gateway_self_link() {
        let temp = TempDb::new("transport", "gateway-bridge");
        let store = temp.open();

        let fe_file = file_node("storefront-web", "src/api.ts");
        let gw_file = file_node("api-gateway", "src/serviceConfigs/items.service.ts");
        let be_file = file_node("items-svc", "provider.py");
        let frontend = symbol_node("storefront-web", "src/api.ts", "createItem", 0);
        let backend = symbol_node("items-svc", "provider.py", "create_item", 0);
        let public_route = route_vnode("POST", "/api/v1/items");
        let backend_route = route_vnode("POST", "/items");

        store
            .bulk_insert(
                &[
                    fe_file.clone(),
                    gw_file.clone(),
                    be_file.clone(),
                    frontend.clone(),
                    backend.clone(),
                    public_route.clone(),
                    backend_route.clone(),
                ],
                &[
                    consumes_edge(frontend.id, public_route.id, fe_file.id),
                    serves_edge(gw_file.id, public_route.id, gw_file.id),
                    consumes_edge(gw_file.id, backend_route.id, gw_file.id),
                    serves_edge(backend.id, backend_route.id, be_file.id),
                ],
            )
            .expect("bulk_insert should succeed");

        let links = transport_links_for(&store, None, 100).expect("links should resolve");
        assert_eq!(links.len(), 2, "expected public and backend route links");
        assert!(
            links.iter().any(|link| {
                link.frontend_node == frontend.id
                    && link.backend_node == gw_file.id
                    && link.canonical_path == "/api/v1/items"
                    && link.confidence == Confidence::Exact
            }),
            "frontend should link to gateway public route: {links:?}"
        );
        assert!(
            links.iter().any(|link| {
                link.frontend_node == gw_file.id
                    && link.backend_node == backend.id
                    && link.canonical_path == "/items"
                    && link.confidence == Confidence::Exact
            }),
            "gateway should link to backend route: {links:?}"
        );
        assert!(
            links
                .iter()
                .all(|link| link.frontend_node != gw_file.id || link.backend_node != gw_file.id),
            "gateway must not link to itself on the public route: {links:?}"
        );
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

    #[test]
    fn suffix_link_gateway_rewrite_matches_unique_server() {
        let temp = TempDb::new("transport", "suffix-basic");
        let store = temp.open();

        let fe_file = file_node("frontend_standard", "src/api.ts");
        let be_file = file_node("items_service", "src/items.controller.ts");
        let frontend = symbol_node("frontend_standard", "src/api.ts", "getItem", 0);
        let backend = symbol_node("items_service", "src/items.controller.ts", "getItem", 0);
        let consumer_route = route_vnode("GET", "/api/v1/items/:id");
        let server_route = route_vnode("GET", "/items/:id");

        store
            .bulk_insert(
                &[
                    fe_file.clone(),
                    be_file.clone(),
                    frontend.clone(),
                    backend.clone(),
                    consumer_route.clone(),
                    server_route.clone(),
                ],
                &[
                    consumes_edge(frontend.id, consumer_route.id, fe_file.id),
                    serves_edge(backend.id, server_route.id, be_file.id),
                ],
            )
            .expect("bulk_insert should succeed");

        let links = transport_links_for(&store, None, 100).expect("links should resolve");
        assert_eq!(links.len(), 1, "expected exactly one suffix link");
        let link = &links[0];
        assert_eq!(link.frontend_node, frontend.id);
        assert_eq!(link.backend_node, backend.id);
        assert_eq!(link.method, "GET");
        assert_eq!(link.canonical_path, "/items/:id");
        assert_eq!(link.confidence, Confidence::Suffix);
        assert_eq!(link.confidence_band, ConfidenceBand::Inferred);
    }

    #[test]
    fn suffix_link_requires_matching_method() {
        let temp = TempDb::new("transport", "suffix-method");
        let store = temp.open();

        let fe_file = file_node("frontend_standard", "src/api.ts");
        let be_file = file_node("items_service", "src/items.controller.ts");
        let frontend = symbol_node("frontend_standard", "src/api.ts", "getItem", 0);
        let backend = symbol_node("items_service", "src/items.controller.ts", "getItem", 0);
        let consumer_route = route_vnode("POST", "/api/v1/items/:id");
        let server_route = route_vnode("GET", "/items/:id");

        store
            .bulk_insert(
                &[
                    fe_file.clone(),
                    be_file.clone(),
                    frontend.clone(),
                    backend.clone(),
                    consumer_route.clone(),
                    server_route.clone(),
                ],
                &[
                    consumes_edge(frontend.id, consumer_route.id, fe_file.id),
                    serves_edge(backend.id, server_route.id, be_file.id),
                ],
            )
            .expect("bulk_insert should succeed");

        let links = transport_links_for(&store, None, 100).expect("links should resolve");
        assert!(links.is_empty(), "method mismatch must not link");
    }

    #[test]
    fn suffix_link_requires_at_least_two_trailing_segments() {
        let temp = TempDb::new("transport", "suffix-onesegment");
        let store = temp.open();

        let fe_file = file_node("frontend_standard", "src/api.ts");
        let be_file = file_node("items_service", "src/items.controller.ts");
        let frontend = symbol_node("frontend_standard", "src/api.ts", "listItems", 0);
        let backend = symbol_node("items_service", "src/items.controller.ts", "listItems", 0);
        let consumer_route = route_vnode("GET", "/api/items");
        let server_route = route_vnode("GET", "/items");

        store
            .bulk_insert(
                &[
                    fe_file.clone(),
                    be_file.clone(),
                    frontend.clone(),
                    backend.clone(),
                    consumer_route.clone(),
                    server_route.clone(),
                ],
                &[
                    consumes_edge(frontend.id, consumer_route.id, fe_file.id),
                    serves_edge(backend.id, server_route.id, be_file.id),
                ],
            )
            .expect("bulk_insert should succeed");

        let links = transport_links_for(&store, None, 100).expect("links should resolve");
        assert!(links.is_empty(), "single trailing segment must not link");
    }

    #[test]
    fn suffix_link_is_segment_aligned_not_string_suffix() {
        let temp = TempDb::new("transport", "suffix-segment");
        let store = temp.open();

        let fe_file = file_node("frontend_standard", "src/api.ts");
        let be_file = file_node("items_service", "src/items.controller.ts");
        let frontend = symbol_node("frontend_standard", "src/api.ts", "listItems", 0);
        let backend = symbol_node("items_service", "src/items.controller.ts", "listItems", 0);
        // "/svc/api-v1/items" ends with the string "v1/items" but its trailing
        // segments are [api-v1, items], not [v1, items].
        let consumer_route = route_vnode("GET", "/svc/api-v1/items");
        let server_route = route_vnode("GET", "/v1/items");

        store
            .bulk_insert(
                &[
                    fe_file.clone(),
                    be_file.clone(),
                    frontend.clone(),
                    backend.clone(),
                    consumer_route.clone(),
                    server_route.clone(),
                ],
                &[
                    consumes_edge(frontend.id, consumer_route.id, fe_file.id),
                    serves_edge(backend.id, server_route.id, be_file.id),
                ],
            )
            .expect("bulk_insert should succeed");

        let links = transport_links_for(&store, None, 100).expect("links should resolve");
        assert!(
            links.is_empty(),
            "string suffix without segment alignment must not link"
        );
    }

    #[test]
    fn suffix_link_param_does_not_match_literal_segment() {
        let temp = TempDb::new("transport", "suffix-param");
        let store = temp.open();

        let fe_file = file_node("frontend_standard", "src/api.ts");
        let be_file = file_node("items_service", "src/items.controller.ts");
        let frontend = symbol_node("frontend_standard", "src/api.ts", "getItem", 0);
        let backend = symbol_node("items_service", "src/items.controller.ts", "getItem", 0);
        let consumer_route = route_vnode("GET", "/api/items/123");
        let server_route = route_vnode("GET", "/items/:id");

        store
            .bulk_insert(
                &[
                    fe_file.clone(),
                    be_file.clone(),
                    frontend.clone(),
                    backend.clone(),
                    consumer_route.clone(),
                    server_route.clone(),
                ],
                &[
                    consumes_edge(frontend.id, consumer_route.id, fe_file.id),
                    serves_edge(backend.id, server_route.id, be_file.id),
                ],
            )
            .expect("bulk_insert should succeed");

        let links = transport_links_for(&store, None, 100).expect("links should resolve");
        assert!(
            links.is_empty(),
            "literal segment must not match a param segment"
        );
    }

    #[test]
    fn suffix_link_ambiguous_candidates_emit_nothing() {
        let temp = TempDb::new("transport", "suffix-ambiguous");
        let store = temp.open();

        let fe_file = file_node("frontend_standard", "src/api.ts");
        let items_ctrl_file = file_node("items_service", "src/items.controller.ts");
        let versioned_ctrl_file = file_node("items_service", "src/versioned.controller.ts");
        let frontend = symbol_node("frontend_standard", "src/api.ts", "getItem", 0);
        let items_handler =
            symbol_node("items_service", "src/items.controller.ts", "handleItems", 0);
        let versioned_handler = symbol_node(
            "items_service",
            "src/versioned.controller.ts",
            "handleVersioned",
            0,
        );
        let consumer_route = route_vnode("GET", "/api/v1/items/:id");
        let items_route = route_vnode("GET", "/items/:id");
        let versioned_route = route_vnode("GET", "/v1/items/:id");

        store
            .bulk_insert(
                &[
                    fe_file.clone(),
                    items_ctrl_file.clone(),
                    versioned_ctrl_file.clone(),
                    frontend.clone(),
                    items_handler.clone(),
                    versioned_handler.clone(),
                    consumer_route.clone(),
                    items_route.clone(),
                    versioned_route.clone(),
                ],
                &[
                    consumes_edge(frontend.id, consumer_route.id, fe_file.id),
                    serves_edge(items_handler.id, items_route.id, items_ctrl_file.id),
                    serves_edge(
                        versioned_handler.id,
                        versioned_route.id,
                        versioned_ctrl_file.id,
                    ),
                ],
            )
            .expect("bulk_insert should succeed");

        let links = transport_links_for(&store, None, 100).expect("links should resolve");
        assert!(links.is_empty(), "two suffix candidates must emit no link");
    }

    #[test]
    fn suffix_link_skips_consumer_route_that_also_serves() {
        let temp = TempDb::new("transport", "suffix-consumer-serves");
        let store = temp.open();

        let fe_file = file_node("frontend_standard", "src/api.ts");
        let be_file = file_node("items_service", "src/items.controller.ts");
        let self_file = file_node("gateway_service", "src/gw.controller.ts");
        let frontend = symbol_node("frontend_standard", "src/api.ts", "getItem", 0);
        let backend = symbol_node("items_service", "src/items.controller.ts", "getItem", 0);
        let gw = symbol_node("gateway_service", "src/gw.controller.ts", "proxy", 0);
        let consumer_route = route_vnode("GET", "/api/v1/items/:id");
        let server_route = route_vnode("GET", "/items/:id");

        store
            .bulk_insert(
                &[
                    fe_file.clone(),
                    be_file.clone(),
                    self_file.clone(),
                    frontend.clone(),
                    backend.clone(),
                    gw.clone(),
                    consumer_route.clone(),
                    server_route.clone(),
                ],
                &[
                    consumes_edge(frontend.id, consumer_route.id, fe_file.id),
                    // The consumer route also serves — it is not consumes-only.
                    serves_edge(gw.id, consumer_route.id, self_file.id),
                    serves_edge(backend.id, server_route.id, be_file.id),
                ],
            )
            .expect("bulk_insert should succeed");

        let links = transport_links_for(&store, None, 100).expect("links should resolve");
        // The consumer route (with its own Serves) yields an Exact link; no
        // Suffix link is produced.
        assert!(
            links.iter().all(|l| l.confidence == Confidence::Exact),
            "a route that serves must not act as a suffix consumer"
        );
    }

    #[test]
    fn suffix_link_skips_server_route_that_also_consumes() {
        let temp = TempDb::new("transport", "suffix-server-consumes");
        let store = temp.open();

        let exact_caller_file = file_node("frontend_standard", "src/exact.ts");
        let suffix_caller_file = file_node("frontend_standard", "src/suffix.ts");
        let be_file = file_node("items_service", "src/items.controller.ts");
        let exact_caller = symbol_node("frontend_standard", "src/exact.ts", "callExact", 0);
        let suffix_caller = symbol_node("frontend_standard", "src/suffix.ts", "callSuffix", 0);
        let backend = symbol_node("items_service", "src/items.controller.ts", "getItem", 0);
        // Server route has BOTH a Serves and a ConsumesApiFrom → not serves-only.
        let server_route = route_vnode("GET", "/items/:id");
        let consumer_route = route_vnode("GET", "/api/v1/items/:id");

        store
            .bulk_insert(
                &[
                    exact_caller_file.clone(),
                    suffix_caller_file.clone(),
                    be_file.clone(),
                    exact_caller.clone(),
                    suffix_caller.clone(),
                    backend.clone(),
                    server_route.clone(),
                    consumer_route.clone(),
                ],
                &[
                    consumes_edge(exact_caller.id, server_route.id, exact_caller_file.id),
                    serves_edge(backend.id, server_route.id, be_file.id),
                    consumes_edge(suffix_caller.id, consumer_route.id, suffix_caller_file.id),
                ],
            )
            .expect("bulk_insert should succeed");

        let links = transport_links_for(&store, None, 100).expect("links should resolve");
        assert!(
            links.iter().all(|l| l.confidence == Confidence::Exact),
            "a route that consumes must not act as a suffix server"
        );
    }
}
