use std::collections::VecDeque;

use gather_step_core::{EdgeKind, NodeData, NodeId, NodeKind};
use gather_step_storage::{GraphStore, GraphStoreError};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::event_topology::{RouteTrace, TopologyMatch, trace_route};

const DEFAULT_CRUD_TRACE_DEPTH: usize = 4;
const DEFAULT_CRUD_EXPANSION_MULTIPLIER: usize = 4;

#[derive(Debug, thiserror::Error)]
pub enum CrudTraceError {
    #[error(transparent)]
    Store(#[from] GraphStoreError),
    #[error(transparent)]
    Topology(#[from] crate::event_topology::EventTopologyError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CrudTraceRole {
    Caller,
    Handler,
    Service,
    Repository,
    Entity,
    Collection,
    DatabaseHint,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CrudTraceEntry {
    pub role: CrudTraceRole,
    pub edge_kind: Option<EdgeKind>,
    pub confidence: Option<u16>,
    pub depth: usize,
    pub file_path: String,
    pub line_number: Option<u32>,
    pub node_id: NodeId,
    pub node_kind: NodeKind,
    pub repo: String,
    pub resolver: Option<String>,
    pub symbol_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CrudTrace {
    pub target: NodeData,
    pub callers: Vec<CrudTraceEntry>,
    pub handlers: Vec<CrudTraceEntry>,
    pub continuation: Vec<CrudTraceEntry>,
    pub entities: Vec<CrudTraceEntry>,
    pub database_hints: Vec<CrudTraceEntry>,
    pub truncated: bool,
}

pub fn trace_crud_route<S: GraphStore>(
    store: &S,
    target: NodeId,
    limit: usize,
) -> Result<CrudTrace, CrudTraceError> {
    let route_trace = trace_route(store, target, limit)?;
    build_crud_trace(store, route_trace, limit, DEFAULT_CRUD_TRACE_DEPTH)
}

pub fn trace_crud_symbol<S: GraphStore>(
    store: &S,
    symbol_id: NodeId,
    limit: usize,
) -> Result<Option<CrudTrace>, CrudTraceError> {
    let Some(symbol) = store.get_node(symbol_id)? else {
        return Ok(None);
    };

    if symbol.kind == NodeKind::Route {
        return Ok(Some(trace_crud_route(store, symbol.id, limit)?));
    }

    let Some(route) = best_route_target_for_symbol(store, symbol.id)? else {
        return Ok(None);
    };

    let mut trace = trace_crud_route(store, route.id, limit)?;
    trace.callers.retain(|entry| entry.node_id == symbol.id);
    Ok(Some(trace))
}

fn build_crud_trace<S: GraphStore>(
    store: &S,
    route_trace: RouteTrace,
    limit: usize,
    max_depth: usize,
) -> Result<CrudTrace, CrudTraceError> {
    let callers = route_trace
        .callers
        .iter()
        .cloned()
        .map(|entry| crud_entry(entry, CrudTraceRole::Caller, 0))
        .collect::<Vec<_>>();
    let handlers = route_trace
        .handlers
        .iter()
        .cloned()
        .map(|entry| crud_entry(entry, CrudTraceRole::Handler, 0))
        .collect::<Vec<_>>();

    let mut continuation = Vec::new();
    let mut entities = Vec::new();
    let mut database_hints = Vec::new();
    let mut truncated = route_trace.truncated;
    let mut queue = VecDeque::new();
    let mut best_seen = FxHashMap::default();
    let mut seen_continuation = FxHashSet::default();
    let mut seen_entities = FxHashSet::default();
    let mut seen_database_hints = FxHashSet::default();
    let expansion_budget = limit
        .saturating_mul(DEFAULT_CRUD_EXPANSION_MULTIPLIER)
        .max(limit);
    let mut expansions = 0_usize;

    for handler in &route_trace.handlers {
        queue.push_back((handler.node_id, 0_usize, handler.confidence));
        best_seen.insert(handler.node_id.as_bytes(), (0_usize, handler.confidence));
    }

    while let Some((node_id, depth, path_confidence)) = queue.pop_front() {
        if expansions >= expansion_budget {
            truncated = true;
            break;
        }
        expansions = expansions.saturating_add(1);
        if depth >= max_depth {
            if has_follow_up(store, node_id, depth, &best_seen)? {
                truncated = true;
            }
            continue;
        }

        for edge in store.get_outgoing(node_id)? {
            if !is_crud_edge(edge.kind) {
                continue;
            }
            let Some(node) = store.get_node(edge.target)? else {
                continue;
            };
            if !is_relevant_crud_node(&node) {
                continue;
            }

            let role = classify_role(&node, edge.kind);
            let confidence = combine_confidence(path_confidence, edge.metadata.confidence);
            let entry = CrudTraceEntry {
                role,
                edge_kind: Some(edge.kind),
                confidence,
                depth: depth + 1,
                file_path: node.file_path.clone(),
                line_number: node.span.as_ref().map(|span| span.line_start),
                node_id: node.id,
                node_kind: node.kind,
                repo: node.repo.clone(),
                resolver: edge.metadata.resolver.clone(),
                symbol_name: node.name.clone(),
            };

            if should_include_in_continuation(role) {
                let inserted = seen_continuation.insert((entry.node_id.as_bytes(), role as u8));
                if inserted && continuation.len() < limit {
                    continuation.push(entry.clone());
                } else if inserted {
                    truncated = true;
                }
            }

            if matches!(role, CrudTraceRole::Entity | CrudTraceRole::Collection) {
                let inserted = seen_entities.insert((entry.node_id.as_bytes(), role as u8));
                if inserted && entities.len() < limit {
                    entities.push(entry.clone());
                } else if inserted {
                    truncated = true;
                }
            }
            if role == CrudTraceRole::DatabaseHint {
                let inserted = seen_database_hints.insert(entry.node_id.as_bytes());
                if inserted && database_hints.len() < limit {
                    database_hints.push(entry.clone());
                } else if inserted {
                    truncated = true;
                }
            }

            if should_recurse(role) && should_enqueue(node.id, depth + 1, confidence, &best_seen) {
                best_seen.insert(node.id.as_bytes(), (depth + 1, confidence));
                queue.push_back((node.id, depth + 1, confidence));
            }
        }
    }

    continuation.sort_by(entry_sort_key);
    entities.sort_by(entry_sort_key);
    database_hints.sort_by(entry_sort_key);

    Ok(CrudTrace {
        target: route_trace.target,
        callers,
        handlers,
        continuation,
        entities,
        database_hints,
        truncated,
    })
}

fn crud_entry(entry: TopologyMatch, role: CrudTraceRole, depth: usize) -> CrudTraceEntry {
    CrudTraceEntry {
        role,
        edge_kind: Some(entry.edge_kind),
        confidence: entry.confidence,
        depth,
        file_path: entry.file_path,
        line_number: entry.line_number,
        node_id: entry.node_id,
        node_kind: entry.node_kind,
        repo: entry.repo,
        resolver: entry.resolver,
        symbol_name: entry.symbol_name,
    }
}

fn has_follow_up<S: GraphStore>(
    store: &S,
    node_id: NodeId,
    depth: usize,
    best_seen: &FxHashMap<[u8; 16], (usize, Option<u16>)>,
) -> Result<bool, GraphStoreError> {
    for edge in store.get_outgoing(node_id)? {
        if !is_crud_edge(edge.kind) {
            continue;
        }
        let Some(node) = store.get_node(edge.target)? else {
            continue;
        };
        if !is_relevant_crud_node(&node) {
            continue;
        }
        let role = classify_role(&node, edge.kind);
        if should_recurse(role)
            && should_enqueue(node.id, depth + 1, edge.metadata.confidence, best_seen)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn best_route_target_for_symbol<S: GraphStore>(
    store: &S,
    symbol_id: NodeId,
) -> Result<Option<NodeData>, GraphStoreError> {
    let mut matches = store
        .get_outgoing(symbol_id)?
        .into_iter()
        .filter(|edge| edge.kind == EdgeKind::Consumes)
        .filter_map(|edge| {
            let node = store.get_node(edge.target).ok().flatten()?;
            (node.kind == NodeKind::Route && node.is_virtual).then_some((edge, node))
        })
        .collect::<Vec<_>>();
    matches.sort_by(|(left_edge, left_node), (right_edge, right_node)| {
        route_target_rank(right_node)
            .cmp(&route_target_rank(left_node))
            .then(
                right_edge
                    .metadata
                    .confidence
                    .cmp(&left_edge.metadata.confidence),
            )
            .then(left_node.name.cmp(&right_node.name))
            .then(left_node.id.as_bytes().cmp(&right_node.id.as_bytes()))
    });
    Ok(matches.into_iter().map(|(_, node)| node).next())
}

fn route_target_rank(node: &NodeData) -> u8 {
    match node.external_id.as_deref() {
        Some(id) if id.starts_with("__route__") => 2,
        Some(id) if id.starts_with("__api_call__") => 1,
        _ => 0,
    }
}

fn is_crud_edge(kind: EdgeKind) -> bool {
    matches!(
        kind,
        EdgeKind::Calls
            | EdgeKind::DependsOn
            | EdgeKind::References
            | EdgeKind::PersistsTo
            | EdgeKind::ConsumesApiFrom
    )
}

fn is_relevant_crud_node(node: &NodeData) -> bool {
    matches!(
        node.kind,
        NodeKind::Function | NodeKind::Class | NodeKind::Service | NodeKind::Entity
    )
}

fn classify_role(node: &NodeData, edge_kind: EdgeKind) -> CrudTraceRole {
    if node.kind == NodeKind::Entity {
        if node
            .external_id
            .as_deref()
            .is_some_and(|id| id.starts_with("__collection__"))
        {
            return CrudTraceRole::Collection;
        }
        return CrudTraceRole::Entity;
    }

    let mut lowered_name = node.name.clone();
    lowered_name.make_ascii_lowercase();
    let mut lowered_qn = node
        .qualified_name
        .as_deref()
        .unwrap_or(node.name.as_str())
        .to_owned();
    lowered_qn.make_ascii_lowercase();
    if lowered_name.contains("repository")
        || lowered_name.ends_with("repo")
        || lowered_qn.contains("repository")
        || lowered_qn.ends_with("repo")
    {
        return CrudTraceRole::Repository;
    }
    if lowered_name.contains("model")
        || lowered_name.contains("schema")
        || lowered_qn.contains("injectmodel")
        || edge_kind == EdgeKind::PersistsTo
    {
        return CrudTraceRole::DatabaseHint;
    }

    CrudTraceRole::Service
}

fn should_include_in_continuation(role: CrudTraceRole) -> bool {
    !matches!(role, CrudTraceRole::Caller | CrudTraceRole::Handler)
}

fn should_recurse(role: CrudTraceRole) -> bool {
    matches!(
        role,
        CrudTraceRole::Service | CrudTraceRole::Repository | CrudTraceRole::DatabaseHint
    )
}

fn combine_confidence(left: Option<u16>, right: Option<u16>) -> Option<u16> {
    match (left, right) {
        (Some(left), Some(right)) => {
            Some(u16::try_from((u32::from(left) * u32::from(right)) / 1000).unwrap_or(u16::MAX))
        }
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn should_enqueue(
    node_id: NodeId,
    depth: usize,
    confidence: Option<u16>,
    best_seen: &FxHashMap<[u8; 16], (usize, Option<u16>)>,
) -> bool {
    match best_seen.get(&node_id.as_bytes()) {
        None => true,
        Some((best_depth, best_confidence)) => {
            depth < *best_depth || (depth == *best_depth && confidence > *best_confidence)
        }
    }
}

fn entry_sort_key(left: &CrudTraceEntry, right: &CrudTraceEntry) -> std::cmp::Ordering {
    left.depth
        .cmp(&right.depth)
        .then(left.repo.cmp(&right.repo))
        .then(left.file_path.cmp(&right.file_path))
        .then(left.symbol_name.cmp(&right.symbol_name))
        .then(left.node_id.as_bytes().cmp(&right.node_id.as_bytes()))
}

#[cfg(test)]
mod tests {
    use gather_step_core::{
        EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, SourceSpan, Visibility, node_id,
        route_qn, virtual_node,
    };
    use gather_step_storage::{GraphStore, GraphStoreDb};

    use super::{CrudTraceRole, trace_crud_route};

    fn sample_node(
        repo: &str,
        file_path: &str,
        kind: NodeKind,
        name: &str,
        _ordinal: u16,
        line_start: u32,
    ) -> NodeData {
        NodeData {
            id: node_id(repo, file_path, kind, name),
            kind,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(name.to_owned()),
            external_id: None,
            signature: None,
            visibility: Some(Visibility::Public),
            span: Some(SourceSpan {
                line_start,
                line_len: 0,
                column_start: 0,
                column_len: 1,
            }),
            is_virtual: false,
        }
    }

    #[test]
    fn traces_handler_into_service_repository_and_entity() {
        let path = std::env::temp_dir().join(format!(
            "gather-step-crud-trace-{}.redb",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let db = GraphStoreDb::open(&path).expect("graph should open");
        let route = virtual_node(
            NodeKind::Route,
            "backend_standard",
            "src/controller.ts",
            "POST /orders",
            route_qn("POST", "/orders"),
        );
        let handler_file = sample_node(
            "backend_standard",
            "src/controller.ts",
            NodeKind::File,
            "src/controller.ts",
            0,
            1,
        );
        let service_file = sample_node(
            "backend_standard",
            "src/service.ts",
            NodeKind::File,
            "src/service.ts",
            0,
            1,
        );
        let repository_file = sample_node(
            "backend_standard",
            "src/repository.ts",
            NodeKind::File,
            "src/repository.ts",
            0,
            1,
        );
        let handler = sample_node(
            "backend_standard",
            "src/controller.ts",
            NodeKind::Function,
            "createOrder",
            0,
            10,
        );
        let service = sample_node(
            "backend_standard",
            "src/service.ts",
            NodeKind::Function,
            "persistOrder",
            0,
            5,
        );
        let repository = sample_node(
            "backend_standard",
            "src/repository.ts",
            NodeKind::Function,
            "orderRepository",
            0,
            8,
        );
        let entity = virtual_node(
            NodeKind::Entity,
            "backend_standard",
            "src/entity.ts",
            "OrderRecord",
            "__entity__OrderRecord",
        );
        db.bulk_insert(
            &[
                handler_file.clone(),
                service_file.clone(),
                repository_file.clone(),
                route.clone(),
                handler.clone(),
                service.clone(),
                repository.clone(),
                entity.clone(),
            ],
            &[
                EdgeData {
                    source: handler.id,
                    target: route.id,
                    kind: EdgeKind::Serves,
                    metadata: EdgeMetadata {
                        confidence: Some(980),
                        ..EdgeMetadata::default()
                    },
                    owner_file: handler_file.id,
                    is_cross_file: false,
                },
                EdgeData {
                    source: handler.id,
                    target: service.id,
                    kind: EdgeKind::Calls,
                    metadata: EdgeMetadata {
                        confidence: Some(920),
                        ..EdgeMetadata::default()
                    },
                    owner_file: handler_file.id,
                    is_cross_file: false,
                },
                EdgeData {
                    source: service.id,
                    target: repository.id,
                    kind: EdgeKind::Calls,
                    metadata: EdgeMetadata {
                        confidence: Some(910),
                        ..EdgeMetadata::default()
                    },
                    owner_file: service_file.id,
                    is_cross_file: false,
                },
                EdgeData {
                    source: repository.id,
                    target: entity.id,
                    kind: EdgeKind::References,
                    metadata: EdgeMetadata {
                        confidence: Some(900),
                        ..EdgeMetadata::default()
                    },
                    owner_file: repository_file.id,
                    is_cross_file: false,
                },
            ],
        )
        .expect("graph insert should succeed");

        let trace = trace_crud_route(&db, route.id, 10).expect("trace should succeed");
        assert_eq!(trace.handlers.len(), 1);
        assert!(trace.continuation.iter().any(|entry| {
            entry.role == CrudTraceRole::Service && entry.symbol_name == "persistOrder"
        }));
        assert!(trace.continuation.iter().any(|entry| {
            entry.role == CrudTraceRole::Repository && entry.symbol_name == "orderRepository"
        }));
        assert!(trace.entities.iter().any(|entry| {
            entry.role == CrudTraceRole::Entity && entry.symbol_name == "OrderRecord"
        }));
        let _ = std::fs::remove_file(path);
    }
}
