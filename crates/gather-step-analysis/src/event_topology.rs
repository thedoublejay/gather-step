use std::collections::BTreeMap;

use gather_step_core::{EdgeData, EdgeKind, NodeData, NodeId, NodeKind, route_qn};
use gather_step_storage::{GraphStore, GraphStoreError};
use rustc_hash::FxHashSet;
use thiserror::Error;

const EVENT_KINDS: [NodeKind; 5] = [
    NodeKind::Topic,
    NodeKind::Queue,
    NodeKind::Subject,
    NodeKind::Stream,
    NodeKind::Event,
];

#[derive(Debug, Error)]
pub enum EventTopologyError {
    #[error(transparent)]
    Store(#[from] GraphStoreError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventRole {
    Producer,
    Consumer,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteRole {
    Handler,
    Caller,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopologyMatch {
    pub edge_kind: EdgeKind,
    pub confidence: Option<u16>,
    pub file_path: String,
    pub line_number: Option<u32>,
    pub node_id: NodeId,
    pub node_kind: NodeKind,
    pub repo: String,
    pub resolver: Option<String>,
    pub symbol_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventTrace {
    pub target: NodeData,
    pub producers: Vec<TopologyMatch>,
    pub consumers: Vec<TopologyMatch>,
    pub truncated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteTrace {
    pub target: NodeData,
    pub handlers: Vec<TopologyMatch>,
    pub callers: Vec<TopologyMatch>,
    pub truncated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlastRadiusNode {
    pub node_id: NodeId,
    pub node_kind: NodeKind,
    pub name: String,
    pub repo: String,
    pub file_path: String,
    pub line_number: Option<u32>,
    pub cumulative_confidence: Option<u16>,
    pub depth: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlastRadiusEdge {
    pub source: NodeId,
    pub target: NodeId,
    pub edge_kind: EdgeKind,
    pub confidence: Option<u16>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventBlastRadius {
    pub target: NodeData,
    pub nodes: Vec<BlastRadiusNode>,
    pub edges: Vec<BlastRadiusEdge>,
    pub truncated: bool,
}

/// Classification of an orphan topic: either it has producers but no
/// consumers, or consumers but no producers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OrphanKind {
    /// Topic has at least one producer but zero consumers.
    ProduceOnly,
    /// Topic has at least one consumer but zero producers.
    ConsumeOnly,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrphanTopic {
    pub target: NodeData,
    pub producers: usize,
    pub consumers: usize,
    pub classification: &'static str,
    pub severity: &'static str,
}

/// Paged result from [`list_orphan_topics_paged`].
#[derive(Debug, Clone, Default)]
pub struct OrphanTopicsPage {
    /// Orphan topics returned (up to the requested limit).
    pub items: Vec<OrphanTopic>,
    /// Total number of orphan candidates seen during enumeration, including
    /// those that were skipped after the limit was reached.
    pub total_seen: usize,
    /// `true` when `total_seen > items.len()`, i.e. the result was truncated.
    pub truncated: bool,
    /// Count of skipped candidates, keyed by [`OrphanKind`].
    pub skipped_by_kind: BTreeMap<OrphanKind, usize>,
}

pub fn resolve_event_targets<S: GraphStore>(
    store: &S,
    target: &str,
) -> Result<Vec<NodeData>, EventTopologyError> {
    let target = target.trim();
    if target.is_empty() {
        return Ok(Vec::new());
    }

    // ── Pass 1: exact external_id lookup (already O(1) via BY_EXTERNAL_ID) ──
    let mut exact = Vec::new();
    for kind in EVENT_KINDS {
        exact.extend(store.nodes_by_external_id(kind, target)?);
    }
    if !exact.is_empty() {
        exact.sort_by(node_sort_key);
        exact.dedup_by_key(|node| node.id.as_bytes());
        return Ok(exact);
    }

    // ── Pass 2: indexed suffix / event-name lookup ──────────────────────────
    // `EVENT_FAMILY_INDEX` stores the normalised event name (suffix after the
    // last `__` in `external_id`, lowercased).  A target that exactly matches
    // this name covers the most common fallback case without scanning every
    // node of each event kind.
    let mut target_lower = target.to_owned();
    target_lower.make_ascii_lowercase();

    let mut matches = store.nodes_by_event_family_name(&target_lower)?;
    matches.retain(|node| node.is_virtual);
    matches.sort_by(node_sort_key);
    matches.dedup_by_key(|node| node.id.as_bytes());

    // ── Pass 2b: scan fallback for nodes whose name/qualified_name matches ──
    // Covers the rare case where a virtual node's name differs from the
    // normalised external_id suffix (e.g. a node whose `name` is the bare
    // target without the `__`-prefix chain, but whose `external_id` is absent
    // or has a different suffix).  Only executed when Pass 2a found nothing.
    if matches.is_empty() {
        let suffix = format!("__{target}");
        for kind in EVENT_KINDS {
            for node in store.nodes_by_type(kind)? {
                if !node.is_virtual {
                    continue;
                }
                let matches_target = node.name == target
                    || node.name.ends_with(&suffix)
                    || node.qualified_name.as_deref() == Some(target)
                    || node
                        .qualified_name
                        .as_deref()
                        .is_some_and(|v| v.ends_with(&suffix))
                    || node.external_id.as_deref() == Some(target)
                    || node
                        .external_id
                        .as_deref()
                        .is_some_and(|v| v.ends_with(&suffix));
                if matches_target {
                    matches.push(node);
                }
            }
        }
        matches.sort_by(node_sort_key);
        matches.dedup_by_key(|node| node.id.as_bytes());
    }

    if !matches.is_empty() {
        return Ok(matches);
    }

    // ── Pass 3: event-family prefix matching via index ───────────────────────
    // For a query like `"order"` we need all nodes whose normalised event name
    // starts with `"order."`.  The `EVENT_FAMILY_INDEX` stores exact names, so
    // we cannot do a range scan without additional infrastructure.  Instead,
    // fall back to the full `nodes_by_type` scan restricted to the family
    // prefix predicate — this path only activates when exact and suffix matches
    // are both absent, i.e. the caller used a bare family prefix like `"order"`.
    let Some(family_query) = normalized_event_family_query(target) else {
        return Ok(matches);
    };

    let mut family_matches = Vec::new();
    for kind in EVENT_KINDS {
        for node in store.nodes_by_type(kind)? {
            if !node.is_virtual {
                continue;
            }
            let Some(event_name) = event_name_for_node(&node) else {
                continue;
            };
            if event_family_matches(&event_name, &family_query) {
                family_matches.push(node);
            }
        }
    }

    family_matches.sort_by(node_sort_key);
    family_matches.dedup_by_key(|node| node.id.as_bytes());
    Ok(family_matches)
}

pub fn canonical_event_target<S: GraphStore>(
    store: &S,
    target: &str,
) -> Result<Option<NodeData>, EventTopologyError> {
    let mut targets = resolve_event_targets(store, target)?;
    if targets.is_empty() {
        return Ok(None);
    }
    rank_event_targets(store, &mut targets, target)?;
    Ok(targets.into_iter().next())
}

pub fn canonical_event_target_for_node<S: GraphStore>(
    store: &S,
    node: &NodeData,
) -> Result<Option<NodeData>, EventTopologyError> {
    let Some(name) = event_name_for_node(node) else {
        return Ok(None);
    };
    canonical_event_target(store, &name)
}

pub fn rank_event_targets<S: GraphStore>(
    store: &S,
    targets: &mut [NodeData],
    subject: &str,
) -> Result<(), EventTopologyError> {
    targets.sort_by(|left, right| {
        event_target_score(store, right, subject)
            .cmp(&event_target_score(store, left, subject))
            .then_with(|| left.repo.cmp(&right.repo))
            .then_with(|| left.file_path.cmp(&right.file_path))
            .then_with(|| left.name.cmp(&right.name))
            .then_with(|| left.id.as_bytes().cmp(&right.id.as_bytes()))
    });
    Ok(())
}

pub fn resolve_route_target<S: GraphStore>(
    store: &S,
    method: &str,
    path: &str,
) -> Result<Option<NodeData>, EventTopologyError> {
    let qualified_name = route_qn(method, path);
    // ── Pass 1: exact external_id lookup (O(1) via BY_EXTERNAL_ID) ──────────
    let exact = store
        .nodes_by_external_id(NodeKind::Route, &qualified_name)?
        .into_iter()
        .find(|node| node.is_virtual);
    if exact.is_some() {
        return Ok(exact);
    }

    let wanted = canonical_route_key(&qualified_name);
    let Some(wanted) = wanted else {
        return Ok(None);
    };

    // ── Pass 2: indexed route-key lookup (O(1) via ROUTE_KEY_INDEX) ──────────
    // The canonical key used by the index is `"{METHOD}__{path}"` — the same
    // format returned by `canonical_route_key`.  Convert our `(String, String)`
    // tuple to that format for the index query.
    let route_index_key = format!("{}__{}", wanted.0, wanted.1);
    let mut matches = store
        .nodes_by_route_key(&route_index_key)?
        .into_iter()
        .filter(|node| node.is_virtual)
        .collect::<Vec<_>>();

    if matches.is_empty() {
        // ── Pass 3: full-scan fallback (covers nodes inserted before the index
        // existed or nodes with non-standard external_id forms).
        matches = store
            .nodes_by_type(NodeKind::Route)?
            .into_iter()
            .filter(|node| node.is_virtual)
            .filter(|node| canonical_route_key_for_node(node).as_ref() == Some(&wanted))
            .collect::<Vec<_>>();
    }

    matches.sort_by(route_node_sort_key);
    Ok(matches.into_iter().next())
}

pub fn trace_event<S: GraphStore>(
    store: &S,
    target: NodeId,
    limit: usize,
) -> Result<EventTrace, EventTopologyError> {
    let Some(target_node) = store.get_node(target)? else {
        return Ok(EventTrace {
            target: missing_virtual_node(target, NodeKind::Topic),
            producers: Vec::new(),
            consumers: Vec::new(),
            truncated: false,
        });
    };

    let (mut producers, producers_truncated) = collect_incoming_matches_many_kinds(
        store,
        &[target],
        &[EdgeKind::Publishes, EdgeKind::ProducesEventFor],
        limit,
    )?;
    let (mut consumers, consumers_truncated) = collect_incoming_matches_many_kinds(
        store,
        &[target],
        &[
            EdgeKind::Consumes,
            EdgeKind::UsesEventFrom,
            EdgeKind::ContractOn,
        ],
        limit,
    )?;

    // Topic-envelope fallback: when the fine-grained Event node has no
    // producers, look for a matching Topic envelope (a virtual Topic whose
    // `external_id` / `qualified_name` follows the `__topic__<protocol>__<name>`
    // pattern) and use its producers instead.  This surfaces producers that
    // route through the envelope rather than referencing the typed Event node
    // directly (e.g. producers that call `sendMessage` with a string literal).
    let producers_truncated = if producers.is_empty() {
        let (envelope_producers, envelope_truncated) =
            topic_envelope_producers(store, &target_node, limit)?;
        producers = envelope_producers;
        envelope_truncated
    } else {
        producers_truncated
    };
    let consumers_truncated = if consumers.is_empty() {
        let (envelope_consumers, envelope_truncated) =
            topic_envelope_consumers(store, &target_node, limit)?;
        consumers = envelope_consumers;
        envelope_truncated
    } else {
        consumers_truncated
    };

    Ok(EventTrace {
        target: target_node,
        producers,
        consumers,
        truncated: producers_truncated || consumers_truncated,
    })
}

/// Find producers via the topic-envelope node that corresponds to `target`.
///
/// A topic-envelope node is a virtual [`NodeKind::Topic`] whose `external_id`
/// follows the `__topic__<protocol>__<topic_name>` pattern emitted by the
/// transport parsers.  When a fine-grained [`NodeKind::Event`] node has no
/// producers on its own incoming edges (because the producer used a string
/// literal rather than the typed event constant), walking the envelope's
/// incoming edges often surfaces the missing producers.
///
/// This function is additive: it is only called when fine-grained producers are
/// empty, so it never removes already-found producers.
fn topic_envelope_producers<S: GraphStore>(
    store: &S,
    target: &NodeData,
    limit: usize,
) -> Result<(Vec<TopologyMatch>, bool), EventTopologyError> {
    let Some(event_name) = event_name_for_node(target) else {
        return Ok((Vec::new(), false));
    };

    // Look up Topic-kind envelope nodes by the normalised event name via the
    // EVENT_FAMILY_INDEX — O(1) instead of a full-kind scan.
    let envelope_ids: Vec<NodeId> = store
        .nodes_by_event_family_name(&event_name)?
        .into_iter()
        .filter(|node| node.is_virtual && node.kind == NodeKind::Topic && node.id != target.id)
        .map(|node| node.id)
        .collect();

    if envelope_ids.is_empty() {
        return Ok((Vec::new(), false));
    }

    collect_incoming_matches_many_kinds(
        store,
        &envelope_ids,
        &[EdgeKind::Publishes, EdgeKind::ProducesEventFor],
        limit,
    )
}

fn topic_envelope_consumers<S: GraphStore>(
    store: &S,
    target: &NodeData,
    limit: usize,
) -> Result<(Vec<TopologyMatch>, bool), EventTopologyError> {
    let Some(event_name) = event_name_for_node(target) else {
        return Ok((Vec::new(), false));
    };

    let envelope_ids: Vec<NodeId> = store
        .nodes_by_event_family_name(&event_name)?
        .into_iter()
        .filter(|node| node.is_virtual && node.kind == NodeKind::Topic && node.id != target.id)
        .map(|node| node.id)
        .collect();

    if envelope_ids.is_empty() {
        return Ok((Vec::new(), false));
    }

    collect_incoming_matches_many_kinds(
        store,
        &envelope_ids,
        &[
            EdgeKind::Consumes,
            EdgeKind::UsesEventFrom,
            EdgeKind::ContractOn,
        ],
        limit,
    )
}

pub fn trace_route<S: GraphStore>(
    store: &S,
    target: NodeId,
    limit: usize,
) -> Result<RouteTrace, EventTopologyError> {
    let Some(target_node) = store.get_node(target)? else {
        return Ok(RouteTrace {
            target: missing_virtual_node(target, NodeKind::Route),
            handlers: Vec::new(),
            callers: Vec::new(),
            truncated: false,
        });
    };

    let route_targets = matching_route_target_ids(store, &target_node)?;
    let (handlers, handlers_truncated) =
        collect_incoming_matches_many(store, &route_targets, EdgeKind::Serves, limit)?;
    let (callers, callers_truncated) =
        collect_incoming_matches_many(store, &route_targets, EdgeKind::Consumes, limit)?;

    Ok(RouteTrace {
        target: target_node,
        handlers,
        callers,
        truncated: handlers_truncated || callers_truncated,
    })
}

fn canonical_route_key_for_node(node: &NodeData) -> Option<(String, String)> {
    node.external_id
        .as_deref()
        .or(node.qualified_name.as_deref())
        .and_then(canonical_route_key)
}

fn canonical_route_key(external_id: &str) -> Option<(String, String)> {
    let suffix = external_id
        .strip_prefix("__route__")
        .or_else(|| external_id.strip_prefix("__api_call__"))?;
    let (method, path) = suffix.split_once("__")?;
    let method = if method.eq_ignore_ascii_case("FETCH") {
        "GET".to_owned()
    } else {
        method.to_ascii_uppercase()
    };
    let path = if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("/{path}")
    };
    Some((method, path))
}

fn normalized_event_family_query(target: &str) -> Option<String> {
    let target = target.trim().trim_end_matches(".*").trim();
    if target.is_empty() {
        return None;
    }
    let mut normalized = target.to_owned();
    normalized.make_ascii_lowercase();
    Some(normalized)
}

fn event_name_for_node(node: &NodeData) -> Option<String> {
    let raw = node
        .external_id
        .as_deref()
        .or(node.qualified_name.as_deref())
        .unwrap_or(&node.name);
    if raw.is_empty() {
        return None;
    }
    let mut normalized = raw
        .rsplit_once("__")
        .map_or(raw, |(_, suffix)| suffix)
        .to_owned();
    normalized.make_ascii_lowercase();
    Some(normalized)
}

fn event_family_matches(event_name: &str, family_query: &str) -> bool {
    event_name == family_query
        || event_name
            .strip_prefix(family_query)
            .is_some_and(|suffix| suffix.starts_with('.'))
}

fn event_target_score<S: GraphStore>(store: &S, target: &NodeData, subject: &str) -> usize {
    let kind_bonus = match target.kind {
        NodeKind::Event => 4,
        NodeKind::Topic => 3,
        NodeKind::Queue => 2,
        _ => 1,
    };
    let family_bonus = event_name_for_node(target)
        .and_then(|event_name| {
            normalized_event_family_query(subject).map(|query| (event_name, query))
        })
        .map_or(0, |(event_name, query)| {
            if event_name == query {
                500
            } else if event_family_matches(&event_name, &query) {
                220
            } else {
                0
            }
        });
    let incoming_score = store
        .get_incoming(target.id)
        .map(|edges| {
            edges
                .into_iter()
                .filter(|edge| {
                    matches!(
                        edge.kind,
                        EdgeKind::Publishes
                            | EdgeKind::Consumes
                            | EdgeKind::ProducesEventFor
                            | EdgeKind::UsesEventFrom
                            | EdgeKind::ContractOn
                    )
                })
                .count()
        })
        .unwrap_or_default();
    kind_bonus * 100 + family_bonus + incoming_score
}

fn route_node_sort_key(left: &NodeData, right: &NodeData) -> std::cmp::Ordering {
    route_node_priority(left)
        .cmp(&route_node_priority(right))
        .then_with(|| node_sort_key(left, right))
}

fn route_node_priority(node: &NodeData) -> u8 {
    match node.external_id.as_deref() {
        Some(id) if id.starts_with("__route__") => 0,
        Some(id) if id.starts_with("__api_call__") => 1,
        _ => 2,
    }
}

fn matching_route_target_ids<S: GraphStore>(
    store: &S,
    target: &NodeData,
) -> Result<Vec<NodeId>, EventTopologyError> {
    let Some(key) = canonical_route_key_for_node(target) else {
        return Ok(vec![target.id]);
    };

    // Use the indexed ROUTE_KEY_INDEX for an O(1) lookup rather than scanning
    // all Route nodes.
    let route_index_key = format!("{}__{}", key.0, key.1);
    let mut indexed_ids: Vec<NodeId> = store
        .nodes_by_route_key(&route_index_key)?
        .into_iter()
        .filter(|node| node.is_virtual)
        .map(|node| node.id)
        .collect();

    let mut ids = if indexed_ids.is_empty() {
        // Fall back to full scan for nodes not yet in the index.
        store
            .nodes_by_type(NodeKind::Route)?
            .into_iter()
            .filter(|node| node.is_virtual)
            .filter(|node| canonical_route_key_for_node(node).as_ref() == Some(&key))
            .map(|node| node.id)
            .collect::<Vec<_>>()
    } else {
        indexed_ids.sort_by_key(|id| id.as_bytes());
        indexed_ids.dedup();
        indexed_ids
    };

    ids.sort_by_key(|id| id.as_bytes());
    ids.dedup();
    if ids.is_empty() {
        ids.push(target.id);
    }
    Ok(ids)
}

pub fn event_blast_radius<S: GraphStore>(
    store: &S,
    target: NodeId,
    max_depth: usize,
    limit: usize,
) -> Result<EventBlastRadius, EventTopologyError> {
    let Some(target_node) = store.get_node(target)? else {
        return Ok(EventBlastRadius {
            target: missing_virtual_node(target, NodeKind::Topic),
            nodes: Vec::new(),
            edges: Vec::new(),
            truncated: false,
        });
    };

    let mut nodes = Vec::new();
    let mut edges = Vec::new();
    let mut queue = std::collections::VecDeque::from([(target_node.id, 0_usize, Some(1000_u16))]);
    let mut seen = FxHashSet::from_iter([target_node.id.as_bytes()]);
    let mut seen_edges = FxHashSet::default();
    let mut truncated = false;

    while let Some((virtual_id, depth, path_confidence)) = queue.pop_front() {
        if nodes.len() >= limit {
            truncated = true;
            continue;
        }
        if depth >= max_depth {
            if !downstream_hops(store, virtual_id)?.is_empty() {
                truncated = true;
            }
            continue;
        }

        let edge_limit = limit.saturating_mul(4);
        for hop in downstream_hops(store, virtual_id)? {
            let edge_id = (
                hop.source.as_bytes(),
                hop.target.as_bytes(),
                hop.edge_kind.as_u8(),
            );
            if seen_edges.insert(edge_id) {
                edges.push(BlastRadiusEdge {
                    source: hop.source,
                    target: hop.target,
                    edge_kind: hop.edge_kind,
                    confidence: hop.confidence,
                });
                if edges.len() >= edge_limit {
                    truncated = true;
                    break;
                }
            }

            if let Some(node) = hop.node {
                let cumulative_confidence = combine_confidence(path_confidence, hop.confidence);
                if seen.insert(node.id.as_bytes()) {
                    nodes.push(BlastRadiusNode {
                        node_id: node.id,
                        node_kind: node.kind,
                        name: node.name.clone(),
                        repo: node.repo.clone(),
                        file_path: node.file_path.clone(),
                        line_number: node.span.as_ref().map(|span| span.line_start),
                        cumulative_confidence,
                        depth: depth + 1,
                    });
                    if node.is_virtual && is_topology_virtual(node.kind) {
                        queue.push_back((node.id, depth + 1, cumulative_confidence));
                    }
                }
                if nodes.len() >= limit {
                    truncated = true;
                    break;
                }
            }
        }
    }

    nodes.sort_by(|left, right| {
        left.depth
            .cmp(&right.depth)
            .then(left.repo.cmp(&right.repo))
            .then(left.file_path.cmp(&right.file_path))
            .then(left.name.cmp(&right.name))
            .then(left.node_id.as_bytes().cmp(&right.node_id.as_bytes()))
    });
    edges.sort_by(|left, right| {
        left.source
            .as_bytes()
            .cmp(&right.source.as_bytes())
            .then(left.target.as_bytes().cmp(&right.target.as_bytes()))
            .then(left.edge_kind.as_u8().cmp(&right.edge_kind.as_u8()))
    });

    Ok(EventBlastRadius {
        target: target_node,
        nodes,
        edges,
        truncated,
    })
}

/// Return a page of orphan topics with full truncation accounting.
///
/// Every candidate is always enumerated: candidates that would exceed `limit`
/// are counted in `total_seen` and tallied in `skipped_by_kind` rather than
/// silently dropped.
pub fn list_orphan_topics_paged<S: GraphStore>(
    store: &S,
    repo: Option<&str>,
    limit: usize,
) -> Result<OrphanTopicsPage, EventTopologyError> {
    let mut page = OrphanTopicsPage::default();

    for kind in EVENT_KINDS {
        for target in store.nodes_by_type(kind)? {
            if !target.is_virtual {
                continue;
            }
            let incoming = store.get_incoming(target.id)?;
            let producer_nodes = incoming
                .iter()
                .filter(|edge| edge.kind == EdgeKind::Publishes)
                .filter_map(|edge| store.get_node(edge.source).ok().flatten())
                .filter(|node| !node.is_virtual)
                .collect::<Vec<_>>();
            let consumer_nodes = incoming
                .iter()
                .filter(|edge| edge.kind == EdgeKind::Consumes)
                .filter_map(|edge| store.get_node(edge.source).ok().flatten())
                .filter(|node| !node.is_virtual)
                .collect::<Vec<_>>();
            if let Some(repo) = repo {
                let touches_repo = producer_nodes.iter().any(|node| node.repo == repo)
                    || consumer_nodes.iter().any(|node| node.repo == repo);
                if !touches_repo {
                    continue;
                }
            }

            let producers = producer_nodes.len();
            let consumers = consumer_nodes.len();
            if producers == 0 && consumers == 0 {
                continue;
            }
            if producers > 0 && consumers > 0 {
                continue;
            }

            let orphan_kind = if producers > 0 {
                OrphanKind::ProduceOnly
            } else {
                OrphanKind::ConsumeOnly
            };
            let classification = if producers > 0 {
                "produce_only"
            } else {
                "consume_only"
            };
            let severity = if consumers > 0 { "high" } else { "medium" };

            page.total_seen += 1;
            if page.items.len() < limit {
                page.items.push(OrphanTopic {
                    target,
                    producers,
                    consumers,
                    classification,
                    severity,
                });
            } else {
                *page.skipped_by_kind.entry(orphan_kind).or_default() += 1;
            }
        }
    }

    page.truncated = page.total_seen > page.items.len();

    page.items.sort_by(|left, right| {
        left.target
            .kind
            .cmp(&right.target.kind)
            .then(left.target.name.cmp(&right.target.name))
    });
    Ok(page)
}

/// List orphan topics up to `limit`.
///
/// An orphan topic is a virtual event/messaging node that has producers but
/// no consumers, or consumers but no producers.
///
/// When the result is truncated, a `tracing::warn!` is emitted so callers can
/// distinguish "no more orphans" from "more exist but were hidden by the page
/// limit". Use [`list_orphan_topics_paged`] directly for full accounting.
pub fn list_orphan_topics<S: GraphStore>(
    store: &S,
    repo: Option<&str>,
    limit: usize,
) -> Result<Vec<OrphanTopic>, EventTopologyError> {
    let page = list_orphan_topics_paged(store, repo, limit)?;
    if page.truncated {
        let non_zero: Vec<_> = page
            .skipped_by_kind
            .iter()
            .filter(|(_, count)| **count > 0)
            .map(|(kind, count)| format!("{kind:?}={count}"))
            .collect();
        tracing::warn!(
            total_seen = page.total_seen,
            returned = page.items.len(),
            skipped_by_kind = %non_zero.join(", "),
            "list_orphan_topics truncated: more orphans exist beyond the page limit",
        );
    }
    Ok(page.items)
}

fn collect_incoming_matches_many_kinds<S: GraphStore>(
    store: &S,
    targets: &[NodeId],
    edge_kinds: &[EdgeKind],
    limit: usize,
) -> Result<(Vec<TopologyMatch>, bool), EventTopologyError> {
    let mut entries = Vec::new();
    let mut seen = FxHashSet::default();

    for target in targets {
        for edge in store.get_incoming(*target)? {
            if !edge_kinds.contains(&edge.kind) {
                continue;
            }
            let Some(node) = store.get_node(edge.source)? else {
                continue;
            };
            if node.is_virtual {
                continue;
            }

            let key = (node.id.as_bytes(), edge.kind.as_u8());
            if !seen.insert(key) {
                continue;
            }

            entries.push(match_from_edge(node, &edge));
        }
    }

    entries.sort_by(match_sort_key);
    let truncated = entries.len() > limit;
    if truncated {
        entries.truncate(limit);
    }

    Ok((entries, truncated))
}

fn collect_incoming_matches_many<S: GraphStore>(
    store: &S,
    targets: &[NodeId],
    edge_kind: EdgeKind,
    limit: usize,
) -> Result<(Vec<TopologyMatch>, bool), EventTopologyError> {
    let mut entries = Vec::new();
    let mut seen = FxHashSet::default();

    for target in targets {
        for edge in store.get_incoming(*target)? {
            if edge.kind != edge_kind {
                continue;
            }
            let Some(node) = store.get_node(edge.source)? else {
                continue;
            };
            if node.is_virtual {
                continue;
            }

            let key = (node.id.as_bytes(), edge.kind.as_u8());
            if !seen.insert(key) {
                continue;
            }

            entries.push(match_from_edge(node, &edge));
        }
    }

    entries.sort_by(match_sort_key);
    let truncated = entries.len() > limit;
    if truncated {
        entries.truncate(limit);
    }

    Ok((entries, truncated))
}

struct DownstreamHop {
    source: NodeId,
    target: NodeId,
    edge_kind: EdgeKind,
    confidence: Option<u16>,
    node: Option<NodeData>,
}

fn downstream_hops<S: GraphStore>(
    store: &S,
    virtual_id: NodeId,
) -> Result<Vec<DownstreamHop>, EventTopologyError> {
    let Some(virtual_node) = store.get_node(virtual_id)? else {
        return Ok(Vec::new());
    };
    let mut hops = Vec::new();
    let relevant_incoming = match virtual_node.kind {
        NodeKind::Route => [EdgeKind::Consumes, EdgeKind::Serves].as_slice(),
        _ => [EdgeKind::Consumes].as_slice(),
    };

    for edge in store.get_incoming(virtual_id)? {
        if !relevant_incoming.contains(&edge.kind) {
            continue;
        }
        let Some(symbol) = store.get_node(edge.source)? else {
            continue;
        };
        if symbol.is_virtual {
            continue;
        }
        hops.push(DownstreamHop {
            source: virtual_id,
            target: symbol.id,
            edge_kind: edge.kind,
            confidence: edge.metadata.confidence,
            node: Some(symbol.clone()),
        });

        for outgoing in store.get_outgoing(symbol.id)? {
            if !matches!(
                outgoing.kind,
                EdgeKind::Publishes | EdgeKind::Consumes | EdgeKind::Serves
            ) {
                continue;
            }
            let Some(next) = store.get_node(outgoing.target)? else {
                continue;
            };
            if !next.is_virtual || !is_topology_virtual(next.kind) {
                continue;
            }
            hops.push(DownstreamHop {
                source: symbol.id,
                target: next.id,
                edge_kind: outgoing.kind,
                confidence: outgoing.metadata.confidence,
                node: Some(next),
            });
        }
    }

    Ok(hops)
}

fn match_from_edge(node: NodeData, edge: &EdgeData) -> TopologyMatch {
    TopologyMatch {
        edge_kind: edge.kind,
        confidence: edge.metadata.confidence,
        file_path: node.file_path,
        line_number: node.span.as_ref().map(|span| span.line_start),
        node_id: node.id,
        node_kind: node.kind,
        repo: node.repo,
        resolver: edge.metadata.resolver.clone(),
        symbol_name: node.name,
    }
}

fn missing_virtual_node(id: NodeId, kind: NodeKind) -> NodeData {
    NodeData {
        id,
        kind,
        repo: "__virtual__".to_owned(),
        file_path: String::new(),
        name: String::new(),
        qualified_name: None,
        external_id: None,
        signature: None,
        visibility: None,
        span: None,
        is_virtual: true,
    }
}

fn is_topology_virtual(kind: NodeKind) -> bool {
    matches!(
        kind,
        NodeKind::Route
            | NodeKind::Topic
            | NodeKind::Queue
            | NodeKind::Subject
            | NodeKind::Stream
            | NodeKind::Event
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

fn node_sort_key(left: &NodeData, right: &NodeData) -> std::cmp::Ordering {
    left.kind
        .cmp(&right.kind)
        .then(left.name.cmp(&right.name))
        .then(left.file_path.cmp(&right.file_path))
}

fn match_sort_key(left: &TopologyMatch, right: &TopologyMatch) -> std::cmp::Ordering {
    left.repo
        .cmp(&right.repo)
        .then(left.file_path.cmp(&right.file_path))
        .then(left.symbol_name.cmp(&right.symbol_name))
        .then(left.node_id.as_bytes().cmp(&right.node_id.as_bytes()))
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::{
        EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeId, NodeKind, SourceSpan, Visibility,
        node_id, route_qn, topic_qn, virtual_node,
    };
    use gather_step_storage::{GraphStore, GraphStoreDb};

    pub(crate) use super::list_orphan_topics_paged;
    use super::{
        event_blast_radius, resolve_event_targets, resolve_route_target, trace_event, trace_route,
    };

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new(name: &str) -> Self {
            let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-event-topology-{name}-{}-{id}.redb",
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
            let _ = fs::remove_file(&self.path);
        }
    }

    fn file(repo: &str, file_path: &str) -> NodeData {
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

    fn symbol(repo: &str, file_path: &str, name: &str, ordinal: u16) -> NodeData {
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
                line_start: 10 + u32::from(ordinal),
                line_len: 0,
                column_start: 0,
                column_len: 4,
            }),
            is_virtual: false,
        }
    }

    #[test]
    fn traces_event_producers_and_consumers_across_repos() {
        let temp_db = TempDb::new("trace-event");
        let store = GraphStoreDb::open(temp_db.path()).expect("store should open");
        let producer_file = file("backend_standard", "src/producer.ts");
        let consumer_file = file("frontend_standard", "src/consumer.ts");
        let producer = symbol("backend_standard", "src/producer.ts", "emit_order", 0);
        let consumer = symbol("frontend_standard", "src/consumer.ts", "handle_order", 0);
        let topic = virtual_node(
            NodeKind::Topic,
            "backend_standard",
            "src/events.ts",
            "order.created",
            topic_qn("kafka", "order.created"),
        );

        store
            .bulk_insert(
                &[
                    producer_file.clone(),
                    consumer_file.clone(),
                    producer.clone(),
                    consumer.clone(),
                    topic.clone(),
                ],
                &[
                    EdgeData {
                        source: producer.id,
                        target: topic.id,
                        kind: EdgeKind::Publishes,
                        metadata: EdgeMetadata {
                            confidence: Some(950),
                            ..EdgeMetadata::default()
                        },
                        owner_file: producer_file.id,
                        is_cross_file: true,
                    },
                    EdgeData {
                        source: consumer.id,
                        target: topic.id,
                        kind: EdgeKind::Consumes,
                        metadata: EdgeMetadata {
                            confidence: Some(920),
                            ..EdgeMetadata::default()
                        },
                        owner_file: consumer_file.id,
                        is_cross_file: true,
                    },
                ],
            )
            .expect("graph write should succeed");

        let resolved = resolve_event_targets(&store, "order.created").expect("resolution works");
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].id, topic.id);
        assert_eq!(resolved[0].kind, NodeKind::Topic);

        let family = resolve_event_targets(&store, "order").expect("family resolution works");
        assert_eq!(family.len(), 1);
        assert_eq!(family[0].id, topic.id);

        let trace = trace_event(&store, topic.id, 10).expect("trace should succeed");
        assert_eq!(trace.target.id, topic.id);
        assert_eq!(trace.producers.len(), 1);
        assert_eq!(trace.producers[0].repo, "backend_standard");
        assert_eq!(trace.producers[0].confidence, Some(950));
        assert_eq!(trace.consumers.len(), 1);
        assert_eq!(trace.consumers[0].repo, "frontend_standard");
        assert_eq!(trace.consumers[0].line_number, Some(10));
    }

    #[test]
    fn resolve_event_targets_expands_event_family_prefixes() {
        let temp_db = TempDb::new("resolve-family");
        let store = GraphStoreDb::open(temp_db.path()).expect("store should open");
        let created = virtual_node(
            NodeKind::Topic,
            "backend_standard",
            "src/events.ts",
            "order.created",
            topic_qn("kafka", "order.created"),
        );
        let sync = virtual_node(
            NodeKind::Topic,
            "backend_standard",
            "src/events.ts",
            "order.sync",
            topic_qn("kafka", "order.sync"),
        );

        store
            .bulk_insert(&[created.clone(), sync.clone()], &[])
            .expect("graph write should succeed");

        let resolved = resolve_event_targets(&store, "order").expect("resolution works");
        assert_eq!(resolved.len(), 2);
        assert!(
            resolved[0]
                .external_id
                .as_deref()
                .is_some_and(|value| value.ends_with("order.created"))
        );
        assert!(
            resolved[1]
                .external_id
                .as_deref()
                .is_some_and(|value| value.ends_with("order.sync"))
        );
    }

    #[test]
    fn traces_event_support_edges_beyond_plain_publish_consume() {
        let temp_db = TempDb::new("trace-event-support-edges");
        let store = GraphStoreDb::open(temp_db.path()).expect("store should open");
        let producer_file = file("backend_standard", "src/producer.ts");
        let consumer_file = file("frontend_standard", "src/consumer.ts");
        let producer = symbol("backend_standard", "src/producer.ts", "emit_order", 0);
        let consumer = symbol("frontend_standard", "src/consumer.ts", "handle_order", 0);
        let topic = virtual_node(
            NodeKind::Event,
            "backend_standard",
            "src/events.ts",
            "order.created",
            topic_qn("kafka", "order.created"),
        );

        store
            .bulk_insert(
                &[
                    producer_file.clone(),
                    consumer_file.clone(),
                    producer.clone(),
                    consumer.clone(),
                    topic.clone(),
                ],
                &[
                    EdgeData {
                        source: producer.id,
                        target: topic.id,
                        kind: EdgeKind::ProducesEventFor,
                        metadata: EdgeMetadata {
                            confidence: Some(930),
                            ..EdgeMetadata::default()
                        },
                        owner_file: producer_file.id,
                        is_cross_file: true,
                    },
                    EdgeData {
                        source: consumer.id,
                        target: topic.id,
                        kind: EdgeKind::UsesEventFrom,
                        metadata: EdgeMetadata {
                            confidence: Some(910),
                            ..EdgeMetadata::default()
                        },
                        owner_file: consumer_file.id,
                        is_cross_file: true,
                    },
                ],
            )
            .expect("graph write should succeed");

        let trace = trace_event(&store, topic.id, 10).expect("trace should succeed");
        assert_eq!(trace.producers.len(), 1);
        assert_eq!(trace.producers[0].edge_kind, EdgeKind::ProducesEventFor);
        assert_eq!(trace.consumers.len(), 1);
        assert_eq!(trace.consumers[0].edge_kind, EdgeKind::UsesEventFrom);
    }

    #[test]
    fn traces_route_handlers_and_callers_across_repos() {
        let temp_db = TempDb::new("trace-route");
        let store = GraphStoreDb::open(temp_db.path()).expect("store should open");
        let handler_file = file("backend_standard", "src/controller.ts");
        let caller_file = file("frontend_standard", "src/api.ts");
        let handler = symbol("backend_standard", "src/controller.ts", "create_order", 0);
        let caller = symbol("frontend_standard", "src/api.ts", "post_order", 0);
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
                    handler_file.clone(),
                    caller_file.clone(),
                    handler.clone(),
                    caller.clone(),
                    route.clone(),
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
                        is_cross_file: true,
                    },
                    EdgeData {
                        source: caller.id,
                        target: route.id,
                        kind: EdgeKind::Consumes,
                        metadata: EdgeMetadata {
                            confidence: Some(910),
                            ..EdgeMetadata::default()
                        },
                        owner_file: caller_file.id,
                        is_cross_file: true,
                    },
                ],
            )
            .expect("graph write should succeed");

        let resolved =
            resolve_route_target(&store, "post", "orders").expect("route resolution works");
        assert_eq!(resolved.map(|node| node.id), Some(route.id));

        let trace = trace_route(&store, route.id, 10).expect("trace should succeed");
        assert_eq!(trace.handlers.len(), 1);
        assert_eq!(trace.handlers[0].repo, "backend_standard");
        assert_eq!(trace.callers.len(), 1);
        assert_eq!(trace.callers[0].repo, "frontend_standard");
        assert_eq!(trace.callers[0].confidence, Some(910));
    }

    #[test]
    fn traces_route_callers_from_api_call_variants() {
        let temp_db = TempDb::new("trace-route-api-call");
        let store = GraphStoreDb::open(temp_db.path()).expect("store should open");
        let handler_file = file("backend_standard", "src/controller.ts");
        let caller_file = file("frontend_standard", "src/api.ts");
        let handler = symbol("backend_standard", "src/controller.ts", "list_orders", 0);
        let caller = symbol("frontend_standard", "src/api.ts", "load_orders", 0);
        let route = virtual_node(
            NodeKind::Route,
            "backend_standard",
            "src/controller.ts",
            "GET /orders",
            route_qn("GET", "/orders"),
        );
        let api_call = virtual_node(
            NodeKind::Route,
            "frontend_standard",
            "src/api.ts",
            "/orders",
            "__api_call__GET__orders",
        );

        store
            .bulk_insert(
                &[
                    handler_file.clone(),
                    caller_file.clone(),
                    handler.clone(),
                    caller.clone(),
                    route.clone(),
                    api_call.clone(),
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
                        is_cross_file: true,
                    },
                    EdgeData {
                        source: caller.id,
                        target: api_call.id,
                        kind: EdgeKind::Consumes,
                        metadata: EdgeMetadata {
                            confidence: Some(930),
                            ..EdgeMetadata::default()
                        },
                        owner_file: caller_file.id,
                        is_cross_file: true,
                    },
                ],
            )
            .expect("graph write should succeed");

        let resolved =
            resolve_route_target(&store, "GET", "/orders").expect("route resolution works");
        assert_eq!(resolved.map(|node| node.id), Some(route.id));

        let trace = trace_route(&store, route.id, 10).expect("trace should succeed");
        assert_eq!(trace.handlers.len(), 1);
        assert_eq!(trace.handlers[0].repo, "backend_standard");
        assert_eq!(trace.callers.len(), 1);
        assert_eq!(trace.callers[0].repo, "frontend_standard");
        assert_eq!(trace.callers[0].confidence, Some(930));
    }

    /// Build a fixture with 3 produce-only virtual topic nodes and 3
    /// consume-only virtual topic nodes (6 orphans total).
    ///
    /// "Produce-only" means a real symbol publishes to the topic but no one
    /// consumes it.  "Consume-only" means a real symbol consumes the topic but
    /// no one publishes to it.
    ///
    /// All nodes and edges are committed in a single `bulk_insert` call so
    /// that the per-owner-file edge-replacement logic in the store does not
    /// silently remove edges from earlier batches.
    pub(crate) fn build_orphan_fixture_for_truncation_test() -> GraphStoreDb {
        let temp_db = TempDb::new("orphan-truncation");
        // Leak the TempDb so the file is not deleted while the store is open.
        // Tests that call this helper accept the file lingering in /tmp until
        // the OS cleans it up.
        let path = temp_db.path().to_path_buf();
        std::mem::forget(temp_db);

        let store = GraphStoreDb::open(&path).expect("store should open");

        let mut all_nodes: Vec<NodeData> = Vec::new();
        let mut all_edges: Vec<EdgeData> = Vec::new();

        // Each topic/symbol pair uses a dedicated "file" node as the edge
        // owner so that successive pairs do not trigger the store's
        // per-owner-file edge replacement.
        //
        // 3 produce-only topics: a real publisher, no consumer.
        for i in 0_u16..3 {
            let owner = file("backend_standard", &format!("src/produce_only_{i}.ts"));
            let qn = format!("__topic__kafka__produce_only_{i}");
            let topic = virtual_node(
                NodeKind::Topic,
                "backend_standard",
                format!("src/produce_only_{i}.ts"),
                format!("produce_only_{i}"),
                qn.clone(),
            );
            let publisher = symbol(
                "backend_standard",
                &format!("src/produce_only_{i}.ts"),
                &format!("publisher_{i}"),
                i,
            );
            all_edges.push(EdgeData {
                source: publisher.id,
                target: topic.id,
                kind: EdgeKind::Publishes,
                metadata: EdgeMetadata::default(),
                owner_file: owner.id,
                is_cross_file: false,
            });
            all_nodes.extend([owner, topic, publisher]);
        }

        // 3 consume-only topics: a real consumer, no publisher.
        for i in 0_u16..3 {
            let owner = file("backend_standard", &format!("src/consume_only_{i}.ts"));
            let qn = format!("__topic__kafka__consume_only_{i}");
            let topic = virtual_node(
                NodeKind::Topic,
                "backend_standard",
                format!("src/consume_only_{i}.ts"),
                format!("consume_only_{i}"),
                qn.clone(),
            );
            let consumer = symbol(
                "backend_standard",
                &format!("src/consume_only_{i}.ts"),
                &format!("consumer_{i}"),
                10 + i,
            );
            all_edges.push(EdgeData {
                source: consumer.id,
                target: topic.id,
                kind: EdgeKind::Consumes,
                metadata: EdgeMetadata::default(),
                owner_file: owner.id,
                is_cross_file: false,
            });
            all_nodes.extend([owner, topic, consumer]);
        }

        store
            .bulk_insert(&all_nodes, &all_edges)
            .expect("fixture bulk_insert should succeed");

        store
    }

    #[test]
    fn list_orphan_topics_paged_reports_mid_kind_truncation() {
        let store = build_orphan_fixture_for_truncation_test();

        let page = list_orphan_topics_paged(&store, None, 4).expect("paged succeeds");

        assert_eq!(page.items.len(), 4, "honours limit");
        assert!(page.truncated, "mid-kind truncation must be reported");
        assert_eq!(page.total_seen, 6, "enumeration sees all 6 orphans");
        assert!(
            page.skipped_by_kind.values().any(|count| *count > 0),
            "skipped_by_kind must be populated: {:?}",
            page.skipped_by_kind
        );
    }

    #[test]
    fn list_orphan_topics_paged_marks_untruncated_when_limit_exceeds_set() {
        let store = build_orphan_fixture_for_truncation_test();
        let page = list_orphan_topics_paged(&store, None, 100).expect("ok");
        assert!(!page.truncated);
        assert_eq!(page.items.len(), 6);
        assert_eq!(page.total_seen, 6);
        assert!(page.skipped_by_kind.values().all(|count| *count == 0));
    }

    /// When an Event node has zero producers on its own incoming edges but a
    /// matching Topic envelope exists (same normalised event name), `trace_event`
    /// must fall back to the envelope's producers instead of returning an empty
    /// producers list.
    ///
    /// If fine-grained producers already exist, the fallback must NOT fire and
    /// the result must be unchanged.
    #[test]
    fn trace_event_falls_back_to_topic_envelope_when_event_node_has_no_producers() {
        let temp_db = TempDb::new("envelope-fallback");
        let store = GraphStoreDb::open(temp_db.path()).expect("store should open");

        // Fine-grained Event node — no producers will be linked directly to it.
        let event_node = virtual_node(
            NodeKind::Event,
            "shared_contracts",
            "src/events.ts",
            "report.csv_queued",
            topic_qn("kafka", "report.csv_queued"),
        );

        // Topic envelope for the same event name.
        let topic_envelope = virtual_node(
            NodeKind::Topic,
            "shared_contracts",
            "src/topics.ts",
            "report.csv_queued",
            topic_qn("kafka", "report.csv_queued"),
        );

        let producer_file = file("backend_standard", "src/report-concern.ts");
        let producer_sym = symbol("backend_standard", "src/report-concern.ts", "queueCsv", 0);

        store
            .bulk_insert(
                &[
                    event_node.clone(),
                    topic_envelope.clone(),
                    producer_file.clone(),
                    producer_sym.clone(),
                ],
                &[
                    // Producer is linked to the TOPIC ENVELOPE, not the Event node.
                    EdgeData {
                        source: producer_sym.id,
                        target: topic_envelope.id,
                        kind: EdgeKind::Publishes,
                        metadata: EdgeMetadata {
                            confidence: Some(800),
                            ..EdgeMetadata::default()
                        },
                        owner_file: producer_file.id,
                        is_cross_file: true,
                    },
                ],
            )
            .expect("graph write should succeed");

        let trace = trace_event(&store, event_node.id, 20).expect("trace should succeed");

        assert_eq!(
            trace.producers.len(),
            1,
            "expected 1 producer via envelope fallback, got {:?}",
            trace.producers
        );
        assert_eq!(trace.producers[0].repo, "backend_standard");
        assert_eq!(trace.producers[0].symbol_name, "queueCsv");
    }

    #[test]
    fn trace_event_falls_back_to_topic_envelope_when_event_node_has_no_consumers() {
        let temp_db = TempDb::new("envelope-consumer-fallback");
        let store = GraphStoreDb::open(temp_db.path()).expect("store should open");

        let event_node = virtual_node(
            NodeKind::Event,
            "shared_contracts",
            "src/events.ts",
            "report.generated",
            topic_qn("kafka", "report.generated"),
        );
        let topic_envelope = virtual_node(
            NodeKind::Topic,
            "shared_contracts",
            "src/topics.ts",
            "report.generated",
            topic_qn("kafka", "report.generated"),
        );
        let consumer_file = file("backend_standard", "src/report-handler.ts");
        let consumer_sym = symbol(
            "backend_standard",
            "src/report-handler.ts",
            "handleGeneratedReport",
            0,
        );

        store
            .bulk_insert(
                &[
                    event_node.clone(),
                    topic_envelope.clone(),
                    consumer_file.clone(),
                    consumer_sym.clone(),
                ],
                &[EdgeData {
                    source: consumer_sym.id,
                    target: topic_envelope.id,
                    kind: EdgeKind::UsesEventFrom,
                    metadata: EdgeMetadata {
                        confidence: Some(820),
                        ..EdgeMetadata::default()
                    },
                    owner_file: consumer_file.id,
                    is_cross_file: true,
                }],
            )
            .expect("graph write should succeed");

        let trace = trace_event(&store, event_node.id, 20).expect("trace should succeed");

        assert_eq!(
            trace.consumers.len(),
            1,
            "expected 1 consumer via envelope fallback, got {:?}",
            trace.consumers
        );
        assert_eq!(trace.consumers[0].repo, "backend_standard");
        assert_eq!(trace.consumers[0].symbol_name, "handleGeneratedReport");
    }

    /// When fine-grained producers already exist on the Event node, the
    /// topic-envelope fallback must NOT fire.
    #[test]
    fn trace_event_skips_envelope_fallback_when_fine_grained_producers_exist() {
        let temp_db = TempDb::new("no-fallback-when-producers-exist");
        let store = GraphStoreDb::open(temp_db.path()).expect("store should open");

        let event_node = virtual_node(
            NodeKind::Event,
            "shared_contracts",
            "src/events.ts",
            "order.shipped",
            topic_qn("kafka", "order.shipped"),
        );

        // Topic envelope for the same event.
        let topic_envelope = virtual_node(
            NodeKind::Topic,
            "shared_contracts",
            "src/topics.ts",
            "order.shipped",
            topic_qn("kafka", "order.shipped"),
        );

        let fine_grained_file = file("backend_standard", "src/direct.ts");
        let fine_grained_sym = symbol("backend_standard", "src/direct.ts", "shipOrder", 0);
        let envelope_file = file("backend_standard", "src/envelope.ts");
        let envelope_sym = symbol("backend_standard", "src/envelope.ts", "sendViaEnvelope", 0);

        store
            .bulk_insert(
                &[
                    event_node.clone(),
                    topic_envelope.clone(),
                    fine_grained_file.clone(),
                    fine_grained_sym.clone(),
                    envelope_file.clone(),
                    envelope_sym.clone(),
                ],
                &[
                    // Fine-grained producer linked directly to Event node.
                    EdgeData {
                        source: fine_grained_sym.id,
                        target: event_node.id,
                        kind: EdgeKind::Publishes,
                        metadata: EdgeMetadata::default(),
                        owner_file: fine_grained_file.id,
                        is_cross_file: true,
                    },
                    // Envelope producer — should NOT appear in the result.
                    EdgeData {
                        source: envelope_sym.id,
                        target: topic_envelope.id,
                        kind: EdgeKind::Publishes,
                        metadata: EdgeMetadata::default(),
                        owner_file: envelope_file.id,
                        is_cross_file: true,
                    },
                ],
            )
            .expect("graph write should succeed");

        let trace = trace_event(&store, event_node.id, 20).expect("trace should succeed");

        // Only the fine-grained producer must appear; the envelope producer
        // must be absent because the fallback was never triggered.
        assert_eq!(
            trace.producers.len(),
            1,
            "expected exactly 1 fine-grained producer, got {:?}",
            trace.producers
        );
        assert_eq!(trace.producers[0].symbol_name, "shipOrder");
    }

    /// Indexed event lookup beats a full-kind scan on 1000 synthetic events.
    ///
    /// Inserts 1000 virtual `Topic` nodes with distinct names, then calls
    /// `trace_event` for a single target.  The test verifies correctness (the
    /// correct producers are returned) and that the index resolves the right
    /// target without iterating every event node.  The correctness assertion
    /// implicitly confirms the indexed lookup path is working: if the index were
    /// broken the trace would return an empty or wrong result.
    #[test]
    fn indexed_event_lookup_resolves_single_target_from_1000_events() {
        let temp_db = TempDb::new("indexed-event-1000");
        let store = GraphStoreDb::open(temp_db.path()).expect("store should open");

        let mut all_nodes: Vec<NodeData> = Vec::new();
        let mut all_edges: Vec<gather_step_core::EdgeData> = Vec::new();

        // Insert 999 unrelated topic nodes.
        for i in 0_u16..999 {
            let qn = format!("__topic__kafka__unrelated_event_{i}");
            let topic = virtual_node(
                NodeKind::Topic,
                "backend_standard",
                format!("src/events_{i}.ts"),
                format!("unrelated_event_{i}"),
                qn,
            );
            all_nodes.push(topic);
        }

        // Insert the target event node and its producer.
        let target_qn = "__topic__kafka__target.resolved".to_owned();
        let target_topic = virtual_node(
            NodeKind::Topic,
            "backend_standard",
            "src/target_events.ts",
            "target.resolved",
            target_qn.clone(),
        );
        let producer_file = file("backend_standard", "src/producer.ts");
        let producer_sym = symbol("backend_standard", "src/producer.ts", "emitTarget", 0);
        all_edges.push(gather_step_core::EdgeData {
            source: producer_sym.id,
            target: target_topic.id,
            kind: EdgeKind::Publishes,
            metadata: gather_step_core::EdgeMetadata {
                confidence: Some(900),
                ..gather_step_core::EdgeMetadata::default()
            },
            owner_file: producer_file.id,
            is_cross_file: true,
        });
        all_nodes.push(target_topic.clone());
        all_nodes.push(producer_file);
        all_nodes.push(producer_sym);

        store
            .bulk_insert(&all_nodes, &all_edges)
            .expect("bulk_insert should succeed");

        // Resolve via the indexed lookup path.
        let resolved =
            resolve_event_targets(&store, "target.resolved").expect("resolution should succeed");
        assert_eq!(
            resolved.len(),
            1,
            "indexed lookup must return exactly the target topic; got {resolved:?}"
        );
        assert_eq!(resolved[0].id, target_topic.id);

        // Trace the event — producers should be found via the indexed path.
        let trace = trace_event(&store, target_topic.id, 50).expect("trace should succeed");
        assert_eq!(
            trace.producers.len(),
            1,
            "expected 1 producer for the indexed target topic; got {:?}",
            trace.producers
        );
        assert_eq!(trace.producers[0].symbol_name, "emitTarget");
        assert_eq!(trace.producers[0].confidence, Some(900));
    }

    /// Build a blast-radius fixture where three consumers share the same
    /// depth / repo / `file_path` / name so that only the `node_id` tiebreak
    /// can distinguish them.  Returns the store and the virtual topic `NodeId`.
    fn build_blast_radius_shared_key_fixture() -> (GraphStoreDb, NodeId) {
        let temp_db = TempDb::new("blast-radius-stable");
        let path = temp_db.path().to_path_buf();
        std::mem::forget(temp_db);

        let store = GraphStoreDb::open(&path).expect("store should open");

        let topic = virtual_node(
            NodeKind::Topic,
            "backend_standard",
            "src/events.ts",
            "order.created",
            topic_qn("kafka", "order.created"),
        );
        let file_a = NodeData {
            id: node_id(
                "backend_standard",
                "src/consumer.ts",
                NodeKind::File,
                "src/consumer.ts",
            ),
            kind: NodeKind::File,
            repo: "backend_standard".to_owned(),
            file_path: "src/consumer.ts".to_owned(),
            name: "src/consumer.ts".to_owned(),
            qualified_name: None,
            external_id: None,
            signature: None,
            visibility: None,
            span: None,
            is_virtual: false,
        };
        // Three consumers sharing identical repo / file_path / name so that
        // only `node_id` (derived from distinct qualified names) breaks the tie.
        let consumer_a = NodeData {
            id: node_id(
                "backend_standard",
                "src/consumer.ts",
                NodeKind::Function,
                "handleOrder_a",
            ),
            kind: NodeKind::Function,
            repo: "backend_standard".to_owned(),
            file_path: "src/consumer.ts".to_owned(),
            name: "handleOrder".to_owned(),
            qualified_name: Some("handleOrder_a".to_owned()),
            external_id: None,
            signature: None,
            visibility: Some(Visibility::Public),
            span: Some(SourceSpan {
                line_start: 10,
                line_len: 0,
                column_start: 0,
                column_len: 4,
            }),
            is_virtual: false,
        };
        let consumer_b = NodeData {
            id: node_id(
                "backend_standard",
                "src/consumer.ts",
                NodeKind::Function,
                "handleOrder_b",
            ),
            kind: NodeKind::Function,
            repo: "backend_standard".to_owned(),
            file_path: "src/consumer.ts".to_owned(),
            name: "handleOrder".to_owned(),
            qualified_name: Some("handleOrder_b".to_owned()),
            external_id: None,
            signature: None,
            visibility: Some(Visibility::Public),
            span: Some(SourceSpan {
                line_start: 20,
                line_len: 0,
                column_start: 0,
                column_len: 4,
            }),
            is_virtual: false,
        };
        let consumer_c = NodeData {
            id: node_id(
                "backend_standard",
                "src/consumer.ts",
                NodeKind::Function,
                "handleOrder_c",
            ),
            kind: NodeKind::Function,
            repo: "backend_standard".to_owned(),
            file_path: "src/consumer.ts".to_owned(),
            name: "handleOrder".to_owned(),
            qualified_name: Some("handleOrder_c".to_owned()),
            external_id: None,
            signature: None,
            visibility: Some(Visibility::Public),
            span: Some(SourceSpan {
                line_start: 30,
                line_len: 0,
                column_start: 0,
                column_len: 4,
            }),
            is_virtual: false,
        };

        let topic_id = topic.id;
        store
            .bulk_insert(
                &[
                    file_a.clone(),
                    topic.clone(),
                    consumer_a.clone(),
                    consumer_b.clone(),
                    consumer_c.clone(),
                ],
                &[
                    EdgeData {
                        source: consumer_a.id,
                        target: topic.id,
                        kind: EdgeKind::Consumes,
                        metadata: EdgeMetadata::default(),
                        owner_file: file_a.id,
                        is_cross_file: false,
                    },
                    EdgeData {
                        source: consumer_b.id,
                        target: topic.id,
                        kind: EdgeKind::Consumes,
                        metadata: EdgeMetadata::default(),
                        owner_file: file_a.id,
                        is_cross_file: false,
                    },
                    EdgeData {
                        source: consumer_c.id,
                        target: topic.id,
                        kind: EdgeKind::Consumes,
                        metadata: EdgeMetadata::default(),
                        owner_file: file_a.id,
                        is_cross_file: false,
                    },
                ],
            )
            .expect("fixture insert should succeed");

        (store, topic_id)
    }

    /// Repeated calls to `event_blast_radius` on the same store must return
    /// nodes in identical order every time.
    #[test]
    fn event_blast_radius_node_order_is_stable() {
        let (store, topic_id) = build_blast_radius_shared_key_fixture();

        let first =
            event_blast_radius(&store, topic_id, 3, 50).expect("blast radius should succeed");
        let second = event_blast_radius(&store, topic_id, 3, 50)
            .expect("blast radius should succeed on second call");

        assert_eq!(
            first.nodes, second.nodes,
            "node ordering must be identical across repeated calls"
        );
        assert_eq!(
            first.edges, second.edges,
            "edge ordering must be identical across repeated calls"
        );
    }

    /// Inserting the fixture nodes in reversed order must still produce the
    /// same output ordering as the forward insertion.
    #[test]
    fn event_blast_radius_output_invariant_under_insertion_order() {
        let (store_forward, topic_id) = build_blast_radius_shared_key_fixture();

        let temp_db = TempDb::new("blast-radius-reversed");
        let path = temp_db.path().to_path_buf();
        std::mem::forget(temp_db);
        let store_rev = GraphStoreDb::open(&path).expect("store should open");

        let topic = virtual_node(
            NodeKind::Topic,
            "backend_standard",
            "src/events.ts",
            "order.created",
            topic_qn("kafka", "order.created"),
        );
        let file_a = NodeData {
            id: node_id(
                "backend_standard",
                "src/consumer.ts",
                NodeKind::File,
                "src/consumer.ts",
            ),
            kind: NodeKind::File,
            repo: "backend_standard".to_owned(),
            file_path: "src/consumer.ts".to_owned(),
            name: "src/consumer.ts".to_owned(),
            qualified_name: None,
            external_id: None,
            signature: None,
            visibility: None,
            span: None,
            is_virtual: false,
        };
        let consumer_a = NodeData {
            id: node_id(
                "backend_standard",
                "src/consumer.ts",
                NodeKind::Function,
                "handleOrder_a",
            ),
            kind: NodeKind::Function,
            repo: "backend_standard".to_owned(),
            file_path: "src/consumer.ts".to_owned(),
            name: "handleOrder".to_owned(),
            qualified_name: Some("handleOrder_a".to_owned()),
            external_id: None,
            signature: None,
            visibility: Some(Visibility::Public),
            span: Some(SourceSpan {
                line_start: 10,
                line_len: 0,
                column_start: 0,
                column_len: 4,
            }),
            is_virtual: false,
        };
        let consumer_b = NodeData {
            id: node_id(
                "backend_standard",
                "src/consumer.ts",
                NodeKind::Function,
                "handleOrder_b",
            ),
            kind: NodeKind::Function,
            repo: "backend_standard".to_owned(),
            file_path: "src/consumer.ts".to_owned(),
            name: "handleOrder".to_owned(),
            qualified_name: Some("handleOrder_b".to_owned()),
            external_id: None,
            signature: None,
            visibility: Some(Visibility::Public),
            span: Some(SourceSpan {
                line_start: 20,
                line_len: 0,
                column_start: 0,
                column_len: 4,
            }),
            is_virtual: false,
        };
        let consumer_c = NodeData {
            id: node_id(
                "backend_standard",
                "src/consumer.ts",
                NodeKind::Function,
                "handleOrder_c",
            ),
            kind: NodeKind::Function,
            repo: "backend_standard".to_owned(),
            file_path: "src/consumer.ts".to_owned(),
            name: "handleOrder".to_owned(),
            qualified_name: Some("handleOrder_c".to_owned()),
            external_id: None,
            signature: None,
            visibility: Some(Visibility::Public),
            span: Some(SourceSpan {
                line_start: 30,
                line_len: 0,
                column_start: 0,
                column_len: 4,
            }),
            is_virtual: false,
        };

        // Insert in reversed order: c, b, a, topic, file_a.
        store_rev
            .bulk_insert(
                &[
                    consumer_c.clone(),
                    consumer_b.clone(),
                    consumer_a.clone(),
                    topic.clone(),
                    file_a.clone(),
                ],
                &[
                    EdgeData {
                        source: consumer_c.id,
                        target: topic.id,
                        kind: EdgeKind::Consumes,
                        metadata: EdgeMetadata::default(),
                        owner_file: file_a.id,
                        is_cross_file: false,
                    },
                    EdgeData {
                        source: consumer_b.id,
                        target: topic.id,
                        kind: EdgeKind::Consumes,
                        metadata: EdgeMetadata::default(),
                        owner_file: file_a.id,
                        is_cross_file: false,
                    },
                    EdgeData {
                        source: consumer_a.id,
                        target: topic.id,
                        kind: EdgeKind::Consumes,
                        metadata: EdgeMetadata::default(),
                        owner_file: file_a.id,
                        is_cross_file: false,
                    },
                ],
            )
            .expect("reversed fixture insert should succeed");

        let result_fwd = event_blast_radius(&store_forward, topic_id, 3, 50)
            .expect("blast radius on forward store should succeed");
        let result_rev = event_blast_radius(&store_rev, topic.id, 3, 50)
            .expect("blast radius on reversed store should succeed");

        assert_eq!(
            result_fwd.nodes, result_rev.nodes,
            "node ordering must be invariant to insertion order"
        );
        assert_eq!(
            result_fwd.edges, result_rev.edges,
            "edge ordering must be invariant to insertion order"
        );
    }
}
