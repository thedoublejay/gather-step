/// Evidence-chain derivation via bi-directional BFS.
///
/// An evidence chain is the shortest path between two graph nodes, recorded as
/// a sequence of [`EvidenceStep`] values.  When multiple shortest paths exist,
/// the one with the highest-specificity edge kinds wins (see
/// [`edge_specificity`] for the ordering).
///
/// The BFS explores at most `MAX_HOPS / 2` hops from each end (total path ≤
/// `MAX_HOPS` edges); paths longer than that are not reported.
use std::collections::{VecDeque, hash_map::Entry};

use gather_step_core::{EdgeKind, NodeId, VirtualNodeKind};
use gather_step_storage::{GraphStore, GraphStoreError};
use rustc_hash::FxHashMap;

/// Maximum total hop count for bi-directional BFS: at most `MAX_HOPS / 2` hops
/// from each end, for a maximum total path of `MAX_HOPS` edges.
const MAX_HOPS: usize = 8;

/// An optional annotation identifying a virtual transport-boundary node that
/// the step passes through.
type ViaNode = Option<VirtualNodeKind>;

/// A single traversal step in an evidence chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceStep {
    /// Source node of this step.
    pub from: NodeId,
    /// Target node of this step.
    pub to: NodeId,
    /// The kind of edge traversed.
    pub edge_kind: EdgeKind,
    /// When this step crosses a virtual transport-boundary node, this field
    /// carries a typed description of that virtual node.
    pub via: ViaNode,
}

/// A complete evidence chain from an anchor node to a target node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceChain {
    /// The target node reached by the chain.
    pub target: NodeId,
    /// Ordered sequence of steps from anchor to target.
    pub steps: Vec<EvidenceStep>,
}

/// Find the shortest evidence chain between `anchor` and `target` using
/// bi-directional BFS.
///
/// When multiple paths of equal length exist, the path with the highest
/// aggregate edge specificity is preferred (see [`edge_specificity`]).
/// Tie-breaking is done at the per-edge level during BFS expansion: if a
/// newly discovered edge reaches an already-visited node via a higher-specificity
/// edge kind, the stored predecessor is updated.
///
/// Returns `None` when no path is found within [`MAX_HOPS`] hops.
///
/// # Errors
///
/// Returns [`GraphStoreError`] on storage read failure.
pub fn evidence_chain_for<S: GraphStore>(
    store: &S,
    anchor: NodeId,
    target: NodeId,
) -> Result<Option<EvidenceChain>, GraphStoreError> {
    if anchor == target {
        return Ok(Some(EvidenceChain {
            target,
            steps: Vec::new(),
        }));
    }

    // parent[node] = (parent_node, edge_kind, specificity)
    // The specificity u8 enables tie-breaking: when two paths of equal length
    // reach the same node, the one with the higher-specificity edge wins.
    let mut forward_parent: FxHashMap<NodeId, (NodeId, EdgeKind, u8)> = FxHashMap::default();
    let mut backward_parent: FxHashMap<NodeId, (NodeId, EdgeKind, u8)> = FxHashMap::default();

    // Sentinels: point to themselves with specificity 0.
    forward_parent.insert(anchor, (anchor, EdgeKind::Defines, 0));
    backward_parent.insert(target, (target, EdgeKind::Defines, 0));

    let mut forward_frontier = VecDeque::from([anchor]);
    let mut backward_frontier = VecDeque::from([target]);

    let half = MAX_HOPS / 2;

    for _depth in 0..half {
        // Expand forward frontier by one hop.
        expand_frontier(store, &mut forward_frontier, &mut forward_parent, true)?;

        // Check for meeting point.
        let meeting = forward_parent
            .keys()
            .find(|node| backward_parent.contains_key(node))
            .copied();
        if let Some(meet) = meeting {
            return Ok(Some(build_chain(
                store,
                meet,
                target,
                &forward_parent,
                &backward_parent,
                anchor,
            )));
        }

        // Expand backward frontier by one hop.
        expand_frontier(store, &mut backward_frontier, &mut backward_parent, false)?;

        // Check for meeting point again after backward expansion.
        let meeting = backward_parent
            .keys()
            .find(|node| forward_parent.contains_key(node))
            .copied();
        if let Some(meet) = meeting {
            return Ok(Some(build_chain(
                store,
                meet,
                target,
                &forward_parent,
                &backward_parent,
                anchor,
            )));
        }
    }

    Ok(None)
}

/// Edge specificity ordering — higher is more informative.
///
/// The ordering defines preference when multiple paths of equal length exist.
/// Semantic-bridge edge kinds that carry cross-service meaning rank higher than
/// structural call/import edges.
#[must_use]
pub fn edge_specificity(kind: EdgeKind) -> u8 {
    match kind {
        EdgeKind::ImplementsContractFrom => 7,
        EdgeKind::ProducesEventFor => 6,
        EdgeKind::ConsumesApiFrom => 5,
        EdgeKind::UsesEventFrom => 4,
        EdgeKind::Calls => 3,
        EdgeKind::References => 2,
        EdgeKind::Imports => 1,
        _ => 0,
    }
}

/// Expand `frontier` by one hop in the graph, recording parent pointers in
/// `parents`.
///
/// When `forward` is `true`, outgoing edges are followed.  When `false`,
/// incoming edges are followed (backward BFS from target).
///
/// Tie-breaking: when a node is already visited at the same BFS depth but via
/// a lower-specificity edge, the stored predecessor is updated to the
/// higher-specificity one.  This ensures the claim in [`evidence_chain_for`]
/// that equal-length paths prefer higher-specificity edge kinds.
fn expand_frontier<S: GraphStore>(
    store: &S,
    frontier: &mut VecDeque<NodeId>,
    parents: &mut FxHashMap<NodeId, (NodeId, EdgeKind, u8)>,
    forward: bool,
) -> Result<(), GraphStoreError> {
    let current_len = frontier.len();
    for _ in 0..current_len {
        let Some(node) = frontier.pop_front() else {
            break;
        };
        let edges = if forward {
            store.get_outgoing(node)?
        } else {
            store.get_incoming(node)?
        };
        for edge in edges {
            let next = if forward { edge.target } else { edge.source };
            let new_spec = edge_specificity(edge.kind);
            match parents.entry(next) {
                Entry::Vacant(entry) => {
                    entry.insert((node, edge.kind, new_spec));
                    frontier.push_back(next);
                }
                Entry::Occupied(mut entry) => {
                    // If the new edge is more specific, update the predecessor
                    // without re-queuing (the depth is the same).
                    let stored_spec = entry.get().2;
                    if new_spec > stored_spec {
                        *entry.get_mut() = (node, edge.kind, new_spec);
                    }
                }
            }
        }
    }
    Ok(())
}

/// Reconstruct the evidence chain once a meeting node has been found.
///
/// The forward half (anchor → meet) is traced by following `forward_parent`
/// pointers back to `anchor` and then reversing.  The backward half
/// (meet → target) is traced by following `backward_parent` pointers forward.
///
/// The `via` field on each step is populated by resolving the step's `to` node
/// against the store: if the node is virtual and its QN matches a known
/// transport-boundary pattern (`__route__`, `__queue__`, `__topic__`,
/// `__event__`), a [`VirtualNodeKind`] value is attached.
fn build_chain<S: GraphStore>(
    store: &S,
    meet: NodeId,
    target: NodeId,
    forward_parent: &FxHashMap<NodeId, (NodeId, EdgeKind, u8)>,
    backward_parent: &FxHashMap<NodeId, (NodeId, EdgeKind, u8)>,
    anchor: NodeId,
) -> EvidenceChain {
    // Reconstruct forward path (anchor → meet).
    let mut forward_steps: Vec<EvidenceStep> = Vec::new();
    let mut current = meet;
    loop {
        let Some(&(parent, edge_kind, _spec)) = forward_parent.get(&current) else {
            break;
        };
        if parent == current {
            // Sentinel — we've reached the anchor.
            break;
        }
        let via = resolve_via(store, current);
        forward_steps.push(EvidenceStep {
            from: parent,
            to: current,
            edge_kind,
            via,
        });
        current = parent;
        if current == anchor {
            break;
        }
    }
    forward_steps.reverse();

    // Reconstruct backward path (meet → target).
    let mut backward_steps: Vec<EvidenceStep> = Vec::new();
    let mut current = meet;
    loop {
        let Some(&(child, edge_kind, _spec)) = backward_parent.get(&current) else {
            break;
        };
        if child == current {
            // Sentinel — we've reached the target.
            break;
        }
        let via = resolve_via(store, child);
        backward_steps.push(EvidenceStep {
            from: current,
            to: child,
            edge_kind,
            via,
        });
        current = child;
        if current == target {
            break;
        }
    }

    forward_steps.extend(backward_steps);
    EvidenceChain {
        target,
        steps: forward_steps,
    }
}

/// Attempt to resolve a [`VirtualNodeKind`] for the given node.
///
/// Returns `None` when the node is not virtual, does not exist in the store, or
/// when its QN does not match a known transport-boundary pattern.
fn resolve_via<S: GraphStore>(store: &S, node_id: NodeId) -> ViaNode {
    let Ok(Some(node)) = store.get_node(node_id) else {
        return None;
    };
    if !node.is_virtual {
        return None;
    }
    let qn = node
        .qualified_name
        .as_deref()
        .or(node.external_id.as_deref())
        .unwrap_or("");

    parse_virtual_node_kind(qn)
}

/// Parse a virtual-node QN into a [`VirtualNodeKind`].
///
/// Recognised prefixes:
/// - `__route__<METHOD>__<path>` → [`VirtualNodeKind::Route`]
/// - `__queue__<protocol>__<name>` → [`VirtualNodeKind::Queue`]
/// - `__topic__<protocol>__<name>` → [`VirtualNodeKind::Topic`]
/// - `__event__<transport>__<name>` → [`VirtualNodeKind::Event`]
fn parse_virtual_node_kind(qn: &str) -> ViaNode {
    if let Some(suffix) = qn.strip_prefix("__route__")
        && let Some((method, path)) = suffix.split_once("__")
    {
        return Some(VirtualNodeKind::Route {
            method: method.to_owned(),
            canonical_path: path.to_owned(),
        });
    } else if let Some(suffix) = qn.strip_prefix("__queue__")
        && let Some((protocol, name)) = suffix.split_once("__")
    {
        return Some(VirtualNodeKind::Queue {
            protocol: protocol.to_owned(),
            name: name.to_owned(),
        });
    } else if let Some(suffix) = qn.strip_prefix("__topic__")
        && let Some((_protocol, name)) = suffix.split_once("__")
    {
        return Some(VirtualNodeKind::Topic {
            name: name.to_owned(),
        });
    } else if let Some(suffix) = qn.strip_prefix("__event__")
        && let Some((_transport, name)) = suffix.split_once("__")
    {
        return Some(VirtualNodeKind::Event {
            name: name.to_owned(),
        });
    }
    None
}

#[cfg(test)]
mod tests {
    use gather_step_core::{EdgeData, EdgeKind, EdgeMetadata};
    use gather_step_storage::GraphStore;

    use crate::test_utils::{TempDb, file_node, symbol_node};

    use super::evidence_chain_for;

    #[test]
    fn same_node_returns_empty_chain() {
        let temp = TempDb::new("evidence", "same-node");
        let store = temp.open();
        let node = symbol_node("repo", "src/a.ts", "fn_a", 0);
        store
            .bulk_insert(std::slice::from_ref(&node), &[])
            .expect("insert");

        let chain = evidence_chain_for(&store, node.id, node.id)
            .expect("should succeed")
            .expect("should find trivial chain");
        assert!(chain.steps.is_empty());
        assert_eq!(chain.target, node.id);
    }

    #[test]
    fn direct_edge_produces_one_step_chain() {
        let temp = TempDb::new("evidence", "direct-edge");
        let store = temp.open();
        let fa = file_node("repo", "src/a.ts");
        let fb = file_node("repo", "src/b.ts");
        let a = symbol_node("repo", "src/a.ts", "fn_a", 0);
        let b = symbol_node("repo", "src/b.ts", "fn_b", 0);

        store
            .bulk_insert(
                &[fa.clone(), fb.clone(), a.clone(), b.clone()],
                &[EdgeData {
                    source: a.id,
                    target: b.id,
                    kind: EdgeKind::Calls,
                    metadata: EdgeMetadata::default(),
                    owner_file: fa.id,
                    is_cross_file: true,
                }],
            )
            .expect("insert");

        let chain = evidence_chain_for(&store, a.id, b.id)
            .expect("should succeed")
            .expect("direct edge must produce a chain");
        assert_eq!(chain.steps.len(), 1);
        assert_eq!(chain.steps[0].from, a.id);
        assert_eq!(chain.steps[0].to, b.id);
        assert_eq!(chain.steps[0].edge_kind, EdgeKind::Calls);
    }

    #[test]
    fn no_path_returns_none_within_hop_limit() {
        let temp = TempDb::new("evidence", "no-path");
        let store = temp.open();
        let fa = file_node("repo", "src/a.ts");
        let fb = file_node("repo", "src/b.ts");
        let a = symbol_node("repo", "src/a.ts", "fn_a", 0);
        let b = symbol_node("repo", "src/b.ts", "fn_b", 0);
        store
            .bulk_insert(&[fa, fb, a.clone(), b.clone()], &[])
            .expect("insert");

        let result = evidence_chain_for(&store, a.id, b.id).expect("should succeed");
        assert!(
            result.is_none(),
            "disconnected nodes must return None within hop limit"
        );
    }

    #[test]
    fn two_hop_path_is_found() {
        let temp = TempDb::new("evidence", "two-hop");
        let store = temp.open();
        let fa = file_node("repo", "src/a.ts");
        let fb = file_node("repo", "src/b.ts");
        let fc = file_node("repo", "src/c.ts");
        let a = symbol_node("repo", "src/a.ts", "fn_a", 0);
        let mid = symbol_node("repo", "src/b.ts", "fn_mid", 0);
        let c = symbol_node("repo", "src/c.ts", "fn_c", 0);

        store
            .bulk_insert(
                &[
                    fa.clone(),
                    fb.clone(),
                    fc.clone(),
                    a.clone(),
                    mid.clone(),
                    c.clone(),
                ],
                &[
                    EdgeData {
                        source: a.id,
                        target: mid.id,
                        kind: EdgeKind::Calls,
                        metadata: EdgeMetadata::default(),
                        owner_file: fa.id,
                        is_cross_file: true,
                    },
                    EdgeData {
                        source: mid.id,
                        target: c.id,
                        kind: EdgeKind::Calls,
                        metadata: EdgeMetadata::default(),
                        owner_file: fb.id,
                        is_cross_file: true,
                    },
                ],
            )
            .expect("insert");

        let chain = evidence_chain_for(&store, a.id, c.id)
            .expect("should succeed")
            .expect("two-hop path must be found");
        assert_eq!(chain.steps.len(), 2);
        assert_eq!(chain.target, c.id);
    }
}
