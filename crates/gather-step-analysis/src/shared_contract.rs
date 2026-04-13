//! Candidate-resolution for shared-contract impact queries.
//!
//! Both the `pack` (MCP) and `impact` (CLI) paths need to expand a query
//! anchor into the set of related virtual / structural nodes that
//! `shared_contract_impact` should be evaluated against. Earlier the two
//! paths had independent implementations: the CLI one was rich (guard-
//! class extraction, event/route shape expansion, edge-neighbour
//! expansion) while the MCP one was a simpler same-name peer scan. They
//! could disagree on what counts as a "candidate" for the same anchor —
//! producing pack/impact divergence on the canonical primary repo.
//!
//! This module factors the resolution into one place. Both paths call
//! [`shared_contract_candidate_ids`] and accept the same expanded set,
//! eliminating the divergence by construction.

use std::collections::BTreeSet;

use gather_step_core::{EdgeKind, NodeData, NodeId, NodeKind};
use gather_step_storage::{GraphStore, GraphStoreError};

use crate::pack_assembly::QueryShape;

/// Build the set of candidate node ids for a shared-contract impact
/// query starting at `node`.
///
/// Resolution rules (in evaluation order):
///   1. The anchor itself is added when it is a contract-shaped kind
///      (`SharedSymbol` / `Type` / `Function` / `Class`).
///   2. Edge neighbours reached via structural shared-contract edges
///      (`UsesShared`, `UsesTypeFrom`, `UsesGuardFrom`,
///      `ImplementsContractFrom`) are added when their kind is
///      contract-shaped.
///   3. Virtual `SharedSymbol` / `Type` peers whose lowercased trailing
///      name matches the anchor's name are added (uses the
///      `SHARED_SYMBOL_NAME_INDEX` for an O(1) lookup; previously this
///      was a full kind-table scan). Hook stubs (`__hook__…`) are
///      excluded because they are augmenter-generated consumer markers,
///      not symbol definitions.
///   4. For event-shaped queries, `Topic` / `Queue` / `Subject` /
///      `Stream` / `Event` nodes reached via `ProducesEventFor`,
///      `UsesEventFrom`, or `Publishes` edges are added.
///   5. For route-shaped queries, `Route` nodes reached via
///      `ConsumesApiFrom` or `Serves` edges are added.
///   6. For guard-shaped anchors (heuristic from
///      [`looks_like_guard_entrypoint`]), the file node is added and
///      every `Class` / virtual `SharedSymbol` whose name matches the
///      extracted guard class name is added.
///
/// # Errors
///
/// Returns [`GraphStoreError`] on storage read failures.
pub fn shared_contract_candidate_ids<S: GraphStore>(
    graph: &S,
    node: &NodeData,
    shape: QueryShape,
) -> Result<Vec<NodeId>, GraphStoreError> {
    let mut candidate_ids = BTreeSet::new();

    if matches!(
        node.kind,
        NodeKind::SharedSymbol | NodeKind::Type | NodeKind::Function | NodeKind::Class
    ) {
        candidate_ids.insert(node.id);
    }

    // ── Edge-neighbour expansion (structural shared-contract edges) ──
    for edge in graph
        .get_outgoing(node.id)?
        .into_iter()
        .chain(graph.get_incoming(node.id)?)
    {
        if !matches!(
            edge.kind,
            EdgeKind::UsesShared
                | EdgeKind::UsesTypeFrom
                | EdgeKind::UsesGuardFrom
                | EdgeKind::ImplementsContractFrom
        ) {
            continue;
        }
        let other_id = if edge.source == node.id {
            edge.target
        } else {
            edge.source
        };
        let Some(other) = graph.get_node(other_id)? else {
            continue;
        };
        if matches!(
            other.kind,
            NodeKind::SharedSymbol | NodeKind::Type | NodeKind::Function | NodeKind::Class
        ) {
            candidate_ids.insert(other.id);
        }
    }

    // ── Virtual SharedSymbol / Type peers via the indexed short-name lookup
    //
    // The index covers virtual stubs only; real `SharedSymbol` / `Type`
    // declarations with the same short name are picked up by the
    // `nodes_by_type` scan below. Both are needed: the index gives O(1)
    // resolution for the virtual-bridge case (the common one for
    // cross-package shared contracts) and the kind scan preserves the
    // pre-existing same-name peer match for real contract declarations
    // (the case exercised by the
    // `shared_contract_match_scores_peer_consumers_against_candidate_repo`
    // test).
    //
    // Bare-function gate: for a `Function` anchor whose qualified name
    // does NOT have a `<Class>.<method>` shape, we skip virtual-peer
    // expansion. Plain functions of identical name in different
    // packages are independent — their cross-repo callers belong on
    // the upstream-caller side (handled by pack's planning-upstream
    // widener and impact's caller traversal), not on the
    // shared-contract-impact side. Without this gate a bare function
    // like `useAuthentication` would pull in every virtual hook stub
    // sharing its name and treat their consumer files as
    // shared-contract impact, breaking the
    // `function_impact_rollout` parity contract.
    let admit_peers = match node.kind {
        NodeKind::SharedSymbol | NodeKind::Type | NodeKind::Class => true,
        NodeKind::Function => has_class_method_qualified_name(node),
        _ => false,
    };
    if admit_peers && !node.name.is_empty() {
        for peer in graph.nodes_by_shared_symbol_name(&node.name)? {
            // Hook stubs are augmenter-generated consumer markers — not
            // contract definitions. Including them would cross-attribute
            // unrelated functions with the same short name.
            if peer
                .qualified_name
                .as_deref()
                .is_some_and(|qn| qn.starts_with("__hook__"))
            {
                continue;
            }
            candidate_ids.insert(peer.id);
        }
        for kind in [NodeKind::SharedSymbol, NodeKind::Type] {
            for peer in graph.nodes_by_type(kind)? {
                // The kind scan is reached only for real declarations
                // (virtual stubs are already covered above and would be
                // double-counted here, but `BTreeSet::insert` dedupes).
                // Hook stubs are excluded for the same reason as above.
                if peer
                    .qualified_name
                    .as_deref()
                    .is_some_and(|qn| qn.starts_with("__hook__"))
                {
                    continue;
                }
                if peer.name == node.name {
                    candidate_ids.insert(peer.id);
                }
            }
        }
    }

    // ── Event-shape anchor expansion ─────────────────────────────────
    if matches!(
        shape,
        QueryShape::EventRollout | QueryShape::GenericSymbolImpact
    ) {
        for edge in graph
            .get_outgoing(node.id)?
            .into_iter()
            .chain(graph.get_incoming(node.id)?)
        {
            if !matches!(
                edge.kind,
                EdgeKind::ProducesEventFor | EdgeKind::UsesEventFrom | EdgeKind::Publishes
            ) {
                continue;
            }
            let other_id = if edge.source == node.id {
                edge.target
            } else {
                edge.source
            };
            if let Some(other) = graph.get_node(other_id)?
                && matches!(
                    other.kind,
                    NodeKind::Topic
                        | NodeKind::Queue
                        | NodeKind::Subject
                        | NodeKind::Stream
                        | NodeKind::Event
                )
            {
                candidate_ids.insert(other.id);
            }
        }
    }

    // ── Route-shape anchor expansion ─────────────────────────────────
    if matches!(
        shape,
        QueryShape::RouteApiRollout | QueryShape::GenericSymbolImpact
    ) {
        for edge in graph
            .get_outgoing(node.id)?
            .into_iter()
            .chain(graph.get_incoming(node.id)?)
        {
            if !matches!(edge.kind, EdgeKind::ConsumesApiFrom | EdgeKind::Serves) {
                continue;
            }
            let other_id = if edge.source == node.id {
                edge.target
            } else {
                edge.source
            };
            if let Some(other) = graph.get_node(other_id)?
                && matches!(other.kind, NodeKind::Route)
            {
                candidate_ids.insert(other.id);
            }
        }
    }

    // ── Guard-shape expansion ────────────────────────────────────────
    if looks_like_guard_entrypoint(node) {
        candidate_ids.insert(gather_step_core::node_id(
            &node.repo,
            &node.file_path,
            NodeKind::File,
            &node.file_path,
        ));
        if let Some(guard_class_name) = guard_class_name_for_anchor(node) {
            // `SharedSymbol` peers come from the indexed short-name
            // lookup; real `Class` declarations still use the kind-table
            // scan since they are not virtual and so are not indexed.
            //
            // Hook stubs (`__hook__…`) must be excluded here for the same
            // reason as in the same-name peer scan above: they are
            // augmenter-generated consumer markers, not guard-class
            // declarations, and admitting them would cross-attribute hooks
            // sharing a short name with a real guard class.
            for peer in graph.nodes_by_shared_symbol_name(&guard_class_name)? {
                if peer
                    .qualified_name
                    .as_deref()
                    .is_some_and(|qn| qn.starts_with("__hook__"))
                {
                    continue;
                }
                if peer_matches_guard_class_name(&peer, &guard_class_name) {
                    candidate_ids.insert(peer.id);
                }
            }
            for peer in graph.nodes_by_type(NodeKind::Class)? {
                if peer_matches_guard_class_name(&peer, &guard_class_name) {
                    candidate_ids.insert(peer.id);
                }
            }
        }
    }

    Ok(candidate_ids.into_iter().collect())
}

/// Returns `true` when `peer` is a `Class` or virtual `SharedSymbol`
/// representing the guard class named `guard_class_name`.
///
/// Real `Class` nodes keep `name` equal to the declared class name, so
/// exact equality works. Virtual `SharedSymbol` nodes are canonicalised
/// by `graph_store::canonicalize_node` to have `name` equal to the full
/// qualified `__…__<GuardName>` external id — the trailing segment after
/// the last `__` is the guard class name. Matching on that suffix lets
/// the peer loop reach both the `__shared__…__UserAuthGuard` virtual
/// node emitted by the shared-lib path and the
/// `__guard__…__UserAuthGuard` virtual node emitted for cross-repo
/// `@UseGuards` imports.
#[must_use]
pub fn peer_matches_guard_class_name(peer: &NodeData, guard_class_name: &str) -> bool {
    if peer.name == guard_class_name {
        return true;
    }
    if peer.is_virtual {
        let last_segment = peer.name.rsplit("__").next().unwrap_or(peer.name.as_str());
        if last_segment == guard_class_name {
            return true;
        }
    }
    false
}

/// Extract the likely guard class name for a guard-shaped anchor.
///
/// Handles three shapes:
///   - a `Class` node named after the guard itself (e.g. `UserAuthGuard`)
///   - a method node whose qualified name is `<GuardClass>.canActivate`
///   - a `canActivate` function node with a qualified-name prefix
#[must_use]
pub fn guard_class_name_for_anchor(node: &NodeData) -> Option<String> {
    if matches!(node.kind, NodeKind::Class) {
        return Some(node.name.clone());
    }
    let qualified = node.qualified_name.as_deref()?;
    let trimmed = qualified.trim_end_matches(".canActivate");
    if trimmed != qualified && !trimmed.is_empty() {
        // `<GuardClass>.canActivate` — return the class segment.
        return Some(trimmed.rsplit(['.', ':']).next()?.to_owned());
    }
    None
}

/// Returns `true` when the anchor's qualified name has a
/// `<Class>.<method>` shape (e.g. `UserAuthGuard.canActivate`).
///
/// Used to gate the same-name peer expansion in
/// [`shared_contract_candidate_ids`] so bare `Function` anchors don't
/// pick up virtual peers that share their short name. Bare functions
/// of identical name across packages are independent symbols whose
/// cross-repo callers are caller-side evidence, not shared-contract
/// consumer evidence.
#[must_use]
pub fn has_class_method_qualified_name(node: &NodeData) -> bool {
    let Some(qualified) = node.qualified_name.as_deref() else {
        return false;
    };
    let trimmed = qualified.trim_end_matches(['(', ')']);
    let Some((prefix, method)) = trimmed.rsplit_once('.') else {
        return false;
    };
    if prefix.is_empty() || method.is_empty() {
        return false;
    }
    let class_segment = prefix.rsplit(['.', ':']).next().unwrap_or(prefix);
    class_segment
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_uppercase())
}

/// Heuristic: does this anchor look like a guard entry point?
///
/// Triggers when the file path, symbol name, or qualified name contains
/// "guard", or when the symbol name is `canActivate` (the conventional
/// `NestJS` guard method).
#[must_use]
pub fn looks_like_guard_entrypoint(node: &NodeData) -> bool {
    if matches!(node.kind, NodeKind::File) {
        return false;
    }
    let mut file_path = node.file_path.clone();
    file_path.make_ascii_lowercase();
    let mut symbol = node.name.clone();
    symbol.make_ascii_lowercase();
    let mut qualified = node.qualified_name.clone().unwrap_or_default();
    qualified.make_ascii_lowercase();
    file_path.contains("guard")
        || symbol.contains("guard")
        || symbol == "canactivate"
        || qualified.contains("guard")
        || qualified.ends_with(".canactivate")
}
