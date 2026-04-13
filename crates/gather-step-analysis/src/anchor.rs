/// Planning anchor ranking.
///
/// Scores candidate nodes by how well they serve as a planning anchor — the
/// ideal starting point for understanding a feature, tracing change impact,
/// or reviewing cross-service dependencies.
///
/// Scoring formula:
///
/// ```text
/// score = fan_out_contribution        (0.4 per cross-service incoming edge)
///       + boundary_bonus              (0.3 for Service; 0.3/0.25 for controller/usecase suffix)
///       + producer_consumer_contribution (0.3 per event-boundary incoming edge)
///       + shared_contract_bonus       (0.2 if node touches shared contracts)
/// ```
///
/// A node may collect multiple [`AnchorRationale`] entries — one per scoring
/// dimension that contributed a bonus.  If no dimension applies the single
/// `Local` rationale is returned.
use gather_step_core::{EdgeKind, NodeId, NodeKind};
use gather_step_storage::{GraphStore, GraphStoreError};

/// Rationale for why a node received a particular scoring bonus.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnchorRationale {
    /// Node is a shared contract definition that multiple repos depend on.
    SharedContract {
        /// Number of cross-service incoming edges counted during scoring.
        fan_out: usize,
    },
    /// Node is on a producer/consumer boundary (emits or receives events).
    ProducerConsumer {
        /// Number of producers connected to this node.
        producer_count: usize,
        /// Number of consumers connected to this node.
        consumer_count: usize,
    },
    /// Node is a controller or service that has significant downstream
    /// dependents.
    ControllerService {
        /// Number of direct downstream nodes (callees/consumers).
        downstream_nodes: usize,
    },
    /// Node has no notable cross-service characteristics — it is a local
    /// implementation detail.
    Local,
}

/// A candidate node with its computed anchor score and the list of rationales
/// that contributed to that score.
#[derive(Debug, Clone, PartialEq)]
pub struct RankedAnchor {
    /// The candidate node's ID.
    pub node: NodeId,
    /// Linear combination score. Higher is more useful as a planning anchor.
    pub score: f32,
    /// One entry per scoring dimension that produced a bonus.  Contains at
    /// least one entry (may be [`AnchorRationale::Local`] when nothing else
    /// applies).
    pub rationale: Vec<AnchorRationale>,
}

/// Edge kinds that count as cross-service fan-out.
const FAN_OUT_EDGE_KINDS: &[EdgeKind] = &[
    EdgeKind::CrossRepoDepends,
    EdgeKind::UsesTypeFrom,
    EdgeKind::ImplementsContractFrom,
    EdgeKind::ConsumesApiFrom,
    EdgeKind::UsesEventFrom,
];

/// Edge kinds that count as producer/consumer connections.
const PRODUCER_CONSUMER_EDGE_KINDS: &[EdgeKind] =
    &[EdgeKind::ProducesEventFor, EdgeKind::UsesEventFrom];

/// Rank a set of candidate nodes by their suitability as planning anchors.
///
/// The result is sorted descending by score.  Nodes with equal scores are
/// ordered by their raw `NodeId` bytes for deterministic output.
///
/// # Errors
///
/// Returns [`GraphStoreError`] on storage read failure.
pub fn rank_anchors<S: GraphStore>(
    store: &S,
    candidates: &[NodeId],
) -> Result<Vec<RankedAnchor>, GraphStoreError> {
    let mut ranked = candidates
        .iter()
        .map(|&node_id| score_candidate(store, node_id))
        .collect::<Result<Vec<_>, _>>()?;

    ranked.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.node.as_bytes().cmp(&right.node.as_bytes()))
    });

    Ok(ranked)
}

fn score_candidate<S: GraphStore>(
    store: &S,
    node_id: NodeId,
) -> Result<RankedAnchor, GraphStoreError> {
    let mut rank_score = 0.0_f32;
    let mut rationale = Vec::new();

    // Fetch the node once for kind and qualified-name checks.
    let node_data = store.get_node(node_id)?;
    let node_kind = node_data.as_ref().map(|n| n.kind);
    let mut node_qn = node_data
        .as_ref()
        .and_then(|n| n.qualified_name.as_deref())
        .unwrap_or("")
        .to_owned();
    node_qn.make_ascii_lowercase();
    let mut node_file_path = node_data
        .as_ref()
        .map_or("", |n| n.file_path.as_str())
        .to_owned();
    node_file_path.make_ascii_lowercase();
    let mut node_name = node_data
        .as_ref()
        .map_or("", |n| n.name.as_str())
        .to_owned();
    node_name.make_ascii_lowercase();

    let incoming = store.get_incoming(node_id)?;
    let outgoing = store.get_outgoing(node_id)?;

    // §3.1 Fan-out score.
    let fan_out = incoming
        .iter()
        .filter(|edge| FAN_OUT_EDGE_KINDS.contains(&edge.kind))
        .count();
    #[expect(
        clippy::cast_precision_loss,
        reason = "fan_out is a small count; precision loss is acceptable here"
    )]
    let fan_out_contribution = fan_out as f32 * 0.4;
    rank_score += fan_out_contribution;

    // §3.2 Boundary bonus.
    let boundary_bonus = boundary_bonus(node_kind, &node_name, &node_qn);
    rank_score += boundary_bonus;
    if boundary_bonus > 0.0 {
        let downstream = outgoing
            .iter()
            .filter(|edge| !matches!(edge.kind, EdgeKind::Defines | EdgeKind::Imports))
            .count();
        rationale.push(AnchorRationale::ControllerService {
            downstream_nodes: downstream,
        });
    }

    // §3.3 Producer/consumer score.
    // Count incoming edges where this node is a target of producer/consumer kinds,
    // AND outgoing edges where this node is the source (i.e. it actively consumes
    // from or produces events for another node).
    let pc_incoming = incoming
        .iter()
        .filter(|edge| PRODUCER_CONSUMER_EDGE_KINDS.contains(&edge.kind))
        .count();
    let pc_outgoing = outgoing
        .iter()
        .filter(|edge| PRODUCER_CONSUMER_EDGE_KINDS.contains(&edge.kind))
        .count();
    let pc_total = pc_incoming + pc_outgoing;
    #[expect(
        clippy::cast_precision_loss,
        reason = "pc_total is a small count; precision loss is acceptable here"
    )]
    let pc_contribution = pc_total as f32 * 0.3;
    rank_score += pc_contribution;
    if pc_total > 0 {
        // Producers: incoming ProducesEventFor (another node produces for this one)
        //           + outgoing ProducesEventFor (this node produces for another).
        let producer_count = incoming
            .iter()
            .filter(|edge| edge.kind == EdgeKind::ProducesEventFor)
            .count()
            + outgoing
                .iter()
                .filter(|edge| edge.kind == EdgeKind::ProducesEventFor)
                .count();
        // Consumers: incoming UsesEventFrom (another node uses events from this one)
        //           + outgoing UsesEventFrom (this node uses events from another).
        let consumer_count = incoming
            .iter()
            .filter(|edge| edge.kind == EdgeKind::UsesEventFrom)
            .count()
            + outgoing
                .iter()
                .filter(|edge| edge.kind == EdgeKind::UsesEventFrom)
                .count();
        rationale.push(AnchorRationale::ProducerConsumer {
            producer_count,
            consumer_count,
        });
    }

    // §3.4 Shared contract bonus.
    let has_implements_contract = incoming
        .iter()
        .any(|edge| edge.kind == EdgeKind::ImplementsContractFrom);
    let is_shared_path = node_file_path.contains("shared")
        || node_file_path.contains("contracts")
        || node_file_path.contains("shared-contracts")
        || node_file_path.contains("shared_contracts");
    if has_implements_contract || is_shared_path {
        rank_score += 0.2;
        rationale.push(AnchorRationale::SharedContract { fan_out });
    }

    // Default rationale when nothing else applied.
    if rationale.is_empty() {
        rationale.push(AnchorRationale::Local);
    }

    Ok(RankedAnchor {
        node: node_id,
        score: rank_score,
        rationale,
    })
}

/// Compute the boundary bonus for a node (§3.2).
///
/// `NodeKind::Service` gets 0.3.  For non-Service nodes the qualified name
/// suffix is checked (case-insensitive) for controller/usecase patterns.
fn boundary_bonus(kind: Option<NodeKind>, name: &str, qn: &str) -> f32 {
    if kind == Some(NodeKind::Service) {
        return 0.3;
    }
    let check = if qn.is_empty() { name } else { qn };
    if check.ends_with("controller") || check.ends_with(".controller") {
        0.3
    } else if check.ends_with("usecase")
        || check.ends_with(".usecase")
        || check.ends_with("use-case")
        || check.ends_with("use_case")
    {
        0.25
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use gather_step_core::{
        EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, SourceSpan, Visibility, node_id,
        virtual_node,
    };
    use gather_step_storage::GraphStore;

    use crate::test_utils::{TempDb, file_node};

    use super::{AnchorRationale, rank_anchors};

    fn symbol_node_with_qn(
        repo: &str,
        file_path: &str,
        name: &str,
        qn: &str,
        kind: NodeKind,
        ordinal: u16,
    ) -> NodeData {
        NodeData {
            id: node_id(repo, file_path, kind, name),
            kind,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(qn.to_owned()),
            external_id: None,
            signature: None,
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
    fn shared_contract_node_outranks_local_leaf() {
        let temp = TempDb::new("anchor", "contract-vs-local");
        let store = temp.open();

        let contract_file = file_node("shared_contracts", "src/types.ts");
        let local_file = file_node("feature_repo", "src/feature.ts");
        // Shared contract node: ImplementsContractFrom incoming edge.
        let contract = symbol_node_with_qn(
            "shared_contracts",
            "src/types.ts",
            "OrderDto",
            "shared_contracts::OrderDto",
            NodeKind::SharedSymbol,
            0,
        );
        // Local leaf: no cross-service edges.
        let local = symbol_node_with_qn(
            "feature_repo",
            "src/feature.ts",
            "localHelper",
            "feature_repo::localHelper",
            NodeKind::Function,
            1,
        );
        // A consumer of the shared contract.
        let consumer_file = file_node("feature_repo", "src/consumer.ts");
        let consumer = symbol_node_with_qn(
            "feature_repo",
            "src/consumer.ts",
            "OrderService",
            "feature_repo::OrderService",
            NodeKind::Class,
            0,
        );

        store
            .bulk_insert(
                &[
                    contract_file.clone(),
                    local_file.clone(),
                    consumer_file.clone(),
                    contract.clone(),
                    local.clone(),
                    consumer.clone(),
                ],
                &[EdgeData {
                    source: consumer.id,
                    target: contract.id,
                    kind: EdgeKind::ImplementsContractFrom,
                    metadata: EdgeMetadata::default(),
                    owner_file: consumer_file.id,
                    is_cross_file: true,
                }],
            )
            .expect("insert should succeed");

        let ranked =
            rank_anchors(&store, &[contract.id, local.id]).expect("ranking should succeed");
        assert_eq!(ranked.len(), 2);
        assert_eq!(
            ranked[0].node, contract.id,
            "shared contract should rank first"
        );
        assert!(
            ranked[0].score > ranked[1].score,
            "contract score {} must exceed local score {}",
            ranked[0].score,
            ranked[1].score
        );
    }

    #[test]
    fn service_node_receives_boundary_bonus() {
        let temp = TempDb::new("anchor", "service-bonus");
        let store = temp.open();

        let file = file_node("backend_standard", "src/order.service.ts");
        let service = symbol_node_with_qn(
            "backend_standard",
            "src/order.service.ts",
            "OrderService",
            "backend_standard::OrderService",
            NodeKind::Service,
            0,
        );
        store
            .bulk_insert(&[file, service.clone()], &[])
            .expect("insert should succeed");

        let ranked = rank_anchors(&store, &[service.id]).expect("ranking should succeed");
        assert_eq!(ranked.len(), 1);
        assert!(
            ranked[0].score >= 0.3,
            "Service node must have at least boundary bonus 0.3, got {}",
            ranked[0].score
        );
        assert!(
            ranked[0]
                .rationale
                .iter()
                .any(|r| matches!(r, AnchorRationale::ControllerService { .. })),
            "Service node must have ControllerService rationale"
        );
    }

    #[test]
    fn node_with_producer_consumer_and_shared_contract_gets_both_rationales() {
        let temp = TempDb::new("anchor", "multi-rationale");
        let store = temp.open();

        let event_file = file_node("backend_standard", "src/events.ts");
        // The candidate: an event handler with both shared-contract path and
        // producer/consumer edges.
        let handler = symbol_node_with_qn(
            "backend_standard",
            "src/events.ts",
            "OrderEventHandler",
            "backend_standard::OrderEventHandler",
            NodeKind::Class,
            0,
        );
        // A virtual event node that the handler listens to (UsesEventFrom).
        let event_node = virtual_node(
            NodeKind::Event,
            "backend_standard",
            "src/events.ts",
            "order.placed",
            "__event__kafka__order.placed",
        );
        // Another class that ImplementsContractFrom the handler (gives shared
        // contract bonus).
        let impl_file = file_node("shared_contracts", "src/shared.ts");
        let impl_node = symbol_node_with_qn(
            "shared_contracts",
            "src/shared.ts",
            "IOrderEventHandler",
            "shared_contracts::IOrderEventHandler",
            NodeKind::Class,
            0,
        );

        store
            .bulk_insert(
                &[
                    event_file.clone(),
                    impl_file.clone(),
                    handler.clone(),
                    event_node.clone(),
                    impl_node.clone(),
                ],
                &[
                    // UsesEventFrom → ProducerConsumer rationale
                    EdgeData {
                        source: handler.id,
                        target: event_node.id,
                        kind: EdgeKind::UsesEventFrom,
                        metadata: EdgeMetadata::default(),
                        owner_file: event_file.id,
                        is_cross_file: false,
                    },
                    // ImplementsContractFrom → SharedContract rationale
                    EdgeData {
                        source: impl_node.id,
                        target: handler.id,
                        kind: EdgeKind::ImplementsContractFrom,
                        metadata: EdgeMetadata::default(),
                        owner_file: impl_file.id,
                        is_cross_file: true,
                    },
                ],
            )
            .expect("insert should succeed");

        let ranked = rank_anchors(&store, &[handler.id]).expect("ranking should succeed");
        assert_eq!(ranked.len(), 1);
        let anchor = &ranked[0];
        let has_producer_consumer = anchor
            .rationale
            .iter()
            .any(|r| matches!(r, AnchorRationale::ProducerConsumer { .. }));
        let has_shared_contract = anchor
            .rationale
            .iter()
            .any(|r| matches!(r, AnchorRationale::SharedContract { .. }));
        assert!(
            has_producer_consumer,
            "expected ProducerConsumer rationale, got: {:?}",
            anchor.rationale
        );
        assert!(
            has_shared_contract,
            "expected SharedContract rationale, got: {:?}",
            anchor.rationale
        );
        assert_eq!(
            anchor.rationale.len(),
            2,
            "expected exactly 2 rationale entries, got {:?}",
            anchor.rationale
        );
    }
}
