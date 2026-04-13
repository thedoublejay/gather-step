use std::collections::{BTreeMap, BTreeSet, VecDeque};

use gather_step_core::{
    EdgeKind, NodeData, NodeId, NodeKind, PlanningProof, ProofHop, ProofKind, node_id,
    proof_sort_key,
};
use gather_step_storage::{GraphStore, GraphStoreError};
use thiserror::Error;

use crate::{
    EventTopologyError, ImpactError, TopologyMatch, canonical_event_target_for_node,
    shared_contract::has_class_method_qualified_name, shared_contract_candidate_ids,
    shared_contract_impact, trace_event, trace_route,
};

/// Maximum per-repo proof count in the output. Only the highest-strength
/// proofs for each target repo are kept after provider output is merged.
pub const MAX_PROOFS_PER_REPO: usize = 2;

#[derive(Debug, Error)]
pub enum ProofEngineError {
    #[error(transparent)]
    Graph(#[from] GraphStoreError),
    #[error(transparent)]
    EventTopology(#[from] EventTopologyError),
    #[error(transparent)]
    Impact(#[from] ImpactError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProofEngineOptions {
    /// Include caller/importer discovery through virtual shared-symbol peers.
    ///
    /// Planning packs enable this so a concrete declaration can still show
    /// consumers whose resolver landed on the package-level virtual peer.
    pub include_shared_peer_callers: bool,
    pub traversal_depth: usize,
    pub traversal_limit: usize,
}

impl Default for ProofEngineOptions {
    fn default() -> Self {
        Self {
            include_shared_peer_callers: true,
            traversal_depth: 1,
            traversal_limit: 50,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProofCaller {
    pub depth: usize,
    pub edge_kind: EdgeKind,
    pub node: NodeData,
}

#[derive(Debug, Clone)]
pub struct ProofEngineOutput {
    pub proofs: Vec<PlanningProof>,
    pub confirmed_downstream_repos: Vec<String>,
    pub probable_downstream_repos: Vec<String>,
    /// Additional real caller/importer nodes discovered while resolving proof
    /// providers. MCP uses these as pack items; repo confidence is still
    /// derived only from `proofs`.
    pub supplemental_callers: Vec<ProofCaller>,
}

/// Strength assigned to each proof kind.
///
/// Invariants checked by tests:
/// - `CoChangeAdvisory` < 33
/// - `ImportBridge` in 33-67
/// - all other kinds >= 67
#[must_use]
pub fn proof_strength(kind: ProofKind) -> u8 {
    match kind {
        ProofKind::DirectCall => 85,
        ProofKind::EventProducerConsumer | ProofKind::GuardUsage => 80,
        ProofKind::SharedContractConsumer => 75,
        ProofKind::RouteClientServer => 70,
        ProofKind::ImportBridge => 55,
        ProofKind::CoChangeAdvisory => 25,
        _ => 67,
    }
}

/// Build all pack-facing [`PlanningProof`] records for `anchor_id`.
///
/// The engine owns all proof providers used by context packs: real graph
/// cross-repo edges, shared-symbol peer callers, shared-contract impact,
/// event topology, frontend-hook bridges, and route topology. Callers should
/// treat the returned `proofs` as canonical and derive downstream repo fields
/// from them instead of maintaining separate confidence paths.
pub fn build_pack_proofs<S: GraphStore>(
    graph: &S,
    anchor_id: NodeId,
    anchor_repo: &str,
    options: ProofEngineOptions,
) -> Result<ProofEngineOutput, ProofEngineError> {
    let Some(anchor) = graph.get_node(anchor_id)? else {
        return Ok(empty_output());
    };

    let mut proofs = base_graph_proofs(graph, anchor_id, anchor_repo)?;
    let supplemental_callers = if options.include_shared_peer_callers {
        shared_peer_callers(
            graph,
            &anchor,
            options.traversal_depth,
            options.traversal_limit,
        )?
    } else {
        Vec::new()
    };
    proofs.extend(shared_peer_caller_proofs(
        &anchor,
        anchor_repo,
        &supplemental_callers,
    ));
    proofs.extend(shared_contract_impact_proofs(graph, &anchor)?);
    proofs.extend(event_trace_proofs(graph, &anchor)?);
    proofs.extend(hook_trace_proofs(graph, &anchor)?);
    proofs.extend(route_trace_proofs(graph, &anchor)?);

    let proofs = finalize_proofs(proofs);
    let (confirmed_downstream_repos, probable_downstream_repos) = derive_repo_sets(&proofs, None);
    Ok(ProofEngineOutput {
        proofs,
        confirmed_downstream_repos,
        probable_downstream_repos,
        supplemental_callers,
    })
}

fn empty_output() -> ProofEngineOutput {
    ProofEngineOutput {
        proofs: Vec::new(),
        confirmed_downstream_repos: Vec::new(),
        probable_downstream_repos: Vec::new(),
        supplemental_callers: Vec::new(),
    }
}

fn edge_to_proof_kind(edge: EdgeKind) -> Option<ProofKind> {
    match edge {
        EdgeKind::Calls => Some(ProofKind::DirectCall),
        EdgeKind::ProducesEventFor | EdgeKind::UsesEventFrom => {
            Some(ProofKind::EventProducerConsumer)
        }
        EdgeKind::UsesGuardFrom => Some(ProofKind::GuardUsage),
        EdgeKind::UsesTypeFrom
        | EdgeKind::ImplementsContractFrom
        | EdgeKind::ContractOn
        | EdgeKind::UsesShared => Some(ProofKind::SharedContractConsumer),
        EdgeKind::ConsumesApiFrom | EdgeKind::Serves => Some(ProofKind::RouteClientServer),
        EdgeKind::Imports | EdgeKind::ConsumesHookFrom => Some(ProofKind::ImportBridge),
        EdgeKind::CoChangesWith => Some(ProofKind::CoChangeAdvisory),
        _ => None,
    }
}

fn base_graph_proofs<S: GraphStore>(
    graph: &S,
    anchor_id: NodeId,
    anchor_repo: &str,
) -> Result<Vec<PlanningProof>, ProofEngineError> {
    let mut proofs = Vec::new();
    let mut visited = BTreeSet::<NodeId>::new();
    visited.insert(anchor_id);
    let mut queue = VecDeque::from([(anchor_id, Vec::<ProofHop>::new())]);

    while let Some((current_id, path_so_far)) = queue.pop_front() {
        let mut all_edges = graph
            .get_outgoing(current_id)?
            .into_iter()
            .chain(graph.get_incoming(current_id)?)
            .collect::<Vec<_>>();
        all_edges.sort_by_key(|edge| {
            (
                edge.kind.as_u8(),
                edge.source.as_bytes(),
                edge.target.as_bytes(),
                edge.owner_file.as_bytes(),
            )
        });

        for edge in all_edges {
            let neighbor_id = if edge.source == current_id {
                edge.target
            } else {
                edge.source
            };
            let Some(neighbor) = graph.get_node(neighbor_id)? else {
                continue;
            };

            if neighbor.is_virtual {
                continue;
            }

            if neighbor.repo == anchor_repo {
                if visited.insert(neighbor_id) && path_so_far.len() < PlanningProof::MAX_PATH_HOPS {
                    let mut next_path = path_so_far.clone();
                    next_path.push(ProofHop {
                        node_id: neighbor_id,
                        edge_kind: edge.kind,
                        repo: neighbor.repo.clone(),
                    });
                    queue.push_back((neighbor_id, next_path));
                }
                continue;
            }

            let Some(kind) = edge_to_proof_kind(edge.kind) else {
                continue;
            };
            let source_file = graph
                .get_node(edge.owner_file)?
                .filter(|node| node.kind == NodeKind::File)
                .map(|node| node.file_path)
                .unwrap_or_default();
            let mut path = path_so_far.clone();
            let path_truncated = path.len() >= PlanningProof::MAX_PATH_HOPS;
            if !path_truncated {
                path.push(ProofHop {
                    node_id: neighbor_id,
                    edge_kind: edge.kind,
                    repo: neighbor.repo.clone(),
                });
            }
            proofs.push(PlanningProof {
                kind,
                strength: proof_strength(kind),
                source_repo: anchor_repo.to_owned(),
                target_repo: neighbor.repo.clone(),
                source_file,
                target_file: neighbor.file_path.clone(),
                edge_kinds: std::iter::once(edge.kind).collect(),
                path,
                path_truncated,
            });
        }
    }

    Ok(proofs)
}

fn shared_peer_callers<S: GraphStore>(
    graph: &S,
    anchor: &NodeData,
    traversal_depth: usize,
    traversal_limit: usize,
) -> Result<Vec<ProofCaller>, ProofEngineError> {
    if !matches!(anchor.kind, NodeKind::Function | NodeKind::Class) || anchor.name.is_empty() {
        return Ok(Vec::new());
    }

    let shared_peers = graph.nodes_by_shared_symbol_name(&anchor.name)?;
    let mut callers = Vec::new();
    let mut seen = BTreeSet::<NodeId>::from([anchor.id]);

    for peer in shared_peers {
        if !peer.is_virtual || peer.id == anchor.id {
            continue;
        }
        callers.extend(callers_by_incoming_calls(
            graph,
            peer.id,
            traversal_depth,
            traversal_limit.saturating_sub(callers.len()),
            &mut seen,
        )?);
        if callers.len() >= traversal_limit {
            break;
        }

        for edge in graph.get_incoming(peer.id)? {
            if !matches!(
                edge.kind,
                EdgeKind::References
                    | EdgeKind::UsesShared
                    | EdgeKind::UsesTypeFrom
                    | EdgeKind::ImplementsContractFrom
            ) {
                continue;
            }
            let Some(source) = graph.get_node(edge.source)? else {
                continue;
            };
            if source.is_virtual || !seen.insert(source.id) {
                continue;
            }
            callers.push(ProofCaller {
                depth: traversal_depth.max(1),
                edge_kind: edge.kind,
                node: source,
            });
            if callers.len() >= traversal_limit {
                break;
            }
        }
    }

    callers.sort_by(|left, right| {
        left.depth
            .cmp(&right.depth)
            .then(left.node.repo.cmp(&right.node.repo))
            .then(left.node.file_path.cmp(&right.node.file_path))
            .then(
                left.node
                    .span
                    .as_ref()
                    .map(|span| span.line_start)
                    .cmp(&right.node.span.as_ref().map(|span| span.line_start)),
            )
            .then(left.node.name.cmp(&right.node.name))
            .then(left.node.id.as_bytes().cmp(&right.node.id.as_bytes()))
    });
    Ok(callers)
}

fn callers_by_incoming_calls<S: GraphStore>(
    graph: &S,
    start: NodeId,
    max_depth: usize,
    limit: usize,
    seen: &mut BTreeSet<NodeId>,
) -> Result<Vec<ProofCaller>, ProofEngineError> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    let max_depth = max_depth.clamp(1, 3);
    let mut queue = VecDeque::from([(start, 0_usize)]);
    let mut callers = Vec::new();
    while let Some((node_id, depth)) = queue.pop_front() {
        if depth >= max_depth || callers.len() >= limit {
            continue;
        }
        for edge in graph
            .get_incoming(node_id)?
            .into_iter()
            .filter(|edge| edge.kind == EdgeKind::Calls)
        {
            let next_id = edge.source;
            if !seen.insert(next_id) {
                continue;
            }
            let Some(node) = graph.get_node(next_id)? else {
                continue;
            };
            callers.push(ProofCaller {
                depth: depth + 1,
                edge_kind: EdgeKind::Calls,
                node: node.clone(),
            });
            if callers.len() >= limit {
                break;
            }
            queue.push_back((next_id, depth + 1));
        }
    }
    Ok(callers)
}

fn shared_peer_caller_proofs(
    anchor: &NodeData,
    anchor_repo: &str,
    callers: &[ProofCaller],
) -> Vec<PlanningProof> {
    callers
        .iter()
        .filter(|caller| {
            caller.edge_kind == EdgeKind::Calls
                && is_real_repo(&caller.node.repo)
                && caller.node.repo != anchor_repo
        })
        .map(|caller| PlanningProof {
            kind: ProofKind::DirectCall,
            strength: proof_strength(ProofKind::DirectCall),
            source_repo: anchor_repo.to_owned(),
            target_repo: caller.node.repo.clone(),
            source_file: anchor.file_path.clone(),
            target_file: caller.node.file_path.clone(),
            edge_kinds: std::iter::once(EdgeKind::Calls).collect(),
            path: vec![
                ProofHop {
                    node_id: anchor.id,
                    edge_kind: EdgeKind::Calls,
                    repo: anchor_repo.to_owned(),
                },
                ProofHop {
                    node_id: caller.node.id,
                    edge_kind: EdgeKind::Calls,
                    repo: caller.node.repo.clone(),
                },
            ],
            path_truncated: false,
        })
        .collect()
}

fn event_trace_proofs<S: GraphStore>(
    graph: &S,
    anchor: &NodeData,
) -> Result<Vec<PlanningProof>, ProofEngineError> {
    let mut targets = canonical_event_target_for_node(graph, anchor)?
        .map(|node| vec![node.id])
        .unwrap_or_default();
    if targets.is_empty() {
        targets = event_adjacent_targets(graph, anchor.id)?;
        targets.sort();
        targets.dedup();
    }
    if targets.is_empty() {
        targets = same_repo_event_context_targets(graph, anchor, 2)?;
    }
    if targets.is_empty() {
        return Ok(Vec::new());
    }

    let mut proofs = Vec::new();
    for event_id in targets {
        let Some(event_node) = graph.get_node(event_id)? else {
            continue;
        };
        let trace = trace_event(graph, event_id, 64)?;
        proofs.extend(trace.producers.iter().filter_map(|producer| {
            if !is_real_repo(&producer.repo) || producer.repo == anchor.repo {
                return None;
            }
            Some(PlanningProof {
                kind: ProofKind::EventProducerConsumer,
                strength: proof_strength(ProofKind::EventProducerConsumer),
                source_repo: event_node.repo.clone(),
                target_repo: producer.repo.clone(),
                source_file: event_node.file_path.clone(),
                target_file: producer.file_path.clone(),
                edge_kinds: std::iter::once(EdgeKind::ProducesEventFor).collect(),
                path: event_or_route_proof_path(&event_node, producer, EdgeKind::ProducesEventFor),
                path_truncated: false,
            })
        }));
        proofs.extend(trace.consumers.iter().filter_map(|consumer| {
            if !is_real_repo(&consumer.repo) || consumer.repo == anchor.repo {
                return None;
            }
            Some(PlanningProof {
                kind: ProofKind::EventProducerConsumer,
                strength: proof_strength(ProofKind::EventProducerConsumer),
                source_repo: event_node.repo.clone(),
                target_repo: consumer.repo.clone(),
                source_file: event_node.file_path.clone(),
                target_file: consumer.file_path.clone(),
                edge_kinds: std::iter::once(EdgeKind::UsesEventFrom).collect(),
                path: event_or_route_proof_path(&event_node, consumer, EdgeKind::UsesEventFrom),
                path_truncated: false,
            })
        }));
    }

    Ok(proofs)
}

fn hook_trace_proofs<S: GraphStore>(
    graph: &S,
    anchor: &NodeData,
) -> Result<Vec<PlanningProof>, ProofEngineError> {
    if !matches!(anchor.kind, NodeKind::Function) || anchor.name.is_empty() {
        return Ok(Vec::new());
    }
    let Some(after_use) = anchor.name.strip_prefix("use") else {
        return Ok(Vec::new());
    };
    if !after_use.chars().next().is_some_and(char::is_uppercase) {
        return Ok(Vec::new());
    }

    let shared_peers = graph.nodes_by_shared_symbol_name(&anchor.name)?;
    let mut proofs = Vec::new();
    let mut seen_repos = BTreeSet::<String>::new();

    for peer in shared_peers {
        if !peer.is_virtual {
            continue;
        }
        let Some(after_prefix) = peer.name.strip_prefix("__hook__") else {
            continue;
        };
        let Some((peer_package, peer_tail)) = after_prefix.rsplit_once("::") else {
            continue;
        };
        if peer_tail != anchor.name || !package_matches_anchor_repo(peer_package, &anchor.repo) {
            continue;
        }

        for edge in graph.get_incoming(peer.id)? {
            if edge.kind != EdgeKind::ConsumesHookFrom {
                continue;
            }
            let Some(consumer) = graph.get_node(edge.source)? else {
                continue;
            };
            if consumer.is_virtual
                || !is_real_repo(&consumer.repo)
                || consumer.repo == anchor.repo
                || !seen_repos.insert(consumer.repo.clone())
            {
                continue;
            }
            proofs.push(PlanningProof {
                kind: ProofKind::ImportBridge,
                strength: proof_strength(ProofKind::ImportBridge),
                source_repo: anchor.repo.clone(),
                target_repo: consumer.repo.clone(),
                source_file: anchor.file_path.clone(),
                target_file: consumer.file_path.clone(),
                edge_kinds: std::iter::once(EdgeKind::ConsumesHookFrom).collect(),
                path: vec![
                    ProofHop {
                        node_id: peer.id,
                        edge_kind: EdgeKind::ConsumesHookFrom,
                        repo: peer.repo.clone(),
                    },
                    ProofHop {
                        node_id: consumer.id,
                        edge_kind: EdgeKind::ConsumesHookFrom,
                        repo: consumer.repo.clone(),
                    },
                ],
                path_truncated: false,
            });
        }
    }

    Ok(proofs)
}

fn package_matches_anchor_repo(package: &str, anchor_repo: &str) -> bool {
    if package.is_empty() || anchor_repo.is_empty() {
        return false;
    }
    let mut package_norm = package.to_owned();
    package_norm.make_ascii_lowercase();
    let mut anchor_norm = anchor_repo.to_owned();
    anchor_norm.make_ascii_lowercase();

    let package_tail = package_norm
        .rsplit('/')
        .next()
        .unwrap_or(package_norm.as_str());
    let anchor_tail = anchor_norm
        .rsplit('/')
        .next()
        .unwrap_or(anchor_norm.as_str());

    package_tail == anchor_tail
        || package_tail.contains(anchor_tail)
        || anchor_tail.contains(package_tail)
}

fn route_trace_proofs<S: GraphStore>(
    graph: &S,
    anchor: &NodeData,
) -> Result<Vec<PlanningProof>, ProofEngineError> {
    let targets = route_adjacent_targets(graph, anchor.id)?;
    if targets.is_empty() {
        return Ok(Vec::new());
    }

    let mut proofs = Vec::new();
    for route_id in targets {
        let Some(route_node) = graph.get_node(route_id)? else {
            continue;
        };
        let trace = trace_route(graph, route_id, 64)?;
        proofs.extend(trace.handlers.iter().filter_map(|handler| {
            if !is_real_repo(&handler.repo) || handler.repo == anchor.repo {
                return None;
            }
            Some(PlanningProof {
                kind: ProofKind::RouteClientServer,
                strength: proof_strength(ProofKind::RouteClientServer),
                source_repo: route_node.repo.clone(),
                target_repo: handler.repo.clone(),
                source_file: route_node.file_path.clone(),
                target_file: handler.file_path.clone(),
                edge_kinds: std::iter::once(EdgeKind::Serves).collect(),
                path: event_or_route_proof_path(&route_node, handler, EdgeKind::Serves),
                path_truncated: false,
            })
        }));
        proofs.extend(trace.callers.iter().filter_map(|caller| {
            if !is_real_repo(&caller.repo) || caller.repo == anchor.repo {
                return None;
            }
            Some(PlanningProof {
                kind: ProofKind::RouteClientServer,
                strength: proof_strength(ProofKind::RouteClientServer),
                source_repo: route_node.repo.clone(),
                target_repo: caller.repo.clone(),
                source_file: route_node.file_path.clone(),
                target_file: caller.file_path.clone(),
                edge_kinds: std::iter::once(caller.edge_kind).collect(),
                path: event_or_route_proof_path(&route_node, caller, caller.edge_kind),
                path_truncated: false,
            })
        }));
    }

    Ok(proofs)
}

fn shared_contract_impact_proofs<S: GraphStore>(
    graph: &S,
    anchor: &NodeData,
) -> Result<Vec<PlanningProof>, ProofEngineError> {
    let participates = match anchor.kind {
        NodeKind::SharedSymbol | NodeKind::Type | NodeKind::Class => true,
        NodeKind::Function => has_class_method_qualified_name(anchor),
        _ => false,
    };
    if !participates {
        return Ok(Vec::new());
    }

    let candidate_ids =
        shared_contract_candidate_ids(graph, anchor, crate::QueryShape::SharedTypeRollout)?;
    let mut proofs = Vec::new();
    let mut seen = BTreeSet::<(String, String, EdgeKind)>::new();
    for id in candidate_ids {
        let Some(target) = graph.get_node(id)? else {
            continue;
        };
        emit_shared_contract_proofs(graph, anchor, &target, &mut seen, &mut proofs)?;
    }
    Ok(proofs)
}

fn emit_shared_contract_proofs<S: GraphStore>(
    graph: &S,
    anchor: &NodeData,
    target: &NodeData,
    seen: &mut BTreeSet<(String, String, EdgeKind)>,
    proofs: &mut Vec<PlanningProof>,
) -> Result<(), ProofEngineError> {
    let impact = shared_contract_impact(graph, target.id)?;
    for (repo, files) in impact.entries {
        if !is_real_repo(&repo) || repo == anchor.repo {
            continue;
        }
        for impacted in files {
            let impacted_file_path = impacted.file_path;
            let edge_kinds = impacted.edge_kinds;
            let is_advisory = edge_kinds
                .iter()
                .all(|kind| matches!(kind, EdgeKind::CoChangesWith));
            let kind = if is_advisory {
                ProofKind::CoChangeAdvisory
            } else {
                ProofKind::SharedContractConsumer
            };
            let edge_kind = edge_kinds
                .first()
                .copied()
                .unwrap_or(EdgeKind::UsesTypeFrom);
            if !seen.insert((repo.clone(), impacted_file_path.clone(), edge_kind)) {
                continue;
            }
            proofs.push(PlanningProof {
                kind,
                strength: proof_strength(kind),
                source_repo: anchor.repo.clone(),
                target_repo: repo.clone(),
                source_file: anchor.file_path.clone(),
                target_file: impacted_file_path.clone(),
                edge_kinds: edge_kinds.into_iter().collect(),
                path: vec![
                    ProofHop {
                        node_id: target.id,
                        edge_kind,
                        repo: target.repo.clone(),
                    },
                    ProofHop {
                        node_id: node_id(
                            &repo,
                            &impacted_file_path,
                            NodeKind::File,
                            &impacted_file_path,
                        ),
                        edge_kind,
                        repo: repo.clone(),
                    },
                ],
                path_truncated: false,
            });
        }
    }
    Ok(())
}

fn event_or_route_proof_path(
    virtual_node: &NodeData,
    target: &TopologyMatch,
    edge_kind: EdgeKind,
) -> Vec<ProofHop> {
    vec![
        ProofHop {
            node_id: virtual_node.id,
            edge_kind,
            repo: virtual_node.repo.clone(),
        },
        ProofHop {
            node_id: target.node_id,
            edge_kind,
            repo: target.repo.clone(),
        },
    ]
}

fn route_adjacent_targets<S: GraphStore>(
    graph: &S,
    anchor_id: NodeId,
) -> Result<Vec<NodeId>, ProofEngineError> {
    let mut targets = Vec::new();
    let Some(anchor) = graph.get_node(anchor_id)? else {
        return Ok(targets);
    };
    if anchor.kind == NodeKind::Route {
        targets.push(anchor.id);
    }

    for edge in graph
        .get_outgoing(anchor_id)?
        .into_iter()
        .chain(graph.get_incoming(anchor_id)?)
    {
        let other_id = if edge.source == anchor_id {
            edge.target
        } else {
            edge.source
        };
        let Some(other) = graph.get_node(other_id)? else {
            continue;
        };
        if other.is_virtual && other.kind == NodeKind::Route {
            targets.push(other.id);
        }
    }

    targets.sort();
    targets.dedup();
    Ok(targets)
}

#[must_use]
pub fn is_real_repo(repo: &str) -> bool {
    !repo.is_empty() && repo != "__virtual__" && !repo.starts_with("__")
}

pub fn event_adjacent_targets<S: GraphStore>(
    graph: &S,
    anchor_id: NodeId,
) -> Result<Vec<NodeId>, ProofEngineError> {
    let mut targets = Vec::new();
    let Some(anchor) = graph.get_node(anchor_id)? else {
        return Ok(targets);
    };
    if is_eventish_kind(anchor.kind) {
        targets.push(anchor.id);
        push_messaging_sibling(graph, &anchor, &mut targets)?;
    }

    for edge in graph
        .get_outgoing(anchor_id)?
        .into_iter()
        .chain(graph.get_incoming(anchor_id)?)
    {
        let other_id = if edge.source == anchor_id {
            edge.target
        } else {
            edge.source
        };
        let Some(other) = graph.get_node(other_id)? else {
            continue;
        };
        if other.is_virtual && is_eventish_kind(other.kind) {
            targets.push(other.id);
            push_messaging_sibling(graph, &other, &mut targets)?;
        }
    }

    Ok(targets)
}

/// Lift event context through a short same-repo caller chain.
pub fn same_repo_event_context_targets<S: GraphStore>(
    graph: &S,
    anchor: &NodeData,
    max_depth: usize,
) -> Result<Vec<NodeId>, ProofEngineError> {
    let mut targets = Vec::new();
    let mut seen = BTreeSet::from([anchor.id]);
    let mut queue = VecDeque::from([(anchor.id, 0_usize)]);

    while let Some((current_id, depth)) = queue.pop_front() {
        if depth >= max_depth {
            continue;
        }
        for edge in graph.get_incoming(current_id)? {
            if edge.kind != EdgeKind::Calls {
                continue;
            }
            let Some(caller) = graph.get_node(edge.source)? else {
                continue;
            };
            if caller.is_virtual || caller.repo != anchor.repo || !seen.insert(caller.id) {
                continue;
            }
            targets.extend(event_adjacent_targets(graph, caller.id)?);
            queue.push_back((caller.id, depth + 1));
        }
    }

    targets.sort();
    targets.dedup();
    Ok(targets)
}

fn push_messaging_sibling<S: GraphStore>(
    graph: &S,
    node: &NodeData,
    targets: &mut Vec<NodeId>,
) -> Result<(), ProofEngineError> {
    if !node.is_virtual {
        return Ok(());
    }
    let Some(external_id) = node.external_id.as_deref() else {
        return Ok(());
    };
    let (sibling_kind, sibling_qn) = if let Some(rest) = external_id.strip_prefix("__topic__") {
        (NodeKind::Event, format!("__event__{rest}"))
    } else if let Some(rest) = external_id.strip_prefix("__event__") {
        (NodeKind::Topic, format!("__topic__{rest}"))
    } else {
        return Ok(());
    };
    if let Some(sibling) = graph
        .nodes_by_external_id(sibling_kind, &sibling_qn)?
        .into_iter()
        .find(|candidate| candidate.is_virtual)
    {
        targets.push(sibling.id);
    }
    Ok(())
}

#[must_use]
pub fn is_eventish_kind(kind: NodeKind) -> bool {
    matches!(
        kind,
        NodeKind::Event | NodeKind::Topic | NodeKind::Queue | NodeKind::Subject | NodeKind::Stream
    )
}

#[must_use]
pub fn finalize_proofs(proofs: Vec<PlanningProof>) -> Vec<PlanningProof> {
    let mut by_key = BTreeMap::<(String, String, String, String, ProofKind), PlanningProof>::new();
    for proof in proofs {
        if !is_real_repo(&proof.target_repo) {
            continue;
        }
        let key = (
            proof.source_repo.clone(),
            proof.source_file.clone(),
            proof.target_repo.clone(),
            proof.target_file.clone(),
            proof.kind,
        );
        match by_key.get_mut(&key) {
            Some(existing) => merge_proof(existing, proof),
            None => {
                by_key.insert(key, proof);
            }
        }
    }

    let mut all = by_key.into_values().collect::<Vec<_>>();
    all.sort_by(|left, right| proof_sort_key(left).cmp(&proof_sort_key(right)));

    let mut per_repo = BTreeMap::<String, usize>::new();
    all.into_iter()
        .filter(|proof| {
            let count = per_repo.entry(proof.target_repo.clone()).or_default();
            if *count >= MAX_PROOFS_PER_REPO {
                return false;
            }
            *count += 1;
            true
        })
        .collect()
}

fn merge_proof(existing: &mut PlanningProof, candidate: PlanningProof) {
    let mut edge_kinds = existing.edge_kinds.clone();
    for edge_kind in &candidate.edge_kinds {
        if !edge_kinds.contains(edge_kind) {
            edge_kinds.push(*edge_kind);
        }
    }
    edge_kinds.sort_by_key(|kind| kind.as_u8());

    if candidate.strength > existing.strength
        || (candidate.strength == existing.strength
            && proof_tie_key(&candidate) < proof_tie_key(existing))
    {
        *existing = candidate;
    }
    existing.edge_kinds = edge_kinds;
}

type ProofTieKey = (String, String, Vec<(String, [u8; 16], u8)>, bool);

fn proof_tie_key(proof: &PlanningProof) -> ProofTieKey {
    let path = proof
        .path
        .iter()
        .map(|hop| {
            (
                hop.repo.clone(),
                hop.node_id.as_bytes(),
                hop.edge_kind.as_u8(),
            )
        })
        .collect::<Vec<_>>();
    (
        proof.target_file.clone(),
        proof.source_file.clone(),
        path,
        proof.path_truncated,
    )
}

#[must_use]
pub fn derive_repo_sets(
    proofs: &[PlanningProof],
    repo_filter: Option<&str>,
) -> (Vec<String>, Vec<String>) {
    let mut confirmed = BTreeSet::<String>::new();
    let mut probable = BTreeSet::<String>::new();
    for proof in proofs
        .iter()
        .filter(|proof| repo_filter.is_none_or(|selected| proof.target_repo == selected))
    {
        if proof.is_structural() {
            confirmed.insert(proof.target_repo.clone());
        } else if proof.is_advisory() {
            probable.insert(proof.target_repo.clone());
        }
    }
    probable.retain(|repo| !confirmed.contains(repo));
    (
        confirmed.into_iter().collect(),
        probable.into_iter().collect(),
    )
}

#[cfg(test)]
mod tests {
    use gather_step_core::{
        EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeId, NodeKind, PlanningProof, ProofHop,
        ProofKind, Visibility, node_id,
    };
    use gather_step_storage::GraphStore;

    use crate::test_utils::{TempDb, file_node, symbol_node};

    use super::{
        ProofEngineOptions, build_pack_proofs, derive_repo_sets, event_adjacent_targets,
        finalize_proofs, is_eventish_kind,
    };

    fn edge(source: NodeId, target: NodeId, kind: EdgeKind, owner_file: NodeId) -> EdgeData {
        EdgeData {
            source,
            target,
            kind,
            owner_file,
            is_cross_file: false,
            metadata: EdgeMetadata::default(),
        }
    }

    fn virtual_shared_symbol(name: &str) -> NodeData {
        let external_id = format!("__shared__@workspace/shared_contracts__{name}");
        NodeData {
            id: node_id(
                "__virtual__",
                &external_id,
                NodeKind::SharedSymbol,
                &external_id,
            ),
            kind: NodeKind::SharedSymbol,
            repo: "__virtual__".to_owned(),
            file_path: external_id.clone(),
            name: external_id.clone(),
            qualified_name: Some(external_id.clone()),
            external_id: Some(external_id),
            signature: None,
            visibility: Some(Visibility::Public),
            span: None,
            is_virtual: true,
        }
    }

    fn virtual_messaging_node(kind: NodeKind, external_id: &str) -> NodeData {
        NodeData {
            id: node_id("__virtual__", external_id, kind, external_id),
            kind,
            repo: "__virtual__".to_owned(),
            file_path: external_id.to_owned(),
            name: external_id.to_owned(),
            qualified_name: Some(external_id.to_owned()),
            external_id: Some(external_id.to_owned()),
            signature: None,
            visibility: Some(Visibility::Public),
            span: None,
            is_virtual: true,
        }
    }

    fn planning_proof_with_edge(edge_kind: EdgeKind, target_file: &str) -> PlanningProof {
        PlanningProof {
            kind: ProofKind::EventProducerConsumer,
            strength: super::proof_strength(ProofKind::EventProducerConsumer),
            source_repo: "shared_contracts".to_owned(),
            target_repo: "backend_standard".to_owned(),
            source_file: "src/source.ts".to_owned(),
            target_file: target_file.to_owned(),
            edge_kinds: std::iter::once(edge_kind).collect(),
            path: vec![ProofHop {
                node_id: node_id(
                    "backend_standard",
                    target_file,
                    NodeKind::Function,
                    target_file,
                ),
                edge_kind,
                repo: "backend_standard".to_owned(),
            }],
            path_truncated: false,
        }
    }

    #[test]
    fn finalize_proofs_keeps_different_target_files_separate() {
        let proofs = finalize_proofs(vec![
            planning_proof_with_edge(EdgeKind::ProducesEventFor, "src/producer.ts"),
            planning_proof_with_edge(EdgeKind::UsesEventFrom, "src/consumer.ts"),
        ]);

        assert_eq!(proofs.len(), 2);
        assert!(proofs.iter().any(|proof| {
            proof.target_file == "src/producer.ts"
                && proof.edge_kinds.as_slice() == [EdgeKind::ProducesEventFor]
        }));
        assert!(proofs.iter().any(|proof| {
            proof.target_file == "src/consumer.ts"
                && proof.edge_kinds.as_slice() == [EdgeKind::UsesEventFrom]
        }));
    }

    #[test]
    fn finalize_proofs_merges_edge_kinds_for_same_evidence_file() {
        let proofs = finalize_proofs(vec![
            planning_proof_with_edge(EdgeKind::ProducesEventFor, "src/consumer.ts"),
            planning_proof_with_edge(EdgeKind::UsesEventFrom, "src/consumer.ts"),
        ]);

        assert_eq!(proofs.len(), 1);
        assert!(proofs[0].edge_kinds.contains(&EdgeKind::ProducesEventFor));
        assert!(proofs[0].edge_kinds.contains(&EdgeKind::UsesEventFrom));
        assert_eq!(proofs[0].target_file, "src/consumer.ts");
    }

    #[test]
    fn derive_repo_sets_separates_structural_and_advisory_in_one_pass() {
        let structural = planning_proof_with_edge(EdgeKind::UsesEventFrom, "src/consumer.ts");
        let mut advisory = planning_proof_with_edge(EdgeKind::Calls, "src/payment.ts");
        advisory.kind = ProofKind::CoChangeAdvisory;
        advisory.strength = 20;
        advisory.target_repo = "payments_standard".to_owned();
        advisory.target_file = "src/payment.ts".to_owned();
        let mut duplicate_advisory = advisory.clone();
        duplicate_advisory.target_repo = "backend_standard".to_owned();

        let (confirmed, probable) =
            derive_repo_sets(&[structural, advisory, duplicate_advisory], None);

        assert_eq!(confirmed, vec!["backend_standard".to_owned()]);
        assert_eq!(probable, vec!["payments_standard".to_owned()]);
    }

    #[test]
    fn event_adjacent_targets_includes_topic_event_sibling_both_directions() {
        let temp = TempDb::new("proofs", "event-topic-sibling");
        let store = temp.open();
        let topic = virtual_messaging_node(NodeKind::Topic, "__topic__kafka__reports.queue");
        let event = virtual_messaging_node(NodeKind::Event, "__event__kafka__reports.queue");
        store
            .bulk_insert(&[topic.clone(), event.clone()], &[])
            .expect("graph seed should succeed");

        let topic_targets =
            event_adjacent_targets(&store, topic.id).expect("topic lookup should succeed");
        assert_eq!(topic_targets, vec![topic.id, event.id]);

        let event_targets =
            event_adjacent_targets(&store, event.id).expect("event lookup should succeed");
        assert_eq!(event_targets, vec![event.id, topic.id]);
    }

    #[test]
    fn is_eventish_kind_covers_virtual_transport_nodes() {
        assert!(is_eventish_kind(NodeKind::Event));
        assert!(is_eventish_kind(NodeKind::Topic));
        assert!(is_eventish_kind(NodeKind::Queue));
        assert!(!is_eventish_kind(NodeKind::Function));
    }

    #[test]
    fn shared_peer_reference_callers_stay_supplemental_only() {
        let temp = TempDb::new("proofs", "shared-peer-reference");
        let store = temp.open();
        let anchor_file = file_node("frontend_standard", "src/auth_api.ts");
        let anchor = symbol_node(
            "frontend_standard",
            "src/auth_api.ts",
            "useAuthentication",
            1,
        );
        let consumer_file = file_node("backend_standard", "src/auth_consumer.ts");
        let peer = virtual_shared_symbol("useAuthentication");
        store
            .bulk_insert(
                &[
                    anchor_file.clone(),
                    anchor.clone(),
                    consumer_file.clone(),
                    peer.clone(),
                ],
                &[edge(
                    consumer_file.id,
                    peer.id,
                    EdgeKind::References,
                    consumer_file.id,
                )],
            )
            .expect("fixture should insert");

        let output = build_pack_proofs(
            &store,
            anchor.id,
            "frontend_standard",
            ProofEngineOptions {
                include_shared_peer_callers: true,
                traversal_depth: 1,
                traversal_limit: 20,
            },
        )
        .expect("proof engine should run");

        assert_eq!(output.supplemental_callers.len(), 1);
        assert_eq!(output.supplemental_callers[0].node.repo, "backend_standard");
        assert!(
            output.confirmed_downstream_repos.is_empty(),
            "weak shared-peer references should not become confirmed downstream repos"
        );
        assert!(
            output.proofs.is_empty(),
            "weak shared-peer references should feed caller projection, not planning proofs"
        );
    }
}
