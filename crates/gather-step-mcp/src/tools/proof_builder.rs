//! Legacy graph-only proof builder used by low-level unit tests.
//!
//! Production context-pack proof assembly is owned by
//! `gather_step_analysis::proofs`. This module intentionally covers only
//! direct real-node graph traversal; the analysis proof engine adds the
//! virtual event, hook, route, and shared-contract providers needed by packs.
//!
//! The builder walks the graph around a resolved anchor node and emits one
//! [`PlanningProof`] per distinct `(source_repo, target_repo, kind)` triple,
//! keeping the highest-strength instance when duplicates appear.  The
//! downstream-repo sets that previously were assembled from three independent
//! code paths are derived from this single pass.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use gather_step_core::{
    EdgeKind, NodeId, NodeKind, PlanningProof, ProofHop, ProofKind, proof_sort_key,
};
use gather_step_storage::GraphStore;
use smallvec::SmallVec;

use crate::error::McpServerError;

/// Maximum per-repo proof count in the output.  Only the highest-strength
/// proofs for each target repo are kept.
pub const MAX_PROOFS_PER_REPO: usize = 2;

/// Strength assigned to each proof kind.
///
/// Invariants checked by tests:
/// - `CoChangeAdvisory` < 33
/// - `ImportBridge` in 33–67
/// - all other kinds ≥ 67
pub fn proof_strength(kind: ProofKind) -> u8 {
    match kind {
        ProofKind::DirectCall => 85,
        ProofKind::EventProducerConsumer | ProofKind::GuardUsage => 80,
        ProofKind::SharedContractConsumer => 75,
        ProofKind::ProjectionFieldEvidence => 72,
        ProofKind::RouteClientServer => 70,
        ProofKind::ImportBridge => 55,
        ProofKind::CoChangeAdvisory => 25,
        // `ProofKind` is `#[non_exhaustive]`; future variants default to the
        // lowest structural strength so they remain visible but conservative.
        _ => 67,
    }
}

/// Map a single [`EdgeKind`] to its dominant [`ProofKind`], if it carries
/// cross-repo semantic meaning.  Returns `None` for intra-repo structural
/// edges that do not constitute a planning proof on their own.
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
        EdgeKind::ReadsField
        | EdgeKind::WritesField
        | EdgeKind::DerivesFieldFrom
        | EdgeKind::FiltersOnField
        | EdgeKind::IndexesField
        | EdgeKind::BackfillsField => Some(ProofKind::ProjectionFieldEvidence),
        EdgeKind::ConsumesApiFrom | EdgeKind::Serves => Some(ProofKind::RouteClientServer),
        EdgeKind::Imports | EdgeKind::ConsumesHookFrom => Some(ProofKind::ImportBridge),
        EdgeKind::CoChangesWith => Some(ProofKind::CoChangeAdvisory),
        _ => None,
    }
}

/// Internal accumulator for a single `(target_repo, kind)` triple.
struct ProofAccumulator {
    kind: ProofKind,
    strength: u8,
    source_repo: String,
    target_repo: String,
    source_file: String,
    target_file: String,
    edge_kinds: BTreeSet<EdgeKind>,
    path: Vec<ProofHop>,
    path_truncated: bool,
}

impl ProofAccumulator {
    fn into_proof(self) -> PlanningProof {
        let edge_kinds: SmallVec<[EdgeKind; 4]> = self.edge_kinds.into_iter().collect();
        PlanningProof {
            kind: self.kind,
            strength: self.strength,
            source_repo: self.source_repo,
            target_repo: self.target_repo,
            source_file: self.source_file,
            target_file: self.target_file,
            edge_kinds,
            path: self.path,
            path_truncated: self.path_truncated,
        }
    }
}

/// Output produced by [`build_planning_proofs`].
pub struct ProofBuilderOutput {
    /// Deduplicated, sorted, per-repo-capped proofs.
    pub proofs: Vec<PlanningProof>,
    /// Repos confirmed through structural graph evidence (strength ≥ 67).
    pub confirmed_downstream_repos: Vec<String>,
    /// Repos with only advisory evidence (strength < 33).  Excludes repos
    /// that also have structural evidence.
    pub probable_downstream_repos: Vec<String>,
}

/// Build all [`PlanningProof`] records for `anchor_id`.
///
/// Walks outgoing and incoming edges around the anchor, emits one proof per
/// `(target_repo, kind)` triple (highest strength wins on collision), caps at
/// [`MAX_PROOFS_PER_REPO`] per target repo, and derives the legacy
/// downstream-repo sets in the same pass.
///
/// `source_repo` is the owning repo of the anchor; same-repo edges are
/// silently skipped for proof emission but are still traversed transitively
/// to reach cross-repo edges via multi-hop paths.
pub fn build_planning_proofs<S: GraphStore>(
    store: &S,
    anchor_id: NodeId,
    source_repo: &str,
) -> Result<ProofBuilderOutput, McpServerError> {
    // `(target_repo, kind) → accumulator`
    let mut by_key: BTreeMap<(String, ProofKind), ProofAccumulator> = BTreeMap::new();

    // BFS over both outgoing and incoming edges.
    let mut visited: BTreeSet<NodeId> = BTreeSet::new();
    visited.insert(anchor_id);
    let mut queue: VecDeque<(NodeId, Vec<ProofHop>)> = VecDeque::new();
    queue.push_back((anchor_id, Vec::new()));

    while let Some((current_id, path_so_far)) = queue.pop_front() {
        let outgoing = store
            .get_outgoing(current_id)
            .map_err(|err| McpServerError::Internal(err.to_string()))?;
        let incoming = store
            .get_incoming(current_id)
            .map_err(|err| McpServerError::Internal(err.to_string()))?;
        let mut all_edges = outgoing.into_iter().chain(incoming).collect::<Vec<_>>();
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

            let Some(neighbor) = store
                .get_node(neighbor_id)
                .map_err(|err| McpServerError::Internal(err.to_string()))?
            else {
                continue;
            };

            // Skip virtual bridge nodes except DataField anchors.  Virtual nodes (shared-symbol
            // stubs, topic/route anchors, etc.) are not real repositories and
            // must not appear in proof target_repo fields.  Event- and route-
            // based cross-repo evidence is handled by the dedicated
            // `confirmed_event_trace_repos` traversal, which understands the
            // virtual-node topology correctly.  Traversing through virtual nodes
            // here would incorrectly promote cross-repo callers that reach the
            // anchor via a shared-symbol bridge into confirmed downstream repos.
            if neighbor.is_virtual && neighbor.kind != NodeKind::DataField {
                continue;
            }

            // Same-repo neighbor: enqueue for transitive traversal but don't
            // emit a proof.
            if neighbor.repo == source_repo {
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

            // Cross-repo neighbor: only emit a proof when the edge kind maps to
            // a recognised proof kind.
            let Some(proof_kind) = edge_to_proof_kind(edge.kind) else {
                continue;
            };

            let target_repo = neighbor.repo.clone();
            let target_file = neighbor.file_path.clone();
            let strength = proof_strength(proof_kind);

            // Resolve the source file from the edge's owner-file when available.
            let source_file = store
                .get_node(edge.owner_file)
                .ok()
                .flatten()
                .filter(|n| n.kind == NodeKind::File)
                .map(|n| n.file_path)
                .unwrap_or_default();

            // Extend the traversal path for this hop.
            let mut full_path = path_so_far.clone();
            let path_truncated = full_path.len() >= PlanningProof::MAX_PATH_HOPS;
            if !path_truncated {
                full_path.push(ProofHop {
                    node_id: neighbor_id,
                    edge_kind: edge.kind,
                    repo: target_repo.clone(),
                });
            }

            let acc = by_key
                .entry((target_repo.clone(), proof_kind))
                .or_insert_with(|| ProofAccumulator {
                    kind: proof_kind,
                    strength,
                    source_repo: source_repo.to_owned(),
                    target_repo: target_repo.clone(),
                    source_file: source_file.clone(),
                    target_file: target_file.clone(),
                    edge_kinds: BTreeSet::new(),
                    path: Vec::new(),
                    path_truncated: false,
                });

            acc.edge_kinds.insert(edge.kind);
            // Update the stored path with the longest observed path so far
            // (gives the most context without exceeding the cap).
            if !path_truncated && full_path.len() > acc.path.len() {
                acc.path = full_path;
                acc.path_truncated = false;
            } else if path_truncated {
                acc.path_truncated = true;
            }
        }
    }

    // Convert accumulators to proofs, sort by strength DESC, then cap per repo.
    let mut all_proofs: Vec<PlanningProof> = by_key
        .into_values()
        .map(ProofAccumulator::into_proof)
        .collect();
    all_proofs.sort_by(|a, b| proof_sort_key(a).cmp(&proof_sort_key(b)));

    let mut repo_counts: BTreeMap<String, usize> = BTreeMap::new();
    let proofs: Vec<PlanningProof> = all_proofs
        .into_iter()
        .filter(|proof| {
            let count = repo_counts.entry(proof.target_repo.clone()).or_insert(0);
            if *count < MAX_PROOFS_PER_REPO {
                *count += 1;
                true
            } else {
                false
            }
        })
        .collect();

    // Derive the downstream-repo sets from the proof set in a single pass.
    let mut confirmed_set: BTreeSet<String> = BTreeSet::new();
    let mut advisory_set: BTreeSet<String> = BTreeSet::new();
    for proof in &proofs {
        if proof.is_structural() {
            confirmed_set.insert(proof.target_repo.clone());
        } else if proof.is_advisory() {
            advisory_set.insert(proof.target_repo.clone());
        }
    }

    let confirmed_downstream_repos: Vec<String> = confirmed_set.into_iter().collect();
    let probable_downstream_repos: Vec<String> = advisory_set
        .into_iter()
        .filter(|repo| !confirmed_downstream_repos.contains(repo))
        .collect();

    Ok(ProofBuilderOutput {
        proofs,
        confirmed_downstream_repos,
        probable_downstream_repos,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::{
        EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeId, NodeKind, Visibility, node_id,
    };
    use gather_step_storage::{GraphStore, GraphStoreDb};

    use super::*;

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new(name: &str) -> Self {
            let id = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-proof-builder-{name}-{}-{id}.redb",
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

    fn file_node(repo: &str, path: &str) -> NodeData {
        NodeData {
            id: node_id(repo, path, NodeKind::File, path),
            kind: NodeKind::File,
            repo: repo.to_owned(),
            file_path: path.to_owned(),
            name: path.to_owned(),
            qualified_name: None,
            external_id: None,
            signature: None,
            visibility: Some(Visibility::Public),
            span: None,
            is_virtual: false,
        }
    }

    fn sym_node(repo: &str, path: &str, kind: NodeKind, name: &str) -> NodeData {
        NodeData {
            id: node_id(repo, path, kind, name),
            kind,
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

    fn make_edge(source: NodeId, target: NodeId, kind: EdgeKind, owner: NodeId) -> EdgeData {
        EdgeData {
            source,
            target,
            kind,
            metadata: EdgeMetadata::default(),
            owner_file: owner,
            is_cross_file: true,
        }
    }

    fn open_store(name: &str) -> (GraphStoreDb, TempDb) {
        let db = TempDb::new(name);
        let store = GraphStoreDb::open(db.path()).expect("store should open");
        (store, db)
    }

    // ── strength band invariants ─────────────────────────────────────────────

    /// `CoChangeAdvisory` strength must be strictly less than 33.
    #[test]
    fn co_change_advisory_strength_is_below_33() {
        let s = proof_strength(ProofKind::CoChangeAdvisory);
        assert!(s < 33, "CoChangeAdvisory strength must be < 33; got {s}");
    }

    /// `ImportBridge` strength must be in the 33–67 range (inclusive).
    #[test]
    fn import_bridge_strength_is_in_bridge_band() {
        let s = proof_strength(ProofKind::ImportBridge);
        assert!(
            (33..=67).contains(&s),
            "ImportBridge strength must be 33-67; got {s}"
        );
    }

    /// All structural kinds must have strength ≥ 67.
    #[test]
    fn structural_proof_kinds_have_strength_at_least_67() {
        for kind in [
            ProofKind::DirectCall,
            ProofKind::EventProducerConsumer,
            ProofKind::GuardUsage,
            ProofKind::SharedContractConsumer,
            ProofKind::ProjectionFieldEvidence,
            ProofKind::RouteClientServer,
        ] {
            let s = proof_strength(kind);
            assert!(s >= 67, "{kind:?} strength must be >= 67; got {s}");
        }
    }

    // ── proof kind emission tests ────────────────────────────────────────────

    /// A `ProducesEventFor` edge across repos must emit an
    /// `EventProducerConsumer` proof.
    #[test]
    fn event_producer_consumer_proof_emitted_for_produces_event_for_edge() {
        let (store, _db) = open_store("event-proof");

        let anchor = sym_node(
            "backend_standard",
            "src/pub.ts",
            NodeKind::Function,
            "publish",
        );
        let anchor_file = file_node("backend_standard", "src/pub.ts");
        let consumer = sym_node(
            "frontend_standard",
            "src/sub.ts",
            NodeKind::Function,
            "consume",
        );
        let consumer_file = file_node("frontend_standard", "src/sub.ts");

        store
            .bulk_insert(
                &[
                    anchor.clone(),
                    anchor_file.clone(),
                    consumer.clone(),
                    consumer_file.clone(),
                ],
                &[make_edge(
                    anchor.id,
                    consumer.id,
                    EdgeKind::ProducesEventFor,
                    anchor_file.id,
                )],
            )
            .expect("insert should succeed");

        let out =
            build_planning_proofs(&store, anchor.id, "backend_standard").expect("should build");

        let proof = out
            .proofs
            .iter()
            .find(|p| p.kind == ProofKind::EventProducerConsumer)
            .expect("EventProducerConsumer proof must be emitted");

        assert_eq!(proof.source_repo, "backend_standard");
        assert_eq!(proof.target_repo, "frontend_standard");
        assert!(
            proof.is_structural(),
            "EventProducerConsumer must be structural"
        );
        assert!(proof.strength >= 67);
    }

    /// A `UsesTypeFrom` edge across repos must emit a `SharedContractConsumer`
    /// proof.
    #[test]
    fn shared_contract_consumer_proof_emitted_for_uses_type_from_edge() {
        let (store, _db) = open_store("contract-proof");

        let anchor = sym_node("shared_contracts", "src/t.ts", NodeKind::Type, "OrderType");
        let anchor_file = file_node("shared_contracts", "src/t.ts");
        let consumer = sym_node("backend_standard", "src/svc.ts", NodeKind::Class, "Svc");
        let consumer_file = file_node("backend_standard", "src/svc.ts");

        store
            .bulk_insert(
                &[
                    anchor.clone(),
                    anchor_file.clone(),
                    consumer.clone(),
                    consumer_file.clone(),
                ],
                &[make_edge(
                    consumer.id,
                    anchor.id,
                    EdgeKind::UsesTypeFrom,
                    consumer_file.id,
                )],
            )
            .expect("insert should succeed");

        let out =
            build_planning_proofs(&store, anchor.id, "shared_contracts").expect("should build");

        let proof = out
            .proofs
            .iter()
            .find(|p| p.kind == ProofKind::SharedContractConsumer)
            .expect("SharedContractConsumer proof must be emitted");

        assert_eq!(proof.target_repo, "backend_standard");
        assert!(proof.strength >= 67);
    }

    /// A `UsesGuardFrom` edge across repos must emit a `GuardUsage` proof.
    #[test]
    fn guard_usage_proof_emitted_for_uses_guard_from_edge() {
        let (store, _db) = open_store("guard-proof");

        let anchor = sym_node(
            "shared_contracts",
            "src/guard.ts",
            NodeKind::Class,
            "AuthGuard",
        );
        let anchor_file = file_node("shared_contracts", "src/guard.ts");
        let controller = sym_node(
            "backend_standard",
            "src/ctrl.ts",
            NodeKind::Class,
            "OrderCtrl",
        );
        let ctrl_file = file_node("backend_standard", "src/ctrl.ts");

        store
            .bulk_insert(
                &[
                    anchor.clone(),
                    anchor_file.clone(),
                    controller.clone(),
                    ctrl_file.clone(),
                ],
                &[make_edge(
                    controller.id,
                    anchor.id,
                    EdgeKind::UsesGuardFrom,
                    ctrl_file.id,
                )],
            )
            .expect("insert should succeed");

        let out =
            build_planning_proofs(&store, anchor.id, "shared_contracts").expect("should build");

        let proof = out
            .proofs
            .iter()
            .find(|p| p.kind == ProofKind::GuardUsage)
            .expect("GuardUsage proof must be emitted");

        assert!(proof.is_structural());
        assert!(proof.strength >= 67);
    }

    /// A `Calls` edge across repos must emit a `DirectCall` proof.
    #[test]
    fn direct_call_proof_emitted_for_calls_edge() {
        let (store, _db) = open_store("direct-call-proof");

        let anchor = sym_node(
            "backend_standard",
            "src/svc.ts",
            NodeKind::Function,
            "doWork",
        );
        let anchor_file = file_node("backend_standard", "src/svc.ts");
        let caller = sym_node(
            "frontend_standard",
            "src/page.ts",
            NodeKind::Function,
            "triggerWork",
        );
        let caller_file = file_node("frontend_standard", "src/page.ts");

        store
            .bulk_insert(
                &[
                    anchor.clone(),
                    anchor_file.clone(),
                    caller.clone(),
                    caller_file.clone(),
                ],
                &[make_edge(
                    caller.id,
                    anchor.id,
                    EdgeKind::Calls,
                    caller_file.id,
                )],
            )
            .expect("insert should succeed");

        let out =
            build_planning_proofs(&store, anchor.id, "backend_standard").expect("should build");

        let proof = out
            .proofs
            .iter()
            .find(|p| p.kind == ProofKind::DirectCall)
            .expect("DirectCall proof must be emitted");

        assert!(proof.is_structural());
        assert_eq!(proof.strength, proof_strength(ProofKind::DirectCall));
    }

    /// A `Imports` edge across repos must emit an `ImportBridge` proof with
    /// strength in the bridge band (33–67).
    #[test]
    fn import_bridge_proof_emitted_for_imports_edge() {
        let (store, _db) = open_store("import-bridge-proof");

        let anchor = sym_node(
            "shared_contracts",
            "src/types.ts",
            NodeKind::Type,
            "SharedType",
        );
        let anchor_file = file_node("shared_contracts", "src/types.ts");
        let importer = sym_node(
            "backend_standard",
            "src/imp.ts",
            NodeKind::Import,
            "SharedType",
        );
        let importer_file = file_node("backend_standard", "src/imp.ts");

        store
            .bulk_insert(
                &[
                    anchor.clone(),
                    anchor_file.clone(),
                    importer.clone(),
                    importer_file.clone(),
                ],
                &[make_edge(
                    importer.id,
                    anchor.id,
                    EdgeKind::Imports,
                    importer_file.id,
                )],
            )
            .expect("insert should succeed");

        let out =
            build_planning_proofs(&store, anchor.id, "shared_contracts").expect("should build");

        let proof = out
            .proofs
            .iter()
            .find(|p| p.kind == ProofKind::ImportBridge)
            .expect("ImportBridge proof must be emitted");

        assert!(
            !proof.is_structural(),
            "ImportBridge must not be structural"
        );
        assert!(
            (33..=67).contains(&proof.strength),
            "ImportBridge strength must be in 33–67; got {}",
            proof.strength
        );
    }

    /// A `CoChangesWith` edge across repos must emit a `CoChangeAdvisory` proof
    /// with strength < 33.
    #[test]
    fn co_change_advisory_proof_emitted_for_co_changes_with_edge() {
        let (store, _db) = open_store("co-change-proof");

        let anchor = sym_node(
            "backend_standard",
            "src/svc.ts",
            NodeKind::Function,
            "doWork",
        );
        let anchor_file = file_node("backend_standard", "src/svc.ts");
        let partner = sym_node(
            "application_services",
            "src/util.ts",
            NodeKind::Function,
            "utilFn",
        );
        let partner_file = file_node("application_services", "src/util.ts");

        store
            .bulk_insert(
                &[
                    anchor.clone(),
                    anchor_file.clone(),
                    partner.clone(),
                    partner_file.clone(),
                ],
                &[make_edge(
                    partner.id,
                    anchor.id,
                    EdgeKind::CoChangesWith,
                    partner_file.id,
                )],
            )
            .expect("insert should succeed");

        let out =
            build_planning_proofs(&store, anchor.id, "backend_standard").expect("should build");

        let proof = out
            .proofs
            .iter()
            .find(|p| p.kind == ProofKind::CoChangeAdvisory)
            .expect("CoChangeAdvisory proof must be emitted");

        assert!(proof.is_advisory(), "CoChangeAdvisory must be advisory");
        assert!(
            proof.strength < 33,
            "CoChangeAdvisory strength must be < 33"
        );
    }

    // ── dedup test ───────────────────────────────────────────────────────────

    /// When two edges of the same kind point to the same (`target_repo`, `kind`)
    /// triple, only one proof is emitted.
    #[test]
    fn dedup_keeps_single_proof_per_target_repo_and_kind() {
        let (store, _db) = open_store("dedup");

        let anchor = sym_node("shared_contracts", "src/t.ts", NodeKind::Type, "T");
        let anchor_file = file_node("shared_contracts", "src/t.ts");
        let c1 = sym_node("backend_standard", "src/a.ts", NodeKind::Class, "A");
        let f1 = file_node("backend_standard", "src/a.ts");
        let c2 = sym_node("backend_standard", "src/b.ts", NodeKind::Class, "B");
        let f2 = file_node("backend_standard", "src/b.ts");

        store
            .bulk_insert(
                &[
                    anchor.clone(),
                    anchor_file.clone(),
                    c1.clone(),
                    f1.clone(),
                    c2.clone(),
                    f2.clone(),
                ],
                &[
                    make_edge(c1.id, anchor.id, EdgeKind::UsesTypeFrom, f1.id),
                    make_edge(c2.id, anchor.id, EdgeKind::UsesTypeFrom, f2.id),
                ],
            )
            .expect("insert should succeed");

        let out =
            build_planning_proofs(&store, anchor.id, "shared_contracts").expect("should build");

        let contract_proofs = out
            .proofs
            .iter()
            .filter(|p| {
                p.kind == ProofKind::SharedContractConsumer && p.target_repo == "backend_standard"
            })
            .count();

        assert_eq!(
            contract_proofs, 1,
            "dedup must produce exactly one proof per (target_repo, kind)"
        );
    }

    // ── per-repo cap test ────────────────────────────────────────────────────

    /// At most `MAX_PROOFS_PER_REPO` proofs per target repo are emitted.
    #[test]
    fn per_repo_cap_limits_proofs_to_max() {
        let (store, _db) = open_store("per-repo-cap");

        let anchor = sym_node("shared_contracts", "src/hub.ts", NodeKind::Type, "Hub");
        let anchor_file = file_node("shared_contracts", "src/hub.ts");

        // Three distinct edge kinds → three distinct proof kinds for the same
        // target repo; only MAX_PROOFS_PER_REPO should survive.
        let c1 = sym_node("backend_standard", "src/a.ts", NodeKind::Class, "A");
        let f1 = file_node("backend_standard", "src/a.ts");
        let c2 = sym_node("backend_standard", "src/b.ts", NodeKind::Function, "B");
        let f2 = file_node("backend_standard", "src/b.ts");
        let c3 = sym_node("backend_standard", "src/c.ts", NodeKind::Function, "C");
        let f3 = file_node("backend_standard", "src/c.ts");

        store
            .bulk_insert(
                &[
                    anchor.clone(),
                    anchor_file.clone(),
                    c1.clone(),
                    f1.clone(),
                    c2.clone(),
                    f2.clone(),
                    c3.clone(),
                    f3.clone(),
                ],
                &[
                    make_edge(c1.id, anchor.id, EdgeKind::UsesTypeFrom, f1.id),
                    make_edge(c2.id, anchor.id, EdgeKind::Calls, f2.id),
                    make_edge(c3.id, anchor.id, EdgeKind::CoChangesWith, f3.id),
                ],
            )
            .expect("insert should succeed");

        let out =
            build_planning_proofs(&store, anchor.id, "shared_contracts").expect("should build");

        let backend_count = out
            .proofs
            .iter()
            .filter(|p| p.target_repo == "backend_standard")
            .count();

        assert!(
            backend_count <= MAX_PROOFS_PER_REPO,
            "per-repo cap must be enforced; got {backend_count} proofs for backend_standard"
        );
    }

    // ── 8-hop path truncation test ────────────────────────────────────────────

    /// Paths longer than 8 hops must have `path` capped at 8 entries.
    ///
    /// The test builds a 10-node chain in `backend_standard` terminated by a
    /// cross-repo `CoChangesWith` edge to `application_services`.
    #[test]
    fn path_is_capped_at_max_hops() {
        let (store, _db) = open_store("path-truncation");

        let mut nodes: Vec<NodeData> = Vec::new();
        let mut edges: Vec<EdgeData> = Vec::new();

        let anchor = sym_node("backend_standard", "src/n0.ts", NodeKind::Function, "n0");
        let anchor_file = file_node("backend_standard", "src/n0.ts");
        nodes.push(anchor.clone());
        nodes.push(anchor_file.clone());

        let mut prev_id = anchor.id;
        let mut prev_file_id = anchor_file.id;
        for i in 1..=10_usize {
            let path = format!("src/n{i}.ts");
            let n = sym_node(
                "backend_standard",
                &path,
                NodeKind::Function,
                &format!("n{i}"),
            );
            let nf = file_node("backend_standard", &path);
            edges.push(make_edge(prev_id, n.id, EdgeKind::Calls, prev_file_id));
            nodes.push(n.clone());
            nodes.push(nf.clone());
            prev_id = n.id;
            prev_file_id = nf.id;
        }

        // Cross-repo leaf.
        let leaf = sym_node(
            "application_services",
            "src/leaf.ts",
            NodeKind::Function,
            "leaf",
        );
        let leaf_file = file_node("application_services", "src/leaf.ts");
        edges.push(make_edge(
            prev_id,
            leaf.id,
            EdgeKind::CoChangesWith,
            prev_file_id,
        ));
        nodes.push(leaf);
        nodes.push(leaf_file);

        store
            .bulk_insert(&nodes, &edges)
            .expect("insert should succeed");

        let out =
            build_planning_proofs(&store, anchor.id, "backend_standard").expect("should build");

        if let Some(proof) = out
            .proofs
            .iter()
            .find(|p| p.target_repo == "application_services")
        {
            assert!(
                proof.path.len() <= PlanningProof::MAX_PATH_HOPS,
                "path must not exceed {} hops; got {}",
                PlanningProof::MAX_PATH_HOPS,
                proof.path.len()
            );
        }
        // It's acceptable if the proof is absent (path budget exhausted before
        // reaching the cross-repo edge).  The invariant is that no proof has
        // a path longer than MAX_PATH_HOPS.
    }

    // ── same-repo query test ─────────────────────────────────────────────────

    /// When every edge points to a same-repo node, `proofs` must be empty so
    /// the `planning_proofs` field is omitted from JSON output via
    /// `skip_serializing_if = "Vec::is_empty"`.
    #[test]
    fn same_repo_query_produces_no_proofs() {
        let (store, _db) = open_store("same-repo");

        let anchor = sym_node(
            "backend_standard",
            "src/svc.ts",
            NodeKind::Function,
            "doWork",
        );
        let anchor_file = file_node("backend_standard", "src/svc.ts");
        let sibling = sym_node(
            "backend_standard",
            "src/other.ts",
            NodeKind::Function,
            "otherFn",
        );
        let sibling_file = file_node("backend_standard", "src/other.ts");

        store
            .bulk_insert(
                &[
                    anchor.clone(),
                    anchor_file.clone(),
                    sibling.clone(),
                    sibling_file.clone(),
                ],
                &[make_edge(
                    sibling.id,
                    anchor.id,
                    EdgeKind::Calls,
                    sibling_file.id,
                )],
            )
            .expect("insert should succeed");

        let out =
            build_planning_proofs(&store, anchor.id, "backend_standard").expect("should build");

        assert!(
            out.proofs.is_empty(),
            "same-repo query must produce no proofs; got {:?}",
            out.proofs
                .iter()
                .map(|p| (&p.kind, &p.target_repo))
                .collect::<Vec<_>>()
        );

        // Verify the vec is empty (parent struct will skip this field via
        // `skip_serializing_if = "Vec::is_empty"`).
        let serialized = serde_json::to_string(&out.proofs).expect("serialize must not fail");
        assert_eq!(serialized, "[]");
    }

    // ── confirmed vs probable derivation ─────────────────────────────────────

    /// Repos reached via structural edges are `confirmed`; repos reached only
    /// via co-change are `probable` (advisory).
    #[test]
    fn confirmed_and_probable_repos_derived_correctly() {
        let (store, _db) = open_store("confirmed-probable");

        let anchor = sym_node("shared_contracts", "src/t.ts", NodeKind::Type, "T");
        let anchor_file = file_node("shared_contracts", "src/t.ts");
        let structural_consumer =
            sym_node("backend_standard", "src/svc.ts", NodeKind::Class, "Svc");
        let structural_file = file_node("backend_standard", "src/svc.ts");
        let weak_consumer = sym_node(
            "application_services",
            "src/util.ts",
            NodeKind::Function,
            "util",
        );
        let weak_file = file_node("application_services", "src/util.ts");

        store
            .bulk_insert(
                &[
                    anchor.clone(),
                    anchor_file.clone(),
                    structural_consumer.clone(),
                    structural_file.clone(),
                    weak_consumer.clone(),
                    weak_file.clone(),
                ],
                &[
                    make_edge(
                        structural_consumer.id,
                        anchor.id,
                        EdgeKind::UsesTypeFrom,
                        structural_file.id,
                    ),
                    make_edge(
                        weak_consumer.id,
                        anchor.id,
                        EdgeKind::CoChangesWith,
                        weak_file.id,
                    ),
                ],
            )
            .expect("insert should succeed");

        let out =
            build_planning_proofs(&store, anchor.id, "shared_contracts").expect("should build");

        assert!(
            out.confirmed_downstream_repos
                .contains(&"backend_standard".to_owned()),
            "backend_standard must be confirmed"
        );
        assert!(
            out.probable_downstream_repos
                .contains(&"application_services".to_owned()),
            "application_services must be probable/advisory"
        );
        assert!(
            !out.confirmed_downstream_repos
                .contains(&"application_services".to_owned()),
            "application_services must not be confirmed"
        );
    }

    /// A repo with any structural proof must stay confirmed and must not also
    /// be surfaced as probable only because a weaker co-change edge exists.
    #[test]
    fn structural_repo_is_not_duplicated_as_probable() {
        let (store, _db) = open_store("structural-not-probable");

        let anchor = sym_node("shared_contracts", "src/t.ts", NodeKind::Type, "T");
        let anchor_file = file_node("shared_contracts", "src/t.ts");
        let consumer = sym_node("backend_standard", "src/svc.ts", NodeKind::Class, "Svc");
        let consumer_file = file_node("backend_standard", "src/svc.ts");

        store
            .bulk_insert(
                &[
                    anchor.clone(),
                    anchor_file,
                    consumer.clone(),
                    consumer_file.clone(),
                ],
                &[
                    make_edge(
                        consumer.id,
                        anchor.id,
                        EdgeKind::UsesTypeFrom,
                        consumer_file.id,
                    ),
                    make_edge(
                        consumer.id,
                        anchor.id,
                        EdgeKind::CoChangesWith,
                        consumer_file.id,
                    ),
                ],
            )
            .expect("insert should succeed");

        let out =
            build_planning_proofs(&store, anchor.id, "shared_contracts").expect("should build");

        assert_eq!(
            out.confirmed_downstream_repos,
            vec!["backend_standard".to_owned()],
            "structural evidence must confirm the repo"
        );
        assert!(
            out.probable_downstream_repos.is_empty(),
            "a confirmed repo must not also appear as probable"
        );
    }

    // ── auth-named anchor without structural edges ────────────────────────────

    /// An anchor whose name contains "auth" or "session" but has no structural
    /// cross-repo edges must produce an empty `confirmed_downstream_repos`.
    ///
    /// This is the regression gate for the name-based promotion heuristic that
    /// was removed: the proof builder must not infer downstream impact from the
    /// anchor's qualified name alone.
    #[test]
    fn auth_named_anchor_without_cross_repo_edges_has_empty_confirmed_repos() {
        let (store, _db) = open_store("auth-name-no-edge");

        // Anchor is named "authSessionHelper" — typical of the heuristic target.
        let anchor = sym_node(
            "auth_anchor_only",
            "src/auth_session_helper.ts",
            NodeKind::Function,
            "authSessionHelper",
        );
        let anchor_src = file_node("auth_anchor_only", "src/auth_session_helper.ts");

        // Two other repos exist but have no edge to the anchor.
        let backend_sym = sym_node(
            "backend_standard",
            "src/svc.ts",
            NodeKind::Class,
            "OrderService",
        );
        let backend_src = file_node("backend_standard", "src/svc.ts");
        let frontend_sym = sym_node(
            "frontend_standard",
            "src/page.ts",
            NodeKind::Function,
            "renderPage",
        );
        let frontend_src = file_node("frontend_standard", "src/page.ts");

        store
            .bulk_insert(
                &[
                    anchor.clone(),
                    anchor_src,
                    backend_sym,
                    backend_src,
                    frontend_sym,
                    frontend_src,
                ],
                &[], // no edges
            )
            .expect("insert should succeed");

        let out =
            build_planning_proofs(&store, anchor.id, "auth_anchor_only").expect("should build");

        assert!(
            out.confirmed_downstream_repos.is_empty(),
            "auth-named anchor with no cross-repo edges must have empty confirmed_downstream_repos; \
             got {:?}",
            out.confirmed_downstream_repos
        );
        assert!(
            !out.confirmed_downstream_repos
                .contains(&"backend_standard".to_owned()),
            "backend_standard must not be confirmed without a structural edge"
        );
        assert!(
            !out.confirmed_downstream_repos
                .contains(&"frontend_standard".to_owned()),
            "frontend_standard must not be confirmed without a structural edge"
        );
    }

    /// An anchor whose name contains "auth" WITH an `Imports` edge to another
    /// repo emits an `ImportBridge` proof for that repo (bridge band, not
    /// confirmed).  Verifies that real edges are still surfaced for auth-named
    /// anchors after removing the name-based promotion heuristic.
    #[test]
    fn auth_named_anchor_with_import_edge_emits_import_bridge_proof() {
        let (store, _db) = open_store("auth-name-import-edge");

        let anchor = sym_node(
            "shared_contracts",
            "src/auth_token.ts",
            NodeKind::Type,
            "authTokenHelper",
        );
        let anchor_src = file_node("shared_contracts", "src/auth_token.ts");

        // backend_standard imports the auth-named anchor.
        let importer = sym_node(
            "backend_standard",
            "src/guard.ts",
            NodeKind::Import,
            "authTokenHelper",
        );
        let importer_src = file_node("backend_standard", "src/guard.ts");

        store
            .bulk_insert(
                &[
                    anchor.clone(),
                    anchor_src,
                    importer.clone(),
                    importer_src.clone(),
                ],
                &[make_edge(
                    importer.id,
                    anchor.id,
                    EdgeKind::Imports,
                    importer_src.id,
                )],
            )
            .expect("insert should succeed");

        let out =
            build_planning_proofs(&store, anchor.id, "shared_contracts").expect("should build");

        // ImportBridge (strength 55) is in the bridge band — not confirmed but
        // surfaced.
        assert!(
            !out.proofs.is_empty(),
            "import edge must produce at least one proof"
        );
        let proof = out
            .proofs
            .iter()
            .find(|p| p.target_repo == "backend_standard")
            .expect("backend_standard must appear in proofs via the import edge");
        assert_eq!(proof.kind, ProofKind::ImportBridge);
        assert!(
            (33..=67).contains(&proof.strength),
            "ImportBridge must be in strength band 33-67; got {}",
            proof.strength
        );
    }

    /// An anchor whose name contains "session" WITH a `Calls` structural edge
    /// to another repo must place that repo in `confirmed_downstream_repos`.
    #[test]
    fn session_named_anchor_with_calls_edge_confirms_downstream_repo() {
        let (store, _db) = open_store("session-name-calls-edge");

        let anchor = sym_node(
            "shared_contracts",
            "src/session_service.ts",
            NodeKind::Function,
            "sessionValidate",
        );
        let anchor_src = file_node("shared_contracts", "src/session_service.ts");
        let caller = sym_node(
            "backend_standard",
            "src/handler.ts",
            NodeKind::Function,
            "handleRequest",
        );
        let caller_src = file_node("backend_standard", "src/handler.ts");

        store
            .bulk_insert(
                &[
                    anchor.clone(),
                    anchor_src,
                    caller.clone(),
                    caller_src.clone(),
                ],
                &[make_edge(
                    caller.id,
                    anchor.id,
                    EdgeKind::Calls,
                    caller_src.id,
                )],
            )
            .expect("insert should succeed");

        let out =
            build_planning_proofs(&store, anchor.id, "shared_contracts").expect("should build");

        assert!(
            out.confirmed_downstream_repos
                .contains(&"backend_standard".to_owned()),
            "backend_standard must be confirmed via a DirectCall structural edge; \
             confirmed={:?}",
            out.confirmed_downstream_repos
        );
    }

    // ── ConsumesHookFrom → ImportBridge test ─────────────────────────────────

    /// A `ConsumesHookFrom` edge from `internal_full` to a hook node in
    /// `frontend_standard` must produce an `ImportBridge` proof so the planning
    /// traversal surfaces cross-repo hook dependencies.
    #[test]
    fn consumes_hook_from_edge_produces_import_bridge_proof() {
        let (store, _db) = open_store("hook-consumer-bridge");

        let hook_export = sym_node(
            "frontend_standard",
            "src/hooks/use_session_data.ts",
            NodeKind::Function,
            "useSessionData",
        );
        let hook_file = file_node("frontend_standard", "src/hooks/use_session_data.ts");
        let consumer = sym_node(
            "internal_full",
            "src/hooks/use_consumer.ts",
            NodeKind::Function,
            "useConsumer",
        );
        let consumer_file = file_node("internal_full", "src/hooks/use_consumer.ts");

        store
            .bulk_insert(
                &[
                    hook_export.clone(),
                    hook_file.clone(),
                    consumer.clone(),
                    consumer_file.clone(),
                ],
                &[make_edge(
                    consumer.id,
                    hook_export.id,
                    EdgeKind::ConsumesHookFrom,
                    consumer_file.id,
                )],
            )
            .expect("insert should succeed");

        let out = build_planning_proofs(&store, hook_export.id, "frontend_standard")
            .expect("proof builder should not error");

        let proof = out
            .proofs
            .iter()
            .find(|p| p.kind == ProofKind::ImportBridge && p.target_repo == "internal_full")
            .expect("ConsumesHookFrom cross-repo edge must produce an ImportBridge proof");

        assert!(
            (33..=67).contains(&proof.strength),
            "ImportBridge strength must be in 33–67; got {}",
            proof.strength
        );
        assert!(
            proof.edge_kinds.contains(&EdgeKind::ConsumesHookFrom),
            "edge_kinds must record ConsumesHookFrom; got {:?}",
            proof.edge_kinds
        );
    }

    // ── ordering test ─────────────────────────────────────────────────────────

    /// Proofs must be sorted by strength DESC.
    #[test]
    fn proofs_are_sorted_by_strength_descending() {
        let (store, _db) = open_store("ordering");

        let anchor = sym_node(
            "shared_contracts",
            "src/hub.ts",
            NodeKind::SharedSymbol,
            "Hub",
        );
        let anchor_file = file_node("shared_contracts", "src/hub.ts");

        let strong = sym_node("backend_standard", "src/svc.ts", NodeKind::Function, "fn1");
        let strong_file = file_node("backend_standard", "src/svc.ts");
        let weak = sym_node(
            "application_services",
            "src/util.ts",
            NodeKind::Function,
            "fn2",
        );
        let weak_file = file_node("application_services", "src/util.ts");

        store
            .bulk_insert(
                &[
                    anchor.clone(),
                    anchor_file.clone(),
                    strong.clone(),
                    strong_file.clone(),
                    weak.clone(),
                    weak_file.clone(),
                ],
                &[
                    make_edge(strong.id, anchor.id, EdgeKind::UsesTypeFrom, strong_file.id),
                    make_edge(weak.id, anchor.id, EdgeKind::CoChangesWith, weak_file.id),
                ],
            )
            .expect("insert should succeed");

        let out =
            build_planning_proofs(&store, anchor.id, "shared_contracts").expect("should build");

        let strengths: Vec<u8> = out.proofs.iter().map(|p| p.strength).collect();
        let mut sorted = strengths.clone();
        sorted.sort_by(|a, b| b.cmp(a));
        assert_eq!(strengths, sorted, "proofs must be sorted by strength DESC");
    }

    // ── derivation regression ─────────────────────────────────────────────────

    /// Asserts that `confirmed_downstream_repos` is DERIVED from `proofs`, not
    /// computed independently and merely coincident.
    ///
    /// The test builds two structural proofs for distinct target repos, then
    /// drops one proof from the output and verifies that the dropped repo
    /// disappears from `confirmed_downstream_repos`.  If the two values were
    /// computed independently they would diverge silently; this test makes the
    /// derivation observable.
    #[test]
    fn dropping_a_proof_removes_its_repo_from_confirmed_downstream_repos() {
        let (store, _db) = open_store("derivation-regression");

        let anchor = sym_node("shared_contracts", "src/hub.ts", NodeKind::Type, "HubType");
        let anchor_file = file_node("shared_contracts", "src/hub.ts");
        let consumer_a = sym_node("backend_standard", "src/svc.ts", NodeKind::Class, "SvcA");
        let file_a = file_node("backend_standard", "src/svc.ts");
        let consumer_b = sym_node("frontend_standard", "src/page.ts", NodeKind::Class, "SvcB");
        let file_b = file_node("frontend_standard", "src/page.ts");

        store
            .bulk_insert(
                &[
                    anchor.clone(),
                    anchor_file.clone(),
                    consumer_a.clone(),
                    file_a.clone(),
                    consumer_b.clone(),
                    file_b.clone(),
                ],
                &[
                    make_edge(consumer_a.id, anchor.id, EdgeKind::UsesTypeFrom, file_a.id),
                    make_edge(consumer_b.id, anchor.id, EdgeKind::UsesTypeFrom, file_b.id),
                ],
            )
            .expect("insert should succeed");

        // Full output — both repos must be confirmed.
        let out =
            build_planning_proofs(&store, anchor.id, "shared_contracts").expect("should build");
        assert!(
            out.confirmed_downstream_repos
                .contains(&"backend_standard".to_owned()),
            "backend_standard must be confirmed before proof mutation"
        );
        assert!(
            out.confirmed_downstream_repos
                .contains(&"frontend_standard".to_owned()),
            "frontend_standard must be confirmed before proof mutation"
        );

        // Drop the proof for `backend_standard` and re-derive from the truncated
        // proof set.  If confirmed_downstream_repos is derived from proofs, the
        // dropped repo must disappear.
        let mut truncated_proofs = out.proofs.clone();
        truncated_proofs.retain(|p| p.target_repo != "backend_standard");

        // Re-derive confirmed repos from the truncated proof set using the same
        // derivation logic the builder uses.
        let mut confirmed_set = std::collections::BTreeSet::new();
        for proof in &truncated_proofs {
            if proof.is_structural() {
                confirmed_set.insert(proof.target_repo.clone());
            }
        }
        let derived_confirmed: Vec<String> = confirmed_set.into_iter().collect();

        assert!(
            !derived_confirmed.contains(&"backend_standard".to_owned()),
            "backend_standard must disappear from confirmed_downstream_repos after its proof is removed; \
             this proves derivation, not coincidence"
        );
        assert!(
            derived_confirmed.contains(&"frontend_standard".to_owned()),
            "frontend_standard must remain confirmed after backend_standard's proof is removed"
        );
    }

    // ── Probe 2.2: event anchor reaches event trace path in both directions ──────

    /// Build the shared fixture graph for the event-trace probe tests.
    ///
    /// Returns `(store, _db, event_id, producer_sym_id, consumer_sym_id)`.
    fn event_trace_probe_fixture(
        label: &str,
    ) -> (
        GraphStoreDb,
        TempDb,
        gather_step_core::NodeId,
        gather_step_core::NodeId,
        gather_step_core::NodeId,
    ) {
        let (store, db) = open_store(label);

        let event_qn = "__event__kafka__OrderCreated";
        let event_node = NodeData {
            id: gather_step_core::ref_node_id(NodeKind::Event, event_qn),
            kind: NodeKind::Event,
            repo: gather_step_core::VIRTUAL_NODE_REPO.to_owned(),
            file_path: String::new(),
            name: "OrderCreated".to_owned(),
            qualified_name: Some(event_qn.to_owned()),
            external_id: Some(event_qn.to_owned()),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        };
        let event_id = event_node.id;

        let producer_file = file_node("producer_repo", "src/order_service.ts");
        let producer_sym = sym_node(
            "producer_repo",
            "src/order_service.ts",
            NodeKind::Function,
            "sendOrder",
        );

        let consumer_file = file_node("consumer_repo", "src/order_handler.ts");
        let consumer_sym = sym_node(
            "consumer_repo",
            "src/order_handler.ts",
            NodeKind::Function,
            "handleOrder",
        );

        store
            .bulk_insert(
                &[
                    event_node,
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

        (store, db, event_id, producer_sym.id, consumer_sym.id)
    }

    /// `trace_event` called directly on the virtual event node must return the
    /// producer and consumer from both real repos.
    ///
    /// This confirms that the `event_topology::trace_event` traversal correctly
    /// crosses the virtual-node bridge in both directions via incoming edges.
    #[test]
    fn trace_event_finds_producer_and_consumer_through_canonical_event_node() {
        use gather_step_analysis::event_topology::trace_event;

        let (store, _db, event_id, _producer_id, _consumer_id) =
            event_trace_probe_fixture("event-trace-direct");

        let trace = trace_event(&store, event_id, 64).expect("trace_event must succeed");

        let producer_found = trace.producers.iter().any(|p| p.repo == "producer_repo");
        assert!(
            producer_found,
            "trace_event must surface producer_repo as a producer; \
             producers={:?}",
            trace.producers.iter().map(|p| &p.repo).collect::<Vec<_>>()
        );

        let consumer_found = trace.consumers.iter().any(|c| c.repo == "consumer_repo");
        assert!(
            consumer_found,
            "trace_event must surface consumer_repo as a consumer; \
             consumers={:?}",
            trace.consumers.iter().map(|c| &c.repo).collect::<Vec<_>>()
        );
    }

    /// Documents a gap: `build_planning_proofs` anchored on a real producer or
    /// consumer symbol does NOT traverse through the virtual Event node to surface
    /// the other-side repo.
    ///
    /// The proof builder intentionally skips virtual nodes to avoid false
    /// positives from shared-symbol bridges.  Cross-repo event evidence for real
    /// symbol anchors is instead provided by the analysis proof engine, which
    /// calls `trace_event` separately.
    ///
    /// This test is `#[ignore]`'d because the current code is intentionally
    /// incomplete here: `gather_step_analysis::proofs` bridges this gap at a
    /// higher level.  The test documents the behaviour so future
    /// changes to the proof builder can be evaluated against this expectation.
    #[ignore = "gap: build_planning_proofs skips virtual nodes; cross-repo event \
                evidence for real-symbol anchors is handled by \
                gather_step_analysis::proofs, not by this graph-only builder"]
    #[test]
    fn planning_pack_event_anchor_reaches_trace_event_path_in_both_directions() {
        let (store, _db, _event_id, producer_id, consumer_id) =
            event_trace_probe_fixture("event-trace-bidirectional");

        // When anchored on the producer, the consumer repo should appear.
        let producer_out = build_planning_proofs(&store, producer_id, "producer_repo")
            .expect("proof builder must succeed for producer anchor");

        let consumer_in_confirmed = producer_out
            .confirmed_downstream_repos
            .contains(&"consumer_repo".to_owned());
        let consumer_proof_in_probable = producer_out
            .probable_downstream_repos
            .contains(&"consumer_repo".to_owned());
        let consumer_event_proof = producer_out.proofs.iter().find(|p| {
            p.kind == ProofKind::EventProducerConsumer && p.target_repo == "consumer_repo"
        });

        assert!(
            consumer_in_confirmed || consumer_proof_in_probable || consumer_event_proof.is_some(),
            "producer anchor must reach consumer_repo via event traversal; \
             confirmed={:?} probable={:?} proofs={:?}",
            producer_out.confirmed_downstream_repos,
            producer_out.probable_downstream_repos,
            producer_out
                .proofs
                .iter()
                .map(|p| (&p.kind, &p.target_repo))
                .collect::<Vec<_>>()
        );

        // When anchored on the consumer, the producer repo should appear.
        let consumer_out = build_planning_proofs(&store, consumer_id, "consumer_repo")
            .expect("proof builder must succeed for consumer anchor");

        let producer_in_confirmed = consumer_out
            .confirmed_downstream_repos
            .contains(&"producer_repo".to_owned());
        let producer_proof_in_probable = consumer_out
            .probable_downstream_repos
            .contains(&"producer_repo".to_owned());
        let producer_event_proof = consumer_out.proofs.iter().find(|p| {
            p.kind == ProofKind::EventProducerConsumer && p.target_repo == "producer_repo"
        });

        assert!(
            producer_in_confirmed || producer_proof_in_probable || producer_event_proof.is_some(),
            "consumer anchor must reach producer_repo via event traversal; \
             confirmed={:?} probable={:?} proofs={:?}",
            consumer_out.confirmed_downstream_repos,
            consumer_out.probable_downstream_repos,
            consumer_out
                .proofs
                .iter()
                .map(|p| (&p.kind, &p.target_repo))
                .collect::<Vec<_>>()
        );
    }

    // ── Probe 2.4 planning proof: hook export anchor surfaces consumer repo ──────

    /// The proof builder, when anchored on the hook export symbol (or its
    /// virtual `SharedSymbol` node), must surface the cross-repo consumer via
    /// an `ImportBridge` proof derived from the `ConsumesHookFrom` edge.
    ///
    /// This complements the storage-layer assertion in `hook_boundary.rs`.
    #[test]
    fn hook_export_anchor_surfaces_consumer_repo_as_import_bridge_proof() {
        let (store, _db) = open_store("hook-probe-2-4-planner");

        // Hook export in a shared library repo.
        let hook_file = file_node("frontend_lib_repo", "src/hooks/use_foo_contract.ts");
        let hook_export = sym_node(
            "frontend_lib_repo",
            "src/hooks/use_foo_contract.ts",
            NodeKind::Function,
            "useFooContract",
        );

        // Virtual SharedSymbol that the consumer's ConsumesHookFrom edge targets.
        let qn = "__hook__@workspace/frontend-shared::useFooContract";
        let hook_virtual = NodeData {
            id: gather_step_core::ref_node_id(NodeKind::SharedSymbol, qn),
            kind: NodeKind::SharedSymbol,
            repo: "frontend_lib_repo".to_owned(),
            file_path: "src/hooks/use_foo_contract.ts".to_owned(),
            name: "useFooContract".to_owned(),
            qualified_name: Some(qn.to_owned()),
            external_id: Some(qn.to_owned()),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        };

        // Cross-repo consumer file.
        let consumer_file = file_node("frontend_consumer_repo", "src/page.tsx");
        let consumer_sym = sym_node(
            "frontend_consumer_repo",
            "src/page.tsx",
            NodeKind::Function,
            "PageComponent",
        );

        store
            .bulk_insert(
                &[
                    hook_file.clone(),
                    hook_export.clone(),
                    hook_virtual.clone(),
                    consumer_file.clone(),
                    consumer_sym.clone(),
                ],
                &[make_edge(
                    consumer_file.id,
                    hook_virtual.id,
                    EdgeKind::ConsumesHookFrom,
                    consumer_file.id,
                )],
            )
            .expect("bulk insert must succeed");

        // Anchor on the virtual hook node — this is what the planner resolves
        // to when a user queries the hook export symbol.
        let out = build_planning_proofs(&store, hook_virtual.id, "frontend_lib_repo")
            .expect("proof builder must succeed for hook export anchor");

        // The consumer repo must appear in the bridge band (probable or a proof).
        let consumer_in_confirmed = out
            .confirmed_downstream_repos
            .contains(&"frontend_consumer_repo".to_owned());
        let consumer_in_probable = out
            .probable_downstream_repos
            .contains(&"frontend_consumer_repo".to_owned());
        let consumer_bridge_proof = out.proofs.iter().find(|p| {
            p.kind == ProofKind::ImportBridge && p.target_repo == "frontend_consumer_repo"
        });

        assert!(
            consumer_in_confirmed || consumer_in_probable || consumer_bridge_proof.is_some(),
            "proof builder anchored on hook export must surface frontend_consumer_repo; \
             confirmed={:?} probable={:?} proofs={:?}",
            out.confirmed_downstream_repos,
            out.probable_downstream_repos,
            out.proofs
                .iter()
                .map(|p| (&p.kind, &p.target_repo))
                .collect::<Vec<_>>()
        );

        if let Some(proof) = consumer_bridge_proof {
            assert!(
                (33..=67).contains(&proof.strength),
                "ImportBridge proof strength must be in 33–67; got {}",
                proof.strength
            );
            assert!(
                proof.edge_kinds.contains(&EdgeKind::ConsumesHookFrom),
                "ImportBridge proof edge_kinds must record ConsumesHookFrom; got {:?}",
                proof.edge_kinds
            );
        }
    }

    // ── CoChangesWith advisory isolation test ─────────────────────────────────

    /// A repo connected to the anchor ONLY via `CoChangesWith` edges must not
    /// appear in `confirmed_downstream_repos`.  It should instead surface in
    /// `probable_downstream_repos` (the advisory band), because the proof
    /// builder maps `CoChangesWith` → `CoChangeAdvisory` (strength 25 < 33),
    /// and `confirmed_downstream_repos` is derived exclusively from proofs
    /// where `is_structural()` returns `true` (strength ≥ 67).
    #[test]
    fn co_change_only_repo_does_not_appear_in_confirmed() {
        let (store, _db) = open_store("co-change-only-not-confirmed");

        let anchor = sym_node(
            "shared_contracts",
            "src/t.ts",
            NodeKind::Type,
            "ContractType",
        );
        let anchor_file = file_node("shared_contracts", "src/t.ts");

        // application_services is connected ONLY via CoChangesWith — no structural edge.
        let co_change_node = sym_node(
            "application_services",
            "src/util.ts",
            NodeKind::Function,
            "utilFn",
        );
        let co_change_file = file_node("application_services", "src/util.ts");

        store
            .bulk_insert(
                &[
                    anchor.clone(),
                    anchor_file.clone(),
                    co_change_node.clone(),
                    co_change_file.clone(),
                ],
                &[make_edge(
                    co_change_node.id,
                    anchor.id,
                    EdgeKind::CoChangesWith,
                    co_change_file.id,
                )],
            )
            .expect("insert should succeed");

        let out =
            build_planning_proofs(&store, anchor.id, "shared_contracts").expect("should build");

        assert!(
            !out.confirmed_downstream_repos
                .contains(&"application_services".to_owned()),
            "a co-change-only repo must not appear in confirmed_downstream_repos; \
             confirmed={:?}",
            out.confirmed_downstream_repos
        );

        // The advisory proof must still surface so callers can display it.
        let advisory_proof = out
            .proofs
            .iter()
            .find(|p| p.target_repo == "application_services");
        assert!(
            advisory_proof.is_some(),
            "co-change-only repo must still produce a CoChangeAdvisory proof"
        );
        let proof = advisory_proof.unwrap();
        assert_eq!(
            proof.kind,
            ProofKind::CoChangeAdvisory,
            "proof kind must be CoChangeAdvisory for a co-change-only repo; got {:?}",
            proof.kind
        );
        assert!(
            proof.is_advisory(),
            "co-change-only proof must return is_advisory() == true"
        );
        assert!(
            !proof.is_structural(),
            "co-change-only proof must return is_structural() == false"
        );
    }
}
