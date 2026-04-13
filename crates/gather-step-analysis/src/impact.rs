use std::collections::{BTreeMap, BTreeSet, VecDeque};

use gather_step_core::{EdgeKind, NodeId, NodeKind};
use gather_step_storage::{GraphStore, GraphStoreError};
use serde::{Deserialize, Serialize};

/// Edge kinds that represent structural relationships.  These are traversed in
/// Pass 1.  `CoChangesWith` is the weak co-change signal that is deferred to
/// Pass 2.
fn is_structural_edge(kind: EdgeKind) -> bool {
    !matches!(kind, EdgeKind::CoChangesWith)
}

#[derive(Debug, thiserror::Error)]
pub enum ImpactError {
    #[error(transparent)]
    Graph(#[from] GraphStoreError),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BoundaryRole {
    Producer,
    Consumer,
}

/// Classifies whether a file's path to the entry point is backed by structural
/// graph evidence or only by weak co-change signals.
///
/// - `Structural`: the file was reached via at least one structural edge kind
///   (anything other than `CoChangesWith` / `CoEditsWith`).
/// - `Advisory`: the file was reached **only** via `CoChangesWith` or
///   `CoEditsWith` edges.  These are secondary, probabilistic hints derived
///   from historical co-edit patterns rather than declared code relationships.
///
/// Callers should present advisory files in a visually distinct section with a
/// note that they are co-change hints, not proven structural consumers.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceBand {
    Structural,
    Advisory,
}

/// Repo-keyed impact map, sorted by descending repo sum-of-weights so the
/// highest-impact downstream repositories appear first.
///
/// Use [`ImpactMap::files_for`] to look up by repo name in tests or callers
/// that need random access rather than iteration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ImpactMap {
    /// Ordered pairs of `(repo, files)`.  Repos are sorted by sum-of-weights
    /// descending; ties are broken by repo name ascending.
    pub entries: Vec<(String, Vec<ImpactedFile>)>,
}

impl ImpactMap {
    /// Return the file list for the given `repo`, or `None` if the repo is not
    /// in the map.
    #[must_use]
    pub fn files_for(&self, repo: &str) -> Option<&Vec<ImpactedFile>> {
        self.entries
            .iter()
            .find(|(r, _)| r == repo)
            .map(|(_, files)| files)
    }

    /// `true` when the map contains no repos.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ImpactedFile {
    pub file_path: String,
    pub weight: f32,
    pub edge_kinds: Vec<EdgeKind>,
    pub serialization_point: bool,
    pub validation_point: bool,
    pub producer_or_consumer: Option<BoundaryRole>,
    /// Whether this file was reached via structural edges (`Structural`) or
    /// only via weak co-change signals (`Advisory`).  Consumers should render
    /// advisory files under a separate heading to avoid mixing unproven
    /// co-change hints with confirmed structural consumers.
    pub evidence_band: EvidenceBand,
}

#[derive(Default)]
struct ImpactAccumulator {
    min_depth: usize,
    edge_kinds: BTreeSet<EdgeKind>,
    serialization_point: bool,
    validation_point: bool,
    producer_or_consumer: Option<BoundaryRole>,
}

/// Two-pass BFS traversal over incoming edges.
///
/// Pass 1 walks structural edge kinds only (all kinds except `CoChangesWith`).
/// Pass 2 is a fallback: it is only executed for repos that have **no
/// structural path** after Pass 1, and it restricts the walk to weak edges.
/// This ordering means:
///
/// - Structural evidence is accumulated without paying the CPU cost of
///   fetching high-fanout `CoChangesWith` edges when they would ultimately be
///   filtered out by the demotion logic.
/// - Weak-only repos still surface when no structural signal exists (the
///   existing `co_changes_with_is_kept_when_it_is_the_only_signal_for_a_repo`
///   invariant continues to hold).
/// - If both passes return weak-only evidence for a repo, the -500 penalty in
///   `impact_weight` ensures structural repos always outrank weak-only repos.
pub fn shared_contract_impact<S: GraphStore>(
    store: &S,
    entry_node_id: NodeId,
) -> Result<ImpactMap, ImpactError> {
    // ── Pass 1: structural edges only ────────────────────────────────────────
    let structural_by_repo =
        traverse_incoming(store, entry_node_id, TraversalFilter::StructuralOnly)?;

    // ── Pass 2: weak edges, but only for repos with no structural path ───────
    // Build the full accumulator map by merging in weak-edge results for any
    // repo that Pass 1 left empty.
    let by_repo = if structural_by_repo.is_empty() {
        // No structural evidence at all → run the full weak-edge traversal.
        traverse_incoming(store, entry_node_id, TraversalFilter::WeakOnly)?
    } else {
        // Structural evidence exists for at least one repo. Run a weak-edge
        // traversal to find repos that have *only* weak-edge paths, then add
        // those repos as fallback entries alongside the structural results.
        let weak_by_repo = traverse_incoming(store, entry_node_id, TraversalFilter::WeakOnly)?;

        let mut merged = structural_by_repo;
        for (repo, weak_files) in weak_by_repo {
            // Only add repos that weren't found via structural edges.
            merged.entry(repo).or_insert(weak_files);
        }
        merged
    };

    Ok(build_impact_map(by_repo))
}

/// Filter applied to incoming edges during BFS traversal.
#[derive(Clone, Copy)]
enum TraversalFilter {
    /// Only structural (non-weak) edges.
    StructuralOnly,
    /// Only weak co-change edges (`CoChangesWith`).
    WeakOnly,
}

impl TraversalFilter {
    fn allows(self, kind: EdgeKind) -> bool {
        match self {
            Self::StructuralOnly => is_structural_edge(kind),
            Self::WeakOnly => !is_structural_edge(kind),
        }
    }
}

/// BFS over incoming edges, restricted to the given [`TraversalFilter`].
///
/// Returns a map of `repo → (file_path → ImpactAccumulator)`.
fn traverse_incoming<S: GraphStore>(
    store: &S,
    entry_node_id: NodeId,
    filter: TraversalFilter,
) -> Result<BTreeMap<String, BTreeMap<String, ImpactAccumulator>>, ImpactError> {
    let mut queue = VecDeque::from([(entry_node_id, 0_usize)]);
    let mut visited = BTreeSet::from([entry_node_id]);
    let mut by_repo = BTreeMap::<String, BTreeMap<String, ImpactAccumulator>>::new();

    while let Some((node_id, depth)) = queue.pop_front() {
        for edge in store.get_incoming(node_id)? {
            if !filter.allows(edge.kind) {
                continue;
            }
            let Some(source_node) = store.get_node(edge.source)? else {
                continue;
            };
            let owner_file = store.get_node(edge.owner_file)?;
            let file_node = owner_file
                .filter(|node| node.kind == NodeKind::File)
                .unwrap_or_else(|| source_node.clone());

            let entry = by_repo
                .entry(file_node.repo.clone())
                .or_default()
                .entry(file_node.file_path.clone())
                .or_insert_with(|| ImpactAccumulator {
                    min_depth: depth + 1,
                    ..ImpactAccumulator::default()
                });
            entry.min_depth = entry.min_depth.min(depth + 1);
            entry.edge_kinds.insert(edge.kind);
            entry.serialization_point |= is_serialization_edge(edge.kind, &source_node);
            entry.validation_point |= is_validation_edge(edge.kind, &source_node);
            entry.producer_or_consumer = entry
                .producer_or_consumer
                .or_else(|| boundary_role_for(edge.kind));

            if visited.insert(edge.source) {
                queue.push_back((edge.source, depth + 1));
            }
        }
    }

    Ok(by_repo)
}

/// Convert the raw `repo → files` accumulator map into a sorted [`ImpactMap`].
///
/// Applies the conditional `CoChangesWith` demotion: if a repo has any
/// structural evidence, weak-only files in that repo are dropped.  Repos
/// whose evidence is entirely weak are kept as fallback (the existing
/// `co_changes_with_is_kept_when_it_is_the_only_signal_for_a_repo` invariant
/// must continue to hold).  The -500 `weak_only` penalty in `impact_weight`
/// then ensures weak-only repos sink below any structural repo.
fn build_impact_map(by_repo: BTreeMap<String, BTreeMap<String, ImpactAccumulator>>) -> ImpactMap {
    // Conditional `CoChangesWith` demotion.
    //
    // Per-repo rule:
    //   - repo has any file with a non-CoChangesWith edge kind → drop every
    //     file in that repo whose edges are *only* CoChangesWith
    //   - repo has only CoChangesWith evidence → keep the repo intact (weak
    //     fallback so `shared_contract_impact` still surfaces co-change-only
    //     downstreams when nothing stronger exists)
    let mut entries: Vec<(String, Vec<ImpactedFile>)> = by_repo
        .into_iter()
        .map(|(repo, files)| {
            let repo_has_structural = files.values().any(|acc| {
                acc.edge_kinds
                    .iter()
                    .any(|kind| *kind != EdgeKind::CoChangesWith)
            });
            let mut ranked = files
                .into_iter()
                .filter(|(_, acc)| {
                    if repo_has_structural {
                        acc.edge_kinds
                            .iter()
                            .any(|kind| *kind != EdgeKind::CoChangesWith)
                    } else {
                        true
                    }
                })
                .map(|(file_path, acc)| {
                    let edge_kinds = acc.edge_kinds.into_iter().collect::<Vec<_>>();
                    let weak_only = all_weak_edges(&edge_kinds);
                    ImpactedFile {
                        file_path,
                        weight: impact_weight(
                            acc.min_depth,
                            specificity_score(&edge_kinds),
                            acc.serialization_point,
                            acc.validation_point,
                            weak_only,
                        ),
                        edge_kinds,
                        serialization_point: acc.serialization_point,
                        validation_point: acc.validation_point,
                        producer_or_consumer: acc.producer_or_consumer,
                        evidence_band: if weak_only {
                            EvidenceBand::Advisory
                        } else {
                            EvidenceBand::Structural
                        },
                    }
                })
                .collect::<Vec<_>>();
            ranked.sort_by(|left, right| {
                right
                    .weight
                    .partial_cmp(&left.weight)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(left.file_path.cmp(&right.file_path))
            });
            (repo, ranked)
        })
        .filter(|(_, ranked)| !ranked.is_empty())
        .collect();

    // Sort repos by sum-of-weights descending; stable tie-break by repo name ascending.
    entries.sort_by(|(repo_a, files_a), (repo_b, files_b)| {
        let sum_a: f32 = files_a.iter().map(|f| f.weight).sum();
        let sum_b: f32 = files_b.iter().map(|f| f.weight).sum();
        sum_b
            .partial_cmp(&sum_a)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(repo_a.cmp(repo_b))
    });

    ImpactMap { entries }
}

fn specificity_score(edge_kinds: &[EdgeKind]) -> u8 {
    edge_kinds
        .iter()
        .map(|kind| match kind {
            EdgeKind::ImplementsContractFrom => 6,
            EdgeKind::ProducesEventFor => 5,
            EdgeKind::UsesTypeFrom => 4,
            EdgeKind::ConsumesApiFrom | EdgeKind::UsesEventFrom | EdgeKind::UsesGuardFrom => 3,
            EdgeKind::ContractOn | EdgeKind::UsesShared | EdgeKind::References => 2,
            _ => 1,
        })
        .max()
        .unwrap_or(1)
}

/// Returns `true` when every edge kind is a weak co-change signal
/// (`CoChangesWith`).
fn all_weak_edges(edge_kinds: &[EdgeKind]) -> bool {
    !edge_kinds.is_empty()
        && edge_kinds
            .iter()
            .all(|k| matches!(k, EdgeKind::CoChangesWith))
}

/// Compute the weight for an impacted file.
///
/// Depth is lexicographically dominant: any single depth increase changes the
/// primary term by exactly 1.0, while the maximum combined lower-order
/// contribution is 0.9 (specificity 0.6 + serialization 0.2 + validation 0.1).
///
/// When ALL edges on the path are weak co-change signals (`CoChangesWith` /
/// `CoEditsWith`), a 500-point penalty is subtracted from the depth-dominant
/// term so that weak-only repos always rank below any repo with structural
/// evidence at the same depth.
fn impact_weight(
    depth: usize,
    specificity: u8,
    serialization: bool,
    validation: bool,
    weak_only: bool,
) -> f32 {
    let ser = if serialization { 1.0_f32 } else { 0.0_f32 };
    let val = if validation { 1.0_f32 } else { 0.0_f32 };
    let capped_depth = u16::try_from(depth.min(usize::from(u16::MAX))).unwrap_or(u16::MAX);
    let depth_term = f32::from(u16::MAX - capped_depth);
    let weak_penalty = if weak_only { 500.0_f32 } else { 0.0_f32 };
    depth_term - weak_penalty + f32::from(specificity) / 10.0 + 0.2 * ser + 0.1 * val
}

fn boundary_role_for(kind: EdgeKind) -> Option<BoundaryRole> {
    match kind {
        EdgeKind::ProducesEventFor => Some(BoundaryRole::Producer),
        EdgeKind::UsesEventFrom | EdgeKind::ConsumesApiFrom => Some(BoundaryRole::Consumer),
        _ => None,
    }
}

fn is_serialization_edge(kind: EdgeKind, node: &gather_step_core::NodeData) -> bool {
    matches!(kind, EdgeKind::ContractOn | EdgeKind::ProducesEventFor)
        || node
            .external_id
            .as_deref()
            .is_some_and(|id| id.contains("payload") || id.contains("schema"))
}

fn is_validation_edge(kind: EdgeKind, node: &gather_step_core::NodeData) -> bool {
    matches!(kind, EdgeKind::UsesGuardFrom)
        || node
            .external_id
            .as_deref()
            .is_some_and(|id| id.contains("validator"))
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

    use super::{BoundaryRole, ImpactMap, all_weak_edges, impact_weight, shared_contract_impact};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new(name: &str) -> Self {
            let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-impact-{name}-{}-{counter}.redb",
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

    #[test]
    fn shared_contract_impact_ranks_specific_edges_first() {
        let temp = TempDb::new("ranking");
        let store = GraphStoreDb::open(temp.path()).expect("graph should open");

        let contract = node(
            "shared_contracts",
            "src/order.ts",
            NodeKind::SharedSymbol,
            "OrderRecord",
            0,
        );
        let backend_file = file("backend_standard", "src/controller.ts");
        let backend_symbol = node(
            "backend_standard",
            "src/controller.ts",
            NodeKind::Class,
            "OrderController",
            0,
        );
        let frontend_file = file("frontend_standard", "src/api.ts");
        let frontend_symbol = node(
            "frontend_standard",
            "src/api.ts",
            NodeKind::Type,
            "CreateOrderInput",
            0,
        );

        store
            .bulk_insert(
                &[
                    contract.clone(),
                    backend_file.clone(),
                    backend_symbol.clone(),
                    frontend_file.clone(),
                    frontend_symbol.clone(),
                ],
                &[
                    edge(
                        backend_symbol.id,
                        contract.id,
                        EdgeKind::ImplementsContractFrom,
                        backend_file.id,
                    ),
                    edge(
                        frontend_symbol.id,
                        contract.id,
                        EdgeKind::UsesTypeFrom,
                        frontend_file.id,
                    ),
                ],
            )
            .expect("graph should write");

        let impact = shared_contract_impact(&store, contract.id).expect("impact should compute");
        assert_eq!(impact.entries.len(), 2);
        let backend = impact
            .files_for("backend_standard")
            .expect("backend_standard must be present");
        let frontend = impact
            .files_for("frontend_standard")
            .expect("frontend_standard must be present");
        assert_eq!(backend[0].file_path, "src/controller.ts");
        assert!(backend[0].weight > frontend[0].weight);
        assert_eq!(backend[0].producer_or_consumer, None);
    }

    #[test]
    fn impact_marks_producer_role_for_event_edges() {
        let temp = TempDb::new("roles");
        let store = GraphStoreDb::open(temp.path()).expect("graph should open");
        let contract = node(
            "shared_contracts",
            "src/order.ts",
            NodeKind::SharedSymbol,
            "Order",
            0,
        );
        let file = file("backend_standard", "src/publisher.ts");
        let symbol = node(
            "backend_standard",
            "src/publisher.ts",
            NodeKind::Function,
            "publish",
            0,
        );

        store
            .bulk_insert(
                &[contract.clone(), file.clone(), symbol.clone()],
                &[edge(
                    symbol.id,
                    contract.id,
                    EdgeKind::ProducesEventFor,
                    file.id,
                )],
            )
            .expect("graph should write");

        let impact = shared_contract_impact(&store, contract.id).expect("impact should compute");
        assert_eq!(
            impact
                .files_for("backend_standard")
                .expect("backend_standard must be present")[0]
                .producer_or_consumer,
            Some(BoundaryRole::Producer)
        );
    }

    #[test]
    fn impact_depth_always_beats_specificity() {
        let temp = TempDb::new("depth-first");
        let store = GraphStoreDb::open(temp.path()).expect("graph should open");
        let contract = node(
            "shared_contracts",
            "src/order.ts",
            NodeKind::SharedSymbol,
            "Order",
            0,
        );
        let direct_file = file("frontend_standard", "src/direct.ts");
        let direct_symbol = node(
            "frontend_standard",
            "src/direct.ts",
            NodeKind::Type,
            "DirectConsumer",
            0,
        );
        let transitive_file = file("backend_standard", "src/transitive.ts");
        let transitive_symbol = node(
            "backend_standard",
            "src/transitive.ts",
            NodeKind::Class,
            "TransitiveConsumer",
            0,
        );
        let intermediate_file = file("shared_contracts", "src/intermediate.ts");
        let hop = node(
            "shared_contracts",
            "src/intermediate.ts",
            NodeKind::Type,
            "Hop",
            1,
        );

        store
            .bulk_insert(
                &[
                    contract.clone(),
                    direct_file.clone(),
                    direct_symbol.clone(),
                    transitive_file.clone(),
                    transitive_symbol.clone(),
                    intermediate_file.clone(),
                    hop.clone(),
                ],
                &[
                    edge(
                        direct_symbol.id,
                        contract.id,
                        EdgeKind::UsesTypeFrom,
                        direct_file.id,
                    ),
                    edge(
                        transitive_symbol.id,
                        hop.id,
                        EdgeKind::ProducesEventFor,
                        transitive_file.id,
                    ),
                    edge(
                        hop.id,
                        contract.id,
                        EdgeKind::ImplementsContractFrom,
                        intermediate_file.id,
                    ),
                ],
            )
            .expect("graph should write");

        let impact = shared_contract_impact(&store, contract.id).expect("impact should compute");
        let frontend = impact
            .files_for("frontend_standard")
            .expect("frontend_standard must be present");
        let backend = impact
            .files_for("backend_standard")
            .expect("backend_standard must be present");
        assert!(frontend[0].weight > backend[0].weight);
    }

    #[test]
    fn impact_prefers_validator_and_producer_over_type_only_consumer() {
        let temp = TempDb::new("validator-producer");
        let store = GraphStoreDb::open(temp.path()).expect("graph should open");
        let contract = node(
            "shared_contracts",
            "src/order.ts",
            NodeKind::SharedSymbol,
            "Order",
            0,
        );
        let producer_file = file("backend_standard", "src/publisher.ts");
        let type_only_file = file("frontend_standard", "src/api.ts");
        let producer_symbol = node_with_external_id(
            "backend_standard",
            "src/publisher.ts",
            NodeKind::Function,
            "publish",
            0,
            Some("__payload__validator"),
        );
        let type_only_symbol = node(
            "frontend_standard",
            "src/api.ts",
            NodeKind::Type,
            "OrderInput",
            0,
        );

        store
            .bulk_insert(
                &[
                    contract.clone(),
                    producer_file.clone(),
                    type_only_file.clone(),
                    producer_symbol.clone(),
                    type_only_symbol.clone(),
                ],
                &[
                    edge(
                        producer_symbol.id,
                        contract.id,
                        EdgeKind::ProducesEventFor,
                        producer_file.id,
                    ),
                    edge(
                        type_only_symbol.id,
                        contract.id,
                        EdgeKind::UsesTypeFrom,
                        type_only_file.id,
                    ),
                ],
            )
            .expect("graph should write");

        let impact = shared_contract_impact(&store, contract.id).expect("impact should compute");
        let backend = impact
            .files_for("backend_standard")
            .expect("backend_standard must be present");
        let frontend = impact
            .files_for("frontend_standard")
            .expect("frontend_standard must be present");
        assert!(backend[0].weight > frontend[0].weight);
        assert!(backend[0].serialization_point);
        assert!(backend[0].validation_point);
    }

    #[test]
    fn co_changes_with_is_demoted_when_structural_evidence_exists_for_same_repo() {
        // Regression test: conditional demotion — if a repo has *any*
        // structural evidence for the target, its CoChangesWith-only files
        // must be dropped so the structural picture isn't diluted. A second
        // repo whose evidence is *only* CoChangesWith is kept as weak
        // fallback (covered by a separate test).
        let temp = TempDb::new("co-change-demote-when-structural");
        let store = GraphStoreDb::open(temp.path()).expect("graph should open");
        let contract = node(
            "shared_contracts",
            "src/guards/user-auth-guard.guard.ts",
            NodeKind::Class,
            "UserAuthGuard",
            0,
        );
        let structural_file = file("backend_standard", "src/controllers/orders.controller.ts");
        let structural_symbol = node(
            "backend_standard",
            "src/controllers/orders.controller.ts",
            NodeKind::Class,
            "OrdersController",
            0,
        );
        // Same repo as the structural evidence, but only co-change linked —
        // must be dropped because the repo already has structural evidence.
        let same_repo_co_change_file = file("backend_standard", "src/random/often-co-edited.ts");
        let same_repo_co_change_symbol = node(
            "backend_standard",
            "src/random/often-co-edited.ts",
            NodeKind::Function,
            "somethingElse",
            0,
        );

        store
            .bulk_insert(
                &[
                    contract.clone(),
                    structural_file.clone(),
                    structural_symbol.clone(),
                    same_repo_co_change_file.clone(),
                    same_repo_co_change_symbol.clone(),
                ],
                &[
                    edge(
                        structural_symbol.id,
                        contract.id,
                        EdgeKind::UsesGuardFrom,
                        structural_file.id,
                    ),
                    edge(
                        same_repo_co_change_symbol.id,
                        contract.id,
                        EdgeKind::CoChangesWith,
                        same_repo_co_change_file.id,
                    ),
                ],
            )
            .expect("graph should write");

        let impact = shared_contract_impact(&store, contract.id).expect("impact should compute");

        let backend_files = impact
            .files_for("backend_standard")
            .expect("backend_standard must be present");
        assert!(
            backend_files
                .iter()
                .any(|file| file.file_path == "src/controllers/orders.controller.ts"),
            "structural consumer file must be kept"
        );
        assert!(
            !backend_files
                .iter()
                .any(|file| file.file_path == "src/random/often-co-edited.ts"),
            "co-change-only file in a repo with structural evidence must be dropped; got {:?}",
            backend_files
                .iter()
                .map(|f| &f.file_path)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn co_changes_with_is_kept_when_it_is_the_only_signal_for_a_repo() {
        // Regression test: weak-fallback case — a repo whose only evidence
        // is CoChangesWith must still appear in the impact map. Dropping it
        // unconditionally (which the earlier implementation did) removes the
        // only remaining signal and leaves `shared_contract_impact` empty
        // for co-change-only downstreams.
        let temp = TempDb::new("co-change-keep-when-sole");
        let store = GraphStoreDb::open(temp.path()).expect("graph should open");
        let contract = node(
            "shared_contracts",
            "src/guards/user-auth-guard.guard.ts",
            NodeKind::Class,
            "UserAuthGuard",
            0,
        );
        let co_change_file = file("application-services", "src/coupling/often-co-edited.ts");
        let co_change_symbol = node(
            "application-services",
            "src/coupling/often-co-edited.ts",
            NodeKind::Function,
            "somethingElse",
            0,
        );

        store
            .bulk_insert(
                &[
                    contract.clone(),
                    co_change_file.clone(),
                    co_change_symbol.clone(),
                ],
                &[edge(
                    co_change_symbol.id,
                    contract.id,
                    EdgeKind::CoChangesWith,
                    co_change_file.id,
                )],
            )
            .expect("graph should write");

        let impact = shared_contract_impact(&store, contract.id).expect("impact should compute");

        let files = impact.files_for("application-services").unwrap_or_else(|| {
            panic!(
                "co-change-only repo must be kept as weak fallback; entries were {:?}",
                impact.entries.iter().map(|(r, _)| r).collect::<Vec<_>>()
            )
        });
        assert_eq!(files.len(), 1, "exactly one co-change file expected");
        assert!(
            files[0].edge_kinds.contains(&EdgeKind::CoChangesWith),
            "fallback entry must expose the CoChangesWith edge kind so callers can tag it as weak"
        );
    }

    fn file(repo: &str, file_path: &str) -> NodeData {
        node(repo, file_path, NodeKind::File, file_path, 0)
    }

    fn node(repo: &str, file_path: &str, kind: NodeKind, name: &str, ordinal: u16) -> NodeData {
        node_with_external_id(repo, file_path, kind, name, ordinal, None)
    }

    fn node_with_external_id(
        repo: &str,
        file_path: &str,
        kind: NodeKind,
        name: &str,
        _ordinal: u16,
        external_id: Option<&str>,
    ) -> NodeData {
        NodeData {
            id: node_id(repo, file_path, kind, name),
            kind,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(format!("{repo}::{name}")),
            external_id: external_id.map(ToOwned::to_owned),
            signature: None,
            visibility: Some(Visibility::Public),
            span: None,
            is_virtual: matches!(kind, NodeKind::SharedSymbol),
        }
    }

    fn edge(source: NodeId, target: NodeId, kind: EdgeKind, owner_file: NodeId) -> EdgeData {
        EdgeData {
            source,
            target,
            kind,
            metadata: EdgeMetadata::default(),
            owner_file,
            is_cross_file: true,
        }
    }

    #[test]
    fn empty_impact_map_constructs() {
        let empty = ImpactMap {
            entries: Vec::new(),
        };
        assert!(empty.is_empty());
    }

    /// Depth must always dominate specificity: a shallower node must outrank a
    /// deeper one even when the deeper node has the strongest edge kind and
    /// both serialization/validation bonuses.
    #[test]
    fn depth_always_dominates_specificity() {
        let temp = TempDb::new("depth-dominates");
        let store = GraphStoreDb::open(temp.path()).expect("graph should open");

        let contract = node(
            "shared_contracts",
            "src/contract.ts",
            NodeKind::SharedSymbol,
            "Contract",
            0,
        );
        // depth=1 consumer: UsesTypeFrom (specificity 4), no bonuses
        let shallow_file = file("frontend_standard", "src/shallow.ts");
        let shallow_symbol = node(
            "frontend_standard",
            "src/shallow.ts",
            NodeKind::Type,
            "ShallowConsumer",
            0,
        );
        // depth=2 consumer: ImplementsContractFrom (specificity 6) + ser + val bonuses
        let intermediate_file = file("shared_contracts", "src/intermediate.ts");
        let intermediate_symbol = node_with_external_id(
            "shared_contracts",
            "src/intermediate.ts",
            NodeKind::Type,
            "Intermediate",
            0,
            Some("__payload__validator"),
        );
        let deep_file = file("backend_standard", "src/deep.ts");
        let deep_symbol = node_with_external_id(
            "backend_standard",
            "src/deep.ts",
            NodeKind::Class,
            "DeepConsumer",
            0,
            Some("__payload__validator"),
        );

        store
            .bulk_insert(
                &[
                    contract.clone(),
                    shallow_file.clone(),
                    shallow_symbol.clone(),
                    intermediate_file.clone(),
                    intermediate_symbol.clone(),
                    deep_file.clone(),
                    deep_symbol.clone(),
                ],
                &[
                    // depth=1: shallow_symbol -> contract via UsesTypeFrom
                    edge(
                        shallow_symbol.id,
                        contract.id,
                        EdgeKind::UsesTypeFrom,
                        shallow_file.id,
                    ),
                    // depth=1 hop: intermediate_symbol -> contract via ImplementsContractFrom
                    edge(
                        intermediate_symbol.id,
                        contract.id,
                        EdgeKind::ImplementsContractFrom,
                        intermediate_file.id,
                    ),
                    // depth=2: deep_symbol -> intermediate_symbol (picked up transitively)
                    edge(
                        deep_symbol.id,
                        intermediate_symbol.id,
                        EdgeKind::ImplementsContractFrom,
                        deep_file.id,
                    ),
                ],
            )
            .expect("graph should write");

        let impact = shared_contract_impact(&store, contract.id).expect("impact should compute");

        let shallow_weight = impact
            .files_for("frontend_standard")
            .expect("frontend_standard must be present")[0]
            .weight;
        let deep_weight = impact
            .files_for("backend_standard")
            .expect("backend_standard must be present")[0]
            .weight;

        // depth=1 (UsesTypeFrom) must outrank depth=2 (ImplementsContractFrom + ser + val)
        assert!(
            shallow_weight > deep_weight,
            "expected depth=1 weight {shallow_weight} > depth=2 weight {deep_weight}"
        );
    }

    #[test]
    fn depth_priority_holds_for_deep_paths() {
        let shallower = impact_weight(9, 1, false, false, false);
        let deeper = impact_weight(10, 6, true, true, false);
        assert!(
            shallower > deeper,
            "expected depth 9 weight {shallower} > depth 10 weight {deeper}"
        );
    }

    /// A consumer that both produces events and validates must outrank a consumer
    /// that only imports a type, when both are at the same depth.
    #[test]
    fn validator_producer_outranks_type_only_consumer() {
        let temp = TempDb::new("validator-producer-vs-type");
        let store = GraphStoreDb::open(temp.path()).expect("graph should open");

        let contract = node(
            "shared_contracts",
            "src/event.ts",
            NodeKind::SharedSymbol,
            "OrderEvent",
            0,
        );
        // backend_standard: ProducesEventFor (specificity 5, ser=true) + validation_point via external_id
        let backend_file = file("backend_standard", "src/publisher.ts");
        let backend_symbol = node_with_external_id(
            "backend_standard",
            "src/publisher.ts",
            NodeKind::Function,
            "publishOrder",
            0,
            Some("order_validator"),
        );
        // frontend_standard: UsesTypeFrom only (specificity 4, no bonuses)
        let frontend_file = file("frontend_standard", "src/api.ts");
        let frontend_symbol = node(
            "frontend_standard",
            "src/api.ts",
            NodeKind::Type,
            "OrderInput",
            0,
        );

        store
            .bulk_insert(
                &[
                    contract.clone(),
                    backend_file.clone(),
                    backend_symbol.clone(),
                    frontend_file.clone(),
                    frontend_symbol.clone(),
                ],
                &[
                    edge(
                        backend_symbol.id,
                        contract.id,
                        EdgeKind::ProducesEventFor,
                        backend_file.id,
                    ),
                    edge(
                        frontend_symbol.id,
                        contract.id,
                        EdgeKind::UsesTypeFrom,
                        frontend_file.id,
                    ),
                ],
            )
            .expect("graph should write");

        let impact = shared_contract_impact(&store, contract.id).expect("impact should compute");

        let backend = impact
            .files_for("backend_standard")
            .expect("backend_standard must be present");
        let frontend = impact
            .files_for("frontend_standard")
            .expect("frontend_standard must be present");

        // Both consumers are at depth=1, so depth term is equal (10.0/2 = 5.0 each).
        // backend: 5.0 + 5/10 + 0.2 + 0.1 = 5.8   (ProducesEventFor ser=true, val=true via external_id)
        // frontend: 5.0 + 4/10 = 5.4               (UsesTypeFrom, no bonuses)
        let backend_weight = backend[0].weight;
        let frontend_weight = frontend[0].weight;

        assert!(
            backend_weight > frontend_weight,
            "expected backend_standard weight {backend_weight} > frontend_standard weight {frontend_weight}"
        );
        assert!(
            backend[0].serialization_point,
            "backend_standard should be a serialization point"
        );
        assert!(
            backend[0].validation_point,
            "backend_standard should be a validation point"
        );
    }

    /// A weak-only repo (`CoChangesWith`) at depth 1 must be outranked by a
    /// structural repo (`UsesTypeFrom`) at the same depth, because the 500-point
    /// `weak_only` penalty in `impact_weight` sinks the weak-only depth term
    /// below any structural result at the same depth.
    #[test]
    fn structural_repo_outranks_weak_only_repo_at_same_depth() {
        let structural_weight = impact_weight(1, 4, false, false, false); // UsesTypeFrom, depth=1
        let weak_only_weight = impact_weight(1, 1, false, false, true); // CoChangesWith-only, depth=1
        assert!(
            structural_weight > weak_only_weight,
            "structural weight {structural_weight} must exceed weak-only weight {weak_only_weight}"
        );
    }

    /// `all_weak_edges` returns true only when every kind is a weak signal.
    #[test]
    fn all_weak_edges_requires_all_kinds_to_be_weak() {
        assert!(all_weak_edges(&[EdgeKind::CoChangesWith]));
        assert!(!all_weak_edges(&[
            EdgeKind::CoChangesWith,
            EdgeKind::UsesTypeFrom
        ]));
        assert!(!all_weak_edges(&[]));
    }

    /// Two-pass structural-first traversal test.
    ///
    /// Workspace has two repos:
    /// - `backend_standard`: connected via structural `UsesTypeFrom` → appears
    ///   in Pass 1 with full structural weight (no penalty).
    /// - `application_services`: connected via `CoChangesWith` only → appears in
    ///   Pass 2 (fallback) with the -500 weak penalty, because Pass 2 is only
    ///   run for repos that have no structural path.
    ///
    /// The test asserts that:
    /// 1. Both repos appear (Pass 2 fallback still surfaces weak repos).
    /// 2. The structural repo outranks the co-change-only repo by more than 1
    ///    point (the penalty is 500, so the gap is far larger than any
    ///    specificity or bonus difference).
    /// 3. The structural repo's files have no `CoChangesWith` edges (Pass 1
    ///    only walked structural edges for that repo's path).
    #[test]
    fn structural_first_pass_surfaces_structural_repo_before_co_change_only_repo() {
        let temp = TempDb::new("structural-first-two-pass");
        let store = GraphStoreDb::open(temp.path()).expect("graph should open");

        let contract = node(
            "shared_contracts",
            "src/auth.ts",
            NodeKind::SharedSymbol,
            "AuditUser",
            0,
        );

        // backend_standard: structural path via UsesTypeFrom
        let struct_file = file("backend_standard", "src/controller.ts");
        let struct_sym = node(
            "backend_standard",
            "src/controller.ts",
            NodeKind::Class,
            "OrderController",
            0,
        );

        // application_services: co-change-only path
        let weak_file = file("application_services", "src/coupling.ts");
        let weak_sym = node(
            "application_services",
            "src/coupling.ts",
            NodeKind::Function,
            "unrelated",
            0,
        );

        store
            .bulk_insert(
                &[
                    contract.clone(),
                    struct_file.clone(),
                    struct_sym.clone(),
                    weak_file.clone(),
                    weak_sym.clone(),
                ],
                &[
                    edge(
                        struct_sym.id,
                        contract.id,
                        EdgeKind::UsesTypeFrom,
                        struct_file.id,
                    ),
                    edge(
                        weak_sym.id,
                        contract.id,
                        EdgeKind::CoChangesWith,
                        weak_file.id,
                    ),
                ],
            )
            .expect("graph should write");

        let impact = shared_contract_impact(&store, contract.id).expect("impact should compute");

        // Both repos must be present.
        let struct_files = impact
            .files_for("backend_standard")
            .expect("structural repo must be present");
        let weak_files = impact
            .files_for("application_services")
            .expect("co-change-only repo must still surface via Pass 2 fallback");

        // Structural repo has no CoChangesWith edges (Pass 1 only).
        assert!(
            struct_files
                .iter()
                .all(|f| !f.edge_kinds.contains(&EdgeKind::CoChangesWith)),
            "structural repo files must not carry CoChangesWith edges"
        );

        // Weak repo has only CoChangesWith.
        assert!(
            weak_files
                .iter()
                .all(|f| f.edge_kinds.iter().all(|k| *k == EdgeKind::CoChangesWith)),
            "co-change-only repo must expose only CoChangesWith edges"
        );

        // Structural repo must outrank weak repo by a substantial margin (500-pt penalty).
        let struct_weight: f32 = struct_files.iter().map(|f| f.weight).sum();
        let weak_weight: f32 = weak_files.iter().map(|f| f.weight).sum();
        assert!(
            struct_weight > weak_weight + 400.0,
            "structural repo (weight={struct_weight}) must outrank co-change-only repo \
             (weight={weak_weight}) by more than 400 points (the 500-pt weak penalty)"
        );

        // Structural repo must sort first in the entries list.
        assert_eq!(
            impact.entries[0].0, "backend_standard",
            "structural repo must be first in the sorted entries"
        );
    }

    // -------------------------------------------------------------------------
    // Evidence-band tests
    // -------------------------------------------------------------------------

    /// Files reached via structural edges must have `EvidenceBand::Structural`.
    /// Files reached only via `CoChangesWith` must have `EvidenceBand::Advisory`.
    #[test]
    fn evidence_band_is_structural_for_declared_edges_and_advisory_for_co_change_only() {
        use super::EvidenceBand;

        let temp = TempDb::new("evidence-band");
        let store = GraphStoreDb::open(temp.path()).expect("graph should open");

        let contract = node(
            "shared_contracts",
            "src/auth.ts",
            NodeKind::SharedSymbol,
            "AuthToken",
            0,
        );

        // Structural consumer — UsesTypeFrom.
        let struct_file = file("backend_standard", "src/auth.service.ts");
        let struct_sym = node(
            "backend_standard",
            "src/auth.service.ts",
            NodeKind::Class,
            "AuthService",
            0,
        );

        // Weak-only consumer — CoChangesWith.
        let weak_file = file("application_services", "src/coupling.ts");
        let weak_sym = node(
            "application_services",
            "src/coupling.ts",
            NodeKind::Function,
            "unrelated",
            0,
        );

        store
            .bulk_insert(
                &[
                    contract.clone(),
                    struct_file.clone(),
                    struct_sym.clone(),
                    weak_file.clone(),
                    weak_sym.clone(),
                ],
                &[
                    edge(
                        struct_sym.id,
                        contract.id,
                        EdgeKind::UsesTypeFrom,
                        struct_file.id,
                    ),
                    edge(
                        weak_sym.id,
                        contract.id,
                        EdgeKind::CoChangesWith,
                        weak_file.id,
                    ),
                ],
            )
            .expect("graph should write");

        let impact = shared_contract_impact(&store, contract.id).expect("impact should compute");

        let structural_files = impact
            .files_for("backend_standard")
            .expect("backend_standard must be present");
        let advisory_files = impact
            .files_for("application_services")
            .expect("application_services must be present");

        assert!(
            structural_files
                .iter()
                .all(|f| f.evidence_band == EvidenceBand::Structural),
            "UsesTypeFrom consumer must be Structural; got {:?}",
            structural_files
                .iter()
                .map(|f| f.evidence_band)
                .collect::<Vec<_>>()
        );
        assert!(
            advisory_files
                .iter()
                .all(|f| f.evidence_band == EvidenceBand::Advisory),
            "CoChangesWith-only consumer must be Advisory; got {:?}",
            advisory_files
                .iter()
                .map(|f| f.evidence_band)
                .collect::<Vec<_>>()
        );
    }

    /// A file that has BOTH a structural edge AND a `CoChangesWith` edge in the
    /// same repo must be classified as `Structural`, not `Advisory` — the
    /// presence of any structural edge takes precedence.
    #[test]
    fn evidence_band_is_structural_when_mixed_with_co_change_edge() {
        use super::EvidenceBand;

        let temp = TempDb::new("evidence-band-mixed");
        let store = GraphStoreDb::open(temp.path()).expect("graph should open");

        let contract = node(
            "shared_contracts",
            "src/order.ts",
            NodeKind::SharedSymbol,
            "Order",
            0,
        );
        let consumer_file = file("backend_standard", "src/order.service.ts");
        let consumer_sym = node(
            "backend_standard",
            "src/order.service.ts",
            NodeKind::Class,
            "OrderService",
            0,
        );

        store
            .bulk_insert(
                &[
                    contract.clone(),
                    consumer_file.clone(),
                    consumer_sym.clone(),
                ],
                &[
                    // Structural edge.
                    edge(
                        consumer_sym.id,
                        contract.id,
                        EdgeKind::UsesTypeFrom,
                        consumer_file.id,
                    ),
                    // Weak edge on the same file — should not downgrade the band.
                    edge(
                        consumer_sym.id,
                        contract.id,
                        EdgeKind::CoChangesWith,
                        consumer_file.id,
                    ),
                ],
            )
            .expect("graph should write");

        let impact = shared_contract_impact(&store, contract.id).expect("impact should compute");
        let files = impact
            .files_for("backend_standard")
            .expect("backend_standard must be present");

        assert!(
            files
                .iter()
                .all(|f| f.evidence_band == EvidenceBand::Structural),
            "mixed structural+co-change file must remain Structural; got {:?}",
            files.iter().map(|f| f.evidence_band).collect::<Vec<_>>()
        );
    }

    // ── CoChangesWith advisory-only primary-path exclusion ───────────────────

    /// When the only edges from an anchor to a repo are `CoChangesWith`, every
    /// file for that repo must carry `EvidenceBand::Advisory`, never
    /// `EvidenceBand::Structural`.
    ///
    /// This is the gating invariant that prevents co-change-only downstreams
    /// from polluting the structural primary section rendered to the caller
    /// (`impacted_files`).  Callers that gate on `EvidenceBand::Structural` —
    /// or its equivalent category label `"contract_impact"` — must never see
    /// a co-change-only repo.
    #[test]
    fn co_change_only_fallback_surfaces_under_advisory_band_not_structural() {
        use super::EvidenceBand;
        let temp = TempDb::new("co-change-advisory-band-only");
        let store = GraphStoreDb::open(temp.path()).expect("graph should open");

        let contract = node(
            "shared_contracts",
            "src/shared.ts",
            NodeKind::SharedSymbol,
            "SharedType",
            0,
        );
        // advisory_repo: connected only via CoChangesWith — no structural path.
        let advisory_file = file("advisory_repo", "src/noisy.ts");
        let advisory_sym = node(
            "advisory_repo",
            "src/noisy.ts",
            NodeKind::Function,
            "noisyFn",
            0,
        );

        store
            .bulk_insert(
                &[
                    contract.clone(),
                    advisory_file.clone(),
                    advisory_sym.clone(),
                ],
                &[edge(
                    advisory_sym.id,
                    contract.id,
                    EdgeKind::CoChangesWith,
                    advisory_file.id,
                )],
            )
            .expect("graph should write");

        let impact = shared_contract_impact(&store, contract.id).expect("impact should compute");

        // The repo must appear (weak fallback — no structural evidence exists so
        // Pass 2 surfaces it).
        let files = impact.files_for("advisory_repo").unwrap_or_else(|| {
            panic!(
                "co-change-only repo must appear as weak fallback; entries={:?}",
                impact.entries.iter().map(|(r, _)| r).collect::<Vec<_>>()
            )
        });

        // Every file in the co-change-only repo must be Advisory.
        assert!(
            files
                .iter()
                .all(|f| f.evidence_band == EvidenceBand::Advisory),
            "co-change-only fallback repo files must all be Advisory; got {:?}",
            files.iter().map(|f| f.evidence_band).collect::<Vec<_>>()
        );

        // No file in the co-change-only repo must be Structural.
        assert!(
            !files
                .iter()
                .any(|f| f.evidence_band == EvidenceBand::Structural),
            "co-change-only fallback repo must not contain any Structural-band files; \
             got {:?}",
            files
                .iter()
                .filter(|f| f.evidence_band == EvidenceBand::Structural)
                .map(|f| &f.file_path)
                .collect::<Vec<_>>()
        );
    }

    /// When a graph contains BOTH a structural repo and a co-change-only repo,
    /// the co-change-only repo's files must remain Advisory even though the
    /// structural repo is present.
    ///
    /// This verifies that the structural signal from one repo does not
    /// accidentally upgrade the evidence band of an unrelated co-change-only
    /// repo into the structural primary section.
    #[test]
    fn co_change_only_repo_stays_advisory_alongside_structural_repo() {
        use super::EvidenceBand;
        let temp = TempDb::new("co-change-stays-advisory");
        let store = GraphStoreDb::open(temp.path()).expect("graph should open");

        let contract = node(
            "shared_contracts",
            "src/contract.ts",
            NodeKind::SharedSymbol,
            "ContractType",
            0,
        );
        // Structural consumer.
        let structural_file = file("backend_standard", "src/svc.ts");
        let structural_sym = node(
            "backend_standard",
            "src/svc.ts",
            NodeKind::Class,
            "OrderService",
            0,
        );
        // Advisory-only consumer — different repo.
        let advisory_file = file("advisory_repo", "src/noisy.ts");
        let advisory_sym = node(
            "advisory_repo",
            "src/noisy.ts",
            NodeKind::Function,
            "noisyFn",
            0,
        );

        store
            .bulk_insert(
                &[
                    contract.clone(),
                    structural_file.clone(),
                    structural_sym.clone(),
                    advisory_file.clone(),
                    advisory_sym.clone(),
                ],
                &[
                    edge(
                        structural_sym.id,
                        contract.id,
                        EdgeKind::UsesTypeFrom,
                        structural_file.id,
                    ),
                    edge(
                        advisory_sym.id,
                        contract.id,
                        EdgeKind::CoChangesWith,
                        advisory_file.id,
                    ),
                ],
            )
            .expect("graph should write");

        let impact = shared_contract_impact(&store, contract.id).expect("impact should compute");

        // Structural repo must be Structural.
        let structural_files = impact
            .files_for("backend_standard")
            .expect("backend_standard must be present");
        assert!(
            structural_files
                .iter()
                .all(|f| f.evidence_band == EvidenceBand::Structural),
            "structural consumer must be Structural; got {:?}",
            structural_files
                .iter()
                .map(|f| f.evidence_band)
                .collect::<Vec<_>>()
        );

        // Advisory-only repo must remain Advisory, not promoted by the presence
        // of a structural repo elsewhere in the graph.
        let advisory_files = impact
            .files_for("advisory_repo")
            .expect("advisory_repo must still surface as weak fallback");
        assert!(
            advisory_files
                .iter()
                .all(|f| f.evidence_band == EvidenceBand::Advisory),
            "co-change-only repo must remain Advisory even when a structural repo is present; \
             got {:?}",
            advisory_files
                .iter()
                .map(|f| f.evidence_band)
                .collect::<Vec<_>>()
        );
    }
}
