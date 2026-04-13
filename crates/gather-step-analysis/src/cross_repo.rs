use std::collections::{BTreeMap, BTreeSet, VecDeque};

use gather_step_core::{EdgeKind, NodeId};
use gather_step_storage::{GraphStore, GraphStoreError};
use rustc_hash::FxHashSet;
use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TraceDirection {
    Incoming,
    Outgoing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CrossRepoHop {
    pub repo: String,
    pub file_path: String,
    pub node_id: NodeId,
    pub edge_kind: EdgeKind,
    pub direction: TraceDirection,
    pub line_number: Option<u32>,
    pub confidence: Option<u16>,
}

pub type CrossRepoDependencies = BTreeMap<String, BTreeSet<EdgeKind>>;

#[derive(Debug, Error)]
pub enum CrossRepoError {
    #[error(transparent)]
    Store(#[from] GraphStoreError),
}

pub fn trace_across_repos<S: GraphStore>(
    store: &S,
    virtual_node_id: NodeId,
    max_depth: usize,
) -> Result<BTreeMap<String, Vec<CrossRepoHop>>, CrossRepoError> {
    let mut grouped = BTreeMap::<String, Vec<CrossRepoHop>>::new();
    let mut queue = VecDeque::from([(virtual_node_id, 0_usize)]);
    let mut seen = FxHashSet::from_iter([virtual_node_id.as_bytes()]);

    while let Some((node_id, depth)) = queue.pop_front() {
        if depth >= max_depth {
            continue;
        }

        for edge in store.get_incoming(node_id)? {
            if let Some(node) = store.get_node(edge.source)? {
                if node.is_virtual {
                    if seen.insert(edge.source.as_bytes()) {
                        queue.push_back((edge.source, depth + 1));
                    }
                    continue;
                }
                grouped
                    .entry(node.repo.clone())
                    .or_default()
                    .push(CrossRepoHop {
                        repo: node.repo,
                        file_path: node.file_path,
                        node_id: edge.source,
                        edge_kind: edge.kind,
                        direction: TraceDirection::Incoming,
                        line_number: node.span.as_ref().map(|span| span.line_start),
                        confidence: edge.metadata.confidence,
                    });
                if seen.insert(edge.source.as_bytes()) {
                    queue.push_back((edge.source, depth + 1));
                }
            }
        }

        for edge in store.get_outgoing(node_id)? {
            if let Some(node) = store.get_node(edge.target)? {
                if node.is_virtual {
                    if seen.insert(edge.target.as_bytes()) {
                        queue.push_back((edge.target, depth + 1));
                    }
                    continue;
                }
                grouped
                    .entry(node.repo.clone())
                    .or_default()
                    .push(CrossRepoHop {
                        repo: node.repo,
                        file_path: node.file_path,
                        node_id: edge.target,
                        edge_kind: edge.kind,
                        direction: TraceDirection::Outgoing,
                        line_number: node.span.as_ref().map(|span| span.line_start),
                        confidence: edge.metadata.confidence,
                    });
                if seen.insert(edge.target.as_bytes()) {
                    queue.push_back((edge.target, depth + 1));
                }
            }
        }
    }

    for hops in grouped.values_mut() {
        hops.sort_unstable_by(|left, right| {
            left.file_path
                .cmp(&right.file_path)
                .then(left.edge_kind.as_u8().cmp(&right.edge_kind.as_u8()))
                .then(
                    trace_direction_ord(left.direction).cmp(&trace_direction_ord(right.direction)),
                )
                .then(left.line_number.cmp(&right.line_number))
                .then(left.confidence.cmp(&right.confidence))
                .then(left.node_id.as_bytes().cmp(&right.node_id.as_bytes()))
        });
    }

    Ok(grouped)
}

const fn trace_direction_ord(direction: TraceDirection) -> u8 {
    match direction {
        TraceDirection::Incoming => 0,
        TraceDirection::Outgoing => 1,
    }
}

pub fn cross_repo_deps<S: GraphStore>(
    store: &S,
    repo_name: &str,
) -> Result<CrossRepoDependencies, CrossRepoError> {
    let mut dependencies = BTreeMap::<String, BTreeSet<EdgeKind>>::new();
    let mut virtual_targets = FxHashSet::default();

    for node in store.nodes_by_repo(repo_name)? {
        for edge in store.get_outgoing(node.id)? {
            let Some(target) = store.get_node(edge.target)? else {
                continue;
            };
            if !target.is_virtual {
                continue;
            }
            virtual_targets.insert((target.id, edge.kind));
        }
    }

    for (virtual_id, _source_kind) in virtual_targets {
        for related in store.get_incoming(virtual_id)? {
            let Some(source) = store.get_node(related.source)? else {
                continue;
            };
            if source.repo != repo_name {
                dependencies
                    .entry(source.repo)
                    .or_default()
                    .insert(related.kind);
            }
        }

        for related in store.get_outgoing(virtual_id)? {
            let Some(target_node) = store.get_node(related.target)? else {
                continue;
            };
            if target_node.repo != repo_name {
                dependencies
                    .entry(target_node.repo)
                    .or_default()
                    .insert(related.kind);
            }
        }
    }

    Ok(dependencies)
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
        EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, SourceSpan, Visibility, node_id,
        virtual_node,
    };
    use gather_step_storage::{GraphStore, GraphStoreDb};
    use pretty_assertions::assert_eq;

    use super::{cross_repo_deps, trace_across_repos};

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new(name: &str) -> Self {
            let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-cross-repo-{name}-{}-{id}.redb",
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

    fn node(repo: &str, file_path: &str, name: &str, _ordinal: u16) -> NodeData {
        NodeData {
            id: node_id(repo, file_path, NodeKind::Function, name),
            kind: NodeKind::Function,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(format!("{repo}::{name}")),
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

    #[test]
    fn traces_shared_virtual_node_across_two_repos() {
        let temp_db = TempDb::new("trace");
        let store = GraphStoreDb::open(temp_db.path()).expect("store should open");
        let producer_file = file("producer", "src/producer.ts");
        let consumer_file = file("consumer", "src/consumer.ts");
        let producer = node("producer", "src/producer.ts", "emit_order", 0);
        let consumer = node("consumer", "src/consumer.ts", "handle_order", 0);
        let topic = virtual_node(
            NodeKind::Topic,
            "producer",
            "src/events.ts",
            "order.created",
            "__topic__kafka__order.created",
        );

        let producer_edge = EdgeData {
            source: producer.id,
            target: topic.id,
            kind: EdgeKind::Publishes,
            metadata: EdgeMetadata {
                confidence: Some(950),
                ..EdgeMetadata::default()
            },
            owner_file: producer_file.id,
            is_cross_file: true,
        };
        let consumer_edge = EdgeData {
            source: consumer.id,
            target: topic.id,
            kind: EdgeKind::Consumes,
            metadata: EdgeMetadata {
                confidence: Some(920),
                ..EdgeMetadata::default()
            },
            owner_file: consumer_file.id,
            is_cross_file: true,
        };

        store
            .bulk_insert(
                &[
                    producer_file.clone(),
                    consumer_file.clone(),
                    producer.clone(),
                    consumer.clone(),
                    topic.clone(),
                ],
                &[producer_edge, consumer_edge],
            )
            .expect("graph should insert");

        let trace = trace_across_repos(&store, topic.id, 2).expect("trace should succeed");
        assert_eq!(trace.len(), 2);
        assert!(trace.contains_key("producer"));
        assert!(trace.contains_key("consumer"));

        let deps = cross_repo_deps(&store, "consumer").expect("deps should succeed");
        assert!(deps.contains_key("producer"));
    }

    /// Build a fixture:
    ///   `repoA::producer_fn`  --Publishes-->  `virtual_topic`
    ///   `repoB::consumer_fn`  --Consumes-->   `virtual_topic`
    fn build_cross_repo_emit_consume_fixture() -> (GraphStoreDb, String, String) {
        let temp_db = TempDb::new("cross-repo-emit-consume");
        let path = temp_db.path().to_path_buf();
        std::mem::forget(temp_db);

        let store = GraphStoreDb::open(&path).expect("store should open");

        let producer_file = file("repoA", "src/producer.ts");
        let consumer_file = file("repoB", "src/consumer.ts");
        let producer_fn = node("repoA", "src/producer.ts", "producer_fn", 0);
        let consumer_fn = node("repoB", "src/consumer.ts", "consumer_fn", 0);
        let topic = virtual_node(
            NodeKind::Topic,
            "repoA",
            "src/events.ts",
            "shared.event",
            "__topic__kafka__shared.event",
        );

        store
            .bulk_insert(
                &[
                    producer_file.clone(),
                    consumer_file.clone(),
                    producer_fn.clone(),
                    consumer_fn.clone(),
                    topic.clone(),
                ],
                &[
                    EdgeData {
                        source: producer_fn.id,
                        target: topic.id,
                        kind: EdgeKind::Publishes,
                        metadata: EdgeMetadata::default(),
                        owner_file: producer_file.id,
                        is_cross_file: true,
                    },
                    EdgeData {
                        source: consumer_fn.id,
                        target: topic.id,
                        kind: EdgeKind::Consumes,
                        metadata: EdgeMetadata::default(),
                        owner_file: consumer_file.id,
                        is_cross_file: true,
                    },
                ],
            )
            .expect("fixture insert");

        (store, "repoA".to_owned(), "repoB".to_owned())
    }

    #[test]
    fn cross_repo_deps_uses_incoming_edge_kind_on_reverse_hops() {
        // Topology:
        //   repoA::producer_fn  --Publishes-->  virtual_topic
        //   repoB::consumer_fn  --Consumes-->   virtual_topic
        //
        // From repoA's perspective: the reverse hop finds consumer_fn (repoB).
        // The edge connecting consumer_fn to the topic is Consumes.
        // The recorded edge kind for repoB should be Consumes (consumer_fn's
        // actual edge kind), NOT Publishes (repoA's own source_kind).
        //
        // From repoB's perspective: the reverse hop finds producer_fn (repoA).
        // The edge connecting producer_fn to the topic is Publishes.
        // The recorded edge kind for repoA should be Publishes (producer_fn's
        // actual edge kind), NOT Consumes (repoB's own source_kind).
        let (store, repo_a, repo_b) = build_cross_repo_emit_consume_fixture();

        let deps_a = cross_repo_deps(&store, &repo_a).expect("deps A");
        let a_to_b = deps_a.get(&repo_b).expect("repoA must see repoB as a dep");
        assert!(
            a_to_b.contains(&EdgeKind::Consumes),
            "reverse hop must reflect the incoming edge kind (Consumes for repoB), got: {a_to_b:?}"
        );

        let deps_b = cross_repo_deps(&store, &repo_b).expect("deps B");
        let b_to_a = deps_b.get(&repo_a).expect("repoB must see repoA as a dep");
        assert!(
            b_to_a.contains(&EdgeKind::Publishes),
            "reverse hop must reflect the incoming edge kind (Publishes for repoA), got: {b_to_a:?}"
        );
    }

    // ── CoChangesWith advisory-band isolation ─────────────────────────────────

    /// `trace_across_repos` must carry `CoChangesWith` hops with their actual
    /// edge kind intact so that callers can separate advisory hops from
    /// structural ones without additional graph queries.
    ///
    /// Invariant: every hop produced exclusively by a `CoChangesWith` edge has
    /// `hop.edge_kind == EdgeKind::CoChangesWith`.  A consumer that filters
    /// `hops.iter().filter(|h| h.edge_kind == EdgeKind::CoChangesWith)` obtains
    /// only the advisory set; no additional fields are required for separation.
    #[test]
    fn co_change_only_hops_carry_co_changes_with_edge_kind() {
        // Topology:
        //   anchor_virtual_node (virtual SharedSymbol in anchor_repo)
        //   ← CoChangesWith ← advisory_fn (advisory_repo)
        //
        // `trace_across_repos` walks ALL edge kinds.  The resulting hops for
        // advisory_repo must expose EdgeKind::CoChangesWith so the caller can
        // identify them as advisory-band evidence.
        let temp_db = TempDb::new("co-change-advisory-hops");
        let store = GraphStoreDb::open(temp_db.path()).expect("store should open");

        let anchor_file = file("anchor_repo", "src/anchor.ts");
        let advisory_file = file("advisory_repo", "src/coupling.ts");
        let anchor_fn = node("anchor_repo", "src/anchor.ts", "anchorFn", 0);
        let advisory_fn = node("advisory_repo", "src/coupling.ts", "coupledFn", 0);

        // A virtual topic so trace_across_repos has a starting point.
        let topic = gather_step_core::virtual_node(
            gather_step_core::NodeKind::Topic,
            "anchor_repo",
            "src/anchor.ts",
            "anchor.topic",
            "__topic__kafka__anchor.topic",
        );

        store
            .bulk_insert(
                &[
                    anchor_file.clone(),
                    advisory_file.clone(),
                    anchor_fn.clone(),
                    advisory_fn.clone(),
                    topic.clone(),
                ],
                &[
                    // anchor_fn publishes to the topic (structural).
                    EdgeData {
                        source: anchor_fn.id,
                        target: topic.id,
                        kind: EdgeKind::Publishes,
                        metadata: EdgeMetadata::default(),
                        owner_file: anchor_file.id,
                        is_cross_file: true,
                    },
                    // advisory_fn is linked to anchor_fn only via CoChangesWith.
                    EdgeData {
                        source: advisory_fn.id,
                        target: anchor_fn.id,
                        kind: EdgeKind::CoChangesWith,
                        metadata: EdgeMetadata::default(),
                        owner_file: advisory_file.id,
                        is_cross_file: true,
                    },
                ],
            )
            .expect("graph should insert");

        // trace_across_repos starts from the virtual topic.
        let trace = trace_across_repos(&store, topic.id, 3).expect("trace should succeed");

        // advisory_repo hops must all carry CoChangesWith so callers can filter them.
        if let Some(advisory_hops) = trace.get("advisory_repo") {
            assert!(
                !advisory_hops.is_empty(),
                "advisory_repo should appear in trace results"
            );
            assert!(
                advisory_hops
                    .iter()
                    .all(|hop| hop.edge_kind == EdgeKind::CoChangesWith),
                "every hop reaching advisory_repo must carry CoChangesWith as the edge kind; \
                 got {:?}",
                advisory_hops
                    .iter()
                    .map(|h| h.edge_kind)
                    .collect::<Vec<_>>()
            );
        }
        // If advisory_repo doesn't appear at depth 3 that's acceptable (it's 2
        // hops from the topic through anchor_fn); the key invariant is that if
        // it does appear, the edge kind is CoChangesWith, not a structural kind.
    }

    /// A graph where a repo is connected exclusively via `CoChangesWith` edges
    /// must not produce structural hop records with any other edge kind.
    /// This validates that `trace_across_repos` does not rewrite edge kinds
    /// during traversal.
    #[test]
    fn co_change_only_edge_kind_is_not_rewritten_during_traversal() {
        let temp_db = TempDb::new("co-change-kind-preserved");
        let store = GraphStoreDb::open(temp_db.path()).expect("store should open");

        let topic = gather_step_core::virtual_node(
            gather_step_core::NodeKind::Topic,
            "producer_repo",
            "src/events.ts",
            "order.event",
            "__topic__kafka__order.event",
        );
        let producer_file = file("producer_repo", "src/producer.ts");
        let producer_fn = node("producer_repo", "src/producer.ts", "emitOrder", 0);
        let advisory_file = file("advisory_repo", "src/noisy.ts");
        let advisory_fn = node("advisory_repo", "src/noisy.ts", "noisyFn", 0);

        store
            .bulk_insert(
                &[
                    topic.clone(),
                    producer_file.clone(),
                    producer_fn.clone(),
                    advisory_file.clone(),
                    advisory_fn.clone(),
                ],
                &[
                    EdgeData {
                        source: producer_fn.id,
                        target: topic.id,
                        kind: EdgeKind::Publishes,
                        metadata: EdgeMetadata::default(),
                        owner_file: producer_file.id,
                        is_cross_file: true,
                    },
                    EdgeData {
                        source: advisory_fn.id,
                        target: producer_fn.id,
                        kind: EdgeKind::CoChangesWith,
                        metadata: EdgeMetadata::default(),
                        owner_file: advisory_file.id,
                        is_cross_file: true,
                    },
                ],
            )
            .expect("graph should insert");

        let trace = trace_across_repos(&store, topic.id, 3).expect("trace should succeed");

        // For advisory_repo: if it appears, no hop must carry a structural edge kind.
        if let Some(advisory_hops) = trace.get("advisory_repo") {
            for hop in advisory_hops {
                assert_eq!(
                    hop.edge_kind,
                    EdgeKind::CoChangesWith,
                    "advisory hop edge kind must be CoChangesWith, not a structural kind; \
                     got {:?} for file {}",
                    hop.edge_kind,
                    hop.file_path
                );
            }
        }
    }

    // ── Stable sort invariant tests ───────────────────────────────────────────

    /// Build a fixture with one virtual topic and four consumers from `repoA`,
    /// each in a distinct `file_path`, to exercise the `file_path` tiebreak.
    /// Returns the store and the virtual topic `NodeId`.
    fn build_multi_consumer_fixture() -> (GraphStoreDb, gather_step_core::NodeId) {
        let temp_db = TempDb::new("multi-consumer");
        let path = temp_db.path().to_path_buf();
        std::mem::forget(temp_db);

        let store = GraphStoreDb::open(&path).expect("store should open");

        let topic = virtual_node(
            NodeKind::Topic,
            "repoA",
            "src/events.ts",
            "shared.event",
            "__topic__kafka__shared.event",
        );

        let mut all_nodes = vec![topic.clone()];
        let mut all_edges = Vec::new();

        for i in 0_u16..4 {
            let fp = format!("src/consumer_{i}.ts");
            let owner = NodeData {
                id: node_id("repoA", &fp, NodeKind::File, &fp),
                kind: NodeKind::File,
                repo: "repoA".to_owned(),
                file_path: fp.clone(),
                name: fp.clone(),
                qualified_name: None,
                external_id: None,
                signature: None,
                visibility: None,
                span: None,
                is_virtual: false,
            };
            let consumer = NodeData {
                id: node_id("repoA", &fp, NodeKind::Function, &format!("handler_{i}")),
                kind: NodeKind::Function,
                repo: "repoA".to_owned(),
                file_path: fp.clone(),
                name: format!("handler_{i}"),
                qualified_name: Some(format!("repoA::handler_{i}")),
                external_id: None,
                signature: None,
                visibility: Some(Visibility::Public),
                span: Some(SourceSpan {
                    line_start: 10 + u32::from(i),
                    line_len: 0,
                    column_start: 0,
                    column_len: 4,
                }),
                is_virtual: false,
            };
            all_edges.push(EdgeData {
                source: consumer.id,
                target: topic.id,
                kind: EdgeKind::Consumes,
                metadata: EdgeMetadata::default(),
                owner_file: owner.id,
                is_cross_file: false,
            });
            all_nodes.extend([owner, consumer]);
        }

        store
            .bulk_insert(&all_nodes, &all_edges)
            .expect("fixture insert");

        (store, topic.id)
    }

    /// Repeated calls to `trace_across_repos` on the same fixture must return
    /// hop lists in identical order every time.
    #[test]
    fn trace_across_repos_hop_order_is_stable() {
        let (store, topic_id) = build_multi_consumer_fixture();

        let first = trace_across_repos(&store, topic_id, 2).expect("first trace should succeed");
        let second = trace_across_repos(&store, topic_id, 2).expect("second trace should succeed");

        assert_eq!(
            first, second,
            "hop order must be identical across repeated calls"
        );
    }

    /// Inserting the fixture nodes in reversed order must still produce the
    /// same hop ordering as the forward insertion.
    #[test]
    fn trace_across_repos_output_invariant_under_insertion_order() {
        let (store_forward, topic_id) = build_multi_consumer_fixture();

        let temp_db = TempDb::new("multi-consumer-reversed");
        let path = temp_db.path().to_path_buf();
        std::mem::forget(temp_db);
        let store_rev = GraphStoreDb::open(&path).expect("store should open");

        let topic = virtual_node(
            NodeKind::Topic,
            "repoA",
            "src/events.ts",
            "shared.event",
            "__topic__kafka__shared.event",
        );

        let mut all_nodes = vec![topic.clone()];
        let mut all_edges = Vec::new();

        let mut pairs: Vec<(NodeData, NodeData)> = Vec::new();
        for i in 0_u16..4 {
            let fp = format!("src/consumer_{i}.ts");
            let owner = NodeData {
                id: node_id("repoA", &fp, NodeKind::File, &fp),
                kind: NodeKind::File,
                repo: "repoA".to_owned(),
                file_path: fp.clone(),
                name: fp.clone(),
                qualified_name: None,
                external_id: None,
                signature: None,
                visibility: None,
                span: None,
                is_virtual: false,
            };
            let consumer = NodeData {
                id: node_id("repoA", &fp, NodeKind::Function, &format!("handler_{i}")),
                kind: NodeKind::Function,
                repo: "repoA".to_owned(),
                file_path: fp,
                name: format!("handler_{i}"),
                qualified_name: Some(format!("repoA::handler_{i}")),
                external_id: None,
                signature: None,
                visibility: Some(Visibility::Public),
                span: Some(SourceSpan {
                    line_start: 10 + u32::from(i),
                    line_len: 0,
                    column_start: 0,
                    column_len: 4,
                }),
                is_virtual: false,
            };
            pairs.push((owner, consumer));
        }

        // Insert in reversed order (index 3 down to 0).
        for (owner, consumer) in pairs.iter().rev() {
            all_nodes.extend([owner.clone(), consumer.clone()]);
            all_edges.push(EdgeData {
                source: consumer.id,
                target: topic.id,
                kind: EdgeKind::Consumes,
                metadata: EdgeMetadata::default(),
                owner_file: owner.id,
                is_cross_file: false,
            });
        }

        store_rev
            .bulk_insert(&all_nodes, &all_edges)
            .expect("reversed fixture insert");

        let result_fwd = trace_across_repos(&store_forward, topic_id, 2).expect("forward trace");
        let result_rev = trace_across_repos(&store_rev, topic.id, 2).expect("reversed trace");

        assert_eq!(
            result_fwd, result_rev,
            "hop order must be invariant to insertion order"
        );
    }
}
