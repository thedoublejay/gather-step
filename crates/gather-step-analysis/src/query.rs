use std::collections::{BTreeMap, VecDeque};

use gather_step_core::{EdgeData, EdgeKind, NodeData, NodeId, NodeKind};
use gather_step_storage::{GraphStore, GraphStoreError};
use rustc_hash::{FxHashMap, FxHashSet};
use thiserror::Error;

const DEFAULT_FANOUT_CAP: usize = 256;

#[derive(Debug, Error)]
pub enum QueryError {
    #[error(transparent)]
    Store(#[from] GraphStoreError),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraversalStep {
    pub node_id: NodeId,
    pub edge_kinds: Vec<EdgeKind>,
    pub depth: usize,
    pub in_paths: Vec<Vec<EdgeKind>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraversalOutcome {
    pub steps: Vec<TraversalStep>,
    pub depth_capped: bool,
    pub truncated: bool,
}

pub struct GraphQuery<'a, S> {
    store: &'a S,
}

impl<'a, S: GraphStore> GraphQuery<'a, S> {
    #[must_use]
    pub fn new(store: &'a S) -> Self {
        Self { store }
    }

    pub fn get_node(&self, id: NodeId) -> Result<Option<NodeData>, QueryError> {
        self.store.get_node(id).map_err(QueryError::from)
    }

    pub fn get_nodes_by_kind(&self, kind: NodeKind) -> Result<Vec<NodeData>, QueryError> {
        self.store.nodes_by_type(kind).map_err(QueryError::from)
    }

    pub fn get_edges(
        &self,
        source: NodeId,
        edge_kind: Option<EdgeKind>,
        min_confidence: Option<u16>,
    ) -> Result<Vec<EdgeData>, QueryError> {
        let mut edges = self.store.get_outgoing(source)?;
        if let Some(edge_kind) = edge_kind {
            edges.retain(|edge| edge.kind == edge_kind);
        }
        edges.retain(|edge| edge.metadata.passes_confidence(min_confidence));
        Ok(edges)
    }

    pub fn get_reverse_edges(
        &self,
        target: NodeId,
        edge_kind: Option<EdgeKind>,
        min_confidence: Option<u16>,
    ) -> Result<Vec<EdgeData>, QueryError> {
        let mut edges = self.store.get_incoming(target)?;
        if let Some(edge_kind) = edge_kind {
            edges.retain(|edge| edge.kind == edge_kind);
        }
        edges.retain(|edge| edge.metadata.passes_confidence(min_confidence));
        Ok(edges)
    }

    pub fn traverse(
        &self,
        start: NodeId,
        edge_kinds: &[EdgeKind],
        max_depth: usize,
        min_confidence: Option<u16>,
    ) -> Result<Vec<TraversalStep>, QueryError> {
        Ok(self
            .traverse_with_provenance(start, edge_kinds, max_depth, min_confidence)?
            .steps)
    }

    pub fn traverse_with_provenance(
        &self,
        start: NodeId,
        edge_kinds: &[EdgeKind],
        max_depth: usize,
        min_confidence: Option<u16>,
    ) -> Result<TraversalOutcome, QueryError> {
        let mut queue = VecDeque::from([(start, Vec::<EdgeKind>::new(), 0_usize)]);
        let mut enqueued = FxHashSet::from_iter([start.as_bytes()]);
        let mut order: Vec<NodeId> = Vec::new();
        let mut primary: FxHashMap<[u8; 16], (Vec<EdgeKind>, usize)> = FxHashMap::default();
        let mut in_paths: FxHashMap<[u8; 16], Vec<Vec<EdgeKind>>> = FxHashMap::default();
        let mut depth_capped = false;
        let mut truncated = false;

        while let Some((node_id, path, depth)) = queue.pop_front() {
            let outgoing: Vec<EdgeData> = self
                .store
                .get_outgoing(node_id)?
                .into_iter()
                .filter(|edge| edge_kinds.is_empty() || edge_kinds.contains(&edge.kind))
                .filter(|edge| edge.metadata.passes_confidence(min_confidence))
                .collect();

            if depth >= max_depth {
                if !outgoing.is_empty() {
                    depth_capped = true;
                }
                continue;
            }

            for (index, edge) in outgoing.iter().enumerate() {
                if index >= DEFAULT_FANOUT_CAP {
                    truncated = true;
                    break;
                }
                let mut next_path = path.clone();
                next_path.push(edge.kind);
                let key = edge.target.as_bytes();

                let recorded = in_paths.entry(key).or_default();
                if recorded.len() < DEFAULT_FANOUT_CAP {
                    recorded.push(next_path.clone());
                } else {
                    truncated = true;
                }

                if enqueued.insert(key) {
                    order.push(edge.target);
                    primary.insert(key, (next_path.clone(), depth + 1));
                    queue.push_back((edge.target, next_path, depth + 1));
                }
            }
        }

        let steps = order
            .into_iter()
            .map(|node_id| {
                let key = node_id.as_bytes();
                let (edges, depth) = primary.get(&key).cloned().unwrap_or_default();
                let paths = in_paths.get(&key).cloned().unwrap_or_default();
                TraversalStep {
                    node_id,
                    edge_kinds: edges,
                    depth,
                    in_paths: paths,
                }
            })
            .collect();

        Ok(TraversalOutcome {
            steps,
            depth_capped,
            truncated,
        })
    }

    pub fn count_by_kind(&self) -> Result<BTreeMap<NodeKind, usize>, QueryError> {
        let mut counts = BTreeMap::new();
        for kind in NodeKind::all() {
            counts.insert(*kind, self.store.count_nodes_by_kind(*kind)?);
        }
        Ok(counts)
    }

    pub fn count_edges_by_kind(&self) -> Result<BTreeMap<EdgeKind, usize>, QueryError> {
        let mut counts = BTreeMap::new();
        for kind in EdgeKind::all() {
            let count = self.store.count_edges_by_kind(*kind)?;
            if count > 0 {
                counts.insert(*kind, count);
            }
        }
        Ok(counts)
    }

    pub fn resolution_fingerprint(&self) -> Result<Vec<String>, QueryError> {
        let mut labels: FxHashMap<[u8; 16], String> = FxHashMap::default();
        let mut node_ids: Vec<NodeId> = Vec::new();
        for kind in NodeKind::all() {
            for node in self.store.nodes_by_type(*kind)? {
                let label = node
                    .qualified_name
                    .clone()
                    .unwrap_or_else(|| format!("{}::{}", node.repo, node.name));
                labels.insert(node.id.as_bytes(), label);
                node_ids.push(node.id);
            }
        }

        let mut lines = Vec::new();
        for node_id in node_ids {
            for edge in self.store.get_outgoing(node_id)? {
                let source = labels
                    .get(&edge.source.as_bytes())
                    .map_or("<unknown>", String::as_str);
                let target = labels
                    .get(&edge.target.as_bytes())
                    .map_or("<unknown>", String::as_str);
                let resolver = edge.metadata.resolver.as_deref().unwrap_or("none");
                let confidence = edge
                    .metadata
                    .confidence
                    .map_or_else(|| "none".to_owned(), |value| value.to_string());
                lines.push(format!(
                    "{source}\t{}\t{target}\t{resolver}\t{confidence}",
                    edge.kind
                ));
            }
        }
        lines.sort();
        lines.dedup();
        Ok(lines)
    }
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
        EdgeKind, EdgeMetadata, NodeData, NodeKind, SourceSpan, Visibility, node_id,
    };
    use gather_step_storage::{GraphStore, GraphStoreDb};
    use pretty_assertions::assert_eq;

    use super::GraphQuery;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new(name: &str) -> Self {
            let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-analysis-{name}-{}-{id}.redb",
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

    fn test_store(path: &Path) -> GraphStoreDb {
        GraphStoreDb::open(path).expect("graph store should open")
    }

    fn node(repo: &str, file_path: &str, kind: NodeKind, name: &str, _ordinal: u16) -> NodeData {
        NodeData {
            id: node_id(repo, file_path, kind, name),
            kind,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(format!("{repo}::{name}")),
            external_id: None,
            signature: None,
            visibility: Some(Visibility::Public),
            span: Some(SourceSpan {
                line_start: 1,
                line_len: 0,
                column_start: 0,
                column_len: 1,
            }),
            is_virtual: false,
        }
    }

    fn edge(
        source: gather_step_core::NodeId,
        target: gather_step_core::NodeId,
        owner_file: gather_step_core::NodeId,
    ) -> gather_step_core::EdgeData {
        gather_step_core::EdgeData {
            source,
            target,
            kind: EdgeKind::Calls,
            metadata: EdgeMetadata::default(),
            owner_file,
            is_cross_file: false,
        }
    }

    #[test]
    fn supports_node_lookup_and_depth_limited_traversal() {
        let temp_db = TempDb::new("query");
        let store = test_store(temp_db.path());
        let file = node("service-a", "src/a.ts", NodeKind::File, "src/a.ts", 0);
        let a = node("service-a", "src/a.ts", NodeKind::Function, "a", 0);
        let b = node("service-a", "src/a.ts", NodeKind::Function, "b", 1);
        let c = node("service-a", "src/a.ts", NodeKind::Function, "c", 2);
        store
            .bulk_insert(
                &[file.clone(), a.clone(), b.clone(), c.clone()],
                &[edge(a.id, b.id, file.id), edge(b.id, c.id, file.id)],
            )
            .expect("graph should write");

        let query = GraphQuery::new(&store);
        assert_eq!(
            query.get_node(a.id).expect("lookup should succeed"),
            Some(a.clone())
        );
        assert_eq!(
            query
                .get_edges(a.id, Some(EdgeKind::Calls), None)
                .expect("edges should load")
                .len(),
            1
        );
        let traversed = query
            .traverse(a.id, &[EdgeKind::Calls], 2, None)
            .expect("traversal should succeed");
        assert_eq!(traversed.len(), 2);
        assert!(
            traversed
                .iter()
                .any(|step| step.node_id == b.id && step.depth == 1)
        );
        assert!(
            traversed
                .iter()
                .any(|step| step.node_id == c.id && step.depth == 2)
        );
    }

    #[test]
    fn min_confidence_filters_low_confidence_edges_but_keeps_structural() {
        let temp_db = TempDb::new("query-confidence");
        let store = test_store(temp_db.path());
        let file = node("service-a", "src/a.ts", NodeKind::File, "src/a.ts", 0);
        let a = node("service-a", "src/a.ts", NodeKind::Function, "a", 0);
        let trusted = node("service-a", "src/a.ts", NodeKind::Function, "trusted", 1);
        let guessed = node("service-a", "src/a.ts", NodeKind::Function, "guessed", 2);

        // a -> trusted has no confidence (a definite structural edge);
        // a -> guessed is a low-confidence heuristic resolution.
        let trusted_edge = edge(a.id, trusted.id, file.id);
        let mut guessed_edge = edge(a.id, guessed.id, file.id);
        guessed_edge.metadata.confidence = Some(300);

        store
            .bulk_insert(
                &[file.clone(), a.clone(), trusted.clone(), guessed.clone()],
                &[trusted_edge, guessed_edge],
            )
            .expect("graph should write");

        let query = GraphQuery::new(&store);

        // No threshold: both edges traversed.
        let all = query
            .traverse(a.id, &[EdgeKind::Calls], 1, None)
            .expect("traversal should succeed");
        assert_eq!(all.len(), 2);

        // Threshold above the heuristic edge: the low-confidence edge is
        // dropped, but the structural (None) edge is kept.
        let filtered = query
            .traverse(a.id, &[EdgeKind::Calls], 1, Some(500))
            .expect("traversal should succeed");
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].node_id, trusted.id);
    }

    #[test]
    fn traverse_with_provenance_flags_depth_capping() {
        let temp_db = TempDb::new("query-depth-cap");
        let store = test_store(temp_db.path());
        let file = node("service-a", "src/a.ts", NodeKind::File, "src/a.ts", 0);
        let a = node("service-a", "src/a.ts", NodeKind::Function, "a", 0);
        let b = node("service-a", "src/a.ts", NodeKind::Function, "b", 1);
        let c = node("service-a", "src/a.ts", NodeKind::Function, "c", 2);
        let d = node("service-a", "src/a.ts", NodeKind::Function, "d", 3);
        store
            .bulk_insert(
                &[file.clone(), a.clone(), b.clone(), c.clone(), d.clone()],
                &[
                    edge(a.id, b.id, file.id),
                    edge(b.id, c.id, file.id),
                    edge(c.id, d.id, file.id),
                ],
            )
            .expect("graph should write");

        let query = GraphQuery::new(&store);

        let capped = query
            .traverse_with_provenance(a.id, &[EdgeKind::Calls], 2, None)
            .expect("traversal should succeed");
        assert_eq!(capped.steps.len(), 2);
        assert!(
            capped.depth_capped,
            "deeper edge beyond max_depth not flagged"
        );
        assert!(!capped.truncated);

        let full = query
            .traverse_with_provenance(a.id, &[EdgeKind::Calls], 8, None)
            .expect("traversal should succeed");
        assert_eq!(full.steps.len(), 3);
        assert!(!full.depth_capped);
    }

    #[test]
    fn traverse_with_provenance_records_every_in_path() {
        let temp_db = TempDb::new("query-provenance");
        let store = test_store(temp_db.path());
        let file = node("service-a", "src/a.ts", NodeKind::File, "src/a.ts", 0);
        let root = node("service-a", "src/a.ts", NodeKind::Function, "root", 0);
        let a = node("service-a", "src/a.ts", NodeKind::Function, "a", 1);
        let b = node("service-a", "src/a.ts", NodeKind::Function, "b", 2);
        let c = node("service-a", "src/a.ts", NodeKind::Function, "c", 3);
        let sink = node("service-a", "src/a.ts", NodeKind::Function, "sink", 4);
        store
            .bulk_insert(
                &[
                    file.clone(),
                    root.clone(),
                    a.clone(),
                    b.clone(),
                    c.clone(),
                    sink.clone(),
                ],
                &[
                    edge(root.id, a.id, file.id),
                    edge(root.id, b.id, file.id),
                    edge(root.id, c.id, file.id),
                    edge(a.id, sink.id, file.id),
                    edge(b.id, sink.id, file.id),
                    edge(c.id, sink.id, file.id),
                ],
            )
            .expect("graph should write");

        let query = GraphQuery::new(&store);
        let outcome = query
            .traverse_with_provenance(root.id, &[EdgeKind::Calls], 4, None)
            .expect("traversal should succeed");

        let sink_step = outcome
            .steps
            .iter()
            .find(|step| step.node_id == sink.id)
            .expect("sink should be reached");
        assert_eq!(
            sink_step.in_paths.len(),
            3,
            "a node reachable three ways should report three caller paths"
        );
    }

    #[test]
    fn traverse_with_provenance_flags_fan_out_truncation() {
        let temp_db = TempDb::new("query-fan-out");
        let store = test_store(temp_db.path());
        let file = node("service-a", "src/a.ts", NodeKind::File, "src/a.ts", 0);
        let hub = node("service-a", "src/a.ts", NodeKind::Function, "hub", 0);

        let fan_out = 300_u16;
        let mut nodes = vec![file.clone(), hub.clone()];
        let mut edges = Vec::new();
        for ordinal in 1..=fan_out {
            let leaf = node(
                "service-a",
                "src/a.ts",
                NodeKind::Function,
                &format!("leaf{ordinal}"),
                ordinal,
            );
            edges.push(edge(hub.id, leaf.id, file.id));
            nodes.push(leaf);
        }
        store
            .bulk_insert(&nodes, &edges)
            .expect("graph should write");

        let query = GraphQuery::new(&store);
        let outcome = query
            .traverse_with_provenance(hub.id, &[EdgeKind::Calls], 2, None)
            .expect("traversal should succeed");
        assert!(
            outcome.truncated,
            "fan-out above the cap must flag truncated"
        );
        assert!(
            outcome.steps.len() <= 256,
            "truncated traversal must not exceed the fan-out cap: {}",
            outcome.steps.len()
        );
    }

    #[test]
    fn counts_nodes_and_edges_by_kind() {
        let temp_db = TempDb::new("counts");
        let store = test_store(temp_db.path());
        let file = node("service-a", "src/a.ts", NodeKind::File, "src/a.ts", 0);
        let a = node("service-a", "src/a.ts", NodeKind::Function, "a", 0);
        let b = node("service-a", "src/a.ts", NodeKind::Function, "b", 1);
        store
            .bulk_insert(
                &[file.clone(), a.clone(), b.clone()],
                &[edge(a.id, b.id, file.id)],
            )
            .expect("graph should write");

        let query = GraphQuery::new(&store);
        let node_counts = query.count_by_kind().expect("node counts should load");
        let edge_counts = query
            .count_edges_by_kind()
            .expect("edge counts should load");
        assert_eq!(node_counts.get(&NodeKind::Function), Some(&2));
        assert_eq!(edge_counts.get(&EdgeKind::Calls), Some(&1));
    }
}
