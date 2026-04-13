use std::collections::{BTreeMap, VecDeque};

use gather_step_core::{EdgeData, EdgeKind, NodeData, NodeId, NodeKind};
use gather_step_storage::{GraphStore, GraphStoreError};
use rustc_hash::FxHashSet;
use thiserror::Error;

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
    ) -> Result<Vec<EdgeData>, QueryError> {
        let mut edges = self.store.get_outgoing(source)?;
        if let Some(edge_kind) = edge_kind {
            edges.retain(|edge| edge.kind == edge_kind);
        }
        Ok(edges)
    }

    pub fn get_reverse_edges(
        &self,
        target: NodeId,
        edge_kind: Option<EdgeKind>,
    ) -> Result<Vec<EdgeData>, QueryError> {
        let mut edges = self.store.get_incoming(target)?;
        if let Some(edge_kind) = edge_kind {
            edges.retain(|edge| edge.kind == edge_kind);
        }
        Ok(edges)
    }

    pub fn traverse(
        &self,
        start: NodeId,
        edge_kinds: &[EdgeKind],
        max_depth: usize,
    ) -> Result<Vec<TraversalStep>, QueryError> {
        let mut queue = VecDeque::from([(start, Vec::<EdgeKind>::new(), 0_usize)]);
        let mut seen = FxHashSet::from_iter([start.as_bytes()]);
        let mut steps = Vec::new();

        while let Some((node_id, path, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }

            for edge in self.store.get_outgoing(node_id)? {
                if !edge_kinds.is_empty() && !edge_kinds.contains(&edge.kind) {
                    continue;
                }
                let mut next_path = path.clone();
                next_path.push(edge.kind);
                if seen.insert(edge.target.as_bytes()) {
                    steps.push(TraversalStep {
                        node_id: edge.target,
                        edge_kinds: next_path.clone(),
                        depth: depth + 1,
                    });
                    queue.push_back((edge.target, next_path, depth + 1));
                }
            }
        }

        Ok(steps)
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
                .get_edges(a.id, Some(EdgeKind::Calls))
                .expect("edges should load")
                .len(),
            1
        );
        let traversed = query
            .traverse(a.id, &[EdgeKind::Calls], 2)
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
