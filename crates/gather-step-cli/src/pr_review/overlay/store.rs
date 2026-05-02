//! `DiffOverlayStore` — Phase 5 Task 2 prototype.
//!
//! A read-only [`GraphStore`] implementation that layers added/changed/removed
//! nodes and edges over an immutable baseline `GraphStore` without mutating it.
//!
//! # Read semantics
//!
//! - `get_node(id)`: overlay `changed_nodes` wins; if tombstoned returns `None`;
//!   otherwise falls through to baseline.
//! - `nodes_by_type(kind)`: baseline results minus tombstones, merged with
//!   added + changed nodes of the requested kind.
//! - `nodes_by_repo(repo)`: same pattern.
//! - `get_incoming(id)`: baseline minus tombstoned edges, plus added edges
//!   targeting `id`.
//! - `get_outgoing(id)`: symmetric.
//!
//! # Write / unsupported methods
//!
//! `DiffOverlayStore` is intentionally read-only for extractors.  Any trait
//! method not used by Phase 2 extractors returns
//! `Err(GraphStoreError::storage("DiffOverlayStore: method not supported"))`.

use rustc_hash::{FxHashMap, FxHashSet};

use gather_step_core::{EdgeData, EdgeKind, NodeData, NodeId, NodeKind};
use gather_step_storage::{GraphStore, GraphStoreError};

// ─── Key types ────────────────────────────────────────────────────────────────

/// Tombstone key for a removed edge `(source, target, edge_kind_discriminant)`.
type EdgeTombstone = (NodeId, NodeId, u8);

fn edge_kind_byte(kind: EdgeKind) -> u8 {
    kind as u8
}

// ─── DiffOverlayStore ─────────────────────────────────────────────────────────

/// Overlay graph that layers added/changed/removed nodes over a baseline
/// [`GraphStore`] without mutating the baseline.
///
/// Reads consult the overlay first, then fall through to the baseline.
/// Tombstones suppress baseline rows during reads.
pub struct DiffOverlayStore<'a, S: GraphStore> {
    baseline: &'a S,
    /// Nodes added by the PR (not present in baseline).
    added_nodes: FxHashMap<NodeId, NodeData>,
    /// Nodes whose payload changed relative to baseline.
    changed_nodes: FxHashMap<NodeId, NodeData>,
    /// Node IDs that were removed in the PR.
    removed_nodes: FxHashSet<NodeId>,
    /// Edges added by the PR.
    added_edges: Vec<EdgeData>,
    /// Edge tombstones for edges removed in the PR.
    removed_edges: FxHashSet<EdgeTombstone>,
}

impl<'a, S: GraphStore> DiffOverlayStore<'a, S> {
    /// Create an empty overlay over `baseline`.
    pub fn new(baseline: &'a S) -> Self {
        Self {
            baseline,
            added_nodes: FxHashMap::default(),
            changed_nodes: FxHashMap::default(),
            removed_nodes: FxHashSet::default(),
            added_edges: Vec::new(),
            removed_edges: FxHashSet::default(),
        }
    }

    /// Record a node that exists in the PR but not in the baseline.
    pub fn add_node(&mut self, node: NodeData) {
        self.added_nodes.insert(node.id, node);
    }

    /// Record a node whose payload changed relative to baseline.
    pub fn change_node(&mut self, node: NodeData) {
        self.changed_nodes.insert(node.id, node);
    }

    /// Tombstone a node that was removed in the PR.
    pub fn remove_node(&mut self, id: NodeId) {
        self.removed_nodes.insert(id);
    }

    /// Record an edge added by the PR.
    pub fn add_edge(&mut self, edge: EdgeData) {
        self.added_edges.push(edge);
    }

    /// Tombstone an edge removed in the PR.
    pub fn remove_edge(&mut self, source: NodeId, target: NodeId, kind: EdgeKind) {
        self.removed_edges
            .insert((source, target, edge_kind_byte(kind)));
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    fn is_node_tombstoned(&self, id: NodeId) -> bool {
        self.removed_nodes.contains(&id)
    }

    fn is_edge_tombstoned(&self, edge: &EdgeData) -> bool {
        self.removed_edges
            .contains(&(edge.source, edge.target, edge_kind_byte(edge.kind)))
    }

    /// Collect all overlay nodes (added + changed) of the requested kind.
    fn overlay_nodes_of_kind(&self, kind: NodeKind) -> impl Iterator<Item = &NodeData> {
        self.added_nodes
            .values()
            .chain(self.changed_nodes.values())
            .filter(move |n| n.kind == kind)
    }
}

// ─── GraphStore impl ──────────────────────────────────────────────────────────

fn unsupported(method: &'static str) -> GraphStoreError {
    GraphStoreError::Storage(format!("DiffOverlayStore: method not supported: {method}"))
}

impl<S: GraphStore> GraphStore for DiffOverlayStore<'_, S> {
    // ── Supported read methods ────────────────────────────────────────────────

    fn get_node(&self, id: NodeId) -> Result<Option<NodeData>, GraphStoreError> {
        // changed wins over baseline
        if let Some(node) = self.changed_nodes.get(&id) {
            return Ok(Some(node.clone()));
        }
        // added nodes are new — they won't be in baseline
        if let Some(node) = self.added_nodes.get(&id) {
            return Ok(Some(node.clone()));
        }
        // tombstone suppresses baseline
        if self.is_node_tombstoned(id) {
            return Ok(None);
        }
        self.baseline.get_node(id)
    }

    fn nodes_by_type(&self, kind: NodeKind) -> Result<Vec<NodeData>, GraphStoreError> {
        let mut baseline = self.baseline.nodes_by_type(kind)?;

        // Remove tombstoned or changed nodes from baseline result.
        baseline
            .retain(|n| !self.is_node_tombstoned(n.id) && !self.changed_nodes.contains_key(&n.id));

        // Add overlay nodes of this kind (changed win over baseline; we already
        // stripped baseline changed above, so add all overlay of this kind).
        for node in self.overlay_nodes_of_kind(kind) {
            baseline.push(node.clone());
        }

        Ok(baseline)
    }

    fn nodes_by_repo(&self, repo: &str) -> Result<Vec<NodeData>, GraphStoreError> {
        let mut baseline = self.baseline.nodes_by_repo(repo)?;

        baseline
            .retain(|n| !self.is_node_tombstoned(n.id) && !self.changed_nodes.contains_key(&n.id));

        // Overlay nodes for this repo.
        for node in self
            .added_nodes
            .values()
            .chain(self.changed_nodes.values())
            .filter(|n| n.repo == repo)
        {
            baseline.push(node.clone());
        }

        Ok(baseline)
    }

    fn get_incoming(&self, target: NodeId) -> Result<Vec<EdgeData>, GraphStoreError> {
        let mut edges = self.baseline.get_incoming(target)?;

        // Suppress tombstoned edges.
        edges.retain(|e| !self.is_edge_tombstoned(e));

        // Add overlay edges targeting this node.
        for edge in self.added_edges.iter().filter(|e| e.target == target) {
            edges.push(edge.clone());
        }

        Ok(edges)
    }

    fn get_outgoing(&self, source: NodeId) -> Result<Vec<EdgeData>, GraphStoreError> {
        let mut edges = self.baseline.get_outgoing(source)?;

        // Suppress tombstoned edges.
        edges.retain(|e| !self.is_edge_tombstoned(e));

        // Add overlay edges from this node.
        for edge in self.added_edges.iter().filter(|e| e.source == source) {
            edges.push(edge.clone());
        }

        Ok(edges)
    }

    // ── Unsupported write methods ─────────────────────────────────────────────

    fn insert_node(&self, _node: &NodeData) -> Result<(), GraphStoreError> {
        Err(unsupported("insert_node"))
    }

    fn delete_node(&self, _id: NodeId) -> Result<Option<NodeData>, GraphStoreError> {
        Err(unsupported("delete_node"))
    }

    fn insert_edge(&self, _edge: &EdgeData) -> Result<(), GraphStoreError> {
        Err(unsupported("insert_edge"))
    }

    fn delete_edge(&self, _edge: &EdgeData) -> Result<(), GraphStoreError> {
        Err(unsupported("delete_edge"))
    }

    fn edges_by_owner(&self, _owner_file: NodeId) -> Result<Vec<EdgeData>, GraphStoreError> {
        Err(unsupported("edges_by_owner"))
    }

    fn delete_edges_for_owner(&self, _owner_file: NodeId) -> Result<(), GraphStoreError> {
        Err(unsupported("delete_edges_for_owner"))
    }

    fn delete_edges_for_owner_by_kind(
        &self,
        _owner_file: NodeId,
        _kinds: &[EdgeKind],
    ) -> Result<(), GraphStoreError> {
        Err(unsupported("delete_edges_for_owner_by_kind"))
    }

    fn replace_edges_for_owners_by_kind(
        &self,
        _owner_files: &[NodeId],
        _kinds: &[EdgeKind],
        _edges: &[EdgeData],
    ) -> Result<(), GraphStoreError> {
        Err(unsupported("replace_edges_for_owners_by_kind"))
    }

    fn nodes_by_file(
        &self,
        _repo: &str,
        _file_path: &str,
    ) -> Result<Vec<NodeData>, GraphStoreError> {
        Err(unsupported("nodes_by_file"))
    }

    fn count_nodes_by_repo(&self, _repo: &str) -> Result<usize, GraphStoreError> {
        Err(unsupported("count_nodes_by_repo"))
    }

    fn count_nodes_by_repo_and_kind(
        &self,
        _repo: &str,
        _kind: NodeKind,
    ) -> Result<usize, GraphStoreError> {
        Err(unsupported("count_nodes_by_repo_and_kind"))
    }

    fn count_edges_by_owner_repo(&self, _repo: &str) -> Result<u64, GraphStoreError> {
        Err(unsupported("count_edges_by_owner_repo"))
    }

    fn nodes_by_external_id(
        &self,
        _kind: NodeKind,
        _external_id: &str,
    ) -> Result<Vec<NodeData>, GraphStoreError> {
        Err(unsupported("nodes_by_external_id"))
    }

    fn nodes_by_candidate_keys(
        &self,
        _candidate_keys: &[String],
    ) -> Result<Vec<NodeData>, GraphStoreError> {
        Err(unsupported("nodes_by_candidate_keys"))
    }

    fn count_nodes_by_kind(&self, _kind: NodeKind) -> Result<usize, GraphStoreError> {
        Err(unsupported("count_nodes_by_kind"))
    }

    fn count_edges_by_kind(&self, _kind: EdgeKind) -> Result<usize, GraphStoreError> {
        Err(unsupported("count_edges_by_kind"))
    }

    fn nodes_by_event_family_name(
        &self,
        _normalized_name: &str,
    ) -> Result<Vec<NodeData>, GraphStoreError> {
        Err(unsupported("nodes_by_event_family_name"))
    }

    fn nodes_by_route_key(&self, _canonical_key: &str) -> Result<Vec<NodeData>, GraphStoreError> {
        Err(unsupported("nodes_by_route_key"))
    }

    fn nodes_by_shared_symbol_name(
        &self,
        _short_name: &str,
    ) -> Result<Vec<NodeData>, GraphStoreError> {
        Err(unsupported("nodes_by_shared_symbol_name"))
    }

    fn bulk_insert(&self, _nodes: &[NodeData], _edges: &[EdgeData]) -> Result<(), GraphStoreError> {
        Err(unsupported("bulk_insert"))
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use gather_step_core::{
        EdgeKind, EdgeMetadata, NodeData, NodeId, NodeKind, SourceSpan, Visibility, node_id,
    };
    use gather_step_storage::{GraphStore, GraphStoreDb};

    use super::DiffOverlayStore;

    // ── baseline helpers ──────────────────────────────────────────────────────

    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
        time::SystemTime,
    };

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_db_path() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("gs-overlay-test-{nanos}-{counter}.redb"))
    }

    fn open_baseline() -> GraphStoreDb {
        GraphStoreDb::open(temp_db_path()).expect("open temp GraphStoreDb")
    }

    fn function_node(repo: &str, file: &str, name: &str) -> NodeData {
        NodeData {
            id: node_id(repo, file, NodeKind::Function, name),
            kind: NodeKind::Function,
            repo: repo.to_owned(),
            file_path: file.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(format!("{repo}::{name}")),
            external_id: None,
            signature: Some(format!("{name}(): void")),
            visibility: Some(Visibility::Public),
            span: Some(SourceSpan {
                line_start: 1,
                line_len: 10,
                column_start: 0,
                column_len: 0,
            }),
            is_virtual: false,
        }
    }

    fn make_edge(source: NodeId, target: NodeId) -> gather_step_core::EdgeData {
        gather_step_core::EdgeData {
            source,
            target,
            kind: EdgeKind::Calls,
            owner_file: source,
            is_cross_file: false,
            metadata: EdgeMetadata::default(),
        }
    }

    // ── 1. Added node appears in nodes_by_type ────────────────────────────────

    #[test]
    fn overlay_added_node_appears_in_nodes_by_type() {
        let baseline = open_baseline();
        let mut overlay = DiffOverlayStore::new(&baseline);

        let node = function_node("repo", "src/a.ts", "newFn");
        let node_id = node.id;
        overlay.add_node(node.clone());

        let results = overlay
            .nodes_by_type(NodeKind::Function)
            .expect("nodes_by_type");

        assert!(
            results.iter().any(|n| n.id == node_id),
            "added node must appear in nodes_by_type result"
        );
    }

    // ── 2. Changed node overrides baseline payload ────────────────────────────

    #[test]
    fn overlay_changed_node_overrides_baseline_payload() {
        let baseline = open_baseline();

        // Insert a v1 node into baseline.
        let mut v1 = function_node("repo", "src/b.ts", "changedFn");
        v1.signature = Some("changedFn(): string_v1".to_owned());
        baseline.insert_node(&v1).expect("insert v1");

        let mut overlay = DiffOverlayStore::new(&baseline);

        // Override with v2 signature.
        let mut v2 = v1.clone();
        v2.signature = Some("changedFn(): string_v2".to_owned());
        overlay.change_node(v2.clone());

        let result = overlay
            .get_node(v1.id)
            .expect("get_node")
            .expect("node should exist");

        assert_eq!(
            result.signature.as_deref(),
            Some("changedFn(): string_v2"),
            "changed overlay must win over baseline payload"
        );
    }

    // ── 3. Removed node returns None ─────────────────────────────────────────

    #[test]
    fn overlay_removed_node_returns_none_from_get_node() {
        let baseline = open_baseline();

        let node = function_node("repo", "src/c.ts", "removedFn");
        baseline.insert_node(&node).expect("insert node");

        let mut overlay = DiffOverlayStore::new(&baseline);
        overlay.remove_node(node.id);

        let result = overlay.get_node(node.id).expect("get_node");
        assert!(
            result.is_none(),
            "tombstoned node must return None from get_node"
        );
    }

    // ── 4. Added edge appears in get_incoming ─────────────────────────────────

    #[test]
    fn overlay_added_edge_appears_in_get_incoming() {
        let baseline = open_baseline();

        let src = function_node("repo", "src/d.ts", "caller");
        let tgt = function_node("repo", "src/e.ts", "callee");

        // Insert nodes so the edge references valid IDs (not required by
        // DiffOverlayStore but good practice for realistic tests).
        baseline.insert_node(&src).expect("insert src");
        baseline.insert_node(&tgt).expect("insert tgt");

        let mut overlay = DiffOverlayStore::new(&baseline);
        overlay.add_edge(make_edge(src.id, tgt.id));

        let incoming = overlay.get_incoming(tgt.id).expect("get_incoming");
        assert!(
            incoming
                .iter()
                .any(|e| e.source == src.id && e.target == tgt.id),
            "overlay added edge must appear in get_incoming(target)"
        );
    }
}
