//! Per-repo, transitive cross-repo participation analysis (v5.1 Part 2,
//! Task 6).
//!
//! [`cross_repo_participation_by_file`] maps each file in a repo to the set of
//! *other* repos that consume something the file produces, either directly
//! (a cross-repo edge) or through a transport boundary (a virtual Route /
//! Topic / Queue / Event node that a foreign repo consumes), then forward-
//! propagates those annotations along intra-repo `owner_file -> target_file`
//! adjacency so that files feeding a producer (e.g. a config referenced by a
//! route handler) inherit the same foreign consumers.
//!
//! It is built to be called once per repo and run in `O(files + edges)`: the
//! intra-repo adjacency is assembled with one [`GraphStore::edges_by_owner`]
//! scan per *file* (the same shape as [`crate::dead_code`]), never one scan
//! per node.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use gather_step_core::{NodeId, VIRTUAL_NODE_REPO};
use gather_step_storage::{GraphStore, GraphStoreError};
use rustc_hash::FxHashMap;

/// Map each file in `repo` to the set of *foreign* repos (repos other than
/// `repo`, and never the synthetic [`VIRTUAL_NODE_REPO`]) that participate as
/// consumers of what the file produces.
///
/// Stages:
/// - **(a)** Seed producer files of transport (virtual) nodes that have a
///   foreign consumer. Each producer's outgoing edges name the virtual nodes;
///   each virtual node's incoming/outgoing edges are scanned once to find
///   consumers in a different real repo.
/// - **(b)** Seed files owning a node with a direct cross-repo edge to another
///   real repo.
/// - **(c)** Forward-propagate the seeded annotations along the intra-repo
///   `owner_file -> target_file` adjacency via BFS, so files referenced by a
///   producer inherit its foreign consumers.
///
/// Returns `file_path -> consumer repos`, excluding `repo` itself and
/// [`VIRTUAL_NODE_REPO`]. Files with no foreign consumer are omitted.
pub fn cross_repo_participation_by_file<S: GraphStore>(
    store: &S,
    repo: &str,
) -> Result<BTreeMap<String, BTreeSet<String>>, GraphStoreError> {
    let nodes = store.nodes_by_repo(repo)?;

    // file_path -> File node id for this repo.
    let mut file_ids = BTreeMap::<String, NodeId>::new();
    for node in &nodes {
        if node.kind == gather_step_core::NodeKind::File {
            file_ids.insert(node.file_path.clone(), node.id);
        }
    }

    // Owning file for any node in this repo (symbols resolve to their file).
    let mut owning_file = FxHashMap::<NodeId, String>::default();
    for node in &nodes {
        owning_file.insert(node.id, node.file_path.clone());
    }

    // file_path -> foreign consumer repos. Only files that are seeded or
    // reached during propagation appear.
    let mut consumers = BTreeMap::<String, BTreeSet<String>>::new();
    let mut add_consumer = |file_path: &str, consumer_repo: &str| {
        consumers
            .entry(file_path.to_owned())
            .or_default()
            .insert(consumer_repo.to_owned());
    };

    // ── (a) transport-mediated seeds ──────────────────────────────────────
    //
    // Collect the virtual nodes this repo produces (the targets of its
    // outgoing edges that resolve to virtual nodes), keyed to the producing
    // owner file. Each virtual node is then scanned exactly once for foreign
    // consumers, mirroring `cross_repo_deps`'s reverse/forward hop walk.
    let mut virtual_producers = FxHashMap::<NodeId, BTreeSet<String>>::default();
    for node in &nodes {
        for edge in store.get_outgoing(node.id)? {
            let Some(target) = store.get_node(edge.target)? else {
                continue;
            };
            if !target.is_virtual {
                continue;
            }
            if let Some(producer_file) = owning_file.get(&edge.owner_file) {
                virtual_producers
                    .entry(target.id)
                    .or_default()
                    .insert(producer_file.clone());
            }
        }
    }

    for (virtual_id, producer_files) in &virtual_producers {
        let mut foreign_consumer_repos = BTreeSet::<String>::new();
        for related in store.get_incoming(*virtual_id)? {
            if let Some(source) = store.get_node(related.source)?
                && is_foreign_repo(&source.repo, repo)
            {
                foreign_consumer_repos.insert(source.repo);
            }
        }
        for related in store.get_outgoing(*virtual_id)? {
            if let Some(target) = store.get_node(related.target)?
                && is_foreign_repo(&target.repo, repo)
            {
                foreign_consumer_repos.insert(target.repo);
            }
        }
        if foreign_consumer_repos.is_empty() {
            continue;
        }
        for producer_file in producer_files {
            for consumer_repo in &foreign_consumer_repos {
                add_consumer(producer_file, consumer_repo);
            }
        }
    }

    // ── (b) direct cross-repo seeds ───────────────────────────────────────
    //
    // Cross-repo `...From` edges point consumer -> producer (the consumer is
    // `source`, the producer/transport is `target`). So a producer in this
    // repo is found by walking each node's *incoming* edges: an incoming edge
    // whose source is a non-virtual node in a foreign real repo is a direct
    // consumer of `node`. Attribute the producer node's own file to the
    // consumer's repo (on an incoming edge `owner_file` is the consumer's
    // file, so it must not be used here).
    for node in &nodes {
        let Some(producer_file) = owning_file.get(&node.id) else {
            continue;
        };
        for edge in store.get_incoming(node.id)? {
            let Some(source) = store.get_node(edge.source)? else {
                continue;
            };
            if source.is_virtual || !is_foreign_repo(&source.repo, repo) {
                continue;
            }
            add_consumer(producer_file, &source.repo);
        }
    }

    // ── (c) forward propagation along owner_file -> target_file adjacency ──
    //
    // Build the same per-file adjacency `dead_code` builds: one
    // `edges_by_owner` scan per File node (O(file_count) store calls, not
    // O(node_count)). An edge `owner_file -> file(target)` means the owner
    // depends on / produces into the target's file, so foreign consumers of
    // the owner flow forward to the files it references.
    let mut adjacency = BTreeMap::<String, BTreeSet<String>>::new();
    for (file_path, file_id) in &file_ids {
        for edge in store.edges_by_owner(*file_id)? {
            let Some(target_file) = owning_file.get(&edge.target) else {
                continue;
            };
            if target_file == file_path {
                continue;
            }
            if !file_ids.contains_key(target_file) {
                continue;
            }
            adjacency
                .entry(file_path.clone())
                .or_default()
                .insert(target_file.clone());
        }
    }

    let seeds: Vec<String> = consumers.keys().cloned().collect();
    let mut queue: VecDeque<String> = seeds.into_iter().collect();
    while let Some(file_path) = queue.pop_front() {
        let Some(targets) = adjacency.get(&file_path) else {
            continue;
        };
        let inherited = consumers.get(&file_path).cloned().unwrap_or_default();
        if inherited.is_empty() {
            continue;
        }
        for target in targets.clone() {
            let entry = consumers.entry(target.clone()).or_default();
            let mut changed = false;
            for repo_name in &inherited {
                changed |= entry.insert(repo_name.clone());
            }
            if changed {
                queue.push_back(target);
            }
        }
    }

    Ok(consumers)
}

/// A repo is a foreign consumer when it is neither the analysed `repo` nor the
/// synthetic [`VIRTUAL_NODE_REPO`] (virtual transport stubs are never a real
/// consuming repo, and same-repo consumers are excluded by design).
fn is_foreign_repo(candidate: &str, repo: &str) -> bool {
    candidate != repo && candidate != VIRTUAL_NODE_REPO
}

/// Per-repo memoized lookup over [`cross_repo_participation_by_file`].
///
/// The participation primitive is computed once per *distinct* repo and cached;
/// repeated `consumer_repos(repo, file)` calls for the same repo reuse the
/// cached projection rather than re-walking the graph. This is the shape both
/// the CLI and MCP `search` surfaces use to annotate hits without paying a
/// per-hit graph scan (one scan per distinct hit repo instead).
#[derive(Default)]
pub struct CrossRepoConsumerLookup {
    by_repo: BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
}

impl CrossRepoConsumerLookup {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the sorted set of foreign consumer repos for `file_path` in
    /// `repo`, computing and caching the whole-repo projection on first use.
    ///
    /// Files with no foreign consumer yield an empty slice. Errors from the
    /// underlying graph store propagate so a caller can decide whether to fail
    /// or fall back to an empty annotation.
    pub fn consumer_repos<S: GraphStore>(
        &mut self,
        store: &S,
        repo: &str,
        file_path: &str,
    ) -> Result<Vec<String>, GraphStoreError> {
        if !self.by_repo.contains_key(repo) {
            let map = cross_repo_participation_by_file(store, repo)?;
            self.by_repo.insert(repo.to_owned(), map);
        }
        Ok(self
            .by_repo
            .get(repo)
            .and_then(|files| files.get(file_path))
            .map(|repos| repos.iter().cloned().collect())
            .unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use gather_step_core::{
        EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeId, NodeKind, Visibility, node_id,
    };
    use gather_step_storage::{GraphStore, GraphStoreDb, GraphStoreError};

    use super::cross_repo_participation_by_file;
    use crate::test_utils::TempDb;

    fn file(repo: &str, file_path: &str) -> NodeData {
        NodeData {
            id: node_id(repo, file_path, NodeKind::File, file_path),
            kind: NodeKind::File,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: file_path.to_owned(),
            qualified_name: None,
            external_id: None,
            signature: None,
            visibility: None,
            span: None,
            is_virtual: false,
            ai_role: None,
        }
    }

    fn symbol(repo: &str, file_path: &str, name: &str) -> NodeData {
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
            span: None,
            is_virtual: false,
            ai_role: None,
        }
    }

    fn defines(owner: NodeId, target: NodeId) -> EdgeData {
        EdgeData {
            source: owner,
            target,
            kind: EdgeKind::Defines,
            metadata: EdgeMetadata::default(),
            owner_file: owner,
            is_cross_file: false,
        }
    }

    fn edge(owner: NodeId, source: NodeId, target: NodeId, kind: EdgeKind) -> EdgeData {
        EdgeData {
            source,
            target,
            kind,
            metadata: EdgeMetadata::default(),
            owner_file: owner,
            is_cross_file: true,
        }
    }

    /// Delegating wrapper that counts `edges_by_owner` calls so the
    /// propagation stage can be proven to issue one scan per *file*, not one
    /// per *node*. Every other method forwards to the inner store unchanged.
    struct CountingStore<'a> {
        inner: &'a GraphStoreDb,
        edges_by_owner_calls: Cell<usize>,
    }

    impl<'a> CountingStore<'a> {
        fn new(inner: &'a GraphStoreDb) -> Self {
            Self {
                inner,
                edges_by_owner_calls: Cell::new(0),
            }
        }
    }

    impl GraphStore for CountingStore<'_> {
        fn edges_by_owner(&self, owner_file: NodeId) -> Result<Vec<EdgeData>, GraphStoreError> {
            self.edges_by_owner_calls
                .set(self.edges_by_owner_calls.get() + 1);
            self.inner.edges_by_owner(owner_file)
        }

        fn insert_node(&self, node: &NodeData) -> Result<(), GraphStoreError> {
            self.inner.insert_node(node)
        }
        fn get_node(&self, id: NodeId) -> Result<Option<NodeData>, GraphStoreError> {
            self.inner.get_node(id)
        }
        fn delete_node(&self, id: NodeId) -> Result<Option<NodeData>, GraphStoreError> {
            self.inner.delete_node(id)
        }
        fn insert_edge(&self, edge: &EdgeData) -> Result<(), GraphStoreError> {
            self.inner.insert_edge(edge)
        }
        fn delete_edge(&self, edge: &EdgeData) -> Result<(), GraphStoreError> {
            self.inner.delete_edge(edge)
        }
        fn get_outgoing(&self, source: NodeId) -> Result<Vec<EdgeData>, GraphStoreError> {
            self.inner.get_outgoing(source)
        }
        fn get_incoming(&self, target: NodeId) -> Result<Vec<EdgeData>, GraphStoreError> {
            self.inner.get_incoming(target)
        }
        fn delete_edges_for_owner(&self, owner_file: NodeId) -> Result<(), GraphStoreError> {
            self.inner.delete_edges_for_owner(owner_file)
        }
        fn delete_edges_for_owner_by_kind(
            &self,
            owner_file: NodeId,
            kinds: &[EdgeKind],
        ) -> Result<(), GraphStoreError> {
            self.inner.delete_edges_for_owner_by_kind(owner_file, kinds)
        }
        fn replace_edges_for_owners_by_kind(
            &self,
            owner_files: &[NodeId],
            kinds: &[EdgeKind],
            edges: &[EdgeData],
        ) -> Result<(), GraphStoreError> {
            self.inner
                .replace_edges_for_owners_by_kind(owner_files, kinds, edges)
        }
        fn nodes_by_file(
            &self,
            repo: &str,
            file_path: &str,
        ) -> Result<Vec<NodeData>, GraphStoreError> {
            self.inner.nodes_by_file(repo, file_path)
        }
        fn nodes_by_repo(&self, repo: &str) -> Result<Vec<NodeData>, GraphStoreError> {
            self.inner.nodes_by_repo(repo)
        }
        fn count_nodes_by_repo(&self, repo: &str) -> Result<usize, GraphStoreError> {
            self.inner.count_nodes_by_repo(repo)
        }
        fn count_nodes_by_repo_and_kind(
            &self,
            repo: &str,
            kind: NodeKind,
        ) -> Result<usize, GraphStoreError> {
            self.inner.count_nodes_by_repo_and_kind(repo, kind)
        }
        fn count_edges_by_owner_repo(&self, repo: &str) -> Result<u64, GraphStoreError> {
            self.inner.count_edges_by_owner_repo(repo)
        }
        fn nodes_by_external_id(
            &self,
            kind: NodeKind,
            external_id: &str,
        ) -> Result<Vec<NodeData>, GraphStoreError> {
            self.inner.nodes_by_external_id(kind, external_id)
        }
        fn nodes_by_type(&self, kind: NodeKind) -> Result<Vec<NodeData>, GraphStoreError> {
            self.inner.nodes_by_type(kind)
        }
        fn nodes_by_candidate_keys(
            &self,
            candidate_keys: &[String],
        ) -> Result<Vec<NodeData>, GraphStoreError> {
            self.inner.nodes_by_candidate_keys(candidate_keys)
        }
        fn count_nodes_by_kind(&self, kind: NodeKind) -> Result<usize, GraphStoreError> {
            self.inner.count_nodes_by_kind(kind)
        }
        fn count_edges_by_kind(&self, kind: EdgeKind) -> Result<usize, GraphStoreError> {
            self.inner.count_edges_by_kind(kind)
        }
        fn nodes_by_event_family_name(
            &self,
            normalized_name: &str,
        ) -> Result<Vec<NodeData>, GraphStoreError> {
            self.inner.nodes_by_event_family_name(normalized_name)
        }
        fn nodes_by_route_key(
            &self,
            canonical_key: &str,
        ) -> Result<Vec<NodeData>, GraphStoreError> {
            self.inner.nodes_by_route_key(canonical_key)
        }
        fn nodes_by_shared_symbol_name(
            &self,
            short_name: &str,
        ) -> Result<Vec<NodeData>, GraphStoreError> {
            self.inner.nodes_by_shared_symbol_name(short_name)
        }
        fn bulk_insert(
            &self,
            nodes: &[NodeData],
            edges: &[EdgeData],
        ) -> Result<(), GraphStoreError> {
            self.inner.bulk_insert(nodes, edges)
        }
    }

    /// A direct, non-virtual cross-repo `...From` edge points consumer ->
    /// producer (the consumer is `source`, the producer is `target`). When the
    /// PRODUCER lives in the analysed repo, its file must be attributed to the
    /// foreign CONSUMER's repo — the function reports who consumes what this
    /// repo produces. This guards stage (b)'s edge direction.
    #[test]
    fn direct_non_virtual_consumer_seeds_producer_file_with_consumer_repo() {
        let temp_db = TempDb::new("xrepo-participation-unit", "direct-inbound");
        let store = GraphStoreDb::open(temp_db.path()).expect("store should open");

        // Producer side: the analysed repo exports a symbol.
        let producer_file = file("producer", "src/api.ts");
        let producer_sym = symbol("producer", "src/api.ts", "exportedThing");
        // Consumer side: a foreign real repo imports it (consumer -> producer).
        let consumer_file = file("consumer", "src/uses.ts");
        let consumer_sym = symbol("consumer", "src/uses.ts", "usesThing");

        store
            .bulk_insert(
                &[
                    producer_file.clone(),
                    producer_sym.clone(),
                    consumer_file.clone(),
                    consumer_sym.clone(),
                ],
                &[
                    defines(producer_file.id, producer_sym.id),
                    defines(consumer_file.id, consumer_sym.id),
                    // Direct, non-virtual cross-repo edge: consumer -> producer.
                    edge(
                        consumer_file.id,
                        consumer_sym.id,
                        producer_sym.id,
                        EdgeKind::ConsumesApiFrom,
                    ),
                ],
            )
            .expect("fixture insert");

        let participation =
            cross_repo_participation_by_file(&store, "producer").expect("participation");

        let consumers = participation
            .get("src/api.ts")
            .expect("producer file must be reported as participating");
        assert!(
            consumers.contains("consumer"),
            "producer file should map to the foreign consumer repo, got {consumers:?}"
        );
        // The consumer's own file must not be attributed to anything when
        // analysing the producer repo.
        assert!(
            !participation.contains_key("src/uses.ts"),
            "consumer file is not part of the producer repo and must not appear"
        );
    }

    /// Per-file adjacency stage must call `edges_by_owner` exactly once per
    /// File node — never once per symbol/node. The fixture deliberately packs
    /// many symbols into a single file so a per-node walk would inflate the
    /// count well past the file count.
    #[test]
    fn propagation_scans_edges_by_owner_once_per_file_not_per_node() {
        let temp_db = TempDb::new("xrepo-participation-unit", "perf-shape");
        let store = GraphStoreDb::open(temp_db.path()).expect("store should open");

        let f1 = file("repo", "src/a.ts");
        let f2 = file("repo", "src/b.ts");
        // Five symbols in f1 — a per-node walk would scan edges 5x for f1.
        let s1 = symbol("repo", "src/a.ts", "one");
        let s2 = symbol("repo", "src/a.ts", "two");
        let s3 = symbol("repo", "src/a.ts", "three");
        let s4 = symbol("repo", "src/a.ts", "four");
        let s5 = symbol("repo", "src/a.ts", "five");
        let b1 = symbol("repo", "src/b.ts", "bee");

        store
            .bulk_insert(
                &[
                    f1.clone(),
                    f2.clone(),
                    s1.clone(),
                    s2.clone(),
                    s3.clone(),
                    s4.clone(),
                    s5.clone(),
                    b1.clone(),
                ],
                &[
                    defines(f1.id, s1.id),
                    defines(f1.id, s2.id),
                    defines(f1.id, s3.id),
                    defines(f1.id, s4.id),
                    defines(f1.id, s5.id),
                    defines(f2.id, b1.id),
                    edge(f1.id, s1.id, b1.id, EdgeKind::References),
                ],
            )
            .expect("fixture insert");

        let counting = CountingStore::new(&store);
        let _ = cross_repo_participation_by_file(&counting, "repo").expect("participation");

        // Two File nodes -> exactly two `edges_by_owner` scans, independent of
        // the six symbol nodes.
        assert_eq!(
            counting.edges_by_owner_calls.get(),
            2,
            "edges_by_owner must be called once per File node (2), not once per node (8)"
        );
    }
}
