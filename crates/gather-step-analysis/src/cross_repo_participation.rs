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

use gather_step_core::{EdgeKind, NodeId, NodeKind, VIRTUAL_NODE_REPO};
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
    let session = store.read_session()?;
    let nodes = session.nodes_by_repo(repo)?;

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
        for edge in session.outgoing(node.id)? {
            if !is_producer_edge(edge.kind) {
                continue;
            }
            let Some(target) = session.node(edge.target)? else {
                continue;
            };
            if !target.is_virtual || is_provenance_virtual(target.kind) {
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
        for related in session.incoming(*virtual_id)? {
            // Virtual identity is workspace-wide, so a foreign repo serving
            // or publishing to an identically-named surface shares this node
            // as a CO-PRODUCER (every service's `GET /healthcheck` collapses
            // onto one route node). Only consumption-direction edges make it
            // a consumer.
            if is_producer_edge(related.kind) {
                continue;
            }
            if let Some(source) = session.node(related.source)?
                && is_foreign_repo(&source.repo, repo)
            {
                foreign_consumer_repos.insert(source.repo);
            }
        }
        for related in session.outgoing(*virtual_id)? {
            if let Some(target) = session.node(related.target)?
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
        // A shared metadata stub (e.g. a `module-import::` node pinned to the
        // first indexing repo) is not a producer surface of this repo.
        if node.is_virtual && is_provenance_virtual(node.kind) {
            continue;
        }
        let Some(producer_file) = owning_file.get(&node.id) else {
            continue;
        };
        for edge in session.incoming(node.id)? {
            let Some(source) = session.node(edge.source)? else {
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

/// Return foreign repositories with a dependency path to the exact symbol.
///
/// Unlike [`cross_repo_participation_by_file`], this traversal never widens a
/// symbol to every other declaration in its file. It walks same-repo reverse
/// dependency edges from the target symbol to its producers, then crosses only
/// explicit transport producer/consumer boundaries or direct foreign usage
/// edges. This keeps route-mediated config consumers while preventing an
/// unrelated co-located symbol from inheriting the file's consumers.
pub fn cross_repo_consumers_for_symbol<S: GraphStore>(
    store: &S,
    symbol_id: NodeId,
) -> Result<Vec<String>, GraphStoreError> {
    let session = store.read_session()?;
    let Some(target) = session.node(symbol_id)? else {
        return Ok(Vec::new());
    };
    if target.is_virtual {
        if is_provenance_virtual(target.kind) {
            return Ok(Vec::new());
        }
        let mut consumers = BTreeSet::new();
        for edge in session.incoming(target.id)? {
            if !is_direct_consumer_edge(edge.kind) {
                continue;
            }
            if let Some(source) = session.node(edge.source)?
                && !source.is_virtual
                && source.repo != VIRTUAL_NODE_REPO
            {
                consumers.insert(source.repo);
            }
        }
        return Ok(consumers.into_iter().collect());
    }
    if matches!(target.kind, NodeKind::File | NodeKind::Module) {
        return Ok(Vec::new());
    }

    let producer_repo = target.repo;
    let mut consumers = BTreeSet::<String>::new();
    let mut queue = VecDeque::from([symbol_id]);
    let mut visited = BTreeSet::from([symbol_id]);

    while let Some(current_id) = queue.pop_front() {
        for edge in session.outgoing(current_id)? {
            if !is_producer_edge(edge.kind) {
                continue;
            }
            let Some(surface) = session.node(edge.target)? else {
                continue;
            };
            if !surface.is_virtual || is_provenance_virtual(surface.kind) {
                continue;
            }
            for related in session.incoming(surface.id)? {
                if !is_virtual_consumer_edge(related.kind) {
                    continue;
                }
                if let Some(source) = session.node(related.source)?
                    && !source.is_virtual
                    && is_foreign_repo(&source.repo, &producer_repo)
                {
                    consumers.insert(source.repo);
                }
            }
        }

        for edge in session.incoming(current_id)? {
            let Some(source) = session.node(edge.source)? else {
                continue;
            };
            if source.is_virtual {
                continue;
            }
            if is_foreign_repo(&source.repo, &producer_repo) {
                if is_direct_consumer_edge(edge.kind) {
                    consumers.insert(source.repo);
                }
                continue;
            }
            if source.repo == producer_repo
                && is_reverse_dependency_edge(edge.kind)
                && visited.insert(source.id)
            {
                queue.push_back(source.id);
            }
        }
    }

    Ok(consumers.into_iter().collect())
}

/// A repo is a foreign consumer when it is neither the analysed `repo` nor the
/// synthetic [`VIRTUAL_NODE_REPO`] (virtual transport stubs are never a real
/// consuming repo, and same-repo consumers are excluded by design).
fn is_foreign_repo(candidate: &str, repo: &str) -> bool {
    candidate != repo && candidate != VIRTUAL_NODE_REPO
}

/// Edge kinds whose source PRODUCES into a virtual surface (serves the route,
/// publishes the topic/event). A foreign co-producer of an identically-named
/// surface is not a consumer of what this repo's files feed it.
const fn is_producer_edge(kind: EdgeKind) -> bool {
    matches!(
        kind,
        EdgeKind::Serves | EdgeKind::Publishes | EdgeKind::ProducesEventFor
    )
}

const fn is_virtual_consumer_edge(kind: EdgeKind) -> bool {
    matches!(
        kind,
        EdgeKind::Consumes | EdgeKind::ConsumesApiFrom | EdgeKind::UsesEventFrom
    )
}

const fn is_direct_consumer_edge(kind: EdgeKind) -> bool {
    matches!(
        kind,
        EdgeKind::Calls
            | EdgeKind::Extends
            | EdgeKind::Implements
            | EdgeKind::References
            | EdgeKind::DependsOn
            | EdgeKind::UsesDecorator
            | EdgeKind::Consumes
            | EdgeKind::Triggers
            | EdgeKind::UsesShared
            | EdgeKind::UsesTypeFrom
            | EdgeKind::UsesEventFrom
            | EdgeKind::UsesGuardFrom
            | EdgeKind::ConsumesApiFrom
            | EdgeKind::ImplementsContractFrom
            | EdgeKind::ConsumesHookFrom
            | EdgeKind::ContractOn
            | EdgeKind::FetchesPromptFrom
            | EdgeKind::Embeds
            | EdgeKind::CallsMcpTool
            | EdgeKind::RetrievesFrom
    )
}

const fn is_reverse_dependency_edge(kind: EdgeKind) -> bool {
    matches!(
        kind,
        EdgeKind::Calls
            | EdgeKind::Extends
            | EdgeKind::Implements
            | EdgeKind::References
            | EdgeKind::DependsOn
            | EdgeKind::UsesDecorator
            | EdgeKind::Triggers
            | EdgeKind::UsesShared
            | EdgeKind::UsesTypeFrom
            | EdgeKind::UsesEventFrom
            | EdgeKind::UsesGuardFrom
            | EdgeKind::ImplementsContractFrom
            | EdgeKind::ConsumesHookFrom
            | EdgeKind::ContractOn
            | EdgeKind::DefinesAgentNode
            | EdgeKind::GraphTransitionsTo
            | EdgeKind::ComposesAgent
            | EdgeKind::SpawnsSubagent
            | EdgeKind::BindsTool
            | EdgeKind::InvokesLlm
            | EdgeKind::ProducesAiContract
            | EdgeKind::UsesPrompt
            | EdgeKind::FetchesPromptFrom
            | EdgeKind::RetrievesFrom
            | EdgeKind::Embeds
            | EdgeKind::CallsMcpTool
            | EdgeKind::ExposesMcpTool
    )
}

/// Virtual kinds carrying provenance or import metadata rather than a
/// consumable transport/contract surface. Their node identity hashes only the
/// qualified name, so one author (or one `module-import::typing` stub) is
/// shared by every repo that touches it — a hub that links repos without
/// either consuming what the other produces. Participation never seeds
/// through them (the "one file -> every repo in the workspace" over-match).
const fn is_provenance_virtual(kind: NodeKind) -> bool {
    matches!(
        kind,
        NodeKind::Author
            | NodeKind::Commit
            | NodeKind::PR
            | NodeKind::Review
            | NodeKind::Comment
            | NodeKind::Ticket
            | NodeKind::Module
    )
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
        EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeId, NodeKind, VIRTUAL_NODE_REPO,
        Visibility, node_id, ref_node_id,
    };
    use gather_step_storage::{GraphStore, GraphStoreDb, GraphStoreError};

    use super::{cross_repo_consumers_for_symbol, cross_repo_participation_by_file};
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

    /// Route identity is workspace-wide (`__route__<METHOD>__<path>`), so
    /// every service serving `GET /healthcheck` collapses onto one virtual
    /// node. A foreign repo that merely SERVES the same path is a
    /// co-producer, not a consumer, and must not seed participation.
    #[test]
    fn foreign_co_producer_of_shared_route_is_not_a_consumer() {
        let temp_db = TempDb::new("xrepo-participation-unit", "route-coproducer");
        let store = GraphStoreDb::open(temp_db.path()).expect("store should open");

        let producer_file = file("producer", "src/health.py");
        let producer_sym = symbol("producer", "src/health.py", "healthcheck");
        let foreign_file = file("consumer", "src/health.py");
        let foreign_sym = symbol("consumer", "src/health.py", "healthcheck");
        let route = gather_step_core::virtual_node(
            NodeKind::Route,
            VIRTUAL_NODE_REPO,
            "__route__GET__/healthcheck",
            "GET /healthcheck",
            "__route__GET__/healthcheck",
        );

        store
            .bulk_insert(
                &[
                    producer_file.clone(),
                    producer_sym.clone(),
                    foreign_file.clone(),
                    foreign_sym.clone(),
                    route.clone(),
                ],
                &[
                    defines(producer_file.id, producer_sym.id),
                    defines(foreign_file.id, foreign_sym.id),
                    edge(
                        producer_file.id,
                        producer_sym.id,
                        route.id,
                        EdgeKind::Serves,
                    ),
                    edge(foreign_file.id, foreign_sym.id, route.id, EdgeKind::Serves),
                ],
            )
            .expect("fixture insert");

        let participation =
            cross_repo_participation_by_file(&store, "producer").expect("participation");
        assert!(
            !participation.contains_key("src/health.py"),
            "a foreign co-producer serving the same route must not read as a consumer, got {participation:?}"
        );
    }

    /// A virtual Author node is provenance, not a transport boundary. Its id
    /// hashes only the redacted email, so an author who commits to two repos
    /// is one shared node — that must not make either repo a "consumer" of
    /// every file the author touched in the other.
    #[test]
    fn shared_author_node_does_not_seed_consumers() {
        let temp_db = TempDb::new("xrepo-participation-unit", "author-hub");
        let store = GraphStoreDb::open(temp_db.path()).expect("store should open");

        let producer_file = file("producer", "src/api.py");
        let foreign_file = file("consumer", "src/other.py");
        let author = NodeData {
            id: ref_node_id(NodeKind::Author, "dev@redacted"),
            kind: NodeKind::Author,
            repo: VIRTUAL_NODE_REPO.to_owned(),
            file_path: "__authors__/dev@redacted".to_owned(),
            name: "dev@redacted".to_owned(),
            qualified_name: Some("dev@redacted".to_owned()),
            external_id: Some("dev@redacted".to_owned()),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
            ai_role: None,
        };

        store
            .bulk_insert(
                &[producer_file.clone(), foreign_file.clone(), author.clone()],
                &[
                    edge(
                        producer_file.id,
                        producer_file.id,
                        author.id,
                        EdgeKind::OwnedBy,
                    ),
                    edge(
                        foreign_file.id,
                        foreign_file.id,
                        author.id,
                        EdgeKind::OwnedBy,
                    ),
                ],
            )
            .expect("fixture insert");

        let participation =
            cross_repo_participation_by_file(&store, "producer").expect("participation");
        assert!(
            !participation.contains_key("src/api.py"),
            "a shared author node must not mark the file as cross-repo consumed, got {participation:?}"
        );
    }

    /// A `module-import::<path>` stub is keyed only by module path, so every
    /// repo importing `typing` collapses onto one virtual node (which also
    /// carries the first indexer's repo name, exposing it to the direct-seed
    /// stage). Co-importing a module must not read as consumption.
    #[test]
    fn shared_module_import_stub_does_not_seed_consumers() {
        let temp_db = TempDb::new("xrepo-participation-unit", "module-stub");
        let store = GraphStoreDb::open(temp_db.path()).expect("store should open");

        let producer_file = file("producer", "src/uses_typing.py");
        let foreign_file = file("consumer", "src/also_typing.py");
        // Mirror production: shared id, `repo` pinned to the first indexing
        // repo, virtual.
        let module_stub = NodeData {
            id: ref_node_id(NodeKind::Module, "module-import::typing"),
            kind: NodeKind::Module,
            repo: "producer".to_owned(),
            file_path: "src/uses_typing.py".to_owned(),
            name: "typing".to_owned(),
            qualified_name: Some("module-import::typing".to_owned()),
            external_id: Some("module-import::typing".to_owned()),
            signature: None,
            visibility: Some(Visibility::Public),
            span: None,
            is_virtual: true,
            ai_role: None,
        };

        store
            .bulk_insert(
                &[
                    producer_file.clone(),
                    foreign_file.clone(),
                    module_stub.clone(),
                ],
                &[
                    edge(
                        producer_file.id,
                        producer_file.id,
                        module_stub.id,
                        EdgeKind::Imports,
                    ),
                    edge(
                        foreign_file.id,
                        foreign_file.id,
                        module_stub.id,
                        EdgeKind::Imports,
                    ),
                ],
            )
            .expect("fixture insert");

        let participation =
            cross_repo_participation_by_file(&store, "producer").expect("participation");
        assert!(
            !participation.contains_key("src/uses_typing.py"),
            "co-importing a shared module stub must not mark the file as consumed, got {participation:?}"
        );
    }

    /// Two repos can reference the same virtual shared-symbol stub without
    /// either producing it for the other. Treating every outgoing edge to a
    /// virtual node as production turns that co-usage into a false consumer
    /// relationship.
    #[test]
    fn shared_virtual_co_usage_does_not_seed_consumers() {
        let temp_db = TempDb::new("xrepo-participation-unit", "shared-symbol-co-usage");
        let store = GraphStoreDb::open(temp_db.path()).expect("store should open");

        let producer_file = file("producer", "app/api/v1/chat/service.py");
        let producer_sym = symbol("producer", "app/api/v1/chat/service.py", "ChatService");
        let foreign_file = file("consumer", "app/api/v1/chat/service.py");
        let foreign_sym = symbol("consumer", "app/api/v1/chat/service.py", "ChatService");
        let shared = NodeData {
            id: ref_node_id(NodeKind::SharedSymbol, "ChatService"),
            kind: NodeKind::SharedSymbol,
            repo: VIRTUAL_NODE_REPO.to_owned(),
            file_path: "__shared__/ChatService".to_owned(),
            name: "ChatService".to_owned(),
            qualified_name: Some("ChatService".to_owned()),
            external_id: Some("ChatService".to_owned()),
            signature: None,
            visibility: Some(Visibility::Public),
            span: None,
            is_virtual: true,
            ai_role: None,
        };

        store
            .bulk_insert(
                &[
                    producer_file.clone(),
                    producer_sym.clone(),
                    foreign_file.clone(),
                    foreign_sym.clone(),
                    shared.clone(),
                ],
                &[
                    defines(producer_file.id, producer_sym.id),
                    defines(foreign_file.id, foreign_sym.id),
                    edge(
                        producer_file.id,
                        producer_sym.id,
                        shared.id,
                        EdgeKind::UsesShared,
                    ),
                    edge(
                        foreign_file.id,
                        foreign_sym.id,
                        shared.id,
                        EdgeKind::UsesShared,
                    ),
                ],
            )
            .expect("fixture insert");

        let participation =
            cross_repo_participation_by_file(&store, "producer").expect("participation");
        assert!(
            !participation.contains_key("app/api/v1/chat/service.py"),
            "co-using a shared virtual symbol must not create a consumer edge: {participation:?}"
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

    /// A foreign repo with a direct cross-repo `...From` edge into the exact
    /// symbol is reported as a consumer of that symbol.
    #[test]
    fn consumers_for_symbol_reports_direct_foreign_usage() {
        let temp_db = TempDb::new("xrepo-consumers-unit", "direct-foreign");
        let store = GraphStoreDb::open(temp_db.path()).expect("store should open");

        let producer_file = file("producer", "src/api.ts");
        let producer_sym = symbol("producer", "src/api.ts", "exportedThing");
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
                    edge(
                        consumer_file.id,
                        consumer_sym.id,
                        producer_sym.id,
                        EdgeKind::ConsumesApiFrom,
                    ),
                ],
            )
            .expect("fixture insert");

        let consumers =
            cross_repo_consumers_for_symbol(&store, producer_sym.id).expect("consumers");
        assert_eq!(consumers, vec!["consumer".to_owned()]);
    }

    /// The core symbol-accuracy guarantee: a co-located, unrelated declaration
    /// in the same file must NOT inherit the consumers of a sibling symbol.
    #[test]
    fn consumers_for_symbol_ignores_co_located_unrelated_symbol() {
        let temp_db = TempDb::new("xrepo-consumers-unit", "co-located");
        let store = GraphStoreDb::open(temp_db.path()).expect("store should open");

        let producer_file = file("producer", "src/config.ts");
        let config_sym = symbol("producer", "src/config.ts", "SHARED_CONFIG");
        let unrelated_sym = symbol("producer", "src/config.ts", "PRIVATE_HELPER");
        let importer_file = file("consumer", "src/uses.ts");
        let importer_sym = symbol("consumer", "src/uses.ts", "useConfig");

        store
            .bulk_insert(
                &[
                    producer_file.clone(),
                    config_sym.clone(),
                    unrelated_sym.clone(),
                    importer_file.clone(),
                    importer_sym.clone(),
                ],
                &[
                    defines(producer_file.id, config_sym.id),
                    defines(producer_file.id, unrelated_sym.id),
                    defines(importer_file.id, importer_sym.id),
                    edge(
                        importer_file.id,
                        importer_sym.id,
                        config_sym.id,
                        EdgeKind::UsesTypeFrom,
                    ),
                ],
            )
            .expect("fixture insert");

        let config_consumers =
            cross_repo_consumers_for_symbol(&store, config_sym.id).expect("config lookup");
        assert_eq!(config_consumers, vec!["consumer".to_owned()]);

        let unrelated =
            cross_repo_consumers_for_symbol(&store, unrelated_sym.id).expect("unrelated lookup");
        assert!(
            unrelated.is_empty(),
            "an unrelated co-located symbol must not inherit its sibling's consumers: {unrelated:?}"
        );
    }

    /// A same-repo reverse-dependency chain to a producer surface resolves the
    /// transport consumer: a config symbol referenced by a route handler whose
    /// route a foreign repo consumes reports that foreign repo.
    #[test]
    fn consumers_for_symbol_follows_reverse_dependency_through_route() {
        let temp_db = TempDb::new("xrepo-consumers-unit", "route-mediated");
        let store = GraphStoreDb::open(temp_db.path()).expect("store should open");

        let config_file = file("service-api", "src/config.ts");
        let config_sym = symbol("service-api", "src/config.ts", "CREDIT_CONFIG");
        let handler_file = file("service-api", "src/handler.ts");
        let handler_sym = symbol("service-api", "src/handler.ts", "getCredits");
        let caller_file = file("service-ui", "src/caller.ts");
        let caller_sym = symbol("service-ui", "src/caller.ts", "callCredits");
        let route = gather_step_core::virtual_node(
            NodeKind::Route,
            VIRTUAL_NODE_REPO,
            "__route__GET__/credits",
            "GET /credits",
            "__route__GET__/credits",
        );

        store
            .bulk_insert(
                &[
                    config_file.clone(),
                    config_sym.clone(),
                    handler_file.clone(),
                    handler_sym.clone(),
                    caller_file.clone(),
                    caller_sym.clone(),
                    route.clone(),
                ],
                &[
                    defines(config_file.id, config_sym.id),
                    defines(handler_file.id, handler_sym.id),
                    defines(caller_file.id, caller_sym.id),
                    // Handler references the config symbol (reverse dependency).
                    edge(
                        handler_file.id,
                        handler_sym.id,
                        config_sym.id,
                        EdgeKind::References,
                    ),
                    // Handler serves the route (producer edge into the surface).
                    edge(handler_file.id, handler_sym.id, route.id, EdgeKind::Serves),
                    // Foreign repo consumes the route.
                    edge(
                        caller_file.id,
                        caller_sym.id,
                        route.id,
                        EdgeKind::ConsumesApiFrom,
                    ),
                ],
            )
            .expect("fixture insert");

        let consumers = cross_repo_consumers_for_symbol(&store, config_sym.id).expect("consumers");
        assert_eq!(consumers, vec!["service-ui".to_owned()]);
    }

    /// A File target has no single declaration to resolve, so the exact-symbol
    /// traversal reports no consumers (file-level participation is a separate
    /// projection).
    #[test]
    fn consumers_for_symbol_is_empty_for_file_target() {
        let temp_db = TempDb::new("xrepo-consumers-unit", "file-target");
        let store = GraphStoreDb::open(temp_db.path()).expect("store should open");

        let f = file("repo", "src/a.ts");
        store
            .bulk_insert(std::slice::from_ref(&f), &[])
            .expect("fixture insert");

        let consumers = cross_repo_consumers_for_symbol(&store, f.id).expect("consumers");
        assert!(
            consumers.is_empty(),
            "a File target yields no consumers: {consumers:?}"
        );
    }

    /// A provenance virtual node (Author) is a shared hub, not a transport
    /// surface, and must never resolve consumers.
    #[test]
    fn consumers_for_symbol_is_empty_for_provenance_virtual() {
        let temp_db = TempDb::new("xrepo-consumers-unit", "provenance-virtual");
        let store = GraphStoreDb::open(temp_db.path()).expect("store should open");

        let author = NodeData {
            id: ref_node_id(NodeKind::Author, "dev@redacted"),
            kind: NodeKind::Author,
            repo: VIRTUAL_NODE_REPO.to_owned(),
            file_path: "__authors__/dev@redacted".to_owned(),
            name: "dev@redacted".to_owned(),
            qualified_name: Some("dev@redacted".to_owned()),
            external_id: Some("dev@redacted".to_owned()),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
            ai_role: None,
        };
        store
            .bulk_insert(std::slice::from_ref(&author), &[])
            .expect("fixture insert");

        let consumers = cross_repo_consumers_for_symbol(&store, author.id).expect("consumers");
        assert!(
            consumers.is_empty(),
            "a provenance virtual node yields no consumers: {consumers:?}"
        );
    }

    /// Querying a non-provenance virtual surface (a shared symbol) directly
    /// reports the real repos that consume it.
    #[test]
    fn consumers_for_symbol_reports_direct_virtual_consumer() {
        let temp_db = TempDb::new("xrepo-consumers-unit", "virtual-target");
        let store = GraphStoreDb::open(temp_db.path()).expect("store should open");

        let consumer_file = file("consumer", "src/uses.ts");
        let consumer_sym = symbol("consumer", "src/uses.ts", "usesShared");
        let shared = NodeData {
            id: ref_node_id(NodeKind::SharedSymbol, "ChatService"),
            kind: NodeKind::SharedSymbol,
            repo: VIRTUAL_NODE_REPO.to_owned(),
            file_path: "__shared__/ChatService".to_owned(),
            name: "ChatService".to_owned(),
            qualified_name: Some("ChatService".to_owned()),
            external_id: Some("ChatService".to_owned()),
            signature: None,
            visibility: Some(Visibility::Public),
            span: None,
            is_virtual: true,
            ai_role: None,
        };

        store
            .bulk_insert(
                &[consumer_file.clone(), consumer_sym.clone(), shared.clone()],
                &[
                    defines(consumer_file.id, consumer_sym.id),
                    edge(
                        consumer_file.id,
                        consumer_sym.id,
                        shared.id,
                        EdgeKind::UsesShared,
                    ),
                ],
            )
            .expect("fixture insert");

        let consumers = cross_repo_consumers_for_symbol(&store, shared.id).expect("consumers");
        assert_eq!(consumers, vec!["consumer".to_owned()]);
    }
}
