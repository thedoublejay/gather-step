use std::collections::{BTreeMap, BTreeSet, VecDeque};

use gather_step_core::{EdgeKind, NodeId, NodeKind};
use gather_step_parser::ParsedPackageManifest;
use gather_step_storage::GraphStore;
use rustc_hash::FxHashSet;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DeadCodeError {
    #[error(transparent)]
    Store(#[from] gather_step_storage::GraphStoreError),
}

/// Confidence band for a [`DeadCodeFinding`]. Ordered from least to most
/// certain so callers can rank findings via the derived [`Ord`] without a
/// separate stringly-typed lookup. Serialised as the lowercase string form
/// so on-the-wire MCP payloads stay stable across crate refactors.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfidenceBand {
    Low,
    Medium,
    High,
}

impl ConfidenceBand {
    /// Stable lowercase tag used by MCP responses and human-facing reports.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

/// Reason a [`DeadCodeFinding`] was emitted. Each variant corresponds to a
/// distinct detector path; new variants must come with their own coverage
/// policy and confidence cap.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DetectorBasis {
    GraphFileReachability,
    UnusedExportSymbol,
    ZombieDependency,
}

impl DetectorBasis {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GraphFileReachability => "graph_file_reachability",
            Self::UnusedExportSymbol => "unused_export_symbol",
            Self::ZombieDependency => "zombie_dependency",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeadCodeFinding {
    pub repo: String,
    pub file_path: String,
    pub package_name: Option<String>,
    pub symbol_name: Option<String>,
    pub confidence: ConfidenceBand,
    pub detector_basis: DetectorBasis,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeadCodeReport {
    pub findings: Vec<DeadCodeFinding>,
    pub root_files: Vec<String>,
    pub coverage_limits: Vec<String>,
}

pub fn find_dead_code<S: GraphStore>(
    store: &S,
    repo: &str,
) -> Result<DeadCodeReport, DeadCodeError> {
    find_dead_code_with_manifest(store, repo, None)
}

pub fn find_dead_code_with_manifest<S: GraphStore>(
    store: &S,
    repo: &str,
    manifest: Option<&ParsedPackageManifest>,
) -> Result<DeadCodeReport, DeadCodeError> {
    let nodes = store.nodes_by_repo(repo)?;
    let mut file_ids = BTreeMap::<String, NodeId>::new();
    for node in &nodes {
        if node.kind == NodeKind::File {
            file_ids.insert(node.file_path.clone(), node.id);
        }
    }

    let mut root_files = BTreeSet::new();
    for node in &nodes {
        if node.kind == NodeKind::File && is_path_root(&node.file_path) {
            root_files.insert(node.file_path.clone());
        }
        if matches!(
            node.kind,
            NodeKind::Route
                | NodeKind::Service
                | NodeKind::Topic
                | NodeKind::Queue
                | NodeKind::Subject
                | NodeKind::Stream
                | NodeKind::Event
        ) {
            root_files.insert(node.file_path.clone());
        }
    }

    // Build a node-id index from the nodes we already loaded so edge target
    // resolution is O(1) without a second store hit. Without it, edge
    // resolution would round-trip to the store once per outgoing edge: O(E)
    // extra calls layered on top of the per-file edge query.
    let nodes_by_id: rustc_hash::FxHashMap<NodeId, &gather_step_core::NodeData> =
        nodes.iter().map(|node| (node.id, node)).collect();

    // Walk edges per *file* via `edges_by_owner` instead of per *node* via
    // `get_outgoing`. The graph stores file-owned edges in a dedicated
    // multimap, so a repo with thousands of symbols-per-file becomes
    // `file_count` redb scans instead of `total_node_count`.
    //
    // Correctness: we want adjacency of the form
    // `owner_file -> file(target_node)` for cross-file edges. `edges_by_owner`
    // returns every edge whose `owner_file` is the queried file, which is
    // exactly the same set of edges that the previous loop assembled by
    // visiting each defining symbol. The shape of `adjacency` is unchanged.
    let mut adjacency = BTreeMap::<NodeId, BTreeSet<NodeId>>::new();
    for file_id in file_ids.values() {
        for edge in store.edges_by_owner(*file_id)? {
            if let Some(target) = nodes_by_id.get(&edge.target)
                && edge.owner_file != target.id
                && let Some(target_file_id) = file_ids.get(&target.file_path)
            {
                adjacency
                    .entry(edge.owner_file)
                    .or_default()
                    .insert(*target_file_id);
            }
        }
    }

    // Reachability set keyed by `NodeId` directly. The previous version
    // stored `[u8; 16]` byte arrays, which forced extra copies and made
    // the type opaque to readers. `NodeId` already derives `Hash + Eq`.
    let mut reachable: FxHashSet<NodeId> = FxHashSet::default();
    let mut queue = VecDeque::new();
    for root_path in &root_files {
        if let Some(file_id) = file_ids.get(root_path) {
            reachable.insert(*file_id);
            queue.push_back(*file_id);
        }
    }

    while let Some(file_id) = queue.pop_front() {
        if let Some(next_files) = adjacency.get(&file_id) {
            for next in next_files {
                if reachable.insert(*next) {
                    queue.push_back(*next);
                }
            }
        }
    }

    let mut coverage_limits = if root_files.is_empty() {
        vec![
            "No explicit production roots were detected; confidence is capped at medium."
                .to_owned(),
        ]
    } else {
        vec![
            "Runtime modeling is path-and-node heuristic based; dynamic loading is not modeled."
                .to_owned(),
        ]
    };

    let mut findings = nodes
        .iter()
        .filter(|node| node.kind == NodeKind::File)
        .filter(|node| !is_test_file(&node.file_path))
        .filter(|node| !root_files.contains(&node.file_path))
        .filter(|node| !reachable.contains(&node.id))
        .map(|node| DeadCodeFinding {
            repo: node.repo.clone(),
            file_path: node.file_path.clone(),
            package_name: None,
            symbol_name: None,
            confidence: if root_files.is_empty() {
                ConfidenceBand::Medium
            } else {
                ConfidenceBand::High
            },
            detector_basis: DetectorBasis::GraphFileReachability,
            reason: "file is not reachable from detected production roots".to_owned(),
        })
        .collect::<Vec<_>>();

    let UnusedExportsOutcome {
        findings: unused_export_findings,
        skipped_export_count,
    } = find_unused_exports(store, &nodes, &nodes_by_id);
    findings.extend(unused_export_findings);
    if skipped_export_count > 0 {
        // A graph-store error on `get_incoming` means we could not verify
        // whether N export candidates are unused. Surface the count so MCP
        // consumers know the unused-export findings are incomplete rather
        // than discovering it accidentally.
        coverage_limits.push(format!(
            "{skipped_export_count} export candidate(s) could not be analyzed due to graph store \
             read errors; unused-export findings may be incomplete."
        ));
    }
    if let Some(manifest) = manifest {
        findings.extend(find_zombie_dependencies(&nodes, repo, manifest));
    }
    // Highest confidence first; ties broken by stable identifying fields.
    findings.sort_by(|left, right| {
        right
            .confidence
            .cmp(&left.confidence)
            .then_with(|| left.file_path.cmp(&right.file_path))
            .then_with(|| left.package_name.cmp(&right.package_name))
            .then_with(|| left.symbol_name.cmp(&right.symbol_name))
    });

    Ok(DeadCodeReport {
        findings,
        root_files: root_files.into_iter().collect(),
        coverage_limits,
    })
}

/// Outcome of [`find_unused_exports`]. Tracks the count of export candidates
/// that could not be analyzed because of a graph-store error so callers can
/// surface partial-coverage warnings instead of silently dropping findings.
struct UnusedExportsOutcome {
    findings: Vec<DeadCodeFinding>,
    skipped_export_count: usize,
}

fn find_unused_exports<S: GraphStore>(
    store: &S,
    nodes: &[gather_step_core::NodeData],
    nodes_by_id: &rustc_hash::FxHashMap<NodeId, &gather_step_core::NodeData>,
) -> UnusedExportsOutcome {
    let mut findings = Vec::new();
    let mut skipped_export_count: usize = 0;
    for node in nodes.iter().filter(|node| is_export_candidate(node.kind)) {
        if is_test_file(&node.file_path) {
            continue;
        }

        // Surface failures in coverage_limits via the count; the alternative —
        // returning Err — would mask all other findings for one bad node.
        // Counting at the boundary keeps the report partial-but-honest.
        let Ok(incoming) = store.get_incoming(node.id) else {
            skipped_export_count = skipped_export_count.saturating_add(1);
            continue;
        };
        let is_exported = incoming.iter().any(|edge| {
            edge.kind == EdgeKind::Exports
                && edge.owner_file != node.id
                && nodes_by_id.get(&edge.source).is_some_and(|source| {
                    source.repo == node.repo
                        && source.file_path == node.file_path
                        && matches!(source.kind, NodeKind::File | NodeKind::Module)
                })
        });
        if !is_exported {
            continue;
        }

        let has_usage = incoming.iter().any(|edge| {
            matches!(
                edge.kind,
                EdgeKind::Calls
                    | EdgeKind::References
                    | EdgeKind::Implements
                    | EdgeKind::ImplementsContractFrom
                    | EdgeKind::Extends
                    | EdgeKind::UsesShared
                    | EdgeKind::UsesTypeFrom
                    | EdgeKind::UsesEventFrom
                    | EdgeKind::UsesGuardFrom
                    | EdgeKind::ConsumesApiFrom
                    | EdgeKind::ProducesEventFor
                    | EdgeKind::ContractOn
            ) && edge.owner_file != node.id
        });
        if has_usage {
            continue;
        }

        findings.push(DeadCodeFinding {
            repo: node.repo.clone(),
            file_path: node.file_path.clone(),
            package_name: None,
            symbol_name: Some(node.name.clone()),
            confidence: ConfidenceBand::Medium,
            detector_basis: DetectorBasis::UnusedExportSymbol,
            reason: format!(
                "exported symbol `{}` has no detected downstream usage",
                node.name
            ),
        });
    }

    UnusedExportsOutcome {
        findings,
        skipped_export_count,
    }
}

fn find_zombie_dependencies(
    nodes: &[gather_step_core::NodeData],
    repo: &str,
    manifest: &ParsedPackageManifest,
) -> Vec<DeadCodeFinding> {
    let used_packages = nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Module && node.is_virtual)
        .filter_map(|node| {
            node.external_id
                .as_deref()
                .and_then(|external_id| external_id.strip_prefix("module-import::"))
                .and_then(imported_package_root)
        })
        .collect::<FxHashSet<_>>();

    let mut findings = manifest
        .dependencies
        .iter()
        .filter(|dependency| !used_packages.contains(dependency.package.as_str()))
        .map(|dependency| DeadCodeFinding {
            repo: repo.to_owned(),
            file_path: "package.json".to_owned(),
            package_name: Some(dependency.package.clone()),
            symbol_name: None,
            confidence: ConfidenceBand::Medium,
            detector_basis: DetectorBasis::ZombieDependency,
            reason: format!(
                "dependency `{}` is declared in package.json but has no detected import usage",
                dependency.package
            ),
        })
        .collect::<Vec<_>>();
    findings.sort_by(|left, right| left.package_name.cmp(&right.package_name));
    findings
}

fn imported_package_root(source: &str) -> Option<&str> {
    if source.is_empty()
        || source.starts_with('.')
        || source.starts_with('/')
        || source.starts_with('#')
    {
        return None;
    }

    if let Some(stripped) = source.strip_prefix('@') {
        let mut segments = stripped.split('/');
        let scope = segments.next()?;
        let package = segments.next()?;
        let scope_len = scope.len() + 1;
        let package_len = package.len();
        return source.get(..scope_len + 1 + package_len);
    }

    source.split('/').next()
}

fn is_export_candidate(kind: NodeKind) -> bool {
    matches!(
        kind,
        NodeKind::Function
            | NodeKind::Class
            | NodeKind::Type
            | NodeKind::Entity
            | NodeKind::Service
    )
}

pub(crate) fn is_path_root(file_path: &str) -> bool {
    file_path.contains("/bin/")
        || file_path.ends_with("/main.rs")
        || file_path.ends_with("/main.ts")
        || file_path.ends_with("/app.ts")
        || file_path.ends_with("/app.js")
        || file_path.ends_with("/server.ts")
        || file_path.ends_with("/server.js")
        || file_path.ends_with("/index.ts")
        || file_path.ends_with("/index.js")
}

fn is_test_file(file_path: &str) -> bool {
    file_path.contains("/test/")
        || file_path.contains("/tests/")
        || file_path.ends_with(".spec.ts")
        || file_path.ends_with(".spec.js")
        || file_path.ends_with(".test.ts")
        || file_path.ends_with(".test.js")
        || file_path.ends_with("_test.rs")
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::{EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, node_id};
    use gather_step_parser::ParsedPackageManifest;
    use gather_step_storage::{GraphStore, GraphStoreDb};
    use pretty_assertions::assert_eq;

    use super::{ConfidenceBand, DetectorBasis, find_dead_code};

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new(name: &str) -> Self {
            let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-dead-code-{name}-{}-{id}.redb",
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
    fn finds_unreachable_file_from_route_root() {
        let temp_db = TempDb::new("dead-code");
        let store = GraphStoreDb::open(temp_db.path()).expect("open graph");
        let route_file = file_node("service-a", "src/routes/items.ts");
        let route = symbol_node(
            "service-a",
            "src/routes/items.ts",
            NodeKind::Route,
            "GET /items",
        );
        let live_file = file_node("service-a", "src/services/items.ts");
        let live_symbol = symbol_node(
            "service-a",
            "src/services/items.ts",
            NodeKind::Function,
            "load_items",
        );
        let dead_file = file_node("service-a", "src/unused.ts");
        let dead_symbol = symbol_node("service-a", "src/unused.ts", NodeKind::Function, "unused");

        store
            .bulk_insert(
                &[
                    route_file.clone(),
                    route,
                    live_file.clone(),
                    live_symbol.clone(),
                    dead_file.clone(),
                    dead_symbol,
                ],
                &[EdgeData {
                    source: route_file.id,
                    target: live_symbol.id,
                    kind: EdgeKind::Calls,
                    metadata: EdgeMetadata::default(),
                    owner_file: route_file.id,
                    is_cross_file: true,
                }],
            )
            .expect("graph write");

        let report = find_dead_code(&store, "service-a").expect("report");
        assert_eq!(report.findings.len(), 1);
        assert_eq!(report.findings[0].file_path, "src/unused.ts");
        assert_eq!(report.findings[0].package_name, None);
        assert_eq!(report.findings[0].symbol_name, None);
        assert_eq!(report.findings[0].confidence, ConfidenceBand::High);
    }

    #[test]
    fn finds_unused_exported_symbol_without_downstream_usage() {
        let temp_db = TempDb::new("unused-export");
        let store = GraphStoreDb::open(temp_db.path()).expect("open graph");
        let file = file_node("service-a", "src/lib.ts");
        let module = module_node("service-a", "src/lib.ts");
        let exported = symbol_node("service-a", "src/lib.ts", NodeKind::Function, "exported_fn");
        let used = symbol_node("service-a", "src/lib.ts", NodeKind::Function, "used_fn");
        let caller_file = file_node("service-a", "src/caller.ts");
        let caller = symbol_node("service-a", "src/caller.ts", NodeKind::Function, "caller");

        store
            .bulk_insert(
                &[
                    file.clone(),
                    module.clone(),
                    exported.clone(),
                    used.clone(),
                    caller_file.clone(),
                    caller.clone(),
                ],
                &[
                    EdgeData {
                        source: file.id,
                        target: module.id,
                        kind: EdgeKind::Defines,
                        metadata: EdgeMetadata::default(),
                        owner_file: file.id,
                        is_cross_file: false,
                    },
                    EdgeData {
                        source: file.id,
                        target: exported.id,
                        kind: EdgeKind::Defines,
                        metadata: EdgeMetadata::default(),
                        owner_file: file.id,
                        is_cross_file: false,
                    },
                    EdgeData {
                        source: file.id,
                        target: used.id,
                        kind: EdgeKind::Defines,
                        metadata: EdgeMetadata::default(),
                        owner_file: file.id,
                        is_cross_file: false,
                    },
                    EdgeData {
                        source: module.id,
                        target: exported.id,
                        kind: EdgeKind::Exports,
                        metadata: EdgeMetadata::default(),
                        owner_file: file.id,
                        is_cross_file: false,
                    },
                    EdgeData {
                        source: module.id,
                        target: used.id,
                        kind: EdgeKind::Exports,
                        metadata: EdgeMetadata::default(),
                        owner_file: file.id,
                        is_cross_file: false,
                    },
                    EdgeData {
                        source: caller.id,
                        target: used.id,
                        kind: EdgeKind::Calls,
                        metadata: EdgeMetadata::default(),
                        owner_file: caller_file.id,
                        is_cross_file: true,
                    },
                ],
            )
            .expect("graph write");

        let report = find_dead_code(&store, "service-a").expect("report");
        assert!(report.findings.iter().any(|finding| {
            finding.file_path == "src/lib.ts"
                && finding.symbol_name.as_deref() == Some("exported_fn")
                && finding.detector_basis == DetectorBasis::UnusedExportSymbol
        }));
        assert!(!report.findings.iter().any(|finding| {
            finding.file_path == "src/lib.ts" && finding.symbol_name.as_deref() == Some("used_fn")
        }));
    }

    #[test]
    fn finds_zombie_dependencies_from_manifest_when_package_is_never_imported() {
        let temp_db = TempDb::new("zombie-deps");
        let store = GraphStoreDb::open(temp_db.path()).expect("open graph");
        let file = file_node("service-a", "src/app.ts");
        let used_module = module_import_node("service-a", "src/app.ts", "@nestjs/common");

        store
            .bulk_insert(
                &[file.clone(), used_module.clone()],
                &[EdgeData {
                    source: file.id,
                    target: used_module.id,
                    kind: EdgeKind::Imports,
                    metadata: EdgeMetadata::default(),
                    owner_file: file.id,
                    is_cross_file: true,
                }],
            )
            .expect("graph write");

        let manifest = ParsedPackageManifest {
            package_name: Some("service-a".to_owned()),
            dependencies: vec![
                gather_step_parser::ManifestDependency {
                    package: "@nestjs/common".to_owned(),
                    version: "^11.0.0".to_owned(),
                },
                gather_step_parser::ManifestDependency {
                    package: "lodash".to_owned(),
                    version: "^4.17.21".to_owned(),
                },
            ],
        };

        let report = super::find_dead_code_with_manifest(&store, "service-a", Some(&manifest))
            .expect("report");
        assert!(report.findings.iter().any(|finding| {
            finding.detector_basis == DetectorBasis::ZombieDependency
                && finding.package_name.as_deref() == Some("lodash")
                && finding.file_path == "package.json"
        }));
        assert!(!report.findings.iter().any(|finding| {
            finding.detector_basis == DetectorBasis::ZombieDependency
                && finding.package_name.as_deref() == Some("@nestjs/common")
        }));
    }

    fn file_node(repo: &str, file_path: &str) -> NodeData {
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
        }
    }

    fn symbol_node(repo: &str, file_path: &str, kind: NodeKind, name: &str) -> NodeData {
        NodeData {
            id: node_id(repo, file_path, kind, name),
            kind,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: name.to_owned(),
            qualified_name: None,
            external_id: None,
            signature: None,
            visibility: None,
            span: None,
            is_virtual: false,
        }
    }

    fn module_node(repo: &str, file_path: &str) -> NodeData {
        NodeData {
            id: node_id(repo, file_path, NodeKind::Module, file_path),
            kind: NodeKind::Module,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: file_path.to_owned(),
            qualified_name: None,
            external_id: None,
            signature: None,
            visibility: None,
            span: None,
            is_virtual: false,
        }
    }

    fn module_import_node(repo: &str, file_path: &str, source: &str) -> NodeData {
        NodeData {
            id: node_id(repo, file_path, NodeKind::Module, source),
            kind: NodeKind::Module,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: source.to_owned(),
            qualified_name: Some(format!("module-import::{source}")),
            external_id: Some(format!("module-import::{source}")),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        }
    }
}
