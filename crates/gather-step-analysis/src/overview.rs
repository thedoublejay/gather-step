use std::collections::BTreeMap;

use gather_step_core::NodeKind;
use gather_step_storage::{GraphStore, MetadataStore};
use thiserror::Error;

use crate::dead_code::{DeadCodeError, find_dead_code};

fn node_kind_label(kind: NodeKind) -> &'static str {
    match kind {
        NodeKind::File => "file",
        NodeKind::Function => "function",
        NodeKind::Class => "class",
        NodeKind::Type => "type",
        NodeKind::Module => "module",
        NodeKind::Import => "import",
        NodeKind::Decorator => "decorator",
        NodeKind::Entity => "entity",
        NodeKind::Route => "route",
        NodeKind::Topic => "topic",
        NodeKind::Queue => "queue",
        NodeKind::Subject => "subject",
        NodeKind::Stream => "stream",
        NodeKind::Event => "event",
        NodeKind::SharedSymbol => "shared_symbol",
        NodeKind::PayloadContract => "payload_contract",
        NodeKind::Repo => "repo",
        NodeKind::Convention => "convention",
        NodeKind::Service => "service",
        NodeKind::Commit => "commit",
        NodeKind::PR => "pr",
        NodeKind::Review => "review",
        NodeKind::Comment => "comment",
        NodeKind::Author => "author",
        NodeKind::Ticket => "ticket",
        _ => "unknown",
    }
}

#[derive(Debug, Error)]
pub enum OverviewError {
    #[error(transparent)]
    Graph(#[from] gather_step_storage::GraphStoreError),
    #[error(transparent)]
    Metadata(#[from] gather_step_storage::MetadataStoreError),
    #[error(transparent)]
    DeadCode(#[from] DeadCodeError),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModuleSummary {
    pub module: String,
    pub file_count: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RepoOverview {
    pub repo: String,
    pub modules: Vec<ModuleSummary>,
    pub entry_points: Vec<String>,
    pub node_counts: BTreeMap<String, usize>,
    pub top_hotspots: Vec<String>,
    pub dead_code_candidates: usize,
    pub git_history_available: bool,
}

pub fn build_overview<G: GraphStore, M: MetadataStore>(
    graph: &G,
    metadata: &M,
    repo: &str,
) -> Result<RepoOverview, OverviewError> {
    let nodes = graph.nodes_by_repo(repo)?;
    let mut node_counts = BTreeMap::<String, usize>::new();
    let mut module_counts = BTreeMap::<String, usize>::new();
    for node in &nodes {
        *node_counts
            .entry(node_kind_label(node.kind).to_owned())
            .or_default() += 1;
        if node.kind == NodeKind::File {
            let module = node
                .file_path
                .split('/')
                .next()
                .map_or_else(|| node.file_path.clone(), str::to_owned);
            *module_counts.entry(module).or_default() += 1;
        }
    }
    let mut modules = module_counts
        .into_iter()
        .map(|(module, file_count)| ModuleSummary { module, file_count })
        .collect::<Vec<_>>();
    modules.sort_by(|left, right| {
        right
            .file_count
            .cmp(&left.file_count)
            .then_with(|| left.module.cmp(&right.module))
    });

    let dead_code = find_dead_code(graph, repo)?;
    let live_file_paths = nodes
        .iter()
        .filter(|node| node.kind == NodeKind::File)
        .map(|node| node.file_path.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let top_hotspots = metadata
        .list_file_analytics_for_repo(repo)?
        .into_iter()
        .filter(|record| live_file_paths.contains(record.file_path.as_str()))
        .take(5)
        .map(|record| record.file_path)
        .collect::<Vec<_>>();

    Ok(RepoOverview {
        repo: repo.to_owned(),
        modules,
        entry_points: dead_code.root_files,
        node_counts,
        top_hotspots,
        dead_code_candidates: dead_code.findings.len(),
        git_history_available: metadata.get_last_commit_sha(repo)?.is_some(),
    })
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::PathBuf,
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::{NodeData, NodeKind, node_id};
    use gather_step_storage::{
        FileAnalytics, GraphStore, GraphStoreDb, MetadataStore, MetadataStoreDb,
    };

    use super::build_overview;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempPaths {
        graph: PathBuf,
        metadata: PathBuf,
    }

    impl TempPaths {
        fn new(name: &str) -> Self {
            let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let root = env::temp_dir().join(format!(
                "gather-step-overview-{name}-{}-{id}",
                process::id()
            ));
            Self {
                graph: root.with_extension("redb"),
                metadata: root.with_extension("sqlite"),
            }
        }
    }

    impl Drop for TempPaths {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.graph);
            for suffix in ["", "-wal", "-shm"] {
                let _ = fs::remove_file(PathBuf::from(format!(
                    "{}{}",
                    self.metadata.display(),
                    suffix
                )));
            }
        }
    }

    #[test]
    fn overview_includes_modules_entry_points_and_git_availability() {
        let temp = TempPaths::new("overview");
        let graph = GraphStoreDb::open(&temp.graph).expect("open graph");
        let metadata = MetadataStoreDb::open(&temp.metadata).expect("open metadata");
        graph
            .bulk_insert(
                &[
                    file("service-a", "src/routes/a.ts"),
                    file("service-a", "src/services/a.ts"),
                    route("service-a", "src/routes/a.ts"),
                ],
                &[],
            )
            .expect("graph write");
        metadata
            .set_last_commit_sha("service-a", "deadbeef", 1)
            .expect("anchor");

        let overview = build_overview(&graph, &metadata, "service-a").expect("overview");
        assert!(overview.git_history_available);
        assert!(
            overview
                .entry_points
                .iter()
                .any(|path| path == "src/routes/a.ts")
        );
        assert!(overview.modules.iter().any(|module| module.module == "src"));
    }

    #[test]
    fn overview_preserves_hotspot_rank_and_filters_missing_files() {
        let temp = TempPaths::new("overview-hotspots");
        let graph = GraphStoreDb::open(&temp.graph).expect("open graph");
        let metadata = MetadataStoreDb::open(&temp.metadata).expect("open metadata");
        graph
            .bulk_insert(
                &[file("service-a", "src/a.ts"), file("service-a", "src/b.ts")],
                &[],
            )
            .expect("graph write");
        metadata
            .replace_file_analytics_for_repo(
                "service-a",
                &[
                    FileAnalytics {
                        repo: "service-a".to_owned(),
                        file_path: "src/deleted.ts".to_owned(),
                        total_commits: 5,
                        commits_90d: 5,
                        commits_180d: 5,
                        commits_365d: 5,
                        hotspot_score: 99.0,
                        bus_factor: 1,
                        top_owner_email: None,
                        top_owner_pct: 0.0,
                        complexity_trend: None,
                        last_modified: 10,
                        computed_at: 10,
                    },
                    FileAnalytics {
                        repo: "service-a".to_owned(),
                        file_path: "src/b.ts".to_owned(),
                        total_commits: 5,
                        commits_90d: 5,
                        commits_180d: 5,
                        commits_365d: 5,
                        hotspot_score: 10.0,
                        bus_factor: 1,
                        top_owner_email: None,
                        top_owner_pct: 0.0,
                        complexity_trend: None,
                        last_modified: 10,
                        computed_at: 10,
                    },
                    FileAnalytics {
                        repo: "service-a".to_owned(),
                        file_path: "src/a.ts".to_owned(),
                        total_commits: 5,
                        commits_90d: 5,
                        commits_180d: 5,
                        commits_365d: 5,
                        hotspot_score: 5.0,
                        bus_factor: 1,
                        top_owner_email: None,
                        top_owner_pct: 0.0,
                        complexity_trend: None,
                        last_modified: 10,
                        computed_at: 10,
                    },
                ],
            )
            .expect("analytics write");

        let overview = build_overview(&graph, &metadata, "service-a").expect("overview");
        assert_eq!(overview.top_hotspots, vec!["src/b.ts", "src/a.ts"]);
    }

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
        }
    }

    fn route(repo: &str, file_path: &str) -> NodeData {
        NodeData {
            id: node_id(repo, file_path, NodeKind::Route, "GET /items"),
            kind: NodeKind::Route,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: "GET /items".to_owned(),
            qualified_name: None,
            external_id: None,
            signature: None,
            visibility: None,
            span: None,
            is_virtual: false,
        }
    }
}
