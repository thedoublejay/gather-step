#![forbid(unsafe_code)]

pub mod context_md;
pub mod evidence;
pub mod sanitize;

pub use context_md::{
    ClaudeMdOptions, ContextMdError, DEFAULT_RULE_BYTE_BUDGET, RuleFile, derive_conventions,
    generate_rule_files, render_workspace_summary_agents, render_workspace_summary_claude,
};
pub use evidence::render_evidence_chain;

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::{
        DepthLevel, EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, RegistryStore,
        RepoIndexMetadata, SourceSpan, Visibility, node_id, route_qn, topic_qn, virtual_node,
    };
    use gather_step_storage::{
        FileAnalytics, GraphStore, GraphStoreDb, MetadataStore, MetadataStoreDb,
    };
    use pretty_assertions::assert_eq;

    use super::{ClaudeMdOptions, derive_conventions, generate_rule_files};

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(name: &str) -> Self {
            let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path =
                env::temp_dir().join(format!("gather-step-output-{name}-{}-{id}", process::id()));
            fs::create_dir_all(&path).expect("temp dir");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
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

    fn function(repo: &str, file_path: &str, name: &str, _ordinal: u16) -> NodeData {
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
                line_start: 1,
                line_len: 0,
                column_start: 0,
                column_len: 1,
            }),
            is_virtual: false,
        }
    }

    #[test]
    fn generates_workspace_rule_files() {
        let root = TempDir::new("rules");
        let graph_path = root.path().join("graph.redb");
        let registry_path = root.path().join("registry.json");
        let metadata_path = root.path().join("metadata.sqlite");
        let graph = GraphStoreDb::open(&graph_path).expect("graph should open");
        let metadata = MetadataStoreDb::open(&metadata_path).expect("metadata should open");

        let producer_file = file("backend_standard", "src/producer.ts");
        let consumer_file = file("frontend_standard", "src/client.ts");
        let producer = function("backend_standard", "src/producer.ts", "emitOrder", 0);
        let consumer = function("frontend_standard", "src/client.ts", "useOrders", 0);
        let topic = virtual_node(
            NodeKind::Topic,
            "backend_standard",
            "src/events.ts",
            "order.created",
            topic_qn("kafka", "order.created"),
        );
        let route = virtual_node(
            NodeKind::Route,
            "backend_standard",
            "src/routes.ts",
            "/orders",
            route_qn("GET", "/orders"),
        );
        graph
            .bulk_insert(
                &[
                    producer_file.clone(),
                    consumer_file.clone(),
                    producer.clone(),
                    consumer.clone(),
                    topic.clone(),
                    route.clone(),
                ],
                &[
                    EdgeData {
                        source: producer.id,
                        target: topic.id,
                        kind: EdgeKind::Publishes,
                        metadata: EdgeMetadata::default(),
                        owner_file: producer_file.id,
                        is_cross_file: true,
                    },
                    EdgeData {
                        source: consumer.id,
                        target: topic.id,
                        kind: EdgeKind::Consumes,
                        metadata: EdgeMetadata::default(),
                        owner_file: consumer_file.id,
                        is_cross_file: true,
                    },
                    EdgeData {
                        source: producer.id,
                        target: route.id,
                        kind: EdgeKind::Serves,
                        metadata: EdgeMetadata::default(),
                        owner_file: producer_file.id,
                        is_cross_file: true,
                    },
                ],
            )
            .expect("graph write");

        let mut registry = RegistryStore::open(&registry_path).expect("registry");
        registry
            .register_repo(
                "backend_standard",
                root.path().join("backend"),
                Some(DepthLevel::Full),
            )
            .expect("register backend");
        registry
            .register_repo(
                "frontend_standard",
                root.path().join("frontend"),
                Some(DepthLevel::Full),
            )
            .expect("register frontend");
        registry
            .update_repo_metadata(
                "backend_standard",
                RepoIndexMetadata {
                    last_indexed_at: Some("1".to_owned()),
                    file_count: 1,
                    symbol_count: 3,
                    frameworks: vec!["nestjs".to_owned()],
                    depth_level: DepthLevel::Full,
                },
            )
            .expect("metadata backend");
        registry
            .update_repo_metadata(
                "frontend_standard",
                RepoIndexMetadata {
                    last_indexed_at: Some("1".to_owned()),
                    file_count: 1,
                    symbol_count: 1,
                    frameworks: vec!["react".to_owned()],
                    depth_level: DepthLevel::Full,
                },
            )
            .expect("metadata frontend");
        metadata
            .replace_file_analytics_for_repo(
                "backend_standard",
                &[FileAnalytics {
                    repo: "backend_standard".to_owned(),
                    file_path: "src/producer.ts".to_owned(),
                    total_commits: 2,
                    commits_90d: 2,
                    commits_180d: 2,
                    commits_365d: 2,
                    hotspot_score: 7.5,
                    bus_factor: 1,
                    top_owner_email: Some("alice@example.com".to_owned()),
                    top_owner_pct: 0.8,
                    complexity_trend: None,
                    last_modified: 1,
                    computed_at: 1,
                }],
            )
            .expect("analytics backend");

        let files = generate_rule_files(
            &graph,
            Some(&metadata),
            registry.registry(),
            &ClaudeMdOptions::default(),
        )
        .expect("rules should render");

        assert_eq!(files.len(), 3);
        assert!(
            files
                .iter()
                .any(|file| file.relative_path.ends_with("architecture.md"))
        );
        assert!(
            files
                .iter()
                .any(|file| file.content.contains("Repository Map"))
        );
        assert!(
            files
                .iter()
                .any(|file| file.content.contains("Event Surface"))
        );
        assert!(
            files
                .iter()
                .any(|file| file.content.contains("src/producer.ts"))
        );
        assert!(files.iter().all(|file| {
            !file
                .content
                .contains("_Git analytics data not available yet_")
        }));
        // Default budget is in effect, so every rule file should fit under it.
        for file in &files {
            assert!(
                file.content.len() <= super::DEFAULT_RULE_BYTE_BUDGET,
                "{} exceeded byte budget ({} bytes)",
                file.relative_path,
                file.content.len()
            );
            assert!(
                !file.content.contains("<!-- Truncated:"),
                "{} unexpectedly truncated",
                file.relative_path
            );
        }
    }

    #[test]
    fn truncation_marker_appears_when_content_exceeds_byte_budget() {
        let root = TempDir::new("budget");
        let graph = GraphStoreDb::open(root.path().join("graph.redb")).expect("graph open");
        graph.bulk_insert(&[], &[]).expect("empty insert");

        let registry_path = root.path().join("registry.json");
        let registry = RegistryStore::open(&registry_path).expect("registry");

        // Tiny budget forces truncation even on near-empty rule files because
        // the auto-generated header alone already exceeds 64 bytes.
        let files = generate_rule_files(
            &graph,
            None,
            registry.registry(),
            &ClaudeMdOptions {
                repo_filter: None,
                byte_budget: Some(64),
                workspace_root: None,
            },
        )
        .expect("rules should render");

        let architecture = files
            .iter()
            .find(|file| file.relative_path.ends_with("architecture.md"))
            .expect("architecture rule should be generated");
        assert!(
            architecture.content.contains("<!-- Truncated:"),
            "architecture rule should have a truncation marker:\n{}",
            architecture.content
        );
    }

    #[test]
    fn derives_framework_and_graph_conventions() {
        let root = TempDir::new("conventions");
        let graph = GraphStoreDb::open(root.path().join("graph.redb")).expect("graph should open");
        let topic = virtual_node(
            NodeKind::Topic,
            "backend_standard",
            "src/events.ts",
            "order.created",
            topic_qn("kafka", "order.created"),
        );
        graph.bulk_insert(&[topic], &[]).expect("insert topic");

        let registry_path = root.path().join("registry.json");
        let mut registry = RegistryStore::open(&registry_path).expect("registry");
        registry
            .register_repo(
                "backend_standard",
                root.path().join("backend"),
                Some(DepthLevel::Full),
            )
            .expect("register");
        registry
            .update_repo_metadata(
                "backend_standard",
                RepoIndexMetadata {
                    last_indexed_at: Some("1".to_owned()),
                    file_count: 1,
                    symbol_count: 1,
                    frameworks: vec!["nestjs".to_owned()],
                    depth_level: DepthLevel::Full,
                },
            )
            .expect("metadata");

        let conventions =
            derive_conventions(&graph, registry.registry(), None).expect("conventions");
        assert_eq!(conventions.iter().any(|item| item.contains("NestJS")), true);
        assert_eq!(conventions.is_empty(), false);
    }

    /// Absolute repo paths in generated rule files must be rendered relative
    /// to the workspace root when `ClaudeMdOptions::workspace_root` is
    /// supplied.  Paths outside the workspace must use the sentinel.
    #[test]
    fn repo_rule_path_is_workspace_relative_when_root_provided() {
        let root = TempDir::new("lsec3");
        let ws = root.path().join("workspace");
        let repo_path = ws.join("service-a");
        std::fs::create_dir_all(&repo_path).expect("repo dir");

        let graph_path = root.path().join("graph.redb");
        let registry_path = root.path().join("registry.json");
        let graph = GraphStoreDb::open(&graph_path).expect("graph should open");
        graph.bulk_insert(&[], &[]).expect("empty insert");

        let mut registry = RegistryStore::open(&registry_path).expect("registry");
        registry
            .register_repo("service-a", repo_path, Some(DepthLevel::Full))
            .expect("register");
        registry
            .update_repo_metadata(
                "service-a",
                RepoIndexMetadata {
                    last_indexed_at: Some("1".to_owned()),
                    file_count: 1,
                    symbol_count: 1,
                    frameworks: vec![],
                    depth_level: DepthLevel::Full,
                },
            )
            .expect("metadata");

        let files = generate_rule_files(
            &graph,
            None,
            registry.registry(),
            &ClaudeMdOptions {
                repo_filter: Some("service-a".to_owned()),
                byte_budget: None,
                workspace_root: Some(ws.clone()),
            },
        )
        .expect("rules should render");

        let repo_rule = files
            .iter()
            .find(|f| f.relative_path.contains("service-a"))
            .expect("repo rule file should be generated");

        // Must contain the relative path, not the absolute path.
        assert!(
            repo_rule.content.contains("service-a"),
            "relative path 'service-a' should appear in content:\n{}",
            repo_rule.content
        );
        assert!(
            !repo_rule.content.contains(ws.to_str().unwrap_or("")),
            "absolute workspace prefix must not appear in content:\n{}",
            repo_rule.content
        );
    }

    /// Paths outside the workspace render as the `<outside-workspace>` sentinel.
    #[test]
    fn repo_rule_outside_path_renders_as_sentinel() {
        let root = TempDir::new("lsec3-outside");
        let ws = root.path().join("workspace");
        // Repo path is outside the declared workspace root.
        let outside = root.path().join("external-service");
        std::fs::create_dir_all(&outside).expect("external dir");
        std::fs::create_dir_all(&ws).expect("ws dir");

        let graph_path = root.path().join("graph.redb");
        let registry_path = root.path().join("registry.json");
        let graph = GraphStoreDb::open(&graph_path).expect("graph should open");
        graph.bulk_insert(&[], &[]).expect("empty insert");

        let mut registry = RegistryStore::open(&registry_path).expect("registry");
        registry
            .register_repo("external-service", outside, Some(DepthLevel::Full))
            .expect("register");
        registry
            .update_repo_metadata(
                "external-service",
                RepoIndexMetadata {
                    last_indexed_at: Some("1".to_owned()),
                    file_count: 0,
                    symbol_count: 0,
                    frameworks: vec![],
                    depth_level: DepthLevel::Full,
                },
            )
            .expect("metadata");

        let files = generate_rule_files(
            &graph,
            None,
            registry.registry(),
            &ClaudeMdOptions {
                repo_filter: Some("external-service".to_owned()),
                byte_budget: None,
                workspace_root: Some(ws),
            },
        )
        .expect("rules should render");

        let repo_rule = files
            .iter()
            .find(|f| f.relative_path.contains("external-service"))
            .expect("repo rule file should be generated");

        assert!(
            repo_rule.content.contains("<outside-workspace>"),
            "external path must render as sentinel:\n{}",
            repo_rule.content
        );
    }
}
