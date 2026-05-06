#![forbid(unsafe_code)]

pub mod budget;
pub mod config;
pub mod error;
pub mod evidence;
pub mod ids;
pub mod output;
pub mod server;
pub mod tool_trace;
pub mod tools;

pub use config::{DEFAULT_MCP_MAX_LIMIT, MAX_INPUT_LENGTH, McpContext, McpServerConfig};
pub use error::McpServerError;
pub use server::GatherStepMcpServer;
pub use tool_trace::{ToolCallRecord, TraceSink, TraceWriter, Tracer};

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    };

    use gather_step_core::{
        DepthLevel, EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, RegistryStore,
        RepoIndexMetadata, ResolverStrategy, SourceSpan, Visibility, node_id, route_qn,
        shared_symbol_qn, topic_qn, virtual_node,
    };
    use gather_step_storage::{
        CommitFileChangeKind, CommitFileDeltaRecord, CommitRecord, FileAnalytics, GraphStore,
        GraphStoreDb, IndexingOptions, MetadataStore, MetadataStoreDb, RepoIndexer, SearchDocument,
        SearchStore, SearchStoreError, TantivySearchStore, WorkspaceStores,
    };
    use pretty_assertions::assert_eq;
    use rmcp::{
        ServiceExt,
        model::{CallToolRequestParams, CallToolResult},
    };
    use serde_json::Value;

    use crate::{
        McpContext, McpServerConfig,
        ids::encode_node_id,
        server::GatherStepMcpServer,
        tools::{
            orientation::{get_graph_schema, list_repos},
            search::{SearchRequest, search_symbols},
        },
    };

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(name: &str) -> Self {
            let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path =
                env::temp_dir().join(format!("gather-step-mcp-{name}-{}-{id}", process::id()));
            fs::create_dir_all(&path).expect("temp dir should create");
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

    fn sample_node(
        repo: &str,
        file_path: &str,
        kind: NodeKind,
        name: &str,
        _ordinal: u16,
    ) -> NodeData {
        NodeData {
            id: node_id(repo, file_path, kind, name),
            kind,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(format!("{repo}::{name}")),
            external_id: None,
            signature: Some(format!("{name}()")),
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

    fn sample_edge(
        source: gather_step_core::NodeId,
        target: gather_step_core::NodeId,
        owner_file: gather_step_core::NodeId,
    ) -> EdgeData {
        EdgeData {
            source,
            target,
            kind: EdgeKind::Calls,
            metadata: EdgeMetadata::default(),
            owner_file,
            is_cross_file: false,
        }
    }

    fn tool_failed<E>(result: Result<CallToolResult, E>) -> bool {
        match result {
            Ok(payload) => payload.is_error == Some(true),
            Err(_) => true,
        }
    }

    #[test]
    fn orientation_helpers_return_expected_shapes() {
        let temp = TempDir::new("orientation");
        let graph_path = temp.path().join("graph.redb");
        let registry_path = temp.path().join("registry.json");

        let graph = GraphStoreDb::open(&graph_path).expect("graph store should open");
        let file = sample_node(
            "backend_standard",
            "src/lib.ts",
            NodeKind::File,
            "src/lib.ts",
            0,
        );
        let a = sample_node("backend_standard", "src/lib.ts", NodeKind::Function, "a", 0);
        let b = sample_node("backend_standard", "src/lib.ts", NodeKind::Function, "b", 1);
        graph
            .bulk_insert(
                &[file.clone(), a.clone(), b.clone()],
                &[sample_edge(a.id, b.id, file.id)],
            )
            .expect("graph write should succeed");
        drop(graph);

        let mut registry = RegistryStore::open(&registry_path).expect("registry should open");
        registry
            .register_repo(
                "backend_standard",
                temp.path().join("repos/backend_standard"),
                Some(DepthLevel::Full),
            )
            .expect("repo registration should succeed");
        registry
            .update_repo_metadata(
                "backend_standard",
                RepoIndexMetadata {
                    last_indexed_at: Some("2026-04-14T00:00:00Z".to_owned()),
                    file_count: 10,
                    symbol_count: 20,
                    frameworks: vec!["nestjs".to_owned()],
                    depth_level: DepthLevel::Full,
                },
            )
            .expect("metadata update should succeed");

        let ctx = McpContext::open(McpServerConfig::new(registry_path, graph_path))
            .expect("context should open");

        let schema = get_graph_schema(&ctx).expect("schema should load");
        let schema_json = serde_json::to_string(&schema).expect("schema should serialize");
        assert!(schema_json.len() < 800, "schema output should stay compact");
        assert!(
            schema
                .data
                .node_kinds
                .iter()
                .any(|entry| entry.starts_with("Function["))
        );
        assert!(
            schema
                .data
                .edge_kinds
                .iter()
                .any(|entry| entry.starts_with("Calls["))
        );

        let repos = list_repos(&ctx).expect("repos should load");
        assert_eq!(repos.data.total, 1);
        assert_eq!(repos.data.repos[0].repo, "backend_standard");
        assert_eq!(repos.data.repos[0].depth_level, "full");
        assert_eq!(repos.data.repos[0].frameworks, vec!["nestjs".to_owned()]);
    }

    #[test]
    fn list_repos_reads_latest_registry_snapshot() {
        let temp = TempDir::new("registry-refresh");
        let graph_path = temp.path().join("graph.redb");
        let registry_path = temp.path().join("registry.json");
        GraphStoreDb::open(&graph_path).expect("graph store should open");

        let mut registry = RegistryStore::open(&registry_path).expect("registry should open");
        registry
            .register_repo(
                "backend_standard",
                temp.path().join("repos/backend_standard"),
                Some(DepthLevel::Full),
            )
            .expect("repo registration should succeed");

        let ctx = McpContext::open(McpServerConfig::new(registry_path.clone(), graph_path))
            .expect("context should open");

        registry
            .register_repo(
                "frontend_standard",
                temp.path().join("repos/frontend_standard"),
                Some(DepthLevel::Full),
            )
            .expect("repo registration should succeed");

        let repos = list_repos(&ctx).expect("repos should load");
        assert_eq!(repos.data.total, 2);
        assert!(
            repos
                .data
                .repos
                .iter()
                .any(|repo| repo.repo == "frontend_standard")
        );
    }

    #[test]
    fn context_can_be_built_from_preopened_workspace_stores() {
        let temp = TempDir::new("shared-stores");
        let graph_path = temp.path().join("graph.redb");
        let registry_path = temp.path().join("registry.json");

        GraphStoreDb::open(&graph_path).expect("graph store should open");
        let stores =
            Arc::new(WorkspaceStores::open(temp.path()).expect("workspace stores should open"));
        let ctx = McpContext::from_workspace_stores(
            McpServerConfig::new(registry_path, graph_path),
            Arc::clone(&stores),
        );

        let doc = SearchDocument {
            node_id: node_id(
                "backend_standard",
                "src/service.ts",
                NodeKind::Function,
                "sharedLookup",
            ),
            repo: "backend_standard".to_owned(),
            file_path: "src/service.ts".to_owned(),
            symbol_name: "sharedLookup".to_owned(),
            content: "shared lookup helper".to_owned(),
            description: "shared lookup helper".to_owned(),
            node_kind: NodeKind::Function,
            last_modified: 7,
            is_exported: true,
            lang: "ts".to_owned(),
        };
        stores
            .search()
            .index_symbol(&doc)
            .expect("document should index");

        let hits = ctx
            .search()
            .search("sharedLookup", 5)
            .expect("search should succeed");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].symbol_name, "sharedLookup");
    }

    #[test]
    fn open_wrapper_preserves_read_only_search_behavior() {
        let temp = TempDir::new("open-read-only");
        let graph_path = temp.path().join("graph.redb");
        let registry_path = temp.path().join("registry.json");

        GraphStoreDb::open(&graph_path).expect("graph store should open");
        let ctx = McpContext::open(McpServerConfig::new(registry_path, graph_path))
            .expect("context should open");

        let error = ctx
            .search()
            .index_symbol(&SearchDocument {
                node_id: node_id(
                    "backend_standard",
                    "src/service.ts",
                    NodeKind::Function,
                    "readOnlyWrite",
                ),
                repo: "backend_standard".to_owned(),
                file_path: "src/service.ts".to_owned(),
                symbol_name: "readOnlyWrite".to_owned(),
                content: "read only".to_owned(),
                description: "read only".to_owned(),
                node_kind: NodeKind::Function,
                last_modified: 1,
                is_exported: true,
                lang: "ts".to_owned(),
            })
            .expect_err("wrapper-opened MCP context should keep search read-only");
        assert!(matches!(error, SearchStoreError::ReadOnly));
    }

    #[test]
    fn search_reads_latest_index_snapshot_without_reopening_context() {
        let temp = TempDir::new("search-refresh");
        let graph_path = temp.path().join("graph.redb");
        let registry_path = temp.path().join("registry.json");
        let search_path = temp.path().join("search");

        let node = sample_node(
            "backend_standard",
            "src/orders.ts",
            NodeKind::Function,
            "createOrder",
            0,
        );

        // S6: `repo` and `file_path` are rehydrated from the graph store, so the
        // node must be present in the graph before McpContext holds the db lock.
        {
            let graph = GraphStoreDb::open(&graph_path).expect("graph store should open");
            graph
                .bulk_insert(std::slice::from_ref(&node), &[])
                .expect("graph write should succeed");
        }

        let mut registry = RegistryStore::open(&registry_path).expect("registry should open");
        registry
            .register_repo(
                "backend_standard",
                temp.path().join("repos/backend_standard"),
                Some(DepthLevel::Full),
            )
            .expect("repo registration should succeed");

        let ctx = McpContext::open(McpServerConfig::new(registry_path, graph_path))
            .expect("context should open");

        // Before Tantivy is indexed the search returns nothing (reader refresh
        // test: the McpContext is not re-opened between writes and reads).
        assert!(
            search_symbols(
                &ctx,
                SearchRequest {
                    budget_bytes: None,
                    cursor: None,
                    kind: None,
                    language: None,
                    limit: Some(10),
                    query: "createOrder".to_owned(),
                    repo: None,
                },
            )
            .expect("search should succeed")
            .data
            .results
            .is_empty()
        );

        let writable = TantivySearchStore::open(&search_path).expect("writable search store");
        let doc = SearchDocument::from_node(&node, 1);
        writable.index_symbol(&doc).expect("document should index");

        // After Tantivy is indexed the context must pick up the new snapshot
        // without reopening (reader-refresh behaviour under test).
        let hits = search_symbols(
            &ctx,
            SearchRequest {
                budget_bytes: None,
                cursor: None,
                kind: None,
                language: None,
                limit: Some(10),
                query: "createOrder".to_owned(),
                repo: None,
            },
        )
        .expect("search should succeed")
        .data
        .results;

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].symbol_name, "createOrder");
    }

    #[tokio::test]
    async fn server_registers_tools_and_returns_structured_content() {
        let temp = TempDir::new("server");
        let graph_path = temp.path().join("graph.redb");
        let registry_path = temp.path().join("registry.json");
        let search_path = temp.path().join("search");

        let graph = GraphStoreDb::open(&graph_path).expect("graph store should open");
        let file = sample_node(
            "shared_contracts",
            "src/types.ts",
            NodeKind::File,
            "src/types.ts",
            0,
        );
        let caller = sample_node(
            "shared_contracts",
            "src/types.ts",
            NodeKind::Function,
            "createOrder",
            0,
        );
        let downstream = sample_node(
            "shared_contracts",
            "src/types.ts",
            NodeKind::Function,
            "normalizeOrder",
            1,
        );
        let consumer_file = sample_node(
            "frontend_standard",
            "src/view.ts",
            NodeKind::File,
            "src/view.ts",
            0,
        );
        let consumer = sample_node(
            "frontend_standard",
            "src/view.ts",
            NodeKind::Function,
            "renderOrder",
            0,
        );
        let decorator = sample_node(
            "shared_contracts",
            "src/types.ts",
            NodeKind::Decorator,
            "Transactional",
            2,
        );
        let shared_symbol = virtual_node(
            NodeKind::SharedSymbol,
            "shared_contracts",
            "src/types.ts",
            "OrderState",
            shared_symbol_qn("@workspace/shared-contracts", "2.0.0", "OrderState"),
        );
        let order_items = virtual_node(
            NodeKind::DataField,
            "shared_contracts",
            "src/types.ts",
            "orderItems",
            "data-field::shared_contracts::src/types.ts::orderItems",
        );
        let order_item_ids = virtual_node(
            NodeKind::DataField,
            "shared_contracts",
            "src/types.ts",
            "orderItemIds",
            "data-field::shared_contracts::src/types.ts::orderItemIds",
        );
        graph
            .bulk_insert(
                &[
                    file.clone(),
                    caller.clone(),
                    downstream.clone(),
                    consumer_file.clone(),
                    consumer.clone(),
                    decorator.clone(),
                    shared_symbol.clone(),
                    order_items.clone(),
                    order_item_ids.clone(),
                ],
                &[
                    sample_edge(caller.id, downstream.id, file.id),
                    EdgeData {
                        source: caller.id,
                        target: decorator.id,
                        kind: EdgeKind::UsesDecorator,
                        metadata: EdgeMetadata::default(),
                        owner_file: file.id,
                        is_cross_file: false,
                    },
                    EdgeData {
                        source: caller.id,
                        target: shared_symbol.id,
                        kind: EdgeKind::UsesShared,
                        metadata: EdgeMetadata {
                            confidence: Some(980),
                            ..EdgeMetadata::default()
                        },
                        owner_file: file.id,
                        is_cross_file: true,
                    },
                    EdgeData {
                        source: consumer.id,
                        target: shared_symbol.id,
                        kind: EdgeKind::UsesShared,
                        metadata: EdgeMetadata {
                            confidence: Some(930),
                            ..EdgeMetadata::default()
                        },
                        owner_file: consumer_file.id,
                        is_cross_file: true,
                    },
                    EdgeData {
                        source: order_items.id,
                        target: order_item_ids.id,
                        kind: EdgeKind::DerivesFieldFrom,
                        metadata: EdgeMetadata {
                            confidence: Some(900),
                            ..EdgeMetadata::default()
                        },
                        owner_file: file.id,
                        is_cross_file: false,
                    },
                ],
            )
            .expect("graph write should succeed");
        drop(graph);

        let search = TantivySearchStore::open(&search_path).expect("search store should open");
        search
            .index_symbols(&[
                SearchDocument::from_node(&caller, 1),
                SearchDocument::from_node(&downstream, 1),
                SearchDocument::from_node(&consumer, 1),
            ])
            .expect("search documents should index");
        drop(search);

        let mut registry = RegistryStore::open(&registry_path).expect("registry should open");
        registry
            .register_repo(
                "shared_contracts",
                temp.path().join("repos/shared_contracts"),
                Some(DepthLevel::Level1),
            )
            .expect("repo registration should succeed");
        registry
            .register_repo(
                "frontend_standard",
                temp.path().join("repos/frontend_standard"),
                Some(DepthLevel::Level1),
            )
            .expect("repo registration should succeed");
        registry
            .update_repo_metadata(
                "shared_contracts",
                RepoIndexMetadata {
                    last_indexed_at: Some("2026-04-14T01:00:00Z".to_owned()),
                    file_count: 1,
                    symbol_count: 2,
                    frameworks: vec!["react".to_owned()],
                    depth_level: DepthLevel::Level1,
                },
            )
            .expect("metadata update should succeed");
        registry
            .update_repo_metadata(
                "frontend_standard",
                RepoIndexMetadata {
                    last_indexed_at: Some("2026-04-14T01:05:00Z".to_owned()),
                    file_count: 1,
                    symbol_count: 1,
                    frameworks: vec!["react".to_owned()],
                    depth_level: DepthLevel::Level1,
                },
            )
            .expect("metadata update should succeed");
        let metadata_path = temp.path().join("metadata.sqlite");
        let metadata = MetadataStoreDb::open(&metadata_path).expect("metadata store should open");
        metadata
            .insert_commits(&[
                CommitRecord {
                    sha: "a1".to_owned(),
                    repo: "shared_contracts".to_owned(),
                    author_email: "alice@example.com".to_owned(),
                    date: 100,
                    message: "feat: add types".to_owned(),
                    classification: Some("feat".to_owned()),
                    files_changed: 1,
                    insertions: 3,
                    deletions: 0,
                    has_decision_signal: false,
                    pr_number: None,
                },
                CommitRecord {
                    sha: "b2".to_owned(),
                    repo: "shared_contracts".to_owned(),
                    author_email: "bob@example.com".to_owned(),
                    date: 200,
                    message: "fix: adjust types".to_owned(),
                    classification: Some("fix".to_owned()),
                    files_changed: 1,
                    insertions: 1,
                    deletions: 1,
                    has_decision_signal: false,
                    pr_number: None,
                },
            ])
            .expect("commit rows should insert");
        metadata
            .upsert_commit_file_deltas(&[
                CommitFileDeltaRecord {
                    repo: "shared_contracts".to_owned(),
                    sha: "a1".to_owned(),
                    file_path: "src/types.ts".to_owned(),
                    change_kind: CommitFileChangeKind::Modified,
                    insertions: Some(3),
                    deletions: Some(0),
                    old_path: None,
                },
                CommitFileDeltaRecord {
                    repo: "shared_contracts".to_owned(),
                    sha: "b2".to_owned(),
                    file_path: "src/types.ts".to_owned(),
                    change_kind: CommitFileChangeKind::Modified,
                    insertions: Some(1),
                    deletions: Some(1),
                    old_path: None,
                },
            ])
            .expect("commit deltas should insert");
        metadata
            .replace_file_analytics_for_repo(
                "shared_contracts",
                &[FileAnalytics {
                    repo: "shared_contracts".to_owned(),
                    file_path: "src/types.ts".to_owned(),
                    total_commits: 2,
                    commits_90d: 2,
                    commits_180d: 2,
                    commits_365d: 2,
                    hotspot_score: 4.5,
                    bus_factor: 1,
                    top_owner_email: Some("alice@example.com".to_owned()),
                    top_owner_pct: 0.75,
                    complexity_trend: None,
                    last_modified: 200,
                    computed_at: 200,
                }],
            )
            .expect("analytics rows should insert");
        metadata
            .set_last_commit_sha("shared_contracts", "b2", 200)
            .expect("anchor should insert");

        let ctx = McpContext::open(McpServerConfig::new(
            registry_path.clone(),
            graph_path.clone(),
        ))
        .expect("context should open");
        let server = GatherStepMcpServer::new(ctx);
        let (server_transport, client_transport) = tokio::io::duplex(16 * 1024);

        let server_handle = tokio::spawn(async move {
            let running = server
                .serve(server_transport)
                .await
                .expect("server should start");
            running.waiting().await.expect("server should wait cleanly");
        });

        let client = ().serve(client_transport).await.expect("client should start");
        let tools = client
            .peer()
            .list_tools(None)
            .await
            .expect("tools/list should succeed");
        let tool_names = tools
            .tools
            .iter()
            .map(|tool| tool.name.as_ref())
            .collect::<Vec<_>>();
        assert!(tools.tools.iter().all(|tool| !tool.input_schema.is_empty()));
        assert!(tools.tools.iter().all(|tool| tool.output_schema.is_some()));
        assert!(tool_names.contains(&"get_graph_schema"));
        assert!(tool_names.contains(&"get_graph_schema_summary"));
        assert!(tool_names.contains(&"list_repos"));
        assert!(tool_names.contains(&"search"));
        assert!(tool_names.contains(&"get_symbol"));
        assert!(tool_names.contains(&"get_callers"));
        assert!(tool_names.contains(&"get_callees"));
        assert!(tool_names.contains(&"trace_impact"));
        assert!(tool_names.contains(&"crud_trace"));
        assert!(tool_names.contains(&"cross_repo_deps"));
        assert!(tool_names.contains(&"get_shared_type_usage"));
        assert!(tool_names.contains(&"brief"));
        assert!(tool_names.contains(&"context"));
        assert!(tool_names.contains(&"batch_query"));
        assert!(tool_names.contains(&"who_owns"));
        assert!(tool_names.contains(&"get_dead_code"));
        assert!(tool_names.contains(&"get_conventions"));
        assert!(tool_names.contains(&"get_overview"));
        assert!(tool_names.contains(&"get_context_pack"));
        assert!(tool_names.contains(&"projection_impact"));
        assert!(tool_names.contains(&"plan_change"));
        assert!(
            !tool_names.contains(&"debug_route"),
            "debug_route alias must be removed"
        );
        assert!(
            !tool_names.contains(&"debug_event"),
            "debug_event alias must be removed"
        );
        assert!(tool_names.contains(&"fix_surface"));
        assert!(tool_names.contains(&"get_change_impact_pack"));

        let schema_tool = tools
            .tools
            .iter()
            .find(|tool| tool.name == "get_graph_schema")
            .expect("schema tool should be registered");
        assert!(schema_tool.output_schema.is_some());

        let result = client
            .call_tool(CallToolRequestParams::new("get_graph_schema"))
            .await
            .expect("tool call should succeed");
        assert_eq!(result.is_error, Some(false));
        assert!(result.structured_content.is_some());

        let summary_result = client
            .call_tool(CallToolRequestParams::new("get_graph_schema_summary"))
            .await
            .expect("summary tool call should succeed");
        assert_eq!(summary_result.is_error, Some(false));
        assert!(summary_result.structured_content.is_some());

        let structured = result
            .structured_content
            .expect("structured content should exist");
        let node_kinds = structured
            .get("data")
            .and_then(|data| data.get("node_kinds"))
            .and_then(Value::as_array)
            .expect("node_kinds should be present");
        assert!(!node_kinds.is_empty());
        let schema_json =
            serde_json::to_string(&structured).expect("schema payload should serialize");
        assert!(schema_json.len() < 800, "graph schema should stay compact");

        let search_result = client
            .call_tool(
                CallToolRequestParams::new("search").with_arguments(
                    serde_json::json!({
                        "query": "createOrder",
                        "limit": 5
                    })
                    .as_object()
                    .expect("search args should be an object")
                    .clone(),
                ),
            )
            .await
            .expect("search tool call should succeed");
        let search_payload = search_result
            .structured_content
            .expect("search structured content should exist");
        let search_items = search_payload
            .get("data")
            .and_then(|data| data.get("results"))
            .and_then(Value::as_array)
            .expect("search results should be present");
        assert_eq!(search_items.len(), 1);
        let found_symbol_id = search_items[0]
            .get("symbol_id")
            .and_then(Value::as_str)
            .expect("search symbol_id should be present")
            .to_owned();

        let symbol_result = client
            .call_tool(
                CallToolRequestParams::new("get_symbol").with_arguments(
                    serde_json::json!({
                        "symbol_id": found_symbol_id
                    })
                    .as_object()
                    .expect("symbol args should be an object")
                    .clone(),
                ),
            )
            .await
            .expect("get_symbol tool call should succeed");
        let symbol_payload = symbol_result
            .structured_content
            .expect("symbol structured content should exist");
        assert_eq!(
            symbol_payload
                .get("data")
                .and_then(|data| data.get("name"))
                .and_then(Value::as_str),
            Some("createOrder")
        );
        let decorators = symbol_payload
            .get("data")
            .and_then(|data| data.get("decorators"))
            .and_then(Value::as_array)
            .expect("decorators should be present");
        assert_eq!(decorators.len(), 1);
        assert_eq!(decorators[0].as_str(), Some("Transactional"));

        let missing_symbol_result = client
            .call_tool(
                CallToolRequestParams::new("get_symbol").with_arguments(
                    serde_json::json!({
                        "symbol_id": encode_node_id(node_id(
                            "shared_contracts",
                            "src/types.ts",
                            NodeKind::Function,
                            "missingOrder",
                        ))
                    })
                    .as_object()
                    .expect("missing symbol args should be an object")
                    .clone(),
                ),
            )
            .await
            .expect("missing get_symbol call should still succeed");
        let missing_symbol_payload = missing_symbol_result
            .structured_content
            .expect("missing symbol structured content should exist");
        assert_eq!(
            missing_symbol_payload
                .get("data")
                .and_then(|data| data.get("found"))
                .and_then(Value::as_bool),
            Some(false)
        );

        let projection_result = client
            .call_tool(
                CallToolRequestParams::new("projection_impact").with_arguments(
                    serde_json::json!({
                        "target": "orderItemIds",
                        "repo": "shared_contracts"
                    })
                    .as_object()
                    .expect("projection args should be an object")
                    .clone(),
                ),
            )
            .await
            .expect("projection_impact tool call should succeed");
        let projection_payload = projection_result
            .structured_content
            .expect("projection structured content should exist");
        let projection_data = projection_payload
            .get("data")
            .expect("projection response should include data");
        assert_eq!(
            projection_data.get("resolved").and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            projection_data
                .get("source_fields")
                .and_then(Value::as_array)
                .and_then(|items| items.first())
                .and_then(|item| item.get("field_path"))
                .and_then(Value::as_str),
            Some("orderItems")
        );

        let empty_search = client
            .call_tool(
                CallToolRequestParams::new("search").with_arguments(
                    serde_json::json!({
                        "query": "   "
                    })
                    .as_object()
                    .expect("empty search args should be an object")
                    .clone(),
                ),
            )
            .await;
        assert!(tool_failed(empty_search));

        let empty_projection_target = client
            .call_tool(
                CallToolRequestParams::new("projection_impact").with_arguments(
                    serde_json::json!({ "target": "   " })
                        .as_object()
                        .expect("empty projection args should be an object")
                        .clone(),
                ),
            )
            .await;
        assert!(tool_failed(empty_projection_target));

        let outgoing_result = client
            .call_tool(
                CallToolRequestParams::new("get_callees").with_arguments(
                    serde_json::json!({
                        "symbol_id": encode_node_id(caller.id),
                        "depth": 2
                    })
                    .as_object()
                    .expect("callee args should be an object")
                    .clone(),
                ),
            )
            .await
            .expect("get_callees tool call should succeed");
        let outgoing_payload = outgoing_result
            .structured_content
            .expect("callee structured content should exist");
        let outgoing = outgoing_payload
            .get("data")
            .and_then(|data| data.get("traversal"))
            .and_then(Value::as_array)
            .expect("callee traversal should be present");
        assert_eq!(outgoing.len(), 1);
        assert_eq!(
            outgoing[0].get("symbol_name").and_then(Value::as_str),
            Some("normalizeOrder")
        );

        let incoming_result = client
            .call_tool(
                CallToolRequestParams::new("get_callers").with_arguments(
                    serde_json::json!({
                        "symbol_id": encode_node_id(downstream.id),
                        "depth": 2
                    })
                    .as_object()
                    .expect("caller args should be an object")
                    .clone(),
                ),
            )
            .await
            .expect("get_callers tool call should succeed");
        let incoming_payload = incoming_result
            .structured_content
            .expect("caller structured content should exist");
        let incoming = incoming_payload
            .get("data")
            .and_then(|data| data.get("traversal"))
            .and_then(Value::as_array)
            .expect("caller traversal should be present");
        assert_eq!(incoming.len(), 1);
        assert_eq!(
            incoming[0].get("symbol_name").and_then(Value::as_str),
            Some("createOrder")
        );

        let capped_callers_result = client
            .call_tool(
                CallToolRequestParams::new("get_callers").with_arguments(
                    serde_json::json!({
                        "symbol_id": encode_node_id(downstream.id),
                        "depth": 99
                    })
                    .as_object()
                    .expect("capped caller args should be an object")
                    .clone(),
                ),
            )
            .await
            .expect("capped get_callers tool call should succeed");
        let capped_callers_payload = capped_callers_result
            .structured_content
            .expect("capped caller structured content should exist");
        assert_eq!(
            capped_callers_payload
                .get("meta")
                .and_then(|meta| meta.get("depth_capped"))
                .and_then(Value::as_bool),
            Some(true)
        );

        let deps_result = client
            .call_tool(
                CallToolRequestParams::new("cross_repo_deps").with_arguments(
                    serde_json::json!({
                        "repo": "shared_contracts"
                    })
                    .as_object()
                    .expect("deps args should be an object")
                    .clone(),
                ),
            )
            .await
            .expect("cross_repo_deps tool call should succeed");
        let deps_payload = deps_result
            .structured_content
            .expect("deps structured content should exist");
        let dependencies = deps_payload
            .get("data")
            .and_then(|data| data.get("dependencies"))
            .and_then(Value::as_array)
            .expect("dependencies should be present");
        assert_eq!(dependencies.len(), 1);
        assert_eq!(
            dependencies[0].get("repo").and_then(Value::as_str),
            Some("frontend_standard")
        );

        let invalid_repo_deps = client
            .call_tool(
                CallToolRequestParams::new("cross_repo_deps").with_arguments(
                    serde_json::json!({
                        "repo": "missing_repo"
                    })
                    .as_object()
                    .expect("invalid repo deps args should be an object")
                    .clone(),
                ),
            )
            .await;
        assert!(tool_failed(invalid_repo_deps));

        let shared_usage_result = client
            .call_tool(
                CallToolRequestParams::new("get_shared_type_usage").with_arguments(
                    serde_json::json!({
                        "type_name": "OrderState"
                    })
                    .as_object()
                    .expect("shared usage args should be an object")
                    .clone(),
                ),
            )
            .await
            .expect("get_shared_type_usage tool call should succeed");
        let shared_usage_payload = shared_usage_result
            .structured_content
            .expect("shared usage structured content should exist");
        let shared_matches = shared_usage_payload
            .get("data")
            .and_then(|data| data.get("matches"))
            .and_then(Value::as_array)
            .expect("shared type matches should be present");
        assert_eq!(shared_matches.len(), 1);

        let impact_result = client
            .call_tool(
                CallToolRequestParams::new("trace_impact").with_arguments(
                    serde_json::json!({
                        "target": "OrderState",
                        "depth": 2
                    })
                    .as_object()
                    .expect("impact args should be an object")
                    .clone(),
                ),
            )
            .await
            .expect("trace_impact tool call should succeed");
        let impact_payload = impact_result
            .structured_content
            .expect("impact structured content should exist");
        let impacted_repos = impact_payload
            .get("data")
            .and_then(|data| data.get("impacted_repos"))
            .and_then(Value::as_array)
            .expect("impacted repos should be present");
        assert!(
            impacted_repos.iter().any(|repo| {
                repo.get("repo").and_then(Value::as_str) == Some("frontend_standard")
            })
        );

        let missing_impact = client
            .call_tool(
                CallToolRequestParams::new("trace_impact").with_arguments(
                    serde_json::json!({
                        "target": "MissingSharedThing"
                    })
                    .as_object()
                    .expect("missing impact args should be an object")
                    .clone(),
                ),
            )
            .await;
        assert!(tool_failed(missing_impact));

        let paged_search_result = client
            .call_tool(
                CallToolRequestParams::new("search").with_arguments(
                    serde_json::json!({
                        "query": "order",
                        "limit": 1
                    })
                    .as_object()
                    .expect("paged search args should be an object")
                    .clone(),
                ),
            )
            .await
            .expect("paged search tool call should succeed");
        let paged_search_payload = paged_search_result
            .structured_content
            .expect("paged search structured content should exist");
        let next_cursor = paged_search_payload
            .get("meta")
            .and_then(|meta| meta.get("next_cursor"))
            .and_then(Value::as_str)
            .expect("next_cursor should be present when results truncate")
            .to_owned();

        let paged_search_followup = client
            .call_tool(
                CallToolRequestParams::new("search").with_arguments(
                    serde_json::json!({
                        "query": "order",
                        "limit": 1,
                        "cursor": next_cursor
                    })
                    .as_object()
                    .expect("paged followup args should be an object")
                    .clone(),
                ),
            )
            .await
            .expect("paged search followup should succeed");
        let paged_followup_payload = paged_search_followup
            .structured_content
            .expect("paged search followup structured content should exist");
        let first_page_name = paged_search_payload
            .get("data")
            .and_then(|data| data.get("results"))
            .and_then(Value::as_array)
            .and_then(|items| items.first())
            .and_then(|item| item.get("symbol_name"))
            .and_then(Value::as_str)
            .expect("first paged result should be present");
        let second_page_name = paged_followup_payload
            .get("data")
            .and_then(|data| data.get("results"))
            .and_then(Value::as_array)
            .and_then(|items| items.first())
            .and_then(|item| item.get("symbol_name"))
            .and_then(Value::as_str)
            .expect("second paged result should be present");
        assert_ne!(first_page_name, second_page_name);

        let bad_cursor_result = client
            .call_tool(
                CallToolRequestParams::new("search").with_arguments(
                    serde_json::json!({
                        "query": "order",
                        "limit": 1,
                        "cursor": "badcursor"
                    })
                    .as_object()
                    .expect("bad cursor args should be an object")
                    .clone(),
                ),
            )
            .await;
        assert!(tool_failed(bad_cursor_result));

        let mismatched_cursor_result = client
            .call_tool(
                CallToolRequestParams::new("search").with_arguments(
                    serde_json::json!({
                        "query": "createOrder",
                        "limit": 1,
                        "cursor": next_cursor
                    })
                    .as_object()
                    .expect("mismatched cursor args should be an object")
                    .clone(),
                ),
            )
            .await;
        assert!(tool_failed(mismatched_cursor_result));

        let brief_result = client
            .call_tool(
                CallToolRequestParams::new("brief").with_arguments(
                    serde_json::json!({
                        "target": "createOrder"
                    })
                    .as_object()
                    .expect("brief args should be an object")
                    .clone(),
                ),
            )
            .await
            .expect("brief tool call should succeed");
        let brief_payload = brief_result
            .structured_content
            .expect("brief structured content should exist");
        assert_eq!(
            brief_payload
                .get("data")
                .and_then(|data| data.get("found"))
                .and_then(Value::as_bool),
            Some(true)
        );
        let brief_summary = brief_payload
            .get("data")
            .and_then(|data| data.get("summary"))
            .and_then(Value::as_str)
            .expect("brief summary should exist");
        assert!(brief_summary.len() < 500);

        let context_result = client
            .call_tool(
                CallToolRequestParams::new("context").with_arguments(
                    serde_json::json!({
                        "target": "createOrder",
                        "depth": 2,
                        "limit": 5
                    })
                    .as_object()
                    .expect("context args should be an object")
                    .clone(),
                ),
            )
            .await
            .expect("context tool call should succeed");
        let context_payload = context_result
            .structured_content
            .expect("context structured content should exist");
        assert_eq!(
            context_payload
                .get("data")
                .and_then(|data| data.get("symbol"))
                .and_then(|symbol| symbol.get("name"))
                .and_then(Value::as_str),
            Some("createOrder")
        );
        let context_hints = context_payload
            .get("meta")
            .and_then(|meta| meta.get("follow_up_hints"))
            .and_then(Value::as_array)
            .expect("context hints should be present");
        assert!(
            context_hints
                .iter()
                .any(|hint| hint.as_str() == Some("trace_impact"))
        );

        let batch_result = client
            .call_tool(
                CallToolRequestParams::new("batch_query").with_arguments(
                    serde_json::json!({
                        "ops": [
                            {
                                "tool": "search",
                                "arguments": {
                                    "query": "createOrder",
                                    "limit": 5
                                }
                            },
                            {
                                "tool": "context",
                                "arguments": {
                                    "target": "createOrder",
                                    "depth": 2
                                }
                            },
                            {
                                "tool": "cross_repo_deps",
                                "arguments": {
                                    "repo": "shared_contracts"
                                }
                            },
                            {
                                "tool": "get_overview",
                                "arguments": {
                                    "repo": "shared_contracts"
                                }
                            }
                        ]
                    })
                    .as_object()
                    .expect("batch args should be an object")
                    .clone(),
                ),
            )
            .await
            .expect("batch_query tool call should succeed");
        let batch_payload = batch_result
            .structured_content
            .expect("batch structured content should exist");
        let batch_results = batch_payload
            .get("data")
            .and_then(|data| data.get("results"))
            .and_then(Value::as_array)
            .expect("batch results should be present");
        assert_eq!(batch_results.len(), 4);
        assert_eq!(
            batch_results[0].get("tool").and_then(Value::as_str),
            Some("search")
        );
        assert_eq!(
            batch_results[1].get("tool").and_then(Value::as_str),
            Some("context")
        );
        assert_eq!(
            batch_results[2].get("tool").and_then(Value::as_str),
            Some("cross_repo_deps")
        );
        assert_eq!(
            batch_results[3].get("tool").and_then(Value::as_str),
            Some("get_overview")
        );
        assert!(
            batch_results
                .iter()
                .all(|item| item.get("ok").and_then(Value::as_bool) == Some(true))
        );

        let ownership_result = client
            .call_tool(
                CallToolRequestParams::new("who_owns").with_arguments(
                    serde_json::json!({
                        "repo": "shared_contracts",
                        "target": "createOrder"
                    })
                    .as_object()
                    .expect("ownership args should be an object")
                    .clone(),
                ),
            )
            .await
            .expect("who_owns tool call should succeed");
        let ownership_payload = ownership_result
            .structured_content
            .expect("ownership structured content should exist");
        assert_eq!(
            ownership_payload
                .get("data")
                .and_then(|data| data.get("file_path"))
                .and_then(Value::as_str),
            Some("src/types.ts")
        );
        assert_eq!(
            ownership_payload
                .get("data")
                .and_then(|data| data.get("ownership"))
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(2)
        );
        // top_owner_email is redacted at the MCP output boundary using a keyed
        // BLAKE3 hash.  The redact key is random per process (seeded from the
        // cursor key, which is sourced from `getrandom`), so the exact hex
        // prefix changes each run.  Assert only the "@redacted"
        // suffix and the correct prefix length (16 hex chars — first 8 bytes of
        // the BLAKE3 digest rendered as lowercase hex).
        let redacted_email = ownership_payload
            .get("data")
            .and_then(|data| data.get("top_owner_email"))
            .and_then(Value::as_str)
            .expect("top_owner_email must be present");
        assert!(
            redacted_email.ends_with("@redacted"),
            "top_owner_email must end with @redacted; got {redacted_email:?}"
        );
        let prefix = redacted_email.trim_end_matches("@redacted");
        assert_eq!(
            prefix.len(),
            16,
            "redacted prefix must be 16 hex chars; got {prefix:?}"
        );
        assert!(
            prefix.chars().all(|c| c.is_ascii_hexdigit()),
            "redacted prefix must be lowercase hex; got {prefix:?}"
        );

        let overview_result = client
            .call_tool(
                CallToolRequestParams::new("get_overview").with_arguments(
                    serde_json::json!({
                        "repo": "shared_contracts"
                    })
                    .as_object()
                    .expect("overview args should be an object")
                    .clone(),
                ),
            )
            .await
            .expect("get_overview tool call should succeed");
        let overview_payload = overview_result
            .structured_content
            .expect("overview structured content should exist");
        assert_eq!(
            overview_payload
                .get("data")
                .and_then(|data| data.get("repo"))
                .and_then(Value::as_str),
            Some("shared_contracts")
        );
        assert_eq!(
            overview_payload
                .get("data")
                .and_then(|data| data.get("git_history_available"))
                .and_then(Value::as_bool),
            Some(true)
        );

        // Coverage gap: exercise the `get_conventions` MCP tool end-to-end.
        // Detection runs against the seeded graph; we don't assert on specific
        // findings (those depend on the fixture's directory shape) but we do
        // assert the response shape so future regressions in the JSON schema
        // surface here rather than only via consumer-side breakage.
        let conventions_result = client
            .call_tool(
                CallToolRequestParams::new("get_conventions").with_arguments(
                    serde_json::json!({
                        "repo": "shared_contracts"
                    })
                    .as_object()
                    .expect("conventions args should be an object")
                    .clone(),
                ),
            )
            .await
            .expect("get_conventions tool call should succeed");
        let conventions_payload = conventions_result
            .structured_content
            .expect("conventions structured content should exist");
        assert!(
            conventions_payload
                .get("data")
                .and_then(|data| data.get("findings"))
                .and_then(Value::as_array)
                .is_some(),
            "get_conventions response should include data.findings array",
        );

        let partial_batch_result = client
            .call_tool(
                CallToolRequestParams::new("batch_query").with_arguments(
                    serde_json::json!({
                        "ops": [
                            {
                                "tool": "search",
                                "arguments": {
                                    "query": "createOrder"
                                }
                            },
                            {
                                "tool": "missing_tool",
                                "arguments": {}
                            },
                            {
                                "tool": "cross_repo_deps",
                                "arguments": {
                                    "repo": "shared_contracts"
                                }
                            }
                        ]
                    })
                    .as_object()
                    .expect("partial batch args should be an object")
                    .clone(),
                ),
            )
            .await
            .expect("partial batch query tool call should succeed");
        let partial_batch_payload = partial_batch_result
            .structured_content
            .expect("partial batch structured content should exist");
        let partial_batch_results = partial_batch_payload
            .get("data")
            .and_then(|data| data.get("results"))
            .and_then(Value::as_array)
            .expect("partial batch results should be present");
        assert_eq!(partial_batch_results.len(), 3);
        assert_eq!(
            partial_batch_results[1].get("ok").and_then(Value::as_bool),
            Some(false)
        );

        let nested_batch_result = client
            .call_tool(
                CallToolRequestParams::new("batch_query").with_arguments(
                    serde_json::json!({
                        "ops": [
                            {
                                "tool": "batch_query",
                                "arguments": {
                                    "ops": []
                                }
                            }
                        ]
                    })
                    .as_object()
                    .expect("nested batch args should be an object")
                    .clone(),
                ),
            )
            .await
            .expect("nested batch query tool call should succeed");
        let nested_batch_payload = nested_batch_result
            .structured_content
            .expect("nested batch structured content should exist");
        let nested_batch_results = nested_batch_payload
            .get("data")
            .and_then(|data| data.get("results"))
            .and_then(Value::as_array)
            .expect("nested batch results should be present");
        assert_eq!(
            nested_batch_results[0].get("ok").and_then(Value::as_bool),
            Some(false)
        );

        let oversized_batch = client
            .call_tool(
                CallToolRequestParams::new("batch_query").with_arguments(
                    serde_json::json!({
                        "ops": [
                            {"tool": "search", "arguments": {"query": "createOrder"}},
                            {"tool": "search", "arguments": {"query": "createOrder"}},
                            {"tool": "search", "arguments": {"query": "createOrder"}},
                            {"tool": "search", "arguments": {"query": "createOrder"}},
                            {"tool": "search", "arguments": {"query": "createOrder"}},
                            {"tool": "search", "arguments": {"query": "createOrder"}},
                            {"tool": "search", "arguments": {"query": "createOrder"}},
                            {"tool": "search", "arguments": {"query": "createOrder"}},
                            {"tool": "search", "arguments": {"query": "createOrder"}},
                            {"tool": "search", "arguments": {"query": "createOrder"}},
                            {"tool": "search", "arguments": {"query": "createOrder"}}
                        ]
                    })
                    .as_object()
                    .expect("oversized batch args should be an object")
                    .clone(),
                ),
            )
            .await;
        assert!(tool_failed(oversized_batch));

        let invalid_params = client
            .call_tool(
                CallToolRequestParams::new("search").with_arguments(
                    serde_json::json!({
                        "query": "createOrder",
                        "limit": "bad"
                    })
                    .as_object()
                    .expect("invalid args should still be encoded as an object")
                    .clone(),
                ),
            )
            .await;
        assert!(tool_failed(invalid_params));

        let unknown_tool = client
            .call_tool(CallToolRequestParams::new("missing_tool"))
            .await;
        assert!(unknown_tool.is_err());

        client.cancel().await.expect("client should cancel");
        server_handle.await.expect("server task should join");
    }

    #[tokio::test]
    async fn server_handles_empty_index_workspace() {
        let temp = TempDir::new("empty-server");
        let graph_path = temp.path().join("graph.redb");
        let registry_path = temp.path().join("registry.json");
        let search_path = temp.path().join("search");

        let graph = GraphStoreDb::open(&graph_path).expect("graph store should open");
        drop(graph);
        let search = TantivySearchStore::open(&search_path).expect("search store should open");
        drop(search);
        let registry = RegistryStore::open(&registry_path).expect("registry should open");
        drop(registry);

        let ctx = McpContext::open(McpServerConfig::new(registry_path, graph_path))
            .expect("context should open");
        let server = GatherStepMcpServer::new(ctx);
        let (server_transport, client_transport) = tokio::io::duplex(16 * 1024);

        let server_handle = tokio::spawn(async move {
            let running = server
                .serve(server_transport)
                .await
                .expect("server should start");
            running.waiting().await.expect("server should wait cleanly");
        });

        let client = ().serve(client_transport).await.expect("client should start");

        let schema = client
            .call_tool(CallToolRequestParams::new("get_graph_schema"))
            .await
            .expect("schema tool should succeed")
            .structured_content
            .expect("schema structured content should exist");
        assert_eq!(
            schema
                .get("data")
                .and_then(|data| data.get("node_kinds"))
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(0)
        );

        let repos = client
            .call_tool(CallToolRequestParams::new("list_repos"))
            .await
            .expect("list_repos should succeed")
            .structured_content
            .expect("list_repos structured content should exist");
        assert_eq!(
            repos
                .get("data")
                .and_then(|data| data.get("total"))
                .and_then(Value::as_u64),
            Some(0)
        );

        let search = client
            .call_tool(
                CallToolRequestParams::new("search").with_arguments(
                    serde_json::json!({
                        "query": "anything"
                    })
                    .as_object()
                    .expect("search args should be an object")
                    .clone(),
                ),
            )
            .await
            .expect("search should succeed")
            .structured_content
            .expect("search structured content should exist");
        assert_eq!(
            search
                .get("data")
                .and_then(|data| data.get("results"))
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(0)
        );

        client.cancel().await.expect("client should cancel");
        server_handle.await.expect("server task should join");
    }

    #[tokio::test]
    async fn source_indexed_repo_is_queryable_over_mcp() {
        let storage_root = TempDir::new("indexed-storage");
        let repo_root = TempDir::new("indexed-repo");
        let registry_path = storage_root.path().join("registry.json");

        fs::create_dir_all(repo_root.path().join("src")).expect("src dir should exist");
        fs::write(
            repo_root.path().join("package.json"),
            r#"{
  "name": "backend-standard",
  "dependencies": {
    "lodash": "^4.17.21",
    "axios": "^1.11.0"
  }
}"#,
        )
        .expect("package manifest should write");
        fs::write(
            repo_root.path().join("src/lib.ts"),
            r#"
import { debounce } from "lodash";

export function normalizeOrder(): string {
  return debounce(() => "ok", 10)();
}

export function createOrder(): string {
  return normalizeOrder();
}
"#,
        )
        .expect("source file should write");

        let indexer = RepoIndexer::open(storage_root.path(), IndexingOptions::default())
            .expect("indexer should open");
        let stats = indexer
            .index_repo("backend_standard", repo_root.path(), None)
            .expect("repo should index");
        let symbol_id = indexer
            .storage()
            .graph()
            .nodes_by_repo("backend_standard")
            .expect("repo nodes should load")
            .into_iter()
            .find(|node| node.name == "createOrder" && node.kind == NodeKind::Function)
            .map(|node| encode_node_id(node.id))
            .expect("createOrder should be indexed into the graph");
        drop(indexer);

        let mut registry = RegistryStore::open(&registry_path).expect("registry should open");
        registry
            .register_repo(
                "backend_standard",
                repo_root.path(),
                Some(DepthLevel::Level1),
            )
            .expect("repo registration should succeed");
        registry
            .update_repo_metadata(
                "backend_standard",
                RepoIndexMetadata {
                    last_indexed_at: Some("2026-04-15T00:00:00Z".to_owned()),
                    file_count: u64::try_from(stats.files_parsed).expect("file count should fit"),
                    symbol_count: u64::try_from(stats.nodes_created)
                        .expect("symbol count should fit"),
                    frameworks: Vec::new(),
                    depth_level: DepthLevel::Level1,
                },
            )
            .expect("metadata update should succeed");

        let ctx = McpContext::open(McpServerConfig::new(
            registry_path,
            storage_root.path().join("graph.redb"),
        ))
        .expect("context should open");
        let server = GatherStepMcpServer::new(ctx);
        let (server_transport, client_transport) = tokio::io::duplex(16 * 1024);

        let server_handle = tokio::spawn(async move {
            let running = server
                .serve(server_transport)
                .await
                .expect("server should start");
            running.waiting().await.expect("server should wait cleanly");
        });

        let client = ().serve(client_transport).await.expect("client should start");
        let symbol_payload = client
            .call_tool(
                CallToolRequestParams::new("get_symbol").with_arguments(
                    serde_json::json!({
                        "symbol_id": symbol_id.clone()
                    })
                    .as_object()
                    .expect("symbol args should be an object")
                    .clone(),
                ),
            )
            .await
            .expect("get_symbol should succeed")
            .structured_content
            .expect("symbol structured content should exist");
        assert_eq!(
            symbol_payload
                .get("data")
                .and_then(|data| data.get("name"))
                .and_then(Value::as_str),
            Some("createOrder")
        );

        let callees_payload = client
            .call_tool(
                CallToolRequestParams::new("get_callees").with_arguments(
                    serde_json::json!({
                        "symbol_id": symbol_id,
                        "depth": 1
                    })
                    .as_object()
                    .expect("callee args should be an object")
                    .clone(),
                ),
            )
            .await
            .expect("get_callees should succeed")
            .structured_content
            .expect("callee structured content should exist");
        let callees = callees_payload
            .get("data")
            .and_then(|data| data.get("traversal"))
            .and_then(Value::as_array)
            .expect("callee traversal should be present");
        assert!(
            callees.iter().any(|entry| {
                entry.get("symbol_name").and_then(Value::as_str) == Some("normalizeOrder")
            }),
            "expected normalizeOrder in callees: {callees:?}"
        );

        let dead_code_payload = client
            .call_tool(
                CallToolRequestParams::new("get_dead_code").with_arguments(
                    serde_json::json!({
                        "repo": "backend_standard",
                        "min_confidence": "medium"
                    })
                    .as_object()
                    .expect("dead-code args should be an object")
                    .clone(),
                ),
            )
            .await
            .expect("get_dead_code should succeed")
            .structured_content
            .expect("dead-code structured content should exist");
        let findings = dead_code_payload
            .get("data")
            .and_then(|data| data.get("findings"))
            .and_then(Value::as_array)
            .expect("dead-code findings should be present");
        assert!(findings.iter().any(|finding| {
            finding.get("detector_basis").and_then(Value::as_str) == Some("zombie_dependency")
                && finding.get("package_name").and_then(Value::as_str) == Some("axios")
        }));

        client.cancel().await.expect("client should cancel");
        server_handle.await.expect("server task should join");
    }

    #[tokio::test]
    async fn trace_impact_should_follow_multi_hop_virtual_chains() {
        let temp = TempDir::new("trace-impact-multi-hop");
        let graph_path = temp.path().join("graph.redb");
        let registry_path = temp.path().join("registry.json");
        let search_path = temp.path().join("search");

        let graph = GraphStoreDb::open(&graph_path).expect("graph store should open");
        let search = TantivySearchStore::open(&search_path).expect("search store should open");
        drop(search);

        let producer_file = sample_node(
            "backend_standard",
            "src/producer.ts",
            NodeKind::File,
            "src/producer.ts",
            0,
        );
        let bridge_file = sample_node(
            "bridge_standard",
            "src/bridge.ts",
            NodeKind::File,
            "src/bridge.ts",
            0,
        );
        let consumer_file = sample_node(
            "frontend_standard",
            "src/api.ts",
            NodeKind::File,
            "src/api.ts",
            0,
        );
        let producer = sample_node(
            "backend_standard",
            "src/producer.ts",
            NodeKind::Function,
            "emitOrder",
            0,
        );
        let bridge = sample_node(
            "bridge_standard",
            "src/bridge.ts",
            NodeKind::Function,
            "forwardOrder",
            0,
        );
        let consumer = sample_node(
            "frontend_standard",
            "src/api.ts",
            NodeKind::Function,
            "callOrderApi",
            0,
        );
        let topic = virtual_node(
            NodeKind::Topic,
            "backend_standard",
            "src/events.ts",
            "order.created",
            topic_qn("kafka", "order.created"),
        );
        let route = virtual_node(
            NodeKind::Route,
            "bridge_standard",
            "src/controller.ts",
            "GET /orders",
            route_qn("GET", "/orders"),
        );

        graph
            .bulk_insert(
                &[
                    producer_file.clone(),
                    bridge_file.clone(),
                    consumer_file.clone(),
                    producer.clone(),
                    bridge.clone(),
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
                        source: bridge.id,
                        target: topic.id,
                        kind: EdgeKind::Consumes,
                        metadata: EdgeMetadata::default(),
                        owner_file: bridge_file.id,
                        is_cross_file: true,
                    },
                    EdgeData {
                        source: bridge.id,
                        target: route.id,
                        kind: EdgeKind::Serves,
                        metadata: EdgeMetadata::default(),
                        owner_file: bridge_file.id,
                        is_cross_file: true,
                    },
                    EdgeData {
                        source: consumer.id,
                        target: route.id,
                        kind: EdgeKind::Consumes,
                        metadata: EdgeMetadata::default(),
                        owner_file: consumer_file.id,
                        is_cross_file: true,
                    },
                ],
            )
            .expect("graph write should succeed");
        drop(graph);

        let mut registry = RegistryStore::open(&registry_path).expect("registry should open");
        for repo in ["backend_standard", "bridge_standard", "frontend_standard"] {
            registry
                .register_repo(repo, temp.path().join(repo), Some(DepthLevel::Full))
                .expect("repo registration should succeed");
        }
        drop(registry);

        let ctx = McpContext::open(McpServerConfig::new(registry_path, graph_path))
            .expect("context should open");
        let server = GatherStepMcpServer::new(ctx);
        let (server_transport, client_transport) = tokio::io::duplex(16 * 1024);

        let server_handle = tokio::spawn(async move {
            let running = server
                .serve(server_transport)
                .await
                .expect("server should start");
            running.waiting().await.expect("server should wait cleanly");
        });

        let client = ().serve(client_transport).await.expect("client should start");
        let result = client
            .call_tool(
                CallToolRequestParams::new("trace_impact").with_arguments(
                    serde_json::json!({
                        "target": topic_qn("kafka", "order.created"),
                        "depth": 4
                    })
                    .as_object()
                    .expect("trace_impact args should be an object")
                    .clone(),
                ),
            )
            .await
            .expect("trace_impact tool call should succeed")
            .structured_content
            .expect("trace_impact structured content should exist");

        let impacted = result
            .get("data")
            .and_then(|data| data.get("impacted_repos"))
            .and_then(Value::as_array)
            .expect("impacted repos should be present");
        assert!(
            impacted.iter().any(|repo| {
                repo.get("repo").and_then(Value::as_str) == Some("frontend_standard")
            }),
            "trace_impact should reach downstream repos through intermediate virtual nodes"
        );

        client.cancel().await.expect("client should cancel");
        server_handle.await.expect("server task should join");
    }

    #[tokio::test]
    async fn crud_trace_is_queryable_over_mcp_and_batch_query() {
        let temp = TempDir::new("crud-trace");
        let graph_path = temp.path().join("graph.redb");
        let registry_path = temp.path().join("registry.json");
        let graph = GraphStoreDb::open(&graph_path).expect("graph store should open");

        let backend_file = sample_node(
            "backend_standard",
            "src/controller.ts",
            NodeKind::File,
            "src/controller.ts",
            0,
        );
        let service_file = sample_node(
            "backend_standard",
            "src/service.ts",
            NodeKind::File,
            "src/service.ts",
            0,
        );
        let repo_file = sample_node(
            "backend_standard",
            "src/repository.ts",
            NodeKind::File,
            "src/repository.ts",
            0,
        );
        let frontend_file = sample_node(
            "frontend_standard",
            "src/api.ts",
            NodeKind::File,
            "src/api.ts",
            0,
        );
        let handler = sample_node(
            "backend_standard",
            "src/controller.ts",
            NodeKind::Function,
            "createOrder",
            0,
        );
        let service = sample_node(
            "backend_standard",
            "src/service.ts",
            NodeKind::Function,
            "persistOrder",
            0,
        );
        let repository = sample_node(
            "backend_standard",
            "src/repository.ts",
            NodeKind::Function,
            "storeOrder",
            0,
        );
        let caller = sample_node(
            "frontend_standard",
            "src/api.ts",
            NodeKind::Function,
            "createOrder",
            0,
        );
        let entity = virtual_node(
            NodeKind::Entity,
            "backend_standard",
            "src/repository.ts",
            "Order",
            "__entity__Order",
        );
        let route = virtual_node(
            NodeKind::Route,
            "backend_standard",
            "src/controller.ts",
            "POST /orders",
            route_qn("POST", "/orders"),
        );
        graph
            .bulk_insert(
                &[
                    backend_file.clone(),
                    service_file.clone(),
                    repo_file.clone(),
                    frontend_file.clone(),
                    handler.clone(),
                    service.clone(),
                    repository.clone(),
                    caller.clone(),
                    entity.clone(),
                    route.clone(),
                ],
                &[
                    EdgeData {
                        source: handler.id,
                        target: route.id,
                        kind: EdgeKind::Serves,
                        metadata: EdgeMetadata {
                            confidence: Some(980),
                            ..EdgeMetadata::default()
                        },
                        owner_file: backend_file.id,
                        is_cross_file: false,
                    },
                    EdgeData {
                        source: caller.id,
                        target: route.id,
                        kind: EdgeKind::Consumes,
                        metadata: EdgeMetadata {
                            confidence: Some(920),
                            resolver: Some(ResolverStrategy::FrontendConstant.as_str().to_owned()),
                            ..EdgeMetadata::default()
                        },
                        owner_file: frontend_file.id,
                        is_cross_file: true,
                    },
                    EdgeData {
                        source: handler.id,
                        target: service.id,
                        kind: EdgeKind::Calls,
                        metadata: EdgeMetadata {
                            confidence: Some(900),
                            ..EdgeMetadata::default()
                        },
                        owner_file: backend_file.id,
                        is_cross_file: true,
                    },
                    EdgeData {
                        source: service.id,
                        target: repository.id,
                        kind: EdgeKind::Calls,
                        metadata: EdgeMetadata {
                            confidence: Some(880),
                            ..EdgeMetadata::default()
                        },
                        owner_file: service_file.id,
                        is_cross_file: true,
                    },
                    EdgeData {
                        source: repository.id,
                        target: entity.id,
                        kind: EdgeKind::References,
                        metadata: EdgeMetadata {
                            confidence: Some(860),
                            ..EdgeMetadata::default()
                        },
                        owner_file: repo_file.id,
                        is_cross_file: false,
                    },
                ],
            )
            .expect("graph write should succeed");
        drop(graph);

        let mut registry = RegistryStore::open(&registry_path).expect("registry should open");
        registry
            .register_repo(
                "backend_standard",
                temp.path().join("repos/backend_standard"),
                Some(DepthLevel::Full),
            )
            .expect("backend repo should register");
        registry
            .register_repo(
                "frontend_standard",
                temp.path().join("repos/frontend_standard"),
                Some(DepthLevel::Full),
            )
            .expect("frontend repo should register");

        let ctx = McpContext::open(McpServerConfig::new(registry_path, graph_path))
            .expect("context should open");
        let server = GatherStepMcpServer::new(ctx);
        let (server_transport, client_transport) = tokio::io::duplex(16 * 1024);

        let server_handle = tokio::spawn(async move {
            let running = server
                .serve(server_transport)
                .await
                .expect("server should start");
            running.waiting().await.expect("server should wait cleanly");
        });

        let client = ().serve(client_transport).await.expect("client should start");
        let result = client
            .call_tool(
                CallToolRequestParams::new("crud_trace").with_arguments(
                    serde_json::json!({
                        "method": "POST",
                        "path": "/orders"
                    })
                    .as_object()
                    .expect("crud args should be object")
                    .clone(),
                ),
            )
            .await
            .expect("crud_trace tool call should succeed");
        let payload = result
            .structured_content
            .expect("crud_trace structured content should exist");
        assert!(
            payload["data"]["callers"]
                .as_array()
                .expect("callers should be array")
                .iter()
                .any(|item| item["repo"] == "frontend_standard")
        );
        assert!(
            payload["data"]["callers"]
                .as_array()
                .expect("callers should be array")
                .iter()
                .any(|item| item["evidence_kind"] == "imported_constant")
        );
        assert!(
            payload["data"]["callers"]
                .as_array()
                .expect("callers should be array")
                .iter()
                .any(|item| item["resolver"] == "frontend_constant")
        );
        assert!(
            payload["data"]["entities"]
                .as_array()
                .expect("entities should be array")
                .iter()
                .any(|item| item["symbol_name"] == "Order")
        );

        let batch_result = client
            .call_tool(
                CallToolRequestParams::new("batch_query").with_arguments(
                    serde_json::json!({
                        "ops": [
                            {
                                "tool": "crud_trace",
                                "arguments": {
                                    "method": "POST",
                                    "path": "/orders"
                                }
                            }
                        ]
                    })
                    .as_object()
                    .expect("batch args should be object")
                    .clone(),
                ),
            )
            .await
            .expect("batch_query call should succeed");
        let batch_payload = batch_result
            .structured_content
            .expect("batch_query structured content should exist");
        assert_eq!(
            batch_payload["data"]["results"][0]["tool"].as_str(),
            Some("crud_trace")
        );
        assert_eq!(
            batch_payload["data"]["results"][0]["ok"].as_bool(),
            Some(true)
        );

        let missing_result = client
            .call_tool(
                CallToolRequestParams::new("crud_trace").with_arguments(
                    serde_json::json!({
                        "method": "DELETE",
                        "path": "/missing"
                    })
                    .as_object()
                    .expect("missing crud args should be object")
                    .clone(),
                ),
            )
            .await
            .expect("missing crud_trace call should succeed");
        let missing_payload = missing_result
            .structured_content
            .expect("missing crud_trace structured content should exist");
        assert_eq!(missing_payload["data"]["target_id"], Value::Null);

        client.cancel().await.expect("client should stop");
        server_handle.await.expect("server task should join");
    }
}
