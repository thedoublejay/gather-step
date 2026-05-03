//! Secret-value smoke tests for generated Gather Step surfaces.

#![forbid(unsafe_code)]

use std::fs;

use gather_step_mcp::{
    config::{McpContext, McpServerConfig},
    tools::{
        context_pack::{ContextPackRequest, context_pack_tool},
        deployment_topology::{
            EnvVarTopologyRequest, RepoTopologyRequest, ServiceTopologyRequest,
            deployed_but_no_code_tool, env_var_consumers_tool, service_env_tool, shared_infra_tool,
            undeployed_services_tool, where_deployed_tool,
        },
        search::{SearchRequest, search_symbols},
    },
};
use gather_step_storage::{
    DeploymentIndexingOptions, GraphStore, IndexingOptions, RepoIndexer, SearchStore,
};

const SOURCE_FAKE_TOKEN: &str = "gs-source-fake-token-123";
const ENV_FILE_FAKE_TOKEN: &str = "gs-env-file-fake-token-456";
const DEPLOYMENT_FAKE_TOKEN: &str = "gs-deployment-fake-token-789";

#[test]
fn generated_surfaces_do_not_include_fake_secret_values() {
    let temp = tempfile::tempdir().expect("temp dir");
    let workspace_root = temp.path();
    let repo_root = workspace_root.join("repos/backend");
    let storage_root = workspace_root.join(".gather-step/storage");
    fs::create_dir_all(repo_root.join("src")).expect("src dir");
    fs::create_dir_all(repo_root.join("deploy")).expect("deploy dir");
    fs::write(
        repo_root.join("package.json"),
        r#"{ "name": "backend", "dependencies": { "@nestjs/core": "^11.0.0" } }"#,
    )
    .expect("package fixture");
    fs::write(
        repo_root.join("src/config.ts"),
        format!(
            r#"
export function loadConfig() {{
  const sourceFixture = "{SOURCE_FAKE_TOKEN}";
  return {{
    sourceFixture,
    databaseUrl: process.env.DATABASE_URL,
    apiToken: process.env.API_TOKEN,
  }};
}}
"#
        ),
    )
    .expect("source fixture");
    fs::write(
        repo_root.join(".env.production"),
        format!("DATABASE_URL=postgres://{ENV_FILE_FAKE_TOKEN}@localhost/app\n"),
    )
    .expect("env fixture");
    fs::write(
        repo_root.join("compose.yaml"),
        format!(
            r#"
services:
  api:
    image: example/api
    env_file:
      - .env.production
    environment:
      API_TOKEN: {DEPLOYMENT_FAKE_TOKEN}
      DATABASE_URL: postgres://{DEPLOYMENT_FAKE_TOKEN}@postgres/app
"#
        ),
    )
    .expect("compose fixture");
    fs::write(
        repo_root.join("deploy/api.yaml"),
        format!(
            r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
spec:
  template:
    spec:
      containers:
        - name: api
          env:
            - name: K8S_API_TOKEN
              value: {DEPLOYMENT_FAKE_TOKEN}
"#
        ),
    )
    .expect("kubernetes fixture");

    let indexer = RepoIndexer::open(
        &storage_root,
        IndexingOptions {
            deployment: DeploymentIndexingOptions {
                env_files: vec![".env.production".to_owned()],
                gitops_roots: vec!["deploy".to_owned()],
                ..DeploymentIndexingOptions::default()
            },
            ..IndexingOptions::default()
        },
    )
    .expect("indexer");
    indexer
        .index_repo("backend", &repo_root, None)
        .expect("indexing should succeed");

    let graph_nodes = indexer
        .storage()
        .graph()
        .nodes_by_repo("backend")
        .expect("graph nodes should load");
    assert_no_fake_tokens(
        "graph JSON",
        &serde_json::to_string(&graph_nodes).expect("graph json"),
    );

    for token in [
        SOURCE_FAKE_TOKEN,
        ENV_FILE_FAKE_TOKEN,
        DEPLOYMENT_FAKE_TOKEN,
    ] {
        let hits = indexer
            .storage()
            .search()
            .search(token, 10)
            .expect("search should succeed");
        assert!(
            hits.is_empty(),
            "storage search must not return raw fake token `{token}`"
        );
        assert_no_fake_tokens("storage search debug", &format!("{hits:?}"));
    }
    drop(indexer);

    let ctx = McpContext::open(McpServerConfig::new(
        workspace_root.join(".gather-step/registry.json"),
        storage_root.join("graph.redb"),
    ))
    .expect("mcp context");

    let mcp_search = search_symbols(
        &ctx,
        SearchRequest {
            budget_bytes: Some(20_000),
            cursor: None,
            kind: None,
            language: None,
            limit: Some(10),
            query: SOURCE_FAKE_TOKEN.to_owned(),
            repo: None,
        },
    )
    .expect("mcp search should succeed");
    assert!(mcp_search.data.results.is_empty());
    assert_no_fake_tokens(
        "MCP search response",
        &serde_json::to_string(&mcp_search).expect("mcp search json"),
    );

    let context_pack = context_pack_tool(
        &ctx,
        ContextPackRequest {
            budget_bytes: Some(20_000),
            depth: Some(2),
            limit: Some(10),
            repo: None,
            mode: "planning".to_owned(),
            target: "loadConfig".to_owned(),
        },
    )
    .expect("context pack should succeed");
    assert!(
        context_pack.data.found,
        "context pack should find loadConfig"
    );
    assert_no_fake_tokens(
        "context pack response",
        &serde_json::to_string(&context_pack).expect("context pack json"),
    );
    let cached_packs = ctx.metadata().list_context_packs().expect("cached packs");
    assert!(!cached_packs.is_empty(), "context pack should be cached");
    for record in cached_packs {
        let cached_response = String::from_utf8(record.response).expect("cached response utf8");
        assert_no_fake_tokens("cached context pack", &cached_response);
    }

    let deployment_responses = [
        serde_json::to_value(
            where_deployed_tool(
                &ctx,
                ServiceTopologyRequest {
                    service: "api".to_owned(),
                    repo: None,
                    limit: 20,
                },
            )
            .expect("where deployed should succeed"),
        )
        .expect("where deployed json"),
        serde_json::to_value(
            service_env_tool(
                &ctx,
                ServiceTopologyRequest {
                    service: "api".to_owned(),
                    repo: None,
                    limit: 20,
                },
            )
            .expect("service env should succeed"),
        )
        .expect("service env json"),
        serde_json::to_value(
            env_var_consumers_tool(
                &ctx,
                EnvVarTopologyRequest {
                    env_var: "DATABASE_URL".to_owned(),
                    repo: None,
                    limit: 20,
                },
            )
            .expect("env var consumers should succeed"),
        )
        .expect("env var consumers json"),
        serde_json::to_value(
            shared_infra_tool(
                &ctx,
                &RepoTopologyRequest {
                    repo: None,
                    limit: 20,
                },
            )
            .expect("shared infra should succeed"),
        )
        .expect("shared infra json"),
        serde_json::to_value(
            undeployed_services_tool(
                &ctx,
                &RepoTopologyRequest {
                    repo: None,
                    limit: 20,
                },
            )
            .expect("undeployed services should succeed"),
        )
        .expect("undeployed services json"),
        serde_json::to_value(
            deployed_but_no_code_tool(
                &ctx,
                &RepoTopologyRequest {
                    repo: None,
                    limit: 20,
                },
            )
            .expect("deployed but no code should succeed"),
        )
        .expect("deployed but no code json"),
    ];
    assert_no_fake_tokens(
        "deployment MCP responses",
        &serde_json::to_string(&deployment_responses).expect("deployment json"),
    );
}

fn assert_no_fake_tokens(label: &str, rendered: &str) {
    for token in [
        SOURCE_FAKE_TOKEN,
        ENV_FILE_FAKE_TOKEN,
        DEPLOYMENT_FAKE_TOKEN,
    ] {
        assert!(
            !rendered.contains(token),
            "{label} leaked fake token `{token}`"
        );
    }
}
