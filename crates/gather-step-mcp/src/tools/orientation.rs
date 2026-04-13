use gather_step_analysis::GraphQuery;
use gather_step_core::DepthLevel;
use rmcp::schemars;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    budget::response_schema_version, config::McpContext, error::McpServerError,
    output::redact::relativize_to_workspace,
};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ResponseMeta {
    #[serde(default = "response_schema_version")]
    pub response_schema_version: u8,
    pub recommended_next_tools: Vec<String>,
    pub summary_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct GraphSchemaData {
    pub edge_kinds: Vec<String>,
    pub node_kinds: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct GraphSchemaResponse {
    pub data: GraphSchemaData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<ResponseMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct RepoSummary {
    pub depth_level: String,
    pub file_count: u64,
    pub frameworks: Vec<String>,
    pub last_indexed_at: Option<String>,
    pub path: String,
    pub repo: String,
    pub symbol_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ListReposData {
    pub repos: Vec<RepoSummary>,
    pub total: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ListReposResponse {
    pub data: ListReposData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<ResponseMeta>,
}

pub fn get_graph_schema(ctx: &McpContext) -> Result<GraphSchemaResponse, McpServerError> {
    let query = GraphQuery::new(ctx.graph());

    let node_kinds = query
        .count_by_kind()?
        .into_iter()
        .filter(|(_, count)| *count > 0)
        .map(|(kind, count)| format!("{kind:?}[{count}]"))
        .collect();
    let edge_kinds = query
        .count_edges_by_kind()?
        .into_iter()
        .map(|(kind, count)| format!("{kind:?}[{count}]"))
        .collect();

    Ok(GraphSchemaResponse {
        data: GraphSchemaData {
            edge_kinds,
            node_kinds,
        },
        meta: Some(ResponseMeta {
            response_schema_version: response_schema_version(),
            recommended_next_tools: vec!["list_repos".to_owned()],
            summary_only: true,
        }),
    })
}

pub fn list_repos(ctx: &McpContext) -> Result<ListReposResponse, McpServerError> {
    let registry = ctx.registry_snapshot()?;
    let workspace_root = ctx.config.workspace_root();
    let repos = registry
        .repos
        .iter()
        .map(|(repo, registered)| RepoSummary {
            depth_level: depth_level_label(registered.depth_level).to_owned(),
            file_count: registered.file_count,
            frameworks: registered.frameworks.clone(),
            last_indexed_at: registered.last_indexed_at.clone(),
            path: relativize_to_workspace(&registered.path, &workspace_root),
            repo: repo.clone(),
            symbol_count: registered.symbol_count,
        })
        .collect::<Vec<_>>();

    Ok(ListReposResponse {
        data: ListReposData {
            total: repos.len(),
            repos,
        },
        meta: Some(ResponseMeta {
            response_schema_version: response_schema_version(),
            recommended_next_tools: vec!["get_graph_schema_summary".to_owned()],
            summary_only: true,
        }),
    })
}

fn depth_level_label(level: DepthLevel) -> &'static str {
    match level {
        DepthLevel::Level1 => "level1",
        DepthLevel::Level2 => "level2",
        DepthLevel::Level3 => "level3",
        DepthLevel::Full => "full",
        _ => "unknown",
    }
}
