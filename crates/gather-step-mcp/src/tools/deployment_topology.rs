use gather_step_analysis::{
    DeploymentTopologyEdge as AnalysisEdge, DeploymentTopologyNode as AnalysisNode,
    DeploymentTopologyQuery, DeploymentTopologyReport, deployment_topology,
};
use rmcp::schemars;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{McpContext, McpServerError, config::validate_input_length};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ServiceTopologyRequest {
    pub service: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct EnvVarTopologyRequest {
    pub env_var: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct RepoTopologyRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct DeploymentTopologyResponse {
    pub data: DeploymentTopologyData,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct DeploymentTopologyData {
    pub query_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    pub deployments: Vec<DeploymentTopologyNodeItem>,
    pub services: Vec<DeploymentTopologyNodeItem>,
    pub env_vars: Vec<DeploymentTopologyNodeItem>,
    pub shared_infra: Vec<DeploymentTopologyNodeItem>,
    pub workflow_jobs: Vec<DeploymentTopologyNodeItem>,
    pub edges: Vec<DeploymentTopologyEdgeItem>,
    pub missing_evidence: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct DeploymentTopologyNodeItem {
    pub repo: String,
    pub kind: String,
    pub name: String,
    pub file_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qualified_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct DeploymentTopologyEdgeItem {
    pub source: DeploymentTopologyNodeItem,
    pub target: DeploymentTopologyNodeItem,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<u16>,
}

fn default_limit() -> usize {
    20
}

pub fn where_deployed_tool(
    ctx: &McpContext,
    request: ServiceTopologyRequest,
) -> Result<DeploymentTopologyResponse, McpServerError> {
    validate_service_request(&request)?;
    run_query(
        ctx,
        DeploymentTopologyQuery::WhereDeployed {
            service: request.service,
        },
        request.repo,
        request.limit,
    )
}

pub fn service_env_tool(
    ctx: &McpContext,
    request: ServiceTopologyRequest,
) -> Result<DeploymentTopologyResponse, McpServerError> {
    validate_service_request(&request)?;
    run_query(
        ctx,
        DeploymentTopologyQuery::ServiceEnv {
            service: request.service,
        },
        request.repo,
        request.limit,
    )
}

pub fn env_var_consumers_tool(
    ctx: &McpContext,
    request: EnvVarTopologyRequest,
) -> Result<DeploymentTopologyResponse, McpServerError> {
    if request.env_var.trim().is_empty() {
        return Err(McpServerError::InvalidInput(
            "env_var must not be empty".to_owned(),
        ));
    }
    validate_input_length("env_var", &request.env_var)?;
    validate_repo_and_limit(request.repo.as_deref(), request.limit)?;
    run_query(
        ctx,
        DeploymentTopologyQuery::EnvVarConsumers {
            env_var: request.env_var,
        },
        request.repo,
        request.limit,
    )
}

pub fn undeployed_services_tool(
    ctx: &McpContext,
    request: RepoTopologyRequest,
) -> Result<DeploymentTopologyResponse, McpServerError> {
    validate_repo_and_limit(request.repo.as_deref(), request.limit)?;
    run_query(
        ctx,
        DeploymentTopologyQuery::UndeployedServices,
        request.repo,
        request.limit,
    )
}

pub fn deployed_but_no_code_tool(
    ctx: &McpContext,
    request: RepoTopologyRequest,
) -> Result<DeploymentTopologyResponse, McpServerError> {
    validate_repo_and_limit(request.repo.as_deref(), request.limit)?;
    run_query(
        ctx,
        DeploymentTopologyQuery::DeployedButNoCode,
        request.repo,
        request.limit,
    )
}

pub fn shared_infra_tool(
    ctx: &McpContext,
    request: RepoTopologyRequest,
) -> Result<DeploymentTopologyResponse, McpServerError> {
    validate_repo_and_limit(request.repo.as_deref(), request.limit)?;
    run_query(
        ctx,
        DeploymentTopologyQuery::SharedInfra,
        request.repo,
        request.limit,
    )
}

fn validate_service_request(request: &ServiceTopologyRequest) -> Result<(), McpServerError> {
    if request.service.trim().is_empty() {
        return Err(McpServerError::InvalidInput(
            "service must not be empty".to_owned(),
        ));
    }
    validate_input_length("service", &request.service)?;
    validate_repo_and_limit(request.repo.as_deref(), request.limit)
}

fn validate_repo_and_limit(repo: Option<&str>, limit: usize) -> Result<(), McpServerError> {
    if let Some(repo) = repo {
        validate_input_length("repo", repo)?;
    }
    if (1..=100).contains(&limit) {
        Ok(())
    } else {
        Err(McpServerError::InvalidInput(
            "limit must be between 1 and 100".to_owned(),
        ))
    }
}

fn run_query(
    ctx: &McpContext,
    query: DeploymentTopologyQuery,
    repo: Option<String>,
    limit: usize,
) -> Result<DeploymentTopologyResponse, McpServerError> {
    let report = deployment_topology(ctx.graph(), query, repo.as_deref(), limit)?;
    Ok(report.into())
}

impl From<DeploymentTopologyReport> for DeploymentTopologyResponse {
    fn from(report: DeploymentTopologyReport) -> Self {
        Self {
            data: DeploymentTopologyData {
                query_kind: query_kind(&report.query).to_owned(),
                repo: report.repo,
                deployments: report.deployments.into_iter().map(Into::into).collect(),
                services: report.services.into_iter().map(Into::into).collect(),
                env_vars: report.env_vars.into_iter().map(Into::into).collect(),
                shared_infra: report.shared_infra.into_iter().map(Into::into).collect(),
                workflow_jobs: report.workflow_jobs.into_iter().map(Into::into).collect(),
                edges: report.edges.into_iter().map(Into::into).collect(),
                missing_evidence: report.missing_evidence,
            },
        }
    }
}

impl From<AnalysisNode> for DeploymentTopologyNodeItem {
    fn from(node: AnalysisNode) -> Self {
        Self {
            repo: node.repo,
            kind: node.kind.to_string(),
            name: node.name,
            file_path: node.file_path,
            qualified_name: node.qualified_name,
        }
    }
}

impl From<AnalysisEdge> for DeploymentTopologyEdgeItem {
    fn from(edge: AnalysisEdge) -> Self {
        Self {
            source: edge.source.into(),
            target: edge.target.into(),
            kind: edge.kind.to_string(),
            confidence: edge.confidence,
        }
    }
}

fn query_kind(query: &DeploymentTopologyQuery) -> &'static str {
    match query {
        DeploymentTopologyQuery::WhereDeployed { .. } => "where_deployed",
        DeploymentTopologyQuery::ServiceEnv { .. } => "service_env",
        DeploymentTopologyQuery::EnvVarConsumers { .. } => "env_var_consumers",
        DeploymentTopologyQuery::UndeployedServices => "undeployed_services",
        DeploymentTopologyQuery::DeployedButNoCode => "deployed_but_no_code",
        DeploymentTopologyQuery::SharedInfra => "shared_infra",
    }
}

#[cfg(test)]
mod tests {
    use super::{RepoTopologyRequest, validate_repo_and_limit};
    use crate::McpServerError;

    #[test]
    fn validates_limit_bounds() {
        assert!(validate_repo_and_limit(None, 1).is_ok());
        assert!(validate_repo_and_limit(None, 100).is_ok());

        let error = validate_repo_and_limit(None, 0).expect_err("limit 0 should fail");
        assert!(matches!(
            error,
            McpServerError::InvalidInput(message)
                if message == "limit must be between 1 and 100"
        ));
    }

    #[test]
    fn repo_request_default_limit_is_stable() {
        let request = serde_json::from_value::<RepoTopologyRequest>(serde_json::json!({}))
            .expect("request should parse");
        assert_eq!(request.limit, 20);
    }
}
