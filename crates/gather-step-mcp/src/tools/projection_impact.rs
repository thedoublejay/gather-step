use gather_step_analysis::{ProjectionImpactRequest as AnalysisRequest, projection_impact};
use rmcp::schemars;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{McpContext, McpServerError};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ProjectionImpactRequest {
    pub target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ProjectionImpactResponse {
    pub data: ProjectionImpactData,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ProjectionImpactData {
    pub target: String,
    pub resolved: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ambiguity: Option<String>,
    pub candidates: Vec<ProjectionFieldItem>,
    pub source_fields: Vec<ProjectionFieldItem>,
    pub projected_fields: Vec<ProjectionFieldItem>,
    pub derivation_edges: Vec<ProjectionDerivationItem>,
    pub readers: Vec<ProjectionEvidenceItem>,
    pub writers: Vec<ProjectionEvidenceItem>,
    pub filters: Vec<ProjectionEvidenceItem>,
    pub indexes: Vec<ProjectionEvidenceItem>,
    pub backfills: Vec<ProjectionEvidenceItem>,
    pub risk_hints: Vec<String>,
    pub missing_evidence: Vec<String>,
    pub confidence: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ProjectionFieldItem {
    pub repo: String,
    pub field_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qualified_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ProjectionDerivationItem {
    pub source: ProjectionFieldItem,
    pub projected: ProjectionFieldItem,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ProjectionEvidenceItem {
    pub repo: String,
    pub file_path: String,
    pub field_path: String,
    pub edge_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<u16>,
}

fn default_limit() -> usize {
    20
}

pub fn projection_impact_tool(
    ctx: &McpContext,
    request: ProjectionImpactRequest,
) -> Result<ProjectionImpactResponse, McpServerError> {
    if request.target.trim().is_empty() {
        return Err(McpServerError::InvalidInput(
            "target must not be empty".to_owned(),
        ));
    }
    let report = projection_impact(
        ctx.graph(),
        AnalysisRequest {
            target: request.target,
            repo: request.repo,
            max_results: request.limit,
        },
    )?;

    Ok(ProjectionImpactResponse {
        data: ProjectionImpactData {
            target: report.target,
            resolved: report.resolved,
            ambiguity: report.ambiguity,
            candidates: report.candidates.into_iter().map(Into::into).collect(),
            source_fields: report.source_fields.into_iter().map(Into::into).collect(),
            projected_fields: report
                .projected_fields
                .into_iter()
                .map(Into::into)
                .collect(),
            derivation_edges: report
                .derivation_edges
                .into_iter()
                .map(|edge| ProjectionDerivationItem {
                    source: edge.source.into(),
                    projected: edge.projected.into(),
                })
                .collect(),
            readers: report.readers.into_iter().map(Into::into).collect(),
            writers: report.writers.into_iter().map(Into::into).collect(),
            filters: report.filters.into_iter().map(Into::into).collect(),
            indexes: report.indexes.into_iter().map(Into::into).collect(),
            backfills: report.backfills.into_iter().map(Into::into).collect(),
            risk_hints: report.risk_hints,
            missing_evidence: report.missing_evidence,
            confidence: report.confidence,
        },
    })
}

impl From<gather_step_analysis::ProjectionField> for ProjectionFieldItem {
    fn from(field: gather_step_analysis::ProjectionField) -> Self {
        Self {
            repo: field.repo,
            field_path: field.field_path,
            qualified_name: field.qualified_name,
        }
    }
}

impl From<gather_step_analysis::ProjectionEvidence> for ProjectionEvidenceItem {
    fn from(item: gather_step_analysis::ProjectionEvidence) -> Self {
        Self {
            repo: item.repo,
            file_path: item.file_path,
            field_path: item.field_path,
            edge_kind: item.edge_kind.to_string(),
            confidence: item.confidence,
        }
    }
}
