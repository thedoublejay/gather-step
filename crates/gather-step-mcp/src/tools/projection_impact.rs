use gather_step_analysis::{
    ProjectionEvidenceVerbosity as AnalysisEvidenceVerbosity,
    ProjectionImpactRequest as AnalysisRequest, projection_impact,
};
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
    #[serde(default)]
    pub evidence_verbosity: ProjectionEvidenceVerbosity,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionEvidenceVerbosity {
    Summary,
    #[default]
    Full,
}

impl From<ProjectionEvidenceVerbosity> for AnalysisEvidenceVerbosity {
    fn from(value: ProjectionEvidenceVerbosity) -> Self {
        match value {
            ProjectionEvidenceVerbosity::Summary => Self::Summary,
            ProjectionEvidenceVerbosity::Full => Self::Full,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ProjectionImpactResponse {
    pub data: ProjectionImpactData,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ProjectionImpactData {
    pub target: String,
    pub resolved: bool,
    #[serde(default)]
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
    #[serde(default)]
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
    #[serde(default)]
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
    validate_projection_impact_limit(request.limit)?;
    let report = projection_impact(
        ctx.graph(),
        AnalysisRequest {
            target: request.target,
            repo: request.repo,
            max_results: request.limit,
            evidence_verbosity: request.evidence_verbosity.into(),
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

fn validate_projection_impact_limit(limit: usize) -> Result<(), McpServerError> {
    if (1..=100).contains(&limit) {
        Ok(())
    } else {
        Err(McpServerError::InvalidInput(
            "limit must be between 1 and 100".to_owned(),
        ))
    }
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        ProjectionDerivationItem, ProjectionEvidenceItem, ProjectionEvidenceVerbosity,
        ProjectionFieldItem, ProjectionImpactData, ProjectionImpactRequest,
        ProjectionImpactResponse, validate_projection_impact_limit,
    };
    use crate::McpServerError;

    fn empty_data(target: &str) -> ProjectionImpactData {
        ProjectionImpactData {
            target: target.to_owned(),
            resolved: false,
            ambiguity: None,
            candidates: Vec::new(),
            source_fields: Vec::new(),
            projected_fields: Vec::new(),
            derivation_edges: Vec::new(),
            readers: Vec::new(),
            writers: Vec::new(),
            filters: Vec::new(),
            indexes: Vec::new(),
            backfills: Vec::new(),
            risk_hints: Vec::new(),
            missing_evidence: Vec::new(),
            confidence: "low".to_owned(),
        }
    }

    fn field(repo: &str, field_path: &str, qualified_name: Option<&str>) -> ProjectionFieldItem {
        ProjectionFieldItem {
            repo: repo.to_owned(),
            field_path: field_path.to_owned(),
            qualified_name: qualified_name.map(str::to_owned),
        }
    }

    fn evidence(
        file_path: &str,
        field_path: &str,
        edge_kind: &str,
        confidence: Option<u16>,
    ) -> ProjectionEvidenceItem {
        ProjectionEvidenceItem {
            repo: "svc".to_owned(),
            file_path: file_path.to_owned(),
            field_path: field_path.to_owned(),
            edge_kind: edge_kind.to_owned(),
            confidence,
        }
    }

    #[test]
    fn request_deserializes_evidence_verbosity_with_full_default() {
        let default_request: ProjectionImpactRequest =
            serde_json::from_value(json!({"target": "legacySeatIds"}))
                .expect("request should deserialize with defaults");
        assert_eq!(default_request.limit, 20);
        assert_eq!(
            default_request.evidence_verbosity,
            ProjectionEvidenceVerbosity::Full
        );

        let summary_request: ProjectionImpactRequest = serde_json::from_value(
            json!({"target": "legacySeatIds", "evidence_verbosity": "summary"}),
        )
        .expect("request should deserialize summary verbosity");
        assert_eq!(
            summary_request.evidence_verbosity,
            ProjectionEvidenceVerbosity::Summary
        );
    }

    #[test]
    fn validates_limit_bounds_before_analysis() {
        assert!(validate_projection_impact_limit(1).is_ok());
        assert!(validate_projection_impact_limit(100).is_ok());

        let error = validate_projection_impact_limit(0).expect_err("limit 0 should be rejected");
        assert!(matches!(
            error,
            McpServerError::InvalidInput(message)
                if message == "limit must be between 1 and 100"
        ));
    }

    #[test]
    fn empty_response_serializes_nulls_and_empty_arrays() {
        let response = ProjectionImpactResponse {
            data: empty_data("missingField"),
        };

        let value = serde_json::to_value(&response).expect("response should serialize");
        assert_eq!(value["data"]["ambiguity"], json!(null));
        for key in [
            "candidates",
            "source_fields",
            "projected_fields",
            "derivation_edges",
            "readers",
            "writers",
            "filters",
            "indexes",
            "backfills",
            "risk_hints",
            "missing_evidence",
        ] {
            assert_eq!(
                value["data"][key],
                json!([]),
                "field `{key}` should serialize as an empty array"
            );
        }
    }

    #[test]
    fn ambiguous_response_serializes_candidate_null_qualified_name() {
        let mut data = empty_data("status");
        data.resolved = true;
        data.ambiguity = Some("multiple_field_candidates".to_owned());
        data.candidates = vec![
            field("svc", "status", None),
            field(
                "svc",
                "status",
                Some("data-field::svc::src/account.ts::status"),
            ),
        ];
        let response = ProjectionImpactResponse { data };

        let value = serde_json::to_value(&response).expect("response should serialize");
        assert_eq!(
            value["data"]["ambiguity"],
            json!("multiple_field_candidates")
        );
        assert_eq!(
            value["data"]["candidates"][0]["qualified_name"],
            json!(null)
        );
        assert_eq!(
            value["data"]["candidates"][1]["qualified_name"],
            json!("data-field::svc::src/account.ts::status")
        );
    }

    #[test]
    fn successful_response_serializes_optional_evidence_confidence() {
        let source = field(
            "svc",
            "lineItems",
            Some("data-field::svc::src/projection.ts::lineItems"),
        );
        let projected = field(
            "svc",
            "lineItemTotal",
            Some("data-field::svc::src/projection.ts::lineItemTotal"),
        );
        let mut data = empty_data("lineItemTotal");
        data.resolved = true;
        data.confidence = "high".to_owned();
        data.source_fields = vec![source.clone()];
        data.projected_fields = vec![projected.clone()];
        data.derivation_edges = vec![ProjectionDerivationItem { source, projected }];
        data.indexes = vec![evidence(
            "src/projection.ts",
            "lineItemTotal",
            "IndexesField",
            Some(900),
        )];
        data.backfills = vec![evidence(
            "src/projection.ts",
            "lineItemTotal",
            "BackfillsField",
            None,
        )];
        let response = ProjectionImpactResponse { data };

        let value = serde_json::to_value(&response).expect("response should serialize");
        assert_eq!(value["data"]["ambiguity"], json!(null));
        assert_eq!(value["data"]["indexes"][0]["confidence"], json!(900));
        assert_eq!(value["data"]["backfills"][0]["confidence"], json!(null));
    }
}
