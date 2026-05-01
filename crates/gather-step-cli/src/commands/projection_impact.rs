use anyhow::{Result, bail};
use clap::{Args, ValueEnum};
use gather_step_analysis::{
    ProjectionEvidenceVerbosity, ProjectionField, ProjectionImpactReport, ProjectionImpactRequest,
    projection_impact_with_payload_contracts,
};
use gather_step_core::PayloadSide;
use gather_step_storage::{MetadataStore, PayloadContractQuery, StorageCoordinator};

use crate::app::AppContext;
use crate::command_render::RenderedCommand;

#[derive(Debug, Args)]
pub struct ProjectionImpactArgs {
    #[arg(
        long,
        help = "Field or projected field name to inspect. Accepts a bare field name (`stepIds`), \
                a typed dotted path (`WorkItem.workflow.stepIds`), or a qualified node id."
    )]
    pub target: String,
    #[arg(
        long,
        default_value_t = 20,
        value_parser = parse_projection_impact_limit,
        help = "Maximum field candidates to inspect (1-100)"
    )]
    pub limit: usize,
    #[arg(
        long,
        value_enum,
        default_value_t = EvidenceVerbosityArg::Full,
        help = "Evidence detail level to return"
    )]
    pub evidence_verbosity: EvidenceVerbosityArg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum EvidenceVerbosityArg {
    Summary,
    Full,
}

impl From<EvidenceVerbosityArg> for ProjectionEvidenceVerbosity {
    fn from(value: EvidenceVerbosityArg) -> Self {
        match value {
            EvidenceVerbosityArg::Summary => Self::Summary,
            EvidenceVerbosityArg::Full => Self::Full,
        }
    }
}

pub fn run(app: &AppContext, args: ProjectionImpactArgs) -> Result<()> {
    run_rendered(app, args)?.emit(&app.output())
}

pub(crate) fn run_rendered(
    app: &AppContext,
    args: ProjectionImpactArgs,
) -> Result<RenderedCommand> {
    let storage = StorageCoordinator::open(app.workspace_paths().storage_root)?;
    execute(&storage, app.repo_filter.as_deref(), args)
}

pub fn execute(
    storage: &StorageCoordinator,
    repo_filter: Option<&str>,
    args: ProjectionImpactArgs,
) -> Result<RenderedCommand> {
    if args.target.trim().is_empty() {
        bail!("projection-impact --target must not be empty");
    }

    let payload_contracts = storage
        .metadata()
        .payload_contracts_for_query(PayloadContractQuery {
            repo: repo_filter.map(ToOwned::to_owned),
            min_confidence: Some(750),
            side: None::<PayloadSide>,
            ..PayloadContractQuery::default()
        })?
        .into_iter()
        .map(|record| record.record)
        .collect::<Vec<_>>();

    let report = projection_impact_with_payload_contracts(
        storage.graph(),
        ProjectionImpactRequest {
            target: args.target,
            repo: repo_filter.map(ToOwned::to_owned),
            max_results: args.limit,
            evidence_verbosity: args.evidence_verbosity.into(),
        },
        &payload_contracts,
    )?;

    let lines = render_text_lines(&report);
    RenderedCommand::success_serialized(&report, lines)
}

fn render_text_lines(report: &ProjectionImpactReport) -> Vec<String> {
    let mut lines = Vec::new();
    if report.resolved {
        lines.push(format!(
            "projection impact for `{}`: {} {}, confidence {}",
            report.target,
            report.candidates.len(),
            pluralize(report.candidates.len(), "candidate", "candidates"),
            report.confidence
        ));
        if let Some(ambiguity) = &report.ambiguity {
            lines.push(format!("ambiguity: {ambiguity}"));
        }
        if report.ambiguity.is_some() && !report.candidates.is_empty() {
            lines.push(format!(
                "candidate fields: {}",
                format_fields(&report.candidates)
            ));
        }
        if !report.source_fields.is_empty() {
            lines.push(format!(
                "source fields: {}",
                format_fields(&report.source_fields)
            ));
        }
        if !report.projected_fields.is_empty() {
            lines.push(format!(
                "projected fields: {}",
                format_fields(&report.projected_fields)
            ));
        }
        if !report.derivation_edges.is_empty() {
            lines.push(format!(
                "projection chain: {}",
                report
                    .derivation_edges
                    .iter()
                    .map(|edge| format!(
                        "{} -> {}",
                        format_field(&edge.source),
                        format_field(&edge.projected)
                    ))
                    .collect::<Vec<_>>()
                    .join("; ")
            ));
        }
        if !report.readers.is_empty() {
            lines.push(format!("readers: {}", format_evidence(&report.readers)));
        }
        if !report.writers.is_empty() {
            lines.push(format!("writers: {}", format_evidence(&report.writers)));
        }
        if !report.filters.is_empty() {
            lines.push(format!("filters: {}", format_evidence(&report.filters)));
        }
        if !report.indexes.is_empty() {
            lines.push(format!("indexes: {}", format_evidence(&report.indexes)));
        }
        if !report.backfills.is_empty() {
            lines.push(format!("backfills: {}", format_evidence(&report.backfills)));
        }
        if !report.missing_evidence.is_empty() {
            lines.push(format!(
                "missing evidence: {}",
                report.missing_evidence.join(", ")
            ));
        }
        if !report.risk_hints.is_empty() {
            lines.push(format!("risk hints: {}", report.risk_hints.join(", ")));
        }
    } else {
        lines.push(format!(
            "projection impact for `{}`: no indexed data field found",
            report.target
        ));
        if !report.missing_evidence.is_empty() {
            lines.push(format!(
                "missing evidence: {}",
                report.missing_evidence.join(", ")
            ));
        }
        if !report.risk_hints.is_empty() {
            lines.push(format!("risk hints: {}", report.risk_hints.join(", ")));
        }
    }
    lines
}

fn format_fields(fields: &[ProjectionField]) -> String {
    fields
        .iter()
        .map(format_field)
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_field(field: &ProjectionField) -> String {
    format!("{}:{}", field.repo, field.field_path)
}

fn format_evidence(evidence: &[gather_step_analysis::ProjectionEvidence]) -> String {
    evidence
        .iter()
        .map(|item| {
            let source = item
                .evidence_source
                .as_deref()
                .map_or(String::new(), |source| format!(", {source}"));
            format!(
                "{}:{} ({}{source})",
                item.repo, item.file_path, item.field_path
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn pluralize<'a>(count: usize, singular: &'a str, plural: &'a str) -> &'a str {
    if count == 1 { singular } else { plural }
}

fn parse_projection_impact_limit(value: &str) -> std::result::Result<usize, String> {
    let limit = value
        .parse::<usize>()
        .map_err(|_| "limit must be an integer between 1 and 100".to_owned())?;
    if (1..=100).contains(&limit) {
        Ok(limit)
    } else {
        Err("limit must be an integer between 1 and 100".to_owned())
    }
}
