use anyhow::{Result, bail};
use clap::Args;
use gather_step_analysis::{ProjectionImpactRequest, projection_impact};
use gather_step_storage::StorageCoordinator;

use crate::app::AppContext;
use crate::command_render::RenderedCommand;

#[derive(Debug, Args)]
pub struct ProjectionImpactArgs {
    #[arg(long, help = "Field or projected field name to inspect")]
    pub target: String,
    #[arg(
        long,
        default_value_t = 20,
        help = "Maximum field candidates to inspect"
    )]
    pub limit: usize,
}

pub fn run(app: &AppContext, args: ProjectionImpactArgs) -> Result<()> {
    run_rendered(app, args)?.emit(&app.output())
}

pub(crate) fn run_rendered(
    app: &AppContext,
    args: ProjectionImpactArgs,
) -> Result<RenderedCommand> {
    if args.target.trim().is_empty() {
        bail!("projection-impact --target must not be empty");
    }

    let storage = StorageCoordinator::open(app.workspace_paths().storage_root)?;
    let report = projection_impact(
        storage.graph(),
        ProjectionImpactRequest {
            target: args.target,
            repo: app.repo_filter.clone(),
            max_results: args.limit,
        },
    )?;

    let mut lines = Vec::new();
    if report.resolved {
        lines.push(format!(
            "projection impact for `{}`: {} candidate(s), confidence {}",
            report.target,
            report.candidates.len(),
            report.confidence
        ));
        if !report.source_fields.is_empty() {
            lines.push(format!(
                "source fields: {}",
                report
                    .source_fields
                    .iter()
                    .map(|field| field.field_path.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if !report.projected_fields.is_empty() {
            lines.push(format!(
                "projected fields: {}",
                report
                    .projected_fields
                    .iter()
                    .map(|field| field.field_path.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
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
        if !report.risk_hints.is_empty() {
            lines.push(format!("risk hints: {}", report.risk_hints.join(", ")));
        }
    }

    RenderedCommand::success_serialized(&report, lines)
}
