use anyhow::{Result, bail};
use clap::{Args, ValueEnum};
use gather_step_mcp::tools::{
    events::{TraceEventRequest, TraceRouteRequest, trace_event_tool, trace_route_tool},
    packs::{ContextPackRequest, context_pack_tool},
};
use serde_json::json;

use crate::command_render::RenderedCommand;
use crate::daemon_protocol::DaemonRequest;
use crate::storage_context::StorageContext;
use crate::{app::AppContext, daemon_proxy};

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum PackModeArg {
    Planning,
    #[value(alias = "debugging")]
    Debug,
    #[value(alias = "fixing")]
    Fix,
    Review,
    #[value(name = "change_impact", alias = "change-impact")]
    ChangeImpact,
}

impl PackModeArg {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Planning => "planning",
            Self::Debug => "debug",
            Self::Fix => "fix",
            Self::Review => "review",
            Self::ChangeImpact => "change_impact",
        }
    }
}

#[derive(Debug, Args)]
pub struct PackArgs {
    #[arg(help = "Target symbol name or symbol_id")]
    pub target: Option<String>,
    #[arg(
        long,
        help = "Explicit symbol target; equivalent to the positional target"
    )]
    pub symbol: Option<String>,
    #[arg(long, help = "Resolve a route target from this HTTP method")]
    pub route_method: Option<String>,
    #[arg(long, help = "Resolve a route target from this HTTP path")]
    pub route_path: Option<String>,
    #[arg(long, help = "Resolve an event-like target from this subject")]
    pub event_target: Option<String>,
    #[arg(
        long,
        value_enum,
        default_value_t = PackModeArg::Planning,
        help = "Pack mode: planning, debug, fix, review, or change_impact"
    )]
    pub mode: PackModeArg,
    #[arg(long, default_value_t = 6, help = "Maximum ranked items to include")]
    pub limit: usize,
    #[arg(
        long,
        default_value_t = 2,
        help = "Traversal depth for caller/callee context"
    )]
    pub depth: usize,
    #[arg(long, help = "Optional response byte budget override")]
    pub budget_bytes: Option<usize>,
}

pub fn run(app: &AppContext, args: &PackArgs) -> Result<()> {
    daemon_proxy::run_read_only_command(
        app,
        &DaemonRequest::Pack {
            target: args.target.clone(),
            symbol: args.symbol.clone(),
            route_method: args.route_method.clone(),
            route_path: args.route_path.clone(),
            event_target: args.event_target.clone(),
            mode: args.mode.as_str().to_owned(),
            limit: args.limit,
            depth: args.depth,
            budget_bytes: args.budget_bytes,
            repo_filter: app.repo_filter.clone(),
        },
        |app| run_rendered(app, &StorageContext::workspace_read_only(app), args),
    )
}

pub(crate) fn run_rendered(
    app: &AppContext,
    ctx: &StorageContext,
    args: &PackArgs,
) -> Result<RenderedCommand> {
    let mcp = gather_step_mcp::McpContext::open(ctx.mcp_server_config())?;
    execute(&mcp, app.repo_filter.clone(), args)
}

pub(crate) fn execute(
    ctx: &gather_step_mcp::McpContext,
    repo_filter: Option<String>,
    args: &PackArgs,
) -> Result<RenderedCommand> {
    let target = resolve_target(ctx, args)?;
    let response = context_pack_tool(
        ctx,
        ContextPackRequest {
            budget_bytes: args.budget_bytes,
            depth: Some(args.depth),
            limit: Some(args.limit),
            repo: repo_filter,
            mode: args.mode.as_str().to_owned(),
            target: target.clone(),
        },
    )?;

    let payload = json!({
        "event": "context_pack_completed",
        "response_schema_version": response
            .meta
            .as_ref()
            .map_or_else(
                gather_step_mcp::budget::response_schema_version,
                |meta| meta.response_schema_version,
            ),
        "data": response.data,
        "meta": response.meta,
    });
    let mut lines = vec![format!(
        "Context pack [{}] for {}",
        response.data.mode, response.data.target
    )];
    for item in &response.data.items {
        lines.push(format!(
            "  [{}] {} {}:{}",
            item.category, item.symbol_name, item.repo, item.file_path
        ));
    }
    if !response.data.semantic_bridges.is_empty() {
        lines.push("Semantic bridges:".to_owned());
        for bridge in &response.data.semantic_bridges {
            lines.push(format!(
                "  [{}] {} ({})",
                bridge.kind, bridge.name, bridge.repo
            ));
        }
    }
    if !response.data.next_steps.is_empty() {
        lines.push(format!(
            "Next steps: {}",
            response.data.next_steps.join(", ")
        ));
    }
    if !response.data.unresolved_gaps.is_empty() {
        lines.push(format!(
            "Gaps: {}",
            response.data.unresolved_gaps.join("; ")
        ));
    }

    Ok(RenderedCommand::success(payload, lines))
}

fn resolve_target(ctx: &gather_step_mcp::McpContext, args: &PackArgs) -> Result<String> {
    let mut provided = 0_u8;
    provided += u8::from(args.target.is_some());
    provided += u8::from(args.symbol.is_some());
    provided += u8::from(args.event_target.is_some());
    provided += u8::from(args.route_method.is_some() || args.route_path.is_some());

    if provided != 1 {
        bail!(
            "provide exactly one target source: positional target, --symbol, --event-target, or --route-method with --route-path"
        );
    }

    if let Some(target) = args.target.as_ref().or(args.symbol.as_ref()) {
        return Ok(target.clone());
    }

    if let Some(target) = args.event_target.as_ref() {
        let response = trace_event_tool(
            ctx,
            TraceEventRequest {
                budget_bytes: None,
                limit: Some(10),
                target: target.clone(),
            },
        )?;
        let Some(match_) = response.data.matches.first() else {
            bail!("no event-like target matched `{target}`");
        };
        return Ok(match_.target_id.clone());
    }

    let (Some(method), Some(path)) = (args.route_method.as_ref(), args.route_path.as_ref()) else {
        bail!("--route-method and --route-path must be provided together");
    };
    let response = trace_route_tool(
        ctx,
        TraceRouteRequest {
            budget_bytes: None,
            limit: Some(10),
            method: method.clone(),
            path: path.clone(),
        },
    )?;
    let Some(target_id) = response.data.target_id else {
        bail!("no route target matched {method} {path}");
    };
    Ok(target_id)
}
