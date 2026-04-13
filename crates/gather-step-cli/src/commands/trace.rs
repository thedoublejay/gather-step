use anyhow::{Result, anyhow};
use clap::{Args, Subcommand};
use gather_step_mcp::tools::crud_trace::{CrudTraceRequest, crud_trace_tool};
use serde_json::json;

use crate::command_render::RenderedCommand;
use crate::daemon_protocol::DaemonRequest;
use crate::{app::AppContext, daemon_proxy};

#[derive(Debug, Args)]
pub struct TraceArgs {
    #[command(subcommand)]
    pub command: TraceCommand,
}

#[derive(Debug, Subcommand)]
pub enum TraceCommand {
    Crud(CrudArgs),
}

#[derive(Debug, Args)]
pub struct CrudArgs {
    #[arg(long, help = "HTTP method to trace")]
    pub method: Option<String>,
    #[arg(long, help = "Route path to trace")]
    pub path: Option<String>,
    #[arg(long, help = "Stable symbol_id entrypoint for the CRUD trace")]
    pub symbol_id: Option<String>,
    #[arg(long, default_value_t = 25, help = "Maximum matches per section")]
    pub limit: usize,
}

pub fn run(app: &AppContext, args: TraceArgs) -> Result<()> {
    let request = daemon_request(&args, app);
    daemon_proxy::run_read_only_command(app, &request, move |app| run_rendered(app, args))
}

pub(crate) fn run_rendered(app: &AppContext, args: TraceArgs) -> Result<RenderedCommand> {
    match args.command {
        TraceCommand::Crud(args) => run_crud_rendered(app, args),
    }
}

fn daemon_request(args: &TraceArgs, app: &AppContext) -> DaemonRequest {
    match &args.command {
        TraceCommand::Crud(args) => DaemonRequest::TraceCrud {
            method: args.method.clone(),
            path: args.path.clone(),
            symbol_id: args.symbol_id.clone(),
            limit: args.limit,
            repo_filter: app.repo_filter.clone(),
        },
    }
}

fn run_crud_rendered(app: &AppContext, args: CrudArgs) -> Result<RenderedCommand> {
    let ctx = gather_step_mcp::McpContext::open(gather_step_mcp::McpServerConfig::new(
        app.workspace_paths().registry_path,
        app.workspace_paths().graph_path,
    ))?;
    execute_crud(&ctx, app.repo_filter.as_deref(), args)
}

pub(crate) fn execute_crud(
    ctx: &gather_step_mcp::McpContext,
    repo_filter: Option<&str>,
    args: CrudArgs,
) -> Result<RenderedCommand> {
    let response = crud_trace_tool(
        ctx,
        CrudTraceRequest {
            budget_bytes: None,
            limit: Some(args.limit),
            method: args.method,
            path: args.path,
            symbol_id: args.symbol_id,
        },
    )?;
    if response.data.target_id.is_none() {
        return Err(anyhow!(
            "no matching route target found for {} {}",
            response.data.method,
            response.data.path
        ));
    }

    let mut response = response;
    if let Some(repo) = repo_filter {
        response.data.callers.retain(|item| item.repo == repo);
        response.data.handlers.retain(|item| item.repo == repo);
        response.data.continuation.retain(|item| item.repo == repo);
        response.data.entities.retain(|item| item.repo == repo);
        response
            .data
            .database_hints
            .retain(|item| item.repo == repo);
    }

    let payload = json!({
        "event": "trace_crud_completed",
        "callers": response.data.callers,
        "continuation": response.data.continuation,
        "database_hints": response.data.database_hints,
        "entities": response.data.entities,
        "handlers": response.data.handlers,
        "method": response.data.method,
        "path": response.data.path,
        "target_id": response.data.target_id,
        "target_name": response.data.target_name,
        "truncated": response.meta.as_ref().is_some_and(|meta| meta.truncated),
    });

    let mut lines = vec![format!(
        "CRUD trace {} {}",
        response.data.method, response.data.path
    )];
    lines.push("Callers:".to_owned());
    if response.data.callers.is_empty() {
        lines.push("  none".to_owned());
    } else {
        for caller in &response.data.callers {
            lines.push(format!(
                "  {} {}:{} evidence={}{}{}",
                caller.symbol_name,
                caller.repo,
                caller.file_path,
                caller.evidence_kind,
                format_confidence(caller.confidence),
                format_resolver(caller.resolver.as_deref()),
            ));
        }
    }
    lines.push("Handlers:".to_owned());
    if response.data.handlers.is_empty() {
        lines.push("  none".to_owned());
    } else {
        for handler in &response.data.handlers {
            lines.push(format!(
                "  {} {}:{} evidence={}{}{}",
                handler.symbol_name,
                handler.repo,
                handler.file_path,
                handler.evidence_kind,
                format_confidence(handler.confidence),
                format_resolver(handler.resolver.as_deref()),
            ));
        }
    }
    lines.push("Continuation:".to_owned());
    if response.data.continuation.is_empty() {
        lines.push("  none".to_owned());
    } else {
        for node in &response.data.continuation {
            lines.push(format!(
                "  [{}] {} {}:{} evidence={}{}{}",
                node.role,
                node.symbol_name,
                node.repo,
                node.file_path,
                node.evidence_kind,
                format_confidence(node.confidence),
                format_resolver(node.resolver.as_deref()),
            ));
        }
    }
    lines.push("Database Hints:".to_owned());
    if response.data.database_hints.is_empty() {
        lines.push("  none".to_owned());
    } else {
        for node in &response.data.database_hints {
            lines.push(format!(
                "  [{}] {} {}:{} evidence={}{}{}",
                node.role,
                node.symbol_name,
                node.repo,
                node.file_path,
                node.evidence_kind,
                format_confidence(node.confidence),
                format_resolver(node.resolver.as_deref()),
            ));
        }
    }
    lines.push("Entities:".to_owned());
    if response.data.entities.is_empty() {
        lines.push("  none".to_owned());
    } else {
        for node in &response.data.entities {
            lines.push(format!(
                "  [{}] {} {}:{} evidence={}{}{}",
                node.role,
                node.symbol_name,
                node.repo,
                node.file_path,
                node.evidence_kind,
                format_confidence(node.confidence),
                format_resolver(node.resolver.as_deref()),
            ));
        }
    }
    lines.push(format!(
        "Truncated: {}",
        response.meta.as_ref().is_some_and(|meta| meta.truncated)
    ));
    if let Some(repo) = repo_filter {
        lines.push(format!("Repo filter: {repo}"));
    }

    Ok(RenderedCommand::success(payload, lines))
}

fn format_confidence(confidence: Option<u16>) -> String {
    confidence.map_or_else(String::new, |value| format!(" confidence={value}"))
}

fn format_resolver(resolver: Option<&str>) -> String {
    resolver.map_or_else(String::new, |value| format!(" resolver={value}"))
}
