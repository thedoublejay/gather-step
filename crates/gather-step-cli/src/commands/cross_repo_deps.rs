use anyhow::{Result, bail};
use clap::Args;
use gather_step_mcp::tools::cross_repo::{
    CrossRepoDepsData, CrossRepoDepsRequest, cross_repo_deps_tool,
};
use serde_json::json;

use crate::command_render::RenderedCommand;
use crate::daemon_protocol::DaemonRequest;
use crate::storage_context::StorageContext;
use crate::{app::AppContext, daemon_proxy};

#[derive(Debug, Args)]
pub struct CrossRepoDepsArgs {
    #[arg(help = "Repo to inspect; defaults to every configured repo")]
    pub repo: Option<String>,
}

pub fn run(app: &AppContext, args: &CrossRepoDepsArgs) -> Result<()> {
    daemon_proxy::run_read_only_command(
        app,
        &DaemonRequest::CrossRepoDeps {
            repo: args.repo.clone(),
            repo_filter: app.repo_filter.clone(),
        },
        |app| run_rendered(app, &StorageContext::workspace_read_only(app), args),
    )
}

pub(crate) fn run_rendered(
    app: &AppContext,
    ctx: &StorageContext,
    args: &CrossRepoDepsArgs,
) -> Result<RenderedCommand> {
    let mcp = gather_step_mcp::McpContext::open(ctx.mcp_server_config())?;
    execute(&mcp, args.repo.as_deref().or(app.repo_filter.as_deref()))
}

pub(crate) fn execute(
    ctx: &gather_step_mcp::McpContext,
    repo: Option<&str>,
) -> Result<RenderedCommand> {
    let registry = ctx.registry_snapshot()?;
    let mut configured: Vec<String> = registry.repos.keys().cloned().collect();
    configured.sort();

    let targets = match repo {
        Some(name) => {
            if !configured.iter().any(|candidate| candidate == name) {
                bail!(
                    "unknown repo `{name}`; configured repos: {}",
                    configured.join(", ")
                );
            }
            vec![name.to_owned()]
        }
        None => configured,
    };

    let mut repos: Vec<CrossRepoDepsData> = Vec::with_capacity(targets.len());
    for repo_name in targets {
        let response = cross_repo_deps_tool(ctx, CrossRepoDepsRequest { repo: repo_name })?;
        repos.push(response.data);
    }

    let mut lines = Vec::new();
    let mut total_edges = 0_usize;
    for entry in &repos {
        if entry.dependencies.is_empty() {
            continue;
        }
        lines.push(format!("{}:", entry.repo));
        for dependency in &entry.dependencies {
            total_edges += 1;
            lines.push(format!(
                "  {} ({})",
                dependency.repo,
                dependency.edge_kinds.join(", ")
            ));
        }
    }
    if total_edges == 0 {
        lines.push("No cross-repo dependency edges found.".to_owned());
    }

    let payload = json!({
        "event": "cross_repo_deps_completed",
        "repos": repos,
    });
    Ok(RenderedCommand::success(payload, lines))
}
