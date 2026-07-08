use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::{Args, Subcommand};
use gather_step_analysis::{
    DeploymentTopologyQuery, DeploymentTopologyReport, deployment_topology,
};
use gather_step_storage::StorageCoordinator;

use crate::app::AppContext;
use crate::command_render::RenderedCommand;
use crate::daemon_protocol::DaemonRequest;
use crate::daemon_proxy;
use crate::storage_context::StorageContext;

#[derive(Debug, Args)]
pub struct DeploymentTopologyArgs {
    #[command(subcommand)]
    pub command: DeploymentTopologyCommand,
    #[arg(long, default_value_t = 20, help = "Maximum result count")]
    pub limit: usize,
    /// Read symbol registry JSON from this path (used by `pr-review`
    /// follow-ups to query a kept review index instead of the workspace
    /// baseline).
    #[arg(long)]
    pub registry: Option<PathBuf>,
    /// Read storage artifacts from this directory.
    #[arg(long)]
    pub storage: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
pub enum DeploymentTopologyCommand {
    WhereDeployed {
        #[arg(long)]
        service: String,
    },
    ServiceEnv {
        #[arg(long)]
        service: String,
    },
    EnvVarConsumers {
        #[arg(long = "env-var")]
        env_var: String,
    },
    UndeployedServices,
    DeployedButNoCode,
    SharedInfra,
}

pub fn run(app: &AppContext, args: DeploymentTopologyArgs) -> Result<()> {
    if args.registry.is_some() || args.storage.is_some() {
        return run_rendered(app, &args)?.emit(&app.output());
    }

    validate_limit(args.limit)?;
    let query = query_from_command(&args.command)?;
    daemon_proxy::run_read_only_command(
        app,
        &DaemonRequest::DeploymentTopology {
            query,
            limit: args.limit,
            repo_filter: app.repo_filter.clone(),
        },
        move |app| run_rendered(app, &args),
    )
}

pub(crate) fn run_rendered(
    app: &AppContext,
    args: &DeploymentTopologyArgs,
) -> Result<RenderedCommand> {
    let ctx = if args.registry.is_some() || args.storage.is_some() {
        StorageContext::workspace_read_only_with_overrides(
            app,
            args.registry.clone(),
            args.storage.clone(),
        )
    } else {
        StorageContext::workspace_read_only(app)
    };
    let storage = ctx.open_storage_coordinator()?;
    execute(&storage, app.repo_filter.as_deref(), args)
}

pub fn execute(
    storage: &StorageCoordinator,
    repo_filter: Option<&str>,
    args: &DeploymentTopologyArgs,
) -> Result<RenderedCommand> {
    let query = query_from_command(&args.command)?;
    execute_query(storage, repo_filter, query, args.limit)
}

pub(crate) fn execute_query(
    storage: &StorageCoordinator,
    repo_filter: Option<&str>,
    query: DeploymentTopologyQuery,
    limit: usize,
) -> Result<RenderedCommand> {
    validate_limit(limit)?;
    let report = deployment_topology(storage.graph(), query, repo_filter, limit)?;
    RenderedCommand::success_serialized(&report, render_text_lines(&report))
}

fn validate_limit(limit: usize) -> Result<()> {
    if !(1..=100).contains(&limit) {
        bail!("The `deployment-topology --limit` flag must be between 1 and 100.");
    }
    Ok(())
}

fn query_from_command(command: &DeploymentTopologyCommand) -> Result<DeploymentTopologyQuery> {
    Ok(match command {
        DeploymentTopologyCommand::WhereDeployed { service } => {
            require_target("service", service)?;
            DeploymentTopologyQuery::WhereDeployed {
                service: service.clone(),
            }
        }
        DeploymentTopologyCommand::ServiceEnv { service } => {
            require_target("service", service)?;
            DeploymentTopologyQuery::ServiceEnv {
                service: service.clone(),
            }
        }
        DeploymentTopologyCommand::EnvVarConsumers { env_var } => {
            require_target("env-var", env_var)?;
            DeploymentTopologyQuery::EnvVarConsumers {
                env_var: env_var.clone(),
            }
        }
        DeploymentTopologyCommand::UndeployedServices => {
            DeploymentTopologyQuery::UndeployedServices
        }
        DeploymentTopologyCommand::DeployedButNoCode => DeploymentTopologyQuery::DeployedButNoCode,
        DeploymentTopologyCommand::SharedInfra => DeploymentTopologyQuery::SharedInfra,
    })
}

fn require_target(name: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("The `deployment-topology --{name}` flag must not be empty.");
    }
    Ok(())
}

fn render_text_lines(report: &DeploymentTopologyReport) -> Vec<String> {
    let mut lines = Vec::new();
    lines.push(format!(
        "deployment topology: {} services, {} deployments, {} env vars, {} shared infra, {} workflow jobs",
        report.services.len(),
        report.deployments.len(),
        report.env_vars.len(),
        report.shared_infra.len(),
        report.workflow_jobs.len()
    ));
    for edge in &report.edges {
        let mut source_kind = format!("{:?}", edge.source.kind);
        source_kind.make_ascii_lowercase();
        let mut target_kind = format!("{:?}", edge.target.kind);
        target_kind.make_ascii_lowercase();
        lines.push(format!(
            "- {} `{}` -> {} `{}` ({})",
            source_kind, edge.source.name, target_kind, edge.target.name, edge.kind
        ));
    }
    for missing in &report.missing_evidence {
        lines.push(format!("- missing evidence: {missing}"));
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::{DeploymentTopologyArgs, DeploymentTopologyCommand, execute, render_text_lines};
    use gather_step_analysis::{DeploymentTopologyQuery, DeploymentTopologyReport};
    use gather_step_storage::StorageCoordinator;

    #[test]
    fn render_text_includes_empty_sections_as_counts() {
        let report = DeploymentTopologyReport {
            query: DeploymentTopologyQuery::SharedInfra,
            repo: None,
            deployments: Vec::new(),
            services: Vec::new(),
            env_vars: Vec::new(),
            shared_infra: Vec::new(),
            workflow_jobs: Vec::new(),
            edges: Vec::new(),
            missing_evidence: Vec::new(),
        };

        assert_eq!(
            render_text_lines(&report)[0],
            "deployment topology: 0 services, 0 deployments, 0 env vars, 0 shared infra, 0 workflow jobs"
        );
    }

    #[test]
    fn rejects_empty_service_target() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("gather-step-cli-deploy-topology-{unique}"));
        let storage = StorageCoordinator::open(&root).expect("storage");
        let error = execute(
            &storage,
            None,
            &DeploymentTopologyArgs {
                command: DeploymentTopologyCommand::WhereDeployed {
                    service: " ".to_owned(),
                },
                limit: 20,
                registry: None,
                storage: None,
            },
        )
        .expect_err("empty service should fail");
        assert!(
            error
                .to_string()
                .contains("`deployment-topology --service` flag must not be empty"),
            "unexpected error text: {error}"
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
