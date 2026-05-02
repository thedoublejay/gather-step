use anyhow::{Context, Result};
use clap::Args;
use comfy_table::{Cell, ContentArrangement, Table, presets::UTF8_BORDERS_ONLY};
use gather_step_core::RegistryStore;
use gather_step_output::derive_conventions;
use gather_step_storage::GraphStoreDb;
use serde::Serialize;
use serde_json::json;

use crate::command_render::RenderedCommand;
use crate::daemon_protocol::DaemonRequest;
use crate::storage_context::StorageContext;
use crate::{app::AppContext, daemon_proxy};

#[derive(Debug, Args, Default)]
pub struct ConventionsArgs {}

#[derive(Debug, Serialize)]
struct ConventionsOutput {
    event: &'static str,
    conventions: Vec<String>,
}

pub fn run(app: &AppContext, _args: ConventionsArgs) -> Result<()> {
    daemon_proxy::run_read_only_command(
        app,
        &DaemonRequest::Conventions {
            repo_filter: app.repo_filter.clone(),
        },
        |app| run_rendered(app, &StorageContext::workspace_read_only(app)),
    )
}

pub(crate) fn run_rendered(app: &AppContext, ctx: &StorageContext) -> Result<RenderedCommand> {
    let registry = RegistryStore::open(ctx.registry_path())
        .with_context(|| format!("opening {}", ctx.registry_path().display()))?;
    let graph = GraphStoreDb::open(ctx.graph_path())
        .with_context(|| format!("opening {}", ctx.graph_path().display()))?;
    execute(&registry, &graph, app.repo_filter.as_deref())
}

pub(crate) fn execute(
    registry: &RegistryStore,
    graph: &GraphStoreDb,
    repo_filter: Option<&str>,
) -> Result<RenderedCommand> {
    let payload = ConventionsOutput {
        event: "conventions_completed",
        conventions: derive_conventions(graph, registry.registry(), repo_filter)?,
    };
    let mut lines = Vec::new();
    if payload.conventions.is_empty() {
        lines.push("No derived conventions found.".to_owned());
    } else {
        let mut table = Table::new();
        table.load_preset(UTF8_BORDERS_ONLY);
        table.set_content_arrangement(ContentArrangement::Dynamic);
        table.set_header(vec!["Derived Convention"]);
        for convention in &payload.conventions {
            table.add_row(vec![Cell::new(convention)]);
        }
        lines.push(table.to_string());
    }
    Ok(RenderedCommand::success(json!(payload), lines))
}
