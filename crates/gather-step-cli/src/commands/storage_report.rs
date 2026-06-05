use std::path::{Path, PathBuf};

use anyhow::Result;
use clap::Args;
use comfy_table::{Cell, ContentArrangement, Table, presets::UTF8_BORDERS_ONLY};
use console::style;
use gather_step_storage::{
    GraphStoreDb, StorageFootprintReport, storage_footprint_report,
    storage_footprint_report_with_open_graph,
};
use serde::Serialize;

use crate::{
    app::AppContext, command_render::RenderedCommand, daemon_protocol::DaemonRequest, daemon_proxy,
};

#[derive(Debug, Args, Default)]
pub struct StorageReportArgs {
    #[arg(long, help = "Read storage artifacts from this directory")]
    pub storage: Option<PathBuf>,
}

#[derive(Debug, Serialize)]
struct StorageReportOutput<'a> {
    event: &'static str,
    #[serde(flatten)]
    report: &'a StorageFootprintReport,
}

pub fn run(app: &AppContext, args: StorageReportArgs) -> Result<()> {
    let Some(storage_root) = args.storage else {
        return daemon_proxy::run_read_only_command(app, &DaemonRequest::StorageReport, |app| {
            run_rendered(&app.workspace_paths().storage_root)
        });
    };

    run_rendered(&storage_root)?.emit(&app.output())
}

pub(crate) fn run_rendered(storage_root: &Path) -> Result<RenderedCommand> {
    let report = storage_footprint_report(storage_root)?;
    render_report(&report)
}

pub(crate) fn run_rendered_with_open_graph(
    storage_root: &Path,
    graph: &GraphStoreDb,
) -> Result<RenderedCommand> {
    let report = storage_footprint_report_with_open_graph(storage_root, graph)?;
    render_report(&report)
}

fn render_report(report: &StorageFootprintReport) -> Result<RenderedCommand> {
    let payload = StorageReportOutput {
        event: "storage_report_completed",
        report,
    };
    let mut lines = Vec::new();

    lines.push(format!(
        "{} {} ({})",
        style("Storage report:").bold(),
        report.storage_root,
        format_bytes(report.total_bytes)
    ));
    lines.push(String::new());
    lines.push(render_components(report));
    if !report.sqlite_objects.is_empty() {
        lines.push(String::new());
        lines.push(render_sqlite_objects(report));
    }
    if !report.graph_tables.is_empty() {
        lines.push(String::new());
        lines.push(render_graph_tables(report));
    }
    for warning in &report.warnings {
        lines.push(format!("{} {warning}", style("Warning:").yellow().bold()));
    }

    RenderedCommand::success_serialized(&payload, lines)
}

fn render_components(report: &StorageFootprintReport) -> String {
    let mut table = Table::new();
    table.load_preset(UTF8_BORDERS_ONLY);
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(["Component", "Kind", "Bytes", "Path"]);
    for component in &report.components {
        table.add_row([
            Cell::new(&component.name),
            Cell::new(&component.store_kind),
            Cell::new(format_bytes(component.bytes)),
            Cell::new(&component.path),
        ]);
    }
    table.to_string()
}

fn render_sqlite_objects(report: &StorageFootprintReport) -> String {
    let mut table = Table::new();
    table.load_preset(UTF8_BORDERS_ONLY);
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(["SQLite object", "Type", "Bytes", "Pages"]);
    for object in report.sqlite_objects.iter().take(20) {
        table.add_row([
            Cell::new(&object.name),
            Cell::new(&object.object_type),
            Cell::new(format_bytes(object.bytes)),
            Cell::new(object.pages),
        ]);
    }
    table.to_string()
}

fn render_graph_tables(report: &StorageFootprintReport) -> String {
    let mut table = Table::new();
    table.load_preset(UTF8_BORDERS_ONLY);
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header([
        "redb table",
        "Kind",
        "Entries",
        "Stored",
        "Metadata",
        "Fragmented",
    ]);
    for table_stats in report.graph_tables.iter().take(20) {
        table.add_row([
            Cell::new(&table_stats.name),
            Cell::new(&table_stats.table_kind),
            Cell::new(table_stats.entries),
            Cell::new(format_bytes(table_stats.stored_bytes)),
            Cell::new(format_bytes(table_stats.metadata_bytes)),
            Cell::new(format_bytes(table_stats.fragmented_bytes)),
        ]);
    }
    table.to_string()
}

#[expect(
    clippy::cast_precision_loss,
    reason = "byte counts are formatted for display only; lossy conversion to f64 is acceptable"
)]
fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}
