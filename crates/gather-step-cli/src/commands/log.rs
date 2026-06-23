use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use clap::Args;
use comfy_table::{Cell, ContentArrangement, Table, presets::UTF8_BORDERS_ONLY};
use gather_step_storage::{TelemetryRunRecord, TelemetryStore, telemetry::TELEMETRY_DB_NAME};
use serde::Serialize;

use crate::app::AppContext;

#[derive(Debug, Args)]
pub struct LogArgs {
    #[arg(
        long,
        default_value_t = 20,
        help = "Maximum number of run rows to show"
    )]
    pub last: usize,
    #[arg(long, help = "Only show runs since an age such as 7d")]
    pub since: Option<String>,
    #[arg(long, help = "Only show runs with errors or non-success status")]
    pub errors_only: bool,
    #[arg(long, help = "Delete telemetry rows older than an age such as 90d")]
    pub clear_before: Option<String>,
}

#[derive(Debug, Serialize)]
struct LogOutput {
    event: &'static str,
    telemetry_db: String,
    cleared_rows: usize,
    records: Vec<TelemetryRunRecord>,
}

pub fn run(app: &AppContext, args: LogArgs) -> Result<()> {
    let Some(root) = super::telemetry_root() else {
        bail!("Could not locate the user data directory for telemetry.");
    };
    let store = TelemetryStore::open(&root).context("opening telemetry database")?;

    let cleared_rows = if let Some(age) = args.clear_before.as_deref() {
        let cutoff = cutoff_from_age(age)?;
        store
            .clear_before(cutoff)
            .with_context(|| format!("clearing telemetry rows older than {age}"))?
    } else {
        0
    };
    let since_ms = args
        .since
        .as_deref()
        .map(cutoff_from_age)
        .transpose()
        .context("parsing --since")?;
    let records = store
        .list_runs(args.last, since_ms, args.errors_only)
        .context("listing telemetry runs")?;
    let output = LogOutput {
        event: "log_completed",
        telemetry_db: TELEMETRY_DB_NAME.to_owned(),
        cleared_rows,
        records,
    };

    if app.json_output {
        app.output().emit(&output)?;
        return Ok(());
    }

    if output.cleared_rows > 0 {
        app.output()
            .line(format!("Cleared {} telemetry row(s).", output.cleared_rows));
    }
    if output.records.is_empty() {
        app.output().line("No telemetry runs found.");
        return Ok(());
    }

    let mut table = Table::new();
    table.load_preset(UTF8_BORDERS_ONLY);
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(vec![
        "Started", "Command", "Status", "Duration", "RSS", "Warn", "Err", "Recovery",
    ]);
    for record in &output.records {
        table.add_row(vec![
            Cell::new(format_ms(record.started_at_ms)),
            Cell::new(&record.command),
            Cell::new(&record.exit_status),
            Cell::new(format_duration(record.duration_ms)),
            Cell::new(format_bytes(record.peak_rss_bytes)),
            Cell::new(record.warn_count),
            Cell::new(record.error_count),
            Cell::new(if record.recovery_event { "yes" } else { "-" }),
        ]);
    }
    app.output().line(table.to_string());
    Ok(())
}

fn cutoff_from_age(value: &str) -> Result<i64> {
    let days = value
        .strip_suffix('d')
        .unwrap_or(value)
        .parse::<i64>()
        .with_context(|| format!("expected an age in days like 7d, got `{value}`"))?;
    if days < 0 {
        bail!("Age must be non-negative, got `{value}`.");
    }
    Ok(now_ms().saturating_sub(days.saturating_mul(24 * 60 * 60 * 1000)))
}

fn now_ms() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis());
    i64::try_from(millis).unwrap_or(i64::MAX)
}

fn format_ms(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms).map_or_else(|| ms.to_string(), |dt| dt.to_rfc3339())
}

fn format_duration(ms: Option<i64>) -> String {
    ms.map_or_else(|| "-".to_owned(), |value| format!("{value}ms"))
}

fn format_bytes(bytes: Option<u64>) -> String {
    let Some(bytes) = bytes else {
        return "-".to_owned();
    };
    if bytes >= 1024 * 1024 {
        format_tenths(bytes, 1024 * 1024, "MiB")
    } else if bytes >= 1024 {
        format_tenths(bytes, 1024, "KiB")
    } else {
        format!("{bytes} B")
    }
}

fn format_tenths(value: u64, unit: u64, suffix: &str) -> String {
    let tenths = value.saturating_mul(10) / unit;
    format!("{}.{:01} {suffix}", tenths / 10, tenths % 10)
}
