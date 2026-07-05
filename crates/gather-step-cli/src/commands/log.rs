use std::collections::BTreeMap;
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
        help = "Maximum number of run rows to consider (also caps the --summary window)"
    )]
    pub last: usize,
    #[arg(long, help = "Only show runs since an age such as 7d")]
    pub since: Option<String>,
    #[arg(long, help = "Only show runs with errors or non-success status")]
    pub errors_only: bool,
    #[arg(long, help = "Only show runs for this command")]
    pub command: Option<String>,
    #[arg(
        long,
        help = "Print an aggregate summary (status, graph availability, slowest commands) instead of rows"
    )]
    pub summary: bool,
    #[arg(
        long,
        help = "Finalize stale `running` rows as `abandoned`, then report how many"
    )]
    pub repair: bool,
    #[arg(long, help = "Delete telemetry rows older than an age such as 90d")]
    pub clear_before: Option<String>,
}

#[derive(Debug, Serialize)]
struct LogOutput {
    event: &'static str,
    telemetry_db: String,
    cleared_rows: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    repaired_stale: Option<usize>,
    records: Vec<TelemetryRunRecord>,
}

#[derive(Debug, Serialize)]
struct CommandDuration {
    command: String,
    runs: usize,
    max_duration_ms: i64,
}

#[derive(Debug, Serialize)]
struct LogSummary {
    event: &'static str,
    telemetry_db: String,
    total_runs: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    repaired_stale: Option<usize>,
    cleared_rows: usize,
    by_status: BTreeMap<String, usize>,
    by_graph_availability: BTreeMap<String, usize>,
    abandoned: usize,
    peak_rss_bytes_max: Option<u64>,
    slowest_commands: Vec<CommandDuration>,
}

const SLOWEST_COMMANDS: usize = 5;

fn summarize(
    records: &[TelemetryRunRecord],
    repaired_stale: Option<usize>,
    cleared_rows: usize,
) -> LogSummary {
    let mut by_status: BTreeMap<String, usize> = BTreeMap::new();
    let mut by_graph_availability: BTreeMap<String, usize> = BTreeMap::new();
    let mut max_by_command: BTreeMap<String, (usize, i64)> = BTreeMap::new();
    let mut peak_rss_bytes_max: Option<u64> = None;

    for record in records {
        *by_status.entry(record.exit_status.clone()).or_default() += 1;
        let availability = record
            .graph_availability
            .clone()
            .unwrap_or_else(|| "unknown".to_owned());
        *by_graph_availability.entry(availability).or_default() += 1;
        let entry = max_by_command
            .entry(record.command.clone())
            .or_insert((0, 0));
        entry.0 += 1;
        entry.1 = entry.1.max(record.duration_ms.unwrap_or(0));
        if let Some(rss) = record.peak_rss_bytes {
            peak_rss_bytes_max = Some(peak_rss_bytes_max.map_or(rss, |current| current.max(rss)));
        }
    }

    let abandoned = by_status.get("abandoned").copied().unwrap_or(0);
    let mut slowest_commands: Vec<CommandDuration> = max_by_command
        .into_iter()
        .map(|(command, (runs, max_duration_ms))| CommandDuration {
            command,
            runs,
            max_duration_ms,
        })
        .collect();
    slowest_commands.sort_by(|left, right| {
        right
            .max_duration_ms
            .cmp(&left.max_duration_ms)
            .then_with(|| left.command.cmp(&right.command))
    });
    slowest_commands.truncate(SLOWEST_COMMANDS);

    LogSummary {
        event: "log_summary",
        telemetry_db: TELEMETRY_DB_NAME.to_owned(),
        total_runs: records.len(),
        repaired_stale,
        cleared_rows,
        by_status,
        by_graph_availability,
        abandoned,
        peak_rss_bytes_max,
        slowest_commands,
    }
}

fn emit_summary(
    app: &AppContext,
    records: &[TelemetryRunRecord],
    repaired_stale: Option<usize>,
    cleared_rows: usize,
) -> Result<()> {
    let summary = summarize(records, repaired_stale, cleared_rows);
    if app.json_output {
        app.output().emit(&summary)?;
        return Ok(());
    }

    if let Some(repaired) = summary.repaired_stale {
        app.output()
            .line(format!("Finalized {repaired} stale running row(s)."));
    }
    if summary.total_runs == 0 {
        app.output().line("No telemetry runs found.");
        return Ok(());
    }

    app.output().line(format!(
        "{} run(s); {} abandoned; peak RSS {}",
        summary.total_runs,
        summary.abandoned,
        format_bytes(summary.peak_rss_bytes_max)
    ));

    let mut status_table = Table::new();
    status_table.load_preset(UTF8_BORDERS_ONLY);
    status_table.set_content_arrangement(ContentArrangement::Dynamic);
    status_table.set_header(vec!["Status", "Runs"]);
    for (status, count) in &summary.by_status {
        status_table.add_row(vec![Cell::new(status), Cell::new(count)]);
    }
    app.output().line(status_table.to_string());

    let mut graph_table = Table::new();
    graph_table.load_preset(UTF8_BORDERS_ONLY);
    graph_table.set_content_arrangement(ContentArrangement::Dynamic);
    graph_table.set_header(vec!["Graph availability", "Runs"]);
    for (availability, count) in &summary.by_graph_availability {
        graph_table.add_row(vec![Cell::new(availability), Cell::new(count)]);
    }
    app.output().line(graph_table.to_string());

    let mut command_table = Table::new();
    command_table.load_preset(UTF8_BORDERS_ONLY);
    command_table.set_content_arrangement(ContentArrangement::Dynamic);
    command_table.set_header(vec!["Slowest command", "Runs", "Max duration"]);
    for entry in &summary.slowest_commands {
        command_table.add_row(vec![
            Cell::new(&entry.command),
            Cell::new(entry.runs),
            Cell::new(format_duration(Some(entry.max_duration_ms))),
        ]);
    }
    app.output().line(command_table.to_string());
    Ok(())
}

pub fn run(app: &AppContext, args: &LogArgs) -> Result<()> {
    let Some(root) = super::telemetry_root() else {
        bail!("Could not locate the user data directory for telemetry.");
    };
    let store = TelemetryStore::open(&root).context("opening telemetry database")?;

    let repaired_stale = if args.repair {
        Some(
            store
                .repair_stale_running()
                .context("finalizing stale running rows")?,
        )
    } else {
        None
    };

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
    let mut records = store
        .list_runs(args.last, since_ms, args.errors_only)
        .context("listing telemetry runs")?;
    if let Some(command) = args.command.as_deref() {
        records.retain(|record| record.command == command);
    }

    if args.summary {
        return emit_summary(app, &records, repaired_stale, cleared_rows);
    }

    let output = LogOutput {
        event: "log_completed",
        telemetry_db: TELEMETRY_DB_NAME.to_owned(),
        cleared_rows,
        repaired_stale,
        records,
    };

    if app.json_output {
        app.output().emit(&output)?;
        return Ok(());
    }

    if let Some(repaired) = output.repaired_stale {
        app.output()
            .line(format!("Finalized {repaired} stale running row(s)."));
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
        "Started", "Command", "Version", "Status", "Duration", "RSS", "Warn", "Err", "Recovery",
    ]);
    for record in &output.records {
        table.add_row(vec![
            Cell::new(format_ms(record.started_at_ms)),
            Cell::new(&record.command),
            Cell::new(&record.cli_version),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn record(
        command: &str,
        status: &str,
        duration_ms: i64,
        availability: &str,
    ) -> TelemetryRunRecord {
        TelemetryRunRecord {
            run_id: format!("{command}-{status}-{duration_ms}"),
            started_at_ms: 0,
            ended_at_ms: Some(duration_ms),
            command: command.to_owned(),
            cli_version: "9.9.9".to_owned(),
            exit_status: status.to_owned(),
            duration_ms: Some(duration_ms),
            peak_rss_bytes: Some(2 * 1024 * 1024),
            warn_count: 0,
            error_count: 0,
            recovery_event: false,
            result_count: None,
            graph_availability: Some(availability.to_owned()),
            build_provenance: Some("release".to_owned()),
        }
    }

    #[test]
    fn summarize_aggregates_status_availability_and_slowest_commands() {
        let records = vec![
            record("index", "success", 1200, "available"),
            record("trace", "error", 30, "locked"),
            record("index", "success", 800, "available"),
            record("status", "abandoned", 0, "unknown"),
        ];

        let summary = summarize(&records, Some(1), 0);

        assert_eq!(summary.total_runs, 4);
        assert_eq!(summary.by_status.get("success"), Some(&2));
        assert_eq!(summary.by_status.get("error"), Some(&1));
        assert_eq!(summary.by_status.get("abandoned"), Some(&1));
        assert_eq!(summary.abandoned, 1);
        assert_eq!(summary.by_graph_availability.get("available"), Some(&2));
        assert_eq!(summary.by_graph_availability.get("locked"), Some(&1));
        assert_eq!(summary.repaired_stale, Some(1));
        // `index` is slowest (1200ms) and aggregates both of its runs.
        let slowest = &summary.slowest_commands[0];
        assert_eq!(slowest.command, "index");
        assert_eq!(slowest.runs, 2);
        assert_eq!(slowest.max_duration_ms, 1200);
    }
}
