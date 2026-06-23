pub mod clean;
pub mod compact;
pub mod conventions;
pub mod cross_repo_deps;
pub mod deployment_topology;
pub mod doctor;
pub mod events;
pub mod generate;
pub mod impact;
pub mod index;
pub mod init;
pub mod log;
pub mod no_args;
pub mod pack;
pub mod pr_review;
pub mod projection_impact;
pub mod qa_evidence;
pub mod reindex;
pub mod search;
pub mod serve;
pub mod setup_mcp;
pub mod status;
pub mod storage_report;
pub mod trace;
pub mod tui;
pub mod watch;
pub mod who_consumes;

use std::{panic::PanicHookInfo, path::PathBuf, process::ExitCode};

use anyhow::{Result, bail};
use clap::{
    ArgAction, Args, Parser, Subcommand,
    builder::styling::{AnsiColor, Effects, Styles},
};
use gather_step_core::capture_rss;
use gather_step_storage::{TelemetryErrorEvent, TelemetryRun, TelemetryRunFinish, TelemetryStore};
use serde_json::json;
use tracing::warn;

use crate::{
    app::{self, AppContext, ColorModeArg},
    errors::graph_lock_contention,
};

/// `--version` long form. Concatenated at compile time from the package
/// version, the current copyright year, and the canonical author so the
/// binary surface keeps the attribution that earlier releases shipped.
const VERSION_LONG: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    "\nCopyright (c) 2026 JJ Adonis. Licensed under the MIT License.",
);

/// Canonical catalog of user-visible CLI subcommands.
///
/// Generated docs (`CLAUDE.gather.md`, `AGENTS.gather.md`) read from this
/// list, so adding or renaming a subcommand needs to be reflected here.
/// Hidden commands (`mcp`) and arg-less invocations (`no-args`) are
/// intentionally omitted.
pub const CLI_COMMANDS: &[(&str, &str)] = &[
    (
        "init",
        "Discover repos, write a config, and run the setup wizard",
    ),
    ("index", "Index configured repos into the workspace graph"),
    ("reindex", "Re-index repos with full or selective coverage"),
    (
        "watch",
        "Watch repos for changes and incrementally re-index",
    ),
    ("clean", "Remove indexed state and storage artifacts"),
    ("compact", "Compact storage in place to reclaim space"),
    ("status", "Show indexing status and counts per repo"),
    ("storage-report", "Print storage size and segment breakdown"),
    ("doctor", "Run health checks against the workspace"),
    ("log", "Inspect local run and error telemetry"),
    ("search", "Search indexed symbols, files, and concepts"),
    ("trace", "Trace impact, events, or routes from a target"),
    ("impact", "Inspect change-impact for a symbol or file"),
    (
        "cross-repo-deps",
        "Inspect cross-repo dependency edges per configured repo",
    ),
    (
        "who-consumes",
        "Find which repos consume what a symbol's file produces",
    ),
    (
        "projection-impact",
        "Trace projected fields, filters, and backfill evidence",
    ),
    (
        "deployment-topology",
        "Inspect deployment topology and shared infra",
    ),
    ("events", "Inspect events, queues, and orphan topics"),
    ("conventions", "Summarize detected workspace conventions"),
    (
        "pack",
        "Render task / planning / debug / review context packs",
    ),
    (
        "qa-evidence",
        "Emit canonical code-evidence metadata for QA planning",
    ),
    (
        "generate",
        "Generate AI docs (claude-md, agents-md, codeowners)",
    ),
    ("setup-mcp", "Register gather-step as an MCP server"),
    ("serve", "Run the long-lived JSON-API server"),
    (
        "pr-review",
        "Build a disposable PR-scoped review and emit the delta report",
    ),
    ("tui", "Launch the interactive terminal UI"),
];

#[derive(Debug, Parser)]
#[command(
    name = "gather-step",
    version,
    long_version = VERSION_LONG,
    about = "Workspace indexing and code graph CLI",
    styles = cli_styles()
)]
pub struct Cli {
    #[arg(long, global = true, default_value = ".", help = "Workspace root path")]
    pub workspace: std::path::PathBuf,
    #[arg(
        long,
        global = true,
        help = "Restrict the command to one configured repo"
    )]
    pub repo: Option<String>,
    #[arg(short = 'v', long = "verbose", action = ArgAction::Count, global = true, help = "Increase logging verbosity")]
    pub verbose: u8,
    #[arg(long, global = true, help = "Emit newline-delimited JSON output")]
    pub json: bool,
    #[arg(
        long,
        global = true,
        value_enum,
        default_value_t = ColorModeArg::Auto,
        help = "Control ANSI color output"
    )]
    pub color: ColorModeArg,
    #[arg(long, global = true, help = "Disable the startup banner")]
    pub no_banner: bool,
    #[arg(
        long,
        global = true,
        help = "Disable interactive prompts (forces all defaults)"
    )]
    pub no_interactive: bool,
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Init(init::InitArgs),
    Index(index::IndexArgs),
    Clean(clean::CleanArgs),
    Compact(compact::CompactArgs),
    Reindex(reindex::ReindexArgs),
    Search(search::SearchArgs),
    Trace(trace::TraceArgs),
    Serve(serve::ServeArgs),
    Watch(watch::WatchArgs),
    Tui(tui::TuiArgs),
    SetupMcp(setup_mcp::SetupMcpArgs),
    Status(status::StatusArgs),
    #[command(name = "storage-report")]
    StorageReport(storage_report::StorageReportArgs),
    Doctor(doctor::DoctorArgs),
    Log(log::LogArgs),
    Generate(generate::GenerateCommand),
    Impact(impact::ImpactArgs),
    #[command(name = "cross-repo-deps", visible_alias = "cross_repo_deps")]
    CrossRepoDeps(cross_repo_deps::CrossRepoDepsArgs),
    #[command(name = "who-consumes", visible_alias = "who_consumes")]
    WhoConsumes(who_consumes::WhoConsumesArgs),
    ProjectionImpact(projection_impact::ProjectionImpactArgs),
    DeploymentTopology(deployment_topology::DeploymentTopologyArgs),
    #[command(name = "qa-evidence")]
    QaEvidence(qa_evidence::QaEvidenceArgs),
    Pack(pack::PackArgs),
    Events(events::EventsArgs),
    Conventions(conventions::ConventionsArgs),
    #[command(name = "pr-review")]
    PrReview(pr_review::PrReviewArgs),
    #[command(hide = true)]
    Mcp(McpCommand),
}

#[derive(Debug, Args)]
pub struct McpCommand {
    #[command(subcommand)]
    pub command: McpSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum McpSubcommand {
    Serve(serve::ServeArgs),
}

/// User-visible command outcome.
///
/// Errors still propagate as `Err` and are mapped to exit 1 in `main`.
/// `ReviewThresholdExceeded` maps to exit 2 so CI can distinguish "tool broke"
/// from "`pr-review` found high-severity changes."
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliOutcome {
    Success,
    ReviewThresholdExceeded,
}

impl CliOutcome {
    #[must_use]
    pub fn exit_code(self) -> ExitCode {
        match self {
            Self::Success => ExitCode::from(0),
            Self::ReviewThresholdExceeded => ExitCode::from(2),
        }
    }

    fn from_pr_review_code(code: u8) -> Result<Self> {
        match code {
            0 => Ok(Self::Success),
            2 => Ok(Self::ReviewThresholdExceeded),
            other => bail!("The `pr-review` command returned an unexpected exit code: {other}."),
        }
    }
}

fn success(result: Result<()>) -> Result<CliOutcome> {
    result.map(|()| CliOutcome::Success)
}

/// Run the dispatched subcommand and return the user-visible outcome.
pub async fn run(cli: Cli, app: AppContext) -> Result<CliOutcome> {
    let command_name = command_telemetry_name(cli.command.as_ref());
    app::reset_telemetry_run_state();
    let workspace_path = app.workspace_path.clone();
    let telemetry = open_telemetry_store();
    let run = telemetry.as_ref().and_then(|store| {
        match store.begin_run(
            command_name,
            &workspace_path,
            env!("CARGO_PKG_VERSION"),
            &telemetry_schema_versions(),
        ) {
            Ok(run) => Some(run),
            Err(error) => {
                warn!(%error, "failed to write telemetry run start");
                None
            }
        }
    });

    if let (Some(store), Some(run)) = (&telemetry, &run) {
        install_telemetry_panic_hook(store.clone(), run.clone());
    }

    let outcome = run_inner(cli, app).await;
    if let (Some(store), Some(run)) = (&telemetry, &run) {
        let finish = telemetry_finish_fields(&outcome);
        if let Err(error) = store.finish_run(run, &finish) {
            warn!(%error, "failed to write telemetry run finish");
        }
    }
    outcome
}

async fn run_inner(cli: Cli, app: AppContext) -> Result<CliOutcome> {
    match cli.command {
        Some(Command::Init(args)) => success(init::run(&app, args).await),
        Some(Command::Index(args)) => success(index::run(&app, args).await),
        Some(Command::Clean(args)) => success(clean::run(&app, args)),
        Some(Command::Compact(args)) => success(compact::run(&app, args)),
        Some(Command::Reindex(args)) => success(reindex::run(&app, args).await),
        Some(Command::Serve(args)) => success(serve::run(&app, args).await),
        Some(Command::Watch(args)) => success(watch::run(&app, args).await),
        Some(Command::Tui(args)) => success(tui::run(&app, args)),
        Some(Command::Search(args)) => success(search::run(&app, args)),
        Some(Command::Trace(args)) => success(trace::run(&app, args)),
        Some(Command::SetupMcp(args)) => success(setup_mcp::run(&app, args)),
        Some(Command::Status(args)) => success(status::run(&app, args)),
        Some(Command::StorageReport(args)) => success(storage_report::run(&app, args)),
        Some(Command::Doctor(args)) => success(doctor::run(&app, args)),
        Some(Command::Log(args)) => success(log::run(&app, &args)),
        Some(Command::Generate(command)) => success(generate::run(&app, command)),
        Some(Command::Impact(args)) => success(impact::run(&app, args)),
        Some(Command::CrossRepoDeps(args)) => success(cross_repo_deps::run(&app, &args)),
        Some(Command::WhoConsumes(args)) => success(who_consumes::run(&app, &args)),
        Some(Command::ProjectionImpact(args)) => success(projection_impact::run(&app, args)),
        Some(Command::DeploymentTopology(args)) => success(deployment_topology::run(&app, args)),
        Some(Command::QaEvidence(args)) => success(qa_evidence::run(&app, &args)),
        Some(Command::Pack(args)) => success(pack::run(&app, &args)),
        Some(Command::Events(args)) => success(events::run(&app, args)),
        Some(Command::Conventions(args)) => success(conventions::run(&app, args)),
        Some(Command::PrReview(args)) => {
            CliOutcome::from_pr_review_code(pr_review::run(&app, args)?)
        }
        Some(Command::Mcp(command)) => match command.command {
            McpSubcommand::Serve(args) => success(serve::run(&app, args).await),
        },
        None => success(no_args::run(&app).await),
    }
}

fn open_telemetry_store() -> Option<TelemetryStore> {
    let Some(telemetry_root) = telemetry_root() else {
        warn!("failed to locate data directory for telemetry store");
        return None;
    };
    match TelemetryStore::open(&telemetry_root) {
        Ok(store) => Some(store),
        Err(error) => {
            warn!(%error, "failed to open telemetry store");
            None
        }
    }
}

fn command_telemetry_name(command: Option<&Command>) -> &'static str {
    match command {
        Some(Command::Init(_)) => "init",
        Some(Command::Index(_)) => "index",
        Some(Command::Clean(_)) => "clean",
        Some(Command::Compact(_)) => "compact",
        Some(Command::Reindex(_)) => "reindex",
        Some(Command::Search(_)) => "search",
        Some(Command::Trace(_)) => "trace",
        Some(Command::Serve(_)) => "serve",
        Some(Command::Watch(_)) => "watch",
        Some(Command::Tui(_)) => "tui",
        Some(Command::SetupMcp(_)) => "setup-mcp",
        Some(Command::Status(_)) => "status",
        Some(Command::StorageReport(_)) => "storage-report",
        Some(Command::Doctor(_)) => "doctor",
        Some(Command::Log(_)) => "log",
        Some(Command::Generate(_)) => "generate",
        Some(Command::Impact(_)) => "impact",
        Some(Command::CrossRepoDeps(_)) => "cross-repo-deps",
        Some(Command::WhoConsumes(_)) => "who-consumes",
        Some(Command::ProjectionImpact(_)) => "projection-impact",
        Some(Command::DeploymentTopology(_)) => "deployment-topology",
        Some(Command::QaEvidence(_)) => "qa-evidence",
        Some(Command::Pack(_)) => "pack",
        Some(Command::Events(_)) => "events",
        Some(Command::Conventions(_)) => "conventions",
        Some(Command::PrReview(_)) => "pr-review",
        Some(Command::Mcp(_)) => "mcp",
        None => "no-args",
    }
}

fn telemetry_finish_fields(result: &Result<CliOutcome>) -> TelemetryRunFinish {
    let (warn_count, traced_error_count) = app::telemetry_counts();
    let (exit_status, error) = match result {
        Ok(CliOutcome::Success) => ("success".to_owned(), None),
        Ok(CliOutcome::ReviewThresholdExceeded) => ("review_threshold_exceeded".to_owned(), None),
        Err(error) => {
            let category = telemetry_error_category(error);
            (
                "error".to_owned(),
                Some(TelemetryErrorEvent {
                    level: "ERROR".to_owned(),
                    category: category.to_owned(),
                    message: error.to_string(),
                    context_json: None,
                }),
            )
        }
    };
    let explicit_error_count = u32::from(error.is_some());
    TelemetryRunFinish {
        exit_status,
        peak_rss_bytes: capture_rss(),
        warn_count,
        error_count: traced_error_count.saturating_add(explicit_error_count),
        recovery_event: app::telemetry_recovery_event(),
        error,
        ..TelemetryRunFinish::default()
    }
}

pub(crate) fn telemetry_root() -> Option<PathBuf> {
    dirs::data_local_dir().map(|root| root.join("gather-step"))
}

fn telemetry_schema_versions() -> serde_json::Value {
    json!({
        "telemetry": gather_step_storage::telemetry::TELEMETRY_SCHEMA_VERSION,
        "graph": gather_step_storage::graph_store::GRAPH_SCHEMA_VERSION,
        "metadata": gather_step_storage::metadata::METADATA_SCHEMA_VERSION,
    })
}

fn telemetry_error_category(error: &anyhow::Error) -> &'static str {
    if graph_lock_contention(error) {
        return "graph_lock_contention";
    }
    let message = error.to_string();
    if contains_ascii_case_insensitive(&message, "schema")
        && contains_ascii_case_insensitive(&message, "version")
    {
        "schema_mismatch"
    } else if contains_ascii_case_insensitive(&message, "git") {
        "git_error"
    } else if contains_ascii_case_insensitive(&message, "storage")
        || contains_ascii_case_insensitive(&message, "sqlite")
        || contains_ascii_case_insensitive(&message, "redb")
    {
        "storage_io"
    } else if contains_ascii_case_insensitive(&message, "config") {
        "config_invalid"
    } else if contains_ascii_case_insensitive(&message, "network")
        || contains_ascii_case_insensitive(&message, "http")
        || contains_ascii_case_insensitive(&message, "github")
        || contains_ascii_case_insensitive(&message, "jira")
    {
        "network"
    } else if contains_ascii_case_insensitive(&message, "parse") {
        "parse_failure"
    } else if contains_ascii_case_insensitive(&message, "auto-recover")
        || contains_ascii_case_insensitive(&message, "auto recovered")
    {
        "auto_recovered"
    } else {
        "unknown"
    }
}

fn contains_ascii_case_insensitive(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle.as_bytes()))
}

fn install_telemetry_panic_hook(store: TelemetryStore, run: TelemetryRun) {
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let message = panic_message(info);
        if let Err(error) = store.mark_panic(&run, "panic", &message) {
            warn!(%error, "failed to write telemetry panic event");
        }
        previous_hook(info);
    }));
}

fn panic_message(info: &PanicHookInfo<'_>) -> String {
    let payload = if let Some(message) = info.payload().downcast_ref::<&str>() {
        *message
    } else if let Some(message) = info.payload().downcast_ref::<String>() {
        message.as_str()
    } else {
        "non-string panic payload"
    };
    if let Some(location) = info.location() {
        format!("{payload} at {}:{}", location.file(), location.line())
    } else {
        payload.to_owned()
    }
}

fn cli_styles() -> Styles {
    Styles::styled()
        .header(AnsiColor::Green.on_default() | Effects::BOLD)
        .usage(AnsiColor::Green.on_default() | Effects::BOLD)
        .literal(AnsiColor::Cyan.on_default() | Effects::BOLD)
        .placeholder(AnsiColor::Cyan.on_default())
        .error(AnsiColor::Red.on_default() | Effects::BOLD)
        .valid(AnsiColor::Green.on_default())
        .invalid(AnsiColor::Red.on_default() | Effects::BOLD)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, path::Path};

    use clap::{CommandFactory, Parser};
    use pretty_assertions::assert_eq;

    use super::{CLI_COMMANDS, Cli, Command};
    use crate::commands::{
        clean::CleanArgs, compact::CompactArgs, index::IndexArgs,
        projection_impact::EvidenceVerbosityArg, reindex::ReindexArgs, serve::ServeArgs,
        setup_mcp::McpScope, trace::TraceCommand, tui::TuiArgs, watch::WatchArgs,
    };

    #[test]
    fn cli_commands_catalog_matches_visible_subcommands() {
        let rendered_commands = CLI_COMMANDS
            .iter()
            .map(|(name, _)| (*name).to_owned())
            .collect::<BTreeSet<_>>();
        let clap_commands = Cli::command()
            .get_subcommands()
            .filter(|command| command.get_name() != "mcp")
            .map(|command| command.get_name().to_owned())
            .collect::<BTreeSet<_>>();

        assert_eq!(rendered_commands, clap_commands);
    }

    #[test]
    fn parses_index_args_with_global_flags() {
        let cli = Cli::parse_from([
            "gather-step",
            "--workspace",
            "/tmp/ws",
            "--repo",
            "backend_standard",
            "--json",
            "index",
            "--config",
            "custom.yaml",
            "--storage",
            ".gather-step/storage",
        ]);

        assert_eq!(cli.workspace, std::path::PathBuf::from("/tmp/ws"));
        assert_eq!(cli.repo.as_deref(), Some("backend_standard"));
        assert_eq!(cli.json, true);

        let Some(Command::Index(args)) = cli.command else {
            unreachable!("expected index command");
        };

        assert_eq!(
            args,
            IndexArgs {
                config: Some("custom.yaml".into()),
                registry: None,
                storage: Some(".gather-step/storage".into()),
                depth: None,
                artifact_path: None,
                release_gate: false,
                auto_recover: false,
                watch: false,
                force_unlock: false,
                lock_timeout: None,
            }
        );
    }

    #[test]
    fn parses_storage_report_command() {
        let cli = Cli::parse_from([
            "gather-step",
            "--json",
            "storage-report",
            "--storage",
            "/tmp/gather-step-storage",
        ]);

        assert!(cli.json);
        match cli.command {
            Some(Command::StorageReport(args)) => {
                assert_eq!(
                    args.storage.as_deref(),
                    Some(Path::new("/tmp/gather-step-storage"))
                );
            }
            other => panic!("expected storage-report command, got {other:?}"),
        }
    }

    #[test]
    fn parses_projection_impact_args_with_bounded_limit() {
        let cli = Cli::parse_from([
            "gather-step",
            "--repo",
            "backend_standard",
            "projection-impact",
            "--target",
            "subtaskIds",
            "--limit",
            "25",
            "--evidence-verbosity",
            "summary",
        ]);

        let Some(Command::ProjectionImpact(args)) = cli.command else {
            unreachable!("expected projection-impact command");
        };

        assert_eq!(args.target, "subtaskIds");
        assert_eq!(args.limit, 25);
        assert_eq!(args.evidence_verbosity, EvidenceVerbosityArg::Summary);
    }

    #[test]
    fn rejects_projection_impact_limit_outside_supported_range() {
        for limit in ["0", "101", "many"] {
            let error = Cli::try_parse_from([
                "gather-step",
                "projection-impact",
                "--target",
                "subtaskIds",
                "--limit",
                limit,
            ])
            .expect_err("unsupported limit should be rejected");

            assert!(
                error
                    .to_string()
                    .contains("limit must be an integer between 1 and 100"),
                "unexpected error for {limit}: {error}"
            );
        }
    }

    #[test]
    fn rejects_zero_pr_review_parallelism_during_parse() {
        let error = Cli::try_parse_from([
            "gather-step",
            "pr-review",
            "--pr-set",
            "examples/pr-set/cross-repo-feature.yaml",
            "--parallelism",
            "0",
        ])
        .expect_err("zero parallelism should be rejected by clap");

        assert!(
            error
                .to_string()
                .contains("--parallelism must be an integer greater than or equal to 1"),
            "unexpected error for zero parallelism: {error}"
        );
    }

    #[test]
    fn parses_top_level_serve_args() {
        let cli = Cli::parse_from([
            "gather-step",
            "serve",
            "--graph",
            "graph.redb",
            "--registry",
            "registry.json",
            "--max-limit",
            "250",
            "--server-name",
            "local-graph",
        ]);

        let Some(Command::Serve(args)) = cli.command else {
            unreachable!("expected serve command");
        };

        assert_eq!(
            args,
            ServeArgs {
                graph: Some("graph.redb".into()),
                registry: Some("registry.json".into()),
                max_limit: 250,
                server_name: "local-graph".to_owned(),
                config: None,
                watch: false,
                poll_interval_ms: 250,
                debounce_ms: 2000,
                consecutive_error_limit: 5,
                error_backoff_ms: 5000,
                trace_tool_calls: None,
            }
        );
    }

    #[test]
    fn parses_reindex_args() {
        let cli = Cli::parse_from([
            "gather-step",
            "--workspace",
            "/tmp/ws",
            "reindex",
            "--config",
            "custom.yaml",
            "--registry",
            "state/registry.json",
            "--storage",
            "state/storage",
            "--depth",
            "level2",
        ]);

        assert_eq!(cli.workspace, std::path::PathBuf::from("/tmp/ws"));

        let Some(Command::Reindex(args)) = cli.command else {
            unreachable!("expected reindex command");
        };

        assert_eq!(
            args,
            ReindexArgs {
                index: IndexArgs {
                    config: Some("custom.yaml".into()),
                    registry: Some("state/registry.json".into()),
                    storage: Some("state/storage".into()),
                    depth: Some(crate::app::DepthArg::Level2),
                    artifact_path: None,
                    release_gate: false,
                    auto_recover: false,
                    watch: false,
                    force_unlock: false,
                    lock_timeout: None,
                },
            }
        );
    }

    #[test]
    fn parses_clean_args() {
        let cli = Cli::parse_from([
            "gather-step",
            "--workspace",
            "/tmp/ws",
            "clean",
            "--registry",
            "state/registry.json",
            "--storage",
            "state/storage",
            "--yes",
        ]);

        assert_eq!(cli.workspace, std::path::PathBuf::from("/tmp/ws"));

        let Some(Command::Clean(args)) = cli.command else {
            unreachable!("expected clean command");
        };

        assert_eq!(
            args,
            CleanArgs {
                registry: Some("state/registry.json".into()),
                storage: Some("state/storage".into()),
                yes: true,
                include_review: false,
            }
        );
    }

    #[test]
    fn parses_compact_args() {
        let cli = Cli::parse_from([
            "gather-step",
            "--workspace",
            "/tmp/ws",
            "compact",
            "--storage",
            "state/storage",
        ]);

        assert_eq!(cli.workspace, std::path::PathBuf::from("/tmp/ws"));

        let Some(Command::Compact(args)) = cli.command else {
            unreachable!("expected compact command");
        };

        assert_eq!(
            args,
            CompactArgs {
                storage: Some("state/storage".into()),
            }
        );
    }

    #[test]
    fn parses_watch_args() {
        let cli = Cli::parse_from([
            "gather-step",
            "watch",
            "--config",
            "workspace.yaml",
            "--storage",
            ".gather-step/storage",
            "--poll-interval-ms",
            "500",
            "--debounce-ms",
            "1500",
            "--consecutive-error-limit",
            "3",
            "--error-backoff-ms",
            "9000",
        ]);

        let Some(Command::Watch(args)) = cli.command else {
            unreachable!("expected watch command");
        };
        assert_eq!(
            args,
            WatchArgs {
                count: None,
                config: Some("workspace.yaml".into()),
                storage: Some(".gather-step/storage".into()),
                poll_interval_ms: 500,
                debounce_ms: 1500,
                consecutive_error_limit: 3,
                error_backoff_ms: 9000,
            }
        );
    }

    #[test]
    fn parses_watch_count_arg() {
        let cli = Cli::parse_from(["gather-step", "watch", "3"]);

        let Some(Command::Watch(args)) = cli.command else {
            unreachable!("expected watch command");
        };
        assert_eq!(
            args,
            WatchArgs {
                count: Some(3),
                config: None,
                storage: None,
                poll_interval_ms: 250,
                debounce_ms: 2000,
                consecutive_error_limit: 5,
                error_backoff_ms: 5000,
            }
        );
    }

    #[test]
    fn parses_tui_args() {
        let cli = Cli::parse_from(["gather-step", "tui"]);

        let Some(Command::Tui(args)) = cli.command else {
            unreachable!("expected tui command");
        };
        assert_eq!(args, TuiArgs {});
    }

    #[test]
    fn parses_trace_crud_args() {
        let cli = Cli::parse_from([
            "gather-step",
            "trace",
            "crud",
            "--method",
            "POST",
            "--path",
            "/orders",
            "--limit",
            "12",
        ]);

        let Some(Command::Trace(args)) = cli.command else {
            unreachable!("expected trace command");
        };

        match args.command {
            TraceCommand::Crud(crud_args) => {
                assert_eq!(crud_args.path.as_deref(), Some("/orders"));
                assert_eq!(crud_args.method.as_deref(), Some("POST"));
                assert_eq!(crud_args.symbol_id, None);
                assert_eq!(crud_args.limit, 12);
            }
        }
    }

    #[test]
    fn parses_trace_crud_symbol_entry_args() {
        let cli = Cli::parse_from([
            "gather-step",
            "trace",
            "crud",
            "--symbol-id",
            "deadbeefdeadbeefdeadbeefdeadbeef",
        ]);

        let Some(Command::Trace(args)) = cli.command else {
            unreachable!("expected trace command");
        };

        match args.command {
            TraceCommand::Crud(crud_args) => {
                assert_eq!(
                    crud_args.symbol_id.as_deref(),
                    Some("deadbeefdeadbeefdeadbeefdeadbeef")
                );
                assert_eq!(crud_args.method, None);
                assert_eq!(crud_args.path, None);
            }
        }
    }

    #[test]
    fn parses_generate_codeowners_args() {
        let cli = Cli::parse_from([
            "gather-step",
            "generate",
            "codeowners",
            "--output",
            "CODEOWNERS",
        ]);

        let Some(Command::Generate(command)) = cli.command else {
            unreachable!("expected generate command");
        };

        match command.command {
            crate::commands::generate::GenerateSubcommand::Codeowners(args) => {
                assert_eq!(args.output, Some("CODEOWNERS".into()));
            }
            crate::commands::generate::GenerateSubcommand::ClaudeMd(_)
            | crate::commands::generate::GenerateSubcommand::AgentsMd(_) => {
                panic!("expected codeowners subcommand")
            }
        }
    }

    #[test]
    fn parses_events_orphans_subcommand() {
        let cli = Cli::parse_from(["gather-step", "events", "orphans", "--limit", "12"]);

        let Some(Command::Events(args)) = cli.command else {
            unreachable!("expected events command");
        };

        match args.command {
            crate::commands::events::EventsCommand::Orphans(orphan_args) => {
                assert_eq!(orphan_args.limit, 12);
            }
            _ => panic!("expected orphans subcommand"),
        }
    }

    #[test]
    fn parses_setup_mcp_scope() {
        let cli = Cli::parse_from(["gather-step", "setup-mcp", "--scope", "global"]);

        let Some(Command::SetupMcp(args)) = cli.command else {
            unreachable!("expected setup-mcp command");
        };

        assert!(matches!(args.scope, McpScope::Global));
    }
}
