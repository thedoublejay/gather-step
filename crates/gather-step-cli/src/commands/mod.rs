pub mod clean;
pub mod compact;
pub mod conventions;
pub mod doctor;
pub mod events;
pub mod generate;
pub mod impact;
pub mod index;
pub mod init;
pub mod no_args;
pub mod pack;
pub mod projection_impact;
pub mod reindex;
pub mod search;
pub mod serve;
pub mod setup_mcp;
pub mod status;
pub mod trace;
pub mod tui;
pub mod watch;

use anyhow::Result;
use clap::{
    ArgAction, Args, Parser, Subcommand,
    builder::styling::{AnsiColor, Effects, Styles},
};

use crate::app::{AppContext, ColorModeArg};

#[derive(Debug, Parser)]
#[command(
    name = "gather-step",
    version,
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
    Doctor(doctor::DoctorArgs),
    Generate(generate::GenerateCommand),
    Impact(impact::ImpactArgs),
    ProjectionImpact(projection_impact::ProjectionImpactArgs),
    Pack(pack::PackArgs),
    Events(events::EventsArgs),
    Conventions(conventions::ConventionsArgs),
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

pub async fn run(cli: Cli, app: AppContext) -> Result<()> {
    match cli.command {
        Some(Command::Init(args)) => init::run(&app, args).await,
        Some(Command::Index(args)) => index::run(&app, args).await,
        Some(Command::Clean(args)) => clean::run(&app, args),
        Some(Command::Compact(args)) => compact::run(&app, args),
        Some(Command::Reindex(args)) => reindex::run(&app, args).await,
        Some(Command::Serve(args)) => serve::run(&app, args).await,
        Some(Command::Watch(args)) => watch::run(&app, args).await,
        Some(Command::Tui(args)) => tui::run(&app, args),
        Some(Command::Search(args)) => search::run(&app, args),
        Some(Command::Trace(args)) => trace::run(&app, args),
        Some(Command::SetupMcp(args)) => setup_mcp::run(&app, args),
        Some(Command::Status(args)) => status::run(&app, args),
        Some(Command::Doctor(args)) => doctor::run(&app, args),
        Some(Command::Generate(command)) => generate::run(&app, command),
        Some(Command::Impact(args)) => impact::run(&app, args),
        Some(Command::ProjectionImpact(args)) => projection_impact::run(&app, args),
        Some(Command::Pack(args)) => pack::run(&app, &args),
        Some(Command::Events(args)) => events::run(&app, args),
        Some(Command::Conventions(args)) => conventions::run(&app, args),
        Some(Command::Mcp(command)) => match command.command {
            McpSubcommand::Serve(args) => serve::run(&app, args).await,
        },
        None => no_args::run(&app).await,
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
    use clap::Parser;
    use pretty_assertions::assert_eq;

    use super::{Cli, Command};
    use crate::commands::{
        clean::CleanArgs, compact::CompactArgs, index::IndexArgs,
        projection_impact::EvidenceVerbosityArg, reindex::ReindexArgs, serve::ServeArgs,
        setup_mcp::McpScope, trace::TraceCommand, tui::TuiArgs, watch::WatchArgs,
    };

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
            }
        );
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
