use std::{
    io::{self, Write},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use clap::Args;
use gather_step_core::GatherStepConfig;
use gather_step_mcp::{GatherStepMcpServer, McpContext, McpServerConfig};
use gather_step_storage::{
    IndexingOptions, StorageDaemonMetadataGuard, WatchCause, WatchEvent, WatcherConfig,
    WorkspaceStores, WorkspaceWatcher, search_store::SearchWorkload,
};
use tokio_util::sync::CancellationToken;

use crate::commands::watch::apply_repo_filter;
use crate::{
    app::AppContext,
    daemon_server::{DaemonRuntime, DaemonServer},
    path_safety,
};

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Args, PartialEq, Eq)]
pub struct ServeArgs {
    #[arg(
        long,
        help = "Path to the graph store (default: workspace-local storage)"
    )]
    pub graph: Option<PathBuf>,
    #[arg(
        long,
        help = "Path to the workspace registry (default: workspace-local registry)"
    )]
    pub registry: Option<PathBuf>,
    #[arg(long, default_value_t = gather_step_mcp::DEFAULT_MCP_MAX_LIMIT)]
    pub max_limit: usize,
    #[arg(long, default_value = "gather-step")]
    pub server_name: String,
    #[arg(
        long,
        help = "Path to workspace config (default: workspace-local config)"
    )]
    pub config: Option<PathBuf>,
    #[arg(long, help = "Run the filesystem watcher in the same process")]
    pub watch: bool,
    #[arg(long, default_value_t = 250)]
    pub poll_interval_ms: u64,
    #[arg(long, default_value_t = 2000)]
    pub debounce_ms: u64,
    #[arg(long, default_value_t = 5)]
    pub consecutive_error_limit: u32,
    #[arg(long, default_value_t = 5000)]
    pub error_backoff_ms: u64,
    /// When set, append tool-call trace records as JSONL to this file.
    /// When absent, traces are emitted via the tracing subscriber instead.
    #[arg(long)]
    pub trace_tool_calls: Option<PathBuf>,
}

fn build_mcp_config(app: &AppContext, args: ServeArgs) -> McpServerConfig {
    let defaults = app.workspace_paths();
    let mut config = McpServerConfig::new(
        args.registry.unwrap_or(defaults.registry_path),
        args.graph.unwrap_or(defaults.graph_path),
    );
    config.max_limit = args.max_limit;
    config.server_name = args.server_name;
    config.trace_tool_calls = args.trace_tool_calls;
    config
}

/// Escape ASCII control characters (including `\n`, `\r`, `\t`) in a field
/// value so that the one-line-per-event contract on stderr is preserved even
/// if a repo name or file path contains embedded control characters.
///
/// Each control character is replaced with its Unicode escape form `\u{XXXX}`.
fn escape_field(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_control() {
            // e.g. '\n' → \u{a}, '\r' → \u{d}
            use std::fmt::Write as _;
            let _ = write!(out, "\\u{{{:x}}}", ch as u32);
        } else {
            out.push(ch);
        }
    }
    out
}

fn emit_watch_line(line: &str) -> Result<()> {
    let mut stderr = io::stderr().lock();
    writeln!(stderr, "{line}")?;
    Ok(())
}

pub async fn run(app: &AppContext, args: ServeArgs) -> Result<()> {
    let daemon_metadata;
    let mcp_config = build_mcp_config(app, args.clone());
    if !args.watch {
        let ctx = McpContext::open(mcp_config)?;
        GatherStepMcpServer::new(ctx).serve_stdio().await?;
        return Ok(());
    }

    let defaults = app.workspace_paths();
    let config_path = args.config.clone().unwrap_or(defaults.config_path);
    let mut workspace = GatherStepConfig::from_yaml_file(&config_path)?;
    apply_repo_filter(&mut workspace, app.repo_filter.as_deref())?;
    let config_root = config_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    workspace.validate_repo_roots_against_config_root(config_root)?;

    let storage_root = mcp_config
        .graph_path
        .parent()
        .map_or_else(|| PathBuf::from("."), std::path::Path::to_path_buf);
    path_safety::reject_symlinked_generated_state(&app.workspace_path, &storage_root)
        .with_context(|| {
            format!(
                "generated-state path `{}` failed symlink check",
                storage_root.display()
            )
        })?;
    let stores = Arc::new(WorkspaceStores::open_with_workload(
        &storage_root,
        SearchWorkload::LongRunning,
    )?);
    let daemon_runtime = DaemonRuntime::from_stores(
        mcp_config.registry_path.clone(),
        mcp_config.graph_path.clone(),
        Arc::clone(&stores),
    );
    let ctx = McpContext::open_with_stores(mcp_config, Arc::clone(&stores))?;
    let watcher = Arc::new(WorkspaceWatcher::new_with_stores(
        stores.as_ref().clone(),
        IndexingOptions::default(),
        WatcherConfig {
            poll_interval: Duration::from_millis(args.poll_interval_ms),
            debounce_duration: Duration::from_millis(args.debounce_ms),
            consecutive_error_limit: args.consecutive_error_limit,
            error_backoff: Duration::from_millis(args.error_backoff_ms),
        },
        &workspace,
        config_root,
    )?);
    daemon_metadata =
        StorageDaemonMetadataGuard::write_for_storage_root(&storage_root, &app.workspace_path)?;
    let daemon = DaemonServer::bind_with_runtime(app, daemon_runtime)?;

    let cancel = CancellationToken::new();
    let mut events = watcher.subscribe();
    let event_task = tokio::spawn(async move {
        loop {
            let event = match events.recv().await {
                Ok(event) => event,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    tracing::warn!(skipped, "serve watch subscriber lagged; continuing");
                    continue;
                }
            };

            let emit_result = match event {
                WatchEvent::IndexingStart { repo, files, cause } => {
                    let cause = match cause {
                        WatchCause::Paths => "paths",
                        WatchCause::Rescan => "rescan",
                    };
                    let repo = escape_field(&repo);
                    let files = files
                        .iter()
                        .map(|f| escape_field(f))
                        .collect::<Vec<_>>()
                        .join(",");
                    emit_watch_line(&format!(
                        "watch:indexing_start repo={repo} cause={cause} files={files}",
                    ))
                }
                WatchEvent::Overflow {
                    repo,
                    dropped_events,
                } => {
                    let repo = escape_field(&repo);
                    emit_watch_line(&format!(
                        "watch:overflow repo={repo} dropped_events={dropped_events}"
                    ))
                }
                WatchEvent::IndexingComplete {
                    repo,
                    changed,
                    stats,
                } => {
                    let repo = escape_field(&repo);
                    emit_watch_line(&format!(
                        "watch:indexing_complete repo={repo} modified={} added={} deleted={} files_parsed={} duration_ms={}",
                        changed.modified.len(),
                        changed.added.len(),
                        changed.deleted.len(),
                        stats.files_parsed,
                        stats.duration_ms,
                    ))
                }
                WatchEvent::Error { repo, error } => {
                    let repo = escape_field(&repo);
                    // Escape the error string too: it may contain repo paths or
                    // OS-level messages with embedded control characters.
                    let error = escape_field(&error);
                    emit_watch_line(&format!("watch:error repo={repo} error={error}"))
                }
            };

            if let Err(error) = emit_result {
                tracing::warn!(%error, "serve watch output failed; stopping event stream");
                break;
            }
        }
    });

    let watch_cancel = cancel.clone();
    let watch_runner = Arc::clone(&watcher);
    let watch_task = tokio::spawn(async move { watch_runner.run(watch_cancel).await });
    let daemon_cancel = cancel.clone();
    let daemon_task =
        tokio::spawn(async move { daemon.serve_until_cancelled(daemon_cancel).await });
    let mcp_cancel = cancel.clone();
    let mcp_server = GatherStepMcpServer::with_cancel(ctx, mcp_cancel.clone());
    let mut mcp_task =
        tokio::spawn(async move { mcp_server.serve_stdio_until_cancelled(mcp_cancel).await });
    let serve_result = tokio::select! {
        result = &mut mcp_task => result?,
        signal = tokio::signal::ctrl_c() => {
            signal?;
            cancel.cancel();
            if let Err(_timeout) = tokio::time::timeout(SHUTDOWN_TIMEOUT, &mut mcp_task).await {
                tracing::warn!(
                    timeout_secs = SHUTDOWN_TIMEOUT.as_secs(),
                    "MCP shutdown timed out; aborting",
                );
                mcp_task.abort();
                let _ = mcp_task.await;
            }
            shutdown_watch_task(watch_task).await;
            if let Err(_timeout) = tokio::time::timeout(SHUTDOWN_TIMEOUT, daemon_task).await {
                tracing::warn!(
                    timeout_secs = SHUTDOWN_TIMEOUT.as_secs(),
                    "daemon shutdown timed out; aborting",
                );
            }
            stores.search().flush()?;
            drop(watcher);
            // Watcher dropped → broadcast channel closes → event task exits on
            // its own.  Bound the wait in case the task is stuck on a stderr
            // write; the tokio runtime will abort any remnant on process exit.
            if tokio::time::timeout(Duration::from_secs(2), event_task)
                .await
                .is_err()
            {
                tracing::warn!("serve watch event task did not exit within 2s");
            }
            drop(stores);
            drop(daemon_metadata);
            return Ok(());
        }
    };
    cancel.cancel();
    // Normal completion path (MCP exited first): drive hard shutdown the same
    // way as Ctrl-C so watch/daemon tasks cannot retain handles after return.
    shutdown_watch_task(watch_task).await;
    if let Err(_timeout) = tokio::time::timeout(SHUTDOWN_TIMEOUT, daemon_task).await {
        tracing::warn!(
            timeout_secs = SHUTDOWN_TIMEOUT.as_secs(),
            "daemon shutdown timed out; aborting",
        );
    }
    stores.search().flush()?;
    drop(watcher);
    if tokio::time::timeout(Duration::from_secs(2), event_task)
        .await
        .is_err()
    {
        tracing::warn!("serve watch event task did not exit within 2s");
    }
    drop(stores);
    drop(daemon_metadata);
    if let Err(error) = serve_result
        && !is_expected_stdio_disconnect(&error)
    {
        return Err(error.into());
    }
    Ok(())
}

/// Wait for the watch task to exit, aborting on timeout.
///
/// The event task is NOT awaited here: the outer `watcher: Arc<WorkspaceWatcher>`
/// still owns the broadcast sender, so `events.recv()` inside the event task
/// cannot see `Closed` until the outer function drops its watcher handle. The
/// caller drops `watcher` after this function returns and then awaits (or
/// aborts) the event task at that point.
async fn shutdown_watch_task(
    watch_task: tokio::task::JoinHandle<Result<(), gather_step_storage::WatcherError>>,
) {
    match tokio::time::timeout(SHUTDOWN_TIMEOUT, watch_task).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(error))) => tracing::warn!(?error, "serve watch task exited with error"),
        Ok(Err(error)) => tracing::warn!(?error, "serve watch task crashed"),
        Err(_timeout) => {
            tracing::warn!(
                timeout_secs = SHUTDOWN_TIMEOUT.as_secs(),
                "watch shutdown timed out; continuing teardown (watch task abandoned)",
            );
        }
    }
}

fn is_expected_stdio_disconnect(error: &gather_step_mcp::McpServerError) -> bool {
    error
        .to_string()
        .contains("connection closed: initialize request")
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use indicatif::MultiProgress;

    use super::{ServeArgs, build_mcp_config, escape_field};
    use crate::app::AppContext;

    #[test]
    fn escape_field_is_identity_for_clean_strings() {
        assert_eq!(escape_field("service-a"), "service-a");
        assert_eq!(escape_field(""), "");
        assert_eq!(escape_field("src/foo.ts"), "src/foo.ts");
    }

    #[test]
    fn escape_field_replaces_newline_and_carriage_return() {
        assert_eq!(
            escape_field("repo\nwith\nnewlines"),
            r"repo\u{a}with\u{a}newlines"
        );
        assert_eq!(escape_field("repo\r\n"), r"repo\u{d}\u{a}");
    }

    #[test]
    fn escape_field_replaces_tab_and_other_control_chars() {
        assert_eq!(escape_field("col\tval"), r"col\u{9}val");
        // NULL byte
        assert_eq!(escape_field("a\x00b"), r"a\u{0}b");
    }

    #[test]
    fn escape_field_preserves_non_ascii_unicode() {
        // Non-ASCII printable chars are not control characters and must pass through.
        assert_eq!(escape_field("café"), "café");
        assert_eq!(escape_field("日本語"), "日本語");
    }

    fn app() -> AppContext {
        AppContext {
            workspace_path: PathBuf::from("/tmp/workspace"),
            repo_filter: None,
            json_output: false,
            no_interactive: true,
            stdin_is_tty: false,
            stdout_is_tty: false,
            ci_env_set: true,
            show_banner: false,
            multi_progress: MultiProgress::new(),
        }
    }

    #[test]
    fn build_mcp_config_uses_workspace_defaults() {
        let config = build_mcp_config(
            &app(),
            ServeArgs {
                graph: None,
                registry: None,
                max_limit: gather_step_mcp::DEFAULT_MCP_MAX_LIMIT,
                server_name: "gather-step".to_owned(),
                config: None,
                watch: false,
                poll_interval_ms: 250,
                debounce_ms: 2000,
                consecutive_error_limit: 5,
                error_backoff_ms: 5000,
                trace_tool_calls: None,
            },
        );

        assert_eq!(
            config.registry_path,
            PathBuf::from("/tmp/workspace/.gather-step/registry.json")
        );
        assert_eq!(
            config.graph_path,
            PathBuf::from("/tmp/workspace/.gather-step/storage/graph.redb")
        );
    }

    #[test]
    fn build_mcp_config_honors_overrides() {
        let config = build_mcp_config(
            &app(),
            ServeArgs {
                graph: Some(PathBuf::from("/tmp/custom/graph.redb")),
                registry: Some(PathBuf::from("/tmp/custom/registry.json")),
                max_limit: 42,
                server_name: "custom".to_owned(),
                config: None,
                watch: false,
                poll_interval_ms: 250,
                debounce_ms: 2000,
                consecutive_error_limit: 5,
                error_backoff_ms: 5000,
                trace_tool_calls: None,
            },
        );

        assert_eq!(
            config.registry_path,
            PathBuf::from("/tmp/custom/registry.json")
        );
        assert_eq!(config.graph_path, PathBuf::from("/tmp/custom/graph.redb"));
        assert_eq!(config.max_limit, 42);
        assert_eq!(config.server_name, "custom");
    }
}
