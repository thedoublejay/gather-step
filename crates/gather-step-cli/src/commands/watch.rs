use std::{
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use clap::Args;
use gather_step_core::GatherStepConfig;
use gather_step_storage::{
    IndexingOptions, StorageDaemonMetadataGuard, WatchCause, WatchEvent, WatcherConfig,
    WorkspaceStores, WorkspaceWatcher, search_store::SearchWorkload,
};
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::{
    app::AppContext,
    daemon_server::{DaemonRuntime, DaemonServer},
    path_safety,
};

#[derive(Debug, Args, PartialEq, Eq)]
pub struct WatchArgs {
    #[arg(
        long,
        help = "Path to workspace config (default: workspace-local config)"
    )]
    pub config: Option<PathBuf>,
    #[arg(long, help = "Path to storage root (default: workspace-local storage)")]
    pub storage: Option<PathBuf>,
    #[arg(long, default_value_t = 250)]
    pub poll_interval_ms: u64,
    #[arg(long, default_value_t = 2000)]
    pub debounce_ms: u64,
    #[arg(long, default_value_t = 5)]
    pub consecutive_error_limit: u32,
    #[arg(long, default_value_t = 5000)]
    pub error_backoff_ms: u64,
}

impl Default for WatchArgs {
    fn default() -> Self {
        Self {
            config: None,
            storage: None,
            poll_interval_ms: 250,
            debounce_ms: 2000,
            consecutive_error_limit: 5,
            error_backoff_ms: 5000,
        }
    }
}

#[derive(Debug, Serialize)]
struct WatchEventOutput {
    event: &'static str,
    repo: String,
    cause: Option<&'static str>,
    files: Vec<String>,
    dropped_events: Option<u64>,
    modified: Option<usize>,
    added: Option<usize>,
    deleted: Option<usize>,
    files_parsed: Option<usize>,
    duration_ms: Option<u64>,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct WatchStatusOutput {
    event: &'static str,
    events_seen: u64,
    dropped_events: u64,
    indexing_runs: u64,
    overflows: u64,
    rescans_requested: u64,
    errors: u64,
    backoff_suppressions: u64,
    cross_repo_reconciliations: u64,
}

fn emit_watch_line(line: &str) -> Result<()> {
    let mut stderr = io::stderr().lock();
    writeln!(stderr, "{line}")?;
    Ok(())
}

pub async fn run(app: &AppContext, args: WatchArgs) -> Result<()> {
    let output = app.output();
    let daemon_metadata;
    let defaults = app.workspace_paths();
    let config_path = args.config.unwrap_or(defaults.config_path);
    let mut config = GatherStepConfig::from_yaml_file(&config_path)?;
    apply_repo_filter(&mut config, app.repo_filter.as_deref())?;
    let config_root = config_path.parent().unwrap_or_else(|| Path::new("."));
    config.validate_repo_roots_against_config_root(config_root)?;
    let storage_root = args.storage.unwrap_or(defaults.storage_root);
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
    let watcher = Arc::new(WorkspaceWatcher::new_with_stores(
        stores.as_ref().clone(),
        IndexingOptions::default(),
        WatcherConfig {
            poll_interval: Duration::from_millis(args.poll_interval_ms),
            debounce_duration: Duration::from_millis(args.debounce_ms),
            consecutive_error_limit: args.consecutive_error_limit,
            error_backoff: Duration::from_millis(args.error_backoff_ms),
        },
        &config,
        config_root,
    )?);
    let daemon_runtime = DaemonRuntime::from_stores(
        defaults.registry_path.clone(),
        defaults.graph_path.clone(),
        Arc::clone(&stores),
    );
    daemon_metadata =
        StorageDaemonMetadataGuard::write_for_storage_root(&storage_root, &app.workspace_path)?;
    let daemon = DaemonServer::bind_with_runtime(app, daemon_runtime)?;

    let cancel = CancellationToken::new();
    let mut events = watcher.subscribe();
    let run_cancel = cancel.clone();
    let event_output = output.clone();
    let event_task = tokio::spawn(async move {
        loop {
            let event = match events.recv().await {
                Ok(event) => event,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    tracing::warn!(skipped, "watch event subscriber lagged; continuing");
                    continue;
                }
            };

            let emit_result = match event {
                WatchEvent::IndexingStart { repo, files, cause } => {
                    let cause = match cause {
                        WatchCause::Paths => "paths",
                        WatchCause::Rescan => "rescan",
                    };
                    if event_output.is_json() {
                        event_output.emit(&WatchEventOutput {
                            event: "watch_indexing_start",
                            repo,
                            cause: Some(cause),
                            files,
                            dropped_events: None,
                            modified: None,
                            added: None,
                            deleted: None,
                            files_parsed: None,
                            duration_ms: None,
                            error: None,
                        })
                    } else {
                        emit_watch_line(&format!(
                            "watch:indexing_start repo={repo} cause={cause} files={}",
                            files.join(",")
                        ))
                    }
                }
                WatchEvent::Overflow {
                    repo,
                    dropped_events,
                } => {
                    if event_output.is_json() {
                        event_output.emit(&WatchEventOutput {
                            event: "watch_overflow",
                            repo,
                            cause: None,
                            files: Vec::new(),
                            dropped_events: Some(dropped_events),
                            modified: None,
                            added: None,
                            deleted: None,
                            files_parsed: None,
                            duration_ms: None,
                            error: None,
                        })
                    } else {
                        emit_watch_line(&format!(
                            "watch:overflow repo={repo} dropped_events={dropped_events}"
                        ))
                    }
                }
                WatchEvent::IndexingComplete {
                    repo,
                    changed,
                    stats,
                } => {
                    if event_output.is_json() {
                        event_output.emit(&WatchEventOutput {
                            event: "watch_indexing_complete",
                            repo,
                            cause: None,
                            files: Vec::new(),
                            dropped_events: None,
                            modified: Some(changed.modified.len()),
                            added: Some(changed.added.len()),
                            deleted: Some(changed.deleted.len()),
                            files_parsed: Some(stats.files_parsed),
                            duration_ms: Some(u64::try_from(stats.duration_ms).unwrap_or(u64::MAX)),
                            error: None,
                        })
                    } else {
                        emit_watch_line(&format!(
                            "watch:indexing_complete repo={repo} modified={} added={} deleted={} files_parsed={} duration_ms={}",
                            changed.modified.len(),
                            changed.added.len(),
                            changed.deleted.len(),
                            stats.files_parsed,
                            stats.duration_ms,
                        ))
                    }
                }
                WatchEvent::Error { repo, error } => {
                    if event_output.is_json() {
                        event_output.emit(&WatchEventOutput {
                            event: "watch_error",
                            repo,
                            cause: None,
                            files: Vec::new(),
                            dropped_events: None,
                            modified: None,
                            added: None,
                            deleted: None,
                            files_parsed: None,
                            duration_ms: None,
                            error: Some(error),
                        })
                    } else {
                        emit_watch_line(&format!("watch:error repo={repo} error={error}"))
                    }
                }
            };

            if let Err(error) = emit_result {
                tracing::warn!(%error, "watch event output failed; stopping event stream");
                break;
            }
        }
    });

    let watch_runner = Arc::clone(&watcher);
    let watch_task = tokio::spawn(async move { watch_runner.run(run_cancel).await });
    let daemon_cancel = cancel.clone();
    let daemon_task =
        tokio::spawn(async move { daemon.serve_until_cancelled(daemon_cancel).await });
    tokio::signal::ctrl_c().await?;
    cancel.cancel();
    watch_task.await??;
    daemon_task.await??;
    let status = watcher.status();
    drop(watcher);
    drop(stores);
    drop(daemon_metadata);
    if let Err(error) = event_task.await {
        tracing::warn!(?error, "watch event task crashed");
    }
    if output.is_json() {
        output.emit(&WatchStatusOutput {
            event: "watch_status",
            events_seen: status.events_seen,
            dropped_events: status.dropped_events,
            indexing_runs: status.indexing_runs,
            overflows: status.overflows,
            rescans_requested: status.rescans_requested,
            errors: status.errors,
            backoff_suppressions: status.backoff_suppressions,
            cross_repo_reconciliations: status.cross_repo_reconciliations,
        })?;
    } else {
        emit_watch_line(&format!(
            "watch:status events_seen={} dropped_events={} indexing_runs={} overflows={} rescans_requested={} errors={} backoff_suppressions={} cross_repo_reconciliations={}",
            status.events_seen,
            status.dropped_events,
            status.indexing_runs,
            status.overflows,
            status.rescans_requested,
            status.errors,
            status.backoff_suppressions,
            status.cross_repo_reconciliations,
        ))?;
    }
    Ok(())
}

pub(crate) fn apply_repo_filter(
    config: &mut GatherStepConfig,
    repo_filter: Option<&str>,
) -> Result<()> {
    let Some(repo_filter) = repo_filter else {
        return Ok(());
    };

    config.repos.retain(|repo| repo.name == repo_filter);
    config.allow_listed_repos.retain(|repo| repo == repo_filter);

    if config.repos.is_empty() {
        bail!("repo `{repo_filter}` was not found in the workspace config");
    }

    config.validate()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::apply_repo_filter;
    use gather_step_core::GatherStepConfig;

    #[test]
    fn apply_repo_filter_rejects_unknown_repo() {
        let mut config = GatherStepConfig::from_yaml_str(
            r"
repos:
  - name: backend_standard
    path: repos/backend_standard
  - name: frontend_standard
    path: repos/frontend_standard
",
        )
        .expect("config should parse");

        let error =
            apply_repo_filter(&mut config, Some("missing_repo")).expect_err("unknown repo fails");

        assert!(error.to_string().contains("was not found"));
    }
}
