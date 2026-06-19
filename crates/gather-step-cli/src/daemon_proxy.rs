use std::path::PathBuf;

use anyhow::{Error, Result};
use gather_step_mcp::output::redact::relativize_to_workspace;
use gather_step_storage::GraphStoreError;
use tracing::warn;

use crate::{
    app::AppContext, command_render::RenderedCommand, daemon_client::DaemonClient,
    daemon_protocol::DaemonRequest,
};

pub fn run_read_only_command<F>(app: &AppContext, request: &DaemonRequest, local: F) -> Result<()>
where
    F: FnOnce(&AppContext) -> Result<RenderedCommand>,
{
    let output = app.output();
    let mut rendered = match try_daemon_first(app, request) {
        Some(rendered) => rendered,
        None => match local(app) {
            Ok(rendered) => rendered,
            Err(error) => match try_daemon_after_lock(app, request, &error) {
                Some(rendered) => rendered,
                None => return Err(error),
            },
        },
    };
    // `pack` output is asserted byte-for-byte against the MCP tool (CLI/MCP
    // parity); it carries freshness via its own response instead.
    if !matches!(
        request,
        DaemonRequest::Pack { .. } | DaemonRequest::StorageReport
    ) {
        inject_freshness(app, &mut rendered);
    }
    rendered.emit(&output)
}

/// Attach a query-time index-freshness verdict to read-command output so a query
/// against a stale index can be recognized rather than trusted blindly.
/// Best-effort: reads only the registry + metadata + git (never the lockable
/// graph), and is skipped silently when the workspace is unindexed.
fn inject_freshness(app: &AppContext, rendered: &mut RenderedCommand) {
    let paths = app.workspace_paths();
    let freshness = crate::freshness::freshness_from_paths(
        &paths.registry_path,
        &paths.storage_root.join("metadata.sqlite"),
    );
    if freshness.is_empty() {
        return;
    }
    if let Some(serde_json::Value::Object(map)) = rendered.payload.as_mut()
        && !map.contains_key("freshness")
        && let Ok(value) = serde_json::to_value(&freshness)
    {
        map.insert("freshness".to_owned(), value);
    }
}

fn try_daemon_first(app: &AppContext, request: &DaemonRequest) -> Option<RenderedCommand> {
    let client = match DaemonClient::try_connect(&app.workspace_path) {
        Ok(Some(client)) => client,
        Ok(None) => return None,
        Err(error) => {
            warn!(
                workspace = %relativize_to_workspace(&app.workspace_path, &app.workspace_path),
                request = request_name(request),
                %error,
                "failed to inspect daemon state; falling back to local execution"
            );
            return None;
        }
    };

    match client.call(request) {
        Ok(rendered) if !daemon_rejected_request(&rendered) => Some(rendered),
        Ok(_) => {
            warn!(
                workspace = %app.workspace_path.display(),
                request = request_name(request),
                "daemon does not understand this request kind (older daemon binary?); falling back to local execution"
            );
            None
        }
        Err(error) => {
            warn!(
                workspace = %app.workspace_path.display(),
                request = request_name(request),
                %error,
                "daemon request failed; falling back to local execution"
            );
            None
        }
    }
}

/// A daemon built before this request variant existed responds with a
/// protocol-level `invalid daemon request` failure rather than executing it.
/// Treat that as "daemon unavailable for this request" so the caller falls
/// back to local execution instead of surfacing the parse error.
fn daemon_rejected_request(rendered: &RenderedCommand) -> bool {
    rendered
        .error
        .as_deref()
        .is_some_and(|message| message.starts_with("invalid daemon request"))
}

fn try_daemon_after_lock(
    app: &AppContext,
    request: &DaemonRequest,
    error: &Error,
) -> Option<RenderedCommand> {
    let daemon_workspace = daemon_workspace_from_graph_lock(error)?;
    let client = match DaemonClient::try_connect(&daemon_workspace) {
        Ok(Some(client)) => client,
        Ok(None) => {
            warn!(
                workspace = %app.workspace_path.display(),
                daemon_workspace = %daemon_workspace.display(),
                request = request_name(request),
                "local read hit a graph lock held by a daemon, but no request socket was available; preserving lock-contention failure"
            );
            return None;
        }
        Err(connect_error) => {
            warn!(
                workspace = %app.workspace_path.display(),
                daemon_workspace = %daemon_workspace.display(),
                request = request_name(request),
                %connect_error,
                "local read hit a graph lock held by a daemon, but daemon inspection failed; preserving lock-contention failure"
            );
            return None;
        }
    };

    match client.call(request) {
        Ok(rendered) if !daemon_rejected_request(&rendered) => Some(rendered),
        Ok(_) => {
            warn!(
                workspace = %app.workspace_path.display(),
                daemon_workspace = %daemon_workspace.display(),
                request = request_name(request),
                "local read hit a graph lock held by a daemon that does not understand this request kind (older daemon binary?); preserving lock-contention failure"
            );
            None
        }
        Err(call_error) => {
            warn!(
                workspace = %app.workspace_path.display(),
                daemon_workspace = %daemon_workspace.display(),
                request = request_name(request),
                %call_error,
                "local read hit a graph lock held by a daemon, but daemon retry failed; preserving lock-contention failure"
            );
            None
        }
    }
}

fn daemon_workspace_from_graph_lock(error: &Error) -> Option<PathBuf> {
    error.chain().find_map(|cause| {
        cause
            .downcast_ref::<GraphStoreError>()
            .and_then(|graph_error| match graph_error {
                GraphStoreError::StorageHeldByDaemon { workspace_root, .. } => {
                    Some(PathBuf::from(workspace_root))
                }
                _ => None,
            })
    })
}

fn request_name(request: &DaemonRequest) -> &'static str {
    match request {
        DaemonRequest::Search { .. } => "search",
        DaemonRequest::Status { .. } => "status",
        DaemonRequest::TraceCrud { .. } => "trace_crud",
        DaemonRequest::Doctor { .. } => "doctor",
        DaemonRequest::Conventions { .. } => "conventions",
        DaemonRequest::CrossRepoDeps { .. } => "cross_repo_deps",
        DaemonRequest::StorageReport => "storage_report",
        DaemonRequest::EventsTrace { .. } => "events_trace",
        DaemonRequest::EventsBlastRadius { .. } => "events_blast_radius",
        DaemonRequest::EventsOrphans { .. } => "events_orphans",
        DaemonRequest::EventsAgentTrace { .. } => "events_agent_trace",
        DaemonRequest::Impact { .. } => "impact",
        DaemonRequest::Pack { .. } => "pack",
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    #[cfg(unix)]
    use std::{
        fs,
        path::Path,
        sync::atomic::{AtomicU64, Ordering},
    };

    #[cfg(unix)]
    use anyhow::Result;
    use gather_step_storage::GraphStoreError;
    #[cfg(unix)]
    use indicatif::MultiProgress;
    #[cfg(unix)]
    use tokio_util::sync::CancellationToken;

    #[cfg(unix)]
    use crate::{
        app::{AppContext, ColorModeArg},
        daemon_protocol::DaemonRequest,
        daemon_server::DaemonServer,
    };

    #[cfg(unix)]
    use super::try_daemon_after_lock;
    use super::{daemon_rejected_request, daemon_workspace_from_graph_lock};
    use crate::command_render::RenderedCommand;

    #[test]
    fn protocol_rejection_is_distinguished_from_command_failure() {
        let rejected = RenderedCommand::failure(
            None,
            Vec::new(),
            "invalid daemon request: unknown variant `CrossRepoDeps`",
        );
        assert!(daemon_rejected_request(&rejected));

        let command_failure = RenderedCommand::failure(None, Vec::new(), "unknown repo `missing`");
        assert!(!daemon_rejected_request(&command_failure));

        let success = RenderedCommand::success(serde_json::json!({}), Vec::new());
        assert!(!daemon_rejected_request(&success));
    }

    #[cfg(unix)]
    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

    #[cfg(unix)]
    struct TestWorkspace {
        root: PathBuf,
    }

    #[cfg(unix)]
    impl TestWorkspace {
        fn new(name: &str) -> Self {
            let id = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let root =
                PathBuf::from("/tmp").join(format!("gsd-proxy-{name}-{}-{id}", std::process::id()));
            fs::create_dir_all(&root).expect("test workspace should exist");
            Self { root }
        }

        fn path(&self) -> &Path {
            &self.root
        }
    }

    #[cfg(unix)]
    impl Drop for TestWorkspace {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    #[cfg(unix)]
    fn app(workspace_root: &Path) -> AppContext {
        AppContext {
            workspace_path: workspace_root.to_path_buf(),
            repo_filter: None,
            json_output: false,
            no_interactive: true,
            stdin_is_tty: false,
            stdout_is_tty: false,
            stderr_is_tty: false,
            ci_env_set: true,
            color_mode: ColorModeArg::Auto,
            show_banner: false,
            multi_progress: MultiProgress::new(),
        }
    }

    #[cfg(unix)]
    fn bind_daemon_or_skip(app: &AppContext) -> Result<Option<DaemonServer>> {
        match DaemonServer::bind(app) {
            Ok(daemon) => Ok(Some(daemon)),
            Err(error) if unix_socket_bind_is_not_permitted(&error) => Ok(None),
            Err(error) => Err(error),
        }
    }

    #[cfg(unix)]
    fn unix_socket_bind_is_not_permitted(error: &anyhow::Error) -> bool {
        error.to_string().contains("binding")
            && error.chain().any(|cause| {
                cause
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|io| io.kind() == std::io::ErrorKind::PermissionDenied)
            })
    }

    #[test]
    fn daemon_workspace_from_graph_lock_extracts_daemon_holder_workspace() {
        let error = anyhow::Error::new(GraphStoreError::StorageHeldByDaemon {
            path: PathBuf::from("/tmp/ws/.gather-step/storage/graph.redb"),
            pid: 1234,
            started_at_epoch_ms: 42,
            workspace_root: "/tmp/ws".to_owned(),
        })
        .context("opening read-only workspace storage");

        assert_eq!(
            daemon_workspace_from_graph_lock(&error),
            Some(PathBuf::from("/tmp/ws"))
        );
    }

    #[test]
    fn daemon_workspace_from_graph_lock_ignores_non_daemon_locks() {
        let error = anyhow::Error::new(GraphStoreError::StorageHeld {
            path: PathBuf::from("/tmp/ws/.gather-step/storage/graph.redb"),
        })
        .context("opening read-only workspace storage");

        assert_eq!(daemon_workspace_from_graph_lock(&error), None);
    }

    #[cfg(unix)]
    #[test]
    fn try_daemon_after_lock_returns_none_when_holder_socket_is_missing() {
        let workspace = TestWorkspace::new("missing-socket");
        let app = app(workspace.path());
        let error = anyhow::Error::new(GraphStoreError::StorageHeldByDaemon {
            path: workspace.path().join(".gather-step/storage/graph.redb"),
            pid: std::process::id(),
            started_at_epoch_ms: 42,
            workspace_root: workspace.path().display().to_string(),
        })
        .context("opening read-only workspace storage");

        let rendered =
            try_daemon_after_lock(&app, &DaemonRequest::Status { repo_filter: None }, &error);

        assert!(
            rendered.is_none(),
            "socket-missing daemon fallback should preserve the original lock error"
        );
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn try_daemon_after_lock_retries_through_holder_daemon() -> Result<()> {
        let workspace = TestWorkspace::new("lock-retry");
        let app = app(workspace.path());
        let Some(daemon) = bind_daemon_or_skip(&app)? else {
            return Ok(());
        };
        let cancel = CancellationToken::new();
        let daemon_task = tokio::spawn(daemon.serve_until_cancelled(cancel.clone()));
        let error = anyhow::Error::new(GraphStoreError::StorageHeldByDaemon {
            path: workspace.path().join(".gather-step/storage/graph.redb"),
            pid: std::process::id(),
            started_at_epoch_ms: 42,
            workspace_root: workspace.path().display().to_string(),
        })
        .context("opening read-only workspace storage");

        let rendered =
            try_daemon_after_lock(&app, &DaemonRequest::Status { repo_filter: None }, &error)
                .expect("holder daemon retry should render a response");

        assert!(rendered.error.is_none());
        assert!(rendered.payload.is_some());

        cancel.cancel();
        daemon_task.await??;
        Ok(())
    }
}
