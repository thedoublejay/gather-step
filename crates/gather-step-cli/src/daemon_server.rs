use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use gather_step_core::RegistryStore;
use gather_step_mcp::{McpContext, McpServerConfig};
use gather_step_storage::{StorageCoordinator, WorkspaceStores};
use tokio_util::sync::CancellationToken;

use crate::{
    app::AppContext,
    command_render::RenderedCommand,
    commands::{
        conventions, doctor,
        events::{self, BlastRadiusArgs, EventsArgs, EventsCommand, OrphansArgs, TraceArgs},
        impact::{self, ImpactArgs},
        pack::{self, PackArgs, PackModeArg},
        search::{self, SearchArgs},
        status,
        trace::{self, CrudArgs, TraceCommand},
    },
    daemon_protocol::{DaemonPidFile, DaemonRequest, DaemonResponse},
    storage_context::StorageContext,
};

#[derive(Clone)]
pub struct DaemonRuntime {
    registry_path: PathBuf,
    stores: Arc<WorkspaceStores>,
    mcp: Arc<McpContext>,
}

impl DaemonRuntime {
    #[must_use]
    pub fn from_stores(
        registry_path: PathBuf,
        graph_path: PathBuf,
        stores: Arc<WorkspaceStores>,
    ) -> Self {
        let mcp = Arc::new(McpContext::from_workspace_stores(
            McpServerConfig::new(registry_path.clone(), graph_path),
            Arc::clone(&stores),
        ));
        Self {
            registry_path,
            stores,
            mcp,
        }
    }

    #[must_use]
    fn storage(&self) -> StorageCoordinator {
        StorageCoordinator::from_stores(self.stores.as_ref().clone())
    }
}

pub fn dispatch_request(app: &AppContext, request: DaemonRequest) -> Result<RenderedCommand> {
    dispatch_request_with_runtime(app, request, None)
}

pub fn dispatch_request_with_runtime(
    app: &AppContext,
    request: DaemonRequest,
    runtime: Option<&DaemonRuntime>,
) -> Result<RenderedCommand> {
    match request {
        DaemonRequest::Search {
            query,
            limit,
            kind,
            repo_filter,
        } => {
            let app = app_with_repo_filter(app, repo_filter);
            if let Some(runtime) = runtime {
                let storage = runtime.storage();
                search::execute(
                    &storage,
                    app.repo_filter.as_deref(),
                    SearchArgs { query, limit, kind },
                )
            } else {
                search::run_rendered(
                    &app,
                    &StorageContext::workspace_read_only(&app),
                    SearchArgs { query, limit, kind },
                )
            }
        }
        DaemonRequest::Status { repo_filter } => {
            let app = app_with_repo_filter(app, repo_filter);
            if let Some(runtime) = runtime {
                let registry = RegistryStore::open(&runtime.registry_path)
                    .with_context(|| format!("opening {}", runtime.registry_path.display()))?;
                let storage = runtime.storage();
                status::execute(
                    &app.workspace_path,
                    &runtime.registry_path,
                    storage.root(),
                    &registry,
                    &storage,
                    app.repo_filter.as_deref(),
                )
            } else {
                status::run_rendered(&app, &StorageContext::workspace_read_only(&app))
            }
        }
        DaemonRequest::TraceCrud {
            method,
            path,
            symbol_id,
            limit,
            repo_filter,
        } => {
            let app = app_with_repo_filter(app, repo_filter);
            let args = CrudArgs {
                method,
                path,
                symbol_id,
                limit,
            };
            if let Some(runtime) = runtime {
                trace::execute_crud(runtime.mcp.as_ref(), app.repo_filter.as_deref(), args)
            } else {
                trace::run_rendered(
                    &app,
                    &StorageContext::workspace_read_only(&app),
                    trace::TraceArgs {
                        registry: None,
                        storage: None,
                        command: TraceCommand::Crud(args),
                    },
                )
            }
        }
        DaemonRequest::Doctor { repo_filter } => {
            let app = app_with_repo_filter(app, repo_filter);
            if let Some(runtime) = runtime {
                let registry = RegistryStore::open(&runtime.registry_path)
                    .with_context(|| format!("opening {}", runtime.registry_path.display()))?;
                let storage = runtime.storage();
                doctor::execute(&registry, &storage, app.repo_filter.as_deref())
            } else {
                doctor::run_rendered(&app, &StorageContext::workspace_read_only(&app))
            }
        }
        DaemonRequest::Conventions { repo_filter } => {
            let app = app_with_repo_filter(app, repo_filter);
            if let Some(runtime) = runtime {
                let registry = RegistryStore::open(&runtime.registry_path)
                    .with_context(|| format!("opening {}", runtime.registry_path.display()))?;
                conventions::execute(
                    &registry,
                    runtime.stores.graph(),
                    app.repo_filter.as_deref(),
                )
            } else {
                conventions::run_rendered(&app, &StorageContext::workspace_read_only(&app))
            }
        }
        DaemonRequest::EventsTrace {
            subject,
            limit,
            repo_filter,
        } => {
            let app = app_with_repo_filter(app, repo_filter);
            let args = TraceArgs { subject, limit };
            if let Some(runtime) = runtime {
                let storage = runtime.storage();
                events::execute_trace(&storage, app.repo_filter.as_deref(), &args)
            } else {
                events::run_rendered(
                    &app,
                    &StorageContext::workspace_read_only(&app),
                    EventsArgs {
                        command: EventsCommand::Trace(args),
                    },
                )
            }
        }
        DaemonRequest::EventsBlastRadius {
            subject,
            limit,
            depth,
            repo_filter,
        } => {
            let app = app_with_repo_filter(app, repo_filter);
            let args = BlastRadiusArgs {
                subject,
                limit,
                depth,
            };
            if let Some(runtime) = runtime {
                let storage = runtime.storage();
                events::execute_blast_radius(&storage, app.repo_filter.as_deref(), &args)
            } else {
                events::run_rendered(
                    &app,
                    &StorageContext::workspace_read_only(&app),
                    EventsArgs {
                        command: EventsCommand::BlastRadius(args),
                    },
                )
            }
        }
        DaemonRequest::EventsOrphans { limit, repo_filter } => {
            let app = app_with_repo_filter(app, repo_filter);
            let args = OrphansArgs { limit };
            if let Some(runtime) = runtime {
                let storage = runtime.storage();
                events::execute_orphans(&storage, app.repo_filter.as_deref(), &args)
            } else {
                events::run_rendered(
                    &app,
                    &StorageContext::workspace_read_only(&app),
                    EventsArgs {
                        command: EventsCommand::Orphans(args),
                    },
                )
            }
        }
        DaemonRequest::Impact {
            symbol,
            limit,
            repo_filter,
        } => {
            let app = app_with_repo_filter(app, repo_filter);
            if let Some(runtime) = runtime {
                let storage = runtime.storage();
                impact::execute(
                    &storage,
                    app.repo_filter.as_deref(),
                    ImpactArgs {
                        registry: None,
                        storage: None,
                        symbol,
                        limit,
                    },
                )
            } else {
                impact::run_rendered(
                    &app,
                    &StorageContext::workspace_read_only(&app),
                    ImpactArgs {
                        registry: None,
                        storage: None,
                        symbol,
                        limit,
                    },
                )
            }
        }
        DaemonRequest::Pack {
            target,
            symbol,
            route_method,
            route_path,
            event_target,
            mode,
            limit,
            depth,
            budget_bytes,
            repo_filter,
        } => {
            let app = app_with_repo_filter(app, repo_filter);
            let args = PackArgs {
                registry: None,
                storage: None,
                target,
                symbol,
                route_method,
                route_path,
                event_target,
                mode: parse_pack_mode(&mode)?,
                limit,
                depth,
                budget_bytes,
            };
            if let Some(runtime) = runtime {
                pack::execute(runtime.mcp.as_ref(), app.repo_filter.clone(), &args)
            } else {
                pack::run_rendered(&app, &StorageContext::workspace_read_only(&app), &args)
            }
        }
    }
}

fn app_with_repo_filter(app: &AppContext, repo_filter: Option<String>) -> AppContext {
    let mut app = app.clone();
    app.repo_filter = repo_filter;
    app
}

fn parse_pack_mode(mode: &str) -> Result<PackModeArg> {
    match mode {
        "planning" => Ok(PackModeArg::Planning),
        "debug" => Ok(PackModeArg::Debug),
        "fix" => Ok(PackModeArg::Fix),
        "review" => Ok(PackModeArg::Review),
        "change_impact" => Ok(PackModeArg::ChangeImpact),
        _ => anyhow::bail!("unsupported pack mode `{mode}`"),
    }
}

fn daemon_dir(workspace_path: &Path) -> PathBuf {
    workspace_path.join(".gather-step")
}

fn daemon_socket_path(workspace_path: &Path) -> PathBuf {
    daemon_dir(workspace_path).join("daemon.sock")
}

fn daemon_pid_path(workspace_path: &Path) -> PathBuf {
    daemon_dir(workspace_path).join("daemon.pid")
}

#[cfg(unix)]
const MAX_DAEMON_REQUEST_BYTES: u64 = 64 * 1024;
#[cfg(unix)]
const DAEMON_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

#[cfg(unix)]
pub struct DaemonServer {
    app: AppContext,
    listener: tokio::net::UnixListener,
    pid_path: PathBuf,
    socket_path: PathBuf,
    allowed_uid: u32,
    runtime: Option<Arc<DaemonRuntime>>,
}

#[cfg(unix)]
impl DaemonServer {
    pub fn bind(app: &AppContext) -> Result<Self> {
        Self::bind_internal(app, None)
    }

    pub fn bind_with_runtime(app: &AppContext, runtime: DaemonRuntime) -> Result<Self> {
        Self::bind_internal(app, Some(Arc::new(runtime)))
    }

    fn bind_internal(app: &AppContext, runtime: Option<Arc<DaemonRuntime>>) -> Result<Self> {
        use std::os::unix::fs::DirBuilderExt as _;

        let daemon_dir = daemon_dir(&app.workspace_path);
        let socket_path = daemon_socket_path(&app.workspace_path);
        let pid_path = daemon_pid_path(&app.workspace_path);
        // Create the daemon directory with mode 0o700 atomically: the kernel
        // applies the mode before any other process can observe the directory,
        // eliminating the TOCTOU window that `create_dir_all` + `set_permissions`
        // leaves open.
        fs::DirBuilder::new()
            .mode(0o700)
            .recursive(true)
            .create(&daemon_dir)
            .with_context(|| format!("creating {}", daemon_dir.display()))?;

        cleanup_stale_daemon_files(&socket_path, &pid_path)?;

        let listener = tokio::net::UnixListener::bind(&socket_path)
            .with_context(|| format!("binding {}", socket_path.display()))?;
        let bind_cleanup = BindCleanupGuard {
            socket_path: Some(socket_path.clone()),
            pid_path: Some(pid_path.clone()),
        };
        set_socket_permissions(&socket_path)?;
        let allowed_uid = socket_file_uid(&socket_path)?;
        DaemonPidFile::for_current_process(&app.workspace_path)
            .write_to_path(&pid_path)
            .with_context(|| format!("writing {}", pid_path.display()))?;
        bind_cleanup.disarm();

        Ok(Self {
            app: app.clone(),
            listener,
            pid_path,
            socket_path,
            allowed_uid,
            runtime,
        })
    }

    pub async fn serve_until_cancelled(self, cancel: CancellationToken) -> Result<()> {
        use std::sync::Arc;
        use tokio::{sync::Semaphore, task::JoinSet};

        let DaemonServer {
            app,
            listener,
            pid_path,
            socket_path,
            allowed_uid,
            runtime,
        } = self;
        let _cleanup = RuntimeCleanupGuard::new(socket_path.clone(), pid_path.clone());

        // Limit to MAX_CONCURRENT_HANDLERS simultaneous handlers so that a
        // burst of clients cannot accumulate unboundedly and retain storage
        // handles after shutdown.  Handlers above the cap wait on the semaphore
        // rather than being rejected; the capacity is generous enough that
        // normal interactive use never blocks.
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_HANDLERS));
        let mut handlers: JoinSet<()> = JoinSet::new();

        loop {
            tokio::select! {
                () = cancel.cancelled() => {
                    // Hard shutdown: abort every in-flight handler so storage
                    // handles and blocking threads are not leaked.
                    handlers.abort_all();
                    // Drain the JoinSet so that all tasks are confirmed gone
                    // before returning (and before the caller drops stores).
                    while handlers.join_next().await.is_some() {}
                    return Ok(());
                }
                // Reap any handler that has already completed so the JoinSet
                // does not grow without bound across long-lived daemon runs.
                Some(_) = handlers.join_next(), if !handlers.is_empty() => {}
                accept = listener.accept() => {
                    let (stream, _) = accept.context("accepting daemon client")?;
                    let app = app.clone();
                    let runtime = runtime.clone();
                    let permit = Arc::clone(&semaphore)
                        .acquire_owned()
                        .await
                        .expect("semaphore should not be closed");
                    let handler_cancel = cancel.clone();
                    handlers.spawn(async move {
                        let _permit = permit; // released when handler exits
                        match tokio::time::timeout(
                            DAEMON_REQUEST_TIMEOUT,
                            handle_client(app, runtime, stream, allowed_uid, handler_cancel),
                        )
                        .await
                        {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => {
                                tracing::warn!(%error, "daemon client handling failed");
                            }
                            Err(_) => tracing::warn!("daemon client timed out"),
                        }
                    });
                }
            }
        }
    }
}

/// Maximum number of daemon request handlers that may run concurrently.
/// Handlers beyond this limit wait on the semaphore before starting.
#[cfg(unix)]
const MAX_CONCURRENT_HANDLERS: usize = 16;

#[cfg(not(unix))]
pub struct DaemonServer;

#[cfg(not(unix))]
impl DaemonServer {
    pub async fn bind(_app: &AppContext) -> Result<Self> {
        anyhow::bail!("daemon IPC is unsupported on this platform")
    }

    pub async fn bind_with_runtime(_app: &AppContext, _runtime: DaemonRuntime) -> Result<Self> {
        anyhow::bail!("daemon IPC is unsupported on this platform")
    }

    pub async fn serve_until_cancelled(self, _cancel: CancellationToken) -> Result<()> {
        let _ = self;
        anyhow::bail!("daemon IPC is unsupported on this platform")
    }
}

#[cfg(unix)]
async fn handle_client(
    app: AppContext,
    runtime: Option<Arc<DaemonRuntime>>,
    stream: tokio::net::UnixStream,
    allowed_uid: u32,
    cancel: CancellationToken,
) -> Result<()> {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

    authorize_peer(&stream, allowed_uid)?;
    let (read_half, mut write_half) = stream.into_split();
    let reader = BufReader::new(read_half);
    let mut request_line = String::new();
    reader
        .take(MAX_DAEMON_REQUEST_BYTES)
        .read_line(&mut request_line)
        .await
        .context("reading daemon request")?;

    let result = match serde_json::from_str::<DaemonRequest>(&request_line) {
        Ok(request) => {
            // Thread the cancellation token into the blocking closure so that
            // long-running phases (graph queries, pack generation) can observe
            // shutdown at natural phase boundaries rather than being abandoned
            // mid-flight with the blocking thread still running.
            match tokio::task::spawn_blocking(move || {
                // Check cancellation at the start of the blocking phase.
                if cancel.is_cancelled() {
                    return Ok(RenderedCommand::failure(
                        None,
                        Vec::new(),
                        "daemon shutting down".to_owned(),
                    ));
                }
                dispatch_request_with_runtime(&app, request, runtime.as_deref())
            })
            .await
            {
                Ok(Ok(rendered)) => rendered,
                Ok(Err(error)) => RenderedCommand::failure(None, Vec::new(), error.to_string()),
                Err(error) => RenderedCommand::failure(
                    None,
                    Vec::new(),
                    format!("daemon request task failed: {error}"),
                ),
            }
        }
        Err(error) => {
            RenderedCommand::failure(None, Vec::new(), format!("invalid daemon request: {error}"))
        }
    };

    let response = serde_json::to_string(&DaemonResponse { result })?;
    write_half
        .write_all(response.as_bytes())
        .await
        .context("writing daemon response")?;
    write_half
        .write_all(b"\n")
        .await
        .context("terminating daemon response")?;
    write_half
        .flush()
        .await
        .context("flushing daemon response")?;
    Ok(())
}

#[cfg(unix)]
fn cleanup_stale_daemon_files(socket_path: &Path, pid_path: &Path) -> Result<()> {
    let daemon_dir = socket_path
        .parent()
        .context("daemon socket path should live under a daemon directory")?;
    let daemon_dir_uid = daemon_dir_uid(daemon_dir)?;

    if socket_path.exists() {
        validate_cleanup_candidate(socket_path, CleanupTarget::Socket, daemon_dir_uid)?;
        match std::os::unix::net::UnixStream::connect(socket_path) {
            Ok(_) => {
                let pid = fs::read_to_string(pid_path)
                    .ok()
                    .and_then(|raw| serde_json::from_str::<DaemonPidFile>(&raw).ok())
                    .map_or_else(|| "unknown".to_owned(), |meta| meta.pid.to_string());
                anyhow::bail!(
                    "workspace daemon is already running at {} (pid={pid})",
                    socket_path.display()
                );
            }
            Err(_) => remove_cleanup_candidate(socket_path, CleanupTarget::Socket, daemon_dir_uid)?,
        }
    }
    if pid_path.exists() {
        remove_cleanup_candidate(pid_path, CleanupTarget::PidFile, daemon_dir_uid)?;
    }
    Ok(())
}

#[cfg(unix)]
#[derive(Clone, Copy)]
enum CleanupTarget {
    Socket,
    PidFile,
}

#[cfg(unix)]
fn daemon_dir_uid(daemon_dir: &Path) -> Result<u32> {
    use std::os::unix::fs::MetadataExt;

    let metadata = fs::symlink_metadata(daemon_dir)
        .with_context(|| format!("reading {}", daemon_dir.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        anyhow::bail!(
            "daemon directory `{}` must be a real directory owned by this workspace",
            daemon_dir.display()
        );
    }
    Ok(metadata.uid())
}

#[cfg(unix)]
fn socket_file_uid(socket_path: &Path) -> Result<u32> {
    use std::os::unix::fs::MetadataExt;

    Ok(fs::symlink_metadata(socket_path)
        .with_context(|| format!("reading {}", socket_path.display()))?
        .uid())
}

#[cfg(unix)]
fn validate_cleanup_candidate(
    path: &Path,
    target: CleanupTarget,
    daemon_dir_uid: u32,
) -> Result<()> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};

    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("reading {}", path.display()))?;
    let file_type = metadata.file_type();
    if file_type.is_symlink() {
        anyhow::bail!(
            "refusing to clean up symlinked daemon path `{}`",
            path.display()
        );
    }

    match target {
        CleanupTarget::Socket if !file_type.is_socket() => anyhow::bail!(
            "refusing to clean up non-socket daemon path `{}`",
            path.display()
        ),
        CleanupTarget::PidFile if !metadata.is_file() => anyhow::bail!(
            "refusing to clean up non-file daemon metadata `{}`",
            path.display()
        ),
        _ => {}
    }

    if metadata.uid() != daemon_dir_uid {
        anyhow::bail!(
            "refusing to clean up daemon path `{}` owned by a different uid",
            path.display()
        );
    }
    Ok(())
}

#[cfg(unix)]
fn remove_cleanup_candidate(path: &Path, target: CleanupTarget, daemon_dir_uid: u32) -> Result<()> {
    validate_cleanup_candidate(path, target, daemon_dir_uid)?;
    remove_if_exists(path)
}

#[cfg(unix)]
fn authorize_peer(stream: &tokio::net::UnixStream, allowed_uid: u32) -> Result<()> {
    let peer_uid = stream
        .peer_cred()
        .context("reading daemon peer credentials")?
        .uid();
    if peer_uid != allowed_uid {
        anyhow::bail!("daemon client uid {peer_uid} does not match socket owner {allowed_uid}");
    }
    Ok(())
}

#[cfg(unix)]
fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("removing {}", path.display())),
    }
}

#[cfg(unix)]
fn set_socket_permissions(socket_path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(socket_path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("setting permissions on {}", socket_path.display()))?;
    Ok(())
}

#[cfg(unix)]
struct BindCleanupGuard {
    socket_path: Option<PathBuf>,
    pid_path: Option<PathBuf>,
}

#[cfg(unix)]
impl BindCleanupGuard {
    fn disarm(mut self) {
        self.socket_path = None;
        self.pid_path = None;
    }
}

#[cfg(unix)]
impl Drop for BindCleanupGuard {
    fn drop(&mut self) {
        if let Some(path) = &self.socket_path {
            let _ = remove_if_exists(path);
        }
        if let Some(path) = &self.pid_path {
            let _ = remove_if_exists(path);
        }
    }
}

#[cfg(unix)]
struct RuntimeCleanupGuard {
    socket_path: PathBuf,
    pid_path: PathBuf,
}

#[cfg(unix)]
impl RuntimeCleanupGuard {
    fn new(socket_path: PathBuf, pid_path: PathBuf) -> Self {
        Self {
            socket_path,
            pid_path,
        }
    }
}

#[cfg(unix)]
impl Drop for RuntimeCleanupGuard {
    fn drop(&mut self) {
        for path in [&self.socket_path, &self.pid_path] {
            if let Err(error) = fs::remove_file(path)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                tracing::warn!(path = %path.display(), %error, "failed to remove daemon file");
            }
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::{
        fs,
        io::{BufRead, BufReader, Write},
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
    };

    use anyhow::Result;
    use indicatif::MultiProgress;
    use tokio_util::sync::CancellationToken;

    use super::{DaemonServer, daemon_pid_path, daemon_socket_path};
    use crate::{
        app::{AppContext, ColorModeArg},
        daemon_client::DaemonClient,
        daemon_protocol::{DaemonRequest, DaemonResponse},
    };

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

    struct TestWorkspace {
        root: PathBuf,
    }

    impl TestWorkspace {
        fn new(name: &str) -> Self {
            let id = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let root =
                PathBuf::from("/tmp").join(format!("gsd-{name}-{}-{id}", std::process::id()));
            fs::create_dir_all(&root).expect("test workspace should exist");
            Self { root }
        }

        fn path(&self) -> &Path {
            &self.root
        }
    }

    impl Drop for TestWorkspace {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

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

    fn bind_daemon_or_skip(app: &AppContext) -> Result<Option<DaemonServer>> {
        match DaemonServer::bind(app) {
            Ok(daemon) => Ok(Some(daemon)),
            Err(error) if unix_socket_bind_is_not_permitted(&error) => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn unix_socket_bind_is_not_permitted(error: &anyhow::Error) -> bool {
        error.to_string().contains("binding")
            && error.chain().any(|cause| {
                cause
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|io| io.kind() == std::io::ErrorKind::PermissionDenied)
            })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn daemon_serves_status_requests_and_cleans_up_runtime_files() -> Result<()> {
        let workspace = TestWorkspace::new("status");
        let app = app(workspace.path());
        let Some(daemon) = bind_daemon_or_skip(&app)? else {
            return Ok(());
        };
        let socket_path = daemon_socket_path(workspace.path());
        let pid_path = daemon_pid_path(workspace.path());
        let cancel = CancellationToken::new();
        let daemon_task = tokio::spawn(daemon.serve_until_cancelled(cancel.clone()));

        let client = DaemonClient::try_connect(workspace.path())?.expect("daemon should connect");
        let rendered = client.call(&DaemonRequest::Status { repo_filter: None })?;
        assert!(rendered.error.is_none());
        assert!(rendered.payload.is_some());

        cancel.cancel();
        daemon_task.await??;
        assert!(!socket_path.exists());
        assert!(!pid_path.exists());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn daemon_wraps_invalid_requests_as_rendered_failures() -> Result<()> {
        let workspace = TestWorkspace::new("invalid");
        let app = app(workspace.path());
        let Some(daemon) = bind_daemon_or_skip(&app)? else {
            return Ok(());
        };
        let socket_path = daemon_socket_path(workspace.path());
        let cancel = CancellationToken::new();
        let daemon_task = tokio::spawn(daemon.serve_until_cancelled(cancel.clone()));

        let mut stream = std::os::unix::net::UnixStream::connect(&socket_path)?;
        stream.write_all(b"not-json\n")?;
        stream.flush()?;

        let mut response_line = String::new();
        let mut reader = BufReader::new(stream);
        reader.read_line(&mut response_line)?;
        let response: DaemonResponse = serde_json::from_str(&response_line)?;
        assert!(
            response
                .result
                .error
                .as_deref()
                .is_some_and(|message| message.contains("invalid daemon request"))
        );

        cancel.cancel();
        daemon_task.await??;
        Ok(())
    }
}
