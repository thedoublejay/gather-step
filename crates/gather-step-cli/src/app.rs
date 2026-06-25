use std::{
    env,
    io::{self, IsTerminal, Write},
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
};

use anyhow::{Context, Result};
use clap::ValueEnum;
use console::{set_colors_enabled, set_colors_enabled_stderr, style};
use indicatif::{MultiProgress, ProgressDrawTarget};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::{
    EnvFilter,
    fmt::{MakeWriter, time::ChronoLocal},
    layer::{Context as LayerContext, Layer},
    prelude::*,
};

use crate::{commands::Cli, path_safety};

const BANNER: &str = include_str!("../assets/banner.txt");
static TELEMETRY_WARN_COUNT: AtomicU32 = AtomicU32::new(0);
static TELEMETRY_ERROR_COUNT: AtomicU32 = AtomicU32::new(0);
static TELEMETRY_RECOVERY_EVENT: AtomicBool = AtomicBool::new(false);

#[derive(Debug)]
struct TelemetryCounterLayer;

impl<S> Layer<S> for TelemetryCounterLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: LayerContext<'_, S>) {
        match *event.metadata().level() {
            Level::WARN => {
                TELEMETRY_WARN_COUNT.fetch_add(1, Ordering::Relaxed);
            }
            Level::ERROR => {
                TELEMETRY_ERROR_COUNT.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }
}

#[must_use]
pub fn telemetry_counts() -> (u32, u32) {
    (
        TELEMETRY_WARN_COUNT.load(Ordering::Relaxed),
        TELEMETRY_ERROR_COUNT.load(Ordering::Relaxed),
    )
}

pub fn mark_telemetry_recovery_event() {
    TELEMETRY_RECOVERY_EVENT.store(true, Ordering::Relaxed);
}

pub fn reset_telemetry_run_state() {
    TELEMETRY_WARN_COUNT.store(0, Ordering::Relaxed);
    TELEMETRY_ERROR_COUNT.store(0, Ordering::Relaxed);
    TELEMETRY_RECOVERY_EVENT.store(false, Ordering::Relaxed);
}

#[must_use]
pub fn telemetry_recovery_event() -> bool {
    TELEMETRY_RECOVERY_EVENT.load(Ordering::Relaxed)
}

#[expect(
    clippy::struct_excessive_bools,
    reason = "AppContext centralizes independent CLI and environment flags"
)]
#[derive(Clone, Debug)]
pub struct AppContext {
    pub workspace_path: PathBuf,
    /// Generated-state base directory (registry, storage, graph, locks, daemon
    /// socket/pid). Equals `<workspace_path>/.gather-step` unless overridden by
    /// `GATHER_STEP_DATA_DIR`. Resolved once in [`AppContext::from_cli`].
    pub data_dir: PathBuf,
    /// Where [`AppContext::data_dir`] came from.
    pub data_dir_source: DataDirSource,
    pub repo_filter: Option<String>,
    pub json_output: bool,
    pub no_interactive: bool,
    pub stdin_is_tty: bool,
    pub stdout_is_tty: bool,
    pub stderr_is_tty: bool,
    pub ci_env_set: bool,
    pub color_mode: ColorModeArg,
    pub show_banner: bool,
    pub multi_progress: MultiProgress,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum ColorModeArg {
    #[default]
    Auto,
    Always,
    Never,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum DepthArg {
    Level1,
    Level2,
    Level3,
    Full,
}

#[derive(Clone, Debug)]
pub struct WorkspacePaths {
    pub config_path: PathBuf,
    pub registry_path: PathBuf,
    pub storage_root: PathBuf,
    pub graph_path: PathBuf,
}

/// Terminal output funnel.
///
/// `emit` writes structured JSON to stdout only when `--json` is set; `line`
/// writes human text to stdout only when `--json` is not set. Callers can
/// invoke both without branching: whichever matches the current mode will run
/// and the other is a no-op.
#[derive(Clone, Debug)]
pub struct Output {
    json: bool,
}

impl Output {
    #[must_use]
    pub fn new(json: bool) -> Self {
        Self { json }
    }

    #[must_use]
    pub fn is_json(&self) -> bool {
        self.json
    }

    #[expect(
        clippy::print_stdout,
        reason = "Output::emit is the single structured-JSON funnel for CLI commands"
    )]
    pub fn emit<T: serde::Serialize>(&self, value: &T) -> Result<()> {
        if self.json {
            println!("{}", serde_json::to_string(value)?);
        }
        Ok(())
    }

    #[expect(
        clippy::print_stdout,
        reason = "Output::line is the single human-text funnel for CLI commands"
    )]
    pub fn line(&self, message: impl AsRef<str>) {
        if !self.json {
            println!("{}", message.as_ref());
        }
    }
}

impl AppContext {
    pub fn from_cli(cli: &Cli, multi_progress: MultiProgress) -> Result<Self> {
        // absolutize first so we have an absolute path to canonicalize, even
        // when the user passes a relative path like ".".
        let raw = absolutize(&cli.workspace)
            .with_context(|| format!("resolving workspace path {}", cli.workspace.display()))?;
        let workspace_path = path_safety::canonical_workspace_root(&raw)
            .with_context(|| format!("canonicalizing workspace root {}", raw.display()))?;

        let (data_dir, data_dir_source) = resolve_data_dir(&workspace_path);

        Ok(Self {
            workspace_path,
            data_dir,
            data_dir_source,
            repo_filter: cli.repo.clone(),
            json_output: cli.json,
            no_interactive: cli.no_interactive,
            stdin_is_tty: std::io::stdin().is_terminal(),
            stdout_is_tty: std::io::stdout().is_terminal(),
            stderr_is_tty: std::io::stderr().is_terminal(),
            ci_env_set: std::env::var("CI").is_ok_and(|value| !value.is_empty()),
            color_mode: cli.color,
            show_banner: !cli.no_banner,
            multi_progress,
        })
    }

    #[must_use]
    pub fn output(&self) -> Output {
        Output::new(self.json_output)
    }

    #[must_use]
    pub fn is_interactive(&self) -> bool {
        self.stdin_is_tty
            && self.stdout_is_tty
            && !self.json_output
            && !self.ci_env_set
            && !self.no_interactive
    }

    #[must_use]
    pub fn progress_is_visible(&self) -> bool {
        self.stderr_is_tty && !self.ci_env_set && !self.json_output
    }

    #[must_use]
    pub fn color_enabled(&self) -> bool {
        color_enabled_for(self.color_mode, self.stdout_is_tty, self.json_output)
    }

    #[must_use]
    pub fn tui_is_available(&self) -> bool {
        self.stdin_is_tty
            && self.stdout_is_tty
            && self.stderr_is_tty
            && !self.json_output
            && !self.no_interactive
    }

    #[must_use]
    pub fn workspace_paths(&self) -> WorkspacePaths {
        workspace_paths_for(&self.workspace_path, &self.data_dir)
    }
}

/// Initialize tracing and return a `MultiProgress` shared with progress-bar
/// commands. The fmt layer's writer routes through `MultiProgress::println`
/// so log lines do not clobber active progress bars.
///
/// In `--json` mode the bars are drawn to a hidden target and tracing emits
/// structured JSON straight to stderr — no coordination is needed because no
/// bars render.
pub fn init_tracing(cli: &Cli) -> Result<MultiProgress> {
    let env_filter = build_env_filter(cli.verbose)?;
    configure_console_colors(cli);

    if cli.json {
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_writer(io::stderr)
            .with_timer(ChronoLocal::rfc_3339())
            .with_target(false)
            .with_ansi(false)
            .json();
        tracing_subscriber::registry()
            .with(env_filter)
            .with(TelemetryCounterLayer)
            .with(fmt_layer)
            .init();
        return Ok(MultiProgress::with_draw_target(ProgressDrawTarget::hidden()));
    }

    // Suppress bars when stderr is not a TTY or when CI is set to a non-empty value.
    let stderr_is_tty = io::stderr().is_terminal();
    let ci_env_set = std::env::var("CI").is_ok_and(|v| !v.is_empty());
    let draw_visible = stderr_is_tty && !ci_env_set;

    let multi_progress = if draw_visible {
        MultiProgress::new()
    } else {
        MultiProgress::with_draw_target(ProgressDrawTarget::hidden())
    };
    let writer = MultiProgressWriter::new(multi_progress.clone());
    // Interactive runs drop the leading timestamp so log lines align
    // visually with the rest of the CLI output. Operators who need
    // machine-parseable timestamps should run with `--json`, which
    // restores the RFC 3339 timer in the JSON formatter above.
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(writer)
        .without_time()
        .with_target(false)
        .with_ansi(color_enabled_for(cli.color, stderr_is_tty, cli.json));
    tracing_subscriber::registry()
        .with(env_filter)
        .with(TelemetryCounterLayer)
        .with(fmt_layer)
        .init();
    Ok(multi_progress)
}

fn configure_console_colors(cli: &Cli) {
    let stdout_is_tty = io::stdout().is_terminal();
    let stderr_is_tty = io::stderr().is_terminal();
    set_colors_enabled(color_enabled_for(cli.color, stdout_is_tty, cli.json));
    set_colors_enabled_stderr(color_enabled_for(cli.color, stderr_is_tty, cli.json));
}

fn color_enabled_for(mode: ColorModeArg, stream_is_tty: bool, json_output: bool) -> bool {
    if json_output {
        return false;
    }
    match mode {
        ColorModeArg::Always => true,
        ColorModeArg::Never => false,
        ColorModeArg::Auto => {
            if env::var_os("NO_COLOR").is_some() {
                return false;
            }
            if env::var_os("FORCE_COLOR").is_some() {
                return true;
            }
            stream_is_tty && env::var("TERM").map_or(true, |term| term != "dumb")
        }
    }
}

fn build_env_filter(verbose: u8) -> Result<EnvFilter> {
    if let Ok(user_filter) = EnvFilter::try_from_env("GATHER_STEP_LOG") {
        return Ok(user_filter);
    }

    EnvFilter::try_new(format!(
        "{},tantivy::indexer::segment_manager=error",
        default_log_level(verbose)
    ))
    .context("building tracing env filter")
}

/// `MakeWriter` that routes formatted log lines through
/// `indicatif::MultiProgress::println`, which is the only safe way to write to
/// stderr while progress bars are active. Each event is buffered in memory and
/// flushed line-by-line on `flush`/`drop`.
#[derive(Clone)]
struct MultiProgressWriter {
    multi: MultiProgress,
}

impl MultiProgressWriter {
    fn new(multi: MultiProgress) -> Self {
        Self { multi }
    }
}

impl<'a> MakeWriter<'a> for MultiProgressWriter {
    type Writer = MultiProgressLineWriter;

    fn make_writer(&'a self) -> Self::Writer {
        MultiProgressLineWriter {
            multi: self.multi.clone(),
            buf: Mutex::new(Vec::new()),
        }
    }
}

struct MultiProgressLineWriter {
    multi: MultiProgress,
    buf: Mutex<Vec<u8>>,
}

impl Write for MultiProgressLineWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if let Ok(mut guard) = self.buf.lock() {
            guard.extend_from_slice(bytes);
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        let bytes = match self.buf.lock() {
            Ok(mut guard) => std::mem::take(&mut *guard),
            Err(_) => return Ok(()),
        };
        if bytes.is_empty() {
            return Ok(());
        }
        let text = String::from_utf8_lossy(&bytes);
        for line in text.split_inclusive('\n') {
            let trimmed = line.strip_suffix('\n').unwrap_or(line);
            // println coordinates with active bars; ignore I/O errors during
            // flush since tracing has no path to surface them.
            let _ = self.multi.println(trimmed);
        }
        Ok(())
    }
}

impl Drop for MultiProgressLineWriter {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

#[expect(
    clippy::print_stderr,
    reason = "maybe_print_banner is the single stderr banner funnel; callers cannot funnel it further"
)]
pub fn maybe_print_banner(app: &AppContext) {
    if app.json_output || !app.show_banner || !io::stderr().is_terminal() {
        return;
    }

    let footer = format!(
        "v{}  ·  © 2026 JJ Adonis  ·  https://gatherstep.dev/",
        env!("CARGO_PKG_VERSION")
    );
    let banner_width = BANNER
        .lines()
        .map(|line| line.chars().count())
        .max()
        .unwrap_or(footer.chars().count());
    let footer_padding = banner_width.saturating_sub(footer.chars().count()) / 2;

    eprintln!();
    eprintln!("{}", style(BANNER).blue().bold());
    eprintln!("{}{}", " ".repeat(footer_padding), style(footer).cyan());
    eprintln!();
}

fn default_log_level(verbosity: u8) -> &'static str {
    match verbosity {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    }
}

fn absolutize(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(env::current_dir()?.join(path))
    }
}

/// Derive the generated-state paths for a workspace from its resolved
/// generated-state base directory (`data_dir`).
///
/// `config_path` stays workspace-relative — `gather-step.config.yaml` is user
/// config, not index state, and is never relocated by `GATHER_STEP_DATA_DIR`.
/// Everything else (registry, storage, graph, and the lock tree under
/// `storage/locks`) hangs off `data_dir`. This is the single source of truth
/// for the layout; both [`AppContext::workspace_paths`] and the pr-review
/// baseline resolver go through it.
#[must_use]
pub fn workspace_paths_for(workspace_path: &Path, data_dir: &Path) -> WorkspacePaths {
    let config_path = workspace_path.join("gather-step.config.yaml");
    let registry_path = data_dir.join("registry.json");
    let storage_root = data_dir.join("storage");
    let graph_path = storage_root.join("graph.redb");

    WorkspacePaths {
        config_path,
        registry_path,
        storage_root,
        graph_path,
    }
}

/// Where the resolved generated-state base directory came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataDirSource {
    /// `<workspace>/.gather-step` — no override set.
    Default,
    /// The `GATHER_STEP_DATA_DIR` environment variable.
    Env,
}

/// Name of the environment variable that relocates the generated-state base
/// directory (registry, storage, graph, locks, daemon socket/pid) for the
/// current invocation's primary workspace. See the v5.4.1 dev-isolation design.
pub const DATA_DIR_ENV: &str = "GATHER_STEP_DATA_DIR";

/// Resolve the generated-state base directory for `workspace_path`, honoring
/// `GATHER_STEP_DATA_DIR`.
///
/// Precedence is completed by callers: explicit `--storage`/`--registry` flags
/// override the returned base, which overrides the `<workspace>/.gather-step`
/// default. Resolving the env value here (once, at `AppContext` construction)
/// keeps [`AppContext::workspace_paths`] a pure function over `data_dir`.
#[must_use]
pub fn resolve_data_dir(workspace_path: &Path) -> (PathBuf, DataDirSource) {
    let env_value = env::var(DATA_DIR_ENV).ok();
    resolve_data_dir_from(env_value.as_deref(), workspace_path)
}

/// Pure core of [`resolve_data_dir`]: resolve from an explicit env value rather
/// than reading the process environment, so it is testable without env races.
///
/// An empty value is treated as unset (mirrors the `CI` handling in
/// [`AppContext::from_cli`] and the XDG/Docker/uv convention). A set value is
/// absolutized (relative values resolve against the current directory) and its
/// longest existing prefix canonicalized — this resolves a symlinked ancestor
/// (e.g. macOS `/tmp` -> `/private/tmp`) before the symlink guard walks it.
fn resolve_data_dir_from(
    env_value: Option<&str>,
    workspace_path: &Path,
) -> (PathBuf, DataDirSource) {
    match env_value.filter(|value| !value.is_empty()) {
        Some(value) => {
            let raw = PathBuf::from(value);
            let absolute = absolutize(&raw).unwrap_or(raw);
            let resolved = path_safety::canonicalize_existing_prefix(&absolute).unwrap_or(absolute);
            (resolved, DataDirSource::Env)
        }
        None => (workspace_path.join(".gather-step"), DataDirSource::Default),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{
        ColorModeArg, DataDirSource, build_env_filter, color_enabled_for, resolve_data_dir_from,
        workspace_paths_for,
    };

    #[test]
    fn workspace_paths_default_layout_is_unchanged() {
        let ws = Path::new("/tmp/ws");
        let data_dir = ws.join(".gather-step");
        let paths = workspace_paths_for(ws, &data_dir);
        assert_eq!(paths.config_path, ws.join("gather-step.config.yaml"));
        assert_eq!(
            paths.registry_path,
            ws.join(".gather-step").join("registry.json")
        );
        assert_eq!(paths.storage_root, ws.join(".gather-step").join("storage"));
        assert_eq!(
            paths.graph_path,
            ws.join(".gather-step").join("storage").join("graph.redb")
        );
    }

    #[test]
    fn workspace_paths_relocate_under_data_dir() {
        let ws = Path::new("/tmp/ws");
        let data_dir = Path::new("/tmp/dev");
        let paths = workspace_paths_for(ws, data_dir);
        // Config stays workspace-relative — it is user config, not index state.
        assert_eq!(paths.config_path, ws.join("gather-step.config.yaml"));
        assert_eq!(paths.registry_path, data_dir.join("registry.json"));
        assert_eq!(paths.storage_root, data_dir.join("storage"));
        assert_eq!(
            paths.graph_path,
            data_dir.join("storage").join("graph.redb")
        );
    }

    #[test]
    fn resolve_data_dir_defaults_to_dot_gather_step_when_unset() {
        let ws = Path::new("/tmp/ws");
        let (dir, source) = resolve_data_dir_from(None, ws);
        assert_eq!(dir, ws.join(".gather-step"));
        assert_eq!(source, DataDirSource::Default);
    }

    #[test]
    fn resolve_data_dir_treats_empty_env_value_as_unset() {
        let ws = Path::new("/tmp/ws");
        let (dir, source) = resolve_data_dir_from(Some(""), ws);
        assert_eq!(dir, ws.join(".gather-step"));
        assert_eq!(source, DataDirSource::Default);
    }

    #[cfg(unix)]
    #[test]
    fn resolve_data_dir_canonicalizes_symlinked_prefix_of_env_value() {
        let tmp = tempfile::tempdir().unwrap();
        let base = std::fs::canonicalize(tmp.path()).unwrap();
        let real = base.join("real");
        std::fs::create_dir_all(&real).unwrap();
        let link = base.join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();

        // The data-dir leaf does not exist yet; the symlinked existing ancestor
        // (`link`) must resolve to its real target (mirrors macOS /tmp).
        let env_value = link.join("gs-dev");
        let (dir, source) =
            resolve_data_dir_from(Some(env_value.to_str().unwrap()), Path::new("/tmp/ws"));
        assert_eq!(dir, real.join("gs-dev"));
        assert_eq!(source, DataDirSource::Env);
    }

    #[test]
    fn default_filter_suppresses_tantivy_segment_manager_warnings() {
        let rendered = build_env_filter(0)
            .expect("env filter should build")
            .to_string();
        assert!(rendered.contains("tantivy::indexer::segment_manager=error"));
    }

    #[test]
    fn explicit_color_modes_override_tty_detection() {
        assert!(color_enabled_for(ColorModeArg::Always, false, false));
        assert!(!color_enabled_for(ColorModeArg::Never, true, false));
        assert!(!color_enabled_for(ColorModeArg::Always, true, true));
    }
}
