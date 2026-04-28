use std::{
    env,
    io::{self, IsTerminal, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

use anyhow::{Context, Result};
use clap::ValueEnum;
use console::{set_colors_enabled, set_colors_enabled_stderr, style};
use indicatif::{MultiProgress, ProgressDrawTarget};
use tracing_subscriber::{EnvFilter, fmt::MakeWriter};

use crate::{commands::Cli, path_safety};

const BANNER: &str = include_str!("../assets/banner.txt");

#[expect(
    clippy::struct_excessive_bools,
    reason = "AppContext centralizes independent CLI and environment flags"
)]
#[derive(Clone, Debug)]
pub struct AppContext {
    pub workspace_path: PathBuf,
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

        Ok(Self {
            workspace_path,
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
        let config_path = self.workspace_path.join("gather-step.config.yaml");
        let registry_path = self
            .workspace_path
            .join(".gather-step")
            .join("registry.json");
        let storage_root = self.workspace_path.join(".gather-step").join("storage");
        let graph_path = storage_root.join("graph.redb");

        WorkspacePaths {
            config_path,
            registry_path,
            storage_root,
            graph_path,
        }
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
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_writer(io::stderr)
            .with_target(false)
            .with_ansi(false)
            .json()
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
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_writer(writer)
        .with_target(false)
        .with_ansi(color_enabled_for(cli.color, stderr_is_tty, cli.json))
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

    eprintln!();
    eprintln!("{}", style(BANNER).dim());
    eprintln!(
        "{}",
        style(format!(
            "v{}  ·  © 2026 JJ Adonis",
            env!("CARGO_PKG_VERSION")
        ))
        .dim()
    );
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

#[cfg(test)]
mod tests {
    use super::{ColorModeArg, build_env_filter, color_enabled_for};

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
