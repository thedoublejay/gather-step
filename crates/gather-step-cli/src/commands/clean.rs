use std::{
    fs,
    io::{self, BufRead, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use clap::Args;
use serde::Serialize;

use crate::{app::AppContext, path_safety};

#[derive(Debug, Args, PartialEq, Eq)]
pub struct CleanArgs {
    #[arg(long, help = "Override the workspace-local registry path")]
    pub registry: Option<std::path::PathBuf>,
    #[arg(long, help = "Override the workspace-local storage directory")]
    pub storage: Option<std::path::PathBuf>,
    #[arg(
        long,
        short = 'y',
        help = "Skip the destructive-action confirmation prompt"
    )]
    pub yes: bool,
}

#[derive(Debug, Serialize)]
struct CleanOutput {
    event: &'static str,
    registry_path: String,
    storage_root: String,
}

pub fn run(app: &AppContext, args: CleanArgs) -> Result<()> {
    let output = app.output();
    let defaults = app.workspace_paths();
    let registry_path = args.registry.unwrap_or(defaults.registry_path);
    let storage_root = args.storage.unwrap_or(defaults.storage_root);
    let registry_path = validate_generated_path_override(app, &registry_path, "registry")?;
    let storage_root = validate_generated_path_override(app, &storage_root, "storage")?;

    confirm_destructive_clean(app, &registry_path, &storage_root, args.yes)?;
    reset_index_state(&registry_path, &storage_root)?;

    let payload = CleanOutput {
        event: "clean_completed",
        registry_path: registry_path.display().to_string(),
        storage_root: storage_root.display().to_string(),
    };
    output.emit(&payload)?;
    output.line(format!(
        "Removed indexed state at {} and {}",
        registry_path.display(),
        storage_root.display()
    ));
    Ok(())
}

pub fn reset_index_state(registry_path: &Path, storage_root: &Path) -> Result<()> {
    remove_generated_path(storage_root)
        .with_context(|| format!("removing storage state at {}", storage_root.display()))?;
    remove_generated_path(registry_path)
        .with_context(|| format!("removing registry state at {}", registry_path.display()))?;
    Ok(())
}

/// Validate that every path in `paths` resolves inside `canonical_root`,
/// then remove each one that exists on disk.
///
/// This function validates ALL entries before performing any removal: if any
/// path escapes the workspace root the whole operation is rejected and nothing
/// is deleted.
///
/// Returns the canonical form of each path for logging.
pub fn validate_and_clean_generated_paths(
    paths: &[PathBuf],
    canonical_root: &Path,
) -> Result<Vec<PathBuf>> {
    // Step 1: validate every path before touching the filesystem.
    let mut canonical_paths = Vec::with_capacity(paths.len());
    for path in paths {
        let canonical = path_safety::canonicalize_inside_workspace(path, canonical_root)
            .with_context(|| {
                format!(
                    "generated path `{}` does not resolve inside workspace root `{}`",
                    path.display(),
                    canonical_root.display()
                )
            })?;
        canonical_paths.push(canonical);
    }

    // Step 2: remove only after every path is confirmed safe.
    for path in &canonical_paths {
        remove_generated_path(path)
            .with_context(|| format!("removing generated path {}", path.display()))?;
    }

    Ok(canonical_paths)
}

fn confirm_destructive_clean(
    app: &AppContext,
    registry_path: &Path,
    storage_root: &Path,
    yes: bool,
) -> Result<()> {
    if yes {
        return Ok(());
    }

    if app.json_output {
        bail!("`clean` is destructive; pass `--yes` to confirm when using `--json`");
    }

    let mut stdout = io::stdout().lock();
    let mut stdin = io::stdin().lock();
    confirm_destructive_clean_io(&mut stdin, &mut stdout, registry_path, storage_root)
}

fn confirm_destructive_clean_io(
    stdin: &mut impl BufRead,
    stdout: &mut impl Write,
    registry_path: &Path,
    storage_root: &Path,
) -> Result<()> {
    writeln!(
        stdout,
        "Warning: this will permanently delete indexed state."
    )?;
    writeln!(stdout, "  registry: {}", registry_path.display())?;
    writeln!(stdout, "  storage: {}", storage_root.display())?;
    write!(stdout, "Type `clean` to proceed: ")?;
    stdout.flush()?;

    let mut confirmation = String::new();
    stdin.read_line(&mut confirmation)?;
    if confirmation.trim() != "clean" {
        bail!("clean aborted");
    }

    Ok(())
}

fn validate_generated_path_override(app: &AppContext, path: &Path, label: &str) -> Result<PathBuf> {
    let canonical_workspace = path_safety::canonical_workspace_root(&app.workspace_path)
        .with_context(|| {
            format!(
                "canonicalizing workspace root `{}`",
                app.workspace_path.display()
            )
        })?;
    let generated_root = canonical_workspace.join(".gather-step");

    let resolved = path_safety::canonicalize_inside_workspace(path, &canonical_workspace)
        .with_context(|| {
            format!(
                "`clean --{label}` path `{}` must resolve inside the workspace",
                path.display()
            )
        })?;

    if !resolved.starts_with(&generated_root) {
        bail!(
            "`clean --{label}` must stay inside the workspace-generated state root at {}",
            generated_root.display()
        );
    }
    Ok(resolved)
}

fn remove_generated_path(path: &Path) -> Result<()> {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return Ok(());
    };

    if metadata.file_type().is_dir() {
        fs::remove_dir_all(path)?;
    } else {
        fs::remove_file(path)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use indicatif::MultiProgress;

    use std::io::Cursor;

    use super::{
        confirm_destructive_clean_io, reset_index_state, validate_generated_path_override,
    };
    use crate::app::AppContext;

    static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(label: &str) -> Self {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time should be monotonic")
                .as_nanos();
            let counter = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("gather-step-clean-{label}-{unique}-{counter}"));
            fs::create_dir_all(&path).expect("temp dir should be created");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn app(workspace_path: PathBuf) -> AppContext {
        AppContext {
            workspace_path,
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
    fn reset_index_state_removes_registry_and_storage() {
        let temp = TestDir::new("state");
        let generated_root = temp.path().join(".gather-step");
        let storage_root = generated_root.join("storage");
        let registry_path = generated_root.join("registry.json");

        fs::create_dir_all(&storage_root).expect("storage dir should exist");
        fs::write(storage_root.join("graph.redb"), b"graph").expect("graph file should exist");
        fs::write(storage_root.join("metadata.sqlite"), b"metadata")
            .expect("metadata file should exist");
        fs::write(&registry_path, b"{}").expect("registry file should exist");

        reset_index_state(&registry_path, &storage_root).expect("reset should succeed");

        assert!(!storage_root.exists());
        assert!(!registry_path.exists());
        assert!(generated_root.exists());
    }

    #[test]
    fn reset_index_state_ignores_missing_paths() {
        let temp = TestDir::new("missing");
        let generated_root = temp.path().join(".gather-step");
        let storage_root = generated_root.join("storage");
        let registry_path = generated_root.join("registry.json");

        reset_index_state(&registry_path, &storage_root).expect("reset should succeed");
        assert!(!storage_root.exists());
        assert!(!registry_path.exists());
    }

    #[test]
    fn clean_rejects_registry_override_outside_generated_root() {
        let temp = TestDir::new("registry-override");
        let app = app(temp.path().to_path_buf());
        let outside = temp.path().join("registry.json");

        let error = validate_generated_path_override(&app, &outside, "registry")
            .expect_err("override outside generated root should fail");
        assert!(
            error.to_string().contains("workspace-generated state root"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn clean_accepts_paths_inside_generated_root() {
        let temp = TestDir::new("generated-root");
        let app = app(temp.path().to_path_buf());
        let allowed = temp.path().join(".gather-step/storage/custom");

        let resolved = validate_generated_path_override(&app, &allowed, "storage")
            .expect("override inside generated root should succeed");

        // `canonicalize_inside_workspace` returns a canonicalized path.  On
        // macOS the test's tempdir lives under `/var/folders/...` but `/var` is
        // a symlink to `/private/var`, so the resolved path starts with
        // `/private/var/...` while `allowed` starts with `/var/...`.  Compare
        // against the canonical form of the expected path.
        let canonical_workspace = std::fs::canonicalize(temp.path()).unwrap();
        let expected = canonical_workspace.join(".gather-step/storage/custom");
        assert_eq!(resolved, expected);
    }

    #[cfg(unix)]
    #[test]
    fn clean_rejects_override_with_intermediate_symlink() {
        let temp = TestDir::new("symlink-escape");
        let workspace = temp.path().join("workspace");
        let generated_root = workspace.join(".gather-step");
        let outside = temp.path().join("outside");
        fs::create_dir_all(&generated_root).expect("generated root should exist");
        fs::create_dir_all(outside.join("victim")).expect("outside victim should exist");
        std::os::unix::fs::symlink(&outside, generated_root.join("link"))
            .expect("symlink should create");

        let app = app(workspace);
        let attack = app.workspace_path.join(".gather-step/link/victim");
        let error = validate_generated_path_override(&app, &attack, "storage")
            .expect_err("intermediate symlink should be rejected");

        // `canonicalize` resolves the `.gather-step/link` symlink and the
        // resulting canonical path is `<tempdir>/outside/victim`, which lies
        // outside the canonical workspace root — `canonicalize_inside_workspace`
        // surfaces that as a `PathEscape` error.
        let msg = error.to_string();
        assert!(
            msg.contains("escapes workspace") || msg.contains("symlink"),
            "unexpected error: {msg}"
        );
        assert!(outside.join("victim").exists());
    }

    #[test]
    fn clean_confirmation_prompt_accepts_clean_token() {
        let mut stdin = Cursor::new(b"clean\n");
        let mut stdout = Vec::new();
        let registry_path = PathBuf::from("/tmp/workspace/.gather-step/registry.json");
        let storage_root = PathBuf::from("/tmp/workspace/.gather-step/storage");

        confirm_destructive_clean_io(&mut stdin, &mut stdout, &registry_path, &storage_root)
            .expect("confirmation should accept clean token");

        let printed = String::from_utf8(stdout).expect("prompt should be utf8");
        assert!(printed.contains("Warning: this will permanently delete indexed state."));
        assert!(printed.contains("Type `clean` to proceed:"));
    }
}
