//! Shared test helpers for `gather-step-cli` unit tests.
//!
//! Provides `indexed_fixture` — a function that copies the fixture workspace
//! into a temp directory, indexes it, and returns a
//! [`crate::storage_context::StorageContext`] pointing at that isolated
//! storage.  This is the test equivalent of the bench `indexed_context()`
//! helper in `benches/query_bench.rs`.
//!
//! Only compiled in `#[cfg(test)]` builds.
use std::{
    env, fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use indicatif::MultiProgress;

use crate::{
    app::{AppContext, ColorModeArg},
    storage_context::StorageContext,
};
use gather_step_core::{GatherStepConfig, RegistryStore};
use gather_step_storage::{IndexingOptions, index_workspace_with_storage};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A temporary directory that removes itself on drop.
pub(crate) struct TempDir {
    path: PathBuf,
}

impl TempDir {
    pub(crate) fn new(name: &str) -> Self {
        let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!(
            "gather-step-test-{name}-{}-{id}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("temp dir should exist");
        Self { path }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures")
        .canonicalize()
        .expect("fixture root should resolve")
}

fn copy_dir_all(from: &Path, to: &Path) {
    fs::create_dir_all(to).expect("destination directory should exist");
    for entry in fs::read_dir(from).expect("source directory should be readable") {
        let entry = entry.expect("directory entry should load");
        if entry.file_name() == ".gather-step" {
            continue;
        }
        let file_type = entry.file_type().expect("file type should load");
        assert!(
            !file_type.is_symlink(),
            "fixture must not contain symlinks (found: {})",
            entry.path().display()
        );
        let from_path = entry.path();
        let to_path = to.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_all(&from_path, &to_path);
        } else {
            fs::copy(&from_path, &to_path).expect("fixture file should copy");
        }
    }
}

/// Stage, index, and return a `StorageContext::review(...)` pointing at a
/// freshly-indexed copy of the fixture workspace.
///
/// The `TempDir` must be held for the lifetime of the context — drop order in
/// the returned tuple keeps `ctx` first so its file handles close before the
/// directory is removed.
///
/// `run_id` is an arbitrary string identifying this test run (e.g. `"pr-test-status"`).
pub(crate) fn indexed_fixture(name: &str, run_id: &str) -> (StorageContext, TempDir) {
    let workspace = TempDir::new(name);
    copy_dir_all(&fixture_root(), workspace.path());

    let config_path = workspace.path().join("gather-step.config.yaml");
    let config =
        GatherStepConfig::from_yaml_file(&config_path).expect("fixture config should load");

    // Use a layout that matches StorageContext::review's contract:
    // storage at <workspace>/storage/  (so graph is at <workspace>/storage/graph.redb)
    let storage_root = workspace.path().join("storage");
    fs::create_dir_all(&storage_root).expect("storage dir should exist");

    let registry_path = workspace.path().join("registry.json");
    let mut registry = RegistryStore::open(&registry_path).expect("registry should open");

    index_workspace_with_storage(
        &config,
        workspace.path(),
        &mut registry,
        &storage_root,
        IndexingOptions::default(),
    )
    .expect("fixture workspace should index");

    let ctx = StorageContext::review(
        workspace.path().to_owned(),
        registry_path,
        storage_root,
        run_id,
    );

    (ctx, workspace)
}

/// Minimal `AppContext` for unit tests.  Workspace path is set to the given
/// directory but storage paths are determined by the `StorageContext` passed
/// to `run_rendered`, not by `app.workspace_paths()`.
pub(crate) fn test_app(workspace_path: PathBuf) -> AppContext {
    AppContext {
        workspace_path,
        repo_filter: None,
        json_output: true,
        no_interactive: true,
        stdin_is_tty: false,
        stdout_is_tty: false,
        stderr_is_tty: false,
        ci_env_set: true,
        color_mode: ColorModeArg::Never,
        show_banner: false,
        multi_progress: MultiProgress::new(),
    }
}
