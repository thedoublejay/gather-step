//! Shared test fixtures for the `pr_review` module and its CLI command.
//!
//! Five test modules (`cache`, `cleanup`, `engine`, `index_runner`, and
//! `commands::pr_review`) historically defined their own near-identical
//! `TempDir` helper. They are unified here so a Drop change (e.g. adding
//! `git worktree prune` to clean up registered worktrees) cannot drift
//! between sites.
//!
//! Module is `#[cfg(test)]`-gated so the helpers are excluded from release
//! builds.

#![cfg(test)]

use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Disposable temp directory under `std::env::temp_dir()`.
///
/// Drop removes the directory recursively after best-effort `git worktree
/// prune` (cleans up worktrees registered against the directory if it ever
/// hosted any — the prune is a no-op when the directory is not under a git
/// repo, since `git -C` will simply fail and we swallow the error).
pub(crate) struct TempDir {
    path: PathBuf,
}

impl TempDir {
    /// Create a unique temp directory tagged with `label`.
    ///
    /// Combines a process-wide atomic counter with the current monotonic
    /// nanos and the pid so concurrent test binaries cannot collide.
    pub(crate) fn new(label: &str) -> Self {
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.subsec_nanos());
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("gs-test-{label}-{pid}-{nanos}-{counter}"));
        std::fs::create_dir_all(&path).expect("create temp dir");
        Self { path }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        // Best-effort: prune any git worktrees this directory had registered
        // before deleting it, otherwise the source repo accumulates dangling
        // refs in `.git/worktrees/`. `git -C <non-repo>` fails harmlessly.
        let _ = Command::new("git")
            .args(["-C", &self.path.to_string_lossy(), "worktree", "prune"])
            .output();
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
