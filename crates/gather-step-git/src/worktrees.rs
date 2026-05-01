//! Detached-worktree helpers for the `pr-review` command.
//!
//! Provides thin wrappers around `git worktree add/remove/prune` so the
//! review mode can materialise a target commit on disk without touching the
//! user's working copy.

use std::path::{Path, PathBuf};

use thiserror::Error;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A detached worktree owned by review mode.
///
/// Drop is intentionally a no-op: cleanup is explicit so callers can opt into
/// `--keep-cache` behaviour without leaking handles.  Call [`remove_worktree`]
/// when the worktree is no longer needed.
#[derive(Debug)]
pub struct ReviewWorktree {
    /// The source repository (the user's checkout).
    pub repo: PathBuf,
    /// The worktree directory that was created.
    pub root: PathBuf,
    /// The SHA that was checked out (40-char lowercase hex).
    pub sha: String,
}

/// Errors returned by worktree operations.
#[derive(Debug, Error)]
pub enum WorktreeError {
    #[error("source repository not found at {path}")]
    RepoNotFound { path: PathBuf },

    #[error("target worktree path already exists: {path}")]
    TargetExists { path: PathBuf },

    #[error("git worktree command failed: {message}")]
    GitOperation { message: String },
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Run `git` with the given arguments (no `-C` dir).
fn run_git_raw(args: &[&str]) -> Result<String, WorktreeError> {
    let out = std::process::Command::new("git")
        .args(args)
        .output()
        .map_err(|e| WorktreeError::GitOperation {
            message: format!("failed to spawn git: {e}"),
        })?;

    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    } else {
        Err(WorktreeError::GitOperation {
            message: String::from_utf8_lossy(&out.stderr).trim().to_owned(),
        })
    }
}

/// Run `git -C <dir> <args…>`.
fn run_git(dir: &Path, args: &[&str]) -> Result<String, WorktreeError> {
    let dir_str = dir.to_string_lossy();
    let mut full: Vec<&str> = vec!["-C", &dir_str];
    full.extend_from_slice(args);
    run_git_raw(&full)
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Create a detached worktree for `sha` at `target` from the repository at
/// `repo`.
///
/// `target` must **not** already exist; this function refuses to clobber an
/// existing path.  After creation the worktree HEAD is verified to match
/// `sha`.
pub fn create_detached_worktree(
    repo: &Path,
    target: &Path,
    sha: &str,
) -> Result<ReviewWorktree, WorktreeError> {
    // --- preconditions -------------------------------------------------------
    if !repo.is_dir() {
        return Err(WorktreeError::RepoNotFound {
            path: repo.to_owned(),
        });
    }
    if target.exists() {
        return Err(WorktreeError::TargetExists {
            path: target.to_owned(),
        });
    }

    // --- create the worktree -------------------------------------------------
    let target_str = target.to_string_lossy();
    run_git(
        repo,
        &["worktree", "add", "--detach", &target_str, sha],
    )?;

    // --- verify HEAD ---------------------------------------------------------
    // `git rev-parse HEAD` always emits lowercase 40-char hex — no case
    // conversion needed; we compare case-insensitively as a belt-and-suspenders
    // measure (some Git builds on macOS historically uppercased short SHAs).
    let head = run_git(target, &["rev-parse", "HEAD"])
        .map(|s| s.trim().to_owned())
        .inspect_err(|_| {
            // Best-effort cleanup: if rev-parse fails the worktree was still
            // created on disk, so remove it before surfacing the error.
            let _ = run_git(
                repo,
                &["worktree", "remove", "--force", &target_str],
            );
            let _ = run_git(repo, &["worktree", "prune"]);
            let _ = std::fs::remove_dir_all(target);
        })?;

    let expected = sha.trim();
    if !head.eq_ignore_ascii_case(expected) {
        // Cleanup then error.
        let _ = run_git(
            repo,
            &["worktree", "remove", "--force", &target_str],
        );
        let _ = run_git(repo, &["worktree", "prune"]);
        let _ = std::fs::remove_dir_all(target);
        return Err(WorktreeError::GitOperation {
            message: format!(
                "worktree HEAD {head} does not match requested SHA {expected}"
            ),
        });
    }

    Ok(ReviewWorktree {
        repo: repo.to_owned(),
        root: target.to_owned(),
        sha: head,
    })
}

/// Remove a worktree from disk and prune the registry entry.
///
/// Idempotent — calling twice with the same `wt` is safe.
pub fn remove_worktree(wt: &ReviewWorktree) -> Result<(), WorktreeError> {
    let root_str = wt.root.to_string_lossy();

    // `git worktree remove --force` handles modified files.  If the entry is
    // already gone git prints an error but we treat that as success.
    match run_git(
        &wt.repo,
        &["worktree", "remove", "--force", &root_str],
    ) {
        Ok(_) => {}
        Err(WorktreeError::GitOperation { ref message })
            if message.contains("is not a working tree")
                || message.contains("is not a worktree")
                || message.contains("No such file") =>
        {
            // Already removed — treat as success.
        }
        Err(e) => return Err(e),
    }

    // Prune stale registry entries.
    let _ = run_git(&wt.repo, &["worktree", "prune"]);

    // Belt-and-suspenders: remove the directory if it still lingers.
    if wt.root.exists() {
        std::fs::remove_dir_all(&wt.root).map_err(|e| WorktreeError::GitOperation {
            message: format!("remove_dir_all({}) failed: {e}", wt.root.display()),
        })?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Helpers — shell out to `git` to build test repos; mirrors refs.rs pattern.
    // -----------------------------------------------------------------------

    mod helpers {
        use std::path::Path;

        pub fn run_git(dir: &Path, args: &[&str]) -> String {
            let out = std::process::Command::new("git")
                .arg("-C")
                .arg(dir)
                .args(args)
                .output()
                .unwrap_or_else(|e| panic!("git {args:?} failed to spawn: {e}"));
            assert!(
                out.status.success(),
                "git {args:?} failed:\n{}",
                String::from_utf8_lossy(&out.stderr)
            );
            String::from_utf8(out.stdout).expect("utf8")
        }

        pub fn init_repo(dir: &Path) {
            run_git(dir, &["init", "--quiet", "--initial-branch=main"]);
            run_git(dir, &["config", "user.email", "test@example.com"]);
            run_git(dir, &["config", "user.name", "Test"]);
            run_git(dir, &["config", "commit.gpgsign", "false"]);
        }

        pub fn commit_file(dir: &Path, name: &str, contents: &str, message: &str) -> String {
            std::fs::write(dir.join(name), contents).expect("write");
            run_git(dir, &["add", name]);
            run_git(dir, &["commit", "--quiet", "-m", message]);
            run_git(dir, &["rev-parse", "HEAD"]).trim().to_string()
        }

        /// Check whether `git` is available on PATH.
        pub fn git_available() -> bool {
            std::process::Command::new("git")
                .arg("--version")
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false)
        }
    }

    // -----------------------------------------------------------------------
    // Test 1 — creates a worktree at the requested SHA
    // -----------------------------------------------------------------------

    /// Requires `git` on PATH; skipped otherwise.
    #[test]
    #[ignore = "requires git on PATH"]
    fn creates_worktree_at_requested_sha() {
        if !helpers::git_available() {
            return;
        }

        let repo_tmp = tempfile::tempdir().expect("repo tempdir");
        let repo = repo_tmp.path();
        helpers::init_repo(repo);

        let first_sha = helpers::commit_file(repo, "a.txt", "first", "first commit");
        let _second_sha = helpers::commit_file(repo, "b.txt", "second", "second commit");

        let wt_tmp = tempfile::tempdir().expect("wt tempdir");
        // tempdir creates the directory — we need a sub-path that doesn't exist yet.
        let target = wt_tmp.path().join("review-wt");

        let wt = create_detached_worktree(repo, &target, &first_sha)
            .expect("create_detached_worktree");

        assert!(target.is_dir(), "worktree directory should exist");
        assert!(
            wt.sha.eq_ignore_ascii_case(&first_sha),
            "sha mismatch: {} vs {}",
            wt.sha,
            first_sha,
        );
        assert_eq!(wt.root, target);

        // Verify via an independent git call.
        let head = helpers::run_git(&target, &["rev-parse", "HEAD"])
            .trim()
            .to_string();
        assert_eq!(head, first_sha);

        remove_worktree(&wt).expect("cleanup");
    }

    // -----------------------------------------------------------------------
    // Test 2 — refuses to clobber an existing target
    // -----------------------------------------------------------------------

    #[test]
    #[ignore = "requires git on PATH"]
    fn refuses_existing_target() {
        if !helpers::git_available() {
            return;
        }

        let repo_tmp = tempfile::tempdir().expect("repo tempdir");
        let repo = repo_tmp.path();
        helpers::init_repo(repo);
        let sha = helpers::commit_file(repo, "a.txt", "v1", "init");

        // Create a directory that collides with the intended target.
        let existing = repo_tmp.path().join("already-exists");
        std::fs::create_dir_all(&existing).expect("mkdir");

        let err = create_detached_worktree(repo, &existing, &sha)
            .expect_err("should fail when target exists");

        assert!(
            matches!(err, WorktreeError::TargetExists { .. }),
            "expected TargetExists, got {err:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Test 3 — errors on missing repo
    // -----------------------------------------------------------------------

    #[test]
    fn errors_on_missing_repo() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let missing_repo = tmp.path().join("no-such-repo");
        let target = tmp.path().join("target");

        let err = create_detached_worktree(&missing_repo, &target, "deadbeef")
            .expect_err("should fail for missing repo");

        assert!(
            matches!(err, WorktreeError::RepoNotFound { .. }),
            "expected RepoNotFound, got {err:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Test 4 — remove_worktree is idempotent
    // -----------------------------------------------------------------------

    #[test]
    #[ignore = "requires git on PATH"]
    fn remove_worktree_is_idempotent() {
        if !helpers::git_available() {
            return;
        }

        let repo_tmp = tempfile::tempdir().expect("repo tempdir");
        let repo = repo_tmp.path();
        helpers::init_repo(repo);
        let sha = helpers::commit_file(repo, "a.txt", "v1", "init");

        let wt_tmp = tempfile::tempdir().expect("wt tempdir");
        let target = wt_tmp.path().join("review-wt");

        let wt = create_detached_worktree(repo, &target, &sha)
            .expect("create_detached_worktree");

        remove_worktree(&wt).expect("first remove");
        remove_worktree(&wt).expect("second remove should be idempotent");
    }

    // -----------------------------------------------------------------------
    // Test 5 — remove deletes files and prunes registry
    // -----------------------------------------------------------------------

    #[test]
    #[ignore = "requires git on PATH"]
    fn remove_worktree_deletes_files_and_prunes_registry() {
        if !helpers::git_available() {
            return;
        }

        let repo_tmp = tempfile::tempdir().expect("repo tempdir");
        let repo = repo_tmp.path();
        helpers::init_repo(repo);
        let sha = helpers::commit_file(repo, "a.txt", "v1", "init");

        let wt_tmp = tempfile::tempdir().expect("wt tempdir");
        let target = wt_tmp.path().join("review-wt");

        let wt = create_detached_worktree(repo, &target, &sha)
            .expect("create_detached_worktree");

        remove_worktree(&wt).expect("remove");

        // Directory must be gone.
        assert!(!target.exists(), "worktree directory should be removed");

        // Registry must no longer list the worktree path.
        let list = helpers::run_git(repo, &["worktree", "list", "--porcelain"]);
        let target_str = target.to_string_lossy();
        assert!(
            !list.contains(target_str.as_ref()),
            "worktree should be pruned from registry; list:\n{list}"
        );
    }
}
