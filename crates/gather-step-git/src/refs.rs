//! Ref resolution helpers for the `pr-review` command.
//!
//! Provides lightweight wrappers around `gix` to resolve branch names, tags,
//! SHAs, and symbolic refs into concrete 40-char hex object IDs, compute merge
//! bases, and enumerate changed files between two commits.

use std::path::{Path, PathBuf};

use gix::{bstr::ByteSlice as _, object::tree::diff::Change};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A fully resolved git ref: an input spec mapped to a concrete SHA.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRef {
    /// The original input string (branch name, short SHA, "HEAD", …).
    pub input: String,
    /// 40-char lowercase hex SHA.
    pub sha: String,
    /// Full ref name (e.g. `"refs/heads/main"`) when `input` named a symbolic
    /// ref. `None` when `input` was already a bare SHA.
    pub symbolic: Option<String>,
}

/// A pair of resolved refs that together describe a commit range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRange {
    pub base: ResolvedRef,
    pub head: ResolvedRef,
}

/// How a file changed between two commits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeKind {
    Added,
    Modified,
    Deleted,
    Renamed,
}

/// One file's change between two tree states.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangedFile {
    /// Repo-relative path using forward slashes.
    pub path: String,
    pub change_kind: ChangeKind,
    /// Previous path for renames.
    pub old_path: Option<String>,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum RefResolveError {
    #[error("repository not found at {path}")]
    RepoNotFound { path: PathBuf },

    #[error("could not resolve ref {input:?} in {repo}")]
    CannotResolve { input: String, repo: PathBuf },

    #[error("git operation failed in {repo}: {message}")]
    GitOperation { repo: PathBuf, message: String },
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn open_repo(repo: &Path) -> Result<gix::Repository, RefResolveError> {
    gix::open(repo).map_err(|_| RefResolveError::RepoNotFound {
        path: repo.to_owned(),
    })
}

fn git_op<E: std::fmt::Display>(repo: &Path, err: E) -> RefResolveError {
    RefResolveError::GitOperation {
        repo: repo.to_owned(),
        message: err.to_string(),
    }
}

/// Parse a hex string into a `gix::ObjectId` without touching gix internals.
fn parse_object_id(repo: &Path, sha: &str) -> Result<gix::ObjectId, RefResolveError> {
    sha.parse::<gix::ObjectId>()
        .map_err(|e| git_op(repo, format!("invalid object id {sha:?}: {e}")))
}

fn path_to_string(bytes: &gix::bstr::BStr) -> String {
    bytes.to_str_lossy().replace('\\', "/")
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Resolve `input` (branch, tag, short SHA, "HEAD", …) to a [`ResolvedRef`].
pub fn resolve_ref(repo: &Path, input: &str) -> Result<ResolvedRef, RefResolveError> {
    let r = open_repo(repo)?;

    let id = r
        .rev_parse_single(input)
        .map_err(|_| RefResolveError::CannotResolve {
            input: input.to_owned(),
            repo: repo.to_owned(),
        })?;

    let sha = id.to_string();

    // Determine symbolic: if the input is already a bare object id, skip the
    // ref lookup — we know there's no symbolic name.
    let symbolic = if input.parse::<gix::ObjectId>().is_ok() {
        None
    } else {
        // Try to find a ref with this name; ignore errors — not every input is
        // a ref (e.g. "HEAD^", "v1.0~3", …).
        r.try_find_reference(input)
            .ok()
            .flatten()
            .map(|reference| reference.name().as_bstr().to_string())
    };

    Ok(ResolvedRef {
        input: input.to_owned(),
        sha,
        symbolic,
    })
}

/// Resolve both ends of a commit range.
pub fn resolve_range(
    repo: &Path,
    base: &str,
    head: &str,
) -> Result<ResolvedRange, RefResolveError> {
    Ok(ResolvedRange {
        base: resolve_ref(repo, base)?,
        head: resolve_ref(repo, head)?,
    })
}

/// Return the best merge-base SHA between commits `a` and `b`.
pub fn merge_base(repo: &Path, a: &str, b: &str) -> Result<String, RefResolveError> {
    let r = open_repo(repo)?;

    let a_sha = resolve_ref(repo, a)?.sha;
    let b_sha = resolve_ref(repo, b)?.sha;

    let a_id = parse_object_id(repo, &a_sha)?;
    let b_id = parse_object_id(repo, &b_sha)?;

    let base_id = r.merge_base(a_id, b_id).map_err(|e| git_op(repo, e))?;

    Ok(base_id.to_string())
}

/// Return all files that changed between `base_sha` and `head_sha`.
///
/// Both arguments must be full or abbreviated commit SHAs resolvable in `repo`.
pub fn changed_files(
    repo: &Path,
    base_sha: &str,
    head_sha: &str,
) -> Result<Vec<ChangedFile>, RefResolveError> {
    let r = open_repo(repo)?;

    let base_id = parse_object_id(repo, base_sha)?;
    let head_id = parse_object_id(repo, head_sha)?;

    let base_commit = r
        .find_object(base_id)
        .map_err(|e| git_op(repo, e))?
        .into_commit();
    let head_commit = r
        .find_object(head_id)
        .map_err(|e| git_op(repo, e))?
        .into_commit();

    let base_tree = base_commit.tree().map_err(|e| git_op(repo, e))?;
    let head_tree = head_commit.tree().map_err(|e| git_op(repo, e))?;

    let mut results: Vec<ChangedFile> = Vec::new();

    base_tree
        .changes()
        .map_err(|e| git_op(repo, e))?
        .for_each_to_obtain_tree(
            &head_tree,
            |change| -> Result<gix::object::tree::diff::Action, std::convert::Infallible> {
                let entry = match change {
                    Change::Addition { location, .. } => Some(ChangedFile {
                        path: path_to_string(location),
                        change_kind: ChangeKind::Added,
                        old_path: None,
                    }),
                    Change::Modification { location, .. } => Some(ChangedFile {
                        path: path_to_string(location),
                        change_kind: ChangeKind::Modified,
                        old_path: None,
                    }),
                    Change::Deletion { location, .. } => Some(ChangedFile {
                        path: path_to_string(location),
                        change_kind: ChangeKind::Deleted,
                        old_path: None,
                    }),
                    Change::Rewrite {
                        source_location,
                        location,
                        ..
                    } => Some(ChangedFile {
                        path: path_to_string(location),
                        change_kind: ChangeKind::Renamed,
                        old_path: Some(path_to_string(source_location)),
                    }),
                };
                if let Some(f) = entry {
                    results.push(f);
                }
                Ok(std::ops::ControlFlow::Continue(()))
            },
        )
        .map_err(|e| git_op(repo, e))?;

    Ok(results)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Helpers — shell out to `git` to build test repos without touching gix
    // internals.
    // -----------------------------------------------------------------------

    mod helpers {
        use std::path::Path;

        pub fn run_git(dir: &Path, args: &[&str]) -> String {
            let out = std::process::Command::new("git")
                .current_dir(dir)
                .args(args)
                .output()
                .expect("git must be on PATH");
            assert!(
                out.status.success(),
                "git {args:?} failed: {}",
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
    }

    // -----------------------------------------------------------------------
    // resolve_ref tests
    // -----------------------------------------------------------------------

    #[test]
    fn resolve_ref_resolves_branch_name() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path();
        helpers::init_repo(dir);
        let expected_sha = helpers::commit_file(dir, "a.txt", "hello", "init");

        let resolved = resolve_ref(dir, "main").expect("resolve main");
        assert_eq!(resolved.sha, expected_sha);
        assert_eq!(resolved.symbolic.as_deref(), Some("refs/heads/main"));
    }

    #[test]
    fn resolve_ref_resolves_full_sha() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path();
        helpers::init_repo(dir);
        let sha = helpers::commit_file(dir, "a.txt", "hello", "init");

        let resolved = resolve_ref(dir, &sha).expect("resolve full sha");
        assert_eq!(resolved.sha, sha);
        assert_eq!(resolved.symbolic, None);
    }

    #[test]
    fn resolve_ref_resolves_short_sha() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path();
        helpers::init_repo(dir);
        let sha = helpers::commit_file(dir, "a.txt", "hello", "init");
        let short = &sha[..7];

        let resolved = resolve_ref(dir, short).expect("resolve short sha");
        assert_eq!(resolved.sha, sha);
    }

    #[test]
    fn resolve_ref_resolves_head() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path();
        helpers::init_repo(dir);
        let expected_sha = helpers::commit_file(dir, "a.txt", "hello", "init");

        let resolved = resolve_ref(dir, "HEAD").expect("resolve HEAD");
        assert_eq!(resolved.sha, expected_sha);
    }

    #[test]
    fn resolve_ref_errors_on_unknown_ref() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path();
        helpers::init_repo(dir);
        helpers::commit_file(dir, "a.txt", "hello", "init");

        let err = resolve_ref(dir, "does-not-exist").expect_err("should fail");
        assert!(
            matches!(err, RefResolveError::CannotResolve { .. }),
            "expected CannotResolve, got {err:?}"
        );
    }

    // -----------------------------------------------------------------------
    // merge_base test
    // -----------------------------------------------------------------------

    #[test]
    fn merge_base_returns_common_ancestor() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path();
        helpers::init_repo(dir);

        // base commit on main
        let base_sha = helpers::commit_file(dir, "a.txt", "base", "base");

        // branch A
        helpers::run_git(dir, &["checkout", "-b", "branch-a"]);
        helpers::commit_file(dir, "b.txt", "branch-a", "branch-a commit");
        let a_head = helpers::run_git(dir, &["rev-parse", "HEAD"])
            .trim()
            .to_string();

        // branch B (from base)
        helpers::run_git(dir, &["checkout", "main"]);
        helpers::run_git(dir, &["checkout", "-b", "branch-b"]);
        helpers::commit_file(dir, "c.txt", "branch-b", "branch-b commit");
        let b_head = helpers::run_git(dir, &["rev-parse", "HEAD"])
            .trim()
            .to_string();

        let mb = merge_base(dir, &a_head, &b_head).expect("merge_base");
        assert_eq!(mb, base_sha);
    }

    // -----------------------------------------------------------------------
    // changed_files tests
    // -----------------------------------------------------------------------

    #[test]
    fn changed_files_added_and_modified() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path();
        helpers::init_repo(dir);

        let base_sha = helpers::commit_file(dir, "a.txt", "original", "base");
        // modify a.txt and add b.txt
        let head_sha = {
            std::fs::write(dir.join("a.txt"), "modified").expect("write");
            helpers::run_git(dir, &["add", "a.txt"]);
            std::fs::write(dir.join("b.txt"), "new file").expect("write");
            helpers::run_git(dir, &["add", "b.txt"]);
            helpers::run_git(dir, &["commit", "--quiet", "-m", "second"]);
            helpers::run_git(dir, &["rev-parse", "HEAD"])
                .trim()
                .to_string()
        };

        let files = changed_files(dir, &base_sha, &head_sha).expect("changed_files");

        let has_modified_a = files
            .iter()
            .any(|f| f.path == "a.txt" && f.change_kind == ChangeKind::Modified);
        let has_added_b = files
            .iter()
            .any(|f| f.path == "b.txt" && f.change_kind == ChangeKind::Added);

        assert!(has_modified_a, "expected Modified a.txt in {files:?}");
        assert!(has_added_b, "expected Added b.txt in {files:?}");
    }

    #[test]
    fn changed_files_deletion() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path();
        helpers::init_repo(dir);

        let base_sha = helpers::commit_file(dir, "a.txt", "to be deleted", "base");
        let head_sha = {
            helpers::run_git(dir, &["rm", "a.txt"]);
            helpers::run_git(dir, &["commit", "--quiet", "-m", "delete"]);
            helpers::run_git(dir, &["rev-parse", "HEAD"])
                .trim()
                .to_string()
        };

        let files = changed_files(dir, &base_sha, &head_sha).expect("changed_files");
        let has_deleted_a = files
            .iter()
            .any(|f| f.path == "a.txt" && f.change_kind == ChangeKind::Deleted);
        assert!(has_deleted_a, "expected Deleted a.txt in {files:?}");
    }
}
