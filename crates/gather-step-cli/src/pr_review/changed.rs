//! Per-repo changed-file resolution for polyrepo `pr-review`.
//!
//! In a polyrepo layout each configured repo is its own git repository. The
//! same `--base`/`--head` ref names are resolved independently in every repo's
//! history; repo-local changed paths are then prefixed with the repo's
//! config-relative path so the aggregated set is workspace-relative (and still
//! prefix-matches `config.repos[*].path` for `compute_affected_repos`). Repos
//! whose refs do not resolve, or that have no changes in range, are skipped
//! with a recorded note rather than failing the whole run.

use gather_step_git::refs::{
    ChangedFile, RefResolveError, changed_files, merge_base, resolve_range,
};

use super::target::{ReviewRepoSpec, ReviewTarget};

/// One repo's resolved range and its changed files, rewritten to
/// workspace-relative paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoChange {
    pub repo_name: String,
    pub base_sha: String,
    pub head_sha: String,
    pub diff_base_sha: String,
    /// Changed files with workspace-relative paths (config prefix applied).
    pub files: Vec<ChangedFile>,
}

/// A configured repo excluded from the review, with a human-readable reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedRepo {
    pub repo_name: String,
    pub reason: String,
}

/// Aggregated per-repo diff across the whole review target.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PerRepoChanges {
    pub repos: Vec<RepoChange>,
    pub skipped: Vec<SkippedRepo>,
}

impl PerRepoChanges {
    /// All changed files across every reviewed repo, workspace-relative.
    #[must_use]
    pub fn all_files(&self) -> Vec<ChangedFile> {
        self.repos
            .iter()
            .flat_map(|repo| repo.files.iter().cloned())
            .collect()
    }
}

/// Resolve `base`/`head` independently in each repo of `target`, returning the
/// repo-tagged, workspace-relative changed files. Repos whose refs do not
/// resolve, or that have no changes in range, are recorded in `skipped`.
#[must_use]
pub fn resolve_per_repo_changes(target: &ReviewTarget, base: &str, head: &str) -> PerRepoChanges {
    let mut out = PerRepoChanges::default();
    for repo in &target.repos {
        match resolve_one(repo, base, head) {
            Ok(Some(change)) => out.repos.push(change),
            Ok(None) => out.skipped.push(SkippedRepo {
                repo_name: repo.repo_name.clone(),
                reason: format!("no changes in `{base}..{head}`"),
            }),
            Err(reason) => out.skipped.push(SkippedRepo {
                repo_name: repo.repo_name.clone(),
                reason,
            }),
        }
    }
    out
}

fn resolve_one(
    repo: &ReviewRepoSpec,
    base: &str,
    head: &str,
) -> Result<Option<RepoChange>, String> {
    let root = repo.git_repo_root.as_path();
    let resolved = resolve_range(root, base, head).map_err(|err| describe(&err))?;
    let base_sha = resolved.base.sha;
    let head_sha = resolved.head.sha;
    let diff_base_sha = merge_base(root, &base_sha, &head_sha).map_err(|err| describe(&err))?;
    let local = changed_files(root, &diff_base_sha, &head_sha).map_err(|err| describe(&err))?;
    if local.is_empty() {
        return Ok(None);
    }
    let files = local
        .into_iter()
        .map(|mut cf| {
            cf.path = join_prefix(&repo.repo_path, &cf.path);
            cf.old_path = cf.old_path.map(|old| join_prefix(&repo.repo_path, &old));
            cf
        })
        .collect();
    Ok(Some(RepoChange {
        repo_name: repo.repo_name.clone(),
        base_sha,
        head_sha,
        diff_base_sha,
        files,
    }))
}

fn join_prefix(prefix: &str, path: &str) -> String {
    if prefix.is_empty() {
        path.to_owned()
    } else {
        format!("{prefix}/{path}")
    }
}

fn describe(err: &RefResolveError) -> String {
    match err {
        RefResolveError::RepoNotFound { .. } => "not a git repository".to_owned(),
        RefResolveError::CannotResolve { input, .. } => format!("ref `{input}` did not resolve"),
        RefResolveError::GitOperation { message, .. } => format!("git error: {message}"),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process::Command,
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::{join_prefix, resolve_per_repo_changes};
    use crate::pr_review::target::{ReviewRepoSpec, ReviewTarget};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(label: &str) -> Self {
            let id = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path =
                env::temp_dir().join(format!("gs-perrepo-{label}-{}-{id}", std::process::id()));
            fs::create_dir_all(&path).expect("temp dir");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn git(dir: &Path, args: &[&str]) {
        let status = Command::new("git")
            .args(args)
            .current_dir(dir)
            .status()
            .expect("git should run");
        assert!(status.success(), "git {} failed", args.join(" "));
    }

    fn git_out(dir: &Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .expect("git should run");
        assert!(out.status.success(), "git {} failed", args.join(" "));
        String::from_utf8_lossy(&out.stdout).trim().to_owned()
    }

    fn init_repo(dir: &Path) {
        fs::create_dir_all(dir).expect("repo dir");
        git(dir, &["init", "-q"]);
        git(dir, &["config", "commit.gpgsign", "false"]);
        git(dir, &["config", "user.email", "test@example.com"]);
        git(dir, &["config", "user.name", "Test"]);
    }

    fn commit(dir: &Path, message: &str) {
        git(dir, &["add", "-A"]);
        git(dir, &["commit", "-q", "-m", message]);
    }

    fn spec(name: &str, path: &str, root: PathBuf) -> ReviewRepoSpec {
        ReviewRepoSpec {
            repo_name: name.to_owned(),
            repo_path: path.to_owned(),
            git_repo_root: root,
        }
    }

    #[test]
    fn join_prefix_handles_root_and_child() {
        assert_eq!(join_prefix("", "src/a.ts"), "src/a.ts");
        assert_eq!(join_prefix("repo_a", "src/a.ts"), "repo_a/src/a.ts");
    }

    #[test]
    fn resolves_changes_per_repo_and_skips_untouched() {
        let ws = TempDir::new("multi");
        let repo_a = ws.path().join("repo_a");
        let repo_b = ws.path().join("repo_b");
        let repo_c = ws.path().join("repo_c");

        // repo_a: base branch + feature commit that adds a file.
        init_repo(&repo_a);
        fs::write(repo_a.join("README.md"), "a\n").expect("seed");
        commit(&repo_a, "base");
        let base_branch = git_out(&repo_a, &["rev-parse", "--abbrev-ref", "HEAD"]);
        git(&repo_a, &["checkout", "-q", "-b", "feature"]);
        fs::create_dir_all(repo_a.join("src")).expect("src");
        fs::write(repo_a.join("src/a.ts"), "export const a = 1;\n").expect("change");
        commit(&repo_a, "add a.ts");
        let repo_a_head = git_out(&repo_a, &["rev-parse", "HEAD"]);

        // repo_b: feature branch exists but points at the same commit as base →
        // no changes in range.
        init_repo(&repo_b);
        fs::write(repo_b.join("README.md"), "b\n").expect("seed");
        commit(&repo_b, "base");
        git(&repo_b, &["checkout", "-q", "-b", "feature"]);

        // repo_c: no feature branch at all → ref will not resolve.
        init_repo(&repo_c);
        fs::write(repo_c.join("README.md"), "c\n").expect("seed");
        commit(&repo_c, "base");

        let target = ReviewTarget {
            index_workspace_root: ws.path().to_path_buf(),
            repos: vec![
                spec("repo_a", "repo_a", repo_a.clone()),
                spec("repo_b", "repo_b", repo_b.clone()),
                spec("repo_c", "repo_c", repo_c.clone()),
            ],
        };

        let result = resolve_per_repo_changes(&target, &base_branch, "feature");

        assert_eq!(result.repos.len(), 1, "only repo_a changed");
        let change = &result.repos[0];
        assert_eq!(change.repo_name, "repo_a");
        assert_eq!(change.head_sha, repo_a_head);
        let paths: Vec<&str> = change.files.iter().map(|f| f.path.as_str()).collect();
        assert!(
            paths.contains(&"repo_a/src/a.ts"),
            "changed file must be workspace-relative (config prefix applied); got {paths:?}"
        );
        assert!(
            paths.iter().all(|p| p.starts_with("repo_a/")),
            "every changed path must carry the repo prefix; got {paths:?}"
        );

        let skipped: Vec<&str> = result
            .skipped
            .iter()
            .map(|s| s.repo_name.as_str())
            .collect();
        assert!(skipped.contains(&"repo_b"), "no-change repo is skipped");
        assert!(
            skipped.contains(&"repo_c"),
            "unresolved-ref repo is skipped"
        );
        let unresolved_reason = &result
            .skipped
            .iter()
            .find(|s| s.repo_name == "repo_c")
            .expect("repo_c skipped")
            .reason;
        assert!(
            unresolved_reason.contains("did not resolve"),
            "unresolved ref reason should be recorded; got `{unresolved_reason}`"
        );
    }
}
