//! Review-mode indexing runner.
//!
//! Indexes the worktree materialized by Phase 1 Task 2 into the isolated
//! storage and registry owned by the [`ReviewArtifactRoot`], reusing the
//! existing [`index_workspace_with_storage`] path verbatim.
//!
//! # Pre-condition
//!
//! A copy of `gather-step.config.yaml` must exist at
//! `artifact_root.worktree_root.join("gather-step.config.yaml")`.  The review
//! worktree may provide that file from the checked-out ref, or the caller may
//! copy a workspace-level config into the temporary worktree before indexing.
//!
//! Phase 1 Task 4 of the PR review mode plan.

use std::path::Path;

use anyhow::{Context, Error, Result, bail};
use gather_step_core::{GatherStepConfig, RegistryStore, WorkspaceStats};
use gather_step_git::worktrees::{ReviewWorktree, create_detached_worktree, remove_worktree};
use gather_step_storage::{IndexingOptions, index_workspace_with_storage};

use crate::pr_review::{
    affected::AffectedRepos, artifact_root::ReviewArtifactRoot, changed::PerRepoChanges,
    target::ReviewTarget,
};

/// Config filename the runner looks for inside the worktree.
const CONFIG_FILENAME: &str = "gather-step.config.yaml";

/// Index the worktree at `artifact_root.worktree_root` into the review
/// artifact root's isolated storage and registry.
///
/// This is a thin wrapper around [`index_workspace_with_storage`].  It does
/// **not** touch the user's normal `.gather-step/` directory; all output goes
/// to `artifact_root.storage_root` and `artifact_root.registry_path`.
///
/// # `affected` parameter
///
/// When `affected` is `Some` and `affected.all_repos == false`, only the repos
/// listed in `affected.repos` are passed to the underlying indexer.  This lets
/// the caller pre-seed the storage from the baseline and only reindex the repos
/// that actually changed.
///
/// When `affected` is `None` or `affected.all_repos == true`, all repos in the
/// config are indexed (the safe default).
///
/// # Pre-condition
///
/// A copy of `gather-step.config.yaml` must exist at
/// `artifact_root.worktree_root.join("gather-step.config.yaml")`. The
/// review worktree may provide that file from the checked-out ref, or the
/// caller may copy a workspace-level config into the temporary worktree.
///
/// # Errors
///
/// Returns an error if:
/// - `gather-step.config.yaml` is absent from the worktree root.
/// - Config loading fails.
/// - Registry open/save fails.
/// - The underlying indexer fails.
pub fn run_review_index(
    artifact_root: &ReviewArtifactRoot,
    affected: Option<&AffectedRepos>,
    options: IndexingOptions,
) -> Result<WorkspaceStats> {
    let config_path = artifact_root.worktree_root.join(CONFIG_FILENAME);
    if !config_path.exists() {
        bail!(
            "review index pre-condition violated: `{}` not found in worktree at `{}`.\n\
             Pass --config with an existing workspace config when the reviewed \
             git repository does not commit its own config file.",
            CONFIG_FILENAME,
            artifact_root.worktree_root.display(),
        );
    }

    let full_config = GatherStepConfig::from_yaml_file(&config_path)
        .with_context(|| format!("loading config from `{}`", config_path.display()))?;

    let config_root: &Path = &artifact_root.worktree_root;

    // Determine which repos to index.  When `affected` is a strict subset,
    // build a temporary config containing only those repos so the indexer
    // skips the unchanged ones.  Fall back to the full config whenever
    // `affected` is None or all_repos is true.
    let effective_config: GatherStepConfig;
    let config = match affected {
        Some(a) if !a.all_repos && !a.repos.is_empty() => {
            let subset: Vec<_> = full_config
                .repos
                .iter()
                .filter(|r| a.repos.contains(&r.name))
                .cloned()
                .collect();

            if subset.is_empty() {
                // No repos match — nothing to reindex; short-circuit.
                tracing::info!("The affected-repo filter produced an empty set; skipping reindex.");
                return Ok(gather_step_core::WorkspaceStats {
                    total_repos: 0,
                    indexed_repos: 0,
                    total_files: 0,
                    total_symbols: 0,
                    total_edges: 0,
                    cross_repo_edges: 0,
                });
            }

            tracing::info!(
                repos = ?subset.iter().map(|r| &r.name).collect::<Vec<_>>(),
                "Reindexing only affected repositories."
            );

            effective_config = GatherStepConfig {
                repos: subset,
                allow_listed_repos: full_config.allow_listed_repos.clone(),
                github: full_config.github.clone(),
                jira: full_config.jira.clone(),
                indexing: full_config.indexing.clone(),
                deployment: full_config.deployment.clone(),
            };
            &effective_config
        }
        _ => &full_config,
    };
    config
        .validate_repo_roots_against_config_root(config_root)
        .with_context(|| {
            format!(
                "validating review config repo roots from `{}`",
                config_path.display()
            )
        })?;

    // Ensure the review storage directory exists (artifact_root creation
    // already does this, but guard here too so the function is self-contained).
    std::fs::create_dir_all(&artifact_root.storage_root).with_context(|| {
        format!(
            "creating review storage directory `{}`",
            artifact_root.storage_root.display()
        )
    })?;

    let mut registry = RegistryStore::open(&artifact_root.registry_path).with_context(|| {
        format!(
            "opening review registry at `{}`",
            artifact_root.registry_path.display()
        )
    })?;

    // register_from_config saves the registry immediately; subsequent
    // update_repo_metadata calls inside index_workspace_with_storage also save
    // after each mutation, so no explicit save is required after the call.
    registry
        .register_from_config(config, config_root)
        .with_context(|| {
            format!(
                "registering repos from config into review registry at `{}`",
                artifact_root.registry_path.display()
            )
        })?;

    let stats = index_workspace_with_storage(
        config,
        config_root,
        &mut registry,
        &artifact_root.storage_root,
        options,
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    Ok(stats)
}

/// Materialize a polyrepo review worktree.
///
/// Each changed repo is checked out at its resolved head SHA into
/// `worktree_root/<repo_path>`, then `config_bytes` is written as the workspace
/// config so the result is indistinguishable from a single-repo worktree to
/// [`run_review_index`]. Only repos present in `changes.repos` are checked out;
/// untouched and unresolved repos are excluded from the review.
///
/// Returns the created worktree handles so the caller can remove them on
/// cleanup.
///
/// # Errors
///
/// Returns an error if a changed repo is absent from `target`, resolves to the
/// workspace root (polyrepo repos must have a non-empty path), the checkout
/// fails, or the config cannot be written.
pub fn materialize_polyrepo_worktree(
    worktree_root: &Path,
    target: &ReviewTarget,
    changes: &PerRepoChanges,
    config_bytes: &[u8],
) -> Result<Vec<ReviewWorktree>> {
    let mut worktrees = Vec::with_capacity(changes.repos.len());
    for change in &changes.repos {
        let spec = target
            .repos
            .iter()
            .find(|repo| repo.repo_name == change.repo_name)
            .with_context(|| {
                format!(
                    "changed repo `{}` is not in the review target",
                    change.repo_name
                )
            })?;
        if spec.repo_path.is_empty() {
            bail!(
                "polyrepo worktree materialization requires a non-empty repo path; \
                 repo `{}` resolved to the workspace root",
                spec.repo_name
            );
        }
        let dest = worktree_root.join(&spec.repo_path);
        if let Some(parent) = dest.parent()
            && let Err(error) = std::fs::create_dir_all(parent)
        {
            let primary = Error::new(error)
                .context(format!("creating worktree parent `{}`", parent.display()));
            return Err(error_with_cleanup_context(&mut worktrees, primary));
        }
        let worktree = match create_detached_worktree(&spec.git_repo_root, &dest, &change.head_sha)
        {
            Ok(worktree) => worktree,
            Err(error) => {
                let primary = Error::new(error).context(format!(
                    "checking out `{}`@`{}` into `{}`",
                    spec.repo_name,
                    change.head_sha,
                    dest.display()
                ));
                return Err(error_with_cleanup_context(&mut worktrees, primary));
            }
        };
        worktrees.push(worktree);
    }

    let config_path = worktree_root.join(CONFIG_FILENAME);
    if let Err(error) = std::fs::write(&config_path, config_bytes) {
        let primary = Error::new(error).context(format!(
            "writing review config to `{}`",
            config_path.display()
        ));
        return Err(error_with_cleanup_context(&mut worktrees, primary));
    }

    Ok(worktrees)
}

fn error_with_cleanup_context(worktrees: &mut Vec<ReviewWorktree>, primary: Error) -> Error {
    match cleanup_created_worktrees(worktrees) {
        Ok(()) => primary,
        Err(cleanup_error) => primary.context(format!(
            "rollback cleanup failed after polyrepo worktree materialization error: {cleanup_error}"
        )),
    }
}

fn cleanup_created_worktrees(worktrees: &mut Vec<ReviewWorktree>) -> Result<()> {
    let mut failures = Vec::new();
    for worktree in worktrees.drain(..) {
        if let Err(error) = remove_worktree(&worktree) {
            tracing::warn!(
                error = %error,
                repo = %worktree.repo.display(),
                worktree = %worktree.root.display(),
                "polyrepo worktree materialization failed and cleanup could not remove a previously-created child worktree.",
            );
            failures.push(format!(
                "`{}` from source repo `{}`: {error}",
                worktree.root.display(),
                worktree.repo.display()
            ));
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        bail!(
            "failed to remove {} previously-created polyrepo review worktree(s): {}",
            failures.len(),
            failures.join("; ")
        );
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::{fs, path::Path, process::Command};

    use gather_step_git::worktrees::ReviewWorktree;
    use gather_step_storage::IndexingOptions;

    use crate::pr_review::{
        artifact_root::create_artifact_root,
        changed::resolve_per_repo_changes,
        target::{ReviewRepoSpec, ReviewTarget},
        test_helpers::TempDir,
    };

    use super::{cleanup_created_worktrees, materialize_polyrepo_worktree, run_review_index};

    // ── git helpers ───────────────────────────────────────────────────────────

    fn git_available() -> bool {
        Command::new("git").arg("--version").output().is_ok()
    }

    /// Init a git repo at `dir`, set minimal identity, commit all staged files,
    /// and return the HEAD SHA.
    fn git_init_and_commit(dir: &Path) -> String {
        let run = |args: &[&str]| {
            let status = Command::new("git")
                .args(args)
                .current_dir(dir)
                .status()
                .expect("git command should run");
            assert!(
                status.success(),
                "git {} failed with status {status}",
                args.join(" ")
            );
        };

        run(&["init", "--initial-branch=main"]);
        run(&["config", "user.email", "test@example.com"]);
        run(&["config", "user.name", "Test"]);
        run(&["config", "commit.gpgsign", "false"]);
        run(&["config", "tag.gpgsign", "false"]);
        run(&["add", "."]);
        run(&["commit", "--message", "initial"]);

        let out = Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(dir)
            .output()
            .expect("rev-parse should run");
        assert!(out.status.success());
        String::from_utf8_lossy(&out.stdout).trim().to_owned()
    }

    // ── fixture builders ──────────────────────────────────────────────────────

    /// Write a minimal workspace fixture into `root`:
    /// ```text
    /// root/
    ///   gather-step.config.yaml   ← single repo "myrepo" at path "myrepo/"
    ///   myrepo/
    ///     package.json
    ///     src/
    ///       hello.ts              ← exports function greetReviewMode
    /// ```
    fn write_minimal_fixture(root: &Path) {
        // Config
        fs::write(
            root.join("gather-step.config.yaml"),
            "repos:\n  - name: myrepo\n    path: myrepo\nindexing:\n  workspace_concurrency: 1\n",
        )
        .expect("config should write");

        // Repo skeleton
        let src = root.join("myrepo/src");
        fs::create_dir_all(&src).expect("src dir should exist");

        fs::write(
            root.join("myrepo/package.json"),
            r#"{"name":"myrepo","version":"0.0.1"}"#,
        )
        .expect("package.json should write");

        fs::write(
            src.join("hello.ts"),
            "export function greetReviewMode(): string {\n  return 'hello from review';\n}\n",
        )
        .expect("hello.ts should write");
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /// End-to-end test: build a real worktree at a committed SHA, run the
    /// review indexer, and assert:
    ///
    /// 1. The call succeeds and reports at least one indexed repo.
    /// 2. The review registry file exists and is non-empty.
    /// 3. The review storage directory exists and contains `graph.redb`.
    /// 4. The **source workspace** has no `.gather-step/` directory — the
    ///    indexer did not pollute the user's workspace.
    #[test]
    fn run_review_index_indexes_worktree_into_review_storage() {
        if !git_available() {
            // git not on PATH — skip silently.
            return;
        }

        // ── 1. Build source workspace + git repo ─────────────────────────────
        let source_ws = TempDir::new("source");
        write_minimal_fixture(source_ws.path());
        let sha = git_init_and_commit(source_ws.path());

        // ── 2. Create the artifact root ──────────────────────────────────────
        let cache_root = TempDir::new("cache");
        let artifact_root = create_artifact_root(
            cache_root.path(),
            source_ws.path(),
            &sha, // base_sha
            &sha, // head_sha (same for this minimal test)
            "test-run-001",
        )
        .expect("artifact root should create");

        // ── 3. Materialise the worktree ──────────────────────────────────────
        // The artifact root pre-created `worktree_root` as an empty directory;
        // git worktree add refuses to clobber an existing directory, so we
        // remove it first and let git recreate it.
        fs::remove_dir(&artifact_root.worktree_root)
            .expect("pre-created worktree dir should be removable");

        gather_step_git::worktrees::create_detached_worktree(
            source_ws.path(),
            &artifact_root.worktree_root,
            &sha,
        )
        .expect("worktree should create");

        // ── 4. Run the review indexer ────────────────────────────────────────
        let stats = run_review_index(&artifact_root, None, IndexingOptions::default())
            .expect("review index should succeed");

        // ── 5. Assertions ────────────────────────────────────────────────────
        assert!(
            stats.total_repos >= 1,
            "expected at least 1 repo; got {stats:?}"
        );

        // Registry file written.
        assert!(
            artifact_root.registry_path.exists(),
            "review registry should exist at `{}`",
            artifact_root.registry_path.display()
        );
        let registry_len = fs::metadata(&artifact_root.registry_path)
            .expect("registry metadata")
            .len();
        assert!(registry_len > 0, "review registry should be non-empty");

        // Storage directory exists and contains graph.redb.
        assert!(
            artifact_root.storage_root.exists(),
            "review storage dir should exist"
        );
        let graph_path = artifact_root.storage_root.join("graph.redb");
        assert!(
            graph_path.exists(),
            "graph.redb should exist at `{}`",
            graph_path.display()
        );

        // Source workspace not polluted.
        let source_gather = source_ws.path().join(".gather-step");
        assert!(
            !source_gather.exists(),
            "source workspace `.gather-step/` must NOT exist; review indexer polluted the workspace"
        );
    }

    #[cfg(unix)]
    #[test]
    fn run_review_index_rejects_symlinked_repo_root_in_review_config() {
        let source_ws = TempDir::new("source-symlink");
        let cache_root = TempDir::new("cache-symlink");
        let artifact_root = create_artifact_root(
            cache_root.path(),
            source_ws.path(),
            "base",
            "head",
            "run-symlink",
        )
        .expect("artifact root should create");

        let outside = TempDir::new("outside-repo");
        fs::create_dir_all(outside.path().join("src")).expect("outside src");
        std::os::unix::fs::symlink(outside.path(), artifact_root.worktree_root.join("repo_a"))
            .expect("symlink repo root");
        fs::write(
            artifact_root.worktree_root.join("gather-step.config.yaml"),
            "repos:\n  - name: repo_a\n    path: repo_a\nindexing:\n  workspace_concurrency: 1\n",
        )
        .expect("config should write");

        let error = run_review_index(&artifact_root, None, IndexingOptions::default())
            .expect_err("review config validation should reject symlinked repo roots");
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("validating review config repo roots"),
            "error should identify the review-config validation phase: {rendered}"
        );
        assert!(
            rendered.contains("symlinked repo root"),
            "error should preserve the config validation reason: {rendered}"
        );
    }

    #[test]
    fn cleanup_created_worktrees_errors_when_a_child_worktree_cannot_be_removed() {
        let temp = TempDir::new("cleanup-failure");
        let fake_repo = temp.path().join("not-a-git-repo");
        fs::create_dir_all(&fake_repo).expect("fake repo dir");
        let mut worktrees = vec![ReviewWorktree {
            repo: fake_repo,
            root: temp.path().join("repo_a"),
            sha: "0123456789abcdef0123456789abcdef01234567".to_owned(),
        }];

        let error = cleanup_created_worktrees(&mut worktrees)
            .expect_err("cleanup must surface child worktree removal failures");

        assert!(
            error
                .to_string()
                .contains("previously-created polyrepo review worktree"),
            "cleanup error should describe leaked child worktrees: {error}"
        );
        assert!(
            worktrees.is_empty(),
            "cleanup should drain attempted worktrees even when removal fails"
        );
    }

    /// Build an independent git repo at `dir` with a `main` base commit and a
    /// `feature` branch that adds a TypeScript file. Returns nothing; refs are
    /// the fixed names `main` / `feature`.
    fn build_repo_with_feature(dir: &Path) {
        let run = |args: &[&str]| {
            let status = Command::new("git")
                .args(args)
                .current_dir(dir)
                .status()
                .expect("git command should run");
            assert!(status.success(), "git {} failed", args.join(" "));
        };
        fs::create_dir_all(dir.join("src")).expect("src dir");
        fs::write(
            dir.join("package.json"),
            r#"{"name":"r","version":"0.0.1"}"#,
        )
        .expect("package.json");
        run(&["init", "-q", "--initial-branch=main"]);
        run(&["config", "user.email", "test@example.com"]);
        run(&["config", "user.name", "Test"]);
        run(&["config", "commit.gpgsign", "false"]);
        run(&["add", "."]);
        run(&["commit", "-q", "-m", "base"]);
        run(&["checkout", "-q", "-b", "feature"]);
        fs::write(dir.join("src/added.ts"), "export const added = 1;\n").expect("added.ts");
        run(&["add", "."]);
        run(&["commit", "-q", "-m", "feature"]);
    }

    /// End-to-end polyrepo test: two independent git repos under a container,
    /// each diffed at the same `main`..`feature` refs, materialized into one
    /// synthesized worktree, then indexed as a single multi-repo workspace.
    #[test]
    fn materialize_polyrepo_worktree_indexes_all_changed_repos() {
        if !git_available() {
            return;
        }

        let ws = TempDir::new("polyrepo-src");
        let repo_a = ws.path().join("repo_a");
        let repo_b = ws.path().join("repo_b");
        build_repo_with_feature(&repo_a);
        build_repo_with_feature(&repo_b);

        let target = ReviewTarget {
            index_workspace_root: ws.path().to_path_buf(),
            repos: vec![
                ReviewRepoSpec {
                    repo_name: "repo_a".to_owned(),
                    repo_path: "repo_a".to_owned(),
                    git_repo_root: repo_a.clone(),
                },
                ReviewRepoSpec {
                    repo_name: "repo_b".to_owned(),
                    repo_path: "repo_b".to_owned(),
                    git_repo_root: repo_b.clone(),
                },
            ],
        };

        let changes = resolve_per_repo_changes(&target, "main", "feature");
        assert_eq!(
            changes.repos.len(),
            2,
            "both repos changed; got {changes:?}"
        );

        let cache_root = TempDir::new("polyrepo-cache");
        let artifact_root = create_artifact_root(
            cache_root.path(),
            ws.path(),
            "polybase",
            "polyhead",
            "run-poly-001",
        )
        .expect("artifact root should create");

        let config = "repos:\n  - name: repo_a\n    path: repo_a\n  \
                      - name: repo_b\n    path: repo_b\nindexing:\n  \
                      workspace_concurrency: 1\n";
        let worktrees = materialize_polyrepo_worktree(
            &artifact_root.worktree_root,
            &target,
            &changes,
            config.as_bytes(),
        )
        .expect("polyrepo worktree should materialize");

        assert_eq!(worktrees.len(), 2, "one worktree per changed repo");
        assert!(
            artifact_root
                .worktree_root
                .join("repo_a/package.json")
                .exists(),
            "repo_a must be checked out into the worktree"
        );
        assert!(
            artifact_root
                .worktree_root
                .join("repo_b/src/added.ts")
                .exists(),
            "repo_b feature change must be present in the worktree"
        );

        let stats = run_review_index(&artifact_root, None, IndexingOptions::default())
            .expect("review index should succeed on the multi-repo worktree");
        assert_eq!(
            stats.total_repos, 2,
            "both changed repos must be indexed; got {stats:?}"
        );

        // Source repos not polluted by the review indexer.
        assert!(!repo_a.join(".gather-step").exists());
        assert!(!repo_b.join(".gather-step").exists());
    }
}
