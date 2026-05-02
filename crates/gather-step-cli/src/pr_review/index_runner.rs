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
//! worktree was created from a SHA in the user's repo, so the config file is
//! naturally present if the user has it checked in.
//!
//! Phase 1 Task 4 of the PR review mode plan.

use std::path::Path;

use anyhow::{Context, Result, bail};
use gather_step_core::{GatherStepConfig, RegistryStore, WorkspaceStats};
use gather_step_storage::{IndexingOptions, index_workspace_with_storage};

use crate::pr_review::{affected::AffectedRepos, artifact_root::ReviewArtifactRoot};

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
/// review worktree was created from a SHA in the user's repo, so the
/// config file is naturally present if the user has it checked in.
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
             The config file must be committed to the repository so it is present \
             in the detached worktree.",
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
                tracing::info!("affected-repo filter produced an empty set; skipping reindex");
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
                "reindexing only affected repos"
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

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process::Command,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_storage::IndexingOptions;

    use crate::pr_review::artifact_root::create_artifact_root;

    use super::run_review_index;

    // ── temp-dir helper ───────────────────────────────────────────────────────

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(name: &str) -> Self {
            let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!("gather-step-review-index-test-{name}-{id}"));
            fs::create_dir_all(&path).expect("temp dir should exist");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            // Prune any git worktrees registered against this directory before
            // removal — otherwise git leaves dangling refs in the source repo.
            let _ = Command::new("git")
                .args(["-C", &self.path.to_string_lossy(), "worktree", "prune"])
                .output();
            let _ = fs::remove_dir_all(&self.path);
        }
    }

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
}
