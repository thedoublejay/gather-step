//! `pr-review` subcommand — run an isolated review index against a PR branch
//! and emit an MVP delta report.
//!
//! Phase 1 Task 5 of the PR review mode plan.
//!
//! # Deferred to Phase 2
//!
//! - `added_routes`, `added_symbols`, `added_payload_contracts` are all empty
//!   arrays in this MVP.  Phase 2 owns diff extraction against the review index.
//! - `--registry`/`--storage` overrides on `trace`, `impact`, and `pack` are
//!   not yet exposed as top-level CLI flags; the suggested commands include them
//!   for documentation purposes and are flagged with `requires_keep_cache: true`.

use std::{
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{Context, Result};
use clap::{Args, ValueEnum};
use gather_step_core::GatherStepConfig;
use gather_step_git::{
    refs::{ChangedFile, resolve_range},
    worktrees::{create_detached_worktree, remove_worktree},
};
use gather_step_storage::IndexingOptions;

use crate::{
    app::AppContext,
    pr_review::{
        artifact_root::{
            ArtifactRootError, ReviewArtifactRoot, create_artifact_root, default_cache_root,
            generate_run_id, write_marker_completed, write_marker_quarantined, workspace_hash,
        },
        delta_report::{
            CleanupPolicy, DeltaReport, ReviewMetadata, SafetyMetadata,
            build_suggested_followups,
        },
        index_runner::run_review_index,
    },
    storage_context::StorageContext,
};

// ─── Args ─────────────────────────────────────────────────────────────────────

/// Maximum number of changed-file paths included in the report.
const MAX_CHANGED_FILES: usize = 200;

#[derive(Args, Debug, Clone)]
pub struct PrReviewArgs {
    /// Base ref (branch, tag, SHA, or any git rev).
    #[arg(long, value_name = "REF")]
    pub base: String,

    /// Head ref (branch, tag, SHA, "HEAD", …).
    #[arg(long, value_name = "REF")]
    pub head: String,

    /// Engine to use for the review.  Only `temp-index` is supported in this MVP.
    #[arg(long, value_enum, default_value_t = ReviewEngine::TempIndex)]
    pub engine: ReviewEngine,

    /// Keep the review artifact root after the run.  Without this flag,
    /// successful runs delete the artifact root on exit.
    #[arg(long)]
    pub keep_cache: bool,

    /// Emit JSON output instead of Markdown.  Overrides the global `--json`
    /// flag for this command (the global flag also works).
    #[arg(long)]
    pub json: bool,

    /// Override the OS cache root used for review artifacts.
    /// Useful for CI and tests.
    #[arg(long, value_name = "PATH", hide = true)]
    pub cache_root: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum ReviewEngine {
    TempIndex,
}

// ─── Handler ──────────────────────────────────────────────────────────────────

#[expect(
    clippy::needless_pass_by_value,
    reason = "matches dispatch signature: clap passes PrReviewArgs by value"
)]
pub fn run(app: &AppContext, args: PrReviewArgs) -> Result<()> {
    let report = run_inner(app, &args)?;
    // Print to stdout.
    #[expect(
        clippy::print_stdout,
        reason = "pr-review is the sole caller of this path; structured output goes here"
    )]
    {
        println!("{report}");
    }
    Ok(())
}

/// Core implementation — returns the rendered string so tests can assert on it
/// without capturing stdout.
pub fn run_inner(app: &AppContext, args: &PrReviewArgs) -> Result<String> {
    let emit_json = args.json || app.json_output;

    // ── 1. Resolve refs ────────────────────────────────────────────────────
    let resolved = resolve_range(&app.workspace_path, &args.base, &args.head).with_context(
        || format!("resolving refs `{}..{}` in `{}`", args.base, args.head, app.workspace_path.display()),
    )?;

    let base_sha = resolved.base.sha.clone();
    let head_sha = resolved.head.sha.clone();

    // ── 2. Changed files ───────────────────────────────────────────────────
    let changed = gather_step_git::refs::changed_files(&app.workspace_path, &base_sha, &head_sha)
        .with_context(|| {
            format!("listing changed files between `{base_sha}` and `{head_sha}`")
        })?;

    let all_changed_paths: Vec<String> = changed
        .iter()
        .map(|cf: &ChangedFile| cf.path.clone())
        .collect();

    let changed_files_truncated = all_changed_paths.len() > MAX_CHANGED_FILES;
    let changed_files_display: Vec<String> = all_changed_paths
        .iter()
        .take(MAX_CHANGED_FILES)
        .cloned()
        .collect();

    // ── 3. Changed-repo mapping ────────────────────────────────────────────
    let changed_repos =
        map_changed_repos(&app.workspace_path, &all_changed_paths);

    // ── 4. Artifact root ───────────────────────────────────────────────────
    let cache_root = args
        .cache_root
        .clone()
        .unwrap_or_else(|| default_cache_root(&app.workspace_path));

    let run_id = generate_run_id();

    let artifact_root = create_artifact_root(
        &cache_root,
        &app.workspace_path,
        &base_sha,
        &head_sha,
        &run_id,
    )
    .with_context(|| format!("creating artifact root for run `{run_id}`"))?;

    // Safety guard: construct both contexts and verify no path overlap before
    // opening any review storage.
    let workspace_ctx = StorageContext::workspace_read_only(app);
    let _review_ctx = StorageContext::review_checked(
        &workspace_ctx,
        artifact_root.root.clone(),
        artifact_root.registry_path.clone(),
        artifact_root.storage_root.clone(),
        run_id.clone(),
    )
    .map_err(ArtifactRootError::Safety)
    .with_context(|| "review safety guard rejected the proposed artifact paths")?;

    // ── 5. Materialize worktree ────────────────────────────────────────────
    // `create_artifact_root` pre-creates the worktree directory; git worktree
    // add refuses to clobber an existing directory, so remove it first.
    if artifact_root.worktree_root.exists() {
        std::fs::remove_dir(&artifact_root.worktree_root).with_context(|| {
            format!(
                "removing pre-created worktree placeholder at `{}`",
                artifact_root.worktree_root.display()
            )
        })?;
    }

    let worktree = match create_detached_worktree(
        &app.workspace_path,
        &artifact_root.worktree_root,
        &head_sha,
    ) {
        Ok(wt) => wt,
        Err(e) => {
            quarantine_on_error(&artifact_root);
            return Err(e).with_context(|| {
                format!(
                    "creating detached worktree at `{}`",
                    artifact_root.worktree_root.display()
                )
            });
        }
    };

    // ── 6. Index ───────────────────────────────────────────────────────────
    let index_start = Instant::now();
    let stats = match run_review_index(&artifact_root, IndexingOptions::default()) {
        Ok(s) => s,
        Err(e) => {
            quarantine_on_error(&artifact_root);
            return Err(e).with_context(|| "review indexer failed");
        }
    };
    // Truncation is intentional: no real indexing run takes > 584 million years.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "elapsed_ms will never overflow u64 in practice"
    )]
    let elapsed_ms = index_start.elapsed().as_millis() as u64;

    // ── 7. Indexed-repo names ──────────────────────────────────────────────
    let indexed_repos: Vec<String> = {
        let config_path = artifact_root.worktree_root.join("gather-step.config.yaml");
        if let Ok(config) = GatherStepConfig::from_yaml_file(&config_path) {
            config.repos.iter().map(|r| r.name.clone()).collect()
        } else {
            // Fallback: use the WorkspaceStats count if available.
            (0..stats.total_repos)
                .map(|i| format!("repo-{i}"))
                .collect()
        }
    };

    // ── 8. Build report ────────────────────────────────────────────────────
    let ws_paths = app.workspace_paths();
    let ws_hash = workspace_hash(&app.workspace_path);
    let cache_key = format!("{ws_hash}:{base_sha}:{head_sha}");

    let cleanup_policy = if args.keep_cache {
        CleanupPolicy::KeepCache
    } else {
        CleanupPolicy::RemoveOnExit
    };

    let suggested_followups = build_suggested_followups(
        &app.workspace_path,
        &artifact_root.registry_path,
        &artifact_root.storage_root,
    );

    let report = DeltaReport {
        schema_version: 1,
        metadata: ReviewMetadata {
            workspace: app.workspace_path.clone(),
            base_input: args.base.clone(),
            base_sha: base_sha.clone(),
            head_input: args.head.clone(),
            head_sha: head_sha.clone(),
            checkout_mode: "head".to_owned(),
            changed_repos,
            indexed_repos,
            elapsed_ms,
        },
        safety: SafetyMetadata {
            baseline_registry_path: ws_paths.registry_path.clone(),
            baseline_storage_path: ws_paths.storage_root.clone(),
            review_registry_path: artifact_root.registry_path.clone(),
            review_storage_path: artifact_root.storage_root.clone(),
            review_root: artifact_root.root.clone(),
            run_id: run_id.clone(),
            cleanup_policy,
            cache_key,
        },
        changed_files: changed_files_display,
        changed_files_truncated,
        added_routes: vec![],
        added_symbols: vec![],
        added_payload_contracts: vec![],
        suggested_followups,
    };

    // ── 9. Update marker ───────────────────────────────────────────────────
    // Mark completed before cleanup so the marker is correct even if cleanup
    // fails.
    let _ = write_marker_completed(&artifact_root);

    // ── 10. Cleanup ────────────────────────────────────────────────────────
    if !args.keep_cache {
        // Best-effort: remove the worktree then the artifact root.  Errors are
        // logged but do not fail the command — the marker is already Completed.
        let _ = remove_worktree(&worktree);
        let _ = std::fs::remove_dir_all(&artifact_root.root);
    }

    // ── 11. Render ─────────────────────────────────────────────────────────
    if emit_json {
        report.render_json().context("serializing delta report to JSON")
    } else {
        Ok(report.render_markdown())
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Map changed file paths to the configured repo names that own them.
///
/// Uses longest-prefix matching against `config.repos[*].path`.  Files that
/// do not match any configured repo are grouped under the synthetic
/// `"<workspace>"` entry.
fn map_changed_repos(workspace_root: &Path, changed_paths: &[String]) -> Vec<String> {
    // Try to load the config; if unavailable, everything maps to <workspace>.
    let config_path = workspace_root.join("gather-step.config.yaml");
    let repos: Vec<(String, String)> = if let Ok(cfg) =
        GatherStepConfig::from_yaml_file(&config_path)
    {
        cfg.repos
            .into_iter()
            .map(|r| (r.name, r.path))
            .collect()
    } else {
        vec![]
    };

    let mut result_set = std::collections::BTreeSet::new();

    for file_path in changed_paths {
        let matched = repos.iter().find(|(_, repo_path)| {
            // Match if the file path starts with the repo path prefix
            // (with a directory separator boundary).
            let prefix = repo_path.trim_end_matches('/');
            file_path == prefix
                || file_path.starts_with(&format!("{prefix}/"))
        });

        match matched {
            Some((name, _)) => {
                result_set.insert(name.clone());
            }
            None => {
                result_set.insert("<workspace>".to_owned());
            }
        }
    }

    // If nothing changed at all, return empty (not "<workspace>").
    result_set.into_iter().collect()
}

/// Mark the artifact root as Quarantined on error, ignoring any secondary
/// failure to write the marker.
fn quarantine_on_error(artifact_root: &ReviewArtifactRoot) {
    let _ = write_marker_quarantined(artifact_root);
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

    use super::*;
    use crate::app::AppContext;

    // ── temp-dir helper ───────────────────────────────────────────────────────

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(label: &str) -> Self {
            let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path =
                env::temp_dir().join(format!("gs-pr-review-test-{label}-{id}"));
            fs::create_dir_all(&path).expect("temp dir");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
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

    fn git_run(dir: &Path, args: &[&str]) {
        let status = Command::new("git")
            .args(args)
            .current_dir(dir)
            .status()
            .expect("git should run");
        assert!(status.success(), "git {} failed", args.join(" "));
    }

    fn git_head_sha(dir: &Path) -> String {
        let out = Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(dir)
            .output()
            .expect("rev-parse HEAD");
        assert!(out.status.success());
        String::from_utf8_lossy(&out.stdout).trim().to_owned()
    }

    // ── fixture builder ───────────────────────────────────────────────────────

    /// Create a workspace with git history:
    ///
    /// - base commit: config + myrepo/src/hello.ts
    /// - head commit (new branch): adds myrepo/src/added.ts
    ///
    /// Returns `(workspace_path, base_sha, head_sha)`.
    fn build_fixture(root: &Path) -> (PathBuf, String, String) {
        let ws = root.to_path_buf();

        // Config
        fs::write(
            ws.join("gather-step.config.yaml"),
            "repos:\n  - name: myrepo\n    path: myrepo\nindexing:\n  workspace_concurrency: 1\n",
        )
        .unwrap();

        // Initial repo content
        let src = ws.join("myrepo/src");
        fs::create_dir_all(&src).unwrap();
        fs::write(ws.join("myrepo/package.json"), r#"{"name":"myrepo","version":"0.0.1"}"#).unwrap();
        fs::write(
            src.join("hello.ts"),
            "export function greet(): string { return 'hello'; }\n",
        )
        .unwrap();

        // Init git and make base commit
        git_run(&ws, &["init", "--initial-branch=main"]);
        git_run(&ws, &["config", "user.email", "test@example.com"]);
        git_run(&ws, &["config", "user.name", "Test"]);
        git_run(&ws, &["add", "."]);
        git_run(&ws, &["commit", "--message", "base"]);
        let base_sha = git_head_sha(&ws);

        // Branch off and add a file
        git_run(&ws, &["checkout", "-b", "feature/add-file"]);
        fs::write(
            src.join("added.ts"),
            "export function added(): string { return 'added'; }\n",
        )
        .unwrap();
        git_run(&ws, &["add", "."]);
        git_run(&ws, &["commit", "--message", "head: add added.ts"]);
        let head_sha = git_head_sha(&ws);

        // Go back to main so we can run worktree-based review
        git_run(&ws, &["checkout", "main"]);

        (ws, base_sha, head_sha)
    }

    fn make_app(workspace: &Path) -> AppContext {
        AppContext {
            workspace_path: workspace.to_path_buf(),
            repo_filter: None,
            json_output: false,
            no_interactive: true,
            stdin_is_tty: false,
            stdout_is_tty: false,
            stderr_is_tty: false,
            ci_env_set: true,
            color_mode: crate::app::ColorModeArg::Never,
            show_banner: false,
            multi_progress: indicatif::MultiProgress::new(),
        }
    }

    // ── Test 1: metadata fields ───────────────────────────────────────────────

    #[test]
    fn pr_review_emits_metadata_for_simple_pr() {
        if !git_available() {
            return;
        }

        let ws_tmp = TempDir::new("ws1");
        let cache_tmp = TempDir::new("cache1");
        let (ws, base_sha, head_sha) = build_fixture(ws_tmp.path());

        let app = make_app(&ws);
        let args = PrReviewArgs {
            base: base_sha.clone(),
            head: head_sha.clone(),
            engine: ReviewEngine::TempIndex,
            keep_cache: true,
            json: true,
            cache_root: Some(cache_tmp.path().to_path_buf()),
        };

        let rendered = run_inner(&app, &args).expect("run_inner should succeed");

        let report: serde_json::Value = serde_json::from_str(&rendered).expect("JSON must parse");

        // base/head SHAs are 40-char hex
        let meta = &report["metadata"];
        assert_eq!(meta["base_sha"].as_str().unwrap().len(), 40);
        assert_eq!(meta["head_sha"].as_str().unwrap().len(), 40);
        assert_eq!(meta["base_sha"].as_str().unwrap(), base_sha);
        assert_eq!(meta["head_sha"].as_str().unwrap(), head_sha);

        // changed_files includes the added file
        let files = report["changed_files"].as_array().unwrap();
        assert!(
            files
                .iter()
                .any(|f| f.as_str().unwrap().contains("added.ts")),
            "expected added.ts in changed_files; got {files:?}"
        );

        // baseline_storage_path != review_storage_path
        let safety = &report["safety"];
        assert_ne!(
            safety["baseline_storage_path"].as_str().unwrap(),
            safety["review_storage_path"].as_str().unwrap(),
        );

        // review_root is under the cache_tmp dir
        let review_root = PathBuf::from(safety["review_root"].as_str().unwrap());
        assert!(review_root.starts_with(cache_tmp.path()));

        // at least 3 suggested followups
        let followups = report["suggested_followups"].as_array().unwrap();
        assert!(followups.len() >= 3, "expected >= 3 followups");
    }

    // ── Test 2: keep_cache leaves artifact root ───────────────────────────────

    #[test]
    fn pr_review_keeps_cache_when_flag_set() {
        if !git_available() {
            return;
        }

        let ws_tmp = TempDir::new("ws2");
        let cache_tmp = TempDir::new("cache2");
        let (ws, base_sha, head_sha) = build_fixture(ws_tmp.path());

        let app = make_app(&ws);
        let args = PrReviewArgs {
            base: base_sha,
            head: head_sha,
            engine: ReviewEngine::TempIndex,
            keep_cache: true,
            json: true,
            cache_root: Some(cache_tmp.path().to_path_buf()),
        };

        let rendered = run_inner(&app, &args).expect("run_inner should succeed");

        let report: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        let review_root = PathBuf::from(report["safety"]["review_root"].as_str().unwrap());

        assert!(
            review_root.exists(),
            "artifact root should still exist after --keep-cache run; got {review_root:?}"
        );
    }

    // ── Test 3: cleanup removes artifact root ─────────────────────────────────

    #[test]
    fn pr_review_cleans_up_when_flag_unset() {
        if !git_available() {
            return;
        }

        let ws_tmp = TempDir::new("ws3");
        let cache_tmp = TempDir::new("cache3");
        let (ws, base_sha, head_sha) = build_fixture(ws_tmp.path());

        let app = make_app(&ws);
        let args = PrReviewArgs {
            base: base_sha,
            head: head_sha,
            engine: ReviewEngine::TempIndex,
            keep_cache: false,
            json: true,
            cache_root: Some(cache_tmp.path().to_path_buf()),
        };

        let rendered = run_inner(&app, &args).expect("run_inner should succeed");

        let report: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        let review_root = PathBuf::from(report["safety"]["review_root"].as_str().unwrap());

        assert!(
            !review_root.exists(),
            "artifact root should be removed after run without --keep-cache; path={review_root:?}"
        );
    }

    // ── Test 4: baseline storage is not touched ───────────────────────────────

    #[test]
    fn pr_review_does_not_touch_baseline_storage() {
        if !git_available() {
            return;
        }

        let ws_tmp = TempDir::new("ws4");
        let cache_tmp = TempDir::new("cache4");
        let (ws, base_sha, head_sha) = build_fixture(ws_tmp.path());

        let baseline_gather_step = ws.join(".gather-step");

        // Capture before state.
        let existed_before = baseline_gather_step.exists();

        let app = make_app(&ws);
        let args = PrReviewArgs {
            base: base_sha,
            head: head_sha,
            engine: ReviewEngine::TempIndex,
            keep_cache: false,
            json: true,
            cache_root: Some(cache_tmp.path().to_path_buf()),
        };

        run_inner(&app, &args).expect("run_inner should succeed");

        // After state: .gather-step should have same existence as before.
        let existed_after = baseline_gather_step.exists();
        assert_eq!(
            existed_before, existed_after,
            ".gather-step baseline state should not change; \
             was {existed_before} before, {existed_after} after"
        );
    }
}
