//! `pr-review` subcommand — run an isolated review index against a PR branch
//! and emit a structured delta report.
//!
//! Phase 1 Task 5 introduced the MVP skeleton.
//! Phase 2 Tasks 1+2 formalise the schema and add route delta extraction.
//!
//! # Sub-commands
//!
//! - `pr-review` (no subcommand): run a review.  Requires `--base` and `--head`.
//! - `pr-review clean ...`: clean up stale review artifacts (Phase 1 Task 6).

use std::{
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand, ValueEnum};
use gather_step_core::GatherStepConfig;
use gather_step_git::{
    refs::{ChangedFile, changed_files, merge_base, resolve_range},
    worktrees::{ReviewWorktree, create_detached_worktree, remove_worktree},
};
use gather_step_storage::IndexingOptions;
use serde::Serialize;

use crate::{
    app::AppContext,
    pr_review::{
        affected::{AffectedRepos, compute_affected_repos},
        artifact_root::{
            ArtifactRootError, MARKER_FILENAME, ReviewArtifactRoot, ReviewStatus,
            default_cache_root, generate_run_id, materialize_artifact_root, plan_artifact_root,
            read_marker, touch_marker_accessed, workspace_hash, write_marker_completed,
            write_marker_quarantined,
        },
        cache::{
            compute_cache_key, is_cache_key_active, pick_seed_source, seed_artifact_root,
            try_reuse_cache,
        },
        delta_report::{
            CleanupPolicy, ContractAlignments, DELTA_REPORT_SCHEMA_VERSION, DecoratorDeltas,
            DeltaReport, DeploymentDeltas, EventDeltas, GITHUB_COMMENT_LIMIT,
            PayloadContractDeltas, ReviewMetadata, RiskSeverity, RouteDeltas, SafetyMetadata,
            SymbolDeltas, build_suggested_followups, synthesize_review_pack_commands,
        },
        engine::{ReviewEngineImpl, TempIndexEngine, UnsupportedSurface},
        extract::{
            contract_alignment::extract_contract_alignments,
            decorators::extract_decorator_deltas,
            deployment::extract_deployment_deltas,
            events::extract_event_deltas,
            impact_attach::impact_for_node,
            payload_contracts::{extract_payload_contract_deltas, find_payload_contract_node_id},
            removed_surfaces::extract_removed_surface_risks,
            routes::{extract_route_deltas, find_route_node_id},
            symbols::{extract_symbol_deltas, find_symbol_node_id},
        },
    },
    storage_context::StorageContext,
};

// ─── Args ─────────────────────────────────────────────────────────────────────

/// Maximum number of changed-file paths included in the report.
const MAX_CHANGED_FILES: usize = 200;

#[cfg(test)]
static TEMP_INDEX_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[derive(Args, Debug, Clone)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "pr-review has several independent opt-in flag fields; a state machine would obscure intent"
)]
pub struct PrReviewArgs {
    #[command(subcommand)]
    pub command: Option<PrReviewSubcommand>,

    /// Base ref (branch, tag, SHA, or any git rev).
    /// Required when no subcommand is given (i.e., when running a review).
    #[arg(long, value_name = "REF")]
    pub base: Option<String>,

    /// Head ref (branch, tag, SHA, "HEAD", …).
    /// Required when no subcommand is given (i.e., when running a review).
    #[arg(long, value_name = "REF")]
    pub head: Option<String>,

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

    /// Path to a `gather-step.config.yaml` to use for the review run.
    ///
    /// The review temp-index requires a config at the worktree root. By
    /// default the worktree is checked out at `--head`, so a config that
    /// is committed in that ref is naturally present. Pass this flag when
    /// the workspace does not have a checked-in config (e.g. during
    /// bootstrap), or to override the committed one for a single run.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Exit with code 2 if any removed-surface risk has severity `High`.
    /// Without this flag the report is always emitted with exit code 0.
    ///
    /// Deprecated: use `--severity strict` instead.  This flag will be
    /// removed in a future release.
    #[arg(long)]
    pub strict: bool,

    /// Severity mode controlling when the command exits with code 2.
    /// `warn` (default) always exits 0.  `strict` exits 2 on High risks or
    /// incompatible payload type changes.  `pedantic` exits 2 on any
    /// Medium+ risk, any payload change, or removed permission decorators.
    #[arg(long, value_enum, default_value_t = SeverityMode::Warn)]
    pub severity: SeverityMode,

    /// Output format.
    /// `markdown` (default) emits a human-readable Markdown report.
    /// `json` emits compact machine-readable JSON (equivalent to `--json`).
    /// `github-comment` emits Markdown truncated to GitHub's 65 536-char comment limit.
    /// `braingent` emits Markdown with YAML frontmatter for Braingent archival.
    #[arg(long, value_enum, default_value_t = OutputFormat::Markdown)]
    pub format: OutputFormat,

    /// Write the GitHub-comment-formatted output to this file in addition to (or
    /// instead of) stdout.  Only meaningful with `--format github-comment`, but
    /// accepted with any format for scripting convenience.
    #[arg(long, value_name = "PATH")]
    pub github_comment_file: Option<PathBuf>,

    /// Skip the check that verifies the workspace HEAD matches `--base`.
    ///
    /// By default, `pr-review` warns when the workspace's current HEAD differs
    /// from the resolved base SHA, because the baseline index may then represent
    /// the feature branch rather than the base ref.  Pass this flag when you
    /// intentionally index from a different ref (e.g. in CI where the workspace
    /// is always checked out at the feature branch and the base is accessed via
    /// `--base`).
    #[arg(long)]
    pub no_baseline_check: bool,
}

#[derive(Subcommand, Debug, Clone)]
pub enum PrReviewSubcommand {
    /// Clean up stale review artifact roots for this workspace.
    Clean(CleanArgs),
}

#[derive(Args, Debug, Clone)]
pub struct CleanArgs {
    /// Dry-run — list artifacts that would be deleted; delete nothing.
    #[arg(long)]
    pub dry_run: bool,

    /// Delete the artifact root for one explicit run id.
    #[arg(long, value_name = "ID")]
    pub run_id: Option<String>,

    /// Delete artifacts whose marker base ref resolves to this ref.
    #[arg(long, value_name = "REF")]
    pub base: Option<String>,

    /// Delete artifacts whose marker head ref resolves to this ref.
    #[arg(long, value_name = "REF")]
    pub head: Option<String>,

    /// Delete completed/failed/quarantined artifacts older than this duration.
    /// Format: `<n><unit>` where unit is one of `s`, `m`, `h`, `d`, `w`.
    ///
    /// By default, artifacts whose cache key is still active (both base and
    /// head SHAs resolvable in the current workspace) are skipped.  Use
    /// `--include-active` to override this protection.
    #[arg(long, value_name = "DURATION")]
    pub older_than: Option<String>,

    /// When combined with `--older-than`, also delete artifacts whose cache
    /// key is currently active (i.e. both SHAs are still reachable in this
    /// workspace).  Without this flag, active artifacts are skipped so they
    /// remain available for the next review run.
    ///
    /// Only meaningful with `--older-than`; rejected when paired with
    /// `--all` / `--run-id` / `--base` / `--head` so users do not believe
    /// they are protecting active artifacts that the other selectors will
    /// delete unconditionally.
    #[arg(
        long,
        requires = "older_than",
        conflicts_with_all = ["all", "run_id", "base", "head"],
    )]
    pub include_active: bool,

    /// Delete ALL review artifacts for this workspace.
    #[arg(long)]
    pub all: bool,
}

/// Controls how strictly the exit code is influenced by risks in the report.
#[derive(Clone, Copy, Debug, ValueEnum, Default, PartialEq, Eq)]
pub enum SeverityMode {
    /// Warn-only: emit the report and exit 0 regardless of risk.
    #[default]
    Warn,
    /// Strict: exit 2 if any High-severity risk OR any incompatible
    /// payload-contract shape change (`fields_type_changed` non-empty).
    Strict,
    /// Pedantic: exit 2 on any Medium-or-higher risk, any payload-contract
    /// change at all, or any removed permission/audit decorator.
    Pedantic,
}

/// Output format for `pr-review`.
#[derive(Clone, Copy, Debug, ValueEnum, Default, PartialEq, Eq)]
pub enum OutputFormat {
    /// Human-readable Markdown (default).
    #[default]
    Markdown,
    /// Compact machine-readable JSON.
    Json,
    /// Markdown truncated to GitHub's 65 536-char comment limit.
    GithubComment,
    /// Markdown with YAML frontmatter for Braingent archival.
    Braingent,
}

/// Decorator names that count as permission/audit guards for the Pedantic threshold.
const PERMISSION_AUDIT_DECORATORS: &[&str] =
    &["Permission", "Audit", "Authenticated", "Authorize", "Guard"];

/// Returns `true` when the severity threshold is exceeded and the caller
/// should exit with code 2.
///
/// Logic per mode:
/// - `Warn`: always `false`.
/// - `Strict`:
///   - Any [`RemovedSurfaceRisk`] with `severity == High` → `true`.
///   - Any `payload_contracts.changed[*].fields_type_changed` non-empty → `true`.
///     MVP heuristic — true positives may include intra-repo type changes;
///     revisit when payload contracts get impact attachment.
/// - `Pedantic`:
///   - Any [`RemovedSurfaceRisk`] with severity `Medium` or `High` → `true`.
///   - Any `payload_contracts.changed` entry → `true` (any payload change).
///   - Any `decorators.removed[*]` matching a permission/audit decorator → `true`.
pub fn evaluate_severity_threshold(mode: SeverityMode, report: &DeltaReport) -> bool {
    match mode {
        SeverityMode::Warn => false,
        SeverityMode::Strict => {
            let has_high = report
                .removed_surface_risks
                .iter()
                .any(|r| r.severity == RiskSeverity::High);
            let has_type_change = report
                .payload_contracts
                .changed
                .iter()
                .any(|c| !c.fields_type_changed.is_empty());
            has_high || has_type_change
        }
        SeverityMode::Pedantic => {
            let has_medium_or_high = report
                .removed_surface_risks
                .iter()
                .any(|r| r.severity >= RiskSeverity::Medium);
            let has_any_payload_change = !report.payload_contracts.changed.is_empty();
            let has_removed_permission_decorator = report.decorators.removed.iter().any(|d| {
                PERMISSION_AUDIT_DECORATORS
                    .iter()
                    .any(|&pat| d.decorator_name.contains(pat))
            });
            has_medium_or_high || has_any_payload_change || has_removed_permission_decorator
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum ReviewEngine {
    TempIndex,
}

// ─── Handler ──────────────────────────────────────────────────────────────────

pub fn run(app: &AppContext, args: PrReviewArgs) -> Result<u8> {
    match args.command {
        Some(PrReviewSubcommand::Clean(ref clean_args)) => {
            run_clean(app, &args, clean_args).map(|()| 0)
        }
        None => {
            // Default path: run a review. --base and --head are required here.
            let base = args
                .base
                .as_deref()
                .context("--base is required when running a review with no subcommand.")?
                .to_owned();
            let head = args
                .head
                .as_deref()
                .context("--head is required when running a review with no subcommand.")?
                .to_owned();

            // --strict is a deprecated alias for --severity strict.
            let effective_severity = if args.strict && args.severity == SeverityMode::Warn {
                tracing::warn!(
                    "--strict is deprecated; use --severity strict instead. \
                     --strict will be removed in a future release."
                );
                SeverityMode::Strict
            } else {
                args.severity
            };

            // --json is a deprecated alias for --format json.
            let effective_format = if args.json || app.json_output {
                if args.json {
                    tracing::warn!(
                        "--json is deprecated; use --format json instead. \
                         --json will be removed in a future release."
                    );
                }
                OutputFormat::Json
            } else {
                args.format
            };

            // Reconstruct typed args with the validated required fields.
            let review_args = PrReviewRunArgs {
                base,
                head,
                engine: args.engine,
                keep_cache: args.keep_cache,
                json: args.json,
                cache_root: args.cache_root,
                config: args.config,
                strict: args.strict,
                severity: effective_severity,
                format: effective_format,
                github_comment_file: args.github_comment_file,
                no_baseline_check: args.no_baseline_check,
            };

            let (report, exceeded) = run_inner(app, &review_args)?;
            // Print to stdout, then explicitly flush so the structured report
            // is on the wire before we return — `main` exits via
            // `std::process::ExitCode` so destructors run, but also flushing
            // here keeps the contract explicit at the print site.
            #[expect(
                clippy::print_stdout,
                reason = "pr-review is the sole caller of this path; structured output goes here"
            )]
            {
                println!("{report}");
            }
            let _ = <std::io::Stdout as std::io::Write>::flush(&mut std::io::stdout());
            // Exit code 2 when the severity threshold is exceeded — threaded
            // back through `Result<u8>` so `main` can flush stdio/tracing
            // properly before terminating. Avoids the abrupt `process::exit`
            // that previously truncated output under fully-buffered stdout.
            // Callers distinguish "broke" (exit 1 from anyhow) from
            // "threshold exceeded" (exit 2).
            Ok(if exceeded { 2 } else { 0 })
        }
    }
}

// ─── Validated run-review args ─────────────────────────────────────────────

/// Validated args for the "run a review" path (no subcommand).
///
/// Extracted from `PrReviewArgs` after confirming `--base` and `--head` are
/// present.  Used internally so `run_inner` can still take typed fields.
#[expect(
    clippy::struct_excessive_bools,
    reason = "mirrors PrReviewArgs — each bool is an independent CLI flag; a state machine would obscure intent"
)]
pub struct PrReviewRunArgs {
    pub base: String,
    pub head: String,
    pub engine: ReviewEngine,
    pub keep_cache: bool,
    pub json: bool,
    pub cache_root: Option<PathBuf>,
    /// Optional override for the worktree-root config. When set, the file at
    /// this path is copied into the materialized worktree before indexing,
    /// so callers can review workspaces that do not check in their
    /// `gather-step.config.yaml`.
    pub config: Option<PathBuf>,
    /// Deprecated: kept for backward-compat.  Callers should prefer `severity`.
    pub strict: bool,
    pub severity: SeverityMode,
    /// Output format.  Defaults to `Markdown`.
    ///
    /// When `json = true` (the legacy flag) is set, this is overridden to `Json`.
    pub format: OutputFormat,
    /// If `Some`, write the GitHub-comment rendering to this path in addition to stdout.
    pub github_comment_file: Option<PathBuf>,
    /// When `true`, suppress the workspace-HEAD-vs-base mismatch warning.
    pub no_baseline_check: bool,
}

/// Internal result type for the cache-hit-or-cold-run branch in [`run_inner`].
enum RunOutcome {
    CacheHit(ReviewArtifactRoot),
    ColdRun {
        artifact_root: ReviewArtifactRoot,
        worktree: gather_step_git::worktrees::ReviewWorktree,
        elapsed_ms: u64,
        total_repos: usize,
        /// Surfaces not supported by the active review engine.  Kept typed
        /// so per-surface `_unavailable` checks can match on the variant
        /// rather than comparing free-form strings.
        unsupported_surfaces: Vec<UnsupportedSurface>,
    },
}

/// Best-effort cleanup guard for the review artifact and worktree.
///
/// Runs cleanup on `Drop` unless [`Self::disarm`] has been called or the
/// guard was constructed with [`CacheRetention::Keep`]. This keeps the
/// disposable cache contract (`<cache>/gather-step/pr-review/<hash>/<id>/`
/// is removed unless `--keep-cache`) under panic, signal, and early-return
/// paths — not just the happy-return path.
///
/// When `retention` is [`CacheRetention::Keep`], the guard does NOT remove
/// anything on Drop. The user explicitly opted to inspect this run's
/// artifact, including failures. Failed runs reach Drop with the artifact
/// marker already set to [`ReviewStatus::Quarantined`] by
/// [`quarantine_on_error`], so the artifact remains discoverable by
/// `pr-review clean --run-id <id>` or `pr-review clean --older-than <duration>`.
struct ReviewCleanupGuard {
    artifact_root_path: Option<std::path::PathBuf>,
    worktree: Option<ReviewWorktree>,
    retention: CacheRetention,
}

impl ReviewCleanupGuard {
    fn new(retention: CacheRetention) -> Self {
        Self {
            artifact_root_path: None,
            worktree: None,
            retention,
        }
    }

    fn arm(&mut self, artifact_root_path: std::path::PathBuf, worktree: Option<ReviewWorktree>) {
        self.artifact_root_path = Some(artifact_root_path);
        self.worktree = worktree;
    }

    fn disarm(&mut self) {
        self.artifact_root_path = None;
        self.worktree = None;
    }
}

impl Drop for ReviewCleanupGuard {
    fn drop(&mut self) {
        if self.retention.keeps_cache() {
            // User opted to inspect the artifact (including failures).
            // Drop the references so we don't double-cleanup if a later
            // explicit cleanup path runs, but do not touch disk.
            self.artifact_root_path = None;
            self.worktree = None;
            return;
        }
        // Worktree removal is the gate for artifact-root removal: if `git
        // worktree remove` fails (e.g. dirty working tree, locked refs), the
        // source repo's `.git/worktrees/<id>` pointer is still present and
        // would be left dangling if we deleted the artifact dir it points
        // at. Quarantine instead so the user can run `pr-review clean
        // --run-id <id>` once the worktree state is unstuck.
        let mut worktree_removed = true;
        if let Some(wt) = self.worktree.take()
            && let Err(e) = remove_worktree(&wt)
        {
            worktree_removed = false;
            tracing::warn!(
                error = %e,
                worktree = %wt.root.display(),
                "pr-review cleanup guard: Failed to remove the worktree on a panic or early-return path; leaving the artifact for manual cleanup.",
            );
        }
        if let Some(path) = self.artifact_root_path.take() {
            if !worktree_removed {
                let quarantined = quarantine_artifact_path(&path);
                if quarantined {
                    tracing::warn!(
                        path = %path.display(),
                        "pr-review cleanup guard: Skipping artifact directory removal because worktree removal failed. The artifact marker was moved to the Quarantined state. \
                         Run `gather-step pr-review clean --run-id <id>` after fixing the worktree state.",
                    );
                } else {
                    tracing::warn!(
                        path = %path.display(),
                        "pr-review cleanup guard: Skipping artifact directory removal because worktree removal failed, and the marker could not be transitioned to Quarantined. \
                         Run `gather-step pr-review clean --run-id <id>` after fixing the worktree state.",
                    );
                }
                return;
            }
            if path.exists()
                && let Err(e) = std::fs::remove_dir_all(&path)
            {
                tracing::warn!(
                    error = %e,
                    path = %path.display(),
                    "pr-review cleanup guard: Failed to remove the artifact directory on a panic or early-return path.",
                );
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CacheRetention {
    Discard,
    Keep,
}

impl CacheRetention {
    fn from_keep_cache(keep_cache: bool) -> Self {
        if keep_cache {
            Self::Keep
        } else {
            Self::Discard
        }
    }

    fn keeps_cache(self) -> bool {
        matches!(self, Self::Keep)
    }
}

/// Core implementation — returns `(rendered_string, threshold_exceeded)`.
///
/// `threshold_exceeded` is `true` when the effective [`SeverityMode`] is
/// exceeded (see [`evaluate_severity_threshold`]).  The caller uses this to
/// exit with code 2 AFTER printing the report.
pub fn run_inner(app: &AppContext, args: &PrReviewRunArgs) -> Result<(String, bool)> {
    #[cfg(test)]
    let _temp_index_test_guard = if matches!(args.engine, ReviewEngine::TempIndex) {
        // TempIndex tests materialize Tantivy search indexes, which can exceed
        // the default macOS file-descriptor limit when lib tests run in
        // parallel. Serialize this path in tests only; production behavior and
        // integration-test binaries are unchanged.
        Some(match TEMP_INDEX_TEST_LOCK.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        })
    } else {
        None
    };

    // Legacy --json flag coerces to Json format.
    let effective_format = if args.json || app.json_output {
        OutputFormat::Json
    } else {
        args.format
    };
    // `effective_format` is used directly for render dispatch; no separate bool needed.

    // ── 1. Resolve refs ────────────────────────────────────────────────────
    let resolved =
        resolve_range(&app.workspace_path, &args.base, &args.head).with_context(|| {
            format!(
                "Resolving refs `{}..{}` in `{}`.",
                args.base,
                args.head,
                app.workspace_path.display()
            )
        })?;

    let base_sha = resolved.base.sha.clone();
    let head_sha = resolved.head.sha.clone();

    // ── 1b. Baseline-index coherence check ────────────────────────────────
    // Warn (but do not abort) when the workspace HEAD differs from the resolved
    // base SHA.  In that case the workspace's `.gather-step/storage` was most
    // likely indexed against the feature branch, so baseline deltas will be
    // empty or misleading.  Users who intentionally index from a different ref
    // (e.g. CI) can suppress this with `--no-baseline-check`.
    let mut baseline_warnings: Vec<String> = Vec::new();
    if !args.no_baseline_check {
        match gather_step_git::refs::resolve_ref(&app.workspace_path, "HEAD") {
            Ok(ws_head) => {
                let ws_short = &ws_head.sha[..ws_head.sha.len().min(12)];
                let base_short = &base_sha[..base_sha.len().min(12)];
                if ws_head.sha != base_sha {
                    baseline_warnings.push(format!(
                        "The workspace HEAD {ws_short} does not match --base {base_short}; \
                         the baseline index may not represent the base reference. \
                         Re-run after `git checkout {base_short}` and `gather-step index` \
                         for accurate deltas, or pass --no-baseline-check to suppress this warning."
                    ));
                }
            }
            Err(e) => {
                tracing::debug!(error = %e, "Could not resolve workspace HEAD for the baseline check.");
            }
        }
    }

    // ── 2. Changed files ───────────────────────────────────────────────────
    let diff_base_sha = merge_base(&app.workspace_path, &base_sha, &head_sha)
        .with_context(|| format!("Finding the merge-base for `{base_sha}` and `{head_sha}`."))?;
    let changed =
        changed_files(&app.workspace_path, &diff_base_sha, &head_sha).with_context(|| {
            format!("Listing changed files between merge-base `{diff_base_sha}` and `{head_sha}`.")
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

    // ── 3b. Cache key ──────────────────────────────────────────────────────
    // Read the config file the engine will actually use, so the cache key
    // identifies "same config" exactly when the config the head worktree
    // sees is identical.
    //
    // Selection precedence:
    //   1. `--config <path>` override → read once, then hash and write those
    //      exact bytes into the worktree.
    //   2. Otherwise, hash the bytes committed at `<head_sha>:gather-step.config.yaml`
    //      via `git show`. This is what the detached worktree will check out,
    //      not the user's working tree (which may be at a different ref).
    //   3. If neither is readable, hash empty bytes — the indexer will fail
    //      later with a clearer error if a config is genuinely required.
    let config_override_path: Option<PathBuf> = args.config.clone();
    if let Some(path) = config_override_path.as_ref() {
        if !path.exists() {
            anyhow::bail!(
                "The --config path does not exist: `{}`. Next step: pass an existing `gather-step.config.yaml` or omit `--config`.",
                path.display()
            );
        }
        if path.is_dir() {
            anyhow::bail!(
                "The --config path is a directory, not a file: `{}`.",
                path.display()
            );
        }
    }
    let config_override_bytes: Option<Vec<u8>> = if let Some(path) = config_override_path.as_ref() {
        Some(
            std::fs::read(path)
                .with_context(|| format!("Reading --config from `{}`.", path.display()))?,
        )
    } else {
        None
    };
    let config_bytes: Vec<u8> = if let Some(bytes) = config_override_bytes.as_ref() {
        bytes.clone()
    } else {
        read_config_at_sha(&app.workspace_path, &head_sha)
    };
    let cache_key_struct =
        compute_cache_key(&app.workspace_path, &base_sha, &head_sha, &config_bytes);

    // ── 4. Artifact root ───────────────────────────────────────────────────
    let cache_root = args
        .cache_root
        .clone()
        .unwrap_or_else(|| default_cache_root(&app.workspace_path));

    // Best-effort cleanup guard. Armed on cold-run paths after the worktree is
    // created; disarmed on the happy path before the explicit cleanup block
    // takes over. If the function panics or returns Err between arming and
    // disarming, the Drop impl removes the worktree and artifact root rather
    // than leaving them as InProgress orphans in the user's OS cache.
    let mut cleanup_guard =
        ReviewCleanupGuard::new(CacheRetention::from_keep_cache(args.keep_cache));

    // Try to reuse a prior completed artifact with the same cache key.
    // Cache reuse is independent of `keep_cache` — `keep_cache` controls
    // whether the CURRENT run's artifact is preserved after delta extraction.
    let mut was_cache_hit = false;
    let outcome: RunOutcome = if let Some(hit_root) =
        try_reuse_cache(&cache_root, &cache_key_struct).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "Cache lookup failed; falling back to a cold run.");
            None
        }) {
        let workspace_ctx = StorageContext::workspace_read_only(app);
        let _review_ctx = StorageContext::review_checked(
            &workspace_ctx,
            hit_root.root.clone(),
            hit_root.registry_path.clone(),
            hit_root.storage_root.clone(),
            hit_root.run_id.clone(),
        )
        .map_err(ArtifactRootError::Safety)
        .with_context(|| "The review safety guard rejected cached artifact paths.")?;
        // Cache hit: skip worktree creation and indexing.
        was_cache_hit = true;
        RunOutcome::CacheHit(hit_root)
    } else {
        // Cache miss: create a fresh artifact root, worktree, and index.
        let run_id = generate_run_id();

        let artifact_root = plan_artifact_root(&cache_root, &app.workspace_path, &run_id)
            .with_context(|| format!("Planning the artifact root for run `{run_id}`."))?;

        // Safety guard: construct both contexts and verify no path overlap before
        // creating review directories, writing the marker, or opening any review
        // storage.
        let workspace_ctx = StorageContext::workspace_read_only(app);
        let _review_ctx = StorageContext::review_checked(
            &workspace_ctx,
            artifact_root.root.clone(),
            artifact_root.registry_path.clone(),
            artifact_root.storage_root.clone(),
            run_id.clone(),
        )
        .map_err(ArtifactRootError::Safety)
        .with_context(|| "The review safety guard rejected the proposed artifact paths.")?;

        materialize_artifact_root(
            &artifact_root,
            &base_sha,
            &head_sha,
            Some(cache_key_struct.clone()),
        )
        .with_context(|| format!("Creating the artifact root for run `{run_id}`."))?;

        // ── 5. Materialize worktree ────────────────────────────────────────
        // `materialize_artifact_root` pre-creates the worktree directory; git worktree
        // add refuses to clobber an existing directory, so remove it first.
        if artifact_root.worktree_root.exists() {
            std::fs::remove_dir(&artifact_root.worktree_root).with_context(|| {
                format!(
                    "Removing the pre-created worktree placeholder at `{}`.",
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
                        "Creating the detached worktree at `{}`.",
                        artifact_root.worktree_root.display()
                    )
                });
            }
        };

        // Arm the cleanup guard now that both the artifact root and worktree
        // exist on disk. Any panic or `?`-propagated error after this point
        // will trigger best-effort cleanup via Drop.
        cleanup_guard.arm(artifact_root.root.clone(), Some(worktree.clone()));

        // ── 5a. --config override ─────────────────────────────────────────
        // When the user passed `--config <path>`, write the exact bytes that
        // were hashed for the cache key into the
        // worktree root so the indexer reads the requested config instead of
        // whatever the head commit checked in (or the absent-file case).
        // Done AFTER worktree creation so we overwrite the committed config
        // rather than racing with `git worktree add`.
        if let (Some(src), Some(bytes)) = (
            config_override_path.as_ref(),
            config_override_bytes.as_ref(),
        ) {
            let dst = artifact_root.worktree_root.join("gather-step.config.yaml");
            if let Err(e) = std::fs::write(&dst, bytes) {
                quarantine_on_error(&artifact_root);
                return Err(anyhow::Error::from(e)).with_context(|| {
                    format!(
                        "Writing --config `{}` into the review worktree at `{}`.",
                        src.display(),
                        dst.display()
                    )
                });
            }
        }

        // ── 5b. Seed from baseline (Task 3) ───────────────────────────────
        // If the workspace has a normal index with a matching config hash,
        // copy it into the review artifact root so the indexer only needs to
        // update changed repos rather than rebuild from scratch.
        match pick_seed_source(&app.workspace_path, &cache_key_struct.config_hash) {
            Ok(Some(seed)) => {
                tracing::info!("Seeding the review artifact from the baseline workspace index.");
                if let Err(e) = seed_artifact_root(&seed, &artifact_root) {
                    // Non-fatal: log and continue with a full index.
                    tracing::warn!(
                        error = %e,
                        "Seeding the artifact root failed. Falling back to a full reindex."
                    );
                }
            }
            Ok(None) => {
                tracing::debug!(
                    "The workspace baseline is not seedable. Running a full review index."
                );
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Seed source selection failed. Falling back to a full reindex."
                );
            }
        }

        // ── 5c. Compute affected repos (Task 4) ───────────────────────────
        // Load the head config from the worktree for prefix matching.
        let head_cfg_for_affected = {
            let cfg_path = artifact_root.worktree_root.join("gather-step.config.yaml");
            GatherStepConfig::from_yaml_file(&cfg_path).ok()
        };

        // Open the baseline graph for reverse-dependents expansion.  Fail-soft:
        // if the baseline doesn't exist yet (user hasn't indexed), pass None and
        // fall back to direct-change-only affected set.
        let affected_baseline_coord = {
            let baseline_storage = app.workspace_paths().storage_root;
            if baseline_storage.join("graph.redb").exists() {
                match gather_step_storage::StorageCoordinator::open_read_only(&baseline_storage) {
                    Ok(coord) => Some(coord),
                    Err(e) => {
                        tracing::debug!(
                            error = %e,
                            "baseline storage not openable for affected-repo expansion; \
                             skipping reverse-dependents walk"
                        );
                        None
                    }
                }
            } else {
                None
            }
        };

        let affected: Option<AffectedRepos> = head_cfg_for_affected.as_ref().map(|cfg| {
            let a = match affected_baseline_coord.as_ref() {
                Some(coord) => compute_affected_repos(cfg, &changed, Some(coord.graph())),
                None => {
                    compute_affected_repos::<gather_step_storage::GraphStoreDb>(cfg, &changed, None)
                }
            };
            if !a.all_repos && !a.repos.is_empty() {
                tracing::info!(
                    repos = ?a.repos,
                    truncated = a.expansion_truncated,
                    "seeded baseline + reindexing {} affected repos: {:?}",
                    a.repos.len(),
                    a.repos
                );
            }
            a
        });

        // ── 6. Materialize via engine ──────────────────────────────────────
        let index_start = Instant::now();
        let engine: Box<dyn ReviewEngineImpl> = match args.engine {
            ReviewEngine::TempIndex => Box::new(TempIndexEngine),
        };
        let engine_snapshot = match engine.materialize(
            &artifact_root,
            affected.as_ref(),
            IndexingOptions::default(),
        ) {
            Ok(s) => s,
            Err(e) => {
                quarantine_on_error(&artifact_root);
                return Err(e).with_context(|| "Review engine materialization failed.");
            }
        };
        let engine_total_repos = engine_snapshot.total_repos;
        let engine_unsupported: Vec<UnsupportedSurface> =
            engine_snapshot.unsupported_surfaces.clone();
        // Truncation is intentional: no real indexing run takes > 584 million years.
        #[expect(
            clippy::cast_possible_truncation,
            reason = "elapsed_ms will never overflow u64 in practice"
        )]
        let elapsed_ms = index_start.elapsed().as_millis() as u64;

        RunOutcome::ColdRun {
            artifact_root,
            worktree,
            elapsed_ms,
            total_repos: engine_total_repos,
            unsupported_surfaces: engine_unsupported,
        }
    };

    // Destructure the outcome for the remainder of the function.
    let (artifact_root, worktree_opt, elapsed_ms, total_repos_hint, run_unsupported_surfaces) =
        match outcome {
            RunOutcome::CacheHit(root) => (root, None, 0u64, None, vec![]),
            RunOutcome::ColdRun {
                artifact_root,
                worktree,
                elapsed_ms,
                total_repos,
                unsupported_surfaces,
            } => (
                artifact_root,
                Some(worktree),
                elapsed_ms,
                Some(total_repos),
                unsupported_surfaces,
            ),
        };

    // ── 7. Head config-derived repo names ─────────────────────────────────
    let config_path = artifact_root.worktree_root.join("gather-step.config.yaml");
    let head_config = GatherStepConfig::from_yaml_file(&config_path).ok();
    let indexed_repos: Vec<String> = head_config.as_ref().map_or_else(
        || {
            let n = total_repos_hint.unwrap_or(0);
            (0..n).map(|i| format!("repo-{i}")).collect()
        },
        |config| config.repos.iter().map(|r| r.name.clone()).collect(),
    );
    let changed_repos = map_changed_repos_from_config(head_config.as_ref(), &all_changed_paths);

    // ── 8. Build report ────────────────────────────────────────────────────
    let ws_paths = app.workspace_paths();
    let ws_hash = workspace_hash(&app.workspace_path);
    let cache_key = format!("{ws_hash}:{base_sha}:{head_sha}");

    let cleanup_policy = if args.keep_cache {
        CleanupPolicy::KeepCache
    } else if was_cache_hit {
        CleanupPolicy::CacheHitRetained
    } else {
        CleanupPolicy::RemoveOnExit
    };

    // ── 8a. Open storage coordinators for diff extraction ─────────────────────
    // Fail-soft: if the workspace has never been indexed, emit empty deltas and
    // log a warning rather than aborting the entire review run.
    // We check for directory existence first so we never create the baseline
    // storage path as a side effect of opening the coordinator.
    //
    // Per-surface `unavailable` flags are set when the engine reports a surface
    // as unsupported.  Extractors are skipped for those surfaces; the flag tells
    // the renderer to print an informational note instead of an empty section.
    let unsupported = &run_unsupported_surfaces;
    let has_surface = |s: UnsupportedSurface| unsupported.contains(&s);
    let routes_unavailable = has_surface(UnsupportedSurface::Routes);
    let symbols_unavailable = has_surface(UnsupportedSurface::Symbols);
    let payload_contracts_unavailable = has_surface(UnsupportedSurface::PayloadContracts);
    let events_unavailable = has_surface(UnsupportedSurface::Events);
    let decorators_unavailable = has_surface(UnsupportedSurface::Decorators);
    let contract_alignments_unavailable = has_surface(UnsupportedSurface::ContractAlignments);
    let deployment_unavailable = has_surface(UnsupportedSurface::Deployment);

    let (
        route_deltas,
        symbol_deltas,
        payload_contract_deltas,
        event_deltas,
        surface_risks,
        contract_alignments,
        decorator_deltas,
        deployment_deltas,
    ) = {
        let baseline_graph_exists = ws_paths.storage_root.join("graph.redb").exists();
        if baseline_graph_exists {
            let review_coord = gather_step_storage::StorageCoordinator::open_read_only(
                &artifact_root.storage_root,
            );
            match review_coord {
                Ok(review_coord) => {
                    let baseline_coord = gather_step_storage::StorageCoordinator::open_read_only(
                        &ws_paths.storage_root,
                    );
                    match baseline_coord {
                        Ok(baseline_coord) => {
                            let routes = if routes_unavailable {
                                RouteDeltas {
                                    unavailable: true,
                                    ..RouteDeltas::default()
                                }
                            } else {
                                match extract_route_deltas(
                                    baseline_coord.graph(),
                                    review_coord.graph(),
                                ) {
                                    Ok(d) => d,
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            "Route delta extraction failed. Emitting empty deltas."
                                        );
                                        RouteDeltas::default()
                                    }
                                }
                            };
                            let symbols = if symbols_unavailable {
                                SymbolDeltas {
                                    unavailable: true,
                                    ..SymbolDeltas::default()
                                }
                            } else {
                                match extract_symbol_deltas(
                                    baseline_coord.graph(),
                                    review_coord.graph(),
                                ) {
                                    Ok(d) => d,
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            "Symbol delta extraction failed. Emitting empty deltas."
                                        );
                                        SymbolDeltas::default()
                                    }
                                }
                            };
                            let payload_contracts = if payload_contracts_unavailable {
                                PayloadContractDeltas {
                                    unavailable: true,
                                    ..PayloadContractDeltas::default()
                                }
                            } else {
                                match extract_payload_contract_deltas(
                                    baseline_coord.metadata(),
                                    review_coord.metadata(),
                                ) {
                                    Ok(d) => d,
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            "Payload-contract delta extraction failed. \
                                             Emitting empty deltas."
                                        );
                                        PayloadContractDeltas::default()
                                    }
                                }
                            };
                            let events = if events_unavailable {
                                EventDeltas {
                                    unavailable: true,
                                    ..EventDeltas::default()
                                }
                            } else {
                                match extract_event_deltas(
                                    baseline_coord.graph(),
                                    review_coord.graph(),
                                ) {
                                    Ok(d) => d,
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            "Event delta extraction failed. Emitting empty deltas."
                                        );
                                        EventDeltas::default()
                                    }
                                }
                            };

                            // Risks depend on routes/symbols/events removed lists; if any
                            // of those are unavailable the removed lists are empty so risks
                            // will be empty too — that is acceptable for the overlay engine.
                            let risks = match extract_removed_surface_risks(
                                baseline_coord.graph(),
                                review_coord.graph(),
                                &routes.removed,
                                &symbols.removed,
                                &events.removed,
                            ) {
                                Ok(r) => r,
                                Err(e) => {
                                    tracing::warn!(
                                        error = %e,
                                        "Removed-surface risk extraction failed. \
                                         Emitting empty risks."
                                    );
                                    vec![]
                                }
                            };

                            // ── Phase 3: attach impact to removed/changed routes ──
                            let mut routes = routes;
                            if !routes.unavailable {
                                for r in &mut routes.removed {
                                    match find_route_node_id(
                                        baseline_coord.graph(),
                                        &r.method,
                                        &r.path,
                                    ) {
                                        Ok(Some(node_id)) => {
                                            match impact_for_node(
                                                baseline_coord.graph(),
                                                node_id,
                                                r.repo.as_deref(),
                                            ) {
                                                Ok(summary) => r.impact = Some(summary),
                                                Err(e) => tracing::warn!(
                                                    error = %e,
                                                    method = %r.method,
                                                    path = %r.path,
                                                    "Impact attachment failed for a removed route."
                                                ),
                                            }
                                        }
                                        Ok(None) => {}
                                        Err(e) => tracing::warn!(
                                            error = %e,
                                            method = %r.method,
                                            path = %r.path,
                                            "Route node lookup failed."
                                        ),
                                    }
                                }
                                for c in &mut routes.changed {
                                    // Impact is computed against the BASELINE node.
                                    let (method, path) = (&c.method.clone(), &c.path.clone());
                                    match find_route_node_id(baseline_coord.graph(), method, path) {
                                        Ok(Some(node_id)) => {
                                            let repo =
                                                c.before.as_ref().and_then(|b| b.repo.as_deref());
                                            match impact_for_node(
                                                baseline_coord.graph(),
                                                node_id,
                                                repo,
                                            ) {
                                                Ok(summary) => {
                                                    if let Some(before) = c.before.as_mut() {
                                                        before.impact = Some(summary);
                                                    }
                                                }
                                                Err(e) => tracing::warn!(
                                                    error = %e,
                                                    method = %method,
                                                    path = %path,
                                                    "Impact attachment failed for a changed route."
                                                ),
                                            }
                                        }
                                        Ok(None) => {}
                                        Err(e) => tracing::warn!(
                                            error = %e,
                                            method = %method,
                                            path = %path,
                                            "Route node lookup failed for a changed route."
                                        ),
                                    }
                                }
                            }

                            // ── Phase 3: attach impact to removed/changed symbols ─
                            let mut symbols = symbols;
                            if !symbols.unavailable {
                                for s in &mut symbols.removed {
                                    match find_symbol_node_id(
                                        baseline_coord.graph(),
                                        &s.repo,
                                        &s.qualified_name,
                                    ) {
                                        Ok(Some(node_id)) => {
                                            match impact_for_node(
                                                baseline_coord.graph(),
                                                node_id,
                                                Some(&s.repo),
                                            ) {
                                                Ok(summary) => s.impact = Some(summary),
                                                Err(e) => tracing::warn!(
                                                    error = %e,
                                                    repo = %s.repo,
                                                    qn = %s.qualified_name,
                                                    "Impact attachment failed for a removed symbol."
                                                ),
                                            }
                                        }
                                        Ok(None) => {}
                                        Err(e) => tracing::warn!(
                                            error = %e,
                                            repo = %s.repo,
                                            qn = %s.qualified_name,
                                            "Symbol node lookup failed."
                                        ),
                                    }
                                }
                                for c in &mut symbols.changed {
                                    // Impact on the BASELINE node.
                                    let (repo, qn) = (c.repo.clone(), c.qualified_name.clone());
                                    match find_symbol_node_id(baseline_coord.graph(), &repo, &qn) {
                                        Ok(Some(node_id)) => {
                                            match impact_for_node(
                                                baseline_coord.graph(),
                                                node_id,
                                                Some(&repo),
                                            ) {
                                                Ok(summary) => c.before.impact = Some(summary),
                                                Err(e) => tracing::warn!(
                                                    error = %e,
                                                    repo = %repo,
                                                    qn = %qn,
                                                    "Impact attachment failed for a changed symbol."
                                                ),
                                            }
                                        }
                                        Ok(None) => {}
                                        Err(e) => tracing::warn!(
                                            error = %e,
                                            repo = %repo,
                                            qn = %qn,
                                            "Symbol node lookup failed for a changed symbol."
                                        ),
                                    }
                                }
                            }

                            // ── Phase 3: attach impact to removed/changed payload contracts ──
                            let mut payload_contracts = payload_contracts;
                            if !payload_contracts.unavailable {
                                for c in &mut payload_contracts.removed {
                                    match find_payload_contract_node_id(
                                        baseline_coord.metadata(),
                                        &c.repo,
                                        &c.file,
                                        &c.target_qualified_name,
                                        &c.side,
                                    ) {
                                        Ok(Some(node_id)) => {
                                            match impact_for_node(
                                                baseline_coord.graph(),
                                                node_id,
                                                Some(&c.repo),
                                            ) {
                                                Ok(summary) => c.impact = Some(summary),
                                                Err(e) => tracing::warn!(
                                                    error = %e,
                                                    repo = %c.repo,
                                                    target = %c.target_qualified_name,
                                                    "Impact attachment failed for a removed \
                                                     payload contract."
                                                ),
                                            }
                                        }
                                        Ok(None) => {}
                                        Err(e) => tracing::warn!(
                                            error = %e,
                                            repo = %c.repo,
                                            target = %c.target_qualified_name,
                                            "Payload contract node lookup failed."
                                        ),
                                    }
                                }
                                for c in &mut payload_contracts.changed {
                                    match find_payload_contract_node_id(
                                        baseline_coord.metadata(),
                                        &c.repo,
                                        &c.file,
                                        &c.target_qualified_name,
                                        &c.side,
                                    ) {
                                        Ok(Some(node_id)) => {
                                            match impact_for_node(
                                                baseline_coord.graph(),
                                                node_id,
                                                Some(&c.repo),
                                            ) {
                                                Ok(summary) => c.impact = Some(summary),
                                                Err(e) => tracing::warn!(
                                                    error = %e,
                                                    repo = %c.repo,
                                                    target = %c.target_qualified_name,
                                                    "Impact attachment failed for a changed \
                                                     payload contract."
                                                ),
                                            }
                                        }
                                        Ok(None) => {}
                                        Err(e) => tracing::warn!(
                                            error = %e,
                                            repo = %c.repo,
                                            target = %c.target_qualified_name,
                                            "Payload contract node lookup failed for a changed contract."
                                        ),
                                    }
                                }
                            }

                            // ── Phase 3 Task 3: contract alignment ───────────
                            let contract_alignments = if contract_alignments_unavailable {
                                ContractAlignments {
                                    unavailable: true,
                                    ..ContractAlignments::default()
                                }
                            } else {
                                match extract_contract_alignments(
                                    review_coord.metadata(),
                                    &payload_contracts,
                                ) {
                                    Ok(a) => a,
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            "Contract alignment extraction failed. \
                                             Emitting empty alignments."
                                        );
                                        ContractAlignments::default()
                                    }
                                }
                            };

                            // ── Phase 3 Task 4: decorator deltas ──────────────
                            let decorator_deltas = if decorators_unavailable {
                                DecoratorDeltas {
                                    unavailable: true,
                                    ..DecoratorDeltas::default()
                                }
                            } else {
                                match extract_decorator_deltas(
                                    baseline_coord.graph(),
                                    review_coord.graph(),
                                ) {
                                    Ok(d) => d,
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            "Decorator delta extraction failed. \
                                             Emitting empty decorator deltas."
                                        );
                                        DecoratorDeltas::default()
                                    }
                                }
                            };

                            // ── Phase 7: deployment topology deltas ──────────
                            let deployment_deltas = if deployment_unavailable {
                                DeploymentDeltas {
                                    unavailable: true,
                                    ..DeploymentDeltas::default()
                                }
                            } else {
                                match extract_deployment_deltas(
                                    baseline_coord.graph(),
                                    review_coord.graph(),
                                ) {
                                    Ok(d) => d,
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            "Deployment delta extraction failed. \
                                             Emitting empty deltas."
                                        );
                                        DeploymentDeltas::default()
                                    }
                                }
                            };

                            (
                                routes,
                                symbols,
                                payload_contracts,
                                events,
                                risks,
                                contract_alignments,
                                decorator_deltas,
                                deployment_deltas,
                            )
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                "Baseline storage could not be opened. \
                                 Emitting empty deltas."
                            );
                            (
                                RouteDeltas::default(),
                                SymbolDeltas::default(),
                                PayloadContractDeltas::default(),
                                EventDeltas::default(),
                                vec![],
                                ContractAlignments::default(),
                                DecoratorDeltas::default(),
                                DeploymentDeltas::default(),
                            )
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "Review storage could not be opened for diff extraction. \
                         Emitting empty deltas."
                    );
                    (
                        RouteDeltas::default(),
                        SymbolDeltas::default(),
                        PayloadContractDeltas::default(),
                        EventDeltas::default(),
                        vec![],
                        ContractAlignments::default(),
                        DecoratorDeltas::default(),
                        DeploymentDeltas::default(),
                    )
                }
            }
        } else {
            tracing::warn!(
                storage = %ws_paths.storage_root.display(),
                "Baseline index was not found. Run `gather-step index` first \
                 to enable PR-review deltas."
            );
            (
                RouteDeltas::default(),
                SymbolDeltas::default(),
                PayloadContractDeltas::default(),
                EventDeltas::default(),
                vec![],
                ContractAlignments::default(),
                DecoratorDeltas::default(),
                DeploymentDeltas::default(),
            )
        }
    };

    // ── Phase 3 Task 5: synthesize targeted pack commands ──────────────────
    let mut suggested_followups = build_suggested_followups(
        &app.workspace_path,
        &artifact_root.registry_path,
        &artifact_root.storage_root,
    );
    let pack_cmds = synthesize_review_pack_commands(
        &app.workspace_path,
        &artifact_root.registry_path,
        &artifact_root.storage_root,
        &route_deltas,
        &symbol_deltas,
        &payload_contract_deltas,
        &surface_risks,
    );
    suggested_followups.extend(pack_cmds);

    // Source unsupported_surfaces from the per-surface unavailable flags so the
    // report-level list stays in sync with the surface structs.
    let derived_unsupported_surfaces: Vec<String> = [
        ("routes", route_deltas.unavailable),
        ("symbols", symbol_deltas.unavailable),
        ("payload_contracts", payload_contract_deltas.unavailable),
        ("events", event_deltas.unavailable),
        ("decorators", decorator_deltas.unavailable),
        ("contract_alignments", contract_alignments.unavailable),
        ("deployment", deployment_deltas.unavailable),
    ]
    .into_iter()
    .filter(|&(_, unavailable)| unavailable)
    .map(|(name, _)| name.to_owned())
    .collect();

    let report = DeltaReport {
        schema_version: DELTA_REPORT_SCHEMA_VERSION,
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
            warnings: baseline_warnings,
        },
        safety: SafetyMetadata {
            baseline_registry_path: ws_paths.registry_path.clone(),
            baseline_storage_path: ws_paths.storage_root.clone(),
            review_registry_path: artifact_root.registry_path.clone(),
            review_storage_path: artifact_root.storage_root.clone(),
            review_root: artifact_root.root.clone(),
            run_id: artifact_root.run_id.clone(),
            cleanup_policy,
            cache_key,
        },
        changed_files: changed_files_display,
        changed_files_truncated,
        routes: route_deltas,
        symbols: symbol_deltas,
        payload_contracts: payload_contract_deltas,
        events: event_deltas,
        removed_surface_risks: surface_risks,
        contract_alignments,
        decorators: decorator_deltas,
        deployment: deployment_deltas,
        suggested_followups,
        unsupported_surfaces: derived_unsupported_surfaces,
    };

    // ── 9. Update marker ───────────────────────────────────────────────────
    // Mark completed before cleanup so the marker is correct even if cleanup
    // fails. Surface the failure as a warning so an unwritable marker (which
    // would silently disable cache reuse) does not pass unnoticed when
    // --keep-cache was requested.
    if let Err(e) = write_marker_completed(&artifact_root) {
        if args.keep_cache {
            tracing::warn!(
                error = %e,
                run_id = %artifact_root.run_id,
                "pr-review --keep-cache: Failed to write the completed marker. The cache may not be reusable on the next run.",
            );
        } else {
            tracing::debug!(error = %e, "The pr-review command failed to write the completed marker.");
        }
    }

    // The explicit cleanup logic below owns deletion; disarm the guard so its
    // Drop does not double-remove or fight the explicit policy.
    cleanup_guard.disarm();

    // ── 10. Evaluate severity threshold ───────────────────────────────────
    let has_high_risk = evaluate_severity_threshold(args.severity, &report);

    // ── 11. Render ─────────────────────────────────────────────────────────
    let rendered = match effective_format {
        OutputFormat::Json => report
            .render_json()
            .context("Serializing the delta report to JSON.")?,
        OutputFormat::GithubComment => report.render_github_comment(GITHUB_COMMENT_LIMIT),
        OutputFormat::Braingent => report.render_braingent(),
        OutputFormat::Markdown => report.render_markdown(),
    };

    // ── 12. Optional github-comment file write ─────────────────────────────
    if let Some(ref path) = args.github_comment_file {
        let comment = match effective_format {
            OutputFormat::GithubComment => rendered.clone(),
            _ => report.render_github_comment(GITHUB_COMMENT_LIMIT),
        };
        std::fs::write(path, &comment)
            .with_context(|| format!("Writing the GitHub comment to `{}`.", path.display()))?;
    }

    // ── 13. Cleanup ────────────────────────────────────────────────────────
    if was_cache_hit {
        // Cache hit: NEVER delete on success.  The cached artifact exists for
        // future reuse and is only wiped explicitly via `pr-review clean`.
        // Update the access timestamp so `--older-than` pruning measures
        // last-use time, not creation time.
        if let Err(e) = touch_marker_accessed(&artifact_root) {
            tracing::debug!(error = %e, "The pr-review command failed to update the marker access timestamp.");
        }
    } else if !args.keep_cache {
        // Fresh run: remove the worktree then the artifact root.  Errors are
        // logged but do not fail the command — the marker is already Completed.
        let mut worktree_removed_cleanly = true;
        if let Some(wt) = worktree_opt
            && let Err(e) = remove_worktree(&wt)
        {
            worktree_removed_cleanly = false;
            tracing::warn!(
                error = %e,
                worktree = %wt.root.display(),
                "pr-review cleanup: Failed to remove the worktree. \
                 `git worktree remove` may have left a dangling pointer in `.git/worktrees/`.",
            );
        }
        if worktree_removed_cleanly {
            // remove_worktree already does belt-and-suspenders directory removal
            // and `git worktree prune`, so only nuke the artifact root if that
            // path succeeded — otherwise we'd leave a dangling worktree pointer
            // behind in the source repo's `.git/worktrees/`.
            if let Err(e) = std::fs::remove_dir_all(&artifact_root.root) {
                tracing::warn!(
                    error = %e,
                    path = %artifact_root.root.display(),
                    "pr-review cleanup: Failed to remove the artifact directory.",
                );
            }
        }
    }

    Ok((rendered, has_high_risk))
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Map changed file paths to configured repo names using longest-prefix
/// matching against the supplied head-worktree config. Files that do not match
/// any configured repo are grouped under the synthetic `"<workspace>"` entry.
fn map_changed_repos_from_config(
    config: Option<&GatherStepConfig>,
    changed_paths: &[String],
) -> Vec<String> {
    let repos: Vec<(String, String)> = config.map_or_else(Vec::new, |cfg| {
        cfg.repos
            .iter()
            .map(|repo| (repo.name.clone(), repo.path.clone()))
            .collect()
    });

    let mut result_set = std::collections::BTreeSet::new();

    for file_path in changed_paths {
        let matched = repos
            .iter()
            .filter(|(_, repo_path)| {
                // Match if the file path starts with the repo path prefix
                // (with a directory separator boundary).
                let prefix = repo_path.trim_end_matches('/');
                file_path == prefix || file_path.starts_with(&format!("{prefix}/"))
            })
            .max_by_key(|(_, repo_path)| repo_path.trim_end_matches('/').len());

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

/// Mark the artifact root as Quarantined on error.
fn quarantine_on_error(artifact_root: &ReviewArtifactRoot) {
    if let Err(e) = write_marker_quarantined(artifact_root) {
        tracing::warn!(
            error = %e,
            root = %artifact_root.root.display(),
            run_id = %artifact_root.run_id,
            "Failed to mark the pr-review artifact as Quarantined after an error.",
        );
    }
}

/// Best-effort quarantine for an artifact root identified only by path.
///
/// Returns `true` when the on-disk marker was successfully transitioned to
/// [`ReviewStatus::Quarantined`], `false` otherwise (marker unreadable,
/// transition rejected, or write failed). Callers use the return value to
/// keep user-facing log messages truthful — claiming the marker was moved
/// when it was not is worse than logging the failure.
fn quarantine_artifact_path(artifact_root_path: &Path) -> bool {
    let marker_path = artifact_root_path.join(MARKER_FILENAME);
    let marker = match read_marker(&marker_path) {
        Ok(marker) => marker,
        Err(e) => {
            tracing::warn!(
                error = %e,
                path = %marker_path.display(),
                "Failed to read the pr-review artifact marker before quarantine.",
            );
            return false;
        }
    };

    let artifact_root = ReviewArtifactRoot::from_existing(
        artifact_root_path.to_path_buf(),
        marker.workspace_root,
        marker.run_id,
        marker.workspace_hash,
    );
    match write_marker_quarantined(&artifact_root) {
        Ok(()) => true,
        Err(e) => {
            tracing::warn!(
                error = %e,
                root = %artifact_root.root.display(),
                run_id = %artifact_root.run_id,
                "Failed to mark the pr-review artifact as Quarantined after worktree cleanup failed.",
            );
            false
        }
    }
}

// ─── pr-review clean ──────────────────────────────────────────────────────────

/// A review artifact discovered in the cache root.
#[derive(Debug, Clone)]
pub struct DiscoveredArtifact {
    pub root: PathBuf,
    pub marker: crate::pr_review::artifact_root::ReviewMarker,
    pub size_bytes: u64,
}

/// Scan `default_cache_root(workspace_root)` for subdirectories that contain a
/// valid `review-marker.json` whose `workspace_hash` matches the current
/// workspace.  Returns one entry per discovered artifact root.
pub fn list_review_artifacts(
    workspace_root: &Path,
    cache_root: &Path,
) -> Result<Vec<DiscoveredArtifact>> {
    let current_hash = workspace_hash(workspace_root);
    let hash_dir = cache_root.join(&current_hash);

    if !hash_dir.is_dir() {
        return Ok(vec![]);
    }

    let mut artifacts = Vec::new();

    let entries = std::fs::read_dir(&hash_dir).with_context(|| {
        format!(
            "Reading the review cache directory `{}`.",
            hash_dir.display()
        )
    })?;

    for entry in entries {
        let entry =
            entry.with_context(|| format!("Reading an entry in `{}`.", hash_dir.display()))?;
        let root = entry.path();

        if !root.is_dir() {
            continue;
        }

        let marker_path = root.join(MARKER_FILENAME);
        match read_marker(&marker_path) {
            Ok(marker) => {
                if marker.workspace_hash != current_hash {
                    tracing::warn!(
                        "Skipping `{}` because the workspace_hash does not match. Expected `{}`, got `{}`.",
                        root.display(),
                        current_hash,
                        marker.workspace_hash,
                    );
                    continue;
                }
                let size_bytes = dir_size_bytes(&root);
                artifacts.push(DiscoveredArtifact {
                    root,
                    marker,
                    size_bytes,
                });
            }
            Err(e) => {
                tracing::warn!(
                    "Skipping `{}` because the marker could not be read: {e}.",
                    root.display()
                );
            }
        }
    }

    Ok(artifacts)
}

/// Recursively sum the sizes of all files under `dir`.  Ignores I/O errors
/// (treats unreadable entries as zero bytes).
fn dir_size_bytes(dir: &Path) -> u64 {
    // Bounded depth + skip symlinks: a symlink loop or a symlink pointing at
    // `/` inside the cache root would otherwise blow the stack or report
    // wildly inflated sizes. The cache directory should never legitimately
    // exceed this depth (workspace_hash / run_id / storage / per-table dirs).
    const MAX_DEPTH: u32 = 16;
    fn inner(dir: &Path, depth: u32) -> u64 {
        if depth >= MAX_DEPTH {
            return 0;
        }
        let mut total = 0u64;
        let Ok(entries) = std::fs::read_dir(dir) else {
            return 0;
        };
        for entry in entries.flatten() {
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_symlink() {
                continue;
            }
            if file_type.is_dir() {
                total = total.saturating_add(inner(&entry.path(), depth + 1));
            } else if let Ok(meta) = entry.metadata() {
                total = total.saturating_add(meta.len());
            }
        }
        total
    }
    inner(dir, 0)
}

/// Parse a duration string like `7d`, `1w`, `12h`, `30m`, `60s` into a
/// `std::time::Duration`.
pub fn parse_duration(s: &str) -> Result<std::time::Duration> {
    let s = s.trim();
    if s.is_empty() {
        bail!("Duration string is empty.");
    }

    let (num_str, unit) = s.split_at(s.len() - 1);
    let n: u64 = num_str.parse().with_context(|| {
        format!("Invalid duration `{s}`. Expected `<n><unit>`, where unit is s, m, h, d, or w.")
    })?;

    let secs = match unit {
        "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        "d" => n * 86_400,
        "w" => n * 7 * 86_400,
        other => bail!("Invalid duration unit `{other}` in `{s}`. Use s, m, h, d, or w."),
    };

    Ok(std::time::Duration::from_secs(secs))
}

// ─── JSON output schema ────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct CleanOutput {
    operation: &'static str,
    dry_run: bool,
    selected_artifacts: Vec<CleanArtifactEntry>,
    skipped_baseline_paths: Vec<String>,
}

#[derive(Debug, Serialize)]
struct CleanArtifactEntry {
    run_id: String,
    root: String,
    size_bytes: u64,
    deleted: bool,
}

/// Read the bytes of `gather-step.config.yaml` as committed at `sha` in
/// `workspace_root`'s git repo via `git show <sha>:gather-step.config.yaml`.
///
/// Returns empty bytes when the file is not committed at that ref or git
/// fails for any reason. The cache-key fingerprint is best-effort here:
/// hashing empty bytes when the file is absent is the same outcome as
/// hashing an empty file, which is the right "no config" semantics.
fn read_config_at_sha(workspace_root: &Path, sha: &str) -> Vec<u8> {
    let spec = format!("{sha}:gather-step.config.yaml");
    match std::process::Command::new("git")
        .arg("-C")
        .arg(workspace_root)
        .args(["show", &spec])
        .output()
    {
        Ok(out) if out.status.success() => out.stdout,
        Ok(out) => {
            tracing::debug!(
                sha = %sha,
                stderr = %String::from_utf8_lossy(&out.stderr).trim(),
                "Git could not read gather-step.config.yaml at the requested SHA. Using empty bytes for the cache key.",
            );
            Vec::new()
        }
        Err(e) => {
            tracing::debug!(
                error = %e,
                sha = %sha,
                "Git show failed. Using empty bytes for the cache key.",
            );
            Vec::new()
        }
    }
}

// ─── Safety guard ─────────────────────────────────────────────────────────────

/// Verify that the artifact's review paths do not overlap the workspace's
/// baseline storage, registry, or root.
///
/// Delegates to [`crate::storage_context::validate_review_paths_disjoint`] —
/// the same canonicalized check used at artifact-creation time
/// ([`crate::storage_context::StorageContext::review_checked`]) — so the
/// creation and deletion guards cannot disagree about which layouts are safe.
///
/// Uses the marker's stored `storage_path` / `registry_path` rather than
/// inferring them from `artifact_root`, so artifacts created with
/// non-default sub-layouts are still validated against the paths the marker
/// actually claims to own.
fn assert_not_baseline_overlap(artifact: &DiscoveredArtifact, workspace_root: &Path) -> Result<()> {
    let baseline_registry = workspace_root.join(".gather-step").join("registry.json");
    let baseline_storage = workspace_root.join(".gather-step").join("storage");
    crate::storage_context::validate_review_paths_disjoint(
        workspace_root,
        &baseline_registry,
        &baseline_storage,
        &artifact.root,
        &artifact.marker.registry_path,
        &artifact.marker.storage_path,
    )
    .map_err(|e| {
        anyhow::anyhow!(
            "artifact at `{}` overlaps baseline state; refusing deletion: {e}",
            artifact.root.display()
        )
    })
}

/// Returns `true` when `s` looks like a full 40-character hex SHA-1.
///
/// Used to decide whether an unresolved `--base`/`--head` input should be
/// silently treated as a literal SHA (safe bypass) or surfaced as an error
/// (likely a typo'd ref name). Accepts uppercase, lowercase, or mixed-case
/// hex; downstream comparisons against stored marker SHAs use
/// [`str::eq_ignore_ascii_case`].
fn is_full_sha(s: &str) -> bool {
    s.len() == 40 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Selector kind passed into [`delete_artifact`] so it can refuse to delete
/// `InProgress` artifacts when the user's selector did not explicitly opt in
/// (e.g. `--older-than`). Explicit selectors (`--all`, `--run-id`,
/// `--base`/`--head`) accept `InProgress` markers because the user named the
/// artifact directly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeleteSelector {
    /// Time-based pruning. Refuses `InProgress` unless `include_active` is true.
    OlderThan { include_active: bool },
    /// User explicitly named the artifact via `--all`, `--run-id`, or
    /// `--base`/`--head`. `InProgress` markers are deletable.
    Explicit,
}

/// Delete a single artifact root, enforcing all safety guards.
///
/// 1. Re-reads the marker (refuses if missing/unparsable).
/// 2. Checks `workspace_hash` matches.
/// 3. Re-checks `InProgress` for time-based selectors (TOCTOU defense vs a
///    concurrent run that flipped state between discovery and delete).
/// 4. Checks no overlap with baseline storage/registry.
/// 5. Attempts worktree removal, then `remove_dir_all`.
///
/// In dry-run mode, logs what would be removed but performs no deletion.
///
/// Test-only thin wrapper around [`delete_artifact_with_selector`] with a
/// fixed [`DeleteSelector::Explicit`]. Production callers always pass an
/// explicit selector so the `InProgress` / TOCTOU re-check matches the
/// user's intent precisely.
#[cfg(test)]
pub(crate) fn delete_artifact(
    artifact: &DiscoveredArtifact,
    workspace_root: &Path,
    dry_run: bool,
) -> Result<()> {
    delete_artifact_with_selector(artifact, workspace_root, dry_run, DeleteSelector::Explicit)
}

pub(crate) fn delete_artifact_with_selector(
    artifact: &DiscoveredArtifact,
    workspace_root: &Path,
    dry_run: bool,
    selector: DeleteSelector,
) -> Result<()> {
    // Step 1: re-read the marker.
    let marker_path = artifact.root.join(MARKER_FILENAME);
    let marker = read_marker(&marker_path).with_context(|| {
        format!(
            "Re-reading the marker for artifact at `{}`.",
            artifact.root.display()
        )
    })?;

    // Step 2: workspace hash check.
    let current_hash = workspace_hash(workspace_root);
    if marker.workspace_hash != current_hash {
        bail!(
            "Refusing to delete `{}` because the workspace_hash in the marker (`{}`) does not match \
             the current workspace hash (`{}`).",
            artifact.root.display(),
            marker.workspace_hash,
            current_hash,
        );
    }

    // Step 3: TOCTOU defense for time-based pruning. A concurrent `pr-review`
    // run may have flipped the marker state between discovery and the read
    // above; refuse to delete InProgress artifacts unless the user opted in.
    // `--all`, `--run-id`, and `--base/--head` are explicit selectors and
    // accept InProgress markers because the user named the artifact directly.
    if marker.status == ReviewStatus::InProgress
        && matches!(
            selector,
            DeleteSelector::OlderThan {
                include_active: false,
            }
        )
    {
        bail!(
            "Skipping `{}` because the artifact is InProgress and the selector did not opt in with `--include-active`.",
            artifact.root.display(),
        );
    }

    // Step 4: no overlap with baseline paths (canonicalized — same check as
    // creation-side `StorageContext::review_checked`).
    assert_not_baseline_overlap(artifact, workspace_root).with_context(|| {
        format!(
            "Running the safety check for `{}`.",
            artifact.root.display()
        )
    })?;

    if dry_run {
        #[expect(clippy::print_stdout, reason = "clean command progress output")]
        {
            println!(
                "would remove {} ({} bytes)",
                artifact.root.display(),
                artifact.size_bytes,
            );
        }
        return Ok(());
    }

    // Step 4: remove worktree first; only nuke the artifact dir if the
    // worktree removed cleanly, otherwise we'd leave a dangling pointer in
    // `<workspace>/.git/worktrees/<run-id>` referencing a now-missing path.
    let worktree_root = artifact.root.join("worktree");
    let mut worktree_removed_cleanly = true;
    if worktree_root.is_dir() {
        let wt = ReviewWorktree {
            repo: workspace_root.to_path_buf(),
            root: worktree_root,
            sha: marker.head_sha.clone(),
        };
        if let Err(e) = remove_worktree(&wt) {
            worktree_removed_cleanly = false;
            tracing::warn!(
                error = %e,
                worktree = %wt.root.display(),
                "pr-review clean: Failed to remove the worktree. \
                 Leaving the artifact directory in place to avoid a dangling pointer in `.git/worktrees/`.",
            );
        }
    }

    if !worktree_removed_cleanly {
        bail!(
            "Refusing to remove `{}` because worktree removal failed. \
             Stop any running Git tooling on that worktree, run `git worktree prune`, then retry.",
            artifact.root.display(),
        );
    }

    std::fs::remove_dir_all(&artifact.root)
        .with_context(|| format!("Removing artifact root `{}`.", artifact.root.display()))?;

    #[expect(clippy::print_stdout, reason = "clean command progress output")]
    {
        println!(
            "removed {} ({} bytes)",
            artifact.root.display(),
            artifact.size_bytes,
        );
    }

    Ok(())
}

/// Handle `gather-step pr-review clean ...`.
fn run_clean(app: &AppContext, top: &PrReviewArgs, args: &CleanArgs) -> Result<()> {
    // Exactly one selector must be given.
    let selectors = [
        args.run_id.is_some(),
        args.base.is_some() || args.head.is_some(),
        args.older_than.is_some(),
        args.all,
    ];
    let selector_count = selectors.iter().filter(|&&v| v).count();

    if selector_count == 0 {
        bail!(
            "pr-review clean requires exactly one selector: \
             --run-id <ID>, --base <REF> --head <REF>, --older-than <DURATION>, or --all."
        );
    }
    if selector_count > 1 {
        bail!(
            "pr-review clean accepts only one selector at a time. \
             Combine --dry-run with any selector to preview."
        );
    }

    // Validate --base/--head: both or neither.
    match (&args.base, &args.head) {
        (Some(_), None) | (None, Some(_)) => {
            bail!("The --base and --head flags must be specified together.");
        }
        _ => {}
    }

    let cache_root = top
        .cache_root
        .clone()
        .unwrap_or_else(|| default_cache_root(&app.workspace_path));

    let emit_json = top.json || app.json_output;

    // Discover all review artifacts for this workspace.
    let all_artifacts = list_review_artifacts(&app.workspace_path, &cache_root)
        .context("Discovering review artifacts.")?;

    // Baseline paths we will never touch.
    let ws_paths = app.workspace_paths();
    let baseline_paths = vec![
        ws_paths.registry_path.display().to_string(),
        ws_paths.storage_root.display().to_string(),
    ];

    // Select artifacts according to the chosen selector.
    let selected: Vec<DiscoveredArtifact> = if args.all {
        // Print baseline banner before any deletions.
        if !emit_json {
            #[expect(clippy::print_stdout, reason = "clean --all baseline safety banner")]
            {
                println!("Will not touch baseline paths:");
                println!("  registry: {}", ws_paths.registry_path.display());
                println!("  storage:  {}", ws_paths.storage_root.display());
            }
        }
        all_artifacts
    } else if let Some(ref run_id) = args.run_id {
        all_artifacts
            .into_iter()
            .filter(|a| &a.marker.run_id == run_id)
            .collect()
    } else if let (Some(base_ref), Some(head_ref)) = (&args.base, &args.head) {
        // Resolve refs against the real workspace so we can compare to stored SHAs.
        // For test paths, literal full SHAs are also accepted when resolution fails.
        let (base_sha, head_sha) = match resolve_range(&app.workspace_path, base_ref, head_ref) {
            Ok(resolved) => (resolved.base.sha, resolved.head.sha),
            Err(e) => {
                // Only allow fallback when both inputs look like full 40-char SHAs.
                // Any other shape means the user likely typo'd a ref name, so surface
                // the error rather than silently matching nothing.
                if is_full_sha(base_ref) && is_full_sha(head_ref) {
                    (base_ref.clone(), head_ref.clone())
                } else {
                    return Err(e).context(format!(
                        "Could not resolve --base {base_ref:?} or --head {head_ref:?} \
                         against workspace at {}. Pass full 40-character SHAs to bypass resolution.",
                        app.workspace_path.display()
                    ));
                }
            }
        };
        all_artifacts
            .into_iter()
            .filter(|a| {
                a.marker.base_sha.eq_ignore_ascii_case(&base_sha)
                    && a.marker.head_sha.eq_ignore_ascii_case(&head_sha)
            })
            .collect()
    } else if let Some(ref duration_str) = args.older_than {
        let max_age = parse_duration(duration_str)
            .with_context(|| format!("Parsing --older-than `{duration_str}`."))?;
        let now = std::time::SystemTime::now();
        all_artifacts
            .into_iter()
            .filter(|a| {
                // Never delete an active run — it may still be indexing.
                if matches!(a.marker.status, ReviewStatus::InProgress) {
                    return false;
                }
                // Unless --include-active is set, skip artifacts whose cache
                // key is still active (both SHAs resolvable in this workspace).
                // The user might need this artifact for the next review run.
                if !args.include_active
                    && a.marker
                        .cache_key
                        .as_ref()
                        .is_some_and(|key| is_cache_key_active(&app.workspace_path, key))
                {
                    tracing::debug!(
                        run_id = %a.marker.run_id,
                        "Skipping the active cache key. Use --include-active to override."
                    );
                    return false;
                }
                // Compute age. Prefer `last_accessed_at` (refreshed on every
                // cache hit) over `created_at` so artifacts that are still
                // being reused don't age out from under an active workflow.
                // If `last_accessed_at` is absent (older marker, or no hit
                // yet), fall back to `created_at`.
                let age_anchor = a
                    .marker
                    .last_accessed_at
                    .as_deref()
                    .unwrap_or(&a.marker.created_at);
                chrono::DateTime::parse_from_rfc3339(age_anchor)
                    .ok()
                    .is_some_and(|dt| {
                        let artifact_time = std::time::SystemTime::UNIX_EPOCH
                            + std::time::Duration::from_secs(dt.timestamp().max(0).cast_unsigned());
                        now.duration_since(artifact_time)
                            .is_ok_and(|age| age >= max_age)
                    })
            })
            .collect()
    } else {
        unreachable!("selector_count == 1 guarantees one branch is taken")
    };

    // Determine the per-call selector kind so `delete_artifact_with_selector`
    // can refuse to remove markers that flipped to InProgress between
    // discovery and the per-artifact re-read (TOCTOU defense).
    let selector_kind = if args.older_than.is_some() {
        DeleteSelector::OlderThan {
            include_active: args.include_active,
        }
    } else {
        DeleteSelector::Explicit
    };

    // Execute or preview deletions.
    let mut entries: Vec<CleanArtifactEntry> = Vec::with_capacity(selected.len());
    let mut had_error = false;

    for artifact in &selected {
        let was_dry = args.dry_run;
        match delete_artifact_with_selector(
            artifact,
            &app.workspace_path,
            args.dry_run,
            selector_kind,
        ) {
            Ok(()) => {
                entries.push(CleanArtifactEntry {
                    run_id: artifact.marker.run_id.clone(),
                    root: artifact.root.display().to_string(),
                    size_bytes: artifact.size_bytes,
                    deleted: !was_dry,
                });
            }
            Err(e) => {
                tracing::error!("Failed to process `{}`: {e:#}.", artifact.root.display());
                had_error = true;
            }
        }
    }

    if emit_json {
        let output = CleanOutput {
            operation: "clean",
            dry_run: args.dry_run,
            selected_artifacts: entries,
            skipped_baseline_paths: baseline_paths,
        };
        let json =
            serde_json::to_string_pretty(&output).context("Serializing clean output to JSON.")?;
        #[expect(clippy::print_stdout, reason = "clean JSON output goes to stdout")]
        {
            println!("{json}");
        }
    }

    if had_error {
        bail!("One or more artifacts could not be cleaned. See the errors above.");
    }

    Ok(())
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::{fs, path::Path, process::Command};

    use super::*;
    use crate::{
        app::AppContext,
        pr_review::{
            artifact_root::{
                MARKER_FILENAME, MARKER_SCHEMA_VERSION, ReviewMarker, ReviewStatus, workspace_hash,
            },
            test_helpers::TempDir,
        },
    };
    use gather_step_core::{GatherStepConfig, RegistryStore};
    use gather_step_storage::{IndexingOptions, index_workspace_with_storage};

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
        fs::write(
            ws.join("myrepo/package.json"),
            r#"{"name":"myrepo","version":"0.0.1"}"#,
        )
        .unwrap();
        fs::write(
            src.join("hello.ts"),
            "export function greet(): string { return 'hello'; }\n",
        )
        .unwrap();

        // Init git and make base commit
        git_run(&ws, &["init", "--initial-branch=main"]);
        git_run(&ws, &["config", "user.email", "test@example.com"]);
        git_run(&ws, &["config", "user.name", "Test"]);
        git_run(&ws, &["config", "commit.gpgsign", "false"]);
        git_run(&ws, &["config", "tag.gpgsign", "false"]);
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

    /// Returns `(workspace_path, advanced_base_sha, feature_head_sha)` where
    /// `base` and `head` have diverged from their merge-base.
    fn build_diverged_fixture(root: &Path) -> (PathBuf, String, String) {
        let ws = root.to_path_buf();

        fs::write(
            ws.join("gather-step.config.yaml"),
            "repos:\n  - name: myrepo\n    path: myrepo\nindexing:\n  workspace_concurrency: 1\n",
        )
        .unwrap();

        let src = ws.join("myrepo/src");
        fs::create_dir_all(&src).unwrap();
        fs::write(
            ws.join("myrepo/package.json"),
            r#"{"name":"myrepo","version":"0.0.1"}"#,
        )
        .unwrap();
        fs::write(
            src.join("hello.ts"),
            "export function greet(): string { return 'hello'; }\n",
        )
        .unwrap();

        git_run(&ws, &["init", "--initial-branch=main"]);
        git_run(&ws, &["config", "user.email", "test@example.com"]);
        git_run(&ws, &["config", "user.name", "Test"]);
        git_run(&ws, &["config", "commit.gpgsign", "false"]);
        git_run(&ws, &["config", "tag.gpgsign", "false"]);
        git_run(&ws, &["add", "."]);
        git_run(&ws, &["commit", "--message", "base"]);

        git_run(&ws, &["checkout", "-b", "feature/diverged"]);
        fs::write(
            src.join("feature_only.ts"),
            "export function featureOnly(): string { return 'feature'; }\n",
        )
        .unwrap();
        git_run(&ws, &["add", "."]);
        git_run(&ws, &["commit", "--message", "head: add feature file"]);
        let head_sha = git_head_sha(&ws);

        git_run(&ws, &["checkout", "main"]);
        fs::write(
            src.join("main_only.ts"),
            "export function mainOnly(): string { return 'main'; }\n",
        )
        .unwrap();
        git_run(&ws, &["add", "."]);
        git_run(&ws, &["commit", "--message", "base: add main file"]);
        let base_sha = git_head_sha(&ws);

        (ws, base_sha, head_sha)
    }

    /// Returns a fixture whose PR changes `gather-step.config.yaml` so the
    /// head-only repo config must drive report metadata.
    fn build_head_config_fixture(root: &Path) -> (PathBuf, String, String) {
        let ws = root.to_path_buf();

        fs::write(
            ws.join("gather-step.config.yaml"),
            "repos:\n  - name: oldrepo\n    path: oldrepo\nindexing:\n  workspace_concurrency: 1\n",
        )
        .unwrap();
        let old_src = ws.join("oldrepo/src");
        fs::create_dir_all(&old_src).unwrap();
        fs::write(
            ws.join("oldrepo/package.json"),
            r#"{"name":"oldrepo","version":"0.0.1"}"#,
        )
        .unwrap();
        fs::write(
            old_src.join("hello.ts"),
            "export function oldHello(): string { return 'old'; }\n",
        )
        .unwrap();

        git_run(&ws, &["init", "--initial-branch=main"]);
        git_run(&ws, &["config", "user.email", "test@example.com"]);
        git_run(&ws, &["config", "user.name", "Test"]);
        git_run(&ws, &["config", "commit.gpgsign", "false"]);
        git_run(&ws, &["config", "tag.gpgsign", "false"]);
        git_run(&ws, &["add", "."]);
        git_run(&ws, &["commit", "--message", "base"]);
        let base_sha = git_head_sha(&ws);

        git_run(&ws, &["checkout", "-b", "feature/head-config"]);
        fs::write(
            ws.join("gather-step.config.yaml"),
            "repos:\n  - name: newrepo\n    path: newrepo\nindexing:\n  workspace_concurrency: 1\n",
        )
        .unwrap();
        let new_src = ws.join("newrepo/src");
        fs::create_dir_all(&new_src).unwrap();
        fs::write(
            ws.join("newrepo/package.json"),
            r#"{"name":"newrepo","version":"0.0.1"}"#,
        )
        .unwrap();
        fs::write(
            new_src.join("added.ts"),
            "export function newAdded(): string { return 'new'; }\n",
        )
        .unwrap();
        git_run(&ws, &["add", "."]);
        git_run(
            &ws,
            &["commit", "--message", "head: switch configured repo"],
        );
        let head_sha = git_head_sha(&ws);

        git_run(&ws, &["checkout", "main"]);

        (ws, base_sha, head_sha)
    }

    /// Returns a fixture whose PR changes both Python code and deployment
    /// topology. The baseline is left checked out so callers can build the
    /// normal workspace index before running `pr-review` against the head SHA.
    fn build_python_deployment_fixture(root: &Path) -> (PathBuf, String, String) {
        let ws = root.to_path_buf();

        fs::write(
            ws.join("gather-step.config.yaml"),
            "repos:\n  - name: pyservice\n    path: pyservice\nindexing:\n  workspace_concurrency: 1\n  include_languages:\n    - python\n",
        )
        .unwrap();

        let package = ws.join("pyservice/src/pyservice");
        fs::create_dir_all(&package).unwrap();
        fs::write(
            ws.join("pyservice/pyproject.toml"),
            "[project]\nname = \"pyservice\"\nversion = \"0.0.1\"\n",
        )
        .unwrap();
        fs::write(package.join("__init__.py"), "").unwrap();
        fs::write(
            package.join("app.py"),
            "def existing_handler() -> str:\n    return \"ok\"\n",
        )
        .unwrap();

        git_run(&ws, &["init", "--initial-branch=main"]);
        git_run(&ws, &["config", "user.email", "test@example.com"]);
        git_run(&ws, &["config", "user.name", "Test"]);
        git_run(&ws, &["config", "commit.gpgsign", "false"]);
        git_run(&ws, &["config", "tag.gpgsign", "false"]);
        git_run(&ws, &["add", "."]);
        git_run(&ws, &["commit", "--message", "base"]);
        let base_sha = git_head_sha(&ws);

        git_run(&ws, &["checkout", "-b", "feature/python-deployment"]);
        fs::write(
            package.join("app.py"),
            "def existing_handler() -> str:\n    return \"ok\"\n\n\ndef rollout_handler(event: dict[str, str]) -> dict[str, str]:\n    return {\"status\": event.get(\"id\", \"ok\")}\n",
        )
        .unwrap();
        fs::write(
            ws.join("pyservice/Dockerfile"),
            "FROM python:3.12-slim\nENV FEATURE_FLAG=enabled\n",
        )
        .unwrap();
        git_run(&ws, &["add", "."]);
        git_run(
            &ws,
            &[
                "commit",
                "--message",
                "head: add python rollout and deployment",
            ],
        );
        let head_sha = git_head_sha(&ws);

        git_run(&ws, &["checkout", "main"]);

        (ws, base_sha, head_sha)
    }

    fn index_baseline_workspace(workspace: &Path) {
        let config_path = workspace.join("gather-step.config.yaml");
        let config = GatherStepConfig::from_yaml_file(&config_path)
            .expect("The baseline fixture config should load.");
        config
            .validate_repo_roots_against_config_root(workspace)
            .expect("The baseline fixture repo roots should validate.");

        let gs_dir = workspace.join(".gather-step");
        fs::create_dir_all(&gs_dir).expect("The generated-state directory should exist.");
        let registry_path = gs_dir.join("registry.json");
        let storage_root = gs_dir.join("storage");
        let mut registry = RegistryStore::open(&registry_path).expect("The registry should open.");

        index_workspace_with_storage(
            &config,
            workspace,
            &mut registry,
            &storage_root,
            IndexingOptions::default(),
        )
        .expect("The baseline fixture should index.");
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

    // ── Helper: write a fake artifact root with a given marker ────────────────

    fn write_fake_artifact(
        cache_root: &Path,
        workspace_root: &Path,
        run_id: &str,
        base_sha: &str,
        head_sha: &str,
        status: ReviewStatus,
        created_at_override: Option<&str>,
    ) -> PathBuf {
        let hash = workspace_hash(workspace_root);
        let root = cache_root.join(&hash).join(run_id);
        fs::create_dir_all(&root).expect("create fake artifact root");

        let marker_path = root.join(MARKER_FILENAME);
        let storage_path = root.join("storage");
        let registry_path = root.join("registry.json");
        fs::create_dir_all(&storage_path).unwrap();
        // Write a small file so size_bytes > 0.
        fs::write(storage_path.join("dummy.txt"), b"data").unwrap();

        let created_at =
            created_at_override.map_or_else(|| chrono::Utc::now().to_rfc3339(), ToOwned::to_owned);

        let marker = ReviewMarker {
            schema_version: MARKER_SCHEMA_VERSION,
            workspace_hash: hash,
            workspace_root: workspace_root.to_path_buf(),
            base_sha: base_sha.to_owned(),
            head_sha: head_sha.to_owned(),
            run_id: run_id.to_owned(),
            storage_path,
            registry_path,
            gather_step_version: env!("CARGO_PKG_VERSION").to_owned(),
            created_at,
            status,
            cache_key: None,
            last_accessed_at: None,
        };

        let json = serde_json::to_vec_pretty(&marker).expect("serialize marker");
        fs::write(&marker_path, json).expect("write marker");

        root
    }

    #[test]
    fn cleanup_guard_with_keep_cache_does_not_touch_disk_on_drop() {
        let ws_tmp = TempDir::new("keep-cache-ws");
        let cache_tmp = TempDir::new("keep-cache-cache");
        let artifact_root = write_fake_artifact(
            cache_tmp.path(),
            ws_tmp.path(),
            "review-keep-cache",
            "baseKC",
            "headKC",
            ReviewStatus::InProgress,
            None,
        );

        let mut guard = ReviewCleanupGuard::new(CacheRetention::Keep);
        guard.arm(artifact_root.clone(), None);
        drop(guard);

        assert!(
            artifact_root.exists(),
            "The cleanup guard must not remove artifacts when cache retention is Keep."
        );
        let marker = read_marker(&artifact_root.join(MARKER_FILENAME)).unwrap();
        assert_eq!(marker.status, ReviewStatus::InProgress);
    }

    #[test]
    fn cleanup_guard_quarantines_artifact_when_worktree_remove_fails() {
        let ws_tmp = TempDir::new("drop-worktree-fail-ws");
        let cache_tmp = TempDir::new("drop-worktree-fail-cache");
        let artifact_root = write_fake_artifact(
            cache_tmp.path(),
            ws_tmp.path(),
            "review-drop-worktree-fail",
            "baseWF",
            "headWF",
            ReviewStatus::InProgress,
            None,
        );
        let worktree = ReviewWorktree {
            repo: ws_tmp.path().to_path_buf(),
            root: artifact_root.join("worktree"),
            sha: "0000000000000000000000000000000000000000".to_owned(),
        };

        let mut guard = ReviewCleanupGuard::new(CacheRetention::Discard);
        guard.arm(artifact_root.clone(), Some(worktree));
        drop(guard);

        assert!(
            artifact_root.exists(),
            "The artifact root must stay on disk when worktree removal fails."
        );
        let marker = read_marker(&artifact_root.join(MARKER_FILENAME)).unwrap();
        assert_eq!(marker.status, ReviewStatus::Quarantined);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 1: dry_run_lists_artifacts_and_deletes_nothing
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn dry_run_lists_artifacts_and_deletes_nothing() {
        let ws_tmp = TempDir::new("dry-ws");
        let cache_tmp = TempDir::new("dry-cache");
        let ws = ws_tmp.path();
        let cache = cache_tmp.path();

        let root1 = write_fake_artifact(
            cache,
            ws,
            "review-dry-run-1",
            "base000",
            "head000",
            ReviewStatus::Completed,
            None,
        );
        let root2 = write_fake_artifact(
            cache,
            ws,
            "review-dry-run-2",
            "base111",
            "head111",
            ReviewStatus::Completed,
            None,
        );

        let app = make_app(ws);
        let top = PrReviewArgs {
            command: None,
            base: None,
            head: None,
            engine: ReviewEngine::TempIndex,
            keep_cache: false,
            json: false,
            cache_root: Some(cache.to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };
        let clean_args = CleanArgs {
            dry_run: true,
            run_id: None,
            base: None,
            head: None,
            older_than: None,
            include_active: false,
            all: true,
        };

        run_clean(&app, &top, &clean_args).expect("dry-run clean should succeed");

        // Both artifact roots must still exist.
        assert!(root1.exists(), "root1 must not be deleted in dry-run");
        assert!(root2.exists(), "root2 must not be deleted in dry-run");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 2: run_id_deletes_only_matching_run
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn run_id_deletes_only_matching_run() {
        let ws_tmp = TempDir::new("runid-ws");
        let cache_tmp = TempDir::new("runid-cache");
        let ws = ws_tmp.path();
        let cache = cache_tmp.path();

        let root1 = write_fake_artifact(
            cache,
            ws,
            "review-target-run",
            "baseA",
            "headA",
            ReviewStatus::Completed,
            None,
        );
        let root2 = write_fake_artifact(
            cache,
            ws,
            "review-other-run",
            "baseB",
            "headB",
            ReviewStatus::Completed,
            None,
        );

        let app = make_app(ws);
        let top = PrReviewArgs {
            command: None,
            base: None,
            head: None,
            engine: ReviewEngine::TempIndex,
            keep_cache: false,
            json: false,
            cache_root: Some(cache.to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };
        let clean_args = CleanArgs {
            dry_run: false,
            run_id: Some("review-target-run".to_owned()),
            base: None,
            head: None,
            older_than: None,
            include_active: false,
            all: false,
        };

        run_clean(&app, &top, &clean_args).expect("clean by run_id should succeed");

        assert!(!root1.exists(), "matching artifact should be deleted");
        assert!(root2.exists(), "non-matching artifact must remain");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 3: base_head_deletes_matching_pair
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn base_head_deletes_matching_pair() {
        let ws_tmp = TempDir::new("bh-ws");
        let cache_tmp = TempDir::new("bh-cache");
        let ws = ws_tmp.path();
        let cache = cache_tmp.path();

        let sha_base = "aaaa1111bbbb2222cccc3333dddd4444eeee5555";
        let sha_head = "ffff6666aaaa7777bbbb8888cccc9999dddd0000";
        let sha_base2 = "1111aaaa2222bbbb3333cccc4444dddd5555eeee";
        let sha_head2 = "6666ffff7777aaaa8888bbbb9999cccc0000dddd";

        let root1 = write_fake_artifact(
            cache,
            ws,
            "review-bh-match",
            sha_base,
            sha_head,
            ReviewStatus::Completed,
            None,
        );
        let root2 = write_fake_artifact(
            cache,
            ws,
            "review-bh-other",
            sha_base2,
            sha_head2,
            ReviewStatus::Completed,
            None,
        );

        let app = make_app(ws);
        let top = PrReviewArgs {
            command: None,
            base: None,
            head: None,
            engine: ReviewEngine::TempIndex,
            keep_cache: false,
            json: false,
            cache_root: Some(cache.to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };
        let clean_args = CleanArgs {
            dry_run: false,
            run_id: None,
            // Pass literal SHAs so no git resolution is needed.
            base: Some(sha_base.to_owned()),
            head: Some(sha_head.to_owned()),
            older_than: None,
            include_active: false,
            all: false,
        };

        run_clean(&app, &top, &clean_args).expect("clean by base/head should succeed");

        assert!(!root1.exists(), "matching artifact should be deleted");
        assert!(root2.exists(), "non-matching artifact must remain");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 4: older_than_deletes_only_old_artifacts
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn older_than_deletes_only_old_artifacts() {
        let ws_tmp = TempDir::new("age-ws");
        let cache_tmp = TempDir::new("age-cache");
        let ws = ws_tmp.path();
        let cache = cache_tmp.path();

        // Old artifact: backdated 2 days ago.
        let old_ts = (chrono::Utc::now() - chrono::Duration::days(2)).to_rfc3339();
        let root_old = write_fake_artifact(
            cache,
            ws,
            "review-old-run",
            "baseOLD",
            "headOLD",
            ReviewStatus::Completed,
            Some(&old_ts),
        );
        // Fresh artifact: created now.
        let root_fresh = write_fake_artifact(
            cache,
            ws,
            "review-fresh-run",
            "baseFRESH",
            "headFRESH",
            ReviewStatus::Completed,
            None,
        );

        let app = make_app(ws);
        let top = PrReviewArgs {
            command: None,
            base: None,
            head: None,
            engine: ReviewEngine::TempIndex,
            keep_cache: false,
            json: false,
            cache_root: Some(cache.to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };
        let clean_args = CleanArgs {
            dry_run: false,
            run_id: None,
            base: None,
            head: None,
            older_than: Some("1d".to_owned()),
            include_active: false,
            all: false,
        };

        run_clean(&app, &top, &clean_args).expect("The clean --older-than command should succeed.");

        assert!(!root_old.exists(), "old artifact should be deleted");
        assert!(root_fresh.exists(), "fresh artifact must remain");
    }

    #[test]
    fn older_than_prefers_last_accessed_at_when_present() {
        let ws_tmp = TempDir::new("age-accessed-ws");
        let cache_tmp = TempDir::new("age-accessed-cache");
        let ws = ws_tmp.path();
        let cache = cache_tmp.path();

        let old_ts = (chrono::Utc::now() - chrono::Duration::days(10)).to_rfc3339();
        let recent_ts = chrono::Utc::now().to_rfc3339();
        let root_recent_access = write_fake_artifact(
            cache,
            ws,
            "review-recent-access",
            "baseACCESS",
            "headACCESS",
            ReviewStatus::Completed,
            Some(&old_ts),
        );
        let marker_path = root_recent_access.join(MARKER_FILENAME);
        let mut marker: ReviewMarker =
            serde_json::from_slice(&fs::read(&marker_path).unwrap()).unwrap();
        marker.last_accessed_at = Some(recent_ts);
        fs::write(&marker_path, serde_json::to_vec_pretty(&marker).unwrap()).unwrap();

        let root_legacy_old = write_fake_artifact(
            cache,
            ws,
            "review-legacy-old",
            "baseLEGACY",
            "headLEGACY",
            ReviewStatus::Completed,
            Some(&old_ts),
        );

        let app = make_app(ws);
        let top = PrReviewArgs {
            command: None,
            base: None,
            head: None,
            engine: ReviewEngine::TempIndex,
            keep_cache: false,
            json: false,
            cache_root: Some(cache.to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };
        let clean_args = CleanArgs {
            dry_run: false,
            run_id: None,
            base: None,
            head: None,
            older_than: Some("1d".to_owned()),
            include_active: false,
            all: false,
        };

        run_clean(&app, &top, &clean_args).expect("The clean --older-than command should succeed.");

        assert!(
            root_recent_access.exists(),
            "Recently accessed artifacts must remain even when created_at is old."
        );
        assert!(
            !root_legacy_old.exists(),
            "Legacy artifacts without last_accessed_at should fall back to created_at."
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 5: all_deletes_every_artifact_for_workspace
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn all_deletes_every_artifact_for_workspace() {
        let ws_tmp = TempDir::new("all-ws");
        let cache_tmp = TempDir::new("all-cache");
        let ws = ws_tmp.path();
        let cache = cache_tmp.path();

        let root1 = write_fake_artifact(
            cache,
            ws,
            "review-all-1",
            "baseX",
            "headX",
            ReviewStatus::Completed,
            None,
        );
        let root2 = write_fake_artifact(
            cache,
            ws,
            "review-all-2",
            "baseY",
            "headY",
            ReviewStatus::Quarantined,
            None,
        );

        let app = make_app(ws);
        let top = PrReviewArgs {
            command: None,
            base: None,
            head: None,
            engine: ReviewEngine::TempIndex,
            keep_cache: false,
            json: false,
            cache_root: Some(cache.to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };
        let clean_args = CleanArgs {
            dry_run: false,
            run_id: None,
            base: None,
            head: None,
            older_than: None,
            include_active: false,
            all: true,
        };

        run_clean(&app, &top, &clean_args).expect("clean --all should succeed");

        assert!(!root1.exists(), "artifact 1 should be deleted");
        assert!(!root2.exists(), "artifact 2 should be deleted");

        // The workspace-hash subdirectory should now be empty.
        let hash = workspace_hash(ws);
        let hash_dir = cache.join(hash);
        let remaining: Vec<_> = fs::read_dir(&hash_dir)
            .expect("hash dir should still exist")
            .flatten()
            .collect();
        assert!(remaining.is_empty(), "hash_dir should be empty after --all");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 6: refuses_to_delete_when_workspace_hash_mismatch
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn refuses_to_delete_when_workspace_hash_mismatch() {
        let ws_tmp = TempDir::new("mismatch-ws");
        let cache_tmp = TempDir::new("mismatch-cache");
        let ws = ws_tmp.path();
        let cache = cache_tmp.path();

        // Write a marker under the real workspace_hash directory but with a
        // *different* workspace_hash value inside the JSON — simulates an
        // artifact from another workspace that somehow landed in this tree.
        let real_hash = workspace_hash(ws);
        let run_id = "review-mismatch-run";
        let root = cache.join(&real_hash).join(run_id);
        fs::create_dir_all(&root).unwrap();
        let marker = ReviewMarker {
            schema_version: MARKER_SCHEMA_VERSION,
            workspace_hash: "deadbeefdeadbeef".to_owned(), // wrong hash
            workspace_root: ws.to_path_buf(),
            base_sha: "base".to_owned(),
            head_sha: "head".to_owned(),
            run_id: run_id.to_owned(),
            storage_path: root.join("storage"),
            registry_path: root.join("registry.json"),
            gather_step_version: env!("CARGO_PKG_VERSION").to_owned(),
            created_at: chrono::Utc::now().to_rfc3339(),
            status: ReviewStatus::Completed,
            cache_key: None,
            last_accessed_at: None,
        };
        let json = serde_json::to_vec_pretty(&marker).unwrap();
        fs::write(root.join(MARKER_FILENAME), json).unwrap();

        // list_review_artifacts should skip it (hash mismatch in discovery).
        let artifacts = list_review_artifacts(ws, cache).expect("list should succeed");
        assert!(
            artifacts.is_empty(),
            "mismatch artifact should be skipped during discovery"
        );

        // Even if we manually try delete_artifact it should fail.
        let fake = DiscoveredArtifact {
            root: root.clone(),
            marker,
            size_bytes: 0,
        };
        let result = delete_artifact(&fake, ws, false);
        assert!(
            result.is_err(),
            "delete must fail on workspace_hash mismatch"
        );
        // Root still exists.
        assert!(root.exists(), "mismatch artifact must not be deleted");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 7: refuses_to_delete_paths_overlapping_baseline
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn refuses_to_delete_paths_overlapping_baseline() {
        let ws_tmp = TempDir::new("overlap-ws");
        let ws = ws_tmp.path();

        // Construct a fake artifact whose root IS the baseline storage path.
        let baseline_storage = ws.join(".gather-step").join("storage");
        fs::create_dir_all(&baseline_storage).unwrap();

        let marker = ReviewMarker {
            schema_version: MARKER_SCHEMA_VERSION,
            workspace_hash: workspace_hash(ws),
            workspace_root: ws.to_path_buf(),
            base_sha: "b".to_owned(),
            head_sha: "h".to_owned(),
            run_id: "review-overlap".to_owned(),
            storage_path: baseline_storage.clone(),
            registry_path: ws.join(".gather-step").join("registry.json"),
            gather_step_version: env!("CARGO_PKG_VERSION").to_owned(),
            created_at: chrono::Utc::now().to_rfc3339(),
            status: ReviewStatus::Completed,
            cache_key: None,
            last_accessed_at: None,
        };

        // Write the marker INTO the baseline storage path so re-read works.
        let marker_path = baseline_storage.join(MARKER_FILENAME);
        let json = serde_json::to_vec_pretty(&marker).unwrap();
        fs::write(&marker_path, json).unwrap();

        let fake = DiscoveredArtifact {
            root: baseline_storage.clone(),
            marker,
            size_bytes: 0,
        };

        let result = delete_artifact(&fake, ws, false);
        assert!(
            result.is_err(),
            "must refuse to delete overlapping baseline path"
        );
        assert!(
            baseline_storage.exists(),
            "baseline storage must not be removed"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 8: older_than_parses_common_units
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn older_than_parses_common_units() {
        assert_eq!(parse_duration("60s").unwrap().as_secs(), 60);
        assert_eq!(parse_duration("30m").unwrap().as_secs(), 1800);
        assert_eq!(parse_duration("12h").unwrap().as_secs(), 43_200);
        assert_eq!(parse_duration("7d").unwrap().as_secs(), 7 * 86_400);
        assert_eq!(parse_duration("1w").unwrap().as_secs(), 7 * 86_400);

        // Malformed inputs must fail.
        assert!(parse_duration("").is_err());
        assert!(parse_duration("7x").is_err());
        assert!(parse_duration("nope").is_err());
        assert!(parse_duration("7").is_err());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Existing pr-review run tests (retained)
    // ─────────────────────────────────────────────────────────────────────────

    // ── Test: metadata fields ─────────────────────────────────────────────────

    #[test]
    fn pr_review_emits_metadata_for_simple_pr() {
        if !git_available() {
            return;
        }

        let ws_tmp = TempDir::new("ws1");
        let cache_tmp = TempDir::new("cache1");
        let (ws, base_sha, head_sha) = build_fixture(ws_tmp.path());

        let app = make_app(&ws);
        let args = PrReviewRunArgs {
            base: base_sha.clone(),
            head: head_sha.clone(),
            engine: ReviewEngine::TempIndex,
            keep_cache: true,
            json: true,
            cache_root: Some(cache_tmp.path().to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };

        let (rendered, _) = run_inner(&app, &args).expect("The pr-review run should succeed.");

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

    // ── Test: keep_cache leaves artifact root ─────────────────────────────────

    #[test]
    fn pr_review_keeps_cache_when_flag_set() {
        if !git_available() {
            return;
        }

        let ws_tmp = TempDir::new("ws2");
        let cache_tmp = TempDir::new("cache2");
        let (ws, base_sha, head_sha) = build_fixture(ws_tmp.path());

        let app = make_app(&ws);
        let args = PrReviewRunArgs {
            base: base_sha,
            head: head_sha,
            engine: ReviewEngine::TempIndex,
            keep_cache: true,
            json: true,
            cache_root: Some(cache_tmp.path().to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };

        let (rendered, _) = run_inner(&app, &args).expect("The pr-review run should succeed.");

        let report: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        let review_root = PathBuf::from(report["safety"]["review_root"].as_str().unwrap());

        assert!(
            review_root.exists(),
            "artifact root should still exist after --keep-cache run; got {review_root:?}"
        );
    }

    // ── Test: cleanup removes artifact root ───────────────────────────────────

    #[test]
    fn pr_review_cleans_up_when_flag_unset() {
        if !git_available() {
            return;
        }

        let ws_tmp = TempDir::new("ws3");
        let cache_tmp = TempDir::new("cache3");
        let (ws, base_sha, head_sha) = build_fixture(ws_tmp.path());

        let app = make_app(&ws);
        let args = PrReviewRunArgs {
            base: base_sha,
            head: head_sha,
            engine: ReviewEngine::TempIndex,
            keep_cache: false,
            json: true,
            cache_root: Some(cache_tmp.path().to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };

        let (rendered, _) = run_inner(&app, &args).expect("The pr-review run should succeed.");

        let report: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        let review_root = PathBuf::from(report["safety"]["review_root"].as_str().unwrap());

        assert!(
            !review_root.exists(),
            "artifact root should be removed after run without --keep-cache; path={review_root:?}"
        );
    }

    // ── Test: baseline storage is not touched ─────────────────────────────────

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
        let args = PrReviewRunArgs {
            base: base_sha,
            head: head_sha,
            engine: ReviewEngine::TempIndex,
            keep_cache: false,
            json: true,
            cache_root: Some(cache_tmp.path().to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };

        let _ = run_inner(&app, &args).expect("The pr-review run should succeed.");

        // After state: .gather-step should have same existence as before.
        let existed_after = baseline_gather_step.exists();
        assert_eq!(
            existed_before, existed_after,
            ".gather-step baseline state should not change; \
             was {existed_before} before, {existed_after} after"
        );
    }

    #[test]
    fn pr_review_rejects_overlapping_cache_root_before_writing_marker() {
        if !git_available() {
            return;
        }

        let ws_tmp = TempDir::new("unsafe-cache-ws");
        let (ws, base_sha, head_sha) = build_fixture(ws_tmp.path());
        let baseline_storage = ws.join(".gather-step/storage");
        fs::create_dir_all(&baseline_storage).unwrap();
        let cache_root = baseline_storage.join("pr-review-cache");

        let app = make_app(&ws);
        let args = PrReviewRunArgs {
            base: base_sha,
            head: head_sha,
            engine: ReviewEngine::TempIndex,
            keep_cache: true,
            json: true,
            cache_root: Some(cache_root.clone()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };

        let err = run_inner(&app, &args).expect_err("overlapping cache root must be rejected");
        let message = format!("{err:#}");
        assert!(
            message.contains("review safety guard rejected"),
            "unexpected error: {message}"
        );
        assert!(
            !cache_root.exists(),
            "unsafe cache root must not be created before safety validation"
        );
    }

    #[test]
    fn pr_review_changed_files_use_merge_base_for_diverged_branch() {
        if !git_available() {
            return;
        }

        let ws_tmp = TempDir::new("diverged-ws");
        let cache_tmp = TempDir::new("diverged-cache");
        let (ws, base_sha, head_sha) = build_diverged_fixture(ws_tmp.path());

        let app = make_app(&ws);
        let args = PrReviewRunArgs {
            base: base_sha,
            head: head_sha,
            engine: ReviewEngine::TempIndex,
            keep_cache: false,
            json: true,
            cache_root: Some(cache_tmp.path().to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };

        let (rendered, _) = run_inner(&app, &args).expect("The pr-review run should succeed.");
        let report: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        let changed_files: Vec<&str> = report["changed_files"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect();

        assert!(
            changed_files
                .iter()
                .any(|path| path.ends_with("myrepo/src/feature_only.ts")),
            "feature-owned file must be reported: {changed_files:?}"
        );
        assert!(
            !changed_files
                .iter()
                .any(|path| path.ends_with("myrepo/src/main_only.ts")),
            "base-only file must not be reported as PR-owned: {changed_files:?}"
        );
    }

    #[test]
    fn pr_review_changed_repos_use_head_worktree_config() {
        if !git_available() {
            return;
        }

        let ws_tmp = TempDir::new("head-config-ws");
        let cache_tmp = TempDir::new("head-config-cache");
        let (ws, base_sha, head_sha) = build_head_config_fixture(ws_tmp.path());

        let app = make_app(&ws);
        let args = PrReviewRunArgs {
            base: base_sha,
            head: head_sha,
            engine: ReviewEngine::TempIndex,
            keep_cache: false,
            json: true,
            cache_root: Some(cache_tmp.path().to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };

        let (rendered, _) = run_inner(&app, &args).expect("The pr-review run should succeed.");
        let report: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        let changed_repos: Vec<&str> = report["metadata"]["changed_repos"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect();
        let indexed_repos: Vec<&str> = report["metadata"]["indexed_repos"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect();

        assert!(
            changed_repos.contains(&"newrepo"),
            "head-only configured repo must be used for changed_repos: {changed_repos:?}"
        );
        assert!(
            !changed_repos.contains(&"oldrepo"),
            "base workspace config must not classify changed_repos: {changed_repos:?}"
        );
        assert_eq!(indexed_repos, vec!["newrepo"]);
    }

    #[test]
    fn pr_review_temp_index_reports_python_symbols_and_deployment_topology() {
        if !git_available() {
            return;
        }

        let ws_tmp = TempDir::new("python-deployment-ws");
        let cache_tmp = TempDir::new("python-deployment-cache");
        let (ws, base_sha, head_sha) = build_python_deployment_fixture(ws_tmp.path());
        index_baseline_workspace(&ws);

        let app = make_app(&ws);
        let args = PrReviewRunArgs {
            base: base_sha,
            head: head_sha,
            engine: ReviewEngine::TempIndex,
            keep_cache: false,
            json: true,
            cache_root: Some(cache_tmp.path().to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };

        let (rendered, _) = run_inner(&app, &args).expect("The pr-review run should succeed.");
        let report: serde_json::Value = serde_json::from_str(&rendered).unwrap();

        let changed_files: Vec<&str> = report["changed_files"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect();
        assert!(
            changed_files
                .iter()
                .any(|path| path.ends_with("pyservice/src/pyservice/app.py")),
            "Python changes must be included in pr-review changed files: {changed_files:?}."
        );
        assert!(
            changed_files
                .iter()
                .any(|path| path.ends_with("pyservice/Dockerfile")),
            "Deployment-topology changes must be included in pr-review changed files: {changed_files:?}."
        );

        let added_symbols = report["symbols"]["added"].as_array().unwrap();
        assert!(
            added_symbols.iter().any(|symbol| {
                symbol["repo"] == "pyservice"
                    && symbol["kind"] == "function"
                    && symbol["qualified_name"]
                        .as_str()
                        .is_some_and(|qn| qn.ends_with("rollout_handler"))
            }),
            "Python symbol deltas must include rollout_handler: {added_symbols:?}."
        );

        let added_deployments = report["deployment"]["deployments"]["added"]
            .as_array()
            .unwrap();
        assert!(
            added_deployments.iter().any(|deployment| {
                deployment["repo"] == "pyservice"
                    && deployment["kind"] == "dockerfile"
                    && deployment["file"] == "Dockerfile"
            }),
            "Deployment topology deltas must include the added Dockerfile: {added_deployments:?}."
        );
    }

    #[test]
    fn map_changed_repos_uses_longest_prefix_match() {
        let config = GatherStepConfig {
            allow_listed_repos: vec![],
            repos: vec![
                gather_step_core::RepoConfig {
                    name: "parent".to_owned(),
                    path: "services".to_owned(),
                    depth: None,
                },
                gather_step_core::RepoConfig {
                    name: "api".to_owned(),
                    path: "services/api".to_owned(),
                    depth: None,
                },
            ],
            github: None,
            jira: None,
            indexing: gather_step_core::IndexingConfig::default(),
            deployment: gather_step_core::DeploymentConfig::default(),
        };

        let changed = vec!["services/api/src/lib.ts".to_owned()];
        assert_eq!(
            map_changed_repos_from_config(Some(&config), &changed),
            vec!["api"]
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Finding 1: clean_older_than_skips_in_progress
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn clean_older_than_skips_in_progress() {
        let ws_tmp = TempDir::new("skip-ip-ws");
        let cache_tmp = TempDir::new("skip-ip-cache");
        let ws = ws_tmp.path();
        let cache = cache_tmp.path();

        // Both artifacts are backdated well past the threshold.
        let old_ts = (chrono::Utc::now() - chrono::Duration::days(3)).to_rfc3339();

        // InProgress: must NOT be deleted by --older-than.
        let root_in_progress = write_fake_artifact(
            cache,
            ws,
            "review-in-progress",
            "baseIP",
            "headIP",
            ReviewStatus::InProgress,
            Some(&old_ts),
        );

        // Completed: must be deleted by --older-than.
        let root_completed = write_fake_artifact(
            cache,
            ws,
            "review-completed",
            "baseCO",
            "headCO",
            ReviewStatus::Completed,
            Some(&old_ts),
        );

        let app = make_app(ws);
        let top = PrReviewArgs {
            command: None,
            base: None,
            head: None,
            engine: ReviewEngine::TempIndex,
            keep_cache: false,
            json: false,
            cache_root: Some(cache.to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };
        let clean_args = CleanArgs {
            dry_run: false,
            run_id: None,
            base: None,
            head: None,
            older_than: Some("1s".to_owned()),
            include_active: false,
            all: false,
        };

        run_clean(&app, &top, &clean_args).expect("The clean --older-than command should succeed.");

        assert!(
            root_in_progress.exists(),
            "InProgress artifact must not be deleted by --older-than"
        );
        assert!(
            !root_completed.exists(),
            "Completed artifact should be deleted by --older-than"
        );

        // Confirm --all still reaches InProgress.
        let clean_all = CleanArgs {
            dry_run: false,
            run_id: None,
            base: None,
            head: None,
            older_than: None,
            include_active: false,
            all: true,
        };
        run_clean(&app, &top, &clean_all).expect("clean --all should succeed");
        assert!(
            !root_in_progress.exists(),
            "InProgress artifact should be deleted when user passes --all"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Finding 3a: clean_base_head_errors_on_unresolved_ref
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn clean_base_head_errors_on_unresolved_ref() {
        let ws_tmp = TempDir::new("err-ref-ws");
        let cache_tmp = TempDir::new("err-ref-cache");
        let ws = ws_tmp.path();
        let cache = cache_tmp.path();

        // Non-repo workspace so resolve_range will fail.
        let app = make_app(ws);
        let top = PrReviewArgs {
            command: None,
            base: None,
            head: None,
            engine: ReviewEngine::TempIndex,
            keep_cache: false,
            json: false,
            cache_root: Some(cache.to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };
        let clean_args = CleanArgs {
            dry_run: false,
            run_id: None,
            base: Some("typo-branch".to_owned()),
            head: Some("main".to_owned()),
            older_than: None,
            include_active: false,
            all: false,
        };

        let result = run_clean(&app, &top, &clean_args);
        assert!(
            result.is_err(),
            "clean --base <non-sha> --head <non-sha> must return Err on unresolved ref"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Finding 3b: clean_base_head_accepts_literal_full_shas
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn clean_base_head_accepts_literal_full_shas() {
        let ws_tmp = TempDir::new("lit-sha-ws");
        let cache_tmp = TempDir::new("lit-sha-cache");
        let ws = ws_tmp.path();
        let cache = cache_tmp.path();

        let sha_base = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef";
        let sha_head = "cafebabecafebabecafebabecafebabecafebabe";

        let root = write_fake_artifact(
            cache,
            ws,
            "review-lit-sha",
            sha_base,
            sha_head,
            ReviewStatus::Completed,
            None,
        );

        // Non-repo workspace — resolve_range will fail, but inputs are full SHAs
        // so the fallback path must kick in and match the marker.
        let app = make_app(ws);
        let top = PrReviewArgs {
            command: None,
            base: None,
            head: None,
            engine: ReviewEngine::TempIndex,
            keep_cache: false,
            json: false,
            cache_root: Some(cache.to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };
        let clean_args = CleanArgs {
            dry_run: false,
            run_id: None,
            base: Some(sha_base.to_owned()),
            head: Some(sha_head.to_owned()),
            older_than: None,
            include_active: false,
            all: false,
        };

        run_clean(&app, &top, &clean_args)
            .expect("clean with literal full SHAs should succeed even without a git repo");

        assert!(
            !root.exists(),
            "artifact matching literal SHAs should be deleted"
        );
    }

    // =========================================================================
    // Phase 4 Task 2: cold-vs-cached parity tests
    // =========================================================================

    // ── Helper: strip volatile fields before JSON comparison ─────────────────

    /// Zero out `metadata.elapsed_ms` and normalise `metadata.indexed_repos`
    /// order so two JSON values from different runs can be byte-compared.
    fn normalize_report_json(v: &mut serde_json::Value) {
        if let Some(Some(obj)) = v.get_mut("metadata").map(serde_json::Value::as_object_mut) {
            obj.insert("elapsed_ms".to_owned(), serde_json::Value::Number(0.into()));
            // Sort indexed_repos so order differences are invisible.
            if let Some(Some(arr)) = obj
                .get_mut("indexed_repos")
                .map(serde_json::Value::as_array_mut)
            {
                arr.sort_by(|a, b| a.as_str().unwrap_or("").cmp(b.as_str().unwrap_or("")));
            }
        }
        // Zero run-specific paths in safety to avoid diff noise.
        if let Some(Some(obj)) = v.get_mut("safety").map(serde_json::Value::as_object_mut) {
            obj.insert(
                "run_id".to_owned(),
                serde_json::Value::String(String::new()),
            );
            obj.insert(
                "review_root".to_owned(),
                serde_json::Value::String(String::new()),
            );
            obj.insert(
                "review_registry_path".to_owned(),
                serde_json::Value::String(String::new()),
            );
            obj.insert(
                "review_storage_path".to_owned(),
                serde_json::Value::String(String::new()),
            );
            // Remove cache_key (contains run-specific fingerprint).
            obj.remove("cache_key");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Parity Test 1: cached run produces identical JSON to cold run
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn cached_run_produces_identical_json_to_cold_run() {
        if !git_available() {
            return;
        }

        let ws_tmp = TempDir::new("parity-ws");
        let cache_tmp = TempDir::new("parity-cache");
        let (ws, base_sha, head_sha) = build_fixture(ws_tmp.path());

        let app = make_app(&ws);

        // First run (cold): keep_cache = true so the artifact persists.
        let args = PrReviewRunArgs {
            base: base_sha.clone(),
            head: head_sha.clone(),
            engine: ReviewEngine::TempIndex,
            keep_cache: true,
            json: true,
            cache_root: Some(cache_tmp.path().to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };
        let (cold_rendered, _) = run_inner(&app, &args).expect("cold run must succeed");
        let mut cold: serde_json::Value =
            serde_json::from_str(&cold_rendered).expect("cold JSON must parse");
        normalize_report_json(&mut cold);

        // Second run (cache hit): same args.
        let (cached_rendered, _) = run_inner(&app, &args).expect("cached run must succeed");
        let mut cached: serde_json::Value =
            serde_json::from_str(&cached_rendered).expect("cached JSON must parse");
        normalize_report_json(&mut cached);

        assert_eq!(
            cold, cached,
            "normalized cold and cached JSON must be identical"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Parity Test 2: cache invalidates on config_hash change
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn cache_invalidates_on_config_hash_change() {
        if !git_available() {
            return;
        }

        let ws_tmp = TempDir::new("cfg-inval-ws");
        let cache_tmp = TempDir::new("cfg-inval-cache");
        let (ws, base_sha, head_sha) = build_fixture(ws_tmp.path());

        let app = make_app(&ws);

        // The cache_key reads the config from `git show <head_sha>:gather-step.config.yaml`
        // (the bytes the worktree will actually see) by default, OR from
        // `--config` when set. To exercise the config_hash invalidation we
        // need a config source the test can mutate without committing — so
        // route both runs through `--config` against an external file.
        // Both configs must validate against the build_fixture worktree
        // (which contains a `myrepo/` directory) — otherwise the indexer
        // will reject the config before we get a cache key. Differ only on
        // an inline comment so the byte-hash differs but the repo set is
        // identical.
        let cfg_a = ws_tmp.path().join("config-a.yaml");
        let cfg_b = ws_tmp.path().join("config-b.yaml");
        let base_yaml =
            "repos:\n  - name: myrepo\n    path: myrepo\nindexing:\n  workspace_concurrency: 1\n";
        fs::write(&cfg_a, format!("# variant: a\n{base_yaml}")).unwrap();
        fs::write(&cfg_b, format!("# variant: b\n{base_yaml}")).unwrap();

        let mut args = PrReviewRunArgs {
            base: base_sha.clone(),
            head: head_sha.clone(),
            engine: ReviewEngine::TempIndex,
            keep_cache: true,
            json: true,
            cache_root: Some(cache_tmp.path().to_path_buf()),
            config: Some(cfg_a.clone()),
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };
        let (first_rendered, _) = run_inner(&app, &args).expect("first run must succeed");
        let first: serde_json::Value = serde_json::from_str(&first_rendered).unwrap();
        let first_run_id = first["safety"]["run_id"].as_str().unwrap().to_owned();

        // Switch to a different config file → different config_hash → cache miss.
        args.config = Some(cfg_b);
        let (second_rendered, _) = run_inner(&app, &args).expect("second run must succeed");
        let second: serde_json::Value = serde_json::from_str(&second_rendered).unwrap();
        let second_run_id = second["safety"]["run_id"].as_str().unwrap().to_owned();

        assert_ne!(
            first_run_id, second_run_id,
            "config change must cause cache miss (different run_id)"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Parity Test 3: cache invalidates on head SHA change
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn cache_invalidates_on_head_sha_change() {
        if !git_available() {
            return;
        }

        let ws_tmp = TempDir::new("sha-inval-ws");
        let cache_tmp = TempDir::new("sha-inval-cache");
        let (ws, base_sha, head_sha) = build_fixture(ws_tmp.path());

        let app = make_app(&ws);

        // First run on original head_sha.
        let args = PrReviewRunArgs {
            base: base_sha.clone(),
            head: head_sha.clone(),
            engine: ReviewEngine::TempIndex,
            keep_cache: true,
            json: true,
            cache_root: Some(cache_tmp.path().to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };
        let (first_rendered, _) = run_inner(&app, &args).expect("first run must succeed");
        let first: serde_json::Value = serde_json::from_str(&first_rendered).unwrap();
        let first_run_id = first["safety"]["run_id"].as_str().unwrap().to_owned();

        // Add a new commit on the feature branch → new head SHA.
        git_run(&ws, &["checkout", "feature/add-file"]);
        fs::write(
            ws.join("myrepo/src/another.ts"),
            "export function another(): string { return 'another'; }\n",
        )
        .unwrap();
        git_run(&ws, &["add", "."]);
        git_run(&ws, &["commit", "--message", "head: add another.ts"]);
        let new_head_sha = git_head_sha(&ws);
        git_run(&ws, &["checkout", "main"]);

        // Second run with new head SHA → cache miss.
        let args2 = PrReviewRunArgs {
            base: base_sha.clone(),
            head: new_head_sha,
            engine: ReviewEngine::TempIndex,
            keep_cache: true,
            json: true,
            cache_root: Some(cache_tmp.path().to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };
        let (second_rendered, _) = run_inner(&app, &args2).expect("second run must succeed");
        let second: serde_json::Value = serde_json::from_str(&second_rendered).unwrap();
        let second_run_id = second["safety"]["run_id"].as_str().unwrap().to_owned();

        assert_ne!(
            first_run_id, second_run_id,
            "changed head SHA must cause cache miss (different run_id)"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Parity Test 4: cache ignores completed markers without cache keys
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn cache_ignores_marker_without_cache_key() {
        if !git_available() {
            return;
        }

        let ws_tmp = TempDir::new("no-cache-key-ws");
        let cache_tmp = TempDir::new("no-cache-key-cache");
        let (ws, base_sha, head_sha) = build_fixture(ws_tmp.path());

        // Manually plant a completed marker without a cache_key in the
        // expected cache dir.
        let ws_hash = workspace_hash(&ws);
        let planted_run_id = "review-no-cache-key-run";
        let planted_root = cache_tmp.path().join(&ws_hash).join(planted_run_id);
        fs::create_dir_all(planted_root.join("storage")).unwrap();
        fs::create_dir_all(planted_root.join("worktree")).unwrap();
        fs::write(planted_root.join("registry.json"), b"{}").unwrap();

        let planted_marker = ReviewMarker {
            schema_version: MARKER_SCHEMA_VERSION,
            workspace_hash: ws_hash.clone(),
            workspace_root: ws.clone(),
            base_sha: base_sha.clone(),
            head_sha: head_sha.clone(),
            run_id: planted_run_id.to_owned(),
            storage_path: planted_root.join("storage"),
            registry_path: planted_root.join("registry.json"),
            gather_step_version: "0.0.0".to_owned(), // old version
            created_at: chrono::Utc::now().to_rfc3339(),
            status: ReviewStatus::Completed,
            cache_key: None,
            last_accessed_at: None,
        };
        let planted_json = serde_json::to_vec_pretty(&planted_marker).unwrap();
        fs::write(planted_root.join(MARKER_FILENAME), planted_json).unwrap();

        // Run with current binary version: it must not reuse an artifact that
        // lacks the branch-scoped cache key.
        let app = make_app(&ws);
        let args = PrReviewRunArgs {
            base: base_sha.clone(),
            head: head_sha.clone(),
            engine: ReviewEngine::TempIndex,
            keep_cache: true,
            json: true,
            cache_root: Some(cache_tmp.path().to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };
        let (rendered, _) = run_inner(&app, &args).expect("run must succeed");
        let report: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        let run_id = report["safety"]["run_id"].as_str().unwrap();

        // The run_id must differ from the planted run: a fresh artifact was created.
        assert_ne!(
            run_id, planted_run_id,
            "marker without cache_key must not be reused; got run_id={run_id}"
        );

        // Planted artifact root still exists (was not deleted by the fresh run).
        assert!(
            planted_root.exists(),
            "planted artifact must remain on disk after fresh run"
        );
    }

    // =========================================================================
    // Phase 4 Task 5: cache pruning policy tests
    // =========================================================================

    // Helper: write a fake artifact with a valid cache_key
    fn write_fake_artifact_with_key(
        cache_root: &Path,
        workspace_root: &Path,
        run_id: &str,
        base_sha: &str,
        head_sha: &str,
        status: ReviewStatus,
        created_at_override: Option<&str>,
        cache_key: Option<crate::pr_review::artifact_root::CacheKey>,
        last_accessed_at: Option<&str>,
    ) -> PathBuf {
        let hash = workspace_hash(workspace_root);
        let root = cache_root.join(&hash).join(run_id);
        fs::create_dir_all(&root).expect("create artifact root");

        let marker_path = root.join(MARKER_FILENAME);
        let storage_path = root.join("storage");
        let registry_path = root.join("registry.json");
        fs::create_dir_all(&storage_path).unwrap();
        fs::write(storage_path.join("dummy.txt"), b"data").unwrap();

        let created_at =
            created_at_override.map_or_else(|| chrono::Utc::now().to_rfc3339(), ToOwned::to_owned);

        let marker = ReviewMarker {
            schema_version: MARKER_SCHEMA_VERSION,
            workspace_hash: hash,
            workspace_root: workspace_root.to_path_buf(),
            base_sha: base_sha.to_owned(),
            head_sha: head_sha.to_owned(),
            run_id: run_id.to_owned(),
            storage_path,
            registry_path,
            gather_step_version: env!("CARGO_PKG_VERSION").to_owned(),
            created_at,
            status,
            cache_key,
            last_accessed_at: last_accessed_at.map(ToOwned::to_owned),
        };

        let json = serde_json::to_vec_pretty(&marker).expect("serialize marker");
        fs::write(&marker_path, json).expect("write marker");

        root
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Pruning Test 1: older-than skips artifacts with currently active keys
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn clean_older_than_skips_currently_active_keys() {
        if !git_available() {
            return;
        }

        let ws_tmp = TempDir::new("prune-active-ws");
        let cache_tmp = TempDir::new("prune-active-cache");
        let (ws, base_sha, head_sha) = build_fixture(ws_tmp.path());

        let cache = cache_tmp.path();

        // Artifact 1: has a cache key with SHAs that resolve in this workspace.
        // Its cache_key.base_sha/head_sha are real commits → is_cache_key_active returns true.
        let old_ts = (chrono::Utc::now() - chrono::Duration::days(10)).to_rfc3339();
        let active_key = crate::pr_review::artifact_root::CacheKey {
            workspace_hash: workspace_hash(&ws),
            base_sha: base_sha.clone(),
            head_sha: head_sha.clone(),
            config_hash: "cfg".to_owned(),
            schema_version: MARKER_SCHEMA_VERSION,
            gather_step_version: env!("CARGO_PKG_VERSION").to_owned(),
        };
        let root_active = write_fake_artifact_with_key(
            cache,
            &ws,
            "review-active-key",
            &base_sha,
            &head_sha,
            ReviewStatus::Completed,
            Some(&old_ts),
            Some(active_key),
            None,
        );

        // Artifact 2: has a cache key with SHAs that do NOT resolve in this
        // workspace (dead SHAs → force-push scenario).
        let inactive_key = crate::pr_review::artifact_root::CacheKey {
            workspace_hash: workspace_hash(&ws),
            base_sha: "deadbeef".repeat(5),
            head_sha: "cafebabe".repeat(5),
            config_hash: "cfg".to_owned(),
            schema_version: MARKER_SCHEMA_VERSION,
            gather_step_version: env!("CARGO_PKG_VERSION").to_owned(),
        };
        let root_inactive = write_fake_artifact_with_key(
            cache,
            &ws,
            "review-inactive-key",
            "deadbeef00000000000000000000000000000000",
            "cafebabe00000000000000000000000000000000",
            ReviewStatus::Completed,
            Some(&old_ts),
            Some(inactive_key),
            None,
        );

        let app = make_app(&ws);
        let top = PrReviewArgs {
            command: None,
            base: None,
            head: None,
            engine: ReviewEngine::TempIndex,
            keep_cache: false,
            json: false,
            cache_root: Some(cache.to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };
        let clean_args = CleanArgs {
            dry_run: false,
            run_id: None,
            base: None,
            head: None,
            older_than: Some("1s".to_owned()),
            include_active: false, // active keys should be skipped
            all: false,
        };

        run_clean(&app, &top, &clean_args).expect("The clean --older-than command should succeed.");

        assert!(
            root_active.exists(),
            "active-key artifact must be skipped (SHAs still resolvable)"
        );
        assert!(
            !root_inactive.exists(),
            "inactive-key artifact must be deleted (SHAs not resolvable)"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Pruning Test 2: --include-active deletes both active and inactive
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn clean_older_than_with_include_active_deletes_both() {
        if !git_available() {
            return;
        }

        let ws_tmp = TempDir::new("prune-incl-ws");
        let cache_tmp = TempDir::new("prune-incl-cache");
        let (ws, base_sha, head_sha) = build_fixture(ws_tmp.path());

        let cache = cache_tmp.path();

        let old_ts = (chrono::Utc::now() - chrono::Duration::days(10)).to_rfc3339();

        // Artifact with active key.
        let active_key = crate::pr_review::artifact_root::CacheKey {
            workspace_hash: workspace_hash(&ws),
            base_sha: base_sha.clone(),
            head_sha: head_sha.clone(),
            config_hash: "cfg".to_owned(),
            schema_version: MARKER_SCHEMA_VERSION,
            gather_step_version: env!("CARGO_PKG_VERSION").to_owned(),
        };
        let root_active = write_fake_artifact_with_key(
            cache,
            &ws,
            "review-incl-active",
            &base_sha,
            &head_sha,
            ReviewStatus::Completed,
            Some(&old_ts),
            Some(active_key),
            None,
        );

        // Artifact with inactive key.
        let inactive_key = crate::pr_review::artifact_root::CacheKey {
            workspace_hash: workspace_hash(&ws),
            base_sha: "deadbeef".repeat(5),
            head_sha: "cafebabe".repeat(5),
            config_hash: "cfg".to_owned(),
            schema_version: MARKER_SCHEMA_VERSION,
            gather_step_version: env!("CARGO_PKG_VERSION").to_owned(),
        };
        let root_inactive = write_fake_artifact_with_key(
            cache,
            &ws,
            "review-incl-inactive",
            "deadbeef00000000000000000000000000000000",
            "cafebabe00000000000000000000000000000000",
            ReviewStatus::Completed,
            Some(&old_ts),
            Some(inactive_key),
            None,
        );

        let app = make_app(&ws);
        let top = PrReviewArgs {
            command: None,
            base: None,
            head: None,
            engine: ReviewEngine::TempIndex,
            keep_cache: false,
            json: false,
            cache_root: Some(cache.to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };
        let clean_args = CleanArgs {
            dry_run: false,
            run_id: None,
            base: None,
            head: None,
            older_than: Some("1s".to_owned()),
            include_active: true, // override the active-skip protection
            all: false,
        };

        run_clean(&app, &top, &clean_args)
            .expect("clean --older-than --include-active should succeed");

        assert!(
            !root_active.exists(),
            "--include-active must delete active-key artifact"
        );
        assert!(
            !root_inactive.exists(),
            "--include-active must delete inactive-key artifact"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Pruning Test 3: --all is unconditional (ignores active keys)
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn clean_all_still_deletes_active_artifacts() {
        if !git_available() {
            return;
        }

        let ws_tmp = TempDir::new("prune-all-ws");
        let cache_tmp = TempDir::new("prune-all-cache");
        let (ws, base_sha, head_sha) = build_fixture(ws_tmp.path());

        let cache = cache_tmp.path();

        let active_key = crate::pr_review::artifact_root::CacheKey {
            workspace_hash: workspace_hash(&ws),
            base_sha: base_sha.clone(),
            head_sha: head_sha.clone(),
            config_hash: "cfg".to_owned(),
            schema_version: MARKER_SCHEMA_VERSION,
            gather_step_version: env!("CARGO_PKG_VERSION").to_owned(),
        };
        let root_active = write_fake_artifact_with_key(
            cache,
            &ws,
            "review-all-active",
            &base_sha,
            &head_sha,
            ReviewStatus::Completed,
            None,
            Some(active_key),
            None,
        );

        let app = make_app(&ws);
        let top = PrReviewArgs {
            command: None,
            base: None,
            head: None,
            engine: ReviewEngine::TempIndex,
            keep_cache: false,
            json: false,
            cache_root: Some(cache.to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };
        let clean_args = CleanArgs {
            dry_run: false,
            run_id: None,
            base: None,
            head: None,
            older_than: None,
            include_active: false,
            all: true, // --all is unconditional
        };

        run_clean(&app, &top, &clean_args).expect("clean --all should succeed");

        assert!(
            !root_active.exists(),
            "--all must delete active-key artifact unconditionally"
        );
    }

    // ── Phase 6 Task 4: severity threshold tests ──────────────────────────────

    use crate::pr_review::delta_report::{
        DELTA_REPORT_SCHEMA_VERSION, DeploymentDeltas, PayloadContractDeltaChange,
        PayloadFieldSummary, PayloadFieldTypeChange, RemovedSurfaceRisk,
    };

    fn make_delta_report_empty() -> DeltaReport {
        DeltaReport {
            schema_version: DELTA_REPORT_SCHEMA_VERSION,
            metadata: ReviewMetadata {
                workspace: std::path::PathBuf::from("/tmp/ws"),
                base_input: "main".to_owned(),
                base_sha: "a".repeat(40),
                head_input: "HEAD".to_owned(),
                head_sha: "b".repeat(40),
                checkout_mode: "head".to_owned(),
                changed_repos: vec![],
                indexed_repos: vec![],
                elapsed_ms: 0,
                warnings: vec![],
            },
            safety: SafetyMetadata {
                baseline_registry_path: std::path::PathBuf::from("/tmp/reg.json"),
                baseline_storage_path: std::path::PathBuf::from("/tmp/storage"),
                review_registry_path: std::path::PathBuf::from("/tmp/rev/reg.json"),
                review_storage_path: std::path::PathBuf::from("/tmp/rev/storage"),
                review_root: std::path::PathBuf::from("/tmp/rev"),
                run_id: "test-run".to_owned(),
                cleanup_policy: CleanupPolicy::RemoveOnExit,
                cache_key: "hash:aaa:bbb".to_owned(),
            },
            changed_files: vec![],
            changed_files_truncated: false,
            routes: RouteDeltas::default(),
            symbols: SymbolDeltas::default(),
            payload_contracts: PayloadContractDeltas::default(),
            events: EventDeltas::default(),
            removed_surface_risks: vec![],
            contract_alignments: ContractAlignments::default(),
            decorators: DecoratorDeltas::default(),
            deployment: DeploymentDeltas::default(),
            suggested_followups: vec![],
            unsupported_surfaces: vec![],
        }
    }

    fn make_severity_risk(severity: RiskSeverity) -> RemovedSurfaceRisk {
        RemovedSurfaceRisk {
            kind: "shared_symbol".to_owned(),
            identity: "SomeSymbol".to_owned(),
            repo: Some("backend".to_owned()),
            surviving_consumers: vec![],
            severity,
        }
    }

    fn make_payload_type_change(qn: &str) -> PayloadContractDeltaChange {
        PayloadContractDeltaChange {
            repo: "backend".to_owned(),
            file: "src/dto.ts".to_owned(),
            target_qualified_name: qn.to_owned(),
            side: "producer".to_owned(),
            fields_added: vec![],
            fields_removed: vec![],
            fields_optional_to_required: vec![],
            fields_required_to_optional: vec![],
            fields_type_changed: vec![PayloadFieldTypeChange {
                name: "status".to_owned(),
                before_type: Some("string".to_owned()),
                after_type: Some("number".to_owned()),
            }],
            impact: None,
        }
    }

    fn make_payload_fields_added_change(qn: &str) -> PayloadContractDeltaChange {
        PayloadContractDeltaChange {
            repo: "backend".to_owned(),
            file: "src/dto.ts".to_owned(),
            target_qualified_name: qn.to_owned(),
            side: "producer".to_owned(),
            fields_added: vec![PayloadFieldSummary {
                name: "newField".to_owned(),
                type_name: Some("string".to_owned()),
                optional: true,
            }],
            fields_removed: vec![],
            fields_optional_to_required: vec![],
            fields_required_to_optional: vec![],
            fields_type_changed: vec![],
            impact: None,
        }
    }

    /// Warn mode never triggers, regardless of how many High risks exist.
    #[test]
    fn severity_warn_never_triggers() {
        let mut report = make_delta_report_empty();
        report
            .removed_surface_risks
            .push(make_severity_risk(RiskSeverity::High));
        report
            .removed_surface_risks
            .push(make_severity_risk(RiskSeverity::High));
        report
            .payload_contracts
            .changed
            .push(make_payload_type_change("Foo"));
        assert!(
            !evaluate_severity_threshold(SeverityMode::Warn, &report),
            "Warn mode must never trigger exit 2"
        );
    }

    /// Strict mode triggers on a High-severity removed-surface risk.
    #[test]
    fn severity_strict_triggers_on_high_risk() {
        let mut report = make_delta_report_empty();
        report
            .removed_surface_risks
            .push(make_severity_risk(RiskSeverity::High));
        assert!(
            evaluate_severity_threshold(SeverityMode::Strict, &report),
            "Strict must trigger on High risk"
        );
    }

    /// Strict mode triggers when any payload contract has a type change.
    #[test]
    fn severity_strict_triggers_on_payload_type_change() {
        let mut report = make_delta_report_empty();
        report
            .payload_contracts
            .changed
            .push(make_payload_type_change("UpdateLabelDto"));
        assert!(
            evaluate_severity_threshold(SeverityMode::Strict, &report),
            "Strict must trigger when fields_type_changed is non-empty"
        );
    }

    /// Pedantic triggers on Medium risk; Strict does not.
    #[test]
    fn severity_pedantic_triggers_on_medium_risk() {
        let mut report = make_delta_report_empty();
        report
            .removed_surface_risks
            .push(make_severity_risk(RiskSeverity::Medium));
        assert!(
            evaluate_severity_threshold(SeverityMode::Pedantic, &report),
            "Pedantic must trigger on Medium risk"
        );
        assert!(
            !evaluate_severity_threshold(SeverityMode::Strict, &report),
            "Strict must NOT trigger on Medium-only risk"
        );
    }

    /// Pedantic triggers on any payload change (`fields_added` only, no type change);
    /// Strict does not.
    #[test]
    fn severity_pedantic_triggers_on_any_payload_change() {
        let mut report = make_delta_report_empty();
        report
            .payload_contracts
            .changed
            .push(make_payload_fields_added_change("CreateOrderDto"));
        assert!(
            evaluate_severity_threshold(SeverityMode::Pedantic, &report),
            "Pedantic must trigger on any payload change"
        );
        assert!(
            !evaluate_severity_threshold(SeverityMode::Strict, &report),
            "Strict must NOT trigger when only fields_added (no type changes)"
        );
    }

    /// `--strict` (old flag) is treated as severity = Strict by `run()` but we can
    /// verify the mapping logic at the args level.
    ///
    /// The actual `tracing::warn` deprecation notice is emitted by `run()`; this
    /// test verifies the semantic: strict==true && severity==Warn → Strict.
    #[test]
    fn legacy_strict_flag_maps_to_severity_strict() {
        // Simulate the logic inside run() that maps --strict to Strict.
        let strict_flag = true;
        let severity_arg = SeverityMode::Warn; // default, user did not pass --severity
        let effective = if strict_flag && severity_arg == SeverityMode::Warn {
            SeverityMode::Strict
        } else {
            severity_arg
        };
        assert_eq!(
            effective,
            SeverityMode::Strict,
            "--strict flag must map to SeverityMode::Strict when --severity is default Warn"
        );
    }

    // ── Phase 6 Task 3: github-comment-file flag ──────────────────────────────

    /// Verify that `--github-comment-file` causes the file to be written with
    /// a GitHub-comment-formatted report.
    ///
    /// This test uses a real fixture + `run_inner` so git must be available.
    #[test]
    fn github_comment_file_written_when_flag_set() {
        if !git_available() {
            return;
        }

        let ws_tmp = TempDir::new("gc-file-ws");
        let cache_tmp = TempDir::new("gc-file-cache");
        let output_tmp = TempDir::new("gc-file-output");
        let (ws, base_sha, head_sha) = build_fixture(ws_tmp.path());

        let comment_path = output_tmp.path().join("github-comment.md");

        let app = make_app(&ws);
        let args = PrReviewRunArgs {
            base: base_sha,
            head: head_sha,
            engine: ReviewEngine::TempIndex,
            keep_cache: false,
            json: false,
            cache_root: Some(cache_tmp.path().to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::GithubComment,
            github_comment_file: Some(comment_path.clone()),
            no_baseline_check: false,
        };

        let _ = run_inner(&app, &args).expect("The pr-review run should succeed.");

        assert!(
            comment_path.exists(),
            "--github-comment-file must create the output file"
        );

        let written =
            std::fs::read_to_string(&comment_path).expect("comment file must be readable");
        assert!(
            written.contains("gather-step pr-review"),
            "comment file must contain report content"
        );
        assert!(
            written.len() <= crate::pr_review::delta_report::GITHUB_COMMENT_LIMIT,
            "comment file must fit within GitHub's comment limit"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Cleanup guard tests
    // ─────────────────────────────────────────────────────────────────────────

    /// A cache hit must NOT delete the artifact directory when `--keep-cache` is
    /// not set.  The artifact must remain on disk for future reuse and should
    /// only be removed by `pr-review clean`.
    #[test]
    fn cache_hit_does_not_delete_artifact_on_default_cleanup() {
        if !git_available() {
            return;
        }

        let ws_tmp = TempDir::new("cleanup-hit-ws");
        let cache_tmp = TempDir::new("cleanup-hit-cache");
        let (ws, base_sha, head_sha) = build_fixture(ws_tmp.path());

        let app = make_app(&ws);

        // Cold run with keep_cache = true so the artifact is preserved.
        let args_keep = PrReviewRunArgs {
            base: base_sha.clone(),
            head: head_sha.clone(),
            engine: ReviewEngine::TempIndex,
            keep_cache: true,
            json: true,
            cache_root: Some(cache_tmp.path().to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };
        let (cold_rendered, _) = run_inner(&app, &args_keep).expect("cold run must succeed");
        let cold_json: serde_json::Value =
            serde_json::from_str(&cold_rendered).expect("cold JSON must parse");
        let artifact_root_path = std::path::PathBuf::from(
            cold_json["safety"]["review_root"]
                .as_str()
                .expect("review_root must be a string"),
        );
        assert!(
            artifact_root_path.exists(),
            "artifact must exist after cold run"
        );

        // Second run (cache hit) without keep_cache — must NOT delete the artifact.
        let args_no_keep = PrReviewRunArgs {
            base: base_sha.clone(),
            head: head_sha.clone(),
            engine: ReviewEngine::TempIndex,
            keep_cache: false,
            json: true,
            cache_root: Some(cache_tmp.path().to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };
        let (cached_rendered, _) =
            run_inner(&app, &args_no_keep).expect("The cached run should succeed.");
        let cached_json: serde_json::Value =
            serde_json::from_str(&cached_rendered).expect("The cached JSON should parse.");
        assert_eq!(
            cached_json["safety"]["cleanup_policy"].as_str(),
            Some("cache_hit_retained"),
            "cache-hit reports must not claim remove-on-exit cleanup"
        );

        assert!(
            artifact_root_path.exists(),
            "artifact must still exist after a cache-hit run with default cleanup"
        );
    }

    /// A cold run (cache miss) with default cleanup (no `--keep-cache`) must
    /// still delete the artifact directory when done.  This guards against
    /// regressions in the cleanup path.
    #[test]
    fn cache_miss_with_default_cleanup_still_deletes() {
        if !git_available() {
            return;
        }

        let ws_tmp = TempDir::new("cleanup-miss-ws");
        let cache_tmp = TempDir::new("cleanup-miss-cache");
        let (ws, base_sha, head_sha) = build_fixture(ws_tmp.path());

        let app = make_app(&ws);

        // Fresh run, no prior artifact — default cleanup should remove it.
        let args = PrReviewRunArgs {
            base: base_sha.clone(),
            head: head_sha.clone(),
            engine: ReviewEngine::TempIndex,
            keep_cache: false,
            json: true,
            cache_root: Some(cache_tmp.path().to_path_buf()),
            config: None,
            strict: false,
            severity: SeverityMode::Warn,
            format: OutputFormat::Markdown,
            github_comment_file: None,
            no_baseline_check: false,
        };
        let (rendered, _) = run_inner(&app, &args).expect("cold run must succeed");
        let report: serde_json::Value = serde_json::from_str(&rendered).expect("JSON must parse");
        let artifact_root_path = std::path::PathBuf::from(
            report["safety"]["review_root"]
                .as_str()
                .expect("review_root must be a string"),
        );

        assert!(
            !artifact_root_path.exists(),
            "artifact must be deleted after cold run with default cleanup (keep_cache=false)"
        );
    }
}
