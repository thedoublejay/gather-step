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
        artifact_root::{
            ArtifactRootError, MARKER_FILENAME, ReviewArtifactRoot, ReviewStatus,
            default_cache_root, generate_run_id, materialize_artifact_root, plan_artifact_root,
            read_marker, workspace_hash, write_marker_completed, write_marker_quarantined,
        },
        cache::{compute_cache_key, is_cache_key_active, try_reuse_cache},
        delta_report::{
            CleanupPolicy, ContractAlignments, DecoratorDeltas, DeltaReport, EventDeltas,
            PayloadContractDeltas, ReviewMetadata, RiskSeverity, RouteDeltas, SafetyMetadata,
            SymbolDeltas, build_suggested_followups, synthesize_review_pack_commands,
        },
        extract::{
            contract_alignment::extract_contract_alignments,
            decorators::extract_decorator_deltas,
            events::extract_event_deltas,
            impact_attach::impact_for_node,
            payload_contracts::extract_payload_contract_deltas,
            removed_surfaces::extract_removed_surface_risks,
            routes::{extract_route_deltas, find_route_node_id},
            symbols::{extract_symbol_deltas, find_symbol_node_id},
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

    /// Exit with code 2 if any removed-surface risk has severity `High`.
    /// Without this flag the report is always emitted with exit code 0.
    #[arg(long)]
    pub strict: bool,
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
    #[arg(long, requires = "older_than")]
    pub include_active: bool,

    /// Delete ALL review artifacts for this workspace.
    #[arg(long)]
    pub all: bool,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum ReviewEngine {
    TempIndex,
}

// ─── Handler ──────────────────────────────────────────────────────────────────

pub fn run(app: &AppContext, args: PrReviewArgs) -> Result<()> {
    match args.command {
        Some(PrReviewSubcommand::Clean(ref clean_args)) => run_clean(app, &args, clean_args),
        None => {
            // Default path: run a review. --base and --head are required here.
            let base = args
                .base
                .as_deref()
                .context("--base is required when running a review (no subcommand given)")?
                .to_owned();
            let head = args
                .head
                .as_deref()
                .context("--head is required when running a review (no subcommand given)")?
                .to_owned();

            // Reconstruct typed args with the validated required fields.
            let review_args = PrReviewRunArgs {
                base,
                head,
                engine: args.engine,
                keep_cache: args.keep_cache,
                json: args.json,
                cache_root: args.cache_root,
                strict: args.strict,
            };

            let (report, has_high_risk) = run_inner(app, &review_args)?;
            // Print to stdout.
            #[expect(
                clippy::print_stdout,
                reason = "pr-review is the sole caller of this path; structured output goes here"
            )]
            {
                println!("{report}");
            }
            // Exit code 2 when --strict and High-severity risks exist.
            // Using std::process::exit after rendering so the report always prints
            // before the process terminates; callers can distinguish "broke" (exit
            // 1 from anyhow) from "high-severity risk found" (exit 2).
            if has_high_risk {
                std::process::exit(2);
            }
            Ok(())
        }
    }
}

// ─── Validated run-review args ─────────────────────────────────────────────

/// Validated args for the "run a review" path (no subcommand).
///
/// Extracted from `PrReviewArgs` after confirming `--base` and `--head` are
/// present.  Used internally so `run_inner` can still take typed fields.
pub struct PrReviewRunArgs {
    pub base: String,
    pub head: String,
    pub engine: ReviewEngine,
    pub keep_cache: bool,
    pub json: bool,
    pub cache_root: Option<PathBuf>,
    pub strict: bool,
}

/// Internal result type for the cache-hit-or-cold-run branch in [`run_inner`].
enum RunOutcome {
    CacheHit(ReviewArtifactRoot),
    ColdRun {
        artifact_root: ReviewArtifactRoot,
        worktree: gather_step_git::worktrees::ReviewWorktree,
        elapsed_ms: u64,
        total_repos: usize,
    },
}

/// Core implementation — returns `(rendered_string, has_high_risk)`.
///
/// `has_high_risk` is `true` when `args.strict` is set and at least one
/// `RemovedSurfaceRisk` has `severity == High`.  The caller uses this to exit
/// with code 2 AFTER printing the report.
pub fn run_inner(app: &AppContext, args: &PrReviewRunArgs) -> Result<(String, bool)> {
    let emit_json = args.json || app.json_output;

    // ── 1. Resolve refs ────────────────────────────────────────────────────
    let resolved =
        resolve_range(&app.workspace_path, &args.base, &args.head).with_context(|| {
            format!(
                "resolving refs `{}..{}` in `{}`",
                args.base,
                args.head,
                app.workspace_path.display()
            )
        })?;

    let base_sha = resolved.base.sha.clone();
    let head_sha = resolved.head.sha.clone();

    // ── 2. Changed files ───────────────────────────────────────────────────
    let diff_base_sha = merge_base(&app.workspace_path, &base_sha, &head_sha)
        .with_context(|| format!("finding merge-base for `{base_sha}` and `{head_sha}`"))?;
    let changed =
        changed_files(&app.workspace_path, &diff_base_sha, &head_sha).with_context(|| {
            format!("listing changed files between merge-base `{diff_base_sha}` and `{head_sha}`")
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
    // Read the workspace config file to compute a stable config_hash.
    // If the file is absent or unreadable, treat as empty bytes.
    let config_file_path = app.workspace_path.join("gather-step.config.yaml");
    let config_bytes: Vec<u8> = std::fs::read(&config_file_path).unwrap_or_default();
    let cache_key_struct =
        compute_cache_key(&app.workspace_path, &base_sha, &head_sha, &config_bytes);

    // ── 4. Artifact root ───────────────────────────────────────────────────
    let cache_root = args
        .cache_root
        .clone()
        .unwrap_or_else(|| default_cache_root(&app.workspace_path));

    // Try to reuse a prior completed artifact with the same cache key.
    // Cache reuse is independent of `keep_cache` — `keep_cache` controls
    // whether the CURRENT run's artifact is preserved after delta extraction.
    let outcome: RunOutcome =
        if let Some(hit_root) = try_reuse_cache(&cache_root, &cache_key_struct).unwrap_or_else(
            |e| {
                tracing::debug!(error = %e, "cache lookup failed; falling back to cold run");
                None
            },
        ) {
            // Cache hit: skip worktree creation and indexing.
            RunOutcome::CacheHit(hit_root)
        } else {
            // Cache miss: create a fresh artifact root, worktree, and index.
            let run_id = generate_run_id();

            let artifact_root =
                plan_artifact_root(&cache_root, &app.workspace_path, &run_id)
                    .with_context(|| format!("planning artifact root for run `{run_id}`"))?;

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
            .with_context(|| "review safety guard rejected the proposed artifact paths")?;

            materialize_artifact_root(
                &artifact_root,
                &base_sha,
                &head_sha,
                Some(cache_key_struct.clone()),
            )
            .with_context(|| format!("creating artifact root for run `{run_id}`"))?;

            // ── 5. Materialize worktree ────────────────────────────────────────
            // `materialize_artifact_root` pre-creates the worktree directory; git worktree
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

            // ── 6. Index ───────────────────────────────────────────────────────
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

            RunOutcome::ColdRun {
                artifact_root,
                worktree,
                elapsed_ms,
                total_repos: stats.total_repos,
            }
        };

    // Destructure the outcome for the remainder of the function.
    let (artifact_root, worktree_opt, elapsed_ms, total_repos_hint) = match outcome {
        RunOutcome::CacheHit(root) => (root, None, 0u64, None),
        RunOutcome::ColdRun {
            artifact_root,
            worktree,
            elapsed_ms,
            total_repos,
        } => (artifact_root, Some(worktree), elapsed_ms, Some(total_repos)),
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
    } else {
        CleanupPolicy::RemoveOnExit
    };

    // ── 8a. Open storage coordinators for diff extraction ─────────────────────
    // Fail-soft: if the workspace has never been indexed, emit empty deltas and
    // log a warning rather than aborting the entire review run.
    // We check for directory existence first so we never create the baseline
    // storage path as a side effect of opening the coordinator.
    let (route_deltas, symbol_deltas, payload_contract_deltas, event_deltas, surface_risks, contract_alignments, decorator_deltas) = {
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
                            let routes =
                                match extract_route_deltas(baseline_coord.graph(), review_coord.graph()) {
                                    Ok(d) => d,
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            "route delta extraction failed; emitting empty deltas"
                                        );
                                        RouteDeltas::default()
                                    }
                                };
                            let symbols =
                                match extract_symbol_deltas(baseline_coord.graph(), review_coord.graph()) {
                                    Ok(d) => d,
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            "symbol delta extraction failed; emitting empty deltas"
                                        );
                                        SymbolDeltas::default()
                                    }
                                };
                            let payload_contracts =
                                match extract_payload_contract_deltas(
                                    baseline_coord.metadata(),
                                    review_coord.metadata(),
                                ) {
                                    Ok(d) => d,
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            "payload-contract delta extraction failed; \
                                             emitting empty deltas"
                                        );
                                        PayloadContractDeltas::default()
                                    }
                                };
                            let events =
                                match extract_event_deltas(baseline_coord.graph(), review_coord.graph()) {
                                    Ok(d) => d,
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            "event delta extraction failed; emitting empty deltas"
                                        );
                                        EventDeltas::default()
                                    }
                                };
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
                                        "removed-surface risk extraction failed; \
                                         emitting empty risks"
                                    );
                                    vec![]
                                }
                            };

                            // ── Phase 3: attach impact to removed/changed routes ──
                            let mut routes = routes;
                            for r in &mut routes.removed {
                                match find_route_node_id(baseline_coord.graph(), &r.method, &r.path) {
                                    Ok(Some(node_id)) => {
                                        match impact_for_node(baseline_coord.graph(), node_id, r.repo.as_deref()) {
                                            Ok(summary) => r.impact = Some(summary),
                                            Err(e) => tracing::warn!(
                                                error = %e,
                                                method = %r.method,
                                                path = %r.path,
                                                "impact attachment failed for removed route"
                                            ),
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(e) => tracing::warn!(
                                        error = %e,
                                        method = %r.method,
                                        path = %r.path,
                                        "route node lookup failed"
                                    ),
                                }
                            }
                            for c in &mut routes.changed {
                                // Impact is computed against the BASELINE node.
                                let (method, path) = (&c.method.clone(), &c.path.clone());
                                match find_route_node_id(baseline_coord.graph(), method, path) {
                                    Ok(Some(node_id)) => {
                                        let repo = c.before.as_ref().and_then(|b| b.repo.as_deref());
                                        match impact_for_node(baseline_coord.graph(), node_id, repo) {
                                            Ok(summary) => {
                                                if let Some(before) = c.before.as_mut() {
                                                    before.impact = Some(summary);
                                                }
                                            }
                                            Err(e) => tracing::warn!(
                                                error = %e,
                                                method = %method,
                                                path = %path,
                                                "impact attachment failed for changed route"
                                            ),
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(e) => tracing::warn!(
                                        error = %e,
                                        method = %method,
                                        path = %path,
                                        "route node lookup failed for changed"
                                    ),
                                }
                            }

                            // ── Phase 3: attach impact to removed/changed symbols ─
                            let mut symbols = symbols;
                            for s in &mut symbols.removed {
                                match find_symbol_node_id(baseline_coord.graph(), &s.repo, &s.qualified_name) {
                                    Ok(Some(node_id)) => {
                                        match impact_for_node(baseline_coord.graph(), node_id, Some(&s.repo)) {
                                            Ok(summary) => s.impact = Some(summary),
                                            Err(e) => tracing::warn!(
                                                error = %e,
                                                repo = %s.repo,
                                                qn = %s.qualified_name,
                                                "impact attachment failed for removed symbol"
                                            ),
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(e) => tracing::warn!(
                                        error = %e,
                                        repo = %s.repo,
                                        qn = %s.qualified_name,
                                        "symbol node lookup failed"
                                    ),
                                }
                            }
                            for c in &mut symbols.changed {
                                // Impact on the BASELINE node.
                                let (repo, qn) = (c.repo.clone(), c.qualified_name.clone());
                                match find_symbol_node_id(baseline_coord.graph(), &repo, &qn) {
                                    Ok(Some(node_id)) => {
                                        match impact_for_node(baseline_coord.graph(), node_id, Some(&repo)) {
                                            Ok(summary) => c.before.impact = Some(summary),
                                            Err(e) => tracing::warn!(
                                                error = %e,
                                                repo = %repo,
                                                qn = %qn,
                                                "impact attachment failed for changed symbol"
                                            ),
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(e) => tracing::warn!(
                                        error = %e,
                                        repo = %repo,
                                        qn = %qn,
                                        "symbol node lookup failed for changed"
                                    ),
                                }
                            }

                            // ── Phase 3 Task 3: contract alignment ───────────
                            let contract_alignments = match extract_contract_alignments(
                                review_coord.metadata(),
                                &payload_contracts,
                            ) {
                                Ok(a) => a,
                                Err(e) => {
                                    tracing::warn!(
                                        error = %e,
                                        "contract alignment extraction failed; \
                                         emitting empty alignments"
                                    );
                                    ContractAlignments::default()
                                }
                            };

                            // ── Phase 3 Task 4: decorator deltas ──────────────
                            let decorator_deltas = match extract_decorator_deltas(
                                baseline_coord.graph(),
                                review_coord.graph(),
                            ) {
                                Ok(d) => d,
                                Err(e) => {
                                    tracing::warn!(
                                        error = %e,
                                        "decorator delta extraction failed; \
                                         emitting empty decorator deltas"
                                    );
                                    DecoratorDeltas::default()
                                }
                            };

                            (routes, symbols, payload_contracts, events, risks, contract_alignments, decorator_deltas)
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                "baseline storage could not be opened; \
                                 emitting empty deltas"
                            );
                            (RouteDeltas::default(), SymbolDeltas::default(), PayloadContractDeltas::default(), EventDeltas::default(), vec![], ContractAlignments::default(), DecoratorDeltas::default())
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "review storage could not be opened for diff extraction; \
                         emitting empty deltas"
                    );
                    (RouteDeltas::default(), SymbolDeltas::default(), PayloadContractDeltas::default(), EventDeltas::default(), vec![], ContractAlignments::default(), DecoratorDeltas::default())
                }
            }
        } else {
            tracing::warn!(
                storage = %ws_paths.storage_root.display(),
                "baseline index not found; run `gather-step index` first \
                 to enable PR-review deltas"
            );
            (RouteDeltas::default(), SymbolDeltas::default(), PayloadContractDeltas::default(), EventDeltas::default(), vec![], ContractAlignments::default(), DecoratorDeltas::default())
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

    let report = DeltaReport {
        schema_version: 3,
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
        // For cache-hit runs, worktree_opt is None — the worktree was created
        // by a prior run and must not be removed (the artifact may still be
        // in use by other commands).  For cold runs, remove the worktree.
        if let Some(wt) = worktree_opt {
            let _ = remove_worktree(&wt);
        }
        let _ = std::fs::remove_dir_all(&artifact_root.root);
    }

    // ── 11. Compute strict-mode signal ────────────────────────────────────
    let has_high_risk = args.strict
        && report
            .removed_surface_risks
            .iter()
            .any(|r| matches!(r.severity, RiskSeverity::High));

    // ── 12. Render ─────────────────────────────────────────────────────────
    let rendered = if emit_json {
        report
            .render_json()
            .context("serializing delta report to JSON")?
    } else {
        report.render_markdown()
    };
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

/// Mark the artifact root as Quarantined on error, ignoring any secondary
/// failure to write the marker.
fn quarantine_on_error(artifact_root: &ReviewArtifactRoot) {
    let _ = write_marker_quarantined(artifact_root);
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

    let entries = std::fs::read_dir(&hash_dir)
        .with_context(|| format!("reading review cache directory `{}`", hash_dir.display()))?;

    for entry in entries {
        let entry = entry.with_context(|| format!("reading entry in `{}`", hash_dir.display()))?;
        let root = entry.path();

        if !root.is_dir() {
            continue;
        }

        let marker_path = root.join(MARKER_FILENAME);
        match read_marker(&marker_path) {
            Ok(marker) => {
                if marker.workspace_hash != current_hash {
                    tracing::warn!(
                        "skipping `{}`: workspace_hash mismatch (expected `{}`, got `{}`)",
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
                tracing::warn!("skipping `{}`: could not read marker: {e}", root.display());
            }
        }
    }

    Ok(artifacts)
}

/// Recursively sum the sizes of all files under `dir`.  Ignores I/O errors
/// (treats unreadable entries as zero bytes).
fn dir_size_bytes(dir: &Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                total += dir_size_bytes(&path);
            } else if let Ok(meta) = entry.metadata() {
                total += meta.len();
            }
        }
    }
    total
}

/// Parse a duration string like `7d`, `1w`, `12h`, `30m`, `60s` into a
/// `std::time::Duration`.
pub fn parse_duration(s: &str) -> Result<std::time::Duration> {
    let s = s.trim();
    if s.is_empty() {
        bail!("empty duration string");
    }

    let (num_str, unit) = s.split_at(s.len() - 1);
    let n: u64 = num_str.parse().with_context(|| {
        format!("invalid duration `{s}`: expected `<n><unit>` where unit is s/m/h/d/w")
    })?;

    let secs = match unit {
        "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        "d" => n * 86_400,
        "w" => n * 7 * 86_400,
        other => bail!("invalid duration unit `{other}` in `{s}`: use s, m, h, d, or w"),
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

// ─── Safety guard ─────────────────────────────────────────────────────────────

/// Verify that `artifact_root` does not overlap the baseline `.gather-step`
/// storage or registry paths.
///
/// Returns `Ok(())` when safe, `Err` with a descriptive message when the
/// artifact root could clobber baseline state.
fn assert_not_baseline_overlap(artifact_root: &Path, workspace_root: &Path) -> Result<()> {
    let baseline_storage = workspace_root.join(".gather-step").join("storage");
    let baseline_registry = workspace_root.join(".gather-step").join("registry.json");

    // Check if artifact_root equals or is an ancestor of baseline paths, or
    // vice-versa (baseline path inside artifact_root).
    for baseline in [&baseline_storage, &baseline_registry] {
        if artifact_root == *baseline
            || baseline.starts_with(artifact_root)
            || artifact_root.starts_with(baseline)
        {
            bail!(
                "artifact root `{}` overlaps baseline path `{}`; refusing deletion",
                artifact_root.display(),
                baseline.display()
            );
        }
    }
    Ok(())
}

/// Returns `true` when `s` looks like a full 40-character lowercase hex SHA-1.
///
/// Used to decide whether an unresolved `--base`/`--head` input should be
/// silently treated as a literal SHA (safe bypass) or surfaced as an error
/// (likely a typo'd ref name).
fn is_full_sha(s: &str) -> bool {
    s.len() == 40 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Delete a single artifact root, enforcing all safety guards.
///
/// 1. Re-reads the marker (refuses if missing/unparseable).
/// 2. Checks `workspace_hash` matches.
/// 3. Checks no overlap with baseline storage/registry.
/// 4. Attempts worktree removal, then `remove_dir_all`.
///
/// In dry-run mode, logs what would be removed but performs no deletion.
pub(crate) fn delete_artifact(
    artifact: &DiscoveredArtifact,
    workspace_root: &Path,
    dry_run: bool,
) -> Result<()> {
    // Step 1: re-read the marker.
    let marker_path = artifact.root.join(MARKER_FILENAME);
    let marker = read_marker(&marker_path).with_context(|| {
        format!(
            "re-reading marker for artifact at `{}`",
            artifact.root.display()
        )
    })?;

    // Step 2: workspace hash check.
    let current_hash = workspace_hash(workspace_root);
    if marker.workspace_hash != current_hash {
        bail!(
            "refusing to delete `{}`: workspace_hash in marker (`{}`) does not match \
             current workspace hash (`{}`)",
            artifact.root.display(),
            marker.workspace_hash,
            current_hash,
        );
    }

    // Step 3: no overlap with baseline paths.
    assert_not_baseline_overlap(&artifact.root, workspace_root)
        .with_context(|| format!("safety check for `{}`", artifact.root.display()))?;

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

    // Step 4: remove worktree (best-effort), then remove the artifact dir.
    let worktree_root = artifact.root.join("worktree");
    if worktree_root.is_dir() {
        let wt = ReviewWorktree {
            repo: workspace_root.to_path_buf(),
            root: worktree_root,
            sha: marker.head_sha.clone(),
        };
        // Best-effort: if removal fails, continue and let remove_dir_all clean up.
        let _ = remove_worktree(&wt);
    }

    std::fs::remove_dir_all(&artifact.root)
        .with_context(|| format!("removing artifact root `{}`", artifact.root.display()))?;

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
             --run-id <ID>, --base <REF> --head <REF>, --older-than <DURATION>, or --all"
        );
    }
    if selector_count > 1 {
        bail!(
            "pr-review clean: only one selector may be given at a time; \
             combine --dry-run with any selector to preview"
        );
    }

    // Validate --base/--head: both or neither.
    match (&args.base, &args.head) {
        (Some(_), None) | (None, Some(_)) => {
            bail!("--base and --head must be specified together");
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
        .context("discovering review artifacts")?;

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
                        "could not resolve --base {base_ref:?} or --head {head_ref:?} \
                         against workspace at {}; pass full 40-char SHAs to bypass resolution",
                        app.workspace_path.display()
                    ));
                }
            }
        };
        all_artifacts
            .into_iter()
            .filter(|a| a.marker.base_sha == base_sha && a.marker.head_sha == head_sha)
            .collect()
    } else if let Some(ref duration_str) = args.older_than {
        let max_age = parse_duration(duration_str)
            .with_context(|| format!("parsing --older-than `{duration_str}`"))?;
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
                        "skipping active cache key (use --include-active to override)"
                    );
                    return false;
                }
                // Parse RFC 3339 created_at to compare age.
                chrono::DateTime::parse_from_rfc3339(&a.marker.created_at)
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

    // Execute or preview deletions.
    let mut entries: Vec<CleanArtifactEntry> = Vec::with_capacity(selected.len());
    let mut had_error = false;

    for artifact in &selected {
        let was_dry = args.dry_run;
        match delete_artifact(artifact, &app.workspace_path, args.dry_run) {
            Ok(()) => {
                entries.push(CleanArtifactEntry {
                    run_id: artifact.marker.run_id.clone(),
                    root: artifact.root.display().to_string(),
                    size_bytes: artifact.size_bytes,
                    deleted: !was_dry,
                });
            }
            Err(e) => {
                tracing::error!("failed to process `{}`: {e:#}", artifact.root.display());
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
            serde_json::to_string_pretty(&output).context("serializing clean output to JSON")?;
        #[expect(clippy::print_stdout, reason = "clean JSON output goes to stdout")]
        {
            println!("{json}");
        }
    }

    if had_error {
        bail!("one or more artifacts could not be cleaned; see errors above");
    }

    Ok(())
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
    use crate::{
        app::AppContext,
        pr_review::artifact_root::{MARKER_FILENAME, ReviewMarker, ReviewStatus, workspace_hash},
    };

    // ── temp-dir helper ───────────────────────────────────────────────────────

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(label: &str) -> Self {
            let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!("gs-pr-review-test-{label}-{id}"));
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
            schema_version: 1,
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
            cache_key: None, // v1 marker — not eligible for cache reuse
        };

        let json = serde_json::to_vec_pretty(&marker).expect("serialize marker");
        fs::write(&marker_path, json).expect("write marker");

        root
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
            strict: false,
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
            strict: false,
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
            strict: false,
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
            strict: false,
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

        run_clean(&app, &top, &clean_args).expect("clean --older-than should succeed");

        assert!(!root_old.exists(), "old artifact should be deleted");
        assert!(root_fresh.exists(), "fresh artifact must remain");
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
            strict: false,
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
            schema_version: 1,
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
            schema_version: 1,
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
            strict: false,
        };

        let (rendered, _) = run_inner(&app, &args).expect("run_inner should succeed");

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
            strict: false,
        };

        let (rendered, _) = run_inner(&app, &args).expect("run_inner should succeed");

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
            strict: false,
        };

        let (rendered, _) = run_inner(&app, &args).expect("run_inner should succeed");

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
            strict: false,
        };

        let _ = run_inner(&app, &args).expect("run_inner should succeed");

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
            strict: false,
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
            strict: false,
        };

        let (rendered, _) = run_inner(&app, &args).expect("run_inner should succeed");
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
            strict: false,
        };

        let (rendered, _) = run_inner(&app, &args).expect("run_inner should succeed");
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
            strict: false,
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

        run_clean(&app, &top, &clean_args).expect("clean --older-than should succeed");

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
            strict: false,
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
            strict: false,
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
            obj.insert(
                "elapsed_ms".to_owned(),
                serde_json::Value::Number(0.into()),
            );
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
            obj.insert("run_id".to_owned(), serde_json::Value::String(String::new()));
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
            strict: false,
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

        // First run: keep_cache.
        let args = PrReviewRunArgs {
            base: base_sha.clone(),
            head: head_sha.clone(),
            engine: ReviewEngine::TempIndex,
            keep_cache: true,
            json: true,
            cache_root: Some(cache_tmp.path().to_path_buf()),
            strict: false,
        };
        let (first_rendered, _) = run_inner(&app, &args).expect("first run must succeed");
        let first: serde_json::Value = serde_json::from_str(&first_rendered).unwrap();
        let first_run_id = first["safety"]["run_id"].as_str().unwrap().to_owned();

        // Modify the config file (different bytes → different config_hash).
        fs::write(
            ws.join("gather-step.config.yaml"),
            "repos:\n  - name: changed\n    path: changed\nindexing:\n  workspace_concurrency: 1\n",
        )
        .unwrap();

        // Second run: same base/head, but config changed → cache miss.
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
            strict: false,
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
            strict: false,
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
    // Parity Test 4: cache invalidates on gather_step_version change (v1 marker)
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn cache_invalidates_on_v1_marker_without_cache_key() {
        if !git_available() {
            return;
        }

        let ws_tmp = TempDir::new("v1-ws");
        let cache_tmp = TempDir::new("v1-cache");
        let (ws, base_sha, head_sha) = build_fixture(ws_tmp.path());

        // Manually plant a v1 marker (no cache_key) in the expected cache dir.
        let ws_hash = workspace_hash(&ws);
        let v1_run_id = "review-v1-legacy-run";
        let v1_root = cache_tmp.path().join(&ws_hash).join(v1_run_id);
        fs::create_dir_all(v1_root.join("storage")).unwrap();
        fs::create_dir_all(v1_root.join("worktree")).unwrap();
        fs::write(v1_root.join("registry.json"), b"{}").unwrap();

        let v1_marker = ReviewMarker {
            schema_version: 1,
            workspace_hash: ws_hash.clone(),
            workspace_root: ws.clone(),
            base_sha: base_sha.clone(),
            head_sha: head_sha.clone(),
            run_id: v1_run_id.to_owned(),
            storage_path: v1_root.join("storage"),
            registry_path: v1_root.join("registry.json"),
            gather_step_version: "0.0.0".to_owned(), // old version
            created_at: chrono::Utc::now().to_rfc3339(),
            status: ReviewStatus::Completed,
            cache_key: None, // v1: no cache key
        };
        let v1_json = serde_json::to_vec_pretty(&v1_marker).unwrap();
        fs::write(v1_root.join(MARKER_FILENAME), v1_json).unwrap();

        // Run with current binary version → must NOT reuse v1 artifact.
        let app = make_app(&ws);
        let args = PrReviewRunArgs {
            base: base_sha.clone(),
            head: head_sha.clone(),
            engine: ReviewEngine::TempIndex,
            keep_cache: true,
            json: true,
            cache_root: Some(cache_tmp.path().to_path_buf()),
            strict: false,
        };
        let (rendered, _) = run_inner(&app, &args).expect("run must succeed");
        let report: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        let run_id = report["safety"]["run_id"].as_str().unwrap();

        // The run_id must differ from v1_run_id — a fresh artifact was created.
        assert_ne!(
            run_id, v1_run_id,
            "v1 marker without cache_key must not be reused; got run_id={run_id}"
        );

        // v1 artifact root still exists (was not deleted by the fresh run).
        assert!(v1_root.exists(), "v1 artifact must remain on disk after fresh run");
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
            schema_version: 2,
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
            schema_version: 2,
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
        );

        // Artifact 2: has a cache key with SHAs that do NOT resolve in this
        // workspace (dead SHAs → force-push scenario).
        let inactive_key = crate::pr_review::artifact_root::CacheKey {
            workspace_hash: workspace_hash(&ws),
            base_sha: "deadbeef".repeat(5),
            head_sha: "cafebabe".repeat(5),
            config_hash: "cfg".to_owned(),
            schema_version: 2,
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
            strict: false,
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

        run_clean(&app, &top, &clean_args).expect("clean --older-than should succeed");

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
            schema_version: 2,
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
        );

        // Artifact with inactive key.
        let inactive_key = crate::pr_review::artifact_root::CacheKey {
            workspace_hash: workspace_hash(&ws),
            base_sha: "deadbeef".repeat(5),
            head_sha: "cafebabe".repeat(5),
            config_hash: "cfg".to_owned(),
            schema_version: 2,
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
            strict: false,
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

        run_clean(&app, &top, &clean_args).expect("clean --older-than --include-active should succeed");

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
            schema_version: 2,
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
            strict: false,
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
}
