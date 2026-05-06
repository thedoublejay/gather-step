use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use clap::Args;
use console::style;
use gather_step_core::{
    GatherStepConfig, NodeData, NodeKind, RegistryStore, RepoIndexMetadata, WorkspaceRepoResult,
    WorkspaceStats,
};
use gather_step_git::{
    GitHistoryIndexer, GitRepoSource, HistorySyncOutcome, RepoIntelligenceOptions,
    refresh_repo_intelligence,
};
use gather_step_mcp::{
    McpContext, McpServerConfig,
    ids::encode_node_id,
    tools::packs::{
        ModePackRequest, change_impact_pack_tool, debug_pack_tool, fix_pack_tool,
        planning_pack_tool, review_pack_tool,
    },
};
use gather_step_parser::frameworks::{Framework, detect_frameworks};
use gather_step_storage::{
    EdgeCountSummary, GraphStore, IndexingOptions, RepoIndexPayload, RepoIndexer,
};
use indicatif::{ProgressBar, ProgressStyle};
use serde::Serialize;
use tracing::{info, warn};

use crate::{
    app::{AppContext, DepthArg},
    commands::clean,
    path_safety,
    pr_review::cleanup::clean_all_for_workspace,
};

#[derive(Debug, Args, Default, PartialEq, Eq)]
pub struct IndexArgs {
    #[arg(long, help = "Path to the workspace config file")]
    pub config: Option<PathBuf>,
    #[arg(long, help = "Override the workspace-local registry path")]
    pub registry: Option<PathBuf>,
    #[arg(long, help = "Override the workspace-local storage directory")]
    pub storage: Option<PathBuf>,
    #[arg(
        long,
        value_enum,
        help = "Override repo depth in the in-memory indexing config"
    )]
    pub depth: Option<DepthArg>,
    #[arg(
        long,
        help = "Write the index JSON payload to this path for release-pipeline archival."
    )]
    pub artifact_path: Option<PathBuf>,
    #[arg(
        long,
        help = "Enforce release-gate policy: require a clean git worktree and assert the summary invariant before emitting an artifact."
    )]
    pub release_gate: bool,
    #[arg(
        long,
        help = "Delete generated index state before rebuilding, recovering corrupt or old-schema state."
    )]
    pub auto_recover: bool,
    #[arg(long, help = "Enter watch mode after indexing completes")]
    pub watch: bool,
}

#[derive(Debug, Serialize)]
struct IndexOutput {
    event: &'static str,
    config_path: String,
    registry_path: String,
    storage_root: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    index_size_bytes: Option<u64>,
    stats: IndexStatsOutput,
    timings: IndexTimingOutput,
    warnings: Vec<String>,
    repos: Vec<IndexedRepoOutput>,
}

#[derive(Debug, Serialize)]
struct IndexStatsOutput {
    total_repos: usize,
    indexed_repos: usize,
    total_files: u64,
    total_symbols: u64,
    total_edges: u64,
    /// Total cross-repo edges — kept for back-compat; equals the sum of
    /// the three split fields below.
    cross_repo_edges: u64,
    /// Edges where both endpoints are real, non-virtual repos.
    true_cross_repo_edges: usize,
    /// Edges whose target is a virtual author-ownership node (`__virtual__`/Author).
    history_ownership_edges: usize,
    /// Edges whose target is a virtual non-author node (`SharedSymbol`, `Route`, …).
    virtual_other_cross_repo_edges: usize,
}

#[derive(Debug, Serialize)]
struct IndexTimingOutput {
    total_wall_ms: u64,
    graph_build_ms: u64,
    parser_augment_ms: u64,
    pack_precompute_ms: u64,
    metadata_persist_ms: u64,
    #[serde(rename = "prepare_total_ms")]
    prepare_total: u64,
    #[serde(rename = "prepare_max_ms")]
    prepare_max: u64,
    #[serde(rename = "writer_storage_commit_total_ms")]
    writer_storage_commit: u64,
    #[serde(rename = "writer_analytics_total_ms")]
    writer_analytics: u64,
    analytics_total_ms: u64,
    analytics_max_ms: u64,
    analytics_recv_wait_total_ms: u64,
    #[serde(rename = "writer_authoritative_count_total_ms")]
    writer_authoritative_count: u64,
    #[serde(rename = "producer_send_wait_total_ms")]
    producer_send_wait_total: u64,
    #[serde(rename = "writer_recv_wait_total_ms")]
    writer_recv_wait: u64,
    search_flush_ms: u64,
    cross_repo_count_ms: u64,
    pack_target_discovery_ms: u64,
    context_pack_cache_clear_ms: u64,
    context_pack_cache_rows_removed: usize,
    durable_sync_ms: u64,
    precompute_ms: u64,
}

#[derive(Debug, Serialize)]
struct IndexedRepoOutput {
    repo: String,
    files: u64,
    symbols: u64,
    edges: u64,
    frameworks: Vec<String>,
    git_analytics_status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    git_analytics_warning: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum RepoAnalyticsStatus {
    Indexed,
    Degraded { warning: String },
}

/// Max number of prepared `RepoIndexPayload`s that may be buffered between
/// the parallel parse phase and the serial write phase.  Capping the channel
/// bounds peak RSS at roughly `PHASE_CHANNEL_DEPTH × per_repo_payload` rather
/// than scaling with the whole workspace.
const PHASE_CHANNEL_DEPTH: usize = 4;

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn format_duration_hh_mm_ss(ms: u64) -> String {
    let total_seconds = ms / 1_000;
    let hours = total_seconds / 3_600;
    let minutes = (total_seconds % 3_600) / 60;
    let seconds = total_seconds % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut divisor = 1_u128;
    let mut unit_idx = 0;
    let bytes = u128::from(bytes);
    while bytes >= divisor * 1024 && unit_idx + 1 < UNITS.len() {
        divisor *= 1024;
        unit_idx += 1;
    }
    if unit_idx == 0 {
        format!("{bytes} {}", UNITS[unit_idx])
    } else {
        let mut whole = bytes / divisor;
        let mut tenth = ((bytes % divisor) * 10 + divisor / 2) / divisor;
        if tenth == 10 {
            whole += 1;
            tenth = 0;
        }
        format!("{whole}.{tenth} {}", UNITS[unit_idx])
    }
}

/// Maximum recursion depth for [`directory_size_bytes`].
///
/// Bounds the stack against pathological / adversarial directory trees. The
/// symlink check at each level prevents loops, but a real tree nested 10k
/// deep would still blow the default 8 MiB thread stack on macOS. 64 levels
/// is well past anything a real source-controlled workspace produces.
const DIR_SIZE_MAX_DEPTH: u32 = 64;

fn directory_size_bytes(path: &Path) -> Result<u64> {
    directory_size_bytes_inner(path, 0)
}

fn directory_size_bytes_inner(path: &Path, depth: u32) -> Result<u64> {
    if depth > DIR_SIZE_MAX_DEPTH {
        tracing::warn!(
            path = %path.display(),
            depth,
            "directory_size_bytes: max depth ({}) exceeded; truncating count",
            DIR_SIZE_MAX_DEPTH,
        );
        return Ok(0);
    }
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("reading metadata for {}", path.display()))?;
    if metadata.file_type().is_symlink() {
        return Ok(0);
    }
    if metadata.is_file() {
        return Ok(metadata.len());
    }
    if !metadata.is_dir() {
        return Ok(0);
    }

    let mut total = 0_u64;
    for entry in fs::read_dir(path).with_context(|| format!("reading {}", path.display()))? {
        let entry = entry.with_context(|| format!("reading entry under {}", path.display()))?;
        total = total.saturating_add(directory_size_bytes_inner(&entry.path(), depth + 1)?);
    }
    Ok(total)
}

fn open_indexer_with_optional_recovery(
    storage_root: &Path,
    registry_path: &Path,
    auto_recover: bool,
    options: IndexingOptions,
    output: &crate::app::Output,
) -> Result<RepoIndexer> {
    if auto_recover {
        return reset_and_reopen_indexer(storage_root, registry_path, options, output);
    }
    match RepoIndexer::open(storage_root, options) {
        Ok(indexer) => Ok(indexer),
        Err(error) => {
            Err(error).with_context(|| format!("opening storage at {}", storage_root.display()))
        }
    }
}

fn reset_and_reopen_indexer(
    storage_root: &Path,
    registry_path: &Path,
    options: IndexingOptions,
    output: &crate::app::Output,
) -> Result<RepoIndexer> {
    clean::reset_index_state(registry_path, storage_root).with_context(|| {
        format!(
            "auto-recover clearing generated state at {} and {}",
            registry_path.display(),
            storage_root.display()
        )
    })?;
    output.line(format!(
        "  {} Rebuilding generated index state from source repositories.",
        style("→").cyan()
    ));
    RepoIndexer::open(storage_root, options)
        .with_context(|| format!("opening storage at {}", storage_root.display()))
}

/// Producer → writer channel payload for the streaming parse→write pipeline.
struct PreparedRepo {
    repo_idx: usize,
    repo_name: String,
    repo_root: PathBuf,
    detected_frameworks: Vec<Framework>,
    payload: RepoIndexPayload,
    prepare_ms: u64,
}

/// Output of the writer thread for a single committed repo.
struct CommittedRepo {
    repo_idx: usize,
    result: WorkspaceRepoResult,
}

#[expect(
    clippy::struct_field_names,
    reason = "shared `repo_` prefix mirrors the AnalyticsRepoResult/CommittedRepo job pairing across the indexing pipeline"
)]
struct AnalyticsJob {
    repo_idx: usize,
    repo_name: String,
    repo_root: PathBuf,
}

struct AnalyticsRepoResult {
    repo_idx: usize,
    status: RepoAnalyticsStatus,
}

#[derive(Clone, Copy)]
struct RepoGraphCounts {
    files: u64,
    symbols: u64,
    edges: u64,
}

#[derive(Default)]
struct ProducerTimings {
    prepare_total: u64,
    prepare_max: u64,
    send_wait_total: u64,
}

#[derive(Default)]
struct WriterTimings {
    storage_commit: u64,
    authoritative_count: u64,
    recv_wait: u64,
}

#[derive(Default)]
struct AnalyticsTimings {
    total: u64,
    max: u64,
    recv_wait: u64,
}

struct PipelineOutput {
    committed: Vec<CommittedRepo>,
    analytics_results: Vec<AnalyticsRepoResult>,
    writer_timings: WriterTimings,
    analytics_timings: AnalyticsTimings,
    producer_timings: ProducerTimings,
}

impl RepoAnalyticsStatus {
    const fn label(&self) -> &'static str {
        match self {
            Self::Indexed => "indexed",
            Self::Degraded { .. } => "degraded",
        }
    }

    fn warning(&self) -> Option<&str> {
        match self {
            Self::Indexed => None,
            Self::Degraded { warning } => Some(warning.as_str()),
        }
    }
}

pub async fn run(app: &AppContext, args: IndexArgs) -> Result<()> {
    let total_start = Instant::now();
    let output = app.output();
    let defaults = app.workspace_paths();
    let config_path = args.config.unwrap_or(defaults.config_path);
    let registry_path = args.registry.unwrap_or(defaults.registry_path);
    let storage_root = args.storage.unwrap_or(defaults.storage_root);
    let artifact_path = args.artifact_path;
    let release_gate = args.release_gate;
    let auto_recover = args.auto_recover;
    let watch = args.watch;

    // A release-gate run must be produced from a clean, committed worktree.
    // Fail fast here rather than emitting an artifact that cannot be reproduced.
    if release_gate && let Some(reason) = release_gate_dirty_reason(&app.workspace_path) {
        bail!(
            "release-gate refused: {reason}. Commit or stash the worktree and rerun, or omit \
             --release-gate for an unsealed run."
        );
    }

    let mut config = GatherStepConfig::from_yaml_file(&config_path)
        .with_context(|| format!("loading {}", config_path.display()))?;
    let configured_repos = config.repos.clone();
    apply_repo_filter(&mut config, app.repo_filter.as_deref())?;
    apply_depth_override(&mut config, args.depth);

    if config.repos.is_empty() {
        bail!("no repos remain after applying filters");
    }

    path_safety::reject_symlinked_generated_state(&app.workspace_path, &storage_root)
        .with_context(|| {
            format!(
                "Generated-state path `{}` failed the symlink check.",
                storage_root.display()
            )
        })?;
    fs::create_dir_all(&storage_root)
        .with_context(|| format!("creating {}", storage_root.display()))?;
    if let Some(parent) = registry_path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    }

    let config_root = config_path
        .parent()
        .map_or_else(|| app.workspace_path.clone(), PathBuf::from);
    config
        .validate_repo_roots_against_config_root(&config_root)
        .with_context(|| format!("validating repo roots under {}", config_root.display()))?;
    info!(
        workspace = %app.workspace_path.display(),
        config = %config_path.display(),
        repos = config.repos.len(),
        "Indexing from directory started.",
    );

    let mut repo_results = Vec::with_capacity(config.repos.len());
    let mut warnings = Vec::new();
    let mut stats = WorkspaceStats {
        total_repos: config.repos.len(),
        ..WorkspaceStats::default()
    };
    let workspace_timestamp = current_unix_timestamp_string();

    let indexer = open_indexer_with_optional_recovery(
        &storage_root,
        &registry_path,
        auto_recover,
        IndexingOptions::from_config(&config),
        &output,
    )?;
    let mut registry = RegistryStore::open(&registry_path)
        .with_context(|| format!("opening {}", registry_path.display()))?;
    let dropped_repos = registry.register_from_config(&config, &config_root)?;
    // Repos that were registered but are no longer in the config must have
    // their graph + search + metadata state purged so subsequent queries do
    // not return rows for repos the operator removed.
    if !dropped_repos.is_empty() {
        for name in &dropped_repos {
            indexer
                .storage()
                .purge_repo(name)
                .with_context(|| format!("purging removed repo `{name}` from generated state"))?;
        }
        output.line(format!(
            "  Removed {} repo(s) from the registry: {}.",
            dropped_repos.len(),
            dropped_repos.join(", ")
        ));
    }

    // Defer all per-batch Tantivy commits to a single end-of-run flush —
    // this collapses ~250 segment commits (one per repo × batch) into 1 and
    // eliminates the `segment_manager "couldn't find segment"` warnings
    // caused by the background merge thread racing per-batch reader reloads.
    let search_deferred_commit = indexer.storage().search().begin_deferred_commit();

    // Both the workspace-level bar and per-repo spinners are registered on the
    // shared MultiProgress so they draw cleanly together and coordinate with
    // tracing log lines routed through the same target.
    let multi = &app.multi_progress;
    let workspace_bar = (!output.is_json()).then(|| {
        let bar = multi.add(ProgressBar::new(config.repos.len() as u64));
        bar.set_style(
            ProgressStyle::with_template(
                " {spinner:.cyan.bold} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len}  {msg}",
            )
            .expect("workspace progress template is valid")
            .progress_chars("█░░")
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏ "),
        );
        bar.enable_steady_tick(std::time::Duration::from_millis(80));
        bar
    });

    // ── Streaming parse → write pipeline ────────────────────────────────────
    // Serial producer prepares one repo at a time on the main thread; a
    // dedicated writer thread drains a bounded `crossbeam-channel` and runs
    // `commit_repo_payload` for each received payload. Git analytics runs on a
    // separate scoped worker fed by the writer after each repo storage commit.
    // This pipelines prepare(N+1) with commit(N) without running multiple
    // whole-repo prepares concurrently, which would split the inner file-level
    // rayon pool and spike peak RSS.
    //
    // Outer repo-level `par_iter()` is explicitly avoided: `prepare_repo_files`
    // wraps its file-level `par_iter()` inside `std::thread::scope(...)`, so
    // rayon workers that claimed outer-repo work get blocked inside a scope
    // that rayon's scheduler cannot see through — the inner par_iter then has
    // no workers left and the whole pool deadlocks.
    info!(
        depth = PHASE_CHANNEL_DEPTH,
        repos = config.repos.len(),
        "workspace index: entering streaming parse→write pipeline"
    );

    // Hold one workspace-level BulkModeGuard for the whole pipeline so redb
    // uses Durability::None for per-repo commits.  The guard's Drop issues a
    // final Immediate-durability commit that fsyncs all accumulated bulk
    // pages before returning — see `BulkModeGuard::drop` in
    // `gather-step-storage::indexer`.
    let workspace_bulk = indexer.begin_workspace_bulk_session();

    let (tx, rx) = kanal::bounded::<PreparedRepo>(PHASE_CHANNEL_DEPTH);
    // One analytics job is produced per repo after its storage commit. Capacity
    // equal to the run's repo count bounds retained jobs while avoiding writer
    // backpressure when analytics is slower than storage commits.
    let analytics_queue_depth = config.repos.len().max(1);
    let (analytics_tx, analytics_rx) = kanal::bounded::<AnalyticsJob>(analytics_queue_depth);
    let (analytics_result_tx, analytics_result_rx) = kanal::unbounded::<AnalyticsRepoResult>();
    let indexer_ref = &indexer;
    let config_ref = &config;
    let config_root_ref = &config_root;
    let workspace_timestamp_ref = &workspace_timestamp;
    let workspace_bar_ref = &workspace_bar;

    let pipeline = std::thread::scope(|scope| -> Result<PipelineOutput> {
        let analytics_worker = scope.spawn(move || -> AnalyticsTimings {
            let mut timings = AnalyticsTimings::default();
            loop {
                let recv_start = Instant::now();
                let Ok(job) = analytics_rx.recv() else {
                    timings.recv_wait = timings.recv_wait.saturating_add(elapsed_ms(recv_start));
                    break;
                };
                timings.recv_wait = timings.recv_wait.saturating_add(elapsed_ms(recv_start));
                let analytics_start = Instant::now();
                let analytics_status = sync_repo_analytics(
                    indexer_ref,
                    &job.repo_name,
                    &job.repo_root,
                    current_unix_timestamp_i64(),
                );
                let analytics_ms = elapsed_ms(analytics_start);
                timings.total = timings.total.saturating_add(analytics_ms);
                timings.max = timings.max.max(analytics_ms);
                if analytics_result_tx
                    .send(AnalyticsRepoResult {
                        repo_idx: job.repo_idx,
                        status: analytics_status,
                    })
                    .is_err()
                {
                    break;
                }
            }
            timings
        });

        // Writer thread: drains the channel, commits repos, and queues git
        // analytics for the dedicated analytics worker.
        let writer = scope.spawn(move || -> Result<(Vec<CommittedRepo>, WriterTimings)> {
            let mut committed = Vec::with_capacity(config_ref.repos.len());
            let mut writer_timings = WriterTimings::default();
            loop {
                let recv_start = Instant::now();
                let Ok(prep) = rx.recv() else {
                    writer_timings.recv_wait = writer_timings
                        .recv_wait
                        .saturating_add(elapsed_ms(recv_start));
                    break;
                };
                writer_timings.recv_wait = writer_timings
                    .recv_wait
                    .saturating_add(elapsed_ms(recv_start));
                let commit_start = Instant::now();
                indexer_ref
                    .commit_repo_payload(prep.payload)
                    .with_context(|| format!("committing repo `{}`", prep.repo_name))?;
                let commit_ms = elapsed_ms(commit_start);
                writer_timings.storage_commit =
                    writer_timings.storage_commit.saturating_add(commit_ms);

                let mut frameworks = prep
                    .detected_frameworks
                    .iter()
                    .copied()
                    .map(framework_label)
                    .collect::<Vec<_>>();
                frameworks.sort();

                info!(
                    repo = %prep.repo_name,
                    path = %prep.repo_root.display(),
                    prepare_ms = prep.prepare_ms,
                    commit_ms,
                    "Indexing from directory finished.",
                );

                let depth_level = config_ref
                    .repos
                    .get(prep.repo_idx)
                    .and_then(|r| r.depth)
                    .unwrap_or(gather_step_core::DepthLevel::Full);

                analytics_tx
                    .send(AnalyticsJob {
                        repo_idx: prep.repo_idx,
                        repo_name: prep.repo_name.clone(),
                        repo_root: prep.repo_root.clone(),
                    })
                    .with_context(|| {
                        format!("queueing git analytics for repo `{}`", prep.repo_name)
                    })?;

                committed.push(CommittedRepo {
                    repo_idx: prep.repo_idx,
                    result: WorkspaceRepoResult {
                        repo: prep.repo_name.clone(),
                        last_indexed_at: Some(workspace_timestamp_ref.clone()),
                        file_count: 0,
                        symbol_count: 0,
                        edge_count: 0,
                        frameworks,
                        depth_level,
                    },
                });

                if let Some(bar) = workspace_bar_ref {
                    bar.inc(1);
                    bar.set_message(prep.repo_name);
                }
            }
            Ok((committed, writer_timings))
        });

        // Producer: serial repo-level loop on the main thread.  Running
        // prepares one at a time lets `prepare_repo_files`'s inner file-level
        // `par_iter()` have the full rayon pool, and the writer thread still
        // overlaps `commit_repo_payload(N)` with `prepare_repo_payload(N+1)`
        // — so the pipeline pipelines at the repo boundary without splitting
        // the rayon pool or inflating peak RSS.  See the top-of-block comment
        // for why outer `par_iter()` is not an option here.
        let mut producer_timings = ProducerTimings::default();
        let producer_result: Result<()> = (|| -> Result<()> {
            for (repo_idx, repo) in config_ref.repos.iter().enumerate() {
                let repo_root = config_root_ref.join(&repo.path);
                let detected_frameworks: Vec<Framework> =
                    detect_frameworks(&repo_root).into_iter().collect();
                if let Some(bar) = workspace_bar_ref {
                    // Show the repo name, not the full path — the workspace
                    // root is already displayed at the top of the run, so
                    // repeating it for every repo is just noise.
                    bar.println(format!(
                        "  {} {}",
                        style("Indexing").cyan().bold(),
                        style(&repo.name).bright()
                    ));
                    bar.set_message(repo.name.clone());
                }
                info!(
                    repo = %repo.name,
                    path = %repo_root.display(),
                    "Indexing from directory started.",
                );
                let prepare_start = Instant::now();
                let payload = indexer_ref
                    .prepare_repo_payload_with_frameworks(
                        &repo.name,
                        &repo_root,
                        &detected_frameworks,
                    )
                    .with_context(|| format!("preparing repo `{}`", repo.name))?;
                let prepare_ms = elapsed_ms(prepare_start);
                producer_timings.prepare_total =
                    producer_timings.prepare_total.saturating_add(prepare_ms);
                producer_timings.prepare_max = producer_timings.prepare_max.max(prepare_ms);
                info!(
                    repo = %repo.name,
                    prepare_ms,
                    "workspace index: producer prepared, handing off to writer"
                );

                let send_start = Instant::now();
                let send_result = tx.send(PreparedRepo {
                    repo_idx,
                    repo_name: repo.name.clone(),
                    repo_root,
                    detected_frameworks,
                    payload,
                    prepare_ms,
                });
                producer_timings.send_wait_total = producer_timings
                    .send_wait_total
                    .saturating_add(elapsed_ms(send_start));
                if send_result.is_err() {
                    // Writer dropped rx (either completed or errored); stop.
                    break;
                }
            }
            Ok(())
        })();

        // Closing the channel signals the writer to drain and exit.
        drop(tx);
        let writer_result = writer
            .join()
            .map_err(|_| anyhow::anyhow!("writer thread panicked"))?;
        let analytics_timings = analytics_worker
            .join()
            .map_err(|_| anyhow::anyhow!("analytics thread panicked"))?;
        // kanal Receiver does not expose `try_iter`; drain explicitly until the
        // analytics worker has dropped its sender, which closes the channel.
        let mut analytics_results = Vec::new();
        while let Ok(result) = analytics_result_rx.recv() {
            analytics_results.push(result);
        }

        // Prefer the writer's error (more specific to storage) over producer's
        // secondary "send failed" symptom.
        match writer_result {
            Err(writer_err) => Err(writer_err),
            Ok((committed, writer_timings)) => {
                producer_result?;
                Ok(PipelineOutput {
                    committed,
                    analytics_results,
                    writer_timings,
                    analytics_timings,
                    producer_timings,
                })
            }
        }
    })?;

    // Apply committed repos to registry / stats / repo_results in config order.
    let PipelineOutput {
        committed,
        analytics_results,
        mut writer_timings,
        analytics_timings,
        producer_timings,
    } = pipeline;
    let analytics_by_repo_idx = analytics_results
        .into_iter()
        .map(|result| (result.repo_idx, result.status))
        .collect::<BTreeMap<_, _>>();
    let mut committed = committed;
    committed.sort_by_key(|c| c.repo_idx);
    let count_start = Instant::now();
    // Recount every CONFIGURED repo, not just the post-filter set. Cross-repo
    // edges mean a partial reindex (`--repo foo`) can change graph totals for
    // sibling repos that were not re-parsed — owner-reset of a foo file may
    // remove edges whose `owner_file` belonged to the OTHER repo. The status
    // command asserts `sum(registry_symbol_count) == sum(graph_node_count)`
    // across all repos, so the registry must be refreshed even for filtered-
    // out repos.
    let final_counts_by_repo = configured_repos
        .iter()
        .map(|repo| {
            count_repo_graph_totals(indexer.storage().graph(), &repo.name)
                .map(|counts| (repo.name.clone(), counts))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    for c in &mut committed {
        if let Some(counts) = final_counts_by_repo.get(&c.result.repo) {
            c.result.file_count = counts.files;
            c.result.symbol_count = counts.symbols;
            c.result.edge_count = counts.edges;
        }
    }
    writer_timings.authoritative_count = writer_timings
        .authoritative_count
        .saturating_add(elapsed_ms(count_start));
    let committed_repo_names = committed
        .iter()
        .map(|c| c.result.repo.clone())
        .collect::<BTreeSet<_>>();
    for c in committed {
        let analytics_status = analytics_by_repo_idx
            .get(&c.repo_idx)
            .cloned()
            .unwrap_or_else(|| RepoAnalyticsStatus::Degraded {
                warning: format!(
                    "repo `{}`: git analytics did not report a result; continuing without ownership and co-change analytics",
                    c.result.repo
                ),
            });
        let analytics_warning = analytics_status.warning().map(ToOwned::to_owned);
        if let Some(warning) = analytics_warning.clone() {
            warnings.push(warning);
        }
        registry.update_repo_metadata(
            &c.result.repo,
            RepoIndexMetadata {
                last_indexed_at: c.result.last_indexed_at.clone(),
                file_count: c.result.file_count,
                symbol_count: c.result.symbol_count,
                frameworks: c.result.frameworks.clone(),
                depth_level: c.result.depth_level,
            },
        )?;

        stats.indexed_repos += 1;
        stats.total_files += c.result.file_count;
        stats.total_symbols += c.result.symbol_count;
        stats.total_edges += c.result.edge_count;
        repo_results.push(IndexedRepoOutput {
            repo: c.result.repo.clone(),
            files: c.result.file_count,
            symbols: c.result.symbol_count,
            edges: c.result.edge_count,
            frameworks: c.result.frameworks.clone(),
            git_analytics_status: analytics_status.label(),
            git_analytics_warning: analytics_warning,
        });
    }
    // For repos that did not commit through the indexer this run (filtered
    // out, or had no indexable sources), still refresh the registry's count
    // metadata using the freshly computed totals so it stays consistent with
    // the live graph state — partial reindexes can shift sibling-repo counts
    // through cross-repo edges (see `final_counts_by_repo` rationale above).
    for repo in &configured_repos {
        if committed_repo_names.contains(&repo.name) {
            continue;
        }
        let Some(counts) = final_counts_by_repo.get(&repo.name) else {
            continue;
        };
        let Some(existing) = registry.registry().repo(&repo.name).cloned() else {
            continue;
        };
        registry.update_repo_metadata(
            &repo.name,
            RepoIndexMetadata {
                last_indexed_at: existing.last_indexed_at,
                file_count: counts.files,
                symbol_count: counts.symbols,
                frameworks: existing.frameworks,
                depth_level: existing.depth_level,
            },
        )?;
    }

    let finalization_bar = (!output.is_json()).then(|| {
        let bar = multi.add(ProgressBar::new_spinner());
        bar.set_style(
            ProgressStyle::with_template("  {spinner:.cyan.bold} {msg}")
                .expect("finalization spinner template is valid")
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏ "),
        );
        bar.enable_steady_tick(std::time::Duration::from_millis(80));
        bar
    });

    // After all repos are indexed, perform the single Tantivy flush that
    // pairs with `set_deferred_commit(true)` above. Without this the
    // search index would remain entirely unwritten.
    if let Some(bar) = &finalization_bar {
        bar.set_message(SEARCH_FLUSH_MESSAGE);
    }
    let search_flush_start = Instant::now();
    indexer
        .storage()
        .search()
        .flush()
        .context("finalizing search index flush")?;
    search_deferred_commit.mark_flushed();
    let search_flush_ms = elapsed_ms(search_flush_start);
    info!(
        flush_ms = search_flush_ms,
        "stage timing: final search flush complete",
    );

    // Count cross-repo edges in a single EDGES-table scan (one read txn,
    // one table scan, small in-memory node_id→repo_id cache) — replaces the
    // previous nested `for NodeKind × nodes_by_type × get_outgoing × get_node`
    // traversal that opened ~270K+ read transactions on the full monorepo.
    //
    // Also reads authoritative `total_edges` from the graph, overriding the
    // per-batch accumulator above which only sees the in-flight write delta
    // (zero on a warm re-run).
    if let Some(bar) = &finalization_bar {
        bar.set_message(CROSS_REPO_COUNT_MESSAGE);
    }
    let cross_repo_start = Instant::now();
    let (true_cross_repo_edges, history_ownership_edges, virtual_other_cross_repo_edges) = {
        let graph = indexer.storage().graph();
        let EdgeCountSummary {
            total_edges: total_edges_in_graph,
            cross_repo_edges,
            true_cross_repo_edges,
            history_ownership_edges,
            virtual_other_cross_repo_edges,
        } = graph
            .count_edge_summary()
            .context("finalizing workspace edge summary")?;
        stats.cross_repo_edges = cross_repo_edges;
        stats.total_edges = u64::try_from(total_edges_in_graph).unwrap_or(u64::MAX);
        (
            true_cross_repo_edges,
            history_ownership_edges,
            virtual_other_cross_repo_edges,
        )
    };
    enforce_summary_invariant(&stats)?;
    let cross_repo_count_ms = elapsed_ms(cross_repo_start);
    info!(
        count_ms = cross_repo_count_ms,
        cross_repo_edges = stats.cross_repo_edges,
        true_cross_repo_edges,
        history_ownership_edges,
        virtual_other_cross_repo_edges,
        "stage timing: cross-repo edge count complete",
    );

    // Prefer hot (target, mode) pairs recorded in previous MCP sessions.
    // Fall back to the static heuristic when the log does not
    // yet cover the full quota — typical for a cold workspace.
    let pack_target_discovery_start = Instant::now();
    let hot_pack_targets = indexer
        .storage()
        .metadata()
        .top_pack_call_log(HOT_PACK_WHITELIST_LIMIT)
        .context("loading hot pack whitelist")
        .unwrap_or_default();
    let precomputed_pack_targets =
        collect_precomputed_pack_targets(indexer.storage().graph(), &config)
            .context("collecting precomputed context-pack targets")?;
    let pack_target_discovery_ms = elapsed_ms(pack_target_discovery_start);
    // The pipeline joins the analytics worker before returning, so no
    // analytics writes can continue past this durable marker. Clearing the
    // derived pack cache before bulk-mode commit would let concurrent pack
    // requests repopulate cache entries from a not-yet-durable graph.
    let durable_sync_start = Instant::now();
    drop(workspace_bulk);
    let durable_sync_ms = elapsed_ms(durable_sync_start);
    info!(durable_sync_ms, "stage timing: graph durable sync complete",);
    // Per-batch graph commits synchronously invalidate packs that depend on
    // changed targets. Do not clear the whole cache here; unrelated packs can
    // survive the index run and generation checks still reject stale rows.
    let context_pack_cache_rows_removed = 0;
    let context_pack_cache_clear_ms = 0;
    info!(
        context_pack_cache_clear_ms,
        context_pack_cache_rows_removed, "stage timing: global context-pack cache clear skipped",
    );
    drop(indexer);

    // Wipe review artifacts AFTER `drop(indexer)` (which fsyncs the new
    // baseline) but BEFORE precompute. Order matters:
    //   1. drop(indexer)            → new workspace index is durable
    //   2. clean_all_for_workspace  → stale review caches gone (their
    //                                 baseline SHAs no longer match)
    //   3. precompute_context_packs → re-populate against the new index
    // Doing the wipe AFTER precompute (the prior order) left a window where
    // a process kill between drop(indexer) and the wipe would leave the new
    // index durable on disk while old review caches still pointed at the
    // now-stale baseline. `clean_all_for_workspace` skips live InProgress
    // artifacts so concurrent `pr-review` runs are not deleted.
    match clean_all_for_workspace(&app.workspace_path) {
        Ok(report) => {
            if report.removed_count > 0 {
                output.line(format!(
                    "  wiped {} review artifact(s) (full reindex invalidates their baseline)",
                    report.removed_count,
                ));
            }
            if report.skipped_count > 0 {
                output.line(format!(
                    "  skipped {} review artifact(s) during cleanup; inspect the warning logs or run `gather-step pr-review clean --all`",
                    report.skipped_count,
                ));
            }
        }
        Err(e) => {
            warn!(
                error = %e,
                "Could not wipe review artifacts after a full reindex; continuing."
            );
        }
    }

    let precompute_start = Instant::now();
    let precompute_pack_count =
        precompute_pack_call_count(&hot_pack_targets, &precomputed_pack_targets);
    if let Some(bar) = &finalization_bar {
        bar.set_message(format_pack_precompute_message(precompute_pack_count));
    }
    precompute_context_packs(
        &registry_path,
        &storage_root,
        &hot_pack_targets,
        &precomputed_pack_targets,
    )
    .context("precomputing context packs")?;
    let precompute_ms = elapsed_ms(precompute_start);
    info!(
        precompute_ms,
        pack_targets = precompute_pack_count,
        "stage timing: precompute context packs complete",
    );

    if let Some(bar) = &finalization_bar {
        bar.finish_and_clear();
    }
    if let Some(bar) = &workspace_bar {
        bar.finish_with_message("Workspace indexing complete.");
    }

    let total_wall_ms = elapsed_ms(total_start);
    let should_measure_index_size = output.is_json() || artifact_path.is_some();
    let index_size_bytes =
        if should_measure_index_size {
            Some(directory_size_bytes(&storage_root).with_context(|| {
                format!("measuring index size under {}", storage_root.display())
            })?)
        } else {
            None
        };
    let graph_build_ms = writer_timings.storage_commit;
    let parser_augment_ms = producer_timings.prepare_total;
    let pack_precompute_ms = precompute_ms;
    let metadata_persist_ms = context_pack_cache_clear_ms;
    let payload = IndexOutput {
        event: "index_completed",
        config_path: config_path.display().to_string(),
        registry_path: registry_path.display().to_string(),
        storage_root: storage_root.display().to_string(),
        index_size_bytes,
        stats: IndexStatsOutput {
            total_repos: stats.total_repos,
            indexed_repos: stats.indexed_repos,
            total_files: stats.total_files,
            total_symbols: stats.total_symbols,
            total_edges: stats.total_edges,
            cross_repo_edges: stats.cross_repo_edges,
            true_cross_repo_edges,
            history_ownership_edges,
            virtual_other_cross_repo_edges,
        },
        timings: IndexTimingOutput {
            total_wall_ms,
            graph_build_ms,
            parser_augment_ms,
            pack_precompute_ms,
            metadata_persist_ms,
            prepare_total: producer_timings.prepare_total,
            prepare_max: producer_timings.prepare_max,
            writer_storage_commit: writer_timings.storage_commit,
            writer_analytics: 0,
            analytics_total_ms: analytics_timings.total,
            analytics_max_ms: analytics_timings.max,
            analytics_recv_wait_total_ms: analytics_timings.recv_wait,
            writer_authoritative_count: writer_timings.authoritative_count,
            producer_send_wait_total: producer_timings.send_wait_total,
            writer_recv_wait: writer_timings.recv_wait,
            search_flush_ms,
            cross_repo_count_ms,
            pack_target_discovery_ms,
            context_pack_cache_clear_ms,
            context_pack_cache_rows_removed,
            durable_sync_ms,
            precompute_ms,
        },
        warnings,
        repos: repo_results,
    };
    info!(
        workspace = %app.workspace_path.display(),
        storage_root = %storage_root.display(),
        duration_ms = total_wall_ms,
        ?index_size_bytes,
        "Indexing from directory finished.",
    );

    output.emit(&payload)?;
    let repo_label = if payload.stats.indexed_repos == 1 {
        "repository"
    } else {
        "repositories"
    };
    output.line(format!(
        "\n  {} {} {}  {}",
        style("✓ Indexed").green().bold(),
        style(payload.stats.indexed_repos).cyan().bold(),
        repo_label,
        &payload.storage_root
    ));
    output.line(format!(
        "    {} files  {} symbols  {} edges  {} cross-repo",
        style(payload.stats.total_files).cyan().bold(),
        style(payload.stats.total_symbols).cyan().bold(),
        style(payload.stats.total_edges).cyan().bold(),
        style(payload.stats.cross_repo_edges).cyan().bold()
    ));
    if let Some(index_size_bytes) = payload.index_size_bytes {
        output.line(format!(
            "    Time: {}  Index size: {}",
            style(format_duration_hh_mm_ss(payload.timings.total_wall_ms))
                .cyan()
                .bold(),
            style(format_bytes(index_size_bytes)).cyan().bold(),
        ));
    } else {
        output.line(format!(
            "    Time: {}",
            style(format_duration_hh_mm_ss(payload.timings.total_wall_ms))
                .cyan()
                .bold(),
        ));
    }
    for warning in &payload.warnings {
        output.line(format!("  {} {warning}", style("Warning:").yellow().bold()));
    }

    // When `--artifact-path` is set, persist the IndexOutput payload so release
    // automation can archive it alongside the commit SHA.
    if let Some(path) = artifact_path.as_deref() {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating artifact directory {}", parent.display()))?;
        }
        let artifact =
            serde_json::to_vec_pretty(&payload).context("serializing index artifact payload")?;
        fs::write(path, artifact)
            .with_context(|| format!("writing artifact to {}", path.display()))?;
        output.line(format!("Release-gate artifact: {}", path.display()));
    }

    if watch || should_prompt_for_watch(app)? {
        return crate::commands::watch::run(app, crate::commands::watch::WatchArgs::default())
            .await;
    }

    Ok(())
}

fn should_prompt_for_watch(app: &AppContext) -> Result<bool> {
    if !app.is_interactive() {
        return Ok(false);
    }

    let mut stdout = io::stdout().lock();
    write!(stdout, "Start watching for changes? [y/N] ")?;
    stdout.flush()?;

    let mut answer = String::new();
    io::stdin().read_line(&mut answer)?;
    Ok(matches!(answer.trim(), "y" | "Y" | "yes" | "YES" | "Yes"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum GitWorktreeState {
    Clean,
    Dirty(String),
    NotGit,
}

fn worktree_state(workspace: &std::path::Path) -> GitWorktreeState {
    use std::process::Command;
    let Ok(output) = Command::new("git")
        .arg("-C")
        .arg(workspace)
        .args(["status", "--porcelain"])
        .output()
    else {
        return GitWorktreeState::NotGit;
    };
    if !output.status.success() {
        return GitWorktreeState::NotGit;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let dirty_lines: Vec<_> = stdout.lines().filter(|line| !line.is_empty()).collect();
    if dirty_lines.is_empty() {
        GitWorktreeState::Clean
    } else {
        let sample: Vec<_> = dirty_lines.iter().take(3).copied().collect();
        GitWorktreeState::Dirty(format!(
            "git worktree is dirty ({} path(s) unstaged/uncommitted, e.g. {:?})",
            dirty_lines.len(),
            sample
        ))
    }
}

/// Detect whether the workspace looks like a dirty git worktree. Returns
/// `Some(reason)` only for dirty git worktrees; non-git workspaces are handled
/// by the release-gate policy wrapper below.
fn worktree_is_dirty(workspace: &std::path::Path) -> Option<String> {
    match worktree_state(workspace) {
        GitWorktreeState::Dirty(reason) => Some(reason),
        GitWorktreeState::Clean | GitWorktreeState::NotGit => None,
    }
}

fn build_checkout_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .map(std::path::Path::to_path_buf)
        .expect("gather-step-cli crate should live under the workspace root")
}

fn release_gate_dirty_reason(workspace: &std::path::Path) -> Option<String> {
    release_gate_dirty_reason_with_build_root(workspace, Some(build_checkout_root().as_path()))
}

fn release_gate_dirty_reason_with_build_root(
    workspace: &std::path::Path,
    build_root: Option<&std::path::Path>,
) -> Option<String> {
    let mut reasons = Vec::new();
    match worktree_state(workspace) {
        GitWorktreeState::Clean => {}
        GitWorktreeState::Dirty(reason) => reasons.push(format!("workspace {reason}")),
        GitWorktreeState::NotGit => reasons.push("workspace is not a git repository".to_owned()),
    }
    if let Some(build_root) = build_root
        && build_root != workspace
        && let Some(reason) = worktree_is_dirty(build_root)
    {
        reasons.push(format!(
            "build checkout at `{}` {reason}",
            build_root.display()
        ));
    }
    (!reasons.is_empty()).then(|| reasons.join("; "))
}

/// Enforces the fresh-index summary invariant: the accumulated per-repo
/// counts must equal the authoritative graph counts. A mismatch indicates a
/// counting regression and fails the release gate. Concretely, when the
/// workspace graph contains any edges, it must also contain at least one file
/// node and at least one symbol node — a violation blocks the run rather than
/// silently emitting misleading totals.
fn enforce_summary_invariant(stats: &WorkspaceStats) -> Result<()> {
    if stats.total_edges > 0 && (stats.total_files == 0 || stats.total_symbols == 0) {
        bail!(
            "summary invariant violated: total_edges={} > 0 but total_files={} or total_symbols={} is 0; \
             this indicates a summary accounting regression",
            stats.total_edges,
            stats.total_files,
            stats.total_symbols,
        );
    }
    Ok(())
}

fn count_repo_graph_totals(graph: &impl GraphStore, repo: &str) -> Result<RepoGraphCounts> {
    let total_nodes = graph
        .count_nodes_by_repo(repo)
        .with_context(|| format!("counting nodes for repo `{repo}`"))?;
    let file_nodes = graph
        .count_nodes_by_repo_and_kind(repo, NodeKind::File)
        .with_context(|| format!("counting file nodes for repo `{repo}`"))?;
    let edge_count = graph
        .count_edges_by_owner_repo(repo)
        .with_context(|| format!("counting edges for repo `{repo}`"))?;
    Ok(RepoGraphCounts {
        files: u64::try_from(file_nodes).unwrap_or(u64::MAX),
        symbols: u64::try_from(total_nodes).unwrap_or(u64::MAX),
        edges: edge_count,
    })
}

const PRECOMPUTED_PACK_TARGETS_PER_REPO: usize = 2;
const PRECOMPUTED_PACK_TARGET_LIMIT: usize = 8;
/// Upper bound on `(target, mode)` pairs pulled from the MCP pack call log
/// when deciding which packs to precompute at index finalize.
const HOT_PACK_WHITELIST_LIMIT: usize = 32;

fn collect_precomputed_pack_targets(
    graph: &impl GraphStore,
    config: &GatherStepConfig,
) -> Result<Vec<String>> {
    let mut targets = Vec::new();
    for repo in &config.repos {
        let remaining = PRECOMPUTED_PACK_TARGET_LIMIT.saturating_sub(targets.len());
        if remaining == 0 {
            break;
        }
        let per_repo_limit = PRECOMPUTED_PACK_TARGETS_PER_REPO.min(remaining);
        let mut candidates = Vec::with_capacity(per_repo_limit);
        for node in graph.nodes_by_repo(&repo.name)? {
            if node.kind.is_search_indexable()
                && !node.is_virtual
                && node.kind != NodeKind::File
                && !node.name.is_empty()
            {
                insert_precompute_candidate(&mut candidates, node, per_repo_limit);
            }
        }
        targets.extend(candidates.into_iter().map(|node| encode_node_id(node.id)));
    }
    Ok(targets)
}

fn insert_precompute_candidate(candidates: &mut Vec<NodeData>, node: NodeData, limit: usize) {
    if limit == 0 || candidates.iter().any(|candidate| candidate.id == node.id) {
        return;
    }
    let insert_at = candidates
        .iter()
        .position(|candidate| precompute_candidate_cmp(&node, candidate).is_lt())
        .unwrap_or(candidates.len());
    if insert_at >= limit {
        return;
    }
    candidates.insert(insert_at, node);
    candidates.truncate(limit);
}

fn precompute_candidate_cmp(left: &NodeData, right: &NodeData) -> Ordering {
    left.file_path
        .cmp(&right.file_path)
        .then(
            left.span
                .as_ref()
                .map(|span| span.line_start)
                .cmp(&right.span.as_ref().map(|span| span.line_start)),
        )
        .then(left.name.cmp(&right.name))
        .then(left.id.cmp(&right.id))
}

fn precompute_context_packs(
    registry_path: &std::path::Path,
    storage_root: &std::path::Path,
    hot_targets: &[gather_step_storage::PackCallLogEntry],
    fallback_targets: &[String],
) -> Result<()> {
    let ctx = McpContext::open(McpServerConfig::new(
        registry_path.to_path_buf(),
        storage_root.join("graph.redb"),
    ))?;

    // Warm only the exact `(target, mode)` pairs recorded in the MCP call log.
    // Each entry represents real usage, so each tool call here is directly
    // amortized by the next cache hit.
    for entry in hot_targets {
        let request = ModePackRequest {
            budget_bytes: Some(18_000),
            depth: Some(2),
            limit: Some(6),
            repo: None,
            target: entry.target.clone(),
        };
        match entry.mode.as_str() {
            mode if !is_precomputable_pack_mode(mode) => {}
            "planning" => {
                planning_pack_tool(&ctx, request)?;
            }
            "debug" => {
                debug_pack_tool(&ctx, request)?;
            }
            "fix" => {
                fix_pack_tool(&ctx, request)?;
            }
            "review" => {
                review_pack_tool(&ctx, request)?;
            }
            "change_impact" => {
                change_impact_pack_tool(&ctx, request)?;
            }
            _ => unreachable!("precomputable mode guard should cover all known modes"),
        }
    }

    // Cold-workspace fallback: only use the static heuristic target set when
    // there is no prior MCP usage. Once the call log exists, warming the exact
    // hot `(target, mode)` pairs is the contract; the fallback set would add
    // unrelated work and dilute the benefit of the whitelist.
    if should_precompute_fallback_targets(hot_targets) {
        for target in fallback_targets {
            let request = ModePackRequest {
                budget_bytes: Some(18_000),
                depth: Some(2),
                limit: Some(6),
                repo: None,
                target: target.clone(),
            };
            planning_pack_tool(&ctx, request.clone())?;
            debug_pack_tool(&ctx, request.clone())?;
            fix_pack_tool(&ctx, request.clone())?;
            review_pack_tool(&ctx, request.clone())?;
            change_impact_pack_tool(&ctx, request)?;
        }
    }
    Ok(())
}

fn should_precompute_fallback_targets(
    hot_targets: &[gather_step_storage::PackCallLogEntry],
) -> bool {
    hot_targets.is_empty()
}

fn is_precomputable_pack_mode(mode: &str) -> bool {
    matches!(
        mode,
        "planning" | "debug" | "fix" | "review" | "change_impact"
    )
}

fn precompute_pack_call_count(
    hot_targets: &[gather_step_storage::PackCallLogEntry],
    fallback_targets: &[String],
) -> usize {
    let hot_count = hot_targets
        .iter()
        .filter(|entry| is_precomputable_pack_mode(&entry.mode))
        .count();
    if hot_count > 0 {
        hot_count
    } else {
        fallback_targets.len() * 5
    }
}

fn apply_repo_filter(config: &mut GatherStepConfig, repo_filter: Option<&str>) -> Result<()> {
    let Some(repo_filter) = repo_filter else {
        return Ok(());
    };

    let original_len = config.repos.len();
    config.repos.retain(|repo| repo.name == repo_filter);
    config.allow_listed_repos.retain(|repo| repo == repo_filter);

    if config.repos.is_empty() {
        bail!("repo `{repo_filter}` was not found in the workspace config");
    }

    if original_len != config.repos.len() {
        config.validate()?;
    }

    Ok(())
}

fn apply_depth_override(config: &mut GatherStepConfig, depth: Option<DepthArg>) {
    let Some(depth) = depth else {
        return;
    };

    let depth = match depth {
        DepthArg::Level1 => gather_step_core::DepthLevel::Level1,
        DepthArg::Level2 => gather_step_core::DepthLevel::Level2,
        DepthArg::Level3 => gather_step_core::DepthLevel::Level3,
        DepthArg::Full => gather_step_core::DepthLevel::Full,
    };

    for repo in &mut config.repos {
        repo.depth = Some(depth);
    }
}

fn framework_label(framework: Framework) -> String {
    match framework {
        Framework::NestJs => "nestjs",
        Framework::Mongoose => "mongoose",
        Framework::NextJs => "nextjs",
        Framework::Tailwind => "tailwind",
        Framework::Prisma => "prisma",
        Framework::Drizzle => "drizzle",
        Framework::TypeOrm => "typeorm",
        Framework::React => "react",
        Framework::ReactRouter => "react_router",
        Framework::ReactHookForm => "react_hook_form",
        Framework::Storybook => "storybook",
        Framework::Azure => "azure",
        Framework::Redux => "redux",
        Framework::Zustand => "zustand",
        Framework::LaunchDarkly => "launchdarkly",
        Framework::FastApi => "fastapi",
        Framework::FrontendHooks => "frontend_hooks",
    }
    .to_owned()
}

fn current_unix_timestamp_string() -> String {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or_else(
        |_| "0".to_owned(),
        |duration| duration.as_secs().to_string(),
    )
}

fn current_unix_timestamp_i64() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_secs()).ok())
        .unwrap_or_default()
}

fn sync_repo_analytics(
    indexer: &RepoIndexer,
    repo: &str,
    repo_root: &std::path::Path,
    computed_at_unix: i64,
) -> RepoAnalyticsStatus {
    let git_indexer = GitHistoryIndexer::new(
        GitRepoSource::from_path(repo_root.to_path_buf()),
        repo.to_owned(),
    );
    match git_indexer.sync(indexer.storage().metadata(), computed_at_unix) {
        Ok(
            HistorySyncOutcome::NoChange { .. }
            | HistorySyncOutcome::Incremental { .. }
            | HistorySyncOutcome::FullRebuild { .. }
            | HistorySyncOutcome::HistoryRewriteFallback { .. },
        ) => {}
        Err(error) => {
            let warning = format!(
                "repo `{repo}`: git analytics unavailable; continuing without ownership and co-change analytics ({error})"
            );
            warn!(
                repo,
                error = %error,
                warning = %warning,
                "git analytics unavailable; continuing with structural indexing only"
            );
            return RepoAnalyticsStatus::Degraded { warning };
        }
    }

    if let Err(error) = refresh_repo_intelligence(
        indexer.storage().graph(),
        indexer.storage().metadata(),
        repo,
        computed_at_unix,
        &RepoIntelligenceOptions::default(),
    ) {
        let warning = format!(
            "repo `{repo}`: repo intelligence refresh unavailable; continuing without ownership and co-change analytics ({error})"
        );
        warn!(
            repo,
            error = %error,
            warning = %warning,
            "repo intelligence refresh unavailable; continuing with structural indexing only"
        );
        return RepoAnalyticsStatus::Degraded { warning };
    }

    RepoAnalyticsStatus::Indexed
}

/// Decides whether a per-repo progress event should drive numeric bar progress.
/// Traverse events are emitted pre-aggregated (`processed == total`), so
/// letting them set length and position would immediately saturate the bar and
/// suppress the spinner animation for the entire traversal phase.
///
/// Currently only used by unit tests; the repo-parallel pipeline emits its own
/// phase-aware progress events.  Retained for potential reuse.
#[cfg(test)]
fn should_update_numeric_progress(phase: &str) -> bool {
    phase != "traverse"
}

const SEARCH_FLUSH_MESSAGE: &str = "Flushing search index...";
const CROSS_REPO_COUNT_MESSAGE: &str = "Counting cross-repo edges...";

fn format_pack_precompute_message(count: usize) -> String {
    format!("Precomputing {count} context packs...")
}

#[cfg(test)]
mod tests {
    use gather_step_core::{NodeData, NodeKind, SourceSpan, WorkspaceStats, node_id};
    use gather_step_storage::PackCallLogEntry;

    use super::{
        CROSS_REPO_COUNT_MESSAGE, SEARCH_FLUSH_MESSAGE, enforce_summary_invariant, format_bytes,
        format_duration_hh_mm_ss, format_pack_precompute_message, insert_precompute_candidate,
        precompute_pack_call_count, should_precompute_fallback_targets,
        should_update_numeric_progress,
    };

    #[test]
    fn traverse_phase_does_not_drive_numeric_progress() {
        assert!(!should_update_numeric_progress("traverse"));
    }

    #[test]
    fn non_traverse_phases_drive_numeric_progress() {
        assert!(should_update_numeric_progress("parse"));
        assert!(should_update_numeric_progress("write"));
    }

    #[test]
    fn finalization_messages_remain_descriptive() {
        assert_eq!(SEARCH_FLUSH_MESSAGE, "Flushing search index...");
        assert_eq!(CROSS_REPO_COUNT_MESSAGE, "Counting cross-repo edges...");
        assert_eq!(
            format_pack_precompute_message(47),
            "Precomputing 47 context packs..."
        );
    }

    #[test]
    fn final_summary_formatters_are_human_readable() {
        assert_eq!(format_duration_hh_mm_ss(3_723_000), "01:02:03");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1_536), "1.5 KB");
    }

    #[test]
    fn fallback_precompute_runs_for_cold_workspaces_only() {
        assert!(should_precompute_fallback_targets(&[]));
        assert!(!should_precompute_fallback_targets(&[PackCallLogEntry {
            target: "target-a".to_owned(),
            mode: "planning".to_owned(),
            call_count: 3,
            last_called_at: 42,
        }]));
    }

    #[test]
    fn precompute_pack_count_tracks_actual_work() {
        let hot_targets = vec![
            PackCallLogEntry {
                target: "target-a".to_owned(),
                mode: "planning".to_owned(),
                call_count: 3,
                last_called_at: 42,
            },
            PackCallLogEntry {
                target: "legacy".to_owned(),
                mode: "legacy_mode".to_owned(),
                call_count: 9,
                last_called_at: 50,
            },
        ];
        assert_eq!(
            precompute_pack_call_count(&hot_targets, &["fallback".to_owned()]),
            1
        );
        assert_eq!(
            precompute_pack_call_count(&[], &["a".to_owned(), "b".to_owned()]),
            10
        );
    }

    #[test]
    fn precompute_candidate_selection_keeps_stable_top_k() {
        let mut candidates = Vec::new();
        insert_precompute_candidate(&mut candidates, candidate_node("later", "src/z.ts", 1), 2);
        insert_precompute_candidate(&mut candidates, candidate_node("second", "src/a.ts", 20), 2);
        insert_precompute_candidate(&mut candidates, candidate_node("first", "src/a.ts", 10), 2);
        insert_precompute_candidate(&mut candidates, candidate_node("first", "src/a.ts", 10), 2);

        let names = candidates
            .into_iter()
            .map(|node| node.name)
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["first", "second"]);
    }

    fn candidate_node(name: &str, file_path: &str, line_start: u32) -> NodeData {
        NodeData {
            id: node_id("repo", file_path, NodeKind::Function, name),
            kind: NodeKind::Function,
            repo: "repo".to_owned(),
            file_path: file_path.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(name.to_owned()),
            external_id: None,
            signature: None,
            visibility: None,
            span: Some(SourceSpan {
                line_start,
                line_len: 0,
                column_start: 0,
                column_len: 0,
            }),
            is_virtual: false,
        }
    }

    fn stats(files: u64, symbols: u64, edges: u64) -> WorkspaceStats {
        WorkspaceStats {
            total_repos: 1,
            indexed_repos: 1,
            total_files: files,
            total_symbols: symbols,
            total_edges: edges,
            ..WorkspaceStats::default()
        }
    }

    #[test]
    fn summary_invariant_passes_when_all_zero() {
        // A genuinely empty workspace must not trip the invariant — there are
        // no edges, so the implication is vacuously true.
        enforce_summary_invariant(&stats(0, 0, 0)).expect("zero edges is allowed");
    }

    #[test]
    fn summary_invariant_passes_when_files_and_symbols_present_with_edges() {
        // Standard cold-index outcome: edges, files, and symbols all non-zero.
        enforce_summary_invariant(&stats(10, 50, 100)).expect("populated graph is allowed");
    }

    #[test]
    fn summary_invariant_fails_when_edges_present_but_files_zero() {
        // Reproduces the V1.10 benchmark regression: edges report non-zero
        // while files report zero. Fail loudly instead of emitting a
        // misleading summary.
        let err = enforce_summary_invariant(&stats(0, 50, 100))
            .expect_err("file count of 0 with edges must violate the invariant");
        assert!(err.to_string().contains("summary invariant violated"));
    }

    #[test]
    fn summary_invariant_fails_when_edges_present_but_symbols_zero() {
        let err = enforce_summary_invariant(&stats(10, 0, 100))
            .expect_err("symbol count of 0 with edges must violate the invariant");
        assert!(err.to_string().contains("summary invariant violated"));
    }

    #[test]
    fn summary_invariant_passes_when_no_edges_even_if_files_or_symbols_zero() {
        // Edge-free workspace: invariant cannot be triggered regardless of
        // the file/symbol totals.
        enforce_summary_invariant(&stats(0, 0, 0)).expect("0/0/0 is allowed");
        enforce_summary_invariant(&stats(5, 0, 0)).expect("0 edges allows 0 symbols");
        enforce_summary_invariant(&stats(0, 5, 0)).expect("0 edges allows 0 files");
    }

    #[test]
    fn worktree_is_dirty_returns_none_for_non_git_paths() {
        // Outside a git repo (or when git cannot observe the path), the
        // release-gate policy is advisory — we do not block the run.
        let tmp = std::env::temp_dir().join(format!(
            "gather-step-release-gate-non-git-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|dur| dur.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&tmp).expect("tmp dir");
        let result = super::worktree_is_dirty(&tmp);
        let _ = std::fs::remove_dir_all(&tmp);
        assert!(
            result.is_none(),
            "non-git workspace must not block the release gate; got {result:?}"
        );
    }

    #[test]
    fn release_gate_dirty_reason_returns_none_for_non_git_paths() {
        let tmp = std::env::temp_dir().join(format!(
            "gather-step-release-gate-check-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|dur| dur.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&tmp).expect("tmp dir");
        let result = super::release_gate_dirty_reason_with_build_root(&tmp, Some(&tmp));
        let _ = std::fs::remove_dir_all(&tmp);
        assert_eq!(
            result.as_deref(),
            Some("workspace is not a git repository"),
            "release-gate runs must reject non-git workspaces with a stable reason"
        );
    }
}
