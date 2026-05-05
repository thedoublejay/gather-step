#![forbid(unsafe_code)]

//! PR-oracle benchmark harness.
//!
//! Measures indexer precision and recall against real merged-PR file sets from
//! a git repository, replacing subjective confidence labels with objective
//! per-PR F1 scores.
//!
//! Three subcommands are exposed through [`run`]:
//!
//! - `build-sample` — walk git history, stratify-sample merged PRs, write
//!   `pr_sample.json`.
//! - `score` — for each PR in the sample, check out the pre-merge commit, run
//!   the indexer, and compute precision / recall / F1.
//! - `compare` — diff two score artifact JSON files and emit a markdown report.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    process::Command as ProcessCommand,
    sync::OnceLock,
};

use anyhow::Context;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use clap::{Args, Subcommand};
use gather_step_core::GatherStepConfig;
use gix::{bstr::ByteSlice, revision::walk::Sorting, traverse::commit::simple::CommitTimeOrder};
use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::compare::redact_local_paths;

// ─── High-bar gate constants ──────────────────────────────────────────────────

/// Minimum median F1 required for the `--gate-high` flag to pass.
const GATE_HIGH_MIN_F1: f64 = 0.75;
/// Minimum median recall required for the `--gate-high` flag to pass.
const GATE_HIGH_MIN_RECALL: f64 = 0.70;

// ─── CLI definitions ──────────────────────────────────────────────────────────

/// Top-level `pr-oracle` subcommand.
#[derive(Debug, Args)]
pub struct PrOracleArgs {
    #[command(subcommand)]
    pub subcommand: PrOracleSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum PrOracleSubcommand {
    /// Walk git history, stratify-sample merged PRs, and write a sample JSON.
    BuildSample(BuildSampleArgs),
    /// Score the indexer against a previously built sample.
    Score(ScoreArgs),
    /// Compare two score artifact JSON files and emit a markdown diff report.
    Compare(CompareArgs),
}

/// Arguments for `pr-oracle build-sample`.
#[derive(Debug, Args)]
pub struct BuildSampleArgs {
    /// Path to the git repository root to sample from.
    #[arg(long)]
    pub repo_path: PathBuf,

    /// Sample window start date (inclusive), in YYYY-MM-DD format.
    /// Defaults to 180 days before today.
    #[arg(long)]
    pub since: Option<String>,

    /// Sample window end date (inclusive), in YYYY-MM-DD format.
    /// Defaults to today.
    #[arg(long)]
    pub until: Option<String>,

    /// Maximum number of PRs to include in the sample (15-25 recommended).
    #[arg(long, default_value = "20")]
    pub count: usize,

    /// Path where the `pr_sample.json` file is written.
    #[arg(long, default_value = "pr_sample.json")]
    pub out: PathBuf,

    /// Seed for reproducible sampling.  The same seed on the same history
    /// produces the same sample.
    #[arg(long)]
    pub seed: Option<u64>,
}

/// Arguments for `pr-oracle score`.
#[derive(Debug, Args)]
pub struct ScoreArgs {
    /// Path to the `pr_sample.json` produced by `build-sample`.
    #[arg(long)]
    pub sample: PathBuf,

    /// Path to the git repository root.
    #[arg(long)]
    pub repo_path: PathBuf,

    /// Path to the `gather-step` binary to invoke for each PR.
    #[arg(long, default_value = "target/release/gather-step")]
    pub gather_step_bin: PathBuf,

    /// Optional config file to pass through to gather-step.
    #[arg(long)]
    pub config: Option<PathBuf>,

    /// Path where the score artifact JSON file is written.
    #[arg(long, default_value = "pr_oracle_scores.json")]
    pub out: PathBuf,

    /// Exit non-zero when median F1 < 0.75 or median recall < 0.70.
    #[arg(long)]
    pub gate_high: bool,
}

/// Arguments for `pr-oracle compare`.
#[derive(Debug, Args)]
pub struct CompareArgs {
    /// Baseline score artifact JSON.
    pub baseline: PathBuf,

    /// Current score artifact JSON to compare against the baseline.
    pub current: PathBuf,
}

// ─── Sample types ─────────────────────────────────────────────────────────────

/// Label assigned to each file in a sampled PR.
///
/// Operators may post-edit the `pr_sample.json` to override these defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileLabel {
    /// A production source file the indexer must surface.
    MustFind,
    /// A test or supporting file that is acceptable to surface but not required.
    AcceptableExtra,
    /// Config, lock, or generated files that are neutral.
    Incidental,
}

/// Heuristic change-category classifier applied to PR title keywords.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeCategoryLabel {
    EventSchema,
    TypeRollout,
    Auth,
    DiWiring,
    Config,
    Other,
}

/// Coarse bucket for the number of files changed in a PR.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffSizeBucket {
    /// 1-5 files changed.
    Small,
    /// 6-30 files changed.
    Medium,
    /// 31+ files changed.
    Large,
}

/// One sampled PR entry in `pr_sample.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SampledPr {
    pub pr_id: String,
    pub pre_merge_commit: String,
    pub merge_commit: String,
    pub title: String,
    pub body: String,
    pub repo_count: u32,
    pub change_category: ChangeCategoryLabel,
    pub diff_size_bucket: DiffSizeBucket,
    pub changed_files: Vec<String>,
    pub labels: BTreeMap<String, FileLabel>,
}

/// The full `pr_sample.json` artifact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrSample {
    pub built_at: String,
    pub repo_path: String,
    pub since: String,
    pub until: String,
    pub seed: Option<u64>,
    pub prs: Vec<SampledPr>,
}

// ─── Score types ──────────────────────────────────────────────────────────────

/// Precision / recall / F1 score for a single PR.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrScore {
    pub pr_id: String,
    pub merge_commit: String,
    pub pre_merge_commit: String,
    pub change_category: ChangeCategoryLabel,
    pub diff_size_bucket: DiffSizeBucket,
    pub repo_count: u32,
    /// False for PRs with no must-find files. They are still scored for
    /// transparency, but excluded from gate medians because they do not test
    /// retrieval quality.
    #[serde(default = "default_gate_evaluable")]
    pub gate_evaluable: bool,
    pub suggested_files: Vec<String>,
    pub precision: f64,
    pub recall: f64,
    pub f1: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Aggregate metrics across a stratum of PRs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StratumAggregate {
    pub stratum: String,
    pub count: usize,
    pub median_f1: f64,
    pub median_precision: f64,
    pub median_recall: f64,
}

/// Full score artifact written by `pr-oracle score`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoreArtifact {
    pub scored_at: String,
    pub sample_path: String,
    pub gather_step_bin: String,
    pub median_f1: f64,
    pub median_precision: f64,
    pub median_recall: f64,
    /// Whether the high-bar gate is met (F1 >= 0.75 and recall >= 0.70).
    pub gate_high_passed: bool,
    pub pr_scores: Vec<PrScore>,
    pub strata: Vec<StratumAggregate>,
}

const fn default_gate_evaluable() -> bool {
    true
}

// ─── Public entry point ───────────────────────────────────────────────────────

/// Dispatch the `pr-oracle` subcommand.
///
/// # Errors
///
/// Propagates errors from git operations, file I/O, or subprocess invocations.
pub fn run(args: &PrOracleArgs) -> anyhow::Result<()> {
    match &args.subcommand {
        PrOracleSubcommand::BuildSample(a) => run_build_sample(a),
        PrOracleSubcommand::Score(a) => run_score(a),
        PrOracleSubcommand::Compare(a) => run_compare(a),
    }
}

// ─── build-sample ─────────────────────────────────────────────────────────────

fn run_build_sample(args: &BuildSampleArgs) -> anyhow::Result<()> {
    let now = Utc::now();
    let since = parse_window_date(args.since.as_deref(), now - chrono::Duration::days(180))?;
    let until = parse_window_date(args.until.as_deref(), now)?;

    print_status(&format!(
        "Building PR sample from {} (window {} to {})",
        args.repo_path.display(),
        since.format("%Y-%m-%d"),
        until.format("%Y-%m-%d"),
    ));

    let candidates = collect_merge_candidates(&args.repo_path, since, until)
        .context("collecting merge commits from git history")?;

    print_status(&format!(
        "Found {} candidate merge commits in window",
        candidates.len()
    ));

    let sampled = stratify_and_sample(&candidates, args.count, args.seed);

    print_status(&format!(
        "Sampled {} PRs after stratification",
        sampled.len()
    ));

    let sample = PrSample {
        built_at: now.to_rfc3339(),
        repo_path: args.repo_path.display().to_string(),
        since: since.format("%Y-%m-%d").to_string(),
        until: until.format("%Y-%m-%d").to_string(),
        seed: args.seed,
        prs: sampled,
    };

    let json = serde_json::to_string_pretty(&sample)?;
    std::fs::write(&args.out, json)
        .with_context(|| format!("writing sample to {}", args.out.display()))?;

    print_status(&format!(
        "Sample written to {} ({} PRs)",
        args.out.display(),
        sample.prs.len()
    ));
    Ok(())
}

/// Raw commit data extracted from the git history walk.
#[derive(Debug)]
struct MergeCandidate {
    merge_sha: String,
    pre_merge_sha: String,
    title: String,
    body: String,
    changed_files: Vec<String>,
}

/// Use gix to walk HEAD and collect all merge-like commits within the date
/// window. Both explicit merge commits (two or more parents) and squash-merge
/// commits (single parent with a `(#N)` title suffix) are treated as PR
/// boundaries.
fn collect_merge_candidates(
    repo_path: &Path,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
) -> anyhow::Result<Vec<MergeCandidate>> {
    let repo = gix::open(repo_path)
        .with_context(|| format!("opening git repository at {}", repo_path.display()))?;

    let head_id = repo.head_id().context("resolving HEAD")?.detach();

    let walk = repo
        .rev_walk([head_id])
        .sorting(Sorting::ByCommitTime(CommitTimeOrder::NewestFirst))
        .all()
        .context("starting rev-walk")?;

    let since_unix = since.timestamp();
    let until_unix = until.timestamp();

    let squash_re = squash_pr_regex();
    let mut candidates = Vec::new();

    for item in walk {
        let item = item.context("iterating rev-walk")?;
        let id = item.id().detach();

        let commit = repo.find_commit(id).context("loading commit")?;

        let author_time = commit
            .author()
            .map_err(|e| anyhow::anyhow!("decoding author: {e}"))?
            .time()
            .map_err(|e| anyhow::anyhow!("decoding author time: {e}"))?
            .seconds;

        if author_time < since_unix {
            break;
        }
        if author_time > until_unix {
            continue;
        }

        let message_bytes = commit.message_raw_sloppy();
        let message_str = message_bytes.to_str_lossy();
        let subject = message_str.lines().next().unwrap_or("").trim().to_owned();
        let body_raw: String = message_str
            .lines()
            .skip(1)
            .collect::<Vec<_>>()
            .join("\n")
            .trim()
            .to_owned();

        let parent_ids: Vec<gix::ObjectId> = commit.parent_ids().map(gix::Id::detach).collect();
        let is_merge = parent_ids.len() >= 2;
        let is_squash_merge = parent_ids.len() == 1 && squash_re.is_match(&subject);

        if !is_merge && !is_squash_merge {
            continue;
        }

        let merge_sha = id.to_string();
        let pre_merge_sha = if is_merge {
            parent_ids
                .first()
                .map_or_else(|| merge_sha.clone(), gix::ObjectId::to_string)
        } else {
            merge_sha.clone()
        };

        let changed_files = git_diff_tree_files(repo_path, &merge_sha).unwrap_or_default();

        candidates.push(MergeCandidate {
            merge_sha,
            pre_merge_sha,
            title: subject,
            body: body_raw,
            changed_files,
        });
    }

    Ok(candidates)
}

fn git_diff_tree_files(repo_path: &Path, commit_sha: &str) -> anyhow::Result<Vec<String>> {
    let output = ProcessCommand::new("git")
        .args([
            "diff-tree",
            "--no-commit-id",
            "-r",
            "--name-only",
            commit_sha,
        ])
        .current_dir(repo_path)
        .output()
        .context("running git diff-tree")?;
    if !output.status.success() {
        return Ok(Vec::new());
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(str::to_owned)
        .collect())
}

fn stratify_and_sample(
    candidates: &[MergeCandidate],
    count: usize,
    seed: Option<u64>,
) -> Vec<SampledPr> {
    let mut buckets: BTreeMap<(u32, String, String), Vec<usize>> = BTreeMap::new();
    for (idx, c) in candidates.iter().enumerate() {
        let repo_count = estimate_repo_count(&c.changed_files);
        let category = classify_change_category(&c.title);
        let size = diff_size_bucket(c.changed_files.len());
        let key = (repo_count, format!("{category:?}"), format!("{size:?}"));
        buckets.entry(key).or_default().push(idx);
    }

    let mut bucket_vec: Vec<Vec<usize>> = buckets.into_values().collect();
    let seed_value = seed.unwrap_or(0xcafe_babe_dead_beef);
    for bucket in &mut bucket_vec {
        seeded_shuffle(bucket, seed_value);
    }

    let mut result_indices: Vec<usize> = Vec::with_capacity(count);
    let mut bucket_cursors: Vec<usize> = vec![0; bucket_vec.len()];
    'outer: loop {
        let mut advanced = false;
        for (bi, bucket) in bucket_vec.iter().enumerate() {
            if bucket_cursors[bi] < bucket.len() {
                result_indices.push(bucket[bucket_cursors[bi]]);
                bucket_cursors[bi] += 1;
                advanced = true;
                if result_indices.len() >= count {
                    break 'outer;
                }
            }
        }
        if !advanced {
            break;
        }
    }

    result_indices
        .into_iter()
        .map(|idx| {
            let c = &candidates[idx];
            let labels = assign_default_labels(&c.changed_files);
            SampledPr {
                pr_id: extract_pr_id_from_title(&c.title)
                    .unwrap_or_else(|| c.merge_sha[..8.min(c.merge_sha.len())].to_owned()),
                pre_merge_commit: c.pre_merge_sha.clone(),
                merge_commit: c.merge_sha.clone(),
                title: c.title.clone(),
                body: c.body.clone(),
                repo_count: estimate_repo_count(&c.changed_files),
                change_category: classify_change_category(&c.title),
                diff_size_bucket: diff_size_bucket(c.changed_files.len()),
                changed_files: c.changed_files.clone(),
                labels,
            }
        })
        .collect()
}

fn estimate_repo_count(files: &[String]) -> u32 {
    let distinct_roots: BTreeSet<&str> = files
        .iter()
        .filter_map(|f| f.split_once('/').map(|(prefix, _)| prefix))
        .collect();
    match distinct_roots.len() {
        0 | 1 => 1,
        2 | 3 => 2,
        _ => 4,
    }
}

fn classify_change_category(title: &str) -> ChangeCategoryLabel {
    let mut lower = title.to_owned();
    lower.make_ascii_lowercase();
    if lower.contains("event") || lower.contains("schema") || lower.contains("topic") {
        ChangeCategoryLabel::EventSchema
    } else if lower.contains("type") || lower.contains("rollout") || lower.contains("migration") {
        ChangeCategoryLabel::TypeRollout
    } else if lower.contains("auth") || lower.contains("jwt") || lower.contains("token") {
        ChangeCategoryLabel::Auth
    } else if lower.contains("inject") || lower.contains("provider") || lower.contains("module") {
        ChangeCategoryLabel::DiWiring
    } else if lower.contains("config") || lower.contains("env") || lower.contains("yaml") {
        ChangeCategoryLabel::Config
    } else {
        ChangeCategoryLabel::Other
    }
}

fn diff_size_bucket(file_count: usize) -> DiffSizeBucket {
    match file_count {
        0..=5 => DiffSizeBucket::Small,
        6..=30 => DiffSizeBucket::Medium,
        _ => DiffSizeBucket::Large,
    }
}

fn assign_default_labels(files: &[String]) -> BTreeMap<String, FileLabel> {
    files
        .iter()
        .map(|file| {
            let label = default_file_label(file);
            (file.clone(), label)
        })
        .collect()
}

fn default_file_label(path: &str) -> FileLabel {
    let p = Path::new(path);
    let ext = p.extension().and_then(|e| e.to_str()).unwrap_or("");

    if ext.eq_ignore_ascii_case("lock")
        || ext.eq_ignore_ascii_case("snap")
        || ext.eq_ignore_ascii_case("md")
        || ext.eq_ignore_ascii_case("toml")
        || ext.eq_ignore_ascii_case("yaml")
        || ext.eq_ignore_ascii_case("yml")
        || ext.eq_ignore_ascii_case("json")
        || ext.eq_ignore_ascii_case("txt")
        || ext.eq_ignore_ascii_case("env")
    {
        return FileLabel::Incidental;
    }

    let mut lower_path = path.to_owned();
    lower_path.make_ascii_lowercase();

    if lower_path.starts_with("tests/")
        || lower_path.starts_with("test/")
        || lower_path.starts_with("__tests__/")
        || lower_path.starts_with("spec/")
        || lower_path.contains(".test.")
        || lower_path.contains(".spec.")
        || lower_path.contains("_test.")
        || lower_path.contains("_spec.")
    {
        return FileLabel::AcceptableExtra;
    }

    let is_source_prefix = lower_path.starts_with("src/")
        || lower_path.starts_with("lib/")
        || lower_path.starts_with("app/");
    let is_source_ext = ext.eq_ignore_ascii_case("rs")
        || ext.eq_ignore_ascii_case("ts")
        || ext.eq_ignore_ascii_case("tsx")
        || ext.eq_ignore_ascii_case("py")
        || ext.eq_ignore_ascii_case("go")
        || ext.eq_ignore_ascii_case("java")
        || ext.eq_ignore_ascii_case("kt")
        || ext.eq_ignore_ascii_case("js")
        || ext.eq_ignore_ascii_case("jsx");
    if is_source_prefix || is_source_ext {
        return FileLabel::MustFind;
    }

    FileLabel::Incidental
}

fn extract_pr_id_from_title(title: &str) -> Option<String> {
    squash_pr_regex()
        .captures(title)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_owned())
}

fn squash_pr_regex() -> &'static Regex {
    static CELL: OnceLock<Regex> = OnceLock::new();
    CELL.get_or_init(|| Regex::new(r"\(#(\d+)\)").expect("squash_pr_regex must compile"))
}

fn seeded_shuffle(slice: &mut [usize], seed: u64) {
    let mut rng_state = seed;
    let len = slice.len();
    if len < 2 {
        return;
    }
    for i in (1..len).rev() {
        rng_state = splitmix64(rng_state);
        #[expect(
            clippy::cast_possible_truncation,
            reason = "modulo bounds the result to (0..=i) which fits in usize on all targets"
        )]
        let j = (rng_state as usize) % (i + 1);
        slice.swap(i, j);
    }
}

fn splitmix64(state: u64) -> u64 {
    let z = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    let z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn parse_window_date(input: Option<&str>, default: DateTime<Utc>) -> anyhow::Result<DateTime<Utc>> {
    let Some(s) = input else {
        return Ok(default);
    };
    let date = NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .with_context(|| format!("parsing date `{s}` -- expected YYYY-MM-DD"))?;
    Ok(Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0).expect("midnight is valid")))
}

// ─── score ────────────────────────────────────────────────────────────────────

fn run_score(args: &ScoreArgs) -> anyhow::Result<()> {
    let raw = std::fs::read_to_string(&args.sample)
        .with_context(|| format!("reading sample from {}", args.sample.display()))?;
    let sample: PrSample = serde_json::from_str(&raw).context("parsing pr_sample.json")?;

    if sample.prs.is_empty() {
        anyhow::bail!("sample contains no PRs; nothing to score");
    }

    print_status(&format!(
        "Scoring {} PRs from sample {}",
        sample.prs.len(),
        args.sample.display()
    ));

    let mut pr_scores: Vec<PrScore> = Vec::with_capacity(sample.prs.len());

    for pr in &sample.prs {
        print_status(&format!(
            "  Scoring PR {} ({})",
            pr.pr_id,
            &pr.pre_merge_commit[..8.min(pr.pre_merge_commit.len())]
        ));

        let score = score_single_pr(
            pr,
            &args.repo_path,
            &args.gather_step_bin,
            args.config.as_deref(),
        );
        pr_scores.push(score);
    }

    let gate_scores = successful_gate_scores(&pr_scores);
    if gate_scores.is_empty() {
        print_status("No successful PR scores with must-find files; gate medians default to 0.0");
    }
    let mut all_f1: Vec<f64> = gate_scores.iter().map(|s| s.f1).collect();
    let mut all_precision: Vec<f64> = gate_scores.iter().map(|s| s.precision).collect();
    let mut all_recall: Vec<f64> = gate_scores.iter().map(|s| s.recall).collect();

    all_f1.sort_by(f64::total_cmp);
    all_precision.sort_by(f64::total_cmp);
    all_recall.sort_by(f64::total_cmp);

    let median_f1 = median_f64(&all_f1);
    let median_precision = median_f64(&all_precision);
    let median_recall = median_f64(&all_recall);

    let gate_high_passed = median_f1 >= GATE_HIGH_MIN_F1 && median_recall >= GATE_HIGH_MIN_RECALL;

    let strata = build_stratum_aggregates(&pr_scores);

    let artifact = ScoreArtifact {
        scored_at: Utc::now().to_rfc3339(),
        sample_path: redact_local_paths(&args.sample.display().to_string()),
        gather_step_bin: redact_local_paths(&args.gather_step_bin.display().to_string()),
        median_f1,
        median_precision,
        median_recall,
        gate_high_passed,
        pr_scores,
        strata,
    };

    let json = serde_json::to_string_pretty(&artifact)?;
    std::fs::write(&args.out, json)
        .with_context(|| format!("writing score artifact to {}", args.out.display()))?;

    print_status(&format!(
        "Score artifact written to {out} -- median_f1={median_f1:.3} median_recall={median_recall:.3} median_precision={median_precision:.3}",
        out = args.out.display(),
    ));

    if args.gate_high && !gate_high_passed {
        anyhow::bail!(
            "high-bar gate failed: median F1 {median_f1:.3} (min {GATE_HIGH_MIN_F1}) \
             and/or median recall {median_recall:.3} (min {GATE_HIGH_MIN_RECALL})"
        );
    }

    Ok(())
}

fn score_single_pr(
    pr: &SampledPr,
    repo_path: &Path,
    gather_step_bin: &Path,
    config: Option<&Path>,
) -> PrScore {
    match score_single_pr_inner(pr, repo_path, gather_step_bin, config) {
        Ok(score) => score,
        Err(err) => PrScore {
            pr_id: pr.pr_id.clone(),
            merge_commit: pr.merge_commit.clone(),
            pre_merge_commit: pr.pre_merge_commit.clone(),
            change_category: pr.change_category.clone(),
            diff_size_bucket: pr.diff_size_bucket.clone(),
            repo_count: pr.repo_count,
            gate_evaluable: pr_has_must_find(pr),
            suggested_files: Vec::new(),
            precision: 0.0,
            recall: 0.0,
            f1: 0.0,
            error: Some(format_error_chain(&err)),
        },
    }
}

fn format_error_chain(err: &anyhow::Error) -> String {
    err.chain()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>()
        .join(": ")
}

fn pr_has_must_find(pr: &SampledPr) -> bool {
    pr.labels
        .values()
        .any(|label| *label == FileLabel::MustFind)
}

fn successful_gate_scores(scores: &[PrScore]) -> Vec<&PrScore> {
    scores
        .iter()
        .filter(|score| score.error.is_none() && score.gate_evaluable)
        .collect()
}

fn resolve_pr_worktree_config(
    config_path: &Path,
    repo_path: &Path,
    worktree_path: &Path,
) -> anyhow::Result<PathBuf> {
    let canonical_repo = fs::canonicalize(repo_path)
        .with_context(|| format!("canonicalizing repo path {}", repo_path.display()))?;
    let canonical_worktree = fs::canonicalize(worktree_path)
        .with_context(|| format!("canonicalizing worktree path {}", worktree_path.display()))?;
    let canonical_config = canonicalize_config_path(config_path, &canonical_repo)?;

    let worktree_config = if canonical_config.starts_with(&canonical_worktree) {
        canonical_config
    } else {
        let relative_config = canonical_config.strip_prefix(&canonical_repo).with_context(|| {
            format!(
                "PR-oracle config {} must live under the sampled repo {} so it can be rebased into the PR worktree",
                canonical_config.display(),
                canonical_repo.display()
            )
        })?;
        canonical_worktree.join(relative_config)
    };

    anyhow::ensure!(
        worktree_config.exists(),
        "PR-oracle config {} is not present in the PR worktree at {}; pass a config committed under the sampled repo or omit --config",
        config_path.display(),
        worktree_config.display(),
    );
    validate_pr_worktree_config(&worktree_config, &canonical_worktree)?;
    Ok(worktree_config)
}

fn canonicalize_config_path(config_path: &Path, canonical_repo: &Path) -> anyhow::Result<PathBuf> {
    if !config_path.is_absolute() {
        let repo_relative = canonical_repo.join(config_path);
        if repo_relative.exists() {
            return fs::canonicalize(&repo_relative)
                .with_context(|| format!("canonicalizing config {}", repo_relative.display()));
        }
    }
    fs::canonicalize(config_path)
        .with_context(|| format!("canonicalizing config {}", config_path.display()))
}

fn validate_pr_worktree_config(
    config_path: &Path,
    canonical_worktree: &Path,
) -> anyhow::Result<()> {
    let config = GatherStepConfig::from_yaml_file(config_path)
        .with_context(|| format!("loading PR-oracle config {}", config_path.display()))?;
    let config_root = config_path.parent().unwrap_or_else(|| Path::new("."));
    let canonical_config_root = fs::canonicalize(config_root)
        .with_context(|| format!("canonicalizing config root {}", config_root.display()))?;
    anyhow::ensure!(
        canonical_config_root.starts_with(canonical_worktree),
        "PR-oracle config root {} is outside the PR worktree {}; configs must be worktree-local",
        canonical_config_root.display(),
        canonical_worktree.display(),
    );
    config
        .validate_repo_roots_against_config_root(config_root)
        .with_context(|| format!("validating PR-oracle config {}", config_path.display()))?;

    for repo in &config.repos {
        let repo_root = config_root.join(&repo.path);
        let canonical_repo_root = fs::canonicalize(&repo_root).with_context(|| {
            format!(
                "PR-oracle config repo `{}` path {} must exist inside the PR worktree",
                repo.name,
                repo_root.display()
            )
        })?;
        anyhow::ensure!(
            canonical_repo_root.starts_with(canonical_worktree),
            "PR-oracle config repo `{}` path {} resolves outside the PR worktree {}",
            repo.name,
            canonical_repo_root.display(),
            canonical_worktree.display(),
        );
    }
    Ok(())
}

fn score_single_pr_inner(
    pr: &SampledPr,
    repo_path: &Path,
    gather_step_bin: &Path,
    config: Option<&Path>,
) -> anyhow::Result<PrScore> {
    let worktree_dir = create_worktree(repo_path, &pr.pre_merge_commit)
        .with_context(|| format!("creating worktree for {}", pr.pre_merge_commit))?;
    let _guard = WorktreeGuard {
        repo_path: repo_path.to_path_buf(),
        worktree_path: worktree_dir.clone(),
    };
    let worktree_config = config
        .map(|config_path| resolve_pr_worktree_config(config_path, repo_path, &worktree_dir))
        .transpose()?;

    let suggested_files = query_indexer_for_pr(
        pr,
        &worktree_dir,
        gather_step_bin,
        worktree_config.as_deref(),
    )?;

    let must_find: BTreeSet<&str> = pr
        .labels
        .iter()
        .filter_map(|(file, label)| {
            if *label == FileLabel::MustFind {
                Some(file.as_str())
            } else {
                None
            }
        })
        .collect();
    let acceptable: BTreeSet<&str> = pr
        .labels
        .iter()
        .filter_map(|(file, label)| {
            if matches!(label, FileLabel::MustFind | FileLabel::AcceptableExtra) {
                Some(file.as_str())
            } else {
                None
            }
        })
        .collect();

    let suggested_set: BTreeSet<&str> = suggested_files.iter().map(String::as_str).collect();
    let gate_evaluable = !must_find.is_empty();

    let precision = if suggested_set.is_empty() {
        if acceptable.is_empty() { 1.0 } else { 0.0 }
    } else {
        let relevant_suggested = suggested_set.intersection(&acceptable).count();
        #[expect(
            clippy::cast_precision_loss,
            reason = "file counts are small; precision loss is negligible"
        )]
        {
            relevant_suggested as f64 / suggested_set.len() as f64
        }
    };

    let recall = if must_find.is_empty() {
        1.0
    } else {
        let found_must = suggested_set.intersection(&must_find).count();
        #[expect(
            clippy::cast_precision_loss,
            reason = "file counts are small; precision loss is negligible"
        )]
        {
            found_must as f64 / must_find.len() as f64
        }
    };

    let f1 = if precision + recall < f64::EPSILON {
        0.0
    } else {
        2.0 * precision * recall / (precision + recall)
    };

    Ok(PrScore {
        pr_id: pr.pr_id.clone(),
        merge_commit: pr.merge_commit.clone(),
        pre_merge_commit: pr.pre_merge_commit.clone(),
        change_category: pr.change_category.clone(),
        diff_size_bucket: pr.diff_size_bucket.clone(),
        repo_count: pr.repo_count,
        gate_evaluable,
        suggested_files,
        precision,
        recall,
        f1,
        error: None,
    })
}

fn create_worktree(repo_path: &Path, commit_sha: &str) -> anyhow::Result<PathBuf> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let worktree_path = std::env::temp_dir().join(format!(
        "gather-step-bench-pr-oracle-wt-{pid}-{id}",
        pid = std::process::id(),
    ));

    let output = ProcessCommand::new("git")
        .args([
            "worktree",
            "add",
            "--detach",
            worktree_path
                .to_str()
                .context("worktree path must be UTF-8")?,
            commit_sha,
        ])
        .current_dir(repo_path)
        .output()
        .context("running git worktree add")?;

    if !output.status.success() {
        anyhow::bail!(
            "git worktree add failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }

    Ok(worktree_path)
}

struct WorktreeGuard {
    repo_path: PathBuf,
    worktree_path: PathBuf,
}

impl Drop for WorktreeGuard {
    fn drop(&mut self) {
        let Some(path_str) = self.worktree_path.to_str() else {
            return;
        };
        let path_str = path_str.to_owned();
        let _ = ProcessCommand::new("git")
            .args(["worktree", "remove", "--force", &path_str])
            .current_dir(&self.repo_path)
            .output();
        let _ = std::fs::remove_dir_all(&self.worktree_path);
    }
}

fn query_indexer_for_pr(
    pr: &SampledPr,
    workspace_path: &Path,
    gather_step_bin: &Path,
    config: Option<&Path>,
) -> anyhow::Result<Vec<String>> {
    let mut index_command = ProcessCommand::new(gather_step_bin);
    index_command
        .arg("--workspace")
        .arg(workspace_path)
        .arg("--json");
    index_command.arg("index");
    if let Some(config_path) = config {
        index_command.arg("--config").arg(config_path);
    }
    run_successful_command(&mut index_command, gather_step_bin, "indexing PR worktree")?;

    let mut command = ProcessCommand::new(gather_step_bin);
    command.arg("--workspace").arg(workspace_path).arg("--json");
    command.args([
        "pack",
        pr.title.as_str(),
        "--mode",
        "planning",
        "--budget-bytes",
        "32000",
    ]);

    let output = run_successful_command(&mut command, gather_step_bin, "querying PR pack")?;

    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).context("parsing gather-step JSON output")?;

    let files = json
        .get("data")
        .and_then(|d| d.get("items"))
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    item.get("file_path")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(files)
}

fn run_successful_command(
    command: &mut ProcessCommand,
    gather_step_bin: &Path,
    action: &str,
) -> anyhow::Result<std::process::Output> {
    let output = command
        .output()
        .with_context(|| format!("running {}", gather_step_bin.display()))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "{action} failed via {}: {}",
            gather_step_bin.display(),
            stderr.trim()
        );
    }
    Ok(output)
}

fn build_stratum_aggregates(scores: &[PrScore]) -> Vec<StratumAggregate> {
    let mut out = Vec::new();

    for (label, variant) in [
        ("event_schema", ChangeCategoryLabel::EventSchema),
        ("type_rollout", ChangeCategoryLabel::TypeRollout),
        ("auth", ChangeCategoryLabel::Auth),
        ("di_wiring", ChangeCategoryLabel::DiWiring),
        ("config", ChangeCategoryLabel::Config),
        ("other", ChangeCategoryLabel::Other),
    ] {
        let subset: Vec<&PrScore> = scores
            .iter()
            .filter(|s| s.error.is_none() && s.gate_evaluable && s.change_category == variant)
            .collect();
        if subset.is_empty() {
            continue;
        }
        out.push(stratum_aggregate(
            &format!("change_category:{label}"),
            &subset,
        ));
    }

    for (label, variant) in [
        ("small", DiffSizeBucket::Small),
        ("medium", DiffSizeBucket::Medium),
        ("large", DiffSizeBucket::Large),
    ] {
        let subset: Vec<&PrScore> = scores
            .iter()
            .filter(|s| s.error.is_none() && s.gate_evaluable && s.diff_size_bucket == variant)
            .collect();
        if subset.is_empty() {
            continue;
        }
        out.push(stratum_aggregate(&format!("diff_size:{label}"), &subset));
    }

    for (label, repo_count) in [("single", 1u32), ("multi_small", 2), ("multi_large", 4)] {
        let subset: Vec<&PrScore> = scores
            .iter()
            .filter(|s| s.error.is_none() && s.gate_evaluable && s.repo_count == repo_count)
            .collect();
        if subset.is_empty() {
            continue;
        }
        out.push(stratum_aggregate(&format!("repo_count:{label}"), &subset));
    }

    out
}

fn stratum_aggregate(stratum: &str, scores: &[&PrScore]) -> StratumAggregate {
    let mut f1_vals: Vec<f64> = scores.iter().map(|s| s.f1).collect();
    let mut prec_vals: Vec<f64> = scores.iter().map(|s| s.precision).collect();
    let mut rec_vals: Vec<f64> = scores.iter().map(|s| s.recall).collect();
    f1_vals.sort_by(f64::total_cmp);
    prec_vals.sort_by(f64::total_cmp);
    rec_vals.sort_by(f64::total_cmp);
    StratumAggregate {
        stratum: stratum.to_owned(),
        count: scores.len(),
        median_f1: median_f64(&f1_vals),
        median_precision: median_f64(&prec_vals),
        median_recall: median_f64(&rec_vals),
    }
}

fn median_f64(sorted: &[f64]) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    if sorted.len().is_multiple_of(2) {
        let mid = sorted.len() / 2;
        f64::midpoint(sorted[mid - 1], sorted[mid])
    } else {
        sorted[sorted.len() / 2]
    }
}

// ─── compare ──────────────────────────────────────────────────────────────────

fn run_compare(args: &CompareArgs) -> anyhow::Result<()> {
    let baseline = load_score_artifact(&args.baseline)?;
    let current = load_score_artifact(&args.current)?;
    let report = build_compare_report(&baseline, &current);
    print_status(&report);
    Ok(())
}

fn load_score_artifact(path: &Path) -> anyhow::Result<ScoreArtifact> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading score artifact from {}", path.display()))?;
    serde_json::from_str(&raw).context("parsing score artifact JSON")
}

fn build_compare_report(baseline: &ScoreArtifact, current: &ScoreArtifact) -> String {
    let mut out = String::new();

    out.push_str("# PR-Oracle Score Comparison\n\n");
    out.push_str("## Aggregate Metrics\n\n");
    out.push_str("| Metric | Baseline | Current | Delta |\n");
    out.push_str("|--------|----------|---------|-------|\n");

    let metrics = [
        ("median_f1", baseline.median_f1, current.median_f1),
        (
            "median_precision",
            baseline.median_precision,
            current.median_precision,
        ),
        (
            "median_recall",
            baseline.median_recall,
            current.median_recall,
        ),
    ];
    for (name, base, curr) in metrics {
        let delta = curr - base;
        let flag = if delta < -f64::EPSILON {
            " REGRESSION"
        } else {
            ""
        };
        let _ = writeln!(
            out,
            "| {name} | {base:.3} | {curr:.3} | {delta:+.3}{flag} |"
        );
    }

    out.push_str("\n## Gate Status\n\n");
    let gate_base = if baseline.gate_high_passed {
        "PASS"
    } else {
        "FAIL"
    };
    let gate_curr = if current.gate_high_passed {
        "PASS"
    } else {
        "FAIL"
    };
    let _ = writeln!(out, "- Baseline gate-high: **{gate_base}**");
    let _ = writeln!(out, "- Current gate-high: **{gate_curr}**");
    if baseline.gate_high_passed && !current.gate_high_passed {
        out.push_str("- **WARNING: gate-high regressed from PASS to FAIL**\n");
    }

    out.push_str("\n## Per-PR F1 Deltas\n\n");
    out.push_str("| PR | Baseline F1 | Current F1 | Delta | Note |\n");
    out.push_str("|----|------------|-----------|-------|------|\n");

    let baseline_map: BTreeMap<&str, &PrScore> = baseline
        .pr_scores
        .iter()
        .map(|s| (s.pr_id.as_str(), s))
        .collect();
    let current_map: BTreeMap<&str, &PrScore> = current
        .pr_scores
        .iter()
        .map(|s| (s.pr_id.as_str(), s))
        .collect();
    let all_ids: BTreeSet<&str> = baseline_map
        .keys()
        .chain(current_map.keys())
        .copied()
        .collect();

    for id in all_ids {
        match (baseline_map.get(id), current_map.get(id)) {
            (Some(base), Some(curr)) => {
                let delta = curr.f1 - base.f1;
                let note = if delta < -0.05 {
                    "REGRESSION"
                } else if delta > 0.05 {
                    "improvement"
                } else {
                    ""
                };
                let base_f1 = base.f1;
                let curr_f1 = curr.f1;
                let _ = writeln!(
                    out,
                    "| {id} | {base_f1:.3} | {curr_f1:.3} | {delta:+.3} | {note} |"
                );
            }
            (None, Some(curr)) => {
                let curr_f1 = curr.f1;
                let _ = writeln!(out, "| {id} | -- | {curr_f1:.3} | -- | new |");
            }
            (Some(base), None) => {
                let base_f1 = base.f1;
                let _ = writeln!(out, "| {id} | {base_f1:.3} | -- | -- | removed |");
            }
            (None, None) => {}
        }
    }

    out.push_str("\n## Stratum Changes\n\n");
    out.push_str("| Stratum | Baseline F1 | Current F1 | Delta |\n");
    out.push_str("|---------|------------|-----------|-------|\n");

    let baseline_strata: BTreeMap<&str, &StratumAggregate> = baseline
        .strata
        .iter()
        .map(|s| (s.stratum.as_str(), s))
        .collect();
    let current_strata: BTreeMap<&str, &StratumAggregate> = current
        .strata
        .iter()
        .map(|s| (s.stratum.as_str(), s))
        .collect();
    let all_strata: BTreeSet<&str> = baseline_strata
        .keys()
        .chain(current_strata.keys())
        .copied()
        .collect();

    for stratum in all_strata {
        match (baseline_strata.get(stratum), current_strata.get(stratum)) {
            (Some(base), Some(curr)) => {
                let delta = curr.median_f1 - base.median_f1;
                let base_f1 = base.median_f1;
                let curr_f1 = curr.median_f1;
                let _ = writeln!(
                    out,
                    "| {stratum} | {base_f1:.3} | {curr_f1:.3} | {delta:+.3} |"
                );
            }
            (None, Some(curr)) => {
                let curr_f1 = curr.median_f1;
                let _ = writeln!(out, "| {stratum} | -- | {curr_f1:.3} | -- |");
            }
            (Some(base), None) => {
                let base_f1 = base.median_f1;
                let _ = writeln!(out, "| {stratum} | {base_f1:.3} | -- | -- |");
            }
            (None, None) => {}
        }
    }

    out
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

#[expect(
    clippy::print_stderr,
    reason = "benchmark binary writes structured status to stderr by design"
)]
fn print_status(line: &str) {
    eprintln!("{line}");
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_score(id: &str, gate_evaluable: bool, f1: f64) -> PrScore {
        PrScore {
            pr_id: id.to_owned(),
            merge_commit: "abc".to_owned(),
            pre_merge_commit: "def".to_owned(),
            change_category: ChangeCategoryLabel::Other,
            diff_size_bucket: DiffSizeBucket::Small,
            repo_count: 1,
            gate_evaluable,
            suggested_files: vec!["src/lib.rs".to_owned()],
            precision: f1,
            recall: f1,
            f1,
            error: None,
        }
    }

    #[test]
    fn default_file_label_classifies_production_source() {
        assert_eq!(
            default_file_label("src/handlers/user.rs"),
            FileLabel::MustFind
        );
        assert_eq!(
            default_file_label("src/services/auth.ts"),
            FileLabel::MustFind
        );
        assert_eq!(
            default_file_label("app/models/user.py"),
            FileLabel::MustFind
        );
    }

    #[test]
    fn default_file_label_classifies_test_files() {
        assert_eq!(
            default_file_label("tests/integration/auth_test.rs"),
            FileLabel::AcceptableExtra
        );
        assert_eq!(
            default_file_label("src/handlers/user.test.ts"),
            FileLabel::AcceptableExtra
        );
        assert_eq!(
            default_file_label("src/handlers/user.spec.ts"),
            FileLabel::AcceptableExtra
        );
    }

    #[test]
    fn default_file_label_classifies_incidental_files() {
        assert_eq!(default_file_label("Cargo.lock"), FileLabel::Incidental);
        assert_eq!(
            default_file_label("docs/architecture.md"),
            FileLabel::Incidental
        );
        assert_eq!(
            default_file_label("infra/deploy.yaml"),
            FileLabel::Incidental
        );
        assert_eq!(
            default_file_label("package-lock.json"),
            FileLabel::Incidental
        );
        assert_eq!(
            default_file_label("tests/snapshots/foo.snap"),
            FileLabel::Incidental
        );
    }

    #[test]
    fn classify_change_category_maps_keywords() {
        assert_eq!(
            classify_change_category("Add event schema for billing"),
            ChangeCategoryLabel::EventSchema
        );
        assert_eq!(
            classify_change_category("Type rollout for user entity"),
            ChangeCategoryLabel::TypeRollout
        );
        assert_eq!(
            classify_change_category("Fix auth session renewal"),
            ChangeCategoryLabel::Auth
        );
        assert_eq!(
            classify_change_category("Inject config provider into module"),
            ChangeCategoryLabel::DiWiring
        );
        assert_eq!(
            classify_change_category("Update env config values"),
            ChangeCategoryLabel::Config
        );
        assert_eq!(
            classify_change_category("Misc cleanup"),
            ChangeCategoryLabel::Other
        );
    }

    #[test]
    fn diff_size_bucket_assigns_correct_stratum() {
        assert_eq!(diff_size_bucket(0), DiffSizeBucket::Small);
        assert_eq!(diff_size_bucket(5), DiffSizeBucket::Small);
        assert_eq!(diff_size_bucket(6), DiffSizeBucket::Medium);
        assert_eq!(diff_size_bucket(30), DiffSizeBucket::Medium);
        assert_eq!(diff_size_bucket(31), DiffSizeBucket::Large);
        assert_eq!(diff_size_bucket(100), DiffSizeBucket::Large);
    }

    #[test]
    fn estimate_repo_count_buckets_correctly() {
        assert_eq!(estimate_repo_count(&[]), 1);
        assert_eq!(
            estimate_repo_count(&["src/a.rs".to_owned(), "src/b.rs".to_owned()]),
            1
        );
        assert_eq!(
            estimate_repo_count(&[
                "service-a/src/a.rs".to_owned(),
                "service-b/src/b.rs".to_owned()
            ]),
            2
        );
        assert_eq!(
            estimate_repo_count(&[
                "svc-a/src/a.rs".to_owned(),
                "svc-b/src/b.rs".to_owned(),
                "svc-c/src/c.rs".to_owned(),
                "svc-d/src/d.rs".to_owned(),
                "svc-e/src/e.rs".to_owned(),
            ]),
            4
        );
    }

    #[test]
    fn median_f64_computes_correct_value() {
        assert!((median_f64(&[0.2, 0.4, 0.8]) - 0.4).abs() < 1e-9);
        assert!((median_f64(&[0.2, 0.6]) - 0.4).abs() < 1e-9);
        assert!((median_f64(&[]) - 0.0).abs() < 1e-9);
    }

    #[test]
    fn gate_metrics_ignore_prs_without_must_find_files() {
        let scores = vec![
            test_score("vacuous", false, 1.0),
            test_score("real", true, 0.4),
        ];
        let gate_scores = successful_gate_scores(&scores);

        assert_eq!(gate_scores.len(), 1);
        assert_eq!(gate_scores[0].pr_id, "real");

        let strata = build_stratum_aggregates(&scores);
        assert!(
            strata
                .iter()
                .all(|stratum| (stratum.median_f1 - 0.4).abs() < 1e-9),
            "vacuous score should not inflate strata: {strata:?}"
        );
    }

    #[test]
    fn pr_worktree_config_is_rebased_under_temp_worktree() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let repo = tmp.path().join("repo");
        let worktree = tmp.path().join("worktree");
        fs::create_dir_all(&repo).expect("repo dir");
        fs::create_dir_all(&worktree).expect("worktree dir");
        let config = "repos:\n  - name: single\n    path: .\n";
        fs::write(repo.join("gather-step.config.yaml"), config).expect("repo config");
        fs::write(worktree.join("gather-step.config.yaml"), config).expect("worktree config");

        let resolved =
            resolve_pr_worktree_config(&repo.join("gather-step.config.yaml"), &repo, &worktree)
                .expect("config should rebase to worktree");

        assert_eq!(
            resolved,
            fs::canonicalize(worktree.join("gather-step.config.yaml")).expect("canonical config")
        );
    }

    #[test]
    fn pr_worktree_config_rejects_missing_worktree_copy() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let repo = tmp.path().join("repo");
        let worktree = tmp.path().join("worktree");
        fs::create_dir_all(&repo).expect("repo dir");
        fs::create_dir_all(&worktree).expect("worktree dir");
        fs::write(
            repo.join("gather-step.config.yaml"),
            "repos:\n  - name: single\n    path: .\n",
        )
        .expect("repo config");

        let error =
            resolve_pr_worktree_config(&repo.join("gather-step.config.yaml"), &repo, &worktree)
                .expect_err("config missing from worktree should fail");

        assert!(
            format_error_chain(&error).contains("is not present in the PR worktree"),
            "{error:?}"
        );
    }

    #[test]
    fn seeded_shuffle_is_deterministic() {
        let mut a = vec![0, 1, 2, 3, 4, 5];
        let mut b = vec![0, 1, 2, 3, 4, 5];
        seeded_shuffle(&mut a, 42);
        seeded_shuffle(&mut b, 42);
        assert_eq!(a, b);
    }

    #[test]
    fn seeded_shuffle_produces_different_orderings_for_different_seeds() {
        let mut a = vec![0usize, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let mut b = a.clone();
        seeded_shuffle(&mut a, 1);
        seeded_shuffle(&mut b, 2);
        assert_ne!(a, b);
    }

    #[test]
    fn extract_pr_id_from_title_finds_squash_merge_number() {
        assert_eq!(
            extract_pr_id_from_title("Add feature (#42)"),
            Some("42".to_owned())
        );
        assert_eq!(extract_pr_id_from_title("No PR number here"), None);
    }

    #[test]
    fn build_compare_report_flags_regression() {
        let baseline = ScoreArtifact {
            scored_at: "2026-01-01T00:00:00Z".to_owned(),
            sample_path: "sample.json".to_owned(),
            gather_step_bin: "gather-step".to_owned(),
            median_f1: 0.80,
            median_precision: 0.85,
            median_recall: 0.75,
            gate_high_passed: true,
            pr_scores: vec![PrScore {
                pr_id: "42".to_owned(),
                merge_commit: "abc".to_owned(),
                pre_merge_commit: "def".to_owned(),
                change_category: ChangeCategoryLabel::Other,
                diff_size_bucket: DiffSizeBucket::Small,
                repo_count: 1,
                gate_evaluable: true,
                suggested_files: vec!["src/lib.rs".to_owned()],
                precision: 0.9,
                recall: 0.8,
                f1: 0.80,
                error: None,
            }],
            strata: Vec::new(),
        };
        let mut current = baseline.clone();
        current.pr_scores[0].f1 = 0.50;
        current.median_f1 = 0.50;
        current.gate_high_passed = false;

        let report = build_compare_report(&baseline, &current);
        assert!(
            report.contains("REGRESSION"),
            "expected regression flag in:\n{report}"
        );
        assert!(
            report.contains("gate-high regressed"),
            "expected gate flag in:\n{report}"
        );
    }
}
