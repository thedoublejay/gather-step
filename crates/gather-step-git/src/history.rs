//! Git history ingestion: types and the indexer that walks a repository, extracts
//! per-commit and per-file facts, and persists them via [`MetadataStore`].
//!
//! This module is the foundation for git analytics. It deliberately keeps
//! storage-layer records (`gather_step_storage::CommitRecord`, …) separate from
//! the ingestion-side types defined here so the analysis crate can consume one
//! without dragging in the other.

use std::path::{Path, PathBuf};

use gather_step_storage::{
    CommitFileChangeKind as StoredChangeKind, CommitFileDeltaRecord, CommitRecord, MetadataStore,
    MetadataStoreError,
};
use gix::{
    ObjectId, Repository, bstr::ByteSlice, revision::walk::Sorting,
    sec::trust::DefaultForLevel as _, traverse::commit::simple::CommitTimeOrder,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, trace, warn};

use crate::classify::{
    DEFAULT_DECISION_SIGNALS, classify_commit_message, detect_decision_signal, extract_pr_number,
};

/// Where to source git history from. Today only an on-disk path is supported,
/// but the enum exists so future variants (e.g. an in-memory `gix` repo for
/// tests, a bare clone, …) do not require breaking callers.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum GitRepoSource {
    /// A local repository discoverable via `gix::discover`. The path may be
    /// either the working tree or the `.git` directory.
    Path(PathBuf),
}

impl GitRepoSource {
    pub fn from_path(path: impl Into<PathBuf>) -> Self {
        Self::Path(path.into())
    }
}

/// Identifier of how a single file appears in a single commit relative to the
/// commit's first parent. Mirrors [`gather_step_storage::CommitFileChangeKind`]
/// so callers can move between ingestion and persistence without reaching for
/// `serde_json` payloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitFileChangeKind {
    Added,
    Modified,
    Deleted,
    Renamed,
    Copied,
    /// Mode/type change without a content modification — e.g. regular file to
    /// symlink, or a permissions-only flip. Kept distinct from `Modified` so
    /// hotspot/ownership analytics can choose to ignore it.
    TypeChanged,
}

/// One file's change facts for one commit. `insertions` / `deletions` are
/// `None` for binary diffs where line counts are not meaningful. `old_path` is
/// `Some` only when [`CommitFileChangeKind::Renamed`] or
/// [`CommitFileChangeKind::Copied`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitFileDelta {
    pub file_path: String,
    pub change_kind: CommitFileChangeKind,
    pub insertions: Option<u64>,
    pub deletions: Option<u64>,
    pub old_path: Option<String>,
}

/// All facts extracted from a single git commit, before persistence. The
/// `repo` field is the workspace-local repo name (matching
/// [`gather_step_storage::CommitRecord::repo`]) — not the path on disk.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitFact {
    pub repo: String,
    pub sha: String,
    pub author_email: String,
    /// Author timestamp as Unix seconds. Negative values are technically
    /// possible (commits dated before 1970) but not normalized here so the
    /// storage layer round-trips whatever the repository contains.
    pub author_date_unix: i64,
    pub message: String,
    /// Conventional-commits style classification (`feat`, `fix`, `refactor`,
    /// …). `None` when the message does not match a recognized prefix.
    pub classification: Option<String>,
    /// Merge-PR number extracted from the commit message, when present.
    pub pr_number: Option<u64>,
    /// `true` when the message contains a heuristic decision-signal token
    /// (`because`, `decided`, `trade-off`, …). Used downstream to surface
    /// rationale-rich commits in overview/architecture queries.
    pub has_decision_signal: bool,
    /// Number of parents the commit had. Useful for callers that want to
    /// classify merge commits without re-walking history.
    pub parent_count: usize,
    /// Per-file changes for this commit relative to its first parent. Empty
    /// for the initial commit (no parent to diff against), in which case
    /// callers wishing to seed initial-commit deltas must do so explicitly.
    pub file_deltas: Vec<CommitFileDelta>,
}

impl CommitFact {
    /// Sum of `insertions` across all non-binary file deltas in this commit.
    /// Binary diffs (which carry `None` line counts) are skipped rather than
    /// counted as zero.
    #[must_use]
    pub fn total_insertions(&self) -> u64 {
        self.file_deltas
            .iter()
            .filter_map(|delta| delta.insertions)
            .sum()
    }

    /// Sum of `deletions` across all non-binary file deltas in this commit.
    #[must_use]
    pub fn total_deletions(&self) -> u64 {
        self.file_deltas
            .iter()
            .filter_map(|delta| delta.deletions)
            .sum()
    }
}

/// Outcome of a single `GitHistoryIndexer::sync` invocation. Sync chooses one
/// of these branches based on what it observes in the repository — callers
/// must not interpret a missing variant as an error.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HistorySyncOutcome {
    /// Repository's recorded `last_commit_sha` was reachable from the new
    /// HEAD. Only commits newer than that anchor were walked.
    Incremental {
        repo: String,
        commits_added: u64,
        deltas_added: u64,
        new_head_sha: String,
    },
    /// No `last_commit_sha` was recorded (first-ever sync) or the indexer was
    /// asked to ignore the anchor. The full bounded history was walked.
    FullRebuild {
        repo: String,
        commits_added: u64,
        deltas_added: u64,
        new_head_sha: String,
    },
    /// The recorded `last_commit_sha` was no longer reachable from the new
    /// HEAD — the working interpretation is force-push, rebase, filter-branch
    /// or another history rewrite. Existing per-repo commit rows were purged
    /// (CASCADE-ing per-file deltas) and a full rescan was performed.
    HistoryRewriteFallback {
        repo: String,
        previous_anchor_sha: String,
        commits_added: u64,
        deltas_added: u64,
        new_head_sha: String,
    },
    /// HEAD already matches the recorded `last_commit_sha`. Storage was not
    /// touched. Returned so callers can distinguish "nothing to do" from
    /// "incremental ran with zero new commits".
    NoChange { repo: String, head_sha: String },
}

impl HistorySyncOutcome {
    #[must_use]
    pub fn repo(&self) -> &str {
        match self {
            Self::Incremental { repo, .. }
            | Self::FullRebuild { repo, .. }
            | Self::HistoryRewriteFallback { repo, .. }
            | Self::NoChange { repo, .. } => repo,
        }
    }

    #[must_use]
    pub fn commits_added(&self) -> u64 {
        match self {
            Self::Incremental { commits_added, .. }
            | Self::FullRebuild { commits_added, .. }
            | Self::HistoryRewriteFallback { commits_added, .. } => *commits_added,
            Self::NoChange { .. } => 0,
        }
    }

    #[must_use]
    pub fn deltas_added(&self) -> u64 {
        match self {
            Self::Incremental { deltas_added, .. }
            | Self::FullRebuild { deltas_added, .. }
            | Self::HistoryRewriteFallback { deltas_added, .. } => *deltas_added,
            Self::NoChange { .. } => 0,
        }
    }
}

/// Anchor that controls how many commits the indexer walks. Mirrors the
/// `commit_depth` workspace-config knob (default 5000).
#[derive(Clone, Debug)]
pub struct GitIndexerOptions {
    /// Maximum number of commits to walk from HEAD. `None` walks the entire
    /// history. Defaults to `Some(5000)` so a one-off `gather-step index` on a
    /// huge repo does not silently turn into a multi-minute operation.
    pub commit_depth: Option<usize>,
    /// Maximum number of commits to process in a single incremental sync
    /// before forcing a fall-back to the history-rewrite (full rebuild) path.
    /// Without this cap, returning to a stale workspace after a long offline
    /// period would walk an unbounded number of commits in one go, holding
    /// large in-memory delta vectors. Defaults to `Some(20_000)` — generous
    /// enough that normal day-to-day syncs are never affected, but small
    /// enough that a year-long catch-up degrades to a full rebuild instead
    /// of a memory blow-up.
    pub max_incremental_commits: Option<usize>,
    /// Maximum number of per-file deltas the indexer will record for a single
    /// commit. Hostile or pathological commits (mass renames, gigantic merge
    /// commits, generated-code dumps) can otherwise produce delta vectors
    /// large enough to exhaust memory while every blob is loaded for
    /// line-stat diffing. When the cap is exceeded the commit is recorded
    /// with no per-file deltas and a warning is logged.
    pub max_deltas_per_commit: Option<usize>,
    /// Tokens passed through to [`detect_decision_signal`]. Owned strings so
    /// callers (including the workspace config loader) can supply
    /// project-tuned tokens without leaking statics.
    pub decision_signal_tokens: Vec<String>,
}

impl Default for GitIndexerOptions {
    fn default() -> Self {
        Self {
            commit_depth: Some(5000),
            max_incremental_commits: Some(20_000),
            max_deltas_per_commit: Some(10_000),
            decision_signal_tokens: DEFAULT_DECISION_SIGNALS
                .iter()
                .map(|signal| (*signal).to_owned())
                .collect(),
        }
    }
}

/// Errors raised while opening or walking a git repository. All variants are
/// `#[from]` on the underlying gix error so callers can `?` through any layer.
/// Each variant boxes its source so the enum stays small (sub-32-byte) and
/// cold paths do not pay the cost of inlining gix's larger error types.
#[derive(Debug, Error)]
pub enum GitHistoryError {
    #[error("failed to open git repository: {0}")]
    Open(#[from] Box<gix::open::Error>),
    #[error("failed to resolve HEAD: {0}")]
    Head(#[from] Box<gix::reference::head_id::Error>),
    #[error("failed to look up commit object: {0}")]
    FindCommit(#[from] Box<gix::object::find::existing::with_conversion::Error>),
    #[error("failed to look up blob object: {0}")]
    FindObject(#[from] Box<gix::object::find::existing::Error>),
    #[error("failed to walk commit graph: {0}")]
    RevWalk(#[from] Box<gix::revision::walk::Error>),
    #[error("rev-walk iteration failed: {0}")]
    RevWalkIter(#[from] Box<gix::revision::walk::iter::Error>),
    #[error("failed to decode commit metadata: {0}")]
    Decode(#[from] Box<gix::object::commit::Error>),
    #[error("failed to decode commit signature: {0}")]
    DecodeSignature(#[from] Box<gix::objs::decode::Error>),
    #[error("failed to parse commit author timestamp: {0}")]
    AuthorTime(#[from] Box<gix::date::Error>),
    #[error("failed to initialise tree-diff platform: {0}")]
    TreeDiffOptions(#[from] Box<gix::diff::options::init::Error>),
    #[error("failed to walk tree changes: {0}")]
    TreeDiffWalk(#[from] Box<gix::object::tree::diff::for_each::Error>),
    #[error("failed to initialise blob-diff resource cache: {0}")]
    BlobDiffCacheInit(#[from] Box<gix::repository::diff_resource_cache::Error>),
    #[error("failed to initialise per-change blob diff: {0}")]
    BlobDiffInit(#[from] Box<gix::object::blob::diff::init::Error>),
    #[error("failed to prepare blob diff for line counts: {0}")]
    BlobDiffPrepare(#[from] Box<gix::diff::blob::platform::prepare_diff::Error>),
}

/// Entry point for git-history ingestion. Construct one indexer per repo,
/// then call [`GitHistoryIndexer::walk`] (pure extraction). Persistence and
/// sync-outcome dispatch are layered on top by `sync`.
///
/// Holds no `gix::Repository` of its own — repositories are opened on demand
/// inside `walk`/`sync` so the indexer is safe to share across threads and
/// reuse between sync passes.
#[derive(Clone, Debug)]
pub struct GitHistoryIndexer {
    source: GitRepoSource,
    repo: String,
    options: GitIndexerOptions,
}

impl GitHistoryIndexer {
    pub fn new(source: GitRepoSource, repo: impl Into<String>) -> Self {
        Self {
            source,
            repo: repo.into(),
            options: GitIndexerOptions::default(),
        }
    }

    #[must_use]
    pub fn with_options(mut self, options: GitIndexerOptions) -> Self {
        self.options = options;
        self
    }

    #[must_use]
    pub fn repo(&self) -> &str {
        &self.repo
    }

    #[must_use]
    pub fn source(&self) -> &GitRepoSource {
        &self.source
    }

    #[must_use]
    pub fn options(&self) -> &GitIndexerOptions {
        &self.options
    }

    /// Walks the repository's commit history from HEAD, stopping at
    /// `stop_at_sha` (exclusive) when supplied, and returns the extracted
    /// [`CommitFact`]s in **newest-first** rev-walk order.
    ///
    /// `stop_at_sha = Some(sha)` performs the incremental sync's "rev-walk
    /// `^stop..HEAD`" semantics: the walk stops when it reaches a commit
    /// whose SHA matches `sha`. The matching commit itself is **not**
    /// included (its ancestors have already been recorded by a prior sync).
    /// If the walk completes without ever encountering `sha`, the function
    /// returns `Ok(None)` so the caller can detect history-rewrite scenarios
    /// — distinct from `Ok(Some(_))` with an empty vector, which means HEAD
    /// already equals the anchor.
    pub fn walk(
        &self,
        stop_at_sha: Option<&str>,
    ) -> Result<Option<Vec<CommitFact>>, GitHistoryError> {
        let repo = self.open_repo()?;
        match stop_at_sha {
            Some(anchor) => self.walk_anchored(&repo, anchor),
            None => self.walk_full(&repo).map(Some),
        }
    }

    /// Resolves the repository's current HEAD commit SHA. Useful for callers
    /// that want to record `repo_sync_state.last_commit_sha` after a
    /// successful sync without re-walking history.
    pub fn head_sha(&self) -> Result<String, GitHistoryError> {
        let repo = self.open_repo()?;
        Ok(repo.head_id().map_err(Box::new)?.detach().to_string())
    }

    /// Walks the repository, persists the new commits + per-file deltas via
    /// `store`, advances `repo_sync_state.last_commit_sha`, and returns the
    /// outcome describing which sync branch ran.
    ///
    /// Decision tree:
    ///  1. Read the recorded anchor from `store.get_last_commit_sha(repo)`.
    ///  2. Resolve HEAD via `gix`. If HEAD == anchor → `NoChange`.
    ///  3. If anchor is `None` → walk full history → `FullRebuild`.
    ///  4. If anchor is reachable from HEAD → walk just `anchor..HEAD` →
    ///     `Incremental`.
    ///  5. If anchor is **not** reachable → assume rebase/force-push, purge
    ///     the repo's existing commit rows (cascading deltas), walk full
    ///     history → `HistoryRewriteFallback`.
    ///
    /// `synced_at_unix` is the wall-clock seconds the caller wants recorded
    /// in `repo_sync_state.synced_at`. Passing it in instead of calling
    /// `SystemTime::now()` here keeps the indexer easy to reason about under
    /// test and lets callers replay sync runs with deterministic timestamps.
    pub fn sync<S: MetadataStore>(
        &self,
        store: &S,
        synced_at_unix: i64,
    ) -> Result<HistorySyncOutcome, GitHistorySyncError> {
        let repo = self.open_repo().map_err(GitHistorySyncError::Git)?;
        let head_sha = repo
            .head_id()
            .map_err(|err| GitHistorySyncError::Git(GitHistoryError::Head(Box::new(err))))?
            .detach()
            .to_string();

        let anchor = store
            .get_last_commit_sha(&self.repo)
            .map_err(GitHistorySyncError::Storage)?;

        // Step 2: HEAD already at anchor.
        if let Some(ref anchor_sha) = anchor
            && anchor_sha.eq_ignore_ascii_case(&head_sha)
        {
            return Ok(HistorySyncOutcome::NoChange {
                repo: self.repo.clone(),
                head_sha,
            });
        }

        // Steps 3 - 5.
        let (facts, outcome_kind) = if let Some(anchor_sha) = anchor {
            // Anchored walk. `walk_anchored` returns `None` when the anchor
            // is no longer reachable from HEAD — that is the history-rewrite
            // signal we recover from below.
            if let Some(facts) = self
                .walk_anchored(&repo, &anchor_sha)
                .map_err(GitHistorySyncError::Git)?
            {
                (facts, OutcomeKind::Incremental)
            } else {
                // History rewrite: prove the rebuild walk can succeed, then
                // replace the stored repo slice atomically below.
                let facts = self.walk_full(&repo).map_err(GitHistorySyncError::Git)?;
                (
                    facts,
                    OutcomeKind::HistoryRewriteFallback {
                        previous: anchor_sha,
                    },
                )
            }
        } else {
            // First-ever sync for this repo: full unanchored walk.
            let facts = self.walk_full(&repo).map_err(GitHistorySyncError::Git)?;
            (facts, OutcomeKind::FullRebuild)
        };

        let (commit_records, delta_records) = facts_to_records(&facts);
        let commits_added = u64::try_from(commit_records.len()).unwrap_or(u64::MAX);
        let deltas_added = u64::try_from(delta_records.len()).unwrap_or(u64::MAX);

        match outcome_kind {
            OutcomeKind::Incremental => {
                // Incremental sync is monotonically additive: new commits and
                // their deltas extend the existing slice. Order matters because
                // delta FKs reference commits.
                store
                    .insert_commits(&commit_records)
                    .map_err(GitHistorySyncError::Storage)?;
                store
                    .upsert_commit_file_deltas(&delta_records)
                    .map_err(GitHistorySyncError::Storage)?;
                store
                    .set_last_commit_sha(&self.repo, &head_sha, synced_at_unix)
                    .map_err(GitHistorySyncError::Storage)?;
            }
            OutcomeKind::FullRebuild | OutcomeKind::HistoryRewriteFallback { .. } => {
                // Full rebuild and history-rewrite fallback both replace the
                // entire repo slice. `replace_repo_history` runs the purge,
                // re-insert, and anchor advance inside one SQLite transaction
                // so a partial failure cannot leave the repo with empty
                // commits but a stale anchor.
                store
                    .replace_repo_history(
                        &self.repo,
                        &commit_records,
                        &delta_records,
                        &head_sha,
                        synced_at_unix,
                    )
                    .map_err(GitHistorySyncError::Storage)?;
            }
        }

        Ok(match outcome_kind {
            OutcomeKind::FullRebuild => HistorySyncOutcome::FullRebuild {
                repo: self.repo.clone(),
                commits_added,
                deltas_added,
                new_head_sha: head_sha,
            },
            OutcomeKind::Incremental => HistorySyncOutcome::Incremental {
                repo: self.repo.clone(),
                commits_added,
                deltas_added,
                new_head_sha: head_sha,
            },
            OutcomeKind::HistoryRewriteFallback { previous } => {
                HistorySyncOutcome::HistoryRewriteFallback {
                    repo: self.repo.clone(),
                    previous_anchor_sha: previous,
                    commits_added,
                    deltas_added,
                    new_head_sha: head_sha,
                }
            }
        })
    }

    fn open_repo(&self) -> Result<Repository, GitHistoryError> {
        match &self.source {
            GitRepoSource::Path(path) => open_repo(path),
        }
    }

    /// Walks the full history from `HEAD`, capped by `options.commit_depth`.
    /// Used for first-ever syncs and for the history-rewrite recovery path.
    /// Returns the facts directly: no `Option` because there is no anchor
    /// that could fail to resolve.
    fn walk_full(&self, repo: &Repository) -> Result<Vec<CommitFact>, GitHistoryError> {
        self.walk_inner(repo, None).map(WalkOutcome::facts_or_empty)
    }

    /// Walks from `HEAD` back to (but not including) `anchor_sha`. Returns
    /// `None` when the anchor is no longer reachable — the history-rewrite
    /// signal that callers translate into a full-rebuild fallback.
    fn walk_anchored(
        &self,
        repo: &Repository,
        anchor_sha: &str,
    ) -> Result<Option<Vec<CommitFact>>, GitHistoryError> {
        let head_id = repo.head_id().map_err(Box::new)?.detach();
        // Optimisation: HEAD already at the anchor means nothing to walk.
        if anchor_sha.eq_ignore_ascii_case(&head_id.to_string()) {
            debug!(
                repo = %self.repo,
                head = %head_id,
                "git history walk: HEAD already at sync anchor, nothing to walk",
            );
            return Ok(Some(Vec::new()));
        }
        match self.walk_inner(repo, Some(anchor_sha))? {
            WalkOutcome::Reached(facts) => Ok(Some(facts)),
            WalkOutcome::AnchorMissing => Ok(None),
        }
    }

    fn walk_inner(
        &self,
        repo: &Repository,
        stop_at_sha: Option<&str>,
    ) -> Result<WalkOutcome, GitHistoryError> {
        let head_id = repo.head_id().map_err(Box::new)?.detach();

        let walk = repo
            .rev_walk([head_id])
            // ByCommitTime(NewestFirst) is what `git log` uses by default and
            // matches the order callers want when they later persist commits
            // and update `last_commit_sha` to the newest observed SHA.
            .sorting(Sorting::ByCommitTime(CommitTimeOrder::NewestFirst))
            .all()
            .map_err(Box::new)?;

        let mut facts = Vec::new();
        let mut anchor_seen = false;
        // Per-mode budget: full walks honour `commit_depth`; anchored walks
        // honour `max_incremental_commits` so a years-stale workspace cannot
        // walk an unbounded number of commits in a single sync.
        let walk_limit = match stop_at_sha {
            Some(_) => self.options.max_incremental_commits,
            None => self.options.commit_depth,
        };

        for info in walk {
            let info = info.map_err(Box::new)?;
            let id = info.id().detach();
            let sha = id.to_string();

            if let Some(anchor) = stop_at_sha
                && anchor.eq_ignore_ascii_case(&sha)
            {
                anchor_seen = true;
                break;
            }

            let commit = repo.find_commit(id).map_err(Box::new)?;
            let fact = self.extract_commit_fact(repo, &commit)?;
            facts.push(fact);

            if let Some(limit) = walk_limit
                && facts.len() >= limit
            {
                if stop_at_sha.is_some() {
                    // Incremental sync hit the cap before reaching the anchor:
                    // the workspace is stale enough that the cheap path is no
                    // longer cheap. Surface as `AnchorMissing` so the sync
                    // layer triggers a transactional full rebuild instead of
                    // partially advancing the anchor and losing intermediate
                    // commits forever.
                    warn!(
                        repo = %self.repo,
                        limit,
                        "git history walk: incremental cap reached before anchor; falling back to full rebuild",
                    );
                    return Ok(WalkOutcome::AnchorMissing);
                }
                debug!(
                    repo = %self.repo,
                    limit,
                    "git history walk: commit_depth limit reached",
                );
                break;
            }
        }

        if let Some(anchor) = stop_at_sha
            && !anchor_seen
        {
            // The previous anchor is no longer reachable from HEAD: history
            // rewrite (rebase, force-push, filter-branch, …). Surface so the
            // sync layer can decide how to recover.
            warn!(
                repo = %self.repo,
                anchor = %anchor,
                "git history walk: previous anchor SHA not found in current HEAD ancestry",
            );
            return Ok(WalkOutcome::AnchorMissing);
        }

        Ok(WalkOutcome::Reached(facts))
    }

    fn extract_commit_fact(
        &self,
        repo: &Repository,
        commit: &gix::Commit<'_>,
    ) -> Result<CommitFact, GitHistoryError> {
        let id = commit.id().detach();
        let author = commit.author().map_err(Box::new)?;
        // `author_email` is used as an identity key for ownership aggregation,
        // so it must be a deterministic function of the raw bytes. `to_str_lossy`
        // would replace invalid UTF-8 with U+FFFD, which collapses *different*
        // invalid byte sequences into the same string and silently merges what
        // are conceptually distinct identities. `normalize_author_email_bytes`
        // preserves valid UTF-8 verbatim and percent-escapes anything else, so
        // round-trips are stable and identical raw bytes always produce the
        // same key.
        let author_email = normalize_author_email_bytes(author.email.as_ref());
        let author_date_unix = author.time().map_err(Box::new)?.seconds;
        let message = commit.message_raw_sloppy().to_str_lossy().into_owned();

        let parent_ids: Vec<ObjectId> = commit.parent_ids().map(gix::Id::detach).collect();
        let parent_count = parent_ids.len();

        let signal_refs: Vec<&str> = self
            .options
            .decision_signal_tokens
            .iter()
            .map(String::as_str)
            .collect();

        let file_deltas = if let Some(parent_id) = parent_ids.first() {
            let parent_commit = repo.find_commit(*parent_id).map_err(Box::new)?;
            let parent_tree = parent_commit.tree().map_err(Box::new)?;
            let commit_tree = commit.tree().map_err(Box::new)?;
            let deltas = collect_file_deltas(
                repo,
                &self.repo,
                &id.to_string(),
                &parent_tree,
                &commit_tree,
                self.options.max_deltas_per_commit,
            )?;
            // If the cap kicked in `collect_file_deltas` will have stopped
            // mid-iteration; we record the commit shell with no deltas so
            // a hostile or pathological commit cannot poison hotspot/owner
            // aggregates with truncated, biased data.
            if let Some(limit) = self.options.max_deltas_per_commit
                && deltas.len() >= limit
            {
                warn!(
                    repo = %self.repo,
                    sha = %id,
                    limit,
                    "git history walk: per-commit delta cap reached; recording commit without deltas",
                );
                Vec::new()
            } else {
                deltas
            }
        } else {
            // Initial commit: by convention we record the commit itself but
            // emit no deltas. Callers that need initial-commit deltas
            // (e.g. ownership seeding) can opt in by diffing against the
            // empty tree explicitly; the default here keeps incremental sync
            // semantics symmetrical with non-initial commits.
            Vec::new()
        };

        Ok(CommitFact {
            repo: self.repo.clone(),
            sha: id.to_string(),
            author_email,
            author_date_unix,
            message: message.clone(),
            classification: classify_commit_message(&message).map(str::to_owned),
            pr_number: extract_pr_number(&message),
            has_decision_signal: detect_decision_signal(&message, &signal_refs),
            parent_count,
            file_deltas,
        })
    }
}

fn open_repo(path: &Path) -> Result<Repository, GitHistoryError> {
    // Open with reduced trust so that a malicious `.git/config` inside an
    // indexed repository cannot influence pack/diff behaviour or exhaust
    // resources.  `Options::default_for_level(Reduced)` disables system,
    // user, git-binary, and environment configuration sources while keeping
    // repository-local config and include directives, which is the minimum
    // needed for correct history traversal.
    let opts = gix::open::Options::default_for_level(gix::sec::Trust::Reduced);
    Ok(gix::open_opts(path, opts).map_err(Box::new)?)
}

/// Normalises raw author-email bytes into a deterministic string suitable for
/// use as an identity key. Valid UTF-8 passes through unchanged; bytes that
/// would otherwise be replaced by U+FFFD (the lossy-conversion fallback) are
/// percent-escaped so distinct invalid byte sequences map to distinct keys.
///
/// Example: `b"alice@example.com"` → `"alice@example.com"` (verbatim).
/// Example: `b"alice@\xFF.com"` → `"alice@%FF.com"`.
///
/// Without this, `to_str_lossy()` would collapse `b"\xFF"` and `b"\xFE"` into
/// the same `"\u{FFFD}"` string, silently merging two distinct identities and
/// distorting downstream ownership aggregates.
fn normalize_author_email_bytes(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    if let Ok(text) = std::str::from_utf8(bytes) {
        return text.to_owned();
    }
    // Walk the bytes via `Utf8Chunks` so valid runs stay readable and only
    // the invalid bytes are percent-escaped. This preserves the distinction
    // between different invalid byte sequences that `to_str_lossy` would
    // otherwise collapse into U+FFFD.
    let mut out = String::with_capacity(bytes.len());
    for chunk in bytes.utf8_chunks() {
        out.push_str(chunk.valid());
        for &byte in chunk.invalid() {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

/// Errors raised by [`GitHistoryIndexer::sync`]. The two-variant split keeps
/// git-side and storage-side failures separable so callers can decide
/// independently — for instance, a `Storage` error during a full rebuild
/// might be retried, whereas a `Git` open failure is generally terminal.
#[derive(Debug, Error)]
pub enum GitHistorySyncError {
    #[error(transparent)]
    Git(GitHistoryError),
    #[error(transparent)]
    Storage(MetadataStoreError),
}

/// Internal scratch enum for [`GitHistoryIndexer::sync`]. Captures which
/// branch of the decision tree fired so we can build the corresponding
/// public [`HistorySyncOutcome`] after persistence completes.
enum OutcomeKind {
    FullRebuild,
    Incremental,
    HistoryRewriteFallback { previous: String },
}

/// Result of a low-level rev-walk pass. Distinguishes a successful walk
/// (anchor reached or no anchor) from the case where a previously stored
/// anchor SHA is no longer reachable from `HEAD` — the history-rewrite
/// signal that callers translate into a full-rebuild fallback.
enum WalkOutcome {
    Reached(Vec<CommitFact>),
    AnchorMissing,
}

impl WalkOutcome {
    /// Used by full-walk callers that have no anchor to miss; the
    /// `AnchorMissing` arm cannot fire because the inner walk only
    /// emits it when `stop_at_sha.is_some()`.
    fn facts_or_empty(self) -> Vec<CommitFact> {
        match self {
            Self::Reached(facts) => facts,
            Self::AnchorMissing => Vec::new(),
        }
    }
}

/// Translates a slice of in-memory [`CommitFact`]s into the storage-layer
/// [`CommitRecord`] + [`CommitFileDeltaRecord`] pair. Splitting them here
/// keeps `GitHistoryIndexer::sync` focused on control flow rather than data
/// reshape, and lets future callers (e.g. the analytics crate) reuse the
/// same projection without touching the indexer.
fn facts_to_records(facts: &[CommitFact]) -> (Vec<CommitRecord>, Vec<CommitFileDeltaRecord>) {
    let mut commits = Vec::with_capacity(facts.len());
    let mut deltas = Vec::new();

    for fact in facts {
        commits.push(CommitRecord {
            sha: fact.sha.clone(),
            repo: fact.repo.clone(),
            author_email: fact.author_email.clone(),
            date: fact.author_date_unix,
            message: fact.message.clone(),
            classification: fact.classification.clone(),
            // `files_changed` reflects the number of persisted per-file deltas
            // attached to this commit.
            files_changed: i64::try_from(fact.file_deltas.len()).unwrap_or(i64::MAX),
            insertions: i64::try_from(fact.total_insertions()).unwrap_or(i64::MAX),
            deletions: i64::try_from(fact.total_deletions()).unwrap_or(i64::MAX),
            has_decision_signal: fact.has_decision_signal,
            pr_number: fact.pr_number.and_then(|n| i64::try_from(n).ok()),
        });

        for delta in &fact.file_deltas {
            deltas.push(CommitFileDeltaRecord {
                repo: fact.repo.clone(),
                sha: fact.sha.clone(),
                file_path: delta.file_path.clone(),
                change_kind: change_kind_to_storage(delta.change_kind),
                insertions: delta.insertions.and_then(|v| i64::try_from(v).ok()),
                deletions: delta.deletions.and_then(|v| i64::try_from(v).ok()),
                old_path: delta.old_path.clone(),
            });
        }
    }

    (commits, deltas)
}

fn change_kind_to_storage(kind: CommitFileChangeKind) -> StoredChangeKind {
    match kind {
        CommitFileChangeKind::Added => StoredChangeKind::Added,
        CommitFileChangeKind::Modified => StoredChangeKind::Modified,
        CommitFileChangeKind::Deleted => StoredChangeKind::Deleted,
        CommitFileChangeKind::Renamed => StoredChangeKind::Renamed,
        CommitFileChangeKind::Copied => StoredChangeKind::Copied,
        CommitFileChangeKind::TypeChanged => StoredChangeKind::TypeChanged,
    }
}

fn collect_file_deltas(
    repo: &Repository,
    repo_name: &str,
    commit_sha: &str,
    parent_tree: &gix::Tree<'_>,
    commit_tree: &gix::Tree<'_>,
    cap: Option<usize>,
) -> Result<Vec<CommitFileDelta>, GitHistoryError> {
    use gix::object::tree::diff::Change;

    let mut deltas = Vec::new();
    // One resource cache per `collect_file_deltas` call, reused across all
    // changes. gix's blob-diff platform internally re-uses interned blob
    // text via this cache, so we get O(distinct blobs) loads instead of
    // O(changes) loads. `clear_resource_cache_keep_allocation` between
    // iterations resets per-change state without freeing the buffer.
    let mut resource_cache = repo.diff_resource_cache_for_tree_diff().map_err(Box::new)?;
    let mut platform = parent_tree
        .changes()
        .map_err(|err| GitHistoryError::TreeDiffOptions(Box::new(err)))?;

    platform
        .for_each_to_obtain_tree(commit_tree, |change| -> Result<_, GitHistoryError> {
            // Bail before the next blob load if the per-commit cap was hit.
            // The caller treats `deltas.len() >= cap` as a poison signal and
            // discards the partial set, so we can stop walking immediately.
            if let Some(limit) = cap
                && deltas.len() >= limit
            {
                return Ok(std::ops::ControlFlow::Break(()));
            }
            // Note: `git`'s default rewrite tracking is on (50% similarity),
            // so renames surface as `Change::Rewrite`. Copies are off by
            // default; if a future caller turns them on, they too come back
            // as `Rewrite { copy: true, .. }`.
            let delta = match &change {
                Change::Addition {
                    location,
                    entry_mode,
                    ..
                } => {
                    if !entry_mode.is_blob() {
                        return Ok(std::ops::ControlFlow::Continue(()));
                    }
                    let (insertions, deletions) = change_line_counts_lenient(
                        &change,
                        &mut resource_cache,
                        repo_name,
                        commit_sha,
                        location,
                    );
                    CommitFileDelta {
                        file_path: location.to_str_lossy().into_owned(),
                        change_kind: CommitFileChangeKind::Added,
                        insertions,
                        deletions,
                        old_path: None,
                    }
                }
                Change::Deletion {
                    location,
                    entry_mode,
                    ..
                } => {
                    if !entry_mode.is_blob() {
                        return Ok(std::ops::ControlFlow::Continue(()));
                    }
                    let (insertions, deletions) = change_line_counts_lenient(
                        &change,
                        &mut resource_cache,
                        repo_name,
                        commit_sha,
                        location,
                    );
                    CommitFileDelta {
                        file_path: location.to_str_lossy().into_owned(),
                        change_kind: CommitFileChangeKind::Deleted,
                        insertions,
                        deletions,
                        old_path: None,
                    }
                }
                Change::Modification {
                    location,
                    previous_entry_mode,
                    entry_mode,
                    ..
                } => {
                    let kind = if previous_entry_mode == entry_mode {
                        CommitFileChangeKind::Modified
                    } else {
                        CommitFileChangeKind::TypeChanged
                    };
                    if !entry_mode.is_blob() && !previous_entry_mode.is_blob() {
                        return Ok(std::ops::ControlFlow::Continue(()));
                    }
                    let (insertions, deletions) = change_line_counts_lenient(
                        &change,
                        &mut resource_cache,
                        repo_name,
                        commit_sha,
                        location,
                    );
                    CommitFileDelta {
                        file_path: location.to_str_lossy().into_owned(),
                        change_kind: kind,
                        insertions,
                        deletions,
                        old_path: None,
                    }
                }
                Change::Rewrite {
                    source_location,
                    location,
                    diff,
                    copy,
                    ..
                } => {
                    // gix already populates `diff` with insertion/removal
                    // counts for rewrites (it has to compute them to decide
                    // similarity); fall back to the full path if it's
                    // somehow absent.
                    let (insertions, deletions) = diff.as_ref().map_or_else(
                        || {
                            change_line_counts_lenient(
                                &change,
                                &mut resource_cache,
                                repo_name,
                                commit_sha,
                                location,
                            )
                        },
                        |stats| {
                            (
                                Some(u64::from(stats.insertions)),
                                Some(u64::from(stats.removals)),
                            )
                        },
                    );
                    CommitFileDelta {
                        file_path: location.to_str_lossy().into_owned(),
                        change_kind: if *copy {
                            CommitFileChangeKind::Copied
                        } else {
                            CommitFileChangeKind::Renamed
                        },
                        insertions,
                        deletions,
                        old_path: Some(source_location.to_str_lossy().into_owned()),
                    }
                }
            };
            deltas.push(delta);
            // Reset per-change cache state but keep the underlying buffer
            // for the next iteration.
            resource_cache.clear_resource_cache_keep_allocation();
            // `Action` is a type alias for `ControlFlow<()>`; `Continue(())`
            // tells the platform to keep emitting changes for this tree.
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .map_err(|err| GitHistoryError::TreeDiffWalk(Box::new(err)))?;

    // Stable, deterministic ordering. The diff platform yields changes in
    // tree-walk order which is already deterministic, but we sort by path
    // so storage rows and downstream snapshots compare cleanly across runs.
    deltas.sort_by(|a, b| a.file_path.cmp(&b.file_path));
    Ok(deltas)
}

/// Returns `(insertions, deletions)` for a single tree-diff change using
/// gix's built-in blob-diff platform. Returns `(None, None)` for binary
/// blobs or any case where line counts cannot be computed (e.g. one side
/// is a non-blob).
///
/// Replaces the previous manual `similar::TextDiff` pass: gix's path uses
/// `imara-diff` (histogram by default), respects the repo's `diff.algorithm`
/// config, handles binary detection via gitattributes, and reuses the
/// resource cache across changes — none of which the manual path did.
fn change_line_counts(
    change: &gix::object::tree::diff::Change<'_, '_, '_>,
    resource_cache: &mut gix::diff::blob::Platform,
) -> Result<(Option<u64>, Option<u64>), GitHistoryError> {
    let mut platform = change.diff(resource_cache).map_err(Box::new)?;
    match platform.line_counts().map_err(Box::new)? {
        Some(counter) => Ok((
            Some(u64::from(counter.insertions)),
            Some(u64::from(counter.removals)),
        )),
        // `None` is gix's signal that one or both sides are binary, so we
        // mirror the previous behaviour of returning `(None, None)` rather
        // than (Some(0), Some(0)) — a binary change is not a zero-line
        // change.
        None => Ok((None, None)),
    }
}

/// Lenient wrapper around [`change_line_counts`] for use inside the tree-walk
/// callback.
///
/// A single bad blob (missing object in a shallow clone, packfile corruption,
/// LFS pointer without data, etc.) previously propagated a `BlobDiffInit` /
/// `BlobDiffPrepare` error out of the callback and poisoned the ENTIRE
/// commit's diff — which, via the caller's `?`, aborted the whole history
/// walk, causing repo analytics to degrade and drop ALL commit deltas for the
/// repo.
///
/// This wrapper contains the blast radius to the single failing change: on
/// error it emits a [`tracing::trace!`] carrying `(repo, sha, path, error)`
/// and returns `(None, None)` so the delta is still recorded with the correct
/// change-kind but without line counts. The tree walk continues; other changes
/// in the same commit still get full data; and the global analytics-degraded
/// path now only fires on truly structural problems (object DB unreadable,
/// malformed trees, rev-walk failures) that correctly abort the walk.
///
/// The log level is [`tracing::trace!`] rather than `warn!` because per-change
/// blob-diff failures are expected operational noise in any workspace that
/// contains shallow clones or LFS objects — emitting `warn!` in a hot inner
/// loop would flood logs without adding actionable signal.
fn change_line_counts_lenient(
    change: &gix::object::tree::diff::Change<'_, '_, '_>,
    resource_cache: &mut gix::diff::blob::Platform,
    repo_name: &str,
    commit_sha: &str,
    path: &gix::bstr::BStr,
) -> (Option<u64>, Option<u64>) {
    match change_line_counts(change, resource_cache) {
        Ok(counts) => counts,
        Err(error) => {
            trace!(
                repo = %repo_name,
                sha = %commit_sha,
                path = %path.to_str_lossy(),
                error = %error,
                "git history: per-change blob diff failed; recording delta without line counts",
            );
            (None, None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::process::Command;

    use pretty_assertions::assert_eq;

    use super::{
        CommitFact, CommitFileChangeKind, CommitFileDelta, GitHistoryIndexer, GitRepoSource,
        HistorySyncOutcome, normalize_author_email_bytes,
    };

    /// Minimal hand-rolled `TempDir` that deletes itself on drop. Avoids
    /// adding a `tempfile` dev-dependency.
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "gather-step-git-test-{label}-{}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).expect("TempDir::new: create_dir_all failed");
            Self { path }
        }

        fn path(&self) -> &std::path::Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    /// Runs a shell command inside `dir`, panicking with the full stderr on
    /// failure. Used by test helpers that build synthetic git repos.
    fn git(dir: &std::path::Path, args: &[&str]) {
        let output = Command::new("git")
            .args(["-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false"])
            .args(args)
            .current_dir(dir)
            .env("GIT_AUTHOR_NAME", "Test")
            .env("GIT_AUTHOR_EMAIL", "test@example.com")
            .env("GIT_COMMITTER_NAME", "Test")
            .env("GIT_COMMITTER_EMAIL", "test@example.com")
            .env("GIT_AUTHOR_DATE", "2024-01-01T00:00:00Z")
            .env("GIT_COMMITTER_DATE", "2024-01-01T00:00:00Z")
            .output()
            .expect("git command failed to spawn");
        assert!(
            output.status.success(),
            "git {args:?} failed:\n{}",
            String::from_utf8_lossy(&output.stderr),
        );
    }

    #[test]
    fn normalize_author_email_passes_through_valid_utf8_unchanged() {
        assert_eq!(
            normalize_author_email_bytes(b"alice@example.com"),
            "alice@example.com",
        );
        // Non-ASCII but valid UTF-8: still verbatim.
        let raw = "tést@example.com".as_bytes();
        assert_eq!(normalize_author_email_bytes(raw), "tést@example.com");
    }

    #[test]
    fn normalize_author_email_keeps_distinct_invalid_byte_sequences_distinct() {
        // Without normalization, both `\xFF` and `\xFE` would collapse into
        // U+FFFD via `to_str_lossy` and silently merge two identities into
        // one ownership bucket. The hex-escape preserves the distinction.
        let one = normalize_author_email_bytes(b"alice@\xFF.com");
        let two = normalize_author_email_bytes(b"alice@\xFE.com");
        assert_eq!(one, "alice@%FF.com");
        assert_eq!(two, "alice@%FE.com");
        assert_ne!(one, two);
    }

    #[test]
    fn total_insertions_skips_binary_diffs_rather_than_treating_them_as_zero() {
        let fact = CommitFact {
            repo: "service-a".to_owned(),
            sha: "abc".to_owned(),
            author_email: "alice@example.com".to_owned(),
            author_date_unix: 0,
            message: "feat: x".to_owned(),
            classification: Some("feat".to_owned()),
            pr_number: None,
            has_decision_signal: false,
            parent_count: 1,
            file_deltas: vec![
                CommitFileDelta {
                    file_path: "src/a.rs".to_owned(),
                    change_kind: CommitFileChangeKind::Modified,
                    insertions: Some(10),
                    deletions: Some(2),
                    old_path: None,
                },
                CommitFileDelta {
                    file_path: "img.png".to_owned(),
                    change_kind: CommitFileChangeKind::Modified,
                    insertions: None,
                    deletions: None,
                    old_path: None,
                },
                CommitFileDelta {
                    file_path: "src/b.rs".to_owned(),
                    change_kind: CommitFileChangeKind::Added,
                    insertions: Some(5),
                    deletions: Some(0),
                    old_path: None,
                },
            ],
        };

        // 10 + 5 = 15; the binary delta contributes nothing rather than being
        // misread as a zero-line modification.
        assert_eq!(fact.total_insertions(), 15);
        assert_eq!(fact.total_deletions(), 2);
    }

    #[test]
    fn outcome_accessors_dispatch_consistently_across_variants() {
        let cases = [
            HistorySyncOutcome::Incremental {
                repo: "service-a".to_owned(),
                commits_added: 3,
                deltas_added: 11,
                new_head_sha: "deadbeef".to_owned(),
            },
            HistorySyncOutcome::FullRebuild {
                repo: "billing".to_owned(),
                commits_added: 100,
                deltas_added: 250,
                new_head_sha: "cafe".to_owned(),
            },
            HistorySyncOutcome::HistoryRewriteFallback {
                repo: "audit".to_owned(),
                previous_anchor_sha: "old".to_owned(),
                commits_added: 50,
                deltas_added: 75,
                new_head_sha: "new".to_owned(),
            },
            HistorySyncOutcome::NoChange {
                repo: "service-c".to_owned(),
                head_sha: "same".to_owned(),
            },
        ];

        let observed: Vec<(&str, u64, u64)> = cases
            .iter()
            .map(|outcome| {
                (
                    outcome.repo(),
                    outcome.commits_added(),
                    outcome.deltas_added(),
                )
            })
            .collect();
        assert_eq!(
            observed,
            vec![
                ("service-a", 3, 11),
                ("billing", 100, 250),
                ("audit", 50, 75),
                ("service-c", 0, 0),
            ]
        );
    }

    /// Builds a synthetic two-commit git repo where the second commit's blob
    /// object is removed from the object database after the commit is created.
    /// This simulates a shallow clone / corrupted packfile scenario where a
    /// single blob is unreadable.
    ///
    /// Asserts that:
    /// 1. `walk` succeeds (does NOT return `Err`) — the blast radius is
    ///    contained to the single bad change.
    /// 2. The commit that contains the bad blob IS recorded (change-kind is
    ///    correct) but with `insertions = None` / `deletions = None`.
    /// 3. The commit that has no bad blob IS recorded with full line counts.
    #[test]
    fn missing_blob_does_not_abort_history_walk() {
        // Skip if git is not available (CI without git binary would fail).
        if Command::new("git").arg("--version").output().is_err() {
            return;
        }

        let tmp = TempDir::new("missing-blob");
        let repo_dir = tmp.path();

        // Initialise repo with a known identity so output is reproducible.
        git(repo_dir, &["init", "-b", "main"]);
        git(repo_dir, &["config", "user.email", "test@example.com"]);
        git(repo_dir, &["config", "user.name", "Test"]);

        // Commit 1: add two files.
        std::fs::write(repo_dir.join("good.txt"), "line1\nline2\nline3\n").expect("write good.txt");
        std::fs::write(repo_dir.join("bad.txt"), "alpha\nbeta\n").expect("write bad.txt");
        git(repo_dir, &["add", "."]);
        git(repo_dir, &["commit", "-m", "initial"]);

        // Commit 2: modify bad.txt (the blob we will later delete).
        std::fs::write(repo_dir.join("bad.txt"), "alpha\nbeta\ngamma\n").expect("write bad.txt v2");
        git(repo_dir, &["add", "bad.txt"]);
        git(repo_dir, &["commit", "-m", "modify bad.txt"]);

        // Commit 3: modify good.txt only — this commit's diff should succeed.
        std::fs::write(repo_dir.join("good.txt"), "line1\nline2\nline3\nline4\n")
            .expect("write good.txt v2");
        git(repo_dir, &["add", "good.txt"]);
        git(repo_dir, &["commit", "-m", "modify good.txt"]);

        // Find and delete the loose blob for bad.txt v2 so the diff for
        // commit 2 will fail with a missing-object error. We use `git
        // cat-file --batch-all-objects` to enumerate all loose objects,
        // identify blobs, and find the one that matches the content of
        // bad.txt v2.
        let hash = {
            let output = Command::new("git")
                .args(["hash-object", "bad.txt"])
                .current_dir(repo_dir)
                .output()
                .expect("git hash-object");
            String::from_utf8(output.stdout)
                .expect("utf8")
                .trim()
                .to_owned()
        };
        // Loose objects live at .git/objects/<2-char-prefix>/<38-char-suffix>.
        assert!(hash.len() == 40, "expected full SHA-1 hash");
        let (prefix, suffix) = hash.split_at(2);
        let obj_path = repo_dir
            .join(".git")
            .join("objects")
            .join(prefix)
            .join(suffix);
        std::fs::remove_file(&obj_path).expect("remove loose blob object");

        // Walk the repository. Must NOT error.
        let indexer = GitHistoryIndexer::new(
            GitRepoSource::from_path(repo_dir.to_path_buf()),
            "test-repo",
        );
        let facts = indexer
            .walk(None)
            .expect("walk must succeed despite missing blob")
            .expect("walk must return Some(facts)");

        // We should have 3 commits (initial + 2 modifying commits).
        assert_eq!(facts.len(), 3, "expected 3 commit facts");

        // The facts come back newest-first from the rev-walk.
        // facts[0] = commit 3 (modify good.txt) — diff succeeds → has counts.
        // facts[1] = commit 2 (modify bad.txt)  — diff fails  → counts are None.
        // facts[2] = initial commit              — no parent   → no deltas.

        let commit3 = &facts[0]; // newest
        let commit2 = &facts[1];
        let commit1 = &facts[2]; // oldest (initial)

        // Commit 3: good.txt modified — should have one delta with counts.
        assert_eq!(commit3.file_deltas.len(), 1);
        let good_delta = &commit3.file_deltas[0];
        assert_eq!(good_delta.file_path, "good.txt");
        assert_eq!(good_delta.change_kind, CommitFileChangeKind::Modified);
        // Line counts for a text diff should be Some.
        assert!(
            good_delta.insertions.is_some(),
            "good.txt diff should have insertion counts",
        );

        // Commit 2: bad.txt modified — delta IS recorded but counts are None.
        assert_eq!(
            commit2.file_deltas.len(),
            1,
            "commit2 must still have a delta"
        );
        let bad_delta = &commit2.file_deltas[0];
        assert_eq!(bad_delta.file_path, "bad.txt");
        assert_eq!(bad_delta.change_kind, CommitFileChangeKind::Modified);
        assert_eq!(
            bad_delta.insertions, None,
            "bad blob must yield None insertions, not an abort",
        );
        assert_eq!(
            bad_delta.deletions, None,
            "bad blob must yield None deletions, not an abort",
        );

        // Commit 1: initial commit — no parent, so no deltas by convention.
        assert_eq!(commit1.file_deltas.len(), 0, "initial commit has no deltas");
    }
}
