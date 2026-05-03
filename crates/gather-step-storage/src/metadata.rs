use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    sync::{Condvar, Mutex, MutexGuard},
    time::Duration,
};

use rustc_hash::FxHashMap;

use gather_step_core::{
    NodeId, NodeKind, PathId, PayloadContractRecord, PayloadInferenceKind, PayloadSide,
};
use gather_step_parser::resolve::ResolutionInput;
use rusqlite::{Connection, OptionalExtension, params};
use thiserror::Error;

use crate::incremental::TrackedPath;

// ---------------------------------------------------------------------------
// Compact bitcode encoding for unresolved_call_candidates payloads (S9)
//
// `ResolutionInput` uses `serde::Serialize/Deserialize` with `PathBuf` fields,
// which bitcode cannot encode natively.  The mirror types below carry the same
// logical fields but represent paths as raw OS-byte `Vec<u8>`, enabling the
// 2-4× payload size reduction that bitcode's varint + field-name-free encoding
// provides over JSON.
//
// Conversion from/to the real types is lossless: `PathBuf` ↔ OsString bytes
// round-trip perfectly via `std::os::unix::ffi::{OsStringExt, OsStrExt}` on
// Unix (and the equivalent UTF-16 / WTF-8 path on Windows).
// ---------------------------------------------------------------------------

/// Compact storage mirror of [`gather_step_parser::resolve::ImportBinding`].
#[derive(bitcode::Encode, bitcode::Decode)]
struct StoredImportBinding {
    local_name: String,
    imported_name: Option<String>,
    source: String,
    resolved_path: Option<Vec<u8>>,
    is_default: bool,
    is_namespace: bool,
    is_type_only: bool,
}

/// Compact storage mirror of [`gather_step_core::SourceSpan`].
#[derive(bitcode::Encode, bitcode::Decode)]
struct StoredSourceSpan {
    line_start: u32,
    line_len: u16,
    column_start: u16,
    column_len: u16,
}

/// Compact storage mirror of [`gather_step_parser::resolve::CallSite`].
#[derive(bitcode::Encode, bitcode::Decode)]
struct StoredCallSite {
    owner_id: NodeId,
    owner_file: NodeId,
    source_path: Vec<u8>,
    callee_name: String,
    callee_qualified_hint: Option<String>,
    span: Option<StoredSourceSpan>,
}

/// Compact storage mirror of [`ResolutionInput`].
#[derive(bitcode::Encode, bitcode::Decode)]
struct StoredResolutionInput {
    file_node: NodeId,
    file_path: Vec<u8>,
    import_bindings: Vec<StoredImportBinding>,
    call_sites: Vec<StoredCallSite>,
}

/// Encode a [`ResolutionInput`] to compact bitcode bytes.
///
/// Paths are stored as raw `OsString` bytes so the encoding is lossless even
/// for non-UTF-8 paths.  [`bitcode::encode`] is infallible; the function
/// always returns the encoded bytes directly.
fn encode_resolution_input(input: &ResolutionInput) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt as _;

    let stored = StoredResolutionInput {
        file_node: input.file_node,
        file_path: input.file_path.as_os_str().as_bytes().to_vec(),
        import_bindings: input
            .import_bindings
            .iter()
            .map(|b| StoredImportBinding {
                local_name: b.local_name.clone(),
                imported_name: b.imported_name.clone(),
                source: b.source.clone(),
                resolved_path: b
                    .resolved_path
                    .as_deref()
                    .map(|p| p.as_os_str().as_bytes().to_vec()),
                is_default: b.is_default,
                is_namespace: b.is_namespace,
                is_type_only: b.is_type_only,
            })
            .collect(),
        call_sites: input
            .call_sites
            .iter()
            .map(|cs| StoredCallSite {
                owner_id: cs.owner_id,
                owner_file: cs.owner_file,
                source_path: cs.source_path.as_os_str().as_bytes().to_vec(),
                callee_name: cs.callee_name.clone(),
                callee_qualified_hint: cs.callee_qualified_hint.clone(),
                span: cs.span.as_ref().map(|s| StoredSourceSpan {
                    line_start: s.line_start,
                    line_len: s.line_len,
                    column_start: s.column_start,
                    column_len: s.column_len,
                }),
            })
            .collect(),
    };
    bitcode::encode(&stored)
}

/// Decode a [`ResolutionInput`] from compact bitcode bytes produced by
/// [`encode_resolution_input`].
///
/// Returns a `rusqlite::Error` on decode failure so callers can propagate
/// through the existing `?` chains without additional error-type plumbing.
fn decode_resolution_input(bytes: &[u8]) -> Result<ResolutionInput, rusqlite::Error> {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt as _;

    let stored: StoredResolutionInput = bitcode::decode(bytes).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            bytes.len(),
            rusqlite::types::Type::Blob,
            Box::new(std::io::Error::other(error.to_string())),
        )
    })?;

    Ok(ResolutionInput {
        file_node: stored.file_node,
        file_path: PathBuf::from(OsString::from_vec(stored.file_path)),
        import_bindings: stored
            .import_bindings
            .into_iter()
            .map(|b| gather_step_parser::resolve::ImportBinding {
                local_name: b.local_name,
                imported_name: b.imported_name,
                source: b.source,
                resolved_path: b
                    .resolved_path
                    .map(|p| PathBuf::from(OsString::from_vec(p))),
                is_default: b.is_default,
                is_namespace: b.is_namespace,
                is_type_only: b.is_type_only,
            })
            .collect(),
        call_sites: stored
            .call_sites
            .into_iter()
            .map(|cs| gather_step_parser::resolve::CallSite {
                owner_id: cs.owner_id,
                owner_file: cs.owner_file,
                source_path: PathBuf::from(OsString::from_vec(cs.source_path)),
                callee_name: cs.callee_name,
                callee_qualified_hint: cs.callee_qualified_hint,
                span: cs.span.map(|s| gather_step_core::SourceSpan {
                    line_start: s.line_start,
                    line_len: s.line_len,
                    column_start: s.column_start,
                    column_len: s.column_len,
                }),
            })
            .collect(),
    })
}

const SQLITE_PAGE_SIZE_BYTES: i64 = 16_384;
const WAL_CACHE_SIZE_PAGES: i64 = -65_536;
const WAL_MMAP_SIZE_BYTES: i64 = 1_073_741_824;
const WAL_AUTOCHECKPOINT_PAGES: i64 = 10_000;
const READER_POOL_SIZE: usize = 4;

/// Flush the in-memory hit-count buffer to `SQLite` after this many accumulated
/// increments.  A single-row UPDATE costs ~10 µs; batching 32 increments into
/// one write transaction keeps per-hit overhead below 1 µs amortised.
const HIT_COUNT_FLUSH_THRESHOLD: i64 = 32;

/// Identifies a single cache row that tracks hit counts.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum CacheRowId {
    AnswerCache { cache_key: String },
}

/// In-memory accumulator for cache hit-count deltas.
///
/// Each cache hit increments the in-memory counter for the affected row.
/// When the total pending count across all rows reaches [`HIT_COUNT_FLUSH_THRESHOLD`]
/// the entire buffer is flushed to `SQLite` in a single transaction.
struct HitCountBuffer {
    pending: FxHashMap<CacheRowId, i64>,
    total_pending: i64,
}

impl HitCountBuffer {
    fn new() -> Self {
        Self {
            pending: FxHashMap::default(),
            total_pending: 0,
        }
    }

    /// Record one hit and return `true` when the flush threshold is reached.
    fn record(&mut self, id: CacheRowId) -> bool {
        *self.pending.entry(id).or_insert(0) += 1;
        self.total_pending += 1;
        self.total_pending >= HIT_COUNT_FLUSH_THRESHOLD
    }

    /// Drain all pending deltas, returning them as an owned map.
    fn drain(&mut self) -> FxHashMap<CacheRowId, i64> {
        self.total_pending = 0;
        std::mem::take(&mut self.pending)
    }
}

const CURRENT_SCHEMA: &str = r"
CREATE TABLE IF NOT EXISTS file_index_state (
    repo         TEXT    NOT NULL,
    file_path    BLOB    NOT NULL,
    content_hash BLOB    NOT NULL,
    size_bytes   INTEGER NOT NULL DEFAULT 0,
    mtime_ns     INTEGER NOT NULL DEFAULT 0,
    node_count   INTEGER NOT NULL DEFAULT 0,
    edge_count   INTEGER NOT NULL DEFAULT 0,
    indexed_at   INTEGER NOT NULL,
    parse_ms     INTEGER,
    PRIMARY KEY (repo, file_path)
);

CREATE TABLE IF NOT EXISTS repo_sync_state (
    repo              TEXT PRIMARY KEY,
    last_commit_sha   TEXT,
    last_pr_cursor    TEXT,
    last_issue_cursor TEXT,
    github_etag       TEXT,
    synced_at         INTEGER NOT NULL
);

-- Commits are keyed by (repo, sha) because a SHA is only unique within one repository.
-- Two distinct repos can legitimately share an identical SHA (e.g. an empty initial commit
-- created by the same `git init` template), so a workspace-global PRIMARY KEY (sha) would
-- silently collapse them into one row.
CREATE TABLE IF NOT EXISTS commits (
    repo                TEXT    NOT NULL,
    sha                 TEXT    NOT NULL,
    author_email        TEXT    NOT NULL,
    date                INTEGER NOT NULL,
    message             TEXT    NOT NULL,
    classification      TEXT,
    files_changed       INTEGER NOT NULL DEFAULT 0,
    insertions          INTEGER NOT NULL DEFAULT 0,
    deletions           INTEGER NOT NULL DEFAULT 0,
    has_decision_signal INTEGER NOT NULL DEFAULT FALSE,
    pr_number           INTEGER,
    PRIMARY KEY (repo, sha)
);
CREATE INDEX IF NOT EXISTS idx_commits_repo_date ON commits(repo, date);
CREATE INDEX IF NOT EXISTS idx_commits_author ON commits(author_email, date);

-- Per-commit file delta facts. Source of truth for hotspot, co-change, and ownership
-- analytics. Insertions/deletions are NULL for binary diffs where line counts
-- are not meaningful. `old_path` is set only when `change_kind = 'rename'` (or 'copy').
-- ON DELETE CASCADE so a per-repo history-rewrite rebuild can purge stale deltas by
-- removing the parent commit row.
CREATE TABLE IF NOT EXISTS commit_file_deltas (
    repo        TEXT NOT NULL,
    sha         TEXT NOT NULL,
    file_path   TEXT NOT NULL,
    change_kind TEXT NOT NULL,
    insertions  INTEGER,
    deletions   INTEGER,
    old_path    TEXT,
    PRIMARY KEY (repo, sha, file_path),
    FOREIGN KEY (repo, sha) REFERENCES commits(repo, sha) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_commit_file_deltas_repo_file
    ON commit_file_deltas(repo, file_path);
CREATE INDEX IF NOT EXISTS idx_commit_file_deltas_repo_sha
    ON commit_file_deltas(repo, sha);

CREATE TABLE IF NOT EXISTS pull_requests (
    id                TEXT PRIMARY KEY,
    repo              TEXT    NOT NULL,
    number            INTEGER NOT NULL,
    title             TEXT    NOT NULL,
    description       TEXT,
    author_email      TEXT    NOT NULL,
    state             TEXT    NOT NULL,
    created_at        INTEGER NOT NULL,
    merged_at         INTEGER,
    merge_commit_sha  TEXT,
    labels            TEXT,
    UNIQUE(repo, number)
);
CREATE INDEX IF NOT EXISTS idx_prs_repo_state ON pull_requests(repo, state);

CREATE TABLE IF NOT EXISTS reviews (
    id           TEXT PRIMARY KEY,
    pr_id        TEXT    NOT NULL REFERENCES pull_requests(id),
    author_email TEXT    NOT NULL,
    state        TEXT    NOT NULL,
    body         TEXT
);

CREATE TABLE IF NOT EXISTS comments (
    id           TEXT PRIMARY KEY,
    parent_type  TEXT    NOT NULL,
    parent_id    TEXT    NOT NULL,
    author_email TEXT    NOT NULL,
    body         TEXT    NOT NULL,
    file_path    TEXT,
    line_start   INTEGER,
    line_end     INTEGER,
    is_resolved  INTEGER NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_comments_parent ON comments(parent_type, parent_id);
CREATE INDEX IF NOT EXISTS idx_comments_file ON comments(file_path) WHERE file_path IS NOT NULL;

CREATE TABLE IF NOT EXISTS tickets (
    id             TEXT PRIMARY KEY,
    title          TEXT    NOT NULL,
    description    TEXT,
    status         TEXT    NOT NULL,
    type           TEXT    NOT NULL,
    assignee_email TEXT,
    priority       TEXT,
    created_at     INTEGER,
    resolved_at    INTEGER
);

CREATE TABLE IF NOT EXISTS authors (
    email         TEXT PRIMARY KEY,
    name          TEXT,
    github_handle TEXT,
    jira_handle   TEXT,
    first_seen    INTEGER NOT NULL,
    last_seen     INTEGER NOT NULL,
    total_commits INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS file_analytics (
    repo             TEXT    NOT NULL,
    file_path        TEXT    NOT NULL,
    total_commits    INTEGER NOT NULL DEFAULT 0,
    commits_90d      INTEGER NOT NULL DEFAULT 0,
    commits_180d     INTEGER NOT NULL DEFAULT 0,
    commits_365d     INTEGER NOT NULL DEFAULT 0,
    hotspot_score    REAL    NOT NULL DEFAULT 0.0,
    bus_factor       INTEGER NOT NULL DEFAULT 0,
    top_owner_email  TEXT,
    top_owner_pct    REAL    NOT NULL DEFAULT 0.0,
    complexity_trend TEXT,
    last_modified    INTEGER NOT NULL,
    computed_at      INTEGER NOT NULL,
    PRIMARY KEY (repo, file_path)
);
CREATE INDEX IF NOT EXISTS idx_analytics_hotspot ON file_analytics(hotspot_score DESC);

CREATE TABLE IF NOT EXISTS co_change_pairs (
    repo        TEXT    NOT NULL,
    file_a      TEXT    NOT NULL,
    file_b      TEXT    NOT NULL,
    strength    REAL    NOT NULL,
    occurrences INTEGER NOT NULL,
    last_seen   INTEGER NOT NULL,
    PRIMARY KEY (repo, file_a, file_b)
);

CREATE TABLE IF NOT EXISTS conventions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    repo        TEXT,
    pattern     TEXT    NOT NULL,
    description TEXT    NOT NULL,
    frequency   REAL    NOT NULL,
    confidence  REAL    NOT NULL,
    examples    TEXT    NOT NULL,
    computed_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS answer_cache (
    cache_key  TEXT PRIMARY KEY,
    response   BLOB    NOT NULL,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    hit_count  INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_cache_expiry ON answer_cache(expires_at);

CREATE TABLE IF NOT EXISTS context_packs (
    pack_key      TEXT PRIMARY KEY,
    mode          TEXT    NOT NULL,
    target        TEXT    NOT NULL,
    generation    INTEGER NOT NULL,
    response      BLOB    NOT NULL,
    created_at    INTEGER NOT NULL,
    last_read_at  INTEGER NOT NULL,
    byte_size     INTEGER NOT NULL,
    hit_count     INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_context_packs_generation
    ON context_packs(generation, mode);
CREATE INDEX IF NOT EXISTS idx_context_packs_last_read
    ON context_packs(last_read_at);

CREATE TABLE IF NOT EXISTS context_pack_files (
    pack_key   TEXT NOT NULL,
    repo       TEXT NOT NULL,
    file_path  TEXT NOT NULL,
    PRIMARY KEY (pack_key, repo, file_path),
    FOREIGN KEY (pack_key) REFERENCES context_packs(pack_key) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_context_pack_files_repo_file
    ON context_pack_files(repo, file_path);

CREATE TABLE IF NOT EXISTS file_dependencies (
    source_repo TEXT    NOT NULL,
    source_path BLOB    NOT NULL,
    target_repo TEXT    NOT NULL,
    target_path BLOB    NOT NULL,
    edge_count  INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (source_repo, source_path, target_repo, target_path)
);
CREATE INDEX IF NOT EXISTS idx_deps_target ON file_dependencies(target_repo, target_path);

CREATE TABLE IF NOT EXISTS unresolved_call_candidates (
    repo       TEXT NOT NULL,
    file_path  BLOB NOT NULL,
    payload    BLOB NOT NULL,
    PRIMARY KEY (repo, file_path)
);
CREATE INDEX IF NOT EXISTS idx_unresolved_calls_repo ON unresolved_call_candidates(repo);

CREATE TABLE IF NOT EXISTS unresolved_call_candidate_keys (
    repo          TEXT NOT NULL,
    source_path   BLOB NOT NULL,
    candidate_key TEXT NOT NULL,
    PRIMARY KEY (repo, source_path, candidate_key)
);
CREATE INDEX IF NOT EXISTS idx_unresolved_call_keys_repo_key
    ON unresolved_call_candidate_keys(repo, candidate_key);

-- Rolling log of MCP pack tool calls. Used to drive the hot-whitelist that
-- precomputes context packs at index finalize time. Primary key
-- deliberately scopes to `(target, mode)` so identical calls increment a single
-- counter instead of appending rows unbounded.
CREATE TABLE IF NOT EXISTS pack_call_log (
    target         TEXT    NOT NULL,
    mode           TEXT    NOT NULL,
    call_count     INTEGER NOT NULL DEFAULT 0,
    last_called_at INTEGER NOT NULL,
    PRIMARY KEY (target, mode)
);
CREATE INDEX IF NOT EXISTS idx_pack_call_log_top
    ON pack_call_log(call_count DESC, last_called_at DESC);

CREATE TABLE IF NOT EXISTS payload_contracts (
    payload_contract_node_id BLOB PRIMARY KEY,
    contract_target_node_id  BLOB NOT NULL,
    contract_target_kind     INTEGER NOT NULL,
    contract_target_qn       TEXT,
    repo                     TEXT NOT NULL,
    file_path                BLOB NOT NULL,
    source_symbol_node_id    BLOB NOT NULL,
    line_start               INTEGER,
    side                     TEXT NOT NULL,
    confidence               INTEGER NOT NULL,
    inference_kind           TEXT NOT NULL,
    source_type_name         TEXT,
    contract_json            BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_payload_contracts_target
    ON payload_contracts(contract_target_node_id, side, confidence DESC);
CREATE INDEX IF NOT EXISTS idx_payload_contracts_target_qn
    ON payload_contracts(contract_target_kind, contract_target_qn, side, confidence DESC);
CREATE INDEX IF NOT EXISTS idx_payload_contracts_symbol
    ON payload_contracts(source_symbol_node_id);
CREATE INDEX IF NOT EXISTS idx_payload_contracts_repo_file
    ON payload_contracts(repo, file_path);
CREATE INDEX IF NOT EXISTS idx_payload_contracts_source_type_confidence
    ON payload_contracts(source_type_name, confidence DESC, repo, file_path);
CREATE INDEX IF NOT EXISTS idx_payload_contracts_symbol_confidence
    ON payload_contracts(source_symbol_node_id, confidence DESC, repo, file_path);
";

const UPSERT_FILE_STATE_SQL: &str = r"
    INSERT INTO file_index_state (
        repo, file_path, content_hash, size_bytes, mtime_ns, node_count, edge_count, indexed_at, parse_ms
    )
    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
    ON CONFLICT(repo, file_path) DO UPDATE SET
        content_hash = excluded.content_hash,
        size_bytes = excluded.size_bytes,
        mtime_ns = excluded.mtime_ns,
        node_count = excluded.node_count,
        edge_count = excluded.edge_count,
        indexed_at = excluded.indexed_at,
        parse_ms = excluded.parse_ms
";

const STORED_CONTENT_HASH_BYTES: usize = 16;
const CONTEXT_PACK_RETENTION_SECONDS: i64 = 30 * 24 * 60 * 60;

/// Store only a 128-bit BLAKE3 prefix for per-file change detection.
///
/// Indexer-produced hashes are full 32-byte BLAKE3 digests. A 16-byte prefix
/// keeps collision risk negligible for this rebuildable cache while halving
/// the hottest `file_index_state.content_hash` BLOB. Tests sometimes use
/// deliberately tiny fixture hashes; those stay unchanged.
fn stored_content_hash(hash: &[u8]) -> &[u8] {
    if hash.len() > STORED_CONTENT_HASH_BYTES {
        &hash[..STORED_CONTENT_HASH_BYTES]
    } else {
        hash
    }
}

const UPSERT_COMMIT_SQL: &str = r"
    INSERT INTO commits (
        sha, repo, author_email, date, message, classification,
        files_changed, insertions, deletions, has_decision_signal, pr_number
    )
    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
    ON CONFLICT(repo, sha) DO UPDATE SET
        author_email = excluded.author_email,
        date = excluded.date,
        message = excluded.message,
        classification = excluded.classification,
        files_changed = excluded.files_changed,
        insertions = excluded.insertions,
        deletions = excluded.deletions,
        has_decision_signal = excluded.has_decision_signal,
        pr_number = excluded.pr_number
";

const UPSERT_COMMIT_FILE_DELTA_SQL: &str = r"
    INSERT INTO commit_file_deltas (
        repo, sha, file_path, change_kind, insertions, deletions, old_path
    )
    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
    ON CONFLICT(repo, sha, file_path) DO UPDATE SET
        change_kind = excluded.change_kind,
        insertions = excluded.insertions,
        deletions = excluded.deletions,
        old_path = excluded.old_path
";

const UPSERT_FILE_ANALYTICS_SQL: &str = r"
    INSERT INTO file_analytics (
        repo, file_path, total_commits, commits_90d, commits_180d, commits_365d,
        hotspot_score, bus_factor, top_owner_email, top_owner_pct, complexity_trend,
        last_modified, computed_at
    )
    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
    ON CONFLICT(repo, file_path) DO UPDATE SET
        total_commits = excluded.total_commits,
        commits_90d = excluded.commits_90d,
        commits_180d = excluded.commits_180d,
        commits_365d = excluded.commits_365d,
        hotspot_score = excluded.hotspot_score,
        bus_factor = excluded.bus_factor,
        top_owner_email = excluded.top_owner_email,
        top_owner_pct = excluded.top_owner_pct,
        complexity_trend = excluded.complexity_trend,
        last_modified = excluded.last_modified,
        computed_at = excluded.computed_at
";

const UPSERT_CO_CHANGE_PAIR_SQL: &str = r"
    INSERT INTO co_change_pairs (
        repo, file_a, file_b, strength, occurrences, last_seen
    )
    VALUES (?1, ?2, ?3, ?4, ?5, ?6)
    ON CONFLICT(repo, file_a, file_b) DO UPDATE SET
        strength = excluded.strength,
        occurrences = excluded.occurrences,
        last_seen = excluded.last_seen
";

pub trait MetadataStore {
    fn upsert_file_state(&self, state: &FileIndexState) -> Result<(), MetadataStoreError>;
    fn upsert_file_states(&self, states: &[FileIndexState]) -> Result<(), MetadataStoreError>;
    fn should_reindex(
        &self,
        repo: &str,
        file_path: &str,
        current_hash: &[u8],
    ) -> Result<bool, MetadataStoreError>;
    /// Batch variant of [`should_reindex`](Self::should_reindex).
    ///
    /// For each `(file_path, current_hash)` pair in `files`, returns the set
    /// of `file_path` values that require re-indexing: either the path is
    /// absent from the `file_index_state` table for `repo`, or its stored
    /// `content_hash` differs from `current_hash`.
    ///
    /// Issues one query per chunk of 400 files rather than one query per file,
    /// making it suitable for large batches (e.g. 500-file repo batches).
    fn should_reindex_batch(
        &self,
        repo: &str,
        files: &[(&str, &[u8])],
    ) -> Result<rustc_hash::FxHashSet<String>, MetadataStoreError>;
    fn should_reindex_batch_by_path_bytes(
        &self,
        repo: &str,
        files: &[(&[u8], &[u8])],
    ) -> Result<rustc_hash::FxHashSet<Vec<u8>>, MetadataStoreError>;
    fn insert_commit(&self, commit: &CommitRecord) -> Result<(), MetadataStoreError>;
    fn insert_commits(&self, commits: &[CommitRecord]) -> Result<(), MetadataStoreError> {
        for commit in commits {
            self.insert_commit(commit)?;
        }
        Ok(())
    }
    fn get_commits_by_repo(
        &self,
        repo: &str,
        start_date: i64,
        end_date: i64,
    ) -> Result<Vec<CommitRecord>, MetadataStoreError>;
    fn upsert_commit_file_delta(
        &self,
        delta: &CommitFileDeltaRecord,
    ) -> Result<(), MetadataStoreError>;
    fn upsert_commit_file_deltas(
        &self,
        deltas: &[CommitFileDeltaRecord],
    ) -> Result<(), MetadataStoreError> {
        for delta in deltas {
            self.upsert_commit_file_delta(delta)?;
        }
        Ok(())
    }
    fn get_commit_file_deltas(
        &self,
        repo: &str,
        sha: &str,
    ) -> Result<Vec<CommitFileDeltaRecord>, MetadataStoreError>;
    fn get_commit_file_deltas_for_repo(
        &self,
        repo: &str,
    ) -> Result<Vec<CommitFileDeltaRecord>, MetadataStoreError>;
    /// Returns commits and per-file deltas relevant to a single file's
    /// ownership history, walking back through any rename chain. Used by
    /// per-file query paths (e.g. the MCP `who_owns` tool) that would
    /// otherwise scan the entire repo just to answer a single-file
    /// question.
    ///
    /// Resolution semantics:
    ///   1. Start from `file_path` and walk backwards through `Renamed`
    ///      deltas (`change_kind = 'renamed'`) where `file_path` matches a
    ///      known successor; each step adds the corresponding `old_path`
    ///      to the historical-paths set.
    ///   2. Iterate until the set stops growing, capped at 64 hops to
    ///      bound pathological rename loops.
    ///   3. Return all commits and deltas whose `file_path` (or, for
    ///      deltas, `old_path`) appears in the resolved set.
    ///
    /// Returns `(commits, deltas)` where commits are sorted by `date ASC`
    /// and deltas are sorted by `(sha, file_path)` for deterministic
    /// downstream ordering. Both vectors may be empty when the file has
    /// no history (e.g. a path that has never been committed).
    fn get_history_for_file_with_renames(
        &self,
        repo: &str,
        file_path: &str,
    ) -> Result<(Vec<CommitRecord>, Vec<CommitFileDeltaRecord>), MetadataStoreError>;
    /// Removes all commits and their cascaded `commit_file_deltas` for a single repo.
    /// Used by the history-rewrite fallback path: when an old `last_commit_sha`
    /// becomes unreachable from the new HEAD (force-push, rebase), the only safe
    /// recovery is to drop the repo's history and re-walk from scratch.
    fn delete_commits_for_repo(&self, repo: &str) -> Result<(), MetadataStoreError>;
    /// Atomically replaces the entire git-history slice for a repo: clears commits
    /// (cascading their deltas), clears derived `file_analytics` and `co_change_pairs`,
    /// inserts the new commits and deltas, and advances `last_commit_sha`. The whole
    /// sequence runs inside a single `SQLite` transaction so a partial failure never
    /// leaves the repo with an empty history but a stale anchor.
    ///
    /// Used by the history-rewrite fallback path. Callers should rebuild
    /// derived analytics (hotspots, ownership, co-change) in a subsequent step.
    fn replace_repo_history(
        &self,
        repo: &str,
        commits: &[CommitRecord],
        deltas: &[CommitFileDeltaRecord],
        last_commit_sha: &str,
        synced_at: i64,
    ) -> Result<(), MetadataStoreError>;
    /// Returns the recorded `last_commit_sha` for a repo, or `None` when no
    /// sync has run yet. Used by [`GitHistoryIndexer`](super) to decide
    /// between an incremental walk and a full rebuild.
    ///
    /// (`MetadataStore::get_last_commit_sha` is intentionally narrower than a
    /// full `RepoSyncState` getter — this path only cares about the git anchor;
    /// the GitHub-side cursor columns on the same row are reserved for a
    /// future PR/issue ingestion phase and are not touched here.)
    fn get_last_commit_sha(&self, repo: &str) -> Result<Option<String>, MetadataStoreError>;
    /// Records the latest synced commit SHA for a repo. Other `repo_sync_state`
    /// columns (`PR`/issue cursors, `GitHub` `ETag`) are preserved when the row
    /// already exists; the row is created on first call.
    fn set_last_commit_sha(
        &self,
        repo: &str,
        last_commit_sha: &str,
        synced_at: i64,
    ) -> Result<(), MetadataStoreError>;
    fn get_file_analytics(
        &self,
        repo: &str,
        file_path: &str,
    ) -> Result<Option<FileAnalytics>, MetadataStoreError>;
    fn list_file_analytics_for_repo(
        &self,
        repo: &str,
    ) -> Result<Vec<FileAnalytics>, MetadataStoreError>;
    fn replace_file_analytics_for_repo(
        &self,
        repo: &str,
        analytics: &[FileAnalytics],
    ) -> Result<(), MetadataStoreError>;
    fn get_co_change_pairs_for_repo(
        &self,
        repo: &str,
    ) -> Result<Vec<CoChangePairRecord>, MetadataStoreError>;
    fn replace_co_change_pairs_for_repo(
        &self,
        repo: &str,
        pairs: &[CoChangePairRecord],
    ) -> Result<(), MetadataStoreError>;
    fn replace_payload_contracts_for_files(
        &self,
        repo: &str,
        file_paths: &[String],
        records: &[PayloadContractStoreRecord],
    ) -> Result<(), MetadataStoreError>;
    fn payload_contracts_for_query(
        &self,
        query: PayloadContractQuery,
    ) -> Result<Vec<PayloadContractStoreRecord>, MetadataStoreError>;
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct FileIndexState {
    pub repo: String,
    pub file_path: String,
    /// Raw `OsStr` bytes used as the BLOB identity key in `file_index_state`.
    ///
    /// When non-empty, `upsert_file_state` binds these bytes to the
    /// `file_path` BLOB column instead of `file_path.as_bytes()`.  This
    /// preserves non-UTF-8 path bytes that the lossy `String` conversion
    /// would otherwise replace with the U+FFFD replacement character.
    ///
    /// Populated from `PathId::from_path(&source_file.path).as_bytes()` in
    /// the indexing pipeline.  Callers that construct `FileIndexState`
    /// directly for test fixtures may leave this empty; the ASCII paths used
    /// in tests are identical under both representations.
    pub path_id_bytes: Vec<u8>,
    pub content_hash: Vec<u8>,
    pub size_bytes: i64,
    pub mtime_ns: i64,
    pub node_count: i64,
    pub edge_count: i64,
    pub indexed_at: i64,
    pub parse_ms: Option<i64>,
}

impl FileIndexState {
    /// Returns the bytes to bind as the `file_path` BLOB column in `SQLite`.
    ///
    /// Uses `path_id_bytes` when non-empty (lossless `OsStr` bytes), falling
    /// back to `file_path.as_bytes()` for states constructed without the raw
    /// byte field (e.g., test fixtures with ASCII paths).
    pub(crate) fn effective_path_bytes(&self) -> &[u8] {
        if self.path_id_bytes.is_empty() {
            self.file_path.as_bytes()
        } else {
            &self.path_id_bytes
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitRecord {
    pub sha: String,
    pub repo: String,
    pub author_email: String,
    pub date: i64,
    pub message: String,
    pub classification: Option<String>,
    pub files_changed: i64,
    pub insertions: i64,
    pub deletions: i64,
    pub has_decision_signal: bool,
    pub pr_number: Option<i64>,
}

/// Per-file change facts attached to a single commit. One row per (repo, sha,
/// `file_path`). `insertions` / `deletions` are `None` for binary diffs where the
/// upstream Git diff machinery does not produce a line count. `old_path` is set
/// only for renames or copies; for plain modifications it stays `None`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitFileDeltaRecord {
    pub repo: String,
    pub sha: String,
    pub file_path: String,
    pub change_kind: CommitFileChangeKind,
    pub insertions: Option<i64>,
    pub deletions: Option<i64>,
    pub old_path: Option<String>,
}

/// Classification of how a file appears in a commit. Stored as a lower-case
/// string in `SQLite` so future kinds can be added without a schema migration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CommitFileChangeKind {
    Added,
    Modified,
    Deleted,
    Renamed,
    Copied,
    /// Type changed (file <-> symlink, regular <-> executable, etc.). Surfaced as
    /// its own variant because rename/copy detectors should not coalesce it with a
    /// content modification.
    TypeChanged,
}

impl CommitFileChangeKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Added => "added",
            Self::Modified => "modified",
            Self::Deleted => "deleted",
            Self::Renamed => "renamed",
            Self::Copied => "copied",
            Self::TypeChanged => "type_changed",
        }
    }

    pub fn from_sql_str(value: &str) -> Option<Self> {
        match value {
            "added" => Some(Self::Added),
            "modified" => Some(Self::Modified),
            "deleted" => Some(Self::Deleted),
            "renamed" => Some(Self::Renamed),
            "copied" => Some(Self::Copied),
            "type_changed" => Some(Self::TypeChanged),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FileAnalytics {
    pub repo: String,
    pub file_path: String,
    pub total_commits: i64,
    pub commits_90d: i64,
    pub commits_180d: i64,
    pub commits_365d: i64,
    pub hotspot_score: f64,
    pub bus_factor: i64,
    pub top_owner_email: Option<String>,
    pub top_owner_pct: f64,
    pub complexity_trend: Option<String>,
    pub last_modified: i64,
    pub computed_at: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CoChangePairRecord {
    pub repo: String,
    pub file_a: String,
    pub file_b: String,
    pub strength: f64,
    pub occurrences: i64,
    pub last_seen: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PayloadContractStoreRecord {
    pub record: PayloadContractRecord,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PayloadContractQuery {
    pub contract_target_node_id: Option<NodeId>,
    pub contract_target_kind: Option<NodeKind>,
    pub contract_target_qualified_name: Option<String>,
    pub min_confidence: Option<u16>,
    pub repo: Option<String>,
    pub side: Option<PayloadSide>,
    pub source_symbol_node_id: Option<NodeId>,
    pub source_type_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextPackRecord {
    pub pack_key: String,
    pub mode: String,
    pub target: String,
    pub generation: i64,
    pub response: Vec<u8>,
    pub created_at: i64,
    pub last_read_at: i64,
    pub byte_size: i64,
    pub hit_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextPackStats {
    pub total_packs: usize,
    pub total_bytes: i64,
    pub total_hits: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackCallLogEntry {
    pub target: String,
    pub mode: String,
    pub call_count: i64,
    pub last_called_at: i64,
}

pub struct MetadataStoreDb {
    writer: Mutex<Connection>,
    readers: ConnectionPool,
    path: PathBuf,
    hit_count_buf: Mutex<HitCountBuffer>,
}

struct ConnectionPool {
    connections: Mutex<Vec<Connection>>,
    available: Condvar,
}

struct PooledConnection<'a> {
    connection: Option<Connection>,
    pool: &'a ConnectionPool,
}

#[derive(Debug, Error)]
pub enum MetadataStoreError {
    #[error("failed to access metadata database file: {0}")]
    Io(#[from] std::io::Error),
    #[error("sqlite metadata store error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("metadata store connection pool mutex was poisoned")]
    Poisoned,
}

/// Current metadata schema version.
///
/// v3.1 is a fresh generated-state release. SQLite metadata starts at user
/// version zero and does not carry migration or upgrade branches.
pub const METADATA_SCHEMA_VERSION: i64 = 0;

impl MetadataStoreDb {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MetadataStoreError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let is_new = !path.exists();
        let mut connection = Connection::open(&path)?;
        // Caller must have validated path via cli::path_safety before opening.
        if is_new {
            crate::fs_mode::apply_private_file(&path)?;
        }
        connection.busy_timeout(Duration::from_secs(5))?;
        Self::configure_new_database(&connection)?;
        Self::configure_connection(&connection)?;
        Self::bootstrap_schema(&mut connection)?;

        let readers = (0..READER_POOL_SIZE)
            .map(|_| Self::open_connection(&path))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            writer: Mutex::new(connection),
            readers: ConnectionPool::new(readers),
            path,
            hit_count_buf: Mutex::new(HitCountBuffer::new()),
        })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn configure_connection(connection: &Connection) -> Result<(), MetadataStoreError> {
        // This SQLite database holds cacheable sync/file-index state. WAL + NORMAL keeps writes
        // atomic while avoiding the fsync cost of FULL; if the latest row is lost, missing
        // `file_index_state` entries trigger a reindex on the next run and sync cursors can be
        // refreshed from upstream systems.
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "NORMAL")?;
        connection.pragma_update(None, "cache_size", WAL_CACHE_SIZE_PAGES)?;
        connection.pragma_update(None, "mmap_size", WAL_MMAP_SIZE_BYTES)?;
        connection.pragma_update(None, "wal_autocheckpoint", WAL_AUTOCHECKPOINT_PAGES)?;
        connection.pragma_update(None, "temp_store", "MEMORY")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        Ok(())
    }

    fn configure_new_database(connection: &Connection) -> Result<(), MetadataStoreError> {
        let object_count = connection.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'",
            [],
            |row| row.get::<_, i64>(0),
        )?;

        if object_count == 0 {
            connection.pragma_update(None, "page_size", SQLITE_PAGE_SIZE_BYTES)?;
        }

        Ok(())
    }

    fn open_connection(path: &Path) -> Result<Connection, MetadataStoreError> {
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        Self::configure_connection(&connection)?;
        Ok(connection)
    }

    fn bootstrap_schema(connection: &mut Connection) -> Result<(), MetadataStoreError> {
        connection.execute_batch(CURRENT_SCHEMA)?;
        connection.pragma_update(None, "user_version", METADATA_SCHEMA_VERSION)?;
        Ok(())
    }

    fn lock_writer(&self) -> Result<MutexGuard<'_, Connection>, MetadataStoreError> {
        self.writer.lock().map_err(|_| MetadataStoreError::Poisoned)
    }

    /// Runs `action` inside a transaction on the configured writer connection.
    ///
    /// Guarantees:
    ///   - The connection honours the same pragmas as the rest of the metadata
    ///     store (WAL journal, `busy_timeout = 5s`, tuned cache/mmap), because
    ///     it is the one opened in `MetadataStoreDb::open`.
    ///   - Callers serialise on the internal writer mutex, so reconciliation
    ///     running in one thread cannot race against other metadata writes on
    ///     the `SQLite` file lock.
    ///
    /// If `action` returns `Err`, the transaction is rolled back. On `Ok`, it
    /// is committed before the function returns.
    ///
    /// # Non-reentrant
    ///
    /// The closure **must not** call any `MetadataStore` method on the same
    /// instance. The internal writer mutex is non-reentrant (`std::sync::Mutex`),
    /// so any path from `action` back into `self.lock_writer()` will deadlock
    /// on the current thread.
    pub fn with_write_txn<F, T>(&self, action: F) -> Result<T, MetadataStoreError>
    where
        F: FnOnce(&rusqlite::Transaction<'_>) -> Result<T, rusqlite::Error>,
    {
        let mut connection = self.lock_writer()?;
        let tx = connection.transaction()?;
        let value = action(&tx)?;
        tx.commit()?;
        Ok(value)
    }

    fn prune_stale_context_packs_in_tx(
        tx: &rusqlite::Transaction<'_>,
        now_unix: i64,
    ) -> Result<usize, rusqlite::Error> {
        let cutoff = now_unix.saturating_sub(CONTEXT_PACK_RETENTION_SECONDS);
        tx.execute(
            "DELETE FROM context_packs WHERE last_read_at < ?1",
            params![cutoff],
        )
    }

    fn read_connection(&self) -> Result<PooledConnection<'_>, MetadataStoreError> {
        self.readers.acquire()
    }

    fn commit_from_row(row: &rusqlite::Row<'_>) -> Result<CommitRecord, rusqlite::Error> {
        Ok(CommitRecord {
            sha: row.get(0)?,
            repo: row.get(1)?,
            author_email: row.get(2)?,
            date: row.get(3)?,
            message: row.get(4)?,
            classification: row.get(5)?,
            files_changed: row.get(6)?,
            insertions: row.get(7)?,
            deletions: row.get(8)?,
            has_decision_signal: row.get(9)?,
            pr_number: row.get(10)?,
        })
    }

    fn commit_file_delta_from_row(
        row: &rusqlite::Row<'_>,
    ) -> Result<CommitFileDeltaRecord, rusqlite::Error> {
        let kind_text: String = row.get(3)?;
        let change_kind = CommitFileChangeKind::from_sql_str(&kind_text).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                kind_text.len(),
                rusqlite::types::Type::Text,
                format!("unknown commit_file_deltas.change_kind value: {kind_text}").into(),
            )
        })?;
        Ok(CommitFileDeltaRecord {
            repo: row.get(0)?,
            sha: row.get(1)?,
            file_path: row.get(2)?,
            change_kind,
            insertions: row.get(4)?,
            deletions: row.get(5)?,
            old_path: row.get(6)?,
        })
    }

    fn file_analytics_from_row(row: &rusqlite::Row<'_>) -> Result<FileAnalytics, rusqlite::Error> {
        Ok(FileAnalytics {
            repo: row.get(0)?,
            file_path: row.get(1)?,
            total_commits: row.get(2)?,
            commits_90d: row.get(3)?,
            commits_180d: row.get(4)?,
            commits_365d: row.get(5)?,
            hotspot_score: row.get(6)?,
            bus_factor: row.get(7)?,
            top_owner_email: row.get(8)?,
            top_owner_pct: row.get(9)?,
            complexity_trend: row.get(10)?,
            last_modified: row.get(11)?,
            computed_at: row.get(12)?,
        })
    }

    fn co_change_pair_from_row(
        row: &rusqlite::Row<'_>,
    ) -> Result<CoChangePairRecord, rusqlite::Error> {
        Ok(CoChangePairRecord {
            repo: row.get(0)?,
            file_a: row.get(1)?,
            file_b: row.get(2)?,
            strength: row.get(3)?,
            occurrences: row.get(4)?,
            last_seen: row.get(5)?,
        })
    }

    fn payload_record_from_row(
        row: &rusqlite::Row<'_>,
    ) -> Result<PayloadContractStoreRecord, rusqlite::Error> {
        let payload_contract_node_id = decode_node_id_blob(&row.get::<_, Vec<u8>>(0)?)?;
        let contract_target_node_id = decode_node_id_blob(&row.get::<_, Vec<u8>>(1)?)?;
        let contract_target_kind = decode_node_kind(row.get::<_, i64>(2)?)?;
        let source_symbol_node_id = decode_node_id_blob(&row.get::<_, Vec<u8>>(6)?)?;
        let contract_json = row.get::<_, Vec<u8>>(12)?;
        let contract = serde_json::from_slice(&contract_json).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                contract_json.len(),
                rusqlite::types::Type::Blob,
                Box::new(error),
            )
        })?;
        let side = match row.get::<_, String>(8)?.as_str() {
            "producer" => PayloadSide::Producer,
            "consumer" => PayloadSide::Consumer,
            other => {
                return Err(rusqlite::Error::FromSqlConversionFailure(
                    other.len(),
                    rusqlite::types::Type::Text,
                    "invalid payload side".into(),
                ));
            }
        };

        let file_path_bytes: Vec<u8> = row.get(5)?;
        Ok(PayloadContractStoreRecord {
            record: PayloadContractRecord {
                payload_contract_node_id,
                contract_target_node_id,
                contract_target_kind,
                contract_target_qualified_name: row.get(3)?,
                repo: row.get(4)?,
                file_path: std::str::from_utf8(&file_path_bytes).map_or_else(
                    |_| String::from_utf8_lossy(&file_path_bytes).into_owned(),
                    String::from,
                ),
                source_symbol_node_id,
                line_start: row.get(7)?,
                side,
                confidence: row.get(9)?,
                inference_kind: {
                    let raw: String = row.get(10)?;
                    PayloadInferenceKind::from_sql_str(&raw).ok_or_else(|| {
                        rusqlite::Error::FromSqlConversionFailure(
                            raw.len(),
                            rusqlite::types::Type::Text,
                            format!("unknown inference kind: {raw}").into(),
                        )
                    })?
                },
                source_type_name: row.get(11)?,
                contract,
            },
        })
    }

    fn optimize_connection(connection: &Connection) {
        let _ = connection.execute_batch("PRAGMA optimize;");
    }

    /// Flush all pending hit-count deltas to `SQLite` in a single transaction.
    ///
    /// Silently ignores mutex-poison errors and `SQLite` failures — a lost hit
    /// count is acceptable; the database must stay consistent.
    ///
    /// Called automatically when the buffer threshold is crossed and on drop.
    /// Callers that need hit counts to be visible immediately — such as tests
    /// that query the database directly — can call this after a cache hit.
    pub fn flush_hit_counts(&self) {
        let deltas = match self.hit_count_buf.lock() {
            Ok(mut guard) => guard.drain(),
            Err(_) => return,
        };
        if deltas.is_empty() {
            return;
        }
        let _ = self.with_write_txn(|tx| {
            for (id, delta) in &deltas {
                let CacheRowId::AnswerCache { cache_key } = id;
                tx.execute(
                    "UPDATE answer_cache SET hit_count = hit_count + ?2 WHERE cache_key = ?1",
                    params![cache_key, delta],
                )?;
            }
            Ok(())
        });
    }

    /// Record a hit and flush the buffer to `SQLite` if the threshold is reached.
    fn record_hit(&self, id: CacheRowId) {
        let should_flush = match self.hit_count_buf.lock() {
            Ok(mut guard) => guard.record(id),
            Err(_) => false,
        };
        if should_flush {
            self.flush_hit_counts();
        }
    }

    /// Flush and compact the database after a bulk-indexing run.
    ///
    /// Issues `PRAGMA wal_checkpoint(TRUNCATE)` to merge the write-ahead log
    /// back into the main database file and reset its size to zero, then
    /// `VACUUM` to compact freed pages.  Both operations are best-effort:
    /// failure is logged but does not propagate — a partially checkpointed or
    /// un-vacuumed database is still fully consistent.
    ///
    /// Call this once at the end of a full workspace index run, not during
    /// incremental per-repo updates.
    pub fn try_finalize(&self) -> Result<(), MetadataStoreError> {
        let connection = self.lock_writer()?;
        connection.pragma_update(None, "wal_checkpoint", "TRUNCATE")?;
        connection.execute_batch("VACUUM;")?;
        Ok(())
    }

    pub fn finalize(&self) {
        if let Err(e) = self.try_finalize() {
            tracing::warn!(error = %e, "metadata finalize failed");
        }
    }

    pub fn file_index_states_by_repo(
        &self,
        repo: &str,
    ) -> Result<Vec<FileIndexState>, MetadataStoreError> {
        let connection = self.read_connection()?;
        let mut statement = connection.prepare_cached(
            r"
            SELECT
                repo,
                file_path,
                content_hash,
                size_bytes,
                mtime_ns,
                node_count,
                edge_count,
                indexed_at,
                parse_ms
            FROM file_index_state
            WHERE repo = ?1
            ORDER BY file_path ASC
            ",
        )?;
        let rows = statement.query_map(params![repo], |row| {
            let file_path_bytes: Vec<u8> = row.get(1)?;
            let file_path = std::str::from_utf8(&file_path_bytes).map_or_else(
                |_| String::from_utf8_lossy(&file_path_bytes).into_owned(),
                String::from,
            );
            Ok(FileIndexState {
                repo: row.get(0)?,
                // Preserve the raw BLOB bytes so any downstream re-write
                // binds the original bytes, not the lossy display string.
                path_id_bytes: file_path_bytes,
                file_path,
                content_hash: row.get(2)?,
                size_bytes: row.get(3)?,
                mtime_ns: row.get(4)?,
                node_count: row.get(5)?,
                edge_count: row.get(6)?,
                indexed_at: row.get(7)?,
                parse_ms: row.get(8)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Path-accepting variant of [`MetadataStore::should_reindex`] that
    /// uses [`PathId::from_path`] to produce the BLOB key, enabling
    /// lossless lookup of non-UTF-8 file paths.
    ///
    /// Prefer this method over `should_reindex` when you have a
    /// [`std::path::Path`]; use `should_reindex` when you only have a
    /// display string (e.g. from a serialised payload).
    pub fn should_reindex_path(
        &self,
        repo: &str,
        file_path: &std::path::Path,
        current_hash: &[u8],
    ) -> Result<bool, MetadataStoreError> {
        let id = PathId::from_path(file_path);
        let connection = self.read_connection()?;
        let stored_hash = connection
            .query_row(
                "SELECT content_hash FROM file_index_state WHERE repo = ?1 AND file_path = ?2",
                params![repo, id.as_bytes()],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?;
        Ok(match stored_hash {
            Some(hash) => hash != stored_content_hash(current_hash),
            None => true,
        })
    }

    pub fn reverse_dependents(
        &self,
        repo: &str,
        file_path: &str,
    ) -> Result<Vec<String>, MetadataStoreError> {
        let connection = self.read_connection()?;
        let mut statement = connection.prepare_cached(
            r"
            SELECT DISTINCT source_path
            FROM file_dependencies
            WHERE source_repo = ?1 AND target_repo = ?1 AND target_path = ?2
            ORDER BY source_path ASC
            ",
        )?;
        let rows = statement.query_map(params![repo, file_path.as_bytes()], |row| {
            let bytes: Vec<u8> = row.get(0)?;
            Ok(match String::from_utf8(bytes) {
                Ok(s) => s,
                Err(err) => String::from_utf8_lossy(err.as_bytes()).into_owned(),
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn reverse_dependents_by_path_id(
        &self,
        repo: &str,
        file_path_id: &[u8],
    ) -> Result<Vec<TrackedPath>, MetadataStoreError> {
        let connection = self.read_connection()?;
        let mut statement = connection.prepare_cached(
            r"
            SELECT DISTINCT source_path
            FROM file_dependencies
            WHERE source_repo = ?1 AND target_repo = ?1 AND target_path = ?2
            ORDER BY source_path ASC
            ",
        )?;
        let rows = statement.query_map(params![repo, file_path_id], |row| {
            let bytes: Vec<u8> = row.get(0)?;
            Ok(TrackedPath {
                path: std::str::from_utf8(&bytes).map_or_else(
                    |_| String::from_utf8_lossy(&bytes).into_owned(),
                    String::from,
                ),
                path_id_bytes: bytes,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn delete_file_state(&self, repo: &str, file_path: &str) -> Result<(), MetadataStoreError> {
        self.with_write_txn(|tx| {
            tx.execute(
                "DELETE FROM file_index_state WHERE repo = ?1 AND file_path = ?2",
                params![repo, file_path.as_bytes()],
            )?;
            Ok(())
        })
    }

    pub fn delete_file_dependencies(
        &self,
        repo: &str,
        file_path: &str,
    ) -> Result<(), MetadataStoreError> {
        self.with_write_txn(|tx| {
            tx.execute(
                "DELETE FROM file_dependencies WHERE source_repo = ?1 AND source_path = ?2",
                params![repo, file_path.as_bytes()],
            )?;
            tx.execute(
                "DELETE FROM file_dependencies WHERE target_repo = ?1 AND target_path = ?2",
                params![repo, file_path.as_bytes()],
            )?;
            Ok(())
        })
    }

    pub fn delete_file_state_and_dependencies(
        &self,
        repo: &str,
        file_path: &str,
    ) -> Result<(), MetadataStoreError> {
        self.with_write_txn(|tx| {
            tx.execute(
                "DELETE FROM file_index_state WHERE repo = ?1 AND file_path = ?2",
                params![repo, file_path.as_bytes()],
            )?;
            tx.execute(
                "DELETE FROM file_dependencies WHERE source_repo = ?1 AND source_path = ?2",
                params![repo, file_path.as_bytes()],
            )?;
            tx.execute(
                "DELETE FROM file_dependencies WHERE target_repo = ?1 AND target_path = ?2",
                params![repo, file_path.as_bytes()],
            )?;
            Ok(())
        })
    }

    pub fn clear_index_metadata_for_files(
        &self,
        repo: &str,
        file_path_ids: &[Vec<u8>],
    ) -> Result<(), MetadataStoreError> {
        self.with_write_txn(|tx| {
            let mut del_unresolved = tx.prepare_cached(
                "DELETE FROM unresolved_call_candidates WHERE repo = ?1 AND file_path = ?2",
            )?;
            let mut del_unresolved_keys = tx.prepare_cached(
                "DELETE FROM unresolved_call_candidate_keys WHERE repo = ?1 AND source_path = ?2",
            )?;
            let mut del_payload = tx.prepare_cached(
                "DELETE FROM payload_contracts WHERE repo = ?1 AND file_path = ?2",
            )?;
            let mut del_state = tx.prepare_cached(
                "DELETE FROM file_index_state WHERE repo = ?1 AND file_path = ?2",
            )?;
            let mut del_deps_src = tx.prepare_cached(
                "DELETE FROM file_dependencies WHERE source_repo = ?1 AND source_path = ?2",
            )?;
            let mut del_deps_tgt = tx.prepare_cached(
                "DELETE FROM file_dependencies WHERE target_repo = ?1 AND target_path = ?2",
            )?;
            for path_id in file_path_ids {
                del_unresolved.execute(params![repo, path_id])?;
                del_unresolved_keys.execute(params![repo, path_id])?;
                del_payload.execute(params![repo, path_id])?;
                del_state.execute(params![repo, path_id])?;
                del_deps_src.execute(params![repo, path_id])?;
                del_deps_tgt.execute(params![repo, path_id])?;
            }
            Ok(())
        })
    }

    pub fn clear_semantic_metadata_for_files(
        &self,
        repo: &str,
        file_path_ids: &[Vec<u8>],
    ) -> Result<(), MetadataStoreError> {
        self.with_write_txn(|tx| {
            let mut del_unresolved = tx.prepare_cached(
                "DELETE FROM unresolved_call_candidates WHERE repo = ?1 AND file_path = ?2",
            )?;
            let mut del_unresolved_keys = tx.prepare_cached(
                "DELETE FROM unresolved_call_candidate_keys WHERE repo = ?1 AND source_path = ?2",
            )?;
            let mut del_payload = tx.prepare_cached(
                "DELETE FROM payload_contracts WHERE repo = ?1 AND file_path = ?2",
            )?;
            for path_id in file_path_ids {
                del_unresolved.execute(params![repo, path_id])?;
                del_unresolved_keys.execute(params![repo, path_id])?;
                del_payload.execute(params![repo, path_id])?;
            }
            Ok(())
        })
    }

    pub fn replace_index_metadata_for_files(
        &self,
        repo: &str,
        file_path_ids: &[Vec<u8>],
        inputs: &[ResolutionInput],
        records: &[PayloadContractStoreRecord],
        states: &[FileIndexState],
    ) -> Result<(), MetadataStoreError> {
        self.with_write_txn(|tx| {
            {
                let mut del_state = tx.prepare_cached(
                    "DELETE FROM file_index_state WHERE repo = ?1 AND file_path = ?2",
                )?;
                for path_id in file_path_ids {
                    del_state.execute(params![repo, path_id])?;
                }
            }

            {
                let mut ins_unresolved = tx.prepare_cached(
                    "INSERT INTO unresolved_call_candidates(repo, file_path, payload) VALUES (?1, ?2, ?3)",
                )?;
                let mut ins_key = tx.prepare_cached(
                    "INSERT INTO unresolved_call_candidate_keys(repo, source_path, candidate_key) VALUES (?1, ?2, ?3)",
                )?;
                for input in inputs {
                    let file_path_bytes = PathId::from_path(&input.file_path)
                        .as_bytes()
                        .to_vec();
                    let payload = encode_resolution_input(input);
                    ins_unresolved.execute(params![repo, &file_path_bytes, payload])?;
                    for candidate_key in Self::unresolved_candidate_keys(input) {
                        ins_key.execute(params![repo, &file_path_bytes, candidate_key])?;
                    }
                }
            }

            for record in records {
                let contract_json = serde_json::to_vec(&record.record.contract)
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
                let inference_kind = record.record.inference_kind.as_sql_str();
                let side = match record.record.side {
                    PayloadSide::Producer => "producer",
                    PayloadSide::Consumer => "consumer",
                };
                tx.execute(
                    r"
                    INSERT INTO payload_contracts(
                        payload_contract_node_id,
                        contract_target_node_id,
                        contract_target_kind,
                        contract_target_qn,
                        repo,
                        file_path,
                        source_symbol_node_id,
                        line_start,
                        side,
                        confidence,
                        inference_kind,
                        source_type_name,
                        contract_json
                    )
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                    ",
                    params![
                        record.record.payload_contract_node_id.as_bytes().to_vec(),
                        record.record.contract_target_node_id.as_bytes().to_vec(),
                        record.record.contract_target_kind as i64,
                        record.record.contract_target_qualified_name,
                        record.record.repo,
                        record.record.file_path.as_bytes(),
                        record.record.source_symbol_node_id.as_bytes().to_vec(),
                        record.record.line_start,
                        side,
                        record.record.confidence,
                        inference_kind,
                        record.record.source_type_name,
                        contract_json,
                    ],
                )?;
            }

            let mut statement = tx.prepare_cached(UPSERT_FILE_STATE_SQL)?;
            for state in states {
                let path_bytes = state.effective_path_bytes();
                statement.execute(params![
                    &state.repo,
                    path_bytes,
                    stored_content_hash(&state.content_hash),
                    state.size_bytes,
                    state.mtime_ns,
                    state.node_count,
                    state.edge_count,
                    state.indexed_at,
                    state.parse_ms
                ])?;
            }
            Ok(())
        })
    }

    fn unresolved_candidate_keys(input: &ResolutionInput) -> Vec<String> {
        let mut keys = std::collections::BTreeSet::new();
        for call_site in &input.call_sites {
            if !call_site.callee_name.is_empty() {
                keys.insert(call_site.callee_name.clone());
            }
        }
        keys.into_iter().collect()
    }

    pub fn replace_unresolved_resolution_inputs_for_files(
        &self,
        repo: &str,
        file_path_ids: &[Vec<u8>],
        inputs: &[ResolutionInput],
    ) -> Result<(), MetadataStoreError> {
        self.with_write_txn(|tx| {
            {
                let mut del_candidates = tx.prepare_cached(
                    "DELETE FROM unresolved_call_candidates WHERE repo = ?1 AND file_path = ?2",
                )?;
                let mut del_keys = tx.prepare_cached(
                    "DELETE FROM unresolved_call_candidate_keys WHERE repo = ?1 AND source_path = ?2",
                )?;
                for path_id in file_path_ids {
                    del_candidates.execute(params![repo, path_id])?;
                    del_keys.execute(params![repo, path_id])?;
                }
            }

            {
                let mut ins_unresolved = tx.prepare_cached(
                    "INSERT INTO unresolved_call_candidates(repo, file_path, payload) VALUES (?1, ?2, ?3)",
                )?;
                let mut ins_key = tx.prepare_cached(
                    "INSERT INTO unresolved_call_candidate_keys(repo, source_path, candidate_key) VALUES (?1, ?2, ?3)",
                )?;
                for input in inputs {
                    let file_path_bytes = PathId::from_path(&input.file_path)
                        .as_bytes()
                        .to_vec();
                    let payload = encode_resolution_input(input);
                    ins_unresolved.execute(params![repo, &file_path_bytes, payload])?;
                    for candidate_key in Self::unresolved_candidate_keys(input) {
                        ins_key.execute(params![repo, &file_path_bytes, candidate_key])?;
                    }
                }
            }

            Ok(())
        })
    }

    pub fn replace_all_unresolved_resolution_inputs_for_repo(
        &self,
        repo: &str,
        inputs: &[ResolutionInput],
    ) -> Result<(), MetadataStoreError> {
        self.with_write_txn(|tx| {
            tx.execute(
                "DELETE FROM unresolved_call_candidates WHERE repo = ?1",
                params![repo],
            )?;
            tx.execute(
                "DELETE FROM unresolved_call_candidate_keys WHERE repo = ?1",
                params![repo],
            )?;

            for input in inputs {
                let file_path_bytes = PathId::from_path(&input.file_path)
                    .as_bytes()
                    .to_vec();
                let payload = encode_resolution_input(input);
                tx.execute(
                    "INSERT INTO unresolved_call_candidates(repo, file_path, payload) VALUES (?1, ?2, ?3)",
                    params![repo, &file_path_bytes, payload],
                )?;
                for candidate_key in Self::unresolved_candidate_keys(input) {
                    tx.execute(
                        "INSERT INTO unresolved_call_candidate_keys(repo, source_path, candidate_key) VALUES (?1, ?2, ?3)",
                        params![repo, &file_path_bytes, candidate_key],
                    )?;
                }
            }

            Ok(())
        })
    }

    /// Returns every `(file_path_bytes, payload_json)` row in
    /// `unresolved_call_candidates` for `repo`, in storage order.
    ///
    /// Only available in test and `test-support` builds so the BLOB key
    /// used during inserts can be compared against the BLOB key used during
    /// deletes without going through the display-string path.
    ///
    /// Returns `(path_bytes, decoded_input)` pairs.  The payload is decoded
    /// from compact bitcode encoding; the `file_path` bytes are the raw
    /// `OsStr` bytes written during indexing.
    #[cfg(any(test, feature = "test-support"))]
    pub fn all_unresolved_rows_for_test(
        &self,
        repo: &str,
    ) -> Result<Vec<(Vec<u8>, ResolutionInput)>, MetadataStoreError> {
        let connection = self.read_connection()?;
        let mut stmt = connection.prepare_cached(
            "SELECT file_path, payload FROM unresolved_call_candidates WHERE repo = ?1",
        )?;
        let rows = stmt.query_map(params![repo], |row| {
            let path_bytes: Vec<u8> = row.get(0)?;
            let payload_bytes: Vec<u8> = row.get(1)?;
            Ok((path_bytes, payload_bytes))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (path_bytes, payload_bytes) = row?;
            let input = decode_resolution_input(&payload_bytes)?;
            out.push((path_bytes, input));
        }
        Ok(out)
    }

    pub fn unresolved_resolution_inputs_by_repo(
        &self,
        repo: &str,
    ) -> Result<Vec<ResolutionInput>, MetadataStoreError> {
        let connection = self.read_connection()?;
        let mut statement = connection.prepare_cached(
            "SELECT payload FROM unresolved_call_candidates WHERE repo = ?1 ORDER BY file_path",
        )?;
        let rows = statement.query_map(params![repo], |row| row.get::<_, Vec<u8>>(0))?;
        let mut inputs = Vec::new();
        for row in rows {
            let payload = row?;
            let input = decode_resolution_input(&payload)?;
            inputs.push(input);
        }
        Ok(inputs)
    }

    pub fn unresolved_resolution_inputs_by_candidate_keys(
        &self,
        repo: &str,
        candidate_keys: &[String],
    ) -> Result<Vec<ResolutionInput>, MetadataStoreError> {
        // SQLite's default SQLITE_LIMIT_VARIABLE_NUMBER is 999. Chunk to stay within it.
        const CHUNK_SIZE: usize = 998; // 1 slot reserved for the repo param

        if candidate_keys.is_empty() {
            return Ok(Vec::new());
        }

        let connection = self.read_connection()?;
        let mut inputs = Vec::new();
        let mut seen = rustc_hash::FxHashSet::default();
        for chunk in candidate_keys.chunks(CHUNK_SIZE) {
            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT DISTINCT c.payload
                 FROM unresolved_call_candidates c
                 JOIN unresolved_call_candidate_keys k
                   ON c.repo = k.repo AND c.file_path = k.source_path
                 WHERE c.repo = ? AND k.candidate_key IN ({placeholders})
                 ORDER BY c.file_path"
            );
            let mut statement = connection.prepare(&sql)?;
            let params = std::iter::once(repo.to_owned()).chain(chunk.iter().cloned());
            let rows = statement.query_map(rusqlite::params_from_iter(params), |row| {
                row.get::<_, Vec<u8>>(0)
            })?;
            for row in rows {
                let payload = row?;
                // Deduplicate across chunks using a hash of the raw payload bytes.
                let hash = *blake3::hash(&payload).as_bytes();
                if seen.insert(hash) {
                    let input = decode_resolution_input(&payload)?;
                    inputs.push(input);
                }
            }
        }
        Ok(inputs)
    }

    pub fn unresolved_resolution_input_count_by_repo(
        &self,
        repo: &str,
    ) -> Result<usize, MetadataStoreError> {
        let connection = self.read_connection()?;
        let count = connection.query_row(
            "SELECT COUNT(*) FROM unresolved_call_candidates WHERE repo = ?1",
            params![repo],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(usize::try_from(count).unwrap_or(usize::MAX))
    }

    pub fn latest_indexed_at(&self, repo: Option<&str>) -> Result<i64, MetadataStoreError> {
        let connection = self.read_connection()?;
        let sql = if repo.is_some() {
            "SELECT COALESCE(MAX(indexed_at), 0) FROM file_index_state WHERE repo = ?1"
        } else {
            "SELECT COALESCE(MAX(indexed_at), 0) FROM file_index_state"
        };
        let value = if let Some(repo) = repo {
            connection.query_row(sql, params![repo], |row| row.get::<_, i64>(0))?
        } else {
            connection.query_row(sql, [], |row| row.get::<_, i64>(0))?
        };
        Ok(value)
    }

    pub fn latest_indexed_at_for_files(
        &self,
        files: &[(String, String)],
    ) -> Result<i64, MetadataStoreError> {
        const CHUNK_SIZE: usize = 400;

        if files.is_empty() {
            return self.latest_indexed_at(None);
        }

        let connection = self.read_connection()?;
        let mut latest = 0_i64;
        let mut by_repo = std::collections::BTreeMap::<&str, Vec<&str>>::new();
        for (repo, file_path) in files {
            by_repo
                .entry(repo.as_str())
                .or_default()
                .push(file_path.as_str());
        }

        for (repo, paths) in by_repo {
            for chunk in paths.chunks(CHUNK_SIZE) {
                let placeholders = std::iter::repeat_n("?", chunk.len())
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!(
                    "SELECT COALESCE(MAX(indexed_at), 0)
                     FROM file_index_state
                     WHERE repo = ?1 AND file_path IN ({placeholders})"
                );
                let mut statement = connection.prepare(&sql)?;
                let path_bytes = chunk
                    .iter()
                    .map(|path| path.as_bytes().to_vec())
                    .collect::<Vec<_>>();
                let mut params: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(1 + chunk.len());
                params.push(&repo);
                for path in &path_bytes {
                    params.push(path);
                }
                let indexed_at = statement
                    .query_row(rusqlite::params_from_iter(params), |row| {
                        row.get::<_, i64>(0)
                    })?;
                latest = latest.max(indexed_at);
            }
        }
        Ok(latest)
    }

    pub fn files_requiring_reindex(
        &self,
        repo: &str,
        files: &[(&str, &[u8])],
    ) -> Result<std::collections::BTreeSet<String>, MetadataStoreError> {
        const CHUNK_SIZE: usize = 400;

        let mut required = files
            .iter()
            .map(|(file_path, _)| (*file_path).to_owned())
            .collect::<std::collections::BTreeSet<_>>();
        if files.is_empty() {
            return Ok(required);
        }

        let connection = self.read_connection()?;
        let mut stored_hashes = std::collections::BTreeMap::<String, Vec<u8>>::new();
        for chunk in files.chunks(CHUNK_SIZE) {
            let placeholders = (1..=chunk.len())
                .map(|index| format!("?{}", index + 1))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT file_path, content_hash FROM file_index_state WHERE repo = ?1 AND file_path IN ({placeholders})"
            );
            let mut statement = connection.prepare(&sql)?;
            let mut params: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(1 + chunk.len());
            params.push(&repo);
            let path_bytes = chunk
                .iter()
                .map(|(file_path, _)| file_path.as_bytes().to_vec())
                .collect::<Vec<_>>();
            for path in &path_bytes {
                params.push(path);
            }
            let rows = statement.query_map(rusqlite::params_from_iter(params), |row| {
                let path_bytes: Vec<u8> = row.get(0)?;
                let path = match String::from_utf8(path_bytes) {
                    Ok(s) => s,
                    Err(err) => String::from_utf8_lossy(err.as_bytes()).into_owned(),
                };
                Ok((path, row.get::<_, Vec<u8>>(1)?))
            })?;
            for row in rows {
                let (file_path, hash) = row?;
                stored_hashes.insert(file_path, hash);
            }
        }

        for (file_path, current_hash) in files {
            if stored_hashes.get(*file_path).is_some_and(|stored_hash| {
                stored_hash.as_slice() == stored_content_hash(current_hash)
            }) {
                required.remove(*file_path);
            }
        }

        Ok(required)
    }

    pub fn get_cached_answer(
        &self,
        cache_key: &str,
        now_unix: i64,
    ) -> Result<Option<Vec<u8>>, MetadataStoreError> {
        let connection = self.read_connection()?;
        let payload = connection
            .query_row(
                "SELECT response FROM answer_cache WHERE cache_key = ?1 AND expires_at > ?2",
                params![cache_key, now_unix],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?;
        drop(connection);

        if payload.is_some() {
            // Buffer the hit-count increment; the write is coalesced with
            // subsequent hits and flushed in a single transaction once the
            // buffer crosses HIT_COUNT_FLUSH_THRESHOLD or the store is dropped.
            self.record_hit(CacheRowId::AnswerCache {
                cache_key: cache_key.to_owned(),
            });
        }

        Ok(payload)
    }

    pub fn put_cached_answer(
        &self,
        cache_key: &str,
        response: &[u8],
        now_unix: i64,
        ttl_seconds: i64,
    ) -> Result<(), MetadataStoreError> {
        self.with_write_txn(|tx| {
            tx.execute(
                "DELETE FROM answer_cache WHERE expires_at <= ?1",
                params![now_unix],
            )?;
            tx.execute(
                "INSERT INTO answer_cache(cache_key, response, created_at, expires_at, hit_count)
                 VALUES (?1, ?2, ?3, ?4, 0)
                 ON CONFLICT(cache_key) DO UPDATE SET
                   response = excluded.response,
                   created_at = excluded.created_at,
                   expires_at = excluded.expires_at",
                params![
                    cache_key,
                    response,
                    now_unix,
                    now_unix.saturating_add(ttl_seconds)
                ],
            )?;
            Ok(())
        })
    }

    pub fn get_context_pack(
        &self,
        pack_key: &str,
    ) -> Result<Option<ContextPackRecord>, MetadataStoreError> {
        let connection = self.read_connection()?;
        let record = connection
            .query_row(
                "SELECT pack_key, mode, target, generation, response, created_at, last_read_at, byte_size, hit_count
                 FROM context_packs
                 WHERE pack_key = ?1",
                params![pack_key],
                |row| {
                    Ok(ContextPackRecord {
                        pack_key: row.get(0)?,
                        mode: row.get(1)?,
                        target: row.get(2)?,
                        generation: row.get(3)?,
                        response: row.get(4)?,
                        created_at: row.get(5)?,
                        last_read_at: row.get(6)?,
                        byte_size: row.get(7)?,
                        hit_count: row.get(8)?,
                    })
                },
            )
            .optional()?;
        Ok(record)
    }

    pub fn touch_context_pack(
        &self,
        pack_key: &str,
        now_unix: i64,
    ) -> Result<(), MetadataStoreError> {
        // Update the LRU timestamp and hit count together in a single
        // transaction — we are already writing, so folding the hit-count
        // increment in costs nothing extra.
        self.with_write_txn(|tx| {
            tx.execute(
                "UPDATE context_packs
                 SET last_read_at = ?2, hit_count = hit_count + 1
                 WHERE pack_key = ?1",
                params![pack_key, now_unix],
            )?;
            Ok(())
        })
    }

    /// Record an MCP pack tool call for the given `(target, mode)` pair.
    ///
    /// The row is created on first observation and incremented on every
    /// subsequent call. Drives the hot-whitelist used by
    /// `top_pack_call_log`.
    pub fn record_pack_call(
        &self,
        target: &str,
        mode: &str,
        now_unix: i64,
    ) -> Result<(), MetadataStoreError> {
        self.with_write_txn(|tx| {
            tx.execute(
                "INSERT INTO pack_call_log (target, mode, call_count, last_called_at)
                 VALUES (?1, ?2, 1, ?3)
                 ON CONFLICT(target, mode) DO UPDATE SET
                     call_count = pack_call_log.call_count + 1,
                     last_called_at = excluded.last_called_at",
                params![target, mode, now_unix],
            )?;
            Ok(())
        })
    }

    /// Top `limit` `(target, mode)` pairs by observed call count.
    ///
    /// Callers use this to select hot targets for precomputation. Ties are
    /// broken by most recent `last_called_at`, then lexicographic `target` and
    /// `mode` so ordering is deterministic across runs.
    pub fn top_pack_call_log(
        &self,
        limit: usize,
    ) -> Result<Vec<PackCallLogEntry>, MetadataStoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let connection = self.read_connection()?;
        let mut statement = connection.prepare_cached(
            "SELECT target, mode, call_count, last_called_at
             FROM pack_call_log
             ORDER BY call_count DESC, last_called_at DESC, target ASC, mode ASC
             LIMIT ?1",
        )?;
        let entries = statement
            .query_map(params![i64::try_from(limit).unwrap_or(i64::MAX)], |row| {
                Ok(PackCallLogEntry {
                    target: row.get(0)?,
                    mode: row.get(1)?,
                    call_count: row.get(2)?,
                    last_called_at: row.get(3)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(entries)
    }

    pub fn list_context_packs(&self) -> Result<Vec<ContextPackRecord>, MetadataStoreError> {
        let connection = self.read_connection()?;
        let mut statement = connection.prepare_cached(
            "SELECT pack_key, mode, target, generation, response, created_at, last_read_at, byte_size, hit_count
             FROM context_packs
             ORDER BY mode, target, pack_key",
        )?;
        let records = statement
            .query_map([], |row| {
                Ok(ContextPackRecord {
                    pack_key: row.get(0)?,
                    mode: row.get(1)?,
                    target: row.get(2)?,
                    generation: row.get(3)?,
                    response: row.get(4)?,
                    created_at: row.get(5)?,
                    last_read_at: row.get(6)?,
                    byte_size: row.get(7)?,
                    hit_count: row.get(8)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(records)
    }

    pub fn context_pack_stats(&self) -> Result<ContextPackStats, MetadataStoreError> {
        let connection = self.read_connection()?;
        let stats = connection.query_row(
            "SELECT COUNT(*), COALESCE(SUM(byte_size), 0), COALESCE(SUM(hit_count), 0)
             FROM context_packs",
            [],
            |row| {
                Ok(ContextPackStats {
                    total_packs: usize::try_from(row.get::<_, i64>(0)?).unwrap_or(usize::MAX),
                    total_bytes: row.get(1)?,
                    total_hits: row.get(2)?,
                })
            },
        )?;
        Ok(stats)
    }

    pub fn delete_context_packs(&self, pack_keys: &[String]) -> Result<usize, MetadataStoreError> {
        if pack_keys.is_empty() {
            return Ok(0);
        }
        self.with_write_txn(|tx| {
            let mut removed = 0_usize;
            for pack_key in pack_keys {
                removed += tx.execute(
                    "DELETE FROM context_packs WHERE pack_key = ?1",
                    params![pack_key],
                )?;
            }
            Ok(removed)
        })
    }

    pub fn clear_context_packs(&self) -> Result<usize, MetadataStoreError> {
        self.with_write_txn(|tx| {
            tx.execute("DELETE FROM context_pack_files", [])?;
            let removed = tx.execute("DELETE FROM context_packs", [])?;
            Ok(removed)
        })
    }

    pub fn put_context_pack(
        &self,
        record: &ContextPackRecord,
        files: &[(String, String)],
    ) -> Result<(), MetadataStoreError> {
        const CONTEXT_PACK_FILE_CHUNK_SIZE: usize = 300;

        self.with_write_txn(|tx| {
            Self::prune_stale_context_packs_in_tx(
                tx,
                record.created_at.max(record.last_read_at),
            )?;
            tx.execute(
                "INSERT INTO context_packs(pack_key, mode, target, generation, response, created_at, last_read_at, byte_size, hit_count)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(pack_key) DO UPDATE SET
                   mode = excluded.mode,
                   target = excluded.target,
                   generation = excluded.generation,
                   response = excluded.response,
                   created_at = excluded.created_at,
                   last_read_at = excluded.last_read_at,
                   byte_size = excluded.byte_size",
                params![
                    record.pack_key,
                    record.mode,
                    record.target,
                    record.generation,
                    record.response,
                    record.created_at,
                    record.last_read_at,
                    record.byte_size,
                    record.hit_count,
                ],
            )?;
            tx.execute(
                "DELETE FROM context_pack_files WHERE pack_key = ?1",
                params![record.pack_key],
            )?;
            for chunk in files.chunks(CONTEXT_PACK_FILE_CHUNK_SIZE) {
                let values = std::iter::repeat_n("(?, ?, ?)", chunk.len())
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!(
                    "INSERT INTO context_pack_files(pack_key, repo, file_path) VALUES {values}"
                );
                let mut params: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(chunk.len() * 3);
                for (repo, file_path) in chunk {
                    params.push(&record.pack_key);
                    params.push(repo);
                    params.push(file_path);
                }
                tx.execute(&sql, rusqlite::params_from_iter(params))?;
            }
            Ok(())
        })
    }

    /// Returns the `(repo, file_path)` dependency pairs recorded for a cached
    /// pack.  Used by the two-phase cache validation path to recompute the
    /// current generation for an already-identified pack without re-running the
    /// full generation-scope computation.
    ///
    /// Returns an empty `Vec` when the pack key is unknown (cache miss) or when
    /// the pack has no recorded file dependencies (e.g. packs for unresolved
    /// targets).
    pub fn context_pack_files_for_key(
        &self,
        pack_key: &str,
    ) -> Result<Vec<(String, String)>, MetadataStoreError> {
        let connection = self.read_connection()?;
        let mut stmt = connection
            .prepare_cached("SELECT repo, file_path FROM context_pack_files WHERE pack_key = ?1")?;
        let rows = stmt.query_map(params![pack_key], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn invalidate_context_packs_for_files(
        &self,
        repo: &str,
        file_paths: &[String],
    ) -> Result<usize, MetadataStoreError> {
        if file_paths.is_empty() {
            return Ok(0);
        }
        let targets = file_paths
            .iter()
            .cloned()
            .map(|file_path| (repo.to_owned(), file_path))
            .collect::<Vec<_>>();
        self.invalidate_context_packs_for_targets(&targets)
    }

    pub fn invalidate_context_packs_for_targets(
        &self,
        targets: &[(String, String)],
    ) -> Result<usize, MetadataStoreError> {
        const TARGET_CHUNK_SIZE: usize = 300;

        if targets.is_empty() {
            return Ok(0);
        }
        self.with_write_txn(|tx| {
            let mut pack_keys = BTreeSet::new();
            for chunk in targets.chunks(TARGET_CHUNK_SIZE) {
                let clauses = std::iter::repeat_n("(repo = ? AND file_path = ?)", chunk.len())
                    .collect::<Vec<_>>()
                    .join(" OR ");
                let sql =
                    format!("SELECT DISTINCT pack_key FROM context_pack_files WHERE {clauses}");
                let mut select = tx.prepare(&sql)?;
                let params = chunk
                    .iter()
                    .flat_map(|(repo, file_path)| [repo as &dyn rusqlite::ToSql, file_path]);
                let rows = select.query_map(rusqlite::params_from_iter(params), |row| {
                    row.get::<_, String>(0)
                })?;
                for pack_key in rows {
                    pack_keys.insert(pack_key?);
                }
            }

            let mut invalidated = 0_usize;
            let pack_keys = pack_keys.into_iter().collect::<Vec<_>>();
            for chunk in pack_keys.chunks(TARGET_CHUNK_SIZE) {
                let placeholders = std::iter::repeat_n("?", chunk.len())
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!("DELETE FROM context_packs WHERE pack_key IN ({placeholders})");
                invalidated += tx.execute(&sql, rusqlite::params_from_iter(chunk.iter()))?;
            }
            Ok(invalidated)
        })
    }
}

impl ConnectionPool {
    fn new(connections: Vec<Connection>) -> Self {
        Self {
            connections: Mutex::new(connections),
            available: Condvar::new(),
        }
    }

    fn acquire(&self) -> Result<PooledConnection<'_>, MetadataStoreError> {
        let mut connections = self
            .connections
            .lock()
            .map_err(|_| MetadataStoreError::Poisoned)?;

        loop {
            if let Some(connection) = connections.pop() {
                return Ok(PooledConnection {
                    connection: Some(connection),
                    pool: self,
                });
            }

            connections = self
                .available
                .wait(connections)
                .map_err(|_| MetadataStoreError::Poisoned)?;
        }
    }

    fn release(&self, connection: Connection) {
        if let Ok(mut connections) = self.connections.lock() {
            connections.push(connection);
            self.available.notify_one();
        }
    }
}

impl std::ops::Deref for PooledConnection<'_> {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        self.connection
            .as_ref()
            .expect("pooled connection must be present while borrowed")
    }
}

impl std::ops::DerefMut for PooledConnection<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.connection
            .as_mut()
            .expect("pooled connection must be present while borrowed")
    }
}

impl Drop for PooledConnection<'_> {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            self.pool.release(connection);
        }
    }
}

/// Test-only: raw bytes stored in the `file_path` BLOB column for a given repo.
///
/// Returns one `Vec<u8>` per row in `file_index_state` for the repo, in the
/// order returned by `SQLite` (ascending by BLOB value). Intended for
/// asserting that non-UTF-8 path bytes survive the full indexing pipeline
/// without lossy conversion.
#[cfg(any(test, feature = "test-support"))]
impl MetadataStoreDb {
    pub fn raw_file_path_bytes_for_repo(
        &self,
        repo: &str,
    ) -> Result<Vec<Vec<u8>>, MetadataStoreError> {
        let connection = self.read_connection()?;
        let mut stmt = connection.prepare_cached(
            "SELECT file_path FROM file_index_state WHERE repo = ?1 ORDER BY file_path ASC",
        )?;
        let rows = stmt
            .query_map(rusqlite::params![repo], |row| row.get::<_, Vec<u8>>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }
}

impl Drop for MetadataStoreDb {
    fn drop(&mut self) {
        // Flush any buffered hit-count deltas before closing the connections.
        self.flush_hit_counts();

        if let Ok(connection) = self.writer.get_mut() {
            Self::optimize_connection(connection);
        }

        if let Ok(connections) = self.readers.connections.get_mut() {
            for connection in connections.iter() {
                Self::optimize_connection(connection);
            }
        }
    }
}

impl MetadataStore for MetadataStoreDb {
    fn upsert_file_state(&self, state: &FileIndexState) -> Result<(), MetadataStoreError> {
        let path_bytes = state.effective_path_bytes();
        let connection = self.lock_writer()?;
        let mut statement = connection.prepare_cached(UPSERT_FILE_STATE_SQL)?;
        statement.execute(params![
            &state.repo,
            path_bytes,
            stored_content_hash(&state.content_hash),
            state.size_bytes,
            state.mtime_ns,
            state.node_count,
            state.edge_count,
            state.indexed_at,
            state.parse_ms
        ])?;
        Ok(())
    }

    fn upsert_file_states(&self, states: &[FileIndexState]) -> Result<(), MetadataStoreError> {
        let mut connection = self.lock_writer()?;
        let tx = connection.transaction()?;
        {
            let mut statement = tx.prepare_cached(UPSERT_FILE_STATE_SQL)?;
            for state in states {
                let path_bytes = state.effective_path_bytes();
                statement.execute(params![
                    &state.repo,
                    path_bytes,
                    stored_content_hash(&state.content_hash),
                    state.size_bytes,
                    state.mtime_ns,
                    state.node_count,
                    state.edge_count,
                    state.indexed_at,
                    state.parse_ms
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn should_reindex(
        &self,
        repo: &str,
        file_path: &str,
        current_hash: &[u8],
    ) -> Result<bool, MetadataStoreError> {
        let connection = self.read_connection()?;
        let stored_hash = connection
            .query_row(
                "SELECT content_hash FROM file_index_state WHERE repo = ?1 AND file_path = ?2",
                params![repo, file_path.as_bytes()],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?;

        Ok(match stored_hash {
            Some(hash) => hash != stored_content_hash(current_hash),
            None => true,
        })
    }

    fn should_reindex_batch(
        &self,
        repo: &str,
        files: &[(&str, &[u8])],
    ) -> Result<rustc_hash::FxHashSet<String>, MetadataStoreError> {
        const CHUNK_SIZE: usize = 400;

        let mut required: rustc_hash::FxHashSet<String> = files
            .iter()
            .map(|(file_path, _)| (*file_path).to_owned())
            .collect();
        if files.is_empty() {
            return Ok(required);
        }

        let connection = self.read_connection()?;
        let mut stored_hashes: rustc_hash::FxHashMap<String, Vec<u8>> =
            rustc_hash::FxHashMap::default();

        for chunk in files.chunks(CHUNK_SIZE) {
            let placeholders = (1..=chunk.len())
                .map(|index| format!("?{}", index + 1))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT file_path, content_hash FROM file_index_state \
                 WHERE repo = ?1 AND file_path IN ({placeholders})"
            );
            let mut statement = connection.prepare(&sql)?;
            let path_bytes = chunk
                .iter()
                .map(|(file_path, _)| file_path.as_bytes().to_vec())
                .collect::<Vec<_>>();
            let mut params: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(1 + chunk.len());
            params.push(&repo);
            for path in &path_bytes {
                params.push(path);
            }
            let rows = statement.query_map(rusqlite::params_from_iter(params), |row| {
                let path_bytes: Vec<u8> = row.get(0)?;
                let path = match String::from_utf8(path_bytes) {
                    Ok(s) => s,
                    Err(err) => String::from_utf8_lossy(err.as_bytes()).into_owned(),
                };
                Ok((path, row.get::<_, Vec<u8>>(1)?))
            })?;
            for row in rows {
                let (file_path, hash) = row?;
                stored_hashes.insert(file_path, hash);
            }
        }

        for (file_path, current_hash) in files {
            if stored_hashes
                .get(*file_path)
                .is_some_and(|stored| stored.as_slice() == stored_content_hash(current_hash))
            {
                required.remove(*file_path);
            }
        }

        Ok(required)
    }

    fn should_reindex_batch_by_path_bytes(
        &self,
        repo: &str,
        files: &[(&[u8], &[u8])],
    ) -> Result<rustc_hash::FxHashSet<Vec<u8>>, MetadataStoreError> {
        const CHUNK_SIZE: usize = 400;

        let mut required: rustc_hash::FxHashSet<Vec<u8>> = files
            .iter()
            .map(|(file_path, _)| (*file_path).to_vec())
            .collect();
        if files.is_empty() {
            return Ok(required);
        }

        let connection = self.read_connection()?;
        let mut stored_hashes: rustc_hash::FxHashMap<Vec<u8>, Vec<u8>> =
            rustc_hash::FxHashMap::default();

        for chunk in files.chunks(CHUNK_SIZE) {
            let placeholders = (1..=chunk.len())
                .map(|index| format!("?{}", index + 1))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT file_path, content_hash FROM file_index_state \
                 WHERE repo = ?1 AND file_path IN ({placeholders})"
            );
            let mut statement = connection.prepare(&sql)?;
            let path_bytes = chunk
                .iter()
                .map(|(file_path, _)| (*file_path).to_vec())
                .collect::<Vec<_>>();
            let mut params: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(1 + chunk.len());
            params.push(&repo);
            for path in &path_bytes {
                params.push(path);
            }
            let rows = statement.query_map(rusqlite::params_from_iter(params), |row| {
                Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
            })?;
            for row in rows {
                let (file_path, hash) = row?;
                stored_hashes.insert(file_path, hash);
            }
        }

        for (file_path, current_hash) in files {
            if stored_hashes
                .get(*file_path)
                .is_some_and(|stored| stored.as_slice() == stored_content_hash(current_hash))
            {
                required.remove(*file_path);
            }
        }

        Ok(required)
    }

    fn insert_commit(&self, commit: &CommitRecord) -> Result<(), MetadataStoreError> {
        let connection = self.lock_writer()?;
        let mut statement = connection.prepare_cached(UPSERT_COMMIT_SQL)?;
        statement.execute(params![
            &commit.sha,
            &commit.repo,
            &commit.author_email,
            commit.date,
            &commit.message,
            &commit.classification,
            commit.files_changed,
            commit.insertions,
            commit.deletions,
            commit.has_decision_signal,
            commit.pr_number
        ])?;
        Ok(())
    }

    fn insert_commits(&self, commits: &[CommitRecord]) -> Result<(), MetadataStoreError> {
        let mut connection = self.lock_writer()?;
        let tx = connection.transaction()?;
        {
            let mut statement = tx.prepare_cached(UPSERT_COMMIT_SQL)?;
            for commit in commits {
                statement.execute(params![
                    &commit.sha,
                    &commit.repo,
                    &commit.author_email,
                    commit.date,
                    &commit.message,
                    &commit.classification,
                    commit.files_changed,
                    commit.insertions,
                    commit.deletions,
                    commit.has_decision_signal,
                    commit.pr_number
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn get_commits_by_repo(
        &self,
        repo: &str,
        start_date: i64,
        end_date: i64,
    ) -> Result<Vec<CommitRecord>, MetadataStoreError> {
        let connection = self.read_connection()?;
        let mut statement = connection.prepare_cached(
            r"
            SELECT
                sha,
                repo,
                author_email,
                date,
                message,
                classification,
                files_changed,
                insertions,
                deletions,
                has_decision_signal,
                pr_number
            FROM commits
            WHERE repo = ?1 AND date >= ?2 AND date <= ?3
            ORDER BY date ASC, sha ASC
            ",
        )?;
        let rows =
            statement.query_map(params![repo, start_date, end_date], Self::commit_from_row)?;
        let commits = rows.collect::<Result<Vec<_>, _>>()?;
        Ok(commits)
    }

    fn upsert_commit_file_delta(
        &self,
        delta: &CommitFileDeltaRecord,
    ) -> Result<(), MetadataStoreError> {
        let connection = self.lock_writer()?;
        let mut statement = connection.prepare_cached(UPSERT_COMMIT_FILE_DELTA_SQL)?;
        statement.execute(params![
            &delta.repo,
            &delta.sha,
            &delta.file_path,
            delta.change_kind.as_str(),
            delta.insertions,
            delta.deletions,
            &delta.old_path,
        ])?;
        Ok(())
    }

    fn upsert_commit_file_deltas(
        &self,
        deltas: &[CommitFileDeltaRecord],
    ) -> Result<(), MetadataStoreError> {
        let mut connection = self.lock_writer()?;
        let tx = connection.transaction()?;
        {
            let mut statement = tx.prepare_cached(UPSERT_COMMIT_FILE_DELTA_SQL)?;
            for delta in deltas {
                statement.execute(params![
                    &delta.repo,
                    &delta.sha,
                    &delta.file_path,
                    delta.change_kind.as_str(),
                    delta.insertions,
                    delta.deletions,
                    &delta.old_path,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn get_commit_file_deltas(
        &self,
        repo: &str,
        sha: &str,
    ) -> Result<Vec<CommitFileDeltaRecord>, MetadataStoreError> {
        let connection = self.read_connection()?;
        let mut statement = connection.prepare_cached(
            r"
            SELECT repo, sha, file_path, change_kind, insertions, deletions, old_path
            FROM commit_file_deltas
            WHERE repo = ?1 AND sha = ?2
            ORDER BY file_path ASC
            ",
        )?;
        let rows = statement.query_map(params![repo, sha], Self::commit_file_delta_from_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    fn get_commit_file_deltas_for_repo(
        &self,
        repo: &str,
    ) -> Result<Vec<CommitFileDeltaRecord>, MetadataStoreError> {
        let connection = self.read_connection()?;
        let mut statement = connection.prepare_cached(
            r"
            SELECT repo, sha, file_path, change_kind, insertions, deletions, old_path
            FROM commit_file_deltas
            WHERE repo = ?1
            ORDER BY sha ASC, file_path ASC
            ",
        )?;
        let rows = statement.query_map(params![repo], Self::commit_file_delta_from_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    fn get_history_for_file_with_renames(
        &self,
        repo: &str,
        file_path: &str,
    ) -> Result<(Vec<CommitRecord>, Vec<CommitFileDeltaRecord>), MetadataStoreError> {
        // Cap on rename hops walked while resolving historical paths. A
        // chain longer than 64 is almost certainly a pathological rename
        // loop; bounding here keeps a single MCP call from degenerating
        // into an unbounded SQL fan-out.
        const MAX_RENAME_HOPS: usize = 64;
        const CHUNK_SIZE: usize = 300;

        let connection = self.read_connection()?;

        // Step 1: walk the rename chain backwards from `file_path` to
        // discover every historical name that contributed to the current
        // file's content. We BFS over `Renamed` deltas because a path
        // may have multiple predecessors via merges or repeated renames.
        let mut historical_paths = std::collections::BTreeSet::<String>::new();
        historical_paths.insert(file_path.to_owned());
        let mut frontier: Vec<String> = vec![file_path.to_owned()];
        let mut hop = 0_usize;
        while !frontier.is_empty() && hop < MAX_RENAME_HOPS {
            let mut next_frontier = Vec::new();
            for chunk in frontier.chunks(CHUNK_SIZE) {
                let placeholders = std::iter::repeat_n("?", chunk.len())
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!(
                    r"
                    SELECT old_path
                    FROM commit_file_deltas
                    WHERE repo = ?1 AND change_kind = 'renamed'
                      AND file_path IN ({placeholders})
                      AND old_path IS NOT NULL
                    "
                );
                let mut statement = connection.prepare(&sql)?;
                let params = std::iter::once(&repo as &dyn rusqlite::ToSql)
                    .chain(chunk.iter().map(|path| path as &dyn rusqlite::ToSql));
                let rows = statement.query_map(rusqlite::params_from_iter(params), |row| {
                    row.get::<_, String>(0)
                })?;
                for old_path in rows {
                    let old_path = old_path?;
                    if historical_paths.insert(old_path.clone()) {
                        next_frontier.push(old_path);
                    }
                }
            }
            frontier = next_frontier;
            hop += 1;
        }

        if historical_paths.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        // Step 2: fetch all deltas whose `file_path` (or `old_path` for
        // rename deltas) appears in the resolved set. We rebuild the
        // placeholders dynamically because rusqlite parameter binding
        // does not support array IN clauses.
        let historical_paths = historical_paths.into_iter().collect::<Vec<_>>();
        let mut deltas = Vec::new();
        for chunk in historical_paths.chunks(CHUNK_SIZE) {
            let placeholders = (1..=chunk.len())
                .map(|index| format!("?{}", index + 1))
                .collect::<Vec<_>>()
                .join(", ");
            let deltas_sql = format!(
                r"
                SELECT repo, sha, file_path, change_kind, insertions, deletions, old_path
                FROM commit_file_deltas
                WHERE repo = ?1
                  AND (file_path IN ({placeholders}) OR old_path IN ({placeholders}))
                ORDER BY sha ASC, file_path ASC
                "
            );
            let mut deltas_stmt = connection.prepare(&deltas_sql)?;
            let mut params_buf: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(1 + chunk.len());
            params_buf.push(&repo);
            for path in chunk {
                params_buf.push(path as &dyn rusqlite::ToSql);
            }
            let delta_rows = deltas_stmt.query_map(
                rusqlite::params_from_iter(params_buf),
                Self::commit_file_delta_from_row,
            )?;
            deltas.extend(delta_rows.collect::<Result<Vec<_>, _>>()?);
        }
        deltas.sort_by(|left, right| {
            left.sha
                .cmp(&right.sha)
                .then(left.file_path.cmp(&right.file_path))
                .then(left.old_path.cmp(&right.old_path))
                .then(left.change_kind.as_str().cmp(right.change_kind.as_str()))
        });
        deltas.dedup_by(|left, right| {
            left.repo == right.repo
                && left.sha == right.sha
                && left.file_path == right.file_path
                && left.old_path == right.old_path
                && left.change_kind == right.change_kind
        });

        // Step 3: fetch the commits referenced by those deltas, in date
        // order to match the existing API contract for callers that
        // expect chronological iteration.
        let unique_shas = deltas
            .iter()
            .map(|delta| delta.sha.clone())
            .collect::<std::collections::BTreeSet<_>>();
        if unique_shas.is_empty() {
            return Ok((Vec::new(), deltas));
        }
        let unique_shas = unique_shas.into_iter().collect::<Vec<_>>();
        let mut commits = Vec::new();
        for chunk in unique_shas.chunks(CHUNK_SIZE) {
            let sha_placeholders = (1..=chunk.len())
                .map(|index| format!("?{}", index + 1))
                .collect::<Vec<_>>()
                .join(", ");
            let commits_sql = format!(
                r"
                SELECT
                    sha, repo, author_email, date, message, classification,
                    files_changed, insertions, deletions, has_decision_signal,
                    pr_number
                FROM commits
                WHERE repo = ?1 AND sha IN ({sha_placeholders})
                ORDER BY date ASC, sha ASC
                "
            );
            let mut commits_stmt = connection.prepare(&commits_sql)?;
            let mut commit_params: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(1 + chunk.len());
            commit_params.push(&repo);
            for sha in chunk {
                commit_params.push(sha as &dyn rusqlite::ToSql);
            }
            let commit_rows = commits_stmt.query_map(
                rusqlite::params_from_iter(commit_params),
                Self::commit_from_row,
            )?;
            commits.extend(commit_rows.collect::<Result<Vec<_>, _>>()?);
        }
        commits.sort_by(|left, right| left.date.cmp(&right.date).then(left.sha.cmp(&right.sha)));
        commits.dedup_by(|left, right| left.repo == right.repo && left.sha == right.sha);

        Ok((commits, deltas))
    }

    fn delete_commits_for_repo(&self, repo: &str) -> Result<(), MetadataStoreError> {
        // The `commit_file_deltas` rows go away via ON DELETE CASCADE on the FK
        // declared in `CURRENT_SCHEMA`. `repo_sync_state.last_commit_sha` is left in
        // place because incremental git sync overwrites it on the next run; the
        // worst-case after this delete is a single full rescan, which is exactly what
        // the history-rewrite fallback already commits to.
        self.with_write_txn(|tx| {
            tx.execute("DELETE FROM commits WHERE repo = ?1", params![repo])?;
            Ok(())
        })
    }

    fn replace_repo_history(
        &self,
        repo: &str,
        commits: &[CommitRecord],
        deltas: &[CommitFileDeltaRecord],
        last_commit_sha: &str,
        synced_at: i64,
    ) -> Result<(), MetadataStoreError> {
        // Single transaction: purge old facts, write new ones, advance anchor.
        // If any step fails the txn rolls back and the previous (consistent)
        // history slice is preserved.
        self.with_write_txn(|tx| {
            tx.execute("DELETE FROM commits WHERE repo = ?1", params![repo])?;
            tx.execute("DELETE FROM file_analytics WHERE repo = ?1", params![repo])?;
            tx.execute("DELETE FROM co_change_pairs WHERE repo = ?1", params![repo])?;

            {
                let mut commit_stmt = tx.prepare_cached(UPSERT_COMMIT_SQL)?;
                for commit in commits {
                    commit_stmt.execute(params![
                        &commit.sha,
                        &commit.repo,
                        &commit.author_email,
                        commit.date,
                        &commit.message,
                        &commit.classification,
                        commit.files_changed,
                        commit.insertions,
                        commit.deletions,
                        commit.has_decision_signal,
                        commit.pr_number,
                    ])?;
                }
            }

            {
                let mut delta_stmt = tx.prepare_cached(UPSERT_COMMIT_FILE_DELTA_SQL)?;
                for delta in deltas {
                    delta_stmt.execute(params![
                        &delta.repo,
                        &delta.sha,
                        &delta.file_path,
                        delta.change_kind.as_str(),
                        delta.insertions,
                        delta.deletions,
                        &delta.old_path,
                    ])?;
                }
            }

            tx.execute(
                r"
                INSERT INTO repo_sync_state (repo, last_commit_sha, synced_at)
                VALUES (?1, ?2, ?3)
                ON CONFLICT(repo) DO UPDATE SET
                    last_commit_sha = excluded.last_commit_sha,
                    synced_at = excluded.synced_at
                ",
                params![repo, last_commit_sha, synced_at],
            )?;

            Ok(())
        })
    }

    fn get_last_commit_sha(&self, repo: &str) -> Result<Option<String>, MetadataStoreError> {
        let connection = self.read_connection()?;
        Ok(connection
            .query_row(
                "SELECT last_commit_sha FROM repo_sync_state WHERE repo = ?1",
                params![repo],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten())
    }

    fn set_last_commit_sha(
        &self,
        repo: &str,
        last_commit_sha: &str,
        synced_at: i64,
    ) -> Result<(), MetadataStoreError> {
        // ON CONFLICT updates only the two columns this git-sync path owns; PR/issue
        // cursors and GitHub ETag belong to a different sync path and must
        // not be clobbered when a git-only sync writes back to this row.
        let connection = self.lock_writer()?;
        connection.execute(
            r"
            INSERT INTO repo_sync_state (repo, last_commit_sha, synced_at)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(repo) DO UPDATE SET
                last_commit_sha = excluded.last_commit_sha,
                synced_at = excluded.synced_at
            ",
            params![repo, last_commit_sha, synced_at],
        )?;
        Ok(())
    }

    fn get_file_analytics(
        &self,
        repo: &str,
        file_path: &str,
    ) -> Result<Option<FileAnalytics>, MetadataStoreError> {
        let connection = self.read_connection()?;
        let analytics = connection
            .query_row(
                r"
                SELECT
                    repo,
                    file_path,
                    total_commits,
                    commits_90d,
                    commits_180d,
                    commits_365d,
                    hotspot_score,
                    bus_factor,
                    top_owner_email,
                    top_owner_pct,
                    complexity_trend,
                    last_modified,
                    computed_at
                FROM file_analytics
                WHERE repo = ?1 AND file_path = ?2
                ",
                params![repo, file_path],
                Self::file_analytics_from_row,
            )
            .optional()?;
        Ok(analytics)
    }

    fn list_file_analytics_for_repo(
        &self,
        repo: &str,
    ) -> Result<Vec<FileAnalytics>, MetadataStoreError> {
        let connection = self.read_connection()?;
        let mut statement = connection.prepare_cached(
            r"
            SELECT
                repo,
                file_path,
                total_commits,
                commits_90d,
                commits_180d,
                commits_365d,
                hotspot_score,
                bus_factor,
                top_owner_email,
                top_owner_pct,
                complexity_trend,
                last_modified,
                computed_at
            FROM file_analytics
            WHERE repo = ?1
            ORDER BY hotspot_score DESC, file_path ASC
            ",
        )?;
        let rows = statement.query_map(params![repo], Self::file_analytics_from_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    fn replace_file_analytics_for_repo(
        &self,
        repo: &str,
        analytics: &[FileAnalytics],
    ) -> Result<(), MetadataStoreError> {
        self.with_write_txn(|tx| {
            tx.execute("DELETE FROM file_analytics WHERE repo = ?1", params![repo])?;
            let mut statement = tx.prepare_cached(UPSERT_FILE_ANALYTICS_SQL)?;
            for record in analytics {
                statement.execute(params![
                    &record.repo,
                    &record.file_path,
                    record.total_commits,
                    record.commits_90d,
                    record.commits_180d,
                    record.commits_365d,
                    record.hotspot_score,
                    record.bus_factor,
                    &record.top_owner_email,
                    record.top_owner_pct,
                    &record.complexity_trend,
                    record.last_modified,
                    record.computed_at,
                ])?;
            }
            Ok(())
        })
    }

    fn get_co_change_pairs_for_repo(
        &self,
        repo: &str,
    ) -> Result<Vec<CoChangePairRecord>, MetadataStoreError> {
        let connection = self.read_connection()?;
        let mut statement = connection.prepare_cached(
            r"
            SELECT repo, file_a, file_b, strength, occurrences, last_seen
            FROM co_change_pairs
            WHERE repo = ?1
            ORDER BY strength DESC, file_a ASC, file_b ASC
            ",
        )?;
        let rows = statement.query_map(params![repo], Self::co_change_pair_from_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    fn replace_co_change_pairs_for_repo(
        &self,
        repo: &str,
        pairs: &[CoChangePairRecord],
    ) -> Result<(), MetadataStoreError> {
        self.with_write_txn(|tx| {
            tx.execute("DELETE FROM co_change_pairs WHERE repo = ?1", params![repo])?;
            let mut statement = tx.prepare_cached(UPSERT_CO_CHANGE_PAIR_SQL)?;
            for pair in pairs {
                statement.execute(params![
                    &pair.repo,
                    &pair.file_a,
                    &pair.file_b,
                    pair.strength,
                    pair.occurrences,
                    pair.last_seen,
                ])?;
            }
            Ok(())
        })
    }

    fn replace_payload_contracts_for_files(
        &self,
        repo: &str,
        file_paths: &[String],
        records: &[PayloadContractStoreRecord],
    ) -> Result<(), MetadataStoreError> {
        self.with_write_txn(|tx| {
            for file_path in file_paths {
                tx.execute(
                    "DELETE FROM payload_contracts WHERE repo = ?1 AND file_path = ?2",
                    params![repo, file_path.as_bytes()],
                )?;
            }

            for record in records {
                let contract_json = serde_json::to_vec(&record.record.contract)
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
                let inference_kind = record.record.inference_kind.as_sql_str();
                let side = match record.record.side {
                    PayloadSide::Producer => "producer",
                    PayloadSide::Consumer => "consumer",
                };
                tx.execute(
                    r"
                    INSERT INTO payload_contracts(
                        payload_contract_node_id,
                        contract_target_node_id,
                        contract_target_kind,
                        contract_target_qn,
                        repo,
                        file_path,
                        source_symbol_node_id,
                        line_start,
                        side,
                        confidence,
                        inference_kind,
                        source_type_name,
                        contract_json
                    )
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                    ",
                    params![
                        record.record.payload_contract_node_id.as_bytes().to_vec(),
                        record.record.contract_target_node_id.as_bytes().to_vec(),
                        record.record.contract_target_kind as i64,
                        record.record.contract_target_qualified_name,
                        record.record.repo,
                        record.record.file_path.as_bytes(),
                        record.record.source_symbol_node_id.as_bytes().to_vec(),
                        record.record.line_start,
                        side,
                        record.record.confidence,
                        inference_kind,
                        record.record.source_type_name,
                        contract_json,
                    ],
                )?;
            }
            Ok(())
        })
    }

    fn payload_contracts_for_query(
        &self,
        query: PayloadContractQuery,
    ) -> Result<Vec<PayloadContractStoreRecord>, MetadataStoreError> {
        let connection = self.read_connection()?;
        let mut sql = String::from(
            r"
            SELECT
                payload_contract_node_id,
                contract_target_node_id,
                contract_target_kind,
                contract_target_qn,
                repo,
                file_path,
                source_symbol_node_id,
                line_start,
                side,
                confidence,
                inference_kind,
                source_type_name,
                contract_json
            FROM payload_contracts
            WHERE 1 = 1
            ",
        );
        let mut params_buf: Vec<rusqlite::types::Value> = Vec::new();
        if let Some(target) = query.contract_target_node_id {
            sql.push_str(" AND contract_target_node_id = ? ");
            params_buf.push(rusqlite::types::Value::Blob(target.as_bytes().to_vec()));
        }
        if let Some(target_kind) = query.contract_target_kind {
            sql.push_str(" AND contract_target_kind = ? ");
            params_buf.push(rusqlite::types::Value::Integer(i64::from(
                target_kind as u8,
            )));
        }
        if let Some(target_qn) = query.contract_target_qualified_name {
            sql.push_str(" AND contract_target_qn = ? ");
            params_buf.push(rusqlite::types::Value::Text(target_qn));
        }
        if let Some(source_symbol_id) = query.source_symbol_node_id {
            sql.push_str(" AND source_symbol_node_id = ? ");
            params_buf.push(rusqlite::types::Value::Blob(
                source_symbol_id.as_bytes().to_vec(),
            ));
        }
        if let Some(repo) = query.repo {
            sql.push_str(" AND repo = ? ");
            params_buf.push(rusqlite::types::Value::Text(repo));
        }
        if let Some(side) = query.side {
            sql.push_str(" AND side = ? ");
            params_buf.push(rusqlite::types::Value::Text(match side {
                PayloadSide::Producer => "producer".to_owned(),
                PayloadSide::Consumer => "consumer".to_owned(),
            }));
        }
        if let Some(min_confidence) = query.min_confidence {
            sql.push_str(" AND confidence >= ? ");
            params_buf.push(rusqlite::types::Value::Integer(i64::from(min_confidence)));
        }
        if let Some(source_type_name) = query.source_type_name {
            sql.push_str(" AND source_type_name = ? ");
            params_buf.push(rusqlite::types::Value::Text(source_type_name));
        }
        sql.push_str(" ORDER BY confidence DESC, repo ASC, file_path ASC");

        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map(
            rusqlite::params_from_iter(params_buf),
            Self::payload_record_from_row,
        )?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }
}

fn decode_node_id_blob(bytes: &[u8]) -> Result<NodeId, rusqlite::Error> {
    if bytes.len() != 16 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            bytes.len(),
            rusqlite::types::Type::Blob,
            "invalid node id length".into(),
        ));
    }
    let mut raw = [0_u8; 16];
    raw.copy_from_slice(bytes);
    Ok(NodeId(raw))
}

fn decode_node_kind(value: i64) -> Result<NodeKind, rusqlite::Error> {
    let value = u8::try_from(value).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            std::mem::size_of::<i64>(),
            rusqlite::types::Type::Integer,
            format!("invalid node kind value: {value}").into(),
        )
    })?;
    NodeKind::all()
        .iter()
        .copied()
        .find(|kind| *kind as u8 == value)
        .ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                std::mem::size_of::<u8>(),
                rusqlite::types::Type::Integer,
                format!("unknown node kind value: {value}").into(),
            )
        })
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        fs,
        path::{Path, PathBuf},
        sync::{
            Arc, Barrier,
            atomic::{AtomicU64, Ordering},
        },
        thread,
        time::Duration,
    };

    use pretty_assertions::assert_eq;
    use rusqlite::params;

    use gather_step_core::{
        PayloadContractDoc, PayloadContractRecord, PayloadField, PayloadInferenceKind, PayloadSide,
        ref_node_id,
    };

    use super::{
        CONTEXT_PACK_RETENTION_SECONDS, CoChangePairRecord, CommitFileChangeKind,
        CommitFileDeltaRecord, CommitRecord, ContextPackRecord, FileAnalytics, FileIndexState,
        MetadataStore, MetadataStoreDb, MetadataStoreError, PayloadContractQuery,
        PayloadContractStoreRecord, SQLITE_PAGE_SIZE_BYTES,
    };

    static NEXT_TEST_DB_ID: AtomicU64 = AtomicU64::new(0);

    const EXPECTED_TABLES: &[&str] = &[
        "answer_cache",
        "authors",
        "co_change_pairs",
        "comments",
        "context_pack_files",
        "context_packs",
        "commit_file_deltas",
        "commits",
        "conventions",
        "file_analytics",
        "file_dependencies",
        "file_index_state",
        "pack_call_log",
        "payload_contracts",
        "pull_requests",
        "repo_sync_state",
        "reviews",
        "tickets",
        "unresolved_call_candidate_keys",
        "unresolved_call_candidates",
    ];

    struct TestDbPath {
        path: PathBuf,
    }

    impl TestDbPath {
        fn new(test_name: &str) -> Self {
            let unique_id = NEXT_TEST_DB_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "gather-step-storage-{test_name}-{}-{unique_id}.sqlite",
                std::process::id()
            ));
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDbPath {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let candidate = PathBuf::from(format!("{}{}", self.path.display(), suffix));
                let _ = fs::remove_file(candidate);
            }
        }
    }

    fn open_store(test_name: &str) -> Result<(TestDbPath, MetadataStoreDb), MetadataStoreError> {
        let db_path = TestDbPath::new(test_name);
        let store = MetadataStoreDb::open(db_path.path())?;
        Ok((db_path, store))
    }

    #[test]
    fn wal_mode_is_enabled() -> Result<(), MetadataStoreError> {
        let (_db_path, store) = open_store("wal-mode")?;
        let connection = store.read_connection()?;
        let journal_mode =
            connection.pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))?;

        assert_eq!(journal_mode, "wal");
        Ok(())
    }

    #[test]
    fn sqlite_pragmas_are_tuned_for_new_databases() -> Result<(), MetadataStoreError> {
        let (_db_path, store) = open_store("sqlite-pragmas")?;
        let connection = store.read_connection()?;
        let temp_store =
            connection.pragma_query_value(None, "temp_store", |row| row.get::<_, i64>(0))?;
        let page_size =
            connection.pragma_query_value(None, "page_size", |row| row.get::<_, i64>(0))?;

        assert_eq!(temp_store, 2);
        assert_eq!(page_size, SQLITE_PAGE_SIZE_BYTES);
        Ok(())
    }

    #[test]
    fn bootstrap_creates_all_expected_tables() -> Result<(), MetadataStoreError> {
        let (_db_path, store) = open_store("bootstrap")?;
        let connection = store.read_connection()?;
        let mut statement = connection.prepare(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
        )?;
        let table_names = statement.query_map([], |row| row.get::<_, String>(0))?;
        let actual_tables = table_names.collect::<Result<BTreeSet<_>, _>>()?;
        let expected_tables = EXPECTED_TABLES
            .iter()
            .map(|name| (*name).to_owned())
            .collect::<BTreeSet<_>>();

        assert_eq!(actual_tables, expected_tables);
        Ok(())
    }

    #[test]
    fn latest_indexed_at_for_files_handles_large_multi_repo_batches()
    -> Result<(), MetadataStoreError> {
        let (_db_path, store) = open_store("latest-indexed-large")?;
        let mut states = Vec::new();
        let mut query_files = Vec::new();
        for index in 0..2_500 {
            let backend = format!("src/backend_{index}.ts");
            let frontend = format!("src/frontend_{index}.ts");
            states.push(FileIndexState {
                repo: "backend_standard".to_owned(),
                file_path: backend.clone(),
                path_id_bytes: Vec::new(),
                content_hash: vec![1, 2, 3, 4],
                size_bytes: 0,
                mtime_ns: 0,
                node_count: 1,
                edge_count: 1,
                indexed_at: 10,
                parse_ms: Some(1),
            });
            states.push(FileIndexState {
                repo: "frontend_standard".to_owned(),
                file_path: frontend.clone(),
                path_id_bytes: Vec::new(),
                content_hash: vec![4, 3, 2, 1],
                size_bytes: 0,
                mtime_ns: 0,
                node_count: 1,
                edge_count: 1,
                indexed_at: 20,
                parse_ms: Some(1),
            });
            query_files.push(("backend_standard".to_owned(), backend));
            query_files.push(("frontend_standard".to_owned(), frontend));
        }
        store.upsert_file_states(&states)?;

        assert_eq!(store.latest_indexed_at_for_files(&query_files)?, 20);
        Ok(())
    }

    #[test]
    fn latest_indexed_at_batches_across_repos() -> Result<(), MetadataStoreError> {
        // Seed 2 repos × 2500 files = 5000 rows with distinct `indexed_at`
        // timestamps so we can verify no cross-repo contamination.
        let (_db_path, store) = open_store("latest-indexed-batches-across-repos")?;

        let mut states = Vec::with_capacity(5_000);
        for index in 0..2_500_usize {
            // repo_a files: indexed_at = 1000 + index (range 1000..3499)
            states.push(FileIndexState {
                repo: "repo_a".to_owned(),
                file_path: format!("src/file_{index}.ts"),
                path_id_bytes: Vec::new(),
                content_hash: vec![1, 2, 3, u8::try_from(index % 256).unwrap_or(0)],
                size_bytes: 0,
                mtime_ns: 0,
                node_count: 1,
                edge_count: 1,
                indexed_at: 1_000 + i64::try_from(index).unwrap_or(0),
                parse_ms: Some(1),
            });
            // repo_b files: indexed_at = 5000 + index (range 5000..7499)
            states.push(FileIndexState {
                repo: "repo_b".to_owned(),
                file_path: format!("src/file_{index}.ts"),
                path_id_bytes: Vec::new(),
                content_hash: vec![9, 8, 7, u8::try_from(index % 256).unwrap_or(0)],
                size_bytes: 0,
                mtime_ns: 0,
                node_count: 2,
                edge_count: 2,
                indexed_at: 5_000 + i64::try_from(index).unwrap_or(0),
                parse_ms: Some(2),
            });
        }
        store.upsert_file_states(&states)?;

        // Build the query with all 5000 (repo, file_path) pairs interleaved.
        let mut query_files: Vec<(String, String)> = Vec::with_capacity(5_000);
        for index in 0..2_500_usize {
            query_files.push(("repo_a".to_owned(), format!("src/file_{index}.ts")));
            query_files.push(("repo_b".to_owned(), format!("src/file_{index}.ts")));
        }

        // The function returns the single maximum across all queried files.
        // repo_b/file_2499.ts has indexed_at = 5000 + 2499 = 7499 — the overall max.
        let max_indexed_at = store.latest_indexed_at_for_files(&query_files)?;
        assert_eq!(max_indexed_at, 7_499);

        // Spot-check that per-file timestamps are stored correctly and that
        // there is no cross-repo contamination. Query each repo individually
        // to isolate their maxima.
        // repo_a max indexed_at = 1000 + 2499 = 3499 (no cross-contamination from repo_b).
        {
            let files: Vec<(String, String)> = (0..2_500_usize)
                .map(|i| ("repo_a".to_owned(), format!("src/file_{i}.ts")))
                .collect();
            assert_eq!(store.latest_indexed_at_for_files(&files)?, 3_499);
        }
        // repo_b max indexed_at = 5000 + 2499 = 7499 (no cross-contamination from repo_a).
        {
            let files: Vec<(String, String)> = (0..2_500_usize)
                .map(|i| ("repo_b".to_owned(), format!("src/file_{i}.ts")))
                .collect();
            assert_eq!(store.latest_indexed_at_for_files(&files)?, 7_499);
        }

        // Spot-check file_0 in each repo returns its own distinct timestamp
        // (1000 for repo_a, 5000 for repo_b), not the other repo's value.
        assert_eq!(
            store.latest_indexed_at_for_files(&[(
                "repo_a".to_owned(),
                "src/file_0.ts".to_owned()
            )])?,
            1_000,
            "repo_a/file_0.ts should return its own indexed_at, not repo_b's"
        );
        assert_eq!(
            store.latest_indexed_at_for_files(&[(
                "repo_b".to_owned(),
                "src/file_0.ts".to_owned()
            )])?,
            5_000,
            "repo_b/file_0.ts should return its own indexed_at, not repo_a's"
        );

        Ok(())
    }

    #[test]
    fn files_requiring_reindex_batches_hash_lookups() -> Result<(), MetadataStoreError> {
        let (_db_path, store) = open_store("files-requiring-reindex")?;
        store.upsert_file_states(&[
            FileIndexState {
                repo: "service-a".to_owned(),
                file_path: "src/a.ts".to_owned(),
                path_id_bytes: Vec::new(),
                content_hash: vec![1, 1, 1],
                size_bytes: 0,
                mtime_ns: 0,
                node_count: 1,
                edge_count: 1,
                indexed_at: 1,
                parse_ms: Some(1),
            },
            FileIndexState {
                repo: "service-a".to_owned(),
                file_path: "src/b.ts".to_owned(),
                path_id_bytes: Vec::new(),
                content_hash: vec![2, 2, 2],
                size_bytes: 0,
                mtime_ns: 0,
                node_count: 1,
                edge_count: 1,
                indexed_at: 1,
                parse_ms: Some(1),
            },
        ])?;

        let required = store.files_requiring_reindex(
            "service-a",
            &[
                ("src/a.ts", &[1, 1, 1]),
                ("src/b.ts", &[9, 9, 9]),
                ("src/c.ts", &[3, 3, 3]),
            ],
        )?;

        assert_eq!(
            required,
            ["src/b.ts".to_owned(), "src/c.ts".to_owned()]
                .into_iter()
                .collect()
        );
        Ok(())
    }

    #[test]
    fn should_reindex_only_when_hash_changes() -> Result<(), MetadataStoreError> {
        let (_db_path, store) = open_store("reindex")?;
        let state = FileIndexState {
            repo: "service-a".to_owned(),
            file_path: "src/lib.rs".to_owned(),
            content_hash: vec![1, 2, 3, 4],
            node_count: 12,
            edge_count: 8,
            indexed_at: 1_713_000_000,
            parse_ms: Some(14),
            ..Default::default()
        };

        assert!(store.should_reindex("service-a", "src/lib.rs", &[1, 2, 3, 4])?);

        store.upsert_file_state(&state)?;

        assert!(!store.should_reindex("service-a", "src/lib.rs", &[1, 2, 3, 4])?);
        assert!(store.should_reindex("service-a", "src/lib.rs", &[9, 9, 9, 9])?);
        assert!(store.should_reindex("service-a", "src/main.rs", &[1, 2, 3, 4])?);

        let long_hash = (0_u8..32).collect::<Vec<_>>();
        store.upsert_file_state(&FileIndexState {
            repo: "service-a".to_owned(),
            file_path: "src/full-hash.rs".to_owned(),
            content_hash: long_hash.clone(),
            ..state.clone()
        })?;
        let stored_len = store.read_connection()?.query_row(
            "SELECT length(content_hash) FROM file_index_state WHERE repo = ?1 AND file_path = ?2",
            params!["service-a", "src/full-hash.rs".as_bytes()],
            |row| row.get::<_, i64>(0),
        )?;
        assert_eq!(stored_len, 16);
        assert!(!store.should_reindex("service-a", "src/full-hash.rs", &long_hash)?);
        let mut changed_prefix = long_hash;
        changed_prefix[0] ^= 0xFF;
        assert!(store.should_reindex("service-a", "src/full-hash.rs", &changed_prefix)?);
        Ok(())
    }

    #[test]
    fn commit_queries_filter_by_repo_and_date_range() -> Result<(), MetadataStoreError> {
        let (_db_path, store) = open_store("commit-range")?;

        for index in 0_i64..1_000 {
            store.insert_commit(&CommitRecord {
                sha: format!("sha-{index:04}"),
                repo: "service-a".to_owned(),
                author_email: format!("author-{}@example.com", index % 7),
                date: index,
                message: format!("commit #{index}"),
                classification: Some(if index % 2 == 0 { "feat" } else { "fix" }.to_owned()),
                files_changed: (index % 5) + 1,
                insertions: index + 10,
                deletions: index / 2,
                has_decision_signal: index % 3 == 0,
                pr_number: Some(index % 100),
            })?;
        }

        for index in 0_i64..25 {
            store.insert_commit(&CommitRecord {
                sha: format!("other-{index:04}"),
                repo: "billing".to_owned(),
                author_email: "billing@example.com".to_owned(),
                date: 200 + index,
                message: format!("billing commit #{index}"),
                classification: None,
                files_changed: 1,
                insertions: 1,
                deletions: 0,
                has_decision_signal: false,
                pr_number: None,
            })?;
        }

        let commits = store.get_commits_by_repo("service-a", 200, 399)?;

        assert_eq!(commits.len(), 200);
        assert_eq!(
            commits.first().map(|commit| commit.sha.as_str()),
            Some("sha-0200")
        );
        assert_eq!(
            commits.last().map(|commit| commit.sha.as_str()),
            Some("sha-0399")
        );
        assert!(commits.iter().all(|commit| commit.repo == "service-a"));
        assert!(
            commits
                .windows(2)
                .all(|pair| pair[0].date <= pair[1].date && pair[0].sha <= pair[1].sha)
        );
        Ok(())
    }

    #[test]
    fn file_analytics_round_trip() -> Result<(), MetadataStoreError> {
        let (_db_path, store) = open_store("analytics")?;
        {
            let connection = store.lock_writer()?;
            connection.execute(
                r"
                INSERT INTO file_analytics (
                    repo, file_path, total_commits, commits_90d, commits_180d, commits_365d,
                    hotspot_score, bus_factor, top_owner_email, top_owner_pct,
                    complexity_trend, last_modified, computed_at
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                ",
                params![
                    "service-a",
                    "src/lib.rs",
                    42_i64,
                    12_i64,
                    20_i64,
                    35_i64,
                    18.5_f64,
                    2_i64,
                    "owner@example.com",
                    0.84_f64,
                    "growing",
                    1_713_000_000_i64,
                    1_713_000_100_i64
                ],
            )?;
        }

        let analytics = store.get_file_analytics("service-a", "src/lib.rs")?;

        assert_eq!(
            analytics,
            Some(FileAnalytics {
                repo: "service-a".to_owned(),
                file_path: "src/lib.rs".to_owned(),
                total_commits: 42,
                commits_90d: 12,
                commits_180d: 20,
                commits_365d: 35,
                hotspot_score: 18.5,
                bus_factor: 2,
                top_owner_email: Some("owner@example.com".to_owned()),
                top_owner_pct: 0.84,
                complexity_trend: Some("growing".to_owned()),
                last_modified: 1_713_000_000,
                computed_at: 1_713_000_100,
            })
        );
        Ok(())
    }

    #[test]
    fn replace_file_analytics_for_repo_rebuilds_repo_slice() -> Result<(), MetadataStoreError> {
        let (_db_path, store) = open_store("replace-file-analytics")?;

        store.replace_file_analytics_for_repo(
            "service-a",
            &[FileAnalytics {
                repo: "service-a".to_owned(),
                file_path: "src/old.rs".to_owned(),
                total_commits: 1,
                commits_90d: 1,
                commits_180d: 1,
                commits_365d: 1,
                hotspot_score: 5.0,
                bus_factor: 0,
                top_owner_email: None,
                top_owner_pct: 0.0,
                complexity_trend: None,
                last_modified: 10,
                computed_at: 20,
            }],
        )?;
        store.replace_file_analytics_for_repo(
            "service-a",
            &[FileAnalytics {
                repo: "service-a".to_owned(),
                file_path: "src/new.rs".to_owned(),
                total_commits: 3,
                commits_90d: 2,
                commits_180d: 3,
                commits_365d: 3,
                hotspot_score: 15.0,
                bus_factor: 0,
                top_owner_email: None,
                top_owner_pct: 0.0,
                complexity_trend: None,
                last_modified: 30,
                computed_at: 40,
            }],
        )?;

        assert_eq!(store.get_file_analytics("service-a", "src/old.rs")?, None);
        assert_eq!(
            store.get_file_analytics("service-a", "src/new.rs")?,
            Some(FileAnalytics {
                repo: "service-a".to_owned(),
                file_path: "src/new.rs".to_owned(),
                total_commits: 3,
                commits_90d: 2,
                commits_180d: 3,
                commits_365d: 3,
                hotspot_score: 15.0,
                bus_factor: 0,
                top_owner_email: None,
                top_owner_pct: 0.0,
                complexity_trend: None,
                last_modified: 30,
                computed_at: 40,
            })
        );

        Ok(())
    }

    #[test]
    fn replace_co_change_pairs_for_repo_round_trips_and_rebuilds_repo_slice()
    -> Result<(), MetadataStoreError> {
        let (_db_path, store) = open_store("co-change-pairs")?;

        store.replace_co_change_pairs_for_repo(
            "service-a",
            &[CoChangePairRecord {
                repo: "service-a".to_owned(),
                file_a: "src/a.rs".to_owned(),
                file_b: "src/b.rs".to_owned(),
                strength: 1.5,
                occurrences: 2,
                last_seen: 100,
            }],
        )?;

        store.replace_co_change_pairs_for_repo(
            "service-a",
            &[CoChangePairRecord {
                repo: "service-a".to_owned(),
                file_a: "src/a.rs".to_owned(),
                file_b: "src/c.rs".to_owned(),
                strength: 2.0,
                occurrences: 3,
                last_seen: 200,
            }],
        )?;

        assert_eq!(
            store.get_co_change_pairs_for_repo("service-a")?,
            vec![CoChangePairRecord {
                repo: "service-a".to_owned(),
                file_a: "src/a.rs".to_owned(),
                file_b: "src/c.rs".to_owned(),
                strength: 2.0,
                occurrences: 3,
                last_seen: 200,
            }]
        );

        Ok(())
    }

    #[test]
    fn readers_can_observe_while_writer_holds_transaction() -> Result<(), MetadataStoreError> {
        let (_db_path, store) = open_store("concurrent-readers")?;
        let store = Arc::new(store);
        store.upsert_file_state(&FileIndexState {
            repo: "service-a".to_owned(),
            file_path: "src/lib.rs".to_owned(),
            content_hash: vec![1, 2, 3, 4],
            node_count: 1,
            edge_count: 1,
            indexed_at: 1,
            parse_ms: Some(1),
            ..Default::default()
        })?;

        let barrier = Arc::new(Barrier::new(2));
        let writer_store = Arc::clone(&store);
        let writer_barrier = Arc::clone(&barrier);
        let writer = thread::spawn(move || -> Result<(), MetadataStoreError> {
            let mut writer = writer_store.lock_writer()?;
            let tx = writer.transaction()?;
            tx.execute(
                "UPDATE file_index_state SET content_hash = ?3 WHERE repo = ?1 AND file_path = ?2",
                params!["service-a", b"src/lib.rs" as &[u8], vec![9_u8, 9, 9, 9]],
            )?;
            writer_barrier.wait();
            thread::sleep(Duration::from_millis(150));
            tx.commit()?;
            Ok(())
        });

        barrier.wait();
        assert!(!store.should_reindex("service-a", "src/lib.rs", &[1, 2, 3, 4])?);
        assert!(store.should_reindex("service-a", "src/lib.rs", &[9, 9, 9, 9])?);

        writer.join().expect("writer thread should finish")?;

        assert!(!store.should_reindex("service-a", "src/lib.rs", &[9, 9, 9, 9])?);
        Ok(())
    }

    #[test]
    fn with_write_txn_runs_on_configured_writer_connection() -> Result<(), MetadataStoreError> {
        // `with_write_txn` must use the store's configured writer connection so
        // callers inherit the expected SQLite pragmas and get back the closure's
        // return value unchanged.
        let (_db_path, store) = open_store("with-write-txn")?;

        let journal_mode: String = store.with_write_txn(|tx| {
            tx.query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
        })?;
        assert_eq!(journal_mode, "wal");

        let row_count: i64 = store.with_write_txn(|tx| {
            tx.execute(
                "INSERT INTO file_index_state(repo, file_path, content_hash, node_count, edge_count, indexed_at, parse_ms) \
                 VALUES (?1, ?2, ?3, 0, 0, 0, NULL)",
                params!["service-a", b"src/a.ts" as &[u8], vec![1_u8]],
            )?;
            tx.query_row(
                "SELECT COUNT(*) FROM file_index_state WHERE repo = 'service-a'",
                [],
                |row| row.get::<_, i64>(0),
            )
        })?;
        assert_eq!(row_count, 1);
        Ok(())
    }

    #[test]
    fn with_write_txn_serializes_concurrent_callers() -> Result<(), MetadataStoreError> {
        // The real value of routing reconcile through the writer mutex is that concurrent
        // callers block on the mutex instead of racing on the `SQLite` file lock. Under the
        // old raw `Connection::open` approach with default pragmas (no busy_timeout),
        // two concurrent writers would hit SQLITE_BUSY. Here we prove both transactions
        // commit successfully and serialize cleanly.
        let (_db_path, store) = open_store("with-write-txn-serialize")?;
        let store = Arc::new(store);

        let barrier = Arc::new(Barrier::new(2));
        let a_store = Arc::clone(&store);
        let a_barrier = Arc::clone(&barrier);
        let handle_a = thread::spawn(move || -> Result<(), MetadataStoreError> {
            a_store.with_write_txn(|tx| {
                tx.execute(
                    "INSERT INTO file_index_state(repo, file_path, content_hash, node_count, edge_count, indexed_at, parse_ms) \
                     VALUES ('svc-a', 'src/a.ts', X'00', 0, 0, 0, NULL)",
                    [],
                )?;
                a_barrier.wait();
                thread::sleep(Duration::from_millis(80));
                Ok(())
            })?;
            Ok(())
        });

        barrier.wait();
        store.with_write_txn(|tx| {
            tx.execute(
                "INSERT INTO file_index_state(repo, file_path, content_hash, node_count, edge_count, indexed_at, parse_ms) \
                 VALUES ('svc-b', 'src/b.ts', X'01', 0, 0, 0, NULL)",
                [],
            )?;
            Ok(())
        })?;

        handle_a.join().expect("writer A should finish")?;

        let reader = store.read_connection()?;
        let count: i64 = reader.query_row(
            "SELECT COUNT(*) FROM file_index_state WHERE repo IN ('svc-a', 'svc-b')",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(count, 2);
        Ok(())
    }

    #[test]
    fn payload_contracts_round_trip_through_metadata_store() -> Result<(), MetadataStoreError> {
        let (_db_path, store) = open_store("payload-contracts")?;
        let target_id = ref_node_id(
            gather_step_core::NodeKind::Topic,
            "__topic__kafka__order.created",
        );
        let symbol_id = ref_node_id(gather_step_core::NodeKind::Function, "symbol::producer");
        let contract_id = ref_node_id(
            gather_step_core::NodeKind::PayloadContract,
            "__payload_contract__sample",
        );
        let record = PayloadContractStoreRecord {
            record: PayloadContractRecord {
                payload_contract_node_id: contract_id,
                contract_target_node_id: target_id,
                contract_target_kind: gather_step_core::NodeKind::Topic,
                contract_target_qualified_name: Some("__topic__kafka__order.created".to_owned()),
                repo: "backend_standard".to_owned(),
                file_path: "src/events.ts".to_owned(),
                source_symbol_node_id: symbol_id,
                line_start: Some(12),
                side: PayloadSide::Producer,
                inference_kind: PayloadInferenceKind::LiteralObject,
                confidence: 950,
                source_type_name: None,
                contract: PayloadContractDoc {
                    content_type: "application/json".to_owned(),
                    schema_format: "normalized_object".to_owned(),
                    side: PayloadSide::Producer,
                    inference_kind: PayloadInferenceKind::LiteralObject,
                    confidence: 950,
                    fields: vec![PayloadField {
                        name: "orderId".to_owned(),
                        type_name: "string".to_owned(),
                        optional: false,
                        confidence: 950,
                    }],
                    source_type_name: None,
                },
            },
        };

        store.replace_payload_contracts_for_files(
            "backend_standard",
            &["src/events.ts".to_owned()],
            std::slice::from_ref(&record),
        )?;

        let loaded = store.payload_contracts_for_query(PayloadContractQuery {
            contract_target_node_id: Some(target_id),
            ..PayloadContractQuery::default()
        })?;
        assert_eq!(loaded, vec![record]);
        Ok(())
    }

    #[test]
    fn file_index_states_by_repo_returns_sorted_rows() -> Result<(), MetadataStoreError> {
        let (_db_path, store) = open_store("file-index-states")?;
        store.upsert_file_states(&[
            FileIndexState {
                repo: "svc".to_owned(),
                file_path: "src/z.ts".to_owned(),
                content_hash: vec![3],
                node_count: 3,
                edge_count: 4,
                indexed_at: 30,
                parse_ms: Some(5),
                ..Default::default()
            },
            FileIndexState {
                repo: "svc".to_owned(),
                file_path: "src/a.ts".to_owned(),
                content_hash: vec![1],
                node_count: 1,
                edge_count: 2,
                indexed_at: 10,
                parse_ms: Some(3),
                ..Default::default()
            },
            FileIndexState {
                repo: "other".to_owned(),
                file_path: "src/skip.ts".to_owned(),
                content_hash: vec![9],
                node_count: 1,
                edge_count: 1,
                indexed_at: 1,
                parse_ms: None,
                ..Default::default()
            },
        ])?;

        let states = store.file_index_states_by_repo("svc")?;
        assert_eq!(states.len(), 2);
        assert_eq!(states[0].file_path, "src/a.ts");
        assert_eq!(states[1].file_path, "src/z.ts");
        Ok(())
    }

    #[test]
    fn answer_cache_round_trips_and_tracks_hits() -> Result<(), MetadataStoreError> {
        let (_db_path, store) = open_store("answer-cache")?;
        store.put_cached_answer("pack:key", br#"{"ok":true}"#, 100, 60)?;

        let cached = store
            .get_cached_answer("pack:key", 110)?
            .expect("cached payload should be present");
        assert_eq!(cached, br#"{"ok":true}"#);

        // Hit counts are coalesced in memory and flushed in a batch.
        // Force a flush so the raw SQL query below sees the updated value.
        store.flush_hit_counts();

        let connection = store.read_connection()?;
        let hit_count: i64 = connection.query_row(
            "SELECT hit_count FROM answer_cache WHERE cache_key = ?1",
            params!["pack:key"],
            |row| row.get(0),
        )?;
        assert_eq!(hit_count, 1);
        assert_eq!(store.latest_indexed_at(None)?, 0);
        Ok(())
    }

    #[test]
    fn context_pack_round_trips_and_invalidates_by_file() -> Result<(), MetadataStoreError> {
        let (_db_path, store) = open_store("context-pack")?;
        let record = ContextPackRecord {
            pack_key: "pack:key".to_owned(),
            mode: "planning".to_owned(),
            target: "listOrders".to_owned(),
            generation: 42,
            response: br#"{"data":{"found":true}}"#.to_vec(),
            created_at: 100,
            last_read_at: 100,
            byte_size: 23,
            hit_count: 0,
        };
        store.put_context_pack(
            &record,
            &[
                ("frontend_standard".to_owned(), "src/orders.ts".to_owned()),
                (
                    "backend_standard".to_owned(),
                    "src/controller.ts".to_owned(),
                ),
            ],
        )?;

        let loaded = store
            .get_context_pack("pack:key")?
            .expect("stored pack should load");
        assert_eq!(loaded, record);

        store.touch_context_pack("pack:key", 111)?;
        let touched = store
            .get_context_pack("pack:key")?
            .expect("touched pack should still load");
        assert_eq!(touched.hit_count, 1);
        assert_eq!(touched.last_read_at, 111);

        let removed = store.invalidate_context_packs_for_files(
            "frontend_standard",
            &["src/orders.ts".to_owned()],
        )?;
        assert_eq!(removed, 1);
        assert!(store.get_context_pack("pack:key")?.is_none());
        Ok(())
    }

    #[test]
    fn clear_context_packs_removes_cached_packs_and_file_deps() -> Result<(), MetadataStoreError> {
        let (_db_path, store) = open_store("context-pack-clear")?;
        let record = ContextPackRecord {
            pack_key: "pack:key".to_owned(),
            mode: "planning".to_owned(),
            target: "listOrders".to_owned(),
            generation: 42,
            response: br#"{"data":{"found":true}}"#.to_vec(),
            created_at: 100,
            last_read_at: 100,
            byte_size: 23,
            hit_count: 0,
        };
        store.put_context_pack(
            &record,
            &[("frontend_standard".to_owned(), "src/orders.ts".to_owned())],
        )?;

        assert_eq!(store.clear_context_packs()?, 1);
        assert!(store.get_context_pack("pack:key")?.is_none());
        assert!(store.context_pack_files_for_key("pack:key")?.is_empty());
        assert_eq!(store.clear_context_packs()?, 0);
        Ok(())
    }

    #[test]
    fn put_context_pack_prunes_stale_packs_and_file_deps() -> Result<(), MetadataStoreError> {
        let (_db_path, store) = open_store("context-pack-age-prune")?;
        let stale = ContextPackRecord {
            pack_key: "pack:stale".to_owned(),
            mode: "planning".to_owned(),
            target: "staleTarget".to_owned(),
            generation: 1,
            response: br#"{"stale":true}"#.to_vec(),
            created_at: 1,
            last_read_at: 1,
            byte_size: 14,
            hit_count: 0,
        };
        let fresh = ContextPackRecord {
            pack_key: "pack:fresh".to_owned(),
            mode: "planning".to_owned(),
            target: "freshTarget".to_owned(),
            generation: 2,
            response: br#"{"fresh":true}"#.to_vec(),
            created_at: CONTEXT_PACK_RETENTION_SECONDS + 10,
            last_read_at: CONTEXT_PACK_RETENTION_SECONDS + 10,
            byte_size: 14,
            hit_count: 0,
        };
        store.put_context_pack(
            &stale,
            &[("frontend_standard".to_owned(), "src/stale.ts".to_owned())],
        )?;

        store.put_context_pack(
            &fresh,
            &[("frontend_standard".to_owned(), "src/fresh.ts".to_owned())],
        )?;

        assert!(store.get_context_pack("pack:stale")?.is_none());
        assert!(store.context_pack_files_for_key("pack:stale")?.is_empty());
        assert_eq!(store.get_context_pack("pack:fresh")?, Some(fresh));
        assert_eq!(
            store.context_pack_files_for_key("pack:fresh")?,
            vec![("frontend_standard".to_owned(), "src/fresh.ts".to_owned())]
        );
        Ok(())
    }

    #[test]
    fn context_pack_invalidates_across_repo_targets() -> Result<(), MetadataStoreError> {
        let (_db_path, store) = open_store("context-pack-cross-repo")?;
        store.put_context_pack(
            &ContextPackRecord {
                pack_key: "pack:cross".to_owned(),
                mode: "planning".to_owned(),
                target: "callOrderApi".to_owned(),
                generation: 99,
                response: br#"{"ok":true}"#.to_vec(),
                created_at: 100,
                last_read_at: 100,
                byte_size: 11,
                hit_count: 0,
            },
            &[("frontend_standard".to_owned(), "src/api.ts".to_owned())],
        )?;

        let removed = store.invalidate_context_packs_for_targets(&[(
            "frontend_standard".to_owned(),
            "src/api.ts".to_owned(),
        )])?;
        assert_eq!(removed, 1);
        assert!(store.get_context_pack("pack:cross")?.is_none());
        Ok(())
    }

    #[test]
    fn context_pack_target_invalidation_preserves_unrelated_packs() -> Result<(), MetadataStoreError>
    {
        let (_db_path, store) = open_store("context-pack-preserve-unrelated")?;
        let changed = ContextPackRecord {
            pack_key: "pack:changed".to_owned(),
            mode: "planning".to_owned(),
            target: "changedTarget".to_owned(),
            generation: 99,
            response: br#"{"changed":true}"#.to_vec(),
            created_at: 100,
            last_read_at: 100,
            byte_size: 16,
            hit_count: 0,
        };
        let unrelated = ContextPackRecord {
            pack_key: "pack:unrelated".to_owned(),
            mode: "planning".to_owned(),
            target: "unrelatedTarget".to_owned(),
            generation: 99,
            response: br#"{"unrelated":true}"#.to_vec(),
            created_at: 100,
            last_read_at: 100,
            byte_size: 18,
            hit_count: 0,
        };
        store.put_context_pack(
            &changed,
            &[("frontend_standard".to_owned(), "src/api.ts".to_owned())],
        )?;
        store.put_context_pack(
            &unrelated,
            &[("frontend_standard".to_owned(), "src/other.ts".to_owned())],
        )?;

        let removed = store.invalidate_context_packs_for_targets(&[(
            "frontend_standard".to_owned(),
            "src/api.ts".to_owned(),
        )])?;

        assert_eq!(removed, 1);
        assert!(store.get_context_pack("pack:changed")?.is_none());
        assert!(store.get_context_pack("pack:unrelated")?.is_some());
        Ok(())
    }

    #[test]
    fn reverse_dependents_returns_same_repo_sources() -> Result<(), MetadataStoreError> {
        let (_db_path, store) = open_store("reverse-dependents")?;
        store.with_write_txn(|tx| {
            tx.execute(
                "INSERT INTO file_dependencies(source_repo, source_path, target_repo, target_path, edge_count)
                 VALUES (?1, ?2, ?3, ?4, 1)",
                params!["svc", b"src/caller.ts" as &[u8], "svc", b"src/helper.ts" as &[u8]],
            )?;
            tx.execute(
                "INSERT INTO file_dependencies(source_repo, source_path, target_repo, target_path, edge_count)
                 VALUES (?1, ?2, ?3, ?4, 1)",
                params!["svc", b"src/other.ts" as &[u8], "svc", b"src/helper.ts" as &[u8]],
            )?;
            tx.execute(
                "INSERT INTO file_dependencies(source_repo, source_path, target_repo, target_path, edge_count)
                 VALUES (?1, ?2, ?3, ?4, 1)",
                params!["consumer", b"src/caller.ts" as &[u8], "svc", b"src/helper.ts" as &[u8]],
            )?;
            Ok(())
        })?;

        let dependents = store.reverse_dependents("svc", "src/helper.ts")?;
        assert_eq!(
            dependents,
            vec!["src/caller.ts".to_owned(), "src/other.ts".to_owned()]
        );
        Ok(())
    }

    #[test]
    fn delete_file_state_and_dependencies_removes_both_directions() -> Result<(), MetadataStoreError>
    {
        let (_db_path, store) = open_store("delete-file-state-and-dependencies")?;
        store.upsert_file_state(&FileIndexState {
            repo: "svc".to_owned(),
            file_path: "src/helper.ts".to_owned(),
            content_hash: vec![1, 2, 3],
            node_count: 2,
            edge_count: 1,
            indexed_at: 100,
            parse_ms: Some(8),
            ..Default::default()
        })?;
        store.with_write_txn(|tx| {
            tx.execute(
                "INSERT INTO file_dependencies(source_repo, source_path, target_repo, target_path, edge_count)
                 VALUES (?1, ?2, ?3, ?4, 1)",
                params!["svc", b"src/caller.ts" as &[u8], "svc", b"src/helper.ts" as &[u8]],
            )?;
            tx.execute(
                "INSERT INTO file_dependencies(source_repo, source_path, target_repo, target_path, edge_count)
                 VALUES (?1, ?2, ?3, ?4, 1)",
                params!["svc", b"src/helper.ts" as &[u8], "svc", b"src/dependency.ts" as &[u8]],
            )?;
            Ok(())
        })?;

        store.delete_file_state_and_dependencies("svc", "src/helper.ts")?;

        let states = store.file_index_states_by_repo("svc")?;
        assert!(states.is_empty());
        let connection = store.read_connection()?;
        let dependency_rows: i64 =
            connection.query_row("SELECT COUNT(*) FROM file_dependencies", [], |row| {
                row.get(0)
            })?;
        assert_eq!(dependency_rows, 0);
        Ok(())
    }

    #[test]
    fn polyrepo_commits_pk_allows_same_sha_in_distinct_repos() -> Result<(), MetadataStoreError> {
        let (_db_path, store) = open_store("polyrepo-commits")?;

        let shared_sha = "deadbeef".to_owned();
        store.insert_commit(&CommitRecord {
            sha: shared_sha.clone(),
            repo: "service-a".to_owned(),
            author_email: "alice@example.com".to_owned(),
            date: 1,
            message: "feat: x".to_owned(),
            classification: Some("feat".to_owned()),
            files_changed: 1,
            insertions: 1,
            deletions: 0,
            has_decision_signal: false,
            pr_number: None,
        })?;
        // A different repo using the same SHA must coexist instead of overwriting.
        store.insert_commit(&CommitRecord {
            sha: shared_sha.clone(),
            repo: "billing".to_owned(),
            author_email: "bob@example.com".to_owned(),
            date: 2,
            message: "fix: y".to_owned(),
            classification: Some("fix".to_owned()),
            files_changed: 2,
            insertions: 5,
            deletions: 1,
            has_decision_signal: true,
            pr_number: Some(7),
        })?;

        let order_commits = store.get_commits_by_repo("service-a", 0, 10)?;
        let billing_commits = store.get_commits_by_repo("billing", 0, 10)?;
        assert_eq!(order_commits.len(), 1);
        assert_eq!(billing_commits.len(), 1);
        assert_eq!(order_commits[0].author_email, "alice@example.com");
        assert_eq!(billing_commits[0].author_email, "bob@example.com");

        Ok(())
    }

    #[test]
    fn commit_file_deltas_round_trip_and_cascade_on_repo_delete() -> Result<(), MetadataStoreError>
    {
        let (_db_path, store) = open_store("commit-deltas")?;

        let commit = CommitRecord {
            sha: "c0ffee".to_owned(),
            repo: "service-a".to_owned(),
            author_email: "alice@example.com".to_owned(),
            date: 100,
            message: "refactor: shuffle".to_owned(),
            classification: Some("refactor".to_owned()),
            files_changed: 3,
            insertions: 12,
            deletions: 4,
            has_decision_signal: false,
            pr_number: None,
        };
        store.insert_commit(&commit)?;

        let deltas = vec![
            CommitFileDeltaRecord {
                repo: "service-a".to_owned(),
                sha: "c0ffee".to_owned(),
                file_path: "src/lib.rs".to_owned(),
                change_kind: CommitFileChangeKind::Modified,
                insertions: Some(8),
                deletions: Some(2),
                old_path: None,
            },
            CommitFileDeltaRecord {
                repo: "service-a".to_owned(),
                sha: "c0ffee".to_owned(),
                file_path: "src/new_module.rs".to_owned(),
                change_kind: CommitFileChangeKind::Added,
                insertions: Some(4),
                deletions: Some(0),
                old_path: None,
            },
            CommitFileDeltaRecord {
                repo: "service-a".to_owned(),
                sha: "c0ffee".to_owned(),
                file_path: "src/renamed.rs".to_owned(),
                change_kind: CommitFileChangeKind::Renamed,
                insertions: Some(0),
                deletions: Some(0),
                old_path: Some("src/old_name.rs".to_owned()),
            },
            CommitFileDeltaRecord {
                repo: "service-a".to_owned(),
                sha: "c0ffee".to_owned(),
                file_path: "assets/binary.png".to_owned(),
                change_kind: CommitFileChangeKind::Added,
                // Binary diffs intentionally have NULL line counts so analytics can
                // skip them rather than miscount them as zero-line edits.
                insertions: None,
                deletions: None,
                old_path: None,
            },
        ];
        store.upsert_commit_file_deltas(&deltas)?;

        let read_back = store.get_commit_file_deltas("service-a", "c0ffee")?;
        assert_eq!(read_back.len(), 4);
        let mut expected = deltas.clone();
        expected.sort_by(|a, b| a.file_path.cmp(&b.file_path));
        assert_eq!(read_back, expected);

        // Upsert path: re-insert one delta with a new line count, confirm it overwrites.
        let mut updated = deltas[0].clone();
        updated.insertions = Some(99);
        store.upsert_commit_file_delta(&updated)?;
        let after_update = store.get_commit_file_deltas("service-a", "c0ffee")?;
        let updated_row = after_update
            .iter()
            .find(|delta| delta.file_path == "src/lib.rs")
            .expect("updated delta should still be present");
        assert_eq!(updated_row.insertions, Some(99));

        // CASCADE: deleting the parent commit row removes its deltas via the FK.
        store.delete_commits_for_repo("service-a")?;
        let after_cascade = store.get_commit_file_deltas("service-a", "c0ffee")?;
        assert!(
            after_cascade.is_empty(),
            "deltas should cascade with the parent commit row"
        );

        Ok(())
    }

    #[test]
    fn get_commit_file_deltas_for_repo_returns_repo_slice() -> Result<(), MetadataStoreError> {
        let (_db_path, store) = open_store("repo-deltas")?;

        for commit in [
            CommitRecord {
                sha: "a".to_owned(),
                repo: "service-a".to_owned(),
                author_email: "alice@example.com".to_owned(),
                date: 1,
                message: "feat: a".to_owned(),
                classification: Some("feat".to_owned()),
                files_changed: 1,
                insertions: 1,
                deletions: 0,
                has_decision_signal: false,
                pr_number: None,
            },
            CommitRecord {
                sha: "b".to_owned(),
                repo: "billing".to_owned(),
                author_email: "bob@example.com".to_owned(),
                date: 2,
                message: "fix: b".to_owned(),
                classification: Some("fix".to_owned()),
                files_changed: 1,
                insertions: 1,
                deletions: 0,
                has_decision_signal: false,
                pr_number: None,
            },
        ] {
            store.insert_commit(&commit)?;
        }
        store.upsert_commit_file_deltas(&[
            CommitFileDeltaRecord {
                repo: "service-a".to_owned(),
                sha: "a".to_owned(),
                file_path: "src/a.rs".to_owned(),
                change_kind: CommitFileChangeKind::Modified,
                insertions: Some(1),
                deletions: Some(0),
                old_path: None,
            },
            CommitFileDeltaRecord {
                repo: "billing".to_owned(),
                sha: "b".to_owned(),
                file_path: "src/b.rs".to_owned(),
                change_kind: CommitFileChangeKind::Modified,
                insertions: Some(1),
                deletions: Some(0),
                old_path: None,
            },
        ])?;

        assert_eq!(
            store.get_commit_file_deltas_for_repo("service-a")?,
            vec![CommitFileDeltaRecord {
                repo: "service-a".to_owned(),
                sha: "a".to_owned(),
                file_path: "src/a.rs".to_owned(),
                change_kind: CommitFileChangeKind::Modified,
                insertions: Some(1),
                deletions: Some(0),
                old_path: None,
            }]
        );

        Ok(())
    }

    #[test]
    fn get_history_for_file_with_renames_walks_chain_and_filters_repo()
    -> Result<(), MetadataStoreError> {
        let (_db_path, store) = open_store("file-history")?;

        // Three commits: original add, a rename, then a modification of the
        // renamed path. A second repo with the same path validates that the
        // query stays repo-scoped.
        let commits = [
            CommitRecord {
                sha: "c1".to_owned(),
                repo: "service-a".to_owned(),
                author_email: "alice@example.com".to_owned(),
                date: 100,
                message: "feat: add original".to_owned(),
                classification: Some("feat".to_owned()),
                files_changed: 1,
                insertions: 10,
                deletions: 0,
                has_decision_signal: false,
                pr_number: None,
            },
            CommitRecord {
                sha: "c2".to_owned(),
                repo: "service-a".to_owned(),
                author_email: "bob@example.com".to_owned(),
                date: 200,
                message: "refactor: rename".to_owned(),
                classification: Some("refactor".to_owned()),
                files_changed: 1,
                insertions: 0,
                deletions: 0,
                has_decision_signal: false,
                pr_number: None,
            },
            CommitRecord {
                sha: "c3".to_owned(),
                repo: "service-a".to_owned(),
                author_email: "alice@example.com".to_owned(),
                date: 300,
                message: "fix: tweak".to_owned(),
                classification: Some("fix".to_owned()),
                files_changed: 1,
                insertions: 5,
                deletions: 2,
                has_decision_signal: false,
                pr_number: None,
            },
            // Same path in a different repo — must NOT show up in the
            // service-a query result.
            CommitRecord {
                sha: "x1".to_owned(),
                repo: "billing".to_owned(),
                author_email: "carol@example.com".to_owned(),
                date: 50,
                message: "feat: noise".to_owned(),
                classification: Some("feat".to_owned()),
                files_changed: 1,
                insertions: 1,
                deletions: 0,
                has_decision_signal: false,
                pr_number: None,
            },
        ];
        for commit in commits {
            store.insert_commit(&commit)?;
        }
        store.upsert_commit_file_deltas(&[
            CommitFileDeltaRecord {
                repo: "service-a".to_owned(),
                sha: "c1".to_owned(),
                file_path: "src/old_name.rs".to_owned(),
                change_kind: CommitFileChangeKind::Added,
                insertions: Some(10),
                deletions: Some(0),
                old_path: None,
            },
            CommitFileDeltaRecord {
                repo: "service-a".to_owned(),
                sha: "c2".to_owned(),
                file_path: "src/new_name.rs".to_owned(),
                change_kind: CommitFileChangeKind::Renamed,
                insertions: Some(0),
                deletions: Some(0),
                old_path: Some("src/old_name.rs".to_owned()),
            },
            CommitFileDeltaRecord {
                repo: "service-a".to_owned(),
                sha: "c3".to_owned(),
                file_path: "src/new_name.rs".to_owned(),
                change_kind: CommitFileChangeKind::Modified,
                insertions: Some(5),
                deletions: Some(2),
                old_path: None,
            },
            CommitFileDeltaRecord {
                repo: "billing".to_owned(),
                sha: "x1".to_owned(),
                file_path: "src/new_name.rs".to_owned(),
                change_kind: CommitFileChangeKind::Added,
                insertions: Some(1),
                deletions: Some(0),
                old_path: None,
            },
        ])?;

        let (commits, deltas) =
            store.get_history_for_file_with_renames("service-a", "src/new_name.rs")?;

        // Should pick up all three service-a commits via the rename chain
        // (the original add of `old_name`, the rename, and the later edit
        // of `new_name`). The billing repo's noise commit must be excluded.
        let shas = commits.iter().map(|c| c.sha.as_str()).collect::<Vec<_>>();
        assert_eq!(shas, vec!["c1", "c2", "c3"]); // sorted by date ASC
        assert_eq!(deltas.len(), 3);
        assert!(
            !deltas.iter().any(|delta| delta.repo == "billing"),
            "cross-repo path collision must not leak into the result",
        );

        // Asking for the original (pre-rename) path should also return the
        // same chain because `file_path` is in the historical set.
        let (commits_via_old, _) =
            store.get_history_for_file_with_renames("service-a", "src/old_name.rs")?;
        let old_shas = commits_via_old
            .iter()
            .map(|c| c.sha.as_str())
            .collect::<Vec<_>>();
        // From `old_name`'s perspective only c1 directly references it (the
        // chain walks *backwards* from the queried path). c2 names c1's
        // path as old_path so it's also captured; c3 only touches the new
        // name and is not reachable from the old path's perspective.
        assert!(old_shas.contains(&"c1"));

        // A path with no history at all returns empty vectors.
        let (no_commits, no_deltas) =
            store.get_history_for_file_with_renames("service-a", "does/not/exist.rs")?;
        assert!(no_commits.is_empty());
        assert!(no_deltas.is_empty());

        Ok(())
    }

    #[test]
    fn get_history_for_file_with_renames_dedups_and_preserves_global_order_across_chunks()
    -> Result<(), MetadataStoreError> {
        let (_db_path, store) = open_store("file-history-chunks")?;
        let mut deltas = Vec::new();

        for index in 0..301 {
            let sha = format!("seed-{index:03}");
            store.insert_commit(&CommitRecord {
                sha: sha.clone(),
                repo: "service-a".to_owned(),
                author_email: "seed@example.com".to_owned(),
                date: i64::from(index),
                message: "seed".to_owned(),
                classification: Some("test".to_owned()),
                files_changed: 1,
                insertions: 0,
                deletions: 0,
                has_decision_signal: false,
                pr_number: None,
            })?;
            deltas.push(CommitFileDeltaRecord {
                repo: "service-a".to_owned(),
                sha,
                file_path: format!("src/path-{index:03}.ts"),
                change_kind: CommitFileChangeKind::Added,
                insertions: Some(0),
                deletions: Some(0),
                old_path: None,
            });
        }

        store.insert_commit(&CommitRecord {
            sha: "rename".to_owned(),
            repo: "service-a".to_owned(),
            author_email: "rename@example.com".to_owned(),
            date: 10_000,
            message: "rename".to_owned(),
            classification: Some("refactor".to_owned()),
            files_changed: 1,
            insertions: 0,
            deletions: 0,
            has_decision_signal: false,
            pr_number: None,
        })?;
        deltas.push(CommitFileDeltaRecord {
            repo: "service-a".to_owned(),
            sha: "rename".to_owned(),
            file_path: "src/final.ts".to_owned(),
            change_kind: CommitFileChangeKind::Renamed,
            insertions: Some(0),
            deletions: Some(0),
            old_path: Some("src/path-300.ts".to_owned()),
        });

        store.upsert_commit_file_deltas(&deltas)?;

        let (commits, deltas) =
            store.get_history_for_file_with_renames("service-a", "src/final.ts")?;

        assert_eq!(
            commits.last().map(|commit| commit.sha.as_str()),
            Some("rename")
        );
        assert_eq!(
            deltas.iter().filter(|delta| delta.sha == "rename").count(),
            1,
            "rename rows should not be duplicated when old/new paths span different chunks"
        );
        assert!(commits.windows(2).all(|window| {
            window[0].date < window[1].date
                || (window[0].date == window[1].date && window[0].sha <= window[1].sha)
        }));
        Ok(())
    }

    /// Verifies the two-phase cache-validation contract used by the
    /// identity-phase cache lookup in the pack tool.
    ///
    /// Scenario: a pack is stored with a generation derived from two file
    /// dependencies.  When both files are unchanged the stored generation
    /// still matches the re-derived one, so the cache hit is valid.  After
    /// one file is re-indexed the generation advances and the cached entry is
    /// stale — the caller must recompute the pack.
    #[test]
    fn context_pack_generation_key_validates_file_deps() -> Result<(), MetadataStoreError> {
        let (_db_path, store) = open_store("pack-gen-key")?;

        // Seed two files into file_index_state with known indexed_at values.
        store.upsert_file_state(&FileIndexState {
            repo: "svc".to_owned(),
            file_path: "src/a.ts".to_owned(),
            content_hash: vec![1],
            indexed_at: 1_000,
            ..FileIndexState::default()
        })?;
        store.upsert_file_state(&FileIndexState {
            repo: "svc".to_owned(),
            file_path: "src/b.ts".to_owned(),
            content_hash: vec![2],
            indexed_at: 2_000,
            ..FileIndexState::default()
        })?;

        // Store a context pack whose generation was computed from those two
        // files (MAX(indexed_at) = 2_000).
        let dep_files = vec![
            ("svc".to_owned(), "src/a.ts".to_owned()),
            ("svc".to_owned(), "src/b.ts".to_owned()),
        ];
        let stored_generation = store.latest_indexed_at_for_files(&dep_files)?;
        assert_eq!(stored_generation, 2_000);

        store.put_context_pack(
            &ContextPackRecord {
                pack_key: "pack:gen-key-test".to_owned(),
                mode: "planning".to_owned(),
                target: "MyService".to_owned(),
                generation: stored_generation,
                response: br#"{"data":{"found":true}}"#.to_vec(),
                created_at: 100,
                last_read_at: 100,
                byte_size: 23,
                hit_count: 0,
            },
            &dep_files,
        )?;

        // Identity lookup: retrieve the stored pack and its dep files.
        let record = store
            .get_context_pack("pack:gen-key-test")?
            .expect("pack must be stored");
        let loaded_deps = store.context_pack_files_for_key("pack:gen-key-test")?;
        let current_generation = store.latest_indexed_at_for_files(&loaded_deps)?;

        // Both files are unchanged — generation matches; cache hit is valid.
        assert_eq!(
            current_generation, record.generation,
            "unchanged files: cache should be valid"
        );

        // Simulate re-indexing src/b.ts (content changed, indexed_at advances).
        store.upsert_file_state(&FileIndexState {
            repo: "svc".to_owned(),
            file_path: "src/b.ts".to_owned(),
            content_hash: vec![99],
            indexed_at: 3_000, // later than stored generation 2_000
            ..FileIndexState::default()
        })?;

        // Re-derive the generation from the same dep-file list.
        let stale_check = store.latest_indexed_at_for_files(&loaded_deps)?;
        assert_ne!(
            stale_check, record.generation,
            "after re-index the generation must change so the cache is considered stale"
        );
        assert_eq!(stale_check, 3_000);

        Ok(())
    }

    /// `context_pack_files_for_key` returns an empty Vec for unknown pack keys.
    #[test]
    fn context_pack_files_for_key_returns_empty_for_unknown_key() -> Result<(), MetadataStoreError>
    {
        let (_db_path, store) = open_store("pack-files-unknown")?;
        let files = store.context_pack_files_for_key("no-such-pack")?;
        assert!(files.is_empty());
        Ok(())
    }

    /// Verifies that `ResolutionInput` values survive a full bitcode
    /// encode → store → load → decode round-trip through
    /// `unresolved_call_candidates` with identical logical content.
    #[test]
    fn unresolved_call_bitcode_round_trips_resolution_input() -> Result<(), MetadataStoreError> {
        use std::path::PathBuf;

        use gather_step_core::{NodeKind, node_id};
        use gather_step_parser::resolve::{CallSite, ImportBinding, ResolutionInput};

        let (_db_path, store) = open_store("bitcode-round-trip")?;
        let repo = "svc-round-trip";

        let file_path = PathBuf::from("src/service.ts");
        let file_node = node_id(repo, "src/service.ts", NodeKind::File, "src/service.ts");
        let owner_id = node_id(repo, "src/service.ts", NodeKind::Function, "execute");

        let input = ResolutionInput {
            file_node,
            file_path: file_path.clone(),
            import_bindings: vec![ImportBinding {
                local_name: "HttpService".to_owned(),
                imported_name: Some("HttpService".to_owned()),
                source: "@nestjs/axios".to_owned(),
                resolved_path: None,
                is_default: false,
                is_namespace: false,
                is_type_only: false,
            }],
            call_sites: vec![CallSite {
                owner_id,
                owner_file: file_node,
                source_path: file_path.clone(),
                callee_name: "httpService.get".to_owned(),
                callee_qualified_hint: Some("HttpService.get".to_owned()),
                span: None,
            }],
        };

        let path_id = gather_step_core::PathId::from_path(&file_path)
            .as_bytes()
            .to_vec();
        store.replace_unresolved_resolution_inputs_for_files(
            repo,
            &[path_id],
            std::slice::from_ref(&input),
        )?;

        let loaded = store.unresolved_resolution_inputs_by_repo(repo)?;
        assert_eq!(loaded.len(), 1, "exactly one row should be stored");

        let loaded_input = &loaded[0];
        assert_eq!(loaded_input.file_node, input.file_node);
        assert_eq!(loaded_input.file_path, input.file_path);
        assert_eq!(
            loaded_input.import_bindings.len(),
            input.import_bindings.len()
        );
        assert_eq!(
            loaded_input.import_bindings[0].local_name,
            input.import_bindings[0].local_name
        );
        assert_eq!(loaded_input.call_sites.len(), input.call_sites.len());
        assert_eq!(
            loaded_input.call_sites[0].callee_name,
            input.call_sites[0].callee_name
        );
        assert_eq!(
            loaded_input.call_sites[0].callee_qualified_hint,
            input.call_sites[0].callee_qualified_hint
        );

        Ok(())
    }
}
