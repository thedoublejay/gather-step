use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rand::RngExt as _;
use regex::Regex;
use rusqlite::{Connection, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const TELEMETRY_DB_NAME: &str = "telemetry.db";
pub const TELEMETRY_SCHEMA_VERSION: i64 = 3;
const TELEMETRY_IDENTITY_KEY_NAME: &str = "telemetry-identity.key";

const MAX_RUN_ROWS: i64 = 10_000;
const RETENTION_DAYS: i64 = 90;
/// A `running` row older than this is assumed to belong to a process that died
/// without writing its finish row (crash, `kill -9`, power loss) and is
/// finalized as `abandoned` so the dashboard stops counting it as in-flight.
const STALE_RUNNING_THRESHOLD_MS: i64 = 6 * 60 * 60 * 1000;
const BUSY_RETRY_ATTEMPTS: u32 = 8;
const BUSY_RETRY_BASE_DELAY: Duration = Duration::from_millis(25);
/// Total wall-clock budget for busy retries: telemetry is best-effort, so a
/// contended database fails open instead of stalling the user's command.
/// 2s rides out a full concurrent-CLI burst (500ms loses runs under the
/// 32-process concurrency test) while bounding the worst-case stall at
/// roughly budget + one in-flight `busy_timeout`.
const BUSY_RETRY_BUDGET: Duration = Duration::from_secs(2);
const IDENTITY_KEY_READ_RETRIES: u32 = 20;
const IDENTITY_KEY_READ_RETRY_DELAY: Duration = Duration::from_millis(10);
const PROCESS_START_TOKEN_READ_RETRIES: u32 = 2;
const PROCESS_START_TOKEN_READ_RETRY_DELAY: Duration = Duration::from_millis(10);

const SCHEMA: &str = r"
CREATE TABLE IF NOT EXISTS run_log (
    run_id          TEXT PRIMARY KEY,
    started_at_ms   INTEGER NOT NULL,
    ended_at_ms     INTEGER,
    command         TEXT NOT NULL,
    workspace_hash  TEXT NOT NULL,
    cli_version     TEXT NOT NULL,
    schema_versions TEXT NOT NULL,
    exit_status     TEXT NOT NULL,
    duration_ms     INTEGER,
    peak_rss_bytes  INTEGER,
    repo_count      INTEGER,
    files_parsed    INTEGER,
    nodes_created   INTEGER,
    warn_count      INTEGER NOT NULL DEFAULT 0,
    error_count     INTEGER NOT NULL DEFAULT 0,
    recovery_event  INTEGER NOT NULL DEFAULT 0,
    extra_json      TEXT,
    result_count       INTEGER,
    graph_availability TEXT,
    build_provenance   TEXT,
    binary_path        TEXT,
    build_sha          TEXT,
    process_id         INTEGER,
    process_start_token TEXT,
    heartbeat_at_ms    INTEGER
);
CREATE INDEX IF NOT EXISTS idx_run_log_started_at ON run_log(started_at_ms DESC);
CREATE INDEX IF NOT EXISTS idx_run_log_workspace_started
    ON run_log(workspace_hash, started_at_ms DESC);

CREATE TABLE IF NOT EXISTS run_errors (
    event_id        TEXT PRIMARY KEY,
    run_id          TEXT NOT NULL,
    occurred_at_ms  INTEGER NOT NULL,
    level           TEXT NOT NULL,
    category        TEXT NOT NULL,
    message_hash    TEXT NOT NULL,
    message_excerpt TEXT,
    context_json    TEXT,
    FOREIGN KEY(run_id) REFERENCES run_log(run_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_run_errors_run_id ON run_errors(run_id);
CREATE INDEX IF NOT EXISTS idx_run_errors_occurred_at ON run_errors(occurred_at_ms DESC);
";

#[derive(Debug, Error)]
pub enum TelemetryError {
    #[error("failed to create telemetry parent directory {path}: {source}")]
    CreateParent {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read telemetry identity key {path}: {source}")]
    ReadIdentityKey {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write telemetry identity key {path}: {source}")]
    WriteIdentityKey {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("telemetry identity key {path} has invalid length {length}; expected 32 bytes")]
    InvalidIdentityKey { path: PathBuf, length: usize },
    #[error("sqlite telemetry error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("failed to serialize telemetry JSON: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error(
        "telemetry schema version {stored} is newer than this binary supports ({supported}); upgrade gather-step"
    )]
    UnsupportedSchemaVersion { stored: i64, supported: i64 },
}

#[derive(Clone, Debug)]
pub struct TelemetryStore {
    path: PathBuf,
    identity_key: [u8; 32],
    busy_retries: Arc<AtomicU32>,
}

#[derive(Clone, Debug)]
pub struct TelemetryRun {
    pub run_id: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryResultKind {
    AgentTraceNodes,
    ConsumerRepos,
    DependencyEdges,
    EventLinks,
    ImpactedFiles,
    OrphanTargets,
    SearchHits,
    TraceNodes,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct TelemetryExtra {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_kind: Option<TelemetryResultKind>,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub graph_open_retries: u32,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub telemetry_busy_retries: u32,
}

impl TelemetryExtra {
    fn is_empty(&self) -> bool {
        self.result_kind.is_none()
            && self.graph_open_retries == 0
            && self.telemetry_busy_retries == 0
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct TelemetryCommandResult {
    pub kind: TelemetryResultKind,
    pub count: i64,
}

#[derive(Clone, Debug, Default)]
pub struct TelemetryRunFinish {
    pub exit_status: String,
    pub peak_rss_bytes: Option<u64>,
    pub repo_count: Option<i64>,
    pub files_parsed: Option<i64>,
    pub nodes_created: Option<i64>,
    pub warn_count: u32,
    pub error_count: u32,
    pub recovery_event: bool,
    pub extra_json: Option<TelemetryExtra>,
    /// Command-specific result magnitude (e.g. dependency count, trace hops,
    /// search hits) for the `log --summary` view. `None` when not applicable.
    pub result_count: Option<i64>,
    /// Graph availability observed for this run: `available` / `locked` /
    /// `missing` / `corrupt` / `not_applicable`. `None` means unrecorded.
    pub graph_availability: Option<String>,
    /// Bounded WARN/ERROR events captured during the command.
    pub events: Vec<TelemetryErrorEvent>,
    pub error: Option<TelemetryErrorEvent>,
}

#[derive(Clone, Debug)]
pub struct TelemetryErrorEvent {
    pub level: String,
    pub category: String,
    pub message: String,
    pub context_json: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TelemetryRunRecord {
    pub run_id: String,
    pub workspace_hash: String,
    pub started_at_ms: i64,
    pub ended_at_ms: Option<i64>,
    pub command: String,
    pub cli_version: String,
    pub exit_status: String,
    pub duration_ms: Option<i64>,
    pub peak_rss_bytes: Option<u64>,
    pub warn_count: u32,
    pub error_count: u32,
    pub recovery_event: bool,
    pub result_count: Option<i64>,
    pub graph_availability: Option<String>,
    pub build_provenance: Option<String>,
    pub binary_path: Option<String>,
    pub build_sha: Option<String>,
    pub repo_count: Option<i64>,
    pub files_parsed: Option<i64>,
    pub nodes_created: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra_json: Option<TelemetryExtra>,
    pub error_categories: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TelemetryEventRecord {
    pub event_id: String,
    pub run_id: String,
    pub occurred_at_ms: i64,
    pub level: String,
    pub category: String,
    pub message_excerpt: Option<String>,
}

impl TelemetryStore {
    pub fn open(state_root: impl AsRef<Path>) -> Result<Self, TelemetryError> {
        let path = state_root.as_ref().join(TELEMETRY_DB_NAME);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| TelemetryError::CreateParent {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        // Close the default-umask window before creating any telemetry files.
        tighten_store_permissions(state_root.as_ref(), &path);
        let identity_key = load_or_create_identity_key(state_root.as_ref())?;
        let store = Self {
            path,
            identity_key,
            busy_retries: Arc::new(AtomicU32::new(0)),
        };
        store.initialize()?;
        tighten_store_permissions(state_root.as_ref(), &store.path);
        Ok(store)
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn workspace_hash(&self, path: &Path) -> String {
        blake3::keyed_hash(&self.identity_key, path.as_os_str().as_encoded_bytes())
            .to_hex()
            .to_string()
    }

    pub fn begin_run(
        &self,
        command: &str,
        workspace_path: &Path,
        cli_version: &str,
        build_provenance: &str,
        schema_versions: &serde_json::Value,
    ) -> Result<TelemetryRun, TelemetryError> {
        let run_id = generate_run_id();
        let schema_versions = serde_json::to_string(schema_versions)?;
        let workspace_hash = self.workspace_hash(workspace_path);
        let binary_path = std::env::current_exe()
            .ok()
            .map(|path| path.display().to_string());
        let build_sha = option_env!("GATHER_STEP_BUILD_SHA")
            .or(option_env!("GITHUB_SHA"))
            .map(ToOwned::to_owned);
        let process_id = process::id();
        let start_token = process_start_token(process_id);
        let now = now_ms();
        // Process-liveness probes spawn subprocesses on some platforms, so
        // stale candidates are resolved before taking the write lock.
        let stale_candidates = self.with_busy_retry(|| {
            let connection = self.connection()?;
            stale_running_candidates(&connection, now, Some(&workspace_hash))
        })?;
        let abandoned_run_ids = dead_candidate_run_ids(stale_candidates);
        self.with_busy_retry(|| {
            let mut connection = self.connection()?;
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            finalize_abandoned(&transaction, now, &abandoned_run_ids)?;
            prune_old_rows(&transaction, &workspace_hash)?;
            transaction.execute(
                "INSERT INTO run_log (
                    run_id, started_at_ms, command, workspace_hash, cli_version,
                    build_provenance, binary_path, build_sha, schema_versions,
                    exit_status, process_id, process_start_token, heartbeat_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'running', ?10, ?11, ?12)",
                params![
                    &run_id,
                    now_ms(),
                    command,
                    &workspace_hash,
                    cli_version,
                    build_provenance,
                    &binary_path,
                    &build_sha,
                    &schema_versions,
                    i64::from(process_id),
                    &start_token,
                    now_ms(),
                ],
            )?;
            transaction.commit()?;
            Ok(())
        })?;
        Ok(TelemetryRun { run_id })
    }

    pub fn finish_run(
        &self,
        run: &TelemetryRun,
        finish: &TelemetryRunFinish,
    ) -> Result<(), TelemetryError> {
        self.with_busy_retry(|| {
            let mut connection = self.connection()?;
            let ended_at = now_ms();
            let mut extra = finish.extra_json.clone().unwrap_or_default();
            extra.telemetry_busy_retries = self.busy_retry_count();
            let extra_json = (!extra.is_empty())
                .then(|| serde_json::to_string(&extra))
                .transpose()?;
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute(
                "UPDATE run_log
                 SET ended_at_ms = ?1,
                     exit_status = ?2,
                     duration_ms = ?1 - started_at_ms,
                     peak_rss_bytes = ?3,
                     repo_count = ?4,
                     files_parsed = ?5,
                     nodes_created = ?6,
                     warn_count = ?7,
                     error_count = ?8,
                     recovery_event = ?9,
                     extra_json = ?10,
                     result_count = ?11,
                     graph_availability = ?12
                 WHERE run_id = ?13",
                params![
                    ended_at,
                    &finish.exit_status,
                    finish.peak_rss_bytes.and_then(u64_to_i64),
                    finish.repo_count,
                    finish.files_parsed,
                    finish.nodes_created,
                    finish.warn_count,
                    finish.error_count,
                    i64::from(finish.recovery_event),
                    extra_json,
                    finish.result_count,
                    &finish.graph_availability,
                    &run.run_id,
                ],
            )?;
            for event in &finish.events {
                self.record_error_with_connection(&transaction, &run.run_id, event, ended_at)?;
            }
            if let Some(event) = &finish.error {
                self.record_error_with_connection(&transaction, &run.run_id, event, ended_at)?;
            }
            transaction.commit()?;
            Ok(())
        })
    }

    pub fn mark_panic(
        &self,
        run: &TelemetryRun,
        category: &str,
        message: &str,
    ) -> Result<(), TelemetryError> {
        let event = TelemetryErrorEvent {
            level: "PANIC".to_owned(),
            category: category.to_owned(),
            message: message.to_owned(),
            context_json: None,
        };
        self.with_busy_retry(|| {
            let mut connection = self.connection()?;
            let ended_at = now_ms();
            let transaction = connection.transaction()?;
            transaction.execute(
                "UPDATE run_log
                 SET ended_at_ms = ?1,
                     exit_status = 'panic',
                     duration_ms = ?1 - started_at_ms,
                     error_count = error_count + 1
                 WHERE run_id = ?2",
                params![ended_at, &run.run_id],
            )?;
            self.record_error_with_connection(&transaction, &run.run_id, &event, ended_at)?;
            transaction.commit()?;
            Ok(())
        })
    }

    /// Refresh the liveness timestamp for a long-running command.
    pub fn heartbeat(&self, run: &TelemetryRun) -> Result<(), TelemetryError> {
        self.with_busy_retry(|| {
            let connection = self.connection()?;
            connection.execute(
                "UPDATE run_log SET heartbeat_at_ms = ?1 WHERE run_id = ?2 AND exit_status = 'running'",
                params![now_ms(), &run.run_id],
            )?;
            Ok(())
        })
    }

    pub fn list_runs(
        &self,
        workspace_path: &Path,
        limit: usize,
        since_ms: Option<i64>,
        before_ms: Option<i64>,
        errors_only: bool,
        command: Option<&str>,
        status: Option<&str>,
        category: Option<&str>,
        exclude_run_id: Option<&str>,
    ) -> Result<Vec<TelemetryRunRecord>, TelemetryError> {
        self.list_runs_inner(
            Some(self.workspace_hash(workspace_path)),
            limit,
            since_ms,
            before_ms,
            errors_only,
            command,
            status,
            category,
            exclude_run_id,
        )
    }

    pub fn list_runs_all_workspaces(
        &self,
        limit: usize,
        since_ms: Option<i64>,
        before_ms: Option<i64>,
        errors_only: bool,
        command: Option<&str>,
        status: Option<&str>,
        category: Option<&str>,
        exclude_run_id: Option<&str>,
    ) -> Result<Vec<TelemetryRunRecord>, TelemetryError> {
        self.list_runs_inner(
            None,
            limit,
            since_ms,
            before_ms,
            errors_only,
            command,
            status,
            category,
            exclude_run_id,
        )
    }

    fn list_runs_inner(
        &self,
        workspace_hash_filter: Option<String>,
        limit: usize,
        since_ms: Option<i64>,
        before_ms: Option<i64>,
        errors_only: bool,
        command: Option<&str>,
        status: Option<&str>,
        category: Option<&str>,
        exclude_run_id: Option<&str>,
    ) -> Result<Vec<TelemetryRunRecord>, TelemetryError> {
        let connection = self.connection()?;
        let mut sql = String::from(
            "SELECT run_id, workspace_hash, started_at_ms, ended_at_ms, command, exit_status,
                    duration_ms, peak_rss_bytes, warn_count, error_count, recovery_event,
                    cli_version, result_count, graph_availability, build_provenance,
                    binary_path, build_sha, repo_count, files_parsed, nodes_created, extra_json,
                    (SELECT group_concat(category, char(31))
                     FROM run_errors WHERE run_errors.run_id = run_log.run_id)
             FROM run_log",
        );
        let mut clauses = Vec::new();
        let mut values = Vec::<rusqlite::types::Value>::new();
        if let Some(hash) = workspace_hash_filter {
            clauses.push("workspace_hash = ?");
            values.push(hash.into());
        }
        if since_ms.is_some() {
            clauses.push("started_at_ms >= ?");
            values.push(since_ms.unwrap_or_default().into());
        }
        if before_ms.is_some() {
            clauses.push("started_at_ms < ?");
            values.push(before_ms.unwrap_or_default().into());
        }
        if errors_only {
            clauses.push(
                "(exit_status NOT IN ('success', 'already_running', 'review_threshold_exceeded') OR error_count > 0)",
            );
        }
        if let Some(command) = command {
            clauses.push("command = ?");
            values.push(command.to_owned().into());
        }
        if let Some(status) = status {
            clauses.push("exit_status = ?");
            values.push(status.to_owned().into());
        }
        if let Some(category) = category {
            clauses.push(
                "EXISTS (SELECT 1 FROM run_errors WHERE run_errors.run_id = run_log.run_id AND run_errors.category = ?)",
            );
            values.push(category.to_owned().into());
        }
        if let Some(run_id) = exclude_run_id {
            clauses.push("run_id != ?");
            values.push(run_id.to_owned().into());
        }
        if !clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&clauses.join(" AND "));
        }
        sql.push_str(" ORDER BY started_at_ms DESC LIMIT ?");
        let limit_param = i64::try_from(limit).unwrap_or(i64::MAX);
        values.push(limit_param.into());
        let mut statement = connection.prepare(&sql)?;
        let rows =
            statement.query_map(rusqlite::params_from_iter(values), telemetry_run_from_row)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(TelemetryError::Sqlite)
    }

    pub fn clear_before(
        &self,
        workspace_path: &Path,
        cutoff_ms: i64,
    ) -> Result<usize, TelemetryError> {
        let workspace_hash = self.workspace_hash(workspace_path);
        self.with_busy_retry(|| {
            let connection = self.connection()?;
            connection
                .execute(
                    "DELETE FROM run_log WHERE workspace_hash = ?1 AND started_at_ms < ?2",
                    params![&workspace_hash, cutoff_ms],
                )
                .map_err(TelemetryError::Sqlite)
        })
    }

    pub fn clear_before_all_workspaces(&self, cutoff_ms: i64) -> Result<usize, TelemetryError> {
        self.with_busy_retry(|| {
            let connection = self.connection()?;
            connection
                .execute(
                    "DELETE FROM run_log WHERE started_at_ms < ?1",
                    params![cutoff_ms],
                )
                .map_err(TelemetryError::Sqlite)
        })
    }

    pub fn list_events(
        &self,
        workspace_path: &Path,
        run_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<TelemetryEventRecord>, TelemetryError> {
        let connection = self.connection()?;
        let mut sql = String::from(
            "SELECT errors.event_id, errors.run_id, errors.occurred_at_ms, errors.level,
                    errors.category, errors.message_excerpt
             FROM run_errors AS errors
             INNER JOIN run_log AS runs ON runs.run_id = errors.run_id
             WHERE runs.workspace_hash = ?1",
        );
        if run_id.is_some() {
            sql.push_str(" AND errors.run_id = ?2 ORDER BY errors.occurred_at_ms DESC LIMIT ?3");
        } else {
            sql.push_str(" ORDER BY errors.occurred_at_ms DESC LIMIT ?2");
        }
        let mut statement = connection.prepare(&sql)?;
        let hash = self.workspace_hash(workspace_path);
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let map = |row: &rusqlite::Row<'_>| {
            Ok(TelemetryEventRecord {
                event_id: row.get(0)?,
                run_id: row.get(1)?,
                occurred_at_ms: row.get(2)?,
                level: row.get(3)?,
                category: row.get(4)?,
                message_excerpt: row.get(5)?,
            })
        };
        let rows = if let Some(run_id) = run_id {
            statement.query_map(params![hash, run_id, limit], map)?
        } else {
            statement.query_map(params![hash, limit], map)?
        };
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(TelemetryError::Sqlite)
    }

    fn initialize(&self) -> Result<(), TelemetryError> {
        self.with_busy_retry(|| {
            let mut connection = self.connection()?;
            let observed_version: i64 =
                connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
            if observed_version > TELEMETRY_SCHEMA_VERSION {
                return Err(TelemetryError::UnsupportedSchemaVersion {
                    stored: observed_version,
                    supported: TELEMETRY_SCHEMA_VERSION,
                });
            }
            // `user_version` is committed in the same transaction as the full
            // schema. Once it reaches the current version, initialization is
            // complete and concurrent CLI processes can avoid competing for a
            // write lock merely to replay idempotent DDL.
            if observed_version == TELEMETRY_SCHEMA_VERSION {
                return Ok(());
            }
            let journal_mode: String =
                connection.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
            if !journal_mode.eq_ignore_ascii_case("wal") {
                connection.pragma_update(None, "journal_mode", "WAL")?;
            }
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let stored_version: i64 =
                transaction.pragma_query_value(None, "user_version", |row| row.get(0))?;
            if stored_version > TELEMETRY_SCHEMA_VERSION {
                return Err(TelemetryError::UnsupportedSchemaVersion {
                    stored: stored_version,
                    supported: TELEMETRY_SCHEMA_VERSION,
                });
            }
            if stored_version == TELEMETRY_SCHEMA_VERSION {
                transaction.commit()?;
                return Ok(());
            }
            transaction.execute_batch(SCHEMA)?;
            migrate_schema(&transaction, stored_version)?;
            transaction.pragma_update(None, "user_version", TELEMETRY_SCHEMA_VERSION)?;
            transaction.commit()?;
            Ok(())
        })
    }

    /// Finalize every `running` row older than [`STALE_RUNNING_THRESHOLD_MS`]
    /// as `abandoned`, returning how many were rewritten. The current workspace
    /// is repaired automatically when a new run begins; this method powers the
    /// explicit `gather-step log --repair` path.
    pub fn repair_stale_running(&self, workspace_path: &Path) -> Result<usize, TelemetryError> {
        let workspace_hash = self.workspace_hash(workspace_path);
        self.finalize_stale_running(Some(&workspace_hash))
    }

    pub fn repair_stale_running_all_workspaces(&self) -> Result<usize, TelemetryError> {
        self.finalize_stale_running(None)
    }

    fn finalize_stale_running(
        &self,
        workspace_hash_filter: Option<&str>,
    ) -> Result<usize, TelemetryError> {
        let now = now_ms();
        let candidates = self.with_busy_retry(|| {
            let connection = self.connection()?;
            stale_running_candidates(&connection, now, workspace_hash_filter)
        })?;
        let dead_run_ids = dead_candidate_run_ids(candidates);
        if dead_run_ids.is_empty() {
            return Ok(0);
        }
        self.with_busy_retry(|| {
            let mut connection = self.connection()?;
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let updated = finalize_abandoned(&transaction, now, &dead_run_ids)?;
            transaction.commit()?;
            Ok(updated)
        })
    }

    fn connection(&self) -> Result<Connection, TelemetryError> {
        let connection = Connection::open(&self.path)?;
        connection.busy_timeout(Duration::from_millis(500))?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        Ok(connection)
    }

    #[must_use]
    pub fn busy_retry_count(&self) -> u32 {
        self.busy_retries.load(Ordering::Relaxed)
    }

    fn with_busy_retry<T>(
        &self,
        mut operation: impl FnMut() -> Result<T, TelemetryError>,
    ) -> Result<T, TelemetryError> {
        let deadline = Instant::now() + BUSY_RETRY_BUDGET;
        let mut attempt = 0_u32;
        loop {
            match operation() {
                Ok(value) => return Ok(value),
                Err(error) if is_sqlite_busy(&error) && attempt < BUSY_RETRY_ATTEMPTS => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(error);
                    }
                    self.busy_retries.fetch_add(1, Ordering::Relaxed);
                    let delay = BUSY_RETRY_BASE_DELAY * 2_u32.saturating_pow(attempt);
                    std::thread::sleep(delay.min(remaining));
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn record_error_with_connection(
        &self,
        connection: &Connection,
        run_id: &str,
        event: &TelemetryErrorEvent,
        occurred_at_ms: i64,
    ) -> Result<(), TelemetryError> {
        let context_json = event
            .context_json
            .as_ref()
            .map(redact_json_value)
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        connection.execute(
            "INSERT INTO run_errors (
                event_id, run_id, occurred_at_ms, level, category, message_hash,
                message_excerpt, context_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                generate_event_id(),
                run_id,
                occurred_at_ms,
                event.level.as_str(),
                event.category.as_str(),
                blake3::keyed_hash(&self.identity_key, event.message.as_bytes())
                    .to_hex()
                    .to_string(),
                redact_message_excerpt(&event.message),
                context_json,
            ],
        )?;
        Ok(())
    }
}

fn is_sqlite_busy(error: &TelemetryError) -> bool {
    matches!(
        error,
        TelemetryError::Sqlite(error)
            if matches!(
                error.sqlite_error_code(),
                Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
            )
    )
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde skip_serializing_if predicates receive a shared reference"
)]
const fn is_zero_u32(value: &u32) -> bool {
    *value == 0
}

fn telemetry_run_from_row(row: &rusqlite::Row<'_>) -> Result<TelemetryRunRecord, rusqlite::Error> {
    let peak_rss_bytes: Option<i64> = row.get(7)?;
    let warn_count: i64 = row.get(8)?;
    let error_count: i64 = row.get(9)?;
    let recovery_event: i64 = row.get(10)?;
    let raw_extra: Option<String> = row.get(20)?;
    let raw_categories: Option<String> = row.get(21)?;
    Ok(TelemetryRunRecord {
        run_id: row.get(0)?,
        workspace_hash: row.get(1)?,
        started_at_ms: row.get(2)?,
        ended_at_ms: row.get(3)?,
        command: row.get(4)?,
        cli_version: row.get(11)?,
        exit_status: row.get(5)?,
        duration_ms: row.get(6)?,
        peak_rss_bytes: peak_rss_bytes.and_then(i64_to_u64),
        warn_count: u32::try_from(warn_count).unwrap_or(u32::MAX),
        error_count: u32::try_from(error_count).unwrap_or(u32::MAX),
        recovery_event: recovery_event != 0,
        result_count: row.get(12)?,
        graph_availability: row.get(13)?,
        build_provenance: row.get(14)?,
        binary_path: row.get(15)?,
        build_sha: row.get(16)?,
        repo_count: row.get(17)?,
        files_parsed: row.get(18)?,
        nodes_created: row.get(19)?,
        extra_json: raw_extra.as_deref().and_then(|value| {
            serde_json::from_str(value)
                .map_err(|error| {
                    tracing::warn!(%error, "telemetry extra_json failed to deserialize; dropping extras");
                })
                .ok()
        }),
        error_categories: raw_categories
            .as_deref()
            .map(|categories| categories.split('\u{1f}').map(ToOwned::to_owned).collect())
            .unwrap_or_default(),
    })
}

/// Bring a pre-existing telemetry database up to the current column shape.
///
/// The base `SCHEMA` only creates missing tables (`CREATE TABLE IF NOT
/// EXISTS`), so a database created by an older gather-step keeps its original
/// columns. Each `ADD COLUMN` here is idempotent: on a fresh database the
/// column already exists and `SQLite` reports a duplicate-column error, which
/// we treat as success.
fn migrate_schema(connection: &Connection, stored_version: i64) -> Result<(), TelemetryError> {
    if stored_version < 2 {
        add_column_if_missing(connection, "run_log", "result_count", "INTEGER")?;
        add_column_if_missing(connection, "run_log", "graph_availability", "TEXT")?;
        add_column_if_missing(connection, "run_log", "build_provenance", "TEXT")?;
    }
    if stored_version < 3 {
        add_column_if_missing(connection, "run_log", "process_id", "INTEGER")?;
        add_column_if_missing(connection, "run_log", "process_start_token", "TEXT")?;
        add_column_if_missing(connection, "run_log", "heartbeat_at_ms", "INTEGER")?;
        add_column_if_missing(connection, "run_log", "binary_path", "TEXT")?;
        add_column_if_missing(connection, "run_log", "build_sha", "TEXT")?;
        add_column_if_missing(connection, "run_errors", "message_excerpt", "TEXT")?;
    }
    Ok(())
}

fn add_column_if_missing(
    connection: &Connection,
    table: &str,
    column: &str,
    sql_type: &str,
) -> Result<(), TelemetryError> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    if !columns.iter().any(|existing| existing == column) {
        connection.execute(
            &format!("ALTER TABLE {table} ADD COLUMN {column} {sql_type}"),
            [],
        )?;
    }
    Ok(())
}

/// `(run_id, process_id, process_start_token)` evidence for a stale
/// `running` row.
type StaleRunningCandidate = (String, Option<u32>, Option<String>);

/// List `running` rows older than the stale threshold, together with the
/// PID/start-token evidence needed to decide whether their process still
/// lives. Read-only so callers can probe liveness outside any write lock.
fn stale_running_candidates(
    connection: &Connection,
    now: i64,
    workspace_hash_filter: Option<&str>,
) -> Result<Vec<StaleRunningCandidate>, TelemetryError> {
    let cutoff = now.saturating_sub(STALE_RUNNING_THRESHOLD_MS);
    let sql = if workspace_hash_filter.is_some() {
        "SELECT run_id, process_id, process_start_token
         FROM run_log
         WHERE exit_status = 'running'
           AND COALESCE(heartbeat_at_ms, started_at_ms) < ?1
           AND workspace_hash = ?2"
    } else {
        "SELECT run_id, process_id, process_start_token
         FROM run_log
         WHERE exit_status = 'running'
           AND COALESCE(heartbeat_at_ms, started_at_ms) < ?1"
    };
    let mut statement = connection.prepare(sql)?;
    let map = |row: &rusqlite::Row<'_>| {
        let raw_pid = row.get::<_, Option<i64>>(1)?;
        Ok((
            row.get::<_, String>(0)?,
            raw_pid.and_then(|pid| u32::try_from(pid).ok()),
            row.get::<_, Option<String>>(2)?,
        ))
    };
    let candidates = if let Some(hash) = workspace_hash_filter {
        statement
            .query_map(params![cutoff, hash], map)?
            .collect::<Result<Vec<_>, _>>()?
    } else {
        statement
            .query_map(params![cutoff], map)?
            .collect::<Result<Vec<_>, _>>()?
    };
    Ok(candidates)
}

/// Keep only the candidates whose recorded process is verifiably gone. Runs
/// the subprocess-backed liveness probes, so call it without holding a
/// `SQLite` write lock.
fn dead_candidate_run_ids(candidates: Vec<StaleRunningCandidate>) -> Vec<String> {
    candidates
        .into_iter()
        .filter(|(_, pid, start_token)| {
            pid.is_none_or(|pid| {
                matches!(
                    process_identity(pid, start_token.as_deref()),
                    ProcessIdentity::Dead | ProcessIdentity::Reused
                )
            })
        })
        .map(|(run_id, _, _)| run_id)
        .collect()
}

/// Rewrite the given runs as `abandoned`. The true end time of an abandoned
/// process is unknown, so duration fields remain NULL. Status and heartbeat
/// are re-checked here because liveness was probed outside the transaction:
/// a run that finished or heartbeated meanwhile is left untouched.
fn finalize_abandoned(
    connection: &Connection,
    now: i64,
    run_ids: &[String],
) -> Result<usize, TelemetryError> {
    let cutoff = now.saturating_sub(STALE_RUNNING_THRESHOLD_MS);
    let mut updated = 0;
    for run_id in run_ids {
        updated += connection.execute(
            "UPDATE run_log
             SET exit_status = 'abandoned', ended_at_ms = NULL, duration_ms = NULL
             WHERE run_id = ?1 AND exit_status = 'running'
               AND COALESCE(heartbeat_at_ms, started_at_ms) < ?2",
            params![run_id, cutoff],
        )?;
    }
    Ok(updated)
}

fn prune_old_rows(connection: &Connection, workspace_hash: &str) -> Result<(), TelemetryError> {
    let cutoff_ms = now_ms().saturating_sub(RETENTION_DAYS * 24 * 60 * 60 * 1000);
    connection.execute(
        "DELETE FROM run_log WHERE workspace_hash = ?1 AND started_at_ms < ?2",
        params![workspace_hash, cutoff_ms],
    )?;
    connection.execute(
        "DELETE FROM run_log
         WHERE run_id IN (
             SELECT run_id
             FROM run_log
             WHERE workspace_hash = ?1
             ORDER BY started_at_ms DESC
             LIMIT -1 OFFSET ?2
         )",
        params![workspace_hash, MAX_RUN_ROWS],
    )?;
    Ok(())
}

fn generate_run_id() -> String {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let material = format!(
        "{}:{}:{}:{}",
        now_ns(),
        process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed),
        std::thread::current().name().unwrap_or("unnamed")
    );
    hash_text(&material)[..32].to_owned()
}

fn generate_event_id() -> String {
    generate_run_id()
}

fn now_ms() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis());
    i64::try_from(millis).unwrap_or(i64::MAX)
}

fn now_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos())
}

fn hash_text(value: &str) -> String {
    blake3::hash(value.as_bytes()).to_hex().to_string()
}

fn load_or_create_identity_key(state_root: &Path) -> Result<[u8; 32], TelemetryError> {
    let path = state_root.join(TELEMETRY_IDENTITY_KEY_NAME);
    match fs::read(&path) {
        Ok(bytes) if bytes.len() == 32 => return identity_key_from_bytes(&path, &bytes),
        Ok(_) => return read_identity_key_retrying(&path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(TelemetryError::ReadIdentityKey { path, source });
        }
    }

    let generated = rand::rng()
        .sample_iter(rand::distr::Alphanumeric)
        .take(32)
        .collect::<Vec<u8>>();
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    match options.open(&path) {
        Ok(mut file) => {
            file.write_all(&generated)
                .and_then(|()| file.sync_data())
                .map_err(|source| TelemetryError::WriteIdentityKey {
                    path: path.clone(),
                    source,
                })?;
            identity_key_from_bytes(&path, &generated)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            read_identity_key_retrying(&path)
        }
        Err(source) => Err(TelemetryError::WriteIdentityKey { path, source }),
    }
}

fn read_identity_key_retrying(path: &Path) -> Result<[u8; 32], TelemetryError> {
    for attempt in 0..=IDENTITY_KEY_READ_RETRIES {
        let bytes = fs::read(path).map_err(|source| TelemetryError::ReadIdentityKey {
            path: path.to_path_buf(),
            source,
        })?;
        if bytes.len() == 32 || attempt == IDENTITY_KEY_READ_RETRIES {
            return identity_key_from_bytes(path, &bytes);
        }
        std::thread::sleep(IDENTITY_KEY_READ_RETRY_DELAY);
    }
    unreachable!("bounded identity-key read loop always returns")
}

fn identity_key_from_bytes(path: &Path, bytes: &[u8]) -> Result<[u8; 32], TelemetryError> {
    bytes
        .try_into()
        .map_err(|_| TelemetryError::InvalidIdentityKey {
            path: path.to_path_buf(),
            length: bytes.len(),
        })
}

/// Restrict the telemetry directory (0700) and database plus its `SQLite`
/// sidecars (0600) to the owning user, tightening pre-existing stores too.
/// Best-effort: a failed chmod (e.g. a store owned by another user) must not
/// break telemetry. No-op off Unix.
#[cfg(unix)]
fn tighten_store_permissions(state_root: &Path, db_path: &Path) {
    use std::os::unix::fs::PermissionsExt as _;
    let chmod = |path: &Path, mode: u32| {
        let Ok(metadata) = fs::metadata(path) else {
            return;
        };
        let mut permissions = metadata.permissions();
        if permissions.mode() & 0o777 != mode {
            permissions.set_mode(mode);
            let _ = fs::set_permissions(path, permissions);
        }
    };
    chmod(state_root, 0o700);
    chmod(&state_root.join(TELEMETRY_IDENTITY_KEY_NAME), 0o600);
    chmod(db_path, 0o600);
    for suffix in ["-wal", "-shm"] {
        let mut sidecar = db_path.as_os_str().to_owned();
        sidecar.push(suffix);
        chmod(Path::new(&sidecar), 0o600);
    }
}

#[cfg(not(unix))]
fn tighten_store_permissions(_state_root: &Path, _db_path: &Path) {}

fn redact_message_excerpt(message: &str) -> String {
    static SECRET_PAIR: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(
            r#"(?i)(?P<key>[A-Za-z0-9_.-]*(?:token|secret|key|password|passwd|auth|credential)[A-Za-z0-9_.-]*)\s*(?:[=:]\s*|\s+)(?:bearer\s+)?(?:"[^"]*"|'[^']*'|[^\s,;]+)"#,
        )
        .expect("valid secret pair redaction regex")
    });
    static URL_USERINFO: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(?P<scheme>[A-Za-z][A-Za-z0-9+.-]*://)[^/@\s]+@")
            .expect("valid URL userinfo redaction regex")
    });
    // The leading path segment must not start with `/` so `scheme://` URL
    // authorities survive with only their userinfo redacted above.
    static UNIX_PATH: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r#"(?P<prefix>^|[\s=:"'(\[])/(?P<path>[^\s,;)"'/][^\s,;)"']*)"#)
            .expect("valid Unix path redaction regex")
    });
    static WINDOWS_PATH: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r#"(?P<prefix>^|[\s=:"'(\[])[A-Za-z]:\\(?P<path>[^\s,;)"']+)"#)
            .expect("valid Windows path redaction regex")
    });
    let redacted = SECRET_PAIR.replace_all(message, "${key}=<redacted>");
    let redacted = URL_USERINFO.replace_all(&redacted, "${scheme}<redacted>@");
    let redacted = UNIX_PATH.replace_all(&redacted, "${prefix}<path>");
    let redacted = WINDOWS_PATH.replace_all(&redacted, "${prefix}<path>");
    redacted.chars().take(240).collect()
}

fn redact_json_value(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::String(value) => {
            serde_json::Value::String(redact_message_excerpt(value))
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(redact_json_value).collect())
        }
        serde_json::Value::Object(values) => serde_json::Value::Object(
            values
                .iter()
                .map(|(key, value)| {
                    let redacted = if sensitive_context_key(key) {
                        serde_json::Value::String("<redacted>".to_owned())
                    } else {
                        redact_json_value(value)
                    };
                    (key.clone(), redacted)
                })
                .collect(),
        ),
        other => other.clone(),
    }
}

fn sensitive_context_key(key: &str) -> bool {
    static SENSITIVE_KEY: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(
            r"(?i)(token|secret|password|passwd|authorization|credential|api[-_.]?key|access[-_.]?key|private[-_.]?key|(^|[_.-])(auth|key)([_.-]|$))",
        )
        .expect("valid sensitive context key regex")
    });
    SENSITIVE_KEY.is_match(key)
}

fn process_start_token(pid: u32) -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let after_name = stat.rsplit_once(')')?.1;
        after_name.split_whitespace().nth(19).map(ToOwned::to_owned)
    }
    #[cfg(target_os = "macos")]
    {
        let output = process::Command::new("ps")
            .args(["-o", "lstart=", "-p", &pid.to_string()])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let token = String::from_utf8(output.stdout).ok()?;
        let token = token.trim();
        (!token.is_empty()).then(|| token.to_owned())
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = pid;
        None
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProcessIdentity {
    Same,
    Reused,
    Dead,
    Unknown,
}

enum ProcessLiveness {
    Alive,
    Dead,
    #[cfg(not(target_os = "linux"))]
    Unknown,
}

fn process_identity(pid: u32, expected_start_token: Option<&str>) -> ProcessIdentity {
    match process_liveness(pid) {
        ProcessLiveness::Dead => return ProcessIdentity::Dead,
        #[cfg(not(target_os = "linux"))]
        ProcessLiveness::Unknown => {
            tracing::warn!(
                pid,
                "process liveness is unsupported on this platform; stale-run identity remains unknown"
            );
            return ProcessIdentity::Unknown;
        }
        ProcessLiveness::Alive => {}
    }
    let Some(expected_start_token) = expected_start_token else {
        tracing::warn!(
            pid,
            "recorded process start token is missing; stale-run identity remains unknown"
        );
        return ProcessIdentity::Unknown;
    };
    for attempt in 0..=PROCESS_START_TOKEN_READ_RETRIES {
        let identity = alive_process_identity(
            Some(expected_start_token),
            process_start_token(pid).as_deref(),
        );
        if identity != ProcessIdentity::Unknown {
            return identity;
        }
        if attempt < PROCESS_START_TOKEN_READ_RETRIES {
            std::thread::sleep(PROCESS_START_TOKEN_READ_RETRY_DELAY);
        }
    }
    tracing::warn!(
        pid,
        "process start token unavailable after retries; stale-run identity remains unknown"
    );
    ProcessIdentity::Unknown
}

/// Identity decision for a PID that is already known to be alive.
///
/// Matching tokens confirm the recorded process; mismatched tokens prove a
/// recycled PID. Missing evidence stays explicitly unknown; callers may retry
/// without incorrectly declaring an unverified PID to be the same process.
fn alive_process_identity(expected: Option<&str>, actual: Option<&str>) -> ProcessIdentity {
    match (expected, actual) {
        (Some(expected), Some(actual)) if expected == actual => ProcessIdentity::Same,
        (Some(_), Some(_)) => ProcessIdentity::Reused,
        (Some(_), None) | (None, _) => ProcessIdentity::Unknown,
    }
}

#[cfg(target_os = "linux")]
fn process_liveness(pid: u32) -> ProcessLiveness {
    if Path::new("/proc").join(pid.to_string()).exists() {
        ProcessLiveness::Alive
    } else {
        ProcessLiveness::Dead
    }
}

#[cfg(all(unix, not(target_os = "linux")))]
fn process_liveness(pid: u32) -> ProcessLiveness {
    match process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .status()
    {
        Ok(status) if status.success() => ProcessLiveness::Alive,
        Ok(_) => ProcessLiveness::Dead,
        Err(_) => ProcessLiveness::Unknown,
    }
}

#[cfg(not(unix))]
fn process_liveness(_pid: u32) -> ProcessLiveness {
    ProcessLiveness::Unknown
}

fn u64_to_i64(value: u64) -> Option<i64> {
    i64::try_from(value).ok()
}

fn i64_to_u64(value: i64) -> Option<u64> {
    u64::try_from(value).ok()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_root(tag: &str) -> PathBuf {
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("gs-telemetry-{tag}-{}-{id}", std::process::id()));
        fs::create_dir_all(&dir).expect("create temp root");
        dir
    }

    fn schema_versions() -> serde_json::Value {
        serde_json::json!({ "telemetry": TELEMETRY_SCHEMA_VERSION })
    }

    #[test]
    fn v3_fields_round_trip() {
        let root = temp_root("roundtrip");
        let store = TelemetryStore::open(&root).expect("open");
        let run = store
            .begin_run(
                "index",
                Path::new("/ws"),
                "9.9.9",
                "release",
                &schema_versions(),
            )
            .expect("begin");
        let finish = TelemetryRunFinish {
            exit_status: "success".to_owned(),
            result_count: Some(42),
            graph_availability: Some("available".to_owned()),
            extra_json: Some(TelemetryExtra {
                result_kind: Some(TelemetryResultKind::SearchHits),
                graph_open_retries: 2,
                telemetry_busy_retries: 0,
            }),
            ..TelemetryRunFinish::default()
        };
        store.finish_run(&run, &finish).expect("finish");

        let runs = store
            .list_runs_all_workspaces(10, None, None, false, None, None, None, None)
            .expect("list");
        let record = runs
            .iter()
            .find(|record| record.run_id == run.run_id)
            .expect("row present");
        assert_eq!(record.result_count, Some(42));
        assert_eq!(record.graph_availability.as_deref(), Some("available"));
        assert_eq!(
            record.extra_json,
            Some(TelemetryExtra {
                result_kind: Some(TelemetryResultKind::SearchHits),
                graph_open_retries: 2,
                telemetry_busy_retries: store.busy_retry_count(),
            })
        );
        assert_eq!(record.build_provenance.as_deref(), Some("release"));
        assert!(record.binary_path.is_some());
    }

    #[test]
    fn migrate_schema_adds_columns_to_pre_v2_db_without_data_loss() {
        let root = temp_root("migrate");
        let db_path = root.join(TELEMETRY_DB_NAME);
        {
            let connection = Connection::open(&db_path).expect("open raw");
            connection
                .execute_batch(
                    "CREATE TABLE run_log (
                        run_id TEXT PRIMARY KEY,
                        started_at_ms INTEGER NOT NULL,
                        ended_at_ms INTEGER,
                        command TEXT NOT NULL,
                        workspace_hash TEXT NOT NULL,
                        cli_version TEXT NOT NULL,
                        schema_versions TEXT NOT NULL,
                        exit_status TEXT NOT NULL,
                        duration_ms INTEGER,
                        peak_rss_bytes INTEGER,
                        repo_count INTEGER,
                        files_parsed INTEGER,
                        nodes_created INTEGER,
                        warn_count INTEGER NOT NULL DEFAULT 0,
                        error_count INTEGER NOT NULL DEFAULT 0,
                        recovery_event INTEGER NOT NULL DEFAULT 0,
                        extra_json TEXT
                    );",
                )
                .expect("create legacy table");
            connection
                .execute(
                    "INSERT INTO run_log
                        (run_id, started_at_ms, command, workspace_hash, cli_version,
                         schema_versions, exit_status)
                     VALUES ('old-1', ?1, 'status', 'hash', '1.0.0', '{}', 'success')",
                    params![now_ms()],
                )
                .expect("seed legacy row");
        }

        let store = TelemetryStore::open(&root).expect("open migrates");
        let runs = store
            .list_runs_all_workspaces(10, None, None, false, None, None, None, None)
            .expect("list");
        let record = runs
            .iter()
            .find(|record| record.run_id == "old-1")
            .expect("legacy row preserved");
        assert_eq!(record.command, "status");
        assert_eq!(record.result_count, None);
        assert_eq!(record.graph_availability, None);
        assert_eq!(record.build_provenance, None);
        assert_eq!(record.binary_path, None);
        assert_eq!(record.build_sha, None);

        // Reopening runs the idempotent migration again without error.
        drop(store);
        TelemetryStore::open(&root).expect("reopen is idempotent");
    }

    #[test]
    fn explicit_repair_finalizes_stale_rows_but_keeps_live_ones() {
        let root = temp_root("stale");
        let db_path = root.join(TELEMETRY_DB_NAME);
        {
            TelemetryStore::open(&root).expect("initialize");
            let connection = Connection::open(&db_path).expect("open raw");
            let stale_start = now_ms() - STALE_RUNNING_THRESHOLD_MS - 1_000;
            connection
                .execute(
                    "INSERT INTO run_log
                        (run_id, started_at_ms, command, workspace_hash, cli_version,
                         schema_versions, exit_status)
                     VALUES ('stale', ?1, 'index', 'hash', '1.0.0', '{}', 'running')",
                    params![stale_start],
                )
                .expect("seed stale");
            connection
                .execute(
                    "INSERT INTO run_log
                        (run_id, started_at_ms, command, workspace_hash, cli_version,
                         schema_versions, exit_status)
                     VALUES ('fresh', ?1, 'index', 'hash', '1.0.0', '{}', 'running')",
                    params![now_ms()],
                )
                .expect("seed fresh");
            connection
                .execute(
                    "INSERT INTO run_log
                        (run_id, started_at_ms, command, workspace_hash, cli_version,
                         schema_versions, exit_status, process_id, process_start_token,
                         heartbeat_at_ms)
                     VALUES ('live', ?1, 'serve', 'hash', '1.0.0', '{}', 'running', ?2, ?3, ?1)",
                    params![
                        stale_start,
                        i64::from(process::id()),
                        process_start_token(process::id())
                    ],
                )
                .expect("seed live process");
        }

        let store = TelemetryStore::open(&root).expect("reopen");
        assert_eq!(
            store.repair_stale_running_all_workspaces().expect("repair"),
            1
        );
        let runs = store
            .list_runs_all_workspaces(10, None, None, false, None, None, None, None)
            .expect("list");
        let status = |id: &str| {
            runs.iter()
                .find(|record| record.run_id == id)
                .map(|record| record.exit_status.clone())
        };
        assert_eq!(status("stale").as_deref(), Some("abandoned"));
        assert_eq!(status("fresh").as_deref(), Some("running"));
        assert_eq!(status("live").as_deref(), Some("running"));

        // Already finalized: another repair rewrites nothing.
        assert_eq!(
            store.repair_stale_running_all_workspaces().expect("repair"),
            0
        );
    }

    #[test]
    fn workspace_reads_and_deletes_are_isolated() {
        let root = temp_root("workspace-scope");
        let store = TelemetryStore::open(&root).expect("open");
        for workspace in ["/workspace/alpha", "/workspace/beta"] {
            let run = store
                .begin_run(
                    "status",
                    Path::new(workspace),
                    "9.9.9",
                    "release",
                    &schema_versions(),
                )
                .expect("begin");
            store
                .finish_run(
                    &run,
                    &TelemetryRunFinish {
                        exit_status: "success".to_owned(),
                        ..TelemetryRunFinish::default()
                    },
                )
                .expect("finish");
        }

        let alpha = store
            .list_runs(
                Path::new("/workspace/alpha"),
                10,
                None,
                None,
                false,
                None,
                None,
                None,
                None,
            )
            .expect("alpha rows");
        let beta = store
            .list_runs(
                Path::new("/workspace/beta"),
                10,
                None,
                None,
                false,
                None,
                None,
                None,
                None,
            )
            .expect("beta rows");
        assert_eq!(alpha.len(), 1);
        assert_eq!(beta.len(), 1);
        assert_ne!(alpha[0].workspace_hash, beta[0].workspace_hash);

        store
            .clear_before(Path::new("/workspace/alpha"), now_ms() + 1)
            .expect("clear alpha");
        assert!(
            store
                .list_runs(
                    Path::new("/workspace/alpha"),
                    10,
                    None,
                    None,
                    false,
                    None,
                    None,
                    None,
                    None,
                )
                .expect("alpha rows")
                .is_empty()
        );
        assert_eq!(
            store
                .list_runs(
                    Path::new("/workspace/beta"),
                    10,
                    None,
                    None,
                    false,
                    None,
                    None,
                    None,
                    None,
                )
                .expect("beta rows")
                .len(),
            1
        );
    }

    #[test]
    fn workspace_identity_is_install_keyed_and_stable() {
        let first_root = temp_root("identity-first");
        let second_root = temp_root("identity-second");
        let workspace = Path::new("/workspace/known-path");

        let first = TelemetryStore::open(&first_root).expect("open first install");
        let first_hash = first.workspace_hash(workspace);
        assert_eq!(first_hash, first.workspace_hash(workspace));
        drop(first);

        let reopened = TelemetryStore::open(&first_root).expect("reopen first install");
        assert_eq!(first_hash, reopened.workspace_hash(workspace));

        let second = TelemetryStore::open(&second_root).expect("open second install");
        assert_ne!(first_hash, second.workspace_hash(workspace));
        assert_ne!(first_hash, hash_text(&workspace.display().to_string()));
    }

    #[test]
    fn future_schema_is_rejected_without_rewrite() {
        let root = temp_root("future-schema");
        let db_path = root.join(TELEMETRY_DB_NAME);
        let connection = Connection::open(&db_path).expect("open raw");
        connection
            .pragma_update(None, "user_version", 99_i64)
            .expect("seed future version");
        drop(connection);

        let error = TelemetryStore::open(&root).expect_err("future schema must fail");
        assert!(matches!(
            error,
            TelemetryError::UnsupportedSchemaVersion {
                stored: 99,
                supported: TELEMETRY_SCHEMA_VERSION
            }
        ));
        let connection = Connection::open(&db_path).expect("reopen raw");
        let stored: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("read version");
        assert_eq!(stored, 99);
    }

    #[test]
    fn finish_and_error_event_commit_atomically_with_redacted_excerpt() {
        let root = temp_root("atomic-finish");
        let store = TelemetryStore::open(&root).expect("open");
        let workspace = Path::new("/workspace/assets");
        let run = store
            .begin_run("index", workspace, "9.9.9", "release", &schema_versions())
            .expect("begin");
        store
            .finish_run(
                &run,
                &TelemetryRunFinish {
                    exit_status: "error".to_owned(),
                    error_count: 1,
                    error: Some(TelemetryErrorEvent {
                        level: "ERROR".to_owned(),
                        category: "parse_failure".to_owned(),
                        message: "/private/workspace/assets.py failed to parse".to_owned(),
                        context_json: Some(serde_json::json!({
                            "request": {
                                "api_token": "context-secret",
                                "source": "/private/workspace/assets.py"
                            }
                        })),
                    }),
                    ..TelemetryRunFinish::default()
                },
            )
            .expect("finish");

        let events = store
            .list_events(workspace, Some(&run.run_id), 10)
            .expect("events");
        assert_eq!(events.len(), 1);
        let excerpt = events[0].message_excerpt.as_deref().expect("excerpt");
        assert!(excerpt.contains("<path>"));
        assert!(!excerpt.contains("/private/workspace"));

        let connection = Connection::open(store.path()).expect("open telemetry database");
        let context: String = connection
            .query_row(
                "SELECT context_json FROM run_errors WHERE run_id = ?1",
                params![run.run_id],
                |row| row.get(0),
            )
            .expect("stored context");
        assert!(!context.contains("context-secret"), "{context}");
        assert!(!context.contains("/private/workspace"), "{context}");
        assert!(context.contains("<redacted>"), "{context}");
        assert!(context.contains("<path>"), "{context}");
    }

    #[test]
    fn migrate_schema_upgrades_v2_store_to_v3_without_data_loss() {
        let root = temp_root("migrate-v2");
        let db_path = root.join(TELEMETRY_DB_NAME);
        {
            let connection = Connection::open(&db_path).expect("open raw");
            connection
                .execute_batch(
                    "CREATE TABLE run_log (
                        run_id TEXT PRIMARY KEY,
                        started_at_ms INTEGER NOT NULL,
                        ended_at_ms INTEGER,
                        command TEXT NOT NULL,
                        workspace_hash TEXT NOT NULL,
                        cli_version TEXT NOT NULL,
                        schema_versions TEXT NOT NULL,
                        exit_status TEXT NOT NULL,
                        duration_ms INTEGER,
                        peak_rss_bytes INTEGER,
                        repo_count INTEGER,
                        files_parsed INTEGER,
                        nodes_created INTEGER,
                        warn_count INTEGER NOT NULL DEFAULT 0,
                        error_count INTEGER NOT NULL DEFAULT 0,
                        recovery_event INTEGER NOT NULL DEFAULT 0,
                        extra_json TEXT,
                        result_count INTEGER,
                        graph_availability TEXT,
                        build_provenance TEXT
                    );
                    CREATE TABLE run_errors (
                        event_id TEXT PRIMARY KEY,
                        run_id TEXT NOT NULL,
                        occurred_at_ms INTEGER NOT NULL,
                        level TEXT NOT NULL,
                        category TEXT NOT NULL,
                        message_hash TEXT NOT NULL,
                        context_json TEXT,
                        FOREIGN KEY(run_id) REFERENCES run_log(run_id) ON DELETE CASCADE
                    );",
                )
                .expect("create v2 schema");
            connection
                .pragma_update(None, "user_version", 2_i64)
                .expect("stamp v2");
            connection
                .execute(
                    "INSERT INTO run_log
                        (run_id, started_at_ms, command, workspace_hash, cli_version,
                         schema_versions, exit_status, result_count, graph_availability,
                         build_provenance)
                     VALUES ('v2-1', ?1, 'index', 'hash', '2.0.0', '{}', 'success', 7,
                             'available', 'release')",
                    params![now_ms()],
                )
                .expect("seed v2 row");
            connection
                .execute(
                    "INSERT INTO run_errors
                        (event_id, run_id, occurred_at_ms, level, category, message_hash,
                         context_json)
                     VALUES ('v2-error', 'v2-1', ?1, 'ERROR', 'legacy', 'hash', '{}')",
                    params![now_ms()],
                )
                .expect("seed v2 error");
        }

        let store = TelemetryStore::open(&root).expect("open migrates v2 to v3");
        let runs = store
            .list_runs_all_workspaces(10, None, None, false, None, None, None, None)
            .expect("list");
        let record = runs
            .iter()
            .find(|record| record.run_id == "v2-1")
            .expect("v2 row preserved");
        assert_eq!(record.result_count, Some(7));
        assert_eq!(record.graph_availability.as_deref(), Some("available"));
        assert_eq!(record.build_provenance.as_deref(), Some("release"));
        assert_eq!(record.binary_path, None);
        assert_eq!(record.build_sha, None);
        drop(store);

        let connection = Connection::open(&db_path).expect("reopen raw");
        let stored_version: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("read user_version");
        assert_eq!(stored_version, TELEMETRY_SCHEMA_VERSION);
        let columns = |table: &str| -> Vec<String> {
            let mut statement = connection
                .prepare(&format!("PRAGMA table_info({table})"))
                .expect("table info");
            statement
                .query_map([], |row| row.get::<_, String>(1))
                .expect("query columns")
                .collect::<Result<Vec<_>, _>>()
                .expect("collect columns")
        };
        let run_log_columns = columns("run_log");
        for column in [
            "process_id",
            "process_start_token",
            "heartbeat_at_ms",
            "binary_path",
            "build_sha",
        ] {
            assert!(
                run_log_columns.iter().any(|existing| existing == column),
                "run_log is missing v3 column {column}"
            );
        }
        assert!(
            columns("run_errors")
                .iter()
                .any(|existing| existing == "message_excerpt"),
            "run_errors is missing v3 column message_excerpt"
        );
        let (category, excerpt): (String, Option<String>) = connection
            .query_row(
                "SELECT category, message_excerpt FROM run_errors WHERE event_id = 'v2-error'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("v2 error row preserved");
        assert_eq!(category, "legacy");
        assert_eq!(excerpt, None);
    }

    #[test]
    fn begin_run_finalizes_stale_rows_for_its_workspace() {
        let root = temp_root("begin-stale");
        let store = TelemetryStore::open(&root).expect("open");
        let workspace = Path::new("/workspace/begin-stale");
        let hash = store.workspace_hash(workspace);
        {
            let connection = Connection::open(store.path()).expect("open raw");
            connection
                .execute(
                    "INSERT INTO run_log
                        (run_id, started_at_ms, command, workspace_hash, cli_version,
                         schema_versions, exit_status)
                     VALUES ('stale-run', ?1, 'index', ?2, '1.0.0', '{}', 'running')",
                    params![now_ms() - STALE_RUNNING_THRESHOLD_MS - 1_000, hash],
                )
                .expect("seed stale row");
        }

        store
            .begin_run("index", workspace, "9.9.9", "release", &schema_versions())
            .expect("begin");
        let runs = store
            .list_runs(workspace, 10, None, None, false, None, None, None, None)
            .expect("list");
        let stale = runs
            .iter()
            .find(|record| record.run_id == "stale-run")
            .expect("stale row present");
        assert_eq!(stale.exit_status, "abandoned");
    }

    #[test]
    fn alive_process_token_semantics_are_pinned() {
        assert_eq!(
            alive_process_identity(Some("token"), Some("token")),
            ProcessIdentity::Same
        );
        assert_eq!(
            alive_process_identity(Some("token"), Some("other")),
            ProcessIdentity::Reused
        );
        assert_eq!(
            alive_process_identity(Some("token"), None),
            ProcessIdentity::Unknown
        );
        assert_eq!(
            alive_process_identity(None, Some("token")),
            ProcessIdentity::Unknown
        );
        assert_eq!(alive_process_identity(None, None), ProcessIdentity::Unknown);
    }

    #[test]
    fn redaction_covers_secrets_urls_and_embedded_paths() {
        let secret = redact_message_excerpt("request failed: API_TOKEN=s3cr3t retrying");
        assert!(!secret.contains("s3cr3t"), "{secret}");
        assert!(secret.contains("API_TOKEN=<redacted>"), "{secret}");

        let password = redact_message_excerpt("db password: hunter2 rejected");
        assert!(!password.contains("hunter2"), "{password}");

        let authorization = redact_message_excerpt("Authorization: Bearer bearer-secret rejected");
        assert!(!authorization.contains("bearer-secret"), "{authorization}");

        let flag = redact_message_excerpt("retry with --api-token flag-secret");
        assert!(!flag.contains("flag-secret"), "{flag}");

        let url = redact_message_excerpt("fetch https://user:hunter2@example.com/callback failed");
        assert!(!url.contains("hunter2"), "{url}");
        assert!(url.contains("https://<redacted>@example.com"), "{url}");

        let embedded_path = redact_message_excerpt("opening /private/tmp/gs-1/config.yaml");
        assert!(!embedded_path.contains("/private"), "{embedded_path}");
        assert!(embedded_path.contains("opening <path>"), "{embedded_path}");

        let multiple_paths = redact_message_excerpt("copy /var/a.txt to /opt/b.txt done");
        assert!(!multiple_paths.contains("/var"), "{multiple_paths}");
        assert!(!multiple_paths.contains("/opt"), "{multiple_paths}");
        assert_eq!(multiple_paths.matches("<path>").count(), 2);

        let windows_path = redact_message_excerpt("read C:\\Users\\jj\\notes.txt failed");
        assert!(!windows_path.contains("Users"), "{windows_path}");
        assert!(windows_path.contains("<path>"), "{windows_path}");

        let truncated = redact_message_excerpt(&"x".repeat(500));
        assert_eq!(truncated.chars().count(), 240);

        let context = redact_json_value(&serde_json::json!({
            "api_token": "json-secret",
            "nested": { "Authorization": "Bearer nested-secret" },
            "keyboard_layout": "dvorak",
            "attempt": 2
        }));
        let encoded = context.to_string();
        assert!(!encoded.contains("json-secret"), "{encoded}");
        assert!(!encoded.contains("nested-secret"), "{encoded}");
        assert_eq!(context["api_token"], "<redacted>");
        assert_eq!(context["nested"]["Authorization"], "<redacted>");
        assert_eq!(context["keyboard_layout"], "dvorak");
        assert_eq!(context["attempt"], 2);
    }

    #[test]
    fn busy_retries_fail_open_within_budget() {
        let root = temp_root("busy-budget");
        let store = TelemetryStore::open(&root).expect("open");
        let run = store
            .begin_run(
                "index",
                Path::new("/ws"),
                "9.9.9",
                "release",
                &schema_versions(),
            )
            .expect("begin");

        let blocker = Connection::open(store.path()).expect("open blocker");
        blocker
            .execute_batch("BEGIN IMMEDIATE")
            .expect("hold write lock");

        let started = Instant::now();
        let result = store.heartbeat(&run);
        let elapsed = started.elapsed();
        blocker.execute_batch("COMMIT").expect("release write lock");

        assert!(
            result.is_err(),
            "heartbeat must fail open while the db is write-locked"
        );
        // Budget (2s) + one in-flight busy_timeout (500ms) + headroom, far
        // below the ~11s the unbounded retry loop could previously stall.
        assert!(
            elapsed < Duration::from_secs(5),
            "busy retries exceeded their wall-clock budget: {elapsed:?}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn store_permissions_are_tightened_on_open() {
        use std::os::unix::fs::PermissionsExt as _;
        let root = temp_root("perms");
        let store = TelemetryStore::open(&root).expect("open");
        let run = store
            .begin_run(
                "index",
                Path::new("/ws"),
                "9.9.9",
                "release",
                &schema_versions(),
            )
            .expect("begin");
        store
            .finish_run(
                &run,
                &TelemetryRunFinish {
                    exit_status: "success".to_owned(),
                    ..TelemetryRunFinish::default()
                },
            )
            .expect("finish");
        let db_path = store.path().to_path_buf();
        let identity_path = root.join(TELEMETRY_IDENTITY_KEY_NAME);
        drop(store);

        // Loosen a pre-existing store to prove reopening tightens it.
        fs::set_permissions(&root, fs::Permissions::from_mode(0o755)).expect("loosen dir");
        fs::set_permissions(&db_path, fs::Permissions::from_mode(0o644)).expect("loosen db");
        fs::set_permissions(&identity_path, fs::Permissions::from_mode(0o644))
            .expect("loosen identity key");
        TelemetryStore::open(&root).expect("reopen");

        let mode = |path: &Path| {
            fs::metadata(path)
                .map(|metadata| metadata.permissions().mode() & 0o777)
                .ok()
        };
        assert_eq!(mode(&root), Some(0o700));
        assert_eq!(mode(&db_path), Some(0o600));
        assert_eq!(mode(&identity_path), Some(0o600));
        for suffix in ["-wal", "-shm"] {
            let sidecar = PathBuf::from(format!("{}{suffix}", db_path.display()));
            if let Some(sidecar_mode) = mode(&sidecar) {
                assert_eq!(
                    sidecar_mode,
                    0o600,
                    "{} is too permissive",
                    sidecar.display()
                );
            }
        }
    }
}
