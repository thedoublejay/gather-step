use std::{
    fs,
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, params};
use serde::Serialize;
use thiserror::Error;

pub const TELEMETRY_DB_NAME: &str = "telemetry.db";
pub const TELEMETRY_SCHEMA_VERSION: i64 = 3;

const MAX_RUN_ROWS: i64 = 10_000;
const RETENTION_DAYS: i64 = 90;
/// A `running` row older than this is assumed to belong to a process that died
/// without writing its finish row (crash, `kill -9`, power loss) and is
/// finalized as `abandoned` so the dashboard stops counting it as in-flight.
const STALE_RUNNING_THRESHOLD_MS: i64 = 6 * 60 * 60 * 1000;

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
}

#[derive(Clone, Debug)]
pub struct TelemetryRun {
    pub run_id: String,
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
    pub extra_json: Option<serde_json::Value>,
    /// Command-specific result magnitude (e.g. dependency count, trace hops,
    /// search hits) for the `log --summary` view. `None` when not applicable.
    pub result_count: Option<i64>,
    /// Graph availability observed for this run: `available` / `locked` /
    /// `missing` / `corrupt` / `not_applicable`. `None` means unrecorded.
    pub graph_availability: Option<String>,
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
    pub repo_count: Option<i64>,
    pub files_parsed: Option<i64>,
    pub nodes_created: Option<i64>,
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
        let store = Self { path };
        store.initialize()?;
        Ok(store)
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn begin_run(
        &self,
        command: &str,
        workspace_path: &Path,
        cli_version: &str,
        build_provenance: &str,
        schema_versions: &serde_json::Value,
    ) -> Result<TelemetryRun, TelemetryError> {
        let connection = self.connection()?;
        let run_id = generate_run_id();
        let schema_versions = serde_json::to_string(schema_versions)?;
        connection.execute(
            "INSERT INTO run_log (
                run_id, started_at_ms, command, workspace_hash, cli_version,
                build_provenance, schema_versions, exit_status, process_id,
                process_start_token, heartbeat_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'running', ?8, ?9, ?10)",
            params![
                &run_id,
                now_ms(),
                command,
                workspace_hash(workspace_path),
                cli_version,
                build_provenance,
                schema_versions,
                i64::from(process::id()),
                process_start_token(process::id()),
                now_ms(),
            ],
        )?;
        Ok(TelemetryRun { run_id })
    }

    pub fn finish_run(
        &self,
        run: &TelemetryRun,
        finish: &TelemetryRunFinish,
    ) -> Result<(), TelemetryError> {
        let mut connection = self.connection()?;
        let ended_at = now_ms();
        let extra_json = finish
            .extra_json
            .as_ref()
            .map(redact_json_value)
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let transaction = connection.transaction()?;
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
                finish.exit_status,
                finish.peak_rss_bytes.and_then(u64_to_i64),
                finish.repo_count,
                finish.files_parsed,
                finish.nodes_created,
                finish.warn_count,
                finish.error_count,
                i64::from(finish.recovery_event),
                extra_json,
                finish.result_count,
                finish.graph_availability,
                &run.run_id,
            ],
        )?;
        if let Some(event) = &finish.error {
            Self::record_error_with_connection(&transaction, &run.run_id, event, ended_at)?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn mark_panic(
        &self,
        run: &TelemetryRun,
        category: &str,
        message: &str,
    ) -> Result<(), TelemetryError> {
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
        let event = TelemetryErrorEvent {
            level: "PANIC".to_owned(),
            category: category.to_owned(),
            message: message.to_owned(),
            context_json: None,
        };
        Self::record_error_with_connection(&transaction, &run.run_id, &event, ended_at)?;
        transaction.commit()?;
        Ok(())
    }

    /// Refresh the liveness timestamp for a long-running command.
    pub fn heartbeat(&self, run: &TelemetryRun) -> Result<(), TelemetryError> {
        let connection = self.connection()?;
        connection.execute(
            "UPDATE run_log SET heartbeat_at_ms = ?1 WHERE run_id = ?2 AND exit_status = 'running'",
            params![now_ms(), &run.run_id],
        )?;
        Ok(())
    }

    pub fn list_runs(
        &self,
        workspace_path: &Path,
        limit: usize,
        since_ms: Option<i64>,
        errors_only: bool,
        command: Option<&str>,
        exclude_run_id: Option<&str>,
    ) -> Result<Vec<TelemetryRunRecord>, TelemetryError> {
        self.list_runs_inner(
            Some(workspace_hash(workspace_path)),
            limit,
            since_ms,
            errors_only,
            command,
            exclude_run_id,
        )
    }

    pub fn list_runs_all_workspaces(
        &self,
        limit: usize,
        since_ms: Option<i64>,
        errors_only: bool,
        command: Option<&str>,
        exclude_run_id: Option<&str>,
    ) -> Result<Vec<TelemetryRunRecord>, TelemetryError> {
        self.list_runs_inner(None, limit, since_ms, errors_only, command, exclude_run_id)
    }

    fn list_runs_inner(
        &self,
        workspace_hash_filter: Option<String>,
        limit: usize,
        since_ms: Option<i64>,
        errors_only: bool,
        command: Option<&str>,
        exclude_run_id: Option<&str>,
    ) -> Result<Vec<TelemetryRunRecord>, TelemetryError> {
        let connection = self.connection()?;
        let mut sql = String::from(
            "SELECT run_id, workspace_hash, started_at_ms, ended_at_ms, command, exit_status,
                    duration_ms, peak_rss_bytes, warn_count, error_count, recovery_event,
                    cli_version, result_count, graph_availability, build_provenance,
                    repo_count, files_parsed, nodes_created
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
        if errors_only {
            clauses.push(
                "(exit_status NOT IN ('success', 'review_threshold_exceeded') OR error_count > 0)",
            );
        }
        if let Some(command) = command {
            clauses.push("command = ?");
            values.push(command.to_owned().into());
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
        let connection = self.connection()?;
        let deleted = connection.execute(
            "DELETE FROM run_log WHERE workspace_hash = ?1 AND started_at_ms < ?2",
            params![workspace_hash(workspace_path), cutoff_ms],
        )?;
        Ok(deleted)
    }

    pub fn clear_before_all_workspaces(&self, cutoff_ms: i64) -> Result<usize, TelemetryError> {
        let connection = self.connection()?;
        let deleted = connection.execute(
            "DELETE FROM run_log WHERE started_at_ms < ?1",
            params![cutoff_ms],
        )?;
        Ok(deleted)
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
        let hash = workspace_hash(workspace_path);
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
        let connection = self.connection()?;
        let stored_version: i64 =
            connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if stored_version > TELEMETRY_SCHEMA_VERSION {
            return Err(TelemetryError::UnsupportedSchemaVersion {
                stored: stored_version,
                supported: TELEMETRY_SCHEMA_VERSION,
            });
        }
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.execute_batch(SCHEMA)?;
        migrate_schema(&connection, stored_version)?;
        connection.pragma_update(None, "user_version", TELEMETRY_SCHEMA_VERSION)?;
        finalize_stale_running(&connection, now_ms(), None)?;
        prune_old_rows(&connection)?;
        Ok(())
    }

    /// Finalize every `running` row older than [`STALE_RUNNING_THRESHOLD_MS`]
    /// as `abandoned`, returning how many were rewritten. Runs automatically on
    /// [`TelemetryStore::open`]; exposed for an explicit `gather-step log
    /// --repair`.
    pub fn repair_stale_running(&self, workspace_path: &Path) -> Result<usize, TelemetryError> {
        let connection = self.connection()?;
        finalize_stale_running(&connection, now_ms(), Some(workspace_hash(workspace_path)))
    }

    pub fn repair_stale_running_all_workspaces(&self) -> Result<usize, TelemetryError> {
        let connection = self.connection()?;
        finalize_stale_running(&connection, now_ms(), None)
    }

    fn connection(&self) -> Result<Connection, TelemetryError> {
        let connection = Connection::open(&self.path)?;
        connection.busy_timeout(Duration::from_millis(500))?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        Ok(connection)
    }

    fn record_error_with_connection(
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
                hash_text(&event.message),
                redact_message_excerpt(&event.message),
                context_json,
            ],
        )?;
        Ok(())
    }
}

fn telemetry_run_from_row(row: &rusqlite::Row<'_>) -> Result<TelemetryRunRecord, rusqlite::Error> {
    let peak_rss_bytes: Option<i64> = row.get(7)?;
    let warn_count: i64 = row.get(8)?;
    let error_count: i64 = row.get(9)?;
    let recovery_event: i64 = row.get(10)?;
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
        repo_count: row.get(15)?,
        files_parsed: row.get(16)?,
        nodes_created: row.get(17)?,
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

/// Rewrite `running` rows older than the stale threshold as `abandoned`.
/// The true end time of an abandoned process is unknown, so duration fields
/// remain NULL. Live processes with a matching PID/start token are preserved.
fn finalize_stale_running(
    connection: &Connection,
    now: i64,
    workspace_hash_filter: Option<String>,
) -> Result<usize, TelemetryError> {
    let cutoff = now.saturating_sub(STALE_RUNNING_THRESHOLD_MS);
    let candidates = {
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
        if let Some(hash) = workspace_hash_filter {
            statement
                .query_map(params![cutoff, hash], map)?
                .collect::<Result<Vec<_>, _>>()?
        } else {
            statement
                .query_map(params![cutoff], map)?
                .collect::<Result<Vec<_>, _>>()?
        }
    };
    let mut updated = 0;
    for (run_id, pid, start_token) in candidates {
        if pid.is_some_and(|pid| process_is_same(pid, start_token.as_deref())) {
            continue;
        }
        updated += connection.execute(
            "UPDATE run_log
             SET exit_status = 'abandoned', ended_at_ms = NULL, duration_ms = NULL
             WHERE run_id = ?1 AND exit_status = 'running'",
            params![run_id],
        )?;
    }
    Ok(updated)
}

fn prune_old_rows(connection: &Connection) -> Result<(), TelemetryError> {
    let cutoff_ms = now_ms().saturating_sub(RETENTION_DAYS * 24 * 60 * 60 * 1000);
    connection.execute(
        "DELETE FROM run_log WHERE started_at_ms < ?1",
        params![cutoff_ms],
    )?;
    connection.execute(
        "DELETE FROM run_log
         WHERE run_id IN (
             SELECT run_id FROM (
                 SELECT run_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY workspace_hash ORDER BY started_at_ms DESC
                        ) AS workspace_row
                 FROM run_log
             ) WHERE workspace_row > ?1
         )",
        params![MAX_RUN_ROWS],
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

#[must_use]
pub fn workspace_hash(path: &Path) -> String {
    hash_text(&path.display().to_string())
}

fn redact_message_excerpt(message: &str) -> String {
    let redacted = message
        .split_whitespace()
        .map(|token| {
            if token.starts_with('/') || token.get(1..3) == Some(":\\") {
                "<path>"
            } else {
                token
            }
        })
        .collect::<Vec<_>>()
        .join(" ");
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
                .map(|(key, value)| (key.clone(), redact_json_value(value)))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn process_start_token(pid: u32) -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let after_name = stat.rsplit_once(')')?.1;
        return after_name.split_whitespace().nth(19).map(ToOwned::to_owned);
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
        None
    }
}

fn process_is_same(pid: u32, expected_start_token: Option<&str>) -> bool {
    if !process_is_alive(pid) {
        return false;
    }
    match (expected_start_token, process_start_token(pid)) {
        (Some(expected), Some(actual)) => expected == actual,
        _ => true,
    }
}

#[cfg(target_os = "linux")]
fn process_is_alive(pid: u32) -> bool {
    Path::new("/proc").join(pid.to_string()).exists()
}

#[cfg(all(unix, not(target_os = "linux")))]
fn process_is_alive(pid: u32) -> bool {
    process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .status()
        .is_ok_and(|status| status.success())
}

#[cfg(not(unix))]
fn process_is_alive(_pid: u32) -> bool {
    false
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
            ..TelemetryRunFinish::default()
        };
        store.finish_run(&run, &finish).expect("finish");

        let runs = store
            .list_runs_all_workspaces(10, None, false, None, None)
            .expect("list");
        let record = runs
            .iter()
            .find(|record| record.run_id == run.run_id)
            .expect("row present");
        assert_eq!(record.result_count, Some(42));
        assert_eq!(record.graph_availability.as_deref(), Some("available"));
        assert_eq!(record.build_provenance.as_deref(), Some("release"));
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
            .list_runs_all_workspaces(10, None, false, None, None)
            .expect("list");
        let record = runs
            .iter()
            .find(|record| record.run_id == "old-1")
            .expect("legacy row preserved");
        assert_eq!(record.command, "status");
        assert_eq!(record.result_count, None);
        assert_eq!(record.graph_availability, None);
        assert_eq!(record.build_provenance, None);

        // Reopening runs the idempotent migration again without error.
        drop(store);
        TelemetryStore::open(&root).expect("reopen is idempotent");
    }

    #[test]
    fn stale_running_rows_are_finalized_but_fresh_ones_are_kept() {
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

        let store = TelemetryStore::open(&root).expect("reopen sweeps");
        let runs = store
            .list_runs_all_workspaces(10, None, false, None, None)
            .expect("list");
        let status = |id: &str| {
            runs.iter()
                .find(|record| record.run_id == id)
                .map(|record| record.exit_status.clone())
        };
        assert_eq!(status("stale").as_deref(), Some("abandoned"));
        assert_eq!(status("fresh").as_deref(), Some("running"));
        assert_eq!(status("live").as_deref(), Some("running"));

        // Already finalized: an explicit repair now rewrites nothing.
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
            .list_runs(Path::new("/workspace/alpha"), 10, None, false, None, None)
            .expect("alpha rows");
        let beta = store
            .list_runs(Path::new("/workspace/beta"), 10, None, false, None, None)
            .expect("beta rows");
        assert_eq!(alpha.len(), 1);
        assert_eq!(beta.len(), 1);
        assert_ne!(alpha[0].workspace_hash, beta[0].workspace_hash);

        store
            .clear_before(Path::new("/workspace/alpha"), now_ms() + 1)
            .expect("clear alpha");
        assert!(
            store
                .list_runs(Path::new("/workspace/alpha"), 10, None, false, None, None,)
                .expect("alpha rows")
                .is_empty()
        );
        assert_eq!(
            store
                .list_runs(Path::new("/workspace/beta"), 10, None, false, None, None,)
                .expect("beta rows")
                .len(),
            1
        );
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
                        context_json: None,
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
    }
}
