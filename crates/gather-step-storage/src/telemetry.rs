use std::{
    fs,
    path::{Path, PathBuf},
    process,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chrono::Utc;
use rusqlite::{Connection, params};
use serde::Serialize;
use thiserror::Error;

pub const TELEMETRY_DB_NAME: &str = "telemetry.db";
pub const TELEMETRY_SCHEMA_VERSION: i64 = 2;

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
    build_provenance   TEXT
);
CREATE INDEX IF NOT EXISTS idx_run_log_started_at ON run_log(started_at_ms DESC);

CREATE TABLE IF NOT EXISTS run_errors (
    event_id        TEXT PRIMARY KEY,
    run_id          TEXT NOT NULL,
    occurred_at_ms  INTEGER NOT NULL,
    level           TEXT NOT NULL,
    category        TEXT NOT NULL,
    message_hash    TEXT NOT NULL,
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
    /// `not_indexed` / `degraded`. `None` when the run did not touch the graph.
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
                build_provenance, schema_versions, exit_status
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'running')",
            params![
                &run_id,
                now_ms(),
                command,
                hash_text(&workspace_path.display().to_string()),
                cli_version,
                build_provenance,
                schema_versions,
            ],
        )?;
        Ok(TelemetryRun { run_id })
    }

    pub fn finish_run(
        &self,
        run: &TelemetryRun,
        finish: &TelemetryRunFinish,
    ) -> Result<(), TelemetryError> {
        let connection = self.connection()?;
        let ended_at = now_ms();
        let extra_json = finish
            .extra_json
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        connection.execute(
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
            Self::record_error_with_connection(&connection, &run.run_id, event, ended_at)?;
        }
        Ok(())
    }

    pub fn mark_panic(
        &self,
        run: &TelemetryRun,
        category: &str,
        message: &str,
    ) -> Result<(), TelemetryError> {
        let connection = self.connection()?;
        let ended_at = now_ms();
        connection.execute(
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
        Self::record_error_with_connection(&connection, &run.run_id, &event, ended_at)
    }

    pub fn list_runs(
        &self,
        limit: usize,
        since_ms: Option<i64>,
        errors_only: bool,
    ) -> Result<Vec<TelemetryRunRecord>, TelemetryError> {
        let connection = self.connection()?;
        let mut sql = String::from(
            "SELECT run_id, started_at_ms, ended_at_ms, command, exit_status,
                    duration_ms, peak_rss_bytes, warn_count, error_count, recovery_event,
                    cli_version, result_count, graph_availability, build_provenance
             FROM run_log",
        );
        let mut clauses = Vec::new();
        if since_ms.is_some() {
            clauses.push("started_at_ms >= ?");
        }
        if errors_only {
            clauses.push(
                "(exit_status NOT IN ('success', 'review_threshold_exceeded') OR error_count > 0)",
            );
        }
        if !clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&clauses.join(" AND "));
        }
        sql.push_str(" ORDER BY started_at_ms DESC LIMIT ?");
        let limit_param = i64::try_from(limit).unwrap_or(i64::MAX);
        let mut statement = connection.prepare(&sql)?;
        let rows = if let Some(since_ms) = since_ms {
            statement.query_map(params![since_ms, limit_param], telemetry_run_from_row)?
        } else {
            statement.query_map(params![limit_param], telemetry_run_from_row)?
        };
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(TelemetryError::Sqlite)
    }

    pub fn clear_before(&self, cutoff_ms: i64) -> Result<usize, TelemetryError> {
        let connection = self.connection()?;
        let deleted = connection.execute(
            "DELETE FROM run_log WHERE started_at_ms < ?1",
            params![cutoff_ms],
        )?;
        Ok(deleted)
    }

    fn initialize(&self) -> Result<(), TelemetryError> {
        let connection = self.connection()?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.execute_batch(SCHEMA)?;
        migrate_schema(&connection)?;
        connection.pragma_update(None, "user_version", TELEMETRY_SCHEMA_VERSION)?;
        finalize_stale_running(&connection, now_ms())?;
        prune_old_rows(&connection)?;
        Ok(())
    }

    /// Finalize every `running` row older than [`STALE_RUNNING_THRESHOLD_MS`]
    /// as `abandoned`, returning how many were rewritten. Runs automatically on
    /// [`TelemetryStore::open`]; exposed for an explicit `gather-step log
    /// --repair`.
    pub fn repair_stale_running(&self) -> Result<usize, TelemetryError> {
        let connection = self.connection()?;
        finalize_stale_running(&connection, now_ms())
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
            .map(serde_json::to_string)
            .transpose()?;
        connection.execute(
            "INSERT INTO run_errors (
                event_id, run_id, occurred_at_ms, level, category, message_hash, context_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                generate_event_id(),
                run_id,
                occurred_at_ms,
                event.level.as_str(),
                event.category.as_str(),
                hash_text(&event.message),
                context_json,
            ],
        )?;
        Ok(())
    }
}

fn telemetry_run_from_row(row: &rusqlite::Row<'_>) -> Result<TelemetryRunRecord, rusqlite::Error> {
    let peak_rss_bytes: Option<i64> = row.get(6)?;
    let warn_count: i64 = row.get(7)?;
    let error_count: i64 = row.get(8)?;
    let recovery_event: i64 = row.get(9)?;
    Ok(TelemetryRunRecord {
        run_id: row.get(0)?,
        started_at_ms: row.get(1)?,
        ended_at_ms: row.get(2)?,
        command: row.get(3)?,
        cli_version: row.get(10)?,
        exit_status: row.get(4)?,
        duration_ms: row.get(5)?,
        peak_rss_bytes: peak_rss_bytes.and_then(i64_to_u64),
        warn_count: u32::try_from(warn_count).unwrap_or(u32::MAX),
        error_count: u32::try_from(error_count).unwrap_or(u32::MAX),
        recovery_event: recovery_event != 0,
        result_count: row.get(11)?,
        graph_availability: row.get(12)?,
        build_provenance: row.get(13)?,
    })
}

/// Bring a pre-existing telemetry database up to the current column shape.
///
/// The base `SCHEMA` only creates missing tables (`CREATE TABLE IF NOT
/// EXISTS`), so a database created by an older gather-step keeps its original
/// columns. Each `ADD COLUMN` here is idempotent: on a fresh database the
/// column already exists and SQLite reports a duplicate-column error, which we
/// treat as success.
fn migrate_schema(connection: &Connection) -> Result<(), TelemetryError> {
    const ADDED_COLUMNS: [&str; 3] = [
        "ALTER TABLE run_log ADD COLUMN result_count INTEGER",
        "ALTER TABLE run_log ADD COLUMN graph_availability TEXT",
        "ALTER TABLE run_log ADD COLUMN build_provenance TEXT",
    ];
    for statement in ADDED_COLUMNS {
        match connection.execute(statement, []) {
            Ok(_) => {}
            Err(rusqlite::Error::SqliteFailure(_, Some(message)))
                if message.contains("duplicate column name") => {}
            Err(error) => return Err(TelemetryError::Sqlite(error)),
        }
    }
    Ok(())
}

/// Rewrite `running` rows older than the stale threshold as `abandoned`.
/// `ended_at_ms`/`duration_ms` are set to a lower-bound estimate (the threshold
/// itself) since the true end time is unknown. Returns the number rewritten.
fn finalize_stale_running(connection: &Connection, now: i64) -> Result<usize, TelemetryError> {
    let cutoff = now.saturating_sub(STALE_RUNNING_THRESHOLD_MS);
    let updated = connection.execute(
        "UPDATE run_log
         SET exit_status = 'abandoned',
             ended_at_ms = started_at_ms + ?1,
             duration_ms = ?1
         WHERE exit_status = 'running' AND started_at_ms < ?2",
        params![STALE_RUNNING_THRESHOLD_MS, cutoff],
    )?;
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
         WHERE run_id NOT IN (
             SELECT run_id FROM run_log ORDER BY started_at_ms DESC LIMIT ?1
         )",
        params![MAX_RUN_ROWS],
    )?;
    Ok(())
}

fn generate_run_id() -> String {
    let now = Utc::now();
    let entropy = entropy24();
    format!("{}-{entropy:06x}", now.format("%Y%m%d-%H%M%S"))
}

fn generate_event_id() -> String {
    format!("{}-{:06x}", generate_run_id(), entropy24())
}

fn entropy24() -> u64 {
    (u64::try_from(now_ms()).unwrap_or(0) ^ u64::from(process::id())) & 0x00ff_ffff
}

fn now_ms() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis());
    i64::try_from(millis).unwrap_or(i64::MAX)
}

fn hash_text(value: &str) -> String {
    blake3::hash(value.as_bytes()).to_hex().to_string()
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
        let dir = std::env::temp_dir().join(format!(
            "gs-telemetry-{tag}-{}-{id}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp root");
        dir
    }

    fn schema_versions() -> serde_json::Value {
        serde_json::json!({ "telemetry": TELEMETRY_SCHEMA_VERSION })
    }

    #[test]
    fn v2_fields_round_trip() {
        let root = temp_root("roundtrip");
        let store = TelemetryStore::open(&root).expect("open");
        let run = store
            .begin_run("index", Path::new("/ws"), "9.9.9", "release", &schema_versions())
            .expect("begin");
        let finish = TelemetryRunFinish {
            exit_status: "success".to_owned(),
            result_count: Some(42),
            graph_availability: Some("available".to_owned()),
            ..TelemetryRunFinish::default()
        };
        store.finish_run(&run, &finish).expect("finish");

        let runs = store.list_runs(10, None, false).expect("list");
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
        let runs = store.list_runs(10, None, false).expect("list");
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
        }

        let store = TelemetryStore::open(&root).expect("reopen sweeps");
        let runs = store.list_runs(10, None, false).expect("list");
        let status = |id: &str| {
            runs.iter()
                .find(|record| record.run_id == id)
                .map(|record| record.exit_status.clone())
        };
        assert_eq!(status("stale").as_deref(), Some("abandoned"));
        assert_eq!(status("fresh").as_deref(), Some("running"));

        // Already finalized: an explicit repair now rewrites nothing.
        assert_eq!(store.repair_stale_running().expect("repair"), 0);
    }
}
