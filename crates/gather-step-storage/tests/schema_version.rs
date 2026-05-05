//! Fresh generated-state schema-version behavior.

use std::{
    env, fs,
    path::PathBuf,
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use gather_step_storage::{
    FileIndexState, GraphStoreDb, GraphStoreError, MetadataStore, MetadataStoreDb,
    MetadataStoreError,
};
use redb::{ReadableDatabase, TableDefinition};
use rusqlite::Connection;

const GRAPH_SCHEMA: TableDefinition<&str, u32> = TableDefinition::new("graph_schema");
const GRAPH_SCHEMA_VERSION_KEY: &str = "version";

fn temp_db_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("The system clock should be after the Unix epoch.")
        .as_nanos();
    env::temp_dir().join(format!(
        "gather-step-{label}-{}-{nanos}.sqlite",
        process::id()
    ))
}

struct Cleanup(PathBuf);

impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
        let _ = fs::remove_file(self.0.with_extension("sqlite-wal"));
        let _ = fs::remove_file(self.0.with_extension("sqlite-shm"));
    }
}

#[test]
fn fresh_schema_stamps_metadata_user_version_zero() {
    let fresh_path = temp_db_path("fresh-schema-version");
    let _cleanup = Cleanup(fresh_path.clone());

    MetadataStoreDb::open(&fresh_path).expect("The fresh metadata store should open.");

    let conn = Connection::open(&fresh_path).expect("The metadata database should reopen.");
    let version: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("The user_version pragma should read.");
    assert_eq!(version, 0);
}

#[test]
fn metadata_store_rejects_future_user_version_with_mismatch_error() {
    let metadata_path = temp_db_path("future-metadata-schema");
    let _cleanup = Cleanup(metadata_path.clone());

    {
        let conn = Connection::open(&metadata_path).expect("The metadata database should create.");
        conn.pragma_update(None, "user_version", 99_i64)
            .expect("The future user_version pragma should stamp.");
    }

    let err = MetadataStoreDb::open(&metadata_path)
        .err()
        .expect("Opening a future-version metadata store must fail.");
    assert!(
        matches!(err, MetadataStoreError::SchemaVersionMismatch { .. }),
        "Expected SchemaVersionMismatch, got {err:?}."
    );
}

#[test]
fn fresh_schema_stamps_graph_version_zero() {
    let graph_path = temp_db_path("fresh-graph-schema");
    let _cleanup = Cleanup(graph_path.clone());

    let store = GraphStoreDb::open(&graph_path).expect("The fresh graph store should open.");
    drop(store);

    let db = redb::Database::open(&graph_path).expect("The graph database should reopen.");
    let read_txn = db.begin_read().expect("The read transaction should begin.");
    let schema = read_txn
        .open_table(GRAPH_SCHEMA)
        .expect("The graph schema table should exist.");
    let version = schema
        .get(GRAPH_SCHEMA_VERSION_KEY)
        .expect("The graph schema version should read.")
        .expect("The graph schema version should be stamped.")
        .value();
    assert_eq!(version, 0);
}

/// Open-time enforcement: a graph store stamped with a future schema
/// version must reject the open with `SchemaVersionMismatch` so the
/// CLI's friendly recovery hint (`gather-step index --auto-recover`) gets
/// a typed error to map. Previously only the formatter-side mapping was
/// covered; without this guard, a regression in `validate_schema_version`
/// could silently allow incompatible stores to open.
#[test]
fn graph_store_rejects_future_schema_version_with_mismatch_error() {
    let graph_path = temp_db_path("future-graph-schema");
    let _cleanup = Cleanup(graph_path.clone());

    // First, create a valid v0 store so the redb file exists with the
    // expected layout, then bump the version stamp to a value the current
    // build cannot understand.
    {
        let store = GraphStoreDb::open(&graph_path).expect("The fresh graph store should open.");
        drop(store);
    }
    {
        let db = redb::Database::open(&graph_path).expect("The graph database should reopen.");
        let write_txn = db
            .begin_write()
            .expect("The write transaction should begin.");
        {
            let mut table = write_txn
                .open_table(GRAPH_SCHEMA)
                .expect("The graph schema table should exist.");
            table
                .insert(GRAPH_SCHEMA_VERSION_KEY, 99_u32)
                .expect("The schema version should bump to 99.");
        }
        write_txn.commit().expect("The version bump should commit.");
    }

    let err = GraphStoreDb::open(&graph_path)
        .err()
        .expect("Opening a future-version store must fail.");
    assert!(
        matches!(err, GraphStoreError::SchemaVersionMismatch { .. }),
        "Expected SchemaVersionMismatch, got {err:?}."
    );
}

#[test]
fn graph_store_accepts_missing_schema_table_as_implicit_v0() {
    let graph_path = temp_db_path("implicit-v0-missing-table");
    let _cleanup = Cleanup(graph_path.clone());

    {
        let db = redb::Database::create(&graph_path).expect("The graph database should create.");
        let write_txn = db
            .begin_write()
            .expect("The write transaction should begin.");
        write_txn
            .commit()
            .expect("The empty graph database should commit.");
    }

    let store = GraphStoreDb::open(&graph_path)
        .expect("The unstamped graph store should open as implicit v0.");
    drop(store);
}

#[test]
fn graph_store_accepts_missing_schema_version_row_as_implicit_v0() {
    let graph_path = temp_db_path("implicit-v0-missing-row");
    let _cleanup = Cleanup(graph_path.clone());

    {
        let db = redb::Database::create(&graph_path).expect("The graph database should create.");
        let write_txn = db
            .begin_write()
            .expect("The write transaction should begin.");
        {
            let _schema = write_txn
                .open_table(GRAPH_SCHEMA)
                .expect("The graph schema table should create.");
        }
        write_txn.commit().expect("The schema table should commit.");
    }

    let store = GraphStoreDb::open(&graph_path)
        .expect("The graph store without a version row should open as implicit v0.");
    drop(store);
}

#[test]
fn fresh_schema_supports_metadata_round_trip() {
    let fresh_path = temp_db_path("fresh-schema");
    let _cleanup = Cleanup(fresh_path.clone());

    let store = MetadataStoreDb::open(&fresh_path).expect("The fresh metadata store should open.");
    store
        .upsert_file_state(&FileIndexState {
            repo: "svc-a".to_owned(),
            file_path: "src/current.ts".to_owned(),
            content_hash: vec![1, 2, 3, 4],
            node_count: 3,
            edge_count: 2,
            indexed_at: 1_713_000_001,
            parse_ms: Some(7),
            ..Default::default()
        })
        .expect("The file state should write.");
    assert!(
        !store
            .should_reindex("svc-a", "src/current.ts", &[1, 2, 3, 4])
            .expect("The matching hash should be readable.")
    );
    assert!(
        store
            .should_reindex("svc-a", "src/current.ts", &[9, 9, 9, 9])
            .expect("The mismatched hash should be readable.")
    );
}
