//! Fresh generated-state schema-version behavior.

use std::{
    env, fs,
    path::PathBuf,
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use gather_step_storage::{FileIndexState, GraphStoreDb, MetadataStore, MetadataStoreDb};
use redb::{ReadableDatabase, TableDefinition};
use rusqlite::Connection;

const GRAPH_SCHEMA: TableDefinition<&str, u32> = TableDefinition::new("graph_schema");
const GRAPH_SCHEMA_VERSION_KEY: &str = "version";

fn temp_db_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
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

    MetadataStoreDb::open(&fresh_path).expect("fresh store should open");

    let conn = Connection::open(&fresh_path).expect("metadata db should reopen");
    let version: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("user_version should read");
    assert_eq!(version, 0);
}

#[test]
fn fresh_schema_stamps_graph_version_zero() {
    let graph_path = temp_db_path("fresh-graph-schema");
    let _cleanup = Cleanup(graph_path.clone());

    let store = GraphStoreDb::open(&graph_path).expect("fresh graph store should open");
    drop(store);

    let db = redb::Database::open(&graph_path).expect("graph db should reopen");
    let read_txn = db.begin_read().expect("read txn should begin");
    let schema = read_txn
        .open_table(GRAPH_SCHEMA)
        .expect("graph schema table should exist");
    let version = schema
        .get(GRAPH_SCHEMA_VERSION_KEY)
        .expect("graph schema version should read")
        .expect("graph schema version should be stamped")
        .value();
    assert_eq!(version, 0);
}

#[test]
fn fresh_schema_supports_metadata_round_trip() {
    let fresh_path = temp_db_path("fresh-schema");
    let _cleanup = Cleanup(fresh_path.clone());

    let store = MetadataStoreDb::open(&fresh_path).expect("fresh store should open");
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
        .expect("write file state");
    assert!(
        !store
            .should_reindex("svc-a", "src/current.ts", &[1, 2, 3, 4])
            .expect("matching hash should be readable")
    );
    assert!(
        store
            .should_reindex("svc-a", "src/current.ts", &[9, 9, 9, 9])
            .expect("mismatched hash should be readable")
    );
}
