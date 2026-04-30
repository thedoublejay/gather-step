//! Metadata schema-version behavior.

use std::{
    env, fs,
    path::PathBuf,
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use gather_step_storage::{FileIndexState, GraphStoreDb, MetadataStore, MetadataStoreDb};
use redb::TableDefinition;
use rusqlite::Connection;

const GRAPH_SCHEMA: TableDefinition<&str, u32> = TableDefinition::new("graph_schema");
const GRAPH_SCHEMA_VERSION_KEY: &str = "version";

fn create_unsupported_schema_database(path: &std::path::Path) {
    let conn = Connection::open(path).expect("create metadata db");
    conn.pragma_update(None, "user_version", 99_i64)
        .expect("set unsupported user_version");
}

fn create_unsupported_graph_database(path: &std::path::Path) {
    let db = redb::Database::create(path).expect("create graph db");
    let write_txn = db.begin_write().expect("begin graph write");
    {
        let mut schema = write_txn
            .open_table(GRAPH_SCHEMA)
            .expect("open graph schema table");
        schema
            .insert(GRAPH_SCHEMA_VERSION_KEY, 99_u32)
            .expect("write unsupported graph schema version");
    }
    write_txn.commit().expect("commit graph schema");
}

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
fn unsupported_schema_requires_reindex() {
    let db_path = temp_db_path("schema-version");
    let _cleanup = Cleanup(db_path.clone());

    create_unsupported_schema_database(&db_path);

    let Err(err) = MetadataStoreDb::open(&db_path) else {
        panic!("unsupported schema must fail fast");
    };
    let message = err.to_string();
    assert!(message.contains("unsupported schema"));
    assert!(message.contains("gather-step clean && gather-step index"));
}

#[test]
fn unsupported_graph_schema_requires_reindex() {
    let db_path = temp_db_path("graph-schema-version");
    let _cleanup = Cleanup(db_path.clone());

    create_unsupported_graph_database(&db_path);

    let Err(err) = GraphStoreDb::open(&db_path) else {
        panic!("unsupported graph schema must fail fast");
    };
    let message = err.to_string();
    assert!(message.contains("unsupported schema"));
    assert!(message.contains("gather-step clean --storage && gather-step index"));
}

#[test]
fn unstamped_graph_schema_requires_reindex() {
    let db_path = temp_db_path("graph-unstamped");
    let _cleanup = Cleanup(db_path.clone());
    redb::Database::create(&db_path).expect("create unstamped graph db");

    let Err(err) = GraphStoreDb::open(&db_path) else {
        panic!("unstamped graph schema must fail fast");
    };
    let message = err.to_string();
    assert!(message.contains("unsupported schema"));
    assert!(message.contains("gather-step clean --storage && gather-step index"));
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
