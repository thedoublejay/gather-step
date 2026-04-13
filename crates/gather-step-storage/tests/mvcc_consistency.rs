use std::{
    env, fs,
    path::{Path, PathBuf},
    process,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use gather_step_core::{
    EdgeKind, EdgeMetadata, NodeData, NodeId, NodeKind, SourceSpan, Visibility, node_id,
};
use gather_step_storage::{FileBatch, RepoBatch, RepoBatchHooks, StorageCoordinator};

struct TestRoot {
    path: PathBuf,
}

impl TestRoot {
    fn new(name: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be monotonic enough for tests")
            .as_nanos();
        let path =
            env::temp_dir().join(format!("gather-step-mvcc-{name}-{}-{nanos}", process::id()));
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn file_node(repo: &str, file_path: &str) -> NodeData {
    NodeData {
        id: node_id(repo, file_path, NodeKind::File, file_path),
        kind: NodeKind::File,
        repo: repo.to_owned(),
        file_path: file_path.to_owned(),
        name: file_path.to_owned(),
        qualified_name: Some(format!("{repo}::{file_path}")),
        external_id: None,
        signature: None,
        visibility: None,
        span: Some(SourceSpan {
            line_start: 1,
            line_len: 0,
            column_start: 0,
            column_len: 0,
        }),
        is_virtual: false,
    }
}

fn function_node(repo: &str, file_path: &str, ordinal: u16) -> NodeData {
    let name = format!("symbol_{ordinal:04}");
    NodeData {
        id: node_id(repo, file_path, NodeKind::Function, &name),
        kind: NodeKind::Function,
        repo: repo.to_owned(),
        file_path: file_path.to_owned(),
        name,
        qualified_name: None,
        external_id: None,
        signature: Some("()".to_owned()),
        visibility: Some(Visibility::Public),
        span: Some(SourceSpan {
            line_start: u32::from(ordinal) + 2,
            line_len: 0,
            column_start: 0,
            column_len: 0,
        }),
        is_virtual: false,
    }
}

fn defines_edge(file_id: NodeId, symbol_id: NodeId) -> gather_step_core::EdgeData {
    gather_step_core::EdgeData {
        source: file_id,
        target: symbol_id,
        kind: EdgeKind::Defines,
        metadata: EdgeMetadata::default(),
        owner_file: file_id,
        is_cross_file: false,
    }
}

fn repo_batch(repo: &str, file_path: &str, symbol_count: u16) -> RepoBatch {
    let file = file_node(repo, file_path);
    let mut nodes = Vec::with_capacity(usize::from(symbol_count) + 1);
    let mut edges = Vec::with_capacity(usize::from(symbol_count));
    nodes.push(file.clone());

    for ordinal in 0..symbol_count {
        let function = function_node(repo, file_path, ordinal);
        edges.push(defines_edge(file.id, function.id));
        nodes.push(function);
    }

    RepoBatch {
        repo: repo.to_owned(),
        files: vec![FileBatch {
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            path_id_bytes: vec![],
            nodes,
            edges,
            content_hash: symbol_count.to_be_bytes().to_vec(),
            size_bytes: 0,
            mtime_ns: 0,
            indexed_at: 1_713_000_000,
            parse_ms: Some(5),
            force: false,
        }],
        test_hooks: RepoBatchHooks::default(),
    }
}

#[test]
fn readers_only_observe_old_or_new_snapshot() {
    let root = TestRoot::new("consistency");
    let coordinator =
        Arc::new(StorageCoordinator::open(root.path()).expect("coordinator should open"));
    let old_batch = repo_batch("service-a", "src/foo.ts", 999);
    coordinator
        .index_repo_batch(&old_batch)
        .expect("seed batch should index");

    let expected_old = coordinator
        .graph()
        .count_nodes()
        .expect("old node count should be readable");
    let new_batch = RepoBatch {
        test_hooks: RepoBatchHooks {
            fail_after_graph_files: None,
            pause_after_graph_stage: Some(Duration::from_millis(50)),
        },
        ..repo_batch("service-a", "src/foo.ts", 1_199)
    };
    let expected_new = new_batch.files[0].nodes.len();

    let inconsistent = Arc::new(AtomicUsize::new(0));
    let old_seen_during_pause = Arc::new(AtomicUsize::new(0));

    let writer = {
        let coordinator = Arc::clone(&coordinator);
        thread::spawn(move || coordinator.index_repo_batch(&new_batch))
    };

    let mut readers = Vec::new();
    for _ in 0..10 {
        let coordinator = Arc::clone(&coordinator);
        let inconsistent = Arc::clone(&inconsistent);
        let old_seen_during_pause = Arc::clone(&old_seen_during_pause);
        readers.push(thread::spawn(move || {
            for _ in 0..10 {
                let count = coordinator
                    .graph()
                    .count_nodes()
                    .expect("node count should be readable");
                if count != expected_old && count != expected_new {
                    inconsistent.fetch_add(1, Ordering::Relaxed);
                }
                if count == expected_old {
                    old_seen_during_pause.fetch_add(1, Ordering::Relaxed);
                }
                thread::sleep(Duration::from_millis(5));
            }
        }));
    }

    writer
        .join()
        .expect("writer thread should join")
        .expect("writer batch should succeed");
    for reader in readers {
        reader.join().expect("reader thread should join");
    }

    assert_eq!(inconsistent.load(Ordering::Relaxed), 0);
    assert!(old_seen_during_pause.load(Ordering::Relaxed) > 0);
    assert_eq!(
        coordinator
            .graph()
            .count_nodes()
            .expect("final node count should be readable"),
        expected_new
    );
}
