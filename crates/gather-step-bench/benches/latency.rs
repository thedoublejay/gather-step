use std::{
    env, fs,
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, Ordering},
};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use gather_step_analysis::anchor::rank_anchors;
use gather_step_core::NodeKind;
use gather_step_storage::{GraphStore, IndexingOptions, RepoIndexer, SearchStore};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir(PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!(
            "gather-step-latency-bench-{label}-{}-{id}",
            process::id()
        ));
        fs::create_dir_all(&path).expect("temp dir should exist");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn curated_fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../benchmark/fixtures/curated-monorepo")
}

/// Index the curated monorepo once and return the warmed indexer + storage
/// guard. The returned `TempDir` must be held for the entire benchmark group.
fn warmed_indexer() -> (RepoIndexer, TempDir) {
    let storage = TempDir::new("latency");
    let indexer =
        RepoIndexer::open(storage.path(), IndexingOptions::default()).expect("indexer should open");
    indexer
        .index_repo("curated_monorepo", curated_fixture_root(), None)
        .expect("fixture should index");
    (indexer, storage)
}

/// Benchmark: symbol search latency.
fn bench_search(c: &mut Criterion) {
    let (indexer, _storage) = warmed_indexer();
    let search = indexer.storage().search();

    let queries = [
        "listOrders",
        "createOrder",
        "OrdersService",
        "OrdersController",
    ];

    let mut group = c.benchmark_group("api_latency_search");
    group.sample_size(50);
    for query in &queries {
        group.bench_with_input(BenchmarkId::new("search", query), query, |b, q| {
            b.iter(|| {
                search.search(q, 20).expect("search should succeed");
            });
        });
    }
    group.finish();
}

/// Benchmark: graph node-by-type (pack-adjacent) query latency.
fn bench_pack(c: &mut Criterion) {
    let (indexer, _storage) = warmed_indexer();
    let graph = indexer.storage().graph();

    let node_kinds = [NodeKind::Class, NodeKind::Function, NodeKind::File];

    let mut group = c.benchmark_group("api_latency_pack");
    group.sample_size(50);
    for kind in &node_kinds {
        group.bench_with_input(
            BenchmarkId::new("nodes_by_type", format!("{kind:?}")),
            kind,
            |b, k| {
                b.iter(|| {
                    graph
                        .nodes_by_type(*k)
                        .expect("nodes_by_type should succeed");
                });
            },
        );
    }
    group.finish();
}

/// Benchmark: impact / graph traversal query latency.
fn bench_impact(c: &mut Criterion) {
    let (indexer, _storage) = warmed_indexer();
    let graph = indexer.storage().graph();

    let mut group = c.benchmark_group("api_latency_impact");
    group.sample_size(50);
    group.bench_function("nodes_by_repo", |b| {
        b.iter(|| {
            graph
                .nodes_by_repo("curated_monorepo")
                .expect("nodes_by_repo should succeed");
        });
    });
    group.bench_function("count_nodes", |b| {
        b.iter(|| {
            graph.count_nodes().expect("count_nodes should succeed");
        });
    });
    group.bench_function("count_edges", |b| {
        b.iter(|| {
            graph.count_edges().expect("count_edges should succeed");
        });
    });
    group.finish();
}

/// Benchmark: anchor ranking latency.
fn bench_rank_anchors(c: &mut Criterion) {
    let (indexer, _storage) = warmed_indexer();
    let graph = indexer.storage().graph();

    // Collect all node IDs from the repo once, outside the timing loop.
    let candidate_ids: Vec<gather_step_core::NodeId> = graph
        .nodes_by_repo("curated_monorepo")
        .expect("nodes_by_repo should succeed")
        .into_iter()
        .map(|n| n.id)
        .collect();

    let mut group = c.benchmark_group("api_latency_rank_anchors");
    group.sample_size(50);
    group.bench_function("rank_anchors", |b| {
        b.iter(|| {
            rank_anchors(graph, &candidate_ids).expect("rank_anchors should succeed");
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_search,
    bench_pack,
    bench_impact,
    bench_rank_anchors
);
criterion_main!(benches);
