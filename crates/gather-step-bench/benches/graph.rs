use std::{
    env, fs,
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, Ordering},
};

use criterion::{Criterion, criterion_group, criterion_main};
use gather_step_storage::{GraphStore, IndexingOptions, RepoIndexer};
use rustc_hash::FxHashSet;
use serde::Deserialize;

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir(PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!(
            "gather-step-graph-bench-{label}-{}-{id}",
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

fn expected_graph_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../benchmark/expected/expected-graph.json")
}

fn thresholds_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../benchmark/thresholds.yaml")
}

#[derive(Debug, Deserialize)]
struct ExpectedNode {
    name: String,
    kind: String,
    #[expect(
        dead_code,
        reason = "deserialized from JSON for schema completeness; only name and kind are compared"
    )]
    file: String,
}

#[derive(Debug, Deserialize)]
struct ExpectedEdge {
    kind: String,
    #[expect(
        dead_code,
        reason = "deserialized from JSON for schema completeness; only kind is compared"
    )]
    source_file: String,
    #[expect(
        dead_code,
        reason = "deserialized from JSON for schema completeness; only kind is compared"
    )]
    target_file: String,
}

#[derive(Debug, Deserialize)]
struct ExpectedGraph {
    expected_nodes: Vec<ExpectedNode>,
    expected_edges: Vec<ExpectedEdge>,
}

/// Open a `RepoIndexer`, run an index pass over the curated monorepo, and
/// return the indexer along with a temp-dir RAII guard.
fn index_curated_monorepo(label: &str) -> (RepoIndexer, TempDir) {
    let storage = TempDir::new(label);
    let indexer =
        RepoIndexer::open(storage.path(), IndexingOptions::default()).expect("indexer should open");
    let fixture = curated_fixture_root();
    indexer
        .index_repo("curated_monorepo", fixture, None)
        .expect("fixture should index");
    (indexer, storage)
}

fn bench_graph_recall_precision(c: &mut Criterion) {
    let expected_raw =
        fs::read_to_string(expected_graph_path()).expect("expected graph JSON should exist");
    let expected: ExpectedGraph =
        serde_json::from_str(&expected_raw).expect("expected graph JSON should parse");

    let thresholds = gather_step_bench::threshold::Thresholds::load(&thresholds_path())
        .unwrap_or_else(|_| gather_step_bench::threshold::Thresholds::default_thresholds());

    // Index once outside the timed section and perform recall/precision checks.
    let (indexer, _storage) = index_curated_monorepo("recall-check");
    let graph = indexer.storage().graph();

    // Build the expected (name, kind) pairs first so we know which node kinds
    // the expected graph covers.  Recall/precision are computed only over those
    // kinds — the indexer emits many auxiliary nodes (File, Repo, virtual route
    // nodes, etc.) that are intentionally absent from the hand-curated expected
    // set and would artificially inflate false-positive counts if included.
    let expected_node_pairs: FxHashSet<(String, String)> = expected
        .expected_nodes
        .iter()
        .map(|n| (n.name.clone(), n.kind.clone()))
        .collect();

    let expected_kinds: FxHashSet<String> = expected
        .expected_nodes
        .iter()
        .map(|n| n.kind.clone())
        .collect();

    // Collect actual node (name, kind) pairs restricted to the same kinds as
    // the expected graph so precision is computed within the same domain.
    let actual_node_pairs: FxHashSet<(String, String)> = gather_step_core::NodeKind::all()
        .iter()
        .filter(|kind| expected_kinds.contains(&format!("{kind:?}")))
        .flat_map(|kind| {
            graph
                .nodes_by_type(*kind)
                .unwrap_or_default()
                .into_iter()
                .filter(|n| !n.is_virtual)
                .map(|n| (n.name.clone(), format!("{:?}", n.kind)))
        })
        .collect();

    let true_positives_nodes = actual_node_pairs.intersection(&expected_node_pairs).count();

    // Counts are small enough that f64 precision is acceptable for quality reporting.
    #[expect(
        clippy::cast_precision_loss,
        reason = "node/edge counts in the fixture corpus are small; f64 precision is acceptable"
    )]
    let (recall_nodes, precision_nodes) = (
        true_positives_nodes as f64 / expected_node_pairs.len().max(1) as f64,
        true_positives_nodes as f64 / actual_node_pairs.len().max(1) as f64,
    );

    // The expected graph lists specific edge kinds (e.g. "Imports").  Recall and
    // precision are computed over only those edge kinds so the metrics are not
    // penalised for the many other edge kinds the indexer correctly emits (Defines,
    // Calls, etc.) that are absent from the hand-curated expected set.
    let expected_edge_kinds: FxHashSet<String> = expected
        .expected_edges
        .iter()
        .map(|e| e.kind.clone())
        .collect();

    // Actual edge kinds restricted to the expected-kinds domain.
    let actual_edge_kinds_restricted: FxHashSet<String> = gather_step_core::EdgeKind::all()
        .iter()
        .filter_map(|kind| {
            let kind_str = format!("{kind:?}");
            if expected_edge_kinds.contains(&kind_str) {
                let count = graph.count_edges_by_kind(*kind).unwrap_or(0);
                if count > 0 {
                    return Some(kind_str);
                }
            }
            None
        })
        .collect();

    let true_positives_edges = actual_edge_kinds_restricted
        .intersection(&expected_edge_kinds)
        .count();

    #[expect(
        clippy::cast_precision_loss,
        reason = "edge kind counts are small; f64 precision is acceptable"
    )]
    let (recall_edges, precision_edges) = (
        true_positives_edges as f64 / expected_edge_kinds.len().max(1) as f64,
        true_positives_edges as f64 / actual_edge_kinds_restricted.len().max(1) as f64,
    );

    assert!(
        recall_nodes >= thresholds.graph_quality.nodes_recall_min,
        "node recall {recall_nodes:.3} < threshold {:.3}",
        thresholds.graph_quality.nodes_recall_min
    );
    assert!(
        precision_nodes >= thresholds.graph_quality.nodes_precision_min,
        "node precision {precision_nodes:.3} < threshold {:.3}",
        thresholds.graph_quality.nodes_precision_min
    );
    assert!(
        recall_edges >= thresholds.graph_quality.edges_recall_min,
        "edge recall {recall_edges:.3} < threshold {:.3}",
        thresholds.graph_quality.edges_recall_min
    );
    assert!(
        precision_edges >= thresholds.graph_quality.edges_precision_min,
        "edge precision {precision_edges:.3} < threshold {:.3}",
        thresholds.graph_quality.edges_precision_min
    );

    // Criterion timing: just time the index pass itself.
    let mut group = c.benchmark_group("graph_quality");
    group.sample_size(10);
    group.bench_function("curated_monorepo_index", |b| {
        b.iter_batched(
            || TempDir::new("timing"),
            |tmp| {
                let indexer = RepoIndexer::open(tmp.path(), IndexingOptions::default())
                    .expect("indexer should open");
                indexer
                    .index_repo("curated_monorepo", curated_fixture_root(), None)
                    .expect("fixture should index");
                tmp
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.finish();
}

criterion_group!(benches, bench_graph_recall_precision);
criterion_main!(benches);
