use std::{
    env, fs,
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, Ordering},
};

use criterion::{Criterion, criterion_group, criterion_main};
use gather_step_bench::metrics::capture_rss;
use gather_step_storage::{IndexingOptions, RepoIndexer};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir(PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!(
            "gather-step-memory-bench-{label}-{}-{id}",
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

fn thresholds_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../benchmark/thresholds.yaml")
}

/// Emit a note to stderr. Intentional in this benchmark runner.
#[expect(
    clippy::print_stderr,
    reason = "benchmark runner emits platform availability notes to stderr"
)]
fn note(msg: &str) {
    eprintln!("{msg}");
}

/// Benchmark: peak-RSS memory regression during index pass.
///
/// Acceptance threshold: RSS growth ≤ 10 % of baseline or absolute ≤ 1 GiB.
fn bench_index_memory(c: &mut Criterion) {
    let thresholds = gather_step_bench::threshold::Thresholds::load(&thresholds_path())
        .unwrap_or_else(|_| gather_step_bench::threshold::Thresholds::default_thresholds());

    let fixture = curated_fixture_root();

    // Capture RSS before any indexing to establish the baseline.
    let rss_before = capture_rss();

    let storage = TempDir::new("memory-check");
    let indexer =
        RepoIndexer::open(storage.path(), IndexingOptions::default()).expect("indexer should open");
    indexer
        .index_repo("curated_monorepo", fixture, None)
        .expect("fixture should index");

    let rss_after = capture_rss();

    if let (Some(before), Some(after)) = (rss_before, rss_after) {
        let growth = after.saturating_sub(before);
        // RSS byte counts fit comfortably within f64 mantissa for realistic workloads.
        #[expect(
            clippy::cast_precision_loss,
            reason = "RSS byte counts; f64 is sufficient for fractional-growth reporting"
        )]
        let growth_fraction = growth as f64 / before.max(1) as f64;

        assert!(
            growth <= thresholds.memory.rss_absolute_max_bytes,
            "RSS growth {growth} bytes exceeds absolute max {}",
            thresholds.memory.rss_absolute_max_bytes
        );

        assert!(
            growth_fraction <= thresholds.memory.rss_growth_max_fraction,
            "RSS growth fraction {growth_fraction:.3} exceeds threshold {}",
            thresholds.memory.rss_growth_max_fraction
        );
    } else {
        note("note: RSS capture not available on this platform; memory assertion skipped");
    }

    // Criterion timing: time the full index pass for throughput context.
    let mut group = c.benchmark_group("memory_rss");
    group.sample_size(10);
    group.bench_function("curated_monorepo_rss", |b| {
        b.iter_batched(
            || TempDir::new("mem-timing"),
            |tmp| {
                let rss_b = capture_rss();
                let indexer = RepoIndexer::open(tmp.path(), IndexingOptions::default())
                    .expect("indexer should open");
                indexer
                    .index_repo("curated_monorepo", curated_fixture_root(), None)
                    .expect("fixture should index");
                let rss_a = capture_rss();
                // Return both so the compiler cannot optimise away the work.
                (tmp, rss_b, rss_a)
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.finish();
}

criterion_group!(benches, bench_index_memory);
criterion_main!(benches);
