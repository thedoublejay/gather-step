use std::{
    env, fs,
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use criterion::{Criterion, criterion_group, criterion_main};
use gather_step_bench::metrics::ResourceSampler;
use gather_step_core::GatherStepConfig;
use gather_step_storage::{
    IndexingOptions, RepoIndexer, WatcherConfig, WorkspaceStores, WorkspaceWatcher,
};
use tokio_util::sync::CancellationToken;

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

    let resources = ResourceSampler::start();
    let storage = TempDir::new("memory-check");
    let indexer =
        RepoIndexer::open(storage.path(), IndexingOptions::default()).expect("indexer should open");
    indexer
        .index_repo("curated_monorepo", fixture, None)
        .expect("fixture should index");
    let resource_peaks = resources.finish();

    if let (Some(before), Some(growth)) = (
        resource_peaks.start_rss_bytes,
        resource_peaks.rss_growth_bytes(),
    ) {
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
    if let (Some(start), Some(peak)) = (resource_peaks.start_open_fds, resource_peaks.peak_open_fds)
    {
        assert!(
            peak >= start,
            "open-FD peak {peak} should be >= start {start}"
        );
    } else {
        note("note: open-FD capture not available on this platform; FD assertion skipped");
    }

    // Criterion timing: time the full index pass for throughput context.
    let mut group = c.benchmark_group("memory_rss");
    group.sample_size(10);
    group.bench_function("curated_monorepo_rss", |b| {
        b.iter_batched(
            || TempDir::new("mem-timing"),
            |tmp| {
                let resources = ResourceSampler::start();
                let indexer = RepoIndexer::open(tmp.path(), IndexingOptions::default())
                    .expect("indexer should open");
                indexer
                    .index_repo("curated_monorepo", curated_fixture_root(), None)
                    .expect("fixture should index");
                let resource_peaks = resources.finish();
                (tmp, resource_peaks)
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.finish();
}

fn bench_watch_resources(c: &mut Criterion) {
    let fixture = curated_fixture_root();
    let config = GatherStepConfig::from_yaml_file(fixture.join("gather-step.config.yaml"))
        .expect("fixture config should load");
    let mut group = c.benchmark_group("watch_resources");
    group.sample_size(10);
    group.bench_function("curated_monorepo_watch_startup", |b| {
        b.iter_batched(
            || TempDir::new("watch-resources"),
            |tmp| {
                let resources = ResourceSampler::start();
                let stores = WorkspaceStores::open(tmp.path()).expect("workspace stores");
                let watcher = WorkspaceWatcher::new_with_stores(
                    stores,
                    IndexingOptions::from_config(&config),
                    WatcherConfig {
                        poll_interval: Duration::from_millis(100),
                        debounce_duration: Duration::from_millis(100),
                        consecutive_error_limit: 1,
                        error_backoff: Duration::from_millis(100),
                    },
                    &config,
                    &fixture,
                )
                .expect("workspace watcher");
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                    .expect("tokio runtime");
                let cancel = CancellationToken::new();
                runtime
                    .block_on(async {
                        let run = watcher.run(cancel.clone());
                        let stop = async {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            cancel.cancel();
                        };
                        tokio::pin!(run);
                        tokio::pin!(stop);
                        tokio::select! {
                            result = &mut run => result,
                            () = &mut stop => run.await,
                        }
                    })
                    .expect("watcher should stop cleanly");
                let resource_peaks = resources.finish();
                (tmp, resource_peaks)
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.finish();
}

criterion_group!(benches, bench_index_memory, bench_watch_resources);
criterion_main!(benches);
