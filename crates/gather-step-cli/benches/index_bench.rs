use std::{
    env, fs,
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, Ordering},
};

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use gather_step_core::{GatherStepConfig, RegistryStore};
use gather_step_storage::{IndexingOptions, index_workspace_with_storage};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Scoped temp directory for bench staging.
///
/// Panics on cleanup failure so stale /tmp residue (e.g. a held file handle on
/// Windows) is surfaced immediately instead of silently accumulating.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(name: &str) -> Self {
        let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!("gather-step-bench-{name}-{}-{id}", process::id()));
        fs::create_dir_all(&path).expect("temp dir should exist");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let _ = fs::remove_dir_all(&self.path);
            return;
        }
        fs::remove_dir_all(&self.path).unwrap_or_else(|err| {
            panic!(
                "failed to remove bench temp dir {}: {err}",
                self.path.display()
            )
        });
    }
}

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures")
        .canonicalize()
        .expect("fixture root should resolve")
}

/// Copy the fixture tree. Skips `.gather-step/` generated state (so cold-index
/// benches really start cold) and refuses symlinks (so a hostile fixture PR
/// cannot escape the sandbox).
fn copy_dir_all(from: &Path, to: &Path) {
    fs::create_dir_all(to).expect("destination directory should exist");
    for entry in fs::read_dir(from).expect("source directory should be readable") {
        let entry = entry.expect("directory entry should load");
        if entry.file_name() == ".gather-step" {
            continue;
        }
        let file_type = entry.file_type().expect("file type should load");
        assert!(
            !file_type.is_symlink(),
            "fixture must not contain symlinks (found: {})",
            entry.path().display()
        );
        let from_path = entry.path();
        let to_path = to.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_all(&from_path, &to_path);
        } else {
            fs::copy(&from_path, &to_path).expect("fixture file should copy");
        }
    }
}

fn stage_fixture_workspace(name: &str) -> TempDir {
    let temp = TempDir::new(name);
    copy_dir_all(&fixture_root(), temp.path());
    temp
}

fn bench_full_workspace_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("fixture_workspace_index");
    group.sample_size(10);
    group.bench_function("full_index", |b| {
        // `iter_batched` runs setup outside the timed routine, so the fixture
        // copy (filesystem I/O) is not bundled into the indexing measurement.
        // `PerIteration` ensures each iter gets a fresh, cold workspace.
        b.iter_batched(
            || stage_fixture_workspace("full-index"),
            |workspace| {
                let config_path = workspace.path().join("gather-step.config.yaml");
                let config = GatherStepConfig::from_yaml_file(&config_path)
                    .expect("fixture config should load");
                let storage_root = workspace.path().join(".gather-step/storage");
                fs::create_dir_all(&storage_root).expect("storage dir should exist");
                let registry_path = workspace.path().join(".gather-step/registry.json");
                let mut registry =
                    RegistryStore::open(&registry_path).expect("registry should open");
                index_workspace_with_storage(
                    &config,
                    workspace.path(),
                    &mut registry,
                    &storage_root,
                    IndexingOptions::default(),
                )
                .expect("fixture workspace should index");
                workspace
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

criterion_group!(benches, bench_full_workspace_index);
criterion_main!(benches);
