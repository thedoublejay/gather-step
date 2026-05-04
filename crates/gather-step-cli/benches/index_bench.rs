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

fn stage_synthetic_workspace(name: &str, repo_count: usize) -> TempDir {
    use std::fmt::Write as _;
    const FIXTURE_REPOS: &[&str] = &[
        "backend_standard",
        "frontend_standard",
        "shared_contracts",
        "route_constants",
        "auth_anchor_only",
        "service_a",
        "service_b",
        "service_c",
        "service_d",
        "service_e",
    ];

    let temp = TempDir::new(name);
    let fixture_workspace = fixture_root().join("workspace");
    let target_workspace = temp.path().join("workspace");
    fs::create_dir_all(&target_workspace).expect("synthetic workspace dir should exist");

    let mut config = String::from("repos:\n");
    for index in 0..repo_count {
        let source_repo = FIXTURE_REPOS[index % FIXTURE_REPOS.len()];
        let target_repo = format!("bench_{index:02}_{source_repo}");
        copy_dir_all(
            &fixture_workspace.join(source_repo),
            &target_workspace.join(&target_repo),
        );
        let _ = writeln!(
            config,
            "  - name: {target_repo}\n    path: workspace/{target_repo}"
        );
    }
    config.push_str("indexing:\n  workspace_concurrency: 4\n");
    fs::write(temp.path().join("gather-step.config.yaml"), config)
        .expect("synthetic config should write");

    temp
}

fn index_staged_workspace(workspace: &TempDir) {
    let config_path = workspace.path().join("gather-step.config.yaml");
    let config =
        GatherStepConfig::from_yaml_file(&config_path).expect("fixture config should load");
    let storage_root = workspace.path().join(".gather-step/storage");
    fs::create_dir_all(&storage_root).expect("storage dir should exist");
    let registry_path = workspace.path().join(".gather-step/registry.json");
    let mut registry = RegistryStore::open(&registry_path).expect("registry should open");
    index_workspace_with_storage(
        &config,
        workspace.path(),
        &mut registry,
        &storage_root,
        IndexingOptions::default(),
    )
    .expect("fixture workspace should index");
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
                index_staged_workspace(&workspace);
                workspace
            },
            BatchSize::PerIteration,
        );
    });
    group.bench_function("cold_index_24_repos", |b| {
        b.iter_batched(
            || stage_synthetic_workspace("cold-index-24-repos", 24),
            |workspace| {
                index_staged_workspace(&workspace);
                workspace
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

criterion_group!(benches, bench_full_workspace_index);
criterion_main!(benches);
