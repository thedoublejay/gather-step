use std::{
    env, fs,
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, Ordering},
};

use criterion::{Criterion, criterion_group, criterion_main};
use gather_step_core::{GatherStepConfig, RegistryStore};
use gather_step_mcp::{
    McpContext, McpServerConfig,
    tools::events::{TraceEventRequest, TraceRouteRequest, trace_event_tool, trace_route_tool},
};
use gather_step_storage::{IndexingOptions, index_workspace_with_storage};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

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

/// Copy the fixture tree. Skips `.gather-step/` generated state and refuses
/// symlinks (hardening against hostile fixture PRs).
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

/// Stage an indexed workspace and return both the `McpContext` and the
/// `TempDir` that backs it. The caller must hold both for the lifetime of the
/// benchmark group — tuple drop order (first field first) guarantees the
/// context's file handles close before the directory is removed.
fn indexed_context() -> (McpContext, TempDir) {
    let workspace = TempDir::new("query-workspace");
    copy_dir_all(&fixture_root(), workspace.path());
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

    let graph_path = storage_root.join("graph.redb");
    let ctx = McpContext::open(McpServerConfig::new(registry_path, graph_path))
        .expect("mcp context should open");
    (ctx, workspace)
}

fn bench_trace_queries(c: &mut Criterion) {
    let (ctx, _workspace) = indexed_context();
    let mut group = c.benchmark_group("fixture_queries");
    group.sample_size(20);
    group.bench_function("trace_event_order_created", |b| {
        b.iter(|| {
            trace_event_tool(
                &ctx,
                TraceEventRequest {
                    budget_bytes: None,
                    limit: None,
                    target: "order.created".to_owned(),
                },
            )
            .expect("trace_event should succeed");
        });
    });
    group.bench_function("trace_route_get_orders", |b| {
        b.iter(|| {
            trace_route_tool(
                &ctx,
                TraceRouteRequest {
                    budget_bytes: None,
                    limit: None,
                    method: "GET".to_owned(),
                    path: "/orders".to_owned(),
                },
            )
            .expect("trace_route should succeed");
        });
    });
    group.finish();
    // `_workspace` drops here, after `ctx` has released its file handles.
}

criterion_group!(benches, bench_trace_queries);
criterion_main!(benches);
