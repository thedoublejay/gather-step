use std::{fs, path::PathBuf, time::Instant};

use anyhow::{Context, Result, bail};
use clap::Args;
use gather_step_storage::StorageCoordinator;
use serde::Serialize;

use crate::{app::AppContext, path_safety};

#[derive(Debug, Args, PartialEq, Eq)]
pub struct CompactArgs {
    #[arg(long, help = "Override the workspace-local storage directory")]
    pub storage: Option<PathBuf>,
}

#[derive(Debug, Serialize)]
struct CompactOutput {
    event: &'static str,
    storage_root: String,
    graph_path: String,
    graph_size_before_bytes: u64,
    graph_size_after_bytes: u64,
    graph_compacted: bool,
    metadata_compacted: bool,
    elapsed_ms: u64,
}

pub fn run(app: &AppContext, args: CompactArgs) -> Result<()> {
    let started = Instant::now();
    let output = app.output();
    let defaults = app.workspace_paths();
    let storage_root = args.storage.unwrap_or(defaults.storage_root);

    path_safety::reject_symlinked_generated_state(&app.workspace_path, &storage_root)
        .with_context(|| {
            format!(
                "Generated-state path `{}` failed the symlink check.",
                storage_root.display()
            )
        })?;
    let graph_path = storage_root.join("graph.redb");
    if !graph_path.exists() {
        bail!(
            "no gather-step index found at {}; run `gather-step index` first",
            storage_root.display()
        );
    }

    let mut storage = StorageCoordinator::open(&storage_root)
        .with_context(|| format!("opening storage at {}", storage_root.display()))?;
    let graph_path = storage.graph().path().to_path_buf();
    let graph_size_before_bytes = graph_file_size(&graph_path);
    let graph_compacted = storage
        .compact_graph()
        .with_context(|| format!("compacting graph store at {}", graph_path.display()))?;
    storage
        .metadata()
        .try_finalize()
        .context("compacting metadata store")?;
    let graph_size_after_bytes = graph_file_size(&graph_path);

    let payload = CompactOutput {
        event: "compact_completed",
        storage_root: storage_root.display().to_string(),
        graph_path: graph_path.display().to_string(),
        graph_size_before_bytes,
        graph_size_after_bytes,
        graph_compacted,
        metadata_compacted: true,
        elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
    };

    output.emit(&payload)?;
    output.line(format!(
        "Compacted graph at {} ({} -> {} bytes)",
        payload.graph_path, payload.graph_size_before_bytes, payload.graph_size_after_bytes
    ));
    Ok(())
}

fn graph_file_size(path: &std::path::Path) -> u64 {
    fs::metadata(path)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}
