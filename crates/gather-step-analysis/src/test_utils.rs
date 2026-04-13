/// Shared test helpers for the `gather-step-analysis` crate.
///
/// These utilities are used by the `#[cfg(test)]` blocks in `transport`,
/// `evidence`, and `anchor` to avoid duplicating fixture-construction logic.
use std::sync::atomic::{AtomicU64, Ordering};
use std::{env, fs, path::PathBuf, process};

use gather_step_core::{NodeData, NodeKind, SourceSpan, Visibility, node_id};
use gather_step_storage::GraphStoreDb;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// RAII wrapper that creates a unique temporary redb file and removes it on
/// drop.  The `prefix` is used in the file name to aid debugging.
pub(crate) struct TempDb {
    pub(crate) path: PathBuf,
}

impl TempDb {
    pub(crate) fn new(prefix: &str, name: &str) -> Self {
        let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!(
            "gather-step-{prefix}-{name}-{}-{id}.redb",
            process::id()
        ));
        Self { path }
    }

    /// Open a [`GraphStoreDb`] at this path.
    pub(crate) fn open(&self) -> GraphStoreDb {
        GraphStoreDb::open(&self.path).expect("store should open")
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// Build a minimal `File` node.
pub(crate) fn file_node(repo: &str, file_path: &str) -> NodeData {
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
        span: None,
        is_virtual: false,
    }
}

/// Build a minimal `Function` node.
pub(crate) fn symbol_node(repo: &str, file_path: &str, name: &str, ordinal: u16) -> NodeData {
    NodeData {
        id: node_id(repo, file_path, NodeKind::Function, name),
        kind: NodeKind::Function,
        repo: repo.to_owned(),
        file_path: file_path.to_owned(),
        name: name.to_owned(),
        qualified_name: Some(format!("{repo}::{name}")),
        external_id: None,
        signature: Some(format!("{name}()")),
        visibility: Some(Visibility::Public),
        span: Some(SourceSpan {
            line_start: u32::from(ordinal) + 1,
            line_len: 0,
            column_start: 0,
            column_len: 4,
        }),
        is_virtual: false,
    }
}
