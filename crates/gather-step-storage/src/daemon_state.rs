use std::path::{Path, PathBuf};

use crate::StorageDaemonMetadata;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DaemonState {
    pub pid: Option<u32>,
    pub started_at_epoch_ms: Option<u128>,
    pub workspace_root: Option<PathBuf>,
}

impl DaemonState {
    #[must_use]
    pub fn from_graph_path(graph_path: &Path) -> Self {
        let Some(daemon_dir) = graph_path.parent().and_then(Path::parent) else {
            return Self {
                pid: None,
                started_at_epoch_ms: None,
                workspace_root: None,
            };
        };
        let default_workspace_root = daemon_dir.parent().map(Path::to_path_buf);
        match StorageDaemonMetadata::read_for_graph_path(graph_path) {
            Some(metadata) => Self {
                pid: Some(metadata.pid),
                started_at_epoch_ms: Some(metadata.started_at_epoch_ms),
                workspace_root: Some(PathBuf::from(metadata.workspace_root)),
            },
            None => Self {
                pid: None,
                started_at_epoch_ms: None,
                workspace_root: default_workspace_root,
            },
        }
    }
}
