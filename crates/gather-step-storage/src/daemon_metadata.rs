use std::{
    fs::{self, OpenOptions},
    io::Write as _,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageDaemonMetadata {
    pub pid: u32,
    pub started_at_epoch_ms: u128,
    pub workspace_root: String,
}

impl StorageDaemonMetadata {
    #[must_use]
    pub fn for_current_process(workspace_root: &Path) -> Self {
        let started_at_epoch_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        Self {
            pid: std::process::id(),
            started_at_epoch_ms,
            workspace_root: workspace_root.display().to_string(),
        }
    }

    pub fn read_from_path(path: &Path) -> Result<Self, StorageDaemonMetadataError> {
        let raw = fs::read_to_string(path)?;
        Ok(serde_json::from_str(&raw)?)
    }

    pub fn write_to_path(&self, path: &Path) -> Result<(), StorageDaemonMetadataError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_string(self)?;

        if fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
            return Err(StorageDaemonMetadataError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "refusing to write daemon metadata through a symlink",
            )));
        }

        let temp_path = temp_metadata_path(path);
        let write_result = write_metadata_temp_file(&temp_path, json.as_bytes());
        if let Err(error) = write_result {
            let _ = fs::remove_file(&temp_path);
            return Err(error.into());
        }
        if let Err(error) = fs::rename(&temp_path, path) {
            let _ = fs::remove_file(&temp_path);
            return Err(error.into());
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        }
        Ok(())
    }

    #[must_use]
    pub fn read_for_graph_path(graph_path: &Path) -> Option<Self> {
        daemon_pid_path_for_graph_path(graph_path)
            .and_then(|pid_path| Self::read_from_path(&pid_path).ok())
    }
}

fn write_metadata_temp_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn temp_metadata_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or("daemon.pid");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    path.with_file_name(format!(".{file_name}.{}.{}.tmp", std::process::id(), nonce))
}

#[must_use]
pub fn daemon_pid_path_for_storage_root(storage_root: &Path) -> Option<PathBuf> {
    storage_root
        .parent()
        .map(|parent| parent.join("daemon.pid"))
}

#[must_use]
pub fn daemon_pid_path_for_graph_path(graph_path: &Path) -> Option<PathBuf> {
    graph_path
        .parent()
        .and_then(daemon_pid_path_for_storage_root)
}

#[derive(Debug, Error)]
pub enum StorageDaemonMetadataError {
    #[error("failed to access storage daemon metadata: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to decode storage daemon metadata: {0}")]
    Json(#[from] serde_json::Error),
}

pub struct StorageDaemonMetadataGuard {
    pid_path: PathBuf,
}

impl StorageDaemonMetadataGuard {
    pub fn write_for_storage_root(
        storage_root: &Path,
        workspace_root: &Path,
    ) -> Result<Option<Self>, StorageDaemonMetadataError> {
        let Some(pid_path) = daemon_pid_path_for_storage_root(storage_root) else {
            return Ok(None);
        };
        let metadata = StorageDaemonMetadata::for_current_process(workspace_root);
        metadata.write_to_path(&pid_path)?;
        Ok(Some(Self { pid_path }))
    }
}

impl Drop for StorageDaemonMetadataGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.pid_path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_to_path_round_trips_private_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("daemon.pid");
        let metadata = StorageDaemonMetadata::for_current_process(dir.path());

        metadata.write_to_path(&path).unwrap();

        assert_eq!(
            StorageDaemonMetadata::read_from_path(&path).unwrap(),
            metadata
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn write_to_path_rejects_symlink_leaf() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target");
        fs::write(&target, "keep").unwrap();
        let path = dir.path().join("daemon.pid");
        symlink(&target, &path).unwrap();
        let metadata = StorageDaemonMetadata::for_current_process(dir.path());

        let error = metadata.write_to_path(&path).unwrap_err().to_string();

        assert!(
            error.contains("symlink"),
            "expected symlink rejection, got: {error}"
        );
        assert_eq!(fs::read_to_string(&target).unwrap(), "keep");
    }
}
