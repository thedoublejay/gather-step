use std::{path::PathBuf, sync::Arc};

use gather_step_core::{RegistryStore, WorkspaceRegistry};
use gather_step_git::set_redact_key;
use gather_step_storage::{GraphStoreDb, MetadataStoreDb, TantivySearchStore, WorkspaceStores};

use crate::{
    error::McpServerError,
    tool_trace::{Tracer, new_session_id},
};

pub const DEFAULT_MCP_MAX_LIMIT: usize = 1_000;
pub const MAX_INPUT_LENGTH: usize = 4_096;

pub fn validate_input_length(field: &str, value: &str) -> Result<(), McpServerError> {
    if value.len() > MAX_INPUT_LENGTH {
        return Err(McpServerError::InvalidInput(format!(
            "`{field}` exceeds maximum length of {MAX_INPUT_LENGTH} bytes"
        )));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct McpServerConfig {
    pub registry_path: PathBuf,
    pub graph_path: PathBuf,
    pub max_limit: usize,
    pub server_name: String,
    pub instructions: String,
    /// When `Some`, tool-call traces are appended as JSONL to this path.
    /// When `None`, traces are emitted via `tracing::info!` instead.
    pub trace_tool_calls: Option<PathBuf>,
}

impl McpServerConfig {
    #[must_use]
    pub fn new(registry_path: PathBuf, graph_path: PathBuf) -> Self {
        Self {
            registry_path,
            graph_path,
            max_limit: DEFAULT_MCP_MAX_LIMIT,
            server_name: "gather-step".to_owned(),
            instructions: "Local code graph and workspace orientation tools.".to_owned(),
            trace_tool_calls: None,
        }
    }

    pub fn with_registry_default(
        graph_path: PathBuf,
    ) -> Result<Self, gather_step_core::RegistryError> {
        Ok(Self::new(
            RegistryStore::open_default()?.path().to_path_buf(),
            graph_path,
        ))
    }

    #[must_use]
    pub fn capped_limit(&self, requested: Option<usize>, default_limit: usize) -> usize {
        requested
            .unwrap_or(default_limit)
            .clamp(1, self.max_limit.max(1))
    }

    /// Return the workspace root directory — the parent of the `.gather-step`
    /// data directory that contains `graph_path`.
    ///
    /// This is used to relativize absolute paths in MCP outputs so that
    /// machine-specific prefixes are never surfaced to LLM consumers.
    #[must_use]
    pub fn workspace_root(&self) -> PathBuf {
        self.graph_path
            .parent() // .gather-step/
            .and_then(std::path::Path::parent) // workspace root
            .map_or_else(|| self.graph_path.clone(), std::path::Path::to_path_buf)
    }

    #[must_use]
    pub fn storage_root(&self) -> PathBuf {
        self.graph_path
            .parent()
            .map_or_else(|| PathBuf::from("."), std::path::Path::to_path_buf)
    }

    #[must_use]
    pub fn search_path(&self) -> PathBuf {
        self.storage_root().join("search")
    }

    #[must_use]
    pub fn metadata_path(&self) -> PathBuf {
        self.storage_root().join("metadata.sqlite")
    }
}

/// Pre-opened stores and config shared across all MCP tool calls. Constructed
/// once at server startup and shared behind an `Arc` so that tool handlers
/// never re-open storage on the hot path.
pub struct McpContext {
    pub config: McpServerConfig,
    stores: Arc<WorkspaceStores>,
    cursor_key: [u8; 32],
    tracer: Tracer,
}

impl McpContext {
    pub fn open(config: McpServerConfig) -> Result<Self, McpServerError> {
        let stores = Arc::new(WorkspaceStores::open_read_only_search(
            config.storage_root(),
        )?);
        Ok(Self::from_workspace_stores(config, stores))
    }

    /// Return the tracer for this context.
    #[must_use]
    pub fn tracer(&self) -> &Tracer {
        &self.tracer
    }

    /// Construct an [`McpContext`] from pre-opened stores.
    ///
    /// The cursor key is 32 cryptographically-random bytes sourced from the OS
    /// via `getrandom`.  Cursors are only valid within a single process run —
    /// there is no cross-run stability requirement for MCP pagination cursors,
    /// so a fresh in-memory random key per process is the right choice: it is
    /// both simpler and more secure than a seeded or persisted key.
    ///
    /// If `getrandom` fails (extremely unlikely on any supported platform), the
    /// method falls back to a `blake3`-derived key seeded from PID + wall-clock
    /// time.  This matches the previous behaviour and preserves forward
    /// compatibility on exotic targets.
    #[must_use]
    pub fn from_workspace_stores(config: McpServerConfig, stores: Arc<WorkspaceStores>) -> Self {
        let mut cursor_key = [0_u8; 32];
        if getrandom::fill(&mut cursor_key).is_err() {
            // Fallback path: PID + nanos derived via BLAKE3.  A same-UID
            // process could observe both values, but this path is only reached
            // on platforms where `getrandom` is unavailable.
            let seed = format!(
                "{}:{}:{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos(),
                config.graph_path.display(),
            );
            cursor_key.copy_from_slice(blake3::hash(seed.as_bytes()).as_bytes());
        }
        // Derive a per-instance redact key from the cursor key so that
        // `redact_email` uses keyed BLAKE3 tied to this server run.
        // The context tag separates the redact domain from the cursor domain
        // even though both are derived from the same entropy source.
        // `blake3::derive_key` returns `[u8; 32]` directly.
        let redact_key = blake3::derive_key("gather-step redact key v1", &cursor_key);
        set_redact_key(redact_key);

        let session_id = new_session_id();
        let tracer = if let Some(path) = &config.trace_tool_calls {
            match Tracer::new_file(session_id.clone(), path) {
                Ok(t) => t,
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        path = %path.display(),
                        "tool_trace: could not open trace file; falling back to tracing::info"
                    );
                    Tracer::new_info(session_id)
                }
            }
        } else {
            Tracer::new_info(session_id)
        };

        Self {
            config,
            stores,
            cursor_key,
            tracer,
        }
    }

    pub fn open_with_stores(
        config: McpServerConfig,
        stores: Arc<WorkspaceStores>,
    ) -> Result<Self, McpServerError> {
        Ok(Self::from_workspace_stores(config, stores))
    }

    #[must_use]
    pub fn graph(&self) -> &GraphStoreDb {
        self.stores.graph()
    }

    #[must_use]
    pub fn search(&self) -> &TantivySearchStore {
        self.stores.search()
    }

    #[must_use]
    pub fn metadata(&self) -> &MetadataStoreDb {
        self.stores.metadata()
    }

    #[must_use]
    pub fn stores(&self) -> &WorkspaceStores {
        self.stores.as_ref()
    }

    pub fn registry_snapshot(&self) -> Result<WorkspaceRegistry, McpServerError> {
        Ok(RegistryStore::open(&self.config.registry_path)?.into_registry())
    }

    #[must_use]
    pub fn cursor_key(&self) -> &[u8; 32] {
        &self.cursor_key
    }
}

impl std::fmt::Debug for McpContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpContext")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::McpServerConfig;

    #[test]
    fn capped_limit_rejects_zero_by_clamping_to_one() {
        let config = McpServerConfig {
            registry_path: PathBuf::from("registry.json"),
            graph_path: PathBuf::from("graph.redb"),
            max_limit: 1_000,
            server_name: "test".to_owned(),
            instructions: String::new(),
            trace_tool_calls: None,
        };

        assert_eq!(config.capped_limit(Some(0), 20), 1);
        assert_eq!(config.capped_limit(Some(5_000), 20), 1_000);
        assert_eq!(config.capped_limit(None, 0), 1);
    }
}
