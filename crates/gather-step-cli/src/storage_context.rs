//! `StorageContext` — a single value type that names the paths and open-mode
//! a Gather Step operation needs to access storage.
//!
//! # Motivation
//!
//! Most commands derive their storage paths from [`AppContext::workspace_paths`]
//! which always points at the current workspace.  The upcoming `pr-review` mode
//! needs to point at isolated review artifacts instead.  `StorageContext` is the
//! shared upstream type from which both [`StorageCoordinator`] and
//! [`McpServerConfig`] are constructed, so there is a single source of truth
//! regardless of whether a command goes through the storage coordinator or the
//! MCP context.
//!
//! # Layout contract for `review` contexts
//!
//! `McpServerConfig::workspace_root()` infers the "workspace root" as
//! `graph_path.parent().parent()`.  For that inference to produce a consistent
//! root, review artifacts must follow the layout
//! `<review_root>/storage/graph.redb` — which the [`StorageContext::review`]
//! constructor enforces by computing `graph_path = storage_root.join("graph.redb")`.
//!
//! The `workspace_root` field exposed by `StorageContext` for a review context
//! is therefore the review artifact root (`storage_root.parent()`), **not** the
//! path of the source workspace being reviewed.  The source workspace path is
//! not needed by any storage operation and is intentionally omitted.
//!
//! This is Phase 0 Task 1 of the PR review mode plan.

use std::path::{Path, PathBuf};

use anyhow::Result;
use gather_step_mcp::McpServerConfig;
use gather_step_storage::{StorageCoordinator, StorageCoordinatorError};

use crate::app::{AppContext, WorkspacePaths};

/// Whether the Tantivy search index is opened for reading only or for
/// read-write indexing.
///
/// Read-only mode avoids acquiring a Tantivy write lock, which is important
/// when read commands run concurrently with an active indexer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TantivyOpenMode {
    /// Open the search index read-only (no write lock acquired).
    ReadOnly,
    /// Open the search index read-write (write lock acquired).
    ReadWrite,
}

/// Describes which kind of storage a context targets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContextLabel {
    /// The current workspace (default for all non-review commands).
    Workspace,
    /// An isolated review artifact set keyed by a review run identifier.
    Review {
        /// Opaque identifier for the review run (e.g. `"pr-42-abc123"`).
        run_id: String,
    },
}

/// Named storage paths and open-mode for a single Gather Step operation.
///
/// Construct with [`StorageContext::workspace`], [`StorageContext::workspace_read_only`],
/// or [`StorageContext::review`].  Then produce a [`StorageCoordinator`] via
/// [`StorageContext::open_storage_coordinator`] or an [`McpServerConfig`] via
/// [`StorageContext::mcp_server_config`].
#[derive(Clone, Debug)]
pub struct StorageContext {
    workspace_root: PathBuf,
    config_path: PathBuf,
    registry_path: PathBuf,
    storage_root: PathBuf,
    graph_path: PathBuf,
    tantivy_mode: TantivyOpenMode,
    label: ContextLabel,
}

impl StorageContext {
    /// Derive a read-write storage context from the current workspace.
    ///
    /// Produces identical paths to [`AppContext::workspace_paths`].
    #[must_use]
    pub fn workspace(app: &AppContext) -> Self {
        Self::from_workspace_paths(app, TantivyOpenMode::ReadWrite)
    }

    /// Derive a read-only storage context from the current workspace.
    ///
    /// Same paths as [`StorageContext::workspace`] but the Tantivy search
    /// index is opened read-only to avoid write-lock contention.
    #[must_use]
    pub fn workspace_read_only(app: &AppContext) -> Self {
        Self::from_workspace_paths(app, TantivyOpenMode::ReadOnly)
    }

    fn from_workspace_paths(app: &AppContext, tantivy_mode: TantivyOpenMode) -> Self {
        let WorkspacePaths {
            config_path,
            registry_path,
            storage_root,
            graph_path,
        } = app.workspace_paths();
        Self {
            workspace_root: app.workspace_path.clone(),
            config_path,
            registry_path,
            storage_root,
            graph_path,
            tantivy_mode,
            label: ContextLabel::Workspace,
        }
    }

    /// Build a read-only storage context for a PR review artifact set.
    ///
    /// # Parameters
    ///
    /// - `workspace_root` — root of the review artifact tree.  Must satisfy
    ///   `storage_root == workspace_root.join("storage")` so that
    ///   [`McpServerConfig::workspace_root`] (which infers from
    ///   `graph_path.parent().parent()`) returns the same value as
    ///   [`StorageContext::workspace_root`].
    /// - `registry_path` — path to the isolated review registry JSON file.
    /// - `storage_root` — path to the isolated review storage directory.
    ///   `graph_path` is derived as `storage_root.join("graph.redb")` and
    ///   `config_path` as `workspace_root.join("gather-step.config.yaml")`.
    /// - `run_id` — opaque review run identifier (e.g. `"pr-42-abc123"`).
    #[must_use]
    pub fn review(
        workspace_root: PathBuf,
        registry_path: PathBuf,
        storage_root: PathBuf,
        run_id: impl Into<String>,
    ) -> Self {
        let graph_path = storage_root.join("graph.redb");
        let config_path = workspace_root.join("gather-step.config.yaml");
        Self {
            workspace_root,
            config_path,
            registry_path,
            graph_path,
            storage_root,
            tantivy_mode: TantivyOpenMode::ReadOnly,
            label: ContextLabel::Review {
                run_id: run_id.into(),
            },
        }
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    /// Root of the storage tree (workspace root for [`ContextLabel::Workspace`],
    /// review artifact root for [`ContextLabel::Review`]).
    #[must_use]
    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    /// Path to `gather-step.config.yaml`.
    #[must_use]
    pub fn config_path(&self) -> &Path {
        &self.config_path
    }

    /// Path to `registry.json`.
    #[must_use]
    pub fn registry_path(&self) -> &Path {
        &self.registry_path
    }

    /// Root of the storage directory (contains `graph.redb`, `search/`,
    /// `metadata.sqlite`).
    #[must_use]
    pub fn storage_root(&self) -> &Path {
        &self.storage_root
    }

    /// Path to `graph.redb`.
    #[must_use]
    pub fn graph_path(&self) -> &Path {
        &self.graph_path
    }

    /// Whether the Tantivy search index should be opened read-only or
    /// read-write.
    #[must_use]
    pub fn tantivy_mode(&self) -> TantivyOpenMode {
        self.tantivy_mode
    }

    /// Label indicating whether this context targets the workspace or an
    /// isolated review artifact set.
    #[must_use]
    pub fn label(&self) -> &ContextLabel {
        &self.label
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// Open a [`StorageCoordinator`] using the paths and open-mode in this
    /// context.
    ///
    /// - [`TantivyOpenMode::ReadWrite`] → [`StorageCoordinator::open`] (write
    ///   lock acquired on the search index).
    /// - [`TantivyOpenMode::ReadOnly`] → [`StorageCoordinator::open_read_only`]
    ///   (no write lock; safe for concurrent read commands).
    pub fn open_storage_coordinator(&self) -> Result<StorageCoordinator, StorageCoordinatorError> {
        match self.tantivy_mode {
            TantivyOpenMode::ReadWrite => StorageCoordinator::open(&self.storage_root),
            TantivyOpenMode::ReadOnly => StorageCoordinator::open_read_only(&self.storage_root),
        }
    }

    /// Build an [`McpServerConfig`] from the paths in this context.
    ///
    /// For review contexts, `graph_path` is at `<workspace_root>/storage/graph.redb`,
    /// so `McpServerConfig::workspace_root()` (which infers `graph_path.parent().parent()`)
    /// returns `self.workspace_root()` — the path-equality property tested in
    /// [`tests::review_mcp_config_workspace_root_inference_matches_self`].
    #[must_use]
    pub fn mcp_server_config(&self) -> McpServerConfig {
        McpServerConfig::new(self.registry_path.clone(), self.graph_path.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use indicatif::MultiProgress;

    use super::*;
    use crate::app::{AppContext, ColorModeArg};

    /// Construct a minimal `AppContext` pointing at the given workspace path.
    fn test_app(workspace_path: PathBuf) -> AppContext {
        AppContext {
            workspace_path,
            repo_filter: None,
            json_output: false,
            no_interactive: true,
            stdin_is_tty: false,
            stdout_is_tty: false,
            stderr_is_tty: false,
            ci_env_set: true,
            color_mode: ColorModeArg::Auto,
            show_banner: false,
            multi_progress: MultiProgress::new(),
        }
    }

    /// `StorageContext::workspace` produces the same four paths as
    /// `AppContext::workspace_paths`.
    #[test]
    fn workspace_default_matches_workspace_paths() {
        let ws = PathBuf::from("/tmp/test-workspace-abc");
        let app = test_app(ws.clone());
        let paths = app.workspace_paths();
        let ctx = StorageContext::workspace(&app);

        assert_eq!(ctx.workspace_root(), ws.as_path());
        assert_eq!(ctx.config_path(), paths.config_path.as_path());
        assert_eq!(ctx.registry_path(), paths.registry_path.as_path());
        assert_eq!(ctx.storage_root(), paths.storage_root.as_path());
        assert_eq!(ctx.graph_path(), paths.graph_path.as_path());
        assert_eq!(ctx.tantivy_mode(), TantivyOpenMode::ReadWrite);
        assert_eq!(ctx.label(), &ContextLabel::Workspace);
    }

    /// All review paths live under the review root, not the workspace root.
    #[test]
    fn review_paths_are_under_review_root() {
        let review_root = PathBuf::from("/tmp/review-run-42");
        let storage_root = review_root.join("storage");
        let registry_path = review_root.join("registry.json");
        let run_id = "pr-42-abc123";

        let ctx = StorageContext::review(
            review_root.clone(),
            registry_path.clone(),
            storage_root.clone(),
            run_id,
        );

        // All paths descend from the review root.
        assert!(ctx.storage_root().starts_with(&review_root));
        assert!(ctx.graph_path().starts_with(&review_root));
        assert!(ctx.registry_path().starts_with(&review_root));

        // Derived paths are correct.
        assert_eq!(ctx.storage_root(), storage_root.as_path());
        assert_eq!(ctx.graph_path(), storage_root.join("graph.redb").as_path());
        assert_eq!(ctx.registry_path(), registry_path.as_path());

        // Mode and label.
        assert_eq!(ctx.tantivy_mode(), TantivyOpenMode::ReadOnly);
        assert_eq!(
            ctx.label(),
            &ContextLabel::Review {
                run_id: run_id.to_owned()
            }
        );
    }

    /// For a review context, `mcp_server_config().workspace_root()` (which
    /// infers via `graph_path.parent().parent()`) must equal
    /// `ctx.workspace_root()`.
    ///
    /// This holds when the review storage layout is
    /// `<review_root>/storage/graph.redb`, because:
    ///   `graph_path.parent()`          = `<review_root>/storage`
    ///   `graph_path.parent().parent()` = `<review_root>`          = `workspace_root`
    #[test]
    fn review_mcp_config_workspace_root_inference_matches_self() {
        let review_root = PathBuf::from("/tmp/review-inference-check");
        let storage_root = review_root.join("storage");
        let registry_path = review_root.join("registry.json");

        let ctx = StorageContext::review(
            review_root.clone(),
            registry_path,
            storage_root,
            "pr-1-test",
        );

        let inferred = ctx.mcp_server_config().workspace_root();
        assert_eq!(
            inferred,
            ctx.workspace_root(),
            "McpServerConfig::workspace_root() inferred {inferred:?} but StorageContext::workspace_root() is {:?}",
            ctx.workspace_root()
        );
    }

    /// `workspace_read_only` sets `TantivyOpenMode::ReadOnly` but otherwise
    /// produces the same paths as `workspace`.
    ///
    /// We verify the mode flag rather than performing real IO, because the
    /// temp path does not contain actual storage files.
    #[test]
    fn read_only_mode_opens_read_only_tantivy() {
        let ws = PathBuf::from("/tmp/test-workspace-readonly");
        let app = test_app(ws);
        let ctx_write = StorageContext::workspace(&app);
        let ctx_read = StorageContext::workspace_read_only(&app);

        // Paths are identical.
        assert_eq!(ctx_write.storage_root(), ctx_read.storage_root());
        assert_eq!(ctx_write.graph_path(), ctx_read.graph_path());
        assert_eq!(ctx_write.registry_path(), ctx_read.registry_path());

        // Modes differ as expected.
        assert_eq!(ctx_write.tantivy_mode(), TantivyOpenMode::ReadWrite);
        assert_eq!(ctx_read.tantivy_mode(), TantivyOpenMode::ReadOnly);
    }
}
