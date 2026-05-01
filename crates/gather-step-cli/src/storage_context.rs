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
use thiserror::Error;

use crate::app::{AppContext, WorkspacePaths};
use crate::path_safety;

/// Errors produced by [`StorageContext::review_checked`].
///
/// All variants carry the two paths that triggered the rejection so the caller
/// can surface a precise, actionable error message.
#[derive(Debug, Error)]
pub enum ReviewSafetyError {
    /// Review storage equals or contains the workspace storage path (or vice
    /// versa).
    #[error(
        "review storage path {review} overlaps the workspace storage path {workspace}"
    )]
    StorageOverlap { review: PathBuf, workspace: PathBuf },

    /// Review registry equals the workspace registry path.
    #[error(
        "review registry path {review} equals the workspace registry path {workspace}"
    )]
    RegistryEqualsWorkspace { review: PathBuf, workspace: PathBuf },

    /// Review storage equals the workspace registry path, or review registry
    /// equals the workspace storage path (cross-collision paranoia check).
    #[error(
        "review path {review} collides with workspace generated state at {workspace}"
    )]
    GeneratedStateCollision { review: PathBuf, workspace: PathBuf },

    /// Review `workspace_root` equals or contains the standing workspace root.
    ///
    /// Note: the reverse (review root *inside* workspace root) is explicitly
    /// allowed — workspace-local review artifacts behind a flag are permitted
    /// by the master plan.
    #[error(
        "review workspace_root {review} equals or contains the standing workspace root {workspace}"
    )]
    WorkspaceRootCollision { review: PathBuf, workspace: PathBuf },

    /// Filesystem error while canonicalizing a path.
    #[error("path canonicalization failed for {path}: {source}")]
    Canonicalize {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// Canonicalize `p` for use in the review-safety guard.
///
/// If `p` exists, delegates to `std::fs::canonicalize`.  If `p` does not
/// exist yet (review root is being created fresh), walks up to the longest
/// existing ancestor, canonicalizes that, and appends the remaining components
/// lexically — the same partial-canonicalization strategy used by
/// [`path_safety::canonicalize_inside_workspace`].
fn canonicalize_for_guard(p: &Path) -> Result<PathBuf, ReviewSafetyError> {
    use std::io;
    use std::path::Component;

    // Fast path: path exists.
    match std::fs::canonicalize(p) {
        Ok(c) => return Ok(c),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(ReviewSafetyError::Canonicalize {
                path: p.to_path_buf(),
                source,
            })
        }
    }

    // Partial-canonicalization: walk up until we find an existing ancestor.
    let mut components: Vec<_> = p.components().collect();
    let mut remaining: Vec<Component<'_>> = Vec::new();

    loop {
        if components.is_empty() {
            // Nothing exists — return a lexically normalized form of the
            // original path (best effort; the guard comparisons are still
            // meaningful for typical fresh-directory scenarios).
            let result = path_safety::lexically_normalize(p);
            return Ok(result);
        }

        let candidate: PathBuf = components.iter().collect();
        match std::fs::canonicalize(&candidate) {
            Ok(canonical_prefix) => {
                let suffix: PathBuf = remaining.iter().rev().collect();
                let joined = canonical_prefix.join(suffix);
                return Ok(path_safety::lexically_normalize(&joined));
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                remaining.push(components.pop().expect("non-empty"));
            }
            Err(source) => {
                return Err(ReviewSafetyError::Canonicalize {
                    path: candidate,
                    source,
                });
            }
        }
    }
}

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
    ///
    /// # Note
    ///
    /// This constructor does **not** validate path overlap with the standing
    /// workspace.  It is intended for unit tests where the caller already knows
    /// the paths are safe (e.g. temp-dir tests).  Production callers should use
    /// [`StorageContext::review_checked`] instead.
    #[doc(hidden)]
    #[must_use]
    pub fn review(
        workspace_root: PathBuf,
        registry_path: PathBuf,
        storage_root: PathBuf,
        run_id: impl Into<String>,
    ) -> Self {
        Self::review_unchecked(workspace_root, registry_path, storage_root, run_id)
    }

    /// Construct a review context, validating that none of its paths overlap
    /// with the standing workspace's paths.
    ///
    /// All six paths (3 review + 3 workspace) are canonicalized before
    /// comparison.  For paths that do not yet exist on disk the longest
    /// existing ancestor is canonicalized and the remaining components are
    /// appended lexically (same strategy as
    /// [`path_safety::canonicalize_inside_workspace`]).
    ///
    /// Returns `Err` if any of the following overlap conditions hold:
    ///
    /// - `review_storage_root` equals or is contained in `workspace.storage_root`,
    ///   or vice versa → [`ReviewSafetyError::StorageOverlap`].
    /// - `review_registry_path` equals `workspace.registry_path` →
    ///   [`ReviewSafetyError::RegistryEqualsWorkspace`].
    /// - `review_storage_root` equals `workspace.registry_path`, or
    ///   `review_registry_path` equals `workspace.storage_root` →
    ///   [`ReviewSafetyError::GeneratedStateCollision`].
    /// - `review_workspace_root` equals or contains `workspace.workspace_root`
    ///   → [`ReviewSafetyError::WorkspaceRootCollision`].
    ///
    /// The reverse of the workspace-root check (review root *inside* workspace
    /// root) is intentionally **allowed** — workspace-local review artifacts
    /// behind a flag are explicitly permitted by the master plan, as long as
    /// the storage and registry paths are disjoint.
    pub fn review_checked(
        workspace: &StorageContext,
        review_workspace_root: PathBuf,
        review_registry_path: PathBuf,
        review_storage_root: PathBuf,
        run_id: String,
    ) -> Result<Self, ReviewSafetyError> {
        // Canonicalize all six paths, tolerating paths that don't exist yet.
        let c_rev_root = canonicalize_for_guard(&review_workspace_root)?;
        let c_rev_reg = canonicalize_for_guard(&review_registry_path)?;
        let c_rev_stor = canonicalize_for_guard(&review_storage_root)?;

        let c_ws_root = canonicalize_for_guard(&workspace.workspace_root)?;
        let c_ws_reg = canonicalize_for_guard(&workspace.registry_path)?;
        let c_ws_stor = canonicalize_for_guard(&workspace.storage_root)?;

        // 1. Storage overlap (bidirectional containment).
        if c_rev_stor == c_ws_stor
            || c_rev_stor.starts_with(&c_ws_stor)
            || c_ws_stor.starts_with(&c_rev_stor)
        {
            return Err(ReviewSafetyError::StorageOverlap {
                review: c_rev_stor,
                workspace: c_ws_stor,
            });
        }

        // 2. Registry identity.
        if c_rev_reg == c_ws_reg {
            return Err(ReviewSafetyError::RegistryEqualsWorkspace {
                review: c_rev_reg,
                workspace: c_ws_reg,
            });
        }

        // 3. Cross-collision: storage ↔ registry.
        if c_rev_stor == c_ws_reg {
            return Err(ReviewSafetyError::GeneratedStateCollision {
                review: c_rev_stor,
                workspace: c_ws_reg,
            });
        }
        if c_rev_reg == c_ws_stor {
            return Err(ReviewSafetyError::GeneratedStateCollision {
                review: c_rev_reg,
                workspace: c_ws_stor,
            });
        }

        // 4. Workspace-root collision: review root equals or *contains* the
        //    workspace root (review root being *inside* workspace root is fine).
        if c_rev_root == c_ws_root || c_ws_root.starts_with(&c_rev_root) {
            return Err(ReviewSafetyError::WorkspaceRootCollision {
                review: c_rev_root,
                workspace: c_ws_root,
            });
        }

        Ok(Self::review_unchecked(
            review_workspace_root,
            review_registry_path,
            review_storage_root,
            run_id,
        ))
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /// Inner constructor shared by [`StorageContext::review`] and
    /// [`StorageContext::review_checked`].
    fn review_unchecked(
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
    use tempfile;

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

    // ── review_checked tests ──────────────────────────────────────────────────

    /// Build a workspace `StorageContext` whose backing directories actually
    /// exist on disk (required so canonicalization succeeds).
    fn test_workspace_ctx(tmp: &tempfile::TempDir) -> StorageContext {
        let ws = tmp.path().to_path_buf();
        let storage = ws.join(".gather-step").join("storage");
        std::fs::create_dir_all(&storage).unwrap();
        let registry = ws.join(".gather-step").join("registry.json");
        std::fs::write(&registry, b"{}").unwrap();

        let app = test_app(ws);
        // Build the context manually so we can point at real paths.
        let paths = app.workspace_paths();
        StorageContext {
            workspace_root: app.workspace_path.clone(),
            config_path: paths.config_path,
            registry_path: paths.registry_path,
            storage_root: paths.storage_root,
            graph_path: paths.graph_path,
            tantivy_mode: TantivyOpenMode::ReadWrite,
            label: ContextLabel::Workspace,
        }
    }

    /// review storage == workspace storage → `StorageOverlap`.
    #[test]
    fn review_checked_rejects_storage_overlap() {
        let tmp = tempfile::TempDir::new().unwrap();
        let ws_ctx = test_workspace_ctx(&tmp);

        // Review storage deliberately set to the same path.
        let rev_root = tmp.path().join("review-root");
        let rev_storage = ws_ctx.storage_root().to_path_buf(); // same
        let rev_registry = rev_root.join("registry.json");

        let err = StorageContext::review_checked(
            &ws_ctx,
            rev_root,
            rev_registry,
            rev_storage,
            "pr-1".into(),
        )
        .unwrap_err();

        assert!(
            matches!(err, ReviewSafetyError::StorageOverlap { .. }),
            "expected StorageOverlap, got {err}"
        );
    }

    /// Review storage is *inside* workspace storage → `StorageOverlap`.
    #[test]
    fn review_checked_rejects_storage_inside_workspace_storage() {
        let tmp = tempfile::TempDir::new().unwrap();
        let ws_ctx = test_workspace_ctx(&tmp);

        let rev_root = tmp.path().join("review-root");
        // Place review storage under workspace storage.
        let rev_storage = ws_ctx.storage_root().join("review");
        let rev_registry = rev_root.join("registry.json");

        let err = StorageContext::review_checked(
            &ws_ctx,
            rev_root,
            rev_registry,
            rev_storage,
            "pr-2".into(),
        )
        .unwrap_err();

        assert!(
            matches!(err, ReviewSafetyError::StorageOverlap { .. }),
            "expected StorageOverlap, got {err}"
        );
    }

    /// Workspace storage is *inside* review storage → `StorageOverlap`.
    #[test]
    fn review_checked_rejects_workspace_storage_inside_review_storage() {
        let tmp = tempfile::TempDir::new().unwrap();
        let ws_ctx = test_workspace_ctx(&tmp);

        let rev_root = tmp.path().join("review-root");
        // Review storage is the parent of workspace storage (.gather-step
        // contains workspace storage under it).
        let rev_storage = tmp.path().join(".gather-step");
        let rev_registry = rev_root.join("registry.json");

        let err = StorageContext::review_checked(
            &ws_ctx,
            rev_root,
            rev_registry,
            rev_storage,
            "pr-3".into(),
        )
        .unwrap_err();

        assert!(
            matches!(err, ReviewSafetyError::StorageOverlap { .. }),
            "expected StorageOverlap, got {err}"
        );
    }

    /// Review registry == workspace registry → `RegistryEqualsWorkspace`.
    #[test]
    fn review_checked_rejects_registry_equals_workspace_registry() {
        let tmp = tempfile::TempDir::new().unwrap();
        let ws_ctx = test_workspace_ctx(&tmp);

        let rev_root = tmp.path().join("review-root");
        let rev_storage = rev_root.join("storage");
        // Same registry path as workspace.
        let rev_registry = ws_ctx.registry_path().to_path_buf();

        let err = StorageContext::review_checked(
            &ws_ctx,
            rev_root,
            rev_registry,
            rev_storage,
            "pr-4".into(),
        )
        .unwrap_err();

        assert!(
            matches!(err, ReviewSafetyError::RegistryEqualsWorkspace { .. }),
            "expected RegistryEqualsWorkspace, got {err}"
        );
    }

    /// `review_workspace_root` == workspace root → `WorkspaceRootCollision`.
    #[test]
    fn review_checked_rejects_workspace_root_collision() {
        let tmp = tempfile::TempDir::new().unwrap();
        let ws_ctx = test_workspace_ctx(&tmp);

        // Review root == workspace root.
        let rev_root = tmp.path().to_path_buf();
        let rev_storage = tmp.path().join(".gather-step-review").join("storage");
        let rev_registry = tmp.path().join(".gather-step-review").join("registry.json");

        let err = StorageContext::review_checked(
            &ws_ctx,
            rev_root,
            rev_registry,
            rev_storage,
            "pr-5".into(),
        )
        .unwrap_err();

        assert!(
            matches!(err, ReviewSafetyError::WorkspaceRootCollision { .. }),
            "expected WorkspaceRootCollision, got {err}"
        );
    }

    /// Fully disjoint paths (review root in a separate temp dir) → `Ok`.
    #[test]
    fn review_checked_accepts_disjoint_paths() {
        let tmp_ws = tempfile::TempDir::new().unwrap();
        let tmp_rev = tempfile::TempDir::new().unwrap();
        let ws_ctx = test_workspace_ctx(&tmp_ws);

        let rev_root = tmp_rev.path().to_path_buf();
        let rev_storage = rev_root.join("storage");
        let rev_registry = rev_root.join("registry.json");

        let result = StorageContext::review_checked(
            &ws_ctx,
            rev_root.clone(),
            rev_registry,
            rev_storage,
            "pr-6".into(),
        );

        assert!(result.is_ok(), "expected Ok, got {}", result.unwrap_err());
        let ctx = result.unwrap();
        assert_eq!(ctx.workspace_root(), rev_root.as_path());
        assert_eq!(ctx.tantivy_mode(), TantivyOpenMode::ReadOnly);
        assert!(matches!(ctx.label(), ContextLabel::Review { run_id } if run_id == "pr-6"));
    }

    /// Review root is *inside* the workspace root, but review storage and
    /// registry are disjoint from workspace generated state → `Ok`.
    ///
    /// This confirms the "workspace-local review artifacts behind a flag" carve-out
    /// from the master plan: the guard blocks review root *containing* the workspace
    /// root, but allows review root *inside* the workspace root as long as storage
    /// and registry paths are disjoint.
    #[test]
    fn review_checked_accepts_workspace_local_review_outside_storage() {
        let tmp = tempfile::TempDir::new().unwrap();
        let ws_ctx = test_workspace_ctx(&tmp);

        // Review root lives inside the workspace, but under a different prefix.
        let rev_root = tmp.path().join(".gather-step-review");
        let rev_storage = rev_root.join("storage");
        let rev_registry = rev_root.join("registry.json");

        let result = StorageContext::review_checked(
            &ws_ctx,
            rev_root.clone(),
            rev_registry,
            rev_storage,
            "pr-7".into(),
        );

        assert!(result.is_ok(), "expected Ok, got {}", result.unwrap_err());
        let ctx = result.unwrap();
        assert_eq!(ctx.workspace_root(), rev_root.as_path());
    }
}
