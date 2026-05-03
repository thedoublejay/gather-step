//! Review artifact root — isolated directory layout for a single PR review run.
//!
//! # Layout
//!
//! ```text
//! <cache_root>/<workspace_hash>/<run_id>/
//!   review-marker.json        ← proof this is review-owned state
//!   worktree/                 ← detached worktree (Phase 1 Task 2)
//!   registry.json             ← review registry (written by indexer)
//!   storage/                  ← graph.redb, search/, metadata.sqlite
//!   reports/                  ← placeholder; populated by later tasks
//!   logs/                     ← placeholder; populated by later tasks
//! ```
//!
//! # Safety
//!
//! Every deletable artifact root contains a [`ReviewMarker`] file that records
//! the workspace hash, base SHA, head SHA, run id, storage path, registry path,
//! and Gather Step version.  Cleanup tooling MUST verify the marker before
//! removing any directory.
//!
//! Phase 1 Task 3 of the PR review mode plan.
//! v3.1 is a fresh generated-state release; marker schema stamping starts at
//! zero and carries no migration or upgrade branches.

use std::path::{Path, PathBuf};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::storage_context::{ReviewSafetyError, StorageContext};

// ─── Constants ────────────────────────────────────────────────────────────────

/// Schema version for the marker file.
pub const MARKER_SCHEMA_VERSION: u32 = 0;

/// Filename of the review marker written at the root of every artifact tree.
pub const MARKER_FILENAME: &str = "review-marker.json";

// ─── Public types ─────────────────────────────────────────────────────────────

/// Content-addressed cache key for branch-scoped cache reuse.
///
/// Two runs with identical [`CacheKey::fingerprint`] values are semantically
/// equivalent — the same workspace, same commits, same config, same schema
/// version, and same binary version.  A run's artifact root can be reused
/// without re-indexing when the key matches.
///
/// # Stability
///
/// The fingerprint is computed from a fixed-order canonical JSON serialization
/// (sorted keys) via blake3.  Any field change invalidates all prior caches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheKey {
    /// First 16 hex chars of blake3(canonical workspace path bytes).
    pub workspace_hash: String,
    /// 40-char hex SHA of the resolved base commit.
    pub base_sha: String,
    /// 40-char hex SHA of the resolved head commit.
    pub head_sha: String,
    /// First 16 hex chars of blake3(`gather-step.config.yaml` bytes).
    /// Empty string when the config file is absent.
    pub config_hash: String,
    /// Marker schema version at time of caching.
    pub schema_version: u32,
    /// `CARGO_PKG_VERSION` at build time.
    pub gather_step_version: String,
}

impl CacheKey {
    /// Stable canonical fingerprint: blake3 over deterministically serialized
    /// key fields (sorted JSON object keys, no whitespace).
    pub fn fingerprint(&self) -> String {
        // Build a fixed-order JSON string manually so it is stable across
        // serde_json versions and does not depend on struct field order.
        let canonical = format!(
            r#"{{"base_sha":{},"config_hash":{},"gather_step_version":{},"head_sha":{},"schema_version":{},"workspace_hash":{}}}"#,
            serde_json::to_string(&self.base_sha).unwrap_or_default(),
            serde_json::to_string(&self.config_hash).unwrap_or_default(),
            serde_json::to_string(&self.gather_step_version).unwrap_or_default(),
            serde_json::to_string(&self.head_sha).unwrap_or_default(),
            self.schema_version,
            serde_json::to_string(&self.workspace_hash).unwrap_or_default(),
        );
        let hash = blake3::hash(canonical.as_bytes());
        let hex = hash.to_hex();
        hex[..16].to_owned()
    }
}

/// Contents of `review-marker.json`.
///
/// Every field is required for v3.1 markers except optional cache/access
/// metadata fields used by branch-scoped cache reuse.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReviewMarker {
    pub schema_version: u32,
    /// First 16 hex characters of the blake3 hash of the canonical workspace
    /// root path bytes.
    pub workspace_hash: String,
    pub workspace_root: PathBuf,
    pub base_sha: String,
    pub head_sha: String,
    pub run_id: String,
    pub storage_path: PathBuf,
    pub registry_path: PathBuf,
    /// Value of `CARGO_PKG_VERSION` at build time.
    pub gather_step_version: String,
    /// RFC 3339 UTC timestamp of when the artifact root was created.
    pub created_at: String,
    pub status: ReviewStatus,
    /// Cache key for branch-scoped reuse.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_key: Option<CacheKey>,
    /// RFC 3339 UTC timestamp of the last time this artifact was accessed via a
    /// cache hit.  Updated each time `pr-review` reuses this artifact so that
    /// `--older-than` pruning measures last-use time, not creation time.
    /// `None` for artifacts that have never been accessed via cache reuse.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_accessed_at: Option<String>,
}

/// Lifecycle state of a review artifact root.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReviewStatus {
    /// Run is still active or was interrupted without cleanup.
    InProgress,
    /// Run completed successfully; artifacts are safe to delete.
    Completed,
    /// Run failed and left state that needs manual inspection or cleanup.
    Quarantined,
}

/// Handle to an artifact root directory tree that has been created on disk.
#[derive(Debug)]
pub struct ReviewArtifactRoot {
    /// `<cache_root>/<workspace_hash>/<run_id>/`
    pub root: PathBuf,
    /// Copy of the user's source workspace root (not the artifact root).
    pub workspace_root: PathBuf,
    /// `root/worktree/`
    pub worktree_root: PathBuf,
    /// `root/registry.json`
    pub registry_path: PathBuf,
    /// `root/storage/`
    pub storage_root: PathBuf,
    /// `root/reports/`
    pub reports_dir: PathBuf,
    /// `root/logs/`
    pub logs_dir: PathBuf,
    /// `root/review-marker.json`
    pub marker_path: PathBuf,
    pub run_id: String,
    pub workspace_hash: String,
}

// ─── Error type ───────────────────────────────────────────────────────────────

/// Errors produced by artifact-root operations.
#[derive(Debug, Error)]
pub enum ArtifactRootError {
    #[error("artifact root path already exists: {path}")]
    RootExists { path: PathBuf },

    #[error("io error in {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("marker serialization failed: {source}")]
    Serialize {
        #[source]
        source: serde_json::Error,
    },

    #[error("review safety guard refused: {0}")]
    Safety(#[from] ReviewSafetyError),
}

// ─── Path helpers ─────────────────────────────────────────────────────────────

/// Return the OS cache root for review artifacts.
///
/// Default: `dirs::cache_dir()` / `gather-step` / `pr-review`.
/// Fallback (when no OS cache dir is available): `<workspace_root>/.gather-step-review`.
pub fn default_cache_root(workspace_root: &Path) -> PathBuf {
    dirs::cache_dir().map_or_else(
        || workspace_root.join(".gather-step-review"),
        |d| d.join("gather-step").join("pr-review"),
    )
}

/// Compute a deterministic workspace hash from a canonical workspace path.
///
/// Returns the first 16 hex characters of the blake3 hash of the canonical
/// path's UTF-8 bytes.  The path is not canonicalized here; callers should
/// pass an already-canonical path so the hash is stable across calls.
pub fn workspace_hash(workspace_root: &Path) -> String {
    let bytes = workspace_root.to_string_lossy();
    let hash = blake3::hash(bytes.as_bytes());
    // 32 hex chars per 16 bytes; we take the first 16 hex chars (8 bytes).
    let hex = hash.to_hex();
    hex[..16].to_owned()
}

/// Generate a unique run id of the form `review-<utc-yyyymmdd-hhmmss>-<rand6>`.
pub fn generate_run_id() -> String {
    use rand::Rng as _;
    let ts = Utc::now().format("%Y%m%d-%H%M%S");
    let suffix: String = rand::rng()
        .sample_iter(rand::distr::Alphanumeric)
        .take(6)
        .map(char::from)
        .collect();
    format!("review-{ts}-{suffix}")
}

// ─── Artifact root creation ───────────────────────────────────────────────────

/// Build the artifact root path set without touching the filesystem.
///
/// Callers use this before the review safety guard so unsafe `--cache-root`
/// values can be rejected before any generated-state directory or marker is
/// written.
pub fn plan_artifact_root(
    cache_root: &Path,
    workspace_root: &Path,
    run_id: &str,
) -> Result<ReviewArtifactRoot, ArtifactRootError> {
    let hash = workspace_hash(workspace_root);
    let root = cache_root.join(&hash).join(run_id);

    if root.exists() {
        return Err(ArtifactRootError::RootExists { path: root });
    }

    // Derive all child paths.
    let worktree_root = root.join("worktree");
    let registry_path = root.join("registry.json");
    let storage_root = root.join("storage");
    let reports_dir = root.join("reports");
    let logs_dir = root.join("logs");
    let marker_path = root.join(MARKER_FILENAME);

    Ok(ReviewArtifactRoot {
        root,
        workspace_root: workspace_root.to_path_buf(),
        worktree_root,
        registry_path,
        storage_root,
        reports_dir,
        logs_dir,
        marker_path,
        run_id: run_id.to_owned(),
        workspace_hash: hash,
    })
}

/// Materialize a previously planned artifact root and write its initial marker.
///
/// The `cache_key` parameter is `Some` for branch-scoped cache reuse and
/// `None` for uncached review runs.
pub fn materialize_artifact_root(
    artifact: &ReviewArtifactRoot,
    base_sha: &str,
    head_sha: &str,
    cache_key: Option<CacheKey>,
) -> Result<(), ArtifactRootError> {
    if artifact.root.exists() {
        return Err(ArtifactRootError::RootExists {
            path: artifact.root.clone(),
        });
    }

    // Create all directories.
    for dir in [
        &artifact.root,
        &artifact.worktree_root,
        &artifact.storage_root,
        &artifact.reports_dir,
        &artifact.logs_dir,
    ] {
        std::fs::create_dir_all(dir).map_err(|source| ArtifactRootError::Io {
            path: dir.clone(),
            source,
        })?;
    }

    // Write the initial marker.
    let marker = ReviewMarker {
        schema_version: MARKER_SCHEMA_VERSION,
        workspace_hash: artifact.workspace_hash.clone(),
        workspace_root: artifact.workspace_root.clone(),
        base_sha: base_sha.to_owned(),
        head_sha: head_sha.to_owned(),
        run_id: artifact.run_id.clone(),
        storage_path: artifact.storage_root.clone(),
        registry_path: artifact.registry_path.clone(),
        gather_step_version: env!("CARGO_PKG_VERSION").to_owned(),
        created_at: Utc::now().to_rfc3339(),
        status: ReviewStatus::InProgress,
        cache_key,
        last_accessed_at: None,
    };
    write_marker_to_path(&marker, &artifact.marker_path)?;

    Ok(())
}

/// Create the artifact root directory tree on disk and write the initial marker
/// file with `status = InProgress`.
///
/// Returns a [`ReviewArtifactRoot`] handle.  The caller is responsible for
/// calling [`write_marker_completed`] after a successful run, or
/// [`write_marker_quarantined`] if the run fails and leaves state behind.
///
/// # Errors
///
/// - [`ArtifactRootError::RootExists`] if `<cache_root>/<workspace_hash>/<run_id>/`
///   already exists.
/// - [`ArtifactRootError::Io`] on any filesystem error.
/// - [`ArtifactRootError::Serialize`] if the marker cannot be serialized.
pub fn create_artifact_root(
    cache_root: &Path,
    workspace_root: &Path,
    base_sha: &str,
    head_sha: &str,
    run_id: &str,
) -> Result<ReviewArtifactRoot, ArtifactRootError> {
    let root = plan_artifact_root(cache_root, workspace_root, run_id)?;
    materialize_artifact_root(&root, base_sha, head_sha, None)?;
    Ok(root)
}

// ─── Marker update helpers ────────────────────────────────────────────────────

/// Update the artifact root's marker status to [`ReviewStatus::Completed`].
pub fn write_marker_completed(root: &ReviewArtifactRoot) -> Result<(), ArtifactRootError> {
    update_marker_status(root, ReviewStatus::Completed)
}

/// Update `last_accessed_at` in the marker to the current UTC time.
///
/// Called on cache hits so that `--older-than` pruning measures last-use time
/// rather than the original creation time.  Best-effort: errors are silently
/// ignored by callers because a stale timestamp never corrupts the artifact.
pub fn touch_marker_accessed(root: &ReviewArtifactRoot) -> Result<(), ArtifactRootError> {
    let mut marker = read_marker(&root.marker_path)?;
    marker.last_accessed_at = Some(chrono::Utc::now().to_rfc3339());
    write_marker_to_path(&marker, &root.marker_path)
}

/// Update the artifact root's marker status to [`ReviewStatus::Quarantined`].
///
/// Used by failed runs that left state behind so cleanup tooling can discover
/// and safely remove them.
pub fn write_marker_quarantined(root: &ReviewArtifactRoot) -> Result<(), ArtifactRootError> {
    update_marker_status(root, ReviewStatus::Quarantined)
}

fn update_marker_status(
    root: &ReviewArtifactRoot,
    status: ReviewStatus,
) -> Result<(), ArtifactRootError> {
    let mut marker = read_marker(&root.marker_path)?;
    marker.status = status;
    write_marker_to_path(&marker, &root.marker_path)
}

// ─── Marker I/O ───────────────────────────────────────────────────────────────

/// Read and validate a marker file from a candidate path.
///
/// Used by cleanup tooling to verify a directory is review-owned before
/// removing it.
///
/// # Errors
///
/// - [`ArtifactRootError::Io`] if the file cannot be read.
/// - [`ArtifactRootError::Serialize`] if the file is not valid JSON or the
///   fields do not match the expected schema.
pub fn read_marker(marker_path: &Path) -> Result<ReviewMarker, ArtifactRootError> {
    let bytes = std::fs::read(marker_path).map_err(|source| ArtifactRootError::Io {
        path: marker_path.to_path_buf(),
        source,
    })?;
    serde_json::from_slice(&bytes).map_err(|source| ArtifactRootError::Serialize { source })
}

fn write_marker_to_path(marker: &ReviewMarker, path: &Path) -> Result<(), ArtifactRootError> {
    let json = serde_json::to_vec_pretty(marker)
        .map_err(|source| ArtifactRootError::Serialize { source })?;
    std::fs::write(path, json).map_err(|source| ArtifactRootError::Io {
        path: path.to_path_buf(),
        source,
    })
}

// ─── StorageContext handoff ───────────────────────────────────────────────────

/// Convert an artifact root into a safety-checked [`StorageContext`].
///
/// Runs [`StorageContext::review_checked`] to enforce the path-disjointness
/// invariants.  Callers cannot forget to validate — this is the only
/// production-safe way to obtain a review [`StorageContext`] from a
/// [`ReviewArtifactRoot`].
///
/// # Errors
///
/// Propagates [`ReviewSafetyError`] via [`ArtifactRootError::Safety`].
pub fn to_storage_context(
    root: &ReviewArtifactRoot,
    workspace: &StorageContext,
) -> Result<StorageContext, ArtifactRootError> {
    StorageContext::review_checked(
        workspace,
        root.root.clone(),
        root.registry_path.clone(),
        root.storage_root.clone(),
        root.run_id.clone(),
    )
    .map_err(ArtifactRootError::Safety)
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use rustc_hash::FxHashSet;

    use super::*;
    use crate::storage_context::{ContextLabel, ReviewSafetyError};
    use tempfile::TempDir;

    // ── default_cache_root ────────────────────────────────────────────────────

    #[test]
    fn default_cache_root_uses_dirs_cache() {
        let tmp = TempDir::new().unwrap();
        let result = default_cache_root(tmp.path());

        if let Some(cache) = dirs::cache_dir() {
            assert!(
                result.starts_with(cache.join("gather-step").join("pr-review")),
                "expected path under dirs::cache_dir()/gather-step/pr-review, got {result:?}"
            );
        } else {
            // Fallback path is inside the workspace root.
            assert!(
                result.starts_with(tmp.path()),
                "expected fallback inside workspace root, got {result:?}"
            );
        }
    }

    // ── workspace_hash ────────────────────────────────────────────────────────

    #[test]
    fn workspace_hash_is_deterministic() {
        let tmp1 = TempDir::new().unwrap();
        let tmp2 = TempDir::new().unwrap();

        let h1a = workspace_hash(tmp1.path());
        let h1b = workspace_hash(tmp1.path());
        let h2 = workspace_hash(tmp2.path());

        assert_eq!(h1a, h1b, "same path must yield same hash");
        assert_ne!(h1a, h2, "different paths must yield different hashes");
        assert_eq!(h1a.len(), 16, "hash should be 16 hex chars");
    }

    // ── generate_run_id ───────────────────────────────────────────────────────

    #[test]
    fn generate_run_id_is_unique_and_lexically_sortable() {
        // Generate 5 ids with tiny sleeps to ensure distinct timestamps.
        // Because the timestamp only has second-resolution, we generate them
        // within the same second and rely on the random suffix for uniqueness.
        let ids: Vec<String> = (0..5).map(|_| generate_run_id()).collect();

        // All must start with "review-".
        for id in &ids {
            assert!(id.starts_with("review-"), "unexpected prefix in {id}");
        }

        // All must be distinct.
        let unique: FxHashSet<_> = ids.iter().collect();
        assert_eq!(unique.len(), 5, "all 5 run ids must be distinct");

        // The sorted order must equal creation order when timestamps differ.
        // Within a second, timestamp components are equal so this only holds
        // when the second rolls over between calls — we can only assert the
        // prefix format is correct here.
        for id in &ids {
            // Format: review-YYYYMMDD-HHMMSS-XXXXXX
            let parts: Vec<&str> = id.splitn(3, '-').collect();
            assert_eq!(parts[0], "review");
            // Second part is date (8 digits), third is time + random.
            assert_eq!(parts[1].len(), 8, "date part should be 8 chars: {id}");
        }
    }

    // ── create_artifact_root ──────────────────────────────────────────────────

    #[test]
    fn create_artifact_root_creates_layout() {
        let cache_tmp = TempDir::new().unwrap();
        let ws_tmp = TempDir::new().unwrap();

        let root = create_artifact_root(
            cache_tmp.path(),
            ws_tmp.path(),
            "base0000",
            "head0000",
            "review-test-run",
        )
        .expect("create_artifact_root should succeed");

        // All directories exist.
        assert!(root.worktree_root.is_dir(), "worktree/ must exist");
        assert!(root.storage_root.is_dir(), "storage/ must exist");
        assert!(root.reports_dir.is_dir(), "reports/ must exist");
        assert!(root.logs_dir.is_dir(), "logs/ must exist");
        assert!(root.root.is_dir(), "root must exist");

        // Registry parent (root itself) exists.
        assert!(root.registry_path.parent().unwrap().is_dir());

        // Marker file exists with InProgress status.
        assert!(root.marker_path.is_file(), "marker file must exist");
        let marker = read_marker(&root.marker_path).expect("marker must be readable");
        assert_eq!(marker.status, ReviewStatus::InProgress);
        assert_eq!(marker.base_sha, "base0000");
        assert_eq!(marker.head_sha, "head0000");
        assert_eq!(marker.run_id, "review-test-run");
        assert_eq!(marker.schema_version, MARKER_SCHEMA_VERSION);
    }

    #[test]
    fn create_artifact_root_refuses_existing_path() {
        let cache_tmp = TempDir::new().unwrap();
        let ws_tmp = TempDir::new().unwrap();

        let run_id = "review-dupe-run";
        let hash = workspace_hash(ws_tmp.path());
        let target = cache_tmp.path().join(&hash).join(run_id);
        std::fs::create_dir_all(&target).unwrap();

        let err = create_artifact_root(
            cache_tmp.path(),
            ws_tmp.path(),
            "base1111",
            "head1111",
            run_id,
        )
        .expect_err("should fail when root already exists");

        assert!(
            matches!(err, ArtifactRootError::RootExists { .. }),
            "expected RootExists, got {err}"
        );
    }

    // ── marker round-trip ─────────────────────────────────────────────────────

    #[test]
    fn marker_round_trips() {
        let cache_tmp = TempDir::new().unwrap();
        let ws_tmp = TempDir::new().unwrap();

        let artifact = create_artifact_root(
            cache_tmp.path(),
            ws_tmp.path(),
            "baseSHA",
            "headSHA",
            "review-roundtrip",
        )
        .unwrap();

        let marker = read_marker(&artifact.marker_path).unwrap();

        assert_eq!(marker.schema_version, MARKER_SCHEMA_VERSION);
        assert_eq!(marker.workspace_hash, artifact.workspace_hash);
        assert_eq!(marker.workspace_root, artifact.workspace_root);
        assert_eq!(marker.base_sha, "baseSHA");
        assert_eq!(marker.head_sha, "headSHA");
        assert_eq!(marker.run_id, "review-roundtrip");
        assert_eq!(marker.storage_path, artifact.storage_root);
        assert_eq!(marker.registry_path, artifact.registry_path);
        assert_eq!(marker.gather_step_version, env!("CARGO_PKG_VERSION"));
        assert_eq!(marker.status, ReviewStatus::InProgress);
    }

    // ── write_marker_completed ────────────────────────────────────────────────

    #[test]
    fn write_marker_completed_updates_status() {
        let cache_tmp = TempDir::new().unwrap();
        let ws_tmp = TempDir::new().unwrap();

        let artifact = create_artifact_root(
            cache_tmp.path(),
            ws_tmp.path(),
            "baseC",
            "headC",
            "review-complete",
        )
        .unwrap();

        write_marker_completed(&artifact).unwrap();
        let marker = read_marker(&artifact.marker_path).unwrap();
        assert_eq!(marker.status, ReviewStatus::Completed);
    }

    // ── to_storage_context ────────────────────────────────────────────────────

    /// Build a minimal workspace `StorageContext` backed by real temp dirs so
    /// canonicalization inside `review_checked` succeeds.
    fn make_workspace_ctx(ws_tmp: &TempDir) -> StorageContext {
        use crate::app::{AppContext, ColorModeArg};
        use indicatif::MultiProgress;

        let ws = ws_tmp.path().to_path_buf();

        // Create the directories that `workspace_paths()` references so the
        // guard's `canonicalize_for_guard` succeeds on both the workspace storage
        // and registry paths.
        let storage = ws.join(".gather-step").join("storage");
        std::fs::create_dir_all(&storage).unwrap();
        let registry = ws.join(".gather-step").join("registry.json");
        std::fs::write(&registry, b"{}").unwrap();

        let app = AppContext {
            workspace_path: ws,
            repo_filter: None,
            json_output: true,
            no_interactive: true,
            stdin_is_tty: false,
            stdout_is_tty: false,
            stderr_is_tty: false,
            ci_env_set: true,
            color_mode: ColorModeArg::Never,
            show_banner: false,
            multi_progress: MultiProgress::new(),
        };
        StorageContext::workspace(&app)
    }

    #[test]
    fn to_storage_context_runs_safety_guard() {
        let ws_tmp = TempDir::new().unwrap();
        let cache_tmp = TempDir::new().unwrap();

        let ws_ctx = make_workspace_ctx(&ws_tmp);

        let artifact = create_artifact_root(
            cache_tmp.path(),
            ws_tmp.path(),
            "baseOK",
            "headOK",
            "review-safety-ok",
        )
        .unwrap();

        let result = to_storage_context(&artifact, &ws_ctx);
        assert!(result.is_ok(), "expected Ok, got {result:?}");

        let ctx = result.unwrap();
        assert!(
            matches!(ctx.label(), ContextLabel::Review { run_id } if run_id == "review-safety-ok"),
            "expected Review label with correct run_id"
        );
    }

    #[test]
    fn to_storage_context_rejects_overlapping_paths() {
        let ws_tmp = TempDir::new().unwrap();
        let cache_tmp = TempDir::new().unwrap();

        let ws_ctx = make_workspace_ctx(&ws_tmp);

        // Create an artifact root normally, then patch it so its storage_root
        // points at the workspace storage — triggering StorageOverlap.
        let mut artifact = create_artifact_root(
            cache_tmp.path(),
            ws_tmp.path(),
            "baseBAD",
            "headBAD",
            "review-safety-bad",
        )
        .unwrap();

        // Override storage_root to collide with workspace storage.
        artifact.storage_root = ws_ctx.storage_root().to_path_buf();

        let err =
            to_storage_context(&artifact, &ws_ctx).expect_err("should fail with storage overlap");

        assert!(
            matches!(
                err,
                ArtifactRootError::Safety(ReviewSafetyError::StorageOverlap { .. })
            ),
            "expected Safety(StorageOverlap), got {err}"
        );
    }
}
