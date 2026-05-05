//! Branch-scoped cache for `pr-review` artifact roots.
//!
//! # Overview
//!
//! `try_reuse_cache` scans the cache root for a prior [`ReviewArtifactRoot`]
//! whose [`CacheKey`] fingerprint matches the current run's key.  When found,
//! the caller can skip worktree creation and indexing entirely.
//!
//! `is_cache_key_active` checks whether a cached key is still valid for the
//! current workspace — used by `pr-review clean --older-than` to protect
//! artifacts the user might need for the next review run.
//!
//! Phase 4 Task 1 of the PR review mode plan.

use std::{
    io,
    path::{Path, PathBuf},
};

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::process::Command;

use anyhow::{Context, Result};
use gather_step_git::refs::resolve_ref;

use super::artifact_root::{
    CacheKey, MARKER_FILENAME, ReviewArtifactRoot, ReviewStatus, read_marker, workspace_hash,
};

/// Compute the 16-char blake3 hex prefix used for cache-key fingerprints.
///
/// Single source of truth so [`compute_cache_key`], [`pick_seed_source`],
/// and the unit-test helpers cannot drift.
fn blake3_hex16(bytes: &[u8]) -> String {
    let hash = blake3::hash(bytes);
    hash.to_hex()[..16].to_owned()
}

// ─── Cache lookup ─────────────────────────────────────────────────────────────

/// Scan `<cache_root>/<workspace_hash>/` for a prior completed artifact whose
/// cache key fingerprint matches `key`.
///
/// Directories are checked most-recently-modified first so we return the
/// freshest hit.  Returns `Ok(None)` when no valid match is found.
///
/// # Eligibility criteria
///
/// 1. Marker deserializes cleanly.
/// 2. Marker `cache_key` is `Some` and its `fingerprint()` equals
///    `key.fingerprint()`.
/// 3. Marker `status == ReviewStatus::Completed`.
/// 4. The artifact `storage/`, `registry.json`, and `worktree/` paths all
///    exist on disk.
pub fn try_reuse_cache(cache_root: &Path, key: &CacheKey) -> Result<Option<ReviewArtifactRoot>> {
    let hash_dir = cache_root.join(&key.workspace_hash);
    if !hash_dir.is_dir() {
        return Ok(None);
    }

    // Collect all run sub-dirs with their mtime so we can sort newest-first.
    let mut candidates: Vec<(std::path::PathBuf, std::time::SystemTime)> =
        std::fs::read_dir(&hash_dir)
            .map_err(|e| anyhow::anyhow!("reading cache hash-dir `{}`: {e}", hash_dir.display()))?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if !path.is_dir() {
                    return None;
                }
                let mtime = entry
                    .metadata()
                    .and_then(|m| m.modified())
                    .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
                Some((path, mtime))
            })
            .collect();

    // Sort newest first.
    candidates.sort_by(|a, b| b.1.cmp(&a.1));

    let target_fp = key.fingerprint();

    for (run_dir, _) in candidates {
        let marker_path = run_dir.join(MARKER_FILENAME);
        let Ok(marker) = read_marker(&marker_path) else {
            continue;
        };

        // Must be completed.
        if !matches!(marker.status, ReviewStatus::Completed) {
            continue;
        }

        // Must have a cache key with a matching fingerprint.
        let Some(cached_key) = &marker.cache_key else {
            continue;
        };
        if cached_key.fingerprint() != target_fp {
            continue;
        }

        // Verify on-disk artifacts still exist.
        let storage_ok = run_dir.join("storage").is_dir();
        let registry_ok = run_dir.join("registry.json").is_file();
        let worktree_ok = run_dir.join("worktree").is_dir();

        if !storage_ok || !registry_ok || !worktree_ok {
            tracing::debug!(
                run_dir = %run_dir.display(),
                storage = storage_ok,
                registry = registry_ok,
                worktree = worktree_ok,
                "cache candidate missing on-disk artifacts; skipping"
            );
            continue;
        }

        // Reconstruct the ReviewArtifactRoot from the on-disk layout. The
        // child-path derivation lives in `from_existing` so it cannot drift
        // from `plan_artifact_root` / `materialize_artifact_root`.
        let root = ReviewArtifactRoot::from_existing(
            run_dir.clone(),
            marker.workspace_root.clone(),
            marker.run_id.clone(),
            marker.workspace_hash.clone(),
        );
        let _ = marker_path; // marker_path is rederived inside `from_existing`

        tracing::info!(
            run_id = %marker.run_id,
            "reusing cached review index"
        );
        return Ok(Some(root));
    }

    Ok(None)
}

// ─── Active-key check ─────────────────────────────────────────────────────────

/// Return `true` when `key` is still active in the workspace — i.e. both
/// `base_sha` and `head_sha` can be resolved against `workspace_root`.
///
/// A key is "active" when the commits it describes are reachable; force-pushes
/// or GC that remove the head SHA make the key inactive and eligible for
/// pruning even with the active-skip protection.
pub fn is_cache_key_active(workspace_root: &Path, key: &CacheKey) -> bool {
    let base_ok = resolve_ref(workspace_root, &key.base_sha).is_ok();
    let head_ok = resolve_ref(workspace_root, &key.head_sha).is_ok();
    base_ok && head_ok
}

/// Compute a content-addressed [`CacheKey`] for a review run.
///
/// `config_content` is the raw bytes of `gather-step.config.yaml`; pass
/// `None` or empty bytes when the file is absent.
pub fn compute_cache_key(
    workspace_root: &Path,
    base_sha: &str,
    head_sha: &str,
    config_content: &[u8],
) -> CacheKey {
    let ws_hash = workspace_hash(workspace_root);

    // blake3 of config bytes (or empty → hash of empty bytes).
    let config_hash = blake3_hex16(config_content);

    CacheKey {
        workspace_hash: ws_hash,
        base_sha: base_sha.to_owned(),
        head_sha: head_sha.to_owned(),
        config_hash,
        schema_version: super::artifact_root::MARKER_SCHEMA_VERSION,
        gather_step_version: env!("CARGO_PKG_VERSION").to_owned(),
    }
}

// ─── Seed-from-baseline ───────────────────────────────────────────────────────

/// Describes the normal workspace index that can be used to pre-populate a
/// fresh review artifact root before the review indexer runs.
pub struct SeedSource {
    /// Path to `<workspace>/.gather-step/registry.json`.
    pub registry_path: PathBuf,
    /// Path to `<workspace>/.gather-step/storage/`.
    pub storage_root: PathBuf,
}

/// Decide whether the workspace's normal index is safe to seed from.
///
/// The seed is safe when all three conditions hold:
/// 1. `<workspace>/.gather-step/storage/graph.redb` exists.
/// 2. `<workspace>/.gather-step/registry.json` exists.
/// 3. The workspace config file (`gather-step.config.yaml`) has a content hash
///    that matches `review_config_hash` — i.e. the schema has not changed.
///
/// Returns `Some(SeedSource)` when seedable, `None` otherwise.
pub fn pick_seed_source(
    workspace_root: &Path,
    review_config_hash: &str,
) -> Result<Option<SeedSource>> {
    let gs_dir = workspace_root.join(".gather-step");
    let storage_root = gs_dir.join("storage");
    let registry_path = gs_dir.join("registry.json");
    let graph_path = storage_root.join("graph.redb");

    // Conditions 1 + 2: required on-disk artifacts.
    if !graph_path.is_file() || !registry_path.is_file() {
        return Ok(None);
    }

    // Condition 3: config hash must match so the schema is compatible.
    let config_file = workspace_root.join("gather-step.config.yaml");
    let config_bytes = std::fs::read(&config_file).unwrap_or_default();
    let ws_config_hash = blake3_hex16(&config_bytes);

    if ws_config_hash != review_config_hash {
        tracing::debug!(
            ws_hash = %ws_config_hash,
            review_hash = %review_config_hash,
            "workspace config hash differs from review config hash; skipping seed"
        );
        return Ok(None);
    }

    Ok(Some(SeedSource {
        registry_path,
        storage_root,
    }))
}

/// Copy `registry.json` and the `storage/` tree from `seed` into
/// `target_artifact_root`.
///
/// Files are first copied with filesystem clone / copy-on-write support where
/// available, then fall back to byte-for-byte copies. Sub-directories are
/// deep-copied and existing files in the target are overwritten.
pub fn seed_artifact_root(
    seed: &SeedSource,
    target_artifact_root: &ReviewArtifactRoot,
) -> Result<()> {
    // Copy registry.json.
    copy_file_fast(&seed.registry_path, &target_artifact_root.registry_path).with_context(
        || {
            format!(
                "copying seed registry from `{}` to `{}`",
                seed.registry_path.display(),
                target_artifact_root.registry_path.display()
            )
        },
    )?;

    // Deep-copy storage/ tree.
    copy_dir_all(&seed.storage_root, &target_artifact_root.storage_root).with_context(|| {
        format!(
            "copying seed storage from `{}` to `{}`",
            seed.storage_root.display(),
            target_artifact_root.storage_root.display()
        )
    })?;

    Ok(())
}

/// Recursively copy all files from `src` into `dst`.  `dst` is created if it
/// does not exist.  Files in `dst` that are not in `src` are left untouched.
fn copy_dir_all(src: &Path, dst: &Path) -> io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            copy_dir_all(&src_path, &dst_path)?;
        } else if file_type.is_file() {
            copy_file_fast(&src_path, &dst_path)?;
        }
        // Symlinks are intentionally skipped — review storage never uses them.
    }
    Ok(())
}

fn copy_file_fast(src: &Path, dst: &Path) -> io::Result<()> {
    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent)?;
    }

    if try_clone_file(src, dst)? {
        return Ok(());
    }

    std::fs::copy(src, dst)?;
    Ok(())
}

fn try_clone_file(src: &Path, dst: &Path) -> io::Result<bool> {
    #[cfg(target_os = "macos")]
    {
        let status = Command::new("/bin/cp").arg("-c").arg(src).arg(dst).status();
        match status {
            Ok(status) if status.success() => Ok(true),
            Ok(_) => Ok(false),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(err) => Err(err),
        }
    }

    #[cfg(target_os = "linux")]
    {
        // Pin to the absolute path so a tampered $PATH cannot substitute a
        // malicious `cp`. macOS uses /bin/cp directly; on Linux the canonical
        // location is the same, but we fall back to a $PATH lookup when
        // /bin/cp is missing (e.g. minimal containers).
        let bin = std::path::Path::new("/bin/cp");
        let mut cmd = if bin.exists() {
            Command::new(bin)
        } else {
            Command::new("cp")
        };
        let status = cmd.arg("--reflink=auto").arg(src).arg(dst).status();
        match status {
            Ok(status) if status.success() => Ok(true),
            Ok(_) => Ok(false),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(err) => Err(err),
        }
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (src, dst);
        Ok(false)
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use super::*;
    use crate::pr_review::artifact_root::{
        CacheKey, MARKER_FILENAME, MARKER_SCHEMA_VERSION, ReviewMarker, ReviewStatus,
        workspace_hash,
    };
    use crate::pr_review::test_helpers::TempDir;

    // ── CacheKey fingerprint ──────────────────────────────────────────────────

    #[test]
    fn fingerprint_is_deterministic() {
        let key = CacheKey {
            workspace_hash: "aabbccdd11223344".to_owned(),
            base_sha: "base000".to_owned(),
            head_sha: "head000".to_owned(),
            config_hash: "cfghash1234567890".to_owned(),
            schema_version: MARKER_SCHEMA_VERSION,
            gather_step_version: "2.3.0".to_owned(),
        };
        let fp1 = key.fingerprint();
        let fp2 = key.fingerprint();
        assert_eq!(fp1, fp2, "fingerprint must be deterministic");
        assert_eq!(fp1.len(), 16, "fingerprint must be 16 hex chars");
    }

    #[test]
    fn fingerprint_differs_on_field_change() {
        let base = CacheKey {
            workspace_hash: "aabbccdd11223344".to_owned(),
            base_sha: "base000".to_owned(),
            head_sha: "head000".to_owned(),
            config_hash: "cfg".to_owned(),
            schema_version: MARKER_SCHEMA_VERSION,
            gather_step_version: "2.3.0".to_owned(),
        };
        let mut changed = base.clone();
        changed.head_sha = "head111".to_owned();
        assert_ne!(
            base.fingerprint(),
            changed.fingerprint(),
            "changed head_sha must change fingerprint"
        );

        let mut changed2 = base.clone();
        changed2.config_hash = "different".to_owned();
        assert_ne!(
            base.fingerprint(),
            changed2.fingerprint(),
            "changed config_hash must change fingerprint"
        );

        let mut changed3 = base.clone();
        changed3.gather_step_version = "0.0.0".to_owned();
        assert_ne!(
            base.fingerprint(),
            changed3.fingerprint(),
            "changed gather_step_version must change fingerprint"
        );
    }

    // ── Helper: write a completed artifact with a given cache key ─────────────

    fn write_cached_artifact(
        cache_root: &Path,
        workspace_root: &Path,
        run_id: &str,
        key: &CacheKey,
        status: ReviewStatus,
    ) -> PathBuf {
        let hash = workspace_hash(workspace_root);
        let root = cache_root.join(&hash).join(run_id);

        // Create required sub-dirs.
        fs::create_dir_all(root.join("storage")).unwrap();
        fs::create_dir_all(root.join("worktree")).unwrap();
        // registry.json must be a file.
        fs::write(root.join("registry.json"), b"{}").unwrap();
        // A small file so the artifact has size.
        fs::write(root.join("storage").join("dummy.txt"), b"data").unwrap();

        let marker = ReviewMarker {
            schema_version: MARKER_SCHEMA_VERSION,
            workspace_hash: hash.clone(),
            workspace_root: workspace_root.to_path_buf(),
            base_sha: key.base_sha.clone(),
            head_sha: key.head_sha.clone(),
            run_id: run_id.to_owned(),
            storage_path: root.join("storage"),
            registry_path: root.join("registry.json"),
            gather_step_version: key.gather_step_version.clone(),
            created_at: chrono::Utc::now().to_rfc3339(),
            status,
            cache_key: Some(key.clone()),
            last_accessed_at: None,
        };
        let json = serde_json::to_vec_pretty(&marker).unwrap();
        fs::write(root.join(MARKER_FILENAME), json).unwrap();

        root
    }

    // ── try_reuse_cache tests ─────────────────────────────────────────────────

    #[test]
    fn try_reuse_cache_finds_matching_completed_artifact() {
        let ws_tmp = TempDir::new("ws");
        let cache_tmp = TempDir::new("cache");

        let key = compute_cache_key(ws_tmp.path(), "base000", "head000", b"config-content");

        let root = write_cached_artifact(
            cache_tmp.path(),
            ws_tmp.path(),
            "review-cached-run",
            &key,
            ReviewStatus::Completed,
        );

        let result = try_reuse_cache(cache_tmp.path(), &key).expect("try_reuse_cache must not err");
        assert!(result.is_some(), "should find matching cached artifact");
        let found = result.unwrap();
        assert_eq!(found.root, root);
        assert_eq!(found.run_id, "review-cached-run");
    }

    #[test]
    fn try_reuse_cache_ignores_incomplete_artifacts() {
        let ws_tmp = TempDir::new("ws2");
        let cache_tmp = TempDir::new("cache2");

        let key = compute_cache_key(ws_tmp.path(), "base111", "head111", b"cfg");

        write_cached_artifact(
            cache_tmp.path(),
            ws_tmp.path(),
            "review-in-progress",
            &key,
            ReviewStatus::InProgress,
        );

        let result = try_reuse_cache(cache_tmp.path(), &key).expect("try_reuse_cache must not err");
        assert!(result.is_none(), "InProgress artifact must not be reused");
    }

    #[test]
    fn try_reuse_cache_returns_none_when_no_match() {
        let ws_tmp = TempDir::new("ws3");
        let cache_tmp = TempDir::new("cache3");

        let key_a = compute_cache_key(ws_tmp.path(), "base000", "head000", b"cfg-a");
        let key_b = compute_cache_key(ws_tmp.path(), "base000", "head111", b"cfg-a");

        write_cached_artifact(
            cache_tmp.path(),
            ws_tmp.path(),
            "review-no-match",
            &key_a,
            ReviewStatus::Completed,
        );

        let result =
            try_reuse_cache(cache_tmp.path(), &key_b).expect("try_reuse_cache must not err");
        assert!(result.is_none(), "different key must not match");
    }

    #[test]
    fn try_reuse_cache_ignores_markers_without_cache_key() {
        let ws_tmp = TempDir::new("ws4");
        let cache_tmp = TempDir::new("cache4");
        let ws = ws_tmp.path();
        let cache = cache_tmp.path();

        let hash = workspace_hash(ws);
        let root = cache.join(&hash).join("review-no-cache-key-run");
        fs::create_dir_all(root.join("storage")).unwrap();
        fs::create_dir_all(root.join("worktree")).unwrap();
        fs::write(root.join("registry.json"), b"{}").unwrap();

        // Uncached marker: no cache_key field.
        let marker = ReviewMarker {
            schema_version: MARKER_SCHEMA_VERSION,
            workspace_hash: hash.clone(),
            workspace_root: ws.to_path_buf(),
            base_sha: "base000".to_owned(),
            head_sha: "head000".to_owned(),
            run_id: "review-no-cache-key-run".to_owned(),
            storage_path: root.join("storage"),
            registry_path: root.join("registry.json"),
            gather_step_version: env!("CARGO_PKG_VERSION").to_owned(),
            created_at: chrono::Utc::now().to_rfc3339(),
            status: ReviewStatus::Completed,
            cache_key: None,
            last_accessed_at: None,
        };
        let json = serde_json::to_vec_pretty(&marker).unwrap();
        fs::write(root.join(MARKER_FILENAME), json).unwrap();

        let key = compute_cache_key(ws, "base000", "head000", b"cfg");
        let result = try_reuse_cache(cache, &key).expect("must not err");
        assert!(result.is_none(), "marker without cache_key must not match");
    }

    #[test]
    fn try_reuse_cache_ignores_missing_on_disk_artifacts() {
        let ws_tmp = TempDir::new("ws5");
        let cache_tmp = TempDir::new("cache5");

        let key = compute_cache_key(ws_tmp.path(), "base999", "head999", b"cfg");

        let root = write_cached_artifact(
            cache_tmp.path(),
            ws_tmp.path(),
            "review-missing-storage",
            &key,
            ReviewStatus::Completed,
        );

        // Remove the storage directory — the artifact is incomplete.
        fs::remove_dir_all(root.join("storage")).unwrap();

        let result = try_reuse_cache(cache_tmp.path(), &key).expect("must not err");
        assert!(
            result.is_none(),
            "artifact with missing storage dir must not be reused"
        );
    }

    // ── is_cache_key_active tests ─────────────────────────────────────────────

    #[test]
    fn is_cache_key_active_returns_false_for_nonexistent_shas() {
        let ws_tmp = TempDir::new("ws6");

        // Not a git repo — resolve_ref will fail for any SHA.
        let key = CacheKey {
            workspace_hash: "dummy".to_owned(),
            base_sha: "deadbeef".repeat(5),
            head_sha: "cafebabe".repeat(5),
            config_hash: "cfg".to_owned(),
            schema_version: MARKER_SCHEMA_VERSION,
            gather_step_version: env!("CARGO_PKG_VERSION").to_owned(),
        };

        assert!(
            !is_cache_key_active(ws_tmp.path(), &key),
            "non-existent SHAs must be inactive"
        );
    }

    // ── compute_cache_key tests ───────────────────────────────────────────────

    #[test]
    fn compute_cache_key_produces_stable_hash() {
        let ws_tmp = TempDir::new("ws7");
        let config = b"repos:\n  - name: myrepo\n    path: myrepo\n";

        let key1 = compute_cache_key(ws_tmp.path(), "aaa", "bbb", config);
        let key2 = compute_cache_key(ws_tmp.path(), "aaa", "bbb", config);
        assert_eq!(key1.config_hash, key2.config_hash);
        assert_eq!(key1.fingerprint(), key2.fingerprint());
    }

    #[test]
    fn compute_cache_key_differs_on_config_change() {
        let ws_tmp = TempDir::new("ws8");

        let key_a = compute_cache_key(ws_tmp.path(), "aaa", "bbb", b"config-a");
        let key_b = compute_cache_key(ws_tmp.path(), "aaa", "bbb", b"config-b");

        assert_ne!(
            key_a.config_hash, key_b.config_hash,
            "different config bytes must produce different config_hash"
        );
        assert_ne!(
            key_a.fingerprint(),
            key_b.fingerprint(),
            "different config must change fingerprint"
        );
    }

    // ── pick_seed_source tests ────────────────────────────────────────────────

    /// Compute a 16-char blake3 hex hash of `bytes`. Thin wrapper around
    /// the production helper to keep the call sites readable in tests.
    fn config_hash_of(bytes: &[u8]) -> String {
        super::blake3_hex16(bytes)
    }

    /// Build a minimal workspace fixture:
    /// ```text
    /// ws/
    ///   gather-step.config.yaml
    ///   .gather-step/
    ///     storage/
    ///       graph.redb   (empty placeholder)
    ///     registry.json  (empty JSON object)
    /// ```
    fn write_indexed_workspace(ws: &Path, config_bytes: &[u8]) {
        fs::write(ws.join("gather-step.config.yaml"), config_bytes).unwrap();
        let gs = ws.join(".gather-step");
        let storage = gs.join("storage");
        fs::create_dir_all(&storage).unwrap();
        fs::write(storage.join("graph.redb"), b"placeholder").unwrap();
        fs::write(gs.join("registry.json"), b"{}").unwrap();
    }

    #[test]
    fn pick_seed_source_returns_some_when_workspace_indexed() {
        let ws_tmp = TempDir::new("seed-ws1");
        let config_bytes = b"repos:\n  - name: myrepo\n    path: myrepo\n";
        write_indexed_workspace(ws_tmp.path(), config_bytes);

        let config_hash = config_hash_of(config_bytes);
        let result =
            pick_seed_source(ws_tmp.path(), &config_hash).expect("pick_seed_source must not err");

        assert!(
            result.is_some(),
            "should return Some when workspace is indexed and config hash matches"
        );
        let seed = result.unwrap();
        assert!(seed.registry_path.is_file());
        assert!(seed.storage_root.is_dir());
    }

    #[test]
    fn pick_seed_source_returns_none_when_config_hash_differs() {
        let ws_tmp = TempDir::new("seed-ws2");
        let config_bytes = b"repos:\n  - name: myrepo\n    path: myrepo\n";
        write_indexed_workspace(ws_tmp.path(), config_bytes);

        // Use a hash computed from different bytes.
        let mismatched_hash = config_hash_of(b"repos:\n  - name: other\n    path: other\n");
        let result = pick_seed_source(ws_tmp.path(), &mismatched_hash)
            .expect("pick_seed_source must not err");

        assert!(
            result.is_none(),
            "should return None when config hashes differ"
        );
    }

    #[test]
    fn pick_seed_source_returns_none_when_workspace_not_indexed() {
        let ws_tmp = TempDir::new("seed-ws3");
        // Write config but no .gather-step/storage/graph.redb.
        let config_bytes = b"repos:\n  - name: myrepo\n    path: myrepo\n";
        fs::write(ws_tmp.path().join("gather-step.config.yaml"), config_bytes).unwrap();

        let config_hash = config_hash_of(config_bytes);
        let result =
            pick_seed_source(ws_tmp.path(), &config_hash).expect("pick_seed_source must not err");

        assert!(
            result.is_none(),
            "should return None when workspace has not been indexed"
        );
    }

    // ── seed_artifact_root tests ──────────────────────────────────────────────

    #[test]
    fn seed_artifact_root_copies_storage_files() {
        let ws_tmp = TempDir::new("seed-ws4");
        let cache_tmp = TempDir::new("seed-cache4");

        // Build a source workspace with indexed data.
        let config_bytes = b"repos:\n  - name: myrepo\n    path: myrepo\n";
        write_indexed_workspace(ws_tmp.path(), config_bytes);

        // Write an extra file in storage to verify deep copy.
        let storage = ws_tmp.path().join(".gather-step/storage");
        fs::write(storage.join("meta.db"), b"metadata").unwrap();

        let seed = SeedSource {
            registry_path: ws_tmp.path().join(".gather-step/registry.json"),
            storage_root: storage.clone(),
        };

        // Create a minimal artifact root (just needs the paths to exist).
        let key = compute_cache_key(ws_tmp.path(), "base", "head", config_bytes);
        let artifact = write_cached_artifact(
            cache_tmp.path(),
            ws_tmp.path(),
            "seed-test-run",
            &key,
            ReviewStatus::InProgress,
        );

        // Reconstruct a ReviewArtifactRoot from the on-disk layout.
        let artifact_root = ReviewArtifactRoot {
            root: artifact.clone(),
            workspace_root: ws_tmp.path().to_path_buf(),
            worktree_root: artifact.join("worktree"),
            registry_path: artifact.join("registry.json"),
            storage_root: artifact.join("storage"),
            reports_dir: artifact.join("reports"),
            logs_dir: artifact.join("logs"),
            marker_path: artifact.join(MARKER_FILENAME),
            run_id: "seed-test-run".to_owned(),
            workspace_hash: workspace_hash(ws_tmp.path()),
        };

        seed_artifact_root(&seed, &artifact_root).expect("seed_artifact_root must not err");

        // Verify registry copied.
        assert!(
            artifact_root.registry_path.is_file(),
            "registry.json should exist in target"
        );
        // Verify storage files copied.
        assert!(
            artifact_root.storage_root.join("graph.redb").is_file(),
            "graph.redb should be copied"
        );
        assert!(
            artifact_root.storage_root.join("meta.db").is_file(),
            "meta.db should be deep-copied"
        );
    }

    #[test]
    fn seed_artifact_root_overwrites_target_with_independent_copy() {
        let ws_tmp = TempDir::new("seed-ws5");
        let cache_tmp = TempDir::new("seed-cache5");

        let config_bytes = b"repos:\n  - name: myrepo\n    path: myrepo\n";
        write_indexed_workspace(ws_tmp.path(), config_bytes);

        let storage = ws_tmp.path().join(".gather-step/storage");
        fs::write(storage.join("meta.db"), b"seed-metadata").unwrap();
        fs::write(
            ws_tmp.path().join(".gather-step/registry.json"),
            b"{\"seed\":true}",
        )
        .unwrap();

        let seed = SeedSource {
            registry_path: ws_tmp.path().join(".gather-step/registry.json"),
            storage_root: storage.clone(),
        };

        let key = compute_cache_key(ws_tmp.path(), "base", "head", config_bytes);
        let artifact = write_cached_artifact(
            cache_tmp.path(),
            ws_tmp.path(),
            "seed-test-overwrite",
            &key,
            ReviewStatus::InProgress,
        );
        fs::write(artifact.join("registry.json"), b"{\"old\":true}").unwrap();
        fs::write(artifact.join("storage").join("meta.db"), b"old-metadata").unwrap();

        let artifact_root = ReviewArtifactRoot {
            root: artifact.clone(),
            workspace_root: ws_tmp.path().to_path_buf(),
            worktree_root: artifact.join("worktree"),
            registry_path: artifact.join("registry.json"),
            storage_root: artifact.join("storage"),
            reports_dir: artifact.join("reports"),
            logs_dir: artifact.join("logs"),
            marker_path: artifact.join(MARKER_FILENAME),
            run_id: "seed-test-overwrite".to_owned(),
            workspace_hash: workspace_hash(ws_tmp.path()),
        };

        seed_artifact_root(&seed, &artifact_root).expect("seed_artifact_root must not err");

        assert_eq!(
            fs::read(&artifact_root.registry_path).unwrap(),
            b"{\"seed\":true}"
        );
        assert_eq!(
            fs::read(artifact_root.storage_root.join("meta.db")).unwrap(),
            b"seed-metadata"
        );

        fs::write(&seed.registry_path, b"{\"seed\":false}").unwrap();
        fs::write(storage.join("meta.db"), b"changed-source").unwrap();

        assert_eq!(
            fs::read(&artifact_root.registry_path).unwrap(),
            b"{\"seed\":true}",
            "target registry should not alias the source after copy"
        );
        assert_eq!(
            fs::read(artifact_root.storage_root.join("meta.db")).unwrap(),
            b"seed-metadata",
            "target storage file should not alias the source after copy"
        );
    }
}
