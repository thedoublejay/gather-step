/// Path-safety helpers for the CLI.
///
/// All path-handling in the CLI distinguishes three operations:
///
/// 1. **Lexical normalization** — resolves `.` and `..` components without
///    touching the filesystem (no symlink resolution, no I/O).
/// 2. **Canonicalization** — resolves symlinks via `std::fs::canonicalize`,
///    which requires the path to exist on disk.
/// 3. **Root-prefix validation** — verifies the canonical result lives inside
///    the canonical workspace root via `Path::strip_prefix`.
///
/// These three operations are intentionally separate so callers know exactly
/// which guarantees they hold.
use std::{
    io,
    path::{Path, PathBuf},
};

use thiserror::Error;

/// Errors produced by path-safety operations.
#[derive(Debug, Error)]
pub enum PathSafetyError {
    /// A resolved path escapes the workspace root.
    #[error(
        "Path escapes the workspace: `{path}` resolves outside workspace root `{workspace_root}`."
    )]
    PathEscape {
        path: PathBuf,
        workspace_root: PathBuf,
    },

    /// A `.git` symlink target resolves outside the workspace root.
    #[error(
        "Git symlink escapes the workspace: link `{link_path}` -> `{target}` resolves outside workspace root."
    )]
    GitSymlinkEscape { link_path: PathBuf, target: PathBuf },

    /// A path component of the generated-state directory is a symlink.
    ///
    /// Generated state must live at a real (non-symlinked) path to prevent
    /// TOCTOU races and to ensure that private-permission enforcement is
    /// applied to the intended location on disk.
    #[error(
        "Generated-state path `{symlink_path}` is a symlink. \
         Remove the symlink and use a real directory."
    )]
    GeneratedStateSymlink { symlink_path: PathBuf },

    /// An I/O error occurred while performing path operations.
    #[error("Path safety I/O error for `{path}`: {source}.")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

/// Canonicalize `raw` to produce the workspace root that all subsequent
/// path operations will validate against.
///
/// This function must be called **once** at CLI startup. The returned
/// [`PathBuf`] is the canonical (symlink-resolved, absolute) workspace root.
///
/// # Errors
///
/// Returns [`PathSafetyError::Io`] if the directory does not exist or cannot
/// be accessed.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
/// use gather_step::path_safety::canonical_workspace_root;
///
/// let root = canonical_workspace_root(Path::new("/tmp/my-workspace")).unwrap();
/// assert!(root.is_absolute());
/// ```
pub fn canonical_workspace_root(raw: &Path) -> Result<PathBuf, PathSafetyError> {
    std::fs::canonicalize(raw).map_err(|source| PathSafetyError::Io {
        path: raw.to_path_buf(),
        source,
    })
}

/// Canonicalize `input` and verify that the result lives inside
/// `canonical_root`.
///
/// - If `input` is relative it is joined against `canonical_root` first.
/// - If `input` does not exist on disk, the function walks up the path
///   components to find the longest-existing prefix, canonicalizes that
///   prefix, appends the remaining components lexically, and then validates.
///   This means callers can safely check paths that will be created shortly.
/// - If the resolved path does not start with `canonical_root`, this function
///   returns [`PathSafetyError::PathEscape`].
///
/// # Errors
///
/// - [`PathSafetyError::Io`] — the path (or its existing prefix) cannot be
///   read.
/// - [`PathSafetyError::PathEscape`] — the resolved path escapes the
///   workspace root.
///
/// # Examples
///
/// ```no_run
/// use std::{fs, path::PathBuf};
/// use gather_step::path_safety::canonicalize_inside_workspace;
///
/// let root: PathBuf = "/tmp/ws".into();
/// let inside = root.join("subdir/file.txt");
/// let canonical = canonicalize_inside_workspace(&inside, &root).unwrap();
/// assert!(canonical.starts_with(&root));
/// ```
pub fn canonicalize_inside_workspace(
    input: &Path,
    canonical_root: &Path,
) -> Result<PathBuf, PathSafetyError> {
    // Make the input absolute (join relative paths against the workspace root).
    let absolute = if input.is_absolute() {
        input.to_path_buf()
    } else {
        canonical_root.join(input)
    };

    // Try a direct canonicalize first (works when the path exists).
    match std::fs::canonicalize(&absolute) {
        Ok(canonical) => {
            if canonical.starts_with(canonical_root) {
                return Ok(canonical);
            }
            return Err(PathSafetyError::PathEscape {
                path: absolute,
                workspace_root: canonical_root.to_path_buf(),
            });
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            // Fall through to the partial-canonicalization path below.
        }
        Err(source) => {
            return Err(PathSafetyError::Io {
                path: absolute,
                source,
            });
        }
    }

    // The path does not exist yet.  Walk up to find the longest existing
    // ancestor, canonicalize it, then re-append the remaining components.
    let mut components: Vec<_> = absolute.components().collect();
    let mut remaining = Vec::new();
    loop {
        if components.is_empty() {
            // Nothing exists — fall back to the root itself.
            let suffix: PathBuf = remaining.iter().rev().collect();
            let joined = canonical_root.join(suffix);
            let result = lexically_normalize(&joined);
            if result.starts_with(canonical_root) {
                return Ok(result);
            }
            return Err(PathSafetyError::PathEscape {
                path: absolute,
                workspace_root: canonical_root.to_path_buf(),
            });
        }

        let candidate: PathBuf = components.iter().collect();
        match std::fs::canonicalize(&candidate) {
            Ok(canonical_prefix) => {
                // Append remaining components lexically, then normalize away
                // any `..` / `.` before checking the root prefix.  Without
                // this normalization a suffix like `missing/../../../escape`
                // would pass the `starts_with` check while resolving outside
                // the workspace.
                let suffix: PathBuf = remaining.iter().rev().collect();
                let joined = canonical_prefix.join(suffix);
                let result = lexically_normalize(&joined);
                if result.starts_with(canonical_root) {
                    return Ok(result);
                }
                return Err(PathSafetyError::PathEscape {
                    path: absolute,
                    workspace_root: canonical_root.to_path_buf(),
                });
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                remaining.push(components.pop().expect("components is non-empty"));
            }
            Err(source) => {
                return Err(PathSafetyError::Io {
                    path: candidate,
                    source,
                });
            }
        }
    }
}

/// Lexically normalize `p` by collapsing `.` and `..` components without
/// touching the filesystem (no symlink resolution, no I/O).
///
/// Rules:
/// - `CurDir` (`.`) components are dropped.
/// - `ParentDir` (`..`) pops the previous `Normal` component when one exists.
///   A `..` that would pop above a `RootDir` or `Prefix` is silently dropped
///   (the root cannot be escaped by lexical normalization alone).
///   A `..` with no preceding `Normal` component — or preceded by another `..`
///   — is kept in place (relative paths that legitimately start with `..`).
pub(crate) fn lexically_normalize(p: &Path) -> PathBuf {
    use std::path::Component;
    let mut out: Vec<Component<'_>> = Vec::with_capacity(p.components().count());
    for comp in p.components() {
        match comp {
            Component::ParentDir => {
                match out.last() {
                    Some(Component::Normal(_)) => {
                        out.pop();
                    }
                    Some(Component::ParentDir) | None => out.push(comp),
                    Some(Component::RootDir | Component::Prefix(_)) => {
                        // Cannot pop the filesystem root; silently drop the `..`.
                    }
                    Some(Component::CurDir) => unreachable!("CurDir is never pushed"),
                }
            }
            Component::CurDir => { /* skip */ }
            _ => out.push(comp),
        }
    }
    out.iter().collect()
}

/// Verify that no path component between `workspace_root` and
/// `generated_state_path` (inclusive) is a symlink.
///
/// Generated state (graph database, search index, registry, and metadata)
/// must live at a real directory path.  If the `.gather-step` directory — or
/// any of its parents between the workspace root and the storage location —
/// were a symlink an attacker could redirect writes to an arbitrary location
/// on the filesystem after the indexer has checked permissions but before it
/// opens the files (a TOCTOU race).  Rejecting symlinks at startup eliminates
/// that class of attack.
///
/// The check uses [`std::fs::symlink_metadata`] rather than
/// [`std::fs::metadata`] so that a symlink entry itself is detected even when
/// the target also exists.
///
/// Non-existent path components are silently skipped — they will be created
/// by the caller, and creation never produces a symlink.
///
/// # Errors
///
/// - [`PathSafetyError::GeneratedStateSymlink`] — a component of the path is
///   a symbolic link.
/// - [`PathSafetyError::Io`] — a filesystem error occurred while reading
///   metadata.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
/// use gather_step::path_safety::reject_symlinked_generated_state;
///
/// let workspace = Path::new("/tmp/ws");
/// let storage = workspace.join(".gather-step/storage");
/// reject_symlinked_generated_state(workspace, &storage).unwrap();
/// ```
pub fn reject_symlinked_generated_state(
    workspace_root: &Path,
    generated_state_path: &Path,
) -> Result<(), PathSafetyError> {
    match std::fs::symlink_metadata(workspace_root) {
        Ok(meta) if meta.file_type().is_symlink() => {
            return Err(PathSafetyError::GeneratedStateSymlink {
                symlink_path: workspace_root.to_path_buf(),
            });
        }
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(PathSafetyError::Io {
                path: workspace_root.to_path_buf(),
                source,
            });
        }
    }

    // Collect the suffix of `generated_state_path` that extends beyond
    // `workspace_root`, then check each component in order.
    let relative = generated_state_path
        .strip_prefix(workspace_root)
        .unwrap_or(generated_state_path);

    let mut current = workspace_root.to_path_buf();
    for component in relative.components() {
        current.push(component);
        match std::fs::symlink_metadata(&current) {
            Ok(meta) if meta.file_type().is_symlink() => {
                return Err(PathSafetyError::GeneratedStateSymlink {
                    symlink_path: current,
                });
            }
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                // Path component does not exist yet — will be created by caller.
            }
            Err(source) => {
                return Err(PathSafetyError::Io {
                    path: current,
                    source,
                });
            }
        }
    }
    Ok(())
}

/// Return the portion of `absolute` that is relative to `canonical_root`.
///
/// This is the inverse of joining a relative path against the workspace root.
/// It is intended for display and MCP output redaction (Task 5) to strip the
/// workspace root from paths before they appear in output.
///
/// # Errors
///
/// Returns [`PathSafetyError::PathEscape`] if `absolute` does not start with
/// `canonical_root`.
///
/// # Examples
///
/// ```no_run
/// use std::path::PathBuf;
/// use gather_step::path_safety::relative_to_workspace;
///
/// let root: PathBuf = "/tmp/ws".into();
/// let absolute: PathBuf = "/tmp/ws/subdir/file.txt".into();
/// let relative = relative_to_workspace(&absolute, &root).unwrap();
/// assert_eq!(relative, PathBuf::from("subdir/file.txt"));
/// ```
pub fn relative_to_workspace(
    absolute: &Path,
    canonical_root: &Path,
) -> Result<PathBuf, PathSafetyError> {
    absolute
        .strip_prefix(canonical_root)
        .map(Path::to_path_buf)
        .map_err(|_| PathSafetyError::PathEscape {
            path: absolute.to_path_buf(),
            workspace_root: canonical_root.to_path_buf(),
        })
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use super::{
        canonical_workspace_root, canonicalize_inside_workspace, reject_symlinked_generated_state,
        relative_to_workspace,
    };

    #[test]
    fn canonical_workspace_root_returns_absolute_path() {
        let tmp = tempfile::tempdir().unwrap();
        let root = canonical_workspace_root(tmp.path()).unwrap();
        assert!(root.is_absolute());
    }

    #[test]
    fn canonical_workspace_root_errors_on_missing_dir() {
        let missing = PathBuf::from("/tmp/this-path-definitely-does-not-exist-gather-step-test");
        let err = canonical_workspace_root(&missing).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("Path safety I/O error"), "got: {msg}");
    }

    #[test]
    fn relative_to_workspace_strips_root() {
        let root = PathBuf::from("/tmp/ws");
        let absolute = PathBuf::from("/tmp/ws/subdir/file.txt");
        let rel = relative_to_workspace(&absolute, &root).unwrap();
        assert_eq!(rel, PathBuf::from("subdir/file.txt"));
    }

    #[test]
    fn relative_to_workspace_rejects_outside_path() {
        let root = PathBuf::from("/tmp/ws");
        let outside = PathBuf::from("/etc/passwd");
        relative_to_workspace(&outside, &root).unwrap_err();
    }

    #[test]
    fn canonicalize_inside_workspace_accepts_non_existing_path_inside_root() {
        let tmp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(tmp.path()).unwrap();
        // Path does not exist yet — should still be accepted.
        let not_yet = root.join("will-be-created/file.txt");
        let resolved = canonicalize_inside_workspace(&not_yet, &root).unwrap();
        assert!(resolved.starts_with(&root));
    }

    #[test]
    fn reject_symlinked_generated_state_accepts_real_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(tmp.path()).unwrap();
        let storage = root.join(".gather-step").join("storage");
        fs::create_dir_all(&storage).unwrap();
        // Real directory — must be accepted.
        reject_symlinked_generated_state(&root, &storage).unwrap();
    }

    #[test]
    fn reject_symlinked_generated_state_accepts_missing_path() {
        let tmp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(tmp.path()).unwrap();
        let storage = root.join(".gather-step").join("storage");
        // Path does not exist yet — must be accepted (will be created later).
        reject_symlinked_generated_state(&root, &storage).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn reject_symlinked_generated_state_rejects_symlinked_dot_gather_step() {
        let tmp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(tmp.path()).unwrap();

        // Create a real target directory outside the workspace.
        let target = tmp.path().join("real-gather-step-target");
        fs::create_dir_all(&target).unwrap();

        // Symlink `.gather-step` to the target.
        let link = root.join(".gather-step");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let storage = link.join("storage");
        let err = reject_symlinked_generated_state(&root, &storage).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("symlink"),
            "expected symlink error message, got: {msg}"
        );
    }
}
