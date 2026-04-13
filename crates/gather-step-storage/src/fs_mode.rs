//! Filesystem permission helpers for generated state.
//!
//! All generated state (graph database, search index, registry, metadata) is
//! private to the indexing user and should not be world-readable.  On Unix
//! platforms [`apply_private_dir`] / [`apply_private_file`] enforce `0o700` /
//! `0o600` permissions immediately after the file is created or renamed into
//! place.  On non-Unix platforms the helpers are no-ops so the code compiles
//! and runs without change.

use std::{io, path::Path};

/// Apply `0o700` permissions to a directory.
///
/// No-op on non-Unix platforms.
pub fn apply_private_dir(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::{fs, os::unix::fs::PermissionsExt};
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    // On non-Unix the `path` argument is intentionally unused; the function
    // is a no-op.  The parameter is kept for API symmetry with the Unix path.
    let _ = path;
    Ok(())
}

/// Apply `0o600` permissions to a file.
///
/// No-op on non-Unix platforms.
pub fn apply_private_file(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::{fs, os::unix::fs::PermissionsExt};
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    // On non-Unix the `path` argument is intentionally unused; the function
    // is a no-op.  The parameter is kept for API symmetry with the Unix path.
    let _ = path;
    Ok(())
}
