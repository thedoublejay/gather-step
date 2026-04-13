use std::{
    fs,
    path::{Component, Path, PathBuf},
};

pub(crate) fn canonicalize_existing_dir_under(path: &Path, root: &Path) -> Option<PathBuf> {
    let canonical_root = fs::canonicalize(root).ok()?;
    if has_symlink_component_under(path, root, &canonical_root) {
        return None;
    }
    let metadata = fs::symlink_metadata(path).ok()?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return None;
    }
    let canonical = fs::canonicalize(path).ok()?;
    canonical.starts_with(canonical_root).then_some(canonical)
}

pub(crate) fn canonicalize_existing_file_under_any(
    path: &Path,
    roots: &[PathBuf],
) -> Option<PathBuf> {
    let metadata = fs::symlink_metadata(path).ok()?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return None;
    }
    let canonical = fs::canonicalize(path).ok()?;
    roots.iter().find_map(|root| {
        let canonical_root = fs::canonicalize(root).unwrap_or_else(|_| root.clone());
        if canonical.starts_with(&canonical_root)
            && !has_symlink_component_under(path, root, &canonical_root)
        {
            Some(canonical.clone())
        } else {
            None
        }
    })
}

pub(crate) fn canonicalize_existing_file_under(path: &Path, root: &Path) -> Option<PathBuf> {
    canonicalize_existing_file_under_any(path, &[root.to_path_buf()])
}

pub(crate) fn normalize_relative_path(path: &Path) -> Option<PathBuf> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(part) => normalized.push(part),
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    Some(normalized)
}

fn has_symlink_component_under(path: &Path, root: &Path, canonical_root: &Path) -> bool {
    let Some(trusted_prefix) = trusted_root_prefix(path, root, canonical_root) else {
        return false;
    };
    let Ok(relative) = path.strip_prefix(&trusted_prefix) else {
        return false;
    };
    let mut current = trusted_prefix;
    for component in relative.components() {
        current.push(component.as_os_str());
        if fs::symlink_metadata(&current).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
            return true;
        }
    }
    false
}

fn trusted_root_prefix(path: &Path, root: &Path, canonical_root: &Path) -> Option<PathBuf> {
    if path.starts_with(root) {
        return Some(root.to_path_buf());
    }
    if path.starts_with(canonical_root) {
        return Some(canonical_root.to_path_buf());
    }

    let mut prefix = PathBuf::new();
    for component in path.components() {
        prefix.push(component.as_os_str());
        if fs::canonicalize(&prefix).ok().as_deref() == Some(canonical_root) {
            return Some(prefix);
        }
    }
    None
}
