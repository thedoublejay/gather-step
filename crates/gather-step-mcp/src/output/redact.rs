//! Output redaction helpers for MCP and CLI rendering.
//!
//! Absolute filesystem paths must never appear in MCP tool outputs because
//! they expose the caller's machine layout to shared LLM context.  Apply
//! [`relativize_to_workspace`] at every user-facing rendering site.

use std::path::Path;

/// Strip the `workspace_root` prefix from `path`, returning the relative
/// display string.
///
/// If `path` does not start with `workspace_root` (e.g. a truly external
/// path), the sentinel `"<outside-workspace>"` is returned so the output
/// remains safe but the caller is alerted that a path escaped the workspace.
///
/// # Examples
///
/// ```
/// use std::path::Path;
/// use gather_step_mcp::output::redact::relativize_to_workspace;
///
/// let root = Path::new("/tmp/ws");
/// let abs  = Path::new("/tmp/ws/repo/src/main.rs");
/// assert_eq!(relativize_to_workspace(abs, root), "repo/src/main.rs");
///
/// let outside = Path::new("/etc/passwd");
/// assert_eq!(relativize_to_workspace(outside, root), "<outside-workspace>");
/// ```
#[must_use]
pub fn relativize_to_workspace(path: &Path, workspace_root: &Path) -> String {
    path.strip_prefix(workspace_root).map_or_else(
        |_| "<outside-workspace>".to_owned(),
        |rel| rel.display().to_string(),
    )
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::relativize_to_workspace;

    #[test]
    fn strips_workspace_prefix() {
        let root = Path::new("/home/user/ws");
        let abs = Path::new("/home/user/ws/service-a/src/lib.rs");
        assert_eq!(relativize_to_workspace(abs, root), "service-a/src/lib.rs");
    }

    #[test]
    fn returns_sentinel_for_outside_path() {
        let root = Path::new("/home/user/ws");
        let outside = Path::new("/etc/hosts");
        assert_eq!(
            relativize_to_workspace(outside, root),
            "<outside-workspace>"
        );
    }

    #[test]
    fn returns_empty_string_for_exact_root() {
        let root = Path::new("/home/user/ws");
        assert_eq!(relativize_to_workspace(root, root), "");
    }
}
