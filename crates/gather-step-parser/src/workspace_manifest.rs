//! Workspace-local package discovery for JavaScript/TypeScript monorepos.
//!
//! When a repo lives inside a Bun, PNPM, or npm `workspaces` monorepo, imports
//! like `import { Foo } from '@scope/contracts'` refer to a sibling package
//! rather than an npm package in `node_modules`. This module reads the root
//! `package.json`'s `workspaces` field, locates each matched directory, and
//! extracts the package name and entry-point path so that `PathAliases` can
//! resolve such imports without requiring `node_modules` to be installed.
//!
//! # Design decisions
//!
//! - All failures are silent: missing files, malformed JSON, and unknown
//!   patterns produce an empty result rather than an error. The workspace root
//!   detection is a best-effort hint, not a hard requirement.
//! - No external glob crate is used. Single-level `packages/*` patterns are
//!   handled with `std::fs::read_dir`; multi-level patterns use a small
//!   recursive walk.
//! - No `regex` crate is used. Pattern matching uses simple string operations.

use std::{
    fs,
    path::{Path, PathBuf},
};

use tracing::trace;

use crate::path_guard::{
    canonicalize_existing_dir_under, canonicalize_existing_file_under, normalize_relative_path,
};

/// A workspace-local package discovered from a monorepo `workspaces` manifest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkspacePackage {
    /// The npm package name, e.g. `"@workspace/contracts"`.
    pub name: String,
    /// Absolute path to the package root directory.
    pub root: PathBuf,
    /// Absolute path to the package entry-point file.
    ///
    /// Resolved from (in priority order):
    /// 1. `exports["."].import`
    /// 2. `exports["."].default`
    /// 3. `exports["."]` (when it is a plain string)
    /// 4. `exports` (when it is a plain string)
    /// 5. `main`
    /// 6. `index.ts` → `index.tsx` → `index.js` → `index.mjs` (fallback)
    pub main_entry: PathBuf,
}

/// Discover all workspace-local packages under `workspace_root`.
///
/// Reads `<workspace_root>/package.json`, extracts the `workspaces` array,
/// expands each glob-like pattern into concrete directories, and for each
/// matched directory reads its own `package.json` to obtain the `name` and
/// entry-point path.
///
/// Returns an empty `Vec` on any failure (missing root manifest, malformed
/// JSON, no `workspaces` field, etc.). Individual package discovery errors
/// are silently skipped.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
/// use gather_step_parser::workspace_manifest::discover_workspace_packages;
///
/// let packages = discover_workspace_packages(Path::new("/workspace"));
/// for pkg in &packages {
///     println!("{} -> {}", pkg.name, pkg.main_entry.display());
/// }
/// ```
#[must_use]
pub fn discover_workspace_packages(workspace_root: &Path) -> Vec<WorkspacePackage> {
    let root_manifest_path = workspace_root.join("package.json");
    let Some(root_manifest_path) =
        canonicalize_existing_file_under(&root_manifest_path, workspace_root)
    else {
        trace!(
            path = %root_manifest_path.display(),
            "workspace root package.json not found — skipping workspace discovery"
        );
        return Vec::new();
    };
    let Ok(raw) = fs::read_to_string(&root_manifest_path) else {
        trace!(
            path = %root_manifest_path.display(),
            "workspace root package.json is unreadable — skipping workspace discovery"
        );
        return Vec::new();
    };
    let Ok(root_manifest) = serde_json::from_str::<serde_json::Value>(&raw) else {
        trace!(
            path = %root_manifest_path.display(),
            "workspace root package.json is malformed JSON — skipping workspace discovery"
        );
        return Vec::new();
    };

    let Some(workspaces_value) = root_manifest.get("workspaces") else {
        trace!(
            path = %root_manifest_path.display(),
            "no `workspaces` field in root package.json — not a monorepo workspace root"
        );
        return Vec::new();
    };

    // `workspaces` can be an array of patterns directly, or (in some Bun
    // configs) an object with a `packages` key.
    let patterns: Vec<String> = if let Some(array) = workspaces_value.as_array() {
        array
            .iter()
            .filter_map(|v| v.as_str().map(ToOwned::to_owned))
            .collect()
    } else if let Some(obj) = workspaces_value.as_object() {
        // Yarn/Bun: `"workspaces": { "packages": ["packages/*"] }`
        obj.get("packages")
            .and_then(|v| v.as_array())
            .map(|array| {
                array
                    .iter()
                    .filter_map(|v| v.as_str().map(ToOwned::to_owned))
                    .collect()
            })
            .unwrap_or_default()
    } else {
        return Vec::new();
    };

    let mut packages = Vec::new();
    for pattern in &patterns {
        let matched_dirs = expand_workspace_pattern(workspace_root, pattern);
        for dir in matched_dirs {
            if let Some(pkg) = read_workspace_package(workspace_root, &dir) {
                trace!(
                    name = %pkg.name,
                    root = %pkg.root.display(),
                    entry = %pkg.main_entry.display(),
                    "discovered workspace package"
                );
                packages.push(pkg);
            }
        }
    }

    packages
}

/// Walk up from `start_dir` towards the filesystem root looking for a
/// `package.json` that has a `workspaces` field, stopping after at most
/// `max_depth` parent steps.
///
/// Returns the directory containing the workspace root manifest, or `None`
/// if no workspace root was found within the depth limit.
///
/// # Errors (silent)
///
/// IO failures (unreadable directories, missing files, permission errors) and
/// JSON parse failures are treated as "not a workspace root" and the walk
/// continues upward.
#[must_use]
pub fn find_workspace_root(start_dir: &Path, max_depth: usize) -> Option<PathBuf> {
    let mut current = start_dir;
    for _ in 0..=max_depth {
        let candidate = current.join("package.json");
        if let Some(candidate) = canonicalize_existing_file_under(&candidate, current)
            && let Ok(raw) = fs::read_to_string(&candidate)
            && let Ok(manifest) = serde_json::from_str::<serde_json::Value>(&raw)
            && manifest.get("workspaces").is_some()
        {
            trace!(
                dir = %current.display(),
                "found workspace root"
            );
            return Some(current.to_path_buf());
        }
        match current.parent() {
            Some(parent) => current = parent,
            None => break,
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Expand a single workspace pattern into a list of existing directories.
///
/// Supports patterns of the form:
/// - `"packages/*"` — single-level glob: list all direct children of `packages/`
/// - `"packages/foo"` — explicit path: the single directory if it exists
/// - `"apps/*/src"` — multi-segment glob: recursive walk matching the pattern
///
/// Does not support `**` double-star globs or character-class patterns (`[abc]`).
fn expand_workspace_pattern(workspace_root: &Path, pattern: &str) -> Vec<PathBuf> {
    // Normalise the pattern to forward slashes and split into segments.
    let segments: Vec<&str> = pattern
        .trim_start_matches("./")
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();

    if segments.is_empty() {
        return Vec::new();
    }

    // If there are no wildcard segments, treat the pattern as an explicit path.
    if !segments.iter().any(|s| *s == "*" || s.contains('*')) {
        let dir = workspace_root.join(segments.iter().collect::<PathBuf>());
        return canonicalize_existing_dir_under(&dir, workspace_root)
            .map(|dir| vec![dir])
            .unwrap_or_default();
    }

    // Walk the segment tree, expanding `*` by listing directory entries.
    expand_segments(workspace_root, &segments)
}

/// Recursively expand a slice of path segments, where `"*"` matches any
/// direct child directory.
fn expand_segments(base: &Path, segments: &[&str]) -> Vec<PathBuf> {
    let Some((head, tail)) = segments.split_first() else {
        // We've consumed all segments — base itself is the match.
        return if canonicalize_existing_dir_under(base, base).is_some() {
            vec![base.to_path_buf()]
        } else {
            Vec::new()
        };
    };

    if *head == "*" || head.contains('*') {
        // Expand by listing direct children; apply glob-like prefix/suffix
        // matching when the segment has content around the `*`.
        let Ok(entries) = fs::read_dir(base) else {
            return Vec::new();
        };
        let (prefix, suffix) = split_glob_segment(head);
        let mut results = Vec::new();
        for entry in entries.flatten() {
            let file_name = entry.file_name();
            let name = file_name.to_string_lossy();
            if canonicalize_existing_dir_under(&entry.path(), base).is_none() {
                continue;
            }
            if glob_segment_matches(&name, prefix, suffix) {
                let mut nested = expand_segments(&entry.path(), tail);
                results.append(&mut nested);
            }
        }
        results
    } else {
        expand_segments(&base.join(head), tail)
    }
}

/// Split a glob segment like `"@scope-*"` into `("@scope-", "")`.
/// For a bare `"*"` returns `("", "")`.
fn split_glob_segment(segment: &str) -> (&str, &str) {
    match segment.find('*') {
        Some(pos) => (&segment[..pos], &segment[pos + 1..]),
        None => (segment, ""),
    }
}

/// Check whether `name` matches a simple glob pattern described by a
/// `(prefix, suffix)` pair. Both prefix and suffix must match, and
/// their combined lengths must not exceed `name.len()`.
fn glob_segment_matches(name: &str, prefix: &str, suffix: &str) -> bool {
    if prefix.len() + suffix.len() > name.len() {
        return false;
    }
    name.starts_with(prefix) && name.ends_with(suffix)
}

/// Read and parse the `package.json` inside `package_dir`, returning a
/// `WorkspacePackage` on success or `None` on any failure.
fn read_workspace_package(workspace_root: &Path, package_dir: &Path) -> Option<WorkspacePackage> {
    let package_dir = canonicalize_existing_dir_under(package_dir, workspace_root)?;
    let manifest_path = package_dir.join("package.json");
    let manifest_path = canonicalize_existing_file_under(&manifest_path, &package_dir)?;
    let raw = fs::read_to_string(&manifest_path).ok()?;
    let manifest = serde_json::from_str::<serde_json::Value>(&raw).ok()?;

    let name = manifest.get("name")?.as_str()?.to_owned();
    if name.is_empty() {
        return None;
    }

    let main_entry = resolve_entry_point(workspace_root, &manifest, &package_dir)?;

    Some(WorkspacePackage {
        name,
        root: package_dir,
        main_entry,
    })
}

/// Resolve the entry-point file for a package, consulting (in order):
/// `exports`, `main`, and then index file fallbacks.
fn resolve_entry_point(
    workspace_root: &Path,
    manifest: &serde_json::Value,
    package_dir: &Path,
) -> Option<PathBuf> {
    // 1. `exports` field
    if let Some(exports) = manifest.get("exports")
        && let Some(path) = resolve_exports_field(workspace_root, exports, package_dir)
    {
        return Some(path);
    }

    // 2. `main` field
    if let Some(main) = manifest.get("main").and_then(|v| v.as_str())
        && let Some(candidate) = safe_package_file(workspace_root, package_dir, main)
    {
        return Some(candidate);
    }

    // 3. Index file fallback
    for index in &["index.ts", "index.tsx", "index.js", "index.mjs"] {
        if let Some(candidate) = safe_package_file(workspace_root, package_dir, index) {
            return Some(candidate);
        }
    }

    // 4. Common src/ sub-paths as a last resort
    for index in &["src/index.ts", "src/index.tsx", "src/index.js"] {
        if let Some(candidate) = safe_package_file(workspace_root, package_dir, index) {
            return Some(candidate);
        }
    }

    None
}

/// Resolve the entry-point from a package `exports` value.
///
/// Handles:
/// - `exports: "./path"` (string shorthand)
/// - `exports: { ".": "./path" }` (top-level map with `"."`)
/// - `exports: { ".": { "import": "./path", "default": "./path" } }` (conditions map)
fn resolve_exports_field(
    workspace_root: &Path,
    exports: &serde_json::Value,
    package_dir: &Path,
) -> Option<PathBuf> {
    // exports is a plain string
    if let Some(path_str) = exports.as_str()
        && let Some(candidate) = safe_package_file(workspace_root, package_dir, path_str)
    {
        return Some(candidate);
    }

    // exports is an object
    if let Some(obj) = exports.as_object() {
        // Check the "." key first
        if let Some(dot) = obj.get(".")
            && let Some(path) = resolve_exports_condition(workspace_root, dot, package_dir)
        {
            return Some(path);
        }
        // If no "." key, check whether all keys look like conditions (no "./" prefix).
        // This handles `exports: { "import": "...", "default": "..." }` at top level.
        if !obj.keys().any(|k| k.starts_with('.'))
            && let Some(path) = resolve_exports_condition(
                workspace_root,
                &serde_json::Value::Object(obj.clone()),
                package_dir,
            )
        {
            return Some(path);
        }
    }

    None
}

/// Resolve a single exports condition value, which may be:
/// - A string path
/// - An object with `"import"`, `"require"`, `"default"`, or `"types"` keys
fn resolve_exports_condition(
    workspace_root: &Path,
    value: &serde_json::Value,
    package_dir: &Path,
) -> Option<PathBuf> {
    if let Some(path_str) = value.as_str()
        && let Some(candidate) = safe_package_file(workspace_root, package_dir, path_str)
    {
        return Some(candidate);
    }

    if let Some(obj) = value.as_object() {
        // Priority: import > require > default > types
        for key in &["import", "require", "default", "types"] {
            if let Some(v) = obj.get(*key)
                && let Some(path) = resolve_exports_condition(workspace_root, v, package_dir)
            {
                return Some(path);
            }
        }
    }

    None
}

fn safe_package_file(
    workspace_root: &Path,
    package_dir: &Path,
    member_path: &str,
) -> Option<PathBuf> {
    let relative = normalize_relative_path(Path::new(member_path.trim_start_matches("./")))?;
    let candidate = package_dir.join(relative);
    let canonical_workspace = fs::canonicalize(workspace_root).ok()?;
    let canonical = canonicalize_existing_file_under(&candidate, package_dir)?;
    canonical
        .starts_with(canonical_workspace)
        .then_some(canonical)
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use pretty_assertions::assert_eq;

    use super::{discover_workspace_packages, find_workspace_root};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn canonical(path: impl AsRef<Path>) -> PathBuf {
        fs::canonicalize(path).expect("expected path should canonicalize")
    }

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(name: &str) -> Self {
            let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-ws-manifest-{name}-{}-{counter}",
                process::id()
            ));
            fs::create_dir_all(&path).expect("temp dir should create");
            Self { path }
        }

        fn write(&self, relative: &str, contents: &str) {
            let full = self.path.join(relative);
            if let Some(parent) = full.parent() {
                fs::create_dir_all(parent).expect("parent dir should create");
            }
            fs::write(full, contents).expect("fixture file should write");
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    // -------------------------------------------------------------------
    // discover_workspace_packages
    // -------------------------------------------------------------------

    #[test]
    fn returns_empty_when_no_root_package_json() {
        let dir = TempDir::new("no-manifest");
        let packages = discover_workspace_packages(&dir.path);
        assert!(packages.is_empty());
    }

    #[test]
    fn returns_empty_when_no_workspaces_field() {
        let dir = TempDir::new("no-workspaces");
        dir.write("package.json", r#"{ "name": "root" }"#);
        let packages = discover_workspace_packages(&dir.path);
        assert!(packages.is_empty());
    }

    #[test]
    fn returns_empty_for_malformed_root_manifest() {
        let dir = TempDir::new("malformed");
        dir.write("package.json", "{ not valid json");
        let packages = discover_workspace_packages(&dir.path);
        assert!(packages.is_empty());
    }

    #[test]
    fn discovers_single_level_glob_pattern() {
        let dir = TempDir::new("single-level");
        dir.write("package.json", r#"{ "workspaces": ["packages/*"] }"#);
        dir.write(
            "packages/contracts/package.json",
            r#"{ "name": "@workspace/contracts", "main": "src/index.ts" }"#,
        );
        dir.write(
            "packages/contracts/src/index.ts",
            "export interface IFoo { id: string; }",
        );

        let packages = discover_workspace_packages(&dir.path);
        assert_eq!(packages.len(), 1);
        assert_eq!(packages[0].name, "@workspace/contracts");
        assert_eq!(
            packages[0].main_entry,
            canonical(dir.path.join("packages/contracts/src/index.ts"))
        );
    }

    #[test]
    fn discovers_explicit_path_without_glob() {
        let dir = TempDir::new("explicit-path");
        dir.write("package.json", r#"{ "workspaces": ["packages/mylib"] }"#);
        dir.write(
            "packages/mylib/package.json",
            r#"{ "name": "@workspace/mylib", "main": "index.ts" }"#,
        );
        dir.write("packages/mylib/index.ts", "export const x = 1;");

        let packages = discover_workspace_packages(&dir.path);
        assert_eq!(packages.len(), 1);
        assert_eq!(packages[0].name, "@workspace/mylib");
    }

    #[test]
    fn skips_package_dir_without_package_json() {
        let dir = TempDir::new("no-pkg-json");
        dir.write("package.json", r#"{ "workspaces": ["packages/*"] }"#);
        // Create directory but no package.json inside it
        fs::create_dir_all(dir.path.join("packages/orphan")).expect("dir should create");

        let packages = discover_workspace_packages(&dir.path);
        assert!(packages.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn skips_symlinked_workspace_package_dir() {
        use std::os::unix::fs::symlink;

        let dir = TempDir::new("symlinked-package-dir");
        let external = TempDir::new("external-package-dir");
        dir.write("package.json", r#"{ "workspaces": ["packages/*"] }"#);
        external.write(
            "contracts/package.json",
            r#"{ "name": "@workspace/contracts", "main": "index.ts" }"#,
        );
        external.write("contracts/index.ts", "export const value = 1;");
        fs::create_dir_all(dir.path.join("packages")).expect("packages dir should create");
        symlink(
            external.path.join("contracts"),
            dir.path.join("packages/contracts"),
        )
        .expect("symlink should create");

        let packages = discover_workspace_packages(&dir.path);

        assert!(packages.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn ignores_symlinked_root_manifest() {
        use std::os::unix::fs::symlink;

        let dir = TempDir::new("symlinked-root-manifest");
        let external = TempDir::new("external-root-manifest");
        external.write("package.json", r#"{ "workspaces": ["packages/*"] }"#);
        symlink(
            external.path.join("package.json"),
            dir.path.join("package.json"),
        )
        .expect("symlink should create");

        assert!(discover_workspace_packages(&dir.path).is_empty());
        assert!(find_workspace_root(&dir.path, 0).is_none());
    }

    #[test]
    fn resolves_exports_dot_string() {
        let dir = TempDir::new("exports-dot-string");
        dir.write("package.json", r#"{ "workspaces": ["packages/*"] }"#);
        dir.write(
            "packages/lib/package.json",
            r#"{ "name": "@workspace/lib", "exports": { ".": "./src/main.ts" } }"#,
        );
        dir.write("packages/lib/src/main.ts", "export const v = 1;");

        let packages = discover_workspace_packages(&dir.path);
        assert_eq!(packages.len(), 1);
        assert_eq!(
            packages[0].main_entry,
            canonical(dir.path.join("packages/lib/src/main.ts"))
        );
    }

    #[test]
    fn resolves_exports_dot_conditions_object() {
        let dir = TempDir::new("exports-conditions");
        dir.write("package.json", r#"{ "workspaces": ["packages/*"] }"#);
        dir.write(
            "packages/lib/package.json",
            r#"{ "name": "@workspace/lib", "exports": { ".": { "import": "./dist/esm/index.js", "default": "./dist/cjs/index.js" } } }"#,
        );
        dir.write("packages/lib/dist/esm/index.js", "module.exports = {};");

        let packages = discover_workspace_packages(&dir.path);
        assert_eq!(packages.len(), 1);
        assert_eq!(
            packages[0].main_entry,
            canonical(dir.path.join("packages/lib/dist/esm/index.js"))
        );
    }

    #[test]
    fn falls_back_to_index_ts_when_no_main_or_exports() {
        let dir = TempDir::new("index-fallback");
        dir.write("package.json", r#"{ "workspaces": ["packages/*"] }"#);
        dir.write(
            "packages/lib/package.json",
            r#"{ "name": "@workspace/lib" }"#,
        );
        dir.write("packages/lib/index.ts", "export const x = 1;");

        let packages = discover_workspace_packages(&dir.path);
        assert_eq!(packages.len(), 1);
        assert_eq!(
            packages[0].main_entry,
            canonical(dir.path.join("packages/lib/index.ts"))
        );
    }

    #[test]
    fn skips_package_without_resolvable_entry_point() {
        let dir = TempDir::new("no-entry");
        dir.write("package.json", r#"{ "workspaces": ["packages/*"] }"#);
        dir.write(
            "packages/lib/package.json",
            r#"{ "name": "@workspace/lib" }"#,
        );
        // No index file created — should be skipped

        let packages = discover_workspace_packages(&dir.path);
        assert!(packages.is_empty());
    }

    #[test]
    fn discovers_multiple_packages_under_single_glob() {
        let dir = TempDir::new("multi-pkg");
        dir.write("package.json", r#"{ "workspaces": ["packages/*"] }"#);

        for (pkg, name) in &[
            ("alpha", "@workspace/alpha"),
            ("beta", "@workspace/beta"),
            ("gamma", "@workspace/gamma"),
        ] {
            dir.write(
                &format!("packages/{pkg}/package.json"),
                &format!(r#"{{ "name": "{name}", "main": "index.ts" }}"#),
            );
            dir.write(&format!("packages/{pkg}/index.ts"), "export const x = 1;");
        }

        let packages = discover_workspace_packages(&dir.path);
        assert_eq!(packages.len(), 3);
        let names: Vec<&str> = packages.iter().map(|p| p.name.as_str()).collect();
        assert!(names.contains(&"@workspace/alpha"));
        assert!(names.contains(&"@workspace/beta"));
        assert!(names.contains(&"@workspace/gamma"));
    }

    #[test]
    fn handles_yarn_workspaces_object_format() {
        let dir = TempDir::new("yarn-format");
        dir.write(
            "package.json",
            r#"{ "workspaces": { "packages": ["packages/*"] } }"#,
        );
        dir.write(
            "packages/shared/package.json",
            r#"{ "name": "@workspace/shared", "main": "index.ts" }"#,
        );
        dir.write("packages/shared/index.ts", "export const s = 1;");

        let packages = discover_workspace_packages(&dir.path);
        assert_eq!(packages.len(), 1);
        assert_eq!(packages[0].name, "@workspace/shared");
    }

    // -------------------------------------------------------------------
    // find_workspace_root
    // -------------------------------------------------------------------

    #[test]
    fn finds_workspace_root_in_current_dir() {
        let dir = TempDir::new("root-self");
        dir.write("package.json", r#"{ "workspaces": ["packages/*"] }"#);
        let found = find_workspace_root(&dir.path, 3);
        assert_eq!(found, Some(dir.path.clone()));
    }

    #[test]
    fn finds_workspace_root_in_parent() {
        let dir = TempDir::new("root-parent");
        dir.write("package.json", r#"{ "workspaces": ["packages/*"] }"#);
        let child = dir.path.join("packages/service");
        fs::create_dir_all(&child).expect("child dir should create");

        let found = find_workspace_root(&child, 3);
        assert_eq!(found, Some(dir.path.clone()));
    }

    #[test]
    fn returns_none_when_no_workspace_root_within_depth() {
        let dir = TempDir::new("no-root");
        // package.json without workspaces field
        dir.write("package.json", r#"{ "name": "standalone" }"#);
        let child = dir.path.join("src");
        fs::create_dir_all(&child).expect("child dir should create");

        let found = find_workspace_root(&child, 2);
        assert!(found.is_none());
    }

    #[test]
    fn stops_at_depth_limit() {
        let dir = TempDir::new("depth-limit");
        dir.write("package.json", r#"{ "workspaces": ["packages/*"] }"#);
        // Create a child path 4 levels deep — beyond the max_depth=3 limit.
        let deep = dir.path.join("a/b/c/d");
        fs::create_dir_all(&deep).expect("deep dir should create");

        // With max_depth=3 we check: d → c → b → a (4 checks including start).
        // The root is at depth 4 from `deep`, so it should NOT be found with max_depth=3.
        let found = find_workspace_root(&deep, 3);
        assert!(found.is_none());
    }
}
