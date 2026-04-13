//! Cross-package frontend hook boundary detection.
//!
//! A "frontend hook" is a TypeScript/JavaScript function export whose name
//! starts with `use` (camelCase), declared in a `.ts`, `.tsx`, or `.vue` file
//! that lives under a recognised hook directory (`src/hooks/`,
//! `src/composables/`, `src/lib/hooks/`, or similar).
//!
//! When a symbol in **any** file imports such a hook via a **cross-package**
//! specifier (a bare `@scope/…` path or a `@workspace/…` monorepo alias that
//! resolves outside the current repo), this module emits a
//! [`EdgeKind::ConsumesHookFrom`] edge from the importing symbol to the hook
//! export's virtual node.
//!
//! Same-package imports are intentionally excluded — those are already covered
//! by the regular `Imports` / `Calls` edge infrastructure.
//!
//! ## Detection heuristic
//!
//! For each import binding in the parsed file:
//! 1. The import source must be a cross-package path (no relative `./` or `../`
//!    prefix, and not resolvable to a path inside the current repo).
//! 2. The imported symbol name must start with `use` and have at least one
//!    additional character after `use` (i.e. `useX…`).
//! 3. The resolved path (when available) must end with a hook-directory segment
//!    or a `.ts`/`.tsx`/`.vue` extension; when the resolved path is absent, the
//!    package path itself is accepted — the name heuristic is the primary gate.
//!
//! On a match, a virtual [`NodeKind::SharedSymbol`] node is created for the
//! hook export and a [`EdgeKind::ConsumesHookFrom`] edge is emitted from the
//! file's node to the hook virtual node.  The virtual node's QN follows the
//! `__hook__<package>::<symbol>` pattern so it is stable across files in the
//! same workspace.

use gather_step_core::{EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, ref_node_id};

use crate::{resolve::ImportBinding, tree_sitter::ParsedFile};

/// Output of the frontend-hook boundary extractor pass.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FrontendHooksAugmentation {
    pub nodes: Vec<NodeData>,
    pub edges: Vec<EdgeData>,
}

/// Analyse `parsed` and emit `ConsumesHookFrom` edges for any cross-package
/// frontend hook imports found in the file.
///
/// Returns a [`FrontendHooksAugmentation`] that the orchestrator merges into
/// the parsed file's output.  The function is side-effect-free — it only reads
/// `parsed`.
#[must_use]
pub fn augment(parsed: &ParsedFile) -> FrontendHooksAugmentation {
    let mut aug = FrontendHooksAugmentation::default();
    add_hook_consumer_edges(parsed, &mut aug);
    aug
}

// ---------------------------------------------------------------------------
// Core detection
// ---------------------------------------------------------------------------

fn add_hook_consumer_edges(parsed: &ParsedFile, aug: &mut FrontendHooksAugmentation) {
    let mut seen_targets: rustc_hash::FxHashSet<gather_step_core::NodeId> =
        rustc_hash::FxHashSet::default();

    for binding in &parsed.import_bindings {
        // Namespace imports and type-only imports carry no concrete hook usage.
        if binding.is_namespace || binding.is_type_only {
            continue;
        }

        // Require a cross-package (non-relative) import source.
        if !is_cross_package_source(&binding.source) {
            continue;
        }
        if resolved_path_is_same_repo(parsed, binding) {
            continue;
        }

        // Derive the symbol name that was actually imported.
        let symbol_name = imported_symbol_name(binding);
        if symbol_name.is_empty() {
            continue;
        }

        // The name must look like a hook (`useXxx`).
        if !is_hook_name(symbol_name) {
            continue;
        }

        // Optionally gate on the resolved path looking like a hook file, but
        // only when a resolved path is available.  If none is available, the
        // name heuristic is sufficient — cross-package hook names rarely clash
        // with non-hook patterns.
        if binding
            .resolved_path
            .as_ref()
            .is_some_and(|resolved| !resolved_path_is_hook_like(resolved))
        {
            continue;
        }

        // Derive a stable virtual node for this cross-package hook export.
        let package = package_root_from_source(&binding.source);
        let qualified_name = format!("__hook__{package}::{symbol_name}");

        let hook_node = NodeData {
            id: ref_node_id(NodeKind::SharedSymbol, &qualified_name),
            kind: NodeKind::SharedSymbol,
            repo: parsed.file_node.repo.clone(),
            file_path: parsed.file_node.file_path.clone(),
            name: symbol_name.to_owned(),
            qualified_name: Some(qualified_name.clone()),
            external_id: Some(qualified_name),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        };

        let node_id = hook_node.id;
        if seen_targets.insert(node_id) {
            aug.nodes.push(hook_node);
        }

        aug.edges.push(EdgeData {
            source: parsed.file_node.id,
            target: node_id,
            kind: EdgeKind::ConsumesHookFrom,
            metadata: EdgeMetadata {
                weight: None,
                confidence: Some(800),
                timestamp_unix: None,
                drift_kind: None,
                resolver: Some("frontend_hook_import".to_owned()),
            },
            owner_file: parsed.file_node.id,
            is_cross_file: false,
        });
    }
}

// ---------------------------------------------------------------------------
// Heuristic helpers
// ---------------------------------------------------------------------------

/// Returns `true` when `source` is a cross-package import specifier — that is,
/// it does NOT start with `.` (relative) and is not an absolute filesystem path.
///
/// Both bare package names (`react`, `lodash`) and scoped packages
/// (`@workspace/frontend-shared`, `@scope/ui`) are cross-package.
fn is_cross_package_source(source: &str) -> bool {
    !source.starts_with('.') && !source.starts_with('/')
}

fn resolved_path_is_same_repo(parsed: &ParsedFile, binding: &ImportBinding) -> bool {
    let Some(resolved) = binding.resolved_path.as_ref() else {
        return false;
    };
    resolved.starts_with(derive_repo_root(parsed))
}

fn derive_repo_root(parsed: &ParsedFile) -> std::path::PathBuf {
    let mut root = parsed.source_path.clone();
    for _ in parsed.file.path.components() {
        root.pop();
    }
    root
}

/// Returns `true` when `name` looks like a React/Vue hook — starts with `use`
/// and has at least one uppercase letter or further character after `use`.
///
/// Examples that match: `useAuthentication`, `useSessionData`, `useState`
/// Examples that do not match: `user`, `use`, `userInfo` (capital after `use` required for camelCase hooks)
fn is_hook_name(name: &str) -> bool {
    let Some(after_use) = name.strip_prefix("use") else {
        return false;
    };
    // Must have at least one character after `use` and start with uppercase
    // (camelCase convention) to avoid matching plain functions like `user`.
    after_use.chars().next().is_some_and(char::is_uppercase)
}

/// Returns `true` when the resolved file path looks like it belongs to a hook
/// module — either lives under a `hooks/` / `composables/` directory, has a
/// hook-shaped filename (`useXxx.ts`, `use_xxx.ts`), or is the package's
/// barrel `index.{ts,tsx,js,jsx,vue}` (where shared packages re-export
/// hooks from a flat index).
///
/// Returning `true` for any TS/JS file regardless of path was emitting a
/// `ConsumesHookFrom` edge for every cross-package import that happened to
/// have a `useXxx`-shaped local name, including non-hook utilities and
/// adjacent re-exports — inflating `virtual_other_cross_repo_edges` on real
/// workspaces. The gate now requires structural evidence that the resolved
/// file is hook-related.
fn resolved_path_is_hook_like(path: &std::path::Path) -> bool {
    // Accepted extensions for hook files.
    let ext_ok = path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| matches!(ext, "ts" | "tsx" | "vue" | "js" | "jsx"));
    if !ext_ok {
        return false;
    }

    let path_str = path.to_string_lossy();

    // 1) Hook-directory segment (forward and back slashes for cross-platform).
    let is_hook_dir = path_str.contains("/hooks/")
        || path_str.contains("/composables/")
        || path_str.contains("/hook/")
        || path_str.contains("\\hooks\\")
        || path_str.contains("\\composables\\")
        || path_str.contains("\\hook\\");
    if is_hook_dir {
        return true;
    }

    // 2) Hook-shaped filename (`use_session_data.ts`, `useSessionData.tsx`).
    //    The leading `use` plus an uppercase or `_` character matches both
    //    snake_case and camelCase conventions used in TS/JS codebases.
    let file_stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or_default();
    let is_hook_filename = file_stem.starts_with("use_")
        || file_stem
            .strip_prefix("use")
            .is_some_and(|after| after.chars().next().is_some_and(char::is_uppercase));
    if is_hook_filename {
        return true;
    }

    // 3) Package barrel — `index.ts` / `index.js` / etc — where shared
    //    packages flatten hook exports through a single entry point. Without
    //    this, a `from '@workspace/frontend-shared'` import resolved to the
    //    package's index file would be rejected even when the hook is real.
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    matches!(
        file_name,
        "index.ts" | "index.tsx" | "index.js" | "index.jsx" | "index.vue"
    )
}

/// Derive the package-root portion of an import source.
///
/// For scoped packages (`@scope/package/sub/path`) returns `@scope/package`.
/// For bare packages (`package/sub`) returns `package`.
fn package_root_from_source(source: &str) -> &str {
    if let Some(rest) = source.strip_prefix('@') {
        // Scoped: two segments form the package root.
        if let Some(slash) = rest.find('/') {
            let after_scope = &rest[slash + 1..];
            if let Some(sub_slash) = after_scope.find('/') {
                let end = 1 + slash + 1 + sub_slash; // '@' + rest[..slash+1+sub_slash]
                return &source[..end];
            }
        }
        return source;
    }
    // Bare package: first slash is the boundary.
    source.find('/').map_or(source, |slash| &source[..slash])
}

/// The symbol name that was actually imported.  For named imports this is the
/// `imported_name` (the name in the exporting module); for default imports it
/// falls back to the local alias.
fn imported_symbol_name(binding: &ImportBinding) -> &str {
    if binding.is_default {
        return binding.local_name.as_str();
    }
    binding
        .imported_name
        .as_deref()
        .filter(|name| !name.is_empty())
        .unwrap_or(binding.local_name.as_str())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::{EdgeKind, NodeKind};
    use pretty_assertions::assert_eq;

    use crate::{
        Language,
        frameworks::Framework,
        tree_sitter::{parse_file_with_context, parse_file_with_frameworks},
        tsconfig::PathAliases,
    };

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> Self {
            let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-parser-frontend-hooks-{name}-{}-{counter}",
                process::id()
            ));
            fs::create_dir_all(&path).expect("test directory should be created");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }

        fn write(&self, relative: &str, contents: &str) {
            let full = self.path.join(relative);
            if let Some(parent) = full.parent() {
                fs::create_dir_all(parent).expect("parent dir should create");
            }
            fs::write(full, contents).expect("fixture should write");
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    /// Parse a single file with the `FrontendHooks` framework pack active.
    fn parse_with_hook_pack(
        repo: &str,
        repo_root: &Path,
        relative: &str,
    ) -> crate::tree_sitter::ParsedFile {
        let file = crate::FileEntry {
            path: relative.into(),
            language: Language::TypeScript,
            size_bytes: 0,
            content_hash: [0; 32],
            source_bytes: None,
        };
        parse_file_with_frameworks(repo, repo_root, &file, &[Framework::FrontendHooks])
            .expect("fixture should parse")
    }

    fn parse_with_hook_pack_and_aliases(
        repo: &str,
        repo_root: &Path,
        relative: &str,
    ) -> crate::tree_sitter::ParsedFile {
        let file = crate::FileEntry {
            path: relative.into(),
            language: Language::TypeScript,
            size_bytes: 0,
            content_hash: [0; 32],
            source_bytes: None,
        };
        let aliases = PathAliases::from_repo_root(repo_root);
        parse_file_with_context(
            repo,
            repo_root,
            &file,
            &[Framework::FrontendHooks],
            &aliases,
        )
        .expect("fixture should parse")
    }

    // -----------------------------------------------------------------------
    // Test 1: cross-repo hook import emits ConsumesHookFrom
    // -----------------------------------------------------------------------

    /// A file in `internal_full` that imports `useSessionData` from the
    /// neutral shared package `@workspace/frontend-shared` should produce a
    /// `ConsumesHookFrom` edge to a virtual `SharedSymbol` hook node.
    #[test]
    fn cross_package_hook_import_emits_consumes_hook_from_edge() {
        let dir = TestDir::new("cross-package-hook");

        // Simulated consumer file in a different repo.
        dir.write(
            "src/hooks/use_consumer.ts",
            r"
import { useSessionData } from '@workspace/frontend-shared';

export function useConsumer() {
  const session = useSessionData();
  return session;
}
",
        );

        let parsed = parse_with_hook_pack("internal_full", dir.path(), "src/hooks/use_consumer.ts");

        // A virtual SharedSymbol node for the hook export must be present.
        let hook_node = parsed.nodes.iter().find(|node| {
            node.kind == NodeKind::SharedSymbol
                && node
                    .qualified_name
                    .as_deref()
                    .is_some_and(|qn| qn.contains("useSessionData"))
        });
        assert!(
            hook_node.is_some(),
            "expected a virtual SharedSymbol node for useSessionData; nodes: {:#?}",
            parsed.nodes
        );
        let hook_node = hook_node.unwrap();

        // A ConsumesHookFrom edge must target that node.
        let edge = parsed
            .edges
            .iter()
            .find(|e| e.kind == EdgeKind::ConsumesHookFrom && e.target == hook_node.id);
        assert!(
            edge.is_some(),
            "expected a ConsumesHookFrom edge pointing at the hook virtual node; edges: {:#?}",
            parsed.edges
        );
    }

    // -----------------------------------------------------------------------
    // Test 2: same-package import does NOT emit ConsumesHookFrom
    // -----------------------------------------------------------------------

    /// A relative import (`./use_session_data`) within the same package must
    /// not produce a `ConsumesHookFrom` edge — that relationship is already
    /// captured by the regular `Imports` / `Calls` infrastructure.
    #[test]
    fn same_package_relative_import_does_not_emit_consumes_hook_from() {
        let dir = TestDir::new("same-package-hook");

        dir.write(
            "src/hooks/use_session_data.ts",
            "export function useSessionData() { return null; }\n",
        );
        dir.write(
            "src/hooks/use_consumer.ts",
            r"
import { useSessionData } from './use_session_data';

export function useConsumer() {
  return useSessionData();
}
",
        );

        let parsed =
            parse_with_hook_pack("frontend_standard", dir.path(), "src/hooks/use_consumer.ts");

        let hook_edge_count = parsed
            .edges
            .iter()
            .filter(|e| e.kind == EdgeKind::ConsumesHookFrom)
            .count();
        assert_eq!(
            hook_edge_count, 0,
            "same-package relative import must not emit ConsumesHookFrom; edges: {:#?}",
            parsed.edges
        );
    }

    #[test]
    fn same_package_alias_import_does_not_emit_consumes_hook_from() {
        let dir = TestDir::new("same-package-alias-hook");

        dir.write(
            "tsconfig.json",
            r#"{
  "compilerOptions": {
    "baseUrl": ".",
    "paths": {
      "@app/*": ["src/*"]
    }
  }
}
"#,
        );
        dir.write(
            "src/hooks/use_authentication.ts",
            "export function useAuthentication() { return null; }\n",
        );
        dir.write(
            "src/components/session.ts",
            r"
import { useAuthentication } from '@app/hooks/use_authentication';

export function SessionPanel() {
  return useAuthentication();
}
",
        );

        let parsed = parse_with_hook_pack_and_aliases(
            "frontend_standard",
            dir.path(),
            "src/components/session.ts",
        );

        let hook_edge_count = parsed
            .edges
            .iter()
            .filter(|e| e.kind == EdgeKind::ConsumesHookFrom)
            .count();
        let hook_node_count = parsed
            .nodes
            .iter()
            .filter(|node| {
                node.kind == NodeKind::SharedSymbol
                    && node
                        .qualified_name
                        .as_deref()
                        .is_some_and(|qn| qn.contains("__hook__"))
            })
            .count();

        assert_eq!(
            hook_edge_count, 0,
            "same-package alias import must not emit ConsumesHookFrom; edges: {:#?}",
            parsed.edges
        );
        assert_eq!(
            hook_node_count, 0,
            "same-package alias import must not mint hook virtual nodes; nodes: {:#?}",
            parsed.nodes
        );
    }

    // -----------------------------------------------------------------------
    // Test 3: non-hook export does NOT emit ConsumesHookFrom
    // -----------------------------------------------------------------------

    /// Importing a plain utility function (`getSessionData`, not starting with
    /// `use`) from a cross-package path must not produce a `ConsumesHookFrom`
    /// edge.
    #[test]
    fn non_hook_cross_package_import_does_not_emit_consumes_hook_from() {
        let dir = TestDir::new("non-hook-import");

        dir.write(
            "src/lib/consumer.ts",
            r"
import { getSessionData } from '@workspace/frontend-shared';

export function loadSession() {
  return getSessionData();
}
",
        );

        let parsed = parse_with_hook_pack("internal_full", dir.path(), "src/lib/consumer.ts");

        let hook_edge_count = parsed
            .edges
            .iter()
            .filter(|e| e.kind == EdgeKind::ConsumesHookFrom)
            .count();
        assert_eq!(
            hook_edge_count, 0,
            "non-hook export import must not emit ConsumesHookFrom; edges: {:#?}",
            parsed.edges
        );
    }

    // -----------------------------------------------------------------------
    // Heuristic unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn is_hook_name_accepts_camel_case_use_prefix() {
        assert!(super::is_hook_name("useAuthentication"));
        assert!(super::is_hook_name("useSessionData"));
        assert!(super::is_hook_name("useState"));
        assert!(super::is_hook_name("useQuery"));
    }

    #[test]
    fn is_hook_name_rejects_non_hook_names() {
        assert!(!super::is_hook_name("use")); // bare use
        assert!(!super::is_hook_name("user")); // lowercase after use
        assert!(!super::is_hook_name("getSessionData")); // wrong prefix
        assert!(!super::is_hook_name("loadAuth")); // no use prefix
    }

    #[test]
    fn is_cross_package_source_accepts_scoped_and_bare_packages() {
        assert!(super::is_cross_package_source("@workspace/frontend-shared"));
        assert!(super::is_cross_package_source("react"));
        assert!(super::is_cross_package_source("@tanstack/react-query"));
    }

    #[test]
    fn is_cross_package_source_rejects_relative_paths() {
        assert!(!super::is_cross_package_source("./hooks/use_session"));
        assert!(!super::is_cross_package_source("../shared/hooks"));
        assert!(!super::is_cross_package_source("/absolute/path"));
    }

    #[test]
    fn package_root_from_source_extracts_scope_and_name() {
        assert_eq!(
            super::package_root_from_source("@workspace/frontend-shared"),
            "@workspace/frontend-shared"
        );
        assert_eq!(
            super::package_root_from_source("@workspace/frontend-shared/hooks"),
            "@workspace/frontend-shared"
        );
        assert_eq!(super::package_root_from_source("react"), "react");
        assert_eq!(super::package_root_from_source("react/hooks"), "react");
    }

    /// Registry integration: `PackId::FrontendHooks` must appear in the
    /// builtin registry's output.
    #[test]
    fn builtin_registry_includes_frontend_hooks_pack() {
        use crate::frameworks::registry::{PackId, PackRegistry};
        let registry = PackRegistry::builtin();
        // FrontendHooks is always-on (no detect predicate), so it must be
        // present for any repo root including a temporary empty directory.
        let active = registry.detect(env::temp_dir().as_path());
        assert!(
            active.contains(&PackId::FrontendHooks),
            "FrontendHooks must always be active"
        );
    }
}
