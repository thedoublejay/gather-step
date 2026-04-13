//! Parse `tsconfig.json` path aliases into a flat prefix → replacement map
//! that the import resolver can consult.
//!
//! TypeScript's `compilerOptions.paths` is a mapping of glob-like source
//! patterns to arrays of replacement patterns:
//!
//! ```json
//! {
//!   "compilerOptions": {
//!     "baseUrl": "./",
//!     "paths": {
//!       "@/*": ["src/*"],
//!       "@lib/*": ["src/lib/*"]
//!     }
//!   }
//! }
//! ```
//!
//! The resolver only needs the "first match wins" prefix mapping:
//! strip the trailing `/*` from the key, strip the trailing `/*` from the
//! first replacement, and prefix the replacement with `baseUrl` (defaulting
//! to repo root). The resulting `PathAliases` lets the resolver rewrite
//! `@lib/foo/bar` → `src/lib/foo/bar` without re-parsing JSON per import.
//!
//! We intentionally ignore patterns that don't have a trailing `/*` (pure
//! exact matches are rare in practice and not worth the added complexity)
//! and alias targets that lead outside the repo (`../external/*`), which
//! would break the resolver's "all paths are repo-relative" invariant.

use std::{
    fs,
    path::{Component, Path, PathBuf},
};

use rustc_hash::FxHashMap;

use crate::workspace_manifest::WorkspacePackage;

/// Repo-level path-alias map used by the import resolver.
///
/// Keys are the alias prefix stripped of its trailing `/*` (e.g. `@/` for
/// `@/*`); values are the replacement prefix stripped of its trailing `/*`
/// and already joined with `baseUrl` (e.g. `src/` for `src/*` under the
/// default `baseUrl`).
///
/// Lookup is "longest-prefix-wins" — for an import source like `@app/foo`,
/// the resolver iterates aliases and applies the one whose key is the
/// longest prefix of the source.
#[derive(Clone, Debug, Default)]
pub struct PathAliases {
    exact_entries: FxHashMap<String, String>,
    prefix_entries: FxHashMap<String, String>,
}

impl PathAliases {
    /// Construct empty aliases — a repo without `tsconfig.json` uses this.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Load aliases from `<repo_root>/tsconfig.json`. Returns empty aliases
    /// if the file is absent or malformed (consistent with
    /// `frameworks::detect::is_nestjs` — detection is best-effort, not a
    /// hard failure).
    #[must_use]
    pub fn from_repo_root(repo_root: &Path) -> Self {
        let tsconfig_path = repo_root.join("tsconfig.json");
        let Ok(raw) = fs::read_to_string(&tsconfig_path) else {
            return Self::empty();
        };
        let raw = strip_jsonc_comments(&raw);
        let Ok(document) = serde_json::from_str::<serde_json::Value>(&raw) else {
            return Self::empty();
        };

        let options = document.get("compilerOptions");
        let base_url = options
            .and_then(|opts| opts.get("baseUrl"))
            .and_then(|value| value.as_str())
            .unwrap_or(".");
        let Some(paths) = options
            .and_then(|opts| opts.get("paths"))
            .and_then(|value| value.as_object())
        else {
            return Self::empty();
        };

        let mut exact_entries = FxHashMap::default();
        let mut prefix_entries = FxHashMap::default();
        for (pattern, replacements) in paths {
            let Some(replacement) = replacements.as_array().and_then(|list| list.first()) else {
                continue;
            };
            let Some(replacement) = replacement.as_str() else {
                continue;
            };
            let base = base_url.trim_start_matches("./").trim_start_matches('.');
            let apply_base = |path: &str| -> Option<PathBuf> {
                let full_replacement = if base.is_empty() {
                    path.to_owned()
                } else {
                    format!("{base}/{path}")
                };
                normalize_repo_relative_path(Path::new(&full_replacement))
            };

            if let Some(key) = pattern.strip_suffix("/*") {
                let Some(replacement) = replacement.strip_suffix("/*") else {
                    continue;
                };
                let Some(full_replacement) = apply_base(replacement) else {
                    continue;
                };
                prefix_entries.insert(
                    format!("{key}/"),
                    format!("{}/", full_replacement.display()),
                );
            } else {
                let Some(full_replacement) = apply_base(replacement) else {
                    continue;
                };
                exact_entries.insert(pattern.clone(), full_replacement.display().to_string());
            }
        }

        Self {
            exact_entries,
            prefix_entries,
        }
    }

    /// Rewrite a module specifier if any alias matches as a prefix.
    ///
    /// Returns `None` if no alias applies — the caller should then fall back
    /// to its default resolution (relative paths, `node_modules`, etc.).
    /// Returns `Some(rewritten)` with the repo-relative path otherwise.
    ///
    /// Uses longest-prefix matching: if two aliases both match (`@/` and
    /// `@app/`), the one with the longer key wins.
    #[must_use]
    pub fn rewrite(&self, module_source: &str) -> Option<String> {
        if let Some(exact) = self.exact_entries.get(module_source) {
            return Some(exact.clone());
        }
        let mut best: Option<(&String, &String)> = None;
        for (prefix, replacement) in &self.prefix_entries {
            if module_source.starts_with(prefix.as_str())
                && best
                    .as_ref()
                    .is_none_or(|(existing, _)| prefix.len() > existing.len())
            {
                best = Some((prefix, replacement));
            }
        }
        best.map(|(prefix, replacement)| {
            let tail = &module_source[prefix.len()..];
            format!("{replacement}{tail}")
        })
    }

    /// Returns `true` when no aliases were loaded — the resolver uses this to
    /// short-circuit to its default `@/*` → `src/*` fallback.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.exact_entries.is_empty() && self.prefix_entries.is_empty()
    }

    /// Insert workspace-local packages as exact alias entries.
    ///
    /// Each `WorkspacePackage` contributes one entry: the package name (e.g.
    /// `"@workspace/contracts"`) maps to the absolute path of its entry-point
    /// file. The path is stored as a `String` via `Display`, consistent with
    /// how tsconfig aliases are stored.
    ///
    /// If an alias for the same package name already exists — because a
    /// `tsconfig.json` path alias already maps it — the tsconfig entry wins
    /// and the workspace entry is silently skipped.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::{Path, PathBuf};
    /// use gather_step_parser::tsconfig::PathAliases;
    /// use gather_step_parser::workspace_manifest::WorkspacePackage;
    ///
    /// let mut aliases = PathAliases::from_repo_root(Path::new("/repo"));
    /// let packages = vec![WorkspacePackage {
    ///     name: "@workspace/contracts".to_owned(),
    ///     root: PathBuf::from("/workspace/packages/contracts"),
    ///     main_entry: PathBuf::from("/workspace/packages/contracts/src/index.ts"),
    /// }];
    /// aliases.add_workspace_packages(&packages);
    /// assert_eq!(
    ///     aliases.rewrite("@workspace/contracts").as_deref(),
    ///     Some("/workspace/packages/contracts/src/index.ts"),
    /// );
    /// ```
    pub fn add_workspace_packages(&mut self, packages: &[WorkspacePackage]) {
        for pkg in packages {
            // tsconfig wins — do not overwrite an existing exact alias.
            if self.exact_entries.contains_key(&pkg.name) {
                continue;
            }
            self.exact_entries.insert(
                pkg.name.clone(),
                pkg.main_entry.to_string_lossy().into_owned(),
            );
        }
    }
}

fn normalize_repo_relative_path(path: &Path) -> Option<PathBuf> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(part) => normalized.push(part),
            Component::ParentDir => {
                if !normalized.pop() {
                    return None;
                }
            }
            Component::RootDir | Component::Prefix(_) => return None,
        }
    }

    Some(normalized)
}

/// Strip line-style `//` comments from `tsconfig.json` so the standard JSON
/// parser can read it. `tsconfig.json` is technically JSONC but most repos
/// only use line comments; we don't attempt to handle block comments or
/// trailing commas. If these become common we can swap in a JSONC parser.
fn strip_jsonc_comments(source: &str) -> String {
    let mut output = String::with_capacity(source.len());
    for line in source.lines() {
        let mut in_string = false;
        let mut chars = line.chars().peekable();
        while let Some(ch) = chars.next() {
            if ch == '"' {
                in_string = !in_string;
                output.push(ch);
            } else if !in_string && ch == '/' && chars.peek() == Some(&'/') {
                // Rest of the line is a comment.
                break;
            } else {
                output.push(ch);
            }
        }
        output.push('\n');
    }
    output
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::PathBuf,
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use pretty_assertions::assert_eq;

    use super::PathAliases;

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(name: &str) -> Self {
            let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-tsconfig-{name}-{}-{counter}",
                process::id()
            ));
            fs::create_dir_all(&path).expect("temp dir should create");
            Self { path }
        }

        fn write(&self, relative: &str, contents: &str) {
            fs::write(self.path.join(relative), contents).expect("fixture should write");
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn from_repo_root_returns_empty_when_tsconfig_missing() {
        let dir = TempDir::new("no-tsconfig");
        let aliases = PathAliases::from_repo_root(&dir.path);
        assert!(aliases.is_empty());
    }

    #[test]
    fn parses_common_at_slash_style_alias() {
        let dir = TempDir::new("common-alias");
        dir.write(
            "tsconfig.json",
            r#"{ "compilerOptions": { "paths": { "@/*": ["src/*"] } } }"#,
        );
        let aliases = PathAliases::from_repo_root(&dir.path);
        assert_eq!(aliases.rewrite("@/foo/bar").as_deref(), Some("src/foo/bar"));
    }

    #[test]
    fn parses_exact_path_alias() {
        let dir = TempDir::new("exact-alias");
        dir.write(
            "tsconfig.json",
            r#"{ "compilerOptions": { "paths": { "@repo/contracts": ["packages/contracts/src/index.ts"] } } }"#,
        );

        let aliases = PathAliases::from_repo_root(&dir.path);
        assert_eq!(
            aliases.rewrite("@repo/contracts").as_deref(),
            Some("packages/contracts/src/index.ts")
        );
    }

    #[test]
    fn applies_base_url_prefix_when_present() {
        let dir = TempDir::new("base-url");
        dir.write(
            "tsconfig.json",
            r#"{ "compilerOptions": { "baseUrl": "./src", "paths": { "@lib/*": ["lib/*"] } } }"#,
        );
        let aliases = PathAliases::from_repo_root(&dir.path);
        assert_eq!(
            aliases.rewrite("@lib/utils/helper").as_deref(),
            Some("src/lib/utils/helper")
        );
    }

    #[test]
    fn longest_prefix_match_wins() {
        let dir = TempDir::new("longest");
        dir.write(
            "tsconfig.json",
            r#"{ "compilerOptions": { "paths": {
                "@/*": ["src/*"],
                "@lib/*": ["src/lib/*"]
            } } }"#,
        );
        let aliases = PathAliases::from_repo_root(&dir.path);
        // @lib/foo should use the more-specific alias
        assert_eq!(
            aliases.rewrite("@lib/foo").as_deref(),
            Some("src/lib/foo"),
            "longer prefix @lib/ should win over shorter @/"
        );
        // @other uses the fallback @/ alias
        assert_eq!(
            aliases.rewrite("@/other").as_deref(),
            Some("src/other"),
            "generic @/ alias should rewrite when no longer prefix matches"
        );
    }

    #[test]
    fn no_rewrite_when_source_has_no_matching_alias() {
        let dir = TempDir::new("no-match");
        dir.write(
            "tsconfig.json",
            r#"{ "compilerOptions": { "paths": { "@app/*": ["src/*"] } } }"#,
        );
        let aliases = PathAliases::from_repo_root(&dir.path);
        // Relative and bare imports are not aliased — caller handles them.
        assert!(aliases.rewrite("./relative").is_none());
        assert!(aliases.rewrite("lodash").is_none());
    }

    #[test]
    fn jsonc_line_comments_do_not_break_parsing() {
        let dir = TempDir::new("jsonc");
        dir.write(
            "tsconfig.json",
            r#"{
                // top-level comment
                "compilerOptions": {
                    "paths": {
                        "@/*": ["src/*"] // inline comment
                    }
                }
            }"#,
        );
        let aliases = PathAliases::from_repo_root(&dir.path);
        assert_eq!(aliases.rewrite("@/foo").as_deref(), Some("src/foo"));
    }

    #[test]
    fn malformed_tsconfig_yields_empty_aliases() {
        let dir = TempDir::new("malformed");
        dir.write("tsconfig.json", "{ not valid json");
        let aliases = PathAliases::from_repo_root(&dir.path);
        assert!(aliases.is_empty());
    }

    #[test]
    fn ignores_alias_targets_that_escape_repo_root() {
        let dir = TempDir::new("outside-root");
        dir.write(
            "tsconfig.json",
            r#"{ "compilerOptions": { "paths": { "@external/*": ["../shared/*"] } } }"#,
        );
        let aliases = PathAliases::from_repo_root(&dir.path);
        assert!(aliases.rewrite("@external/foo").is_none());
    }
}
