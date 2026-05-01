//! Affected-repo expansion for `pr-review` mode.
//!
//! Given a list of changed files and the workspace config, `compute_affected_repos`
//! returns the set of configured repos that need re-indexing in the review pass.
//!
//! # Expansion rules
//!
//! - A change to `gather-step.config.yaml` at the repo root expands to all repos.
//! - A change under a shared-package indicator (`shared_contracts/`, top-level
//!   `*/package.json`, workspace-root `*/Cargo.toml`) expands to all repos.
//! - A change to a routing/gateway config file (`**/router.{ts,js}`,
//!   `**/routes.{ts,js}`, `**/gateway/*.{ts,js}`) expands to all repos.
//! - Otherwise, only the directly-changed repos (longest-prefix match against
//!   `config.repos[*].path`) are included.
//!
//! # Reverse-dependents expansion
//!
//! Full reverse-dependent expansion (i.e. "repo A imports repo B, so a change
//! in B also requires re-indexing A") requires loading the workspace registry
//! and is deferred to a follow-up task.
//!
//! TODO(follow-up): load the workspace `RegistryStore`, resolve reverse deps, and
//! include them in the affected set.
//!
//! Phase 4 Task 4 of the PR review mode plan.

use gather_step_core::GatherStepConfig;
use gather_step_git::refs::ChangedFile;

// ─── Public types ─────────────────────────────────────────────────────────────

/// Set of repos that must be re-indexed in the review pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AffectedRepos {
    pub repos: Vec<String>,
    /// `true` when the affected set was expanded to the full configured-repo
    /// list (e.g. because a shared package or root config file changed).
    pub all_repos: bool,
}

// ─── Core logic ───────────────────────────────────────────────────────────────

/// Compute the set of repos that need re-indexing for this PR.
///
/// See module-level docs for the expansion rules.
pub fn compute_affected_repos(
    config: &GatherStepConfig,
    changed_files: &[ChangedFile],
) -> AffectedRepos {
    // Fast path: nothing changed.
    if changed_files.is_empty() {
        return AffectedRepos::default();
    }

    // Build a repo-prefix lookup: (name, path-prefix-without-trailing-slash).
    let repo_prefixes: Vec<(&str, &str)> = config
        .repos
        .iter()
        .map(|r| (r.name.as_str(), r.path.trim_end_matches('/')))
        .collect();

    let mut directly_changed: Vec<String> = Vec::new();
    let mut expand_all = false;

    for cf in changed_files {
        let path = cf.path.as_str();

        // ── Expansion rule 1: root config change ──────────────────────────
        if path == "gather-step.config.yaml" {
            expand_all = true;
        }

        // ── Expansion rule 2: shared-package indicators ───────────────────
        //
        // Matches:
        //   - anything under shared_contracts/ (the data-model fixture name)
        //   - a top-level *.json manifest at depth 1: <segment>/package.json
        //   - a Cargo.toml at depth 1 (workspace-root manifest): <segment>/Cargo.toml
        if !expand_all
            && (path.starts_with("shared_contracts/")
                || path == "shared_contracts"
                || is_depth1_workspace_manifest(path))
        {
            expand_all = true;
        }

        // ── Expansion rule 3: gateway/routing config files ─────────────────
        if !expand_all && is_gateway_or_route_config(path) {
            expand_all = true;
        }

        // ── Direct repo matching (longest-prefix) ──────────────────────────
        if !expand_all {
            let matched = repo_prefixes
                .iter()
                .filter(|(_, prefix)| path == *prefix || path.starts_with(&format!("{prefix}/")))
                .max_by_key(|(_, prefix)| prefix.len());

            if let Some((name, _)) = matched {
                directly_changed.push((*name).to_owned());
            }
        }
    }

    if expand_all {
        let mut repos: Vec<String> = config.repos.iter().map(|r| r.name.clone()).collect();
        repos.sort();
        repos.dedup();
        return AffectedRepos {
            repos,
            all_repos: true,
        };
    }

    directly_changed.sort();
    directly_changed.dedup();
    AffectedRepos {
        repos: directly_changed,
        all_repos: false,
    }
}

// ─── Pattern helpers ──────────────────────────────────────────────────────────

/// Returns `true` when `path` is a workspace-root package manifest at depth 1:
///   - `<segment>/package.json`
///   - `<segment>/Cargo.toml`
///
/// "Depth 1" means exactly one directory component before the filename.
fn is_depth1_workspace_manifest(path: &str) -> bool {
    // Find the last slash; the filename is everything after it.
    let Some(slash_pos) = path.rfind('/') else {
        return false;
    };
    let filename = &path[slash_pos + 1..];
    let prefix = &path[..slash_pos];

    if !filename.eq_ignore_ascii_case("package.json")
        && !filename.eq_ignore_ascii_case("cargo.toml")
    {
        return false;
    }

    // prefix must be a single path segment (no slashes).
    !prefix.is_empty() && !prefix.contains('/')
}

/// Returns `true` when `path` matches a routing or gateway config file heuristic:
///   - `**/router.ts`, `**/router.js`
///   - `**/routes.ts`, `**/routes.js`
///   - `**/gateway/*.ts`, `**/gateway/*.js`
fn is_gateway_or_route_config(path: &str) -> bool {
    // Check filename (last path component) case-insensitively.
    let filename = path.rsplit('/').next().unwrap_or(path);

    if filename.eq_ignore_ascii_case("router.ts")
        || filename.eq_ignore_ascii_case("router.js")
        || filename.eq_ignore_ascii_case("routes.ts")
        || filename.eq_ignore_ascii_case("routes.js")
    {
        return true;
    }

    // Check if parent directory is named "gateway" and file is .ts/.js.
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() >= 2 {
        let parent = parts[parts.len() - 2];
        let file = parts[parts.len() - 1];
        let ext_ok = std::path::Path::new(file)
            .extension()
            .is_some_and(|e| e.eq_ignore_ascii_case("ts") || e.eq_ignore_ascii_case("js"));
        if parent.eq_ignore_ascii_case("gateway") && ext_ok {
            return true;
        }
    }

    false
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use gather_step_core::GatherStepConfig;
    use gather_step_git::refs::ChangedFile;

    use super::compute_affected_repos;

    fn make_config() -> GatherStepConfig {
        GatherStepConfig::from_yaml_str(
            r"
repos:
  - name: repo_a
    path: services/repo_a
  - name: repo_b
    path: services/repo_b
  - name: repo_c
    path: services/repo_c
",
        )
        .expect("fixture config should parse")
    }

    fn changed(paths: &[&str]) -> Vec<ChangedFile> {
        paths
            .iter()
            .map(|p| ChangedFile {
                path: (*p).to_owned(),
                change_kind: gather_step_git::refs::ChangeKind::Modified,
                old_path: None,
            })
            .collect()
    }

    // ── Task 4 tests ─────────────────────────────────────────────────────────

    #[test]
    fn single_repo_change_lists_only_that_repo() {
        let config = make_config();
        let files = changed(&["services/repo_a/src/lib.ts"]);
        let result = compute_affected_repos(&config, &files);
        assert_eq!(result.repos, vec!["repo_a".to_owned()]);
        assert!(!result.all_repos);
    }

    #[test]
    fn shared_package_change_expands_to_all() {
        let config = make_config();
        let files = changed(&["shared_contracts/events/order.ts"]);
        let result = compute_affected_repos(&config, &files);
        assert!(result.all_repos, "shared_contracts change should expand to all");
        assert_eq!(result.repos.len(), 3);
    }

    #[test]
    fn root_config_change_expands_to_all() {
        let config = make_config();
        let files = changed(&["gather-step.config.yaml"]);
        let result = compute_affected_repos(&config, &files);
        assert!(result.all_repos, "root config change should expand to all");
        assert_eq!(result.repos.len(), 3);
    }

    #[test]
    fn gateway_config_change_expands_to_all() {
        let config = make_config();
        let files = changed(&["services/repo_a/src/gateway/routes.ts"]);
        let result = compute_affected_repos(&config, &files);
        assert!(
            result.all_repos,
            "gateway/routes.ts change should expand to all"
        );
        assert_eq!(result.repos.len(), 3);
    }

    #[test]
    fn unrelated_root_file_does_not_expand() {
        let config = make_config();
        let files = changed(&["README.md"]);
        let result = compute_affected_repos(&config, &files);
        assert!(!result.all_repos);
        assert!(
            result.repos.is_empty(),
            "README.md at root should not match any repo"
        );
    }
}
