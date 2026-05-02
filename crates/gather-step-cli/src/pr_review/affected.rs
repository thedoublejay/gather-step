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
//! When a [`GraphStore`] baseline is provided and `all_repos` did not trigger,
//! the function expands the affected set to include any repo that has incoming
//! cross-repo edges from a directly-changed repo.  This catches consumers that
//! import shared symbols, types, events, or contracts from the changed repo.
//!
//! The walk is capped at [`REVERSE_DEP_CAP`] incoming edges per directly-changed
//! repo.  If the cap is hit, `expansion_truncated` is set to `true` in the
//! returned [`AffectedRepos`].
//!
//! Pass `None` for `baseline` to skip the walk (MVP / no-index fallback).
//!
//! Phase 4 Task 4 of the PR review mode plan.

use gather_step_core::{EdgeKind, GatherStepConfig};
use rustc_hash::FxHashSet;
use gather_step_git::refs::ChangedFile;
use gather_step_storage::GraphStore;

/// Maximum number of incoming edges inspected per directly-changed repo before
/// truncating the reverse-dependents walk.
const REVERSE_DEP_CAP: usize = 5_000;

// ─── Public types ─────────────────────────────────────────────────────────────

/// Set of repos that must be re-indexed in the review pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AffectedRepos {
    pub repos: Vec<String>,
    /// `true` when the affected set was expanded to the full configured-repo
    /// list (e.g. because a shared package or root config file changed).
    pub all_repos: bool,
    /// `true` when the reverse-dependents walk hit [`REVERSE_DEP_CAP`] and was
    /// truncated.  The affected set may be incomplete in that case.
    pub expansion_truncated: bool,
}

// ─── Core logic ───────────────────────────────────────────────────────────────

/// Compute the set of repos that need re-indexing for this PR.
///
/// See module-level docs for the expansion rules.
///
/// `baseline` is optional: pass `None` to skip the reverse-dependents walk
/// (e.g. when no baseline index exists yet).
pub fn compute_affected_repos<S: GraphStore>(
    config: &GatherStepConfig,
    changed_files: &[ChangedFile],
    baseline: Option<&S>,
) -> AffectedRepos {
    compute_affected_repos_with_cap(config, changed_files, baseline, REVERSE_DEP_CAP)
}

/// Internal helper that accepts a configurable cap — used by tests.
pub(crate) fn compute_affected_repos_with_cap<S: GraphStore>(
    config: &GatherStepConfig,
    changed_files: &[ChangedFile],
    baseline: Option<&S>,
    cap: usize,
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
            expansion_truncated: false,
        };
    }

    directly_changed.sort();
    directly_changed.dedup();

    // ── Reverse-dependents expansion ──────────────────────────────────────────
    // Only when: a baseline graph is available, all_repos is false, and there
    // are directly-changed repos to walk from.
    let mut expansion_truncated = false;

    if let Some(store) = baseline.filter(|_| !directly_changed.is_empty()) {
        let mut consumer_repos: FxHashSet<String> = FxHashSet::default();
        let cross_repo_kinds: &[EdgeKind] = &[
            EdgeKind::UsesShared,
            EdgeKind::UsesTypeFrom,
            EdgeKind::UsesEventFrom,
            EdgeKind::ConsumesApiFrom,
            EdgeKind::ImplementsContractFrom,
        ];

        'outer: for changed_repo in &directly_changed {
            let Ok(nodes) = store.nodes_by_repo(changed_repo) else {
                continue;
            };

            let mut edge_count: usize = 0;

            for node in &nodes {
                let Ok(incoming) = store.get_incoming(node.id) else {
                    continue;
                };

                for edge in incoming {
                    if !cross_repo_kinds.contains(&edge.kind) {
                        continue;
                    }

                    edge_count += 1;
                    if edge_count > cap {
                        expansion_truncated = true;
                        break 'outer;
                    }

                    // Look up the source node to find its repo.
                    if let Ok(Some(source_node)) = store.get_node(edge.source) {
                        // Only add if it's a different repo (avoid self-loops).
                        if source_node.repo != *changed_repo {
                            consumer_repos.insert(source_node.repo.clone());
                        }
                    }
                }
            }
        }

        for repo in consumer_repos {
            if !directly_changed.contains(&repo) {
                directly_changed.push(repo);
            }
        }

        directly_changed.sort();
        directly_changed.dedup();
    }

    AffectedRepos {
        repos: directly_changed,
        all_repos: false,
        expansion_truncated,
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
    use std::{
        env,
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::{
        EdgeData, EdgeKind, EdgeMetadata, GatherStepConfig, NodeData, NodeId, NodeKind, Visibility,
        node_id,
    };
    use gather_step_git::refs::ChangedFile;
    use gather_step_storage::{GraphStore, GraphStoreDb};

    use super::{compute_affected_repos, compute_affected_repos_with_cap};

    // ── temp-db helpers ───────────────────────────────────────────────────────

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new(label: &str) -> Self {
            let id = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gs-affected-{label}-{}-{id}.redb",
                std::process::id()
            ));
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    fn open_store(label: &str) -> (TempDb, GraphStoreDb) {
        let tmp = TempDb::new(label);
        let db = GraphStoreDb::open(tmp.path()).expect("store should open");
        (tmp, db)
    }

    // ── graph-building helpers ────────────────────────────────────────────────

    fn symbol_node(repo: &str, file: &str, name: &str) -> NodeData {
        NodeData {
            id: node_id(repo, file, NodeKind::Function, name),
            kind: NodeKind::Function,
            repo: repo.to_owned(),
            file_path: file.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(format!("{repo}::{name}")),
            external_id: None,
            signature: None,
            visibility: Some(Visibility::Public),
            span: None,
            is_virtual: false,
        }
    }

    fn file_node(repo: &str, path: &str) -> NodeData {
        NodeData {
            id: node_id(repo, path, NodeKind::File, path),
            kind: NodeKind::File,
            repo: repo.to_owned(),
            file_path: path.to_owned(),
            name: path.to_owned(),
            qualified_name: None,
            external_id: None,
            signature: None,
            visibility: None,
            span: None,
            is_virtual: false,
        }
    }

    fn edge(source: NodeId, target: NodeId, kind: EdgeKind, owner: NodeId) -> EdgeData {
        EdgeData {
            source,
            target,
            kind,
            metadata: EdgeMetadata::default(),
            owner_file: owner,
            is_cross_file: true,
        }
    }

    // ── config helper ─────────────────────────────────────────────────────────

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
        let result = compute_affected_repos::<GraphStoreDb>(&config, &files, None);
        assert_eq!(result.repos, vec!["repo_a".to_owned()]);
        assert!(!result.all_repos);
    }

    #[test]
    fn shared_package_change_expands_to_all() {
        let config = make_config();
        let files = changed(&["shared_contracts/events/order.ts"]);
        let result = compute_affected_repos::<GraphStoreDb>(&config, &files, None);
        assert!(
            result.all_repos,
            "shared_contracts change should expand to all"
        );
        assert_eq!(result.repos.len(), 3);
    }

    #[test]
    fn root_config_change_expands_to_all() {
        let config = make_config();
        let files = changed(&["gather-step.config.yaml"]);
        let result = compute_affected_repos::<GraphStoreDb>(&config, &files, None);
        assert!(result.all_repos, "root config change should expand to all");
        assert_eq!(result.repos.len(), 3);
    }

    #[test]
    fn gateway_config_change_expands_to_all() {
        let config = make_config();
        let files = changed(&["services/repo_a/src/gateway/routes.ts"]);
        let result = compute_affected_repos::<GraphStoreDb>(&config, &files, None);
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
        let result = compute_affected_repos::<GraphStoreDb>(&config, &files, None);
        assert!(!result.all_repos);
        assert!(
            result.repos.is_empty(),
            "README.md at root should not match any repo"
        );
    }

    // ── Reverse-dependents expansion tests ───────────────────────────────────

    /// A `UsesShared` edge from `repo_b` → `repo_a` symbol causes `repo_b` to
    /// appear in the affected set when `repo_a` is the changed repo.
    #[test]
    fn reverse_dependents_expansion_includes_consumer_repos() {
        let (_td, store) = open_store("rev-dep-basic");
        let config = make_config();

        // repo_a owns a shared symbol
        let a_file = file_node("repo_a", "src/lib.ts");
        let a_sym = symbol_node("repo_a", "src/lib.ts", "SharedThing");
        // repo_b consumes it via UsesShared
        let b_file = file_node("repo_b", "src/consumer.ts");
        let b_sym = symbol_node("repo_b", "src/consumer.ts", "useIt");

        store
            .bulk_insert(
                &[a_file.clone(), a_sym.clone(), b_file.clone(), b_sym.clone()],
                &[edge(b_sym.id, a_sym.id, EdgeKind::UsesShared, b_file.id)],
            )
            .expect("bulk insert");

        let files = changed(&["services/repo_a/src/lib.ts"]);
        let result = compute_affected_repos(&config, &files, Some(&store));

        assert!(!result.all_repos);
        assert!(!result.expansion_truncated);
        let mut expected = vec!["repo_a".to_owned(), "repo_b".to_owned()];
        expected.sort();
        assert_eq!(result.repos, expected);
    }

    /// When `all_repos` is already triggered by a shared-package change, the
    /// reverse-dependents walk is not performed.  The function must not fail
    /// even when baseline is `None`.
    #[test]
    fn reverse_dependents_does_not_apply_when_all_repos_set() {
        let config = make_config();
        let files = changed(&["shared_contracts/events/order.ts"]);
        // No baseline — must still succeed.
        let result = compute_affected_repos::<GraphStoreDb>(&config, &files, None);
        assert!(result.all_repos, "all_repos must be true");
        assert!(!result.expansion_truncated);
        assert_eq!(result.repos.len(), 3);
    }

    /// When the baseline has nodes for the changed repo but no incoming
    /// cross-repo edges, the result contains only the directly-changed repo.
    #[test]
    fn reverse_dependents_with_no_consumers_returns_only_changed_repo() {
        let (_td, store) = open_store("rev-dep-no-consumers");
        let config = make_config();

        let a_file = file_node("repo_a", "src/lib.ts");
        let a_sym = symbol_node("repo_a", "src/lib.ts", "PrivateThing");

        store
            .bulk_insert(&[a_file.clone(), a_sym.clone()], &[])
            .expect("bulk insert");

        let files = changed(&["services/repo_a/src/lib.ts"]);
        let result = compute_affected_repos(&config, &files, Some(&store));

        assert!(!result.all_repos);
        assert!(!result.expansion_truncated);
        assert_eq!(result.repos, vec!["repo_a".to_owned()]);
    }

    /// When incoming edge count exceeds the cap, `expansion_truncated` is set.
    /// Uses a low cap (5) via the internal helper to avoid building 5001 nodes.
    #[test]
    fn reverse_dependents_truncation_sets_flag() {
        let (_td, store) = open_store("rev-dep-truncation");
        let config = make_config();

        let a_file = file_node("repo_a", "src/lib.ts");
        let a_sym = symbol_node("repo_a", "src/lib.ts", "BigSharedThing");

        // Build 6 consumers (more than cap=5).
        let mut nodes: Vec<NodeData> = vec![a_file.clone(), a_sym.clone()];
        let mut edges: Vec<EdgeData> = Vec::new();

        for i in 0..6u64 {
            let b_file = file_node("repo_b", &format!("src/consumer_{i}.ts"));
            let b_sym = symbol_node(
                "repo_b",
                &format!("src/consumer_{i}.ts"),
                &format!("use_{i}"),
            );
            edges.push(edge(b_sym.id, a_sym.id, EdgeKind::UsesShared, b_file.id));
            nodes.push(b_file);
            nodes.push(b_sym);
        }

        store.bulk_insert(&nodes, &edges).expect("bulk insert");

        let files = changed(&["services/repo_a/src/lib.ts"]);
        // Cap of 5 — 6 edges will exceed it.
        let result = compute_affected_repos_with_cap(&config, &files, Some(&store), 5);

        assert!(result.expansion_truncated, "truncation flag must be set");
    }

    /// An incoming edge whose source node is in the same repo as the changed
    /// repo does not double-count that repo.
    #[test]
    fn reverse_dependents_handles_self_loops() {
        let (_td, store) = open_store("rev-dep-self-loop");
        let config = make_config();

        let a_file = file_node("repo_a", "src/lib.ts");
        let a_sym = symbol_node("repo_a", "src/lib.ts", "SharedThing");
        let a_consumer = symbol_node("repo_a", "src/internal.ts", "internalUse");
        let a_con_file = file_node("repo_a", "src/internal.ts");

        store
            .bulk_insert(
                &[
                    a_file.clone(),
                    a_sym.clone(),
                    a_con_file.clone(),
                    a_consumer.clone(),
                ],
                &[edge(
                    a_consumer.id,
                    a_sym.id,
                    EdgeKind::UsesShared,
                    a_con_file.id,
                )],
            )
            .expect("bulk insert");

        let files = changed(&["services/repo_a/src/lib.ts"]);
        let result = compute_affected_repos(&config, &files, Some(&store));

        assert!(!result.all_repos);
        assert!(!result.expansion_truncated);
        // repo_a should appear exactly once.
        assert_eq!(result.repos, vec!["repo_a".to_owned()]);
    }

    /// Multiple incoming edges from the same consuming repo produce one entry.
    #[test]
    fn reverse_dependents_dedupes_repos() {
        let (_td, store) = open_store("rev-dep-dedup");
        let config = make_config();

        let a_file = file_node("repo_a", "src/lib.ts");
        let a_sym = symbol_node("repo_a", "src/lib.ts", "SharedThing");

        // Two separate consumers in repo_b pointing at the same symbol.
        let b_file = file_node("repo_b", "src/c1.ts");
        let b_sym1 = symbol_node("repo_b", "src/c1.ts", "consumer1");
        let b_file2 = file_node("repo_b", "src/c2.ts");
        let b_sym2 = symbol_node("repo_b", "src/c2.ts", "consumer2");

        store
            .bulk_insert(
                &[
                    a_file.clone(),
                    a_sym.clone(),
                    b_file.clone(),
                    b_sym1.clone(),
                    b_file2.clone(),
                    b_sym2.clone(),
                ],
                &[
                    edge(b_sym1.id, a_sym.id, EdgeKind::UsesShared, b_file.id),
                    edge(b_sym2.id, a_sym.id, EdgeKind::UsesTypeFrom, b_file2.id),
                ],
            )
            .expect("bulk insert");

        let files = changed(&["services/repo_a/src/lib.ts"]);
        let result = compute_affected_repos(&config, &files, Some(&store));

        assert!(!result.all_repos);
        assert!(!result.expansion_truncated);
        let mut expected = vec!["repo_a".to_owned(), "repo_b".to_owned()];
        expected.sort();
        assert_eq!(result.repos, expected, "repo_b must appear exactly once");
    }
}
