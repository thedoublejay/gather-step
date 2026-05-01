//! Symbol delta extraction — Phase 2 Task 3.
//!
//! Diffs exported symbols between a baseline graph and a review graph to produce
//! [`SymbolDeltas`] (added / removed / changed).
//!
//! # What is enumerated
//!
//! - All `NodeKind::SharedSymbol` virtual stubs (cross-repo contracts).
//! - All concrete `NodeKind::Function | Class | Type` nodes with
//!   `visibility == Some(Visibility::Public)` and `is_virtual == false`.
//!
//! Private / internal / file / module / import nodes are excluded — they are
//! not part of the PR-review-relevant public surface.
//!
//! # Diff key
//!
//! `(repo, qualified_name)` — avoids false rename-induced removals caused by
//! node-id changes.

use anyhow::Result;
use gather_step_core::{NodeData, NodeKind, Visibility};
use gather_step_storage::GraphStore;
use rustc_hash::FxHashMap;

use crate::pr_review::delta_report::{SymbolDelta, SymbolDeltaChange, SymbolDeltas};

/// `(repo, qualified_name)` → `SymbolDelta` mapping built from one snapshot.
type SymbolMap = FxHashMap<(String, String), SymbolDelta>;

/// Extract added / removed / changed exported symbols by diffing the graphs in
/// `baseline` against `review`.
///
/// If `baseline` is an empty / never-indexed store every review symbol is
/// reported as `added` — no error is returned.
pub fn extract_symbol_deltas<S: GraphStore>(baseline: &S, review: &S) -> Result<SymbolDeltas> {
    let baseline_map = build_symbol_map(baseline)?;
    let review_map = build_symbol_map(review)?;

    let mut added: Vec<SymbolDelta> = Vec::new();
    let mut removed: Vec<SymbolDelta> = Vec::new();
    let mut changed: Vec<SymbolDeltaChange> = Vec::new();

    // Added: in review but not in baseline.
    for (key, delta) in &review_map {
        if !baseline_map.contains_key(key) {
            added.push(delta.clone());
        }
    }

    // Removed: in baseline but not in review.
    for (key, delta) in &baseline_map {
        if !review_map.contains_key(key) {
            removed.push(delta.clone());
        }
    }

    // Changed: same key in both — diff signature and visibility.
    for (key, review_delta) in &review_map {
        if let Some(baseline_delta) = baseline_map.get(key) {
            let signature_changed = baseline_delta.signature != review_delta.signature;
            let visibility_changed = baseline_delta.visibility != review_delta.visibility;
            if signature_changed || visibility_changed {
                changed.push(SymbolDeltaChange {
                    kind: review_delta.kind.clone(),
                    repo: key.0.clone(),
                    qualified_name: key.1.clone(),
                    before: baseline_delta.clone(),
                    after: review_delta.clone(),
                    signature_changed,
                    visibility_changed,
                });
            }
        }
    }

    // Deterministic output — sort by (kind, repo, qualified_name).
    added.sort_by(|a, b| (&a.kind, &a.repo, &a.qualified_name).cmp(&(&b.kind, &b.repo, &b.qualified_name)));
    removed.sort_by(|a, b| (&a.kind, &a.repo, &a.qualified_name).cmp(&(&b.kind, &b.repo, &b.qualified_name)));
    changed.sort_by(|a, b| (&a.kind, &a.repo, &a.qualified_name).cmp(&(&b.kind, &b.repo, &b.qualified_name)));

    Ok(SymbolDeltas {
        added,
        removed,
        changed,
        unavailable: false,
    })
}

// ── Public helpers ────────────────────────────────────────────────────────────

/// Find the [`NodeId`] of the node that matches `(repo, qualified_name)` in
/// `store`.
///
/// Searches `SharedSymbol` virtual stubs first, then concrete public
/// `Function | Class | Type` nodes.  Returns `None` when no match is found.
///
/// Used by the impact-attachment wiring in `commands/pr_review.rs`.
pub fn find_symbol_node_id<S: GraphStore>(
    store: &S,
    repo: &str,
    qualified_name: &str,
) -> Result<Option<gather_step_core::NodeId>> {
    // ── SharedSymbol virtual stubs ────────────────────────────────────────────
    for node in store.nodes_by_type(NodeKind::SharedSymbol)? {
        if !node.is_virtual {
            continue;
        }
        if node.repo == repo
            && node.qualified_name.as_deref() == Some(qualified_name)
        {
            return Ok(Some(node.id));
        }
    }

    // ── Concrete public symbols ───────────────────────────────────────────────
    for node in store.nodes_by_repo(repo)? {
        if node.is_virtual {
            continue;
        }
        if !matches!(node.kind, NodeKind::Function | NodeKind::Class | NodeKind::Type) {
            continue;
        }
        if node.visibility != Some(Visibility::Public) {
            continue;
        }
        if node.qualified_name.as_deref() == Some(qualified_name) {
            return Ok(Some(node.id));
        }
    }

    Ok(None)
}

// ── Internals ─────────────────────────────────────────────────────────────────

/// Build `(repo, qualified_name) → SymbolDelta` for all relevant nodes in `store`.
///
/// Enumerates:
/// 1. All `SharedSymbol` virtual nodes (keyed by repo = `"__virtual__"` when
///    `node.repo` is the virtual sentinel, or the actual repo if set).
/// 2. All concrete `Function | Class | Type` nodes with `Public` visibility.
fn build_symbol_map<S: GraphStore>(store: &S) -> Result<SymbolMap> {
    let mut map = SymbolMap::default();

    // ── 1. SharedSymbol virtual stubs ─────────────────────────────────────────
    for node in store.nodes_by_type(NodeKind::SharedSymbol)? {
        if !node.is_virtual {
            // Only virtual stubs are cross-repo contracts; skip concrete declarations.
            continue;
        }
        let Some(qn) = node.qualified_name.clone() else {
            continue;
        };
        let delta = node_to_delta(&node, "shared_symbol");
        map.insert((node.repo.clone(), qn), delta);
    }

    // ── 2. Concrete public Function / Class / Type nodes ──────────────────────
    // Enumerate all repos via NodeKind::Repo virtual nodes so we can call
    // nodes_by_repo per repo.
    let repo_nodes = store.nodes_by_type(NodeKind::Repo)?;
    let repos: Vec<String> = if repo_nodes.is_empty() {
        // Fallback: collect repos from SharedSymbol scan above — but here we
        // do a full type scan for the three concrete kinds instead.
        collect_repos_from_kinds(store)?
    } else {
        repo_nodes.into_iter().map(|n| n.name).collect()
    };

    for repo in &repos {
        for node in store.nodes_by_repo(repo)? {
            if node.is_virtual {
                continue;
            }
            let kind_str = match node.kind {
                NodeKind::Function => "function",
                NodeKind::Class => "class",
                NodeKind::Type => "type",
                _ => continue,
            };
            // Only public symbols are PR-review relevant.
            if node.visibility != Some(Visibility::Public) {
                continue;
            }
            let Some(qn) = node.qualified_name.clone() else {
                continue;
            };
            let delta = node_to_delta(&node, kind_str);
            map.insert((node.repo.clone(), qn), delta);
        }
    }

    Ok(map)
}

/// Collect repo names by scanning Function/Class/Type nodes (fallback when no
/// `Repo` virtual nodes are indexed yet).
fn collect_repos_from_kinds<S: GraphStore>(store: &S) -> Result<Vec<String>> {
    let mut repos = rustc_hash::FxHashSet::default();
    for kind in [NodeKind::Function, NodeKind::Class, NodeKind::Type] {
        for node in store.nodes_by_type(kind)? {
            if !node.is_virtual {
                repos.insert(node.repo.clone());
            }
        }
    }
    Ok(repos.into_iter().collect())
}

/// Convert a graph node to the report struct.
fn node_to_delta(node: &NodeData, kind_str: &str) -> SymbolDelta {
    let visibility = node.visibility.clone().map(|v| match v {
        Visibility::Public => "public",
        Visibility::Protected => "protected",
        Visibility::Private => "private",
        Visibility::Package => "package",
        Visibility::Internal => "internal",
        _ => "unknown",
    });
    SymbolDelta {
        kind: kind_str.to_owned(),
        repo: node.repo.clone(),
        qualified_name: node.qualified_name.clone().unwrap_or_else(|| node.name.clone()),
        file: if node.file_path.is_empty() {
            None
        } else {
            Some(node.file_path.clone())
        },
        line: node.span.as_ref().map(|s| s.line_start),
        signature: node.signature.clone(),
        visibility: visibility.map(str::to_owned),
        is_virtual: node.is_virtual,
        impact: None,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::{NodeData, NodeKind, SourceSpan, Visibility, node_id, virtual_node};
    use gather_step_storage::{GraphStore, GraphStoreDb};

    use super::extract_symbol_deltas;

    // ── temp helpers ──────────────────────────────────────────────────────────

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new(label: &str) -> Self {
            let id = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gs-symbol-extractor-{label}-{}-{id}.redb",
                std::process::id()
            ));
            Self { path }
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
        }
    }

    fn open_store(label: &str) -> (TempDb, GraphStoreDb) {
        let tmp = TempDb::new(label);
        let db = GraphStoreDb::open(&tmp.path).expect("store should open");
        (tmp, db)
    }

    // ── node builders ─────────────────────────────────────────────────────────

    fn shared_symbol_node(repo: &str, name: &str, qn: &str) -> NodeData {
        let mut n = virtual_node(NodeKind::SharedSymbol, repo, "__virtual__", name, qn);
        // virtual_node sets repo to the first arg — ensure it matches.
        n.repo = repo.to_owned();
        n
    }

    fn function_node(repo: &str, file: &str, name: &str, qn: &str, vis: Visibility) -> NodeData {
        NodeData {
            id: node_id(repo, file, NodeKind::Function, qn),
            kind: NodeKind::Function,
            repo: repo.to_owned(),
            file_path: file.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(qn.to_owned()),
            external_id: None,
            signature: Some(format!("{name}(): void")),
            visibility: Some(vis),
            span: Some(SourceSpan {
                line_start: 10,
                line_len: 5,
                column_start: 0,
                column_len: 0,
            }),
            is_virtual: false,
        }
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /// A `SharedSymbol` virtual stub present in review but absent in baseline
    /// must appear in `added` with `kind == "shared_symbol"`.
    #[test]
    fn shared_symbol_added_appears_in_added_list() {
        let (_td_b, baseline) = open_store("ss-added-baseline");
        let (_td_r, review) = open_store("ss-added-review");

        let node = shared_symbol_node("__virtual__", "OrderCreated", "__shared__OrderCreated");
        review
            .bulk_insert(&[node], &[])
            .expect("insert should succeed");

        let deltas = extract_symbol_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(deltas.added.len(), 1, "expected one added symbol");
        assert_eq!(deltas.added[0].kind, "shared_symbol");
        assert!(deltas.removed.is_empty(), "nothing removed");
        assert!(deltas.changed.is_empty(), "nothing changed");
    }

    /// A concrete `Function` with `Public` visibility in baseline but absent in
    /// review must appear in `removed` with `kind == "function"`.
    #[test]
    fn public_function_removed_appears_in_removed_list() {
        let (_td_b, baseline) = open_store("fn-removed-baseline");
        let (_td_r, review) = open_store("fn-removed-review");

        let node = function_node("api", "src/orders.ts", "listOrders", "api::listOrders", Visibility::Public);
        baseline
            .bulk_insert(&[node], &[])
            .expect("insert should succeed");

        let deltas = extract_symbol_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(deltas.removed.len(), 1, "expected one removed symbol");
        assert_eq!(deltas.removed[0].kind, "function");
        assert_eq!(deltas.removed[0].qualified_name, "api::listOrders");
        assert!(deltas.added.is_empty(), "nothing added");
        assert!(deltas.changed.is_empty(), "nothing changed");
    }

    /// Same `(repo, qualified_name)` in both snapshots but `signature` differs →
    /// appears in `changed` with `signature_changed = true, visibility_changed = false`.
    #[test]
    fn signature_change_appears_in_changed_list() {
        let (_td_b, baseline) = open_store("sig-changed-baseline");
        let (_td_r, review) = open_store("sig-changed-review");

        let mut base_node =
            function_node("api", "src/orders.ts", "listOrders", "api::listOrders", Visibility::Public);
        base_node.signature = Some("listOrders(): Order[]".to_owned());
        baseline
            .bulk_insert(&[base_node], &[])
            .expect("baseline insert");

        let mut review_node =
            function_node("api", "src/orders.ts", "listOrders", "api::listOrders", Visibility::Public);
        review_node.signature = Some("listOrders(filter: Filter): Order[]".to_owned());
        review
            .bulk_insert(&[review_node], &[])
            .expect("review insert");

        let deltas = extract_symbol_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(deltas.changed.len(), 1, "expected one changed symbol");
        let c = &deltas.changed[0];
        assert!(c.signature_changed, "signature_changed must be true");
        assert!(!c.visibility_changed, "visibility_changed must be false");
        assert!(deltas.added.is_empty(), "nothing added");
        assert!(deltas.removed.is_empty(), "nothing removed");
    }

    /// A `Function` with `Visibility::Private` must NOT appear in any list.
    #[test]
    fn private_functions_are_excluded() {
        let (_td_b, baseline) = open_store("private-fn-baseline");
        let (_td_r, review) = open_store("private-fn-review");

        let node = function_node(
            "api",
            "src/orders.ts",
            "internalHelper",
            "api::internalHelper",
            Visibility::Private,
        );
        review
            .bulk_insert(&[node], &[])
            .expect("insert should succeed");

        let deltas = extract_symbol_deltas(&baseline, &review).expect("should succeed");

        assert!(
            deltas.added.is_empty(),
            "private function must not appear in added"
        );
        assert!(deltas.removed.is_empty(), "nothing removed");
        assert!(deltas.changed.is_empty(), "nothing changed");
    }
}
