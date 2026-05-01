//! Decorator delta extraction — Phase 3 Task 4.
//!
//! Diffs `NodeKind::Decorator` nodes between the baseline and review graph
//! stores to surface added / removed / changed RBAC, audit, and auth
//! annotations.
//!
//! # Interesting decorators
//!
//! Only decorators whose `name` matches (case-insensitively) one of the
//! following are emitted:
//!
//! ```text
//! Audit, Permission, Authenticated, Authorized,
//! RolesAllowed, RequiresPermission
//! ```
//!
//! This list will grow as new security-relevant decorators are registered.
//!
//! # Diff key
//!
//! `(repo, file_path, decorator_name, target_qualified_name)`.
//!
//! Decorator nodes encode the target via `UsesDecorator` outgoing edges.  If
//! the target edge is absent the key uses an empty `target_qualified_name`
//! string, which may cause spurious diffs — this is logged as a concern.
//!
//! # Data-model note
//!
//! Decorator nodes store the decorator name in `NodeData::name` and the raw
//! argument signature (when available) in `NodeData::signature`.  The target
//! symbol is found by walking the incoming `UsesDecorator` edge from the
//! *target* node, or the outgoing edge from the decorator itself — both
//! directions are tried.  If the data model lacks enough information for a
//! reliable target resolution the extractor emits an empty list and logs a
//! warning so the parser can be extended later.

use anyhow::Result;
use gather_step_core::{EdgeKind, NodeKind};
use gather_step_storage::GraphStore;
use rustc_hash::FxHashMap;

use crate::pr_review::delta_report::{DecoratorDelta, DecoratorDeltaChange, DecoratorDeltas};

/// Decorator names considered interesting for PR review (case-insensitive).
///
/// This list will grow as new security/RBAC decorators are registered.
const INTERESTING_DECORATORS: &[&str] = &[
    "audit",
    "permission",
    "authenticated",
    "authorized",
    "rolesallowed",
    "requirespermission",
];

/// `(repo, file_path, decorator_name, target_qualified_name)` diff key.
type DecoratorKey = (String, String, String, String);

/// Extract added / removed / changed decorator deltas.
pub fn extract_decorator_deltas<S: GraphStore>(
    baseline: &S,
    review: &S,
) -> Result<DecoratorDeltas> {
    let baseline_map = build_decorator_map(baseline)?;
    let review_map = build_decorator_map(review)?;

    let mut added: Vec<DecoratorDelta> = Vec::new();
    let mut removed: Vec<DecoratorDelta> = Vec::new();
    let mut changed: Vec<DecoratorDeltaChange> = Vec::new();

    // Added: in review only.
    for (key, delta) in &review_map {
        if !baseline_map.contains_key(key) {
            added.push(delta.clone());
        }
    }

    // Removed: in baseline only.
    for (key, delta) in &baseline_map {
        if !review_map.contains_key(key) {
            removed.push(delta.clone());
        }
    }

    // Changed: same key, different args.
    for (key, review_delta) in &review_map {
        if let Some(baseline_delta) = baseline_map.get(key).filter(|b| b.args != review_delta.args) {
            changed.push(DecoratorDeltaChange {
                repo: key.0.clone(),
                target_qualified_name: key.3.clone(),
                before: baseline_delta.clone(),
                after: review_delta.clone(),
                args_changed: true,
            });
        }
    }

    // Sort for deterministic output.
    added.sort_by(|a, b| {
        a.repo
            .cmp(&b.repo)
            .then_with(|| a.decorator_name.cmp(&b.decorator_name))
    });
    removed.sort_by(|a, b| {
        a.repo
            .cmp(&b.repo)
            .then_with(|| a.decorator_name.cmp(&b.decorator_name))
    });
    changed.sort_by(|a, b| {
        a.repo
            .cmp(&b.repo)
            .then_with(|| a.target_qualified_name.cmp(&b.target_qualified_name))
    });

    Ok(DecoratorDeltas { added, removed, changed, unavailable: false })
}

// ── Internals ─────────────────────────────────────────────────────────────────

/// Returns `true` if the decorator name is on the interesting list.
fn is_interesting(name: &str) -> bool {
    INTERESTING_DECORATORS
        .iter()
        .any(|&n| n.eq_ignore_ascii_case(name))
}

/// Enumerate all `NodeKind::Decorator` nodes from `store` and index them by
/// diff key.
///
/// Target resolution: walk `UsesDecorator` edges.  If neither direction yields
/// a target the field is left empty (and a debug-level log is emitted).
fn build_decorator_map<S: GraphStore>(store: &S) -> Result<FxHashMap<DecoratorKey, DecoratorDelta>> {
    let nodes = store.nodes_by_type(NodeKind::Decorator)?;

    if nodes.is_empty() {
        return Ok(FxHashMap::default());
    }

    let mut map: FxHashMap<DecoratorKey, DecoratorDelta> = FxHashMap::default();

    for node in nodes {
        if !is_interesting(&node.name) {
            continue;
        }

        // Attempt target resolution via outgoing edges from this decorator.
        let target_qn: Option<String> = resolve_target(store, &node)?;

        let key = (
            node.repo.clone(),
            node.file_path.clone(),
            node.name.clone(),
            target_qn.clone().unwrap_or_default(),
        );

        let delta = DecoratorDelta {
            repo: node.repo.clone(),
            file: if node.file_path.is_empty() {
                None
            } else {
                Some(node.file_path.clone())
            },
            line: node.span.as_ref().map(|s| s.line_start),
            decorator_name: node.name.clone(),
            target_qualified_name: target_qn,
            args: node.signature.clone(),
        };

        // Last record for a given key wins (deduplication).
        map.insert(key, delta);
    }

    Ok(map)
}

/// Resolve the target symbol for a decorator node by walking edges.
///
/// Strategy:
/// 1. Walk outgoing edges from the decorator node — look for `UsesDecorator`
///    edges pointing from a *target* symbol TO the decorator (i.e., we check
///    *incoming* edges on the decorator).
/// 2. Return the first target's `qualified_name` if found; `None` otherwise.
fn resolve_target<S: GraphStore>(store: &S, node: &gather_step_core::NodeData) -> Result<Option<String>> {
    // Walk incoming edges: target → (UsesDecorator) → decorator
    let incoming = store.get_incoming(node.id)?;
    for edge in &incoming {
        if edge.kind == EdgeKind::UsesDecorator {
            // The source of this edge is the target symbol.
            if let Some(src_node) = store.get_node(edge.source)?
                && let Some(qn) = src_node.qualified_name
            {
                return Ok(Some(qn));
            }
        }
    }

    // Fallback: use qualified_name of the decorator node itself as a hint.
    // Many parsers encode `TargetSymbol::@DecoratorName` in `qualified_name`.
    if let Some(qn) = &node.qualified_name {
        // Heuristic: if qualified_name contains "::" take the part before the last "::".
        if let Some(sep) = qn.rfind("::") {
            let prefix = &qn[..sep];
            if !prefix.is_empty() && prefix != node.repo {
                return Ok(Some(prefix.to_owned()));
            }
        }
    }

    Ok(None)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use gather_step_core::{NodeData, NodeKind, SourceSpan, node_id};
    use gather_step_storage::GraphStoreDb;

    use super::*;

    fn open_store(label: &str) -> (tempfile::TempDir, GraphStoreDb) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let td = tempfile::Builder::new()
            .prefix(&format!("gs-dec-{label}-{id}-"))
            .tempdir()
            .expect("tempdir");
        let db = GraphStoreDb::open(td.path().join("graph.redb")).expect("open store");
        (td, db)
    }

    fn decorator_node(repo: &str, file: &str, name: &str, line: u32) -> NodeData {
        let qn = format!("{repo}::{name}");
        NodeData {
            id: node_id(repo, file, NodeKind::Decorator, &qn),
            kind: NodeKind::Decorator,
            repo: repo.to_owned(),
            file_path: file.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(qn),
            external_id: None,
            signature: None,
            visibility: None,
            span: Some(SourceSpan { line_start: line, line_len: 1, column_start: 0, column_len: 0 }),
            is_virtual: false,
        }
    }

    fn insert_node(store: &GraphStoreDb, node: NodeData) {
        store.bulk_insert(&[node], &[]).expect("insert node");
    }

    /// Decorator added in review → appears in `added`.
    #[test]
    fn decorator_added_appears_in_added_list() {
        let (_td_b, baseline) = open_store("added-baseline");
        let (_td_r, review) = open_store("added-review");

        insert_node(&review, decorator_node("backend", "src/ctrl.ts", "Audit", 10));

        let deltas = extract_decorator_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(deltas.added.len(), 1, "expected 1 added decorator");
        assert_eq!(deltas.added[0].decorator_name, "Audit");
        assert!(deltas.removed.is_empty());
        assert!(deltas.changed.is_empty());
    }

    /// Decorator removed in review → appears in `removed`.
    #[test]
    fn decorator_removed_appears_in_removed_list() {
        let (_td_b, baseline) = open_store("removed-baseline");
        let (_td_r, review) = open_store("removed-review");

        insert_node(&baseline, decorator_node("backend", "src/ctrl.ts", "Permission", 5));

        let deltas = extract_decorator_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(deltas.removed.len(), 1, "expected 1 removed decorator");
        assert_eq!(deltas.removed[0].decorator_name, "Permission");
        assert!(deltas.added.is_empty());
        assert!(deltas.changed.is_empty());
    }

    /// Decorator with an uninteresting name is filtered out.
    #[test]
    fn uninteresting_decorator_is_filtered_out() {
        let (_td_b, baseline) = open_store("uninteresting-baseline");
        let (_td_r, review) = open_store("uninteresting-review");

        // "Component" is not on the interesting list.
        insert_node(&review, decorator_node("backend", "src/comp.ts", "Component", 1));

        let deltas = extract_decorator_deltas(&baseline, &review).expect("should succeed");

        assert!(deltas.added.is_empty(), "Component decorator must be filtered out");
        assert!(deltas.removed.is_empty());
        assert!(deltas.changed.is_empty());
    }
}
