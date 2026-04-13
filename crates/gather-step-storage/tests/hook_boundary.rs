// Probe: cross-repo hook import produces `ConsumesHookFrom` edges and persists
// through the storage layer.
//
// Test name:
//   `cross_repo_hook_import_produces_consumes_hook_from_edge`
//
// This probe operates at the storage layer, seeding the graph directly with the
// nodes and edges that the parser's `FrontendHooks` augmenter would emit, then
// asserting the storage layer handles them correctly.
//
// Result: determined at runtime — see assertion messages for the verdict.

use std::{
    env,
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use gather_step_core::{
    EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeId, NodeKind, Visibility, node_id, ref_node_id,
};
use gather_step_storage::{GraphStore, GraphStoreDb};

// ---------------------------------------------------------------------------
// Helpers (mirrors those in proof_builder.rs tests)
// ---------------------------------------------------------------------------

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDb {
    path: PathBuf,
}

impl TempDb {
    fn new(name: &str) -> Self {
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = env::temp_dir().join(format!(
            "gather-step-hook-boundary-{name}-{}-{nanos}-{seq}.redb",
            process::id()
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

fn open_store(name: &str) -> (GraphStoreDb, TempDb) {
    let db = TempDb::new(name);
    let store = GraphStoreDb::open(db.path()).expect("store should open");
    (store, db)
}

fn file_node(repo: &str, path: &str) -> NodeData {
    NodeData {
        id: node_id(repo, path, NodeKind::File, path),
        kind: NodeKind::File,
        repo: repo.to_owned(),
        file_path: path.to_owned(),
        name: path.to_owned(),
        qualified_name: Some(format!("{repo}::{path}")),
        external_id: None,
        signature: None,
        visibility: Some(Visibility::Public),
        span: None,
        is_virtual: false,
    }
}

fn sym_node(repo: &str, path: &str, kind: NodeKind, name: &str) -> NodeData {
    NodeData {
        id: node_id(repo, path, kind, name),
        kind,
        repo: repo.to_owned(),
        file_path: path.to_owned(),
        name: name.to_owned(),
        qualified_name: Some(format!("{repo}::{name}")),
        external_id: None,
        signature: None,
        visibility: Some(Visibility::Public),
        span: None,
        is_virtual: false,
    }
}

/// Build the virtual `SharedSymbol` hook node that the `FrontendHooks`
/// augmenter emits for a cross-package hook import.
///
/// The qualified name follows the `__hook__<package>::<symbol>` pattern used
/// by the parser's `add_hook_consumer_edges` function.
fn hook_virtual_node(package: &str, hook_name: &str, consumer_file: &NodeData) -> NodeData {
    let qn = format!("__hook__{package}::{hook_name}");
    NodeData {
        id: ref_node_id(NodeKind::SharedSymbol, &qn),
        kind: NodeKind::SharedSymbol,
        // The parser records hook virtual nodes at the importing file location.
        repo: consumer_file.repo.clone(),
        file_path: consumer_file.file_path.clone(),
        name: hook_name.to_owned(),
        qualified_name: Some(qn.clone()),
        external_id: Some(qn),
        signature: None,
        visibility: None,
        span: None,
        is_virtual: true,
    }
}

fn make_edge(source: NodeId, target: NodeId, kind: EdgeKind, owner: NodeId) -> EdgeData {
    EdgeData {
        source,
        target,
        kind,
        metadata: EdgeMetadata {
            weight: None,
            confidence: Some(800),
            timestamp_unix: None,
            drift_kind: None,
            resolver: Some("frontend_hook_import".to_owned()),
        },
        owner_file: owner,
        is_cross_file: true,
    }
}

// ---------------------------------------------------------------------------
// Probe 2.4
// ---------------------------------------------------------------------------

/// Verify that a cross-package hook import produces a `ConsumesHookFrom` edge
/// that:
///
/// 1. Persists correctly through the graph store.
/// 2. Can be retrieved via `get_outgoing` from the consumer file node.
/// 3. Does NOT appear for a same-package relative import (regular `Imports` only).
/// 4. Does NOT appear for a non-hook cross-package import.
///
#[test]
fn cross_repo_hook_import_produces_consumes_hook_from_edge() {
    let (store, _db) = open_store("hook-boundary-probe");

    // ── Graph nodes ──────────────────────────────────────────────────────────

    // Hook export in a shared library repo.
    let hook_file = file_node("frontend_lib_repo", "src/hooks/use_foo_contract.ts");
    let hook_export = sym_node(
        "frontend_lib_repo",
        "src/hooks/use_foo_contract.ts",
        NodeKind::Function,
        "useFooContract",
    );

    // Cross-repo consumer.
    let consumer_file = file_node("frontend_consumer_repo", "src/page.tsx");
    let consumer_sym = sym_node(
        "frontend_consumer_repo",
        "src/page.tsx",
        NodeKind::Function,
        "PageComponent",
    );

    // Virtual node representing the hook (emitted by the parser augmenter).
    let hook_virtual = hook_virtual_node(
        "@workspace/frontend-shared",
        "useFooContract",
        &consumer_file,
    );

    // Same-repo helper hook (same-package — must NOT produce ConsumesHookFrom).
    let helper_file = file_node("frontend_lib_repo", "src/hooks/use_bar_helper.ts");
    let helper_hook = sym_node(
        "frontend_lib_repo",
        "src/hooks/use_bar_helper.ts",
        NodeKind::Function,
        "useBarHelper",
    );

    // Non-hook cross-package export (no `use` prefix — must NOT produce ConsumesHookFrom).
    let util_file = file_node("frontend_lib_repo", "src/utils/get_foo_data.ts");
    let util_fn = sym_node(
        "frontend_lib_repo",
        "src/utils/get_foo_data.ts",
        NodeKind::Function,
        "getFooData",
    );
    let util_virtual = {
        let qn = "__shared_get_foo_data";
        NodeData {
            id: ref_node_id(NodeKind::SharedSymbol, qn),
            kind: NodeKind::SharedSymbol,
            repo: "frontend_lib_repo".to_owned(),
            file_path: "src/utils/get_foo_data.ts".to_owned(),
            name: "getFooData".to_owned(),
            qualified_name: Some(qn.to_owned()),
            external_id: Some(qn.to_owned()),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        }
    };

    // A third file that imports within the same repo via a relative path.
    let same_repo_consumer_file = file_node("frontend_lib_repo", "src/composite/page.tsx");

    store
        .bulk_insert(
            &[
                hook_file.clone(),
                hook_export.clone(),
                hook_virtual.clone(),
                consumer_file.clone(),
                consumer_sym.clone(),
                helper_file.clone(),
                helper_hook.clone(),
                same_repo_consumer_file.clone(),
                util_file.clone(),
                util_fn.clone(),
                util_virtual.clone(),
            ],
            &[
                // Cross-repo hook import → ConsumesHookFrom
                make_edge(
                    consumer_file.id,
                    hook_virtual.id,
                    EdgeKind::ConsumesHookFrom,
                    consumer_file.id,
                ),
                // Same-package hook usage (relative import) → regular Imports only
                make_edge(
                    same_repo_consumer_file.id,
                    helper_hook.id,
                    EdgeKind::Imports,
                    same_repo_consumer_file.id,
                ),
                // Non-hook cross-package import → regular Imports only
                make_edge(
                    consumer_file.id,
                    util_virtual.id,
                    EdgeKind::Imports,
                    consumer_file.id,
                ),
            ],
        )
        .expect("bulk insert must succeed");

    // ── Assert 1: ConsumesHookFrom edge persists and is retrievable ───────────

    let consumer_outgoing = store
        .get_outgoing(consumer_file.id)
        .expect("get_outgoing must succeed");

    let hook_edge = consumer_outgoing
        .iter()
        .find(|e| e.kind == EdgeKind::ConsumesHookFrom && e.target == hook_virtual.id);
    assert!(
        hook_edge.is_some(),
        "a ConsumesHookFrom edge from the consumer file to the hook virtual node \
         must persist in the store; outgoing: {consumer_outgoing:#?}"
    );

    // ── Assert 2: same-package import produces only Imports, no ConsumesHookFrom

    let same_repo_outgoing = store
        .get_outgoing(same_repo_consumer_file.id)
        .expect("get_outgoing must succeed");

    let unexpected_hook_edge = same_repo_outgoing
        .iter()
        .find(|e| e.kind == EdgeKind::ConsumesHookFrom);
    assert!(
        unexpected_hook_edge.is_none(),
        "same-package (relative) import must not produce a ConsumesHookFrom edge; \
         outgoing: {same_repo_outgoing:#?}"
    );

    // ── Assert 3: non-hook cross-package import produces only Imports ─────────

    let non_hook_hook_edges: Vec<_> = consumer_outgoing
        .iter()
        .filter(|e| e.kind == EdgeKind::ConsumesHookFrom && e.target == util_virtual.id)
        .collect();
    assert!(
        non_hook_hook_edges.is_empty(),
        "non-hook export import must not produce a ConsumesHookFrom edge; \
         relevant outgoing: {non_hook_hook_edges:#?}"
    );

    // ── Assert 4: ConsumesHookFrom is an is_semantic_bridge edge kind ─────────
    //
    // The schema marks ConsumesHookFrom as a semantic bridge edge. Verify this
    // invariant holds at the schema level.
    assert!(
        EdgeKind::ConsumesHookFrom.is_semantic_bridge(),
        "ConsumesHookFrom must be classified as a semantic bridge edge so the \
         analysis layers can distinguish it from a plain import"
    );

    // ── Assert 5: ConsumesHookFrom edge can be queried by kind ───────────────

    let count_by_kind = store
        .count_edges_by_kind(EdgeKind::ConsumesHookFrom)
        .expect("count_edges_by_kind must succeed");
    assert_eq!(
        count_by_kind, 1,
        "exactly one ConsumesHookFrom edge must be stored; got {count_by_kind}"
    );

    // ── Assert 6: incoming query on the hook virtual node finds the consumer ──

    let hook_incoming = store
        .get_incoming(hook_virtual.id)
        .expect("get_incoming must succeed");
    let incoming_hook_edge = hook_incoming
        .iter()
        .find(|e| e.kind == EdgeKind::ConsumesHookFrom);
    assert!(
        incoming_hook_edge.is_some(),
        "hook virtual node must have an incoming ConsumesHookFrom edge; \
         incoming: {hook_incoming:#?}"
    );
    if let Some(e) = incoming_hook_edge {
        assert_eq!(
            e.source, consumer_file.id,
            "ConsumesHookFrom source must be the consumer file node"
        );
    }
}
