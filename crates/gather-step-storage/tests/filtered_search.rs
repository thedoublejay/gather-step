//! Regression tests for filtered search correctness.
//!
//! These tests verify that repo/kind/language filters applied to a search
//! query return matching results even when those matches would rank below
//! a global (unfiltered) top-N window.  The fix pushes all filter conditions
//! into the Tantivy query as MUST clauses before `TopDocs` collection, so the
//! returned page is already filtered and the window is not wasted on documents
//! that would be removed post-fetch.

use std::{
    env, fs,
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use gather_step_core::{
    EdgeKind, EdgeMetadata, NodeData, NodeId, NodeKind, SourceSpan, Visibility, node_id,
};
use gather_step_storage::{
    FileBatch, GraphStore, RepoBatch, RepoBatchHooks, SearchFilters, SearchStore,
    StorageCoordinator,
};

/// Look up the repo of a search hit via the graph store, since `repo` is not
/// stored in the Tantivy document.
fn hit_repo(coord: &StorageCoordinator, node_id: NodeId) -> String {
    coord
        .graph()
        .get_node(node_id)
        .expect("graph lookup should succeed")
        .map(|n| n.repo)
        .unwrap_or_default()
}

// ── helpers ──────────────────────────────────────────────────────────────────

struct TestRoot {
    path: PathBuf,
}

impl TestRoot {
    fn new(name: &str) -> Self {
        // Counter ensures distinct paths even when multiple tests start in the
        // same nanosecond under the rayon test runner.
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be monotonic")
            .as_nanos();
        let path = env::temp_dir().join(format!(
            "gather-step-fsearch-{name}-{}-{nanos}-{seq}",
            process::id()
        ));
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn function_node(repo: &str, file_path: &str, name: &str, ordinal: u16) -> NodeData {
    NodeData {
        id: node_id(repo, file_path, NodeKind::Function, name),
        kind: NodeKind::Function,
        repo: repo.to_owned(),
        file_path: file_path.to_owned(),
        name: name.to_owned(),
        qualified_name: Some(format!("{repo}::{name}")),
        external_id: None,
        signature: Some(format!("{name}()")),
        visibility: Some(Visibility::Public),
        span: Some(SourceSpan {
            line_start: u32::from(ordinal) + 1,
            line_len: 1,
            column_start: 0,
            column_len: 0,
        }),
        is_virtual: false,
    }
}

fn route_node(repo: &str, file_path: &str, name: &str, ordinal: u16) -> NodeData {
    NodeData {
        id: node_id(repo, file_path, NodeKind::Route, name),
        kind: NodeKind::Route,
        repo: repo.to_owned(),
        file_path: file_path.to_owned(),
        name: name.to_owned(),
        qualified_name: Some(format!("{repo}::routes::{name}")),
        external_id: Some(format!("route::{name}")),
        signature: None,
        visibility: Some(Visibility::Public),
        span: Some(SourceSpan {
            line_start: u32::from(ordinal) + 1,
            line_len: 1,
            column_start: 0,
            column_len: 0,
        }),
        is_virtual: false,
    }
}

fn file_node(repo: &str, file_path: &str) -> NodeData {
    NodeData {
        id: node_id(repo, file_path, NodeKind::File, file_path),
        kind: NodeKind::File,
        repo: repo.to_owned(),
        file_path: file_path.to_owned(),
        name: file_path.to_owned(),
        qualified_name: None,
        external_id: None,
        signature: None,
        visibility: None,
        span: Some(SourceSpan {
            line_start: 1,
            line_len: 0,
            column_start: 0,
            column_len: 0,
        }),
        is_virtual: false,
    }
}

fn defines_edge(file_id: NodeId, symbol_id: NodeId) -> gather_step_core::EdgeData {
    gather_step_core::EdgeData {
        source: file_id,
        target: symbol_id,
        kind: EdgeKind::Defines,
        metadata: EdgeMetadata::default(),
        owner_file: file_id,
        is_cross_file: false,
    }
}

/// Build a `RepoBatch` with `count` function nodes whose names are all
/// `target_name`.  The high symbol count means these documents accumulate
/// more BM25 weight and rank higher in an unfiltered search.
fn dominant_repo_batch(repo: &str, file_path: &str, target_name: &str, count: u16) -> RepoBatch {
    let file = file_node(repo, file_path);
    let mut nodes = vec![file.clone()];
    let mut edges = Vec::new();
    for ordinal in 0..count {
        let sym = function_node(repo, file_path, target_name, ordinal);
        edges.push(defines_edge(file.id, sym.id));
        nodes.push(sym);
    }
    RepoBatch {
        repo: repo.to_owned(),
        files: vec![FileBatch {
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            path_id_bytes: vec![],
            nodes,
            edges,
            content_hash: vec![0xDD, 0xAA],
            size_bytes: 0,
            mtime_ns: 0,
            indexed_at: 1_713_000_000,
            parse_ms: Some(5),
            force: false,
        }],
        test_hooks: RepoBatchHooks::default(),
    }
}

/// Build a `RepoBatch` with a single function node.
fn single_node_batch(repo: &str, file_path: &str, name: &str, hash: u8) -> RepoBatch {
    let file = file_node(repo, file_path);
    let sym = function_node(repo, file_path, name, 0);
    let edge = defines_edge(file.id, sym.id);
    RepoBatch {
        repo: repo.to_owned(),
        files: vec![FileBatch {
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            path_id_bytes: vec![],
            nodes: vec![file, sym],
            edges: vec![edge],
            content_hash: vec![hash],
            size_bytes: 0,
            mtime_ns: 0,
            indexed_at: 1_713_000_000,
            parse_ms: Some(5),
            force: false,
        }],
        test_hooks: RepoBatchHooks::default(),
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// Repo filter must return results from the target repo even when the target
/// repo's matches rank below the global top-N collected without a filter.
///
/// Setup: `dominant_repo` has many functions named `handleRequest` — they
/// dominate the global BM25 ranking.  `target_repo` has a single function
/// also named `handleRequest`, but with a much lower BM25 score because it
/// appears only once.
///
/// Without the Tantivy-level MUST clause the target repo's document would
/// fall outside any small global window and the filtered search would return
/// zero results.  With the MUST clause Tantivy scores only documents in
/// `target_repo` and returns the hit correctly.
#[test]
fn filtered_repo_search_returns_low_ranked_matches() {
    let root = TestRoot::new("repo-filter-low-rank");
    let coord = StorageCoordinator::open(root.path()).expect("coordinator should open");

    // dominant_repo floods the index with many `handleRequest` symbols so
    // that those documents claim all the top slots in a global query.
    coord
        .index_repo_batch(&dominant_repo_batch(
            "dominant_repo",
            "src/handler.ts",
            "handleRequest",
            // Enough duplicates to ensure the target repo's single document
            // would rank below a window of any reasonable size.
            200,
        ))
        .expect("dominant batch should index");

    // target_repo has exactly one `handleRequest` symbol.
    coord
        .index_repo_batch(&single_node_batch(
            "target_repo",
            "src/server.ts",
            "handleRequest",
            0xAB,
        ))
        .expect("target batch should index");

    // Unfiltered search: dominant_repo's documents dominate.
    let global_hits = coord
        .search()
        .search("handleRequest", 10)
        .expect("global search should succeed");
    let global_repos: Vec<_> = global_hits
        .iter()
        .map(|h| hit_repo(&coord, h.node_id))
        .collect();
    // The global top-10 are overwhelmingly from dominant_repo.
    assert!(
        global_repos.iter().any(|r| r == "dominant_repo"),
        "dominant_repo should appear in global top-10; got: {global_repos:?}"
    );

    // Filtered search for target_repo must find its symbol regardless of
    // global ranking.
    let filtered_hits = coord
        .search()
        .search_filtered(
            "handleRequest",
            10,
            SearchFilters {
                repo: Some("target_repo"),
                node_kind: None,
                lang: None,
            },
        )
        .expect("filtered search should succeed");

    assert!(
        !filtered_hits.is_empty(),
        "filtered search for target_repo must return at least one result"
    );
    let filtered_repos: Vec<_> = filtered_hits
        .iter()
        .map(|h| hit_repo(&coord, h.node_id))
        .collect();
    assert!(
        filtered_repos.iter().all(|r| r == "target_repo"),
        "every filtered hit must belong to target_repo; got: {filtered_repos:?}"
    );
    assert_eq!(
        filtered_hits[0].symbol_name, "handleRequest",
        "filtered hit must be the target symbol"
    );
}

/// Kind filter must return only results of the requested `NodeKind` even
/// when higher-scoring documents of a different kind share the same name.
///
/// Setup: both `Function` and `Route` nodes named `handleRequest` are indexed.
/// The function nodes are ranked higher in a global search.  A kind-filtered
/// search for `Route` must still return the route node.
#[test]
fn filtered_kind_search_returns_correct_kind() {
    let root = TestRoot::new("kind-filter");
    let coord = StorageCoordinator::open(root.path()).expect("coordinator should open");

    // Index a batch of Function nodes with the query name to dominate ranking.
    coord
        .index_repo_batch(&dominant_repo_batch(
            "svc_a",
            "src/handler.ts",
            "handleRequest",
            50,
        ))
        .expect("function batch should index");

    // Also index a Route node with the same name.
    let file = file_node("svc_a", "src/routes.ts");
    let route = route_node("svc_a", "src/routes.ts", "handleRequest", 0);
    let edge = defines_edge(file.id, route.id);
    let route_batch = RepoBatch {
        repo: "svc_a".to_owned(),
        files: vec![FileBatch {
            repo: "svc_a".to_owned(),
            file_path: "src/routes.ts".to_owned(),
            path_id_bytes: vec![],
            nodes: vec![file, route],
            edges: vec![edge],
            content_hash: vec![0xCC],
            size_bytes: 0,
            mtime_ns: 0,
            indexed_at: 1_713_000_000,
            parse_ms: Some(3),
            force: false,
        }],
        test_hooks: RepoBatchHooks::default(),
    };
    coord
        .index_repo_batch(&route_batch)
        .expect("route batch should index");

    let filtered_hits = coord
        .search()
        .search_filtered(
            "handleRequest",
            10,
            SearchFilters {
                repo: None,
                node_kind: Some(NodeKind::Route),
                lang: None,
            },
        )
        .expect("kind-filtered search should succeed");

    assert!(
        !filtered_hits.is_empty(),
        "kind-filtered search must return at least one Route hit"
    );
    assert!(
        filtered_hits.iter().all(|h| h.node_kind == NodeKind::Route),
        "every filtered hit must be a Route; got: {:?}",
        filtered_hits
            .iter()
            .map(|h| h.node_kind)
            .collect::<Vec<_>>()
    );
}

/// Combined repo + kind filter must work together: only documents that satisfy
/// both constraints are returned.
#[test]
fn combined_repo_and_kind_filter_returns_correct_results() {
    let root = TestRoot::new("repo-kind-combined");
    let coord = StorageCoordinator::open(root.path()).expect("coordinator should open");

    // svc_a has many Function nodes named `processEvent` — high global rank.
    coord
        .index_repo_batch(&dominant_repo_batch(
            "svc_a",
            "src/processor.ts",
            "processEvent",
            100,
        ))
        .expect("svc_a function batch should index");

    // svc_b has a single Route node named `processEvent` — low global rank.
    let file = file_node("svc_b", "src/routes.ts");
    let route = route_node("svc_b", "src/routes.ts", "processEvent", 0);
    let edge = defines_edge(file.id, route.id);
    let svc_b_batch = RepoBatch {
        repo: "svc_b".to_owned(),
        files: vec![FileBatch {
            repo: "svc_b".to_owned(),
            file_path: "src/routes.ts".to_owned(),
            path_id_bytes: vec![],
            nodes: vec![file, route],
            edges: vec![edge],
            content_hash: vec![0xEE],
            size_bytes: 0,
            mtime_ns: 0,
            indexed_at: 1_713_000_000,
            parse_ms: Some(3),
            force: false,
        }],
        test_hooks: RepoBatchHooks::default(),
    };
    coord
        .index_repo_batch(&svc_b_batch)
        .expect("svc_b route batch should index");

    let hits = coord
        .search()
        .search_filtered(
            "processEvent",
            20,
            SearchFilters {
                repo: Some("svc_b"),
                node_kind: Some(NodeKind::Route),
                lang: None,
            },
        )
        .expect("combined filter search should succeed");

    assert!(
        !hits.is_empty(),
        "combined filter (repo=svc_b, kind=route) must return at least one result"
    );
    for hit in &hits {
        assert_eq!(
            hit_repo(&coord, hit.node_id),
            "svc_b",
            "hit repo must be svc_b"
        );
        assert_eq!(hit.node_kind, NodeKind::Route, "hit kind must be Route");
    }
}

/// Unfiltered search must continue to return results from all repos and all
/// kinds — the filter-push change must not regress the no-filter path.
#[test]
fn unfiltered_search_returns_results_from_all_repos() {
    let root = TestRoot::new("unfiltered");
    let coord = StorageCoordinator::open(root.path()).expect("coordinator should open");

    for (repo, hash) in [("svc_a", 0x01_u8), ("svc_b", 0x02), ("svc_c", 0x03)] {
        coord
            .index_repo_batch(&single_node_batch(
                repo,
                "src/shared.ts",
                "sharedHelper",
                hash,
            ))
            .expect("batch should index");
    }

    let hits = coord
        .search()
        .search("sharedHelper", 20)
        .expect("unfiltered search should succeed");

    // All three repos must appear somewhere in the results.
    let repos: std::collections::BTreeSet<_> =
        hits.iter().map(|h| hit_repo(&coord, h.node_id)).collect();
    for expected in ["svc_a", "svc_b", "svc_c"] {
        assert!(
            repos.iter().any(|r| r == expected),
            "unfiltered search must return results from {expected}; got: {repos:?}"
        );
    }
}
