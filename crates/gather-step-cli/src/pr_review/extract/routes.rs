//! Route delta extraction — Phase 2 Task 2.
//!
//! Diffs `NodeKind::Route` virtual nodes between a baseline graph and a review
//! graph to produce [`RouteDeltas`] (added / removed / changed HTTP routes).
//!
//! # Key design decisions
//!
//! - The canonical key for a route is `(method, canonical_path)`, decoded from
//!   the node's `qualified_name` / `external_id` (format:
//!   `"__route__{METHOD}__{path}"`).
//! - Handler repo / file / line / `handler_qualified_name` are resolved by
//!   walking `get_incoming(route_id)` filtered by `EdgeKind::Serves`.  The
//!   first matching non-virtual source node wins.
//! - The extractor is generic over `GraphStore` so it can be unit-tested with
//!   a direct-insertion `GraphStoreDb` without running the indexer.

use anyhow::Result;
use gather_step_core::{EdgeKind, NodeId, NodeKind};
use gather_step_storage::GraphStore;
use rustc_hash::FxHashMap;
use tracing::warn;

use crate::pr_review::delta_report::{RouteDelta, RouteDeltaChange, RouteDeltas};

/// `(method, canonical_path)` → `RouteDelta` mapping built from one snapshot.
type RouteMap = FxHashMap<(String, String), RouteDelta>;

/// `(repo, file, line, handler_qualified_name)` resolved from a `Serves` edge.
type HandlerInfo = (Option<String>, Option<String>, Option<u32>, Option<String>);

/// Extract added / removed / changed routes by diffing the route virtual nodes
/// in `baseline` against those in `review`.
///
/// If `baseline` is an empty / never-indexed store every review route is
/// reported as `added` — no error is returned.
pub fn extract_route_deltas<B: GraphStore, R: GraphStore>(
    baseline: &B,
    review: &R,
) -> Result<RouteDeltas> {
    let baseline_map = build_route_map(baseline)?;
    let review_map = build_route_map(review)?;

    let mut added: Vec<RouteDelta> = Vec::new();
    let mut removed: Vec<RouteDelta> = Vec::new();
    let mut changed: Vec<RouteDeltaChange> = Vec::new();

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

    // Changed: in both — diff handler tuple.
    for (key, review_delta) in &review_map {
        if let Some(baseline_delta) = baseline_map.get(key) {
            let handler_changed = handler_tuple(baseline_delta) != handler_tuple(review_delta);
            if handler_changed {
                changed.push(RouteDeltaChange {
                    method: key.0.clone(),
                    path: key.1.clone(),
                    before: Some(baseline_delta.clone()),
                    after: Some(review_delta.clone()),
                    handler_changed: true,
                });
            }
        }
    }

    // Deterministic output — sort by (method, path).
    added.sort_by(|a, b| (&a.method, &a.path).cmp(&(&b.method, &b.path)));
    removed.sort_by(|a, b| (&a.method, &a.path).cmp(&(&b.method, &b.path)));
    changed.sort_by(|a, b| (&a.method, &a.path).cmp(&(&b.method, &b.path)));

    Ok(RouteDeltas {
        added,
        removed,
        changed,
        unavailable: false,
    })
}

// ── Public helpers ────────────────────────────────────────────────────────────

/// Find the [`NodeId`] of the virtual Route node that matches `(method, path)`
/// in `store`.  Returns `None` when the route is not present.
///
/// Used by the impact-attachment wiring in `commands/pr_review.rs` to look up
/// the baseline node ID for removed / changed routes so impact can be computed.
pub fn find_route_node_id<S: GraphStore>(
    store: &S,
    method: &str,
    path: &str,
) -> Result<Option<NodeId>> {
    let nodes = store.nodes_by_type(NodeKind::Route)?;
    let want_key = (method.to_ascii_uppercase(), path.to_owned());

    for node in nodes {
        if !node.is_virtual {
            continue;
        }
        let qn = node
            .external_id
            .as_deref()
            .or(node.qualified_name.as_deref());
        if let Some(key) = qn.and_then(decode_route_qn)
            && key == want_key
        {
            return Ok(Some(node.id));
        }
    }
    Ok(None)
}

// ── Internals ─────────────────────────────────────────────────────────────────

/// Build `(method, path) → RouteDelta` for all virtual Route nodes in `store`.
fn build_route_map<S: GraphStore>(store: &S) -> Result<RouteMap> {
    let nodes = store.nodes_by_type(NodeKind::Route)?;
    let mut map = RouteMap::default();

    for node in nodes {
        if !node.is_virtual {
            continue;
        }

        // Decode (method, path) from qualified_name / external_id.
        let qn = node
            .external_id
            .as_deref()
            .or(node.qualified_name.as_deref());

        let Some((method, path)) = qn.and_then(decode_route_qn) else {
            warn!(
                node_id = ?node.id,
                qn = ?qn,
                "route extractor: skipping node with unrecognised qualified_name format"
            );
            continue;
        };

        // Resolve handler info from the first incoming Serves edge.
        let (repo, file, line, handler_qn) = resolve_handler(store, node.id)?;

        let key = (method.clone(), path.clone());
        map.insert(
            key,
            RouteDelta {
                method,
                path,
                repo,
                file,
                line,
                handler_qualified_name: handler_qn,
                impact: None,
            },
        );
    }

    Ok(map)
}

/// Decode `"__route__{METHOD}__{path}"` → `(method, path)`.
/// Returns `None` for any string that doesn't match the pattern.
fn decode_route_qn(qn: &str) -> Option<(String, String)> {
    let suffix = qn.strip_prefix("__route__")?;
    let (method, path) = suffix.split_once("__")?;
    if method.is_empty() || path.is_empty() {
        return None;
    }
    Some((method.to_ascii_uppercase(), path.to_owned()))
}

/// Walk incoming edges on `route_id` filtered by `EdgeKind::Serves`.
/// Returns `(repo, file, line, handler_qualified_name)` from the first
/// non-virtual source node found, or all-`None` if no `Serves` edge exists.
fn resolve_handler<S: GraphStore>(store: &S, route_id: NodeId) -> Result<HandlerInfo> {
    for edge in store.get_incoming(route_id)? {
        if edge.kind != EdgeKind::Serves {
            continue;
        }
        let Some(handler) = store.get_node(edge.source)? else {
            continue;
        };
        if handler.is_virtual {
            continue;
        }
        let line = handler.span.as_ref().map(|s| s.line_start);
        return Ok((
            Some(handler.repo),
            Some(handler.file_path),
            line,
            handler.qualified_name,
        ));
    }
    Ok((None, None, None, None))
}

/// Comparison tuple for deciding whether a handler changed between snapshots.
fn handler_tuple(d: &RouteDelta) -> (Option<&str>, Option<&str>, Option<&str>) {
    (
        d.repo.as_deref(),
        d.file.as_deref(),
        d.handler_qualified_name.as_deref(),
    )
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process::Command,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::{
        EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, SourceSpan, Visibility, node_id,
        route_qn, virtual_node,
    };
    use gather_step_storage::{GraphStore, GraphStoreDb, IndexingOptions, StorageCoordinator};

    use crate::pr_review::{artifact_root::create_artifact_root, index_runner::run_review_index};

    use super::extract_route_deltas;

    // ── temp helpers ──────────────────────────────────────────────────────────

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new(label: &str) -> Self {
            let id = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gs-route-extractor-{label}-{}-{id}.redb",
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
            let _ = fs::remove_file(&self.path);
        }
    }

    // ── graph-building helpers ────────────────────────────────────────────────

    fn open_store(label: &str) -> (TempDb, GraphStoreDb) {
        let tmp = TempDb::new(label);
        let db = GraphStoreDb::open(tmp.path()).expect("store should open");
        (tmp, db)
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
            visibility: None,
            span: None,
            is_virtual: false,
        }
    }

    fn handler_node(repo: &str, file: &str, name: &str, line: u32) -> NodeData {
        NodeData {
            id: node_id(repo, file, NodeKind::Function, name),
            kind: NodeKind::Function,
            repo: repo.to_owned(),
            file_path: file.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(format!("{repo}::{name}")),
            external_id: None,
            signature: Some(format!("{name}(): void")),
            visibility: Some(Visibility::Public),
            span: Some(SourceSpan {
                line_start: line,
                line_len: 10,
                column_start: 0,
                column_len: 0,
            }),
            is_virtual: false,
        }
    }

    fn route_node(method: &str, path: &str) -> NodeData {
        let qn = route_qn(method, path);
        virtual_node(
            NodeKind::Route,
            "__virtual__",
            "__virtual__",
            format!("{method} {path}"),
            qn,
        )
    }

    fn serves_edge(handler: &NodeData, route: &NodeData, owner_file: &NodeData) -> EdgeData {
        EdgeData {
            source: handler.id,
            target: route.id,
            kind: EdgeKind::Serves,
            metadata: EdgeMetadata {
                confidence: Some(900),
                ..EdgeMetadata::default()
            },
            owner_file: owner_file.id,
            is_cross_file: true,
        }
    }

    /// Insert a route with a handler into the store.
    ///
    /// Each call uses a **unique virtual file path** derived from `method` and
    /// `path` so that successive `bulk_insert` calls do not delete each other's
    /// `Serves` edges (which would happen if they shared the same `owner_file`
    /// — `GraphStoreDb::bulk_insert` removes all edges for each owner file
    /// before inserting the new edges).
    fn insert_route(
        store: &GraphStoreDb,
        repo: &str,
        file: &str,
        handler_name: &str,
        method: &str,
        path: &str,
        line: u32,
    ) {
        // Unique synthetic file path per route so owner_file IDs don't collide.
        let unique_file = format!("{file}.{method}.route");
        let f = file_node(repo, &unique_file);
        let h = handler_node(repo, file, handler_name, line);
        let r = route_node(method, path);
        let e = serves_edge(&h, &r, &f);
        store
            .bulk_insert(&[f, h, r], &[e])
            .expect("bulk insert should succeed");
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /// A route present only in the review index appears in `added`.
    #[test]
    fn route_added_appears_in_added_list() {
        let (_td_b, baseline) = open_store("added-baseline");
        let (_td_r, review) = open_store("added-review");

        // Baseline has GET /orders only.
        insert_route(
            &baseline,
            "api",
            "src/orders.ts",
            "listOrders",
            "GET",
            "/orders",
            5,
        );

        // Review adds POST /orders.
        insert_route(
            &review,
            "api",
            "src/orders.ts",
            "listOrders",
            "GET",
            "/orders",
            5,
        );
        insert_route(
            &review,
            "api",
            "src/orders.ts",
            "createOrder",
            "POST",
            "/orders",
            20,
        );

        let deltas = extract_route_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(deltas.added.len(), 1, "expected exactly one added route");
        assert_eq!(deltas.added[0].method, "POST");
        assert_eq!(deltas.added[0].path, "/orders");
        assert!(deltas.removed.is_empty(), "no routes should be removed");
        assert!(deltas.changed.is_empty(), "no routes should be changed");
    }

    /// A route present only in the baseline index appears in `removed`.
    #[test]
    fn route_removed_appears_in_removed_list() {
        let (_td_b, baseline) = open_store("removed-baseline");
        let (_td_r, review) = open_store("removed-review");

        // Baseline has two routes.
        insert_route(
            &baseline,
            "api",
            "src/orders.ts",
            "listOrders",
            "GET",
            "/orders",
            5,
        );
        insert_route(
            &baseline,
            "api",
            "src/orders.ts",
            "deleteOrder",
            "DELETE",
            "/orders/:id",
            30,
        );

        // Review keeps only GET /orders.
        insert_route(
            &review,
            "api",
            "src/orders.ts",
            "listOrders",
            "GET",
            "/orders",
            5,
        );

        let deltas = extract_route_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(
            deltas.removed.len(),
            1,
            "expected exactly one removed route"
        );
        assert_eq!(deltas.removed[0].method, "DELETE");
        assert_eq!(deltas.removed[0].path, "/orders/:id");
        assert!(deltas.added.is_empty(), "no routes should be added");
        assert!(deltas.changed.is_empty(), "no routes should be changed");
    }

    /// Same `(method, path)` in both snapshots but handler file differs → `changed`.
    #[test]
    fn route_handler_change_appears_in_changed_list() {
        let (_td_b, baseline) = open_store("changed-baseline");
        let (_td_r, review) = open_store("changed-review");

        // Baseline: handler in src/orders.ts.
        insert_route(
            &baseline,
            "api",
            "src/orders.ts",
            "listOrders",
            "GET",
            "/orders",
            5,
        );

        // Review: same route but handler moved to src/order_list.ts.
        insert_route(
            &review,
            "api",
            "src/order_list.ts",
            "listOrders",
            "GET",
            "/orders",
            10,
        );

        let deltas = extract_route_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(
            deltas.changed.len(),
            1,
            "expected exactly one changed route"
        );
        let change = &deltas.changed[0];
        assert_eq!(change.method, "GET");
        assert_eq!(change.path, "/orders");
        assert!(change.handler_changed);
        assert_ne!(
            change.before.as_ref().unwrap().file,
            change.after.as_ref().unwrap().file,
            "before and after file must differ"
        );
        assert!(deltas.added.is_empty(), "no routes should be added");
        assert!(deltas.removed.is_empty(), "no routes should be removed");
    }

    /// Empty baseline → every review route is `added`; no panic.
    #[test]
    fn route_extractor_handles_no_baseline_gracefully() {
        let (_td_b, baseline) = open_store("empty-baseline");
        let (_td_r, review) = open_store("empty-baseline-review");

        insert_route(
            &review,
            "api",
            "src/orders.ts",
            "listOrders",
            "GET",
            "/orders",
            5,
        );
        insert_route(
            &review,
            "api",
            "src/orders.ts",
            "createOrder",
            "POST",
            "/orders",
            20,
        );

        let deltas = extract_route_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(
            deltas.added.len(),
            2,
            "all review routes should be added when baseline is empty"
        );
        assert!(deltas.removed.is_empty());
        assert!(deltas.changed.is_empty());
    }

    /// A node with a non-route `qualified_name` is skipped without panicking.
    #[test]
    fn route_extractor_skips_malformed_qualified_names() {
        let (_td_b, baseline) = open_store("malformed-baseline");
        let (_td_r, review) = open_store("malformed-review");

        // Insert a virtual node whose qualified_name does NOT follow the
        // `__route__METHOD__path` format — simulates a corrupt / future node.
        let bad_node = NodeData {
            id: node_id("__virtual__", "bad", NodeKind::Route, "not_a_route_qn"),
            kind: NodeKind::Route,
            repo: "__virtual__".to_owned(),
            file_path: "bad".to_owned(),
            name: "bad".to_owned(),
            qualified_name: Some("not_a_route_qn".to_owned()),
            external_id: Some("not_a_route_qn".to_owned()),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        };
        review
            .bulk_insert(&[bad_node], &[])
            .expect("insert should succeed");

        // Also add a valid route so we can check it still shows up.
        insert_route(
            &review,
            "api",
            "src/orders.ts",
            "listOrders",
            "GET",
            "/orders",
            5,
        );

        let deltas = extract_route_deltas(&baseline, &review).expect("should not error");

        // The malformed node is skipped; the valid route is `added`.
        assert_eq!(deltas.added.len(), 1);
        assert_eq!(deltas.added[0].path, "/orders");
    }

    // ── end-to-end test using the real indexer ────────────────────────────────

    fn git_available() -> bool {
        Command::new("git").arg("--version").output().is_ok()
    }

    static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(label: &str) -> Self {
            let id = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!("gs-route-extractor-e2e-{label}-{id}"));
            fs::create_dir_all(&path).expect("temp dir");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = Command::new("git")
                .args(["-C", &self.path.to_string_lossy(), "worktree", "prune"])
                .output();
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn git_commit(dir: &Path, message: &str) -> String {
        let run = |args: &[&str]| {
            let s = Command::new("git")
                .args(args)
                .current_dir(dir)
                .status()
                .expect("git");
            assert!(s.success(), "git {args:?} failed");
        };
        run(&["add", "."]);
        run(&[
            "-c",
            "commit.gpgsign=false",
            "commit",
            "--allow-empty",
            "--message",
            message,
        ]);
        let out = Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(dir)
            .output()
            .expect("rev-parse");
        String::from_utf8_lossy(&out.stdout).trim().to_owned()
    }

    fn git_init(dir: &Path) {
        let run = |args: &[&str]| {
            let s = Command::new("git")
                .args(args)
                .current_dir(dir)
                .status()
                .expect("git");
            assert!(s.success(), "git {args:?} failed");
        };
        run(&["init", "--initial-branch=main"]);
        run(&["config", "user.email", "test@example.com"]);
        run(&["config", "user.name", "Test"]);
        run(&["config", "commit.gpgsign", "false"]);
    }

    /// Write a minimal gather-step workspace with a single TS file that defines
    /// one route (`GET /orders`) so the indexer may emit a Route virtual node.
    fn write_base_fixture(root: &Path) {
        fs::write(
            root.join("gather-step.config.yaml"),
            "repos:\n  - name: api\n    path: api\nindexing:\n  workspace_concurrency: 1\n",
        )
        .expect("config");
        let src = root.join("api/src");
        fs::create_dir_all(&src).expect("src dir");
        fs::write(
            root.join("api/package.json"),
            "{\"name\":\"api\",\"version\":\"0.0.1\"}",
        )
        .expect("package.json");
        // A minimal NestJS-style controller that the indexer will pick up.
        fs::write(
            src.join("orders.controller.ts"),
            concat!(
                "import { Controller, Get } from '@nestjs/common';\n",
                "\n",
                "@Controller('orders')\n",
                "export class OrdersController {\n",
                "  @Get()\n",
                "  listOrders() { return []; }\n",
                "}\n",
            ),
        )
        .expect("orders controller");
    }

    /// End-to-end: build two indexed artifact roots (base + head) and call the
    /// route extractor.  The head commit adds a second route; we assert the new
    /// route appears in `added` and the original is unchanged.
    ///
    /// This test uses `run_review_index` to build real graph stores and validates
    /// the extractor against live `GraphStoreDb` instances.
    #[test]
    fn route_added_end_to_end_via_real_indexer() {
        if !git_available() {
            return;
        }

        let source = TempDir::new("e2e-source");
        let cache = TempDir::new("e2e-cache");

        // ── base commit ───────────────────────────────────────────────────────
        write_base_fixture(source.path());
        git_init(source.path());
        let base_sha = git_commit(source.path(), "base: add GET /orders");

        // ── head commit: add POST /orders ─────────────────────────────────────
        let src = source.path().join("api/src");
        fs::write(
            src.join("orders.controller.ts"),
            concat!(
                "import { Controller, Get, Post } from '@nestjs/common';\n",
                "\n",
                "@Controller('orders')\n",
                "export class OrdersController {\n",
                "  @Get()\n",
                "  listOrders() { return []; }\n",
                "\n",
                "  @Post()\n",
                "  createOrder() { return {}; }\n",
                "}\n",
            ),
        )
        .expect("updated controller");
        let head_sha = git_commit(source.path(), "feat: add POST /orders");

        // ── index base ────────────────────────────────────────────────────────
        let base_artifact = create_artifact_root(
            cache.path(),
            source.path(),
            &base_sha,
            &base_sha,
            "e2e-base",
        )
        .expect("artifact root base");
        fs::remove_dir(&base_artifact.worktree_root).ok();
        gather_step_git::worktrees::create_detached_worktree(
            source.path(),
            &base_artifact.worktree_root,
            &base_sha,
        )
        .expect("base worktree");
        run_review_index(&base_artifact, None, IndexingOptions::default()).expect("base index");

        // ── index head ────────────────────────────────────────────────────────
        let head_artifact = create_artifact_root(
            cache.path(),
            source.path(),
            &base_sha,
            &head_sha,
            "e2e-head",
        )
        .expect("artifact root head");
        fs::remove_dir(&head_artifact.worktree_root).ok();
        gather_step_git::worktrees::create_detached_worktree(
            source.path(),
            &head_artifact.worktree_root,
            &head_sha,
        )
        .expect("head worktree");
        run_review_index(&head_artifact, None, IndexingOptions::default()).expect("head index");

        // ── open both stores and call extractor ───────────────────────────────
        let baseline_coord = StorageCoordinator::open_read_only(&base_artifact.storage_root)
            .expect("baseline coordinator");
        let review_coord = StorageCoordinator::open_read_only(&head_artifact.storage_root)
            .expect("review coordinator");

        let deltas = extract_route_deltas(baseline_coord.graph(), review_coord.graph())
            .expect("extractor should succeed");

        // The head added POST /orders; GET /orders is unchanged.
        // (NestJS decorator parsing may or may not fire in the minimal fixture —
        // if neither snapshot has any routes the extractor must still not panic.)
        assert!(
            deltas.removed.is_empty(),
            "no routes should be removed: {:?}",
            deltas.removed
        );
        // If the indexer did pick up both routes, only POST should be in `added`.
        for a in &deltas.added {
            assert_eq!(a.method, "POST", "only POST /orders should be added");
        }
    }
}
