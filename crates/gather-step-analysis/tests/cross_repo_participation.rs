//! Tests for the per-repo `cross_repo_participation_by_file` primitive
//! (v5.1 Part 2, Task 6).
//!
//! These build real multi-repo [`GraphStoreDb`] fixtures (mirroring the
//! `cross_repo` / `dead_code` fixture pattern) and assert that producer files
//! reachable to a foreign consumer get annotated, that same-repo-only flows
//! stay empty, and that the implementation walks edges per *file* (not per
//! *node*).

use std::{
    env, fs,
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, Ordering},
};

use gather_step_analysis::cross_repo_participation::cross_repo_participation_by_file;
use gather_step_core::{
    EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeId, NodeKind, SourceSpan, Visibility, node_id,
    virtual_node,
};
use gather_step_storage::{GraphStore, GraphStoreDb};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDb {
    path: PathBuf,
}

impl TempDb {
    fn new(name: &str) -> Self {
        let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!(
            "gather-step-xrepo-participation-{name}-{}-{id}.redb",
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
        let _ = fs::remove_file(&self.path);
    }
}

fn file(repo: &str, file_path: &str) -> NodeData {
    NodeData {
        id: node_id(repo, file_path, NodeKind::File, file_path),
        kind: NodeKind::File,
        repo: repo.to_owned(),
        file_path: file_path.to_owned(),
        name: file_path.to_owned(),
        qualified_name: Some(format!("{repo}::{file_path}")),
        external_id: None,
        signature: None,
        visibility: None,
        span: None,
        is_virtual: false,
        ai_role: None,
    }
}

fn symbol(repo: &str, file_path: &str, name: &str) -> NodeData {
    NodeData {
        id: node_id(repo, file_path, NodeKind::Function, name),
        kind: NodeKind::Function,
        repo: repo.to_owned(),
        file_path: file_path.to_owned(),
        name: name.to_owned(),
        qualified_name: Some(format!("{repo}::{name}")),
        external_id: None,
        signature: None,
        visibility: Some(Visibility::Public),
        span: Some(SourceSpan {
            line_start: 1,
            line_len: 1,
            column_start: 0,
            column_len: 0,
        }),
        is_virtual: false,
        ai_role: None,
    }
}

fn defines(owner: NodeId, target: NodeId) -> EdgeData {
    EdgeData {
        source: owner,
        target,
        kind: EdgeKind::Defines,
        metadata: EdgeMetadata::default(),
        owner_file: owner,
        is_cross_file: false,
    }
}

fn edge(owner: NodeId, source: NodeId, target: NodeId, kind: EdgeKind) -> EdgeData {
    EdgeData {
        source,
        target,
        kind,
        metadata: EdgeMetadata::default(),
        owner_file: owner,
        is_cross_file: true,
    }
}

/// Build a route-mediated two-repo fixture:
///
/// ```text
/// service-api/src/config/credit.ts   (CREDIT_AGENT_CONFIGS)
///        ^-- References -- service-api/src/handlers/credit.ts (handler)
///        handler -- Serves --> __route__GET__/credits (virtual Route)
/// service-ui/src/caller.ts (caller) -- ConsumesApiFrom --> __route__GET__/credits
/// ```
///
/// From `service-api`'s perspective the route has a foreign consumer
/// (`service-ui`); the producing handler file and — transitively — the
/// config file it references must both be annotated with `service-ui`.
fn fixture_route_mediated_two_repo() -> GraphStoreDb {
    let temp_db = TempDb::new("route-mediated");
    let path = temp_db.path().to_path_buf();
    std::mem::forget(temp_db);
    let store = GraphStoreDb::open(&path).expect("store should open");

    let config_file = file("service-api", "src/config/credit.ts");
    let config_sym = symbol(
        "service-api",
        "src/config/credit.ts",
        "CREDIT_AGENT_CONFIGS",
    );
    let handler_file = file("service-api", "src/handlers/credit.ts");
    let handler_sym = symbol("service-api", "src/handlers/credit.ts", "getCredits");

    let caller_file = file("service-ui", "src/caller.ts");
    let caller_sym = symbol("service-ui", "src/caller.ts", "callCredits");

    let route = virtual_node(
        NodeKind::Route,
        "service-api",
        "src/handlers/credit.ts",
        "GET /credits",
        "__route__GET__/credits",
    );

    store
        .bulk_insert(
            &[
                config_file.clone(),
                config_sym.clone(),
                handler_file.clone(),
                handler_sym.clone(),
                caller_file.clone(),
                caller_sym.clone(),
                route.clone(),
            ],
            &[
                defines(config_file.id, config_sym.id),
                defines(handler_file.id, handler_sym.id),
                defines(caller_file.id, caller_sym.id),
                // handler file references the config file's symbol.
                edge(
                    handler_file.id,
                    handler_sym.id,
                    config_sym.id,
                    EdgeKind::References,
                ),
                // handler serves the route.
                edge(handler_file.id, handler_sym.id, route.id, EdgeKind::Serves),
                // service-ui consumes the route.
                edge(
                    caller_file.id,
                    caller_sym.id,
                    route.id,
                    EdgeKind::ConsumesApiFrom,
                ),
            ],
        )
        .expect("fixture insert");

    store
}

#[test]
fn route_mediated_config_file_is_marked_with_foreign_consumer() {
    let store = fixture_route_mediated_two_repo();
    let map = cross_repo_participation_by_file(&store, "service-api").expect("participation");

    assert!(
        map.get("src/config/credit.ts")
            .is_some_and(|repos| repos.contains("service-ui")),
        "config file must inherit service-ui as a transitive foreign consumer, got: {map:?}"
    );
    assert!(
        map.get("src/handlers/credit.ts")
            .is_some_and(|repos| repos.contains("service-ui")),
        "handler file (route producer) must be marked, got: {map:?}"
    );
}

/// Single-repo route: the producer and consumer both live in `solo`, so the
/// map must be empty (no foreign consumer).
fn fixture_single_repo_route() -> GraphStoreDb {
    let temp_db = TempDb::new("single-repo");
    let path = temp_db.path().to_path_buf();
    std::mem::forget(temp_db);
    let store = GraphStoreDb::open(&path).expect("store should open");

    let handler_file = file("solo", "src/handlers/x.ts");
    let handler_sym = symbol("solo", "src/handlers/x.ts", "getX");
    let caller_file = file("solo", "src/caller.ts");
    let caller_sym = symbol("solo", "src/caller.ts", "callX");
    let route = virtual_node(
        NodeKind::Route,
        "solo",
        "src/handlers/x.ts",
        "GET /x",
        "__route__GET__/x",
    );

    store
        .bulk_insert(
            &[
                handler_file.clone(),
                handler_sym.clone(),
                caller_file.clone(),
                caller_sym.clone(),
                route.clone(),
            ],
            &[
                defines(handler_file.id, handler_sym.id),
                defines(caller_file.id, caller_sym.id),
                edge(handler_file.id, handler_sym.id, route.id, EdgeKind::Serves),
                edge(
                    caller_file.id,
                    caller_sym.id,
                    route.id,
                    EdgeKind::ConsumesApiFrom,
                ),
            ],
        )
        .expect("fixture insert");

    store
}

#[test]
fn single_repo_route_has_no_foreign_consumers() {
    let store = fixture_single_repo_route();
    let map = cross_repo_participation_by_file(&store, "solo").expect("participation");
    assert!(
        map.is_empty(),
        "same-repo producer+consumer must yield no foreign-consumer annotations, got: {map:?}"
    );
}
