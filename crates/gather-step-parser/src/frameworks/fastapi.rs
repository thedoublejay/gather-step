//! `FastAPI` route augmentation (v5 Phase 1).
//!
//! Emits a virtual `Route` node + `Serves` edge for each `@app.<method>(...)`
//! / `@router.<method>(...)` handler — parity with the `NestJS` route pass.
//!
//! Verified decorator-capture facts: `single_decorator` keeps only the last
//! name segment, so `@app.get(...)` is seen as `"get"`; `split_arguments`
//! already strips quotes, so the first argument is the bare path.
//!
//! Same-file `APIRouter(prefix="…")` and `app.include_router(router, prefix="…")`
//! bindings are composed into the public route path (`app_prefix` +
//! `router_prefix` + decorator path). Cross-file `include_router` resolution is
//! out of scope.

use gather_step_core::{
    EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, ref_node_id, route_qn,
};

use crate::{
    frameworks::join_route_path,
    tree_sitter::{ParsedFile, RouterPrefixBindings, SymbolCapture},
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FastapiAugmentation {
    pub nodes: Vec<NodeData>,
    pub edges: Vec<EdgeData>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FastapiRouteBinding {
    pub handler: NodeData,
    pub file_node: gather_step_core::NodeId,
    pub receiver: String,
    pub method: String,
    /// Router-constructor prefix plus decorator path. Repo-level mounts are
    /// deliberately excluded and composed by the fixed-point indexer pass.
    pub local_path: String,
}

/// HTTP-method decorator names `FastAPI` exposes on `app`/`router`. Lowercase
/// because Python uses `@app.get`, unlike `NestJS`'s `@Get`.
pub(crate) const HTTP_METHODS: &[&str] = &[
    "get",
    "post",
    "put",
    "delete",
    "patch",
    "options",
    "head",
    "trace",
    "websocket",
];

#[must_use]
pub fn augment(parsed: &ParsedFile) -> FastapiAugmentation {
    let mut augmentation = FastapiAugmentation::default();
    for symbol in &parsed.symbols {
        add_route(symbol, &parsed.router_prefixes, &mut augmentation);
    }
    for mount in &parsed.router_prefixes.asgi_mounts {
        let path = join_route_path(&mount.path, "/:path");
        let qualified_name = route_qn("MOUNT", &path);
        let route_node = NodeData {
            id: ref_node_id(NodeKind::Route, &qualified_name),
            kind: NodeKind::Route,
            repo: parsed.file_node.repo.clone(),
            file_path: parsed.file_node.file_path.clone(),
            name: qualified_name.clone(),
            qualified_name: Some(qualified_name.clone()),
            external_id: Some(qualified_name),
            signature: Some(format!("{}.mount({})", mount.receiver, mount.path)),
            visibility: None,
            span: None,
            is_virtual: true,
            ai_role: None,
        };
        augmentation.edges.push(EdgeData {
            source: parsed.file_node.id,
            target: route_node.id,
            kind: EdgeKind::Serves,
            metadata: EdgeMetadata {
                confidence: Some(950),
                resolver: Some("fastapi_asgi_mount".to_owned()),
                ..EdgeMetadata::default()
            },
            owner_file: parsed.file_node.id,
            is_cross_file: false,
        });
        augmentation.nodes.push(route_node);
    }
    augmentation
}

#[must_use]
pub fn route_bindings(parsed: &ParsedFile) -> Vec<FastapiRouteBinding> {
    parsed
        .symbols
        .iter()
        .filter_map(|symbol| route_binding(symbol, &parsed.router_prefixes))
        .collect()
}

fn route_binding(
    symbol: &SymbolCapture,
    prefixes: &RouterPrefixBindings,
) -> Option<FastapiRouteBinding> {
    let decorator = symbol
        .decorators
        .iter()
        .find(|decorator| HTTP_METHODS.contains(&decorator.name.as_str()))?;
    let receiver = decorator.receiver.clone()?;
    let mut local_path = decorator.arguments.first()?.to_string();
    if let Some(prefix) = prefixes.ctor.get(&receiver) {
        local_path = join_route_path(prefix, &local_path);
    }
    Some(FastapiRouteBinding {
        handler: symbol.node.clone(),
        file_node: symbol.file_node,
        receiver,
        method: decorator.name.to_ascii_uppercase(),
        local_path,
    })
}

fn add_route(
    symbol: &SymbolCapture,
    prefixes: &RouterPrefixBindings,
    augmentation: &mut FastapiAugmentation,
) {
    let Some((method, path)) = route_method_and_path(symbol, prefixes) else {
        return;
    };
    let qn = route_qn(&method, &path);
    emit_route(symbol, &qn, augmentation);
}

pub(crate) fn route_method_and_path(
    symbol: &SymbolCapture,
    prefixes: &RouterPrefixBindings,
) -> Option<(String, String)> {
    let decorator = symbol
        .decorators
        .iter()
        .find(|decorator| HTTP_METHODS.contains(&decorator.name.as_str()))?;
    // A matching-named decorator with no path argument is not a route.
    // `split_arguments` already strips quotes, so this is the bare path.
    let mut path = decorator.arguments.first().map(ToString::to_string)?;
    let method = decorator.name.to_ascii_uppercase();
    let Some(receiver) = decorator.receiver.as_deref() else {
        return Some((method, path));
    };
    if let Some(prefix) = prefixes.ctor.get(receiver) {
        path = join_route_path(prefix, &path);
    }
    if let Some(prefix) = prefixes.include.get(receiver) {
        path = join_route_path(prefix, &path);
    }
    Some((method, path))
}

fn emit_route(
    symbol: &SymbolCapture,
    qualified_name: &str,
    augmentation: &mut FastapiAugmentation,
) {
    let route_node = virtual_node(NodeKind::Route, qualified_name, symbol);
    augmentation.edges.push(EdgeData {
        source: symbol.node.id,
        target: route_node.id,
        kind: EdgeKind::Serves,
        metadata: EdgeMetadata::default(),
        owner_file: symbol.file_node,
        is_cross_file: false,
    });
    augmentation.nodes.push(route_node);
}

fn virtual_node(kind: NodeKind, qualified_name: &str, symbol: &SymbolCapture) -> NodeData {
    NodeData {
        id: ref_node_id(kind, qualified_name),
        kind,
        repo: symbol.node.repo.clone(),
        file_path: symbol.node.file_path.clone(),
        name: qualified_name.to_owned(),
        qualified_name: Some(qualified_name.to_owned()),
        external_id: Some(qualified_name.to_owned()),
        signature: None,
        visibility: None,
        span: symbol.node.span.clone(),
        is_virtual: true,
        ai_role: None,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::{EdgeKind, NodeKind};

    use crate::{Language, frameworks::Framework, tree_sitter::parse_file_with_frameworks};

    static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> Self {
            let counter = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-parser-fastapi-{name}-{}-{counter}",
                process::id()
            ));
            fs::create_dir_all(&path).expect("test directory should be created");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn fastapi_routes_are_extracted_from_fixture() {
        let dir = TestDir::new("routes");
        fs::write(
            dir.path().join("api.py"),
            r#"
from fastapi import APIRouter, FastAPI

app = FastAPI()
router = APIRouter()


@app.get("/items")
def list_items():
    return []


@router.post("/items/{item_id}")
def create_item(item_id: int):
    return {}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file_with_frameworks(
            "ingestion",
            dir.path(),
            &crate::FileEntry {
                path: "api.py".into(),
                language: Language::Python,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &[Framework::FastApi],
        )
        .expect("fixture should parse");

        let mut routes = parsed
            .nodes
            .iter()
            .filter(|node| node.kind == NodeKind::Route)
            .map(|node| node.external_id.clone().unwrap_or_default())
            .collect::<Vec<_>>();
        routes.sort();
        assert_eq!(
            routes,
            vec![
                "__route__GET__/items".to_owned(),
                "__route__POST__/items/:item_id".to_owned(),
            ]
        );

        let serves = parsed
            .edges
            .iter()
            .filter(|edge| edge.kind == EdgeKind::Serves)
            .count();
        assert_eq!(serves, 2, "each handler should Serve its route");
    }

    fn route_ids(source: &str) -> Vec<String> {
        let dir = TestDir::new("compose");
        fs::write(dir.path().join("api.py"), source).expect("fixture should write");
        let parsed = parse_file_with_frameworks(
            "ingestion",
            dir.path(),
            &crate::FileEntry {
                path: "api.py".into(),
                language: Language::Python,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &[Framework::FastApi],
        )
        .expect("fixture should parse");
        let mut routes = parsed
            .nodes
            .iter()
            .filter(|node| node.kind == NodeKind::Route)
            .map(|node| node.external_id.clone().unwrap_or_default())
            .collect::<Vec<_>>();
        routes.sort();
        routes
    }

    #[test]
    fn apirouter_prefix_composes_into_route_identity() {
        let routes = route_ids(
            r#"
from fastapi import APIRouter

router = APIRouter(prefix="/items")


@router.get("/{item_id}")
def get_item(item_id: int):
    return {}
"#,
        );
        assert_eq!(
            routes,
            vec!["__route__GET__/items/:item_id".to_owned()],
            "APIRouter prefix should compose into the only public route path: {routes:?}"
        );
    }

    #[test]
    fn include_router_prefix_composes_with_apirouter_prefix() {
        let routes = route_ids(
            r#"
from fastapi import APIRouter, FastAPI

app = FastAPI()
router = APIRouter(prefix="/items")


@router.get("/{item_id}")
def get_item(item_id: int):
    return {}


app.include_router(router, prefix="/v1")
"#,
        );
        assert_eq!(
            routes,
            vec!["__route__GET__/v1/items/:item_id".to_owned()],
            "include_router + APIRouter prefixes should compose into the only public route path: {routes:?}"
        );
    }

    #[test]
    fn dynamic_router_prefix_skips_composition() {
        let routes = route_ids(
            r#"
from fastapi import APIRouter

router = APIRouter(prefix=dynamic_prefix)


@router.get("/things")
def list_things():
    return []
"#,
        );
        assert_eq!(
            routes,
            vec!["__route__GET__/things".to_owned()],
            "a dynamic prefix should keep only the bare route: {routes:?}"
        );
    }

    #[test]
    fn asgi_mount_emits_a_static_mount_surface() {
        let routes = route_ids(
            r#"
from fastapi import FastAPI
from starlette.staticfiles import StaticFiles

app = FastAPI()
app.mount("/assets", StaticFiles(directory="assets"), name="assets")
"#,
        );
        assert_eq!(routes, vec!["__route__MOUNT__/assets/:path".to_owned()]);
    }
}
