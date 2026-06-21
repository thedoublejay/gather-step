//! `FastAPI` route augmentation (v5 Phase 1).
//!
//! Emits a virtual `Route` node + `Serves` edge for each `@app.<method>(...)`
//! / `@router.<method>(...)` handler — parity with the `NestJS` route pass.
//!
//! Verified decorator-capture facts: `single_decorator` keeps only the last
//! name segment, so `@app.get(...)` is seen as `"get"`; `split_arguments`
//! already strips quotes, so the first argument is the bare path. `APIRouter`
//! `prefix=` resolution needs receiver/RHS binding and is deferred to R2.

use gather_step_core::{
    EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, ref_node_id, route_qn,
};

use crate::tree_sitter::{ParsedFile, SymbolCapture};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FastapiAugmentation {
    pub nodes: Vec<NodeData>,
    pub edges: Vec<EdgeData>,
}

/// HTTP-method decorator names `FastAPI` exposes on `app`/`router`. Lowercase
/// because Python uses `@app.get`, unlike `NestJS`'s `@Get`.
const HTTP_METHODS: &[&str] = &[
    "get", "post", "put", "delete", "patch", "options", "head", "trace",
];

#[must_use]
pub fn augment(parsed: &ParsedFile) -> FastapiAugmentation {
    let mut augmentation = FastapiAugmentation::default();
    for symbol in &parsed.symbols {
        add_route(symbol, &mut augmentation);
    }
    augmentation
}

fn add_route(symbol: &SymbolCapture, augmentation: &mut FastapiAugmentation) {
    let Some(decorator) = symbol
        .decorators
        .iter()
        .find(|decorator| HTTP_METHODS.contains(&decorator.name.as_str()))
    else {
        return;
    };
    // A matching-named decorator with no path argument is not a route.
    // `split_arguments` already strips quotes, so this is the bare path.
    let Some(path) = decorator.arguments.first().map(ToString::to_string) else {
        return;
    };
    let method = decorator.name.to_ascii_uppercase();
    let qualified_name = route_qn(&method, &path);
    let route_node = virtual_node(NodeKind::Route, &qualified_name, symbol);
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
}
