use std::fs;

use gather_step_core::{EdgeKind, NodeData, NodeKind};
use gather_step_storage::{GraphStore, IndexingOptions, RepoIndexer};

fn write_fixture(root: &std::path::Path, relative: &str, contents: &str) {
    let path = root.join(relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("fixture parent should create");
    }
    fs::write(path, contents).expect("fixture should write");
}

fn find_node(nodes: &[NodeData], kind: NodeKind, name: &str) -> NodeData {
    nodes
        .iter()
        .find(|node| node.kind == kind && node.name == name)
        .cloned()
        .unwrap_or_else(|| panic!("{kind:?} node {name} should exist"))
}

#[test]
fn indexes_python_imported_module_calls_into_storage_graph() {
    let repo_root = tempfile::tempdir().expect("repo tempdir should create");
    let storage_root = tempfile::tempdir().expect("storage tempdir should create");

    write_fixture(repo_root.path(), "package/__init__.py", "");
    write_fixture(repo_root.path(), "package/app/__init__.py", "");
    write_fixture(repo_root.path(), "pkg/__init__.py", "");
    write_fixture(
        repo_root.path(),
        "package/app/imports.py",
        r#"
from . import services as svc
import pkg.submodule as submodule

def run_pipeline(item):
    runner = svc.Runner()
    return runner.run(item) or submodule.fallback(item)
"#,
    );
    write_fixture(
        repo_root.path(),
        "package/app/services.py",
        r#"
class Runner:
    def run(self, item):
        return item
"#,
    );
    write_fixture(
        repo_root.path(),
        "pkg/submodule.py",
        r#"
def fallback(item):
    return item
"#,
    );

    let indexer =
        RepoIndexer::open(storage_root.path(), IndexingOptions::default()).expect("indexer");
    indexer
        .index_repo("python-service", repo_root.path(), None)
        .expect("indexing should succeed");

    let graph = indexer.storage().graph();
    let imports_nodes = graph
        .nodes_by_file("python-service", "package/app/imports.py")
        .expect("imports nodes should load");
    let services_nodes = graph
        .nodes_by_file("python-service", "package/app/services.py")
        .expect("services nodes should load");
    let submodule_nodes = graph
        .nodes_by_file("python-service", "pkg/submodule.py")
        .expect("submodule nodes should load");

    let run_pipeline = find_node(&imports_nodes, NodeKind::Function, "run_pipeline");
    let runner = find_node(&services_nodes, NodeKind::Class, "Runner");
    let fallback = find_node(&submodule_nodes, NodeKind::Function, "fallback");
    let outgoing = graph
        .get_outgoing(run_pipeline.id)
        .expect("outgoing edges should load");

    assert!(outgoing.iter().any(|edge| {
        edge.kind == EdgeKind::Calls
            && edge.target == runner.id
            && edge.metadata.resolver.as_deref() == Some("import_map")
    }));
    assert!(outgoing.iter().any(|edge| {
        edge.kind == EdgeKind::Calls
            && edge.target == fallback.id
            && edge.metadata.resolver.as_deref() == Some("import_map")
    }));
}
