use std::{fs, path::PathBuf};

use gather_step_core::{EdgeKind, NodeData, NodeKind, config::GatherStepConfig};
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

fn find_file_node(nodes: &[NodeData], file_path: &str) -> NodeData {
    nodes
        .iter()
        .find(|node| node.kind == NodeKind::File && node.file_path == file_path)
        .cloned()
        .unwrap_or_else(|| panic!("file node {file_path} should exist"))
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
        r"
from . import services as svc
import pkg.submodule as submodule

def run_pipeline(item):
    runner = svc.Runner()
    return runner.run(item) or submodule.fallback(item)
",
    );
    write_fixture(
        repo_root.path(),
        "package/app/services.py",
        r"
class Runner:
    def run(self, item):
        return item
",
    );
    write_fixture(
        repo_root.path(),
        "pkg/submodule.py",
        r"
def fallback(item):
    return item
",
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

#[test]
fn python_planning_workspace_fixture_indexes_from_config() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("tests/fixtures/python_planning_workspace")
        .canonicalize()
        .expect("fixture workspace should exist");
    let config = GatherStepConfig::from_yaml_file(repo_root.join("gather-step.config.yaml"))
        .expect("fixture config should parse");
    let storage_root = tempfile::tempdir().expect("storage tempdir should create");
    let indexer =
        RepoIndexer::open(storage_root.path(), IndexingOptions::default()).expect("indexer");

    assert_eq!(config.repos.len(), 4);
    for repo in &config.repos {
        indexer
            .index_repo(&repo.name, repo_root.join(&repo.path), None)
            .unwrap_or_else(|error| panic!("{} should index: {error}", repo.name));
    }

    let graph = indexer.storage().graph();
    let transform_nodes = graph
        .nodes_by_file("py_transform_service", "src/transform_service/pipeline.py")
        .expect("transform nodes should load");
    let shared_nodes = graph
        .nodes_by_file("py_shared_models", "src/shared_models/records.py")
        .expect("shared model nodes should load");
    let api_nodes = graph
        .nodes_by_file("py_api_service", "src/api_service/app.py")
        .expect("api nodes should load");

    find_node(&transform_nodes, NodeKind::Function, "transform_batch");
    find_node(&shared_nodes, NodeKind::Class, "ParsedDocument");
    find_node(&api_nodes, NodeKind::Function, "ingest_documents");

    let api_file = find_file_node(&api_nodes, "src/api_service/app.py");
    let transform_file = find_file_node(&transform_nodes, "src/transform_service/pipeline.py");
    let shared_file = find_file_node(&shared_nodes, "src/shared_models/records.py");

    let api_outgoing = graph
        .get_outgoing(api_file.id)
        .expect("api file outgoing edges should load");
    assert!(
        api_outgoing
            .iter()
            .any(|edge| { edge.kind == EdgeKind::Imports && edge.target == transform_file.id })
    );
    assert!(
        api_outgoing
            .iter()
            .any(|edge| { edge.kind == EdgeKind::Imports && edge.target == shared_file.id })
    );

    let transform_outgoing = graph
        .get_outgoing(transform_file.id)
        .expect("transform file outgoing edges should load");
    assert!(
        transform_outgoing
            .iter()
            .any(|edge| { edge.kind == EdgeKind::Imports && edge.target == shared_file.id })
    );
    assert!(
        transform_outgoing
            .iter()
            .any(|edge| { edge.kind == EdgeKind::UsesTypeFrom && edge.target == shared_file.id })
    );
}
