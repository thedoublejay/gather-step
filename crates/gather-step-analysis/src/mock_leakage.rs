use gather_step_core::EdgeKind;
use gather_step_storage::{GraphStore, GraphStoreError};
use rustc_hash::FxHashMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MockLeakageError {
    #[error(transparent)]
    Store(#[from] GraphStoreError),
}

const MOCK_MARKERS: &[&str] = &[
    "__mocks__",
    ".mock.",
    "/mocks/",
    ".fixture.",
    "/fixtures/",
    ".stub.",
];

const TEST_MARKERS: &[&str] = &[".test.", ".spec.", "/__tests__/", "/tests/", "/test/"];

#[must_use]
pub fn is_mock_path(file_path: &str) -> bool {
    MOCK_MARKERS.iter().any(|marker| file_path.contains(marker))
}

#[must_use]
pub fn is_test_path(file_path: &str) -> bool {
    TEST_MARKERS.iter().any(|marker| file_path.contains(marker))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MockLeakage {
    pub repo: String,
    pub importer_file: String,
    pub mock_file: String,
}

pub fn find_mock_leakage<S: GraphStore>(
    store: &S,
    repo: &str,
) -> Result<Vec<MockLeakage>, MockLeakageError> {
    let nodes = store.nodes_by_repo(repo)?;
    let file_of: FxHashMap<[u8; 16], String> = nodes
        .iter()
        .map(|node| (node.id.as_bytes(), node.file_path.clone()))
        .collect();

    let mut leaks = Vec::new();
    for node in &nodes {
        if is_test_path(&node.file_path) || is_mock_path(&node.file_path) {
            continue;
        }
        for edge in store.get_outgoing(node.id)? {
            if edge.kind != EdgeKind::Imports {
                continue;
            }
            let Some(target_file) = file_of.get(&edge.target.as_bytes()) else {
                continue;
            };
            if is_mock_path(target_file) {
                leaks.push(MockLeakage {
                    repo: repo.to_owned(),
                    importer_file: node.file_path.clone(),
                    mock_file: target_file.clone(),
                });
            }
        }
    }

    leaks.sort_by(|left, right| {
        left.importer_file
            .cmp(&right.importer_file)
            .then(left.mock_file.cmp(&right.mock_file))
    });
    leaks.dedup();
    Ok(leaks)
}

#[cfg(test)]
mod tests {
    use gather_step_core::{EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeId, NodeKind, node_id};
    use gather_step_storage::{GraphStore, GraphStoreDb};

    use super::find_mock_leakage;
    use crate::test_utils::TempDb;

    fn module(repo: &str, file_path: &str) -> NodeData {
        NodeData {
            id: node_id(repo, file_path, NodeKind::File, file_path),
            kind: NodeKind::File,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: file_path.to_owned(),
            qualified_name: Some(file_path.to_owned()),
            external_id: None,
            signature: None,
            visibility: None,
            span: None,
            is_virtual: false,
            ai_role: None,
        }
    }

    fn imports(source: NodeId, target: NodeId, owner: NodeId) -> EdgeData {
        EdgeData {
            source,
            target,
            kind: EdgeKind::Imports,
            metadata: EdgeMetadata::default(),
            owner_file: owner,
            is_cross_file: true,
        }
    }

    #[test]
    fn flags_prod_module_importing_a_mock() {
        let temp = TempDb::new("mock-leakage", "leak");
        let store = GraphStoreDb::open(temp.path()).expect("store");
        let prod = module("web", "src/features/OrderList.tsx");
        let mock = module("web", "src/features/__mocks__/orders.mock.ts");
        store
            .bulk_insert(
                &[prod.clone(), mock.clone()],
                &[imports(prod.id, mock.id, prod.id)],
            )
            .expect("write");

        let leaks = find_mock_leakage(&store, "web").expect("analyze");
        assert_eq!(leaks.len(), 1);
        assert_eq!(leaks[0].importer_file, "src/features/OrderList.tsx");
        assert_eq!(leaks[0].mock_file, "src/features/__mocks__/orders.mock.ts");
    }

    #[test]
    fn test_file_importing_a_mock_is_not_flagged() {
        let temp = TempDb::new("mock-leakage", "test-import");
        let store = GraphStoreDb::open(temp.path()).expect("store");
        let spec = module("web", "src/features/OrderList.test.tsx");
        let mock = module("web", "src/features/__mocks__/orders.mock.ts");
        store
            .bulk_insert(
                &[spec.clone(), mock.clone()],
                &[imports(spec.id, mock.id, spec.id)],
            )
            .expect("write");

        assert!(
            find_mock_leakage(&store, "web")
                .expect("analyze")
                .is_empty()
        );
    }

    #[test]
    fn prod_importing_prod_is_not_flagged() {
        let temp = TempDb::new("mock-leakage", "clean");
        let store = GraphStoreDb::open(temp.path()).expect("store");
        let prod = module("web", "src/features/OrderList.tsx");
        let helper = module("web", "src/features/format.ts");
        store
            .bulk_insert(
                &[prod.clone(), helper.clone()],
                &[imports(prod.id, helper.id, prod.id)],
            )
            .expect("write");

        assert!(
            find_mock_leakage(&store, "web")
                .expect("analyze")
                .is_empty()
        );
    }
}
