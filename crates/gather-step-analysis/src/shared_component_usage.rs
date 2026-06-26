use std::collections::BTreeMap;

use gather_step_core::{NodeData, NodeKind};
use gather_step_storage::{GraphStore, GraphStoreError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SharedComponentError {
    #[error(transparent)]
    Store(#[from] GraphStoreError),
}

// Design-system-specific markers only. Generic shared-code dirs (`/lib/`,
// `common/`, bare `packages/`, `internal/`, `/pkg/`) are deliberately excluded:
// they over-match ordinary code (`src/lib/api`) and inflate the "shared" set.
const DESIGN_SYSTEM_MARKERS: &[&str] = &[
    "design-system",
    "@shared",
    "shared/components",
    "packages/ui",
    "/ui/components",
];

#[must_use]
pub fn is_design_system_path(file_path: &str) -> bool {
    DESIGN_SYSTEM_MARKERS
        .iter()
        .any(|marker| file_path.contains(marker))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReuseOpportunity {
    pub repo: String,
    pub local_symbol: String,
    pub local_file: String,
    pub shared_symbol: String,
    pub shared_file: String,
}

fn is_component_like(node: &NodeData) -> bool {
    matches!(node.kind, NodeKind::Function | NodeKind::Class)
}

// Stories, tests, figma snapshots, mocks and the MSW worker frequently live
// under `shared/components/` but are not reusable components. Treating their
// exports as "shared" produces the dominant false positives (story `Template`,
// test `constructor`, figma `noop`).
const NON_COMPONENT_FILE_MARKERS: &[&str] = &[
    ".stories.",
    "/stories/",
    ".test.",
    ".spec.",
    ".figma.",
    ".mock.",
    "mockServiceWorker",
];

fn is_component_module(file_path: &str) -> bool {
    !NON_COMPONENT_FILE_MARKERS
        .iter()
        .any(|marker| file_path.contains(marker))
}

// Component identity is PascalCase. Bare handler/lifecycle names
// (`handleChange`, `render`, `noop`, `constructor`) collide across unrelated
// files and are not component forks.
fn is_component_name(name: &str) -> bool {
    name.chars().next().is_some_and(|c| c.is_ascii_uppercase())
}

pub fn analyze_shared_component_reuse<S: GraphStore>(
    store: &S,
    repo: &str,
) -> Result<Vec<ReuseOpportunity>, SharedComponentError> {
    let nodes = store.nodes_by_repo(repo)?;

    let mut shared: BTreeMap<String, String> = BTreeMap::new();
    for node in &nodes {
        if is_component_like(node)
            && is_design_system_path(&node.file_path)
            && is_component_module(&node.file_path)
            && is_component_name(&node.name)
        {
            shared
                .entry(node.name.clone())
                .or_insert_with(|| node.file_path.clone());
        }
    }

    let mut opportunities = Vec::new();
    for node in &nodes {
        if !is_component_like(node)
            || is_design_system_path(&node.file_path)
            || !is_component_module(&node.file_path)
        {
            continue;
        }
        if let Some(shared_file) = shared.get(&node.name) {
            opportunities.push(ReuseOpportunity {
                repo: repo.to_owned(),
                local_symbol: node.name.clone(),
                local_file: node.file_path.clone(),
                shared_symbol: node.name.clone(),
                shared_file: shared_file.clone(),
            });
        }
    }

    opportunities.sort_by(|left, right| {
        left.local_file
            .cmp(&right.local_file)
            .then(left.local_symbol.cmp(&right.local_symbol))
    });
    opportunities.dedup();
    Ok(opportunities)
}

#[cfg(test)]
mod tests {
    use gather_step_core::{NodeData, NodeKind, SourceSpan, Visibility, node_id};
    use gather_step_storage::{GraphStore, GraphStoreDb};

    use super::{analyze_shared_component_reuse, is_design_system_path};
    use crate::test_utils::TempDb;

    fn node(repo: &str, file_path: &str, name: &str, ordinal: u32) -> NodeData {
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
                line_start: ordinal,
                line_len: 0,
                column_start: 0,
                column_len: 1,
            }),
            is_virtual: false,
            ai_role: None,
        }
    }

    #[test]
    fn design_system_path_markers() {
        assert!(is_design_system_path("packages/ui/components/Button.tsx"));
        assert!(is_design_system_path("src/shared/components/Card.tsx"));
        assert!(is_design_system_path("libs/design-system/Modal.tsx"));
        // Generic shared-code dirs must NOT be treated as design-system.
        assert!(!is_design_system_path("src/lib/api.ts"));
        assert!(!is_design_system_path("src/common/utils.ts"));
        assert!(!is_design_system_path("src/features/orders/Button.tsx"));
    }

    #[test]
    fn flags_local_fork_of_a_shared_component() {
        let temp = TempDb::new("shared-component", "fork");
        let store = GraphStoreDb::open(temp.path()).expect("store");
        let shared = node("web", "packages/ui/components/Button.tsx", "Button", 0);
        let fork = node("web", "src/features/orders/Button.tsx", "Button", 1);
        let unique = node("web", "src/features/orders/OrderList.tsx", "OrderList", 2);
        store
            .bulk_insert(&[shared.clone(), fork.clone(), unique.clone()], &[])
            .expect("write");

        let opportunities = analyze_shared_component_reuse(&store, "web").expect("analyze");
        assert_eq!(opportunities.len(), 1);
        assert_eq!(
            opportunities[0].local_file,
            "src/features/orders/Button.tsx"
        );
        assert_eq!(
            opportunities[0].shared_file,
            "packages/ui/components/Button.tsx"
        );
    }

    #[test]
    fn ignores_story_test_and_figma_targets_under_shared() {
        // Storybook/test/figma files living under `shared/components/` are not
        // reusable components. A local symbol matching one of their exports must
        // not be reported as a fork (the dominant false-positive source).
        let temp = TempDb::new("shared-component", "non-component-target");
        let store = GraphStoreDb::open(temp.path()).expect("store");
        let story_export = node(
            "web",
            "src/v2/shared/components/Button/stories/Loading.tsx",
            "Loading",
            0,
        );
        let local = node("web", "src/admin/loader.jsx", "Loading", 1);
        store
            .bulk_insert(&[story_export, local], &[])
            .expect("write");

        assert!(
            analyze_shared_component_reuse(&store, "web")
                .expect("analyze")
                .is_empty(),
            "story/test/figma targets must not count as shared components"
        );
    }

    #[test]
    fn ignores_generic_non_component_symbol_names() {
        // Bare handler / lifecycle names (`handleChange`, `render`, `noop`,
        // `constructor`) collide constantly and are not component identities.
        // Only component-shaped (PascalCase) names should match.
        let temp = TempDb::new("shared-component", "generic-name");
        let store = GraphStoreDb::open(temp.path()).expect("store");
        let shared = node(
            "web",
            "src/v2/shared/components/CommentBox/CommentInput.tsx",
            "handleChange",
            0,
        );
        let local = node("web", "src/admin/hooks/useFilter.tsx", "handleChange", 1);
        store.bulk_insert(&[shared, local], &[]).expect("write");

        assert!(
            analyze_shared_component_reuse(&store, "web")
                .expect("analyze")
                .is_empty(),
            "generic camelCase names must not be reported as component forks"
        );
    }

    #[test]
    fn shared_only_or_no_duplicate_yields_nothing() {
        let temp = TempDb::new("shared-component", "clean");
        let store = GraphStoreDb::open(temp.path()).expect("store");
        let shared = node("web", "packages/ui/components/Modal.tsx", "Modal", 0);
        let local = node("web", "src/features/orders/OrderRow.tsx", "OrderRow", 1);
        store.bulk_insert(&[shared, local], &[]).expect("write");

        assert!(
            analyze_shared_component_reuse(&store, "web")
                .expect("analyze")
                .is_empty()
        );
    }
}
