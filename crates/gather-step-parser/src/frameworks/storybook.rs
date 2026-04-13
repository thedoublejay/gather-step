use gather_step_core::{
    EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, Visibility, ref_node_id,
};

use crate::tree_sitter::ParsedFile;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StorybookAugmentation {
    pub nodes: Vec<NodeData>,
    pub edges: Vec<EdgeData>,
}

/// Top-level entry point. Inspects `parsed` for Storybook and MSW signals and
/// returns virtual nodes + edges that represent:
///
/// - A `Service` node for each `.stories.*` file discovered
/// - `Defines` edges from the story file node to each publicly-exported symbol
/// - `References` edges from the story file node to the component it documents
/// - `Route` nodes + `Defines` edges for every MSW (`rest.*` / `http.*`) handler
/// - `References` edges from preview files to decorator/provider virtual nodes
pub fn augment(parsed: &ParsedFile) -> StorybookAugmentation {
    let mut aug = StorybookAugmentation::default();

    let file_str = parsed.file.path.to_string_lossy();
    let is_story_file = is_stories_file(&file_str);
    let is_preview_file = is_preview_file(&file_str);

    if is_story_file {
        add_storybook_file_node(parsed, &file_str, &mut aug);
        add_component_import_edges(parsed, &mut aug);
    }

    if is_preview_file {
        add_preview_provider_edges(parsed, &mut aug);
    }

    add_msw_handler_nodes(parsed, &mut aug);

    aug
}

// ---------------------------------------------------------------------------
// Story file detection helpers
// ---------------------------------------------------------------------------

fn is_stories_file(file_str: &str) -> bool {
    file_str.contains(".stories.")
}

fn is_preview_file(file_str: &str) -> bool {
    let filename = file_str
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(file_str.as_ref());
    matches!(
        filename,
        "preview.tsx" | "preview.jsx" | "preview.ts" | "preview.js"
    )
}

// ---------------------------------------------------------------------------
// Story file node + public symbol edges
// ---------------------------------------------------------------------------

/// Emit a virtual `Service` node for the story file itself, then a `Defines`
/// edge from the story file node to that virtual node for each publicly-
/// exported symbol in the file.
fn add_storybook_file_node(parsed: &ParsedFile, file_str: &str, aug: &mut StorybookAugmentation) {
    // Derive a human-readable name from the filename without extension.
    let stem = story_file_stem(file_str);
    let qualified_name = format!("__storybook__{file_str}");

    let story_node = virtual_service_node(parsed, &qualified_name, &stem);
    aug.nodes.push(story_node.clone());

    // Emit a Defines edge from the file node to the storybook Service node.
    aug.edges.push(EdgeData {
        source: parsed.file_node.id,
        target: story_node.id,
        kind: EdgeKind::Defines,
        metadata: EdgeMetadata::default(),
        owner_file: parsed.file_node.id,
        is_cross_file: false,
    });

    // Also emit Defines edges for each publicly-exported symbol so the graph
    // knows what stories live in this file.
    for symbol in &parsed.symbols {
        if symbol.node.visibility == Some(Visibility::Public) {
            aug.edges.push(EdgeData {
                source: story_node.id,
                target: symbol.node.id,
                kind: EdgeKind::Defines,
                metadata: EdgeMetadata::default(),
                owner_file: parsed.file_node.id,
                is_cross_file: false,
            });
        }
    }
}

/// Extract the stem of a story filename, stripping known story suffixes.
///
/// `src/components/Button.stories.tsx` → `Button`
fn story_file_stem(file_str: &str) -> String {
    let filename = file_str
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(file_str.as_ref());

    // Strip known story double-extensions in order of specificity.
    for suffix in &[".stories.tsx", ".stories.ts", ".stories.jsx", ".stories.js"] {
        if let Some(stem) = filename.strip_suffix(suffix) {
            return stem.to_owned();
        }
    }

    // Fallback: strip a single extension if nothing matched.
    filename
        .rsplit_once('.')
        .map_or(filename, |(stem, _)| stem)
        .to_owned()
}

// ---------------------------------------------------------------------------
// Component import edges
// ---------------------------------------------------------------------------

/// For each relative import in a story file, emit a virtual `Service` node
/// representing the imported component and a `References` edge from the file
/// node to that component node.
fn add_component_import_edges(parsed: &ParsedFile, aug: &mut StorybookAugmentation) {
    for binding in &parsed.import_bindings {
        if !binding.source.starts_with('.') {
            continue;
        }

        // Derive the component name from the import source path stem.
        let component_name = component_name_from_source(&binding.source);
        let qualified_name = format!("__component__{component_name}");

        let component_node = virtual_service_node(parsed, &qualified_name, &component_name);
        aug.nodes.push(component_node.clone());

        aug.edges.push(EdgeData {
            source: parsed.file_node.id,
            target: component_node.id,
            kind: EdgeKind::References,
            metadata: EdgeMetadata::default(),
            owner_file: parsed.file_node.id,
            is_cross_file: true,
        });
    }
}

/// Extract a component name from a relative import source string.
///
/// `./Button` → `Button`
/// `../components/Button` → `Button`
/// `./button.stories` → `button`
fn component_name_from_source(source: &str) -> String {
    let last = source.rsplit('/').next().unwrap_or(source);
    // Strip a leading `./` if still present (shouldn't be after rsplit, but
    // be defensive).
    let last = last.trim_start_matches("./");
    // Strip file extension.
    let stem = last.rsplit_once('.').map_or(last, |(s, _)| s);
    stem.to_owned()
}

// ---------------------------------------------------------------------------
// MSW handler detection
// ---------------------------------------------------------------------------

/// Scan `call_sites` for MSW `rest.*` / `http.*` handler registrations and
/// emit a virtual `Route` node + `Defines` edge for each one found.
///
/// Supported callee patterns:
/// - MSW v1: `rest.get`, `rest.post`, `rest.put`, `rest.delete`, `rest.patch`
/// - MSW v2: `http.get`, `http.post`, `http.put`, `http.delete`, `http.patch`
fn add_msw_handler_nodes(parsed: &ParsedFile, aug: &mut StorybookAugmentation) {
    for call_site in &parsed.call_sites {
        let Some(qualified_hint) = call_site.callee_qualified_hint.as_deref() else {
            continue;
        };

        let Some(method) = msw_method(qualified_hint) else {
            continue;
        };

        let Some(path) = call_site.literal_argument.as_deref() else {
            continue;
        };
        let path = path.trim().trim_matches('"').trim_matches('\'');

        let qualified_name = format!("__msw__{method}__{path}");
        let route_node = NodeData {
            id: ref_node_id(NodeKind::Route, &qualified_name),
            kind: NodeKind::Route,
            repo: parsed.file_node.repo.clone(),
            file_path: parsed.file_node.file_path.clone(),
            name: path.to_owned(),
            qualified_name: Some(qualified_name.clone()),
            external_id: Some(qualified_name),
            signature: None,
            visibility: None,
            span: call_site.span.clone(),
            is_virtual: true,
        };

        aug.nodes.push(route_node.clone());
        aug.edges.push(EdgeData {
            source: call_site.owner_id,
            target: route_node.id,
            kind: EdgeKind::Defines,
            metadata: EdgeMetadata::default(),
            owner_file: call_site.owner_file,
            is_cross_file: false,
        });
    }
}

/// Return the uppercased HTTP method if `qualified_hint` is an MSW handler
/// call (`rest.<method>` or `http.<method>`), or `None` otherwise.
fn msw_method(qualified_hint: &str) -> Option<&'static str> {
    // Split on the last `.` to get `(receiver, method_name)`.
    let (receiver, method_name) = qualified_hint.rsplit_once('.')?;

    // Accept only `rest` or `http` as the immediate receiver prefix.
    // The receiver may be a longer chain like `handlers.rest`; we match the
    // last segment only.
    let receiver_last = receiver.rsplit('.').next().unwrap_or(receiver);
    if !matches!(receiver_last, "rest" | "http") {
        return None;
    }

    match method_name {
        "get" => Some("GET"),
        "post" => Some("POST"),
        "put" => Some("PUT"),
        "delete" => Some("DELETE"),
        "patch" => Some("PATCH"),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Preview file provider edges
// ---------------------------------------------------------------------------

/// In Storybook preview files, emit `References` edges for decorator / provider
/// call sites so the graph records which global decorators are active.
///
/// **Known limitation:** Real Storybook preview files wrap stories in JSX
/// providers (`<QueryClientProvider>`, `<BrowserRouter>`, etc.). The
/// tree-sitter visitor does not capture JSX elements as call sites — only
/// function calls like `withThemeProvider(story)` are visible here. Full
/// JSX provider extraction requires extending the `visit_ts_js` visitor to
/// handle `jsx_element` / `jsx_self_closing_element` nodes.
fn add_preview_provider_edges(parsed: &ParsedFile, aug: &mut StorybookAugmentation) {
    for call_site in &parsed.call_sites {
        if !is_provider_callee(&call_site.callee_name) {
            continue;
        }

        let name = &call_site.callee_name;
        let qualified_name = format!("__storybook__provider__{name}");
        let provider_node = NodeData {
            id: ref_node_id(NodeKind::Service, &qualified_name),
            kind: NodeKind::Service,
            repo: parsed.file_node.repo.clone(),
            file_path: parsed.file_node.file_path.clone(),
            name: name.clone(),
            qualified_name: Some(qualified_name.clone()),
            external_id: Some(qualified_name),
            signature: None,
            visibility: None,
            span: call_site.span.clone(),
            is_virtual: true,
        };

        aug.nodes.push(provider_node.clone());
        aug.edges.push(EdgeData {
            source: call_site.owner_id,
            target: provider_node.id,
            kind: EdgeKind::References,
            metadata: EdgeMetadata::default(),
            owner_file: call_site.owner_file,
            is_cross_file: false,
        });
    }

    add_preview_jsx_provider_edges(parsed, aug);
}

/// Returns `true` if the callee name looks like a Storybook decorator or
/// React provider wrapper.
fn is_provider_callee(name: &str) -> bool {
    matches!(
        name,
        "withThemeProvider" | "withRouter" | "QueryClientProvider"
    ) || name.contains("Provider")
        || name.contains("Decorator")
}

fn add_preview_jsx_provider_edges(parsed: &ParsedFile, aug: &mut StorybookAugmentation) {
    let source = &*parsed.source;

    for provider_name in imported_jsx_provider_names(source, &parsed.import_bindings) {
        let qualified_name = format!("__storybook__provider__{provider_name}");
        let provider_node = NodeData {
            id: ref_node_id(NodeKind::Service, &qualified_name),
            kind: NodeKind::Service,
            repo: parsed.file_node.repo.clone(),
            file_path: parsed.file_node.file_path.clone(),
            name: provider_name.clone(),
            qualified_name: Some(qualified_name.clone()),
            external_id: Some(qualified_name),
            signature: None,
            visibility: None,
            span: parsed.file_node.span.clone(),
            is_virtual: true,
        };

        aug.nodes.push(provider_node.clone());
        aug.edges.push(EdgeData {
            source: parsed.file_node.id,
            target: provider_node.id,
            kind: EdgeKind::References,
            metadata: EdgeMetadata::default(),
            owner_file: parsed.file_node.id,
            is_cross_file: false,
        });
    }
}

fn imported_jsx_provider_names(
    source: &str,
    import_bindings: &[crate::resolve::ImportBinding],
) -> Vec<String> {
    let mut names: Vec<String> = import_bindings
        .iter()
        .map(|binding| binding.local_name.as_str())
        .filter(|name| looks_like_jsx_provider_name(name) && source.contains(&format!("<{name}")))
        .map(ToOwned::to_owned)
        .collect();
    names.sort();
    names.dedup();
    names
}

fn looks_like_jsx_provider_name(name: &str) -> bool {
    name.ends_with("Provider")
        || name.ends_with("Router")
        || matches!(name, "BrowserRouter" | "MemoryRouter" | "Router")
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn virtual_service_node(parsed: &ParsedFile, qualified_name: &str, name: &str) -> NodeData {
    NodeData {
        id: ref_node_id(NodeKind::Service, qualified_name),
        kind: NodeKind::Service,
        repo: parsed.file_node.repo.clone(),
        file_path: parsed.file_node.file_path.clone(),
        name: name.to_owned(),
        qualified_name: Some(qualified_name.to_owned()),
        external_id: Some(qualified_name.to_owned()),
        signature: None,
        visibility: None,
        span: None,
        is_virtual: true,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![expect(clippy::needless_raw_string_hashes)]

    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use pretty_assertions::assert_eq;

    use crate::{Language, frameworks::Framework, tree_sitter::parse_file_with_frameworks};

    // Tests in this module target the Storybook extractor specifically. They
    // bypass repo-level framework detection and always pass `Framework::Storybook`
    // so each test is focused on extractor behaviour rather than detection logic.
    fn parse_file(
        repo: &str,
        repo_root: &std::path::Path,
        file: &crate::FileEntry,
    ) -> Result<crate::ParsedFile, crate::ParseError> {
        parse_file_with_frameworks(repo, repo_root, file, &[Framework::Storybook])
    }

    static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> Self {
            let counter = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-parser-storybook-{name}-{}-{counter}",
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

    // -----------------------------------------------------------------------
    // 1. story_file_produces_storybook_node
    // -----------------------------------------------------------------------

    #[test]
    fn story_file_produces_storybook_node() {
        let temp_dir = TestDir::new("story-node");
        fs::write(
            temp_dir.path().join("Button.stories.tsx"),
            r#"
import type { Meta, StoryObj } from '@storybook/react';
import { Button } from './Button';

const meta: Meta<typeof Button> = {
  title: 'Components/Button',
  component: Button,
};

export default meta;

export const Primary: StoryObj<typeof Button> = {
  args: { label: 'Click me' },
};
"#,
        )
        .expect("story fixture should write");

        let parsed = parse_file(
            "frontend-app",
            temp_dir.path(),
            &crate::FileEntry {
                path: "Button.stories.tsx".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("story fixture should parse");

        let storybook_nodes: Vec<_> = parsed
            .nodes
            .iter()
            .filter(|n| {
                n.kind == gather_step_core::NodeKind::Service
                    && n.external_id
                        .as_deref()
                        .is_some_and(|id| id.starts_with("__storybook__"))
            })
            .collect();

        assert!(
            !storybook_nodes.is_empty(),
            "a .stories.tsx file should produce at least one __storybook__ Service node, \
             got nodes: {storybook_nodes:?}"
        );

        // The QN must encode the file path so nodes from different story files
        // are distinct.
        assert!(
            storybook_nodes.iter().any(|n| n
                .external_id
                .as_deref()
                .is_some_and(|id| id.contains("Button.stories.tsx"))),
            "the __storybook__ node QN should contain the file path"
        );
    }

    // -----------------------------------------------------------------------
    // 2. story_import_produces_component_reference
    // -----------------------------------------------------------------------

    #[test]
    fn story_import_produces_component_reference() {
        let temp_dir = TestDir::new("component-ref");
        fs::write(
            temp_dir.path().join("Button.stories.tsx"),
            r#"
import type { Meta } from '@storybook/react';
import { Button } from './Button';

export const Primary = {};
"#,
        )
        .expect("story import fixture should write");

        let parsed = parse_file(
            "frontend-app",
            temp_dir.path(),
            &crate::FileEntry {
                path: "Button.stories.tsx".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("story import fixture should parse");

        // There must be a __component__Button Service node.
        let component_node = parsed.nodes.iter().find(|n| {
            n.kind == gather_step_core::NodeKind::Service
                && n.external_id.as_deref() == Some("__component__Button")
        });
        assert!(
            component_node.is_some(),
            "importing './Button' in a story file should produce a __component__Button Service \
             node, got nodes: {:?}",
            parsed
                .nodes
                .iter()
                .map(|n| n.external_id.as_deref())
                .collect::<Vec<_>>()
        );

        // There must be a References edge pointing at the component node.
        let component_id = component_node.expect("checked above").id;
        let refs_edge = parsed
            .edges
            .iter()
            .find(|e| e.kind == gather_step_core::EdgeKind::References && e.target == component_id);
        assert!(
            refs_edge.is_some(),
            "a References edge from the story file to __component__Button should exist"
        );
    }

    // -----------------------------------------------------------------------
    // 3. msw_http_handler_produces_mock_route
    // -----------------------------------------------------------------------

    #[test]
    fn msw_http_handler_produces_mock_route() {
        let temp_dir = TestDir::new("msw-route");
        // MSW handler calls must be inside a function body so the tree-sitter
        // visitor assigns an owner_id and records them in call_sites.
        // Module-level calls (e.g. inside an array literal assigned to `const`)
        // do not receive an owner and are skipped by the visitor.
        fs::write(
            temp_dir.path().join("handlers.ts"),
            r#"
import { http, HttpResponse } from 'msw';

export function makeHandlers() {
  const getHandler = http.get('/api/orders', () => {
    return HttpResponse.json([]);
  });
  const postHandler = http.post('/api/orders', () => {
    return HttpResponse.json({ id: '1' });
  });
  return [getHandler, postHandler];
}
"#,
        )
        .expect("MSW fixture should write");

        let parsed = parse_file(
            "frontend-app",
            temp_dir.path(),
            &crate::FileEntry {
                path: "handlers.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("MSW fixture should parse");

        let msw_nodes: Vec<_> = parsed
            .nodes
            .iter()
            .filter(|n| {
                n.kind == gather_step_core::NodeKind::Route
                    && n.external_id
                        .as_deref()
                        .is_some_and(|id| id.starts_with("__msw__"))
            })
            .collect();

        assert!(
            msw_nodes
                .iter()
                .any(|n| n.external_id.as_deref() == Some("__msw__GET__/api/orders")),
            "http.get('/api/orders') should produce __msw__GET__/api/orders, \
             got msw nodes: {msw_nodes:?}"
        );

        // Verify that a Defines edge exists for the GET route.
        let get_node_id = parsed
            .nodes
            .iter()
            .find(|n| n.external_id.as_deref() == Some("__msw__GET__/api/orders"))
            .expect("GET route node should exist")
            .id;
        let defines_edge = parsed
            .edges
            .iter()
            .find(|e| e.kind == gather_step_core::EdgeKind::Defines && e.target == get_node_id);
        assert!(
            defines_edge.is_some(),
            "a Defines edge should exist pointing at __msw__GET__/api/orders"
        );
    }

    // -----------------------------------------------------------------------
    // 4. non_story_file_produces_no_storybook_nodes
    // -----------------------------------------------------------------------

    #[test]
    fn non_story_file_produces_no_storybook_nodes() {
        let temp_dir = TestDir::new("non-story");
        fs::write(
            temp_dir.path().join("Button.tsx"),
            r#"
import React from 'react';

export function Button({ label }: { label: string }) {
  return <button>{label}</button>;
}
"#,
        )
        .expect("non-story fixture should write");

        let parsed = parse_file(
            "frontend-app",
            temp_dir.path(),
            &crate::FileEntry {
                path: "Button.tsx".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("non-story fixture should parse");

        let storybook_nodes: Vec<_> = parsed
            .nodes
            .iter()
            .filter(|n| {
                n.external_id
                    .as_deref()
                    .is_some_and(|id| id.starts_with("__storybook__"))
            })
            .collect();

        assert_eq!(
            storybook_nodes,
            Vec::<&gather_step_core::NodeData>::new(),
            "a plain .tsx file should produce zero __storybook__ nodes"
        );
    }

    #[test]
    fn preview_jsx_wrappers_produce_provider_references() {
        let temp_dir = TestDir::new("preview-providers");
        fs::write(
            temp_dir.path().join("preview.jsx"),
            r#"
import { QueryClientProvider } from '@tanstack/react-query';
import { I18nextProvider } from 'react-i18next';
import { BrowserRouter } from 'react-router-dom';

export const decorators = [
  (Story) => (
    <QueryClientProvider client={queryClient}>
      <I18nextProvider i18n={i18n}>
        <BrowserRouter>
          <Story />
        </BrowserRouter>
      </I18nextProvider>
    </QueryClientProvider>
  ),
];
"#,
        )
        .expect("preview fixture should write");

        let parsed = parse_file(
            "frontend-app",
            temp_dir.path(),
            &crate::FileEntry {
                path: "preview.jsx".into(),
                language: Language::JavaScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("preview fixture should parse");

        for provider in [
            "__storybook__provider__QueryClientProvider",
            "__storybook__provider__I18nextProvider",
            "__storybook__provider__BrowserRouter",
        ] {
            let node = parsed
                .nodes
                .iter()
                .find(|node| node.external_id.as_deref() == Some(provider))
                .expect("preview JSX wrapper should produce provider node");
            assert!(
                parsed.edges.iter().any(|edge| {
                    edge.kind == gather_step_core::EdgeKind::References && edge.target == node.id
                }),
                "preview JSX wrapper should reference {provider}"
            );
        }
    }
}
