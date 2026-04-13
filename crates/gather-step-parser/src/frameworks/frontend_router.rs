//! Frontend routing / state / form framework extraction pack.
//!
//! This pack augments a [`ParsedFile`] with virtual nodes and edges for:
//!
//! 1. **React Router** — `createBrowserRouter`, `createHashRouter`,
//!    `createMemoryRouter`, navigation hooks (`useNavigate`, `useLocation`,
//!    `useParams`, `useSearchParams`), and JSX `<Route path="…">` components.
//! 2. **Zustand** — `create(…)` store factories detected by owner-name or
//!    qualified-hint heuristics.
//! 3. **Redux / Redux Saga** — `createStore`, `configureStore`, `createSlice`,
//!    `createAction`, `createAsyncThunk`, and saga effects (`takeEvery`,
//!    `takeLatest`, `call`, `put`).
//! 4. **React Hook Form** — `useForm`, `useFormContext`, `yupResolver`,
//!    `zodResolver`.
//! 5. **Provider / Context** — `createContext` and `useContext`.
//!
//! All emitted nodes carry `is_virtual: true`.  Qualified-name conventions
//! follow the same `__prefix__name` style used by the `NestJS` and React packs.

use gather_step_core::{EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, ref_node_id};

use crate::tree_sitter::{EnrichedCallSite, ParsedFile};

// ---------------------------------------------------------------------------
// Public surface
// ---------------------------------------------------------------------------

/// Output produced by [`augment`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FrontendRouterAugmentation {
    pub nodes: Vec<NodeData>,
    pub edges: Vec<EdgeData>,
}

/// Augment `parsed` with frontend framework nodes and edges.
///
/// Five extraction passes run in order:
///
/// 1. React Router router factories and navigation hooks.
/// 2. Zustand store factories.
/// 3. Redux / Redux Saga patterns.
/// 4. React Hook Form hooks and resolvers.
/// 5. React `createContext` / `useContext` patterns.
pub fn augment(parsed: &ParsedFile) -> FrontendRouterAugmentation {
    let mut aug = FrontendRouterAugmentation::default();
    add_react_router_edges(parsed, &mut aug);
    add_zustand_edges(parsed, &mut aug);
    add_redux_edges(parsed, &mut aug);
    add_hook_form_edges(parsed, &mut aug);
    add_context_edges(parsed, &mut aug);
    aug
}

// ---------------------------------------------------------------------------
// 1. React Router
// ---------------------------------------------------------------------------

/// Detect React Router call sites and emit virtual `Route` nodes plus edges.
///
/// | Call | QN | Edge kind |
/// |---|---|---|
/// | `createBrowserRouter` | `__router__browser` | `Defines` |
/// | `createHashRouter` | `__router__hash` | `Defines` |
/// | `createMemoryRouter` | `__router__memory` | `Defines` |
/// | `useNavigate` / `useLocation` / `useParams` / `useSearchParams` | `__router__navigation` | `References` |
/// | `Route` (component call with path arg) | `__frontend_route__<path>` | `Defines` |
fn add_react_router_edges(parsed: &ParsedFile, aug: &mut FrontendRouterAugmentation) {
    for call in &parsed.call_sites {
        match call.callee_name.as_str() {
            "createBrowserRouter" => {
                let node = virtual_node_from_call(
                    NodeKind::Route,
                    "__router__browser",
                    "browser",
                    parsed,
                    call,
                );
                push_node_and_edge(node, call, parsed, aug, EdgeKind::Defines);
            }
            "createHashRouter" => {
                let node =
                    virtual_node_from_call(NodeKind::Route, "__router__hash", "hash", parsed, call);
                push_node_and_edge(node, call, parsed, aug, EdgeKind::Defines);
            }
            "createMemoryRouter" => {
                let node = virtual_node_from_call(
                    NodeKind::Route,
                    "__router__memory",
                    "memory",
                    parsed,
                    call,
                );
                push_node_and_edge(node, call, parsed, aug, EdgeKind::Defines);
            }
            "useNavigate" | "useLocation" | "useParams" | "useSearchParams" => {
                let node = virtual_node_from_call(
                    NodeKind::Route,
                    "__router__navigation",
                    "navigation",
                    parsed,
                    call,
                );
                push_node_and_edge(node, call, parsed, aug, EdgeKind::References);
            }
            "Route" => {
                // Only emit when a path string argument is present.
                let Some(raw_path) = call.literal_argument.as_deref() else {
                    continue;
                };
                let path = sanitize_string(raw_path);
                if path.is_empty() {
                    continue;
                }
                let qn = format!("__frontend_route__{path}");
                let node = virtual_node_from_call(NodeKind::Route, &qn, &path, parsed, call);
                push_node_and_edge(node, call, parsed, aug, EdgeKind::Defines);
            }
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// 2. Zustand
// ---------------------------------------------------------------------------

/// Detect Zustand `create(…)` store factories.
///
/// Two heuristics are applied because the qualified-hint may or may not carry
/// import information, depending on whether the TypeScript parser could resolve
/// the binding:
///
/// - **Hint heuristic**: `callee_qualified_hint` contains `"zustand"`.
/// - **Owner-name heuristic**: `callee_name == "create"` and the owner
///   function/variable name contains `"Store"` or `"store"`.
///
/// When either heuristic fires, a virtual `Service` node with QN
/// `__store__zustand__<StoreName>` is emitted, where `<StoreName>` is derived
/// from the owner symbol name stored in `owner_id`.  Because `EnrichedCallSite`
/// does not expose a name string for the owner, we fall back to extracting it
/// from `callee_qualified_hint` or use a stable sentinel.
fn add_zustand_edges(parsed: &ParsedFile, aug: &mut FrontendRouterAugmentation) {
    for call in &parsed.call_sites {
        if call.callee_name != "create" {
            continue;
        }

        let hint_matches = call
            .callee_qualified_hint
            .as_deref()
            .is_some_and(|h| h.contains("zustand"));

        // Derive store name from owner function ID or the qualified hint.
        // `owner_id` is a hash, so we cannot recover the original name from
        // it directly. We look for a symbol whose node.id matches owner_id.
        let owner_name = parsed
            .symbols
            .iter()
            .find(|s| s.node.id == call.owner_id)
            .map(|s| s.node.name.clone())
            .unwrap_or_default();

        let name_matches = owner_name.contains("Store") || owner_name.contains("store");

        if !hint_matches && !name_matches {
            continue;
        }

        // Use the owner function name when available; fall back to "store".
        let store_name = if owner_name.is_empty() {
            "store".to_owned()
        } else {
            owner_name
        };

        let qn = format!("__store__zustand__{store_name}");
        let node = virtual_node_from_call(NodeKind::Service, &qn, &store_name, parsed, call);
        push_node_and_edge(node, call, parsed, aug, EdgeKind::Defines);
    }
}

// ---------------------------------------------------------------------------
// 3. Redux / Redux Saga
// ---------------------------------------------------------------------------

/// Detect Redux and Redux Saga call sites and emit virtual nodes and edges.
///
/// | Call | QN | Edge kind |
/// |---|---|---|
/// | `createStore` / `configureStore` | `__store__redux__<owner>` | `Defines` |
/// | `createSlice(name, …)` | `__store__redux__<name>` | `Defines` |
/// | `createAction(type, …)` | `__action__redux__<type>` | `Defines` |
/// | `createAsyncThunk(type, …)` | `__action__redux__<type>` | `Defines` |
/// | `takeEvery` / `takeLatest` / `call` / `put` | `__saga__effect` | `References` |
fn add_redux_edges(parsed: &ParsedFile, aug: &mut FrontendRouterAugmentation) {
    for call in &parsed.call_sites {
        match call.callee_name.as_str() {
            "createStore" | "configureStore" => {
                // Use owner symbol name to distinguish multiple stores when
                // possible; fall back to a generic sentinel.
                let owner_name = owner_symbol_name(parsed, call);
                let name = if owner_name.is_empty() {
                    "store".to_owned()
                } else {
                    owner_name
                };
                let qn = format!("__store__redux__{name}");
                let node = virtual_node_from_call(NodeKind::Service, &qn, &name, parsed, call);
                push_node_and_edge(node, call, parsed, aug, EdgeKind::Defines);
            }
            "createSlice" => {
                // First string argument is the slice name.
                let name = call
                    .literal_argument
                    .as_deref()
                    .map(sanitize_string)
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| owner_symbol_name(parsed, call))
                    .clone();
                let effective = if name.is_empty() {
                    "slice".to_owned()
                } else {
                    name
                };
                let qn = format!("__store__redux__{effective}");
                let node = virtual_node_from_call(NodeKind::Service, &qn, &effective, parsed, call);
                push_node_and_edge(node, call, parsed, aug, EdgeKind::Defines);
            }
            "createAction" | "createAsyncThunk" => {
                let Some(raw) = call.literal_argument.as_deref() else {
                    continue;
                };
                let action_type = sanitize_string(raw);
                if action_type.is_empty() {
                    continue;
                }
                let qn = format!("__action__redux__{action_type}");
                let node =
                    virtual_node_from_call(NodeKind::Service, &qn, &action_type, parsed, call);
                push_node_and_edge(node, call, parsed, aug, EdgeKind::Defines);
            }
            "takeEvery" | "takeLatest" | "call" | "put" => {
                let node = virtual_node_from_call(
                    NodeKind::Service,
                    "__saga__effect",
                    "saga_effect",
                    parsed,
                    call,
                );
                push_node_and_edge(node, call, parsed, aug, EdgeKind::References);
            }
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// 4. React Hook Form
// ---------------------------------------------------------------------------

/// Detect React Hook Form call sites and emit virtual nodes and edges.
///
/// | Call | QN | Edge kind |
/// |---|---|---|
/// | `useForm` | `__form__hookform` | `DependsOn` |
/// | `useFormContext` | `__form__hookform` | `References` |
/// | `yupResolver` | `__validator__yup` | `DependsOn` |
/// | `zodResolver` | `__validator__zod` | `DependsOn` |
fn add_hook_form_edges(parsed: &ParsedFile, aug: &mut FrontendRouterAugmentation) {
    for call in &parsed.call_sites {
        match call.callee_name.as_str() {
            "useForm" => {
                let node = virtual_node_from_call(
                    NodeKind::Service,
                    "__form__hookform",
                    "hookform",
                    parsed,
                    call,
                );
                push_node_and_edge(node, call, parsed, aug, EdgeKind::DependsOn);
            }
            "useFormContext" => {
                let node = virtual_node_from_call(
                    NodeKind::Service,
                    "__form__hookform",
                    "hookform",
                    parsed,
                    call,
                );
                push_node_and_edge(node, call, parsed, aug, EdgeKind::References);
            }
            "yupResolver" => {
                let node = virtual_node_from_call(
                    NodeKind::Service,
                    "__validator__yup",
                    "yup",
                    parsed,
                    call,
                );
                push_node_and_edge(node, call, parsed, aug, EdgeKind::DependsOn);
            }
            "zodResolver" => {
                let node = virtual_node_from_call(
                    NodeKind::Service,
                    "__validator__zod",
                    "zod",
                    parsed,
                    call,
                );
                push_node_and_edge(node, call, parsed, aug, EdgeKind::DependsOn);
            }
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// 5. Provider / Context
// ---------------------------------------------------------------------------

/// Detect React context creation and consumption call sites.
///
/// | Call | QN | Edge kind |
/// |---|---|---|
/// | `createContext(name)` | `__context__<name>` | `Defines` |
/// | `useContext` | `__context__usage` | `References` |
///
/// For `createContext`, the context name is derived from the first string
/// argument when present, or from the owner symbol name, or `"context"`.
fn add_context_edges(parsed: &ParsedFile, aug: &mut FrontendRouterAugmentation) {
    for call in &parsed.call_sites {
        match call.callee_name.as_str() {
            "createContext" => {
                // Prefer the literal argument, then the owner name, then a
                // sentinel — all stripped of quotes/whitespace.
                let name = call
                    .literal_argument
                    .as_deref()
                    .map(sanitize_string)
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| {
                        let n = owner_symbol_name(parsed, call);
                        if n.is_empty() {
                            "context".to_owned()
                        } else {
                            n
                        }
                    });
                let qn = format!("__context__{name}");
                let node = virtual_node_from_call(NodeKind::Service, &qn, &name, parsed, call);
                push_node_and_edge(node, call, parsed, aug, EdgeKind::Defines);
            }
            "useContext" => {
                let node = virtual_node_from_call(
                    NodeKind::Service,
                    "__context__usage",
                    "context_usage",
                    parsed,
                    call,
                );
                push_node_and_edge(node, call, parsed, aug, EdgeKind::References);
            }
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Build a virtual [`NodeData`] derived from a call site.
///
/// Uses [`ref_node_id`] for a deterministic ID so duplicate call sites across
/// the file resolve to the same node (deduplication is handled by
/// `append_unique_nodes` in the orchestrator).
fn virtual_node_from_call(
    kind: NodeKind,
    qualified_name: &str,
    name: &str,
    parsed: &ParsedFile,
    call: &EnrichedCallSite,
) -> NodeData {
    NodeData {
        id: ref_node_id(kind, qualified_name),
        kind,
        repo: parsed.file_node.repo.clone(),
        file_path: parsed.file_node.file_path.clone(),
        name: name.to_owned(),
        qualified_name: Some(qualified_name.to_owned()),
        external_id: Some(qualified_name.to_owned()),
        signature: None,
        visibility: None,
        span: call.span.clone(),
        is_virtual: true,
    }
}

/// Push `node` onto the augmentation's node list and add an edge of `kind`
/// from `call.owner_id` to the node.
fn push_node_and_edge(
    node: NodeData,
    call: &EnrichedCallSite,
    parsed: &ParsedFile,
    aug: &mut FrontendRouterAugmentation,
    kind: EdgeKind,
) {
    let target_id = node.id;
    aug.nodes.push(node);
    aug.edges.push(EdgeData {
        source: call.owner_id,
        target: target_id,
        kind,
        metadata: EdgeMetadata::default(),
        owner_file: parsed.file_node.id,
        is_cross_file: false,
    });
}

/// Derive a display name for the owner symbol by matching `call.owner_id`
/// against parsed symbols. Returns an empty string when no match is found.
fn owner_symbol_name(parsed: &ParsedFile, call: &EnrichedCallSite) -> String {
    parsed
        .symbols
        .iter()
        .find(|s| s.node.id == call.owner_id)
        .map(|s| s.node.name.clone())
        .unwrap_or_default()
}

/// Strip surrounding quotes, brackets, and whitespace from a raw string
/// literal captured by the parser.
fn sanitize_string(raw: &str) -> String {
    raw.trim()
        .trim_matches('[')
        .trim_matches(']')
        .trim_matches('"')
        .trim_matches('\'')
        .trim()
        .to_owned()
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

    use crate::{Language, frameworks::Framework, tree_sitter::parse_file_with_frameworks};

    // Tests in this module target the frontend router extractor specifically.
    // They bypass repo-level framework detection and always pass
    // `Framework::ReactRouter` so the unit tests remain focused on extractor
    // behaviour rather than detection heuristics.
    fn parse_file(
        repo: &str,
        repo_root: &Path,
        file: &crate::FileEntry,
    ) -> Result<crate::ParsedFile, crate::ParseError> {
        parse_file_with_frameworks(repo, repo_root, file, &[Framework::ReactRouter])
    }

    static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> Self {
            let counter = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-parser-frontend-router-{name}-{}-{counter}",
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
    // 1. React Router
    // -----------------------------------------------------------------------

    #[test]
    fn create_browser_router_produces_router_node() {
        // `createBrowserRouter` must be called inside a named function body so
        // the tree-sitter visitor assigns an `owner_id` and records the call in
        // `call_sites`. Module-level calls (owner = None) are skipped.
        let temp_dir = TestDir::new("browser-router");
        fs::write(
            temp_dir.path().join("router.tsx"),
            r#"
import { createBrowserRouter } from 'react-router-dom';

export function makeRouter() {
  const router = createBrowserRouter([
    { path: '/', element: null },
  ]);
  return router;
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "frontend-app",
            temp_dir.path(),
            &crate::FileEntry {
                path: "router.tsx".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        let route_nodes: Vec<_> = parsed
            .nodes
            .iter()
            .filter(|n| n.kind == gather_step_core::NodeKind::Route)
            .collect();

        assert!(
            route_nodes
                .iter()
                .any(|n| n.external_id.as_deref() == Some("__router__browser")),
            "expected __router__browser Route node, got: {route_nodes:?}"
        );

        // Verify a Defines edge was emitted from makeRouter to the router node.
        let defines_edges: Vec<_> = parsed
            .edges
            .iter()
            .filter(|e| e.kind == gather_step_core::EdgeKind::Defines)
            .collect();
        assert!(
            !defines_edges.is_empty(),
            "expected at least one Defines edge"
        );
    }

    // -----------------------------------------------------------------------
    // 2. Zustand
    // -----------------------------------------------------------------------

    #[test]
    fn zustand_create_produces_store_node() {
        // The `create` call must be inside a named function whose name contains
        // "Store" so the owner-name heuristic fires. The owner function becomes
        // the store name in the emitted QN.
        let temp_dir = TestDir::new("zustand-store");
        fs::write(
            temp_dir.path().join("store.ts"),
            r#"
import { create } from 'zustand';

export function useProductStore() {
  return create((set) => ({
    items: [],
    addProduct: (item) => set((state) => ({ items: [...state.items, item] })),
  }));
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "frontend-app",
            temp_dir.path(),
            &crate::FileEntry {
                path: "store.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        // The QN is `__store__zustand__useProductStore` because the owner function
        // is `useProductStore` and its name contains "Store".
        let store_nodes: Vec<_> = parsed
            .nodes
            .iter()
            .filter(|n| {
                n.kind == gather_step_core::NodeKind::Service
                    && n.external_id
                        .as_deref()
                        .is_some_and(|id| id.starts_with("__store__zustand__"))
            })
            .collect();

        assert!(
            store_nodes
                .iter()
                .any(|n| n.external_id.as_deref() == Some("__store__zustand__useProductStore")),
            "expected __store__zustand__useProductStore Service node, got nodes: {store_nodes:?}"
        );
    }

    // -----------------------------------------------------------------------
    // 3. Redux
    // -----------------------------------------------------------------------

    #[test]
    fn redux_create_slice_produces_store_node() {
        // `createSlice` must be called inside a named function so the
        // tree-sitter visitor assigns an owner_id and records the call.
        let temp_dir = TestDir::new("redux-slice");
        fs::write(
            temp_dir.path().join("productsSlice.ts"),
            r#"
import { createSlice } from '@reduxjs/toolkit';

export function makeProductsSlice() {
  const productsSlice = createSlice('products', {
    initialState: [],
    reducers: {
      addProduct: (state, action) => { state.push(action.payload); },
    },
  });
  return productsSlice;
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "frontend-app",
            temp_dir.path(),
            &crate::FileEntry {
                path: "productsSlice.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        let store_nodes: Vec<_> = parsed
            .nodes
            .iter()
            .filter(|n| {
                n.kind == gather_step_core::NodeKind::Service
                    && n.external_id
                        .as_deref()
                        .is_some_and(|id| id.starts_with("__store__redux__"))
            })
            .collect();

        assert!(
            store_nodes
                .iter()
                .any(|n| n.external_id.as_deref() == Some("__store__redux__products")),
            "expected __store__redux__products node, got: {store_nodes:?}"
        );
    }

    // -----------------------------------------------------------------------
    // 4. React Hook Form
    // -----------------------------------------------------------------------

    #[test]
    fn use_form_produces_form_node() {
        let temp_dir = TestDir::new("hook-form");
        fs::write(
            temp_dir.path().join("form.tsx"),
            r#"
import { useForm } from 'react-hook-form';

export function ProductForm() {
  const { register, handleSubmit } = useForm();
  return null;
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "frontend-app",
            temp_dir.path(),
            &crate::FileEntry {
                path: "form.tsx".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        let form_nodes: Vec<_> = parsed
            .nodes
            .iter()
            .filter(|n| {
                n.kind == gather_step_core::NodeKind::Service
                    && n.external_id.as_deref() == Some("__form__hookform")
            })
            .collect();

        assert!(
            !form_nodes.is_empty(),
            "expected __form__hookform Service node, got nodes: {:?}",
            parsed
                .nodes
                .iter()
                .map(|n| n.external_id.as_deref())
                .collect::<Vec<_>>()
        );

        // Verify a DependsOn edge was emitted.
        let depends_edges: Vec<_> = parsed
            .edges
            .iter()
            .filter(|e| e.kind == gather_step_core::EdgeKind::DependsOn)
            .collect();
        assert!(
            !depends_edges.is_empty(),
            "expected at least one DependsOn edge from useForm"
        );
    }

    // -----------------------------------------------------------------------
    // 5. Provider / Context
    // -----------------------------------------------------------------------

    #[test]
    fn create_context_produces_context_node() {
        // `createContext` must be called inside a named function so the
        // tree-sitter visitor assigns an owner_id and records the call.
        let temp_dir = TestDir::new("create-context");
        fs::write(
            temp_dir.path().join("auth-context.tsx"),
            r#"
import { createContext } from 'react';

export function makeAuthContext() {
  const AuthContext = createContext('auth');
  return AuthContext;
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "frontend-app",
            temp_dir.path(),
            &crate::FileEntry {
                path: "auth-context.tsx".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        let context_nodes: Vec<_> = parsed
            .nodes
            .iter()
            .filter(|n| {
                n.kind == gather_step_core::NodeKind::Service
                    && n.external_id
                        .as_deref()
                        .is_some_and(|id| id.starts_with("__context__"))
            })
            .collect();

        assert!(
            context_nodes
                .iter()
                .any(|n| n.external_id.as_deref() == Some("__context__auth")),
            "expected __context__auth Service node, got context nodes: {context_nodes:?}"
        );

        // Verify a Defines edge was emitted.
        let defines_edges: Vec<_> = parsed
            .edges
            .iter()
            .filter(|e| e.kind == gather_step_core::EdgeKind::Defines)
            .collect();
        assert!(
            !defines_edges.is_empty(),
            "expected at least one Defines edge from createContext"
        );
    }
}
