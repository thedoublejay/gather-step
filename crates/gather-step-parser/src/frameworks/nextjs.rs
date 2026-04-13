use std::sync::OnceLock;

use aho_corasick::AhoCorasick;
use gather_step_core::{EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, ref_node_id};
use memchr::memmem;

use crate::tree_sitter::ParsedFile;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NextjsAugmentation {
    pub nodes: Vec<NodeData>,
    pub edges: Vec<EdgeData>,
}

pub fn augment(parsed: &ParsedFile) -> NextjsAugmentation {
    let mut augmentation = NextjsAugmentation::default();
    let file_path = parsed.file_node.file_path.clone();
    let source = &*parsed.source;

    add_route_node(parsed, &file_path, &mut augmentation);
    add_layout_node(parsed, &file_path, &mut augmentation);
    add_middleware_node(parsed, &file_path, &mut augmentation);
    add_boundary_nodes(parsed, &file_path, source, &mut augmentation);
    add_metadata_nodes(parsed, &file_path, source, &mut augmentation);
    add_server_action_nodes(parsed, &file_path, source, &mut augmentation);

    augmentation
}

fn add_route_node(parsed: &ParsedFile, file_path: &str, augmentation: &mut NextjsAugmentation) {
    let Some((route_kind, route_path)) = next_route_descriptor(file_path) else {
        return;
    };

    let qualified_name = format!("__next_route__{route_kind}__{route_path}");
    let route_node = NodeData {
        id: ref_node_id(NodeKind::Route, &qualified_name),
        kind: NodeKind::Route,
        repo: parsed.file_node.repo.clone(),
        file_path: file_path.to_owned(),
        name: route_path.clone(),
        qualified_name: Some(qualified_name.clone()),
        external_id: Some(qualified_name),
        signature: Some(route_kind.to_owned()),
        visibility: None,
        span: parsed.file_node.span.clone(),
        is_virtual: true,
    };
    augmentation.nodes.push(route_node.clone());

    let handler_ids = match route_kind {
        "route_handler" => exported_http_handlers(parsed),
        _ => Vec::new(),
    };
    if handler_ids.is_empty() {
        augmentation.edges.push(EdgeData {
            source: parsed.file_node.id,
            target: route_node.id,
            kind: EdgeKind::Defines,
            metadata: EdgeMetadata::default(),
            owner_file: parsed.file_node.id,
            is_cross_file: false,
        });
        return;
    }

    for handler_id in handler_ids {
        augmentation.edges.push(EdgeData {
            source: handler_id,
            target: route_node.id,
            kind: EdgeKind::Serves,
            metadata: EdgeMetadata::default(),
            owner_file: parsed.file_node.id,
            is_cross_file: false,
        });
    }
}

fn add_layout_node(parsed: &ParsedFile, file_path: &str, augmentation: &mut NextjsAugmentation) {
    let Some(layout_path) = next_layout_path(file_path) else {
        return;
    };

    let qualified_name = format!("__next_layout__{layout_path}");
    let layout_node = NodeData {
        id: ref_node_id(NodeKind::Service, &qualified_name),
        kind: NodeKind::Service,
        repo: parsed.file_node.repo.clone(),
        file_path: file_path.to_owned(),
        name: layout_path,
        qualified_name: Some(qualified_name.clone()),
        external_id: Some(qualified_name),
        signature: Some("layout".to_owned()),
        visibility: None,
        span: parsed.file_node.span.clone(),
        is_virtual: true,
    };
    augmentation.nodes.push(layout_node.clone());
    augmentation.edges.push(EdgeData {
        source: parsed.file_node.id,
        target: layout_node.id,
        kind: EdgeKind::Defines,
        metadata: EdgeMetadata::default(),
        owner_file: parsed.file_node.id,
        is_cross_file: false,
    });
}

fn add_middleware_node(
    parsed: &ParsedFile,
    file_path: &str,
    augmentation: &mut NextjsAugmentation,
) {
    if !is_next_middleware(file_path) {
        return;
    }

    let qualified_name = "__next_middleware__root";
    let middleware_node = NodeData {
        id: ref_node_id(NodeKind::Service, qualified_name),
        kind: NodeKind::Service,
        repo: parsed.file_node.repo.clone(),
        file_path: file_path.to_owned(),
        name: "middleware".to_owned(),
        qualified_name: Some(qualified_name.to_owned()),
        external_id: Some(qualified_name.to_owned()),
        signature: Some("middleware".to_owned()),
        visibility: None,
        span: parsed.file_node.span.clone(),
        is_virtual: true,
    };
    augmentation.nodes.push(middleware_node.clone());
    augmentation.edges.push(EdgeData {
        source: parsed.file_node.id,
        target: middleware_node.id,
        kind: EdgeKind::Defines,
        metadata: EdgeMetadata::default(),
        owner_file: parsed.file_node.id,
        is_cross_file: false,
    });
}

/// `AhoCorasick` automaton covering all four `use client` / `use server` marker
/// variants (single-quoted and double-quoted).
///
/// Pattern indices:
/// - 0: `'use client'`
/// - 1: `"use client"`
/// - 2: `'use server'`
/// - 3: `"use server"`
static BOUNDARY_AC: OnceLock<AhoCorasick> = OnceLock::new();

fn boundary_ac() -> &'static AhoCorasick {
    BOUNDARY_AC.get_or_init(|| {
        AhoCorasick::new([
            "'use client'",
            "\"use client\"",
            "'use server'",
            "\"use server\"",
        ])
        .expect("BOUNDARY_AC patterns are valid")
    })
}

/// Returns a bitmask indicating which boundary markers are present in `source`.
///
/// Bit 0: `use client` present (either quote style).
/// Bit 1: `use server` present (either quote style).
fn boundary_flags(source: &str) -> u8 {
    let mut flags = 0_u8;
    for mat in boundary_ac().find_iter(source.as_bytes()) {
        match mat.pattern().as_usize() {
            // patterns 0–1 are 'use client' / "use client"
            0 | 1 => flags |= 0b01,
            // patterns 2–3 are 'use server' / "use server"
            _ => flags |= 0b10,
        }
        if flags == 0b11 {
            break; // both found; no need to scan further
        }
    }
    flags
}

fn add_boundary_nodes(
    parsed: &ParsedFile,
    file_path: &str,
    source: &str,
    augmentation: &mut NextjsAugmentation,
) {
    let flags = boundary_flags(source);
    for (bit, kind) in [(0b01_u8, "client"), (0b10_u8, "server")] {
        if flags & bit == 0 {
            continue;
        }
        let qn = format!("__next_boundary__{kind}__{file_path}");
        let node = NodeData {
            id: ref_node_id(NodeKind::Convention, &qn),
            kind: NodeKind::Convention,
            repo: parsed.file_node.repo.clone(),
            file_path: file_path.to_owned(),
            name: kind.to_owned(),
            qualified_name: Some(qn.clone()),
            external_id: Some(qn),
            signature: Some("boundary".to_owned()),
            visibility: None,
            span: parsed.file_node.span.clone(),
            is_virtual: true,
        };
        augmentation.nodes.push(node.clone());
        augmentation.edges.push(EdgeData {
            source: parsed.file_node.id,
            target: node.id,
            kind: EdgeKind::Defines,
            metadata: EdgeMetadata::default(),
            owner_file: parsed.file_node.id,
            is_cross_file: false,
        });
    }
}

/// SIMD-accelerated single-needle finders for the Next.js metadata markers.
static EXPORT_METADATA_FINDER: OnceLock<memmem::Finder<'static>> = OnceLock::new();
static GENERATE_METADATA_FINDER: OnceLock<memmem::Finder<'static>> = OnceLock::new();

fn export_metadata_finder() -> &'static memmem::Finder<'static> {
    EXPORT_METADATA_FINDER.get_or_init(|| memmem::Finder::new("export const metadata").into_owned())
}

fn generate_metadata_finder() -> &'static memmem::Finder<'static> {
    GENERATE_METADATA_FINDER.get_or_init(|| memmem::Finder::new("generateMetadata(").into_owned())
}

fn add_metadata_nodes(
    parsed: &ParsedFile,
    file_path: &str,
    source: &str,
    augmentation: &mut NextjsAugmentation,
) {
    let bytes = source.as_bytes();
    if export_metadata_finder().find(bytes).is_none()
        && generate_metadata_finder().find(bytes).is_none()
    {
        return;
    }

    let route_name = next_route_descriptor(file_path)
        .map(|(_, route)| route)
        .or_else(|| next_layout_path(file_path))
        .unwrap_or_else(|| file_path.to_owned());
    let qn = format!("__next_metadata__{route_name}");
    let node = NodeData {
        id: ref_node_id(NodeKind::Convention, &qn),
        kind: NodeKind::Convention,
        repo: parsed.file_node.repo.clone(),
        file_path: file_path.to_owned(),
        name: route_name,
        qualified_name: Some(qn.clone()),
        external_id: Some(qn),
        signature: Some("metadata".to_owned()),
        visibility: None,
        span: parsed.file_node.span.clone(),
        is_virtual: true,
    };
    augmentation.nodes.push(node.clone());
    augmentation.edges.push(EdgeData {
        source: parsed.file_node.id,
        target: node.id,
        kind: EdgeKind::Defines,
        metadata: EdgeMetadata::default(),
        owner_file: parsed.file_node.id,
        is_cross_file: false,
    });
}

fn add_server_action_nodes(
    parsed: &ParsedFile,
    file_path: &str,
    source: &str,
    augmentation: &mut NextjsAugmentation,
) {
    // Reuse the boundary automaton: bit 1 indicates `use server` presence.
    if boundary_flags(source) & 0b10 == 0 {
        return;
    }

    let qn = format!("__next_server_action__{file_path}");
    let node = NodeData {
        id: ref_node_id(NodeKind::Service, &qn),
        kind: NodeKind::Service,
        repo: parsed.file_node.repo.clone(),
        file_path: file_path.to_owned(),
        name: "server_action".to_owned(),
        qualified_name: Some(qn.clone()),
        external_id: Some(qn),
        signature: Some("server_action".to_owned()),
        visibility: None,
        span: parsed.file_node.span.clone(),
        is_virtual: true,
    };
    augmentation.nodes.push(node.clone());
    augmentation.edges.push(EdgeData {
        source: parsed.file_node.id,
        target: node.id,
        kind: EdgeKind::Defines,
        metadata: EdgeMetadata::default(),
        owner_file: parsed.file_node.id,
        is_cross_file: false,
    });
}

fn next_route_descriptor(file_path: &str) -> Option<(&'static str, String)> {
    let normalized = file_path.trim_start_matches("./");

    if let Some(route) = normalized
        .strip_prefix("app/")
        .and_then(app_router_route_path)
    {
        return Some(route);
    }
    if let Some(route) = normalized
        .strip_prefix("pages/")
        .and_then(pages_router_route_path)
    {
        return Some(route);
    }
    None
}

fn app_router_route_path(relative: &str) -> Option<(&'static str, String)> {
    let without_ext = strip_known_extension(relative)?;
    let segments = split_segments(without_ext);
    let last = *segments.last()?;

    if last == "page" {
        return Some((
            "page",
            route_path_from_segments(&segments[..segments.len() - 1]),
        ));
    }
    if last == "route" {
        return Some((
            "route_handler",
            route_path_from_segments(&segments[..segments.len() - 1]),
        ));
    }
    None
}

fn pages_router_route_path(relative: &str) -> Option<(&'static str, String)> {
    let without_ext = strip_known_extension(relative)?;
    let segments = split_segments(without_ext);
    let first = *segments.first()?;
    let last = *segments.last()?;

    if first == "api" {
        let api_segments = if last == "index" {
            &segments[..segments.len() - 1]
        } else {
            &segments
        };
        return Some(("api", route_path_from_segments(api_segments)));
    }

    if matches!(last, "_app" | "_document" | "_error") {
        return None;
    }

    let page_segments = if last == "index" {
        &segments[..segments.len() - 1]
    } else {
        &segments
    };
    Some(("page", route_path_from_segments(page_segments)))
}

fn next_layout_path(file_path: &str) -> Option<String> {
    let normalized = file_path.trim_start_matches("./");
    let relative = normalized.strip_prefix("app/")?;
    let without_ext = strip_known_extension(relative)?;
    let segments = split_segments(without_ext);
    if *segments.last()? != "layout" {
        return None;
    }
    Some(route_path_from_segments(&segments[..segments.len() - 1]))
}

fn is_next_middleware(file_path: &str) -> bool {
    matches!(
        file_path.trim_start_matches("./"),
        "middleware.ts" | "middleware.tsx" | "src/middleware.ts" | "src/middleware.tsx"
    )
}

fn strip_known_extension(path: &str) -> Option<&str> {
    const EXTS: [&str; 6] = [".tsx", ".ts", ".jsx", ".js", ".mts", ".cts"];
    EXTS.iter().find_map(|ext| path.strip_suffix(ext))
}

fn split_segments(path: &str) -> Vec<&str> {
    path.split('/')
        .filter(|segment| !segment.is_empty())
        .collect()
}

fn route_path_from_segments(segments: &[&str]) -> String {
    let parts = segments
        .iter()
        .filter_map(|segment| normalize_route_segment(segment))
        .collect::<Vec<_>>();
    if parts.is_empty() {
        "/".to_owned()
    } else {
        format!("/{}", parts.join("/"))
    }
}

fn normalize_route_segment(segment: &str) -> Option<String> {
    if segment.is_empty()
        || (segment.starts_with('(') && segment.ends_with(')'))
        || segment.starts_with('@')
    {
        return None;
    }

    if let Some(inner) = segment
        .strip_prefix("[[...")
        .and_then(|value| value.strip_suffix("]]"))
    {
        return Some(format!("*{inner}?"));
    }
    if let Some(inner) = segment
        .strip_prefix("[...")
        .and_then(|value| value.strip_suffix(']'))
    {
        return Some(format!("*{inner}"));
    }
    if let Some(inner) = segment
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
    {
        return Some(format!(":{inner}"));
    }
    Some(segment.to_owned())
}

fn exported_http_handlers(parsed: &ParsedFile) -> Vec<gather_step_core::NodeId> {
    parsed
        .symbols
        .iter()
        .filter(|symbol| {
            symbol.node.kind == NodeKind::Function
                && symbol.node.visibility.is_some()
                && matches!(
                    symbol.node.name.as_str(),
                    "GET" | "POST" | "PUT" | "PATCH" | "DELETE" | "HEAD" | "OPTIONS"
                )
        })
        .map(|symbol| symbol.node.id)
        .collect()
}

#[cfg(test)]
mod tests {
    #![expect(clippy::needless_raw_string_hashes)]

    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::{EdgeKind, NodeKind};

    use crate::{FileEntry, Language, frameworks::detect::is_nextjs, tree_sitter::parse_file};

    use super::augment;

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> Self {
            let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-nextjs-{name}-{}-{counter}",
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
    fn detects_nextjs_from_package_manifest() {
        let dir = TestDir::new("detect");
        fs::write(
            dir.path().join("package.json"),
            r#"{ "dependencies": { "next": "15.0.0", "react": "19.0.0" } }"#,
        )
        .expect("package.json should write");

        assert!(is_nextjs(dir.path()));
    }

    #[test]
    fn app_router_page_and_layout_routes_are_emitted() {
        let dir = TestDir::new("app-router");
        fs::create_dir_all(dir.path().join("app/(marketing)/blog/[slug]"))
            .expect("app dir should exist");
        fs::write(
            dir.path().join("app/(marketing)/blog/[slug]/page.tsx"),
            "export default function Page() { return null; }\n",
        )
        .expect("page should write");
        fs::write(
            dir.path().join("app/(marketing)/blog/layout.tsx"),
            "export default function Layout({ children }) { return children; }\n",
        )
        .expect("layout should write");

        let page = parse_file(
            "sample-web",
            dir.path(),
            &FileEntry {
                path: "app/(marketing)/blog/[slug]/page.tsx".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("page file should parse");
        let layout = parse_file(
            "sample-web",
            dir.path(),
            &FileEntry {
                path: "app/(marketing)/blog/layout.tsx".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("layout file should parse");

        let page_aug = augment(&page);
        let layout_aug = augment(&layout);

        assert!(page_aug.nodes.iter().any(|node| {
            node.kind == NodeKind::Route
                && node.external_id.as_deref() == Some("__next_route__page__/blog/:slug")
        }));
        assert!(layout_aug.nodes.iter().any(|node| {
            node.kind == NodeKind::Service
                && node.external_id.as_deref() == Some("__next_layout__/blog")
        }));
    }

    #[test]
    fn route_handler_methods_serve_app_api_route() {
        let dir = TestDir::new("route-handler");
        fs::create_dir_all(dir.path().join("app/api/items")).expect("api dir should exist");
        fs::write(
            dir.path().join("app/api/items/route.ts"),
            "export async function GET() { return Response.json([]); }\n",
        )
        .expect("route handler should write");

        let parsed = parse_file(
            "sample-web",
            dir.path(),
            &FileEntry {
                path: "app/api/items/route.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("route file should parse");
        let augmentation = augment(&parsed);

        let route_node = augmentation
            .nodes
            .iter()
            .find(|node| node.kind == NodeKind::Route)
            .expect("route node should exist");
        assert_eq!(
            route_node.external_id.as_deref(),
            Some("__next_route__route_handler__/api/items")
        );
        assert!(
            augmentation
                .edges
                .iter()
                .any(|edge| { edge.kind == EdgeKind::Serves && edge.target == route_node.id })
        );
    }

    #[test]
    fn pages_routes_and_middleware_are_emitted() {
        let dir = TestDir::new("pages-router");
        fs::create_dir_all(dir.path().join("pages/docs")).expect("pages docs dir should exist");
        fs::create_dir_all(dir.path().join("pages/api/users")).expect("pages dir should exist");
        fs::write(
            dir.path().join("pages/docs/index.tsx"),
            "export default function DocsPage() { return null; }\n",
        )
        .expect("page should write");
        fs::write(
            dir.path().join("pages/api/users/[id].ts"),
            "export default function handler() { return null; }\n",
        )
        .expect("api page should write");
        fs::write(
            dir.path().join("middleware.ts"),
            "export function middleware() { return null; }\n",
        )
        .expect("middleware should write");

        let docs = parse_file(
            "sample-web",
            dir.path(),
            &FileEntry {
                path: "pages/docs/index.tsx".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("docs page should parse");
        let api = parse_file(
            "sample-web",
            dir.path(),
            &FileEntry {
                path: "pages/api/users/[id].ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("api page should parse");
        let middleware = parse_file(
            "sample-web",
            dir.path(),
            &FileEntry {
                path: "middleware.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("middleware file should parse");

        let docs_aug = augment(&docs);
        let api_aug = augment(&api);
        let middleware_aug = augment(&middleware);

        assert!(docs_aug.nodes.iter().any(|node| {
            node.kind == NodeKind::Route
                && node.external_id.as_deref() == Some("__next_route__page__/docs")
        }));
        assert!(api_aug.nodes.iter().any(|node| {
            node.kind == NodeKind::Route
                && node.external_id.as_deref() == Some("__next_route__api__/api/users/:id")
        }));
        assert!(middleware_aug.nodes.iter().any(|node| {
            node.kind == NodeKind::Service
                && node.external_id.as_deref() == Some("__next_middleware__root")
        }));
    }

    #[test]
    fn client_boundary_metadata_and_server_action_signals_are_emitted() {
        let dir = TestDir::new("signals");
        fs::create_dir_all(dir.path().join("app/dashboard")).expect("app dir should exist");
        fs::write(
            dir.path().join("app/dashboard/page.tsx"),
            r#"
'use client';

export const metadata = { title: 'Dashboard' };

export default function DashboardPage() {
  return null;
}
"#,
        )
        .expect("page should write");
        fs::write(
            dir.path().join("app/dashboard/actions.ts"),
            r#"
'use server';

export async function saveThing() {
  return true;
}
"#,
        )
        .expect("actions should write");

        let page = parse_file(
            "sample-web",
            dir.path(),
            &FileEntry {
                path: "app/dashboard/page.tsx".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("page should parse");
        let actions = parse_file(
            "sample-web",
            dir.path(),
            &FileEntry {
                path: "app/dashboard/actions.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("actions should parse");

        let page_aug = augment(&page);
        let actions_aug = augment(&actions);

        assert!(page_aug.nodes.iter().any(|node| {
            node.kind == NodeKind::Convention
                && node.external_id.as_deref()
                    == Some("__next_boundary__client__app/dashboard/page.tsx")
        }));
        assert!(page_aug.nodes.iter().any(|node| {
            node.kind == NodeKind::Convention
                && node.external_id.as_deref() == Some("__next_metadata__/dashboard")
        }));
        assert!(actions_aug.nodes.iter().any(|node| {
            node.kind == NodeKind::Service
                && node.external_id.as_deref()
                    == Some("__next_server_action__app/dashboard/actions.ts")
        }));
    }
}
