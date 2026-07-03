use gather_step_core::{EdgeData, NodeData, NodeKind};

use crate::{
    frameworks::http_client::{
        api_virtual_node, extract_object_property_value, first_top_level_argument,
        push_node_and_consumes_edge,
    },
    top_level_split::split_top_level,
    tree_sitter::{EnrichedCallSite, ParsedFile},
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReactAugmentation {
    pub nodes: Vec<NodeData>,
    pub edges: Vec<EdgeData>,
}

/// Augment a parsed React/TypeScript file with framework-specific nodes and
/// edges.
///
/// Runs the React Query hook pass (`useQuery`, `useMutation`,
/// `useInfiniteQuery`, `useSuspenseQuery` → virtual `Route` nodes with QN
/// `__api_query__<key>`) and then delegates to the shared HTTP-client passes in
/// [`crate::frameworks::http_client`] for axios/`fetch`/`this.httpService.*`
/// calls and config-driven endpoint hints.
pub fn augment(parsed: &ParsedFile) -> ReactAugmentation {
    let mut augmentation = ReactAugmentation::default();
    add_query_hook_edges(parsed, &mut augmentation);
    let http = crate::frameworks::http_client::augment(parsed);
    augmentation.nodes.extend(http.nodes);
    augmentation.edges.extend(http.edges);
    augmentation
}

// ---------------------------------------------------------------------------
// React Query hook detection
// ---------------------------------------------------------------------------

/// Detect React Query hook call sites and emit a virtual `Route` node plus a
/// `Consumes` edge for each hook whose first string argument can be extracted
/// as a query key.
///
/// Recognised hooks: `useQuery`, `useMutation`, `useInfiniteQuery`,
/// `useSuspenseQuery`.
fn add_query_hook_edges(parsed: &ParsedFile, augmentation: &mut ReactAugmentation) {
    for call in &parsed.call_sites {
        if !is_query_hook(&call.callee_name) {
            continue;
        }

        let key = extract_query_key(call);
        if key.is_empty() {
            continue;
        }

        let qualified_name = format!("__api_query__{key}");
        let node = api_virtual_node(NodeKind::Route, &qualified_name, &key, parsed, call);
        push_node_and_consumes_edge(
            node,
            call,
            parsed,
            &mut augmentation.nodes,
            &mut augmentation.edges,
            Some(900),
            "frontend_query_key",
            None,
        );
    }
}

fn extract_query_key(call: &EnrichedCallSite) -> String {
    if let Some(raw_key) = call.literal_argument.as_deref()
        && !looks_like_object_or_complex(raw_key)
    {
        return sanitize_key(raw_key);
    }

    call.raw_arguments
        .as_deref()
        .and_then(extract_query_key_from_raw_arguments)
        .unwrap_or_default()
}

/// Returns `true` for the four React Query hook names that represent data
/// fetching (and thus imply an API dependency).
fn is_query_hook(name: &str) -> bool {
    matches!(
        name,
        "useQuery" | "useMutation" | "useInfiniteQuery" | "useSuspenseQuery"
    )
}

/// Strip surrounding quotes, brackets, and whitespace from a raw query key
/// argument captured by the parser.
/// Returns `true` when the raw argument looks like an object expression,
/// function call, template literal, or other complex construct that cannot
/// be meaningfully used as a query key identifier.
///
/// Heuristics (fast, no parsing):
/// - Contains `{` → object literal / destructuring
/// - Contains `(` → nested function call
/// - Contains `:` → object property or ternary
/// - Contains `=>` → arrow function
/// - Contains `` ` `` → template literal
/// - Longer than 80 chars → almost certainly not a simple key
fn looks_like_object_or_complex(raw: &str) -> bool {
    let trimmed = raw.trim();
    trimmed.len() > 80
        || trimmed.contains('{')
        || trimmed.contains('(')
        || trimmed.contains(':')
        || trimmed.contains("=>")
        || trimmed.contains('`')
}

fn sanitize_key(raw: &str) -> String {
    raw.trim()
        .trim_matches('[')
        .trim_matches(']')
        .trim_matches('"')
        .trim_matches('\'')
        .trim()
        .to_owned()
}

fn extract_query_key_from_raw_arguments(raw_arguments: &str) -> Option<String> {
    let query_key_expr = if raw_arguments.trim_start().starts_with('{') {
        extract_object_property_value(raw_arguments, "queryKey")?
    } else {
        first_top_level_argument(raw_arguments)?
    };
    normalize_query_key_expression(query_key_expr)
}

fn normalize_query_key_expression(expr: &str) -> Option<String> {
    let trimmed = expr.trim();
    if trimmed.is_empty() || trimmed.contains("=>") || trimmed.contains('{') {
        return None;
    }

    if trimmed.starts_with('[') {
        let items = split_top_level(trimmed.trim_matches(['[', ']']), ',');
        let segments: Vec<String> = items
            .into_iter()
            .filter_map(normalize_query_key_segment)
            .collect();
        if segments.is_empty() {
            None
        } else {
            Some(segments.join("/"))
        }
    } else {
        normalize_query_key_segment(trimmed)
    }
}

fn normalize_query_key_segment(segment: &str) -> Option<String> {
    let trimmed = segment.trim();
    if trimmed.is_empty()
        || trimmed.contains('(')
        || trimmed.contains("=>")
        || trimmed.contains('`')
        || trimmed.starts_with('{')
    {
        return None;
    }

    let sanitized = trimmed
        .trim_matches('"')
        .trim_matches('\'')
        .trim()
        .replace('.', "/");
    if sanitized.is_empty() {
        None
    } else {
        Some(sanitized)
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

    // Tests in this module target the React extractor specifically; they bypass
    // repo-level framework detection and always pass `Framework::React` so the
    // unit tests remain focused on extractor behaviour.
    fn parse_file(
        repo: &str,
        repo_root: &Path,
        file: &crate::FileEntry,
    ) -> Result<crate::ParsedFile, crate::ParseError> {
        parse_file_with_frameworks(repo, repo_root, file, &[Framework::React])
    }

    static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> Self {
            let counter = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-parser-react-{name}-{}-{counter}",
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
    // React Query hooks
    // -----------------------------------------------------------------------

    #[test]
    fn use_query_hook_produces_api_query_node() {
        // `useQuery('products', fetchProducts)` → virtual Route `__api_query__products`
        // + a Consumes edge from the enclosing function to it.
        let temp_dir = TestDir::new("use-query");
        fs::write(
            temp_dir.path().join("products.tsx"),
            r#"
import { useQuery } from '@tanstack/react-query';

export function ProductList() {
  const data = useQuery('products', fetchProducts);
  return null;
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "frontend-app",
            temp_dir.path(),
            &crate::FileEntry {
                path: "products.tsx".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        let query_nodes: Vec<_> = parsed
            .nodes
            .iter()
            .filter(|node| {
                node.kind == gather_step_core::NodeKind::Route
                    && node
                        .external_id
                        .as_deref()
                        .is_some_and(|id| id.starts_with("__api_query__"))
            })
            .collect();

        assert_eq!(
            query_nodes.len(),
            1,
            "expected one api_query virtual node, got: {query_nodes:?}"
        );
        assert_eq!(
            query_nodes[0].external_id.as_deref(),
            Some("__api_query__products"),
            "query key should be 'products'"
        );

        let consumes_count = parsed
            .edges
            .iter()
            .filter(|edge| {
                edge.kind == gather_step_core::EdgeKind::Consumes
                    && edge.target == query_nodes[0].id
            })
            .count();
        assert_eq!(consumes_count, 1, "one Consumes edge expected for useQuery");
    }

    #[test]
    fn use_mutation_hook_produces_api_query_node() {
        // `useMutation('createProduct', createFn)` → virtual Route
        // `__api_query__createProduct` + Consumes edge.
        let temp_dir = TestDir::new("use-mutation");
        fs::write(
            temp_dir.path().join("create.tsx"),
            r#"
import { useMutation } from '@tanstack/react-query';

export function CreateProductForm() {
  const mutation = useMutation('createProduct', createFn);
  return null;
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "frontend-app",
            temp_dir.path(),
            &crate::FileEntry {
                path: "create.tsx".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        let query_node = parsed
            .nodes
            .iter()
            .find(|node| {
                node.kind == gather_step_core::NodeKind::Route
                    && node.external_id.as_deref() == Some("__api_query__createProduct")
            })
            .expect("useMutation should produce __api_query__createProduct node");
        assert!(
            parsed.edges.iter().any(|edge| {
                edge.kind == gather_step_core::EdgeKind::Consumes && edge.target == query_node.id
            }),
            "a Consumes edge should target the __api_query__createProduct node"
        );
    }

    #[test]
    fn use_query_object_form_produces_api_query_node() {
        let temp_dir = TestDir::new("use-query-object");
        fs::write(
            temp_dir.path().join("details.tsx"),
            r#"
import { useQuery } from '@tanstack/react-query';

export function Details({ id }: { id: string }) {
  const data = useQuery({
    queryKey: ['label-library', id],
    queryFn: () => fetchLabel(id),
  });
  return null;
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "frontend-app",
            temp_dir.path(),
            &crate::FileEntry {
                path: "details.tsx".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        let query_node = parsed
            .nodes
            .iter()
            .find(|node| {
                node.kind == gather_step_core::NodeKind::Route
                    && node.external_id.as_deref() == Some("__api_query__label-library/id")
            })
            .expect("object-form useQuery should produce a query route node");
        assert!(
            parsed.edges.iter().any(|edge| {
                edge.kind == gather_step_core::EdgeKind::Consumes && edge.target == query_node.id
            }),
            "a Consumes edge should target the object-form useQuery route node"
        );
    }

    // -----------------------------------------------------------------------
    // Axios / HTTP service wrapper calls
    // -----------------------------------------------------------------------

    #[test]
    fn axios_get_call_produces_canonical_route_node() {
        // `this.httpService.get('/api/products')` should produce a canonical
        // route node so frontend callers can join backend handlers directly.
        let temp_dir = TestDir::new("axios-get");
        fs::write(
            temp_dir.path().join("product.service.ts"),
            r#"
export class ProductService {
  constructor(private readonly httpService: HttpService) {}

  async fetchProducts() {
    return this.httpService.get('/api/products');
  }
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "frontend-app",
            temp_dir.path(),
            &crate::FileEntry {
                path: "product.service.ts".into(),
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
            .filter(|node| {
                node.kind == gather_step_core::NodeKind::Route
                    && node.external_id.as_deref() == Some("__route__GET__/api/products")
            })
            .collect();

        assert_eq!(
            route_nodes.len(),
            1,
            "expected one canonical GET route node, got: {route_nodes:?}"
        );
        assert!(
            parsed.edges.iter().any(|edge| {
                edge.kind == gather_step_core::EdgeKind::Consumes
                    && edge.target == route_nodes[0].id
                    && edge.metadata.confidence == Some(950)
            }),
            "a Consumes edge should point at the GET Route node"
        );
        assert!(
            parsed.edges.iter().any(|edge| {
                edge.kind == gather_step_core::EdgeKind::ConsumesApiFrom
                    && edge.target == route_nodes[0].id
                    && edge.metadata.confidence == Some(950)
            }),
            "a ConsumesApiFrom edge should point at the GET Route node"
        );
    }

    #[test]
    fn fetch_call_produces_canonical_get_route_node() {
        // `fetch('/api/companies')` should normalize to the same canonical GET
        // route identity a backend handler would use.
        let temp_dir = TestDir::new("fetch-call");
        fs::write(
            temp_dir.path().join("companies.ts"),
            r#"
export async function loadCompanies() {
  const response = fetch('/api/companies');
  return response;
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "frontend-app",
            temp_dir.path(),
            &crate::FileEntry {
                path: "companies.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        let fetch_nodes: Vec<_> = parsed
            .nodes
            .iter()
            .filter(|node| {
                node.kind == gather_step_core::NodeKind::Route
                    && node.external_id.as_deref() == Some("__route__GET__/api/companies")
            })
            .collect();

        assert_eq!(
            fetch_nodes.len(),
            1,
            "expected one canonical route node, got: {fetch_nodes:?}"
        );
        assert!(
            parsed
                .edges
                .iter()
                .any(|edge| edge.kind == gather_step_core::EdgeKind::Consumes
                    && edge.target == fetch_nodes[0].id
                    && edge.metadata.confidence == Some(950)),
            "a Consumes edge should point at the FETCH Route node"
        );
        assert!(
            parsed.edges.iter().any(|edge| edge.kind
                == gather_step_core::EdgeKind::ConsumesApiFrom
                && edge.target == fetch_nodes[0].id
                && edge.metadata.confidence == Some(950)),
            "a ConsumesApiFrom edge should point at the FETCH Route node"
        );
    }

    #[test]
    fn fetch_method_option_produces_canonical_post_route_node() {
        let temp_dir = TestDir::new("fetch-post-call");
        fs::write(
            temp_dir.path().join("orders.ts"),
            r#"
export async function submitOrder() {
  return fetch('/api/orders', { method: 'POST' });
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "frontend-app",
            temp_dir.path(),
            &crate::FileEntry {
                path: "orders.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        assert!(parsed.nodes.iter().any(|node| {
            node.kind == gather_step_core::NodeKind::Route
                && node.external_id.as_deref() == Some("__route__POST__/api/orders")
        }));
    }

    #[test]
    fn absolute_urls_queries_and_trailing_slashes_canonicalize_to_route_node() {
        let temp_dir = TestDir::new("fetch-route-normalization");
        fs::write(
            temp_dir.path().join("orders.ts"),
            r#"
export async function loadOrders(apiClient: any) {
  return apiClient.get('https://api.example.com/orders/?page=1#top');
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "frontend-app",
            temp_dir.path(),
            &crate::FileEntry {
                path: "orders.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        assert!(parsed.nodes.iter().any(|node| {
            node.kind == gather_step_core::NodeKind::Route
                && node.external_id.as_deref() == Some("__route__GET__/orders")
        }));
    }

    // -----------------------------------------------------------------------
    // Config-driven endpoint detection
    // -----------------------------------------------------------------------

    #[test]
    fn config_endpoint_constant_produces_canonical_route_node() {
        let temp_dir = TestDir::new("config-endpoint");
        fs::write(
            temp_dir.path().join("api-client.ts"),
            r#"
const config = { apiPath: { gw: { orders: { create: '/orders' } } } };

export class ApiClient {
  async createOrder(payload: unknown) {
    return this.apiClient.post(config.apiPath.gw.orders.create, payload);
  }
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "frontend-app",
            temp_dir.path(),
            &crate::FileEntry {
                path: "api-client.ts".into(),
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
            .filter(|node| {
                node.kind == gather_step_core::NodeKind::Route
                    && node.external_id.as_deref() == Some("__route__POST__/orders")
            })
            .collect();

        assert!(
            !route_nodes.is_empty(),
            "expected a canonical route node from the config.apiPath constant"
        );
        assert!(
            parsed.nodes.iter().all(|node| {
                node.external_id.as_deref()
                    != Some("__route__POST__/config.apiPath.gw.orders.create")
            }),
            "config expressions must not be canonicalized from raw expression text"
        );
        assert!(
            parsed
                .edges
                .iter()
                .any(|edge| edge.kind == gather_step_core::EdgeKind::Consumes
                    && route_nodes.iter().any(|node| node.id == edge.target)
                    && edge.metadata.confidence == Some(900)),
            "a Consumes edge should point at the canonical config Route node"
        );
    }

    #[test]
    fn imported_route_constants_produce_canonical_route_node() {
        let temp_dir = TestDir::new("imported-route-constants");
        fs::write(
            temp_dir.path().join("route_constants.ts"),
            r#"
export const route_constants = {
  orders: {
    create: '/orders',
  },
};
"#,
        )
        .expect("fixture should write");
        fs::write(
            temp_dir.path().join("api-client.ts"),
            r#"
import { route_constants } from './route_constants';

export class ApiClient {
  async createOrder(payload: unknown) {
    return this.apiClient.post(route_constants.orders.create, payload);
  }
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "frontend-app",
            temp_dir.path(),
            &crate::FileEntry {
                path: "api-client.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        assert!(parsed.nodes.iter().any(|node| {
            node.kind == gather_step_core::NodeKind::Route
                && node.external_id.as_deref() == Some("__route__POST__/orders")
        }));
        assert!(
            parsed
                .edges
                .iter()
                .any(|edge| edge.kind == gather_step_core::EdgeKind::Consumes
                    && edge.metadata.confidence == Some(900)
                    && parsed.nodes.iter().any(|node| {
                        node.id == edge.target
                            && node.external_id.as_deref() == Some("__route__POST__/orders")
                    })),
            "imported route constants should produce a canonical route consumes edge"
        );
    }

    #[test]
    fn default_imported_route_constants_produce_canonical_route_node() {
        let temp_dir = TestDir::new("default-imported-route-constants");
        fs::write(
            temp_dir.path().join("route_constants.ts"),
            r#"
const route_constants = {
  orders: {
    create: '/orders',
  },
};

export default route_constants;
"#,
        )
        .expect("fixture should write");
        fs::write(
            temp_dir.path().join("api-client.ts"),
            r#"
import route_constants from './route_constants';

export class ApiClient {
  async createOrder(payload: unknown) {
    return this.apiClient.post(route_constants.orders.create, payload);
  }
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "frontend-app",
            temp_dir.path(),
            &crate::FileEntry {
                path: "api-client.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        assert!(parsed.nodes.iter().any(|node| {
            node.kind == gather_step_core::NodeKind::Route
                && node.external_id.as_deref() == Some("__route__POST__/orders")
        }));
    }

    #[test]
    fn default_reexported_route_constants_produce_canonical_route_node() {
        let temp_dir = TestDir::new("default-reexported-route-constants");
        fs::write(
            temp_dir.path().join("route_constants.ts"),
            r#"
export default {
  orders: {
    create: '/orders',
  },
};
"#,
        )
        .expect("fixture should write");
        fs::write(
            temp_dir.path().join("route_barrel.ts"),
            r#"
export { default } from './route_constants';
"#,
        )
        .expect("fixture should write");
        fs::write(
            temp_dir.path().join("api-client.ts"),
            r#"
import route_constants from './route_barrel';

export class ApiClient {
  async createOrder(payload: unknown) {
    return this.apiClient.post(route_constants.orders.create, payload);
  }
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "frontend-app",
            temp_dir.path(),
            &crate::FileEntry {
                path: "api-client.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        assert!(parsed.nodes.iter().any(|node| {
            node.kind == gather_step_core::NodeKind::Route
                && node.external_id.as_deref() == Some("__route__POST__/orders")
        }));
    }

    #[test]
    fn unresolved_config_endpoint_stays_non_canonical() {
        let temp_dir = TestDir::new("config-unresolved");
        fs::write(
            temp_dir.path().join("api-client.ts"),
            r#"
export class ApiClient {
  async createOrder(payload: unknown) {
    return this.config.apiPath.gw.orders.create.post(payload);
  }
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "frontend-app",
            temp_dir.path(),
            &crate::FileEntry {
                path: "api-client.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        assert!(
            parsed
                .nodes
                .iter()
                .all(|node| node.external_id.as_deref() != Some("__route__POST__/orders")),
            "unresolved config paths must not invent canonical route ids"
        );
        assert!(
            parsed.nodes.iter().any(|node| {
                node.kind == gather_step_core::NodeKind::Route
                    && node.external_id.as_deref() == Some("__api_config__gw/orders/create")
            }),
            "unresolved config paths should still emit a non-canonical hint node"
        );
    }
}
