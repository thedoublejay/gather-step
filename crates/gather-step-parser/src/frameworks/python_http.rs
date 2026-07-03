//! Python HTTP-client consumer augmentation (v5.10).
//!
//! Emits the same route-convergence vocabulary as the frontend/gateway HTTP
//! passes — virtual `NodeKind::Route` nodes keyed `__route__<METHOD>__<path>`
//! with `Consumes` + `ConsumesApiFrom` edges — so a Python service calling
//! another service over HTTP joins the same `Route` node the provider `Serves`,
//! making a cross-language request round trip visible end to end.
//!
//! Detection is signature-based and deliberately conservative (the project's
//! confidence-banding goal): a call counts only when its callee is an HTTP verb
//! (`get`/`post`/`put`/`patch`/`delete`/`head`/`options`) invoked through a
//! recognised client receiver (`requests`/`httpx`/`aiohttp` modules, or a
//! `client`/`session`/`http`/`api` instance with a URL-shaped literal), and only
//! when the URL argument is a statically resolvable string — a quoted literal, a
//! module-level string constant, or a `+`-concatenation of those. Dynamic URLs
//! (f-strings, variables, keyword-only `url=`) are skipped rather than
//! fabricated.
//!
//! Known recall gaps mirror the Kafka pack: module-level calls (no owning
//! function) are not captured, since call sites require an owner, and aliased
//! imports (`import requests as r`) are not tracked.

use gather_step_core::{EdgeKind, NodeKind, canonical_route_path, route_qn};

use crate::{
    frameworks::http_client::{api_virtual_node, push_node_and_consumes_edge},
    top_level_split::split_top_level,
    tree_sitter::{EnrichedCallSite, ParsedFile},
};
use gather_step_core::{EdgeData, NodeData, ResolverStrategy};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PythonHttpAugmentation {
    pub nodes: Vec<NodeData>,
    pub edges: Vec<EdgeData>,
}

#[must_use]
pub fn augment(parsed: &ParsedFile) -> PythonHttpAugmentation {
    let mut augmentation = PythonHttpAugmentation::default();
    for call_site in &parsed.call_sites {
        if let Some(route) = resolve_consumed_route(parsed, call_site) {
            emit_route(parsed, call_site, &route, &mut augmentation);
        }
    }
    augmentation
}

struct ConsumedRoute {
    method: &'static str,
    url: String,
    literal_only: bool,
}

/// The route consumed by this call site, if it is a resolvable HTTP-client call.
fn resolve_consumed_route(
    parsed: &ParsedFile,
    call_site: &EnrichedCallSite,
) -> Option<ConsumedRoute> {
    let (method, receiver) = http_method_and_receiver(call_site)?;
    let (url, literal_only) = resolve_url(parsed, call_site)?;
    if !receiver_is_http_client(parsed, receiver, &url) {
        return None;
    }
    Some(ConsumedRoute {
        method,
        url,
        literal_only,
    })
}

/// Upper-cased HTTP method for a call site, or `None` when the call is not an
/// HTTP-client verb invoked through a dotted receiver.
fn http_method_and_receiver(call_site: &EnrichedCallSite) -> Option<(&'static str, &str)> {
    let method = match call_site.callee_name.as_str() {
        "get" => "GET",
        "post" => "POST",
        "put" => "PUT",
        "patch" => "PATCH",
        "delete" => "DELETE",
        "head" => "HEAD",
        "options" => "OPTIONS",
        _ => return None,
    };
    // Require a dotted receiver (`requests.get`, `client.post`); a bare `get(…)`
    // has no receiver and is never an HTTP client call.
    let hint = call_site.callee_qualified_hint.as_deref()?;
    let (receiver, _operation) = hint.rsplit_once('.')?;
    Some((method, receiver))
}

/// Whether the dotted receiver path names a recognised HTTP client.
///
/// Imported module/client provenance is accepted directly. Generic instance
/// names (`client`, `session`, `api`, `http`) require a URL-shaped target so
/// data-store calls such as `redis_client.get("key")` are not fabricated into
/// HTTP routes.
fn receiver_is_http_client(parsed: &ParsedFile, receiver: &str, url: &str) -> bool {
    receiver_has_http_import(parsed, receiver)
        || receiver.split('.').any(segment_is_known_http_module)
        || (url_looks_like_http_target(url)
            && receiver.split('.').any(segment_is_generic_http_client))
}

fn receiver_has_http_import(parsed: &ParsedFile, receiver: &str) -> bool {
    let root = receiver.split('.').next().unwrap_or(receiver);
    parsed.import_bindings.iter().any(|binding| {
        binding.local_name == root
            && (source_is_http_client(&binding.source)
                || binding
                    .imported_name
                    .as_deref()
                    .is_some_and(imported_name_is_http_client))
    })
}

fn source_is_http_client(source: &str) -> bool {
    matches!(source, "requests" | "httpx" | "aiohttp")
}

fn imported_name_is_http_client(name: &str) -> bool {
    matches!(name, "ClientSession" | "AsyncClient" | "Client")
}

fn segment_is_known_http_module(segment: &str) -> bool {
    segment.eq_ignore_ascii_case("requests")
        || segment.eq_ignore_ascii_case("httpx")
        || segment.eq_ignore_ascii_case("aiohttp")
}

fn segment_is_generic_http_client(segment: &str) -> bool {
    segment.eq_ignore_ascii_case("session")
        || segment.eq_ignore_ascii_case("api")
        // Substring checks cover `http_client`, `apiClient`, `self.client`.
        || contains_ignore_ascii_case(segment, "http")
        || contains_ignore_ascii_case(segment, "client")
}

fn url_looks_like_http_target(url: &str) -> bool {
    let url = url.trim();
    url.starts_with('/') || url.starts_with("http://") || url.starts_with("https://")
}

/// Resolve the first positional argument of a client call to a static URL.
/// Returns the URL and whether it resolved from string literals alone (used to
/// band edge confidence). Keyword-only `url=` arguments are intentionally
/// skipped.
fn resolve_url(parsed: &ParsedFile, call_site: &EnrichedCallSite) -> Option<(String, bool)> {
    let raw = call_site.raw_arguments.as_deref()?;
    let first = split_top_level(raw, ',').into_iter().next()?;
    resolve_url_expression(parsed, first)
}

/// Resolve a URL expression: a single literal/constant, or a `+`-concatenation
/// of literals/constants. Returns `None` if any component is dynamic.
fn resolve_url_expression(parsed: &ParsedFile, expression: &str) -> Option<(String, bool)> {
    let mut url = String::new();
    let mut literal_only = true;
    for atom in split_top_level(expression, '+') {
        let (value, is_literal) = resolve_atom(parsed, atom)?;
        url.push_str(&value);
        literal_only &= is_literal;
    }
    let url = url.trim();
    (!url.is_empty()).then(|| (url.to_owned(), literal_only))
}

/// Resolve a single atom to its string value: a quoted literal (`is_literal`
/// true) or a module-level string constant (`is_literal` false). Returns `None`
/// for dynamic values (variables, f-strings) so junk routes are never invented.
fn resolve_atom(parsed: &ParsedFile, atom: &str) -> Option<(String, bool)> {
    let atom = atom.trim();
    if let Some(literal) = string_literal(atom) {
        return Some((literal, true));
    }
    parsed
        .constant_strings
        .get(atom)
        .map(|value| (value.clone(), false))
}

/// Inner text of a plain quoted string literal, or `None` for prefixed strings
/// (`f"…"`, `b"…"`, `r"…"`) and non-strings.
fn string_literal(atom: &str) -> Option<String> {
    let bytes = atom.as_bytes();
    if bytes.len() < 2 {
        return None;
    }
    let quote = bytes[0];
    if (quote == b'"' || quote == b'\'') && bytes[bytes.len() - 1] == quote {
        return Some(atom[1..atom.len() - 1].to_owned());
    }
    None
}

fn emit_route(
    parsed: &ParsedFile,
    call_site: &EnrichedCallSite,
    route: &ConsumedRoute,
    augmentation: &mut PythonHttpAugmentation,
) {
    let qualified_name = route_qn(route.method, &route.url);
    let display = canonical_route_path(&route.url);
    let (confidence, resolver) = if route.literal_only {
        (950, ResolverStrategy::FrontendLiteral)
    } else {
        (900, ResolverStrategy::FrontendConstant)
    };
    let node = api_virtual_node(
        NodeKind::Route,
        &qualified_name,
        &display,
        parsed,
        call_site,
    );
    push_node_and_consumes_edge(
        node,
        call_site,
        parsed,
        &mut augmentation.nodes,
        &mut augmentation.edges,
        Some(confidence),
        resolver.as_str(),
        Some(EdgeKind::ConsumesApiFrom),
    );
}

/// Allocation-free case-insensitive substring test (`needle` must be ASCII).
fn contains_ignore_ascii_case(haystack: &str, needle: &str) -> bool {
    let (haystack, needle) = (haystack.as_bytes(), needle.as_bytes());
    if needle.is_empty() {
        return true;
    }
    if needle.len() > haystack.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle))
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
                "gather-step-parser-pyhttp-{name}-{}-{counter}",
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

    fn parse(dir: &TestDir, file: &str, body: &str) -> crate::tree_sitter::ParsedFile {
        fs::write(dir.path().join(file), body).expect("fixture should write");
        parse_file_with_frameworks(
            "ingestion",
            dir.path(),
            &crate::FileEntry {
                path: file.into(),
                language: Language::Python,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &[Framework::PythonHttp],
        )
        .expect("fixture should parse")
    }

    fn route_ids(parsed: &crate::tree_sitter::ParsedFile) -> Vec<String> {
        let mut ids = parsed
            .nodes
            .iter()
            .filter(|node| node.kind == NodeKind::Route)
            .map(|node| node.external_id.clone().unwrap_or_default())
            .collect::<Vec<_>>();
        ids.sort();
        ids.dedup();
        ids
    }

    fn edge_count(parsed: &crate::tree_sitter::ParsedFile, kind: EdgeKind) -> usize {
        parsed.edges.iter().filter(|edge| edge.kind == kind).count()
    }

    #[test]
    fn requests_post_literal_path_consumes_route() {
        let dir = TestDir::new("requests-post");
        let parsed = parse(
            &dir,
            "client.py",
            r#"
import requests


def create_item(payload):
    return requests.post("/items", json=payload)
"#,
        );

        assert_eq!(route_ids(&parsed), vec!["__route__POST__/items".to_owned()]);
        assert_eq!(edge_count(&parsed, EdgeKind::ConsumesApiFrom), 1);
    }

    #[test]
    fn httpx_get_constant_plus_literal_concatenation_resolves() {
        let dir = TestDir::new("httpx-concat");
        let parsed = parse(
            &dir,
            "client.py",
            r#"
import httpx

BASE = "http://gateway:8000"


def load_items():
    return httpx.get(BASE + "/items")
"#,
        );

        assert_eq!(route_ids(&parsed), vec!["__route__GET__/items".to_owned()]);
        assert_eq!(edge_count(&parsed, EdgeKind::ConsumesApiFrom), 1);
    }

    #[test]
    fn session_put_instance_receiver_consumes_route() {
        let dir = TestDir::new("session-put");
        let parsed = parse(
            &dir,
            "client.py",
            r#"
def update(session, payload):
    return session.put("http://svc/api/v1/orders", json=payload)
"#,
        );

        assert_eq!(
            route_ids(&parsed),
            vec!["__route__PUT__/api/v1/orders".to_owned()]
        );
        assert_eq!(edge_count(&parsed, EdgeKind::ConsumesApiFrom), 1);
    }

    #[test]
    fn awaited_client_post_consumes_route() {
        let dir = TestDir::new("await-client");
        let parsed = parse(
            &dir,
            "client.py",
            r#"
async def create(client, payload):
    return await client.post("/documents", json=payload)
"#,
        );

        assert_eq!(
            route_ids(&parsed),
            vec!["__route__POST__/documents".to_owned()]
        );
        assert_eq!(edge_count(&parsed, EdgeKind::ConsumesApiFrom), 1);
    }

    #[test]
    fn self_attribute_client_receiver_consumes_route() {
        let dir = TestDir::new("self-client");
        let parsed = parse(
            &dir,
            "service.py",
            r#"
class Gateway:
    async def fetch(self, item_id):
        return await self.client.get("/items/list")
"#,
        );

        assert_eq!(
            route_ids(&parsed),
            vec!["__route__GET__/items/list".to_owned()]
        );
    }

    #[test]
    fn non_http_get_calls_are_ignored() {
        let dir = TestDir::new("negative");
        let parsed = parse(
            &dir,
            "negative.py",
            r#"
def handler(config, cache, os):
    config.get("timeout")
    cache.get("key")
    os.environ.get("PATH")
"#,
        );

        assert!(route_ids(&parsed).is_empty());
        assert_eq!(edge_count(&parsed, EdgeKind::ConsumesApiFrom), 0);
    }

    #[test]
    fn datastore_receivers_with_plain_keys_are_ignored() {
        let dir = TestDir::new("datastore-negative");
        let parsed = parse(
            &dir,
            "negative.py",
            r#"
def handler(redis_client, db_session):
    redis_client.get("user:123")
    db_session.delete("orders")
"#,
        );

        assert!(route_ids(&parsed).is_empty());
        assert_eq!(edge_count(&parsed, EdgeKind::ConsumesApiFrom), 0);
    }

    #[test]
    fn dynamic_urls_are_skipped() {
        let dir = TestDir::new("dynamic");
        let parsed = parse(
            &dir,
            "dynamic.py",
            r#"
def load(client, path, item_id):
    client.get(path)
    client.get(f"/items/{item_id}")
"#,
        );

        assert!(route_ids(&parsed).is_empty());
        assert_eq!(edge_count(&parsed, EdgeKind::ConsumesApiFrom), 0);
    }
}
