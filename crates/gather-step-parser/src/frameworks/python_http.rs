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
//! when the URL argument is a safely resolvable string — literals, single-
//! assignment local/module constants, f-strings, adjacent literals, or `+`
//! concatenation. Dynamic values that cannot preserve a stable route shape are
//! skipped rather than fabricated.
//!
//! Known recall gaps mirror the Kafka pack: module-level calls (no owning
//! function) are not captured, since call sites require an owner, and aliased
//! imports (`import requests as r`) are not tracked.

use gather_step_core::{
    EdgeKind, NodeKind, SourceScope, canonical_route_path, classify_source_scope, route_qn,
};
use std::path::Path;

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
    // Test HTTP clients prove endpoint coverage; they are not production
    // service consumers. Until source-scope evidence has its own graph surface,
    // omitting these edges is safer than promoting tests into dependency and
    // caller reports.
    if is_test_source(call_site.source_path.as_path()) {
        return None;
    }
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

fn is_test_source(path: &Path) -> bool {
    classify_source_scope(path.to_string_lossy().as_ref()) == SourceScope::Test
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
    resolve_url_expression(parsed, call_site, first, 0)
}

/// Resolve a URL expression: a single literal/constant, or a `+`-concatenation
/// of literals/constants. Returns `None` if any component is dynamic.
fn resolve_url_expression(
    parsed: &ParsedFile,
    call_site: &EnrichedCallSite,
    expression: &str,
    depth: usize,
) -> Option<(String, bool)> {
    if depth > 4 {
        return None;
    }
    let mut url = String::new();
    let mut literal_only = true;
    for atom in split_top_level(expression, '+') {
        let (value, is_literal) = resolve_atom(parsed, call_site, atom, depth)?;
        url.push_str(&value);
        literal_only &= is_literal;
    }
    let url = url.trim();
    (!url.is_empty()).then(|| (url.to_owned(), literal_only))
}

/// Resolve a single atom to its string value: a quoted literal (`is_literal`
/// true) or a same-function/module string constant (`is_literal` false).
/// Returns `None` for dynamic values so junk routes are never invented.
fn resolve_atom(
    parsed: &ParsedFile,
    call_site: &EnrichedCallSite,
    atom: &str,
    depth: usize,
) -> Option<(String, bool)> {
    let atom = atom.trim();
    if let Some(literal) = adjacent_string_literals(atom) {
        return Some((literal, true));
    }
    if let Some(literal) = string_literal(atom) {
        return Some((literal, true));
    }
    if let Some(template) = f_string(parsed, call_site, atom, depth) {
        return Some((template, false));
    }
    if let Some(assignment) = single_assignment(parsed, call_site, atom) {
        return resolve_url_expression(parsed, call_site, &assignment, depth + 1)
            .map(|(value, _)| (value, false));
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

fn adjacent_string_literals(expression: &str) -> Option<String> {
    let mut rest = expression.trim();
    let mut value = String::new();
    let mut count = 0_usize;
    while !rest.is_empty() {
        let quote = rest.as_bytes().first().copied()?;
        if !matches!(quote, b'\'' | b'"') {
            return None;
        }
        let mut escaped = false;
        let mut end = None;
        for (index, byte) in rest.as_bytes().iter().copied().enumerate().skip(1) {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == quote {
                end = Some(index);
                break;
            }
        }
        let end = end?;
        value.push_str(&rest[1..end]);
        count += 1;
        rest = rest[end + 1..].trim_start();
    }
    (count > 1).then_some(value)
}

fn f_string(
    parsed: &ParsedFile,
    call_site: &EnrichedCallSite,
    expression: &str,
    depth: usize,
) -> Option<String> {
    let expression = expression.trim();
    let quote_offset = expression
        .char_indices()
        .find_map(|(index, character)| matches!(character, '\'' | '"').then_some(index))?;
    let prefix = &expression[..quote_offset];
    if !prefix
        .chars()
        .any(|character| matches!(character, 'f' | 'F'))
    {
        return None;
    }
    let quote = expression.as_bytes()[quote_offset];
    if expression.as_bytes().last().copied() != Some(quote) {
        return None;
    }
    let template = &expression[quote_offset + 1..expression.len() - 1];
    let mut output = String::new();
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        output.push_str(&rest[..open]);
        let expression = &rest[open + 1..];
        let close = expression.find('}')?;
        let interpolation = expression[..close]
            .split(['!', ':'])
            .next()
            .unwrap_or_default()
            .trim();
        if interpolation.is_empty() {
            return None;
        }
        if let Some((resolved, _)) = resolve_atom(parsed, call_site, interpolation, depth + 1) {
            output.push_str(&resolved);
        } else {
            // An unresolved leading interpolation could be a host/base URL or
            // an arbitrary path prefix. Emitting it as `/:name/...` invents a
            // route. Once a stable literal prefix exists, preserving a later
            // interpolation as a route parameter is safe.
            if output.is_empty() {
                return None;
            }
            let parameter = interpolation
                .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
                .rfind(|part| !part.is_empty())?;
            output.push(':');
            output.push_str(parameter);
        }
        rest = &expression[close + 1..];
    }
    output.push_str(rest);
    Some(output)
}

fn single_assignment(
    parsed: &ParsedFile,
    call_site: &EnrichedCallSite,
    name: &str,
) -> Option<String> {
    if name.is_empty()
        || !name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return None;
    }
    let lines = parsed.source.lines().collect::<Vec<_>>();
    let owner_span = parsed
        .symbols
        .iter()
        .find(|symbol| symbol.node.id == call_site.owner_id)
        .and_then(|symbol| symbol.node.span.as_ref());
    let scoped_lines = owner_span.map_or_else(
        || lines.as_slice(),
        |span| {
            let start = usize::try_from(span.line_start.saturating_sub(1)).unwrap_or(0);
            let call_line = call_site
                .span
                .as_ref()
                .map_or(span.line_end(), |call_span| call_span.line_start);
            let end = usize::try_from(call_line.saturating_sub(1))
                .unwrap_or(lines.len())
                .min(lines.len());
            &lines[start.min(end)..end]
        },
    );
    let owner_indent = scoped_lines
        .first()
        .map_or(0, |line| line.len().saturating_sub(line.trim_start().len()));
    let body_indent = scoped_lines
        .iter()
        .skip(1)
        .filter(|line| !line.trim().is_empty() && !line.trim_start().starts_with('#'))
        .map(|line| line.len().saturating_sub(line.trim_start().len()))
        .filter(|indent| *indent > owner_indent)
        .min();
    let mut values = scoped_lines.iter().filter_map(|line| {
        let indent = line.len().saturating_sub(line.trim_start().len());
        if body_indent.is_some_and(|expected| indent != expected) {
            return None;
        }
        let line = line.trim();
        let rest = line.strip_prefix(name)?;
        if rest
            .chars()
            .next()
            .is_some_and(|character| character.is_ascii_alphanumeric() || character == '_')
        {
            return None;
        }
        let value = rest.trim_start().strip_prefix('=')?.trim();
        (!value.is_empty()).then(|| value.to_owned())
    });
    let value = values.next()?;
    values.next().is_none().then_some(value)
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
        let path = dir.path().join(file);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("fixture parent should create");
        }
        fs::write(path, body).expect("fixture should write");
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
    fn local_string_and_f_string_preserve_static_route_shape() {
        let dir = TestDir::new("dynamic");
        let parsed = parse(
            &dir,
            "dynamic.py",
            r#"
def load(client, path, item_id):
    base_url = "https://assets.example"
    client.get(path)
    client.get(f"{base_url}/items/{item_id}?include=owner#details")
"#,
        );

        assert_eq!(
            route_ids(&parsed),
            vec!["__route__GET__/items/:item_id".to_owned()]
        );
        assert_eq!(edge_count(&parsed, EdgeKind::ConsumesApiFrom), 1);
    }

    #[test]
    fn assignments_do_not_cross_function_boundaries() {
        let dir = TestDir::new("function-local-assignment");
        let parsed = parse(
            &dir,
            "client.py",
            r#"
import httpx


def first():
    target = "/first"
    return httpx.get(target)


def second():
    target = "/second"
    return httpx.get(target)
"#,
        );

        assert_eq!(
            route_ids(&parsed),
            vec![
                "__route__GET__/first".to_owned(),
                "__route__GET__/second".to_owned(),
            ]
        );
    }

    #[test]
    fn unresolved_leading_f_string_value_does_not_invent_route_prefix() {
        let dir = TestDir::new("dynamic-leading-template");
        let parsed = parse(
            &dir,
            "client.py",
            r#"
import httpx


def load(base_url):
    return httpx.get(f"{base_url}/items")
"#,
        );

        assert!(route_ids(&parsed).is_empty());
    }

    #[test]
    fn adjacent_literals_and_concatenated_locals_form_one_clean_route() {
        let dir = TestDir::new("adjacent-literals");
        let parsed = parse(
            &dir,
            "client.py",
            r#"
import httpx


def load_assets():
    return httpx.get("/api" "/assets" "?sort=desc")
"#,
        );

        assert_eq!(
            route_ids(&parsed),
            vec!["__route__GET__/api/assets".to_owned()]
        );
        assert_eq!(edge_count(&parsed, EdgeKind::ConsumesApiFrom), 1);
    }

    #[test]
    fn test_client_calls_do_not_become_production_consumers() {
        let dir = TestDir::new("test-client-scope");
        let parsed = parse(
            &dir,
            "tests/test_assets.py",
            r#"
from starlette.testclient import TestClient


def test_delete_asset(test_client: TestClient):
    response = test_client.delete("/api/v1/assets/asset-1")
    assert response.status_code == 204
"#,
        );

        assert!(route_ids(&parsed).is_empty());
        assert_eq!(edge_count(&parsed, EdgeKind::ConsumesApiFrom), 0);
    }
}
