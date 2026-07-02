use gather_step_core::{
    EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, ResolverStrategy, canonical_route_path,
    ref_node_id, route_qn,
};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::{
    top_level_split::split_top_level,
    traverse::{FileEntry, Language},
    tree_sitter::{EnrichedCallSite, ParsedFile, parse_file},
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct HttpClientAugmentation {
    pub nodes: Vec<NodeData>,
    pub edges: Vec<EdgeData>,
}

/// Augment a parsed TypeScript/JavaScript file with HTTP-client dependency
/// edges. Two extraction passes are run in order:
///
/// 1. Axios/`fetch`/`this.httpService.*` HTTP method calls (`get`, `post`,
///    `put`, `patch`, `delete`, `fetch`) → canonical virtual `Route` nodes with
///    QN `__route__<METHOD>__<path>` when method+path are known.
/// 2. Config-driven endpoint hints (`config.apiPath.*`, `API_BASE`, etc.)
///    → canonical virtual `Route` nodes when method+path resolve, otherwise
///    hint `Route` nodes with QN `__api_config__<endpoint>`.
///
/// All resulting nodes carry `is_virtual: true` and a `Consumes` +
/// `ConsumesApiFrom` edge from the call-site owner to the virtual route node.
///
/// These passes are framework-agnostic: they run for React frontends and for
/// `NestJS` gateway repos that axios-forward requests to backend services.
pub(crate) fn augment(parsed: &ParsedFile) -> HttpClientAugmentation {
    let mut augmentation = HttpClientAugmentation::default();
    let mut import_cache = FxHashMap::default();
    add_service_call_edges(parsed, &mut augmentation, &mut import_cache);
    add_config_endpoint_edges(parsed, &mut augmentation, &mut import_cache);
    augmentation
}

// ---------------------------------------------------------------------------
// Axios / fetch service call detection
// ---------------------------------------------------------------------------

/// Detect HTTP method calls made through axios, a typed HTTP client wrapper,
/// or raw `fetch`. Emits a virtual `Route` node plus a `Consumes` edge for
/// each call whose endpoint path can be extracted as the first string argument.
///
/// HTTP method calls (`get`, `post`, `put`, `patch`, `delete`) are only
/// counted when the qualified receiver contains a recognised service marker
/// (`axios`, `http`, `api`, `service`, `client`). This avoids false positives
/// from unrelated `.get()` calls on plain objects.
///
/// Direct `fetch(…)` calls are always included regardless of receiver.
fn add_service_call_edges(
    parsed: &ParsedFile,
    augmentation: &mut HttpClientAugmentation,
    import_cache: &mut FxHashMap<String, Option<ParsedFile>>,
) {
    for call in &parsed.call_sites {
        let Some(method_upper) = http_method_for_call(parsed, call) else {
            continue;
        };

        let Some(target) = resolve_frontend_route_target(parsed, call, &method_upper, import_cache)
        else {
            continue;
        };

        let node = api_virtual_node(
            NodeKind::Route,
            &target.qualified_name,
            &target.display_name,
            parsed,
            call,
        );
        push_node_and_consumes_edge(
            node,
            call,
            parsed,
            &mut augmentation.nodes,
            &mut augmentation.edges,
            target.confidence,
            &target.resolver,
            Some(EdgeKind::ConsumesApiFrom),
        );
    }
}

/// Map a call site to an HTTP method string (already upper-cased) when the
/// call represents an HTTP operation, or return `None` to skip it.
fn http_method_for_call(parsed: &ParsedFile, call: &EnrichedCallSite) -> Option<String> {
    // Raw `fetch(url, { method: 'POST' })` — default to GET when method is absent.
    if call.callee_name == "fetch" {
        return Some(fetch_method_for_call(parsed, call).unwrap_or_else(|| "GET".to_owned()));
    }

    // Named HTTP methods only when invoked through a recognised HTTP client.
    let method = match call.callee_name.as_str() {
        "get" => "GET".to_owned(),
        "post" => "POST".to_owned(),
        "put" => "PUT".to_owned(),
        "patch" => "PATCH".to_owned(),
        "delete" => "DELETE".to_owned(),
        _ => return None,
    };

    // Require a qualified receiver that looks like an HTTP client to avoid
    // matching arbitrary `.get()`/`.post()` calls on unrelated objects.
    let hint = call.callee_qualified_hint.as_deref()?;
    if is_http_client_receiver(hint) {
        Some(method)
    } else {
        None
    }
}

/// Heuristic: does the dotted receiver path look like an HTTP client?
///
/// We check each segment of the dotted path for known service markers. For
/// example `this.httpService.get` → `["this", "httpService", "get"]` → the
/// `httpService` segment contains `http` → `true`.
fn is_http_client_receiver(qualified_hint: &str) -> bool {
    qualified_hint.split('.').any(is_http_client_segment)
}

fn is_http_client_segment(segment: &str) -> bool {
    // Use byte-level contains comparisons to stay allocation-free. The segment
    // is already lowercase-ish TypeScript identifiers so ASCII comparison is
    // sufficient in practice.
    let s = segment;
    s.eq_ignore_ascii_case("axios")
        || s.eq_ignore_ascii_case("http")
        || s.eq_ignore_ascii_case("api")
        || s.eq_ignore_ascii_case("client")
        // Substring checks for compound names like `httpService`, `apiClient`,
        // `httpClient`.
        || contains_ignore_ascii_case(s, "axios")
        || contains_ignore_ascii_case(s, "http")
        || contains_ignore_ascii_case(s, "client")
        || (contains_ignore_ascii_case(s, "api") && !contains_ignore_ascii_case(s, "rapid"))
        || contains_ignore_ascii_case(s, "fetch")
        || contains_ignore_ascii_case(s, "request")
    // Note: bare "service" is intentionally excluded. NestJS `*Service`
    // classes (e.g., `productService.get(id)`) would false-positive because
    // `.get()` is also an HTTP method name.  Compound names that include
    // `http` or `api` (`httpService`, `apiService`) are already covered by
    // the `http`/`api` substring checks above.
}

/// ASCII-case-insensitive substring containment check that avoids allocating a
/// lowercase copy. Operates byte-by-byte; safe for ASCII identifiers.
fn contains_ignore_ascii_case(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    let needle_bytes = needle.as_bytes();
    let haystack_bytes = haystack.as_bytes();
    if needle_bytes.len() > haystack_bytes.len() {
        return false;
    }
    haystack_bytes.windows(needle_bytes.len()).any(|window| {
        window
            .iter()
            .zip(needle_bytes)
            .all(|(h, n)| h.eq_ignore_ascii_case(n))
    })
}

// ---------------------------------------------------------------------------
// Config-driven endpoint detection
// ---------------------------------------------------------------------------

/// Detect call sites whose qualified receiver hint contains a config-based API
/// endpoint pattern (e.g., `config.apiPath.gw.orders`). Emits a virtual
/// `Route` node with QN `__api_config__<endpoint>` plus a `Consumes` edge.
///
/// Recognised config root segments: `config.apiPath`, `config.api`,
/// `API_BASE`, `apiUrl`.
fn add_config_endpoint_edges(
    parsed: &ParsedFile,
    augmentation: &mut HttpClientAugmentation,
    import_cache: &mut FxHashMap<String, Option<ParsedFile>>,
) {
    for call in &parsed.call_sites {
        let Some(method_upper) = http_method_for_call(parsed, call) else {
            continue;
        };
        if resolve_frontend_route_target(parsed, call, &method_upper, import_cache).is_some() {
            // The service-call pass already emitted the canonical route node for
            // this call site, so avoid adding a second duplicate route/hint.
            continue;
        }

        let Some(endpoint) = extract_config_endpoint_from_call(parsed, call, import_cache) else {
            continue;
        };
        if endpoint.path_or_endpoint.is_empty() {
            continue;
        }

        let target = if endpoint.resolved {
            let path = sanitize_path(&endpoint.path_or_endpoint);
            FrontendRouteTarget {
                qualified_name: route_qn(&method_upper, &path),
                display_name: path,
                confidence: Some(900),
                resolver: ResolverStrategy::FrontendConstant.as_str().to_owned(),
            }
        } else {
            FrontendRouteTarget {
                qualified_name: format!("__api_config__{}", endpoint.path_or_endpoint),
                display_name: endpoint.path_or_endpoint,
                confidence: Some(450),
                resolver: ResolverStrategy::FrontendHint.as_str().to_owned(),
            }
        };
        let node = api_virtual_node(
            NodeKind::Route,
            &target.qualified_name,
            &target.display_name,
            parsed,
            call,
        );
        push_node_and_consumes_edge(
            node,
            call,
            parsed,
            &mut augmentation.nodes,
            &mut augmentation.edges,
            target.confidence,
            &target.resolver,
            Some(EdgeKind::ConsumesApiFrom),
        );
    }
}

/// Extract an endpoint path string from a qualified hint that starts with a
/// recognised config root pattern.
///
/// Examples:
/// - `config.apiPath.gw.orders.get` → `gw/orders`  (last segment is the HTTP
///   method, not part of the path)
/// - `config.api.users` → `users`
/// - `API_BASE` → `API_BASE` (used directly as endpoint token)
/// - `apiUrl.reports.pdf` → `reports/pdf`
///
/// Returns `None` when the hint does not match any recognised config pattern.
fn extract_config_endpoint(hint: &str) -> Option<String> {
    // Strip optional leading `this.` so `this.config.apiPath.*` also matches.
    let stripped = hint.strip_prefix("this.").unwrap_or(hint);

    let path_tail = if let Some(tail) = stripped
        .strip_prefix("config.apiPath.")
        .or_else(|| stripped.strip_prefix("config.api."))
    {
        // Segments after the config root form the endpoint path. Drop the
        // trailing segment if it looks like an HTTP method name, since callers
        // often do `config.apiPath.gw.orders.get(…)` where `.get` is the verb.
        let segments: Vec<&str> = tail.split('.').collect();
        let trimmed = trim_http_verb_suffix(&segments);
        trimmed.join("/")
    } else if stripped == "API_BASE" || stripped.starts_with("API_BASE.") {
        let after = stripped.strip_prefix("API_BASE.").unwrap_or("API_BASE");
        let segments: Vec<&str> = after.split('.').collect();
        let trimmed = trim_http_verb_suffix(&segments);
        trimmed.join("/")
    } else if let Some(tail) = stripped.strip_prefix("apiUrl.") {
        let segments: Vec<&str> = tail.split('.').collect();
        let trimmed = trim_http_verb_suffix(&segments);
        trimmed.join("/")
    } else {
        return None;
    };

    if path_tail.is_empty() {
        None
    } else {
        Some(path_tail)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FrontendRouteTarget {
    qualified_name: String,
    display_name: String,
    confidence: Option<u16>,
    resolver: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ConfigEndpointTarget {
    path_or_endpoint: String,
    resolved: bool,
}

fn resolve_frontend_route_target(
    parsed: &ParsedFile,
    call: &EnrichedCallSite,
    method_upper: &str,
    import_cache: &mut FxHashMap<String, Option<ParsedFile>>,
) -> Option<FrontendRouteTarget> {
    let canonical_method = canonical_http_method(method_upper);
    if let Some(expression) = first_argument_expression(call)
        && let Some(raw_path) = string_literal_value(expression)
    {
        let path = sanitize_path(raw_path);
        if !path.is_empty() {
            return Some(FrontendRouteTarget {
                qualified_name: route_qn(canonical_method, &path),
                display_name: path,
                confidence: Some(950),
                resolver: ResolverStrategy::FrontendLiteral.as_str().to_owned(),
            });
        }
    }

    let expression = first_argument_expression(call)?;
    let mut visited = FxHashSet::default();
    if let Some(resolved) =
        resolve_expression_to_path_with_imports(parsed, expression, import_cache, &mut visited)
    {
        let path = sanitize_path(&resolved);
        if !path.is_empty() {
            return Some(FrontendRouteTarget {
                qualified_name: route_qn(canonical_method, &path),
                display_name: path,
                confidence: Some(900),
                resolver: ResolverStrategy::FrontendConstant.as_str().to_owned(),
            });
        }
    }

    // Template-literal base URL (e.g. `` `${SERVICE_URL}/items` ``): the base is
    // unknowable but the static tail after the final interpolation is a real
    // path. Degrade to that tail at reduced confidence.
    let tail = template_literal_path_tail(expression)?;
    Some(FrontendRouteTarget {
        qualified_name: route_qn(canonical_method, &tail),
        display_name: tail,
        confidence: Some(600),
        resolver: ResolverStrategy::FrontendHint.as_str().to_owned(),
    })
}

/// Extract the static path tail from an interpolated template literal such as
/// `` `${SERVICE_URL}/items` `` → `items`. Returns `None` when the expression is
/// not an interpolated template or has no static tail after the final `${…}`.
fn template_literal_path_tail(expression: &str) -> Option<String> {
    let trimmed = expression.trim();
    if !(trimmed.starts_with('`') && trimmed.ends_with('`') && trimmed.contains("${")) {
        return None;
    }
    let body = trimmed.trim_matches('`');
    let tail = body.rsplit_once('}').map_or(body, |(_, rest)| rest);
    let path = sanitize_path(tail);
    (!path.is_empty()).then_some(path)
}

fn extract_config_endpoint_from_call(
    parsed: &ParsedFile,
    call: &EnrichedCallSite,
    import_cache: &mut FxHashMap<String, Option<ParsedFile>>,
) -> Option<ConfigEndpointTarget> {
    if let Some(expression) = first_argument_expression(call) {
        let mut visited = FxHashSet::default();
        if let Some(path) =
            resolve_expression_to_path_with_imports(parsed, expression, import_cache, &mut visited)
        {
            return Some(ConfigEndpointTarget {
                path_or_endpoint: path,
                resolved: true,
            });
        }
    }

    let hint = call.callee_qualified_hint.as_deref()?;
    let endpoint = extract_config_endpoint(hint)?;
    Some(ConfigEndpointTarget {
        path_or_endpoint: endpoint,
        resolved: false,
    })
}

fn first_argument_expression(call: &EnrichedCallSite) -> Option<&str> {
    let raw = call.raw_arguments.as_deref()?;
    first_top_level_argument(raw)
}

fn second_argument_expression(call: &EnrichedCallSite) -> Option<&str> {
    let raw = call.raw_arguments.as_deref()?;
    split_top_level(raw, ',').get(1).copied()
}

fn resolve_expression_to_path_with_imports(
    parsed: &ParsedFile,
    expression: &str,
    import_cache: &mut FxHashMap<String, Option<ParsedFile>>,
    visited: &mut FxHashSet<(String, String)>,
) -> Option<String> {
    let trimmed = expression.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Some(literal) = string_literal_value(trimmed) {
        let path = sanitize_path(literal);
        return (!path.is_empty()).then_some(path);
    }

    if let Some(path) = parsed
        .constant_strings
        .get(trimmed.strip_prefix("this.").unwrap_or(trimmed))
        .map(|value| sanitize_path(value))
        .filter(|path| !path.is_empty())
    {
        return Some(path);
    }

    resolve_imported_expression_to_path(parsed, trimmed, import_cache, visited)
}

fn resolve_imported_expression_to_path(
    parsed: &ParsedFile,
    expression: &str,
    import_cache: &mut FxHashMap<String, Option<ParsedFile>>,
    visited: &mut FxHashSet<(String, String)>,
) -> Option<String> {
    let mut segments = expression
        .trim()
        .split('.')
        .filter(|segment| !segment.is_empty());
    let local_name = segments.next()?;
    let remainder = segments.collect::<Vec<_>>().join(".");

    for binding in &parsed.import_bindings {
        if binding.local_name != local_name {
            continue;
        }

        let imported =
            load_imported_parsed_file(parsed, binding.resolved_path.as_ref()?, import_cache)?;
        let imported_expression = imported_expression_for_binding(binding, &remainder)?;
        let visit_key = (
            imported.source_path.to_string_lossy().into_owned(),
            imported_expression.clone(),
        );
        if !visited.insert(visit_key.clone()) {
            continue;
        }

        let resolved = resolve_expression_to_path_with_imports(
            &imported,
            &imported_expression,
            import_cache,
            visited,
        );
        visited.remove(&visit_key);

        if resolved.is_some() {
            return resolved;
        }
    }

    None
}

fn imported_expression_for_binding(
    binding: &crate::resolve::ImportBinding,
    remainder: &str,
) -> Option<String> {
    if binding.is_namespace {
        return (!remainder.is_empty()).then_some(remainder.to_owned());
    }

    let imported_root = binding.imported_name.as_deref().or(if binding.is_default {
        Some("default")
    } else {
        None
    })?;
    if remainder.is_empty() {
        Some(imported_root.to_owned())
    } else {
        Some(format!("{imported_root}.{remainder}"))
    }
}

fn load_imported_parsed_file(
    parsed: &ParsedFile,
    resolved_path: &std::path::Path,
    import_cache: &mut FxHashMap<String, Option<ParsedFile>>,
) -> Option<ParsedFile> {
    let cache_key = resolved_path.to_string_lossy().into_owned();
    if !import_cache.contains_key(&cache_key) {
        let repo_root = derive_repo_root(parsed);
        let relative_path = resolved_path.strip_prefix(&repo_root).ok()?.to_path_buf();
        let language = language_for_path(&relative_path)?;
        let file = FileEntry {
            path: relative_path,
            language,
            size_bytes: 0,
            content_hash: [0; 32],
            source_bytes: None,
        };
        let imported = parse_file(&parsed.file_node.repo, &repo_root, &file).ok();
        import_cache.insert(cache_key.clone(), imported);
    }

    import_cache.get(&cache_key).cloned().flatten()
}

fn derive_repo_root(parsed: &ParsedFile) -> std::path::PathBuf {
    let mut root = parsed.source_path.clone();
    for _ in parsed.file.path.components() {
        root.pop();
    }
    root
}

fn language_for_path(path: &std::path::Path) -> Option<Language> {
    let extension = path.extension()?.to_str()?;
    match extension {
        "ts" | "tsx" => Some(Language::TypeScript),
        "js" | "jsx" | "mjs" | "cjs" => Some(Language::JavaScript),
        _ => None,
    }
}

fn canonical_http_method(method: &str) -> &str {
    method
}

fn string_literal_value(expression: &str) -> Option<&str> {
    let trimmed = expression.trim();
    let quoted = (trimmed.starts_with('"') && trimmed.ends_with('"'))
        || (trimmed.starts_with('\'') && trimmed.ends_with('\''));
    let simple_template =
        trimmed.starts_with('`') && trimmed.ends_with('`') && !trimmed.contains("${");
    if quoted || simple_template {
        Some(
            trimmed
                .trim_matches('"')
                .trim_matches('\'')
                .trim_matches('`'),
        )
    } else {
        None
    }
}

fn fetch_method_for_call(parsed: &ParsedFile, call: &EnrichedCallSite) -> Option<String> {
    let second = second_argument_expression(call)?;
    let method_expr = extract_object_property_value(second, "method")?;
    let method = if let Some(literal) = string_literal_value(method_expr) {
        literal.trim().to_ascii_uppercase()
    } else {
        parsed
            .constant_strings
            .get(method_expr.trim())
            .map(|value| value.trim().to_ascii_uppercase())?
    };
    (!method.is_empty()).then_some(method)
}

/// If the last segment of a path looks like an HTTP method name, drop it so
/// the endpoint path doesn't include the verb.
fn trim_http_verb_suffix<'a>(segments: &'a [&'a str]) -> &'a [&'a str] {
    match segments.last() {
        Some(&("get" | "post" | "put" | "patch" | "delete" | "head" | "options" | "request")) => {
            &segments[..segments.len().saturating_sub(1)]
        }
        _ => segments,
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Build a virtual `NodeData` for an API endpoint node derived from a call
/// site. Uses `ref_node_id` for a deterministic ID so duplicate call sites
/// across the file resolve to the same node (deduplication is handled by
/// `append_unique_nodes` in the orchestrator).
pub(crate) fn api_virtual_node(
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
        ai_role: None,
    }
}

/// Push `node` onto `nodes` and add a `Consumes` edge from `call.owner_id` to
/// the node. When `semantic_kind` is set, a second edge of that kind is added.
pub(crate) fn push_node_and_consumes_edge(
    node: NodeData,
    call: &EnrichedCallSite,
    parsed: &ParsedFile,
    nodes: &mut Vec<NodeData>,
    edges: &mut Vec<EdgeData>,
    confidence: Option<u16>,
    resolver: &str,
    semantic_kind: Option<EdgeKind>,
) {
    let target_id = node.id;
    nodes.push(node);
    edges.push(EdgeData {
        source: call.owner_id,
        target: target_id,
        kind: EdgeKind::Consumes,
        metadata: EdgeMetadata {
            weight: None,
            confidence,
            timestamp_unix: None,
            drift_kind: None,
            resolver: Some(resolver.to_owned()),
            guard_has_default: None,
            enum_qn: None,
            access_mechanism: None,
        },
        owner_file: parsed.file_node.id,
        is_cross_file: false,
    });
    if let Some(kind) = semantic_kind {
        edges.push(EdgeData {
            source: call.owner_id,
            target: target_id,
            kind,
            metadata: EdgeMetadata {
                weight: None,
                confidence,
                timestamp_unix: None,
                drift_kind: None,
                resolver: Some(resolver.to_owned()),
                guard_has_default: None,
                enum_qn: None,
                access_mechanism: None,
            },
            owner_file: parsed.file_node.id,
            is_cross_file: false,
        });
    }
}

pub(crate) fn extract_object_property_value<'a>(
    raw: &'a str,
    property_name: &str,
) -> Option<&'a str> {
    let property_index = raw.find(property_name)?;
    let after_name = &raw[property_index + property_name.len()..];
    let colon_index = after_name.find(':')?;
    let after_colon = after_name[colon_index + 1..].trim_start();
    if after_colon.is_empty() {
        return None;
    }

    if after_colon.starts_with('[') {
        return balanced_prefix(after_colon, '[', ']');
    }
    if after_colon.starts_with('{') {
        return balanced_prefix(after_colon, '{', '}');
    }

    Some(first_top_level_token(after_colon))
}

pub(crate) fn first_top_level_argument(raw_arguments: &str) -> Option<&str> {
    split_top_level(raw_arguments, ',').into_iter().next()
}

fn first_top_level_token(raw: &str) -> &str {
    let end = raw
        .char_indices()
        .find_map(|(index, ch)| (matches!(ch, ',' | '}' | ')') && index > 0).then_some(index))
        .unwrap_or(raw.len());
    raw[..end].trim()
}

fn balanced_prefix(raw: &str, open: char, close: char) -> Option<&str> {
    let mut depth = 0_u32;
    let mut in_single = false;
    let mut in_double = false;
    for (index, ch) in raw.char_indices() {
        match ch {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            _ if in_single || in_double => {}
            _ if ch == open => depth = depth.saturating_add(1),
            _ if ch == close => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(raw[..=index].trim());
                }
            }
            _ => {}
        }
    }
    None
}

/// Normalise an API path: strip leading `/` and surrounding whitespace/quotes.
fn sanitize_path(raw: &str) -> String {
    let trimmed = raw.trim().trim_matches('"').trim_matches('\'');
    let canonical = canonical_route_path(trimmed);
    canonical.trim_start_matches('/').to_owned()
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
                "gather-step-parser-http-client-{name}-{}-{counter}",
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
    fn http_service_post_produces_canonical_route_node() {
        let temp_dir = TestDir::new("http-service-post");
        fs::write(
            temp_dir.path().join("gateway.service.ts"),
            r#"
export class GatewayService {
  constructor(private readonly httpService: HttpService) {}

  async createItem(payload: unknown) {
    return this.httpService.post('/items', payload);
  }
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "gateway-app",
            temp_dir.path(),
            &crate::FileEntry {
                path: "gateway.service.ts".into(),
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
                    && node.external_id.as_deref() == Some("__route__POST__/items")
            })
            .collect();

        assert_eq!(
            route_nodes.len(),
            1,
            "expected one canonical POST route node, got: {route_nodes:?}"
        );
        assert!(
            parsed.edges.iter().any(|edge| {
                edge.kind == gather_step_core::EdgeKind::ConsumesApiFrom
                    && edge.target == route_nodes[0].id
            }),
            "a ConsumesApiFrom edge should point at the POST route node"
        );
    }

    #[test]
    fn template_literal_base_url_degrades_to_path_tail() {
        let temp_dir = TestDir::new("template-base-url");
        fs::write(
            temp_dir.path().join("gateway.ts"),
            r#"
export async function createItem(axios: any) {
  return axios.post(`${SERVICE_URL}/items`, {});
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "gateway-app",
            temp_dir.path(),
            &crate::FileEntry {
                path: "gateway.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        let route_node = parsed
            .nodes
            .iter()
            .find(|node| {
                node.kind == gather_step_core::NodeKind::Route
                    && node.external_id.as_deref() == Some("__route__POST__/items")
            })
            .expect("template-literal base URL should degrade to the /items path tail");
        assert!(
            parsed.edges.iter().any(|edge| {
                edge.kind == gather_step_core::EdgeKind::ConsumesApiFrom
                    && edge.target == route_node.id
                    && edge.metadata.confidence == Some(600)
            }),
            "a reduced-confidence ConsumesApiFrom edge should point at the tail route node"
        );
    }

    #[test]
    fn fully_interpolated_template_produces_no_route() {
        let temp_dir = TestDir::new("template-no-tail");
        fs::write(
            temp_dir.path().join("gateway.ts"),
            r#"
export async function loadItem(axios: any, id: string) {
  return axios.get(`${SERVICE_URL}/items/${id}`);
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "gateway-app",
            temp_dir.path(),
            &crate::FileEntry {
                path: "gateway.ts".into(),
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
                .all(|node| node.kind != gather_step_core::NodeKind::Route),
            "a template with no static tail after the final interpolation must not invent a route"
        );
    }
}
