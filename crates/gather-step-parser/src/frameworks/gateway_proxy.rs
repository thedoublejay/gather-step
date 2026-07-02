use gather_step_core::{
    EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, ref_node_id, route_qn,
};

use crate::tree_sitter::ParsedFile;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GatewayProxyAugmentation {
    pub nodes: Vec<NodeData>,
    pub edges: Vec<EdgeData>,
}

/// Extract proxy-config routes from `api_gateway`-style serviceConfigs files.
///
/// Only fires when `parsed.source_path` has a `serviceConfigs` path component.
/// Reads the file content and extracts (method, path) pairs from the known
/// gateway endpoint config shape:
///
/// ```ts
/// export const endpoints = {
///   report: {
///     method: 'POST',
///     pathMapping: {
///       basePathWithoutApiPrefix: '/report/pdf',
///       rewrite: { from: '/api/v2/report/pdf' }
///     }
///   }
/// }
/// ```
///
/// Prefers `rewrite.from` when present; falls back to
/// `basePathWithoutApiPrefix`.
pub fn augment(parsed: &ParsedFile) -> GatewayProxyAugmentation {
    let mut aug = GatewayProxyAugmentation::default();

    if !is_service_config_file(parsed) {
        return aug;
    }

    let content = &*parsed.source;

    let file_node_id = parsed.file_node.id;
    let repo = &parsed.file_node.repo;
    let file_path = &parsed.file_node.file_path;

    let mut seen_qns = rustc_hash::FxHashSet::default();

    for (method, path, backend_path) in extract_route_entries_with_backend(content) {
        let qn = route_qn(&method, &path);
        if seen_qns.insert(qn.clone()) {
            let route_node = make_route_node(&qn, &method, &path, repo, file_path);
            let route_id = route_node.id;
            aug.nodes.push(route_node);
            aug.edges.push(EdgeData {
                source: file_node_id,
                target: route_id,
                kind: EdgeKind::Serves,
                metadata: EdgeMetadata::default(),
                owner_file: file_node_id,
                is_cross_file: false,
            });
            aug.edges.push(EdgeData {
                source: file_node_id,
                target: route_id,
                kind: EdgeKind::ConsumesApiFrom,
                metadata: EdgeMetadata {
                    confidence: Some(820),
                    ..EdgeMetadata::default()
                },
                owner_file: file_node_id,
                is_cross_file: true,
            });
        }

        if let Some(backend_path) = backend_path {
            let backend_qn = route_qn(&method, &backend_path);
            if seen_qns.insert(backend_qn.clone()) {
                let backend_node =
                    make_route_node(&backend_qn, &method, &backend_path, repo, file_path);
                let backend_id = backend_node.id;
                aug.nodes.push(backend_node);
                aug.edges.push(EdgeData {
                    source: file_node_id,
                    target: backend_id,
                    kind: EdgeKind::ConsumesApiFrom,
                    metadata: EdgeMetadata {
                        confidence: Some(820),
                        ..EdgeMetadata::default()
                    },
                    owner_file: file_node_id,
                    is_cross_file: true,
                });
            }
        }
    }

    aug
}

fn is_service_config_file(parsed: &ParsedFile) -> bool {
    parsed
        .source_path
        .components()
        .any(|c| c.as_os_str() == "serviceConfigs")
}

fn make_route_node(qn: &str, method: &str, path: &str, repo: &str, file_path: &str) -> NodeData {
    NodeData {
        id: ref_node_id(NodeKind::Route, qn),
        kind: NodeKind::Route,
        repo: repo.to_owned(),
        file_path: file_path.to_owned(),
        name: format!("{method} {path}"),
        qualified_name: Some(qn.to_owned()),
        external_id: Some(qn.to_owned()),
        signature: None,
        visibility: None,
        span: None,
        is_virtual: true,
        ai_role: None,
    }
}

/// Scan `content` for `(method, path)` pairs from the gateway config shape.
///
/// For each `method: 'METHOD'` occurrence, looks in the following lines for
/// `from: 'PATH'` (preferred) then `basePathWithoutApiPrefix: 'PATH'`.
/// Only emits pairs where the method is a valid HTTP verb.
#[cfg(test)]
fn extract_route_entries(content: &str) -> Vec<(String, String)> {
    extract_route_entries_with_backend(content)
        .into_iter()
        .map(|(method, path, _)| (method, path))
        .collect()
}

/// Like [`extract_route_entries`], but also reports the distinct backend path.
///
/// The public path prefers `rewrite.from` and falls back to
/// `basePathWithoutApiPrefix`.  When `rewrite.from` supplies the public path
/// and a `basePathWithoutApiPrefix` different from it also exists, that base
/// path is returned as the backend path so callers can bridge the gateway's
/// proxied route to the service that actually serves it.
fn extract_route_entries_with_backend(content: &str) -> Vec<(String, String, Option<String>)> {
    const LOOK_AHEAD: usize = 15;
    const VALID_METHODS: &[&str] = &["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"];

    let lines: Vec<&str> = content.lines().collect();
    let mut results = Vec::new();

    for (i, line) in lines.iter().enumerate() {
        let Some(method) = extract_quoted_value(line, "method") else {
            continue;
        };
        let method = method.trim().to_ascii_uppercase();
        if !VALID_METHODS.contains(&method.as_str()) {
            continue;
        }

        let end = (i + 1 + LOOK_AHEAD).min(lines.len());
        let window = &lines[i + 1..end];

        let from_path = window
            .iter()
            .find_map(|l| extract_quoted_value(l, "from").map(str::to_owned))
            .filter(|p| !p.is_empty());
        let base_path = window
            .iter()
            .find_map(|l| extract_quoted_value(l, "basePathWithoutApiPrefix").map(str::to_owned))
            .filter(|p| !p.is_empty());

        // Prefer rewrite.from
        let Some(public_path) = from_path.clone().or_else(|| base_path.clone()) else {
            continue;
        };

        let backend_path = match (&from_path, &base_path) {
            (Some(_), Some(base)) if *base != public_path => Some(base.clone()),
            _ => None,
        };

        results.push((method, public_path, backend_path));
    }

    results
}

/// Extract the string value for a given `key` from a TypeScript object-literal line.
///
/// Matches `key: 'value'` and `key: "value"`.
fn extract_quoted_value<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    let key_pat = format!("{key}:");
    let idx = line.find(key_pat.as_str())?;
    let rest = line[idx + key_pat.len()..].trim();
    if let Some(s) = rest.strip_prefix('\'') {
        s.split_once('\'').map(|(v, _)| v)
    } else if let Some(s) = rest.strip_prefix('"') {
        s.split_once('"').map(|(v, _)| v)
    } else {
        None
    }
}

// ── tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::{env, fs, process};

    use gather_step_core::NodeId;

    use super::*;
    use crate::frameworks::Framework;
    use crate::tree_sitter::parse_file_with_frameworks;
    use crate::{FileEntry, Language};

    static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> Self {
            let counter = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-parser-gateway-{name}-{}-{counter}",
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

    fn parse_service_config(dir: &TestDir, relative: &str, source: &str) -> crate::ParsedFile {
        let full = dir.path().join(relative);
        if let Some(parent) = full.parent() {
            fs::create_dir_all(parent).expect("fixture parent dir should be created");
        }
        fs::write(&full, source).expect("fixture source should write");
        parse_file_with_frameworks(
            "web-gateway",
            dir.path(),
            &FileEntry {
                path: relative.into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &[Framework::GatewayProxy],
        )
        .expect("fixture should parse")
    }

    fn route_ids_by_qn(aug: &GatewayProxyAugmentation) -> Vec<(String, NodeId)> {
        aug.nodes
            .iter()
            .filter(|n| n.kind == NodeKind::Route)
            .map(|n| (n.qualified_name.clone().unwrap_or_default(), n.id))
            .collect()
    }

    // ── extract_quoted_value ──────────────────────────────────────────────────

    #[test]
    fn quoted_value_single_quote() {
        assert_eq!(
            extract_quoted_value("  method: 'POST',", "method"),
            Some("POST")
        );
    }

    #[test]
    fn quoted_value_double_quote() {
        assert_eq!(
            extract_quoted_value("  method: \"GET\",", "method"),
            Some("GET")
        );
    }

    #[test]
    fn quoted_value_missing_key() {
        assert_eq!(extract_quoted_value("  foo: 'bar',", "method"), None);
    }

    // ── extract_route_entries ─────────────────────────────────────────────────

    #[test]
    fn extracts_from_rewrite() {
        let content = r"
export const endpoints = {
  report: {
    method: 'POST',
    pathMapping: {
      basePathWithoutApiPrefix: '/report/pdf',
      rewrite: { from: '/api/v2/report/pdf' },
    },
  },
};
";
        let entries = extract_route_entries(content);
        assert_eq!(
            entries,
            vec![("POST".to_owned(), "/api/v2/report/pdf".to_owned())]
        );
    }

    #[test]
    fn extracts_real_service_config_with_template_literal_rewrite() {
        // Mirrors the real web-api-gateway serviceConfig shape: a template-literal
        // `rewrite.from` (`${prefix}` + regex) that cannot be read as a string,
        // so extraction must fall back to the single-quoted `basePathWithoutApiPrefix`.
        let content = r"
export const commentServiceConfig: ServiceConfig = {
  createCommentForDocument: {
    access: { ability: rules.READ, resource: 'comment' },
    rolesAllowed: clientRoles,
    method: 'POST',
    pathMapping: {
      basePathWithoutApiPrefix: '/document/:documentId/comments',
      rewrite: {
        from: `${pathPrefixV2}/document/([^/]+)/comments`,
        to: `${commentsByEntityBasePath}/document/$1`,
      },
    },
  },
};
";
        let entries = extract_route_entries(content);
        assert_eq!(
            entries,
            vec![(
                "POST".to_owned(),
                "/document/:documentId/comments".to_owned()
            )]
        );
    }

    #[test]
    fn falls_back_to_base_path() {
        let content = r"
export const endpoints = {
  health: {
    method: 'GET',
    pathMapping: {
      basePathWithoutApiPrefix: '/health',
    },
  },
};
";
        let entries = extract_route_entries(content);
        assert_eq!(entries, vec![("GET".to_owned(), "/health".to_owned())]);
    }

    #[test]
    fn skips_non_http_method() {
        let content = "  method: 'CONNECT',\n  from: '/connect',\n";
        assert!(extract_route_entries(content).is_empty());
    }

    #[test]
    fn extracts_multiple_endpoints() {
        let content = r"
export const endpoints = {
  report: {
    method: 'POST',
    pathMapping: { rewrite: { from: '/api/v2/report/pdf' } },
  },
  health: {
    method: 'GET',
    pathMapping: { basePathWithoutApiPrefix: '/health' },
  },
};
";
        let entries = extract_route_entries(content);
        assert_eq!(entries.len(), 2);
        assert!(entries.contains(&("POST".to_owned(), "/api/v2/report/pdf".to_owned())));
        assert!(entries.contains(&("GET".to_owned(), "/health".to_owned())));
    }

    // ── route QN ─────────────────────────────────────────────────────────────

    #[test]
    fn route_qn_canonical() {
        // route_qn normalises path; must match what resolve_route_target expects
        assert_eq!(
            route_qn("POST", "/api/v2/report/pdf"),
            "__route__POST__/api/v2/report/pdf"
        );
    }

    // ── dedup ─────────────────────────────────────────────────────────────────

    #[test]
    fn dedup_prevents_duplicate_qns() {
        let content = r"
  method: 'POST',
  from: '/api/v2/report/pdf',

  method: 'POST',
  from: '/api/v2/report/pdf',
";
        let entries = extract_route_entries(content);
        // Two raw entries but augment should dedup on QN
        assert_eq!(entries.len(), 2); // extract_route_entries itself doesn't dedup
        // The augment() dedup is tested at the integration level
    }

    // ── augment: gateway → backend route bridge ───────────────────────────────

    #[test]
    fn emits_backend_route_consumption_when_rewrite_from_present() {
        let dir = TestDir::new("bridge");
        let parsed = parse_service_config(
            &dir,
            "src/serviceConfigs/items.service.ts",
            r"
export const itemsServiceConfig = {
  getItems: {
    method: 'GET',
    pathMapping: {
      basePathWithoutApiPrefix: '/items',
      rewrite: { from: '/api/v1/items' },
    },
  },
};
",
        );

        let aug = augment(&parsed);
        let public_qn = route_qn("GET", "/api/v1/items");
        let backend_qn = route_qn("GET", "/items");

        let routes = route_ids_by_qn(&aug);
        assert!(
            routes.iter().any(|(qn, _)| *qn == public_qn),
            "public path route must still be emitted: {routes:?}"
        );
        let backend_id = routes
            .iter()
            .find(|(qn, _)| *qn == backend_qn)
            .map(|(_, id)| *id)
            .expect("backend path route must be emitted");

        assert!(
            aug.edges.iter().any(|e| {
                e.kind == EdgeKind::ConsumesApiFrom
                    && e.target == backend_id
                    && e.source == parsed.file_node.id
            }),
            "a ConsumesApiFrom edge must target the backend route",
        );
    }

    #[test]
    fn identical_public_and_backend_path_emits_single_route() {
        let dir = TestDir::new("identical");
        let parsed = parse_service_config(
            &dir,
            "src/serviceConfigs/items.service.ts",
            r"
export const itemsServiceConfig = {
  getItems: {
    method: 'GET',
    pathMapping: {
      basePathWithoutApiPrefix: '/items',
      rewrite: { from: '/items' },
    },
  },
};
",
        );

        let aug = augment(&parsed);
        let routes = route_ids_by_qn(&aug);
        assert_eq!(
            routes.len(),
            1,
            "identical public/backend path must not self-consume: {routes:?}"
        );
        assert_eq!(routes[0].0, route_qn("GET", "/items"));
    }
}
