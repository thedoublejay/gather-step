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

    for (method, path) in extract_route_entries(content) {
        let qn = route_qn(&method, &path);
        if !seen_qns.insert(qn.clone()) {
            continue;
        }
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
    }
}

/// Scan `content` for `(method, path)` pairs from the gateway config shape.
///
/// For each `method: 'METHOD'` occurrence, looks in the following lines for
/// `from: 'PATH'` (preferred) then `basePathWithoutApiPrefix: 'PATH'`.
/// Only emits pairs where the method is a valid HTTP verb.
fn extract_route_entries(content: &str) -> Vec<(String, String)> {
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

        // Prefer rewrite.from
        let path = window
            .iter()
            .find_map(|l| extract_quoted_value(l, "from").map(str::to_owned))
            .or_else(|| {
                window.iter().find_map(|l| {
                    extract_quoted_value(l, "basePathWithoutApiPrefix").map(str::to_owned)
                })
            });

        if let Some(path) = path.filter(|p| !p.is_empty()) {
            results.push((method, path));
        }
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
    use super::*;

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
}
