use std::fmt;

use gather_step_core::{
    NodeData, NodeKind, canonical_route_path, parse_shared_symbol_qn, shared_package_root,
};
use tracing::debug;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopicKind {
    Topic,
    Queue,
    Subject,
    Stream,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Canonical {
    value: String,
}

impl Canonical {
    #[must_use]
    pub fn route(method: &str, path: &str) -> Self {
        let mut method = method.trim().to_owned();
        method.make_ascii_uppercase();
        Self {
            value: format!("{method} {}", canonical_route_path(path)),
        }
    }

    #[must_use]
    pub fn topic(kind: TopicKind, name: &str) -> Self {
        let prefix = match kind {
            TopicKind::Topic => "topic",
            TopicKind::Queue => "queue",
            TopicKind::Subject => "subject",
            TopicKind::Stream => "stream",
        };
        Self {
            value: format!("{prefix}:{}", normalize_topic_name(name)),
        }
    }

    #[must_use]
    pub fn shared_symbol(package: &str, symbol: &str) -> Self {
        let package = normalize_name(package);
        let package = if package.is_empty() {
            "unknown".to_owned()
        } else {
            package
        };
        Self {
            value: format!("shared:{package}::{}", normalize_name(symbol)),
        }
    }

    #[must_use]
    pub fn payload_contract(target: &str) -> Self {
        Self {
            value: format!("payload:{}", normalize_name(target)),
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.value
    }
}

impl fmt::Display for Canonical {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.value)
    }
}

#[must_use]
pub fn canonical_for_node(node: &NodeData) -> Option<Canonical> {
    match node.kind {
        NodeKind::Route => node
            .external_id
            .as_deref()
            .and_then(parse_route_external_id)
            .map(|(method, path)| Canonical::route(&method, &path)),
        NodeKind::Event => node
            .external_id
            .as_deref()
            .and_then(parse_event_external_id)
            .map(|topic| Canonical::topic(TopicKind::Topic, &topic)),
        NodeKind::Topic => Some(Canonical::topic(TopicKind::Topic, &node.name)),
        NodeKind::Queue => Some(Canonical::topic(TopicKind::Queue, &node.name)),
        NodeKind::Subject => Some(Canonical::topic(TopicKind::Subject, &node.name)),
        NodeKind::Stream => Some(Canonical::topic(TopicKind::Stream, &node.name)),
        NodeKind::SharedSymbol => Some(canonical_shared_symbol(node)),
        NodeKind::PayloadContract => node
            .qualified_name
            .as_deref()
            .map(Canonical::payload_contract),
        _ => {
            debug!(
                kind = ?node.kind,
                name = %node.name,
                "canonical_for_node: no canonical form defined for this NodeKind"
            );
            None
        }
    }
}

/// Parse a route external-id in either of the two accepted forms:
///
/// - `"__route__METHOD__path"` (double-underscore, produced by [`route_qn`])
/// - `"METHOD path"` (space-delimited, used internally by virtual helpers)
///
/// Returns `(method, path)` or `None` if neither form matches.
fn parse_route_external_id(external_id: &str) -> Option<(String, String)> {
    // Double-underscore form: __route__GET__/api/alerts
    if let Some(suffix) = external_id
        .strip_prefix("__route__")
        .or_else(|| external_id.strip_prefix("__api_call__"))
    {
        let (method, path) = suffix.split_once("__")?;
        return Some((method.to_owned(), path.to_owned()));
    }
    // Space-delimited form: GET /api/alerts
    let (method, path) = external_id.split_once(' ')?;
    Some((method.to_owned(), path.to_owned()))
}

/// Parse an event external-id of the form `"__event__<transport>__<topic>"`.
///
/// Returns the topic name or `None` if the form does not match.
fn parse_event_external_id(external_id: &str) -> Option<String> {
    let body = external_id.strip_prefix("__event__")?;
    // body is "<transport>__<topic>"; take everything after the first "__"
    let (_, topic) = body.split_once("__")?;
    if topic.is_empty() {
        None
    } else {
        Some(topic.to_owned())
    }
}

fn canonical_shared_symbol(node: &NodeData) -> Canonical {
    let Some(qualified_name) = node.qualified_name.as_deref() else {
        return Canonical::shared_symbol("", &node.name);
    };
    let Some(identity) = parse_shared_symbol_qn(qualified_name) else {
        return Canonical::shared_symbol("", &node.name);
    };
    let package = identity
        .package
        .and_then(shared_package_root)
        .or(identity.package)
        .unwrap_or("");
    Canonical::shared_symbol(package, identity.symbol)
}

fn normalize_name(value: &str) -> String {
    let mut normalized = value.trim().to_owned();
    normalized.make_ascii_lowercase();
    normalized
}

/// Normalize a topic, queue, subject, or stream name for canonical identity.
///
/// Extends `normalize_name` by stripping a trailing version suffix so producers
/// and consumers referring to the same logical topic across schema versions
/// converge on one canonical identity. Accepted suffix forms (case-insensitive
/// after prior lowercasing): `.v1`, `_v1`, `-v1`, and bare `.1`/`_1`/`-1` when
/// preceded by a `v` boundary.
fn normalize_topic_name(value: &str) -> String {
    let normalized = normalize_name(value);
    strip_version_suffix(&normalized).to_owned()
}

fn strip_version_suffix(value: &str) -> &str {
    // Only strip a suffix that matches `[._-]v?<digits>` at the very end.
    let bytes = value.as_bytes();
    let Some(mut cursor) = bytes.len().checked_sub(1) else {
        return value;
    };
    // Walk back over trailing ASCII digits.
    let mut digit_count = 0usize;
    while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
        digit_count += 1;
        if cursor == 0 {
            return value;
        }
        cursor -= 1;
    }
    if digit_count == 0 {
        return value;
    }
    // Optional `v` separator (e.g. `.v1`).
    if bytes[cursor] == b'v' {
        if cursor == 0 {
            return value;
        }
        cursor -= 1;
    }
    // Required delimiter so we never truncate inside a word like `events2`.
    if matches!(bytes[cursor], b'.' | b'_' | b'-') {
        &value[..cursor]
    } else {
        value
    }
}

#[cfg(test)]
mod tests {
    use gather_step_core::{NodeData, NodeKind, route_qn};

    use super::{Canonical, TopicKind, canonical_for_node};

    fn virtual_route(path: &str) -> NodeData {
        let qn = route_qn("GET", path);
        NodeData {
            id: gather_step_core::ref_node_id(NodeKind::Route, &qn),
            kind: NodeKind::Route,
            repo: "__virtual__".to_owned(),
            file_path: qn.clone(),
            name: format!("GET {path}"),
            qualified_name: Some(qn),
            external_id: Some(format!("GET {path}")),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        }
    }

    #[test]
    fn route_canonical_normalizes_path_parameter_styles() {
        let colon = virtual_route("/orders/:id");
        let braces = virtual_route("/orders/{ID}");
        let dollar = virtual_route("/orders/$id");

        let colon = canonical_for_node(&colon).expect("colon canonical");
        let braces = canonical_for_node(&braces).expect("braces canonical");
        let dollar = canonical_for_node(&dollar).expect("dollar canonical");

        assert_eq!(colon.as_str(), "GET /orders/:id");
        assert_eq!(colon, braces);
        assert_eq!(colon, dollar);
    }

    #[test]
    fn subject_and_stream_nodes_receive_canonical_identities() {
        let subject = NodeData {
            id: gather_step_core::ref_node_id(NodeKind::Subject, "__subject__nats__orders.created"),
            kind: NodeKind::Subject,
            repo: "backend_standard".to_owned(),
            file_path: "src/events.ts".to_owned(),
            name: "Orders.Created".to_owned(),
            qualified_name: Some("__subject__nats__orders.created".to_owned()),
            external_id: Some("__subject__nats__orders.created".to_owned()),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        };
        let stream = NodeData {
            id: gather_step_core::ref_node_id(NodeKind::Stream, "__stream__kinesis__orders"),
            kind: NodeKind::Stream,
            repo: "backend_standard".to_owned(),
            file_path: "src/events.ts".to_owned(),
            name: "Orders".to_owned(),
            qualified_name: Some("__stream__kinesis__orders".to_owned()),
            external_id: Some("__stream__kinesis__orders".to_owned()),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        };

        assert_eq!(
            canonical_for_node(&subject),
            Some(Canonical::topic(TopicKind::Subject, "Orders.Created"))
        );
        assert_eq!(
            canonical_for_node(&stream),
            Some(Canonical::topic(TopicKind::Stream, "Orders"))
        );
    }

    #[test]
    fn topic_canonical_converges_across_version_suffixes() {
        let unversioned = Canonical::topic(TopicKind::Topic, "orders");
        let cases = [
            "orders.v1",
            "orders.V2",
            "orders_v3",
            "orders-v10",
            "orders.1",
            "orders_2",
            "orders-5",
            "Orders.V1",
        ];
        for raw in cases {
            assert_eq!(
                Canonical::topic(TopicKind::Topic, raw),
                unversioned,
                "{raw} should converge to the unversioned canonical"
            );
        }
    }

    #[test]
    fn topic_canonical_preserves_names_that_only_look_versioned() {
        // No `.`, `_`, or `-` delimiter before the trailing digits — must not strip.
        assert_ne!(
            Canonical::topic(TopicKind::Topic, "events2"),
            Canonical::topic(TopicKind::Topic, "events"),
        );
        // Lone `v` without digits is not a version suffix.
        assert_ne!(
            Canonical::topic(TopicKind::Topic, "orders.v"),
            Canonical::topic(TopicKind::Topic, "orders"),
        );
    }

    #[test]
    fn shared_symbol_canonical_converges_across_version_and_barrel_forms() {
        fn shared_symbol(repo: &str, qualified_name: &str, name: &str) -> NodeData {
            NodeData {
                id: gather_step_core::ref_node_id(NodeKind::SharedSymbol, qualified_name),
                kind: NodeKind::SharedSymbol,
                repo: repo.to_owned(),
                file_path: "src/contracts.ts".to_owned(),
                name: name.to_owned(),
                qualified_name: Some(qualified_name.to_owned()),
                external_id: Some(qualified_name.to_owned()),
                signature: None,
                visibility: None,
                span: None,
                is_virtual: true,
            }
        }

        let direct = shared_symbol(
            "backend_standard",
            "__shared__@workspace/shared-contracts__OrderStatus",
            "OrderStatus",
        );
        let subpath = shared_symbol(
            "frontend_standard",
            "__shared__@workspace/shared-contracts/dtos__OrderStatus",
            "OrderStatus",
        );
        let versioned = shared_symbol(
            "shared_contracts",
            "__shared__@workspace/shared-contracts@2.3.1__OrderStatus",
            "OrderStatus",
        );

        let expected = Some(Canonical::shared_symbol(
            "@workspace/shared-contracts",
            "OrderStatus",
        ));

        assert_eq!(canonical_for_node(&direct), expected);
        assert_eq!(canonical_for_node(&subpath), expected);
        assert_eq!(canonical_for_node(&versioned), expected);
    }

    fn route_node_for_test(
        repo: &str,
        file_path: &str,
        external_id: &str,
        _ordinal: u16,
    ) -> NodeData {
        NodeData {
            id: gather_step_core::node_id(repo, file_path, NodeKind::Route, external_id),
            kind: NodeKind::Route,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: external_id.to_owned(),
            qualified_name: Some(external_id.to_owned()),
            external_id: Some(external_id.to_owned()),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        }
    }

    fn event_node_for_test(
        repo: &str,
        file_path: &str,
        external_id: &str,
        _ordinal: u16,
    ) -> NodeData {
        NodeData {
            id: gather_step_core::node_id(repo, file_path, NodeKind::Event, external_id),
            kind: NodeKind::Event,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: external_id.to_owned(),
            qualified_name: Some(external_id.to_owned()),
            external_id: Some(external_id.to_owned()),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        }
    }

    fn file_node_for_test(repo: &str, file_path: &str) -> NodeData {
        NodeData {
            id: gather_step_core::node_id(repo, file_path, NodeKind::File, file_path),
            kind: NodeKind::File,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: file_path.to_owned(),
            qualified_name: None,
            external_id: None,
            signature: None,
            visibility: None,
            span: None,
            is_virtual: false,
        }
    }

    #[test]
    fn canonical_for_node_parses_route_external_id_with_double_underscore() {
        let node = route_node_for_test("repoA", "/src/routes.ts", "__route__GET__/api/alerts", 1);
        let canonical = canonical_for_node(&node).expect("route canonicalization");
        let text = canonical.to_string();
        assert!(text.contains("GET"), "method must parse: {text}");
        assert!(text.contains("/api/alerts"), "path must parse: {text}");
    }

    #[test]
    fn canonical_for_node_covers_event_nodes() {
        let node =
            event_node_for_test("repoA", "/src/events.ts", "__event__kafka__user_created", 1);
        let canonical = canonical_for_node(&node).expect("event canonicalization");
        assert!(canonical.to_string().contains("user_created"));
    }

    #[test]
    fn canonical_for_node_returns_none_for_kinds_without_a_defined_form() {
        // File nodes intentionally have no canonical form.
        let node = file_node_for_test("repoA", "/src/index.ts");
        assert!(canonical_for_node(&node).is_none());
    }
}
