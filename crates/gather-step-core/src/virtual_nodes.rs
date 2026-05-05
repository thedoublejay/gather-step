use crate::{NodeData, NodeId, NodeKind, ref_node_id};

/// Typed description of a virtual transport-boundary node.
///
/// Virtual nodes are not stored as files on disk — they exist only in the
/// graph as deterministic stubs that link producers to consumers across
/// service boundaries.  This enum makes the transport kind explicit at the
/// type level so analysis code can branch on it without string parsing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VirtualNodeKind {
    /// An HTTP route boundary (e.g. `POST /orders`).
    Route {
        /// HTTP method in uppercase (e.g. `"GET"`).
        method: String,
        /// Canonical path with normalised params (e.g. `/orders/:id`).
        canonical_path: String,
    },
    /// A Kafka topic (or similar pub/sub channel).
    Topic {
        /// Topic name as indexed.
        name: String,
    },
    /// A Bull (or similar) queue.
    Queue {
        /// Queue protocol (e.g. `"bull"`).
        protocol: String,
        /// Queue name.
        name: String,
    },
    /// A fire-and-forget event.
    Event {
        /// Event name.
        name: String,
    },
}

/// Repository name used for all virtual (non-file) nodes — author ownership
/// anchors, shared-symbol stubs, route descriptors, etc.
///
/// This constant is the single source of truth; the git-intelligence layer
/// (`gather-step-git::intelligence`) also references this value by keeping its
/// own private alias in sync.
pub const VIRTUAL_NODE_REPO: &str = "__virtual__";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SharedSymbolRef<'a> {
    pub package: Option<&'a str>,
    pub version: Option<&'a str>,
    pub symbol: &'a str,
}

#[must_use]
pub fn route_qn(method: &str, path: &str) -> String {
    let method = method.trim().to_ascii_uppercase();
    let path = canonical_route_path(path);
    format!("__route__{method}__{path}")
}

#[must_use]
pub fn topic_qn(protocol: &str, name: &str) -> String {
    let mut protocol = protocol.trim().to_owned();
    protocol.make_ascii_lowercase();
    let name = name.trim();
    format!("__topic__{protocol}__{name}")
}

#[must_use]
pub fn queue_qn(protocol: &str, name: &str) -> String {
    let mut protocol = protocol.trim().to_owned();
    protocol.make_ascii_lowercase();
    let name = name.trim();
    format!("__queue__{protocol}__{name}")
}

#[must_use]
pub fn deployment_qn(repo: &str, name: &str) -> String {
    let repo = canonical_topology_part_or(repo, "unknown_repo");
    let name = canonical_topology_part_or(name, "unknown_deployment");
    format!("__deployment__{repo}__{name}")
}

#[must_use]
pub fn env_var_qn(name: &str) -> String {
    let name = canonical_topology_part_or(name, "unknown_env");
    format!("__env_var__{name}")
}

#[must_use]
pub fn secret_qn(name: &str) -> String {
    let name = canonical_topology_part_or(name, "unknown_secret");
    format!("__secret__{name}")
}

#[must_use]
pub fn config_map_qn(name: &str) -> String {
    let name = canonical_topology_part_or(name, "unknown_config_map");
    format!("__config_map__{name}")
}

#[must_use]
pub fn broker_qn(kind: &str, endpoint_or_name: &str) -> String {
    let kind = canonical_topology_part_or(kind, "unknown");
    let endpoint_or_name = canonical_topology_part_or(endpoint_or_name, "unknown");
    format!("__broker__{kind}__{endpoint_or_name}")
}

#[must_use]
pub fn database_qn(kind: &str, endpoint_or_name: &str) -> String {
    let kind = canonical_topology_part_or(kind, "unknown");
    let endpoint_or_name = canonical_topology_part_or(endpoint_or_name, "unknown");
    format!("__database__{kind}__{endpoint_or_name}")
}

#[must_use]
pub fn shared_symbol_qn(package: &str, version: &str, symbol: &str) -> String {
    let package = package.trim();
    let version = version.trim();
    let symbol = symbol.trim();
    format!("__shared__{package}@{version}__{symbol}")
}

#[must_use]
pub fn shared_symbol_qn_unversioned(package: &str, symbol: &str) -> String {
    let package = package.trim();
    let symbol = symbol.trim();
    format!("__shared__{package}__{symbol}")
}

#[must_use]
pub fn parse_shared_symbol_qn(qualified_name: &str) -> Option<SharedSymbolRef<'_>> {
    let body = qualified_name.strip_prefix("__shared__")?;
    if body.is_empty() {
        return None;
    }

    if let Some((package_and_version, symbol)) = body.rsplit_once("__") {
        if symbol.is_empty() {
            return None;
        }
        let (package, version) = split_shared_symbol_package_version(package_and_version);
        return Some(SharedSymbolRef {
            package: (!package.is_empty()).then_some(package),
            version,
            symbol,
        });
    }

    None
}

#[must_use]
pub fn shared_package_root(specifier: &str) -> Option<&str> {
    let trimmed = specifier.trim();
    if trimmed.is_empty() || trimmed.starts_with('.') || trimmed.starts_with('/') {
        return None;
    }

    if let Some(rest) = trimmed.strip_prefix('@') {
        let (scope, tail) = rest.split_once('/')?;
        let (package, _) = tail.split_once('/').unwrap_or((tail, ""));
        let end = 1 + scope.len() + 1 + package.len();
        Some(&trimmed[..end])
    } else {
        Some(trimmed.split('/').next().unwrap_or(trimmed))
    }
}

#[must_use]
pub fn virtual_node_id(kind: NodeKind, qualified_name: &str) -> NodeId {
    ref_node_id(kind, qualified_name)
}

#[must_use]
pub fn virtual_node(
    kind: NodeKind,
    repo: impl Into<String>,
    file_path: impl Into<String>,
    name: impl Into<String>,
    qualified_name: impl Into<String>,
) -> NodeData {
    let qualified_name = qualified_name.into();
    NodeData {
        id: virtual_node_id(kind, &qualified_name),
        kind,
        repo: repo.into(),
        file_path: file_path.into(),
        name: name.into(),
        qualified_name: Some(qualified_name.clone()),
        external_id: Some(qualified_name),
        signature: None,
        visibility: None,
        span: None,
        is_virtual: true,
    }
}

#[must_use]
pub fn canonical_route_path(path: &str) -> String {
    let mut trimmed = path.trim();
    if trimmed.is_empty() {
        return "/".to_owned();
    }

    if let Some((_, rest)) = trimmed.split_once("://") {
        trimmed = rest;
        trimmed = match trimmed.find('/') {
            Some(index) => &trimmed[index..],
            None => "/",
        };
    }

    if let Some(index) = trimmed.find(['?', '#']) {
        trimmed = &trimmed[..index];
    }

    let trimmed = trimmed.trim();
    if trimmed.is_empty() {
        return "/".to_owned();
    }

    let mut normalized = if trimmed.starts_with('/') {
        trimmed.to_owned()
    } else {
        format!("/{trimmed}")
    };

    if normalized.len() > 1 {
        while normalized.ends_with('/') {
            normalized.pop();
        }
    }

    let segments = normalized
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(normalize_route_segment)
        .collect::<Vec<_>>();

    if segments.is_empty() {
        "/".to_owned()
    } else {
        format!("/{}", segments.join("/"))
    }
}

fn normalize_route_segment(segment: &str) -> String {
    let trimmed = segment.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let parameter = if let Some(name) = trimmed.strip_prefix(':') {
        Some(name)
    } else if let Some(name) = trimmed.strip_prefix('$') {
        Some(name)
    } else if trimmed.starts_with('{') && trimmed.ends_with('}') && trimmed.len() > 2 {
        Some(&trimmed[1..trimmed.len() - 1])
    } else {
        None
    };

    if let Some(name) = parameter {
        let mut canonical = String::from(":");
        let mut lowered = name.trim().to_owned();
        lowered.make_ascii_lowercase();
        canonical.push_str(&lowered);
        canonical
    } else {
        let mut lowered = trimmed.to_owned();
        lowered.make_ascii_lowercase();
        lowered
    }
}

fn split_shared_symbol_package_version(value: &str) -> (&str, Option<&str>) {
    let Some(index) = value.rfind('@') else {
        return (value, None);
    };
    if index == 0 || index + 1 >= value.len() {
        return (value, None);
    }

    let package = &value[..index];
    let version = &value[index + 1..];
    (package, Some(version))
}

/// Normalize a value into a canonical topology identifier component:
/// lowercase ASCII alphanumerics and `.`, `-`, `:` are kept verbatim; every
/// other run of characters collapses to a single `_`. Returns the empty
/// string when the input contains no canonicalizable characters; callers
/// that need a fallback should use [`canonical_topology_part_or`].
///
/// Used by both this crate (to mint stable virtual-node qualified names)
/// and `gather-step-analysis::deployment_topology` (to match user-supplied
/// targets against those same names) so they cannot drift.
#[must_use]
pub fn canonical_topology_part(value: &str) -> String {
    let mut normalized = String::new();
    let mut previous_was_separator = false;
    for ch in value.trim().chars() {
        let next = if ch.is_ascii_alphanumeric() {
            previous_was_separator = false;
            ch.to_ascii_lowercase()
        } else if matches!(ch, '.' | '-' | ':') {
            previous_was_separator = false;
            ch
        } else if !previous_was_separator {
            previous_was_separator = true;
            '_'
        } else {
            continue;
        };
        normalized.push(next);
    }

    normalized.trim_matches('_').replace("__", "_")
}

/// Like [`canonical_topology_part`] but returns `fallback.to_owned()` when
/// normalization produces an empty string (e.g. the input was whitespace or
/// only non-canonicalizable characters).
#[must_use]
pub fn canonical_topology_part_or(value: &str, fallback: &str) -> String {
    let canonical = canonical_topology_part(value);
    if canonical.is_empty() {
        fallback.to_owned()
    } else {
        canonical
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::{
        broker_qn, config_map_qn, database_qn, deployment_qn, env_var_qn, parse_shared_symbol_qn,
        queue_qn, route_qn, secret_qn, shared_package_root, shared_symbol_qn,
        shared_symbol_qn_unversioned, topic_qn, virtual_node, virtual_node_id,
    };
    use crate::NodeKind;

    #[test]
    fn topic_ids_are_stable_across_repos() {
        let qn = topic_qn("kafka", "order.created");
        let producer = virtual_node(
            NodeKind::Topic,
            "producer",
            "src/events.ts",
            "order.created",
            qn.clone(),
        );
        let consumer = virtual_node(
            NodeKind::Topic,
            "consumer",
            "src/consumer.ts",
            "order.created",
            qn,
        );

        assert_eq!(producer.id, consumer.id);
    }

    #[test]
    fn route_ids_are_deterministic_regardless_of_creator() {
        let qn = route_qn("get", "v1/orders/:id");
        let first = virtual_node_id(NodeKind::Route, &qn);
        let second = virtual_node_id(NodeKind::Route, &qn);

        assert_eq!(first, second);
        assert_eq!(
            route_qn("GET", "/v1/orders/:id"),
            "__route__GET__/v1/orders/:id"
        );
    }

    #[test]
    fn shared_symbol_ids_include_package_and_version() {
        let qn = shared_symbol_qn("@workspace/shared-contracts", "2.3.1", "OrderStatus");
        let first = virtual_node_id(NodeKind::SharedSymbol, &qn);
        let second = virtual_node_id(
            NodeKind::SharedSymbol,
            "__shared__@workspace/shared-contracts@2.3.1__OrderStatus",
        );

        assert_eq!(first, second);
    }

    #[test]
    fn shared_symbol_helpers_support_versioned_and_unversioned_forms() {
        assert_eq!(
            parse_shared_symbol_qn("__shared__@workspace/shared-contracts@2.3.1__OrderStatus"),
            Some(super::SharedSymbolRef {
                package: Some("@workspace/shared-contracts"),
                version: Some("2.3.1"),
                symbol: "OrderStatus",
            })
        );
        assert_eq!(
            parse_shared_symbol_qn(&shared_symbol_qn_unversioned(
                "@workspace/shared-contracts/dtos",
                "OrderStatus"
            )),
            Some(super::SharedSymbolRef {
                package: Some("@workspace/shared-contracts/dtos"),
                version: None,
                symbol: "OrderStatus",
            })
        );
        assert_eq!(parse_shared_symbol_qn("__shared__OrderStatus"), None);
    }

    #[test]
    fn shared_package_root_normalizes_scoped_and_unscoped_specifiers() {
        assert_eq!(
            shared_package_root("@workspace/shared-contracts/dtos"),
            Some("@workspace/shared-contracts")
        );
        assert_eq!(shared_package_root("zod/v4"), Some("zod"));
        assert_eq!(shared_package_root("./local/barrel"), None);
    }

    #[test]
    fn queue_qn_is_protocol_aware() {
        assert_eq!(
            queue_qn("bull", "report-generation"),
            "__queue__bull__report-generation"
        );
    }

    #[test]
    fn deployment_topology_qns_are_canonical_and_redaction_safe() {
        assert_eq!(
            deployment_qn("Backend Standard", "API / Prod"),
            "__deployment__backend_standard__api_prod"
        );
        assert_eq!(env_var_qn(" DATABASE_URL "), "__env_var__database_url");
        assert_eq!(secret_qn("db/password"), "__secret__db_password");
        assert_eq!(config_map_qn("App Config"), "__config_map__app_config");
        assert_eq!(
            broker_qn("Kafka", "Broker.Internal:9092"),
            "__broker__kafka__broker.internal:9092"
        );
        assert_eq!(
            database_qn("Postgres", "Primary DB"),
            "__database__postgres__primary_db"
        );
        assert_eq!(database_qn("", ""), "__database__unknown__unknown");
    }

    #[test]
    fn route_qn_normalizes_route_noise() {
        assert_eq!(route_qn("GET", "/orders"), "__route__GET__/orders");
        assert_eq!(route_qn("GET", "/orders/"), "__route__GET__/orders");
        assert_eq!(
            route_qn("GET", "https://api.example.com/orders/?page=1#details"),
            "__route__GET__/orders"
        );
        assert_eq!(route_qn("GET", "/Orders/{ID}"), "__route__GET__/orders/:id");
        assert_eq!(route_qn("GET", "/orders/$id"), "__route__GET__/orders/:id");
        assert_eq!(route_qn("GET", "/orders/:Id"), "__route__GET__/orders/:id");
    }

    #[test]
    fn virtual_node_marks_external_id_and_virtual_flag() {
        let qualified_name = route_qn("POST", "/orders");
        let node = virtual_node(
            NodeKind::Route,
            "backend_standard",
            "src/controller.ts",
            "POST /orders",
            qualified_name.clone(),
        );

        assert!(node.is_virtual);
        assert_eq!(node.external_id.as_deref(), Some(qualified_name.as_str()));
        assert_eq!(
            node.qualified_name.as_deref(),
            Some(qualified_name.as_str())
        );
    }
}
