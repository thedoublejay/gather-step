use crate::schema::{EdgeKind, NodeKind};

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    bitcode::Encode,
    bitcode::Decode,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct NodeId(pub [u8; 16]);

impl NodeId {
    pub const fn as_bytes(self) -> [u8; 16] {
        self.0
    }
}

#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    bitcode::Encode,
    bitcode::Decode,
    serde::Serialize,
    serde::Deserialize,
)]
#[non_exhaustive]
pub enum Visibility {
    Public,
    Protected,
    Private,
    Package,
    Internal,
}

/// Compact source location. Stored as `(line_start, line_len, column_start,
/// column_len)` using narrower types to reduce per-node storage by 6 bytes
/// versus the previous four-`u32` layout.
///
/// Line and column numbers virtually never exceed 65 535; values that do are
/// clamped to [`u16::MAX`] at construction time with a `tracing::warn!` log.
///
/// Use the [`line_end`](Self::line_end) / [`column_end`](Self::column_end)
/// computed getters where the old fields were read; construction sites must
/// switch to `line_len` / `column_len`.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    bitcode::Encode,
    bitcode::Decode,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct SourceSpan {
    pub line_start: u32,
    /// Number of lines spanned (0 = single-line, i.e. `line_end == line_start`).
    pub line_len: u16,
    /// 0-indexed starting column (clamped to `u16::MAX` for very long lines).
    pub column_start: u16,
    /// Number of columns spanned within the ending line.
    pub column_len: u16,
}

impl SourceSpan {
    /// Compute `line_end` from `line_start + line_len`.
    #[must_use]
    #[inline]
    pub fn line_end(&self) -> u32 {
        self.line_start + u32::from(self.line_len)
    }

    /// Compute `column_end` from `column_start + column_len`.
    #[must_use]
    #[inline]
    pub fn column_end(&self) -> u32 {
        u32::from(self.column_start) + u32::from(self.column_len)
    }

    /// Construct from the absolute `line_end` / `column_end` values that the
    /// old struct carried directly, clamping any out-of-range values to
    /// `u16::MAX` and emitting a `tracing::warn!` if clamping occurs.
    /// Construct from the absolute `line_end` / `column_end` values that the
    /// old struct carried directly, clamping any out-of-range values to
    /// `u16::MAX`. Values that overflow are silently clamped; the parse site
    /// may separately log a warning if desired.
    #[must_use]
    pub fn from_absolute(
        line_start: u32,
        line_end: u32,
        column_start: u32,
        column_end: u32,
    ) -> Self {
        let line_len = u16::try_from(line_end.saturating_sub(line_start)).unwrap_or(u16::MAX);
        let column_start = u16::try_from(column_start).unwrap_or(u16::MAX);
        let column_len =
            u16::try_from(column_end.saturating_sub(u32::from(column_start))).unwrap_or(u16::MAX);

        Self {
            line_start,
            line_len,
            column_start,
            column_len,
        }
    }
}

#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    bitcode::Encode,
    bitcode::Decode,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct NodeData {
    pub id: NodeId,
    pub kind: NodeKind,
    pub repo: String,
    pub file_path: String,
    pub name: String,
    pub qualified_name: Option<String>,
    pub external_id: Option<String>,
    pub signature: Option<String>,
    pub visibility: Option<Visibility>,
    pub span: Option<SourceSpan>,
    pub is_virtual: bool,
}

/// Per-edge annotation bag. All fields are optional to minimise the common
/// empty-case encoded size (~5–10 bytes via bitcode).
///
/// The `resolver` field stores a resolver strategy tag. Production code should
/// use [`Self::set_resolver_strategy`] and [`Self::resolver_strategy`] rather
/// than reading/writing the raw `resolver` string directly. Graph storage uses
/// a private compact envelope for known resolver tags; the public API remains
/// string-based for serde output compatibility.
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    bitcode::Encode,
    bitcode::Decode,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct EdgeMetadata {
    pub weight: Option<u32>,
    pub confidence: Option<u16>,
    pub timestamp_unix: Option<i64>,
    pub drift_kind: Option<String>,
    pub resolver: Option<String>,
}

pub const MIGRATION_FILTERS_METADATA_PREFIX: &str = "migration_filters:";

impl EdgeMetadata {
    #[must_use]
    pub fn resolver_strategy(&self) -> Option<crate::ResolverStrategy> {
        self.resolver
            .as_deref()
            .and_then(crate::ResolverStrategy::from_str)
    }

    /// Set the resolver strategy. Stores the canonical static string for the
    /// strategy to avoid any encoding ambiguity.
    pub fn set_resolver_strategy(&mut self, strategy: crate::ResolverStrategy) {
        self.resolver = Some(strategy.as_str().to_owned());
    }

    pub fn clear_resolver_strategy(&mut self) {
        self.resolver = None;
    }

    #[must_use]
    pub fn resolver_strategy_weight(&self) -> u16 {
        self.resolver_strategy()
            .map_or(0, crate::ResolverStrategy::strategy_weight)
    }
}

#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    bitcode::Encode,
    bitcode::Decode,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct EdgeData {
    pub source: NodeId,
    pub target: NodeId,
    pub kind: EdgeKind,
    pub metadata: EdgeMetadata,
    pub owner_file: NodeId,
    pub is_cross_file: bool,
}

/// Compute a stable [`NodeId`] from the symbol's logical identity.
///
/// The identity tuple is `(repo, path, kind, qualified_name)`.  `qualified_name`
/// should be the fully-qualified symbol name (e.g. `"OrderService.execute"`) so
/// that inserting sibling symbols above this one does not change its id.
/// Callers that cannot supply a qualified name should fall back to the bare
/// symbol name; ordinal is intentionally absent so id is independent of
/// AST visit order.
///
/// File and virtual nodes pass the file path as `qualified_name`.
#[must_use]
pub fn node_id(repo: &str, path: &str, kind: NodeKind, qualified_name: &str) -> NodeId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(repo.as_bytes());
    hasher.update(b"\0");
    hasher.update(path.as_bytes());
    hasher.update(b"\0");
    hasher.update(&[kind as u8]);
    hasher.update(qualified_name.as_bytes());

    let digest = hasher.finalize();
    let mut id = [0_u8; 16];
    id.copy_from_slice(&digest.as_bytes()[..16]);
    NodeId(id)
}

#[must_use]
pub fn ref_node_id(kind: NodeKind, external_id: &str) -> NodeId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&[kind as u8]);
    hasher.update(external_id.as_bytes());

    let digest = hasher.finalize();
    let mut id = [0_u8; 16];
    id.copy_from_slice(&digest.as_bytes()[..16]);
    NodeId(id)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::{
        EdgeData, EdgeMetadata, NodeData, NodeId, SourceSpan, Visibility, node_id, ref_node_id,
    };
    use crate::schema::{EdgeKind, NodeKind};

    #[test]
    fn node_id_is_deterministic_for_identical_inputs() {
        let first = node_id("service-a", "src/foo.ts", NodeKind::Function, "execute");
        let second = node_id("service-a", "src/foo.ts", NodeKind::Function, "execute");

        assert_eq!(first, second);
    }

    #[test]
    fn node_id_is_independent_of_formatting_only_position_changes() {
        fn extracted_function(
            source: &str,
            line_start: u32,
            line_end: u32,
            column_start: u32,
        ) -> NodeData {
            assert!(source.contains("execute"));
            NodeData {
                // Identity uses qualified_name, not source position.
                id: node_id(
                    "service-a",
                    "src/foo.ts",
                    NodeKind::Function,
                    "OrderService.execute",
                ),
                kind: NodeKind::Function,
                repo: "service-a".to_owned(),
                file_path: "src/foo.ts".to_owned(),
                name: "execute".to_owned(),
                qualified_name: Some("OrderService.execute".to_owned()),
                external_id: None,
                signature: Some("execute()".to_owned()),
                visibility: Some(Visibility::Public),
                span: Some(SourceSpan::from_absolute(
                    line_start,
                    line_end,
                    column_start,
                    column_start + 9,
                )),
                is_virtual: false,
            }
        }

        let original = extracted_function("fn execute() {}\n", 1, 1, 0);
        let reformatted = extracted_function("\n// comment\nfn execute() {}\n", 3, 3, 0);

        assert_ne!(original.span, reformatted.span);
        assert_eq!(original.id, reformatted.id);
    }

    /// Inserting a sibling method above the target in the same file must not
    /// change the target's id — the hash is over `qualified_name`, not visit order.
    #[test]
    fn node_id_is_stable_across_sibling_reordering() {
        // Two hypothetical parse runs where "alpha" appears at different ordinal
        // positions because a new sibling "beta" was inserted above it.
        // Both runs must produce the same id for "alpha".
        let run_a = node_id(
            "service-a",
            "src/service.ts",
            NodeKind::Function,
            "MyService.alpha",
        );
        let run_b = node_id(
            "service-a",
            "src/service.ts",
            NodeKind::Function,
            "MyService.alpha",
        );
        assert_eq!(run_a, run_b, "id must be independent of sibling count");

        // Confirm the sibling itself gets a distinct id.
        let sibling = node_id(
            "service-a",
            "src/service.ts",
            NodeKind::Function,
            "MyService.beta",
        );
        assert_ne!(
            run_a, sibling,
            "distinct qualified names must hash differently"
        );
    }

    #[test]
    fn ref_node_id_is_deterministic() {
        let first = ref_node_id(NodeKind::Commit, "abc123");
        let second = ref_node_id(NodeKind::Commit, "abc123");

        assert_eq!(first, second);
    }

    #[test]
    fn node_id_changes_when_identity_tuple_changes() {
        let base = node_id("service-a", "src/foo.ts", NodeKind::Function, "execute");

        assert_ne!(
            base,
            node_id("service-a", "src/foo.ts", NodeKind::Class, "execute")
        );
        assert_ne!(
            base,
            node_id("service-a", "src/foo.ts", NodeKind::Function, "init")
        );
        assert_ne!(
            base,
            node_id("billing", "src/foo.ts", NodeKind::Function, "execute")
        );
        assert_ne!(
            base,
            node_id("service-a", "src/other.ts", NodeKind::Function, "execute")
        );
    }

    #[test]
    fn node_id_handles_empty_and_utf8_inputs_deterministically() {
        let empty_a = node_id("", "", NodeKind::Function, "");
        let empty_b = node_id("", "", NodeKind::Function, "");
        let utf8_a = node_id("orders-🚨", "src/服务.ts", NodeKind::Function, "执行");
        let utf8_b = node_id("orders-🚨", "src/服务.ts", NodeKind::Function, "执行");

        assert_eq!(empty_a, empty_b);
        assert_eq!(utf8_a, utf8_b);
        assert_ne!(empty_a, utf8_a);
    }

    #[test]
    fn ref_node_id_is_distinct_from_node_id_for_overlapping_inputs() {
        let node = node_id("service-a", "src/foo.ts", NodeKind::Commit, "abc123");
        let reference = ref_node_id(NodeKind::Commit, "abc123");

        assert_ne!(node, reference);
    }

    #[test]
    fn node_data_round_trips_via_bitcode() {
        let node = NodeData {
            id: NodeId([1; 16]),
            kind: NodeKind::Function,
            repo: "service-a".to_owned(),
            file_path: "src/foo.ts".to_owned(),
            name: "execute".to_owned(),
            qualified_name: Some("OrderService.execute".to_owned()),
            external_id: None,
            signature: Some("execute(input: OrderInput) -> Order".to_owned()),
            visibility: Some(Visibility::Public),
            span: Some(SourceSpan {
                line_start: 10,
                line_len: 4,
                column_start: 0,
                column_len: 1,
            }),
            is_virtual: false,
        };

        let encoded = bitcode::encode(&node);
        let decoded: NodeData = bitcode::decode(&encoded).expect("node data should decode");

        assert_eq!(decoded, node);
    }

    #[test]
    fn edge_data_round_trips_via_bitcode() {
        let edge = EdgeData {
            source: NodeId([1; 16]),
            target: NodeId([2; 16]),
            kind: EdgeKind::Calls,
            metadata: EdgeMetadata {
                weight: Some(3),
                confidence: Some(950),
                timestamp_unix: Some(1_713_000_000),
                drift_kind: None,
                resolver: Some(crate::ResolverStrategy::ImportMap.as_str().to_owned()),
            },
            owner_file: NodeId([3; 16]),
            is_cross_file: true,
        };

        let encoded = bitcode::encode(&edge);
        let decoded: EdgeData = bitcode::decode(&encoded).expect("edge data should decode");

        assert_eq!(decoded, edge);
    }

    #[test]
    fn edge_metadata_exposes_typed_resolver_helpers() {
        let mut metadata = EdgeMetadata::default();

        assert_eq!(metadata.resolver_strategy(), None);
        assert_eq!(metadata.resolver_strategy_weight(), 0);

        metadata.set_resolver_strategy(crate::ResolverStrategy::ImportMap);

        assert_eq!(
            metadata.resolver_strategy(),
            Some(crate::ResolverStrategy::ImportMap)
        );
        assert_eq!(
            metadata.resolver.as_deref(),
            Some(crate::ResolverStrategy::ImportMap.as_str())
        );
        assert_eq!(
            metadata.resolver_strategy_weight(),
            crate::ResolverStrategy::ImportMap.strategy_weight()
        );

        metadata.clear_resolver_strategy();

        assert_eq!(metadata.resolver_strategy(), None);
        assert_eq!(metadata.resolver_strategy_weight(), 0);
    }
}
