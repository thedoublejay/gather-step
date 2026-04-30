use core::fmt;

use smallvec::SmallVec;
use thiserror::Error;

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    bitcode::Encode,
    bitcode::Decode,
    serde::Serialize,
    serde::Deserialize,
)]
#[repr(u8)]
#[non_exhaustive]
pub enum NodeKind {
    File = 0,
    Function = 1,
    Class = 2,
    Type = 3,
    Module = 4,
    Import = 5,
    Decorator = 6,
    Entity = 7,
    Route = 8,
    Topic = 9,
    Queue = 10,
    Subject = 11,
    Stream = 12,
    Event = 13,
    SharedSymbol = 14,
    PayloadContract = 15,
    Repo = 16,
    Convention = 17,
    Service = 18,
    Commit = 19,
    PR = 20,
    Review = 21,
    Comment = 22,
    Author = 23,
    Ticket = 24,
    DataField = 25,
}

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    bitcode::Encode,
    bitcode::Decode,
    serde::Serialize,
    serde::Deserialize,
)]
#[repr(u8)]
#[non_exhaustive]
pub enum EdgeKind {
    Defines = 0,
    Calls = 1,
    Imports = 2,
    Exports = 3,
    Extends = 4,
    Implements = 5,
    References = 6,
    DependsOn = 7,
    UsesDecorator = 8,
    Publishes = 20,
    Consumes = 21,
    Triggers = 22,
    Serves = 23,
    PersistsTo = 24,
    UsesShared = 25,
    UsesTypeFrom = 26,
    UsesEventFrom = 27,
    UsesGuardFrom = 28,
    ConsumesApiFrom = 29,
    ProducesEventFor = 30,
    ImplementsContractFrom = 31,
    /// A symbol in one package imports a frontend hook export (function whose
    /// name starts with `use`) from another package via a cross-package path
    /// (e.g. `@workspace/frontend-shared`).  The edge runs from the importing
    /// symbol to the hook export node.
    ConsumesHookFrom = 32,
    ChangedIn = 40,
    IntroducedBy = 41,
    AuthoredBy = 42,
    ReviewedBy = 43,
    MergedAs = 44,
    CommentedOn = 45,
    Resolves = 60,
    RelatesTo = 61,
    PartOf = 62,
    BreaksIfChanged = 80,
    CoChangesWith = 81,
    OwnedBy = 82,
    CrossRepoDepends = 83,
    PropagatesEvent = 84,
    DriftsFrom = 85,
    ContractOn = 86,
    /// A migration symbol changes documents in a virtual database collection
    /// node such as `__migration_collection__alerts`.
    MigratesCollection = 87,
    ReadsField = 90,
    WritesField = 91,
    DerivesFieldFrom = 92,
    FiltersOnField = 93,
    IndexesField = 94,
    BackfillsField = 95,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("invalid {kind} discriminant: {value}")]
pub struct DiscriminantError {
    kind: &'static str,
    value: u8,
}

impl NodeKind {
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    pub const fn is_search_indexable(self) -> bool {
        // Search policy: index primary code/domain entities in Tantivy, and leave structural
        // nodes plus temporal review history to graph/SQLite until query patterns justify more.
        match self {
            Self::File
            | Self::Function
            | Self::Class
            | Self::Type
            | Self::Module
            | Self::Entity
            | Self::Route
            | Self::Topic
            | Self::Queue
            | Self::Subject
            | Self::Stream
            | Self::Event
            | Self::SharedSymbol
            | Self::PayloadContract
            | Self::Repo
            | Self::Convention
            | Self::Service
            | Self::DataField => true,
            Self::Import
            | Self::Decorator
            | Self::Commit
            | Self::PR
            | Self::Review
            | Self::Comment
            | Self::Author
            | Self::Ticket => false,
        }
    }

    pub const fn all() -> &'static [Self] {
        &[
            Self::File,
            Self::Function,
            Self::Class,
            Self::Type,
            Self::Module,
            Self::Import,
            Self::Decorator,
            Self::Entity,
            Self::Route,
            Self::Topic,
            Self::Queue,
            Self::Subject,
            Self::Stream,
            Self::Event,
            Self::SharedSymbol,
            Self::PayloadContract,
            Self::Repo,
            Self::Convention,
            Self::Service,
            Self::Commit,
            Self::PR,
            Self::Review,
            Self::Comment,
            Self::Author,
            Self::Ticket,
            Self::DataField,
        ]
    }
}

impl EdgeKind {
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Whether this edge kind represents a semantic bridge link
    /// connecting a real symbol to a virtual bridge node (`Route`, `Topic`,
    /// `SharedSymbol`, `PayloadContract`, …) — i.e. an edge whose correctness
    /// depends on cross-repo resolution, not just on local AST parsing.
    ///
    /// Reconciliation uses this to separate semantic-link health from
    /// structural call-edge health when reporting `ReconcileStats` and in
    /// `doctor` / `status` output.
    #[must_use]
    pub const fn is_semantic_bridge(self) -> bool {
        matches!(
            self,
            Self::Serves
                | Self::Consumes
                | Self::ConsumesApiFrom
                | Self::Publishes
                | Self::UsesEventFrom
                | Self::ProducesEventFor
                | Self::References
                | Self::Implements
                | Self::UsesShared
                | Self::UsesTypeFrom
                | Self::UsesGuardFrom
                | Self::ImplementsContractFrom
                | Self::ContractOn
                | Self::DriftsFrom
                | Self::PropagatesEvent
                | Self::ConsumesHookFrom
        )
    }

    pub const fn all() -> &'static [Self] {
        &[
            Self::Defines,
            Self::Calls,
            Self::Imports,
            Self::Exports,
            Self::Extends,
            Self::Implements,
            Self::References,
            Self::DependsOn,
            Self::UsesDecorator,
            Self::Publishes,
            Self::Consumes,
            Self::Triggers,
            Self::Serves,
            Self::PersistsTo,
            Self::UsesShared,
            Self::UsesTypeFrom,
            Self::UsesEventFrom,
            Self::UsesGuardFrom,
            Self::ConsumesApiFrom,
            Self::ProducesEventFor,
            Self::ImplementsContractFrom,
            Self::ConsumesHookFrom,
            Self::ChangedIn,
            Self::IntroducedBy,
            Self::AuthoredBy,
            Self::ReviewedBy,
            Self::MergedAs,
            Self::CommentedOn,
            Self::Resolves,
            Self::RelatesTo,
            Self::PartOf,
            Self::BreaksIfChanged,
            Self::CoChangesWith,
            Self::OwnedBy,
            Self::CrossRepoDepends,
            Self::PropagatesEvent,
            Self::DriftsFrom,
            Self::ContractOn,
            Self::MigratesCollection,
            Self::ReadsField,
            Self::WritesField,
            Self::DerivesFieldFrom,
            Self::FiltersOnField,
            Self::IndexesField,
            Self::BackfillsField,
        ]
    }
}

impl TryFrom<u8> for NodeKind {
    type Error = DiscriminantError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::File),
            1 => Ok(Self::Function),
            2 => Ok(Self::Class),
            3 => Ok(Self::Type),
            4 => Ok(Self::Module),
            5 => Ok(Self::Import),
            6 => Ok(Self::Decorator),
            7 => Ok(Self::Entity),
            8 => Ok(Self::Route),
            9 => Ok(Self::Topic),
            10 => Ok(Self::Queue),
            11 => Ok(Self::Subject),
            12 => Ok(Self::Stream),
            13 => Ok(Self::Event),
            14 => Ok(Self::SharedSymbol),
            15 => Ok(Self::PayloadContract),
            16 => Ok(Self::Repo),
            17 => Ok(Self::Convention),
            18 => Ok(Self::Service),
            19 => Ok(Self::Commit),
            20 => Ok(Self::PR),
            21 => Ok(Self::Review),
            22 => Ok(Self::Comment),
            23 => Ok(Self::Author),
            24 => Ok(Self::Ticket),
            25 => Ok(Self::DataField),
            _ => Err(DiscriminantError {
                kind: "NodeKind",
                value,
            }),
        }
    }
}

impl TryFrom<u8> for EdgeKind {
    type Error = DiscriminantError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Defines),
            1 => Ok(Self::Calls),
            2 => Ok(Self::Imports),
            3 => Ok(Self::Exports),
            4 => Ok(Self::Extends),
            5 => Ok(Self::Implements),
            6 => Ok(Self::References),
            7 => Ok(Self::DependsOn),
            8 => Ok(Self::UsesDecorator),
            20 => Ok(Self::Publishes),
            21 => Ok(Self::Consumes),
            22 => Ok(Self::Triggers),
            23 => Ok(Self::Serves),
            24 => Ok(Self::PersistsTo),
            25 => Ok(Self::UsesShared),
            26 => Ok(Self::UsesTypeFrom),
            27 => Ok(Self::UsesEventFrom),
            28 => Ok(Self::UsesGuardFrom),
            29 => Ok(Self::ConsumesApiFrom),
            30 => Ok(Self::ProducesEventFor),
            31 => Ok(Self::ImplementsContractFrom),
            32 => Ok(Self::ConsumesHookFrom),
            40 => Ok(Self::ChangedIn),
            41 => Ok(Self::IntroducedBy),
            42 => Ok(Self::AuthoredBy),
            43 => Ok(Self::ReviewedBy),
            44 => Ok(Self::MergedAs),
            45 => Ok(Self::CommentedOn),
            60 => Ok(Self::Resolves),
            61 => Ok(Self::RelatesTo),
            62 => Ok(Self::PartOf),
            80 => Ok(Self::BreaksIfChanged),
            81 => Ok(Self::CoChangesWith),
            82 => Ok(Self::OwnedBy),
            83 => Ok(Self::CrossRepoDepends),
            84 => Ok(Self::PropagatesEvent),
            85 => Ok(Self::DriftsFrom),
            86 => Ok(Self::ContractOn),
            87 => Ok(Self::MigratesCollection),
            90 => Ok(Self::ReadsField),
            91 => Ok(Self::WritesField),
            92 => Ok(Self::DerivesFieldFrom),
            93 => Ok(Self::FiltersOnField),
            94 => Ok(Self::IndexesField),
            95 => Ok(Self::BackfillsField),
            _ => Err(DiscriminantError {
                kind: "EdgeKind",
                value,
            }),
        }
    }
}

impl fmt::Display for NodeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl fmt::Display for EdgeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// The kind of cross-repo relationship a [`PlanningProof`] captures.
///
/// Each variant maps to a distinct class of edges in the graph and carries a
/// canonical strength band (see [`PlanningProof::strength`]).
///
/// | Variant                  | Strength band |
/// |--------------------------|---------------|
/// | `DirectCall`             | ≥ 67 (85)     |
/// | `EventProducerConsumer`  | ≥ 67 (80)     |
/// | `GuardUsage`             | ≥ 67 (80)     |
/// | `SharedContractConsumer` | ≥ 67 (75)     |
/// | `ProjectionFieldEvidence` | ≥ 67 (72)    |
/// | `RouteClientServer`      | ≥ 67 (70)     |
/// | `ImportBridge`           | 33–67 (55)    |
/// | `CoChangeAdvisory`       | < 33 (25)     |
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[non_exhaustive]
pub enum ProofKind {
    /// A direct `Calls` edge crosses a repo boundary.
    DirectCall,
    /// A `ProducesEventFor` / `UsesEventFrom` edge pair links two repos through
    /// a shared event or topic.
    EventProducerConsumer,
    /// A `UsesGuardFrom` edge references an auth/guard defined in another repo.
    GuardUsage,
    /// A `UsesTypeFrom` edge consumes a shared type or contract from another
    /// repo's canonical package.
    SharedContractConsumer,
    /// A field-level read/write/filter/index/backfill/derivation edge links a
    /// projection data field to another repo.
    ProjectionFieldEvidence,
    /// A `Calls` + `ConsumesApiFrom` pair represents an HTTP client/server
    /// boundary across repos.
    RouteClientServer,
    /// An `Imports` edge crosses package boundaries (weaker than a structural
    /// semantic edge).
    ImportBridge,
    /// Only `CoChangesWith` edges were found; the relationship is inferred from
    /// historical co-edit patterns rather than declared code structure.
    CoChangeAdvisory,
}

/// A single node visited while walking the evidence path for a [`PlanningProof`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProofHop {
    /// The graph node at this position in the path.
    pub node_id: crate::graph::NodeId,
    /// The edge kind used to reach this hop from the previous one.
    pub edge_kind: EdgeKind,
    /// Owning repo of the node at this hop.
    pub repo: String,
}

/// A machine-readable justification for why a repo pair appears in the pack
/// response.
///
/// Proofs are derived as a single projection over the edge graph; the legacy
/// `confirmed_downstream_repos`, `probable_downstream_repos`, and
/// `cross_repo_callers` fields on the pack response are populated from the
/// proof builder's output so all three sources share the same traversal.
///
/// # Strength bands
///
/// The `strength` field encodes confidence on a 0–100 scale and is assigned
/// based on the edge kinds observed:
///
/// - `CoChangeAdvisory` → STRICTLY less than 33.
/// - Bridge/import kinds (`ImportBridge`) → 33–67.
/// - Structural confirmed kinds (all others) → ≥ 67.
///
/// Oracle assertions depend on these ranges; do not reassign a kind to a
/// different band.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PlanningProof {
    /// Coarse classification of the relationship.
    pub kind: ProofKind,
    /// Confidence score in the range 0–100.
    ///
    /// Invariant: `CoChangeAdvisory` < 33, `ImportBridge` in 33–67,
    /// all other kinds ≥ 67.
    pub strength: u8,
    /// Repo containing the source symbol (usually the pack anchor's repo).
    pub source_repo: String,
    /// Repo that this proof establishes as a downstream or related consumer.
    pub target_repo: String,
    /// File path within `source_repo` where the relationship originates.
    pub source_file: String,
    /// File path within `target_repo` where the relationship is consumed.
    pub target_file: String,
    /// All distinct edge kinds observed on the path.
    pub edge_kinds: SmallVec<[EdgeKind; 4]>,
    /// The traversal path from anchor to the evidence node.
    ///
    /// Capped at 8 hops; see `path_truncated`.
    pub path: Vec<ProofHop>,
    /// `true` when the original path was longer than 8 hops and was truncated.
    pub path_truncated: bool,
}

impl PlanningProof {
    /// Maximum number of hops stored in [`PlanningProof::path`].
    pub const MAX_PATH_HOPS: usize = 8;

    /// Returns `true` when the `strength` value falls in the advisory band
    /// (< 33), corresponding to co-change-only evidence.
    #[must_use]
    pub fn is_advisory(&self) -> bool {
        self.strength < 33
    }

    /// Returns `true` when the `strength` value falls in the confirmed
    /// structural band (≥ 67).
    #[must_use]
    pub fn is_structural(&self) -> bool {
        self.strength >= 67
    }
}

/// Sort key for a [`PlanningProof`]: strength DESC, kind ordinal ASC, then
/// `(source_repo, target_repo)` for determinism.
pub fn proof_sort_key(proof: &PlanningProof) -> impl Ord {
    (
        std::cmp::Reverse(proof.strength),
        proof.kind,
        proof.source_repo.clone(),
        proof.target_repo.clone(),
    )
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::{EdgeKind, NodeKind};

    #[test]
    fn node_kind_round_trips_through_u8() {
        let original = NodeKind::PayloadContract;
        let decoded = NodeKind::try_from(original.as_u8()).expect("node kind should decode");

        assert_eq!(decoded, original);
    }

    #[test]
    fn edge_kind_round_trips_through_u8() {
        let original = EdgeKind::MigratesCollection;
        let decoded = EdgeKind::try_from(original.as_u8()).expect("edge kind should decode");

        assert_eq!(decoded, original);
    }

    #[test]
    fn search_indexable_policy_excludes_structural_and_temporal_kinds() {
        assert!(NodeKind::Function.is_search_indexable());
        assert!(NodeKind::Event.is_search_indexable());
        assert!(NodeKind::DataField.is_search_indexable());
        assert!(!NodeKind::Import.is_search_indexable());
        assert!(!NodeKind::Decorator.is_search_indexable());
        assert!(!NodeKind::PR.is_search_indexable());
        assert!(!NodeKind::Ticket.is_search_indexable());
    }

    #[test]
    fn node_kind_invalid_u8_rejects() {
        for value in [26_u8, 50, 100, 255] {
            assert!(NodeKind::try_from(value).is_err(), "{value} should reject");
        }
    }

    #[test]
    fn edge_kind_invalid_u8_rejects() {
        for value in [9_u8, 19, 33, 39, 46, 59, 63, 79, 88, 89, 100, 255] {
            assert!(EdgeKind::try_from(value).is_err(), "{value} should reject");
        }
    }

    #[test]
    fn kind_display_uses_debug_name() {
        assert_eq!(NodeKind::Function.to_string(), "Function");
        assert_eq!(EdgeKind::CrossRepoDepends.to_string(), "CrossRepoDepends");
    }
}
