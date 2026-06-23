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
    Deployment = 26,
    EnvVar = 27,
    Secret = 28,
    ConfigMap = 29,
    WorkflowJob = 30,
    Broker = 31,
    Database = 32,
    // AI-flow kinds (v5). Tier-1 only: constructs that back a cross-repo
    // convergence virtual node or are a first-class rendering/contract target.
    // LlmCall/Tool/Agent/AgentNode/Embedder/McpClient are `ai_role` facets, not kinds.
    AgentGraph = 33,
    Prompt = 34,
    AiContract = 35,
    VectorIndex = 36,
    McpServer = 37,
    McpTool = 38,
    LlmModel = 39,
    /// Virtual node representing a value mirrored verbatim across repo boundaries.
    ValueMirror = 40,
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
    /// A mirror endpoint links a call site to the `__value__` virtual node for
    /// a value carried verbatim across repo boundaries.
    MirrorsValueFrom = 88,
    /// A guard endpoint (switch/if branch keyed on an enum value) links to the
    /// `__value__` virtual node. Distinguishes a guard surface from an array
    /// `MirrorsValueFrom` mirror; carries `EdgeMetadata.guard_has_default`.
    GuardsEnumValue = 89,
    ReadsField = 90,
    WritesField = 91,
    DerivesFieldFrom = 92,
    FiltersOnField = 93,
    IndexesField = 94,
    BackfillsField = 95,
    DeployedAs = 100,
    ReadsEnv = 101,
    BackedBy = 102,
    BuiltBy = 103,
    UsesBroker = 104,
    UsesDatabase = 105,
    // AI-flow edges (v5). New range 110+ (existing discriminants are sparse,
    // grouped by semantic range; 110+ is the next free range, not "next int").
    /// An agent graph defines an internal node (faceted function).
    DefinesAgentNode = 110,
    /// A graph node transitions to another node. Conditional-vs-fixed routing
    /// is carried in edge metadata via a typed accessor, not `drift_kind`.
    GraphTransitionsTo = 111,
    /// An agent graph / agent composes another agent.
    ComposesAgent = 112,
    /// An agent spawns a sub-agent at runtime (e.g. `spawn_subagent`).
    SpawnsSubagent = 113,
    /// An agent or graph node binds a tool the LLM may call.
    BindsTool = 114,
    /// A call site invokes an LLM; target is the converged `LlmModel` node.
    InvokesLlm = 115,
    /// A call site produces a structured-output `AiContract`.
    ProducesAiContract = 116,
    /// A symbol uses a managed `Prompt` artifact.
    UsesPrompt = 117,
    /// Cross-repo: a consumer fetches a prompt from a prompt service by `keyName`.
    FetchesPromptFrom = 118,
    /// A tool or graph node retrieves from a `VectorIndex`.
    RetrievesFrom = 119,
    /// A symbol embeds text via an embedding endpoint (cross-repo to an embedding service).
    Embeds = 120,
    /// A collection is indexed into a `VectorIndex`.
    IndexesVector = 121,
    /// An MCP client calls a tool on the converged `McpTool` node.
    CallsMcpTool = 122,
    /// An MCP server exposes a tool (converged `McpTool` node).
    ExposesMcpTool = 123,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("invalid {kind} discriminant: {value}")]
pub struct DiscriminantError {
    kind: &'static str,
    value: u8,
}

impl NodeKind {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Function => "function",
            Self::Class => "class",
            Self::Type => "type",
            Self::Module => "module",
            Self::Import => "import",
            Self::Decorator => "decorator",
            Self::Entity => "entity",
            Self::Route => "route",
            Self::Topic => "topic",
            Self::Queue => "queue",
            Self::Subject => "subject",
            Self::Stream => "stream",
            Self::Event => "event",
            Self::SharedSymbol => "shared_symbol",
            Self::PayloadContract => "payload_contract",
            Self::Repo => "repo",
            Self::Convention => "convention",
            Self::Service => "service",
            Self::Commit => "commit",
            Self::PR => "pr",
            Self::Review => "review",
            Self::Comment => "comment",
            Self::Author => "author",
            Self::Ticket => "ticket",
            Self::DataField => "data_field",
            Self::Deployment => "deployment",
            Self::EnvVar => "env_var",
            Self::Secret => "secret",
            Self::ConfigMap => "config_map",
            Self::WorkflowJob => "workflow_job",
            Self::Broker => "broker",
            Self::Database => "database",
            Self::AgentGraph => "agent_graph",
            Self::Prompt => "prompt",
            Self::AiContract => "ai_contract",
            Self::VectorIndex => "vector_index",
            Self::McpServer => "mcp_server",
            Self::McpTool => "mcp_tool",
            Self::LlmModel => "llm_model",
            Self::ValueMirror => "value_mirror",
        }
    }

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
            | Self::DataField
            | Self::Deployment
            | Self::EnvVar
            | Self::ConfigMap
            | Self::WorkflowJob
            | Self::Broker
            | Self::Database
            | Self::AgentGraph
            | Self::Prompt
            | Self::AiContract
            | Self::VectorIndex
            | Self::McpServer
            | Self::McpTool
            | Self::LlmModel
            | Self::ValueMirror => true,
            Self::Import
            | Self::Decorator
            | Self::Commit
            | Self::PR
            | Self::Review
            | Self::Comment
            | Self::Author
            | Self::Ticket
            | Self::Secret => false,
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
            Self::Deployment,
            Self::EnvVar,
            Self::Secret,
            Self::ConfigMap,
            Self::WorkflowJob,
            Self::Broker,
            Self::Database,
            Self::AgentGraph,
            Self::Prompt,
            Self::AiContract,
            Self::VectorIndex,
            Self::McpServer,
            Self::McpTool,
            Self::LlmModel,
            Self::ValueMirror,
        ]
    }
}

impl EdgeKind {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Defines => "defines",
            Self::Calls => "calls",
            Self::Imports => "imports",
            Self::Exports => "exports",
            Self::Extends => "extends",
            Self::Implements => "implements",
            Self::References => "references",
            Self::DependsOn => "depends_on",
            Self::UsesDecorator => "uses_decorator",
            Self::Publishes => "publishes",
            Self::Consumes => "consumes",
            Self::Triggers => "triggers",
            Self::Serves => "serves",
            Self::PersistsTo => "persists_to",
            Self::UsesShared => "uses_shared",
            Self::UsesTypeFrom => "uses_type_from",
            Self::UsesEventFrom => "uses_event_from",
            Self::UsesGuardFrom => "uses_guard_from",
            Self::ConsumesApiFrom => "consumes_api_from",
            Self::ProducesEventFor => "produces_event_for",
            Self::ImplementsContractFrom => "implements_contract_from",
            Self::ConsumesHookFrom => "consumes_hook_from",
            Self::ChangedIn => "changed_in",
            Self::IntroducedBy => "introduced_by",
            Self::AuthoredBy => "authored_by",
            Self::ReviewedBy => "reviewed_by",
            Self::MergedAs => "merged_as",
            Self::CommentedOn => "commented_on",
            Self::Resolves => "resolves",
            Self::RelatesTo => "relates_to",
            Self::PartOf => "part_of",
            Self::BreaksIfChanged => "breaks_if_changed",
            Self::CoChangesWith => "co_changes_with",
            Self::OwnedBy => "owned_by",
            Self::CrossRepoDepends => "cross_repo_depends",
            Self::PropagatesEvent => "propagates_event",
            Self::DriftsFrom => "drifts_from",
            Self::ContractOn => "contract_on",
            Self::MigratesCollection => "migrates_collection",
            Self::MirrorsValueFrom => "mirrors_value_from",
            Self::GuardsEnumValue => "guards_enum_value",
            Self::ReadsField => "reads_field",
            Self::WritesField => "writes_field",
            Self::DerivesFieldFrom => "derives_field_from",
            Self::FiltersOnField => "filters_on_field",
            Self::IndexesField => "indexes_field",
            Self::BackfillsField => "backfills_field",
            Self::DeployedAs => "deployed_as",
            Self::ReadsEnv => "reads_env",
            Self::BackedBy => "backed_by",
            Self::BuiltBy => "built_by",
            Self::UsesBroker => "uses_broker",
            Self::UsesDatabase => "uses_database",
            Self::DefinesAgentNode => "defines_agent_node",
            Self::GraphTransitionsTo => "graph_transitions_to",
            Self::ComposesAgent => "composes_agent",
            Self::SpawnsSubagent => "spawns_subagent",
            Self::BindsTool => "binds_tool",
            Self::InvokesLlm => "invokes_llm",
            Self::ProducesAiContract => "produces_ai_contract",
            Self::UsesPrompt => "uses_prompt",
            Self::FetchesPromptFrom => "fetches_prompt_from",
            Self::RetrievesFrom => "retrieves_from",
            Self::Embeds => "embeds",
            Self::IndexesVector => "indexes_vector",
            Self::CallsMcpTool => "calls_mcp_tool",
            Self::ExposesMcpTool => "exposes_mcp_tool",
        }
    }

    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Whether this edge represents a *consumer*/usage of the target rather
    /// than a structural `Defines` (file→symbol) or `Imports` edge. Used to
    /// count real consumers for reuse ranking and resolution scoring, so a
    /// "consumer count" reflects callers/users, not raw inbound-edge volume.
    #[must_use]
    pub const fn is_consumer_edge(self) -> bool {
        !matches!(self, Self::Defines | Self::Imports)
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
                | Self::DeployedAs
                | Self::ReadsEnv
                | Self::BackedBy
                | Self::BuiltBy
                | Self::UsesBroker
                | Self::UsesDatabase
                // AI cross-repo convergence edges (same class as ConsumesApiFrom /
                // UsesEventFrom). Intra-service AI edges (GraphTransitionsTo,
                // InvokesLlm, BindsTool, …) are deliberately NOT bridges.
                | Self::FetchesPromptFrom
                | Self::Embeds
                | Self::CallsMcpTool
                | Self::RetrievesFrom
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
            Self::MirrorsValueFrom,
            Self::GuardsEnumValue,
            Self::ReadsField,
            Self::WritesField,
            Self::DerivesFieldFrom,
            Self::FiltersOnField,
            Self::IndexesField,
            Self::BackfillsField,
            Self::DeployedAs,
            Self::ReadsEnv,
            Self::BackedBy,
            Self::BuiltBy,
            Self::UsesBroker,
            Self::UsesDatabase,
            Self::DefinesAgentNode,
            Self::GraphTransitionsTo,
            Self::ComposesAgent,
            Self::SpawnsSubagent,
            Self::BindsTool,
            Self::InvokesLlm,
            Self::ProducesAiContract,
            Self::UsesPrompt,
            Self::FetchesPromptFrom,
            Self::RetrievesFrom,
            Self::Embeds,
            Self::IndexesVector,
            Self::CallsMcpTool,
            Self::ExposesMcpTool,
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
            26 => Ok(Self::Deployment),
            27 => Ok(Self::EnvVar),
            28 => Ok(Self::Secret),
            29 => Ok(Self::ConfigMap),
            30 => Ok(Self::WorkflowJob),
            31 => Ok(Self::Broker),
            32 => Ok(Self::Database),
            33 => Ok(Self::AgentGraph),
            34 => Ok(Self::Prompt),
            35 => Ok(Self::AiContract),
            36 => Ok(Self::VectorIndex),
            37 => Ok(Self::McpServer),
            38 => Ok(Self::McpTool),
            39 => Ok(Self::LlmModel),
            40 => Ok(Self::ValueMirror),
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
            88 => Ok(Self::MirrorsValueFrom),
            89 => Ok(Self::GuardsEnumValue),
            90 => Ok(Self::ReadsField),
            91 => Ok(Self::WritesField),
            92 => Ok(Self::DerivesFieldFrom),
            93 => Ok(Self::FiltersOnField),
            94 => Ok(Self::IndexesField),
            95 => Ok(Self::BackfillsField),
            100 => Ok(Self::DeployedAs),
            101 => Ok(Self::ReadsEnv),
            102 => Ok(Self::BackedBy),
            103 => Ok(Self::BuiltBy),
            104 => Ok(Self::UsesBroker),
            105 => Ok(Self::UsesDatabase),
            110 => Ok(Self::DefinesAgentNode),
            111 => Ok(Self::GraphTransitionsTo),
            112 => Ok(Self::ComposesAgent),
            113 => Ok(Self::SpawnsSubagent),
            114 => Ok(Self::BindsTool),
            115 => Ok(Self::InvokesLlm),
            116 => Ok(Self::ProducesAiContract),
            117 => Ok(Self::UsesPrompt),
            118 => Ok(Self::FetchesPromptFrom),
            119 => Ok(Self::RetrievesFrom),
            120 => Ok(Self::Embeds),
            121 => Ok(Self::IndexesVector),
            122 => Ok(Self::CallsMcpTool),
            123 => Ok(Self::ExposesMcpTool),
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
    fn new_ai_node_kinds_round_trip_and_are_listed() {
        for kind in [
            NodeKind::AgentGraph,
            NodeKind::Prompt,
            NodeKind::AiContract,
            NodeKind::VectorIndex,
            NodeKind::McpServer,
            NodeKind::McpTool,
            NodeKind::LlmModel,
        ] {
            let decoded = NodeKind::try_from(kind.as_u8())
                .unwrap_or_else(|_| panic!("{kind:?} should decode"));
            assert_eq!(decoded, kind);
            assert!(
                NodeKind::all().contains(&kind),
                "{kind:?} missing from all()"
            );
        }
    }

    #[test]
    fn edge_kind_all_is_in_sync_with_try_from() {
        // Guards the class of bug where a new variant is added to the enum +
        // TryFrom but forgotten in all() (which drives count_edges_by_kind and
        // fixture validation). all() must list exactly the decodable discriminants.
        let mut listed: Vec<u8> = EdgeKind::all().iter().map(|k| k.as_u8()).collect();
        listed.sort_unstable();
        let mut decodable: Vec<u8> = (0..=u8::MAX)
            .filter(|byte| EdgeKind::try_from(*byte).is_ok())
            .collect();
        decodable.sort_unstable();
        assert_eq!(
            listed, decodable,
            "EdgeKind::all() is out of sync with TryFrom"
        );
    }

    #[test]
    fn node_kind_all_is_in_sync_with_try_from() {
        let mut listed: Vec<u8> = NodeKind::all().iter().map(|k| k.as_u8()).collect();
        listed.sort_unstable();
        let mut decodable: Vec<u8> = (0..=u8::MAX)
            .filter(|byte| NodeKind::try_from(*byte).is_ok())
            .collect();
        decodable.sort_unstable();
        assert_eq!(
            listed, decodable,
            "NodeKind::all() is out of sync with TryFrom"
        );
    }

    #[test]
    fn ai_cross_repo_edges_are_semantic_bridges() {
        // The AI cross-repo convergence edges are structurally the same class as
        // ConsumesApiFrom/UsesEventFrom and must be classified as bridges.
        for kind in [
            EdgeKind::FetchesPromptFrom,
            EdgeKind::Embeds,
            EdgeKind::CallsMcpTool,
            EdgeKind::RetrievesFrom,
        ] {
            assert!(
                kind.is_semantic_bridge(),
                "{kind:?} should be a semantic bridge"
            );
        }
        // Intra-service AI edges are NOT cross-repo bridges.
        assert!(!EdgeKind::GraphTransitionsTo.is_semantic_bridge());
        assert!(!EdgeKind::InvokesLlm.is_semantic_bridge());
    }

    #[test]
    fn new_ai_edge_kinds_round_trip_through_u8() {
        for kind in [
            EdgeKind::DefinesAgentNode,
            EdgeKind::GraphTransitionsTo,
            EdgeKind::ComposesAgent,
            EdgeKind::SpawnsSubagent,
            EdgeKind::BindsTool,
            EdgeKind::InvokesLlm,
            EdgeKind::ProducesAiContract,
            EdgeKind::UsesPrompt,
            EdgeKind::FetchesPromptFrom,
            EdgeKind::RetrievesFrom,
            EdgeKind::Embeds,
            EdgeKind::IndexesVector,
            EdgeKind::CallsMcpTool,
            EdgeKind::ExposesMcpTool,
        ] {
            let decoded = EdgeKind::try_from(kind.as_u8())
                .unwrap_or_else(|_| panic!("{kind:?} should decode"));
            assert_eq!(decoded, kind);
        }
    }

    #[test]
    fn search_indexable_policy_excludes_structural_and_temporal_kinds() {
        assert!(NodeKind::Function.is_search_indexable());
        assert!(NodeKind::Event.is_search_indexable());
        assert!(NodeKind::DataField.is_search_indexable());
        assert!(NodeKind::Deployment.is_search_indexable());
        assert!(NodeKind::EnvVar.is_search_indexable());
        assert!(!NodeKind::Secret.is_search_indexable());
        assert!(!NodeKind::Import.is_search_indexable());
        assert!(!NodeKind::Decorator.is_search_indexable());
        assert!(!NodeKind::PR.is_search_indexable());
        assert!(!NodeKind::Ticket.is_search_indexable());
    }

    #[test]
    fn node_kind_invalid_u8_rejects() {
        for value in [41_u8, 50, 100, 255] {
            assert!(NodeKind::try_from(value).is_err(), "{value} should reject");
        }
    }

    #[test]
    fn edge_kind_invalid_u8_rejects() {
        for value in [9_u8, 19, 33, 39, 46, 59, 63, 79, 96, 106, 255] {
            assert!(EdgeKind::try_from(value).is_err(), "{value} should reject");
        }
    }

    #[test]
    fn kind_display_uses_debug_name() {
        assert_eq!(NodeKind::Function.to_string(), "Function");
        assert_eq!(EdgeKind::CrossRepoDepends.to_string(), "CrossRepoDepends");
    }
}
