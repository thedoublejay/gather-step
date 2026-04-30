#![forbid(unsafe_code)]

pub mod config;
pub mod graph;
pub mod high_contract;
pub mod path_id;
pub mod payload;
pub mod registry;
pub mod resolver;
pub mod schema;
pub mod virtual_nodes;
pub mod workspace;

pub use config::{
    ConfigError, DepthLevel, GatherStepConfig, GithubConfig, IndexingConfig, JiraConfig,
    LanguageExcludeConfig, RepoConfig,
};
pub use graph::{
    EdgeData, EdgeMetadata, MIGRATION_FILTERS_METADATA_PREFIX, NodeData, NodeId, SourceSpan,
    Visibility, node_id, ref_node_id,
};
pub use path_id::{PathId, normalize_path_separators};
pub use payload::{
    DriftKind, PayloadConfidenceBand, PayloadContractDoc, PayloadContractRecord, PayloadField,
    PayloadInferenceKind, PayloadSide, payload_contract_external_id, payload_contract_node_id,
};
pub use registry::{
    CursorState, RegisteredRepo, RegistryError, RegistrySource, RegistryStore, RepoIndexMetadata,
    WorkspaceRegistry,
};
pub use resolver::{ResolverStrategy, strategy_weight};
pub use schema::{EdgeKind, NodeKind, PlanningProof, ProofHop, ProofKind, proof_sort_key};
pub use virtual_nodes::{
    VIRTUAL_NODE_REPO, VirtualNodeKind, canonical_route_path, parse_shared_symbol_qn, queue_qn,
    route_qn, shared_package_root, shared_symbol_qn, shared_symbol_qn_unversioned, topic_qn,
    virtual_node, virtual_node_id,
};
pub use workspace::{
    WorkspaceIndexDelegate, WorkspaceIndexError, WorkspaceRepoResult, WorkspaceStats,
    index_workspace,
};
