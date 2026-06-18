#![forbid(unsafe_code)]

pub mod ai_contract;
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

pub use ai_contract::{
    AiConfidenceBand, AiContractDoc, AiContractField, AiContractInferenceKind, AiContractRecord,
    ai_confidence_band, ai_contract_external_id, ai_contract_node_id,
};
pub use config::{
    ConfigError, DeploymentConfig, DepthLevel, GatherStepConfig, GithubConfig, IndexingConfig,
    JiraConfig, LanguageExcludeConfig, RepoConfig,
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
    VIRTUAL_NODE_REPO, VirtualNodeKind, broker_qn, canonical_route_path, canonical_topology_part,
    canonical_topology_part_or, config_map_qn, database_qn, deployment_qn, env_var_qn,
    llm_model_qn, mcp_tool_qn, parse_shared_symbol_qn, prompt_qn, queue_qn, route_qn, secret_qn,
    shared_package_root, shared_symbol_qn, shared_symbol_qn_unversioned, topic_qn, vector_index_qn,
    virtual_node, virtual_node_id,
};
pub use workspace::{
    WorkspaceIndexDelegate, WorkspaceIndexError, WorkspaceRepoResult, WorkspaceStats,
    index_workspace,
};
