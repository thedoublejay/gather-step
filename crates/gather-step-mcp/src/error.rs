use thiserror::Error;

#[derive(Debug, Error)]
pub enum McpServerError {
    #[error(transparent)]
    Graph(#[from] gather_step_storage::GraphStoreError),
    #[error(transparent)]
    Search(#[from] gather_step_storage::SearchStoreError),
    #[error(transparent)]
    CrossRepo(#[from] gather_step_analysis::CrossRepoError),
    #[error(transparent)]
    EventTopology(#[from] gather_step_analysis::EventTopologyError),
    #[error(transparent)]
    ContractDrift(#[from] gather_step_analysis::ContractDriftAnalysisError),
    #[error(transparent)]
    CrudTrace(#[from] gather_step_analysis::CrudTraceError),
    #[error(transparent)]
    DeadCode(#[from] gather_step_analysis::DeadCodeError),
    #[error(transparent)]
    Convention(#[from] gather_step_analysis::ConventionError),
    #[error(transparent)]
    Overview(#[from] gather_step_analysis::OverviewError),
    #[error(transparent)]
    Query(#[from] gather_step_analysis::QueryError),
    #[error(transparent)]
    Registry(#[from] gather_step_core::RegistryError),
    #[error(transparent)]
    Metadata(#[from] gather_step_storage::MetadataStoreError),
    #[error(transparent)]
    Stores(#[from] gather_step_storage::WorkspaceStoresError),
    #[error("failed to initialize MCP server: {0}")]
    Initialize(String),
    #[error("failed while waiting for MCP server task: {0}")]
    Join(String),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("not found: {0}")]
    NotFound(String),
}
