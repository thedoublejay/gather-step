#![forbid(unsafe_code)]

#[cfg(test)]
mod test_utils;

pub mod anchor;
pub mod canonical;
pub mod contract_drift;
pub mod conventions;
pub mod cross_repo;
pub mod crud_trace;
pub mod dead_code;
pub mod event_topology;
pub mod evidence;
pub mod impact;
pub mod overview;
pub mod pack_assembly;
pub mod projection_impact;
pub mod proofs;
pub mod query;
pub mod semantic_health;
pub mod shared_contract;
pub mod transport;

pub use canonical::{Canonical, TopicKind, canonical_for_node};
pub use contract_drift::{
    BreakingChangeCandidate, ContractDrift, ContractDriftAnalysisError, DriftField, PayloadSchema,
    breaking_change_candidates, compare_contracts, payload_schema,
};
pub use conventions::{ConventionError, ConventionFinding, ConventionReport, detect_conventions};
pub use cross_repo::{
    CrossRepoDependencies, CrossRepoError, CrossRepoHop, TraceDirection, cross_repo_deps,
    trace_across_repos,
};
pub use crud_trace::{
    CrudTrace, CrudTraceEntry, CrudTraceError, CrudTraceRole, trace_crud_route, trace_crud_symbol,
};
pub use dead_code::{
    ConfidenceBand, DeadCodeError, DeadCodeFinding, DeadCodeReport, DetectorBasis, find_dead_code,
    find_dead_code_with_manifest,
};
pub use event_topology::{
    BlastRadiusEdge, BlastRadiusNode, EventBlastRadius, EventRole, EventTopologyError, EventTrace,
    OrphanKind, OrphanTopic, OrphanTopicsPage, RouteRole, RouteTrace, TopologyMatch,
    canonical_event_target, canonical_event_target_for_node, event_blast_radius,
    list_orphan_topics, list_orphan_topics_paged, rank_event_targets, resolve_event_targets,
    resolve_route_target, trace_event, trace_route,
};
pub use impact::{
    BoundaryRole, EvidenceBand, ImpactError, ImpactMap, ImpactedFile, shared_contract_impact,
};
pub use overview::{ModuleSummary, OverviewError, RepoOverview, build_overview};
pub use pack_assembly::{
    CandidateKey, Pack, PackAssembler, PackItem, PackMode, QueryShape, SimplePackAssembler,
    classify_query_shape,
};
pub use projection_impact::{
    ProjectionDerivation, ProjectionEvidence, ProjectionEvidenceVerbosity, ProjectionField,
    ProjectionImpactError, ProjectionImpactReport, ProjectionImpactRequest, projection_impact,
    projection_impact_with_payload_contracts,
};
pub use proofs::{
    MAX_PROOFS_PER_REPO, ProofCaller, ProofEngineError, ProofEngineOptions, ProofEngineOutput,
    build_pack_proofs, derive_repo_sets, finalize_proofs, proof_strength,
};
pub use query::{GraphQuery, QueryError, TraversalStep};
pub use semantic_health::{
    SemanticHealthError, SemanticHealthReport, SemanticLinkHealth, semantic_health_for_repo,
    semantic_health_for_workspace,
};
pub use shared_contract::{
    guard_class_name_for_anchor, looks_like_guard_entrypoint, peer_matches_guard_class_name,
    shared_contract_candidate_ids,
};
