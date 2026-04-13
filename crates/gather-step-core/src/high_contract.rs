//! Shared HIGH-bar contract used by benchmark oracles and release gates.
//!
//! The scenario names and release probes here are the authoritative release gate.
//! Fixture oracles, analyze-report, and real-workspace release-gate code should
//! refer to this module instead of keeping their own string lists.

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HighContractKind {
    FrontendHookSession,
    ProducerConsumerEvent,
    SharedApiRollout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HighScenarioContract {
    pub kind: HighContractKind,
    pub scenario_name: &'static str,
    pub release_probe_name: &'static str,
}

pub const HIGH_SCENARIO_CONTRACTS: [HighScenarioContract; 3] = [
    HighScenarioContract {
        kind: HighContractKind::FrontendHookSession,
        scenario_name: "frontend_hook_rollout",
        release_probe_name: "planning_frontend_hook_session",
    },
    HighScenarioContract {
        kind: HighContractKind::ProducerConsumerEvent,
        scenario_name: "event_producer_consumer_rollout",
        release_probe_name: "canonical_event_producer_consumer",
    },
    HighScenarioContract {
        kind: HighContractKind::SharedApiRollout,
        scenario_name: "shared_api_rollout_split",
        release_probe_name: "shared_api_rollout",
    },
];

pub const VALID_HIGH_RESOLUTION_STRATEGIES: &[&str] = &[
    "exact",
    "ranked",
    "rescue",
    "symbol_id",
    "search_resolved",
    "search_ranked_resolved",
    "search_ranked_alternates",
    "event_anchor",
    "route_anchor",
    "impact",
];

pub const MIN_PR_ORACLE_MEDIAN_F1: f64 = 0.75;
pub const MIN_PR_ORACLE_MEDIAN_RECALL: f64 = 0.70;
pub const MAX_ADVISORY_ONLY_FRACTION: f64 = 0.50;
pub const MIN_PROOF_PRECISION: f64 = 0.80;
pub const MIN_PROOF_RECALL: f64 = 0.70;
pub const MIN_HIGH_REAL_WORKSPACE_INDEXED_REPOS: u64 = 5;
pub const MIN_HIGH_REAL_WORKSPACE_TOTAL_FILES: u64 = 1_000;

#[must_use]
pub fn normalize_high_contract_name(name: &str) -> String {
    let mut normalized = name.trim().to_owned();
    normalized.make_ascii_lowercase();
    normalized
}

impl HighContractKind {
    #[must_use]
    pub const fn scenario_name(self) -> &'static str {
        match self {
            Self::FrontendHookSession => "frontend_hook_rollout",
            Self::ProducerConsumerEvent => "event_producer_consumer_rollout",
            Self::SharedApiRollout => "shared_api_rollout_split",
        }
    }

    #[must_use]
    pub const fn release_probe_name(self) -> &'static str {
        match self {
            Self::FrontendHookSession => "planning_frontend_hook_session",
            Self::ProducerConsumerEvent => "canonical_event_producer_consumer",
            Self::SharedApiRollout => "shared_api_rollout",
        }
    }
}
