use rmcp::schemars;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct Evidence {
    pub id: String,
    pub kind: EvidenceKind,
    pub source: EvidenceSource,
    pub citation: EvidenceCitation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<EvidenceSubject>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub support: Option<EvidenceSupport>,
}

impl Evidence {
    #[must_use]
    pub fn new(kind: EvidenceKind, source: EvidenceSource, citation: EvidenceCitation) -> Self {
        let mut evidence = Self {
            id: String::new(),
            kind,
            source,
            citation,
            subject: None,
            support: None,
        };
        evidence.refresh_id();
        evidence
    }

    #[must_use]
    pub fn with_subject(mut self, subject: EvidenceSubject) -> Self {
        self.subject = Some(subject);
        self.refresh_id();
        self
    }

    #[must_use]
    pub fn with_support(mut self, support: EvidenceSupport) -> Self {
        self.support = Some(support);
        self
    }

    fn refresh_id(&mut self) {
        self.id = evidence_id(
            &self.kind,
            &self.source,
            &self.citation,
            self.subject.as_ref(),
        );
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    PlanningContext,
    ReviewContext,
    ChangeImpactCandidate,
    CrossRepoCaller,
    ConfirmedDownstreamRepo,
    ProbableDownstreamRepo,
    UnresolvedPossibleRepo,
    TruncatedRepos,
    RouteDefinition,
    RouteHandler,
    RouteCaller,
    EventDefinition,
    EventProducer,
    EventConsumer,
    PayloadContract,
    ExistingTestSignal,
    FeatureFlag,
    ChangedSymbol,
    RemovedSurface,
    Decorator,
    ContractAlignment,
    DeploymentTouchpoint,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceSource {
    PlanningPack,
    ReviewPack,
    ChangeImpactPack,
    TraceRoute,
    TraceEvent,
    WorkspaceScan,
    PrReview,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct EvidenceCitation {
    pub kind: CitationKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_method: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_target: Option<String>,
}

impl EvidenceCitation {
    #[must_use]
    pub fn file_line(repo: impl Into<String>, path: impl Into<String>, line: Option<u32>) -> Self {
        Self {
            kind: CitationKind::FileLine,
            repo: Some(repo.into()),
            path: Some(path.into()),
            line,
            symbol_id: None,
            symbol_kind: None,
            symbol_name: None,
            route_method: None,
            route_path: None,
            event_target: None,
        }
    }

    #[must_use]
    pub fn repo(repo: impl Into<String>) -> Self {
        Self {
            kind: CitationKind::Repo,
            repo: Some(repo.into()),
            path: None,
            line: None,
            symbol_id: None,
            symbol_kind: None,
            symbol_name: None,
            route_method: None,
            route_path: None,
            event_target: None,
        }
    }

    #[must_use]
    pub fn symbol(
        repo: impl Into<String>,
        path: impl Into<String>,
        line: Option<u32>,
        symbol_id: impl Into<String>,
        symbol_kind: impl Into<String>,
        symbol_name: impl Into<String>,
    ) -> Self {
        Self {
            kind: CitationKind::Symbol,
            repo: Some(repo.into()),
            path: Some(path.into()),
            line,
            symbol_id: Some(symbol_id.into()),
            symbol_kind: Some(symbol_kind.into()),
            symbol_name: Some(symbol_name.into()),
            route_method: None,
            route_path: None,
            event_target: None,
        }
    }

    #[must_use]
    pub fn route(method: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            kind: CitationKind::Route,
            repo: None,
            path: None,
            line: None,
            symbol_id: None,
            symbol_kind: None,
            symbol_name: None,
            route_method: Some(method.into()),
            route_path: Some(path.into()),
            event_target: None,
        }
    }

    #[must_use]
    pub fn event(target: impl Into<String>) -> Self {
        Self {
            kind: CitationKind::Event,
            repo: None,
            path: None,
            line: None,
            symbol_id: None,
            symbol_kind: None,
            symbol_name: None,
            route_method: None,
            route_path: None,
            event_target: Some(target.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CitationKind {
    FileLine,
    Repo,
    Symbol,
    Route,
    Event,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct EvidenceSubject {
    pub surface: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl EvidenceSubject {
    #[must_use]
    pub fn new(surface: impl Into<String>) -> Self {
        Self {
            surface: surface.into(),
            category: None,
            name: None,
            reason: None,
        }
    }

    #[must_use]
    pub fn with_category(mut self, category: impl Into<String>) -> Self {
        self.category = Some(category.into());
        self
    }

    #[must_use]
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    #[must_use]
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct EvidenceSupport {
    pub method: EvidenceSupportMethod,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<u16>,
}

impl EvidenceSupport {
    #[must_use]
    pub const fn new(method: EvidenceSupportMethod, score: Option<u16>) -> Self {
        Self { method, score }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceSupportMethod {
    StaticAnalyzer,
    RetrievalRank,
    GraphTraversal,
    HeuristicScan,
    DiffExtraction,
}

#[must_use]
pub fn evidence_source_for_pack_mode(mode: &str) -> EvidenceSource {
    match mode {
        "planning" => EvidenceSource::PlanningPack,
        "review" => EvidenceSource::ReviewPack,
        "change_impact" => EvidenceSource::ChangeImpactPack,
        _ => EvidenceSource::PlanningPack,
    }
}

#[must_use]
pub fn evidence_kind_for_pack_item(mode: &str, category: &str, file_path: &str) -> EvidenceKind {
    if mode == "change_impact" {
        return EvidenceKind::ChangeImpactCandidate;
    }
    if mode == "review" && is_test_path(file_path) {
        return EvidenceKind::ExistingTestSignal;
    }
    match category {
        "route" => EvidenceKind::RouteDefinition,
        "payload_contract" => EvidenceKind::PayloadContract,
        _ if mode == "review" => EvidenceKind::ReviewContext,
        _ => EvidenceKind::PlanningContext,
    }
}

#[must_use]
pub fn infer_surface(
    symbol_kind: &str,
    category: &str,
    file_path: &str,
    symbol_name: &str,
) -> String {
    let kind = symbol_kind.to_ascii_lowercase();
    let category = category.to_ascii_lowercase();
    let file_path = file_path.to_ascii_lowercase();
    let symbol_name = symbol_name.to_ascii_lowercase();

    if category.contains("payload") || kind.contains("payload") || symbol_name.contains("dto") {
        "payload_contract".to_owned()
    } else if category.contains("route") || kind.contains("route") || file_path.contains("route") {
        "route".to_owned()
    } else if kind.contains("event") || file_path.contains("event") {
        "event".to_owned()
    } else if is_test_path(&file_path) {
        "test".to_owned()
    } else if kind.contains("class") || kind.contains("component") {
        "ui_or_service".to_owned()
    } else {
        "symbol".to_owned()
    }
}

fn is_test_path(path: &str) -> bool {
    let path = path.to_ascii_lowercase();
    path.contains(".test.")
        || path.contains(".spec.")
        || path.contains("/test/")
        || path.contains("/tests/")
        || path.ends_with("_test.rs")
}

fn evidence_id(
    kind: &EvidenceKind,
    source: &EvidenceSource,
    citation: &EvidenceCitation,
    subject: Option<&EvidenceSubject>,
) -> String {
    #[derive(Serialize)]
    struct Identity<'a> {
        kind: &'a EvidenceKind,
        source: &'a EvidenceSource,
        citation: &'a EvidenceCitation,
        subject: Option<&'a EvidenceSubject>,
    }

    let identity = Identity {
        kind,
        source,
        citation,
        subject,
    };
    let bytes = serde_json::to_vec(&identity).unwrap_or_else(|_| b"invalid-evidence".to_vec());
    format!("GS-EVID-{:016x}", fnv1a64(&bytes))
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evidence_ids_are_stable_for_same_identity() {
        let left = Evidence::new(
            EvidenceKind::FeatureFlag,
            EvidenceSource::WorkspaceScan,
            EvidenceCitation::file_line("frontend", "src/flags.ts", Some(12)),
        )
        .with_subject(EvidenceSubject::new("feature_flag").with_name("OrderList"));
        let right = Evidence::new(
            EvidenceKind::FeatureFlag,
            EvidenceSource::WorkspaceScan,
            EvidenceCitation::file_line("frontend", "src/flags.ts", Some(12)),
        )
        .with_subject(EvidenceSubject::new("feature_flag").with_name("OrderList"));

        assert_eq!(left.id, right.id);
    }

    #[test]
    fn support_does_not_change_identity() {
        let base = Evidence::new(
            EvidenceKind::PlanningContext,
            EvidenceSource::PlanningPack,
            EvidenceCitation::symbol(
                "backend",
                "src/orders.ts",
                Some(10),
                "symbol-1",
                "function",
                "listOrders",
            ),
        );
        let supported = base.clone().with_support(EvidenceSupport::new(
            EvidenceSupportMethod::RetrievalRank,
            Some(900),
        ));

        assert_eq!(base.id, supported.id);
    }
}
