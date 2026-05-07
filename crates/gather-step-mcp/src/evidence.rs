use rmcp::schemars;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, de};

#[derive(Debug, Clone, Serialize, JsonSchema, PartialEq, Eq)]
pub struct Evidence {
    id: String,
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
    pub fn id(&self) -> &str {
        &self.id
    }

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
            self.source,
            &self.citation,
            self.subject.as_ref(),
        );
    }
}

impl<'de> Deserialize<'de> for Evidence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawEvidence {
            #[serde(default)]
            id: String,
            kind: EvidenceKind,
            source: EvidenceSource,
            citation: EvidenceCitation,
            #[serde(default)]
            subject: Option<EvidenceSubject>,
            #[serde(default)]
            support: Option<EvidenceSupport>,
        }

        let raw = RawEvidence::deserialize(deserializer)?;
        let expected_id = evidence_id(&raw.kind, raw.source, &raw.citation, raw.subject.as_ref());
        if !raw.id.is_empty() && raw.id != expected_id {
            return Err(de::Error::custom(format!(
                "evidence id `{}` does not match canonical id `{expected_id}`",
                raw.id
            )));
        }
        Ok(Self {
            id: expected_id,
            kind: raw.kind,
            source: raw.source,
            citation: raw.citation,
            subject: raw.subject,
            support: raw.support,
        })
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
    EventBlastRadiusNode,
    OrphanTopic,
    PayloadContract,
    PayloadField,
    PayloadFieldAdded,
    PayloadFieldRemoved,
    PayloadFieldChanged,
    ProjectionImpact,
    ExistingTestSignal,
    FeatureFlag,
    ChangedSymbol,
    RemovedSurface,
    RiskNote,
    Decorator,
    ContractAlignment,
    DeploymentTouchpoint,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceSource {
    PlanningPack,
    DebugPack,
    FixPack,
    ReviewPack,
    ChangeImpactPack,
    TraceRoute,
    TraceEvent,
    CrudTrace,
    EventBlastRadius,
    CrossRepoDeps,
    TraceImpact,
    PayloadSchema,
    ProjectionImpact,
    OrphanTopicScan,
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

    #[must_use]
    pub fn symbol_id(symbol_id: impl Into<String>, symbol_kind: impl Into<String>) -> Self {
        Self {
            kind: CitationKind::Symbol,
            repo: None,
            path: None,
            line: None,
            symbol_id: Some(symbol_id.into()),
            symbol_kind: Some(symbol_kind.into()),
            symbol_name: None,
            route_method: None,
            route_path: None,
            event_target: None,
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

/// Confidence value scaled to permille (0..=1000) — `1000` means "fully
/// confident". The integer encoding keeps the schema language-agnostic and
/// avoids float drift in JSON; consumers that want a 0..=1 ratio divide by
/// 1000.
pub const EVIDENCE_SCORE_MAX: u16 = 1000;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct EvidenceSupport {
    pub method: EvidenceSupportMethod,
    /// Optional confidence score in permille (0..=`EVIDENCE_SCORE_MAX`).
    /// Missing means the support method does not produce a quantitative score.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<u16>,
}

impl EvidenceSupport {
    /// Construct a support record. Scores above [`EVIDENCE_SCORE_MAX`] are
    /// clamped so the on-wire contract (0..=1000 permille) stays honest even
    /// if a producer feeds in a bare integer like `confidence as u16`.
    #[must_use]
    pub fn new(method: EvidenceSupportMethod, score: Option<u16>) -> Self {
        Self {
            method,
            score: score.map(|s| s.min(EVIDENCE_SCORE_MAX)),
        }
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
pub fn evidence_source_for_pack_mode(mode: &str) -> Option<EvidenceSource> {
    match mode {
        "planning" => Some(EvidenceSource::PlanningPack),
        "debug" => Some(EvidenceSource::DebugPack),
        "fix" => Some(EvidenceSource::FixPack),
        "review" => Some(EvidenceSource::ReviewPack),
        "change_impact" => Some(EvidenceSource::ChangeImpactPack),
        _ => None,
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
    if contains_ascii_case_insensitive(category, "payload")
        || contains_ascii_case_insensitive(symbol_kind, "payload")
        || contains_ascii_case_insensitive(symbol_name, "dto")
    {
        "payload_contract".to_owned()
    } else if contains_ascii_case_insensitive(category, "route")
        || contains_ascii_case_insensitive(symbol_kind, "route")
        || contains_ascii_case_insensitive(file_path, "route")
    {
        "route".to_owned()
    } else if contains_ascii_case_insensitive(symbol_kind, "event")
        || contains_ascii_case_insensitive(file_path, "event")
    {
        "event".to_owned()
    } else if is_test_path(file_path) {
        "test".to_owned()
    } else if contains_ascii_case_insensitive(symbol_kind, "class")
        || contains_ascii_case_insensitive(symbol_kind, "component")
    {
        "ui_or_service".to_owned()
    } else {
        "symbol".to_owned()
    }
}

fn is_test_path(path: &str) -> bool {
    contains_ascii_case_insensitive(path, ".test.")
        || contains_ascii_case_insensitive(path, ".spec.")
        || contains_ascii_case_insensitive(path, "/test/")
        || contains_ascii_case_insensitive(path, "/tests/")
        || ends_with_ascii_case_insensitive(path, "_test.rs")
}

fn contains_ascii_case_insensitive(haystack: &str, needle: &str) -> bool {
    let haystack = haystack.as_bytes();
    let needle = needle.as_bytes();
    !needle.is_empty()
        && haystack.windows(needle.len()).any(|window| {
            window
                .iter()
                .zip(needle.iter())
                .all(|(left, right)| left.eq_ignore_ascii_case(right))
        })
}

fn ends_with_ascii_case_insensitive(haystack: &str, needle: &str) -> bool {
    let haystack = haystack.as_bytes();
    let needle = needle.as_bytes();
    haystack.len() >= needle.len()
        && haystack[haystack.len() - needle.len()..]
            .iter()
            .zip(needle.iter())
            .all(|(left, right)| left.eq_ignore_ascii_case(right))
}

fn evidence_id(
    kind: &EvidenceKind,
    source: EvidenceSource,
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
        source: &source,
        citation,
        subject,
    };
    let bytes = serde_json::to_vec(&identity).expect("Evidence identity is always serializable.");
    format!("GS-EVID-{:016x}", fnv1a64(&bytes))
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
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

    #[test]
    fn evidence_id_is_stable_under_builder_order_permutations() {
        // Identity is `(kind, source, citation, subject)` — `with_support` must
        // never enter the hash and the order of `with_subject`/`with_support`
        // must not matter. A regression that folds support into the ID hash
        // would silently break every consumer that pins IDs.
        let citation = EvidenceCitation::symbol(
            "backend",
            "src/orders.ts",
            Some(10),
            "symbol-1",
            "function",
            "listOrders",
        );
        let subject = EvidenceSubject::new("symbol")
            .with_category("function")
            .with_name("listOrders");
        let support = EvidenceSupport::new(EvidenceSupportMethod::GraphTraversal, Some(750));

        let subject_then_support = Evidence::new(
            EvidenceKind::ChangedSymbol,
            EvidenceSource::PrReview,
            citation.clone(),
        )
        .with_subject(subject.clone())
        .with_support(support.clone());
        let support_then_subject = Evidence::new(
            EvidenceKind::ChangedSymbol,
            EvidenceSource::PrReview,
            citation.clone(),
        )
        .with_support(support.clone())
        .with_subject(subject.clone());
        let no_support = Evidence::new(
            EvidenceKind::ChangedSymbol,
            EvidenceSource::PrReview,
            citation,
        )
        .with_subject(subject);

        assert_eq!(subject_then_support.id, support_then_subject.id);
        assert_eq!(no_support.id, subject_then_support.id);
    }

    #[test]
    fn evidence_id_round_trips_through_serde_json() {
        // The ID hash inputs are `serde_json::to_vec` over a struct with named
        // fields whose order is load-bearing. Round-trip via `to_value` /
        // `from_value` must yield byte-equal IDs so caches and dedupes that
        // marshal Evidence through JSON stay valid.
        let original = Evidence::new(
            EvidenceKind::EventConsumer,
            EvidenceSource::TraceEvent,
            EvidenceCitation::event("kafka:order.created"),
        )
        .with_subject(
            EvidenceSubject::new("event_consumer")
                .with_category("consumer")
                .with_name("OrdersConsumer.handleOrderCreated"),
        )
        .with_support(EvidenceSupport::new(
            EvidenceSupportMethod::GraphTraversal,
            Some(600),
        ));

        let value = serde_json::to_value(&original).expect("serializes");
        let back: Evidence = serde_json::from_value(value).expect("round-trips");
        assert_eq!(original.id, back.id);
    }

    #[test]
    fn evidence_score_is_clamped_to_permille_max() {
        // The wire contract says scores are 0..=1000 permille. A producer
        // accidentally feeding in a u16 above 1000 must be clamped at the
        // boundary so consumers that divide by 1000 to get a 0..=1 ratio
        // never see a > 1.0 value.
        let support = EvidenceSupport::new(EvidenceSupportMethod::RetrievalRank, Some(50_000));
        assert_eq!(support.score, Some(EVIDENCE_SCORE_MAX));
        let pinned = EvidenceSupport::new(EvidenceSupportMethod::RetrievalRank, Some(750));
        assert_eq!(pinned.score, Some(750));
        let none = EvidenceSupport::new(EvidenceSupportMethod::StaticAnalyzer, None);
        assert!(none.score.is_none());
    }

    #[test]
    fn deserialization_rejects_id_drift() {
        let raw = serde_json::json!({
            "id": "GS-EVID-deadbeefdeadbeef",
            "kind": "feature_flag",
            "source": "workspace_scan",
            "citation": {
                "kind": "file_line",
                "repo": "frontend",
                "path": "src/flags.ts",
                "line": 12
            }
        });
        let err = serde_json::from_value::<Evidence>(raw).expect_err("drifted id must fail");
        assert!(err.to_string().contains("does not match canonical id"));
    }
}
