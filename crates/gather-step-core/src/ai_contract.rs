//! AI (structured-output) contracts — the LLM-call analogue of payload
//! contracts. Mirrors `payload.rs` shapes with AI-specific facets
//! (`provider`, `model`, `temperature`, `structured`, `prompt_keys`).
//! Deliberately parallel for now; the shared `ContractDoc<Facets>` unification
//! (plan §3.4) is a follow-up so the existing payload-contract storage and
//! pr-review surface is not destabilised in the same change.

use crate::{NodeId, NodeKind, ref_node_id};

/// Provenance of an AI contract's field shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiContractInferenceKind {
    /// Schema literal extracted in place (`z.object({...})` / Pydantic class body).
    LiteralSchema,
    /// Schema resolved from a referenced/derived definition (`.extend`/`.merge`/
    /// `create_model`) whose fields are not locally enumerable — track change,
    /// not exact shape (plan §3.4 field-extraction soft spot).
    ReferencedSchema,
    /// Shape inferred from usage, not a declared schema.
    UsageInferred,
}

impl AiContractInferenceKind {
    #[must_use]
    pub fn as_sql_str(self) -> &'static str {
        match self {
            Self::LiteralSchema => "literal_schema",
            Self::ReferencedSchema => "referenced_schema",
            Self::UsageInferred => "usage_inferred",
        }
    }

    #[must_use]
    pub fn from_sql_str(s: &str) -> Option<Self> {
        match s {
            "literal_schema" => Some(Self::LiteralSchema),
            "referenced_schema" => Some(Self::ReferencedSchema),
            "usage_inferred" => Some(Self::UsageInferred),
            _ => None,
        }
    }
}

/// AI-edge confidence bands on the 0-1000 edge scale (plan §3.6) — distinct
/// from `PayloadConfidenceBand` (900/700) because AI wiring fails categorically.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiConfidenceBand {
    Strong,
    Medium,
    Weak,
}

#[must_use]
pub fn ai_confidence_band(confidence: u16) -> AiConfidenceBand {
    match confidence {
        800..=u16::MAX => AiConfidenceBand::Strong,
        500..=799 => AiConfidenceBand::Medium,
        _ => AiConfidenceBand::Weak,
    }
}

/// One field of a structured-output schema.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AiContractField {
    pub name: String,
    pub type_name: String,
    pub optional: bool,
    pub confidence: u16,
}

/// The structured-output contract document stored alongside an `AiContract` node.
///
/// `temperature` is stored as a string (e.g. `"0"`, `"0.7"`) to preserve the
/// source literal and keep the struct `Eq`-able (f32 is not `Eq`).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AiContractDoc {
    pub provider: Option<String>,
    pub model: Option<String>,
    pub temperature: Option<String>,
    pub structured: bool,
    pub schema_format: String,
    pub inference_kind: AiContractInferenceKind,
    pub confidence: u16,
    pub fields: Vec<AiContractField>,
    pub prompt_keys: Vec<String>,
    pub source_type_name: Option<String>,
}

/// A persisted AI contract: where it lives, what call site it belongs to, and
/// the contract document itself.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AiContractRecord {
    pub ai_contract_node_id: NodeId,
    pub contract_target_node_id: NodeId,
    pub contract_target_kind: NodeKind,
    pub contract_target_qualified_name: Option<String>,
    pub repo: String,
    pub file_path: String,
    pub source_symbol_node_id: NodeId,
    pub line_start: Option<u32>,
    pub inference_kind: AiContractInferenceKind,
    pub confidence: u16,
    pub source_type_name: Option<String>,
    pub contract: AiContractDoc,
}

/// Stable external id for an AI contract, keyed by `(repo, file, target, source)`.
#[must_use]
pub fn ai_contract_external_id(
    repo: &str,
    file_path: &str,
    target_id: NodeId,
    source_symbol_id: NodeId,
) -> String {
    // Canonicalize the free-text segments so a repo/path containing the `__`
    // delimiter collapses rather than mangling the id (parity with the
    // `*_qn` helpers in `virtual_nodes`). Hex segments are delimiter-safe.
    let repo = crate::virtual_nodes::canonical_topology_part_or(repo, "unknown_repo");
    let file_path = crate::virtual_nodes::canonical_topology_part_or(file_path, "unknown_file");
    format!(
        "__ai_contract__{repo}__{file_path}__{}__{}",
        hex_encode(&target_id.as_bytes()),
        hex_encode(&source_symbol_id.as_bytes()),
    )
}

#[must_use]
pub fn ai_contract_node_id(external_id: &str) -> NodeId {
    ref_node_id(NodeKind::AiContract, external_id)
}

fn hex_encode(bytes: &[u8; 16]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::{
        AiConfidenceBand, AiContractDoc, AiContractField, AiContractInferenceKind,
        AiContractRecord, ai_confidence_band, ai_contract_external_id, ai_contract_node_id,
    };
    use crate::{NodeKind, node_id};

    fn sample_record() -> AiContractRecord {
        let target = node_id(
            "events",
            "src/agent.ts",
            NodeKind::LlmModel,
            "__llm__openai__gpt-4.1-mini",
        );
        let source = node_id("events", "src/agent.ts", NodeKind::Function, "compareItems");
        let external_id = ai_contract_external_id("events", "src/agent.ts", target, source);
        AiContractRecord {
            ai_contract_node_id: ai_contract_node_id(&external_id),
            contract_target_node_id: target,
            contract_target_kind: NodeKind::LlmModel,
            contract_target_qualified_name: Some("__llm__openai__gpt-4.1-mini".to_owned()),
            repo: "events".to_owned(),
            file_path: "src/agent.ts".to_owned(),
            source_symbol_node_id: source,
            line_start: Some(42),
            inference_kind: AiContractInferenceKind::LiteralSchema,
            confidence: 850,
            source_type_name: Some("ItemComparisonOutputSchema".to_owned()),
            contract: AiContractDoc {
                provider: Some("openai".to_owned()),
                model: Some("gpt-4.1-mini".to_owned()),
                temperature: Some("0".to_owned()),
                structured: true,
                schema_format: "zod".to_owned(),
                inference_kind: AiContractInferenceKind::LiteralSchema,
                confidence: 850,
                fields: vec![
                    AiContractField {
                        name: "is_related".to_owned(),
                        type_name: "boolean".to_owned(),
                        optional: false,
                        confidence: 900,
                    },
                    AiContractField {
                        name: "reason".to_owned(),
                        type_name: "string".to_owned(),
                        optional: false,
                        confidence: 900,
                    },
                ],
                prompt_keys: vec!["doc-summary".to_owned()],
                source_type_name: Some("ItemComparisonOutputSchema".to_owned()),
            },
        }
    }

    #[test]
    fn ai_confidence_band_uses_v5_thresholds() {
        // Plan S3.6: Strong >= 800, Medium 500-799, Weak < 500 on the 0-1000 edge scale.
        assert_eq!(ai_confidence_band(1000), AiConfidenceBand::Strong);
        assert_eq!(ai_confidence_band(800), AiConfidenceBand::Strong);
        assert_eq!(ai_confidence_band(799), AiConfidenceBand::Medium);
        assert_eq!(ai_confidence_band(500), AiConfidenceBand::Medium);
        assert_eq!(ai_confidence_band(499), AiConfidenceBand::Weak);
        assert_eq!(ai_confidence_band(0), AiConfidenceBand::Weak);
    }

    #[test]
    fn external_id_is_deterministic_and_namespaced() {
        let target = node_id("r", "f.ts", NodeKind::LlmModel, "__llm__openai__gpt");
        let source = node_id("r", "f.ts", NodeKind::Function, "fn");
        let a = ai_contract_external_id("r", "f.ts", target, source);
        let b = ai_contract_external_id("r", "f.ts", target, source);
        assert_eq!(a, b);
        assert!(a.starts_with("__ai_contract__"), "got {a}");
    }

    #[test]
    fn node_id_is_stable_and_typed() {
        let ext = "__ai_contract__r__f__aa__bb";
        assert_eq!(ai_contract_node_id(ext), ai_contract_node_id(ext));
    }

    #[test]
    fn inference_kind_sql_round_trips() {
        for kind in [
            AiContractInferenceKind::LiteralSchema,
            AiContractInferenceKind::ReferencedSchema,
            AiContractInferenceKind::UsageInferred,
        ] {
            assert_eq!(
                AiContractInferenceKind::from_sql_str(kind.as_sql_str()),
                Some(kind)
            );
        }
        assert_eq!(AiContractInferenceKind::from_sql_str("nope"), None);
    }

    #[test]
    fn record_round_trips_through_serde() {
        let record = sample_record();
        let encoded = serde_norway::to_string(&record).expect("serialize");
        let decoded: AiContractRecord = serde_norway::from_str(&encoded).expect("deserialize");
        assert_eq!(decoded, record);
    }
}
