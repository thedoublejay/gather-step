use crate::{NodeId, NodeKind, ref_node_id};

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadSide {
    Producer,
    Consumer,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadInferenceKind {
    LiteralObject,
    TypedParameter,
    UsageInferred,
}

impl PayloadInferenceKind {
    pub fn as_sql_str(self) -> &'static str {
        match self {
            Self::LiteralObject => "literal_object",
            Self::TypedParameter => "typed_parameter",
            Self::UsageInferred => "usage_inferred",
        }
    }

    pub fn from_sql_str(s: &str) -> Option<Self> {
        match s {
            "literal_object" => Some(Self::LiteralObject),
            "typed_parameter" => Some(Self::TypedParameter),
            "usage_inferred" => Some(Self::UsageInferred),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadConfidenceBand {
    Strong,
    Medium,
    Weak,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DriftKind {
    Shape,
    Type,
    Optionality,
    MissingField,
    ExtraField,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PayloadField {
    pub name: String,
    pub type_name: String,
    pub optional: bool,
    pub confidence: u16,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PayloadContractDoc {
    pub content_type: String,
    pub schema_format: String,
    pub side: PayloadSide,
    pub inference_kind: PayloadInferenceKind,
    pub confidence: u16,
    pub fields: Vec<PayloadField>,
    pub source_type_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PayloadContractRecord {
    pub payload_contract_node_id: NodeId,
    pub contract_target_node_id: NodeId,
    pub contract_target_kind: NodeKind,
    pub contract_target_qualified_name: Option<String>,
    pub repo: String,
    pub file_path: String,
    pub source_symbol_node_id: NodeId,
    pub line_start: Option<u32>,
    pub side: PayloadSide,
    pub inference_kind: PayloadInferenceKind,
    pub confidence: u16,
    pub source_type_name: Option<String>,
    pub contract: PayloadContractDoc,
}

#[must_use]
pub fn payload_contract_external_id(
    repo: &str,
    file_path: &str,
    target_id: NodeId,
    source_symbol_id: NodeId,
    side: PayloadSide,
) -> String {
    format!(
        "__payload_contract__{repo}__{file_path}__{}__{}__{}",
        hex_encode(&target_id.as_bytes()),
        hex_encode(&source_symbol_id.as_bytes()),
        match side {
            PayloadSide::Producer => "producer",
            PayloadSide::Consumer => "consumer",
        }
    )
}

#[must_use]
pub fn payload_contract_node_id(external_id: &str) -> NodeId {
    ref_node_id(NodeKind::PayloadContract, external_id)
}

#[must_use]
pub fn confidence_band(confidence: u16) -> PayloadConfidenceBand {
    match confidence {
        900..=u16::MAX => PayloadConfidenceBand::Strong,
        700..=899 => PayloadConfidenceBand::Medium,
        _ => PayloadConfidenceBand::Weak,
    }
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
