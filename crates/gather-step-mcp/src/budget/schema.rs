use rmcp::schemars;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::writer::BudgetedTool;

pub const RESPONSE_SCHEMA_VERSION: u8 = 3;

#[must_use]
pub const fn response_schema_version() -> u8 {
    RESPONSE_SCHEMA_VERSION
}

/// Reason an AI-facing response dropped lower-ranked evidence.
///
/// These four categorical values are locked so agents can branch uniformly on
/// truncation causes.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OmittedReason {
    /// Ranked truncation triggered by the response byte budget.
    Budget,
    /// Items dropped because their confidence was below a retention threshold.
    LowConfidence,
    /// A fan-out cap (for example `change_impact_pack`'s max repos) was hit.
    FanOutCap,
    /// Ambiguity was detected and lower-ranked candidates were collapsed.
    Ambiguity,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ResponseBudget {
    pub budget_bytes: usize,
    #[serde(default)]
    pub items_dropped: usize,
    #[serde(default)]
    pub items_included: usize,
    pub omitted_items: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub omission_reason: Option<OmittedReason>,
    pub tool_default_bytes: usize,
    pub tool_max_bytes: usize,
    pub truncated: bool,
    pub used_bytes: usize,
}

impl ResponseBudget {
    #[must_use]
    pub fn not_truncated(tool: BudgetedTool, budget_bytes: usize, used_bytes: usize) -> Self {
        Self {
            budget_bytes,
            items_dropped: 0,
            items_included: 0,
            omitted_items: 0,
            omission_reason: None,
            tool_default_bytes: tool.default_bytes(),
            tool_max_bytes: tool.max_bytes(),
            truncated: false,
            used_bytes,
        }
    }
}
