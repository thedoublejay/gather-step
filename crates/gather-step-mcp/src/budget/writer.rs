use serde::Serialize;

use crate::error::McpServerError;

use super::schema::{OmittedReason, ResponseBudget};

const BUDGET_WRITER_LIMIT_EXCEEDED: &str = "budget writer limit exceeded";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetedTool {
    Search,
    Traversal,
    TraceImpact,
    TraceEvent,
    TraceRoute,
    EventBlastRadius,
    CrudTrace,
    Brief,
    SchemaSummary,
    LegacyRaw,
    Context,
    ContextPack,
    ChangeImpact,
    /// Payload-contract tools (`payload_schema`, `contract_drift`,
    /// `breaking_change_candidates`). Compact responses, modeled on `Brief`.
    Contract,
    /// Repo-intelligence tools (`who_owns`, `get_dead_code`, `get_conventions`,
    /// `get_overview`). List-heavy AI-facing tools, modeled on `ContextPack`.
    RepoIntelligence,
}

impl BudgetedTool {
    #[must_use]
    pub const fn default_bytes(self) -> usize {
        match self {
            Self::Search | Self::TraceRoute => 12_000,
            Self::Traversal => 10_000,
            Self::TraceImpact | Self::TraceEvent => 14_000,
            Self::EventBlastRadius | Self::CrudTrace | Self::ChangeImpact => 16_000,
            Self::Brief | Self::Contract => 4_000,
            Self::SchemaSummary => 2_000,
            Self::LegacyRaw => 32_000,
            Self::Context => 18_000,
            Self::ContextPack | Self::RepoIntelligence => 8_000,
        }
    }

    #[must_use]
    pub const fn max_bytes(self) -> usize {
        match self {
            Self::Search | Self::TraceRoute => 48_000,
            Self::Traversal => 40_000,
            Self::TraceImpact | Self::TraceEvent => 56_000,
            Self::EventBlastRadius | Self::CrudTrace | Self::ChangeImpact => 64_000,
            Self::Brief | Self::Contract => 16_000,
            Self::SchemaSummary => 8_000,
            Self::LegacyRaw => 128_000,
            Self::Context => 72_000,
            Self::ContextPack | Self::RepoIntelligence => 32_000,
        }
    }
}

pub fn requested_budget_bytes(
    tool: BudgetedTool,
    requested: Option<usize>,
) -> Result<usize, McpServerError> {
    let budget = requested.unwrap_or(tool.default_bytes());
    if budget == 0 {
        return Err(McpServerError::InvalidInput(
            "budget_bytes must be greater than zero".to_owned(),
        ));
    }
    Ok(budget.min(tool.max_bytes()))
}

pub fn apply_response_budget<T>(
    tool: BudgetedTool,
    requested_budget: Option<usize>,
    response: &mut T,
    mut remove_lowest_ranked: impl FnMut(&mut T) -> bool,
) -> Result<ResponseBudget, McpServerError>
where
    T: Serialize,
{
    // Drop in exponentially-growing batches so a deeply over-budget pack with
    // L items needs O(log L) serialize-to-budget checks instead of O(L). The
    // serializer short-circuits at `budget_bytes`, so each check is bounded
    // by the budget — the win is fewer checks, not cheaper checks. The
    // doubling cap keeps us from over-dropping wildly when the first
    // boundary check trips.
    const MAX_BATCH: usize = 64;

    let budget_bytes = requested_budget_bytes(tool, requested_budget)?;
    let mut omitted_items = 0;
    let mut batch = 1;
    loop {
        if !serialized_len_exceeds(response, budget_bytes)? {
            break;
        }
        let mut dropped_this_round = 0;
        while dropped_this_round < batch {
            if !remove_lowest_ranked(response) {
                break;
            }
            dropped_this_round += 1;
            omitted_items += 1;
        }
        if dropped_this_round == 0 {
            // Trimmer is exhausted — nothing more we can do; accept the
            // current response.
            break;
        }
        batch = (batch * 2).min(MAX_BATCH);
    }

    let used_bytes = serialized_len(response)?;
    let truncated = omitted_items > 0 || used_bytes > budget_bytes;
    Ok(ResponseBudget {
        budget_bytes,
        items_dropped: omitted_items,
        items_included: 0,
        omitted_items,
        omission_reason: truncated.then_some(OmittedReason::Budget),
        tool_default_bytes: tool.default_bytes(),
        tool_max_bytes: tool.max_bytes(),
        truncated,
        used_bytes,
    })
}

fn serialized_len<T: Serialize>(value: &T) -> Result<usize, McpServerError> {
    let mut writer = BudgetWriter::new(None);
    serde_json::to_writer(&mut writer, value)
        .map_err(|error| McpServerError::Internal(error.to_string()))?;
    Ok(writer.bytes_written())
}

fn serialized_len_exceeds<T: Serialize>(
    value: &T,
    budget_bytes: usize,
) -> Result<bool, McpServerError> {
    let mut writer = BudgetWriter::new(Some(budget_bytes));
    match serde_json::to_writer(&mut writer, value) {
        Ok(()) => Ok(false),
        Err(error)
            if writer.limit_exceeded() || error.to_string() == BUDGET_WRITER_LIMIT_EXCEEDED =>
        {
            Ok(true)
        }
        Err(error) => Err(McpServerError::Internal(error.to_string())),
    }
}

pub struct BudgetWriter {
    bytes_written: usize,
    limit: Option<usize>,
    limit_exceeded: bool,
}

impl BudgetWriter {
    #[must_use]
    pub const fn new(limit: Option<usize>) -> Self {
        Self {
            bytes_written: 0,
            limit,
            limit_exceeded: false,
        }
    }

    #[must_use]
    pub const fn bytes_written(&self) -> usize {
        self.bytes_written
    }

    #[must_use]
    pub const fn limit_exceeded(&self) -> bool {
        self.limit_exceeded
    }
}

impl std::io::Write for BudgetWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.bytes_written = self.bytes_written.saturating_add(buf.len());
        if self.limit.is_some_and(|limit| self.bytes_written > limit) {
            self.limit_exceeded = true;
            return Err(std::io::Error::other(BUDGET_WRITER_LIMIT_EXCEEDED));
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::{BudgetedTool, apply_response_budget};
    use crate::budget::schema::OmittedReason;

    #[derive(Debug, Clone, Serialize)]
    struct DummyResponse {
        values: Vec<String>,
    }

    #[test]
    fn budget_helper_truncates_lowest_ranked_items_until_fit() {
        let mut response = DummyResponse {
            values: vec![
                "keep-high".to_owned(),
                "keep-mid".to_owned(),
                "drop-low".to_owned(),
            ],
        };

        let budget =
            apply_response_budget(BudgetedTool::Search, Some(40), &mut response, |payload| {
                payload.values.pop().is_some()
            })
            .expect("budget should apply");

        assert!(budget.truncated);
        assert_eq!(budget.omitted_items, 1);
        assert_eq!(budget.omission_reason, Some(OmittedReason::Budget));
        assert_eq!(response.values, vec!["keep-high", "keep-mid"]);
    }

    #[test]
    fn omitted_reason_serializes_to_plan_locked_tokens() {
        let cases = [
            (OmittedReason::Budget, "\"budget\""),
            (OmittedReason::LowConfidence, "\"low_confidence\""),
            (OmittedReason::FanOutCap, "\"fan_out_cap\""),
            (OmittedReason::Ambiguity, "\"ambiguity\""),
        ];
        for (reason, expected) in cases {
            assert_eq!(serde_json::to_string(&reason).unwrap(), expected);
        }
    }
}
