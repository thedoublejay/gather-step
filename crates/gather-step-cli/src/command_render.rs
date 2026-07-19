use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::app::Output;
use gather_step_storage::{TelemetryCommandResult, TelemetryResultKind};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RenderedCommand {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lines: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub telemetry_result: Option<TelemetryCommandResult>,
}

impl RenderedCommand {
    #[must_use]
    pub fn success(payload: Value, lines: Vec<String>) -> Self {
        Self {
            payload: Some(payload),
            lines,
            error: None,
            telemetry_result: None,
        }
    }

    #[must_use]
    pub fn failure(payload: Option<Value>, lines: Vec<String>, error: impl Into<String>) -> Self {
        Self {
            payload,
            lines,
            error: Some(error.into()),
            telemetry_result: None,
        }
    }

    #[must_use]
    pub fn with_telemetry_result(mut self, kind: TelemetryResultKind, count: usize) -> Self {
        self.telemetry_result = Some(TelemetryCommandResult {
            kind,
            count: i64::try_from(count).unwrap_or(i64::MAX),
        });
        self
    }

    pub fn success_serialized<T: Serialize>(payload: &T, lines: Vec<String>) -> Result<Self> {
        Ok(Self::success(serde_json::to_value(payload)?, lines))
    }

    pub fn failure_serialized<T: Serialize>(
        payload: Option<&T>,
        lines: Vec<String>,
        error: impl Into<String>,
    ) -> Result<Self> {
        let payload = payload.map(serde_json::to_value).transpose()?;
        Ok(Self::failure(payload, lines, error))
    }

    pub fn emit(self, output: &Output) -> Result<()> {
        if let Some(result) = self.telemetry_result {
            crate::app::mark_telemetry_result(result);
        }
        if let Some(payload) = &self.payload {
            output.emit(payload)?;
        }
        if !output.is_json() {
            for line in &self.lines {
                output.line(line);
            }
        }
        if let Some(error) = self.error {
            bail!(error);
        }
        Ok(())
    }
}
