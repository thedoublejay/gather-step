use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::app::Output;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RenderedCommand {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lines: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl RenderedCommand {
    #[must_use]
    pub fn success(payload: Value, lines: Vec<String>) -> Self {
        Self {
            payload: Some(payload),
            lines,
            error: None,
        }
    }

    #[must_use]
    pub fn failure(payload: Option<Value>, lines: Vec<String>, error: impl Into<String>) -> Self {
        Self {
            payload,
            lines,
            error: Some(error.into()),
        }
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
