//! MCP tool: `pr_review`
//!
//! Builds a disposable review index for a PR branch and returns the delta
//! report as structured JSON.
//!
//! # Implementation note
//!
//! The delta extraction pipeline lives in `gather-step` (the CLI crate).
//! Because `gather-step-mcp` cannot depend on `gather-step` without introducing
//! a circular dependency (the CLI depends on the MCP server), this tool shells
//! out to the `gather-step` binary.
//!
//! The tool is registered in the MCP server.  Integration tests that require a
//! compiled binary are gated with `#[ignore]` — see the test module below.
//!
//! # Workspace storage invariant
//!
//! The CLI's `StorageContext::review_checked` guard ensures the review
//! artifact root never overlaps with `.gather-step/storage`.  By shelling out
//! to the CLI, this crate inherits that protection automatically.

use rmcp::schemars;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// ─── Request / response types ─────────────────────────────────────────────────

/// Input parameters for the `pr_review` MCP tool.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct PrReviewInput {
    /// Base ref (branch, tag, SHA, or any git rev).
    pub base: String,
    /// Head ref (branch, tag, SHA, "HEAD", …).
    pub head: String,
    /// Keep the review artifact after the run.
    #[serde(default)]
    pub keep_cache: Option<bool>,
    /// Severity mode: `"warn"` (default) | `"strict"` | `"pedantic"`.
    #[serde(default)]
    pub severity: Option<String>,
}

/// Structured response returned by the `pr_review` MCP tool.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct PrReviewResponse {
    /// The full `DeltaReport` as a JSON value.
    pub delta_report: serde_json::Value,
    /// `true` when the effective severity threshold was exceeded.
    pub threshold_exceeded: bool,
}

// ─── Tool function ────────────────────────────────────────────────────────────

/// Run the pr-review pipeline via the `gather-step` CLI binary and return the
/// structured delta report.
///
/// Returns `Err(String)` on any failure (binary not found, non-zero exit, JSON
/// parse error).
pub fn run_pr_review(
    workspace: &std::path::Path,
    input: &PrReviewInput,
) -> Result<PrReviewResponse, String> {
    let binary = resolve_binary();

    let mut cmd = std::process::Command::new(&binary);
    cmd.arg("--workspace").arg(workspace);
    cmd.arg("pr-review");
    cmd.arg("--base").arg(&input.base);
    cmd.arg("--head").arg(&input.head);
    cmd.arg("--format").arg("json");

    if input.keep_cache.unwrap_or(false) {
        cmd.arg("--keep-cache");
    }

    if let Some(sev) = &input.severity {
        cmd.arg("--severity").arg(sev);
    }

    let output = cmd.output().map_err(|e| {
        format!(
            "failed to launch `{}`: {e}. \
             Ensure the `gather-step` binary is on PATH or the same directory as the MCP server.",
            binary.display()
        )
    })?;

    // Exit code 2 means threshold exceeded (not an error).
    let threshold_exceeded = output.status.code() == Some(2);
    if !output.status.success() && !threshold_exceeded {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "gather-step pr-review exited with {}: {stderr}",
            output.status
        ));
    }

    let stdout = String::from_utf8(output.stdout)
        .map_err(|e| format!("pr-review output was not valid UTF-8: {e}"))?;

    let delta_report: serde_json::Value = serde_json::from_str(stdout.trim())
        .map_err(|e| format!("failed to parse pr-review JSON output: {e}\nOutput: {stdout}"))?;

    Ok(PrReviewResponse {
        delta_report,
        threshold_exceeded,
    })
}

/// Resolve the path to the `gather-step` binary.
///
/// Searches (in order):
/// 1. Same directory as the current executable.
/// 2. `gather-step` on `PATH`.
fn resolve_binary() -> std::path::PathBuf {
    if let Ok(exe) = std::env::current_exe() {
        let sibling = exe.with_file_name("gather-step");
        if sibling.exists() {
            return sibling;
        }
    }
    std::path::PathBuf::from("gather-step")
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that `PrReviewInput` deserialises from the minimal JSON that the
    /// MCP client would send.
    #[test]
    fn pr_review_input_deserialises_minimal() {
        let json = r#"{"base": "main", "head": "HEAD"}"#;
        let input: PrReviewInput = serde_json::from_str(json).unwrap();
        assert_eq!(input.base, "main");
        assert_eq!(input.head, "HEAD");
        assert!(input.keep_cache.is_none());
        assert!(input.severity.is_none());
    }

    /// Verify that `PrReviewInput` deserialises with all optional fields set.
    #[test]
    fn pr_review_input_deserialises_full() {
        let json = r#"{
            "base": "v1.0.0",
            "head": "feat/my-feature",
            "keep_cache": true,
            "severity": "strict"
        }"#;
        let input: PrReviewInput = serde_json::from_str(json).unwrap();
        assert_eq!(input.base, "v1.0.0");
        assert_eq!(input.severity.as_deref(), Some("strict"));
        assert_eq!(input.keep_cache, Some(true));
    }

    /// Verify that `PrReviewResponse` serialises with the expected top-level keys.
    #[test]
    fn pr_review_response_serialises() {
        let resp = PrReviewResponse {
            delta_report: serde_json::json!({"schema_version": 7}),
            threshold_exceeded: false,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert!(json.get("delta_report").is_some());
        assert!(json.get("threshold_exceeded").is_some());
    }

    // ── Integration tests (require compiled gather-step binary on PATH) ────────
    //
    // These tests are gated with #[ignore] because they require:
    //   1. A compiled `gather-step` binary on PATH or beside the test binary.
    //   2. A git repository with at least two commits on disk.
    //
    // TODO: wire a fixture-based integration test once the binary is
    // reliably available in CI without a full workspace build pre-step.

    /// Invoke `pr_review` against a 2-commit fixture and verify the response
    /// has `schema_version >= 5` and all expected top-level keys.
    ///
    /// Requires `gather-step` binary. Run with:
    /// `cargo test -p gather-step-mcp mcp_pr_review_returns_delta_report -- --ignored`
    #[test]
    #[ignore = "requires gather-step binary on PATH and a git fixture on disk"]
    fn mcp_pr_review_returns_delta_report_for_fixture() {
        // This test is a placeholder; real implementation requires a fixture.
        // See the TODO in the module-level doc comment.
        unimplemented!(
            "wire a 2-commit fixture and invoke run_pr_review; \
             assert delta_report.schema_version >= 5"
        );
    }

    /// Verify the workspace `.gather-step/` directory is not mutated by a
    /// `pr_review` tool call.
    ///
    /// Requires `gather-step` binary.
    #[test]
    #[ignore = "requires gather-step binary on PATH and a git fixture on disk"]
    fn mcp_pr_review_does_not_touch_workspace_storage() {
        // TODO: checksum .gather-step/ before and after run_pr_review call.
        unimplemented!("implement once a fixture helper is available in gather-step-mcp tests");
    }
}
