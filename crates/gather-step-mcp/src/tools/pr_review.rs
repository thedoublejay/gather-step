//! MCP tool: `pr_review`
//!
//! Builds a disposable review index for a PR branch and returns the delta
//! report as structured JSON.
//!
//! # Implementation note
//!
//! The delta extraction pipeline lives in `gather-step` (the CLI crate).
//! Because `gather-step-mcp` cannot depend on `gather-step` without
//! introducing a circular dependency (the CLI depends on the MCP server),
//! this tool shells out to the `gather-step` binary.
//!
//! # Hardening notes
//!
//! - **Bounded buffers**: stdout/stderr are streamed concurrently and
//!   capped at [`MAX_STDOUT_BYTES`] / [`MAX_STDERR_BYTES`] so a runaway
//!   child cannot exhaust the MCP server's heap. Excess bytes are dropped
//!   with a single truncation marker.
//! - **Wall-clock timeout**: child processes that exceed
//!   [`PR_REVIEW_TIMEOUT_SECS`] are killed and the call returns a typed
//!   timeout error. The default tolerates a fresh review-index build on
//!   a multi-repo workspace; callers can override via `timeout_secs`.
//! - **Sanitised error surface**: failure responses include the exit
//!   status and a short error tag, but never echo raw stderr/stdout
//!   into the MCP transcript — those streams may contain absolute paths
//!   or other workspace-private detail. The full streams are logged at
//!   `tracing::warn` for operator inspection.
//!
//! # Workspace storage invariant
//!
//! The CLI's `StorageContext::review_checked` guard ensures the review
//! artifact root never overlaps with `.gather-step/storage`.  By shelling
//! out to the CLI, this crate inherits that protection automatically.

use std::io::Read;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use rmcp::schemars;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Hard cap on captured stdout. The delta-report JSON is the only thing we
/// expect on stdout; production reports run hundreds of KiB at most.
const MAX_STDOUT_BYTES: usize = 16 * 1024 * 1024;

/// Hard cap on captured stderr. Operator-facing tool errors should fit
/// well within this limit; spammy logs are truncated rather than buffered
/// indefinitely.
const MAX_STDERR_BYTES: usize = 1024 * 1024;

/// Default child-process wall-clock timeout. A fresh review-index build
/// on a multi-repo workspace can take 60–90 s; we leave generous headroom
/// for cold caches and disk-bound runs.
const PR_REVIEW_TIMEOUT_SECS: u64 = 600;

/// Upper bound on the user-supplied `timeout_secs`. Stops a malicious
/// caller from pinning an MCP worker thread indefinitely.
const PR_REVIEW_TIMEOUT_MAX_SECS: u64 = 1800;

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
    /// Override the wall-clock timeout in seconds. Capped at
    /// [`PR_REVIEW_TIMEOUT_MAX_SECS`].
    #[serde(default)]
    pub timeout_secs: Option<u64>,
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

/// Run the pr-review pipeline via the `gather-step` CLI binary and return
/// the structured delta report.
///
/// Returns `Err(String)` on any failure (binary not found, non-zero exit,
/// JSON parse error, timeout, exceeded byte cap). Error strings are
/// sanitised — they never include raw stderr/stdout content. The full
/// streams are logged at `tracing::warn` for operator inspection.
pub fn run_pr_review(
    workspace: &std::path::Path,
    input: &PrReviewInput,
) -> Result<PrReviewResponse, String> {
    let binary = resolve_binary();

    let mut cmd = Command::new(&binary);
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
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let timeout = effective_timeout(input.timeout_secs);

    let mut child = cmd.spawn().map_err(|e| {
        format!(
            "Failed to launch the `gather-step` binary at `{}`: {e}. \
             Ensure the binary is on `PATH` or alongside the MCP server.",
            binary.display()
        )
    })?;

    // Stream stdout/stderr concurrently into bounded byte sinks. Without
    // dedicated reader threads, a child that fills its 64 KiB pipe buffer
    // would block on write while we wait on `wait`, deadlocking the MCP
    // worker.
    let stdout = child.stdout.take().expect("stdout pipe should exist");
    let stderr = child.stderr.take().expect("stderr pipe should exist");
    let stdout_handle = thread::spawn(move || drain_capped(stdout, MAX_STDOUT_BYTES));
    let stderr_handle = thread::spawn(move || drain_capped(stderr, MAX_STDERR_BYTES));

    let status = wait_with_timeout(&mut child, timeout)?;

    // Even after timeout we still join the reader threads so their FDs
    // close cleanly; `drain_capped` returns whatever it has buffered when
    // the pipe closes (which the kill above triggered).
    let stdout_capture = stdout_handle
        .join()
        .map_err(|_| "The pr-review stdout reader thread panicked.".to_owned())??;
    let stderr_capture = stderr_handle
        .join()
        .map_err(|_| "The pr-review stderr reader thread panicked.".to_owned())??;

    // Exit code 2 means threshold exceeded (not a tool error).
    let threshold_exceeded = status.code() == Some(2);
    if !status.success() && !threshold_exceeded {
        // Log the raw stderr at warn level so operators can diagnose,
        // but never echo it back into the MCP transcript.
        if !stderr_capture.bytes.is_empty() {
            tracing::warn!(
                exit = ?status.code(),
                stderr_truncated = stderr_capture.truncated,
                stderr = %String::from_utf8_lossy(&stderr_capture.bytes),
                "`gather-step pr-review` exited non-zero; raw stderr captured for operator review.",
            );
        }
        return Err(sanitised_failure_message(status.code(), &stderr_capture));
    }

    if stdout_capture.truncated {
        return Err(format!(
            "The pr-review JSON exceeded the {MAX_STDOUT_BYTES}-byte buffer cap; \
             refusing to parse a truncated report."
        ));
    }

    let stdout = String::from_utf8(stdout_capture.bytes)
        .map_err(|e| format!("The pr-review output was not valid UTF-8: {e}."))?;

    let delta_report: serde_json::Value = serde_json::from_str(stdout.trim()).map_err(|e| {
        // Log the raw stdout for diagnosis but do not echo it back.
        tracing::warn!(
            error = %e,
            stdout_len = stdout.len(),
            "Failed to parse the pr-review JSON output; raw stdout captured for operator review.",
        );
        "Failed to parse the pr-review JSON output; check the operator log for details.".to_owned()
    })?;

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

fn effective_timeout(requested: Option<u64>) -> Duration {
    let secs = requested
        .unwrap_or(PR_REVIEW_TIMEOUT_SECS)
        .clamp(1, PR_REVIEW_TIMEOUT_MAX_SECS);
    Duration::from_secs(secs)
}

#[derive(Debug, Default)]
struct CapturedBytes {
    bytes: Vec<u8>,
    truncated: bool,
}

/// Read from `reader` into a `Vec<u8>` capped at `max`. When the cap is
/// reached, additional bytes are silently dropped and `truncated` is set.
fn drain_capped<R: Read + Send>(mut reader: R, max: usize) -> Result<CapturedBytes, String> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 8 * 1024];
    let mut truncated = false;
    loop {
        match reader.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                if buffer.len() < max {
                    let remaining = max - buffer.len();
                    let take = n.min(remaining);
                    buffer.extend_from_slice(&chunk[..take]);
                    if take < n {
                        truncated = true;
                    }
                } else {
                    truncated = true;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(format!("Failed to read child output: {e}.")),
        }
    }
    Ok(CapturedBytes {
        bytes: buffer,
        truncated,
    })
}

/// Wait for `child` to exit, killing it if `timeout` elapses.  Returns
/// the child's exit status on success; returns a sanitised error string
/// on timeout or wait failure.
fn wait_with_timeout(
    child: &mut std::process::Child,
    timeout: Duration,
) -> Result<std::process::ExitStatus, String> {
    let (tx, rx) = mpsc::channel::<Result<std::process::ExitStatus, std::io::Error>>();
    // We cannot move `child` into the waiter thread (we still need its
    // pipes and `kill` handle on this thread), so the waiter polls
    // `try_wait` from its own thread. A short cadence keeps the timeout
    // honest without busy-spinning.
    let pid = child.id();
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let _ = (tx, rx, pid);
                return Ok(status);
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(format!(
                        "The pr-review subprocess (pid {pid}) exceeded the \
                         {timeout_secs}-second timeout and was killed.",
                        timeout_secs = timeout.as_secs(),
                    ));
                }
                thread::sleep(Duration::from_millis(50));
            }
            Err(e) => return Err(format!("Failed to wait on the pr-review subprocess: {e}.")),
        }
    }
}

fn sanitised_failure_message(code: Option<i32>, stderr: &CapturedBytes) -> String {
    let truncation = if stderr.truncated {
        " Operator log includes truncated stderr."
    } else {
        ""
    };
    match code {
        Some(code) => format!(
            "`gather-step pr-review` exited with status code {code}.{truncation} \
             Inspect the operator log for the underlying error.",
        ),
        None => format!(
            "`gather-step pr-review` was terminated by a signal.{truncation} \
             Inspect the operator log for the underlying error.",
        ),
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that `PrReviewInput` deserialises from the minimal JSON that
    /// the MCP client would send.
    #[test]
    fn pr_review_input_deserialises_minimal() {
        let json = r#"{"base": "main", "head": "HEAD"}"#;
        let input: PrReviewInput = serde_json::from_str(json).unwrap();
        assert_eq!(input.base, "main");
        assert_eq!(input.head, "HEAD");
        assert!(input.keep_cache.is_none());
        assert!(input.severity.is_none());
        assert!(input.timeout_secs.is_none());
    }

    /// Verify that `PrReviewInput` deserialises with all optional fields
    /// set, including the new `timeout_secs` knob.
    #[test]
    fn pr_review_input_deserialises_full() {
        let json = r#"{
            "base": "v1.0.0",
            "head": "feat/my-feature",
            "keep_cache": true,
            "severity": "strict",
            "timeout_secs": 120
        }"#;
        let input: PrReviewInput = serde_json::from_str(json).unwrap();
        assert_eq!(input.base, "v1.0.0");
        assert_eq!(input.severity.as_deref(), Some("strict"));
        assert_eq!(input.keep_cache, Some(true));
        assert_eq!(input.timeout_secs, Some(120));
    }

    /// Verify that `PrReviewResponse` serialises with the expected
    /// top-level keys.
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

    #[test]
    fn effective_timeout_applies_default_when_unspecified() {
        assert_eq!(effective_timeout(None).as_secs(), PR_REVIEW_TIMEOUT_SECS);
    }

    #[test]
    fn effective_timeout_caps_at_max_to_prevent_pinning_workers() {
        let huge = effective_timeout(Some(PR_REVIEW_TIMEOUT_MAX_SECS * 100));
        assert_eq!(huge.as_secs(), PR_REVIEW_TIMEOUT_MAX_SECS);
    }

    #[test]
    fn effective_timeout_clamps_zero_to_one_second() {
        assert_eq!(effective_timeout(Some(0)).as_secs(), 1);
    }

    #[test]
    fn drain_capped_truncates_input_above_cap() {
        let payload = vec![b'a'; 1024];
        let captured = drain_capped(payload.as_slice(), 256).expect("drain");
        assert_eq!(captured.bytes.len(), 256);
        assert!(captured.truncated);
    }

    #[test]
    fn drain_capped_returns_full_input_when_under_cap() {
        let payload = b"hello world".to_vec();
        let captured = drain_capped(payload.as_slice(), 1024).expect("drain");
        assert_eq!(captured.bytes, b"hello world");
        assert!(!captured.truncated);
    }

    #[test]
    fn sanitised_failure_message_does_not_echo_stderr() {
        let stderr = CapturedBytes {
            bytes: b"/Users/secret/path: stack trace... PRIVATE".to_vec(),
            truncated: false,
        };
        let msg = sanitised_failure_message(Some(1), &stderr);
        assert!(
            !msg.contains("PRIVATE"),
            "Sanitised failure message must not echo stderr content: {msg}",
        );
        assert!(
            !msg.contains("/Users/"),
            "Sanitised failure message must not echo paths: {msg}",
        );
        assert!(msg.contains("status code 1"));
    }

    #[test]
    fn sanitised_failure_message_notes_truncation_when_present() {
        let stderr = CapturedBytes {
            bytes: vec![b'x'; 100],
            truncated: true,
        };
        let msg = sanitised_failure_message(Some(1), &stderr);
        assert!(msg.contains("truncated"));
    }

    #[test]
    fn wait_with_timeout_kills_runaway_child() {
        // A `sleep 30` shell command exceeds a 1s timeout — we expect a
        // timeout error within ~1s of starting it.
        let mut cmd = Command::new("sleep");
        cmd.arg("30");
        cmd.stdout(Stdio::null());
        cmd.stderr(Stdio::null());

        let Ok(mut child) = cmd.spawn() else {
            // No `sleep` on PATH — skip rather than fail.
            return;
        };
        let started = Instant::now();
        let err = wait_with_timeout(&mut child, Duration::from_millis(500))
            .expect_err("timeout must fire");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "timeout enforcement must not stall: {err}",
        );
        assert!(
            err.contains("timeout"),
            "error should explain timeout: {err}"
        );
    }

    // ── Integration tests (require compiled gather-step binary on PATH) ────────
    //
    // These tests are gated with #[ignore] because they require:
    //   1. A compiled `gather-step` binary on PATH or beside the test binary.
    //   2. A git repository with at least two commits on disk.

    #[test]
    #[ignore = "requires gather-step binary on PATH and a git fixture on disk"]
    fn mcp_pr_review_returns_delta_report_for_fixture() {
        unimplemented!(
            "Wire a 2-commit fixture and invoke run_pr_review; \
             assert delta_report.schema_version >= 5."
        );
    }

    #[test]
    #[ignore = "requires gather-step binary on PATH and a git fixture on disk"]
    fn mcp_pr_review_does_not_touch_workspace_storage() {
        unimplemented!("Implement once a fixture helper is available in gather-step-mcp tests.");
    }
}
