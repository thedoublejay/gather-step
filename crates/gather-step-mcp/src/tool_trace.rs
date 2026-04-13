//! Per-MCP-tool-call structured trace emission.
//!
//! Each tool invocation emits one JSONL record to either a configured file
//! sink or stderr.  The record schema is:
//!
//! ```json
//! {
//!   "session_id": "<uuid-like string>",
//!   "tool": "search_symbols",
//!   "args_summary": "<redacted short hash or verbatim identifier>",
//!   "elapsed_ms": 12,
//!   "result_count": 5,
//!   "zero_result": false,
//!   "error": null
//! }
//! ```
//!
//! # Privacy
//!
//! Tool-call arguments may contain user-supplied symbol names that include
//! private workspace identifiers.  The [`redact_args`] function hashes
//! free-form string components by default and only keeps a small allowlist of
//! enum-like mode fields verbatim. File paths and repo names are never included
//! verbatim.

#![forbid(unsafe_code)]

use std::{
    fmt::Write as _,
    fs::{self, OpenOptions},
    io::{BufWriter, Write as _},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use tracing::info;

// ─── Privacy redactor ────────────────────────────────────────────────────────

/// Field paths whose values are enum-like and safe to keep readable in traces.
const VERBATIM_STRING_FIELDS: &[&str] = &[
    "mode",
    "scope",
    "kind",
    "language",
    "direction",
    "strategy",
    "resolution",
    "max_depth",
];

/// Redact a single string argument.
///
/// String arguments are hashed with BLAKE3 and returned as
/// `"h:<8-hex-chars>"`.
///
/// # Examples
///
/// ```rust
/// use gather_step_mcp::tool_trace::redact_arg;
///
/// let short = redact_arg("findUser");
/// assert!(short.starts_with("h:"));
///
/// let long_val = "a".repeat(33);
/// let hashed = redact_arg(&long_val);
/// assert!(hashed.starts_with("h:"), "long arg must be hashed");
/// ```
#[must_use]
pub fn redact_arg(value: &str) -> String {
    hash_string(value)
}

fn hash_string(value: &str) -> String {
    let digest = blake3::hash(value.as_bytes());
    let hex: String = digest.as_bytes()[..4]
        .iter()
        .fold(String::with_capacity(8), |mut acc, b| {
            let _ = write!(acc, "{b:02x}");
            acc
        });
    format!("h:{hex}")
}

fn redact_string_field(prefix: &str, value: &str) -> String {
    if VERBATIM_STRING_FIELDS.contains(&prefix) {
        value.to_owned()
    } else {
        redact_arg(value)
    }
}

/// Build a compact args summary from a `serde_json::Value`.
///
/// Only string leaf values are redacted.  Non-string scalars (numbers,
/// booleans, nulls) are kept verbatim because they carry no PII.  Nested
/// objects and arrays are flattened into `key=value` pairs joined by `|`.
///
/// # Examples
///
/// ```rust
/// use gather_step_mcp::tool_trace::redact_args;
/// use serde_json::json;
///
/// let args = json!({"query": "findUser", "limit": 10});
/// let summary = redact_args(&args);
/// assert!(summary.contains("query=h:"));
/// assert!(summary.contains("limit=10"));
/// ```
#[must_use]
pub fn redact_args(args: &serde_json::Value) -> String {
    let mut parts: Vec<String> = Vec::new();
    collect_parts(args, "", &mut parts);
    parts.join("|")
}

fn collect_parts(value: &serde_json::Value, prefix: &str, out: &mut Vec<String>) {
    match value {
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                let key = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}.{k}")
                };
                collect_parts(v, &key, out);
            }
        }
        serde_json::Value::Array(arr) => {
            for (i, v) in arr.iter().enumerate() {
                let key = if prefix.is_empty() {
                    format!("[{i}]")
                } else {
                    format!("{prefix}[{i}]")
                };
                collect_parts(v, &key, out);
            }
        }
        serde_json::Value::String(s) => {
            let redacted = redact_string_field(prefix, s);
            out.push(format!("{prefix}={redacted}"));
        }
        other => {
            out.push(format!("{prefix}={other}"));
        }
    }
}

// ─── Trace record ────────────────────────────────────────────────────────────

// ─── Result-count extractor ───────────────────────────────────────────────────

/// Probe a serialised tool response for a meaningful item count.
///
/// Walks common result-bearing fields in approximate priority order.  Returns
/// the first non-zero count found, or `0` when the response carries no
/// recognisable array payload.
///
/// This is intentionally a best-effort heuristic: it trades precision for
/// zero coupling with individual tool response types.
///
/// # Examples
///
/// ```rust
/// use gather_step_mcp::tool_trace::count_results_from_json;
/// use serde_json::json;
///
/// let v = json!({"data": {"results": [1, 2, 3]}});
/// assert_eq!(count_results_from_json(&v), 3);
///
/// let empty = json!({"data": {"results": []}});
/// assert_eq!(count_results_from_json(&empty), 0);
/// ```
#[must_use]
pub fn count_results_from_json(value: &serde_json::Value) -> usize {
    // Ordered list of (path segments, field name) to probe.  Earlier entries
    // shadow later ones.  We look at both top-level and nested `data.*` forms.
    const ARRAY_FIELDS: &[&str] = &[
        "results",
        "items",
        "matches",
        "topics",
        "callers",
        "consumers",
        "producers",
        "files",
        "repos",
        "symbols",
        "candidates",
        "edges",
        "nodes",
        "drift",
        "sequences",
        "orphans",
    ];

    // Helper: count items in `root[field]` if it's an array.
    let count_in = |root: &serde_json::Value, field: &str| -> Option<usize> {
        root.get(field)
            .and_then(serde_json::Value::as_array)
            .map(Vec::len)
    };

    // Try top-level fields.
    for field in ARRAY_FIELDS {
        if let Some(n) = count_in(value, field) {
            return n;
        }
    }

    // Try under `data`.
    if let Some(data) = value.get("data") {
        for field in ARRAY_FIELDS {
            if let Some(n) = count_in(data, field) {
                return n;
            }
        }
    }

    // Try under `data.X` for any single-key envelope.
    if let Some(data) = value.get("data")
        && let Some(obj) = data.as_object()
    {
        for inner_val in obj.values() {
            for field in ARRAY_FIELDS {
                if let Some(n) = count_in(inner_val, field) {
                    return n;
                }
            }
        }
    }

    0
}

// ─── Timing helper ────────────────────────────────────────────────────────────

/// Record the elapsed milliseconds since `start`, saturating at [`u64::MAX`].
#[must_use]
pub fn elapsed_ms(start: std::time::Instant) -> u64 {
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

// ─── Trace record ────────────────────────────────────────────────────────────

/// A single tool-call trace record, serialisable as one JSONL line.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct ToolCallRecord {
    /// Opaque session identifier (stable within one server process lifetime).
    pub session_id: String,
    /// Tool name (e.g. `"search"`, `"planning_pack"`).
    pub tool: String,
    /// Privacy-safe summary of the call arguments.
    pub args_summary: String,
    /// Wall-clock duration of the tool execution in milliseconds.
    pub elapsed_ms: u64,
    /// Number of result items returned by the tool (0 when the tool returned
    /// an empty or error result).
    pub result_count: usize,
    /// `true` when `result_count == 0`.
    pub zero_result: bool,
    /// Error message if the tool returned an error, otherwise `null`.
    pub error: Option<String>,
}

impl ToolCallRecord {
    /// Construct a trace record from raw components.
    #[must_use]
    pub fn new(
        session_id: &str,
        tool: &str,
        args_summary: String,
        elapsed_ms: u64,
        result_count: usize,
        error: Option<String>,
    ) -> Self {
        Self {
            session_id: session_id.to_owned(),
            tool: tool.to_owned(),
            args_summary,
            elapsed_ms,
            result_count,
            zero_result: result_count == 0,
            error,
        }
    }
}

// ─── Trace sink ──────────────────────────────────────────────────────────────

/// Where tool-call trace records are written.
#[derive(Clone, Debug)]
pub enum TraceSink {
    /// Emit records as `tracing::info!` events (the default).
    TracingInfo,
    /// Append records as JSONL to the given file path.
    File(PathBuf),
}

/// Shared, thread-safe writer for the file-based trace sink.
///
/// Internally wraps a [`BufWriter`] behind a [`Mutex`] so that concurrent
/// tool calls can safely write without interleaving partial lines.
#[derive(Clone, Debug)]
pub struct TraceWriter(Arc<Mutex<BufWriter<std::fs::File>>>);

impl TraceWriter {
    /// Open or create `path` for append, wrapping it in a buffered writer.
    ///
    /// # Errors
    ///
    /// Returns an [`std::io::Error`] if the file cannot be opened.
    pub fn open(path: &std::path::Path) -> std::io::Result<Self> {
        let file = open_trace_file(path)?;
        Ok(Self(Arc::new(Mutex::new(BufWriter::new(file)))))
    }

    /// Serialise `record` as a single JSONL line and flush.
    ///
    /// # Errors
    ///
    /// Returns an [`std::io::Error`] if serialisation or writing fails.
    pub fn write(&self, record: &ToolCallRecord) -> std::io::Result<()> {
        let line = serde_json::to_string(record).map_err(std::io::Error::other)?;
        let mut guard = self
            .0
            .lock()
            .map_err(|_| std::io::Error::other("trace writer mutex poisoned"))?;
        guard.write_all(line.as_bytes())?;
        guard.write_all(b"\n")?;
        guard.flush()?;
        Ok(())
    }
}

fn open_trace_file(path: &Path) -> std::io::Result<std::fs::File> {
    if let Ok(metadata) = fs::symlink_metadata(path) {
        if metadata.file_type().is_symlink() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "refusing to open trace sink through a symlink",
            ));
        }
        if !metadata.file_type().is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "trace sink path is not a regular file",
            ));
        }
    }
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(file)
}

// ─── Session-scoped tracer ────────────────────────────────────────────────────

/// A lightweight per-session tracer that knows where to emit records.
///
/// Construct one via [`Tracer::new`] and pass it into the dispatch layer.
/// The session ID is a short hex string derived from a 64-bit counter + PID;
/// it is scoped to a single process lifetime and is never reused across
/// restarts.
#[derive(Clone, Debug)]
pub struct Tracer {
    session_id: String,
    writer: Option<TraceWriter>,
}

impl Tracer {
    /// Create a tracer that emits to `tracing::info!`.
    #[must_use]
    pub fn new_info(session_id: String) -> Self {
        Self {
            session_id,
            writer: None,
        }
    }

    /// Create a tracer that appends JSONL to a file.
    ///
    /// # Errors
    ///
    /// Returns an [`std::io::Error`] if the file cannot be opened.
    pub fn new_file(session_id: String, path: &std::path::Path) -> std::io::Result<Self> {
        Ok(Self {
            session_id,
            writer: Some(TraceWriter::open(path)?),
        })
    }

    /// Emit one trace record.
    ///
    /// Failures in the file sink are silently dropped so that instrumentation
    /// never interrupts a tool call.
    pub fn emit(&self, record: &ToolCallRecord) {
        if let Some(w) = &self.writer {
            if let Err(err) = w.write(record) {
                info!(
                    error = %err,
                    "tool_trace: file write failed; dropping record"
                );
            }
        } else {
            // Emit to tracing at INFO level.  The JSON value is inlined so
            // that structured log processors can parse it directly.
            let json = serde_json::to_string(record).unwrap_or_default();
            info!(tool_trace = %json, "tool_call");
        }
    }

    /// Return the session identifier.
    #[must_use]
    pub fn session_id(&self) -> &str {
        &self.session_id
    }
}

/// Generate a session ID that is stable within one process lifetime.
///
/// The ID is a hex string composed of the PID and a per-process monotonic
/// counter, making collisions across sessions extremely unlikely while keeping
/// the value short and opaque.
#[must_use]
pub fn new_session_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{:08x}{:08x}", std::process::id(), seq)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_arg_hashes_short_values() {
        let result = redact_arg("findUser");
        assert!(result.starts_with("h:"), "expected h: prefix; got {result}");
    }

    #[test]
    fn redact_arg_hashes_long_values() {
        let long_val = "a".repeat(64);
        let result = redact_arg(&long_val);
        assert!(result.starts_with("h:"), "expected h: prefix; got {result}");
        // Must be h: followed by exactly 8 hex chars.
        let hex_part = result.trim_start_matches("h:");
        assert_eq!(hex_part.len(), 8, "expected 8 hex chars after h:");
        assert!(
            hex_part.chars().all(|c| c.is_ascii_hexdigit()),
            "expected hex chars; got {hex_part}"
        );
    }

    #[test]
    fn redact_arg_is_deterministic_for_long_values() {
        let long_val = "x".repeat(100);
        assert_eq!(redact_arg(&long_val), redact_arg(&long_val));
    }

    #[test]
    fn redact_args_handles_nested_object() {
        let args = serde_json::json!({
            "query": "findUser",
            "limit": 10,
            "filter": {"kind": "Function"},
            "mode": "planning"
        });
        let summary = redact_args(&args);
        assert!(summary.contains("query=h:"), "summary: {summary}");
        assert!(summary.contains("limit=10"), "summary: {summary}");
        assert!(summary.contains("mode=planning"), "summary: {summary}");
        assert!(summary.contains("filter.kind=h:"), "summary: {summary}");
    }

    #[test]
    fn tool_call_record_zero_result_matches_count() {
        let r = ToolCallRecord::new("sess", "search", "q=foo".to_owned(), 5, 0, None);
        assert!(r.zero_result);

        let r2 = ToolCallRecord::new("sess", "search", "q=foo".to_owned(), 5, 3, None);
        assert!(!r2.zero_result);
    }

    #[test]
    fn trace_writer_emits_valid_jsonl() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let writer = TraceWriter::open(tmp.path()).unwrap();

        let record = ToolCallRecord::new(
            "aabbccdd00000000",
            "search",
            "query=findUser".to_owned(),
            12,
            5,
            None,
        );
        writer.write(&record).unwrap();

        let contents = std::fs::read_to_string(tmp.path()).unwrap();
        let line = contents.lines().next().unwrap();
        let parsed: ToolCallRecord = serde_json::from_str(line).unwrap();
        assert_eq!(parsed, record);
    }

    #[cfg(unix)]
    #[test]
    fn trace_writer_creates_private_file() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trace.jsonl");

        let _writer = TraceWriter::open(&path).unwrap();

        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[cfg(unix)]
    #[test]
    fn trace_writer_rejects_symlink_target() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target.jsonl");
        std::fs::write(&target, "").unwrap();
        let link = dir.path().join("trace.jsonl");
        symlink(&target, &link).unwrap();

        let error = TraceWriter::open(&link).unwrap_err().to_string();

        assert!(
            error.contains("symlink"),
            "expected symlink rejection, got: {error}"
        );
    }

    #[test]
    fn count_results_from_json_top_level_array() {
        use super::count_results_from_json;
        let v = serde_json::json!({"results": [1, 2, 3]});
        assert_eq!(count_results_from_json(&v), 3);
    }

    #[test]
    fn count_results_from_json_nested_data() {
        use super::count_results_from_json;
        let v = serde_json::json!({"data": {"results": ["a", "b"]}});
        assert_eq!(count_results_from_json(&v), 2);
    }

    #[test]
    fn count_results_from_json_empty_returns_zero() {
        use super::count_results_from_json;
        let v = serde_json::json!({"data": {"results": []}});
        assert_eq!(count_results_from_json(&v), 0);
    }

    #[test]
    fn count_results_from_json_no_known_field_returns_zero() {
        use super::count_results_from_json;
        let v = serde_json::json!({"other": [1, 2, 3]});
        assert_eq!(count_results_from_json(&v), 0);
    }

    #[test]
    fn tracer_file_sink_round_trips() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let tracer = Tracer::new_file("test-session".to_owned(), tmp.path()).unwrap();

        let record = ToolCallRecord::new(
            "test-session",
            "planning_pack",
            "target=SomeService".to_owned(),
            42,
            3,
            None,
        );
        tracer.emit(&record);

        let contents = std::fs::read_to_string(tmp.path()).unwrap();
        let line = contents.lines().next().unwrap();
        let parsed: ToolCallRecord = serde_json::from_str(line).unwrap();
        assert_eq!(parsed, record);
    }
}
