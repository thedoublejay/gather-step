//! Tool-call trace analysis for benchmark sessions.
//!
//! Reads JSONL files produced by the MCP server's tool-call tracer and computes
//! per-session and aggregate metrics that help distinguish "fast and right" from
//! "fast because the tool stopped early (zero results)".
//!
//! # Per-session metrics
//!
//! - `call_count` — total tool calls in the session.
//! - `zero_result_count` — calls that returned `result_count == 0`.
//! - `wrong_path_rate` — `zero_result_count / call_count`.
//! - `time_to_first_correct_anchor_ms` — milliseconds from the first call's
//!   timestamp offset to the first call whose `tool` appears in the supplied
//!   ground-truth `must_find` set (optional, requires `--ground-truth`).
//! - `call_sequence_pattern` — ordered list of tool names.
//!
//! # Aggregate report (across sessions)
//!
//! - Median `wrong_path_rate`.
//! - Median `time_to_first_correct_anchor_ms`.
//! - Top-N modal call sequences.

#![forbid(unsafe_code)]

use std::{collections::BTreeMap, path::Path};

use gather_step_mcp::ToolCallRecord;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};

// ─── Per-session aggregation ─────────────────────────────────────────────────

/// Metrics derived from all tool calls within a single benchmark session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionMetrics {
    /// Session identifier extracted from the first record.
    pub session_id: String,
    /// Total number of tool calls observed.
    pub call_count: usize,
    /// Number of calls that returned zero results.
    pub zero_result_count: usize,
    /// Fraction of calls that returned zero results (`zero_result_count /
    /// call_count`), or `0.0` when `call_count == 0`.
    pub wrong_path_rate: f64,
    /// Milliseconds from the start of the session to the first call that
    /// returned a result-count > 0 for a tool name that appears in the
    /// `must_find` ground-truth set.  `None` when no such call was found or
    /// when no ground-truth set was supplied.
    pub time_to_first_correct_anchor_ms: Option<u64>,
    /// Ordered list of tool names called during this session.
    pub call_sequence_pattern: Vec<String>,
}

/// Compute [`SessionMetrics`] from a slice of trace records.
///
/// `must_find` is an optional set of tool names that count as "correct anchor"
/// calls.  Pass an empty set (or `None`) to skip the anchor metric.
///
/// Returns `None` when `records` is empty.
#[must_use]
#[expect(
    clippy::implicit_hasher,
    reason = "FxHashSet is already an explicit hasher alias; callers use FxHashSet throughout"
)]
pub fn aggregate_session(
    records: &[ToolCallRecord],
    must_find: &FxHashSet<String>,
) -> Option<SessionMetrics> {
    if records.is_empty() {
        return None;
    }

    let session_id = records[0].session_id.clone();
    let call_count = records.len();
    let zero_result_count = records.iter().filter(|r| r.zero_result).count();
    let wrong_path_rate = if call_count == 0 {
        0.0
    } else {
        #[expect(
            clippy::cast_precision_loss,
            reason = "call counts are always small; f64 precision is adequate"
        )]
        let rate = zero_result_count as f64 / call_count as f64;
        rate
    };

    // Time-to-first-correct-anchor: cumulative elapsed_ms up to the first
    // non-zero-result call whose tool is in must_find.
    let time_to_first_correct_anchor_ms = if must_find.is_empty() {
        None
    } else {
        let mut cumulative: u64 = 0;
        let mut found = None;
        for record in records {
            cumulative = cumulative.saturating_add(record.elapsed_ms);
            if !record.zero_result && must_find.contains(&record.tool) {
                found = Some(cumulative);
                break;
            }
        }
        found
    };

    let call_sequence_pattern = records.iter().map(|r| r.tool.clone()).collect();

    Some(SessionMetrics {
        session_id,
        call_count,
        zero_result_count,
        wrong_path_rate,
        time_to_first_correct_anchor_ms,
        call_sequence_pattern,
    })
}

// ─── JSONL parsing ────────────────────────────────────────────────────────────

/// Parse all [`ToolCallRecord`] lines from a JSONL file.
///
/// Lines that fail to parse are silently skipped (with a warning logged at
/// `tracing::warn`).
///
/// # Errors
///
/// Returns an [`std::io::Error`] if the file cannot be read.
pub fn parse_trace_file(path: &Path) -> std::io::Result<Vec<ToolCallRecord>> {
    let content = std::fs::read_to_string(path)?;
    let mut records = Vec::new();
    for (line_no, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<ToolCallRecord>(line) {
            Ok(r) => records.push(r),
            Err(err) => {
                tracing::warn!(
                    path = %path.display(),
                    line = line_no + 1,
                    error = %err,
                    "tool_trace: skipping malformed JSONL line"
                );
            }
        }
    }
    Ok(records)
}

/// Group a flat list of [`ToolCallRecord`] values by session ID.
#[must_use]
pub fn group_by_session(records: Vec<ToolCallRecord>) -> BTreeMap<String, Vec<ToolCallRecord>> {
    let mut map: BTreeMap<String, Vec<ToolCallRecord>> = BTreeMap::new();
    for record in records {
        map.entry(record.session_id.clone())
            .or_default()
            .push(record);
    }
    map
}

// ─── Cross-session aggregate ──────────────────────────────────────────────────

/// Aggregate metrics computed across multiple benchmark sessions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateReport {
    /// Number of sessions included in this report.
    pub session_count: usize,
    /// Median wrong-path rate across sessions (0.0–1.0).
    pub median_wrong_path_rate: f64,
    /// Median time-to-first-correct-anchor in milliseconds.  `None` when no
    /// session had a matching anchor call.
    pub median_time_to_first_correct_anchor_ms: Option<u64>,
    /// Top modal call sequences (most common orderings), up to `top_n`.
    pub top_modal_sequences: Vec<ModalSequence>,
    /// Per-session metrics, sorted by session ID for deterministic output.
    pub sessions: Vec<SessionMetrics>,
}

/// A call-sequence pattern together with the number of sessions that matched it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModalSequence {
    /// The sequence of tool names.
    pub sequence: Vec<String>,
    /// Number of sessions whose call sequence exactly matches this pattern.
    pub count: usize,
}

/// Compute an [`AggregateReport`] from a set of sessions.
///
/// `must_find` is forwarded to [`aggregate_session`].
/// `top_n` controls how many modal sequences are included in the report.
#[must_use]
#[expect(
    clippy::implicit_hasher,
    reason = "FxHashSet is already an explicit hasher alias; callers use FxHashSet throughout"
)]
pub fn aggregate_report(
    sessions_by_id: &BTreeMap<String, Vec<ToolCallRecord>>,
    must_find: &FxHashSet<String>,
    top_n: usize,
) -> AggregateReport {
    let mut session_metrics: Vec<SessionMetrics> = sessions_by_id
        .values()
        .filter_map(|records| aggregate_session(records, must_find))
        .collect();
    session_metrics.sort_by(|a, b| a.session_id.cmp(&b.session_id));

    let session_count = session_metrics.len();

    let median_wrong_path_rate = median_f64(
        &session_metrics
            .iter()
            .map(|s| s.wrong_path_rate)
            .collect::<Vec<_>>(),
    );

    let anchor_values: Vec<u64> = session_metrics
        .iter()
        .filter_map(|s| s.time_to_first_correct_anchor_ms)
        .collect();
    let median_time_to_first_correct_anchor_ms = median_u64(&anchor_values);

    let top_modal_sequences = top_modal_call_sequences(&session_metrics, top_n);

    AggregateReport {
        session_count,
        median_wrong_path_rate,
        median_time_to_first_correct_anchor_ms,
        top_modal_sequences,
        sessions: session_metrics,
    }
}

// ─── Modal sequence ───────────────────────────────────────────────────────────

fn top_modal_call_sequences(sessions: &[SessionMetrics], top_n: usize) -> Vec<ModalSequence> {
    if top_n == 0 || sessions.is_empty() {
        return Vec::new();
    }
    let mut freq: FxHashMap<Vec<String>, usize> = FxHashMap::default();
    for s in sessions {
        *freq.entry(s.call_sequence_pattern.clone()).or_insert(0) += 1;
    }
    let mut sorted: Vec<(Vec<String>, usize)> = freq.into_iter().collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    sorted
        .into_iter()
        .take(top_n)
        .map(|(sequence, count)| ModalSequence { sequence, count })
        .collect()
}

// ─── Statistics helpers ───────────────────────────────────────────────────────

fn median_f64(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let n = sorted.len();
    let mid = n / 2;
    if n.is_multiple_of(2) {
        sorted[mid - 1].midpoint(sorted[mid])
    } else {
        sorted[mid]
    }
}

fn median_u64(values: &[u64]) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let n = sorted.len();
    let mid = n / 2;
    if n.is_multiple_of(2) {
        Some(sorted[mid - 1].midpoint(sorted[mid]))
    } else {
        Some(sorted[mid])
    }
}

// ─── Ground-truth loader ──────────────────────────────────────────────────────

/// A minimal ground-truth structure used to derive the `must_find` set.
///
/// The file is expected to be a JSON object with an array field `"must_find"`
/// containing tool names (strings).
///
/// Example:
/// ```json
/// { "must_find": ["search", "planning_pack"] }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroundTruth {
    /// Tool names that are considered "correct anchor" calls.
    pub must_find: Vec<String>,
}

impl GroundTruth {
    /// Load a ground-truth JSON file.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or parsed.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let gt: Self = serde_json::from_str(&content)?;
        Ok(gt)
    }

    /// Convert to a `FxHashSet` of tool names for fast lookup.
    #[must_use]
    pub fn into_must_find_set(self) -> FxHashSet<String> {
        self.must_find.into_iter().collect()
    }
}

// ─── Report rendering ─────────────────────────────────────────────────────────

/// Render an [`AggregateReport`] as a human-readable string for printing.
#[must_use]
pub fn render_aggregate_report(report: &AggregateReport) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();

    let _ = writeln!(out, "sessions: {}", report.session_count);
    let _ = writeln!(
        out,
        "median_wrong_path_rate: {:.3}",
        report.median_wrong_path_rate
    );
    if let Some(ms) = report.median_time_to_first_correct_anchor_ms {
        let _ = writeln!(out, "median_time_to_first_correct_anchor_ms: {ms}");
    } else {
        out.push_str("median_time_to_first_correct_anchor_ms: n/a\n");
    }
    out.push_str("\ntop modal call sequences:\n");
    for (i, seq) in report.top_modal_sequences.iter().enumerate() {
        let _ = writeln!(
            out,
            "  #{}: [{}] (count={})",
            i + 1,
            seq.sequence.join(" -> "),
            seq.count
        );
    }

    out.push_str("\nper-session:\n");
    for s in &report.sessions {
        let anchor = s
            .time_to_first_correct_anchor_ms
            .map_or_else(|| "n/a".to_owned(), |ms| format!("{ms}ms"));
        let _ = writeln!(
            out,
            "  {} calls={} zero={} wpr={:.3} anchor={}",
            s.session_id, s.call_count, s.zero_result_count, s.wrong_path_rate, anchor
        );
    }

    out
}

// ─── Glob expansion helper ────────────────────────────────────────────────────

/// Expand a glob pattern into a sorted list of matching paths.
///
/// # Errors
///
/// Returns an error if the glob pattern is invalid or path enumeration fails.
pub fn expand_glob(pattern: &str) -> anyhow::Result<Vec<std::path::PathBuf>> {
    use std::path::PathBuf;
    // Simple implementation: walk the parent directory and match by glob.
    // We use the `globset` crate already in the workspace.
    use globset::{Glob, GlobSetBuilder};

    let glob = Glob::new(pattern)?;
    let mut builder = GlobSetBuilder::new();
    builder.add(glob);
    let set = builder.build()?;

    // Determine the search root from the pattern's parent path.
    let base = {
        let p = Path::new(pattern);
        // Walk up until we find a component without glob metacharacters.
        let mut root = PathBuf::new();
        for component in p.components() {
            let s = component.as_os_str().to_string_lossy();
            if s.contains('*') || s.contains('?') || s.contains('[') {
                break;
            }
            root.push(component);
        }
        if root.as_os_str().is_empty() {
            PathBuf::from(".")
        } else {
            root
        }
    };

    let mut paths = Vec::new();
    if base.is_dir() {
        for entry in std::fs::read_dir(&base)? {
            let entry = entry?;
            let path = entry.path();
            if set.is_match(&path) {
                paths.push(path);
            }
        }
    } else if base.is_file() && set.is_match(&base) {
        paths.push(base);
    }

    paths.sort();
    Ok(paths)
}

#[cfg(test)]
mod tests {
    use gather_step_mcp::ToolCallRecord;
    use rustc_hash::FxHashSet;

    use super::*;

    fn make_record(
        session: &str,
        tool: &str,
        elapsed_ms: u64,
        result_count: usize,
    ) -> ToolCallRecord {
        ToolCallRecord::new(
            session,
            tool,
            format!("args={tool}"),
            elapsed_ms,
            result_count,
            None,
        )
    }

    // ─── Wrong-path-rate computation ──────────────────────────────────────────

    #[test]
    fn wrong_path_rate_is_correct_for_mixed_results() {
        // 2 zero-result out of 5 calls => rate = 0.4
        let records = vec![
            make_record("s1", "search", 10, 3),        // non-zero
            make_record("s1", "search", 5, 0),         // zero
            make_record("s1", "planning_pack", 20, 1), // non-zero
            make_record("s1", "get_callers", 8, 0),    // zero
            make_record("s1", "search", 12, 2),        // non-zero
        ];
        let metrics = aggregate_session(&records, &FxHashSet::default()).unwrap();
        assert_eq!(metrics.call_count, 5);
        assert_eq!(metrics.zero_result_count, 2);
        // 2/5 = 0.4
        assert!((metrics.wrong_path_rate - 0.4).abs() < f64::EPSILON);
    }

    #[test]
    fn wrong_path_rate_is_zero_when_all_calls_return_results() {
        let records = vec![
            make_record("s2", "search", 10, 5),
            make_record("s2", "planning_pack", 20, 2),
        ];
        let metrics = aggregate_session(&records, &FxHashSet::default()).unwrap();
        assert!((metrics.wrong_path_rate - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn wrong_path_rate_is_one_when_all_calls_return_zero() {
        let records = vec![
            make_record("s3", "search", 10, 0),
            make_record("s3", "get_callers", 5, 0),
        ];
        let metrics = aggregate_session(&records, &FxHashSet::default()).unwrap();
        assert!((metrics.wrong_path_rate - 1.0).abs() < f64::EPSILON);
    }

    // ─── Time-to-first-correct-anchor ─────────────────────────────────────────

    #[test]
    fn time_to_first_correct_anchor_identifies_first_match() {
        // Ground truth: "planning_pack" must be found.
        // Call sequence: search(10ms, 0 results), planning_pack(20ms, 3 results)
        // Cumulative at planning_pack hit: 10 + 20 = 30ms.
        let records = vec![
            make_record("s4", "search", 10, 0),        // miss
            make_record("s4", "planning_pack", 20, 3), // hit
            make_record("s4", "search", 5, 1),         // irrelevant
        ];
        let must_find: FxHashSet<String> = ["planning_pack".to_owned()].into_iter().collect();
        let metrics = aggregate_session(&records, &must_find).unwrap();
        assert_eq!(metrics.time_to_first_correct_anchor_ms, Some(30));
    }

    #[test]
    fn time_to_first_correct_anchor_none_when_no_matching_tool() {
        let records = vec![
            make_record("s5", "search", 10, 3),
            make_record("s5", "get_callers", 5, 2),
        ];
        let must_find: FxHashSet<String> = ["planning_pack".to_owned()].into_iter().collect();
        let metrics = aggregate_session(&records, &must_find).unwrap();
        assert_eq!(metrics.time_to_first_correct_anchor_ms, None);
    }

    #[test]
    fn time_to_first_correct_anchor_requires_nonzero_result() {
        // planning_pack is called but returns zero results; should not count.
        let records = vec![
            make_record("s6", "planning_pack", 10, 0), // zero result — skip
            make_record("s6", "planning_pack", 15, 2), // non-zero — count this one
        ];
        let must_find: FxHashSet<String> = ["planning_pack".to_owned()].into_iter().collect();
        let metrics = aggregate_session(&records, &must_find).unwrap();
        // Cumulative = 10 + 15 = 25ms
        assert_eq!(metrics.time_to_first_correct_anchor_ms, Some(25));
    }

    // ─── Aggregate across sessions ────────────────────────────────────────────

    #[test]
    fn aggregate_report_computes_median_wrong_path_rate() {
        // Session A: rate 0.0, Session B: rate 1.0, Session C: rate 0.5
        // Sorted: [0.0, 0.5, 1.0] → median = 0.5
        let mut sessions: BTreeMap<String, Vec<ToolCallRecord>> = BTreeMap::new();
        sessions.insert(
            "a".to_owned(),
            vec![make_record("a", "search", 10, 5)], // 0.0
        );
        sessions.insert(
            "b".to_owned(),
            vec![make_record("b", "search", 10, 0)], // 1.0
        );
        sessions.insert(
            "c".to_owned(),
            vec![
                make_record("c", "search", 10, 1), // non-zero
                make_record("c", "search", 5, 0),  // zero
            ], // 0.5
        );

        let report = aggregate_report(&sessions, &FxHashSet::default(), 3);
        assert_eq!(report.session_count, 3);
        assert!((report.median_wrong_path_rate - 0.5).abs() < 1e-9);
    }

    // ─── Modal sequences ──────────────────────────────────────────────────────

    #[test]
    fn top_modal_sequences_returns_most_common_pattern() {
        // Two sessions with [search, planning_pack], one with [search]
        let sessions = vec![
            SessionMetrics {
                session_id: "a".to_owned(),
                call_count: 2,
                zero_result_count: 0,
                wrong_path_rate: 0.0,
                time_to_first_correct_anchor_ms: None,
                call_sequence_pattern: vec!["search".to_owned(), "planning_pack".to_owned()],
            },
            SessionMetrics {
                session_id: "b".to_owned(),
                call_count: 2,
                zero_result_count: 0,
                wrong_path_rate: 0.0,
                time_to_first_correct_anchor_ms: None,
                call_sequence_pattern: vec!["search".to_owned(), "planning_pack".to_owned()],
            },
            SessionMetrics {
                session_id: "c".to_owned(),
                call_count: 1,
                zero_result_count: 0,
                wrong_path_rate: 0.0,
                time_to_first_correct_anchor_ms: None,
                call_sequence_pattern: vec!["search".to_owned()],
            },
        ];
        let seqs = top_modal_call_sequences(&sessions, 2);
        assert_eq!(seqs[0].count, 2);
        assert_eq!(
            seqs[0].sequence,
            vec!["search".to_owned(), "planning_pack".to_owned()]
        );
    }

    // ─── JSONL parse + group ──────────────────────────────────────────────────

    #[test]
    fn parse_and_group_trace_file() {
        use std::io::Write;

        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        let r1 = ToolCallRecord::new("sess-a", "search", "q=foo".to_owned(), 10, 3, None);
        let r2 = ToolCallRecord::new("sess-b", "planning_pack", "t=bar".to_owned(), 20, 1, None);
        writeln!(tmp, "{}", serde_json::to_string(&r1).unwrap()).unwrap();
        writeln!(tmp, "{}", serde_json::to_string(&r2).unwrap()).unwrap();

        let records = parse_trace_file(tmp.path()).unwrap();
        assert_eq!(records.len(), 2);

        let by_session = group_by_session(records);
        assert_eq!(by_session.len(), 2);
        assert!(by_session.contains_key("sess-a"));
        assert!(by_session.contains_key("sess-b"));
    }

    #[test]
    fn parse_trace_file_skips_malformed_lines() {
        use std::io::Write;

        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        let r = ToolCallRecord::new("sess-c", "search", "q=ok".to_owned(), 5, 2, None);
        writeln!(tmp, "{}", serde_json::to_string(&r).unwrap()).unwrap();
        writeln!(tmp, "not valid json {{{{{{").unwrap();

        let records = parse_trace_file(tmp.path()).unwrap();
        // Only the valid line should be parsed
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].session_id, "sess-c");
    }
}
