//! Markdown sanitization utilities for `context_md` render surfaces.
//!
//! User-controlled content (repo names, symbol names, file paths) flows into
//! three distinct markdown surfaces: table cells, inline code spans, and fenced
//! code blocks. Each surface has its own injection risk:
//!
//! - **Table cells** — a bare `|` is interpreted by `CommonMark` renderers as a
//!   column separator, breaking table layout and potentially misguiding
//!   downstream LLM interpretation.
//! - **Inline code spans** — a backtick run of length ≥ N can close a span
//!   delimited by N backticks, allowing embedded markup to escape and render.
//! - **Fenced code blocks** — a line beginning with a backtick run of length
//!   ≥ N closes a fence opened with N backticks, leaking content outside the
//!   block.
//!
//! This module provides one function per surface. Apply them at every render
//! site where content originates from user-controlled repository data.

/// Escape `|` and newline characters so `content` is safe to embed in a single
/// markdown table cell.
///
/// # Rules enforced
///
/// - **`CommonMark` §4.10 (pipe tables, GFM extension):** a bare `|` inside a
///   cell terminates the current column. Escaping as `\|` prevents this.
/// - **Newlines** are replaced with `<br>` (HTML line-break). `CommonMark`
///   renderers and the LLM-facing planning surfaces render `<br>` as a soft
///   break within a cell, which is far safer than a raw newline that would
///   start a new table row or paragraph.
/// - **Backslash-before-pipe** is doubled (`\\|` → `\\\|`) so that pre-existing
///   escaped pipes survive the transformation without being double-interpreted.
///
/// # Example
///
/// ```
/// use gather_step_output::sanitize::sanitize_table_cell;
///
/// let safe = sanitize_table_cell("repo|injected");
/// assert_eq!(safe, r"repo\|injected");
///
/// let safe = sanitize_table_cell("line1\nline2");
/// assert_eq!(safe, "line1<br>line2");
/// ```
pub fn sanitize_table_cell(content: &str) -> String {
    // Pre-size: content length plus some headroom for escapes.
    let mut out = String::with_capacity(content.len() + 8);
    let chars: Vec<char> = content.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '\n' => out.push_str("<br>"),
            '\r' => {
                // Collapse \r\n into a single <br>.
                if chars.get(i + 1) == Some(&'\n') {
                    i += 1;
                }
                out.push_str("<br>");
            }
            '\\' if chars.get(i + 1) == Some(&'|') => {
                // Existing escaped pipe: double the backslash so the downstream
                // renderer still sees `\|` and does not misinterpret the pipe.
                out.push_str("\\\\|");
                i += 1; // skip the `|` too — we just emitted it
            }
            '|' => out.push_str("\\|"),
            c => out.push(c),
        }
        i += 1;
    }
    out
}

/// Wrap `content` as an inline code span with a backtick delimiter run that is
/// strictly longer than any run of backticks inside `content`.
///
/// # Rules enforced
///
/// **`CommonMark` §6.1 (code spans):** an inline code span opened with a run of
/// *N* backticks can only be closed by a run of exactly *N* backticks that is
/// not part of a longer run. Choosing *N* = `max_backtick_run(content) + 1`
/// guarantees the content can never close the span early.
///
/// # Empty content
///
/// Returns `` ` ` `` (a space-delimited single-backtick pair) — the `CommonMark`
/// canonical form for an empty inline code span.
///
/// # Example
///
/// ```
/// use gather_step_output::sanitize::wrap_inline_code;
///
/// // Ordinary content.
/// assert_eq!(wrap_inline_code("hello"), "`hello`");
///
/// // Content containing a single backtick requires a two-backtick delimiter.
/// let wrapped = wrap_inline_code("foo` bar");
/// assert!(wrapped.starts_with("``"), "outer delimiter too short: {wrapped}");
/// assert!(wrapped.contains("foo` bar"), "content missing: {wrapped}");
///
/// // Empty content.
/// assert_eq!(wrap_inline_code(""), "` `");
/// ```
pub fn wrap_inline_code(content: &str) -> String {
    if content.is_empty() {
        // CommonMark canonical empty code span.
        return "` `".to_owned();
    }

    let n = outer_backtick_count(content);
    let fence = "`".repeat(n);

    // CommonMark §6.1: if the content begins or ends with a space (or backtick),
    // one leading/trailing space is stripped when rendering. To avoid accidental
    // stripping we pad with a space when the content starts or ends with a
    // backtick or a space character.
    let needs_padding = content.starts_with('`')
        || content.ends_with('`')
        || content.starts_with(' ')
        || content.ends_with(' ');

    if needs_padding {
        format!("{fence} {content} {fence}")
    } else {
        format!("{fence}{content}{fence}")
    }
}

/// Wrap `content` as a fenced code block whose opening fence is strictly longer
/// than any run of backticks inside `content`.
///
/// # Rules enforced
///
/// **`CommonMark` §4.5 (fenced code blocks):** a closing fence must be a run of
/// backticks of length ≥ the opening fence length. By choosing the opening fence
/// length as `max_backtick_run(content) + 1` (minimum 3, per the spec) the
/// content can never close the block early.
///
/// If `content` does not end with a newline, one is appended before the closing
/// fence so the fence always starts on its own line.
///
/// # Example
///
/// ```
/// use gather_step_output::sanitize::wrap_fenced_code;
///
/// let block = wrap_fenced_code("fn main() {}", "rust");
/// assert!(block.starts_with("```rust\n"), "unexpected opening: {block}");
/// assert!(block.ends_with("\n```\n"), "unexpected closing: {block}");
///
/// // Embedded triple-backtick requires a four-backtick outer fence.
/// let block = wrap_fenced_code("```\nsome content\n```\n", "text");
/// assert!(block.starts_with("````"), "fence not escalated: {block}");
/// ```
pub fn wrap_fenced_code(content: &str, info_string: &str) -> String {
    // CommonMark §4.5: fence must be at least 3 backticks.
    let n = outer_backtick_count(content).max(3);
    let fence = "`".repeat(n);

    let trailing_newline = if content.ends_with('\n') { "" } else { "\n" };

    format!("{fence}{info_string}\n{content}{trailing_newline}{fence}\n")
}

/// Return the minimum backtick delimiter length needed to safely wrap `content`.
///
/// This is `max_consecutive_backtick_run(content) + 1`, with a floor of 1 so
/// that content with no backticks uses a single backtick.
fn outer_backtick_count(content: &str) -> usize {
    let max_run = max_backtick_run(content);
    max_run + 1
}

/// Count the length of the longest consecutive run of backtick (`` ` ``)
/// characters in `content`. Returns 0 if there are none.
fn max_backtick_run(content: &str) -> usize {
    let mut max = 0_usize;
    let mut run = 0_usize;
    for c in content.chars() {
        if c == '`' {
            run += 1;
            if run > max {
                max = run;
            }
        } else {
            run = 0;
        }
    }
    max
}

#[cfg(test)]
mod unit_tests {
    use super::{max_backtick_run, sanitize_table_cell, wrap_fenced_code, wrap_inline_code};

    #[test]
    fn max_run_empty() {
        assert_eq!(max_backtick_run(""), 0);
    }

    #[test]
    fn max_run_single() {
        assert_eq!(max_backtick_run("`"), 1);
    }

    #[test]
    fn max_run_triple() {
        assert_eq!(max_backtick_run("a```b``c"), 3);
    }

    #[test]
    fn table_cell_no_pipe_no_change() {
        assert_eq!(sanitize_table_cell("hello world"), "hello world");
    }

    #[test]
    fn table_cell_pipe_escaped() {
        assert_eq!(sanitize_table_cell("a|b"), r"a\|b");
    }

    #[test]
    fn table_cell_newline_replaced() {
        assert_eq!(sanitize_table_cell("a\nb"), "a<br>b");
    }

    #[test]
    fn table_cell_crlf_single_br() {
        assert_eq!(sanitize_table_cell("a\r\nb"), "a<br>b");
    }

    #[test]
    fn table_cell_existing_escaped_pipe_doubled() {
        // Input: `a\|b` — an already-escaped pipe. Output must double the
        // backslash so the downstream renderer still reads `\|` (escaped pipe).
        let input = "a\\|b";
        let output = sanitize_table_cell(input);
        assert_eq!(output, "a\\\\|b");
    }

    #[test]
    fn inline_code_no_backtick_single_delimiter() {
        assert_eq!(wrap_inline_code("hello"), "`hello`");
    }

    #[test]
    fn inline_code_empty_space_pair() {
        assert_eq!(wrap_inline_code(""), "` `");
    }

    #[test]
    fn inline_code_contains_backtick_double_delimiter() {
        let wrapped = wrap_inline_code("foo`bar");
        assert!(wrapped.starts_with("``"), "{wrapped}");
        assert!(wrapped.ends_with("``"), "{wrapped}");
        assert!(wrapped.contains("foo`bar"), "{wrapped}");
    }

    #[test]
    fn fenced_code_basic() {
        let block = wrap_fenced_code("let x = 1;", "rust");
        assert!(block.starts_with("```rust\n"), "{block}");
        assert!(block.ends_with("\n```\n"), "{block}");
        assert!(block.contains("let x = 1;"), "{block}");
    }

    #[test]
    fn fenced_code_adds_trailing_newline() {
        let block = wrap_fenced_code("no newline", "");
        assert!(block.contains("no newline\n"), "{block}");
    }

    #[test]
    fn fenced_code_escalates_for_triple_backtick() {
        let block = wrap_fenced_code("```\ncontent\n```\n", "text");
        assert!(block.starts_with("````"), "{block}");
    }
}
