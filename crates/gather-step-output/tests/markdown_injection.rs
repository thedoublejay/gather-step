use gather_step_output::sanitize::{sanitize_table_cell, wrap_fenced_code, wrap_inline_code};

#[test]
fn markdown_table_cell_escapes_pipe_injection() {
    let rendered = sanitize_table_cell("repo|injected|row");
    // Bare pipes must be escaped so the markdown renderer does not
    // interpret them as column separators.
    assert!(
        !rendered.contains('|') || rendered.contains("\\|"),
        "bare pipes must be escaped inside markdown cells; got {rendered:?}"
    );
    // The round-trip is deterministic: escaped output still contains the
    // original segments joined by the escape.
    assert!(rendered.contains("repo"));
    assert!(rendered.contains("injected"));
    assert!(rendered.contains("row"));
}

#[test]
fn markdown_table_cell_escapes_newlines() {
    let rendered = sanitize_table_cell("multi\nline\ncell");
    assert!(
        !rendered.contains('\n'),
        "newlines must not survive into a cell"
    );
    assert!(
        rendered.contains("<br>") || rendered.contains("\\n"),
        "newlines must be replaced, got {rendered:?}"
    );
}

#[test]
fn markdown_inline_code_cannot_be_broken_out_of() {
    let rendered = wrap_inline_code("foo` injected **bold** `bar");
    // The outer delimiter must be longer than any backtick run inside.
    // Simplest sufficient condition: outer is at least `` `` `` (two).
    assert!(
        rendered.starts_with("``") && rendered.ends_with("``"),
        "outer delimiter must be at least 2 backticks; got {rendered:?}"
    );
    // The content appears verbatim inside.
    assert!(rendered.contains("foo` injected **bold** `bar"));
}

#[test]
fn markdown_inline_code_empty_content_uses_space_pair() {
    let rendered = wrap_inline_code("");
    // Per CommonMark, an inline code span with exactly one inner space
    // is rendered as an empty code span; use `` ` ` `` for consistency.
    assert!(
        rendered == "` `" || rendered == "``  ``",
        "empty content should still produce a valid code span, got {rendered:?}"
    );
}

#[test]
fn markdown_fence_escalates_backtick_count_for_embedded_triple_backticks() {
    let rendered = wrap_fenced_code("```rust\nfn main() {}\n```\n", "text");
    assert!(
        rendered.starts_with("````"),
        "outer fence must be longer than inner run; got {rendered:?}"
    );
    // Content is preserved.
    assert!(rendered.contains("fn main()"));
}

#[test]
fn markdown_fence_adds_trailing_newline_before_closing_fence_if_missing() {
    let rendered = wrap_fenced_code("one line without newline", "rust");
    assert!(rendered.contains("one line without newline\n"));
}
