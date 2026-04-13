//! Pure functions for extracting structured signals from a raw git commit
//! message. Kept separate from `history.rs` so the indexer wiring and the
//! parsing logic can be tested independently and re-used elsewhere
//! (e.g. when ingesting PR titles).

use std::sync::OnceLock;

use regex::Regex;

/// Conventional-commit and project-style commit type prefixes. Extend here when
/// the codebase consistently uses a new prefix; downstream classification
/// callers treat anything outside this list as `None`.
///
/// Order is irrelevant to matching; the list covers the standard
/// Conventional Commits set.
const COMMIT_TYPE_PREFIXES: &[&str] = &[
    "feat", "fix", "refactor", "chore", "docs", "test", "build", "ci", "perf", "style", "revert",
];

/// Default heuristic tokens that suggest a commit message records a design
/// decision rather than a routine change. Matched case-insensitively as
/// substrings on the full message body. Kept intentionally short — adding
/// noisy phrases here would dilute the signal and reduce its usefulness for
/// the architecture/overview tools that read it.
pub const DEFAULT_DECISION_SIGNALS: &[&str] = &[
    "because",
    "decided",
    "trade-off",
    "tradeoff",
    "rationale",
    "we chose",
];

/// Recognises the leading `type[(scope)|[scope]][!]:` portion of a commit
/// subject. Both `feat(scope): …` (Conventional Commits) and the bracketed
/// `feat[TICKET-123]: …` ticket-tag variant are accepted; the `!` marker is
/// optional and indicates a breaking change.
fn commit_type_regex() -> &'static Regex {
    static CELL: OnceLock<Regex> = OnceLock::new();
    CELL.get_or_init(|| {
        let alternation = COMMIT_TYPE_PREFIXES.join("|");
        // (?i) makes the type token case-insensitive; the `(?:…)` non-capturing
        // group lets us accept a parenthesised scope OR a bracketed scope OR
        // nothing at all.
        let pattern = format!(r"(?ix)^\s*({alternation})(?:\([^)]*\)|\[[^\]]*\])?!?:\s+");
        Regex::new(&pattern).expect("commit_type_regex pattern must compile")
    })
}

/// Recognises a GitHub-style "Merge pull request #N from …" merge subject.
/// Anchored at the start of the message after optional whitespace.
fn merge_pr_regex() -> &'static Regex {
    static CELL: OnceLock<Regex> = OnceLock::new();
    CELL.get_or_init(|| {
        Regex::new(r"(?i)^\s*Merge\s+pull\s+request\s+#(\d+)\b")
            .expect("merge_pr_regex pattern must compile")
    })
}

/// Recognises the `(#N)` suffix that GitHub appends on squash-merge subjects.
/// Searched anywhere in the message so messages with body lines (like a
/// `Co-Authored-By` trailer) still match against the subject occurrence.
fn squash_pr_regex() -> &'static Regex {
    static CELL: OnceLock<Regex> = OnceLock::new();
    CELL.get_or_init(|| Regex::new(r"\(#(\d+)\)").expect("squash_pr_regex pattern must compile"))
}

/// Returns the conventional-commits type prefix for a message, or `None` if
/// the subject does not start with a recognised type.
///
/// The returned string is one of the entries in [`COMMIT_TYPE_PREFIXES`] and
/// is always lower-case, regardless of the casing in the source message.
///
/// # Examples
///
/// ```
/// use gather_step_git::classify_commit_message;
/// assert_eq!(classify_commit_message("feat: add x"), Some("feat"));
/// assert_eq!(classify_commit_message("fix(auth): null"), Some("fix"));
/// assert_eq!(classify_commit_message("fix[TICKET-1]: null"), Some("fix"));
/// assert_eq!(classify_commit_message("FEAT!: bang"), Some("feat"));
/// assert_eq!(classify_commit_message("random update"), None);
/// ```
#[must_use]
pub fn classify_commit_message(message: &str) -> Option<&'static str> {
    // Match against the first line only; `^…` would otherwise incorrectly fire
    // on body lines that happen to begin with `feat:` etc.
    let subject = message.lines().next().unwrap_or("");
    let captures = commit_type_regex().captures(subject)?;
    let raw = captures.get(1)?.as_str();
    // Re-anchor on the static slice so callers receive a `&'static str` rather
    // than an owned `String`. `eq_ignore_ascii_case` avoids allocating a
    // lowercase copy of `raw` just to compare it against ASCII prefixes.
    COMMIT_TYPE_PREFIXES
        .iter()
        .copied()
        .find(|candidate| candidate.eq_ignore_ascii_case(raw))
}

/// Extracts the merge-PR number from a commit message, preferring the explicit
/// `Merge pull request #N` form (which is unambiguous) over `(#N)` (which can
/// appear in commit bodies that mention unrelated PRs).
///
/// # Examples
///
/// ```
/// use gather_step_git::extract_pr_number;
/// assert_eq!(
///     extract_pr_number("Merge pull request #42 from user/branch"),
///     Some(42),
/// );
/// assert_eq!(extract_pr_number("Add user auth (#123)"), Some(123));
/// assert_eq!(extract_pr_number("plain commit"), None);
/// ```
#[must_use]
pub fn extract_pr_number(message: &str) -> Option<u64> {
    if let Some(captures) = merge_pr_regex().captures(message)
        && let Some(value) = captures.get(1).and_then(|m| m.as_str().parse().ok())
    {
        return Some(value);
    }
    // For the squash-merge `(#N)` form prefer the **subject** match: bodies
    // sometimes reference older PRs as context (`see #99`), and we only want to
    // attribute the commit to the PR it merged for.
    let subject = message.lines().next().unwrap_or("");
    squash_pr_regex()
        .captures(subject)
        .and_then(|captures| captures.get(1))
        .and_then(|m| m.as_str().parse().ok())
}

/// Returns `true` when any of the supplied `signals` appears (case-insensitively)
/// as a substring of `message`. Use [`DEFAULT_DECISION_SIGNALS`] for the
/// canonical list, or pass a project-tuned slice to suppress false positives.
///
/// The check is intentionally a substring — bounded-word matching would miss
/// project-specific phrasing like "we re-chose" or compound forms like
/// "tradeoff-driven", and the downstream consumer (architecture/overview
/// tools) treats this as a heuristic prompt for a human or LLM to read the
/// rationale, not as a load-bearing classification.
///
/// # Examples
///
/// ```
/// use gather_step_git::{detect_decision_signal, DEFAULT_DECISION_SIGNALS};
/// assert!(detect_decision_signal(
///     "Switch to async because the sync API blocks under load",
///     DEFAULT_DECISION_SIGNALS,
/// ));
/// assert!(!detect_decision_signal("update lockfile", DEFAULT_DECISION_SIGNALS));
/// ```
#[must_use]
pub fn detect_decision_signal(message: &str, signals: &[&str]) -> bool {
    if signals.is_empty() {
        return false;
    }
    let mut haystack = message.to_owned();
    haystack.make_ascii_lowercase();
    signals.iter().any(|signal| {
        let mut needle = (*signal).to_owned();
        needle.make_ascii_lowercase();
        haystack.contains(&needle)
    })
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::{
        DEFAULT_DECISION_SIGNALS, classify_commit_message, detect_decision_signal,
        extract_pr_number,
    };

    #[test]
    fn classify_recognises_conventional_commit_prefixes() {
        let cases = [
            ("feat: add user auth", Some("feat")),
            ("fix: resolve crash on startup", Some("fix")),
            ("refactor: simplify event router", Some("refactor")),
            ("chore: bump deps", Some("chore")),
            ("docs: clarify README", Some("docs")),
            ("test: add fixture", Some("test")),
            ("build: tweak Dockerfile", Some("build")),
            ("ci: pin runner version", Some("ci")),
            ("perf: precompute lookup", Some("perf")),
            ("style: format imports", Some("style")),
            ("revert: roll back v1.2", Some("revert")),
        ];
        for (message, expected) in cases {
            assert_eq!(
                classify_commit_message(message),
                expected,
                "message: {message}"
            );
        }
    }

    #[test]
    fn classify_accepts_scope_and_breaking_marker() {
        assert_eq!(
            classify_commit_message("fix(auth): null check"),
            Some("fix")
        );
        assert_eq!(
            classify_commit_message("feat(api)!: drop /v1"),
            Some("feat")
        );
        // Bracketed `type[TICKET-XXXX]:` scope variant.
        assert_eq!(classify_commit_message("fix[TICKET-1234]: x"), Some("fix"));
        assert_eq!(classify_commit_message("feat[TICKET-1]!: y"), Some("feat"));
    }

    #[test]
    fn classify_is_case_insensitive_on_the_type_token() {
        assert_eq!(classify_commit_message("FEAT: shouty"), Some("feat"));
        assert_eq!(classify_commit_message("Fix: titled"), Some("fix"));
    }

    #[test]
    fn classify_rejects_messages_without_a_recognised_prefix() {
        let rejected = [
            "random commit",
            // `feature` is a near-miss; only the canonical short forms count.
            "feature: not a real type",
            // Missing colon.
            "fix add user auth",
            // Colon present but no space after.
            "fix:nospace",
            // Empty body.
            "",
        ];
        for message in rejected {
            assert_eq!(
                classify_commit_message(message),
                None,
                "message should not classify: {message:?}"
            );
        }
    }

    #[test]
    fn classify_only_inspects_the_subject_line() {
        // A body line that *looks* like a conventional commit prefix must not
        // count when the subject does not.
        let message = "miscellaneous tweaks\n\nfix: this is a body line";
        assert_eq!(classify_commit_message(message), None);
    }

    #[test]
    fn extract_pr_number_prefers_explicit_merge_form() {
        let message = "Merge pull request #42 from feature/x\n\nfeat: add y (#99)";
        // Even though `(#99)` appears in the body, the explicit merge subject
        // attribution wins so the commit is mapped to the PR it merged.
        assert_eq!(extract_pr_number(message), Some(42));
    }

    #[test]
    fn extract_pr_number_handles_squash_merge_form() {
        assert_eq!(extract_pr_number("Add user auth (#123)"), Some(123));
        assert_eq!(extract_pr_number("Tweak (#1)"), Some(1));
    }

    #[test]
    fn extract_pr_number_ignores_body_only_squash_references() {
        // The subject does not contain the PR reference; we deliberately do
        // not attribute the commit to a body-only mention to avoid spurious
        // ownership of unrelated PRs.
        let message = "Routine tweak\n\nFollow-up to (#99)";
        assert_eq!(extract_pr_number(message), None);
    }

    #[test]
    fn extract_pr_number_returns_none_when_no_number_present() {
        let cases = ["plain commit", "(#)", "merge pull request from user", ""];
        for message in cases {
            assert_eq!(
                extract_pr_number(message),
                None,
                "expected no PR number in: {message:?}"
            );
        }
    }

    #[test]
    fn detect_decision_signal_matches_any_token_case_insensitively() {
        assert!(detect_decision_signal(
            "Switch to async because the sync API blocks under load",
            DEFAULT_DECISION_SIGNALS,
        ));
        assert!(detect_decision_signal(
            "We Decided to drop v1 support",
            DEFAULT_DECISION_SIGNALS,
        ));
        assert!(detect_decision_signal(
            "Trade-off: latency for throughput",
            DEFAULT_DECISION_SIGNALS,
        ));
        // The hyphen-free form is recognised as well, mirroring how teams
        // alternate between "trade-off" and "tradeoff" in practice.
        assert!(detect_decision_signal(
            "tradeoff-driven choice",
            DEFAULT_DECISION_SIGNALS,
        ));
    }

    #[test]
    fn detect_decision_signal_returns_false_for_neutral_messages() {
        assert!(!detect_decision_signal(
            "update lockfile",
            DEFAULT_DECISION_SIGNALS
        ));
        assert!(!detect_decision_signal(
            "bump dependency",
            DEFAULT_DECISION_SIGNALS
        ));
    }

    #[test]
    fn detect_decision_signal_returns_false_when_signal_list_is_empty() {
        // A caller passing an empty signal list intentionally suppresses the
        // heuristic; the function must respect that rather than always
        // returning false-by-coincidence.
        assert!(!detect_decision_signal("we decided to refactor", &[]));
    }

    #[test]
    fn detect_decision_signal_supports_caller_supplied_tokens() {
        // A team that wants to enforce a custom decision-marker convention
        // (e.g. `[ADR]`) can supply their own signal list and the heuristic
        // honours it.
        let signals = ["[adr]", "RFC:"];
        assert!(detect_decision_signal("[ADR] choose redb 4.0", &signals));
        assert!(detect_decision_signal("RFC: tighten payloads", &signals));
        assert!(!detect_decision_signal("we decided to refactor", &signals));
    }
}
