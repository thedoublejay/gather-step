//! Pre-built DFA-backed regex matchers.
//!
//! `regex-automata`'s `meta::Regex` is the high-level engine that selects a
//! DFA, lazy DFA, or NFA backend per-pattern. Building these once at startup
//! and reusing them for the lifetime of the process avoids per-call build
//! overhead that the `regex` crate's API also amortizes — but going through
//! `regex_automata` directly opens the door to lower-level engine selection
//! (`dfa::regex::Regex` for guaranteed-DFA execution, `hybrid::regex::Regex`
//! for memory-bounded lazy DFAs) when a future hot path warrants it.
//!
//! This module currently exposes one matcher to demonstrate the integration
//! and seed the migration path; additional patterns can adopt the same shape
//! as they are profiled into the hot set.
//!
//! Returning `None` on a failed search keeps the public API ergonomic — the
//! underlying `regex_automata` errors are infallible for these compile-time
//! patterns, so call sites do not need to thread a Result.

use std::sync::LazyLock;

use regex_automata::meta::Regex as MetaRegex;

/// Matches a TypeScript / JavaScript dotted access expression and captures
/// the trailing identifier, e.g. `user?.name` → `name`.  Used by projection
/// extraction; high-volume on TS sources.
static DOTTED_ACCESS_DFA: LazyLock<MetaRegex> = LazyLock::new(|| {
    MetaRegex::new(r"(?:this|[A-Za-z_$][A-Za-z0-9_$]*)\??\.([A-Za-z_$][A-Za-z0-9_$]*)")
        .expect("dotted-access DFA pattern must compile")
});

/// Return the captured identifier slices that follow a dotted-access prefix.
///
/// Allocation-free: each returned `&str` borrows from `haystack`. The caller
/// drives the iterator and decides whether to collect.
pub(crate) fn iter_dotted_access_captures(haystack: &str) -> impl Iterator<Item = &str> {
    DOTTED_ACCESS_DFA
        .captures_iter(haystack)
        .filter_map(move |captures| {
            let span = captures.get_group(1)?;
            haystack.get(span.start..span.end)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dotted_access_captures_trailing_identifier() {
        let captured: Vec<&str> = iter_dotted_access_captures("user.name; this?.email").collect();
        assert_eq!(captured, vec!["name", "email"]);
    }

    #[test]
    fn dotted_access_handles_no_match() {
        let captured: Vec<&str> = iter_dotted_access_captures("plain text").collect();
        assert!(captured.is_empty());
    }

    #[test]
    fn dotted_access_skips_invalid_prefixes() {
        // No dotted access here — leading numeric is not a valid prefix.
        let captured: Vec<&str> = iter_dotted_access_captures("1.foo").collect();
        assert!(captured.is_empty());
    }
}
