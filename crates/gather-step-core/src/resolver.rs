//! Canonical identity for the `EdgeMetadata.resolver` field.
//!
//! This closed enum replaced an ad-hoc set of free-form resolver strings.
//! Producers MUST set `EdgeMetadata.resolver` to the output of
//! [`ResolverStrategy::as_str`] so no new undocumented strategy names creep in
//! between producers and consumers. The field type remains `Option<String>`
//! for on-disk serde compatibility; [`ResolverStrategy::from_str`] parses a
//! stored string back into the enum for ranking and display.
//!
//! `strategy_weight` returns the ordering weight used as the second tuple in
//! the deterministic sort contract
//! `(confidence desc, strategy_weight desc, repo asc, file asc, line asc, qn asc)`.

/// Closed set of known values for `EdgeMetadata.resolver`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ResolverStrategy {
    // --- parser call resolution strategies (mirror gather_step_parser::ResolutionStrategy) ---
    /// Resolved via the file's explicit import map — the strongest structural signal.
    ImportMap,
    /// Resolved against another symbol in the same module.
    SameModule,
    /// Resolved because exactly one symbol across the workspace matched.
    Unique,
    /// Resolved via suffix match against a qualified name.
    Suffix,
    /// Resolved via fuzzy name similarity above a threshold.
    FuzzyName,
    /// Last-resort fallback with low confidence.
    Fallback,
    // --- frontend framework resolvers ---
    /// Frontend caller resolved through an imported constant (e.g. `ROUTES.ORDERS`).
    FrontendConstant,
    /// Frontend caller resolved via a parser-provided hint (framework-specific heuristic).
    FrontendHint,
    /// Frontend caller resolved from a string literal inside the call site.
    FrontendLiteral,
    // --- git analytics heuristics ---
    /// Ownership edge inferred from a file's historical authorship.
    HistoryOwnership,
    /// Co-change edge inferred from commits that touched two files together.
    CoChange,
    // --- storage two-pass fallback ---
    /// First-pass edge emitted during the initial repo index (pre-cross-repo).
    FirstPass,
    /// Second-pass edge emitted after cross-repo resolution.
    SecondPass,
}

impl ResolverStrategy {
    /// Serialize to the canonical wire string used throughout the codebase.
    ///
    /// Hyphenated forms (`first-pass`, `second-pass`) are preserved verbatim for
    /// on-disk compatibility with data written before this enum existed.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ImportMap => "import_map",
            Self::SameModule => "same_module",
            Self::Unique => "unique",
            Self::Suffix => "suffix",
            Self::FuzzyName => "fuzzy_name",
            Self::Fallback => "fallback",
            Self::FrontendConstant => "frontend_constant",
            Self::FrontendHint => "frontend_hint",
            Self::FrontendLiteral => "frontend_literal",
            Self::HistoryOwnership => "history_ownership",
            Self::CoChange => "co_change",
            Self::FirstPass => "first-pass",
            Self::SecondPass => "second-pass",
        }
    }

    /// Parse a stored resolver string back into the enum.
    ///
    /// Returns `None` for unknown strings. Any `None` return from a field that
    /// was non-empty indicates silent drift between a producer and this enum
    /// and should be treated as a bug.
    #[must_use]
    // We deliberately do NOT implement `std::str::FromStr` for this enum: the
    // standard trait requires a distinct error type, but all we want here is
    // "is this one of the known wire strings, yes or no". Returning
    // `Option<Self>` matches the caller pattern at every use site.
    #[expect(
        clippy::should_implement_trait,
        reason = "FromStr requires an error type; this helper deliberately returns Option"
    )]
    pub fn from_str(value: &str) -> Option<Self> {
        Some(match value {
            "import_map" => Self::ImportMap,
            "same_module" => Self::SameModule,
            "unique" => Self::Unique,
            "suffix" => Self::Suffix,
            "fuzzy_name" => Self::FuzzyName,
            "fallback" => Self::Fallback,
            "frontend_constant" => Self::FrontendConstant,
            "frontend_hint" => Self::FrontendHint,
            "frontend_literal" => Self::FrontendLiteral,
            "history_ownership" => Self::HistoryOwnership,
            "co_change" => Self::CoChange,
            "first-pass" => Self::FirstPass,
            "second-pass" => Self::SecondPass,
            _ => return None,
        })
    }

    /// Deterministic ranking weight used in the locked sort tuple.
    ///
    /// Higher weight means the resolver is considered more reliable. Ties are
    /// broken by the remaining components of the sort tuple
    /// (`repo asc, file asc, line asc, qn asc`).
    #[must_use]
    pub const fn strategy_weight(self) -> u16 {
        match self {
            Self::ImportMap => 100,
            Self::SameModule => 90,
            Self::FrontendConstant => 80,
            Self::Unique => 70,
            Self::FrontendLiteral => 65,
            Self::FrontendHint => 60,
            Self::HistoryOwnership => 50,
            Self::CoChange => 45,
            Self::Suffix => 40,
            Self::FuzzyName => 30,
            Self::FirstPass => 25,
            Self::SecondPass => 20,
            Self::Fallback => 10,
        }
    }
}

/// Convenience helper for producers that already have an `Option<&str>` in hand.
#[must_use]
pub fn strategy_weight(resolver: Option<&str>) -> u16 {
    resolver
        .and_then(ResolverStrategy::from_str)
        .map_or(0, ResolverStrategy::strategy_weight)
}

#[cfg(test)]
mod tests {
    use super::ResolverStrategy;

    #[test]
    fn as_str_round_trips_through_from_str() {
        let cases = [
            ResolverStrategy::ImportMap,
            ResolverStrategy::SameModule,
            ResolverStrategy::Unique,
            ResolverStrategy::Suffix,
            ResolverStrategy::FuzzyName,
            ResolverStrategy::Fallback,
            ResolverStrategy::FrontendConstant,
            ResolverStrategy::FrontendHint,
            ResolverStrategy::FrontendLiteral,
            ResolverStrategy::HistoryOwnership,
            ResolverStrategy::CoChange,
            ResolverStrategy::FirstPass,
            ResolverStrategy::SecondPass,
        ];
        for case in cases {
            assert_eq!(ResolverStrategy::from_str(case.as_str()), Some(case));
        }
    }

    #[test]
    fn preserves_hyphenated_storage_forms() {
        // These two forms MUST stay hyphenated for on-disk compatibility with
        // edges written before ResolverStrategy existed.
        assert_eq!(ResolverStrategy::FirstPass.as_str(), "first-pass");
        assert_eq!(ResolverStrategy::SecondPass.as_str(), "second-pass");
    }

    #[test]
    fn from_str_returns_none_for_unknown_values() {
        assert_eq!(ResolverStrategy::from_str(""), None);
        assert_eq!(ResolverStrategy::from_str("IMPORT_MAP"), None); // case-sensitive
        assert_eq!(ResolverStrategy::from_str("unknown_strategy"), None);
    }

    #[test]
    fn strategy_weight_orders_import_map_above_fallback() {
        assert!(
            ResolverStrategy::ImportMap.strategy_weight()
                > ResolverStrategy::Fallback.strategy_weight()
        );
        assert!(
            ResolverStrategy::SameModule.strategy_weight()
                > ResolverStrategy::FuzzyName.strategy_weight()
        );
    }

    #[test]
    fn strategy_weight_for_unknown_string_is_zero() {
        assert_eq!(super::strategy_weight(None), 0);
        assert_eq!(super::strategy_weight(Some("totally_unknown")), 0);
    }
}
