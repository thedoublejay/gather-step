//! Backend boundary for TypeScript/JavaScript parsing.
//!
//! v3.1 ships Oxc as the default and only production backend for TS/JS
//! sources. Oxc AST types stay inside `ts_js_oxc` so they cannot leak into
//! the parser crate's public surfaces or framework augmenters.

use std::path::Path;

use crate::{
    traverse::FileEntry,
    tree_sitter::ParseState,
    ts_js_oxc::{self, OxcParseStatus},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TsJsParseStatus {
    Parsed,
    Recovered,
    Unrecoverable,
}

impl TsJsParseStatus {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Parsed => "parsed",
            Self::Recovered => "recovered",
            Self::Unrecoverable => "unrecoverable",
        }
    }
}

impl From<OxcParseStatus> for TsJsParseStatus {
    fn from(status: OxcParseStatus) -> Self {
        match status {
            OxcParseStatus::Parsed => Self::Parsed,
            OxcParseStatus::Recovered => Self::Recovered,
            OxcParseStatus::Unrecoverable => Self::Unrecoverable,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TsJsParserBackend {
    Oxc,
}

impl TsJsParserBackend {
    #[must_use]
    pub(crate) const fn current() -> Self {
        Self::Oxc
    }
}

pub(crate) fn parse_ts_js_with_backend(
    backend: TsJsParserBackend,
    file: &FileEntry,
    state: &mut ParseState<'_>,
    source: &str,
    absolute_path: &Path,
) -> TsJsParseStatus {
    match backend {
        TsJsParserBackend::Oxc => {
            ts_js_oxc::parse_ts_js_with_oxc_with_status(file, state, source, absolute_path).into()
        }
    }
}
