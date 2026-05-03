//! Backend boundary for TypeScript/JavaScript parsing.
//!
//! Oxc migration work should stay behind this module so Oxc AST types do not
//! leak into the parser crate's public surfaces or framework augmenters.

use std::path::Path;

use crate::{
    traverse::FileEntry,
    tree_sitter::ParseState,
    ts_js_swc::{self, SwcParseStatus},
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

impl From<SwcParseStatus> for TsJsParseStatus {
    fn from(status: SwcParseStatus) -> Self {
        match status {
            SwcParseStatus::Parsed => Self::Parsed,
            SwcParseStatus::Recovered => Self::Recovered,
            SwcParseStatus::Unrecoverable => Self::Unrecoverable,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TsJsParserBackend {
    Swc,
}

impl TsJsParserBackend {
    #[must_use]
    pub(crate) const fn current() -> Self {
        Self::Swc
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
        TsJsParserBackend::Swc => {
            ts_js_swc::parse_ts_js_with_swc_with_status(file, state, source, absolute_path).into()
        }
    }
}
