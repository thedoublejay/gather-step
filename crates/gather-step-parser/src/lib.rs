#![forbid(unsafe_code)]

pub mod frameworks;
pub mod manifests;
pub(crate) mod path_guard;
pub mod payload;
pub(crate) mod projection;
pub mod resolve;
pub mod traverse;
pub mod tree_sitter;
pub(crate) mod ts_js_backend;
pub(crate) mod ts_js_swc;
pub mod tsconfig;
pub mod workspace_manifest;

#[cfg(feature = "test-support")]
pub use ts_js_swc::swc_test_support;

pub use manifests::{
    ManifestDependency, ManifestError, ManifestExtraction, ParsedPackageManifest, VersionMismatch,
    detect_version_mismatches, extract_package_manifest, parse_package_manifest_str,
};
pub use payload::{InferredPayloadContract, infer_payload_contracts};
pub use resolve::{
    CallSite, CallTargetCandidate, ImportBinding, ResolutionInput, ResolutionOutcome,
    ResolutionStrategy, ResolvedCall, resolve_calls, resolve_calls_with_unresolved,
};
pub use traverse::{
    FileEntry, FileStat, Language, TraversalSummary, TraverseConfig, TraverseError,
    classify_language, collect_repo_files, collect_selected_repo_files,
};
pub use tree_sitter::{
    ParseError, ParsedFile, SymbolCapture, parse_file, parse_file_with_context,
    parse_file_with_frameworks, parse_file_with_packs,
};
