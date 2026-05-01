use std::{
    cell::RefCell,
    fs,
    num::NonZeroUsize,
    ops::ControlFlow,
    path::{Component, Path, PathBuf},
    time::{Duration, Instant},
};

use camino::Utf8PathBuf;

use gather_step_core::{
    EdgeData, EdgeKind, EdgeMetadata, GatherStepConfig, NodeData, NodeKind, SourceSpan, Visibility,
    node_id, ref_node_id, shared_package_root, shared_symbol_qn_unversioned, virtual_node,
};
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;
use thiserror::Error;
use tree_sitter::{Language as TsLanguage, Node, Parser};

use crate::{
    frameworks::{
        Framework,
        registry::{PackId, PackRegistry},
    },
    path_guard::{
        canonicalize_existing_dir_under, canonicalize_existing_file_under,
        canonicalize_existing_file_under_any,
    },
    resolve::ImportBinding,
    traverse::{FileEntry, Language},
    tsconfig::PathAliases,
    workspace_manifest::find_workspace_root,
};

/// Captured data for a single decorator occurrence.
///
/// - `raw`: the argument expression only — the content inside the outermost
///   `(…)` of the decorator call, trimmed.  For `@EventPattern('order.placed')`
///   this is `'order.placed'`.  For decorators without arguments this is an
///   empty string.
/// - `arguments`: the individual comma-separated arguments, each stripped of
///   surrounding quotes.  Most decorators carry 0–1 args, so a two-element
///   inline buffer avoids heap allocation in the common case.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecoratorCapture {
    pub name: String,
    pub arguments: SmallVec<[Box<str>; 2]>,
    pub raw: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SymbolCapture {
    pub node: NodeData,
    pub file_node: gather_step_core::NodeId,
    pub parent_class: Option<String>,
    pub decorators: Vec<DecoratorCapture>,
    pub class_decorators: Vec<DecoratorCapture>,
    pub constructor_dependencies: Vec<String>,
    pub implemented_interfaces: Vec<String>,
    pub base_classes: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedFile {
    pub file: FileEntry,
    pub source_path: PathBuf,
    /// UTF-8 source text decoded at parse time.  Invalid byte sequences are
    /// replaced with U+FFFD (lossy fallback) so indexing continues rather than
    /// aborting the entire repo.  Shared via [`Arc`] so framework augmenters
    /// can clone the reference without copying the string body.
    pub source: std::sync::Arc<str>,
    pub file_node: NodeData,
    pub nodes: Vec<NodeData>,
    pub edges: Vec<EdgeData>,
    pub symbols: Vec<SymbolCapture>,
    pub call_sites: Vec<EnrichedCallSite>,
    pub import_bindings: Vec<ImportBinding>,
    pub constant_strings: FxHashMap<String, String>,
    pub parse_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnrichedCallSite {
    pub owner_id: gather_step_core::NodeId,
    pub owner_file: gather_step_core::NodeId,
    pub source_path: PathBuf,
    pub callee_name: String,
    pub callee_qualified_hint: Option<String>,
    pub literal_argument: Option<String>,
    pub raw_arguments: Option<String>,
    pub span: Option<SourceSpan>,
}

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("failed to read source file {path}: {source}")]
    ReadFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to configure tree-sitter language: {0}")]
    Language(#[from] tree_sitter::LanguageError),
    #[error("tree-sitter produced no parse tree for {path}")]
    MissingTree { path: PathBuf },
    #[error("tree-sitter parsing timed out for {path} after {timeout_ms}ms")]
    Timeout { path: PathBuf, timeout_ms: u64 },
}

const MAX_VISITOR_DEPTH: usize = 256;
const TREE_SITTER_PARSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Process-wide built-in registry — reused across all `parse_file_core` calls.
/// `PackRegistry::builtin()` allocates a 15-entry `Vec<PackEntry>`.
static BUILTIN_REGISTRY: std::sync::OnceLock<PackRegistry> = std::sync::OnceLock::new();

pub fn parse_file(
    repo: &str,
    repo_root: &Path,
    file: &FileEntry,
) -> Result<ParsedFile, ParseError> {
    let aliases = PathAliases::empty();
    parse_file_with_context(repo, repo_root, file, &[], &aliases)
}

/// Like [`parse_file`], but only applies framework-specific extractors listed
/// in `active_frameworks`. The orchestrator detects the frameworks for a repo
/// once and passes them through, so per-file parsing doesn't re-scan
/// manifests for every file.
pub fn parse_file_with_frameworks(
    repo: &str,
    repo_root: &Path,
    file: &FileEntry,
    active_frameworks: &[Framework],
) -> Result<ParsedFile, ParseError> {
    let aliases = PathAliases::empty();
    parse_file_with_context(repo, repo_root, file, active_frameworks, &aliases)
}

/// Primary entry point used by the orchestrator when a local config provides
/// explicit pack overrides: takes both pack selection and repo-level path
/// aliases loaded from `tsconfig.json`.
///
/// This is the lowest-level parsing entry point.  All other public
/// `parse_file_*` functions ultimately delegate here (possibly after
/// converting [`Framework`]s to [`PackId`]s).
pub fn parse_file_with_packs(
    repo: &str,
    repo_root: &Path,
    file: &FileEntry,
    active_packs: &[crate::frameworks::profile::ResolvedPack],
    path_aliases: &PathAliases,
) -> Result<ParsedFile, ParseError> {
    parse_file_core(repo, repo_root, file, active_packs, path_aliases)
}

/// Entry point used by the orchestrator for auto-detected repos: takes
/// framework selection and repo-level path aliases loaded from `tsconfig.json`.
/// The orchestrator detects frameworks and loads path aliases once per repo and
/// passes them into each rayon worker, so per-file parsing never re-parses
/// manifests.
///
/// Internally converts [`Framework`] variants to [`PackId`]s and delegates to
/// [`parse_file_with_packs`].
pub fn parse_file_with_context(
    repo: &str,
    repo_root: &Path,
    file: &FileEntry,
    active_frameworks: &[Framework],
    path_aliases: &PathAliases,
) -> Result<ParsedFile, ParseError> {
    let packs: Vec<crate::frameworks::profile::ResolvedPack> = active_frameworks
        .iter()
        .copied()
        .flat_map(framework_to_pack_ids)
        .copied()
        .map(|id| crate::frameworks::profile::ResolvedPack {
            id,
            options: serde_yaml_ng::Value::Null,
        })
        .collect();
    parse_file_core(repo, repo_root, file, &packs, path_aliases)
}

fn parse_file_core(
    repo: &str,
    repo_root: &Path,
    file: &FileEntry,
    active_packs_arg: &[crate::frameworks::profile::ResolvedPack],
    path_aliases: &PathAliases,
) -> Result<ParsedFile, ParseError> {
    // Clear the path-existence cache once per repo boundary (not per file).
    // Existence checks are stable across files within the same repo root.
    clear_import_path_exists_cache_if_new_repo(repo_root);
    let absolute_path = repo_root.join(&file.path);
    // Use bytes captured at traversal time when available; otherwise fall back
    // to a disk read.  This eliminates the second `fs::read` call for files
    // that have already been read during directory walk.
    let raw_bytes: std::sync::Arc<[u8]> = match file.source_bytes.clone() {
        Some(b) => b,
        None => fs::read(&absolute_path)
            .map_err(|source| ParseError::ReadFile {
                path: absolute_path.clone(),
                source,
            })?
            .into(),
    };
    // Tolerate non-UTF-8 source files (Latin-1, Windows-1252, stray high bytes
    // in copyright headers, etc.).  Invalid byte sequences are replaced with
    // U+FFFD so that indexing continues rather than aborting the entire repo.
    let source: std::sync::Arc<str> = match std::str::from_utf8(&raw_bytes) {
        Ok(s) => s.into(),
        Err(_) => String::from_utf8_lossy(&raw_bytes).as_ref().into(),
    };

    let parse_start = std::time::Instant::now();

    // Diagnostic bypass: when `GATHER_STEP_DIAG_TS=treesitter` is set, force
    // TS/JS files through the tree-sitter path instead of swc. Used for
    // empirical parity comparison against the swc output.
    let force_tree_sitter = std::env::var("GATHER_STEP_DIAG_TS")
        .map(|v| v == "treesitter")
        .unwrap_or(false);

    // ── TS/JS: use swc (unless the diagnostic bypass is active) ─────────────
    if should_use_swc(file) && !force_tree_sitter {
        let file_path_utf8 = path_to_utf8(&file.path);
        let file_path = file_path_utf8.as_str();

        // Compute line count from source bytes — mirrors tree-sitter's
        // `root_node().end_position().row + 1` (row is 0-indexed, so the last
        // line index + 1 == number of lines).
        let line_count = u32::try_from(source.lines().count()).unwrap_or(u32::MAX);

        let file_node = NodeData {
            id: node_id(repo, file_path, NodeKind::File, file_path),
            kind: NodeKind::File,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: file_path.to_owned(),
            qualified_name: Some(format!("{repo}::{file_path}")),
            external_id: None,
            signature: None,
            visibility: None,
            span: Some(SourceSpan {
                line_start: 1,
                line_len: u16::try_from(line_count.saturating_sub(1)).unwrap_or(u16::MAX),
                column_start: 0,
                column_len: 0,
            }),
            is_virtual: false,
        };
        let module_external_id = format!("module::{repo}::{file_path}");
        let module_node = NodeData {
            id: ref_node_id(NodeKind::Module, &module_external_id),
            kind: NodeKind::Module,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: file_path
                .rsplit('/')
                .next()
                .map_or_else(|| file_path.to_owned(), ToOwned::to_owned),
            qualified_name: Some(module_external_id.clone()),
            external_id: Some(module_external_id),
            signature: None,
            visibility: Some(Visibility::Public),
            span: file_node.span.clone(),
            is_virtual: true,
        };

        let mut state = ParseState::new(
            repo,
            repo_root,
            file,
            &source,
            path_aliases,
            file_node.clone(),
            module_node.clone(),
        );
        state.nodes.push(file_node.clone());
        state.nodes.push(module_node.clone());
        state.edges.push(EdgeData {
            source: file_node.id,
            target: module_node.id,
            kind: EdgeKind::Defines,
            metadata: EdgeMetadata::default(),
            owner_file: file_node.id,
            is_cross_file: false,
        });

        let fallback_checkpoint = state.checkpoint();
        let swc_status = crate::ts_js_swc::parse_ts_js_with_swc_with_status(
            file,
            &mut state,
            &source,
            &absolute_path,
        );
        if swc_status != crate::ts_js_swc::SwcParseStatus::Parsed {
            let status = match swc_status {
                crate::ts_js_swc::SwcParseStatus::Parsed => "parsed",
                crate::ts_js_swc::SwcParseStatus::Recovered => "recovered",
                crate::ts_js_swc::SwcParseStatus::Unrecoverable => "unrecoverable",
            };
            state.restore(fallback_checkpoint);
            tracing::warn!(
                path = %absolute_path.display(),
                swc_status = status,
                "falling back to tree-sitter TS/JS parser after non-parsed SWC result"
            );
            let fallback_start = std::time::Instant::now();
            let tree = PARSER.with_borrow_mut(|parser| -> Result<_, ParseError> {
                let language = parser_language(file);
                parser.set_language(&language)?;
                parse_tree_with_timeout(
                    parser,
                    &source,
                    fallback_start,
                    &absolute_path,
                    "tree-sitter TS/JS fallback",
                )
            })?;
            visit_ts_js(tree.root_node(), &mut state, None, None, false, &[], 0);
        }

        // Framework augmentations (same logic as the non-swc path below).
        macro_rules! state_snapshot_swc {
            () => {
                ParsedFile {
                    file: file.clone(),
                    source_path: absolute_path.clone(),
                    source: source.clone(),
                    file_node: file_node.clone(),
                    nodes: state.nodes.clone(),
                    edges: state.edges.clone(),
                    symbols: state.symbols.clone(),
                    call_sites: state.call_sites.clone(),
                    import_bindings: state.import_bindings.clone(),
                    constant_strings: state.constant_strings.clone(),
                    parse_ms: i64::try_from(parse_start.elapsed().as_millis()).unwrap_or(i64::MAX),
                }
            };
        }

        let mut active_pack_refs = active_packs_arg.to_vec();
        if !active_pack_refs
            .iter()
            .any(|pack| pack.id == crate::frameworks::registry::PackId::SharedLib)
        {
            active_pack_refs.push(crate::frameworks::profile::ResolvedPack {
                id: crate::frameworks::registry::PackId::SharedLib,
                options: serde_yaml_ng::Value::Null,
            });
        }
        if !active_pack_refs
            .iter()
            .any(|pack| pack.id == crate::frameworks::registry::PackId::FrontendHooks)
        {
            active_pack_refs.push(crate::frameworks::profile::ResolvedPack {
                id: crate::frameworks::registry::PackId::FrontendHooks,
                options: serde_yaml_ng::Value::Null,
            });
        }

        let registry =
            BUILTIN_REGISTRY.get_or_init(crate::frameworks::registry::PackRegistry::builtin);
        let mut seen_groups = rustc_hash::FxHashSet::default();
        for pack in &active_pack_refs {
            let pack_id = pack.id;
            let group = pack_id.aug_group();
            if !seen_groups.insert(group) {
                continue;
            }
            let augmentation = registry.augment(pack_id, &state_snapshot_swc!());
            append_unique_nodes(&mut state.nodes, augmentation.nodes);
            state.edges.extend(augmentation.edges);
        }

        let mut parsed = ParsedFile {
            file: file.clone(),
            source_path: absolute_path,
            source: source.clone(),
            file_node,
            nodes: state.nodes,
            edges: state.edges,
            symbols: state.symbols,
            call_sites: state.call_sites,
            import_bindings: state.import_bindings,
            constant_strings: state.constant_strings,
            parse_ms: i64::try_from(parse_start.elapsed().as_millis()).unwrap_or(i64::MAX),
        };
        crate::projection::augment_projection_fields(&mut parsed);
        apply_workspace_semantic_edges(&mut parsed, repo_root);
        return Ok(parsed);
    }

    // ── All other languages: use tree-sitter ────────────────────────────────
    let tree = PARSER.with_borrow_mut(|parser| -> Result<_, ParseError> {
        let language = parser_language(file);
        parser.set_language(&language)?;
        parse_tree_with_timeout(
            parser,
            &source,
            parse_start,
            &absolute_path,
            "tree-sitter parser",
        )
    })?;

    let file_path_utf8 = path_to_utf8(&file.path);
    let file_path = file_path_utf8.as_str();
    let file_node = NodeData {
        id: node_id(repo, file_path, NodeKind::File, file_path),
        kind: NodeKind::File,
        repo: repo.to_owned(),
        file_path: file_path.to_owned(),
        name: file_path.to_owned(),
        qualified_name: Some(format!("{repo}::{file_path}")),
        external_id: None,
        signature: None,
        visibility: None,
        span: Some(SourceSpan {
            line_start: 1,
            // `end_position().row` is 0-indexed, so +1 gives line count (1-indexed end).
            // line_len = end_line - line_start (1) = end_row + 1 - 1 = end_row.
            line_len: u16::try_from(tree.root_node().end_position().row).unwrap_or(u16::MAX),
            column_start: 0,
            column_len: 0,
        }),
        is_virtual: false,
    };
    let module_external_id = format!("module::{repo}::{file_path}");
    let module_node = NodeData {
        id: ref_node_id(NodeKind::Module, &module_external_id),
        kind: NodeKind::Module,
        repo: repo.to_owned(),
        file_path: file_path.to_owned(),
        name: file_path
            .rsplit('/')
            .next()
            .map_or_else(|| file_path.to_owned(), ToOwned::to_owned),
        qualified_name: Some(module_external_id.clone()),
        external_id: Some(module_external_id),
        signature: None,
        visibility: Some(Visibility::Public),
        span: file_node.span.clone(),
        is_virtual: true,
    };

    let mut state = ParseState::new(
        repo,
        repo_root,
        file,
        &source,
        path_aliases,
        file_node.clone(),
        module_node.clone(),
    );
    state.nodes.push(file_node.clone());
    state.nodes.push(module_node.clone());
    state.edges.push(EdgeData {
        source: file_node.id,
        target: module_node.id,
        kind: EdgeKind::Defines,
        metadata: EdgeMetadata::default(),
        owner_file: file_node.id,
        is_cross_file: false,
    });

    match file.language {
        Language::TypeScript | Language::JavaScript => {
            // Should be unreachable; handled above by swc path.
            visit_ts_js(tree.root_node(), &mut state, None, None, false, &[], 0);
        }
        Language::Python => {
            visit_python(tree.root_node(), &mut state, None, None, None, &[], 0);
        }
        Language::Rust | Language::Go | Language::Java => {}
    }

    // Framework augmentations: convert the active_frameworks slice to a
    // Vec<PackId>, then run each pack's augmentation group at most once.
    // The snapshot is rebuilt before each group so every group sees the
    // accumulated output of all previous groups - preserving the original
    // sequential semantics while eliminating the hardcoded if-chain.
    macro_rules! state_snapshot {
        () => {
            ParsedFile {
                file: file.clone(),
                source_path: absolute_path.clone(),
                source: source.clone(),
                file_node: file_node.clone(),
                nodes: state.nodes.clone(),
                edges: state.edges.clone(),
                symbols: state.symbols.clone(),
                call_sites: state.call_sites.clone(),
                import_bindings: state.import_bindings.clone(),
                constant_strings: state.constant_strings.clone(),
                parse_ms: i64::try_from(parse_start.elapsed().as_millis()).unwrap_or(i64::MAX),
            }
        };
    }

    // Build the final active-packs list from the caller-supplied slice.
    // SharedLib and FrontendHooks always run for TS/JS files regardless of
    // which packs were requested, so we append them when absent.
    let mut active_pack_refs = active_packs_arg.to_vec();
    if matches!(file.language, Language::TypeScript | Language::JavaScript) {
        if !active_pack_refs
            .iter()
            .any(|pack| pack.id == PackId::SharedLib)
        {
            active_pack_refs.push(crate::frameworks::profile::ResolvedPack {
                id: PackId::SharedLib,
                options: serde_yaml_ng::Value::Null,
            });
        }
        if !active_pack_refs
            .iter()
            .any(|pack| pack.id == PackId::FrontendHooks)
        {
            active_pack_refs.push(crate::frameworks::profile::ResolvedPack {
                id: PackId::FrontendHooks,
                options: serde_yaml_ng::Value::Null,
            });
        }
    }

    // Reuse a single process-wide registry: `builtin()` allocates a 15-entry
    // Vec<PackEntry> and was previously called per file.
    let registry = BUILTIN_REGISTRY.get_or_init(PackRegistry::builtin);

    // Deduplicate by augmentation *group* and run each group exactly once.
    // Multiple packs may share a group (e.g. Azure + LaunchDarkly both map
    // to AugGroup::Azure).  A fresh snapshot is taken before each group so
    // every group sees the accumulated output of all previous groups,
    // preserving the original sequential augmentation semantics.
    let mut seen_groups = rustc_hash::FxHashSet::default();
    for pack in &active_pack_refs {
        let pack_id = pack.id;
        // Skip SharedLib and FrontendHooks for non-TS/JS files (belt-and-suspenders guard).
        if matches!(pack_id, PackId::SharedLib | PackId::FrontendHooks)
            && !matches!(file.language, Language::TypeScript | Language::JavaScript)
        {
            continue;
        }
        let group = pack_id.aug_group();
        if !seen_groups.insert(group) {
            // Another pack in this group was already processed.
            continue;
        }
        let augmentation = registry.augment(pack_id, &state_snapshot!());
        append_unique_nodes(&mut state.nodes, augmentation.nodes);
        state.edges.extend(augmentation.edges);
    }

    let mut parsed = ParsedFile {
        file: file.clone(),
        source_path: absolute_path,
        source: source.clone(),
        file_node,
        nodes: state.nodes,
        edges: state.edges,
        symbols: state.symbols,
        call_sites: state.call_sites,
        import_bindings: state.import_bindings,
        constant_strings: state.constant_strings,
        parse_ms: i64::try_from(parse_start.elapsed().as_millis()).unwrap_or(i64::MAX),
    };
    crate::projection::augment_projection_fields(&mut parsed);
    apply_workspace_semantic_edges(&mut parsed, repo_root);
    Ok(parsed)
}

fn should_use_swc(file: &FileEntry) -> bool {
    if !matches!(file.language, Language::TypeScript | Language::JavaScript) {
        return false;
    }
    !file
        .path
        .extension()
        .and_then(std::ffi::OsStr::to_str)
        .is_some_and(|ext| {
            ext.eq_ignore_ascii_case("json")
                || ext.eq_ignore_ascii_case("yaml")
                || ext.eq_ignore_ascii_case("yml")
        })
}

fn parse_tree_with_timeout(
    parser: &mut Parser,
    source: &str,
    started_at: Instant,
    path: &Path,
    timeout_context: &'static str,
) -> Result<tree_sitter::Tree, ParseError> {
    let mut timed_out = false;
    let mut progress_cb = |_: &tree_sitter::ParseState| -> ControlFlow<()> {
        if started_at.elapsed() >= TREE_SITTER_PARSE_TIMEOUT {
            timed_out = true;
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    };
    let options = tree_sitter::ParseOptions::new().progress_callback(&mut progress_cb);
    let len = source.len();
    let bytes = source.as_bytes();
    let tree = parser.parse_with_options(
        &mut |i, _| {
            if i < len { &bytes[i..] } else { &[] }
        },
        None,
        Some(options),
    );
    tree.ok_or_else(|| {
        if timed_out {
            let timeout_ms =
                u64::try_from(TREE_SITTER_PARSE_TIMEOUT.as_millis()).unwrap_or(u64::MAX);
            tracing::warn!(
                path = %path.display(),
                timeout_ms,
                timeout_context,
                "tree-sitter parse timed out"
            );
            ParseError::Timeout {
                path: path.to_path_buf(),
                timeout_ms,
            }
        } else {
            ParseError::MissingTree {
                path: path.to_path_buf(),
            }
        }
    })
}

/// Maximum number of package-import resolution entries retained per rayon
/// worker thread.  Each entry maps `(repo_root, import_source)` → resolved
/// path.  Most repositories have fewer than 100 distinct package imports, so
/// 1024 entries is generous while bounding the per-thread footprint under
/// long-running `watch` / `serve` daemons.
const PACKAGE_IMPORT_CACHE_CAPACITY: usize = 1024;
const IMPORT_PATH_EXISTS_CACHE_CAPACITY: usize = 4_096;
const WORKSPACE_REPO_IDENTITY_CACHE_CAPACITY: usize = 64;

/// Maximum number of ancestor directories of a repo root to scan for sibling
/// Python projects.  Six levels covers `monorepo/services/<repo>` (1),
/// `monorepo/<repo>` (2), `monorepo/<group>/<repo>` (2), and a comfortable
/// margin for nested layouts without making `read_dir` quadratic on
/// pathological depths.  Increase only if a real layout requires it.
const PYTHON_SIBLING_SEARCH_ANCESTORS: usize = 6;

/// Maximum number of directory levels to climb from a parsed file looking for
/// a `gather-step.config.yaml`.  Twelve covers all configured layouts seen in
/// the project's fixtures and real users (deepest known is around eight).  A
/// hard cap prevents pathological filesystem walks when the file is outside
/// any configured workspace.
const GATHER_STEP_CONFIG_MAX_ASCEND: usize = 12;

/// Identity of a configured repo within a workspace.  Constructed only via
/// [`WorkspaceRepoIdentity::new`] from a non-empty name and absolute root so
/// callers cannot accidentally fall back to `root.file_name()` as a name (the
/// bug fixed in 5a5563a).  Fields are kept private; use the accessors.
#[derive(Clone, Debug)]
struct WorkspaceRepoIdentity {
    name: String,
    root: PathBuf,
}

impl WorkspaceRepoIdentity {
    fn new(name: &str, root: PathBuf) -> Option<Self> {
        let name = name.trim();
        if name.is_empty() || !root.is_absolute() {
            return None;
        }
        Some(Self {
            name: name.to_owned(),
            root,
        })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn root(&self) -> &Path {
        &self.root
    }
}

thread_local! {
    static PARSER: RefCell<Parser> = RefCell::new(Parser::new());
    static IMPORT_PATH_EXISTS_CACHE: RefCell<lru::LruCache<PathBuf, bool>> =
        RefCell::new(lru::LruCache::new(
            NonZeroUsize::new(IMPORT_PATH_EXISTS_CACHE_CAPACITY)
                .expect("IMPORT_PATH_EXISTS_CACHE_CAPACITY is non-zero"),
        ));
    /// Tracks the repo root seen by the most recent `parse_file_core` call on
    /// this thread.  Used to clear `IMPORT_PATH_EXISTS_CACHE` once per repo
    /// rather than once per file.  Path-alias-resolved existence checks are
    /// stable across files within a repo (aliases only change at the repo
    /// boundary), so per-file clearing throws away valid cache hits.
    static LAST_SEEN_REPO_ROOT: RefCell<Option<PathBuf>> = const { RefCell::new(None) };
    // Memoizes workspace/sibling-package resolution per `(repo_root, source)`.
    // Without this, every bare import (`@repo/shared_contracts`, etc.) triggered a
    // 6-ancestor `read_dir` plus a `package.json` read-and-parse for every
    // sibling — once per import per file. On a 24-repo monorepo that was
    // dominating the hot path. The result depends only on the workspace
    // layout, which is stable across an index run, so a thread-local cache
    // per rayon worker is safe and has no cross-thread coordination cost.
    //
    // Capped at `PACKAGE_IMPORT_CACHE_CAPACITY` entries via LRU eviction to
    // bound per-thread memory consumption under `watch` / `serve` long-running
    // daemons.  One-shot CLI runs are unaffected: they index far fewer than
    // 1024 distinct package imports per thread.
    static PACKAGE_IMPORT_RESOLUTION_CACHE: RefCell<lru::LruCache<(PathBuf, String), Option<PathBuf>>> =
        RefCell::new(lru::LruCache::new(
            NonZeroUsize::new(PACKAGE_IMPORT_CACHE_CAPACITY)
                .expect("PACKAGE_IMPORT_CACHE_CAPACITY is non-zero"),
        ));
    static WORKSPACE_REPO_IDENTITY_CACHE: RefCell<lru::LruCache<PathBuf, Option<Vec<WorkspaceRepoIdentity>>>> =
        RefCell::new(lru::LruCache::new(
            NonZeroUsize::new(WORKSPACE_REPO_IDENTITY_CACHE_CAPACITY)
                .expect("WORKSPACE_REPO_IDENTITY_CACHE_CAPACITY is non-zero"),
        ));
}

/// Clear `IMPORT_PATH_EXISTS_CACHE` if `repo_root` differs from the last repo
/// root seen on this thread.  Path-alias-resolved existence checks are stable
/// within a repo (aliases only change at the repo boundary), so per-file
/// clearing discards valid hits.  Clearing once per repo retains the hits
/// across all files in the same repo while still invalidating when the parse
/// moves to a new repo.
fn clear_import_path_exists_cache_if_new_repo(repo_root: &Path) {
    let should_clear = LAST_SEEN_REPO_ROOT.with_borrow(|last| last.as_deref() != Some(repo_root));
    if should_clear {
        IMPORT_PATH_EXISTS_CACHE.with_borrow_mut(lru::LruCache::clear);
        LAST_SEEN_REPO_ROOT.with_borrow_mut(|last| {
            *last = Some(repo_root.to_path_buf());
        });
    }
}

fn resolve_workspace_or_sibling_package_cached(repo_root: &Path, source: &str) -> Option<PathBuf> {
    let key = (repo_root.to_path_buf(), source.to_owned());
    // `LruCache::get` requires `&mut self` because it updates the recency order.
    if let Some(hit) =
        PACKAGE_IMPORT_RESOLUTION_CACHE.with_borrow_mut(|cache| cache.get(&key).cloned())
    {
        return hit;
    }
    let resolved = resolve_workspace_package_import(repo_root, source)
        .or_else(|| resolve_sibling_package_import(repo_root, source));
    PACKAGE_IMPORT_RESOLUTION_CACHE.with_borrow_mut(|cache| {
        cache.put(key, resolved.clone());
    });
    resolved
}

/// Convert a [`std::path::Path`] to a UTF-8 forward-slash string suitable for
/// use as a parser-internal file path.
///
/// On all platforms the codebase assumes UTF-8 paths, so the conversion via
/// [`camino::Utf8PathBuf`] is expected to succeed. When it does not (exotic
/// non-UTF-8 OS paths), the function falls back to [`Path::to_string_lossy`]
/// followed by a backslash-to-slash replacement, preserving the previous
/// behaviour while surfacing the fact that the path is non-UTF-8 through the
/// replacement character U+FFFD that `to_string_lossy` inserts.
fn path_to_utf8(path: &Path) -> Utf8PathBuf {
    Utf8PathBuf::from_path_buf(path.to_path_buf()).unwrap_or_else(|p| {
        let s = p.to_string_lossy().replace('\\', "/");
        Utf8PathBuf::from(s)
    })
}

pub(crate) struct ParseState<'a> {
    repo: &'a str,
    repo_root: &'a Path,
    file: &'a FileEntry,
    source: &'a str,
    path_aliases: &'a PathAliases,
    nodes: Vec<NodeData>,
    edges: Vec<EdgeData>,
    symbols: Vec<SymbolCapture>,
    call_sites: Vec<EnrichedCallSite>,
    import_bindings: Vec<ImportBinding>,
    module_cache: FxHashMap<String, gather_step_core::NodeId>,
    constant_strings: FxHashMap<String, String>,
    function_ordinal: u16,
    class_ordinal: u16,
    type_ordinal: u16,
    import_ordinal: u16,
    decorator_ordinal: u16,
    file_node: NodeData,
    module_node: NodeData,
    /// UTF-8 forward-slash path computed once at construction from `file.path`.
    /// Returned by `file_path()` without allocation.
    cached_file_path: Utf8PathBuf,
}

#[derive(Clone)]
struct ParseStateCheckpoint {
    nodes_len: usize,
    edges_len: usize,
    symbols_len: usize,
    call_sites_len: usize,
    import_bindings_len: usize,
    module_cache: FxHashMap<String, gather_step_core::NodeId>,
    constant_strings: FxHashMap<String, String>,
    function_ordinal: u16,
    class_ordinal: u16,
    type_ordinal: u16,
    import_ordinal: u16,
    decorator_ordinal: u16,
}

impl<'a> ParseState<'a> {
    fn new(
        repo: &'a str,
        repo_root: &'a Path,
        file: &'a FileEntry,
        source: &'a str,
        path_aliases: &'a PathAliases,
        file_node: NodeData,
        module_node: NodeData,
    ) -> Self {
        let cached_file_path = path_to_utf8(&file.path);
        Self {
            repo,
            repo_root,
            file,
            source,
            path_aliases,
            nodes: Vec::new(),
            edges: Vec::new(),
            symbols: Vec::new(),
            call_sites: Vec::new(),
            import_bindings: Vec::new(),
            module_cache: FxHashMap::default(),
            constant_strings: FxHashMap::default(),
            function_ordinal: 0,
            class_ordinal: 0,
            type_ordinal: 0,
            import_ordinal: 0,
            decorator_ordinal: 0,
            file_node,
            module_node,
            cached_file_path,
        }
    }

    pub(crate) fn file_path(&self) -> &str {
        self.cached_file_path.as_str()
    }

    /// Test-only constructor that builds a minimal `ParseState` suitable for
    /// driving `parse_ts_js_with_swc` in isolation.  The returned state has
    /// empty `nodes` / `edges` / `symbols` and dummy file/module nodes.
    ///
    /// Only available when the crate is compiled for testing or with the
    /// `test-support` feature.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn for_test(file: &'a FileEntry, source: &'a str) -> Self {
        use gather_step_core::{NodeKind, SourceSpan, Visibility, node_id, ref_node_id};

        let path_utf8 = path_to_utf8(&file.path);
        let path_str = path_utf8.as_str();
        let file_node = NodeData {
            id: node_id("test", path_str, NodeKind::File, path_str),
            kind: NodeKind::File,
            repo: "test".to_owned(),
            file_path: path_str.to_owned(),
            name: path_str.to_owned(),
            qualified_name: Some(format!("test::{path_str}")),
            external_id: None,
            signature: None,
            visibility: None,
            span: Some(SourceSpan {
                line_start: 1,
                line_len: 0,
                column_start: 0,
                column_len: 0,
            }),
            is_virtual: false,
        };
        let module_external_id = format!("module::test::{path_str}");
        let module_node = NodeData {
            id: ref_node_id(NodeKind::Module, &module_external_id),
            kind: NodeKind::Module,
            repo: "test".to_owned(),
            file_path: path_str.to_owned(),
            name: path_str
                .rsplit('/')
                .next()
                .map_or_else(|| path_str.to_owned(), ToOwned::to_owned),
            qualified_name: Some(module_external_id.clone()),
            external_id: Some(module_external_id),
            signature: None,
            visibility: Some(Visibility::Public),
            span: file_node.span.clone(),
            is_virtual: true,
        };
        let path_aliases = Self::empty_aliases_for_test();
        Self::new(
            "test",
            std::path::Path::new("/tmp"),
            file,
            source,
            path_aliases,
            file_node,
            module_node,
        )
    }

    #[cfg(any(test, feature = "test-support"))]
    fn empty_aliases_for_test() -> &'static PathAliases {
        static EMPTY_ALIASES: std::sync::OnceLock<PathAliases> = std::sync::OnceLock::new();
        EMPTY_ALIASES.get_or_init(PathAliases::empty)
    }

    /// Returns a reference to the symbols accumulated during parsing.  Only
    /// available when compiled for testing or with the `test-support` feature.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn symbols(&self) -> &[crate::tree_sitter::SymbolCapture] {
        &self.symbols
    }

    fn checkpoint(&self) -> ParseStateCheckpoint {
        ParseStateCheckpoint {
            nodes_len: self.nodes.len(),
            edges_len: self.edges.len(),
            symbols_len: self.symbols.len(),
            call_sites_len: self.call_sites.len(),
            import_bindings_len: self.import_bindings.len(),
            module_cache: self.module_cache.clone(),
            constant_strings: self.constant_strings.clone(),
            function_ordinal: self.function_ordinal,
            class_ordinal: self.class_ordinal,
            type_ordinal: self.type_ordinal,
            import_ordinal: self.import_ordinal,
            decorator_ordinal: self.decorator_ordinal,
        }
    }

    fn restore(&mut self, checkpoint: ParseStateCheckpoint) {
        self.nodes.truncate(checkpoint.nodes_len);
        self.edges.truncate(checkpoint.edges_len);
        self.symbols.truncate(checkpoint.symbols_len);
        self.call_sites.truncate(checkpoint.call_sites_len);
        self.import_bindings
            .truncate(checkpoint.import_bindings_len);
        self.module_cache = checkpoint.module_cache;
        self.constant_strings = checkpoint.constant_strings;
        self.function_ordinal = checkpoint.function_ordinal;
        self.class_ordinal = checkpoint.class_ordinal;
        self.type_ordinal = checkpoint.type_ordinal;
        self.import_ordinal = checkpoint.import_ordinal;
        self.decorator_ordinal = checkpoint.decorator_ordinal;
    }

    fn next_ordinal(&mut self, kind: NodeKind) -> u16 {
        match kind {
            NodeKind::Function => {
                let value = self.function_ordinal;
                self.function_ordinal = self.function_ordinal.saturating_add(1);
                if self.function_ordinal == value {
                    tracing::warn!(
                        file = %self.file_path(),
                        kind = %"Function",
                        "ordinal saturated at u16::MAX — symbols beyond this point will share node IDs"
                    );
                }
                value
            }
            NodeKind::Class => {
                let value = self.class_ordinal;
                self.class_ordinal = self.class_ordinal.saturating_add(1);
                if self.class_ordinal == value {
                    tracing::warn!(
                        file = %self.file_path(),
                        kind = %"Class",
                        "ordinal saturated at u16::MAX — symbols beyond this point will share node IDs"
                    );
                }
                value
            }
            NodeKind::Type | NodeKind::Entity => {
                let value = self.type_ordinal;
                self.type_ordinal = self.type_ordinal.saturating_add(1);
                if self.type_ordinal == value {
                    tracing::warn!(
                        file = %self.file_path(),
                        kind = %"Type",
                        "ordinal saturated at u16::MAX — symbols beyond this point will share node IDs"
                    );
                }
                value
            }
            NodeKind::Import | NodeKind::Decorator => {
                let slot = if kind == NodeKind::Import {
                    &mut self.import_ordinal
                } else {
                    &mut self.decorator_ordinal
                };
                let old = *slot;
                *slot = slot.saturating_add(1);
                if *slot == old {
                    tracing::warn!(
                        file = %self.file_path(),
                        kind = %if kind == NodeKind::Import { "Import" } else { "Decorator" },
                        "ordinal saturated at u16::MAX — symbols beyond this point will share node IDs"
                    );
                }
                old
            }
            _ => 0,
        }
    }

    #[expect(clippy::needless_pass_by_value)]
    pub(crate) fn push_symbol(
        &mut self,
        kind: NodeKind,
        name: String,
        qualified_name: Option<String>,
        span: Option<SourceSpan>,
        signature: Option<String>,
        visibility: Option<Visibility>,
        parent_class: Option<String>,
        decorators: Vec<DecoratorCapture>,
        class_decorators: Vec<DecoratorCapture>,
        constructor_dependencies: Vec<String>,
    ) -> NodeData {
        let file_path = self.file_path().to_owned();
        // Identity is (repo, path, kind, qualified_name) — independent of AST
        // visit order so inserting a sibling symbol above this one does not
        // invalidate its id or any inbound edges.
        let identity = qualified_name
            .as_deref()
            .filter(|qn| !qn.is_empty())
            .unwrap_or(name.as_str());
        let node = NodeData {
            id: node_id(self.repo, &file_path, kind, identity),
            kind,
            repo: self.repo.to_owned(),
            file_path: file_path.clone(),
            name: name.clone(),
            qualified_name: qualified_name.clone(),
            external_id: None,
            signature,
            visibility,
            span,
            is_virtual: false,
        };
        self.nodes.push(node.clone());
        self.edges.push(EdgeData {
            source: self.file_node.id,
            target: node.id,
            kind: EdgeKind::Defines,
            metadata: EdgeMetadata::default(),
            owner_file: self.file_node.id,
            is_cross_file: false,
        });

        if matches!(node.visibility, Some(Visibility::Public)) {
            self.edges.push(EdgeData {
                source: self.module_node.id,
                target: node.id,
                kind: EdgeKind::Exports,
                metadata: EdgeMetadata::default(),
                owner_file: self.file_node.id,
                is_cross_file: false,
            });
        }

        for decorator in &decorators {
            self.push_decorator_edge(&node, decorator);
        }
        for decorator in &class_decorators {
            self.push_decorator_edge(&node, decorator);
        }

        self.symbols.push(SymbolCapture {
            node: node.clone(),
            file_node: self.file_node.id,
            parent_class,
            decorators,
            class_decorators,
            constructor_dependencies,
            implemented_interfaces: Vec::new(),
            base_classes: Vec::new(),
        });
        node
    }

    pub(crate) fn set_symbol_implemented_interfaces(
        &mut self,
        node_id: gather_step_core::NodeId,
        implemented_interfaces: Vec<String>,
    ) {
        if let Some(symbol) = self
            .symbols
            .iter_mut()
            .find(|symbol| symbol.node.id == node_id)
        {
            symbol.implemented_interfaces = implemented_interfaces;
        }
    }

    pub(crate) fn set_symbol_base_classes(
        &mut self,
        node_id: gather_step_core::NodeId,
        base_classes: Vec<String>,
    ) {
        if let Some(symbol) = self
            .symbols
            .iter_mut()
            .find(|symbol| symbol.node.id == node_id)
        {
            symbol.base_classes = base_classes;
        }
    }

    fn push_decorator_edge(&mut self, owner: &NodeData, decorator: &DecoratorCapture) {
        let decorator_qn = format!(
            "{}::@{}",
            owner
                .qualified_name
                .as_deref()
                .unwrap_or(owner.name.as_str()),
            decorator.name
        );
        let decorator_node = NodeData {
            // Identity uses the owner-qualified decorator name so the id is
            // stable regardless of how many other decorators exist in the file.
            id: node_id(
                self.repo,
                self.file_path(),
                NodeKind::Decorator,
                &decorator_qn,
            ),
            kind: NodeKind::Decorator,
            repo: self.repo.to_owned(),
            file_path: self.file_path().to_owned(),
            name: decorator.name.clone(),
            qualified_name: Some(decorator_qn),
            external_id: None,
            signature: Some(decorator.raw.clone()),
            visibility: None,
            span: owner.span.clone(),
            is_virtual: false,
        };
        self.nodes.push(decorator_node.clone());
        self.edges.push(EdgeData {
            source: owner.id,
            target: decorator_node.id,
            kind: EdgeKind::UsesDecorator,
            metadata: EdgeMetadata::default(),
            owner_file: self.file_node.id,
            is_cross_file: false,
        });
    }

    fn push_imports(&mut self, statement: Node<'_>) {
        let raw = node_text(statement, self.source);
        let import_groups = if self.file.language == Language::Python {
            let groups = parse_python_import_groups(statement, self.source);
            if groups.is_empty() {
                parse_python_import_groups_from_raw(raw)
            } else {
                groups
            }
        } else {
            let Some(source) = parse_import_source(raw) else {
                return;
            };
            let bindings = parse_ts_import_bindings(raw, &source);
            vec![(source, bindings)]
        };

        for (source, bindings) in import_groups {
            if source.is_empty() || bindings.is_empty() {
                continue;
            }
            let resolved_path = resolve_import_path(
                self.repo_root,
                &self.file.path,
                &source,
                self.file.language,
                self.path_aliases,
            );
            let mut is_new_module = false;
            let module_id = *self.module_cache.entry(source.clone()).or_insert_with(|| {
                is_new_module = true;
                ref_node_id(NodeKind::Module, &format!("module-import::{source}"))
            });
            if is_new_module {
                self.nodes.push(NodeData {
                    id: module_id,
                    kind: NodeKind::Module,
                    repo: self.repo.to_owned(),
                    file_path: self.file_path().to_owned(),
                    name: source.clone(),
                    qualified_name: Some(format!("module-import::{source}")),
                    external_id: Some(format!("module-import::{source}")),
                    signature: None,
                    visibility: Some(Visibility::Public),
                    span: Some(span_from(statement)),
                    is_virtual: true,
                });
            }
            self.edges.push(EdgeData {
                source: self.file_node.id,
                target: module_id,
                kind: EdgeKind::Imports,
                metadata: EdgeMetadata::default(),
                owner_file: self.file_node.id,
                is_cross_file: true,
            });

            for binding in bindings {
                // Import identity: file-path-scoped local name, stable across
                // reordering of other imports in the same file.
                let import_qn = format!("{}::{}", self.file_path(), binding.local_name);
                let import_node = NodeData {
                    id: node_id(self.repo, self.file_path(), NodeKind::Import, &import_qn),
                    kind: NodeKind::Import,
                    repo: self.repo.to_owned(),
                    file_path: self.file_path().to_owned(),
                    name: binding.local_name.clone(),
                    qualified_name: Some(import_qn),
                    external_id: None,
                    signature: Some(format!("from {source}")),
                    visibility: None,
                    span: Some(span_from(statement)),
                    is_virtual: false,
                };
                self.nodes.push(import_node.clone());
                self.edges.push(EdgeData {
                    source: self.file_node.id,
                    target: import_node.id,
                    kind: EdgeKind::Defines,
                    metadata: EdgeMetadata::default(),
                    owner_file: self.file_node.id,
                    is_cross_file: false,
                });
                self.import_bindings.push(ImportBinding {
                    resolved_path: resolved_path.clone(),
                    source: source.clone(),
                    ..binding
                });
            }
        }
    }

    pub(crate) fn record_constant_string(&mut self, key: String, value: String) {
        if !key.is_empty() && !value.is_empty() {
            self.constant_strings.insert(key, value);
        }
    }

    fn push_call_site(
        &mut self,
        owner_id: gather_step_core::NodeId,
        callee_name: String,
        callee_qualified_hint: Option<String>,
        literal_argument: Option<String>,
        raw_arguments: Option<String>,
        node: Node<'_>,
    ) {
        if callee_name.is_empty() {
            return;
        }
        self.call_sites.push(EnrichedCallSite {
            owner_id,
            owner_file: self.file_node.id,
            source_path: self.repo_root.join(&self.file.path),
            callee_name,
            callee_qualified_hint,
            literal_argument,
            raw_arguments,
            span: Some(span_from(node)),
        });
    }

    // ── pub(crate) accessors used by ts_js_swc ──────────────────────────────

    pub(crate) fn repo(&self) -> &str {
        self.repo
    }

    pub(crate) fn repo_root(&self) -> &Path {
        self.repo_root
    }

    pub(crate) fn file(&self) -> &FileEntry {
        self.file
    }

    pub(crate) fn path_aliases(&self) -> &PathAliases {
        self.path_aliases
    }

    pub(crate) fn module_cache_mut(&mut self) -> &mut FxHashMap<String, gather_step_core::NodeId> {
        &mut self.module_cache
    }

    pub(crate) fn constant_strings_mut(&mut self) -> &mut FxHashMap<String, String> {
        &mut self.constant_strings
    }

    pub(crate) fn file_node_id(&self) -> gather_step_core::NodeId {
        self.file_node.id
    }

    #[expect(
        dead_code,
        reason = "ordinal no longer feeds NodeId hashing; retained for potential future per-parent ordinal tracking"
    )]
    pub(crate) fn next_ordinal_pub(&mut self, kind: NodeKind) -> u16 {
        self.next_ordinal(kind)
    }

    pub(crate) fn push_raw_node(&mut self, node: NodeData) {
        self.nodes.push(node);
    }

    pub(crate) fn push_raw_edge(&mut self, edge: EdgeData) {
        self.edges.push(edge);
    }

    pub(crate) fn push_import_binding(&mut self, binding: crate::resolve::ImportBinding) {
        self.import_bindings.push(binding);
    }

    /// swc variant of `push_call_site` — takes a [`SourceSpan`] directly.
    pub(crate) fn push_call_site_swc(
        &mut self,
        owner_id: gather_step_core::NodeId,
        callee_name: String,
        callee_qualified_hint: Option<String>,
        literal_argument: Option<String>,
        raw_arguments: Option<String>,
        span: SourceSpan,
    ) {
        if callee_name.is_empty() {
            return;
        }
        self.call_sites.push(EnrichedCallSite {
            owner_id,
            owner_file: self.file_node.id,
            source_path: self.repo_root.join(&self.file.path),
            callee_name,
            callee_qualified_hint,
            literal_argument,
            raw_arguments,
            span: Some(span),
        });
    }
}

#[expect(clippy::semicolon_if_nothing_returned)]
fn visit_ts_js(
    node: Node<'_>,
    state: &mut ParseState<'_>,
    parent_class: Option<&NodeData>,
    owner: Option<gather_step_core::NodeId>,
    force_exported: bool,
    class_decorators: &[DecoratorCapture],
    depth: usize,
) {
    if depth > MAX_VISITOR_DEPTH {
        return;
    }
    match node.kind() {
        "program" | "statement_block" | "class_body" => {
            visit_ts_js_sequence(
                node,
                state,
                parent_class,
                owner,
                force_exported,
                class_decorators,
                depth,
            );
        }
        "export_statement" => {
            let raw = node_text(node, state.source);
            if raw.contains(" from ") {
                state.push_imports(node);
            }
            record_default_export_constants(node, state);
            let mut cursor = node.walk();
            let mut pending_decorators = Vec::new();
            for child in node.children(&mut cursor) {
                if child.kind() == "decorator" {
                    pending_decorators.push(single_decorator(child, state.source));
                    continue;
                }
                if child.kind() != "export" {
                    visit_ts_js_with_pending(
                        child,
                        state,
                        parent_class,
                        owner,
                        true,
                        class_decorators,
                        &pending_decorators,
                        depth + 1,
                    );
                    pending_decorators.clear();
                }
            }
        }
        "import_statement" => state.push_imports(node),
        "class_declaration" => {
            let name = child_text(node, "name", state.source)
                .unwrap_or_else(|| "AnonymousClass".to_owned());
            let decorators = collect_decorators(node, state.source);
            let constructor_dependencies = collect_constructor_dependencies(node, state.source);
            let implemented_interfaces = collect_implemented_interfaces(node, state.source);
            let exported = force_exported || is_exported(node);
            let class_node = state.push_symbol(
                NodeKind::Class,
                name.clone(),
                Some(name.clone()),
                Some(span_from(node)),
                None,
                if exported {
                    Some(Visibility::Public)
                } else {
                    None
                },
                None,
                decorators.clone(),
                Vec::new(),
                constructor_dependencies,
            );
            state.set_symbol_implemented_interfaces(class_node.id, implemented_interfaces);

            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if child.kind() == "class_body" {
                    visit_ts_js_sequence(
                        child,
                        state,
                        Some(&class_node),
                        Some(class_node.id),
                        force_exported,
                        &decorators,
                        depth,
                    );
                }
            }
        }
        "function_declaration" => {
            let name =
                child_text(node, "name", state.source).unwrap_or_else(|| "anonymous".to_owned());
            let decorators = collect_decorators(node, state.source);
            let exported = force_exported || is_exported(node);
            let function_node = state.push_symbol(
                NodeKind::Function,
                name.clone(),
                Some(name.clone()),
                Some(span_from(node)),
                function_signature(node, state.source),
                if exported {
                    Some(Visibility::Public)
                } else {
                    None
                },
                parent_class.map(|class_node| class_node.name.clone()),
                decorators,
                class_decorators.to_vec(),
                Vec::new(),
            );
            recurse_children(node, |child| {
                visit_ts_js(
                    child,
                    state,
                    parent_class,
                    Some(function_node.id),
                    false,
                    class_decorators,
                    depth + 1,
                )
            });
        }
        "lexical_declaration" | "variable_declaration" => {
            recurse_children(node, |child| {
                visit_ts_js(
                    child,
                    state,
                    parent_class,
                    owner,
                    force_exported,
                    class_decorators,
                    depth + 1,
                )
            });
        }
        "variable_declarator" => {
            if let Some(value) = node.child_by_field_name("value").or_else(|| node.child(2)) {
                let name = child_text(node, "name", state.source)
                    .or_else(|| {
                        node.child_by_field_name("pattern")
                            .map(|pattern| node_text(pattern, state.source).to_owned())
                    })
                    .unwrap_or_else(|| "anonymous".to_owned());
                if matches!(value.kind(), "arrow_function" | "function") {
                    let function_node = state.push_symbol(
                        NodeKind::Function,
                        name.clone(),
                        Some(parent_class.map_or_else(
                            || name.clone(),
                            |class_node| format!("{}.{}", class_node.name, name),
                        )),
                        Some(span_from(node)),
                        function_signature(value, state.source).or_else(|| Some(name.clone())),
                        if force_exported {
                            Some(Visibility::Public)
                        } else {
                            None
                        },
                        parent_class.map(|class_node| class_node.name.clone()),
                        Vec::new(),
                        class_decorators.to_vec(),
                        Vec::new(),
                    );
                    recurse_children(value, |child| {
                        visit_ts_js(
                            child,
                            state,
                            parent_class,
                            Some(function_node.id),
                            false,
                            class_decorators,
                            depth + 1,
                        )
                    });
                    return;
                }

                if let Some(constant_value) =
                    extract_constant_string_value(&name, value, state.source)
                {
                    for (key, value) in constant_value {
                        state.record_constant_string(key, value);
                    }
                }
            }
            recurse_children(node, |child| {
                visit_ts_js(
                    child,
                    state,
                    parent_class,
                    owner,
                    force_exported,
                    class_decorators,
                    depth + 1,
                )
            });
        }
        "method_definition" => {
            let Some(parent_class) = parent_class else {
                recurse_children(node, |child| {
                    visit_ts_js(
                        child,
                        state,
                        None,
                        owner,
                        force_exported,
                        class_decorators,
                        depth + 1,
                    )
                });
                return;
            };
            let name =
                child_text(node, "name", state.source).unwrap_or_else(|| "anonymous".to_owned());
            let decorators = collect_decorators(node, state.source);
            let method_node = state.push_symbol(
                NodeKind::Function,
                name.clone(),
                Some(format!("{}.{}", parent_class.name, name)),
                Some(span_from(node)),
                function_signature(node, state.source),
                Some(method_visibility(node, state.source)),
                Some(parent_class.name.clone()),
                decorators,
                class_decorators.to_vec(),
                Vec::new(),
            );
            recurse_children(node, |child| {
                visit_ts_js(
                    child,
                    state,
                    Some(parent_class),
                    Some(method_node.id),
                    false,
                    class_decorators,
                    depth + 1,
                )
            });
        }
        "interface_declaration" | "type_alias_declaration" | "enum_declaration" => {
            let name = child_text(node, "name", state.source)
                .unwrap_or_else(|| "AnonymousType".to_owned());
            let exported = force_exported || is_exported(node);
            state.push_symbol(
                NodeKind::Type,
                name.clone(),
                Some(name),
                Some(span_from(node)),
                Some(node_text(node, state.source).to_owned()),
                if exported {
                    Some(Visibility::Public)
                } else {
                    None
                },
                parent_class.map(|class_node| class_node.name.clone()),
                collect_decorators(node, state.source),
                class_decorators.to_vec(),
                Vec::new(),
            );
        }
        "call_expression" | "new_expression" => {
            if let Some(owner_id) = owner
                && let Some(function_node) = node
                    .child_by_field_name("function")
                    .or_else(|| node.child(0))
            {
                let (callee_name, qualified_hint) = expression_name(function_node, state.source);
                let literal_argument = first_literal_argument(node, state.source);
                let raw_arguments = raw_arguments(node, state.source);
                state.push_call_site(
                    owner_id,
                    callee_name,
                    qualified_hint,
                    literal_argument,
                    raw_arguments,
                    node,
                );
            }
            recurse_children(node, |child| {
                visit_ts_js(
                    child,
                    state,
                    parent_class,
                    owner,
                    force_exported,
                    class_decorators,
                    depth + 1,
                )
            });
        }
        _ => {
            recurse_children(node, |child| {
                visit_ts_js(
                    child,
                    state,
                    parent_class,
                    owner,
                    force_exported,
                    class_decorators,
                    depth + 1,
                )
            });
        }
    }
}

fn visit_ts_js_sequence(
    node: Node<'_>,
    state: &mut ParseState<'_>,
    parent_class: Option<&NodeData>,
    owner: Option<gather_step_core::NodeId>,
    force_exported: bool,
    class_decorators: &[DecoratorCapture],
    depth: usize,
) {
    if depth > MAX_VISITOR_DEPTH {
        return;
    }
    let mut cursor = node.walk();
    let mut pending_decorators = Vec::new();

    for child in node.children(&mut cursor) {
        if child.kind() == "decorator" {
            pending_decorators.push(single_decorator(child, state.source));
            continue;
        }

        visit_ts_js_with_pending(
            child,
            state,
            parent_class,
            owner,
            force_exported,
            class_decorators,
            &pending_decorators,
            depth,
        );
        pending_decorators.clear();
    }
}

#[expect(clippy::semicolon_if_nothing_returned)]
fn visit_ts_js_with_pending(
    node: Node<'_>,
    state: &mut ParseState<'_>,
    parent_class: Option<&NodeData>,
    owner: Option<gather_step_core::NodeId>,
    force_exported: bool,
    class_decorators: &[DecoratorCapture],
    pending_decorators: &[DecoratorCapture],
    depth: usize,
) {
    match node.kind() {
        "class_declaration" => {
            let name = child_text(node, "name", state.source)
                .unwrap_or_else(|| "AnonymousClass".to_owned());
            let mut decorators = pending_decorators.to_vec();
            decorators.extend(collect_decorators(node, state.source));
            let constructor_dependencies = collect_constructor_dependencies(node, state.source);
            let implemented_interfaces = collect_implemented_interfaces(node, state.source);
            let exported = force_exported || is_exported(node);
            let class_node = state.push_symbol(
                NodeKind::Class,
                name.clone(),
                Some(name.clone()),
                Some(span_from(node)),
                None,
                if exported {
                    Some(Visibility::Public)
                } else {
                    None
                },
                None,
                decorators.clone(),
                Vec::new(),
                constructor_dependencies,
            );
            state.set_symbol_implemented_interfaces(class_node.id, implemented_interfaces);

            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if child.kind() == "class_body" {
                    visit_ts_js_sequence(
                        child,
                        state,
                        Some(&class_node),
                        Some(class_node.id),
                        force_exported,
                        &decorators,
                        depth,
                    );
                }
            }
        }
        "function_declaration" => {
            let name =
                child_text(node, "name", state.source).unwrap_or_else(|| "anonymous".to_owned());
            let mut decorators = pending_decorators.to_vec();
            decorators.extend(collect_decorators(node, state.source));
            let exported = force_exported || is_exported(node);
            let function_node = state.push_symbol(
                NodeKind::Function,
                name.clone(),
                Some(name.clone()),
                Some(span_from(node)),
                function_signature(node, state.source),
                if exported {
                    Some(Visibility::Public)
                } else {
                    None
                },
                parent_class.map(|class_node| class_node.name.clone()),
                decorators,
                class_decorators.to_vec(),
                Vec::new(),
            );
            recurse_children(node, |child| {
                visit_ts_js(
                    child,
                    state,
                    parent_class,
                    Some(function_node.id),
                    false,
                    class_decorators,
                    depth + 1,
                )
            });
        }
        "method_definition" => {
            let Some(parent_class) = parent_class else {
                recurse_children(node, |child| {
                    visit_ts_js(
                        child,
                        state,
                        None,
                        owner,
                        force_exported,
                        class_decorators,
                        depth + 1,
                    )
                });
                return;
            };
            let name =
                child_text(node, "name", state.source).unwrap_or_else(|| "anonymous".to_owned());
            let mut decorators = pending_decorators.to_vec();
            decorators.extend(collect_decorators(node, state.source));
            let method_node = state.push_symbol(
                NodeKind::Function,
                name.clone(),
                Some(format!("{}.{}", parent_class.name, name)),
                Some(span_from(node)),
                function_signature(node, state.source),
                Some(method_visibility(node, state.source)),
                Some(parent_class.name.clone()),
                decorators,
                class_decorators.to_vec(),
                Vec::new(),
            );
            recurse_children(node, |child| {
                visit_ts_js(
                    child,
                    state,
                    Some(parent_class),
                    Some(method_node.id),
                    false,
                    class_decorators,
                    depth + 1,
                )
            });
        }
        _ => visit_ts_js(
            node,
            state,
            parent_class,
            owner,
            force_exported,
            class_decorators,
            depth + 1,
        ),
    }
}

#[expect(clippy::semicolon_if_nothing_returned)]
fn visit_python(
    node: Node<'_>,
    state: &mut ParseState<'_>,
    parent_class: Option<&NodeData>,
    owner: Option<gather_step_core::NodeId>,
    owner_qname: Option<&str>,
    class_decorators: &[DecoratorCapture],
    depth: usize,
) {
    if depth > MAX_VISITOR_DEPTH {
        return;
    }
    match node.kind() {
        "module" | "block" => {
            recurse_children(node, |child| {
                visit_python(
                    child,
                    state,
                    parent_class,
                    owner,
                    owner_qname,
                    class_decorators,
                    depth + 1,
                )
            });
        }
        "decorated_definition" => {
            let decorators = collect_decorators(node, state.source);
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                match child.kind() {
                    "class_definition" => {
                        visit_python(
                            child,
                            state,
                            parent_class,
                            owner,
                            owner_qname,
                            &decorators,
                            depth + 1,
                        );
                    }
                    "function_definition" => {
                        let merged = if class_decorators.is_empty() {
                            decorators.clone()
                        } else {
                            let mut all = class_decorators.to_vec();
                            all.extend(decorators.clone());
                            all
                        };
                        visit_python(
                            child,
                            state,
                            parent_class,
                            owner,
                            owner_qname,
                            &merged,
                            depth + 1,
                        );
                    }
                    _ => {}
                }
            }
        }
        "import_statement" | "import_from_statement" => state.push_imports(node),
        "class_definition" => {
            let name = child_text(node, "name", state.source)
                .unwrap_or_else(|| "AnonymousClass".to_owned());
            let qualified_name = match owner_qname {
                Some(parent) => format!("{parent}.{name}"),
                None => name.clone(),
            };
            let implemented_interfaces = collect_implemented_interfaces(node, state.source);
            let base_classes = collect_python_base_classes(node, state.source);
            let class_node = state.push_symbol(
                NodeKind::Class,
                name.clone(),
                Some(qualified_name.clone()),
                Some(span_from(node)),
                None,
                Some(Visibility::Public),
                None,
                class_decorators.to_vec(),
                Vec::new(),
                collect_constructor_dependencies(node, state.source),
            );
            state.set_symbol_implemented_interfaces(class_node.id, implemented_interfaces);
            state.set_symbol_base_classes(class_node.id, base_classes);
            recurse_children(node, |child| {
                visit_python(
                    child,
                    state,
                    Some(&class_node),
                    Some(class_node.id),
                    Some(qualified_name.as_str()),
                    class_decorators,
                    depth + 1,
                )
            });
        }
        "function_definition" => {
            let name =
                child_text(node, "name", state.source).unwrap_or_else(|| "anonymous".to_owned());
            let qualified_name = match owner_qname {
                Some(parent) => format!("{parent}.{name}"),
                None => name.clone(),
            };
            let function_node = state.push_symbol(
                NodeKind::Function,
                name,
                Some(qualified_name.clone()),
                Some(span_from(node)),
                function_signature(node, state.source),
                Some(Visibility::Public),
                parent_class.map(|class_node| class_node.name.clone()),
                class_decorators.to_vec(),
                Vec::new(),
                Vec::new(),
            );
            recurse_children(node, |child| {
                visit_python(
                    child,
                    state,
                    parent_class,
                    Some(function_node.id),
                    Some(qualified_name.as_str()),
                    class_decorators,
                    depth + 1,
                )
            });
        }
        "call" => {
            if let Some(owner_id) = owner
                && let Some(function_node) = node
                    .child_by_field_name("function")
                    .or_else(|| node.child(0))
            {
                let (callee_name, qualified_hint) = expression_name(function_node, state.source);
                let literal_argument = first_literal_argument(node, state.source);
                let raw_arguments = raw_arguments(node, state.source);
                state.push_call_site(
                    owner_id,
                    callee_name,
                    qualified_hint,
                    literal_argument,
                    raw_arguments,
                    node,
                );
            }
            recurse_children(node, |child| {
                visit_python(
                    child,
                    state,
                    parent_class,
                    owner,
                    owner_qname,
                    class_decorators,
                    depth + 1,
                )
            });
        }
        _ => recurse_children(node, |child| {
            visit_python(
                child,
                state,
                parent_class,
                owner,
                owner_qname,
                class_decorators,
                depth + 1,
            )
        }),
    }
}

fn parser_language(file: &FileEntry) -> TsLanguage {
    match file.language {
        Language::TypeScript => {
            if file
                .path
                .extension()
                .and_then(std::ffi::OsStr::to_str)
                .is_some_and(|ext| ext.eq_ignore_ascii_case("tsx"))
            {
                tree_sitter_typescript::LANGUAGE_TSX.into()
            } else {
                tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into()
            }
        }
        Language::JavaScript => tree_sitter_javascript::LANGUAGE.into(),
        Language::Python => tree_sitter_python::LANGUAGE.into(),
        Language::Rust => tree_sitter_rust::LANGUAGE.into(),
        Language::Go => tree_sitter_go::LANGUAGE.into(),
        Language::Java => tree_sitter_java::LANGUAGE.into(),
    }
}

fn recurse_children(node: Node<'_>, mut visit: impl FnMut(Node<'_>)) {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        visit(child);
    }
}

fn collect_decorators(node: Node<'_>, source: &str) -> Vec<DecoratorCapture> {
    let mut decorators = Vec::new();
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "decorator" {
            decorators.push(single_decorator(child, source));
        }
    }
    decorators
}

fn single_decorator(node: Node<'_>, source: &str) -> DecoratorCapture {
    let full_raw = node_text(node, source).trim();
    let expression = node
        .named_child(0)
        .or_else(|| node.child(0))
        .unwrap_or(node);
    let (name, _) = expression_name(expression, source);
    let arg_node = node
        .child_by_field_name("arguments")
        .or_else(|| find_child_by_kind(node, "arguments"))
        .or_else(|| expression.child_by_field_name("arguments"))
        .or_else(|| find_child_by_kind(expression, "arguments"));
    // `raw` is trimmed to the argument expression only (content inside the
    // outermost parens), so callers do not need to strip the decorator name.
    let raw = arg_node.map_or_else(
        || {
            // Fall back: strip `@Name(` prefix and `)` suffix from the full text.
            let after_at = full_raw.strip_prefix('@').unwrap_or(full_raw);
            let after_name = after_at.find('(').map_or("", |i| &after_at[i + 1..]);
            after_name.trim_end_matches(')').trim().to_owned()
        },
        |a| {
            node_text(a, source)
                .trim_start_matches('(')
                .trim_end_matches(')')
                .trim()
                .to_owned()
        },
    );
    let arguments: SmallVec<[Box<str>; 2]> = arg_node
        .map(|a| {
            split_arguments(node_text(a, source))
                .into_iter()
                .map(String::into_boxed_str)
                .collect()
        })
        .unwrap_or_default();
    DecoratorCapture {
        name,
        arguments,
        raw,
    }
}

fn collect_constructor_dependencies(node: Node<'_>, source: &str) -> Vec<String> {
    let mut dependencies = Vec::new();
    let mut stack = vec![node];
    while let Some(current) = stack.pop() {
        let mut cursor = current.walk();
        for child in current.children(&mut cursor) {
            let is_constructor = child.kind() == "constructor"
                || (child.kind() == "method_definition"
                    && child_text(child, "name", source).as_deref() == Some("constructor"));
            let is_python_init = child.kind() == "function_definition"
                && child_text(child, "name", source).as_deref() == Some("__init__");
            if is_python_init {
                dependencies.extend(collect_python_constructor_dependencies(child, source));
            } else if is_constructor {
                let text = node_text(child, source);
                if let Some(open) = text.find('(')
                    && let Some(close) = text[open + 1..].find(')')
                {
                    for parameter in split_top_level_commas(&text[open + 1..open + 1 + close]) {
                        let parameter = parameter.trim();
                        if parameter.is_empty() || matches!(parameter, "self" | "this") {
                            continue;
                        }
                        let name = parameter
                            .split(':')
                            .nth(1)
                            .unwrap_or(parameter)
                            .trim()
                            .trim_start_matches("private")
                            .trim_start_matches("public")
                            .trim_start_matches("protected")
                            .trim_start_matches("readonly")
                            .split_whitespace()
                            .last()
                            .unwrap_or(parameter)
                            .trim_matches('?')
                            .to_owned();
                        if !name.is_empty() {
                            dependencies.push(name);
                        }
                    }
                }
            } else {
                stack.push(child);
            }
        }
    }
    dependencies
}

fn collect_python_constructor_dependencies(function_node: Node<'_>, source: &str) -> Vec<String> {
    let Some(parameters) = find_child_by_kind(function_node, "parameters") else {
        return Vec::new();
    };

    let mut dependencies = Vec::new();
    let mut cursor = parameters.walk();
    for parameter in parameters.named_children(&mut cursor) {
        if parameter.kind() == "identifier" {
            let name = node_text(parameter, source).trim();
            if !matches!(name, "" | "self" | "cls") {
                dependencies.push(name.to_owned());
            }
            continue;
        }
        if parameter.kind() == "keyword_separator" {
            continue;
        }
        let Some(type_node) = parameter
            .child_by_field_name("type")
            .or_else(|| find_child_by_kind(parameter, "type"))
        else {
            continue;
        };
        let type_name = clean_python_type_annotation(node_text(type_node, source));
        if !type_name.is_empty() {
            dependencies.push(type_name);
        }
    }

    dependencies
}

fn collect_implemented_interfaces(node: Node<'_>, source: &str) -> Vec<String> {
    let Ok(text) = node.utf8_text(source.as_bytes()) else {
        return Vec::new();
    };
    let header = text.split('{').next().unwrap_or(text);
    let Some((_, implements_clause)) = header.split_once("implements") else {
        return Vec::new();
    };

    split_top_level_commas(implements_clause)
        .into_iter()
        .filter_map(|entry| {
            let trimmed = entry.trim();
            if trimmed.is_empty() {
                return None;
            }
            let head = trimmed
                .split(['<', ' ', '\n', '\r', '\t'])
                .next()
                .unwrap_or(trimmed)
                .trim_end_matches(',')
                .trim();
            (!head.is_empty()).then(|| head.to_owned())
        })
        .collect()
}

fn collect_python_base_classes(node: Node<'_>, source: &str) -> Vec<String> {
    let Some(argument_list) = find_child_by_kind(node, "argument_list") else {
        return Vec::new();
    };

    let mut bases = Vec::new();
    let mut cursor = argument_list.walk();
    for argument in argument_list.named_children(&mut cursor) {
        if argument.kind() == "keyword_argument" {
            continue;
        }
        let base = clean_python_type_annotation(node_text(argument, source));
        if !base.is_empty() {
            bases.push(base);
        }
    }

    bases
}

fn clean_python_type_annotation(raw: &str) -> String {
    raw.trim()
        .trim_matches('"')
        .trim_matches('\'')
        .trim_end_matches(',')
        .trim()
        .to_owned()
}

fn type_reference_head(reference: &str) -> &str {
    reference
        .trim()
        .split(['<', '[', '(', ' ', '\n', '\r', '\t'])
        .next()
        .unwrap_or(reference)
        .trim_end_matches(',')
        .trim()
}

fn split_top_level_commas(input: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0_usize;
    let mut angle_depth = 0_u32;
    let mut paren_depth = 0_u32;
    let mut brace_depth = 0_u32;
    let mut bracket_depth = 0_u32;

    for (index, ch) in input.char_indices() {
        match ch {
            '<' => angle_depth = angle_depth.saturating_add(1),
            '>' => angle_depth = angle_depth.saturating_sub(1),
            '(' => paren_depth = paren_depth.saturating_add(1),
            ')' => paren_depth = paren_depth.saturating_sub(1),
            '{' => brace_depth = brace_depth.saturating_add(1),
            '}' => brace_depth = brace_depth.saturating_sub(1),
            '[' => bracket_depth = bracket_depth.saturating_add(1),
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            ',' if angle_depth == 0
                && paren_depth == 0
                && brace_depth == 0
                && bracket_depth == 0 =>
            {
                parts.push(&input[start..index]);
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(&input[start..]);
    parts
}

fn parse_import_source(raw: &str) -> Option<String> {
    let source = if let Some(index) = raw.rfind(" from ") {
        raw[index + 6..].trim()
    } else {
        raw.trim_start_matches("import").trim()
    };
    let quote = source
        .chars()
        .find(|character| *character == '"' || *character == '\'')?;
    let rest = source.split_once(quote)?.1;
    let end = rest.find(quote)?;
    Some(rest[..end].to_owned())
}

fn parse_ts_import_bindings(raw: &str, source: &str) -> Vec<ImportBinding> {
    let is_type_only = raw
        .trim_start()
        .strip_prefix("import")
        .is_some_and(|rest| rest.trim_start().starts_with("type "));
    let head = raw
        .trim()
        .trim_start_matches("import")
        .trim_start_matches("export")
        .split(" from ")
        .next()
        .unwrap_or_default()
        .trim();
    // `import type { … }` and `import type Foo` are TS-only forms — strip the
    // `type ` keyword before shape-matching so the named-import parser sees
    // `{ Foo }` instead of `type { Foo }` (and binds `Foo`, not `type { Foo`).
    let head = head.strip_prefix("type ").map_or(head, str::trim_start);
    if head.is_empty() {
        return Vec::new();
    }
    let mut bindings = Vec::new();
    // Check the brace/star forms FIRST — a named-import list like
    // `{ A, B, C }` also contains commas and would otherwise be routed
    // to the default-plus-named branch, where the leading `{ A` gets bound
    // literally as the default import.
    if head.starts_with('{') {
        bindings.extend(parse_named_imports(head, source, is_type_only));
    } else if head.starts_with('*') {
        if let Some(alias) = head.split_whitespace().last() {
            bindings.push(ImportBinding {
                local_name: alias.to_owned(),
                imported_name: None,
                source: source.to_owned(),
                resolved_path: None,
                is_default: false,
                is_namespace: true,
                is_type_only,
            });
        }
    } else if let Some((default_part, rest)) = head.split_once(',') {
        // `import Default, { Named } from …` — comma only appears after the
        // default binding here because the brace form was handled above.
        let default_name = default_part.trim().trim_end_matches(',');
        if !default_name.is_empty() {
            bindings.push(ImportBinding {
                local_name: default_name.to_owned(),
                imported_name: None,
                source: source.to_owned(),
                resolved_path: None,
                is_default: true,
                is_namespace: false,
                is_type_only,
            });
        }
        bindings.extend(parse_named_imports(rest, source, is_type_only));
    } else {
        bindings.push(ImportBinding {
            local_name: head.to_owned(),
            imported_name: None,
            source: source.to_owned(),
            resolved_path: None,
            is_default: true,
            is_namespace: false,
            is_type_only,
        });
    }
    bindings
}

fn parse_named_imports(raw: &str, source: &str, is_type_only: bool) -> Vec<ImportBinding> {
    raw.trim()
        .trim_start_matches('{')
        .trim_end_matches('}')
        .split(',')
        .filter_map(|piece| {
            let piece = piece.trim();
            if piece.is_empty() {
                return None;
            }
            let (imported_name, local_name) = if let Some((left, right)) = piece.split_once(" as ")
            {
                (left.trim().to_owned(), right.trim().to_owned())
            } else {
                (piece.to_owned(), piece.to_owned())
            };
            Some(ImportBinding {
                local_name,
                imported_name: Some(imported_name),
                source: source.to_owned(),
                resolved_path: None,
                is_default: false,
                is_namespace: false,
                is_type_only,
            })
        })
        .collect()
}

fn parse_python_import_groups(
    statement: Node<'_>,
    source_text: &str,
) -> Vec<(String, Vec<ImportBinding>)> {
    match statement.kind() {
        "import_statement" => parse_python_import_statement(statement, source_text),
        "import_from_statement" => parse_python_from_import_statement(statement, source_text),
        _ => Vec::new(),
    }
}

fn parse_python_import_statement(
    statement: Node<'_>,
    source_text: &str,
) -> Vec<(String, Vec<ImportBinding>)> {
    let mut groups = Vec::new();
    let mut cursor = statement.walk();
    for child in statement.named_children(&mut cursor) {
        let Some((imported_name, local_name)) = python_import_item_name(child, source_text) else {
            continue;
        };
        groups.push((
            imported_name.clone(),
            vec![python_import_binding(local_name, None, imported_name, true)],
        ));
    }
    groups
}

fn parse_python_from_import_statement(
    statement: Node<'_>,
    source_text: &str,
) -> Vec<(String, Vec<ImportBinding>)> {
    let Some(module_node) = statement.child_by_field_name("module_name") else {
        return Vec::new();
    };
    let source = normalize_python_module_text(node_text(module_node, source_text));
    if source.is_empty() {
        return Vec::new();
    }

    let mut bindings = Vec::new();
    let module_range = module_node.byte_range();
    let mut cursor = statement.walk();
    for child in statement.named_children(&mut cursor) {
        if child.byte_range() == module_range {
            continue;
        }
        let Some((imported_name, local_name)) = python_import_item_name(child, source_text) else {
            continue;
        };
        bindings.push(python_import_binding(
            local_name,
            Some(imported_name),
            source.clone(),
            false,
        ));
    }

    let raw = node_text(statement, source_text);
    if raw
        .split_once(" import ")
        .is_some_and(|(_, tail)| tail.trim_start().starts_with('*'))
    {
        bindings.push(python_import_binding(
            "*".to_owned(),
            Some("*".to_owned()),
            source.clone(),
            false,
        ));
    }

    if python_module_source_is_dots_only(&source) {
        return bindings
            .into_iter()
            .filter_map(|binding| {
                let imported_name = binding.imported_name.as_deref()?;
                if imported_name == "*" {
                    return Some((source.clone(), vec![binding]));
                }
                let binding_source = format!("{source}{imported_name}");
                Some((
                    binding_source.clone(),
                    vec![ImportBinding {
                        source: binding_source,
                        ..binding
                    }],
                ))
            })
            .collect();
    }

    if bindings.is_empty() {
        Vec::new()
    } else {
        vec![(source, bindings)]
    }
}

fn python_module_source_is_dots_only(source: &str) -> bool {
    !source.is_empty() && source.chars().all(|ch| ch == '.')
}

fn python_import_item_name(item: Node<'_>, source_text: &str) -> Option<(String, String)> {
    match item.kind() {
        "aliased_import" => {
            let imported_name = child_text(item, "name", source_text)
                .map(|value| normalize_python_module_text(&value))?;
            let local_name = child_text(item, "alias", source_text)?;
            Some((imported_name, local_name))
        }
        "dotted_name" => {
            let imported_name = normalize_python_module_text(node_text(item, source_text));
            if imported_name.is_empty() {
                None
            } else {
                let local_name = python_import_local_name(&imported_name);
                Some((imported_name, local_name))
            }
        }
        _ => None,
    }
}

fn normalize_python_module_text(raw: &str) -> String {
    raw.chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>()
}

fn python_import_local_name(imported_name: &str) -> String {
    imported_name
        .rsplit('.')
        .next()
        .filter(|name| !name.is_empty())
        .unwrap_or(imported_name)
        .to_owned()
}

fn python_import_binding(
    local_name: String,
    imported_name: Option<String>,
    source: String,
    is_namespace: bool,
) -> ImportBinding {
    ImportBinding {
        local_name,
        imported_name,
        source,
        resolved_path: None,
        is_default: false,
        is_namespace,
        is_type_only: false,
    }
}

fn parse_python_import_groups_from_raw(raw: &str) -> Vec<(String, Vec<ImportBinding>)> {
    let trimmed = raw.trim();
    if let Some(rest) = trimmed.strip_prefix("from ") {
        let Some((source, imported)) = rest.split_once(" import ") else {
            return Vec::new();
        };
        let source = normalize_python_module_text(source);
        let bindings = imported
            .split(',')
            .filter_map(|piece| {
                let piece = piece
                    .trim()
                    .trim_start_matches('(')
                    .trim_end_matches(')')
                    .trim();
                if piece.is_empty() {
                    return None;
                }
                let (imported_name, local_name) =
                    if let Some((left, right)) = piece.split_once(" as ") {
                        (
                            normalize_python_module_text(left),
                            normalize_python_module_text(right),
                        )
                    } else {
                        let imported_name = normalize_python_module_text(piece);
                        let local_name = python_import_local_name(&imported_name);
                        (imported_name, local_name)
                    };
                Some(python_import_binding(
                    local_name,
                    Some(imported_name),
                    source.clone(),
                    false,
                ))
            })
            .collect::<Vec<_>>();
        return if bindings.is_empty() {
            Vec::new()
        } else {
            vec![(source, bindings)]
        };
    }

    let Some(rest) = trimmed.strip_prefix("import ") else {
        return Vec::new();
    };
    rest.split(',')
        .filter_map(|piece| {
            let piece = piece.trim();
            if piece.is_empty() {
                return None;
            }
            let (source, local_name) = if let Some((left, right)) = piece.split_once(" as ") {
                (
                    normalize_python_module_text(left),
                    normalize_python_module_text(right),
                )
            } else {
                let source = normalize_python_module_text(
                    piece.split_whitespace().next().unwrap_or_default(),
                );
                let local_name = python_import_local_name(&source);
                (source, local_name)
            };
            if source.is_empty() || local_name.is_empty() {
                None
            } else {
                Some((
                    source.clone(),
                    vec![python_import_binding(local_name, None, source, true)],
                ))
            }
        })
        .collect()
}

pub(crate) fn resolve_import_path_pub(
    repo_root: &Path,
    current_file: &Path,
    source: &str,
    language: Language,
    aliases: &PathAliases,
) -> Option<PathBuf> {
    resolve_import_path(repo_root, current_file, source, language, aliases)
}

fn resolve_import_path(
    repo_root: &Path,
    current_file: &Path,
    source: &str,
    language: Language,
    aliases: &PathAliases,
) -> Option<PathBuf> {
    enum ImportBase {
        Relative(PathBuf),
        Absolute(PathBuf),
    }

    let base = if source.starts_with("./") || source.starts_with("../") {
        ImportBase::Relative(
            current_file
                .parent()
                .unwrap_or_else(|| Path::new(""))
                .join(source),
        )
    } else if language == Language::Python && source.starts_with('.') {
        ImportBase::Relative(python_relative_import_path(current_file, source)?)
    } else if let Some(rewritten) = aliases.rewrite(source) {
        // tsconfig.json / workspace alias — prefer when present since it's the
        // repo's declared intent. Most rewrites are repo-relative, but
        // workspace package aliases can point at an absolute sibling package.
        let rewritten = PathBuf::from(rewritten);
        if rewritten.is_absolute() {
            ImportBase::Absolute(rewritten)
        } else {
            ImportBase::Relative(rewritten)
        }
    } else if let Some(alias) = source.strip_prefix("@/") {
        // Fallback for repos without a tsconfig.json (or where tsconfig
        // didn't declare `@/*`): many NestJS projects use the `@/*` →
        // `src/*` convention by default. Drops away entirely once aliases
        // loaded from tsconfig cover `@/`.
        ImportBase::Relative(PathBuf::from("src").join(alias))
    } else if language == Language::Python
        && let Some(absolute) = resolve_python_current_package_import(repo_root, source)
    {
        return Some(absolute);
    } else if language == Language::Python
        && let Some(absolute) =
            resolve_python_sibling_package_import(repo_root, current_file, source)
    {
        return Some(absolute);
    } else if let Some(absolute) = resolve_workspace_or_sibling_package_cached(repo_root, source) {
        return Some(absolute);
    } else if language == Language::Python {
        ImportBase::Relative(PathBuf::from(source.replace('.', "/")))
    } else {
        return None;
    };

    let base_path = match &base {
        ImportBase::Relative(path) | ImportBase::Absolute(path) => path,
    };
    let candidates = match language {
        Language::Python => vec![
            base_path.with_extension("py"),
            base_path.with_extension("pyi"),
            base_path.join("__init__.py"),
            base_path.join("__init__.pyi"),
        ],
        _ => vec![
            base_path.clone(),
            base_path.with_extension("ts"),
            base_path.with_extension("d.ts"),
            base_path.with_extension("tsx"),
            base_path.with_extension("js"),
            base_path.with_extension("jsx"),
            base_path.join("index.d.ts"),
            base_path.join("index.ts"),
            base_path.join("index.tsx"),
            base_path.join("index.js"),
            base_path.join("index.jsx"),
        ],
    };

    candidates
        .into_iter()
        .filter_map(|candidate| match &base {
            ImportBase::Relative(_) => {
                normalize_repo_relative_path(&candidate).map(|path| repo_root.join(path))
            }
            ImportBase::Absolute(_) => Some(candidate),
        })
        .find(|candidate| import_path_exists_inside_allowed_roots(repo_root, candidate))
}

fn python_relative_import_path(current_file: &Path, source: &str) -> Option<PathBuf> {
    let leading_dots = source.chars().take_while(|ch| *ch == '.').count();
    if leading_dots == 0 {
        return None;
    }

    let mut base = current_file
        .parent()
        .unwrap_or_else(|| Path::new(""))
        .to_path_buf();
    for _ in 1..leading_dots {
        if !base.pop() {
            return None;
        }
    }

    let remainder = source.trim_start_matches('.');
    if remainder.is_empty() {
        Some(base)
    } else {
        Some(base.join(remainder.replace('.', "/")))
    }
}

fn normalize_repo_relative_path(path: &Path) -> Option<PathBuf> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(part) => normalized.push(part),
            Component::ParentDir => {
                if !normalized.pop() {
                    return None;
                }
            }
            Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    Some(normalized)
}

fn import_path_exists_inside_allowed_roots(repo_root: &Path, candidate: &Path) -> bool {
    let key = candidate.to_path_buf();
    if let Some(cached) = IMPORT_PATH_EXISTS_CACHE.with_borrow_mut(|cache| cache.get(&key).copied())
    {
        return cached;
    }

    let mut allowed_roots = Vec::new();
    let canonical_repo_root = match fs::canonicalize(repo_root) {
        Ok(path) => path,
        Err(error) => {
            tracing::warn!(
                repo_root = %repo_root.display(),
                error = %error,
                "failed to canonicalize repo root while checking import path; result will not be cached"
            );
            return false;
        }
    };
    allowed_roots.push(canonical_repo_root);
    if let Some(workspace_root) = find_workspace_root(repo_root, 12) {
        match fs::canonicalize(&workspace_root) {
            Ok(canonical_workspace_root) => allowed_roots.push(canonical_workspace_root),
            Err(error) => {
                tracing::warn!(
                    workspace_root = %workspace_root.display(),
                    error = %error,
                    "failed to canonicalize workspace root while checking import path; result will not be cached"
                );
                return canonicalize_existing_file_under_any(candidate, &allowed_roots).is_some();
            }
        }
    }

    let exists = canonicalize_existing_file_under_any(candidate, &allowed_roots).is_some();
    IMPORT_PATH_EXISTS_CACHE.with_borrow_mut(|cache| {
        cache.put(key, exists);
    });
    exists
}

fn resolve_workspace_package_import(repo_root: &Path, source: &str) -> Option<PathBuf> {
    let package_name = shared_package_root(source)?;
    let (workspace_root, package_dir, manifest) = find_workspace_package(repo_root, package_name)?;
    let tail = source
        .strip_prefix(package_name)
        .unwrap_or("")
        .trim_start_matches('/');

    if tail.is_empty() {
        resolve_workspace_package_entry(&workspace_root, &package_dir, &manifest)
    } else {
        resolve_workspace_package_subpath(&workspace_root, &package_dir, &manifest, tail)
    }
}

fn resolve_sibling_package_import(repo_root: &Path, source: &str) -> Option<PathBuf> {
    let package_name = shared_package_root(source)?;
    let tail = source
        .strip_prefix(package_name)
        .unwrap_or("")
        .trim_start_matches('/');
    let canonical_repo_root = match fs::canonicalize(repo_root) {
        Ok(path) => Some(path),
        Err(error) => {
            tracing::warn!(
                repo_root = %repo_root.display(),
                error = %error,
                "failed to canonicalize repo root while resolving sibling package import"
            );
            None
        }
    };

    for root in repo_root.ancestors().skip(1).take(6) {
        let entries = match fs::read_dir(root) {
            Ok(entries) => entries,
            Err(error) => {
                tracing::warn!(
                    dir = %root.display(),
                    error = %error,
                    "failed to enumerate ancestor while resolving sibling package import; skipping"
                );
                continue;
            }
        };
        let mut package_dirs = Vec::new();
        for entry in entries {
            match entry {
                Ok(entry) => package_dirs.push(entry.path()),
                Err(error) => {
                    tracing::warn!(
                        dir = %root.display(),
                        error = %error,
                        "skipping unreadable entry while resolving sibling package import"
                    );
                }
            }
        }
        package_dirs.sort();

        let mut first_match: Option<(PathBuf, PathBuf)> = None;
        for package_dir in package_dirs {
            let Some(package_dir) = canonicalize_existing_dir_under(&package_dir, root) else {
                continue;
            };
            if canonical_repo_root
                .as_ref()
                .is_some_and(|root| &package_dir == root)
            {
                continue;
            }
            let manifest_path = package_dir.join("package.json");
            let Some(manifest_path) =
                canonicalize_existing_file_under(&manifest_path, &package_dir)
            else {
                continue;
            };
            let raw = match fs::read_to_string(&manifest_path) {
                Ok(raw) => raw,
                Err(error) => {
                    tracing::warn!(
                        manifest_path = %manifest_path.display(),
                        error = %error,
                        "failed to read package.json while resolving sibling package import; skipping"
                    );
                    continue;
                }
            };
            let manifest = match serde_json::from_str::<serde_json::Value>(&raw) {
                Ok(manifest) => manifest,
                Err(error) => {
                    tracing::warn!(
                        manifest_path = %manifest_path.display(),
                        error = %error,
                        "failed to parse package.json while resolving sibling package import; skipping"
                    );
                    continue;
                }
            };
            let manifest_name = manifest
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            if manifest_name != package_name {
                continue;
            }
            let resolved = if tail.is_empty() {
                resolve_workspace_package_entry(root, &package_dir, &manifest)
            } else {
                resolve_workspace_package_subpath(root, &package_dir, &manifest, tail)
            };
            let Some(resolved) = resolved else {
                continue;
            };
            if let Some((first_package_dir, first_resolved)) = first_match.as_ref() {
                tracing::warn!(
                    package = package_name,
                    first_package_dir = %first_package_dir.display(),
                    duplicate_package_dir = %package_dir.display(),
                    resolved = %first_resolved.display(),
                    "multiple sibling packages matched import; using first match"
                );
                continue;
            }
            first_match = Some((package_dir, resolved));
        }
        if let Some((_, resolved)) = first_match {
            return Some(resolved);
        }
    }
    None
}

/// Resolve a top-level Python import like `package_a.module_b` against
/// repositories that live next to (or near) `repo_root` in a multi-repo
/// monorepo layout.
///
/// Walks up to [`PYTHON_SIBLING_SEARCH_ANCESTORS`] ancestors of `repo_root`
/// and inspects each ancestor's children for directories that look like
/// Python projects (have a `pyproject.toml`/`setup.py`/`setup.cfg`).  The
/// first match whose package layout (`src/<pkg>/...` or `<pkg>/...`) resolves
/// the import wins.
///
/// The current repo itself is filtered out — see
/// [`resolve_python_current_package_import`] for in-repo resolution.
fn resolve_python_sibling_package_import(
    repo_root: &Path,
    current_file: &Path,
    source: &str,
) -> Option<PathBuf> {
    let (package_name, tail) = python_package_root_and_tail(source)?;
    let canonical_repo_root = match fs::canonicalize(repo_root) {
        Ok(path) => Some(path),
        Err(error) => {
            tracing::warn!(
                repo_root = %repo_root.display(),
                error = %error,
                "failed to canonicalize repo root while resolving Python sibling import"
            );
            None
        }
    };
    let absolute_current_file = absolute_current_file_path(repo_root, current_file);

    for root in repo_root
        .ancestors()
        .skip(1)
        .take(PYTHON_SIBLING_SEARCH_ANCESTORS)
    {
        let entries = match fs::read_dir(root) {
            Ok(entries) => entries,
            Err(error) => {
                tracing::warn!(
                    dir = %root.display(),
                    error = %error,
                    "failed to enumerate ancestor while resolving Python sibling import; skipping"
                );
                continue;
            }
        };
        let mut package_dirs = Vec::new();
        for entry in entries {
            match entry {
                Ok(entry) => package_dirs.push(entry.path()),
                Err(error) => {
                    tracing::warn!(
                        dir = %root.display(),
                        error = %error,
                        "skipping unreadable entry while resolving Python sibling import"
                    );
                }
            }
        }
        package_dirs.sort();

        let mut first_match: Option<(PathBuf, PathBuf)> = None;
        for package_dir in package_dirs {
            let Some(package_dir) = canonicalize_existing_dir_under(&package_dir, root) else {
                continue;
            };
            if canonical_repo_root
                .as_ref()
                .is_some_and(|repo_root| &package_dir == repo_root)
            {
                continue;
            }
            if !has_python_project_marker(&package_dir) {
                continue;
            }
            if let Some(resolved) =
                resolve_python_package_subpath(&package_dir, package_name, tail.as_deref())
            {
                if absolute_current_file.starts_with(&package_dir) {
                    return Some(resolved);
                }
                if let Some((first_package_dir, _first_resolved)) = first_match.as_ref() {
                    tracing::debug!(
                        package = package_name,
                        first_package_dir = %first_package_dir.display(),
                        duplicate_package_dir = %package_dir.display(),
                        "multiple sibling Python packages matched import; leaving import unresolved"
                    );
                    return None;
                }
                first_match = Some((package_dir, resolved));
            }
        }
        if let Some((_, resolved)) = first_match {
            return Some(resolved);
        }
    }

    None
}

fn absolute_current_file_path(repo_root: &Path, current_file: &Path) -> PathBuf {
    let absolute = if current_file.is_absolute() {
        current_file.to_path_buf()
    } else {
        repo_root.join(current_file)
    };
    fs::canonicalize(&absolute).unwrap_or(absolute)
}

/// Resolve an absolute Python import (`my_pkg.submodule`) against the current
/// repo's package layout, supporting both `src/<pkg>/...` and flat
/// `<pkg>/...` conventions.  Used after relative-import handling and before
/// sibling-repo lookup.
fn resolve_python_current_package_import(repo_root: &Path, source: &str) -> Option<PathBuf> {
    let (package_name, tail) = python_package_root_and_tail(source)?;
    let package_dir = fs::canonicalize(repo_root).ok()?;
    if !has_python_project_marker(&package_dir) {
        return None;
    }
    resolve_python_package_subpath(&package_dir, package_name, tail.as_deref())
}

fn has_python_project_marker(package_dir: &Path) -> bool {
    package_dir.join("pyproject.toml").is_file()
        || package_dir.join("setup.py").is_file()
        || package_dir.join("setup.cfg").is_file()
}

fn python_package_root_and_tail(source: &str) -> Option<(&str, Option<PathBuf>)> {
    if source.is_empty()
        || source.starts_with('.')
        || source.starts_with('/')
        || source.contains('/')
        || source.split('.').any(str::is_empty)
    {
        return None;
    }
    let mut parts = source.split('.');
    let package_name = parts.next()?;
    let tail = parts.collect::<PathBuf>();
    let tail = if tail.as_os_str().is_empty() {
        None
    } else {
        Some(tail)
    };
    Some((package_name, tail))
}

fn resolve_python_package_subpath(
    package_dir: &Path,
    package_name: &str,
    tail: Option<&Path>,
) -> Option<PathBuf> {
    let mut bases = Vec::with_capacity(2);
    let mut src_base = package_dir.join("src").join(package_name);
    if let Some(tail) = tail {
        src_base = src_base.join(tail);
    }
    bases.push(src_base);

    let mut flat_base = package_dir.join(package_name);
    if let Some(tail) = tail {
        flat_base = flat_base.join(tail);
    }
    bases.push(flat_base);

    bases
        .into_iter()
        .flat_map(|base| {
            [
                base.with_extension("py"),
                base.with_extension("pyi"),
                base.join("__init__.py"),
                base.join("__init__.pyi"),
            ]
        })
        .find_map(|candidate| canonicalize_existing_file_under(&candidate, package_dir))
}

fn find_workspace_package(
    repo_root: &Path,
    package_name: &str,
) -> Option<(PathBuf, PathBuf, serde_json::Value)> {
    let workspace_root = find_workspace_root(repo_root, 12)?;
    for package_dir in workspace_package_dirs(&workspace_root) {
        let Some(package_dir) = canonicalize_existing_dir_under(&package_dir, &workspace_root)
        else {
            continue;
        };
        let manifest_path = package_dir.join("package.json");
        let Some(manifest_path) = canonicalize_existing_file_under(&manifest_path, &package_dir)
        else {
            continue;
        };
        let Ok(raw) = fs::read_to_string(&manifest_path) else {
            continue;
        };
        let Ok(manifest) = serde_json::from_str::<serde_json::Value>(&raw) else {
            continue;
        };
        let manifest_name = manifest
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        if manifest_name == package_name {
            return Some((workspace_root.clone(), package_dir, manifest));
        }
    }
    None
}

fn workspace_package_dirs(workspace_root: &Path) -> Vec<PathBuf> {
    let mut patterns = Vec::new();
    let root_manifest = workspace_root.join("package.json");
    if let Some(root_manifest) = canonicalize_existing_file_under(&root_manifest, workspace_root)
        && let Ok(raw) = fs::read_to_string(root_manifest)
        && let Ok(manifest) = serde_json::from_str::<serde_json::Value>(&raw)
    {
        if let Some(entries) = manifest
            .get("workspaces")
            .and_then(serde_json::Value::as_array)
        {
            patterns.extend(
                entries
                    .iter()
                    .filter_map(|value| value.as_str().map(ToOwned::to_owned)),
            );
        } else if let Some(entries) = manifest
            .get("workspaces")
            .and_then(|value| value.get("packages"))
            .and_then(serde_json::Value::as_array)
        {
            patterns.extend(
                entries
                    .iter()
                    .filter_map(|value| value.as_str().map(ToOwned::to_owned)),
            );
        }
    }
    let pnpm_manifest = workspace_root.join("pnpm-workspace.yaml");
    if let Some(pnpm_manifest) = canonicalize_existing_file_under(&pnpm_manifest, workspace_root)
        && let Ok(raw) = fs::read_to_string(pnpm_manifest)
        && let Ok(document) = serde_yaml_ng::from_str::<serde_yaml_ng::Value>(&raw)
        && let Some(entries) = document
            .get("packages")
            .and_then(serde_yaml_ng::Value::as_sequence)
    {
        patterns.extend(
            entries
                .iter()
                .filter_map(|value| value.as_str().map(ToOwned::to_owned)),
        );
    }

    let mut dirs = Vec::new();
    for pattern in patterns {
        dirs.extend(expand_workspace_pattern_dirs(workspace_root, &pattern));
    }
    dirs.sort();
    dirs.dedup();
    dirs
}

fn expand_workspace_pattern_dirs(workspace_root: &Path, pattern: &str) -> Vec<PathBuf> {
    let segments: Vec<&str> = pattern
        .trim_start_matches("./")
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();

    if segments.is_empty() {
        return Vec::new();
    }

    if !segments
        .iter()
        .any(|segment| *segment == "*" || segment.contains('*'))
    {
        let candidate = workspace_root.join(segments.iter().collect::<PathBuf>());
        return canonicalize_existing_dir_under(&candidate, workspace_root)
            .map(|dir| vec![dir])
            .unwrap_or_default();
    }

    expand_workspace_segments(workspace_root, &segments)
}

fn expand_workspace_segments(base: &Path, segments: &[&str]) -> Vec<PathBuf> {
    let Some((head, tail)) = segments.split_first() else {
        return if let Some(base) = canonicalize_existing_dir_under(base, base) {
            vec![base]
        } else {
            Vec::new()
        };
    };

    if *head == "*" || head.contains('*') {
        let Ok(entries) = fs::read_dir(base) else {
            return Vec::new();
        };
        let (prefix, suffix) = split_workspace_glob_segment(head);
        let mut results = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(path) = canonicalize_existing_dir_under(&path, base) else {
                continue;
            };
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if workspace_glob_segment_matches(&name, prefix, suffix) {
                results.extend(expand_workspace_segments(&path, tail));
            }
        }
        results
    } else {
        expand_workspace_segments(&base.join(head), tail)
    }
}

fn split_workspace_glob_segment(segment: &str) -> (&str, &str) {
    match segment.split_once('*') {
        Some((prefix, suffix)) => (prefix, suffix),
        None => (segment, ""),
    }
}

fn workspace_glob_segment_matches(name: &str, prefix: &str, suffix: &str) -> bool {
    name.starts_with(prefix) && name.ends_with(suffix)
}

fn resolve_workspace_package_entry(
    workspace_root: &Path,
    package_dir: &Path,
    manifest: &serde_json::Value,
) -> Option<PathBuf> {
    for key in ["types", "typings", "module", "main", "source"] {
        if let Some(path) = manifest.get(key).and_then(serde_json::Value::as_str)
            && let Some(candidate) =
                normalize_workspace_member_path(workspace_root, package_dir, path)
        {
            return Some(candidate);
        }
    }

    if let Some(exports) = manifest.get("exports")
        && let Some(path) = export_target(exports, ".")
        && let Some(candidate) = normalize_workspace_member_path(workspace_root, package_dir, &path)
    {
        return Some(candidate);
    }

    ["src/index.ts", "src/index.tsx", "index.ts", "index.js"]
        .into_iter()
        .find_map(|path| normalize_workspace_member_path(workspace_root, package_dir, path))
}

fn resolve_workspace_package_subpath(
    workspace_root: &Path,
    package_dir: &Path,
    manifest: &serde_json::Value,
    tail: &str,
) -> Option<PathBuf> {
    let normalized_tail = normalize_repo_relative_path(Path::new(tail))?;
    let normalized_tail = normalized_tail.to_string_lossy();

    if let Some(exports) = manifest.get("exports") {
        let exact_key = format!("./{normalized_tail}");
        if let Some(path) = export_target(exports, &exact_key)
            && let Some(candidate) =
                normalize_workspace_member_path(workspace_root, package_dir, &path)
        {
            return Some(candidate);
        }
        if let Some(pattern) = export_target(exports, "./*")
            && let Some((prefix, suffix)) = pattern.split_once('*')
            && let Some(candidate) = normalize_workspace_member_path(
                workspace_root,
                package_dir,
                &format!("{prefix}{normalized_tail}{suffix}"),
            )
        {
            return Some(candidate);
        }
    }

    let mut candidates = Vec::new();
    if let Some(root_entry) = resolve_workspace_package_entry(workspace_root, package_dir, manifest)
    {
        let base_dir = if root_entry
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .is_some_and(|name| name.starts_with("index."))
        {
            root_entry.parent().unwrap_or(package_dir).to_path_buf()
        } else {
            package_dir.to_path_buf()
        };
        candidates.push(base_dir.join(normalized_tail.as_ref()));
    }
    candidates.push(package_dir.join("src").join(normalized_tail.as_ref()));
    candidates.push(package_dir.join(normalized_tail.as_ref()));

    candidates.into_iter().find_map(|candidate| {
        if let Some(candidate) =
            safe_workspace_member_candidate(workspace_root, package_dir, &candidate)
        {
            return Some(candidate);
        }
        for ext in ["ts", "d.ts", "tsx", "js", "jsx"] {
            let with_ext = candidate.with_extension(ext);
            if let Some(with_ext) =
                safe_workspace_member_candidate(workspace_root, package_dir, &with_ext)
            {
                return Some(with_ext);
            }
        }
        [
            "index.ts",
            "index.d.ts",
            "index.tsx",
            "index.js",
            "index.jsx",
        ]
        .into_iter()
        .map(|index| candidate.join(index))
        .find_map(|path| safe_workspace_member_candidate(workspace_root, package_dir, &path))
    })
}

fn export_target(exports: &serde_json::Value, key: &str) -> Option<String> {
    match exports {
        serde_json::Value::String(value) if key == "." => Some(value.clone()),
        serde_json::Value::Object(map) => {
            let entry = map.get(key)?;
            match entry {
                serde_json::Value::String(value) => Some(value.clone()),
                serde_json::Value::Object(obj) => ["types", "import", "default", "require"]
                    .into_iter()
                    .find_map(|field| obj.get(field).and_then(serde_json::Value::as_str))
                    .map(ToOwned::to_owned),
                _ => None,
            }
        }
        _ => None,
    }
}

fn normalize_workspace_member_path(
    workspace_root: &Path,
    package_dir: &Path,
    member_path: &str,
) -> Option<PathBuf> {
    let relative = normalize_repo_relative_path(Path::new(member_path.trim_start_matches("./")))?;
    let candidate = package_dir.join(relative);
    safe_workspace_member_candidate(workspace_root, package_dir, &candidate)
}

fn safe_workspace_member_candidate(
    workspace_root: &Path,
    package_dir: &Path,
    candidate: &Path,
) -> Option<PathBuf> {
    let canonical_workspace = fs::canonicalize(workspace_root).ok()?;
    let canonical = canonicalize_existing_file_under(candidate, package_dir)?;
    canonical
        .starts_with(canonical_workspace)
        .then_some(canonical)
}

fn append_unique_nodes(existing: &mut Vec<NodeData>, additions: Vec<NodeData>) {
    let mut seen: FxHashSet<_> = existing.iter().map(|node| node.id).collect();
    for node in additions {
        if seen.insert(node.id) {
            existing.push(node);
        }
    }
}

fn apply_workspace_semantic_edges(parsed: &mut ParsedFile, repo_root: &Path) {
    let mut added_nodes = Vec::new();
    let mut added_edges = Vec::new();
    let mut seen_node_ids = parsed
        .nodes
        .iter()
        .map(|node| node.id)
        .collect::<FxHashSet<_>>();
    let mut seen_edge_keys = parsed
        .edges
        .iter()
        .map(|edge| (edge.source, edge.target, edge.kind, edge.owner_file))
        .collect::<FxHashSet<_>>();

    for binding in &parsed.import_bindings {
        if let Some((node, edge)) = external_import_file_edge(parsed, repo_root, binding) {
            if should_add_semantic_node(parsed, &node) && seen_node_ids.insert(node.id) {
                added_nodes.push(node);
            }
            let key = (edge.source, edge.target, edge.kind, edge.owner_file);
            if seen_edge_keys.insert(key) {
                added_edges.push(edge);
            }
        }

        if !binding.is_type_only && !is_contract_like_import_binding(repo_root, binding) {
            continue;
        }
        if let Some((node, edge)) = semantic_import_edge(parsed, repo_root, binding) {
            if should_add_semantic_node(parsed, &node) && seen_node_ids.insert(node.id) {
                added_nodes.push(node);
            }
            let key = (edge.source, edge.target, edge.kind, edge.owner_file);
            if seen_edge_keys.insert(key) {
                added_edges.push(edge);
            }
        }
    }

    for symbol in &parsed.symbols {
        if symbol.node.kind != NodeKind::Class {
            continue;
        }
        for interface_name in &symbol.implemented_interfaces {
            let Some((binding, imported_symbol_name)) =
                parsed.import_bindings.iter().find_map(|binding| {
                    interface_symbol_from_binding(binding, interface_name)
                        .map(|symbol_name| (binding, symbol_name))
                })
            else {
                continue;
            };

            let Some(node) =
                semantic_type_node_from_binding(parsed, repo_root, binding, imported_symbol_name)
            else {
                continue;
            };

            if should_add_semantic_node(parsed, &node) && seen_node_ids.insert(node.id) {
                added_nodes.push(node.clone());
            }
            let edge = EdgeData {
                source: symbol.node.id,
                target: node.id,
                kind: EdgeKind::ImplementsContractFrom,
                metadata: EdgeMetadata::default(),
                owner_file: parsed.file_node.id,
                is_cross_file: true,
            };
            let key = (edge.source, edge.target, edge.kind, edge.owner_file);
            if seen_edge_keys.insert(key) {
                added_edges.push(edge);
            }
            if binding.is_type_only {
                let edge = EdgeData {
                    source: parsed.file_node.id,
                    target: node.id,
                    kind: EdgeKind::UsesTypeFrom,
                    metadata: EdgeMetadata::default(),
                    owner_file: parsed.file_node.id,
                    is_cross_file: true,
                };
                let key = (edge.source, edge.target, edge.kind, edge.owner_file);
                if seen_edge_keys.insert(key) {
                    added_edges.push(edge);
                }
            }
        }

        for base_class in &symbol.base_classes {
            let base_head = type_reference_head(base_class);
            if base_head.is_empty() {
                continue;
            }

            let node = if let Some(node) = same_file_class_node(parsed, symbol.node.id, base_head) {
                node
            } else if let Some((binding, imported_symbol_name)) =
                parsed.import_bindings.iter().find_map(|binding| {
                    interface_symbol_from_binding(binding, base_head)
                        .map(|symbol_name| (binding, symbol_name))
                })
            {
                let Some(node) = semantic_type_node_from_binding(
                    parsed,
                    repo_root,
                    binding,
                    imported_symbol_name,
                ) else {
                    continue;
                };
                node
            } else {
                continue;
            };

            if should_add_semantic_node(parsed, &node) && seen_node_ids.insert(node.id) {
                added_nodes.push(node.clone());
            }
            let edge = EdgeData {
                source: symbol.node.id,
                target: node.id,
                kind: EdgeKind::Extends,
                metadata: EdgeMetadata::default(),
                owner_file: parsed.file_node.id,
                is_cross_file: node.file_path != symbol.node.file_path,
            };
            let key = (edge.source, edge.target, edge.kind, edge.owner_file);
            if seen_edge_keys.insert(key) {
                added_edges.push(edge);
            }
        }
    }

    append_unique_nodes(&mut parsed.nodes, added_nodes);
    parsed.edges.extend(added_edges);
}

fn should_add_semantic_node(parsed: &ParsedFile, node: &NodeData) -> bool {
    node.repo == parsed.file_node.repo || node.is_virtual
}

fn semantic_import_edge(
    parsed: &ParsedFile,
    repo_root: &Path,
    binding: &ImportBinding,
) -> Option<(NodeData, EdgeData)> {
    let source_file_utf8 = path_to_utf8(&parsed.file.path);
    let source_file_path = source_file_utf8.as_str();

    if is_python_shared_contract_source(&binding.source)
        && let Some(resolved) = binding.resolved_path.as_ref()
        && let Some(target_file) = external_file_node_from_resolved_path(repo_root, resolved)
    {
        let edge = EdgeData {
            source: parsed.file_node.id,
            target: target_file.id,
            kind: EdgeKind::UsesTypeFrom,
            metadata: EdgeMetadata::default(),
            owner_file: parsed.file_node.id,
            is_cross_file: true,
        };
        return Some((target_file, edge));
    }

    if let Some(shared_node) = shared_contract_node_from_binding(
        parsed,
        binding,
        binding
            .imported_name
            .as_deref()
            .unwrap_or(binding.local_name.as_str()),
    ) {
        let edge = EdgeData {
            source: parsed.file_node.id,
            target: shared_node.id,
            kind: EdgeKind::UsesTypeFrom,
            metadata: EdgeMetadata::default(),
            owner_file: parsed.file_node.id,
            is_cross_file: true,
        };
        return Some((shared_node, edge));
    }

    let resolved = binding.resolved_path.as_ref()?;
    let relative = resolved.strip_prefix(repo_root).ok()?;
    let relative_utf8 = path_to_utf8(relative);
    let relative_str = relative_utf8.as_str();
    let target_file = NodeData {
        id: node_id(
            &parsed.file_node.repo,
            relative_str,
            NodeKind::File,
            relative_str,
        ),
        kind: NodeKind::File,
        repo: parsed.file_node.repo.clone(),
        file_path: relative_str.to_owned(),
        name: relative_str.to_owned(),
        qualified_name: Some(format!("{}::{relative_str}", parsed.file_node.repo)),
        external_id: None,
        signature: None,
        visibility: None,
        span: None,
        is_virtual: false,
    };
    let edge = EdgeData {
        source: parsed.file_node.id,
        target: target_file.id,
        kind: EdgeKind::UsesTypeFrom,
        metadata: EdgeMetadata::default(),
        owner_file: parsed.file_node.id,
        is_cross_file: relative_str != source_file_path,
    };
    Some((target_file, edge))
}

fn external_import_file_edge(
    parsed: &ParsedFile,
    repo_root: &Path,
    binding: &ImportBinding,
) -> Option<(NodeData, EdgeData)> {
    let target_file =
        external_file_node_from_resolved_path(repo_root, binding.resolved_path.as_ref()?)?;
    let edge = EdgeData {
        source: parsed.file_node.id,
        target: target_file.id,
        kind: EdgeKind::Imports,
        metadata: EdgeMetadata::default(),
        owner_file: parsed.file_node.id,
        is_cross_file: true,
    };
    Some((target_file, edge))
}

fn external_file_node_from_resolved_path(repo_root: &Path, resolved: &Path) -> Option<NodeData> {
    let (repo, relative) = external_repo_file_identity(repo_root, resolved)?;
    let relative_utf8 = path_to_utf8(&relative);
    let relative_str = relative_utf8.as_str();
    Some(NodeData {
        id: node_id(&repo, relative_str, NodeKind::File, relative_str),
        kind: NodeKind::File,
        repo: repo.clone(),
        file_path: relative_str.to_owned(),
        name: relative_str.to_owned(),
        qualified_name: Some(format!("{repo}::{relative_str}")),
        external_id: None,
        signature: None,
        visibility: None,
        span: None,
        is_virtual: false,
    })
}

/// Resolve the (repo name, repo-relative path) of an externally-owned file.
///
/// Tier 1: configured `gather-step.config.yaml` lookup (the 5a5563a fix).
/// Tier 2: walk ancestors looking for a project marker
/// (`pyproject.toml`, `setup.py`, `setup.cfg`, `package.json`).  When the
/// marker is `pyproject.toml`, prefer the declared `[project].name`; otherwise
/// fall back to the directory basename.
/// Tier 3: scan siblings of the *current* repo's parent for a directory that
/// contains the resolved file.  Same name-resolution policy as tier 2.
fn external_repo_file_identity(repo_root: &Path, resolved: &Path) -> Option<(String, PathBuf)> {
    let canonical_resolved = fs::canonicalize(resolved).ok()?;
    let canonical_repo_root = fs::canonicalize(repo_root).ok()?;
    if canonical_resolved.starts_with(&canonical_repo_root) {
        return None;
    }

    if let Some(identity) = configured_external_repo_file_identity(repo_root, &canonical_resolved) {
        return Some(identity);
    }

    for ancestor in canonical_resolved.ancestors().skip(1) {
        if ancestor == canonical_repo_root {
            return None;
        }
        if ancestor.join("pyproject.toml").is_file()
            || ancestor.join("setup.py").is_file()
            || ancestor.join("setup.cfg").is_file()
            || ancestor.join("package.json").is_file()
        {
            let repo = repo_name_from_manifest_dir(ancestor)?;
            let relative = canonical_resolved
                .strip_prefix(ancestor)
                .ok()?
                .to_path_buf();
            return Some((repo, relative));
        }
    }

    let workspace_root = canonical_repo_root.parent()?;
    let entries = match fs::read_dir(workspace_root) {
        Ok(entries) => entries,
        Err(error) => {
            tracing::warn!(
                workspace_root = %workspace_root.display(),
                error = %error,
                "failed to enumerate workspace root while resolving external repo identity; cross-repo edge will be unresolved"
            );
            return None;
        }
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                tracing::warn!(
                    workspace_root = %workspace_root.display(),
                    error = %error,
                    "skipping unreadable entry while resolving external repo identity"
                );
                continue;
            }
        };
        let package_dir = entry.path();
        let Some(package_dir) = canonicalize_existing_dir_under(&package_dir, workspace_root)
        else {
            continue;
        };
        if package_dir == canonical_repo_root || !canonical_resolved.starts_with(&package_dir) {
            continue;
        }
        let repo = repo_name_from_manifest_dir(&package_dir)?;
        let relative = canonical_resolved
            .strip_prefix(&package_dir)
            .ok()?
            .to_path_buf();
        return Some((repo, relative));
    }

    None
}

/// Best-effort repo name for a directory that contains a recognized project
/// manifest.  Prefers `pyproject.toml`'s `[project].name` so a Python repo
/// whose package name differs from its directory basename produces stable
/// cross-repo node IDs.  Falls back to the directory basename for non-Python
/// manifests or when the TOML cannot be parsed.
fn repo_name_from_manifest_dir(dir: &Path) -> Option<String> {
    if let Some(name) = python_project_name_from_pyproject(&dir.join("pyproject.toml")) {
        return Some(name);
    }
    Some(dir.file_name()?.to_string_lossy().into_owned())
}

fn python_project_name_from_pyproject(pyproject_path: &Path) -> Option<String> {
    if !pyproject_path.is_file() {
        return None;
    }
    let text = match fs::read_to_string(pyproject_path) {
        Ok(text) => text,
        Err(error) => {
            tracing::warn!(
                pyproject = %pyproject_path.display(),
                error = %error,
                "failed to read pyproject.toml while deriving repo name; falling back to directory basename"
            );
            return None;
        }
    };
    let value = match text.parse::<toml::Value>() {
        Ok(value) => value,
        Err(error) => {
            tracing::warn!(
                pyproject = %pyproject_path.display(),
                error = %error,
                "failed to parse pyproject.toml while deriving repo name; falling back to directory basename"
            );
            return None;
        }
    };
    let name = value
        .get("project")
        .and_then(|project| project.get("name"))
        .and_then(toml::Value::as_str)?
        .trim();
    if name.is_empty() {
        return None;
    }
    Some(name.to_owned())
}

fn configured_external_repo_file_identity(
    repo_root: &Path,
    canonical_resolved: &Path,
) -> Option<(String, PathBuf)> {
    let canonical_repo_root = fs::canonicalize(repo_root).ok()?;
    let repos = configured_workspace_repo_identities(&canonical_repo_root)?;
    for repo in repos {
        if repo.root() == canonical_repo_root || !canonical_resolved.starts_with(repo.root()) {
            continue;
        }
        let relative = canonical_resolved
            .strip_prefix(repo.root())
            .ok()?
            .to_path_buf();
        return Some((repo.name().to_owned(), relative));
    }
    None
}

fn configured_workspace_repo_identities(repo_root: &Path) -> Option<Vec<WorkspaceRepoIdentity>> {
    // Only cache under a canonical key.  A non-canonical fallback key would
    // miss on every subsequent call (the file-system view of `repo_root` is
    // canonicalized by inner helpers), forcing the identity loader to re-run
    // for every import in the file.
    let Ok(key) = fs::canonicalize(repo_root) else {
        return load_configured_workspace_repo_identities(repo_root);
    };
    if let Some(hit) =
        WORKSPACE_REPO_IDENTITY_CACHE.with_borrow_mut(|cache| cache.get(&key).cloned())
    {
        return hit;
    }

    let identities = load_configured_workspace_repo_identities(repo_root);
    if identities.is_some() {
        WORKSPACE_REPO_IDENTITY_CACHE.with_borrow_mut(|cache| {
            cache.put(key, identities.clone());
        });
    }
    identities
}

fn load_configured_workspace_repo_identities(
    repo_root: &Path,
) -> Option<Vec<WorkspaceRepoIdentity>> {
    let config_root = find_gather_step_config_root(repo_root, GATHER_STEP_CONFIG_MAX_ASCEND)?;
    let canonical_config_root = match fs::canonicalize(&config_root) {
        Ok(path) => path,
        Err(error) => {
            tracing::warn!(
                config_root = %config_root.display(),
                error = %error,
                "failed to canonicalize gather-step config root; falling back to directory-basename heuristic"
            );
            return None;
        }
    };
    let config_path = config_root.join("gather-step.config.yaml");
    let config = match GatherStepConfig::from_yaml_file(&config_path) {
        Ok(config) => config,
        Err(error) => {
            tracing::warn!(
                config_path = %config_path.display(),
                error = %error,
                "failed to parse gather-step.config.yaml; falling back to directory-basename heuristic"
            );
            return None;
        }
    };
    let total = config.repos.len();
    let mut repos = Vec::with_capacity(total);
    let mut seen_names = FxHashSet::default();
    for repo in config.repos {
        let absolute = config_root.join(&repo.path);
        let canonical_repo_root = match fs::canonicalize(&absolute) {
            Ok(path) => path,
            Err(error) => {
                tracing::warn!(
                    repo = %repo.name,
                    repo_path = %absolute.display(),
                    error = %error,
                    "failed to canonicalize configured repo path; cross-repo edges from this repo will fall back to directory-basename heuristic"
                );
                continue;
            }
        };
        if !canonical_repo_root.starts_with(&canonical_config_root) {
            tracing::warn!(
                repo = %repo.name,
                repo_root = %canonical_repo_root.display(),
                config_root = %canonical_config_root.display(),
                "configured repo root escapes the gather-step config root; ignoring"
            );
            continue;
        }
        let Some(identity) = WorkspaceRepoIdentity::new(&repo.name, canonical_repo_root) else {
            tracing::warn!(
                repo = %repo.name,
                "configured repo entry has empty name or non-absolute root after canonicalization; ignoring"
            );
            continue;
        };
        if !seen_names.insert(identity.name().to_owned()) {
            tracing::warn!(
                repo = %identity.name(),
                config_path = %config_path.display(),
                "duplicate configured repo name; ignoring later entry"
            );
            continue;
        }
        repos.push(identity);
    }
    if repos.is_empty() {
        if total > 0 {
            tracing::warn!(
                config_path = %config_path.display(),
                "gather-step.config.yaml present but no repo entries resolved; falling back to directory-basename heuristic"
            );
        }
        None
    } else {
        Some(repos)
    }
}

fn find_gather_step_config_root(start_dir: &Path, max_depth: usize) -> Option<PathBuf> {
    let mut current = start_dir;
    for _ in 0..=max_depth {
        if current.join("gather-step.config.yaml").is_file() {
            return Some(current.to_path_buf());
        }
        match current.parent() {
            Some(parent) => current = parent,
            None => break,
        }
    }
    None
}

fn semantic_type_node_from_binding(
    parsed: &ParsedFile,
    repo_root: &Path,
    binding: &ImportBinding,
    symbol_name: &str,
) -> Option<NodeData> {
    if let Some(shared_node) = shared_contract_node_from_binding(parsed, binding, symbol_name) {
        return Some(shared_node);
    }

    let resolved = binding.resolved_path.as_ref()?;
    let relative = resolved.strip_prefix(repo_root).ok()?;
    let relative_utf8 = path_to_utf8(relative);
    let relative_str = relative_utf8.as_str();
    Some(NodeData {
        id: node_id(
            &parsed.file_node.repo,
            relative_str,
            NodeKind::File,
            relative_str,
        ),
        kind: NodeKind::File,
        repo: parsed.file_node.repo.clone(),
        file_path: relative_str.to_owned(),
        name: relative_str.to_owned(),
        qualified_name: Some(format!("{}::{relative_str}", parsed.file_node.repo)),
        external_id: None,
        signature: None,
        visibility: None,
        span: None,
        is_virtual: false,
    })
}

fn same_file_class_node(
    parsed: &ParsedFile,
    source_id: gather_step_core::NodeId,
    base_name: &str,
) -> Option<NodeData> {
    parsed
        .symbols
        .iter()
        .find(|symbol| {
            symbol.node.kind == NodeKind::Class
                && symbol.node.id != source_id
                && symbol.node.name == base_name
        })
        .map(|symbol| symbol.node.clone())
}

fn shared_contract_node_from_binding(
    parsed: &ParsedFile,
    binding: &ImportBinding,
    symbol_name: &str,
) -> Option<NodeData> {
    if binding.is_default {
        return None;
    }
    let package = shared_package_root(&binding.source)?;
    if !is_shared_contract_package(package) && !resolved_path_looks_contract_like(binding) {
        return None;
    }
    let imported_name = if binding.is_namespace {
        symbol_name.trim()
    } else {
        binding
            .imported_name
            .as_deref()
            .unwrap_or(symbol_name)
            .trim()
    };
    if imported_name.is_empty() {
        return None;
    }
    let file_path_utf8 = path_to_utf8(&parsed.file.path);
    let qualified_name = shared_symbol_qn_unversioned(package, imported_name);
    Some(virtual_node(
        NodeKind::SharedSymbol,
        parsed.file_node.repo.clone(),
        file_path_utf8.as_str().to_owned(),
        imported_name.to_owned(),
        qualified_name,
    ))
}

fn interface_symbol_from_binding<'a>(
    binding: &'a ImportBinding,
    interface_name: &'a str,
) -> Option<&'a str> {
    let interface_name = type_reference_head(interface_name);
    if binding.is_namespace {
        interface_name
            .strip_prefix(binding.local_name.as_str())
            .and_then(|rest| rest.strip_prefix('.'))
            .map(str::trim)
            .filter(|symbol| !symbol.is_empty())
    } else if binding.local_name == interface_name {
        Some(interface_name)
    } else {
        None
    }
}

fn is_shared_contract_package(package: &str) -> bool {
    package.starts_with("@workspace/")
        || package.starts_with("@shared/")
        || package.contains("shared")
        || package.contains("contract")
        || package.contains("schema")
        || package.contains("types")
}

fn is_contract_like_import_binding(repo_root: &Path, binding: &ImportBinding) -> bool {
    if source_path_looks_contract_like(&binding.source)
        && shared_package_root(&binding.source).is_some()
    {
        return true;
    }
    if is_python_shared_contract_source(&binding.source) {
        return true;
    }

    let Some(resolved) = binding.resolved_path.as_ref() else {
        return false;
    };
    !resolved.starts_with(repo_root) && resolved_path_looks_contract_like(binding)
}

fn is_python_shared_contract_source(source: &str) -> bool {
    python_package_root_and_tail(source)
        .is_some_and(|(package, _)| is_shared_contract_package(package))
}

fn resolved_path_looks_contract_like(binding: &ImportBinding) -> bool {
    binding
        .resolved_path
        .as_ref()
        .is_some_and(|path| path_looks_contract_like(path))
        || source_path_looks_contract_like(&binding.source)
}

#[expect(
    clippy::disallowed_methods,
    reason = "contract import detection needs one-shot lowercase normalization for substring checks"
)]
#[expect(
    clippy::case_sensitive_file_extension_comparisons,
    reason = "specifier suffixes are normalized to lowercase before matching `.type`/`.dto` markers"
)]
fn source_path_looks_contract_like(source: &str) -> bool {
    let normalized = source.to_ascii_lowercase();
    normalized.contains("/types/")
        || normalized.ends_with("/types")
        || normalized.contains("/dtos/")
        || normalized.ends_with("/dtos")
        || normalized.contains("/contracts/")
        || normalized.ends_with("/contracts")
        || normalized.contains("/schemas/")
        || normalized.ends_with("/schemas")
        || normalized.ends_with(".type")
        || normalized.ends_with(".dto")
        || normalized.ends_with(".schema")
}

#[expect(
    clippy::disallowed_methods,
    reason = "contract import detection needs one-shot lowercase normalization for substring checks"
)]
fn path_looks_contract_like(path: &Path) -> bool {
    let normalized = path_to_utf8(path).as_str().to_ascii_lowercase();
    normalized.contains("/types/")
        || normalized.contains("/types/index.")
        || normalized.contains("/dtos/")
        || normalized.contains("/dtos/index.")
        || normalized.contains("/contracts/")
        || normalized.contains("/contracts/index.")
        || normalized.contains("/schemas/")
        || normalized.contains("/schemas/index.")
        || normalized.ends_with(".type.ts")
        || normalized.ends_with(".type.tsx")
        || normalized.ends_with(".type.js")
        || normalized.ends_with(".type.jsx")
        || normalized.ends_with(".dto.ts")
        || normalized.ends_with(".dto.tsx")
        || normalized.ends_with(".dto.js")
        || normalized.ends_with(".dto.jsx")
        || normalized.ends_with(".schema.ts")
        || normalized.ends_with(".schema.tsx")
        || normalized.ends_with(".schema.js")
        || normalized.ends_with(".schema.jsx")
}

/// Map a detected [`Framework`] variant to the one or more [`PackId`]s that
/// drive parser augmentation.
///
/// Used inside [`parse_file_with_context`] to convert auto-detection output
/// into the pack IDs consumed by the augmentation layer.
fn framework_to_pack_ids(fw: Framework) -> &'static [PackId] {
    match fw {
        Framework::NestJs => &[PackId::Nestjs],
        Framework::Mongoose => &[PackId::Mongoose],
        Framework::NextJs => &[PackId::Nextjs],
        Framework::Tailwind => &[PackId::Tailwind],
        Framework::Prisma => &[PackId::Prisma],
        Framework::Drizzle => &[PackId::Drizzle],
        Framework::TypeOrm => &[PackId::TypeOrm],
        Framework::React => &[PackId::React],
        Framework::ReactRouter => &[PackId::ReactRouter],
        Framework::ReactHookForm => &[PackId::ReactHookForm],
        Framework::Storybook => &[PackId::Storybook],
        Framework::Azure => &[PackId::Azure],
        Framework::Redux => &[PackId::Redux],
        Framework::Zustand => &[PackId::Zustand],
        Framework::LaunchDarkly => &[PackId::LaunchDarkly],
        Framework::FastApi => &[PackId::Fastapi],
        Framework::FrontendHooks => &[PackId::FrontendHooks],
    }
}

fn child_text(node: Node<'_>, field_name: &str, source: &str) -> Option<String> {
    node.child_by_field_name(field_name).map(|child| {
        node_text(child, source)
            .trim_matches('"')
            .trim_matches('\'')
            .to_owned()
    })
}

fn node_text<'a>(node: Node<'_>, source: &'a str) -> &'a str {
    source.get(node.byte_range()).unwrap_or("")
}

fn find_child_by_kind<'a>(node: Node<'a>, kind: &str) -> Option<Node<'a>> {
    let mut cursor = node.walk();
    node.children(&mut cursor)
        .find(|child| child.kind() == kind)
}

fn span_from(node: Node<'_>) -> SourceSpan {
    let start = node.start_position();
    let end = node.end_position();
    let line_start = u32::try_from(start.row + 1).unwrap_or(u32::MAX);
    let line_end = u32::try_from(end.row + 1).unwrap_or(u32::MAX);
    let column_start = u32::try_from(start.column).unwrap_or(u32::MAX);
    let column_end = u32::try_from(end.column).unwrap_or(u32::MAX);
    SourceSpan::from_absolute(line_start, line_end, column_start, column_end)
}

fn is_exported(node: Node<'_>) -> bool {
    node.parent()
        .is_some_and(|parent| parent.kind() == "export_statement")
}

fn method_visibility(node: Node<'_>, source: &str) -> Visibility {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        match child.kind() {
            "accessibility_modifier" => match node_text(child, source).trim() {
                "private" => return Visibility::Private,
                "protected" => return Visibility::Protected,
                "public" => return Visibility::Public,
                _ => {}
            },
            "public" => return Visibility::Public,
            "private" => return Visibility::Private,
            "protected" => return Visibility::Protected,
            _ => {}
        }
    }
    Visibility::Public
}

fn function_signature(node: Node<'_>, source: &str) -> Option<String> {
    let name = child_text(node, "name", source)?;
    let params = node.child_by_field_name("parameters").map_or_else(
        || "()".to_owned(),
        |params| node_text(params, source).to_owned(),
    );
    let return_type = node
        .child_by_field_name("return_type")
        .map(|return_type| format!(" -> {}", node_text(return_type, source)))
        .unwrap_or_default();
    let async_prefix = if node_text(node, source).trim_start().starts_with("async ") {
        "async "
    } else {
        ""
    };
    Some(format!("{async_prefix}{name}{params}{return_type}"))
}

fn expression_name(node: Node<'_>, source: &str) -> (String, Option<String>) {
    let mut current = node;
    let mut parts = Vec::new();

    loop {
        match current.kind() {
            "member_expression" | "attribute" => {
                let property = current
                    .child_by_field_name("property")
                    .or_else(|| current.child_by_field_name("attribute"))
                    .or_else(|| {
                        u32::try_from(current.child_count().saturating_sub(1))
                            .ok()
                            .and_then(|index| current.child(index))
                    })
                    .map(|child| node_text(child, source).trim().to_owned())
                    .unwrap_or_default();
                if !property.is_empty() {
                    parts.push(property);
                }
                if let Some(object) = current
                    .child_by_field_name("object")
                    .or_else(|| current.child(0))
                {
                    current = object;
                    continue;
                }
                break;
            }
            "call_expression" | "call" => {
                if let Some(function) = current
                    .child_by_field_name("function")
                    .or_else(|| current.child(0))
                {
                    current = function;
                    continue;
                }
                break;
            }
            _ => {
                let text = node_text(current, source).trim().to_owned();
                if !text.is_empty() {
                    parts.push(text);
                }
                break;
            }
        }
    }

    if parts.is_empty() {
        return (String::new(), None);
    }

    parts.reverse();
    let name = parts.last().cloned().unwrap_or_default();
    let qualified = parts.join(".");
    (name, Some(qualified))
}

fn first_literal_argument(node: Node<'_>, source: &str) -> Option<String> {
    let arguments = node
        .child_by_field_name("arguments")
        .or_else(|| find_child_by_kind(node, "arguments"))?;
    let mut cursor = arguments.walk();
    let named_children = arguments
        .children(&mut cursor)
        .filter(tree_sitter::Node::is_named)
        .collect::<Vec<_>>();
    for child in &named_children {
        match child.kind() {
            "string" | "string_fragment" => {
                return Some(
                    node_text(*child, source)
                        .trim_matches('"')
                        .trim_matches('\'')
                        .to_owned(),
                );
            }
            "array" => {
                let text = node_text(*child, source).trim();
                return Some(text.trim_matches('[').trim_matches(']').to_owned());
            }
            _ => {}
        }
    }
    named_children
        .first()
        .map(|child| node_text(*child, source).trim().to_owned())
}

fn raw_arguments(node: Node<'_>, source: &str) -> Option<String> {
    let arguments = node
        .child_by_field_name("arguments")
        .or_else(|| find_child_by_kind(node, "arguments"))?;
    Some(
        node_text(arguments, source)
            .trim_matches('(')
            .trim_matches(')')
            .trim()
            .to_owned(),
    )
}

fn split_arguments(raw: &str) -> Vec<String> {
    raw.trim()
        .trim_start_matches('(')
        .trim_end_matches(')')
        .split(',')
        .map(|piece| piece.trim().trim_matches('"').trim_matches('\'').to_owned())
        .filter(|piece| !piece.is_empty())
        .collect()
}

fn extract_constant_string_value(
    base_name: &str,
    value: Node<'_>,
    source: &str,
) -> Option<Vec<(String, String)>> {
    match value.kind() {
        "string" => Some(vec![(
            base_name.to_owned(),
            sanitize_string_literal(node_text(value, source)),
        )]),
        "object" => {
            let mut constants = Vec::new();
            extract_object_constants(base_name, value, source, &mut constants);
            Some(constants)
        }
        _ => None,
    }
}

fn record_default_export_constants(node: Node<'_>, state: &mut ParseState<'_>) {
    let raw = node_text(node, state.source);
    if !raw.trim_start().starts_with("export default") {
        return;
    }

    let Some(value) = default_export_value_node(node) else {
        return;
    };

    if let Some(constant_value) = extract_constant_string_value("default", value, state.source) {
        for (key, value) in constant_value {
            state.record_constant_string(key, value);
        }
        return;
    }

    if value.kind() == "identifier" {
        let alias = node_text(value, state.source).trim().to_owned();
        if !alias.is_empty() {
            mirror_constant_prefix(&mut state.constant_strings, &alias, "default");
        }
    }
}

fn default_export_value_node(node: Node<'_>) -> Option<Node<'_>> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor).last()
}

fn mirror_constant_prefix(
    constants: &mut FxHashMap<String, String>,
    source_prefix: &str,
    target_prefix: &str,
) {
    let mut mirrored = Vec::new();
    for (key, value) in constants.iter() {
        if key == source_prefix {
            mirrored.push((target_prefix.to_owned(), value.clone()));
            continue;
        }
        if let Some(suffix) = key.strip_prefix(source_prefix)
            && suffix.starts_with('.')
        {
            mirrored.push((format!("{target_prefix}{suffix}"), value.clone()));
        }
    }

    for (key, value) in mirrored {
        constants.insert(key, value);
    }
}

fn extract_object_constants(
    prefix: &str,
    object: Node<'_>,
    source: &str,
    constants: &mut Vec<(String, String)>,
) {
    let mut cursor = object.walk();
    for child in object.children(&mut cursor) {
        if child.kind() != "pair" {
            continue;
        }
        let key = child
            .child_by_field_name("key")
            .or_else(|| child.named_child(0))
            .map(|node| sanitize_string_literal(node_text(node, source)))
            .unwrap_or_default();
        let Some(value) = child
            .child_by_field_name("value")
            .or_else(|| child.named_child(1))
        else {
            continue;
        };
        let full_key = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        match value.kind() {
            "string" => {
                constants.push((full_key, sanitize_string_literal(node_text(value, source))));
            }
            "object" => extract_object_constants(&full_key, value, source, constants),
            _ => {}
        }
    }
}

fn sanitize_string_literal(value: &str) -> String {
    value.trim().trim_matches('"').trim_matches('\'').to_owned()
}

#[cfg(test)]
mod tests {
    #![expect(clippy::needless_raw_string_hashes)]

    use std::{
        env,
        fmt::Write as _,
        fs, io,
        path::{Path, PathBuf},
        process,
        sync::{
            Arc, Mutex,
            atomic::{AtomicU64, Ordering},
        },
    };

    use gather_step_core::{EdgeKind, NodeKind, node_id};

    use crate::{Language, tsconfig::PathAliases};

    use super::{
        WorkspaceRepoIdentity, configured_workspace_repo_identities,
        import_path_exists_inside_allowed_roots, load_configured_workspace_repo_identities,
        parse_file, parse_file_with_context, resolve_import_path,
        resolve_python_sibling_package_import, resolve_sibling_package_import, should_use_swc,
    };

    static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn canonical(path: impl AsRef<Path>) -> PathBuf {
        fs::canonicalize(path).expect("expected path should canonicalize")
    }

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> Self {
            let counter = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-parser-tree-{name}-{}-{counter}",
                process::id()
            ));
            fs::create_dir_all(&path).expect("test directory should be created");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[derive(Clone)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

    struct CapturedLogWriter(Arc<Mutex<Vec<u8>>>);

    impl io::Write for CapturedLogWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .expect("log capture lock should not be poisoned")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for CapturedLogs {
        type Writer = CapturedLogWriter;

        fn make_writer(&'writer self) -> Self::Writer {
            CapturedLogWriter(Arc::clone(&self.0))
        }
    }

    fn capture_warnings(run: impl FnOnce()) -> String {
        let logs = Arc::new(Mutex::new(Vec::new()));
        let writer = CapturedLogs(Arc::clone(&logs));
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_writer(writer)
            .without_time()
            .finish();

        tracing::subscriber::with_default(subscriber, run);

        let bytes = logs
            .lock()
            .expect("log capture lock should not be poisoned")
            .clone();
        String::from_utf8(bytes).expect("captured logs should be utf-8")
    }

    #[test]
    fn parses_typescript_fixture_symbols() {
        let temp_dir = TestDir::new("typescript");
        fs::write(
            temp_dir.path().join("controller.ts"),
            r#"
import { Controller, Get } from '@nestjs/common';
import { helper } from './helper';

@Controller('items')
export class ItemController {
  @Get('list')
  async listItems() {
    return helper();
  }
}
"#,
        )
        .expect("fixture should write");
        fs::write(
            temp_dir.path().join("helper.ts"),
            "export function helper() { return 1; }\n",
        )
        .expect("helper should write");
        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "controller.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        let kinds = parsed
            .nodes
            .iter()
            .map(|node| node.kind)
            .collect::<Vec<_>>();
        assert!(kinds.contains(&NodeKind::Class));
        assert!(kinds.contains(&NodeKind::Function));
        assert!(kinds.contains(&NodeKind::Import));
        assert!(kinds.contains(&NodeKind::Module));
        assert!(
            parsed
                .edges
                .iter()
                .any(|edge| edge.kind == EdgeKind::Exports)
        );
        assert!(parsed.symbols.iter().any(|symbol| {
            symbol.node.name == "listItems"
                && symbol
                    .decorators
                    .iter()
                    .any(|decorator| decorator.name == "Get")
        }));
    }

    #[test]
    fn method_call_via_this_receiver_resolves_to_remote_method() {
        // End-to-end probe of the `this.field.method()` resolution path.
        // Two files, one with a service class, one with a controller that
        // calls the service method via an injected instance. After parsing
        // both files and feeding the accumulated symbols through the
        // resolver, the call must appear in `resolved` (via Unique strategy)
        // — not in `unresolved` — otherwise the doctor output for every
        // NestJS-style workspace drowns in false positives.
        use crate::ResolutionInput;
        use crate::resolve::resolve_calls_with_unresolved;

        let temp_dir = TestDir::new("this-receiver");
        fs::write(
            temp_dir.path().join("order.service.ts"),
            r#"
export class OrderService {
  async persistOrder(payload: string): Promise<string> {
    return payload;
  }
}
"#,
        )
        .expect("service fixture should write");
        fs::write(
            temp_dir.path().join("controller.ts"),
            r#"
import { OrderService } from './order.service';

export class ServiceAController {
  constructor(private readonly orderService: OrderService) {}

  async createOrder(payload: string): Promise<string> {
    return this.orderService.persistOrder(payload);
  }
}
"#,
        )
        .expect("controller fixture should write");

        let parse = |path: &str| {
            parse_file(
                "sample-service",
                temp_dir.path(),
                &crate::FileEntry {
                    path: path.into(),
                    language: Language::TypeScript,
                    size_bytes: 0,
                    content_hash: [0; 32],
                    source_bytes: None,
                },
            )
            .expect("fixture should parse")
        };

        let service_parsed = parse("order.service.ts");
        let controller_parsed = parse("controller.ts");

        let mut all_nodes = Vec::new();
        all_nodes.extend(service_parsed.symbols.iter().map(|s| s.node.clone()));
        all_nodes.extend(controller_parsed.symbols.iter().map(|s| s.node.clone()));

        let inputs = vec![
            ResolutionInput {
                file_node: service_parsed.file_node.id,
                file_path: temp_dir.path().join("order.service.ts"),
                import_bindings: service_parsed.import_bindings.clone(),
                call_sites: service_parsed
                    .call_sites
                    .iter()
                    .map(|c| crate::resolve::CallSite {
                        owner_id: c.owner_id,
                        owner_file: c.owner_file,
                        source_path: temp_dir.path().join("order.service.ts"),
                        callee_name: c.callee_name.clone(),
                        callee_qualified_hint: c.callee_qualified_hint.clone(),
                        span: c.span.clone(),
                    })
                    .collect(),
            },
            ResolutionInput {
                file_node: controller_parsed.file_node.id,
                file_path: temp_dir.path().join("controller.ts"),
                import_bindings: controller_parsed.import_bindings.clone(),
                call_sites: controller_parsed
                    .call_sites
                    .iter()
                    .map(|c| crate::resolve::CallSite {
                        owner_id: c.owner_id,
                        owner_file: c.owner_file,
                        source_path: temp_dir.path().join("controller.ts"),
                        callee_name: c.callee_name.clone(),
                        callee_qualified_hint: c.callee_qualified_hint.clone(),
                        span: c.span.clone(),
                    })
                    .collect(),
            },
        ];

        let outcome = resolve_calls_with_unresolved(temp_dir.path(), &all_nodes, &inputs);

        // The only meaningful call across these fixtures is
        // `this.orderService.persistOrder(payload)` — if the resolver works,
        // the outcome has exactly one resolved edge targeting persistOrder.
        let persist = all_nodes
            .iter()
            .find(|n| n.name == "persistOrder")
            .expect("persistOrder symbol should exist");
        let resolved_persist = outcome.resolved.iter().any(|r| r.edge.target == persist.id);
        let unresolved_persist = outcome
            .unresolved
            .iter()
            .flat_map(|input| input.call_sites.iter())
            .any(|c| c.callee_name == "persistOrder");
        assert!(
            resolved_persist,
            "persistOrder call should resolve via Unique. outcome.resolved={:?}, outcome.unresolved={:?}",
            outcome.resolved.len(),
            outcome
                .unresolved
                .iter()
                .flat_map(|input| input
                    .call_sites
                    .iter()
                    .map(|c| (c.callee_name.clone(), c.callee_qualified_hint.clone())))
                .collect::<Vec<_>>()
        );
        assert!(
            !unresolved_persist,
            "persistOrder call should not appear in unresolved outcome"
        );
    }

    #[test]
    fn class_field_arrow_method_call_via_this_receiver_resolves() {
        use crate::ResolutionInput;
        use crate::resolve::resolve_calls_with_unresolved;

        let temp_dir = TestDir::new("this-class-field");
        fs::write(
            temp_dir.path().join("dashboard.tsx"),
            r#"
export class Dashboard {
  handleSortChange = (value: number) => {
    return value;
  };

  buildTabs() {
    return [{ onClick: () => this.handleSortChange(0) }];
  }
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "sample-frontend",
            temp_dir.path(),
            &crate::FileEntry {
                path: "dashboard.tsx".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        let all_nodes: Vec<_> = parsed.symbols.iter().map(|s| s.node.clone()).collect();
        let inputs = vec![ResolutionInput {
            file_node: parsed.file_node.id,
            file_path: temp_dir.path().join("dashboard.tsx"),
            import_bindings: parsed.import_bindings.clone(),
            call_sites: parsed
                .call_sites
                .iter()
                .map(|c| crate::resolve::CallSite {
                    owner_id: c.owner_id,
                    owner_file: c.owner_file,
                    source_path: temp_dir.path().join("dashboard.tsx"),
                    callee_name: c.callee_name.clone(),
                    callee_qualified_hint: c.callee_qualified_hint.clone(),
                    span: c.span.clone(),
                })
                .collect(),
        }];

        let outcome = resolve_calls_with_unresolved(temp_dir.path(), &all_nodes, &inputs);
        let target = all_nodes
            .iter()
            .find(|n| n.name == "handleSortChange")
            .expect("handleSortChange symbol should exist");

        assert!(
            outcome.resolved.iter().any(|r| r.edge.target == target.id),
            "class-field arrow method call should resolve. unresolved={:?}",
            outcome
                .unresolved
                .iter()
                .flat_map(|input| input
                    .call_sites
                    .iter()
                    .map(|c| (c.callee_name.clone(), c.callee_qualified_hint.clone())))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn method_call_via_this_receiver_resolves_with_nestjs_decorators() {
        // Same as the minimal `this.X.Y()` probe above but with the full
        // NestJS decorator stack the real fixture uses. If decorators
        // somehow steal the call_site or wrongly own it, this test fails
        // and points at the regression.
        use crate::ResolutionInput;
        use crate::resolve::resolve_calls_with_unresolved;

        let temp_dir = TestDir::new("this-receiver-nest");
        fs::write(
            temp_dir.path().join("order.service.ts"),
            r#"
import type { CreateOrderInput, OrderRecord } from '@workspace/shared-contracts';

export class OrderService {
  async persistOrder(payload: CreateOrderInput): Promise<OrderRecord> {
    return { orderId: payload.email, email: payload.email, status: 'active' };
  }
}
"#,
        )
        .expect("service fixture should write");
        fs::write(
            temp_dir.path().join("controller.ts"),
            r#"
import { Body, Controller, Get, Post } from '@nestjs/common';
import { EventPattern } from '@nestjs/microservices';
import type { CreateOrderInput, OrderRecord } from '@workspace/shared-contracts';

import { OrderService } from './order.service';

const Routes = { orders: { list: 'orders' } };

@Controller({ path: Routes.orders.list })
export class ServiceAController {
  constructor(private readonly orderService: OrderService) {}

  @Get()
  listOrders(): OrderRecord[] {
    return [];
  }

  @Post()
  async createOrder(@Body() payload: CreateOrderInput): Promise<OrderRecord> {
    return this.orderService.persistOrder(payload);
  }

  @EventPattern('order.created')
  handleOrderCreated(data: OrderRecord) {
    return data.status;
  }
}
"#,
        )
        .expect("controller fixture should write");

        // Use the same framework mix the real backend_standard fixture does.
        let frameworks = &[
            crate::frameworks::Framework::NestJs,
            crate::frameworks::Framework::Mongoose,
        ];
        let aliases = PathAliases::empty();
        let parse = |path: &str| {
            parse_file_with_context(
                "sample-service",
                temp_dir.path(),
                &crate::FileEntry {
                    path: path.into(),
                    language: Language::TypeScript,
                    size_bytes: 0,
                    content_hash: [0; 32],
                    source_bytes: None,
                },
                frameworks,
                &aliases,
            )
            .expect("fixture should parse")
        };

        let service_parsed = parse("order.service.ts");
        let controller_parsed = parse("controller.ts");

        let mut all_nodes = Vec::new();
        all_nodes.extend(service_parsed.symbols.iter().map(|s| s.node.clone()));
        all_nodes.extend(controller_parsed.symbols.iter().map(|s| s.node.clone()));

        let inputs = vec![
            ResolutionInput {
                file_node: service_parsed.file_node.id,
                file_path: temp_dir.path().join("order.service.ts"),
                import_bindings: service_parsed.import_bindings.clone(),
                call_sites: service_parsed
                    .call_sites
                    .iter()
                    .map(|c| crate::resolve::CallSite {
                        owner_id: c.owner_id,
                        owner_file: c.owner_file,
                        source_path: temp_dir.path().join("order.service.ts"),
                        callee_name: c.callee_name.clone(),
                        callee_qualified_hint: c.callee_qualified_hint.clone(),
                        span: c.span.clone(),
                    })
                    .collect(),
            },
            ResolutionInput {
                file_node: controller_parsed.file_node.id,
                file_path: temp_dir.path().join("controller.ts"),
                import_bindings: controller_parsed.import_bindings.clone(),
                call_sites: controller_parsed
                    .call_sites
                    .iter()
                    .map(|c| crate::resolve::CallSite {
                        owner_id: c.owner_id,
                        owner_file: c.owner_file,
                        source_path: temp_dir.path().join("controller.ts"),
                        callee_name: c.callee_name.clone(),
                        callee_qualified_hint: c.callee_qualified_hint.clone(),
                        span: c.span.clone(),
                    })
                    .collect(),
            },
        ];

        // Debug aid: dump what call sites the controller actually emits
        // (appears on test failure).
        let controller_call_sites: Vec<_> = controller_parsed
            .call_sites
            .iter()
            .map(|c| (c.callee_name.clone(), c.callee_qualified_hint.clone()))
            .collect();
        let persist_count = all_nodes
            .iter()
            .filter(|n| n.name == "persistOrder")
            .count();

        let outcome = resolve_calls_with_unresolved(temp_dir.path(), &all_nodes, &inputs);

        let persist = all_nodes
            .iter()
            .find(|n| n.name == "persistOrder")
            .expect("persistOrder symbol should exist");
        let resolved_persist = outcome.resolved.iter().any(|r| r.edge.target == persist.id);
        let unresolved_calls: Vec<_> = outcome
            .unresolved
            .iter()
            .flat_map(|input| {
                input
                    .call_sites
                    .iter()
                    .map(|c| (c.callee_name.clone(), c.callee_qualified_hint.clone()))
            })
            .collect();
        assert!(
            resolved_persist,
            "persistOrder call should resolve; persistOrder node count={persist_count}; \
             controller emitted {controller_call_sites:?}; \
             unresolved after resolve: {unresolved_calls:?}"
        );
    }

    #[test]
    fn parses_arrow_function_and_constant_routes() {
        let temp_dir = TestDir::new("arrow");
        fs::write(
            temp_dir.path().join("feature.ts"),
            r#"
const Routes = { items: { list: 'items/list' } };
export const listItems = async () => {
  return Routes.items.list;
};
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "feature.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        assert!(
            parsed
                .nodes
                .iter()
                .any(|node| node.kind == NodeKind::Function && node.name == "listItems")
        );
        assert_eq!(
            parsed
                .constant_strings
                .get("Routes.items.list")
                .map(String::as_str),
            Some("items/list")
        );
    }

    #[test]
    fn parses_python_source_file() {
        let temp_dir = std::env::temp_dir().join("gather-step-parser-python-fixture.py");
        std::fs::write(
            &temp_dir,
            "@decorator\nclass Example:\n    def __init__(self, repo):\n        self.repo = repo\n\n    def run(self):\n        return helper()\n\ndef helper():\n    return 1\n",
        )
        .expect("temp fixture should write");
        let file = crate::FileEntry {
            path: temp_dir.file_name().expect("file name").into(),
            language: Language::Python,
            size_bytes: 0,
            content_hash: [0; 32],
            source_bytes: None,
        };
        let parsed = parse_file("sample-service", temp_dir.parent().expect("parent"), &file)
            .expect("python fixture should parse");

        assert!(
            parsed
                .nodes
                .iter()
                .any(|node| node.kind == NodeKind::Class && node.name == "Example")
        );
        assert!(
            parsed
                .nodes
                .iter()
                .any(|node| node.kind == NodeKind::Function && node.name == "helper")
        );
        assert!(
            parsed
                .symbols
                .iter()
                .any(|symbol| symbol.node.name == "Example")
        );
        assert!(
            parsed
                .symbols
                .iter()
                .any(|symbol| symbol.node.name == "Example"
                    && symbol.constructor_dependencies == vec!["repo"])
        );
    }

    #[test]
    fn malformed_typescript_produces_partial_results_without_panicking() {
        let temp_dir = TestDir::new("malformed-ts");
        fs::write(
            temp_dir.path().join("broken.ts"),
            "export function good() { return helper();\nexport class Broken {\n",
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "broken.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        assert!(
            parsed
                .nodes
                .iter()
                .any(|node| node.kind == NodeKind::Function && node.name == "good")
        );
    }

    #[test]
    fn recovered_swc_fallback_resets_state_before_tree_sitter_retry() {
        let temp_dir = TestDir::new("recovered-ts-fallback");
        let source = "export function good() { return helper(); }\nexport class Broken {\n";
        fs::write(temp_dir.path().join("broken.ts"), source).expect("fixture should write");

        assert_eq!(
            crate::ts_js_swc::swc_test_support::parse_recovery_status_for_extension("ts", source),
            "recovered"
        );

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "broken.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        let good_functions = parsed
            .nodes
            .iter()
            .filter(|node| node.kind == NodeKind::Function && node.name == "good")
            .count();
        assert_eq!(
            good_functions, 1,
            "fallback should not duplicate symbols emitted by a prior recovered SWC pass"
        );
    }

    #[test]
    fn unrecoverable_swc_parse_falls_back_to_tree_sitter_for_parity() {
        let temp_dir = TestDir::new("swc-unrecoverable-fallback");
        let source = "@\nexport function good() { return 1; }\n";
        fs::write(temp_dir.path().join("broken.ts"), source).expect("fixture should write");

        assert_eq!(
            crate::ts_js_swc::swc_test_support::parse_recovery_status_for_extension("ts", source),
            "unrecoverable"
        );

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "broken.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        assert!(
            parsed
                .nodes
                .iter()
                .any(|node| node.kind == NodeKind::Function && node.name == "good"),
            "tree-sitter fallback should preserve later symbols when SWC recovery is unrecoverable"
        );
    }

    #[test]
    fn parse_ts_import_bindings_strips_braces_and_type_keyword() {
        // Regression: first name in a `{ A, B, C }` list was returned with a
        // leading `{ ` because the default-plus-named branch ran before the
        // brace-only branch. `import type { X }` also leaked the `type`
        // keyword. Any downstream consumer that looked up a call by its
        // imported name (e.g. the resolver's external-import filter) silently
        // missed those bindings because `"{ Body"` never matched `"Body"`.
        let named = super::parse_ts_import_bindings(
            "import { Body, Controller, Get } from '@nestjs/common';",
            "@nestjs/common",
        );
        let locals: Vec<_> = named.iter().map(|b| b.local_name.clone()).collect();
        assert_eq!(locals, vec!["Body", "Controller", "Get"]);
        assert!(named.iter().all(|b| !b.is_default));

        let type_named = super::parse_ts_import_bindings(
            "import type { CreateOrderInput, OrderRecord } from '@workspace/shared-contracts';",
            "@workspace/shared-contracts",
        );
        let type_locals: Vec<_> = type_named.iter().map(|b| b.local_name.clone()).collect();
        assert_eq!(type_locals, vec!["CreateOrderInput", "OrderRecord"]);

        let default_plus_named = super::parse_ts_import_bindings(
            "import React, { useState, useEffect } from 'react';",
            "react",
        );
        let default_plus_locals: Vec<_> = default_plus_named
            .iter()
            .map(|b| b.local_name.clone())
            .collect();
        assert_eq!(default_plus_locals, vec!["React", "useState", "useEffect"]);
        assert!(default_plus_named[0].is_default);

        let namespace =
            super::parse_ts_import_bindings("import * as fs from 'node:fs';", "node:fs");
        assert_eq!(namespace.len(), 1);
        assert_eq!(namespace[0].local_name, "fs");
        assert!(namespace[0].is_namespace);

        let renamed = super::parse_ts_import_bindings(
            "import { Body as PayloadBody, Controller } from '@nestjs/common';",
            "@nestjs/common",
        );
        assert_eq!(renamed.len(), 2);
        assert_eq!(renamed[0].local_name, "PayloadBody");
        assert_eq!(renamed[0].imported_name.as_deref(), Some("Body"));
        assert_eq!(renamed[1].local_name, "Controller");
    }

    #[test]
    fn parses_tsx_files_and_collects_calls() {
        let temp_dir = TestDir::new("tsx");
        fs::write(
            temp_dir.path().join("component.tsx"),
            r#"
export function renderButton() {
  return <button>{helper()}</button>;
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "component.tsx".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        assert!(
            parsed
                .call_sites
                .iter()
                .any(|call| call.callee_name == "helper")
        );
    }

    #[test]
    fn parses_re_exports_and_default_exported_class() {
        let temp_dir = TestDir::new("re-exports");
        fs::write(
            temp_dir.path().join("index.ts"),
            r#"
export { Foo } from './foo';
export * from './bar';
export default class DefaultThing {}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "index.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        assert!(
            parsed
                .import_bindings
                .iter()
                .any(|binding| binding.source == "./foo" && binding.local_name == "Foo")
        );
        assert!(
            parsed
                .nodes
                .iter()
                .any(|node| node.kind == NodeKind::Class && node.name == "DefaultThing")
        );
    }

    #[test]
    fn parses_typescript_type_constructs() {
        let temp_dir = TestDir::new("type-constructs");
        fs::write(
            temp_dir.path().join("types.ts"),
            r#"
export interface Item { id: string }
export type ItemId = string;
export enum Status { Active, Inactive }
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "types.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        let type_names = parsed
            .nodes
            .iter()
            .filter(|node| node.kind == NodeKind::Type)
            .map(|node| node.name.as_str())
            .collect::<Vec<_>>();
        assert!(type_names.contains(&"Item"));
        assert!(type_names.contains(&"ItemId"));
        assert!(type_names.contains(&"Status"));
    }

    #[test]
    fn parses_python_import_forms_and_multiple_decorators() {
        let temp_dir = TestDir::new("python-imports");
        fs::write(
            temp_dir.path().join("module.py"),
            r#"
from pkg.helpers import helper as local_helper
import toolkit.api

@decorator_one
@decorator_two
def run():
    return local_helper()
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "module.py".into(),
                language: Language::Python,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        assert!(parsed.import_bindings.iter().any(|binding| {
            binding.source == "pkg.helpers"
                && binding.local_name == "local_helper"
                && binding.imported_name.as_deref() == Some("helper")
        }));
        assert!(parsed.import_bindings.iter().any(|binding| {
            binding.source == "toolkit.api" && binding.local_name == "api" && binding.is_namespace
        }));
        assert!(parsed.symbols.iter().any(|symbol| {
            symbol.node.name == "run"
                && symbol
                    .decorators
                    .iter()
                    .map(|d| d.name.as_str())
                    .collect::<Vec<_>>()
                    == vec!["decorator_one", "decorator_two"]
        }));
    }

    #[test]
    fn nested_python_classes_keep_parent_qualified_names() {
        let temp_dir = TestDir::new("python-nested-class-qname");
        fs::write(
            temp_dir.path().join("module.py"),
            r#"
class Outer:
    class Inner:
        def method(self):
            return "ok"
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "module.py".into(),
                language: Language::Python,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        assert!(parsed.symbols.iter().any(|symbol| {
            symbol.node.name == "Inner"
                && symbol.node.qualified_name.as_deref() == Some("Outer.Inner")
        }));
        assert!(parsed.symbols.iter().any(|symbol| {
            symbol.node.name == "method"
                && symbol.node.qualified_name.as_deref() == Some("Outer.Inner.method")
        }));
    }

    #[test]
    fn relative_imports_cannot_escape_repo_root() {
        let temp_dir = TestDir::new("relative-escape");
        fs::create_dir_all(temp_dir.path().join("src")).expect("src dir should exist");

        let resolved = resolve_import_path(
            temp_dir.path(),
            Path::new("src/service.ts"),
            "../shared/secret",
            Language::TypeScript,
            &PathAliases::empty(),
        );

        assert!(resolved.is_none());
    }

    #[test]
    fn import_path_existence_miss_from_uncanonical_root_is_not_cached() {
        let temp_dir = TestDir::new("import-path-cache-canonicalize");
        let repo_root = temp_dir.path().join("repo");
        let candidate = repo_root.join("src/index.ts");

        assert!(!import_path_exists_inside_allowed_roots(
            &repo_root, &candidate
        ));

        fs::create_dir_all(candidate.parent().expect("candidate has parent"))
            .expect("repo source dir should write");
        fs::write(&candidate, "export const value = 1;\n").expect("candidate should write");

        assert!(import_path_exists_inside_allowed_roots(
            &repo_root, &candidate
        ));
    }

    #[test]
    fn python_sibling_package_imports_resolve_through_pyproject_layout() {
        let temp_dir = TestDir::new("python-sibling-package");
        let app_root = temp_dir.path().join("api_service");
        let shared_root = temp_dir.path().join("shared_models_repo");
        fs::create_dir_all(app_root.join("src/api_service")).expect("app package should exist");
        fs::create_dir_all(shared_root.join("src/shared_models"))
            .expect("shared package should exist");
        fs::write(
            app_root.join("pyproject.toml"),
            "[project]\nname = \"api-service\"\n",
        )
        .expect("app pyproject should write");
        fs::write(
            shared_root.join("pyproject.toml"),
            "[project]\nname = \"shared-models\"\n",
        )
        .expect("shared pyproject should write");
        fs::write(
            shared_root.join("src/shared_models/records.py"),
            "class RawDocument: ...\n",
        )
        .expect("records module should write");

        let resolved = resolve_import_path(
            &app_root,
            Path::new("src/api_service/app.py"),
            "shared_models.records",
            Language::Python,
            &PathAliases::empty(),
        )
        .expect("sibling Python package should resolve");

        assert_eq!(
            resolved,
            canonical(shared_root.join("src/shared_models/records.py"))
        );
    }

    #[test]
    fn duplicate_python_sibling_packages_stay_unresolved_when_current_file_cannot_disambiguate() {
        let temp_dir = TestDir::new("python-sibling-duplicates");
        let app_root = temp_dir.path().join("api_service");
        let first_root = temp_dir.path().join("a_shared_models");
        let second_root = temp_dir.path().join("b_shared_models");
        fs::create_dir_all(&app_root).expect("app repo should exist");
        for root in [&first_root, &second_root] {
            fs::create_dir_all(root.join("src/shared_models"))
                .expect("shared package should exist");
            fs::write(
                root.join("pyproject.toml"),
                "[project]\nname = \"shared-models\"\n",
            )
            .expect("pyproject should write");
            fs::write(
                root.join("src/shared_models/records.py"),
                "class RawDocument: ...\n",
            )
            .expect("records module should write");
        }

        let mut resolved = None;
        let logs = capture_warnings(|| {
            resolved = resolve_python_sibling_package_import(
                &app_root,
                Path::new("src/api_service/app.py"),
                "shared_models.records",
            );
        });

        assert_eq!(resolved.as_deref(), None);
        assert!(
            !logs.contains("multiple sibling Python packages matched import"),
            "duplicate sibling package ambiguity should not warn at default log level, got {logs}"
        );
    }

    #[test]
    fn swc_skips_static_mapping_files() {
        let mut file = crate::traverse::FileEntry {
            path: PathBuf::from("app/src/v2/app/translate/en/globalSearch.json"),
            language: Language::JavaScript,
            size_bytes: 0,
            content_hash: [0; 32],
            source_bytes: None,
        };
        assert!(!should_use_swc(&file));

        file.path = PathBuf::from("src/routes.ts");
        file.language = Language::TypeScript;
        assert!(should_use_swc(&file));
    }

    #[test]
    fn python_sibling_package_import_prefers_project_containing_current_file() {
        let temp_dir = TestDir::new("python-sibling-current-project");
        let workspace_root = temp_dir.path();
        let content_pipeline_root = workspace_root.join("content-pipeline");
        let web_scraper_root = workspace_root.join("web-scraper");
        let package_root = web_scraper_root.join("app");

        for root in [&content_pipeline_root, &web_scraper_root] {
            fs::create_dir_all(root.join("app/repositories"))
                .expect("app repository package should exist");
            fs::write(
                root.join("pyproject.toml"),
                "[project]\nname = \"service\"\n",
            )
            .expect("pyproject should write");
            fs::write(
                root.join("app/repositories/article_content_repository.py"),
                "class ArticleContentRepository: ...\n",
            )
            .expect("repository module should write");
        }
        fs::write(
            package_root.join("consumer.py"),
            "from app.repositories.article_content_repository import ArticleContentRepository\n",
        )
        .expect("consumer should write");

        let mut resolved = None;
        let logs = capture_warnings(|| {
            resolved = resolve_import_path(
                &package_root,
                Path::new("consumer.py"),
                "app.repositories.article_content_repository",
                Language::Python,
                &PathAliases::empty(),
            );
        });

        assert_eq!(
            resolved.as_deref(),
            Some(
                canonical(web_scraper_root.join("app/repositories/article_content_repository.py"))
                    .as_path()
            )
        );
        assert!(
            !logs.contains("multiple sibling Python packages matched import"),
            "current project should disambiguate sibling matches, got {logs}"
        );
    }

    #[test]
    fn python_neutral_fixture_sibling_imports_resolve_between_repos() {
        let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("tests/fixtures/python_planning_workspace/workspace")
            .canonicalize()
            .expect("neutral Python workspace should exist");
        let transform_root = workspace_root.join("py_transform_service");
        let shared_root = workspace_root.join("py_shared_models");

        let resolved = resolve_import_path(
            &transform_root,
            Path::new("src/transform_service/pipeline.py"),
            "shared_models.records",
            Language::Python,
            &PathAliases::empty(),
        )
        .expect("neutral fixture sibling Python package should resolve");

        assert_eq!(
            resolved,
            canonical(shared_root.join("src/shared_models/records.py"))
        );
    }

    #[test]
    fn python_neutral_fixture_parse_emits_cross_repo_import_edges() {
        let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("tests/fixtures/python_planning_workspace/workspace")
            .canonicalize()
            .expect("neutral Python workspace should exist");
        let transform_root = workspace_root.join("py_transform_service");
        let shared_root = workspace_root.join("py_shared_models");
        let file = crate::traverse::FileEntry {
            path: PathBuf::from("src/transform_service/pipeline.py"),
            language: Language::Python,
            size_bytes: 0,
            content_hash: [0; 32],
            source_bytes: None,
        };

        let parsed = parse_file("py_transform_service", &transform_root, &file)
            .expect("neutral Python fixture should parse");
        let shared_path = canonical(shared_root.join("src/shared_models/records.py"));

        assert!(parsed.import_bindings.iter().any(|binding| {
            binding.source == "shared_models.records"
                && binding.resolved_path.as_deref() == Some(shared_path.as_path())
        }));
        let shared_file_id = node_id(
            "py_shared_models",
            "src/shared_models/records.py",
            NodeKind::File,
            "src/shared_models/records.py",
        );
        assert!(
            parsed
                .edges
                .iter()
                .any(|edge| { edge.kind == EdgeKind::Imports && edge.target == shared_file_id })
        );
    }

    #[cfg(unix)]
    #[test]
    fn relative_imports_cannot_escape_repo_root_through_symlink() {
        use std::os::unix::fs::symlink;

        let temp_dir = TestDir::new("relative-symlink-escape");
        let external = TestDir::new("relative-symlink-external");
        fs::create_dir_all(temp_dir.path().join("src")).expect("src dir should exist");
        fs::write(
            external.path().join("secret.ts"),
            "export const secret = 1;",
        )
        .expect("external file should write");
        symlink(
            external.path().join("secret.ts"),
            temp_dir.path().join("src/secret.ts"),
        )
        .expect("symlink should create");

        let resolved = resolve_import_path(
            temp_dir.path(),
            Path::new("src/service.ts"),
            "./secret",
            Language::TypeScript,
            &PathAliases::empty(),
        );

        assert!(resolved.is_none());
    }

    #[test]
    fn alias_imports_cannot_escape_repo_root() {
        let temp_dir = TestDir::new("alias-escape");
        fs::write(
            temp_dir.path().join("tsconfig.json"),
            r#"{ "compilerOptions": { "paths": { "@shared/*": ["../shared/*"] } } }"#,
        )
        .expect("tsconfig should write");
        fs::write(
            temp_dir.path().join("index.ts"),
            "import { secret } from '@shared/secret';\nexport const value = secret;\n",
        )
        .expect("fixture should write");

        let aliases = PathAliases::from_repo_root(temp_dir.path());
        let parsed = parse_file_with_context(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "index.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &[],
            &aliases,
        )
        .expect("fixture should parse");

        assert!(
            parsed
                .import_bindings
                .iter()
                .all(|binding| binding.resolved_path.is_none())
        );
    }

    #[test]
    fn workspace_package_import_resolves_without_node_modules() {
        let temp_dir = TestDir::new("workspace-package-import");
        fs::create_dir_all(temp_dir.path().join("packages/contracts/src"))
            .expect("contracts dir should exist");
        fs::create_dir_all(temp_dir.path().join("apps/api/src")).expect("api dir should exist");
        fs::write(
            temp_dir.path().join("package.json"),
            r#"{ "workspaces": ["packages/*", "apps/*"] }"#,
        )
        .expect("workspace manifest should write");
        fs::write(
            temp_dir.path().join("packages/contracts/package.json"),
            r#"{ "name": "@repo/contracts", "types": "src/index.ts" }"#,
        )
        .expect("contracts package should write");
        fs::write(
            temp_dir.path().join("packages/contracts/src/index.ts"),
            "export type OrderDto = { id: string };\n",
        )
        .expect("contracts source should write");
        fs::write(
            temp_dir.path().join("apps/api/src/handler.ts"),
            "import type { OrderDto } from '@repo/contracts';\nexport type Local = OrderDto;\n",
        )
        .expect("consumer source should write");

        let parsed = parse_file(
            "backend_standard",
            &temp_dir.path().join("apps/api"),
            &crate::FileEntry {
                path: "src/handler.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        assert_eq!(
            parsed.import_bindings[0].resolved_path.as_deref(),
            Some(canonical(temp_dir.path().join("packages/contracts/src/index.ts")).as_path())
        );
    }

    #[test]
    fn malformed_sibling_package_manifest_emits_warning() {
        let temp_dir = TestDir::new("workspace-package-malformed-manifest");
        let api_root = temp_dir.path().join("apps/api");
        fs::create_dir_all(api_root.join("src")).expect("api dir should exist");
        fs::create_dir_all(temp_dir.path().join("contracts")).expect("contracts dir should exist");
        fs::write(
            temp_dir.path().join("contracts/package.json"),
            "{ this is not valid json",
        )
        .expect("malformed manifest should write");

        let logs = capture_warnings(|| {
            assert!(resolve_sibling_package_import(&api_root, "contracts").is_none());
        });

        assert!(
            logs.contains("failed to parse package.json while resolving sibling package import"),
            "expected malformed package.json warning, got {logs}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn workspace_package_imports_ignore_symlinked_package_dirs() {
        use std::os::unix::fs::symlink;

        let temp_dir = TestDir::new("workspace-package-symlink-escape");
        let external = TestDir::new("workspace-package-symlink-external");
        fs::create_dir_all(temp_dir.path().join("packages")).expect("packages dir should exist");
        fs::create_dir_all(temp_dir.path().join("apps/api/src")).expect("api dir should exist");
        fs::create_dir_all(external.path().join("contracts/src"))
            .expect("external package dir should exist");
        fs::write(
            temp_dir.path().join("package.json"),
            r#"{ "workspaces": ["packages/*", "apps/*"] }"#,
        )
        .expect("workspace manifest should write");
        fs::write(
            external.path().join("contracts/package.json"),
            r#"{ "name": "@repo/contracts", "types": "src/index.ts" }"#,
        )
        .expect("external manifest should write");
        fs::write(
            external.path().join("contracts/src/index.ts"),
            "export type SecretDto = { id: string };\n",
        )
        .expect("external source should write");
        symlink(
            external.path().join("contracts"),
            temp_dir.path().join("packages/contracts"),
        )
        .expect("symlink should create");
        fs::write(
            temp_dir.path().join("apps/api/src/handler.ts"),
            "import type { SecretDto } from '@repo/contracts';\nexport type Local = SecretDto;\n",
        )
        .expect("consumer source should write");

        let parsed = parse_file(
            "backend_standard",
            &temp_dir.path().join("apps/api"),
            &crate::FileEntry {
                path: "src/handler.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        assert!(
            parsed
                .import_bindings
                .iter()
                .all(|binding| binding.resolved_path.is_none())
        );
    }

    #[test]
    fn workspace_package_alias_from_context_resolves_absolute_entrypoint() {
        let temp_dir = TestDir::new("workspace-package-alias-context");
        fs::create_dir_all(temp_dir.path().join("packages/contracts/src"))
            .expect("contracts dir should exist");
        fs::create_dir_all(temp_dir.path().join("apps/team/api/src"))
            .expect("api dir should exist");
        fs::write(
            temp_dir.path().join("package.json"),
            r#"{ "workspaces": ["packages/*", "apps/*/api"] }"#,
        )
        .expect("workspace manifest should write");
        fs::write(
            temp_dir.path().join("packages/contracts/package.json"),
            r#"{ "name": "@repo/contracts", "types": "src/index.ts" }"#,
        )
        .expect("contracts package should write");
        fs::write(
            temp_dir.path().join("packages/contracts/src/index.ts"),
            "export type OrderDto = { id: string };\n",
        )
        .expect("contracts source should write");
        fs::write(
            temp_dir.path().join("apps/team/api/src/handler.ts"),
            "import type { OrderDto } from '@repo/contracts';\nexport type Local = OrderDto;\n",
        )
        .expect("consumer source should write");

        let packages = crate::workspace_manifest::discover_workspace_packages(temp_dir.path());
        let mut aliases = crate::tsconfig::PathAliases::empty();
        aliases.add_workspace_packages(&packages);

        let parsed = parse_file_with_context(
            "backend_standard",
            &temp_dir.path().join("apps/team/api"),
            &crate::FileEntry {
                path: "src/handler.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &[],
            &aliases,
        )
        .expect("fixture should parse");

        assert_eq!(
            parsed.import_bindings[0].resolved_path.as_deref(),
            Some(canonical(temp_dir.path().join("packages/contracts/src/index.ts")).as_path())
        );
    }

    #[test]
    fn nested_workspace_package_import_resolves_without_node_modules() {
        let temp_dir = TestDir::new("nested-workspace-package-import");
        fs::create_dir_all(temp_dir.path().join("packages/shared/contracts/src"))
            .expect("contracts dir should exist");
        fs::create_dir_all(temp_dir.path().join("apps/api/src")).expect("api dir should exist");
        fs::write(
            temp_dir.path().join("package.json"),
            r#"{ "workspaces": ["packages/*/*", "apps/*"] }"#,
        )
        .expect("workspace manifest should write");
        fs::write(
            temp_dir
                .path()
                .join("packages/shared/contracts/package.json"),
            r#"{ "name": "@repo/contracts", "types": "src/index.ts" }"#,
        )
        .expect("contracts package should write");
        fs::write(
            temp_dir
                .path()
                .join("packages/shared/contracts/src/index.ts"),
            "export type OrderDto = { id: string };\n",
        )
        .expect("contracts source should write");
        fs::write(
            temp_dir.path().join("apps/api/src/handler.ts"),
            "import type { OrderDto } from '@repo/contracts';\nexport type Local = OrderDto;\n",
        )
        .expect("consumer source should write");

        let parsed = parse_file(
            "backend_standard",
            &temp_dir.path().join("apps/api"),
            &crate::FileEntry {
                path: "src/handler.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        assert_eq!(
            parsed.import_bindings[0].resolved_path.as_deref(),
            Some(
                canonical(
                    temp_dir
                        .path()
                        .join("packages/shared/contracts/src/index.ts")
                )
                .as_path()
            )
        );
    }

    #[test]
    fn sibling_package_import_resolves_without_workspace_manifest() {
        let temp_dir = TestDir::new("sibling-package-import");
        fs::create_dir_all(temp_dir.path().join("shared_contracts/src"))
            .expect("shared_contracts dir should exist");
        fs::create_dir_all(temp_dir.path().join("backend_standard/src"))
            .expect("backend_standard dir should exist");
        fs::write(
            temp_dir.path().join("shared_contracts/package.json"),
            r#"{ "name": "@repo/shared_contracts", "types": "src/index.ts" }"#,
        )
        .expect("shared_contracts package should write");
        fs::write(
            temp_dir.path().join("shared_contracts/src/enums.ts"),
            "export enum EventType { DocumentQueued = 'document.queued' }\n",
        )
        .expect("shared_contracts source should write");
        fs::write(
            temp_dir.path().join("backend_standard/package.json"),
            r#"{ "name": "@repo/backend_standard" }"#,
        )
        .expect("backend_standard package should write");
        fs::write(
            temp_dir.path().join("backend_standard/src/handler.ts"),
            "import { EventType } from '@repo/shared_contracts/enums';\nexport const eventType = EventType.DocumentQueued;\n",
        )
        .expect("consumer source should write");

        let aliases = PathAliases::from_repo_root(&temp_dir.path().join("backend_standard"));
        let parsed = parse_file_with_context(
            "backend_standard",
            &temp_dir.path().join("backend_standard"),
            &crate::FileEntry {
                path: "src/handler.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &[],
            &aliases,
        )
        .expect("fixture should parse");

        assert_eq!(
            parsed.import_bindings[0].resolved_path.as_deref(),
            Some(canonical(temp_dir.path().join("shared_contracts/src/enums.ts")).as_path())
        );
    }

    #[test]
    fn sibling_package_subpath_import_resolves_when_manifest_points_to_built_root() {
        let temp_dir = TestDir::new("sibling-package-built-root-subpath");
        fs::create_dir_all(temp_dir.path().join("shared_contracts/src/kafka"))
            .expect("shared_contracts dir should exist");
        fs::create_dir_all(temp_dir.path().join("backend_standard/src"))
            .expect("backend_standard dir should exist");
        fs::write(
            temp_dir.path().join("shared_contracts/package.json"),
            r#"{ "name": "@repo/shared_contracts", "types": "index.d.ts", "main": "index.js" }"#,
        )
        .expect("shared_contracts package should write");
        fs::write(
            temp_dir.path().join("shared_contracts/index.d.ts"),
            "export {};\n",
        )
        .expect("shared_contracts built root types should write");
        fs::write(
            temp_dir.path().join("shared_contracts/src/kafka/enums.ts"),
            "export enum EventType { DocumentQueued = 'document.queued' }\n",
        )
        .expect("shared_contracts source should write");
        fs::write(
            temp_dir.path().join("backend_standard/package.json"),
            r#"{ "name": "@repo/backend_standard" }"#,
        )
        .expect("backend_standard package should write");
        fs::write(
            temp_dir.path().join("backend_standard/src/handler.ts"),
            "import { EventType } from '@repo/shared_contracts/kafka/enums';\nexport const eventType = EventType.DocumentQueued;\n",
        )
        .expect("consumer source should write");

        let aliases = PathAliases::from_repo_root(&temp_dir.path().join("backend_standard"));
        let parsed = parse_file_with_context(
            "backend_standard",
            &temp_dir.path().join("backend_standard"),
            &crate::FileEntry {
                path: "src/handler.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &[],
            &aliases,
        )
        .expect("fixture should parse");

        assert_eq!(
            parsed.import_bindings[0].resolved_path.as_deref(),
            Some(canonical(temp_dir.path().join("shared_contracts/src/kafka/enums.ts")).as_path())
        );
    }

    #[test]
    fn parser_emits_shared_contract_type_and_implements_edges() {
        let temp_dir = TestDir::new("shared-contract-edges");
        fs::write(
            temp_dir.path().join("consumer.ts"),
            r#"
import type { OrderDto } from '@workspace/shared-contracts';

export class Handler implements OrderDto {}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "backend_standard",
            temp_dir.path(),
            &crate::FileEntry {
                path: "consumer.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        let shared_symbol = parsed
            .nodes
            .iter()
            .find(|node| {
                node.kind == NodeKind::SharedSymbol
                    && node.external_id.as_deref()
                        == Some("__shared__@workspace/shared-contracts__OrderDto")
            })
            .expect("shared symbol should be emitted");
        let handler = parsed
            .symbols
            .iter()
            .find(|symbol| symbol.node.name == "Handler")
            .expect("handler class should exist");

        assert!(
            parsed.edges.iter().any(|edge| {
                edge.kind == EdgeKind::UsesTypeFrom
                    && edge.source == parsed.file_node.id
                    && edge.target == shared_symbol.id
            }),
            "type-only imports should emit UsesTypeFrom from the file to the shared symbol"
        );
        assert!(
            parsed.edges.iter().any(|edge| {
                edge.kind == EdgeKind::ImplementsContractFrom
                    && edge.source == handler.node.id
                    && edge.target == shared_symbol.id
            }),
            "class implements clauses should emit ImplementsContractFrom"
        );
    }

    #[test]
    fn parser_emits_shared_contract_edges_for_namespace_imports() {
        let temp_dir = TestDir::new("shared-contract-namespace-edges");
        fs::write(
            temp_dir.path().join("consumer.ts"),
            r#"
import type * as Contracts from '@workspace/shared-contracts';

export class Handler implements Contracts.OrderDto {}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "backend_standard",
            temp_dir.path(),
            &crate::FileEntry {
                path: "consumer.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        let shared_symbol = parsed
            .nodes
            .iter()
            .find(|node| {
                node.kind == NodeKind::SharedSymbol
                    && node.external_id.as_deref()
                        == Some("__shared__@workspace/shared-contracts__OrderDto")
            })
            .expect("shared symbol should be emitted");
        let handler = parsed
            .symbols
            .iter()
            .find(|symbol| symbol.node.name == "Handler")
            .expect("handler class should exist");

        assert!(
            parsed.edges.iter().any(|edge| {
                edge.kind == EdgeKind::UsesTypeFrom
                    && edge.source == parsed.file_node.id
                    && edge.target == shared_symbol.id
            }),
            "namespace type-only imports should emit UsesTypeFrom for the referenced contract"
        );
        assert!(
            parsed.edges.iter().any(|edge| {
                edge.kind == EdgeKind::ImplementsContractFrom
                    && edge.source == handler.node.id
                    && edge.target == shared_symbol.id
            }),
            "namespace-based implements clauses should emit ImplementsContractFrom"
        );
    }

    #[test]
    fn parser_emits_shared_contract_edges_for_workspace_type_imports_without_import_type() {
        let temp_dir = TestDir::new("workspace-shared-contract-edges");
        fs::write(
            temp_dir.path().join("package.json"),
            r#"{ "workspaces": ["packages/*", "apps/*"] }"#,
        )
        .expect("workspace manifest should write");
        fs::create_dir_all(
            temp_dir
                .path()
                .join("packages/shared_contracts/src/microservices/identity/types"),
        )
        .expect("shared_contracts types dir should exist");
        fs::write(
            temp_dir
                .path()
                .join("packages/shared_contracts/package.json"),
            r#"{"name":"@vendor/shared_contracts","version":"2.0.0"}"#,
        )
        .expect("package manifest should write");
        fs::write(
            temp_dir.path().join(
                "packages/shared_contracts/src/microservices/identity/types/audit-user.type.ts",
            ),
            "export type AuditUser = { userId: string };\n",
        )
        .expect("shared contract fixture should write");
        fs::create_dir_all(temp_dir.path().join("apps/api/src"))
            .expect("consumer src dir should exist");
        fs::write(
            temp_dir.path().join("apps/api/src/controller.ts"),
            r#"
import { AuditUser } from '@vendor/shared_contracts/microservices/identity/types/audit-user.type';

export class Controller {
  handle(user: AuditUser) {
    return user.userId;
  }
}
"#,
        )
        .expect("consumer fixture should write");

        let packages = crate::workspace_manifest::discover_workspace_packages(temp_dir.path());
        let mut aliases = crate::tsconfig::PathAliases::empty();
        aliases.add_workspace_packages(&packages);

        let parsed = parse_file_with_context(
            "backend_standard",
            &temp_dir.path().join("apps/api"),
            &crate::FileEntry {
                path: "src/controller.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &[],
            &aliases,
        )
        .expect("fixture should parse");

        let shared_symbol = parsed
            .nodes
            .iter()
            .find(|node| {
                node.kind == NodeKind::SharedSymbol
                    && node.external_id.as_deref()
                        == Some("__shared__@vendor/shared_contracts__AuditUser")
            })
            .expect("workspace package type import should emit a shared symbol");

        assert!(
            parsed.edges.iter().any(|edge| {
                edge.kind == EdgeKind::UsesTypeFrom
                    && edge.source == parsed.file_node.id
                    && edge.target == shared_symbol.id
            }),
            "contract-like workspace imports should emit UsesTypeFrom even without `import type`"
        );
    }

    #[test]
    fn parser_emits_shared_contract_edges_for_workspace_barrel_type_imports() {
        let temp_dir = TestDir::new("workspace-shared-contract-barrel");
        fs::write(
            temp_dir.path().join("package.json"),
            r#"{ "workspaces": ["packages/*", "apps/*"] }"#,
        )
        .expect("workspace manifest should write");
        fs::create_dir_all(
            temp_dir
                .path()
                .join("packages/shared_contracts/src/microservices/identity/types"),
        )
        .expect("shared_contracts types dir should exist");
        fs::write(
            temp_dir
                .path()
                .join("packages/shared_contracts/package.json"),
            r#"{"name":"@vendor/shared_contracts","version":"2.0.0","types":"src/index.ts"}"#,
        )
        .expect("package manifest should write");
        fs::write(
            temp_dir.path().join(
                "packages/shared_contracts/src/microservices/identity/types/audit-user.type.ts",
            ),
            "export type AuditUser = { userId: string };\n",
        )
        .expect("shared contract fixture should write");
        fs::write(
            temp_dir
                .path()
                .join("packages/shared_contracts/src/microservices/identity/types/index.ts"),
            "export * from './audit-user.type';\n",
        )
        .expect("barrel file should write");
        fs::create_dir_all(temp_dir.path().join("apps/api/src"))
            .expect("consumer src dir should exist");
        fs::write(
            temp_dir.path().join("apps/api/src/controller.ts"),
            r#"
import { AuditUser } from '@vendor/shared_contracts/microservices/identity/types';

export class Controller {
  handle(user: AuditUser) {
    return user.userId;
  }
}
"#,
        )
        .expect("consumer fixture should write");

        let packages = crate::workspace_manifest::discover_workspace_packages(temp_dir.path());
        let mut aliases = crate::tsconfig::PathAliases::empty();
        aliases.add_workspace_packages(&packages);

        let parsed = parse_file_with_context(
            "backend_standard",
            &temp_dir.path().join("apps/api"),
            &crate::FileEntry {
                path: "src/controller.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &[],
            &aliases,
        )
        .expect("fixture should parse");

        assert!(
            parsed.nodes.iter().any(|node| {
                node.kind == NodeKind::SharedSymbol
                    && node.external_id.as_deref()
                        == Some("__shared__@vendor/shared_contracts__AuditUser")
            }),
            "barrel type imports should emit a shared symbol"
        );
        assert!(
            parsed.edges.iter().any(|edge| {
                edge.kind == EdgeKind::UsesTypeFrom && edge.source == parsed.file_node.id
            }),
            "barrel type imports should emit UsesTypeFrom"
        );
    }

    #[test]
    fn deep_member_chains_do_not_overflow_callsite_parsing() {
        let temp_dir = TestDir::new("deep-member-chain");
        let chain_len = 5_000;
        let mut chain = String::from("root");
        for index in 0..chain_len {
            let _ = write!(chain, ".segment{index}");
        }
        let source = format!("export function run() {{\n  return {chain}();\n}}\n");
        fs::write(temp_dir.path().join("deep.ts"), source).expect("fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "deep.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        let callsite = parsed
            .call_sites
            .iter()
            .find(|call| call.callee_name == format!("segment{}", chain_len - 1))
            .expect("deep chained call should be recorded");
        assert_eq!(
            callsite.callee_qualified_hint.as_deref(),
            Some(chain.as_str()),
            "qualified name should preserve the full member chain without overflowing"
        );
    }

    // ── Source-bytes threading tests ──────────────────────────────────────────

    /// Bytes pre-loaded into `FileEntry.source_bytes` must flow through
    /// `parse_file_core` without triggering an additional `fs::read` call.
    /// We verify this by writing a file to disk, parsing it with
    /// `source_bytes` pre-populated from different bytes — if the parser used
    /// disk bytes the symbol name would be `"from_disk"`, not `"from_cache"`.
    #[test]
    fn source_bytes_preloaded_are_used_without_disk_reread() {
        use crate::traverse::FileEntry;

        let dir = TestDir::new("source-bytes-threading");
        // The file on disk exports `from_disk`.
        fs::write(
            dir.path().join("check.ts"),
            b"export function from_disk() {}",
        )
        .expect("fixture file");

        // But we pre-populate source_bytes with a different function name.
        let preloaded: std::sync::Arc<[u8]> = b"export function from_cache() {}".to_vec().into();

        let entry = FileEntry {
            path: "check.ts".into(),
            language: Language::TypeScript,
            size_bytes: 0,
            content_hash: [0; 32],
            source_bytes: Some(preloaded),
        };

        let parsed = parse_file("repo", dir.path(), &entry).expect("parse");

        let found = parsed.symbols.iter().any(|s| s.node.name == "from_cache");
        assert!(
            found,
            "parser should use pre-loaded bytes (from_cache), not disk bytes (from_disk)"
        );
        // Confirm the source Arc also reflects the preloaded content.
        assert!(
            parsed.source.contains("from_cache"),
            "parsed.source should match the pre-loaded bytes"
        );
    }

    /// `ParsedFile.source` must survive non-UTF-8 byte sequences via lossy
    /// replacement instead of aborting the parse.
    #[test]
    fn invalid_utf8_is_tolerated_via_lossy_fallback() {
        use crate::traverse::FileEntry;

        let dir = TestDir::new("invalid-utf8");
        // ISO-8859-1 high byte 0xFF followed by valid ASCII — not valid UTF-8.
        let content = b"export function ok() { /* \xFF */ }".to_vec();
        // Stub file on disk so path resolution doesn't fail.
        fs::write(dir.path().join("bad.ts"), b"// placeholder").expect("fixture");

        let entry = FileEntry {
            path: "bad.ts".into(),
            language: Language::TypeScript,
            size_bytes: content.len() as u64,
            content_hash: [0; 32],
            // Pass via source_bytes so the test exercises the same code-path
            // as the traversal pipeline (no disk re-read needed).
            source_bytes: Some(content.into()),
        };

        // Must not panic or return an error; the invalid byte is replaced with U+FFFD.
        let parsed = parse_file("repo", dir.path(), &entry).expect("lossy parse must succeed");
        assert!(
            parsed.source.contains("ok"),
            "valid parts of source should survive lossy decoding"
        );
        // The replacement character is present where the bad byte was.
        assert!(
            parsed.source.contains('\u{FFFD}'),
            "U+FFFD replacement character must appear in place of invalid byte"
        );
    }

    /// `ParsedFile.source` is an `Arc<str>`, so cloning it costs only a
    /// reference-count increment — not a string copy.  Verify the Arc count
    /// advances without allocating a second copy.
    #[test]
    fn source_arc_clone_does_not_copy_string() {
        use crate::traverse::FileEntry;

        let dir = TestDir::new("arc-clone");
        fs::write(dir.path().join("simple.ts"), b"export const x = 1;").expect("fixture");

        let entry = FileEntry {
            path: "simple.ts".into(),
            language: Language::TypeScript,
            size_bytes: 20,
            content_hash: [0; 32],
            source_bytes: None,
        };

        let parsed = parse_file("repo", dir.path(), &entry).expect("parse");

        // Clone the Arc — same heap allocation, second strong reference.
        let cloned = parsed.source.clone();
        // Both point to the same buffer: pointer equality holds.
        assert!(
            std::ptr::eq(parsed.source.as_ptr(), cloned.as_ptr()),
            "Arc<str> clone must share the same heap allocation"
        );
        // Content is still correct after cloning.
        assert!(cloned.contains("const x"));
    }

    /// Locks the contract that `load_configured_workspace_repo_identities`
    /// returns the configured names for repos under the workspace, so the
    /// 5a5563a fix (cross-repo nodes use the configured `name` field rather
    /// than the directory basename) cannot regress silently.
    #[test]
    fn load_configured_workspace_repo_identities_returns_configured_names() {
        let dir = TestDir::new("identities-valid");
        let workspace = dir.path();
        let repo_a = workspace.join("services/alpha");
        let repo_b = workspace.join("services/beta");
        fs::create_dir_all(&repo_a).expect("repo a created");
        fs::create_dir_all(&repo_b).expect("repo b created");
        fs::write(
            workspace.join("gather-step.config.yaml"),
            "repos:\n  - name: configured_alpha\n    path: services/alpha\n  - name: configured_beta\n    path: services/beta\n",
        )
        .expect("config written");

        let identities = load_configured_workspace_repo_identities(&repo_a)
            .expect("identities should resolve from configured workspace");
        let names: Vec<&str> = identities.iter().map(WorkspaceRepoIdentity::name).collect();
        assert!(
            names.contains(&"configured_alpha") && names.contains(&"configured_beta"),
            "expected configured names, got {names:?}"
        );
        let alpha = identities
            .iter()
            .find(|repo| repo.name() == "configured_alpha")
            .expect("configured_alpha entry");
        assert_eq!(alpha.root(), canonical(&repo_a));
    }

    #[test]
    fn workspace_repo_identity_rejects_whitespace_only_names() {
        let dir = TestDir::new("identity-whitespace-name");
        let root = canonical(dir.path());

        assert!(WorkspaceRepoIdentity::new("   ", root).is_none());
    }

    #[test]
    fn missing_workspace_config_result_is_not_cached() {
        let dir = TestDir::new("identities-none-cache");
        let workspace = dir.path();
        let repo_a = workspace.join("services/alpha");
        let repo_b = workspace.join("services/beta");
        fs::create_dir_all(&repo_a).expect("repo a created");
        fs::create_dir_all(&repo_b).expect("repo b created");

        assert!(configured_workspace_repo_identities(&repo_a).is_none());

        fs::write(
            workspace.join("gather-step.config.yaml"),
            "repos:\n  - name: configured_alpha\n    path: services/alpha\n  - name: configured_beta\n    path: services/beta\n",
        )
        .expect("config written");

        let identities = configured_workspace_repo_identities(&repo_a)
            .expect("newly added config should be visible");
        let names: Vec<&str> = identities.iter().map(WorkspaceRepoIdentity::name).collect();
        assert!(names.contains(&"configured_alpha"));
        assert!(names.contains(&"configured_beta"));
    }

    /// Locks the diagnostic fallback path: a malformed `gather-step.config.yaml`
    /// must return `None` so callers fall back to the directory-basename
    /// heuristic instead of crashing or partially loading.
    #[test]
    fn load_configured_workspace_repo_identities_falls_back_on_malformed_yaml() {
        let dir = TestDir::new("identities-malformed");
        let workspace = dir.path();
        let repo_a = workspace.join("services/alpha");
        fs::create_dir_all(&repo_a).expect("repo a created");
        fs::write(
            workspace.join("gather-step.config.yaml"),
            "repos:\n  - name: configured_alpha\n    path:\n      this is not valid yaml: [unclosed",
        )
        .expect("malformed config written");

        let identities = load_configured_workspace_repo_identities(&repo_a);
        assert!(
            identities.is_none(),
            "malformed config should fall back to None, got {identities:?}"
        );
    }
}
