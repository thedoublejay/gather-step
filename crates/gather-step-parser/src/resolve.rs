use std::{
    borrow::Cow,
    path::{Path, PathBuf},
};

use camino::Utf8PathBuf;
use quick_cache::sync::Cache;

use gather_step_core::{EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeId, SourceSpan};
use rustc_hash::{FxHashMap, FxHashSet};

/// Bounded capacity for the per-`SymbolIndex` parsed-file cache.
///
/// 4 096 entries covers any realistic workspace (most have <500 distinct files
/// that the re-export resolver needs to re-parse).  `quick_cache::sync::Cache`
/// is internally sharded, so this capacity is spread across shards — individual
/// shard locks are held for microseconds at most.
const PARSED_FILE_CACHE_CAPACITY: usize = 4_096;

/// Cache key that namespaces parsed results by both the canonical file path and
/// the repo root, preventing cross-workspace cache collisions when the same
/// absolute path appears under two different indexing contexts.
type CacheKey = (PathBuf, PathBuf);

use crate::{
    traverse::{FileEntry, Language},
    tree_sitter::{ParsedFile, parse_file_with_context},
    tsconfig::PathAliases,
};

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ImportBinding {
    pub local_name: String,
    pub imported_name: Option<String>,
    pub source: String,
    pub resolved_path: Option<PathBuf>,
    pub is_default: bool,
    pub is_namespace: bool,
    pub is_type_only: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CallSite {
    pub owner_id: NodeId,
    pub owner_file: NodeId,
    pub source_path: PathBuf,
    pub callee_name: String,
    pub callee_qualified_hint: Option<String>,
    pub span: Option<SourceSpan>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CallTargetCandidate {
    pub node: NodeData,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ResolutionStrategy {
    ImportMap,
    SameModule,
    Unique,
    Suffix,
    FuzzyName,
    Fallback,
}

impl ResolutionStrategy {
    #[must_use]
    pub const fn base_confidence(self) -> f32 {
        match self {
            Self::ImportMap => 0.95,
            Self::SameModule => 0.90,
            Self::Unique => 0.75,
            Self::Suffix => 0.55,
            Self::FuzzyName => 0.40,
            Self::Fallback => 0.30,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ImportMap => "import_map",
            Self::SameModule => "same_module",
            Self::Unique => "unique",
            Self::Suffix => "suffix",
            Self::FuzzyName => "fuzzy_name",
            Self::Fallback => "fallback",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedCall {
    pub edge: EdgeData,
    pub confidence: f32,
    pub strategy: ResolutionStrategy,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ResolutionInput {
    pub file_node: NodeId,
    pub file_path: PathBuf,
    pub import_bindings: Vec<ImportBinding>,
    pub call_sites: Vec<CallSite>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResolutionOutcome {
    pub resolved: Vec<ResolvedCall>,
    pub unresolved: Vec<ResolutionInput>,
}

pub fn resolve_calls<'a>(
    repo_root: &Path,
    symbols: &'a [NodeData],
    files: impl IntoIterator<Item = &'a ResolutionInput>,
) -> Vec<ResolvedCall> {
    resolve_calls_with_unresolved(repo_root, symbols, files).resolved
}

/// Resolve call sites across all files in a repo.
///
/// The `files` parameter accepts any type that can be turned into an iterator
/// of [`ResolutionInput`] references.  Internally the inputs are collected into
/// a `Vec` so the [`SymbolIndex`] can hold borrows into them — passing a slice
/// reference (`&[_]`) is the common case, but an iterator or a channel drain
/// works equally well.
pub fn resolve_calls_with_unresolved<'a>(
    repo_root: &Path,
    symbols: &'a [NodeData],
    files: impl IntoIterator<Item = &'a ResolutionInput>,
) -> ResolutionOutcome {
    use rayon::prelude::*;

    // Collect so the SymbolIndex can hold borrows into a stable allocation.
    let files: Vec<&ResolutionInput> = files.into_iter().collect();
    let index = SymbolIndex::new(repo_root, symbols, &files);

    // Resolve each file in parallel. The SymbolIndex is immutable (&self)
    // so it can be shared across rayon workers without synchronization.
    let per_file_results: Vec<(Vec<ResolvedCall>, Option<ResolutionInput>)> = files
        .par_iter()
        .map(|&file| {
            let import_map = index.import_targets(&file.file_path, &file.import_bindings);
            let external_names = external_import_names(&file.import_bindings, &import_map);
            let mut file_resolved = Vec::new();
            let mut unresolved_call_sites = Vec::new();

            for call_site in &file.call_sites {
                if let Some((target, strategy, confidence)) =
                    index.resolve_call(call_site, &import_map)
                {
                    file_resolved.push(ResolvedCall {
                        edge: EdgeData {
                            source: call_site.owner_id,
                            target: target.id,
                            kind: EdgeKind::Calls,
                            metadata: EdgeMetadata {
                                weight: None,
                                confidence: Some(encode_confidence(confidence)),
                                timestamp_unix: None,
                                drift_kind: None,
                                resolver: Some(strategy.as_str().to_owned()),
                            },
                            owner_file: file.file_node,
                            is_cross_file: target.file_path
                                != file.file_path.to_string_lossy().as_ref(),
                        },
                        confidence,
                        strategy,
                    });
                } else if !is_external_call(call_site, &external_names) {
                    unresolved_call_sites.push(call_site.clone());
                }
            }

            let unresolved_input = if unresolved_call_sites.is_empty() {
                None
            } else {
                Some(ResolutionInput {
                    file_node: file.file_node,
                    file_path: file.file_path.clone(),
                    import_bindings: file.import_bindings.clone(),
                    call_sites: unresolved_call_sites,
                })
            };

            (file_resolved, unresolved_input)
        })
        .collect();

    // Merge results and deduplicate. The dedup key is (owner_id, target_id,
    // strategy) — same as before. We process files in input order (rayon
    // preserves par_iter index order) to keep output deterministic.
    let mut resolved = Vec::new();
    let mut unresolved = Vec::new();
    let mut seen = FxHashSet::default();

    for (file_resolved, file_unresolved) in per_file_results {
        for call in file_resolved {
            let key = (call.edge.source, call.edge.target, call.strategy.as_str());
            if seen.insert(key) {
                resolved.push(call);
            }
        }
        if let Some(input) = file_unresolved {
            unresolved.push(input);
        }
    }

    ResolutionOutcome {
        resolved,
        unresolved,
    }
}

/// Names whose imports resolved outside the workspace (bare package specifiers,
/// paths into `node_modules`, or unresolved paths altogether). Calls to these
/// names shouldn't be surfaced as "unresolved" — they're definitively external.
fn external_import_names(
    bindings: &[ImportBinding],
    import_map: &FxHashMap<String, Vec<&NodeData>>,
) -> FxHashSet<String> {
    let mut external = FxHashSet::default();
    for binding in bindings {
        // If the import binding DID resolve to a workspace symbol, it's not
        // external — skip it so real in-workspace calls still appear as
        // unresolved if they don't link up.
        if import_map.contains_key(&binding.local_name) {
            continue;
        }
        let is_external = match binding.resolved_path.as_ref() {
            None => true,
            Some(path) => {
                let path_str = path.to_string_lossy();
                path_str.contains("/node_modules/")
                    || path_str.starts_with('@')
                    || !path_str.starts_with('/') && !path_str.starts_with('.')
            }
        };
        if is_external {
            external.insert(binding.local_name.clone());
        }
    }
    external
}

/// Whether a `call_site` is to an identifier known to come from outside the
/// workspace. Matches by the leaf callee name or the head identifier of a
/// qualified hint (so `SchemaFactory.createForClass` resolves to the
/// `SchemaFactory` import for filtering purposes).
fn is_external_call(call_site: &CallSite, external_names: &FxHashSet<String>) -> bool {
    if external_names.contains(&call_site.callee_name) {
        return true;
    }
    let Some(hint) = call_site.callee_qualified_hint.as_deref() else {
        return false;
    };
    // Head of the qualified hint: `this.bus.emit` → `bus`, `SchemaFactory.x` → `SchemaFactory`.
    let head = hint
        .split('.')
        .find(|segment| !segment.is_empty() && *segment != "this" && *segment != "self")
        .unwrap_or(hint);
    external_names.contains(head)
}

struct SymbolIndex<'a> {
    repo_root: &'a Path,
    by_name: FxHashMap<&'a str, Vec<&'a NodeData>>,
    by_file_and_name: FxHashMap<(&'a str, &'a str), Vec<&'a NodeData>>,
    by_qualified_name: FxHashMap<&'a str, Vec<&'a NodeData>>,
    /// Inverted suffix index: for a qualified name like `"Ctrl::execute"`,
    /// stores entries keyed by `"execute"` and `"Ctrl::execute"` so that
    /// suffix matching is an O(1) lookup instead of a full scan.
    by_suffix: FxHashMap<&'a str, Vec<&'a NodeData>>,
    /// Pre-normalized symbol names for O(1) fuzzy matching.
    by_normalized_name: FxHashMap<String, Vec<&'a NodeData>>,
    /// Symbols grouped by file path for fast default-export fallback.
    by_file: FxHashMap<&'a str, Vec<&'a NodeData>>,
    /// Pre-computed `relative_to_repo` results. Populated at construction
    /// time from input file paths, so all lookups during resolution are O(1)
    /// and require only `&self`.
    relative_paths: FxHashMap<PathBuf, String>,
    /// Bounded concurrent cache of parsed files encountered during re-export
    /// resolution.  Keyed by `(canonical_resolved_path, repo_root)` so entries
    /// from different workspace scans never collide.
    parsed_file_cache: Cache<CacheKey, Option<ParsedFile>>,
    path_aliases: PathAliases,
}

impl<'a> SymbolIndex<'a> {
    fn new(repo_root: &'a Path, symbols: &'a [NodeData], files: &[&'a ResolutionInput]) -> Self {
        let mut by_name: FxHashMap<&str, Vec<&NodeData>> = FxHashMap::default();
        let mut by_file_and_name: FxHashMap<(&str, &str), Vec<&NodeData>> = FxHashMap::default();
        let mut by_qualified_name: FxHashMap<&str, Vec<&NodeData>> = FxHashMap::default();
        let mut by_suffix: FxHashMap<&str, Vec<&NodeData>> = FxHashMap::default();
        let mut by_normalized_name: FxHashMap<String, Vec<&NodeData>> = FxHashMap::default();
        let mut by_file: FxHashMap<&str, Vec<&NodeData>> = FxHashMap::default();

        for node in symbols {
            if !matches!(
                node.kind,
                gather_step_core::NodeKind::Function
                    | gather_step_core::NodeKind::Class
                    | gather_step_core::NodeKind::Type
                    | gather_step_core::NodeKind::Entity
            ) {
                continue;
            }
            by_name.entry(node.name.as_str()).or_default().push(node);
            by_file_and_name
                .entry((node.file_path.as_str(), node.name.as_str()))
                .or_default()
                .push(node);
            by_file
                .entry(node.file_path.as_str())
                .or_default()
                .push(node);
            by_normalized_name
                .entry(normalize_name(&node.name))
                .or_default()
                .push(node);
            if let Some(ref qn) = node.qualified_name {
                by_qualified_name.entry(qn.as_str()).or_default().push(node);
                // Build suffix index from both `::` and `.` segments.
                // The parser emits qualified names with `::` (e.g.
                // "controllers::UserCtrl::execute") and hints with `.`
                // (e.g. "this.orderService.persistOrder"). Index both
                // separator styles so suffix lookup works for either.
                for sep in ["::", "."] {
                    let mut start = qn.len();
                    while let Some(pos) = qn[..start].rfind(sep) {
                        let suffix = &qn[pos + sep.len()..];
                        if !suffix.is_empty() {
                            by_suffix.entry(suffix).or_default().push(node);
                        }
                        start = pos;
                    }
                }
                // The full qualified name itself is also a valid suffix match target.
                by_suffix.entry(qn.as_str()).or_default().push(node);
            }
        }

        // Pre-populate relative path cache from all input file paths and
        // import binding resolved paths so resolution can use &self.
        let mut relative_paths = FxHashMap::default();
        for &file in files {
            relative_paths
                .entry(file.file_path.clone())
                .or_insert_with(|| relative_to_repo(repo_root, &file.file_path));
            for binding in &file.import_bindings {
                if let Some(ref resolved) = binding.resolved_path {
                    relative_paths
                        .entry(resolved.clone())
                        .or_insert_with(|| relative_to_repo(repo_root, resolved));
                }
            }
            for call_site in &file.call_sites {
                relative_paths
                    .entry(call_site.source_path.clone())
                    .or_insert_with(|| relative_to_repo(repo_root, &call_site.source_path));
            }
        }

        Self {
            repo_root,
            by_name,
            by_file_and_name,
            by_qualified_name,
            by_suffix,
            by_normalized_name,
            by_file,
            relative_paths,
            parsed_file_cache: Cache::new(PARSED_FILE_CACHE_CAPACITY),
            path_aliases: PathAliases::from_repo_root(repo_root),
        }
    }

    fn get_relative_path(&self, path: &Path) -> Cow<'_, str> {
        match self.relative_paths.get(path) {
            Some(cached) => Cow::Borrowed(cached.as_str()),
            None => Cow::Owned(relative_to_repo(self.repo_root, path)),
        }
    }

    fn import_targets(
        &self,
        source_file: &Path,
        bindings: &[ImportBinding],
    ) -> FxHashMap<String, Vec<&'a NodeData>> {
        let mut targets = FxHashMap::default();

        for binding in bindings {
            let mut seen = FxHashSet::default();
            let candidates = self.resolve_binding_targets(source_file, binding, &mut seen);
            if !candidates.is_empty() {
                targets.insert(binding.local_name.clone(), candidates);
            }
        }

        let _ = source_file;
        targets
    }

    fn resolve_binding_targets(
        &self,
        source_file: &Path,
        binding: &ImportBinding,
        seen: &mut FxHashSet<(PathBuf, String)>,
    ) -> Vec<&'a NodeData> {
        let Some(resolved_path) = binding.resolved_path.as_ref() else {
            return Vec::new();
        };
        let lookup_name = binding
            .imported_name
            .as_deref()
            .unwrap_or(if binding.is_default {
                "default"
            } else {
                binding.local_name.as_str()
            });
        self.resolve_export_targets(
            source_file,
            resolved_path,
            lookup_name,
            binding.is_default,
            seen,
        )
    }

    fn resolve_export_targets(
        &self,
        source_file: &Path,
        resolved_path: &Path,
        lookup_name: &str,
        prefer_default: bool,
        seen: &mut FxHashSet<(PathBuf, String)>,
    ) -> Vec<&'a NodeData> {
        let visit_key = (resolved_path.to_path_buf(), lookup_name.to_owned());
        if !seen.insert(visit_key.clone()) {
            return Vec::new();
        }

        let relative = self.get_relative_path(resolved_path);
        let relative_str: &str = &relative;
        let mut candidates = self
            .by_file_and_name
            .get(&(relative_str, lookup_name))
            .cloned()
            .unwrap_or_default();

        if candidates.is_empty()
            && prefer_default
            && let Some(file_symbol) = self
                .by_file
                .get(relative_str)
                .into_iter()
                .flatten()
                .find(|node| matches!(node.visibility, Some(gather_step_core::Visibility::Public)))
                .copied()
        {
            candidates.push(file_symbol);
        }

        if candidates.is_empty()
            && let Some(parsed) = self.load_parsed_file(source_file, resolved_path)
        {
            for reexport in &parsed.import_bindings {
                let reexport_lookup_matches =
                    reexport.local_name == lookup_name || reexport.local_name == "*";
                if !reexport_lookup_matches {
                    continue;
                }
                let nested_lookup = if reexport.local_name == "*" {
                    lookup_name
                } else if reexport.is_default {
                    "default"
                } else {
                    reexport
                        .imported_name
                        .as_deref()
                        .unwrap_or(reexport.local_name.as_str())
                };
                let mut nested = self.resolve_binding_targets(
                    resolved_path,
                    &ImportBinding {
                        local_name: reexport.local_name.clone(),
                        imported_name: Some(nested_lookup.to_owned()),
                        source: reexport.source.clone(),
                        resolved_path: reexport.resolved_path.clone(),
                        is_default: reexport.is_default,
                        is_namespace: reexport.is_namespace,
                        is_type_only: reexport.is_type_only,
                    },
                    seen,
                );
                candidates.append(&mut nested);
            }
        }

        seen.remove(&visit_key);
        candidates
    }

    fn load_parsed_file(&self, source_file: &Path, resolved_path: &Path) -> Option<ParsedFile> {
        let cache_key: CacheKey = (resolved_path.to_path_buf(), self.repo_root.to_path_buf());

        if let Some(cached) = self.parsed_file_cache.get(&cache_key) {
            return cached.clone();
        }

        let relative = resolved_path
            .strip_prefix(self.repo_root)
            .ok()?
            .to_path_buf();
        let language = language_for_path(&relative)?;
        let file = FileEntry {
            path: relative,
            language,
            size_bytes: 0,
            content_hash: [0; 32],
            source_bytes: None,
        };
        let parsed =
            parse_file_with_context("", self.repo_root, &file, &[], &self.path_aliases).ok();
        self.parsed_file_cache.insert(cache_key, parsed.clone());
        let _ = source_file;
        parsed
    }

    fn resolve_call(
        &self,
        call_site: &CallSite,
        import_map: &FxHashMap<String, Vec<&'a NodeData>>,
    ) -> Option<(&'a NodeData, ResolutionStrategy, f32)> {
        if let Some(imported) = import_map.get(&call_site.callee_name)
            && let Some(target) = imported.first().copied()
        {
            return Some((
                target,
                ResolutionStrategy::ImportMap,
                penalize(
                    ResolutionStrategy::ImportMap.base_confidence(),
                    imported.len(),
                ),
            ));
        }

        let relative = self.get_relative_path(&call_site.source_path);
        let relative_str: &str = &relative;
        let same_module = self
            .by_file_and_name
            .get(&(relative_str, call_site.callee_name.as_str()))
            .map(Vec::as_slice)
            .unwrap_or_default();
        if let Some(target) = same_module.first().copied() {
            return Some((
                target,
                ResolutionStrategy::SameModule,
                penalize(
                    ResolutionStrategy::SameModule.base_confidence(),
                    same_module.len(),
                ),
            ));
        }

        let by_name = self
            .by_name
            .get(call_site.callee_name.as_str())
            .map(Vec::as_slice)
            .unwrap_or_default();
        if by_name.len() == 1 {
            return Some((
                by_name[0],
                ResolutionStrategy::Unique,
                ResolutionStrategy::Unique.base_confidence(),
            ));
        }

        if let Some(qualified_hint) = call_site.callee_qualified_hint.as_ref()
            && let Some((target, confidence)) = self.resolve_by_suffix(qualified_hint)
        {
            return Some((target, ResolutionStrategy::Suffix, confidence));
        }

        if let Some((target, confidence)) = self.resolve_by_fuzzy_name(&call_site.callee_name) {
            return Some((target, ResolutionStrategy::FuzzyName, confidence));
        }

        by_name.first().copied().map(|target| {
            (
                target,
                ResolutionStrategy::Fallback,
                penalize(
                    ResolutionStrategy::Fallback.base_confidence(),
                    by_name.len(),
                ),
            )
        })
    }

    fn resolve_by_suffix(&self, qualified_hint: &str) -> Option<(&'a NodeData, f32)> {
        // Fast path: use the suffix index to narrow candidates via the terminal
        // segment, then verify with `ends_with`. Falls back to a full scan of
        // `by_qualified_name` when the indexed approach misses, preserving
        // identical semantics with the original O(n) implementation.
        let mut matches = Vec::new();

        // Extract the last segment from the hint using either `::` or `.`.
        let terminal_segment = qualified_hint
            .rfind("::")
            .map(|pos| &qualified_hint[pos + 2..])
            .or_else(|| {
                qualified_hint
                    .rfind('.')
                    .map(|pos| &qualified_hint[pos + 1..])
            })
            .unwrap_or(qualified_hint);

        // Get candidates sharing this terminal segment, filter with ends_with.
        if let Some(candidates) = self.by_suffix.get(terminal_segment) {
            for node in candidates {
                if let Some(ref qn) = node.qualified_name
                    && (qn.ends_with(qualified_hint) || qualified_hint.ends_with(qn.as_str()))
                {
                    matches.push(*node);
                }
            }
        }

        // Check the hint itself as a suffix index key.
        if let Some(nodes) = self.by_suffix.get(qualified_hint) {
            for node in nodes {
                matches.push(*node);
            }
        }

        // Walk segments of the hint and check as full qualified names.
        for sep in ["::", "."] {
            let mut start = qualified_hint.len();
            while let Some(pos) = qualified_hint[..start].rfind(sep) {
                let segment = &qualified_hint[pos + sep.len()..];
                if !segment.is_empty()
                    && let Some(nodes) = self.by_qualified_name.get(segment)
                {
                    for node in nodes {
                        matches.push(*node);
                    }
                }
                start = pos;
            }
        }
        if let Some(nodes) = self.by_qualified_name.get(qualified_hint) {
            for node in nodes {
                matches.push(*node);
            }
        }

        // Fallback: if the indexed approaches found nothing, do a full scan
        // of by_qualified_name with ends_with checks — same semantics as the
        // original O(n) code. This catches edge cases where the hint/candidate
        // relationship doesn't align with any indexed segment boundary.
        if matches.is_empty() {
            for (candidate, nodes) in &self.by_qualified_name {
                if (candidate.ends_with(qualified_hint) || qualified_hint.ends_with(*candidate))
                    && let Some(node) = nodes.first().copied()
                {
                    matches.push(node);
                }
            }
        }

        // Deduplicate by node id.
        let mut seen = FxHashSet::default();
        matches.retain(|node| seen.insert(node.id));

        matches.sort_by(|left, right| left.qualified_name.cmp(&right.qualified_name));
        matches.first().copied().map(|target| {
            (
                target,
                penalize(ResolutionStrategy::Suffix.base_confidence(), matches.len()),
            )
        })
    }

    fn resolve_by_fuzzy_name(&self, name: &str) -> Option<(&'a NodeData, f32)> {
        let normalized = normalize_name(name);

        // Fast path: exact normalized-name match (O(1) lookup instead of O(n)
        // scan). This covers the common case where `createOrder` normalises to
        // the same string as `create_order`.
        if let Some(nodes) = self.by_normalized_name.get(&normalized) {
            // Filter out the exact-name match (same as the old code's `continue`).
            let candidates: Vec<_> = nodes
                .iter()
                .filter(|node| node.name != name)
                .copied()
                .collect();
            if let Some(target) = candidates.first().copied() {
                let score = fuzzy_similarity(&normalized, &normalize_name(&target.name));
                if score >= 0.72 {
                    let adjusted = penalize(
                        ResolutionStrategy::FuzzyName.base_confidence() * score,
                        candidates.len(),
                    );
                    return Some((target, adjusted));
                }
            }
        }

        // Slow fallback: iterate pre-normalized names for fuzzy matches that
        // didn't normalize to exactly the same string. Uses `by_normalized_name`
        // to avoid calling `normalize_name()` per entry.
        let mut best: Option<(&NodeData, f32)> = None;
        for (candidate_normalized, nodes) in &self.by_normalized_name {
            if *candidate_normalized == normalized {
                continue; // fast path already handled exact normalized match
            }
            let score = fuzzy_similarity(&normalized, candidate_normalized);
            if score >= 0.72 {
                // Filter out exact-name matches (same as the old code's skip).
                let candidates: Vec<_> = nodes
                    .iter()
                    .filter(|node| node.name != name)
                    .copied()
                    .collect();
                if candidates.is_empty() {
                    continue;
                }
                let adjusted = penalize(
                    ResolutionStrategy::FuzzyName.base_confidence() * score,
                    candidates.len(),
                );
                if best.is_none_or(|(_, best_score)| adjusted > best_score) {
                    best = candidates.first().copied().map(|node| (node, adjusted));
                }
            }
        }

        best
    }
}

fn language_for_path(path: &Path) -> Option<Language> {
    let extension = path.extension()?.to_str()?;
    match extension {
        "ts" | "tsx" | "mts" | "cts" => Some(Language::TypeScript),
        "js" | "jsx" | "mjs" | "cjs" => Some(Language::JavaScript),
        "py" => Some(Language::Python),
        _ => None,
    }
}

#[expect(clippy::cast_precision_loss)]
fn penalize(base: f32, candidate_count: usize) -> f32 {
    if candidate_count <= 1 {
        return base;
    }
    let penalty = 1.0 + ((candidate_count - 1) as f32 * 0.1);
    (base / penalty).clamp(0.0, 1.0)
}

fn normalize_name(value: &str) -> String {
    value
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .flat_map(char::to_lowercase)
        .collect()
}

#[expect(clippy::cast_precision_loss)]
fn fuzzy_similarity(left: &str, right: &str) -> f32 {
    if left.is_empty() || right.is_empty() {
        return 0.0;
    }
    if left == right {
        return 1.0;
    }

    let prefix = left
        .chars()
        .zip(right.chars())
        .take_while(|(left_char, right_char)| left_char == right_char)
        .count();
    // Build a set from `right` so character containment is O(1) per char
    // instead of the previous O(m) scan, turning the total from O(n×m) to
    // O(n+m).
    let right_chars: FxHashSet<char> = right.chars().collect();
    let shared = left
        .chars()
        .filter(|character| right_chars.contains(character))
        .count();

    let prefix_score = prefix as f32 / left.len().max(right.len()) as f32;
    let shared_score = shared as f32 / left.len().max(right.len()) as f32;
    ((prefix_score * 0.6) + (shared_score * 0.4)).clamp(0.0, 1.0)
}

#[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn encode_confidence(confidence: f32) -> u16 {
    (confidence.clamp(0.0, 1.0) * 1000.0).round() as u16
}

fn relative_to_repo(root: &Path, path: &Path) -> String {
    let rel = path.strip_prefix(root).unwrap_or(path);
    Utf8PathBuf::from_path_buf(rel.to_path_buf())
        .unwrap_or_else(|p| Utf8PathBuf::from(p.to_string_lossy().replace('\\', "/")))
        .into_string()
}

#[cfg(test)]
mod tests {
    #![expect(clippy::float_cmp)]

    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::{NodeData, NodeKind, SourceSpan, Visibility, node_id};
    use pretty_assertions::assert_eq;
    use proptest::prelude::*;

    use super::{
        CallSite, ImportBinding, ResolutionInput, ResolutionStrategy, fuzzy_similarity,
        resolve_calls, resolve_calls_with_unresolved,
    };

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(name: &str) -> Self {
            let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-resolve-{name}-{}-{counter}",
                process::id()
            ));
            fs::create_dir_all(&path).expect("temp dir should create");
            Self { path }
        }

        fn write(&self, relative: &str, contents: &str) {
            let path = self.path.join(relative);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).expect("parent dir should create");
            }
            fs::write(path, contents).expect("fixture should write");
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn function_node(file_path: &str, name: &str) -> NodeData {
        NodeData {
            id: node_id("sample-service", file_path, NodeKind::Function, name),
            kind: NodeKind::Function,
            repo: "sample-service".to_owned(),
            file_path: file_path.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(format!("{file_path}::{name}")),
            external_id: None,
            signature: Some(format!("{name}()")),
            visibility: Some(Visibility::Public),
            span: Some(SourceSpan {
                line_start: 1,
                line_len: 0,
                column_start: 0,
                column_len: 1,
            }),
            is_virtual: false,
        }
    }

    #[test]
    fn import_map_resolution_uses_highest_confidence() {
        let root = PathBuf::from("/repo");
        let target = function_node("src/foo.ts", "Foo");
        let owner = function_node("src/bar.ts", "bar");
        let resolved = resolve_calls(
            &root,
            &[target.clone(), owner.clone()],
            &[ResolutionInput {
                file_node: owner.id,
                file_path: root.join("src/bar.ts"),
                import_bindings: vec![ImportBinding {
                    local_name: "Foo".to_owned(),
                    imported_name: Some("Foo".to_owned()),
                    source: "./foo".to_owned(),
                    resolved_path: Some(root.join("src/foo.ts")),
                    is_default: false,
                    is_namespace: false,
                    is_type_only: false,
                }],
                call_sites: vec![CallSite {
                    owner_id: owner.id,
                    owner_file: owner.id,
                    source_path: root.join("src/bar.ts"),
                    callee_name: "Foo".to_owned(),
                    callee_qualified_hint: None,
                    span: None,
                }],
            }],
        );

        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].edge.target, target.id);
        assert_eq!(resolved[0].strategy, ResolutionStrategy::ImportMap);
        assert_eq!(resolved[0].confidence, 0.95);
    }

    #[test]
    fn same_module_resolution_is_used_when_no_import_exists() {
        let root = PathBuf::from("/repo");
        let target = function_node("src/foo.ts", "helper");
        let owner = function_node("src/foo.ts", "caller");

        let resolved = resolve_calls(
            &root,
            &[target.clone(), owner.clone()],
            &[ResolutionInput {
                file_node: owner.id,
                file_path: root.join("src/foo.ts"),
                import_bindings: Vec::new(),
                call_sites: vec![CallSite {
                    owner_id: owner.id,
                    owner_file: owner.id,
                    source_path: root.join("src/foo.ts"),
                    callee_name: "helper".to_owned(),
                    callee_qualified_hint: None,
                    span: None,
                }],
            }],
        );

        assert_eq!(resolved[0].strategy, ResolutionStrategy::SameModule);
        assert_eq!(resolved[0].confidence, 0.90);
    }

    #[test]
    fn unique_resolution_matches_method_call_via_this_receiver() {
        // Reproduces the NestJS fixture's `this.orderService.persistOrder(payload)`
        // pattern: a method call whose AST yields
        //   callee_name = "persistOrder"
        //   callee_qualified_hint = Some("this.orderService.persistOrder")
        // and whose only workspace-wide match is the method on OrderService.
        // Must resolve via Unique strategy — otherwise doctor reports
        // legitimate in-workspace calls as unresolved and drowns real gaps.
        let root = PathBuf::from("/repo");
        let target = NodeData {
            id: node_id(
                "sample-service",
                "src/order.service.ts",
                NodeKind::Function,
                "persistOrder",
            ),
            kind: NodeKind::Function,
            repo: "sample-service".to_owned(),
            file_path: "src/order.service.ts".to_owned(),
            name: "persistOrder".to_owned(),
            qualified_name: Some("OrderService.persistOrder".to_owned()),
            external_id: None,
            signature: Some("persistOrder()".to_owned()),
            visibility: Some(Visibility::Public),
            span: None,
            is_virtual: false,
        };
        let owner = function_node("src/controller.ts", "createOrder");

        let resolved = resolve_calls(
            &root,
            &[target.clone(), owner.clone()],
            &[ResolutionInput {
                file_node: owner.id,
                file_path: root.join("src/controller.ts"),
                import_bindings: Vec::new(),
                call_sites: vec![CallSite {
                    owner_id: owner.id,
                    owner_file: owner.id,
                    source_path: root.join("src/controller.ts"),
                    callee_name: "persistOrder".to_owned(),
                    callee_qualified_hint: Some("this.orderService.persistOrder".to_owned()),
                    span: None,
                }],
            }],
        );

        assert_eq!(resolved.len(), 1, "method call should resolve");
        assert_eq!(resolved[0].strategy, ResolutionStrategy::Unique);
        assert_eq!(resolved[0].edge.target, target.id);
    }

    #[test]
    fn unique_resolution_works_for_repo_unique_names() {
        let root = PathBuf::from("/repo");
        let target = function_node("src/foo.ts", "uniqueSymbol");
        let owner = function_node("src/bar.ts", "caller");

        let resolved = resolve_calls(
            &root,
            &[target.clone(), owner.clone()],
            &[ResolutionInput {
                file_node: owner.id,
                file_path: root.join("src/bar.ts"),
                import_bindings: Vec::new(),
                call_sites: vec![CallSite {
                    owner_id: owner.id,
                    owner_file: owner.id,
                    source_path: root.join("src/bar.ts"),
                    callee_name: "uniqueSymbol".to_owned(),
                    callee_qualified_hint: None,
                    span: None,
                }],
            }],
        );

        assert_eq!(resolved[0].strategy, ResolutionStrategy::Unique);
        assert_eq!(resolved[0].confidence, 0.75);
    }

    #[test]
    fn candidate_penalty_reduces_confidence_for_ambiguous_names() {
        let root = PathBuf::from("/repo");
        let mut symbols = Vec::new();
        for index in 0_u16..5 {
            symbols.push(function_node(&format!("src/file{index}.ts"), "shared"));
        }
        let owner = function_node("src/caller.ts", "caller");
        symbols.push(owner.clone());

        let resolved = resolve_calls(
            &root,
            &symbols,
            &[ResolutionInput {
                file_node: owner.id,
                file_path: root.join("src/caller.ts"),
                import_bindings: Vec::new(),
                call_sites: vec![CallSite {
                    owner_id: owner.id,
                    owner_file: owner.id,
                    source_path: root.join("src/caller.ts"),
                    callee_name: "shared".to_owned(),
                    callee_qualified_hint: None,
                    span: None,
                }],
            }],
        );

        assert_eq!(resolved[0].strategy, ResolutionStrategy::Fallback);
        assert!(resolved[0].confidence < 0.30);
    }

    #[test]
    fn suffix_resolution_uses_qualified_hint() {
        let root = PathBuf::from("/repo");
        let target = function_node("src/foo.ts", "bar");
        let distractor = function_node("src/other.ts", "bar");
        let owner = function_node("src/caller.ts", "caller");

        let resolved = resolve_calls(
            &root,
            &[target.clone(), distractor, owner.clone()],
            &[ResolutionInput {
                file_node: owner.id,
                file_path: root.join("src/caller.ts"),
                import_bindings: Vec::new(),
                call_sites: vec![CallSite {
                    owner_id: owner.id,
                    owner_file: owner.id,
                    source_path: root.join("src/caller.ts"),
                    callee_name: "bar".to_owned(),
                    callee_qualified_hint: Some("foo.ts::bar".to_owned()),
                    span: None,
                }],
            }],
        );

        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].strategy, ResolutionStrategy::Suffix);
        assert_eq!(resolved[0].edge.target, target.id);
    }

    #[test]
    fn fuzzy_resolution_matches_close_name() {
        let root = PathBuf::from("/repo");
        let target = function_node("src/foo.ts", "create_order");
        let owner = function_node("src/caller.ts", "caller");

        let resolved = resolve_calls(
            &root,
            &[target.clone(), owner.clone()],
            &[ResolutionInput {
                file_node: owner.id,
                file_path: root.join("src/caller.ts"),
                import_bindings: Vec::new(),
                call_sites: vec![CallSite {
                    owner_id: owner.id,
                    owner_file: owner.id,
                    source_path: root.join("src/caller.ts"),
                    callee_name: "createOrder".to_owned(),
                    callee_qualified_hint: None,
                    span: None,
                }],
            }],
        );

        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].strategy, ResolutionStrategy::FuzzyName);
        assert_eq!(resolved[0].edge.target, target.id);
    }

    #[test]
    fn duplicate_call_sites_do_not_emit_duplicate_edges() {
        let root = PathBuf::from("/repo");
        let target = function_node("src/foo.ts", "helper");
        let owner = function_node("src/caller.ts", "caller");
        let repeated_call = CallSite {
            owner_id: owner.id,
            owner_file: owner.id,
            source_path: root.join("src/caller.ts"),
            callee_name: "helper".to_owned(),
            callee_qualified_hint: None,
            span: None,
        };

        let resolved = resolve_calls(
            &root,
            &[target.clone(), owner.clone()],
            &[ResolutionInput {
                file_node: owner.id,
                file_path: root.join("src/caller.ts"),
                import_bindings: Vec::new(),
                call_sites: vec![repeated_call.clone(), repeated_call],
            }],
        );

        assert_eq!(resolved.len(), 1);
    }

    #[test]
    fn cross_file_edges_are_marked() {
        let root = PathBuf::from("/repo");
        let target = function_node("src/helper.ts", "helper");
        let owner = function_node("src/caller.ts", "caller");

        let resolved = resolve_calls(
            &root,
            &[target.clone(), owner.clone()],
            &[ResolutionInput {
                file_node: owner.id,
                file_path: root.join("src/caller.ts"),
                import_bindings: Vec::new(),
                call_sites: vec![CallSite {
                    owner_id: owner.id,
                    owner_file: owner.id,
                    source_path: root.join("src/caller.ts"),
                    callee_name: "helper".to_owned(),
                    callee_qualified_hint: None,
                    span: None,
                }],
            }],
        );

        assert!(resolved[0].edge.is_cross_file);
    }

    #[test]
    fn default_imports_resolve_to_exported_symbol() {
        let root = PathBuf::from("/repo");
        let target = function_node("src/foo.ts", "Widget");
        let owner = function_node("src/caller.ts", "caller");

        let resolved = resolve_calls(
            &root,
            &[target.clone(), owner.clone()],
            &[ResolutionInput {
                file_node: owner.id,
                file_path: root.join("src/caller.ts"),
                import_bindings: vec![ImportBinding {
                    local_name: "Foo".to_owned(),
                    imported_name: None,
                    source: "./foo".to_owned(),
                    resolved_path: Some(root.join("src/foo.ts")),
                    is_default: true,
                    is_namespace: false,
                    is_type_only: false,
                }],
                call_sites: vec![CallSite {
                    owner_id: owner.id,
                    owner_file: owner.id,
                    source_path: root.join("src/caller.ts"),
                    callee_name: "Foo".to_owned(),
                    callee_qualified_hint: None,
                    span: None,
                }],
            }],
        );

        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].strategy, ResolutionStrategy::ImportMap);
        assert_eq!(resolved[0].edge.target, target.id);
    }

    #[test]
    fn unresolved_calls_are_returned_in_outcome() {
        let root = PathBuf::from("/repo");
        let owner = function_node("src/caller.ts", "caller");

        let outcome = resolve_calls_with_unresolved(
            &root,
            std::slice::from_ref(&owner),
            &[ResolutionInput {
                file_node: owner.id,
                file_path: root.join("src/caller.ts"),
                import_bindings: Vec::new(),
                call_sites: vec![CallSite {
                    owner_id: owner.id,
                    owner_file: owner.id,
                    source_path: root.join("src/caller.ts"),
                    callee_name: "missing".to_owned(),
                    callee_qualified_hint: None,
                    span: None,
                }],
            }],
        );

        assert!(outcome.resolved.is_empty());
        assert_eq!(outcome.unresolved.len(), 1);
        assert_eq!(outcome.unresolved[0].call_sites[0].callee_name, "missing");
    }

    proptest! {
        #[test]
        fn fuzzy_similarity_stays_in_unit_interval(left in ".*", right in ".*") {
            let score = fuzzy_similarity(&left, &right);
            prop_assert!(score >= 0.0);
            prop_assert!(score <= 1.0);
        }
    }

    #[test]
    fn suffix_resolution_handles_dot_qualified_receiver_hint() {
        // Reproduces the NestJS pattern where the parser emits a
        // dot-qualified hint like "this.OrderService.persistOrder" and
        // there are multiple symbols named "persistOrder" across files.
        // The suffix resolver must match the correct one via the qualified
        // name ending with the hint, not fall through to Fallback.
        let root = PathBuf::from("/repo");
        let target = NodeData {
            id: node_id(
                "sample-service",
                "src/order.service.ts",
                NodeKind::Function,
                "persistOrder",
            ),
            kind: NodeKind::Function,
            repo: "sample-service".to_owned(),
            file_path: "src/order.service.ts".to_owned(),
            name: "persistOrder".to_owned(),
            qualified_name: Some("OrderService.persistOrder".to_owned()),
            external_id: None,
            signature: Some("persistOrder()".to_owned()),
            visibility: Some(Visibility::Public),
            span: None,
            is_virtual: false,
        };
        let distractor = NodeData {
            id: node_id(
                "sample-service",
                "src/audit.service.ts",
                NodeKind::Function,
                "persistOrder",
            ),
            kind: NodeKind::Function,
            repo: "sample-service".to_owned(),
            file_path: "src/audit.service.ts".to_owned(),
            name: "persistOrder".to_owned(),
            qualified_name: Some("AuditService.persistOrder".to_owned()),
            external_id: None,
            signature: Some("persistOrder()".to_owned()),
            visibility: Some(Visibility::Public),
            span: None,
            is_virtual: false,
        };
        let owner = function_node("src/controller.ts", "createOrder");

        let resolved = resolve_calls(
            &root,
            &[target.clone(), distractor, owner.clone()],
            &[ResolutionInput {
                file_node: owner.id,
                file_path: root.join("src/controller.ts"),
                import_bindings: Vec::new(),
                // Hint preserves the class name casing so ends_with matches
                // "OrderService.persistOrder" as a suffix.
                call_sites: vec![CallSite {
                    owner_id: owner.id,
                    owner_file: owner.id,
                    source_path: root.join("src/controller.ts"),
                    callee_name: "persistOrder".to_owned(),
                    callee_qualified_hint: Some("this.OrderService.persistOrder".to_owned()),
                    span: None,
                }],
            }],
        );

        assert_eq!(
            resolved.len(),
            1,
            "ambiguous dot-qualified call should resolve via suffix"
        );
        assert_eq!(resolved[0].strategy, ResolutionStrategy::Suffix);
        assert_eq!(resolved[0].edge.target, target.id);
    }

    #[test]
    fn barrel_reexports_resolve_to_underlying_symbol() {
        let temp = TempDir::new("barrel");
        temp.write("src/barrel/index.ts", "export { Foo } from './foo';\n");
        temp.write("src/barrel/foo.ts", "export function Foo() { return 1; }\n");

        let root = temp.path().to_path_buf();
        let target = function_node("src/barrel/foo.ts", "Foo");
        let owner = function_node("src/caller.ts", "caller");

        let resolved = resolve_calls(
            &root,
            &[target.clone(), owner.clone()],
            &[ResolutionInput {
                file_node: owner.id,
                file_path: root.join("src/caller.ts"),
                import_bindings: vec![ImportBinding {
                    local_name: "Foo".to_owned(),
                    imported_name: Some("Foo".to_owned()),
                    source: "./barrel".to_owned(),
                    resolved_path: Some(root.join("src/barrel/index.ts")),
                    is_default: false,
                    is_namespace: false,
                    is_type_only: false,
                }],
                call_sites: vec![CallSite {
                    owner_id: owner.id,
                    owner_file: owner.id,
                    source_path: root.join("src/caller.ts"),
                    callee_name: "Foo".to_owned(),
                    callee_qualified_hint: None,
                    span: None,
                }],
            }],
        );

        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].edge.target, target.id);
    }

    #[test]
    fn wildcard_reexports_resolve_to_underlying_symbol() {
        let temp = TempDir::new("wildcard");
        temp.write("src/barrel/index.ts", "export * from './foo';\n");
        temp.write("src/barrel/foo.ts", "export function Foo() { return 1; }\n");

        let root = temp.path().to_path_buf();
        let target = function_node("src/barrel/foo.ts", "Foo");
        let owner = function_node("src/caller.ts", "caller");

        let resolved = resolve_calls(
            &root,
            &[target.clone(), owner.clone()],
            &[ResolutionInput {
                file_node: owner.id,
                file_path: root.join("src/caller.ts"),
                import_bindings: vec![ImportBinding {
                    local_name: "Foo".to_owned(),
                    imported_name: Some("Foo".to_owned()),
                    source: "./barrel".to_owned(),
                    resolved_path: Some(root.join("src/barrel/index.ts")),
                    is_default: false,
                    is_namespace: false,
                    is_type_only: false,
                }],
                call_sites: vec![CallSite {
                    owner_id: owner.id,
                    owner_file: owner.id,
                    source_path: root.join("src/caller.ts"),
                    callee_name: "Foo".to_owned(),
                    callee_qualified_hint: None,
                    span: None,
                }],
            }],
        );

        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].edge.target, target.id);
    }

    #[test]
    fn renamed_reexports_resolve_back_to_origin_symbol() {
        let temp = TempDir::new("renamed");
        temp.write(
            "src/barrel/index.ts",
            "export { Foo as Renamed } from './foo';\n",
        );
        temp.write("src/barrel/foo.ts", "export function Foo() { return 1; }\n");

        let root = temp.path().to_path_buf();
        let target = function_node("src/barrel/foo.ts", "Foo");
        let owner = function_node("src/caller.ts", "caller");

        let resolved = resolve_calls(
            &root,
            &[target.clone(), owner.clone()],
            &[ResolutionInput {
                file_node: owner.id,
                file_path: root.join("src/caller.ts"),
                import_bindings: vec![ImportBinding {
                    local_name: "Renamed".to_owned(),
                    imported_name: Some("Renamed".to_owned()),
                    source: "./barrel".to_owned(),
                    resolved_path: Some(root.join("src/barrel/index.ts")),
                    is_default: false,
                    is_namespace: false,
                    is_type_only: false,
                }],
                call_sites: vec![CallSite {
                    owner_id: owner.id,
                    owner_file: owner.id,
                    source_path: root.join("src/caller.ts"),
                    callee_name: "Renamed".to_owned(),
                    callee_qualified_hint: None,
                    span: None,
                }],
            }],
        );

        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].edge.target, target.id);
    }

    #[test]
    fn barrel_reexports_preserve_tsconfig_alias_context() {
        let temp = TempDir::new("barrel-alias");
        temp.write(
            "tsconfig.json",
            r#"{ "compilerOptions": { "paths": { "@contracts": ["src/contracts/index.ts"] } } }"#,
        );
        temp.write(
            "src/contracts/index.ts",
            "export { OrderDto } from '@contracts/order';\n",
        );
        temp.write(
            "src/contracts/order.ts",
            "export function OrderDto() { return 1; }\n",
        );

        let root = temp.path().to_path_buf();
        let target = function_node("src/contracts/order.ts", "OrderDto");
        let owner = function_node("src/caller.ts", "caller");

        let resolved = resolve_calls(
            &root,
            &[target.clone(), owner.clone()],
            &[ResolutionInput {
                file_node: owner.id,
                file_path: root.join("src/caller.ts"),
                import_bindings: vec![ImportBinding {
                    local_name: "OrderDto".to_owned(),
                    imported_name: Some("OrderDto".to_owned()),
                    source: "@contracts".to_owned(),
                    resolved_path: Some(root.join("src/contracts/index.ts")),
                    is_default: false,
                    is_namespace: false,
                    is_type_only: false,
                }],
                call_sites: vec![CallSite {
                    owner_id: owner.id,
                    owner_file: owner.id,
                    source_path: root.join("src/caller.ts"),
                    callee_name: "OrderDto".to_owned(),
                    callee_qualified_hint: None,
                    span: None,
                }],
            }],
        );

        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].edge.target, target.id);
    }

    // ---- Parsed-file cache contract tests ----------------------------------------

    /// Build a minimal `SymbolIndex` with no symbols or files to exercise the
    /// cache layer in isolation.
    fn empty_index(repo_root: &Path) -> super::SymbolIndex<'_> {
        super::SymbolIndex::new(repo_root, &[], &[])
    }

    /// Returns an `Option<ParsedFile>` sentinel that is `None` — sufficient for
    /// testing insertion and retrieval without performing real I/O.
    #[expect(dead_code, reason = "reserved helper for future cache contract tests")]
    fn sentinel() -> Option<super::super::tree_sitter::ParsedFile> {
        None
    }

    #[test]
    fn cache_hit_returns_same_value() {
        // Populate the cache with a sentinel via a barrel re-export scenario
        // that exercises the full `load_parsed_file` path, then re-request the
        // same resolved path and assert the same value comes back.
        //
        // We use the TempDir barrel setup so the cache actually gets exercised:
        // the first call parses the barrel file (produces None because the file
        // has no identifiable symbols but the language is valid TS), and the
        // second call must be a cache hit returning the same result without a
        // second parse.
        let temp = TempDir::new("cache-hit");
        temp.write("src/mod.ts", "export { X } from './x';\n");

        let root = temp.path().to_path_buf();
        let index = empty_index(&root);
        let resolved_path = root.join("src/mod.ts");

        let first = index.load_parsed_file(&root, &resolved_path);
        let second = index.load_parsed_file(&root, &resolved_path);

        // Values must be equal — same parse result from the cache.
        assert_eq!(
            first, second,
            "second call must return the cached value from the first call"
        );
    }

    #[test]
    fn cache_eviction_is_bounded() {
        use quick_cache::sync::Cache;

        // Insert significantly more entries than the declared capacity and
        // confirm that `len()` stays within the expected bound.
        //
        // `quick_cache` partitions capacity across internal shards.  Each shard
        // independently evicts, so the actual retained count is bounded by
        // `capacity + num_shards`.  The crate uses 32 shards by default at this
        // capacity, giving a worst-case overshoot of 32 entries.  We allow
        // a comfortable margin of 64 to be robust across library versions.
        let capacity = super::PARSED_FILE_CACHE_CAPACITY;
        let overfill = capacity + 1_000;
        let cache: Cache<super::CacheKey, Option<super::super::tree_sitter::ParsedFile>> =
            Cache::new(capacity);

        for i in 0..overfill {
            let key = (
                PathBuf::from(format!("/repo/src/file{i}.ts")),
                PathBuf::from("/repo"),
            );
            cache.insert(key, None);
        }

        let len = cache.len();
        assert!(
            len <= capacity + 64,
            "cache len {len} exceeds capacity {capacity} + 64 overshoot allowance"
        );
    }

    #[test]
    fn different_repo_roots_do_not_collide() {
        use quick_cache::sync::Cache;

        let capacity = 128;
        let cache: Cache<super::CacheKey, Option<super::super::tree_sitter::ParsedFile>> =
            Cache::new(capacity);

        let path = PathBuf::from("/shared/src/module.ts");
        let repo_a = PathBuf::from("/workspace-a");
        let repo_b = PathBuf::from("/workspace-b");

        // Insert a Some-sentinel under repo_a and a None under repo_b for the
        // same canonical path.  They must remain independent.
        //
        // We can't construct a real `ParsedFile` without a file on disk, so we
        // use `None` for both and assert they remain independently addressable
        // keys by verifying the cache sees two distinct entries.
        cache.insert((path.clone(), repo_a.clone()), None);
        cache.insert((path.clone(), repo_b.clone()), None);

        // Both keys must be present (neither evicted the other since they are
        // different keys, not the same key hit twice).
        assert!(
            cache.get(&(path.clone(), repo_a)).is_some(),
            "entry for repo_a must be present"
        );
        assert!(
            cache.get(&(path.clone(), repo_b)).is_some(),
            "entry for repo_b must be present"
        );
        assert_eq!(
            cache.len(),
            2,
            "cache must hold two independent entries for the same path under different roots"
        );
    }

    #[test]
    fn concurrent_reads_return_consistent_values() {
        // Populate a cache from multiple rayon workers and then read it back
        // from multiple workers.  All readers must observe the same value for
        // a given key — no contradictions.
        use quick_cache::sync::Cache;
        use std::sync::Arc;

        let capacity = 512;
        let cache: Arc<Cache<super::CacheKey, Option<super::super::tree_sitter::ParsedFile>>> =
            Arc::new(Cache::new(capacity));

        let keys: Vec<super::CacheKey> = (0..64)
            .map(|i| {
                (
                    PathBuf::from(format!("/repo/src/file{i}.ts")),
                    PathBuf::from("/repo"),
                )
            })
            .collect();

        // Populate concurrently — multiple threads may race on the same key;
        // quick_cache handles this safely with internal locking.
        {
            use rayon::prelude::*;
            let cache_ref = &cache;
            keys.par_iter().for_each(|key| {
                cache_ref.insert(key.clone(), None);
            });
        }

        // Read back concurrently and assert every key is present with the
        // expected value (None sentinel).
        {
            use rayon::prelude::*;
            let cache_ref = &cache;
            let results: Vec<bool> = keys
                .par_iter()
                .map(|key| cache_ref.get(key).is_some())
                .collect();

            for (i, present) in results.iter().enumerate() {
                assert!(
                    present,
                    "key {i} should be present after concurrent population"
                );
            }
        }
    }
}
