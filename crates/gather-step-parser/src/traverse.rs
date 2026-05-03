use std::{
    fs,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::UNIX_EPOCH,
};

use globset::{Glob, GlobSet, GlobSetBuilder};
use rustc_hash::FxHashMap;

use gather_step_core::{IndexingConfig, PathId};
use ignore::{WalkBuilder, WalkState};
use thiserror::Error;

const BINARY_SNIFF_BYTES: usize = 8 * 1024;
const DEFAULT_MAX_FILE_SIZE_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Language {
    TypeScript,
    JavaScript,
    Python,
    Rust,
    Go,
    Java,
}

impl Language {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TypeScript => "typescript",
            Self::JavaScript => "javascript",
            Self::Python => "python",
            Self::Rust => "rust",
            Self::Go => "go",
            Self::Java => "java",
        }
    }
}

impl FromStr for Language {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        if ["ts", "tsx", "typescript"]
            .iter()
            .any(|candidate| value.eq_ignore_ascii_case(candidate))
        {
            Ok(Self::TypeScript)
        } else if ["js", "jsx", "mjs", "cjs", "javascript"]
            .iter()
            .any(|candidate| value.eq_ignore_ascii_case(candidate))
        {
            Ok(Self::JavaScript)
        } else if ["py", "pyi", "python"]
            .iter()
            .any(|candidate| value.eq_ignore_ascii_case(candidate))
        {
            Ok(Self::Python)
        } else if ["rs", "rust"]
            .iter()
            .any(|candidate| value.eq_ignore_ascii_case(candidate))
        {
            Ok(Self::Rust)
        } else if value.eq_ignore_ascii_case("go") {
            Ok(Self::Go)
        } else if value.eq_ignore_ascii_case("java") {
            Ok(Self::Java)
        } else {
            Err(())
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileEntry {
    pub path: PathBuf,
    pub language: Language,
    pub size_bytes: u64,
    pub content_hash: [u8; 32],
    /// Source bytes captured at traversal time.  Set by `collect_repo_files`
    /// and `collect_selected_repo_files` so that the parser can skip a second
    /// `fs::read` call for the same file.  `None` when the entry was
    /// constructed without a traversal pass (e.g. in unit tests or for
    /// imported-file resolution).
    pub source_bytes: Option<std::sync::Arc<[u8]>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FileStat {
    pub size_bytes: i64,
    pub mtime_ns: i64,
}

#[derive(Clone, Debug, Default)]
pub struct TraversalSummary {
    pub files: Vec<FileEntry>,
    /// Maps raw `OsStr` bytes of each file path to its filesystem metadata.
    /// No ordering is required — lookups are pure key→value; `FxHashMap`
    /// avoids the O(log n) `BTreeMap` cost at the call sites in the indexer.
    pub file_stats: FxHashMap<Vec<u8>, FileStat>,
    pub skipped_binary: usize,
    pub skipped_too_large: usize,
    pub skipped_unsupported: usize,
    pub skipped_excluded: usize,
}

/// Configuration for a repository traversal.
///
/// Exclude patterns are compiled into a [`GlobSet`] once at construction time
/// and reused for every path check, avoiding repeated compilation overhead in
/// hot traversal loops.
///
/// Pattern promotion rules applied at construction time:
///
/// - If the pattern already contains `**`, it is used verbatim.
/// - If the pattern contains `/` (but no `**`), two globs are added: the
///   pattern as-is (exact relative-path match) and `{pattern}/**` (anything
///   inside that subtree).
/// - Otherwise (plain name or `*.ext`), two globs are added: `**/{pattern}`
///   (the name appearing at any depth) and `**/{pattern}/**` (anything inside
///   a directory of that name at any depth).  This preserves the previous
///   behaviour where `node_modules` matched any path component named
///   `node_modules`.
#[derive(Clone, Debug)]
pub struct TraverseConfig {
    exclude: Vec<String>,
    /// Pre-compiled matcher built from `exclude`.  Stored here so that
    /// `is_index_relevant_path` (called per-path from the file-system watcher)
    /// never re-compiles the patterns.
    exclude_matcher: GlobSet,
    include_languages: Vec<Language>,
    include_dotfiles: bool,
    max_file_size_bytes: u64,
}

/// Equality is defined by the *source* configuration fields.  The compiled
/// [`GlobSet`] is a deterministic function of `exclude`, so it does not
/// participate in the comparison.
impl PartialEq for TraverseConfig {
    fn eq(&self, other: &Self) -> bool {
        self.exclude == other.exclude
            && self.include_languages == other.include_languages
            && self.include_dotfiles == other.include_dotfiles
            && self.max_file_size_bytes == other.max_file_size_bytes
    }
}

impl Eq for TraverseConfig {}

/// Build a [`GlobSet`] from a list of raw exclude pattern strings.
///
/// Each pattern is promoted to one or more globs that correctly match
/// relative file paths anywhere within a repository root:
///
/// - Patterns already containing `**` are used verbatim.
/// - Patterns containing `/` (slash-anchored) expand to two entries:
///   `{pattern}` and `{pattern}/**`.
/// - Plain names and `*.ext` patterns expand to `**/{pattern}` and
///   `**/{pattern}/**` so they match at any depth.
///
/// All globs are case-sensitive (no `GlobBuilder::case_insensitive` override).
fn build_exclude_matcher(patterns: &[String]) -> GlobSet {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        let normalized = pattern.trim().replace('\\', "/");
        if normalized.is_empty() {
            continue;
        }
        if normalized.contains("**") {
            // Already contains a double-star; trust the user's intent.
            if let Ok(g) = Glob::new(&normalized) {
                builder.add(g);
            }
        } else if normalized.contains('/') {
            // Slash-anchored: treat as a relative path prefix.
            if let Ok(g) = Glob::new(&normalized) {
                builder.add(g);
            }
            if let Ok(g) = Glob::new(&format!("{normalized}/**")) {
                builder.add(g);
            }
        } else {
            // Plain name or `*.ext`: match anywhere in the tree.
            if let Ok(g) = Glob::new(&format!("**/{normalized}")) {
                builder.add(g);
            }
            if let Ok(g) = Glob::new(&format!("**/{normalized}/**")) {
                builder.add(g);
            }
        }
    }
    // A build failure here would require an internal logic error (the patterns
    // above are all valid globs), so we fall back to an empty matcher.
    builder.build().unwrap_or_else(|_| GlobSet::empty())
}

impl TraverseConfig {
    pub fn from_indexing_config(config: &IndexingConfig) -> Result<Self, TraverseError> {
        let include_languages = config
            .include_languages
            .iter()
            .map(|value| {
                Language::from_str(value).map_err(|()| TraverseError::UnsupportedLanguageFilter {
                    value: value.clone(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let max_file_size_bytes = parse_byte_size(&config.max_file_size).ok_or_else(|| {
            TraverseError::InvalidSizeThreshold {
                value: config.max_file_size.clone(),
            }
        })?;

        let exclude_matcher = build_exclude_matcher(&config.exclude);

        Ok(Self {
            exclude_matcher,
            exclude: config.exclude.clone(),
            include_languages,
            include_dotfiles: config.include_dotfiles,
            max_file_size_bytes,
        })
    }

    #[must_use]
    pub fn new(
        exclude: Vec<String>,
        include_languages: Vec<Language>,
        include_dotfiles: bool,
        max_file_size_bytes: u64,
    ) -> Self {
        let exclude_matcher = build_exclude_matcher(&exclude);
        Self {
            exclude,
            exclude_matcher,
            include_languages,
            include_dotfiles,
            max_file_size_bytes,
        }
    }

    #[must_use]
    pub const fn max_file_size_bytes(&self) -> u64 {
        self.max_file_size_bytes
    }

    #[must_use]
    pub const fn include_dotfiles(&self) -> bool {
        self.include_dotfiles
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn byte_buffer_capacity(size_bytes: u64, config: &TraverseConfig) -> usize {
    usize::try_from(size_bytes.min(config.max_file_size_bytes())).unwrap_or(usize::MAX)
}

impl Default for TraverseConfig {
    fn default() -> Self {
        Self::new(
            vec![
                "node_modules".to_owned(),
                "dist".to_owned(),
                "*.min.js".to_owned(),
                "*.map".to_owned(),
                "*.lock".to_owned(),
                "*.d.ts".to_owned(),
            ],
            Vec::new(),
            false,
            DEFAULT_MAX_FILE_SIZE_BYTES,
        )
    }
}

impl TraverseConfig {
    #[must_use]
    pub fn is_index_relevant_path(&self, path: &Path) -> bool {
        let normalized = PathBuf::from(path);
        if self.exclude_matcher.is_match(&normalized) {
            return false;
        }

        let file_name = normalized.file_name().and_then(std::ffi::OsStr::to_str);
        // `.gather-step.local.yaml` is intentionally excluded from the parser
        // allow-list: changes to it should be picked up only by the dedicated
        // `local_config::LocalConfig::load` path, not by the source walker.
        if file_name.is_some_and(|name| name == ".gather-step.local.yaml") {
            return false;
        }
        if file_name
            .is_some_and(|name| matches!(name, "package.json" | "tsconfig.json" | ".gitignore"))
        {
            return true;
        }

        if is_binary_path(&normalized) {
            return false;
        }

        let Some(language) = classify_language(&normalized) else {
            return false;
        };

        self.include_languages.is_empty() || self.include_languages.contains(&language)
    }
}

#[derive(Debug, Error)]
pub enum TraverseError {
    #[error("failed to read traversed path {path}: {source}")]
    ReadPath {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to open traversed file {path}: {source}")]
    ReadFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid max file size threshold `{value}`")]
    InvalidSizeThreshold { value: String },
    #[error("unsupported include_languages value `{value}`")]
    UnsupportedLanguageFilter { value: String },
    #[error("walk error while traversing {root}: {source}")]
    Walk {
        root: PathBuf,
        #[source]
        source: ignore::Error,
    },
    #[error("repo root {root} is a symlink and cannot be traversed")]
    SymlinkRoot { root: PathBuf },
}

/// Per-worker accumulator flushed to the shared sink on `Drop`.
///
/// Each parallel walk worker owns one `WorkerGuard`.  When the worker is done
/// (the closure captured by `ignore::WalkParallel::run` is dropped), the guard
/// pushes its accumulated `TraversalSummary` and error list to the shared
/// vectors, taking the mutex exactly once per worker instead of once per file.
struct WorkerGuard {
    summary: TraversalSummary,
    errors: Vec<TraverseError>,
    sink_summaries: Arc<Mutex<Vec<TraversalSummary>>>,
    sink_errors: Arc<Mutex<Vec<TraverseError>>>,
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        let summary = std::mem::take(&mut self.summary);
        lock_unpoisoned(&self.sink_summaries).push(summary);
        if !self.errors.is_empty() {
            let mut errs = std::mem::take(&mut self.errors);
            lock_unpoisoned(&self.sink_errors).append(&mut errs);
        }
    }
}

pub fn collect_repo_files(
    root: impl AsRef<Path>,
    config: &TraverseConfig,
) -> Result<TraversalSummary, TraverseError> {
    let root = root.as_ref().to_path_buf();
    let metadata = fs::symlink_metadata(&root).map_err(|source| TraverseError::ReadPath {
        path: root.clone(),
        source,
    })?;
    if metadata.file_type().is_symlink() {
        return Err(TraverseError::SymlinkRoot { root });
    }

    let mut builder = WalkBuilder::new(&root);
    builder.hidden(!config.include_dotfiles);
    builder.follow_links(false);
    builder.git_ignore(true);
    builder.git_exclude(true);
    builder.git_global(true);
    builder.require_git(false);

    // Parallel traversal via `ignore::WalkParallel`.
    // Each worker owns a `WorkerGuard` whose `Drop` impl flushes the
    // accumulated per-worker `TraversalSummary` to the shared sink.
    let per_worker_summaries: Arc<Mutex<Vec<TraversalSummary>>> = Arc::new(Mutex::new(Vec::new()));
    let per_worker_errors: Arc<Mutex<Vec<TraverseError>>> = Arc::new(Mutex::new(Vec::new()));
    let root_arc = Arc::new(root.clone());
    let config_arc = Arc::new(config.clone());

    builder.build_parallel().run(|| {
        let root = Arc::clone(&root_arc);
        let config = Arc::clone(&config_arc);

        // Each worker gets its own guard — no per-file locking needed.
        let mut guard = WorkerGuard {
            summary: TraversalSummary {
                files: Vec::new(),
                file_stats: FxHashMap::default(),
                skipped_binary: 0,
                skipped_too_large: 0,
                skipped_unsupported: 0,
                skipped_excluded: 0,
            },
            errors: Vec::new(),
            sink_summaries: Arc::clone(&per_worker_summaries),
            sink_errors: Arc::clone(&per_worker_errors),
        };

        Box::new(move |entry_result| {
            let entry = match entry_result {
                Ok(entry) => entry,
                Err(source) => {
                    guard.errors.push(TraverseError::Walk {
                        root: (*root).clone(),
                        source,
                    });
                    return WalkState::Quit;
                }
            };

            let path = entry.path();
            if path == root.as_path() || entry.file_type().is_some_and(|kind| kind.is_dir()) {
                return WalkState::Continue;
            }
            if entry.path_is_symlink() {
                return WalkState::Continue;
            }

            let relative_path = match path.strip_prefix(root.as_path()) {
                Ok(rel) => rel.to_path_buf(),
                Err(_) => return WalkState::Continue,
            };

            if config.exclude_matcher.is_match(&relative_path) {
                guard.summary.skipped_excluded += 1;
                return WalkState::Continue;
            }

            let mut file = match fs::File::open(path) {
                Ok(file) => file,
                Err(source) => {
                    guard.errors.push(TraverseError::ReadPath {
                        path: path.to_path_buf(),
                        source,
                    });
                    return WalkState::Quit;
                }
            };
            let metadata = match file.metadata() {
                Ok(metadata) => metadata,
                Err(source) => {
                    guard.errors.push(TraverseError::ReadPath {
                        path: path.to_path_buf(),
                        source,
                    });
                    return WalkState::Quit;
                }
            };
            if metadata.len() > config.max_file_size_bytes {
                guard.summary.skipped_too_large += 1;
                return WalkState::Continue;
            }

            let mut bytes = Vec::with_capacity(byte_buffer_capacity(metadata.len(), &config));
            if let Err(source) = std::io::Read::read_to_end(&mut file, &mut bytes) {
                guard.errors.push(TraverseError::ReadFile {
                    path: path.to_path_buf(),
                    source,
                });
                return WalkState::Quit;
            }
            if is_binary_path(&relative_path) || is_binary(&bytes) {
                guard.summary.skipped_binary += 1;
                return WalkState::Continue;
            }

            let Some(language) = classify_language(&relative_path) else {
                guard.summary.skipped_unsupported += 1;
                return WalkState::Continue;
            };

            if !config.include_languages.is_empty() && !config.include_languages.contains(&language)
            {
                guard.summary.skipped_excluded += 1;
                return WalkState::Continue;
            }

            let content_hash = hash_bytes(&bytes);
            let source_bytes: std::sync::Arc<[u8]> = bytes.into_boxed_slice().into();
            let file = FileEntry {
                path: relative_path.clone(),
                language,
                size_bytes: metadata.len(),
                content_hash,
                source_bytes: Some(source_bytes),
            };
            let path_id_bytes = PathId::from_path(&relative_path).as_bytes().to_vec();
            guard
                .summary
                .file_stats
                .insert(path_id_bytes, file_stat(&metadata));
            guard.summary.files.push(file);
            WalkState::Continue
        })
    });

    // After `run` returns, `ignore` has dropped all worker closures and each
    // `WorkerGuard` has flushed its accumulated data.  Merge now.
    let worker_summaries = Arc::try_unwrap(per_worker_summaries)
        .expect("per_worker_summaries Arc should have a single strong ref after walk")
        .into_inner()
        .unwrap_or_else(PoisonError::into_inner);
    let collected_errors = Arc::try_unwrap(per_worker_errors)
        .expect("per_worker_errors Arc should have a single strong ref after walk")
        .into_inner()
        .unwrap_or_else(PoisonError::into_inner);

    if let Some(error) = collected_errors.into_iter().next() {
        return Err(error);
    }

    let mut summary = worker_summaries
        .into_iter()
        .reduce(|mut acc, part| {
            acc.files.extend(part.files);
            acc.file_stats.extend(part.file_stats);
            acc.skipped_binary += part.skipped_binary;
            acc.skipped_too_large += part.skipped_too_large;
            acc.skipped_unsupported += part.skipped_unsupported;
            acc.skipped_excluded += part.skipped_excluded;
            acc
        })
        .unwrap_or_default();

    // Parallel traversal visits files in non-deterministic order, but
    // downstream code (and tests) depend on stable ordering. Sorting here
    // restores determinism for roughly the same cost as the previous
    // end-of-function sort.
    summary
        .files
        .sort_by(|left, right| left.path.cmp(&right.path));
    Ok(summary)
}

pub fn collect_selected_repo_files(
    root: impl AsRef<Path>,
    paths: &[PathBuf],
    config: &TraverseConfig,
) -> Result<TraversalSummary, TraverseError> {
    let root = root.as_ref().to_path_buf();
    let mut summary = TraversalSummary {
        files: Vec::new(),
        file_stats: FxHashMap::default(),
        skipped_binary: 0,
        skipped_too_large: 0,
        skipped_unsupported: 0,
        skipped_excluded: 0,
    };

    let mut seen = std::collections::BTreeSet::new();
    for relative_path in paths {
        let normalized = relative_path.components().collect::<PathBuf>();
        if normalized.as_os_str().is_empty()
            || normalized.is_absolute()
            || normalized
                .components()
                .any(|component| matches!(component, std::path::Component::ParentDir))
            || !seen.insert(normalized.clone())
        {
            continue;
        }

        if config.exclude_matcher.is_match(&normalized) {
            summary.skipped_excluded += 1;
            continue;
        }
        if is_binary_path(&normalized) {
            summary.skipped_binary += 1;
            continue;
        }

        let full_path = root.join(&normalized);
        let metadata = match fs::symlink_metadata(&full_path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => {
                return Err(TraverseError::ReadPath {
                    path: full_path,
                    source,
                });
            }
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            continue;
        }
        let mut file = match fs::File::open(&full_path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => {
                return Err(TraverseError::ReadPath {
                    path: full_path,
                    source,
                });
            }
        };
        let file_metadata = match file.metadata() {
            Ok(metadata) => metadata,
            Err(source) => {
                return Err(TraverseError::ReadPath {
                    path: full_path,
                    source,
                });
            }
        };
        if file_metadata.len() > config.max_file_size_bytes {
            summary.skipped_too_large += 1;
            continue;
        }

        let mut bytes = Vec::with_capacity(byte_buffer_capacity(file_metadata.len(), config));
        std::io::Read::read_to_end(&mut file, &mut bytes).map_err(|source| {
            TraverseError::ReadFile {
                path: full_path.clone(),
                source,
            }
        })?;
        if is_binary(&bytes) {
            summary.skipped_binary += 1;
            continue;
        }

        let Some(language) = classify_language(&normalized) else {
            summary.skipped_unsupported += 1;
            continue;
        };
        if !config.include_languages.is_empty() && !config.include_languages.contains(&language) {
            summary.skipped_unsupported += 1;
            continue;
        }

        let source_bytes: std::sync::Arc<[u8]> = bytes.clone().into();
        let path_id_bytes = PathId::from_path(&normalized).as_bytes().to_vec();
        summary
            .file_stats
            .insert(path_id_bytes, file_stat(&file_metadata));
        summary.files.push(FileEntry {
            path: normalized,
            language,
            size_bytes: file_metadata.len(),
            content_hash: hash_bytes(&bytes),
            source_bytes: Some(source_bytes),
        });
    }

    Ok(summary)
}

#[must_use]
pub fn classify_language(path: impl AsRef<Path>) -> Option<Language> {
    let extension = path
        .as_ref()
        .extension()
        .and_then(std::ffi::OsStr::to_str)?;

    if ["ts", "tsx", "mts", "cts"]
        .iter()
        .any(|candidate| extension.eq_ignore_ascii_case(candidate))
    {
        Some(Language::TypeScript)
    } else if ["js", "jsx", "mjs", "cjs"]
        .iter()
        .any(|candidate| extension.eq_ignore_ascii_case(candidate))
        || (["json", "yaml", "yml"]
            .iter()
            .any(|candidate| extension.eq_ignore_ascii_case(candidate))
            && is_static_mapping_path(path.as_ref()))
    {
        Some(Language::JavaScript)
    } else if ["py", "pyi"]
        .iter()
        .any(|candidate| extension.eq_ignore_ascii_case(candidate))
    {
        Some(Language::Python)
    } else if extension.eq_ignore_ascii_case("rs") {
        Some(Language::Rust)
    } else if extension.eq_ignore_ascii_case("go") {
        Some(Language::Go)
    } else if extension.eq_ignore_ascii_case("java") {
        Some(Language::Java)
    } else {
        None
    }
}

fn is_static_mapping_path(path: &Path) -> bool {
    let Some(file_name) = path.file_name().and_then(std::ffi::OsStr::to_str) else {
        return false;
    };
    ["mapping", "index", "search", "projection"]
        .iter()
        .any(|token| contains_ascii_case(file_name, token))
}

fn contains_ascii_case(haystack: &str, needle: &str) -> bool {
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle.as_bytes()))
}

fn is_binary_path(path: &Path) -> bool {
    const BINARY_EXTENSIONS: &[&str] = &[
        "png", "jpg", "jpeg", "gif", "webp", "ico", "bmp", "tiff", "woff", "woff2", "ttf", "eot",
        "otf", "pdf", "zip", "gz", "tar", "jar", "class", "exe", "dll", "so", "dylib",
    ];

    path.extension()
        .and_then(std::ffi::OsStr::to_str)
        .is_some_and(|extension| {
            BINARY_EXTENSIONS
                .iter()
                .any(|candidate| extension.eq_ignore_ascii_case(candidate))
        })
}

fn is_binary(bytes: &[u8]) -> bool {
    bytes.iter().take(BINARY_SNIFF_BYTES).any(|byte| *byte == 0)
}

fn file_stat(metadata: &fs::Metadata) -> FileStat {
    FileStat {
        size_bytes: i64::try_from(metadata.len()).unwrap_or(i64::MAX),
        mtime_ns: metadata_mtime_ns(metadata),
    }
}

fn metadata_mtime_ns(metadata: &fs::Metadata) -> i64 {
    metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

fn hash_bytes(bytes: &[u8]) -> [u8; 32] {
    // blake3 matches the hash algorithm used by `gather-step-core` (node IDs)
    // and `gather-step-storage` (graph store) — keeping one hash across the
    // workspace means file-level content hashes can be compared directly
    // against any stored hash without algorithm translation.
    *blake3::hash(bytes).as_bytes()
}

fn parse_byte_size(value: &str) -> Option<u64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    let split_index = trimmed
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (digits, unit) = trimmed.split_at(split_index);
    if digits.is_empty() {
        return None;
    }

    let number = digits.parse::<u64>().ok()?;
    let multiplier = match unit.trim().to_ascii_uppercase().as_str() {
        "" | "B" => 1,
        "KB" => 1024,
        "MB" => 1024 * 1024,
        "GB" => 1024 * 1024 * 1024,
        _ => return None,
    };

    number.checked_mul(multiplier)
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::IndexingConfig;
    use pretty_assertions::assert_eq;

    use super::{
        DEFAULT_MAX_FILE_SIZE_BYTES, Language, TraverseConfig, TraverseError, classify_language,
        collect_repo_files, collect_selected_repo_files, parse_byte_size,
    };

    static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> Self {
            let counter = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-parser-traverse-{name}-{}-{counter}",
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

    #[test]
    fn walk_respects_gitignore_and_explicit_excludes() {
        let temp_dir = TestDir::new("gitignore");
        fs::write(temp_dir.path().join(".gitignore"), "ignored.ts\n").expect("gitignore writes");
        fs::create_dir_all(temp_dir.path().join("src")).expect("src directory");
        fs::create_dir_all(temp_dir.path().join("node_modules/pkg")).expect("node_modules");
        fs::write(
            temp_dir.path().join("src/app.ts"),
            "export const app = 1;\n",
        )
        .expect("app");
        fs::write(
            temp_dir.path().join("ignored.ts"),
            "export const bad = 1;\n",
        )
        .expect("ignored file");
        fs::write(
            temp_dir.path().join("node_modules/pkg/index.ts"),
            "export const dep = 1;\n",
        )
        .expect("dependency");

        let summary =
            collect_repo_files(temp_dir.path(), &TraverseConfig::default()).expect("walk passes");

        assert_eq!(summary.files.len(), 1);
        assert_eq!(summary.files[0].path, PathBuf::from("src/app.ts"));
    }

    #[test]
    fn walk_skips_binary_files() {
        let temp_dir = TestDir::new("binary");
        fs::create_dir_all(temp_dir.path().join("assets")).expect("assets directory");
        fs::write(
            temp_dir.path().join("assets/icon.png"),
            [137, 80, 78, 71, 0, 1, 2, 3],
        )
        .expect("binary file");
        fs::write(temp_dir.path().join("main.ts"), "export const ok = true;\n").expect("ts file");

        let summary =
            collect_repo_files(temp_dir.path(), &TraverseConfig::default()).expect("walk passes");

        assert_eq!(summary.files.len(), 1);
        assert_eq!(summary.skipped_binary, 1);
        assert_eq!(summary.files[0].path, PathBuf::from("main.ts"));
    }

    #[test]
    fn walk_skips_symlinked_files_and_directories() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let temp_dir = TestDir::new("symlinks");
            fs::create_dir_all(temp_dir.path().join("src")).expect("src directory");
            fs::create_dir_all(temp_dir.path().join("linked")).expect("linked directory");
            fs::write(
                temp_dir.path().join("src/app.ts"),
                "export const app = 1;\n",
            )
            .expect("real ts file");
            fs::write(
                temp_dir.path().join("linked/inner.ts"),
                "export const inner = 1;\n",
            )
            .expect("linked target");
            symlink(
                temp_dir.path().join("src/app.ts"),
                temp_dir.path().join("src/app-link.ts"),
            )
            .expect("file symlink");
            symlink(
                temp_dir.path().join("linked"),
                temp_dir.path().join("src/linked-dir"),
            )
            .expect("directory symlink");

            let summary = collect_repo_files(temp_dir.path(), &TraverseConfig::default())
                .expect("walk passes");

            assert_eq!(summary.files.len(), 2);
            assert_eq!(
                summary
                    .files
                    .iter()
                    .map(|file| file.path.clone())
                    .collect::<Vec<_>>(),
                vec![
                    PathBuf::from("linked/inner.ts"),
                    PathBuf::from("src/app.ts")
                ]
            );
        }
    }

    #[test]
    fn walk_ignores_broken_symlinks() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let temp_dir = TestDir::new("broken-symlink");
            fs::write(temp_dir.path().join("main.ts"), "export const ok = true;\n")
                .expect("ts file");
            symlink(
                temp_dir.path().join("missing.ts"),
                temp_dir.path().join("broken.ts"),
            )
            .expect("broken symlink");

            let summary = collect_repo_files(temp_dir.path(), &TraverseConfig::default())
                .expect("walk passes");

            assert_eq!(summary.files.len(), 1);
            assert_eq!(summary.files[0].path, PathBuf::from("main.ts"));
        }
    }

    #[test]
    #[cfg(unix)]
    fn walk_rejects_symlink_repo_root() {
        use std::os::unix::fs::symlink;

        let temp_dir = TestDir::new("symlink-root");
        let external = temp_dir.path().join("external");
        let root_link = temp_dir.path().join("repo-link");
        fs::create_dir_all(&external).expect("external dir");
        fs::write(external.join("main.ts"), "export const ok = true;\n").expect("ts file");
        symlink(&external, &root_link).expect("repo root symlink");

        let error = collect_repo_files(&root_link, &TraverseConfig::default())
            .expect_err("symlink root should fail");

        assert!(matches!(error, TraverseError::SymlinkRoot { .. }));
    }

    #[test]
    #[cfg(unix)]
    fn selected_walk_skips_symlinked_files() {
        use std::os::unix::fs::symlink;

        let temp_dir = TestDir::new("selected-symlink");
        fs::create_dir_all(temp_dir.path().join("src")).expect("src directory");
        fs::write(
            temp_dir.path().join("src/real.ts"),
            "export const real = 1;\n",
        )
        .expect("real source");
        symlink(
            temp_dir.path().join("src/real.ts"),
            temp_dir.path().join("src/link.ts"),
        )
        .expect("file symlink");

        let summary = collect_selected_repo_files(
            temp_dir.path(),
            &[PathBuf::from("src/real.ts"), PathBuf::from("src/link.ts")],
            &TraverseConfig::default(),
        )
        .expect("selected walk passes");

        assert_eq!(
            summary
                .files
                .iter()
                .map(|file| file.path.clone())
                .collect::<Vec<_>>(),
            vec![PathBuf::from("src/real.ts")]
        );
    }

    #[test]
    fn selected_walk_skips_binary_paths_before_reading_contents() {
        let temp_dir = TestDir::new("selected-binary-path");
        fs::create_dir_all(temp_dir.path().join("assets")).expect("assets directory");
        fs::write(temp_dir.path().join("assets/logo.png"), b"not-really-a-png").expect("binary");

        let summary = collect_selected_repo_files(
            temp_dir.path(),
            &[PathBuf::from("assets/logo.png")],
            &TraverseConfig::default(),
        )
        .expect("selected walk passes");

        assert!(summary.files.is_empty());
        assert_eq!(summary.skipped_binary, 1);
    }

    #[test]
    fn walk_detects_supported_languages() {
        let temp_dir = TestDir::new("languages");
        fs::write(temp_dir.path().join("a.ts"), "export const ts = 1;\n").expect("ts");
        fs::write(temp_dir.path().join("b.mts"), "export const mts = 1;\n").expect("mts");
        fs::write(temp_dir.path().join("c.cts"), "export const cts = 1;\n").expect("cts");
        fs::write(temp_dir.path().join("d.js"), "export const js = 1;\n").expect("js");
        fs::write(
            temp_dir.path().join("e.search-index.json"),
            r#"{"name":"fixture"}"#,
        )
        .expect("json");
        fs::write(
            temp_dir.path().join("f.search-index.yaml"),
            "name: fixture\n",
        )
        .expect("yaml");
        fs::write(temp_dir.path().join("g.py"), "x = 1\n").expect("py");
        fs::write(temp_dir.path().join("h.pyi"), "x: int\n").expect("pyi");

        let summary =
            collect_repo_files(temp_dir.path(), &TraverseConfig::default()).expect("walk passes");

        let detected = summary
            .files
            .iter()
            .map(|file| (file.path.clone(), file.language))
            .collect::<Vec<_>>();
        assert_eq!(
            detected,
            vec![
                (PathBuf::from("a.ts"), Language::TypeScript),
                (PathBuf::from("b.mts"), Language::TypeScript),
                (PathBuf::from("c.cts"), Language::TypeScript),
                (PathBuf::from("d.js"), Language::JavaScript),
                (PathBuf::from("e.search-index.json"), Language::JavaScript),
                (PathBuf::from("f.search-index.yaml"), Language::JavaScript),
                (PathBuf::from("g.py"), Language::Python),
                (PathBuf::from("h.pyi"), Language::Python),
            ]
        );
    }

    #[test]
    fn content_hash_matches_workspace_standard_blake3() {
        // The rest of the workspace (core / storage) uses blake3 for content
        // hashing. This test pins the algorithm choice by comparing against a
        // directly-computed blake3 digest of the fixture bytes. If someone
        // reverts to sha2 this test fails unambiguously — no more "algorithm
        // drift" where parser hashes can't be compared against storage's.
        let temp_dir = TestDir::new("blake3-pin");
        let payload = b"export const value = 42;\n";
        fs::write(temp_dir.path().join("pin.ts"), payload).expect("source file");

        let summary =
            collect_repo_files(temp_dir.path(), &TraverseConfig::default()).expect("walk");
        let expected: [u8; 32] = *blake3::hash(payload).as_bytes();

        assert_eq!(summary.files.len(), 1);
        assert_eq!(summary.files[0].content_hash, expected);
    }

    #[test]
    fn hashes_are_deterministic_for_identical_content() {
        let temp_dir = TestDir::new("hash");
        fs::write(
            temp_dir.path().join("same.ts"),
            "export const value = 42;\n",
        )
        .expect("source file");

        let first =
            collect_repo_files(temp_dir.path(), &TraverseConfig::default()).expect("first walk");
        let second =
            collect_repo_files(temp_dir.path(), &TraverseConfig::default()).expect("second walk");

        assert_eq!(first.files.len(), 1);
        assert_eq!(first.files[0].content_hash, second.files[0].content_hash);
    }

    #[test]
    fn indexing_config_maps_to_traverse_config() {
        let config = IndexingConfig {
            exclude: vec!["generated".to_owned()],
            language_excludes: Vec::new(),
            include_languages: vec!["typescript".to_owned(), "python".to_owned()],
            include_dotfiles: true,
            min_file_size: None,
            max_file_size: "2MB".to_owned(),
            workspace_concurrency: None,
        };

        let traverse =
            TraverseConfig::from_indexing_config(&config).expect("config should convert");

        assert_eq!(
            traverse,
            TraverseConfig::new(
                vec!["generated".to_owned()],
                vec![Language::TypeScript, Language::Python],
                true,
                2 * 1024 * 1024,
            )
        );
    }

    #[test]
    fn walk_filters_to_included_languages_only() {
        let temp_dir = TestDir::new("include-languages");
        fs::write(temp_dir.path().join("a.ts"), "export const ts = 1;\n").expect("ts");
        fs::write(temp_dir.path().join("b.py"), "x = 1\n").expect("py");
        fs::write(temp_dir.path().join("c.rs"), "fn main() {}\n").expect("rs");

        let summary = collect_repo_files(
            temp_dir.path(),
            &TraverseConfig::new(
                Vec::new(),
                vec![Language::TypeScript, Language::Python],
                false,
                1024 * 1024,
            ),
        )
        .expect("walk passes");

        assert_eq!(
            summary
                .files
                .iter()
                .map(|file| file.path.clone())
                .collect::<Vec<_>>(),
            vec![PathBuf::from("a.ts"), PathBuf::from("b.py")]
        );
        assert_eq!(summary.skipped_excluded, 1);
    }

    #[test]
    fn index_relevant_path_filters_noise_but_keeps_config_inputs() {
        let config = TraverseConfig::default();

        assert!(config.is_index_relevant_path(Path::new("src/app.ts")));
        assert!(config.is_index_relevant_path(Path::new("package.json")));
        assert!(config.is_index_relevant_path(Path::new("tsconfig.json")));
        assert!(config.is_index_relevant_path(Path::new(".gitignore")));
        // `.gather-step.local.yaml` is excluded from the allow-list: it must
        // route through the dedicated local_config loader, not the source walker.
        assert!(!config.is_index_relevant_path(Path::new(".gather-step.local.yaml")));
        assert!(!config.is_index_relevant_path(Path::new("node_modules/pkg/index.ts")));
        assert!(!config.is_index_relevant_path(Path::new("dist/bundle.js")));
        assert!(!config.is_index_relevant_path(Path::new("README.md")));
        assert!(!config.is_index_relevant_path(Path::new("assets/logo.png")));
    }

    #[test]
    fn walk_includes_dotfiles_when_enabled() {
        let temp_dir = TestDir::new("dotfiles");
        fs::create_dir_all(temp_dir.path().join(".hidden")).expect("hidden dir");
        fs::write(
            temp_dir.path().join(".hidden/config.ts"),
            "export const hidden = true;\n",
        )
        .expect("hidden file");

        let hidden_off =
            collect_repo_files(temp_dir.path(), &TraverseConfig::default()).expect("walk passes");
        let hidden_on = collect_repo_files(
            temp_dir.path(),
            &TraverseConfig::new(Vec::new(), Vec::new(), true, 1024 * 1024),
        )
        .expect("walk passes");

        assert!(hidden_off.files.is_empty());
        assert_eq!(hidden_on.files.len(), 1);
        assert_eq!(hidden_on.files[0].path, PathBuf::from(".hidden/config.ts"));
    }

    #[test]
    fn walk_respects_max_file_size_boundary() {
        let temp_dir = TestDir::new("max-file-size");
        fs::write(temp_dir.path().join("limit.ts"), "abcd").expect("limit file");
        fs::write(temp_dir.path().join("over.ts"), "abcde").expect("oversized file");

        let summary = collect_repo_files(
            temp_dir.path(),
            &TraverseConfig::new(Vec::new(), Vec::new(), false, 4),
        )
        .expect("walk passes");

        assert_eq!(
            summary
                .files
                .iter()
                .map(|file| file.path.clone())
                .collect::<Vec<_>>(),
            vec![PathBuf::from("limit.ts")]
        );
        assert_eq!(summary.skipped_too_large, 1);
    }

    #[test]
    fn language_classifier_handles_expected_extensions() {
        assert_eq!(classify_language("src/main.ts"), Some(Language::TypeScript));
        assert_eq!(
            classify_language("src/main.tsx"),
            Some(Language::TypeScript)
        );
        assert_eq!(
            classify_language("src/main.mts"),
            Some(Language::TypeScript)
        );
        assert_eq!(
            classify_language("src/main.cts"),
            Some(Language::TypeScript)
        );
        assert_eq!(classify_language("src/main.js"), Some(Language::JavaScript));
        assert_eq!(
            classify_language("src/search-index.json"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            classify_language("src/search-index.yaml"),
            Some(Language::JavaScript)
        );
        assert_eq!(
            classify_language("src/search-index.yml"),
            Some(Language::JavaScript)
        );
        assert_eq!(classify_language("package.json"), None);
        assert_eq!(classify_language("tsconfig.json"), None);
        assert_eq!(classify_language("src/main.py"), Some(Language::Python));
        assert_eq!(classify_language("src/main.pyi"), Some(Language::Python));
        assert_eq!(classify_language("src/main.rs"), Some(Language::Rust));
        assert_eq!(classify_language("src/main.go"), Some(Language::Go));
        assert_eq!(classify_language("src/Main.java"), Some(Language::Java));
        assert_eq!(classify_language("src/main.txt"), None);
    }

    #[test]
    fn byte_size_parser_supports_expected_units() {
        assert_eq!(parse_byte_size("512"), Some(512));
        assert_eq!(parse_byte_size("1KB"), Some(1024));
        assert_eq!(parse_byte_size("2MB"), Some(2 * 1024 * 1024));
        assert_eq!(parse_byte_size("3 GB"), Some(3 * 1024 * 1024 * 1024));
        assert_eq!(parse_byte_size("bogus"), None);
    }

    #[cfg(unix)]
    #[test]
    fn walk_returns_error_for_unreadable_file() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = TestDir::new("permissions");
        let unreadable = temp_dir.path().join("secret.ts");
        fs::write(&unreadable, "export const x = 1;\n").expect("write");
        fs::set_permissions(&unreadable, fs::Permissions::from_mode(0o000))
            .expect("set permissions");

        let result = collect_repo_files(temp_dir.path(), &TraverseConfig::default());

        // Restore permissions before assertion so cleanup succeeds
        let _ = fs::set_permissions(&unreadable, fs::Permissions::from_mode(0o644));

        // The walker should return an error because the file cannot be opened
        assert!(
            result.is_err() || {
                let summary = result.as_ref().unwrap();
                // If the ignore crate skips the file without error, that's also acceptable
                summary.files.is_empty()
            }
        );
    }

    // ---------------------------------------------------------------------------
    // Exclude-pattern matching tests
    // ---------------------------------------------------------------------------

    /// Literal directory names match files nested inside that directory.
    #[test]
    fn exclude_literal_directory_name_blocks_nested_files() {
        let temp_dir = TestDir::new("exclude-literal");
        fs::create_dir_all(temp_dir.path().join("node_modules/pkg")).expect("node_modules");
        fs::write(
            temp_dir.path().join("node_modules/pkg/index.ts"),
            "export const dep = 1;\n",
        )
        .expect("dep file");
        fs::write(temp_dir.path().join("main.ts"), "export const ok = 1;\n").expect("main");

        let summary =
            collect_repo_files(temp_dir.path(), &TraverseConfig::default()).expect("walk");

        assert_eq!(summary.files.len(), 1);
        assert_eq!(summary.files[0].path, PathBuf::from("main.ts"));
        assert_eq!(summary.skipped_excluded, 1);
    }

    /// `*.lock` suffix patterns exclude matching files at any depth.
    #[test]
    fn exclude_suffix_pattern_blocks_matching_files_at_any_depth() {
        let temp_dir = TestDir::new("exclude-suffix");
        fs::create_dir_all(temp_dir.path().join("packages/app")).expect("packages dir");
        fs::write(temp_dir.path().join("yarn.lock"), b"# lockfile").expect("root lock");
        fs::write(
            temp_dir.path().join("packages/app/package-lock.json"),
            b"{}",
        )
        .expect("nested json");
        fs::write(
            temp_dir.path().join("packages/app/index.ts"),
            "export const x = 1;\n",
        )
        .expect("ts file");

        let config = TraverseConfig::new(
            vec!["*.lock".to_owned()],
            Vec::new(),
            false,
            DEFAULT_MAX_FILE_SIZE_BYTES,
        );
        let summary = collect_repo_files(temp_dir.path(), &config).expect("walk");

        let paths: Vec<_> = summary.files.iter().map(|f| f.path.clone()).collect();
        assert!(
            !paths
                .iter()
                .any(|p| p.extension().is_some_and(|e| e == "lock")),
            "no .lock files should be collected: {paths:?}"
        );
        assert!(
            paths.iter().any(|p| p.ends_with("index.ts")),
            "ts file should be collected"
        );
    }

    /// Slash-scoped patterns (e.g. `apps/*/coverage`) exclude matching subtrees.
    #[test]
    fn exclude_slash_scoped_pattern_blocks_matching_subtree() {
        let temp_dir = TestDir::new("exclude-slash");
        fs::create_dir_all(temp_dir.path().join("apps/web/coverage")).expect("coverage dir");
        fs::create_dir_all(temp_dir.path().join("apps/web/src")).expect("src dir");
        fs::write(
            temp_dir.path().join("apps/web/coverage/lcov.ts"),
            "export const cov = 1;\n",
        )
        .expect("coverage file");
        fs::write(
            temp_dir.path().join("apps/web/src/app.ts"),
            "export const app = 1;\n",
        )
        .expect("src file");

        let config = TraverseConfig::new(
            vec!["apps/*/coverage".to_owned()],
            Vec::new(),
            false,
            DEFAULT_MAX_FILE_SIZE_BYTES,
        );
        let summary = collect_repo_files(temp_dir.path(), &config).expect("walk");

        let paths: Vec<_> = summary.files.iter().map(|f| f.path.clone()).collect();
        assert!(
            !paths.iter().any(|p| p.starts_with("apps/web/coverage")),
            "coverage subtree should be excluded: {paths:?}"
        );
        assert_eq!(
            paths,
            vec![PathBuf::from("apps/web/src/app.ts")],
            "only src file should survive"
        );
    }

    /// `**/generated/**` recursive patterns exclude the entire generated subtree.
    #[test]
    fn exclude_double_star_recursive_pattern_blocks_subtree() {
        let temp_dir = TestDir::new("exclude-doublestar");
        fs::create_dir_all(temp_dir.path().join("src/generated")).expect("generated dir");
        fs::create_dir_all(temp_dir.path().join("src/manual")).expect("manual dir");
        fs::write(
            temp_dir.path().join("src/generated/types.ts"),
            "export type T = never;\n",
        )
        .expect("generated file");
        fs::write(
            temp_dir.path().join("src/manual/logic.ts"),
            "export const ok = 1;\n",
        )
        .expect("manual file");

        let config = TraverseConfig::new(
            vec!["**/generated/**".to_owned()],
            Vec::new(),
            false,
            DEFAULT_MAX_FILE_SIZE_BYTES,
        );
        let summary = collect_repo_files(temp_dir.path(), &config).expect("walk");

        let paths: Vec<_> = summary.files.iter().map(|f| f.path.clone()).collect();
        assert!(
            !paths.iter().any(|p| p.starts_with("src/generated")),
            "generated subtree should be excluded: {paths:?}"
        );
        assert_eq!(paths, vec![PathBuf::from("src/manual/logic.ts")]);
    }

    /// Brace alternation patterns (e.g. `{dist,build}`) exclude all named
    /// directories.
    #[test]
    fn exclude_brace_alternation_pattern_blocks_all_alternatives() {
        let temp_dir = TestDir::new("exclude-brace");
        fs::create_dir_all(temp_dir.path().join("dist")).expect("dist dir");
        fs::create_dir_all(temp_dir.path().join("build")).expect("build dir");
        fs::create_dir_all(temp_dir.path().join("src")).expect("src dir");
        fs::write(
            temp_dir.path().join("dist/bundle.ts"),
            "export const dist = 1;\n",
        )
        .expect("dist file");
        fs::write(
            temp_dir.path().join("build/out.ts"),
            "export const build = 1;\n",
        )
        .expect("build file");
        fs::write(
            temp_dir.path().join("src/app.ts"),
            "export const app = 1;\n",
        )
        .expect("src file");

        let config = TraverseConfig::new(
            vec!["{dist,build}".to_owned()],
            Vec::new(),
            false,
            DEFAULT_MAX_FILE_SIZE_BYTES,
        );
        let summary = collect_repo_files(temp_dir.path(), &config).expect("walk");

        let paths: Vec<_> = summary.files.iter().map(|f| f.path.clone()).collect();
        assert!(
            !paths
                .iter()
                .any(|p| p.starts_with("dist") || p.starts_with("build")),
            "dist and build should be excluded: {paths:?}"
        );
        assert_eq!(paths, vec![PathBuf::from("src/app.ts")]);
    }
}
