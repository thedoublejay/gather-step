use std::{
    borrow::Cow,
    cmp::Ordering,
    fs,
    path::{Path, PathBuf},
    sync::{
        Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering},
    },
};

use rustc_hash::FxHashSet;

use gather_step_core::{NodeData, NodeId, NodeKind, Visibility};
#[cfg(not(test))]
use tantivy::directory::MmapDirectory;
#[cfg(test)]
use tantivy::directory::RamDirectory;
use tantivy::{
    DocAddress, Index, IndexReader, IndexWriter, ReloadPolicy, Searcher, TantivyDocument, Term,
    collector::TopDocs,
    directory::error::OpenDirectoryError,
    query::{BooleanQuery, Occur, Query, QueryParser, TermQuery},
    schema::{
        BytesOptions, FAST, Field, IndexRecordOption, NumericOptions, STORED, STRING, Schema,
        TextFieldIndexing, TextOptions, document::Value as _,
    },
    tokenizer::{RemoveLongFilter, TextAnalyzer, Token, TokenStream, Tokenizer},
};
use thiserror::Error;
use tracing::warn;

const CODE_TOKENIZER_NAME: &str = "code";
const PATH_TOKENIZER_NAME: &str = "path";
/// Minimum heap the tantivy index writer will accept (50 MiB).
const MIN_WRITER_HEAP_BYTES: usize = 50 * 1024 * 1024;
/// Heap budget for one-shot CLI runs: 64 MiB.  Lower is fine because the
/// run completes quickly and the process exits immediately after.
const ONESHOT_WRITER_HEAP_BYTES: usize = 64 * 1024 * 1024;
/// Heap budget for long-running daemon processes: 200 MiB.  The writer
/// lives for the lifetime of the process, so a larger heap reduces merge
/// stalls during high-throughput watch/serve bursts.
const DAEMON_WRITER_HEAP_BYTES: usize = 200 * 1024 * 1024;
const MAX_TOKEN_BYTES: usize = 80;
const MAX_RESULT_WINDOW: usize = 10_000;

/// Current search-index schema version.
///
/// Bump this constant whenever the Tantivy schema changes (fields added or
/// removed) so that incompatible on-disk indexes are rejected at open time
/// rather than silently producing wrong results.
pub const SEARCH_INDEX_VERSION: u32 = 3;

/// File name written into the search directory to record the schema version.
const SEARCH_VERSION_FILE: &str = "gather_step_schema_version";

const FIELD_NODE_ID: &str = "node_id";
const FIELD_REPO: &str = "repo";
const FIELD_FILE_KEY: &str = "file_key";
const FIELD_SYMBOL_NAME: &str = "symbol_name";
const FIELD_CONTENT: &str = "content";
const FIELD_FILE_NAME: &str = "file_name";
const FIELD_FILE_PATH_TOKENS: &str = "file_path_tokens";
const FIELD_NODE_KIND: &str = "node_kind";
const FIELD_LAST_MODIFIED: &str = "last_modified";
const FIELD_IS_EXPORTED: &str = "is_exported";
const FIELD_LANG: &str = "lang";
/// Stored copy of the file path for query-aware rerank path-token boosting.
/// Not indexed for search — retrieval only.  Absent on documents indexed
/// before this field was added; callers treat an absent value as empty string.
const FIELD_FILE_PATH_STORED: &str = "file_path_stored";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchDocument {
    pub node_id: NodeId,
    pub repo: String,
    pub file_path: String,
    pub symbol_name: String,
    pub content: String,
    pub description: String,
    pub node_kind: NodeKind,
    pub last_modified: u64,
    pub is_exported: bool,
    pub lang: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SearchHit {
    pub node_id: NodeId,
    pub repo: String,
    pub file_path: String,
    pub symbol_name: String,
    pub node_kind: NodeKind,
    pub adjusted_score: f32,
    pub exact_match: bool,
    pub is_exported: bool,
    pub lang: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SearchFilters<'a> {
    pub repo: Option<&'a str>,
    pub node_kind: Option<NodeKind>,
    pub lang: Option<&'a str>,
}

/// Describes how the search store will be used, so the writer heap can be
/// tuned appropriately.
///
/// Pass [`SearchWorkload::OneShot`] for CLI `index` runs that complete and
/// exit quickly.  Pass [`SearchWorkload::LongRunning`] for `watch` and
/// `serve` daemons that keep the writer alive for extended periods.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SearchWorkload {
    /// One-shot batch run — 64 MiB writer heap (sufficient for full workspace
    /// indexing; the lower limit reduces peak RSS in the common case).
    #[default]
    OneShot,
    /// Long-running daemon — 200 MiB writer heap (reduces merge stalls during
    /// high-frequency `watch` / `serve` event bursts).
    LongRunning,
}

impl SearchWorkload {
    /// Translate the workload variant to the heap byte budget for the tantivy
    /// index writer.  The returned value is always at least
    /// [`MIN_WRITER_HEAP_BYTES`].
    #[must_use]
    pub const fn writer_heap_bytes(self) -> usize {
        let raw = match self {
            Self::OneShot => ONESHOT_WRITER_HEAP_BYTES,
            Self::LongRunning => DAEMON_WRITER_HEAP_BYTES,
        };
        if raw < MIN_WRITER_HEAP_BYTES {
            MIN_WRITER_HEAP_BYTES
        } else {
            raw
        }
    }
}

pub trait SearchStore {
    fn index_symbols(&self, documents: &[SearchDocument]) -> Result<(), SearchStoreError>;
    fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>, SearchStoreError>;
    fn search_filtered(
        &self,
        query: &str,
        limit: usize,
        filters: SearchFilters<'_>,
    ) -> Result<Vec<SearchHit>, SearchStoreError>;
    fn delete_by_files(&self, files: &[(&str, &str)]) -> Result<(), SearchStoreError>;
    fn delete_by_repo(&self, repo: &str) -> Result<(), SearchStoreError>;
    fn replace_by_files(
        &self,
        files: &[(&str, &str)],
        documents: &[SearchDocument],
    ) -> Result<(), SearchStoreError>;
}

pub struct TantivySearchStore {
    path: PathBuf,
    index: Index,
    reader: Mutex<IndexReader>,
    writer: Option<Mutex<IndexWriter>>,
    fields: SearchFields,
    /// When true, `commit()` becomes a no-op. Documents are staged but not
    /// flushed until `flush()` is called. Used during workspace indexing to
    /// batch all repos into a single Tantivy commit.
    deferred_commit: AtomicBool,
    /// Monotonically increasing counter bumped on every successful
    /// [`flush`] call.  Starts at 1 so that the sentinel value of
    /// `last_seen_commit` (0) is always less than the initial value, causing
    /// the first search to perform exactly one reader reload.
    writer_commit_counter: AtomicU64,
    /// The commit counter value observed during the most recent
    /// [`refresh_reader`] call in [`search_filtered`].  When this equals
    /// [`writer_commit_counter`] no new commits have occurred and the reload
    /// can be skipped.
    last_seen_commit: AtomicU64,
    /// The workload mode used to open this store.  [`None`] means read-only.
    workload: Option<SearchWorkload>,
}

pub struct DeferredCommitGuard<'store> {
    store: &'store TantivySearchStore,
    flushed: bool,
}

impl DeferredCommitGuard<'_> {
    pub fn mark_flushed(mut self) {
        self.flushed = true;
        self.store.set_deferred_commit(false);
    }
}

impl Drop for DeferredCommitGuard<'_> {
    fn drop(&mut self) {
        if !self.flushed
            && let Err(error) = self.store.rollback()
        {
            warn!(
                error = %error,
                "failed to roll back deferred search index writes",
            );
        }
        self.store.set_deferred_commit(false);
    }
}

#[derive(Clone, Copy)]
struct SearchFields {
    node_id: Field,
    repo: Field,
    file_key: Field,
    symbol_name: Field,
    content: Field,
    file_name: Field,
    file_path_tokens: Field,
    node_kind: Field,
    last_modified: Field,
    is_exported: Field,
    lang: Field,
    file_path_stored: Field,
}

#[derive(Clone)]
struct ScoredSearchHit {
    hit: SearchHit,
    base_score: f32,
    last_modified: u64,
    /// Stored file path, populated from `FIELD_FILE_PATH_STORED` for
    /// query-aware path-token rerank.  Empty for documents indexed before the
    /// field was added (the boost is a no-op in that case).
    file_path_stored: String,
}

#[derive(Debug, Error)]
pub enum SearchStoreError {
    #[error("failed to access search index: {0}")]
    Tantivy(#[from] tantivy::TantivyError),
    #[error("failed to open search index directory: {0}")]
    OpenDirectory(#[from] OpenDirectoryError),
    #[error("failed to create search index directory: {0}")]
    Io(#[from] std::io::Error),
    #[error("search writer mutex was poisoned")]
    WriterPoisoned,
    #[error("search reader mutex was poisoned")]
    ReaderPoisoned,
    #[error("search store is read-only")]
    ReadOnly,
    #[error("search document missing required field `{0}`")]
    MissingField(&'static str),
    #[error("invalid node id length `{0}`")]
    InvalidNodeId(usize),
    #[error("invalid node kind tag `{0}`")]
    InvalidNodeKind(u64),
    #[error(
        "your local index uses an unsupported schema; run `gather-step clean && gather-step index` to rebuild"
    )]
    VersionMismatch { stored: u32, expected: u32 },
}

impl TantivySearchStore {
    /// Open the search store for read-write access.
    ///
    /// The default [`SearchWorkload::OneShot`] heap (64 MiB) is used.  Call
    /// [`open_with_workload`] when you need the larger daemon heap.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SearchStoreError> {
        Self::open_with_workload(path, SearchWorkload::OneShot)
    }

    /// Open the search store for read-write access with the given writer heap
    /// budget.
    pub fn open_with_workload(
        path: impl AsRef<Path>,
        workload: SearchWorkload,
    ) -> Result<Self, SearchStoreError> {
        Self::open_with_mode(path, Some(workload))
    }

    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self, SearchStoreError> {
        Self::open_with_mode(path, None)
    }

    fn open_with_mode(
        path: impl AsRef<Path>,
        workload: Option<SearchWorkload>,
    ) -> Result<Self, SearchStoreError> {
        let path = path.as_ref().to_path_buf();
        fs::create_dir_all(&path)?;
        // Caller must have validated path via cli::path_safety before opening.
        crate::fs_mode::apply_private_dir(&path)?;

        // Check (non-test) or skip (test/RamDirectory) schema version guard.
        // RamDirectory indexes are always fresh so the version file is not
        // written or read in test builds.
        #[cfg(not(test))]
        check_or_write_schema_version(&path)?;

        let schema = build_schema();
        #[cfg(test)]
        let mut index = Index::open_or_create(RamDirectory::create(), schema)?;
        #[cfg(not(test))]
        let mut index = {
            let directory = MmapDirectory::open(&path)?;
            Index::open_or_create(directory, schema)?
        };
        register_tokenizers(&mut index);

        let fields = SearchFields::from_schema(&index.schema());
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let writer = workload
            .map(|wl| index.writer(wl.writer_heap_bytes()))
            .transpose()?
            .map(Mutex::new);
        // Note: we deliberately keep Tantivy's default `LogMergePolicy`.
        // Earlier we experimented with `NoMergePolicy` to eliminate the
        // `segment_manager "couldn't find segment"` warnings, but the real
        // root cause was per-batch commits: the deferred-commit path (see
        // `set_deferred_commit` + `commit()`) collapses ~250 commits into 1,
        // which by itself produces at most 1-2 segments and no racing merges.
        // Keeping the default policy means incremental `watch` runs don't
        // accumulate unmerged segments indefinitely.

        Ok(Self {
            path,
            index,
            reader: Mutex::new(reader),
            writer,
            fields,
            deferred_commit: AtomicBool::new(false),
            // Counter starts at 1; `last_seen_commit` starts at 0.  This
            // sentinel guarantees the very first search performs exactly one
            // reader reload to pick up the on-disk initial index state.
            writer_commit_counter: AtomicU64::new(1),
            last_seen_commit: AtomicU64::new(0),
            workload,
        })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn writer(&self) -> Result<MutexGuard<'_, IndexWriter>, SearchStoreError> {
        self.writer
            .as_ref()
            .ok_or(SearchStoreError::ReadOnly)?
            .lock()
            .map_err(|_| SearchStoreError::WriterPoisoned)
    }

    fn ensure_writer_health_for_reads(&self) -> Result<(), SearchStoreError> {
        if let Some(writer) = &self.writer {
            drop(
                writer
                    .lock()
                    .map_err(|_| SearchStoreError::WriterPoisoned)?,
            );
        }
        Ok(())
    }

    pub fn refresh_reader(&self) -> Result<(), SearchStoreError> {
        let reader = self
            .reader
            .lock()
            .map_err(|_| SearchStoreError::ReaderPoisoned)?;
        reader.reload()?;
        Ok(())
    }

    /// Conditionally reload the reader only when the index has been written
    /// since the last search.
    ///
    /// Rules:
    /// - For [`SearchWorkload::OneShot`] (CLI index runs): always skip.
    ///   Writes complete before searches begin; [`flush`] already reloads.
    /// - For long-running daemons and read-only stores: compare
    ///   [`writer_commit_counter`] against [`last_seen_commit`].  Reload
    ///   only when the counter has advanced (i.e. a new commit occurred since
    ///   the last search).  Update [`last_seen_commit`] after reloading.
    fn refresh_reader_if_needed(&self) -> Result<(), SearchStoreError> {
        match self.workload {
            // One-shot CLI runs complete all writes before any search.
            // `flush()` already reloads the reader and advances both counters,
            // so there is nothing to do here.
            Some(SearchWorkload::OneShot) => Ok(()),

            // Long-running daemons own their writer.  Compare the commit
            // counter against the last observed generation and reload only
            // when a new commit has been made since the last search.
            Some(SearchWorkload::LongRunning) => {
                let current = self.writer_commit_counter.load(AtomicOrdering::Acquire);
                let seen = self.last_seen_commit.load(AtomicOrdering::Acquire);
                if seen >= current {
                    // No new commits since the last reload — skip.
                    return Ok(());
                }
                self.refresh_reader()?;
                // Record the generation we just caught up to.
                self.last_seen_commit
                    .store(current, AtomicOrdering::Release);
                Ok(())
            }

            // Read-only stores (e.g. the MCP context) have no writer of their
            // own and cannot track when an external process commits.  Always
            // reload so that external writes made by a concurrent indexer run
            // are immediately visible.
            None => self.refresh_reader(),
        }
    }

    pub fn stage_index_symbol(&self, document: &SearchDocument) -> Result<(), SearchStoreError> {
        let node_id = document.node_id.as_bytes();
        let writer = self.writer()?;
        writer.delete_term(Term::from_field_bytes(
            self.fields.node_id,
            node_id.as_slice(),
        ));
        writer.add_document(self.to_document(document))?;
        Ok(())
    }

    pub fn index_symbol(&self, document: &SearchDocument) -> Result<(), SearchStoreError> {
        self.index_symbols(std::slice::from_ref(document))
    }

    pub fn stage_delete_by_file(
        &self,
        repo: &str,
        file_path: &str,
    ) -> Result<(), SearchStoreError> {
        let writer = self.writer()?;
        writer.delete_term(Term::from_field_text(
            self.fields.file_key,
            &file_key(repo, file_path),
        ));
        Ok(())
    }

    pub fn delete_by_file(&self, repo: &str, file_path: &str) -> Result<(), SearchStoreError> {
        self.delete_by_files(&[(repo, file_path)])
    }

    pub fn stage_delete_by_repo(&self, repo: &str) -> Result<(), SearchStoreError> {
        let writer = self.writer()?;
        writer.delete_term(Term::from_field_text(self.fields.repo, repo));
        Ok(())
    }

    /// Enable or disable deferred commit mode. When enabled, `commit()` is a
    /// no-op — documents are staged but not flushed. Call `flush()` to perform
    /// the actual commit after all repos are done.
    ///
    /// Uses `Release`/`Acquire` ordering so the toggle is visible to rayon
    /// workers reading the flag inside `commit()` on other cores.
    pub fn set_deferred_commit(&self, enabled: bool) {
        self.deferred_commit.store(enabled, AtomicOrdering::Release);
    }

    #[must_use]
    pub fn begin_deferred_commit(&self) -> DeferredCommitGuard<'_> {
        self.set_deferred_commit(true);
        DeferredCommitGuard {
            store: self,
            flushed: false,
        }
    }

    pub fn commit(&self) -> Result<(), SearchStoreError> {
        if self.deferred_commit.load(AtomicOrdering::Acquire) {
            return Ok(());
        }
        self.flush()
    }

    /// Force a commit + reader refresh regardless of deferred mode.
    ///
    /// Called either once per batch (non-deferred mode) or once at the end
    /// of a workspace run (deferred mode). In both cases we must reload the
    /// reader after the commit, otherwise subsequent `search()` calls would
    /// observe a stale segment set.
    ///
    /// Bumps [`writer_commit_counter`] so that [`search_filtered`] knows a
    /// new generation of data is available.
    pub fn flush(&self) -> Result<(), SearchStoreError> {
        {
            let mut writer = self.writer()?;
            writer.commit()?;
        }
        self.refresh_reader()?;
        // Bump the commit counter AFTER the reader has been reloaded.  Any
        // subsequent search will observe the new counter and skip the reload
        // because the reader is already up to date.
        self.writer_commit_counter
            .fetch_add(1, AtomicOrdering::Release);
        // Also advance last_seen_commit so the very next search does not
        // re-reload the reader unnecessarily (flush already did it).
        let current = self.writer_commit_counter.load(AtomicOrdering::Acquire);
        self.last_seen_commit
            .store(current, AtomicOrdering::Release);
        Ok(())
    }

    pub fn rollback(&self) -> Result<(), SearchStoreError> {
        let mut writer = self.writer()?;
        writer.rollback()?;
        Ok(())
    }

    fn to_document(&self, document: &SearchDocument) -> TantivyDocument {
        let mut tantivy_doc = TantivyDocument::default();
        let node_id = document.node_id.as_bytes();

        tantivy_doc.add_bytes(self.fields.node_id, node_id.as_slice());
        tantivy_doc.add_text(self.fields.repo, &document.repo);
        tantivy_doc.add_text(
            self.fields.file_key,
            file_key(&document.repo, &document.file_path),
        );
        tantivy_doc.add_text(self.fields.symbol_name, &document.symbol_name);
        if !document.content.is_empty() {
            tantivy_doc.add_text(self.fields.content, &document.content);
        }
        tantivy_doc.add_text(self.fields.file_name, file_name(&document.file_path));
        tantivy_doc.add_text(self.fields.file_path_tokens, &document.file_path);
        // Stored (not indexed) copy of the path for query-aware rerank.
        tantivy_doc.add_text(self.fields.file_path_stored, &document.file_path);
        tantivy_doc.add_u64(self.fields.node_kind, u64::from(document.node_kind.as_u8()));
        tantivy_doc.add_u64(self.fields.last_modified, document.last_modified);
        tantivy_doc.add_bool(self.fields.is_exported, document.is_exported);
        tantivy_doc.add_text(self.fields.lang, &document.lang);

        tantivy_doc
    }

    fn build_query_parser(&self, fuzzy: bool) -> QueryParser {
        let mut parser = QueryParser::for_index(
            &self.index,
            vec![
                self.fields.symbol_name,
                self.fields.content,
                self.fields.file_name,
                self.fields.file_path_tokens,
            ],
        );
        parser.set_conjunction_by_default();
        parser.set_field_boost(self.fields.symbol_name, 3.0);

        if fuzzy {
            for field in [
                self.fields.symbol_name,
                self.fields.content,
                self.fields.file_name,
                self.fields.file_path_tokens,
            ] {
                parser.set_field_fuzzy(field, false, 1, true);
            }
        }

        parser
    }

    fn execute_search(
        &self,
        query_text: &str,
        limit: usize,
        exact_match: bool,
        fuzzy: bool,
        filters: SearchFilters<'_>,
    ) -> Result<Vec<SearchHit>, SearchStoreError> {
        let reader = self
            .reader
            .lock()
            .map_err(|_| SearchStoreError::ReaderPoisoned)?;
        let searcher = reader.searcher();
        let parser = self.build_query_parser(fuzzy);
        let parser_text = parser_query_text(query_text);
        let (query, _) = parser.parse_query_lenient(parser_text.as_ref());
        let query = self.apply_filters(query, filters);
        let fetch_limit = limit.max(1).saturating_mul(5).min(MAX_RESULT_WINDOW);
        let collector = TopDocs::with_limit(fetch_limit).order_by_score();
        let docs: Vec<(f32, DocAddress)> = searcher.search(&query, &collector)?;

        let mut hits = docs
            .into_iter()
            .map(|(score, address)| self.decode_hit(&searcher, address, score, exact_match))
            .collect::<Result<Vec<_>, _>>()?;
        rerank_hits(&mut hits, query_text);

        Ok(hits
            .into_iter()
            .take(limit.max(1))
            .map(|scored| scored.hit)
            .collect())
    }

    fn apply_filters(&self, query: Box<dyn Query>, filters: SearchFilters<'_>) -> Box<dyn Query> {
        let mut clauses: Vec<(Occur, Box<dyn Query>)> = vec![(Occur::Must, query)];
        if let Some(repo) = filters.repo {
            clauses.push((
                Occur::Must,
                Box::new(TermQuery::new(
                    Term::from_field_text(self.fields.repo, repo),
                    IndexRecordOption::Basic,
                )),
            ));
        }
        if let Some(lang) = filters.lang {
            clauses.push((
                Occur::Must,
                Box::new(TermQuery::new(
                    Term::from_field_text(self.fields.lang, lang),
                    IndexRecordOption::Basic,
                )),
            ));
        }
        if let Some(node_kind) = filters.node_kind {
            clauses.push((
                Occur::Must,
                Box::new(TermQuery::new(
                    Term::from_field_u64(self.fields.node_kind, u64::from(node_kind.as_u8())),
                    IndexRecordOption::Basic,
                )),
            ));
        }
        if clauses.len() == 1 {
            clauses.pop().expect("query clause exists").1
        } else {
            Box::new(BooleanQuery::from(clauses))
        }
    }

    fn decode_hit(
        &self,
        searcher: &Searcher,
        address: DocAddress,
        score: f32,
        exact_match: bool,
    ) -> Result<ScoredSearchHit, SearchStoreError> {
        let document: TantivyDocument = searcher.doc(address)?;
        let segment_reader = searcher.segment_reader(address.segment_ord);
        let node_id = first_node_id(&document, self.fields.node_id)?;
        // `repo` and `file_path` are not stored in Tantivy (S6): callers must
        // rehydrate from the graph store using `node_id`.
        let symbol_name = first_text(&document, self.fields.symbol_name, FIELD_SYMBOL_NAME)?;
        let node_kind = fast_node_kind(segment_reader, address.doc_id)?;
        let last_modified = fast_u64(segment_reader, FIELD_LAST_MODIFIED, address.doc_id)?;
        let is_exported = fast_bool(segment_reader, FIELD_IS_EXPORTED, address.doc_id)?;
        let lang = fast_string(segment_reader, FIELD_LANG, address.doc_id)?;
        // `file_path_stored` may be absent for documents written before the
        // field was added.  Treat absence as empty string; the path-token boost
        // is a no-op on empty paths.
        let file_path_stored = document
            .get_first(self.fields.file_path_stored)
            .and_then(|v| v.as_str())
            .map(ToOwned::to_owned)
            .unwrap_or_default();

        Ok(ScoredSearchHit {
            hit: SearchHit {
                node_id,
                // Populated as empty placeholders — rehydrate via graph store.
                repo: String::new(),
                file_path: String::new(),
                symbol_name,
                node_kind,
                adjusted_score: score,
                exact_match,
                is_exported,
                lang,
            },
            base_score: score.max(0.0),
            last_modified,
            file_path_stored,
        })
    }
}

impl SearchStore for TantivySearchStore {
    fn index_symbols(&self, documents: &[SearchDocument]) -> Result<(), SearchStoreError> {
        for document in documents {
            self.stage_index_symbol(document)?;
        }
        self.commit()
    }

    fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>, SearchStoreError> {
        self.search_filtered(query, limit, SearchFilters::default())
    }

    fn search_filtered(
        &self,
        query: &str,
        limit: usize,
        filters: SearchFilters<'_>,
    ) -> Result<Vec<SearchHit>, SearchStoreError> {
        let trimmed = query.trim();
        if trimmed.is_empty() {
            return Ok(Vec::new());
        }
        self.ensure_writer_health_for_reads()?;
        self.refresh_reader_if_needed()?;

        let exact_hits = self.execute_search(trimmed, limit, true, false, filters)?;
        if !exact_hits.is_empty() {
            return Ok(exact_hits);
        }

        self.execute_search(trimmed, limit, false, true, filters)
    }

    fn delete_by_files(&self, files: &[(&str, &str)]) -> Result<(), SearchStoreError> {
        for (repo, file_path) in files {
            self.stage_delete_by_file(repo, file_path)?;
        }
        self.commit()
    }

    fn delete_by_repo(&self, repo: &str) -> Result<(), SearchStoreError> {
        self.stage_delete_by_repo(repo)?;
        self.commit()
    }

    fn replace_by_files(
        &self,
        files: &[(&str, &str)],
        documents: &[SearchDocument],
    ) -> Result<(), SearchStoreError> {
        for (repo, file_path) in files {
            self.stage_delete_by_file(repo, file_path)?;
        }
        for document in documents {
            self.stage_index_symbol(document)?;
        }
        self.commit()
    }
}

impl SearchDocument {
    #[must_use]
    pub fn from_node(node: &NodeData, last_modified: u64) -> Self {
        Self {
            node_id: node.id,
            repo: node.repo.clone(),
            file_path: node.file_path.clone(),
            symbol_name: node.name.clone(),
            content: node.signature.clone().unwrap_or_default(),
            description: node.qualified_name.clone().unwrap_or_default(),
            node_kind: node.kind,
            last_modified,
            is_exported: matches!(node.visibility, Some(Visibility::Public)),
            lang: detect_lang(&node.file_path),
        }
    }
}

/// Read the schema version recorded in `SEARCH_VERSION_FILE` inside `dir`.
///
/// - If the file is absent this is a fresh index: write the current version
///   and proceed.
/// - If the file exists and its version matches [`SEARCH_INDEX_VERSION`],
///   proceed normally.
/// - If the file exists but the version differs, return
///   [`SearchStoreError::VersionMismatch`] so the caller can prompt the user
///   to rebuild generated index state.
///
/// The version file is a plain decimal `u32` followed by a newline.
///
/// Exposed as `pub(crate)` so the unit-test suite can exercise it directly
/// against a real filesystem directory without needing a non-test binary.
pub(crate) fn check_or_write_schema_version(dir: &std::path::Path) -> Result<(), SearchStoreError> {
    let version_path = dir.join(SEARCH_VERSION_FILE);
    match fs::read_to_string(&version_path) {
        Ok(contents) => {
            let stored: u32 = contents.trim().parse().unwrap_or(0);
            if stored != SEARCH_INDEX_VERSION {
                return Err(SearchStoreError::VersionMismatch {
                    stored,
                    expected: SEARCH_INDEX_VERSION,
                });
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // Stamp the version file only when the directory is genuinely
            // empty (or contains nothing but our sentinel file).  A non-empty
            // directory without a version file is incompatible generated
            // state. Treating it as "fresh" would silently stamp the sentinel
            // and the next schema-dependent lookup (`SearchFields::from_schema`)
            // could panic on a missing field. Reject with `VersionMismatch` so
            // the operator can run `gather-step clean --storage` and reindex.
            if directory_has_index_artifacts(dir)? {
                return Err(SearchStoreError::VersionMismatch {
                    stored: 0,
                    expected: SEARCH_INDEX_VERSION,
                });
            }
            fs::write(&version_path, format!("{SEARCH_INDEX_VERSION}\n"))?;
            Ok(())
        }
        Err(error) => Err(SearchStoreError::Io(error)),
    }
}

/// Returns `true` when `dir` contains recognizable Tantivy index artifacts.
///
/// Stray platform/editor files in an otherwise fresh directory (for example
/// `.DS_Store`) should not force a legacy-index failure; only real index files
/// without the schema sentinel mean we must reject and ask for a clean reindex.
fn directory_has_index_artifacts(dir: &std::path::Path) -> Result<bool, SearchStoreError> {
    let entries = match fs::read_dir(dir) {
        Ok(it) => it,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(SearchStoreError::Io(error)),
    };
    for entry in entries {
        let entry = entry.map_err(SearchStoreError::Io)?;
        let file_name = entry.file_name();
        if file_name == SEARCH_VERSION_FILE {
            continue;
        }
        if is_search_index_artifact_name(&file_name) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn is_search_index_artifact_name(file_name: &std::ffi::OsStr) -> bool {
    let Some(name) = file_name.to_str() else {
        return false;
    };
    if matches!(name, "meta.json" | ".managed.json" | "managed.json") {
        return true;
    }
    let Some((_, extension)) = name.rsplit_once('.') else {
        return false;
    };
    matches!(
        extension,
        "idx" | "term" | "pos" | "fast" | "fieldnorm" | "store" | "del"
    )
}

fn build_schema() -> Schema {
    let mut builder = Schema::builder();

    builder.add_bytes_field(
        FIELD_NODE_ID,
        BytesOptions::default().set_stored().set_indexed(),
    );
    // `repo` is indexed for filter queries but NOT stored: stored values are
    // rehydrated from the graph store via `node_id`.
    builder.add_text_field(FIELD_REPO, STRING);
    // `file_key` is a write-time denormalization of `(repo, file_path)` used to delete all
    // documents for one file with a single exact-term delete.  Not stored.
    builder.add_text_field(FIELD_FILE_KEY, STRING);
    builder.add_text_field(FIELD_SYMBOL_NAME, code_text_options(true, false));
    builder.add_text_field(FIELD_CONTENT, code_text_options(false, true));
    builder.add_text_field(FIELD_FILE_NAME, code_text_options(false, false));
    builder.add_text_field(FIELD_FILE_PATH_TOKENS, path_text_options(false, false));
    builder.add_u64_field(FIELD_NODE_KIND, fast_numeric_options(false));
    builder.add_u64_field(FIELD_LAST_MODIFIED, fast_numeric_options(false));
    builder.add_bool_field(FIELD_IS_EXPORTED, FAST);
    builder.add_text_field(FIELD_LANG, STRING | FAST);
    // Stored (not indexed) copy of the file path for query-aware rerank.
    // Uses `STORED` only — no tokenizer, no index.
    builder.add_text_field(FIELD_FILE_PATH_STORED, STORED);

    builder.build()
}

fn code_text_options(stored: bool, with_positions: bool) -> TextOptions {
    let indexing = TextFieldIndexing::default()
        .set_tokenizer(CODE_TOKENIZER_NAME)
        .set_index_option(if with_positions {
            IndexRecordOption::WithFreqsAndPositions
        } else {
            IndexRecordOption::WithFreqs
        });
    let mut options = TextOptions::default().set_indexing_options(indexing);
    if stored {
        options = options.set_stored();
    }
    options
}

fn path_text_options(stored: bool, with_positions: bool) -> TextOptions {
    let indexing = TextFieldIndexing::default()
        .set_tokenizer(PATH_TOKENIZER_NAME)
        .set_index_option(if with_positions {
            IndexRecordOption::WithFreqsAndPositions
        } else {
            IndexRecordOption::WithFreqs
        });
    let mut options = TextOptions::default().set_indexing_options(indexing);
    if stored {
        options = options.set_stored();
    }
    options
}

fn fast_numeric_options(stored: bool) -> NumericOptions {
    let mut options = NumericOptions::default().set_fast().set_indexed();
    if stored {
        options = options.set_stored();
    }
    options
}

fn register_tokenizers(index: &mut Index) {
    index.tokenizers().register(
        CODE_TOKENIZER_NAME,
        TextAnalyzer::builder(CodeTokenizer)
            .filter(RemoveLongFilter::limit(MAX_TOKEN_BYTES))
            .build(),
    );
    index.tokenizers().register(
        PATH_TOKENIZER_NAME,
        TextAnalyzer::builder(PathTokenizer)
            .filter(RemoveLongFilter::limit(MAX_TOKEN_BYTES))
            .build(),
    );
}

#[expect(
    clippy::disallowed_methods,
    reason = "query expansion needs a short owned string only when a single CamelCase identifier splits"
)]
fn parser_query_text(query: &str) -> Cow<'_, str> {
    let trimmed = query.trim();
    if trimmed.is_empty() || !trimmed.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return Cow::Borrowed(query);
    }

    let spans = split_identifier_spans(trimmed);
    if spans.len() <= 1 {
        return Cow::Borrowed(query);
    }

    Cow::Owned(
        spans
            .into_iter()
            .map(|span| trimmed[span.start..span.end].to_ascii_lowercase())
            .collect::<Vec<_>>()
            .join(" "),
    )
}

/// Return the first code-token (split at CamelCase / underscore boundaries)
/// of `symbol_name`, lowercased, for prefix-match boosting.
fn first_code_token(symbol_name: &str) -> &str {
    let bytes = symbol_name.as_bytes();
    let mut end = bytes.len();
    for (i, &b) in bytes.iter().enumerate().skip(1) {
        // CamelCase boundary: previous byte was lowercase ASCII, current is uppercase
        if bytes[i - 1].is_ascii_lowercase() && b.is_ascii_uppercase() {
            end = i;
            break;
        }
        // Separator boundary
        if b == b'_' || b == b'-' {
            end = i;
            break;
        }
    }
    &symbol_name[..end]
}

/// Tokenize a camelCase or `PascalCase` identifier into lowercase word tokens.
///
/// `StreamableSession` → `["streamable", "session"]`
/// `useAuthSession`    → `["use", "auth", "session"]`
///
/// Used only for query-aware rerank path boosting, not for Tantivy indexing.
#[expect(
    clippy::disallowed_methods,
    reason = "one-shot owned lowercase needed to build token set for path-match comparison"
)]
fn tokenize_camel_case(s: &str) -> Vec<String> {
    // Reuse the existing span splitter, then lowercase each span.
    split_identifier_spans(s)
        .into_iter()
        .map(|span| s[span.start..span.end].to_ascii_lowercase())
        .filter(|t| !t.is_empty())
        .collect()
}

/// File-path token match boost.
///
/// Splits the query into camelCase tokens and the file path into path segments
/// (split on `/`, `-`, `_`, `.`).  For each query token that appears in the
/// path token set, the multiplier grows by 1.08×, capped at 1.3× total.
///
/// Example: query `StreamableSession` → tokens `["streamable", "session"]`.
/// A file at `src/session/streamable.ts` contains both → 1.08 × 1.08 = 1.1664,
/// capped at 1.3.  A file at `src/mcp/transport.ts` contains neither → 1.0.
#[expect(
    clippy::disallowed_methods,
    reason = "one-shot owned lowercase needed to build path token set for membership checks"
)]
fn path_token_match_boost(query_tokens: &[String], file_path: &str) -> f32 {
    if query_tokens.is_empty() || file_path.is_empty() {
        return 1.0;
    }
    // Collect path tokens (split on separators, lowercase).
    let path_tokens: FxHashSet<String> = file_path
        .split(['/', '-', '_', '.'])
        .filter(|s| !s.is_empty())
        .map(str::to_ascii_lowercase)
        .collect();

    let mut multiplier = 1.0_f32;
    for token in query_tokens {
        if path_tokens.contains(token.as_str()) {
            multiplier *= 1.08;
            if multiplier >= 1.3 {
                return 1.3;
            }
        }
    }
    multiplier
}

/// Hook-name preference boost.
///
/// When the query looks like a React/TS hook (`use` followed by an uppercase
/// letter, e.g. `useAuthSession`), a hit whose `symbol_name` also starts with
/// `use` followed by an uppercase letter AND whose `node_kind` is `Function`
/// receives a 1.3× boost.  All other hits receive 1.0×.
fn hook_name_boost(query: &str, symbol_name: &str, node_kind: NodeKind) -> f32 {
    let is_hook_query =
        query.starts_with("use") && query.chars().nth(3).is_some_and(|c| c.is_ascii_uppercase());
    if !is_hook_query {
        return 1.0;
    }
    let symbol_is_hook = symbol_name.starts_with("use")
        && symbol_name
            .chars()
            .nth(3)
            .is_some_and(|c| c.is_ascii_uppercase());
    if symbol_is_hook && matches!(node_kind, NodeKind::Function) {
        1.3
    } else {
        1.0
    }
}

/// Infrastructure-named repo penalty.
///
/// When a `PascalCase` query has an exact symbol-name match (`symbol_exact_boost
/// == 1.6×`), an exported symbol, and the hit's repo name contains a common
/// infrastructure qualifier (`mcp`, `transport`, `sdk`, `driver`), apply a
/// 0.85× penalty.  This prevents generic infrastructure stubs with the same
/// name from ranking above a non-infrastructure repo's declaration.
///
/// The heuristic is intentionally narrow — it only fires when ALL three
/// conditions are true simultaneously — and the coefficient is small enough
/// that a single extra structural signal (different BM25, recency) would
/// override it.
fn infra_repo_penalty(symbol_exact_boost: f32, is_exported: bool, repo: &str) -> f32 {
    if (symbol_exact_boost - 1.6).abs() > f32::EPSILON || !is_exported {
        return 1.0;
    }
    // Repo name tokens compared case-insensitively, split on `-` and `_`.
    let infra_words = ["mcp", "transport", "sdk", "driver"];
    let is_infra = repo
        .split(['-', '_'])
        .any(|part| infra_words.iter().any(|w| part.eq_ignore_ascii_case(w)));
    if is_infra { 0.85 } else { 1.0 }
}

fn rerank_hits(hits: &mut [ScoredSearchHit], query: &str) {
    let newest_timestamp = hits
        .iter()
        .map(|scored| scored.last_modified)
        .max()
        .unwrap_or(0);
    let highest_score = hits
        .iter()
        .map(|scored| scored.base_score)
        .fold(0.0_f32, f32::max);

    // Determine per-query boosts once outside the loop.
    let query_is_pascal = query.chars().next().is_some_and(|c| c.is_ascii_uppercase());
    // Tokenize the query once for the file-path token match boost.
    let query_tokens = tokenize_camel_case(query);

    for scored in hits.iter_mut() {
        let export_boost = if scored.hit.is_exported { 1.10 } else { 1.0 };
        let exact_boost = if scored.hit.exact_match { 1.0 } else { 0.98 };
        let recency_bonus = if newest_timestamp > 0 {
            let scaled = scored.last_modified.saturating_mul(15) / newest_timestamp;
            f32::from(u16::try_from(scaled).unwrap_or(15)) / 100.0
        } else {
            0.0
        };
        let score_ratio = if highest_score > 0.0 {
            scored.base_score / highest_score
        } else {
            0.0
        };
        let recency_boost = 1.0 + recency_bonus;
        let query_boost = 1.0 + (score_ratio * 0.05);

        // Exact-symbol boost: 1.6× when the full symbol name matches the query
        // case-insensitively, 1.3× when only the first CamelCase token matches.
        // Raised from 1.4× to break ties between multiple same-name hits where
        // secondary factors (BM25, recency) were pulling unrelated-repo symbols
        // above the user's likely target.
        let symbol_name_matches = scored.hit.symbol_name.eq_ignore_ascii_case(query);
        let symbol_token_matches = !symbol_name_matches
            && first_code_token(&scored.hit.symbol_name).eq_ignore_ascii_case(query);
        let symbol_exact_boost = if symbol_name_matches {
            1.6_f32
        } else if symbol_token_matches {
            1.3_f32
        } else {
            1.0_f32
        };

        // PascalCase-type boost: 1.2× when the query starts with an uppercase
        // letter and the hit is a concrete type-like declaration (`Type` also
        // covers TypeScript interfaces per the parser mapping).
        //
        // `SharedSymbol` is deliberately NOT boosted — it's the virtual shape
        // the graph emits for cross-repo imports/re-exports, not a
        // declaration.  Including it here lets an imported/shared-symbol hit
        // out-rank the repo that actually declares the type.
        let pascal_type_boost = if query_is_pascal
            && matches!(scored.hit.node_kind, NodeKind::Type | NodeKind::Class)
        {
            1.2_f32
        } else {
            1.0_f32
        };

        // file-path token match boost — rewards hits whose path contains
        // tokens from the query.  E.g. query `StreamableSession` boosts files
        // under `session/` or `streamable/` paths.  Uses `file_path_stored`
        // (the stored copy of the path) rather than `hit.file_path` which is
        // always empty until the graph store rehydrates it.
        let path_boost = path_token_match_boost(&query_tokens, &scored.file_path_stored);

        // hook-name preference — rewards React/TS `use*` hook symbols when
        // the query itself looks like a hook name.
        let hook_boost = hook_name_boost(query, &scored.hit.symbol_name, scored.hit.node_kind);

        // infrastructure-repo penalty — demotes repos with common
        // infrastructure qualifiers (`mcp`, `transport`, `sdk`, `driver`) when
        // they share an exact name with a non-infrastructure repo's symbol.
        let infra_penalty =
            infra_repo_penalty(symbol_exact_boost, scored.hit.is_exported, &scored.hit.repo);

        scored.hit.adjusted_score = scored.base_score
            * export_boost
            * exact_boost
            * recency_boost
            * query_boost
            * symbol_exact_boost
            * pascal_type_boost
            * path_boost
            * hook_boost
            * infra_penalty;
    }

    hits.sort_by(|left, right| {
        right
            .hit
            .adjusted_score
            .partial_cmp(&left.hit.adjusted_score)
            .unwrap_or(Ordering::Equal)
    });
}

fn file_key(repo: &str, file_path: &str) -> String {
    format!("{repo}\0{file_path}")
}

fn file_name(file_path: &str) -> &str {
    Path::new(file_path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(file_path)
}

fn detect_lang(file_path: &str) -> String {
    match Path::new(file_path)
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or_default()
    {
        "ts" | "tsx" => "typescript",
        "js" | "jsx" => "javascript",
        "py" => "python",
        "rs" => "rust",
        "go" => "go",
        "java" => "java",
        _ => "unknown",
    }
    .to_owned()
}

fn first_text(
    document: &TantivyDocument,
    field: Field,
    name: &'static str,
) -> Result<String, SearchStoreError> {
    document
        .get_first(field)
        .and_then(|value| value.as_str())
        .map(ToOwned::to_owned)
        .ok_or(SearchStoreError::MissingField(name))
}

fn first_node_id(document: &TantivyDocument, field: Field) -> Result<NodeId, SearchStoreError> {
    let bytes = document
        .get_first(field)
        .and_then(|value| value.as_bytes())
        .ok_or(SearchStoreError::MissingField(FIELD_NODE_ID))?;
    if bytes.len() != 16 {
        return Err(SearchStoreError::InvalidNodeId(bytes.len()));
    }

    let mut node_id = [0_u8; 16];
    node_id.copy_from_slice(bytes);
    Ok(NodeId(node_id))
}

fn fast_u64(
    segment_reader: &tantivy::SegmentReader,
    field_name: &'static str,
    doc_id: tantivy::DocId,
) -> Result<u64, SearchStoreError> {
    segment_reader
        .fast_fields()
        .u64(field_name)?
        .values_for_doc(doc_id)
        .next()
        .ok_or(SearchStoreError::MissingField(field_name))
}

fn fast_bool(
    segment_reader: &tantivy::SegmentReader,
    field_name: &'static str,
    doc_id: tantivy::DocId,
) -> Result<bool, SearchStoreError> {
    segment_reader
        .fast_fields()
        .bool(field_name)?
        .first(doc_id)
        .ok_or(SearchStoreError::MissingField(field_name))
}

fn fast_string(
    segment_reader: &tantivy::SegmentReader,
    field_name: &'static str,
    doc_id: tantivy::DocId,
) -> Result<String, SearchStoreError> {
    let column = segment_reader
        .fast_fields()
        .str(field_name)?
        .ok_or(SearchStoreError::MissingField(field_name))?;
    let ord = column
        .ords()
        .first(doc_id)
        .ok_or(SearchStoreError::MissingField(field_name))?;
    let mut value = String::new();
    if column.ord_to_str(ord, &mut value)? {
        Ok(value)
    } else {
        Err(SearchStoreError::MissingField(field_name))
    }
}

fn fast_node_kind(
    segment_reader: &tantivy::SegmentReader,
    doc_id: tantivy::DocId,
) -> Result<NodeKind, SearchStoreError> {
    let raw = fast_u64(segment_reader, FIELD_NODE_KIND, doc_id)?;
    let raw_u8 = u8::try_from(raw).map_err(|_| SearchStoreError::InvalidNodeKind(raw))?;
    NodeKind::try_from(raw_u8).map_err(|_| SearchStoreError::InvalidNodeKind(raw))
}

impl SearchFields {
    fn from_schema(schema: &Schema) -> Self {
        Self {
            node_id: schema
                .get_field(FIELD_NODE_ID)
                .expect("node_id field should exist"),
            repo: schema
                .get_field(FIELD_REPO)
                .expect("repo field should exist"),
            file_key: schema
                .get_field(FIELD_FILE_KEY)
                .expect("file_key field should exist"),
            symbol_name: schema
                .get_field(FIELD_SYMBOL_NAME)
                .expect("symbol_name field should exist"),
            content: schema
                .get_field(FIELD_CONTENT)
                .expect("content field should exist"),
            file_name: schema
                .get_field(FIELD_FILE_NAME)
                .expect("file_name field should exist"),
            file_path_tokens: schema
                .get_field(FIELD_FILE_PATH_TOKENS)
                .expect("file_path_tokens field should exist"),
            node_kind: schema
                .get_field(FIELD_NODE_KIND)
                .expect("node_kind field should exist"),
            last_modified: schema
                .get_field(FIELD_LAST_MODIFIED)
                .expect("last_modified field should exist"),
            is_exported: schema
                .get_field(FIELD_IS_EXPORTED)
                .expect("is_exported field should exist"),
            lang: schema
                .get_field(FIELD_LANG)
                .expect("lang field should exist"),
            file_path_stored: schema
                .get_field(FIELD_FILE_PATH_STORED)
                .expect("file_path_stored field should exist"),
        }
    }
}

#[derive(Clone, Default)]
struct CodeTokenizer;

impl Tokenizer for CodeTokenizer {
    type TokenStream<'a> = IdentifierTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        IdentifierTokenStream::new(text, split_identifier_spans(text))
    }
}

#[derive(Clone, Default)]
struct PathTokenizer;

impl Tokenizer for PathTokenizer {
    type TokenStream<'a> = IdentifierTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        IdentifierTokenStream::new(text, split_identifier_spans(text))
    }
}

struct IdentifierTokenStream<'a> {
    source: &'a str,
    spans: Vec<Span>,
    cursor: usize,
    token: Token,
}

impl<'a> IdentifierTokenStream<'a> {
    fn new(source: &'a str, spans: Vec<Span>) -> Self {
        Self {
            source,
            spans,
            cursor: 0,
            token: Token::default(),
        }
    }
}

impl TokenStream for IdentifierTokenStream<'_> {
    fn advance(&mut self) -> bool {
        let Some(span) = self.spans.get(self.cursor).copied() else {
            return false;
        };

        self.token.reset();
        self.token.offset_from = span.start;
        self.token.offset_to = span.end;
        self.token.position = self.cursor;
        self.token.text.clear();
        let text = &self.source[span.start..span.end];
        if text.is_ascii() {
            self.token.text.push_str(text);
            self.token.text.make_ascii_lowercase();
        } else {
            self.token
                .text
                .extend(text.chars().flat_map(char::to_lowercase));
        }
        self.cursor += 1;
        true
    }

    fn token(&self) -> &Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.token
    }
}

#[derive(Clone, Copy)]
struct Span {
    start: usize,
    end: usize,
}

fn split_identifier_spans(input: &str) -> Vec<Span> {
    let mut spans = Vec::new();
    let mut chunk_start = None;

    for (offset, ch) in input.char_indices() {
        if ch.is_alphanumeric() {
            chunk_start.get_or_insert(offset);
            continue;
        }

        if let Some(start) = chunk_start.take() {
            split_identifier_chunk(input, start, offset, &mut spans);
        }
    }

    if let Some(start) = chunk_start {
        split_identifier_chunk(input, start, input.len(), &mut spans);
    }

    spans
}

fn split_identifier_chunk(input: &str, start: usize, end: usize, spans: &mut Vec<Span>) {
    let chunk = &input[start..end];
    let chars = chunk.char_indices().collect::<Vec<_>>();
    if chars.is_empty() {
        return;
    }

    let mut token_start = 0;
    for index in 1..chars.len() {
        let previous = chars[index - 1].1;
        let current = chars[index].1;
        let next = chars.get(index + 1).map(|(_, ch)| *ch);

        if is_boundary(previous, current, next) {
            spans.push(Span {
                start: start + token_start,
                end: start + chars[index].0,
            });
            token_start = chars[index].0;
        }
    }

    spans.push(Span {
        start: start + token_start,
        end,
    });
}

fn is_boundary(previous: char, current: char, next: Option<char>) -> bool {
    if previous.is_numeric() != current.is_numeric() {
        return true;
    }

    if previous.is_lowercase() && current.is_uppercase() {
        return true;
    }

    previous.is_uppercase() && current.is_uppercase() && next.is_some_and(char::is_lowercase)
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        path::PathBuf,
        process,
        time::{SystemTime, UNIX_EPOCH},
    };

    use gather_step_core::{NodeData, NodeKind, SourceSpan, Visibility, node_id};
    use pretty_assertions::assert_eq;

    use super::{
        CODE_TOKENIZER_NAME, PATH_TOKENIZER_NAME, SearchDocument, SearchFilters, SearchStore,
        SearchStoreError, TantivySearchStore,
    };

    fn temp_search_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be monotonic enough for tests")
            .as_nanos();
        env::temp_dir().join(format!(
            "gather-step-search-{name}-{}-{nanos}",
            process::id()
        ))
    }

    fn node(name: &str, file_path: &str) -> NodeData {
        NodeData {
            id: node_id("service-a", file_path, NodeKind::Function, name),
            kind: NodeKind::Function,
            repo: "service-a".to_owned(),
            file_path: file_path.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(format!("service-a::{name}")),
            external_id: None,
            signature: Some(format!("{name}(input: OrderInput)")),
            visibility: Some(Visibility::Public),
            span: Some(SourceSpan {
                line_start: 1,
                line_len: 1,
                column_start: 0,
                column_len: 1,
            }),
            is_virtual: false,
        }
    }

    #[test]
    fn indexes_symbol_and_matches_order_search() {
        let store =
            TantivySearchStore::open(temp_search_dir("index-symbol")).expect("store should open");
        let mut symbol = node(
            "createOrderUseCase",
            "src/workflows/order/create-order.use-case.ts",
        );
        symbol.signature = None;
        let doc = SearchDocument::from_node(&symbol, 1_713_000_000);

        store.index_symbol(&doc).expect("document should index");
        let results = store
            .search("createOrderUseCase", 10)
            .expect("search should succeed");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].symbol_name, "createOrderUseCase");
        assert!(results[0].exact_match);
    }

    #[test]
    fn search_filters_apply_before_collecting_hits() {
        let store = TantivySearchStore::open(temp_search_dir("filtered-search"))
            .expect("store should open");
        let service_a = node("sharedLookup", "src/a.ts");
        let mut service_b = node("sharedLookup", "src/b.ts");
        service_b.repo = "service-b".to_owned();
        service_b.id = node_id("service-b", "src/b.ts", NodeKind::Class, "sharedLookup");
        service_b.kind = NodeKind::Class;

        store
            .index_symbols(&[
                SearchDocument::from_node(&service_a, 1),
                SearchDocument::from_node(&service_b, 1),
            ])
            .expect("documents should index");

        let filtered = store
            .search_filtered(
                "sharedLookup",
                10,
                SearchFilters {
                    repo: Some("service-b"),
                    node_kind: Some(NodeKind::Class),
                    lang: Some("typescript"),
                },
            )
            .expect("filtered search should succeed");

        assert_eq!(filtered.len(), 1);
        // `repo` is not stored in Tantivy (S6 schema); the filter still
        // restricts the result set but the field is empty on the raw hit.
        assert_eq!(filtered[0].repo, "");
        assert_eq!(filtered[0].node_kind, NodeKind::Class);
    }

    #[test]
    fn camel_case_splits() {
        assert_eq!(
            token_texts(CODE_TOKENIZER_NAME, "getUserById"),
            vec!["get", "user", "by", "id"]
        );
    }

    #[test]
    fn pascal_case_splits() {
        assert_eq!(
            token_texts(CODE_TOKENIZER_NAME, "OrderStatusEnum"),
            vec!["order", "status", "enum"]
        );
    }

    #[test]
    fn acronym_splits() {
        assert_eq!(
            token_texts(CODE_TOKENIZER_NAME, "XMLParser"),
            vec!["xml", "parser"]
        );
    }

    #[test]
    fn path_tokenizer_splits_paths() {
        assert_eq!(
            token_texts(
                PATH_TOKENIZER_NAME,
                "src/workflows/order/create-order.use-case.ts"
            ),
            vec![
                "src",
                "workflows",
                "order",
                "create",
                "order",
                "use",
                "case",
                "ts"
            ]
        );
    }

    #[test]
    fn fuzzy_only_activates_after_exact_miss() {
        let store = TantivySearchStore::open(temp_search_dir("fuzzy")).expect("store should open");
        let doc = SearchDocument::from_node(&node("createOrderUseCase", "src/foo.ts"), 1);
        store.index_symbol(&doc).expect("document should index");

        let exact = store
            .search("order", 10)
            .expect("exact search should succeed");
        let fuzzy = store
            .search("oreder", 10)
            .expect("fuzzy search should succeed");

        assert!(exact.iter().all(|hit| hit.exact_match));
        assert!(fuzzy.iter().all(|hit| !hit.exact_match));
        assert_eq!(fuzzy[0].symbol_name, "createOrderUseCase");
    }

    #[test]
    fn from_node_does_not_duplicate_symbol_name_into_optional_fields() {
        let mut node = node("createOrderUseCase", "src/foo.ts");
        node.signature = None;
        node.qualified_name = None;

        let doc = SearchDocument::from_node(&node, 1);

        assert_eq!(doc.symbol_name, "createOrderUseCase");
        assert!(doc.content.is_empty());
        assert!(doc.description.is_empty());
    }

    #[test]
    fn replace_by_files_updates_a_file_in_one_commit_path() {
        let store = TantivySearchStore::open(temp_search_dir("replace-by-files"))
            .expect("store should open");
        let original = SearchDocument::from_node(&node("createOrderUseCase", "src/foo.ts"), 1);
        let replacement = SearchDocument::from_node(&node("updateOrderUseCase", "src/foo.ts"), 2);

        store
            .index_symbol(&original)
            .expect("original document should index");
        store
            .replace_by_files(&[("service-a", "src/foo.ts")], &[replacement])
            .expect("replacement should commit");

        let old_results = store
            .search("createOrderUseCase", 10)
            .expect("old symbol search should succeed");
        let new_results = store
            .search("updateOrderUseCase", 10)
            .expect("new symbol search should succeed");

        assert!(old_results.is_empty());
        assert_eq!(new_results.len(), 1);
        assert_eq!(new_results[0].symbol_name, "updateOrderUseCase");
    }

    #[test]
    fn deferred_commit_guard_rolls_back_unflushed_writes_on_unwind() {
        let store = TantivySearchStore::open(temp_search_dir("deferred-rollback"))
            .expect("store should open");
        let original = SearchDocument::from_node(&node("createOrderUseCase", "src/foo.ts"), 1);
        let replacement = SearchDocument::from_node(&node("updateOrderUseCase", "src/foo.ts"), 2);

        store
            .index_symbol(&original)
            .expect("original document should index");

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = store.begin_deferred_commit();
            store
                .replace_by_files(&[("service-a", "src/foo.ts")], &[replacement])
                .expect("deferred replacement should stage");
            panic!("injected deferred search panic");
        }));
        assert!(result.is_err(), "test must exercise the unwind path");

        let old_results = store
            .search("createOrderUseCase", 10)
            .expect("old symbol search should succeed");
        let new_results = store
            .search("updateOrderUseCase", 10)
            .expect("new symbol search should succeed");

        assert_eq!(old_results.len(), 1);
        assert!(
            new_results.is_empty(),
            "unflushed deferred writes must not survive guard drop"
        );
    }

    #[test]
    fn deferred_commit_guard_keeps_flushed_writes() {
        let store =
            TantivySearchStore::open(temp_search_dir("deferred-flush")).expect("store should open");
        let original = SearchDocument::from_node(&node("createOrderUseCase", "src/foo.ts"), 1);
        let replacement = SearchDocument::from_node(&node("updateOrderUseCase", "src/foo.ts"), 2);

        store
            .index_symbol(&original)
            .expect("original document should index");
        let guard = store.begin_deferred_commit();
        store
            .replace_by_files(&[("service-a", "src/foo.ts")], &[replacement])
            .expect("deferred replacement should stage");
        store.flush().expect("deferred writes should flush");
        guard.mark_flushed();

        let old_results = store
            .search("createOrderUseCase", 10)
            .expect("old symbol search should succeed");
        let new_results = store
            .search("updateOrderUseCase", 10)
            .expect("new symbol search should succeed");

        assert!(old_results.is_empty());
        assert_eq!(new_results.len(), 1);
    }

    #[test]
    fn read_only_store_rejects_writes() {
        let store = TantivySearchStore::open_read_only(temp_search_dir("read-only"))
            .expect("store should open");
        let doc = SearchDocument::from_node(&node("createOrderUseCase", "src/foo.ts"), 1);

        let error = store
            .index_symbol(&doc)
            .expect_err("read-only store should reject writes");

        assert!(matches!(error, SearchStoreError::ReadOnly));
    }

    #[test]
    fn pascal_query_type_hit_beats_higher_scored_noise() {
        // When the query starts with an uppercase letter the PascalCase-type
        // boost (1.2×) combined with the exact-symbol boost (1.6×) must lift
        // a matching Type node above a Function hit that only has a higher
        // raw BM25 score due to appearing repeatedly in content.
        let store =
            TantivySearchStore::open(temp_search_dir("pascal-boost")).expect("store should open");

        // Type node — exact name match and PascalCase type boost should win.
        let mut type_node = node("StreamableSession", "src/session.ts");
        type_node.kind = NodeKind::Type;
        type_node.id = gather_step_core::node_id(
            "service-a",
            "src/session.ts",
            NodeKind::Type,
            "StreamableSession",
        );
        let type_doc = SearchDocument::from_node(&type_node, 1_713_000_001);

        // Function node — deliberately named with "session" repeated in its
        // signature so it accumulates more BM25 mass than the type doc.
        let mut noise_node = node("handleStreamableSessionFallback", "src/session-fallback.ts");
        noise_node.signature =
            Some("handleStreamableSessionFallback(session: StreamableSession): void".to_owned());
        let noise_doc = SearchDocument::from_node(&noise_node, 1_713_000_000);

        store
            .index_symbols(&[type_doc, noise_doc])
            .expect("documents should index");

        let results = store
            .search("StreamableSession", 10)
            .expect("search should succeed");

        assert!(
            !results.is_empty(),
            "search should return at least one result"
        );
        assert_eq!(
            results[0].symbol_name,
            "StreamableSession",
            "type hit should rank first after PascalCase and exact-symbol boosts; \
             got: {:?}",
            results
                .iter()
                .map(|h| h.symbol_name.as_str())
                .collect::<Vec<_>>()
        );
        assert_eq!(results[0].node_kind, NodeKind::Type);
    }

    fn token_texts(tokenizer_name: &str, text: &str) -> Vec<String> {
        let store =
            TantivySearchStore::open(temp_search_dir("tokenizer")).expect("store should open");
        let mut tokenizer = store
            .index
            .tokenizers()
            .get(tokenizer_name)
            .expect("tokenizer should be registered");
        let mut stream = tokenizer.token_stream(text);
        let mut tokens = Vec::new();
        while let Some(token) = stream.next() {
            tokens.push(token.text.clone());
        }
        tokens
    }

    // -------------------------------------------------------------------------
    // Guarded `refresh_reader` correctness
    //
    // Rules verified:
    //  1. First search on a new store reloads the reader exactly once
    //     (sentinel: writer_commit_counter starts at 1, last_seen at 0).
    //  2. After a new commit the next search reloads once and sees the new doc.
    //  3. A subsequent search without intervening writes skips the reload
    //     and still returns the same correct results.
    // -------------------------------------------------------------------------

    #[test]
    fn guarded_refresh_reader_sees_new_commits_and_skips_reload_between_writes() {
        use super::SearchWorkload;

        // Use LongRunning workload so refresh_reader_if_needed is active
        // (OneShot always skips the guard to avoid the reload overhead).
        let store = TantivySearchStore::open_with_workload(
            temp_search_dir("refresh-guard"),
            SearchWorkload::LongRunning,
        )
        .expect("store should open");

        let doc_a = SearchDocument::from_node(&node("alphaFn", "src/alpha.ts"), 1_000);

        // Index and commit the first document.
        store
            .index_symbol(&doc_a)
            .expect("first index should succeed");

        // Search #1 — sentinel forces one reload; doc_a must be visible.
        // (last_seen_commit = 0, writer_commit_counter = 2 after flush)
        let first_results = store
            .search("alphaFn", 10)
            .expect("first search should succeed");
        assert_eq!(
            first_results.len(),
            1,
            "first search must find the first indexed document"
        );
        assert_eq!(first_results[0].symbol_name, "alphaFn");

        // Index a second document and commit.
        let doc_b = SearchDocument::from_node(&node("betaFn", "src/beta.ts"), 2_000);
        store
            .index_symbol(&doc_b)
            .expect("second index should succeed");

        // Search #2 — new commit counter means refresh happens; both docs visible.
        let second_results = store
            .search("betaFn", 10)
            .expect("second search should succeed");
        assert_eq!(
            second_results.len(),
            1,
            "second search must find the second indexed document"
        );
        assert_eq!(second_results[0].symbol_name, "betaFn");

        // Search #3 — no new writes; last_seen_commit == writer_commit_counter,
        // so refresh_reader is skipped.  Results must be identical to search #2.
        let third_results = store
            .search("betaFn", 10)
            .expect("third search (no writes) should succeed");
        assert_eq!(
            third_results.len(),
            1,
            "third search must still find betaFn without a stale-reader regression"
        );
        assert_eq!(third_results[0].symbol_name, "betaFn");

        // Verify the first doc is also still visible (reader is not stale).
        let alpha_results = store
            .search("alphaFn", 10)
            .expect("alpha re-search should succeed");
        assert_eq!(
            alpha_results.len(),
            1,
            "first document must remain visible after repeated searches"
        );
    }

    /// For [`SearchWorkload::OneShot`] the guard always skips the reload.
    /// Correctness is preserved because `flush()` already reloads the reader
    /// before any search is issued in a CLI indexing run.
    #[test]
    fn oneshot_workload_skips_refresh_after_flush() {
        use super::SearchWorkload;

        let store = TantivySearchStore::open_with_workload(
            temp_search_dir("oneshot-guard"),
            SearchWorkload::OneShot,
        )
        .expect("store should open");

        let doc = SearchDocument::from_node(&node("oneShotFn", "src/one.ts"), 500);
        // `index_symbol` calls `commit()` which calls `flush()` which reloads
        // the reader.  Subsequent searches must still find the document even
        // though refresh_reader_if_needed is a no-op for OneShot.
        store.index_symbol(&doc).expect("index should succeed");

        let results = store
            .search("oneShotFn", 10)
            .expect("search should succeed");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].symbol_name, "oneShotFn");
    }

    // -------------------------------------------------------------------------
    // query-aware scoring tests
    // -------------------------------------------------------------------------

    /// Path-token boost fires when query tokens appear in the hit's file path.
    #[test]
    fn path_token_boost_fires_for_matching_path() {
        use super::{path_token_match_boost, tokenize_camel_case};

        let tokens = tokenize_camel_case("StreamableSession");
        // Path contains both "streamable" and "session" → multiplier should
        // be 1.08 × 1.08 = 1.1664, which is less than the 1.3 cap.
        let boost = path_token_match_boost(&tokens, "src/session/streamable-handler.ts");
        assert!(
            boost > 1.1 && boost <= 1.3,
            "expected boost in (1.1, 1.3], got {boost}"
        );
    }

    /// Path-token boost does NOT fire when no query tokens appear in the path.
    #[test]
    fn path_token_boost_does_not_fire_for_unrelated_path() {
        use super::{path_token_match_boost, tokenize_camel_case};

        let tokens = tokenize_camel_case("StreamableSession");
        let boost = path_token_match_boost(&tokens, "src/mcp/transport/server.ts");
        assert!(
            (boost - 1.0).abs() < f32::EPSILON,
            "expected no boost for unrelated path, got {boost}"
        );
    }

    /// Hook-name boost fires when query and symbol both start with `use` + uppercase.
    #[test]
    fn hook_name_boost_fires_for_hook_query_and_hook_symbol() {
        use super::hook_name_boost;

        let boost = hook_name_boost("useAuthSession", "useAuthSession", NodeKind::Function);
        assert!(
            (boost - 1.3).abs() < f32::EPSILON,
            "expected hook-name boost of 1.3, got {boost}"
        );
    }

    /// Hook-name boost does NOT fire for a non-hook query.
    #[test]
    fn hook_name_boost_does_not_fire_for_non_hook_query() {
        use super::hook_name_boost;

        // PascalCase type query — not a hook.
        let boost = hook_name_boost("AuthSession", "useAuthSession", NodeKind::Function);
        assert!(
            (boost - 1.0).abs() < f32::EPSILON,
            "expected no hook boost for PascalCase query, got {boost}"
        );
    }

    /// Hook-name boost does NOT fire when the symbol kind is not Function.
    #[test]
    fn hook_name_boost_does_not_fire_for_non_function_kind() {
        use super::hook_name_boost;

        let boost = hook_name_boost("useAuthSession", "useAuthSession", NodeKind::Type);
        assert!(
            (boost - 1.0).abs() < f32::EPSILON,
            "expected no hook boost for Type node kind, got {boost}"
        );
    }

    /// Infrastructure-repo penalty fires for repos named with infra qualifiers.
    #[test]
    fn infra_repo_penalty_fires_for_mcp_repo() {
        use super::infra_repo_penalty;

        // Exact symbol boost 1.6×, exported, repo contains "mcp".
        let penalty = infra_repo_penalty(1.6, true, "mcp-transport-layer");
        assert!(
            (penalty - 0.85).abs() < f32::EPSILON,
            "expected infra penalty 0.85 for mcp repo, got {penalty}"
        );
    }

    /// Infrastructure-repo penalty does NOT fire for non-infra repos.
    #[test]
    fn infra_repo_penalty_does_not_fire_for_normal_repo() {
        use super::infra_repo_penalty;

        let penalty = infra_repo_penalty(1.6, true, "frontend_standard");
        assert!(
            (penalty - 1.0).abs() < f32::EPSILON,
            "expected no penalty for non-infra repo, got {penalty}"
        );
    }

    /// Infrastructure-repo penalty does NOT fire when `symbol_exact_boost` != 1.6.
    #[test]
    fn infra_repo_penalty_requires_exact_name_match() {
        use super::infra_repo_penalty;

        // Boost is 1.0 (not an exact match) — penalty must not fire.
        let penalty = infra_repo_penalty(1.0, true, "mcp-transport-layer");
        assert!(
            (penalty - 1.0).abs() < f32::EPSILON,
            "expected no penalty when boost is not 1.6, got {penalty}"
        );
    }

    /// End-to-end: a session-related hit with a session-path wins over a same-score
    /// hit whose path has no session tokens, when the query is `useAuthSession`.
    #[test]
    fn path_and_hook_boost_ranks_session_hook_above_unrelated_same_score_hit() {
        let store = TantivySearchStore::open(temp_search_dir("path-hook-boost"))
            .expect("store should open");

        // Hook symbol in a session-related path — should win.
        let mut session_node = node(
            "useAuthSession",
            "src/hooks/auth/session/use-auth-session.ts",
        );
        session_node.kind = NodeKind::Function;
        session_node.id = gather_step_core::node_id(
            "service-a",
            "src/hooks/auth/session/use-auth-session.ts",
            NodeKind::Function,
            "useAuthSession",
        );
        let session_doc = SearchDocument::from_node(&session_node, 1_713_000_001);

        // Same name but lives in an unrelated transport path — no path bonus.
        let mut transport_node = node("useAuthSession", "src/mcp/transport/connection.ts");
        transport_node.kind = NodeKind::Function;
        transport_node.id = gather_step_core::node_id(
            "service-a",
            "src/mcp/transport/connection.ts",
            NodeKind::Function,
            "useAuthSession",
        );
        let transport_doc = SearchDocument::from_node(&transport_node, 1_713_000_000);

        store
            .index_symbols(&[session_doc, transport_doc])
            .expect("documents should index");

        let results = store
            .search("useAuthSession", 10)
            .expect("search should succeed");

        assert!(
            !results.is_empty(),
            "search should return at least one result"
        );
        assert_eq!(
            results[0].symbol_name,
            "useAuthSession",
            "the session-path hook hit should rank first; got: {:?}",
            results
                .iter()
                .map(|h| h.symbol_name.as_str())
                .collect::<Vec<_>>()
        );
    }

    // -------------------------------------------------------------------------
    // Schema-version guard tests
    //
    // These tests exercise `check_or_write_schema_version` directly against
    // a real filesystem directory because the version file is not used in
    // in-memory (RamDirectory) test indexes.
    // -------------------------------------------------------------------------

    /// A directory with no version file is treated as a fresh index: the
    /// current version is stamped and `Ok(())` is returned.
    #[test]
    fn schema_version_check_stamps_fresh_directory() {
        use super::{SEARCH_INDEX_VERSION, SEARCH_VERSION_FILE, check_or_write_schema_version};
        use std::fs;

        let dir = temp_search_dir("version-fresh");
        fs::create_dir_all(&dir).expect("dir should exist");

        check_or_write_schema_version(&dir).expect("fresh directory should succeed");

        let written = fs::read_to_string(dir.join(SEARCH_VERSION_FILE))
            .expect("version file should have been created");
        let parsed: u32 = written.trim().parse().expect("version should be numeric");
        assert_eq!(parsed, SEARCH_INDEX_VERSION);

        let _ = fs::remove_dir_all(&dir);
    }

    /// A directory already at the current version passes the check.
    #[test]
    fn schema_version_check_accepts_current_version() {
        use super::{SEARCH_INDEX_VERSION, SEARCH_VERSION_FILE, check_or_write_schema_version};
        use std::fs;

        let dir = temp_search_dir("version-current");
        fs::create_dir_all(&dir).expect("dir should exist");
        fs::write(
            dir.join(SEARCH_VERSION_FILE),
            format!("{SEARCH_INDEX_VERSION}\n"),
        )
        .expect("write version file");

        check_or_write_schema_version(&dir)
            .expect("directory at current version should pass check");

        let _ = fs::remove_dir_all(&dir);
    }

    /// A directory stamped with a mismatched version returns `VersionMismatch`.
    #[test]
    fn schema_version_check_rejects_mismatched_version() {
        use super::{SEARCH_INDEX_VERSION, SEARCH_VERSION_FILE, check_or_write_schema_version};
        use std::fs;

        let dir = temp_search_dir("version-mismatch");
        fs::create_dir_all(&dir).expect("dir should exist");
        let mismatched_version = SEARCH_INDEX_VERSION + 1;
        fs::write(
            dir.join(SEARCH_VERSION_FILE),
            format!("{mismatched_version}\n"),
        )
        .expect("write mismatched version file");

        let err = check_or_write_schema_version(&dir)
            .expect_err("mismatched-version directory must be rejected");

        assert!(
            matches!(
                err,
                SearchStoreError::VersionMismatch {
                    stored,
                    expected,
                } if stored == mismatched_version && expected == SEARCH_INDEX_VERSION
            ),
            "expected VersionMismatch error; got: {err}"
        );
        assert!(
            err.to_string()
                .contains("gather-step clean && gather-step index"),
            "error message should instruct the user to rebuild generated state; got: {err}"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// A non-empty index directory with no version sentinel must be rejected
    /// with `VersionMismatch { stored: 0 }`, not silently stamped as fresh.
    /// Stamping it would let the next schema-dependent lookup panic on a
    /// missing field.
    #[test]
    fn schema_version_check_rejects_unstamped_non_empty_directory() {
        use super::{SEARCH_INDEX_VERSION, check_or_write_schema_version};
        use std::fs;

        let dir = temp_search_dir("version-unstamped");
        fs::create_dir_all(&dir).expect("dir should exist");
        // Simulate index artifacts without the schema-version sentinel.
        fs::write(dir.join("meta.json"), "{}").expect("write index artifact");

        let err = check_or_write_schema_version(&dir)
            .expect_err("unstamped non-empty directory must be rejected");

        assert!(
            matches!(
                err,
                SearchStoreError::VersionMismatch {
                    stored: 0,
                    expected,
                } if expected == SEARCH_INDEX_VERSION
            ),
            "expected VersionMismatch {{ stored: 0 }} for unstamped non-empty directory; got: {err}"
        );

        // Sentinel file must NOT have been written; the directory stays as-is
        // until the operator cleans it.
        assert!(
            !dir.join(super::SEARCH_VERSION_FILE).exists(),
            "version sentinel must not be stamped on unstamped non-empty directory"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// A directory containing ONLY the schema-version sentinel (e.g. from a
    /// prior clean run) is treated as fresh — re-stamping is a no-op when
    /// the existing stamp matches the current version, and other branches
    /// already cover stamped directories.  This guards against the edge case
    /// where `directory_has_index_artifacts` would otherwise misclassify a
    /// directory that contains only its own sentinel as having artifacts.
    #[test]
    fn schema_version_check_treats_sentinel_only_directory_as_fresh() {
        use super::{SEARCH_INDEX_VERSION, SEARCH_VERSION_FILE, check_or_write_schema_version};
        use std::fs;

        let dir = temp_search_dir("version-sentinel-only");
        fs::create_dir_all(&dir).expect("dir should exist");
        // No artifacts — directory is empty.  Stamping should succeed.
        check_or_write_schema_version(&dir).expect("empty dir should be stampable");

        // Now the sentinel exists.  A subsequent call must accept it (the
        // current-version branch in `check_or_write_schema_version`).
        let stamp = fs::read_to_string(dir.join(SEARCH_VERSION_FILE)).expect("sentinel readable");
        assert_eq!(stamp.trim(), SEARCH_INDEX_VERSION.to_string());
        check_or_write_schema_version(&dir).expect("sentinel-only dir should accept current ver");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn schema_version_check_ignores_stray_non_index_files_in_fresh_directory() {
        use super::{SEARCH_INDEX_VERSION, SEARCH_VERSION_FILE, check_or_write_schema_version};
        use std::fs;

        let dir = temp_search_dir("version-stray-file");
        fs::create_dir_all(&dir).expect("dir should exist");
        fs::write(dir.join(".DS_Store"), "finder metadata").expect("write stray file");

        check_or_write_schema_version(&dir)
            .expect("stray non-index files should not be treated as legacy index artifacts");

        let stamp = fs::read_to_string(dir.join(SEARCH_VERSION_FILE)).expect("sentinel readable");
        assert_eq!(stamp.trim(), SEARCH_INDEX_VERSION.to_string());

        let _ = fs::remove_dir_all(&dir);
    }

    /// Round-trip: index a fresh store at the current schema version and
    /// confirm that search still returns the expected hit after the field
    /// removal (i.e., no regression from removing `FIELD_FILE_PATH`).
    #[test]
    fn fresh_index_at_current_schema_version_returns_search_hits() {
        let store =
            TantivySearchStore::open(temp_search_dir("schema-v2-roundtrip")).expect("store opens");

        let doc = SearchDocument::from_node(
            &node("processOrderEvent", "src/events/order-processor.ts"),
            1_713_000_000,
        );
        store.index_symbol(&doc).expect("document should index");

        let results = store
            .search("processOrderEvent", 10)
            .expect("search should succeed");

        assert_eq!(results.len(), 1, "search must return one hit");
        assert_eq!(results[0].symbol_name, "processOrderEvent");
        assert_eq!(results[0].node_kind, NodeKind::Function);
        // `repo` and `file_path` are intentionally empty on raw search hits —
        // callers rehydrate them via the graph store using `node_id`.
        assert_eq!(results[0].repo, "");
        assert_eq!(results[0].file_path, "");
    }
}
