use std::{
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use gather_step_core::{
    EdgeData, EdgeKind, EdgeMetadata, MIGRATION_FILTERS_METADATA_PREFIX, NodeData, NodeId,
    NodeKind, ResolverStrategy, VIRTUAL_NODE_REPO,
};
use redb::{
    Database, DatabaseError, Durability, MultimapTable, MultimapTableDefinition, ReadableDatabase,
    ReadableMultimapTable, ReadableTable, ReadableTableMetadata, StorageError, TableDefinition,
};
use rustc_hash::FxHashSet;
use serde::Serialize;
use thiserror::Error;

use crate::StorageDaemonMetadata;

type EdgeIdBytes = [u8; 16];
type NodeIdBytes = [u8; 16];
#[cfg(any(test, feature = "test-support"))]
type AttributedEdge = (EdgeData, String, bool, NodeKind, String, bool, NodeKind);
type StringId = u32;

/// On-disk edge representation. Drops `is_cross_file` (derivable) and keeps
/// `owner_file` because it is needed for delete operations that don't have
/// access to the inverted indexes.
///
/// `is_cross_file` is reconstructed as `false` by default on decode. Callers
/// that need the correct value must derive it from the node data.
#[derive(Clone, Debug, PartialEq, Eq, bitcode::Encode, bitcode::Decode)]
struct StoredEdge {
    source: NodeId,
    target: NodeId,
    kind: gather_step_core::EdgeKind,
    owner_file: NodeId,
    metadata: StoredEdgeMetadata,
}

/// Compact on-disk mirror of [`EdgeMetadata`].
///
/// The public metadata type stays serde-friendly and string-based. The graph
/// store is a fresh/rebuildable index, so it can encode known high-repeat
/// strings as enum tags while preserving unknown producer strings.
#[derive(Clone, Debug, PartialEq, Eq, bitcode::Encode, bitcode::Decode)]
struct StoredEdgeMetadata {
    weight: Option<u32>,
    confidence: Option<u16>,
    timestamp_unix: Option<i64>,
    drift_kind: Option<StoredDriftKind>,
    resolver: Option<StoredResolver>,
}

#[derive(Clone, Debug, PartialEq, Eq, bitcode::Encode, bitcode::Decode)]
enum StoredResolver {
    Known(ResolverStrategy),
    Other(String),
}

#[derive(Clone, Debug, PartialEq, Eq, bitcode::Encode, bitcode::Decode)]
enum StoredDriftKind {
    Shape,
    Type,
    Optionality,
    MissingField,
    ExtraField,
    MigrationFilters(String),
    Other(String),
}

impl StoredEdgeMetadata {
    fn from_public(metadata: &EdgeMetadata) -> Self {
        Self {
            weight: metadata.weight,
            confidence: metadata.confidence,
            timestamp_unix: metadata.timestamp_unix,
            drift_kind: metadata
                .drift_kind
                .as_deref()
                .map(StoredDriftKind::from_public),
            resolver: metadata
                .resolver
                .as_deref()
                .map(StoredResolver::from_public),
        }
    }

    fn into_public(self) -> EdgeMetadata {
        EdgeMetadata {
            weight: self.weight,
            confidence: self.confidence,
            timestamp_unix: self.timestamp_unix,
            drift_kind: self.drift_kind.map(StoredDriftKind::into_public),
            resolver: self.resolver.map(StoredResolver::into_public),
        }
    }
}

impl StoredResolver {
    fn from_public(value: &str) -> Self {
        ResolverStrategy::from_str(value).map_or_else(|| Self::Other(value.to_owned()), Self::Known)
    }

    fn into_public(self) -> String {
        match self {
            Self::Known(strategy) => strategy.as_str().to_owned(),
            Self::Other(value) => value,
        }
    }
}

impl StoredDriftKind {
    fn from_public(value: &str) -> Self {
        match value {
            "shape" => Self::Shape,
            "type" => Self::Type,
            "optionality" => Self::Optionality,
            "missing_field" => Self::MissingField,
            "extra_field" => Self::ExtraField,
            value => value
                .strip_prefix(MIGRATION_FILTERS_METADATA_PREFIX)
                .map_or_else(
                    || Self::Other(value.to_owned()),
                    |filters| Self::MigrationFilters(filters.to_owned()),
                ),
        }
    }

    fn into_public(self) -> String {
        match self {
            Self::Shape => "shape".to_owned(),
            Self::Type => "type".to_owned(),
            Self::Optionality => "optionality".to_owned(),
            Self::MissingField => "missing_field".to_owned(),
            Self::ExtraField => "extra_field".to_owned(),
            Self::MigrationFilters(filters) => {
                format!("{MIGRATION_FILTERS_METADATA_PREFIX}{filters}")
            }
            Self::Other(value) => value,
        }
    }
}

/// On-disk node representation. Signatures are interned in the `SIGNATURES`
/// table and stored as a `u32` ID to reduce per-node payload size.
/// `repo` and `file_path` are already interned via `REPO_IDS`/`FILE_PATH_IDS`.
#[derive(Clone, Debug, PartialEq, Eq, bitcode::Encode, bitcode::Decode)]
struct StoredNode {
    id: NodeId,
    kind: NodeKind,
    repo_id: StringId,
    file_path_id: StringId,
    name: String,
    qualified_name: Option<String>,
    external_id: Option<String>,
    /// Interned signature — look up in `SIGNATURES` table by this ID.
    /// `None` means the node has no signature.
    signature_id: Option<u32>,
    visibility: Option<gather_step_core::Visibility>,
    span: Option<gather_step_core::SourceSpan>,
    is_virtual: bool,
}

struct NodeIndexTables<'txn> {
    by_file: MultimapTable<'txn, &'static [u8], NodeIdBytes>,
    by_repo: MultimapTable<'txn, u32, NodeIdBytes>,
    by_type: MultimapTable<'txn, u8, NodeIdBytes>,
    by_external_id: MultimapTable<'txn, &'static str, NodeIdBytes>,
    cross_file_candidates: MultimapTable<'txn, &'static str, NodeIdBytes>,
    event_family_index: MultimapTable<'txn, &'static str, NodeIdBytes>,
    route_key_index: MultimapTable<'txn, &'static str, NodeIdBytes>,
    shared_symbol_name_index: MultimapTable<'txn, &'static str, NodeIdBytes>,
}

struct EdgeIndexTables<'txn> {
    nodes: redb::Table<'txn, NodeIdBytes, &'static [u8]>,
    edges: redb::Table<'txn, EdgeIdBytes, &'static [u8]>,
    edges_out: MultimapTable<'txn, NodeIdBytes, EdgeIdBytes>,
    edges_in: MultimapTable<'txn, NodeIdBytes, EdgeIdBytes>,
    edges_by_owner: MultimapTable<'txn, NodeIdBytes, EdgeIdBytes>,
    edge_kind_counts: redb::Table<'txn, u8, u64>,
}

const NODES: TableDefinition<NodeIdBytes, &[u8]> = TableDefinition::new("nodes");
const EDGES: TableDefinition<EdgeIdBytes, &[u8]> = TableDefinition::new("edges");
const EDGES_OUT: MultimapTableDefinition<NodeIdBytes, EdgeIdBytes> =
    MultimapTableDefinition::new("edges_out");
const EDGES_IN: MultimapTableDefinition<NodeIdBytes, EdgeIdBytes> =
    MultimapTableDefinition::new("edges_in");
const EDGES_BY_OWNER: MultimapTableDefinition<NodeIdBytes, EdgeIdBytes> =
    MultimapTableDefinition::new("edges_by_owner");
const EDGE_KIND_COUNTS: TableDefinition<u8, u64> = TableDefinition::new("edge_kind_counts");
const BY_FILE: MultimapTableDefinition<&[u8], NodeIdBytes> =
    MultimapTableDefinition::new("by_file");
const BY_REPO: MultimapTableDefinition<u32, NodeIdBytes> = MultimapTableDefinition::new("by_repo");
const BY_TYPE: MultimapTableDefinition<u8, NodeIdBytes> = MultimapTableDefinition::new("by_type");
const BY_EXTERNAL_ID: MultimapTableDefinition<&str, NodeIdBytes> =
    MultimapTableDefinition::new("by_external_id");
const CROSS_FILE_CANDIDATES: MultimapTableDefinition<&str, NodeIdBytes> =
    MultimapTableDefinition::new("cross_file_candidates");
/// Index for O(1) event-family lookups.
///
/// Key: normalized event name (the suffix after the last `__` in a virtual
/// event/topic/queue/subject/stream node's `external_id`, lowercased).
/// Value: `NodeId` of the matching virtual node.
///
/// Maintained on every insert/remove of a virtual event-like node that carries
/// an `external_id`.  Queried by `resolve_event_targets` instead of scanning
/// all nodes of each event kind when the normalized key is known.
const EVENT_FAMILY_INDEX: MultimapTableDefinition<&str, NodeIdBytes> =
    MultimapTableDefinition::new("event_family_index");
/// Index for O(1) route/api-call key lookups.
///
/// Key: canonical route key string `"{METHOD}__{path}"` (both components
/// normalised by [`Self::canonical_route_key_for_stored_node`]).
/// Value: `NodeId` of the matching virtual `Route` node.
///
/// Maintained on every insert/remove of a virtual `Route` node that carries
/// an `external_id` with a recognisable `__route__` or `__api_call__` prefix.
/// Queried by `resolve_route_target` instead of a full `nodes_by_type` scan.
const ROUTE_KEY_INDEX: MultimapTableDefinition<&str, NodeIdBytes> =
    MultimapTableDefinition::new("route_key_index");
/// Index for O(1) `SharedSymbol`-stub lookups by short name.
///
/// Key: lowercased trailing segment of a virtual `SharedSymbol`'s
/// `external_id` / `qualified_name` (the part after the last `__` or `::`).
/// For a hook stub `__hook__@workspace/ui::useAuthentication` the key is
/// `useauthentication`; for a shared-contract stub
/// `__shared__@workspace/contracts__OrderDto` the key is `orderdto`.
/// Value: `NodeId` of the matching virtual node.
///
/// Maintained on every insert / remove of a virtual `SharedSymbol` node
/// that carries an `external_id` or `qualified_name`. Replaces the
/// previous full `nodes_by_type(SharedSymbol)` table scans in the proof
/// builder's peer-discovery paths (hook trace, shared-contract impact,
/// upstream widening), each of which paid O(N) over every stub on every
/// pack call. Real (non-virtual) `SharedSymbol` nodes are excluded —
/// their consumers are already discoverable via direct edges from the
/// declaration node.
const SHARED_SYMBOL_NAME_INDEX: MultimapTableDefinition<&str, NodeIdBytes> =
    MultimapTableDefinition::new("shared_symbol_name_index");
const REPO_IDS: TableDefinition<&str, u32> = TableDefinition::new("repo_ids");
const REPOS: TableDefinition<u32, &str> = TableDefinition::new("repos");
const FILE_PATH_IDS: TableDefinition<&str, u32> = TableDefinition::new("file_path_ids");
const FILE_PATHS: TableDefinition<u32, &str> = TableDefinition::new("file_paths");
/// Forward map: signature string → interned ID.
const SIGNATURE_IDS: TableDefinition<&str, u32> = TableDefinition::new("signature_ids");
/// Reverse map: interned ID → signature string.
const SIGNATURES: TableDefinition<u32, &str> = TableDefinition::new("signatures");
const STRING_METADATA: TableDefinition<u8, u32> = TableDefinition::new("string_metadata");
const GRAPH_SCHEMA: TableDefinition<&str, u32> = TableDefinition::new("graph_schema");
const GRAPH_SCHEMA_VERSION_KEY: &str = "version";
const NEXT_REPO_ID_KEY: u8 = 0;
const NEXT_FILE_PATH_ID_KEY: u8 = 1;
const NEXT_SIGNATURE_ID_KEY: u8 = 2;

/// Current graph-store schema version.
///
/// v3.1 is a fresh generated-state release. There are no production users for
/// older graph store layouts, so the physical schema baseline starts at zero
/// and does not carry migration or upgrade branches.
pub const GRAPH_SCHEMA_VERSION: u32 = 0;

/// All five cross-repo and total-edge counters aggregated in one EDGES scan.
///
/// Returned by [`GraphStoreDb::count_edge_summary`].  The three split counters
/// (`true_cross_repo_edges` + `history_ownership_edges` +
/// `virtual_other_cross_repo_edges`) must equal `cross_repo_edges`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EdgeCountSummary {
    /// Total number of edges in the graph.
    pub total_edges: usize,
    /// All edges that cross a repo boundary (including virtual endpoints).
    pub cross_repo_edges: u64,
    /// Edges where both endpoints are in real (non-virtual) repos.
    pub true_cross_repo_edges: usize,
    /// Edges whose target is a virtual `Author` node (git-history coverage).
    pub history_ownership_edges: usize,
    /// Edges whose target is a virtual non-`Author` node.
    pub virtual_other_cross_repo_edges: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct GraphTableFootprint {
    pub name: String,
    pub table_kind: String,
    pub entries: u64,
    pub stored_bytes: u64,
    pub metadata_bytes: u64,
    pub fragmented_bytes: u64,
    pub leaf_pages: u64,
    pub branch_pages: u64,
    pub tree_height: u32,
}

pub trait GraphStore {
    fn insert_node(&self, node: &NodeData) -> Result<(), GraphStoreError>;
    fn get_node(&self, id: NodeId) -> Result<Option<NodeData>, GraphStoreError>;
    fn delete_node(&self, id: NodeId) -> Result<Option<NodeData>, GraphStoreError>;
    fn insert_edge(&self, edge: &EdgeData) -> Result<(), GraphStoreError>;
    fn delete_edge(&self, edge: &EdgeData) -> Result<(), GraphStoreError>;
    fn get_outgoing(&self, source: NodeId) -> Result<Vec<EdgeData>, GraphStoreError>;
    fn get_incoming(&self, target: NodeId) -> Result<Vec<EdgeData>, GraphStoreError>;
    fn edges_by_owner(&self, owner_file: NodeId) -> Result<Vec<EdgeData>, GraphStoreError>;
    fn delete_edges_for_owner(&self, owner_file: NodeId) -> Result<(), GraphStoreError>;
    fn delete_edges_for_owner_by_kind(
        &self,
        owner_file: NodeId,
        kinds: &[EdgeKind],
    ) -> Result<(), GraphStoreError>;
    fn replace_edges_for_owners_by_kind(
        &self,
        owner_files: &[NodeId],
        kinds: &[EdgeKind],
        edges: &[EdgeData],
    ) -> Result<(), GraphStoreError>;
    fn nodes_by_file(&self, repo: &str, file_path: &str) -> Result<Vec<NodeData>, GraphStoreError>;
    fn nodes_by_repo(&self, repo: &str) -> Result<Vec<NodeData>, GraphStoreError>;
    fn count_nodes_by_repo(&self, repo: &str) -> Result<usize, GraphStoreError>;
    /// Count nodes for `repo` whose decoded `kind` equals `kind`. Used by the
    /// fresh-index summary path to report authoritative per-repo file counts
    /// without materializing every `NodeData` for the repo.
    fn count_nodes_by_repo_and_kind(
        &self,
        repo: &str,
        kind: NodeKind,
    ) -> Result<usize, GraphStoreError>;
    /// Count edges whose `owner_file` belongs to `repo`. Used by the
    /// fresh-index summary so per-repo edge counts reflect the indexed graph
    /// state rather than the per-batch write delta (which is zero on warm
    /// reruns where no files changed). One read transaction per call.
    fn count_edges_by_owner_repo(&self, repo: &str) -> Result<u64, GraphStoreError>;
    fn nodes_by_external_id(
        &self,
        kind: NodeKind,
        external_id: &str,
    ) -> Result<Vec<NodeData>, GraphStoreError>;
    fn nodes_by_type(&self, kind: NodeKind) -> Result<Vec<NodeData>, GraphStoreError>;
    fn nodes_by_candidate_keys(
        &self,
        candidate_keys: &[String],
    ) -> Result<Vec<NodeData>, GraphStoreError>;
    fn count_nodes_by_kind(&self, kind: NodeKind) -> Result<usize, GraphStoreError>;
    fn count_edges_by_kind(&self, kind: EdgeKind) -> Result<usize, GraphStoreError>;
    /// Look up virtual event-like nodes by their normalised event name
    /// (the suffix after the last `__` in `external_id`, lowercased).
    ///
    /// This is an O(1) index lookup — it does **not** scan all nodes of the
    /// given kind.  Only nodes indexed by the `EVENT_FAMILY_INDEX` are
    /// returned; nodes without an `external_id` or without the `__`-delimited
    /// convention are invisible to this query.
    ///
    /// Use [`GraphStore::nodes_by_type`] when you need the full kind scan
    /// (e.g. orphan-topic enumeration).
    fn nodes_by_event_family_name(
        &self,
        normalized_name: &str,
    ) -> Result<Vec<NodeData>, GraphStoreError>;
    /// Look up virtual `Route` nodes by their canonical route key
    /// `"{METHOD}__{path}"`.
    ///
    /// This is an O(1) index lookup — it does **not** scan all `Route` nodes.
    fn nodes_by_route_key(&self, canonical_key: &str) -> Result<Vec<NodeData>, GraphStoreError>;
    /// Look up virtual `SharedSymbol` stubs by their lowercased trailing
    /// short name (the segment after the last `::` or `__` in
    /// `external_id` / `qualified_name`).
    ///
    /// This is an O(1) index lookup — it does **not** scan all
    /// `SharedSymbol` nodes. Used by the proof engine's peer-discovery
    /// paths in place of the previous `nodes_by_type(SharedSymbol)`
    /// scans, which paid O(N) per pack call. Real (non-virtual)
    /// `SharedSymbol` nodes are not indexed; their consumers reach them
    /// via direct edges from the canonical declaration.
    fn nodes_by_shared_symbol_name(
        &self,
        short_name: &str,
    ) -> Result<Vec<NodeData>, GraphStoreError>;
    fn bulk_insert(&self, nodes: &[NodeData], edges: &[EdgeData]) -> Result<(), GraphStoreError>;
}

pub struct GraphStoreDb {
    db: Database,
    path: PathBuf,
    /// When `true`, write transactions use `Durability::None` to skip fsync.
    /// Set during bulk indexing, cleared after the last batch write.
    bulk_mode: std::sync::atomic::AtomicBool,
}

#[derive(Debug, Error)]
pub enum GraphStoreError {
    #[error("failed to access graph database file: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to decode bitcode payload: {0}")]
    Decode(#[from] bitcode::Error),
    #[error(
        "graph storage `{path}` is locked by gather-step pid {pid} for workspace `{workspace_root}` \
         (started_at_epoch_ms={started_at_epoch_ms}); stop that process or wait for it to exit before retrying"
    )]
    StorageHeldByDaemon {
        path: PathBuf,
        pid: u32,
        started_at_epoch_ms: u128,
        workspace_root: String,
    },
    #[error("graph store error: {0}")]
    Storage(String),
    #[error(
        "graph index at `{path}` is corrupt or incomplete; run `gather-step index --auto-recover` to rebuild generated state, or run `gather-step clean && gather-step index`"
    )]
    Corrupt { path: PathBuf },
    #[error(
        "graph storage `{path}` is already locked by another gather-step process; \
         if `watch` or `serve --watch` is running, stop it or wait for it to exit before retrying"
    )]
    StorageHeld { path: PathBuf },
    #[error(
        "graph storage `{path}` is shared across active handles; compact it only before the \
         workspace stores are shared or after all clones are dropped"
    )]
    CompactionRequiresExclusiveHandle { path: PathBuf },
    #[error("node not found for edge reference: {0:?}")]
    MissingNode(NodeId),
    #[error("edge owner node is not a file: {0:?}")]
    OwnerNotAFile(NodeId),
}

impl GraphStoreError {
    pub(crate) fn storage(error: impl core::fmt::Display) -> Self {
        Self::Storage(error.to_string())
    }
}

/// Default redb page cache size.
///
/// redb's internal default is conservative (a few MiB).  Production graph
/// stores are hundreds of MiB, so a larger cache keeps hot pages resident and
/// reduces page faults during `pack` / `impact` queries.  256 MiB is a safe
/// middle ground for a workstation; bumping further shows diminishing returns
/// once the working set fits.
const DEFAULT_GRAPH_CACHE_BYTES: usize = 256 * 1024 * 1024;

impl GraphStoreDb {
    fn is_missing_table_error(error: &impl core::fmt::Display) -> bool {
        error.to_string().contains("does not exist")
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, GraphStoreError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        if path.is_dir() {
            return Err(GraphStoreError::Io(std::io::Error::new(
                ErrorKind::IsADirectory,
                format!("graph store path must be a file: {}", path.display()),
            )));
        }

        let is_new = !path.exists();
        let mut builder = Database::builder();
        builder.set_cache_size(DEFAULT_GRAPH_CACHE_BYTES);
        let db = if is_new {
            builder
                .create(&path)
                .map_err(|error| Self::map_open_error(&path, error))?
        } else {
            builder
                .open(&path)
                .map_err(|error| Self::map_open_error(&path, error))?
        };
        // Caller must have validated path via cli::path_safety before opening.
        if is_new {
            crate::fs_mode::apply_private_file(&path)?;
        }

        let store = Self {
            db,
            path,
            bulk_mode: std::sync::atomic::AtomicBool::new(false),
        };
        if is_new {
            store.write_schema_version()?;
        }
        Ok(store)
    }

    fn write_schema_version(&self) -> Result<(), GraphStoreError> {
        let write_txn = self.begin_write_txn()?;
        {
            let mut table = write_txn
                .open_table(GRAPH_SCHEMA)
                .map_err(GraphStoreError::storage)?;
            table
                .insert(GRAPH_SCHEMA_VERSION_KEY, GRAPH_SCHEMA_VERSION)
                .map_err(GraphStoreError::storage)?;
        }
        write_txn.commit().map_err(GraphStoreError::storage)
    }

    /// Run redb's integrity check.
    ///
    /// Returns `Ok(true)` when the database passed all checks, `Ok(false)`
    /// when a problem was detected and auto-repaired, and an error when the
    /// file is unrepairable.  This is an expensive operation — only invoke it
    /// when corruption is suspected (e.g. after an unexpected process exit or
    /// filesystem fault), not on every startup.
    ///
    /// Callers must ensure no read or write transactions are open on this
    /// store at the time of the call — redb returns
    /// `DatabaseError::TransactionInProgress` otherwise.
    pub fn check_integrity(&mut self) -> Result<bool, GraphStoreError> {
        self.db.check_integrity().map_err(GraphStoreError::storage)
    }

    fn map_open_error(path: &Path, error: DatabaseError) -> GraphStoreError {
        match error {
            DatabaseError::DatabaseAlreadyOpen => StorageDaemonMetadata::read_for_graph_path(path)
                .map_or_else(
                    || GraphStoreError::StorageHeld {
                        path: path.to_path_buf(),
                    },
                    |holder| GraphStoreError::StorageHeldByDaemon {
                        path: path.to_path_buf(),
                        pid: holder.pid,
                        started_at_epoch_ms: holder.started_at_epoch_ms,
                        workspace_root: holder.workspace_root,
                    },
                ),
            DatabaseError::RepairAborted
            | DatabaseError::UpgradeRequired(_)
            | DatabaseError::Storage(StorageError::Corrupted(_)) => GraphStoreError::Corrupt {
                path: path.to_path_buf(),
            },
            other => GraphStoreError::storage(other),
        }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Return database file size in bytes for diagnostic logging.
    pub fn file_size_bytes(&self) -> u64 {
        std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0)
    }

    pub fn table_footprints(&self) -> Result<Vec<GraphTableFootprint>, GraphStoreError> {
        let read_txn = self.db.begin_read().map_err(GraphStoreError::storage)?;
        let mut tables = Vec::new();

        macro_rules! record_table {
            ($name:literal, $kind:literal, $open:expr) => {
                match $open {
                    Ok(table) => {
                        let stats = table.stats().map_err(GraphStoreError::storage)?;
                        let entries = table.len().map_err(GraphStoreError::storage)?;
                        tables.push(graph_table_footprint($name, $kind, entries, &stats));
                    }
                    Err(error) if Self::is_missing_table_error(&error) => {}
                    Err(error) => return Err(GraphStoreError::storage(error)),
                }
            };
        }

        record_table!("nodes", "table", read_txn.open_table(NODES));
        record_table!("edges", "table", read_txn.open_table(EDGES));
        record_table!(
            "edges_out",
            "multimap",
            read_txn.open_multimap_table(EDGES_OUT)
        );
        record_table!(
            "edges_in",
            "multimap",
            read_txn.open_multimap_table(EDGES_IN)
        );
        record_table!(
            "edges_by_owner",
            "multimap",
            read_txn.open_multimap_table(EDGES_BY_OWNER)
        );
        record_table!(
            "edge_kind_counts",
            "table",
            read_txn.open_table(EDGE_KIND_COUNTS)
        );
        record_table!("by_file", "multimap", read_txn.open_multimap_table(BY_FILE));
        record_table!("by_repo", "multimap", read_txn.open_multimap_table(BY_REPO));
        record_table!("by_type", "multimap", read_txn.open_multimap_table(BY_TYPE));
        record_table!(
            "by_external_id",
            "multimap",
            read_txn.open_multimap_table(BY_EXTERNAL_ID)
        );
        record_table!(
            "cross_file_candidates",
            "multimap",
            read_txn.open_multimap_table(CROSS_FILE_CANDIDATES)
        );
        record_table!(
            "event_family_index",
            "multimap",
            read_txn.open_multimap_table(EVENT_FAMILY_INDEX)
        );
        record_table!(
            "route_key_index",
            "multimap",
            read_txn.open_multimap_table(ROUTE_KEY_INDEX)
        );
        record_table!(
            "shared_symbol_name_index",
            "multimap",
            read_txn.open_multimap_table(SHARED_SYMBOL_NAME_INDEX)
        );
        record_table!("repo_ids", "table", read_txn.open_table(REPO_IDS));
        record_table!("repos", "table", read_txn.open_table(REPOS));
        record_table!("file_path_ids", "table", read_txn.open_table(FILE_PATH_IDS));
        record_table!("file_paths", "table", read_txn.open_table(FILE_PATHS));
        record_table!("signature_ids", "table", read_txn.open_table(SIGNATURE_IDS));
        record_table!("signatures", "table", read_txn.open_table(SIGNATURES));
        record_table!(
            "string_metadata",
            "table",
            read_txn.open_table(STRING_METADATA)
        );
        record_table!("graph_schema", "table", read_txn.open_table(GRAPH_SCHEMA));

        tables.sort_by(|left, right| {
            right
                .stored_bytes
                .cmp(&left.stored_bytes)
                .then_with(|| left.name.cmp(&right.name))
        });
        Ok(tables)
    }

    /// Compact the database to reclaim dead space from delete/rewrite cycles.
    /// Must be called when no transactions are open.
    pub fn compact(&mut self) -> Result<bool, GraphStoreError> {
        self.db.compact().map_err(GraphStoreError::storage)
    }

    /// Enable or disable bulk mode. When enabled, write transactions use
    /// `Durability::None` to skip fsync on each commit.  Flipping the flag
    /// off does **not** by itself flush the unsynced pages; a follow-up
    /// `Durability::Immediate` commit is required.  Callers should prefer
    /// [`crate::indexer::BulkModeGuard`] over calling this directly — the
    /// guard's `Drop` handles the flush via [`Self::commit_durable_marker`].
    pub fn set_bulk_mode(&self, enabled: bool) {
        self.bulk_mode
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
    }

    /// Commit an empty write transaction at whatever durability level the
    /// store is currently configured for (`Immediate` when bulk mode is off,
    /// `None` otherwise).  Used by [`crate::indexer::BulkModeGuard::drop`]
    /// to fsync pages accumulated under `Durability::None` before the guard
    /// goes out of scope.
    pub(crate) fn commit_durable_marker(&self) -> Result<(), GraphStoreError> {
        let write_txn = self.begin_write_txn()?;
        write_txn.commit().map_err(GraphStoreError::storage)
    }

    /// Whether bulk mode is currently enabled.
    pub fn is_bulk_mode(&self) -> bool {
        self.bulk_mode.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Count edges where source and target nodes belong to different repos,
    /// OR where the edge itself is classified as `EdgeKind::CrossRepoDepends`.
    ///
    /// Replaces the earlier nested "for every `NodeKind`, for every node, for
    /// every outgoing edge" traversal (~270K read transactions on the full
    /// monorepo) with a single pass over the EDGES table and a tiny in-memory
    /// `node_id → repo_id` cache. One read transaction; one EDGES scan.
    pub fn count_cross_repo_edges(&self) -> Result<u64, GraphStoreError> {
        let read_txn = self.db.begin_read().map_err(GraphStoreError::storage)?;
        let nodes = match read_txn.open_table(NODES) {
            Ok(nodes) => nodes,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(0),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let edges = match read_txn.open_table(EDGES) {
            Ok(edges) => edges,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(0),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let mut repo_of_node: rustc_hash::FxHashMap<NodeIdBytes, StringId> =
            rustc_hash::FxHashMap::default();
        let repo_id_of = |id: NodeId,
                          nodes: &redb::ReadOnlyTable<NodeIdBytes, &'static [u8]>,
                          cache: &mut rustc_hash::FxHashMap<NodeIdBytes, StringId>|
         -> Result<Option<StringId>, GraphStoreError> {
            let key = id.as_bytes();
            if let Some(value) = cache.get(&key) {
                return Ok(Some(*value));
            }
            let Some(raw) = nodes.get(key).map_err(GraphStoreError::storage)? else {
                return Ok(None);
            };
            let stored = Self::decode_stored_node(raw.value())?;
            cache.insert(key, stored.repo_id);
            Ok(Some(stored.repo_id))
        };
        let mut count: u64 = 0;
        let iter = edges.iter().map_err(GraphStoreError::storage)?;
        for entry in iter {
            let (_edge_id, raw) = entry.map_err(GraphStoreError::storage)?;
            let edge = Self::decode_edge(raw.value())?;
            if edge.kind == EdgeKind::CrossRepoDepends {
                count = count.saturating_add(1);
                continue;
            }
            let Some(source_repo) = repo_id_of(edge.source, &nodes, &mut repo_of_node)? else {
                continue;
            };
            let Some(target_repo) = repo_id_of(edge.target, &nodes, &mut repo_of_node)? else {
                continue;
            };
            if source_repo != target_repo {
                count = count.saturating_add(1);
            }
        }
        Ok(count)
    }

    /// Count edges where both the source and target nodes belong to real,
    /// non-virtual repos (neither endpoint's repo equals [`VIRTUAL_NODE_REPO`]).
    ///
    /// This is the "true cross-repo code link" metric: it grows when cross-repo
    /// import/call/dependency structure changes, not when git-history indexing
    /// discovers more commit authors.
    ///
    /// Together with [`Self::count_history_ownership_edges`] and
    /// [`Self::count_virtual_other_cross_repo_edges`] this must equal
    /// [`Self::count_cross_repo_edges`].
    pub fn count_true_cross_repo_edges(&self) -> Result<usize, GraphStoreError> {
        let read_txn = self.db.begin_read().map_err(GraphStoreError::storage)?;
        let nodes = match read_txn.open_table(NODES) {
            Ok(nodes) => nodes,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(0),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let edges = match read_txn.open_table(EDGES) {
            Ok(edges) => edges,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(0),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let repos = match read_txn.open_table(REPOS) {
            Ok(repos) => repos,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(0),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        // Cache: node-id bytes → (repo_string, is_virtual, kind)
        let mut node_cache: rustc_hash::FxHashMap<NodeIdBytes, (String, bool, NodeKind)> =
            rustc_hash::FxHashMap::default();
        let mut repo_str_cache: rustc_hash::FxHashMap<StringId, String> =
            rustc_hash::FxHashMap::default();
        let mut count: usize = 0;
        let iter = edges.iter().map_err(GraphStoreError::storage)?;
        for entry in iter {
            let (_edge_id, raw) = entry.map_err(GraphStoreError::storage)?;
            let edge = Self::decode_edge(raw.value())?;
            let Some((src_repo, _src_virt, _src_kind)) = Self::resolve_node_for_split(
                edge.source.as_bytes(),
                &nodes,
                &repos,
                &mut node_cache,
                &mut repo_str_cache,
            )?
            else {
                continue;
            };
            let Some((tgt_repo, _tgt_virt, _tgt_kind)) = Self::resolve_node_for_split(
                edge.target.as_bytes(),
                &nodes,
                &repos,
                &mut node_cache,
                &mut repo_str_cache,
            )?
            else {
                continue;
            };
            if src_repo == tgt_repo {
                continue;
            }
            // Both repos are non-virtual → true cross-repo code link.
            if src_repo != VIRTUAL_NODE_REPO && tgt_repo != VIRTUAL_NODE_REPO {
                count = count.saturating_add(1);
            }
        }
        Ok(count)
    }

    /// Count edges whose target is a virtual author-ownership node
    /// (`target.repo == VIRTUAL_NODE_REPO && target.kind == NodeKind::Author`).
    ///
    /// This metric reflects git-history coverage: it grows when the
    /// git-history extraction pass ingests more commits, not when cross-repo
    /// code structure changes.
    ///
    /// Together with [`Self::count_true_cross_repo_edges`] and
    /// [`Self::count_virtual_other_cross_repo_edges`] this must equal
    /// [`Self::count_cross_repo_edges`].
    pub fn count_history_ownership_edges(&self) -> Result<usize, GraphStoreError> {
        self.count_virtual_target_edges_by_kind(NodeKind::Author)
    }

    /// Count edges whose target is a virtual non-author node
    /// (`target.repo == VIRTUAL_NODE_REPO && target.kind != NodeKind::Author`).
    ///
    /// This covers `SharedSymbol`, `Route`, and other virtual stub targets.
    ///
    /// Together with [`Self::count_true_cross_repo_edges`] and
    /// [`Self::count_history_ownership_edges`] this must equal
    /// [`Self::count_cross_repo_edges`].
    pub fn count_virtual_other_cross_repo_edges(&self) -> Result<usize, GraphStoreError> {
        let read_txn = self.db.begin_read().map_err(GraphStoreError::storage)?;
        let nodes = match read_txn.open_table(NODES) {
            Ok(nodes) => nodes,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(0),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let edges = match read_txn.open_table(EDGES) {
            Ok(edges) => edges,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(0),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let repos = match read_txn.open_table(REPOS) {
            Ok(repos) => repos,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(0),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let mut node_cache: rustc_hash::FxHashMap<NodeIdBytes, (String, bool, NodeKind)> =
            rustc_hash::FxHashMap::default();
        let mut repo_str_cache: rustc_hash::FxHashMap<StringId, String> =
            rustc_hash::FxHashMap::default();
        let mut count: usize = 0;
        let iter = edges.iter().map_err(GraphStoreError::storage)?;
        for entry in iter {
            let (_edge_id, raw) = entry.map_err(GraphStoreError::storage)?;
            let edge = Self::decode_edge(raw.value())?;
            let Some((src_repo, _src_virt, _src_kind)) = Self::resolve_node_for_split(
                edge.source.as_bytes(),
                &nodes,
                &repos,
                &mut node_cache,
                &mut repo_str_cache,
            )?
            else {
                continue;
            };
            let Some((tgt_repo, _tgt_virt, tgt_kind)) = Self::resolve_node_for_split(
                edge.target.as_bytes(),
                &nodes,
                &repos,
                &mut node_cache,
                &mut repo_str_cache,
            )?
            else {
                continue;
            };
            if src_repo != tgt_repo && tgt_repo == VIRTUAL_NODE_REPO && tgt_kind != NodeKind::Author
            {
                count = count.saturating_add(1);
            }
        }
        Ok(count)
    }

    /// Shared single-pass helper: resolve a node's `(repo_string, is_virtual,
    /// kind)` from the NODES + REPOS tables, with a two-level cache.
    fn resolve_node_for_split(
        id_bytes: NodeIdBytes,
        nodes: &redb::ReadOnlyTable<NodeIdBytes, &'static [u8]>,
        repos: &redb::ReadOnlyTable<u32, &'static str>,
        node_cache: &mut rustc_hash::FxHashMap<NodeIdBytes, (String, bool, NodeKind)>,
        repo_str_cache: &mut rustc_hash::FxHashMap<StringId, String>,
    ) -> Result<Option<(String, bool, NodeKind)>, GraphStoreError> {
        if let Some(cached) = node_cache.get(&id_bytes) {
            return Ok(Some(cached.clone()));
        }
        let Some(raw) = nodes.get(id_bytes).map_err(GraphStoreError::storage)? else {
            return Ok(None);
        };
        let stored = Self::decode_stored_node(raw.value())?;
        let repo_str = if let Some(s) = repo_str_cache.get(&stored.repo_id) {
            s.clone()
        } else {
            let s = repos
                .get(stored.repo_id)
                .map_err(GraphStoreError::storage)?
                .ok_or_else(|| {
                    GraphStoreError::storage(format!("missing repo id {}", stored.repo_id))
                })?
                .value()
                .to_owned();
            repo_str_cache.insert(stored.repo_id, s.clone());
            s
        };
        let entry = (repo_str, stored.is_virtual, stored.kind);
        node_cache.insert(id_bytes, entry.clone());
        Ok(Some(entry))
    }

    /// Internal helper used by [`Self::count_history_ownership_edges`].
    /// Counts edges with `target.repo == VIRTUAL_NODE_REPO && target.kind == kind`,
    /// skipping same-repo pairs.
    fn count_virtual_target_edges_by_kind(&self, kind: NodeKind) -> Result<usize, GraphStoreError> {
        let read_txn = self.db.begin_read().map_err(GraphStoreError::storage)?;
        let nodes = match read_txn.open_table(NODES) {
            Ok(nodes) => nodes,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(0),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let edges = match read_txn.open_table(EDGES) {
            Ok(edges) => edges,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(0),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let repos = match read_txn.open_table(REPOS) {
            Ok(repos) => repos,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(0),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let mut node_cache: rustc_hash::FxHashMap<NodeIdBytes, (String, bool, NodeKind)> =
            rustc_hash::FxHashMap::default();
        let mut repo_str_cache: rustc_hash::FxHashMap<StringId, String> =
            rustc_hash::FxHashMap::default();
        let mut count: usize = 0;
        let iter = edges.iter().map_err(GraphStoreError::storage)?;
        for entry in iter {
            let (_edge_id, raw) = entry.map_err(GraphStoreError::storage)?;
            let edge = Self::decode_edge(raw.value())?;
            let Some((src_repo, _src_virt, _src_kind)) = Self::resolve_node_for_split(
                edge.source.as_bytes(),
                &nodes,
                &repos,
                &mut node_cache,
                &mut repo_str_cache,
            )?
            else {
                continue;
            };
            let Some((tgt_repo, _tgt_virt, tgt_kind)) = Self::resolve_node_for_split(
                edge.target.as_bytes(),
                &nodes,
                &repos,
                &mut node_cache,
                &mut repo_str_cache,
            )?
            else {
                continue;
            };
            if src_repo != tgt_repo && tgt_repo == VIRTUAL_NODE_REPO && tgt_kind == kind {
                count = count.saturating_add(1);
            }
        }
        Ok(count)
    }

    pub(crate) fn begin_write_txn(&self) -> Result<redb::WriteTransaction, GraphStoreError> {
        let mut write_txn = self.db.begin_write().map_err(GraphStoreError::storage)?;
        // The graph store is a derived projection of source content. If a crash
        // loses the last committed batch, the coordinator can rebuild it by
        // reindexing files. In bulk mode we skip fsync entirely for speed; in
        // normal mode we use Immediate durability (one fsync per commit).
        let durability = if self.bulk_mode.load(std::sync::atomic::Ordering::Relaxed) {
            Durability::None
        } else {
            Durability::Immediate
        };
        write_txn
            .set_durability(durability)
            .map_err(GraphStoreError::storage)?;
        Ok(write_txn)
    }

    pub(crate) fn begin_read_txn(&self) -> Result<redb::ReadTransaction, GraphStoreError> {
        self.db.begin_read().map_err(GraphStoreError::storage)
    }

    pub(crate) fn get_node_in_read_txn(
        read_txn: &redb::ReadTransaction,
        id: NodeId,
    ) -> Result<Option<NodeData>, GraphStoreError> {
        let nodes = match read_txn.open_table(NODES) {
            Ok(nodes) => nodes,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(None),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let node = nodes
            .get(id.as_bytes())
            .map_err(GraphStoreError::storage)?
            .map(|raw| Self::decode_stored_node(raw.value()))
            .transpose()?;
        node.map_or(Ok(None), |stored| {
            Self::rehydrate_node_in_read_txn(read_txn, stored).map(Some)
        })
    }

    pub(crate) fn get_outgoing_in_read_txn(
        read_txn: &redb::ReadTransaction,
        source: NodeId,
    ) -> Result<Vec<EdgeData>, GraphStoreError> {
        let edges = match read_txn.open_multimap_table(EDGES_OUT) {
            Ok(edges) => edges,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(Vec::new()),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let values = edges
            .get(source.as_bytes())
            .map_err(GraphStoreError::storage)?;
        let mut result = Vec::new();
        for value in values {
            let raw = value.map_err(GraphStoreError::storage)?;
            if let Some(edge) = Self::stored_edge_in_read_txn(read_txn, raw.value())? {
                result.push(edge);
            }
        }
        Ok(result)
    }

    pub(crate) fn get_incoming_in_read_txn(
        read_txn: &redb::ReadTransaction,
        target: NodeId,
    ) -> Result<Vec<EdgeData>, GraphStoreError> {
        let edges = match read_txn.open_multimap_table(EDGES_IN) {
            Ok(edges) => edges,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(Vec::new()),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let values = edges
            .get(target.as_bytes())
            .map_err(GraphStoreError::storage)?;
        let mut result = Vec::new();
        for value in values {
            let raw = value.map_err(GraphStoreError::storage)?;
            if let Some(edge) = Self::stored_edge_in_read_txn(read_txn, raw.value())? {
                result.push(edge);
            }
        }
        Ok(result)
    }

    pub(crate) fn edges_by_owner_in_read_txn(
        read_txn: &redb::ReadTransaction,
        owner_file: NodeId,
    ) -> Result<Vec<EdgeData>, GraphStoreError> {
        let edges = match read_txn.open_multimap_table(EDGES_BY_OWNER) {
            Ok(edges) => edges,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(Vec::new()),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let values = edges
            .get(owner_file.as_bytes())
            .map_err(GraphStoreError::storage)?;
        let mut result = Vec::new();
        for value in values {
            let raw = value.map_err(GraphStoreError::storage)?;
            if let Some(edge) = Self::stored_edge_in_read_txn(read_txn, raw.value())? {
                result.push(edge);
            }
        }
        Ok(result)
    }

    pub(crate) fn nodes_by_file_in_read_txn(
        read_txn: &redb::ReadTransaction,
        repo: &str,
        file_path: &str,
    ) -> Result<Vec<NodeData>, GraphStoreError> {
        let Some(repo_id) = Self::lookup_repo_id(read_txn, repo)? else {
            return Ok(Vec::new());
        };
        let Some(file_path_id) = Self::lookup_file_path_id(read_txn, file_path)? else {
            return Ok(Vec::new());
        };
        let by_file = match read_txn.open_multimap_table(BY_FILE) {
            Ok(by_file) => by_file,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(Vec::new()),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let file_key = Self::file_index_key(repo_id, file_path_id);
        let values = by_file
            .get(file_key.as_slice())
            .map_err(GraphStoreError::storage)?;
        let mut ids = Vec::new();
        for value in values {
            let raw = value.map_err(GraphStoreError::storage)?;
            ids.push(raw.value());
        }
        Self::collect_nodes_for_ids(read_txn, ids)
    }

    pub(crate) fn nodes_by_candidate_keys_in_read_txn(
        read_txn: &redb::ReadTransaction,
        candidate_keys: &[String],
    ) -> Result<Vec<NodeData>, GraphStoreError> {
        if candidate_keys.is_empty() {
            return Ok(Vec::new());
        }
        let candidates = match read_txn.open_multimap_table(CROSS_FILE_CANDIDATES) {
            Ok(candidates) => candidates,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(Vec::new()),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let mut ids = Vec::new();
        let mut seen = FxHashSet::default();
        for candidate_key in candidate_keys {
            let values = candidates
                .get(candidate_key.as_str())
                .map_err(GraphStoreError::storage)?;
            for value in values {
                let raw = value.map_err(GraphStoreError::storage)?;
                if seen.insert(raw.value()) {
                    ids.push(raw.value());
                }
            }
        }
        Self::collect_nodes_for_ids(read_txn, ids)
    }

    pub(crate) fn with_write_txn<T>(
        &self,
        action: impl FnOnce(&redb::WriteTransaction) -> Result<T, GraphStoreError>,
    ) -> Result<T, GraphStoreError> {
        let write_txn = self.begin_write_txn()?;
        let result = action(&write_txn)?;
        write_txn.commit().map_err(GraphStoreError::storage)?;
        Ok(result)
    }

    fn candidate_keys<'a>(name: &'a str, qualified_name: Option<&'a str>) -> [Option<&'a str>; 2] {
        [Some(name), qualified_name]
    }

    fn candidate_keys_for_node(node: &StoredNode) -> [Option<&str>; 2] {
        Self::candidate_keys(node.name.as_str(), node.qualified_name.as_deref())
    }

    fn file_index_key(repo_id: StringId, file_path_id: StringId) -> [u8; 8] {
        let mut key = [0_u8; 8];
        key[..4].copy_from_slice(&repo_id.to_be_bytes());
        key[4..].copy_from_slice(&file_path_id.to_be_bytes());
        key
    }

    fn external_id_key(kind: NodeKind, external_id: &str) -> String {
        format!("{}\0{external_id}", kind.as_u8())
    }

    /// Extract the normalised event-family name from a stored node's
    /// `external_id` or `qualified_name`, if the node is a virtual event-like
    /// node.
    ///
    /// The convention is `__<kind>__<protocol>__<event_name>` (e.g.
    /// `__topic__kafka__order.created`).  The normalised name is the suffix
    /// after the last `__`, lowercased.  Returns `None` if the node is not
    /// virtual, not an event-like kind, or has no extractable name.
    fn event_family_name_for_stored_node(node: &StoredNode) -> Option<String> {
        if !node.is_virtual {
            return None;
        }
        let is_event_kind = matches!(
            node.kind,
            NodeKind::Topic
                | NodeKind::Queue
                | NodeKind::Subject
                | NodeKind::Stream
                | NodeKind::Event
        );
        if !is_event_kind {
            return None;
        }
        let raw = node
            .external_id
            .as_deref()
            .or(node.qualified_name.as_deref())?;
        if raw.is_empty() {
            return None;
        }
        let mut normalized = raw
            .rsplit_once("__")
            .map_or(raw, |(_, suffix)| suffix)
            .to_owned();
        if normalized.is_empty() {
            return None;
        }
        normalized.make_ascii_lowercase();
        Some(normalized)
    }

    /// Extract the lowercase short-name key for a virtual `SharedSymbol`
    /// or `Type` stub.
    ///
    /// Stubs are emitted by the cross-package augmenters with conventions
    /// like `__shared__<pkg>__<Name>`, `__guard__<src>__<Name>`, or
    /// `__hook__<pkg>::<name>`. The key is the trailing segment after the
    /// last `::` (preferred for hook stubs) or `__` (used by shared and
    /// guard stubs), lowercased.
    ///
    /// Returns `None` for non-virtual nodes, non-`SharedSymbol` /
    /// non-`Type` nodes, or nodes with no extractable trailing segment.
    fn shared_symbol_name_for_stored_node(node: &StoredNode) -> Option<String> {
        if !node.is_virtual {
            return None;
        }
        if !matches!(node.kind, NodeKind::SharedSymbol | NodeKind::Type) {
            return None;
        }
        let raw = node
            .external_id
            .as_deref()
            .or(node.qualified_name.as_deref())?;
        if raw.is_empty() {
            return None;
        }
        let after_double_colon = raw.rsplit_once("::").map_or(raw, |(_, suffix)| suffix);
        let trailing = after_double_colon
            .rsplit_once("__")
            .map_or(after_double_colon, |(_, suffix)| suffix);
        if trailing.is_empty() {
            return None;
        }
        let mut normalized = trailing.to_owned();
        normalized.make_ascii_lowercase();
        Some(normalized)
    }

    fn shared_symbol_name_for_node_data(node: &NodeData) -> Option<String> {
        if !node.is_virtual {
            return None;
        }
        if !matches!(node.kind, NodeKind::SharedSymbol | NodeKind::Type) {
            return None;
        }
        let raw = node
            .external_id
            .as_deref()
            .or(node.qualified_name.as_deref())?;
        if raw.is_empty() {
            return None;
        }
        let after_double_colon = raw.rsplit_once("::").map_or(raw, |(_, suffix)| suffix);
        let trailing = after_double_colon
            .rsplit_once("__")
            .map_or(after_double_colon, |(_, suffix)| suffix);
        if trailing.is_empty() {
            return None;
        }
        let mut normalized = trailing.to_owned();
        normalized.make_ascii_lowercase();
        Some(normalized)
    }

    fn nodes_by_shared_symbol_name_scan(
        &self,
        normalized_short_name: &str,
    ) -> Result<Vec<NodeData>, GraphStoreError> {
        let mut peers = Vec::new();
        for kind in [NodeKind::SharedSymbol, NodeKind::Type] {
            for node in self.nodes_by_type(kind)? {
                if Self::shared_symbol_name_for_node_data(&node).as_deref()
                    == Some(normalized_short_name)
                {
                    peers.push(node);
                }
            }
        }
        Ok(peers)
    }

    /// Extract the canonical route key `"{METHOD}__{path}"` from a stored
    /// `Route` node's `external_id`.
    ///
    /// Recognises the `__route__<method>__<path>` and
    /// `__api_call__<method>__<path>` conventions.  Returns `None` if the
    /// node is not a virtual `Route` node or the `external_id` doesn't follow
    /// either convention.
    fn canonical_route_key_for_stored_node(node: &StoredNode) -> Option<String> {
        if !node.is_virtual || node.kind != NodeKind::Route {
            return None;
        }
        let ext = node.external_id.as_deref()?;
        let suffix = ext
            .strip_prefix("__route__")
            .or_else(|| ext.strip_prefix("__api_call__"))?;
        let (method, path) = suffix.split_once("__")?;
        let method = if method.eq_ignore_ascii_case("FETCH") {
            "GET".to_owned()
        } else {
            method.to_ascii_uppercase()
        };
        let path = if path.starts_with('/') {
            path.to_owned()
        } else {
            format!("/{path}")
        };
        Some(format!("{method}__{path}"))
    }

    fn encode_stored_node(node: &StoredNode) -> Vec<u8> {
        bitcode::encode(node)
    }

    fn decode_stored_node(bytes: &[u8]) -> Result<StoredNode, GraphStoreError> {
        bitcode::decode(bytes).map_err(GraphStoreError::from)
    }

    fn encode_edge(edge: &EdgeData) -> Vec<u8> {
        bitcode::encode(&StoredEdge {
            source: edge.source,
            target: edge.target,
            kind: edge.kind,
            owner_file: edge.owner_file,
            metadata: StoredEdgeMetadata::from_public(&edge.metadata),
        })
    }

    fn decode_edge(bytes: &[u8]) -> Result<EdgeData, GraphStoreError> {
        let stored: StoredEdge = bitcode::decode(bytes).map_err(GraphStoreError::from)?;
        // `is_cross_file` is derivable from the source and owner_file nodes.
        // It is reconstructed as `false` here; callers that need the precise
        // value can compare source node file_path with owner_file.
        Ok(EdgeData {
            source: stored.source,
            target: stored.target,
            kind: stored.kind,
            owner_file: stored.owner_file,
            metadata: stored.metadata.into_public(),
            is_cross_file: false,
        })
    }

    fn increment_edge_kind_count(
        counts: &mut redb::Table<'_, u8, u64>,
        kind: EdgeKind,
    ) -> Result<(), GraphStoreError> {
        let key = kind.as_u8();
        let next = counts
            .get(key)
            .map_err(GraphStoreError::storage)?
            .map_or(1, |current| current.value().saturating_add(1));
        counts.insert(key, next).map_err(GraphStoreError::storage)?;
        Ok(())
    }

    fn decrement_edge_kind_count(
        counts: &mut redb::Table<'_, u8, u64>,
        kind: EdgeKind,
    ) -> Result<(), GraphStoreError> {
        let key = kind.as_u8();
        let Some(current) = counts
            .get(key)
            .map_err(GraphStoreError::storage)?
            .map(|current| current.value())
        else {
            return Ok(());
        };
        if current <= 1 {
            let _ = counts.remove(key).map_err(GraphStoreError::storage)?;
        } else {
            counts
                .insert(key, current - 1)
                .map_err(GraphStoreError::storage)?;
        }
        Ok(())
    }

    fn edge_id(edge: &EdgeData) -> EdgeIdBytes {
        // Pack identity fields into a fixed stack buffer (no heap allocation).
        // `is_cross_file` is derived and not included in the identity hash.
        let mut buf = [0_u8; 16 + 16 + 1 + 16]; // source + target + kind + owner_file
        buf[..16].copy_from_slice(&edge.source.as_bytes());
        buf[16..32].copy_from_slice(&edge.target.as_bytes());
        buf[32] = edge.kind.as_u8();
        buf[33..49].copy_from_slice(&edge.owner_file.as_bytes());
        let mut id = [0_u8; 16];
        id.copy_from_slice(&blake3::hash(&buf).as_bytes()[..16]);
        id
    }

    fn same_edge_identity(left: &EdgeData, right: &EdgeData) -> bool {
        left.source == right.source
            && left.target == right.target
            && left.kind == right.kind
            && left.owner_file == right.owner_file
    }

    fn next_string_id(
        write_txn: &redb::WriteTransaction,
        counter_key: u8,
    ) -> Result<StringId, GraphStoreError> {
        let mut metadata = write_txn
            .open_table(STRING_METADATA)
            .map_err(GraphStoreError::storage)?;
        let next_id = metadata
            .get(counter_key)
            .map_err(GraphStoreError::storage)?
            .map_or(0, |value| value.value());
        let following = next_id
            .checked_add(1)
            .ok_or_else(|| GraphStoreError::storage("string id counter overflow"))?;
        metadata
            .insert(counter_key, following)
            .map_err(GraphStoreError::storage)?;
        Ok(next_id)
    }

    fn intern_repo_id(
        write_txn: &redb::WriteTransaction,
        repo: &str,
    ) -> Result<StringId, GraphStoreError> {
        {
            let repo_ids = write_txn
                .open_table(REPO_IDS)
                .map_err(GraphStoreError::storage)?;
            if let Some(existing) = repo_ids.get(repo).map_err(GraphStoreError::storage)? {
                return Ok(existing.value());
            }
        }

        let repo_id = Self::next_string_id(write_txn, NEXT_REPO_ID_KEY)?;

        {
            let mut repo_ids = write_txn
                .open_table(REPO_IDS)
                .map_err(GraphStoreError::storage)?;
            if let Some(existing) = repo_ids.get(repo).map_err(GraphStoreError::storage)? {
                return Ok(existing.value());
            }
            repo_ids
                .insert(repo, repo_id)
                .map_err(GraphStoreError::storage)?;
        }

        let mut repos = write_txn
            .open_table(REPOS)
            .map_err(GraphStoreError::storage)?;
        repos
            .insert(repo_id, repo)
            .map_err(GraphStoreError::storage)?;
        Ok(repo_id)
    }

    fn intern_file_path_id(
        write_txn: &redb::WriteTransaction,
        file_path: &str,
    ) -> Result<StringId, GraphStoreError> {
        {
            let path_ids = write_txn
                .open_table(FILE_PATH_IDS)
                .map_err(GraphStoreError::storage)?;
            if let Some(existing) = path_ids.get(file_path).map_err(GraphStoreError::storage)? {
                return Ok(existing.value());
            }
        }

        let file_path_id = Self::next_string_id(write_txn, NEXT_FILE_PATH_ID_KEY)?;

        {
            let mut path_ids = write_txn
                .open_table(FILE_PATH_IDS)
                .map_err(GraphStoreError::storage)?;
            if let Some(existing) = path_ids.get(file_path).map_err(GraphStoreError::storage)? {
                return Ok(existing.value());
            }
            path_ids
                .insert(file_path, file_path_id)
                .map_err(GraphStoreError::storage)?;
        }

        let mut file_paths = write_txn
            .open_table(FILE_PATHS)
            .map_err(GraphStoreError::storage)?;
        file_paths
            .insert(file_path_id, file_path)
            .map_err(GraphStoreError::storage)?;
        Ok(file_path_id)
    }

    /// Intern a signature string and return its stable numeric ID.
    ///
    /// Uses the same double-check pattern as [`Self::intern_repo_id`] to
    /// survive concurrent writers without a race between the read check and
    /// the write: check under a read handle, allocate the ID, re-check under
    /// a write handle before inserting.
    fn intern_signature_id(
        write_txn: &redb::WriteTransaction,
        signature: &str,
    ) -> Result<u32, GraphStoreError> {
        {
            let sig_ids = write_txn
                .open_table(SIGNATURE_IDS)
                .map_err(GraphStoreError::storage)?;
            if let Some(existing) = sig_ids.get(signature).map_err(GraphStoreError::storage)? {
                return Ok(existing.value());
            }
        }

        let sig_id = Self::next_string_id(write_txn, NEXT_SIGNATURE_ID_KEY)?;

        {
            let mut sig_ids = write_txn
                .open_table(SIGNATURE_IDS)
                .map_err(GraphStoreError::storage)?;
            if let Some(existing) = sig_ids.get(signature).map_err(GraphStoreError::storage)? {
                return Ok(existing.value());
            }
            sig_ids
                .insert(signature, sig_id)
                .map_err(GraphStoreError::storage)?;
        }

        let mut sigs = write_txn
            .open_table(SIGNATURES)
            .map_err(GraphStoreError::storage)?;
        sigs.insert(sig_id, signature)
            .map_err(GraphStoreError::storage)?;
        Ok(sig_id)
    }

    /// Look up a signature string by its interned ID in a read transaction.
    fn lookup_signature_in_read_txn(
        read_txn: &redb::ReadTransaction,
        sig_id: u32,
    ) -> Result<Option<String>, GraphStoreError> {
        let sigs = match read_txn.open_table(SIGNATURES) {
            Ok(sigs) => sigs,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(None),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        Ok(sigs
            .get(sig_id)
            .map_err(GraphStoreError::storage)?
            .map(|value| value.value().to_owned()))
    }

    /// Look up a signature string by its interned ID in a write transaction.
    fn lookup_signature_in_write_txn(
        write_txn: &redb::WriteTransaction,
        sig_id: u32,
    ) -> Result<Option<String>, GraphStoreError> {
        let sigs = match write_txn.open_table(SIGNATURES) {
            Ok(sigs) => sigs,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(None),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        Ok(sigs
            .get(sig_id)
            .map_err(GraphStoreError::storage)?
            .map(|value| value.value().to_owned()))
    }

    fn lookup_repo_id(
        read_txn: &redb::ReadTransaction,
        repo: &str,
    ) -> Result<Option<StringId>, GraphStoreError> {
        let repo_ids = match read_txn.open_table(REPO_IDS) {
            Ok(repo_ids) => repo_ids,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(None),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        Ok(repo_ids
            .get(repo)
            .map_err(GraphStoreError::storage)?
            .map(|value| value.value()))
    }

    fn lookup_file_path_id(
        read_txn: &redb::ReadTransaction,
        file_path: &str,
    ) -> Result<Option<StringId>, GraphStoreError> {
        let path_ids = match read_txn.open_table(FILE_PATH_IDS) {
            Ok(path_ids) => path_ids,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(None),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        Ok(path_ids
            .get(file_path)
            .map_err(GraphStoreError::storage)?
            .map(|value| value.value()))
    }

    fn resolve_repo_path_in_write_txn(
        write_txn: &redb::WriteTransaction,
        repo_id: StringId,
        file_path_id: StringId,
    ) -> Result<(String, String), GraphStoreError> {
        let repos = write_txn
            .open_table(REPOS)
            .map_err(GraphStoreError::storage)?;
        let repo = repos
            .get(repo_id)
            .map_err(GraphStoreError::storage)?
            .ok_or_else(|| GraphStoreError::storage(format!("missing repo id {repo_id}")))?
            .value()
            .to_owned();

        let file_paths = write_txn
            .open_table(FILE_PATHS)
            .map_err(GraphStoreError::storage)?;
        let file_path = file_paths
            .get(file_path_id)
            .map_err(GraphStoreError::storage)?
            .ok_or_else(|| {
                GraphStoreError::storage(format!("missing file path id {file_path_id}"))
            })?
            .value()
            .to_owned();

        Ok((repo, file_path))
    }

    fn store_node_in_txn(
        write_txn: &redb::WriteTransaction,
        node: &NodeData,
    ) -> Result<StoredNode, GraphStoreError> {
        let canonical_node = Self::canonicalize_node(node);
        let repo_id = Self::intern_repo_id(write_txn, &canonical_node.repo)?;
        let file_path_id = Self::intern_file_path_id(write_txn, &canonical_node.file_path)?;
        let signature_id = canonical_node
            .signature
            .as_deref()
            .map(|sig| Self::intern_signature_id(write_txn, sig))
            .transpose()?;

        Ok(StoredNode {
            id: canonical_node.id,
            kind: canonical_node.kind,
            repo_id,
            file_path_id,
            name: canonical_node.name,
            qualified_name: canonical_node.qualified_name,
            external_id: canonical_node.external_id,
            signature_id,
            visibility: canonical_node.visibility,
            span: canonical_node.span,
            is_virtual: canonical_node.is_virtual,
        })
    }

    fn canonicalize_node(node: &NodeData) -> NodeData {
        if !Self::uses_shared_virtual_payload(node) {
            return node.clone();
        }

        let stable_name = node
            .external_id
            .clone()
            .or_else(|| node.qualified_name.clone())
            .unwrap_or_else(|| node.name.clone());
        let stable_path = node
            .qualified_name
            .clone()
            .or_else(|| node.external_id.clone())
            .unwrap_or_else(|| stable_name.clone());

        NodeData {
            id: node.id,
            kind: node.kind,
            repo: "__virtual__".to_owned(),
            file_path: stable_path,
            name: stable_name,
            qualified_name: node.qualified_name.clone(),
            external_id: node.external_id.clone(),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        }
    }

    fn uses_shared_virtual_payload(node: &NodeData) -> bool {
        node.is_virtual
            && matches!(
                node.kind,
                NodeKind::Route
                    | NodeKind::Topic
                    | NodeKind::Queue
                    | NodeKind::Subject
                    | NodeKind::Stream
                    | NodeKind::Event
                    | NodeKind::SharedSymbol
                    | NodeKind::Repo
                    | NodeKind::Service
            )
    }

    /// Rehydrate a stored node into a [`NodeData`] using a read transaction.
    ///
    /// For bulk paths prefer [`Self::rehydrate_node_with_cache`] which threads
    /// per-transaction string caches to avoid repeated interning-table lookups.
    fn rehydrate_node_in_read_txn(
        read_txn: &redb::ReadTransaction,
        node: StoredNode,
    ) -> Result<NodeData, GraphStoreError> {
        let mut repo_cache: rustc_hash::FxHashMap<u32, String> = rustc_hash::FxHashMap::default();
        let mut file_path_cache: rustc_hash::FxHashMap<u32, String> =
            rustc_hash::FxHashMap::default();
        let mut sig_cache: rustc_hash::FxHashMap<u32, String> = rustc_hash::FxHashMap::default();
        Self::rehydrate_node_with_cache(
            read_txn,
            node,
            &mut repo_cache,
            &mut file_path_cache,
            &mut sig_cache,
        )
    }

    /// Core rehydration with per-transaction caches (M5).
    ///
    /// Callers that process many nodes in a single transaction should create
    /// the three cache maps once, then pass them through the loop so each
    /// interned ID is resolved at most once per transaction.
    fn rehydrate_node_with_cache(
        read_txn: &redb::ReadTransaction,
        node: StoredNode,
        repo_cache: &mut rustc_hash::FxHashMap<u32, String>,
        file_path_cache: &mut rustc_hash::FxHashMap<u32, String>,
        sig_cache: &mut rustc_hash::FxHashMap<u32, String>,
    ) -> Result<NodeData, GraphStoreError> {
        // --- resolve repo ---
        let repo = if let Some(cached) = repo_cache.get(&node.repo_id) {
            cached.clone()
        } else {
            let repos = read_txn
                .open_table(REPOS)
                .map_err(GraphStoreError::storage)?;
            let value = repos
                .get(node.repo_id)
                .map_err(GraphStoreError::storage)?
                .ok_or_else(|| {
                    GraphStoreError::storage(format!("missing repo id {}", node.repo_id))
                })?
                .value()
                .to_owned();
            repo_cache.insert(node.repo_id, value.clone());
            value
        };

        // --- resolve file_path ---
        let file_path = if let Some(cached) = file_path_cache.get(&node.file_path_id) {
            cached.clone()
        } else {
            let file_paths = read_txn
                .open_table(FILE_PATHS)
                .map_err(GraphStoreError::storage)?;
            let value = file_paths
                .get(node.file_path_id)
                .map_err(GraphStoreError::storage)?
                .ok_or_else(|| {
                    GraphStoreError::storage(format!("missing file path id {}", node.file_path_id))
                })?
                .value()
                .to_owned();
            file_path_cache.insert(node.file_path_id, value.clone());
            value
        };

        // --- resolve signature ---
        let signature = node
            .signature_id
            .map(|sig_id| {
                if let Some(cached) = sig_cache.get(&sig_id) {
                    return Ok(cached.clone());
                }
                let value =
                    Self::lookup_signature_in_read_txn(read_txn, sig_id)?.ok_or_else(|| {
                        GraphStoreError::storage(format!("missing signature id {sig_id}"))
                    })?;
                sig_cache.insert(sig_id, value.clone());
                Ok::<_, GraphStoreError>(value)
            })
            .transpose()?;

        Ok(NodeData {
            id: node.id,
            kind: node.kind,
            repo,
            file_path,
            name: node.name,
            qualified_name: node.qualified_name,
            external_id: node.external_id,
            signature,
            visibility: node.visibility,
            span: node.span,
            is_virtual: node.is_virtual,
        })
    }

    fn rehydrate_node_in_write_txn(
        write_txn: &redb::WriteTransaction,
        node: StoredNode,
    ) -> Result<NodeData, GraphStoreError> {
        let (repo, file_path) =
            Self::resolve_repo_path_in_write_txn(write_txn, node.repo_id, node.file_path_id)?;
        let signature = node
            .signature_id
            .map(|sig_id| {
                Self::lookup_signature_in_write_txn(write_txn, sig_id)?.ok_or_else(|| {
                    GraphStoreError::storage(format!("missing signature id {sig_id}"))
                })
            })
            .transpose()?;
        Ok(NodeData {
            id: node.id,
            kind: node.kind,
            repo,
            file_path,
            name: node.name,
            qualified_name: node.qualified_name,
            external_id: node.external_id,
            signature,
            visibility: node.visibility,
            span: node.span,
            is_virtual: node.is_virtual,
        })
    }

    fn stored_node_in_txn(
        write_txn: &redb::WriteTransaction,
        node_id: NodeId,
    ) -> Result<Option<StoredNode>, GraphStoreError> {
        let nodes = write_txn
            .open_table(NODES)
            .map_err(GraphStoreError::storage)?;
        nodes
            .get(node_id.as_bytes())
            .map_err(GraphStoreError::storage)?
            .map(|raw| Self::decode_stored_node(raw.value()))
            .transpose()
    }

    fn stored_edge_in_txn(
        write_txn: &redb::WriteTransaction,
        edge_id: EdgeIdBytes,
    ) -> Result<Option<EdgeData>, GraphStoreError> {
        let edges = write_txn
            .open_table(EDGES)
            .map_err(GraphStoreError::storage)?;
        edges
            .get(edge_id)
            .map_err(GraphStoreError::storage)?
            .map(|raw| Self::decode_edge(raw.value()))
            .transpose()
    }

    fn stored_edge_in_read_txn(
        read_txn: &redb::ReadTransaction,
        edge_id: EdgeIdBytes,
    ) -> Result<Option<EdgeData>, GraphStoreError> {
        let edges = read_txn
            .open_table(EDGES)
            .map_err(GraphStoreError::storage)?;
        edges
            .get(edge_id)
            .map_err(GraphStoreError::storage)?
            .map(|raw| Self::decode_edge(raw.value()))
            .transpose()
    }

    fn insert_edge_refs(
        write_txn: &redb::WriteTransaction,
        edge_id: EdgeIdBytes,
        edge: &EdgeData,
    ) -> Result<(), GraphStoreError> {
        {
            let mut edges_out = write_txn
                .open_multimap_table(EDGES_OUT)
                .map_err(GraphStoreError::storage)?;
            edges_out
                .insert(edge.source.as_bytes(), edge_id)
                .map_err(GraphStoreError::storage)?;
        }

        {
            let mut edges_in = write_txn
                .open_multimap_table(EDGES_IN)
                .map_err(GraphStoreError::storage)?;
            edges_in
                .insert(edge.target.as_bytes(), edge_id)
                .map_err(GraphStoreError::storage)?;
        }

        {
            let mut edges_by_owner = write_txn
                .open_multimap_table(EDGES_BY_OWNER)
                .map_err(GraphStoreError::storage)?;
            edges_by_owner
                .insert(edge.owner_file.as_bytes(), edge_id)
                .map_err(GraphStoreError::storage)?;
        }

        {
            let mut edge_kind_counts = write_txn
                .open_table(EDGE_KIND_COUNTS)
                .map_err(GraphStoreError::storage)?;
            Self::increment_edge_kind_count(&mut edge_kind_counts, edge.kind)?;
        }

        Ok(())
    }

    fn open_edge_index_tables(
        write_txn: &redb::WriteTransaction,
    ) -> Result<EdgeIndexTables<'_>, GraphStoreError> {
        Ok(EdgeIndexTables {
            nodes: write_txn
                .open_table(NODES)
                .map_err(GraphStoreError::storage)?,
            edges: write_txn
                .open_table(EDGES)
                .map_err(GraphStoreError::storage)?,
            edges_out: write_txn
                .open_multimap_table(EDGES_OUT)
                .map_err(GraphStoreError::storage)?,
            edges_in: write_txn
                .open_multimap_table(EDGES_IN)
                .map_err(GraphStoreError::storage)?,
            edges_by_owner: write_txn
                .open_multimap_table(EDGES_BY_OWNER)
                .map_err(GraphStoreError::storage)?,
            edge_kind_counts: write_txn
                .open_table(EDGE_KIND_COUNTS)
                .map_err(GraphStoreError::storage)?,
        })
    }

    fn bulk_insert_edge_with_tables(
        tables: &mut EdgeIndexTables<'_>,
        edge: &EdgeData,
        validate_nodes: bool,
    ) -> Result<(), GraphStoreError> {
        // Node validation (3 B-tree reads per edge) is skippable when the
        // caller guarantees source/target/owner were written earlier in the
        // same transaction — i.e. when we're inside `bulk_insert_in_txn`.
        if validate_nodes {
            for node_id in [edge.source, edge.target] {
                if tables
                    .nodes
                    .get(node_id.as_bytes())
                    .map_err(GraphStoreError::storage)?
                    .is_none()
                {
                    return Err(GraphStoreError::MissingNode(node_id));
                }
            }
            let owner = tables
                .nodes
                .get(edge.owner_file.as_bytes())
                .map_err(GraphStoreError::storage)?
                .map(|raw| Self::decode_stored_node(raw.value()))
                .transpose()?
                .ok_or(GraphStoreError::MissingNode(edge.owner_file))?;
            if !matches!(owner.kind, NodeKind::File) {
                return Err(GraphStoreError::OwnerNotAFile(edge.owner_file));
            }
        }

        let edge_id = Self::edge_id(edge);
        // Check for existing edge without re-opening EDGES table.
        let is_new_edge = match tables
            .edges
            .get(edge_id)
            .map_err(GraphStoreError::storage)?
            .map(|raw| Self::decode_edge(raw.value()))
            .transpose()?
        {
            Some(existing) => {
                if !Self::same_edge_identity(&existing, edge) {
                    return Err(GraphStoreError::storage(format!(
                        "edge id collision for structural identity {:?}->{:?}",
                        edge.source, edge.target
                    )));
                }
                false
            }
            None => true,
        };

        if is_new_edge {
            tables
                .edges_out
                .insert(edge.source.as_bytes(), edge_id)
                .map_err(GraphStoreError::storage)?;
            tables
                .edges_in
                .insert(edge.target.as_bytes(), edge_id)
                .map_err(GraphStoreError::storage)?;
            tables
                .edges_by_owner
                .insert(edge.owner_file.as_bytes(), edge_id)
                .map_err(GraphStoreError::storage)?;
            Self::increment_edge_kind_count(&mut tables.edge_kind_counts, edge.kind)?;
        }

        let encoded = Self::encode_edge(edge);
        tables
            .edges
            .insert(edge_id, encoded.as_slice())
            .map_err(GraphStoreError::storage)?;
        Ok(())
    }

    fn remove_edge_refs(
        write_txn: &redb::WriteTransaction,
        edge_id: EdgeIdBytes,
        edge: &EdgeData,
    ) -> Result<(), GraphStoreError> {
        {
            let mut edges_out = write_txn
                .open_multimap_table(EDGES_OUT)
                .map_err(GraphStoreError::storage)?;
            edges_out
                .remove(edge.source.as_bytes(), edge_id)
                .map_err(GraphStoreError::storage)?;
        }

        {
            let mut edges_in = write_txn
                .open_multimap_table(EDGES_IN)
                .map_err(GraphStoreError::storage)?;
            edges_in
                .remove(edge.target.as_bytes(), edge_id)
                .map_err(GraphStoreError::storage)?;
        }

        {
            let mut edges_by_owner = write_txn
                .open_multimap_table(EDGES_BY_OWNER)
                .map_err(GraphStoreError::storage)?;
            edges_by_owner
                .remove(edge.owner_file.as_bytes(), edge_id)
                .map_err(GraphStoreError::storage)?;
        }

        {
            let mut edge_kind_counts = write_txn
                .open_table(EDGE_KIND_COUNTS)
                .map_err(GraphStoreError::storage)?;
            Self::decrement_edge_kind_count(&mut edge_kind_counts, edge.kind)?;
        }

        Ok(())
    }

    fn delete_canonical_edge(
        write_txn: &redb::WriteTransaction,
        edge_id: EdgeIdBytes,
    ) -> Result<(), GraphStoreError> {
        let mut edges = write_txn
            .open_table(EDGES)
            .map_err(GraphStoreError::storage)?;
        let _ = edges.remove(edge_id).map_err(GraphStoreError::storage)?;
        Ok(())
    }

    fn remove_edge_by_id(
        write_txn: &redb::WriteTransaction,
        edge_id: EdgeIdBytes,
    ) -> Result<(), GraphStoreError> {
        if let Some(edge) = Self::stored_edge_in_txn(write_txn, edge_id)? {
            Self::remove_edge_refs(write_txn, edge_id, &edge)?;
            Self::delete_canonical_edge(write_txn, edge_id)?;
        }
        Ok(())
    }

    fn open_node_index_tables(
        write_txn: &redb::WriteTransaction,
    ) -> Result<NodeIndexTables<'_>, GraphStoreError> {
        Ok(NodeIndexTables {
            by_file: write_txn
                .open_multimap_table(BY_FILE)
                .map_err(GraphStoreError::storage)?,
            by_repo: write_txn
                .open_multimap_table(BY_REPO)
                .map_err(GraphStoreError::storage)?,
            by_type: write_txn
                .open_multimap_table(BY_TYPE)
                .map_err(GraphStoreError::storage)?,
            by_external_id: write_txn
                .open_multimap_table(BY_EXTERNAL_ID)
                .map_err(GraphStoreError::storage)?,
            cross_file_candidates: write_txn
                .open_multimap_table(CROSS_FILE_CANDIDATES)
                .map_err(GraphStoreError::storage)?,
            event_family_index: write_txn
                .open_multimap_table(EVENT_FAMILY_INDEX)
                .map_err(GraphStoreError::storage)?,
            route_key_index: write_txn
                .open_multimap_table(ROUTE_KEY_INDEX)
                .map_err(GraphStoreError::storage)?,
            shared_symbol_name_index: write_txn
                .open_multimap_table(SHARED_SYMBOL_NAME_INDEX)
                .map_err(GraphStoreError::storage)?,
        })
    }

    fn insert_node_indexes_with_tables(
        tables: &mut NodeIndexTables<'_>,
        node: &StoredNode,
    ) -> Result<(), GraphStoreError> {
        let node_id = node.id.as_bytes();
        let file_key = Self::file_index_key(node.repo_id, node.file_path_id);

        tables
            .by_file
            .insert(file_key.as_slice(), node_id)
            .map_err(GraphStoreError::storage)?;
        tables
            .by_repo
            .insert(node.repo_id, node_id)
            .map_err(GraphStoreError::storage)?;
        tables
            .by_type
            .insert(node.kind.as_u8(), node_id)
            .map_err(GraphStoreError::storage)?;

        if let Some(external_id) = node.external_id.as_deref() {
            let key = Self::external_id_key(node.kind, external_id);
            tables
                .by_external_id
                .insert(key.as_str(), node_id)
                .map_err(GraphStoreError::storage)?;
        }

        for candidate in Self::candidate_keys_for_node(node).into_iter().flatten() {
            tables
                .cross_file_candidates
                .insert(candidate, node_id)
                .map_err(GraphStoreError::storage)?;
        }

        // Maintain the event-family and route-key indexes for targeted lookups.
        if let Some(event_name) = Self::event_family_name_for_stored_node(node) {
            tables
                .event_family_index
                .insert(event_name.as_str(), node_id)
                .map_err(GraphStoreError::storage)?;
        }
        if let Some(route_key) = Self::canonical_route_key_for_stored_node(node) {
            tables
                .route_key_index
                .insert(route_key.as_str(), node_id)
                .map_err(GraphStoreError::storage)?;
        }
        if let Some(symbol_name) = Self::shared_symbol_name_for_stored_node(node) {
            tables
                .shared_symbol_name_index
                .insert(symbol_name.as_str(), node_id)
                .map_err(GraphStoreError::storage)?;
        }

        Ok(())
    }

    fn remove_node_indexes_with_tables(
        tables: &mut NodeIndexTables<'_>,
        node: &StoredNode,
    ) -> Result<(), GraphStoreError> {
        let node_id = node.id.as_bytes();
        let file_key = Self::file_index_key(node.repo_id, node.file_path_id);

        tables
            .by_file
            .remove(file_key.as_slice(), node_id)
            .map_err(GraphStoreError::storage)?;
        tables
            .by_repo
            .remove(node.repo_id, node_id)
            .map_err(GraphStoreError::storage)?;
        tables
            .by_type
            .remove(node.kind.as_u8(), node_id)
            .map_err(GraphStoreError::storage)?;

        if let Some(external_id) = node.external_id.as_deref() {
            let key = Self::external_id_key(node.kind, external_id);
            tables
                .by_external_id
                .remove(key.as_str(), node_id)
                .map_err(GraphStoreError::storage)?;
        }

        for candidate in Self::candidate_keys_for_node(node).into_iter().flatten() {
            tables
                .cross_file_candidates
                .remove(candidate, node_id)
                .map_err(GraphStoreError::storage)?;
        }

        // Maintain the event-family and route-key indexes.
        if let Some(event_name) = Self::event_family_name_for_stored_node(node) {
            tables
                .event_family_index
                .remove(event_name.as_str(), node_id)
                .map_err(GraphStoreError::storage)?;
        }
        if let Some(route_key) = Self::canonical_route_key_for_stored_node(node) {
            tables
                .route_key_index
                .remove(route_key.as_str(), node_id)
                .map_err(GraphStoreError::storage)?;
        }
        if let Some(symbol_name) = Self::shared_symbol_name_for_stored_node(node) {
            tables
                .shared_symbol_name_index
                .remove(symbol_name.as_str(), node_id)
                .map_err(GraphStoreError::storage)?;
        }

        Ok(())
    }

    fn insert_node_indexes(
        write_txn: &redb::WriteTransaction,
        node: &StoredNode,
    ) -> Result<(), GraphStoreError> {
        let mut tables = Self::open_node_index_tables(write_txn)?;
        Self::insert_node_indexes_with_tables(&mut tables, node)
    }

    fn remove_node_indexes(
        write_txn: &redb::WriteTransaction,
        node: &StoredNode,
    ) -> Result<(), GraphStoreError> {
        let mut tables = Self::open_node_index_tables(write_txn)?;
        Self::remove_node_indexes_with_tables(&mut tables, node)
    }

    fn remove_node_edges(
        write_txn: &redb::WriteTransaction,
        node_id: NodeId,
    ) -> Result<(), GraphStoreError> {
        let removed_outgoing = {
            let mut edges_out = write_txn
                .open_multimap_table(EDGES_OUT)
                .map_err(GraphStoreError::storage)?;
            let removed = edges_out
                .remove_all(node_id.as_bytes())
                .map_err(GraphStoreError::storage)?;
            let mut edge_ids = Vec::new();
            for value in removed {
                edge_ids.push(value.map_err(GraphStoreError::storage)?.value());
            }
            edge_ids
        };

        for edge_id in removed_outgoing {
            if let Some(edge) = Self::stored_edge_in_txn(write_txn, edge_id)? {
                {
                    let mut edges_in = write_txn
                        .open_multimap_table(EDGES_IN)
                        .map_err(GraphStoreError::storage)?;
                    edges_in
                        .remove(edge.target.as_bytes(), edge_id)
                        .map_err(GraphStoreError::storage)?;
                }
                {
                    let mut edges_by_owner = write_txn
                        .open_multimap_table(EDGES_BY_OWNER)
                        .map_err(GraphStoreError::storage)?;
                    edges_by_owner
                        .remove(edge.owner_file.as_bytes(), edge_id)
                        .map_err(GraphStoreError::storage)?;
                }
                {
                    let mut edge_kind_counts = write_txn
                        .open_table(EDGE_KIND_COUNTS)
                        .map_err(GraphStoreError::storage)?;
                    Self::decrement_edge_kind_count(&mut edge_kind_counts, edge.kind)?;
                }
                Self::delete_canonical_edge(write_txn, edge_id)?;
            }
        }

        let removed_incoming = {
            let mut edges_in = write_txn
                .open_multimap_table(EDGES_IN)
                .map_err(GraphStoreError::storage)?;
            let removed = edges_in
                .remove_all(node_id.as_bytes())
                .map_err(GraphStoreError::storage)?;
            let mut edge_ids = Vec::new();
            for value in removed {
                edge_ids.push(value.map_err(GraphStoreError::storage)?.value());
            }
            edge_ids
        };

        for edge_id in removed_incoming {
            if let Some(edge) = Self::stored_edge_in_txn(write_txn, edge_id)? {
                {
                    let mut edges_out = write_txn
                        .open_multimap_table(EDGES_OUT)
                        .map_err(GraphStoreError::storage)?;
                    edges_out
                        .remove(edge.source.as_bytes(), edge_id)
                        .map_err(GraphStoreError::storage)?;
                }
                {
                    let mut edges_by_owner = write_txn
                        .open_multimap_table(EDGES_BY_OWNER)
                        .map_err(GraphStoreError::storage)?;
                    edges_by_owner
                        .remove(edge.owner_file.as_bytes(), edge_id)
                        .map_err(GraphStoreError::storage)?;
                }
                {
                    let mut edge_kind_counts = write_txn
                        .open_table(EDGE_KIND_COUNTS)
                        .map_err(GraphStoreError::storage)?;
                    Self::decrement_edge_kind_count(&mut edge_kind_counts, edge.kind)?;
                }
                Self::delete_canonical_edge(write_txn, edge_id)?;
            }
        }

        Ok(())
    }

    fn collect_nodes_for_ids(
        read_txn: &redb::ReadTransaction,
        ids: Vec<NodeIdBytes>,
    ) -> Result<Vec<NodeData>, GraphStoreError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let nodes = match read_txn.open_table(NODES) {
            Ok(nodes) => nodes,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(Vec::new()),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        // M5: per-transaction caches for interned strings — repo, file_path,
        // and signature are all resolved at most once per unique ID per call.
        let mut repo_cache: rustc_hash::FxHashMap<StringId, String> =
            rustc_hash::FxHashMap::default();
        let mut file_path_cache: rustc_hash::FxHashMap<StringId, String> =
            rustc_hash::FxHashMap::default();
        let mut sig_cache: rustc_hash::FxHashMap<u32, String> = rustc_hash::FxHashMap::default();
        let mut result = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(raw) = nodes.get(id).map_err(GraphStoreError::storage)? {
                let stored = Self::decode_stored_node(raw.value())?;
                let node = Self::rehydrate_node_with_cache(
                    read_txn,
                    stored,
                    &mut repo_cache,
                    &mut file_path_cache,
                    &mut sig_cache,
                )?;
                result.push(node);
            }
        }
        result.sort_by(|left, right| {
            left.repo
                .cmp(&right.repo)
                .then_with(|| left.file_path.cmp(&right.file_path))
                .then_with(|| left.kind.as_u8().cmp(&right.kind.as_u8()))
                .then_with(|| left.name.cmp(&right.name))
        });
        Ok(result)
    }

    fn validate_edge_nodes(
        write_txn: &redb::WriteTransaction,
        edge: &EdgeData,
    ) -> Result<(), GraphStoreError> {
        for node_id in [edge.source, edge.target] {
            if Self::stored_node_in_txn(write_txn, node_id)?.is_none() {
                return Err(GraphStoreError::MissingNode(node_id));
            }
        }

        let owner = Self::stored_node_in_txn(write_txn, edge.owner_file)?
            .ok_or(GraphStoreError::MissingNode(edge.owner_file))?;
        if !matches!(owner.kind, NodeKind::File) {
            return Err(GraphStoreError::OwnerNotAFile(edge.owner_file));
        }

        Ok(())
    }

    fn insert_edge_in_txn(
        write_txn: &redb::WriteTransaction,
        edge: &EdgeData,
    ) -> Result<(), GraphStoreError> {
        Self::validate_edge_nodes(write_txn, edge)?;
        let edge_id = Self::edge_id(edge);
        let is_new_edge = match Self::stored_edge_in_txn(write_txn, edge_id)? {
            Some(existing) => {
                if !Self::same_edge_identity(&existing, edge) {
                    return Err(GraphStoreError::storage(format!(
                        "edge id collision for structural identity {:?}->{:?}",
                        edge.source, edge.target
                    )));
                }
                false
            }
            None => true,
        };

        if is_new_edge {
            Self::insert_edge_refs(write_txn, edge_id, edge)?;
        }

        let encoded = Self::encode_edge(edge);
        let mut edges = write_txn
            .open_table(EDGES)
            .map_err(GraphStoreError::storage)?;
        edges
            .insert(edge_id, encoded.as_slice())
            .map_err(GraphStoreError::storage)?;
        Ok(())
    }

    fn remove_edges_for_owner_in_txn(
        write_txn: &redb::WriteTransaction,
        owner_file: NodeId,
    ) -> Result<(), GraphStoreError> {
        let owned_edges = {
            let mut edges_by_owner = write_txn
                .open_multimap_table(EDGES_BY_OWNER)
                .map_err(GraphStoreError::storage)?;
            let removed = edges_by_owner
                .remove_all(owner_file.as_bytes())
                .map_err(GraphStoreError::storage)?;
            let mut edge_ids = Vec::new();
            for value in removed {
                edge_ids.push(value.map_err(GraphStoreError::storage)?.value());
            }
            edge_ids
        };

        for edge_id in owned_edges {
            if let Some(edge) = Self::stored_edge_in_txn(write_txn, edge_id)? {
                {
                    let mut edges_out = write_txn
                        .open_multimap_table(EDGES_OUT)
                        .map_err(GraphStoreError::storage)?;
                    edges_out
                        .remove(edge.source.as_bytes(), edge_id)
                        .map_err(GraphStoreError::storage)?;
                }
                {
                    let mut edges_in = write_txn
                        .open_multimap_table(EDGES_IN)
                        .map_err(GraphStoreError::storage)?;
                    edges_in
                        .remove(edge.target.as_bytes(), edge_id)
                        .map_err(GraphStoreError::storage)?;
                }
                {
                    let mut edge_kind_counts = write_txn
                        .open_table(EDGE_KIND_COUNTS)
                        .map_err(GraphStoreError::storage)?;
                    Self::decrement_edge_kind_count(&mut edge_kind_counts, edge.kind)?;
                }
                Self::delete_canonical_edge(write_txn, edge_id)?;
            }
        }

        Ok(())
    }

    fn remove_edges_for_owner_by_kind_in_txn(
        write_txn: &redb::WriteTransaction,
        owner_file: NodeId,
        kinds: &[EdgeKind],
    ) -> Result<(), GraphStoreError> {
        if kinds.is_empty() {
            return Ok(());
        }

        let edge_ids = {
            let edges_by_owner = write_txn
                .open_multimap_table(EDGES_BY_OWNER)
                .map_err(GraphStoreError::storage)?;
            edges_by_owner
                .get(owner_file.as_bytes())
                .map_err(GraphStoreError::storage)?
                .map(|entry| {
                    entry
                        .map(|value| value.value())
                        .map_err(GraphStoreError::storage)
                })
                .collect::<Result<Vec<_>, _>>()?
        };

        let matching = {
            let edges = write_txn
                .open_table(EDGES)
                .map_err(GraphStoreError::storage)?;
            let mut matching = Vec::new();
            for edge_id in edge_ids {
                let Some(encoded) = edges.get(edge_id).map_err(GraphStoreError::storage)? else {
                    continue;
                };
                let edge = Self::decode_edge(encoded.value())?;
                if kinds.contains(&edge.kind) {
                    matching.push(edge_id);
                }
            }
            matching
        };

        for edge_id in matching {
            Self::remove_edge_by_id(write_txn, edge_id)?;
        }

        Ok(())
    }

    pub(crate) fn insert_node_in_txn(
        write_txn: &redb::WriteTransaction,
        node: &NodeData,
    ) -> Result<(), GraphStoreError> {
        let stored = Self::store_node_in_txn(write_txn, node)?;
        let mut nodes = write_txn
            .open_table(NODES)
            .map_err(GraphStoreError::storage)?;
        if let Some(existing) = nodes
            .get(node.id.as_bytes())
            .map_err(GraphStoreError::storage)?
            .map(|raw| Self::decode_stored_node(raw.value()))
            .transpose()?
        {
            Self::remove_node_indexes(write_txn, &existing)?;
        }

        let encoded = Self::encode_stored_node(&stored);
        nodes
            .insert(node.id.as_bytes(), encoded.as_slice())
            .map_err(GraphStoreError::storage)?;

        Self::insert_node_indexes(write_txn, &stored)
    }

    pub(crate) fn delete_node_in_txn(
        write_txn: &redb::WriteTransaction,
        id: NodeId,
    ) -> Result<Option<NodeData>, GraphStoreError> {
        let existing = Self::stored_node_in_txn(write_txn, id)?;

        let Some(existing) = existing else {
            return Ok(None);
        };

        if matches!(existing.kind, NodeKind::File) {
            Self::remove_edges_for_owner_in_txn(write_txn, existing.id)?;
        }
        Self::remove_node_edges(write_txn, id)?;
        Self::remove_node_indexes(write_txn, &existing)?;

        {
            let mut nodes = write_txn
                .open_table(NODES)
                .map_err(GraphStoreError::storage)?;
            let _ = nodes
                .remove(id.as_bytes())
                .map_err(GraphStoreError::storage)?;
        }

        Ok(Some(Self::rehydrate_node_in_write_txn(
            write_txn, existing,
        )?))
    }

    pub(crate) fn insert_edge_validated_in_txn(
        write_txn: &redb::WriteTransaction,
        edge: &EdgeData,
    ) -> Result<(), GraphStoreError> {
        Self::insert_edge_in_txn(write_txn, edge)
    }

    /// Bulk-insert a batch of edges using pre-opened tables — saves 6-7 table
    /// opens per edge vs. calling `insert_edge_validated_in_txn` in a loop.
    pub(crate) fn bulk_insert_edges_in_txn(
        write_txn: &redb::WriteTransaction,
        edges: &[EdgeData],
    ) -> Result<(), GraphStoreError> {
        if edges.is_empty() {
            return Ok(());
        }
        let mut tables = Self::open_edge_index_tables(write_txn)?;
        for edge in edges {
            // Cross-file path: referenced nodes live in earlier batches or
            // other files, so we keep the existence check for safety.
            Self::bulk_insert_edge_with_tables(&mut tables, edge, true)?;
        }
        Ok(())
    }

    pub(crate) fn delete_edge_in_txn(
        write_txn: &redb::WriteTransaction,
        edge: &EdgeData,
    ) -> Result<(), GraphStoreError> {
        Self::remove_edge_by_id(write_txn, Self::edge_id(edge))
    }

    pub(crate) fn delete_edges_for_owner_in_txn(
        write_txn: &redb::WriteTransaction,
        owner_file: NodeId,
    ) -> Result<(), GraphStoreError> {
        Self::remove_edges_for_owner_in_txn(write_txn, owner_file)
    }

    pub(crate) fn delete_edges_for_owner_by_kind_in_txn(
        write_txn: &redb::WriteTransaction,
        owner_file: NodeId,
        kinds: &[EdgeKind],
    ) -> Result<(), GraphStoreError> {
        Self::remove_edges_for_owner_by_kind_in_txn(write_txn, owner_file, kinds)
    }

    /// Public entry point that preserves per-edge node validation (`source`,
    /// `target`, `owner_file`) and is used by the `GraphStore::bulk_insert` trait
    /// impl and any external caller that may supply unverified edges.
    pub(crate) fn bulk_insert_in_txn(
        write_txn: &redb::WriteTransaction,
        nodes: &[NodeData],
        edges: &[EdgeData],
    ) -> Result<(), GraphStoreError> {
        Self::bulk_insert_in_txn_inner(write_txn, nodes, edges, true)
    }

    /// Internal variant used by the indexer's hot write path. The caller
    /// guarantees that every edge's `source`/`target`/`owner_file` was produced
    /// by the same parser run as the `nodes` slice, so node existence is
    /// invariant by construction — skipping the 3 per-edge B-tree reads
    /// removes ~800K reads on the full-monorepo cold index.
    pub(crate) fn bulk_insert_in_txn_trusted(
        write_txn: &redb::WriteTransaction,
        nodes: &[NodeData],
        edges: &[EdgeData],
    ) -> Result<(), GraphStoreError> {
        Self::bulk_insert_in_txn_inner(write_txn, nodes, edges, false)
    }

    fn bulk_insert_in_txn_inner(
        write_txn: &redb::WriteTransaction,
        nodes: &[NodeData],
        edges: &[EdgeData],
        validate_nodes: bool,
    ) -> Result<(), GraphStoreError> {
        let mut owner_file_set = FxHashSet::default();
        let mut replaced_owner_files = Vec::new();
        for edge in edges {
            if owner_file_set.insert(edge.owner_file) {
                replaced_owner_files.push(edge.owner_file);
            }
        }

        for owner_file in &replaced_owner_files {
            Self::remove_edges_for_owner_in_txn(write_txn, *owner_file)?;
        }

        let mut table = write_txn
            .open_table(NODES)
            .map_err(GraphStoreError::storage)?;
        let mut index_tables = Self::open_node_index_tables(write_txn)?;
        for node in nodes {
            let stored = Self::store_node_in_txn(write_txn, node)?;
            if let Some(existing) = table
                .get(node.id.as_bytes())
                .map_err(GraphStoreError::storage)?
                .map(|raw| Self::decode_stored_node(raw.value()))
                .transpose()?
            {
                Self::remove_node_indexes_with_tables(&mut index_tables, &existing)?;
                if !matches!(existing.kind, NodeKind::File) && !existing.is_virtual {
                    Self::remove_node_edges(write_txn, existing.id)?;
                }
            }

            let encoded = Self::encode_stored_node(&stored);
            table
                .insert(node.id.as_bytes(), encoded.as_slice())
                .map_err(GraphStoreError::storage)?;
            Self::insert_node_indexes_with_tables(&mut index_tables, &stored)?;
        }
        drop(table);

        // Use pre-opened edge tables for the entire edge loop, avoiding
        // ~5 table opens per edge. Node validation is controlled by the
        // caller — the trusted indexer path skips it for ~800K fewer B-tree
        // reads on the full monorepo.
        let mut edge_tables = Self::open_edge_index_tables(write_txn)?;
        for edge in edges {
            Self::bulk_insert_edge_with_tables(&mut edge_tables, edge, validate_nodes)?;
        }

        Ok(())
    }

    pub(crate) fn delete_file_nodes_in_txn(
        write_txn: &redb::WriteTransaction,
        repo: &str,
        file_path: &str,
    ) -> Result<(), GraphStoreError> {
        let repo_id = {
            let repo_ids = write_txn
                .open_table(REPO_IDS)
                .map_err(GraphStoreError::storage)?;
            repo_ids
                .get(repo)
                .map_err(GraphStoreError::storage)?
                .map(|value| value.value())
        };
        let Some(repo_id) = repo_id else {
            return Ok(());
        };

        let file_path_id = {
            let file_path_ids = write_txn
                .open_table(FILE_PATH_IDS)
                .map_err(GraphStoreError::storage)?;
            file_path_ids
                .get(file_path)
                .map_err(GraphStoreError::storage)?
                .map(|value| value.value())
        };
        let Some(file_path_id) = file_path_id else {
            return Ok(());
        };

        let file_key = Self::file_index_key(repo_id, file_path_id);
        let node_ids = {
            let by_file = write_txn
                .open_multimap_table(BY_FILE)
                .map_err(GraphStoreError::storage)?;
            let values = by_file
                .get(file_key.as_slice())
                .map_err(GraphStoreError::storage)?;
            let mut ids = Vec::new();
            for value in values {
                ids.push(value.map_err(GraphStoreError::storage)?.value());
            }
            ids
        };

        for node_id in node_ids {
            let _ = Self::delete_node_in_txn(write_txn, NodeId(node_id))?;
        }

        Ok(())
    }

    pub fn count_nodes(&self) -> Result<usize, GraphStoreError> {
        let read_txn = self.begin_read_txn()?;
        let table = match read_txn.open_table(NODES) {
            Ok(table) => table,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(0),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        usize::try_from(table.len().map_err(GraphStoreError::storage)?)
            .map_err(|_| GraphStoreError::storage(redb::StorageError::ValueTooLarge(usize::MAX)))
    }

    pub fn count_edges(&self) -> Result<usize, GraphStoreError> {
        let read_txn = self.begin_read_txn()?;
        let table = match read_txn.open_table(EDGES) {
            Ok(table) => table,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(0),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        usize::try_from(table.len().map_err(GraphStoreError::storage)?)
            .map_err(|_| GraphStoreError::storage(redb::StorageError::ValueTooLarge(usize::MAX)))
    }

    /// Compute all five cross-repo edge counters in a single EDGES-table scan
    /// and one read transaction, replacing the previous five sequential calls.
    ///
    /// The returned [`EdgeCountSummary`] contains:
    /// - `cross_repo_edges` — all edges that cross a repo boundary (including
    ///   virtual nodes), equivalent to [`Self::count_cross_repo_edges`].
    /// - `true_cross_repo_edges` — edges where both endpoints are in real
    ///   (non-virtual) repos, equivalent to [`Self::count_true_cross_repo_edges`].
    /// - `history_ownership_edges` — edges whose target is a virtual `Author`
    ///   node, equivalent to [`Self::count_history_ownership_edges`].
    /// - `virtual_other_cross_repo_edges` — edges whose target is a virtual
    ///   non-`Author` node, equivalent to
    ///   [`Self::count_virtual_other_cross_repo_edges`].
    /// - `total_edges` — total number of edges in the graph, equivalent to
    ///   [`Self::count_edges`].
    pub fn count_edge_summary(&self) -> Result<EdgeCountSummary, GraphStoreError> {
        let read_txn = self.db.begin_read().map_err(GraphStoreError::storage)?;
        let nodes = match read_txn.open_table(NODES) {
            Ok(nodes) => nodes,
            Err(error) if Self::is_missing_table_error(&error) => {
                return Ok(EdgeCountSummary::default());
            }
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let edges = match read_txn.open_table(EDGES) {
            Ok(edges) => edges,
            Err(error) if Self::is_missing_table_error(&error) => {
                return Ok(EdgeCountSummary::default());
            }
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let repos = match read_txn.open_table(REPOS) {
            Ok(repos) => repos,
            Err(error) if Self::is_missing_table_error(&error) => {
                return Ok(EdgeCountSummary::default());
            }
            Err(error) => return Err(GraphStoreError::storage(error)),
        };

        // Two-level cache shared across all counters: node-id bytes →
        // (repo_string, is_virtual, kind).
        let mut node_cache: rustc_hash::FxHashMap<NodeIdBytes, (String, bool, NodeKind)> =
            rustc_hash::FxHashMap::default();
        let mut repo_str_cache: rustc_hash::FxHashMap<StringId, String> =
            rustc_hash::FxHashMap::default();

        let mut summary = EdgeCountSummary::default();

        let iter = edges.iter().map_err(GraphStoreError::storage)?;
        for entry in iter {
            let (_edge_id, raw) = entry.map_err(GraphStoreError::storage)?;
            let edge = Self::decode_edge(raw.value())?;
            summary.total_edges = summary.total_edges.saturating_add(1);

            // `CrossRepoDepends` edges are always cross-repo by definition.
            if edge.kind == EdgeKind::CrossRepoDepends {
                summary.cross_repo_edges = summary.cross_repo_edges.saturating_add(1);
                // They link two real repos (no virtual endpoint) → true.
                summary.true_cross_repo_edges = summary.true_cross_repo_edges.saturating_add(1);
                continue;
            }

            let Some((src_repo, _src_virt, _src_kind)) = Self::resolve_node_for_split(
                edge.source.as_bytes(),
                &nodes,
                &repos,
                &mut node_cache,
                &mut repo_str_cache,
            )?
            else {
                continue;
            };
            let Some((tgt_repo, _tgt_virt, tgt_kind)) = Self::resolve_node_for_split(
                edge.target.as_bytes(),
                &nodes,
                &repos,
                &mut node_cache,
                &mut repo_str_cache,
            )?
            else {
                continue;
            };

            if src_repo == tgt_repo {
                continue;
            }

            // Cross-repo (by either the legacy repo-id check or the string check).
            summary.cross_repo_edges = summary.cross_repo_edges.saturating_add(1);

            if tgt_repo == VIRTUAL_NODE_REPO {
                if tgt_kind == NodeKind::Author {
                    summary.history_ownership_edges =
                        summary.history_ownership_edges.saturating_add(1);
                } else {
                    summary.virtual_other_cross_repo_edges =
                        summary.virtual_other_cross_repo_edges.saturating_add(1);
                }
            } else {
                // Both repos are real, non-virtual.
                summary.true_cross_repo_edges = summary.true_cross_repo_edges.saturating_add(1);
            }
        }

        Ok(summary)
    }

    /// Full-graph edge scan returning [`AttributedEdge`] for every edge where
    /// the source and target nodes could be resolved.
    ///
    /// Uses a single read transaction and an in-memory repo-string cache — the
    /// same strategy as `count_cross_repo_edges`. Intended only for diagnostic
    /// examples and tests; never call from production code paths.
    #[cfg(any(test, feature = "test-support"))]
    pub fn all_edges_attributed(&self) -> Result<Vec<AttributedEdge>, GraphStoreError> {
        let read_txn = self.db.begin_read().map_err(GraphStoreError::storage)?;
        let nodes_table = match read_txn.open_table(NODES) {
            Ok(t) => t,
            Err(e) if Self::is_missing_table_error(&e) => return Ok(Vec::new()),
            Err(e) => return Err(GraphStoreError::storage(e)),
        };
        let edges_table = match read_txn.open_table(EDGES) {
            Ok(t) => t,
            Err(e) if Self::is_missing_table_error(&e) => return Ok(Vec::new()),
            Err(e) => return Err(GraphStoreError::storage(e)),
        };
        let repos_table = match read_txn.open_table(REPOS) {
            Ok(t) => t,
            Err(e) if Self::is_missing_table_error(&e) => return Ok(Vec::new()),
            Err(e) => return Err(GraphStoreError::storage(e)),
        };

        // Cache: node id bytes → (repo_string, is_virtual, kind)
        let mut node_cache: rustc_hash::FxHashMap<NodeIdBytes, (String, bool, NodeKind)> =
            rustc_hash::FxHashMap::default();
        // Cache: repo_id → repo string
        let mut repo_str_cache: rustc_hash::FxHashMap<StringId, String> =
            rustc_hash::FxHashMap::default();

        let resolve_node =
            |id_bytes: NodeIdBytes,
             node_cache: &mut rustc_hash::FxHashMap<NodeIdBytes, (String, bool, NodeKind)>,
             repo_str_cache: &mut rustc_hash::FxHashMap<StringId, String>|
             -> Result<Option<(String, bool, NodeKind)>, GraphStoreError> {
                if let Some(cached) = node_cache.get(&id_bytes) {
                    return Ok(Some(cached.clone()));
                }
                let Some(raw) = nodes_table
                    .get(id_bytes)
                    .map_err(GraphStoreError::storage)?
                else {
                    return Ok(None);
                };
                let stored = Self::decode_stored_node(raw.value())?;
                let repo = if let Some(s) = repo_str_cache.get(&stored.repo_id) {
                    s.clone()
                } else {
                    let s = repos_table
                        .get(stored.repo_id)
                        .map_err(GraphStoreError::storage)?
                        .ok_or_else(|| {
                            GraphStoreError::storage(format!("missing repo id {}", stored.repo_id))
                        })?
                        .value()
                        .to_owned();
                    repo_str_cache.insert(stored.repo_id, s.clone());
                    s
                };
                let entry = (repo, stored.is_virtual, stored.kind);
                node_cache.insert(id_bytes, entry.clone());
                Ok(Some(entry))
            };

        let mut result = Vec::new();
        let iter = edges_table.iter().map_err(GraphStoreError::storage)?;
        for entry in iter {
            let (_edge_id, raw) = entry.map_err(GraphStoreError::storage)?;
            let edge = Self::decode_edge(raw.value())?;
            let src_bytes = edge.source.as_bytes();
            let tgt_bytes = edge.target.as_bytes();
            let Some((src_repo, src_virt, src_kind)) =
                resolve_node(src_bytes, &mut node_cache, &mut repo_str_cache)?
            else {
                continue;
            };
            let Some((tgt_repo, tgt_virt, tgt_kind)) =
                resolve_node(tgt_bytes, &mut node_cache, &mut repo_str_cache)?
            else {
                continue;
            };
            result.push((
                edge, src_repo, src_virt, src_kind, tgt_repo, tgt_virt, tgt_kind,
            ));
        }
        Ok(result)
    }
}

fn graph_table_footprint(
    name: &str,
    table_kind: &str,
    entries: u64,
    stats: &redb::TableStats,
) -> GraphTableFootprint {
    GraphTableFootprint {
        name: name.to_owned(),
        table_kind: table_kind.to_owned(),
        entries,
        stored_bytes: stats.stored_bytes(),
        metadata_bytes: stats.metadata_bytes(),
        fragmented_bytes: stats.fragmented_bytes(),
        leaf_pages: stats.leaf_pages(),
        branch_pages: stats.branch_pages(),
        tree_height: stats.tree_height(),
    }
}

impl GraphStore for GraphStoreDb {
    fn insert_node(&self, node: &NodeData) -> Result<(), GraphStoreError> {
        self.with_write_txn(|write_txn| Self::insert_node_in_txn(write_txn, node))
    }

    fn get_node(&self, id: NodeId) -> Result<Option<NodeData>, GraphStoreError> {
        let read_txn = self.db.begin_read().map_err(GraphStoreError::storage)?;
        let nodes = match read_txn.open_table(NODES) {
            Ok(nodes) => nodes,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(None),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let node = nodes
            .get(id.as_bytes())
            .map_err(GraphStoreError::storage)?
            .map(|raw| Self::decode_stored_node(raw.value()))
            .transpose()?;
        node.map_or(Ok(None), |stored| {
            Self::rehydrate_node_in_read_txn(&read_txn, stored).map(Some)
        })
    }

    fn delete_node(&self, id: NodeId) -> Result<Option<NodeData>, GraphStoreError> {
        self.with_write_txn(|write_txn| Self::delete_node_in_txn(write_txn, id))
    }

    fn insert_edge(&self, edge: &EdgeData) -> Result<(), GraphStoreError> {
        self.with_write_txn(|write_txn| Self::insert_edge_validated_in_txn(write_txn, edge))
    }

    fn delete_edge(&self, edge: &EdgeData) -> Result<(), GraphStoreError> {
        self.with_write_txn(|write_txn| Self::delete_edge_in_txn(write_txn, edge))
    }

    fn get_outgoing(&self, source: NodeId) -> Result<Vec<EdgeData>, GraphStoreError> {
        let read_txn = self.db.begin_read().map_err(GraphStoreError::storage)?;
        let edges = match read_txn.open_multimap_table(EDGES_OUT) {
            Ok(edges) => edges,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(Vec::new()),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let values = edges
            .get(source.as_bytes())
            .map_err(GraphStoreError::storage)?;
        let mut result = Vec::new();
        for value in values {
            let raw = value.map_err(GraphStoreError::storage)?;
            if let Some(edge) = Self::stored_edge_in_read_txn(&read_txn, raw.value())? {
                result.push(edge);
            }
        }
        Ok(result)
    }

    fn get_incoming(&self, target: NodeId) -> Result<Vec<EdgeData>, GraphStoreError> {
        let read_txn = self.db.begin_read().map_err(GraphStoreError::storage)?;
        let edges = match read_txn.open_multimap_table(EDGES_IN) {
            Ok(edges) => edges,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(Vec::new()),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let values = edges
            .get(target.as_bytes())
            .map_err(GraphStoreError::storage)?;
        let mut result = Vec::new();
        for value in values {
            let raw = value.map_err(GraphStoreError::storage)?;
            if let Some(edge) = Self::stored_edge_in_read_txn(&read_txn, raw.value())? {
                result.push(edge);
            }
        }
        Ok(result)
    }

    fn edges_by_owner(&self, owner_file: NodeId) -> Result<Vec<EdgeData>, GraphStoreError> {
        let read_txn = self.db.begin_read().map_err(GraphStoreError::storage)?;
        let edges = match read_txn.open_multimap_table(EDGES_BY_OWNER) {
            Ok(edges) => edges,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(Vec::new()),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let values = edges
            .get(owner_file.as_bytes())
            .map_err(GraphStoreError::storage)?;
        let mut result = Vec::new();
        for value in values {
            let raw = value.map_err(GraphStoreError::storage)?;
            if let Some(edge) = Self::stored_edge_in_read_txn(&read_txn, raw.value())? {
                result.push(edge);
            }
        }
        Ok(result)
    }

    fn delete_edges_for_owner(&self, owner_file: NodeId) -> Result<(), GraphStoreError> {
        self.with_write_txn(|write_txn| Self::delete_edges_for_owner_in_txn(write_txn, owner_file))
    }

    fn delete_edges_for_owner_by_kind(
        &self,
        owner_file: NodeId,
        kinds: &[EdgeKind],
    ) -> Result<(), GraphStoreError> {
        self.with_write_txn(|write_txn| {
            Self::delete_edges_for_owner_by_kind_in_txn(write_txn, owner_file, kinds)
        })
    }

    fn replace_edges_for_owners_by_kind(
        &self,
        owner_files: &[NodeId],
        kinds: &[EdgeKind],
        edges: &[EdgeData],
    ) -> Result<(), GraphStoreError> {
        self.with_write_txn(|write_txn| {
            for owner_file in owner_files {
                Self::delete_edges_for_owner_by_kind_in_txn(write_txn, *owner_file, kinds)?;
            }
            Self::bulk_insert_edges_in_txn(write_txn, edges)
        })
    }

    fn nodes_by_file(&self, repo: &str, file_path: &str) -> Result<Vec<NodeData>, GraphStoreError> {
        let read_txn = self.db.begin_read().map_err(GraphStoreError::storage)?;
        let Some(repo_id) = Self::lookup_repo_id(&read_txn, repo)? else {
            return Ok(Vec::new());
        };
        let Some(file_path_id) = Self::lookup_file_path_id(&read_txn, file_path)? else {
            return Ok(Vec::new());
        };
        let by_file = match read_txn.open_multimap_table(BY_FILE) {
            Ok(by_file) => by_file,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(Vec::new()),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let file_key = Self::file_index_key(repo_id, file_path_id);
        let values = by_file
            .get(file_key.as_slice())
            .map_err(GraphStoreError::storage)?;
        let mut ids = Vec::new();
        for value in values {
            let raw = value.map_err(GraphStoreError::storage)?;
            ids.push(raw.value());
        }
        Self::collect_nodes_for_ids(&read_txn, ids)
    }

    fn nodes_by_type(&self, kind: NodeKind) -> Result<Vec<NodeData>, GraphStoreError> {
        let read_txn = self.db.begin_read().map_err(GraphStoreError::storage)?;
        let by_type = match read_txn.open_multimap_table(BY_TYPE) {
            Ok(by_type) => by_type,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(Vec::new()),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let values = by_type
            .get(kind.as_u8())
            .map_err(GraphStoreError::storage)?;
        let mut ids = Vec::new();
        for value in values {
            let raw = value.map_err(GraphStoreError::storage)?;
            ids.push(raw.value());
        }
        Self::collect_nodes_for_ids(&read_txn, ids)
    }

    fn nodes_by_candidate_keys(
        &self,
        candidate_keys: &[String],
    ) -> Result<Vec<NodeData>, GraphStoreError> {
        if candidate_keys.is_empty() {
            return Ok(Vec::new());
        }

        let read_txn = self.db.begin_read().map_err(GraphStoreError::storage)?;
        let candidates = match read_txn.open_multimap_table(CROSS_FILE_CANDIDATES) {
            Ok(candidates) => candidates,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(Vec::new()),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };

        let mut ids = Vec::new();
        let mut seen = FxHashSet::default();
        for candidate_key in candidate_keys {
            let values = candidates
                .get(candidate_key.as_str())
                .map_err(GraphStoreError::storage)?;
            for value in values {
                let raw = value.map_err(GraphStoreError::storage)?;
                if seen.insert(raw.value()) {
                    ids.push(raw.value());
                }
            }
        }

        Self::collect_nodes_for_ids(&read_txn, ids)
    }

    fn nodes_by_repo(&self, repo: &str) -> Result<Vec<NodeData>, GraphStoreError> {
        let read_txn = self.db.begin_read().map_err(GraphStoreError::storage)?;
        let Some(repo_id) = Self::lookup_repo_id(&read_txn, repo)? else {
            return Ok(Vec::new());
        };
        let by_repo = match read_txn.open_multimap_table(BY_REPO) {
            Ok(by_repo) => by_repo,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(Vec::new()),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let values = by_repo.get(repo_id).map_err(GraphStoreError::storage)?;
        let mut ids = Vec::new();
        for value in values {
            let raw = value.map_err(GraphStoreError::storage)?;
            ids.push(raw.value());
        }
        Self::collect_nodes_for_ids(&read_txn, ids)
    }

    fn count_nodes_by_repo(&self, repo: &str) -> Result<usize, GraphStoreError> {
        let read_txn = self.db.begin_read().map_err(GraphStoreError::storage)?;
        let Some(repo_id) = Self::lookup_repo_id(&read_txn, repo)? else {
            return Ok(0);
        };
        let by_repo = match read_txn.open_multimap_table(BY_REPO) {
            Ok(by_repo) => by_repo,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(0),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        Ok(by_repo
            .get(repo_id)
            .map_err(GraphStoreError::storage)?
            .count())
    }

    fn count_edges_by_owner_repo(&self, repo: &str) -> Result<u64, GraphStoreError> {
        let read_txn = self.db.begin_read().map_err(GraphStoreError::storage)?;
        let Some(repo_id) = Self::lookup_repo_id(&read_txn, repo)? else {
            return Ok(0);
        };
        let by_repo = match read_txn.open_multimap_table(BY_REPO) {
            Ok(by_repo) => by_repo,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(0),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let nodes = match read_txn.open_table(NODES) {
            Ok(nodes) => nodes,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(0),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let edges_by_owner = match read_txn.open_multimap_table(EDGES_BY_OWNER) {
            Ok(edges) => edges,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(0),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        // Every edge's `owner_file` references a File node, so to count edges
        // for `repo` we walk File nodes in the repo via BY_REPO and sum the
        // EDGES_BY_OWNER multimap value counts. This replaces the previous
        // full EDGES-table scan that ran once per repo (O(repos × total_edges)
        // per workspace index) with an O(edges_in_repo) walk.
        let mut count: u64 = 0;
        let values = by_repo.get(repo_id).map_err(GraphStoreError::storage)?;
        for value in values {
            let raw = value.map_err(GraphStoreError::storage)?;
            let id = raw.value();
            let Some(node_raw) = nodes.get(id).map_err(GraphStoreError::storage)? else {
                continue;
            };
            let stored = Self::decode_stored_node(node_raw.value())?;
            if stored.kind != NodeKind::File {
                continue;
            }
            let owned = edges_by_owner.get(id).map_err(GraphStoreError::storage)?;
            count = count.saturating_add(u64::try_from(owned.count()).unwrap_or(u64::MAX));
        }
        Ok(count)
    }

    fn count_nodes_by_repo_and_kind(
        &self,
        repo: &str,
        kind: NodeKind,
    ) -> Result<usize, GraphStoreError> {
        let read_txn = self.db.begin_read().map_err(GraphStoreError::storage)?;
        let Some(repo_id) = Self::lookup_repo_id(&read_txn, repo)? else {
            return Ok(0);
        };
        let by_repo = match read_txn.open_multimap_table(BY_REPO) {
            Ok(by_repo) => by_repo,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(0),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let nodes = match read_txn.open_table(NODES) {
            Ok(nodes) => nodes,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(0),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let mut count = 0_usize;
        let values = by_repo.get(repo_id).map_err(GraphStoreError::storage)?;
        for value in values {
            let raw = value.map_err(GraphStoreError::storage)?;
            let id = raw.value();
            if let Some(node_raw) = nodes.get(id).map_err(GraphStoreError::storage)? {
                let stored = Self::decode_stored_node(node_raw.value())?;
                if stored.kind == kind {
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    fn nodes_by_external_id(
        &self,
        kind: NodeKind,
        external_id: &str,
    ) -> Result<Vec<NodeData>, GraphStoreError> {
        let read_txn = self.db.begin_read().map_err(GraphStoreError::storage)?;
        let by_external_id = match read_txn.open_multimap_table(BY_EXTERNAL_ID) {
            Ok(by_external_id) => by_external_id,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(Vec::new()),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let key = Self::external_id_key(kind, external_id);
        let values = by_external_id
            .get(key.as_str())
            .map_err(GraphStoreError::storage)?;
        let mut ids = Vec::new();
        for value in values {
            let raw = value.map_err(GraphStoreError::storage)?;
            ids.push(raw.value());
        }
        Self::collect_nodes_for_ids(&read_txn, ids)
    }

    fn count_nodes_by_kind(&self, kind: NodeKind) -> Result<usize, GraphStoreError> {
        let read_txn = self.db.begin_read().map_err(GraphStoreError::storage)?;
        let by_type = match read_txn.open_multimap_table(BY_TYPE) {
            Ok(by_type) => by_type,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(0),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        Ok(by_type
            .get(kind.as_u8())
            .map_err(GraphStoreError::storage)?
            .count())
    }

    fn count_edges_by_kind(&self, kind: EdgeKind) -> Result<usize, GraphStoreError> {
        let read_txn = self.db.begin_read().map_err(GraphStoreError::storage)?;
        let edge_kind_counts = match read_txn.open_table(EDGE_KIND_COUNTS) {
            Ok(edge_kind_counts) => edge_kind_counts,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(0),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        Ok(edge_kind_counts
            .get(kind.as_u8())
            .map_err(GraphStoreError::storage)?
            .map_or(0, |count| {
                usize::try_from(count.value()).unwrap_or(usize::MAX)
            }))
    }

    fn nodes_by_event_family_name(
        &self,
        normalized_name: &str,
    ) -> Result<Vec<NodeData>, GraphStoreError> {
        let read_txn = self.db.begin_read().map_err(GraphStoreError::storage)?;
        let index = match read_txn.open_multimap_table(EVENT_FAMILY_INDEX) {
            Ok(index) => index,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(Vec::new()),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let values = index
            .get(normalized_name)
            .map_err(GraphStoreError::storage)?;
        let mut ids = Vec::new();
        for value in values {
            let raw = value.map_err(GraphStoreError::storage)?;
            ids.push(raw.value());
        }
        Self::collect_nodes_for_ids(&read_txn, ids)
    }

    fn nodes_by_route_key(&self, canonical_key: &str) -> Result<Vec<NodeData>, GraphStoreError> {
        let read_txn = self.db.begin_read().map_err(GraphStoreError::storage)?;
        let index = match read_txn.open_multimap_table(ROUTE_KEY_INDEX) {
            Ok(index) => index,
            Err(error) if Self::is_missing_table_error(&error) => return Ok(Vec::new()),
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let values = index.get(canonical_key).map_err(GraphStoreError::storage)?;
        let mut ids = Vec::new();
        for value in values {
            let raw = value.map_err(GraphStoreError::storage)?;
            ids.push(raw.value());
        }
        Self::collect_nodes_for_ids(&read_txn, ids)
    }

    fn nodes_by_shared_symbol_name(
        &self,
        short_name: &str,
    ) -> Result<Vec<NodeData>, GraphStoreError> {
        let read_txn = self.db.begin_read().map_err(GraphStoreError::storage)?;
        let index = match read_txn.open_multimap_table(SHARED_SYMBOL_NAME_INDEX) {
            Ok(index) => index,
            Err(error) if Self::is_missing_table_error(&error) => {
                let mut normalized = short_name.to_owned();
                normalized.make_ascii_lowercase();
                return self.nodes_by_shared_symbol_name_scan(&normalized);
            }
            Err(error) => return Err(GraphStoreError::storage(error)),
        };
        let mut normalized = short_name.to_owned();
        normalized.make_ascii_lowercase();
        let values = index
            .get(normalized.as_str())
            .map_err(GraphStoreError::storage)?;
        let mut ids = Vec::new();
        for value in values {
            let raw = value.map_err(GraphStoreError::storage)?;
            ids.push(raw.value());
        }
        let nodes = Self::collect_nodes_for_ids(&read_txn, ids)?;
        if nodes.is_empty() {
            // Existing warm stores created before SHARED_SYMBOL_NAME_INDEX was
            // introduced may contain virtual shared-symbol nodes that have not
            // been rewritten yet. Falling back to a kind scan preserves
            // correctness for those stores; fresh indexes still use the O(1)
            // table above.
            return self.nodes_by_shared_symbol_name_scan(&normalized);
        }
        Ok(nodes)
    }

    fn bulk_insert(&self, nodes: &[NodeData], edges: &[EdgeData]) -> Result<(), GraphStoreError> {
        self.with_write_txn(|write_txn| Self::bulk_insert_in_txn(write_txn, nodes, edges))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::PathBuf,
        process,
        time::{SystemTime, UNIX_EPOCH},
    };

    use gather_step_core::{
        EdgeKind, EdgeMetadata, MIGRATION_FILTERS_METADATA_PREFIX, ResolverStrategy, SourceSpan,
        Visibility, node_id,
    };
    use pretty_assertions::assert_eq;
    use redb::ReadableDatabase;

    use super::{
        BY_EXTERNAL_ID, CROSS_FILE_CANDIDATES, EDGE_KIND_COUNTS, GraphStore, GraphStoreDb,
        StoredDriftKind, StoredEdgeMetadata, StoredResolver,
    };
    use crate::{StorageDaemonMetadata, StorageDaemonMetadataGuard};
    use gather_step_core::{EdgeData, NodeData, NodeId, NodeKind};

    fn temp_db_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should be monotonic enough for tests")
            .as_nanos();
        env::temp_dir().join(format!("gather-step-{name}-{}-{nanos}.redb", process::id()))
    }

    fn test_store(name: &str) -> GraphStoreDb {
        GraphStoreDb::open(temp_db_path(name)).expect("graph store should open")
    }

    fn temp_workspace_graph_path(name: &str) -> (PathBuf, PathBuf, PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should be monotonic enough for tests")
            .as_nanos();
        let workspace = env::temp_dir().join(format!(
            "gather-step-storage-held-{name}-{}-{nanos}",
            process::id()
        ));
        let storage = workspace.join(".gather-step/storage");
        fs::create_dir_all(&storage).expect("storage dir should exist");
        let graph = storage.join("graph.redb");
        (workspace, storage, graph)
    }

    fn node(repo: &str, file_path: &str, kind: NodeKind, name: &str, _ordinal: u16) -> NodeData {
        NodeData {
            id: node_id(repo, file_path, kind, name),
            kind,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(format!("{repo}::{name}")),
            external_id: None,
            signature: Some(format!("{name}()")),
            visibility: Some(Visibility::Public),
            span: Some(SourceSpan {
                line_start: 1,
                line_len: 1,
                column_start: 0,
                column_len: 8,
            }),
            is_virtual: false,
        }
    }

    fn edge(source: NodeId, target: NodeId, owner_file: NodeId) -> EdgeData {
        EdgeData {
            source,
            target,
            kind: EdgeKind::Calls,
            metadata: EdgeMetadata::default(),
            owner_file,
            is_cross_file: false,
        }
    }

    fn pr_node(external_id: &str, name: &str) -> NodeData {
        NodeData {
            id: node_id("service-a", "prs", NodeKind::PR, name),
            kind: NodeKind::PR,
            repo: "service-a".to_owned(),
            file_path: "prs".to_owned(),
            name: name.to_owned(),
            qualified_name: Some(format!("service-a::{name}")),
            external_id: Some(external_id.to_owned()),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        }
    }

    fn virtual_topic(repo: &str, file_path: &str) -> NodeData {
        NodeData {
            id: gather_step_core::ref_node_id(NodeKind::Topic, "__topic__kafka__order.created"),
            kind: NodeKind::Topic,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: "order.created".to_owned(),
            qualified_name: Some("__topic__kafka__order.created".to_owned()),
            external_id: Some("__topic__kafka__order.created".to_owned()),
            signature: None,
            visibility: None,
            span: Some(SourceSpan {
                line_start: 1,
                line_len: 0,
                column_start: 0,
                column_len: 1,
            }),
            is_virtual: true,
        }
    }

    #[test]
    fn inserts_and_reads_node() {
        let store = test_store("insert-node");
        let function = node("service-a", "src/foo.ts", NodeKind::Function, "execute", 0);

        store.insert_node(&function).expect("node should insert");
        let loaded = store
            .get_node(function.id)
            .expect("node fetch should succeed")
            .expect("node should exist");

        assert_eq!(loaded, function);
    }

    #[test]
    fn open_reports_storage_held_with_daemon_metadata() {
        let (workspace, storage, graph_path) = temp_workspace_graph_path("held-daemon");
        let _holder = GraphStoreDb::open(&graph_path).expect("first graph open should succeed");
        let _metadata = StorageDaemonMetadataGuard::write_for_storage_root(&storage, &workspace)
            .expect("metadata guard should write")
            .expect("storage root should map to daemon pid path");

        let Err(err) = GraphStoreDb::open(&graph_path) else {
            panic!("second open should fail");
        };

        assert!(matches!(
            err,
            super::GraphStoreError::StorageHeldByDaemon {
                path,
                pid,
                started_at_epoch_ms,
                workspace_root,
            } if path == graph_path
                && pid == process::id()
                && started_at_epoch_ms > 0
                && workspace_root == workspace.display().to_string()
        ));
    }

    #[test]
    fn open_reports_storage_held_without_daemon_metadata() {
        let (_workspace, _storage, graph_path) = temp_workspace_graph_path("held-generic");
        let _holder = GraphStoreDb::open(&graph_path).expect("first graph open should succeed");

        let Err(err) = GraphStoreDb::open(&graph_path) else {
            panic!("second open should fail");
        };

        assert!(matches!(
            err,
            super::GraphStoreError::StorageHeld { path } if path == graph_path
        ));
    }

    #[test]
    fn open_stamps_fresh_schema_version_zero() {
        let graph_path = temp_db_path("fresh-graph-schema");
        let store = GraphStoreDb::open(&graph_path).expect("fresh graph db should open");
        drop(store);

        let db = redb::Database::open(&graph_path).expect("graph db should reopen");
        let read_txn = db.begin_read().expect("read txn should begin");
        let schema = read_txn
            .open_table(super::GRAPH_SCHEMA)
            .expect("schema table should exist");
        let version = schema
            .get(super::GRAPH_SCHEMA_VERSION_KEY)
            .expect("schema version should read")
            .expect("schema version should be stamped")
            .value();
        assert_eq!(version, 0);
        drop(schema);
        drop(read_txn);
        drop(db);
        fs::remove_file(graph_path).ok();
    }

    #[test]
    fn daemon_metadata_round_trips_from_graph_path() {
        let (workspace, _storage, graph_path) = temp_workspace_graph_path("metadata-read");
        let metadata = StorageDaemonMetadata::for_current_process(&workspace);
        let pid_path = graph_path
            .parent()
            .and_then(std::path::Path::parent)
            .expect("graph path should live under storage root")
            .join("daemon.pid");
        metadata
            .write_to_path(&pid_path)
            .expect("metadata should write");

        let loaded = StorageDaemonMetadata::read_for_graph_path(&graph_path)
            .expect("metadata should load from graph path");

        assert_eq!(loaded, metadata);
    }

    #[test]
    fn stores_multimap_outgoing_edges() {
        let store = test_store("outgoing-edges");
        let file = node("service-a", "src/foo.ts", NodeKind::File, "src/foo.ts", 0);
        let a = node("service-a", "src/foo.ts", NodeKind::Function, "a", 0);
        let b = node("service-a", "src/foo.ts", NodeKind::Function, "b", 1);
        let c = node("service-a", "src/foo.ts", NodeKind::Function, "c", 2);

        store
            .bulk_insert(
                &[file.clone(), a.clone(), b.clone(), c.clone()],
                &[edge(a.id, b.id, file.id), edge(a.id, c.id, file.id)],
            )
            .expect("batch should insert");

        let outgoing = store.get_outgoing(a.id).expect("outgoing should load");

        assert_eq!(outgoing.len(), 2);
        assert_eq!(outgoing[0].source, a.id);
        assert_eq!(outgoing[1].source, a.id);
    }

    #[test]
    fn stores_reverse_edge_lookup() {
        let store = test_store("incoming-edges");
        let file = node("service-a", "src/foo.ts", NodeKind::File, "src/foo.ts", 0);
        let a = node("service-a", "src/foo.ts", NodeKind::Function, "a", 0);
        let b = node("service-a", "src/foo.ts", NodeKind::Function, "b", 1);
        let relation = edge(a.id, b.id, file.id);

        store
            .bulk_insert(
                &[file, a.clone(), b.clone()],
                std::slice::from_ref(&relation),
            )
            .expect("batch should insert");

        let incoming = store.get_incoming(b.id).expect("incoming should load");

        assert_eq!(incoming, vec![relation]);
    }

    #[test]
    fn looks_up_nodes_by_file() {
        let store = test_store("nodes-by-file");
        let file = node("service-a", "src/foo.ts", NodeKind::File, "src/foo.ts", 0);
        let function = node("service-a", "src/foo.ts", NodeKind::Function, "execute", 0);
        let class = node(
            "service-a",
            "src/foo.ts",
            NodeKind::Class,
            "OrderService",
            1,
        );
        let other = node("service-a", "src/bar.ts", NodeKind::Function, "skip", 0);

        store
            .bulk_insert(&[file.clone(), function.clone(), class.clone(), other], &[])
            .expect("batch should insert");

        let nodes = store
            .nodes_by_file("service-a", "src/foo.ts")
            .expect("file lookup should succeed");

        assert_eq!(nodes, vec![file, function, class]);
    }

    #[test]
    fn bulk_insert_supports_large_batch() {
        let store = test_store("bulk-insert");
        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        let file = node("service-a", "src/bulk.ts", NodeKind::File, "src/bulk.ts", 0);

        for index in 0..1_000_u16 {
            nodes.push(node(
                "service-a",
                "src/bulk.ts",
                NodeKind::Function,
                &format!("symbol_{index}"),
                index,
            ));
        }

        let owner_file = file.id;
        nodes.push(file);
        for index in 0..1_000_usize {
            let source = nodes[index].id;
            let first = nodes[(index + 1) % nodes.len()].id;
            let second = nodes[(index + 2) % nodes.len()].id;
            let third = nodes[(index + 3) % nodes.len()].id;
            edges.push(edge(source, first, owner_file));
            edges.push(edge(source, second, owner_file));
            edges.push(edge(source, third, owner_file));
        }

        store
            .bulk_insert(&nodes, &edges)
            .expect("batch should insert");

        let functions = store
            .nodes_by_type(NodeKind::Function)
            .expect("type lookup should succeed");

        assert_eq!(functions.len(), 1_000);
    }

    #[test]
    fn insert_edge_rejects_missing_nodes() {
        let store = test_store("reject-dangling-edge");
        let missing = NodeId([9; 16]);

        let result = store.insert_edge(&edge(missing, missing, missing));

        assert!(matches!(
            result,
            Err(super::GraphStoreError::MissingNode(node_id)) if node_id == missing
        ));
    }

    #[test]
    fn insert_edge_rejects_non_file_owner() {
        let store = test_store("reject-non-file-owner");
        let file = node("service-a", "src/foo.ts", NodeKind::File, "src/foo.ts", 0);
        let owner = node("service-a", "src/foo.ts", NodeKind::Function, "owner", 0);
        let source = node("service-a", "src/foo.ts", NodeKind::Function, "source", 1);
        let target = node("service-a", "src/foo.ts", NodeKind::Function, "target", 2);

        store
            .bulk_insert(&[file, owner.clone(), source.clone(), target.clone()], &[])
            .expect("nodes should insert");

        let result = store.insert_edge(&edge(source.id, target.id, owner.id));

        assert!(matches!(
            result,
            Err(super::GraphStoreError::OwnerNotAFile(node_id)) if node_id == owner.id
        ));
    }

    #[test]
    fn insert_edge_upserts_metadata_without_accumulating_duplicates() {
        let store = test_store("edge-metadata-upsert");
        let file = node("service-a", "src/foo.ts", NodeKind::File, "src/foo.ts", 0);
        let source = node("service-a", "src/foo.ts", NodeKind::Function, "source", 1);
        let target = node("service-a", "src/foo.ts", NodeKind::Function, "target", 2);

        store
            .bulk_insert(&[file.clone(), source.clone(), target.clone()], &[])
            .expect("nodes should insert");

        let original = EdgeData {
            source: source.id,
            target: target.id,
            kind: EdgeKind::Calls,
            metadata: EdgeMetadata {
                weight: Some(1),
                confidence: Some(90),
                timestamp_unix: Some(1_700_000_000),
                drift_kind: None,
                resolver: Some(ResolverStrategy::FirstPass.as_str().to_owned()),
            },
            owner_file: file.id,
            is_cross_file: false,
        };
        let updated = EdgeData {
            metadata: EdgeMetadata {
                weight: Some(2),
                confidence: Some(75),
                timestamp_unix: Some(1_700_000_100),
                drift_kind: Some("rescored".to_owned()),
                resolver: Some(ResolverStrategy::SecondPass.as_str().to_owned()),
            },
            ..original.clone()
        };

        store
            .insert_edge(&original)
            .expect("original edge should insert");
        store
            .insert_edge(&updated)
            .expect("updated edge should upsert");

        let outgoing = store.get_outgoing(source.id).expect("outgoing should load");
        assert_eq!(outgoing, vec![updated]);
        assert_eq!(store.count_edges().expect("edge count should load"), 1);
    }

    #[test]
    fn stored_edge_metadata_compacts_known_strings_and_round_trips() {
        let metadata = EdgeMetadata {
            weight: Some(7),
            confidence: Some(88),
            timestamp_unix: Some(1_700_000_123),
            drift_kind: Some(format!("{MIGRATION_FILTERS_METADATA_PREFIX}[\"tenantId\"]")),
            resolver: Some(ResolverStrategy::ImportMap.as_str().to_owned()),
        };

        let stored = StoredEdgeMetadata::from_public(&metadata);

        assert_eq!(
            stored.drift_kind,
            Some(StoredDriftKind::MigrationFilters(
                "[\"tenantId\"]".to_owned()
            ))
        );
        assert_eq!(
            stored.resolver,
            Some(StoredResolver::Known(ResolverStrategy::ImportMap))
        );
        assert_eq!(stored.into_public(), metadata);
    }

    #[test]
    fn indexes_external_ids_for_non_commit_nodes() {
        let store = test_store("external-id-pr");
        let pr = pr_node("pull/123", "PR #123");

        store.insert_node(&pr).expect("pr node should insert");

        let loaded = store
            .nodes_by_external_id(NodeKind::PR, "pull/123")
            .expect("external id lookup should succeed");
        assert_eq!(loaded, vec![pr.clone()]);

        let read_txn = store.db.begin_read().expect("read txn should open");
        let by_external_id = read_txn
            .open_multimap_table(BY_EXTERNAL_ID)
            .expect("by_external_id table should open");
        let key = GraphStoreDb::external_id_key(NodeKind::PR, "pull/123");
        assert_eq!(
            by_external_id
                .get(key.as_str())
                .expect("external lookup should work")
                .count(),
            1
        );
    }

    #[test]
    fn bulk_insert_replaces_cross_file_candidates() {
        let store = test_store("stale-candidates");
        let old_node = node("service-a", "src/foo.ts", NodeKind::Function, "execute", 0);
        let new_node = NodeData {
            qualified_name: Some("service-a::run".to_owned()),
            ..old_node.clone()
        };

        store
            .bulk_insert(std::slice::from_ref(&old_node), &[])
            .expect("old node should insert");
        store
            .bulk_insert(std::slice::from_ref(&new_node), &[])
            .expect("new node should replace old one");

        let read_txn = store.db.begin_read().expect("read txn should open");
        let candidates = read_txn
            .open_multimap_table(CROSS_FILE_CANDIDATES)
            .expect("candidate table should open");

        assert_eq!(
            candidates
                .get("service-a::execute")
                .expect("old candidate lookup should work")
                .count(),
            0
        );
        assert_eq!(
            candidates
                .get("service-a::run")
                .expect("new candidate lookup should work")
                .count(),
            1
        );
    }

    #[test]
    fn nodes_by_candidate_keys_returns_unique_matches() {
        let store = test_store("candidate-lookup");
        let first = NodeData {
            qualified_name: Some("service-a::execute".to_owned()),
            ..node("service-a", "src/foo.ts", NodeKind::Function, "execute", 0)
        };
        let second = NodeData {
            qualified_name: Some("service-a::run".to_owned()),
            ..node("service-a", "src/bar.ts", NodeKind::Function, "run", 0)
        };

        store
            .bulk_insert(&[first.clone(), second.clone()], &[])
            .expect("nodes should insert");

        let matches = store
            .nodes_by_candidate_keys(&[
                "execute".to_owned(),
                "service-a::execute".to_owned(),
                "run".to_owned(),
            ])
            .expect("candidate lookup should succeed");

        assert_eq!(matches.len(), 2);
        assert!(matches.contains(&first));
        assert!(matches.contains(&second));
    }

    #[test]
    fn delete_edges_for_owner_removes_all_owned_edges() {
        let store = test_store("owner-edge-delete");
        let file = node("service-a", "src/foo.ts", NodeKind::File, "src/foo.ts", 0);
        let a = node("service-a", "src/foo.ts", NodeKind::Function, "a", 0);
        let b = node("service-a", "src/foo.ts", NodeKind::Function, "b", 1);
        let c = node("service-a", "src/foo.ts", NodeKind::Function, "c", 2);

        store
            .bulk_insert(
                &[file.clone(), a.clone(), b.clone(), c.clone()],
                &[edge(a.id, b.id, file.id), edge(a.id, c.id, file.id)],
            )
            .expect("batch should insert");

        store
            .delete_edges_for_owner(file.id)
            .expect("owner delete should succeed");

        assert_eq!(
            store
                .get_outgoing(a.id)
                .expect("outgoing should load")
                .len(),
            0
        );
        assert_eq!(
            store
                .get_incoming(b.id)
                .expect("incoming should load")
                .len(),
            0
        );
        assert_eq!(
            store
                .get_incoming(c.id)
                .expect("incoming should load")
                .len(),
            0
        );
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let store = test_store("get-missing-node");

        let loaded = store
            .get_node(NodeId([42; 16]))
            .expect("missing lookup should succeed");

        assert!(loaded.is_none());
    }

    #[test]
    fn delete_nonexistent_returns_none() {
        let store = test_store("delete-missing-node");

        let deleted = store
            .delete_node(NodeId([77; 16]))
            .expect("missing delete should succeed");

        assert!(deleted.is_none());
    }

    #[test]
    fn nodes_by_file_nonexistent_returns_empty() {
        let store = test_store("nodes-by-file-empty");

        let nodes = store
            .nodes_by_file("service-a", "src/missing.ts")
            .expect("lookup should succeed");

        assert!(nodes.is_empty());
    }

    #[test]
    fn get_outgoing_empty_returns_empty() {
        let store = test_store("outgoing-empty");
        let file = node("service-a", "src/foo.ts", NodeKind::File, "src/foo.ts", 0);
        let function = node("service-a", "src/foo.ts", NodeKind::Function, "execute", 0);

        store
            .bulk_insert(&[file, function.clone()], &[])
            .expect("nodes should insert");

        let outgoing = store
            .get_outgoing(function.id)
            .expect("outgoing lookup should succeed");

        assert!(outgoing.is_empty());
    }

    #[test]
    fn delete_edges_for_owner_nonexistent_is_noop() {
        let store = test_store("delete-owner-noop");
        let file = node("service-a", "src/foo.ts", NodeKind::File, "src/foo.ts", 0);
        let source = node("service-a", "src/foo.ts", NodeKind::Function, "source", 0);
        let target = node("service-a", "src/foo.ts", NodeKind::Function, "target", 1);

        store
            .bulk_insert(
                &[file.clone(), source.clone(), target.clone()],
                &[edge(source.id, target.id, file.id)],
            )
            .expect("batch should insert");

        store
            .delete_edges_for_owner(NodeId([9; 16]))
            .expect("missing owner delete should succeed");

        let outgoing = store
            .get_outgoing(source.id)
            .expect("outgoing lookup should succeed");
        assert_eq!(outgoing, vec![edge(source.id, target.id, file.id)]);
    }

    #[test]
    fn multiple_edges_same_source_target_different_kind_are_distinct() {
        let store = test_store("multi-kind-edges");
        let file = node("service-a", "src/foo.ts", NodeKind::File, "src/foo.ts", 0);
        let source = node("service-a", "src/foo.ts", NodeKind::Function, "source", 0);
        let target = node("service-a", "src/foo.ts", NodeKind::Function, "target", 1);
        let calls = EdgeData {
            kind: EdgeKind::Calls,
            ..edge(source.id, target.id, file.id)
        };
        let imports = EdgeData {
            kind: EdgeKind::Imports,
            ..edge(source.id, target.id, file.id)
        };
        let depends = EdgeData {
            kind: EdgeKind::DependsOn,
            ..edge(source.id, target.id, file.id)
        };

        store
            .bulk_insert(
                &[file, source.clone(), target.clone()],
                &[calls.clone(), imports.clone(), depends.clone()],
            )
            .expect("batch should insert");

        let mut outgoing_kinds = store
            .get_outgoing(source.id)
            .expect("outgoing lookup should succeed")
            .into_iter()
            .map(|edge| edge.kind)
            .collect::<Vec<_>>();
        outgoing_kinds.sort_by_key(|kind| kind.as_u8());
        assert_eq!(outgoing_kinds, vec![calls.kind, imports.kind, depends.kind]);
    }

    #[test]
    fn self_loop_edge_is_supported() {
        let store = test_store("self-loop-edge");
        let file = node("service-a", "src/foo.ts", NodeKind::File, "src/foo.ts", 0);
        let function = node("service-a", "src/foo.ts", NodeKind::Function, "loop", 0);
        let relation = edge(function.id, function.id, file.id);

        store
            .bulk_insert(&[file, function.clone()], std::slice::from_ref(&relation))
            .expect("batch should insert");

        assert_eq!(
            store
                .get_outgoing(function.id)
                .expect("outgoing lookup should succeed"),
            vec![relation.clone()]
        );
        assert_eq!(
            store
                .get_incoming(function.id)
                .expect("incoming lookup should succeed"),
            vec![relation]
        );
    }

    #[test]
    fn bulk_insert_empty_is_noop() {
        let store = test_store("bulk-empty");

        store
            .bulk_insert(&[], &[])
            .expect("empty batch should succeed");

        assert_eq!(store.count_nodes().expect("node count should load"), 0);
        assert_eq!(store.count_edges().expect("edge count should load"), 0);
    }

    #[test]
    fn delete_edges_for_owner_by_kind_only_removes_selected_kinds() {
        let store = test_store("delete-edges-for-owner-by-kind");
        let file = node("service-a", "src/foo.ts", NodeKind::File, "src/foo.ts", 0);
        let source = node("service-a", "src/foo.ts", NodeKind::Function, "source", 0);
        let target = node("service-a", "src/foo.ts", NodeKind::Function, "target", 1);
        let author = NodeData {
            id: gather_step_core::ref_node_id(NodeKind::Author, "alice@example.com"),
            kind: NodeKind::Author,
            repo: "__virtual__".to_owned(),
            file_path: "__authors__/alice@example.com".to_owned(),
            name: "alice@example.com".to_owned(),
            qualified_name: Some("alice@example.com".to_owned()),
            external_id: Some("alice@example.com".to_owned()),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        };

        let calls = edge(source.id, target.id, file.id);
        let owned_by = EdgeData {
            source: file.id,
            target: author.id,
            kind: EdgeKind::OwnedBy,
            metadata: EdgeMetadata::default(),
            owner_file: file.id,
            is_cross_file: true,
        };

        store
            .bulk_insert(
                &[file.clone(), source.clone(), target.clone(), author.clone()],
                &[calls.clone(), owned_by.clone()],
            )
            .expect("batch should insert");

        store
            .delete_edges_for_owner_by_kind(file.id, &[EdgeKind::OwnedBy])
            .expect("filtered delete should succeed");

        assert_eq!(
            store.get_outgoing(source.id).expect("calls should remain"),
            vec![calls]
        );
        assert_eq!(
            store
                .get_outgoing(file.id)
                .expect("owned_by edge list should load")
                .into_iter()
                .filter(|edge| edge.kind == EdgeKind::OwnedBy)
                .count(),
            0
        );
    }

    #[test]
    fn replace_edges_for_owners_by_kind_preserves_unselected_edges() {
        let store = test_store("replace-edges-for-owner-by-kind");
        let file = node("service-a", "src/foo.ts", NodeKind::File, "src/foo.ts", 0);
        let source = node("service-a", "src/foo.ts", NodeKind::Function, "source", 0);
        let target = node("service-a", "src/foo.ts", NodeKind::Function, "target", 1);
        let target_b = node("service-a", "src/bar.ts", NodeKind::Function, "target_b", 0);
        let author_old = NodeData {
            id: gather_step_core::ref_node_id(NodeKind::Author, "alice@example.com"),
            kind: NodeKind::Author,
            repo: "__virtual__".to_owned(),
            file_path: "__authors__/alice@example.com".to_owned(),
            name: "alice@example.com".to_owned(),
            qualified_name: Some("alice@example.com".to_owned()),
            external_id: Some("alice@example.com".to_owned()),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        };
        let author_new = NodeData {
            id: gather_step_core::ref_node_id(NodeKind::Author, "bob@example.com"),
            kind: NodeKind::Author,
            repo: "__virtual__".to_owned(),
            file_path: "__authors__/bob@example.com".to_owned(),
            name: "bob@example.com".to_owned(),
            qualified_name: Some("bob@example.com".to_owned()),
            external_id: Some("bob@example.com".to_owned()),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        };

        let calls = edge(source.id, target.id, file.id);
        let owned_by_old = EdgeData {
            source: file.id,
            target: author_old.id,
            kind: EdgeKind::OwnedBy,
            metadata: EdgeMetadata::default(),
            owner_file: file.id,
            // `is_cross_file` is not stored (S1); decoded edges always return
            // `false`. Use `false` so equality checks against decoded edges pass.
            is_cross_file: false,
        };
        let owned_by_new = EdgeData {
            source: file.id,
            target: author_new.id,
            kind: EdgeKind::OwnedBy,
            metadata: EdgeMetadata::default(),
            owner_file: file.id,
            is_cross_file: false,
        };
        let co_change_new = EdgeData {
            source: file.id,
            target: target_b.id,
            kind: EdgeKind::CoChangesWith,
            metadata: EdgeMetadata::default(),
            owner_file: file.id,
            is_cross_file: false,
        };

        store
            .bulk_insert(
                &[
                    file.clone(),
                    source.clone(),
                    target.clone(),
                    target_b.clone(),
                    author_old.clone(),
                    author_new.clone(),
                ],
                &[calls.clone(), owned_by_old],
            )
            .expect("seed insert should succeed");

        store
            .replace_edges_for_owners_by_kind(
                &[file.id],
                &[EdgeKind::OwnedBy, EdgeKind::CoChangesWith],
                &[owned_by_new.clone(), co_change_new.clone()],
            )
            .expect("selective edge replacement should succeed");

        assert_eq!(
            store.get_outgoing(source.id).expect("calls should remain"),
            vec![calls]
        );

        let file_edges = store.get_outgoing(file.id).expect("file edges should load");
        assert!(file_edges.iter().any(|edge| edge == &owned_by_new));
        assert!(file_edges.iter().any(|edge| edge == &co_change_new));
        assert!(
            file_edges
                .iter()
                .all(|edge| !matches!(edge.kind, EdgeKind::OwnedBy) || edge.target == author_new.id)
        );
    }

    #[test]
    fn insert_node_replaces_stale_indexes_when_shape_changes() {
        let store = test_store("replace-node-indexes");
        let original = pr_node("pull/123", "PR #123");
        let updated = NodeData {
            repo: "backend".to_owned(),
            file_path: "prs/123.json".to_owned(),
            kind: NodeKind::Ticket,
            name: "Issue #123".to_owned(),
            qualified_name: Some("backend::Issue123".to_owned()),
            external_id: Some("issue/123".to_owned()),
            ..original.clone()
        };

        store
            .insert_node(&original)
            .expect("original should insert");
        store.insert_node(&updated).expect("updated should replace");

        assert!(
            store
                .nodes_by_repo("service-a")
                .expect("old repo lookup should succeed")
                .is_empty()
        );
        assert!(
            store
                .nodes_by_file("service-a", "prs")
                .expect("old file lookup should succeed")
                .is_empty()
        );
        assert!(
            store
                .nodes_by_external_id(NodeKind::PR, "pull/123")
                .expect("old external id lookup should succeed")
                .is_empty()
        );
        assert!(
            store
                .nodes_by_type(NodeKind::PR)
                .expect("old type lookup should succeed")
                .is_empty()
        );
        assert_eq!(
            store
                .nodes_by_repo("backend")
                .expect("new repo lookup should succeed"),
            vec![updated.clone()]
        );
        assert_eq!(
            store
                .nodes_by_file("backend", "prs/123.json")
                .expect("new file lookup should succeed"),
            vec![updated.clone()]
        );
        assert_eq!(
            store
                .nodes_by_external_id(NodeKind::Ticket, "issue/123")
                .expect("new external id lookup should succeed"),
            vec![updated.clone()]
        );
        assert_eq!(
            store
                .get_node(updated.id)
                .expect("node load should succeed"),
            Some(updated)
        );
    }

    #[test]
    fn virtual_nodes_are_canonicalized_independent_of_writer_repo() {
        let store = test_store("virtual-node-canonical");
        let first = virtual_topic("producer", "src/producer.ts");
        let second = virtual_topic("consumer", "src/consumer.ts");

        store.insert_node(&first).expect("first should insert");
        store.insert_node(&second).expect("second should insert");

        let loaded = store
            .get_node(first.id)
            .expect("lookup should succeed")
            .expect("node should exist");
        assert_eq!(loaded.repo, "__virtual__");
        assert_eq!(loaded.file_path, "__topic__kafka__order.created");
        assert_eq!(loaded.name, "__topic__kafka__order.created");
        assert!(loaded.span.is_none());
    }

    #[test]
    fn bulk_insert_preserves_edges_on_shared_virtual_nodes() {
        let store = test_store("shared-virtual-edges");
        let producer_file = node(
            "producer",
            "src/producer.ts",
            NodeKind::File,
            "src/producer.ts",
            0,
        );
        let producer_fn = node(
            "producer",
            "src/producer.ts",
            NodeKind::Function,
            "publish",
            1,
        );
        let consumer_file = node(
            "consumer",
            "src/consumer.ts",
            NodeKind::File,
            "src/consumer.ts",
            0,
        );
        let consumer_fn = node(
            "consumer",
            "src/consumer.ts",
            NodeKind::Function,
            "handle",
            1,
        );
        let shared_event = NodeData {
            kind: NodeKind::Event,
            ..virtual_topic("producer", "src/producer.ts")
        };

        let publishes = EdgeData {
            source: producer_fn.id,
            target: shared_event.id,
            kind: EdgeKind::Publishes,
            metadata: EdgeMetadata::default(),
            owner_file: producer_file.id,
            is_cross_file: false,
        };
        let consumes = EdgeData {
            source: consumer_fn.id,
            target: shared_event.id,
            kind: EdgeKind::Consumes,
            metadata: EdgeMetadata::default(),
            owner_file: consumer_file.id,
            is_cross_file: false,
        };

        store
            .bulk_insert(
                &[
                    producer_file.clone(),
                    producer_fn.clone(),
                    shared_event.clone(),
                ],
                std::slice::from_ref(&publishes),
            )
            .expect("producer batch should insert");
        store
            .bulk_insert(
                &[
                    consumer_file.clone(),
                    consumer_fn.clone(),
                    shared_event.clone(),
                ],
                std::slice::from_ref(&consumes),
            )
            .expect("consumer batch should insert");

        let incoming = store
            .get_incoming(shared_event.id)
            .expect("incoming edges should load");
        assert!(incoming.iter().any(|edge| edge.kind == EdgeKind::Publishes));
        assert!(incoming.iter().any(|edge| edge.kind == EdgeKind::Consumes));
    }

    #[test]
    fn deleting_one_owner_edges_preserves_other_shared_virtual_edges() {
        let store = test_store("shared-virtual-owner-delete");
        let producer_file = node(
            "producer",
            "src/producer.ts",
            NodeKind::File,
            "src/producer.ts",
            0,
        );
        let producer_fn = node(
            "producer",
            "src/producer.ts",
            NodeKind::Function,
            "publish",
            1,
        );
        let consumer_file = node(
            "consumer",
            "src/consumer.ts",
            NodeKind::File,
            "src/consumer.ts",
            0,
        );
        let consumer_fn = node(
            "consumer",
            "src/consumer.ts",
            NodeKind::Function,
            "handle",
            1,
        );
        let shared_event = NodeData {
            kind: NodeKind::Event,
            ..virtual_topic("producer", "src/producer.ts")
        };

        let publishes = EdgeData {
            source: producer_fn.id,
            target: shared_event.id,
            kind: EdgeKind::Publishes,
            metadata: EdgeMetadata::default(),
            owner_file: producer_file.id,
            is_cross_file: false,
        };
        let consumes = EdgeData {
            source: consumer_fn.id,
            target: shared_event.id,
            kind: EdgeKind::Consumes,
            metadata: EdgeMetadata::default(),
            owner_file: consumer_file.id,
            is_cross_file: false,
        };

        store
            .bulk_insert(
                &[
                    producer_file.clone(),
                    producer_fn.clone(),
                    consumer_file.clone(),
                    consumer_fn.clone(),
                    shared_event.clone(),
                ],
                &[publishes.clone(), consumes.clone()],
            )
            .expect("initial batch should insert");

        store
            .delete_edges_for_owner(producer_file.id)
            .expect("producer owner edges should delete");

        let shared = store
            .get_node(shared_event.id)
            .expect("shared node lookup should succeed");
        assert!(shared.is_some(), "shared virtual node should remain");

        let incoming = store
            .get_incoming(shared_event.id)
            .expect("incoming edges should load");
        assert!(
            incoming
                .iter()
                .all(|edge| edge.owner_file != producer_file.id),
            "producer-owned edges should be removed: {incoming:?}"
        );
        assert!(
            incoming.iter().any(|edge| {
                edge.kind == EdgeKind::Consumes && edge.owner_file == consumer_file.id
            }),
            "consumer edge must survive owner-specific deletion"
        );
    }

    #[test]
    fn edge_kind_index_tracks_counts() {
        let store = test_store("edge-kind-counts");
        let file = node("service-a", "src/foo.ts", NodeKind::File, "src/foo.ts", 0);
        let source = node("service-a", "src/foo.ts", NodeKind::Function, "source", 0);
        let target = node("service-a", "src/foo.ts", NodeKind::Function, "target", 1);
        let relation = edge(source.id, target.id, file.id);

        store
            .bulk_insert(&[file, source, target], std::slice::from_ref(&relation))
            .expect("batch should insert");

        assert_eq!(
            store
                .count_edges_by_kind(EdgeKind::Calls)
                .expect("edge count should load"),
            1
        );
        assert_eq!(
            store
                .count_nodes_by_kind(NodeKind::Function)
                .expect("node count should load"),
            2
        );

        let read_txn = store.db.begin_read().expect("read txn should open");
        let edge_kind_counts = read_txn
            .open_table(EDGE_KIND_COUNTS)
            .expect("edge kind counts table should open");
        assert_eq!(
            edge_kind_counts
                .get(EdgeKind::Calls.as_u8())
                .expect("lookup should work")
                .map(|count| count.value()),
            Some(1)
        );
    }

    #[test]
    fn delete_node_returns_removed_payload_and_cleans_incident_edges() {
        let store = test_store("delete-node-cascade");
        let file = node("service-a", "src/foo.ts", NodeKind::File, "src/foo.ts", 0);
        let source = node("service-a", "src/foo.ts", NodeKind::Function, "source", 0);
        let target = node("service-a", "src/foo.ts", NodeKind::Function, "target", 1);
        let relation = edge(source.id, target.id, file.id);

        store
            .bulk_insert(
                &[file, source.clone(), target.clone()],
                std::slice::from_ref(&relation),
            )
            .expect("batch should insert");

        let deleted = store
            .delete_node(source.id)
            .expect("delete should succeed")
            .expect("node should exist");

        assert_eq!(deleted, source);
        assert_eq!(
            store.get_node(source.id).expect("lookup should succeed"),
            None
        );
        assert!(
            store
                .get_outgoing(source.id)
                .expect("outgoing should load")
                .is_empty()
        );
        assert!(
            store
                .get_incoming(target.id)
                .expect("incoming should load")
                .is_empty()
        );
        assert_eq!(store.count_edges().expect("edge count should load"), 0);
    }

    #[test]
    fn deleting_file_node_cascades_owned_edges() {
        let store = test_store("delete-file-owner-cascade");
        let file = node("service-a", "src/foo.ts", NodeKind::File, "src/foo.ts", 0);
        let a = node("service-a", "src/foo.ts", NodeKind::Function, "a", 0);
        let b = node("service-a", "src/foo.ts", NodeKind::Function, "b", 1);
        let c = node("service-a", "src/foo.ts", NodeKind::Function, "c", 2);

        store
            .bulk_insert(
                &[file.clone(), a.clone(), b.clone(), c.clone()],
                &[edge(a.id, b.id, file.id), edge(a.id, c.id, file.id)],
            )
            .expect("batch should insert");

        let deleted = store
            .delete_node(file.id)
            .expect("delete should succeed")
            .expect("file should exist");

        assert_eq!(deleted, file);
        assert!(
            store
                .get_outgoing(a.id)
                .expect("outgoing should load")
                .is_empty()
        );
        assert!(
            store
                .get_incoming(b.id)
                .expect("incoming should load")
                .is_empty()
        );
        assert!(
            store
                .get_incoming(c.id)
                .expect("incoming should load")
                .is_empty()
        );
        assert_eq!(store.count_edges().expect("edge count should load"), 0);
    }

    #[test]
    fn delete_edge_removes_it_from_all_indexes() {
        let store = test_store("delete-single-edge");
        let file = node("service-a", "src/foo.ts", NodeKind::File, "src/foo.ts", 0);
        let source = node("service-a", "src/foo.ts", NodeKind::Function, "source", 0);
        let target = node("service-a", "src/foo.ts", NodeKind::Function, "target", 1);
        let relation = edge(source.id, target.id, file.id);

        store
            .bulk_insert(
                &[file, source.clone(), target.clone()],
                std::slice::from_ref(&relation),
            )
            .expect("batch should insert");

        store
            .delete_edge(&relation)
            .expect("edge delete should succeed");

        assert!(
            store
                .get_outgoing(source.id)
                .expect("outgoing should load")
                .is_empty()
        );
        assert!(
            store
                .get_incoming(target.id)
                .expect("incoming should load")
                .is_empty()
        );
        assert_eq!(store.count_edges().expect("edge count should load"), 0);
    }

    // ── split-metric tests ───────────────────────────────────────────────────

    /// Build a minimal virtual author node whose repo is `__virtual__`.
    fn author_node(qualified_name: &str) -> NodeData {
        use gather_step_core::VIRTUAL_NODE_REPO;
        NodeData {
            id: gather_step_core::ref_node_id(NodeKind::Author, qualified_name),
            kind: NodeKind::Author,
            repo: VIRTUAL_NODE_REPO.to_owned(),
            file_path: String::new(),
            name: qualified_name.to_owned(),
            qualified_name: Some(qualified_name.to_owned()),
            external_id: Some(qualified_name.to_owned()),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        }
    }

    /// Build a minimal virtual `SharedSymbol` node whose repo is `__virtual__`.
    fn shared_symbol_node(qualified_name: &str) -> NodeData {
        use gather_step_core::VIRTUAL_NODE_REPO;
        NodeData {
            id: gather_step_core::ref_node_id(NodeKind::SharedSymbol, qualified_name),
            kind: NodeKind::SharedSymbol,
            repo: VIRTUAL_NODE_REPO.to_owned(),
            file_path: String::new(),
            name: qualified_name.to_owned(),
            qualified_name: Some(qualified_name.to_owned()),
            external_id: Some(qualified_name.to_owned()),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        }
    }

    #[test]
    fn count_true_cross_repo_edges_excludes_virtual_targets() {
        let store = test_store("split-true-excl-virt");

        // Two real-repo nodes in different repos with an Imports edge.
        let file_a = node("service-a", "src/a.ts", NodeKind::File, "src/a.ts", 0);
        let file_b = node("service-b", "src/b.ts", NodeKind::File, "src/b.ts", 0);
        let author = author_node("__author__git:alice");

        let import_edge = EdgeData {
            source: file_a.id,
            target: file_b.id,
            kind: EdgeKind::Imports,
            metadata: EdgeMetadata::default(),
            owner_file: file_a.id,
            is_cross_file: true,
        };
        let owned_by_edge = EdgeData {
            source: file_a.id,
            target: author.id,
            kind: EdgeKind::OwnedBy,
            metadata: EdgeMetadata::default(),
            owner_file: file_a.id,
            is_cross_file: false,
        };

        store
            .bulk_insert(&[file_a, file_b, author], &[import_edge, owned_by_edge])
            .expect("seed should insert");

        assert_eq!(
            store
                .count_true_cross_repo_edges()
                .expect("count should succeed"),
            1,
            "only the Imports edge between real repos counts"
        );
        assert_eq!(
            store
                .count_history_ownership_edges()
                .expect("count should succeed"),
            1,
            "the OwnedBy → __virtual__/Author counts as ownership"
        );
        assert_eq!(
            store
                .count_virtual_other_cross_repo_edges()
                .expect("count should succeed"),
            0,
            "no SharedSymbol or Route targets were added"
        );
        assert_eq!(
            store
                .count_cross_repo_edges()
                .expect("count should succeed"),
            2,
            "total must equal 2"
        );
    }

    #[test]
    fn split_metrics_reconcile_with_total() {
        let store = test_store("split-reconcile");

        let file_a = node("repo-a", "src/a.ts", NodeKind::File, "src/a.ts", 0);
        let file_b = node("repo-b", "src/b.ts", NodeKind::File, "src/b.ts", 0);
        let author = author_node("__author__git:bob");
        let sym = shared_symbol_node("__shared__@pkg@2.0.0__Sym");

        let import_edge = EdgeData {
            source: file_a.id,
            target: file_b.id,
            kind: EdgeKind::Imports,
            metadata: EdgeMetadata::default(),
            owner_file: file_a.id,
            is_cross_file: true,
        };
        let owned_by_edge = EdgeData {
            source: file_a.id,
            target: author.id,
            kind: EdgeKind::OwnedBy,
            metadata: EdgeMetadata::default(),
            owner_file: file_a.id,
            is_cross_file: false,
        };
        let sym_edge = EdgeData {
            source: file_b.id,
            target: sym.id,
            kind: EdgeKind::Imports,
            metadata: EdgeMetadata::default(),
            owner_file: file_b.id,
            is_cross_file: false,
        };

        store
            .bulk_insert(
                &[file_a, file_b, author, sym],
                &[import_edge, owned_by_edge, sym_edge],
            )
            .expect("seed should insert");

        let true_count = store
            .count_true_cross_repo_edges()
            .expect("true count should succeed");
        let own_count = store
            .count_history_ownership_edges()
            .expect("ownership count should succeed");
        let virt_other = store
            .count_virtual_other_cross_repo_edges()
            .expect("virtual-other count should succeed");
        let total = store
            .count_cross_repo_edges()
            .expect("total count should succeed");

        assert_eq!(
            true_count + own_count + virt_other,
            usize::try_from(total).expect("total fits in usize"),
            "split counts must sum to count_cross_repo_edges"
        );
    }

    #[test]
    fn count_history_ownership_edges_counts_only_author_kind_targets() {
        let store = test_store("split-author-only");

        let file_a = node("repo-a", "src/a.ts", NodeKind::File, "src/a.ts", 0);
        let author = author_node("__author__git:carol");
        let sym = shared_symbol_node("__shared__@pkg@2.0.0__Widget");

        let owned_by_edge = EdgeData {
            source: file_a.id,
            target: author.id,
            kind: EdgeKind::OwnedBy,
            metadata: EdgeMetadata::default(),
            owner_file: file_a.id,
            is_cross_file: false,
        };
        let sym_ref_edge = EdgeData {
            source: file_a.id,
            target: sym.id,
            kind: EdgeKind::Imports,
            metadata: EdgeMetadata::default(),
            owner_file: file_a.id,
            is_cross_file: false,
        };

        store
            .bulk_insert(&[file_a, author, sym], &[owned_by_edge, sym_ref_edge])
            .expect("seed should insert");

        assert_eq!(
            store
                .count_history_ownership_edges()
                .expect("ownership count should succeed"),
            1,
            "only the OwnedBy → Author edge"
        );
        assert_eq!(
            store
                .count_virtual_other_cross_repo_edges()
                .expect("virtual-other count should succeed"),
            1,
            "only the Imports → SharedSymbol edge"
        );
    }

    #[test]
    fn count_edge_summary_matches_individual_counters() {
        // The summary must return identical values to the five individual
        // counter methods so callers can safely replace five sequential scans
        // with one.
        let store = test_store("edge-summary-parity");

        let file_a = node("repo-a", "src/a.ts", NodeKind::File, "src/a.ts", 0);
        let file_b = node("repo-b", "src/b.ts", NodeKind::File, "src/b.ts", 0);
        let author = author_node("__author__git:summary-alice");
        let sym = shared_symbol_node("__shared__@pkg@3.0.0__Ctrl");

        let import_edge = EdgeData {
            source: file_a.id,
            target: file_b.id,
            kind: EdgeKind::Imports,
            metadata: EdgeMetadata::default(),
            owner_file: file_a.id,
            is_cross_file: true,
        };
        let owned_by_edge = EdgeData {
            source: file_a.id,
            target: author.id,
            kind: EdgeKind::OwnedBy,
            metadata: EdgeMetadata::default(),
            owner_file: file_a.id,
            is_cross_file: false,
        };
        let sym_edge = EdgeData {
            source: file_b.id,
            target: sym.id,
            kind: EdgeKind::Imports,
            metadata: EdgeMetadata::default(),
            owner_file: file_b.id,
            is_cross_file: false,
        };
        let self_edge = EdgeData {
            source: file_a.id,
            target: file_a.id,
            kind: EdgeKind::Defines,
            metadata: EdgeMetadata::default(),
            owner_file: file_a.id,
            is_cross_file: false,
        };

        store
            .bulk_insert(
                &[file_a, file_b, author, sym],
                &[import_edge, owned_by_edge, sym_edge, self_edge],
            )
            .expect("seed should insert");

        let summary = store.count_edge_summary().expect("summary should succeed");

        assert_eq!(
            summary.total_edges,
            store.count_edges().expect("edge count should succeed"),
            "total_edges must match count_edges()"
        );
        assert_eq!(
            summary.cross_repo_edges,
            store
                .count_cross_repo_edges()
                .expect("cross-repo count should succeed"),
            "cross_repo_edges must match count_cross_repo_edges()"
        );
        assert_eq!(
            summary.true_cross_repo_edges,
            store
                .count_true_cross_repo_edges()
                .expect("true cross-repo count should succeed"),
            "true_cross_repo_edges must match count_true_cross_repo_edges()"
        );
        assert_eq!(
            summary.history_ownership_edges,
            store
                .count_history_ownership_edges()
                .expect("ownership count should succeed"),
            "history_ownership_edges must match count_history_ownership_edges()"
        );
        assert_eq!(
            summary.virtual_other_cross_repo_edges,
            store
                .count_virtual_other_cross_repo_edges()
                .expect("virtual-other count should succeed"),
            "virtual_other_cross_repo_edges must match count_virtual_other_cross_repo_edges()"
        );

        // The three split counters must sum to the total cross-repo count.
        assert_eq!(
            summary.true_cross_repo_edges
                + summary.history_ownership_edges
                + summary.virtual_other_cross_repo_edges,
            usize::try_from(summary.cross_repo_edges).expect("cross_repo_edges fits in usize"),
            "split counts must sum to cross_repo_edges"
        );
    }

    #[test]
    fn bulk_insert_is_atomic_when_edge_validation_fails() {
        let store = test_store("bulk-insert-atomic-invalid-edge");
        let file = node("service-a", "src/foo.ts", NodeKind::File, "src/foo.ts", 0);
        let source = node("service-a", "src/foo.ts", NodeKind::Function, "source", 0);
        let target = node("service-a", "src/foo.ts", NodeKind::Function, "target", 1);
        let baseline = edge(source.id, target.id, file.id);

        store
            .bulk_insert(
                &[file.clone(), source.clone(), target.clone()],
                std::slice::from_ref(&baseline),
            )
            .expect("baseline batch should insert");

        let replacement = NodeData {
            name: "source_updated".to_owned(),
            qualified_name: Some("service-a::source_updated".to_owned()),
            ..source.clone()
        };
        let missing = NodeId([5; 16]);
        let invalid = edge(replacement.id, missing, file.id);
        let result = store.bulk_insert(&[replacement], std::slice::from_ref(&invalid));

        assert!(matches!(
            result,
            Err(super::GraphStoreError::MissingNode(node_id)) if node_id == missing
        ));
        assert_eq!(
            store
                .get_node(source.id)
                .expect("source lookup should succeed"),
            Some(source.clone())
        );
        assert_eq!(
            store.get_outgoing(source.id).expect("outgoing should load"),
            vec![baseline]
        );
    }
}
