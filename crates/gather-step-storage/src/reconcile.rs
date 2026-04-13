use std::path::Path;

use gather_step_core::{EdgeData, NodeKind, PathId, node_id};
use gather_step_parser::resolve_calls_with_unresolved;
use rusqlite::params;
use rustc_hash::{FxHashMap, FxHashSet};
use thiserror::Error;

use crate::{
    GraphStoreDb, GraphStoreError, StorageCoordinator, StorageCoordinatorError,
    incremental::TrackedPath, metadata::MetadataStoreError,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReconcileStats {
    pub files_processed: usize,
    pub dependency_rows_written: usize,
    pub semantic_peer_files_affected: usize,
    pub dangling_edges_removed: usize,
    /// Subset of `dangling_edges_removed` that targeted semantic
    /// bridges (`Serves` / `Consumes` / `Publishes` / `References` /
    /// `Implements` / `UsesShared` / `ContractOn` / `DriftsFrom` /
    /// `PropagatesEvent`). Reported separately so operators can distinguish
    /// semantic-link rot from plain call-edge churn.
    pub semantic_dangling_edges_removed: usize,
    pub unresolved_inputs_scanned: usize,
    pub unresolved_calls_resolved: usize,
    pub unresolved_calls_remaining: usize,
}

/// Per-edge-class cleanup counts returned alongside the total in
/// [`cleanup_dangling_edges_for_files`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DanglingCleanup {
    pub total: usize,
    pub semantic: usize,
}

#[derive(Debug, Error)]
pub enum StorageReconcileError {
    #[error(transparent)]
    Graph(#[from] GraphStoreError),
    #[error(transparent)]
    Metadata(#[from] MetadataStoreError),
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Coordinator(#[from] StorageCoordinatorError),
}

pub fn reconcile_changed_files(
    coordinator: &StorageCoordinator,
    repo: &str,
    changed_files: &[TrackedPath],
) -> Result<ReconcileStats, StorageReconcileError> {
    reconcile_changed_files_with_mode(coordinator, repo, changed_files, false)
}

/// When `cold_index` is true, skip work that is redundant on a fresh full
/// build: unresolved-call reconciliation (no prior unresolved state),
/// dangling-edge cleanup (no stale edges), semantic bridge traversal (no
/// prior bridge state), and pack invalidation (no stale packs).
pub fn reconcile_changed_files_with_mode(
    coordinator: &StorageCoordinator,
    repo: &str,
    changed_files: &[TrackedPath],
    cold_index: bool,
) -> Result<ReconcileStats, StorageReconcileError> {
    // Open a single redb read transaction for all graph reads in the reconcile
    // pass. This eliminates ~180K+ tiny read transactions that previously
    // opened/closed per helper call.
    let read_txn = coordinator.graph().begin_read_txn()?;

    // Step 1: gather outgoing file-level dependencies from the graph store.
    // Graph reads stay OUTSIDE the SQLite write transaction so we don't hold
    // the metadata writer mutex while doing redb work. `deps_by_file[i]` lines
    // up with `changed_files[i]` for the write phase below.
    let mut deps_by_file: Vec<FxHashSet<(String, String)>> =
        Vec::with_capacity(changed_files.len());
    for file_path in changed_files {
        let mut dependencies: FxHashSet<(String, String)> = FxHashSet::default();
        for node in GraphStoreDb::nodes_by_file_in_read_txn(&read_txn, repo, &file_path.path)? {
            for edge in GraphStoreDb::get_outgoing_in_read_txn(&read_txn, node.id)? {
                let Some(target) = GraphStoreDb::get_node_in_read_txn(&read_txn, edge.target)?
                else {
                    continue;
                };
                if target.repo != repo || target.file_path != file_path.path {
                    dependencies.insert((target.repo, target.file_path));
                }
            }
        }
        deps_by_file.push(dependencies);
    }

    let mut same_repo_path_ids: FxHashMap<String, Option<Vec<u8>>> = FxHashMap::default();
    let mut record_same_repo_path =
        |display: String, path_id_bytes: Vec<u8>| match same_repo_path_ids.get(&display) {
            None => {
                same_repo_path_ids.insert(display, Some(path_id_bytes));
            }
            Some(Some(existing)) if *existing == path_id_bytes => {}
            Some(_) => {
                same_repo_path_ids.insert(display, None);
            }
        };
    for state in coordinator.metadata().file_index_states_by_repo(repo)? {
        record_same_repo_path(
            state.file_path.clone(),
            state.effective_path_bytes().to_vec(),
        );
    }
    for file in changed_files {
        record_same_repo_path(file.path.clone(), file.path_id_bytes.clone());
    }

    // Step 2: apply all SQLite writes atomically through the metadata store's
    // configured writer connection. `with_write_txn` routes through the writer
    // mutex (serialising against other metadata writers) and uses the same
    // WAL/busy_timeout pragmas as the rest of the metadata store — replacing
    // the previous raw `Connection::open` that bypassed both.
    let (files_processed, dependency_rows_written) = coordinator
        .metadata()
        .with_write_txn(|tx| {
            let mut files_processed = 0_usize;
            let mut rows_written = 0_usize;
            for (file_path, dependencies) in changed_files.iter().zip(deps_by_file.iter()) {
                tx.execute(
                    "DELETE FROM file_dependencies WHERE source_repo = ?1 AND source_path = ?2",
                    params![repo, &file_path.path_id_bytes],
                )?;
                for (target_repo, target_path) in dependencies {
                    let target_path_bytes = if target_repo == repo {
                        same_repo_path_ids
                            .get(target_path)
                            .and_then(std::clone::Clone::clone)
                            .unwrap_or_else(|| target_path.as_bytes().to_vec())
                    } else {
                        target_path.as_bytes().to_vec()
                    };
                    tx.execute(
                        "INSERT INTO file_dependencies(source_repo, source_path, target_repo, target_path, edge_count)
                         VALUES (?1, ?2, ?3, ?4, 1)
                         ON CONFLICT(source_repo, source_path, target_repo, target_path)
                         DO UPDATE SET edge_count = excluded.edge_count",
                        params![repo, &file_path.path_id_bytes, target_repo, target_path_bytes],
                    )?;
                    rows_written += 1;
                }
                files_processed += 1;
            }
            Ok((files_processed, rows_written))
        })?;

    // On a cold full-index, skip unresolved-call reconciliation, semantic
    // bridge traversal, pack invalidation, and dangling-edge cleanup — none
    // of these have prior state to reconcile against.
    if cold_index {
        return Ok(ReconcileStats {
            files_processed,
            dependency_rows_written,
            ..ReconcileStats::default()
        });
    }

    let (
        affected_unresolved_files,
        unresolved_inputs_scanned,
        unresolved_calls_resolved,
        unresolved_calls_remaining,
    ) = reconcile_unresolved_calls(coordinator, &read_txn, repo, changed_files)?;
    let base_cleanup_targets: FxHashSet<String> = changed_files
        .iter()
        .map(|file| file.path.clone())
        .chain(affected_unresolved_files)
        .collect();
    let semantic_related_files = semantic_bridge_related_files_in_read_txn(
        &read_txn,
        repo,
        &changed_files
            .iter()
            .map(|file| file.path.clone())
            .collect::<Vec<_>>(),
    )?;
    let semantic_related_same_repo: FxHashSet<String> = semantic_related_files
        .iter()
        .filter(|(related_repo, _)| related_repo == repo)
        .map(|(_, file_path)| file_path.clone())
        .collect();
    let semantic_peer_files_affected = semantic_related_same_repo
        .difference(&base_cleanup_targets)
        .count();
    let cleanup_targets: Vec<String> = base_cleanup_targets
        .into_iter()
        .chain(semantic_related_same_repo)
        .collect();
    let invalidation_targets: Vec<(String, String)> = cleanup_targets
        .iter()
        .cloned()
        .map(|file_path| (repo.to_owned(), file_path))
        .chain(semantic_related_files)
        .collect::<FxHashSet<_>>()
        .into_iter()
        .collect();
    let _invalidated_packs = coordinator
        .metadata()
        .invalidate_context_packs_for_targets(&invalidation_targets)?;
    let dangling =
        cleanup_dangling_edges_classified(coordinator.graph(), &read_txn, repo, &cleanup_targets)?;
    Ok(ReconcileStats {
        files_processed,
        dependency_rows_written,
        semantic_peer_files_affected,
        dangling_edges_removed: dangling.total,
        semantic_dangling_edges_removed: dangling.semantic,
        unresolved_inputs_scanned,
        unresolved_calls_resolved,
        unresolved_calls_remaining,
    })
}

fn reconcile_unresolved_calls(
    coordinator: &StorageCoordinator,
    read_txn: &redb::ReadTransaction,
    repo: &str,
    changed_files: &[TrackedPath],
) -> Result<(Vec<String>, usize, usize, usize), StorageReconcileError> {
    let candidate_keys = changed_symbol_keys(read_txn, repo, changed_files)?;
    if candidate_keys.is_empty() {
        let remaining = coordinator
            .metadata()
            .unresolved_resolution_input_count_by_repo(repo)?;
        return Ok((Vec::new(), 0, 0, remaining));
    }

    let unresolved = coordinator
        .metadata()
        .unresolved_resolution_inputs_by_candidate_keys(repo, &candidate_keys)?;
    if unresolved.is_empty() {
        let remaining = coordinator
            .metadata()
            .unresolved_resolution_input_count_by_repo(repo)?;
        return Ok((Vec::new(), 0, 0, remaining));
    }
    let symbols = GraphStoreDb::nodes_by_candidate_keys_in_read_txn(read_txn, &candidate_keys)?
        .into_iter()
        .filter(|node| node.repo == repo)
        .collect::<Vec<_>>();
    let outcome = resolve_calls_with_unresolved(Path::new(""), &symbols, &unresolved);

    // Batch all resolved edge inserts into a single write transaction.
    // Use the per-edge validated path rather than the bulk path so a
    // MissingNode on one edge does not abort the whole batch — reconcile
    // may run against a graph where some referenced nodes were purged by
    // concurrent file changes.
    if !outcome.resolved.is_empty() {
        let edges: Vec<&EdgeData> = outcome.resolved.iter().map(|r| &r.edge).collect();
        coordinator.graph().with_write_txn(|write_txn| {
            for edge in &edges {
                GraphStoreDb::insert_edge_validated_in_txn(write_txn, edge)?;
            }
            Ok(())
        })?;
    }

    // Build PathId byte keys for the metadata store's delete/insert cycle.
    // Using raw OsStr bytes ensures the DELETE BLOB key matches the INSERT
    // BLOB key that was written at parse time (via PathId::from_path).
    // A separate lossy display Vec<String> is returned for the callers that
    // need string paths for graph-store / reconcile bookkeeping.
    let affected_path_ids: Vec<Vec<u8>> = unresolved
        .iter()
        .map(|input| PathId::from_path(&input.file_path).as_bytes().to_vec())
        .collect();
    let affected_file_paths: Vec<String> = unresolved
        .iter()
        .map(|input| input.file_path.to_string_lossy().replace('\\', "/"))
        .collect();
    coordinator
        .metadata()
        .replace_unresolved_resolution_inputs_for_files(
            repo,
            &affected_path_ids,
            &outcome.unresolved,
        )?;

    Ok((
        affected_file_paths,
        unresolved.len(),
        outcome.resolved.len(),
        coordinator
            .metadata()
            .unresolved_resolution_input_count_by_repo(repo)?,
    ))
}

fn changed_symbol_keys(
    read_txn: &redb::ReadTransaction,
    repo: &str,
    changed_files: &[TrackedPath],
) -> Result<Vec<String>, StorageReconcileError> {
    let mut keys: FxHashSet<String> = FxHashSet::default();
    for file_path in changed_files {
        for node in GraphStoreDb::nodes_by_file_in_read_txn(read_txn, repo, &file_path.path)? {
            if matches!(
                node.kind,
                gather_step_core::NodeKind::Function
                    | gather_step_core::NodeKind::Class
                    | gather_step_core::NodeKind::Type
                    | gather_step_core::NodeKind::Entity
            ) {
                keys.insert(node.name);
            }
        }
    }
    Ok(keys.into_iter().collect())
}

pub fn cleanup_dangling_edges_for_files(
    graph: &GraphStoreDb,
    repo: &str,
    file_paths: &[String],
) -> Result<usize, StorageReconcileError> {
    let read_txn = graph.begin_read_txn()?;
    Ok(cleanup_dangling_edges_classified(graph, &read_txn, repo, file_paths)?.total)
}

/// Like [`cleanup_dangling_edges_for_files`] but returns a `(total, semantic)`
/// split so callers can report semantic-link health separately from
/// the plain call-edge cleanup count.
///
/// Reads are performed through the supplied `read_txn`; dangling edge deletes
/// are batched into a single write transaction at the end.
pub fn cleanup_dangling_edges_classified(
    graph: &GraphStoreDb,
    read_txn: &redb::ReadTransaction,
    repo: &str,
    file_paths: &[String],
) -> Result<DanglingCleanup, StorageReconcileError> {
    let mut stats = DanglingCleanup::default();
    let mut seen: FxHashSet<(
        gather_step_core::NodeId,
        gather_step_core::NodeId,
        u8,
        gather_step_core::NodeId,
    )> = FxHashSet::default();
    let mut edges_to_delete = Vec::new();

    for file_path in file_paths {
        for edge in
            GraphStoreDb::edges_by_owner_in_read_txn(read_txn, file_node_id(repo, file_path))?
        {
            let key = (edge.source, edge.target, edge.kind.as_u8(), edge.owner_file);
            if !seen.insert(key) {
                continue;
            }
            if GraphStoreDb::get_node_in_read_txn(read_txn, edge.target)?.is_none() {
                let is_semantic = edge.kind.is_semantic_bridge();
                edges_to_delete.push(edge);
                stats.total += 1;
                if is_semantic {
                    stats.semantic += 1;
                }
            }
        }
    }

    // Batch all dangling edge deletes into a single write transaction.
    if !edges_to_delete.is_empty() {
        graph.with_write_txn(|write_txn| {
            for edge in &edges_to_delete {
                GraphStoreDb::delete_edge_in_txn(write_txn, edge)?;
            }
            Ok(())
        })?;
    }

    Ok(stats)
}

fn file_node_id(repo: &str, file_path: &str) -> gather_step_core::NodeId {
    node_id(repo, file_path, NodeKind::File, file_path)
}

/// Variant that accepts an existing read transaction (used inside
/// `reconcile_changed_files` where one read txn covers the entire pass).
pub(crate) fn semantic_bridge_related_files_in_read_txn(
    read_txn: &redb::ReadTransaction,
    repo: &str,
    file_paths: &[String],
) -> Result<Vec<(String, String)>, GraphStoreError> {
    let mut bridge_nodes: FxHashSet<gather_step_core::NodeId> = FxHashSet::default();
    for file_path in file_paths {
        for edge in
            GraphStoreDb::edges_by_owner_in_read_txn(read_txn, file_node_id(repo, file_path))?
        {
            if !edge.kind.is_semantic_bridge() {
                continue;
            }
            if let Some(bridge_node_id) = semantic_bridge_node_id_in_read_txn(read_txn, &edge)? {
                bridge_nodes.insert(bridge_node_id);
            }
        }
    }

    let mut related_files: FxHashSet<(String, String)> = FxHashSet::default();
    for bridge_node_id in bridge_nodes {
        for edge in GraphStoreDb::get_incoming_in_read_txn(read_txn, bridge_node_id)?
            .into_iter()
            .chain(GraphStoreDb::get_outgoing_in_read_txn(
                read_txn,
                bridge_node_id,
            )?)
        {
            if !edge.kind.is_semantic_bridge() {
                continue;
            }
            let Some(owner_file) = GraphStoreDb::get_node_in_read_txn(read_txn, edge.owner_file)?
            else {
                continue;
            };
            if owner_file.kind == NodeKind::File {
                related_files.insert((owner_file.repo, owner_file.file_path));
            }
        }
    }

    Ok(related_files.into_iter().collect())
}

/// Original entry point for callers that don't hold a read transaction.
pub(crate) fn semantic_bridge_related_files(
    graph: &GraphStoreDb,
    repo: &str,
    file_paths: &[String],
) -> Result<Vec<(String, String)>, GraphStoreError> {
    let read_txn = graph.begin_read_txn()?;
    semantic_bridge_related_files_in_read_txn(&read_txn, repo, file_paths)
}

fn semantic_bridge_node_id_in_read_txn(
    read_txn: &redb::ReadTransaction,
    edge: &gather_step_core::EdgeData,
) -> Result<Option<gather_step_core::NodeId>, GraphStoreError> {
    let Some(target) = GraphStoreDb::get_node_in_read_txn(read_txn, edge.target)? else {
        return Ok(None);
    };
    if is_semantic_bridge_node(&target) {
        return Ok(Some(edge.target));
    }

    let Some(source) = GraphStoreDb::get_node_in_read_txn(read_txn, edge.source)? else {
        return Ok(None);
    };
    if is_semantic_bridge_node(&source) {
        return Ok(Some(edge.source));
    }

    Ok(None)
}

fn is_semantic_bridge_node(node: &gather_step_core::NodeData) -> bool {
    node.is_virtual
        || matches!(
            node.kind,
            NodeKind::Route
                | NodeKind::Topic
                | NodeKind::Queue
                | NodeKind::Subject
                | NodeKind::Stream
                | NodeKind::Event
                | NodeKind::SharedSymbol
                | NodeKind::PayloadContract
        )
}
