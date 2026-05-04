use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Read as _,
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};

use rustc_hash::FxHashMap;

use gather_step_core::PathId;
use gather_step_parser::{
    FileEntry as SourceFileEntry, FileStat, TraverseConfig, TraverseError, classify_language,
    collect_selected_repo_files,
};
use ignore::WalkBuilder;
use thiserror::Error;

use crate::{
    MetadataStoreDb,
    metadata::{MetadataStoreError, stored_content_hash},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IncrementalFileEntry {
    pub path: String,
    pub path_id_bytes: Vec<u8>,
    pub content_hash: Vec<u8>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct TrackedPath {
    pub path: String,
    pub path_id_bytes: Vec<u8>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChangedSet {
    pub added: Vec<IncrementalFileEntry>,
    pub modified: Vec<IncrementalFileEntry>,
    pub deleted: Vec<IncrementalFileEntry>,
    pub unchanged_count: usize,
}

#[derive(Clone, Debug, Default)]
pub struct RepoSnapshot {
    pub source_files: Vec<SourceFileEntry>,
    /// Maps raw `OsStr` bytes of each file path to its filesystem metadata.
    /// No ordering required — lookups are pure key→value; `FxHashMap`
    /// avoids the O(log n) `BTreeMap` overhead at index time.
    pub file_stats: FxHashMap<Vec<u8>, FileStat>,
    /// Maps raw `OsStr` bytes of each file path to its entry.  Keying on
    /// bytes rather than the lossy UTF-8 display string ensures two
    /// byte-distinct non-UTF-8 filenames are never collapsed into one entry.
    /// The display string is still available via `IncrementalFileEntry::path`.
    pub files_by_path: BTreeMap<Vec<u8>, IncrementalFileEntry>,
}

#[derive(Debug, Error)]
pub enum IncrementalError {
    #[error(transparent)]
    Metadata(#[from] MetadataStoreError),
    #[error(transparent)]
    Traverse(#[from] TraverseError),
    #[error("failed to read manifest {path}: {source}")]
    ReadManifest {
        path: String,
        #[source]
        source: std::io::Error,
    },
}

pub fn snapshot_repo_files(
    metadata: &MetadataStoreDb,
    repo: &str,
    repo_root: &Path,
    traverse: &TraverseConfig,
) -> Result<RepoSnapshot, IncrementalError> {
    let root_metadata =
        fs::symlink_metadata(repo_root).map_err(|source| TraverseError::ReadPath {
            path: repo_root.to_path_buf(),
            source,
        })?;
    if root_metadata.file_type().is_symlink() {
        return Err(TraverseError::SymlinkRoot {
            root: repo_root.to_path_buf(),
        }
        .into());
    }

    let stored_states = metadata
        .file_index_states_by_repo(repo)?
        .into_iter()
        .map(|state| (state.effective_path_bytes().to_vec(), state))
        .collect::<BTreeMap<_, _>>();

    let mut builder = WalkBuilder::new(repo_root);
    builder.hidden(!traverse.include_dotfiles());
    builder.follow_links(false);
    builder.git_ignore(true);
    builder.git_exclude(true);
    builder.git_global(true);
    builder.require_git(false);

    let mut source_files = Vec::new();
    let mut file_stats = FxHashMap::default();
    let mut files_by_path = BTreeMap::new();

    for entry_result in builder.build() {
        let entry = entry_result.map_err(|source| TraverseError::Walk {
            root: repo_root.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if path == repo_root || entry.file_type().is_some_and(|kind| kind.is_dir()) {
            continue;
        }
        if entry.path_is_symlink() {
            continue;
        }

        let relative_path = match path.strip_prefix(repo_root) {
            Ok(rel) => rel.to_path_buf(),
            Err(_) => continue,
        };
        let Some(language) = classify_language(&relative_path) else {
            continue;
        };
        if !traverse.is_index_relevant_path(&relative_path) {
            continue;
        }

        let file_metadata = fs::metadata(path).map_err(|source| TraverseError::ReadPath {
            path: path.to_path_buf(),
            source,
        })?;
        if file_metadata.len() > traverse.max_file_size_bytes() {
            continue;
        }

        let path_id_bytes = PathId::from_path(&relative_path).as_bytes().to_vec();
        let size_bytes = i64::try_from(file_metadata.len()).unwrap_or(i64::MAX);
        let mtime_ns = metadata_mtime_ns(&file_metadata);

        // Determine the content hash and the stat that describes the hashed
        // bytes.  The stat recorded in `file_stats` must match the bytes that
        // produced `content_hash`; using the pre-hash stat from a separate
        // `fs::metadata` call risks pairing a stale stat with freshly-read
        // bytes if the file is modified between the two syscalls.
        let (content_hash, consistent_stat) =
            if let Some(existing) = stored_states.get(&path_id_bytes) {
                if existing.size_bytes == size_bytes && existing.mtime_ns == mtime_ns {
                    // Fast path: the stored stat matches the current stat.
                    // Reuse the stored hash; the current pre-hash stat is a
                    // valid description of the bytes we would read right now
                    // (nothing changed).
                    if let Some(hash) = bytes_to_hash32(&existing.content_hash) {
                        (
                            hash,
                            FileStat {
                                size_bytes,
                                mtime_ns,
                            },
                        )
                    } else {
                        // Stored hash is malformed — re-hash with a stable read.
                        let Some((hash, stat)) = read_hashed_source_stable(path)? else {
                            continue;
                        };
                        (hash, stat)
                    }
                } else {
                    // Stat changed — hash the file; use the stat that matches
                    // the bytes we actually read.
                    let Some((hash, stat)) = read_hashed_source_stable(path)? else {
                        continue;
                    };
                    (hash, stat)
                }
            } else {
                // New file — hash it and record the consistent stat.
                let Some((hash, stat)) = read_hashed_source_stable(path)? else {
                    continue;
                };
                (hash, stat)
            };

        let path_display = relative_path.to_string_lossy().replace('\\', "/");
        source_files.push(SourceFileEntry {
            path: relative_path,
            language,
            size_bytes: u64::try_from(consistent_stat.size_bytes).unwrap_or(u64::MAX),
            content_hash,
            source_bytes: None,
        });
        files_by_path.insert(
            path_id_bytes.clone(),
            IncrementalFileEntry {
                path: path_display,
                path_id_bytes: path_id_bytes.clone(),
                content_hash: content_hash.to_vec(),
            },
        );
        file_stats.insert(path_id_bytes, consistent_stat);
    }

    if let Some(manifest_entry) = snapshot_manifest_entry(repo_root, &stored_states)? {
        files_by_path.insert(manifest_entry.path_id_bytes.clone(), manifest_entry);
    }

    source_files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(RepoSnapshot {
        source_files,
        file_stats,
        files_by_path,
    })
}

pub fn snapshot_selected_repo_files(
    repo_root: &Path,
    paths: &[String],
    traverse: &TraverseConfig,
) -> Result<RepoSnapshot, IncrementalError> {
    let selected_paths = paths.iter().map(PathBuf::from).collect::<Vec<_>>();
    let traversal = collect_selected_repo_files(repo_root, &selected_paths, traverse)?;
    let include_manifest = paths.iter().any(|path| path == "package.json");
    snapshot_from_traversal(
        repo_root,
        traversal.files,
        traversal.file_stats,
        include_manifest,
    )
}

fn snapshot_from_traversal(
    repo_root: &Path,
    source_files: Vec<SourceFileEntry>,
    file_stats: FxHashMap<Vec<u8>, FileStat>,
    include_manifest: bool,
) -> Result<RepoSnapshot, IncrementalError> {
    let mut files_by_path: BTreeMap<Vec<u8>, IncrementalFileEntry> = BTreeMap::new();
    for file in &source_files {
        // Key on raw OsStr bytes so two byte-distinct non-UTF-8 filenames
        // (e.g. b"bad-\xff.ts" and b"bad-\xfe.ts") are not collapsed to the
        // same map entry under to_string_lossy (which maps both to
        // "bad-\u{FFFD}.ts").
        let path_key = PathId::from_path(&file.path).as_bytes().to_vec();
        let path_display = file.path.to_string_lossy().replace('\\', "/");
        files_by_path.insert(
            path_key,
            IncrementalFileEntry {
                path: path_display,
                path_id_bytes: PathId::from_path(&file.path).as_bytes().to_vec(),
                content_hash: file.content_hash.to_vec(),
            },
        );
    }

    if include_manifest && let Some(bytes) = read_manifest_bytes(repo_root)? {
        files_by_path.insert(
            b"package.json".to_vec(),
            IncrementalFileEntry {
                path: "package.json".to_owned(),
                path_id_bytes: b"package.json".to_vec(),
                content_hash: blake3::hash(&bytes).as_bytes().to_vec(),
            },
        );
    }

    Ok(RepoSnapshot {
        source_files,
        file_stats,
        files_by_path,
    })
}

fn snapshot_manifest_entry(
    repo_root: &Path,
    stored_states: &BTreeMap<Vec<u8>, crate::FileIndexState>,
) -> Result<Option<IncrementalFileEntry>, IncrementalError> {
    let manifest_path = repo_root.join("package.json");
    let metadata = match fs::symlink_metadata(&manifest_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(IncrementalError::ReadManifest {
                path: manifest_path.display().to_string(),
                source,
            });
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Ok(None);
    }

    let path_id_bytes = b"package.json".to_vec();
    let size_bytes = i64::try_from(metadata.len()).unwrap_or(i64::MAX);
    let mtime_ns = metadata_mtime_ns(&metadata);
    let content_hash = if let Some(existing) = stored_states.get(&path_id_bytes) {
        if existing.size_bytes == size_bytes && existing.mtime_ns == mtime_ns {
            existing.content_hash.clone()
        } else {
            read_manifest_bytes(repo_root)?
                .map(|bytes| blake3::hash(&bytes).as_bytes().to_vec())
                .unwrap_or_default()
        }
    } else {
        read_manifest_bytes(repo_root)?
            .map(|bytes| blake3::hash(&bytes).as_bytes().to_vec())
            .unwrap_or_default()
    };

    Ok(Some(IncrementalFileEntry {
        path: "package.json".to_owned(),
        path_id_bytes,
        content_hash,
    }))
}

/// Read, hash, and stat a source file atomically enough to be safe for
/// incremental-cache writes.
///
/// The file is opened once; the pre-read `FileStat` is obtained from the open
/// file descriptor (`fstat`), the bytes are read on the same descriptor, and
/// the stat is checked again after the read.  If the stat changed during the
/// read (i.e. another process modified the file while we were reading it), the
/// operation is retried up to `MAX_RETRIES` more times.  If the stat remains
/// unstable after all retries, `Ok(None)` is returned so the caller treats the
/// file as absent from the snapshot; the next indexing run will re-snapshot it
/// from a clean state.
///
/// Returns `Ok(None)` for binary files (no hash needed) and on persistent
/// instability.  The returned [`FileStat`] describes the same bytes that
/// produced the returned hash.
fn read_hashed_source_stable(
    path: &Path,
) -> Result<Option<([u8; 32], FileStat)>, IncrementalError> {
    const MAX_RETRIES: u8 = 2;

    for _ in 0..=MAX_RETRIES {
        let mut file = fs::File::open(path).map_err(|source| TraverseError::ReadFile {
            path: path.to_path_buf(),
            source,
        })?;
        // fstat on the open fd — consistent with the bytes we are about to read.
        let pre_metadata = file.metadata().map_err(|source| TraverseError::ReadPath {
            path: path.to_path_buf(),
            source,
        })?;
        let pre_stat = stat_from_metadata(&pre_metadata);

        let mut bytes =
            Vec::with_capacity(usize::try_from(pre_metadata.len()).unwrap_or(usize::MAX / 2));
        file.read_to_end(&mut bytes)
            .map_err(|source| TraverseError::ReadFile {
                path: path.to_path_buf(),
                source,
            })?;

        if is_binary(&bytes) {
            return Ok(None);
        }

        // Re-stat after reading to detect concurrent modifications.
        let post_stat = match fs::metadata(path) {
            Ok(meta) => stat_from_metadata(&meta),
            Err(_) => {
                // File vanished between open and post-stat — treat as unstable
                // and let the retry loop decide whether to give up.
                continue;
            }
        };

        if pre_stat == post_stat {
            // Stat was stable across the read: the hash describes `pre_stat`.
            let hash = *blake3::hash(&bytes).as_bytes();
            return Ok(Some((hash, pre_stat)));
        }
        // Stat changed — the bytes we read may be a partial or inconsistent
        // view.  Discard them and retry.
    }

    // All retries exhausted.  Return None so the file is excluded from the
    // snapshot and picked up cleanly on the next run.
    Ok(None)
}

fn stat_from_metadata(metadata: &fs::Metadata) -> FileStat {
    FileStat {
        size_bytes: i64::try_from(metadata.len()).unwrap_or(i64::MAX),
        mtime_ns: metadata_mtime_ns(metadata),
    }
}

fn bytes_to_hash32(bytes: &[u8]) -> Option<[u8; 32]> {
    if bytes.len() != 32 {
        return None;
    }
    let mut hash = [0_u8; 32];
    hash.copy_from_slice(bytes);
    Some(hash)
}

fn metadata_mtime_ns(metadata: &fs::Metadata) -> i64 {
    metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

fn is_binary(bytes: &[u8]) -> bool {
    bytes.iter().take(8 * 1024).any(|byte| *byte == 0)
}

/// Narrow test-support helpers for verifying `snapshot_from_traversal`
/// correctness.  Only compiled in test / `test-support` builds.
#[cfg(any(test, feature = "test-support"))]
pub mod test_support {
    use super::{RepoSnapshot, snapshot_from_traversal};
    use gather_step_parser::{TraverseConfig, collect_repo_files};
    use std::path::Path;

    use crate::incremental::IncrementalError;

    /// Build a `RepoSnapshot` for `root` using the default traverse config.
    /// Returns the snapshot so callers can inspect `files_by_path` directly.
    pub fn snapshot_for_test(root: &Path) -> Result<RepoSnapshot, IncrementalError> {
        let traversal = collect_repo_files(root, &TraverseConfig::default())?;
        snapshot_from_traversal(root, traversal.files, traversal.file_stats, false)
    }
}

impl RepoSnapshot {
    /// Iterator over raw `OsStr` byte keys of all entries in `files_by_path`.
    /// Only available in test / `test-support` builds.
    #[cfg(any(test, feature = "test-support"))]
    pub fn files_iter_for_test(&self) -> impl Iterator<Item = &[u8]> {
        self.files_by_path.keys().map(Vec::as_slice)
    }
}

fn read_manifest_bytes(repo_root: &Path) -> Result<Option<Vec<u8>>, IncrementalError> {
    let manifest_path = repo_root.join("package.json");
    let metadata = match fs::symlink_metadata(&manifest_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(IncrementalError::ReadManifest {
                path: manifest_path.display().to_string(),
                source,
            });
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Ok(None);
    }

    match fs::read(&manifest_path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(IncrementalError::ReadManifest {
            path: manifest_path.display().to_string(),
            source,
        }),
    }
}

pub fn classify_changes(
    metadata: &MetadataStoreDb,
    repo: &str,
    snapshot: &RepoSnapshot,
) -> Result<ChangedSet, IncrementalError> {
    let stored = metadata.file_index_states_by_repo(repo)?;
    let mut stored_by_path = stored
        .into_iter()
        .map(|state| {
            let path_id_bytes = state.effective_path_bytes().to_vec();
            (path_id_bytes, state)
        })
        .collect::<BTreeMap<_, _>>();

    let mut changed = ChangedSet::default();
    for (path_id, entry) in &snapshot.files_by_path {
        match stored_by_path.remove(path_id) {
            Some(existing_state)
                if existing_state.content_hash == stored_content_hash(&entry.content_hash) =>
            {
                changed.unchanged_count += 1;
            }
            Some(_) => changed.modified.push(entry.clone()),
            None => changed.added.push(entry.clone()),
        }
    }
    changed.deleted = stored_by_path
        .into_values()
        .map(|state| {
            let path_id_bytes = state.effective_path_bytes().to_vec();
            IncrementalFileEntry {
                path: state.file_path,
                path_id_bytes,
                content_hash: state.content_hash,
            }
        })
        .collect();
    Ok(changed)
}

pub fn compute_affected_set(
    metadata: &MetadataStoreDb,
    repo: &str,
    changed_paths: &[TrackedPath],
) -> Result<Vec<TrackedPath>, IncrementalError> {
    let mut affected = BTreeSet::new();
    for path in changed_paths {
        affected.insert(path.clone());
        for dependent in metadata.reverse_dependents_by_path_id(repo, &path.path_id_bytes)? {
            affected.insert(dependent);
        }
    }
    Ok(affected.into_iter().collect())
}

pub fn classify_selected_changes(
    metadata: &MetadataStoreDb,
    repo: &str,
    snapshot: &RepoSnapshot,
    candidate_paths: &[String],
) -> Result<ChangedSet, IncrementalError> {
    let stored = metadata.file_index_states_by_repo(repo)?;
    let stored_by_path = stored
        .into_iter()
        .map(|state| {
            let key = state.file_path.clone();
            (key, state)
        })
        .collect::<BTreeMap<_, _>>();

    let mut changed = ChangedSet::default();
    let mut seen = BTreeSet::new();
    for path in candidate_paths {
        if !seen.insert(path.clone()) {
            continue;
        }
        let snapshot_key: &[u8] = path.as_bytes();
        match (
            snapshot.files_by_path.get(snapshot_key),
            stored_by_path.get(path),
        ) {
            (Some(entry), Some(existing_state))
                if existing_state.content_hash == stored_content_hash(&entry.content_hash) =>
            {
                changed.unchanged_count += 1;
            }
            (Some(entry), Some(_)) => changed.modified.push(entry.clone()),
            (Some(entry), None) => changed.added.push(entry.clone()),
            (None, Some(existing_state)) => changed.deleted.push(IncrementalFileEntry {
                path: existing_state.file_path.clone(),
                path_id_bytes: existing_state.effective_path_bytes().to_vec(),
                content_hash: existing_state.content_hash.clone(),
            }),
            (None, None) => {}
        }
    }

    Ok(changed)
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use pretty_assertions::assert_eq;
    use rusqlite::params;

    use crate::{FileIndexState, MetadataStore, MetadataStoreDb, TrackedPath};
    use gather_step_parser::TraverseConfig;

    use super::{
        ChangedSet, IncrementalFileEntry, classify_changes, classify_selected_changes,
        compute_affected_set, snapshot_repo_files, snapshot_selected_repo_files,
    };

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> Self {
            let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-incremental-{name}-{}-{id}",
                process::id()
            ));
            fs::create_dir_all(&path).expect("test dir should exist");
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

    fn open_metadata(name: &str) -> (TestDir, MetadataStoreDb) {
        let root = TestDir::new(name);
        let store = MetadataStoreDb::open(root.path().join("metadata.sqlite"))
            .expect("metadata should open");
        (root, store)
    }

    #[test]
    fn classify_changes_detects_added_modified_deleted_and_unchanged() {
        let repo_root = TestDir::new("classify");
        fs::create_dir_all(repo_root.path().join("src")).expect("src dir should exist");
        fs::write(
            repo_root.path().join("src/unchanged.ts"),
            "export const unchanged = 1;\n",
        )
        .expect("unchanged fixture should write");
        fs::write(
            repo_root.path().join("src/modified.ts"),
            "export const modified = 2;\n",
        )
        .expect("modified fixture should write");
        fs::write(
            repo_root.path().join("src/added.ts"),
            "export const added = 3;\n",
        )
        .expect("added fixture should write");
        fs::write(
            repo_root.path().join("package.json"),
            r#"{ "dependencies": { "@workspace/shared-contracts": "2.0.0" } }"#,
        )
        .expect("manifest should write");

        let (_db_root, metadata) = open_metadata("classify-db");
        let unchanged_hash = blake3::hash(b"export const unchanged = 1;\n")
            .as_bytes()
            .to_vec();
        metadata
            .upsert_file_states(&[
                FileIndexState {
                    repo: "svc".to_owned(),
                    file_path: "src/unchanged.ts".to_owned(),
                    content_hash: unchanged_hash,
                    node_count: 1,
                    edge_count: 1,
                    indexed_at: 1,
                    parse_ms: Some(1),
                    ..Default::default()
                },
                FileIndexState {
                    repo: "svc".to_owned(),
                    file_path: "src/modified.ts".to_owned(),
                    content_hash: vec![9, 9, 9],
                    node_count: 1,
                    edge_count: 1,
                    indexed_at: 1,
                    parse_ms: Some(1),
                    ..Default::default()
                },
                FileIndexState {
                    repo: "svc".to_owned(),
                    file_path: "src/deleted.ts".to_owned(),
                    content_hash: vec![7, 7, 7],
                    node_count: 1,
                    edge_count: 1,
                    indexed_at: 1,
                    parse_ms: Some(1),
                    ..Default::default()
                },
            ])
            .expect("file states should write");

        let snapshot = snapshot_repo_files(
            &metadata,
            "svc",
            repo_root.path(),
            &gather_step_parser::TraverseConfig::default(),
        )
        .expect("snapshot should succeed");
        let changed = classify_changes(&metadata, "svc", &snapshot).expect("classification");

        assert_eq!(
            changed,
            ChangedSet {
                added: vec![
                    IncrementalFileEntry {
                        path: "package.json".to_owned(),
                        path_id_bytes: b"package.json".to_vec(),
                        content_hash: blake3::hash(
                            br#"{ "dependencies": { "@workspace/shared-contracts": "2.0.0" } }"#,
                        )
                        .as_bytes()
                        .to_vec(),
                    },
                    IncrementalFileEntry {
                        path: "src/added.ts".to_owned(),
                        path_id_bytes: b"src/added.ts".to_vec(),
                        content_hash: blake3::hash(b"export const added = 3;\n")
                            .as_bytes()
                            .to_vec(),
                    },
                ],
                modified: vec![IncrementalFileEntry {
                    path: "src/modified.ts".to_owned(),
                    path_id_bytes: b"src/modified.ts".to_vec(),
                    content_hash: blake3::hash(b"export const modified = 2;\n")
                        .as_bytes()
                        .to_vec(),
                }],
                deleted: vec![IncrementalFileEntry {
                    path: "src/deleted.ts".to_owned(),
                    path_id_bytes: b"src/deleted.ts".to_vec(),
                    content_hash: vec![7, 7, 7],
                }],
                unchanged_count: 1,
            }
        );
    }

    #[test]
    #[cfg(unix)]
    fn snapshot_selected_repo_files_ignores_symlinked_package_manifest() {
        use std::os::unix::fs::symlink;

        let repo_root = TestDir::new("manifest-symlink");
        fs::write(
            repo_root.path().join("external.json"),
            r#"{ "dependencies": { "@nestjs/core": "^11.0.0" } }"#,
        )
        .expect("external manifest");
        symlink(
            repo_root.path().join("external.json"),
            repo_root.path().join("package.json"),
        )
        .expect("manifest symlink");

        let snapshot = snapshot_selected_repo_files(
            repo_root.path(),
            &["package.json".to_owned()],
            &TraverseConfig::default(),
        )
        .expect("snapshot should succeed");

        assert!(
            !snapshot
                .files_by_path
                .contains_key(b"package.json".as_ref())
        );
    }

    #[test]
    fn compute_affected_set_includes_reverse_dependents_once() {
        let (_db_root, metadata) = open_metadata("affected-set");
        metadata
            .with_write_txn(|tx| {
                tx.execute(
                    "INSERT INTO file_dependencies(source_repo, source_path, target_repo, target_path, edge_count)
                     VALUES (?1, ?2, ?3, ?4, 1)",
                    params!["svc", b"src/caller.ts" as &[u8], "svc", b"src/helper.ts" as &[u8]],
                )?;
                tx.execute(
                    "INSERT INTO file_dependencies(source_repo, source_path, target_repo, target_path, edge_count)
                     VALUES (?1, ?2, ?3, ?4, 1)",
                    params!["svc", b"src/other.ts" as &[u8], "svc", b"src/helper.ts" as &[u8]],
                )?;
                Ok(())
            })
            .expect("dependency rows should write");

        let affected = compute_affected_set(
            &metadata,
            "svc",
            &[
                TrackedPath {
                    path: "src/helper.ts".to_owned(),
                    path_id_bytes: b"src/helper.ts".to_vec(),
                },
                TrackedPath {
                    path: "src/helper.ts".to_owned(),
                    path_id_bytes: b"src/helper.ts".to_vec(),
                },
            ],
        )
        .expect("affected set should compute");

        assert_eq!(
            affected,
            vec![
                TrackedPath {
                    path: "src/caller.ts".to_owned(),
                    path_id_bytes: b"src/caller.ts".to_vec(),
                },
                TrackedPath {
                    path: "src/helper.ts".to_owned(),
                    path_id_bytes: b"src/helper.ts".to_vec(),
                },
                TrackedPath {
                    path: "src/other.ts".to_owned(),
                    path_id_bytes: b"src/other.ts".to_vec(),
                }
            ]
        );
    }

    #[test]
    fn classify_selected_changes_deduplicates_candidates_and_marks_deleted_files() {
        let (_db_root, metadata) = open_metadata("selected");
        metadata
            .upsert_file_states(&[
                FileIndexState {
                    repo: "svc".to_owned(),
                    file_path: "src/existing.ts".to_owned(),
                    content_hash: vec![1, 2, 3],
                    node_count: 1,
                    edge_count: 1,
                    indexed_at: 1,
                    parse_ms: Some(1),
                    ..Default::default()
                },
                FileIndexState {
                    repo: "svc".to_owned(),
                    file_path: "src/deleted.ts".to_owned(),
                    content_hash: vec![9, 9, 9],
                    node_count: 1,
                    edge_count: 1,
                    indexed_at: 1,
                    parse_ms: Some(1),
                    ..Default::default()
                },
            ])
            .expect("file states should write");

        let snapshot = super::RepoSnapshot {
            source_files: Vec::new(),
            file_stats: rustc_hash::FxHashMap::default(),
            files_by_path: std::collections::BTreeMap::from([(
                b"src/existing.ts".to_vec(),
                IncrementalFileEntry {
                    path: "src/existing.ts".to_owned(),
                    path_id_bytes: b"src/existing.ts".to_vec(),
                    content_hash: vec![4, 5, 6],
                },
            )]),
        };

        let changed = classify_selected_changes(
            &metadata,
            "svc",
            &snapshot,
            &[
                "src/existing.ts".to_owned(),
                "src/existing.ts".to_owned(),
                "src/deleted.ts".to_owned(),
            ],
        )
        .expect("selected changes should classify");

        assert!(changed.added.is_empty());
        assert_eq!(changed.unchanged_count, 0);
        assert_eq!(changed.modified.len(), 1);
        assert_eq!(changed.modified[0].path, "src/existing.ts");
        assert_eq!(
            changed.deleted,
            vec![IncrementalFileEntry {
                path: "src/deleted.ts".to_owned(),
                path_id_bytes: b"src/deleted.ts".to_vec(),
                content_hash: vec![9, 9, 9],
            }]
        );
    }
}
