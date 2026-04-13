use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use gather_step_core::GatherStepConfig;
#[cfg(test)]
use notify::PollWatcher;
#[cfg(not(test))]
use notify::RecommendedWatcher;
use notify::{Config as NotifyConfig, Event, RecursiveMode, Watcher as _};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{
    ChangedSet, GraphStore, GraphStoreError, IndexingOptions, IndexingStats, RepoIndexer,
    RepoIndexerError, StorageCoordinatorError, TrackedPath, WorkspaceStores,
    reconcile_changed_files,
};
use gather_step_parser::TraverseConfig;

#[cfg(test)]
type BackendWatcher = PollWatcher;
#[cfg(not(test))]
type BackendWatcher = RecommendedWatcher;

/// Capacity of the mpsc channel between the notify callback and the async
/// event loop.  A larger buffer reduces the likelihood of drops during heavy
/// edit bursts (e.g. `git checkout`, bulk formatter runs) while keeping memory
/// cost negligible — each `notify::Event` is a small heap allocation.
///
/// The inline reindex path means that while `process_repo_change` runs for one
/// repo, new filesystem events are not drained.  65 536 slots is large enough
/// to absorb a typical burst from a bulk-formatter run on the largest repos
/// without overflowing into forced rescans.  A future full-split (per-repo
/// worker queues) would make the buffer size less critical.
const NOTIFY_EVENT_BUFFER: usize = 65_536;

/// Warn when the notify channel's pending count exceeds this fraction of its
/// capacity.  Logging at 75 % gives operators time to react before drops occur.
const NOTIFY_BUFFER_WARN_THRESHOLD: usize = (NOTIFY_EVENT_BUFFER * 3) / 4;

const MAX_PENDING_FILE_HINTS: usize = 500;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WatcherConfig {
    pub poll_interval: Duration,
    pub debounce_duration: Duration,
    pub consecutive_error_limit: u32,
    pub error_backoff: Duration,
}

impl Default for WatcherConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(250),
            debounce_duration: Duration::from_secs(2),
            consecutive_error_limit: 5,
            error_backoff: Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WatchCause {
    Paths,
    Rescan,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WatchEvent {
    IndexingStart {
        repo: String,
        files: Vec<String>,
        cause: WatchCause,
    },
    Overflow {
        repo: String,
        dropped_events: u64,
    },
    IndexingComplete {
        repo: String,
        changed: ChangedSet,
        stats: IndexingStats,
    },
    Error {
        repo: String,
        error: String,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WatcherStatus {
    pub events_seen: u64,
    pub dropped_events: u64,
    pub indexing_runs: u64,
    pub overflows: u64,
    pub rescans_requested: u64,
    pub errors: u64,
    pub backoff_suppressions: u64,
    pub cross_repo_reconciliations: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum WatcherError {
    #[error("repo `{repo}` is already being watched")]
    DuplicateRepo { repo: String },
    #[error("watch loop has no repos configured")]
    NoRepos,
    #[error(transparent)]
    Notify(#[from] notify::Error),
    #[error(transparent)]
    Incremental(#[from] crate::IncrementalError),
    #[error(transparent)]
    Graph(#[from] GraphStoreError),
    #[error(transparent)]
    Indexer(#[from] RepoIndexerError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProcessRepoChangeOutcome {
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug)]
struct RepoWatch {
    repo: String,
    repo_root: PathBuf,
}

#[derive(Clone, Debug, Default)]
struct PendingRepoChange {
    files: BTreeSet<String>,
    last_event_at: Option<Instant>,
    rescan_requested: bool,
    dropped_events: u64,
}

impl PendingRepoChange {
    fn request_rescan(&mut self) {
        self.rescan_requested = true;
        self.files.clear();
    }

    fn record_path(&mut self, relative_path: String) {
        if self.rescan_requested {
            return;
        }
        if self.files.len() >= MAX_PENDING_FILE_HINTS {
            self.request_rescan();
            return;
        }
        self.files.insert(relative_path);
        if self.files.len() > MAX_PENDING_FILE_HINTS {
            self.request_rescan();
        }
    }

    fn merge(&mut self, other: PendingRepoChange) {
        self.dropped_events = self.dropped_events.saturating_add(other.dropped_events);
        self.last_event_at = match (self.last_event_at, other.last_event_at) {
            (_, Some(other_last)) => Some(other_last),
            (current, None) => current,
        };
        if self.rescan_requested || other.rescan_requested {
            self.request_rescan();
            return;
        }
        for file in other.files {
            self.record_path(file);
        }
    }
}

#[derive(Clone, Debug, Default)]
struct RepoFailureState {
    consecutive_errors: u32,
    suppressed_until: Option<Instant>,
}

pub struct Watcher {
    config: WatcherConfig,
    indexer: Arc<RepoIndexer>,
    traverse: TraverseConfig,
    repos: BTreeMap<String, RepoWatch>,
    event_tx: broadcast::Sender<WatchEvent>,
    status: Arc<Mutex<WatcherStatus>>,
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Watcher {
    pub fn new(
        storage_root: impl AsRef<Path>,
        options: IndexingOptions,
        config: WatcherConfig,
    ) -> Result<Self, WatcherError> {
        let stores = WorkspaceStores::open(storage_root)
            .map_err(StorageCoordinatorError::from)
            .map_err(RepoIndexerError::from)?;
        Self::new_with_stores(stores, options, config)
    }

    pub fn new_with_stores(
        stores: WorkspaceStores,
        options: IndexingOptions,
        config: WatcherConfig,
    ) -> Result<Self, WatcherError> {
        let traverse = options.traverse.clone();
        let indexer = Arc::new(RepoIndexer::open_with_stores(stores, options)?);
        let (event_tx, _) = broadcast::channel(1024);
        Ok(Self {
            config,
            indexer,
            traverse,
            repos: BTreeMap::new(),
            event_tx,
            status: Arc::new(Mutex::new(WatcherStatus::default())),
        })
    }

    pub fn add_repo(
        &mut self,
        repo: impl Into<String>,
        repo_root: impl AsRef<Path>,
    ) -> Result<(), WatcherError> {
        let repo = repo.into();
        if self.repos.contains_key(&repo) {
            return Err(WatcherError::DuplicateRepo { repo });
        }
        self.repos.insert(
            repo.clone(),
            RepoWatch {
                repo,
                repo_root: repo_root.as_ref().to_path_buf(),
            },
        );
        Ok(())
    }

    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<WatchEvent> {
        self.event_tx.subscribe()
    }

    #[must_use]
    pub fn status(&self) -> WatcherStatus {
        lock_unpoisoned(&self.status).clone()
    }

    fn update_status(&self, update: impl FnOnce(&mut WatcherStatus)) {
        let mut status = lock_unpoisoned(&self.status);
        update(&mut status);
    }

    pub async fn run(&self, cancel: CancellationToken) -> Result<(), WatcherError> {
        if self.repos.is_empty() {
            return Err(WatcherError::NoRepos);
        }

        let (notify_tx, mut notify_rx) =
            mpsc::channel::<Result<Event, notify::Error>>(NOTIFY_EVENT_BUFFER);
        let overflow_requested = Arc::new(AtomicBool::new(false));
        let dropped_events = Arc::new(std::sync::atomic::AtomicU64::new(0));
        // Track the approximate number of events currently queued in the
        // channel so we can warn before the buffer fills up.
        let pending_in_channel = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut watchers = Vec::<BackendWatcher>::with_capacity(self.repos.len());
        for repo_watch in self.repos.values() {
            let tx = notify_tx.clone();
            let overflow_flag = Arc::clone(&overflow_requested);
            let dropped_counter = Arc::clone(&dropped_events);
            let pending_counter = Arc::clone(&pending_in_channel);
            #[cfg(test)]
            let mut watcher = PollWatcher::new(
                move |result| {
                    if tx.try_send(result).is_ok() {
                        pending_counter.fetch_add(1, Ordering::Relaxed);
                    } else {
                        overflow_flag.store(true, Ordering::Relaxed);
                        dropped_counter.fetch_add(1, Ordering::Relaxed);
                    }
                },
                NotifyConfig::default().with_poll_interval(self.config.poll_interval),
            )?;
            #[cfg(not(test))]
            let mut watcher = RecommendedWatcher::new(
                move |result| {
                    if tx.try_send(result).is_ok() {
                        pending_counter.fetch_add(1, Ordering::Relaxed);
                    } else {
                        overflow_flag.store(true, Ordering::Relaxed);
                        dropped_counter.fetch_add(1, Ordering::Relaxed);
                    }
                },
                NotifyConfig::default(),
            )?;
            watcher.watch(&repo_watch.repo_root, RecursiveMode::Recursive)?;
            watchers.push(watcher);
        }
        drop(notify_tx);

        let mut interval = tokio::time::interval(self.config.poll_interval);
        let mut pending = BTreeMap::<String, PendingRepoChange>::new();
        let mut failures = BTreeMap::<String, RepoFailureState>::new();

        loop {
            tokio::select! {
                () = cancel.cancelled() => return Ok(()),
                maybe_event = notify_rx.recv() => {
                    match maybe_event {
                        Some(Ok(event)) => {
                            pending_in_channel.fetch_sub(1, Ordering::Relaxed);
                            let queued = pending_in_channel.load(Ordering::Relaxed);
                            if queued >= NOTIFY_BUFFER_WARN_THRESHOLD {
                                warn!(
                                    queued,
                                    capacity = NOTIFY_EVENT_BUFFER,
                                    "notify event buffer is at or above 75% capacity; \
                                     consider reducing indexing load to avoid event drops",
                                );
                            }
                            self.record_event_paths(&event, &mut pending);
                            self.update_status(|status| {
                                status.events_seen = status.events_seen.saturating_add(1);
                            });
                        }
                        Some(Err(error)) => {
                            pending_in_channel.fetch_sub(1, Ordering::Relaxed);
                            self.update_status(|status| {
                                status.errors = status.errors.saturating_add(1);
                            });
                            let _ = self.event_tx.send(WatchEvent::Error {
                                repo: "*".to_owned(),
                                error: error.to_string(),
                            });
                        }
                        None => return Ok(()),
                    }
                }
                _ = interval.tick() => {}
            }

            if overflow_requested.swap(false, Ordering::Relaxed) {
                let dropped_count = dropped_events.swap(0, Ordering::Relaxed);
                for repo in self.repos.keys() {
                    let state = pending.entry(repo.clone()).or_default();
                    state.request_rescan();
                    state.last_event_at = Some(Instant::now());
                    state.dropped_events = state.dropped_events.saturating_add(dropped_count);
                }
                self.update_status(|status| {
                    status.dropped_events = status.dropped_events.saturating_add(dropped_count);
                    status.rescans_requested = status
                        .rescans_requested
                        .saturating_add(u64::try_from(self.repos.len()).unwrap_or(u64::MAX));
                });
                warn!(
                    dropped_events = dropped_count,
                    repos = self.repos.len(),
                    "watcher queue overflowed; scheduling repo rescans"
                );
            }

            let ready = pending
                .iter()
                .filter_map(|(repo, state)| {
                    let last_event_at = state.last_event_at?;
                    let failure_state = failures.get(repo).cloned().unwrap_or_default();
                    let suppressed = failure_state
                        .suppressed_until
                        .is_some_and(|until| Instant::now() < until);
                    if suppressed {
                        self.update_status(|status| {
                            status.backoff_suppressions =
                                status.backoff_suppressions.saturating_add(1);
                        });
                    }
                    (!suppressed && last_event_at.elapsed() >= self.config.debounce_duration)
                        .then(|| repo.clone())
                })
                .collect::<Vec<_>>();

            for repo in ready {
                let Some(repo_watch) = self.repos.get(&repo) else {
                    continue;
                };
                let Some(state) = pending.remove(&repo) else {
                    continue;
                };
                let rescan_requested = state.rescan_requested;
                let dropped_events = state.dropped_events;
                let files = state.files.iter().cloned().collect::<Vec<_>>();
                if rescan_requested {
                    self.update_status(|status| {
                        status.overflows = status.overflows.saturating_add(1);
                    });
                    warn!(repo, "watcher backend requested rescan");
                    let _ = self.event_tx.send(WatchEvent::Overflow {
                        repo: repo.clone(),
                        dropped_events,
                    });
                }
                if cancel.is_cancelled() {
                    return Ok(());
                }
                let outcome = self
                    .process_repo_change(
                        &repo,
                        &repo_watch.repo_root,
                        files,
                        rescan_requested,
                        cancel.clone(),
                    )
                    .await;
                if matches!(outcome, ProcessRepoChangeOutcome::Cancelled) {
                    return Ok(());
                }
                let failure_state = failures.entry(repo.clone()).or_default();
                if matches!(outcome, ProcessRepoChangeOutcome::Succeeded) {
                    failure_state.consecutive_errors = 0;
                    failure_state.suppressed_until = None;
                } else {
                    failure_state.consecutive_errors =
                        failure_state.consecutive_errors.saturating_add(1);
                    let mut retry_state = state;
                    retry_state.last_event_at = Some(Instant::now());
                    pending.entry(repo.clone()).or_default().merge(retry_state);
                    if failure_state.consecutive_errors >= self.config.consecutive_error_limit {
                        failure_state.suppressed_until =
                            Some(Instant::now() + self.config.error_backoff);
                        warn!(
                            repo,
                            consecutive_errors = failure_state.consecutive_errors,
                            backoff_ms = self.config.error_backoff.as_millis(),
                            "suppressing watch reindex after repeated failures"
                        );
                    }
                }
            }

            let _keep_watchers_alive = &watchers;
        }
    }

    fn record_event_paths(&self, event: &Event, pending: &mut BTreeMap<String, PendingRepoChange>) {
        if event.need_rescan() {
            let repos = self.repos_for_event(event);
            for repo in repos {
                let state = pending.entry(repo.clone()).or_default();
                state.request_rescan();
                state.last_event_at = Some(Instant::now());
            }
        }
        for path in &event.paths {
            let Some((repo, relative_path)) = self.repo_for_path(path) else {
                continue;
            };
            let state = pending.entry(repo.to_owned()).or_default();
            state.last_event_at = Some(Instant::now());
            state.record_path(relative_path);
        }
    }

    fn repos_for_event(&self, event: &Event) -> Vec<String> {
        let repos = event
            .paths
            .iter()
            .filter_map(|path| self.repo_for_path(path).map(|(repo, _)| repo.to_owned()))
            .collect::<BTreeSet<_>>();
        if repos.is_empty() {
            self.repos.keys().cloned().collect()
        } else {
            repos.into_iter().collect()
        }
    }

    fn repo_for_path<'a>(&'a self, path: &Path) -> Option<(&'a str, String)> {
        self.repos
            .values()
            .filter_map(|repo_watch| {
                let relative = path.strip_prefix(&repo_watch.repo_root).ok()?;
                let relative = relative.to_string_lossy().replace('\\', "/");
                if relative.is_empty()
                    || !self.traverse.is_index_relevant_path(Path::new(&relative))
                {
                    return None;
                }
                Some((
                    repo_watch.repo.as_str(),
                    repo_watch.repo_root.components().count(),
                    relative,
                ))
            })
            .max_by_key(|(_, depth, _)| *depth)
            .map(|(repo, _, relative)| (repo, relative))
    }

    async fn process_repo_change(
        &self,
        repo: &str,
        repo_root: &Path,
        files: Vec<String>,
        rescan_requested: bool,
        cancel: CancellationToken,
    ) -> ProcessRepoChangeOutcome {
        if cancel.is_cancelled() {
            return ProcessRepoChangeOutcome::Cancelled;
        }
        let before_virtuals = if rescan_requested {
            self.virtual_external_ids_for_repo(repo)
        } else {
            self.virtual_external_ids_for_files(repo, &files)
        };
        let cause = if rescan_requested {
            WatchCause::Rescan
        } else {
            WatchCause::Paths
        };
        debug!(repo, files = ?files, "processing repo watch change");
        let _ = self.event_tx.send(WatchEvent::IndexingStart {
            repo: repo.to_owned(),
            files: files.clone(),
            cause: cause.clone(),
        });

        match incremental_reindex_async(
            Arc::clone(&self.indexer),
            repo.to_owned(),
            repo_root.to_path_buf(),
            (!rescan_requested).then_some(files),
            cancel.clone(),
        )
        .await
        {
            Ok((changed, stats)) => {
                self.update_status(|status| {
                    status.indexing_runs = status.indexing_runs.saturating_add(1);
                });
                let changed_files = changed
                    .added
                    .iter()
                    .map(|file| file.path.clone())
                    .chain(changed.modified.iter().map(|file| file.path.clone()))
                    .chain(changed.deleted.iter().map(|file| file.path.clone()))
                    .collect::<Vec<_>>();
                let after_virtuals = if rescan_requested {
                    self.virtual_external_ids_for_repo(repo)
                } else {
                    self.virtual_external_ids_for_files(repo, &changed_files)
                };
                let changed_virtuals = before_virtuals
                    .symmetric_difference(&after_virtuals)
                    .cloned()
                    .collect::<BTreeSet<_>>();
                if !changed_virtuals.is_empty() {
                    match self.reconcile_cross_repo_virtual_dependents(repo, &changed_virtuals) {
                        Ok(count) => {
                            self.update_status(|status| {
                                status.cross_repo_reconciliations = status
                                    .cross_repo_reconciliations
                                    .saturating_add(u64::try_from(count).unwrap_or(u64::MAX));
                            });
                            if count > 0 {
                                info!(
                                    repo,
                                    cause = ?cause,
                                    changed_virtuals = changed_virtuals.len(),
                                    impacted_repos = count,
                                    "reconciled cross-repo dependents after virtual node change"
                                );
                            }
                        }
                        Err(error) => {
                            if matches!(error, WatcherError::Indexer(RepoIndexerError::Cancelled)) {
                                return ProcessRepoChangeOutcome::Cancelled;
                            }
                            self.update_status(|status| {
                                status.errors = status.errors.saturating_add(1);
                            });
                            let _ = self.event_tx.send(WatchEvent::Error {
                                repo: repo.to_owned(),
                                error: error.to_string(),
                            });
                            return ProcessRepoChangeOutcome::Failed;
                        }
                    }
                }
                let _ = self.event_tx.send(WatchEvent::IndexingComplete {
                    repo: repo.to_owned(),
                    changed,
                    stats,
                });
                info!(
                    repo,
                    cause = ?cause,
                    changed_files = changed_files.len(),
                    files_parsed = stats.files_parsed,
                    duration_ms = stats.duration_ms,
                    "watch reindex completed"
                );
                ProcessRepoChangeOutcome::Succeeded
            }
            Err(error) => {
                if matches!(error, RepoIndexerError::Cancelled) {
                    return ProcessRepoChangeOutcome::Cancelled;
                }
                self.update_status(|status| {
                    status.errors = status.errors.saturating_add(1);
                });
                let _ = self.event_tx.send(WatchEvent::Error {
                    repo: repo.to_owned(),
                    error: error.to_string(),
                });
                ProcessRepoChangeOutcome::Failed
            }
        }
    }

    fn virtual_external_ids_for_files(&self, repo: &str, files: &[String]) -> BTreeSet<String> {
        files
            .iter()
            .flat_map(|file| {
                self.indexer
                    .storage()
                    .graph()
                    .nodes_by_file(repo, file)
                    .unwrap_or_default()
            })
            .filter(|node| {
                node.is_virtual
                    && matches!(
                        node.kind,
                        gather_step_core::NodeKind::Route
                            | gather_step_core::NodeKind::Topic
                            | gather_step_core::NodeKind::Queue
                            | gather_step_core::NodeKind::Subject
                            | gather_step_core::NodeKind::Stream
                            | gather_step_core::NodeKind::Event
                            | gather_step_core::NodeKind::SharedSymbol
                    )
            })
            .filter_map(|node| node.external_id)
            .collect()
    }

    fn virtual_external_ids_for_repo(&self, repo: &str) -> BTreeSet<String> {
        self.indexer
            .storage()
            .graph()
            .nodes_by_repo(repo)
            .unwrap_or_default()
            .into_iter()
            .filter(|node| {
                node.is_virtual
                    && matches!(
                        node.kind,
                        gather_step_core::NodeKind::Route
                            | gather_step_core::NodeKind::Topic
                            | gather_step_core::NodeKind::Queue
                            | gather_step_core::NodeKind::Subject
                            | gather_step_core::NodeKind::Stream
                            | gather_step_core::NodeKind::Event
                            | gather_step_core::NodeKind::SharedSymbol
                    )
            })
            .filter_map(|node| node.external_id)
            .collect()
    }

    fn reconcile_cross_repo_virtual_dependents(
        &self,
        source_repo: &str,
        external_ids: &BTreeSet<String>,
    ) -> Result<usize, WatcherError> {
        let mut impacted = BTreeMap::<String, BTreeSet<String>>::new();
        for external_id in external_ids {
            for kind in [
                gather_step_core::NodeKind::Route,
                gather_step_core::NodeKind::Topic,
                gather_step_core::NodeKind::Queue,
                gather_step_core::NodeKind::Subject,
                gather_step_core::NodeKind::Stream,
                gather_step_core::NodeKind::Event,
                gather_step_core::NodeKind::SharedSymbol,
            ] {
                for virtual_node in self
                    .indexer
                    .storage()
                    .graph()
                    .nodes_by_external_id(kind, external_id)?
                {
                    for incoming in self
                        .indexer
                        .storage()
                        .graph()
                        .get_incoming(virtual_node.id)?
                    {
                        let Some(owner_file) = self
                            .indexer
                            .storage()
                            .graph()
                            .get_node(incoming.owner_file)?
                        else {
                            continue;
                        };
                        if owner_file.repo != source_repo {
                            impacted
                                .entry(owner_file.repo.clone())
                                .or_default()
                                .insert(owner_file.file_path.clone());
                        }
                    }
                }
            }
        }

        for (repo, files) in &impacted {
            let tracked_files = files
                .iter()
                .cloned()
                .map(|path| TrackedPath {
                    path_id_bytes: path.as_bytes().to_vec(),
                    path,
                })
                .collect::<Vec<_>>();
            reconcile_changed_files(self.indexer.storage(), repo, &tracked_files)
                .map_err(RepoIndexerError::from)?;
        }

        Ok(impacted.len())
    }
}

async fn incremental_reindex_async(
    indexer: Arc<RepoIndexer>,
    repo: String,
    repo_root: PathBuf,
    changed_paths_hint: Option<Vec<String>>,
    cancel: CancellationToken,
) -> Result<(ChangedSet, IndexingStats), RepoIndexerError> {
    let (tx, rx) = oneshot::channel();
    rayon::spawn(move || {
        let result = indexer.index_repo_incremental_with_hint_cancellable(
            &repo,
            &repo_root,
            changed_paths_hint.as_deref(),
            Some(&cancel),
            None,
        );
        let _ = tx.send(result);
    });
    rx.await
        .map_err(|_| RepoIndexerError::IncrementalWorkerPanicked)?
}

pub struct WorkspaceWatcher {
    inner: Watcher,
}

impl WorkspaceWatcher {
    pub fn new(
        storage_root: impl AsRef<Path>,
        options: IndexingOptions,
        config: WatcherConfig,
        workspace: &GatherStepConfig,
        config_root: impl AsRef<Path>,
    ) -> Result<Self, WatcherError> {
        let stores = WorkspaceStores::open(storage_root)
            .map_err(StorageCoordinatorError::from)
            .map_err(RepoIndexerError::from)?;
        Self::new_with_stores(stores, options, config, workspace, config_root)
    }

    pub fn new_with_stores(
        stores: WorkspaceStores,
        options: IndexingOptions,
        config: WatcherConfig,
        workspace: &GatherStepConfig,
        config_root: impl AsRef<Path>,
    ) -> Result<Self, WatcherError> {
        let mut watcher = Watcher::new_with_stores(stores, options, config)?;
        let config_root = config_root.as_ref();
        for repo in &workspace.repos {
            watcher.add_repo(repo.name.clone(), config_root.join(&repo.path))?;
        }
        Ok(Self { inner: watcher })
    }

    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<WatchEvent> {
        self.inner.subscribe()
    }

    #[must_use]
    pub fn status(&self) -> WatcherStatus {
        self.inner.status()
    }

    pub async fn run(&self, cancel: CancellationToken) -> Result<(), WatcherError> {
        self.inner.run(cancel).await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
        time::{Duration, Instant},
    };

    use notify::{
        Event,
        event::{EventKind, Flag},
    };
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use gather_step_core::NodeKind;

    use crate::GraphStore;

    use super::{
        MAX_PENDING_FILE_HINTS, PendingRepoChange, ProcessRepoChangeOutcome, RepoFailureState,
        WatchEvent, Watcher, WatcherConfig, WorkspaceWatcher,
    };
    use crate::{IndexingOptions, RepoIndexerError};

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> Self {
            let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
            let path =
                env::temp_dir().join(format!("gather-step-watcher-{name}-{}-{id}", process::id()));
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn watcher_emits_reindex_events_for_modified_file() {
        let repo_root = TestDir::new("single-repo");
        let storage_root = TestDir::new("single-storage");
        fs::create_dir_all(repo_root.path().join("src")).expect("src dir should exist");
        fs::write(
            repo_root.path().join("src/helper.ts"),
            "export function helper() { return 1; }\n",
        )
        .expect("helper fixture should write");
        fs::write(
            repo_root.path().join("src/caller.ts"),
            "import { helper } from './helper';\nexport function caller() { return helper(); }\n",
        )
        .expect("caller fixture should write");

        let mut watcher = Watcher::new(
            storage_root.path(),
            IndexingOptions::default(),
            WatcherConfig {
                poll_interval: Duration::from_millis(50),
                debounce_duration: Duration::from_millis(150),
                ..WatcherConfig::default()
            },
        )
        .expect("watcher should open");
        watcher
            .add_repo("sample-service", repo_root.path())
            .expect("repo should register");
        watcher
            .indexer
            .index_repo("sample-service", repo_root.path(), None)
            .expect("initial index should succeed");

        let mut events = watcher.subscribe();
        fs::write(
            repo_root.path().join("src/helper.ts"),
            "export function helper() { return 2; }\n",
        )
        .expect("helper update should write");

        watcher
            .process_repo_change(
                "sample-service",
                repo_root.path(),
                vec!["src/helper.ts".to_owned()],
                false,
                CancellationToken::new(),
            )
            .await;

        let complete = timeout(Duration::from_secs(5), async {
            loop {
                match events.recv().await.expect("event should arrive") {
                    WatchEvent::IndexingComplete {
                        repo,
                        changed,
                        stats,
                    } => {
                        assert_eq!(repo, "sample-service");
                        assert_eq!(stats.files_parsed, 2);
                        assert_eq!(changed.modified.len(), 1);
                        break;
                    }
                    WatchEvent::IndexingStart { .. }
                    | WatchEvent::Overflow { .. }
                    | WatchEvent::Error { .. } => {}
                }
            }
        })
        .await;
        assert!(complete.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn workspace_watcher_tracks_multiple_repos() {
        let root = TestDir::new("workspace-root");
        let storage_root = TestDir::new("workspace-storage");
        fs::create_dir_all(root.path().join("repos/a/src")).expect("repo a dir");
        fs::create_dir_all(root.path().join("repos/b/src")).expect("repo b dir");
        fs::write(
            root.path().join("repos/a/src/a.ts"),
            "export function alpha() { return 1; }\n",
        )
        .expect("repo a file");
        fs::write(
            root.path().join("repos/b/src/b.ts"),
            "export function beta() { return 2; }\n",
        )
        .expect("repo b file");

        let config = gather_step_core::GatherStepConfig::from_yaml_str(
            r"
repos:
  - name: alpha
    path: repos/a
  - name: beta
    path: repos/b
",
        )
        .expect("config should parse");

        let workspace = WorkspaceWatcher::new(
            storage_root.path(),
            IndexingOptions::default(),
            WatcherConfig {
                poll_interval: Duration::from_millis(50),
                debounce_duration: Duration::from_millis(150),
                ..WatcherConfig::default()
            },
            &config,
            root.path(),
        )
        .expect("workspace watcher should open");
        let mut events = workspace.subscribe();

        workspace
            .inner
            .indexer
            .index_repo("alpha", root.path().join("repos/a"), None)
            .expect("alpha index");
        workspace
            .inner
            .indexer
            .index_repo("beta", root.path().join("repos/b"), None)
            .expect("beta index");

        fs::write(
            root.path().join("repos/a/src/a.ts"),
            "export function alpha() { return 3; }\n",
        )
        .expect("repo a update");

        workspace
            .inner
            .process_repo_change(
                "alpha",
                &root.path().join("repos/a"),
                vec!["src/a.ts".to_owned()],
                false,
                CancellationToken::new(),
            )
            .await;

        let repo = timeout(Duration::from_secs(5), async {
            loop {
                match events.recv().await.expect("event should arrive") {
                    WatchEvent::IndexingComplete { repo, .. } => break repo,
                    WatchEvent::IndexingStart { .. }
                    | WatchEvent::Overflow { .. }
                    | WatchEvent::Error { .. } => {}
                }
            }
        })
        .await
        .expect("workspace event should arrive");
        assert_eq!(repo, "alpha");

        let graph = workspace.inner.indexer.storage().graph();
        assert_eq!(
            graph
                .nodes_by_file("beta", "src/b.ts")
                .expect("beta nodes should load")
                .iter()
                .filter(|node| node.kind == NodeKind::Function)
                .count(),
            1
        );

        let status = workspace.inner.status();
        assert_eq!(status.indexing_runs, 1);
        assert_eq!(status.errors, 0);
    }

    #[test]
    fn record_event_paths_ignores_excluded_noise_and_keeps_config_files() {
        let storage_root = TestDir::new("filter-storage");
        let repo_root = TestDir::new("filter-repo");
        let mut watcher = Watcher::new(
            storage_root.path(),
            IndexingOptions::default(),
            WatcherConfig::default(),
        )
        .expect("watcher should open");
        watcher
            .add_repo("sample-service", repo_root.path())
            .expect("repo should register");

        let mut pending = BTreeMap::new();
        watcher.record_event_paths(
            &Event::new(EventKind::Any)
                .add_path(repo_root.path().join("node_modules/pkg/index.ts")),
            &mut pending,
        );
        watcher.record_event_paths(
            &Event::new(EventKind::Any).add_path(repo_root.path().join("README.md")),
            &mut pending,
        );
        watcher.record_event_paths(
            &Event::new(EventKind::Any).add_path(repo_root.path().join("package.json")),
            &mut pending,
        );

        let queued = pending
            .get("sample-service")
            .expect("repo should be queued")
            .files
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(queued, vec!["package.json".to_owned()]);
    }

    #[test]
    fn record_event_paths_marks_rescan_requests() {
        let storage_root = TestDir::new("rescan-storage");
        let repo_root = TestDir::new("rescan-repo");
        let mut watcher = Watcher::new(
            storage_root.path(),
            IndexingOptions::default(),
            WatcherConfig::default(),
        )
        .expect("watcher should open");
        watcher
            .add_repo("sample-service", repo_root.path())
            .expect("repo should register");

        let mut pending = BTreeMap::new();
        watcher.record_event_paths(
            &Event::new(EventKind::Any).set_flag(Flag::Rescan),
            &mut pending,
        );

        let state = pending
            .get("sample-service")
            .expect("repo should be queued for rescan");
        assert!(state.rescan_requested);
        assert!(state.files.is_empty());
        assert!(state.last_event_at.is_some());
    }

    #[test]
    fn record_event_paths_caps_pending_files_and_switches_to_rescan() {
        let storage_root = TestDir::new("pending-cap-storage");
        let repo_root = TestDir::new("pending-cap-repo");
        let mut watcher = Watcher::new(
            storage_root.path(),
            IndexingOptions::default(),
            WatcherConfig::default(),
        )
        .expect("watcher should open");
        watcher
            .add_repo("sample-service", repo_root.path())
            .expect("repo should register");

        let mut pending = BTreeMap::new();
        for index in 0..=MAX_PENDING_FILE_HINTS {
            watcher.record_event_paths(
                &Event::new(EventKind::Any)
                    .add_path(repo_root.path().join(format!("src/file-{index}.ts"))),
                &mut pending,
            );
        }

        let state = pending
            .get("sample-service")
            .expect("repo should be queued after overflow");
        assert!(state.rescan_requested);
        assert!(state.files.is_empty());
    }

    #[test]
    fn repo_backoff_suppresses_ready_processing_after_repeated_errors() {
        let storage_root = TestDir::new("backoff-storage");
        let repo_root = TestDir::new("backoff-repo");
        let mut watcher = Watcher::new(
            storage_root.path(),
            IndexingOptions::default(),
            WatcherConfig {
                consecutive_error_limit: 2,
                error_backoff: Duration::from_secs(30),
                ..WatcherConfig::default()
            },
        )
        .expect("watcher should open");
        watcher
            .add_repo("sample-service", repo_root.path())
            .expect("repo should register");

        let mut failures = BTreeMap::<String, RepoFailureState>::new();
        let failure_state = failures.entry("sample-service".to_owned()).or_default();
        failure_state.consecutive_errors = 2;
        failure_state.suppressed_until = Some(Instant::now() + Duration::from_secs(30));

        let mut pending = BTreeMap::<String, PendingRepoChange>::new();
        pending.insert(
            "sample-service".to_owned(),
            PendingRepoChange {
                files: BTreeSet::from(["src/app.ts".to_owned()]),
                last_event_at: Some(Instant::now().checked_sub(Duration::from_secs(10)).unwrap()),
                rescan_requested: false,
                dropped_events: 0,
            },
        );

        let ready = pending
            .iter()
            .filter_map(|(repo, state)| {
                let last_event_at = state.last_event_at?;
                let failure_state = failures.get(repo).cloned().unwrap_or_default();
                let suppressed = failure_state
                    .suppressed_until
                    .is_some_and(|until| Instant::now() < until);
                (!suppressed && last_event_at.elapsed() >= watcher.config.debounce_duration)
                    .then(|| repo.clone())
            })
            .collect::<Vec<_>>();

        assert!(ready.is_empty());
    }

    #[test]
    fn cancellable_index_repo_returns_cancelled_before_parse_work() {
        let repo_root = TestDir::new("cancelled-index-repo");
        let storage_root = TestDir::new("cancelled-index-storage");
        fs::create_dir_all(repo_root.path().join("src")).expect("src dir should exist");
        fs::write(
            repo_root.path().join("src/helper.ts"),
            "export function helper() { return 1; }\n",
        )
        .expect("helper fixture should write");

        let indexer = crate::RepoIndexer::open(storage_root.path(), IndexingOptions::default())
            .expect("indexer should open");
        let cancel = CancellationToken::new();
        cancel.cancel();

        let error = indexer
            .index_repo_cancellable("cancelled-service", repo_root.path(), Some(&cancel), None)
            .expect_err("indexing should cancel");
        assert!(matches!(error, RepoIndexerError::Cancelled));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn process_repo_change_returns_cancelled_without_emitting_error() {
        let repo_root = TestDir::new("cancelled-process-repo");
        let storage_root = TestDir::new("cancelled-process-storage");
        fs::create_dir_all(repo_root.path().join("src")).expect("src dir should exist");
        fs::write(
            repo_root.path().join("src/helper.ts"),
            "export function helper() { return 1; }\n",
        )
        .expect("helper fixture should write");

        let mut watcher = Watcher::new(
            storage_root.path(),
            IndexingOptions::default(),
            WatcherConfig::default(),
        )
        .expect("watcher should open");
        watcher
            .add_repo("sample-service", repo_root.path())
            .expect("repo should register");

        let cancel = CancellationToken::new();
        cancel.cancel();
        let outcome = watcher
            .process_repo_change(
                "sample-service",
                repo_root.path(),
                vec!["src/helper.ts".to_owned()],
                false,
                cancel,
            )
            .await;

        assert_eq!(outcome, ProcessRepoChangeOutcome::Cancelled);
        assert_eq!(watcher.status().errors, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn status_tracks_events_and_index_runs() {
        let repo_root = TestDir::new("status-repo");
        let storage_root = TestDir::new("status-storage");
        fs::create_dir_all(repo_root.path().join("src")).expect("src dir should exist");
        fs::write(
            repo_root.path().join("src/helper.ts"),
            "export function helper() { return 1; }\n",
        )
        .expect("helper fixture should write");

        let mut watcher = Watcher::new(
            storage_root.path(),
            IndexingOptions::default(),
            WatcherConfig::default(),
        )
        .expect("watcher should open");
        watcher
            .add_repo("sample-service", repo_root.path())
            .expect("repo should register");
        watcher
            .indexer
            .index_repo("sample-service", repo_root.path(), None)
            .expect("initial index should succeed");

        let mut pending = BTreeMap::new();
        watcher.record_event_paths(
            &Event::new(EventKind::Any).add_path(repo_root.path().join("src/helper.ts")),
            &mut pending,
        );
        watcher.update_status(|status| {
            status.events_seen = status.events_seen.saturating_add(1);
        });
        watcher
            .process_repo_change(
                "sample-service",
                repo_root.path(),
                vec!["src/helper.ts".to_owned()],
                false,
                CancellationToken::new(),
            )
            .await;

        let status = watcher.status();
        assert_eq!(status.events_seen, 1);
        assert_eq!(status.indexing_runs, 1);
    }
}
