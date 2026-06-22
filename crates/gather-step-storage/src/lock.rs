//! Per-repo index lock with self-heal: owner metadata, bounded acquisition,
//! process-liveness probing, and a read-only scan for `status`/`doctor`.
//!
//! The lock is an OS-advisory `flock` (`std::fs::File::try_lock`) on
//! `<storage>/locks/<blake3(repo)>.lock`. The kernel releases a dead owner's
//! advisory lock automatically, so a contended `try_lock` (`WouldBlock`)
//! genuinely means a live holder on this host. The two failure modes this
//! module fixes are (a) the historical acquisition loop never timed out and
//! (b) `status`/`doctor` never revealed that a lock existed.

use std::fs::{self, File, OpenOptions};
use std::io::{Seek, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

const BACKOFF_INITIAL_MS: u64 = 10;
const BACKOFF_MAX_MS: u64 = 500;

/// Default cap on how long acquisition waits for a contended lock before
/// reporting it as held rather than hanging indefinitely.
#[expect(
    clippy::duration_suboptimal_units,
    reason = "expressed in seconds to match the --lock-timeout flag's unit"
)]
pub const DEFAULT_LOCK_TIMEOUT: Duration = Duration::from_secs(300);

/// Ownership metadata stamped into a lock file once the OS lock is acquired.
///
/// Written as JSON. Legacy 0-byte locks and any unparseable content are
/// treated as "unknown owner" — see [`read_owner`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockOwner {
    pub pid: u32,
    pub hostname: String,
    pub started_at_unix: u64,
    pub version: String,
}

impl LockOwner {
    /// Capture the current process as the lock owner.
    #[must_use]
    pub fn current() -> Self {
        Self {
            pid: std::process::id(),
            hostname: current_hostname(),
            started_at_unix: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_secs()),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// Whether the owning process is still alive.
    ///
    /// Returns `None` when liveness cannot be decided safely — the owner is on
    /// a different host (shared filesystem), or the platform offers no probe.
    /// This conservative `None` prevents falsely reclaiming another host's
    /// live lock.
    #[must_use]
    pub fn liveness(&self) -> Option<bool> {
        if self.hostname != current_hostname() {
            return None;
        }
        process_is_alive(self.pid)
    }
}

/// A held lock, reported when acquisition times out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LockHeld {
    pub repo: String,
    pub owner: Option<LockOwner>,
    pub age: Duration,
    pub owner_alive: Option<bool>,
}

impl LockHeld {
    /// An actionable, user-facing message naming the repo, owner, and the
    /// exact recovery command.
    #[must_use]
    pub fn user_message(&self) -> String {
        let age = format_duration(self.age);
        let recover = format!("gather-step index --force-unlock --repo {}", self.repo);
        match (&self.owner, self.owner_alive) {
            (Some(owner), Some(true)) => format!(
                "Index lock for `{}` held {age} by pid {} (since unix {}, gather-step {}). \
                 Another gather-step index may be in progress. If you're sure it's stuck, clear it:\n  {recover}",
                self.repo, owner.pid, owner.started_at_unix, owner.version,
            ),
            (Some(owner), Some(false)) => format!(
                "Index lock for `{}` held {age} by pid {}, which is no longer running, but the \
                 lock did not release. Clear it:\n  {recover}",
                self.repo, owner.pid,
            ),
            (Some(owner), None) => format!(
                "Index lock for `{}` held {age} by pid {} on host {}. If you're sure it's stuck, \
                 clear it:\n  {recover}",
                self.repo, owner.pid, owner.hostname,
            ),
            (None, _) => format!(
                "Index lock for `{}` held {age} by an unknown owner (legacy lock file, no \
                 metadata). If you're sure it's stuck, clear it:\n  {recover}",
                self.repo,
            ),
        }
    }
}

/// Failure modes of [`acquire`].
#[derive(Debug)]
pub enum LockError {
    /// The lock was held past the timeout. Carries actionable owner details.
    Held(LockHeld),
    /// The cancellation token fired while waiting.
    Cancelled,
    /// An I/O error opening or locking the file.
    Io(std::io::Error),
}

/// RAII guard releasing the OS-advisory lock on drop.
#[derive(Debug)]
pub struct LockGuard {
    file: File,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

/// A single lock file's state, for the `status`/`doctor` "Locks" summary.
#[derive(Clone, Debug)]
pub struct LockReport {
    /// Repo name resolved from the supplied registry list, or `None` for an
    /// orphan lock whose hash matched no known repo.
    pub repo: Option<String>,
    /// The blake3 hash stem of the lock file (shown when `repo` is `None`).
    pub hash: String,
    /// Age derived from the lock file's mtime.
    pub age: Duration,
    pub owner: Option<LockOwner>,
    pub owner_alive: Option<bool>,
}

/// The single source of truth for the per-workspace lock directory:
/// `<storage_root>/locks`. All producers and scanners must derive the directory
/// here so indexing and `status`/`doctor` never diverge on where locks live.
#[must_use]
pub fn lock_dir(storage_root: &Path) -> PathBuf {
    storage_root.join("locks")
}

/// The lock file name for a repo: `<blake3(repo)>.lock`.
#[must_use]
pub fn lock_file_name(repo: &str) -> String {
    format!("{}.lock", blake3::hash(repo.as_bytes()).to_hex())
}

/// Acquire the per-repo advisory lock at `lock_path`.
///
/// Waits with exponential back-off (10–500 ms). With `timeout` set, a lock
/// still held when the deadline passes returns [`LockError::Held`] rather than
/// hanging. `force_unlock` reports a held lock immediately instead of waiting; it
/// never breaks a live lock (advisory flock cannot be safely reclaimed from the
/// outside — a dead owner's lock is already released by the kernel). On success
/// the owner metadata is stamped into the file.
pub fn acquire(
    lock_path: &Path,
    repo: &str,
    timeout: Option<Duration>,
    force_unlock: bool,
    cancel: Option<&CancellationToken>,
) -> Result<LockGuard, LockError> {
    if let Some(dir) = lock_path.parent() {
        fs::create_dir_all(dir).map_err(LockError::Io)?;
    }

    let mut file = open_lock_file(lock_path)?;

    let wait_started = Instant::now();
    let mut backoff_ms = BACKOFF_INITIAL_MS;
    loop {
        if cancel.is_some_and(CancellationToken::is_cancelled) {
            return Err(LockError::Cancelled);
        }
        match file.try_lock() {
            Ok(()) => break,
            Err(std::fs::TryLockError::WouldBlock) => {
                // A live process holds the lock. Advisory flock is released by the
                // kernel when its owner dies, so `WouldBlock` always means a genuine
                // live holder — never a dead owner's leftover. We must not break it
                // from the outside: unlinking the file only forks a second inode and
                // admits concurrent writers. `force_unlock` therefore reports the
                // held lock immediately (with manual-recovery instructions) rather
                // than waiting; otherwise we wait until the timeout.
                let deadline_passed =
                    timeout.is_some_and(|timeout| wait_started.elapsed() >= timeout);
                if force_unlock || deadline_passed {
                    let owner = read_owner(lock_path);
                    let owner_alive = owner.as_ref().and_then(LockOwner::liveness);
                    return Err(LockError::Held(LockHeld {
                        repo: repo.to_string(),
                        owner,
                        age: wait_started.elapsed(),
                        owner_alive,
                    }));
                }
                thread::sleep(Duration::from_millis(backoff_ms));
                backoff_ms = (backoff_ms * 2).min(BACKOFF_MAX_MS);
            }
            Err(std::fs::TryLockError::Error(error)) => return Err(LockError::Io(error)),
        }
    }

    let waited = wait_started.elapsed();
    if waited >= Duration::from_millis(BACKOFF_INITIAL_MS) {
        info!(
            repo,
            wait_ms = u64::try_from(waited.as_millis()).unwrap_or(u64::MAX),
            "Acquired repo index lock after waiting."
        );
    }
    if let Err(error) = stamp_owner(&mut file) {
        warn!(repo, %error, "Failed to write index lock owner metadata; continuing.");
    }
    Ok(LockGuard { file })
}

/// Read the owner metadata from a lock file. Empty/legacy/unparseable files
/// yield `None` ("unknown owner") without erroring.
#[must_use]
pub fn read_owner(lock_path: &Path) -> Option<LockOwner> {
    let bytes = fs::read(lock_path).ok()?;
    if bytes.is_empty() {
        return None;
    }
    serde_json::from_slice::<LockOwner>(&bytes).ok()
}

fn open_lock_file(lock_path: &Path) -> Result<File, LockError> {
    OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(lock_path)
        .map_err(LockError::Io)
}

/// Whether `lock_path` is currently held by some process. A nonblocking
/// `try_lock` that succeeds proves the file is a stale, unheld leftover (we
/// release it immediately); `WouldBlock` means a holder has it.
fn lock_is_held(lock_path: &Path) -> bool {
    match OpenOptions::new().read(true).write(true).open(lock_path) {
        Ok(file) => match file.try_lock() {
            Ok(()) => {
                let _ = file.unlock();
                false
            }
            Err(std::fs::TryLockError::WouldBlock) => true,
            Err(std::fs::TryLockError::Error(_)) => false,
        },
        Err(_) => false,
    }
}

/// Scan a lock directory and report the `*.lock` files that are **currently
/// held**, resolving repo names by hashing each entry in `repos` and matching
/// the file stem. Stale, released leftover files are skipped (a dropped guard
/// unlocks but does not unlink), so a released lock is never shown as active.
#[must_use]
pub fn scan_locks(lock_dir: &Path, repos: &[String]) -> Vec<LockReport> {
    let by_hash: rustc_hash::FxHashMap<String, String> = repos
        .iter()
        .map(|repo| {
            (
                blake3::hash(repo.as_bytes()).to_hex().to_string(),
                repo.clone(),
            )
        })
        .collect();

    let Ok(entries) = fs::read_dir(lock_dir) else {
        return Vec::new();
    };

    let mut reports = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("lock") {
            continue;
        }
        if !lock_is_held(&path) {
            continue;
        }
        let hash = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or_default()
            .to_string();
        let owner = read_owner(&path);
        let owner_alive = owner.as_ref().and_then(LockOwner::liveness);
        reports.push(LockReport {
            repo: by_hash.get(&hash).cloned(),
            hash,
            age: file_age(&path),
            owner,
            owner_alive,
        });
    }
    reports
}

// Written in place into the already-locked fd, not via a temp file + rename: a
// rename would swap the inode out from under the lock holder (the flock binds to
// the inode), forking a second lockable file. A concurrent reader hitting the
// brief write window falls back to "unknown owner" — benign and diagnostic-only.
fn stamp_owner(file: &mut File) -> std::io::Result<()> {
    let json = serde_json::to_vec(&LockOwner::current())?;
    file.set_len(0)?;
    file.rewind()?;
    file.write_all(&json)?;
    file.flush()
}

fn file_age(path: &Path) -> Duration {
    fs::metadata(path)
        .and_then(|meta| meta.modified())
        .ok()
        .and_then(|modified| SystemTime::now().duration_since(modified).ok())
        .unwrap_or_default()
}

fn format_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    if secs >= 60 {
        format!("{}m{:02}s", secs / 60, secs % 60)
    } else {
        format!("{secs}s")
    }
}

/// Probe whether a process is alive via `kill(pid, 0)` on Unix.
///
/// `Some(true)` alive (or alive-but-not-permitted, `EPERM`), `Some(false)`
/// dead (`ESRCH`), `None` undeterminable or non-Unix. Uses `rustix` so the
/// crate stays `forbid(unsafe_code)`-clean.
#[cfg(unix)]
#[must_use]
pub fn process_is_alive(pid: u32) -> Option<bool> {
    let raw = i32::try_from(pid).ok()?;
    let pid = rustix::process::Pid::from_raw(raw)?;
    match rustix::process::test_kill_process(pid) {
        // Alive, or alive-but-not-permitted-to-signal (EPERM).
        Ok(()) | Err(rustix::io::Errno::PERM) => Some(true),
        Err(rustix::io::Errno::SRCH) => Some(false),
        Err(_) => None,
    }
}

#[cfg(not(unix))]
#[must_use]
pub fn process_is_alive(_pid: u32) -> Option<bool> {
    None
}

#[cfg(unix)]
fn current_hostname() -> String {
    rustix::system::uname()
        .nodename()
        .to_string_lossy()
        .into_owned()
}

#[cfg(not(unix))]
fn current_hostname() -> String {
    "unknown".to_string()
}
