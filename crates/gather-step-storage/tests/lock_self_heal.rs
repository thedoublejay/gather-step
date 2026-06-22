//! v5.2 stale-lock self-heal: owner metadata, bounded acquisition, liveness, scan.

use std::fs;
use std::time::Duration;

use gather_step_storage::lock::{self, LockError, LockHeld, LockOwner};

fn lock_path(dir: &std::path::Path, repo: &str) -> std::path::PathBuf {
    dir.join(lock::lock_file_name(repo))
}

#[test]
fn acquire_fresh_lock_stamps_owner_metadata() {
    let tmp = tempfile::tempdir().unwrap();
    let path = lock_path(tmp.path(), "repo-a");

    let guard = lock::acquire(&path, "repo-a", None, false, None).expect("fresh acquire succeeds");

    let owner = lock::read_owner(&path).expect("metadata written");
    assert_eq!(owner.pid, std::process::id());
    assert!(!owner.version.is_empty());
    assert!(!owner.hostname.is_empty());
    drop(guard);
}

#[test]
fn contended_lock_times_out_and_reports_live_owner() {
    let tmp = tempfile::tempdir().unwrap();
    let path = lock_path(tmp.path(), "repo-b");

    let _held = lock::acquire(&path, "repo-b", None, false, None).expect("first acquire");

    let err = lock::acquire(
        &path,
        "repo-b",
        Some(Duration::from_millis(80)),
        false,
        None,
    )
    .expect_err("second acquire must time out");

    match err {
        LockError::Held(held) => {
            assert_eq!(held.repo, "repo-b");
            assert!(held.age >= Duration::from_millis(80), "age={:?}", held.age);
            let owner = held.owner.expect("owner metadata present");
            assert_eq!(owner.pid, std::process::id());
            assert_eq!(held.owner_alive, Some(true), "current process is alive");
        }
        other => panic!("expected Held, got {other:?}"),
    }
}

#[test]
fn force_unlock_does_not_steal_a_live_lock() {
    // A lock held by a live owner must NOT be bypassed even with force_unlock:
    // stealing it would let two indexers run concurrently and corrupt state.
    let tmp = tempfile::tempdir().unwrap();
    let path = lock_path(tmp.path(), "repo-c");

    let _held = lock::acquire(&path, "repo-c", None, false, None).expect("first acquire");

    let err = lock::acquire(&path, "repo-c", Some(Duration::from_millis(80)), true, None)
        .expect_err("force-unlock must not steal a live lock");
    assert!(matches!(err, LockError::Held(_)), "got {err:?}");
}

#[test]
fn acquire_reclaims_free_lock_file_with_stale_metadata() {
    // A leftover lock file from a dead process is not OS-held (the kernel
    // releases advisory locks on exit), so acquire reuses it and overwrites the
    // stale owner metadata — dead-owner self-heal, no force needed.
    let tmp = tempfile::tempdir().unwrap();
    let path = lock_path(tmp.path(), "repo-d");
    fs::write(
        &path,
        br#"{"pid":999999,"hostname":"old-host","started_at_unix":1,"version":"0.0.0"}"#,
    )
    .unwrap();

    let guard = lock::acquire(&path, "repo-d", Some(Duration::from_millis(80)), false, None)
        .expect("a free, stale lock file is reclaimed");
    let owner = lock::read_owner(&path).expect("metadata refreshed");
    assert_eq!(owner.pid, std::process::id());
    drop(guard);
}

#[test]
fn read_owner_tolerates_legacy_and_garbage_files() {
    let tmp = tempfile::tempdir().unwrap();

    let empty = tmp.path().join("empty.lock");
    fs::write(&empty, b"").unwrap();
    assert!(lock::read_owner(&empty).is_none());

    let garbage = tmp.path().join("garbage.lock");
    fs::write(&garbage, b"not json at all").unwrap();
    assert!(lock::read_owner(&garbage).is_none());

    let missing = tmp.path().join("missing.lock");
    assert!(lock::read_owner(&missing).is_none());
}

#[cfg(unix)]
#[test]
fn process_liveness_probe_distinguishes_alive_and_dead() {
    assert_eq!(lock::process_is_alive(std::process::id()), Some(true));
    // A very high PID is overwhelmingly unlikely to exist.
    assert_eq!(lock::process_is_alive(0x7FFF_FFFE), Some(false));
}

#[test]
fn user_message_for_live_owner_names_repo_pid_and_recovery() {
    let held = LockHeld {
        repo: "label-review".to_string(),
        owner: Some(LockOwner {
            pid: 48217,
            hostname: "host-x".to_string(),
            started_at_unix: 0,
            version: "5.2.0".to_string(),
        }),
        age: Duration::from_secs(363),
        owner_alive: Some(true),
    };
    let msg = held.user_message();
    assert!(msg.contains("label-review"), "msg={msg}");
    assert!(msg.contains("48217"), "msg={msg}");
    assert!(msg.contains("--force-unlock"), "msg={msg}");
}

#[test]
fn user_message_for_unknown_owner_is_actionable() {
    let held = LockHeld {
        repo: "label-review".to_string(),
        owner: None,
        age: Duration::from_secs(363),
        owner_alive: None,
    };
    let msg = held.user_message();
    assert!(msg.contains("unknown owner"), "msg={msg}");
    assert!(msg.contains("--force-unlock"), "msg={msg}");
}

#[test]
fn scan_locks_resolves_repo_name_from_registry_list() {
    let tmp = tempfile::tempdir().unwrap();
    let path = lock_path(tmp.path(), "web-api-gateway");
    let _held = lock::acquire(&path, "web-api-gateway", None, false, None).expect("acquire");

    let reports = lock::scan_locks(
        tmp.path(),
        &["web-api-gateway".to_string(), "other".to_string()],
    );

    let report = reports
        .iter()
        .find(|r| r.repo.as_deref() == Some("web-api-gateway"))
        .expect("a held lock is resolved to its repo name");
    assert!(report.owner.is_some());
    assert_eq!(report.owner_alive, Some(true));
}

#[test]
fn scan_locks_skips_released_lock_files() {
    // A dropped guard releases the OS lock but leaves the file behind. A stale,
    // unheld lock file must NOT be reported as an active lock.
    let tmp = tempfile::tempdir().unwrap();
    let path = lock_path(tmp.path(), "repo-e");
    let guard = lock::acquire(&path, "repo-e", None, false, None).expect("acquire");
    drop(guard);
    assert!(path.exists(), "the lock file remains after the guard drops");

    let reports = lock::scan_locks(tmp.path(), &["repo-e".to_string()]);
    assert!(
        reports.iter().all(|r| r.repo.as_deref() != Some("repo-e")),
        "a released lock must not be reported as held: {reports:?}"
    );
}
