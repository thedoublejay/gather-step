#![cfg(unix)]

use std::{
    env, fs,
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
    process::{self, Child, Command, Stdio},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use serde_json::Value;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(name: &str) -> Self {
        let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = PathBuf::from("/tmp").join(format!("gs-cw-{name}-{}-{id}", process::id()));
        fs::create_dir_all(&path).expect("temp dir should exist");
        Self { path }
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

fn gather_step() -> Command {
    Command::new(env!("CARGO_BIN_EXE_gather-step"))
}

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures")
        .canonicalize()
        .expect("fixture root should resolve")
}

fn copy_dir_all(from: &Path, to: &Path) {
    fs::create_dir_all(to).expect("destination directory should exist");
    for entry in fs::read_dir(from).expect("source directory should be readable") {
        let entry = entry.expect("directory entry should load");
        if entry.file_name() == ".gather-step" {
            continue;
        }
        let file_type = entry.file_type().expect("file type should load");
        assert!(
            !file_type.is_symlink(),
            "fixture must not contain symlinks (found: {})",
            entry.path().display()
        );
        let from_path = entry.path();
        let to_path = to.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_all(&from_path, &to_path);
        } else {
            fs::copy(&from_path, &to_path).expect("fixture file should copy");
        }
    }
}

fn stage_fixture_workspace() -> TempDir {
    let temp = TempDir::new("workspace");
    copy_dir_all(&fixture_root(), temp.path());
    temp
}

fn run_ok_json(workspace: &Path, args: &[&str]) -> Value {
    let output = gather_step()
        .arg("--workspace")
        .arg(workspace)
        .arg("--json")
        .arg("--no-banner")
        .args(args)
        .output()
        .expect("command should run");

    assert!(
        output.status.success(),
        "command failed: {}\nstdout:\n{}\nstderr:\n{}",
        args.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    serde_json::from_slice(&output.stdout).expect("stdout should contain valid json")
}

fn run_ok_json_retry(workspace: &Path, args: &[&str]) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let output = gather_step()
            .arg("--workspace")
            .arg(workspace)
            .arg("--json")
            .arg("--no-banner")
            .args(args)
            .output()
            .expect("command should run");
        if output.status.success() {
            return serde_json::from_slice(&output.stdout)
                .expect("stdout should contain valid json");
        }
        assert!(
            Instant::now() < deadline,
            "command failed: {}\nstdout:\n{}\nstderr:\n{}",
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        thread::sleep(Duration::from_millis(50));
    }
}

fn run_fail_text(workspace: &Path, args: &[&str]) -> String {
    let output = gather_step()
        .arg("--workspace")
        .arg(workspace)
        .arg("--no-banner")
        .args(args)
        .output()
        .expect("command should run");

    assert!(
        !output.status.success(),
        "command unexpectedly succeeded: {}\nstdout:\n{}\nstderr:\n{}",
        args.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn read_child_stderr(child: &mut Child) -> String {
    let mut stderr = String::new();
    if let Some(mut pipe) = child.stderr.take() {
        let _ = pipe.read_to_string(&mut stderr);
    }
    stderr
}

fn spawn_stdout_lines(child: &mut Child) -> mpsc::Receiver<String> {
    let stdout = child.stdout.take().expect("child stdout should be piped");
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else {
                break;
            };
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    rx
}

fn daemon_bind_unavailable_in_test_env(stderr: &str) -> bool {
    // The CLI's stable operator error intentionally hides the OS errno, so a
    // sandboxed Unix-socket bind failure only reaches this test as the bind
    // context. The daemon unit tests still assert non-permission bind errors.
    stderr.trim_start().starts_with("binding ") && stderr.contains(".gather-step/daemon.sock")
}

fn watcher_backend_unavailable_in_test_env(stderr: &str) -> bool {
    stderr.contains("filesystem watcher failed: unable to start the FSEvent stream")
        || stderr.contains("filesystem watcher failed: unable to start FSEvent stream")
        || stderr.contains("unable to start FSEvent stream")
}

fn wait_for_daemon_or_skip(child: &mut Child, daemon_pid: &Path, daemon_sock: &Path) -> bool {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if daemon_pid.exists() && daemon_sock.exists() {
            return true;
        }
        if let Some(status) = child.try_wait().expect("child status should load") {
            let stderr = read_child_stderr(child);
            if daemon_bind_unavailable_in_test_env(&stderr)
                || watcher_backend_unavailable_in_test_env(&stderr)
            {
                return false;
            }
            panic!(
                "child exited before daemon files appeared: {status}\nmissing_pid={}\nmissing_sock={}\nstderr:\n{}",
                !daemon_pid.exists(),
                !daemon_sock.exists(),
                stderr
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
    let _ = child.kill();
    let _ = child.wait();
    let stderr = read_child_stderr(child);
    panic!(
        "timed out waiting for daemon files\nmissing_pid={}\nmissing_sock={}\nstderr:\n{}",
        !daemon_pid.exists(),
        !daemon_sock.exists(),
        stderr
    );
}

fn wait_for_child_exit(child: &mut Child) -> process::ExitStatus {
    drop(child.stdin.take());
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("child status should load") {
            return status;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let _ = child.kill();
    let _ = child.wait();
    let stderr = read_child_stderr(child);
    panic!("timed out waiting for child exit\nstderr:\n{stderr}");
}

#[test]
fn watch_json_ready_event_allows_scripts_to_avoid_startup_race() {
    let workspace = stage_fixture_workspace();
    run_ok_json(workspace.path(), &["index"]);

    let mut child = gather_step()
        .arg("--workspace")
        .arg(workspace.path())
        .arg("--json")
        .arg("--no-banner")
        .arg("watch")
        .arg("--debounce-ms")
        .arg("100")
        .arg("--poll-interval-ms")
        .arg("100")
        .arg("1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("watch should spawn");

    let stdout_lines = spawn_stdout_lines(&mut child);
    let ready_line = match stdout_lines.recv_timeout(Duration::from_secs(10)) {
        Ok(line) => line,
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            let stderr = read_child_stderr(&mut child);
            if daemon_bind_unavailable_in_test_env(&stderr)
                || watcher_backend_unavailable_in_test_env(&stderr)
            {
                return;
            }
            panic!("watch did not emit ready event: {error}\nstderr:\n{stderr}");
        }
    };
    let ready: Value = serde_json::from_str(&ready_line).expect("ready line should be json");
    assert_eq!(ready["event"], "watch_ready");

    let changed_file = workspace
        .path()
        .join("workspace/service_a/src/payment.service.ts");
    fs::write(
        &changed_file,
        format!(
            "{}\n// gather-step watch readiness regression\n",
            fs::read_to_string(&changed_file).expect("fixture file should read")
        ),
    )
    .expect("fixture file should update");

    let exited = wait_for_child_exit(&mut child);
    let stderr = read_child_stderr(&mut child);
    assert!(
        exited.success(),
        "watch should exit after one indexing run\nstatus: {exited}\nstderr:\n{stderr}"
    );

    let mut events = vec![ready];
    while let Ok(line) = stdout_lines.recv_timeout(Duration::from_millis(100)) {
        events.push(serde_json::from_str(&line).expect("watch event should be json"));
    }
    assert!(
        events
            .iter()
            .any(|event| event["event"] == "watch_indexing_start"),
        "missing watch_indexing_start in events: {events:#?}"
    );
    assert!(
        events
            .iter()
            .any(|event| event["event"] == "watch_indexing_complete"),
        "missing watch_indexing_complete in events: {events:#?}"
    );
    assert!(
        events.iter().any(|event| event["event"] == "watch_status"),
        "missing watch_status in events: {events:#?}"
    );
}

#[test]
fn serve_watch_proxies_read_only_commands_and_cleans_up_daemon_files() {
    let workspace = stage_fixture_workspace();
    run_ok_json(workspace.path(), &["index"]);

    let search_before = run_ok_json(workspace.path(), &["search", "OrderController"]);
    let status_before = run_ok_json(workspace.path(), &["status"]);
    let trace_before = run_ok_json(
        workspace.path(),
        &["trace", "crud", "--method", "GET", "--path", "/orders"],
    );
    let doctor_before = run_ok_json(workspace.path(), &["doctor"]);

    let mut child = gather_step()
        .arg("--workspace")
        .arg(workspace.path())
        .arg("--no-banner")
        .arg("serve")
        .arg("--watch")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("serve --watch should spawn");

    let daemon_pid = workspace.path().join(".gather-step/daemon.pid");
    let daemon_sock = workspace.path().join(".gather-step/daemon.sock");
    if !wait_for_daemon_or_skip(&mut child, &daemon_pid, &daemon_sock) {
        return;
    }

    let search_during = run_ok_json_retry(workspace.path(), &["search", "OrderController"]);
    let status_during = run_ok_json_retry(workspace.path(), &["status"]);
    let trace_during = run_ok_json_retry(
        workspace.path(),
        &["trace", "crud", "--method", "GET", "--path", "/orders"],
    );
    let doctor_during = run_ok_json_retry(workspace.path(), &["doctor"]);

    assert_eq!(search_before, search_during);
    assert_eq!(status_before, status_during);
    assert_eq!(trace_before, trace_during);
    assert_eq!(doctor_before, doctor_during);

    let status = Command::new("kill")
        .arg("-INT")
        .arg(child.id().to_string())
        .status()
        .expect("kill should run");
    assert!(status.success(), "kill -INT should succeed");

    let exited = wait_for_child_exit(&mut child);
    let stderr = read_child_stderr(&mut child);
    assert!(
        exited.success(),
        "The serve --watch command should exit cleanly.\nStderr:\n{stderr}"
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && (daemon_pid.exists() || daemon_sock.exists()) {
        thread::sleep(Duration::from_millis(50));
    }
    assert!(!daemon_pid.exists(), "daemon pid file should be cleaned up");
    assert!(!daemon_sock.exists(), "daemon socket should be cleaned up");
}

#[test]
fn watch_rejects_concurrent_index_with_storage_held_error_and_cleans_up_daemon_files() {
    let workspace = stage_fixture_workspace();
    run_ok_json(workspace.path(), &["index"]);

    let mut child = gather_step()
        .arg("--workspace")
        .arg(workspace.path())
        .arg("--no-banner")
        .arg("watch")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("watch should spawn");

    let daemon_pid = workspace.path().join(".gather-step/daemon.pid");
    let daemon_sock = workspace.path().join(".gather-step/daemon.sock");
    if !wait_for_daemon_or_skip(&mut child, &daemon_pid, &daemon_sock) {
        return;
    }

    let stderr = run_fail_text(workspace.path(), &["index"]);
    assert!(
        stderr.contains("Another gather-step process is using this workspace")
            && stderr.contains("Stop `gather-step watch` or `gather-step serve --watch`"),
        "expected actionable storage-held error, got:\n{stderr}"
    );

    let status = Command::new("kill")
        .arg("-INT")
        .arg(child.id().to_string())
        .status()
        .expect("kill should run");
    assert!(status.success(), "kill -INT should succeed");

    let exited = wait_for_child_exit(&mut child);
    let stderr = read_child_stderr(&mut child);
    if !exited.success() && watcher_backend_unavailable_in_test_env(&stderr) {
        return;
    }
    assert!(
        exited.success(),
        "The watch command should exit cleanly.\nStatus: {exited}\nStderr:\n{stderr}"
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && (daemon_pid.exists() || daemon_sock.exists()) {
        thread::sleep(Duration::from_millis(50));
    }
    assert!(!daemon_pid.exists(), "daemon pid file should be cleaned up");
    assert!(!daemon_sock.exists(), "daemon socket should be cleaned up");
}
