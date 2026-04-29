use std::process::Command;

use tempfile::tempdir;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_gather-step")
}

#[test]
fn index_accepts_watch_flag() {
    let tmp = tempdir().expect("temp dir");

    let output = Command::new(bin())
        .args([
            "--workspace",
            tmp.path().to_str().expect("utf-8 temp path"),
            "index",
            "--watch",
            "--help",
        ])
        .output()
        .expect("command should run");

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("--watch"));
}

#[test]
fn watch_help_does_not_advertise_tui_until_live_dashboard_exists() {
    let tmp = tempdir().expect("temp dir");

    let output = Command::new(bin())
        .args([
            "--workspace",
            tmp.path().to_str().expect("utf-8 temp path"),
            "watch",
            "--help",
        ])
        .output()
        .expect("command should run");

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains("--tui"));
}
