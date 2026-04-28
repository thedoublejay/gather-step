use std::process::Command;

use tempfile::tempdir;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_gather-step")
}

#[test]
fn no_args_non_interactive_prints_help() {
    let tmp = tempdir().expect("temp dir");

    let output = Command::new(bin())
        .args(["--workspace", tmp.path().to_str().expect("utf-8 temp path")])
        .output()
        .expect("command should run");

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Usage: gather-step"));
    assert!(stdout.contains("init"));
    assert!(stdout.contains("--no-interactive"));
}

#[test]
fn no_args_json_mode_prints_help_without_prompting() {
    let tmp = tempdir().expect("temp dir");

    let output = Command::new(bin())
        .args([
            "--workspace",
            tmp.path().to_str().expect("utf-8 temp path"),
            "--json",
        ])
        .output()
        .expect("command should run");

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("Usage: gather-step"));
}
