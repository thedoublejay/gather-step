use std::{fs, process::Command};

use tempfile::tempdir;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_gather-step")
}

#[test]
fn init_existing_config_requires_force() {
    let tmp = tempdir().expect("temp dir");
    fs::create_dir_all(tmp.path().join(".git")).expect("git dir");
    let config_path = tmp.path().join("gather-step.config.yaml");
    fs::write(&config_path, "repos: []\n").expect("config");

    let output = Command::new(bin())
        .args([
            "--workspace",
            tmp.path().to_str().expect("utf-8 temp path"),
            "init",
        ])
        .output()
        .expect("command should run");

    assert!(
        !output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("pass --force"));
    assert_eq!(
        fs::read_to_string(config_path).expect("config"),
        "repos: []\n"
    );
}

#[test]
fn init_force_non_interactive_writes_config_without_optional_steps() {
    let tmp = tempdir().expect("temp dir");
    fs::create_dir_all(tmp.path().join(".git")).expect("git dir");
    fs::write(tmp.path().join("gather-step.config.yaml"), "repos: []\n").expect("config");

    let output = Command::new(bin())
        .args([
            "--workspace",
            tmp.path().to_str().expect("utf-8 temp path"),
            "init",
            "--force",
            "--no-index",
            "--no-watch",
            "--no-generate-ai-files",
        ])
        .output()
        .expect("command should run");

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let config = fs::read_to_string(tmp.path().join("gather-step.config.yaml")).expect("config");
    assert!(config.contains("repos:"));
    assert!(config.contains("path: ."));
    assert!(!tmp.path().join(".gather-step/registry.json").exists());
}

#[test]
fn init_generate_ai_files_without_index_writes_summaries_and_skips_rules() {
    let tmp = tempdir().expect("temp dir");
    fs::create_dir_all(tmp.path().join(".git")).expect("git dir");

    let output = Command::new(bin())
        .args([
            "--workspace",
            tmp.path().to_str().expect("utf-8 temp path"),
            "init",
            "--force",
            "--no-index",
            "--generate-ai-files",
            "--no-watch",
        ])
        .output()
        .expect("command should run");

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("warning: skipped .claude/rules/ generation"));
    assert!(tmp.path().join("CLAUDE.gather.md").exists());
    assert!(tmp.path().join("AGENTS.gather.md").exists());
    assert!(!tmp.path().join(".claude/rules").exists());
}
