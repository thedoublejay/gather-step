use std::{fs, process::Command};

use tempfile::tempdir;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_gather-step")
}

#[test]
fn init_existing_config_is_reused_without_force() {
    let tmp = tempdir().expect("temp dir");
    fs::create_dir_all(tmp.path().join("api/.git")).expect("api git dir");
    fs::create_dir_all(tmp.path().join("web/.git")).expect("web git dir");
    let config_path = tmp.path().join("gather-step.config.yaml");
    let config = "repos:\n- name: api\n  path: api\n  depth: level2\nindexing:\n  exclude:\n  - node_modules\n  - web\n";
    fs::write(&config_path, config).expect("config");

    let output = Command::new(bin())
        .args([
            "--workspace",
            tmp.path().to_str().expect("utf-8 temp path"),
            "init",
        ])
        .output()
        .expect("command should run");

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("Using existing config"));
    assert_eq!(fs::read_to_string(config_path).expect("config"), config);
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
    assert!(stdout.contains("Warning: Skipped generating .claude/rules/"));
    assert!(tmp.path().join("CLAUDE.gather.md").exists());
    assert!(tmp.path().join("AGENTS.gather.md").exists());
    assert!(!tmp.path().join(".claude/rules").exists());
}
