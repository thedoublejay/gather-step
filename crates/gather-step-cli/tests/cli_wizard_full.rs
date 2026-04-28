use std::{fs, process::Command};

use tempfile::tempdir;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_gather-step")
}

#[test]
fn init_flag_overrides_run_full_setup_without_prompting() {
    let tmp = tempdir().expect("temp dir");
    fs::create_dir_all(tmp.path().join(".git")).expect("git dir");
    fs::create_dir_all(tmp.path().join("src")).expect("src dir");
    fs::write(tmp.path().join("src/lib.rs"), "pub fn demo() -> u8 { 1 }\n").expect("source");

    let output = Command::new(bin())
        .args([
            "--workspace",
            tmp.path().to_str().expect("utf-8 temp path"),
            "init",
            "--force",
            "--index",
            "--generate-ai-files",
            "--setup-mcp",
            "local",
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
    assert!(tmp.path().join("gather-step.config.yaml").exists());
    assert!(tmp.path().join(".gather-step/registry.json").exists());
    assert!(tmp.path().join("CLAUDE.gather.md").exists());
    assert!(tmp.path().join("AGENTS.gather.md").exists());
    assert!(tmp.path().join(".claude/settings.json").exists());
}

#[test]
fn init_flag_overrides_keep_setup_mcp_idempotent() {
    let tmp = tempdir().expect("temp dir");
    fs::create_dir_all(tmp.path().join(".git")).expect("git dir");

    for _ in 0..2 {
        let output = Command::new(bin())
            .args([
                "--workspace",
                tmp.path().to_str().expect("utf-8 temp path"),
                "init",
                "--force",
                "--no-index",
                "--no-generate-ai-files",
                "--setup-mcp",
                "local",
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
    }

    let settings = fs::read_to_string(tmp.path().join(".claude/settings.json")).expect("settings");
    let value: serde_json::Value = serde_json::from_str(&settings).expect("settings json");
    assert_eq!(
        value["mcpServers"]
            .as_object()
            .expect("mcpServers object")
            .len(),
        1
    );
}
