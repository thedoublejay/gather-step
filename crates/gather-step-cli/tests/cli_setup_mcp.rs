use std::path::PathBuf;

use gather_step::commands::setup_mcp::write_settings;

#[test]
fn writes_workspace_pinned_block_to_local_settings() {
    let temp = tempfile::tempdir().expect("temp dir");
    let workspace = temp.path().to_path_buf();
    let settings_path = workspace.join(".claude/settings.json");

    write_settings(
        &settings_path,
        &workspace,
        &PathBuf::from("/usr/local/bin/gather-step"),
    )
    .expect("settings should write");

    let body = std::fs::read_to_string(&settings_path).expect("settings body");
    let value: serde_json::Value = serde_json::from_str(&body).expect("settings json");
    assert_eq!(
        value["mcpServers"]["gather-step"]["command"],
        "/usr/local/bin/gather-step"
    );
    assert_eq!(
        value["mcpServers"]["gather-step"]["args"],
        serde_json::json!(["--workspace", workspace.to_str().unwrap(), "mcp", "serve"])
    );
}

#[test]
fn idempotent_merge_preserves_other_keys() {
    let temp = tempfile::tempdir().expect("temp dir");
    let workspace = temp.path().to_path_buf();
    let settings_path = workspace.join(".claude/settings.json");
    std::fs::create_dir_all(settings_path.parent().expect("settings parent"))
        .expect("settings parent");
    std::fs::write(
        &settings_path,
        r#"{"otherKey":"keep","mcpServers":{"existing":{"command":"x"}}}"#,
    )
    .expect("seed settings");

    let exe = PathBuf::from("/usr/local/bin/gather-step");
    write_settings(&settings_path, &workspace, &exe).expect("first write");
    write_settings(&settings_path, &workspace, &exe).expect("second write");

    let body = std::fs::read_to_string(&settings_path).expect("settings body");
    let value: serde_json::Value = serde_json::from_str(&body).expect("settings json");
    assert_eq!(value["otherKey"], "keep");
    assert_eq!(value["mcpServers"]["existing"]["command"], "x");
    assert_eq!(
        value["mcpServers"]["gather-step"]["command"],
        "/usr/local/bin/gather-step"
    );
}
