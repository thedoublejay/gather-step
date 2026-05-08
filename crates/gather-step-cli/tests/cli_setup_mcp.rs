use gather_step::commands::setup_mcp::write_settings;

#[test]
fn writes_workspace_pinned_block_to_local_settings() {
    let temp = tempfile::tempdir().expect("temp dir");
    let workspace = temp.path().to_path_buf();
    let settings_path = workspace.join(".claude/settings.json");

    write_settings(&settings_path, &workspace).expect("settings should write");

    let body = std::fs::read_to_string(&settings_path).expect("settings body");
    let value: serde_json::Value = serde_json::from_str(&body).expect("settings json");
    assert_eq!(value["mcpServers"]["gather-step"]["command"], "gather-step");
    assert_eq!(
        value["mcpServers"]["gather-step"]["args"],
        serde_json::json!(["--workspace", workspace.to_str().unwrap(), "serve"])
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

    write_settings(&settings_path, &workspace).expect("first write");
    write_settings(&settings_path, &workspace).expect("second write");

    let body = std::fs::read_to_string(&settings_path).expect("settings body");
    let value: serde_json::Value = serde_json::from_str(&body).expect("settings json");
    assert_eq!(value["otherKey"], "keep");
    assert_eq!(value["mcpServers"]["existing"]["command"], "x");
    assert_eq!(value["mcpServers"]["gather-step"]["command"], "gather-step");
}

#[test]
fn malformed_existing_settings_json_returns_error_and_preserves_file() {
    let temp = tempfile::tempdir().expect("temp dir");
    let workspace = temp.path().to_path_buf();
    let settings_path = workspace.join(".claude/settings.json");
    std::fs::create_dir_all(settings_path.parent().expect("settings parent"))
        .expect("settings parent");
    let original = "{not valid json";
    std::fs::write(&settings_path, original).expect("seed settings");

    let error = write_settings(&settings_path, &workspace).expect_err("malformed json errors");

    assert!(error.to_string().contains("parsing"));
    assert_eq!(
        std::fs::read_to_string(&settings_path).expect("settings body"),
        original
    );
}

#[test]
fn existing_gather_step_entry_is_replaced_not_merged() {
    let temp = tempfile::tempdir().expect("temp dir");
    let workspace = temp.path().to_path_buf();
    let settings_path = workspace.join(".claude/settings.json");
    std::fs::create_dir_all(settings_path.parent().expect("settings parent"))
        .expect("settings parent");
    std::fs::write(
        &settings_path,
        r#"{"mcpServers":{"gather-step":{"command":"/old/path","args":["mcp","serve"],"env":{"OLD":"1"}}}}"#,
    )
    .expect("seed settings");

    write_settings(&settings_path, &workspace).expect("settings should write");

    let body = std::fs::read_to_string(&settings_path).expect("settings body");
    let value: serde_json::Value = serde_json::from_str(&body).expect("settings json");
    let entry = &value["mcpServers"]["gather-step"];
    assert_eq!(entry["command"], "gather-step");
    assert_eq!(
        entry["args"],
        serde_json::json!(["--workspace", workspace.to_str().unwrap(), "serve"])
    );
    assert!(entry.get("env").is_none());
}

#[test]
fn non_object_mcp_servers_returns_error_and_preserves_file() {
    let temp = tempfile::tempdir().expect("temp dir");
    let workspace = temp.path().to_path_buf();
    let settings_path = workspace.join(".claude/settings.json");
    std::fs::create_dir_all(settings_path.parent().expect("settings parent"))
        .expect("settings parent");
    let original = r#"{"mcpServers":[]}"#;
    std::fs::write(&settings_path, original).expect("seed settings");

    let error = write_settings(&settings_path, &workspace).expect_err("bad mcpServers errors");

    assert!(error.to_string().contains("mcpServers is not an object"));
    assert_eq!(
        std::fs::read_to_string(&settings_path).expect("settings body"),
        original
    );
}
