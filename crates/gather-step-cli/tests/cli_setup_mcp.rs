use gather_step::commands::setup_mcp::{write_codex_config, write_settings};

#[test]
fn writes_workspace_pinned_block_to_mcp_json() {
    let temp = tempfile::tempdir().expect("temp dir");
    let workspace = temp.path().to_path_buf();
    let settings_path = workspace.join(".mcp.json");

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
    let settings_path = workspace.join(".mcp.json");
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
    let settings_path = workspace.join(".mcp.json");
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
    let settings_path = workspace.join(".mcp.json");
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
    let settings_path = workspace.join(".mcp.json");
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

#[test]
fn writes_codex_server_block_to_config_toml() {
    let temp = tempfile::tempdir().expect("temp dir");
    let workspace = temp.path().to_path_buf();
    let config_path = workspace.join(".codex/config.toml");

    write_codex_config(&config_path, &workspace).expect("codex config should write");

    let body = std::fs::read_to_string(&config_path).expect("config body");
    let value: toml::Value = toml::from_str(&body).expect("config toml");
    assert_eq!(
        value["mcp_servers"]["gather-step"]["command"]
            .as_str()
            .unwrap(),
        "gather-step"
    );
    let args = value["mcp_servers"]["gather-step"]["args"]
        .as_array()
        .expect("args array");
    let args: Vec<&str> = args.iter().map(|v| v.as_str().unwrap()).collect();
    assert_eq!(args, ["--workspace", workspace.to_str().unwrap(), "serve"]);
}

#[test]
fn codex_merge_preserves_other_servers_and_comments() {
    let temp = tempfile::tempdir().expect("temp dir");
    let workspace = temp.path().to_path_buf();
    let config_path = workspace.join(".codex/config.toml");
    std::fs::create_dir_all(config_path.parent().expect("config parent")).expect("config parent");
    std::fs::write(
        &config_path,
        "# top comment\nmodel = \"o3\"\n\n[mcp_servers.other]\ncommand = \"x\"\n",
    )
    .expect("seed config");

    write_codex_config(&config_path, &workspace).expect("first write");
    write_codex_config(&config_path, &workspace).expect("second write");

    let body = std::fs::read_to_string(&config_path).expect("config body");
    assert!(body.contains("# top comment"));
    let value: toml::Value = toml::from_str(&body).expect("config toml");
    assert_eq!(value["model"].as_str().unwrap(), "o3");
    assert_eq!(
        value["mcp_servers"]["other"]["command"].as_str().unwrap(),
        "x"
    );
    assert_eq!(
        value["mcp_servers"]["gather-step"]["command"]
            .as_str()
            .unwrap(),
        "gather-step"
    );
}

#[test]
fn codex_non_table_mcp_servers_returns_error_and_preserves_file() {
    let temp = tempfile::tempdir().expect("temp dir");
    let workspace = temp.path().to_path_buf();
    let config_path = workspace.join(".codex/config.toml");
    std::fs::create_dir_all(config_path.parent().expect("config parent")).expect("config parent");
    let original = "mcp_servers = 1\n";
    std::fs::write(&config_path, original).expect("seed config");

    let error =
        write_codex_config(&config_path, &workspace).expect_err("non-table mcp_servers errors");

    assert!(error.to_string().contains("mcp_servers is not a table"));
    assert_eq!(
        std::fs::read_to_string(&config_path).expect("config body"),
        original
    );
}
