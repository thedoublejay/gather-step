use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{self, Command},
    sync::atomic::{AtomicU64, Ordering},
};

use gather_step_storage::{FileAnalytics, GraphStoreDb, MetadataStore, MetadataStoreDb};
use rusqlite::Connection;
use serde_json::Value;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(name: &str) -> Self {
        let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path =
            env::temp_dir().join(format!("gather-step-cli-it-{name}-{}-{id}", process::id()));
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

fn run_ok(workspace: &Path, args: &[&str]) -> process::Output {
    let output = gather_step()
        .arg("--workspace")
        .arg(workspace)
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

    output
}

fn run_fail(workspace: &Path, args: &[&str]) -> process::Output {
    let output = gather_step()
        .arg("--workspace")
        .arg(workspace)
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

    output
}

fn stdout_json(output: &process::Output) -> Value {
    serde_json::from_slice(&output.stdout).expect("stdout should contain valid json")
}

#[test]
fn no_args_non_interactive_prints_help() {
    let temp = TempDir::new("no-args-help");

    let output = run_ok(temp.path(), &[]);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(stdout.contains("Usage: gather-step"));
    assert!(stdout.contains("init"));
    assert!(stdout.contains("--no-interactive"));
}

#[test]
fn setup_mcp_local_writes_workspace_settings() {
    let temp = TempDir::new("setup-mcp-local");

    let output = run_ok(temp.path(), &["setup-mcp", "--scope", "local"]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let settings_path = temp.path().join(".claude/settings.json");
    let settings = fs::read_to_string(&settings_path).expect("settings file should be written");
    let value: Value = serde_json::from_str(&settings).expect("settings json");

    assert!(stdout.contains("Updated"));
    assert_eq!(
        value["mcpServers"]["gather-step"]["args"],
        serde_json::json!([
            "--workspace",
            fs::canonicalize(temp.path())
                .expect("canonical workspace")
                .to_str()
                .expect("utf-8 temp path"),
            "mcp",
            "serve"
        ])
    );
}

fn write_fixture_workspace(root: &Path) {
    let backend = root.join("apps/backend_standard");
    let frontend = root.join("apps/frontend_standard");
    fs::create_dir_all(backend.join(".git")).expect("backend git dir");
    fs::create_dir_all(frontend.join(".git")).expect("frontend git dir");
    fs::create_dir_all(backend.join("src")).expect("backend src");
    fs::create_dir_all(frontend.join("src")).expect("frontend src");

    fs::write(
        backend.join("package.json"),
        r#"{
  "name": "backend-standard",
  "dependencies": {
    "@nestjs/common": "^11.0.0",
    "@nestjs/core": "^11.0.0",
    "@nestjs/microservices": "^11.0.0"
  }
}"#,
    )
    .expect("backend package");
    fs::write(
        frontend.join("package.json"),
        r#"{
  "name": "frontend-standard",
  "dependencies": {
    "react": "^19.0.0",
    "react-router-dom": "^7.0.0"
  }
}"#,
    )
    .expect("frontend package");
    fs::write(
        backend.join("src/controller.ts"),
        r"
import { Controller, Get } from '@nestjs/common';
import { MessagePattern } from '@nestjs/microservices';

@Controller('orders')
export class ServiceAController {
  constructor(private readonly bus: EventBusClient) {}

  @Get()
  listOrders() {
    this.bus.send('order.created', {});
    return [];
  }

  @MessagePattern(['order.created'])
  handleOrderCreated(payload: {}) {
    return payload;
  }
}
",
    )
    .expect("backend source");
    fs::write(
        frontend.join("src/OrderList.tsx"),
        r"
export function OrderList() {
  return <div>Orders</div>;
}
",
    )
    .expect("frontend source");
}

#[test]
fn cli_commands_work_on_indexed_fixture_workspace() {
    let temp = TempDir::new("cli-commands");
    write_fixture_workspace(temp.path());

    let init = run_ok(temp.path(), &["init"]);
    let init_stdout = String::from_utf8_lossy(&init.stdout);
    assert!(init_stdout.contains("2 configured repositories"));
    assert!(!init_stdout.contains("backend_standard"));
    assert!(!init_stdout.contains("frontend_ui"));
    assert!(temp.path().join("gather-step.config.yaml").exists());

    let index = run_ok(temp.path(), &["--json", "index"]);
    let index_json = stdout_json(&index);
    assert_eq!(index_json["event"], "index_completed");
    for key in [
        "graph_build_ms",
        "parser_augment_ms",
        "pack_precompute_ms",
        "metadata_persist_ms",
    ] {
        assert!(
            index_json["timings"][key].is_u64(),
            "index timings.{key} must be numeric"
        );
    }
    assert_eq!(index_json["repos"][0]["git_analytics_status"], "degraded");
    assert!(
        index_json["warnings"]
            .as_array()
            .is_some_and(|warnings| !warnings.is_empty())
    );

    let status = run_ok(temp.path(), &["status", "--json"]);
    let status_json = stdout_json(&status);
    assert_eq!(status_json["event"], "status_completed");
    assert!(
        !status_json["repos"]
            .as_array()
            .expect("repos array")
            .is_empty()
    );

    let search = run_ok(temp.path(), &["search", "OrderList", "--json"]);
    let search_json = stdout_json(&search);
    assert_eq!(search_json["event"], "search_completed");
    assert!(
        search_json["hits"]
            .as_array()
            .expect("hits array")
            .iter()
            .any(|item| item["symbol_name"] == "OrderList")
    );

    let doctor = run_ok(temp.path(), &["doctor", "--json"]);
    let doctor_json = stdout_json(&doctor);
    assert_eq!(doctor_json["event"], "doctor_completed");
    assert_eq!(doctor_json["ok"], true);

    let compact = run_ok(temp.path(), &["compact", "--json"]);
    let compact_json = stdout_json(&compact);
    assert_eq!(compact_json["event"], "compact_completed");
    assert!(
        compact_json["graph_path"]
            .as_str()
            .is_some_and(|path| { path.ends_with(".gather-step/storage/graph.redb") })
    );
    assert!(compact_json["graph_size_before_bytes"].is_u64());
    assert!(compact_json["graph_size_after_bytes"].is_u64());

    let conventions = run_ok(temp.path(), &["conventions", "--json"]);
    let conventions_json = stdout_json(&conventions);
    assert_eq!(conventions_json["event"], "conventions_completed");
    assert!(
        !conventions_json["conventions"]
            .as_array()
            .expect("conventions array")
            .is_empty()
    );

    let impact = run_ok(temp.path(), &["impact", "listOrders", "--json"]);
    let impact_json = stdout_json(&impact);
    assert_eq!(impact_json["event"], "impact_completed");

    let pack = run_ok(
        temp.path(),
        &[
            "pack",
            "listOrders",
            "--mode",
            "planning",
            "--limit",
            "5",
            "--json",
        ],
    );
    let pack_json = stdout_json(&pack);
    assert_eq!(pack_json["event"], "context_pack_completed");
    assert_eq!(pack_json["data"]["mode"], "planning");
    assert!(
        !pack_json["data"]["items"]
            .as_array()
            .expect("pack items array")
            .is_empty()
    );

    let repo_pack = run_ok(
        temp.path(),
        &[
            "--repo",
            "backend_standard",
            "pack",
            "listOrders",
            "--mode",
            "planning",
            "--json",
        ],
    );
    let repo_pack_json = stdout_json(&repo_pack);
    assert!(
        repo_pack_json["data"]["items"]
            .as_array()
            .expect("repo pack items array")
            .iter()
            .all(|item| item["repo"] == "backend_standard")
    );

    let invalid_pack = run_fail(
        temp.path(),
        &["pack", "listOrders", "--mode", "invalid", "--json"],
    );
    assert!(
        String::from_utf8_lossy(&invalid_pack.stderr).contains("possible values")
            || String::from_utf8_lossy(&invalid_pack.stderr).contains("invalid value")
    );

    let generate = run_ok(temp.path(), &["generate", "claude-md", "--json"]);
    let generate_json = stdout_json(&generate);
    assert_eq!(generate_json["event"], "generate_claude_md_completed");
    assert!(
        temp.path()
            .join(".claude/rules/gather-step-architecture.md")
            .exists()
    );
    assert!(
        temp.path()
            .join(".claude/rules/gather-step-events.md")
            .exists()
    );
    assert!(
        temp.path()
            .join(".claude/rules/gather-step-routes.md")
            .exists()
    );

    let repo_generate = run_ok(
        temp.path(),
        &[
            "generate",
            "claude-md",
            "--repo",
            "backend_standard",
            "--json",
        ],
    );
    let repo_generate_json = stdout_json(&repo_generate);
    assert_eq!(repo_generate_json["event"], "generate_claude_md_completed");
    assert!(
        temp.path()
            .join(".claude/rules/gather-step-repo-backend_standard.md")
            .exists()
    );
    let repo_rule_path = temp
        .path()
        .join(".claude/rules/gather-step-repo-backend_standard.md");
    let repo_rule = fs::read_to_string(&repo_rule_path).expect("repo rule should be readable");
    assert!(
        repo_rule.contains("Path: `apps/backend_standard`"),
        "repo rule should render a workspace-relative repo path:\n{repo_rule}"
    );
    for forbidden_prefix in [
        temp.path().display().to_string(),
        fs::canonicalize(temp.path())
            .expect("canonical temp path")
            .display()
            .to_string(),
    ] {
        assert!(
            !repo_rule.contains(&forbidden_prefix),
            "repo rule must not contain absolute temp path prefix {forbidden_prefix:?}:\n{repo_rule}"
        );
    }
    if let Some(home) = env::var_os("HOME").and_then(|home| home.into_string().ok()) {
        assert!(
            !repo_rule.contains(&home),
            "repo rule must not contain absolute home path prefix {home:?}:\n{repo_rule}"
        );
    }

    let file_like_output = temp.path().join("CLAUDE.md");
    let file_like_output_str = file_like_output.to_str().expect("utf-8 temp path");
    let repo_generate_file = run_fail(
        temp.path(),
        &[
            "generate",
            "claude-md",
            "--repo",
            "backend_standard",
            "--output",
            file_like_output_str,
        ],
    );
    assert!(
        String::from_utf8_lossy(&repo_generate_file.stderr)
            .contains("explicit file output requires a single generated file")
    );

    let metadata = MetadataStoreDb::open(
        temp.path()
            .join(".gather-step")
            .join("storage")
            .join("metadata.sqlite"),
    )
    .expect("metadata store should open");
    metadata
        .replace_file_analytics_for_repo(
            "backend_standard",
            &[FileAnalytics {
                repo: "backend_standard".to_owned(),
                file_path: "src/controller.ts".to_owned(),
                total_commits: 3,
                commits_90d: 3,
                commits_180d: 3,
                commits_365d: 3,
                hotspot_score: 2.0,
                bus_factor: 1,
                top_owner_email: Some("owner@example.com".to_owned()),
                top_owner_pct: 0.8,
                complexity_trend: None,
                last_modified: 1,
                computed_at: 1,
            }],
        )
        .expect("ownership analytics should write");

    let codeowners = run_ok(temp.path(), &["generate", "codeowners", "--json"]);
    let codeowners_json = stdout_json(&codeowners);
    assert_eq!(codeowners_json["event"], "generate_codeowners_completed");
    let rendered = fs::read_to_string(temp.path().join("CODEOWNERS")).expect("CODEOWNERS");
    assert!(rendered.contains("/apps/backend_standard/src/controller.ts owner@example.com"));
}

#[test]
fn generate_claude_md_rejects_unknown_repo_filter() {
    let temp = TempDir::new("cli-commands-missing-repo");
    write_fixture_workspace(temp.path());

    run_ok(temp.path(), &["init"]);
    run_ok(temp.path(), &["index"]);

    let output = run_fail(
        temp.path(),
        &["generate", "claude-md", "--repo", "missing_repo", "--json"],
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("repo `missing_repo` is not present in the workspace registry"));
}

#[test]
fn clean_removes_registry_and_storage_with_yes() {
    let temp = TempDir::new("clean-yes");
    write_fixture_workspace(temp.path());

    run_ok(temp.path(), &["init"]);
    run_ok(temp.path(), &["index"]);

    let output = run_ok(temp.path(), &["clean", "--yes", "--json"]);
    let json = stdout_json(&output);
    assert_eq!(json["event"], "clean_completed");

    assert!(!temp.path().join(".gather-step/registry.json").exists());
    assert!(!temp.path().join(".gather-step/storage").exists());
}

#[test]
fn clean_requires_explicit_confirmation_in_json_mode() {
    let temp = TempDir::new("clean-json-confirm");
    write_fixture_workspace(temp.path());

    run_ok(temp.path(), &["init"]);
    run_ok(temp.path(), &["index"]);

    let output = run_fail(temp.path(), &["clean", "--json"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("pass `--yes` to confirm"));

    assert!(temp.path().join(".gather-step/registry.json").exists());
    assert!(temp.path().join(".gather-step/storage").exists());
}

#[test]
fn stable_error_when_config_is_missing() {
    let temp = TempDir::new("missing-config");

    let output = run_fail(temp.path(), &["index", "--json"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Config not found:"));
    assert!(stderr.contains("Next step: run `gather-step init`"));
}

#[test]
fn stable_error_when_config_yaml_is_malformed() {
    let temp = TempDir::new("malformed-config");
    fs::write(temp.path().join("gather-step.config.yaml"), "repos: [").expect("config write");

    let output = run_fail(temp.path(), &["index", "--json"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Config YAML is malformed:"));
    assert!(stderr.contains("Next step: fix the YAML syntax and rerun"));
}

#[test]
fn stable_error_when_configured_repo_path_is_missing() {
    let temp = TempDir::new("missing-repo-path");
    fs::write(
        temp.path().join("gather-step.config.yaml"),
        r"
repos:
  - name: backend_standard
    path: apps/backend_standard
",
    )
    .expect("config write");

    let output = run_fail(temp.path(), &["index", "--json"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Configured repo path does not exist:"));
    assert!(stderr.contains("repo `backend_standard` path does not exist"));
}

#[test]
fn release_gate_rejects_non_git_workspace_with_stable_error() {
    let temp = TempDir::new("release-gate-non-git");

    let output = run_fail(temp.path(), &["index", "--release-gate", "--json"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Workspace is not a git repository"));
    assert!(stderr.contains("omit `--release-gate`"));
}

#[test]
fn corrupt_graph_index_reports_auto_recover_and_auto_recover_rebuilds() {
    let temp = TempDir::new("corrupt-graph");
    write_fixture_workspace(temp.path());
    run_ok(temp.path(), &["init"]);

    let storage_root = temp.path().join(".gather-step/storage");
    fs::create_dir_all(&storage_root).expect("storage dir");
    fs::write(storage_root.join("graph.redb"), b"not a redb database").expect("corrupt graph");

    let output = run_fail(temp.path(), &["index", "--json"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Your index is corrupt or incomplete"));
    assert!(stderr.contains("gather-step index --auto-recover"));

    let recovered = run_ok(temp.path(), &["index", "--auto-recover", "--json"]);
    let recovered_json = stdout_json(&recovered);
    assert_eq!(recovered_json["event"], "index_completed");
}

#[test]
fn unsupported_metadata_schema_reports_stable_rebuild_message() {
    let temp = TempDir::new("unsupported-metadata-schema");
    write_fixture_workspace(temp.path());
    run_ok(temp.path(), &["init"]);

    let storage_root = temp.path().join(".gather-step/storage");
    fs::create_dir_all(&storage_root).expect("storage dir");
    let conn = Connection::open(storage_root.join("metadata.sqlite")).expect("metadata sqlite");
    conn.pragma_update(None, "user_version", 99)
        .expect("stamp unsupported schema");
    drop(conn);

    let output = run_fail(temp.path(), &["index", "--json"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unsupported schema"));
    assert!(stderr.contains("gather-step index --auto-recover"));

    let recovered = run_ok(temp.path(), &["index", "--auto-recover", "--json"]);
    let recovered_json = stdout_json(&recovered);
    assert_eq!(recovered_json["event"], "index_completed");
}

#[test]
fn concurrent_graph_open_reports_stable_process_error() {
    let temp = TempDir::new("concurrent-open");
    write_fixture_workspace(temp.path());
    run_ok(temp.path(), &["init"]);
    run_ok(temp.path(), &["index"]);

    let _held_graph = GraphStoreDb::open(temp.path().join(".gather-step/storage/graph.redb"))
        .expect("graph should open and hold the redb lock");
    let output = run_fail(temp.path(), &["status", "--json"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Another gather-step process is using this workspace"));
    assert!(stderr.contains("Stop `gather-step watch`"));
}

#[test]
#[cfg(unix)]
fn generated_state_permission_denied_reports_stable_error() {
    use std::os::unix::fs::PermissionsExt;

    let temp = TempDir::new("generated-state-permission");
    write_fixture_workspace(temp.path());
    run_ok(temp.path(), &["init"]);

    let generated_root = temp.path().join(".gather-step");
    fs::create_dir_all(&generated_root).expect("generated dir");
    fs::set_permissions(&generated_root, fs::Permissions::from_mode(0o500))
        .expect("remove write permission");

    let output = run_fail(temp.path(), &["index", "--json"]);
    let _ = fs::set_permissions(&generated_root, fs::Permissions::from_mode(0o700));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Cannot write `.gather-step` generated state"));
    assert!(stderr.contains("fix permissions on `.gather-step`"));
}

/// Assert that `stderr` bytes contain no ANSI escape sequences and no indicatif
/// bar-rendered output. ANSI detection (`\x1b[`) is the primary signal; the
/// multi-byte patterns match the active workspace bar's `progress_chars("=> ")`
/// template so a plain-text progress leak is caught even if ANSI happens to be
/// stripped by an intermediate layer.
fn assert_stderr_has_no_progress_output(stderr: &[u8]) {
    assert!(
        !stderr.windows(2).any(|w| w == b"\x1b["),
        "stderr must not contain ANSI escape sequences (\\x1b[); stderr:\n{}",
        String::from_utf8_lossy(stderr)
    );
    for glyph in [&b"[==>"[..], &b"[==="[..], &b"==> "[..]] {
        assert!(
            !stderr.windows(glyph.len()).any(|w| w == glyph),
            "stderr must not contain indicatif bar glyph {:?}; stderr:\n{}",
            String::from_utf8_lossy(glyph),
            String::from_utf8_lossy(stderr)
        );
    }
}

#[test]
fn index_stderr_is_clean_on_non_tty_and_when_ci_env_set() {
    let temp = TempDir::new("stderr-clean");
    write_fixture_workspace(temp.path());

    let init_out = gather_step()
        .arg("--workspace")
        .arg(temp.path())
        .arg("init")
        .output()
        .expect("init should run");
    assert!(
        init_out.status.success(),
        "init failed; stderr:\n{}",
        String::from_utf8_lossy(&init_out.stderr)
    );

    // First invocation: non-TTY path (cargo test pipes stderr).
    let out_non_tty = gather_step()
        .arg("--workspace")
        .arg(temp.path())
        .arg("index")
        .output()
        .expect("index should run");
    assert!(
        out_non_tty.status.success(),
        "index (non-TTY) failed — an aborted run would satisfy the stderr-purity check vacuously; stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out_non_tty.stdout),
        String::from_utf8_lossy(&out_non_tty.stderr)
    );
    assert_stderr_has_no_progress_output(&out_non_tty.stderr);

    // Second invocation: CI=true must suppress bars even if stderr were a TTY.
    let out_ci = gather_step()
        .arg("--workspace")
        .arg(temp.path())
        .arg("index")
        .env("CI", "true")
        .output()
        .expect("index should run");
    assert!(
        out_ci.status.success(),
        "index (CI=true) failed — an aborted run would satisfy the stderr-purity check vacuously; stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out_ci.stdout),
        String::from_utf8_lossy(&out_ci.stderr)
    );
    assert_stderr_has_no_progress_output(&out_ci.stderr);
}
