use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
};

use gather_step_storage::{TelemetryErrorEvent, TelemetryRunFinish, TelemetryStore};

struct LogFixture {
    _root: tempfile::TempDir,
    telemetry_root: PathBuf,
    workspace: PathBuf,
}

impl LogFixture {
    fn new() -> Self {
        let root = tempfile::tempdir().expect("create fixture root");
        let telemetry_root = root.path().join("telemetry");
        let workspace = root.path().join("workspace");
        fs::create_dir_all(&workspace).expect("create workspace");
        let workspace = fs::canonicalize(&workspace).expect("canonicalize workspace");
        Self {
            _root: root,
            telemetry_root,
            workspace,
        }
    }

    fn store(&self) -> TelemetryStore {
        TelemetryStore::open(&self.telemetry_root).expect("open telemetry store")
    }

    fn seed_run_with_event(&self, workspace: &Path, category: &str, message: &str) {
        let store = self.store();
        let run = store
            .begin_run(
                "index",
                workspace,
                "9.9.9",
                "release",
                &serde_json::json!({ "telemetry": 3 }),
            )
            .expect("begin seeded run");
        store
            .finish_run(
                &run,
                &TelemetryRunFinish {
                    exit_status: "error".to_owned(),
                    error_count: 1,
                    error: Some(TelemetryErrorEvent {
                        level: "ERROR".to_owned(),
                        category: category.to_owned(),
                        message: message.to_owned(),
                        context_json: None,
                    }),
                    ..TelemetryRunFinish::default()
                },
            )
            .expect("finish seeded run");
    }

    fn seed_successful_run(&self) {
        let store = self.store();
        let run = store
            .begin_run(
                "status",
                &self.workspace,
                "9.9.9",
                "release",
                &serde_json::json!({ "telemetry": 3 }),
            )
            .expect("begin seeded run");
        store
            .finish_run(
                &run,
                &TelemetryRunFinish {
                    exit_status: "success".to_owned(),
                    ..TelemetryRunFinish::default()
                },
            )
            .expect("finish seeded run");
    }

    fn run_log(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_gather-step"))
            .arg("--workspace")
            .arg(&self.workspace)
            .args(args)
            .env("GATHER_STEP_TELEMETRY_ROOT", &self.telemetry_root)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .expect("run gather-step log")
    }
}

#[test]
fn log_events_human_mode_prints_retained_events() {
    let fixture = LogFixture::new();
    let workspace = fixture.workspace.clone();
    fixture.seed_run_with_event(&workspace, "probe_category", "probe telemetry message");

    let output = fixture.run_log(&["log", "--events"]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "log --events failed; stdout={stdout}; stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains("probe_category"),
        "human output must list the event category; stdout={stdout}"
    );
    assert!(
        stdout.contains("probe telemetry message"),
        "human output must list the event message; stdout={stdout}"
    );
}

#[test]
fn log_events_human_mode_reports_when_no_events_exist() {
    let fixture = LogFixture::new();
    fixture.store();

    let output = fixture.run_log(&["log", "--events"]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success());
    assert!(
        stdout.contains("No telemetry events found."),
        "human output must not be silent; stdout={stdout}"
    );
}

#[test]
fn log_summary_with_before_is_not_capped_at_twenty_rows() {
    let fixture = LogFixture::new();
    for _ in 0..25 {
        fixture.seed_successful_run();
    }

    let output = fixture.run_log(&["--json", "log", "--summary", "--before", "0d"]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "summary failed; stdout={stdout}; stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: serde_json::Value = serde_json::from_str(stdout.trim()).expect("json output");
    assert_eq!(payload["event"], "log_summary");
    assert_eq!(payload["total_runs"], 25);
}

#[test]
fn log_events_with_clear_before_actually_clears() {
    let fixture = LogFixture::new();
    let workspace = fixture.workspace.clone();
    fixture.seed_run_with_event(&workspace, "probe_category", "probe telemetry message");

    let output = fixture.run_log(&["--json", "log", "--events", "--clear-before", "0d"]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "clear-before in events mode failed; stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: serde_json::Value = serde_json::from_str(stdout.trim()).expect("json output");
    assert_eq!(payload["event"], "log_events");
    assert!(
        payload["cleared_rows"].as_u64().unwrap_or(0) >= 1,
        "clear-before must report deleted rows; stdout={stdout}"
    );
    assert_eq!(payload["events"].as_array().map(Vec::len), Some(0));

    let store = fixture.store();
    let events = store
        .list_events(&workspace, None, 10)
        .expect("list events after clear");
    assert!(events.is_empty(), "events must be gone after clear-before");
}

#[test]
fn all_workspaces_clear_without_yes_is_refused_and_deletes_nothing() {
    let fixture = LogFixture::new();
    let workspace = fixture.workspace.clone();
    fixture.seed_run_with_event(&workspace, "probe_category", "probe telemetry message");
    fixture.seed_run_with_event(Path::new("/workspace/other"), "other_category", "other");

    let output = fixture.run_log(&["log", "--clear-before", "0d", "--all-workspaces"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "all-workspaces clear without --yes must fail; stderr={stderr}"
    );
    assert!(
        stderr.contains("requires --yes"),
        "refusal must mention --yes; stderr={stderr}"
    );

    let store = fixture.store();
    let runs = store
        .list_runs_all_workspaces(10, None, None, false, None, None, None, None)
        .expect("list runs after refused clear");
    assert_eq!(runs.len(), 2, "refused clear must delete nothing");
}
