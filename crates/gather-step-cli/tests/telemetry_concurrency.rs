use std::{
    fs,
    process::{Child, Command, Stdio},
};

use gather_step_storage::TelemetryStore;

const PROCESS_COUNT: usize = 32;

#[test]
fn concurrent_cli_runs_are_all_finalized_in_shared_telemetry() {
    let fixture = tempfile::tempdir().expect("create fixture root");
    let telemetry_root = fixture.path().join("telemetry");
    let valid_workspace = fixture.path().join("valid-workspace");
    let invalid_workspace = fixture.path().join("invalid-workspace");
    fs::create_dir_all(&valid_workspace).expect("create valid workspace");
    fs::create_dir_all(&invalid_workspace).expect("create invalid workspace");
    fs::write(
        valid_workspace.join("gather-step.config.yaml"),
        "repos: []\n",
    )
    .expect("write valid config");

    let mut children = Vec::<(Child, bool)>::with_capacity(PROCESS_COUNT);
    for index in 0..PROCESS_COUNT {
        let should_succeed = index % 2 == 0;
        let workspace = if should_succeed {
            &valid_workspace
        } else {
            &invalid_workspace
        };
        let child = Command::new(env!("CARGO_BIN_EXE_gather-step"))
            .arg("--workspace")
            .arg(workspace)
            .args(["doctor", "--config-only"])
            .env("GATHER_STEP_TELEMETRY", "on")
            .env("GATHER_STEP_TELEMETRY_ROOT", &telemetry_root)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn gather-step child");
        children.push((child, should_succeed));
    }

    for (mut child, should_succeed) in children {
        let status = child.wait().expect("wait for gather-step child");
        assert_eq!(
            status.success(),
            should_succeed,
            "child exit status should match fixture validity"
        );
    }

    let store = TelemetryStore::open(&telemetry_root).expect("open shared telemetry");
    let runs = store
        .list_runs_all_workspaces(PROCESS_COUNT, None, None, false, None, None, None, None)
        .expect("read concurrent telemetry rows");

    assert_eq!(
        runs.len(),
        PROCESS_COUNT,
        "every child must persist one run"
    );
    assert!(
        runs.iter().all(|run| run.exit_status != "running"),
        "every child run must be finalized"
    );
    assert_eq!(
        runs.iter()
            .filter(|run| run.exit_status == "success")
            .count(),
        PROCESS_COUNT / 2
    );
    assert_eq!(
        runs.iter().filter(|run| run.exit_status == "error").count(),
        PROCESS_COUNT / 2
    );
}
