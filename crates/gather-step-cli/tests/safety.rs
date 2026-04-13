/// Path-safety integration tests.
///
/// These tests exercise the `path_safety` module, the `init` symlink guard,
/// and the `reindex` cleanup guard introduced in this patch series.  All tests
/// are Unix-only because symlink creation via `std::os::unix::fs::symlink` is
/// a Unix API.
use std::path::PathBuf;

// ── path_safety::canonicalize_inside_workspace ───────────────────────────────

#[cfg(unix)]
#[test]
fn canonicalize_inside_workspace_accepts_path_inside_root() {
    use std::fs;

    let tmp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(tmp.path()).unwrap();
    let inside = root.join("data.txt");
    fs::write(&inside, "x").unwrap();

    let resolved = gather_step::path_safety::canonicalize_inside_workspace(&inside, &root)
        .expect("path inside workspace must be accepted");
    assert!(resolved.starts_with(&root));
}

#[cfg(unix)]
#[test]
fn canonicalize_inside_workspace_rejects_symlink_that_escapes_root() {
    use std::fs;
    use std::os::unix::fs::symlink;

    let tmp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(tmp.path()).unwrap();
    let escape_target: PathBuf = "/etc".into();
    let link = root.join("escape");
    symlink(&escape_target, &link).expect("symlink creation");

    let err = gather_step::path_safety::canonicalize_inside_workspace(&link.join("passwd"), &root)
        .expect_err("escape via symlink must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("outside workspace") || msg.contains("escape") || msg.contains("PathEscape"),
        "error should describe the escape, got: {msg}"
    );
}

#[cfg(unix)]
#[test]
fn canonicalize_inside_workspace_rejects_parent_traversal() {
    use std::fs;

    let tmp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(tmp.path()).unwrap();
    // Build a path that walks outside via "..".
    // After canonicalization this resolves to /tmp (or similar) which is
    // outside the workspace root, so the function must reject it.
    let outside = root.join("..").join("should-not-resolve");
    gather_step::path_safety::canonicalize_inside_workspace(&outside, &root)
        .expect_err("parent traversal must be rejected or normalized outside root");
}

#[cfg(unix)]
#[test]
fn canonicalize_inside_workspace_accepts_non_existing_path_inside_root() {
    use std::fs;

    let tmp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(tmp.path()).unwrap();
    // This path does not exist — the function should still accept it because
    // the longest-existing prefix (root) is inside the workspace.
    let future = root.join("not-created-yet").join("file.txt");
    let resolved = gather_step::path_safety::canonicalize_inside_workspace(&future, &root)
        .expect("non-existing path inside workspace must be accepted");
    assert!(resolved.starts_with(&root));
}

// ── init: .git symlink escape guard ──────────────────────────────────────────

#[cfg(unix)]
#[test]
fn init_refuses_symlinked_dot_git_pointing_outside_workspace() {
    use std::fs;
    use std::os::unix::fs::symlink;

    // Build a fake workspace where the .git entry is a symlink pointing outside.
    let tmp = tempfile::tempdir().unwrap();
    let workspace = fs::canonicalize(tmp.path()).unwrap();
    let repo_dir = workspace.join("my-repo");
    fs::create_dir_all(&repo_dir).unwrap();
    // Create a directory outside the workspace that will be the symlink target.
    let outside_dir = workspace
        .parent()
        .unwrap()
        .join(format!("gather-step-test-outside-{}", std::process::id()));
    fs::create_dir_all(&outside_dir).unwrap();
    // Create a fake .git inside the outside directory so `.git` as a symlink
    // actually "exists" (exists() follows symlinks).
    fs::create_dir_all(outside_dir.join(".git")).unwrap();
    // The .git entry inside the repo dir is itself the symlink pointing outside.
    let git_link = repo_dir.join(".git");
    symlink(&outside_dir, &git_link).expect("create .git symlink");

    // Call the internal discovery function exposed for testing.
    let err = gather_step::commands::init::discover_git_repos_for_test(&workspace)
        .expect_err("discovery with escaping .git symlink must fail");
    // Downcast to the typed variant — the error must be
    // PathSafetyError::GitSymlinkEscape, not a plain anyhow string.
    let typed = err
        .downcast_ref::<gather_step::path_safety::PathSafetyError>()
        .unwrap_or_else(|| {
            panic!("expected PathSafetyError, got: {err:?}");
        });
    assert!(
        matches!(
            typed,
            gather_step::path_safety::PathSafetyError::GitSymlinkEscape { .. }
        ),
        "expected GitSymlinkEscape variant, got: {typed:?}"
    );

    // Clean up the directory outside the workspace.
    let _ = fs::remove_dir_all(&outside_dir);
}

// ── canonicalize_inside_workspace: parent-dir escape via non-existent suffix ──

#[cfg(unix)]
#[test]
fn canonicalize_inside_workspace_rejects_parent_dir_escape_via_nonexistent_suffix() {
    use std::fs;
    let tmp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(tmp.path()).unwrap();
    // Create an existing subdir so the partial-canonicalization branch engages.
    let inside = root.join("existing");
    fs::create_dir_all(&inside).unwrap();

    // Attempt escape: existing/missing/../../../escape should resolve to
    // something OUTSIDE the workspace after the `..` sequence is applied.
    // The pre-fix implementation rejoined lexically and passed the
    // starts_with check; the fix must reject.
    let attack = inside
        .join("missing")
        .join("..")
        .join("..")
        .join("..")
        .join("escape");

    let err = gather_step::path_safety::canonicalize_inside_workspace(&attack, &root)
        .expect_err("parent-dir escape via non-existent suffix must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("escape") || msg.contains("PathEscape") || msg.contains("outside"),
        "error must describe the escape; got {msg:?}"
    );
}

#[cfg(unix)]
#[test]
fn canonicalize_inside_workspace_accepts_normal_parent_dir_inside_root() {
    use std::fs;
    let tmp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(tmp.path()).unwrap();
    fs::create_dir_all(root.join("a").join("b")).unwrap();
    // a/b/../c is inside root; it must be accepted and normalized to root/a/c.
    let target = root.join("a").join("b").join("..").join("c");
    let resolved = gather_step::path_safety::canonicalize_inside_workspace(&target, &root)
        .expect("normal parent-dir inside root must be accepted");
    assert!(resolved.starts_with(&root));
    assert!(resolved.ends_with("a/c"));
}

// ── reindex: validate_and_clean_generated_paths ──────────────────────────────

#[cfg(unix)]
#[test]
fn reindex_refuses_to_clean_paths_outside_workspace_root() {
    use std::fs;
    use std::os::unix::fs::symlink;

    let tmp = tempfile::tempdir().unwrap();
    let workspace = fs::canonicalize(tmp.path()).unwrap();

    // Create a directory outside the workspace that we want to protect.
    let outside_dir = workspace
        .parent()
        .unwrap()
        .join(format!("gather-step-test-victim-{}", std::process::id()));
    fs::create_dir_all(&outside_dir).unwrap();
    fs::write(outside_dir.join("secret.txt"), "do-not-delete").unwrap();

    // Create a symlink inside the workspace that points at the outside dir.
    let gs_dir = workspace.join(".gather-step");
    fs::create_dir_all(&gs_dir).unwrap();
    let evil_storage = gs_dir.join("storage");
    symlink(&outside_dir, &evil_storage).expect("symlink creation");

    let registry = gs_dir.join("registry.json");
    fs::write(&registry, "{}").unwrap();

    // The validate_and_clean helper should reject the symlinked path.
    let err = gather_step::commands::clean::validate_and_clean_generated_paths(
        &[evil_storage.clone(), registry.clone()],
        &workspace,
    )
    .expect_err("cleanup of symlinked-outside path must be rejected");

    let msg = format!("{err}");
    assert!(
        msg.contains("escape")
            || msg.contains("outside")
            || msg.contains("PathEscape")
            || msg.contains("does not resolve inside"),
        "error should describe the escape, got: {msg}"
    );

    // The outside directory must NOT have been removed.
    assert!(
        outside_dir.exists(),
        "outside directory must not have been deleted"
    );
    assert!(
        outside_dir.join("secret.txt").exists(),
        "secret file inside outside directory must not have been deleted"
    );

    // Clean up.
    let _ = fs::remove_dir_all(&outside_dir);
}
