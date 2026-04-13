//! Integration tests for `GitHistoryIndexer::sync` and `walk` against real
//! on-disk git repositories. Each test builds a deterministic fixture by
//! shelling out to the `git` CLI (with author identity, dates, and commit
//! messages pinned via environment variables) and then drives the indexer
//! against the result.
//!
//! We use the system `git` rather than constructing commits programmatically
//! through `gix` because:
//!   1. The fixtures here exercise git's *behaviour* (default rename
//!      detection, `Merge pull request` subjects, force-push semantics) —
//!      shelling to git is the most faithful reproduction of that behaviour.
//!   2. The committed fixture is human-readable in test output: every
//!      commit corresponds to a `git commit -m "…"` line, no opaque object
//!      construction.
//!
//! `git` is required on `PATH`. CI images that run `cargo nextest` already
//! have it; local developer machines invariably do too.

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{self, Command},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use gather_step_git::{
    CommitFileChangeKind, GitHistoryIndexer, GitIndexerOptions, GitRepoSource, HistorySyncOutcome,
};
use gather_step_storage::{CoChangePairRecord, FileAnalytics, MetadataStore, MetadataStoreDb};
use pretty_assertions::assert_eq;

static TEST_ID: AtomicU64 = AtomicU64::new(0);

/// On-disk scratch space. Drops the directory tree on test exit so a panic
/// in the middle of building the fixture does not leak state into the next
/// run.
struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(name: &str) -> Self {
        let unique = TEST_ID.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be monotonic enough for tests")
            .as_nanos();
        let path = env::temp_dir().join(format!(
            "gather-step-git-{name}-{}-{nanos}-{unique}",
            process::id()
        ));
        fs::create_dir_all(&path).expect("test scratch dir should be creatable");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

/// Helper that shells out to `git`. Panics on non-zero exit so each fixture
/// step fails the test loudly rather than silently producing an inconsistent
/// repository state.
///
/// Author and committer identity + dates are pinned via env vars so commit
/// SHAs are reproducible enough to compare across runs **on the same git
/// version**. (We do not assert on the SHAs themselves — only counts and
/// metadata — because git's exact hashing also depends on tree contents,
/// which is fine but not worth pinning here.)
fn git(cwd: &Path, args: &[&str], extra_env: &[(&str, &str)]) {
    let mut command = Command::new("git");
    command.current_dir(cwd).args(args);
    // Test isolation: prevent the user's `~/.gitconfig` (commit signing,
    // alias overrides, default branch overrides, …) from changing the
    // fixture's behaviour.
    command
        .env_remove("HOME")
        .env_remove("XDG_CONFIG_HOME")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .env("GIT_AUTHOR_NAME", "Alice Tester")
        .env("GIT_AUTHOR_EMAIL", "alice@example.com")
        .env("GIT_COMMITTER_NAME", "Alice Tester")
        .env("GIT_COMMITTER_EMAIL", "alice@example.com")
        .env("GIT_AUTHOR_DATE", "2026-01-15T10:00:00 +0000")
        .env("GIT_COMMITTER_DATE", "2026-01-15T10:00:00 +0000");
    for (key, value) in extra_env {
        command.env(key, value);
    }
    let output = command
        .output()
        .unwrap_or_else(|err| panic!("failed to spawn git {args:?}: {err}"));
    assert!(
        output.status.success(),
        "git {args:?} failed with status {}: stdout={} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

/// Convenience wrapper for `git commit -m`. Sets author/committer dates
/// monotonically based on the supplied `seconds_after_epoch` so commits land
/// in the expected newest-first walk order without depending on real wall
/// clock progression.
fn commit_at(cwd: &Path, message: &str, seconds_after_epoch: i64, author_email: Option<&str>) {
    let date = format!("@{seconds_after_epoch} +0000");
    let mut env: Vec<(&str, &str)> = vec![
        ("GIT_AUTHOR_DATE", date.as_str()),
        ("GIT_COMMITTER_DATE", date.as_str()),
    ];
    if let Some(email) = author_email {
        env.push(("GIT_AUTHOR_EMAIL", email));
        env.push(("GIT_COMMITTER_EMAIL", email));
    }
    git(cwd, &["commit", "-m", message, "--allow-empty"], &env);
}

fn write_file(cwd: &Path, relative: &str, contents: &str) {
    let path = cwd.join(relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("test fixture parent dir should be creatable");
    }
    fs::write(&path, contents).expect("test fixture file should be writable");
}

fn init_repo(dir: &Path) {
    git(dir, &["init", "--initial-branch=main"], &[]);
    // Disable GPG signing in case the unset config still surfaces a default.
    git(dir, &["config", "commit.gpgsign", "false"], &[]);
    git(dir, &["config", "tag.gpgsign", "false"], &[]);
}

fn add_all(dir: &Path) {
    git(dir, &["add", "-A"], &[]);
}

/// Builds a four-commit fixture exercising:
///  - initial add
///  - modification
///  - rename (default git rename tracking with 50% similarity)
///  - decision-signal commit message
///  - merge-PR subject
fn build_fixture_repo(dir: &Path) {
    init_repo(dir);

    // Commit 1: initial add (an "initial commit" has no parent, so deltas
    // are intentionally empty per the indexer's documented contract).
    write_file(dir, "src/lib.rs", "fn alpha() {}\n");
    add_all(dir);
    commit_at(dir, "feat: add alpha", 1_736_900_000, None);

    // Commit 2: modify alpha. This is the first commit with a parent, so
    // the first one that should produce a Modified delta.
    write_file(dir, "src/lib.rs", "fn alpha() { /* updated */ }\n");
    add_all(dir);
    commit_at(dir, "fix: tidy alpha", 1_736_900_100, None);

    // Commit 3: rename src/lib.rs -> src/renamed.rs, plus a decision signal
    // in the message. Default git rename tracking should pick this up; we
    // also keep some shared content so similarity is well above the 50%
    // threshold.
    fs::rename(dir.join("src/lib.rs"), dir.join("src/renamed.rs"))
        .expect("test fixture rename should succeed");
    add_all(dir);
    commit_at(
        dir,
        "refactor: relocate alpha because the layout changed",
        1_736_900_200,
        Some("bob@example.com"),
    );

    // Commit 4: merge-PR style subject for PR-number extraction.
    write_file(dir, "src/renamed.rs", "fn alpha() { /* finalised */ }\n");
    add_all(dir);
    commit_at(
        dir,
        "Merge pull request #42 from feature/alpha-finalise",
        1_736_900_300,
        None,
    );
}

#[test]
fn walk_returns_facts_in_newest_first_order_with_correct_metadata() {
    let scratch = TestDir::new("walk-newest-first");
    build_fixture_repo(scratch.path());

    let indexer = GitHistoryIndexer::new(
        GitRepoSource::from_path(scratch.path().to_path_buf()),
        "service-a".to_owned(),
    );

    let facts = indexer
        .walk(None)
        .expect("walk should succeed against the fixture")
        .expect("walk(None) cannot return Ok(None)");

    assert_eq!(facts.len(), 4, "all four fixture commits should be walked");

    // Newest first: PR merge → rename → fix → feat add.
    let messages: Vec<&str> = facts
        .iter()
        .map(|fact| fact.message.lines().next().unwrap_or(""))
        .collect();
    assert_eq!(
        messages,
        vec![
            "Merge pull request #42 from feature/alpha-finalise",
            "refactor: relocate alpha because the layout changed",
            "fix: tidy alpha",
            "feat: add alpha",
        ]
    );

    // PR number is extracted from the merge subject only.
    assert_eq!(facts[0].pr_number, Some(42));
    assert_eq!(facts[1].pr_number, None);

    // Decision-signal heuristic fires only on the rename commit's message.
    let decision_flags: Vec<bool> = facts.iter().map(|fact| fact.has_decision_signal).collect();
    assert_eq!(decision_flags, vec![false, true, false, false]);

    // Classification falls back to None for the merge subject (it does not
    // start with a recognised conventional-commit type).
    assert_eq!(facts[0].classification.as_deref(), None);
    assert_eq!(facts[1].classification.as_deref(), Some("refactor"));
    assert_eq!(facts[2].classification.as_deref(), Some("fix"));
    assert_eq!(facts[3].classification.as_deref(), Some("feat"));

    // Per-file deltas:
    //   - The initial commit has no parent, so deltas are empty.
    //   - The fix commit modifies one file.
    //   - The rename commit emits one Renamed delta with old_path set.
    //   - The merge commit (single-parent merge in this fixture) modifies
    //     one file.
    assert_eq!(
        facts[3].file_deltas,
        Vec::new(),
        "initial commit should have no deltas"
    );

    let fix_deltas = &facts[2].file_deltas;
    assert_eq!(fix_deltas.len(), 1);
    assert_eq!(fix_deltas[0].file_path, "src/lib.rs");
    assert_eq!(fix_deltas[0].change_kind, CommitFileChangeKind::Modified);
    assert_eq!(fix_deltas[0].old_path, None);

    let rename_deltas = &facts[1].file_deltas;
    assert_eq!(rename_deltas.len(), 1);
    let rename = &rename_deltas[0];
    assert_eq!(rename.change_kind, CommitFileChangeKind::Renamed);
    assert_eq!(rename.file_path, "src/renamed.rs");
    assert_eq!(rename.old_path.as_deref(), Some("src/lib.rs"));

    // Author attribution: the rename commit overrode GIT_AUTHOR_EMAIL to
    // bob; everything else stays on alice. Validates per-commit signature
    // decoding rather than reading from a stale repo-level default.
    assert_eq!(facts[0].author_email, "alice@example.com");
    assert_eq!(facts[1].author_email, "bob@example.com");
    assert_eq!(facts[2].author_email, "alice@example.com");
    assert_eq!(facts[3].author_email, "alice@example.com");
}

struct TestDb {
    path: PathBuf,
}

impl TestDb {
    fn new(name: &str) -> Self {
        let unique = TEST_ID.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be monotonic enough for tests")
            .as_nanos();
        let path = env::temp_dir().join(format!(
            "gather-step-git-db-{name}-{}-{nanos}-{unique}.sqlite",
            process::id()
        ));
        Self { path }
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let candidate = PathBuf::from(format!("{}{}", self.path.display(), suffix));
            let _ = fs::remove_file(candidate);
        }
    }
}

#[test]
fn sync_first_run_is_full_rebuild_then_subsequent_runs_are_incremental_or_no_change() {
    let scratch = TestDir::new("sync-incremental");
    build_fixture_repo(scratch.path());
    let db = TestDb::new("sync-incremental");
    let store = MetadataStoreDb::open(&db.path).expect("metadata store should open");
    let indexer = GitHistoryIndexer::new(
        GitRepoSource::from_path(scratch.path().to_path_buf()),
        "service-a".to_owned(),
    );

    // First sync: no anchor recorded -> FullRebuild.
    let outcome = indexer
        .sync(&store, 1_700_000_000)
        .expect("first sync should succeed");
    let HistorySyncOutcome::FullRebuild {
        repo,
        commits_added,
        deltas_added,
        new_head_sha,
    } = outcome
    else {
        panic!("first sync should be FullRebuild, got {outcome:?}");
    };
    assert_eq!(repo, "service-a");
    assert_eq!(commits_added, 4);
    // Two non-initial commits produced one delta each, plus one rename delta;
    // the merge commit modified one file. 1 + 1 + 1 = 3.
    assert_eq!(deltas_added, 3);
    assert_eq!(
        new_head_sha.len(),
        40,
        "HEAD SHA should be a full 40-char hex"
    );

    // Verify the persisted state matches the walked facts.
    let stored = store
        .get_commits_by_repo("service-a", 0, i64::MAX)
        .expect("commit query should succeed");
    assert_eq!(stored.len(), 4);
    assert_eq!(
        stored[0].files_changed, 0,
        "initial commit still persists zero deltas"
    );
    assert_eq!(stored[1].files_changed, 1);
    assert_eq!(stored[1].insertions, 1);
    assert_eq!(stored[1].deletions, 1);
    assert_eq!(stored[2].files_changed, 1);
    assert_eq!(stored[2].insertions, 0);
    assert_eq!(stored[2].deletions, 0);
    assert_eq!(stored[3].files_changed, 1);
    assert_eq!(stored[3].insertions, 1);
    assert_eq!(stored[3].deletions, 1);
    let stored_anchor = store
        .get_last_commit_sha("service-a")
        .expect("sync state read should succeed");
    assert_eq!(stored_anchor.as_deref(), Some(new_head_sha.as_str()));

    // Second sync without changes: HEAD == anchor -> NoChange (no extra writes).
    let outcome = indexer
        .sync(&store, 1_700_000_100)
        .expect("second sync should succeed");
    assert!(
        matches!(outcome, HistorySyncOutcome::NoChange { .. }),
        "unchanged HEAD should be NoChange, got {outcome:?}"
    );
    let still = store
        .get_commits_by_repo("service-a", 0, i64::MAX)
        .expect("commit re-query should succeed");
    assert_eq!(still.len(), 4, "NoChange branch must not insert duplicates");

    // Add a fifth commit and sync again -> Incremental with exactly one new row.
    write_file(
        scratch.path(),
        "src/renamed.rs",
        "fn alpha() { /* further */ }\n",
    );
    add_all(scratch.path());
    commit_at(scratch.path(), "perf: cache alpha", 1_736_900_400, None);

    let outcome = indexer
        .sync(&store, 1_700_000_200)
        .expect("third sync should succeed");
    let HistorySyncOutcome::Incremental {
        commits_added,
        deltas_added,
        ..
    } = outcome
    else {
        panic!("post-add sync should be Incremental, got {outcome:?}");
    };
    assert_eq!(commits_added, 1);
    assert_eq!(deltas_added, 1);
    let after_inc = store
        .get_commits_by_repo("service-a", 0, i64::MAX)
        .expect("commit query should succeed");
    assert_eq!(after_inc.len(), 5);
}

#[test]
fn sync_does_not_treat_a_reachable_anchor_beyond_commit_depth_as_a_rewrite() {
    let scratch = TestDir::new("sync-depth-anchor");
    build_fixture_repo(scratch.path());
    let db = TestDb::new("sync-depth-anchor");
    let store = MetadataStoreDb::open(&db.path).expect("metadata store should open");
    let bootstrap_indexer = GitHistoryIndexer::new(
        GitRepoSource::from_path(scratch.path().to_path_buf()),
        "service-a".to_owned(),
    )
    .with_options(GitIndexerOptions {
        commit_depth: None,
        ..GitIndexerOptions::default()
    });

    let HistorySyncOutcome::FullRebuild { .. } = bootstrap_indexer
        .sync(&store, 1_700_000_000)
        .expect("bootstrap sync should succeed")
    else {
        panic!("bootstrap sync should be FullRebuild");
    };

    write_file(
        scratch.path(),
        "src/renamed.rs",
        "fn alpha() { /* v2 */ }\n",
    );
    add_all(scratch.path());
    commit_at(scratch.path(), "perf: cache alpha", 1_736_900_400, None);

    write_file(
        scratch.path(),
        "src/renamed.rs",
        "fn alpha() { /* v3 */ }\n",
    );
    add_all(scratch.path());
    commit_at(scratch.path(), "fix: patch alpha", 1_736_900_500, None);

    let depth_limited_indexer = GitHistoryIndexer::new(
        GitRepoSource::from_path(scratch.path().to_path_buf()),
        "service-a".to_owned(),
    )
    .with_options(GitIndexerOptions {
        commit_depth: Some(1),
        ..GitIndexerOptions::default()
    });

    let outcome = depth_limited_indexer
        .sync(&store, 1_700_000_100)
        .expect("incremental sync should succeed");
    let HistorySyncOutcome::Incremental { commits_added, .. } = outcome else {
        panic!("reachable anchor beyond commit_depth must stay incremental, got {outcome:?}");
    };
    assert_eq!(commits_added, 2);

    let stored = store
        .get_commits_by_repo("service-a", 0, i64::MAX)
        .expect("commit query should succeed");
    assert_eq!(stored.len(), 6);
}

#[test]
fn sync_recovers_from_history_rewrite_via_full_rescan() {
    let scratch = TestDir::new("sync-rewrite");
    build_fixture_repo(scratch.path());
    let db = TestDb::new("sync-rewrite");
    let store = MetadataStoreDb::open(&db.path).expect("metadata store should open");
    let indexer = GitHistoryIndexer::new(
        GitRepoSource::from_path(scratch.path().to_path_buf()),
        "service-a".to_owned(),
    );

    // Seed the store with the original four commits.
    let HistorySyncOutcome::FullRebuild {
        new_head_sha: original_anchor,
        ..
    } = indexer.sync(&store, 1).expect("first sync should succeed")
    else {
        panic!("first sync should be FullRebuild");
    };
    store
        .replace_file_analytics_for_repo(
            "service-a",
            &[FileAnalytics {
                repo: "service-a".to_owned(),
                file_path: "src/renamed.rs".to_owned(),
                total_commits: 3,
                commits_90d: 3,
                commits_180d: 3,
                commits_365d: 3,
                hotspot_score: 8.0,
                bus_factor: 1,
                top_owner_email: Some("alice@example.com".to_owned()),
                top_owner_pct: 0.9,
                complexity_trend: None,
                last_modified: 1,
                computed_at: 1,
            }],
        )
        .expect("seed analytics");
    store
        .replace_co_change_pairs_for_repo(
            "service-a",
            &[CoChangePairRecord {
                repo: "service-a".to_owned(),
                file_a: "src/renamed.rs".to_owned(),
                file_b: "src/helper.rs".to_owned(),
                strength: 1.0,
                occurrences: 2,
                last_seen: 1,
            }],
        )
        .expect("seed co-change");

    // Force a history rewrite: reset to the parent of HEAD, then create a
    // fresh commit on top. The previously recorded anchor SHA still exists
    // as a dangling object but is no longer reachable from the new HEAD.
    git(scratch.path(), &["reset", "--hard", "HEAD~1"], &[]);
    write_file(
        scratch.path(),
        "src/replacement.rs",
        "fn replacement() {}\n",
    );
    add_all(scratch.path());
    commit_at(scratch.path(), "feat: replacement", 1_736_900_500, None);

    // Sync again — the indexer must detect the rewrite, purge the repo's
    // existing commit rows, full-rebuild, and report the previous anchor.
    let outcome = indexer
        .sync(&store, 2)
        .expect("rewrite sync should succeed");
    let HistorySyncOutcome::HistoryRewriteFallback {
        previous_anchor_sha,
        commits_added,
        new_head_sha,
        ..
    } = outcome
    else {
        panic!("expected HistoryRewriteFallback, got {outcome:?}");
    };
    assert_eq!(previous_anchor_sha, original_anchor);
    assert_ne!(new_head_sha, original_anchor, "HEAD should have moved");
    // After rewrite: 3 original commits (commits 1, 2, 3 of the fixture
    // remained reachable after `reset --hard HEAD~1`) plus the new
    // `replacement` commit.
    assert_eq!(commits_added, 4);

    let after = store
        .get_commits_by_repo("service-a", 0, i64::MAX)
        .expect("commit query should succeed");
    assert_eq!(after.len(), 4, "rewrite path must purge the dropped commit");
    assert!(
        after.iter().all(|commit| commit.sha != original_anchor),
        "the dropped commit's row must be gone after the rewrite recovery"
    );
    assert!(
        store
            .list_file_analytics_for_repo("service-a")
            .expect("analytics query should succeed")
            .is_empty()
    );
    assert!(
        store
            .get_co_change_pairs_for_repo("service-a")
            .expect("co-change query should succeed")
            .is_empty()
    );
}
