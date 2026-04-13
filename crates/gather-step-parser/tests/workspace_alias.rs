//! Integration tests for workspace-local package alias resolution.
//!
//! Tests that imports from workspace-local package names (e.g.
//! `@workspace/contracts`) are resolved to the actual source file paths of the
//! package entry-point, without requiring `node_modules` to be present.

use std::path::Path;

use gather_step_parser::{tsconfig::PathAliases, workspace_manifest::discover_workspace_packages};

/// Canonical fixture root for these tests.
fn fixture_root() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/workspace_alias/workspace_root")
}

/// `discover_workspace_packages` finds the `@workspace/contracts` package and
/// its `src/index.ts` entry-point from the fixture workspace.
#[test]
fn discovers_contracts_package_from_fixture() {
    let workspace_root = fixture_root();
    let packages = discover_workspace_packages(&workspace_root);

    // The fixture workspace has two packages: contracts and consumer.
    // consumer has no resolvable entry-point (no index file, no main pointing
    // at an existing file in the fixture), so only contracts should appear.
    let contracts = packages
        .iter()
        .find(|p| p.name == "@workspace/contracts")
        .expect("@workspace/contracts should be discovered");

    assert_eq!(
        contracts.main_entry,
        workspace_root.join("packages/contracts/src/index.ts"),
        "entry-point should resolve to packages/contracts/src/index.ts"
    );
}

/// `PathAliases::add_workspace_packages` inserts the workspace package entry
/// so that `rewrite("@workspace/contracts")` returns the absolute path to
/// `packages/contracts/src/index.ts`.
#[test]
fn workspace_alias_resolves_cross_package_import() {
    let workspace_root = fixture_root();

    // Step 1: discover packages
    let packages = discover_workspace_packages(&workspace_root);
    assert!(
        !packages.is_empty(),
        "workspace packages should be discovered from fixture"
    );

    // Step 2: build PathAliases (no tsconfig.json in fixture — starts empty)
    let mut aliases = PathAliases::empty();
    aliases.add_workspace_packages(&packages);

    // Step 3: resolve @workspace/contracts via the alias system
    let resolved = aliases
        .rewrite("@workspace/contracts")
        .expect("@workspace/contracts should resolve via workspace alias");

    // Step 4: assert it resolves to the absolute path of packages/contracts/src/index.ts
    let expected = workspace_root
        .join("packages/contracts/src/index.ts")
        .to_string_lossy()
        .into_owned();

    assert_eq!(
        resolved, expected,
        "@workspace/contracts should resolve to packages/contracts/src/index.ts"
    );
}

/// When a `tsconfig.json` already provides an alias for the same package name,
/// the tsconfig alias wins and the workspace entry is not inserted.
#[test]
fn tsconfig_alias_takes_precedence_over_workspace_alias() {
    use std::{
        env, fs, process,
        sync::atomic::{AtomicU64, Ordering},
    };

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let temp_dir = env::temp_dir().join(format!(
        "gather-step-ws-alias-precedence-{}-{counter}",
        process::id()
    ));
    fs::create_dir_all(&temp_dir).expect("temp dir should create");

    // Write a tsconfig that maps @workspace/contracts to a different path.
    fs::write(
        temp_dir.join("tsconfig.json"),
        r#"{ "compilerOptions": { "paths": { "@workspace/contracts": ["overridden/index.ts"] } } }"#,
    )
    .expect("tsconfig fixture should write");

    let workspace_root = fixture_root();
    let packages = discover_workspace_packages(&workspace_root);

    let mut aliases = PathAliases::from_repo_root(&temp_dir);
    aliases.add_workspace_packages(&packages);

    let resolved = aliases
        .rewrite("@workspace/contracts")
        .expect("@workspace/contracts should resolve");

    // The tsconfig path should win, not the workspace-discovered path.
    assert_eq!(
        resolved, "overridden/index.ts",
        "tsconfig alias should take precedence over workspace discovery"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

/// Workspace packages that lack a resolvable entry-point are simply absent
/// from the resolved aliases — they neither error nor produce a broken alias.
#[test]
fn package_without_entry_point_is_absent_from_aliases() {
    let workspace_root = fixture_root();
    let packages = discover_workspace_packages(&workspace_root);

    let mut aliases = PathAliases::empty();
    aliases.add_workspace_packages(&packages);

    // @workspace/consumer has no index file and no `main` pointing at an
    // existing file — it should not appear as an alias at all.
    let consumer_resolved = aliases.rewrite("@workspace/consumer");
    assert!(
        consumer_resolved.is_none(),
        "@workspace/consumer should not resolve when no entry-point exists"
    );
}
