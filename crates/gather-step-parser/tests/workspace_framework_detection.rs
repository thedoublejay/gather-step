//! Integration tests for workspace-aware framework detection.
//!
//! `detect_frameworks` only scans the repo root manifest and `<root>/src`. In a
//! monorepo whose members live under `apps/*` / `packages/*` (npm `workspaces`
//! or `pnpm-workspace.yaml`), the framework dependencies and source markers sit
//! inside the member directories, so a root-only scan detects nothing.
//! `detect_frameworks_workspace_aware` unions detection across discovered
//! members, which is what makes NestJS detection fire — and, via each member's
//! own `src/`, what lets the nested app's events extract.

use std::path::{Path, PathBuf};

use gather_step_core::NodeKind;
use gather_step_parser::{
    FileEntry, classify_language,
    frameworks::{Framework, detect_frameworks, detect_frameworks_workspace_aware},
    parse_file_with_frameworks,
};

fn fixture_root(monorepo: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/workspace_framework_detection")
        .join(monorepo)
}

/// Parse a fixture-relative TypeScript file with the given frameworks active.
fn parse_member_file(monorepo: &str, relative_path: &str, frameworks: &[Framework]) -> Vec<String> {
    let root = fixture_root(monorepo);
    let path = Path::new(relative_path);
    let language = classify_language(path)
        .unwrap_or_else(|| panic!("unsupported fixture path: {}", path.display()));
    let file = FileEntry {
        path: path.into(),
        language,
        size_bytes: 0,
        content_hash: [0; 32],
        source_bytes: None,
    };
    let parsed =
        parse_file_with_frameworks("workspace-detection-fixtures", &root, &file, frameworks)
            .expect("fixture should parse");
    parsed
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Event)
        .filter_map(|node| node.external_id.clone())
        .collect()
}

#[test]
fn pnpm_monorepo_detects_nestjs_in_nested_app() {
    let root = fixture_root("pnpm_monorepo");

    // Root-only detection sees only the always-on FrontendHooks pack: the root
    // manifest has no framework deps and there is no `<root>/src`.
    let root_only = detect_frameworks(&root);
    assert!(
        !root_only.contains(&Framework::NestJs),
        "root-only detection must miss the nested NestJS app (this is the bug)"
    );

    // Workspace-aware detection unions the `apps/api` member, where
    // `@nestjs/core` lives.
    let aware = detect_frameworks_workspace_aware(&root);
    assert!(
        aware.contains(&Framework::NestJs),
        "workspace-aware detection must find NestJS in the pnpm-workspace member"
    );
}

#[test]
fn npm_workspaces_monorepo_detects_nestjs_in_nested_app() {
    let root = fixture_root("npm_monorepo");

    let root_only = detect_frameworks(&root);
    assert!(
        !root_only.contains(&Framework::NestJs),
        "root-only detection must miss the nested NestJS app (this is the bug)"
    );

    let aware = detect_frameworks_workspace_aware(&root);
    assert!(
        aware.contains(&Framework::NestJs),
        "workspace-aware detection must find NestJS in the npm-workspaces member"
    );
}

#[test]
fn pnpm_monorepo_nested_app_emits_event_node() {
    // With NestJS active (as workspace-aware detection now resolves), the nested
    // app's `@EventPattern('order.created')` handler emits a kafka Event node.
    let events = parse_member_file(
        "pnpm_monorepo",
        "apps/api/src/orders.controller.ts",
        &[Framework::NestJs],
    );
    assert!(
        events
            .iter()
            .any(|id| id == "__event__kafka__order.created"),
        "nested app should emit an Event node for the consumed kafka topic; got {events:?}"
    );
}

#[test]
fn npm_monorepo_nested_app_emits_event_node() {
    let events = parse_member_file(
        "npm_monorepo",
        "apps/api/src/orders.controller.ts",
        &[Framework::NestJs],
    );
    assert!(
        events
            .iter()
            .any(|id| id == "__event__kafka__order.created"),
        "nested app should emit an Event node for the consumed kafka topic; got {events:?}"
    );
}
