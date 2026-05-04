//! Oxc adapter self-validation guards.
//!
//! Before v3.1 these tests cross-validated the Oxc adapter against the SWC
//! visitor on every TS/JS extraction fixture. SWC is no longer linked into
//! the build, so the parity guards now anchor the Oxc adapter against
//! curated expected shapes — every fixture must still produce a
//! parseable program with at least one declared name and a stable
//! import-binding shape.

use std::{
    fs,
    path::{Path, PathBuf},
};

use gather_step_parser::{ImportBinding, oxc_test_support};

const TS_JS_EXTENSIONS: &[&str] = &["ts", "tsx", "mts", "cts", "js", "jsx", "mjs", "cjs"];

#[test]
fn oxc_parses_every_extraction_fixture_without_unrecoverable_errors() {
    let fixtures = collect_ts_js_fixtures(&fixture_root());
    assert!(
        !fixtures.is_empty(),
        "expected TS/JS extraction fixtures to exercise the Oxc adapter"
    );

    let mut regressions = Vec::new();
    for fixture in fixtures {
        let source = fs::read_to_string(&fixture)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", fixture.display()));
        let status = oxc_test_support::parse_recovery_status_for_path(&fixture, &source);
        if status == "unrecoverable" {
            let relative = fixture.strip_prefix(fixture_root()).unwrap_or(&fixture);
            regressions.push(format!("{}", relative.display()));
        }
    }

    assert!(
        regressions.is_empty(),
        "Oxc parser hit unrecoverable errors on:\n{}",
        regressions.join("\n")
    );
}

#[test]
fn oxc_top_level_declared_names_have_stable_shape() {
    let root = fixture_root();
    let fixtures = collect_ts_js_fixtures(&root);
    let mut empty_fixtures = Vec::new();

    for fixture in fixtures {
        let source = fs::read_to_string(&fixture)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", fixture.display()));
        let names = oxc_test_support::top_level_declared_names_for_path(&fixture, &source);
        if names.is_empty() && !source_has_only_imports_or_reexports(&source) {
            let relative = fixture.strip_prefix(&root).unwrap_or(&fixture);
            empty_fixtures.push(format!("{}", relative.display()));
        }
    }

    assert!(
        empty_fixtures.is_empty(),
        "Oxc adapter returned no top-level declarations for fixtures with declarations:\n{}",
        empty_fixtures.join("\n")
    );
}

#[test]
fn oxc_import_bindings_round_trip_on_extraction_fixtures() {
    let root = fixture_root();
    let fixtures = collect_ts_js_fixtures(&root);

    for fixture in fixtures {
        let source = fs::read_to_string(&fixture)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", fixture.display()));
        let bindings = oxc_test_support::parse_import_bindings_for_path(&fixture, &source);
        for binding in &bindings {
            assert!(
                !binding.local_name.is_empty(),
                "{}: empty local_name in import binding",
                fixture.display()
            );
            assert!(
                !binding.source.is_empty(),
                "{}: empty source in import binding",
                fixture.display()
            );
        }
        assert!(
            import_shapes_are_unique(&bindings),
            "duplicate (local, source) pair in {}",
            fixture.display()
        );
    }
}

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/extraction_fidelity")
}

fn import_shapes_are_unique(bindings: &[ImportBinding]) -> bool {
    let mut seen = std::collections::BTreeSet::new();
    for binding in bindings {
        let key = (
            binding.local_name.clone(),
            binding.source.clone(),
            binding.imported_name.clone(),
            binding.is_default,
            binding.is_namespace,
        );
        if !seen.insert(key) {
            return false;
        }
    }
    true
}

fn source_has_only_imports_or_reexports(source: &str) -> bool {
    let trimmed = source
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with("//"))
        .collect::<Vec<_>>();
    trimmed.iter().all(|line| {
        line.starts_with("import ")
            || line.starts_with("export {")
            || line.starts_with("export *")
            || line.starts_with("export type ")
    })
}

fn collect_ts_js_fixtures(root: &Path) -> Vec<PathBuf> {
    let mut fixtures = Vec::new();
    collect_ts_js_fixtures_inner(root, &mut fixtures);
    fixtures.sort();
    fixtures
}

fn collect_ts_js_fixtures_inner(path: &Path, fixtures: &mut Vec<PathBuf>) {
    if path.is_dir() {
        let mut entries: Vec<_> = fs::read_dir(path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()))
            .map(|entry| {
                entry.unwrap_or_else(|error| {
                    panic!(
                        "failed to read directory entry in {}: {error}",
                        path.display()
                    )
                })
            })
            .collect();
        entries.sort_by_key(|entry| entry.path());
        for entry in entries {
            collect_ts_js_fixtures_inner(&entry.path(), fixtures);
        }
    } else if is_ts_js_fixture(path) {
        fixtures.push(path.to_path_buf());
    }
}

fn is_ts_js_fixture(path: &Path) -> bool {
    path.extension()
        .and_then(|value| value.to_str())
        .is_some_and(|ext| {
            TS_JS_EXTENSIONS
                .iter()
                .any(|candidate| ext.eq_ignore_ascii_case(candidate))
        })
}
