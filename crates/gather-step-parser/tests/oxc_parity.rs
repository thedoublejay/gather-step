use std::{
    fs,
    path::{Path, PathBuf},
};

use gather_step_parser::{
    FileEntry, ImportBinding, classify_language, oxc_test_support, parse_file, swc_test_support,
};

const TS_JS_EXTENSIONS: &[&str] = &["ts", "tsx", "mts", "cts", "js", "jsx", "mjs", "cjs"];

#[test]
fn oxc_parse_status_is_not_worse_than_swc_on_extraction_fixtures() {
    let fixtures = collect_ts_js_fixtures(&fixture_root());
    assert!(
        !fixtures.is_empty(),
        "expected TS/JS extraction fixtures to exercise Oxc parity"
    );

    let mut regressions = Vec::new();
    for fixture in fixtures {
        let source = fs::read_to_string(&fixture)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", fixture.display()));
        let ext = fixture
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or_default();
        let swc_status = swc_test_support::parse_recovery_status_for_extension(ext, &source);
        let oxc_status = oxc_test_support::parse_recovery_status_for_path(&fixture, &source);
        if status_rank(oxc_status) > status_rank(swc_status) {
            let relative = fixture
                .strip_prefix(fixture_root())
                .unwrap_or(&fixture)
                .display();
            regressions.push(format!("{relative}: swc={swc_status}, oxc={oxc_status}"));
        }
    }

    assert!(
        regressions.is_empty(),
        "Oxc parser status regressed relative to SWC:\n{}",
        regressions.join("\n")
    );
}

#[test]
fn oxc_import_bindings_match_swc_on_extraction_fixtures() {
    let root = fixture_root();
    let fixtures = collect_ts_js_fixtures(&root);
    let mut mismatches = Vec::new();

    for fixture in fixtures {
        let source = fs::read_to_string(&fixture)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", fixture.display()));
        let oxc = import_shapes(oxc_test_support::parse_import_bindings_for_path(
            &fixture, &source,
        ));
        let swc = import_shapes(swc_import_bindings(&root, &fixture));

        if oxc != swc {
            let relative = fixture.strip_prefix(&root).unwrap_or(&fixture).display();
            mismatches.push(format!("{relative}: swc={swc:?}, oxc={oxc:?}"));
        }
    }

    assert!(
        mismatches.is_empty(),
        "Oxc import/re-export extraction diverged from SWC:\n{}",
        mismatches.join("\n")
    );
}

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/extraction_fidelity")
}

fn swc_import_bindings(root: &Path, fixture: &Path) -> Vec<ImportBinding> {
    let relative = fixture
        .strip_prefix(root)
        .unwrap_or_else(|error| panic!("fixture path should be under root: {error}"));
    let language = classify_language(relative)
        .unwrap_or_else(|| panic!("unsupported fixture path: {}", relative.display()));
    let file = FileEntry {
        path: relative.to_path_buf(),
        language,
        size_bytes: 0,
        content_hash: [0; 32],
        source_bytes: None,
    };
    parse_file("extraction-fixtures", root, &file)
        .unwrap_or_else(|error| panic!("failed to parse {}: {error}", relative.display()))
        .import_bindings
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ImportShape {
    local_name: String,
    imported_name: Option<String>,
    source: String,
    is_default: bool,
    is_namespace: bool,
    is_type_only: bool,
}

fn import_shapes(bindings: Vec<ImportBinding>) -> Vec<ImportShape> {
    let mut shapes = bindings
        .into_iter()
        .map(|binding| ImportShape {
            local_name: binding.local_name,
            imported_name: binding.imported_name,
            source: binding.source,
            is_default: binding.is_default,
            is_namespace: binding.is_namespace,
            is_type_only: binding.is_type_only,
        })
        .collect::<Vec<_>>();
    shapes.sort();
    shapes
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

fn status_rank(status: &str) -> u8 {
    match status {
        "parsed" => 0,
        "recovered" => 1,
        "unrecoverable" => 2,
        _ => panic!("unknown parser status {status:?}"),
    }
}
