use std::{
    fs,
    path::{Path, PathBuf},
};

use gather_step_parser::{oxc_test_support, swc_test_support};

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

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/extraction_fidelity")
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
