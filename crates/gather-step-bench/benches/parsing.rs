use std::path::{Path, PathBuf};

use criterion::{Criterion, criterion_group, criterion_main};
use gather_step_parser::{
    FileEntry, Language, ParseError, frameworks::Framework, parse_file_with_frameworks,
};

fn extraction_fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../crates/gather-step-parser/tests/fixtures/extraction_fidelity")
}

/// All extraction-fixture files with their expected languages.
fn extraction_fixtures() -> Vec<(&'static str, Language)> {
    vec![
        ("barrel_reexports.ts", Language::TypeScript),
        ("bom_unicode.ts", Language::TypeScript),
        (
            "controller_with_imported_route_constant.ts",
            Language::TypeScript,
        ),
        ("custom_event_pattern.ts", Language::TypeScript),
        ("generic_constructor.ts", Language::TypeScript),
        ("import_comment_bindings.ts", Language::TypeScript),
        ("js_imports.js", Language::JavaScript),
        ("jsdoc_decorators.ts", Language::TypeScript),
        ("module_types.mts", Language::TypeScript),
        ("multi_pattern_events.ts", Language::TypeScript),
        ("nested_class_declaration.ts", Language::TypeScript),
        ("nested_decorator_args.ts", Language::TypeScript),
        ("processor_without_args.ts", Language::TypeScript),
        ("route_constants.ts", Language::TypeScript),
        ("x.ts", Language::TypeScript),
        ("y.ts", Language::TypeScript),
    ]
}

fn try_parse_fixture(root: &Path, file_name: &str, language: Language) -> Result<(), ParseError> {
    let path = Path::new(file_name);
    let file = FileEntry {
        path: path.into(),
        language,
        size_bytes: 0,
        content_hash: [0; 32],
        source_bytes: None,
    };
    parse_file_with_frameworks(
        "bench-extraction-fidelity",
        root,
        &file,
        &[Framework::NestJs],
    )?;
    Ok(())
}

/// Parsing-correctness benchmark: parse every extraction fixture and assert
/// 100 % pass rate.  This is both a correctness check and a throughput
/// measurement — criterion times the inner loop.
fn bench_parsing_correctness(c: &mut Criterion) {
    let root = extraction_fixture_root();
    let fixtures = extraction_fixtures();

    // Verify correctness outside the timed loop.
    let total = fixtures.len();
    let mut passed = 0_usize;
    let mut failed: Vec<String> = Vec::new();

    for (file_name, language) in &fixtures {
        match try_parse_fixture(&root, file_name, *language) {
            Ok(()) => passed += 1,
            Err(e) => failed.push(format!("{file_name}: {e}")),
        }
    }

    // Counts for this fixture corpus will never exceed f64 precision range.
    #[expect(
        clippy::cast_precision_loss,
        reason = "fixture counts are small; f64 is sufficient for pass-rate display"
    )]
    let pass_rate = passed as f64 / total as f64;
    // Enforce 100 % pass rate threshold (parsing_correctness.pass_rate_min = 1.0).
    assert!(
        pass_rate >= 1.0,
        "parsing pass rate {pass_rate:.2} < 1.00 (100%); failures: {failed:?}"
    );

    // Criterion benchmark: time parsing all fixtures in one group iteration.
    let mut group = c.benchmark_group("parsing_correctness");
    group.sample_size(20);
    group.bench_function("extraction_fixtures", |b| {
        b.iter(|| {
            for (file_name, language) in &fixtures {
                let _ = try_parse_fixture(&root, file_name, *language);
            }
        });
    });
    group.finish();
}

criterion_group!(benches, bench_parsing_correctness);
criterion_main!(benches);
