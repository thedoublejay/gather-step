use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use gather_step_parser::{FileEntry, ImportBinding, Language, ParsedFile, parse_file};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ImportSummary {
    source: String,
    local_name: String,
    imported_name: Option<String>,
    is_namespace: bool,
    resolved_path: Option<String>,
}

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/python/import_resolution")
}

fn parse_python_fixture(relative_path: &str) -> ParsedFile {
    let root = fixture_root();
    let path = Path::new(relative_path);
    let language = match path.extension().and_then(std::ffi::OsStr::to_str) {
        Some("py" | "pyi") => Language::Python,
        other => panic!("unsupported Python fixture extension: {other:?}"),
    };
    let file = FileEntry {
        path: path.into(),
        language,
        size_bytes: 0,
        content_hash: [0; 32],
        source_bytes: None,
    };
    parse_file("python-import-regression", &root, &file).expect("fixture should parse")
}

fn import_summary(parsed: &ParsedFile) -> BTreeSet<ImportSummary> {
    parsed
        .import_bindings
        .iter()
        .map(summarize_import)
        .collect()
}

fn summarize_import(binding: &ImportBinding) -> ImportSummary {
    let root = fixture_root();
    ImportSummary {
        source: binding.source.clone(),
        local_name: binding.local_name.clone(),
        imported_name: binding.imported_name.clone(),
        is_namespace: binding.is_namespace,
        resolved_path: binding
            .resolved_path
            .as_ref()
            .and_then(|path| path.strip_prefix(&root).ok())
            .map(|path| path.to_string_lossy().replace('\\', "/")),
    }
}

#[test]
fn python_import_extraction_is_ast_backed_and_deterministic() {
    let first = parse_python_fixture("package/app/imports.py");
    let second = parse_python_fixture("package/app/imports.py");
    let imports = import_summary(&first);

    assert_eq!(imports, import_summary(&second));
    assert!(imports.contains(&ImportSummary {
        source: "os".to_owned(),
        local_name: "os".to_owned(),
        imported_name: None,
        is_namespace: true,
        resolved_path: None,
    }));
    assert!(imports.contains(&ImportSummary {
        source: "pkg.submodule".to_owned(),
        local_name: "submodule".to_owned(),
        imported_name: None,
        is_namespace: true,
        resolved_path: Some("pkg/submodule.py".to_owned()),
    }));
    assert!(imports.contains(&ImportSummary {
        source: "shared.models".to_owned(),
        local_name: "BillingAccount".to_owned(),
        imported_name: Some("Account".to_owned()),
        is_namespace: false,
        resolved_path: Some("shared/models.py".to_owned()),
    }));
    assert!(imports.contains(&ImportSummary {
        source: ".services".to_owned(),
        local_name: "TaskRunner".to_owned(),
        imported_name: Some("Runner".to_owned()),
        is_namespace: false,
        resolved_path: Some("package/app/services.py".to_owned()),
    }));
    assert!(imports.contains(&ImportSummary {
        source: ".services".to_owned(),
        local_name: "svc".to_owned(),
        imported_name: Some("services".to_owned()),
        is_namespace: false,
        resolved_path: Some("package/app/services.py".to_owned()),
    }));
    assert!(imports.contains(&ImportSummary {
        source: "..shared".to_owned(),
        local_name: "shared".to_owned(),
        imported_name: Some("shared".to_owned()),
        is_namespace: false,
        resolved_path: Some("package/shared.py".to_owned()),
    }));
    assert!(imports.contains(&ImportSummary {
        source: "..shared".to_owned(),
        local_name: "*".to_owned(),
        imported_name: Some("*".to_owned()),
        is_namespace: false,
        resolved_path: Some("package/shared.py".to_owned()),
    }));
}

#[test]
fn python_parser_accepts_stubs_and_malformed_files_without_panics() {
    let stub = parse_python_fixture("shared/models.pyi");
    assert!(
        stub.symbols
            .iter()
            .any(|symbol| symbol.node.name == "StubbedModel")
    );

    let malformed = parse_python_fixture("package/app/malformed.py");
    let imports = import_summary(&malformed);
    assert!(imports.contains(&ImportSummary {
        source: "shared.models".to_owned(),
        local_name: "User".to_owned(),
        imported_name: Some("User".to_owned()),
        is_namespace: false,
        resolved_path: Some("shared/models.py".to_owned()),
    }));
    assert!(
        malformed
            .symbols
            .iter()
            .any(|symbol| symbol.node.name == "StillUseful")
    );
}
