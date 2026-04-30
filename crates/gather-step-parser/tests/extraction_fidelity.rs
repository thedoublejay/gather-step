use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use gather_step_core::{EdgeKind, NodeKind};
use gather_step_parser::{
    FileEntry, Language, ParsedFile, classify_language, frameworks::Framework, parse_file,
    parse_file_with_context, parse_file_with_frameworks, parse_file_with_packs,
};
use insta::assert_debug_snapshot;

#[expect(dead_code, reason = "fields used only in Debug snapshot output")]
#[derive(Debug)]
struct ParsedSummary {
    nodes: Vec<NodeSummary>,
    edges: Vec<EdgeSummary>,
    symbols: Vec<SymbolSummary>,
    imports: Vec<ImportSummary>,
    call_sites: Vec<CallSiteSummary>,
    constant_strings: BTreeMap<String, String>,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct NodeSummary {
    kind: String,
    name: String,
    qualified_name: Option<String>,
    external_id: Option<String>,
    signature: Option<String>,
    is_virtual: bool,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct EdgeSummary {
    kind: String,
    source: String,
    target: String,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct SymbolSummary {
    name: String,
    decorators: Vec<(String, Vec<String>)>,
    constructor_dependencies: Vec<String>,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ImportSummary {
    local_name: String,
    imported_name: Option<String>,
    source: String,
    resolved_path: Option<String>,
    is_default: bool,
    is_namespace: bool,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CallSiteSummary {
    callee: String,
    literal_argument: Option<String>,
}

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/extraction_fidelity")
}

fn parse_fixture(relative_path: &str, frameworks: &[Framework]) -> ParsedFile {
    let root = fixture_root();
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
    if frameworks.is_empty() {
        parse_file("extraction-fixtures", &root, &file).expect("fixture should parse")
    } else {
        parse_file_with_frameworks("extraction-fixtures", &root, &file, frameworks)
            .expect("fixture should parse")
    }
}

fn display_node(parsed: &ParsedFile, id: gather_step_core::NodeId) -> String {
    let mut names = BTreeMap::new();
    names.insert(
        parsed.file_node.id,
        parsed
            .file_node
            .qualified_name
            .clone()
            .unwrap_or_else(|| parsed.file_node.name.clone()),
    );
    for node in &parsed.nodes {
        names.insert(
            node.id,
            node.external_id
                .clone()
                .or_else(|| node.qualified_name.clone())
                .unwrap_or_else(|| format!("{:?}:{}", node.kind, node.name)),
        );
    }
    names
        .get(&id)
        .cloned()
        .unwrap_or_else(|| format!("unknown:{id:?}"))
}

fn summarize(parsed: &ParsedFile) -> ParsedSummary {
    let mut nodes: Vec<_> = parsed
        .nodes
        .iter()
        .map(|node| NodeSummary {
            kind: format!("{:?}", node.kind),
            name: node.name.clone(),
            qualified_name: node.qualified_name.clone(),
            external_id: node.external_id.clone(),
            signature: node.signature.clone(),
            is_virtual: node.is_virtual,
        })
        .collect();
    nodes.sort();

    let mut edges: Vec<_> = parsed
        .edges
        .iter()
        .map(|edge| EdgeSummary {
            kind: format!("{:?}", edge.kind),
            source: display_node(parsed, edge.source),
            target: display_node(parsed, edge.target),
        })
        .collect();
    edges.sort();

    let mut symbols: Vec<_> = parsed
        .symbols
        .iter()
        .map(|symbol| SymbolSummary {
            name: symbol.node.name.clone(),
            decorators: symbol
                .decorators
                .iter()
                .map(|decorator| {
                    let args: Vec<String> = decorator
                        .arguments
                        .iter()
                        .map(|arg| arg.as_ref().to_owned())
                        .collect();
                    (decorator.name.clone(), args)
                })
                .collect(),
            constructor_dependencies: symbol.constructor_dependencies.clone(),
        })
        .collect();
    symbols.sort();

    let mut imports: Vec<_> = parsed
        .import_bindings
        .iter()
        .map(|binding| ImportSummary {
            local_name: binding.local_name.clone(),
            imported_name: binding.imported_name.clone(),
            source: binding.source.clone(),
            resolved_path: binding.resolved_path.as_ref().map(|p| {
                p.file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned()
            }),
            is_default: binding.is_default,
            is_namespace: binding.is_namespace,
        })
        .collect();
    imports.sort();

    let mut call_sites: Vec<_> = parsed
        .call_sites
        .iter()
        .map(|cs| CallSiteSummary {
            callee: cs.callee_name.clone(),
            literal_argument: cs.literal_argument.clone(),
        })
        .collect();
    call_sites.sort();

    let constant_strings: BTreeMap<String, String> = parsed
        .constant_strings
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    ParsedSummary {
        nodes,
        edges,
        symbols,
        imports,
        call_sites,
        constant_strings,
    }
}

fn data_field_names(parsed: &ParsedFile) -> BTreeSet<String> {
    parsed
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::DataField)
        .map(|node| node.name.clone())
        .collect()
}

fn field_edge_kinds(parsed: &ParsedFile, field: &str) -> BTreeSet<String> {
    parsed
        .edges
        .iter()
        .filter_map(|edge| {
            parsed
                .nodes
                .iter()
                .find(|node| node.id == edge.target && node.kind == NodeKind::DataField)
                .filter(|node| node.name == field)
                .map(|_| format!("{:?}", edge.kind))
        })
        .collect()
}

fn has_derivation(parsed: &ParsedFile, source: &str, target: &str) -> bool {
    parsed.edges.iter().any(|edge| {
        if edge.kind != EdgeKind::DerivesFieldFrom {
            return false;
        }
        let source_name = parsed
            .nodes
            .iter()
            .find(|node| node.id == edge.source)
            .map(|node| node.name.as_str());
        let target_name = parsed
            .nodes
            .iter()
            .find(|node| node.id == edge.target)
            .map(|node| node.name.as_str());
        source_name == Some(source) && target_name == Some(target)
    })
}

fn assert_fields(parsed: &ParsedFile, expected: &[&str]) {
    let fields = data_field_names(parsed);
    for field in expected {
        assert!(
            fields.contains(*field),
            "missing data field `{field}`; observed={fields:?}"
        );
    }
}

#[test]
fn projection_schema_pipeline_extracts_class_interface_and_mongoose_fields() {
    let parsed = parse_fixture("projection_schema_pipeline.ts", &[Framework::NestJs]);

    assert_fields(
        &parsed,
        &["customers", "customerIds", "status", "customerStatus"],
    );
    assert!(has_derivation(&parsed, "customers", "customerIds"));
    assert!(has_derivation(&parsed, "status", "customerStatus"));
}

#[test]
fn projection_mongo_pipeline_extracts_update_lookup_and_mapping_edges() {
    let parsed = parse_fixture("projection_mongo_pipeline.ts", &[]);

    assert_fields(
        &parsed,
        &[
            "invoiceItems",
            "invoiceItemTotal",
            "orders",
            "orderIds",
            "archivedOrderIds",
            "tagIds",
        ],
    );
    assert!(has_derivation(&parsed, "invoiceItems", "invoiceItemTotal"));
    assert!(has_derivation(&parsed, "orders", "orderIds"));
    assert!(field_edge_kinds(&parsed, "invoiceItemTotal").contains("WritesField"));
    assert!(field_edge_kinds(&parsed, "invoiceItemTotal").contains("BackfillsField"));
    assert!(field_edge_kinds(&parsed, "invoiceItemTotal").contains("FiltersOnField"));
    assert!(field_edge_kinds(&parsed, "invoiceItemTotal").contains("IndexesField"));
    assert!(field_edge_kinds(&parsed, "orderIds").contains("WritesField"));
    assert!(field_edge_kinds(&parsed, "orderIds").contains("FiltersOnField"));
    assert!(field_edge_kinds(&parsed, "archivedOrderIds").contains("WritesField"));
    assert!(field_edge_kinds(&parsed, "tagIds").contains("WritesField"));
}

#[test]
fn projection_optional_chaining_fixture_extracts_derivations() {
    let parsed = parse_fixture("projection_optional_chaining.ts", &[]);

    assert_fields(
        &parsed,
        &[
            "lineItems",
            "lineItemTotal",
            "orders",
            "orderIds",
            "status",
            "accountStatus",
        ],
    );
    assert!(has_derivation(&parsed, "lineItems", "lineItemTotal"));
    assert!(has_derivation(&parsed, "orders", "orderIds"));
    assert!(has_derivation(&parsed, "status", "accountStatus"));
}

#[test]
fn projection_mapping_fixtures_extract_json_and_yaml_index_fields() {
    let json = parse_fixture("projection_json_mapping.json", &[]);
    let yaml = parse_fixture("projection_yaml_mapping.yaml", &[]);

    for parsed in [&json, &yaml] {
        assert_fields(parsed, &["invoiceItemTotal", "orderIds"]);
        assert!(
            field_edge_kinds(parsed, "invoiceItemTotal").contains("IndexesField"),
            "mapping fixture should mark invoiceItemTotal as indexed"
        );
        assert!(
            field_edge_kinds(parsed, "orderIds").contains("IndexesField"),
            "mapping fixture should mark orderIds as indexed"
        );
    }
}

#[test]
fn projection_false_positive_fixtures_do_not_emit_data_fields() {
    for fixture in [
        "projection_false_positive_logs.ts",
        "translations/projection_labels.ts",
        "ui/projection_summary.tsx",
        "__mocks__/projection_mock.ts",
        "projection_contract.test.ts",
        "projection_false_positive_count_locals.ts",
    ] {
        let parsed = parse_fixture(fixture, &[]);
        assert!(
            data_field_names(&parsed).is_empty(),
            "fixture `{fixture}` should not emit projection fields; observed={:?}",
            data_field_names(&parsed)
        );
    }
}

#[test]
fn projection_fixtures_accept_separate_domain_names() {
    let parsed = parse_fixture("projection_acme_loyalty.ts", &[]);

    assert_fields(
        &parsed,
        &[
            "pointEvents",
            "loyaltyPointTotal",
            "households",
            "householdIds",
            "rewardBalance",
        ],
    );
    assert!(has_derivation(&parsed, "pointEvents", "loyaltyPointTotal"));
    assert!(has_derivation(&parsed, "households", "householdIds"));
    assert!(has_derivation(&parsed, "pointEvents", "rewardBalance"));
}

#[test]
fn generic_constructor_dependencies_preserve_full_types() {
    let parsed = parse_fixture("generic_constructor.ts", &[Framework::NestJs]);
    let class_symbol = parsed
        .symbols
        .iter()
        .find(|symbol| symbol.node.name == "GenericController")
        .expect("GenericController symbol should exist");
    assert_eq!(
        class_symbol.constructor_dependencies,
        vec!["Service<Generic<T>>", "Repo<A, B>"]
    );
    assert_debug_snapshot!("generic_constructor", summarize(&parsed));
}

#[test]
fn decorator_arguments_preserve_nested_structure() {
    let parsed = parse_fixture("nested_decorator_args.ts", &[]);
    let method_symbol = parsed
        .symbols
        .iter()
        .find(|symbol| symbol.node.name == "handle")
        .expect("handle symbol should exist");
    let metadata = method_symbol
        .decorators
        .iter()
        .find(|decorator| decorator.name == "SetMetadata")
        .expect("SetMetadata decorator should exist");
    let args_vec: Vec<String> = metadata
        .arguments
        .iter()
        .map(|arg| arg.as_ref().to_owned())
        .collect();
    assert_eq!(args_vec, vec!["roles", "['admin', 'ops']"]);
    assert_debug_snapshot!("nested_decorator_args", summarize(&parsed));
}

#[test]
fn import_sources_ignore_comment_noise() {
    let parsed = parse_fixture("import_comment_bindings.ts", &[]);
    assert!(parsed.import_bindings.iter().any(|binding| {
        binding.source == "./x" && binding.local_name == "X" && binding.is_namespace
    }));
    assert!(parsed.import_bindings.iter().any(|binding| {
        binding.source == "./y"
            && binding.local_name == "Y"
            && binding.imported_name.as_deref() == Some("Y")
    }));
    // Finding 8 — now that sibling files x.ts and y.ts exist, the direct
    // `import * as X from "./x"` binding must have its resolved_path populated.
    assert!(
        parsed.import_bindings.iter().any(|b| {
            b.source == "./x"
                && b.resolved_path.as_ref().is_some_and(|p| {
                    p.file_name()
                        .is_some_and(|f| f == std::ffi::OsStr::new("x.ts"))
                })
        }),
        "direct import from ./x must have resolved_path pointing to x.ts"
    );
    assert_debug_snapshot!("import_comment_bindings", summarize(&parsed));
}

#[test]
fn nested_class_declarations_do_not_pollute_outer_constructor_dependencies() {
    let parsed = parse_fixture("nested_class_declaration.ts", &[]);
    let outer = parsed
        .symbols
        .iter()
        .find(|symbol| symbol.node.name == "OuterController")
        .expect("OuterController symbol should exist");
    assert_eq!(outer.constructor_dependencies, vec!["Repository<User>"]);
    assert!(
        parsed
            .symbols
            .iter()
            .all(|symbol| symbol.node.name != "Nested")
    );
    assert_debug_snapshot!("nested_class_declaration", summarize(&parsed));
}

#[test]
fn nestjs_custom_event_and_multi_pattern_fixtures_are_stable() {
    let custom = parse_fixture("custom_event_pattern.ts", &[Framework::NestJs]);
    assert!(
        custom
            .nodes
            .iter()
            .any(|node| { node.external_id.as_deref() == Some("__event__kafka__order.created") })
    );
    assert_debug_snapshot!("custom_event_pattern", summarize(&custom));

    let multi = parse_fixture("multi_pattern_events.ts", &[Framework::NestJs]);
    assert!(
        multi
            .nodes
            .iter()
            .any(|node| { node.external_id.as_deref() == Some("__event__kafka__user.created") })
    );
    assert!(
        multi
            .nodes
            .iter()
            .any(|node| { node.external_id.as_deref() == Some("__event__kafka__user.updated") })
    );
    assert_debug_snapshot!("multi_pattern_events", summarize(&multi));
}

#[test]
fn imported_route_constants_and_barrel_reexports_remain_visible() {
    let route_controller = parse_fixture(
        "controller_with_imported_route_constant.ts",
        &[Framework::NestJs],
    );
    // route_qn() lowercases all path segments via canonical_route_path; the
    // member-expression path is preserved but in lowercase.
    assert!(route_controller.nodes.iter().any(|node| {
        node.external_id.as_deref() == Some("__route__GET__/routeconstants.v2.accounts.details")
    }));
    assert_debug_snapshot!(
        "controller_with_imported_route_constant",
        summarize(&route_controller)
    );

    // Finding 2 — known product gap: imported route constants are not resolved
    // to their literal string value. The current external_id encodes the raw
    // member-expression path rather than the resolved "accounts/details" string.
    // This assertion intentionally locks in the current (non-canonical) behavior
    // so any change — whether regression or fix — is detected.
    assert!(
        route_controller
            .nodes
            .iter()
            .all(|node| { node.external_id.as_deref() != Some("__route__GET__/accounts/details") }),
        "if this fails, imported-constant route resolution is now working — \
         remove this assertion"
    );

    let barrel = parse_fixture("barrel_reexports.ts", &[]);
    assert!(barrel.edges.iter().any(|edge| {
        format!("{:?}", edge.kind) == "Imports"
            && display_node(&barrel, edge.target).contains("module-import::./x")
    }));
    assert!(
        barrel
            .import_bindings
            .iter()
            .any(|binding| { binding.source == "./y" && binding.local_name == "Y" })
    );
    // Finding 8 — re-export sources (`export * from './x'`, `export { Y } from './y'`)
    // are recorded as import_bindings but their resolved_path is left None by the SWC
    // visitor (only direct imports get path resolution at parse time). This is a known
    // depth gap documented in the review. The snapshot captures the current state.
    assert_debug_snapshot!("barrel_reexports", summarize(&barrel));
}

#[test]
fn jsdoc_decorators_are_ignored_and_processor_without_args_is_safe() {
    let jsdoc = parse_fixture("jsdoc_decorators.ts", &[Framework::NestJs]);
    assert!(
        jsdoc
            .nodes
            .iter()
            .all(|node| node.external_id.as_deref() != Some("__route__GET__/fake"))
    );
    // The `list()` method has no HTTP verb decorator — it must produce
    // zero Route nodes. A regression that treated bare methods as implicit
    // @Get() would be caught here.
    assert!(
        jsdoc
            .nodes
            .iter()
            .all(|node| node.kind != gather_step_core::NodeKind::Route),
        "bare method with no HTTP decorator must not produce a Route node"
    );
    assert_debug_snapshot!("jsdoc_decorators", summarize(&jsdoc));

    let processor = parse_fixture("processor_without_args.ts", &[Framework::NestJs]);
    assert!(
        processor
            .nodes
            .iter()
            .all(|node| node.kind != gather_step_core::NodeKind::Queue)
    );
    // An argless @Processor() must also produce zero Consumes edges —
    // there is no queue to consume from.
    assert!(
        processor
            .edges
            .iter()
            .all(|edge| edge.kind != gather_step_core::EdgeKind::Consumes),
        "argless @Processor must produce zero Consumes edges"
    );
    assert_debug_snapshot!("processor_without_args", summarize(&processor));
}

#[test]
fn javascript_file_extracts_imports_and_functions() {
    let parsed = parse_fixture("js_imports.js", &[]);
    // Import bindings must be extracted from plain .js files through the
    // full SWC → visit_module pipeline (not just tree-sitter).
    assert!(
        parsed
            .import_bindings
            .iter()
            .any(|b| b.source == "./utils" && b.local_name == "helper"),
        "named import from ./utils must be captured"
    );
    assert!(
        parsed
            .import_bindings
            .iter()
            .any(|b| b.source == "./lib" && b.is_default),
        "default import from ./lib must be captured"
    );
    // The two exported functions must appear as symbols.
    assert!(
        parsed.nodes.iter().any(|n| n.name == "processItem"),
        "processItem function must be extracted"
    );
    assert!(
        parsed.nodes.iter().any(|n| n.name == "transform"),
        "transform arrow function/const must be extracted"
    );
}

#[test]
fn mts_file_is_classified_as_typescript_and_extracts_symbols() {
    let parsed = parse_fixture("module_types.mts", &[]);
    assert!(
        parsed.nodes.iter().any(|n| n.name == "readStream"),
        "readStream function must be extracted from .mts file"
    );
    assert!(
        parsed
            .import_bindings
            .iter()
            .any(|b| b.source == "node:stream"),
        "type import from node:stream must be captured"
    );
}

#[test]
fn bom_and_unicode_fixture_parses_cleanly() {
    let parsed = parse_fixture("bom_unicode.ts", &[]);
    assert!(
        parsed
            .nodes
            .iter()
            .any(|node| node.name == "café" || node.name == "run")
    );
    // The run() function must be present — it appears after the BOM and the
    // const declaration, so if BOM handling truncated the parse, run() would
    // be absent.
    assert!(
        parsed.nodes.iter().any(|node| node.name == "run"),
        "run() function must be present after BOM stripping"
    );
    // No node name should start with the raw BOM codepoint — that would
    // indicate the BOM was not stripped and leaked into an identifier.
    assert!(
        parsed
            .nodes
            .iter()
            .all(|node| !node.name.starts_with('\u{FEFF}')),
        "no node name should start with BOM codepoint; got: {:?}",
        parsed
            .nodes
            .iter()
            .filter(|n| n.name.starts_with('\u{FEFF}'))
            .collect::<Vec<_>>()
    );
    // run() must have a non-zero span, confirming it was parsed from real
    // source content rather than a fallback empty stub.
    if let Some(run_node) = parsed.nodes.iter().find(|n| n.name == "run")
        && let Some(span) = &run_node.span
    {
        assert!(
            span.line_end() > 0 || span.column_end() > 0,
            "run() node must have a non-zero source span"
        );
    }
    assert_debug_snapshot!("bom_unicode", summarize(&parsed));
}

// ---------------------------------------------------------------------------
// Finding 6 — parse_file_with_context and parse_file_with_packs entry points
// ---------------------------------------------------------------------------

#[test]
fn parse_file_with_context_extracts_symbols_on_empty_aliases() {
    // Success-path: parse_file_with_context with no path aliases should produce
    // the same symbols as parse_file for a plain TypeScript file.
    use gather_step_parser::tsconfig::PathAliases;

    let root = fixture_root();
    let file = FileEntry {
        path: Path::new("generic_constructor.ts").into(),
        language: Language::TypeScript,
        size_bytes: 0,
        content_hash: [0; 32],
        source_bytes: None,
    };
    let aliases = PathAliases::empty();
    let parsed = parse_file_with_context(
        "extraction-fixtures",
        &root,
        &file,
        &[Framework::NestJs],
        &aliases,
    )
    .expect("parse_file_with_context should succeed");

    assert!(
        parsed
            .symbols
            .iter()
            .any(|s| s.node.name == "GenericController"),
        "parse_file_with_context must extract GenericController symbol"
    );
    // Constructor dependencies must still be captured through this entry point.
    let class_symbol = parsed
        .symbols
        .iter()
        .find(|s| s.node.name == "GenericController")
        .unwrap();
    assert_eq!(
        class_symbol.constructor_dependencies,
        vec!["Service<Generic<T>>", "Repo<A, B>"],
        "parse_file_with_context must preserve full generic constructor dependency strings"
    );
}

#[test]
fn parse_file_with_packs_activates_nestjs_extraction() {
    // Success-path: parse_file_with_packs with an explicit NestJS ResolvedPack
    // should produce the same route/event nodes as parse_file_with_frameworks.
    use gather_step_parser::frameworks::profile::ResolvedPack;
    use gather_step_parser::frameworks::registry::PackId;
    use gather_step_parser::tsconfig::PathAliases;

    let root = fixture_root();
    let file = FileEntry {
        path: Path::new("custom_event_pattern.ts").into(),
        language: Language::TypeScript,
        size_bytes: 0,
        content_hash: [0; 32],
        source_bytes: None,
    };
    let nestjs_pack = ResolvedPack {
        id: PackId::Nestjs,
        options: serde_yaml_ng::Value::Null,
    };
    let aliases = PathAliases::empty();
    let parsed = parse_file_with_packs(
        "extraction-fixtures",
        &root,
        &file,
        &[nestjs_pack],
        &aliases,
    )
    .expect("parse_file_with_packs should succeed");

    assert!(
        parsed
            .nodes
            .iter()
            .any(|n| n.external_id.as_deref() == Some("__event__kafka__order.created")),
        "parse_file_with_packs must produce the order.created event node via NestJS pack"
    );
}

// ---------------------------------------------------------------------------
// Finding 9 — SWC traversal skips computed-property subexpressions (#[ignore])
// ---------------------------------------------------------------------------

// Helper used by the SWC traversal gap tests: writes `source` to a unique
// temp dir (isolated from the shared fixture root to avoid races under
// parallel test execution), parses it, and returns the ParsedFile.
fn parse_ts_source_in_tempdir(source: &str, filename: &str) -> ParsedFile {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let dir =
        std::env::temp_dir().join(format!("gather-step-fidelity-{}-{id}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir create should succeed");
    let path = dir.join(filename);
    std::fs::write(&path, source).expect("fixture write should succeed");
    let file = FileEntry {
        path: filename.into(),
        language: Language::TypeScript,
        size_bytes: 0,
        content_hash: [0; 32],
        source_bytes: None,
    };
    let result = parse_file("gap-test", &dir, &file).expect("should parse");
    std::fs::remove_dir_all(&dir).ok();
    result
}

#[test]
fn swc_traversal_captures_call_inside_computed_member_property() {
    let parsed = parse_ts_source_in_tempdir(
        "export function run(obj: any) { return obj[getKey()]?.value; }\n\
         function getKey(): string { return 'k'; }\n",
        "computed_member.ts",
    );
    assert!(
        parsed
            .call_sites
            .iter()
            .any(|cs| cs.callee_name == "getKey"),
        "getKey() in computed member position must appear in call_sites; got: {:?}",
        parsed
            .call_sites
            .iter()
            .map(|cs| &cs.callee_name)
            .collect::<Vec<_>>()
    );
}

#[test]
fn swc_traversal_captures_call_inside_optional_computed_member_property() {
    let parsed = parse_ts_source_in_tempdir(
        "export function run(obj: any) { return obj?.[getName()]?.fn(); }\n\
         function getName(): string { return 'k'; }\n",
        "opt_computed_member.ts",
    );
    assert!(
        parsed
            .call_sites
            .iter()
            .any(|cs| cs.callee_name == "getName"),
        "getName() in optional computed position must appear in call_sites"
    );
}

// ---------------------------------------------------------------------------
// Finding 10 — SWC traversal skips destructuring-assignment expressions (#[ignore])
// ---------------------------------------------------------------------------

#[test]
fn swc_traversal_captures_call_inside_destructuring_default() {
    let parsed = parse_ts_source_in_tempdir(
        "function buildDefault(): number { return 0; }\n\
         export function run(arr: number[]) { let x: number; ([x = buildDefault()] = arr); return x; }\n",
        "destructure_default.ts",
    );
    assert!(
        parsed
            .call_sites
            .iter()
            .any(|cs| cs.callee_name == "buildDefault"),
        "buildDefault() in destructuring default must appear in call_sites; got: {:?}",
        parsed
            .call_sites
            .iter()
            .map(|cs| &cs.callee_name)
            .collect::<Vec<_>>()
    );
}

// ---------------------------------------------------------------------------
// Imported decorator constant / enum resolution
// ---------------------------------------------------------------------------

#[test]
fn imported_enum_member_resolves_to_event_node() {
    let parsed = parse_fixture("imported_enum_consumer.ts", &[Framework::NestJs]);
    assert!(
        parsed
            .nodes
            .iter()
            .any(|n| n.external_id.as_deref() == Some("__event__kafka__notification.events")),
        "imported EventTopic.NotificationEvents must resolve to a canonical event node; nodes: {:?}",
        parsed
            .nodes
            .iter()
            .map(|n| &n.external_id)
            .collect::<Vec<_>>()
    );
    assert!(
        parsed
            .nodes
            .iter()
            .any(|n| n.external_id.as_deref() == Some("__event__kafka__pdf.generation")),
        "imported EventTopic.PdfGeneration must resolve to a canonical event node"
    );
}

#[test]
fn imported_const_string_resolves_to_event_node() {
    let parsed = parse_fixture("imported_const_consumer.ts", &[Framework::NestJs]);
    assert!(
        parsed
            .nodes
            .iter()
            .any(|n| n.external_id.as_deref() == Some("__event__kafka__order.created")),
        "imported ORDER_CREATED const must resolve to an event node; nodes: {:?}",
        parsed
            .nodes
            .iter()
            .map(|n| &n.external_id)
            .collect::<Vec<_>>()
    );
    assert!(
        parsed
            .nodes
            .iter()
            .any(|n| n.external_id.as_deref() == Some("__event__kafka__user.updated")),
        "imported USER_UPDATED const must resolve to an event node"
    );
}

#[test]
fn dynamic_member_expression_emits_nothing() {
    let parsed = parse_fixture("dynamic_member_consumer.ts", &[Framework::NestJs]);
    assert!(
        parsed.nodes.iter().all(|n| !matches!(
            n.kind,
            gather_step_core::NodeKind::Topic | gather_step_core::NodeKind::Event
        )),
        "dynamic template literal topic must not produce topic/event nodes; got: {:?}",
        parsed
            .nodes
            .iter()
            .filter(|n| matches!(
                n.kind,
                gather_step_core::NodeKind::Topic | gather_step_core::NodeKind::Event
            ))
            .collect::<Vec<_>>()
    );
}

#[test]
fn literal_topic_strings_still_resolve_unchanged() {
    // Regression guard: existing literal-string decorator resolution must not break
    let parsed = parse_fixture("custom_event_pattern.ts", &[Framework::NestJs]);
    assert!(
        parsed
            .nodes
            .iter()
            .any(|n| n.external_id.as_deref() == Some("__event__kafka__order.created")),
        "literal @CustomEventPattern('order.created') must still emit an event node"
    );
}

// ---------------------------------------------------------------------------
// Payload-level consumer dispatch
// ---------------------------------------------------------------------------

#[test]
fn payload_dispatch_switch_emits_per_event_consumer_edges() {
    let parsed = parse_fixture("payload_dispatcher.ts", &[Framework::NestJs]);

    // Broad canonical event node (@MessagePattern('pdf.generation')) must still be present
    assert!(
        parsed
            .nodes
            .iter()
            .any(|n| n.external_id.as_deref() == Some("__event__kafka__pdf.generation")),
        "broad canonical event node must still exist alongside per-event nodes"
    );

    // Per-event node from switch case must be emitted
    assert!(
        parsed
            .nodes
            .iter()
            .any(|n| n.external_id.as_deref() == Some("__event__kafka__pdf.generation.completed")),
        "switch(event.eventType) case EventType.PdfGenerationCompleted must emit a per-event Event node; \
         nodes: {:?}",
        parsed
            .nodes
            .iter()
            .map(|n| &n.external_id)
            .collect::<Vec<_>>()
    );

    let event_id = parsed
        .nodes
        .iter()
        .find(|n| n.external_id.as_deref() == Some("__event__kafka__pdf.generation.completed"))
        .map(|n| n.id)
        .expect("per-event Event node must exist");

    assert!(
        parsed
            .edges
            .iter()
            .any(|e| e.kind == gather_step_core::EdgeKind::Consumes && e.target == event_id),
        "must have a Consumes edge pointing at the per-event Event node"
    );
    assert!(
        parsed
            .edges
            .iter()
            .any(|e| e.kind == gather_step_core::EdgeKind::UsesEventFrom && e.target == event_id),
        "must have a UsesEventFrom edge pointing at the per-event Event node"
    );
}

#[test]
fn unrelated_eventtype_switch_outside_consumer_emits_nothing() {
    // A method without a messaging decorator must not produce Event nodes even
    // when it contains an eventType switch.
    let parsed = parse_fixture("unrelated_event_type_switch.ts", &[Framework::NestJs]);
    assert!(
        parsed
            .nodes
            .iter()
            .all(|n| n.kind != gather_step_core::NodeKind::Event),
        "eventType switch outside a @MessagePattern handler must not produce Event nodes; got: {:?}",
        parsed
            .nodes
            .iter()
            .filter(|n| n.kind == gather_step_core::NodeKind::Event)
            .collect::<Vec<_>>()
    );
}

// ---------------------------------------------------------------------------
// Wrapper-Based Event Producers
// ---------------------------------------------------------------------------

#[test]
fn emit_with_payload_event_type_emits_fine_grained_event() {
    // PdfService.processCompleted calls `this.client.emit('pdf.generation', { eventType: EventType.PdfGenerationCompleted, ... })`.
    // Expected: both the broad Event node (pdf.generation) and a fine-grained
    // Event node (pdf.generation.completed) with Publishes + ProducesEventFor edges.
    let parsed = parse_fixture("emit_with_event_type.ts", &[Framework::NestJs]);

    let fine_qn = "__event__kafka__pdf.generation.completed";
    let has_fine_node = parsed
        .nodes
        .iter()
        .any(|n| n.external_id.as_deref() == Some(fine_qn));
    assert!(
        has_fine_node,
        "expected fine-grained Event node {fine_qn}; nodes: {:?}",
        parsed
            .nodes
            .iter()
            .map(|n| n.external_id.as_deref())
            .collect::<Vec<_>>()
    );

    let fine_id = parsed
        .nodes
        .iter()
        .find(|n| n.external_id.as_deref() == Some(fine_qn))
        .map(|n| n.id)
        .unwrap();

    assert!(
        parsed
            .edges
            .iter()
            .any(|e| e.target == fine_id && e.kind == gather_step_core::EdgeKind::Publishes),
        "expected Publishes edge to fine-grained Event node"
    );
    assert!(
        parsed
            .edges
            .iter()
            .any(|e| e.target == fine_id && e.kind == gather_step_core::EdgeKind::ProducesEventFor),
        "expected ProducesEventFor edge to fine-grained Event node"
    );

    // Broad event node must still exist (extra detail; does not replace)
    let broad_qn = "__event__kafka__pdf.generation";
    assert!(
        parsed
            .nodes
            .iter()
            .any(|n| n.external_id.as_deref() == Some(broad_qn)),
        "broad Event node {broad_qn} must still be present"
    );
}
