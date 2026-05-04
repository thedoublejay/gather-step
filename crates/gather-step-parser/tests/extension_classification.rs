//! Extension-classification regression tests.
//!
//! Verifies that `parse_ts_js_with_swc` routes `.mts`, `.cts`, and uppercase
//! variants (`.TS`, `.TSX`) through the TypeScript parser rather than the
//! JavaScript parser.
//!
//! Prior to the fix, the extension gate matched only lowercase `ts` and `tsx`
//! exactly, so these extensions fell through to `Syntax::Es(...)`.  A source
//! file containing type annotations would then either trigger a parse error or
//! produce an empty / recovered module, both of which this test detects.

/// Sources that are only valid as TypeScript (contain type annotations).
/// If the extension gate misroutes them as plain JS, the parser will
/// either fail to recover or produce an empty module, causing this test
/// to fail.
const TS_ONLY_SOURCE: &str = "export interface User { name: string; id: number; }\n\
     export function greet(u: User): string { return `hi ${u.name}`; }\n";

#[test]
fn mts_extension_parses_as_typescript() {
    let found =
        gather_step_parser::oxc_test_support::parse_ts_file_via_extension("mts", TS_ONLY_SOURCE);
    assert!(
        found,
        ".mts extension must route to the TypeScript parser; \
         type-annotated source produced an empty module — extension gate may still be wrong"
    );
}

#[test]
fn cts_extension_parses_as_typescript() {
    let found =
        gather_step_parser::oxc_test_support::parse_ts_file_via_extension("cts", TS_ONLY_SOURCE);
    assert!(
        found,
        ".cts extension must route to the TypeScript parser; \
         type-annotated source produced an empty module — extension gate may still be wrong"
    );
}

#[test]
fn uppercase_ts_extension_parses_as_typescript() {
    let found =
        gather_step_parser::oxc_test_support::parse_ts_file_via_extension("TS", TS_ONLY_SOURCE);
    assert!(
        found,
        ".TS extension must be treated case-insensitively and route to the TypeScript parser"
    );
}

#[test]
fn uppercase_tsx_extension_parses_as_typescript_with_jsx() {
    // This source requires both TypeScript (type assertion) and JSX parsing.
    const TSX_SOURCE: &str = "export const X: React.FC = () => <div>{(42 as number)}</div>;\n";
    let found =
        gather_step_parser::oxc_test_support::parse_ts_file_via_extension("TSX", TSX_SOURCE);
    assert!(
        found,
        ".TSX extension must be treated case-insensitively and enable tsx mode in the TypeScript parser"
    );
}
