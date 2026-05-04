//! Parser regression tests covering the release-safety defect set so they
//! cannot silently regress.

use rayon::ThreadPoolBuilder;
use rayon::prelude::*;

/// Stress-exercise the full `parse_ts_js_with_swc` → `visit_module` path
/// under rayon.  A regression that moved `visit_module` back outside the outer
/// `GLOBALS.set` scope — the R2#1 concern — would panic or produce inconsistent
/// `ParseState` symbols.
///
/// Unlike `swc_parser_survives_rayon_parallel_calls` (which only drives
/// `try_parse` via `parse_source_contains_ident`), this test drives the
/// complete pipeline including `visit_module`, and asserts per-source symbol
/// identity on the extracted `ParseState`.
///
/// The marker checked is the function name `run_N` (which `visit_module` /
/// `visit_fn_decl` captures as a `SymbolCapture`) rather than the const
/// variable name (which is not captured as a `SymbolCapture` by design since
/// simple value declarations are not code-intelligence symbols).
#[test]
fn parse_ts_js_with_swc_survives_rayon_parallel_calls() {
    const SOURCES: usize = 256;
    const ROUNDS: usize = 4;

    let sources: Vec<(usize, String)> = (0..SOURCES)
        .map(|i| {
            (
                i,
                format!(
                    "export const value_{i}: number = {i};\n\
                     export function run_{i}(): void {{ console.log({i}); }}\n"
                ),
            )
        })
        .collect();

    let pool = ThreadPoolBuilder::new()
        .num_threads(8)
        .build()
        .expect("rayon pool builds");

    for round in 0..ROUNDS {
        pool.install(|| {
            sources.par_iter().for_each(|(i, source)| {
                // `run_N` is a named function export — visit_module captures
                // it as a SymbolCapture (unlike const numeric initialisers,
                // which are not code-intelligence symbols).
                let marker = format!("run_{i}");
                let found =
                    gather_step_parser::oxc_test_support::parse_full_pipeline_contains_symbol(
                        "ts", source, &marker,
                    );
                assert!(
                    found,
                    "round {round} source {i}: full pipeline missing symbol {marker}; \
                     possible span cross-talk after visit_module or GLOBALS.set regression"
                );
            });
        });
    }
}

/// Stress-exercise the SWC helpers from many rayon threads with a bounded
/// thread pool.  Asserts both "every parse produced a module" AND "the
/// module's content identity matches the source's expected marker", so that
/// span cross-talk (where thread A parses thread B's content) would fail
/// the assertion instead of passing silently.
#[test]
fn swc_parser_survives_rayon_parallel_calls() {
    const SOURCES: usize = 256;
    const ROUNDS: usize = 4;

    let sources: Vec<(usize, String)> = (0..SOURCES)
        .map(|i| {
            (
                i,
                format!(
                    "export const value_{i}: number = {i};\n\
                     export class Thing{i} {{ run(): void {{ console.log({i}); }} }}\n"
                ),
            )
        })
        .collect();

    let pool = ThreadPoolBuilder::new()
        .num_threads(8)
        .build()
        .expect("rayon thread pool builds");

    for round in 0..ROUNDS {
        pool.install(|| {
            sources.par_iter().for_each(|(i, source)| {
                // Use a `const` ident as the marker. This test exercises span cross-talk
                // at the raw SWC parse level via `parse_source_contains_ident`, NOT through
                // `visit_module`. The companion test
                // `parse_ts_js_with_swc_survives_rayon_parallel_calls` covers the full
                // `visit_module` pipeline with function-name markers.
                let marker = format!("value_{i}");
                let ok = gather_step_parser::oxc_test_support::parse_source_contains_ident(
                    source, &marker,
                );
                assert!(
                    ok,
                    "round {round} source {i}: expected parsed module to contain ident {marker}; \
                     possible span cross-talk or panic in rayon worker"
                );
            });
        });
    }
}
