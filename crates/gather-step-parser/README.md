# gather-step-parser

AST parsing, traversal, and semantic augmentation for Gather Step.

This crate is where raw repository files become structured graph candidates. It
combines:

- repo traversal and file classification
- tree-sitter parsing across supported languages
- import and call resolution helpers
- framework and pack-specific semantic augmentation

## What This Crate Provides

- `collect_repo_files`: repo traversal with language filtering, exclusion rules, and content hashing
- `parse_file*` entry points: file parsing with syntax extraction and optional framework/pack context
- `ParsedFile`: parsed nodes, edges, symbols, call sites, imports, and constant strings for one file
- `resolve_calls*`: post-parse call-target resolution helpers
- framework detection, local profile config, and pack registry support
- `PathAliases`: repo-level `tsconfig.json` path alias loading for import resolution

## When To Use It

Use `gather-step-parser` when you need to:

- walk a repo and identify supported source files
- extract file/module/type/function/class/import/call structure from source code
- enrich parsed output with framework-aware semantics
- resolve imports and likely call targets across parsed symbols
- prepare graph-shaped data before persistence

If you only need schema, IDs, and graph data types, use `gather-step-core`.

## Minimal Example

```rust
use std::path::Path;

use gather_step_parser::{
    TraverseConfig, collect_repo_files, parse_file_with_context,
    frameworks::detect::detect_frameworks, tsconfig::PathAliases,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let repo_root = Path::new(".");
    let repo_name = "example_repo";

    let frameworks = detect_frameworks(repo_root).into_iter().collect::<Vec<_>>();
    let aliases = PathAliases::from_repo_root(repo_root);
    let config = TraverseConfig::new(vec![], vec![], false, 1_048_576);
    let summary = collect_repo_files(repo_root, &config)?;

    if let Some(file) = summary.files.first() {
        let parsed = parse_file_with_context(repo_name, repo_root, file, &frameworks, &aliases)?;
        let _nodes = parsed.nodes;
        let _edges = parsed.edges;
    }

    Ok(())
}
```

Most real indexing paths detect frameworks and path aliases once per repo, then
parse many files with that shared context.

## Public Surface

### Traversal

`collect_repo_files` is the entry point for repo scanning.

Key operations:

- walk a repo with `.gitignore` support
- classify supported languages
- skip binary, oversized, unsupported, and excluded files
- compute per-file content hashes
- report traversal summary counters

### Parsing

`parse_file`, `parse_file_with_frameworks`, `parse_file_with_context`, and
`parse_file_with_packs` produce a `ParsedFile`.

Key operations:

- parse TypeScript, JavaScript, Python, Rust, Go, and Java source files
- emit graph candidates such as file, module, symbol, and relationship data
- capture imports, call sites, decorators, and constant strings
- apply pack-aware semantic augmentation on top of syntax extraction
- record per-file parse timing

### Resolution

`resolve_calls` and `resolve_calls_with_unresolved` provide lightweight
post-parse call resolution.

Key operations:

- map import bindings to likely local targets
- resolve direct and qualified call sites
- distinguish resolution strategy and unresolved cases
- support repo-relative alias rewriting via `PathAliases`

### Frameworks And Packs

The `frameworks` module provides repo-level detection plus pack orchestration.

Key operations:

- detect active frameworks from repo manifests and config files
- convert framework detection into pack activation
- load local profile overrides from `.gather-step.local.yaml`
- run augmentation groups once even when multiple packs share logic

## Architecture Notes

This crate intentionally separates parser responsibilities:

- traversal decides which files are worth parsing
- tree-sitter extracts language-level structure
- framework packs add semantic edges and nodes only when relevant
- resolution runs after parsing so import/call heuristics stay isolated

The output is still derived data. This crate prepares graph-shaped records but
does not persist them.

## Important Invariants

- File identity and graph node shapes come from `gather-step-core`.
- Traversal returns repo-relative paths and deterministic content hashes.
- Framework detection is best-effort and should never hard-fail indexing.
- Pack augmentation is additive and grouped to avoid duplicate work.
- Path alias rewriting stays repo-relative and ignores targets outside the repo.
- Parsing may be partial by language: some languages are traversed and classified before deeper extraction is added.

## Failure Model

This crate does not attempt to prove full semantic correctness.

Instead:

- traversal is conservative and skips files that are unsafe or out of scope
- parsing returns structured best-effort output from supported grammars
- framework detection falls back to no active packs on missing or malformed manifests
- local config loading falls back to auto-detection when config is absent or invalid
- call resolution exposes unresolved results instead of inventing certainty

That is a deliberate design choice for a fast local indexing pipeline.

## Module Map

- `traverse`: repo walking, language classification, and file hashing
- `tree_sitter`: parsing entry points and core extraction logic
- `resolve`: import and call-target resolution
- `frameworks`: detection, profiles, local config, registry, and augmentations
- `tsconfig`: `tsconfig.json` path alias parsing
- `lib.rs`: public exports and crate entry points

## Testing

The crate is covered by unit tests across traversal, parsing, framework
augmentation, config loading, and resolution logic.

Typical verification:

```bash
cargo test -p gather-step-parser
```

For workspace-level validation, run the full `gather-step` test suite.

## See Also

- `crates/gather-step-core`: schema, graph types, IDs, config, and registry
- `crates/gather-step-storage`: persistence and indexing coordination
- `OVERVIEW/`: plain-language project overview materials
- `COMPLETED/`: implementation notes and completion records
