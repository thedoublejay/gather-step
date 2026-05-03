//! Oxc parser adapter for TypeScript/JavaScript sources.
//!
//! This module owns Oxc-specific allocator/parser/span details. Callers should
//! consume only owned Gather Step data so Oxc AST lifetimes stay contained.

#![allow(
    dead_code,
    reason = "Oxc adapter helpers are staged before the visitor backend is switched on"
)]

use std::{ffi::OsStr, path::Path};

use gather_step_core::SourceSpan;
use oxc_allocator::Allocator;
use oxc_parser::{ParseOptions, Parser};
use oxc_span::{SourceType, Span};

use crate::{traverse::FileEntry, ts_js_backend::TsJsParseStatus};

#[cfg(any(test, feature = "test-support"))]
use {
    crate::resolve::ImportBinding,
    oxc_ast::ast::{
        ExportAllDeclaration, ExportNamedDeclaration, ImportDeclaration,
        ImportDeclarationSpecifier, ImportOrExportKind, ModuleExportName, Statement,
    },
};

pub(crate) fn parse_ts_js_for_status(file: &FileEntry, source: &str) -> TsJsParseStatus {
    let allocator = Allocator::default();
    let options = ParseOptions {
        allow_return_outside_function: true,
        ..ParseOptions::default()
    };
    let parsed = Parser::new(&allocator, source, source_type_for_path(&file.path))
        .with_options(options)
        .parse();
    if parsed.panicked {
        TsJsParseStatus::Unrecoverable
    } else if parsed.errors.is_empty() {
        TsJsParseStatus::Parsed
    } else {
        TsJsParseStatus::Recovered
    }
}

pub(crate) fn source_type_for_path(path: &Path) -> SourceType {
    match path.extension().and_then(OsStr::to_str).unwrap_or_default() {
        ext if ext.eq_ignore_ascii_case("ts")
            || ext.eq_ignore_ascii_case("mts")
            || ext.eq_ignore_ascii_case("cts") =>
        {
            SourceType::ts()
        }
        ext if ext.eq_ignore_ascii_case("tsx") => SourceType::tsx(),
        ext if ext.eq_ignore_ascii_case("cjs") => SourceType::cjs().with_jsx(true),
        ext if ext.eq_ignore_ascii_case("mjs") => SourceType::mjs().with_jsx(true),
        _ => SourceType::jsx(),
    }
}

pub(crate) fn line_offsets(source: &str) -> Vec<u32> {
    let mut offsets = vec![0_u32];
    for (idx, byte) in source.bytes().enumerate() {
        if byte == b'\n' {
            offsets.push(u32::try_from(idx + 1).unwrap_or(u32::MAX));
        }
    }
    offsets
}

pub(crate) fn span_to_source_span(span: Span, line_offsets: &[u32]) -> Option<SourceSpan> {
    if span.end < span.start || line_offsets.is_empty() {
        return None;
    }
    let (start_line, start_col) = line_col(span.start, line_offsets)?;
    let (end_line, end_col) = line_col(span.end, line_offsets)?;
    Some(SourceSpan {
        line_start: start_line,
        line_len: u16::try_from(end_line.saturating_sub(start_line)).unwrap_or(u16::MAX),
        column_start: u16::try_from(start_col).unwrap_or(u16::MAX),
        column_len: u16::try_from(if end_line == start_line {
            end_col.saturating_sub(start_col)
        } else {
            end_col
        })
        .unwrap_or(u16::MAX),
    })
}

fn line_col(offset: u32, line_offsets: &[u32]) -> Option<(u32, u32)> {
    let line_idx = line_offsets.partition_point(|line_start| *line_start <= offset);
    let line_idx = line_idx.checked_sub(1)?;
    let line_start = *line_offsets.get(line_idx)?;
    Some((
        u32::try_from(line_idx + 1).unwrap_or(u32::MAX),
        offset.saturating_sub(line_start),
    ))
}

#[cfg(any(test, feature = "test-support"))]
fn parse_import_bindings(file: &FileEntry, source: &str) -> Vec<ImportBinding> {
    let allocator = Allocator::default();
    let options = ParseOptions {
        allow_return_outside_function: true,
        ..ParseOptions::default()
    };
    let parsed = Parser::new(&allocator, source, source_type_for_path(&file.path))
        .with_options(options)
        .parse();
    if parsed.panicked {
        return Vec::new();
    }

    let mut bindings = Vec::new();
    for statement in &parsed.program.body {
        match statement {
            Statement::ImportDeclaration(decl) => {
                bindings.extend(import_bindings_from_decl(decl));
            }
            Statement::ExportNamedDeclaration(decl) => {
                bindings.extend(import_bindings_from_named_export(decl));
            }
            Statement::ExportAllDeclaration(decl) => {
                bindings.push(import_binding_from_export_all(decl));
            }
            _ => {}
        }
    }
    bindings
}

#[cfg(any(test, feature = "test-support"))]
fn import_bindings_from_decl(decl: &ImportDeclaration<'_>) -> Vec<ImportBinding> {
    let source = decl.source.value.to_string();
    let is_type_only = decl.import_kind == ImportOrExportKind::Type;
    decl.specifiers
        .as_ref()
        .into_iter()
        .flatten()
        .map(|specifier| match specifier {
            ImportDeclarationSpecifier::ImportSpecifier(specifier) => ImportBinding {
                local_name: specifier.local.name.to_string(),
                imported_name: Some(module_export_name(&specifier.imported)),
                source: source.clone(),
                resolved_path: None,
                is_default: false,
                is_namespace: false,
                is_type_only: is_type_only || specifier.import_kind == ImportOrExportKind::Type,
            },
            ImportDeclarationSpecifier::ImportDefaultSpecifier(specifier) => ImportBinding {
                local_name: specifier.local.name.to_string(),
                imported_name: None,
                source: source.clone(),
                resolved_path: None,
                is_default: true,
                is_namespace: false,
                is_type_only,
            },
            ImportDeclarationSpecifier::ImportNamespaceSpecifier(specifier) => ImportBinding {
                local_name: specifier.local.name.to_string(),
                imported_name: None,
                source: source.clone(),
                resolved_path: None,
                is_default: false,
                is_namespace: true,
                is_type_only,
            },
        })
        .collect()
}

#[cfg(any(test, feature = "test-support"))]
fn import_bindings_from_named_export(decl: &ExportNamedDeclaration<'_>) -> Vec<ImportBinding> {
    let Some(source) = decl.source.as_ref().map(|source| source.value.to_string()) else {
        return Vec::new();
    };
    let is_type_only = decl.export_kind == ImportOrExportKind::Type;
    decl.specifiers
        .iter()
        .map(|specifier| {
            let local_name = module_export_name(&specifier.exported);
            let imported_name = module_export_name(&specifier.local);
            ImportBinding {
                local_name,
                imported_name: Some(imported_name),
                source: source.clone(),
                resolved_path: None,
                is_default: false,
                is_namespace: false,
                is_type_only: is_type_only || specifier.export_kind == ImportOrExportKind::Type,
            }
        })
        .collect()
}

#[cfg(any(test, feature = "test-support"))]
fn import_binding_from_export_all(decl: &ExportAllDeclaration<'_>) -> ImportBinding {
    ImportBinding {
        local_name: decl
            .exported
            .as_ref()
            .map_or_else(|| "*".to_owned(), module_export_name),
        imported_name: Some("*".to_owned()),
        source: decl.source.value.to_string(),
        resolved_path: None,
        is_default: false,
        is_namespace: true,
        is_type_only: decl.export_kind == ImportOrExportKind::Type,
    }
}

#[cfg(any(test, feature = "test-support"))]
fn module_export_name(name: &ModuleExportName<'_>) -> String {
    match name {
        ModuleExportName::IdentifierName(name) => name.name.to_string(),
        ModuleExportName::IdentifierReference(name) => name.name.to_string(),
        ModuleExportName::StringLiteral(value) => value.value.to_string(),
    }
}

#[cfg(any(test, feature = "test-support"))]
pub mod oxc_test_support {
    use std::path::{Path, PathBuf};

    use crate::{
        resolve::ImportBinding,
        traverse::{FileEntry, Language},
    };

    pub fn parse_recovery_status_for_path(path: &Path, source: &str) -> &'static str {
        let file = FileEntry {
            path: path.to_path_buf(),
            language: Language::TypeScript,
            size_bytes: source.len() as u64,
            content_hash: [0u8; 32],
            source_bytes: None,
        };
        super::parse_ts_js_for_status(&file, source).as_str()
    }

    pub fn parse_recovery_status_for_extension(ext: &str, source: &str) -> &'static str {
        let path = PathBuf::from(format!("status.{ext}"));
        parse_recovery_status_for_path(&path, source)
    }

    pub fn parse_import_bindings_for_path(path: &Path, source: &str) -> Vec<ImportBinding> {
        let file = FileEntry {
            path: path.to_path_buf(),
            language: Language::TypeScript,
            size_bytes: source.len() as u64,
            content_hash: [0u8; 32],
            source_bytes: None,
        };
        super::parse_import_bindings(&file, source)
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use oxc_span::Span;
    use pretty_assertions::assert_eq;

    use crate::{FileEntry, Language, ts_js_backend::TsJsParseStatus};

    use super::{line_offsets, parse_ts_js_for_status, source_type_for_path, span_to_source_span};

    fn file(path: &str) -> FileEntry {
        FileEntry {
            path: PathBuf::from(path),
            language: Language::TypeScript,
            size_bytes: 0,
            content_hash: [0; 32],
            source_bytes: None,
        }
    }

    #[test]
    fn source_type_matches_ts_js_extensions() {
        assert!(source_type_for_path(Path::new("component.ts")).is_typescript());
        assert!(!source_type_for_path(Path::new("component.ts")).is_jsx());
        assert!(source_type_for_path(Path::new("component.tsx")).is_typescript());
        assert!(source_type_for_path(Path::new("component.tsx")).is_jsx());
        assert!(source_type_for_path(Path::new("component.jsx")).is_jsx());
        assert!(source_type_for_path(Path::new("component.js")).is_jsx());
        assert!(source_type_for_path(Path::new("component.cjs")).is_commonjs());
    }

    #[test]
    fn oxc_parses_jsx_in_js_source() {
        let status = parse_ts_js_for_status(&file("component.js"), "export const view = <div />;");
        assert_eq!(status, TsJsParseStatus::Parsed);
    }

    #[test]
    fn oxc_span_maps_to_source_span() {
        let offsets = line_offsets("a\nbc\ndef");
        let span = span_to_source_span(Span::new(2, 4), &offsets).expect("span should map");
        assert_eq!(span.line_start, 2);
        assert_eq!(span.line_len, 0);
        assert_eq!(span.column_start, 0);
        assert_eq!(span.column_len, 2);
    }
}
