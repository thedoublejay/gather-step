//! Oxc-based TypeScript/JavaScript visitor.
//!
//! Production parser path for TS/JS sources. Mirrors the data the previous
//! SWC visitor wrote into [`ParseState`] — same `NodeId`s, same edges, same
//! call-site / decorator / constant-string semantics — so downstream
//! consumers (storage, framework augmenters, snapshot tests) need no changes.
//!
//! All Oxc-specific types (`Allocator`, `Span`, AST nodes) stay inside this
//! module. Public surfaces consume only owned, span-free Gather Step data.

use std::{ffi::OsStr, path::Path};

use gather_step_core::{NodeData, NodeKind, SourceSpan, Visibility};
use oxc_allocator::Allocator;
#[cfg(feature = "test-support")]
use oxc_ast::ast::TSModuleDeclarationName;
use oxc_ast::ast::{
    Argument, ArrayAssignmentTarget, ArrowFunctionExpression, AssignmentTarget,
    AssignmentTargetMaybeDefault, AssignmentTargetProperty, BindingPattern, CallExpression,
    ChainElement, Class, ClassElement, Declaration, Decorator, ExportAllDeclaration,
    ExportDefaultDeclaration, ExportDefaultDeclarationKind, ExportNamedDeclaration, Expression,
    ForStatementInit, ForStatementLeft, Function, FunctionBody, ImportDeclaration,
    ImportDeclarationSpecifier, ImportOrExportKind, JSXAttributeItem, JSXAttributeValue, JSXChild,
    JSXElement, JSXExpression, MemberExpression, MethodDefinition, MethodDefinitionKind,
    ModuleExportName, NewExpression, ObjectAssignmentTarget, ObjectExpression, ObjectPropertyKind,
    PropertyKey, PropertyKind, SimpleAssignmentTarget, Statement, TSAccessibility,
    TSEnumMemberName, TSImportEqualsDeclaration, TSModuleDeclarationBody, TSTypeName,
    VariableDeclaration, VariableDeclarator,
};
use oxc_parser::{ParseOptions, Parser};
use oxc_span::{GetSpan, SourceType, Span};

#[cfg(feature = "test-support")]
use crate::ts_js_backend::TsJsParseStatus;
use crate::{
    resolve::ImportBinding,
    traverse::FileEntry,
    tree_sitter::{DecoratorCapture, ParseState},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OxcParseStatus {
    Parsed,
    Recovered,
    Unrecoverable,
}

const MAX_DEPTH: usize = 256;

// ── Public entry point ───────────────────────────────────────────────────────

/// Parse a TypeScript or JavaScript file with Oxc and populate `state`.
///
/// Always succeeds: when Oxc panics on the source we emit no symbols and
/// return `Unrecoverable`, mirroring the previous SWC visitor's behaviour of
/// always producing a (possibly empty) partial result.
pub(crate) fn parse_ts_js_with_oxc_with_status(
    file: &FileEntry,
    state: &mut ParseState<'_>,
    source: &str,
    _absolute_path: &Path,
) -> OxcParseStatus {
    let allocator = Allocator::default();
    let options = ParseOptions {
        allow_return_outside_function: true,
        ..ParseOptions::default()
    };
    let parsed = Parser::new(&allocator, source, source_type_for_path(&file.path))
        .with_options(options)
        .parse();

    let status = if parsed.panicked {
        OxcParseStatus::Unrecoverable
    } else if parsed.errors.is_empty() {
        OxcParseStatus::Parsed
    } else {
        OxcParseStatus::Recovered
    };

    if status == OxcParseStatus::Unrecoverable {
        return status;
    }

    let offsets = build_line_offsets(source);
    let mut ctx = VisitCtx::new(source, &offsets);
    for stmt in &parsed.program.body {
        visit_top_level_statement(stmt, state, &mut ctx);
    }

    status
}

#[cfg(feature = "test-support")]
pub fn parse_ts_js_with_oxc(
    file: &FileEntry,
    state: &mut ParseState<'_>,
    source: &str,
    absolute_path: &Path,
) {
    let _ = parse_ts_js_with_oxc_with_status(file, state, source, absolute_path);
}

// ── Source-type / span helpers ───────────────────────────────────────────────

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

fn build_line_offsets(source: &str) -> Vec<u32> {
    let mut offsets = vec![0_u32];
    for (idx, byte) in source.bytes().enumerate() {
        if byte == b'\n' {
            offsets.push(u32::try_from(idx + 1).unwrap_or(u32::MAX));
        }
    }
    offsets
}

#[cfg(all(test, feature = "test-support"))]
pub(crate) fn line_offsets(source: &str) -> Vec<u32> {
    build_line_offsets(source)
}

/// Convert an Oxc [`Span`] to the absolute-coordinate [`SourceSpan`] used by
/// the rest of Gather Step. `Span` start/end are raw byte offsets in the
/// source string (no SWC-style 1-bias) so the math is straightforward.
fn span_from_oxc(span: Span, offsets: &[u32]) -> SourceSpan {
    let (line_start, column_start) = byte_to_line_col(span.start, offsets);
    let (line_end, column_end) = byte_to_line_col(span.end, offsets);
    SourceSpan::from_absolute(line_start, line_end, column_start, column_end)
}

fn byte_to_line_col(offset: u32, offsets: &[u32]) -> (u32, u32) {
    let idx = offsets
        .partition_point(|&line_start| line_start <= offset)
        .saturating_sub(1);
    let line_start = *offsets.get(idx).unwrap_or(&0);
    let line = u32::try_from(idx + 1).unwrap_or(u32::MAX);
    (line, offset.saturating_sub(line_start))
}

/// Optional helper retained for the test-only span parity helper.
#[cfg(all(test, feature = "test-support"))]
pub(crate) fn span_to_source_span(span: Span, offsets: &[u32]) -> Option<SourceSpan> {
    if span.end < span.start || offsets.is_empty() {
        return None;
    }
    Some(span_from_oxc(span, offsets))
}

fn source_slice(source: &str, span: Span) -> &str {
    let lo = (span.start as usize).min(source.len());
    let hi = (span.end as usize).min(source.len());
    if hi < lo {
        &source[..0]
    } else {
        &source[lo..hi]
    }
}

// ── Visitor context ──────────────────────────────────────────────────────────

struct VisitCtx<'a> {
    source: &'a str,
    offsets: &'a [u32],
    parent_class: Option<NodeData>,
    class_decl_depth: usize,
    owner: Option<gather_step_core::NodeId>,
    force_exported: bool,
    class_decorators: Vec<DecoratorCapture>,
    depth: usize,
}

impl<'a> VisitCtx<'a> {
    fn new(source: &'a str, offsets: &'a [u32]) -> Self {
        Self {
            source,
            offsets,
            parent_class: None,
            class_decl_depth: 0,
            owner: None,
            force_exported: false,
            class_decorators: Vec::new(),
            depth: 0,
        }
    }

    fn span(&self, span: Span) -> SourceSpan {
        span_from_oxc(span, self.offsets)
    }

    fn child_with_owner(&self, owner: gather_step_core::NodeId) -> Self {
        Self {
            source: self.source,
            offsets: self.offsets,
            parent_class: self.parent_class.clone(),
            class_decl_depth: self.class_decl_depth,
            owner: Some(owner),
            force_exported: false,
            class_decorators: self.class_decorators.clone(),
            depth: self.depth + 1,
        }
    }

    fn child_with_class(
        &self,
        class_node: &NodeData,
        class_decorators: Vec<DecoratorCapture>,
    ) -> Self {
        Self {
            source: self.source,
            offsets: self.offsets,
            parent_class: Some(class_node.clone()),
            class_decl_depth: self.class_decl_depth.saturating_add(1),
            owner: Some(class_node.id),
            force_exported: self.force_exported,
            class_decorators,
            depth: self.depth + 1,
        }
    }

    fn exported_child(&self) -> Self {
        Self {
            source: self.source,
            offsets: self.offsets,
            parent_class: self.parent_class.clone(),
            class_decl_depth: self.class_decl_depth,
            owner: self.owner,
            force_exported: true,
            class_decorators: self.class_decorators.clone(),
            depth: self.depth + 1,
        }
    }

    fn child_no_export(&self) -> Self {
        Self {
            source: self.source,
            offsets: self.offsets,
            parent_class: self.parent_class.clone(),
            class_decl_depth: self.class_decl_depth,
            owner: self.owner,
            force_exported: false,
            class_decorators: self.class_decorators.clone(),
            depth: self.depth + 1,
        }
    }
}

// ── Top-level dispatch ───────────────────────────────────────────────────────

fn visit_top_level_statement(
    stmt: &Statement<'_>,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    if ctx.depth > MAX_DEPTH {
        return;
    }

    match stmt {
        Statement::ImportDeclaration(decl) => visit_import_declaration(decl, state, ctx),
        Statement::ExportAllDeclaration(decl) => visit_export_all_declaration(decl, state, ctx),
        Statement::ExportNamedDeclaration(decl) => visit_export_named_declaration(decl, state, ctx),
        Statement::ExportDefaultDeclaration(decl) => {
            visit_export_default_declaration(decl, state, ctx);
        }
        Statement::TSExportAssignment(_) | Statement::TSNamespaceExportDeclaration(_) => {}
        // Declarations and regular statements
        _ => visit_statement(stmt, state, ctx),
    }
}

fn visit_statement(stmt: &Statement<'_>, state: &mut ParseState<'_>, ctx: &mut VisitCtx<'_>) {
    if ctx.depth > MAX_DEPTH {
        return;
    }

    match stmt {
        Statement::FunctionDeclaration(func) => {
            let exported = ctx.force_exported;
            visit_function_declaration(func, &Vec::new(), exported, state, ctx);
        }
        Statement::ClassDeclaration(class) => {
            let exported = ctx.force_exported;
            if ctx.class_decl_depth > 0 {
                visit_nested_class_body(class, state, ctx);
            } else {
                visit_class_declaration(class, exported, state, ctx);
            }
        }
        Statement::VariableDeclaration(var) => visit_variable_declaration(var, state, ctx),
        Statement::TSTypeAliasDeclaration(decl) => {
            push_type_symbol(
                decl.id.name.as_str(),
                decl.span,
                source_slice(ctx.source, decl.span).to_owned(),
                state,
                ctx,
            );
        }
        Statement::TSInterfaceDeclaration(decl) => {
            push_type_symbol(
                decl.id.name.as_str(),
                decl.span,
                source_slice(ctx.source, decl.span).to_owned(),
                state,
                ctx,
            );
        }
        Statement::TSEnumDeclaration(decl) => {
            push_type_symbol(
                decl.id.name.as_str(),
                decl.span,
                source_slice(ctx.source, decl.span).to_owned(),
                state,
                ctx,
            );
            // Enum members can carry initializer expressions with calls.
            for member in &decl.body.members {
                if let Some(init) = member.initializer.as_ref() {
                    visit_expression(init, state, ctx);
                }
                if let TSEnumMemberName::ComputedString(_)
                | TSEnumMemberName::ComputedTemplateString(_) = &member.id
                {
                    // computed enum keys can hold calls; skip — Oxc does not
                    // expose the inner expression directly here.
                }
            }
        }
        Statement::TSModuleDeclaration(decl) => {
            if ctx.depth >= MAX_DEPTH {
                return;
            }
            if let Some(body) = decl.body.as_ref() {
                visit_ts_module_body(body, state, ctx);
            }
        }
        Statement::TSImportEqualsDeclaration(decl) => visit_import_equals(decl, state, ctx),
        Statement::TSGlobalDeclaration(decl) => {
            for inner in &decl.body.body {
                visit_top_level_statement(inner, state, ctx);
            }
        }
        Statement::BlockStatement(block) => {
            for inner in &block.body {
                visit_statement(inner, state, ctx);
            }
        }
        Statement::IfStatement(s) => {
            visit_expression(&s.test, state, ctx);
            visit_statement(&s.consequent, state, ctx);
            if let Some(alt) = &s.alternate {
                visit_statement(alt, state, ctx);
            }
        }
        Statement::WhileStatement(s) => {
            visit_expression(&s.test, state, ctx);
            visit_statement(&s.body, state, ctx);
        }
        Statement::DoWhileStatement(s) => {
            visit_expression(&s.test, state, ctx);
            visit_statement(&s.body, state, ctx);
        }
        Statement::ForStatement(s) => {
            if let Some(init) = &s.init {
                match init {
                    ForStatementInit::VariableDeclaration(var) => {
                        for declarator in &var.declarations {
                            visit_variable_declarator(declarator, state, ctx);
                        }
                    }
                    other => {
                        if let Some(expr) = other.as_expression() {
                            visit_expression(expr, state, ctx);
                        }
                    }
                }
            }
            if let Some(test) = &s.test {
                visit_expression(test, state, ctx);
            }
            if let Some(update) = &s.update {
                visit_expression(update, state, ctx);
            }
            visit_statement(&s.body, state, ctx);
        }
        Statement::ForInStatement(s) => {
            visit_for_left(&s.left, state, ctx);
            visit_expression(&s.right, state, ctx);
            visit_statement(&s.body, state, ctx);
        }
        Statement::ForOfStatement(s) => {
            visit_for_left(&s.left, state, ctx);
            visit_expression(&s.right, state, ctx);
            visit_statement(&s.body, state, ctx);
        }
        Statement::SwitchStatement(s) => {
            visit_expression(&s.discriminant, state, ctx);
            for case in &s.cases {
                if let Some(test) = &case.test {
                    visit_expression(test, state, ctx);
                }
                for inner in &case.consequent {
                    visit_statement(inner, state, ctx);
                }
            }
        }
        Statement::TryStatement(s) => {
            for inner in &s.block.body {
                visit_statement(inner, state, ctx);
            }
            if let Some(handler) = &s.handler {
                for inner in &handler.body.body {
                    visit_statement(inner, state, ctx);
                }
            }
            if let Some(finalizer) = &s.finalizer {
                for inner in &finalizer.body {
                    visit_statement(inner, state, ctx);
                }
            }
        }
        Statement::ReturnStatement(s) => {
            if let Some(arg) = &s.argument {
                visit_expression(arg, state, ctx);
            }
        }
        Statement::ThrowStatement(s) => visit_expression(&s.argument, state, ctx),
        Statement::LabeledStatement(s) => visit_statement(&s.body, state, ctx),
        Statement::ExpressionStatement(s) => visit_expression(&s.expression, state, ctx),
        // Module-level constructs that surface inside non-top-level scopes get
        // re-dispatched for their import/export side effects.
        Statement::ImportDeclaration(decl) => visit_import_declaration(decl, state, ctx),
        Statement::ExportAllDeclaration(decl) => visit_export_all_declaration(decl, state, ctx),
        Statement::ExportNamedDeclaration(decl) => visit_export_named_declaration(decl, state, ctx),
        Statement::ExportDefaultDeclaration(decl) => {
            visit_export_default_declaration(decl, state, ctx);
        }
        // Plain statements without sub-expressions of interest.
        _ => {}
    }
}

fn visit_for_left(left: &ForStatementLeft<'_>, state: &mut ParseState<'_>, ctx: &mut VisitCtx<'_>) {
    if let ForStatementLeft::VariableDeclaration(var) = left {
        for declarator in &var.declarations {
            visit_variable_declarator(declarator, state, ctx);
        }
    }
}

// ── Module declarations ──────────────────────────────────────────────────────

fn visit_import_declaration(
    decl: &ImportDeclaration<'_>,
    state: &mut ParseState<'_>,
    ctx: &VisitCtx<'_>,
) {
    let source_specifier = decl.source.value.to_string();
    let bindings = import_bindings_from_decl(decl);
    let stmt_span = ctx.span(decl.span);
    push_imports(state, &source_specifier, bindings, &stmt_span);
}

fn visit_export_all_declaration(
    decl: &ExportAllDeclaration<'_>,
    state: &mut ParseState<'_>,
    ctx: &VisitCtx<'_>,
) {
    let source_specifier = decl.source.value.to_string();
    let stmt_span = ctx.span(decl.span);
    let local_name = decl
        .exported
        .as_ref()
        .map_or_else(|| "*".to_owned(), module_export_name);
    push_imports(
        state,
        &source_specifier,
        vec![ImportBinding {
            local_name,
            imported_name: Some("*".to_owned()),
            source: source_specifier.clone(),
            resolved_path: None,
            is_default: false,
            is_namespace: true,
            is_type_only: decl.export_kind == ImportOrExportKind::Type,
        }],
        &stmt_span,
    );
}

fn visit_export_named_declaration(
    decl: &ExportNamedDeclaration<'_>,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    if let Some(source) = &decl.source {
        let source_specifier = source.value.to_string();
        let bindings = import_bindings_from_named_export(decl);
        let stmt_span = ctx.span(decl.span);
        push_imports(state, &source_specifier, bindings, &stmt_span);
        return;
    }

    if let Some(declaration) = decl.declaration.as_ref() {
        let mut child_ctx = ctx.exported_child();
        visit_inline_declaration(declaration, state, &mut child_ctx);
    }
}

fn visit_export_default_declaration(
    decl: &ExportDefaultDeclaration<'_>,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    match &decl.declaration {
        ExportDefaultDeclarationKind::FunctionDeclaration(func) => {
            visit_function_declaration_default(func, state, ctx);
        }
        ExportDefaultDeclarationKind::ClassDeclaration(class) => {
            visit_class_declaration_default(class, state, ctx);
        }
        ExportDefaultDeclarationKind::TSInterfaceDeclaration(iface) => {
            push_type_symbol(
                iface.id.name.as_str(),
                iface.span,
                source_slice(ctx.source, iface.span).to_owned(),
                state,
                ctx,
            );
        }
        other => {
            if let Some(expr) = other.as_expression() {
                visit_export_default_expression(expr, state, ctx);
            }
        }
    }
}

fn visit_export_default_expression(
    expr: &Expression<'_>,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    match expr {
        Expression::Identifier(ident) => {
            let alias = ident.name.to_string();
            mirror_constant_prefix(state.constant_strings_mut(), &alias, "default");
        }
        Expression::ObjectExpression(obj) => {
            let mut constants = Vec::new();
            extract_object_constants("default", obj, &mut constants);
            for (k, v) in constants {
                state.record_constant_string(k, v);
            }
        }
        Expression::StringLiteral(s) => {
            state.record_constant_string("default".to_owned(), s.value.to_string());
        }
        _ => {}
    }
    visit_expression(expr, state, ctx);
}

fn visit_inline_declaration(
    declaration: &Declaration<'_>,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    match declaration {
        Declaration::FunctionDeclaration(func) => {
            visit_function_declaration(func, &Vec::new(), ctx.force_exported, state, ctx);
        }
        Declaration::ClassDeclaration(class) => {
            if ctx.class_decl_depth > 0 {
                visit_nested_class_body(class, state, ctx);
            } else {
                visit_class_declaration(class, ctx.force_exported, state, ctx);
            }
        }
        Declaration::VariableDeclaration(var) => visit_variable_declaration(var, state, ctx),
        Declaration::TSTypeAliasDeclaration(decl) => {
            push_type_symbol(
                decl.id.name.as_str(),
                decl.span,
                source_slice(ctx.source, decl.span).to_owned(),
                state,
                ctx,
            );
        }
        Declaration::TSInterfaceDeclaration(decl) => {
            push_type_symbol(
                decl.id.name.as_str(),
                decl.span,
                source_slice(ctx.source, decl.span).to_owned(),
                state,
                ctx,
            );
        }
        Declaration::TSEnumDeclaration(decl) => {
            push_type_symbol(
                decl.id.name.as_str(),
                decl.span,
                source_slice(ctx.source, decl.span).to_owned(),
                state,
                ctx,
            );
            for member in &decl.body.members {
                if let Some(init) = member.initializer.as_ref() {
                    visit_expression(init, state, ctx);
                }
            }
        }
        Declaration::TSModuleDeclaration(decl) => {
            if let Some(body) = decl.body.as_ref() {
                visit_ts_module_body(body, state, ctx);
            }
        }
        Declaration::TSImportEqualsDeclaration(decl) => visit_import_equals(decl, state, ctx),
        Declaration::TSGlobalDeclaration(decl) => {
            for stmt in &decl.body.body {
                visit_top_level_statement(stmt, state, ctx);
            }
        }
    }
}

fn visit_ts_module_body(
    body: &TSModuleDeclarationBody<'_>,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    if ctx.depth >= MAX_DEPTH {
        return;
    }
    match body {
        TSModuleDeclarationBody::TSModuleBlock(block) => {
            let mut child_ctx = ctx.child_no_export();
            for stmt in &block.body {
                visit_top_level_statement(stmt, state, &mut child_ctx);
            }
        }
        TSModuleDeclarationBody::TSModuleDeclaration(nested) => {
            if let Some(inner_body) = nested.body.as_ref() {
                let mut child_ctx = ctx.child_no_export();
                visit_ts_module_body(inner_body, state, &mut child_ctx);
            }
        }
    }
}

fn visit_import_equals(
    _decl: &TSImportEqualsDeclaration<'_>,
    _state: &mut ParseState<'_>,
    _ctx: &VisitCtx<'_>,
) {
    // `import x = require('m')` and `import x = ns.thing` were not surfaced as
    // imports by the SWC visitor either; preserved as a no-op for parity.
}

// ── Function declarations ────────────────────────────────────────────────────

fn visit_function_declaration(
    func: &Function<'_>,
    extra_decorators: &[DecoratorCapture],
    exported: bool,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    let name = func
        .id
        .as_ref()
        .map_or_else(|| "anonymous".to_owned(), |i| i.name.to_string());
    let decorators = extra_decorators.to_vec();
    let signature = function_signature_from_function(&name, func, ctx.source);

    let func_node = state.push_symbol(
        NodeKind::Function,
        name.clone(),
        Some(name.clone()),
        Some(ctx.span(func.span)),
        Some(signature),
        if exported {
            Some(Visibility::Public)
        } else {
            None
        },
        ctx.parent_class.as_ref().map(|c| c.name.clone()),
        decorators,
        ctx.class_decorators.clone(),
        Vec::new(),
    );

    if ctx.depth < MAX_DEPTH
        && let Some(body) = func.body.as_ref()
    {
        let mut body_ctx = ctx.child_with_owner(func_node.id);
        visit_function_body(body, state, &mut body_ctx);
    }
}

fn visit_function_declaration_default(
    func: &Function<'_>,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    let name = func
        .id
        .as_ref()
        .map_or_else(|| "anonymous".to_owned(), |i| i.name.to_string());
    let decorators: Vec<DecoratorCapture> = Vec::new();
    let signature = function_signature_from_function(&name, func, ctx.source);

    let func_node = state.push_symbol(
        NodeKind::Function,
        name.clone(),
        Some(name.clone()),
        Some(ctx.span(func.span)),
        Some(signature),
        Some(Visibility::Public),
        ctx.parent_class.as_ref().map(|c| c.name.clone()),
        decorators,
        ctx.class_decorators.clone(),
        Vec::new(),
    );

    if ctx.depth < MAX_DEPTH
        && let Some(body) = func.body.as_ref()
    {
        let mut body_ctx = ctx.child_with_owner(func_node.id);
        visit_function_body(body, state, &mut body_ctx);
    }
}

// ── Class declarations ───────────────────────────────────────────────────────

fn visit_class_declaration(
    class: &Class<'_>,
    exported: bool,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    let name = class
        .id
        .as_ref()
        .map_or_else(|| "AnonymousClass".to_owned(), |i| i.name.to_string());
    let decorators: Vec<DecoratorCapture> =
        decorators_from_iter(&class.decorators, ctx.source, ctx.offsets);
    let constructor_deps = collect_constructor_deps(class, ctx.source);
    let implemented = collect_implemented_interfaces(class);

    let class_node = state.push_symbol(
        NodeKind::Class,
        name.clone(),
        Some(name.clone()),
        Some(ctx.span(class.span)),
        None,
        if exported {
            Some(Visibility::Public)
        } else {
            None
        },
        None,
        decorators.clone(),
        Vec::new(),
        constructor_deps,
    );
    state.set_symbol_implemented_interfaces(class_node.id, implemented);

    let mut class_ctx = ctx.child_with_class(&class_node, decorators);
    for element in &class.body.body {
        visit_class_element(element, state, &mut class_ctx);
    }
}

fn visit_class_declaration_default(
    class: &Class<'_>,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    let name = class
        .id
        .as_ref()
        .map_or_else(|| "AnonymousClass".to_owned(), |i| i.name.to_string());
    let decorators: Vec<DecoratorCapture> =
        decorators_from_iter(&class.decorators, ctx.source, ctx.offsets);
    let constructor_deps = collect_constructor_deps(class, ctx.source);
    let implemented = collect_implemented_interfaces(class);

    let class_node = state.push_symbol(
        NodeKind::Class,
        name.clone(),
        Some(name.clone()),
        Some(ctx.span(class.span)),
        None,
        Some(Visibility::Public),
        None,
        decorators.clone(),
        Vec::new(),
        constructor_deps,
    );
    state.set_symbol_implemented_interfaces(class_node.id, implemented);

    let mut class_ctx = ctx.child_with_class(&class_node, decorators);
    for element in &class.body.body {
        visit_class_element(element, state, &mut class_ctx);
    }
}

fn visit_nested_class_body(class: &Class<'_>, state: &mut ParseState<'_>, ctx: &mut VisitCtx<'_>) {
    if ctx.depth >= MAX_DEPTH {
        return;
    }
    for element in &class.body.body {
        match element {
            ClassElement::MethodDefinition(method) => {
                if let Some(body) = method.value.body.as_ref() {
                    let mut body_ctx = VisitCtx {
                        source: ctx.source,
                        offsets: ctx.offsets,
                        parent_class: ctx.parent_class.clone(),
                        class_decl_depth: ctx.class_decl_depth.saturating_add(1),
                        owner: ctx.owner,
                        force_exported: false,
                        class_decorators: ctx.class_decorators.clone(),
                        depth: ctx.depth + 1,
                    };
                    visit_function_body(body, state, &mut body_ctx);
                }
            }
            ClassElement::PropertyDefinition(prop) => {
                if let Some(value) = prop.value.as_ref() {
                    visit_expression(value, state, ctx);
                }
            }
            ClassElement::AccessorProperty(prop) => {
                if let Some(value) = prop.value.as_ref() {
                    visit_expression(value, state, ctx);
                }
            }
            ClassElement::StaticBlock(block) => {
                let mut body_ctx = VisitCtx {
                    source: ctx.source,
                    offsets: ctx.offsets,
                    parent_class: ctx.parent_class.clone(),
                    class_decl_depth: ctx.class_decl_depth.saturating_add(1),
                    owner: ctx.owner,
                    force_exported: false,
                    class_decorators: ctx.class_decorators.clone(),
                    depth: ctx.depth + 1,
                };
                for stmt in &block.body {
                    visit_statement(stmt, state, &mut body_ctx);
                }
            }
            ClassElement::TSIndexSignature(_) => {}
        }
    }
}

fn visit_class_element(
    element: &ClassElement<'_>,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    match element {
        ClassElement::MethodDefinition(method) => visit_method_definition(method, state, ctx),
        ClassElement::PropertyDefinition(prop) => {
            let key_text = property_key_text(&prop.key);
            if !key_text.is_empty()
                && let Some(value) = prop.value.as_ref()
            {
                match value {
                    Expression::ArrowFunctionExpression(arrow) => {
                        visit_arrow_property(
                            &key_text,
                            prop.span,
                            arrow,
                            prop.accessibility,
                            state,
                            ctx,
                        );
                        return;
                    }
                    Expression::FunctionExpression(func) => {
                        visit_function_property(
                            &key_text,
                            prop.span,
                            func,
                            prop.accessibility,
                            state,
                            ctx,
                        );
                        return;
                    }
                    _ => {}
                }
            }
            if let Some(value) = prop.value.as_ref()
                && ctx.depth < MAX_DEPTH
            {
                let owner = ctx.parent_class.as_ref().map(|c| c.id);
                let mut val_ctx = VisitCtx {
                    source: ctx.source,
                    offsets: ctx.offsets,
                    parent_class: ctx.parent_class.clone(),
                    class_decl_depth: ctx.class_decl_depth,
                    owner,
                    force_exported: false,
                    class_decorators: ctx.class_decorators.clone(),
                    depth: ctx.depth + 1,
                };
                visit_expression(value, state, &mut val_ctx);
            }
        }
        ClassElement::AccessorProperty(prop) => {
            if let Some(value) = prop.value.as_ref() {
                visit_expression(value, state, ctx);
            }
        }
        ClassElement::StaticBlock(block) => {
            if ctx.depth < MAX_DEPTH
                && let Some(owner) = ctx.parent_class.as_ref().map(|c| c.id)
            {
                let mut body_ctx = ctx.child_with_owner(owner);
                for stmt in &block.body {
                    visit_statement(stmt, state, &mut body_ctx);
                }
            }
        }
        ClassElement::TSIndexSignature(_) => {}
    }
}

fn visit_method_definition(
    method: &MethodDefinition<'_>,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    if matches!(method.kind, MethodDefinitionKind::Constructor) {
        visit_constructor_method(method, state, ctx);
        return;
    }

    let raw_name = property_key_text(&method.key);
    let mut name = if raw_name.is_empty() {
        "anonymous".to_owned()
    } else {
        raw_name
    };
    if let PropertyKey::PrivateIdentifier(private) = &method.key {
        name = format!("#{}", private.name);
    }

    let Some(parent_class) = ctx.parent_class.clone() else {
        // No parent class — recurse into body for call sites only.
        if ctx.depth < MAX_DEPTH
            && let Some(body) = method.value.body.as_ref()
        {
            let mut body_ctx = VisitCtx {
                source: ctx.source,
                offsets: ctx.offsets,
                parent_class: None,
                class_decl_depth: ctx.class_decl_depth,
                owner: ctx.owner,
                force_exported: false,
                class_decorators: ctx.class_decorators.clone(),
                depth: ctx.depth + 1,
            };
            visit_function_body(body, state, &mut body_ctx);
        }
        return;
    };

    let decorators = decorators_from_iter(&method.decorators, ctx.source, ctx.offsets);

    let visibility = visibility_from_accessibility(method.accessibility);
    let signature = function_signature_from_function(&name, &method.value, ctx.source);

    let method_node = state.push_symbol(
        NodeKind::Function,
        name.clone(),
        Some(format!("{}.{}", parent_class.name, name)),
        Some(ctx.span(method.span)),
        Some(signature),
        Some(visibility),
        Some(parent_class.name.clone()),
        decorators,
        ctx.class_decorators.clone(),
        Vec::new(),
    );

    if ctx.depth < MAX_DEPTH
        && let Some(body) = method.value.body.as_ref()
    {
        let mut body_ctx = ctx.child_with_owner(method_node.id);
        visit_function_body(body, state, &mut body_ctx);
    }
}

fn visit_constructor_method(
    method: &MethodDefinition<'_>,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    let Some(parent_class) = ctx.parent_class.clone() else {
        return;
    };

    let name = "constructor".to_owned();
    let constructor_deps = constructor_deps_from_function(&method.value, ctx.source);
    let visibility = visibility_from_accessibility(method.accessibility);
    let constructor_node = state.push_symbol(
        NodeKind::Function,
        name.clone(),
        Some(format!("{}.{}", parent_class.name, name)),
        Some(ctx.span(method.span)),
        None,
        Some(visibility),
        Some(parent_class.name.clone()),
        Vec::new(),
        ctx.class_decorators.clone(),
        constructor_deps,
    );

    // Parameter decorators (e.g. @Inject, @InjectModel) — emit call sites
    // owned by the parent class to mirror the SWC visitor and the
    // tree-sitter visitor that came before it.
    for param in &method.value.params.items {
        for decorator in &param.decorators {
            emit_decorator_call_site(parent_class.id, decorator, ctx, state);
        }
    }

    if ctx.depth < MAX_DEPTH
        && let Some(body) = method.value.body.as_ref()
    {
        let mut body_ctx = ctx.child_with_owner(constructor_node.id);
        visit_function_body(body, state, &mut body_ctx);
    }
}

fn visit_arrow_property(
    name: &str,
    prop_span: Span,
    arrow: &ArrowFunctionExpression<'_>,
    accessibility: Option<TSAccessibility>,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    let qualified_name = ctx
        .parent_class
        .as_ref()
        .map_or_else(|| name.to_owned(), |c| format!("{}.{}", c.name, name));
    let signature =
        function_signature_from_arrow(name, arrow, ctx.source).unwrap_or_else(|| name.to_owned());
    let visibility = visibility_from_accessibility(accessibility);
    let func_node = state.push_symbol(
        NodeKind::Function,
        name.to_owned(),
        Some(qualified_name),
        Some(ctx.span(prop_span)),
        Some(signature),
        Some(visibility),
        ctx.parent_class.as_ref().map(|c| c.name.clone()),
        Vec::new(),
        ctx.class_decorators.clone(),
        Vec::new(),
    );
    if ctx.depth < MAX_DEPTH {
        let mut body_ctx = ctx.child_with_owner(func_node.id);
        visit_function_body(&arrow.body, state, &mut body_ctx);
    }
}

fn visit_function_property(
    name: &str,
    prop_span: Span,
    func: &Function<'_>,
    accessibility: Option<TSAccessibility>,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    let qualified_name = ctx
        .parent_class
        .as_ref()
        .map_or_else(|| name.to_owned(), |c| format!("{}.{}", c.name, name));
    let inner_name = func
        .id
        .as_ref()
        .map_or_else(|| name.to_owned(), |i| i.name.to_string());
    let signature = function_signature_from_function(&inner_name, func, ctx.source);
    let visibility = visibility_from_accessibility(accessibility);

    let func_node = state.push_symbol(
        NodeKind::Function,
        name.to_owned(),
        Some(qualified_name),
        Some(ctx.span(prop_span)),
        Some(signature),
        Some(visibility),
        ctx.parent_class.as_ref().map(|c| c.name.clone()),
        Vec::new(),
        ctx.class_decorators.clone(),
        Vec::new(),
    );

    if ctx.depth < MAX_DEPTH
        && let Some(body) = func.body.as_ref()
    {
        let mut body_ctx = ctx.child_with_owner(func_node.id);
        visit_function_body(body, state, &mut body_ctx);
    }
}

// ── Variables ────────────────────────────────────────────────────────────────

fn visit_variable_declaration(
    var: &VariableDeclaration<'_>,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    for declarator in &var.declarations {
        visit_variable_declarator(declarator, state, ctx);
    }
}

fn visit_variable_declarator(
    declarator: &VariableDeclarator<'_>,
    state: &mut ParseState<'_>,
    ctx: &VisitCtx<'_>,
) {
    let name = pattern_name_from_source(&declarator.id, ctx.source)
        .unwrap_or_else(|| "anonymous".to_owned());

    let Some(init) = declarator.init.as_ref() else {
        return;
    };

    match init {
        Expression::ArrowFunctionExpression(arrow) => {
            let qualified_name = ctx
                .parent_class
                .as_ref()
                .map_or_else(|| name.clone(), |c| format!("{}.{}", c.name, name));
            let signature = function_signature_from_arrow(&name, arrow, ctx.source)
                .unwrap_or_else(|| name.clone());
            let func_node = state.push_symbol(
                NodeKind::Function,
                name.clone(),
                Some(qualified_name),
                Some(ctx.span(declarator.span)),
                Some(signature),
                if ctx.force_exported {
                    Some(Visibility::Public)
                } else {
                    None
                },
                ctx.parent_class.as_ref().map(|c| c.name.clone()),
                Vec::new(),
                ctx.class_decorators.clone(),
                Vec::new(),
            );
            if ctx.depth < MAX_DEPTH {
                let mut body_ctx = ctx.child_with_owner(func_node.id);
                visit_function_body(&arrow.body, state, &mut body_ctx);
            }
            return;
        }
        Expression::FunctionExpression(func) => {
            let qualified_name = ctx
                .parent_class
                .as_ref()
                .map_or_else(|| name.clone(), |c| format!("{}.{}", c.name, name));
            let inner_name = func
                .id
                .as_ref()
                .map_or_else(|| name.clone(), |i| i.name.to_string());
            let signature = function_signature_from_function(&inner_name, func, ctx.source);
            let func_node = state.push_symbol(
                NodeKind::Function,
                name.clone(),
                Some(qualified_name),
                Some(ctx.span(declarator.span)),
                Some(signature),
                if ctx.force_exported {
                    Some(Visibility::Public)
                } else {
                    None
                },
                ctx.parent_class.as_ref().map(|c| c.name.clone()),
                Vec::new(),
                ctx.class_decorators.clone(),
                Vec::new(),
            );
            if ctx.depth < MAX_DEPTH
                && let Some(body) = func.body.as_ref()
            {
                let mut body_ctx = ctx.child_with_owner(func_node.id);
                visit_function_body(body, state, &mut body_ctx);
            }
            return;
        }
        other => {
            if let Some(constants) = extract_constant_string_value(&name, other) {
                for (k, v) in constants {
                    state.record_constant_string(k, v);
                }
            }
        }
    }

    if ctx.depth < MAX_DEPTH {
        let mut expr_ctx = VisitCtx {
            source: ctx.source,
            offsets: ctx.offsets,
            parent_class: ctx.parent_class.clone(),
            class_decl_depth: ctx.class_decl_depth,
            owner: ctx.owner,
            force_exported: ctx.force_exported,
            class_decorators: ctx.class_decorators.clone(),
            depth: ctx.depth + 1,
        };
        visit_expression(init, state, &mut expr_ctx);
    }
}

// ── Expressions / call sites ─────────────────────────────────────────────────

fn visit_function_body(
    body: &FunctionBody<'_>,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    for stmt in &body.statements {
        visit_statement(stmt, state, ctx);
    }
}

fn visit_expression(expr: &Expression<'_>, state: &mut ParseState<'_>, ctx: &mut VisitCtx<'_>) {
    if ctx.depth > MAX_DEPTH {
        return;
    }
    match expr {
        Expression::CallExpression(call) => visit_call_expression(call, state, ctx),
        Expression::NewExpression(new_expr) => visit_new_expression(new_expr, state, ctx),
        Expression::AssignmentExpression(assign) => {
            visit_assignment_target(&assign.left, state, ctx);
            visit_expression(&assign.right, state, ctx);
        }
        Expression::SequenceExpression(seq) => {
            for inner in &seq.expressions {
                visit_expression(inner, state, ctx);
            }
        }
        Expression::ParenthesizedExpression(p) => visit_expression(&p.expression, state, ctx),
        Expression::ConditionalExpression(c) => {
            visit_expression(&c.test, state, ctx);
            visit_expression(&c.consequent, state, ctx);
            visit_expression(&c.alternate, state, ctx);
        }
        Expression::UnaryExpression(u) => visit_expression(&u.argument, state, ctx),
        Expression::AwaitExpression(a) => visit_expression(&a.argument, state, ctx),
        Expression::YieldExpression(y) => {
            if let Some(arg) = &y.argument {
                visit_expression(arg, state, ctx);
            }
        }
        Expression::BinaryExpression(b) => {
            visit_expression(&b.left, state, ctx);
            visit_expression(&b.right, state, ctx);
        }
        Expression::LogicalExpression(b) => {
            visit_expression(&b.left, state, ctx);
            visit_expression(&b.right, state, ctx);
        }
        Expression::ComputedMemberExpression(m) => {
            visit_expression(&m.expression, state, ctx);
            // Walk the object chain iteratively so nested call sites surface.
            walk_member_object(&m.object, state, ctx);
        }
        Expression::StaticMemberExpression(m) => {
            walk_member_object(&m.object, state, ctx);
        }
        Expression::PrivateFieldExpression(m) => {
            walk_member_object(&m.object, state, ctx);
        }
        Expression::TaggedTemplateExpression(t) => {
            visit_expression(&t.tag, state, ctx);
            for inner in &t.quasi.expressions {
                visit_expression(inner, state, ctx);
            }
        }
        Expression::TemplateLiteral(t) => {
            for inner in &t.expressions {
                visit_expression(inner, state, ctx);
            }
        }
        Expression::ChainExpression(chain) => visit_chain_element(&chain.expression, state, ctx),
        Expression::UpdateExpression(u) => visit_simple_assign_target(&u.argument, state, ctx),
        Expression::TSAsExpression(t) => visit_expression(&t.expression, state, ctx),
        Expression::TSSatisfiesExpression(t) => visit_expression(&t.expression, state, ctx),
        Expression::TSTypeAssertion(t) => visit_expression(&t.expression, state, ctx),
        Expression::TSNonNullExpression(t) => visit_expression(&t.expression, state, ctx),
        Expression::TSInstantiationExpression(t) => visit_expression(&t.expression, state, ctx),
        Expression::ClassExpression(class) => visit_class_expression(class, state, ctx),
        Expression::ArrowFunctionExpression(arrow) => {
            let mut body_ctx = ctx.child_no_export();
            visit_function_body(&arrow.body, state, &mut body_ctx);
        }
        Expression::FunctionExpression(func) => {
            if ctx.depth < MAX_DEPTH
                && let Some(body) = func.body.as_ref()
            {
                let mut body_ctx = ctx.child_no_export();
                visit_function_body(body, state, &mut body_ctx);
            }
        }
        Expression::ObjectExpression(obj) => visit_object_expression(obj, state, ctx),
        Expression::ArrayExpression(arr) => {
            for elem in &arr.elements {
                if let Some(expr) = elem.as_expression() {
                    visit_expression(expr, state, ctx);
                }
            }
        }
        Expression::JSXElement(jsx) => visit_jsx_element(jsx, state, ctx),
        Expression::JSXFragment(frag) => {
            for child in &frag.children {
                visit_jsx_child(child, state, ctx);
            }
        }
        Expression::ImportExpression(imp) => {
            // Dynamic `import('m')` — emit a call site named `import` so the
            // module-resolution side of augmentation can pick it up.
            if let Some(owner) = ctx.owner {
                let literal_argument =
                    first_literal_argument_from_expression(&imp.source, ctx.source);
                let raw_arguments = expr_raw_text(&imp.source, ctx.source);
                state.push_call_site_with_span(
                    owner,
                    "import".to_owned(),
                    Some("import".to_owned()),
                    literal_argument,
                    Some(raw_arguments),
                    ctx.span(imp.span),
                );
            }
            visit_expression(&imp.source, state, ctx);
        }
        Expression::V8IntrinsicExpression(intr) => {
            for arg in &intr.arguments {
                visit_argument(arg, state, ctx);
            }
        }
        // Literals, identifiers, this/super, meta — nothing recursive needed.
        _ => {}
    }
}

fn visit_class_expression(class: &Class<'_>, state: &mut ParseState<'_>, ctx: &mut VisitCtx<'_>) {
    if ctx.depth >= MAX_DEPTH {
        return;
    }
    for element in &class.body.body {
        match element {
            ClassElement::MethodDefinition(method) => {
                if let Some(body) = method.value.body.as_ref() {
                    let mut body_ctx = VisitCtx {
                        source: ctx.source,
                        offsets: ctx.offsets,
                        parent_class: ctx.parent_class.clone(),
                        class_decl_depth: ctx.class_decl_depth,
                        owner: ctx.owner,
                        force_exported: false,
                        class_decorators: ctx.class_decorators.clone(),
                        depth: ctx.depth + 1,
                    };
                    visit_function_body(body, state, &mut body_ctx);
                }
            }
            ClassElement::PropertyDefinition(prop) => {
                if let Some(value) = prop.value.as_ref() {
                    visit_expression(value, state, ctx);
                }
            }
            ClassElement::AccessorProperty(prop) => {
                if let Some(value) = prop.value.as_ref() {
                    visit_expression(value, state, ctx);
                }
            }
            ClassElement::StaticBlock(block) => {
                let mut body_ctx = VisitCtx {
                    source: ctx.source,
                    offsets: ctx.offsets,
                    parent_class: ctx.parent_class.clone(),
                    class_decl_depth: ctx.class_decl_depth,
                    owner: ctx.owner,
                    force_exported: false,
                    class_decorators: ctx.class_decorators.clone(),
                    depth: ctx.depth + 1,
                };
                for stmt in &block.body {
                    visit_statement(stmt, state, &mut body_ctx);
                }
            }
            ClassElement::TSIndexSignature(_) => {}
        }
    }
}

fn visit_object_expression(
    obj: &ObjectExpression<'_>,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    for prop_or_spread in &obj.properties {
        match prop_or_spread {
            ObjectPropertyKind::ObjectProperty(prop) => match prop.kind {
                PropertyKind::Init => {
                    visit_expression(&prop.value, state, ctx);
                }
                PropertyKind::Get | PropertyKind::Set => {
                    if let Expression::FunctionExpression(func) = &prop.value
                        && let Some(body) = func.body.as_ref()
                    {
                        let mut body_ctx = ctx.child_no_export();
                        visit_function_body(body, state, &mut body_ctx);
                    }
                }
            },
            ObjectPropertyKind::SpreadProperty(spread) => {
                visit_expression(&spread.argument, state, ctx);
            }
        }
    }
}

fn walk_member_object(object: &Expression<'_>, state: &mut ParseState<'_>, ctx: &mut VisitCtx<'_>) {
    let mut current = object;
    let mut limit = 10_000_usize;
    loop {
        if limit == 0 {
            break;
        }
        limit -= 1;
        match current {
            Expression::ComputedMemberExpression(m) => {
                visit_expression(&m.expression, state, ctx);
                current = &m.object;
            }
            Expression::StaticMemberExpression(m) => {
                current = &m.object;
            }
            Expression::PrivateFieldExpression(m) => {
                current = &m.object;
            }
            other => {
                visit_expression(other, state, ctx);
                break;
            }
        }
    }
}

fn visit_chain_element(
    element: &ChainElement<'_>,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    match element {
        ChainElement::CallExpression(call) => visit_call_expression(call, state, ctx),
        ChainElement::TSNonNullExpression(t) => visit_expression(&t.expression, state, ctx),
        ChainElement::ComputedMemberExpression(m) => {
            visit_expression(&m.expression, state, ctx);
            walk_member_object(&m.object, state, ctx);
        }
        ChainElement::StaticMemberExpression(m) => walk_member_object(&m.object, state, ctx),
        ChainElement::PrivateFieldExpression(m) => walk_member_object(&m.object, state, ctx),
    }
}

fn visit_call_expression(
    call: &CallExpression<'_>,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    if let Some(owner) = ctx.owner {
        let (callee_name, qualified_hint) = expression_name_from_expr(&call.callee);
        if !callee_name.is_empty() {
            let literal_argument =
                first_literal_argument_from_arguments(&call.arguments, ctx.source);
            let raw_arguments = raw_arguments_from_arguments(&call.arguments, ctx.source);
            state.push_call_site_with_span(
                owner,
                callee_name,
                qualified_hint,
                literal_argument,
                Some(raw_arguments),
                ctx.span(call.span),
            );
        }
    }
    for arg in &call.arguments {
        visit_argument(arg, state, ctx);
    }
    visit_expression(&call.callee, state, ctx);
}

fn visit_new_expression(
    new_expr: &NewExpression<'_>,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    if let Some(owner) = ctx.owner {
        let (callee_name, qualified_hint) = expression_name_from_expr(&new_expr.callee);
        if !callee_name.is_empty() {
            let literal_argument =
                first_literal_argument_from_arguments(&new_expr.arguments, ctx.source);
            let raw_arguments = Some(raw_arguments_from_arguments(
                &new_expr.arguments,
                ctx.source,
            ));
            state.push_call_site_with_span(
                owner,
                callee_name,
                qualified_hint,
                literal_argument,
                raw_arguments,
                ctx.span(new_expr.span),
            );
        }
    }
    for arg in &new_expr.arguments {
        visit_argument(arg, state, ctx);
    }
    visit_expression(&new_expr.callee, state, ctx);
}

fn visit_argument(arg: &Argument<'_>, state: &mut ParseState<'_>, ctx: &mut VisitCtx<'_>) {
    match arg {
        Argument::SpreadElement(spread) => visit_expression(&spread.argument, state, ctx),
        other => {
            if let Some(expr) = other.as_expression() {
                visit_expression(expr, state, ctx);
            }
        }
    }
}

fn visit_assignment_target(
    target: &AssignmentTarget<'_>,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    match target {
        AssignmentTarget::ComputedMemberExpression(m) => {
            walk_member_object(&m.object, state, ctx);
            visit_expression(&m.expression, state, ctx);
        }
        AssignmentTarget::StaticMemberExpression(m) => walk_member_object(&m.object, state, ctx),
        AssignmentTarget::PrivateFieldExpression(m) => walk_member_object(&m.object, state, ctx),
        AssignmentTarget::TSAsExpression(t) => visit_expression(&t.expression, state, ctx),
        AssignmentTarget::TSSatisfiesExpression(t) => visit_expression(&t.expression, state, ctx),
        AssignmentTarget::TSNonNullExpression(t) => visit_expression(&t.expression, state, ctx),
        AssignmentTarget::TSTypeAssertion(t) => visit_expression(&t.expression, state, ctx),
        AssignmentTarget::ArrayAssignmentTarget(arr) => {
            visit_array_assignment_target(arr, state, ctx);
        }
        AssignmentTarget::ObjectAssignmentTarget(obj) => {
            visit_object_assignment_target(obj, state, ctx);
        }
        AssignmentTarget::AssignmentTargetIdentifier(_) => {}
    }
}

fn visit_simple_assign_target(
    target: &SimpleAssignmentTarget<'_>,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    match target {
        SimpleAssignmentTarget::ComputedMemberExpression(m) => {
            walk_member_object(&m.object, state, ctx);
            visit_expression(&m.expression, state, ctx);
        }
        SimpleAssignmentTarget::StaticMemberExpression(m) => {
            walk_member_object(&m.object, state, ctx);
        }
        SimpleAssignmentTarget::PrivateFieldExpression(m) => {
            walk_member_object(&m.object, state, ctx);
        }
        SimpleAssignmentTarget::TSAsExpression(t) => visit_expression(&t.expression, state, ctx),
        SimpleAssignmentTarget::TSSatisfiesExpression(t) => {
            visit_expression(&t.expression, state, ctx);
        }
        SimpleAssignmentTarget::TSNonNullExpression(t) => {
            visit_expression(&t.expression, state, ctx);
        }
        SimpleAssignmentTarget::TSTypeAssertion(t) => visit_expression(&t.expression, state, ctx),
        SimpleAssignmentTarget::AssignmentTargetIdentifier(_) => {}
    }
}

fn visit_array_assignment_target(
    arr: &ArrayAssignmentTarget<'_>,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    for elem in &arr.elements {
        if let Some(elem) = elem.as_ref() {
            visit_assignment_target_maybe_default(elem, state, ctx);
        }
    }
    if let Some(rest) = arr.rest.as_ref() {
        visit_assignment_target(&rest.target, state, ctx);
    }
}

fn visit_object_assignment_target(
    obj: &ObjectAssignmentTarget<'_>,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    for property in &obj.properties {
        match property {
            AssignmentTargetProperty::AssignmentTargetPropertyIdentifier(prop) => {
                if let Some(default) = &prop.init {
                    visit_expression(default, state, ctx);
                }
            }
            AssignmentTargetProperty::AssignmentTargetPropertyProperty(prop) => {
                visit_assignment_target_maybe_default(&prop.binding, state, ctx);
            }
        }
    }
    if let Some(rest) = obj.rest.as_ref() {
        visit_assignment_target(&rest.target, state, ctx);
    }
}

fn visit_assignment_target_maybe_default(
    target: &AssignmentTargetMaybeDefault<'_>,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    match target {
        AssignmentTargetMaybeDefault::AssignmentTargetWithDefault(with_default) => {
            visit_assignment_target(&with_default.binding, state, ctx);
            visit_expression(&with_default.init, state, ctx);
        }
        AssignmentTargetMaybeDefault::AssignmentTargetIdentifier(_) => {}
        AssignmentTargetMaybeDefault::ComputedMemberExpression(m) => {
            walk_member_object(&m.object, state, ctx);
            visit_expression(&m.expression, state, ctx);
        }
        AssignmentTargetMaybeDefault::StaticMemberExpression(m) => {
            walk_member_object(&m.object, state, ctx);
        }
        AssignmentTargetMaybeDefault::PrivateFieldExpression(m) => {
            walk_member_object(&m.object, state, ctx);
        }
        AssignmentTargetMaybeDefault::TSAsExpression(t) => {
            visit_expression(&t.expression, state, ctx);
        }
        AssignmentTargetMaybeDefault::TSSatisfiesExpression(t) => {
            visit_expression(&t.expression, state, ctx);
        }
        AssignmentTargetMaybeDefault::TSNonNullExpression(t) => {
            visit_expression(&t.expression, state, ctx);
        }
        AssignmentTargetMaybeDefault::TSTypeAssertion(t) => {
            visit_expression(&t.expression, state, ctx);
        }
        AssignmentTargetMaybeDefault::ArrayAssignmentTarget(arr) => {
            visit_array_assignment_target(arr, state, ctx);
        }
        AssignmentTargetMaybeDefault::ObjectAssignmentTarget(obj) => {
            visit_object_assignment_target(obj, state, ctx);
        }
    }
}

// ── JSX ──────────────────────────────────────────────────────────────────────

fn visit_jsx_element(jsx: &JSXElement<'_>, state: &mut ParseState<'_>, ctx: &mut VisitCtx<'_>) {
    visit_jsx_attributes(&jsx.opening_element.attributes, state, ctx);
    for child in &jsx.children {
        visit_jsx_child(child, state, ctx);
    }
}

fn visit_jsx_attributes(
    attrs: &[JSXAttributeItem<'_>],
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    for attr in attrs {
        match attr {
            JSXAttributeItem::Attribute(attr) => {
                if let Some(value) = attr.value.as_ref() {
                    match value {
                        JSXAttributeValue::ExpressionContainer(c) => {
                            if let JSXExpression::EmptyExpression(_) = &c.expression {
                                continue;
                            }
                            if let Some(expr) = c.expression.as_expression() {
                                visit_expression(expr, state, ctx);
                            }
                        }
                        JSXAttributeValue::Element(el) => visit_jsx_element(el, state, ctx),
                        JSXAttributeValue::Fragment(frag) => {
                            for child in &frag.children {
                                visit_jsx_child(child, state, ctx);
                            }
                        }
                        JSXAttributeValue::StringLiteral(_) => {}
                    }
                }
            }
            JSXAttributeItem::SpreadAttribute(spread) => {
                visit_expression(&spread.argument, state, ctx);
            }
        }
    }
}

fn visit_jsx_child(child: &JSXChild<'_>, state: &mut ParseState<'_>, ctx: &mut VisitCtx<'_>) {
    match child {
        JSXChild::ExpressionContainer(c) => {
            if let JSXExpression::EmptyExpression(_) = &c.expression {
                return;
            }
            if let Some(expr) = c.expression.as_expression() {
                visit_expression(expr, state, ctx);
            }
        }
        JSXChild::Spread(spread) => visit_expression(&spread.expression, state, ctx),
        JSXChild::Element(el) => visit_jsx_element(el, state, ctx),
        JSXChild::Fragment(frag) => {
            for child in &frag.children {
                visit_jsx_child(child, state, ctx);
            }
        }
        JSXChild::Text(_) => {}
    }
}

// ── Imports / exports ────────────────────────────────────────────────────────

fn import_bindings_from_decl(decl: &ImportDeclaration<'_>) -> Vec<ImportBinding> {
    let source = decl.source.value.to_string();
    let is_type_only = decl.import_kind == ImportOrExportKind::Type;
    decl.specifiers
        .as_ref()
        .into_iter()
        .flatten()
        .map(|specifier| match specifier {
            ImportDeclarationSpecifier::ImportSpecifier(spec) => {
                let imported_name = module_export_name(&spec.imported);
                ImportBinding {
                    local_name: spec.local.name.to_string(),
                    imported_name: Some(imported_name),
                    source: source.clone(),
                    resolved_path: None,
                    is_default: false,
                    is_namespace: false,
                    is_type_only: is_type_only || spec.import_kind == ImportOrExportKind::Type,
                }
            }
            ImportDeclarationSpecifier::ImportDefaultSpecifier(spec) => ImportBinding {
                local_name: spec.local.name.to_string(),
                imported_name: None,
                source: source.clone(),
                resolved_path: None,
                is_default: true,
                is_namespace: false,
                is_type_only,
            },
            ImportDeclarationSpecifier::ImportNamespaceSpecifier(spec) => ImportBinding {
                local_name: spec.local.name.to_string(),
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

fn import_bindings_from_named_export(decl: &ExportNamedDeclaration<'_>) -> Vec<ImportBinding> {
    let Some(source) = decl.source.as_ref().map(|s| s.value.to_string()) else {
        return Vec::new();
    };
    let outer_is_type = decl.export_kind == ImportOrExportKind::Type;
    decl.specifiers
        .iter()
        .map(|spec| {
            let local_name = module_export_name(&spec.exported);
            let imported_name = module_export_name(&spec.local);
            ImportBinding {
                local_name,
                imported_name: Some(imported_name),
                source: source.clone(),
                resolved_path: None,
                is_default: false,
                is_namespace: false,
                is_type_only: outer_is_type || spec.export_kind == ImportOrExportKind::Type,
            }
        })
        .collect()
}

fn module_export_name(name: &ModuleExportName<'_>) -> String {
    match name {
        ModuleExportName::IdentifierName(ident) => ident.name.to_string(),
        ModuleExportName::IdentifierReference(ident) => ident.name.to_string(),
        ModuleExportName::StringLiteral(value) => value.value.to_string(),
    }
}

fn push_imports(
    state: &mut ParseState<'_>,
    source_specifier: &str,
    bindings: Vec<ImportBinding>,
    stmt_span: &SourceSpan,
) {
    use crate::tree_sitter::resolve_import_path_pub;
    use gather_step_core::{EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, ref_node_id};

    let resolved_path = resolve_import_path_pub(
        state.repo_root(),
        state.file().path.as_path(),
        source_specifier,
        state.file().language,
        state.path_aliases(),
    );

    let mut is_new_module = false;
    let module_id = {
        let ext_id = format!("module-import::{source_specifier}");
        let entry = state.module_cache_mut().entry(source_specifier.to_owned());
        *entry.or_insert_with(|| {
            is_new_module = true;
            ref_node_id(NodeKind::Module, &ext_id)
        })
    };

    if is_new_module {
        let ext_id = format!("module-import::{source_specifier}");
        state.push_raw_node(NodeData {
            id: module_id,
            kind: NodeKind::Module,
            repo: state.repo().to_owned(),
            file_path: state.file_path().to_owned(),
            name: source_specifier.to_owned(),
            qualified_name: Some(ext_id.clone()),
            external_id: Some(ext_id),
            signature: None,
            visibility: Some(Visibility::Public),
            span: Some(stmt_span.clone()),
            is_virtual: true,
        });
    }
    state.push_raw_edge(EdgeData {
        source: state.file_node_id(),
        target: module_id,
        kind: EdgeKind::Imports,
        metadata: EdgeMetadata::default(),
        owner_file: state.file_node_id(),
        is_cross_file: true,
    });

    for binding in bindings {
        let import_qn = format!("{}::{}", state.file_path(), binding.local_name);
        let import_node = NodeData {
            id: gather_step_core::node_id(
                state.repo(),
                state.file_path(),
                NodeKind::Import,
                &import_qn,
            ),
            kind: NodeKind::Import,
            repo: state.repo().to_owned(),
            file_path: state.file_path().to_owned(),
            name: binding.local_name.clone(),
            qualified_name: Some(import_qn),
            external_id: None,
            signature: Some(format!("from {source_specifier}")),
            visibility: None,
            span: Some(stmt_span.clone()),
            is_virtual: false,
        };
        state.push_raw_node(import_node.clone());
        state.push_raw_edge(EdgeData {
            source: state.file_node_id(),
            target: import_node.id,
            kind: EdgeKind::Defines,
            metadata: EdgeMetadata::default(),
            owner_file: state.file_node_id(),
            is_cross_file: false,
        });
        state.push_import_binding(ImportBinding {
            resolved_path: resolved_path.clone(),
            source: source_specifier.to_owned(),
            ..binding
        });
    }
}

// ── Decorators ───────────────────────────────────────────────────────────────

fn decorators_from_iter<'a>(
    decorators: impl IntoIterator<Item = &'a Decorator<'a>>,
    source: &str,
    offsets: &[u32],
) -> Vec<DecoratorCapture> {
    decorators
        .into_iter()
        .map(|decorator| single_decorator_from_oxc(decorator, source, offsets))
        .collect()
}

fn single_decorator_from_oxc(
    decorator: &Decorator<'_>,
    source: &str,
    offsets: &[u32],
) -> DecoratorCapture {
    let (name, _) = expression_name_from_expr(&decorator.expression);
    let (raw, arguments) = if let Expression::CallExpression(call) = &decorator.expression {
        let args_strings = args_text(&call.arguments, source);
        let call_text = source_slice(source, call.span);
        let callee_text = source_slice(source, call.callee.span());
        let after_callee = call_text
            .strip_prefix(callee_text)
            .unwrap_or(call_text)
            .trim_start_matches('(')
            .trim_end_matches(')')
            .trim();
        let raw = after_callee.to_owned();
        let arguments: smallvec::SmallVec<[Box<str>; 2]> = args_strings
            .into_iter()
            .map(String::into_boxed_str)
            .collect();
        (raw, arguments)
    } else {
        (String::new(), smallvec::SmallVec::new())
    };

    DecoratorCapture {
        name,
        arguments,
        raw,
        span: Some(span_from_oxc(decorator.span, offsets)),
    }
}

fn emit_decorator_call_site(
    owner_id: gather_step_core::NodeId,
    decorator: &Decorator<'_>,
    ctx: &VisitCtx<'_>,
    state: &mut ParseState<'_>,
) {
    let (callee_name, qualified_hint) = expression_name_from_expr(&decorator.expression);
    if callee_name.is_empty() {
        return;
    }
    let (literal_argument, raw_arguments) =
        if let Expression::CallExpression(call) = &decorator.expression {
            let literal = first_literal_argument_from_arguments(&call.arguments, ctx.source)
                .or_else(|| first_raw_arg_text(&call.arguments, ctx.source));
            let raw = Some(raw_arguments_from_arguments(&call.arguments, ctx.source));
            (literal, raw)
        } else {
            (None, None)
        };
    state.push_call_site_with_span(
        owner_id,
        callee_name,
        qualified_hint,
        literal_argument,
        raw_arguments,
        ctx.span(decorator.span),
    );
}

// ── Helpers shared with the SWC-era contract ─────────────────────────────────

fn args_text(args: &[Argument<'_>], source: &str) -> Vec<String> {
    args.iter()
        .filter_map(|arg| match arg {
            Argument::SpreadElement(_) => None,
            other => other.as_expression().map(|expr| {
                source_slice(source, expr.span())
                    .trim()
                    .trim_matches('"')
                    .trim_matches('\'')
                    .to_owned()
            }),
        })
        .filter(|piece| !piece.is_empty())
        .collect()
}

fn first_literal_argument_from_arguments(args: &[Argument<'_>], source: &str) -> Option<String> {
    for arg in args {
        if matches!(arg, Argument::SpreadElement(_)) {
            continue;
        }
        let Some(expr) = arg.as_expression() else {
            continue;
        };
        if let Some(literal) = first_literal_argument_from_expression(expr, source) {
            return Some(literal);
        }
    }
    None
}

fn first_literal_argument_from_expression(expr: &Expression<'_>, source: &str) -> Option<String> {
    match expr {
        Expression::StringLiteral(s) => Some(s.value.to_string()),
        Expression::ArrayExpression(arr) => {
            let raw = source_slice(source, arr.span);
            let stripped = raw.trim().trim_matches('[').trim_matches(']').trim();
            Some(stripped.to_owned())
        }
        Expression::TemplateLiteral(tpl) if tpl.expressions.is_empty() => {
            tpl.quasis.first().map(|q| q.value.raw.to_string())
        }
        _ => None,
    }
}

fn first_raw_arg_text(args: &[Argument<'_>], source: &str) -> Option<String> {
    args.iter().find_map(|arg| match arg {
        Argument::SpreadElement(_) => None,
        other => other
            .as_expression()
            .map(|expr| source_slice(source, expr.span()).trim().to_owned()),
    })
}

fn raw_arguments_from_arguments(args: &[Argument<'_>], source: &str) -> String {
    if args.is_empty() {
        return String::new();
    }
    args.iter()
        .map(|arg| match arg {
            Argument::SpreadElement(spread) => {
                format!("...{}", source_slice(source, spread.argument.span()))
            }
            other => other.as_expression().map_or_else(String::new, |expr| {
                source_slice(source, expr.span()).to_owned()
            }),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn expr_raw_text(expr: &Expression<'_>, source: &str) -> String {
    source_slice(source, expr.span()).to_owned()
}

fn member_property_text(member: &MemberExpression<'_>) -> String {
    match member {
        MemberExpression::StaticMemberExpression(m) => m.property.name.to_string(),
        MemberExpression::PrivateFieldExpression(m) => m.field.name.to_string(),
        MemberExpression::ComputedMemberExpression(_) => String::new(),
    }
}

enum NameCursor<'a> {
    Expr(&'a Expression<'a>),
    #[expect(
        dead_code,
        reason = "constructed by future overloads of expression_name_from_expr that traverse standalone MemberExpression nodes"
    )]
    Member(&'a MemberExpression<'a>),
}

fn expression_name_from_expr(expr: &Expression<'_>) -> (String, Option<String>) {
    let mut parts: Vec<String> = Vec::new();
    let mut limit = 10_000_usize;
    let mut current = NameCursor::Expr(expr);

    loop {
        if limit == 0 {
            break;
        }
        limit -= 1;
        match current {
            NameCursor::Expr(expr) => match expr {
                Expression::StaticMemberExpression(m) => {
                    let prop = m.property.name.to_string();
                    if !prop.is_empty() {
                        parts.push(prop);
                    }
                    current = NameCursor::Expr(&m.object);
                }
                Expression::PrivateFieldExpression(m) => {
                    let prop = m.field.name.to_string();
                    if !prop.is_empty() {
                        parts.push(prop);
                    }
                    current = NameCursor::Expr(&m.object);
                }
                Expression::ComputedMemberExpression(m) => {
                    current = NameCursor::Expr(&m.object);
                }
                Expression::CallExpression(call) => current = NameCursor::Expr(&call.callee),
                Expression::Identifier(ident) => {
                    parts.push(ident.name.to_string());
                    break;
                }
                Expression::ThisExpression(_) => {
                    parts.push("this".to_owned());
                    break;
                }
                Expression::ParenthesizedExpression(p) => current = NameCursor::Expr(&p.expression),
                Expression::ChainExpression(chain) => match &chain.expression {
                    ChainElement::CallExpression(call) => current = NameCursor::Expr(&call.callee),
                    ChainElement::ComputedMemberExpression(m) => {
                        current = NameCursor::Expr(&m.object)
                    }
                    ChainElement::StaticMemberExpression(m) => {
                        let prop = m.property.name.to_string();
                        if !prop.is_empty() {
                            parts.push(prop);
                        }
                        current = NameCursor::Expr(&m.object);
                    }
                    ChainElement::PrivateFieldExpression(m) => {
                        let prop = m.field.name.to_string();
                        if !prop.is_empty() {
                            parts.push(prop);
                        }
                        current = NameCursor::Expr(&m.object);
                    }
                    ChainElement::TSNonNullExpression(t) => {
                        current = NameCursor::Expr(&t.expression)
                    }
                },
                Expression::TSAsExpression(t) => current = NameCursor::Expr(&t.expression),
                Expression::TSSatisfiesExpression(t) => current = NameCursor::Expr(&t.expression),
                Expression::TSNonNullExpression(t) => current = NameCursor::Expr(&t.expression),
                Expression::TSTypeAssertion(t) => current = NameCursor::Expr(&t.expression),
                Expression::TSInstantiationExpression(t) => {
                    current = NameCursor::Expr(&t.expression)
                }
                _ => break,
            },
            NameCursor::Member(member) => {
                let prop = member_property_text(member);
                if !prop.is_empty() {
                    parts.push(prop);
                }
                let object = match member {
                    MemberExpression::ComputedMemberExpression(m) => &m.object,
                    MemberExpression::StaticMemberExpression(m) => &m.object,
                    MemberExpression::PrivateFieldExpression(m) => &m.object,
                };
                current = NameCursor::Expr(object);
            }
        }
    }

    if parts.is_empty() {
        return (String::new(), None);
    }
    parts.reverse();
    let name = parts.last().cloned().unwrap_or_default();
    let qualified = parts.join(".");
    (name, Some(qualified))
}

fn property_key_text(key: &PropertyKey<'_>) -> String {
    match key {
        PropertyKey::StaticIdentifier(ident) => ident.name.to_string(),
        PropertyKey::PrivateIdentifier(private) => private.name.to_string(),
        PropertyKey::StringLiteral(s) => s.value.to_string(),
        PropertyKey::NumericLiteral(n) => n.value.to_string(),
        PropertyKey::BigIntLiteral(b) => b.value.to_string(),
        PropertyKey::TemplateLiteral(tpl) if tpl.expressions.is_empty() => tpl
            .quasis
            .first()
            .map(|q| q.value.raw.to_string())
            .unwrap_or_default(),
        _ => String::new(),
    }
}

fn pattern_name_from_source(pattern: &BindingPattern<'_>, source: &str) -> Option<String> {
    match pattern {
        BindingPattern::BindingIdentifier(binding) => Some(binding.name.to_string()),
        BindingPattern::AssignmentPattern(assign) => pattern_name_from_source(&assign.left, source),
        BindingPattern::ObjectPattern(_) | BindingPattern::ArrayPattern(_) => {
            Some(source_slice(source, pattern.span()).to_owned())
        }
    }
}

fn visibility_from_accessibility(acc: Option<TSAccessibility>) -> Visibility {
    match acc {
        Some(TSAccessibility::Private) => Visibility::Private,
        Some(TSAccessibility::Protected) => Visibility::Protected,
        Some(TSAccessibility::Public) | None => Visibility::Public,
    }
}

fn function_signature_from_function(name: &str, func: &Function<'_>, source: &str) -> String {
    let params_str = if func.params.items.is_empty() && func.params.rest.is_none() {
        let fn_text = source_slice(source, func.span);
        fn_text
            .find('(')
            .and_then(|open| {
                fn_text[open..]
                    .find(')')
                    .map(|close| fn_text[open..=open + close].to_owned())
            })
            .unwrap_or_else(|| "()".to_owned())
    } else {
        let first_lo = func
            .params
            .items
            .first()
            .map(|p| p.span.start)
            .or_else(|| func.params.rest.as_ref().map(|r| r.span.start))
            .unwrap_or(0);
        let last_hi = func
            .params
            .rest
            .as_ref()
            .map(|r| r.span.end)
            .or_else(|| func.params.items.last().map(|p| p.span.end))
            .unwrap_or(0);
        slice_with_paren_padding(source, first_lo, last_hi)
    };
    let return_type = func
        .return_type
        .as_ref()
        .map(|rt| {
            let rt_text = source_slice(source, rt.span);
            format!(" -> {rt_text}")
        })
        .unwrap_or_default();
    let async_prefix = if func.r#async { "async " } else { "" };
    format!("{async_prefix}{name}{params_str}{return_type}")
}

fn function_signature_from_arrow(
    name: &str,
    arrow: &ArrowFunctionExpression<'_>,
    source: &str,
) -> Option<String> {
    let fn_text = source_slice(source, arrow.span);
    let open = fn_text.find('(')?;
    let close = fn_text[open..].find(')')?;
    let params = &fn_text[open..=(open + close)];
    let return_type = arrow
        .return_type
        .as_ref()
        .map(|rt| {
            let rt_text = source_slice(source, rt.span);
            format!(" -> {rt_text}")
        })
        .unwrap_or_default();
    let async_prefix = if arrow.r#async { "async " } else { "" };
    Some(format!("{async_prefix}{name}{params}{return_type}"))
}

fn slice_with_paren_padding(source: &str, first_lo: u32, last_hi: u32) -> String {
    let src_lo = (first_lo as usize).min(source.len());
    let src_hi = (last_hi as usize).min(source.len());
    let prefix = &source[..src_lo];
    let open = prefix.rfind('(').unwrap_or(src_lo);
    let suffix = &source[src_hi..];
    let close_offset = suffix.find(')').unwrap_or(0);
    let close = src_hi + close_offset + 1;
    source[open.min(source.len())..close.min(source.len())].to_owned()
}

fn collect_constructor_deps(class: &Class<'_>, source: &str) -> Vec<String> {
    for element in &class.body.body {
        if let ClassElement::MethodDefinition(method) = element
            && matches!(method.kind, MethodDefinitionKind::Constructor)
        {
            return constructor_deps_from_function(&method.value, source);
        }
    }
    Vec::new()
}

fn constructor_deps_from_function(func: &Function<'_>, source: &str) -> Vec<String> {
    let span = func.params.span;
    let text = source_slice(source, span);
    extract_deps_from_param_text(text)
}

fn collect_implemented_interfaces(class: &Class<'_>) -> Vec<String> {
    class
        .implements
        .iter()
        .filter_map(|implements| ts_type_name_text(&implements.expression))
        .collect()
}

fn ts_type_name_text(name: &TSTypeName<'_>) -> Option<String> {
    match name {
        TSTypeName::IdentifierReference(ident) => Some(ident.name.to_string()),
        TSTypeName::QualifiedName(qualified) => {
            let head = ts_type_name_text(&qualified.left)?;
            let tail = qualified.right.name.to_string();
            Some(format!("{head}.{tail}"))
        }
        TSTypeName::ThisExpression(_) => Some("this".to_owned()),
    }
}

fn scan_matched_paren(open: char, close: char, s: &str, start: usize) -> Option<usize> {
    let mut depth = 0_u32;
    let mut in_single = false;
    let mut in_double = false;
    let mut escape = false;
    let mut saw_open = false;

    for (index, ch) in s.char_indices().skip_while(|(idx, _)| *idx < start) {
        if escape {
            escape = false;
            continue;
        }
        match ch {
            '\\' if in_single || in_double => escape = true,
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            _ if in_single || in_double => {}
            _ if ch == open => {
                depth = depth.saturating_add(1);
                saw_open = true;
            }
            _ if ch == close => {
                depth = depth.saturating_sub(1);
                if saw_open && depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

fn split_top_level(input: &str, separator: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut paren_depth = 0_u32;
    let mut angle_depth = 0_u32;
    let mut bracket_depth = 0_u32;
    let mut brace_depth = 0_u32;
    let mut in_single = false;
    let mut in_double = false;
    let mut escape = false;

    for (index, ch) in input.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        match ch {
            '\\' if in_single || in_double => escape = true,
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            _ if in_single || in_double => {}
            '(' => paren_depth = paren_depth.saturating_add(1),
            ')' => paren_depth = paren_depth.saturating_sub(1),
            '<' => angle_depth = angle_depth.saturating_add(1),
            '>' => angle_depth = angle_depth.saturating_sub(1),
            '[' => bracket_depth = bracket_depth.saturating_add(1),
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            '{' => brace_depth = brace_depth.saturating_add(1),
            '}' => brace_depth = brace_depth.saturating_sub(1),
            _ if ch == separator
                && paren_depth == 0
                && angle_depth == 0
                && bracket_depth == 0
                && brace_depth == 0 =>
            {
                parts.push(input[start..index].trim());
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }
    let tail = input[start..].trim();
    if !tail.is_empty() {
        parts.push(tail);
    }
    parts
}

fn extract_deps_from_param_text(text: &str) -> Vec<String> {
    let Some(open) = text.find('(') else {
        return Vec::new();
    };
    let Some(close) = scan_matched_paren('(', ')', text, open) else {
        return Vec::new();
    };
    let params_text = &text[open + 1..close];
    let mut deps = Vec::new();
    for parameter in split_top_level(params_text, ',') {
        let parameter = parameter.trim();
        if parameter.is_empty() || parameter == "self" {
            continue;
        }
        let has_type_annotation = parameter.contains(':');
        let name = parameter
            .split_once(':')
            .map_or(parameter, |(_, ty)| ty)
            .split('=')
            .next()
            .unwrap_or(parameter)
            .trim()
            .trim_start_matches("private")
            .trim_start_matches("public")
            .trim_start_matches("protected")
            .trim_start_matches("readonly")
            .trim_matches('?')
            .to_owned();
        let name = if has_type_annotation {
            name
        } else {
            name.split_whitespace()
                .last()
                .unwrap_or(parameter)
                .trim_matches('?')
                .to_owned()
        };
        if !name.is_empty() {
            deps.push(name);
        }
    }
    deps
}

fn extract_object_constants(
    prefix: &str,
    obj: &ObjectExpression<'_>,
    constants: &mut Vec<(String, String)>,
) {
    for prop_or_spread in &obj.properties {
        let ObjectPropertyKind::ObjectProperty(prop) = prop_or_spread else {
            continue;
        };
        if !matches!(prop.kind, PropertyKind::Init) {
            continue;
        }
        let key_text = property_key_text(&prop.key);
        if key_text.is_empty() {
            continue;
        }
        let full_key = if prefix.is_empty() {
            key_text.clone()
        } else {
            format!("{prefix}.{key_text}")
        };
        match &prop.value {
            Expression::StringLiteral(s) => {
                constants.push((full_key, s.value.to_string()));
            }
            Expression::ObjectExpression(inner) => {
                extract_object_constants(&full_key, inner, constants);
            }
            _ => {}
        }
    }
}

fn extract_constant_string_value(
    base_name: &str,
    expr: &Expression<'_>,
) -> Option<Vec<(String, String)>> {
    match expr {
        Expression::StringLiteral(s) => Some(vec![(base_name.to_owned(), s.value.to_string())]),
        Expression::ObjectExpression(obj) => {
            let mut constants = Vec::new();
            extract_object_constants(base_name, obj, &mut constants);
            Some(constants)
        }
        _ => None,
    }
}

fn mirror_constant_prefix(
    constants: &mut rustc_hash::FxHashMap<String, String>,
    source_prefix: &str,
    target_prefix: &str,
) {
    let mut mirrored = Vec::new();
    for (key, value) in constants.iter() {
        if key == source_prefix {
            mirrored.push((target_prefix.to_owned(), value.clone()));
            continue;
        }
        if let Some(suffix) = key.strip_prefix(source_prefix)
            && suffix.starts_with('.')
        {
            mirrored.push((format!("{target_prefix}{suffix}"), value.clone()));
        }
    }
    for (key, value) in mirrored {
        constants.insert(key, value);
    }
}

fn push_type_symbol(
    name: &str,
    span: Span,
    signature: String,
    state: &mut ParseState<'_>,
    ctx: &VisitCtx<'_>,
) {
    state.push_symbol(
        NodeKind::Type,
        name.to_owned(),
        Some(name.to_owned()),
        Some(ctx.span(span)),
        Some(signature),
        if ctx.force_exported {
            Some(Visibility::Public)
        } else {
            None
        },
        ctx.parent_class.as_ref().map(|c| c.name.clone()),
        Vec::new(),
        ctx.class_decorators.clone(),
        Vec::new(),
    );
}

// ── Test-support module (preserved for cross-backend parity tests) ──────────

#[cfg(feature = "test-support")]
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

#[cfg(feature = "test-support")]
fn parse_import_bindings_for_test(file: &FileEntry, source: &str) -> Vec<ImportBinding> {
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
                bindings.push(ImportBinding {
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
                });
            }
            _ => {}
        }
    }
    bindings
}

#[cfg(feature = "test-support")]
fn parse_top_level_declared_names(file: &FileEntry, source: &str) -> Vec<String> {
    use std::collections::BTreeSet;

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

    let mut names: BTreeSet<String> = BTreeSet::new();
    for statement in &parsed.program.body {
        match statement {
            Statement::ExportNamedDeclaration(decl) => {
                if let Some(declaration) = decl.declaration.as_ref() {
                    collect_declaration_names(declaration, &mut names);
                }
            }
            Statement::ExportDefaultDeclaration(decl) => match &decl.declaration {
                ExportDefaultDeclarationKind::FunctionDeclaration(func) => {
                    if let Some(ident) = func.id.as_ref() {
                        names.insert(ident.name.to_string());
                    }
                }
                ExportDefaultDeclarationKind::ClassDeclaration(class) => {
                    if let Some(ident) = class.id.as_ref() {
                        names.insert(ident.name.to_string());
                    }
                }
                ExportDefaultDeclarationKind::TSInterfaceDeclaration(ts) => {
                    names.insert(ts.id.name.to_string());
                }
                _ => {}
            },
            other => {
                if let Some(declaration) = other.as_declaration() {
                    collect_declaration_names(declaration, &mut names);
                }
            }
        }
    }
    names.into_iter().collect()
}

#[cfg(feature = "test-support")]
fn collect_declaration_names(
    declaration: &Declaration<'_>,
    names: &mut std::collections::BTreeSet<String>,
) {
    match declaration {
        Declaration::FunctionDeclaration(func) => {
            if let Some(ident) = func.id.as_ref() {
                names.insert(ident.name.to_string());
            }
        }
        Declaration::ClassDeclaration(class) => {
            if let Some(ident) = class.id.as_ref() {
                names.insert(ident.name.to_string());
            }
        }
        Declaration::VariableDeclaration(var) => {
            for declarator in &var.declarations {
                if let BindingPattern::BindingIdentifier(binding) = &declarator.id {
                    names.insert(binding.name.to_string());
                }
            }
        }
        Declaration::TSTypeAliasDeclaration(decl) => {
            names.insert(decl.id.name.to_string());
        }
        Declaration::TSInterfaceDeclaration(decl) => {
            names.insert(decl.id.name.to_string());
        }
        Declaration::TSEnumDeclaration(decl) => {
            names.insert(decl.id.name.to_string());
        }
        Declaration::TSModuleDeclaration(decl) => {
            if let TSModuleDeclarationName::Identifier(ident) = &decl.id {
                names.insert(ident.name.to_string());
            }
        }
        Declaration::TSImportEqualsDeclaration(_) | Declaration::TSGlobalDeclaration(_) => {}
    }
}

#[cfg(feature = "test-support")]
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
        super::parse_import_bindings_for_test(&file, source)
    }

    pub fn top_level_declared_names_for_path(path: &Path, source: &str) -> Vec<String> {
        let file = FileEntry {
            path: path.to_path_buf(),
            language: Language::TypeScript,
            size_bytes: source.len() as u64,
            content_hash: [0u8; 32],
            source_bytes: None,
        };
        super::parse_top_level_declared_names(&file, source)
    }

    /// Drive the full parse + visit pipeline through an extension-routed
    /// `FileEntry` and return whether the extracted `ParseState` exposes at
    /// least one symbol. Used by extension-classification regression tests
    /// that pin `.mts`, `.cts`, and uppercase variants to the TypeScript
    /// parser.
    pub fn parse_ts_file_via_extension(ext: &str, source: &str) -> bool {
        use crate::tree_sitter::ParseState;

        let path = PathBuf::from(format!("test_file.{ext}"));
        let file = FileEntry {
            path,
            language: Language::TypeScript,
            size_bytes: source.len() as u64,
            content_hash: [0u8; 32],
            source_bytes: None,
        };
        let mut state = ParseState::for_test(&file, source);
        super::parse_ts_js_with_oxc(&file, &mut state, source, std::path::Path::new("/tmp"));
        !state.symbols().is_empty()
    }

    /// Drive the full pipeline and report whether the extracted symbols
    /// contain a node whose name equals `ident_name`. Used by parallel-parse
    /// regression tests to assert per-source identity under rayon load.
    pub fn parse_full_pipeline_contains_symbol(ext: &str, source: &str, ident_name: &str) -> bool {
        use crate::tree_sitter::ParseState;

        let path = PathBuf::from(format!("test.{ext}"));
        let file = FileEntry {
            path,
            language: Language::TypeScript,
            size_bytes: source.len() as u64,
            content_hash: [0u8; 32],
            source_bytes: None,
        };
        let mut state = ParseState::for_test(&file, source);
        super::parse_ts_js_with_oxc(&file, &mut state, source, std::path::Path::new("/tmp"));
        state
            .symbols()
            .iter()
            .any(|s| s.node.name.as_str() == ident_name)
    }

    /// Drive the raw parse path (no visitor) and return whether any
    /// top-level declared name matches `ident_name`. The companion to
    /// [`parse_full_pipeline_contains_symbol`] that exercises only the
    /// parser layer for span-cross-talk tests.
    pub fn parse_source_contains_ident(source: &str, ident_name: &str) -> bool {
        let names = top_level_declared_names_for_path(std::path::Path::new("source.ts"), source);
        names.iter().any(|n| n == ident_name)
    }
}

#[cfg(all(test, feature = "test-support"))]
mod tests {
    use std::path::{Path, PathBuf};

    use oxc_span::Span;
    use pretty_assertions::assert_eq;

    use crate::{FileEntry, Language, ts_js_backend::TsJsParseStatus};

    use super::{
        line_offsets, parse_top_level_declared_names, parse_ts_js_for_status, source_type_for_path,
        span_to_source_span,
    };

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

    /// Top-level declared-names parity helper covers exports, plain decls,
    /// the TS-only forms (interface/type/enum/namespace), and JSX-aware
    /// extensions so the parity guard against SWC has a concrete shape it
    /// can compare against.
    #[test]
    fn oxc_top_level_declared_names_covers_export_and_ts_only_forms() {
        let typescript_source = "\
            export const value = 1;\n\
            export function plain() {}\n\
            export class Widget {}\n\
            export interface Shape { kind: string }\n\
            export type Maybe<T> = T | undefined;\n\
            export enum Color { Red, Blue }\n\
            export default function defaultFn() {}\n\
            namespace Outer { export const inner = 0 }\n\
        ";
        let names = parse_top_level_declared_names(&file("decls.ts"), typescript_source);
        assert_eq!(
            names,
            vec![
                "Color".to_owned(),
                "Maybe".to_owned(),
                "Outer".to_owned(),
                "Shape".to_owned(),
                "Widget".to_owned(),
                "defaultFn".to_owned(),
                "plain".to_owned(),
                "value".to_owned(),
            ],
        );

        let react_source = "\
            export default function ProjectionSummary() { return null }\n\
            export interface Props {}\n\
        ";
        let react_names = parse_top_level_declared_names(&file("ui.tsx"), react_source);
        assert_eq!(
            react_names,
            vec!["ProjectionSummary".to_owned(), "Props".to_owned()],
        );
    }
}
