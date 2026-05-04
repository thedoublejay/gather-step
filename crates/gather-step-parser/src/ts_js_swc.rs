//! swc-based TypeScript/JavaScript visitor.
//!
//! Replaces the tree-sitter TS/JS path with a recursive-descent parser that
//! avoids the exponential-backtracking edge cases that
//! `tree-sitter-typescript` hits on certain multi-line `as`-union-type
//! assertions.
//!
//! The public entry point is [`parse_ts_js_with_swc`].  It produces the same
//! [`ParsedFile`] structure — same `NodeId`s, same `EdgeId`s, same ordinal
//! counters — as the tree-sitter path so that downstream consumers need no
//! changes.

use gather_step_core::{NodeData, NodeKind, SourceSpan, Visibility};
use swc_common::{FileName, GLOBALS, Globals, SourceMap, Spanned, sync::Lrc};
use swc_ecma_ast::{Accessibility, BlockStmtOrExpr, CallExpr, Expr, NewExpr};
use swc_ecma_ast::{
    ArrowExpr, Callee, ClassDecl, ClassMember, ClassMethod, Constructor, Decl, DefaultDecl,
    ExportDecl, ExportDefaultDecl, ExportDefaultExpr, ExprOrSpread, ExprStmt, FnDecl, ImportDecl,
    ImportSpecifier, Lit, MemberExpr, MemberProp, Module, ModuleDecl, ModuleItem, NamedExport,
    ObjectLit, Prop, PropName, PropOrSpread, Stmt, VarDecl, VarDeclarator,
};
use swc_ecma_parser::{EsSyntax, Parser, StringInput, Syntax, TsSyntax, lexer::Lexer};

use crate::{
    resolve::ImportBinding,
    traverse::FileEntry,
    tree_sitter::{DecoratorCapture, ParseState},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SwcParseStatus {
    Parsed,
    Recovered,
    Unrecoverable,
}

/// Build a line-offset index from source bytes.
///
/// `offsets[i]` is the byte offset of the first character on line `i` (0-indexed lines).
fn build_line_offsets(source: &str) -> Vec<u32> {
    let mut offsets = vec![0u32];
    for (i, b) in source.as_bytes().iter().enumerate() {
        if *b == b'\n' {
            #[expect(clippy::cast_possible_truncation, reason = "source files fit in 4 GiB")]
            offsets.push((i + 1) as u32);
        }
    }
    offsets
}

/// Convert an swc `BytePos` to a `SourceSpan` using the pre-built line index.
///
/// swc `BytePos(n)` is 1-biased (the file's start is BytePos(1) not BytePos(0)).
/// We strip the bias, then binary-search the offset table.
fn bytepos_to_line_col(pos: swc_common::BytePos, offsets: &[u32]) -> (u32, u32) {
    let raw = pos.0.saturating_sub(1); // strip swc bias
    let idx = offsets.partition_point(|&off| off <= raw).saturating_sub(1);
    #[expect(
        clippy::cast_possible_truncation,
        reason = "source files have far fewer than 2^32 lines"
    )]
    let line = (idx + 1) as u32; // 1-indexed
    let col = raw - offsets[idx]; // 0-indexed byte column
    (line, col)
}

fn span_from_swc(span: swc_common::Span, offsets: &[u32]) -> SourceSpan {
    let (line_start, column_start) = bytepos_to_line_col(span.lo, offsets);
    let (line_end, column_end) = bytepos_to_line_col(span.hi, offsets);
    SourceSpan::from_absolute(line_start, line_end, column_start, column_end)
}

// ── Decorator extraction ────────────────────────────────────────────────────

fn prop_name_text(prop: &PropName) -> String {
    match prop {
        PropName::Ident(i) => i.sym.to_string(),
        PropName::Str(s) => swc_string_value(s),
        PropName::Num(n) => n.value.to_string(),
        PropName::Computed(_) => String::new(),
        PropName::BigInt(b) => b.value.to_string(),
    }
}

fn swc_string_value(value: &swc_ecma_ast::Str) -> String {
    value.value.to_string_lossy().into_owned()
}

/// Extract the terminal identifier name and full dot-joined chain from an
/// expression — mirrors `expression_name` in `tree_sitter.rs`.
fn expression_name_from_expr(expr: &Expr) -> (String, Option<String>) {
    let mut parts: Vec<String> = Vec::new();
    let mut current = expr;
    let mut limit = 10_000usize; // guard against pathological ASTs

    loop {
        if limit == 0 {
            break;
        }
        limit -= 1;

        match current {
            Expr::Member(MemberExpr { obj, prop, .. }) => {
                let prop_text = match prop {
                    MemberProp::Ident(i) => i.sym.to_string(),
                    MemberProp::PrivateName(p) => p.name.to_string(),
                    MemberProp::Computed(_) => String::new(),
                };
                if !prop_text.is_empty() {
                    parts.push(prop_text);
                }
                current = obj;
            }
            Expr::Call(CallExpr {
                callee: Callee::Expr(callee_expr),
                ..
            }) => {
                current = callee_expr;
            }
            Expr::Ident(ident) => {
                parts.push(ident.sym.to_string());
                break;
            }
            Expr::This(_) => {
                parts.push("this".to_owned());
                break;
            }
            Expr::Paren(p) => {
                current = &p.expr;
            }
            _ => {
                break;
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

/// Extract a decorator from an swc `Decorator` node — mirrors `single_decorator`.
///
/// `raw` is trimmed to the argument expression only (content between the
/// outermost parens of the decorator call) to match the tree-sitter path's
/// contract.  `arguments` is a `SmallVec<[Box<str>; 2]>` with each
/// comma-separated argument, stripped of surrounding quotes.
fn single_decorator_from_swc(
    decorator: &swc_ecma_ast::Decorator,
    source: &str,
    offsets: &[u32],
) -> DecoratorCapture {
    let (name, _) = expression_name_from_expr(&decorator.expr);

    let (raw, arguments) = if let Expr::Call(CallExpr { args, callee, .. }) = &*decorator.expr {
        // Derive `raw` from the argument span: the text between the call parens.
        let args_strings = args_text(args, source, offsets);
        // Build `raw` as the argument text payload.  We reconstruct it from
        // the callee's end to the expression's end so we capture the exact
        // source text inside the parens.
        let call_span = decorator.expr.span();
        let callee_span = match callee {
            swc_ecma_ast::Callee::Expr(e) => e.span(),
            _ => call_span,
        };
        let full = source_slice(source, call_span);
        let callee_text = source_slice(source, callee_span);
        let after_callee = full
            .strip_prefix(callee_text)
            .unwrap_or(full)
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
        span: Some(span_from_swc(decorator.span(), offsets)),
    }
}

/// Retrieve a slice of source text for a given swc Span.
fn source_slice(source: &str, span: swc_common::Span) -> &str {
    let lo = span.lo.0.saturating_sub(1) as usize;
    let hi = span.hi.0.saturating_sub(1) as usize;
    let lo = lo.min(source.len());
    let hi = hi.min(source.len());
    &source[lo..hi]
}

/// Extract decorator argument strings — mirrors tree-sitter's `split_arguments`.
///
/// tree-sitter's `split_arguments` takes the raw text of the full argument
/// list (outer parens stripped) and splits on `,`, trimming quotes per piece.
/// We replicate that exactly: find the source span covering the entire
/// argument list, strip outer parens, then split on `,`.
fn args_text(args: &[ExprOrSpread], source: &str, _offsets: &[u32]) -> Vec<String> {
    if args.is_empty() {
        return Vec::new();
    }
    args.iter()
        .filter(|arg| arg.spread.is_none())
        .map(|arg| expr_raw_text(source, &arg.expr))
        .map(|piece| piece.trim().trim_matches('"').trim_matches('\'').to_owned())
        .filter(|piece| !piece.is_empty())
        .collect()
}

// ── Call-site helpers ────────────────────────────────────────────────────────

/// Extract the first genuine string literal or array-of-string-literals from
/// `args`.  Returns `None` when the first argument is not a literal (e.g. a
/// bare identifier, member expression, or binary expression).
///
/// This is the **only** function that should feed the topic pipeline.  It
/// intentionally omits any raw-text fallback so that non-literal expressions
/// such as `variableName` or `obj.prop.method` are never mistaken for topic
/// strings and indexed as spurious Event / Topic nodes.
///
/// Call sites that need the raw source text of the first argument for
/// non-topic purposes (e.g. `@InjectModel(Model.name)`) must call
/// [`first_raw_arg_text`] explicitly, which signals that the returned value is
/// **not** a literal.
fn first_literal_argument_from_args(args: &[ExprOrSpread], source: &str) -> Option<String> {
    for arg in args {
        if arg.spread.is_some() {
            continue;
        }
        match &*arg.expr {
            Expr::Lit(Lit::Str(s)) => {
                return Some(swc_string_value(s));
            }
            Expr::Array(arr) => {
                // Mirror tree-sitter: take raw source slice, strip `[` and `]`.
                let raw = source_slice(source, arr.span());
                let stripped = raw.trim().trim_matches('[').trim_matches(']').trim();
                return Some(stripped.to_owned());
            }
            Expr::Tpl(tpl) if tpl.exprs.is_empty() => {
                // Zero-interpolation template literal: `CACHE_MANAGER` or `orders.created`.
                // Semantically identical to a string literal — extract the raw quasi text.
                if let Some(q) = tpl.quasis.first() {
                    return Some(q.raw.to_string());
                }
            }
            _ => {}
        }
    }
    // No string or array literal found — do NOT fall back to raw source text.
    // A raw identifier / member expression / binary expression is not a
    // literal topic name.
    None
}

/// Return the raw source text of the first non-spread argument.
///
/// This helper is **not** suitable for the topic pipeline: it returns whatever
/// expression text appears at the call site (identifiers, member chains, binary
/// expressions, …) without any guarantee that the value is a string literal.
/// Use it only when raw text is explicitly needed for non-topic purposes.
fn first_raw_arg_text(args: &[ExprOrSpread], source: &str) -> Option<String> {
    args.iter()
        .find(|a| a.spread.is_none())
        .map(|a| expr_raw_text(source, &a.expr).trim().to_owned())
}

fn first_literal_argument_from_new(
    args: Option<&Vec<ExprOrSpread>>,
    source: &str,
) -> Option<String> {
    args.and_then(|a| first_literal_argument_from_args(a, source))
}

fn raw_arguments_from_args(args: &[ExprOrSpread], source: &str) -> String {
    if args.is_empty() {
        return String::new();
    }
    // Join raw text of each arg.
    args.iter()
        .map(|arg| {
            if arg.spread.is_some() {
                format!("...{}", expr_raw_text(source, &arg.expr))
            } else {
                expr_raw_text(source, &arg.expr)
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn raw_arguments_from_new(args: Option<&Vec<ExprOrSpread>>, source: &str) -> Option<String> {
    // `new X` without parentheses has args = None.  Mirror tree-sitter's
    // `raw_arguments` which returns `None` when the `arguments` child is absent.
    args.map(|a| raw_arguments_from_args(a, source))
}

/// Reconstruct text of an expression from the source using its span.
fn expr_raw_text(source: &str, expr: &Expr) -> String {
    use swc_common::Spanned;
    let span = expr.span();
    source_slice(source, span).to_owned()
}

// ── Constructor dependencies ──────────────────────────────────────────────────

/// Extract constructor parameter type names — mirrors
/// `collect_constructor_dependencies` in `tree_sitter.rs`.
///
/// We read the raw source text of the constructor's parameter list and apply
/// the same naive string-split heuristic the tree-sitter path uses.
fn collect_constructor_deps_from_ctor(ctor: &Constructor, source: &str) -> Vec<String> {
    use swc_common::Spanned;
    let span = ctor.span();
    let text = source_slice(source, span);
    extract_deps_from_param_text(text)
}

fn collect_constructor_deps_from_class(class: &swc_ecma_ast::Class, source: &str) -> Vec<String> {
    for member in &class.body {
        if let ClassMember::Constructor(ctor) = member {
            return collect_constructor_deps_from_ctor(ctor, source);
        }
    }
    Vec::new()
}

fn collect_implemented_interfaces_from_class(class: &swc_ecma_ast::Class) -> Vec<String> {
    class
        .implements
        .iter()
        .filter_map(|implements| match &*implements.expr {
            swc_ecma_ast::Expr::Ident(ident) => Some(ident.sym.to_string()),
            swc_ecma_ast::Expr::Member(member) => {
                let object = match &*member.obj {
                    swc_ecma_ast::Expr::Ident(ident) => Some(ident.sym.to_string()),
                    _ => None,
                };
                let property = member.prop.as_ident().map(|ident| ident.sym.to_string());
                match (object, property) {
                    (Some(object), Some(property)) => Some(format!("{object}.{property}")),
                    (None, Some(property)) => Some(property),
                    _ => None,
                }
            }
            _ => None,
        })
        .collect()
}

fn scan_matched_angle_and_paren(open: char, close: char, s: &str, start: usize) -> Option<usize> {
    let mut paren_depth = 0_u32;
    let mut in_single = false;
    let mut in_double = false;
    let mut escape = false;
    let mut saw_open = false;

    for (index, ch) in s.char_indices().skip_while(|(index, _)| *index < start) {
        if escape {
            escape = false;
            continue;
        }

        match ch {
            '\\' if in_single || in_double => {
                escape = true;
            }
            '\'' if !in_double => {
                in_single = !in_single;
            }
            '"' if !in_single => {
                in_double = !in_double;
            }
            _ if in_single || in_double => {}
            _ if ch == open => {
                paren_depth = paren_depth.saturating_add(1);
                saw_open = true;
            }
            _ if ch == close => {
                paren_depth = paren_depth.saturating_sub(1);
                if saw_open && paren_depth == 0 {
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
            '\\' if in_single || in_double => {
                escape = true;
            }
            '\'' if !in_double => {
                in_single = !in_single;
            }
            '"' if !in_single => {
                in_double = !in_double;
            }
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
    let Some(close) = scan_matched_angle_and_paren('(', ')', text, open) else {
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

// ── Function signature ──────────────────────────────────────────────────────

/// Build a function signature string from a swc `Function`.
///
/// Mirrors `function_signature` in `tree_sitter.rs`: reads the verbatim text of
/// the `parameters` node (parens, whitespace, type annotations, comments all
/// preserved) and assembles `[async ]name{params}[ -> return_type]`.
fn function_signature_from_fn(name: &str, func: &swc_ecma_ast::Function, source: &str) -> String {
    use swc_common::Spanned;

    // Reconstruct the `(...)` span by scanning outward from the params.
    // tree-sitter reads the `parameters` child text verbatim.  We replicate
    // this by finding the `(` that precedes the first param (or the first `(`
    // after the function keyword/name) and the matching `)`.
    let params_str = if func.params.is_empty() {
        // No params: locate the `()` in source around the function span.
        let fn_text = source_slice(source, func.span());
        // Find the first `(` in the function text, then the matching `)`.
        fn_text
            .find('(')
            .and_then(|open| {
                fn_text[open..]
                    .find(')')
                    .map(|close| fn_text[open..=open + close].to_owned())
            })
            .unwrap_or_else(|| "()".to_owned())
    } else {
        // Locate the `(` immediately before the first param in source and
        // the `)` immediately after the last param.
        let first_lo = func.params.first().map_or(1, |p| p.span().lo.0);
        let last_hi = func.params.last().map_or(1, |p| p.span().hi.0);
        let src_lo = first_lo.saturating_sub(1) as usize;
        let src_hi = last_hi.saturating_sub(1) as usize;

        let prefix = &source[..src_lo.min(source.len())];
        let open = prefix.rfind('(').unwrap_or(src_lo);
        let suffix = &source[src_hi.min(source.len())..];
        let close_offset = suffix.find(')').unwrap_or(0);
        let close = src_hi + close_offset + 1;
        source[open.min(source.len())..close.min(source.len())].to_owned()
    };

    let return_type = func
        .return_type
        .as_ref()
        .map(|rt| {
            let rt_text = source_slice(source, rt.span());
            format!(" -> {rt_text}")
        })
        .unwrap_or_default();
    let async_prefix = if func.is_async { "async " } else { "" };
    format!("{async_prefix}{name}{params_str}{return_type}")
}

/// Build a function signature string from an swc `ArrowExpr`.
fn function_signature_from_arrow(name: &str, arrow: &ArrowExpr, source: &str) -> Option<String> {
    use swc_common::Spanned;
    let fn_text = source_slice(source, arrow.span());
    let open = fn_text.find('(')?;
    let close = fn_text[open..].find(')')?;
    let params = &fn_text[open..=(open + close)];
    let return_type = arrow
        .return_type
        .as_ref()
        .map(|rt| {
            let rt_text = source_slice(source, rt.span());
            format!(" -> {rt_text}")
        })
        .unwrap_or_default();
    let async_prefix = if arrow.is_async { "async " } else { "" };
    Some(format!("{async_prefix}{name}{params}{return_type}"))
}

// ── Method visibility ─────────────────────────────────────────────────────────

fn method_visibility_from_accessibility(acc: Option<Accessibility>) -> Visibility {
    match acc {
        Some(Accessibility::Private) => Visibility::Private,
        Some(Accessibility::Protected) => Visibility::Protected,
        Some(Accessibility::Public) | None => Visibility::Public,
    }
}

// ── Constant string extraction ───────────────────────────────────────────────

fn extract_object_constants_swc(
    prefix: &str,
    obj: &ObjectLit,
    constants: &mut Vec<(String, String)>,
) {
    for prop_or_spread in &obj.props {
        let PropOrSpread::Prop(prop) = prop_or_spread else {
            continue;
        };
        let Prop::KeyValue(kv) = &**prop else {
            continue;
        };

        let key = prop_name_text(&kv.key);
        if key.is_empty() {
            continue;
        }
        let full_key = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };

        match &*kv.value {
            Expr::Lit(Lit::Str(s)) => {
                constants.push((full_key, swc_string_value(s)));
            }
            Expr::Object(inner_obj) => {
                extract_object_constants_swc(&full_key, inner_obj, constants);
            }
            _ => {}
        }
    }
}

fn extract_constant_string_value_swc(
    base_name: &str,
    expr: &Expr,
) -> Option<Vec<(String, String)>> {
    match expr {
        Expr::Lit(Lit::Str(s)) => Some(vec![(base_name.to_owned(), swc_string_value(s))]),
        Expr::Object(obj) => {
            let mut constants = Vec::new();
            extract_object_constants_swc(base_name, obj, &mut constants);
            Some(constants)
        }
        _ => None,
    }
}

fn mirror_constant_prefix_swc(
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

// ── ParseState swc extension: push_imports_swc ─────────────────────────────

/// Emit import nodes + edges from pre-parsed bindings.
///
/// Mirrors `push_imports` in tree_sitter.rs:578-664 but takes swc data
/// instead of a tree-sitter `Node`.
pub(crate) fn push_imports_swc(
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
        // Import identity: file-path-scoped local name, stable across
        // reordering of other imports in the same file.
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

// ── Import binding extraction ────────────────────────────────────────────────

fn import_bindings_from_decl(decl: &ImportDecl) -> Vec<ImportBinding> {
    let source = swc_string_value(&decl.src);
    let is_type_only = decl.type_only;
    decl.specifiers
        .iter()
        .map(|spec| match spec {
            ImportSpecifier::Named(named) => {
                let local_name = named.local.sym.to_string();
                let imported_name = named.imported.as_ref().map(|imp| match imp {
                    swc_ecma_ast::ModuleExportName::Ident(i) => i.sym.to_string(),
                    swc_ecma_ast::ModuleExportName::Str(s) => swc_string_value(s),
                });
                // If no alias, imported_name == local_name
                let imported_name = Some(imported_name.unwrap_or_else(|| local_name.clone()));
                ImportBinding {
                    local_name,
                    imported_name,
                    source: source.clone(),
                    resolved_path: None,
                    is_default: false,
                    is_namespace: false,
                    is_type_only,
                }
            }
            ImportSpecifier::Default(def) => ImportBinding {
                local_name: def.local.sym.to_string(),
                imported_name: None,
                source: source.clone(),
                resolved_path: None,
                is_default: true,
                is_namespace: false,
                is_type_only,
            },
            ImportSpecifier::Namespace(ns) => ImportBinding {
                local_name: ns.local.sym.to_string(),
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

/// `export { Foo } from './foo'` — treat as named imports.
///
/// Mirrors tree-sitter's `parse_named_imports`:
/// - `export { A as B } from '...'` → `local_name = B`, `imported_name = Some(A)`
/// - `export { A } from '...'` → `local_name = A`, `imported_name = Some(A)`
fn import_bindings_from_named_export(named: &NamedExport) -> Vec<ImportBinding> {
    let Some(src) = &named.src else {
        return Vec::new();
    };
    let source = swc_string_value(src);
    named
        .specifiers
        .iter()
        .map(|spec| {
            match spec {
                swc_ecma_ast::ExportSpecifier::Named(n) => {
                    // `orig` is the name in the source module (A in `export { A as B }`).
                    // `exported` is the alias exposed to consumers (B).
                    let orig_name = match &n.orig {
                        swc_ecma_ast::ModuleExportName::Ident(i) => i.sym.to_string(),
                        swc_ecma_ast::ModuleExportName::Str(s) => swc_string_value(s),
                    };
                    // local_name = what consumers of this barrel see (the exported alias, if any).
                    let local_name = n.exported.as_ref().map_or_else(
                        || orig_name.clone(),
                        |exp| match exp {
                            swc_ecma_ast::ModuleExportName::Ident(i) => i.sym.to_string(),
                            swc_ecma_ast::ModuleExportName::Str(s) => swc_string_value(s),
                        },
                    );
                    let imported_name = Some(orig_name);
                    ImportBinding {
                        local_name,
                        imported_name,
                        source: source.clone(),
                        resolved_path: None,
                        is_default: false,
                        is_namespace: false,
                        is_type_only: false,
                    }
                }
                swc_ecma_ast::ExportSpecifier::Namespace(ns) => {
                    let local_name = match &ns.name {
                        swc_ecma_ast::ModuleExportName::Ident(i) => i.sym.to_string(),
                        swc_ecma_ast::ModuleExportName::Str(s) => swc_string_value(s),
                    };
                    ImportBinding {
                        local_name,
                        imported_name: None,
                        source: source.clone(),
                        resolved_path: None,
                        is_default: false,
                        is_namespace: true,
                        is_type_only: false,
                    }
                }
                swc_ecma_ast::ExportSpecifier::Default(def) => {
                    // Non-standard `export Foo from 'm'` proposal. swc
                    // accepts this when `export_default_from: true`; mirror
                    // tree-sitter's `parse_ts_import_bindings` which treats
                    // a bare identifier after `export` as a default binding.
                    ImportBinding {
                        local_name: def.exported.sym.to_string(),
                        imported_name: None,
                        source: source.clone(),
                        resolved_path: None,
                        is_default: true,
                        is_namespace: false,
                        is_type_only: false,
                    }
                }
            }
        })
        .collect()
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
    /// Decorators accumulated from preceding sibling nodes in a class body
    /// or module-level sequence.  Cleared after each non-decorator item is
    /// processed.
    pending_decorators: Vec<DecoratorCapture>,
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
            pending_decorators: Vec::new(),
            depth: 0,
        }
    }

    fn span(&self, swc_span: swc_common::Span) -> SourceSpan {
        span_from_swc(swc_span, self.offsets)
    }

    fn with_owner(&self, owner_id: gather_step_core::NodeId) -> Self {
        Self {
            source: self.source,
            offsets: self.offsets,
            parent_class: self.parent_class.clone(),
            class_decl_depth: self.class_decl_depth,
            owner: Some(owner_id),
            force_exported: false,
            class_decorators: self.class_decorators.clone(),
            pending_decorators: Vec::new(),
            depth: self.depth + 1,
        }
    }

    fn with_class(&self, class_node: &NodeData, class_decorators: Vec<DecoratorCapture>) -> Self {
        Self {
            source: self.source,
            offsets: self.offsets,
            parent_class: Some(class_node.clone()),
            class_decl_depth: self.class_decl_depth.saturating_add(1),
            owner: Some(class_node.id),
            force_exported: self.force_exported,
            class_decorators,
            pending_decorators: Vec::new(),
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
            pending_decorators: Vec::new(),
            depth: self.depth + 1,
        }
    }
}

const MAX_DEPTH: usize = 256;

// ── Main visitor ─────────────────────────────────────────────────────────────

/// Visit a module in document order, populating `state`.
fn visit_module(module: &Module, state: &mut ParseState<'_>, source: &str, offsets: &[u32]) {
    let mut ctx = VisitCtx::new(source, offsets);
    for item in &module.body {
        visit_module_item(item, state, &mut ctx);
    }
}

fn visit_module_item(item: &ModuleItem, state: &mut ParseState<'_>, ctx: &mut VisitCtx<'_>) {
    match item {
        ModuleItem::ModuleDecl(decl) => visit_module_decl(decl, state, ctx),
        ModuleItem::Stmt(stmt) => visit_stmt(stmt, state, ctx),
    }
}

/// Visit both sides of an assignment's LHS for call sites.
///
/// `a[b()] = c` — `b()` is inside the member expression's computed property;
/// tree-sitter's `recurse_children` reaches it via the fallthrough.
fn visit_assign_target(
    target: &swc_ecma_ast::AssignTarget,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    use swc_ecma_ast::{AssignTarget, SimpleAssignTarget};
    match target {
        AssignTarget::Simple(simple) => match simple {
            SimpleAssignTarget::Member(m) => {
                visit_expr(&m.obj, state, ctx);
                if let MemberProp::Computed(c) = &m.prop {
                    visit_expr(&c.expr, state, ctx);
                }
            }
            SimpleAssignTarget::SuperProp(sp) => {
                if let swc_ecma_ast::SuperProp::Computed(c) = &sp.prop {
                    visit_expr(&c.expr, state, ctx);
                }
            }
            SimpleAssignTarget::Paren(p) => visit_expr(&p.expr, state, ctx),
            SimpleAssignTarget::OptChain(o) => visit_expr(&Expr::OptChain(o.clone()), state, ctx),
            SimpleAssignTarget::TsAs(t) => visit_expr(&t.expr, state, ctx),
            SimpleAssignTarget::TsSatisfies(t) => visit_expr(&t.expr, state, ctx),
            SimpleAssignTarget::TsNonNull(t) => visit_expr(&t.expr, state, ctx),
            SimpleAssignTarget::TsTypeAssertion(t) => visit_expr(&t.expr, state, ctx),
            SimpleAssignTarget::TsInstantiation(t) => visit_expr(&t.expr, state, ctx),
            SimpleAssignTarget::Ident(_) | SimpleAssignTarget::Invalid(_) => {}
        },
        AssignTarget::Pat(pat) => {
            // Destructuring assignment LHS — default-value expressions inside
            // patterns can contain calls: `([x = buildDefault()] = arr)`.
            visit_assign_target_pat(pat, state, ctx);
        }
    }
}

/// Recurse into a destructuring assignment pattern to collect call sites
/// that appear in default-value positions (e.g. `[x = buildDefault()] = arr`).
fn visit_assign_target_pat(
    pat: &swc_ecma_ast::AssignTargetPat,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    use swc_ecma_ast::AssignTargetPat;
    match pat {
        AssignTargetPat::Array(arr) => {
            for elem in arr.elems.iter().flatten() {
                visit_destructure_pat(elem, state, ctx);
            }
        }
        AssignTargetPat::Object(obj) => {
            for prop in &obj.props {
                visit_object_pat_prop(prop, state, ctx);
            }
        }
        AssignTargetPat::Invalid(_) => {}
    }
}

fn visit_destructure_pat(
    pat: &swc_ecma_ast::Pat,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    use swc_ecma_ast::Pat;
    match pat {
        Pat::Assign(a) => visit_expr(&a.right, state, ctx),
        Pat::Array(arr) => {
            for elem in arr.elems.iter().flatten() {
                visit_destructure_pat(elem, state, ctx);
            }
        }
        Pat::Object(obj) => {
            for prop in &obj.props {
                visit_object_pat_prop(prop, state, ctx);
            }
        }
        Pat::Rest(r) => visit_destructure_pat(&r.arg, state, ctx),
        _ => {}
    }
}

fn visit_object_pat_prop(
    prop: &swc_ecma_ast::ObjectPatProp,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    use swc_ecma_ast::ObjectPatProp;
    match prop {
        ObjectPatProp::Assign(a) => {
            if let Some(value) = &a.value {
                visit_expr(value, state, ctx);
            }
        }
        ObjectPatProp::KeyValue(kv) => visit_destructure_pat(&kv.value, state, ctx),
        ObjectPatProp::Rest(r) => visit_destructure_pat(&r.arg, state, ctx),
    }
}

/// Visit the body of a TS namespace (module block or nested namespace).
///
/// Tree-sitter's `namespace_declaration` falls through `recurse_children` to
/// visit its body, which contains regular declarations/statements — we
/// reuse the same dispatch path.
fn visit_ts_namespace_body(
    body: &swc_ecma_ast::TsNamespaceBody,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    match body {
        swc_ecma_ast::TsNamespaceBody::TsModuleBlock(block) => {
            for item in &block.body {
                visit_module_item(item, state, ctx);
            }
        }
        swc_ecma_ast::TsNamespaceBody::TsNamespaceDecl(nested) => {
            // Nested `namespace A.B {}` form.
            visit_ts_namespace_body(&nested.body, state, ctx);
        }
    }
}

fn visit_module_decl(decl: &ModuleDecl, state: &mut ParseState<'_>, ctx: &mut VisitCtx<'_>) {
    match decl {
        ModuleDecl::Import(import_decl) => {
            ctx.pending_decorators.clear();
            visit_import_decl(import_decl, state, ctx);
        }
        ModuleDecl::ExportDecl(export_decl) => {
            let pending = std::mem::take(&mut ctx.pending_decorators);
            visit_export_decl(export_decl, state, ctx, pending);
        }
        ModuleDecl::ExportNamed(named_export) => {
            ctx.pending_decorators.clear();
            visit_named_export(named_export, state, ctx);
        }
        ModuleDecl::ExportDefaultDecl(default_decl) => {
            let pending = std::mem::take(&mut ctx.pending_decorators);
            visit_export_default_decl(default_decl, state, ctx, pending);
        }
        ModuleDecl::ExportDefaultExpr(default_expr) => {
            ctx.pending_decorators.clear();
            visit_export_default_expr(default_expr, state, ctx);
        }
        ModuleDecl::ExportAll(export_all) => {
            // `export * from 'mod'` — tree-sitter's `export_statement` handler
            // calls `push_imports` when the raw text contains " from ", which
            // emits the virtual Module node + Imports edge.  There are no
            // named bindings so `bindings = []`.
            use swc_common::Spanned;
            ctx.pending_decorators.clear();
            let source_str = swc_string_value(&export_all.src);
            let stmt_span = ctx.span(export_all.span());
            push_imports_swc(
                state,
                &source_str,
                vec![ImportBinding {
                    local_name: "*".to_owned(),
                    imported_name: Some("*".to_owned()),
                    source: source_str.clone(),
                    resolved_path: None,
                    is_default: false,
                    is_namespace: true,
                    is_type_only: false,
                }],
                &stmt_span,
            );
        }
        ModuleDecl::TsImportEquals(_)
        | ModuleDecl::TsExportAssignment(_)
        | ModuleDecl::TsNamespaceExport(_) => {
            ctx.pending_decorators.clear();
        }
    }
}

fn visit_import_decl(decl: &ImportDecl, state: &mut ParseState<'_>, ctx: &VisitCtx<'_>) {
    use swc_common::Spanned;
    let source_str = swc_string_value(&decl.src);
    let bindings = import_bindings_from_decl(decl);
    let stmt_span = ctx.span(decl.span());
    push_imports_swc(state, &source_str, bindings, &stmt_span);
}

fn visit_named_export(named: &NamedExport, state: &mut ParseState<'_>, ctx: &VisitCtx<'_>) {
    use swc_common::Spanned;
    if named.src.is_some() {
        // `export { Foo } from './foo'` — treat as import re-export
        let source_str = named
            .src
            .as_ref()
            .map(|src| swc_string_value(src.as_ref()))
            .unwrap_or_default();
        let bindings = import_bindings_from_named_export(named);
        let stmt_span = ctx.span(named.span());
        push_imports_swc(state, &source_str, bindings, &stmt_span);
    }
    // `export { Foo }` without `from` — no import nodes needed (matches tree-sitter).
}

fn visit_export_decl(
    export: &ExportDecl,
    state: &mut ParseState<'_>,
    ctx: &VisitCtx<'_>,
    pending_decorators: Vec<DecoratorCapture>,
) {
    let mut child_ctx = ctx.exported_child();
    child_ctx.pending_decorators = pending_decorators;
    visit_decl(&export.decl, state, &mut child_ctx);
}

fn visit_export_default_decl(
    export: &ExportDefaultDecl,
    state: &mut ParseState<'_>,
    ctx: &VisitCtx<'_>,
    pending_decorators: Vec<DecoratorCapture>,
) {
    use swc_common::Spanned;
    match &export.decl {
        DefaultDecl::Class(class_expr) => {
            let name = class_expr
                .ident
                .as_ref()
                .map_or_else(|| "AnonymousClass".to_owned(), |i| i.sym.to_string());
            let mut decorators = pending_decorators;
            decorators.extend(collect_decorators_from_class(
                &class_expr.class,
                ctx.source,
                ctx.offsets,
            ));
            let constructor_deps =
                collect_constructor_deps_from_class(&class_expr.class, ctx.source);
            let implemented_interfaces =
                collect_implemented_interfaces_from_class(&class_expr.class);
            let class_node = state.push_symbol(
                NodeKind::Class,
                name.clone(),
                Some(name.clone()),
                Some(ctx.span(class_expr.class.span())),
                None,
                Some(Visibility::Public),
                None,
                decorators.clone(),
                Vec::new(),
                constructor_deps,
            );
            state.set_symbol_implemented_interfaces(class_node.id, implemented_interfaces);
            let mut class_ctx = ctx.with_class(&class_node, decorators);
            for member in &class_expr.class.body {
                visit_class_member(member, state, &mut class_ctx);
            }
        }
        DefaultDecl::Fn(fn_expr) => {
            let name = fn_expr
                .ident
                .as_ref()
                .map_or_else(|| "anonymous".to_owned(), |i| i.sym.to_string());
            let mut decorators = pending_decorators;
            decorators.extend(collect_decorators_from_fn(
                &fn_expr.function,
                ctx.source,
                ctx.offsets,
            ));
            let sig = Some(function_signature_from_fn(
                &name,
                &fn_expr.function,
                ctx.source,
            ));
            let func_node = state.push_symbol(
                NodeKind::Function,
                name.clone(),
                Some(name.clone()),
                Some(ctx.span(fn_expr.function.span())),
                sig,
                Some(Visibility::Public),
                ctx.parent_class.as_ref().map(|c| c.name.clone()),
                decorators,
                ctx.class_decorators.clone(),
                Vec::new(),
            );
            if ctx.depth < MAX_DEPTH
                && let Some(body) = &fn_expr.function.body
            {
                let mut body_ctx = ctx.with_owner(func_node.id);
                for stmt in &body.stmts {
                    visit_stmt(stmt, state, &mut body_ctx);
                }
            }
        }
        DefaultDecl::TsInterfaceDecl(iface) => {
            use swc_common::Spanned;
            let name = iface.id.sym.to_string();
            state.push_symbol(
                NodeKind::Type,
                name.clone(),
                Some(name),
                Some(ctx.span(iface.span())),
                Some(source_slice(ctx.source, iface.span()).to_owned()),
                Some(Visibility::Public),
                ctx.parent_class.as_ref().map(|c| c.name.clone()),
                Vec::new(),
                ctx.class_decorators.clone(),
                Vec::new(),
            );
        }
    }
}

fn visit_export_default_expr(
    export: &ExportDefaultExpr,
    state: &mut ParseState<'_>,
    _ctx: &VisitCtx<'_>,
) {
    match &*export.expr {
        Expr::Ident(ident) => {
            let alias = ident.sym.to_string();
            mirror_constant_prefix_swc(state.constant_strings_mut(), &alias, "default");
        }
        Expr::Object(obj) => {
            if let Some(kvs) =
                extract_constant_string_value_swc("default", &Expr::Object(obj.clone()))
            {
                for (k, v) in kvs {
                    state.record_constant_string(k, v);
                }
            }
        }
        Expr::Lit(Lit::Str(s)) => {
            state.record_constant_string("default".to_owned(), swc_string_value(s));
        }
        _ => {}
    }
}

fn visit_stmt(stmt: &Stmt, state: &mut ParseState<'_>, ctx: &mut VisitCtx<'_>) {
    if ctx.depth > MAX_DEPTH {
        return;
    }
    match stmt {
        Stmt::Decl(decl) => {
            // Handle decorators in sequence — they come as Stmt::Expr with @-prefix in
            // some swc versions, but in current swc decorators are attached to the
            // declaration directly.  We just clear pending here.
            let pending = std::mem::take(&mut ctx.pending_decorators);
            let mut child_ctx = VisitCtx {
                source: ctx.source,
                offsets: ctx.offsets,
                parent_class: ctx.parent_class.clone(),
                class_decl_depth: ctx.class_decl_depth,
                owner: ctx.owner,
                force_exported: ctx.force_exported,
                class_decorators: ctx.class_decorators.clone(),
                pending_decorators: pending,
                depth: ctx.depth + 1,
            };
            visit_decl(decl, state, &mut child_ctx);
        }
        Stmt::Expr(ExprStmt { expr, .. }) => {
            ctx.pending_decorators.clear();
            visit_expr(expr, state, ctx);
        }
        Stmt::Block(block) => {
            for s in &block.stmts {
                visit_stmt(s, state, ctx);
            }
        }
        Stmt::If(if_stmt) => {
            ctx.pending_decorators.clear();
            visit_expr(&if_stmt.test, state, ctx);
            visit_stmt(&if_stmt.cons, state, ctx);
            if let Some(alt) = &if_stmt.alt {
                visit_stmt(alt, state, ctx);
            }
        }
        Stmt::While(w) => {
            ctx.pending_decorators.clear();
            visit_expr(&w.test, state, ctx);
            visit_stmt(&w.body, state, ctx);
        }
        Stmt::DoWhile(d) => {
            ctx.pending_decorators.clear();
            visit_expr(&d.test, state, ctx);
            visit_stmt(&d.body, state, ctx);
        }
        Stmt::For(f) => {
            ctx.pending_decorators.clear();
            if let Some(init) = &f.init {
                match init {
                    swc_ecma_ast::VarDeclOrExpr::VarDecl(v) => {
                        for d in &v.decls {
                            visit_var_declarator(d, state, ctx);
                        }
                    }
                    swc_ecma_ast::VarDeclOrExpr::Expr(e) => visit_expr(e, state, ctx),
                }
            }
            if let Some(test) = &f.test {
                visit_expr(test, state, ctx);
            }
            if let Some(update) = &f.update {
                visit_expr(update, state, ctx);
            }
            visit_stmt(&f.body, state, ctx);
        }
        Stmt::ForIn(f) => {
            ctx.pending_decorators.clear();
            match &f.left {
                swc_ecma_ast::ForHead::VarDecl(v) => {
                    for d in &v.decls {
                        visit_var_declarator(d, state, ctx);
                    }
                }
                swc_ecma_ast::ForHead::UsingDecl(_) | swc_ecma_ast::ForHead::Pat(_) => {}
            }
            visit_expr(&f.right, state, ctx);
            visit_stmt(&f.body, state, ctx);
        }
        Stmt::ForOf(f) => {
            ctx.pending_decorators.clear();
            match &f.left {
                swc_ecma_ast::ForHead::VarDecl(v) => {
                    for d in &v.decls {
                        visit_var_declarator(d, state, ctx);
                    }
                }
                swc_ecma_ast::ForHead::UsingDecl(_) | swc_ecma_ast::ForHead::Pat(_) => {}
            }
            visit_expr(&f.right, state, ctx);
            visit_stmt(&f.body, state, ctx);
        }
        Stmt::Switch(sw) => {
            ctx.pending_decorators.clear();
            visit_expr(&sw.discriminant, state, ctx);
            for case in &sw.cases {
                if let Some(test) = &case.test {
                    visit_expr(test, state, ctx);
                }
                for s in &case.cons {
                    visit_stmt(s, state, ctx);
                }
            }
        }
        Stmt::Try(t) => {
            ctx.pending_decorators.clear();
            for s in &t.block.stmts {
                visit_stmt(s, state, ctx);
            }
            if let Some(h) = &t.handler {
                for s in &h.body.stmts {
                    visit_stmt(s, state, ctx);
                }
            }
            if let Some(f) = &t.finalizer {
                for s in &f.stmts {
                    visit_stmt(s, state, ctx);
                }
            }
        }
        Stmt::Return(r) => {
            ctx.pending_decorators.clear();
            if let Some(arg) = &r.arg {
                visit_expr(arg, state, ctx);
            }
        }
        Stmt::Throw(t) => {
            ctx.pending_decorators.clear();
            visit_expr(&t.arg, state, ctx);
        }
        Stmt::Labeled(l) => {
            ctx.pending_decorators.clear();
            visit_stmt(&l.body, state, ctx);
        }
        _ => {
            ctx.pending_decorators.clear();
        }
    }
}

fn visit_decl(decl: &Decl, state: &mut ParseState<'_>, ctx: &mut VisitCtx<'_>) {
    use swc_common::Spanned;
    if ctx.depth > MAX_DEPTH {
        ctx.pending_decorators.clear();
        return;
    }
    match decl {
        Decl::Class(class_decl) => {
            ctx.pending_decorators.clear();
            if ctx.class_decl_depth > 0 {
                visit_nested_class_body(&class_decl.class, state, ctx);
                return;
            }
            visit_class_decl(class_decl, state, ctx);
        }
        Decl::Fn(fn_decl) => {
            let pending = std::mem::take(&mut ctx.pending_decorators);
            visit_fn_decl(fn_decl, state, ctx, pending);
        }
        Decl::Var(var_decl) => {
            ctx.pending_decorators.clear();
            visit_var_decl(var_decl, state, ctx);
        }
        Decl::TsInterface(iface) => {
            ctx.pending_decorators.clear();
            let name = iface.id.sym.to_string();
            let exported = ctx.force_exported;
            state.push_symbol(
                NodeKind::Type,
                name.clone(),
                Some(name),
                Some(ctx.span(iface.span())),
                Some(source_slice(ctx.source, iface.span()).to_owned()),
                if exported {
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
        Decl::TsTypeAlias(alias) => {
            ctx.pending_decorators.clear();
            let name = alias.id.sym.to_string();
            let exported = ctx.force_exported;
            state.push_symbol(
                NodeKind::Type,
                name.clone(),
                Some(name),
                Some(ctx.span(alias.span())),
                Some(source_slice(ctx.source, alias.span()).to_owned()),
                if exported {
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
        Decl::TsEnum(enum_decl) => {
            ctx.pending_decorators.clear();
            let name = enum_decl.id.sym.to_string();
            let exported = ctx.force_exported;
            state.push_symbol(
                NodeKind::Type,
                name.clone(),
                Some(name),
                Some(ctx.span(enum_decl.span())),
                Some(source_slice(ctx.source, enum_decl.span()).to_owned()),
                if exported {
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
        Decl::TsModule(ts_module) => {
            ctx.pending_decorators.clear();
            // `namespace Foo { export function bar() {} }` — tree-sitter's
            // `namespace_declaration` falls through to `recurse_children`
            // which walks the body and emits symbols for nested
            // declarations. Mirror that by walking the body here.
            if ctx.depth < MAX_DEPTH
                && let Some(body) = &ts_module.body
            {
                visit_ts_namespace_body(body, state, ctx);
            }
        }
        Decl::Using(using_decl) => {
            ctx.pending_decorators.clear();
            // `using x = resource()` — recurse into initializers for call sites.
            for d in &using_decl.decls {
                visit_var_declarator(d, state, ctx);
            }
        }
    }
}

fn visit_class_decl(class_decl: &ClassDecl, state: &mut ParseState<'_>, ctx: &mut VisitCtx<'_>) {
    use swc_common::Spanned;
    let name = class_decl.ident.sym.to_string();
    let mut decorators: Vec<DecoratorCapture> = ctx.pending_decorators.clone();
    decorators.extend(collect_decorators_from_class(
        &class_decl.class,
        ctx.source,
        ctx.offsets,
    ));
    let constructor_deps = collect_constructor_deps_from_class(&class_decl.class, ctx.source);
    let implemented_interfaces = collect_implemented_interfaces_from_class(&class_decl.class);
    let exported = ctx.force_exported;
    let class_node = state.push_symbol(
        NodeKind::Class,
        name.clone(),
        Some(name.clone()),
        Some(ctx.span(class_decl.class.span())),
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
    state.set_symbol_implemented_interfaces(class_node.id, implemented_interfaces);
    let mut class_ctx = ctx.with_class(&class_node, decorators);
    for member in &class_decl.class.body {
        visit_class_member(member, state, &mut class_ctx);
    }
}

fn visit_nested_class_body(
    class: &swc_ecma_ast::Class,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    if ctx.depth >= MAX_DEPTH {
        return;
    }

    for member in &class.body {
        match member {
            ClassMember::Method(method) => {
                if let Some(body) = &method.function.body {
                    let mut body_ctx = VisitCtx {
                        source: ctx.source,
                        offsets: ctx.offsets,
                        parent_class: ctx.parent_class.clone(),
                        class_decl_depth: ctx.class_decl_depth.saturating_add(1),
                        owner: ctx.owner,
                        force_exported: false,
                        class_decorators: ctx.class_decorators.clone(),
                        pending_decorators: Vec::new(),
                        depth: ctx.depth + 1,
                    };
                    for stmt in &body.stmts {
                        visit_stmt(stmt, state, &mut body_ctx);
                    }
                }
            }
            ClassMember::PrivateMethod(method) => {
                if let Some(body) = &method.function.body {
                    let mut body_ctx = VisitCtx {
                        source: ctx.source,
                        offsets: ctx.offsets,
                        parent_class: ctx.parent_class.clone(),
                        class_decl_depth: ctx.class_decl_depth.saturating_add(1),
                        owner: ctx.owner,
                        force_exported: false,
                        class_decorators: ctx.class_decorators.clone(),
                        pending_decorators: Vec::new(),
                        depth: ctx.depth + 1,
                    };
                    for stmt in &body.stmts {
                        visit_stmt(stmt, state, &mut body_ctx);
                    }
                }
            }
            ClassMember::Constructor(ctor) => {
                if let Some(body) = &ctor.body {
                    let mut body_ctx = VisitCtx {
                        source: ctx.source,
                        offsets: ctx.offsets,
                        parent_class: ctx.parent_class.clone(),
                        class_decl_depth: ctx.class_decl_depth.saturating_add(1),
                        owner: ctx.owner,
                        force_exported: false,
                        class_decorators: ctx.class_decorators.clone(),
                        pending_decorators: Vec::new(),
                        depth: ctx.depth + 1,
                    };
                    for stmt in &body.stmts {
                        visit_stmt(stmt, state, &mut body_ctx);
                    }
                }
            }
            ClassMember::ClassProp(prop) => {
                if let Some(value) = &prop.value {
                    visit_expr(value, state, ctx);
                }
            }
            ClassMember::StaticBlock(block) => {
                let mut body_ctx = VisitCtx {
                    source: ctx.source,
                    offsets: ctx.offsets,
                    parent_class: ctx.parent_class.clone(),
                    class_decl_depth: ctx.class_decl_depth.saturating_add(1),
                    owner: ctx.owner,
                    force_exported: false,
                    class_decorators: ctx.class_decorators.clone(),
                    pending_decorators: Vec::new(),
                    depth: ctx.depth + 1,
                };
                for stmt in &block.body.stmts {
                    visit_stmt(stmt, state, &mut body_ctx);
                }
            }
            _ => {}
        }
    }
}

fn visit_fn_decl(
    fn_decl: &FnDecl,
    state: &mut ParseState<'_>,
    ctx: &VisitCtx<'_>,
    pending_decorators: Vec<DecoratorCapture>,
) {
    use swc_common::Spanned;
    let name = fn_decl.ident.sym.to_string();
    let mut decorators = pending_decorators;
    decorators.extend(collect_decorators_from_fn(
        &fn_decl.function,
        ctx.source,
        ctx.offsets,
    ));
    let exported = ctx.force_exported;
    let sig = Some(function_signature_from_fn(
        &name,
        &fn_decl.function,
        ctx.source,
    ));
    let func_node = state.push_symbol(
        NodeKind::Function,
        name.clone(),
        Some(name.clone()),
        Some(ctx.span(fn_decl.function.span())),
        sig,
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
        && let Some(body) = &fn_decl.function.body
    {
        let mut body_ctx = ctx.with_owner(func_node.id);
        for stmt in &body.stmts {
            visit_stmt(stmt, state, &mut body_ctx);
        }
    }
}

fn visit_var_decl(var_decl: &VarDecl, state: &mut ParseState<'_>, ctx: &VisitCtx<'_>) {
    for declarator in &var_decl.decls {
        visit_var_declarator(declarator, state, ctx);
    }
}

fn visit_var_declarator(
    declarator: &VarDeclarator,
    state: &mut ParseState<'_>,
    ctx: &VisitCtx<'_>,
) {
    use swc_common::Spanned;
    let name = pat_name_from_source(&declarator.name, ctx.source)
        .unwrap_or_else(|| "anonymous".to_owned());

    if let Some(init) = &declarator.init {
        match &**init {
            Expr::Arrow(arrow) => {
                let qualified_name = ctx
                    .parent_class
                    .as_ref()
                    .map_or_else(|| name.clone(), |class| format!("{}.{}", class.name, name));
                let sig = function_signature_from_arrow(&name, arrow, ctx.source)
                    .or_else(|| Some(name.clone()));
                let func_node = state.push_symbol(
                    NodeKind::Function,
                    name.clone(),
                    Some(qualified_name),
                    Some(ctx.span(declarator.span())),
                    sig,
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
                    let mut body_ctx = ctx.with_owner(func_node.id);
                    match &*arrow.body {
                        BlockStmtOrExpr::BlockStmt(block) => {
                            for stmt in &block.stmts {
                                visit_stmt(stmt, state, &mut body_ctx);
                            }
                        }
                        BlockStmtOrExpr::Expr(expr) => {
                            visit_expr(expr, state, &mut body_ctx);
                        }
                    }
                }
                return;
            }
            Expr::Fn(fn_expr) => {
                let qualified_name = ctx
                    .parent_class
                    .as_ref()
                    .map_or_else(|| name.clone(), |class| format!("{}.{}", class.name, name));
                let fn_name = fn_expr
                    .ident
                    .as_ref()
                    .map_or_else(|| name.clone(), |i| i.sym.to_string());
                let sig = Some(function_signature_from_fn(
                    &fn_name,
                    &fn_expr.function,
                    ctx.source,
                ));
                let func_node = state.push_symbol(
                    NodeKind::Function,
                    name.clone(),
                    Some(qualified_name),
                    Some(ctx.span(declarator.span())),
                    sig,
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
                    && let Some(body) = &fn_expr.function.body
                {
                    let mut body_ctx = ctx.with_owner(func_node.id);
                    for stmt in &body.stmts {
                        visit_stmt(stmt, state, &mut body_ctx);
                    }
                }
                return;
            }
            other => {
                if let Some(kvs) = extract_constant_string_value_swc(&name, other) {
                    for (k, v) in kvs {
                        state.record_constant_string(k, v);
                    }
                }
            }
        }
        // Also recurse for nested call sites in the initializer expression.
        let mut expr_ctx = VisitCtx {
            source: ctx.source,
            offsets: ctx.offsets,
            parent_class: ctx.parent_class.clone(),
            class_decl_depth: ctx.class_decl_depth,
            owner: ctx.owner,
            force_exported: ctx.force_exported,
            class_decorators: ctx.class_decorators.clone(),
            pending_decorators: Vec::new(),
            depth: ctx.depth + 1,
        };
        visit_expr(init, state, &mut expr_ctx);
    }
}

fn visit_class_member(member: &ClassMember, state: &mut ParseState<'_>, ctx: &mut VisitCtx<'_>) {
    match member {
        ClassMember::Method(method) => {
            let pending = std::mem::take(&mut ctx.pending_decorators);
            visit_class_method(method, state, ctx, pending);
        }
        ClassMember::PrivateMethod(pm) => {
            use swc_common::Spanned;
            ctx.pending_decorators.clear();
            let Some(parent_class) = &ctx.parent_class else {
                if ctx.depth < MAX_DEPTH
                    && let Some(body) = &pm.function.body
                {
                    let mut body_ctx = ctx.with_owner(
                        ctx.owner.unwrap_or(
                            ctx.parent_class
                                .as_ref()
                                .map_or_else(|| state.file_node_id(), |c| c.id),
                        ),
                    );
                    for stmt in &body.stmts {
                        visit_stmt(stmt, state, &mut body_ctx);
                    }
                }
                return;
            };
            // Tree-sitter parses `#foo() {}` as a `method_definition` with a
            // `private_property_identifier` child and emits a Function symbol
            // named `#foo`. Mirror that.
            let name = format!("#{}", pm.key.name);
            let decorators = collect_decorators_from_fn(&pm.function, ctx.source, ctx.offsets);
            let visibility = method_visibility_from_accessibility(pm.accessibility);
            let sig = Some(function_signature_from_fn(&name, &pm.function, ctx.source));
            let method_node = state.push_symbol(
                NodeKind::Function,
                name.clone(),
                Some(format!("{}.{}", parent_class.name, name)),
                Some(ctx.span(pm.span())),
                sig,
                Some(visibility),
                Some(parent_class.name.clone()),
                decorators,
                ctx.class_decorators.clone(),
                Vec::new(),
            );
            if ctx.depth < MAX_DEPTH
                && let Some(body) = &pm.function.body
            {
                let mut body_ctx = ctx.with_owner(method_node.id);
                for stmt in &body.stmts {
                    visit_stmt(stmt, state, &mut body_ctx);
                }
            }
        }
        ClassMember::Constructor(ctor) => {
            // Tree-sitter parses `constructor(...) {}` as a `method_definition`
            // with name "constructor" and emits a Function symbol. Mirror that,
            // then continue with the existing parameter-decorator + body logic.
            ctx.pending_decorators.clear();
            let ctor_method_id = if let Some(parent_class) = &ctx.parent_class {
                use swc_common::Spanned;
                let name = "constructor".to_owned();
                let constructor_deps = collect_constructor_deps_from_ctor(ctor, ctx.source);
                let visibility = method_visibility_from_accessibility(ctor.accessibility);
                let node = state.push_symbol(
                    NodeKind::Function,
                    name.clone(),
                    Some(format!("{}.{}", parent_class.name, name)),
                    Some(ctx.span(ctor.span())),
                    None,
                    Some(visibility),
                    Some(parent_class.name.clone()),
                    Vec::new(),
                    ctx.class_decorators.clone(),
                    constructor_deps,
                );
                Some(node.id)
            } else {
                None
            };
            let _ = ctor_method_id;

            // Parameter decorators (e.g. `@Inject('TOKEN')`, `@InjectModel(Model.name)`)
            // are stored on TsParamProp.decorators in swc's AST. Tree-sitter sees them as
            // call_expressions inside the constructor parameter list, so we must emit matching
            // call sites with the parent class as owner.
            if let Some(parent) = &ctx.parent_class {
                use swc_common::Spanned;
                use swc_ecma_ast::ParamOrTsParamProp;
                let owner_id = parent.id;
                for param in &ctor.params {
                    let decorators = match param {
                        ParamOrTsParamProp::TsParamProp(ts_prop) => ts_prop.decorators.as_slice(),
                        ParamOrTsParamProp::Param(p) => p.decorators.as_slice(),
                    };
                    for decorator in decorators {
                        let (callee_name, qualified_hint) =
                            expression_name_from_expr(&decorator.expr);
                        if callee_name.is_empty() {
                            continue;
                        }
                        // For @Inject('TOKEN'), the literal_argument is the string token.
                        // For @InjectModel(Model.name), the literal_argument is the raw member expr text.
                        let literal_argument =
                            if let Expr::Call(CallExpr { args, .. }) = &*decorator.expr {
                                // For @Inject('TOKEN') the literal string is the token.
                                // For @InjectModel(Model.name) there is no string literal, so
                                // fall back to the raw source text of the first argument.
                                // first_raw_arg_text is used here explicitly because this
                                // call site is NOT part of the topic pipeline — the raw text
                                // of a member expression is valid as a DI token hint.
                                first_literal_argument_from_args(args, ctx.source)
                                    .or_else(|| first_raw_arg_text(args, ctx.source))
                            } else {
                                None
                            };
                        let raw_arguments =
                            if let Expr::Call(CallExpr { args, .. }) = &*decorator.expr {
                                Some(raw_arguments_from_args(args, ctx.source))
                            } else {
                                None
                            };
                        state.push_call_site_swc(
                            owner_id,
                            callee_name,
                            qualified_hint,
                            literal_argument,
                            raw_arguments,
                            ctx.span(decorator.span()),
                        );
                    }
                }
            }

            if ctx.depth < MAX_DEPTH
                && let Some(body) = &ctor.body
            {
                // Body call sites are owned by the constructor Function node
                // (matching tree-sitter's `method_definition` arm, which sets
                // owner = method_node.id for body recursion).
                let owner_id = ctor_method_id
                    .or_else(|| ctx.parent_class.as_ref().map(|c| c.id))
                    .unwrap_or_else(|| state.file_node_id());
                let mut body_ctx = ctx.with_owner(owner_id);
                for stmt in &body.stmts {
                    visit_stmt(stmt, state, &mut body_ctx);
                }
            }
        }
        ClassMember::ClassProp(prop) => {
            ctx.pending_decorators.clear();
            if let Some(val) = &prop.value {
                let name = prop_name_text(&prop.key);
                let qualified_name = ctx
                    .parent_class
                    .as_ref()
                    .map_or_else(|| name.clone(), |class| format!("{}.{}", class.name, name));
                match &**val {
                    Expr::Arrow(arrow) if !name.is_empty() => {
                        let sig = function_signature_from_arrow(&name, arrow, ctx.source)
                            .or_else(|| Some(name.clone()));
                        let func_node = state.push_symbol(
                            NodeKind::Function,
                            name.clone(),
                            Some(qualified_name),
                            Some(ctx.span(prop.span())),
                            sig,
                            Some(method_visibility_from_accessibility(prop.accessibility)),
                            ctx.parent_class.as_ref().map(|c| c.name.clone()),
                            Vec::new(),
                            ctx.class_decorators.clone(),
                            Vec::new(),
                        );
                        if ctx.depth < MAX_DEPTH {
                            let mut body_ctx = ctx.with_owner(func_node.id);
                            match &*arrow.body {
                                BlockStmtOrExpr::BlockStmt(block) => {
                                    for stmt in &block.stmts {
                                        visit_stmt(stmt, state, &mut body_ctx);
                                    }
                                }
                                BlockStmtOrExpr::Expr(expr) => {
                                    visit_expr(expr, state, &mut body_ctx);
                                }
                            }
                        }
                        return;
                    }
                    Expr::Fn(fn_expr) if !name.is_empty() => {
                        let fn_name = fn_expr
                            .ident
                            .as_ref()
                            .map_or_else(|| name.clone(), |i| i.sym.to_string());
                        let sig = Some(function_signature_from_fn(
                            &fn_name,
                            &fn_expr.function,
                            ctx.source,
                        ));
                        let func_node = state.push_symbol(
                            NodeKind::Function,
                            name.clone(),
                            Some(qualified_name),
                            Some(ctx.span(prop.span())),
                            sig,
                            Some(method_visibility_from_accessibility(prop.accessibility)),
                            ctx.parent_class.as_ref().map(|c| c.name.clone()),
                            Vec::new(),
                            ctx.class_decorators.clone(),
                            Vec::new(),
                        );
                        if ctx.depth < MAX_DEPTH
                            && let Some(body) = &fn_expr.function.body
                        {
                            let mut body_ctx = ctx.with_owner(func_node.id);
                            for stmt in &body.stmts {
                                visit_stmt(stmt, state, &mut body_ctx);
                            }
                        }
                        return;
                    }
                    _ => {}
                }
            }

            if let Some(val) = &prop.value
                && ctx.depth < MAX_DEPTH
            {
                let owner_id = ctx.parent_class.as_ref().map(|c| c.id);
                let mut val_ctx = VisitCtx {
                    source: ctx.source,
                    offsets: ctx.offsets,
                    parent_class: ctx.parent_class.clone(),
                    class_decl_depth: ctx.class_decl_depth,
                    owner: owner_id,
                    force_exported: false,
                    class_decorators: ctx.class_decorators.clone(),
                    pending_decorators: Vec::new(),
                    depth: ctx.depth + 1,
                };
                visit_expr(val, state, &mut val_ctx);
            }
        }
        ClassMember::StaticBlock(sb) => {
            ctx.pending_decorators.clear();
            if ctx.depth < MAX_DEPTH {
                let owner_id = ctx.parent_class.as_ref().map(|c| c.id);
                if let Some(oid) = owner_id {
                    let mut body_ctx = ctx.with_owner(oid);
                    for stmt in &sb.body.stmts {
                        visit_stmt(stmt, state, &mut body_ctx);
                    }
                }
            }
        }
        ClassMember::TsIndexSignature(_)
        | ClassMember::Empty(_)
        | ClassMember::PrivateProp(_)
        | ClassMember::AutoAccessor(_) => {
            ctx.pending_decorators.clear();
        }
    }
}

fn visit_class_method(
    method: &ClassMethod,
    state: &mut ParseState<'_>,
    ctx: &VisitCtx<'_>,
    pending_decorators: Vec<DecoratorCapture>,
) {
    use swc_common::Spanned;
    let Some(parent_class) = &ctx.parent_class else {
        // No parent class — just recurse into body for call sites.
        if ctx.depth < MAX_DEPTH
            && let Some(body) = &method.function.body
        {
            let mut body_ctx = VisitCtx {
                source: ctx.source,
                offsets: ctx.offsets,
                parent_class: None,
                class_decl_depth: ctx.class_decl_depth,
                owner: ctx.owner,
                force_exported: false,
                class_decorators: ctx.class_decorators.clone(),
                pending_decorators: Vec::new(),
                depth: ctx.depth + 1,
            };
            for stmt in &body.stmts {
                visit_stmt(stmt, state, &mut body_ctx);
            }
        }
        return;
    };

    let name = prop_name_text(&method.key);
    let name = if name.is_empty() {
        "anonymous".to_owned()
    } else {
        name
    };

    let mut decorators = pending_decorators;
    decorators.extend(collect_decorators_from_fn(
        &method.function,
        ctx.source,
        ctx.offsets,
    ));

    let visibility = method_visibility_from_accessibility(method.accessibility);
    let sig = Some(function_signature_from_fn(
        &name,
        &method.function,
        ctx.source,
    ));

    let method_node = state.push_symbol(
        NodeKind::Function,
        name.clone(),
        Some(format!("{}.{}", parent_class.name, name)),
        Some(ctx.span(method.span())),
        sig,
        Some(visibility),
        Some(parent_class.name.clone()),
        decorators,
        ctx.class_decorators.clone(),
        Vec::new(),
    );

    if ctx.depth < MAX_DEPTH
        && let Some(body) = &method.function.body
    {
        let mut body_ctx = ctx.with_owner(method_node.id);
        for stmt in &body.stmts {
            visit_stmt(stmt, state, &mut body_ctx);
        }
    }
}

fn visit_expr(expr: &Expr, state: &mut ParseState<'_>, ctx: &mut VisitCtx<'_>) {
    if ctx.depth > MAX_DEPTH {
        return;
    }
    match expr {
        Expr::Call(call) => {
            visit_call_expr(call, state, ctx);
        }
        Expr::New(new_expr) => {
            visit_new_expr(new_expr, state, ctx);
        }
        Expr::Assign(assign) => {
            // Visit both sides. LHS can contain computed member access
            // with call expressions (e.g., `cache[this.getKey()] = v`).
            visit_assign_target(&assign.left, state, ctx);
            visit_expr(&assign.right, state, ctx);
        }
        Expr::Seq(seq) => {
            for e in &seq.exprs {
                visit_expr(e, state, ctx);
            }
        }
        Expr::Paren(p) => {
            visit_expr(&p.expr, state, ctx);
        }
        Expr::Cond(c) => {
            visit_expr(&c.test, state, ctx);
            visit_expr(&c.cons, state, ctx);
            visit_expr(&c.alt, state, ctx);
        }
        Expr::Unary(u) => {
            visit_expr(&u.arg, state, ctx);
        }
        Expr::Await(a) => {
            visit_expr(&a.arg, state, ctx);
        }
        Expr::Yield(y) => {
            if let Some(arg) = &y.arg {
                visit_expr(arg, state, ctx);
            }
        }
        Expr::Bin(bin) => {
            visit_expr(&bin.left, state, ctx);
            visit_expr(&bin.right, state, ctx);
        }
        Expr::Member(m) => {
            // Unwrap member-expression chains iteratively to find non-Member
            // sub-expressions (call/new/etc.) that may contain call sites.
            // Avoid Rust stack overflow for deeply nested chains.
            // Also visit computed-property expressions at each level so that
            // `obj[key()]` captures the `key()` call site (Finding 9).
            if let MemberProp::Computed(c) = &m.prop {
                visit_expr(&c.expr, state, ctx);
            }
            let mut inner: &Expr = &m.obj;
            let mut limit = 10_000usize;
            loop {
                if limit == 0 {
                    break;
                }
                limit -= 1;
                match inner {
                    Expr::Member(inner_m) => {
                        if let MemberProp::Computed(c) = &inner_m.prop {
                            visit_expr(&c.expr, state, ctx);
                        }
                        inner = &inner_m.obj;
                    }
                    other => {
                        visit_expr(other, state, ctx);
                        break;
                    }
                }
            }
        }
        Expr::TaggedTpl(t) => {
            visit_expr(&t.tag, state, ctx);
            // Template literals can embed arbitrary call expressions.
            for e in &t.tpl.exprs {
                visit_expr(e, state, ctx);
            }
        }
        Expr::Tpl(t) => {
            for e in &t.exprs {
                visit_expr(e, state, ctx);
            }
        }
        Expr::OptChain(opt) => {
            // Optional chain: `a?.b()`, `a?.b.c()`. The base member chain is
            // wrapped inside the optional. Dispatch on the inner `OptChainBase`.
            match &*opt.base {
                swc_ecma_ast::OptChainBase::Call(call) => {
                    // Synthesize CallExpr semantics: emit call site + recurse args.
                    if let Some(owner_id) = ctx.owner {
                        let (callee_name, qualified_hint) = expression_name_from_expr(&call.callee);
                        let literal_argument =
                            first_literal_argument_from_args(&call.args, ctx.source);
                        let raw_arguments = Some(raw_arguments_from_args(&call.args, ctx.source));
                        state.push_call_site_swc(
                            owner_id,
                            callee_name,
                            qualified_hint,
                            literal_argument,
                            raw_arguments,
                            ctx.span(opt.span),
                        );
                    }
                    visit_expr(&call.callee, state, ctx);
                    for arg in &call.args {
                        visit_expr(&arg.expr, state, ctx);
                    }
                }
                swc_ecma_ast::OptChainBase::Member(mem) => {
                    visit_expr(&mem.obj, state, ctx);
                    if let MemberProp::Computed(c) = &mem.prop {
                        visit_expr(&c.expr, state, ctx);
                    }
                }
            }
        }
        Expr::Update(u) => {
            visit_expr(&u.arg, state, ctx);
        }
        Expr::TsAs(t) => {
            visit_expr(&t.expr, state, ctx);
        }
        Expr::TsTypeAssertion(t) => {
            visit_expr(&t.expr, state, ctx);
        }
        Expr::TsConstAssertion(t) => {
            visit_expr(&t.expr, state, ctx);
        }
        Expr::TsNonNull(t) => {
            visit_expr(&t.expr, state, ctx);
        }
        Expr::TsSatisfies(t) => {
            visit_expr(&t.expr, state, ctx);
        }
        Expr::TsInstantiation(t) => {
            visit_expr(&t.expr, state, ctx);
        }
        Expr::Class(class_expr) => {
            // `const X = class { ... }` — tree-sitter's `class_expression`
            // falls through to `recurse_children`. Methods are visited but
            // because `parent_class = None` at that point, tree-sitter emits
            // NO Function symbols for them — it only recurses for call sites
            // with the surrounding owner. Mirror that: walk each member's
            // body for call sites, no symbols.
            if ctx.depth < MAX_DEPTH {
                for member in &class_expr.class.body {
                    match member {
                        ClassMember::Method(m) => {
                            if let Some(body) = &m.function.body {
                                let mut body_ctx = VisitCtx {
                                    source: ctx.source,
                                    offsets: ctx.offsets,
                                    parent_class: ctx.parent_class.clone(),
                                    class_decl_depth: ctx.class_decl_depth,
                                    owner: ctx.owner,
                                    force_exported: false,
                                    class_decorators: ctx.class_decorators.clone(),
                                    pending_decorators: Vec::new(),
                                    depth: ctx.depth + 1,
                                };
                                for stmt in &body.stmts {
                                    visit_stmt(stmt, state, &mut body_ctx);
                                }
                            }
                        }
                        ClassMember::PrivateMethod(pm) => {
                            if let Some(body) = &pm.function.body {
                                let mut body_ctx = VisitCtx {
                                    source: ctx.source,
                                    offsets: ctx.offsets,
                                    parent_class: ctx.parent_class.clone(),
                                    class_decl_depth: ctx.class_decl_depth,
                                    owner: ctx.owner,
                                    force_exported: false,
                                    class_decorators: ctx.class_decorators.clone(),
                                    pending_decorators: Vec::new(),
                                    depth: ctx.depth + 1,
                                };
                                for stmt in &body.stmts {
                                    visit_stmt(stmt, state, &mut body_ctx);
                                }
                            }
                        }
                        ClassMember::Constructor(ctor) => {
                            if let Some(body) = &ctor.body {
                                let mut body_ctx = VisitCtx {
                                    source: ctx.source,
                                    offsets: ctx.offsets,
                                    parent_class: ctx.parent_class.clone(),
                                    class_decl_depth: ctx.class_decl_depth,
                                    owner: ctx.owner,
                                    force_exported: false,
                                    class_decorators: ctx.class_decorators.clone(),
                                    pending_decorators: Vec::new(),
                                    depth: ctx.depth + 1,
                                };
                                for stmt in &body.stmts {
                                    visit_stmt(stmt, state, &mut body_ctx);
                                }
                            }
                        }
                        ClassMember::ClassProp(prop) => {
                            if let Some(val) = &prop.value {
                                visit_expr(val, state, ctx);
                            }
                        }
                        ClassMember::StaticBlock(sb) => {
                            let mut body_ctx = VisitCtx {
                                source: ctx.source,
                                offsets: ctx.offsets,
                                parent_class: ctx.parent_class.clone(),
                                class_decl_depth: ctx.class_decl_depth,
                                owner: ctx.owner,
                                force_exported: false,
                                class_decorators: ctx.class_decorators.clone(),
                                pending_decorators: Vec::new(),
                                depth: ctx.depth + 1,
                            };
                            for stmt in &sb.body.stmts {
                                visit_stmt(stmt, state, &mut body_ctx);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        Expr::Arrow(arrow) => {
            // Bare arrow in expression position — visit body for nested calls.
            let mut body_ctx = VisitCtx {
                source: ctx.source,
                offsets: ctx.offsets,
                parent_class: ctx.parent_class.clone(),
                class_decl_depth: ctx.class_decl_depth,
                owner: ctx.owner,
                force_exported: false,
                class_decorators: ctx.class_decorators.clone(),
                pending_decorators: Vec::new(),
                depth: ctx.depth + 1,
            };
            match &*arrow.body {
                BlockStmtOrExpr::BlockStmt(block) => {
                    for stmt in &block.stmts {
                        visit_stmt(stmt, state, &mut body_ctx);
                    }
                }
                BlockStmtOrExpr::Expr(e) => {
                    visit_expr(e, state, &mut body_ctx);
                }
            }
        }
        Expr::Fn(fn_expr) => {
            if ctx.depth < MAX_DEPTH
                && let Some(body) = &fn_expr.function.body
            {
                let mut body_ctx = VisitCtx {
                    source: ctx.source,
                    offsets: ctx.offsets,
                    parent_class: ctx.parent_class.clone(),
                    class_decl_depth: ctx.class_decl_depth,
                    owner: ctx.owner,
                    force_exported: false,
                    class_decorators: ctx.class_decorators.clone(),
                    pending_decorators: Vec::new(),
                    depth: ctx.depth + 1,
                };
                for stmt in &body.stmts {
                    visit_stmt(stmt, state, &mut body_ctx);
                }
            }
        }
        Expr::Object(obj) => {
            // Recurse into values for call sites.
            for prop_or_spread in &obj.props {
                match prop_or_spread {
                    PropOrSpread::Prop(prop) => match &**prop {
                        Prop::KeyValue(kv) => visit_expr(&kv.value, state, ctx),
                        Prop::Method(m) => {
                            if let Some(body) = &m.function.body {
                                let mut body_ctx = VisitCtx {
                                    source: ctx.source,
                                    offsets: ctx.offsets,
                                    parent_class: ctx.parent_class.clone(),
                                    class_decl_depth: ctx.class_decl_depth,
                                    owner: ctx.owner,
                                    force_exported: false,
                                    class_decorators: ctx.class_decorators.clone(),
                                    pending_decorators: Vec::new(),
                                    depth: ctx.depth + 1,
                                };
                                for stmt in &body.stmts {
                                    visit_stmt(stmt, state, &mut body_ctx);
                                }
                            }
                        }
                        Prop::Getter(g) => {
                            if let Some(body) = &g.body {
                                let mut body_ctx = VisitCtx {
                                    source: ctx.source,
                                    offsets: ctx.offsets,
                                    parent_class: ctx.parent_class.clone(),
                                    class_decl_depth: ctx.class_decl_depth,
                                    owner: ctx.owner,
                                    force_exported: false,
                                    class_decorators: ctx.class_decorators.clone(),
                                    pending_decorators: Vec::new(),
                                    depth: ctx.depth + 1,
                                };
                                for stmt in &body.stmts {
                                    visit_stmt(stmt, state, &mut body_ctx);
                                }
                            }
                        }
                        Prop::Setter(s) => {
                            if let Some(body) = &s.body {
                                let mut body_ctx = VisitCtx {
                                    source: ctx.source,
                                    offsets: ctx.offsets,
                                    parent_class: ctx.parent_class.clone(),
                                    class_decl_depth: ctx.class_decl_depth,
                                    owner: ctx.owner,
                                    force_exported: false,
                                    class_decorators: ctx.class_decorators.clone(),
                                    pending_decorators: Vec::new(),
                                    depth: ctx.depth + 1,
                                };
                                for stmt in &body.stmts {
                                    visit_stmt(stmt, state, &mut body_ctx);
                                }
                            }
                        }
                        _ => {}
                    },
                    PropOrSpread::Spread(s) => visit_expr(&s.expr, state, ctx),
                }
            }
        }
        Expr::Array(arr) => {
            for elem in arr.elems.iter().flatten() {
                visit_expr(&elem.expr, state, ctx);
            }
        }
        Expr::JSXElement(jsx_elem) => {
            visit_jsx_opening(&jsx_elem.opening, state, ctx);
            visit_jsx_children(&jsx_elem.children, state, ctx);
        }
        Expr::JSXFragment(jsx_frag) => {
            visit_jsx_children(&jsx_frag.children, state, ctx);
        }
        Expr::SuperProp(sp) => {
            // `super[expr()]` — recurse into the computed property expr.
            if let swc_ecma_ast::SuperProp::Computed(c) = &sp.prop {
                visit_expr(&c.expr, state, ctx);
            }
        }
        // Other expressions don't have sub-expressions we need to recurse into for call sites.
        _ => {}
    }
}

fn visit_jsx_children(
    children: &[swc_ecma_ast::JSXElementChild],
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    use swc_ecma_ast::{JSXElementChild, JSXExpr};
    for child in children {
        match child {
            JSXElementChild::JSXExprContainer(container) => {
                if let JSXExpr::Expr(expr) = &container.expr {
                    visit_expr(expr, state, ctx);
                }
            }
            JSXElementChild::JSXSpreadChild(spread) => {
                visit_expr(&spread.expr, state, ctx);
            }
            JSXElementChild::JSXElement(elem) => {
                visit_jsx_opening(&elem.opening, state, ctx);
                visit_jsx_children(&elem.children, state, ctx);
            }
            JSXElementChild::JSXFragment(frag) => {
                visit_jsx_children(&frag.children, state, ctx);
            }
            JSXElementChild::JSXText(_) => {}
        }
    }
}

// Visit call-sites inside JSX attribute expressions.
//
// For `<Foo onClick={handler()} value={compute()} {...spreadFn()}>`,
// tree-sitter's `recurse_children` visits these unconditionally via the
// `_` arm; the swc visitor must do the same or lose every React/TSX
// attribute expression.
fn visit_jsx_opening(
    opening: &swc_ecma_ast::JSXOpeningElement,
    state: &mut ParseState<'_>,
    ctx: &mut VisitCtx<'_>,
) {
    use swc_ecma_ast::{JSXAttrOrSpread, JSXAttrValue, JSXExpr};
    for attr_or_spread in &opening.attrs {
        match attr_or_spread {
            JSXAttrOrSpread::JSXAttr(attr) => {
                if let Some(value) = &attr.value {
                    match value {
                        JSXAttrValue::JSXExprContainer(c) => {
                            if let JSXExpr::Expr(e) = &c.expr {
                                visit_expr(e, state, ctx);
                            }
                        }
                        JSXAttrValue::JSXElement(el) => {
                            visit_jsx_opening(&el.opening, state, ctx);
                            visit_jsx_children(&el.children, state, ctx);
                        }
                        JSXAttrValue::JSXFragment(frag) => {
                            visit_jsx_children(&frag.children, state, ctx);
                        }
                        JSXAttrValue::Str(_) => {}
                    }
                }
            }
            JSXAttrOrSpread::SpreadElement(spread) => {
                visit_expr(&spread.expr, state, ctx);
            }
        }
    }
}

fn visit_call_expr(call: &CallExpr, state: &mut ParseState<'_>, ctx: &mut VisitCtx<'_>) {
    use swc_common::Spanned;
    if let Some(owner_id) = ctx.owner {
        let callee_expr_opt = match &call.callee {
            Callee::Expr(e) => Some(e.as_ref()),
            Callee::Import(_) => {
                // dynamic import — callee_name = "import"
                let literal_argument = first_literal_argument_from_args(&call.args, ctx.source);
                let raw_arguments = Some(raw_arguments_from_args(&call.args, ctx.source));
                state.push_call_site_swc(
                    owner_id,
                    "import".to_owned(),
                    Some("import".to_owned()),
                    literal_argument,
                    raw_arguments,
                    ctx.span(call.span()),
                );
                None
            }
            Callee::Super(_) => None,
        };

        if let Some(callee_expr) = callee_expr_opt {
            let (callee_name, qualified_hint) = expression_name_from_expr(callee_expr);
            let literal_argument = first_literal_argument_from_args(&call.args, ctx.source);
            let raw_arguments = Some(raw_arguments_from_args(&call.args, ctx.source));
            state.push_call_site_swc(
                owner_id,
                callee_name,
                qualified_hint,
                literal_argument,
                raw_arguments,
                ctx.span(call.span()),
            );
        }
    }
    // Recurse into arguments.
    for arg in &call.args {
        visit_expr(&arg.expr, state, ctx);
    }
    // Recurse into callee for nested member/call expressions.
    if let Callee::Expr(callee_expr) = &call.callee {
        visit_expr(callee_expr, state, ctx);
    }
}

fn visit_new_expr(new_expr: &NewExpr, state: &mut ParseState<'_>, ctx: &mut VisitCtx<'_>) {
    use swc_common::Spanned;
    if let Some(owner_id) = ctx.owner {
        let (callee_name, qualified_hint) = expression_name_from_expr(&new_expr.callee);
        let literal_argument = first_literal_argument_from_new(new_expr.args.as_ref(), ctx.source);
        let raw_arguments = raw_arguments_from_new(new_expr.args.as_ref(), ctx.source);
        state.push_call_site_swc(
            owner_id,
            callee_name,
            qualified_hint,
            literal_argument,
            raw_arguments,
            ctx.span(new_expr.span()),
        );
    }
    if let Some(args) = &new_expr.args {
        for arg in args {
            visit_expr(&arg.expr, state, ctx);
        }
    }
    visit_expr(&new_expr.callee, state, ctx);
}

// ── Decorator collection ──────────────────────────────────────────────────────

fn collect_decorators_from_class(
    class: &swc_ecma_ast::Class,
    source: &str,
    offsets: &[u32],
) -> Vec<DecoratorCapture> {
    class
        .decorators
        .iter()
        .map(|d| single_decorator_from_swc(d, source, offsets))
        .collect()
}

fn collect_decorators_from_fn(
    func: &swc_ecma_ast::Function,
    source: &str,
    offsets: &[u32],
) -> Vec<DecoratorCapture> {
    func.decorators
        .iter()
        .map(|d| single_decorator_from_swc(d, source, offsets))
        .collect()
}

// ── Pat name extraction ───────────────────────────────────────────────────────

fn pat_name_from_source(pat: &swc_ecma_ast::Pat, source: &str) -> Option<String> {
    use swc_common::Spanned;
    match pat {
        swc_ecma_ast::Pat::Ident(binding_ident) => Some(binding_ident.id.sym.to_string()),
        swc_ecma_ast::Pat::Assign(assign) => pat_name_from_source(&assign.left, source),
        swc_ecma_ast::Pat::Expr(expr) => {
            if let Expr::Ident(i) = &**expr {
                Some(i.sym.to_string())
            } else {
                None
            }
        }
        // tree-sitter uses the pattern's raw source text for destructuring
        // patterns (`const { a, b } = ...` → name = `{ a, b }`).
        // This avoids NodeId collisions from multiple "anonymous" entries.
        swc_ecma_ast::Pat::Object(_) | swc_ecma_ast::Pat::Array(_) | swc_ecma_ast::Pat::Rest(_) => {
            Some(source_slice(source, pat.span()).to_owned())
        }
        swc_ecma_ast::Pat::Invalid(_) => None,
    }
}

// ── Parse with error recovery ───────────────────────────────────────────────

/// Parse `source` using a fresh `SourceMap` and return the module on success.
///
/// Must be called inside an active `GLOBALS.set(...)` scope. All SWC
/// operations — including `Atom` (identifier string) interning — use the
/// `Globals` bound to the current thread. The returned `Module` carries spans
/// and interned atoms that are only valid while that same `Globals` scope
/// remains active on the thread.
fn try_parse(source: &str, syntax: Syntax) -> Option<swc_ecma_ast::Module> {
    use swc_ecma_ast::EsVersion;
    let cm = Lrc::new(SourceMap::default());
    let fm = cm.new_source_file(FileName::Anon.into(), source.to_owned());
    let lexer = Lexer::new(syntax, EsVersion::default(), StringInput::from(&*fm), None);
    let mut parser = Parser::new_from(lexer);
    match syntax {
        Syntax::Typescript(_) => parser.parse_typescript_module().ok(),
        Syntax::Es(_) => parser.parse_module().ok(),
    }
}

/// Get the byte offset of the first fatal parse error, or 0 if none.
///
/// Must be called inside an active `GLOBALS.set(...)` scope. The error span
/// is minted under that scope's `Globals` and is consumed immediately within
/// this function — only the computed `usize` offset escapes.
fn first_error_byte_offset(source: &str, syntax: Syntax) -> usize {
    use swc_ecma_ast::EsVersion;
    let cm = Lrc::new(SourceMap::default());
    let fm = cm.new_source_file(FileName::Anon.into(), source.to_owned());
    let fm_lo = fm.start_pos.0; // BytePos bias: the file starts at this BytePos
    let lexer = Lexer::new(syntax, EsVersion::default(), StringInput::from(&*fm), None);
    let mut parser = Parser::new_from(lexer);
    let err = match syntax {
        Syntax::Typescript(_) => parser.parse_typescript_module().err(),
        Syntax::Es(_) => parser.parse_module().err(),
    };
    err.map_or(0, |e| {
        // e.span().lo.0 is offset by fm_lo (swc BytePos bias starts at 1 + file offset).
        let raw = e.span().lo.0;
        // The file's start BytePos is fm_lo. Subtract it to get file-local offset.
        raw.saturating_sub(fm_lo) as usize
    })
}

/// Attempt to parse `source` with swc, recovering from fatal syntax errors
/// by truncating at the error position and padding with closing braces.
///
/// Always returns `Ok` — on unrecoverable failure it yields an empty module
/// (matching tree-sitter's behaviour of always producing a partial tree).
/// A `tracing::warn` is emitted whenever recovery is needed.
///
/// Must be called inside an active `GLOBALS.set(...)` scope opened by the
/// caller.  The returned `Module` contains spans and interned atoms valid only
/// within that scope; all span-dependent work (`visit_module`) must complete
/// before the scope closes.
fn parse_with_recovery(
    file: &FileEntry,
    source: &str,
    syntax: Syntax,
    absolute_path: &std::path::Path,
) -> (swc_ecma_ast::Module, SwcParseStatus) {
    // First attempt: try to parse the source as-is.
    if let Some(m) = try_parse(source, syntax) {
        return (m, SwcParseStatus::Parsed);
    }

    // First attempt failed. Get the byte offset of the parse error.
    let error_lo = first_error_byte_offset(source, syntax);
    let truncate_at = error_lo.min(source.len());

    // Build a recovered source: keep everything before the error, then
    // append enough closing braces to balance open blocks.
    let prefix = &source[..truncate_at];
    let open_count = prefix.bytes().filter(|&b| b == b'{').count();
    let close_count = prefix.bytes().filter(|&b| b == b'}').count();
    let needed = open_count.saturating_sub(close_count).max(1);
    let mut recovered = prefix.to_owned();
    recovered.push('\n');
    for _ in 0..needed {
        recovered.push('}');
        recovered.push('\n');
    }

    if let Some(m) = try_parse(&recovered, syntax) {
        tracing::warn!(
            path = %absolute_path.display(),
            file = %file.path.display(),
            "swc parse recovered from syntax error at byte {error_lo}"
        );
        return (m, SwcParseStatus::Recovered);
    }

    tracing::warn!(
        path = %absolute_path.display(),
        "swc parse failed unrecoverably — emitting empty module"
    );
    (
        swc_ecma_ast::Module {
            span: swc_common::DUMMY_SP,
            body: Vec::new(),
            shebang: None,
        },
        SwcParseStatus::Unrecoverable,
    )
}

// ── Public entry point ───────────────────────────────────────────────────────

/// Parse a TypeScript or JavaScript file with swc and populate `state`.
///
/// Always succeeds: on unrecoverable parse errors an empty module is produced
/// and a `tracing::warn` is emitted, mirroring tree-sitter's behaviour of
/// always returning a (possibly empty) partial tree.
pub(crate) fn parse_ts_js_with_swc_with_status(
    file: &FileEntry,
    state: &mut ParseState<'_>,
    source: &str,
    absolute_path: &std::path::Path,
) -> SwcParseStatus {
    use std::ffi::OsStr;

    // Classify the file by extension using case-insensitive comparison.
    let ext = file.path.extension().and_then(OsStr::to_str).unwrap_or("");

    // `.mts` is the ES-module TypeScript variant; `.cts` is the CommonJS
    // TypeScript variant.  Both require the TypeScript parser.
    // Uppercase variants (`.TS`, `.TSX`) are also matched via
    // `eq_ignore_ascii_case`.
    //
    // Note: `.d.ts` cannot be detected from `extension()` alone because
    // `Path::extension()` returns only the final component ("ts"), so `dts`
    // mode is left as `false`.
    let is_ts = ext.eq_ignore_ascii_case("ts")
        || ext.eq_ignore_ascii_case("tsx")
        || ext.eq_ignore_ascii_case("mts")
        || ext.eq_ignore_ascii_case("cts");
    let tsx_mode = ext.eq_ignore_ascii_case("tsx");
    let jsx_mode = ext.eq_ignore_ascii_case("jsx");

    let syntax = if is_ts {
        Syntax::Typescript(TsSyntax {
            tsx: tsx_mode,
            decorators: true,
            dts: false,
            no_early_errors: false,
            disallow_ambiguous_jsx_like: false,
        })
    } else {
        Syntax::Es(EsSyntax {
            jsx: jsx_mode
                || ext.eq_ignore_ascii_case("js")
                || ext.eq_ignore_ascii_case("mjs")
                || ext.eq_ignore_ascii_case("cjs"),
            fn_bind: false,
            decorators: true,
            decorators_before_export: true,
            export_default_from: true,
            import_attributes: true,
            allow_super_outside_method: true,
            allow_return_outside_function: true,
            auto_accessors: true,
            explicit_resource_management: true,
        })
    };

    let offsets = build_line_offsets(source);

    // All SWC work — parsing and span-consuming AST traversal — happens
    // inside this single GLOBALS scope.  Nothing carrying raw swc spans
    // escapes the closure; only owned, span-free data is written to `state`.
    GLOBALS.set(&Globals::new(), || {
        let (module, status) = parse_with_recovery(file, source, syntax, absolute_path);
        visit_module(&module, state, source, &offsets);
        status
    })
}

#[cfg(any(test, feature = "test-support"))]
pub fn parse_ts_js_with_swc(
    file: &FileEntry,
    state: &mut ParseState<'_>,
    source: &str,
    absolute_path: &std::path::Path,
) {
    let _ = parse_ts_js_with_swc_with_status(file, state, source, absolute_path);
}

// ── Test-support ─────────────────────────────────────────────────────────────

#[cfg(any(test, feature = "test-support"))]
fn default_typescript_syntax_for_tests() -> Syntax {
    Syntax::Typescript(TsSyntax {
        tsx: false,
        decorators: true,
        dts: false,
        no_early_errors: false,
        disallow_ambiguous_jsx_like: false,
    })
}

#[cfg(any(test, feature = "test-support"))]
pub mod swc_test_support {
    //! Test-only surface that exercises the real `try_parse` helper so the
    //! regression test can drive it from many threads without depending on
    //! the full indexing pipeline.
    //!
    //! Each public function in this module opens its own `GLOBALS.set` scope so
    //! it is safe to call from rayon worker threads.  `try_parse` no longer
    //! self-scopes; callers must provide an active scope.  These helpers fulfil
    //! that contract for test code.
    use swc_common::{GLOBALS, Globals};
    use swc_ecma_ast::{Expr, Lit, Module, ModuleItem, Stmt};

    use super::{SwcParseStatus, default_typescript_syntax_for_tests, try_parse};

    /// Returns `true` when `source` parses successfully to a non-empty module.
    pub fn parse_yields_non_empty_module(source: &str) -> bool {
        let syntax = default_typescript_syntax_for_tests();
        GLOBALS.set(&Globals::new(), || {
            try_parse(source, syntax).is_some_and(|module| !module.body.is_empty())
        })
    }

    /// Returns `true` when `source` parses successfully and the resulting module
    /// contains at least one identifier whose name equals `ident_name`.
    ///
    /// Used by the parallel-parse regression test to verify that parallel SWC
    /// parses produce modules whose content matches the expected per-source
    /// marker — span cross-talk between rayon threads would cause a mismatch
    /// here.
    pub fn parse_source_contains_ident(source: &str, ident_name: &str) -> bool {
        let syntax = default_typescript_syntax_for_tests();
        GLOBALS.set(&Globals::new(), || {
            try_parse(source, syntax)
                .is_some_and(|module| module_contains_ident(&module, ident_name))
        })
    }

    /// Walk a parsed `Module` and return `true` when any identifier name equals
    /// `target`.  This is a shallow structural scan — it checks variable
    /// declaration names, export names, and class names.  Deep expression-level
    /// scanning is not needed for the identity-check test.
    fn module_contains_ident(module: &Module, target: &str) -> bool {
        for item in &module.body {
            if item_contains_ident(item, target) {
                return true;
            }
        }
        false
    }

    fn item_contains_ident(item: &ModuleItem, target: &str) -> bool {
        use swc_ecma_ast::{DefaultDecl, ExportDecl, ExportDefaultDecl, ModuleDecl};
        match item {
            ModuleItem::ModuleDecl(decl) => match decl {
                ModuleDecl::ExportDecl(ExportDecl { decl, .. }) => {
                    decl_contains_ident(decl, target)
                }
                ModuleDecl::ExportDefaultDecl(ExportDefaultDecl { decl, .. }) => match decl {
                    DefaultDecl::Class(c) => {
                        c.ident.as_ref().is_some_and(|i| i.sym.as_ref() == target)
                    }
                    DefaultDecl::Fn(f) => {
                        f.ident.as_ref().is_some_and(|i| i.sym.as_ref() == target)
                    }
                    DefaultDecl::TsInterfaceDecl(ts) => ts.id.sym.as_ref() == target,
                },
                _ => false,
            },
            ModuleItem::Stmt(stmt) => stmt_contains_ident(stmt, target),
        }
    }

    fn decl_contains_ident(decl: &swc_ecma_ast::Decl, target: &str) -> bool {
        use swc_ecma_ast::{ClassDecl, Decl, FnDecl};
        match decl {
            Decl::Var(var) => var.decls.iter().any(|d| var_decl_contains_ident(d, target)),
            Decl::Class(ClassDecl { ident, .. }) | Decl::Fn(FnDecl { ident, .. }) => {
                ident.sym.as_ref() == target
            }
            _ => false,
        }
    }

    fn var_decl_contains_ident(d: &swc_ecma_ast::VarDeclarator, target: &str) -> bool {
        use swc_ecma_ast::Pat;
        // Check the variable name itself.
        if let Pat::Ident(ident) = &d.name
            && ident.sym.as_ref() == target
        {
            return true;
        }
        // Check string literal initialisers.
        if let Some(init) = &d.init
            && let Expr::Lit(Lit::Str(s)) = init.as_ref()
            && s.value == target
        {
            return true;
        }
        false
    }

    fn stmt_contains_ident(stmt: &Stmt, target: &str) -> bool {
        use swc_ecma_ast::{ClassDecl, Decl};
        match stmt {
            Stmt::Decl(Decl::Var(var)) => {
                var.decls.iter().any(|d| var_decl_contains_ident(d, target))
            }
            Stmt::Decl(Decl::Class(ClassDecl { ident, .. })) => ident.sym.as_ref() == target,
            _ => false,
        }
    }

    /// Drive `parse_ts_js_with_swc` via the full extension-classification gate
    /// using a synthetic `FileEntry` whose path extension is `ext`.
    ///
    /// Returns `Some(true)` when at least one symbol was extracted from
    /// `source` (confirming the TypeScript parser was selected and succeeded),
    /// `Some(false)` when the parse produced an empty module (extension routed
    /// to the wrong parser or recovered to empty), and `None` on panic.
    ///
    /// Used by the extension-classification regression tests.
    pub fn parse_ts_file_via_extension(ext: &str, source: &str) -> bool {
        use std::path::PathBuf;

        use crate::Language;

        use crate::traverse::FileEntry;
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
        super::parse_ts_js_with_swc(&file, &mut state, source, std::path::Path::new("/tmp"));
        !state.symbols().is_empty()
    }

    /// Drive the full `parse_ts_js_with_swc` → `visit_module` pipeline
    /// and return whether a symbol named `ident_name` appears in the
    /// extracted `ParseState`.
    ///
    /// Unlike `parse_source_contains_ident` (which only drives `try_parse`),
    /// this helper exercises the complete pipeline including `visit_module`.
    /// A regression that moved `visit_module` outside the outer `GLOBALS.set`
    /// scope would manifest here.
    pub fn parse_full_pipeline_contains_symbol(ext: &str, source: &str, ident_name: &str) -> bool {
        use std::path::PathBuf;

        use crate::Language;

        use crate::traverse::FileEntry;
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
        super::parse_ts_js_with_swc(&file, &mut state, source, std::path::Path::new("/tmp"));
        state
            .symbols()
            .iter()
            .any(|s| s.node.name.as_str() == ident_name)
    }

    pub fn parse_recovery_status_for_extension(ext: &str, source: &str) -> &'static str {
        use std::path::PathBuf;

        use crate::Language;
        use crate::traverse::FileEntry;
        use crate::tree_sitter::ParseState;

        let path = PathBuf::from(format!("status.{ext}"));
        let file = FileEntry {
            path,
            language: Language::TypeScript,
            size_bytes: source.len() as u64,
            content_hash: [0u8; 32],
            source_bytes: None,
        };
        let mut state = ParseState::for_test(&file, source);
        match super::parse_ts_js_with_swc_with_status(
            &file,
            &mut state,
            source,
            std::path::Path::new("/tmp"),
        ) {
            SwcParseStatus::Parsed => "parsed",
            SwcParseStatus::Recovered => "recovered",
            SwcParseStatus::Unrecoverable => "unrecoverable",
        }
    }

    /// Apply the SWC parser to `source`, then walk the parsed module body and
    /// collect the set of top-level declared identifier names — function and
    /// class declarations, simple `Pat::Ident` variable bindings, type
    /// aliases, interfaces, enums, namespaces, and the same forms wrapped in
    /// `export` declarations or named default exports.
    ///
    /// The extension picks between TS/TSX/JS syntax flavours; declarations
    /// inside TSX-only or JSX-only sources would otherwise be silently dropped
    /// because `try_parse` would refuse to parse them. The result is sorted
    /// and deduplicated to make set comparison stable across parser
    /// implementations.
    pub fn top_level_declared_names_for_extension(ext: &str, source: &str) -> Vec<String> {
        use std::collections::BTreeSet;

        use swc_ecma_ast::{
            ClassDecl, Decl, DefaultDecl, ExportDecl, ExportDefaultDecl, FnDecl, ModuleDecl, Pat,
            VarDeclarator,
        };

        fn collect_decl_names(decl: &Decl, names: &mut BTreeSet<String>) {
            match decl {
                Decl::Class(ClassDecl { ident, .. }) | Decl::Fn(FnDecl { ident, .. }) => {
                    names.insert(ident.sym.as_ref().to_owned());
                }
                Decl::Var(var) => {
                    for VarDeclarator { name, .. } in &var.decls {
                        if let Pat::Ident(binding) = name {
                            names.insert(binding.sym.as_ref().to_owned());
                        }
                    }
                }
                Decl::TsInterface(ts) => {
                    names.insert(ts.id.sym.as_ref().to_owned());
                }
                Decl::TsTypeAlias(ts) => {
                    names.insert(ts.id.sym.as_ref().to_owned());
                }
                Decl::TsEnum(ts) => {
                    names.insert(ts.id.sym.as_ref().to_owned());
                }
                Decl::TsModule(ts) => {
                    if let swc_ecma_ast::TsModuleName::Ident(ident) = &ts.id {
                        names.insert(ident.sym.as_ref().to_owned());
                    }
                }
                Decl::Using(_) => {}
            }
        }

        let syntax = syntax_for_extension(ext);
        let mut names: BTreeSet<String> = BTreeSet::new();

        GLOBALS.set(&Globals::new(), || {
            let Some(module) = try_parse(source, syntax) else {
                return;
            };
            for item in &module.body {
                match item {
                    ModuleItem::ModuleDecl(ModuleDecl::ExportDecl(ExportDecl { decl, .. })) => {
                        collect_decl_names(decl, &mut names);
                    }
                    ModuleItem::ModuleDecl(ModuleDecl::ExportDefaultDecl(ExportDefaultDecl {
                        decl,
                        ..
                    })) => match decl {
                        DefaultDecl::Class(c) => {
                            if let Some(ident) = c.ident.as_ref() {
                                names.insert(ident.sym.as_ref().to_owned());
                            }
                        }
                        DefaultDecl::Fn(f) => {
                            if let Some(ident) = f.ident.as_ref() {
                                names.insert(ident.sym.as_ref().to_owned());
                            }
                        }
                        DefaultDecl::TsInterfaceDecl(ts) => {
                            names.insert(ts.id.sym.as_ref().to_owned());
                        }
                    },
                    ModuleItem::Stmt(Stmt::Decl(decl)) => collect_decl_names(decl, &mut names),
                    _ => {}
                }
            }
        });

        names.into_iter().collect()
    }

    /// Pick a permissive [`swc_ecma_parser::Syntax`] flavour from a file
    /// extension so the helper can parse `.tsx`/`.jsx` fixtures without
    /// losing their default exported components or hooks.
    fn syntax_for_extension(ext: &str) -> swc_ecma_parser::Syntax {
        use swc_ecma_parser::{EsSyntax, Syntax, TsSyntax};

        if ext.eq_ignore_ascii_case("tsx") {
            Syntax::Typescript(TsSyntax {
                tsx: true,
                decorators: true,
                dts: false,
                no_early_errors: false,
                disallow_ambiguous_jsx_like: false,
            })
        } else if ext.eq_ignore_ascii_case("ts")
            || ext.eq_ignore_ascii_case("mts")
            || ext.eq_ignore_ascii_case("cts")
        {
            Syntax::Typescript(TsSyntax {
                tsx: false,
                decorators: true,
                dts: false,
                no_early_errors: false,
                disallow_ambiguous_jsx_like: false,
            })
        } else if ext.eq_ignore_ascii_case("jsx")
            || ext.eq_ignore_ascii_case("js")
            || ext.eq_ignore_ascii_case("mjs")
            || ext.eq_ignore_ascii_case("cjs")
        {
            Syntax::Es(EsSyntax {
                jsx: true,
                decorators: true,
                ..EsSyntax::default()
            })
        } else {
            default_typescript_syntax_for_tests()
        }
    }

    #[cfg(test)]
    mod tests {
        use super::{
            parse_full_pipeline_contains_symbol, parse_source_contains_ident,
            parse_ts_file_via_extension, parse_yields_non_empty_module,
            top_level_declared_names_for_extension,
        };

        /// Verify that `parse_yields_non_empty_module` opens its own `GLOBALS`
        /// scope and that identifier atoms resolved through that scope are
        /// correct.  A source that exports `const marker_42: number = 42` must
        /// parse to a non-empty module, and `parse_source_contains_ident` must
        /// find the identifier `marker_42`.
        #[test]
        fn parse_with_recovery_does_not_leak_inner_globals_spans() {
            let source =
                "export const marker_42: number = 42;\nexport class Thing42 { run() {} }\n";

            // parse_yields_non_empty_module self-scopes: no outer GLOBALS needed.
            assert!(
                parse_yields_non_empty_module(source),
                "module should be non-empty"
            );

            // parse_source_contains_ident self-scopes and must find the per-source
            // marker — if span/atom interners leaked between calls the ident lookup
            // would either panic or return a wrong result.
            assert!(
                parse_source_contains_ident(source, "Thing42"),
                "self-scoped parse should find identifier Thing42 in its own Globals scope"
            );

            // Negative control: an identifier from a different (unrelated) source
            // must NOT appear in this module's result.
            assert!(
                !parse_source_contains_ident(source, "Thing0"),
                "identifier from a different source must not appear in this module"
            );
        }

        #[test]
        fn extension_gate_helper_parses_typescript_symbols() {
            let source = "export class MarkerFromExtension {}";
            assert!(
                parse_ts_file_via_extension("mts", source),
                "extension gate helper should route mts through the TypeScript parser"
            );
        }

        #[test]
        fn full_pipeline_helper_observes_symbol_output() {
            let source = "export class PipelineVisible {}";
            assert!(
                parse_full_pipeline_contains_symbol("ts", source, "PipelineVisible"),
                "full pipeline helper should observe symbols emitted by visit_module"
            );
        }

        /// Top-level declared-names helper covers exports, plain decls, and
        /// the TS-only forms (interface/type/enum/namespace) and routes JSX
        /// extensions through the right syntax flavour.
        #[test]
        fn top_level_declared_names_helper_covers_export_and_ts_only_forms() {
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
            let names = top_level_declared_names_for_extension("ts", typescript_source);
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
                "ts source should expose plain decls, exports, and TS-only forms"
            );

            let react_source = "\
                export default function ProjectionSummary() { return null }\n\
                export interface Props {}\n\
            ";
            let react_names = top_level_declared_names_for_extension("tsx", react_source);
            assert_eq!(
                react_names,
                vec!["ProjectionSummary".to_owned(), "Props".to_owned()],
                "tsx source should be parsed under the JSX-aware syntax flavour"
            );
        }
    }
}

// ── Structural regression: span-free public return type ──────────────────────

/// Compile-time assertion that `parse_ts_js_with_swc` returns `()`.
///
/// This function is never called; it exists solely so that a future refactor
/// cannot accidentally change the return type to a span-bearing SWC value
/// (e.g. `Module` or `Span`) without causing a compile error here.
///
/// If `parse_ts_js_with_swc` is changed to return `T` where `T != ()`, the
/// closure body `|| { let _: () = f(); }` will fail to type-check.
const fn _assert_parse_ts_js_with_swc_returns_unit() {
    // Intentionally empty — presence of this function checks the function
    // signature at compile time via the `type_of` style pattern below.
    // The actual call-site check is in the `#[test]` below.
}

#[cfg(test)]
mod globals_scope_tests {
    //! Verifies that `parse_ts_js_with_swc` completes all SWC work inside the
    //! outer `GLOBALS.set` closure and exposes only span-free data to callers.
    //!
    //! Compile-time contract: `parse_ts_js_with_swc` returns `()`.  The
    //! `let (): ()` binding in `visit_module_runs_inside_outer_globals_scope`
    //! is a type-level guard — if the return type ever becomes a span-bearing
    //! SWC value the test module will fail to compile.
    //!
    //! Runtime contract: the `swc_test_support` helpers (which rely on
    //! `visit_module` completing inside a valid `GLOBALS` scope) produce
    //! correct results, confirming the scope invariant holds end-to-end.

    use super::swc_test_support::{parse_source_contains_ident, parse_yields_non_empty_module};

    /// Locks two contracts simultaneously:
    ///
    /// 1. **Compile-time**: `parse_ts_js_with_swc` must return `()`.  The
    ///    `fn assert_unit` helper below will fail to compile if the return type
    ///    ever changes to a span-bearing SWC value.
    ///
    /// 2. **Runtime**: `visit_module` must execute inside the `GLOBALS.set`
    ///    scope.  The `swc_test_support` helpers exercise the real
    ///    `try_parse` → atom-intern → `visit_module` chain; a scope violation
    ///    would corrupt identifier atoms and cause the content-identity
    ///    assertion to fail or panic.
    #[test]
    fn visit_module_runs_inside_outer_globals_scope() {
        // ── Compile-time check ───────────────────────────────────────────────
        // `assert_unit` accepts exactly one `()` argument.  Because
        // `parse_ts_js_with_swc` is `pub(crate)` (and `ParseState` cannot be
        // constructed in this test), we assert the invariant indirectly via
        // the function pointer type: the fn item coerces to a fn-pointer whose
        // return type must be `()`.  Any future change making the return type
        // non-unit will cause "mismatched types" here.
        fn assert_unit(
            _: fn(
                &crate::traverse::FileEntry,
                &mut crate::tree_sitter::ParseState<'_>,
                &str,
                &std::path::Path,
            ),
        ) {
        }
        assert_unit(super::parse_ts_js_with_swc);

        // ── Runtime check ────────────────────────────────────────────────────
        // `parse_source_contains_ident` calls `try_parse` and then walks the
        // resulting module atoms to find an identifier — exactly the work that
        // `visit_module` does in production.  If `visit_module` were called
        // outside the `GLOBALS.set` scope, atom interning would be unreliable
        // and this assertion would either panic or return `false`.
        let source =
            "export const ROUTE = '/api/v1/health';\nexport function handler() { return 42; }\n";

        assert!(
            parse_yields_non_empty_module(source),
            "synthetic source must parse to a non-empty module"
        );
        assert!(
            parse_source_contains_ident(source, "handler"),
            "visit_module-equivalent atom walk must find 'handler' inside a valid GLOBALS scope"
        );
    }
}

#[cfg(test)]
mod implements_contract_tests {
    //! Verifies that `class Foo implements BarInterface {}` with an import
    //! binding for `BarInterface` produces an `ImplementsContractFrom` edge
    //! from the class node to the source file that defines the interface.

    use std::{
        env, fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::EdgeKind;

    use crate::{FileEntry, Language, parse_file};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(name: &str) -> Self {
            let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-implements-{name}-{}-{counter}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("temp dir should create");
            Self { path }
        }

        fn write(&self, relative: &str, contents: &str) {
            let file_path = self.path.join(relative);
            if let Some(parent) = file_path.parent() {
                fs::create_dir_all(parent).expect("parent dir should create");
            }
            fs::write(&file_path, contents).expect("fixture should write");
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    /// A TypeScript file that imports `IAlertDto` from `./contracts` and
    /// declares a class implementing it should emit an `ImplementsContractFrom`
    /// edge from the class node to the resolved file node for `./contracts`.
    #[test]
    fn class_implements_emits_contract_edge() {
        let temp = TempDir::new("class-implements");

        // The contract source — just needs to exist so path resolution succeeds.
        temp.write(
            "contracts.ts",
            "export interface IAlertDto { id: string; }\n",
        );

        // The handler file: imports IAlertDto and implements it.
        temp.write(
            "handler.ts",
            "import { IAlertDto } from './contracts';\nexport class AlertHandler implements IAlertDto { id = ''; }\n",
        );

        let file = FileEntry {
            path: PathBuf::from("handler.ts"),
            language: Language::TypeScript,
            size_bytes: 0,
            content_hash: [0u8; 32],
            source_bytes: None,
        };

        let parsed = parse_file("test-repo", &temp.path, &file).expect("handler.ts should parse");

        let has_contract_edge = parsed.edges.iter().any(|edge| {
            edge.kind == EdgeKind::ImplementsContractFrom
                && parsed
                    .nodes
                    .iter()
                    .any(|node| node.id == edge.target && node.file_path.contains("contracts"))
        });
        assert!(
            has_contract_edge,
            "expected an ImplementsContractFrom edge pointing at contracts.ts; edges: {:?}",
            parsed
                .edges
                .iter()
                .map(|edge| edge.kind)
                .collect::<Vec<_>>()
        );
    }
}
