use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use camino::Utf8PathBuf;
use gather_step_core::{
    EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, node_id, ref_node_id, route_qn,
};
use memchr::memmem;
use quick_cache::{Weighter, sync::Cache};
use rustc_hash::FxHashSet;

use crate::{
    FileEntry,
    frameworks::Framework,
    resolve::ImportBinding,
    traverse::{Language, classify_language},
    tree_sitter::{
        DecoratorCapture, EnrichedCallSite, ParsedFile, SymbolCapture, parse_file_with_context,
        resolve_import_path_pub,
    },
    tsconfig::PathAliases,
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NestjsAugmentation {
    pub nodes: Vec<NodeData>,
    pub edges: Vec<EdgeData>,
}

pub fn augment(parsed: &ParsedFile) -> NestjsAugmentation {
    let mut augmentation = NestjsAugmentation::default();
    let source_content = &*parsed.source;

    for symbol in &parsed.symbols {
        if symbol.node.kind == NodeKind::Class {
            if has_decorator(symbol, "Controller") {
                add_di_edges(parsed, symbol, &mut augmentation);
            }
            if has_decorator(symbol, "Schema") {
                add_entity_node(symbol, &mut augmentation);
            }
            add_guard_edges(parsed, symbol, &mut augmentation);
            // `@Processor('queue-name')` identifies a Bull queue consumer
            // class. We emit the Queue virtual node and a class-level
            // Consumes edge here so there's one-stop definition of the
            // queue regardless of how many `@Process` methods follow.
            add_bull_queue_consumer(parsed, symbol, &mut augmentation);
        }

        if symbol.node.kind == NodeKind::Function {
            add_route_edges(parsed, symbol, &mut augmentation);
            add_topic_consumer_edges(parsed, symbol, &mut augmentation);
            add_guard_edges(parsed, symbol, &mut augmentation);
            // `@Process('job-type')` inside a `@Processor` class handles a
            // specific job type from the parent class's queue. Each handler
            // gets its own Consumes edge pointing at the same queue node
            // (virtual node dedup ensures one Queue node per queue name).
            add_bull_process_handler(parsed, symbol, &mut augmentation);
            // Second pass: inside confirmed consumer methods, detect per-event
            // dispatch via `switch (event.eventType)` or `if` chains and emit
            // fine-grained Event nodes alongside the broad-topic node.
            if is_payload_dispatch_candidate(symbol) {
                add_payload_dispatch_consumer_edges(
                    parsed,
                    source_content,
                    symbol,
                    &mut augmentation,
                );
            }
        }
    }

    add_topic_producer_edges(parsed, source_content, &mut augmentation);
    add_bull_queue_producer_edges(parsed, &mut augmentation);
    add_inject_edges(parsed, &mut augmentation);
    add_guard_callsite_edges(parsed, &mut augmentation);
    augmentation
}

fn add_route_edges(
    parsed: &ParsedFile,
    symbol: &SymbolCapture,
    augmentation: &mut NestjsAugmentation,
) {
    let Some(http_decorator) = symbol.decorators.iter().find(|decorator| {
        matches!(
            decorator.name.as_str(),
            "Get"
                | "Post"
                | "Put"
                | "Delete"
                | "Patch"
                | "Options"
                | "Head"
                | "All"
                | "RequestMapping"
        )
    }) else {
        return;
    };

    // For `@RequestMapping({ method: 'GET', path: '...' })` the HTTP method
    // lives inside the object argument; for all other decorators the decorator
    // name IS the method.
    let method = if http_decorator.name == "RequestMapping" {
        let raw_arg = http_decorator
            .arguments
            .first()
            .cloned()
            .unwrap_or_default();
        let inner = raw_arg
            .trim()
            .trim_start_matches('{')
            .trim_end_matches('}')
            .trim();
        extract_object_key(inner, "method").map_or_else(
            || "GET".to_owned(),
            |m| sanitize_topic_name(m).to_ascii_uppercase(),
        )
    } else {
        http_decorator.name.to_ascii_uppercase()
    };

    let controller_base = symbol
        .parent_class
        .as_ref()
        .and_then(|_| {
            symbol
                .class_decorators
                .iter()
                .find(|decorator| decorator.name == "Controller")
        })
        .map(|decorator| controller_path(parsed, decorator))
        .unwrap_or_default();

    // `@Version('N')` can appear on the method or, as a default, on the
    // controller class. Method-level takes precedence over class-level.
    let version_prefix = symbol
        .decorators
        .iter()
        .find(|d| d.name == "Version")
        .or_else(|| symbol.class_decorators.iter().find(|d| d.name == "Version"))
        .and_then(|d| d.arguments.first())
        .map(|raw| {
            let v = sanitize_topic_name(raw);
            format!("/v{v}")
        })
        .unwrap_or_default();

    // When a version prefix exists, compose version → controller → method
    // through two join_route_path passes so each segment's slashes are
    // normalised correctly.  Without a version, a single pass suffices.
    let versioned_base = if version_prefix.is_empty() {
        controller_base.clone()
    } else {
        join_route_path(&version_prefix, &controller_base)
    };

    // For `@RequestMapping` the route sub-path also comes from the object
    // argument's `path` property; for all other decorators the first string
    // argument is the sub-path.
    let method_path_raw = if http_decorator.name == "RequestMapping" {
        let raw_arg = http_decorator
            .arguments
            .first()
            .cloned()
            .unwrap_or_default();
        let inner = raw_arg
            .trim()
            .trim_start_matches('{')
            .trim_end_matches('}')
            .trim();
        extract_object_key(inner, "path")
            .map(|p| resolve_argument(parsed, p.trim()))
            .unwrap_or_default()
    } else {
        resolve_argument(parsed, &first_string_arg(http_decorator))
    };

    let route_path = join_route_path(&versioned_base, &method_path_raw);
    // Use route_qn so path params (`:id`, `{id}`, `$id`) are normalised to the
    // canonical `:param` form, matching what the frontend emitter produces.
    let qualified_name = route_qn(&method, &route_path);
    let route_node = virtual_node(NodeKind::Route, &qualified_name, &qualified_name, symbol);

    augmentation.nodes.push(route_node.clone());
    augmentation.edges.push(EdgeData {
        source: symbol.node.id,
        target: route_node.id,
        kind: EdgeKind::Serves,
        metadata: EdgeMetadata::default(),
        owner_file: symbol.file_node,
        is_cross_file: false,
    });
}

fn add_topic_consumer_edges(
    parsed: &ParsedFile,
    symbol: &SymbolCapture,
    augmentation: &mut NestjsAugmentation,
) {
    let Some(topic_decorator) = symbol.decorators.iter().find(|decorator| {
        matches!(
            decorator.name.as_str(),
            "MessagePattern" | "EventPattern" | "CustomEventPattern"
        )
    }) else {
        return;
    };

    // Canonical messaging identity: every messaging decorator
    // (`@MessagePattern` / `@EventPattern` / `@CustomEventPattern`) emits on
    // `NodeKind::Event` with the `__event__kafka__` prefix so producer and
    // consumer emissions converge on a single canonical virtual node per
    // topic name. Previously `@MessagePattern` mapped to `NodeKind::Topic`
    // while `@CustomEventPattern` mapped to `NodeKind::Event`, which
    // produced split identities (`__topic__kafka__X` vs `__event__kafka__X`)
    // for the same name and blocked the producer/consumer join when the two
    // sides used different conventions.
    let _ = topic_decorator;
    let kind = NodeKind::Event;
    let prefix = "__event__kafka__";

    for name in topic_names_from_decorator(parsed, topic_decorator, &symbol.node.name) {
        if should_skip_dynamic_topic(&name) {
            continue;
        }
        let qualified_name = format!("{prefix}{name}");
        let virt_node = virtual_node(kind, &qualified_name, &name, symbol);

        augmentation.nodes.push(virt_node.clone());
        augmentation.edges.push(EdgeData {
            source: symbol.node.id,
            target: virt_node.id,
            kind: EdgeKind::Consumes,
            metadata: EdgeMetadata::default(),
            owner_file: symbol.file_node,
            is_cross_file: false,
        });
        augmentation.edges.push(EdgeData {
            source: symbol.node.id,
            target: virt_node.id,
            kind: EdgeKind::UsesEventFrom,
            metadata: EdgeMetadata::default(),
            owner_file: symbol.file_node,
            is_cross_file: false,
        });
    }
}

/// SIMD-accelerated single-needle finder for `eventType` body-scan guards.
///
/// Called per-symbol inside confirmed messaging handlers; the static finder
/// amortises the SIMD setup cost across all files processed in a run.
static EVENT_TYPE_FINDER: OnceLock<memmem::Finder<'static>> = OnceLock::new();

fn event_type_finder() -> &'static memmem::Finder<'static> {
    EVENT_TYPE_FINDER.get_or_init(|| memmem::Finder::new("eventType").into_owned())
}

fn has_messaging_consumer_decorator(symbol: &SymbolCapture) -> bool {
    symbol.decorators.iter().any(|decorator| {
        matches!(
            decorator.name.as_str(),
            "MessagePattern" | "EventPattern" | "CustomEventPattern"
        )
    })
}

fn is_payload_dispatch_candidate(symbol: &SymbolCapture) -> bool {
    has_messaging_consumer_decorator(symbol) || symbol.node.name == "handleEvent"
}

/// Scan for `switch (event.eventType)` / `if (event.eventType ===)` patterns
/// and emit a per-event `Event` virtual node with `Consumes` and
/// `UsesEventFrom` edges.
///
/// Applies to decorated messaging handlers and to the established delegated
/// `handleEvent` dispatcher shape. Undecorated arbitrary methods are skipped
/// so ordinary business logic with an `eventType` switch does not fabricate
/// messaging edges.
fn add_payload_dispatch_consumer_edges(
    parsed: &ParsedFile,
    source_content: &str,
    symbol: &SymbolCapture,
    augmentation: &mut NestjsAugmentation,
) {
    let Some(span) = &symbol.node.span else {
        return;
    };

    let all_lines: Vec<&str> = source_content.lines().collect();
    let start = (span.line_start as usize).saturating_sub(1);
    let end = (span.line_end() as usize).min(all_lines.len());
    if start >= end {
        return;
    }
    let body = &all_lines[start..end];

    // Use a SIMD-accelerated finder to gate the expensive per-line scan: scan
    // each line's bytes with the prebuilt finder rather than 8 sequential
    // `str::contains` calls.
    if !body
        .iter()
        .any(|line| event_type_finder().find(line.as_bytes()).is_some())
    {
        return;
    }

    let raw_values = extract_event_type_values(body);
    let mut seen: FxHashSet<String> = FxHashSet::default();
    for raw in raw_values {
        let Some(resolved) = resolve_topic_decorator_argument(parsed, &raw) else {
            continue;
        };
        if should_skip_dynamic_topic(&resolved) || !seen.insert(resolved.clone()) {
            continue;
        }
        let qualified_name = format!("__event__kafka__{resolved}");
        let virt_node = virtual_node(NodeKind::Event, &qualified_name, &resolved, symbol);
        augmentation.nodes.push(virt_node.clone());
        augmentation.edges.push(EdgeData {
            source: symbol.node.id,
            target: virt_node.id,
            kind: EdgeKind::Consumes,
            metadata: EdgeMetadata::default(),
            owner_file: symbol.file_node,
            is_cross_file: false,
        });
        augmentation.edges.push(EdgeData {
            source: symbol.node.id,
            target: virt_node.id,
            kind: EdgeKind::UsesEventFrom,
            metadata: EdgeMetadata::default(),
            owner_file: symbol.file_node,
            is_cross_file: false,
        });
    }
}

/// Scan body lines for `switch (*.eventType)` case values and `if/else if`
/// comparisons on `*.eventType`.  Returns the raw (unresolved) value strings
/// from each arm.
fn extract_event_type_values(body: &[&str]) -> Vec<String> {
    let mut values = Vec::new();
    let mut in_event_switch = false;
    let mut switch_depth: i32 = 0;

    for line in body {
        let trimmed = line.trim();

        if in_event_switch {
            // Track brace depth; exit switch when we return to depth 0.
            for ch in trimmed.chars() {
                match ch {
                    '{' => switch_depth += 1,
                    '}' => {
                        switch_depth -= 1;
                        if switch_depth <= 0 {
                            in_event_switch = false;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            if in_event_switch && let Some(v) = extract_switch_case_value(trimmed) {
                values.push(v);
            }
            continue;
        }

        // Detect `switch (*.eventType) {`
        if trimmed.contains("switch") && trimmed.contains("eventType") {
            in_event_switch = true;
            switch_depth = trimmed.chars().fold(0_i32, |d, c| match c {
                '{' => d + 1,
                '}' => (d - 1).max(0),
                _ => d,
            });
            continue;
        }

        // Detect `if (*.eventType === X)` / `else if (*.eventType === X)` chains
        if trimmed.contains("eventType")
            && (trimmed.starts_with("if ")
                || trimmed.starts_with("if(")
                || trimmed.contains("else if"))
            && let Some(v) = extract_comparison_rhs(trimmed)
        {
            values.push(v);
        }
    }

    values
}

/// Extract the value from a `case VALUE:` line, stripping surrounding syntax.
/// Returns `None` for `default:` and empty values.
fn extract_switch_case_value(line: &str) -> Option<String> {
    let rest = line.strip_prefix("case ")?;
    let rest = rest
        .trim()
        .trim_end_matches([':', '{', ' '])
        .trim_end_matches(':')
        .trim();
    if rest == "default" || rest.is_empty() {
        return None;
    }
    Some(rest.to_owned())
}

/// Extract the RHS of an `eventType === VALUE` comparison from an if-condition
/// line, returning the raw token before any closing paren or brace.
fn extract_comparison_rhs(line: &str) -> Option<String> {
    let pos = line.find("eventType")?;
    let after = line[pos + "eventType".len()..].trim_start();
    let after = after
        .strip_prefix("===")
        .or_else(|| after.strip_prefix("=="))?;
    let after = after.trim();
    let end = after
        .find([')', ' ', '\n', ';', '{'])
        .unwrap_or(after.len());
    let value = after[..end].trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_owned())
    }
}

fn add_topic_producer_edges(
    parsed: &ParsedFile,
    source_content: &str,
    augmentation: &mut NestjsAugmentation,
) {
    for call_site in &parsed.call_sites {
        let Some((hint, kind)) = producer_messaging_operation(call_site) else {
            continue;
        };

        let Some(topic_name) = resolve_producer_topic_name(parsed, call_site) else {
            continue;
        };
        let transport = detect_transport(hint);
        let type_segment = match kind {
            NodeKind::Event => "event",
            _ => "topic",
        };
        let prefix = format!("__{type_segment}__{transport}__");
        let qualified_name = format!("{prefix}{topic_name}");
        let producer_node = NodeData {
            id: ref_node_id(kind, &qualified_name),
            kind,
            repo: parsed.file_node.repo.clone(),
            file_path: parsed.file_node.file_path.clone(),
            name: topic_name.clone(),
            qualified_name: Some(qualified_name.clone()),
            external_id: Some(qualified_name),
            signature: None,
            visibility: None,
            span: call_site.span.clone(),
            is_virtual: true,
        };

        augmentation.nodes.push(producer_node.clone());
        augmentation.edges.push(EdgeData {
            source: call_site.owner_id,
            target: producer_node.id,
            kind: EdgeKind::Publishes,
            metadata: EdgeMetadata::default(),
            owner_file: parsed.file_node.id,
            is_cross_file: false,
        });
        if kind == NodeKind::Event {
            augmentation.edges.push(EdgeData {
                source: call_site.owner_id,
                target: producer_node.id,
                kind: EdgeKind::ProducesEventFor,
                metadata: EdgeMetadata::default(),
                owner_file: parsed.file_node.id,
                is_cross_file: false,
            });
        }

        // If the call passes a payload object with a static `eventType`
        // key, also emit a fine-grained Event node for that specific event
        // type — this lets the producer converge with a consumer that
        // identifies events by `eventType` inside `switch` / `if` dispatch
        // bodies.
        let event_types = call_site
            .raw_arguments
            .as_deref()
            .map(|raw_args| {
                extract_event_type_values_from_producer_call(
                    parsed,
                    source_content,
                    call_site.span.as_ref(),
                    raw_args,
                )
            })
            .unwrap_or_default();
        for resolved in event_types {
            let event_qn = format!("__event__kafka__{resolved}");
            let fine_node = NodeData {
                id: ref_node_id(NodeKind::Event, &event_qn),
                kind: NodeKind::Event,
                repo: parsed.file_node.repo.clone(),
                file_path: parsed.file_node.file_path.clone(),
                name: resolved.clone(),
                qualified_name: Some(event_qn.clone()),
                external_id: Some(event_qn),
                signature: None,
                visibility: None,
                span: call_site.span.clone(),
                is_virtual: true,
            };
            let fine_id = fine_node.id;
            augmentation.nodes.push(fine_node);
            augmentation.edges.push(EdgeData {
                source: call_site.owner_id,
                target: fine_id,
                kind: EdgeKind::Publishes,
                metadata: EdgeMetadata::default(),
                owner_file: parsed.file_node.id,
                is_cross_file: false,
            });
            augmentation.edges.push(EdgeData {
                source: call_site.owner_id,
                target: fine_id,
                kind: EdgeKind::ProducesEventFor,
                metadata: EdgeMetadata::default(),
                owner_file: parsed.file_node.id,
                is_cross_file: false,
            });
        }
    }
}

/// Extract the raw `eventType` value from a call's second argument payload.
///
/// Expects `raw_arguments` to be the full raw call arguments string (with
/// outer parens). Returns the trimmed value string for `eventType: <value>`.
fn extract_event_type_from_payload(raw_arguments: &str) -> Option<String> {
    let inner = raw_arguments
        .trim()
        .trim_start_matches('(')
        .trim_end_matches(')');
    let args = split_top_level(inner, ',');
    let payload = args.get(1).copied()?.trim();
    if !payload.starts_with('{') || !payload.ends_with('}') {
        return None;
    }
    let body = &payload[1..payload.len() - 1];
    for entry in split_top_level(body, ',') {
        let entry = entry.trim();
        let Some((key, value)) = entry.split_once(':') else {
            continue;
        };
        if key.trim() == "eventType" {
            return Some(value.trim().to_owned());
        }
    }
    None
}

fn extract_event_type_values_from_producer_call(
    parsed: &ParsedFile,
    source_content: &str,
    call_span: Option<&gather_step_core::SourceSpan>,
    raw_arguments: &str,
) -> Vec<String> {
    let mut values = Vec::new();

    // `emit(topic, { eventType: ... })` / `send(topic, { eventType: ... })`
    if let Some(value) = extract_event_type_from_payload(raw_arguments) {
        values.extend(resolve_event_type_expression_values(
            parsed,
            source_content,
            call_span,
            &value,
        ));
    }

    // `sendMessage({ topic: ..., message: { eventType: ... } })`
    if let Some(payload) = extract_call_argument(raw_arguments, 0)
        && let Some(message_value) = extract_object_key_value(payload, "message")
        && let Some(value) = extract_object_key_value(message_value, "eventType")
            .map(str::to_owned)
            .or_else(|| {
                resolve_helper_payload_event_type_argument(parsed, source_content, message_value)
            })
    {
        values.extend(resolve_event_type_expression_values(
            parsed,
            source_content,
            call_span,
            &value,
        ));
    }

    // `sendMessage({ topic: ..., eventType: ..., payload: ... })` — a common
    // variant where `eventType` lives at the top level of the first-argument
    // options object instead of under a nested `message` key.
    if let Some(payload) = extract_call_argument(raw_arguments, 0)
        && let Some(value) = extract_object_key_value(payload, "eventType")
    {
        values.extend(resolve_event_type_expression_values(
            parsed,
            source_content,
            call_span,
            value,
        ));
    }

    values.sort();
    values.dedup();
    values
}

fn resolve_event_type_expression_values(
    parsed: &ParsedFile,
    source_content: &str,
    call_span: Option<&gather_step_core::SourceSpan>,
    raw_expression: &str,
) -> Vec<String> {
    let trimmed = raw_expression.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }

    if let Some((when_true, when_false)) = split_top_level_ternary(trimmed) {
        let mut values =
            resolve_event_type_expression_values(parsed, source_content, call_span, &when_true);
        values.extend(resolve_event_type_expression_values(
            parsed,
            source_content,
            call_span,
            &when_false,
        ));
        values.sort();
        values.dedup();
        return values;
    }

    if let Some(resolved) = resolve_topic_decorator_argument(parsed, trimmed)
        && !should_skip_dynamic_topic(&resolved)
    {
        return vec![resolved];
    }

    if is_identifier_like(trimmed)
        && let Some(span) = call_span
        && let Some(local_value) =
            resolve_local_expression_before_line(source_content, trimmed, span.line_start)
    {
        return resolve_event_type_expression_values(
            parsed,
            source_content,
            call_span,
            &local_value,
        );
    }

    Vec::new()
}

fn resolve_helper_payload_event_type_argument(
    parsed: &ParsedFile,
    source_content: &str,
    raw_expression: &str,
) -> Option<String> {
    let (helper_name, helper_args) = parse_method_call_expression(raw_expression)?;
    if let Some((params, event_param)) =
        local_helper_event_type_binding(parsed, source_content, &helper_name)
            .or_else(|| imported_helper_event_type_binding(parsed, &helper_name))
    {
        let param_index = params.iter().position(|param| param == &event_param)?;
        return helper_args.get(param_index).cloned();
    }
    None
}

fn local_helper_event_type_binding(
    parsed: &ParsedFile,
    source_content: &str,
    helper_name: &str,
) -> Option<(Vec<String>, String)> {
    let helper_symbol = parsed.symbols.iter().find(|symbol| {
        symbol.node.kind == NodeKind::Function
            && symbol.node.name.trim_start_matches('#') == helper_name
    })?;
    let params = function_parameter_names(helper_symbol.node.signature.as_deref()?);
    let event_param = helper_return_event_type_binding(source_content, helper_symbol)?;
    Some((params, event_param))
}

fn imported_helper_event_type_binding(
    parsed: &ParsedFile,
    helper_name: &str,
) -> Option<(Vec<String>, String)> {
    let binding = parsed
        .import_bindings
        .iter()
        .find(|binding| binding.local_name == helper_name && !binding.is_type_only)?;
    let imported_name = binding.imported_name.as_deref().unwrap_or(helper_name);
    let path = binding.resolved_path.as_ref()?;
    let imported = parse_imported_file(parsed, path)?;
    let helper_symbol = imported.symbols.iter().find(|symbol| {
        symbol.node.kind == NodeKind::Function
            && symbol.node.name.trim_start_matches('#') == imported_name
    })?;
    let params = function_parameter_names(helper_symbol.node.signature.as_deref()?);
    let event_param = helper_return_event_type_binding(imported.source.as_ref(), helper_symbol)?;
    Some((params, event_param))
}

fn parse_method_call_expression(raw_expression: &str) -> Option<(String, Vec<String>)> {
    let trimmed = raw_expression.trim();
    let open = trimmed.find('(')?;
    let close = find_matching_close_local(trimmed, open)?;
    let callee = trimmed[..open].trim();
    let helper_name = callee
        .rsplit('.')
        .next()
        .unwrap_or(callee)
        .trim()
        .trim_start_matches('#')
        .to_owned();
    if helper_name.is_empty() {
        return None;
    }
    let args = split_top_level(&trimmed[open + 1..close], ',')
        .into_iter()
        .map(ToOwned::to_owned)
        .collect();
    Some((helper_name, args))
}

fn function_parameter_names(signature: &str) -> Vec<String> {
    let Some(open) = signature.find('(') else {
        return Vec::new();
    };
    let Some(close) = find_matching_close_local(signature, open) else {
        return Vec::new();
    };
    split_top_level(&signature[open + 1..close], ',')
        .into_iter()
        .filter_map(|param| {
            let param = param.trim();
            if param.is_empty() {
                return None;
            }
            let name = param.split_once(':').map_or(param, |(name, _)| name).trim();
            let name = name
                .split_once('=')
                .map_or(name, |(name, _)| name)
                .trim()
                .trim_start_matches("public ")
                .trim_start_matches("private ")
                .trim_start_matches("protected ")
                .trim_start_matches("readonly ")
                .trim_start_matches("...")
                .trim()
                .trim_end_matches('?')
                .trim();
            (!name.is_empty()).then(|| name.to_owned())
        })
        .collect()
}

fn helper_return_event_type_binding(
    source_content: &str,
    helper_symbol: &SymbolCapture,
) -> Option<String> {
    let body = symbol_body_text(source_content, helper_symbol)?;
    let object = extract_returned_object_literal(&body)?;
    extract_object_key_value(&object, "eventType")
        .map(str::to_owned)
        .or_else(|| extract_object_shorthand_key(&object, "eventType"))
}

fn symbol_body_text(source_content: &str, symbol: &SymbolCapture) -> Option<String> {
    let span = symbol.node.span.as_ref()?;
    let all_lines: Vec<&str> = source_content.lines().collect();
    let start = (span.line_start as usize).saturating_sub(1);
    let end = (span.line_end() as usize).min(all_lines.len());
    (start < end).then(|| all_lines[start..end].join("\n"))
}

fn extract_returned_object_literal(body: &str) -> Option<String> {
    let return_pos = body.find("return")?;
    let after_return = &body[return_pos + "return".len()..];
    let open = after_return.find('{')?;
    let close = find_matching_close_local(after_return, open)?;
    Some(after_return[open..=close].to_owned())
}

fn extract_object_shorthand_key(raw: &str, key: &str) -> Option<String> {
    let trimmed = raw.trim();
    if !trimmed.starts_with('{') || !trimmed.ends_with('}') {
        return None;
    }
    let body = &trimmed[1..trimmed.len() - 1];
    for entry in split_top_level(body, ',') {
        let entry = entry.trim();
        if entry.is_empty() || entry.contains(':') {
            continue;
        }
        if sanitize_topic_name(entry) == key {
            return Some(key.to_owned());
        }
    }
    None
}

fn resolve_local_expression_before_line(
    source_content: &str,
    name: &str,
    line_start: u32,
) -> Option<String> {
    let lines: Vec<&str> = source_content.lines().collect();
    let upper = (line_start as usize).saturating_sub(1).min(lines.len());
    for index in (0..upper).rev() {
        let trimmed = lines[index].trim();
        for prefix in ["const ", "let ", "var "] {
            let Some(rest) = trimmed.strip_prefix(prefix) else {
                continue;
            };
            let Some(remainder) = rest.strip_prefix(name) else {
                continue;
            };
            let Some(after_equals) = remainder.trim_start().strip_prefix('=') else {
                continue;
            };
            let mut expr = after_equals.trim().to_owned();
            let mut next = index + 1;
            while !expr.contains(';') && next < upper {
                expr.push(' ');
                expr.push_str(lines[next].trim());
                next += 1;
            }
            return Some(expr.trim_end_matches(';').trim().to_owned());
        }
    }
    None
}

fn split_top_level_ternary(raw: &str) -> Option<(String, String)> {
    let mut question = None;
    let mut bracket_depth = 0_u32;
    let mut brace_depth = 0_u32;
    let mut paren_depth = 0_u32;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    let mut escape = false;
    let mut ternary_depth = 0_u32;

    for (index, ch) in raw.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        match ch {
            '\\' if in_single || in_double || in_backtick => {
                escape = true;
            }
            '\'' if !in_double && !in_backtick => in_single = !in_single,
            '"' if !in_single && !in_backtick => in_double = !in_double,
            '`' if !in_single && !in_double => in_backtick = !in_backtick,
            _ if in_single || in_double || in_backtick => {}
            '[' => bracket_depth = bracket_depth.saturating_add(1),
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            '{' => brace_depth = brace_depth.saturating_add(1),
            '}' => brace_depth = brace_depth.saturating_sub(1),
            '(' => paren_depth = paren_depth.saturating_add(1),
            ')' => paren_depth = paren_depth.saturating_sub(1),
            '?' if bracket_depth == 0 && brace_depth == 0 && paren_depth == 0 => {
                ternary_depth += 1;
                question.get_or_insert(index);
            }
            ':' if bracket_depth == 0 && brace_depth == 0 && paren_depth == 0 => {
                if ternary_depth == 1 {
                    let question = question?;
                    let when_true = raw[question + 1..index].trim().to_owned();
                    let when_false = raw[index + 1..].trim().to_owned();
                    if when_true.is_empty() || when_false.is_empty() {
                        return None;
                    }
                    return Some((when_true, when_false));
                }
                ternary_depth = ternary_depth.saturating_sub(1);
            }
            _ => {}
        }
    }
    None
}

fn find_matching_close_local(input: &str, open_pos: usize) -> Option<usize> {
    let open_char = input.get(open_pos..=open_pos)?.chars().next()?;
    let close_char = match open_char {
        '(' => ')',
        '{' => '}',
        '[' => ']',
        _ => return None,
    };
    let mut depth = 0_u32;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    let mut escape = false;
    for (index, ch) in input.char_indices().skip_while(|(idx, _)| *idx < open_pos) {
        if escape {
            escape = false;
            continue;
        }
        match ch {
            '\\' if in_single || in_double || in_backtick => {
                escape = true;
            }
            '\'' if !in_double && !in_backtick => in_single = !in_single,
            '"' if !in_single && !in_backtick => in_double = !in_double,
            '`' if !in_single && !in_double => in_backtick = !in_backtick,
            _ if in_single || in_double || in_backtick => {}
            _ if ch == open_char => depth += 1,
            _ if ch == close_char => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

fn extract_object_key_value<'a>(raw: &'a str, key: &str) -> Option<&'a str> {
    let trimmed = raw.trim();
    if !trimmed.starts_with('{') || !trimmed.ends_with('}') {
        return None;
    }
    let body = &trimmed[1..trimmed.len() - 1];
    for entry in split_top_level(body, ',') {
        let entry = entry.trim();
        let Some((entry_key, entry_value)) = entry.split_once(':') else {
            continue;
        };
        let normalized = sanitize_topic_name(entry_key);
        if normalized == key {
            return Some(entry_value.trim());
        }
    }
    None
}

fn add_guard_edges(
    parsed: &ParsedFile,
    symbol: &SymbolCapture,
    augmentation: &mut NestjsAugmentation,
) {
    let repo_root = derive_repo_root(parsed);
    for decorator in symbol
        .decorators
        .iter()
        .chain(symbol.class_decorators.iter())
        .filter(|decorator| decorator.name == "UseGuards")
    {
        for guard in decorator
            .arguments
            .iter()
            .flat_map(|argument| split_top_level(argument, ','))
            .map(str::trim)
            .filter(|guard| !guard.is_empty())
        {
            let guard_name = guard
                .trim_start_matches("new ")
                .split_once('(')
                .map_or(guard, |(name, _)| name)
                .trim();
            let binding = parsed
                .import_bindings
                .iter()
                .find(|binding| binding.local_name == guard_name);
            let guard_relative_path = binding
                .and_then(|binding| binding.resolved_path.as_ref())
                .and_then(|resolved| {
                    resolved.strip_prefix(&repo_root).ok().map(|path| {
                        Utf8PathBuf::from_path_buf(path.to_path_buf()).unwrap_or_else(|p| {
                            Utf8PathBuf::from(p.to_string_lossy().replace('\\', "/"))
                        })
                    })
                });
            let file_target = guard_relative_path
                .as_ref()
                .map_or(symbol.file_node, |relative| {
                    let s = relative.as_str();
                    node_id(&parsed.file_node.repo, s, NodeKind::File, s)
                });
            augmentation.edges.push(EdgeData {
                source: symbol.node.id,
                target: file_target,
                kind: EdgeKind::UsesGuardFrom,
                metadata: EdgeMetadata::default(),
                owner_file: symbol.file_node,
                is_cross_file: file_target != symbol.file_node,
            });

            // Same-repo guard: the resolver stripped the consumer repo root
            // from the resolved path, so we can reconstruct a deterministic
            // `Class` NodeId for the guard declaration and emit an additive
            // edge there. This lets `impact` / `shared_contract_impact`
            // walk inbound guard usage back to the guard's canonical class
            // instead of only to its file (which is too coarse for
            // `is_canonical_boundary` anchor resolution).
            if let Some(relative) = guard_relative_path.as_ref() {
                let class_target = node_id(
                    &parsed.file_node.repo,
                    relative.as_str(),
                    NodeKind::Class,
                    guard_name,
                );
                augmentation.edges.push(EdgeData {
                    source: symbol.node.id,
                    target: class_target,
                    kind: EdgeKind::UsesGuardFrom,
                    metadata: EdgeMetadata::default(),
                    owner_file: symbol.file_node,
                    is_cross_file: class_target != symbol.file_node,
                });
            }

            // Cross-repo guard: the consumer imports a guard whose resolved
            // path lies outside its own repo root (e.g., a shared-contracts
            // guard package imported by a backend service). We can't
            // reconstruct the guard's owning repo or its in-repo path from
            // the parser alone, so emit a virtual `SharedSymbol` keyed by
            // `__guard__<import_source>__<guard_name>` and edge the
            // consumer's method to it. Because the virtual node id is
            // derived from `(kind, qualified_name)` only, all consumer
            // repos that import the same guard converge on the same virtual
            // node — which lets `shared_contract_candidate_ids`
            // peer-matching surface the consumers together, and pairs with
            // the existing `is_canonical_boundary = true` rule for virtual
            // nodes so `compare_impact_rank` gives the canonical source
            // primacy. Without this, cross-repo `UsesGuardFrom` edges
            // degenerate to self-targeting file edges and never reach the
            // canonical guard declaration.
            if guard_relative_path.is_none()
                && let Some(source) = binding.map(|binding| binding.source.as_str())
                && !source.is_empty()
            {
                let qualified_name = format!("__guard__{source}__{guard_name}");
                let virt_node =
                    virtual_node(NodeKind::SharedSymbol, &qualified_name, guard_name, symbol);
                augmentation.nodes.push(virt_node.clone());
                augmentation.edges.push(EdgeData {
                    source: symbol.node.id,
                    target: virt_node.id,
                    kind: EdgeKind::UsesGuardFrom,
                    metadata: EdgeMetadata::default(),
                    owner_file: symbol.file_node,
                    is_cross_file: false,
                });
            }
        }
    }
}

fn add_guard_callsite_edges(parsed: &ParsedFile, augmentation: &mut NestjsAugmentation) {
    for call_site in &parsed.call_sites {
        if call_site.callee_name != "UseGuards" {
            continue;
        }
        augmentation.edges.push(EdgeData {
            source: call_site.owner_id,
            target: call_site.owner_file,
            kind: EdgeKind::UsesGuardFrom,
            metadata: EdgeMetadata::default(),
            owner_file: parsed.file_node.id,
            is_cross_file: false,
        });
    }
}

fn derive_repo_root(parsed: &ParsedFile) -> std::path::PathBuf {
    let mut root = parsed.source_path.clone();
    for _ in parsed.file.path.components() {
        root.pop();
    }
    root
}

/// Infers the messaging transport from the qualified callee hint.
///
/// The hint is the full dotted receiver expression, e.g.
/// `this.serviceBusClient.emit`. A case-insensitive substring match is used
/// because naming conventions vary (`ServiceBusClient`, `sbClient`, etc.).
///
/// | Hint contains    | Transport     |
/// |------------------|---------------|
/// | `servicebus`     | `servicebus`  |
/// | `pubsub` / `webpubsub` | `pubsub` |
/// | _(anything else)_ | `kafka`      |
fn detect_transport(qualified_hint: &str) -> &'static str {
    // Work on the portion before the last `.` (receiver object path only) so
    // the operation name itself (`emit`, `send`) doesn't skew the match.
    let receiver = qualified_hint
        .rsplit_once('.')
        .map_or(qualified_hint, |(recv, _)| recv);
    // A transport-specific client must both contain the transport keyword at a
    // genuine word boundary AND include "client" in its name (e.g.
    // `serviceBusClient`, `webPubSubClient`). This prevents `pubsubEnabled`
    // from matching pubsub just because "pubsub" is a prefix of the name.
    if transport_keyword_match(receiver, "servicebus")
        && contains_ignore_ascii_case(receiver, "client")
    {
        "servicebus"
    } else if transport_keyword_match(receiver, "pubsub")
        && contains_ignore_ascii_case(receiver, "client")
    {
        "pubsub"
    } else {
        "kafka"
    }
}

/// Checks whether `keyword` appears as a word boundary within `receiver`.
///
/// A match is valid only when the character immediately following the keyword
/// in the *original* (un-lowercased) string is:
/// - an uppercase ASCII letter (camelCase boundary: `serviceBusClient`), or
/// - a non-alphanumeric character (separator boundary: `service_bus_client`), or
/// - the end of the string.
///
/// This prevents "servicebus" from matching inside `myServiceBusyClient`
/// because the character after "servicebus" is `y`, which is a lowercase
/// alphanumeric — not a word boundary.
#[expect(
    clippy::disallowed_methods,
    reason = "to_ascii_lowercase is needed here to produce a searchable lowercase copy \
              while preserving the original for boundary-character inspection; \
              eq_ignore_ascii_case cannot replace this because we must correlate \
              match positions back to the original bytes"
)]
fn transport_keyword_match(receiver: &str, keyword: &str) -> bool {
    let hay_lower = receiver.to_ascii_lowercase();
    let mut start = 0;
    while let Some(idx) = hay_lower[start..].find(keyword) {
        let abs = start + idx;
        let after = receiver.as_bytes().get(abs + keyword.len()).copied();
        // Word boundary: next char is uppercase, non-alphanumeric, or end of string.
        if after.is_none_or(|b| b.is_ascii_uppercase() || !b.is_ascii_alphanumeric()) {
            return true;
        }
        start = abs + 1;
    }
    false
}

/// Case-insensitive ASCII substring check that avoids heap allocation.
///
/// Uses a sliding window of `needle.len()` bytes and compares each window
/// against the needle with `eq_ignore_ascii_case`. Both the haystack and the
/// needle must be valid UTF-8 (ensured by Rust's type system).
fn contains_ignore_ascii_case(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    let n = needle.len();
    haystack
        .as_bytes()
        .windows(n)
        // SAFETY: `windows(n)` produces byte slices of exactly `n` bytes
        // that are a contiguous sub-slice of the original UTF-8 str; the
        // needle is also ASCII so comparing byte-by-byte is correct.
        .any(|window| window.eq_ignore_ascii_case(needle.as_bytes()))
}

fn add_di_edges(
    parsed: &ParsedFile,
    symbol: &SymbolCapture,
    augmentation: &mut NestjsAugmentation,
) {
    for dependency in &symbol.constructor_dependencies {
        let qualified_name = format!("__di__{dependency}");
        let dependency_node = NodeData {
            id: ref_node_id(NodeKind::Class, &qualified_name),
            kind: NodeKind::Class,
            repo: parsed.file_node.repo.clone(),
            file_path: parsed.file_node.file_path.clone(),
            name: dependency.clone(),
            qualified_name: Some(qualified_name.clone()),
            external_id: Some(qualified_name),
            signature: None,
            visibility: None,
            span: symbol.node.span.clone(),
            is_virtual: true,
        };
        augmentation.nodes.push(dependency_node.clone());
        augmentation.edges.push(EdgeData {
            source: symbol.node.id,
            target: dependency_node.id,
            kind: EdgeKind::DependsOn,
            metadata: EdgeMetadata::default(),
            owner_file: symbol.file_node,
            is_cross_file: false,
        });
    }
}

fn add_entity_node(symbol: &SymbolCapture, augmentation: &mut NestjsAugmentation) {
    let qualified_name = format!(
        "__entity__{}",
        symbol
            .node
            .qualified_name
            .clone()
            .unwrap_or_else(|| symbol.node.name.clone())
    );
    let entity = NodeData {
        id: ref_node_id(NodeKind::Entity, &qualified_name),
        kind: NodeKind::Entity,
        repo: symbol.node.repo.clone(),
        file_path: symbol.node.file_path.clone(),
        name: symbol.node.name.clone(),
        qualified_name: Some(qualified_name.clone()),
        external_id: Some(qualified_name),
        signature: symbol.node.signature.clone(),
        visibility: symbol.node.visibility.clone(),
        span: symbol.node.span.clone(),
        is_virtual: true,
    };
    augmentation.nodes.push(entity.clone());
    augmentation.edges.push(EdgeData {
        source: symbol.node.id,
        target: entity.id,
        kind: EdgeKind::PersistsTo,
        metadata: EdgeMetadata::default(),
        owner_file: symbol.file_node,
        is_cross_file: false,
    });
}

/// Emit a `Queue` virtual node + class-level `Consumes` edge for a class
/// carrying the `@Processor('queue-name')` decorator (`NestJS` Bull integration).
///
/// Returns the queue's `NodeId` if one was emitted so the method-level handler
/// emitter (`add_bull_process_handler`) can reuse it via the same QN-based
/// deduplication.
fn add_bull_queue_consumer(
    parsed: &ParsedFile,
    symbol: &SymbolCapture,
    augmentation: &mut NestjsAugmentation,
) {
    let Some(queue_name) = bull_queue_name_from_class(symbol) else {
        return;
    };
    let queue_node = bull_queue_node(parsed, symbol, &queue_name);
    let queue_id = queue_node.id;
    augmentation.nodes.push(queue_node);
    augmentation.edges.push(EdgeData {
        source: symbol.node.id,
        target: queue_id,
        kind: EdgeKind::Consumes,
        metadata: EdgeMetadata::default(),
        owner_file: symbol.file_node,
        is_cross_file: false,
    });
}

/// Emit a `Consumes` edge from a function to its parent class's queue when the
/// function has `@Process('job-type')` and the enclosing class has `@Processor`.
///
/// The same Queue virtual node that `add_bull_queue_consumer` produced is
/// referenced here via deterministic QN — `ref_node_id(Queue, qn)` yields the
/// same `NodeId`, so the edge target points at one canonical queue node.
fn add_bull_process_handler(
    parsed: &ParsedFile,
    symbol: &SymbolCapture,
    augmentation: &mut NestjsAugmentation,
) {
    if !has_own_decorator(symbol, "Process") {
        return;
    }
    let Some(queue_name) = bull_queue_name_from_class_decorators(&symbol.class_decorators) else {
        // `@Process` on a function whose parent class has no `@Processor`
        // is malformed NestJS code — skip rather than guess.
        return;
    };
    let queue_node = bull_queue_node(parsed, symbol, &queue_name);
    let queue_id = queue_node.id;
    augmentation.nodes.push(queue_node);
    augmentation.edges.push(EdgeData {
        source: symbol.node.id,
        target: queue_id,
        kind: EdgeKind::Consumes,
        metadata: EdgeMetadata::default(),
        owner_file: symbol.file_node,
        is_cross_file: false,
    });
}

/// Detect Bull queue producer call sites and emit `Publishes` edges.
///
/// Matches `this.<field>.add('queue-name', ...)` patterns where the receiver
/// starts with `this.` (an injected queue client) and the method name is
/// `"add"`.  The first string literal argument is treated as the queue name.
///
/// This mirrors how `add_topic_producer_edges` detects Kafka `.send()` /
/// `.emit()` calls, applied to the Bull queue protocol.
fn add_bull_queue_producer_edges(parsed: &ParsedFile, augmentation: &mut NestjsAugmentation) {
    use gather_step_core::queue_qn;

    for call_site in &parsed.call_sites {
        let Some(hint) = call_site.callee_qualified_hint.as_deref() else {
            continue;
        };

        // Must be a receiver-qualified call starting with `this.`
        let receiver = hint.rsplit_once('.').map_or(hint, |(recv, _)| recv);
        let receiver_starts_with_this = receiver.eq_ignore_ascii_case("this")
            || receiver
                .get(..5)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("this."));
        if !receiver_starts_with_this {
            continue;
        }

        // Operation must be `.add(...)` (Bull queue producer API).
        let operation = hint.rsplit('.').next().unwrap_or(hint);
        if !operation.eq_ignore_ascii_case("add") {
            continue;
        }

        let Some(queue_name) = call_site.literal_argument.as_ref() else {
            continue;
        };
        if should_skip_dynamic_topic(queue_name) {
            continue;
        }
        let Some(queue_name) = sanitize_queue_name(queue_name) else {
            continue;
        };

        let qualified_name = queue_qn("bull", &queue_name);
        let queue_node = NodeData {
            id: ref_node_id(NodeKind::Queue, &qualified_name),
            kind: NodeKind::Queue,
            repo: parsed.file_node.repo.clone(),
            file_path: parsed.file_node.file_path.clone(),
            name: queue_name.clone(),
            qualified_name: Some(qualified_name.clone()),
            external_id: Some(qualified_name),
            signature: None,
            visibility: None,
            span: call_site.span.clone(),
            is_virtual: true,
        };

        augmentation.nodes.push(queue_node.clone());
        augmentation.edges.push(EdgeData {
            source: call_site.owner_id,
            target: queue_node.id,
            kind: EdgeKind::Publishes,
            metadata: EdgeMetadata::default(),
            owner_file: parsed.file_node.id,
            is_cross_file: false,
        });
    }
}

/// Builds a deterministic Queue virtual node for the given Bull queue name.
/// Callers supply `symbol` for source location and repo/file provenance; the
/// `NodeId` is computed purely from the QN so two processors of the same queue
/// (even across files) produce the same node.
fn bull_queue_node(parsed: &ParsedFile, symbol: &SymbolCapture, queue_name: &str) -> NodeData {
    let qualified_name = format!("__queue__bull__{queue_name}");
    NodeData {
        id: ref_node_id(NodeKind::Queue, &qualified_name),
        kind: NodeKind::Queue,
        repo: parsed.file_node.repo.clone(),
        file_path: parsed.file_node.file_path.clone(),
        name: queue_name.to_owned(),
        qualified_name: Some(qualified_name.clone()),
        external_id: Some(qualified_name),
        signature: None,
        visibility: None,
        span: symbol.node.span.clone(),
        is_virtual: true,
    }
}

fn bull_queue_name_from_class(symbol: &SymbolCapture) -> Option<String> {
    symbol
        .decorators
        .iter()
        .find(|decorator| decorator.name == "Processor")
        .and_then(|decorator| decorator.arguments.first())
        .and_then(|raw| sanitize_queue_name(raw))
}

fn bull_queue_name_from_class_decorators(decorators: &[DecoratorCapture]) -> Option<String> {
    decorators
        .iter()
        .find(|decorator| decorator.name == "Processor")
        .and_then(|decorator| decorator.arguments.first())
        .and_then(|raw| sanitize_queue_name(raw))
}

/// Like [`has_decorator`], but only checks the symbol's own decorators —
/// ignoring those inherited from the enclosing class. Used when the check
/// must apply to the method, not the class (e.g., `@Process` is a method
/// decorator, not a class one).
fn has_own_decorator(symbol: &SymbolCapture, name: &str) -> bool {
    symbol
        .decorators
        .iter()
        .any(|decorator| decorator.name == name)
}

fn has_decorator(symbol: &SymbolCapture, name: &str) -> bool {
    symbol
        .decorators
        .iter()
        .any(|decorator| decorator.name == name)
        || symbol
            .class_decorators
            .iter()
            .any(|decorator| decorator.name == name)
}

fn controller_path(parsed: &ParsedFile, decorator: &DecoratorCapture) -> String {
    let raw = first_string_arg(decorator);
    let trimmed = raw.trim();

    // Object-literal form: @Controller({ path: 'x', ... })
    if trimmed.starts_with('{') {
        let inner = trimmed.trim_start_matches('{').trim_end_matches('}').trim();
        // Find the `path` key in any position: `path: ...`, `'path': ...`, `"path": ...`
        if let Some(path_value) = extract_object_key(inner, "path") {
            let resolved = resolve_argument(parsed, path_value.trim());
            return format!("/{}", sanitize_topic_name(&resolved));
        }
        // No `path` key found — object with no path (e.g. version-only).
        return String::new();
    }

    // String literal or identifier form: @Controller('prefix') or @Controller(PREFIX)
    let resolved = resolve_argument(parsed, &raw);
    if resolved.starts_with('/') {
        resolved
    } else if resolved.is_empty() {
        String::new()
    } else {
        format!("/{}", sanitize_topic_name(&resolved))
    }
}

/// Extract the value of a named key from a JS object literal's inner text.
///
/// Handles bare, single-quoted, and double-quoted key forms:
/// - `key: value`
/// - `'key': value`
/// - `"key": value`
///
/// Returns a slice of the value text (before the next top-level comma) if
/// found, or `None` if the key is absent.
fn extract_object_key<'a>(inner: &'a str, key: &str) -> Option<&'a str> {
    for prefix in [
        format!("{key}:"),
        format!("'{key}':"),
        format!("\"{key}\":"),
    ] {
        if let Some(pos) = inner.find(prefix.as_str()) {
            let after_colon = inner[pos + prefix.len()..].trim_start();
            // Extract until the next top-level comma or end.
            let value_end = split_top_level(after_colon, ',')
                .into_iter()
                .next()
                .map_or(after_colon.len(), str::len);
            return Some(after_colon[..value_end].trim());
        }
    }
    None
}

fn first_string_arg(decorator: &DecoratorCapture) -> String {
    decorator
        .arguments
        .first()
        .map(|value| sanitize_topic_name(value))
        .unwrap_or_default()
}

fn join_route_path(base: &str, method_path: &str) -> String {
    let mut pieces = Vec::new();
    for piece in [base, method_path] {
        let trimmed = piece
            .trim()
            .trim_matches('"')
            .trim_matches('\'')
            .trim_matches('/');
        if !trimmed.is_empty() {
            pieces.push(trimmed);
        }
    }
    if pieces.is_empty() {
        "/".to_owned()
    } else {
        format!("/{}", pieces.join("/"))
    }
}

fn topic_names_from_decorator(
    parsed: &ParsedFile,
    decorator: &DecoratorCapture,
    handler_name: &str,
) -> Vec<String> {
    let names: Vec<String> = first_topic_metadata_argument(decorator)
        .into_iter()
        .flat_map(|value| split_topic_metadata_values(&value))
        .filter_map(|value| resolve_topic_decorator_argument(parsed, &value))
        .collect();
    if names.is_empty() {
        // Demoted from `warn` to `debug` because real NestJS codebases use
        // `@MessagePattern(SOME_CONSTANT)` everywhere, which can never be
        // resolved to a literal at parse time. Logging at warn produced
        // dozens of identical lines per indexing run that drowned out
        // actionable warnings. Operators who want this signal can opt
        // back in via `RUST_LOG=gather_step_parser=debug`.
        tracing::debug!(
            decorator = %decorator.name,
            handler = %handler_name,
            "Skipping a NestJS topic decorator that has no extractable literal topic.",
        );
    }
    names
}

/// Extract the first comma-separated argument from the decorator.
///
/// `decorator.raw` is the argument expression only (content inside the
/// outermost parens) with quotes and bracket syntax preserved — which is
/// exactly what downstream resolvers (`resolve_string_expression`,
/// `split_topic_metadata_values`) expect.  `decorator.arguments` strips
/// quotes via `split_arguments`, so it cannot be used here without losing
/// the quoted-literal signal that `resolve_string_expression` keys off.
///
/// Split the raw argument expression on top-level commas and return the
/// first entry (mirrors the pre-M2 behaviour).
fn first_topic_metadata_argument(decorator: &DecoratorCapture) -> Option<String> {
    let raw = decorator.raw.trim();
    if !raw.is_empty()
        && let Some(first) = split_top_level(raw, ',').into_iter().next()
    {
        let trimmed = first.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_owned());
        }
    }
    // Fallback to `arguments` only if `raw` is unusable.  Note that quotes
    // are stripped here — downstream resolvers that rely on quoted-literal
    // classification will miss, but returning something is preferable to
    // None for decorators with empty `raw`.
    decorator
        .arguments
        .first()
        .map(|first| first.trim().to_owned())
        .filter(|s| !s.is_empty())
}

fn split_topic_metadata_values(raw: &str) -> Vec<String> {
    let trimmed = raw.trim();
    if trimmed.starts_with('[') && trimmed.ends_with(']') {
        split_top_level(trimmed.trim_matches(['[', ']']), ',')
            .into_iter()
            .map(ToOwned::to_owned)
            .collect()
    } else if trimmed.is_empty() {
        Vec::new()
    } else {
        vec![trimmed.to_owned()]
    }
}

fn split_top_level(raw: &str, delimiter: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut bracket_depth = 0_u32;
    let mut brace_depth = 0_u32;
    let mut paren_depth = 0_u32;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    let mut escape = false;

    for (index, ch) in raw.char_indices() {
        if escape {
            escape = false;
            continue;
        }

        match ch {
            '\\' if in_single || in_double || in_backtick => {
                escape = true;
            }
            '\'' if !in_double && !in_backtick => in_single = !in_single,
            '"' if !in_single && !in_backtick => in_double = !in_double,
            '`' if !in_single && !in_double => in_backtick = !in_backtick,
            _ if in_single || in_double || in_backtick => {}
            '[' => bracket_depth = bracket_depth.saturating_add(1),
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            '{' => brace_depth = brace_depth.saturating_add(1),
            '}' => brace_depth = brace_depth.saturating_sub(1),
            '(' => paren_depth = paren_depth.saturating_add(1),
            ')' => paren_depth = paren_depth.saturating_sub(1),
            _ if ch == delimiter && bracket_depth == 0 && brace_depth == 0 && paren_depth == 0 => {
                parts.push(raw[start..index].trim());
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }

    let tail = raw[start..].trim();
    if !tail.is_empty() {
        parts.push(tail);
    }
    parts
}

pub(crate) fn extract_call_argument(raw_arguments: &str, index: usize) -> Option<&str> {
    let args = split_top_level(
        raw_arguments
            .trim()
            .trim_start_matches('(')
            .trim_end_matches(')'),
        ',',
    );
    args.get(index).copied().map(str::trim)
}

fn sanitize_topic_name(value: &str) -> String {
    value
        .trim()
        .trim_matches('[')
        .trim_matches(']')
        .trim_matches('"')
        .trim_matches('\'')
        .trim_matches('`')
        .trim()
        .to_owned()
}

pub(crate) fn resolve_topic_decorator_argument(parsed: &ParsedFile, raw: &str) -> Option<String> {
    resolve_string_expression(parsed, raw, 0)
}

pub(crate) fn resolve_producer_topic_name(
    parsed: &ParsedFile,
    call_site: &crate::tree_sitter::EnrichedCallSite,
) -> Option<String> {
    let raw = call_site
        .raw_arguments
        .as_deref()
        .and_then(|raw_arguments| {
            let first = extract_call_argument(raw_arguments, 0)?;
            extract_object_key_value(first, "topic").or(Some(first))
        })
        .or(call_site.literal_argument.as_deref())?;
    let resolved = resolve_topic_decorator_argument(parsed, raw)?;
    if should_skip_dynamic_topic(&resolved) || resolved.is_empty() {
        return None;
    }
    Some(resolved)
}

pub(crate) fn producer_messaging_operation(
    call_site: &crate::tree_sitter::EnrichedCallSite,
) -> Option<(&str, NodeKind)> {
    // Guard 1: bare `send(...)` / `emit(...)` with no receiver is never a
    // NestJS messaging client call — skip it early.
    let hint = call_site.callee_qualified_hint.as_deref()?;

    // Guard 2: the receiver (everything before the last `.`) must start with
    // `this` to restrict matches to injected class-level messaging clients
    // (`this.bus`, `this.kafkaClient`, `this.serviceBusClient`, etc.).
    // This excludes HTTP response objects (`res.send`), WebSocket sockets
    // (`socket.emit`), and other local-variable method calls whose receiver is
    // a bare identifier without a `this.` prefix.
    let receiver = hint.rsplit_once('.').map_or(hint, |(recv, _)| recv);
    let receiver_starts_with_this = receiver.eq_ignore_ascii_case("this")
        || receiver
            .get(..5)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("this."));
    if !receiver_starts_with_this {
        return None;
    }

    let operation = hint.rsplit('.').next().unwrap_or(hint);
    if operation.eq_ignore_ascii_case("emit")
        || operation.eq_ignore_ascii_case("send")
        || operation.eq_ignore_ascii_case("sendmessage")
    {
        // All messaging producers converge on `NodeKind::Event` so the
        // producer's virtual target shares a canonical id with the
        // consumer's (see `add_topic_consumer_edges`).
        Some((hint, NodeKind::Event))
    } else {
        None
    }
}

/// Resolve a backtick template literal where every `${…}` interpolation is a
/// statically resolvable expression.
///
/// The function walks the template content between the outer backticks,
/// collecting literal text spans and resolving each `${…}` fragment via
/// `resolve_string_expression`.  The resolved fragments are concatenated to
/// form the final string.
///
/// Returns `None` if any fragment cannot be resolved (non-static) so that
/// fabricated partial values are never returned.
fn resolve_static_template_literal(parsed: &ParsedFile, raw: &str, depth: usize) -> Option<String> {
    // Strip the surrounding backticks.
    let inner = raw.strip_prefix('`')?.strip_suffix('`')?;
    let mut result = String::new();
    let mut remaining = inner;
    loop {
        match remaining.find("${") {
            None => {
                // No more interpolations — append the remaining literal text.
                result.push_str(remaining);
                break;
            }
            Some(start) => {
                // Append the literal text before the interpolation.
                result.push_str(&remaining[..start]);
                let after_dollar = &remaining[start + 2..]; // skip `${`
                // Find the matching `}` — handle nested braces.
                let mut depth_count = 1_u32;
                let mut end = None;
                for (i, ch) in after_dollar.char_indices() {
                    match ch {
                        '{' => depth_count += 1,
                        '}' => {
                            depth_count -= 1;
                            if depth_count == 0 {
                                end = Some(i);
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                let close = end?;
                let fragment = after_dollar[..close].trim();
                let resolved = resolve_string_expression(parsed, fragment, depth + 1)?;
                result.push_str(&resolved);
                remaining = &after_dollar[close + 1..];
            }
        }
    }
    Some(result)
}

fn resolve_string_expression(parsed: &ParsedFile, raw: &str, depth: usize) -> Option<String> {
    if depth > 4 {
        return None;
    }
    let trimmed = raw.trim();
    let sanitized = sanitize_topic_name(trimmed);
    if sanitized.is_empty() {
        return None;
    }

    // Same-file constant_strings lookup (plain consts defined in this file)
    if let Some(resolved) = parsed.constant_strings.get(&sanitized) {
        return (!resolved.is_empty()).then_some(resolved.clone());
    }

    // Quoted string literal
    let is_quoted_literal = (trimmed.starts_with('\'') && trimmed.ends_with('\''))
        || (trimmed.starts_with('"') && trimmed.ends_with('"'))
        || (trimmed.starts_with('`') && trimmed.ends_with('`') && !trimmed.contains("${"));
    if is_quoted_literal {
        return Some(sanitized);
    }

    // Static template literal: `` `${CONST_A}.${CONST_B}` `` where every
    // interpolation is a resolvable constant expression.  Walk the template
    // text, resolve each `${…}` fragment, concatenate with the literal parts.
    // If any fragment cannot be resolved, return `None` rather than fabricating
    // a partial value.
    if trimmed.starts_with('`') && trimmed.ends_with('`') && trimmed.contains("${") {
        if let Some(resolved) = resolve_static_template_literal(parsed, trimmed, depth) {
            return (!resolved.is_empty()).then_some(resolved);
        }
        return None;
    }

    let fallback_parts = split_logical_or_values(trimmed);
    if fallback_parts.len() > 1 {
        for part in fallback_parts.iter().rev() {
            if let Some(resolved) = resolve_string_expression(parsed, part, depth + 1) {
                return Some(resolved);
            }
        }
        return None;
    }

    if let Some(resolved) = resolve_member_chain(parsed, &sanitized, depth) {
        return (!resolved.is_empty()).then_some(resolved);
    }

    // Imported bare const: `SOME_CONST` — find in binding source file
    if is_identifier_like(&sanitized)
        && let Some(resolved) = resolve_imported_const(parsed, &sanitized, depth)
    {
        return (!resolved.is_empty()).then_some(resolved);
    }

    None
}

fn split_logical_or_values(raw: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut bracket_depth = 0_u32;
    let mut brace_depth = 0_u32;
    let mut paren_depth = 0_u32;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    let mut escape = false;
    let chars = raw.char_indices().collect::<Vec<_>>();
    let mut index = 0usize;

    while index < chars.len() {
        let (offset, ch) = chars[index];
        if escape {
            escape = false;
            index += 1;
            continue;
        }

        match ch {
            '\\' if in_single || in_double || in_backtick => {
                escape = true;
            }
            '\'' if !in_double && !in_backtick => in_single = !in_single,
            '"' if !in_single && !in_backtick => in_double = !in_double,
            '`' if !in_single && !in_double => in_backtick = !in_backtick,
            _ if in_single || in_double || in_backtick => {}
            '[' => bracket_depth = bracket_depth.saturating_add(1),
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            '{' => brace_depth = brace_depth.saturating_add(1),
            '}' => brace_depth = brace_depth.saturating_sub(1),
            '(' => paren_depth = paren_depth.saturating_add(1),
            ')' => paren_depth = paren_depth.saturating_sub(1),
            '|' if bracket_depth == 0 && brace_depth == 0 && paren_depth == 0 => {
                if chars.get(index + 1).is_some_and(|(_, next)| *next == '|') {
                    let part = raw[start..offset].trim();
                    if !part.is_empty() {
                        parts.push(part.to_owned());
                    }
                    let next_offset = chars
                        .get(index + 1)
                        .map_or(offset + 1, |(next_offset, _)| *next_offset + 1);
                    start = next_offset;
                    index += 2;
                    continue;
                }
            }
            _ => {}
        }
        index += 1;
    }

    let tail = raw[start..].trim();
    if !tail.is_empty() {
        parts.push(tail.to_owned());
    }
    parts
}

fn resolve_member_chain(parsed: &ParsedFile, expression: &str, depth: usize) -> Option<String> {
    if let Some(resolved) = parsed.constant_strings.get(expression) {
        return (!resolved.is_empty()).then_some(resolved.clone());
    }

    let (identifier, remainder) = expression.split_once('.')?;
    let identifier = identifier.trim();
    let remainder = remainder.trim();
    if identifier.is_empty() || remainder.is_empty() {
        return None;
    }

    resolve_imported_chain(parsed, identifier, remainder, depth)
}

/// Resolve `Identifier.Member` across the import boundary.
///
/// Finds `Identifier` in `parsed.import_bindings`, reads its resolved source
/// file, and returns the string value assigned to `Member` in that file.
/// Limited to static string-valued enum members and const object properties.
fn resolve_imported_chain(
    parsed: &ParsedFile,
    identifier: &str,
    member_chain: &str,
    depth: usize,
) -> Option<String> {
    let binding = parsed
        .import_bindings
        .iter()
        .find(|b| b.local_name == identifier && !b.is_type_only)?;
    let path = binding.resolved_path.as_ref()?;

    // For namespace imports (`import * as Ns from '...'`), `identifier` is the
    // local alias for the entire module namespace — there is no `imported_name`.
    // The `member_chain` already encodes the full dotted path within the module
    // (e.g. `EventType.Foo` from `Ns.EventType.Foo`), so we search the imported
    // file with `member_chain` directly rather than prepending the alias name.
    let is_namespace = binding.is_namespace;
    let imported_root = if is_namespace {
        String::new()
    } else {
        binding
            .imported_name
            .as_deref()
            .unwrap_or(identifier)
            .to_owned()
    };
    if let Some(imported) = parse_imported_file(parsed, path) {
        let imported_expression = match (is_namespace, member_chain.is_empty()) {
            // Namespace import, member chain present: search the chain directly.
            (true, false) => member_chain.to_owned(),
            // Namespace import, no member chain: nothing useful to resolve.
            (true, true) => return None,
            // Named import, member chain present.
            (false, false) => format!("{imported_root}.{member_chain}"),
            // Named import, no member chain.
            (false, true) => imported_root.clone(),
        };
        if let Some(resolved) =
            resolve_string_expression(&imported, &imported_expression, depth + 1)
        {
            return Some(resolved);
        }
    }

    let content = std::fs::read_to_string(path).ok()?;
    let member = member_chain
        .rsplit('.')
        .next()
        .unwrap_or(member_chain)
        .trim();
    if member.is_empty() {
        return None;
    }
    if let Some(value) = extract_string_member_value(&content, member) {
        return Some(value);
    }
    // Barrel re-export hop: when the resolved file re-exports the identifier
    // via `export { X } from '…'` or `export * from '…'`, follow that
    // one additional hop and retry extraction.  Depth is intentionally
    // limited to a single extra hop to avoid unbounded traversal.
    //
    // Workspace-package specifiers (e.g. `export { Foo } from '@pkg/lib'`)
    // are resolved via `path_aliases` derived from the repo root, so cross-
    // package barrel hops are now supported.
    let repo_root = repo_root_for(parsed);
    let path_aliases = PathAliases::from_repo_root(&repo_root);
    barrel_hop_lookup(&content, member, path, &repo_root, &path_aliases)
}

/// Maximum number of `export { X } from '…'` / `export * from '…'`
/// barrel hops to follow when resolving a string-valued constant. A package
/// `index.ts` typically re-exports through one or two intermediate barrel
/// files (e.g. `index.ts → kafka/index.ts → kafka/topics.ts`); 3 hops
/// covers the common monorepo shapes without risking pathological loops.
const MAX_BARREL_HOPS: usize = 3;

/// Follow `export { X } from '…'` / `export * from '…'` re-export chains and
/// attempt `extract_string_member_value` on each resolved file.
///
/// Recognises two patterns:
/// - `export { X } from './path'`         (relative or workspace specifier)
/// - `export * from './path'`             (relative or workspace specifier)
///
/// Up to [`MAX_BARREL_HOPS`] hops are followed: when a hop lands on a file
/// that is itself a barrel and does not directly expose `member`, the hop
/// recurses on the resolved file's content. Depth is bounded to avoid
/// pathological cycles in malformed sources.
///
/// The name comparison for named re-exports is case-sensitive
/// (TypeScript/JavaScript identifiers are). Both relative specifiers and
/// workspace-package specifiers (e.g. `export { Foo } from '@pkg/lib/events'`)
/// are supported. Workspace specifiers are resolved via `path_aliases` derived
/// from the `repo_root`.
fn barrel_hop_lookup(
    barrel_content: &str,
    member: &str,
    barrel_path: &std::path::Path,
    repo_root: &std::path::Path,
    path_aliases: &PathAliases,
) -> Option<String> {
    barrel_hop_lookup_inner(
        barrel_content,
        member,
        barrel_path,
        repo_root,
        path_aliases,
        0,
    )
}

fn barrel_hop_lookup_inner(
    barrel_content: &str,
    member: &str,
    barrel_path: &std::path::Path,
    repo_root: &std::path::Path,
    path_aliases: &PathAliases,
    depth: usize,
) -> Option<String> {
    if depth >= MAX_BARREL_HOPS {
        return None;
    }
    let barrel_dir = barrel_path.parent()?;
    for line in barrel_content.lines() {
        let trimmed = line.trim();

        // `export { X } from '…'` or `export { X as Y } from '…'`
        if let Some(rest) = trimmed.strip_prefix("export") {
            let rest = rest.trim();
            if let Some(brace_content) = rest.strip_prefix('{') {
                // Named re-export: check that `member` appears in the braces.
                let Some(close) = brace_content.find('}') else {
                    continue;
                };
                let names_str = &brace_content[..close];
                let exports_member = names_str.split(',').any(|entry| {
                    let entry = entry.trim();
                    // Handle `X as Y` — match on the original name (before `as`).
                    let original = entry.split_whitespace().next().unwrap_or(entry);
                    original == member
                });
                if !exports_member {
                    continue;
                }
                let after_brace = &brace_content[close + 1..];
                if let Some(specifier) = extract_from_specifier(after_brace)
                    && let Some(resolved) = resolve_barrel_specifier(
                        barrel_dir,
                        specifier,
                        repo_root,
                        path_aliases,
                        barrel_path,
                    )
                    && let Ok(content) = std::fs::read_to_string(&resolved)
                {
                    if let Some(value) = extract_string_member_value(&content, member) {
                        return Some(value);
                    }
                    if let Some(value) = barrel_hop_lookup_inner(
                        &content,
                        member,
                        &resolved,
                        repo_root,
                        path_aliases,
                        depth + 1,
                    ) {
                        return Some(value);
                    }
                }
            } else if let Some(after_star) = rest.strip_prefix('*') {
                // Star re-export: `export * from '…'`
                let after_star = after_star.trim();
                if let Some(specifier) = extract_from_specifier(after_star)
                    && let Some(resolved) = resolve_barrel_specifier(
                        barrel_dir,
                        specifier,
                        repo_root,
                        path_aliases,
                        barrel_path,
                    )
                    && let Ok(content) = std::fs::read_to_string(&resolved)
                {
                    if let Some(value) = extract_string_member_value(&content, member) {
                        return Some(value);
                    }
                    if let Some(value) = barrel_hop_lookup_inner(
                        &content,
                        member,
                        &resolved,
                        repo_root,
                        path_aliases,
                        depth + 1,
                    ) {
                        return Some(value);
                    }
                }
            }
        }
    }
    None
}

/// Resolve a barrel re-export specifier to a filesystem path.
///
/// Tries relative resolution first (fast path); falls back to
/// `resolve_import_path_pub` for workspace-package and `@`-prefixed specifiers.
/// Returns `None` when neither strategy produces an existing file.
fn resolve_barrel_specifier(
    barrel_dir: &std::path::Path,
    specifier: &str,
    repo_root: &std::path::Path,
    path_aliases: &PathAliases,
    barrel_path: &std::path::Path,
) -> Option<std::path::PathBuf> {
    // Fast path: relative specifier — no alias look-up needed.
    if specifier.starts_with('.') {
        return resolve_specifier_path(barrel_dir, specifier);
    }
    // Workspace / node_modules specifier: use the full import resolver.
    // `barrel_path` is the absolute path of the barrel file; strip the
    // repo_root prefix to get a repo-relative path for the resolver.
    let relative_barrel = barrel_path.strip_prefix(repo_root).ok()?;
    // Use TypeScript since barrel files are always .ts / .d.ts in NestJS repos.
    resolve_import_path_pub(
        repo_root,
        relative_barrel,
        specifier,
        Language::TypeScript,
        path_aliases,
    )
}

/// Parse the module specifier from `from '…'` or `from "…"` text.
fn extract_from_specifier(text: &str) -> Option<&str> {
    let rest = text.trim();
    let rest = rest.strip_prefix("from")?.trim();
    let (open, close) = if rest.starts_with('\'') {
        ('\'', '\'')
    } else if rest.starts_with('"') {
        ('"', '"')
    } else {
        return None;
    };
    let inner = rest.strip_prefix(open)?;
    let end = inner.find(close)?;
    Some(&inner[..end])
}

/// Resolve a relative module specifier from `dir` to a filesystem path.
///
/// Tries `.ts`, `.d.ts`, `.js`, and `index.ts` / `index.d.ts` inside the
/// named directory, in that order — matching the import resolution order used
/// elsewhere in the `NestJS` parser.
fn resolve_specifier_path(dir: &std::path::Path, specifier: &str) -> Option<std::path::PathBuf> {
    if !specifier.starts_with('.') {
        return None; // Only relative imports are followed.
    }
    let base = dir.join(specifier);
    for suffix in &[".ts", ".d.ts", ".js", "/index.ts", "/index.d.ts"] {
        let candidate = std::path::PathBuf::from(format!("{}{suffix}", base.display()));
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    if base.is_file() {
        return Some(base);
    }
    None
}

/// Resolve a bare imported identifier to its string value in the source file.
///
/// Handles `export const NAME = 'value'` patterns in the resolved source.
fn resolve_imported_const(parsed: &ParsedFile, name: &str, depth: usize) -> Option<String> {
    let binding = parsed
        .import_bindings
        .iter()
        .find(|b| b.local_name == name && !b.is_type_only)?;
    let imported = binding.imported_name.as_deref().unwrap_or(name);
    let path = binding.resolved_path.as_ref()?;
    let imported_parsed = parse_imported_file(parsed, path)?;
    resolve_string_expression(&imported_parsed, imported, depth + 1).or_else(|| {
        let content = std::fs::read_to_string(path).ok()?;
        extract_string_member_value(&content, imported)
    })
}

/// Bounded capacity for the imported-file parse cache.
///
/// Real workspaces re-import a small set of helper modules (kafka client
/// builders, event-type registries, shared topic constants) from many call
/// sites across many consumer files. Without a cache, each call site parses
/// the same helper file from scratch — once per consumer — which dominates
/// indexing wall time on large monorepos. 4 096 entries comfortably covers
/// any realistic workspace; `quick_cache::sync::Cache` is sharded so the
/// per-entry lock is held for microseconds.
const PARSE_IMPORTED_CACHE_CAPACITY: usize = 4_096;

/// Cache key for [`parse_imported_file`]. Keyed by the absolute target path,
/// the effective package root used for path-alias resolution, the labelling
/// repo string, and a file-state token (`mtime_ns + len`) so that watch /
/// serve / repeated in-process indexing pick up edits to the imported file
/// without an explicit cache flush. A miss on the metadata read short-
/// circuits to a fresh parse.
type ParseImportedCacheKey = (PathBuf, PathBuf, String, u128, u64);

const PARSE_IMPORTED_CACHE_WEIGHT_CAPACITY_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone)]
struct ParsedFileWeighter;

impl Weighter<ParseImportedCacheKey, Option<ParsedFile>> for ParsedFileWeighter {
    fn weight(&self, _key: &ParseImportedCacheKey, value: &Option<ParsedFile>) -> u64 {
        let Some(parsed) = value else {
            return 1;
        };
        u64::try_from(parsed_file_weight(parsed)).unwrap_or(u64::MAX)
    }
}

fn parsed_file_weight(parsed: &ParsedFile) -> usize {
    std::mem::size_of::<ParsedFile>()
        .saturating_add(file_entry_weight(&parsed.file))
        .saturating_add(path_weight(&parsed.source_path))
        .saturating_add(parsed.source.len())
        .saturating_add(node_weight(&parsed.file_node))
        .saturating_add(vec_weight(
            &parsed.nodes,
            parsed.nodes.capacity(),
            node_weight,
        ))
        .saturating_add(vec_weight(
            &parsed.edges,
            parsed.edges.capacity(),
            edge_weight,
        ))
        .saturating_add(vec_weight(
            &parsed.symbols,
            parsed.symbols.capacity(),
            symbol_weight,
        ))
        .saturating_add(vec_weight(
            &parsed.call_sites,
            parsed.call_sites.capacity(),
            call_site_weight,
        ))
        .saturating_add(vec_weight(
            &parsed.import_bindings,
            parsed.import_bindings.capacity(),
            import_binding_weight,
        ))
        .saturating_add(
            parsed
                .constant_strings
                .capacity()
                .saturating_mul(std::mem::size_of::<(String, String)>())
                .saturating_add(
                    parsed
                        .constant_strings
                        .iter()
                        .map(|(key, value)| key.len().saturating_add(value.len()))
                        .sum::<usize>(),
                ),
        )
}

fn vec_weight<T>(items: &[T], capacity: usize, item_weight: fn(&T) -> usize) -> usize {
    capacity
        .saturating_mul(std::mem::size_of::<T>())
        .saturating_add(items.iter().map(item_weight).sum::<usize>())
}

fn file_entry_weight(file: &FileEntry) -> usize {
    path_weight(&file.path)
        .saturating_add(file.source_bytes.as_ref().map_or(0, |source| source.len()))
}

fn path_weight(path: &Path) -> usize {
    path.as_os_str().to_string_lossy().len()
}

fn option_string_weight(value: Option<&String>) -> usize {
    value.map_or(0, String::len)
}

fn node_weight(node: &NodeData) -> usize {
    node.repo
        .len()
        .saturating_add(node.file_path.len())
        .saturating_add(node.name.len())
        .saturating_add(option_string_weight(node.qualified_name.as_ref()))
        .saturating_add(option_string_weight(node.external_id.as_ref()))
        .saturating_add(option_string_weight(node.signature.as_ref()))
}

fn edge_weight(edge: &EdgeData) -> usize {
    option_string_weight(edge.metadata.drift_kind.as_ref())
        .saturating_add(option_string_weight(edge.metadata.resolver.as_ref()))
}

fn symbol_weight(symbol: &SymbolCapture) -> usize {
    node_weight(&symbol.node)
        .saturating_add(option_string_weight(symbol.parent_class.as_ref()))
        .saturating_add(vec_weight(
            &symbol.decorators,
            symbol.decorators.capacity(),
            decorator_weight,
        ))
        .saturating_add(vec_weight(
            &symbol.class_decorators,
            symbol.class_decorators.capacity(),
            decorator_weight,
        ))
        .saturating_add(
            symbol
                .constructor_dependencies
                .capacity()
                .saturating_mul(std::mem::size_of::<String>())
                .saturating_add(
                    symbol
                        .constructor_dependencies
                        .iter()
                        .map(String::len)
                        .sum::<usize>(),
                ),
        )
        .saturating_add(
            symbol
                .implemented_interfaces
                .capacity()
                .saturating_mul(std::mem::size_of::<String>())
                .saturating_add(
                    symbol
                        .implemented_interfaces
                        .iter()
                        .map(String::len)
                        .sum::<usize>(),
                ),
        )
}

fn decorator_weight(decorator: &DecoratorCapture) -> usize {
    decorator
        .name
        .len()
        .saturating_add(decorator.raw.len())
        .saturating_add(
            decorator
                .arguments
                .iter()
                .map(|argument| argument.len())
                .sum::<usize>(),
        )
}

fn call_site_weight(call_site: &EnrichedCallSite) -> usize {
    path_weight(&call_site.source_path)
        .saturating_add(call_site.callee_name.len())
        .saturating_add(option_string_weight(
            call_site.callee_qualified_hint.as_ref(),
        ))
        .saturating_add(option_string_weight(call_site.literal_argument.as_ref()))
        .saturating_add(option_string_weight(call_site.raw_arguments.as_ref()))
}

fn import_binding_weight(binding: &ImportBinding) -> usize {
    binding
        .local_name
        .len()
        .saturating_add(option_string_weight(binding.imported_name.as_ref()))
        .saturating_add(binding.source.len())
        .saturating_add(
            binding
                .resolved_path
                .as_ref()
                .map_or(0, |path| path_weight(path)),
        )
}

fn parse_imported_cache()
-> &'static Cache<ParseImportedCacheKey, Option<ParsedFile>, ParsedFileWeighter> {
    static CACHE: OnceLock<Cache<ParseImportedCacheKey, Option<ParsedFile>, ParsedFileWeighter>> =
        OnceLock::new();
    CACHE.get_or_init(|| {
        Cache::with_weighter(
            PARSE_IMPORTED_CACHE_CAPACITY,
            PARSE_IMPORTED_CACHE_WEIGHT_CAPACITY_BYTES,
            ParsedFileWeighter,
        )
    })
}

fn file_state_token(metadata: &std::fs::Metadata) -> (u128, u64) {
    let mtime_ns = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or(0, |dur| dur.as_nanos());
    (mtime_ns, metadata.len())
}

fn parse_imported_file(parsed: &ParsedFile, path: &std::path::Path) -> Option<ParsedFile> {
    let repo_root = repo_root_for(parsed);

    // Try to strip the repo root to get a repo-relative path.  When the
    // target is inside the same package this always succeeds.  When the
    // target is in a different workspace package (a cross-package import),
    // `strip_prefix` fails and we fall back to walking up from `path` to
    // find a package boundary (a directory containing `package.json`).
    let (effective_root, relative) = if let Ok(rel) = path.strip_prefix(&repo_root) {
        (repo_root.clone(), rel.to_path_buf())
    } else {
        // Cross-package import: find the nearest ancestor of `path` that
        // contains a `package.json` and use it as the effective repo root.
        let alt_root = path
            .ancestors()
            .skip(1) // skip the file itself
            .find(|ancestor| ancestor.join("package.json").is_file())?;
        let rel = path.strip_prefix(alt_root).ok()?;
        (alt_root.to_path_buf(), rel.to_path_buf())
    };

    // Read metadata once: used for both cache-key freshness and the parse
    // path's size/mtime threading. A failure here means the file is gone or
    // unreadable — return None without touching the cache.
    let metadata = std::fs::metadata(path).ok()?;
    let (mtime_ns, len) = file_state_token(&metadata);
    let cache_key: ParseImportedCacheKey = (
        path.to_path_buf(),
        effective_root.clone(),
        parsed.file_node.repo.clone(),
        mtime_ns,
        len,
    );
    if let Some(cached) = parse_imported_cache().get(&cache_key) {
        return cached;
    }

    let parsed_result = (|| -> Option<ParsedFile> {
        let language = classify_language(&relative)?;
        // Load path aliases from the effective root so that barrel hops through
        // workspace-package specifiers (e.g. `@workspace/shared-types/...`) are
        // resolved correctly inside the imported file.
        let path_aliases = PathAliases::from_repo_root(&effective_root);
        parse_file_with_context(
            parsed.file_node.repo.as_str(),
            &effective_root,
            &FileEntry {
                path: relative,
                language,
                size_bytes: len,
                content_hash: [0; 32],
                source_bytes: None,
            },
            // Preserve NestJS framework so augmentors and the import resolver
            // use the full NestJS extraction path in the imported file.
            &[Framework::NestJs],
            &path_aliases,
        )
        .ok()
    })();

    parse_imported_cache().insert(cache_key, parsed_result.clone());
    parsed_result
}

fn repo_root_for(parsed: &ParsedFile) -> std::path::PathBuf {
    let mut root = parsed.source_path.clone();
    for _ in parsed.file.path.components() {
        root.pop();
    }
    root
}

/// Scan `content` for `Member = 'value'` or `Member: 'value'` patterns.
///
/// Handles enum members (`Member = 'value'`), object properties (`member: 'value'`),
/// and exported constants (`export const Member = 'value'`).
fn extract_string_member_value(content: &str, member: &str) -> Option<String> {
    for line in content.lines() {
        let trimmed = line.trim();
        // Strip `export` and `const` prefixes so `export const MEMBER = ...`
        // is treated the same as a bare `MEMBER = ...` enum member.
        let stripped = trimmed
            .strip_prefix("export")
            .map_or(trimmed, str::trim_start);
        let stripped = stripped
            .strip_prefix("const")
            .map_or(stripped, str::trim_start);
        if !stripped.starts_with(member) {
            continue;
        }
        let rest = stripped[member.len()..].trim_start();
        let value_str = if let Some(s) = rest.strip_prefix('=') {
            s.trim()
        } else if let Some(s) = rest.strip_prefix(':') {
            s.trim()
        } else {
            continue;
        };
        let value_str = value_str.trim_end_matches(',').trim_end_matches(';').trim();
        if let Some(s) = value_str.strip_prefix('\'') {
            return s.split_once('\'').map(|(v, _)| v.to_owned());
        }
        if let Some(s) = value_str.strip_prefix('"') {
            return s.split_once('"').map(|(v, _)| v.to_owned());
        }
        if let Some(s) = value_str.strip_prefix('`') {
            return (!s.contains("${"))
                .then(|| s.split_once('`').map(|(v, _)| v.to_owned()))
                .flatten();
        }
    }
    None
}

/// Returns `true` when `s` looks like a TypeScript identifier (no dots, spaces,
/// or expression characters).  Used as a guard before cross-file const lookup
/// to avoid triggering on dynamic or compound expressions.
fn is_identifier_like(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '$')
}

fn sanitize_queue_name(value: &str) -> Option<String> {
    let sanitized = sanitize_topic_name(value);
    if sanitized.starts_with('{') && sanitized.ends_with('}') {
        let inner = sanitized.trim_matches('{').trim_matches('}').trim();
        if let Some(name_value) = inner.strip_prefix("name:") {
            let queue_name = sanitize_topic_name(name_value.trim());
            return (!queue_name.is_empty()).then_some(queue_name);
        }
        return None;
    }
    (!sanitized.is_empty()).then_some(sanitized)
}

/// Returns `true` when `value` looks like a *dynamic* topic expression that
/// should be skipped rather than indexed as a concrete topic literal.
///
/// Concrete topic names — including dot-separated (`order.placed`),
/// underscore-separated (`user_created`), and kebab-case (`order-placed`)
/// forms — are real literals and must be kept.  Values that contain
/// template/interpolation characters (`$`, `{`, `}`) or whitespace are
/// treated as dynamic expressions and skipped.  Empty values are also
/// skipped because they are not indexable.
fn should_skip_dynamic_topic(value: &str) -> bool {
    if value.is_empty() {
        return true;
    }
    // Skip anything that looks like a template or interpolation expression.
    value
        .chars()
        .any(|c| matches!(c, '$' | '{' | '}' | ' ' | '\t' | '\n'))
}

fn resolve_argument(parsed: &ParsedFile, value: &str) -> String {
    parsed
        .constant_strings
        .get(value)
        .cloned()
        .unwrap_or_else(|| sanitize_topic_name(value))
}

fn virtual_node(
    kind: NodeKind,
    qualified_name: &str,
    name: &str,
    symbol: &SymbolCapture,
) -> NodeData {
    NodeData {
        id: ref_node_id(kind, qualified_name),
        kind,
        repo: symbol.node.repo.clone(),
        file_path: symbol.node.file_path.clone(),
        name: name.to_owned(),
        qualified_name: Some(qualified_name.to_owned()),
        external_id: Some(qualified_name.to_owned()),
        signature: None,
        visibility: None,
        span: symbol.node.span.clone(),
        is_virtual: true,
    }
}

/// Emit virtual `Class` nodes and `DependsOn` edges for parameter-level
/// injection decorators: `@Inject('TOKEN')` and `@InjectModel(Model.name)`.
///
/// These decorators are captured as call expressions rather than as class
/// decorators because tree-sitter sees them at the constructor parameter site.
/// They appear in `parsed.call_sites` with `callee_name` of `"Inject"` or
/// `"InjectModel"` and their token/model reference in `literal_argument`.
///
/// QN conventions:
/// - `@Inject('TOKEN')` → `__di__TOKEN`
/// - `@InjectModel(Product.name)` → `__di__model__Product`  (`.name` suffix is stripped)
fn add_inject_edges(parsed: &ParsedFile, augmentation: &mut NestjsAugmentation) {
    // Only match `Inject` / `InjectModel` when they are imported from a NestJS
    // package. A user-defined function named `Inject` called outside of a
    // decorator context must not fabricate a `__di__` virtual node.
    let inject_from_nestjs = parsed
        .import_bindings
        .iter()
        .any(|b| b.source.starts_with("@nestjs/") && b.local_name == "Inject");
    let inject_model_from_nestjs = parsed.import_bindings.iter().any(|b| {
        (b.source.starts_with("@nestjs/") || b.source == "nestjs-typeorm-paginate")
            && b.local_name == "InjectModel"
    });

    for call_site in &parsed.call_sites {
        let token_arg = match call_site.callee_name.as_str() {
            "Inject" => {
                if !inject_from_nestjs {
                    continue;
                }
                let Some(raw) = call_site.literal_argument.as_ref() else {
                    continue;
                };
                let token = sanitize_topic_name(raw);
                if token.is_empty() {
                    continue;
                }
                format!("__di__{token}")
            }
            "InjectModel" => {
                if !inject_model_from_nestjs {
                    continue;
                }
                let Some(raw) = call_site.literal_argument.as_ref() else {
                    continue;
                };
                // `Product.name` → strip the `.name` property access suffix so
                // we get the bare class name `Product`.
                let base = sanitize_topic_name(raw);
                let model = base
                    .strip_suffix(".name")
                    .unwrap_or(&base)
                    .trim()
                    .to_owned();
                if model.is_empty() {
                    continue;
                }
                format!("__di__model__{model}")
            }
            _ => continue,
        };

        let dep_node = NodeData {
            id: ref_node_id(NodeKind::Class, &token_arg),
            kind: NodeKind::Class,
            repo: parsed.file_node.repo.clone(),
            file_path: parsed.file_node.file_path.clone(),
            name: token_arg.clone(),
            qualified_name: Some(token_arg.clone()),
            external_id: Some(token_arg.clone()),
            signature: None,
            visibility: None,
            span: call_site.span.clone(),
            is_virtual: true,
        };
        augmentation.nodes.push(dep_node.clone());
        augmentation.edges.push(EdgeData {
            source: call_site.owner_id,
            target: dep_node.id,
            kind: EdgeKind::DependsOn,
            metadata: EdgeMetadata::default(),
            owner_file: parsed.file_node.id,
            is_cross_file: false,
        });
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::needless_raw_string_hashes)]

    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::NodeKind;
    use pretty_assertions::assert_eq;

    use crate::{Language, frameworks::Framework, tree_sitter::parse_file_with_frameworks};

    // Tests in this module target the NestJS extractor specifically, so they
    // bypass repo-level framework detection and always pass `Framework::NestJs`.
    // This keeps unit tests focused on extractor behaviour rather than repo
    // detection, which is covered in `frameworks::detect::tests` and in the
    // orchestrator end-to-end tests.
    fn parse_file(
        repo: &str,
        repo_root: &std::path::Path,
        file: &crate::FileEntry,
    ) -> Result<crate::ParsedFile, crate::ParseError> {
        parse_file_with_frameworks(repo, repo_root, file, &[Framework::NestJs])
    }

    static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> Self {
            let counter = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-parser-nestjs-{name}-{}-{counter}",
                process::id()
            ));
            fs::create_dir_all(&path).expect("test directory should be created");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn nestjs_routes_and_topics_are_extracted_from_fixture() {
        let temp_dir = TestDir::new("fixture");
        fs::write(
            temp_dir.path().join("controller.ts"),
            r#"
import { Controller, Get, Put } from '@nestjs/common';

@Controller('resources')
export class ResourceController {
  constructor(private readonly workflow: WorkflowService) {}

  @Get('health')
  checkHealth() {
    return { status: 'ok' };
  }

  @Put(':id')
  update() {
    return { updated: true };
  }
}
"#,
        )
        .expect("controller fixture should write");
        fs::write(
            temp_dir.path().join("events.ts"),
            r#"
import { Controller } from '@nestjs/common';
import { MessagePattern } from '@nestjs/microservices';

@Controller()
export class EventController {
  constructor(private readonly bus: EventBusClient) {}

  @MessagePattern(['sample.created'])
  async handleCreated() {
    return {};
  }

  async publish() {
    this.bus.emit('sample.updated', { ok: true });
  }
}
"#,
        )
        .expect("event fixture should write");

        let controller = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "controller.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("controller fixture should parse");
        let events = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "events.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("event fixture should parse");

        let route_nodes = controller
            .nodes
            .iter()
            .filter(|node| node.kind == gather_step_core::NodeKind::Route)
            .map(|node| node.external_id.clone().unwrap_or_default())
            .collect::<Vec<_>>();
        let event_nodes = events
            .nodes
            .iter()
            .filter(|node| node.kind == gather_step_core::NodeKind::Event)
            .map(|node| node.external_id.clone().unwrap_or_default())
            .collect::<Vec<_>>();
        assert!(
            route_nodes
                .iter()
                .any(|route| route == "__route__GET__/resources/health")
        );
        assert!(
            route_nodes
                .iter()
                .any(|route| route == "__route__PUT__/resources/:id")
        );
        // Every NestJS messaging decorator and every messaging client method
        // emits on `NodeKind::Event` with the `__event__kafka__` prefix, so
        // producer and consumer emissions share a single canonical virtual
        // node per topic name.
        assert!(
            event_nodes
                .iter()
                .any(|event| event == "__event__kafka__sample.created")
        );
        assert!(
            event_nodes
                .iter()
                .any(|event| event == "__event__kafka__sample.updated")
        );
        assert!(
            events
                .edges
                .iter()
                .any(|edge| edge.kind == gather_step_core::EdgeKind::Publishes),
            "expected emit() to produce a Publishes edge, got edges: {:?}",
            events.edges
        );
        assert_eq!(
            events
                .symbols
                .iter()
                .any(|symbol| !symbol.constructor_dependencies.is_empty()),
            true
        );
    }

    #[test]
    fn messaging_decorators_and_client_methods_converge_on_canonical_event_nodes() {
        // Locks canonical messaging identity: every NestJS messaging
        // decorator (`@MessagePattern` / `@EventPattern` /
        // `@CustomEventPattern`) and every messaging client method (`emit`,
        // `send`, `sendMessage`) emits on `NodeKind::Event` with the
        // `__event__kafka__` prefix, so producer and consumer emissions
        // share the same `ref_node_id(NodeKind::Event, qn)` id for a given
        // topic name — no more split Topic/Event identities.
        let temp_dir = TestDir::new("canonical-event-identity");
        fs::write(
            temp_dir.path().join("handler.ts"),
            r#"
import { Controller } from '@nestjs/common';
import { EventPattern, MessagePattern } from '@nestjs/microservices';

@Controller()
export class MixedHandler {
  @EventPattern('order.placed')
  async handleOrderPlaced() {}

  @MessagePattern('order.query')
  async handleOrderQuery() { return {}; }

  async notify() {
    this.bus.emit('order.notification', {});
    this.bus.send('order.request', {});
  }
}
"#,
        )
        .expect("handler fixture should write");

        let parsed = parse_file(
            "order-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "handler.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("handler fixture should parse");

        let event_nodes: Vec<_> = parsed
            .nodes
            .iter()
            .filter(|n| n.kind == gather_step_core::NodeKind::Event)
            .collect();
        let topic_nodes: Vec<_> = parsed
            .nodes
            .iter()
            .filter(|n| n.kind == gather_step_core::NodeKind::Topic)
            .collect();

        for expected in [
            "__event__kafka__order.placed",       // @EventPattern
            "__event__kafka__order.query",        // @MessagePattern (converged from Topic)
            "__event__kafka__order.notification", // client.emit
            "__event__kafka__order.request",      // client.send (converged from Topic)
        ] {
            assert!(
                event_nodes
                    .iter()
                    .any(|n| n.external_id.as_deref() == Some(expected)),
                "missing canonical Event node `{expected}`; got events: {event_nodes:?}"
            );
        }

        assert!(
            topic_nodes.is_empty(),
            "NestJS messaging must no longer emit NodeKind::Topic (canonical identity); got: {topic_nodes:?}"
        );
        assert!(
            event_nodes.iter().all(|n| !n
                .external_id
                .as_deref()
                .unwrap_or("")
                .starts_with("__topic__")),
            "Event nodes must not use __topic__ prefix"
        );
        assert!(
            parsed
                .edges
                .iter()
                .any(|edge| edge.kind == gather_step_core::EdgeKind::UsesEventFrom),
            "expected EventPattern handlers to emit UsesEventFrom"
        );
        assert!(
            parsed
                .edges
                .iter()
                .any(|edge| edge.kind == gather_step_core::EdgeKind::ProducesEventFor),
            "expected emit() to emit ProducesEventFor"
        );
    }

    #[test]
    fn use_guards_emits_guard_file_edges() {
        let temp_dir = TestDir::new("use-guards");
        fs::write(
            temp_dir.path().join("auth.guard.ts"),
            "export class AuthGuard {}\n",
        )
        .expect("guard fixture should write");
        fs::write(
            temp_dir.path().join("controller.ts"),
            r#"
import { Controller, Get, UseGuards } from '@nestjs/common';
import { AuthGuard } from './auth.guard';

@Controller('orders')
@UseGuards(AuthGuard)
export class OrdersController {
  @Get()
  listOrders() {
    return [];
  }
}
"#,
        )
        .expect("controller fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "controller.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("controller fixture should parse");
        let augmentation = super::augment(&parsed);

        assert!(
            augmentation
                .edges
                .iter()
                .any(|edge| { edge.kind == gather_step_core::EdgeKind::UsesGuardFrom }),
            "expected UseGuards(AuthGuard) to emit UsesGuardFrom"
        );
        // NOTE: the additive Class-level `UsesGuardFrom` target is only
        // emitted when the import binding has `resolved_path = Some(..)`.
        // This fixture does not exercise the resolver, so we do not assert
        // on the class-level target here; the end-to-end effect is exercised
        // by `shared_contract_impact` tests in `gather-step-analysis`.
    }

    #[test]
    fn custom_event_pattern_produces_event_node() {
        let temp_dir = TestDir::new("custom-event");
        fs::write(
            temp_dir.path().join("handler.ts"),
            r#"
import { CustomEventPattern } from '@nestjs/microservices';

export class CustomHandler {
  @CustomEventPattern('order.created')
  handle() {}
}
"#,
        )
        .expect("handler fixture should write");

        let parsed = parse_file(
            "order-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "handler.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("handler fixture should parse");

        assert!(parsed.nodes.iter().any(|node| {
            node.kind == gather_step_core::NodeKind::Event
                && node.external_id.as_deref() == Some("__event__kafka__order.created")
        }));
    }

    #[test]
    fn schema_classes_produce_entity_nodes_without_controller_decorator() {
        let temp_dir = TestDir::new("schema");
        fs::write(
            temp_dir.path().join("entity.ts"),
            r#"
import { Schema } from '@nestjs/mongoose';

@Schema()
export class SampleEntity {}
"#,
        )
        .expect("entity fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "entity.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("entity fixture should parse");

        assert!(parsed.nodes.iter().any(|node| {
            node.kind == gather_step_core::NodeKind::Entity && node.name == "SampleEntity"
        }));
    }

    #[test]
    fn route_constants_are_resolved_from_module_local_object_literals() {
        let temp_dir = TestDir::new("routes");
        fs::write(
            temp_dir.path().join("controller.ts"),
            r#"
import { Controller, Get } from '@nestjs/common';

const Routes = { items: { list: 'items/list' } };

@Controller({ path: Routes.items.list })
export class ItemController {
  @Get()
  list() {
    return [];
  }
}
"#,
        )
        .expect("controller fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "controller.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("controller fixture should parse");

        assert!(parsed.nodes.iter().any(|node| {
            node.kind == gather_step_core::NodeKind::Route
                && node.external_id.as_deref() == Some("__route__GET__/items/list")
        }));
    }

    #[test]
    fn message_pattern_resolves_topic_from_constant_reference() {
        let temp_dir = TestDir::new("topic-constant");
        fs::write(
            temp_dir.path().join("controller.ts"),
            r#"
import { MessagePattern } from '@nestjs/microservices';

const Routes = { events: { created: 'items.created' } };

export class EventController {
  @MessagePattern([Routes.events.created])
  handleCreated() {
    return {};
  }
}
"#,
        )
        .expect("controller fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "controller.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("controller fixture should parse");

        // Canonical identity: `@MessagePattern` converges on `NodeKind::Event`
        // with the `__event__kafka__` prefix.
        assert!(parsed.nodes.iter().any(|node| {
            node.kind == gather_step_core::NodeKind::Event
                && node.external_id.as_deref() == Some("__event__kafka__items.created")
        }));
    }

    #[test]
    fn event_pattern_array_fans_out_every_literal() {
        let temp_dir = TestDir::new("multi-event-pattern");
        fs::write(
            temp_dir.path().join("handler.ts"),
            r#"
import { EventPattern } from '@nestjs/microservices';

export class EventController {
  @EventPattern(['user.created', 'user.updated'])
  handleEvent() {
    return {};
  }
}
"#,
        )
        .expect("handler fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "handler.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("handler fixture should parse");

        let matching_events: Vec<_> = parsed
            .nodes
            .iter()
            .filter(|node| {
                node.kind == gather_step_core::NodeKind::Event
                    && matches!(
                        node.external_id.as_deref(),
                        Some("__event__kafka__user.created" | "__event__kafka__user.updated")
                    )
            })
            .collect();
        assert_eq!(
            matching_events.len(),
            2,
            "expected one event node per array literal, got {matching_events:?}"
        );
        let matching_event_ids: Vec<_> = matching_events.iter().map(|node| node.id).collect();
        let consumes_count = parsed
            .edges
            .iter()
            .filter(|edge| {
                edge.kind == gather_step_core::EdgeKind::Consumes
                    && matching_event_ids.contains(&edge.target)
            })
            .count();
        let uses_event_from_count = parsed
            .edges
            .iter()
            .filter(|edge| {
                edge.kind == gather_step_core::EdgeKind::UsesEventFrom
                    && matching_event_ids.contains(&edge.target)
            })
            .count();
        assert_eq!(
            consumes_count, 2,
            "expected one Consumes edge per resolved array element"
        );
        assert_eq!(
            uses_event_from_count, 2,
            "expected one UsesEventFrom edge per resolved array element"
        );
    }

    #[test]
    fn event_pattern_resolves_imported_enum_member_from_d_ts() {
        let temp_dir = TestDir::new("event-pattern-d-ts-enum");
        fs::write(
            temp_dir.path().join("topics.d.ts"),
            r#"
export declare enum EventTopic {
  Platform = 'platform.lifecycle'
}
"#,
        )
        .expect("topics fixture should write");
        fs::write(
            temp_dir.path().join("handler.ts"),
            r#"
import { EventPattern } from '@nestjs/microservices';
import { EventTopic } from './topics';

export class EventController {
  @EventPattern(EventTopic.Platform)
  handleEvent() {
    return {};
  }
}
"#,
        )
        .expect("handler fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "handler.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("handler fixture should parse");

        assert!(parsed.nodes.iter().any(|node| {
            node.kind == gather_step_core::NodeKind::Event
                && node.external_id.as_deref() == Some("__event__kafka__platform.lifecycle")
        }));
    }

    /// Two-file barrel chain: `index.d.ts` re-exports `EventType` via
    /// `export { EventType } from './enums'`; `enums.d.ts` defines the enum
    /// with a literal member.  The producer resolution should follow the
    /// one-hop barrel and land on the literal member value.
    #[test]
    fn event_pattern_resolves_enum_member_through_barrel_reexport() {
        let temp_dir = TestDir::new("event-pattern-barrel-reexport");
        // Define the enum in a leaf file.
        fs::write(
            temp_dir.path().join("enums.d.ts"),
            r#"
export declare enum EventType {
  OrderPlaced = 'order.placed'
}
"#,
        )
        .expect("enums fixture should write");
        // Barrel that re-exports the enum.
        fs::write(
            temp_dir.path().join("index.d.ts"),
            r#"
export { EventType } from './enums';
"#,
        )
        .expect("index barrel fixture should write");
        // Handler that imports from the barrel.
        fs::write(
            temp_dir.path().join("handler.ts"),
            r#"
import { EventPattern } from '@nestjs/microservices';
import { EventType } from './index';

export class OrderController {
  @EventPattern(EventType.OrderPlaced)
  handleOrderPlaced() {}
}
"#,
        )
        .expect("handler fixture should write");

        let parsed = parse_file(
            "order-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "handler.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("handler fixture should parse");

        assert!(
            parsed.nodes.iter().any(|node| {
                node.kind == gather_step_core::NodeKind::Event
                    && node.external_id.as_deref() == Some("__event__kafka__order.placed")
            }),
            "expected an Event node for 'order.placed' resolved through barrel re-export;\n\
             nodes found: {:#?}",
            parsed
                .nodes
                .iter()
                .filter(|n| n.kind == gather_step_core::NodeKind::Event)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn message_pattern_resolves_cross_file_multi_level_member_chain() {
        let temp_dir = TestDir::new("cross-file-topic-chain");
        fs::write(
            temp_dir.path().join("topics.ts"),
            r#"
export const Messaging = {
  kafka: {
    orders: {
      sync: 'orders.sync',
    },
  },
};
"#,
        )
        .expect("topics fixture should write");
        fs::write(
            temp_dir.path().join("handler.ts"),
            r#"
import { MessagePattern } from '@nestjs/microservices';
import { Messaging } from './topics';

export class EventController {
  @MessagePattern([Messaging.kafka.orders.sync])
  handleEvent() {
    return {};
  }
}
"#,
        )
        .expect("handler fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "handler.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("handler fixture should parse");

        // `@MessagePattern` emits on Event kind (canonical messaging identity).
        assert!(parsed.nodes.iter().any(|node| {
            node.kind == gather_step_core::NodeKind::Event
                && node.external_id.as_deref() == Some("__event__kafka__orders.sync")
        }));
    }

    #[test]
    fn message_pattern_resolves_imported_static_template_literal_const() {
        let temp_dir = TestDir::new("imported-template-topic");
        fs::write(
            temp_dir.path().join("topics.ts"),
            "export const ORDER_SYNC = `orders.sync`;\n",
        )
        .expect("topics fixture should write");
        fs::write(
            temp_dir.path().join("handler.ts"),
            r#"
import { MessagePattern } from '@nestjs/microservices';
import { ORDER_SYNC } from './topics';

export class EventController {
  @MessagePattern([ORDER_SYNC])
  handleEvent() {
    return {};
  }
}
"#,
        )
        .expect("handler fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "handler.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("handler fixture should parse");

        // `@MessagePattern` emits on Event kind (canonical messaging identity).
        assert!(parsed.nodes.iter().any(|node| {
            node.kind == gather_step_core::NodeKind::Event
                && node.external_id.as_deref() == Some("__event__kafka__orders.sync")
        }));
    }

    #[test]
    fn event_pattern_ignores_transport_argument_after_pattern_metadata() {
        let temp_dir = TestDir::new("event-pattern-transport");
        fs::write(
            temp_dir.path().join("handler.ts"),
            r#"
import { EventPattern, Transport } from '@nestjs/microservices';

export class EventController {
  @EventPattern('user.created', Transport.KAFKA)
  handleEvent() {
    return {};
  }
}
"#,
        )
        .expect("handler fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "handler.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("handler fixture should parse");

        let event_ids: Vec<_> = parsed
            .nodes
            .iter()
            .filter(|node| node.kind == gather_step_core::NodeKind::Event)
            .filter_map(|node| node.external_id.as_deref())
            .collect();
        assert_eq!(event_ids, vec!["__event__kafka__user.created"]);
    }

    #[test]
    fn message_pattern_with_object_argument_does_not_synthesize_topic_name() {
        let temp_dir = TestDir::new("message-pattern-object");
        fs::write(
            temp_dir.path().join("handler.ts"),
            r#"
import { MessagePattern } from '@nestjs/microservices';

export class EventController {
  @MessagePattern({})
  handleEvent() {
    return {};
  }
}
"#,
        )
        .expect("handler fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "handler.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("handler fixture should parse");

        assert!(
            !parsed
                .nodes
                .iter()
                .any(|node| node.kind == gather_step_core::NodeKind::Topic),
            "non-literal MessagePattern metadata must not synthesize a topic node"
        );
    }

    #[test]
    fn message_pattern_with_unresolved_identifier_does_not_emit_topic() {
        let temp_dir = TestDir::new("message-pattern-ident");
        fs::write(
            temp_dir.path().join("handler.ts"),
            r#"
import { MessagePattern } from '@nestjs/microservices';

export class EventController {
  @MessagePattern(TOPIC)
  handleEvent() {
    return {};
  }
}
"#,
        )
        .expect("handler fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "handler.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("handler fixture should parse");

        assert!(
            !parsed
                .nodes
                .iter()
                .any(|node| node.kind == gather_step_core::NodeKind::Topic),
            "unresolved MessagePattern identifier must not fabricate a topic node"
        );
    }

    #[test]
    fn message_pattern_with_unresolved_member_expression_does_not_emit_topic() {
        let temp_dir = TestDir::new("message-pattern-member");
        fs::write(
            temp_dir.path().join("handler.ts"),
            r#"
import { MessagePattern } from '@nestjs/microservices';

export class EventController {
  @MessagePattern(Topics.UserCreated)
  handleEvent() {
    return {};
  }
}
"#,
        )
        .expect("handler fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "handler.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("handler fixture should parse");

        assert!(
            !parsed
                .nodes
                .iter()
                .any(|node| node.kind == gather_step_core::NodeKind::Topic),
            "unresolved MessagePattern member expression must not fabricate a topic node"
        );
    }

    #[test]
    fn bull_processor_produces_queue_virtual_node_and_consumer_edges() {
        // `@Processor('queue-name')` on a class identifies a Bull queue. Each
        // `@Process('job-type')` method inside that class handles jobs from
        // that queue — so we emit one `Queue` virtual node per processor with
        // QN `__queue__bull__<name>` and two Consumes edges: one from the
        // class (the processor itself) and one from each job handler method.
        let temp_dir = TestDir::new("bull");
        fs::write(
            temp_dir.path().join("processor.ts"),
            r#"
import { Processor, Process } from '@nestjs/bull';

@Processor('report-generation')
export class ReportProcessor {
  @Process('pdf-export')
  async exportPdf() { return {}; }

  @Process('csv-export')
  async exportCsv() { return {}; }
}
"#,
        )
        .expect("processor fixture should write");

        let parsed = parse_file(
            "report-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "processor.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("processor fixture should parse");

        let queue_external_id = "__queue__bull__report-generation";
        let queue_nodes: Vec<_> = parsed
            .nodes
            .iter()
            .filter(|node| node.kind == gather_step_core::NodeKind::Queue)
            .collect();
        assert_eq!(queue_nodes.len(), 1, "one queue virtual node expected");
        assert_eq!(
            queue_nodes[0].external_id.as_deref(),
            Some(queue_external_id),
            "queue QN should match __queue__bull__<name>"
        );

        // One Consumes edge from the processor class + one from each job
        // handler method. All point at the same queue NodeId (virtual node
        // deduplication means subsequent processors on the same queue share
        // the node).
        let consumes_count = parsed
            .edges
            .iter()
            .filter(|edge| {
                edge.kind == gather_step_core::EdgeKind::Consumes
                    && edge.target == queue_nodes[0].id
            })
            .count();
        assert_eq!(
            consumes_count, 3,
            "class + 2 handler methods should each Consumes the queue, got {consumes_count}"
        );
    }

    #[test]
    fn bull_processor_supports_object_argument_name() {
        let temp_dir = TestDir::new("bull-object");
        fs::write(
            temp_dir.path().join("processor.ts"),
            r#"
import { Processor, Process } from '@nestjs/bull';

@Processor({ name: 'report-generation' })
export class ReportProcessor {
  @Process('pdf-export')
  async exportPdf() { return {}; }
}
"#,
        )
        .expect("processor fixture should write");

        let parsed = parse_file(
            "report-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "processor.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("processor fixture should parse");

        assert!(parsed.nodes.iter().any(|node| {
            node.kind == gather_step_core::NodeKind::Queue
                && node.external_id.as_deref() == Some("__queue__bull__report-generation")
        }));
    }

    #[test]
    fn inject_decorator_produces_di_edge_with_token() {
        // `@Inject('CACHE_MANAGER')` is a parameter decorator that tree-sitter
        // captures as a call expression, not as a class decorator. The extractor
        // must scan `call_sites` for callee_name == "Inject" and produce a
        // virtual Class node with QN `__di__CACHE_MANAGER` plus a DependsOn edge
        // from the call owner (the constructor) to that node.
        let temp_dir = TestDir::new("inject");
        fs::write(
            temp_dir.path().join("service.ts"),
            r#"
import { Injectable, Inject } from '@nestjs/common';

@Injectable()
export class CacheService {
  constructor(
    @Inject('CACHE_MANAGER') private cache: any,
  ) {}
}
"#,
        )
        .expect("inject fixture should write");

        let parsed = parse_file(
            "cache-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "service.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("inject fixture should parse");

        let di_node = parsed
            .nodes
            .iter()
            .find(|n| n.external_id.as_deref() == Some("__di__CACHE_MANAGER"));
        assert!(
            di_node.is_some(),
            "@Inject('CACHE_MANAGER') should produce a virtual node with QN __di__CACHE_MANAGER, \
             got nodes: {:?}",
            parsed
                .nodes
                .iter()
                .map(|n| n.external_id.as_deref())
                .collect::<Vec<_>>()
        );

        let di_node = di_node.unwrap();
        let has_depends_on = parsed
            .edges
            .iter()
            .any(|e| e.kind == gather_step_core::EdgeKind::DependsOn && e.target == di_node.id);
        assert!(
            has_depends_on,
            "a DependsOn edge should point to the __di__CACHE_MANAGER node"
        );
    }

    #[test]
    fn inject_model_produces_di_edge() {
        // `@InjectModel(Product.name)` is captured as a call expression with
        // literal_argument == "Product.name". The extractor strips the `.name`
        // suffix to produce QN `__di__model__Product`.
        let temp_dir = TestDir::new("inject-model");
        fs::write(
            temp_dir.path().join("repo.ts"),
            r#"
import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';

export class Product {}
Product.name = 'Product';

@Injectable()
export class ProductRepository {
  constructor(
    @InjectModel(Product.name) private productModel: any,
  ) {}
}
"#,
        )
        .expect("inject-model fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "repo.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("inject-model fixture should parse");

        let di_node = parsed
            .nodes
            .iter()
            .find(|n| n.external_id.as_deref() == Some("__di__model__Product"));
        assert!(
            di_node.is_some(),
            "@InjectModel(Product.name) should produce a virtual node with QN __di__model__Product, \
             got nodes: {:?}",
            parsed
                .nodes
                .iter()
                .map(|n| n.external_id.as_deref())
                .collect::<Vec<_>>()
        );

        let di_node = di_node.unwrap();
        let has_depends_on = parsed
            .edges
            .iter()
            .any(|e| e.kind == gather_step_core::EdgeKind::DependsOn && e.target == di_node.id);
        assert!(
            has_depends_on,
            "a DependsOn edge should point to the __di__model__Product node"
        );
    }

    #[test]
    fn inject_without_argument_does_not_create_spurious_node() {
        let temp_dir = TestDir::new("inject-empty");
        fs::write(
            temp_dir.path().join("service.ts"),
            r#"
import { Injectable, Inject } from '@nestjs/common';

@Injectable()
export class CacheService {
  constructor(@Inject() private cache: any) {}
}
"#,
        )
        .expect("inject fixture should write");

        let parsed = parse_file(
            "cache-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "service.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("inject fixture should parse");

        assert!(
            parsed
                .nodes
                .iter()
                .all(|node| node.external_id.as_deref() != Some("__di__"))
        );
    }

    #[test]
    fn transport_aware_producer_detects_service_bus() {
        // When the callee's receiver object path contains "servicebus"
        // (case-insensitive), the extractor should use the `servicebus`
        // transport prefix instead of the default `kafka` prefix.
        // `this.serviceBusClient.emit('order.placed', data)` → qualified hint
        // `this.serviceBusClient.emit` → transport `servicebus`.
        let temp_dir = TestDir::new("servicebus");
        fs::write(
            temp_dir.path().join("publisher.ts"),
            r#"
import { Injectable } from '@nestjs/common';

@Injectable()
export class OrderPublisher {
  async publish() {
    this.serviceBusClient.emit('order.placed', { id: 1 });
  }
}
"#,
        )
        .expect("servicebus fixture should write");

        let parsed = parse_file(
            "order-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "publisher.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("servicebus fixture should parse");

        let event_node = parsed
            .nodes
            .iter()
            .find(|n| n.external_id.as_deref() == Some("__event__servicebus__order.placed"));
        assert!(
            event_node.is_some(),
            "serviceBusClient.emit should produce an Event node with QN \
             __event__servicebus__order.placed, got nodes: {:?}",
            parsed
                .nodes
                .iter()
                .filter(|n| n.kind == gather_step_core::NodeKind::Event)
                .map(|n| n.external_id.as_deref())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn transport_aware_producer_detects_pubsub() {
        let temp_dir = TestDir::new("pubsub");
        fs::write(
            temp_dir.path().join("publisher.ts"),
            r#"
export class OrderPublisher {
  async publish() {
    this.webPubSubClient.emit('order.placed', { id: 1 });
  }
}
"#,
        )
        .expect("pubsub fixture should write");

        let parsed = parse_file(
            "order-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "publisher.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("pubsub fixture should parse");

        assert!(
            parsed.nodes.iter().any(|node| {
                node.external_id.as_deref() == Some("__event__pubsub__order.placed")
            })
        );
    }

    #[test]
    fn method_level_version_overrides_class_version() {
        let temp_dir = TestDir::new("version-override");
        fs::write(
            temp_dir.path().join("controller.ts"),
            r#"
import { Controller, Get, Version } from '@nestjs/common';

@Version('1')
@Controller('items')
export class ItemsController {
  @Version('2')
  @Get()
  list() {
    return [];
  }
}
"#,
        )
        .expect("version fixture should write");

        let parsed = parse_file(
            "items-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "controller.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("version fixture should parse");

        assert!(
            parsed
                .nodes
                .iter()
                .any(|node| { node.external_id.as_deref() == Some("__route__GET__/v2/items") })
        );
        assert!(parsed.nodes.iter().all(|node| {
            node.kind != gather_step_core::NodeKind::Route
                || node.external_id.as_deref() != Some("__route__GET__/v1/items")
        }));
    }

    #[test]
    fn constructor_di_collects_multiple_dependencies() {
        let temp_dir = TestDir::new("multi-di");
        fs::write(
            temp_dir.path().join("service.ts"),
            r#"
import { Injectable } from '@nestjs/common';

@Injectable()
export class WorkflowService {}

@Injectable()
export class AuditService {}

@Injectable()
export class CacheService {
  constructor(
    private readonly workflow: WorkflowService,
    private readonly audit: AuditService,
  ) {}
}
"#,
        )
        .expect("service fixture should write");

        let parsed = parse_file(
            "cache-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "service.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("service fixture should parse");

        let class_symbol = parsed
            .symbols
            .iter()
            .find(|symbol| symbol.node.name == "CacheService")
            .expect("class symbol should exist");
        assert_eq!(
            class_symbol.constructor_dependencies,
            vec!["WorkflowService", "AuditService"]
        );
    }

    #[test]
    fn producer_skips_template_interpolation_topic_argument() {
        // Template literals like `topic-${id}` are dynamic expressions — they
        // must NOT be indexed as concrete topic literals, because the actual
        // topic name is only known at runtime.
        let temp_dir = TestDir::new("dynamic-topic");
        // Note: the backtick template literal is written as a raw string to
        // avoid Rust escape-sequence conflicts.
        fs::write(
            temp_dir.path().join("publisher.ts"),
            concat!(
                "export class OrderPublisher {\n",
                "  async publish(id: number) {\n",
                "    this.client.emit(`topic-${id}`, { id });\n",
                "  }\n",
                "}\n",
            ),
        )
        .expect("publisher fixture should write");

        let parsed = parse_file(
            "order-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "publisher.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("publisher fixture should parse");

        assert!(
            parsed
                .nodes
                .iter()
                .all(|node| node.kind != gather_step_core::NodeKind::Event),
            "template-literal topic expressions must not produce Event nodes, \
             got: {:?}",
            parsed
                .nodes
                .iter()
                .filter(|n| n.kind == gather_step_core::NodeKind::Event)
                .map(|n| n.external_id.as_deref())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn nested_controller_with_multi_segment_base_path() {
        // Test that @Controller('api/v1/resources') + @Get(':id/details')
        // produces __route__GET__/api/v1/resources/:id/details
        let temp_dir = TestDir::new("nested-controller");
        fs::write(
            temp_dir.path().join("controller.ts"),
            r#"
import { Controller, Get } from '@nestjs/common';

@Controller('api/v1/resources')
export class ResourceController {
  @Get(':id/details')
  getDetails() {
    return {};
  }
}
"#,
        )
        .expect("controller fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "controller.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("controller fixture should parse");

        let route_node = parsed
            .nodes
            .iter()
            .find(|n| n.kind == gather_step_core::NodeKind::Route);
        assert!(
            route_node.is_some(),
            "a Route virtual node should be emitted for @Get(':id/details')"
        );
        let route_node = route_node.unwrap();
        assert_eq!(
            route_node.qualified_name.as_deref(),
            Some("__route__GET__/api/v1/resources/:id/details"),
            "multi-segment controller base path + method sub-path should compose correctly, \
             got route nodes: {:?}",
            parsed
                .nodes
                .iter()
                .filter(|n| n.kind == gather_step_core::NodeKind::Route)
                .map(|n| n.qualified_name.as_deref())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn version_decorator_composes_route_path() {
        // `@Version('2')` on a controller class causes all its routes to be
        // prefixed with `/v2` before the controller base path, so:
        // @Version('2') + @Controller('items') + @Get() → /v2/items
        let temp_dir = TestDir::new("version");
        fs::write(
            temp_dir.path().join("controller.ts"),
            r#"
import { Controller, Get, Version } from '@nestjs/common';

@Version('2')
@Controller('items')
export class ItemsController {
  @Get()
  list() {
    return [];
  }
}
"#,
        )
        .expect("version fixture should write");

        let parsed = parse_file(
            "items-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "controller.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("version fixture should parse");

        let route_node = parsed
            .nodes
            .iter()
            .find(|n| n.external_id.as_deref() == Some("__route__GET__/v2/items"));
        assert!(
            route_node.is_some(),
            "@Version('2') + @Controller('items') + @Get() should produce route \
             __route__GET__/v2/items, got route nodes: {:?}",
            parsed
                .nodes
                .iter()
                .filter(|n| n.kind == gather_step_core::NodeKind::Route)
                .map(|n| n.external_id.as_deref())
                .collect::<Vec<_>>()
        );
    }

    /// Write `source` to a temp file, parse it through the full `NestJS`
    /// augmentation pipeline, and return the resulting `ParsedFile`.
    ///
    /// `ParsedFile::nodes` will include both the base AST nodes and any virtual
    /// nodes (Event, Topic, Route, …) produced by the `NestJS` augmentor.
    fn parse_nestjs_fixture(source: &str) -> crate::ParsedFile {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let label = format!("nestjs-inline-{id}");
        let filename = format!("{label}.ts");
        let temp_dir = TestDir::new(&label);
        let path = temp_dir.path().join(&filename);
        fs::write(&path, source).expect("fixture source should write");
        parse_file(
            "test-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: filename.into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("inline fixture should parse")
    }

    #[test]
    fn producer_does_not_emit_event_for_variable_identifier_argument() {
        // emit(variableName, payload) — variableName is a raw identifier,
        // NOT a string literal. No Event node should be produced.
        let parsed = parse_nestjs_fixture(
            r#"
            export class Svc {
                constructor(private readonly client: ClientKafka) {}
                run() {
                    this.client.emit(variableName, { payload: 1 });
                }
            }
            "#,
        );
        assert!(
            !parsed
                .nodes
                .iter()
                .any(|n| matches!(n.kind, NodeKind::Event)),
            "raw identifier must not produce an Event node; got: {:?}",
            parsed
                .nodes
                .iter()
                .filter(|n| matches!(n.kind, NodeKind::Event))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn producer_does_not_emit_event_for_member_expression_argument() {
        // emit(obj.prop.method, ...) — member expression, not a literal.
        let parsed = parse_nestjs_fixture(
            r#"
            export class Svc {
                constructor(private readonly client: ClientKafka) {}
                run() {
                    this.client.emit(obj.prop.method, { x: 1 });
                }
            }
            "#,
        );
        assert!(
            !parsed
                .nodes
                .iter()
                .any(|n| matches!(n.kind, NodeKind::Event)),
            "member expression must not produce an Event node"
        );
    }

    #[test]
    fn producer_still_emits_event_for_literal_string_argument() {
        // Positive control: a real string literal MUST still produce an Event.
        let parsed = parse_nestjs_fixture(
            r#"
            export class Svc {
                constructor(private readonly client: ClientKafka) {}
                run() {
                    this.client.emit("orders.created", { payload: 1 });
                }
            }
            "#,
        );
        assert!(
            parsed
                .nodes
                .iter()
                .any(|n| matches!(n.kind, NodeKind::Event)
                    && n.qualified_name
                        .as_deref()
                        .unwrap_or("")
                        .contains("orders.created")),
            "real string literal must still produce an Event node"
        );
    }

    #[test]
    fn producer_resolves_imported_member_chain_target() {
        let temp_dir = TestDir::new("producer-imported-chain");
        fs::write(
            temp_dir.path().join("topics.ts"),
            r#"
export const Topics = {
    events: {
        created: 'orders.created',
    },
};
"#,
        )
        .expect("fixture should write");
        fs::write(
            temp_dir.path().join("service.ts"),
            r#"
import { Topics } from './topics';

export class Svc {
    constructor(private readonly client: ClientKafka) {}

    run() {
        this.client.emit(Topics.events.created, { payload: 1 });
    }
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "service.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        assert!(
            parsed.nodes.iter().any(|n| {
                matches!(n.kind, NodeKind::Event)
                    && n.qualified_name.as_deref() == Some("__event__kafka__orders.created")
            }),
            "imported member-chain producer target must produce an Event node"
        );
    }

    #[test]
    fn should_skip_dynamic_topic_keeps_ordinary_alphanumeric_topic_literals() {
        use super::should_skip_dynamic_topic;
        // Plain alphanumeric/underscore names are real literals — must NOT be skipped.
        assert!(!should_skip_dynamic_topic("user_created"));
        assert!(!should_skip_dynamic_topic("USER_CREATED"));
        assert!(!should_skip_dynamic_topic("userCreated"));
        // Dot-separated and kebab-case names are also valid NestJS topic literals.
        assert!(!should_skip_dynamic_topic("order.placed"));
        assert!(!should_skip_dynamic_topic("order-placed"));
        assert!(!should_skip_dynamic_topic("sample.updated"));
        // Empty strings are not useful literals — skip.
        assert!(should_skip_dynamic_topic(""));
        // Template/interpolation expressions must be skipped — the `$`, `{`, `}`
        // characters indicate a dynamic (non-literal) value.
        assert!(should_skip_dynamic_topic("user.${tenant}"));
        assert!(should_skip_dynamic_topic("user-${tenant}"));
        // Whitespace also indicates a non-literal value.
        assert!(should_skip_dynamic_topic("user created"));
    }

    #[test]
    fn split_top_level_keeps_template_literals_intact() {
        let values = super::split_top_level("`hello,${world}`,'user.updated'", ',');
        assert_eq!(values, vec!["`hello,${world}`", "'user.updated'"]);
    }

    // -----------------------------------------------------------------------
    // False-positive negative tests — these document known bugs in the
    // producer extractor that match by method-name only without checking the
    // receiver type. They are `#[ignore]`d so CI stays green while the bugs
    // remain unfixed; removing `#[ignore]` is the acceptance test for the fix.
    // -----------------------------------------------------------------------

    #[test]
    fn producer_does_not_fabricate_topic_for_http_response_send() {
        let parsed = parse_nestjs_fixture(
            r#"
            import { Controller, Get, Res } from '@nestjs/common';
            import { Response } from 'express';

            @Controller()
            export class AppController {
                @Get('/')
                index(@Res() res: Response) {
                    res.send("ok");
                }
            }
            "#,
        );
        assert!(
            !parsed
                .nodes
                .iter()
                .any(|n| matches!(n.kind, NodeKind::Topic)),
            "res.send() must not produce a Topic node; got: {:?}",
            parsed
                .nodes
                .iter()
                .filter(|n| matches!(n.kind, NodeKind::Topic))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn producer_does_not_fabricate_event_for_socket_emit() {
        let parsed = parse_nestjs_fixture(
            r#"
            export class ChatGateway {
                handleJoin(socket: any, room: string) {
                    socket.emit("joined", { room });
                }
            }
            "#,
        );
        assert!(
            !parsed
                .nodes
                .iter()
                .any(|n| matches!(n.kind, NodeKind::Event)),
            "socket.emit() must not produce an Event node; got: {:?}",
            parsed
                .nodes
                .iter()
                .filter(|n| matches!(n.kind, NodeKind::Event))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn producer_does_not_fabricate_topic_for_bare_send_call() {
        // A plain `send("topic", payload)` with no receiver (e.g. imported from
        // a utility lib) should not be indexed as a Kafka Publishes edge.
        let parsed = parse_nestjs_fixture(
            r#"
            import { send } from 'some-util';

            export class NotifierService {
                notify() {
                    send("notification.sent", { id: 1 });
                }
            }
            "#,
        );
        assert!(
            !parsed
                .nodes
                .iter()
                .any(|n| matches!(n.kind, NodeKind::Topic)),
            "bare send() without a messaging client receiver must not produce a Topic node; got: {:?}",
            parsed
                .nodes
                .iter()
                .filter(|n| matches!(n.kind, NodeKind::Topic))
                .collect::<Vec<_>>()
        );
    }

    // -----------------------------------------------------------------------
    // detect_transport unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn detect_transport_defaults_to_kafka_for_unrecognised_receiver() {
        // A receiver that contains neither "servicebus" nor "pubsub" should
        // resolve to the kafka transport.
        assert_eq!(super::detect_transport("this.myClient.emit"), "kafka");
        assert_eq!(
            super::detect_transport("this.messagingClient.send"),
            "kafka"
        );
        assert_eq!(super::detect_transport("client.emit"), "kafka");
    }

    // -----------------------------------------------------------------------
    // Known gap: zero-interpolation template literal arguments (Finding 4)
    // -----------------------------------------------------------------------

    #[test]
    fn producer_extracts_event_from_static_template_literal() {
        // `client.emit(\`orders.created\`, ...)` — a zero-interpolation template
        // literal is semantically equivalent to a string literal but represented
        // as a `TemplateLiteral` AST node. Producer extraction currently misses it.
        let parsed = parse_nestjs_fixture(
            r#"
            export class OrderService {
                constructor(private readonly client: ClientKafka) {}
                placeOrder() {
                    this.client.emit(`orders.created`, { id: 1 });
                }
            }
            "#,
        );
        assert!(
            parsed.nodes.iter().any(|n| {
                matches!(n.kind, NodeKind::Event)
                    && n.qualified_name
                        .as_deref()
                        .unwrap_or("")
                        .contains("orders.created")
            }),
            "static template literal must produce an Event node"
        );
    }

    // -----------------------------------------------------------------------
    // Known gap: @Controller object-literal variants (Finding 5)
    // -----------------------------------------------------------------------

    #[test]
    fn controller_object_with_multi_property_options_extracts_path() {
        let temp_dir = TestDir::new("controller-multi-prop");
        fs::write(
            temp_dir.path().join("ctrl.ts"),
            r#"
import { Controller, Get } from '@nestjs/common';

@Controller({ version: '1', path: 'items' })
export class ItemController {
    @Get()
    list() { return []; }
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "ctrl.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        assert!(
            parsed.nodes.iter().any(|n| {
                n.external_id
                    .as_deref()
                    .is_some_and(|id| id.contains("/items"))
            }),
            "multi-property @Controller options must still extract the path segment"
        );
    }

    #[test]
    fn controller_object_with_quoted_path_key_extracts_path() {
        let temp_dir = TestDir::new("controller-quoted-key");
        fs::write(
            temp_dir.path().join("ctrl.ts"),
            r#"
import { Controller, Get } from '@nestjs/common';

@Controller({'path': 'widgets'})
export class WidgetController {
    @Get()
    list() { return []; }
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "ctrl.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        assert!(
            parsed.nodes.iter().any(|n| {
                n.external_id
                    .as_deref()
                    .is_some_and(|id| id.contains("/widgets"))
            }),
            "quoted path key in @Controller options must still extract the path segment"
        );
    }

    // -----------------------------------------------------------------------
    // @Process in argless @Processor → zero Consumes edges
    // (processor with no decorator argument)
    // -----------------------------------------------------------------------

    #[test]
    fn process_handler_inside_argless_processor_produces_zero_consumes_edges() {
        // `@Processor()` with no argument defines no queue. A `@Process('job')`
        // method inside it must produce zero Consumes edges — not a fallback edge
        // to a synthetic or unnamed queue.
        let temp_dir = TestDir::new("processor-argless-process");
        fs::write(
            temp_dir.path().join("proc.ts"),
            r#"
import { Processor, Process } from '@nestjs/bull';

@Processor()
export class GenericProcessor {
    @Process('build')
    handleBuild() { return {}; }
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "worker-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "proc.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        let consumes_count = parsed
            .edges
            .iter()
            .filter(|e| e.kind == gather_step_core::EdgeKind::Consumes)
            .count();
        assert_eq!(
            consumes_count, 0,
            "@Process inside an argless @Processor must produce zero Consumes edges, got {consumes_count}"
        );
    }

    // -----------------------------------------------------------------------
    // Known bug A2: detect_transport substring false positives
    // -----------------------------------------------------------------------

    #[test]
    fn detect_transport_does_not_misclassify_substring_servicebus_receiver() {
        // `myServiceBusyClient` contains "servicebus" as a substring.
        // detect_transport currently returns "servicebus" for it.
        assert_eq!(
            super::detect_transport("this.myServiceBusyClient.emit"),
            "kafka",
            "receiver containing 'servicebus' as non-word substring must not classify as servicebus"
        );
        assert_eq!(
            super::detect_transport("pubsubEnabled.emit"),
            "kafka",
            "receiver containing 'pubsub' as a prefix must not classify as pubsub when it is not a client"
        );
    }

    // -----------------------------------------------------------------------
    // Known bug A3: @Inject / @InjectModel matched on callee name only
    // -----------------------------------------------------------------------

    #[test]
    fn inject_does_not_match_non_decorator_user_defined_function() {
        let parsed = parse_nestjs_fixture(
            r#"
            function Inject(token: string): void {
                console.log('injecting', token);
            }

            export class Util {
                setup() {
                    Inject('some-token');
                }
            }
            "#,
        );
        let has_di_node = parsed.nodes.iter().any(|n| {
            n.external_id
                .as_deref()
                .is_some_and(|id| id.starts_with("__di__"))
        });
        assert!(
            !has_di_node,
            "user-defined Inject() called outside a decorator context must not produce a __di__ node; \
             got: {:?}",
            parsed
                .nodes
                .iter()
                .filter(|n| n
                    .external_id
                    .as_deref()
                    .is_some_and(|id| id.starts_with("__di__")))
                .collect::<Vec<_>>()
        );
    }

    // -----------------------------------------------------------------------
    // Known bug A4: Expr::Tpl gap — @Inject(`TOKEN`) drops the DI edge
    // -----------------------------------------------------------------------

    #[test]
    fn inject_with_backtick_token_produces_di_edge() {
        let parsed = parse_nestjs_fixture(
            r#"
            import { Injectable, Inject } from '@nestjs/common';
            import { Cache } from 'cache-manager';

            @Injectable()
            export class CacheService {
                constructor(@Inject(`CACHE_MANAGER`) private cache: Cache) {}
            }
            "#,
        );
        let has_di_node = parsed.nodes.iter().any(|n| {
            n.external_id
                .as_deref()
                .is_some_and(|id| id.contains("CACHE_MANAGER"))
        });
        assert!(
            has_di_node,
            "@Inject with backtick token must produce a __di__CACHE_MANAGER virtual node; \
             got nodes: {:?}",
            parsed
                .nodes
                .iter()
                .map(|n| n.external_id.as_deref().unwrap_or("<none>"))
                .collect::<Vec<_>>()
        );
    }

    // -----------------------------------------------------------------------
    // Known gap Finding 11: @RequestMapping is unsupported
    // -----------------------------------------------------------------------

    #[test]
    fn request_mapping_decorator_produces_route_node() {
        let parsed = parse_nestjs_fixture(
            r#"
            import { Controller, RequestMapping } from '@nestjs/common';

            @Controller('api')
            export class ApiController {
                @RequestMapping({ path: 'search', method: 'GET' })
                search() { return []; }
            }
            "#,
        );
        let has_route = parsed
            .nodes
            .iter()
            .any(|n| matches!(n.kind, NodeKind::Route));
        assert!(
            has_route,
            "@RequestMapping must produce a Route node; got nodes: {:?}",
            parsed
                .nodes
                .iter()
                .map(|n| (&n.kind, n.external_id.as_deref().unwrap_or(&n.name)))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn send_message_object_argument_emits_topic_and_event_nodes() {
        let temp_dir = TestDir::new("send-message-object");
        fs::write(
            temp_dir.path().join("topics.ts"),
            r#"
export enum EventTopic {
  PlatformEvents = 'platform.events',
}
"#,
        )
        .expect("fixture should write");
        fs::write(
            temp_dir.path().join("event-types.ts"),
            r#"
export enum EventType {
  DocumentQueued = 'document.queued',
}
"#,
        )
        .expect("fixture should write");
        fs::write(
            temp_dir.path().join("service.ts"),
            r#"
import { EventTopic } from './topics';
import { EventType } from './event-types';

export class ProducerService {
  async run() {
    await this.kafkaProducerService.sendMessage({
      topic: EventTopic.PlatformEvents,
      message: {
        eventType: EventType.DocumentQueued,
      },
      messageKey: 'doc',
    });
  }
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "backend_standard",
            temp_dir.path(),
            &crate::FileEntry {
                path: "service.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        // Canonical identity: `sendMessage` emits on `NodeKind::Event`,
        // sharing a virtual node with any `@CustomEventPattern` /
        // `@EventPattern` / `@MessagePattern` consumer of the same topic
        // name.
        assert!(
            parsed.nodes.iter().any(|n| {
                matches!(n.kind, NodeKind::Event)
                    && n.qualified_name.as_deref() == Some("__event__kafka__platform.events")
            }),
            "sendMessage object arg must produce a canonical Event node for the topic"
        );
        let event_id = parsed
            .nodes
            .iter()
            .find(|n| {
                matches!(n.kind, NodeKind::Event)
                    && n.qualified_name.as_deref() == Some("__event__kafka__document.queued")
            })
            .map(|n| n.id)
            .expect("sendMessage object arg must produce a fine-grained Event node");
        assert!(
            parsed.edges.iter().any(|edge| {
                edge.kind == gather_step_core::EdgeKind::ProducesEventFor && edge.target == event_id
            }),
            "sendMessage object arg must produce ProducesEventFor edge to fine-grained Event node"
        );
    }

    #[test]
    fn decorated_consumer_switch_emits_fine_grained_event_consumer() {
        let temp_dir = TestDir::new("event-dispatch-switch");
        fs::write(
            temp_dir.path().join("event-types.ts"),
            r#"
export enum EventType {
  DocumentQueued = 'document.queued',
}
"#,
        )
        .expect("fixture should write");
        fs::write(
            temp_dir.path().join("service.ts"),
            r#"
import { CustomEventPattern } from '@nestjs/microservices';
import { EventType } from './event-types';

export class EventHandlersService {
  @CustomEventPattern('document-events')
  async handleEvent(event: any) {
    switch (event.eventType) {
      case EventType.DocumentQueued:
        return true;
      default:
        return false;
    }
  }
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "backend_standard",
            temp_dir.path(),
            &crate::FileEntry {
                path: "service.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        let event_id = parsed
            .nodes
            .iter()
            .find(|n| {
                matches!(n.kind, NodeKind::Event)
                    && n.qualified_name.as_deref() == Some("__event__kafka__document.queued")
            })
            .map(|n| n.id)
            .expect("decorated consumer switch must emit a fine-grained Event node");
        assert!(
            parsed.edges.iter().any(|edge| {
                edge.kind == gather_step_core::EdgeKind::UsesEventFrom && edge.target == event_id
            }),
            "decorated consumer switch must emit UsesEventFrom edge to fine-grained Event node"
        );
    }

    #[test]
    fn helper_built_send_message_payload_emits_all_possible_event_nodes() {
        let temp_dir = TestDir::new("send-message-helper-payload");
        fs::write(
            temp_dir.path().join("topics.ts"),
            r#"
export enum EventTopic {
  PlatformEvents = 'platform.events',
}
"#,
        )
        .expect("fixture should write");
        fs::write(
            temp_dir.path().join("event-types.ts"),
            r#"
export enum EventType {
  CsvGenerationQueued = 'csv.generation.queued',
  PdfGenerationQueued = 'pdf.generation.queued',
}
"#,
        )
        .expect("fixture should write");
        fs::write(
            temp_dir.path().join("service.ts"),
            r#"
import { EventTopic } from './topics';
import { EventType } from './event-types';

export class ProducerService {
  async emitReportQueued(type: 'csv' | 'pdf') {
    const eventType =
      type === 'csv'
        ? EventType.CsvGenerationQueued
        : EventType.PdfGenerationQueued;

    await this.kafkaProducerService.sendMessage({
      topic: EventTopic.PlatformEvents,
      message: this.buildEventPayload(eventType),
    });
  }

  buildEventPayload(eventType: EventType) {
    return {
      eventType,
      eventBody: { id: '1' },
    };
  }
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "backend_standard",
            temp_dir.path(),
            &crate::FileEntry {
                path: "service.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        for event_qn in [
            "__event__kafka__csv.generation.queued",
            "__event__kafka__pdf.generation.queued",
        ] {
            let event_id = parsed
                .nodes
                .iter()
                .find(|n| {
                    matches!(n.kind, NodeKind::Event)
                        && n.qualified_name.as_deref() == Some(event_qn)
                })
                .map_or_else(
                    || panic!("helper-built payload must emit {event_qn}"),
                    |n| n.id,
                );
            assert!(
                parsed.edges.iter().any(|edge| {
                    edge.kind == gather_step_core::EdgeKind::ProducesEventFor
                        && edge.target == event_id
                }),
                "helper-built payload must produce ProducesEventFor edge to {event_qn}"
            );
        }
    }

    #[test]
    fn private_helper_built_send_message_payload_emits_all_possible_event_nodes() {
        let temp_dir = TestDir::new("send-message-private-helper-payload");
        fs::write(
            temp_dir.path().join("topics.ts"),
            r#"
export enum EventTopic {
  PlatformEvents = 'platform.events',
}
"#,
        )
        .expect("fixture should write");
        fs::write(
            temp_dir.path().join("event-types.ts"),
            r#"
export enum EventType {
  CsvGenerationQueued = 'csv.generation.queued',
  PdfGenerationQueued = 'pdf.generation.queued',
}
"#,
        )
        .expect("fixture should write");
        fs::write(
            temp_dir.path().join("service.ts"),
            r#"
import { EventTopic } from './topics';
import { EventType } from './event-types';

type Input = { kind: 'csv' | 'pdf' };

export class ProducerService {
  async emitReportQueued(input: Input) {
    const eventType =
      input.kind === 'csv'
        ? EventType.CsvGenerationQueued
        : EventType.PdfGenerationQueued;

    await this.kafkaProducerService.sendMessage({
      topic: EventTopic.PlatformEvents,
      message: this.#buildEventPayload(input, eventType),
    });
  }

  #buildEventPayload(input: Input, eventType: EventType) {
    return {
      eventType,
      eventBody: { kind: input.kind },
    };
  }
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "backend_standard",
            temp_dir.path(),
            &crate::FileEntry {
                path: "service.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        for event_qn in [
            "__event__kafka__csv.generation.queued",
            "__event__kafka__pdf.generation.queued",
        ] {
            assert!(
                parsed.nodes.iter().any(|n| {
                    matches!(n.kind, NodeKind::Event)
                        && n.qualified_name.as_deref() == Some(event_qn)
                }),
                "private helper-built payload must emit {event_qn}"
            );
        }
    }

    #[test]
    fn imported_helper_built_send_message_payload_emits_all_possible_event_nodes() {
        let temp_dir = TestDir::new("send-message-imported-helper-payload");
        fs::write(
            temp_dir.path().join("topics.ts"),
            r#"
export enum EventTopic {
  PlatformEvents = 'platform.events',
}
"#,
        )
        .expect("fixture should write");
        fs::write(
            temp_dir.path().join("event-types.ts"),
            r#"
export enum EventType {
  CsvGenerationQueued = 'csv.generation.queued',
  PdfGenerationQueued = 'pdf.generation.queued',
}
"#,
        )
        .expect("fixture should write");
        fs::write(
            temp_dir.path().join("payload.ts"),
            r#"
import { EventType } from './event-types';

export function buildEventPayload(input: unknown, eventType: EventType) {
  return {
    eventType,
    eventBody: input,
  };
}
"#,
        )
        .expect("fixture should write");
        fs::write(
            temp_dir.path().join("service.ts"),
            r#"
import { EventTopic } from './topics';
import { EventType } from './event-types';
import { buildEventPayload } from './payload';

export class ProducerService {
  async emitReportQueued(input: { fileType: 'csv' | 'pdf' }) {
    const eventType =
      input.fileType === 'csv'
        ? EventType.CsvGenerationQueued
        : EventType.PdfGenerationQueued;

    await this.kafkaProducerService.sendMessage({
      topic: EventTopic.PlatformEvents,
      message: buildEventPayload(input, eventType),
    });
  }
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "backend_standard",
            temp_dir.path(),
            &crate::FileEntry {
                path: "service.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        let producer_id = parsed
            .symbols
            .iter()
            .find(|symbol| symbol.node.name == "emitReportQueued")
            .map(|symbol| symbol.node.id)
            .expect("producer method should exist");

        for event_qn in [
            "__event__kafka__csv.generation.queued",
            "__event__kafka__pdf.generation.queued",
        ] {
            let event_id = parsed
                .nodes
                .iter()
                .find(|n| {
                    matches!(n.kind, NodeKind::Event)
                        && n.qualified_name.as_deref() == Some(event_qn)
                })
                .map_or_else(
                    || panic!("imported helper-built payload must emit {event_qn}"),
                    |n| n.id,
                );
            assert!(
                parsed.edges.iter().any(|edge| {
                    edge.kind == gather_step_core::EdgeKind::ProducesEventFor
                        && edge.source == producer_id
                        && edge.target == event_id
                }),
                "imported helper-built payload must produce ProducesEventFor edge to {event_qn}"
            );
        }
    }

    #[test]
    fn message_pattern_array_resolves_sibling_package_enum_from_built_root_manifest() {
        let temp_dir = TestDir::new("message-pattern-array-built-root");
        fs::create_dir_all(temp_dir.path().join("shared_contracts/src/kafka"))
            .expect("shared_contracts dir should exist");
        fs::create_dir_all(temp_dir.path().join("backend_standard/src"))
            .expect("backend_standard dir should exist");
        fs::write(
            temp_dir.path().join("shared_contracts/package.json"),
            r#"{ "name": "@repo/shared_contracts", "types": "index.d.ts", "main": "index.js" }"#,
        )
        .expect("shared_contracts package should write");
        fs::write(
            temp_dir.path().join("shared_contracts/index.d.ts"),
            "export {};\n",
        )
        .expect("shared_contracts built root types should write");
        fs::write(
            temp_dir.path().join("shared_contracts/src/kafka/enums.ts"),
            r#"
export enum EventTopic {
  BackendStandardEvents = 'backend-standard-events',
  PlatformEvents = 'platform-events',
}
"#,
        )
        .expect("shared_contracts enums should write");
        fs::write(
            temp_dir.path().join("backend_standard/package.json"),
            r#"{ "name": "@repo/backend_standard", "dependencies": { "@nestjs/cli": "^11.0.0" }, "scripts": { "build": "nest build" } }"#,
        )
        .expect("backend_standard package should write");
        fs::write(
            temp_dir.path().join("backend_standard/src/controller.ts"),
            r#"
import { Controller } from '@nestjs/common';
import { MessagePattern } from '@nestjs/microservices';
import { EventTopic } from '@repo/shared_contracts/kafka/enums';

@Controller()
export class EventHandlersController {
  @MessagePattern([EventTopic.BackendStandardEvents, EventTopic.PlatformEvents])
  async handleEvent(event: any) {
    return event;
  }
}
"#,
        )
        .expect("controller should write");

        let parsed = parse_file(
            "backend_standard",
            &temp_dir.path().join("backend_standard"),
            &crate::FileEntry {
                path: "src/controller.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        for event_qn in [
            "__event__kafka__backend-standard-events",
            "__event__kafka__platform-events",
        ] {
            assert!(
                parsed.nodes.iter().any(|n| {
                    matches!(n.kind, NodeKind::Event)
                        && n.qualified_name.as_deref() == Some(event_qn)
                }),
                "MessagePattern array must emit {event_qn}"
            );
        }
    }

    #[test]
    fn decorated_consumer_switch_resolves_enum_from_sibling_package_repo() {
        let temp_dir = TestDir::new("event-dispatch-switch-sibling-package");
        fs::create_dir_all(temp_dir.path().join("shared_contracts/src/kafka"))
            .expect("shared_contracts dir should exist");
        fs::create_dir_all(temp_dir.path().join("backend_standard/src"))
            .expect("backend_standard dir should exist");
        fs::write(
            temp_dir.path().join("shared_contracts/package.json"),
            r#"{ "name": "@repo/shared_contracts", "types": "src/index.ts" }"#,
        )
        .expect("shared_contracts package should write");
        fs::write(
            temp_dir.path().join("shared_contracts/src/kafka/enums.ts"),
            r#"
export enum EventType {
  DocumentQueued = 'document.queued',
}
"#,
        )
        .expect("shared_contracts enums should write");
        fs::write(
            temp_dir.path().join("backend_standard/package.json"),
            r#"{ "name": "@repo/backend_standard", "dependencies": { "@nestjs/common": "^11.0.0", "@nestjs/microservices": "^11.0.0" } }"#,
        )
        .expect("backend_standard package should write");
        fs::write(
            temp_dir.path().join("backend_standard/src/service.ts"),
            r#"
import { Injectable } from '@nestjs/common';
import { CustomEventPattern } from '@nestjs/microservices';
import { EventType } from '@repo/shared_contracts/kafka/enums';

@Injectable()
export class EventHandlersService {
  @CustomEventPattern('document-events')
  async handleEvent(event: any) {
    switch (event.eventType) {
      case EventType.DocumentQueued:
        return true;
      default:
        return false;
    }
  }
}
"#,
        )
        .expect("service should write");

        let parsed = parse_file(
            "backend_standard",
            &temp_dir.path().join("backend_standard"),
            &crate::FileEntry {
                path: "src/service.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        assert!(
            parsed.nodes.iter().any(|n| {
                matches!(n.kind, NodeKind::Event)
                    && n.qualified_name.as_deref() == Some("__event__kafka__document.queued")
            }),
            "decorated consumer switch must emit a fine-grained Event node when enum comes from a sibling package repo"
        );
    }

    // -----------------------------------------------------------------------
    // Generalised dispatcher detection: method name is irrelevant, decorator
    // + body heuristics (eventType switch/if) determine dispatcher status.
    // -----------------------------------------------------------------------

    #[test]
    fn process_event_dispatcher_with_decorator_emits_per_event_consumer_edges() {
        // A method named `processEvent` (not `handleEvent`) with a consumer
        // decorator and a `switch (event.eventType)` body must emit per-case
        // Event consumer edges, confirming the name gate is gone.
        let temp_dir = TestDir::new("dispatch-process-event");
        fs::write(
            temp_dir.path().join("event-types.ts"),
            r#"
export enum EventType {
  OrderCreated = 'order.created',
  OrderCancelled = 'order.cancelled',
}
"#,
        )
        .expect("fixture should write");
        fs::write(
            temp_dir.path().join("service.ts"),
            r#"
import { CustomEventPattern } from '@nestjs/microservices';
import { EventType } from './event-types';

export class OrderService {
  @CustomEventPattern('order-events')
  async processEvent(event: any) {
    switch (event.eventType) {
      case EventType.OrderCreated:
        return this.handleCreated(event);
      case EventType.OrderCancelled:
        return this.handleCancelled(event);
      default:
        return null;
    }
  }
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "backend_standard",
            temp_dir.path(),
            &crate::FileEntry {
                path: "service.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        for event_qn in [
            "__event__kafka__order.created",
            "__event__kafka__order.cancelled",
        ] {
            let event_id = parsed
                .nodes
                .iter()
                .find(|n| {
                    matches!(n.kind, NodeKind::Event)
                        && n.qualified_name.as_deref() == Some(event_qn)
                })
                .map_or_else(
                    || panic!("processEvent switch must emit Event node {event_qn}"),
                    |n| n.id,
                );
            assert!(
                parsed.edges.iter().any(|edge| {
                    edge.kind == gather_step_core::EdgeKind::Consumes && edge.target == event_id
                }),
                "processEvent switch must emit Consumes edge to {event_qn}"
            );
            assert!(
                parsed.edges.iter().any(|edge| {
                    edge.kind == gather_step_core::EdgeKind::UsesEventFrom
                        && edge.target == event_id
                }),
                "processEvent switch must emit UsesEventFrom edge to {event_qn}"
            );
        }
    }

    #[test]
    fn on_event_dispatcher_with_decorator_emits_per_event_consumer_edges() {
        // A method named `onEvent` with a consumer decorator and a
        // `switch (event.eventType)` body must emit per-case edges.
        let temp_dir = TestDir::new("dispatch-on-event");
        fs::write(
            temp_dir.path().join("event-types.ts"),
            r#"
export enum EventType {
  UserSignedUp = 'user.signed-up',
  UserDeleted = 'user.deleted',
}
"#,
        )
        .expect("fixture should write");
        fs::write(
            temp_dir.path().join("listener.ts"),
            r#"
import { MessagePattern } from '@nestjs/microservices';
import { EventType } from './event-types';

export class UserListener {
  @MessagePattern('user-events')
  async onEvent(event: any) {
    switch (event.eventType) {
      case EventType.UserSignedUp:
        return this.onSignedUp(event);
      case EventType.UserDeleted:
        return this.onDeleted(event);
      default:
        return null;
    }
  }
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "backend_standard",
            temp_dir.path(),
            &crate::FileEntry {
                path: "listener.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        for event_qn in [
            "__event__kafka__user.signed-up",
            "__event__kafka__user.deleted",
        ] {
            let event_id = parsed
                .nodes
                .iter()
                .find(|n| {
                    matches!(n.kind, NodeKind::Event)
                        && n.qualified_name.as_deref() == Some(event_qn)
                })
                .map_or_else(
                    || panic!("onEvent switch must emit Event node {event_qn}"),
                    |n| n.id,
                );
            assert!(
                parsed.edges.iter().any(|edge| {
                    edge.kind == gather_step_core::EdgeKind::UsesEventFrom
                        && edge.target == event_id
                }),
                "onEvent switch must emit UsesEventFrom edge to {event_qn}"
            );
        }
    }

    #[test]
    fn handle_message_conditional_dispatch_emits_per_event_consumer_edges() {
        // A method named `handleMessage` with a consumer decorator and an
        // `if (event.eventType === ...)` chain must emit per-branch edges.
        let temp_dir = TestDir::new("dispatch-handle-message-if");
        fs::write(
            temp_dir.path().join("handler.ts"),
            r#"
import { EventPattern } from '@nestjs/microservices';

export class NotificationHandler {
  @EventPattern('notification-events')
  async handleMessage(event: any) {
    if (event.eventType === 'notification.sent') {
      return this.onSent(event);
    } else if (event.eventType === 'notification.failed') {
      return this.onFailed(event);
    }
  }
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "backend_standard",
            temp_dir.path(),
            &crate::FileEntry {
                path: "handler.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        for event_qn in [
            "__event__kafka__notification.sent",
            "__event__kafka__notification.failed",
        ] {
            let event_id = parsed
                .nodes
                .iter()
                .find(|n| {
                    matches!(n.kind, NodeKind::Event)
                        && n.qualified_name.as_deref() == Some(event_qn)
                })
                .map_or_else(
                    || panic!("handleMessage conditional dispatch must emit Event node {event_qn}"),
                    |n| n.id,
                );
            assert!(
                parsed.edges.iter().any(|edge| {
                    edge.kind == gather_step_core::EdgeKind::UsesEventFrom
                        && edge.target == event_id
                }),
                "handleMessage conditional dispatch must emit UsesEventFrom edge to {event_qn}"
            );
        }
    }

    #[test]
    fn delegated_event_service_dispatch_emits_per_event_consumer_edges() {
        let temp_dir = TestDir::new("delegated-event-service-dispatch");
        fs::write(
            temp_dir.path().join("event-types.ts"),
            r#"
export enum EventType {
  ReportQueued = 'document.reg-genius-report-generation.queued',
}
"#,
        )
        .expect("event type fixture should write");
        fs::write(
            temp_dir.path().join("event-handlers.service.ts"),
            r#"
import { EventType } from './event-types';

export class EventHandlersService {
  async handleEvent(event: { eventType: EventType }) {
    switch (event.eventType) {
      case EventType.ReportQueued:
        return this.generalReportGenerationService.handleEvent(event);
      default:
        return undefined;
    }
  }
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "backend_standard",
            temp_dir.path(),
            &crate::FileEntry {
                path: "event-handlers.service.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        let event_qn = "__event__kafka__document.reg-genius-report-generation.queued";
        let event_id = parsed
            .nodes
            .iter()
            .find(|n| {
                matches!(n.kind, NodeKind::Event) && n.qualified_name.as_deref() == Some(event_qn)
            })
            .map_or_else(
                || panic!("delegated event service dispatch must emit Event node {event_qn}"),
                |n| n.id,
            );
        let service_dispatcher = parsed
            .nodes
            .iter()
            .find(|n| matches!(n.kind, NodeKind::Function) && n.name == "handleEvent")
            .expect("service dispatcher should parse as a function");

        assert!(
            parsed.edges.iter().any(|edge| {
                edge.kind == gather_step_core::EdgeKind::UsesEventFrom
                    && edge.source == service_dispatcher.id
                    && edge.target == event_id
            }),
            "delegated service dispatcher must emit UsesEventFrom edge to {event_qn}"
        );
    }

    #[test]
    fn process_event_without_event_type_switching_does_not_emit_event_consumer_edges() {
        // Negative case: a method named `processEvent` with a consumer
        // decorator but NO `event.eventType` switching must not emit any
        // per-event consumer edges. Guards against false positives from the
        // decorator gate alone.
        let temp_dir = TestDir::new("dispatch-no-switch");
        fs::write(
            temp_dir.path().join("service.ts"),
            r#"
import { CustomEventPattern } from '@nestjs/microservices';

export class SideEffectService {
  @CustomEventPattern('task-events')
  async processEvent(event: any) {
    this.logger.log('received event', event);
    await this.repository.save(event);
    return { acknowledged: true };
  }
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "backend_standard",
            temp_dir.path(),
            &crate::FileEntry {
                path: "service.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        // The decorator creates exactly one broad-topic Event node for
        // `task-events` and one `Consumes` + one `UsesEventFrom` edge pointing
        // to it. The body has NO `event.eventType` switching, so the dispatcher
        // path must not fire and no additional fine-grained Event nodes should
        // be present.
        let broad_topic_qn = "__event__kafka__task-events";
        let broad_topic_id = parsed
            .nodes
            .iter()
            .find(|n| {
                matches!(n.kind, NodeKind::Event)
                    && n.qualified_name.as_deref() == Some(broad_topic_qn)
            })
            .map(|n| n.id)
            .expect("decorator must emit the broad-topic Event node for task-events");

        // No Event node other than the broad topic should be present.
        let extra_event_nodes: Vec<_> = parsed
            .nodes
            .iter()
            .filter(|n| matches!(n.kind, NodeKind::Event) && n.id != broad_topic_id)
            .collect();
        assert!(
            extra_event_nodes.is_empty(),
            "processEvent without eventType switching must not emit per-event Event nodes beyond the broad topic; found: {extra_event_nodes:?}"
        );

        // No `UsesEventFrom` edge should target anything other than the broad topic.
        let extra_uses_event_from: Vec<_> = parsed
            .edges
            .iter()
            .filter(|e| {
                e.kind == gather_step_core::EdgeKind::UsesEventFrom && e.target != broad_topic_id
            })
            .collect();
        assert!(
            extra_uses_event_from.is_empty(),
            "processEvent without eventType switching must not emit extra UsesEventFrom edges beyond the broad topic; found: {extra_uses_event_from:?}"
        );
    }

    #[test]
    #[ignore = "requires GATHER_STEP_REAL_WORKSPACE to point at a real multi-repo workspace"]
    fn real_workspace_report_flow_emits_event_nodes() {
        let workspace = std::env::var("GATHER_STEP_REAL_WORKSPACE")
            .expect("GATHER_STEP_REAL_WORKSPACE must be set for the real-workspace parser probe");
        let workspace = PathBuf::from(workspace);

        let report_root = workspace.join("report");
        let report = parse_file(
            "report",
            &report_root,
            &crate::FileEntry {
                path: "src/workflows/shared/concerns/report-events.concern.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("report concern should parse");
        for event_qn in [
            "__event__kafka__platform-events",
            "__event__kafka__csv.generation.queued",
            "__event__kafka__pdf.generation.queued",
        ] {
            assert!(
                report.nodes.iter().any(|n| {
                    matches!(n.kind, NodeKind::Event)
                        && n.qualified_name.as_deref() == Some(event_qn)
                }),
                "real report concern should emit {event_qn}"
            );
        }
        let report_producer = report
            .nodes
            .iter()
            .find(|n| matches!(n.kind, NodeKind::Function) && n.name == "emitReportQueued")
            .expect("real report concern should include emitReportQueued");
        for event_qn in [
            "__event__kafka__csv.generation.queued",
            "__event__kafka__pdf.generation.queued",
        ] {
            let event_id = report
                .nodes
                .iter()
                .find(|n| {
                    matches!(n.kind, NodeKind::Event)
                        && n.qualified_name.as_deref() == Some(event_qn)
                })
                .map_or_else(
                    || panic!("real report concern should emit {event_qn}"),
                    |n| n.id,
                );
            assert!(
                report.edges.iter().any(|edge| {
                    edge.kind == gather_step_core::EdgeKind::ProducesEventFor
                        && edge.source == report_producer.id
                        && edge.target == event_id
                }),
                "real report concern should attach ProducesEventFor from emitReportQueued to {event_qn}"
            );
        }

        let document_root = workspace.join("backend_standard");
        let controller = parse_file(
            "backend_standard",
            &document_root,
            &crate::FileEntry {
                path: "src/event-handlers/event-handlers.controller.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("event-handlers.controller should parse");
        for event_qn in [
            "__event__kafka__backend-standard-events",
            "__event__kafka__platform-events",
        ] {
            assert!(
                controller.nodes.iter().any(|n| {
                    matches!(n.kind, NodeKind::Event)
                        && n.qualified_name.as_deref() == Some(event_qn)
                }),
                "real event controller should emit {event_qn}"
            );
        }

        let service = parse_file(
            "backend_standard",
            &document_root,
            &crate::FileEntry {
                path: "src/event-handlers/event-handlers.service.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("event-handlers.service should parse");
        for event_qn in [
            "__event__kafka__csv.generation.queued",
            "__event__kafka__pdf.generation.queued",
            "__event__kafka__document.report-generation.queued",
        ] {
            assert!(
                service.nodes.iter().any(|n| {
                    matches!(n.kind, NodeKind::Event)
                        && n.qualified_name.as_deref() == Some(event_qn)
                }),
                "real event service should emit {event_qn}"
            );
        }
        let service_dispatcher = service
            .nodes
            .iter()
            .find(|n| matches!(n.kind, NodeKind::Function) && n.name == "handleEvent")
            .expect("real event service should include handleEvent");
        for event_qn in [
            "__event__kafka__csv.generation.queued",
            "__event__kafka__pdf.generation.queued",
            "__event__kafka__document.report-generation.queued",
        ] {
            let event_id = service
                .nodes
                .iter()
                .find(|n| {
                    matches!(n.kind, NodeKind::Event)
                        && n.qualified_name.as_deref() == Some(event_qn)
                })
                .map_or_else(
                    || panic!("real event service should emit {event_qn}"),
                    |n| n.id,
                );
            assert!(
                service.edges.iter().any(|edge| {
                    edge.kind == gather_step_core::EdgeKind::UsesEventFrom
                        && edge.source == service_dispatcher.id
                        && edge.target == event_id
                }),
                "real event service should attach UsesEventFrom from handleEvent to {event_qn}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Pattern (a): barrel re-exports through workspace-package specifiers
    // -----------------------------------------------------------------------

    #[test]
    fn producer_resolves_event_type_through_workspace_package_barrel_reexport() {
        // Simulates:
        //   shared_contracts/src/kafka/event-types.ts  →  enum EventType { Queued = 'item.queued' }
        //   shared_contracts/package.json              →  { "name": "@repo/shared_contracts", "types": "src/index.ts" }
        //   backend_standard/src/event-barrel.ts       →  export { EventType } from '@repo/shared_contracts'
        //   backend_standard/src/service.ts            →  import { EventType } from './event-barrel'
        //                                                  this.client.emit(EventType.Queued, payload)
        //
        // The producer service imports `EventType` from a local barrel that
        // re-exports via a workspace-package specifier.  `barrel_hop_lookup`
        // must follow the non-relative specifier using sibling-package
        // discovery, which is the gap addressed by pattern (a).
        let temp_dir = TestDir::new("producer-workspace-barrel");
        // shared_contracts sibling package
        fs::create_dir_all(temp_dir.path().join("shared_contracts/src/kafka"))
            .expect("shared_contracts kafka dir should exist");
        fs::write(
            temp_dir.path().join("shared_contracts/package.json"),
            r#"{ "name": "@repo/shared_contracts", "types": "src/index.ts" }"#,
        )
        .expect("shared_contracts package.json should write");
        fs::write(
            temp_dir.path().join("shared_contracts/src/index.ts"),
            r#"
export enum EventType {
    Queued = 'item.queued',
}
"#,
        )
        .expect("shared_contracts index.ts should write");
        // Consumer (producer) service
        fs::create_dir_all(temp_dir.path().join("backend_standard/src"))
            .expect("backend_standard src dir should exist");
        fs::write(
            temp_dir.path().join("backend_standard/package.json"),
            r#"{ "name": "@repo/backend_standard", "dependencies": { "@nestjs/common": "^11.0.0" } }"#,
        )
        .expect("backend_standard package.json should write");
        // Local barrel that re-exports EventType from the workspace package
        // using a non-relative specifier (pattern a).
        fs::write(
            temp_dir.path().join("backend_standard/src/event-barrel.ts"),
            r#"export { EventType } from '@repo/shared_contracts';"#,
        )
        .expect("event-barrel.ts should write");
        fs::write(
            temp_dir.path().join("backend_standard/src/service.ts"),
            r#"
import { EventType } from './event-barrel';

export class ItemService {
    constructor(private readonly client: ClientKafka) {}
    queue() {
        this.client.emit(EventType.Queued, { id: 1 });
    }
}
"#,
        )
        .expect("service.ts should write");

        let parsed = parse_file(
            "backend_standard",
            &temp_dir.path().join("backend_standard"),
            &crate::FileEntry {
                path: "src/service.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        assert!(
            parsed.nodes.iter().any(|n| {
                matches!(n.kind, NodeKind::Event)
                    && n.qualified_name
                        .as_deref()
                        .unwrap_or("")
                        .contains("item.queued")
            }),
            "producer must resolve EventType.Queued through a local barrel that re-exports from a workspace package"
        );
    }

    // -----------------------------------------------------------------------
    // Pattern (b): namespace imports (`import * as Ns from '...'`)
    // -----------------------------------------------------------------------

    #[test]
    fn producer_resolves_topic_through_namespace_import() {
        // `import * as Events from './events'`
        // `this.client.emit(Events.EventType.Queued, payload)`
        let temp_dir = TestDir::new("producer-namespace-import");
        fs::write(
            temp_dir.path().join("events.ts"),
            r#"
export enum EventType {
    Queued = 'order.queued',
}
"#,
        )
        .expect("events.ts should write");
        fs::write(
            temp_dir.path().join("service.ts"),
            r#"
import * as Events from './events';

export class OrderService {
    constructor(private readonly client: ClientKafka) {}
    place() {
        this.client.emit(Events.EventType.Queued, { id: 1 });
    }
}
"#,
        )
        .expect("service.ts should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "service.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        assert!(
            parsed.nodes.iter().any(|n| {
                matches!(n.kind, NodeKind::Event)
                    && n.qualified_name
                        .as_deref()
                        .unwrap_or("")
                        .contains("order.queued")
            }),
            "producer must resolve topic through a namespace (star) import"
        );
    }

    // -----------------------------------------------------------------------
    // Pattern (c): static template literals with `${CONST}` interpolations
    // -----------------------------------------------------------------------

    #[test]
    fn producer_resolves_static_template_literal_with_const_interpolation() {
        // `` `${PREFIX}.queued` `` where `PREFIX` is a local const.
        let parsed = parse_nestjs_fixture(
            r#"
            const PREFIX = 'document';

            export class DocService {
                constructor(private readonly client: ClientKafka) {}
                queue() {
                    this.client.emit(`${PREFIX}.queued`, { id: 1 });
                }
            }
            "#,
        );

        assert!(
            parsed.nodes.iter().any(|n| {
                matches!(n.kind, NodeKind::Event)
                    && n.qualified_name
                        .as_deref()
                        .unwrap_or("")
                        .contains("document.queued")
            }),
            "producer must resolve static template literal with const interpolation"
        );
    }

    // -----------------------------------------------------------------------
    // Pattern (d): helper-payload event type argument with member chain
    // -----------------------------------------------------------------------

    #[test]
    fn producer_resolves_event_type_from_helper_payload_with_member_chain() {
        // `sendMessage(this.#buildPayload(EventType.Csv))`
        // where `#buildPayload(type)` returns `{ eventType: type, ... }`.
        let temp_dir = TestDir::new("producer-helper-payload-chain");
        fs::write(
            temp_dir.path().join("event-type.ts"),
            r#"
export enum EventType {
    Csv = 'csv.generation.queued',
    Pdf = 'pdf.generation.queued',
}
"#,
        )
        .expect("event-type.ts should write");
        fs::write(
            temp_dir.path().join("service.ts"),
            r#"
import { EventType } from './event-type';

export class ReportService {
    constructor(private readonly client: ClientKafka) {}

    generate(type: EventType) {
        this.client.sendMessage(this.#buildPayload(type));
    }

    #buildPayload(eventType: EventType) {
        return { eventType, topic: 'reports' };
    }
}
"#,
        )
        .expect("service.ts should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "service.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        // The helper-payload path resolves `eventType` from the helper's
        // return object to the caller's argument.  Since `type` is a
        // parameter (not a literal), this is a dynamic value — the extractor
        // should not fabricate a topic.  This test documents the current
        // boundary: the helper pattern is detected and the argument is
        // forwarded, but a bare parameter cannot be resolved further without
        // type information.  No spurious event node should be produced.
        let has_spurious = parsed.nodes.iter().any(|n| {
            matches!(n.kind, NodeKind::Event)
                && n.qualified_name
                    .as_deref()
                    .unwrap_or("")
                    .contains("generation.queued")
        });
        // When `type` is statically known (e.g. EventType.Csv passed directly),
        // the extractor can resolve it.  With a bare parameter variable it
        // should silently skip rather than crash.
        assert!(
            !has_spurious,
            "extractor must not fabricate a topic from a bare parameter variable"
        );
    }
}
