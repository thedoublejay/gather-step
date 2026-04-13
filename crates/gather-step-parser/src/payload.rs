use std::{fs, path::Path};

use gather_step_core::{
    EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, PayloadContractDoc,
    PayloadContractRecord, PayloadField, PayloadInferenceKind, PayloadSide,
    payload_contract_external_id, payload_contract_node_id, ref_node_id,
};

use crate::frameworks::nestjs::{
    extract_call_argument, producer_messaging_operation, resolve_producer_topic_name,
    resolve_topic_decorator_argument,
};
use crate::path_guard::canonicalize_existing_file_under;
use crate::traverse::classify_language;
use crate::tree_sitter::{ParsedFile, SymbolCapture};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InferredPayloadContract {
    pub node: NodeData,
    pub edge: EdgeData,
    pub record: PayloadContractRecord,
}

pub fn infer_payload_contracts(parsed: &ParsedFile) -> Vec<InferredPayloadContract> {
    let mut inferred = Vec::new();

    for call_site in &parsed.call_sites {
        let Some((target, target_kind, target_qualified_name)) = producer_target(parsed, call_site)
        else {
            continue;
        };
        let Some(raw_args) = call_site.raw_arguments.as_deref() else {
            continue;
        };
        let Some(payload_expr) = extract_payload_argument(raw_args) else {
            continue;
        };
        let Some(fields) = parse_object_literal_fields(payload_expr) else {
            continue;
        };
        if fields.is_empty() {
            continue;
        }

        let external_id = payload_contract_external_id(
            &parsed.file_node.repo,
            &parsed.file_node.file_path,
            target,
            call_site.owner_id,
            PayloadSide::Producer,
        );
        let contract = PayloadContractDoc {
            content_type: "application/json".to_owned(),
            schema_format: "normalized_object".to_owned(),
            side: PayloadSide::Producer,
            inference_kind: PayloadInferenceKind::LiteralObject,
            confidence: 950,
            fields: fields.clone(),
            source_type_name: None,
        };
        inferred.push(InferredPayloadContract {
            node: payload_contract_node(
                parsed,
                &external_id,
                call_site.span.as_ref().map(|s| s.line_start),
            ),
            edge: EdgeData {
                source: payload_contract_node_id(&external_id),
                target,
                kind: EdgeKind::ContractOn,
                metadata: EdgeMetadata {
                    confidence: Some(950),
                    ..EdgeMetadata::default()
                },
                owner_file: parsed.file_node.id,
                is_cross_file: false,
            },
            record: PayloadContractRecord {
                payload_contract_node_id: payload_contract_node_id(&external_id),
                contract_target_node_id: target,
                contract_target_kind: target_kind,
                contract_target_qualified_name: Some(target_qualified_name),
                repo: parsed.file_node.repo.clone(),
                file_path: parsed.file_node.file_path.clone(),
                source_symbol_node_id: call_site.owner_id,
                line_start: call_site.span.as_ref().map(|span| span.line_start),
                side: PayloadSide::Producer,
                inference_kind: PayloadInferenceKind::LiteralObject,
                confidence: 950,
                source_type_name: None,
                contract,
            },
        });
    }

    let source = &*parsed.source;

    for symbol in &parsed.symbols {
        let Some((target, target_kind, target_qualified_name)) = consumer_target(parsed, symbol)
        else {
            continue;
        };
        let Some(signature) = symbol.node.signature.as_deref() else {
            continue;
        };
        let Some((source_type_name, fields)) = infer_consumer_fields(parsed, signature, source)
        else {
            continue;
        };
        if fields.is_empty() {
            continue;
        }

        let external_id = payload_contract_external_id(
            &parsed.file_node.repo,
            &parsed.file_node.file_path,
            target,
            symbol.node.id,
            PayloadSide::Consumer,
        );
        let contract = PayloadContractDoc {
            content_type: "application/json".to_owned(),
            schema_format: "normalized_object".to_owned(),
            side: PayloadSide::Consumer,
            inference_kind: PayloadInferenceKind::TypedParameter,
            confidence: 900,
            fields: fields.clone(),
            source_type_name: source_type_name.clone(),
        };
        inferred.push(InferredPayloadContract {
            node: payload_contract_node(
                parsed,
                &external_id,
                symbol.node.span.as_ref().map(|s| s.line_start),
            ),
            edge: EdgeData {
                source: payload_contract_node_id(&external_id),
                target,
                kind: EdgeKind::ContractOn,
                metadata: EdgeMetadata {
                    confidence: Some(900),
                    ..EdgeMetadata::default()
                },
                owner_file: parsed.file_node.id,
                is_cross_file: false,
            },
            record: PayloadContractRecord {
                payload_contract_node_id: payload_contract_node_id(&external_id),
                contract_target_node_id: target,
                contract_target_kind: target_kind,
                contract_target_qualified_name: Some(target_qualified_name),
                repo: parsed.file_node.repo.clone(),
                file_path: parsed.file_node.file_path.clone(),
                source_symbol_node_id: symbol.node.id,
                line_start: symbol.node.span.as_ref().map(|span| span.line_start),
                side: PayloadSide::Consumer,
                inference_kind: PayloadInferenceKind::TypedParameter,
                confidence: 900,
                source_type_name,
                contract,
            },
        });
    }

    inferred
}

fn payload_contract_node(
    parsed: &ParsedFile,
    external_id: &str,
    line_start: Option<u32>,
) -> NodeData {
    NodeData {
        id: payload_contract_node_id(external_id),
        kind: NodeKind::PayloadContract,
        repo: parsed.file_node.repo.clone(),
        file_path: parsed.file_node.file_path.clone(),
        name: external_id.to_owned(),
        qualified_name: Some(external_id.to_owned()),
        external_id: Some(external_id.to_owned()),
        signature: None,
        visibility: None,
        span: line_start.map(|line_start| gather_step_core::SourceSpan {
            line_start,
            line_len: 0,
            column_start: 0,
            column_len: 0,
        }),
        is_virtual: true,
    }
}

fn producer_target(
    parsed: &ParsedFile,
    call_site: &crate::tree_sitter::EnrichedCallSite,
) -> Option<(gather_step_core::NodeId, NodeKind, String)> {
    let (_, kind) = producer_messaging_operation(call_site)
        .or_else(|| payload_messaging_operation(call_site))?;
    // `producer_messaging_operation` / `payload_messaging_operation` always
    // return `NodeKind::Event` (canonical messaging identity). Keep the kind
    // in scope for the return tuple but use the canonical `__event__…`
    // prefix unconditionally.
    let topic_name = resolve_producer_topic_name(parsed, call_site)?;
    let transport = call_site
        .callee_qualified_hint
        .as_deref()
        .map_or("kafka", detect_transport);
    let qualified_name = format!("__event__{transport}__{topic_name}");
    Some((ref_node_id(kind, &qualified_name), kind, qualified_name))
}

fn payload_messaging_operation(
    call_site: &crate::tree_sitter::EnrichedCallSite,
) -> Option<(&str, NodeKind)> {
    let hint = call_site.callee_qualified_hint.as_deref()?;
    let operation = hint.rsplit('.').next().unwrap_or(hint);
    if operation.eq_ignore_ascii_case("emit") || operation.eq_ignore_ascii_case("send") {
        // Align with `producer_messaging_operation`: payload contracts for
        // messaging producers target the canonical `NodeKind::Event` virtual
        // node so producer and consumer contracts converge on a single
        // identity per topic.
        Some((hint, NodeKind::Event))
    } else {
        None
    }
}

fn consumer_target(
    parsed: &ParsedFile,
    symbol: &SymbolCapture,
) -> Option<(gather_step_core::NodeId, NodeKind, String)> {
    let decorator = symbol.decorators.iter().find(|decorator| {
        matches!(
            decorator.name.as_str(),
            "MessagePattern" | "EventPattern" | "CustomEventPattern"
        )
    })?;
    let name = decorator
        .arguments
        .first()
        .and_then(|raw| {
            resolve_topic_decorator_argument(parsed, raw).or_else(|| quoted_literal_topic_name(raw))
        })
        .or_else(|| {
            first_decorator_argument(&decorator.raw).and_then(|raw| {
                resolve_topic_decorator_argument(parsed, &raw)
                    .or_else(|| quoted_literal_topic_name(&raw))
            })
        })?;
    if name.is_empty() {
        return None;
    }
    // All messaging decorators converge on the canonical `NodeKind::Event`
    // virtual node; producer-side `producer_target` uses the same canonical
    // form.
    let _ = decorator.name.as_str();
    let kind = NodeKind::Event;
    let qn = format!("__event__kafka__{name}");
    Some((ref_node_id(kind, &qn), kind, qn))
}

/// Extract the first comma-separated argument from `raw`.
///
/// `raw` is now the argument expression only — the content inside the
/// decorator's outermost parens — so we no longer need to strip the
/// `@Name(…)` wrapper.  We use the existing `extract_call_argument` helper
/// by wrapping `raw` back in parens so the helper's expected format is
/// satisfied.
fn first_decorator_argument(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    // `extract_call_argument` expects `(…)` around the arguments.
    let wrapped = format!("({trimmed})");
    extract_call_argument(&wrapped, 0).map(ToOwned::to_owned)
}

fn quoted_literal_topic_name(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    let is_quoted_literal = (trimmed.starts_with('\'') && trimmed.ends_with('\''))
        || (trimmed.starts_with('"') && trimmed.ends_with('"'))
        || (trimmed.starts_with('`') && trimmed.ends_with('`') && !trimmed.contains("${"));
    if !is_quoted_literal {
        return None;
    }
    Some(
        trimmed
            .trim_matches('\'')
            .trim_matches('"')
            .trim_matches('`')
            .trim()
            .to_owned(),
    )
}

fn extract_payload_argument(raw_arguments: &str) -> Option<&str> {
    extract_call_argument(raw_arguments, 1)
}

fn parse_object_literal_fields(raw: &str) -> Option<Vec<PayloadField>> {
    let trimmed = raw.trim();
    if !(trimmed.starts_with('{') && trimmed.ends_with('}')) {
        return None;
    }
    let body = &trimmed[1..trimmed.len() - 1];
    let mut fields = Vec::new();
    for entry in split_top_level(body, ',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let Some((name, value)) = entry.split_once(':') else {
            continue;
        };
        fields.push(PayloadField {
            name: name.trim().trim_matches('"').trim_matches('\'').to_owned(),
            type_name: infer_literal_type(value.trim()),
            optional: false,
            confidence: 950,
        });
    }
    Some(fields)
}

fn infer_consumer_fields(
    parsed: &ParsedFile,
    signature: &str,
    source: &str,
) -> Option<(Option<String>, Vec<PayloadField>)> {
    let open = signature.find('(')?;
    let close = find_matching_close(signature, open)?;
    let params = &signature[open + 1..close];
    let first = split_top_level(params, ',')
        .into_iter()
        .next()?
        .trim()
        .to_owned();
    let (_, type_expr) = first.split_once(':')?;
    let type_expr = type_expr.trim();
    if let Some(fields) = parse_inline_type_literal(type_expr) {
        return Some((None, fields));
    }
    let type_name = type_expr
        .trim_start_matches("Promise<")
        .trim_end_matches('>')
        .trim()
        .trim_matches('?')
        .to_owned();
    let fields = extract_named_type_fields(parsed, source, &type_name)?;
    Some((Some(type_name), fields))
}

fn parse_inline_type_literal(type_expr: &str) -> Option<Vec<PayloadField>> {
    parse_type_literal_fields(type_expr)
}

fn extract_named_type_fields(
    parsed: &ParsedFile,
    source: &str,
    type_name: &str,
) -> Option<Vec<PayloadField>> {
    extract_local_named_type_fields(source, type_name)
        .or_else(|| extract_imported_type_fields(parsed, type_name))
}

fn extract_local_named_type_fields(source: &str, type_name: &str) -> Option<Vec<PayloadField>> {
    let interface_marker = format!("interface {type_name}");
    if let Some(index) = source.find(&interface_marker) {
        let rest = &source[index + interface_marker.len()..];
        // Verify the match is a whole-word boundary (next char must be whitespace, '{', or '<')
        if rest.starts_with(|ch: char| ch.is_whitespace() || ch == '{' || ch == '<')
            && let Some(block) = extract_braced_block(rest)
        {
            return parse_type_literal_fields(block);
        }
    }
    let type_marker = format!("type {type_name} =");
    if let Some(index) = source.find(&type_marker) {
        let rest = &source[index + type_marker.len()..];
        if let Some(block) = extract_braced_block(rest) {
            return parse_type_literal_fields(block);
        }
    }
    extract_class_fields(source, type_name)
}

fn extract_braced_block(input: &str) -> Option<&str> {
    let start = input.find('{')?;
    let mut depth = 0_i32;
    for (offset, ch) in input[start..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&input[start..=start + offset]);
                }
            }
            _ => {}
        }
    }
    None
}

fn extract_class_fields(source: &str, type_name: &str) -> Option<Vec<PayloadField>> {
    let class_marker = format!("class {type_name}");
    let index = source.find(&class_marker)?;
    let rest = &source[index + class_marker.len()..];
    if !rest.starts_with(|ch: char| ch.is_whitespace() || ch == '{' || ch == '<') {
        return None;
    }
    let block = extract_braced_block(rest)?;
    parse_class_fields(block)
}

fn extract_imported_type_fields(parsed: &ParsedFile, type_name: &str) -> Option<Vec<PayloadField>> {
    let imported = resolve_imported_type_source(parsed, type_name)?;
    extract_local_named_type_fields(imported.parsed.source.as_ref(), &imported.imported_name)
}

fn resolve_imported_type_source(
    parsed: &ParsedFile,
    type_name: &str,
) -> Option<ParsedImportedType> {
    let binding = parsed.import_bindings.iter().find(|binding| {
        binding.local_name == type_name && binding.resolved_path.is_some() && !binding.is_namespace
    })?;
    let imported_name = binding.imported_name.as_deref().unwrap_or(type_name);
    let path = binding.resolved_path.as_ref()?;
    let imported = parse_imported_file(parsed, path)?;
    Some(ParsedImportedType {
        imported_name: imported_name.to_owned(),
        parsed: imported,
    })
}

struct ParsedImportedType {
    imported_name: String,
    parsed: ParsedFile,
}

fn parse_imported_file(parsed: &ParsedFile, path: &Path) -> Option<ParsedFile> {
    let repo_root = fs::canonicalize(repo_root_for(parsed)).ok()?;
    let safe_path = canonicalize_existing_file_under(path, &repo_root)?;
    let relative = safe_path.strip_prefix(&repo_root).ok()?;
    let language = classify_language(relative)?;
    let metadata = fs::symlink_metadata(&safe_path).ok()?;
    if metadata.len() > crate::TraverseConfig::default().max_file_size_bytes() {
        return None;
    }
    crate::tree_sitter::parse_file(
        parsed.file_node.repo.as_str(),
        &repo_root,
        &crate::FileEntry {
            path: relative.to_path_buf(),
            language,
            size_bytes: metadata.len(),
            content_hash: [0; 32],
            source_bytes: None,
        },
    )
    .ok()
}

fn repo_root_for(parsed: &ParsedFile) -> std::path::PathBuf {
    let mut root = parsed.source_path.clone();
    for _ in parsed.file.path.components() {
        root.pop();
    }
    root
}

fn find_matching_close(input: &str, open_pos: usize) -> Option<usize> {
    let mut depth = 0_i32;
    for (offset, ch) in input[open_pos..].char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(open_pos + offset);
                }
            }
            _ => {}
        }
    }
    None
}

fn parse_type_literal_fields(raw: &str) -> Option<Vec<PayloadField>> {
    let trimmed = raw.trim();
    if !(trimmed.starts_with('{') && trimmed.ends_with('}')) {
        return None;
    }
    let body = &trimmed[1..trimmed.len() - 1];
    let mut fields = Vec::new();
    for entry in body.lines().flat_map(|line| line.split(';')) {
        let entry = entry.trim().trim_end_matches(',');
        if entry.is_empty() {
            continue;
        }
        let Some((name_part, type_part)) = entry.split_once(':') else {
            continue;
        };
        let name = name_part.trim();
        fields.push(PayloadField {
            name: name.trim_end_matches('?').to_owned(),
            type_name: type_part.trim().to_owned(),
            optional: name.ends_with('?'),
            confidence: 900,
        });
    }
    Some(fields)
}

fn parse_class_fields(raw: &str) -> Option<Vec<PayloadField>> {
    let trimmed = raw.trim();
    if !(trimmed.starts_with('{') && trimmed.ends_with('}')) {
        return None;
    }
    let body = &trimmed[1..trimmed.len() - 1];
    let mut fields = Vec::new();
    for entry in body.lines().flat_map(|line| line.split(';')) {
        let entry = entry.trim().trim_end_matches(',');
        if entry.is_empty()
            || entry.starts_with('@')
            || entry.contains('(')
            || entry.starts_with("constructor")
            || entry.starts_with("get ")
            || entry.starts_with("set ")
        {
            continue;
        }
        let entry = strip_access_modifiers(entry);
        let Some((name_part, type_part)) = entry.split_once(':') else {
            continue;
        };
        let name = name_part
            .split('=')
            .next()
            .unwrap_or(name_part)
            .trim()
            .trim_end_matches('!')
            .trim();
        if name.is_empty() {
            continue;
        }
        fields.push(PayloadField {
            name: name.trim_end_matches('?').to_owned(),
            type_name: type_part
                .split('=')
                .next()
                .unwrap_or(type_part)
                .trim()
                .to_owned(),
            optional: name.ends_with('?'),
            confidence: 900,
        });
    }
    Some(fields)
}

fn strip_access_modifiers(entry: &str) -> &str {
    let mut rest = entry.trim();
    loop {
        let next = rest
            .strip_prefix("public ")
            .or_else(|| rest.strip_prefix("private "))
            .or_else(|| rest.strip_prefix("protected "))
            .or_else(|| rest.strip_prefix("readonly "))
            .or_else(|| rest.strip_prefix("declare "));
        let Some(candidate) = next else {
            break;
        };
        rest = candidate.trim_start();
    }
    rest
}

fn infer_literal_type(value: &str) -> String {
    let value = value.trim();
    if value.starts_with('"') || value.starts_with('\'') || value.starts_with('`') {
        "string".to_owned()
    } else if value == "true" || value == "false" {
        "boolean".to_owned()
    } else if value.starts_with('{') {
        "object".to_owned()
    } else if value.starts_with('[') {
        "array".to_owned()
    } else if value.parse::<f64>().is_ok() {
        "number".to_owned()
    } else {
        "unknown".to_owned()
    }
}

fn detect_transport(qualified_hint: &str) -> &'static str {
    let receiver = qualified_hint
        .rsplit_once('.')
        .map_or(qualified_hint, |(recv, _)| recv);
    let mut lower = receiver.to_owned();
    lower.make_ascii_lowercase();
    if lower.contains("servicebus") {
        "servicebus"
    } else if lower.contains("pubsub") {
        "pubsub"
    } else {
        "kafka"
    }
}

fn split_top_level(input: &str, separator: char) -> Vec<&str> {
    let mut result = Vec::new();
    let mut start = 0_usize;
    let mut braces = 0_i32;
    let mut brackets = 0_i32;
    let mut parens = 0_i32;
    let mut in_string: Option<char> = None;
    for (offset, ch) in input.char_indices() {
        match in_string {
            Some(quote) if ch == quote => in_string = None,
            Some(_) => {}
            None => match ch {
                '"' | '\'' | '`' => in_string = Some(ch),
                '{' => braces += 1,
                '}' => braces -= 1,
                '[' => brackets += 1,
                ']' => brackets -= 1,
                '(' => parens += 1,
                ')' => parens -= 1,
                _ if ch == separator && braces == 0 && brackets == 0 && parens == 0 => {
                    result.push(input[start..offset].trim());
                    start = offset + ch.len_utf8();
                }
                _ => {}
            },
        }
    }
    if start < input.len() {
        result.push(input[start..].trim());
    }
    result
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::{
        env,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::PayloadSide;
    use pretty_assertions::assert_eq;

    use crate::{
        frameworks::Framework,
        payload::infer_payload_contracts,
        traverse::{FileEntry, Language},
        tree_sitter::parse_file_with_frameworks,
    };

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(name: &str) -> Self {
            let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path =
                env::temp_dir().join(format!("gather-step-payload-{name}-{}-{id}", process::id()));
            fs::create_dir_all(&path).expect("temp dir");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn infers_literal_object_and_typed_consumer_contracts() {
        let temp = TempDir::new("contracts");
        let repo_root = temp.path();
        let source = r"
import { EventPattern } from '@nestjs/microservices';

type OrderCreatedDto = {
  orderId: string;
  severity?: number;
};

export class Orders {
  publish(client: any) {
    return client.send('order.created', { orderId: '123', status: 'active' });
  }

  @EventPattern('order.created')
  handle(data: OrderCreatedDto) {
    return data.orderId;
  }
}
";
        fs::write(repo_root.join("orders.ts"), source).expect("fixture");
        let file = FileEntry {
            path: Path::new("orders.ts").to_path_buf(),
            language: Language::TypeScript,
            size_bytes: u64::try_from(source.len()).unwrap_or(u64::MAX),
            content_hash: *blake3::hash(source.as_bytes()).as_bytes(),
            source_bytes: None,
        };
        let parsed =
            parse_file_with_frameworks("backend_standard", repo_root, &file, &[Framework::NestJs])
                .expect("parse");

        let inferred = infer_payload_contracts(&parsed);
        assert_eq!(inferred.len(), 2);
        let producer = inferred
            .iter()
            .find(|item| item.record.side == PayloadSide::Producer)
            .expect("producer");
        assert_eq!(producer.record.contract.fields[0].name, "orderId");
        assert_eq!(producer.record.contract.fields[0].type_name, "string");
        let consumer = inferred
            .iter()
            .find(|item| item.record.side == PayloadSide::Consumer)
            .expect("consumer");
        assert_eq!(
            consumer.record.source_type_name.as_deref(),
            Some("OrderCreatedDto")
        );
        assert_eq!(consumer.record.contract.fields[1].name, "severity");
        assert!(consumer.record.contract.fields[1].optional);
    }

    #[test]
    fn infers_consumer_contracts_from_same_resolver_as_nestjs_decorators() {
        let temp = TempDir::new("consumer-topic-const");
        let repo_root = temp.path();
        let source = r"
import { EventPattern } from '@nestjs/microservices';

const Topics = {
  events: {
    created: 'order.created'
  }
};

type OrderCreatedDto = {
  orderId: string;
};

export class Orders {
  @EventPattern(Topics.events.created)
  handle(data: OrderCreatedDto) {
    return data.orderId;
  }
}
";
        fs::write(repo_root.join("orders.ts"), source).expect("fixture");
        let file = FileEntry {
            path: Path::new("orders.ts").to_path_buf(),
            language: Language::TypeScript,
            size_bytes: u64::try_from(source.len()).unwrap_or(u64::MAX),
            content_hash: *blake3::hash(source.as_bytes()).as_bytes(),
            source_bytes: None,
        };
        let parsed =
            parse_file_with_frameworks("backend_standard", repo_root, &file, &[Framework::NestJs])
                .expect("parse");

        let inferred = infer_payload_contracts(&parsed);
        let consumer = inferred
            .iter()
            .find(|item| item.record.side == PayloadSide::Consumer)
            .expect("consumer");
        assert_eq!(
            consumer.record.contract_target_qualified_name.as_deref(),
            Some("__event__kafka__order.created")
        );
    }

    #[test]
    fn infers_imported_producer_and_consumer_contracts() {
        let temp = TempDir::new("imported-contracts");
        let repo_root = temp.path();
        fs::write(
            repo_root.join("topics.ts"),
            r"
export const Topics = {
  requests: {
    created: 'order.created'
  }
};
",
        )
        .expect("fixture");
        fs::write(
            repo_root.join("dto.ts"),
            r"
export class ImportedOrderDto {
  readonly orderId!: string;
  status?: 'active' | 'pending';
}
",
        )
        .expect("fixture");
        let source = r"
import { EventPattern } from '@nestjs/microservices';
import { Topics } from './topics';
import type { ImportedOrderDto } from './dto';

export class Orders {
  publish(client: any) {
    return this.client.send(Topics.requests.created, { orderId: '123', status: 'active' });
  }

  @EventPattern(Topics.requests.created)
  handle(data: ImportedOrderDto) {
    return data.orderId;
  }
}
";
        fs::write(repo_root.join("orders.ts"), source).expect("fixture");
        let file = FileEntry {
            path: Path::new("orders.ts").to_path_buf(),
            language: Language::TypeScript,
            size_bytes: u64::try_from(source.len()).unwrap_or(u64::MAX),
            content_hash: *blake3::hash(source.as_bytes()).as_bytes(),
            source_bytes: None,
        };
        let parsed =
            parse_file_with_frameworks("backend_standard", repo_root, &file, &[Framework::NestJs])
                .expect("parse");

        let inferred = infer_payload_contracts(&parsed);
        let producer = inferred
            .iter()
            .find(|item| item.record.side == PayloadSide::Producer)
            .expect("producer");
        // Producer `client.send(...)` contracts target the canonical
        // `NodeKind::Event` node with the `__event__kafka__` prefix, so
        // producer and consumer contracts for the same topic converge.
        assert_eq!(
            producer.record.contract_target_qualified_name.as_deref(),
            Some("__event__kafka__order.created")
        );
        let consumer = inferred
            .iter()
            .find(|item| item.record.side == PayloadSide::Consumer)
            .expect("consumer");
        assert_eq!(
            consumer.record.source_type_name.as_deref(),
            Some("ImportedOrderDto")
        );
        assert_eq!(consumer.record.contract.fields[0].name, "orderId");
        assert_eq!(consumer.record.contract.fields[0].type_name, "string");
        assert_eq!(consumer.record.contract.fields[1].name, "status");
        assert!(consumer.record.contract.fields[1].optional);
    }

    #[test]
    fn infers_consumer_contracts_from_logical_or_fallback_topic_expression() {
        let temp = TempDir::new("consumer-topic-fallback");
        let repo_root = temp.path();
        fs::write(
            repo_root.join("topics.ts"),
            r"
export enum EventTopic {
  Platform = 'platform.lifecycle'
}
",
        )
        .expect("fixture");
        let source = r"
import { EventPattern } from '@nestjs/microservices';
import { EventTopic } from './topics';

type PlatformEventDto = {
  eventId: string;
};

export class Orders {
  @EventPattern(process.env.EVENT_TOPIC || EventTopic.Platform)
  handle(data: PlatformEventDto) {
    return data.eventId;
  }
}
";
        fs::write(repo_root.join("orders.ts"), source).expect("fixture");
        let file = FileEntry {
            path: Path::new("orders.ts").to_path_buf(),
            language: Language::TypeScript,
            size_bytes: u64::try_from(source.len()).unwrap_or(u64::MAX),
            content_hash: *blake3::hash(source.as_bytes()).as_bytes(),
            source_bytes: None,
        };
        let parsed =
            parse_file_with_frameworks("backend_standard", repo_root, &file, &[Framework::NestJs])
                .expect("parse");

        let inferred = infer_payload_contracts(&parsed);
        let consumer = inferred
            .iter()
            .find(|item| item.record.side == PayloadSide::Consumer)
            .expect("consumer");
        assert_eq!(
            consumer.record.contract_target_qualified_name.as_deref(),
            Some("__event__kafka__platform.lifecycle")
        );
        assert_eq!(consumer.edge.kind, gather_step_core::EdgeKind::ContractOn);
    }

    #[test]
    fn does_not_emit_consumer_contract_for_unresolvable_decorator_expression() {
        let temp = TempDir::new("consumer-topic-unresolvable");
        let repo_root = temp.path();
        let source = r"
import { EventPattern } from '@nestjs/microservices';

type PlatformEventDto = {
  eventId: string;
};

export class Orders {
  @EventPattern(process.env.EVENT_TOPIC || UnknownTopic.Platform)
  handle(data: PlatformEventDto) {
    return data.eventId;
  }
}
";
        fs::write(repo_root.join("orders.ts"), source).expect("fixture");
        let file = FileEntry {
            path: Path::new("orders.ts").to_path_buf(),
            language: Language::TypeScript,
            size_bytes: u64::try_from(source.len()).unwrap_or(u64::MAX),
            content_hash: *blake3::hash(source.as_bytes()).as_bytes(),
            source_bytes: None,
        };
        let parsed =
            parse_file_with_frameworks("backend_standard", repo_root, &file, &[Framework::NestJs])
                .expect("parse");

        let inferred = infer_payload_contracts(&parsed);
        assert!(
            !inferred
                .iter()
                .any(|item| item.record.side == PayloadSide::Consumer),
            "unresolvable decorator expressions must not emit a ContractOn edge"
        );
    }
}
