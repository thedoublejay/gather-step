//! Python Kafka producer/consumer augmentation (v5 Phase 1b).
//!
//! Emits the same convergence vocabulary as the `NestJS` event pass — virtual
//! `NodeKind::Event` nodes keyed `__event__kafka__<topic>` with `Publishes` /
//! `ProducesEventFor` (producers) and `Consumes` / `UsesEventFrom` (consumers)
//! — so a Python producer/consumer joins the same topic node a TS service uses,
//! making a cross-language event round trip visible end to end.
//!
//! Detection is signature-based and deliberately conservative (the project's
//! confidence-banding goal): only statically resolvable topic strings, enum
//! members, and simple class attribute bindings become nodes. Dynamic /
//! f-string / unconstrained variable topics are skipped rather than fabricated.
//!
//! Idioms covered: `aiokafka` `producer.send`/`send_and_wait` and the
//! `AIOKafkaConsumer(...)` constructor; `confluent-kafka` `producer.produce`
//! and `consumer.subscribe([...])`; plus wrapper-based
//! `KafkaRuntime.get().send_message(...)` / `@kafka_event(...)` APIs.
//! Module-level consumer construction (no enclosing function) is not captured,
//! since call sites require an owning function.

use std::{fs, sync::Arc};

use gather_step_core::{EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, ref_node_id};

use crate::{
    FileEntry, Language,
    top_level_split::split_top_level,
    tree_sitter::{EnrichedCallSite, ParsedFile, parse_file},
};

const MAX_IMPORTED_TOPIC_ENUM_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PythonKafkaAugmentation {
    pub nodes: Vec<NodeData>,
    pub edges: Vec<EdgeData>,
}

#[must_use]
pub fn augment(parsed: &ParsedFile) -> PythonKafkaAugmentation {
    let mut augmentation = PythonKafkaAugmentation::default();
    for call_site in &parsed.call_sites {
        if let Some(topic) = producer_topic(parsed, call_site) {
            emit_topic(parsed, call_site, &topic, true, &mut augmentation);
        }
        for topic in consumer_topics(parsed, call_site) {
            emit_topic(parsed, call_site, &topic, false, &mut augmentation);
        }
    }
    emit_proxy_producers(parsed, &mut augmentation);
    emit_decorator_consumers(parsed, &mut augmentation);
    augmentation
}

/// Topic published by a Kafka producer call, if this call site is one.
///
/// `send_and_wait`/`produce` are distinctive enough to match unconditionally;
/// the heavily-overloaded `send` only counts when its receiver names a Kafka
/// producer (so `res.send(...)` / `log.send(...)` are not misread as producers).
fn producer_topic(parsed: &ParsedFile, call_site: &EnrichedCallSite) -> Option<String> {
    if !is_producer_call(call_site) {
        return None;
    }
    let raw = call_site.raw_arguments.as_deref()?;
    let first = split_top_level(raw, ',').into_iter().next()?;
    resolve_topic(parsed, first, Some(call_site.owner_id))
}

fn is_producer_call(call_site: &EnrichedCallSite) -> bool {
    let Some(hint) = call_site.callee_qualified_hint.as_deref() else {
        return false;
    };
    let (receiver, operation) = hint.rsplit_once('.').unwrap_or(("", hint));
    match operation {
        "send_and_wait" | "produce" => true,
        "send" => receiver_names_kafka(receiver, "producer"),
        "send_message" => {
            receiver_names_kafka(receiver, "producer")
                || contains_ignore_ascii_case(receiver, "runtime")
        }
        _ => false,
    }
}

fn emit_proxy_producers(parsed: &ParsedFile, augmentation: &mut PythonKafkaAugmentation) {
    let proxies = parsed
        .call_sites
        .iter()
        .filter_map(|call_site| producer_proxy(parsed, call_site))
        .collect::<Vec<_>>();
    for call_site in &parsed.call_sites {
        for (proxy_name, topic_index) in &proxies {
            if call_site.callee_name != *proxy_name {
                continue;
            }
            let Some(raw) = call_site.raw_arguments.as_deref() else {
                continue;
            };
            let Some(argument) = split_top_level(raw, ',').get(*topic_index).copied() else {
                continue;
            };
            let Some(topic) = resolve_topic(parsed, argument, Some(call_site.owner_id)) else {
                continue;
            };
            let edge_start = augmentation.edges.len();
            emit_topic(parsed, call_site, &topic, true, augmentation);
            for edge in &mut augmentation.edges[edge_start..] {
                edge.metadata.confidence = Some(750);
                edge.metadata.resolver = Some("python_kafka_producer_proxy".to_owned());
            }
        }
    }
}

fn producer_proxy(parsed: &ParsedFile, call_site: &EnrichedCallSite) -> Option<(String, usize)> {
    if !is_producer_call(call_site) || producer_topic(parsed, call_site).is_some() {
        return None;
    }
    let raw = call_site.raw_arguments.as_deref()?;
    let topic_parameter = split_top_level(raw, ',').into_iter().next()?.trim();
    if topic_parameter.is_empty()
        || !topic_parameter
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return None;
    }
    let owner = parsed
        .symbols
        .iter()
        .find(|symbol| symbol.node.id == call_site.owner_id)?;
    let signature = owner.node.signature.as_deref()?;
    let open = signature.find('(')?;
    let close = signature.rfind(')')?;
    let parameters = split_top_level(&signature[open + 1..close], ',');
    let parameter_index = parameters.iter().position(|parameter| {
        parameter
            .split([':', '='])
            .next()
            .is_some_and(|name| name.trim() == topic_parameter)
    })?;
    let receiver_offset = if parameters
        .first()
        .and_then(|parameter| parameter.split([':', '=']).next())
        .is_some_and(|name| matches!(name.trim(), "self" | "cls"))
    {
        1
    } else {
        0
    };
    let call_index = parameter_index.checked_sub(receiver_offset)?;
    Some((owner.node.name.clone(), call_index))
}

/// Topics consumed at this call site (the `AIOKafkaConsumer(...)` constructor
/// or a `consumer.subscribe([...])` call), if any.
fn consumer_topics(parsed: &ParsedFile, call_site: &EnrichedCallSite) -> Vec<String> {
    if call_site.callee_name == "AIOKafkaConsumer" {
        return constructor_topics(parsed, call_site);
    }
    let hint = call_site
        .callee_qualified_hint
        .as_deref()
        .unwrap_or_default();
    let (receiver, operation) = hint.rsplit_once('.').unwrap_or(("", hint));
    if operation == "subscribe" && receiver_names_kafka(receiver, "consumer") {
        return subscribe_topics(parsed, call_site);
    }
    Vec::new()
}

/// Leading positional string-literal/constant topics of `AIOKafkaConsumer(...)`.
/// Stops at the first non-topic argument (a keyword arg or non-static value),
/// since topics are always the leading positional arguments.
fn constructor_topics(parsed: &ParsedFile, call_site: &EnrichedCallSite) -> Vec<String> {
    let Some(raw) = call_site.raw_arguments.as_deref() else {
        return Vec::new();
    };
    let mut topics = Vec::new();
    for argument in split_top_level(raw, ',') {
        if argument.contains('=') {
            break;
        }
        match resolve_topic(parsed, argument, Some(call_site.owner_id)) {
            Some(topic) => topics.push(topic),
            None => break,
        }
    }
    topics
}

/// Some Kafka libraries register consumers through a decorator rather than an
/// explicit `AIOKafkaConsumer.subscribe` call. Decorator captures already
/// retain the raw expression, so this path shares the same topic resolver as
/// producer calls and direct consumers.
fn emit_decorator_consumers(parsed: &ParsedFile, augmentation: &mut PythonKafkaAugmentation) {
    for symbol in &parsed.symbols {
        for decorator in &symbol.decorators {
            if !decorator
                .name
                .rsplit('.')
                .next()
                .is_some_and(|name| name.eq_ignore_ascii_case("kafka_event"))
            {
                continue;
            }
            let Some(first) = split_top_level(decorator.raw.as_str(), ',')
                .into_iter()
                .next()
            else {
                continue;
            };
            let Some(topic) = resolve_topic(parsed, first, Some(symbol.node.id)) else {
                continue;
            };
            emit_topic_for_owner(
                parsed,
                symbol.node.id,
                symbol.file_node,
                decorator.span.clone().or_else(|| symbol.node.span.clone()),
                &topic,
                false,
                augmentation,
            );
        }
    }
}

/// String-literal/constant topics inside the `subscribe([...])` list argument.
fn subscribe_topics(parsed: &ParsedFile, call_site: &EnrichedCallSite) -> Vec<String> {
    let Some(raw) = call_site.raw_arguments.as_deref() else {
        return Vec::new();
    };
    let Some(list) = split_top_level(raw, ',').into_iter().next() else {
        return Vec::new();
    };
    let inner = list
        .trim()
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
        .unwrap_or(list);
    split_top_level(inner, ',')
        .iter()
        .filter_map(|element| resolve_topic(parsed, element, Some(call_site.owner_id)))
        .collect()
}

fn emit_topic(
    parsed: &ParsedFile,
    call_site: &EnrichedCallSite,
    topic: &str,
    is_producer: bool,
    augmentation: &mut PythonKafkaAugmentation,
) {
    emit_topic_for_owner(
        parsed,
        call_site.owner_id,
        call_site.owner_file,
        call_site.span.clone(),
        topic,
        is_producer,
        augmentation,
    );
}

fn emit_topic_for_owner(
    parsed: &ParsedFile,
    owner_id: gather_step_core::NodeId,
    owner_file: gather_step_core::NodeId,
    span: Option<gather_step_core::SourceSpan>,
    topic: &str,
    is_producer: bool,
    augmentation: &mut PythonKafkaAugmentation,
) {
    let qualified_name = format!("__event__kafka__{topic}");
    let display_name = topic.strip_prefix("symbolic::").unwrap_or(topic);
    let node = NodeData {
        id: ref_node_id(NodeKind::Event, &qualified_name),
        kind: NodeKind::Event,
        repo: parsed.file_node.repo.clone(),
        file_path: parsed.file_node.file_path.clone(),
        name: display_name.to_owned(),
        qualified_name: Some(qualified_name.clone()),
        external_id: Some(qualified_name),
        signature: None,
        visibility: None,
        span,
        is_virtual: true,
        ai_role: None,
    };
    let node_id = node.id;
    augmentation.nodes.push(node);
    let kinds = if is_producer {
        [EdgeKind::Publishes, EdgeKind::ProducesEventFor]
    } else {
        [EdgeKind::Consumes, EdgeKind::UsesEventFrom]
    };
    for kind in kinds {
        let metadata = if topic.starts_with("symbolic::") {
            EdgeMetadata {
                confidence: Some(550),
                resolver: Some("python_enum_symbolic".to_owned()),
                ..EdgeMetadata::default()
            }
        } else {
            EdgeMetadata::default()
        };
        augmentation.edges.push(EdgeData {
            source: owner_id,
            target: node_id,
            kind,
            metadata,
            owner_file,
            is_cross_file: false,
        });
    }
}

/// Whether the receiver of a method call names a Kafka client of `role`
/// (`"producer"` / `"consumer"`) or Kafka itself — used to disambiguate the
/// overloaded `send`/`subscribe` operations.
fn receiver_names_kafka(receiver: &str, role: &str) -> bool {
    contains_ignore_ascii_case(receiver, role) || contains_ignore_ascii_case(receiver, "kafka")
}

/// Allocation-free case-insensitive substring test (`needle` must be ASCII).
fn contains_ignore_ascii_case(haystack: &str, needle: &str) -> bool {
    let (haystack, needle) = (haystack.as_bytes(), needle.as_bytes());
    if needle.is_empty() {
        return true;
    }
    haystack
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle))
}

/// Resolve a call/decorator argument to a static topic name. In addition to
/// literals and module constants, this supports local Enum members and the
/// wrapper-library pattern where an external event/topic enum is the only stable
/// static identity. A narrow `self.field = Enum.Member` scan handles producer
/// classes that bind their topic once in `__init__`.
pub(crate) fn resolve_topic(
    parsed: &ParsedFile,
    argument: &str,
    owner_id: Option<gather_step_core::NodeId>,
) -> Option<String> {
    resolve_topic_inner(parsed, argument, owner_id, 0)
}

fn resolve_topic_inner(
    parsed: &ParsedFile,
    argument: &str,
    owner_id: Option<gather_step_core::NodeId>,
    depth: usize,
) -> Option<String> {
    if depth > 2 {
        return None;
    }
    let argument = argument.trim();
    if let Some(literal) = string_literal(argument) {
        if literal.is_empty() || literal.contains('{') {
            return None;
        }
        return Some(literal);
    }
    if let Some(value) = parsed.constant_strings.get(argument) {
        return Some(value.clone());
    }
    // Resolve imports before local leaf-name lookup. A namespace-qualified
    // expression such as `module.StreamTopic.Member` must not attach to an
    // unrelated local `StreamTopic` with the same member name.
    if let Some(value) = imported_enum_member_value(parsed, argument) {
        return Some(value);
    }
    if let Some(value) = local_enum_member_value(parsed, argument) {
        return Some(value);
    }
    if let Some(bound) = python_self_attribute_binding(parsed, argument, owner_id) {
        return resolve_topic_inner(parsed, bound.as_str(), owner_id, depth + 1);
    }
    external_enum_member_topic(argument)
}

fn local_enum_member_value(parsed: &ParsedFile, argument: &str) -> Option<String> {
    let (enum_name, member) = argument.rsplit_once('.')?;
    let enum_name = enum_name.rsplit('.').next().unwrap_or(enum_name);
    parsed.value_mirror_candidates.iter().find_map(|candidate| {
        let crate::ts_js_oxc::ValueMirrorKind::EnumMemberDef {
            enum_qn,
            member: candidate_member,
        } = &candidate.kind
        else {
            return None;
        };
        let candidate_enum = enum_qn.rsplit('.').next().unwrap_or(enum_qn);
        (candidate_enum == enum_name && candidate_member == member).then(|| candidate.value.clone())
    })
}

fn python_self_attribute_binding(
    parsed: &ParsedFile,
    argument: &str,
    owner_id: Option<gather_step_core::NodeId>,
) -> Option<String> {
    if !argument.starts_with("self.") {
        return None;
    }
    let owner = parsed
        .symbols
        .iter()
        .find(|symbol| Some(symbol.node.id) == owner_id)?;
    let class_name = owner.node.qualified_name.as_deref()?.rsplit_once('.')?.0;
    let class_span = parsed
        .symbols
        .iter()
        .find(|symbol| {
            symbol.node.kind == NodeKind::Class
                && symbol.node.qualified_name.as_deref() == Some(class_name)
        })?
        .node
        .span
        .as_ref()?;
    let lines = parsed.source.lines().collect::<Vec<_>>();
    let start = usize::try_from(class_span.line_start.saturating_sub(1)).unwrap_or(0);
    let end = usize::try_from(class_span.line_end())
        .unwrap_or(lines.len())
        .min(lines.len());
    let mut bindings = lines[start.min(end)..end].iter().filter_map(|line| {
        let (left, right) = line.trim().split_once('=')?;
        (left.trim() == argument).then(|| right.trim().to_owned())
    });
    let first = bindings.next()?;
    // The parser does not yet carry class ownership on assignments. Refuse a
    // file-wide binding when another class reuses the same attribute with a
    // different topic; choosing the first would silently cross-wire events.
    bindings.all(|binding| binding == first).then_some(first)
}

/// Resolve a directly imported Python enum member from its defining file.
/// Import paths have already passed the parser's allowed-root/symlink checks;
/// this pass remains bounded and parses only the referenced file. The result
/// is the canonical wire value, so Python wrapper events converge with literal
/// and cross-language producers/consumers instead of remaining symbolic.
fn imported_enum_member_value(parsed: &ParsedFile, argument: &str) -> Option<String> {
    let (enum_expression, member) = argument.rsplit_once('.')?;
    let (binding, imported_enum) = parsed.import_bindings.iter().find_map(|binding| {
        if binding.local_name == enum_expression && !binding.is_namespace {
            let imported = binding
                .imported_name
                .as_deref()
                .unwrap_or(binding.local_name.as_str());
            return Some((binding, imported.to_owned()));
        }
        if binding.is_namespace {
            if let Some(imported) = enum_expression
                .strip_prefix(binding.source.as_str())
                .and_then(|value| value.strip_prefix('.'))
                && !imported.is_empty()
            {
                return Some((binding, imported.to_owned()));
            }
            let imported = enum_expression
                .strip_prefix(binding.local_name.as_str())?
                .strip_prefix('.')?;
            if !imported.is_empty() {
                return Some((binding, imported.to_owned()));
            }
        }
        None
    })?;
    let path = binding.resolved_path.as_deref()?;
    let size = usize::try_from(fs::metadata(path).ok()?.len()).ok()?;
    if size > MAX_IMPORTED_TOPIC_ENUM_BYTES {
        return None;
    }
    let bytes = fs::read(path).ok()?;
    let parent = path.parent()?;
    let file_name = path.file_name()?.into();
    let imported = parse_file(
        &parsed.file_node.repo,
        parent,
        &FileEntry {
            path: file_name,
            language: Language::Python,
            size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            content_hash: *blake3::hash(&bytes).as_bytes(),
            source_bytes: Some(Arc::from(bytes.into_boxed_slice())),
        },
    )
    .ok()?;
    local_enum_member_value(&imported, &format!("{imported_enum}.{member}"))
}

/// Preserve an external enum member as a symbolic topic identity. This is
/// deliberately gated to enum type names containing event/topic/kafka so
/// arbitrary dotted attributes do not become event nodes. A workspace-level
/// enum-value reconciliation pass may later join the symbolic node to a
/// canonical wire value; guessing a value from `PascalCase` would invent edges.
fn external_enum_member_topic(argument: &str) -> Option<String> {
    let (enum_name, member) = argument.rsplit_once('.')?;
    if !["event", "topic", "kafka"]
        .iter()
        .any(|needle| contains_ignore_ascii_case(enum_name, needle))
    {
        return None;
    }
    if member.is_empty()
        || !member
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return None;
    }
    Some(format!("symbolic::{enum_name}.{member}"))
}

/// Inner text of a plain quoted string literal, or `None` for prefixed strings
/// (`f"..."`, `b"..."`, `r"..."`) and non-strings.
fn string_literal(argument: &str) -> Option<String> {
    let bytes = argument.as_bytes();
    if bytes.len() < 2 {
        return None;
    }
    let quote = bytes[0];
    if (quote == b'"' || quote == b'\'') && bytes[bytes.len() - 1] == quote {
        return Some(argument[1..argument.len() - 1].to_owned());
    }
    None
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::{EdgeKind, NodeKind};

    use crate::{Language, frameworks::Framework, tree_sitter::parse_file_with_frameworks};

    static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> Self {
            let counter = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-parser-pykafka-{name}-{}-{counter}",
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

    fn parse(dir: &TestDir, file: &str, body: &str) -> crate::tree_sitter::ParsedFile {
        fs::write(dir.path().join(file), body).expect("fixture should write");
        parse_file_with_frameworks(
            "ingestion",
            dir.path(),
            &crate::FileEntry {
                path: file.into(),
                language: Language::Python,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &[Framework::PythonKafka],
        )
        .expect("fixture should parse")
    }

    fn event_ids(parsed: &crate::tree_sitter::ParsedFile) -> Vec<String> {
        let mut ids = parsed
            .nodes
            .iter()
            .filter(|node| node.kind == NodeKind::Event)
            .map(|node| node.external_id.clone().unwrap_or_default())
            .collect::<Vec<_>>();
        ids.sort();
        ids.dedup();
        ids
    }

    fn edge_count(parsed: &crate::tree_sitter::ParsedFile, kind: EdgeKind) -> usize {
        parsed.edges.iter().filter(|edge| edge.kind == kind).count()
    }

    #[test]
    fn aiokafka_producer_send_and_wait_publishes_event() {
        let dir = TestDir::new("producer");
        let parsed = parse(
            &dir,
            "producer.py",
            r#"
from aiokafka import AIOKafkaProducer


async def publish(producer, value):
    await producer.send_and_wait("document-indexed", value)
"#,
        );

        assert_eq!(
            event_ids(&parsed),
            vec!["__event__kafka__document-indexed".to_owned()]
        );
        assert_eq!(edge_count(&parsed, EdgeKind::Publishes), 1);
        assert_eq!(edge_count(&parsed, EdgeKind::ProducesEventFor), 1);
    }

    #[test]
    fn aiokafka_consumer_constructor_consumes_each_topic() {
        let dir = TestDir::new("consumer");
        let parsed = parse(
            &dir,
            "consumer.py",
            r#"
from aiokafka import AIOKafkaConsumer


async def consume():
    consumer = AIOKafkaConsumer(
        "document-indexed", "user-created", bootstrap_servers="kafka:9092"
    )
    async for message in consumer:
        handle(message)
"#,
        );

        assert_eq!(
            event_ids(&parsed),
            vec![
                "__event__kafka__document-indexed".to_owned(),
                "__event__kafka__user-created".to_owned(),
            ]
        );
        assert_eq!(edge_count(&parsed, EdgeKind::Consumes), 2);
        assert_eq!(edge_count(&parsed, EdgeKind::UsesEventFrom), 2);
    }

    #[test]
    fn confluent_produce_and_subscribe_are_detected() {
        let dir = TestDir::new("confluent");
        let parsed = parse(
            &dir,
            "confluent.py",
            r#"
def run(producer, consumer):
    producer.produce("user-created", b"payload")
    consumer.subscribe(["user-created", "report-ready"])
"#,
        );

        assert_eq!(
            event_ids(&parsed),
            vec![
                "__event__kafka__report-ready".to_owned(),
                "__event__kafka__user-created".to_owned(),
            ]
        );
        assert_eq!(edge_count(&parsed, EdgeKind::Publishes), 1);
        assert_eq!(edge_count(&parsed, EdgeKind::Consumes), 2);
    }

    #[test]
    fn non_kafka_send_emit_calls_are_ignored() {
        let dir = TestDir::new("negative");
        let parsed = parse(
            &dir,
            "negative.py",
            r#"
def handler(res, socket, log):
    res.send("ok")
    socket.emit("ping", data)
    log.send("a message")
"#,
        );

        assert!(event_ids(&parsed).is_empty());
        assert_eq!(edge_count(&parsed, EdgeKind::Publishes), 0);
        assert_eq!(edge_count(&parsed, EdgeKind::Consumes), 0);
    }

    #[test]
    fn module_level_constant_topic_resolves() {
        let dir = TestDir::new("const-topic");
        let parsed = parse(
            &dir,
            "producer.py",
            r#"
from aiokafka import AIOKafkaProducer

DOCUMENT_TOPIC = "document-indexed"


async def publish(producer, value):
    await producer.send_and_wait(DOCUMENT_TOPIC, value)
"#,
        );

        assert_eq!(
            event_ids(&parsed),
            vec!["__event__kafka__document-indexed".to_owned()]
        );
        assert_eq!(edge_count(&parsed, EdgeKind::Publishes), 1);
    }

    #[test]
    fn dynamic_topics_are_skipped() {
        let dir = TestDir::new("dynamic");
        let parsed = parse(
            &dir,
            "dynamic.py",
            r#"
async def publish(producer, topic, value):
    await producer.send_and_wait(topic, value)
    await producer.send_and_wait(f"prefix-{topic}", value)
"#,
        );

        assert!(event_ids(&parsed).is_empty());
        assert_eq!(edge_count(&parsed, EdgeKind::Publishes), 0);
    }

    #[test]
    fn decorator_and_runtime_wrapper_converge_on_enum_values() {
        let dir = TestDir::new("decorator-runtime-wrapper");
        let parsed = parse(
            &dir,
            "asset_events.py",
            r#"
from enum import Enum


class BrokerTopic(str, Enum):
    AssetUploaded = "asset.uploaded"
    AssetProcessed = "asset.processed"


@kafka_event(BrokerTopic.AssetUploaded)
async def consume_asset_uploaded(message):
    await KafkaRuntime.get().send_message(
        BrokerTopic.AssetProcessed, message
    )
"#,
        );

        assert_eq!(
            event_ids(&parsed),
            vec![
                "__event__kafka__asset.processed".to_owned(),
                "__event__kafka__asset.uploaded".to_owned(),
            ]
        );
        assert_eq!(edge_count(&parsed, EdgeKind::Consumes), 1);
        assert_eq!(edge_count(&parsed, EdgeKind::Publishes), 1);
    }

    #[test]
    fn runtime_wrapper_producer_resolves_self_topic_assignment() {
        let dir = TestDir::new("self-topic");
        let parsed = parse(
            &dir,
            "asset_processed.py",
            r#"
from enum import Enum


class BrokerTopic(str, Enum):
    AssetProcessed = "asset.processed"


class AssetProcessedPublisher:
    def __init__(self):
        self.event_topic = BrokerTopic.AssetProcessed

    async def publish(self, message):
        await KafkaRuntime.get().send_message(self.event_topic, message)
"#,
        );

        assert_eq!(
            event_ids(&parsed),
            vec!["__event__kafka__asset.processed".to_owned()]
        );
        assert_eq!(edge_count(&parsed, EdgeKind::Publishes), 1);
    }

    #[test]
    fn one_hop_producer_proxy_resolves_topic_at_call_site() {
        let dir = TestDir::new("producer-proxy");
        let parsed = parse(
            &dir,
            "publisher.py",
            r#"
from enum import Enum


class BrokerTopic(str, Enum):
    AssetQueued = "asset.queued"


async def publish(producer, topic, payload):
    await producer.send_and_wait(topic, payload)


async def queue_asset(producer, payload):
    await publish(producer, BrokerTopic.AssetQueued, payload)
"#,
        );

        assert_eq!(
            event_ids(&parsed),
            vec!["__event__kafka__asset.queued".to_owned()]
        );
        assert_eq!(edge_count(&parsed, EdgeKind::Publishes), 1);
        assert!(parsed.edges.iter().any(|edge| {
            edge.kind == EdgeKind::Publishes
                && edge.metadata.confidence == Some(750)
                && edge.metadata.resolver.as_deref() == Some("python_kafka_producer_proxy")
        }));
    }

    #[test]
    fn producer_proxy_does_not_fabricate_edges_without_a_static_caller_topic() {
        let dir = TestDir::new("dynamic-producer-proxy");
        let parsed = parse(
            &dir,
            "publisher.py",
            r#"
async def publish(producer, topic, payload):
    await producer.send_and_wait(topic, payload)


async def forward(producer, topic, payload):
    await publish(producer, topic, payload)
"#,
        );

        assert!(event_ids(&parsed).is_empty());
        assert_eq!(edge_count(&parsed, EdgeKind::Publishes), 0);
    }

    #[test]
    fn external_event_enum_members_remain_symbolic_until_value_reconciliation() {
        let dir = TestDir::new("external-enum");
        let parsed = parse(
            &dir,
            "events.py",
            r#"
from broker_runtime import BrokerTopic, KafkaRuntime, kafka_event


@kafka_event(BrokerTopic.AssetUploaded)
async def consume(message):
    await KafkaRuntime.get().send_message(
        BrokerTopic.ContentProcessingCompleted, message
    )
    await KafkaRuntime.get().send_message(
        BrokerTopic.SystemAuditEvents, message
    )
"#,
        );

        assert_eq!(
            event_ids(&parsed),
            vec![
                "__event__kafka__symbolic::BrokerTopic.AssetUploaded".to_owned(),
                "__event__kafka__symbolic::BrokerTopic.ContentProcessingCompleted".to_owned(),
                "__event__kafka__symbolic::BrokerTopic.SystemAuditEvents".to_owned(),
            ]
        );
        assert_eq!(edge_count(&parsed, EdgeKind::Consumes), 1);
        assert_eq!(edge_count(&parsed, EdgeKind::Publishes), 2);
    }

    #[test]
    fn imported_topic_enum_resolves_to_canonical_wire_value() {
        let dir = TestDir::new("imported-enum");
        fs::write(
            dir.path().join("topic_defs.py"),
            r#"
from enum import Enum


class StreamTopic(str, Enum):
    JobQueued = "jobs.queued"
"#,
        )
        .expect("topic definition should write");
        let parsed = parse(
            &dir,
            "events.py",
            r#"
from topic_defs import StreamTopic


@kafka_event(StreamTopic.JobQueued)
async def consume(message):
    await KafkaRuntime.get().send_message(StreamTopic.JobQueued, message)
"#,
        );

        assert_eq!(
            event_ids(&parsed),
            vec!["__event__kafka__jobs.queued".to_owned()]
        );
        assert_eq!(edge_count(&parsed, EdgeKind::Consumes), 1);
        assert_eq!(edge_count(&parsed, EdgeKind::Publishes), 1);
    }

    #[test]
    fn python_kafka_pack_runs_without_fastapi_dependency() {
        let dir = TestDir::new("kafka-no-fastapi");
        fs::write(
            dir.path().join("pyproject.toml"),
            "[project]\ndependencies = [\"aiokafka>=0.10\"]\n",
        )
        .expect("manifest should write");
        let frameworks: Vec<Framework> = crate::frameworks::detect::detect_frameworks(dir.path())
            .into_iter()
            .collect();
        assert!(
            !frameworks.contains(&Framework::FastApi),
            "repo declares aiokafka but not fastapi"
        );

        let file = "producer.py";
        fs::write(
            dir.path().join(file),
            r#"
from aiokafka import AIOKafkaProducer


async def publish(producer, value):
    await producer.send_and_wait("document-indexed", value)
"#,
        )
        .expect("fixture should write");
        let parsed = parse_file_with_frameworks(
            "ingestion",
            dir.path(),
            &crate::FileEntry {
                path: file.into(),
                language: Language::Python,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &frameworks,
        )
        .expect("fixture should parse");

        assert_eq!(
            event_ids(&parsed),
            vec!["__event__kafka__document-indexed".to_owned()],
            "Kafka pack must run from an aiokafka dependency with no fastapi"
        );
        assert_eq!(edge_count(&parsed, EdgeKind::Publishes), 1);
    }
}
