//! Python Kafka producer/consumer augmentation (v5 Phase 1b).
//!
//! Emits the same convergence vocabulary as the `NestJS` event pass — virtual
//! `NodeKind::Event` nodes keyed `__event__kafka__<topic>` with `Publishes` /
//! `ProducesEventFor` (producers) and `Consumes` / `UsesEventFrom` (consumers)
//! — so a Python producer/consumer joins the same topic node a TS service uses,
//! making a cross-language event round trip visible end to end.
//!
//! Detection is signature-based and deliberately conservative (the project's
//! confidence-banding goal): only statically resolvable topic strings (quoted
//! literals or module-level constants) become nodes — dynamic / f-string /
//! variable topics are skipped rather than fabricated.
//!
//! Idioms covered: `aiokafka` `producer.send`/`send_and_wait` and the
//! `AIOKafkaConsumer(...)` constructor; `confluent-kafka` `producer.produce`
//! and `consumer.subscribe([...])`. This runs under the Python augmentation
//! arm (gated on the `FastAPI` repo dependency); a Python+Kafka repo without
//! `FastAPI` is a known recall gap until a dedicated Kafka-dependency gate
//! lands. Module-level consumer construction (no enclosing function) is also
//! not captured, since call sites require an owning function.

use gather_step_core::{EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, ref_node_id};

use crate::tree_sitter::{EnrichedCallSite, ParsedFile};

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
    augmentation
}

/// Topic published by a Kafka producer call, if this call site is one.
///
/// `send_and_wait`/`produce` are distinctive enough to match unconditionally;
/// the heavily-overloaded `send` only counts when its receiver names a Kafka
/// producer (so `res.send(...)` / `log.send(...)` are not misread as producers).
fn producer_topic(parsed: &ParsedFile, call_site: &EnrichedCallSite) -> Option<String> {
    let hint = call_site.callee_qualified_hint.as_deref()?;
    let (receiver, operation) = hint.rsplit_once('.').unwrap_or(("", hint));
    let is_producer = match operation {
        "send_and_wait" | "produce" => true,
        "send" => receiver_names_kafka(receiver, "producer"),
        _ => false,
    };
    if !is_producer {
        return None;
    }
    let raw = call_site.raw_arguments.as_deref()?;
    let first = split_top_level(raw).into_iter().next()?;
    resolve_topic(parsed, &first)
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
    for argument in split_top_level(raw) {
        match resolve_topic(parsed, &argument) {
            Some(topic) => topics.push(topic),
            None => break,
        }
    }
    topics
}

/// String-literal/constant topics inside the `subscribe([...])` list argument.
fn subscribe_topics(parsed: &ParsedFile, call_site: &EnrichedCallSite) -> Vec<String> {
    let Some(raw) = call_site.raw_arguments.as_deref() else {
        return Vec::new();
    };
    let Some(list) = split_top_level(raw).into_iter().next() else {
        return Vec::new();
    };
    let inner = list
        .trim()
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
        .unwrap_or(&list);
    split_top_level(inner)
        .iter()
        .filter_map(|element| resolve_topic(parsed, element))
        .collect()
}

fn emit_topic(
    parsed: &ParsedFile,
    call_site: &EnrichedCallSite,
    topic: &str,
    is_producer: bool,
    augmentation: &mut PythonKafkaAugmentation,
) {
    let qualified_name = format!("__event__kafka__{topic}");
    let node = NodeData {
        id: ref_node_id(NodeKind::Event, &qualified_name),
        kind: NodeKind::Event,
        repo: parsed.file_node.repo.clone(),
        file_path: parsed.file_node.file_path.clone(),
        name: topic.to_owned(),
        qualified_name: Some(qualified_name.clone()),
        external_id: Some(qualified_name),
        signature: None,
        visibility: None,
        span: call_site.span.clone(),
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
        augmentation.edges.push(EdgeData {
            source: call_site.owner_id,
            target: node_id,
            kind,
            metadata: EdgeMetadata::default(),
            owner_file: parsed.file_node.id,
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

/// Resolve a call argument to a static topic name: a quoted string literal, or
/// a module-level string constant referenced by bare name. Returns `None` for
/// dynamic values (variables, f-strings, interpolated literals) so junk topic
/// nodes are never fabricated.
fn resolve_topic(parsed: &ParsedFile, argument: &str) -> Option<String> {
    let argument = argument.trim();
    if let Some(literal) = string_literal(argument) {
        if literal.is_empty() || literal.contains('{') {
            return None;
        }
        return Some(literal);
    }
    parsed.constant_strings.get(argument).cloned()
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

/// Split a comma-separated argument string at top level, respecting quotes and
/// `()`/`[]`/`{}` nesting. Returns trimmed, non-empty fragments.
fn split_top_level(raw: &str) -> Vec<String> {
    let mut fragments = Vec::new();
    let mut depth = 0_i32;
    let mut in_string: Option<u8> = None;
    let mut start = 0;
    for (index, &byte) in raw.as_bytes().iter().enumerate() {
        match in_string {
            Some(quote) => {
                if byte == quote {
                    in_string = None;
                }
            }
            None => match byte {
                b'"' | b'\'' => in_string = Some(byte),
                b'(' | b'[' | b'{' => depth += 1,
                b')' | b']' | b'}' => depth -= 1,
                b',' if depth == 0 => {
                    fragments.push(raw[start..index].trim().to_owned());
                    start = index + 1;
                }
                _ => {}
            },
        }
    }
    fragments.push(raw[start..].trim().to_owned());
    fragments.retain(|fragment| !fragment.is_empty());
    fragments
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
            &[Framework::FastApi],
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
}
