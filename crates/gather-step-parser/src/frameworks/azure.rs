//! Azure Service Bus, Azure Web `PubSub`, and `LaunchDarkly` feature flag
//! extractor pack.
//!
//! `augment()` is called by the tree-sitter orchestrator when either the
//! `Framework::Azure` or `Framework::LaunchDarkly` flag is active for the
//! repo.  It receives a fully-resolved [`ParsedFile`] snapshot and returns
//! supplementary [`NodeData`] / [`EdgeData`] pairs that are merged into the
//! caller's output.
//!
//! ## Virtual-node naming conventions
//!
//! | Pattern              | Kind          | QN prefix                        |
//! |----------------------|---------------|----------------------------------|
//! | Service Bus queue/topic | `Topic`    | `__topic__servicebus__`          |
//! | Web PubSub group/event  | `Event`    | `__event__pubsub__`              |
//! | LaunchDarkly flag key   | `Service`  | `__feature_flag__`               |
//! | LaunchDarkly client     | `Service`  | `__feature_flag__client`         |
//! | LaunchDarkly hook usage | `Service`  | `__feature_flag__usage`          |
//! | shared-lib import       | `SharedSymbol` | `__shared__`                 |

use std::{
    cell::RefCell,
    collections::BTreeSet,
    path::{Path, PathBuf},
};

use gather_step_core::{
    EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, ref_node_id, shared_package_root,
    shared_symbol_qn_unversioned,
};

use crate::{resolve::ImportBinding, tree_sitter::ParsedFile};

/// Output of the Azure / `LaunchDarkly` extractor pass.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AzureAugmentation {
    pub nodes: Vec<NodeData>,
    pub edges: Vec<EdgeData>,
}

/// Analyse `parsed` and emit Azure / `LaunchDarkly` semantic nodes and edges.
///
/// The function is intentionally side-effect-free: it only reads `parsed` and
/// returns a fresh [`AzureAugmentation`].  Deduplication of virtual nodes is
/// handled by the orchestrator's `append_unique_nodes` call after this returns.
///
/// Note: shared-contract detection (`add_shared_lib_edges`) has been moved to
/// [`augment_shared_lib`] and runs unconditionally for all TS/JS files, not
/// gated behind Azure/LaunchDarkly.
pub fn augment(parsed: &ParsedFile) -> AzureAugmentation {
    let mut aug = AzureAugmentation::default();

    add_service_bus_edges(parsed, &mut aug);
    add_web_pubsub_edges(parsed, &mut aug);
    add_launchdarkly_edges(parsed, &mut aug);

    aug
}

/// Extract shared-contract edges from `shared-lib` imports (e.g.,
/// `@scope/shared-lib`).  This is NOT gated behind any framework — any
/// TS/JS file that imports from a `shared-lib` package gets `SharedSymbol` nodes.
///
/// Called unconditionally from the tree-sitter orchestrator.
pub fn augment_shared_lib(parsed: &ParsedFile) -> AzureAugmentation {
    let relative_path = parsed.file.path.clone();
    let Some(_guard) = SharedLibParseGuard::enter(&relative_path) else {
        return AzureAugmentation::default();
    };
    let mut aug = AzureAugmentation::default();
    add_shared_lib_edges(parsed, &mut aug);
    aug
}

// ---------------------------------------------------------------------------
// Azure Service Bus
// ---------------------------------------------------------------------------

/// Recognises Service Bus SDK call sites and emits `Topic` virtual nodes with
/// `Publishes` (send path) or `Consumes` (receive path) edges.
///
/// Detection criterion: `callee_qualified_hint` contains one of
/// `ServiceBusClient`, `ServiceBusSender`, `ServiceBusReceiver`, or
/// `serviceBus` (case-insensitive on the last segment).
fn add_service_bus_edges(parsed: &ParsedFile, aug: &mut AzureAugmentation) {
    for call_site in &parsed.call_sites {
        let Some(hint) = call_site.callee_qualified_hint.as_deref() else {
            continue;
        };

        if !is_service_bus_hint(hint) {
            continue;
        }

        let operation = last_segment(hint);
        let Some(edge_kind) = service_bus_edge_kind(operation) else {
            continue;
        };

        // We still emit the virtual node even when no literal argument is
        // present, but we skip if there's nothing useful to name it by.
        let Some(raw_name) = call_site.literal_argument.as_deref() else {
            continue;
        };
        let name = sanitize_name(raw_name);
        if name.is_empty() {
            continue;
        }

        let qualified_name = format!("__topic__servicebus__{name}");
        let topic_node =
            virtual_node_from_call_site(NodeKind::Topic, &qualified_name, &name, parsed, call_site);
        aug.nodes.push(topic_node.clone());
        aug.edges.push(EdgeData {
            source: call_site.owner_id,
            target: topic_node.id,
            kind: edge_kind,
            metadata: EdgeMetadata::default(),
            owner_file: parsed.file_node.id,
            is_cross_file: false,
        });
    }
}

/// Returns `true` when the qualified hint refers to a Service Bus SDK object.
///
/// Matches `ServiceBusClient`, `ServiceBusSender`, `ServiceBusReceiver`, or
/// any object whose name contains `serviceBus` / `servicebus`.
fn is_service_bus_hint(hint: &str) -> bool {
    // Strip the trailing operation segment so we only inspect the receiver.
    let receiver = hint.rsplit_once('.').map_or(hint, |(recv, _)| recv);
    contains_ignore_ascii_case(receiver, "ServiceBusClient")
        || contains_ignore_ascii_case(receiver, "ServiceBusSender")
        || contains_ignore_ascii_case(receiver, "ServiceBusReceiver")
        || contains_ignore_ascii_case(receiver, "serviceBus")
}

/// Maps a Service Bus operation name to the appropriate edge kind.
///
/// | Operation                         | Edge        |
/// |-----------------------------------|-------------|
/// | `send`, `sendMessages`, `sendBatch`, `createSender` | `Publishes` |
/// | `subscribe`, `receiveMessages`, `createReceiver`    | `Consumes`  |
fn service_bus_edge_kind(operation: &str) -> Option<EdgeKind> {
    match operation {
        "send" | "sendMessages" | "sendBatch" | "createSender" => Some(EdgeKind::Publishes),
        "subscribe" | "receiveMessages" | "createReceiver" => Some(EdgeKind::Consumes),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Azure Web PubSub
// ---------------------------------------------------------------------------

/// Recognises Web `PubSub` SDK call sites and emits `Event` virtual nodes with
/// `Publishes` or `Consumes` edges.
///
/// Detection criterion: `callee_qualified_hint` contains `WebPubSub`,
/// `PubSub`, or `webPubSub` in the receiver portion.
fn add_web_pubsub_edges(parsed: &ParsedFile, aug: &mut AzureAugmentation) {
    for call_site in &parsed.call_sites {
        let Some(hint) = call_site.callee_qualified_hint.as_deref() else {
            continue;
        };

        if !is_web_pubsub_hint(hint) {
            continue;
        }

        let operation = last_segment(hint);
        let Some(edge_kind) = web_pubsub_edge_kind(operation) else {
            continue;
        };

        let Some(raw_name) = call_site.literal_argument.as_deref() else {
            continue;
        };
        let name = sanitize_name(raw_name);
        if name.is_empty() {
            continue;
        }

        let qualified_name = format!("__event__pubsub__{name}");
        let event_node =
            virtual_node_from_call_site(NodeKind::Event, &qualified_name, &name, parsed, call_site);
        aug.nodes.push(event_node.clone());
        aug.edges.push(EdgeData {
            source: call_site.owner_id,
            target: event_node.id,
            kind: edge_kind,
            metadata: EdgeMetadata::default(),
            owner_file: parsed.file_node.id,
            is_cross_file: false,
        });
    }
}

/// Returns `true` when the hint receiver contains a Web `PubSub` SDK reference.
fn is_web_pubsub_hint(hint: &str) -> bool {
    let receiver = hint.rsplit_once('.').map_or(hint, |(recv, _)| recv);
    contains_ignore_ascii_case(receiver, "WebPubSub")
        || contains_ignore_ascii_case(receiver, "PubSub")
        || contains_ignore_ascii_case(receiver, "webPubSub")
}

/// Maps a Web `PubSub` operation to the appropriate edge kind.
///
/// | Operation                                     | Edge        |
/// |-----------------------------------------------|-------------|
/// | `sendToAll`, `sendToUser`, `sendToGroup`, `sendEvent` | `Publishes` |
/// | `on`, `subscribe`                             | `Consumes`  |
fn web_pubsub_edge_kind(operation: &str) -> Option<EdgeKind> {
    match operation {
        "sendToAll" | "sendToUser" | "sendToGroup" | "sendEvent" => Some(EdgeKind::Publishes),
        "on" | "subscribe" => Some(EdgeKind::Consumes),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// LaunchDarkly feature flags
// ---------------------------------------------------------------------------

/// Recognises `LaunchDarkly` SDK call sites and emits `Service` virtual nodes
/// with `References` edges.
///
/// Three sub-patterns are handled:
///
/// 1. **Flag evaluation** — `variation`, `boolVariation`, `stringVariation`,
///    `numberVariation`, `jsonVariation`: extracts the literal flag key and
///    creates a `__feature_flag__<key>` Service node.
/// 2. **React SDK hooks** — `useFlags`, `useLDClient`,
///    `useFeatureFlag`, `useBooleanFlag`: emits a single
///    `__feature_flag__usage` Service node (no flag key needed).
/// 3. **Client initialisation** — `LDClient.init` / any `initialize` call
///    whose hint contains `LaunchDarkly` or `LD`: emits
///    `__feature_flag__client`.
fn add_launchdarkly_edges(parsed: &ParsedFile, aug: &mut AzureAugmentation) {
    for call_site in &parsed.call_sites {
        let operation = call_site.callee_name.as_str();

        // --- flag evaluation ---
        if matches!(
            operation,
            "variation" | "boolVariation" | "stringVariation" | "numberVariation" | "jsonVariation"
        ) {
            if let Some(raw_key) = call_site.literal_argument.as_deref() {
                let key = sanitize_name(raw_key);
                if !key.is_empty() {
                    let qualified_name = format!("__feature_flag__{key}");
                    let flag_node = virtual_node_from_call_site(
                        NodeKind::Service,
                        &qualified_name,
                        &key,
                        parsed,
                        call_site,
                    );
                    aug.nodes.push(flag_node.clone());
                    aug.edges.push(EdgeData {
                        source: call_site.owner_id,
                        target: flag_node.id,
                        kind: EdgeKind::References,
                        metadata: EdgeMetadata::default(),
                        owner_file: parsed.file_node.id,
                        is_cross_file: false,
                    });
                }
            }
            continue;
        }

        // --- React SDK hooks ---
        if matches!(
            operation,
            "useFlags" | "useLDClient" | "useFeatureFlag" | "useBooleanFlag"
        ) {
            let qualified_name = "__feature_flag__usage";
            let usage_node = virtual_node_from_call_site(
                NodeKind::Service,
                qualified_name,
                "feature-flag-usage",
                parsed,
                call_site,
            );
            aug.nodes.push(usage_node.clone());
            aug.edges.push(EdgeData {
                source: call_site.owner_id,
                target: usage_node.id,
                kind: EdgeKind::References,
                metadata: EdgeMetadata::default(),
                owner_file: parsed.file_node.id,
                is_cross_file: false,
            });
            continue;
        }

        // --- client initialisation ---
        let is_init = operation == "initialize"
            && call_site.callee_qualified_hint.as_deref().is_some_and(|h| {
                contains_ignore_ascii_case(h, "LaunchDarkly")
                    || contains_ignore_ascii_case(h, "LDClient")
                    || contains_ignore_ascii_case(h, "ldClient")
            });
        let is_ld_init = call_site.callee_qualified_hint.as_deref().is_some_and(|h| {
            contains_ignore_ascii_case(last_segment(h), "init")
                && (contains_ignore_ascii_case(h, "LaunchDarkly")
                    || contains_ignore_ascii_case(h, "LDClient"))
        });

        if is_init || is_ld_init {
            let qualified_name = "__feature_flag__client";
            let client_node = virtual_node_from_call_site(
                NodeKind::Service,
                qualified_name,
                "feature-flag-client",
                parsed,
                call_site,
            );
            aug.nodes.push(client_node.clone());
            aug.edges.push(EdgeData {
                source: call_site.owner_id,
                target: client_node.id,
                kind: EdgeKind::References,
                metadata: EdgeMetadata::default(),
                owner_file: parsed.file_node.id,
                is_cross_file: false,
            });
        }
    }
}

// ---------------------------------------------------------------------------
// shared-lib shared contract detection
// ---------------------------------------------------------------------------

/// Scans `import_bindings` for imports from any `@<scope>/shared-lib`
/// package (detected by `shared-lib` substring in the import source).
///
/// For every named import binding, a `SharedSymbol` virtual node is created
/// with QN `__shared__<importedName>` and a `References` edge is drawn from
/// the file node.  This lets downstream tools answer "who depends on this
/// shared-lib contract?" across all microservices.
///
/// Namespace imports (`import * as lib from '...'`) and default imports are
/// skipped because we cannot determine which symbols are actually used without
/// a use-site analysis pass.
fn add_shared_lib_edges(parsed: &ParsedFile, aug: &mut AzureAugmentation) {
    let mut seen_targets = BTreeSet::new();
    for binding in &parsed.import_bindings {
        if binding.is_namespace || binding.is_default {
            continue;
        }

        let Some((package, symbol_name)) =
            resolve_shared_symbol_binding(parsed, binding, &mut BTreeSet::new())
        else {
            continue;
        };

        let qualified_name = shared_symbol_qn_unversioned(&package, &symbol_name);
        let shared_node = virtual_node_from_binding(
            NodeKind::SharedSymbol,
            &qualified_name,
            &symbol_name,
            parsed,
            binding,
        );
        if !seen_targets.insert(shared_node.id) {
            continue;
        }
        aug.nodes.push(shared_node.clone());
        aug.edges.push(EdgeData {
            source: parsed.file_node.id,
            target: shared_node.id,
            kind: EdgeKind::References,
            metadata: EdgeMetadata::default(),
            owner_file: parsed.file_node.id,
            is_cross_file: false,
        });
    }
}

fn resolve_shared_symbol_binding(
    parsed: &ParsedFile,
    binding: &ImportBinding,
    visited_paths: &mut BTreeSet<PathBuf>,
) -> Option<(String, String)> {
    let symbol_name = binding
        .imported_name
        .as_deref()
        .filter(|name| !name.is_empty())
        .unwrap_or(binding.local_name.as_str());
    if symbol_name.is_empty() {
        return None;
    }

    if let Some(package) = shared_package_for_source(&binding.source) {
        return Some((package, symbol_name.to_owned()));
    }

    let resolved_path = binding.resolved_path.as_ref()?;
    let repo_root = derive_repo_root(parsed);
    let relative_path = resolved_path.strip_prefix(&repo_root).ok()?.to_path_buf();
    if !visited_paths.insert(relative_path.clone()) {
        return None;
    }

    let imported = parse_local_barrel_file(parsed, &repo_root, &relative_path)?;
    let export_name = binding
        .imported_name
        .as_deref()
        .unwrap_or(&binding.local_name);
    imported.import_bindings.iter().find_map(|candidate| {
        (candidate.local_name == export_name)
            .then(|| resolve_shared_symbol_binding(&imported, candidate, visited_paths))
            .flatten()
    })
}

fn shared_package_for_source(source: &str) -> Option<String> {
    let package = shared_package_root(source)?;
    is_shared_lib_source(package).then(|| package.to_owned())
}

/// Returns `true` when the import source names a likely shared-contract package,
/// covering `@scope/shared-lib`, `@org/shared-contracts/dtos`, `types/foo`, etc.
fn is_shared_lib_source(source: &str) -> bool {
    contains_ignore_ascii_case(source, "shared")
        || contains_ignore_ascii_case(source, "contract")
        || contains_ignore_ascii_case(source, "schema")
        || contains_ignore_ascii_case(source, "types")
}

fn parse_local_barrel_file(
    parsed: &ParsedFile,
    repo_root: &std::path::Path,
    relative_path: &std::path::Path,
) -> Option<ParsedFile> {
    let language = language_for_path(relative_path)?;
    let file = crate::FileEntry {
        path: relative_path.to_path_buf(),
        language,
        size_bytes: 0,
        content_hash: [0; 32],
        source_bytes: None,
    };

    crate::tree_sitter::parse_file(&parsed.file_node.repo, repo_root, &file).ok()
}

thread_local! {
    static ACTIVE_SHARED_LIB_PATHS: RefCell<BTreeSet<PathBuf>> = const { RefCell::new(BTreeSet::new()) };
}

struct SharedLibParseGuard {
    path: PathBuf,
}

impl SharedLibParseGuard {
    fn enter(path: &Path) -> Option<Self> {
        ACTIVE_SHARED_LIB_PATHS.with(|active| {
            let mut active = active.borrow_mut();
            if !active.insert(path.to_path_buf()) {
                return None;
            }
            Some(Self {
                path: path.to_path_buf(),
            })
        })
    }
}

impl Drop for SharedLibParseGuard {
    fn drop(&mut self) {
        ACTIVE_SHARED_LIB_PATHS.with(|active| {
            active.borrow_mut().remove(&self.path);
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

fn language_for_path(path: &std::path::Path) -> Option<crate::Language> {
    let extension = path.extension()?.to_str()?;
    match extension {
        "ts" | "tsx" => Some(crate::Language::TypeScript),
        "js" | "jsx" | "mjs" | "cjs" => Some(crate::Language::JavaScript),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Node construction helpers
// ---------------------------------------------------------------------------

/// Construct a virtual [`NodeData`] using provenance from an
/// [`EnrichedCallSite`].  The `NodeId` is derived deterministically from
/// `kind` + `qualified_name` via [`ref_node_id`] so multiple call sites
/// targeting the same logical resource produce the same node ID.
fn virtual_node_from_call_site(
    kind: NodeKind,
    qualified_name: &str,
    name: &str,
    parsed: &ParsedFile,
    call_site: &crate::tree_sitter::EnrichedCallSite,
) -> NodeData {
    NodeData {
        id: ref_node_id(kind, qualified_name),
        kind,
        repo: parsed.file_node.repo.clone(),
        file_path: parsed.file_node.file_path.clone(),
        name: name.to_owned(),
        qualified_name: Some(qualified_name.to_owned()),
        external_id: Some(qualified_name.to_owned()),
        signature: None,
        visibility: None,
        span: call_site.span.clone(),
        is_virtual: true,
    }
}

/// Construct a virtual [`NodeData`] using provenance from an
/// [`ImportBinding`].  No span information is available from bindings, so
/// `span` is `None`.
fn virtual_node_from_binding(
    kind: NodeKind,
    qualified_name: &str,
    name: &str,
    parsed: &ParsedFile,
    _binding: &ImportBinding,
) -> NodeData {
    NodeData {
        id: ref_node_id(kind, qualified_name),
        kind,
        repo: parsed.file_node.repo.clone(),
        file_path: parsed.file_node.file_path.clone(),
        name: name.to_owned(),
        qualified_name: Some(qualified_name.to_owned()),
        external_id: Some(qualified_name.to_owned()),
        signature: None,
        visibility: None,
        span: None,
        is_virtual: true,
    }
}

// ---------------------------------------------------------------------------
// String utilities
// ---------------------------------------------------------------------------

/// Extract the last dot-separated segment from a qualified hint string.
///
/// `"this.serviceBusSender.send"` → `"send"`.
/// `"send"` → `"send"` (no dot present).
fn last_segment(hint: &str) -> &str {
    hint.rsplit('.').next().unwrap_or(hint)
}

/// Strip surrounding quotes, brackets, and whitespace from a raw string
/// literal captured by the tree-sitter extractor.
fn sanitize_name(value: &str) -> String {
    value
        .trim()
        .trim_matches('[')
        .trim_matches(']')
        .trim_matches('"')
        .trim_matches('\'')
        .trim()
        .to_owned()
}

/// Case-insensitive ASCII substring check that avoids heap allocation.
///
/// Uses a sliding window of `needle.len()` bytes and compares each window
/// against the needle with `eq_ignore_ascii_case`.
fn contains_ignore_ascii_case(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    let n = needle.len();
    haystack
        .as_bytes()
        .windows(n)
        .any(|window| window.eq_ignore_ascii_case(needle.as_bytes()))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![expect(clippy::needless_raw_string_hashes)]

    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use crate::{Language, frameworks::Framework, tree_sitter::parse_file_with_frameworks};

    // Tests in this module target the Azure extractor specifically, so they
    // bypass repo-level framework detection and always pass `Framework::Azure`.
    fn parse_file(
        repo: &str,
        repo_root: &std::path::Path,
        file: &crate::FileEntry,
    ) -> Result<crate::ParsedFile, crate::ParseError> {
        parse_file_with_frameworks(repo, repo_root, file, &[Framework::Azure])
    }

    static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> Self {
            let counter = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-parser-azure-{name}-{}-{counter}",
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

    // ------------------------------------------------------------------
    // Service Bus
    // ------------------------------------------------------------------

    #[test]
    fn service_bus_send_produces_topic_node() {
        // `this.serviceBusSender.send('orders')` should produce:
        //   - a Topic virtual node with QN `__topic__servicebus__orders`
        //   - a Publishes edge from the owning function to that node
        let temp_dir = TestDir::new("sb-send");
        fs::write(
            temp_dir.path().join("sender.ts"),
            r#"
export class OrderPublisher {
  async publishOrder() {
    this.serviceBusSender.send('orders');
  }
}
"#,
        )
        .expect("sender fixture should write");

        let parsed = parse_file(
            "order-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "sender.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("sender fixture should parse");

        let topic_nodes: Vec<_> = parsed
            .nodes
            .iter()
            .filter(|n| n.kind == gather_step_core::NodeKind::Topic)
            .collect();
        let publishes_edges: Vec<_> = parsed
            .edges
            .iter()
            .filter(|e| e.kind == gather_step_core::EdgeKind::Publishes)
            .collect();

        assert!(
            topic_nodes
                .iter()
                .any(|n| n.external_id.as_deref() == Some("__topic__servicebus__orders")),
            "expected __topic__servicebus__orders, got: {topic_nodes:?}"
        );
        assert!(
            !publishes_edges.is_empty(),
            "expected at least one Publishes edge, got none"
        );
        // The edge target must point at the orders topic node.
        let orders_node = topic_nodes
            .iter()
            .find(|n| n.external_id.as_deref() == Some("__topic__servicebus__orders"))
            .expect("orders topic node must exist");
        assert!(
            publishes_edges.iter().any(|e| e.target == orders_node.id),
            "Publishes edge must target the orders topic node"
        );
    }

    #[test]
    fn service_bus_receive_produces_consumes_edge() {
        // `this.serviceBusReceiver.subscribe('orders')` should produce:
        //   - a Topic virtual node with QN `__topic__servicebus__orders`
        //   - a Consumes edge from the owning function to that node
        let temp_dir = TestDir::new("sb-receive");
        fs::write(
            temp_dir.path().join("receiver.ts"),
            r#"
export class OrderConsumer {
  async startConsuming() {
    this.serviceBusReceiver.subscribe('orders');
  }
}
"#,
        )
        .expect("receiver fixture should write");

        let parsed = parse_file(
            "order-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "receiver.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("receiver fixture should parse");

        let topic_nodes: Vec<_> = parsed
            .nodes
            .iter()
            .filter(|n| n.kind == gather_step_core::NodeKind::Topic)
            .collect();
        let consumes_edges: Vec<_> = parsed
            .edges
            .iter()
            .filter(|e| e.kind == gather_step_core::EdgeKind::Consumes)
            .collect();

        assert!(
            topic_nodes
                .iter()
                .any(|n| n.external_id.as_deref() == Some("__topic__servicebus__orders")),
            "expected __topic__servicebus__orders, got: {topic_nodes:?}"
        );
        assert!(
            !consumes_edges.is_empty(),
            "expected at least one Consumes edge, got none"
        );
        let orders_node = topic_nodes
            .iter()
            .find(|n| n.external_id.as_deref() == Some("__topic__servicebus__orders"))
            .expect("orders topic node must exist");
        assert!(
            consumes_edges.iter().any(|e| e.target == orders_node.id),
            "Consumes edge must target the orders topic node"
        );
    }

    // ------------------------------------------------------------------
    // LaunchDarkly
    // ------------------------------------------------------------------

    #[test]
    fn launchdarkly_variation_produces_flag_node() {
        // `ldClient.variation('enable-feature', false)` should produce:
        //   - a Service virtual node with QN `__feature_flag__enable-feature`
        //   - a References edge from the owning function to that node
        let temp_dir = TestDir::new("ld-variation");
        fs::write(
            temp_dir.path().join("flags.ts"),
            r#"
export class FeatureService {
  check() {
    const enabled = ldClient.variation('enable-feature', false);
    return enabled;
  }
}
"#,
        )
        .expect("flags fixture should write");

        let parsed = parse_file(
            "feature-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "flags.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("flags fixture should parse");

        let flag_nodes: Vec<_> = parsed
            .nodes
            .iter()
            .filter(|n| n.kind == gather_step_core::NodeKind::Service)
            .filter(|n| {
                n.external_id
                    .as_deref()
                    .unwrap_or("")
                    .starts_with("__feature_flag__")
            })
            .collect();
        let references_edges: Vec<_> = parsed
            .edges
            .iter()
            .filter(|e| e.kind == gather_step_core::EdgeKind::References)
            .collect();

        assert!(
            flag_nodes
                .iter()
                .any(|n| n.external_id.as_deref() == Some("__feature_flag__enable-feature")),
            "expected __feature_flag__enable-feature, got: {flag_nodes:?}"
        );
        assert!(
            !references_edges.is_empty(),
            "expected at least one References edge, got none"
        );
        let flag_node = flag_nodes
            .iter()
            .find(|n| n.external_id.as_deref() == Some("__feature_flag__enable-feature"))
            .expect("enable-feature flag node must exist");
        assert!(
            references_edges.iter().any(|e| e.target == flag_node.id),
            "References edge must target the enable-feature flag node"
        );
    }

    // ------------------------------------------------------------------
    // Web PubSub
    // ------------------------------------------------------------------

    #[test]
    fn web_pubsub_send_produces_event_node() {
        // `this.webPubSubClient.sendToAll('notification')` should produce:
        //   - an Event virtual node with QN `__event__pubsub__notification`
        //   - a Publishes edge from the owning function to that node
        let temp_dir = TestDir::new("pubsub-send");
        fs::write(
            temp_dir.path().join("publisher.ts"),
            r#"
export class NotificationPublisher {
  async broadcast() {
    this.webPubSubClient.sendToAll('notification');
  }
}
"#,
        )
        .expect("publisher fixture should write");

        let parsed = parse_file(
            "service-b",
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

        let event_nodes: Vec<_> = parsed
            .nodes
            .iter()
            .filter(|n| n.kind == gather_step_core::NodeKind::Event)
            .collect();
        let publishes_edges: Vec<_> = parsed
            .edges
            .iter()
            .filter(|e| e.kind == gather_step_core::EdgeKind::Publishes)
            .collect();

        assert!(
            event_nodes
                .iter()
                .any(|n| n.external_id.as_deref() == Some("__event__pubsub__notification")),
            "expected __event__pubsub__notification, got: {event_nodes:?}"
        );
        assert!(
            !publishes_edges.is_empty(),
            "expected at least one Publishes edge, got none"
        );
        let notif_node = event_nodes
            .iter()
            .find(|n| n.external_id.as_deref() == Some("__event__pubsub__notification"))
            .expect("notification event node must exist");
        assert!(
            publishes_edges.iter().any(|e| e.target == notif_node.id),
            "Publishes edge must target the notification event node"
        );
    }

    // ------------------------------------------------------------------
    // shared-lib shared symbol
    // ------------------------------------------------------------------

    #[test]
    fn shared_lib_import_produces_shared_symbol() {
        // `import { ProductDto } from '@example/shared-lib/dtos'` should produce:
        //   - a SharedSymbol virtual node with QN `__shared__@example/shared-lib__ProductDto`
        //   - a References edge from the file node to the shared symbol node
        let temp_dir = TestDir::new("shared-lib");
        fs::write(
            temp_dir.path().join("handler.ts"),
            r#"
import { ProductDto } from '@example/shared-lib/dtos';

export class ProductHandler {
  handle(dto: ProductDto) {
    return dto;
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

        let shared_nodes: Vec<_> = parsed
            .nodes
            .iter()
            .filter(|n| n.kind == gather_step_core::NodeKind::SharedSymbol)
            .collect();
        let references_edges: Vec<_> = parsed
            .edges
            .iter()
            .filter(|e| e.kind == gather_step_core::EdgeKind::References)
            .collect();

        assert!(
            shared_nodes.iter().any(|n| {
                n.external_id.as_deref() == Some("__shared__@example/shared-lib__ProductDto")
            }),
            "expected __shared__@example/shared-lib__ProductDto, got: {shared_nodes:?}"
        );
        assert!(
            !references_edges.is_empty(),
            "expected at least one References edge, got none"
        );
        let product_dto_node = shared_nodes
            .iter()
            .find(|n| n.external_id.as_deref() == Some("__shared__@example/shared-lib__ProductDto"))
            .expect("ProductDto shared node must exist");
        assert!(
            references_edges
                .iter()
                .any(|e| e.target == product_dto_node.id),
            "References edge must target the ProductDto shared symbol node"
        );
        // The edge must originate from the file node (not a function inside it)
        assert!(
            references_edges
                .iter()
                .any(|e| e.source == parsed.file_node.id && e.target == product_dto_node.id),
            "References edge source must be the file node"
        );
    }

    #[test]
    fn shared_lib_barrel_alias_produces_canonical_package_symbol() {
        let temp_dir = TestDir::new("shared-lib-barrel");
        fs::write(
            temp_dir.path().join("shared_barrel.ts"),
            r#"
export { ProductDto as Product } from '@example/shared-lib/dtos';
"#,
        )
        .expect("barrel fixture should write");
        fs::write(
            temp_dir.path().join("handler.ts"),
            r#"
import { Product } from './shared_barrel';

export class ProductHandler {
  handle(dto: Product) {
    return dto;
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

        let shared_nodes: Vec<_> = parsed
            .nodes
            .iter()
            .filter(|n| n.kind == gather_step_core::NodeKind::SharedSymbol)
            .collect();
        let references_edges: Vec<_> = parsed
            .edges
            .iter()
            .filter(|e| e.kind == gather_step_core::EdgeKind::References)
            .collect();

        let product_dto_node = shared_nodes
            .iter()
            .find(|n| n.external_id.as_deref() == Some("__shared__@example/shared-lib__ProductDto"))
            .expect("ProductDto shared node must exist through the barrel");
        assert!(
            references_edges
                .iter()
                .any(|e| e.source == parsed.file_node.id && e.target == product_dto_node.id),
            "References edge should point at the canonical shared symbol through the barrel"
        );
    }

    #[test]
    fn shared_lib_cyclic_barrels_do_not_recurse_forever() {
        let temp_dir = TestDir::new("shared-lib-cycle");
        fs::write(
            temp_dir.path().join("a.ts"),
            r#"
export { ProductDto } from './b';
"#,
        )
        .expect("barrel a fixture should write");
        fs::write(
            temp_dir.path().join("b.ts"),
            r#"
export { ProductDto } from './a';
export { ProductDto } from '@example/shared-lib/dtos';
"#,
        )
        .expect("barrel b fixture should write");
        fs::write(
            temp_dir.path().join("handler.ts"),
            r#"
import { ProductDto } from './a';

export class ProductHandler {
  handle(dto: ProductDto) {
    return dto;
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
        .expect("handler fixture should parse without overflowing");

        let product_dto_node = parsed
            .nodes
            .iter()
            .find(|n| n.external_id.as_deref() == Some("__shared__@example/shared-lib__ProductDto"))
            .expect("ProductDto shared node must resolve through cyclic barrels");
        assert!(
            parsed
                .edges
                .iter()
                .any(|e| e.source == parsed.file_node.id && e.target == product_dto_node.id),
            "References edge should resolve through cyclic barrels"
        );
    }
}
