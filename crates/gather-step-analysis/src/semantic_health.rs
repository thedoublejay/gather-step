use std::collections::{BTreeMap, BTreeSet};

use gather_step_core::{EdgeKind, NodeKind};
use gather_step_storage::{GraphStore, MetadataStore, MetadataStoreError, PayloadContractQuery};

use crate::event_topology::list_orphan_topics_paged;
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SemanticHealthError {
    #[error(transparent)]
    Graph(#[from] gather_step_storage::GraphStoreError),
    #[error(transparent)]
    Metadata(#[from] MetadataStoreError),
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SemanticHealthReport {
    pub route_links: SemanticLinkHealth,
    pub event_links: SemanticLinkHealth,
    pub shared_symbol_links: SemanticLinkHealth,
    pub payload_contract_links: SemanticLinkHealth,
    pub orphan_topics: usize,
    pub unresolved_call_inputs: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SemanticLinkHealth {
    pub total_targets: usize,
    pub linked_targets: usize,
    pub partially_linked_targets: usize,
    pub unlinked_targets: usize,
    pub ambiguous_targets: usize,
    pub coverage_ratio: f32,
}

pub fn semantic_health_for_repo(
    graph: &impl GraphStore,
    metadata: &impl MetadataStore,
    repo: &str,
    unresolved_call_inputs: usize,
) -> Result<SemanticHealthReport, SemanticHealthError> {
    // Routes use the loose "any qualifying edge = linked" classifier so a
    // real-workspace route with a server but no indexed client (common:
    // external callers, CLIs, services not yet indexed) does not collapse
    // into a failing `partial` status.
    let route_links = classify_attached_virtual_targets(
        graph,
        repo,
        &[NodeKind::Route],
        &[EdgeKind::Serves, EdgeKind::ConsumesApiFrom],
    )?;
    let event_links = classify_attached_virtual_targets(
        graph,
        repo,
        &[
            NodeKind::Topic,
            NodeKind::Queue,
            NodeKind::Subject,
            NodeKind::Stream,
            NodeKind::Event,
        ],
        // The NestJS parser emits `Consumes` from `@MessagePattern` /
        // `@EventPattern` / `@CustomEventPattern` handlers and `Publishes`
        // from `bus.send(...)`-style producers (see
        // `gather-step-parser/src/frameworks/nestjs.rs`). Excluding them
        // classifies legitimate event consumers/producers as unlinked. Keep
        // the stronger typed edges alongside so confirmed-contract events
        // still count.
        &[
            EdgeKind::ProducesEventFor,
            EdgeKind::UsesEventFrom,
            EdgeKind::ContractOn,
            EdgeKind::Consumes,
            EdgeKind::Publishes,
        ],
    )?;
    let shared_symbol_links = classify_attached_virtual_targets(
        graph,
        repo,
        &[NodeKind::SharedSymbol],
        // The accepted edge set must cover every legitimate way the parsers
        // attach a SharedSymbol virtual node, otherwise doctor reports false
        // "shared symbol links incomplete" failures (see the committed-fixture
        // integration regression this guards against):
        //   - `References`: code imports of a shared-lib symbol via
        //     `add_shared_lib_edges` in
        //     `gather-step-parser/src/frameworks/azure.rs`.
        //   - `UsesTypeFrom`, `UsesShared`, `ImplementsContractFrom`: stronger
        //     shared-contract edges from cross-repo type and contract
        //     resolution.
        //   - `CrossRepoDepends`: package-level shared symbols emitted from
        //     `manifests.rs` (`__shared__<pkg>__package`) only ever receive
        //     this edge.
        &[
            EdgeKind::References,
            EdgeKind::UsesTypeFrom,
            EdgeKind::UsesShared,
            EdgeKind::ImplementsContractFrom,
            EdgeKind::CrossRepoDepends,
            // `__guard__*` SharedSymbols emitted by the NestJS cross-repo
            // `@UseGuards` path receive only `UsesGuardFrom` edges from their
            // consumers. Without this kind in the accepted set, those virtual
            // guards show up as unlinked shared symbols and trip the
            // committed-fixture integration test.
            EdgeKind::UsesGuardFrom,
            // `__hook__<pkg>::<name>` SharedSymbols emitted by the
            // `FrontendHooks` augmenter receive only `ConsumesHookFrom` edges.
            // Adding this kind prevents doctor from classifying cross-package
            // hook imports as unlinked.
            EdgeKind::ConsumesHookFrom,
        ],
    )?;
    let payload_contract_links = classify_payload_contracts(metadata, Some(repo))?;

    Ok(SemanticHealthReport {
        route_links,
        event_links,
        shared_symbol_links,
        payload_contract_links,
        orphan_topics: count_orphan_topics(graph, Some(repo))?,
        unresolved_call_inputs,
    })
}

pub fn semantic_health_for_workspace(
    graph: &impl GraphStore,
    metadata: &impl MetadataStore,
) -> Result<SemanticHealthReport, SemanticHealthError> {
    // See `semantic_health_for_repo` — routes use the loose classifier for
    // the same reason (server without indexed client is not a bug).
    let route_links = classify_attached_virtual_targets(
        graph,
        "",
        &[NodeKind::Route],
        &[EdgeKind::Serves, EdgeKind::ConsumesApiFrom],
    )?;
    let event_links = classify_attached_virtual_targets(
        graph,
        "",
        &[
            NodeKind::Topic,
            NodeKind::Queue,
            NodeKind::Subject,
            NodeKind::Stream,
            NodeKind::Event,
        ],
        // The NestJS parser emits `Consumes` from `@MessagePattern` /
        // `@EventPattern` / `@CustomEventPattern` handlers and `Publishes`
        // from `bus.send(...)`-style producers (see
        // `gather-step-parser/src/frameworks/nestjs.rs`). Excluding them
        // classifies legitimate event consumers/producers as unlinked. Keep
        // the stronger typed edges alongside so confirmed-contract events
        // still count.
        &[
            EdgeKind::ProducesEventFor,
            EdgeKind::UsesEventFrom,
            EdgeKind::ContractOn,
            EdgeKind::Consumes,
            EdgeKind::Publishes,
        ],
    )?;
    let shared_symbol_links = classify_attached_virtual_targets(
        graph,
        "",
        &[NodeKind::SharedSymbol],
        // The accepted edge set must cover every legitimate way the parsers
        // attach a SharedSymbol virtual node, otherwise doctor reports false
        // "shared symbol links incomplete" failures (see the committed-fixture
        // integration regression this guards against):
        //   - `References`: code imports of a shared-lib symbol via
        //     `add_shared_lib_edges` in
        //     `gather-step-parser/src/frameworks/azure.rs`.
        //   - `UsesTypeFrom`, `UsesShared`, `ImplementsContractFrom`: stronger
        //     shared-contract edges from cross-repo type and contract
        //     resolution.
        //   - `CrossRepoDepends`: package-level shared symbols emitted from
        //     `manifests.rs` (`__shared__<pkg>__package`) only ever receive
        //     this edge.
        &[
            EdgeKind::References,
            EdgeKind::UsesTypeFrom,
            EdgeKind::UsesShared,
            EdgeKind::ImplementsContractFrom,
            EdgeKind::CrossRepoDepends,
            // `__guard__*` SharedSymbols emitted by the NestJS cross-repo
            // `@UseGuards` path receive only `UsesGuardFrom` edges from their
            // consumers. Without this kind in the accepted set, those virtual
            // guards show up as unlinked shared symbols and trip the
            // committed-fixture integration test.
            EdgeKind::UsesGuardFrom,
            // `__hook__<pkg>::<name>` SharedSymbols emitted by the
            // `FrontendHooks` augmenter receive only `ConsumesHookFrom` edges.
            // Adding this kind prevents doctor from classifying cross-package
            // hook imports as unlinked.
            EdgeKind::ConsumesHookFrom,
        ],
    )?;
    let payload_contract_links = classify_payload_contracts(metadata, None)?;

    Ok(SemanticHealthReport {
        route_links,
        event_links,
        shared_symbol_links,
        payload_contract_links,
        orphan_topics: count_orphan_topics(graph, None)?,
        unresolved_call_inputs: 0,
    })
}

fn classify_attached_virtual_targets(
    graph: &impl GraphStore,
    repo: &str,
    kinds: &[NodeKind],
    relevant_edges: &[EdgeKind],
) -> Result<SemanticLinkHealth, SemanticHealthError> {
    let mut total_targets = 0_usize;
    let mut linked_targets = 0_usize;
    let partially_linked_targets = 0_usize;
    let mut unlinked_targets = 0_usize;
    let mut duplicate_tracker = BTreeMap::<(NodeKind, String), usize>::new();

    for &kind in kinds {
        for node in graph.nodes_by_type(kind)? {
            if !node.is_virtual || !virtual_target_in_scope(graph, &node, repo)? {
                continue;
            }
            total_targets += 1;
            if let Some(external_id) = node.external_id.as_ref() {
                *duplicate_tracker
                    .entry((kind, external_id.clone()))
                    .or_default() += 1;
            }

            let has_relevant_edge = graph
                .get_incoming(node.id)?
                .iter()
                .any(|edge| relevant_edges.contains(&edge.kind))
                || graph
                    .get_outgoing(node.id)?
                    .iter()
                    .any(|edge| relevant_edges.contains(&edge.kind));
            if has_relevant_edge {
                linked_targets += 1;
            } else {
                unlinked_targets += 1;
            }
        }
    }

    let ambiguous_targets = duplicate_tracker
        .into_values()
        .filter(|count| *count > 1)
        .map(|count| count - 1)
        .sum();

    Ok(SemanticLinkHealth {
        total_targets,
        linked_targets,
        partially_linked_targets,
        unlinked_targets,
        ambiguous_targets,
        coverage_ratio: coverage_ratio(linked_targets, total_targets),
    })
}

fn virtual_target_in_scope(
    graph: &impl GraphStore,
    node: &gather_step_core::NodeData,
    repo: &str,
) -> Result<bool, SemanticHealthError> {
    if repo.is_empty() || node.repo == repo {
        return Ok(true);
    }

    for edge in graph
        .get_incoming(node.id)?
        .into_iter()
        .chain(graph.get_outgoing(node.id)?)
    {
        let other_id = if edge.source == node.id {
            edge.target
        } else {
            edge.source
        };
        if let Some(other) = graph.get_node(other_id)?
            && !other.is_virtual
            && other.repo == repo
        {
            return Ok(true);
        }
        if let Some(owner) = graph.get_node(edge.owner_file)?
            && owner.kind == NodeKind::File
            && owner.repo == repo
        {
            return Ok(true);
        }
    }

    Ok(false)
}

fn classify_payload_contracts(
    metadata: &impl MetadataStore,
    repo: Option<&str>,
) -> Result<SemanticLinkHealth, SemanticHealthError> {
    // Track two things per target QN:
    //   (workspace-wide producer flag, workspace-wide consumer flag, distinct target node-ids)
    // — so we can judge "is this contract linked somewhere in the workspace?"
    //
    // When scoped to a specific repo, we additionally collect which repos the
    // target touches on each side, and only count targets this repo
    // participates in. This fixes a prior bug where a producer in repo A plus
    // a consumer in repo B showed as `partially_linked=1` in **both** per-repo
    // doctor reports even though the contract is fully linked workspace-wide.
    let mut groups = BTreeMap::<
        String,
        (
            /* has_producer_anywhere */ bool,
            /* has_consumer_anywhere */ bool,
            /* distinct target node-ids */ BTreeSet<Vec<u8>>,
            /* producer side lives in this repo */ bool,
            /* consumer side lives in this repo */ bool,
        ),
    >::new();
    for row in metadata.payload_contracts_for_query(PayloadContractQuery::default())? {
        let key = row
            .record
            .contract_target_qualified_name
            .clone()
            .unwrap_or_else(|| format!("{:?}", row.record.contract_target_node_id.0));
        let entry = groups
            .entry(key)
            .or_insert((false, false, BTreeSet::new(), false, false));
        let in_scope = repo.is_none_or(|wanted| row.record.repo == wanted);
        match row.record.side {
            gather_step_core::PayloadSide::Producer => {
                entry.0 = true;
                if in_scope {
                    entry.3 = true;
                }
            }
            gather_step_core::PayloadSide::Consumer => {
                entry.1 = true;
                if in_scope {
                    entry.4 = true;
                }
            }
        }
        entry
            .2
            .insert(row.record.contract_target_node_id.0.to_vec());
    }

    // When scoped to a repo, keep only targets this repo participates in.
    // Workspace-scope keeps every group.
    let scoped = groups
        .values()
        .filter(|(_, _, _, in_repo_producer, in_repo_consumer)| {
            repo.is_none() || *in_repo_producer || *in_repo_consumer
        });

    let mut total_targets = 0_usize;
    let mut linked_targets = 0_usize;
    let mut partially_linked_targets = 0_usize;
    let mut unlinked_targets = 0_usize;
    let mut ambiguous_targets = 0_usize;
    for (has_producer, has_consumer, ids, _, _) in scoped {
        total_targets += 1;
        if *has_producer && *has_consumer {
            linked_targets += 1;
        } else if *has_producer || *has_consumer {
            partially_linked_targets += 1;
        } else {
            unlinked_targets += 1;
        }
        if ids.len() > 1 {
            ambiguous_targets += 1;
        }
    }

    Ok(SemanticLinkHealth {
        total_targets,
        linked_targets,
        partially_linked_targets,
        unlinked_targets,
        ambiguous_targets,
        coverage_ratio: coverage_ratio(linked_targets, total_targets),
    })
}

#[expect(
    clippy::cast_precision_loss,
    reason = "coverage_ratio is UI/reporting metadata and intentionally approximate"
)]
fn coverage_ratio(linked_targets: usize, total_targets: usize) -> f32 {
    if total_targets == 0 {
        return 1.0;
    }

    let linked_targets = u32::try_from(linked_targets).unwrap_or(u32::MAX);
    let total_targets = u32::try_from(total_targets).unwrap_or(u32::MAX);
    linked_targets as f32 / total_targets as f32
}

fn count_orphan_topics(
    graph: &impl GraphStore,
    repo: Option<&str>,
) -> Result<usize, SemanticHealthError> {
    // Delegate to the canonical paged enumerator so that both count and list
    // paths share identical classification logic and cannot disagree on the
    // same graph.
    //
    // EventTopologyError wraps GraphStoreError via #[from]; unwrap it to the
    // underlying store error so SemanticHealthError::Graph can hold it.
    let page = list_orphan_topics_paged(graph, repo, usize::MAX).map_err(|e| {
        let crate::event_topology::EventTopologyError::Store(store_err) = e;
        SemanticHealthError::Graph(store_err)
    })?;
    Ok(page.items.len())
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::{
        EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, SourceSpan, Visibility, node_id,
        ref_node_id,
    };
    use gather_step_storage::{GraphStore, GraphStoreDb};

    use crate::event_topology::tests::{
        build_orphan_fixture_for_truncation_test, list_orphan_topics_paged,
    };

    use super::{classify_attached_virtual_targets, count_orphan_topics};

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new(name: &str) -> Self {
            let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-semantic-health-{name}-{}-{id}.redb",
                process::id()
            ));
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
        }
    }

    fn file_node(repo: &str, file_path: &str) -> NodeData {
        NodeData {
            id: node_id(repo, file_path, NodeKind::File, file_path),
            kind: NodeKind::File,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: file_path.to_owned(),
            qualified_name: Some(format!("{repo}::{file_path}")),
            external_id: None,
            signature: None,
            visibility: None,
            span: None,
            is_virtual: false,
        }
    }

    fn function_node(repo: &str, file_path: &str, name: &str, _ordinal: u16) -> NodeData {
        NodeData {
            id: node_id(repo, file_path, NodeKind::Function, name),
            kind: NodeKind::Function,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(format!("{repo}::{name}")),
            external_id: None,
            signature: Some(format!("{name}()")),
            visibility: Some(Visibility::Public),
            span: Some(SourceSpan {
                line_start: 1,
                line_len: 0,
                column_start: 0,
                column_len: 4,
            }),
            is_virtual: false,
        }
    }

    fn shared_symbol_node(repo: &str, file_path: &str, name: &str, qn: &str) -> NodeData {
        NodeData {
            id: ref_node_id(NodeKind::SharedSymbol, qn),
            kind: NodeKind::SharedSymbol,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(qn.to_owned()),
            external_id: Some(qn.to_owned()),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        }
    }

    fn event_node(repo: &str, file_path: &str, qn: &str) -> NodeData {
        NodeData {
            id: ref_node_id(NodeKind::Event, qn),
            kind: NodeKind::Event,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: qn.to_owned(),
            qualified_name: Some(qn.to_owned()),
            external_id: Some(qn.to_owned()),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        }
    }

    #[test]
    fn count_orphan_topics_agrees_with_paged_list() {
        let store = build_orphan_fixture_for_truncation_test();
        let count = count_orphan_topics(&store, None).expect("count ok");
        let page = list_orphan_topics_paged(&store, None, usize::MAX).expect("list ok");
        assert_eq!(count, page.items.len(), "count == unbounded list size");
        assert_eq!(count, page.total_seen);
    }

    #[test]
    fn shared_symbol_health_counts_type_edges_as_linked() {
        let temp = TempDb::new("shared-symbol");
        let store = GraphStoreDb::open(temp.path()).expect("store should open");
        let file = file_node("backend_standard", "src/controller.ts");
        let function = function_node("backend_standard", "src/controller.ts", "handle", 0);
        let shared = shared_symbol_node(
            "backend_standard",
            "__shared__@vendor/shared-types__AuditUser",
            "AuditUser",
            "__shared__@vendor/shared-types__AuditUser",
        );
        for node in [&file, &function, &shared] {
            store.insert_node(node).expect("node should insert");
        }
        store
            .insert_edge(&EdgeData {
                source: file.id,
                target: shared.id,
                kind: EdgeKind::UsesTypeFrom,
                metadata: EdgeMetadata::default(),
                owner_file: file.id,
                is_cross_file: true,
            })
            .expect("edge should insert");
        store
            .insert_edge(&EdgeData {
                source: function.id,
                target: shared.id,
                kind: EdgeKind::ImplementsContractFrom,
                metadata: EdgeMetadata::default(),
                owner_file: file.id,
                is_cross_file: false,
            })
            .expect("edge should insert");

        assert!(
            store
                .nodes_by_type(NodeKind::SharedSymbol)
                .expect("shared symbols should load")
                .iter()
                .any(|node| node.id == shared.id),
            "shared symbol should be present in the graph"
        );
        assert!(
            store
                .get_incoming(shared.id)
                .expect("incoming edges should load")
                .iter()
                .any(|edge| edge.kind == EdgeKind::UsesTypeFrom),
            "shared symbol should have incoming type edges"
        );

        let health = classify_attached_virtual_targets(
            &store,
            "backend_standard",
            &[NodeKind::SharedSymbol],
            &[
                EdgeKind::UsesTypeFrom,
                EdgeKind::UsesShared,
                EdgeKind::ImplementsContractFrom,
            ],
        )
        .expect("health should compute");

        assert_eq!(health.total_targets, 1);
        assert_eq!(health.linked_targets, 1);
        assert_eq!(health.unlinked_targets, 0);
    }

    #[test]
    fn event_health_counts_payload_and_consumer_edges_as_linked() {
        let temp = TempDb::new("event");
        let store = GraphStoreDb::open(temp.path()).expect("store should open");
        let file = file_node("backend_standard", "src/events.ts");
        let producer = function_node("backend_standard", "src/events.ts", "emit", 0);
        let payload = NodeData {
            id: ref_node_id(
                NodeKind::PayloadContract,
                "__payload__backend_standard__src/events.ts__order.created",
            ),
            kind: NodeKind::PayloadContract,
            repo: "backend_standard".to_owned(),
            file_path: "src/events.ts".to_owned(),
            name: "__payload__backend_standard__src/events.ts__order.created".to_owned(),
            qualified_name: Some(
                "__payload__backend_standard__src/events.ts__order.created".to_owned(),
            ),
            external_id: Some(
                "__payload__backend_standard__src/events.ts__order.created".to_owned(),
            ),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        };
        let event = event_node(
            "backend_standard",
            "__event__kafka__order.created",
            "__event__kafka__order.created",
        );
        for node in [&file, &producer, &payload, &event] {
            store.insert_node(node).expect("node should insert");
        }
        store
            .insert_edge(&EdgeData {
                source: producer.id,
                target: event.id,
                kind: EdgeKind::UsesEventFrom,
                metadata: EdgeMetadata::default(),
                owner_file: file.id,
                is_cross_file: false,
            })
            .expect("edge should insert");
        store
            .insert_edge(&EdgeData {
                source: payload.id,
                target: event.id,
                kind: EdgeKind::ContractOn,
                metadata: EdgeMetadata::default(),
                owner_file: file.id,
                is_cross_file: false,
            })
            .expect("edge should insert");

        let health = classify_attached_virtual_targets(
            &store,
            "backend_standard",
            &[NodeKind::Event],
            &[
                EdgeKind::ProducesEventFor,
                EdgeKind::UsesEventFrom,
                EdgeKind::ContractOn,
            ],
        )
        .expect("health should compute");

        assert_eq!(health.total_targets, 1);
        assert_eq!(health.linked_targets, 1);
        assert_eq!(health.unlinked_targets, 0);
    }
}
