use anyhow::{Result, bail};
use clap::{Args, Subcommand};
use gather_step_analysis::{
    event_blast_radius, list_orphan_topics, rank_event_targets, resolve_event_targets, trace_event,
};
use gather_step_core::NodeData;
use gather_step_storage::GraphStore;
use gather_step_storage::StorageCoordinator;
use serde::Serialize;
use serde_json::json;

use crate::command_render::RenderedCommand;
use crate::daemon_protocol::DaemonRequest;
use crate::{app::AppContext, daemon_proxy};

#[derive(Debug, Args)]
pub struct EventsArgs {
    #[command(subcommand)]
    pub command: EventsCommand,
}

#[derive(Debug, Subcommand)]
pub enum EventsCommand {
    Trace(TraceArgs),
    BlastRadius(BlastRadiusArgs),
    Orphans(OrphansArgs),
}

#[derive(Debug, Args)]
pub struct TraceArgs {
    #[arg(help = "Event/topic identifier or suffix to trace")]
    pub subject: String,
    #[arg(long, default_value_t = 20, help = "Maximum matches per section")]
    pub limit: usize,
}

#[derive(Debug, Args)]
pub struct BlastRadiusArgs {
    #[arg(help = "Event/topic identifier or suffix to trace")]
    pub subject: String,
    #[arg(long, default_value_t = 20, help = "Maximum nodes to return")]
    pub limit: usize,
    #[arg(long, default_value_t = 2, help = "Blast-radius traversal depth")]
    pub depth: usize,
}

#[derive(Debug, Args)]
pub struct OrphansArgs {
    #[arg(long, default_value_t = 20, help = "Maximum orphan targets to return")]
    pub limit: usize,
}

#[derive(Debug, Serialize)]
struct EventsOutput {
    event: &'static str,
    target: EventTargetOutput,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    alternates: Vec<EventTargetOutput>,
    producers: Vec<TopologyMatchOutput>,
    consumers: Vec<TopologyMatchOutput>,
    blast_radius: Vec<BlastRadiusNodeOutput>,
    truncated: bool,
}

#[derive(Debug, Serialize)]
struct EventTargetOutput {
    repo: String,
    file_path: String,
    name: String,
    node_kind: String,
}

#[derive(Debug, Serialize)]
struct TopologyMatchOutput {
    repo: String,
    file_path: String,
    line_number: Option<u32>,
    symbol_name: String,
    node_kind: String,
    edge_kind: String,
    confidence: Option<u16>,
}

#[derive(Debug, Serialize)]
struct BlastRadiusNodeOutput {
    repo: String,
    file_path: String,
    line_number: Option<u32>,
    name: String,
    node_kind: String,
    depth: usize,
    cumulative_confidence: Option<u16>,
}

pub fn run(app: &AppContext, args: EventsArgs) -> Result<()> {
    let request = daemon_request(&args, app);
    daemon_proxy::run_read_only_command(app, &request, move |app| run_rendered(app, args))
}

pub(crate) fn run_rendered(app: &AppContext, args: EventsArgs) -> Result<RenderedCommand> {
    match args.command {
        EventsCommand::Trace(args) => run_trace_rendered(app, &args),
        EventsCommand::BlastRadius(args) => run_blast_radius_rendered(app, &args),
        EventsCommand::Orphans(args) => run_orphans_rendered(app, &args),
    }
}

fn daemon_request(args: &EventsArgs, app: &AppContext) -> DaemonRequest {
    match &args.command {
        EventsCommand::Trace(args) => DaemonRequest::EventsTrace {
            subject: args.subject.clone(),
            limit: args.limit,
            repo_filter: app.repo_filter.clone(),
        },
        EventsCommand::BlastRadius(args) => DaemonRequest::EventsBlastRadius {
            subject: args.subject.clone(),
            limit: args.limit,
            depth: args.depth,
            repo_filter: app.repo_filter.clone(),
        },
        EventsCommand::Orphans(args) => DaemonRequest::EventsOrphans {
            limit: args.limit,
            repo_filter: app.repo_filter.clone(),
        },
    }
}

fn run_trace_rendered(app: &AppContext, args: &TraceArgs) -> Result<RenderedCommand> {
    let storage = StorageCoordinator::open(app.workspace_paths().storage_root)?;
    execute_trace(&storage, app.repo_filter.as_deref(), args)
}

pub(crate) fn execute_trace(
    storage: &StorageCoordinator,
    repo_filter: Option<&str>,
    args: &TraceArgs,
) -> Result<RenderedCommand> {
    let targets = resolve_event_targets(storage.graph(), &args.subject)?;
    let selection = select_event_target(storage.graph(), targets, repo_filter, &args.subject)?;

    if !selection.target.is_virtual {
        bail!(
            "resolved target `{}` is not a virtual event node",
            selection.target.name
        );
    }

    let trace = trace_event(storage.graph(), selection.target.id, args.limit)?;
    let payload = EventsOutput {
        event: "events_trace_completed",
        target: EventTargetOutput {
            repo: trace.target.repo,
            file_path: trace.target.file_path,
            name: trace.target.name,
            node_kind: trace.target.kind.to_string(),
        },
        alternates: selection.alternates,
        producers: trace
            .producers
            .into_iter()
            .map(|item| TopologyMatchOutput {
                repo: item.repo,
                file_path: item.file_path,
                line_number: item.line_number,
                symbol_name: item.symbol_name,
                node_kind: item.node_kind.to_string(),
                edge_kind: item.edge_kind.to_string(),
                confidence: item.confidence,
            })
            .collect(),
        consumers: trace
            .consumers
            .into_iter()
            .map(|item| TopologyMatchOutput {
                repo: item.repo,
                file_path: item.file_path,
                line_number: item.line_number,
                symbol_name: item.symbol_name,
                node_kind: item.node_kind.to_string(),
                edge_kind: item.edge_kind.to_string(),
                confidence: item.confidence,
            })
            .collect(),
        blast_radius: Vec::new(),
        truncated: trace.truncated,
    };

    let mut lines = vec![format!(
        "Target: {} {}:{}",
        payload.target.name, payload.target.repo, payload.target.file_path
    )];
    lines.push("Producers:".to_owned());
    if payload.producers.is_empty() {
        lines.push("  none".to_owned());
    } else {
        for producer in &payload.producers {
            lines.push(format!(
                "  {} {}:{}",
                producer.symbol_name, producer.repo, producer.file_path
            ));
        }
    }
    lines.push("Consumers:".to_owned());
    if payload.consumers.is_empty() {
        lines.push("  none".to_owned());
    } else {
        for consumer in &payload.consumers {
            lines.push(format!(
                "  {} {}:{}",
                consumer.symbol_name, consumer.repo, consumer.file_path
            ));
        }
    }
    if !payload.alternates.is_empty() {
        lines.push("Alternates:".to_owned());
        for alternate in &payload.alternates {
            lines.push(format!(
                "  {} {}:{}",
                alternate.name, alternate.repo, alternate.file_path
            ));
        }
    }

    Ok(RenderedCommand::success(json!(payload), lines))
}

fn run_blast_radius_rendered(app: &AppContext, args: &BlastRadiusArgs) -> Result<RenderedCommand> {
    let storage = StorageCoordinator::open(app.workspace_paths().storage_root)?;
    execute_blast_radius(&storage, app.repo_filter.as_deref(), args)
}

pub(crate) fn execute_blast_radius(
    storage: &StorageCoordinator,
    repo_filter: Option<&str>,
    args: &BlastRadiusArgs,
) -> Result<RenderedCommand> {
    let targets = resolve_event_targets(storage.graph(), &args.subject)?;
    let selection = select_event_target(storage.graph(), targets, repo_filter, &args.subject)?;

    let blast_radius =
        event_blast_radius(storage.graph(), selection.target.id, args.depth, args.limit)?;
    let payload = EventsOutput {
        event: "events_blast_radius_completed",
        target: EventTargetOutput {
            repo: blast_radius.target.repo,
            file_path: blast_radius.target.file_path,
            name: blast_radius.target.name,
            node_kind: blast_radius.target.kind.to_string(),
        },
        alternates: selection.alternates,
        producers: Vec::new(),
        consumers: Vec::new(),
        blast_radius: blast_radius
            .nodes
            .into_iter()
            .map(|node| BlastRadiusNodeOutput {
                repo: node.repo,
                file_path: node.file_path,
                line_number: node.line_number,
                name: node.name,
                node_kind: node.node_kind.to_string(),
                depth: node.depth,
                cumulative_confidence: node.cumulative_confidence,
            })
            .collect(),
        truncated: blast_radius.truncated,
    };

    let mut lines = vec![format!("Blast radius for {}", payload.target.name)];
    for node in &payload.blast_radius {
        lines.push(format!(
            "  depth={} {} {}:{}",
            node.depth, node.name, node.repo, node.file_path
        ));
    }
    if !payload.alternates.is_empty() {
        lines.push("Alternates:".to_owned());
        for alternate in &payload.alternates {
            lines.push(format!(
                "  {} {}:{}",
                alternate.name, alternate.repo, alternate.file_path
            ));
        }
    }

    Ok(RenderedCommand::success(json!(payload), lines))
}

#[derive(Debug, Serialize)]
struct OrphansOutput {
    event: &'static str,
    orphans: Vec<OrphanOutput>,
}

#[derive(Debug, Serialize)]
struct OrphanOutput {
    name: String,
    kind: String,
    producers: usize,
    consumers: usize,
    classification: String,
    severity: String,
}

fn run_orphans_rendered(app: &AppContext, args: &OrphansArgs) -> Result<RenderedCommand> {
    let storage = StorageCoordinator::open(app.workspace_paths().storage_root)?;
    execute_orphans(&storage, app.repo_filter.as_deref(), args)
}

pub(crate) fn execute_orphans(
    storage: &StorageCoordinator,
    repo_filter: Option<&str>,
    args: &OrphansArgs,
) -> Result<RenderedCommand> {
    let orphans = list_orphan_topics(storage.graph(), repo_filter, args.limit)?;
    let payload = OrphansOutput {
        event: "events_orphans_completed",
        orphans: orphans
            .into_iter()
            .map(|item| OrphanOutput {
                name: item.target.name,
                kind: item.target.kind.to_string(),
                producers: item.producers,
                consumers: item.consumers,
                classification: item.classification.to_owned(),
                severity: item.severity.to_owned(),
            })
            .collect(),
    };
    let mut lines = Vec::new();
    if payload.orphans.is_empty() {
        lines.push("No orphan topics found.".to_owned());
    } else {
        for orphan in &payload.orphans {
            lines.push(format!(
                "- {} [{}] producers={} consumers={} {}",
                orphan.name, orphan.kind, orphan.producers, orphan.consumers, orphan.classification
            ));
        }
    }

    Ok(RenderedCommand::success(json!(payload), lines))
}

struct SelectedEventTarget {
    target: NodeData,
    alternates: Vec<EventTargetOutput>,
}

fn select_event_target(
    graph: &impl GraphStore,
    targets: Vec<NodeData>,
    repo_filter: Option<&str>,
    subject: &str,
) -> Result<SelectedEventTarget> {
    let mut candidates = targets
        .into_iter()
        .filter(|target| repo_filter.is_none_or(|repo| target.repo == repo))
        .collect::<Vec<_>>();

    rank_event_targets(graph, &mut candidates, subject)
        .map_err(|error| anyhow::anyhow!("failed to rank event targets: {error}"))?;

    match candidates.as_slice() {
        [] => bail!("no matching event target found for `{subject}`"),
        [target] => Ok(SelectedEventTarget {
            target: target.clone(),
            alternates: Vec::new(),
        }),
        [target, ..] => Ok(SelectedEventTarget {
            target: target.clone(),
            alternates: candidates
                .iter()
                .skip(1)
                .take(3)
                .map(|item| EventTargetOutput {
                    repo: item.repo.clone(),
                    file_path: item.file_path.clone(),
                    name: item.name.clone(),
                    node_kind: item.kind.to_string(),
                })
                .collect(),
        }),
    }
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
        EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, Visibility, node_id, topic_qn,
        virtual_node,
    };
    use gather_step_storage::{GraphStore, GraphStoreDb};

    use super::select_event_target;

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new(name: &str) -> Self {
            let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-events-cli-{name}-{}-{counter}.redb",
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

    #[test]
    fn select_event_target_prefers_richer_event_candidate() {
        let temp = TempDb::new("ranking");
        let store = GraphStoreDb::open(temp.path()).expect("graph should open");
        let file = file("backend_standard", "src/events.ts");
        let event_target = virtual_node(
            NodeKind::Event,
            "backend_standard",
            "src/events.ts",
            "order.created",
            topic_qn("kafka", "order.created"),
        );
        let topic_target = virtual_node(
            NodeKind::Topic,
            "backend_standard",
            "src/events.ts",
            "order.created",
            "__topic__kafka__order.created",
        );
        let producer = symbol("backend_standard", "src/events.ts", "emitOrder", 0);
        let consumer = symbol("frontend_standard", "src/api.ts", "handleOrder", 1);

        store
            .bulk_insert(
                &[
                    file.clone(),
                    event_target.clone(),
                    topic_target.clone(),
                    producer.clone(),
                    consumer.clone(),
                ],
                &[
                    EdgeData {
                        source: producer.id,
                        target: event_target.id,
                        kind: EdgeKind::ProducesEventFor,
                        metadata: EdgeMetadata::default(),
                        owner_file: file.id,
                        is_cross_file: true,
                    },
                    EdgeData {
                        source: consumer.id,
                        target: event_target.id,
                        kind: EdgeKind::UsesEventFrom,
                        metadata: EdgeMetadata::default(),
                        owner_file: file.id,
                        is_cross_file: true,
                    },
                ],
            )
            .expect("graph should write");

        let selected = select_event_target(
            &store,
            vec![topic_target.clone(), event_target.clone()],
            None,
            "order.created",
        )
        .expect("selection should succeed");

        assert_eq!(selected.target.id, event_target.id);
    }

    #[test]
    fn select_event_target_surfaces_family_alternates() {
        let temp = TempDb::new("family");
        let store = GraphStoreDb::open(temp.path()).expect("graph should open");
        let file = file("backend_standard", "src/events.ts");
        let created = virtual_node(
            NodeKind::Topic,
            "backend_standard",
            "src/events.ts",
            "order.created",
            topic_qn("kafka", "order.created"),
        );
        let sync = virtual_node(
            NodeKind::Topic,
            "backend_standard",
            "src/events.ts",
            "order.sync",
            topic_qn("kafka", "order.sync"),
        );
        let producer = symbol("backend_standard", "src/events.ts", "emitOrder", 0);

        store
            .bulk_insert(
                &[
                    file.clone(),
                    created.clone(),
                    sync.clone(),
                    producer.clone(),
                ],
                &[EdgeData {
                    source: producer.id,
                    target: created.id,
                    kind: EdgeKind::Publishes,
                    metadata: EdgeMetadata::default(),
                    owner_file: file.id,
                    is_cross_file: true,
                }],
            )
            .expect("graph should write");

        let selected =
            select_event_target(&store, vec![sync.clone(), created.clone()], None, "order")
                .expect("selection should succeed");

        assert_eq!(selected.target.id, created.id);
        assert_eq!(selected.alternates.len(), 1);
        assert_eq!(selected.alternates[0].name, "order.sync");
    }

    fn file(repo: &str, file_path: &str) -> NodeData {
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

    fn symbol(repo: &str, file_path: &str, name: &str, _ordinal: u16) -> NodeData {
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
            span: None,
            is_virtual: false,
        }
    }
}
