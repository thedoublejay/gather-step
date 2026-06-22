//! Indexer integration test for decorator-mediated event producers
//! (v5.1, Task 10 — structural path A).
//!
//! Indexes a two-repo fixture into a shared store:
//!   - repo A defines a custom `@AuditLog` decorator whose definition publishes
//!     to the literal topic `service-log-events`, and a method decorated with it;
//!   - repo B is a `@MessagePattern('service-log-events')` consumer of that topic.
//!
//! Asserts that after indexing, the shared `__event__kafka__service-log-events`
//! virtual node has BOTH a producer (the decorated method, via the decorator's
//! publishing definition) and the consumer — i.e. the topic is no longer a
//! producer-orphan and a producer→topic→consumer path exists across repos.
//! This mirrors what `events orphans` / `events blast-radius` compute, without
//! depending on the analysis crate: producers are incoming
//! `Publishes`/`ProducesEventFor` edges and consumers are incoming
//! `Consumes`/`UsesEventFrom` edges to the canonical Event node.

use std::fs;

use gather_step_core::{EdgeKind, NodeKind, ref_node_id};
use gather_step_storage::{GraphStore, IndexingOptions, RepoIndexer};

fn write_fixture(root: &std::path::Path, relative: &str, contents: &str) {
    let path = root.join(relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("fixture parent should create");
    }
    fs::write(path, contents).expect("fixture should write");
}

/// Minimal `NestJS` manifest so `is_nestjs` detection activates the pack.
const NESTJS_MANIFEST: &str = r#"{
  "name": "fixture",
  "dependencies": { "@nestjs/core": "^10.0.0", "@nestjs/microservices": "^10.0.0" }
}"#;

#[test]
fn decorator_mediated_producer_converges_with_cross_repo_consumer() {
    let producer_root = tempfile::tempdir().expect("producer tempdir should create");
    let consumer_root = tempfile::tempdir().expect("consumer tempdir should create");
    let storage_root = tempfile::tempdir().expect("storage tempdir should create");

    // Repo A: a custom decorator whose definition publishes to a literal topic,
    // plus a method decorated with it (same file — the structural rule joins a
    // decorator defined and used within one parsed file). The publish lives in
    // the decorator's own body; the rule attributes the producer breadcrumb to
    // every method that uses the decorator.
    write_fixture(producer_root.path(), "package.json", NESTJS_MANIFEST);
    write_fixture(
        producer_root.path(),
        "src/report.service.ts",
        r"
export function AuditLog() {
    return function (target, key, descriptor) {
        this.bus.emit('service-log-events', { action: key });
    };
}

export class ReportService {
    @AuditLog()
    generateReport() {
        return 1;
    }
}
",
    );

    // Repo B: a NestJS microservice consumer of the same topic.
    write_fixture(consumer_root.path(), "package.json", NESTJS_MANIFEST);
    write_fixture(
        consumer_root.path(),
        "src/audit.controller.ts",
        r"
import { Controller } from '@nestjs/common';
import { MessagePattern } from '@nestjs/microservices';

@Controller()
export class AuditController {
    @MessagePattern('service-log-events')
    handleAudit(data) {
        return data;
    }
}
",
    );

    let indexer =
        RepoIndexer::open(storage_root.path(), IndexingOptions::default()).expect("indexer");
    indexer
        .index_repo("audit-service", producer_root.path(), None)
        .expect("producer repo indexing should succeed");
    indexer
        .index_repo("report-consumer", consumer_root.path(), None)
        .expect("consumer repo indexing should succeed");

    let graph = indexer.storage().graph();

    let event_qn = "__event__kafka__service-log-events";
    let event_id = ref_node_id(NodeKind::Event, event_qn);

    let incoming = graph
        .get_incoming(event_id)
        .expect("incoming edges to the event node should load");

    let producers: Vec<&gather_step_core::EdgeData> = incoming
        .iter()
        .filter(|edge| matches!(edge.kind, EdgeKind::Publishes | EdgeKind::ProducesEventFor))
        .collect();
    let consumers: Vec<&gather_step_core::EdgeData> = incoming
        .iter()
        .filter(|edge| matches!(edge.kind, EdgeKind::Consumes | EdgeKind::UsesEventFrom))
        .collect();

    // The consumer side proves the topic was a producer-orphan candidate.
    assert!(
        !consumers.is_empty(),
        "consumer repo must contribute a Consumes/UsesEventFrom edge to {event_qn}; incoming: {incoming:#?}"
    );

    // The decorator-mediated producer breadcrumb must now exist: the decorated
    // method publishes to the topic, so producers > 0 (orphan resolved).
    assert!(
        !producers.is_empty(),
        "decorator-mediated producer edge must exist for {event_qn} (orphan should be resolved); incoming: {incoming:#?}"
    );

    // Reachability: a producer node reaches the consumer node through the
    // shared topic node (producer --Publishes--> topic <--UsesEventFrom-- consumer).
    let generate_report = graph
        .nodes_by_file("audit-service", "src/report.service.ts")
        .expect("producer file nodes should load")
        .into_iter()
        .find(|node| node.name == "generateReport")
        .expect("decorated generateReport symbol should be indexed");

    assert!(
        producers
            .iter()
            .any(|edge| edge.source == generate_report.id),
        "the decorated method `generateReport` must be a producer of {event_qn}; producers: {producers:#?}"
    );
}
