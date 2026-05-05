use gather_step_core::{
    EdgeData, EdgeKind, EdgeMetadata, MIGRATION_FILTERS_METADATA_PREFIX, NodeData, NodeKind,
    ref_node_id,
};

use crate::frameworks::typeorm_migration;
use crate::tree_sitter::ParsedFile;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TypeormAugmentation {
    pub nodes: Vec<NodeData>,
    pub edges: Vec<EdgeData>,
}

pub fn augment(parsed: &ParsedFile) -> TypeormAugmentation {
    let mut augmentation = TypeormAugmentation::default();
    for migration in typeorm_migration::detect_migrations(parsed) {
        add_migration_table_edge(parsed, &mut augmentation, migration);
    }
    augmentation
}

fn add_migration_table_edge(
    parsed: &ParsedFile,
    augmentation: &mut TypeormAugmentation,
    migration: typeorm_migration::TypeormMigration,
) {
    let qualified_name = format!("__migration_collection__{}", migration.table_name);
    let collection_node = NodeData {
        id: ref_node_id(NodeKind::Entity, &qualified_name),
        kind: NodeKind::Entity,
        repo: parsed.file_node.repo.clone(),
        file_path: parsed.file_node.file_path.clone(),
        name: migration.table_name,
        qualified_name: Some(qualified_name.clone()),
        external_id: Some(qualified_name),
        signature: None,
        visibility: None,
        span: None,
        is_virtual: true,
    };
    let metadata = EdgeMetadata {
        drift_kind: migration_filters_metadata(&migration.filter_literals),
        ..EdgeMetadata::default()
    };
    augmentation.edges.push(EdgeData {
        source: migration_primary_symbol(parsed),
        target: collection_node.id,
        kind: EdgeKind::MigratesCollection,
        metadata,
        owner_file: parsed.file_node.id,
        is_cross_file: false,
    });
    augmentation.nodes.push(collection_node);
}

fn migration_primary_symbol(parsed: &ParsedFile) -> gather_step_core::NodeId {
    // TypeORM migrations always implement `MigrationInterface` with an `up`
    // (and usually `down`) method, so the `up` function is the most reliable
    // anchor. Fall back to a class whose name *ends* with `Migration` (some
    // codebases name them `AddAlertWorkflowMigration` rather than the
    // timestamped convention). Substring `contains("Migration")` is rejected
    // because it would falsely catch unrelated classes such as
    // `DataMigrationHelper`, anchoring the MigratesCollection edge on the
    // wrong symbol.
    parsed
        .nodes
        .iter()
        .find(|node| matches!(node.kind, NodeKind::Function) && node.name == "up")
        .or_else(|| {
            parsed.nodes.iter().find(|node| {
                matches!(node.kind, NodeKind::Class) && node.name.ends_with("Migration")
            })
        })
        .map_or(parsed.file_node.id, |node| node.id)
}

fn migration_filters_metadata(filters: &[String]) -> Option<String> {
    if filters.is_empty() {
        return None;
    }
    serde_json::to_string(filters)
        .ok()
        .map(|json| format!("{MIGRATION_FILTERS_METADATA_PREFIX}{json}"))
}

#[cfg(test)]
mod tests {
    use gather_step_core::EdgeKind;

    use crate::{
        frameworks::{Framework, typeorm::augment},
        tree_sitter::parse_file_with_frameworks,
    };

    #[test]
    fn typeorm_migration_produces_migrates_collection_edge() {
        let parsed = parse_file_with_frameworks(
            "svc",
            std::path::Path::new("/repo"),
            &crate::FileEntry {
                path: "src/migrations/1714410000000-add-alert-workflow.ts".into(),
                language: crate::Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: Some(
                    br#"
                    import { MigrationInterface, QueryRunner } from 'typeorm';

                    export class AddAlertWorkflow1714410000000 implements MigrationInterface {
                      public async up(queryRunner: QueryRunner): Promise<void> {
                        await queryRunner.query(`ALTER TABLE "alerts" ADD COLUMN "workflow" jsonb`);
                      }
                    }
                    "#
                    .to_vec()
                    .into(),
                ),
            },
            &[Framework::TypeOrm],
        )
        .expect("fixture should parse");

        let augmentation = augment(&parsed);
        assert!(augmentation.nodes.iter().any(|node| {
            node.name == "alerts"
                && node
                    .external_id
                    .as_deref()
                    .is_some_and(|external_id| external_id == "__migration_collection__alerts")
        }));
        assert!(
            augmentation
                .edges
                .iter()
                .any(|edge| edge.kind == EdgeKind::MigratesCollection)
        );
    }
}
