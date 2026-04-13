use gather_step_core::{EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, ref_node_id};

use crate::tree_sitter::{EnrichedCallSite, ParsedFile};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MongooseAugmentation {
    pub nodes: Vec<NodeData>,
    pub edges: Vec<EdgeData>,
}

/// Augment a parsed file with Mongoose-specific graph edges and virtual nodes.
///
/// The `NestJS` extractor runs first (see `parse_file_with_frameworks`), so
/// entity nodes for `@Schema`-decorated classes are already present in
/// `parsed.nodes` when this function executes.  This extractor adds:
///
/// - `@Prop` field references back to the parent entity node.
/// - `SchemaFactory.createForClass()` → entity `References` edges.
/// - `MongooseModule.forFeature[Async]()` → virtual entity `DependsOn` edges.
/// - `@InjectModel()` → virtual model `Service` node + `DependsOn` edges.
/// - Common repository operation calls → virtual entity `References` edges.
/// - `db.collection()` → virtual collection `Entity` node + `References` edge.
pub fn augment(parsed: &ParsedFile) -> MongooseAugmentation {
    let mut augmentation = MongooseAugmentation::default();

    // NOTE: `@Prop()` field extraction is deferred. The tree-sitter visitor
    // does not emit `SymbolCapture` entries for class property declarations
    // (only methods and functions), so `@Prop`-decorated fields are invisible
    // to the augmentation layer. Adding support requires extending the
    // `visit_ts_js` `class_body` visitor to handle `public_field_definition`
    // nodes — tracked for a future tree-sitter enhancement.

    add_inject_model_edges(parsed, &mut augmentation);
    add_schema_factory_edges(parsed, &mut augmentation);
    add_for_feature_edges(parsed, &mut augmentation);
    add_repository_operation_edges(parsed, &mut augmentation);
    add_db_collection_edges(parsed, &mut augmentation);

    augmentation
}

/// Emit a `Service` virtual node and a `DependsOn` edge for every
/// `InjectModel(ModelName.name)` call site.
///
/// `@InjectModel` is a parameter decorator on constructor parameters, not a
/// class-level decorator.  Tree-sitter captures it as a `call_expression` when
/// recursing into the constructor body, so it appears in `parsed.call_sites`
/// with owner being the constructor method's `NodeId`.
///
/// The model name is normalised by stripping the `.name` property-access
/// suffix that `NestJS` idiom requires (`Product.name` → `Product`).
fn add_inject_model_edges(parsed: &ParsedFile, augmentation: &mut MongooseAugmentation) {
    for call_site in &parsed.call_sites {
        if call_site.callee_name != "InjectModel" {
            continue;
        }
        let Some(raw_arg) = call_site.literal_argument.as_deref() else {
            continue;
        };
        let model_name = strip_dot_name(raw_arg.trim());
        let model_name = strip_quotes(model_name);
        if model_name.is_empty() {
            continue;
        }
        let qualified_name = format!("__model__{model_name}");
        let model_node = virtual_node_from_call(
            parsed,
            call_site,
            NodeKind::Service,
            &qualified_name,
            model_name,
        );
        let model_id = model_node.id;
        augmentation.nodes.push(model_node);
        augmentation.edges.push(EdgeData {
            source: call_site.owner_id,
            target: model_id,
            kind: EdgeKind::DependsOn,
            metadata: EdgeMetadata::default(),
            owner_file: call_site.owner_file,
            is_cross_file: false,
        });
    }
}

// ---------------------------------------------------------------------------
// Call-site extractors
// ---------------------------------------------------------------------------

/// Emit `References` edges for `SchemaFactory.createForClass(ClassName)` calls.
///
/// The first literal argument is the class name.  We point at the virtual
/// entity node `__entity__ClassName` produced by the `NestJS` extractor for the
/// corresponding `@Schema` class.
fn add_schema_factory_edges(parsed: &ParsedFile, augmentation: &mut MongooseAugmentation) {
    for call_site in &parsed.call_sites {
        if !is_schema_factory_call(call_site) {
            continue;
        }
        // `literal_argument` is set only when the argument is a genuine string
        // or array literal. For `SchemaFactory.createForClass(Product)` the
        // argument is a bare identifier (a class reference), so we fall back to
        // `raw_arguments`, which holds the verbatim source text of all args.
        let class_name = call_site
            .literal_argument
            .as_deref()
            .or(call_site.raw_arguments.as_deref())
            .unwrap_or("")
            .trim();
        let class_name = strip_quotes(class_name);
        if class_name.is_empty() {
            continue;
        }
        let qualified_name = format!("__entity__{class_name}");
        let entity_node = virtual_node_from_call(
            parsed,
            call_site,
            NodeKind::Entity,
            &qualified_name,
            class_name,
        );
        let entity_id = entity_node.id;
        augmentation.nodes.push(entity_node);
        augmentation.edges.push(EdgeData {
            source: call_site.owner_id,
            target: entity_id,
            kind: EdgeKind::References,
            metadata: EdgeMetadata::default(),
            owner_file: call_site.owner_file,
            is_cross_file: false,
        });
    }
}

/// Emit `DependsOn` edges for `MongooseModule.forFeature(...)` and
/// `MongooseModule.forFeatureAsync(...)` calls.
///
/// The real NestJS/Mongoose pattern is:
/// ```text
/// MongooseModule.forFeature([{ name: 'Product', schema: ProductSchema }])
/// ```
///
/// `first_literal_argument` may capture the first string inside the array
/// (the `name` field value, e.g., `'Product'`), but it can also capture the
/// entire raw array/object expression when no simple string child exists.
/// We skip structured arguments (containing `{`, `[`, `:`) to avoid
/// generating garbage entity names.
fn add_for_feature_edges(parsed: &ParsedFile, augmentation: &mut MongooseAugmentation) {
    for call_site in &parsed.call_sites {
        if !is_for_feature_call(call_site) {
            continue;
        }
        for model_name in extract_for_feature_model_names(call_site) {
            let qualified_name = format!("__entity__{model_name}");
            let entity_node = virtual_node_from_call(
                parsed,
                call_site,
                NodeKind::Entity,
                &qualified_name,
                &model_name,
            );
            let entity_id = entity_node.id;
            augmentation.nodes.push(entity_node);
            augmentation.edges.push(EdgeData {
                source: call_site.owner_id,
                target: entity_id,
                kind: EdgeKind::DependsOn,
                metadata: EdgeMetadata::default(),
                owner_file: call_site.owner_file,
                is_cross_file: false,
            });
        }
    }
}

/// Emit `References` edges for common Mongoose repository operations such as
/// `findOne`, `find`, `create`, `save`, etc.
///
/// When the `callee_qualified_hint` contains a model-like receiver
/// (e.g. `this.productModel.findOne`), the model name is extracted by taking
/// the second-to-last segment and stripping the `Model` suffix when present.
fn add_repository_operation_edges(parsed: &ParsedFile, augmentation: &mut MongooseAugmentation) {
    for call_site in &parsed.call_sites {
        if !is_repository_operation(call_site) {
            continue;
        }
        let Some(hint) = call_site.callee_qualified_hint.as_deref() else {
            continue;
        };
        let Some(model_name) = extract_model_name_from_hint(hint) else {
            continue;
        };
        if model_name.is_empty() {
            continue;
        }
        let qualified_name = format!("__entity__{model_name}");
        let entity_node = virtual_node_from_call(
            parsed,
            call_site,
            NodeKind::Entity,
            &qualified_name,
            &model_name,
        );
        let entity_id = entity_node.id;
        augmentation.nodes.push(entity_node);
        augmentation.edges.push(EdgeData {
            source: call_site.owner_id,
            target: entity_id,
            kind: EdgeKind::References,
            metadata: EdgeMetadata::default(),
            owner_file: call_site.owner_file,
            is_cross_file: false,
        });
    }
}

/// Emit `References` edges for `db.collection('collectionName')` calls used
/// in raw migration scripts.
fn add_db_collection_edges(parsed: &ParsedFile, augmentation: &mut MongooseAugmentation) {
    for call_site in &parsed.call_sites {
        if !is_db_collection_call(call_site) {
            continue;
        }
        let Some(collection_name) = call_site.literal_argument.as_deref() else {
            continue;
        };
        let collection_name = strip_quotes(collection_name.trim());
        if collection_name.is_empty() {
            continue;
        }
        let qualified_name = format!("__collection__{collection_name}");
        let collection_node = virtual_node_from_call(
            parsed,
            call_site,
            NodeKind::Entity,
            &qualified_name,
            collection_name,
        );
        let collection_id = collection_node.id;
        augmentation.nodes.push(collection_node);
        augmentation.edges.push(EdgeData {
            source: call_site.owner_id,
            target: collection_id,
            kind: EdgeKind::References,
            metadata: EdgeMetadata::default(),
            owner_file: call_site.owner_file,
            is_cross_file: false,
        });
    }
}

// ---------------------------------------------------------------------------
// Predicate helpers
// ---------------------------------------------------------------------------

fn is_schema_factory_call(call_site: &EnrichedCallSite) -> bool {
    let name_matches = call_site.callee_name == "createForClass";
    let hint_matches = call_site
        .callee_qualified_hint
        .as_deref()
        .is_some_and(|hint| hint.ends_with("SchemaFactory.createForClass"));
    name_matches || hint_matches
}

fn is_for_feature_call(call_site: &EnrichedCallSite) -> bool {
    // When a qualified hint is present, require `MongooseModule` in the
    // receiver to avoid false positives from TypeORM's `forFeature`,
    // `PassportModule.forFeature`, or other NestJS modules that share the
    // method name.  Fall back to name-only matching only when no hint is
    // available (i.e., the tree-sitter visitor could not resolve the
    // receiver chain).
    if let Some(hint) = call_site.callee_qualified_hint.as_deref() {
        hint.contains("MongooseModule.forFeature")
            || hint.contains("MongooseModule.forFeatureAsync")
    } else {
        matches!(
            call_site.callee_name.as_str(),
            "forFeature" | "forFeatureAsync"
        )
    }
}

fn is_repository_operation(call_site: &EnrichedCallSite) -> bool {
    matches!(
        call_site.callee_name.as_str(),
        "findOne"
            | "find"
            | "findById"
            | "updateOne"
            | "updateMany"
            | "deleteOne"
            | "deleteMany"
            | "aggregate"
            | "create"
            | "save"
            | "countDocuments"
            | "distinct"
    )
}

fn is_db_collection_call(call_site: &EnrichedCallSite) -> bool {
    call_site.callee_name == "collection"
        && call_site
            .callee_qualified_hint
            .as_deref()
            .is_some_and(|hint| hint.contains("db.collection"))
}

// ---------------------------------------------------------------------------
// Name extraction helpers
// ---------------------------------------------------------------------------

/// Given a qualified hint like `this.productModel.findOne`, extract the receiver
/// segment (`productModel`) and strip the trailing `Model` suffix if present,
/// yielding a capitalised entity name (`Product`).
///
/// Returns `None` when no model-like receiver can be identified (e.g. the hint
/// has fewer than two segments or the receiver doesn't end with `Model`).
fn extract_model_name_from_hint(hint: &str) -> Option<String> {
    // Segments split by `.`; the last is the operation name, second-to-last
    // is the receiver.
    let segments: Vec<&str> = hint.split('.').collect();
    // Need at least receiver + method (2 segments).
    if segments.len() < 2 {
        return None;
    }
    let receiver = segments[segments.len() - 2];
    // `this` is not a useful receiver — skip.
    if receiver == "this" || receiver.is_empty() {
        return None;
    }
    // Strip a trailing "Model" suffix (case-sensitive per NestJS convention).
    let base = receiver.strip_suffix("Model").unwrap_or(receiver);
    if base.is_empty() {
        return None;
    }
    // Capitalise the first character so `productModel` → `Product`.
    let mut chars = base.chars();
    let first = chars.next()?;
    let mut name: String = first.to_uppercase().collect();
    name.push_str(chars.as_str());
    Some(name)
}

/// Strip a trailing `.name` property-access suffix from a raw decorator
/// argument, as in `Product.name` → `Product`.
fn strip_dot_name(value: &str) -> &str {
    value.strip_suffix(".name").unwrap_or(value)
}

/// Strip surrounding single or double quotes from a string value.
fn strip_quotes(value: &str) -> &str {
    let value = value.trim_matches('"');
    let value = value.trim_matches('\'');
    value.trim()
}

fn extract_for_feature_model_names(call_site: &EnrichedCallSite) -> Vec<String> {
    let mut model_names = Vec::new();

    if let Some(raw_arg) = call_site.literal_argument.as_deref() {
        let trimmed = raw_arg.trim();
        if !trimmed.is_empty()
            && !trimmed.contains('{')
            && !trimmed.contains(':')
            && !trimmed.starts_with('[')
        {
            let model_name = strip_quotes(trimmed);
            if !model_name.is_empty() {
                model_names.push(model_name.to_owned());
            }
        }
    }

    if let Some(raw_arguments) = call_site.raw_arguments.as_deref() {
        extract_named_properties(raw_arguments, "name", &mut model_names);
    }

    model_names.sort();
    model_names.dedup();
    model_names
}

fn extract_named_properties(raw: &str, property_name: &str, output: &mut Vec<String>) {
    let mut search_start = 0;
    while let Some(found) = raw[search_start..].find(property_name) {
        let absolute = search_start + found;
        let after_name = &raw[absolute + property_name.len()..];
        let Some(colon_index) = after_name.find(':') else {
            break;
        };
        let value = first_property_token(after_name[colon_index + 1..].trim_start());
        let normalized = strip_quotes(strip_dot_name(value.trim()));
        if !normalized.is_empty()
            && normalized
                .chars()
                .next()
                .is_some_and(|ch| ch.is_ascii_alphanumeric())
        {
            output.push(normalized.to_owned());
        }
        search_start = absolute + property_name.len();
    }
}

fn first_property_token(raw: &str) -> &str {
    let end = raw
        .char_indices()
        .find_map(|(index, ch)| (matches!(ch, ',' | '}' | ']') && index > 0).then_some(index))
        .unwrap_or(raw.len());
    raw[..end].trim()
}

// ---------------------------------------------------------------------------
// Virtual node constructors
// ---------------------------------------------------------------------------

/// Build a virtual node anchored to the file containing `call_site` — used
/// for call-site extractors where we have an `EnrichedCallSite` for
/// provenance.
fn virtual_node_from_call(
    parsed: &ParsedFile,
    call_site: &EnrichedCallSite,
    kind: NodeKind,
    qualified_name: &str,
    name: &str,
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

#[cfg(test)]
mod tests {
    #![expect(clippy::needless_raw_string_hashes)]

    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use pretty_assertions::assert_eq;

    use crate::{Language, frameworks::Framework, tree_sitter::parse_file_with_frameworks};

    // Tests target both NestJS and Mongoose extractors because entity detection
    // (`@Schema` → Entity node) lives in nestjs.rs and Mongoose edges build on
    // top of those nodes.
    fn parse_file(
        repo: &str,
        repo_root: &Path,
        file: &crate::FileEntry,
    ) -> Result<crate::ParsedFile, crate::ParseError> {
        parse_file_with_frameworks(
            repo,
            repo_root,
            file,
            &[Framework::NestJs, Framework::Mongoose],
        )
    }

    static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> Self {
            let counter = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-parser-mongoose-{name}-{}-{counter}",
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
    fn inject_model_decorator_produces_model_di_edge() {
        // `@InjectModel(Product.name)` on a repository class declares that the
        // class depends on the `Product` Mongoose model.  We should emit:
        //   - a virtual `Service` node with QN `__model__Product`
        //   - a `DependsOn` edge from the repository class to that node
        let temp_dir = TestDir::new("inject-model");
        fs::write(
            temp_dir.path().join("product.repository.ts"),
            r#"
import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';

@Injectable()
export class ProductRepository {
  constructor(
    @InjectModel(Product.name)
    private readonly productModel: Model<Product>,
  ) {}
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "product.repository.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        let model_node = parsed
            .nodes
            .iter()
            .find(|node| node.external_id.as_deref() == Some("__model__Product"));
        assert!(
            model_node.is_some(),
            "expected a Service virtual node with QN __model__Product, nodes: {:?}",
            parsed
                .nodes
                .iter()
                .map(|n| n.external_id.as_deref())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            model_node.unwrap().kind,
            gather_step_core::NodeKind::Service
        );

        let depends_on_count = parsed
            .edges
            .iter()
            .filter(|edge| {
                edge.kind == gather_step_core::EdgeKind::DependsOn
                    && edge.target == model_node.unwrap().id
            })
            .count();
        assert_eq!(
            depends_on_count, 1,
            "expected exactly one DependsOn edge to the model node"
        );
    }

    #[test]
    fn schema_factory_create_for_class_produces_reference() {
        // `SchemaFactory.createForClass(Product)` wires the Mongoose schema to
        // the class.  We should emit:
        //   - a virtual `Entity` node with QN `__entity__Product` (may already
        //     exist from the NestJS extractor — virtual-node dedup handles it)
        //   - a `References` edge from the calling function to that node
        //
        // The call must be inside a function body so the tree-sitter visitor
        // records it as a call site (module-level expressions lack an owner
        // and are skipped by the call-site recorder).
        let temp_dir = TestDir::new("schema-factory");
        fs::write(
            temp_dir.path().join("product.schema.ts"),
            r#"
import { Schema, SchemaFactory } from '@nestjs/mongoose';

@Schema()
export class Product {}

export function buildProductSchema() {
  return SchemaFactory.createForClass(Product);
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "product.schema.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        let entity_node = parsed
            .nodes
            .iter()
            .find(|node| node.external_id.as_deref() == Some("__entity__Product"));
        assert!(
            entity_node.is_some(),
            "expected an Entity virtual node with QN __entity__Product"
        );

        let references_edge = parsed.edges.iter().find(|edge| {
            edge.kind == gather_step_core::EdgeKind::References
                && edge.target == entity_node.unwrap().id
        });
        assert!(
            references_edge.is_some(),
            "expected a References edge to __entity__Product from SchemaFactory call"
        );
    }

    #[test]
    fn for_feature_array_form_produces_entity_dependency() {
        let temp_dir = TestDir::new("for-feature-array");
        fs::write(
            temp_dir.path().join("provider.module.ts"),
            r#"
import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';

@Module({})
export class ProviderModule {
  configure() {
    return MongooseModule.forFeature([
      { name: 'ProviderRawData', schema: ProviderRawDataSchema },
    ]);
  }
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "provider.module.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        let entity_node = parsed
            .nodes
            .iter()
            .find(|node| node.external_id.as_deref() == Some("__entity__ProviderRawData"))
            .expect("forFeature array form should produce an entity node");
        assert!(
            parsed.edges.iter().any(|edge| {
                edge.kind == gather_step_core::EdgeKind::DependsOn && edge.target == entity_node.id
            }),
            "forFeature array form should create a DependsOn edge to the entity node"
        );
    }

    #[test]
    fn repository_operation_produces_entity_reference() {
        // `this.productModel.findOne(...)` inside a repository method should
        // produce:
        //   - a virtual `Entity` node with QN `__entity__Product`
        //   - a `References` edge from the method to that node
        let temp_dir = TestDir::new("repo-op");
        fs::write(
            temp_dir.path().join("product.repository.ts"),
            r#"
import { Injectable } from '@nestjs/common';

@Injectable()
export class ProductRepository {
  async findByTitle(title: string) {
    return this.productModel.findOne({ title });
  }
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "product.repository.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        let entity_node = parsed
            .nodes
            .iter()
            .find(|node| node.external_id.as_deref() == Some("__entity__Product"));
        assert!(
            entity_node.is_some(),
            "expected an Entity virtual node with QN __entity__Product from repository operation, \
             nodes: {:?}",
            parsed
                .nodes
                .iter()
                .map(|n| n.external_id.as_deref())
                .collect::<Vec<_>>()
        );

        let references_edge = parsed.edges.iter().find(|edge| {
            edge.kind == gather_step_core::EdgeKind::References
                && edge.target == entity_node.unwrap().id
        });
        assert!(
            references_edge.is_some(),
            "expected a References edge from the method to __entity__Product"
        );
    }

    #[test]
    fn db_collection_produces_collection_reference() {
        // `db.collection('orders')` in a migration script should produce:
        //   - a virtual `Entity` node with QN `__collection__orders`
        //   - a `References` edge from the calling function to that node
        let temp_dir = TestDir::new("db-collection");
        fs::write(
            temp_dir.path().join("migrate.ts"),
            r#"
export async function up(db: Db): Promise<void> {
  await db.collection('orders').updateMany({}, { $set: { active: true } });
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "migrate.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
        )
        .expect("fixture should parse");

        let collection_node = parsed
            .nodes
            .iter()
            .find(|node| node.external_id.as_deref() == Some("__collection__orders"));
        assert!(
            collection_node.is_some(),
            "expected an Entity virtual node with QN __collection__orders, nodes: {:?}",
            parsed
                .nodes
                .iter()
                .map(|n| n.external_id.as_deref())
                .collect::<Vec<_>>()
        );

        let references_edge = parsed.edges.iter().find(|edge| {
            edge.kind == gather_step_core::EdgeKind::References
                && edge.target == collection_node.unwrap().id
        });
        assert!(
            references_edge.is_some(),
            "expected a References edge to __collection__orders"
        );
    }
}
