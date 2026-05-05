use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use gather_step_core::{NodeId, NodeKind};

use crate::path_guard::canonicalize_existing_file_under;
use crate::traverse::classify_language;
use crate::tree_sitter::{EnrichedCallSite, ParsedFile};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MongooseMigration {
    pub collection_name: String,
    pub filter_literals: Vec<String>,
}

#[must_use]
pub fn detect_migration(parsed: &ParsedFile) -> Option<MongooseMigration> {
    let mut migrations = detect_migrations(parsed);
    (migrations.len() == 1).then(|| migrations.remove(0))
}

#[must_use]
pub fn detect_migrations(parsed: &ParsedFile) -> Vec<MongooseMigration> {
    if !is_migration_path(&parsed.file.path) {
        return Vec::new();
    }
    if !imports_mongoose(&parsed.source) {
        return Vec::new();
    }
    if !exports_up_down(&parsed.source) {
        return Vec::new();
    }

    let up_owner_id = migration_up_owner_id(parsed);
    let model_collections = mongoose_model_collection_names(parsed);
    let mut collection_calls = parsed
        .call_sites
        .iter()
        .filter(|call_site| up_owner_id == Some(call_site.owner_id))
        .filter_map(collection_call_hint)
        .collect::<Vec<_>>();
    collection_calls.sort_by_key(|(line, _)| *line);

    let mut filters_by_collection = BTreeMap::<String, Vec<String>>::new();
    for call_site in parsed
        .call_sites
        .iter()
        .filter(|call_site| up_owner_id == Some(call_site.owner_id))
        .filter(|call_site| is_migration_write_method(&call_site.callee_name))
    {
        let Some(collection_name) =
            migration_write_collection_name(call_site, &model_collections, &collection_calls)
        else {
            continue;
        };
        let filter_literals = call_site
            .raw_arguments
            .as_deref()
            .and_then(first_argument)
            .map(normalize_literal)
            .filter(|literal| !literal.is_empty())
            .into_iter();
        filters_by_collection
            .entry(collection_name)
            .or_default()
            .extend(filter_literals);
    }

    filters_by_collection
        .into_iter()
        .map(|(collection_name, mut filter_literals)| {
            filter_literals.sort();
            filter_literals.dedup();
            MongooseMigration {
                collection_name,
                filter_literals,
            }
        })
        .collect()
}

fn migration_up_owner_id(parsed: &ParsedFile) -> Option<NodeId> {
    parsed
        .nodes
        .iter()
        .find(|node| node.kind == NodeKind::Function && node.name == "up")
        .map(|node| node.id)
}

fn migration_write_collection_name(
    call_site: &EnrichedCallSite,
    model_collections: &BTreeMap<String, String>,
    collection_calls: &[(u32, String)],
) -> Option<String> {
    let hint = call_site.callee_qualified_hint.as_deref()?;
    let receiver = hint.rsplit_once('.')?.0.rsplit('.').next()?;
    if let Some(model_name) = normalize_model_receiver(receiver)
        && let Some(collection_name) = model_collections.get(&model_name)
    {
        return Some(collection_name.clone());
    }

    let line_start = call_site.span.as_ref()?.line_start;
    collection_calls
        .iter()
        .rev()
        .find(|(line, _)| *line <= line_start)
        .map(|(_, collection_name)| collection_name.clone())
}

fn collection_call_hint(call_site: &EnrichedCallSite) -> Option<(u32, String)> {
    if call_site.callee_name != "collection"
        || !call_site
            .callee_qualified_hint
            .as_deref()
            .is_some_and(|hint| hint.contains("db.collection"))
    {
        return None;
    }

    let line_start = call_site.span.as_ref()?.line_start;
    let collection_name = call_site
        .literal_argument
        .as_deref()
        .map(strip_quotes)
        .filter(|name| !name.is_empty())?
        .to_owned();
    Some((line_start, collection_name))
}

fn is_migration_write_method(name: &str) -> bool {
    matches!(
        name,
        "updateMany" | "updateOne" | "replaceOne" | "deleteMany" | "deleteOne"
    )
}

fn normalize_model_receiver(receiver: &str) -> Option<String> {
    let receiver = receiver.rsplit("::").next().unwrap_or(receiver);
    let base = receiver.strip_suffix("Model").unwrap_or(receiver);
    let mut chars = base.chars();
    let first = chars.next()?;
    let mut normalized: String = first.to_uppercase().collect();
    normalized.push_str(chars.as_str());
    Some(normalized)
}

fn mongoose_model_collection_names(parsed: &ParsedFile) -> BTreeMap<String, String> {
    let mut result = mongoose_model_collection_names_in_source(&parsed.source);
    for imported in imported_mongoose_model_files(parsed) {
        result.extend(mongoose_model_collection_names_in_source(&imported.source));
    }
    result
}

fn mongoose_model_collection_names_in_source(source: &str) -> BTreeMap<String, String> {
    let mut result = BTreeMap::new();
    let mut cursor = 0;
    while let Some(relative_index) = source[cursor..].find("mongoose.model") {
        let start = cursor + relative_index + "mongoose.model".len();
        let Some(open_relative) = source[start..].find('(') else {
            break;
        };
        let open = start + open_relative;
        let Some(close) = matching_closing_paren(source, open) else {
            cursor = open + 1;
            continue;
        };
        if let Some(raw_arguments) = source.get(open + 1..close) {
            let arguments = top_level_arguments(raw_arguments);
            if arguments.len() >= 3
                && let (Some(model_name), Some(collection_name)) = (
                    quoted_literal_value(arguments[0]),
                    quoted_literal_value(arguments[2]),
                )
            {
                result.insert(model_name.to_owned(), collection_name.to_owned());
            }
        }
        cursor = close + 1;
    }
    result
}

fn imported_mongoose_model_files(parsed: &ParsedFile) -> Vec<ParsedFile> {
    parsed
        .import_bindings
        .iter()
        .filter(|binding| binding.resolved_path.is_some() && !binding.is_namespace)
        .filter_map(|binding| binding.resolved_path.as_ref())
        .filter_map(|path| parse_imported_file(parsed, path))
        .collect()
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
    let source_bytes: std::sync::Arc<[u8]> = fs::read(&safe_path).ok()?.into();
    if !contains_mongoose_model_marker(&source_bytes) {
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
            source_bytes: Some(source_bytes),
        },
    )
    .ok()
}

fn contains_mongoose_model_marker(source: &[u8]) -> bool {
    source
        .windows(b"mongoose.model".len())
        .any(|window| window == b"mongoose.model")
}

fn repo_root_for(parsed: &ParsedFile) -> PathBuf {
    let mut root = parsed.source_path.clone();
    for _ in parsed.file.path.components() {
        root.pop();
    }
    root
}

use super::migration_utils::{is_migration_path, matching_closing_paren, top_level_arguments};

fn imports_mongoose(source: &str) -> bool {
    source.contains("from 'mongoose'")
        || source.contains("from \"mongoose\"")
        || source.contains("require('mongoose')")
        || source.contains("require(\"mongoose\")")
}

fn exports_up_down(source: &str) -> bool {
    if source.contains("module.exports") {
        return source.contains("up") && source.contains("down");
    }

    has_exported_member(source, "up") && has_exported_member(source, "down")
}

fn has_exported_member(source: &str, name: &str) -> bool {
    const PREFIXES: &[&str] = &[
        "export async function ",
        "export function ",
        "export const ",
        "export { ",
    ];
    PREFIXES
        .iter()
        .any(|prefix| contains_after_prefix(source, prefix, name))
}

fn contains_after_prefix(source: &str, prefix: &str, suffix: &str) -> bool {
    let mut start = 0;
    while let Some(offset) = source[start..].find(prefix) {
        let after_prefix = start + offset + prefix.len();
        if source[after_prefix..].starts_with(suffix) {
            return true;
        }
        start = after_prefix;
    }
    false
}

fn first_argument(raw_arguments: &str) -> Option<&str> {
    top_level_arguments(raw_arguments).into_iter().next()
}

fn normalize_literal(raw: &str) -> String {
    raw.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn strip_quotes(value: &str) -> &str {
    value.trim().trim_matches('"').trim_matches('\'').trim()
}

fn quoted_literal_value(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    let is_quoted = (trimmed.starts_with('"') && trimmed.ends_with('"'))
        || (trimmed.starts_with('\'') && trimmed.ends_with('\''));
    is_quoted
        .then(|| strip_quotes(trimmed))
        .filter(|value| !value.is_empty())
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

    use crate::{
        Language,
        frameworks::{
            Framework,
            mongoose_migration::{detect_migration, detect_migrations},
        },
        tree_sitter::parse_file_with_frameworks,
    };

    static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> Self {
            let counter = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-parser-mongoose-migration-{name}-{}-{counter}",
                process::id()
            ));
            fs::create_dir_all(path.join("migrations")).expect("test directory should be created");
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
    fn detects_mongoose_migration_collection_and_filter_literal() {
        let temp_dir = TestDir::new("positive");
        fs::write(
            temp_dir
                .path()
                .join("migrations/20260430-backfill-alert-workflow.ts"),
            r#"
import mongoose from 'mongoose';

export async function up(db: mongoose.Connection['db']): Promise<void> {
  await db.collection('alerts').updateMany(
    { workflow: { $type: 'object' } },
    { $set: { migrated: true } },
  );
}

export async function down(db: mongoose.Connection['db']): Promise<void> {
  await db.collection('alerts').updateMany(
    { migrated: true },
    { $unset: { migrated: '' } },
  );
}

async function helper(db: mongoose.Connection['db']): Promise<void> {
  await db.collection('alerts').updateMany(
    { helperOnly: true },
    { $set: { ignored: true } },
  );
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file_with_frameworks(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "migrations/20260430-backfill-alert-workflow.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &[Framework::Mongoose],
        )
        .expect("fixture should parse");

        let migration = detect_migration(&parsed).expect("migration should be detected");
        assert_eq!(migration.collection_name, "alerts");
        assert_eq!(
            migration.filter_literals,
            vec!["{ workflow: { $type: 'object' } }".to_owned()]
        );
    }

    #[test]
    fn detects_model_update_many_collection_and_filter_literal() {
        let temp_dir = TestDir::new("model-update-many");
        fs::write(
            temp_dir
                .path()
                .join("migrations/20260430-backfill-alert-workflow.ts"),
            r#"
import mongoose from 'mongoose';

const alertSchema = new mongoose.Schema({});
const AlertModel = mongoose.model('Alert', alertSchema, 'alerts');
const OtherModel = mongoose.model('Other', alertSchema, 'other_records');

export async function up(): Promise<void> {
  await AlertModel.updateMany(
    { workflow: { $exists: false } },
    { $set: { migrated: true } },
  );
}

export async function down(): Promise<void> {
  await OtherModel.updateMany(
    { migrated: true },
    { $unset: { migrated: '' } },
  );
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file_with_frameworks(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "migrations/20260430-backfill-alert-workflow.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &[Framework::Mongoose],
        )
        .expect("fixture should parse");

        let migration = detect_migration(&parsed).expect("migration should be detected");
        assert_eq!(migration.collection_name, "alerts");
        assert_eq!(
            migration.filter_literals,
            vec!["{ workflow: { $exists: false } }".to_owned()]
        );
    }

    #[test]
    fn detects_imported_model_update_many_collection_and_filter_literal() {
        let temp_dir = TestDir::new("imported-model-update-many");
        fs::create_dir_all(temp_dir.path().join("models")).expect("models dir should write");
        fs::write(
            temp_dir.path().join("models/alert.model.ts"),
            r#"
import mongoose from 'mongoose';

const alertSchema = new mongoose.Schema({});
export const AlertModel = mongoose.model('Alert', alertSchema, 'alerts');
"#,
        )
        .expect("model fixture should write");
        fs::write(
            temp_dir
                .path()
                .join("migrations/20260430-backfill-alert-workflow.ts"),
            r#"
import mongoose from 'mongoose';
import { AlertModel } from '../models/alert.model.ts';

export async function up(): Promise<void> {
  await AlertModel.updateMany(
    { workflow: { $exists: false } },
    { $set: { migrated: true } },
  );
}

export async function down(): Promise<void> {}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file_with_frameworks(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "migrations/20260430-backfill-alert-workflow.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &[Framework::Mongoose],
        )
        .expect("fixture should parse");

        assert!(
            parsed.import_bindings.iter().any(
                |binding| binding.local_name == "AlertModel" && binding.resolved_path.is_some()
            ),
            "expected AlertModel import to resolve: {:?}",
            parsed.import_bindings
        );
        let migrations = detect_migrations(&parsed);
        assert_eq!(migrations.len(), 1, "migrations: {migrations:?}");
        let migration = detect_migration(&parsed).expect("migration should be detected");
        assert_eq!(migration.collection_name, "alerts");
        assert_eq!(
            migration.filter_literals,
            vec!["{ workflow: { $exists: false } }".to_owned()]
        );
    }

    #[test]
    fn ignores_imported_model_when_import_is_unresolved() {
        let temp_dir = TestDir::new("unresolved-imported-model");
        fs::write(
            temp_dir
                .path()
                .join("migrations/20260430-backfill-alert-workflow.ts"),
            r#"
import mongoose from 'mongoose';
import { AlertModel } from '../models/missing.ts';

export async function up(): Promise<void> {
  await AlertModel.updateMany(
    { workflow: { $exists: false } },
    { $set: { migrated: true } },
  );
}

export async function down(): Promise<void> {}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file_with_frameworks(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "migrations/20260430-backfill-alert-workflow.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &[Framework::Mongoose],
        )
        .expect("fixture should parse");

        assert_eq!(detect_migration(&parsed), None);
    }

    #[test]
    fn imported_model_prefilter_skips_non_model_sources() {
        assert!(super::contains_mongoose_model_marker(
            b"export const AlertModel = mongoose.model('Alert', schema, 'alerts');"
        ));
        assert!(!super::contains_mongoose_model_marker(
            b"export const buildAlert = () => ({ workflow: true });"
        ));
    }

    #[test]
    fn ignores_model_update_many_with_dynamic_collection_name() {
        let temp_dir = TestDir::new("dynamic-model-collection");
        fs::write(
            temp_dir.path().join("migrations/20260430-alerts.ts"),
            r#"
import mongoose from 'mongoose';

const collectionName = 'alerts';
const alertSchema = new mongoose.Schema({});
const AlertModel = mongoose.model('Alert', alertSchema, collectionName);

export async function up(): Promise<void> {
  await AlertModel.updateMany({}, { $set: { migrated: true } });
}

export async function down(): Promise<void> {}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file_with_frameworks(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "migrations/20260430-alerts.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &[Framework::Mongoose],
        )
        .expect("fixture should parse");

        assert_eq!(detect_migration(&parsed), None);
    }

    #[test]
    fn ignores_down_collection_when_detecting_target_collection() {
        let temp_dir = TestDir::new("down-other-collection");
        fs::write(
            temp_dir.path().join("migrations/20260430-alerts.ts"),
            r#"
import mongoose from 'mongoose';

export async function up(db: mongoose.Connection['db']): Promise<void> {
  await db.collection('alerts').updateMany({}, { $set: { migrated: true } });
}

export async function down(db: mongoose.Connection['db']): Promise<void> {
  await db.collection('users').updateMany({}, { $unset: { migrated: '' } });
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file_with_frameworks(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "migrations/20260430-alerts.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &[Framework::Mongoose],
        )
        .expect("fixture should parse");

        let migration = detect_migration(&parsed).expect("migration should be detected");
        assert_eq!(migration.collection_name, "alerts");
    }

    #[test]
    fn ignores_typeorm_style_migration() {
        let temp_dir = TestDir::new("typeorm-negative");
        fs::write(
            temp_dir.path().join("migrations/20260430-alerts.ts"),
            r#"
import { MigrationInterface, QueryRunner } from 'typeorm';

export class BackfillAlerts20260430 implements MigrationInterface {
  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query('update alerts set migrated = true');
  }

  async down(queryRunner: QueryRunner): Promise<void> {}
}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file_with_frameworks(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "migrations/20260430-alerts.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &[Framework::Mongoose],
        )
        .expect("fixture should parse");

        assert_eq!(detect_migration(&parsed), None);
    }

    #[test]
    fn ignores_ambiguous_multi_collection_migration() {
        let temp_dir = TestDir::new("ambiguous");
        fs::write(
            temp_dir.path().join("migrations/20260430-alerts.ts"),
            r#"
import mongoose from 'mongoose';

export async function up(db: mongoose.Connection['db']): Promise<void> {
  await db.collection('alerts').updateMany({}, { $set: { migrated: true } });
  await db.collection('users').updateMany({}, { $set: { migrated: true } });
}

export async function down(db: mongoose.Connection['db']): Promise<void> {}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file_with_frameworks(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "migrations/20260430-alerts.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &[Framework::Mongoose],
        )
        .expect("fixture should parse");

        assert_eq!(detect_migration(&parsed), None);
    }

    #[test]
    fn detects_multi_collection_migration_for_graph_edges() {
        let temp_dir = TestDir::new("multi-collection");
        fs::write(
            temp_dir.path().join("migrations/20260430-alerts.ts"),
            r#"
import mongoose from 'mongoose';

export async function up(db: mongoose.Connection['db']): Promise<void> {
  await db.collection('alerts').updateMany(
    { workflow: { $type: 'object' } },
    { $set: { migrated: true } },
  );
  await db.collection('users').deleteMany(
    { stale: true },
  );
}

export async function down(db: mongoose.Connection['db']): Promise<void> {}
"#,
        )
        .expect("fixture should write");

        let parsed = parse_file_with_frameworks(
            "sample-service",
            temp_dir.path(),
            &crate::FileEntry {
                path: "migrations/20260430-alerts.ts".into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &[Framework::Mongoose],
        )
        .expect("fixture should parse");

        let migrations = detect_migrations(&parsed);
        assert_eq!(migrations.len(), 2);
        assert_eq!(migrations[0].collection_name, "alerts");
        assert_eq!(
            migrations[0].filter_literals,
            vec!["{ workflow: { $type: 'object' } }".to_owned()]
        );
        assert_eq!(migrations[1].collection_name, "users");
        assert_eq!(
            migrations[1].filter_literals,
            vec!["{ stale: true }".to_owned()]
        );
        assert_eq!(
            detect_migration(&parsed),
            None,
            "single-migration compatibility helper should stay conservative for multi-collection files"
        );
    }
}
