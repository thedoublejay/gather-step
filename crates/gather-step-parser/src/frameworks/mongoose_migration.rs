use std::collections::BTreeMap;

use gather_step_core::{NodeId, NodeKind};

use crate::tree_sitter::{EnrichedCallSite, ParsedFile};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MongooseMigration {
    pub collection_name: String,
    pub filter_literals: Vec<String>,
}

#[must_use]
pub fn detect_migration(parsed: &ParsedFile) -> Option<MongooseMigration> {
    if !is_migration_path(&parsed.file.path) {
        return None;
    }
    if !imports_mongoose(&parsed.source) {
        return None;
    }
    if !exports_up_down(&parsed.source) {
        return None;
    }

    let up_owner_id = migration_up_owner_id(parsed);
    let model_collections = mongoose_model_collection_names(&parsed.source);
    let mut collection_names = parsed
        .call_sites
        .iter()
        .filter(|call_site| up_owner_id == Some(call_site.owner_id))
        .filter_map(|call_site| migration_collection_name(call_site, &model_collections))
        .collect::<Vec<_>>();
    collection_names.sort();
    collection_names.dedup();

    let [collection_name] = collection_names.as_slice() else {
        return None;
    };

    let filter_literals = parsed
        .call_sites
        .iter()
        .filter(|call_site| up_owner_id == Some(call_site.owner_id))
        .filter(|call_site| call_site.callee_name == "updateMany")
        .filter_map(|call_site| call_site.raw_arguments.as_deref())
        .filter_map(first_argument)
        .map(normalize_literal)
        .filter(|literal| !literal.is_empty())
        .collect::<Vec<_>>();

    Some(MongooseMigration {
        collection_name: collection_name.clone(),
        filter_literals,
    })
}

fn migration_up_owner_id(parsed: &ParsedFile) -> Option<NodeId> {
    parsed
        .nodes
        .iter()
        .find(|node| node.kind == NodeKind::Function && node.name == "up")
        .map(|node| node.id)
}

fn migration_collection_name(
    call_site: &EnrichedCallSite,
    model_collections: &BTreeMap<String, String>,
) -> Option<String> {
    if call_site.callee_name == "collection"
        && call_site
            .callee_qualified_hint
            .as_deref()
            .is_some_and(|hint| hint.contains("db.collection"))
    {
        return call_site
            .literal_argument
            .as_deref()
            .map(strip_quotes)
            .filter(|name| !name.is_empty())
            .map(ToOwned::to_owned);
    }

    if call_site.callee_name != "updateMany" {
        return None;
    }
    let hint = call_site.callee_qualified_hint.as_deref()?;
    let receiver = hint.rsplit_once('.')?.0.rsplit('.').next()?;
    let model_name = normalize_model_receiver(receiver)?;
    model_collections.get(&model_name).cloned()
}

fn normalize_model_receiver(receiver: &str) -> Option<String> {
    let base = receiver.strip_suffix("Model").unwrap_or(receiver);
    let mut chars = base.chars();
    let first = chars.next()?;
    let mut normalized: String = first.to_uppercase().collect();
    normalized.push_str(chars.as_str());
    Some(normalized)
}

fn mongoose_model_collection_names(source: &str) -> BTreeMap<String, String> {
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

fn is_migration_path(path: &std::path::Path) -> bool {
    path.components().any(|component| {
        component
            .as_os_str()
            .to_str()
            .is_some_and(|segment| segment.eq_ignore_ascii_case("migrations"))
    })
}

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

fn top_level_arguments(raw_arguments: &str) -> Vec<&str> {
    let mut arguments = Vec::new();
    let mut depth = 0_u32;
    let mut quote: Option<char> = None;
    let mut escaped = false;
    let mut argument_start = 0;

    for (index, ch) in raw_arguments.char_indices() {
        if let Some(current_quote) = quote {
            if escaped {
                escaped = false;
                continue;
            }
            if ch == '\\' {
                escaped = true;
                continue;
            }
            if ch == current_quote {
                quote = None;
            }
            continue;
        }

        match ch {
            '\'' | '"' | '`' => quote = Some(ch),
            '{' | '[' | '(' => depth = depth.saturating_add(1),
            '}' | ']' | ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                arguments.push(raw_arguments[argument_start..index].trim());
                argument_start = index + ch.len_utf8();
            }
            _ => {}
        }
    }

    let trailing = raw_arguments[argument_start..].trim();
    if !trailing.is_empty() {
        arguments.push(trailing);
    }
    arguments
}

fn matching_closing_paren(source: &str, open: usize) -> Option<usize> {
    let mut depth = 0_u32;
    let mut quote: Option<char> = None;
    let mut escaped = false;

    for (relative_index, ch) in source.get(open..)?.char_indices() {
        let index = open + relative_index;
        if let Some(current_quote) = quote {
            if escaped {
                escaped = false;
                continue;
            }
            if ch == '\\' {
                escaped = true;
                continue;
            }
            if ch == current_quote {
                quote = None;
            }
            continue;
        }

        match ch {
            '\'' | '"' | '`' => quote = Some(ch),
            '(' => depth = depth.saturating_add(1),
            ')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }

    None
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
        frameworks::{Framework, mongoose_migration::detect_migration},
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
}
