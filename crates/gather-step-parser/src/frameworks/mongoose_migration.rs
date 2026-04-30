use crate::tree_sitter::ParsedFile;

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

    let mut collection_names = parsed
        .call_sites
        .iter()
        .filter(|call_site| {
            call_site.callee_name == "collection"
                && call_site
                    .callee_qualified_hint
                    .as_deref()
                    .is_some_and(|hint| hint.contains("db.collection"))
        })
        .filter_map(|call_site| call_site.literal_argument.as_deref())
        .map(strip_quotes)
        .filter(|name| !name.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    collection_names.sort();
    collection_names.dedup();

    let [collection_name] = collection_names.as_slice() else {
        return None;
    };

    let filter_literals = parsed
        .call_sites
        .iter()
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
    let function_form = format!("export async function {name}");
    let sync_function_form = format!("export function {name}");
    let const_form = format!("export const {name}");
    let named_export_form = format!("export {{ {name}");
    source.contains(&function_form)
        || source.contains(&sync_function_form)
        || source.contains(&const_form)
        || source.contains(&named_export_form)
}

fn first_argument(raw_arguments: &str) -> Option<&str> {
    let mut depth = 0_u32;
    let mut quote: Option<char> = None;
    let mut escaped = false;

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
            ',' if depth == 0 => return Some(raw_arguments[..index].trim()),
            _ => {}
        }
    }

    let trimmed = raw_arguments.trim();
    (!trimmed.is_empty()).then_some(trimmed)
}

fn normalize_literal(raw: &str) -> String {
    raw.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn strip_quotes(value: &str) -> &str {
    value.trim().trim_matches('"').trim_matches('\'').trim()
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
            vec![
                "{ workflow: { $type: 'object' } }".to_owned(),
                "{ migrated: true }".to_owned()
            ]
        );
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
