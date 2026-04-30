use std::collections::BTreeSet;

use crate::tree_sitter::{EnrichedCallSite, ParsedFile};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeormMigration {
    pub table_name: String,
    pub filter_literals: Vec<String>,
}

#[must_use]
pub fn detect_migrations(parsed: &ParsedFile) -> Vec<TypeormMigration> {
    if !is_migration_path(&parsed.file.path)
        || !uses_typeorm(&parsed.source)
        || !exports_up(&parsed.source)
    {
        return Vec::new();
    }

    let mut table_names = BTreeSet::new();
    for sql in query_runner_sql_literals(&parsed.source) {
        table_names.extend(table_names_from_sql(&sql));
    }
    for call_site in &parsed.call_sites {
        if let Some(table_name) = query_runner_table_method_name(call_site) {
            table_names.insert(table_name);
        }
    }

    table_names
        .into_iter()
        .map(|table_name| TypeormMigration {
            table_name,
            filter_literals: Vec::new(),
        })
        .collect()
}

fn is_migration_path(path: &std::path::Path) -> bool {
    path.components().any(|component| {
        component
            .as_os_str()
            .to_str()
            .is_some_and(|segment| segment.eq_ignore_ascii_case("migrations"))
    })
}

fn uses_typeorm(source: &str) -> bool {
    source.contains("from 'typeorm'")
        || source.contains("from \"typeorm\"")
        || source.contains("require('typeorm')")
        || source.contains("require(\"typeorm\")")
}

fn exports_up(source: &str) -> bool {
    source.contains("MigrationInterface") && source.contains("up(queryRunner")
        || source.contains("async up(queryRunner")
        || source.contains("public async up(queryRunner")
        || source.contains("export async function up")
}

fn query_runner_sql_literals(source: &str) -> Vec<String> {
    let mut literals = Vec::new();
    let mut cursor = 0;
    while let Some(relative_index) = source[cursor..].find("queryRunner.query") {
        let start = cursor + relative_index + "queryRunner.query".len();
        let Some(open_relative) = source[start..].find('(') else {
            break;
        };
        let open = start + open_relative;
        let Some(close) = matching_closing_paren(source, open) else {
            cursor = open + 1;
            continue;
        };
        if let Some(raw_arguments) = source.get(open + 1..close)
            && let Some(first) = top_level_arguments(raw_arguments).into_iter().next()
            && let Some(literal) = quoted_or_template_literal_value(first)
        {
            literals.push(literal.to_owned());
        }
        cursor = close + 1;
    }
    literals
}

fn query_runner_table_method_name(call_site: &EnrichedCallSite) -> Option<String> {
    if !matches!(
        call_site.callee_name.as_str(),
        "addColumn"
            | "changeColumn"
            | "createForeignKey"
            | "createIndex"
            | "delete"
            | "dropColumn"
            | "dropForeignKey"
            | "dropIndex"
            | "dropTable"
            | "insert"
            | "renameTable"
            | "update"
    ) || !call_site
        .callee_qualified_hint
        .as_deref()
        .is_some_and(|hint| hint.contains("queryRunner."))
    {
        return None;
    }
    call_site
        .literal_argument
        .as_deref()
        .and_then(|value| normalize_table_name(strip_quotes(value)))
}

fn table_names_from_sql(sql: &str) -> BTreeSet<String> {
    let tokens = sql_tokens(sql);
    let mut table_names = BTreeSet::new();
    for index in 0..tokens.len() {
        let token = tokens[index].as_str();
        let next = tokens.get(index + 1).map(String::as_str);
        match (token, next) {
            ("update", Some(table))
            | ("into", Some(table))
            | ("table", Some(table))
            | ("from", Some(table)) => {
                if let Some(table_name) = normalize_table_name(table) {
                    table_names.insert(table_name);
                }
            }
            _ => {}
        }
    }
    table_names
}

fn sql_tokens(sql: &str) -> Vec<String> {
    sql.split(|ch: char| {
        ch.is_ascii_whitespace() || matches!(ch, '"' | '\'' | '`' | ';' | '(' | ')' | ',')
    })
    .filter_map(|token| {
        let normalized = token.trim().trim_matches('.').to_ascii_lowercase();
        (!normalized.is_empty()).then_some(normalized)
    })
    .collect()
}

fn normalize_table_name(value: &str) -> Option<String> {
    let trimmed = value
        .trim()
        .trim_matches('"')
        .trim_matches('\'')
        .trim_matches('`');
    if trimmed.is_empty() || trimmed.starts_with('$') {
        return None;
    }
    let table = trimmed.rsplit('.').next().unwrap_or(trimmed);
    if table
        .bytes()
        .all(|byte| byte == b'_' || byte == b'-' || byte.is_ascii_alphanumeric())
    {
        Some(table.to_owned())
    } else {
        None
    }
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

fn quoted_or_template_literal_value(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    if (trimmed.starts_with('"') && trimmed.ends_with('"'))
        || (trimmed.starts_with('\'') && trimmed.ends_with('\''))
        || (trimmed.starts_with('`') && trimmed.ends_with('`'))
    {
        Some(strip_quotes(trimmed))
    } else {
        None
    }
}

fn strip_quotes(value: &str) -> &str {
    value
        .trim()
        .trim_matches('"')
        .trim_matches('\'')
        .trim_matches('`')
        .trim()
}

#[cfg(test)]
mod tests {
    #![expect(clippy::needless_raw_string_hashes)]

    use pretty_assertions::assert_eq;

    use crate::{
        frameworks::{Framework, typeorm_migration::detect_migrations},
        tree_sitter::parse_file_with_frameworks,
    };

    #[test]
    fn detects_typeorm_sql_migration_tables() {
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
                        await queryRunner.query(`UPDATE "alerts" SET "workflow" = '{}' WHERE "workflow" IS NULL`);
                        await queryRunner.addColumn('alert_events', {});
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

        let table_names = detect_migrations(&parsed)
            .into_iter()
            .map(|migration| migration.table_name)
            .collect::<Vec<_>>();
        assert_eq!(table_names, vec!["alert_events", "alerts"]);
    }

    #[test]
    fn ignores_typeorm_source_outside_migrations() {
        let parsed = parse_file_with_frameworks(
            "svc",
            std::path::Path::new("/repo"),
            &crate::FileEntry {
                path: "src/services/alert.repository.ts".into(),
                language: crate::Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: Some(
                    br#"
                    import { QueryRunner } from 'typeorm';
                    export async function update(queryRunner: QueryRunner) {
                      await queryRunner.query(`UPDATE "alerts" SET status = 'seen'`);
                    }
                    "#
                    .to_vec()
                    .into(),
                ),
            },
            &[Framework::TypeOrm],
        )
        .expect("fixture should parse");

        assert!(detect_migrations(&parsed).is_empty());
    }
}
