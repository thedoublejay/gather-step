//! Lightweight TS/JS string utilities shared by ORM-migration parsers.
//!
//! Migration parsers (`mongoose_migration`, `typeorm_migration`) live in
//! separate modules because their detection criteria and extraction shapes
//! differ. The three helpers below — `is_migration_path`,
//! `top_level_arguments`, and `matching_closing_paren` — are byte-identical
//! between those parsers and not framework-specific, so they live here.

/// Whether `path` looks like a migration source file based on its directory
/// structure. Matches any path component spelled `migrations`
/// case-insensitively (`migrations/`, `db/migrations/`, `Migrations/`, …).
pub(crate) fn is_migration_path(path: &std::path::Path) -> bool {
    path.components().any(|component| {
        component
            .as_os_str()
            .to_str()
            .is_some_and(|segment| segment.eq_ignore_ascii_case("migrations"))
    })
}

/// Split a comma-separated argument string into top-level arguments,
/// ignoring commas that fall inside string literals (`"`, `'`, backtick) or
/// inside nested `{}`/`[]`/`()`.
///
/// Returns trimmed slices into `raw_arguments` so callers can keep
/// borrowing the original buffer.
pub(crate) fn top_level_arguments(raw_arguments: &str) -> Vec<&str> {
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

/// Locate the byte offset of the `)` that closes the `(` at byte offset
/// `open` in `source`, respecting string literals and nested parens.
/// Returns `None` if the source ends before the matching `)` is found.
pub(crate) fn matching_closing_paren(source: &str, open: usize) -> Option<usize> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn is_migration_path_matches_case_insensitive() {
        assert!(is_migration_path(Path::new("src/migrations/foo.ts")));
        assert!(is_migration_path(Path::new("src/Migrations/foo.ts")));
        assert!(is_migration_path(Path::new("db/migrations/2024-add.ts")));
        assert!(!is_migration_path(Path::new("src/migration_helper.ts")));
        assert!(!is_migration_path(Path::new("src/util/migrate.ts")));
    }

    #[test]
    fn top_level_arguments_handles_strings_and_nesting() {
        assert_eq!(top_level_arguments(""), Vec::<&str>::new());
        assert_eq!(top_level_arguments("a"), vec!["a"]);
        assert_eq!(top_level_arguments("a, b"), vec!["a", "b"]);
        assert_eq!(
            top_level_arguments("'a, b', c"),
            vec!["'a, b'", "c"],
            "comma inside single quote stays in arg 0"
        );
        assert_eq!(
            top_level_arguments("{x:1, y:2}, z"),
            vec!["{x:1, y:2}", "z"],
            "comma inside braces stays in arg 0"
        );
        assert_eq!(
            top_level_arguments("`tpl ${a, b}`, c"),
            vec!["`tpl ${a, b}`", "c"],
            "comma inside backtick template stays in arg 0"
        );
    }

    #[test]
    fn matching_closing_paren_respects_quotes_and_depth() {
        let s = "(a, b)c";
        assert_eq!(matching_closing_paren(s, 0), Some(5));

        let s = "(a, (b, c))d";
        assert_eq!(matching_closing_paren(s, 0), Some(10));

        let s = "(\")\")d";
        assert_eq!(
            matching_closing_paren(s, 0),
            Some(4),
            "paren inside string literal does not close"
        );

        let s = "(unterminated";
        assert_eq!(matching_closing_paren(s, 0), None);
    }
}
