/// Split a separator-delimited string at top level, ignoring separators inside
/// string/template literals and nested delimiters.
pub(crate) fn split_top_level(input: &str, separator: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut paren_depth = 0_u32;
    let mut angle_depth = 0_u32;
    let mut bracket_depth = 0_u32;
    let mut brace_depth = 0_u32;
    let mut quote: Option<char> = None;
    let mut escaped = false;

    for (index, ch) in input.char_indices() {
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
            '(' => paren_depth = paren_depth.saturating_add(1),
            ')' => paren_depth = paren_depth.saturating_sub(1),
            '<' => angle_depth = angle_depth.saturating_add(1),
            '>' => angle_depth = angle_depth.saturating_sub(1),
            '[' => bracket_depth = bracket_depth.saturating_add(1),
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            '{' => brace_depth = brace_depth.saturating_add(1),
            '}' => brace_depth = brace_depth.saturating_sub(1),
            _ if ch == separator
                && paren_depth == 0
                && angle_depth == 0
                && bracket_depth == 0
                && brace_depth == 0 =>
            {
                let part = input[start..index].trim();
                if !part.is_empty() {
                    parts.push(part);
                }
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }

    let tail = input[start..].trim();
    if !tail.is_empty() {
        parts.push(tail);
    }
    parts
}

#[cfg(test)]
mod tests {
    use super::split_top_level;

    #[test]
    fn keeps_template_literals_intact() {
        assert_eq!(
            split_top_level("`hello,${world}`,'user.updated'", ','),
            vec!["`hello,${world}`", "'user.updated'"]
        );
    }

    #[test]
    fn keeps_nested_generics_intact() {
        assert_eq!(
            split_top_level("foo: Promise<Result<A, B>>, bar: string", ','),
            vec!["foo: Promise<Result<A, B>>", "bar: string"]
        );
    }
}
