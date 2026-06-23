//! Materialize `MongoDB` query / aggregation objects and `Atlas` search-index
//! definitions found in TS/JS source into [`serde_json::Value`] so the
//! structural safety detectors in
//! `gather_step_analysis::mongo_query_safety` can run on them.
//!
//! The detectors are pure over `serde_json::Value`; this module is the
//! production extractor that feeds them. It scans the parsed file's call
//! sites for Mongo query operators (`aggregate`, `find`, `updateOne`, …) and
//! Atlas index builders (`createSearchIndex`), then parses the relevant
//! argument literal — which is a JS object/array literal, not strict JSON
//! (unquoted keys, single quotes, `$`-operators) — into a `Value`.

use serde_json::{Map, Value};

use crate::tree_sitter::ParsedFile;

use super::migration_utils::top_level_arguments;

/// Query operators whose first object/array argument is a Mongo query or
/// aggregation pipeline worth running the structural detectors over.
const QUERY_OPERATORS: &[&str] = &[
    "aggregate",
    "find",
    "findOne",
    "updateOne",
    "updateMany",
    "findOneAndUpdate",
    "deleteMany",
    "countDocuments",
];

/// Operators that define an Atlas search index. The first object argument is
/// the index definition (`{ name, definition: { mappings: { … } } }` or the
/// bare `{ mappings: { … } }` shape).
const ATLAS_INDEX_OPERATORS: &[&str] = &["createSearchIndex", "createSearchIndexes"];

/// Query / pipeline objects and Atlas index definitions materialized from a
/// single parsed file. `referenced_fields` are the top-level field names seen
/// across the file's extracted queries — the input
/// `analyze_atlas_index_drift` needs to decide whether a `dynamic:false`
/// index silently drops a queried field.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MongoExtraction {
    pub queries: Vec<Value>,
    pub atlas_indexes: Vec<Value>,
    pub referenced_fields: Vec<String>,
}

impl MongoExtraction {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queries.is_empty() && self.atlas_indexes.is_empty()
    }
}

/// Extract Mongo query/pipeline objects and Atlas index definitions from a
/// parsed TS/JS file.
#[must_use]
pub fn extract(parsed: &ParsedFile) -> MongoExtraction {
    let mut extraction = MongoExtraction::default();
    let mut referenced = std::collections::BTreeSet::new();

    for call in &parsed.call_sites {
        let Some(raw) = call.raw_arguments.as_deref() else {
            continue;
        };
        let arguments = top_level_arguments(raw);
        let Some(first) = arguments.first() else {
            continue;
        };

        if QUERY_OPERATORS.contains(&call.callee_name.as_str())
            && let Some(value) = parse_js_value(first)
        {
            collect_referenced_fields(&value, &mut referenced);
            extraction.queries.push(value);
        } else if ATLAS_INDEX_OPERATORS.contains(&call.callee_name.as_str())
            && let Some(value) = parse_js_value(first)
        {
            extraction.atlas_indexes.push(normalize_atlas_index(value));
        }
    }

    extraction.referenced_fields = referenced.into_iter().collect();
    extraction
}

/// Reduce an Atlas index argument to the `{ mappings: … }` shape that
/// `analyze_atlas_index_drift` expects. `createSearchIndex` callers wrap the
/// mappings under a `definition` key; unwrap it when present.
fn normalize_atlas_index(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            if let Some(definition) = map.get("definition").filter(|d| d.is_object()) {
                definition.clone()
            } else {
                Value::Object(map)
            }
        }
        other => other,
    }
}

/// Collect the top-level field names referenced by a query/filter object so
/// the Atlas drift detector can see which fields the code queries. Mongo
/// operators (keys starting with `$`) are pipeline directives, not fields, so
/// they are skipped; for a pipeline array each stage's `$match`/`$search`
/// object contributes its non-operator keys.
fn collect_referenced_fields(value: &Value, out: &mut std::collections::BTreeSet<String>) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if let Some(operator) = key.strip_prefix('$') {
                    if matches!(operator, "match" | "search" | "text") {
                        collect_referenced_fields(child, out);
                    }
                } else if !key.contains('$') {
                    out.insert(key.clone());
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_referenced_fields(item, out);
            }
        }
        _ => {}
    }
}

/// Parse a JS object/array literal (the Mongo subset: unquoted identifier
/// keys, single- or double-quoted strings, `$`-prefixed operator keys,
/// numbers, booleans, null, nested objects/arrays) into a
/// [`serde_json::Value`]. Returns `None` for inputs that are not an object or
/// array literal, or that contain JS the subset parser cannot represent
/// (function expressions, identifiers used as values, template strings).
#[must_use]
pub fn parse_js_value(source: &str) -> Option<Value> {
    let mut parser = LiteralParser::new(source);
    parser.skip_whitespace();
    if !matches!(parser.peek(), Some('{' | '[')) {
        return None;
    }
    let value = parser.parse_value()?;
    parser.skip_whitespace();
    parser.at_end().then_some(value)
}

/// Recursive-descent parser for the JS-literal subset. Deliberately rejects
/// anything it cannot faithfully turn into a `serde_json::Value` rather than
/// guessing, so the detectors never run on an incorrectly materialized value.
struct LiteralParser<'a> {
    chars: std::iter::Peekable<std::str::Chars<'a>>,
}

impl<'a> LiteralParser<'a> {
    fn new(source: &'a str) -> Self {
        Self {
            chars: source.chars().peekable(),
        }
    }

    fn peek(&mut self) -> Option<char> {
        self.chars.peek().copied()
    }

    fn bump(&mut self) -> Option<char> {
        self.chars.next()
    }

    fn at_end(&mut self) -> bool {
        self.peek().is_none()
    }

    fn skip_whitespace(&mut self) {
        while let Some(ch) = self.peek() {
            if ch.is_whitespace() {
                self.bump();
            } else {
                break;
            }
        }
    }

    fn parse_value(&mut self) -> Option<Value> {
        self.skip_whitespace();
        match self.peek()? {
            '{' => self.parse_object(),
            '[' => self.parse_array(),
            '"' | '\'' => self.parse_string().map(Value::String),
            _ => self.parse_keyword_or_number(),
        }
    }

    fn parse_object(&mut self) -> Option<Value> {
        self.expect('{')?;
        let mut map = Map::new();
        loop {
            self.skip_whitespace();
            match self.peek()? {
                '}' => {
                    self.bump();
                    return Some(Value::Object(map));
                }
                ',' => {
                    self.bump();
                }
                _ => {
                    let key = self.parse_key()?;
                    self.skip_whitespace();
                    self.expect(':')?;
                    let value = self.parse_value()?;
                    map.insert(key, value);
                }
            }
        }
    }

    fn parse_array(&mut self) -> Option<Value> {
        self.expect('[')?;
        let mut items = Vec::new();
        loop {
            self.skip_whitespace();
            match self.peek()? {
                ']' => {
                    self.bump();
                    return Some(Value::Array(items));
                }
                ',' => {
                    self.bump();
                }
                _ => items.push(self.parse_value()?),
            }
        }
    }

    /// Object keys may be quoted (`'$match'`, `"field"`) or bare identifiers
    /// (`field`, `$lookup`, `meta.flags`). Bare keys run up to the next `:`,
    /// whitespace, or terminator.
    fn parse_key(&mut self) -> Option<String> {
        self.skip_whitespace();
        match self.peek()? {
            '"' | '\'' => self.parse_string(),
            _ => {
                let mut key = String::new();
                while let Some(ch) = self.peek() {
                    if ch == ':' || ch.is_whitespace() || ch == ',' || ch == '}' {
                        break;
                    }
                    key.push(ch);
                    self.bump();
                }
                (!key.is_empty()).then_some(key)
            }
        }
    }

    fn parse_string(&mut self) -> Option<String> {
        let quote = self.bump()?;
        let mut out = String::new();
        while let Some(ch) = self.bump() {
            match ch {
                '\\' => {
                    let escaped = self.bump()?;
                    match escaped {
                        'n' => out.push('\n'),
                        't' => out.push('\t'),
                        'r' => out.push('\r'),
                        other => out.push(other),
                    }
                }
                c if c == quote => return Some(out),
                c => out.push(c),
            }
        }
        None
    }

    fn parse_keyword_or_number(&mut self) -> Option<Value> {
        let mut token = String::new();
        while let Some(ch) = self.peek() {
            if ch == ',' || ch == '}' || ch == ']' || ch == ':' || ch.is_whitespace() {
                break;
            }
            token.push(ch);
            self.bump();
        }
        match token.as_str() {
            "true" => Some(Value::Bool(true)),
            "false" => Some(Value::Bool(false)),
            "null" | "undefined" => Some(Value::Null),
            "" => None,
            other => other.parse::<i64>().map(Value::from).ok().or_else(|| {
                other
                    .parse::<f64>()
                    .ok()
                    .and_then(serde_json::Number::from_f64)
                    .map(Value::Number)
            }),
        }
    }

    fn expect(&mut self, expected: char) -> Option<()> {
        (self.bump()? == expected).then_some(())
    }
}

#[cfg(test)]
mod tests {
    use super::{extract, parse_js_value};
    use crate::frameworks::Framework;
    use crate::tree_sitter::parse_file_with_frameworks;
    use crate::{FileEntry, Language};
    use serde_json::json;
    use std::path::{Path, PathBuf};

    fn parse(source: &str) -> crate::tree_sitter::ParsedFile {
        let entry = FileEntry {
            path: PathBuf::from("src/repo.ts"),
            language: Language::TypeScript,
            size_bytes: 0,
            content_hash: [0; 32],
            source_bytes: Some(source.as_bytes().to_vec().into()),
        };
        parse_file_with_frameworks("demo", Path::new("/tmp/demo"), &entry, &[Framework::NestJs])
            .expect("parse")
    }

    #[test]
    fn parses_object_with_unquoted_and_operator_keys() {
        let value =
            parse_js_value("{ status: 'active', $set: { 'meta.x': true, n: 3 } }").expect("parses");
        assert_eq!(
            value,
            json!({ "status": "active", "$set": { "meta.x": true, "n": 3 } })
        );
    }

    #[test]
    fn parses_aggregation_pipeline_array() {
        let value = parse_js_value("[{ $match: { status: 'active' } }, { $project: { name: 1 } }]")
            .expect("parses");
        assert_eq!(
            value,
            json!([{ "$match": { "status": "active" } }, { "$project": { "name": 1 } }])
        );
    }

    #[test]
    fn rejects_non_literal_and_unrepresentable_input() {
        assert!(parse_js_value("collection").is_none());
        assert!(parse_js_value("'just a string'").is_none());
        // A value that is an identifier (not a literal) cannot be materialized.
        assert!(parse_js_value("{ when: Date.now() }").is_none());
    }

    #[test]
    fn extractor_materializes_query_and_atlas_index_from_source() {
        let source = r"
            async function run(model) {
                await model.aggregate([
                    { $match: { status: 'active' } },
                    { $lookup: {
                        from: 'users',
                        let: { uid: '$userId' },
                        pipeline: [{ $match: { $expr: { $eq: [{ $toString: '$_id' }, '$$uid'] } } }],
                        as: 'user'
                    } }
                ]);
                await collection.createSearchIndex({
                    name: 'default',
                    definition: { mappings: { dynamic: false, fields: { title: {} } } }
                });
            }
        ";
        let parsed = parse(source);
        let extraction = extract(&parsed);

        assert_eq!(extraction.queries.len(), 1, "one aggregate pipeline");
        assert_eq!(extraction.atlas_indexes.len(), 1, "one search index");
        // The Atlas index is unwrapped from `definition` to the `{ mappings }` shape.
        assert!(
            extraction.atlas_indexes[0].get("mappings").is_some(),
            "definition wrapper is unwrapped"
        );
        // `status` is a referenced field; `$match`/`$lookup` operators are not.
        assert!(extraction.referenced_fields.iter().any(|f| f == "status"));
        assert!(
            !extraction
                .referenced_fields
                .iter()
                .any(|f| f.starts_with('$'))
        );
    }

    #[test]
    fn extractor_yields_nothing_without_mongo_calls() {
        let parsed = parse("const x = compute({ a: 1 }); export default x;");
        assert!(extract(&parsed).is_empty());
    }
}
