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

/// Mutating operators whose dangerous document is the *update* argument
/// (`arg 2`), not the filter (`arg 1`): `updateOne(filter, update)`,
/// `findOneAndReplace(filter, replacement, opts)`, etc. For these the filter
/// and the update doc are both scanned; the `$set`/`$toObjectId`/null-parent
/// rules key on the update doc, so scanning only `arg 1` would never fire.
const UPDATE_OPERATORS: &[&str] = &[
    "updateOne",
    "updateMany",
    "findOneAndUpdate",
    "findOneAndReplace",
    "replaceOne",
];

/// Maximum object/array nesting the JS-literal parser descends before bailing.
/// Bounds recursion on attacker-influenceable indexed source so a deeply
/// nested literal cannot overflow the stack and abort the indexer.
const MAX_PARSE_DEPTH: usize = 128;

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
        let callee = call.callee_name.as_str();

        if ATLAS_INDEX_OPERATORS.contains(&callee) {
            if let Some(value) = parse_js_value(first) {
                extraction.atlas_indexes.push(normalize_atlas_index(value));
            }
            continue;
        }

        let is_query = QUERY_OPERATORS.contains(&callee);
        let is_update = UPDATE_OPERATORS.contains(&callee);
        if !is_query && !is_update {
            continue;
        }

        // For mutating ops the dangerous document is the update doc (arg 2);
        // scan both the filter (arg 1) and the update doc. For read ops only
        // arg 1 (the query/pipeline) is structurally interesting.
        let scan_upto = if is_update { 2 } else { 1 };
        for arg in arguments.iter().take(scan_upto) {
            if let Some(value) = parse_js_value(arg) {
                collect_referenced_fields(&value, &mut referenced);
                extraction.queries.push(value);
            }
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

/// Collect the field names referenced by a query/filter/pipeline object so the
/// Atlas drift detector can see which fields the code queries. Mongo operators
/// (keys starting with `$`) are pipeline directives, not fields, so plain keys
/// are skipped; specific operators contribute fields via their own shape:
/// `$match`/`$text` descend into their object; Atlas `$search` exposes its
/// matched field(s) under a nested `path` (string or array of strings);
/// `$group._id` and `$lookup` (`localField`/`foreignField`) name fields by
/// `$`-prefixed string reference.
fn collect_referenced_fields(value: &Value, out: &mut std::collections::BTreeSet<String>) {
    collect_referenced_fields_inner(value, 0, out);
}

fn collect_referenced_fields_inner(
    value: &Value,
    depth: usize,
    out: &mut std::collections::BTreeSet<String>,
) {
    if depth >= MAX_PARSE_DEPTH {
        return;
    }
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if let Some(operator) = key.strip_prefix('$') {
                    match operator {
                        "match" | "text" => collect_referenced_fields_inner(child, depth + 1, out),
                        "search" => collect_search_paths(child, depth + 1, out),
                        "group" => collect_group_fields(child, out),
                        "lookup" => collect_lookup_fields(child, out),
                        _ => {}
                    }
                } else if !key.contains('$') {
                    out.insert(key.clone());
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_referenced_fields_inner(item, depth + 1, out);
            }
        }
        _ => {}
    }
}

/// Atlas `$search` operators name their target field(s) under a `path` key,
/// which is a single string or an array of strings (and may sit one level
/// down under the operator name, e.g. `{ text: { query, path } }`). Pull every
/// referenced field out of any `path` encountered in the subtree.
fn collect_search_paths(value: &Value, depth: usize, out: &mut std::collections::BTreeSet<String>) {
    if depth >= MAX_PARSE_DEPTH {
        return;
    }
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if key == "path" {
                    insert_path_fields(child, out);
                } else {
                    collect_search_paths(child, depth + 1, out);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_search_paths(item, depth + 1, out);
            }
        }
        _ => {}
    }
}

fn insert_path_fields(value: &Value, out: &mut std::collections::BTreeSet<String>) {
    match value {
        Value::String(field) => {
            out.insert(field.clone());
        }
        Value::Array(items) => {
            for item in items {
                insert_path_fields(item, out);
            }
        }
        // `{ value: "field", multi: "..." }` wrapped path form.
        Value::Object(map) => {
            if let Some(Value::String(field)) = map.get("value") {
                out.insert(field.clone());
            }
        }
        _ => {}
    }
}

/// `$group._id` references the grouped field(s) by `$`-prefixed string
/// (`{ _id: "$entity" }` or `{ _id: { e: "$entity" } }`); accumulator
/// expressions likewise reference fields by `$field` strings.
fn collect_group_fields(value: &Value, out: &mut std::collections::BTreeSet<String>) {
    let Value::Object(map) = value else {
        return;
    };
    for child in map.values() {
        collect_field_references(child, out);
    }
}

/// `$lookup` names the local/foreign join fields directly as bare strings.
fn collect_lookup_fields(value: &Value, out: &mut std::collections::BTreeSet<String>) {
    let Value::Object(map) = value else {
        return;
    };
    for key in ["localField", "foreignField"] {
        if let Some(Value::String(field)) = map.get(key) {
            out.insert(field.clone());
        }
    }
}

/// Pull field names out of an aggregation expression: a `$field` string is a
/// field path reference; objects/arrays are descended.
fn collect_field_references(value: &Value, out: &mut std::collections::BTreeSet<String>) {
    match value {
        Value::String(text) => {
            if let Some(field) = text.strip_prefix('$') {
                if !field.is_empty() && !field.starts_with('$') {
                    out.insert(field.to_owned());
                }
            }
        }
        Value::Object(map) => {
            for child in map.values() {
                collect_field_references(child, out);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_field_references(item, out);
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
    parser.skip_trivia();
    if !matches!(parser.peek(), Some('{' | '[')) {
        return None;
    }
    let value = parser.parse_value(0)?;
    // Tolerate a trailing TS cast (`as Filter<T>` / `satisfies UpdateDoc`) and
    // comments after the literal — they do not change the materialized value
    // but would otherwise fail the strict end-of-input check (false negative).
    parser.skip_trivia();
    parser.skip_trailing_cast();
    parser.skip_trivia();
    parser.at_end().then_some(value)
}

/// Recursive-descent parser for the JS-literal subset. Deliberately rejects
/// anything it cannot faithfully turn into a `serde_json::Value` rather than
/// guessing, so the detectors never run on an incorrectly materialized value.
///
/// Backed by an indexed `Vec<char>` rather than a one-char `Peekable` so the
/// trivia skipper can look ahead for `//` / `/* */` comments and a trailing
/// `as` / `satisfies <type>` cast.
struct LiteralParser {
    chars: Vec<char>,
    pos: usize,
}

impl LiteralParser {
    fn new(source: &str) -> Self {
        Self {
            chars: source.chars().collect(),
            pos: 0,
        }
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn peek_at(&self, offset: usize) -> Option<char> {
        self.chars.get(self.pos + offset).copied()
    }

    fn bump(&mut self) -> Option<char> {
        let ch = self.chars.get(self.pos).copied();
        if ch.is_some() {
            self.pos += 1;
        }
        ch
    }

    fn at_end(&self) -> bool {
        self.pos >= self.chars.len()
    }

    /// Skip whitespace and `//` line / `/* */` block comments. Comments may
    /// appear between tokens in real source (`{ /* keep */ status: 1 }`), so
    /// every former `skip_whitespace` call site uses this.
    fn skip_trivia(&mut self) {
        loop {
            match self.peek() {
                Some(ch) if ch.is_whitespace() => {
                    self.bump();
                }
                Some('/') if self.peek_at(1) == Some('/') => {
                    while let Some(ch) = self.peek() {
                        if ch == '\n' {
                            break;
                        }
                        self.bump();
                    }
                }
                Some('/') if self.peek_at(1) == Some('*') => {
                    self.bump();
                    self.bump();
                    while let Some(ch) = self.bump() {
                        if ch == '*' && self.peek() == Some('/') {
                            self.bump();
                            break;
                        }
                    }
                }
                _ => break,
            }
        }
    }

    /// Tolerate a trailing TS type assertion after the literal: `as Filter<T>`
    /// or `satisfies UpdateDoc`. Consumes the keyword and the remaining type
    /// expression up to end-of-input so the strict `at_end` check still passes.
    fn skip_trailing_cast(&mut self) {
        for keyword in ["as", "satisfies"] {
            if self.matches_keyword(keyword) {
                self.pos += keyword.len();
                // The type expression runs to the end of this argument slice.
                self.pos = self.chars.len();
                return;
            }
        }
    }

    /// True when the next chars spell `keyword` followed by a non-identifier
    /// boundary (so `assignee` is not mistaken for the `as` keyword).
    fn matches_keyword(&self, keyword: &str) -> bool {
        let kw: Vec<char> = keyword.chars().collect();
        if self.chars[self.pos..].iter().take(kw.len()).ne(kw.iter()) {
            return false;
        }
        match self.peek_at(kw.len()) {
            Some(ch) => !(ch.is_alphanumeric() || ch == '_' || ch == '$'),
            None => true,
        }
    }

    fn parse_value(&mut self, depth: usize) -> Option<Value> {
        if depth >= MAX_PARSE_DEPTH {
            return None;
        }
        self.skip_trivia();
        match self.peek()? {
            '{' => self.parse_object(depth),
            '[' => self.parse_array(depth),
            '"' | '\'' => self.parse_string().map(Value::String),
            _ => self.parse_keyword_or_number(),
        }
    }

    fn parse_object(&mut self, depth: usize) -> Option<Value> {
        if depth >= MAX_PARSE_DEPTH {
            return None;
        }
        self.expect('{')?;
        let mut map = Map::new();
        loop {
            self.skip_trivia();
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
                    self.skip_trivia();
                    self.expect(':')?;
                    let value = self.parse_value(depth + 1)?;
                    map.insert(key, value);
                }
            }
        }
    }

    fn parse_array(&mut self, depth: usize) -> Option<Value> {
        if depth >= MAX_PARSE_DEPTH {
            return None;
        }
        self.expect('[')?;
        let mut items = Vec::new();
        loop {
            self.skip_trivia();
            match self.peek()? {
                ']' => {
                    self.bump();
                    return Some(Value::Array(items));
                }
                ',' => {
                    self.bump();
                }
                _ => items.push(self.parse_value(depth + 1)?),
            }
        }
    }

    /// Object keys may be quoted (`'$match'`, `"field"`) or bare identifiers
    /// (`field`, `$lookup`, `meta.flags`). Bare keys run up to the next `:`,
    /// whitespace, or terminator.
    fn parse_key(&mut self) -> Option<String> {
        self.skip_trivia();
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

    #[test]
    fn parses_literal_with_trailing_ts_cast_and_comments() {
        // F5: a trailing `as Filter<T>` / `satisfies` cast and `//` / `/* */`
        // comments must not defeat the strict end-of-input check.
        let cast = parse_js_value("{ status: 'active' } as Filter<User>").expect("cast tolerated");
        assert_eq!(cast, json!({ "status": "active" }));

        let satisfies =
            parse_js_value("{ $set: { name: 'x' } } satisfies UpdateDoc").expect("satisfies");
        assert_eq!(satisfies, json!({ "$set": { "name": "x" } }));

        let commented = parse_js_value("{ /* keep */ status: 'active' } // trailing line comment")
            .expect("comments tolerated");
        assert_eq!(commented, json!({ "status": "active" }));
    }

    #[test]
    fn deeply_nested_literal_returns_none_without_panic() {
        // F3: 500 levels of nesting must bail past the depth bound and return
        // None instead of overflowing the stack.
        let mut source = String::new();
        for _ in 0..500 {
            source.push('[');
        }
        for _ in 0..500 {
            source.push(']');
        }
        assert!(parse_js_value(&source).is_none());
    }

    #[test]
    fn update_op_scans_filter_and_update_doc_arguments() {
        // F1: the dangerous doc on `updateOne` is arg 2; both args are scanned
        // and materialized as queries.
        let source = r"
            async function run(model) {
                await model.updateOne(
                    { _id: '123' },
                    { $set: { 'meta.flags.active': true } }
                );
            }
        ";
        let extraction = extract(&parse(source));
        assert_eq!(extraction.queries.len(), 2, "filter + update doc");
        assert!(
            extraction.queries.iter().any(|q| q.get("$set").is_some()),
            "the update doc (arg 2) is captured"
        );
    }

    #[test]
    fn search_path_and_pipeline_stage_fields_are_referenced() {
        // F4: real Atlas `$search` exposes its field under `path`; `$group._id`
        // and `$lookup` local/foreign fields are referenced too.
        let source = r"
            async function run(model) {
                await model.aggregate([
                    { $search: { text: { query: 'x', path: 'entity' } } },
                    { $group: { _id: '$category', n: { $sum: 1 } } },
                    { $lookup: {
                        from: 'users', localField: 'ownerId', foreignField: '_id', as: 'owner'
                    } }
                ]);
            }
        ";
        let extraction = extract(&parse(source));
        let fields = &extraction.referenced_fields;
        assert!(fields.iter().any(|f| f == "entity"), "$search.path field");
        assert!(fields.iter().any(|f| f == "category"), "$group._id field");
        assert!(fields.iter().any(|f| f == "ownerId"), "$lookup localField");
        assert!(fields.iter().any(|f| f == "_id"), "$lookup foreignField");
        assert!(!fields.iter().any(|f| f.starts_with('$')));
    }
}
