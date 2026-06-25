//! Structural safety detectors for Mongo queries, aggregations, and Atlas
//! search-index definitions, plus the production wiring that runs them over a
//! parsed file.
//!
//! The detectors are pure over a [`serde_json::Value`] so they are testable
//! without a live parser. The extractor in [`super::mongo`] materializes the
//! relevant values out of TS/JS source; [`scan_parsed_file`] wires the two
//! together. These live in the parser crate (rather than
//! `gather-step-analysis`) so the storage indexer can call them without a
//! crate cycle.

use serde_json::Value;

use super::mongo::{self, MongoExtraction};
use crate::tree_sitter::ParsedFile;

pub const RULE_INDEX_DEFEAT: &str = "GS-MONGO-INDEX-DEFEAT";
pub const RULE_UNSAFE_COERCION: &str = "GS-MONGO-UNSAFE-COERCION";
pub const RULE_NULL_PARENT_PATH: &str = "GS-MONGO-NULL-PARENT-PATH";
pub const RULE_ATLAS_INDEX_DRIFT: &str = "GS-MONGO-ATLAS-INDEX-DRIFT";

/// Maximum object/array nesting the structural walk descends before bailing.
/// Bounds recursion on attacker-influenceable indexed source so a deeply
/// nested literal cannot overflow the stack and abort the indexer.
pub const MAX_SCAN_DEPTH: usize = 128;

#[derive(Clone, Debug, PartialEq)]
pub struct MongoQueryFinding {
    pub rule_id: &'static str,
    pub confidence: f64,
    pub message: String,
    pub path: String,
}

/// Run the Mongo safety detectors over the queries and Atlas indexes
/// extracted from `parsed`. Findings are sorted by `(rule_id, path)` so the
/// output is stable across runs.
#[must_use]
pub fn scan_parsed_file(parsed: &ParsedFile) -> Vec<MongoQueryFinding> {
    scan_extraction(&mongo::extract(parsed))
}

/// Run the detectors over an already-materialized [`MongoExtraction`]. Split
/// out so callers that extract once can scan without re-parsing.
#[must_use]
pub fn scan_extraction(extraction: &MongoExtraction) -> Vec<MongoQueryFinding> {
    let referenced: Vec<&str> = extraction
        .referenced_fields
        .iter()
        .map(String::as_str)
        .collect();

    let mut findings = Vec::new();
    for query in &extraction.queries {
        findings.extend(analyze_mongo_value(query));
    }
    for index in &extraction.atlas_indexes {
        findings.extend(analyze_atlas_index_drift(index, &referenced));
    }

    findings.sort_by(|left, right| {
        left.rule_id
            .cmp(right.rule_id)
            .then_with(|| left.path.cmp(&right.path))
    });
    findings.dedup();
    findings
}

#[must_use]
pub fn analyze_mongo_value(value: &Value) -> Vec<MongoQueryFinding> {
    let mut findings = Vec::new();
    walk(value, "$", 0, &mut findings);
    findings.sort_by(|left, right| {
        left.rule_id
            .cmp(right.rule_id)
            .then_with(|| left.path.cmp(&right.path))
    });
    findings
}

#[must_use]
pub fn analyze_atlas_index_drift(
    index_def: &Value,
    referenced_fields: &[&str],
) -> Vec<MongoQueryFinding> {
    let mappings = index_def.get("mappings");
    let dynamic = mappings
        .and_then(|m| m.get("dynamic"))
        .and_then(Value::as_bool)
        .unwrap_or(true);
    if dynamic {
        return Vec::new();
    }
    let mut mapped = std::collections::BTreeSet::new();
    if let Some(fields) = mappings.and_then(|m| m.get("fields")) {
        collect_mapped_fields(fields, "", 0, &mut mapped);
    }

    let mut findings: Vec<MongoQueryFinding> = referenced_fields
        .iter()
        .filter(|field| !mapped.contains(**field))
        .map(|field| MongoQueryFinding {
            rule_id: RULE_ATLAS_INDEX_DRIFT,
            confidence: 0.75,
            message: format!(
                "Field `{field}` is queried but absent from the `dynamic:false` Atlas index \
                 mapping; the search silently matches nothing on it. Add it to the mapping."
            ),
            path: (*field).to_owned(),
        })
        .collect();
    findings.sort_by(|left, right| left.path.cmp(&right.path));
    findings
}

fn collect_mapped_fields(
    fields: &Value,
    prefix: &str,
    depth: usize,
    out: &mut std::collections::BTreeSet<String>,
) {
    if depth >= MAX_SCAN_DEPTH {
        return;
    }
    let Some(map) = fields.as_object() else {
        return;
    };
    for (name, def) in map {
        let full = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}.{name}")
        };
        if let Some(nested) = def.get("fields") {
            collect_mapped_fields(nested, &full, depth + 1, out);
        }
        out.insert(full);
    }
}

fn walk(value: &Value, path: &str, depth: usize, findings: &mut Vec<MongoQueryFinding>) {
    if depth >= MAX_SCAN_DEPTH {
        return;
    }
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                let child_path = format!("{path}.{key}");
                match key.as_str() {
                    "$lookup" => detect_index_defeat(child, &child_path, findings),
                    "$toObjectId" => findings.push(MongoQueryFinding {
                        rule_id: RULE_UNSAFE_COERCION,
                        confidence: 0.8,
                        message: "Bare `$toObjectId` throws on a malformed id; use `$convert` \
                                  with an `onError` fallback for untrusted input."
                            .to_owned(),
                        path: child_path.clone(),
                    }),
                    "$set" | "$addFields" => detect_null_parent_path(child, &child_path, findings),
                    _ => {}
                }
                walk(child, &child_path, depth + 1, findings);
            }
        }
        Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                walk(item, &format!("{path}[{index}]"), depth + 1, findings);
            }
        }
        _ => {}
    }
}

fn detect_index_defeat(lookup: &Value, path: &str, findings: &mut Vec<MongoQueryFinding>) {
    // Only the join condition defeats the index. That lives in a `$match` stage
    // of the sub-pipeline; coercion in a `$project`/`$addFields` (merely shaping
    // output) does not, and the `localField`/`foreignField` form has no pipeline.
    let coerces_join_key = lookup
        .get("pipeline")
        .and_then(Value::as_array)
        .is_some_and(|stages| {
            stages.iter().any(|stage| {
                stage.get("$match").is_some_and(|matcher| {
                    mentions_operator(matcher, "$toString")
                        || mentions_operator(matcher, "$toObjectId")
                })
            })
        });
    if coerces_join_key {
        findings.push(MongoQueryFinding {
            rule_id: RULE_INDEX_DEFEAT,
            confidence: 0.7,
            message: "`$lookup` coerces its join key (`$toString`/`$toObjectId`) in the match \
                      stage, defeating the index on the join field; align field types or \
                      pre-store the join key."
                .to_owned(),
            path: path.to_owned(),
        });
    }
}

fn detect_null_parent_path(set_doc: &Value, path: &str, findings: &mut Vec<MongoQueryFinding>) {
    let Value::Object(map) = set_doc else {
        return;
    };
    for field in map.keys() {
        let Some((parent, _)) = field.rsplit_once('.') else {
            continue;
        };
        // Array-positional operators (`$[elem]`, `$[]`, positional `$`) target
        // array elements that exist by virtue of matching the query/arrayFilter,
        // so the null-parent reasoning does not apply — skip rather than flag.
        if has_array_positional_operator(field) {
            continue;
        }
        // Only a sibling assignment that establishes the parent object is a real
        // guard. An `$ifNull`/`$cond` on the assigned value guards the value, not
        // the parent path's existence, so it does not suppress the finding.
        if sibling_assigns_parent(map, parent) {
            continue;
        }
        findings.push(MongoQueryFinding {
            rule_id: RULE_NULL_PARENT_PATH,
            confidence: 0.6,
            message: format!(
                "`$set`/`$addFields` on dotted path `{field}` has no existence/`$type:object` \
                 guard on parent `{parent}`; a null or scalar parent will be clobbered or error."
            ),
            path: format!("{path}.{field}"),
        });
    }
}

/// Whether any segment of a dotted update key is an array-positional operator:
/// the all/filtered positional `$[]` / `$[elem]` (segment starts with `$[`) or
/// the matched-element positional `$` (segment is exactly `$`).
fn has_array_positional_operator(field: &str) -> bool {
    field
        .split('.')
        .any(|segment| segment == "$" || segment.starts_with("$["))
}

fn sibling_assigns_parent(map: &serde_json::Map<String, Value>, parent: &str) -> bool {
    map.keys()
        .any(|key| key == parent || parent.starts_with(&format!("{key}.")))
}

fn mentions_operator(value: &Value, op: &str) -> bool {
    match value {
        Value::Object(map) => map
            .iter()
            .any(|(key, child)| key == op || mentions_operator(child, op)),
        Value::Array(items) => items.iter().any(|item| mentions_operator(item, op)),
        _ => false,
    }
}

#[cfg(test)]
mod detector_tests {
    use super::{
        RULE_INDEX_DEFEAT, RULE_NULL_PARENT_PATH, RULE_UNSAFE_COERCION, analyze_mongo_value,
    };
    use serde_json::{Value, json};

    fn rule_ids(value: &serde_json::Value) -> Vec<&'static str> {
        analyze_mongo_value(value)
            .into_iter()
            .map(|finding| finding.rule_id)
            .collect()
    }

    #[test]
    fn flags_lookup_that_coerces_join_key_but_not_index_aligned_lookup() {
        let coercing = json!([{
            "$lookup": {
                "from": "users",
                "let": { "uid": "$userId" },
                "pipeline": [
                    { "$match": { "$expr": { "$eq": [{ "$toString": "$_id" }, "$$uid"] } } }
                ],
                "as": "user"
            }
        }]);
        assert!(rule_ids(&coercing).contains(&RULE_INDEX_DEFEAT));

        let aligned = json!([{
            "$lookup": {
                "from": "users",
                "localField": "userId",
                "foreignField": "_id",
                "as": "user"
            }
        }]);
        assert!(!rule_ids(&aligned).contains(&RULE_INDEX_DEFEAT));
    }

    #[test]
    fn flags_bare_to_object_id_but_not_convert_with_on_error() {
        let bare = json!({ "$match": { "_id": { "$toObjectId": "$$req.id" } } });
        assert!(rule_ids(&bare).contains(&RULE_UNSAFE_COERCION));

        let safe = json!({
            "$match": {
                "_id": { "$convert": { "input": "$$req.id", "to": "objectId", "onError": null } }
            }
        });
        assert!(!rule_ids(&safe).contains(&RULE_UNSAFE_COERCION));
    }

    #[test]
    fn flags_unguarded_dotted_set_in_set_and_add_fields_but_not_guarded_sibling() {
        let unguarded = json!({ "$set": { "meta.flags.active": true } });
        assert!(rule_ids(&unguarded).contains(&RULE_NULL_PARENT_PATH));

        // `$addFields` is an alias of `$set` and must be covered too.
        let add_fields = json!({ "$addFields": { "meta.flags.active": true } });
        assert!(rule_ids(&add_fields).contains(&RULE_NULL_PARENT_PATH));

        // An `$ifNull` on the assigned value guards the value, not the parent
        // path's existence — so it is still flagged (no false negative).
        let value_only_guard =
            json!({ "$set": { "meta.flags.active": { "$ifNull": ["$meta.flags.active", true] } } });
        assert!(rule_ids(&value_only_guard).contains(&RULE_NULL_PARENT_PATH));

        // A sibling that establishes the parent object IS a real guard.
        let guarded_sibling =
            json!({ "$set": { "meta": { "flags": {} }, "meta.flags.active": true } });
        assert!(!rule_ids(&guarded_sibling).contains(&RULE_NULL_PARENT_PATH));
    }

    #[test]
    fn does_not_flag_null_parent_for_array_positional_operators() {
        // Filtered positional `$[elem]`: the array element exists by matching
        // the arrayFilter, so the null-parent reasoning does not apply.
        let filtered = json!({ "$set": { "companyData.customFields.$[elem].originalValue": 1 } });
        assert!(!rule_ids(&filtered).contains(&RULE_NULL_PARENT_PATH));

        // All-positional `$[]`.
        let all_positional = json!({ "$set": { "tags.$[].value": 1 } });
        assert!(!rule_ids(&all_positional).contains(&RULE_NULL_PARENT_PATH));

        // Trailing positional `$` operator.
        let positional = json!({ "$set": { "organizations.$": 1 } });
        assert!(!rule_ids(&positional).contains(&RULE_NULL_PARENT_PATH));
    }

    #[test]
    fn still_flags_plain_dotted_path_without_array_operator() {
        // A plain dotted path (no array operator) is still the flagged pattern.
        let plain = json!({ "$set": { "automation.organizationScope.deleted": true } });
        assert!(rule_ids(&plain).contains(&RULE_NULL_PARENT_PATH));
    }

    #[test]
    fn lookup_projection_coercion_does_not_flag_index_defeat() {
        // Coercion in a `$project` of the sub-pipeline shapes output; it is not
        // the join key, so it must not be flagged.
        let projecting = json!([{ "$lookup": {
            "from": "u", "localField": "userId", "foreignField": "_id",
            "pipeline": [{ "$project": { "label": { "$toString": "$_id" } } }],
            "as": "u"
        }}]);
        assert!(!rule_ids(&projecting).contains(&RULE_INDEX_DEFEAT));
    }

    #[test]
    fn flags_unmapped_field_on_dynamic_false_index_but_not_mapped_or_nested_field() {
        use super::{RULE_ATLAS_INDEX_DRIFT, analyze_atlas_index_drift};

        let index = json!({
            "mappings": { "dynamic": false, "fields": { "title": {}, "body": {} } }
        });
        let findings = analyze_atlas_index_drift(&index, &["title", "entity"]);
        let ids: Vec<_> = findings.iter().map(|f| f.rule_id).collect();
        assert!(ids.contains(&RULE_ATLAS_INDEX_DRIFT));
        assert!(findings.iter().any(|f| f.path == "entity"));
        assert!(!findings.iter().any(|f| f.path == "title"));

        // A nested mapping (`entity.name`) must not be reported as unmapped.
        let nested = json!({
            "mappings": { "dynamic": false, "fields": { "entity": { "fields": { "name": {} } } } }
        });
        let nested_findings =
            analyze_atlas_index_drift(&nested, &["entity.name", "entity.missing"]);
        assert!(nested_findings.iter().all(|f| f.path != "entity.name"));
        assert!(nested_findings.iter().any(|f| f.path == "entity.missing"));

        let dynamic_index = json!({ "mappings": { "dynamic": true } });
        assert!(analyze_atlas_index_drift(&dynamic_index, &["anything"]).is_empty());
    }

    #[test]
    fn trap_detectors_cover_each_pattern_and_clean_siblings() {
        use super::{
            RULE_ATLAS_INDEX_DRIFT, RULE_INDEX_DEFEAT, RULE_NULL_PARENT_PATH, RULE_UNSAFE_COERCION,
            analyze_atlas_index_drift,
        };

        let trap_a = json!([{ "$lookup": {
            "from": "u", "let": { "k": "$userId" },
            "pipeline": [{ "$match": { "$expr": { "$eq": [{ "$toString": "$_id" }, "$$k"] } } }],
            "as": "u"
        }}]);
        let clean_a = json!([{ "$lookup": {
            "from": "u", "localField": "userId", "foreignField": "_id", "as": "u"
        }}]);
        assert!(rule_ids(&trap_a).contains(&RULE_INDEX_DEFEAT));
        assert!(!rule_ids(&clean_a).contains(&RULE_INDEX_DEFEAT));

        let trap_b = json!({ "_id": { "$toObjectId": "$$req.id" } });
        let clean_b = json!({ "_id": { "$convert": { "input": "$$req.id", "to": "objectId", "onError": null } } });
        assert!(rule_ids(&trap_b).contains(&RULE_UNSAFE_COERCION));
        assert!(!rule_ids(&clean_b).contains(&RULE_UNSAFE_COERCION));

        let trap_c = json!({ "$set": { "meta.flags.active": true } });
        let clean_c = json!({ "$set": { "meta": { "flags": {} }, "meta.flags.active": true } });
        assert!(rule_ids(&trap_c).contains(&RULE_NULL_PARENT_PATH));
        assert!(!rule_ids(&clean_c).contains(&RULE_NULL_PARENT_PATH));

        let index = json!({ "mappings": { "dynamic": false, "fields": { "title": {} } } });
        let trap_d = analyze_atlas_index_drift(&index, &["title", "entity"]);
        assert!(
            trap_d
                .iter()
                .any(|f| f.rule_id == RULE_ATLAS_INDEX_DRIFT && f.path == "entity")
        );
        assert!(!trap_d.iter().any(|f| f.path == "title"));
    }

    #[test]
    fn clean_pipeline_yields_no_findings() {
        let clean = json!([
            { "$match": { "status": "active" } },
            { "$project": { "name": 1, "email": 1 } }
        ]);
        assert!(analyze_mongo_value(&clean).is_empty());
    }

    #[test]
    fn findings_are_deterministically_sorted() {
        let value = json!({
            "$set": { "a.b": true },
            "stage": { "$toObjectId": "$x" }
        });
        let first = analyze_mongo_value(&value);
        let second = analyze_mongo_value(&value);
        assert_eq!(first, second);
        let ids: Vec<_> = first.iter().map(|f| f.rule_id).collect();
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        assert_eq!(ids, sorted);
    }

    #[test]
    fn deeply_nested_value_returns_without_panic() {
        // 500 levels of nesting must not overflow the stack; the bounded walk
        // bails past MAX_SCAN_DEPTH and returns whatever it found above it.
        let mut value = json!({ "$toObjectId": "$x" });
        for _ in 0..500 {
            value = Value::Object(serde_json::Map::from_iter([("a".to_owned(), value)]));
        }
        let findings = analyze_mongo_value(&value);
        // The deeply buried `$toObjectId` is below the depth bound, so it is
        // not reached — the point is that the walk terminates without aborting.
        assert!(findings.is_empty());
    }
}

#[cfg(test)]
mod scan_tests {
    use super::{RULE_ATLAS_INDEX_DRIFT, RULE_INDEX_DEFEAT, scan_extraction, scan_parsed_file};
    use crate::frameworks::Framework;
    use crate::frameworks::mongo::MongoExtraction;
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
    fn flags_index_defeating_lookup_extracted_from_source() {
        let source = r"
            async function run(model) {
                await model.aggregate([
                    { $lookup: {
                        from: 'users',
                        let: { uid: '$userId' },
                        pipeline: [{ $match: { $expr: { $eq: [{ $toString: '$_id' }, '$$uid'] } } }],
                        as: 'user'
                    } }
                ]);
            }
        ";
        let findings = scan_parsed_file(&parse(source));
        assert!(
            findings.iter().any(|f| f.rule_id == RULE_INDEX_DEFEAT),
            "the coercing $lookup join key must be flagged"
        );
    }

    #[test]
    fn flags_null_parent_path_in_update_doc_second_argument() {
        // The dangerous doc on a mutating op is arg 2, not arg 1. The filter
        // (arg 1) is clean; the `$set` update doc (arg 2) has an unguarded
        // dotted path that must be flagged.
        let source = r"
            async function run(model) {
                await model.updateOne(
                    { _id: '123' },
                    { $set: { 'meta.flags.active': true } }
                );
            }
        ";
        let findings = scan_parsed_file(&parse(source));
        assert!(
            findings
                .iter()
                .any(|f| f.rule_id == super::RULE_NULL_PARENT_PATH),
            "the unguarded dotted $set in the update doc (arg 2) must be flagged"
        );
    }

    #[test]
    fn flags_atlas_drift_for_unmapped_referenced_field() {
        // `entity` is queried but absent from the `dynamic:false` mapping.
        let extraction = MongoExtraction {
            queries: vec![json!([{ "$search": { "path": "entity", "query": "x" } }])],
            atlas_indexes: vec![
                json!({ "mappings": { "dynamic": false, "fields": { "title": {} } } }),
            ],
            referenced_fields: vec!["entity".to_owned(), "title".to_owned()],
        };
        let findings = scan_extraction(&extraction);
        assert!(
            findings
                .iter()
                .any(|f| f.rule_id == RULE_ATLAS_INDEX_DRIFT && f.path == "entity")
        );
        assert!(!findings.iter().any(|f| f.path == "title"));
    }

    #[test]
    fn clean_source_yields_no_findings() {
        let source = r"
            async function run(model) {
                await model.find({ status: 'active' });
            }
        ";
        assert!(scan_parsed_file(&parse(source)).is_empty());
    }
}
