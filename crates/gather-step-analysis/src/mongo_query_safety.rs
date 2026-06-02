//! Mongo/Atlas structural safety detectors (WS-G3).
//!
//! These flag AST-detectable antipatterns that recur in ad-hoc mongo queries
//! and aggregations, not just migrations. Each finding carries a stable rule ID
//! and a confidence score so it can be surfaced in review without prose-only
//! noise (the v4.3.1 non-goal "no prose-only findings" applies here).
//!
//! The detectors are pure over a [`serde_json::Value`] modelling a mongo
//! operation (an update document or an aggregation pipeline), so they are
//! unit-testable without a live parser. Wiring the parser's pipeline extraction
//! into these detectors is a separate concern (tracked under MQS1's key files).

use serde_json::Value;

/// MQS1 — an aggregation `$lookup` whose join key is coerced (`$toString` /
/// `$toObjectId`) inside a sub-pipeline, defeating the index on the join field.
pub const RULE_INDEX_DEFEAT: &str = "GS-MONGO-INDEX-DEFEAT";
/// MQS2 — a bare `$toObjectId` coercion (recommend `$convert` with `onError`).
pub const RULE_UNSAFE_COERCION: &str = "GS-MONGO-UNSAFE-COERCION";
/// MQS3 — a `$set` on a dotted path with no existence/`$type:object` guard on
/// the parent, which can clobber or fail on a null/scalar parent.
pub const RULE_NULL_PARENT_PATH: &str = "GS-MONGO-NULL-PARENT-PATH";

/// One mongo-query-safety finding. `path` is a dotted location into the analysed
/// value so a caller can point at the offending stage/field.
#[derive(Clone, Debug, PartialEq)]
pub struct MongoQueryFinding {
    pub rule_id: &'static str,
    pub confidence: f64,
    pub message: String,
    pub path: String,
}

/// Analyse a JSON-shaped mongo operation (update document or aggregation
/// pipeline) for the WS-G3 structural traps. Findings are sorted by
/// `(rule_id, path)` so the output is deterministic.
#[must_use]
pub fn analyze_mongo_value(value: &Value) -> Vec<MongoQueryFinding> {
    let mut findings = Vec::new();
    walk(value, "$", &mut findings);
    findings.sort_by(|left, right| {
        left.rule_id
            .cmp(right.rule_id)
            .then_with(|| left.path.cmp(&right.path))
    });
    findings
}

fn walk(value: &Value, path: &str, findings: &mut Vec<MongoQueryFinding>) {
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
                    "$set" => detect_null_parent_path(child, &child_path, findings),
                    _ => {}
                }
                walk(child, &child_path, findings);
            }
        }
        Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                walk(item, &format!("{path}[{index}]"), findings);
            }
        }
        _ => {}
    }
}

/// MQS1: a `$lookup` is index-defeating when its join key is coerced with
/// `$toString`/`$toObjectId` (typically inside a `let` + sub-`pipeline`). The
/// index-aligned `localField`/`foreignField` form does no coercion.
fn detect_index_defeat(lookup: &Value, path: &str, findings: &mut Vec<MongoQueryFinding>) {
    if mentions_operator(lookup, "$toString") || mentions_operator(lookup, "$toObjectId") {
        findings.push(MongoQueryFinding {
            rule_id: RULE_INDEX_DEFEAT,
            confidence: 0.7,
            message: "`$lookup` coerces its join key (`$toString`/`$toObjectId`), defeating the \
                      index on the join field; align field types or pre-store the join key."
                .to_owned(),
            path: path.to_owned(),
        });
    }
}

/// MQS3: a dotted-path key under `$set` is unguarded when neither the value nor
/// the sibling assignments establish the parent object first.
fn detect_null_parent_path(set_doc: &Value, path: &str, findings: &mut Vec<MongoQueryFinding>) {
    let Value::Object(map) = set_doc else {
        return;
    };
    for (field, expr) in map {
        let Some((parent, _)) = field.rsplit_once('.') else {
            continue;
        };
        if value_guards_parent(expr) || sibling_assigns_parent(map, parent) {
            continue;
        }
        findings.push(MongoQueryFinding {
            rule_id: RULE_NULL_PARENT_PATH,
            confidence: 0.6,
            message: format!(
                "`$set` on dotted path `{field}` has no existence/`$type:object` guard on parent \
                 `{parent}`; a null or scalar parent will be clobbered or error."
            ),
            path: format!("{path}.{field}"),
        });
    }
}

/// The assigned expression itself guards the parent (e.g. `$ifNull`/`$cond`).
fn value_guards_parent(expr: &Value) -> bool {
    mentions_operator(expr, "$ifNull") || mentions_operator(expr, "$cond")
}

/// A sibling key in the same `$set` establishes the parent (or an ancestor) as
/// an object before the dotted write — i.e. a sibling that is exactly the
/// parent path or a prefix-ancestor of it.
fn sibling_assigns_parent(map: &serde_json::Map<String, Value>, parent: &str) -> bool {
    map.keys()
        .any(|key| key == parent || parent.starts_with(&format!("{key}.")))
}

/// Whether `value` contains `op` as an object key anywhere in its tree.
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
mod tests {
    use super::{
        RULE_INDEX_DEFEAT, RULE_NULL_PARENT_PATH, RULE_UNSAFE_COERCION, analyze_mongo_value,
    };
    use serde_json::json;

    fn rule_ids(value: &serde_json::Value) -> Vec<&'static str> {
        analyze_mongo_value(value)
            .into_iter()
            .map(|finding| finding.rule_id)
            .collect()
    }

    #[test]
    fn flags_lookup_that_coerces_join_key_but_not_index_aligned_lookup() {
        // GO4(a): a $lookup coercing the join key with $toString defeats the index.
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

        // An index-aligned localField/foreignField join is clean.
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
        // GO4(b): bare $toObjectId on untrusted input.
        let bare = json!({ "$match": { "_id": { "$toObjectId": "$$req.id" } } });
        assert!(rule_ids(&bare).contains(&RULE_UNSAFE_COERCION));

        // $convert with onError is the safe sibling — no $toObjectId key.
        let safe = json!({
            "$match": {
                "_id": { "$convert": { "input": "$$req.id", "to": "objectId", "onError": null } }
            }
        });
        assert!(!rule_ids(&safe).contains(&RULE_UNSAFE_COERCION));
    }

    #[test]
    fn flags_unguarded_dotted_set_but_not_guarded_sibling() {
        // GO4(c): dotted $set with no parent guard.
        let unguarded = json!({ "$set": { "meta.flags.active": true } });
        assert!(rule_ids(&unguarded).contains(&RULE_NULL_PARENT_PATH));

        // Guarded by wrapping the value in $ifNull.
        let guarded_value =
            json!({ "$set": { "meta.flags.active": { "$ifNull": ["$meta.flags.active", true] } } });
        assert!(!rule_ids(&guarded_value).contains(&RULE_NULL_PARENT_PATH));

        // Guarded by establishing the parent in a sibling assignment.
        let guarded_sibling =
            json!({ "$set": { "meta": { "flags": {} }, "meta.flags.active": true } });
        assert!(!rule_ids(&guarded_sibling).contains(&RULE_NULL_PARENT_PATH));
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
        // Findings come back sorted by (rule_id, path).
        let ids: Vec<_> = first.iter().map(|f| f.rule_id).collect();
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        assert_eq!(ids, sorted);
    }
}
