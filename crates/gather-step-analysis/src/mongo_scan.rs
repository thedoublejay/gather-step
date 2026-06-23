//! Production call site for the Mongo query/Atlas-index safety detectors.
//!
//! [`crate::mongo_query_safety`] holds pure detectors over
//! [`serde_json::Value`]; the parser extractor in
//! `gather_step_parser::frameworks::mongo` materializes the relevant values
//! out of TS/JS source. This module wires the two together: given a parsed
//! file, run every detector over every extracted value and return the merged,
//! deterministically-ordered findings.

use gather_step_parser::frameworks::mongo::{self, MongoExtraction};
use gather_step_parser::tree_sitter::ParsedFile;

use crate::mongo_query_safety::{
    MongoQueryFinding, analyze_atlas_index_drift, analyze_mongo_value,
};

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

#[cfg(test)]
mod tests {
    use super::{scan_extraction, scan_parsed_file};
    use crate::mongo_query_safety::{RULE_ATLAS_INDEX_DRIFT, RULE_INDEX_DEFEAT};
    use gather_step_parser::frameworks::Framework;
    use gather_step_parser::frameworks::mongo::MongoExtraction;
    use gather_step_parser::tree_sitter::parse_file_with_frameworks;
    use gather_step_parser::{FileEntry, Language};
    use serde_json::json;
    use std::path::{Path, PathBuf};

    fn parse(source: &str) -> gather_step_parser::tree_sitter::ParsedFile {
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
    fn flags_atlas_drift_for_unmapped_referenced_field() {
        // `entity` is queried but absent from the `dynamic:false` mapping.
        let extraction = MongoExtraction {
            queries: vec![json!([{ "$search": { "entity": "x" } }])],
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
