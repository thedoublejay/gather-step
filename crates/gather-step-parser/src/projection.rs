use std::{collections::BTreeSet, sync::OnceLock};

use gather_step_core::{EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, virtual_node};
use regex::Regex;

use crate::{traverse::Language, tree_sitter::ParsedFile};

const CONFIDENCE_HIGH: u16 = 900;
const CONFIDENCE_MEDIUM: u16 = 700;

pub(crate) fn augment_projection_fields(parsed: &mut ParsedFile) {
    if !matches!(
        parsed.file.language,
        Language::TypeScript | Language::JavaScript
    ) {
        return;
    }

    let source = parsed.source.as_ref();
    if !should_scan_projection_fields(source, &parsed.file_node.file_path) {
        return;
    }

    let mut known_fields = BTreeSet::new();
    let mut facts = ProjectionFacts::default();
    let has_backfill_context = has_backfill_context(source, &parsed.file_node.file_path);

    if has_schema_field_context(source) {
        for field in property_declarations(source) {
            known_fields.insert(field);
        }
    }

    for (target, value) in object_field_assignments(source) {
        if is_write_context(&value) {
            known_fields.insert(target.clone());
            facts.writes.insert(target.clone());
        }

        let source_fields = source_fields_from_value(&value);
        if is_derivation(&target, &value, &source_fields) {
            known_fields.insert(target.clone());
            facts.writes.insert(target.clone());
            for source_field in source_fields {
                known_fields.insert(source_field.clone());
                facts.reads.insert(source_field.clone());
                facts.derives.insert((source_field, target.clone()));
            }
        }
    }

    for line in source.lines() {
        let fields = line_fields(line, &known_fields);
        if fields.is_empty() {
            continue;
        }
        let write_fields = object_line_fields(line, &known_fields);
        if is_filter_line(line) {
            facts.filters.extend(fields.iter().cloned());
            facts.reads.extend(fields.iter().cloned());
        }
        if is_write_line(line) {
            facts.writes.extend(write_fields.iter().cloned());
        }
        if is_index_line(line) {
            facts.indexes.extend(write_fields.iter().cloned());
        }
        if is_backfill_line(line, &parsed.file_node.file_path)
            || (has_backfill_context && is_write_line(line))
        {
            if write_fields.is_empty() {
                facts.backfills.extend(fields);
            } else {
                facts.backfills.extend(write_fields);
            }
        }
    }

    for field in known_fields
        .iter()
        .chain(facts.reads.iter())
        .chain(facts.writes.iter())
        .chain(facts.filters.iter())
        .chain(facts.indexes.iter())
        .chain(facts.backfills.iter())
    {
        push_node(parsed, data_field_node(parsed, field));
    }
    for (source_field, target_field) in &facts.derives {
        push_node(parsed, data_field_node(parsed, source_field));
        push_node(parsed, data_field_node(parsed, target_field));
        push_edge(
            parsed,
            field_id(parsed, source_field),
            field_id(parsed, target_field),
            EdgeKind::DerivesFieldFrom,
            CONFIDENCE_HIGH,
        );
    }
    for field in facts.reads {
        push_field_edge(parsed, &field, EdgeKind::ReadsField, CONFIDENCE_MEDIUM);
    }
    for field in facts.writes {
        push_field_edge(parsed, &field, EdgeKind::WritesField, CONFIDENCE_HIGH);
    }
    for field in facts.filters {
        push_field_edge(parsed, &field, EdgeKind::FiltersOnField, CONFIDENCE_HIGH);
    }
    for field in facts.indexes {
        push_field_edge(parsed, &field, EdgeKind::IndexesField, CONFIDENCE_HIGH);
    }
    for field in facts.backfills {
        push_field_edge(parsed, &field, EdgeKind::BackfillsField, CONFIDENCE_HIGH);
    }
}

#[derive(Default)]
struct ProjectionFacts {
    derives: BTreeSet<(String, String)>,
    reads: BTreeSet<String>,
    writes: BTreeSet<String>,
    filters: BTreeSet<String>,
    indexes: BTreeSet<String>,
    backfills: BTreeSet<String>,
}

fn property_declarations(source: &str) -> BTreeSet<String> {
    property_re()
        .captures_iter(source)
        .filter_map(|capture| normalize_field_name(capture.get(1)?.as_str()))
        .filter(|field| !is_method_or_noise(field))
        .collect()
}

fn has_schema_field_context(source: &str) -> bool {
    source.contains("@Prop")
        || source.contains("@Schema")
        || source.contains("SchemaFactory")
        || source.contains("new Schema")
        || source.contains("mongoose.Schema")
}

fn should_scan_projection_fields(source: &str, file_path: &str) -> bool {
    !is_false_positive_projection_path(file_path)
        && (has_schema_field_context(source)
            || is_backfill_path(file_path)
            || has_projection_context_token(source)
            || has_projected_object_key(source))
}

fn has_projection_context_token(source: &str) -> bool {
    source.contains("$project")
        || source.contains("$addFields")
        || source.contains("$lookup")
        || source.contains("$set")
        || source.contains("$unset")
        || source.contains("$inc")
        || source.contains("$push")
        || source.contains("$pull")
        || source.contains("$addToSet")
        || source.contains("find(")
        || source.contains("findOne(")
        || source.contains("where(")
        || source.contains("updateOne(")
        || source.contains("updateMany(")
        || source.contains("insertOne(")
        || source.contains("insertMany(")
        || source.contains(".index(")
        || source.contains("createIndex")
        || source.contains("searchIndex")
        || source.contains("backfill")
        || source.contains("migration")
}

fn has_projected_object_key(source: &str) -> bool {
    let bytes = source.as_bytes();
    let mut cursor = 0;
    while cursor < bytes.len() {
        if bytes[cursor] != b':' {
            cursor += 1;
            continue;
        }

        let mut end = cursor;
        while end > 0 && bytes[end - 1].is_ascii_whitespace() {
            end -= 1;
        }
        if end > 0 && matches!(bytes[end - 1], b'"' | b'\'' | b'`') {
            end -= 1;
        }

        let mut start = end;
        while start > 0 && is_field_name_byte(bytes[start - 1]) {
            start -= 1;
        }

        if start < end
            && let Ok(field) = std::str::from_utf8(&bytes[start..end])
            && is_projected_field_name(field)
        {
            return true;
        }
        cursor += 1;
    }
    false
}

fn is_field_name_byte(byte: u8) -> bool {
    byte == b'_' || byte == b'$' || byte.is_ascii_alphanumeric()
}

fn object_field_assignments(source: &str) -> Vec<(String, String)> {
    object_field_re()
        .captures_iter(source)
        .filter_map(|capture| {
            let key = normalize_field_name(capture.get(2)?.as_str())?;
            let value = capture.get(3)?.as_str().trim().to_owned();
            if is_method_or_noise(&key) {
                return None;
            }
            Some((key, value))
        })
        .collect()
}

fn source_fields_from_value(value: &str) -> BTreeSet<String> {
    dotted_access_re()
        .captures_iter(value)
        .filter_map(|capture| normalize_field_name(capture.get(1)?.as_str()))
        .filter(|field| !is_method_or_noise(field))
        .collect()
}

fn line_fields(line: &str, known_fields: &BTreeSet<String>) -> BTreeSet<String> {
    let mut fields = object_line_fields(line, known_fields);
    for capture in dotted_access_re().captures_iter(line) {
        if let Some(field) = capture
            .get(1)
            .and_then(|value| normalize_field_name(value.as_str()))
            && !is_method_or_noise(&field)
        {
            fields.insert(field);
        }
    }
    if is_field_context_line(line) {
        for capture in dotted_string_re().captures_iter(line) {
            if let Some(field) = capture
                .get(1)
                .and_then(|value| normalize_field_name(value.as_str()))
                && !is_method_or_noise(&field)
            {
                fields.insert(field);
            }
        }
    }
    fields.retain(|field| known_fields.contains(field) || is_projected_field_name(field));
    fields
}

fn object_line_fields(line: &str, known_fields: &BTreeSet<String>) -> BTreeSet<String> {
    let mut fields = BTreeSet::new();
    for capture in object_field_re().captures_iter(line) {
        if let Some(field) = capture
            .get(2)
            .and_then(|value| normalize_field_name(value.as_str()))
            && !is_method_or_noise(&field)
        {
            fields.insert(field);
        }
    }
    fields.retain(|field| known_fields.contains(field) || is_projected_field_name(field));
    fields
}

fn is_derivation(target: &str, value: &str, sources: &BTreeSet<String>) -> bool {
    !sources.is_empty()
        && !sources.contains(target)
        && is_projection_target_field(target)
        && (value.contains("?.")
            || value.contains(".map")
            || value.contains(".length")
            || value.contains('.')
            || value.contains("reduce(")
            || value.contains("filter("))
}

fn is_projection_target_field(field: &str) -> bool {
    !is_method_or_noise(field) && !is_container_or_operator_key(field)
}

fn is_projected_field_name(field: &str) -> bool {
    field.ends_with("Ids")
        || field.ends_with("Id")
        || field.ends_with("Count")
        || field.ends_with("Total")
        || field.ends_with("Status")
}

fn is_container_or_operator_key(field: &str) -> bool {
    matches!(
        field,
        "where"
            | "data"
            | "select"
            | "include"
            | "orderBy"
            | "fields"
            | "mapping"
            | "properties"
            | "$set"
            | "$unset"
            | "$inc"
            | "$push"
            | "$pull"
            | "$addToSet"
            | "$project"
            | "$match"
    )
}

fn is_write_context(value: &str) -> bool {
    value.contains("$set")
        || value.contains("$unset")
        || value.contains("$inc")
        || value.contains("$addFields")
        || value.contains("$push")
        || value.contains("$pull")
        || value.contains("$addToSet")
}

fn is_filter_line(line: &str) -> bool {
    contains_ascii_case(line, "find(")
        || contains_ascii_case(line, "findone(")
        || contains_ascii_case(line, "$match")
        || contains_ascii_case(line, "filterby")
        || contains_ascii_case(line, "where(")
        || contains_ascii_case(line, "match(")
}

fn is_write_line(line: &str) -> bool {
    contains_ascii_case(line, "$set")
        || contains_ascii_case(line, "$unset")
        || contains_ascii_case(line, "$inc")
        || contains_ascii_case(line, "$addfields")
        || contains_ascii_case(line, "$push")
        || contains_ascii_case(line, "$pull")
        || contains_ascii_case(line, "$addtoset")
        || contains_ascii_case(line, "updateone(")
        || contains_ascii_case(line, "updatemany(")
        || contains_ascii_case(line, "insertone(")
        || contains_ascii_case(line, "insertmany(")
        || contains_ascii_case(line, "save(")
}

fn is_index_line(line: &str) -> bool {
    contains_ascii_case(line, ".index(")
        || contains_ascii_case(line, "createindex")
        || contains_ascii_case(line, "searchindex")
        || contains_ascii_case(line, "atlas")
        || contains_ascii_case(line, "mapping")
}

fn is_backfill_line(line: &str, file_path: &str) -> bool {
    is_backfill_path(file_path)
        || contains_ascii_case(line, "migration")
        || contains_ascii_case(line, "backfill")
}

fn is_backfill_path(file_path: &str) -> bool {
    contains_ascii_case(file_path, "migration") || contains_ascii_case(file_path, "backfill")
}

fn has_backfill_context(source: &str, file_path: &str) -> bool {
    is_backfill_path(file_path)
        || contains_ascii_case(source, "migration")
        || contains_ascii_case(source, "backfill")
}

fn is_false_positive_projection_path(file_path: &str) -> bool {
    let normalized = file_path.replace('\\', "/");
    normalized.starts_with("__mocks__/")
        || normalized.starts_with("mocks/")
        || normalized.starts_with("ui/")
        || normalized.starts_with("components/")
        || normalized.starts_with("i18n/")
        || normalized.starts_with("locales/")
        || normalized.starts_with("translations/")
        || contains_ascii_case(&normalized, "/__mocks__/")
        || contains_ascii_case(&normalized, "/mocks/")
        || contains_ascii_case(&normalized, ".mock.")
        || contains_ascii_case(&normalized, ".test.")
        || contains_ascii_case(&normalized, ".spec.")
        || contains_ascii_case(&normalized, ".stories.")
        || contains_ascii_case(&normalized, "/components/")
        || contains_ascii_case(&normalized, "/ui/")
        || contains_ascii_case(&normalized, "/i18n/")
        || contains_ascii_case(&normalized, "/locales/")
        || contains_ascii_case(&normalized, "/translations/")
        || contains_ascii_case(&normalized, "translation")
}

fn is_field_context_line(line: &str) -> bool {
    is_filter_line(line) || is_index_line(line) || is_write_line(line) || line.contains("$project")
}

fn contains_ascii_case(haystack: &str, needle: &str) -> bool {
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle.as_bytes()))
}

fn normalize_field_name(raw: &str) -> Option<String> {
    let trimmed = raw
        .trim()
        .trim_matches('"')
        .trim_matches('\'')
        .trim_matches('`')
        .trim();
    if trimmed.is_empty() {
        return None;
    }
    let field = trimmed.rsplit('.').next().unwrap_or(trimmed);
    if !field
        .chars()
        .next()
        .is_some_and(|ch| ch == '_' || ch == '$' || ch.is_ascii_alphabetic())
    {
        return None;
    }
    Some(field.to_owned())
}

fn is_method_or_noise(field: &str) -> bool {
    matches!(
        field,
        "map"
            | "filter"
            | "reduce"
            | "forEach"
            | "length"
            | "toString"
            | "toLowerCase"
            | "toUpperCase"
            | "trim"
            | "then"
            | "catch"
            | "emit"
            | "log"
            | "debug"
            | "info"
            | "warn"
            | "error"
    )
}

fn data_field_node(parsed: &ParsedFile, field: &str) -> NodeData {
    let qualified_name = data_field_qn(&parsed.file_node.repo, &parsed.file_node.file_path, field);
    virtual_node(
        NodeKind::DataField,
        parsed.file_node.repo.clone(),
        parsed.file_node.file_path.clone(),
        field.to_owned(),
        qualified_name,
    )
}

fn field_id(parsed: &ParsedFile, field: &str) -> gather_step_core::NodeId {
    data_field_node(parsed, field).id
}

fn data_field_qn(repo: &str, file_path: &str, field: &str) -> String {
    format!("data-field::{repo}::{file_path}::{field}")
}

fn push_node(parsed: &mut ParsedFile, node: NodeData) {
    if !parsed.nodes.iter().any(|existing| existing.id == node.id) {
        parsed.nodes.push(node);
    }
}

fn push_field_edge(parsed: &mut ParsedFile, field: &str, kind: EdgeKind, confidence: u16) {
    push_node(parsed, data_field_node(parsed, field));
    push_edge(
        parsed,
        parsed.file_node.id,
        field_id(parsed, field),
        kind,
        confidence,
    );
}

fn push_edge(
    parsed: &mut ParsedFile,
    source: gather_step_core::NodeId,
    target: gather_step_core::NodeId,
    kind: EdgeKind,
    confidence: u16,
) {
    let edge = EdgeData {
        source,
        target,
        kind,
        metadata: EdgeMetadata {
            confidence: Some(confidence),
            ..EdgeMetadata::default()
        },
        owner_file: parsed.file_node.id,
        is_cross_file: false,
    };
    if !parsed.edges.iter().any(|existing| {
        existing.source == edge.source
            && existing.target == edge.target
            && existing.kind == edge.kind
            && existing.owner_file == edge.owner_file
    }) {
        parsed.edges.push(edge);
    }
}

fn property_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?m)^\s*(?:public\s+|private\s+|protected\s+|readonly\s+|static\s+)*([A-Za-z_$][A-Za-z0-9_$]*)[!?]?\s*:\s*[^;=\n]+;",
        )
        .expect("property regex should compile")
    })
}

fn object_field_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r#"(?m)(^|[,{]\s*)["']?([A-Za-z_$][A-Za-z0-9_$.]*)["']?\s*:\s*([^,\n}{;]+)"#)
            .expect("object field regex should compile")
    })
}

fn dotted_access_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?:this|[A-Za-z_$][A-Za-z0-9_$]*)\??\.([A-Za-z_$][A-Za-z0-9_$]*)")
            .expect("dotted access regex should compile")
    })
}

fn dotted_string_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r#"["']([A-Za-z_$][A-Za-z0-9_$]*(?:\.[A-Za-z_$][A-Za-z0-9_$]*)+)["']"#)
            .expect("dotted string regex should compile")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traverse::{FileEntry, Language};
    use gather_step_core::{NodeKind, node_id};

    fn parsed(source: &str, path: &str) -> ParsedFile {
        let file_node = NodeData {
            id: node_id("svc", path, NodeKind::File, path),
            kind: NodeKind::File,
            repo: "svc".to_owned(),
            file_path: path.to_owned(),
            name: path.to_owned(),
            qualified_name: Some(path.to_owned()),
            external_id: None,
            signature: None,
            visibility: None,
            span: None,
            is_virtual: false,
        };
        ParsedFile {
            file: FileEntry {
                path: path.into(),
                language: Language::TypeScript,
                size_bytes: source.len() as u64,
                content_hash: [0; 32],
                source_bytes: None,
            },
            source_path: path.into(),
            source: source.into(),
            file_node,
            nodes: Vec::new(),
            edges: Vec::new(),
            symbols: Vec::new(),
            call_sites: Vec::new(),
            import_bindings: Vec::new(),
            constant_strings: rustc_hash::FxHashMap::default(),
            parse_ms: 0,
        }
    }

    #[test]
    fn emits_projection_derivation_edges() {
        let mut parsed = parsed(
            "const dto = { subtaskIds: task.subtasks?.map((subtask) => subtask.id) };",
            "src/task.ts",
        );

        augment_projection_fields(&mut parsed);

        assert!(
            parsed
                .nodes
                .iter()
                .any(|node| node.kind == NodeKind::DataField && node.name == "subtaskIds")
        );
        assert!(
            parsed
                .nodes
                .iter()
                .any(|node| node.kind == NodeKind::DataField && node.name == "subtasks")
        );
        assert!(
            parsed
                .edges
                .iter()
                .any(|edge| edge.kind == EdgeKind::DerivesFieldFrom)
        );
    }

    #[test]
    fn ignores_unrelated_local_count_names() {
        let mut parsed = parsed(
            "const totalCount = items.length; console.log('subtaskIds');",
            "src/local.ts",
        );

        augment_projection_fields(&mut parsed);

        assert!(
            parsed
                .edges
                .iter()
                .all(|edge| edge.kind != EdgeKind::DerivesFieldFrom)
        );
    }

    #[test]
    fn ignores_unprojected_generic_object_literals() {
        let mut parsed = parsed(
            "const result = { first: items.map((item) => item.value), item: current.value };",
            "src/generic.ts",
        );

        augment_projection_fields(&mut parsed);

        assert!(
            parsed
                .nodes
                .iter()
                .all(|node| node.kind != NodeKind::DataField)
        );
    }

    #[test]
    fn write_lines_do_not_mark_source_fields_as_written() {
        let mut parsed = parsed(
            "await TaskModel.updateMany({}, { $set: { subtaskIds: task.subtasks.map((subtask) => subtask.id) } });",
            "src/migrations/backfill.ts",
        );

        augment_projection_fields(&mut parsed);

        let written = parsed
            .edges
            .iter()
            .filter(|edge| edge.kind == EdgeKind::WritesField)
            .filter_map(|edge| parsed.nodes.iter().find(|node| node.id == edge.target))
            .map(|node| node.name.as_str())
            .collect::<BTreeSet<_>>();
        assert!(written.contains("subtaskIds"));
        assert!(!written.contains("subtasks"));
    }

    #[test]
    fn field_identity_includes_evidence_file_path() {
        let source = "const dto = { subtaskIds: task.subtasks?.map((subtask) => subtask.id) };";
        let mut first = parsed(source, "src/task_projection.ts");
        let mut second = parsed(source, "src/other_projection.ts");

        augment_projection_fields(&mut first);
        augment_projection_fields(&mut second);

        let first_field = first
            .nodes
            .iter()
            .find(|node| node.kind == NodeKind::DataField && node.name == "subtaskIds")
            .expect("first field should be indexed");
        let second_field = second
            .nodes
            .iter()
            .find(|node| node.kind == NodeKind::DataField && node.name == "subtaskIds")
            .expect("second field should be indexed");
        assert_ne!(first_field.id, second_field.id);
        assert!(
            first_field
                .qualified_name
                .as_deref()
                .is_some_and(|name| name.contains("src/task_projection.ts::subtaskIds"))
        );
    }
}
