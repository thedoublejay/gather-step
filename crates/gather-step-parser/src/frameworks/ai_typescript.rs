//! TypeScript/JavaScript AI augmentation (v5 Phase 2).
//!
//! Detects LangChain-style AI constructs on the working oxc path and emits the
//! v5 AI vocabulary. This increment covers the LLM factory:
//! `LLMFactory.createChatModel({ provider, model, temperature })` → a converged
//! `LlmModel` node (keyed `__llm__<provider>__<model>`) plus an `InvokesLlm`
//! edge from the enclosing symbol. Provider/model must be literal; a factory
//! call whose provider or model is dynamic is skipped rather than fabricated
//! (the project's confidence-banding goal).

use gather_step_core::{
    AiContractDoc, AiContractInferenceKind, AiContractRecord, EdgeData, EdgeKind, EdgeMetadata,
    NodeData, NodeId, NodeKind, ai_contract_external_id, ai_contract_node_id, llm_model_qn,
    ref_node_id,
};

use crate::frameworks::nestjs::extract_object_key_value;
use crate::tree_sitter::{EnrichedCallSite, ParsedFile};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AiTypescriptAugmentation {
    pub nodes: Vec<NodeData>,
    pub edges: Vec<EdgeData>,
}

#[must_use]
pub fn augment(parsed: &ParsedFile) -> AiTypescriptAugmentation {
    let mut augmentation = AiTypescriptAugmentation::default();
    for call_site in &parsed.call_sites {
        if let Some((provider, model)) = chat_model_factory(call_site) {
            emit_llm_model(parsed, call_site, &provider, &model, &mut augmentation);
        }
        if let Some(schema) = structured_output_schema(call_site) {
            emit_ai_contract(parsed, call_site, &schema, &mut augmentation);
        }
    }
    augmentation
}

/// Schema label of a `…withStructuredOutput(<schema>)` call: the referenced
/// schema identifier, or `"inline"` for an inline schema definition (e.g.
/// `z.object({…})`). Inline field extraction is a follow-up; resolving a
/// *named* schema's field shape needs RHS-binding (R2), so a referenced schema
/// is captured by name only here.
fn structured_output_schema(call_site: &EnrichedCallSite) -> Option<String> {
    if call_site.callee_name != "withStructuredOutput" {
        return None;
    }
    let raw = call_site.raw_arguments.as_deref()?.trim();
    if raw.is_empty() {
        return None;
    }
    if raw.contains(['(', '{']) {
        return Some("inline".to_owned());
    }
    Some(raw.to_owned())
}

/// Persistable structured-output contract records for this file, derived from
/// the same `withStructuredOutput` detection that emits the graph nodes — so a
/// contract's node id matches its stored record. Mirrors `infer_payload_contracts`.
#[must_use]
pub fn infer_ai_contracts(parsed: &ParsedFile) -> Vec<AiContractRecord> {
    parsed
        .call_sites
        .iter()
        .filter_map(|call_site| {
            let schema = structured_output_schema(call_site)?;
            Some(structured_output_record(parsed, call_site, &schema))
        })
        .collect()
}

struct ContractIdentity {
    external_id: String,
    target: NodeId,
    target_kind: NodeKind,
    source: NodeId,
}

/// Shared identity for a structured-output contract. No model/schema-definition
/// resolution in R1 (needs receiver/RHS binding), so the contract is keyed to
/// the producing symbol; the schema name carries the identity a reviewer reads.
fn contract_identity(
    parsed: &ParsedFile,
    call_site: &EnrichedCallSite,
    schema_label: &str,
) -> ContractIdentity {
    let source = call_site.owner_id;
    let (target, target_kind) =
        resolve_named_node(parsed, schema_label).unwrap_or((source, NodeKind::Function));
    let external_id = ai_contract_external_id(
        &parsed.file_node.repo,
        &parsed.file_node.file_path,
        target,
        source,
    );
    ContractIdentity {
        external_id,
        target,
        target_kind,
        source,
    }
}

fn emit_ai_contract(
    parsed: &ParsedFile,
    call_site: &EnrichedCallSite,
    schema_label: &str,
    augmentation: &mut AiTypescriptAugmentation,
) {
    let identity = contract_identity(parsed, call_site, schema_label);
    let node = NodeData {
        id: ai_contract_node_id(&identity.external_id),
        kind: NodeKind::AiContract,
        repo: parsed.file_node.repo.clone(),
        file_path: parsed.file_node.file_path.clone(),
        name: schema_label.to_owned(),
        qualified_name: Some(identity.external_id.clone()),
        external_id: Some(identity.external_id),
        signature: None,
        visibility: None,
        span: call_site.span.clone(),
        is_virtual: true,
        ai_role: None,
    };
    let node_id = node.id;
    augmentation.nodes.push(node);
    augmentation.edges.push(EdgeData {
        source: identity.source,
        target: node_id,
        kind: EdgeKind::ProducesAiContract,
        metadata: EdgeMetadata::default(),
        owner_file: parsed.file_node.id,
        is_cross_file: false,
    });
}

fn structured_output_record(
    parsed: &ParsedFile,
    call_site: &EnrichedCallSite,
    schema_label: &str,
) -> AiContractRecord {
    let identity = contract_identity(parsed, call_site, schema_label);
    // R1 captures contract identity/shape-provenance, not field shape: a named
    // schema's fields need RHS-binding (R2), and inline-schema field extraction
    // is a follow-up — so fields stay empty and confidence is banded accordingly.
    let inference_kind = if schema_label == "inline" {
        AiContractInferenceKind::LiteralSchema
    } else {
        AiContractInferenceKind::ReferencedSchema
    };
    let confidence = match inference_kind {
        AiContractInferenceKind::LiteralSchema => 850,
        AiContractInferenceKind::ReferencedSchema => 700,
        AiContractInferenceKind::UsageInferred => 500,
    };
    let source_type_name = (schema_label != "inline").then(|| schema_label.to_owned());
    AiContractRecord {
        ai_contract_node_id: ai_contract_node_id(&identity.external_id),
        contract_target_node_id: identity.target,
        contract_target_kind: identity.target_kind,
        contract_target_qualified_name: None,
        repo: parsed.file_node.repo.clone(),
        file_path: parsed.file_node.file_path.clone(),
        source_symbol_node_id: identity.source,
        line_start: None,
        inference_kind,
        confidence,
        source_type_name: source_type_name.clone(),
        contract: AiContractDoc {
            provider: None,
            model: None,
            temperature: None,
            structured: true,
            schema_format: "zod".to_owned(),
            inference_kind,
            confidence,
            fields: Vec::new(),
            prompt_keys: Vec::new(),
            source_type_name,
        },
    }
}

/// `(id, kind)` of an in-file symbol whose name matches `name`, if one exists.
fn resolve_named_node(parsed: &ParsedFile, name: &str) -> Option<(NodeId, NodeKind)> {
    parsed
        .nodes
        .iter()
        .find(|node| node.name == name)
        .map(|node| (node.id, node.kind))
}

/// `(provider, model)` of a `…createChatModel({ provider, model, … })` factory
/// call, when both are string literals. Dynamic provider/model is skipped so a
/// junk `LlmModel` node is never fabricated.
fn chat_model_factory(call_site: &EnrichedCallSite) -> Option<(String, String)> {
    if call_site.callee_name != "createChatModel" {
        return None;
    }
    let raw = call_site.raw_arguments.as_deref()?;
    let provider = literal_object_value(raw, "provider")?;
    let model = literal_object_value(raw, "model")?;
    Some((provider, model))
}

fn literal_object_value(raw: &str, key: &str) -> Option<String> {
    string_literal(extract_object_key_value(raw, key)?.trim())
}

/// Inner text of a plain quoted string literal (`"x"`/`'x'`/`` `x` ``), or
/// `None` for non-literals (identifiers, member expressions, templates with
/// interpolation).
fn string_literal(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    if bytes.len() < 2 {
        return None;
    }
    let quote = bytes[0];
    if matches!(quote, b'"' | b'\'' | b'`') && bytes[bytes.len() - 1] == quote {
        let inner = &value[1..value.len() - 1];
        if inner.contains(['{', '$']) {
            return None;
        }
        return Some(inner.to_owned());
    }
    None
}

fn emit_llm_model(
    parsed: &ParsedFile,
    call_site: &EnrichedCallSite,
    provider: &str,
    model: &str,
    augmentation: &mut AiTypescriptAugmentation,
) {
    let qualified_name = llm_model_qn(provider, model);
    let node = NodeData {
        id: ref_node_id(NodeKind::LlmModel, &qualified_name),
        kind: NodeKind::LlmModel,
        repo: parsed.file_node.repo.clone(),
        file_path: parsed.file_node.file_path.clone(),
        name: model.to_owned(),
        qualified_name: Some(qualified_name.clone()),
        external_id: Some(qualified_name),
        signature: None,
        visibility: None,
        span: call_site.span.clone(),
        is_virtual: true,
        ai_role: None,
    };
    let node_id = node.id;
    augmentation.nodes.push(node);
    augmentation.edges.push(EdgeData {
        source: call_site.owner_id,
        target: node_id,
        kind: EdgeKind::InvokesLlm,
        metadata: EdgeMetadata::default(),
        owner_file: parsed.file_node.id,
        is_cross_file: false,
    });
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::{EdgeKind, NodeKind};

    use crate::{Language, frameworks::Framework, tree_sitter::parse_file_with_frameworks};

    static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> Self {
            let counter = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-parser-aits-{name}-{}-{counter}",
                process::id()
            ));
            fs::create_dir_all(&path).expect("test directory should be created");
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

    fn parse(dir: &TestDir, file: &str, body: &str) -> crate::tree_sitter::ParsedFile {
        fs::write(dir.path().join(file), body).expect("fixture should write");
        parse_file_with_frameworks(
            "events",
            dir.path(),
            &crate::FileEntry {
                path: file.into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &[Framework::AiTypescript],
        )
        .expect("fixture should parse")
    }

    fn llm_model_ids(parsed: &crate::tree_sitter::ParsedFile) -> Vec<String> {
        let mut ids = parsed
            .nodes
            .iter()
            .filter(|node| node.kind == NodeKind::LlmModel)
            .map(|node| node.external_id.clone().unwrap_or_default())
            .collect::<Vec<_>>();
        ids.sort();
        ids.dedup();
        ids
    }

    fn edge_count(parsed: &crate::tree_sitter::ParsedFile, kind: EdgeKind) -> usize {
        parsed.edges.iter().filter(|edge| edge.kind == kind).count()
    }

    #[test]
    fn create_chat_model_emits_converged_llm_model_and_invokes_edge() {
        let dir = TestDir::new("factory");
        let parsed = parse(
            &dir,
            "agent.ts",
            r#"
import { LLMFactory } from "./model-factory";

export async function compareItems(a: string, b: string) {
    const model = LLMFactory.createChatModel({
        provider: "OPENAI",
        model: "gpt-4.1-mini",
        temperature: 0,
    });
    return model;
}
"#,
        );

        assert_eq!(
            llm_model_ids(&parsed),
            vec!["__llm__openai__gpt-4.1-mini".to_owned()]
        );
        assert_eq!(edge_count(&parsed, EdgeKind::InvokesLlm), 1);
    }

    fn ai_contract_names(parsed: &crate::tree_sitter::ParsedFile) -> Vec<String> {
        let mut names = parsed
            .nodes
            .iter()
            .filter(|node| node.kind == NodeKind::AiContract)
            .map(|node| node.name.clone())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    #[test]
    fn with_structured_output_emits_ai_contract_and_produces_edge() {
        let dir = TestDir::new("structured");
        let parsed = parse(
            &dir,
            "agent.ts",
            r#"
import { LLMFactory } from "./model-factory";
import { ItemComparisonOutputSchema } from "./schema";

export async function compareItems(a: string, b: string) {
    const model = LLMFactory.createChatModel({ provider: "OPENAI", model: "gpt-4.1-mini", temperature: 0 });
    const structured = model.withStructuredOutput(ItemComparisonOutputSchema);
    return structured.invoke({ a, b });
}
"#,
        );

        assert_eq!(
            ai_contract_names(&parsed),
            vec!["ItemComparisonOutputSchema".to_owned()]
        );
        assert_eq!(edge_count(&parsed, EdgeKind::ProducesAiContract), 1);
    }

    #[test]
    fn infer_ai_contracts_yields_a_referenced_schema_record() {
        use gather_step_core::AiContractInferenceKind;

        let dir = TestDir::new("infer");
        let parsed = parse(
            &dir,
            "agent.ts",
            r#"
import { ItemComparisonOutputSchema } from "./schema";

export async function compareItems(model: any) {
    return model.withStructuredOutput(ItemComparisonOutputSchema);
}
"#,
        );

        let contracts = super::infer_ai_contracts(&parsed);
        assert_eq!(contracts.len(), 1);
        let contract = &contracts[0];
        assert_eq!(
            contract.source_type_name.as_deref(),
            Some("ItemComparisonOutputSchema")
        );
        assert_eq!(
            contract.inference_kind,
            AiContractInferenceKind::ReferencedSchema
        );
        assert!(contract.contract.fields.is_empty());
        assert!(contract.contract.structured);
    }

    #[test]
    fn dynamic_provider_or_model_is_skipped() {
        let dir = TestDir::new("dynamic");
        let parsed = parse(
            &dir,
            "agent.ts",
            r#"
import { LLMFactory } from "./model-factory";

export function build(config: { provider: string }) {
    return LLMFactory.createChatModel({ provider: config.provider, model: modelName });
}
"#,
        );

        assert!(llm_model_ids(&parsed).is_empty());
        assert_eq!(edge_count(&parsed, EdgeKind::InvokesLlm), 0);
    }
}
