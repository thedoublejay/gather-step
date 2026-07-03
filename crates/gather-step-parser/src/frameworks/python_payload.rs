//! Python payload-contract inference (v5.10).
//!
//! Mirrors the TypeScript payload pass in [`crate::payload`] for Python: a
//! class that subclasses `BaseModel` (Pydantic v1/v2, direct inheritance) or
//! `TypedDict`, or that carries a `@dataclass` decorator, is a contract source.
//! Its annotated fields become [`PayloadField`]s (types kept raw). When a
//! `FastAPI` route handler in the same file takes a parameter annotated with such
//! a class, a consumer-side record is attached to the route; `response_model=`
//! kwargs and return annotations naming a contract class attach a producer-side
//! record. The emitted records reuse the exact record/edge/node shapes of the
//! TypeScript pass, so the store and `pr-review` see them identically with zero
//! downstream changes.
//!
//! Out of scope (v5.10): indirect/nested inheritance (`Base -> MyBase ->
//! Model`) and cross-file model resolution.

use gather_step_core::{
    EdgeData, EdgeKind, EdgeMetadata, NodeKind, PayloadContractDoc, PayloadContractRecord,
    PayloadField, PayloadInferenceKind, PayloadSide, payload_contract_external_id,
    payload_contract_node_id, ref_node_id, route_qn,
};
use rustc_hash::FxHashMap;

use crate::frameworks::fastapi::{HTTP_METHODS, route_method_and_path};
use crate::payload::{InferredPayloadContract, payload_contract_node};
use crate::top_level_split::split_top_level;
use crate::traverse::Language;
use crate::tree_sitter::{ParsedFile, SymbolCapture};

const FIELD_CONFIDENCE: u16 = 900;
const CONTRACT_CONFIDENCE: u16 = 900;

pub fn infer(parsed: &ParsedFile) -> Vec<InferredPayloadContract> {
    if parsed.file.language != Language::Python {
        return Vec::new();
    }

    let mut contracts: FxHashMap<String, Vec<PayloadField>> = FxHashMap::default();
    for symbol in &parsed.symbols {
        if symbol.node.kind != NodeKind::Class || !is_contract_class(symbol) {
            continue;
        }
        let fields = contract_fields(parsed, symbol);
        if !fields.is_empty() {
            contracts.insert(symbol.node.name.clone(), fields);
        }
    }
    if contracts.is_empty() {
        return Vec::new();
    }

    let mut inferred = Vec::new();
    for symbol in &parsed.symbols {
        let Some((method, path)) = route_method_and_path(symbol, &parsed.router_prefixes) else {
            continue;
        };
        let qn = route_qn(&method, &path);
        let target = ref_node_id(NodeKind::Route, &qn);

        if let Some((type_name, fields)) = consumer_contract(symbol, &contracts) {
            inferred.push(build(
                parsed,
                symbol,
                target,
                &qn,
                PayloadSide::Consumer,
                type_name,
                fields,
            ));
        }
        if let Some((type_name, fields)) = producer_contract(symbol, &contracts) {
            inferred.push(build(
                parsed,
                symbol,
                target,
                &qn,
                PayloadSide::Producer,
                type_name,
                fields,
            ));
        }
    }
    inferred
}

fn is_contract_class(symbol: &SymbolCapture) -> bool {
    let base_hit = symbol.base_classes.iter().any(|base| {
        let head = base.rsplit('.').next().unwrap_or(base);
        head == "BaseModel" || head == "TypedDict"
    });
    base_hit
        || symbol
            .decorators
            .iter()
            .any(|decorator| decorator.name == "dataclass")
}

fn contract_fields(parsed: &ParsedFile, symbol: &SymbolCapture) -> Vec<PayloadField> {
    let typed_dict_total_false =
        is_typed_dict_class(symbol) && typed_dict_total_false(parsed, symbol);
    symbol
        .python_class_fields
        .iter()
        .filter(|field| !is_class_var(&field.type_annotation))
        .map(|field| PayloadField {
            name: field.name.clone(),
            type_name: field.type_annotation.clone(),
            optional: field_is_optional(
                field.has_default,
                typed_dict_total_false,
                &field.type_annotation,
            ),
            confidence: FIELD_CONFIDENCE,
        })
        .collect()
}

/// Consumer side: the first handler parameter annotated with a contract class.
/// Only the first match is emitted so the `(target, symbol, side)`-keyed
/// external id stays unique, mirroring the TypeScript pass.
fn consumer_contract(
    symbol: &SymbolCapture,
    contracts: &FxHashMap<String, Vec<PayloadField>>,
) -> Option<(String, Vec<PayloadField>)> {
    let signature = symbol.node.signature.as_deref()?;
    let params = signature_params(signature)?;
    for param in split_top_level(params, ',') {
        let Some((_, annotation)) = param.split_once(':') else {
            continue;
        };
        let type_name = annotation_type_head(annotation);
        if let Some(fields) = contracts.get(&type_name) {
            return Some((type_name, fields.clone()));
        }
    }
    None
}

/// Producer side: a `response_model=<Class>` decorator kwarg, else the handler's
/// `-> <Class>` return annotation.
fn producer_contract(
    symbol: &SymbolCapture,
    contracts: &FxHashMap<String, Vec<PayloadField>>,
) -> Option<(String, Vec<PayloadField>)> {
    if let Some(type_name) = response_model_type(symbol)
        && let Some(fields) = contracts.get(&type_name)
    {
        return Some((type_name, fields.clone()));
    }
    let signature = symbol.node.signature.as_deref()?;
    let type_name = annotation_type_head(return_annotation(signature)?);
    let fields = contracts.get(&type_name)?;
    Some((type_name, fields.clone()))
}

fn response_model_type(symbol: &SymbolCapture) -> Option<String> {
    let decorator = symbol
        .decorators
        .iter()
        .find(|decorator| HTTP_METHODS.contains(&decorator.name.as_str()))?;
    decorator.arguments.iter().find_map(|argument| {
        let (key, value) = argument.split_once('=')?;
        (key.trim() == "response_model").then(|| annotation_type_head(value))
    })
}

fn signature_params(signature: &str) -> Option<&str> {
    let open = signature.find('(')?;
    let close = signature.rfind(')')?;
    (close > open).then(|| signature[open + 1..close].trim())
}

fn return_annotation(signature: &str) -> Option<&str> {
    let close = signature.rfind(')')?;
    signature[close + 1..]
        .split_once("->")
        .map(|(_, rest)| rest.trim())
        .filter(|annotation| !annotation.is_empty())
}

/// Reduce a raw annotation to the referenced contract type name.
fn annotation_type_head(annotation: &str) -> String {
    let annotation = split_top_level(annotation, '=')
        .into_iter()
        .next()
        .unwrap_or("")
        .trim();
    unwrap_contract_annotation(annotation)
}

fn unwrap_contract_annotation(annotation: &str) -> String {
    let mut current = annotation.trim().to_owned();
    loop {
        let next = unwrap_one_contract_annotation(&current);
        if next == current {
            return next.trim().to_owned();
        }
        current = next;
    }
}

fn unwrap_one_contract_annotation(annotation: &str) -> String {
    let annotation = annotation.trim();
    if annotation.contains('|')
        && let Some(inner) = annotation
            .split('|')
            .map(str::trim)
            .find(|part| !part.is_empty() && *part != "None")
    {
        return inner.to_owned();
    }
    if let Some(open) = annotation.find('[') {
        let head = annotation[..open].trim();
        let head_simple = head.rsplit('.').next().unwrap_or(head);
        let inner = annotation[open + 1..].trim_end_matches(']').trim();
        if matches!(head_simple, "Optional" | "Required" | "NotRequired") {
            return inner.to_owned();
        }
        if head_simple == "Annotated" {
            return split_top_level(inner, ',')
                .into_iter()
                .next()
                .unwrap_or("")
                .trim()
                .to_owned();
        }
        if matches!(head_simple, "list" | "List" | "Sequence") {
            return inner.to_owned();
        }
    }
    annotation.to_owned()
}

fn is_typed_dict_class(symbol: &SymbolCapture) -> bool {
    symbol.base_classes.iter().any(|base| {
        let head = base.rsplit('.').next().unwrap_or(base);
        head == "TypedDict"
    })
}

fn typed_dict_total_false(parsed: &ParsedFile, symbol: &SymbolCapture) -> bool {
    let Some(span) = symbol.node.span.as_ref() else {
        return false;
    };
    let start = span.line_start.saturating_sub(1) as usize;
    let mut header_lines = Vec::new();
    for line in parsed.source.lines().skip(start) {
        header_lines.push(line);
        if line.contains(':') {
            break;
        }
    }
    let header = header_lines.join(" ");
    header
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>()
        .contains("total=False")
}

fn field_is_optional(has_default: bool, typed_dict_total_false: bool, annotation: &str) -> bool {
    if is_required_annotation(annotation) {
        return has_default;
    }
    has_default || typed_dict_total_false || is_optional_annotation(annotation)
}

fn is_optional_annotation(annotation: &str) -> bool {
    let annotation = annotation.trim();
    if is_not_required_annotation(annotation) {
        return true;
    }
    if let Some(inner) = generic_inner(annotation, "Annotated") {
        return is_optional_annotation(inner);
    }
    let head_simple = annotation
        .split(['[', ' '])
        .next()
        .unwrap_or(annotation)
        .rsplit('.')
        .next()
        .unwrap_or(annotation);
    head_simple == "Optional" || annotation.split('|').any(|part| part.trim() == "None")
}

fn is_required_annotation(annotation: &str) -> bool {
    generic_inner(annotation, "Required").is_some()
}

fn is_not_required_annotation(annotation: &str) -> bool {
    generic_inner(annotation, "NotRequired").is_some()
}

fn is_class_var(annotation: &str) -> bool {
    let head_simple = annotation
        .trim()
        .split(['[', ' '])
        .next()
        .unwrap_or(annotation)
        .rsplit('.')
        .next()
        .unwrap_or(annotation);
    head_simple == "ClassVar"
}

fn generic_inner<'a>(annotation: &'a str, expected_head: &str) -> Option<&'a str> {
    let annotation = annotation.trim();
    let open = annotation.find('[')?;
    let head = annotation[..open].trim().rsplit('.').next().unwrap_or("");
    if head != expected_head {
        return None;
    }
    Some(annotation[open + 1..].trim_end_matches(']').trim())
}

fn build(
    parsed: &ParsedFile,
    symbol: &SymbolCapture,
    target: gather_step_core::NodeId,
    qn: &str,
    side: PayloadSide,
    source_type_name: String,
    fields: Vec<PayloadField>,
) -> InferredPayloadContract {
    let external_id = payload_contract_external_id(
        &parsed.file_node.repo,
        &parsed.file_node.file_path,
        target,
        symbol.node.id,
        side,
    );
    let line_start = symbol.node.span.as_ref().map(|span| span.line_start);
    let contract = PayloadContractDoc {
        content_type: "application/json".to_owned(),
        schema_format: "normalized_object".to_owned(),
        side,
        inference_kind: PayloadInferenceKind::TypedParameter,
        confidence: CONTRACT_CONFIDENCE,
        fields,
        source_type_name: Some(source_type_name.clone()),
    };
    InferredPayloadContract {
        node: payload_contract_node(parsed, &external_id, line_start),
        edge: EdgeData {
            source: payload_contract_node_id(&external_id),
            target,
            kind: EdgeKind::ContractOn,
            metadata: EdgeMetadata {
                confidence: Some(CONTRACT_CONFIDENCE),
                ..EdgeMetadata::default()
            },
            owner_file: parsed.file_node.id,
            is_cross_file: false,
        },
        record: PayloadContractRecord {
            payload_contract_node_id: payload_contract_node_id(&external_id),
            contract_target_node_id: target,
            contract_target_kind: NodeKind::Route,
            contract_target_qualified_name: Some(qn.to_owned()),
            repo: parsed.file_node.repo.clone(),
            file_path: parsed.file_node.file_path.clone(),
            source_symbol_node_id: symbol.node.id,
            line_start,
            side,
            inference_kind: PayloadInferenceKind::TypedParameter,
            confidence: CONTRACT_CONFIDENCE,
            source_type_name: Some(source_type_name),
            contract,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::{NodeKind, PayloadSide, ref_node_id, route_qn};

    use crate::{
        Language, frameworks::Framework, payload::infer_payload_contracts,
        tree_sitter::parse_file_with_frameworks,
    };

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(name: &str) -> Self {
            let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-pypayload-{name}-{}-{id}",
                process::id()
            ));
            fs::create_dir_all(&path).expect("temp dir");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn parse(source: &str) -> crate::tree_sitter::ParsedFile {
        let temp = TempDir::new("case");
        fs::write(temp.path().join("api.py"), source).expect("fixture");
        parse_file_with_frameworks(
            "py_service",
            temp.path(),
            &crate::FileEntry {
                path: "api.py".into(),
                language: Language::Python,
                size_bytes: u64::try_from(source.len()).unwrap_or(u64::MAX),
                content_hash: *blake3::hash(source.as_bytes()).as_bytes(),
                source_bytes: None,
            },
            &[Framework::FastApi],
        )
        .expect("parse")
    }

    #[test]
    fn pydantic_basemodel_consumer_on_route() {
        let parsed = parse(
            r#"
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()


class ItemCreate(BaseModel):
    name: str
    price: float | None


@app.post("/items")
def create_item(item: ItemCreate):
    return {}
"#,
        );
        let inferred = infer_payload_contracts(&parsed);
        let consumer = inferred
            .iter()
            .find(|item| item.record.side == PayloadSide::Consumer)
            .expect("consumer contract");
        assert_eq!(
            consumer.record.source_type_name.as_deref(),
            Some("ItemCreate")
        );
        assert_eq!(consumer.record.contract_target_kind, NodeKind::Route);
        assert_eq!(
            consumer.record.contract_target_node_id,
            ref_node_id(NodeKind::Route, &route_qn("POST", "/items"))
        );
        let fields = &consumer.record.contract.fields;
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "name");
        assert_eq!(fields[0].type_name, "str");
        assert!(!fields[0].optional);
        assert_eq!(fields[1].name, "price");
        assert_eq!(fields[1].type_name, "float | None");
        assert!(fields[1].optional, "X | None must be optional");
    }

    #[test]
    fn dataclass_and_typeddict_are_contract_sources() {
        let parsed = parse(
            r#"
from dataclasses import dataclass
from typing import TypedDict
from fastapi import APIRouter

router = APIRouter()


@dataclass
class Money:
    amount: int
    currency: str = "USD"


class Filters(TypedDict):
    tags: list[str]


@router.post("/pay")
def pay(m: Money):
    return {}


@router.post("/search")
def search(f: Filters):
    return {}
"#,
        );
        let inferred = infer_payload_contracts(&parsed);
        let money = inferred
            .iter()
            .find(|i| i.record.source_type_name.as_deref() == Some("Money"))
            .expect("dataclass consumer");
        assert_eq!(money.record.contract.fields.len(), 2);
        assert!(
            money.record.contract.fields[1].optional,
            "field with a default is optional"
        );
        let filters = inferred
            .iter()
            .find(|i| i.record.source_type_name.as_deref() == Some("Filters"))
            .expect("typeddict consumer");
        assert_eq!(filters.record.contract.fields[0].type_name, "list[str]");
    }

    #[test]
    fn optionality_matrix() {
        let parsed = parse(
            r#"
from typing import Optional, ClassVar
from pydantic import BaseModel
from fastapi import FastAPI

app = FastAPI()


class Sample(BaseModel):
    required: int
    opt_generic: Optional[str]
    union_none: str | None
    has_default: int = 3
    shared: ClassVar[int] = 0


@app.put("/sample")
def put_sample(s: Sample):
    return {}
"#,
        );
        let inferred = infer_payload_contracts(&parsed);
        let consumer = inferred
            .iter()
            .find(|item| item.record.side == PayloadSide::Consumer)
            .expect("consumer");
        let fields = &consumer.record.contract.fields;
        assert_eq!(
            fields.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(),
            vec!["required", "opt_generic", "union_none", "has_default"],
            "ClassVar must be excluded"
        );
        assert!(!fields[0].optional);
        assert!(fields[1].optional);
        assert!(fields[2].optional);
        assert!(fields[3].optional);
    }

    #[test]
    fn producer_from_response_model_kwarg() {
        let parsed = parse(
            r#"
from pydantic import BaseModel
from fastapi import FastAPI

app = FastAPI()


class Item(BaseModel):
    id: int


@app.post("/items", response_model=Item)
def create_item():
    return {}
"#,
        );
        let inferred = infer_payload_contracts(&parsed);
        let producer = inferred
            .iter()
            .find(|item| item.record.side == PayloadSide::Producer)
            .expect("producer contract");
        assert_eq!(producer.record.source_type_name.as_deref(), Some("Item"));
        assert_eq!(
            producer.record.contract_target_node_id,
            ref_node_id(NodeKind::Route, &route_qn("POST", "/items"))
        );
        assert_eq!(producer.record.contract.fields[0].name, "id");
    }

    #[test]
    fn producer_from_return_annotation() {
        let parsed = parse(
            r#"
from pydantic import BaseModel
from fastapi import FastAPI

app = FastAPI()


class Item(BaseModel):
    id: int


@app.get("/items/{item_id}")
def get_item(item_id: int) -> Item:
    return Item(id=item_id)
"#,
        );
        let inferred = infer_payload_contracts(&parsed);
        let producer = inferred
            .iter()
            .find(|item| item.record.side == PayloadSide::Producer)
            .expect("producer contract");
        assert_eq!(producer.record.source_type_name.as_deref(), Some("Item"));
        assert!(
            !inferred
                .iter()
                .any(|item| item.record.side == PayloadSide::Consumer),
            "int path param is not a contract consumer"
        );
    }

    #[test]
    fn composed_fastapi_route_targets_payload_contracts() {
        let parsed = parse(
            r#"
from typing import Annotated
from fastapi import APIRouter, Body, FastAPI
from pydantic import BaseModel

app = FastAPI()
router = APIRouter(prefix="/items")


class Item(BaseModel):
    id: int


@router.post("/{item_id}", response_model=list[Item])
def update_item(item_id: int, item: Annotated[Item, Body(...)]):
    return [item]


app.include_router(router, prefix="/v1")
"#,
        );
        let inferred = infer_payload_contracts(&parsed);
        let composed_target = ref_node_id(NodeKind::Route, &route_qn("POST", "/v1/items/:item_id"));
        let bare_target = ref_node_id(NodeKind::Route, &route_qn("POST", "/:item_id"));

        let consumer = inferred
            .iter()
            .find(|item| item.record.side == PayloadSide::Consumer)
            .expect("Annotated request body should infer a consumer contract");
        assert_eq!(consumer.record.source_type_name.as_deref(), Some("Item"));
        assert_eq!(consumer.record.contract_target_node_id, composed_target);

        let producer = inferred
            .iter()
            .find(|item| item.record.side == PayloadSide::Producer)
            .expect("list[Item] response_model should infer a producer contract");
        assert_eq!(producer.record.source_type_name.as_deref(), Some("Item"));
        assert_eq!(producer.record.contract_target_node_id, composed_target);
        assert!(
            inferred
                .iter()
                .all(|item| item.record.contract_target_node_id != bare_target),
            "contracts must not attach to the unmounted decorator path: {inferred:?}"
        );
    }

    #[test]
    fn typeddict_total_false_and_required_wrappers_set_optionality() {
        let parsed = parse(
            r#"
from typing import Annotated, NotRequired, Required, TypedDict
from fastapi import APIRouter, Body

router = APIRouter()


class Filters(TypedDict, total=False):
    required_id: Required[int]
    query: str
    explicit_optional: NotRequired[str]


@router.post("/search")
def search(filters: Annotated[Filters, Body(...)]):
    return {}
"#,
        );
        let inferred = infer_payload_contracts(&parsed);
        let consumer = inferred
            .iter()
            .find(|item| item.record.side == PayloadSide::Consumer)
            .expect("Annotated TypedDict body should infer a consumer contract");
        assert_eq!(consumer.record.source_type_name.as_deref(), Some("Filters"));
        let fields = &consumer.record.contract.fields;
        assert_eq!(
            fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            vec!["required_id", "query", "explicit_optional"]
        );
        assert!(
            !fields[0].optional,
            "Required[T] overrides TypedDict(total=False)"
        );
        assert!(
            fields[1].optional,
            "total=False makes ordinary keys optional"
        );
        assert!(fields[2].optional, "NotRequired[T] keys are optional");
    }

    #[test]
    fn plain_class_without_route_emits_nothing() {
        let parsed = parse(
            r"
from pydantic import BaseModel


class Orphan(BaseModel):
    x: int
",
        );
        assert!(infer_payload_contracts(&parsed).is_empty());
    }
}
