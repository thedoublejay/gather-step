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
    mcp_tool_qn, ref_node_id,
};

use crate::frameworks::nestjs::{extract_call_argument, extract_object_key_value};
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
        emit_agent_graph(parsed, call_site, &mut augmentation);
        emit_tool(parsed, call_site, &mut augmentation);
        emit_vector_search(parsed, call_site, &mut augmentation);
        emit_managed_prompt(parsed, call_site, &mut augmentation);
        emit_mcp_tool_call(parsed, call_site, &mut augmentation);
        emit_mcp_tool_expose(parsed, call_site, &mut augmentation);
        emit_mcp_server(parsed, call_site, &mut augmentation);
        emit_indexes_vector(parsed, call_site, &mut augmentation);
        emit_embeds(parsed, call_site, &mut augmentation);
    }
    augmentation
}

/// `client.callTool({ name: "mcp__<server>__<tool>", … })` → a converged
/// `McpTool` node (keyed by `(server, tool)`) + a `CallsMcpTool` edge from the
/// calling symbol. The tool name must be a literal of the fully-qualified
/// `mcp__server__tool` form; a dynamic name, or one without that shape, is
/// skipped rather than fabricated, since the server segment is what makes the
/// cross-repo convergence id resolvable against an exposing server.
fn emit_mcp_tool_call(
    parsed: &ParsedFile,
    call_site: &EnrichedCallSite,
    augmentation: &mut AiTypescriptAugmentation,
) {
    if call_site.callee_name != "callTool" {
        return;
    }
    let Some(name) = call_site
        .raw_arguments
        .as_deref()
        .and_then(|raw| extract_call_argument(raw, 0))
        .and_then(|options| extract_object_key_value(options, "name"))
        .and_then(|value| string_literal(value.trim()))
    else {
        return;
    };
    let Some((server, tool)) = parse_mcp_tool_name(&name) else {
        return;
    };
    let qualified_name = mcp_tool_qn(server, tool);
    let node = virtual_ai_node(
        parsed,
        call_site,
        NodeKind::McpTool,
        &qualified_name,
        &format!("{server}/{tool}"),
        None,
    );
    let node_id = node.id;
    augmentation.nodes.push(node);
    augmentation.edges.push(ai_edge(
        parsed,
        call_site.owner_id,
        node_id,
        EdgeKind::CallsMcpTool,
    ));
}

/// `(server, tool)` parsed from a fully-qualified MCP tool name of the form
/// `mcp__<server>__<tool>`. Returns `None` for any other shape so a non-MCP
/// `callTool` or a bare tool name (no resolvable server) is not converged.
fn parse_mcp_tool_name(name: &str) -> Option<(&str, &str)> {
    let body = name.strip_prefix("mcp__")?;
    let (server, tool) = body.split_once("__")?;
    (!server.is_empty() && !tool.is_empty()).then_some((server, tool))
}

/// MCP server tool registration → an `ExposesMcpTool` edge to the converged
/// `McpTool` node, the provider counterpart to `CallsMcpTool`. Covers the SDK
/// method idioms `server.tool("mcp__<server>__<tool>", …)` /
/// `server.registerTool("mcp__<server>__<tool>", …)` and the decorator factory
/// `@Tool({ name: "mcp__<server>__<tool>" })`. Only a literal fully-qualified
/// name converges (the server segment is required); a name built at runtime
/// (e.g. `buildToolName('…')`) is skipped rather than fabricated.
fn emit_mcp_tool_expose(
    parsed: &ParsedFile,
    call_site: &EnrichedCallSite,
    augmentation: &mut AiTypescriptAugmentation,
) {
    let Some(raw) = call_site.raw_arguments.as_deref() else {
        return;
    };
    let name = match call_site.callee_name.as_str() {
        // SDK form: positional string name as arg 0. (LangChain `tool(fn, …)`
        // passes a function here, not a string, so it falls through.)
        "tool" | "registerTool" => {
            extract_call_argument(raw, 0).and_then(|arg| string_literal(arg.trim()))
        }
        // Decorator factory `@Tool({ name })` — captured as a call expression.
        "Tool" => extract_call_argument(raw, 0)
            .and_then(|options| extract_object_key_value(options, "name"))
            .and_then(|value| string_literal(value.trim())),
        _ => return,
    };
    let Some((server, tool)) = name.as_deref().and_then(parse_mcp_tool_name) else {
        return;
    };
    let qualified_name = mcp_tool_qn(server, tool);
    let node = virtual_ai_node(
        parsed,
        call_site,
        NodeKind::McpTool,
        &qualified_name,
        &format!("{server}/{tool}"),
        None,
    );
    let node_id = node.id;
    augmentation.nodes.push(node);
    augmentation.edges.push(ai_edge(
        parsed,
        call_site.owner_id,
        node_id,
        EdgeKind::ExposesMcpTool,
    ));
}

fn mcp_server_qn(file_path: &str) -> String {
    format!("__mcp_server__{file_path}")
}

/// `new McpServer(…)` → a per-file `McpServer` node. The server name is usually
/// passed dynamically (`new McpServer(this.serverInfo, …)`), so the node is
/// scoped by file rather than keyed by a (usually unavailable) literal name —
/// mirrors the one-graph-per-file `AgentGraph` assumption.
fn emit_mcp_server(
    parsed: &ParsedFile,
    call_site: &EnrichedCallSite,
    augmentation: &mut AiTypescriptAugmentation,
) {
    if call_site.callee_name != "McpServer" {
        return;
    }
    let qualified_name = mcp_server_qn(parsed.file_node.file_path.as_str());
    augmentation.nodes.push(virtual_ai_node(
        parsed,
        call_site,
        NodeKind::McpServer,
        &qualified_name,
        "mcp_server",
        None,
    ));
}

/// `MongoDB` Atlas `collection.createSearchIndex({ name, definition })` for a
/// vector index → an `IndexesVector` edge to the SAME converged `VectorIndex`
/// node the `$vectorSearch` read side resolves to, so the write and read sides
/// of one index meet. Gated on the args mentioning a vector type so a plain
/// text search index is not mistaken for a vector index; requires a literal
/// index `name`.
fn emit_indexes_vector(
    parsed: &ParsedFile,
    call_site: &EnrichedCallSite,
    augmentation: &mut AiTypescriptAugmentation,
) {
    if call_site.callee_name != "createSearchIndex" {
        return;
    }
    let Some(raw) = call_site.raw_arguments.as_deref() else {
        return;
    };
    if !raw.contains("vector") {
        return;
    }
    let Some(name) = extract_call_argument(raw, 0)
        .and_then(|options| extract_object_key_value(options, "name"))
        .and_then(|value| string_literal(value.trim()))
    else {
        return;
    };
    let qualified_name = gather_step_core::vector_index_qn(&name, "vector_index");
    let node = virtual_ai_node(
        parsed,
        call_site,
        NodeKind::VectorIndex,
        &qualified_name,
        &name,
        None,
    );
    let node_id = node.id;
    augmentation.nodes.push(node);
    augmentation.edges.push(ai_edge(
        parsed,
        call_site.owner_id,
        node_id,
        EdgeKind::IndexesVector,
    ));
}

/// An HTTP POST to an embedding-service endpoint (`…/embed`, `…/embeddings`,
/// `…/vectorize`) → an `Embeds` edge to the provider's `Route` node, reusing
/// the existing `route_qn` convergence id rather than a fabricated AI node. The
/// URL must carry a literal path segment with an embedding token; a fully
/// dynamic URL is skipped. Token matching is intentionally narrow.
fn emit_embeds(
    parsed: &ParsedFile,
    call_site: &EnrichedCallSite,
    augmentation: &mut AiTypescriptAugmentation,
) {
    if call_site.callee_name != "post" {
        return;
    }
    let Some(path) = call_site
        .raw_arguments
        .as_deref()
        .and_then(|raw| extract_call_argument(raw, 0))
        .and_then(|url| embedding_endpoint_path(url.trim()))
    else {
        return;
    };
    let qualified_name = gather_step_core::route_qn("POST", &path);
    let node = virtual_ai_node(
        parsed,
        call_site,
        NodeKind::Route,
        &qualified_name,
        &path,
        None,
    );
    let node_id = node.id;
    augmentation.nodes.push(node);
    augmentation.edges.push(ai_edge(
        parsed,
        call_site.owner_id,
        node_id,
        EdgeKind::Embeds,
    ));
}

/// The static path of an embedding-endpoint URL argument, if it carries one.
/// Handles a plain string (`"/api/v1/vectorize"`) and a template whose static
/// tail follows an interpolation (`` `${base}/api/v1/vectorize` ``). Returns
/// `None` unless the tail is a path containing an embedding token.
fn embedding_endpoint_path(url_arg: &str) -> Option<String> {
    const TOKENS: [&str; 3] = ["/vectorize", "/embeddings", "/embed"];
    let bytes = url_arg.as_bytes();
    let inner = match (bytes.first(), bytes.last()) {
        (Some(&open), Some(&close))
            if open == close && matches!(open, b'"' | b'\'' | b'`') && bytes.len() >= 2 =>
        {
            &url_arg[1..url_arg.len() - 1]
        }
        _ => url_arg,
    };
    // For a template literal the static path tail follows the last `}`.
    let tail = inner.rfind('}').map_or(inner, |idx| &inner[idx + 1..]);
    if !tail.starts_with('/') || tail.contains(['$', '{']) {
        return None;
    }
    TOKENS
        .iter()
        .any(|token| tail.contains(token))
        .then(|| tail.to_owned())
}

/// `new AdminPrompt({ keyName, … })` / `new ContextualPrompt({ keyName, … })` →
/// a `Prompt` node keyed by `keyName` + a `UsesPrompt` edge from the enclosing
/// symbol. `AdminPrompt`/`ContextualPrompt` are managed-prompt types; the
/// `keyName` field is the cross-repo prompt identity (the source segment of
/// `prompt_qn` is aligned with the consumer side in the Phase 3 resolver).
fn emit_managed_prompt(
    parsed: &ParsedFile,
    call_site: &EnrichedCallSite,
    augmentation: &mut AiTypescriptAugmentation,
) {
    if !matches!(
        call_site.callee_name.as_str(),
        "AdminPrompt" | "ContextualPrompt"
    ) {
        return;
    }
    let Some(key_name) = call_site
        .raw_arguments
        .as_deref()
        .and_then(|raw| extract_call_argument(raw, 0))
        .and_then(|options| extract_object_key_value(options, "keyName"))
        .and_then(|value| string_literal(value.trim()))
    else {
        return;
    };
    let qualified_name = gather_step_core::prompt_qn("managed", &key_name);
    let node = virtual_ai_node(
        parsed,
        call_site,
        NodeKind::Prompt,
        &qualified_name,
        &key_name,
        None,
    );
    let node_id = node.id;
    augmentation.nodes.push(node);
    augmentation.edges.push(ai_edge(
        parsed,
        call_site.owner_id,
        node_id,
        EdgeKind::UsesPrompt,
    ));
}

/// `MongoDB` Atlas `$vectorSearch` aggregation stage → a converged `VectorIndex`
/// node (keyed by the Atlas search-index name) + a `RetrievesFrom` edge from the
/// querying symbol. The index name is the first literal `index` value inside the
/// `$vectorSearch` stage.
fn emit_vector_search(
    parsed: &ParsedFile,
    call_site: &EnrichedCallSite,
    augmentation: &mut AiTypescriptAugmentation,
) {
    if call_site.callee_name != "aggregate" {
        return;
    }
    let Some(raw) = call_site.raw_arguments.as_deref() else {
        return;
    };
    let Some(index) = vector_search_index(raw) else {
        return;
    };
    let qualified_name = gather_step_core::vector_index_qn(&index, "vector_index");
    let node = virtual_ai_node(
        parsed,
        call_site,
        NodeKind::VectorIndex,
        &qualified_name,
        &index,
        None,
    );
    let node_id = node.id;
    augmentation.nodes.push(node);
    augmentation.edges.push(ai_edge(
        parsed,
        call_site.owner_id,
        node_id,
        EdgeKind::RetrievesFrom,
    ));
}

/// First literal `index` value inside a `$vectorSearch` aggregation stage.
/// Anchors on an `index` key (boundary before, `:` after) so a value/identifier
/// that merely contains `index` (e.g. `"reindexed"`) is not mistaken for the key.
fn vector_search_index(raw: &str) -> Option<String> {
    let stage = raw.get(raw.find("$vectorSearch")?..)?;
    let mut from = 0;
    while let Some(rel) = stage[from..].find("index") {
        let pos = from + rel;
        let boundary_before = pos == 0
            || matches!(
                stage.as_bytes()[pos - 1],
                b'{' | b',' | b' ' | b'\n' | b'\t' | b'"' | b'\''
            );
        let after = stage[pos + "index".len()..].trim_start();
        if boundary_before && let Some(value) = after.strip_prefix(':') {
            return string_literal(leading_quoted_token(value.trim_start()));
        }
        from = pos + "index".len();
    }
    None
}

/// The leading quoted-string token of `s` (`"…"`/`'…'`), or `s` unchanged.
fn leading_quoted_token(s: &str) -> &str {
    let bytes = s.as_bytes();
    let Some(&quote) = bytes.first() else {
        return s;
    };
    if (quote == b'"' || quote == b'\'')
        && let Some(end) = s[1..].find(quote as char)
    {
        return &s[..end + 2];
    }
    s
}

fn tool_qn(file_path: &str, name: &str) -> String {
    format!("__tool__{file_path}__{name}")
}

/// `new DynamicStructuredTool({ name, … })` / `tool(fn, { name, … })` → a faceted
/// `Tool` node (`ai_role="tool"`) keyed by tool name + a `BindsTool` edge from
/// the enclosing symbol. Requires a literal `name`.
fn emit_tool(
    parsed: &ParsedFile,
    call_site: &EnrichedCallSite,
    augmentation: &mut AiTypescriptAugmentation,
) {
    let options_arg = match call_site.callee_name.as_str() {
        "DynamicStructuredTool" => 0,
        "tool" => 1,
        _ => return,
    };
    let Some(raw) = call_site.raw_arguments.as_deref() else {
        return;
    };
    let Some(name) = extract_call_argument(raw, options_arg)
        .and_then(|options| extract_object_key_value(options, "name"))
        .and_then(|value| string_literal(value.trim()))
    else {
        return;
    };
    let qualified_name = tool_qn(parsed.file_node.file_path.as_str(), &name);
    let node = virtual_ai_node(
        parsed,
        call_site,
        NodeKind::Function,
        &qualified_name,
        &name,
        Some("tool"),
    );
    let node_id = node.id;
    augmentation.nodes.push(node);
    augmentation.edges.push(ai_edge(
        parsed,
        call_site.owner_id,
        node_id,
        EdgeKind::BindsTool,
    ));
}

fn agent_graph_qn(file_path: &str) -> String {
    format!("__agent_graph__{file_path}")
}

fn agent_node_qn(file_path: &str, name: &str) -> String {
    format!("__agent_node__{file_path}__{name}")
}

/// `LangGraph`'s reserved entry/exit markers. They appear as string literals in
/// `addEdge("__start__", …)` / `addEdge(…, "__end__")` but are never declared
/// via `addNode`, so a `GraphTransitionsTo` edge to/from one would reference a
/// node that does not exist (a dangling edge). They denote graph entry/exit, not
/// a node-to-node transition, so edges touching them are skipped. The imported
/// `START`/`END` constants are already skipped upstream (they are not string
/// literals); this guards the literal form.
fn is_graph_sentinel(name: &str) -> bool {
    matches!(name, "__start__" | "__end__")
}

/// `LangGraph` `StateGraph` wiring → `AgentGraph` + faceted `AgentNode`s +
/// `GraphTransitionsTo`. Node identity is scoped by file (one graph per file),
/// since associating `.addNode`/`.addEdge` with a specific graph instance needs
/// receiver tracking (R2); only string-literal node names/edges are captured.
fn emit_agent_graph(
    parsed: &ParsedFile,
    call_site: &EnrichedCallSite,
    augmentation: &mut AiTypescriptAugmentation,
) {
    let file_path = parsed.file_node.file_path.as_str();
    match call_site.callee_name.as_str() {
        "StateGraph" => {
            let qualified_name = agent_graph_qn(file_path);
            augmentation.nodes.push(virtual_ai_node(
                parsed,
                call_site,
                NodeKind::AgentGraph,
                &qualified_name,
                "graph",
                None,
            ));
        }
        "addNode" => {
            // Gate on arg 0 specifically being a string literal (mirror addEdge);
            // `literal_argument` is the first literal in *any* arg, which would
            // wrongly read `addNode(dynamicName, "label")` as a node named "label".
            let Some(name) = call_site
                .raw_arguments
                .as_deref()
                .and_then(|raw| extract_call_argument(raw, 0))
                .and_then(|arg| string_literal(arg.trim()))
            else {
                return;
            };
            if name.is_empty() {
                return;
            }
            let node_qn = agent_node_qn(file_path, &name);
            let node = virtual_ai_node(
                parsed,
                call_site,
                NodeKind::Function,
                &node_qn,
                &name,
                Some("agent_node"),
            );
            let node_id = node.id;
            augmentation.nodes.push(node);
            augmentation.edges.push(ai_edge(
                parsed,
                ref_node_id(NodeKind::AgentGraph, &agent_graph_qn(file_path)),
                node_id,
                EdgeKind::DefinesAgentNode,
            ));
        }
        "addEdge" => {
            let Some(raw) = call_site.raw_arguments.as_deref() else {
                return;
            };
            let from = extract_call_argument(raw, 0).and_then(|arg| string_literal(arg.trim()));
            let to = extract_call_argument(raw, 1).and_then(|arg| string_literal(arg.trim()));
            if let (Some(from), Some(to)) = (from, to)
                && !is_graph_sentinel(&from)
                && !is_graph_sentinel(&to)
            {
                augmentation.edges.push(ai_edge(
                    parsed,
                    ref_node_id(NodeKind::Function, &agent_node_qn(file_path, &from)),
                    ref_node_id(NodeKind::Function, &agent_node_qn(file_path, &to)),
                    EdgeKind::GraphTransitionsTo,
                ));
            }
        }
        _ => {}
    }
}

fn virtual_ai_node(
    parsed: &ParsedFile,
    call_site: &EnrichedCallSite,
    kind: NodeKind,
    qualified_name: &str,
    name: &str,
    ai_role: Option<&str>,
) -> NodeData {
    NodeData {
        id: ref_node_id(kind, qualified_name),
        kind,
        repo: parsed.file_node.repo.clone(),
        file_path: parsed.file_node.file_path.clone(),
        name: name.to_owned(),
        qualified_name: Some(qualified_name.to_owned()),
        external_id: Some(qualified_name.to_owned()),
        signature: None,
        visibility: None,
        span: call_site.span.clone(),
        is_virtual: true,
        ai_role: ai_role.map(str::to_owned),
    }
}

fn ai_edge(parsed: &ParsedFile, source: NodeId, target: NodeId, kind: EdgeKind) -> EdgeData {
    EdgeData {
        source,
        target,
        kind,
        metadata: EdgeMetadata::default(),
        owner_file: parsed.file_node.id,
        is_cross_file: false,
    }
}

/// Schema label of a structured-output call: the referenced schema identifier,
/// or `"inline"` for an inline schema definition (e.g. `z.object({…})`). Covers
/// two idioms — `LangChain` `…withStructuredOutput(<schema>)` and a
/// `{ responseSchema: <schema> }` options object (the Gemini `@google/genai`
/// convention, used via project wrappers like `generateStructured(...)`).
/// Inline field extraction is a follow-up; a *named* schema's field shape needs
/// RHS-binding (R2), so a referenced schema is captured by name only here.
fn structured_output_schema(call_site: &EnrichedCallSite) -> Option<String> {
    let raw = call_site.raw_arguments.as_deref()?;
    // LangChain: `withStructuredOutput(Schema, { ... })` — arg 0 only, so a
    // trailing options object's braces don't make it read as inline.
    if call_site.callee_name == "withStructuredOutput" {
        return schema_label(extract_call_argument(raw, 0)?);
    }
    // Gemini-style: any wrapper call passing `{ responseSchema: <schema> }`.
    schema_label(&response_schema_argument(raw)?)
}

/// Normalize a schema argument to its label: `"inline"` for a definition
/// expression, otherwise the referenced identifier. `None` if empty.
fn schema_label(argument: &str) -> Option<String> {
    let argument = argument.trim();
    if argument.is_empty() {
        return None;
    }
    if argument.contains(['(', '{']) {
        return Some("inline".to_owned());
    }
    Some(argument.to_owned())
}

/// The `responseSchema` value from an options object in any leading argument,
/// e.g. `generateStructured(messages, { responseSchema: OutputSchema })`.
fn response_schema_argument(raw: &str) -> Option<String> {
    (0..4)
        .map_while(|index| extract_call_argument(raw, index))
        .find_map(|argument| {
            extract_object_key_value(argument, "responseSchema")
                .map(|value| value.trim().to_owned())
        })
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
    // No in-file schema symbol (imported / inline) → synthesize a stable,
    // collision-free target from the schema label + call-site line so two
    // contracts in one owner do not share an id.
    let (target, target_kind) = if let Some(resolved) = resolve_named_node(parsed, schema_label) {
        resolved
    } else {
        let line = call_site.span.as_ref().map_or(0, |span| span.line_start);
        let synthetic = format!(
            "__ai_schema__{}__{schema_label}__{line}",
            parsed.file_node.file_path
        );
        (ref_node_id(NodeKind::Type, &synthetic), NodeKind::Type)
    };
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
    fn response_schema_options_emit_ai_contract() {
        let dir = TestDir::new("response-schema");
        let parsed = parse(
            &dir,
            "agent.ts",
            r#"
import { DomainExpertLLMOutputSchema } from "./schemas";

export async function run(llm: any, messages: any) {
    return llm.generateStructured(messages, {
        responseSchema: DomainExpertLLMOutputSchema,
        responseMimeType: "application/json",
    });
}
"#,
        );

        // The Gemini-style `{ responseSchema: <schema> }` options object is a
        // common structured-output idiom, used via project wrappers rather than
        // LangChain's `withStructuredOutput`.
        assert_eq!(
            ai_contract_names(&parsed),
            vec!["DomainExpertLLMOutputSchema".to_owned()]
        );
        assert_eq!(edge_count(&parsed, EdgeKind::ProducesAiContract), 1);
    }

    fn nodes_with_role<'a>(
        parsed: &'a crate::tree_sitter::ParsedFile,
        role: &str,
    ) -> Vec<&'a gather_step_core::NodeData> {
        parsed
            .nodes
            .iter()
            .filter(|node| node.ai_role.as_deref() == Some(role))
            .collect()
    }

    #[test]
    fn state_graph_emits_agent_graph_nodes_and_transitions() {
        let dir = TestDir::new("graph");
        let parsed = parse(
            &dir,
            "pipeline.ts",
            r#"
import { StateGraph } from "@langchain/langgraph";

export function buildGraph(state: any) {
    const graph = new StateGraph(state)
        .addNode("intent", classifyIntent)
        .addNode("retrieve", retrieveDocs)
        .addNode("respond", generateResponse);
    graph.addEdge("intent", "retrieve");
    graph.addEdge("retrieve", "respond");
    return graph.compile();
}
"#,
        );

        let graphs = parsed
            .nodes
            .iter()
            .filter(|node| node.kind == NodeKind::AgentGraph)
            .count();
        assert_eq!(graphs, 1, "one AgentGraph node");

        let mut agent_nodes = nodes_with_role(&parsed, "agent_node")
            .iter()
            .map(|node| node.name.clone())
            .collect::<Vec<_>>();
        agent_nodes.sort();
        agent_nodes.dedup();
        assert_eq!(
            agent_nodes,
            vec![
                "intent".to_owned(),
                "respond".to_owned(),
                "retrieve".to_owned()
            ]
        );

        assert_eq!(
            edge_count(&parsed, EdgeKind::GraphTransitionsTo),
            2,
            "intent->retrieve and retrieve->respond"
        );
    }

    #[test]
    fn managed_prompt_construction_emits_prompt_node_and_uses_edge() {
        let dir = TestDir::new("prompt");
        let parsed = parse(
            &dir,
            "prompts.ts",
            r#"
import { AdminPrompt } from "@org/prompts";

export function classifyPrompt() {
    return new AdminPrompt({ keyName: "doc.classify", version: 3 });
}
"#,
        );

        let prompts = parsed
            .nodes
            .iter()
            .filter(|node| node.kind == NodeKind::Prompt)
            .map(|node| node.name.clone())
            .collect::<Vec<_>>();
        assert_eq!(prompts, vec!["doc.classify".to_owned()]);
        assert_eq!(edge_count(&parsed, EdgeKind::UsesPrompt), 1);
    }

    #[test]
    fn vector_search_emits_vector_index_and_retrieves_edge() {
        let dir = TestDir::new("vector");
        let parsed = parse(
            &dir,
            "rag.ts",
            r#"
export async function retrieve(collection: any, queryVector: number[]) {
    return collection.aggregate([
        {
            $vectorSearch: {
                index: "embedding_index",
                path: "embedding",
                queryVector: queryVector,
                numCandidates: 100,
                limit: 5,
            },
        },
    ]);
}
"#,
        );

        let indexes = parsed
            .nodes
            .iter()
            .filter(|node| node.kind == NodeKind::VectorIndex)
            .map(|node| node.name.clone())
            .collect::<Vec<_>>();
        assert_eq!(indexes, vec!["embedding_index".to_owned()]);
        assert_eq!(edge_count(&parsed, EdgeKind::RetrievesFrom), 1);
    }

    #[test]
    fn dynamic_structured_tool_emits_tool_facet_and_binds_edge() {
        let dir = TestDir::new("tool");
        let parsed = parse(
            &dir,
            "tools.ts",
            r#"
import { DynamicStructuredTool } from "@langchain/core/tools";

export function makeSearchTool() {
    return new DynamicStructuredTool({
        name: "search_docs",
        description: "search the corpus",
        func: async () => "",
    });
}
"#,
        );

        let tools = nodes_with_role(&parsed, "tool")
            .iter()
            .map(|node| node.name.clone())
            .collect::<Vec<_>>();
        assert_eq!(tools, vec!["search_docs".to_owned()]);
        assert_eq!(edge_count(&parsed, EdgeKind::BindsTool), 1);
    }

    #[test]
    fn with_structured_output_reads_schema_arg_not_options_object() {
        let dir = TestDir::new("structured-multiarg");
        let parsed = parse(
            &dir,
            "agent.ts",
            r#"
import { OrderSchema } from "./schema";

export function build(model: any) {
    return model.withStructuredOutput(OrderSchema, { name: "order" });
}
"#,
        );

        // arg 0 is a referenced identifier; the options object's braces must not
        // make this read as "inline".
        assert_eq!(ai_contract_names(&parsed), vec!["OrderSchema".to_owned()]);
    }

    #[test]
    fn add_node_skips_non_literal_name() {
        let dir = TestDir::new("graph-addnode-dynamic");
        let parsed = parse(
            &dir,
            "pipeline.ts",
            r#"
import { StateGraph } from "@langchain/langgraph";

export function build(state: any) {
    const graph = new StateGraph(state);
    graph.addNode(dynamicName, "display label");
    return graph;
}
"#,
        );

        assert!(
            nodes_with_role(&parsed, "agent_node").is_empty(),
            "addNode with a non-literal first arg must not fabricate a node"
        );
    }

    #[test]
    fn add_edge_skips_langgraph_start_end_sentinels() {
        let dir = TestDir::new("graph-sentinels");
        let parsed = parse(
            &dir,
            "agent.ts",
            r#"
import { StateGraph } from "@langchain/langgraph";

export function build(state: any) {
    return new StateGraph(state)
        .addNode("callLlm1", step1)
        .addNode("aggregator", agg)
        .addEdge("__start__", "callLlm1")
        .addEdge("callLlm1", "aggregator")
        .addEdge("aggregator", "__end__")
        .compile();
}
"#,
        );

        // Only callLlm1 -> aggregator is a real node-to-node transition; the
        // "__start__"/"__end__" string-literal sentinels are never addNode'd, so
        // emitting edges to them would dangle.
        assert_eq!(
            edge_count(&parsed, EdgeKind::GraphTransitionsTo),
            1,
            "only the callLlm1->aggregator edge is between declared nodes"
        );
        let sentinel_targets = [
            gather_step_core::ref_node_id(
                NodeKind::Function,
                &super::agent_node_qn("agent.ts", "__start__"),
            ),
            gather_step_core::ref_node_id(
                NodeKind::Function,
                &super::agent_node_qn("agent.ts", "__end__"),
            ),
        ];
        assert!(
            parsed
                .edges
                .iter()
                .filter(|edge| edge.kind == EdgeKind::GraphTransitionsTo)
                .all(|edge| !sentinel_targets.contains(&edge.source)
                    && !sentinel_targets.contains(&edge.target)),
            "no GraphTransitionsTo edge may reference a __start__/__end__ sentinel node"
        );
    }

    #[test]
    fn state_graph_skips_dynamic_edge_endpoints() {
        let dir = TestDir::new("graph-dynamic");
        let parsed = parse(
            &dir,
            "pipeline.ts",
            r#"
import { StateGraph, START } from "@langchain/langgraph";

export function buildGraph(state: any) {
    const graph = new StateGraph(state).addNode("agent", agentStep);
    graph.addEdge(START, "agent");
    return graph;
}
"#,
        );

        assert_eq!(
            edge_count(&parsed, EdgeKind::GraphTransitionsTo),
            0,
            "START is not a string literal"
        );
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

    fn mcp_tool_qns(parsed: &crate::tree_sitter::ParsedFile) -> Vec<String> {
        let mut ids = parsed
            .nodes
            .iter()
            .filter(|node| node.kind == NodeKind::McpTool)
            .map(|node| node.external_id.clone().unwrap_or_default())
            .collect::<Vec<_>>();
        ids.sort();
        ids.dedup();
        ids
    }

    #[test]
    fn call_tool_emits_converged_mcp_tool_and_calls_edge() {
        let dir = TestDir::new("mcp-call");
        let parsed = parse(
            &dir,
            "client.ts",
            r#"
import { Client } from "@modelcontextprotocol/sdk/client/index.js";

export async function searchChunks(client: Client, query: string) {
    return client.callTool({
        name: "mcp__document__search_document_chunks",
        arguments: { query },
    });
}
"#,
        );

        assert_eq!(
            mcp_tool_qns(&parsed),
            vec!["__mcp__document__search_document_chunks".to_owned()]
        );
        assert_eq!(edge_count(&parsed, EdgeKind::CallsMcpTool), 1);
    }

    #[test]
    fn call_tool_skips_dynamic_tool_name() {
        let dir = TestDir::new("mcp-call-dynamic");
        let parsed = parse(
            &dir,
            "client.ts",
            r"
export async function run(client: any, toolName: string, args: any) {
    return client.callTool({ name: toolName, arguments: args });
}
",
        );

        assert!(
            mcp_tool_qns(&parsed).is_empty(),
            "a dynamic tool name has no resolvable server and must not converge"
        );
        assert_eq!(edge_count(&parsed, EdgeKind::CallsMcpTool), 0);
    }

    #[test]
    fn call_tool_skips_name_without_mcp_shape() {
        let dir = TestDir::new("mcp-call-bare");
        let parsed = parse(
            &dir,
            "client.ts",
            r#"
export async function run(client: any) {
    return client.callTool({ name: "search", arguments: {} });
}
"#,
        );

        // A bare tool name (no `mcp__server__tool` shape) carries no server
        // segment, so there is no cross-repo identity to converge on.
        assert!(mcp_tool_qns(&parsed).is_empty());
        assert_eq!(edge_count(&parsed, EdgeKind::CallsMcpTool), 0);
    }

    fn node_names_of_kind(parsed: &crate::tree_sitter::ParsedFile, kind: NodeKind) -> Vec<String> {
        let mut names = parsed
            .nodes
            .iter()
            .filter(|node| node.kind == kind)
            .map(|node| node.name.clone())
            .collect::<Vec<_>>();
        names.sort();
        names.dedup();
        names
    }

    #[test]
    fn register_tool_exposes_and_converges_with_consumer_call() {
        let dir = TestDir::new("mcp-expose-converge");
        let parsed = parse(
            &dir,
            "server.ts",
            r#"
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";

export function register(server: McpServer, handler: any) {
    server.registerTool("mcp__document__search_document_chunks", { title: "t" }, handler);
}

export async function consume(client: any) {
    return client.callTool({ name: "mcp__document__search_document_chunks", arguments: {} });
}
"#,
        );

        // Both sides resolve to ONE converged McpTool node (provider + consumer).
        assert_eq!(
            mcp_tool_qns(&parsed),
            vec!["__mcp__document__search_document_chunks".to_owned()]
        );
        assert_eq!(edge_count(&parsed, EdgeKind::ExposesMcpTool), 1);
        assert_eq!(edge_count(&parsed, EdgeKind::CallsMcpTool), 1);
    }

    #[test]
    fn register_tool_skips_runtime_built_name() {
        let dir = TestDir::new("mcp-expose-dynamic");
        let parsed = parse(
            &dir,
            "server.ts",
            r#"
export function register(server: any, handler: any) {
    server.registerTool(buildToolName("search_document_chunks"), { title: "t" }, handler);
}
"#,
        );

        // buildToolName(...) is not a literal mcp__server__tool, so it cannot
        // converge and must not fabricate a node.
        assert!(mcp_tool_qns(&parsed).is_empty());
        assert_eq!(edge_count(&parsed, EdgeKind::ExposesMcpTool), 0);
    }

    #[test]
    fn mcp_server_construction_emits_server_node() {
        let dir = TestDir::new("mcp-server");
        let parsed = parse(
            &dir,
            "transport.ts",
            r#"
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";

export function build(serverInfo: any, options: any) {
    return new McpServer(serverInfo, options);
}
"#,
        );

        assert_eq!(
            parsed
                .nodes
                .iter()
                .filter(|node| node.kind == NodeKind::McpServer)
                .count(),
            1,
            "one McpServer node per file"
        );
    }

    #[test]
    fn create_search_index_emits_indexes_vector() {
        let dir = TestDir::new("indexes-vector");
        let parsed = parse(
            &dir,
            "index.ts",
            r#"
export async function ensureIndex(collection: any) {
    return collection.createSearchIndex({
        name: "embedding_index",
        type: "vectorSearch",
        definition: { fields: [{ type: "vector", path: "embedding", numDimensions: 1536 }] },
    });
}
"#,
        );

        // Converges on the SAME vector_index_qn the $vectorSearch read side uses.
        assert_eq!(
            node_names_of_kind(&parsed, NodeKind::VectorIndex),
            vec!["embedding_index".to_owned()]
        );
        assert_eq!(edge_count(&parsed, EdgeKind::IndexesVector), 1);
    }

    #[test]
    fn create_search_index_skips_non_vector_index() {
        let dir = TestDir::new("indexes-text");
        let parsed = parse(
            &dir,
            "index.ts",
            r#"
export async function ensureIndex(collection: any) {
    return collection.createSearchIndex({ name: "title_text", definition: { mappings: {} } });
}
"#,
        );

        assert!(node_names_of_kind(&parsed, NodeKind::VectorIndex).is_empty());
        assert_eq!(edge_count(&parsed, EdgeKind::IndexesVector), 0);
    }

    #[test]
    fn post_to_embedding_endpoint_emits_embeds() {
        let dir = TestDir::new("embeds");
        let parsed = parse(
            &dir,
            "vectorizer.ts",
            r"
export async function vectorize(http: any, content: string) {
    return http.axiosRef.post(`${process.env.VECTORIZER_URL}/api/v1/vectorize`, { content });
}
",
        );

        assert_eq!(
            node_names_of_kind(&parsed, NodeKind::Route),
            vec!["/api/v1/vectorize".to_owned()]
        );
        assert_eq!(edge_count(&parsed, EdgeKind::Embeds), 1);
    }

    #[test]
    fn post_to_non_embedding_endpoint_is_skipped() {
        let dir = TestDir::new("embeds-skip");
        let parsed = parse(
            &dir,
            "orders.ts",
            r"
export async function create(http: any, order: any) {
    return http.axiosRef.post(`${base}/api/v1/orders`, order);
}
",
        );

        assert_eq!(edge_count(&parsed, EdgeKind::Embeds), 0);
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
