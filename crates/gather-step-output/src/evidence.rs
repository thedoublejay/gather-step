/// Markdown rendering for evidence chains.
///
/// Converts an [`EvidenceChain`] into a human-readable chain diagram suitable
/// for embedding in LLM context or developer-facing planning output.
///
/// All node names and paths flow through the sanitize helpers from
/// [`crate::sanitize`] before being emitted, preventing markdown injection.
use gather_step_analysis::evidence::EvidenceChain;
use gather_step_core::EdgeKind;
use gather_step_storage::GraphStore;

use crate::sanitize::wrap_inline_code;

/// Render an [`EvidenceChain`] as a markdown chain diagram.
///
/// Each step is rendered on its own line with an arrow:
///
/// ```text
/// `useAuthentication.ts`
///  → ConsumesApiFrom `__route__POST__/auth/refresh`
///  → Serves `identity/refresh-access-token/controller.ts`
/// ```
///
/// Node names are resolved from `store`; when a node is not found, the raw
/// node ID hex is used as a fallback.  All names go through
/// [`wrap_inline_code`] to prevent backtick injection.
///
/// # Errors
///
/// Returns [`gather_step_storage::GraphStoreError`] when a node lookup fails.
pub fn render_evidence_chain<S: GraphStore>(
    chain: &EvidenceChain,
    store: &S,
) -> Result<String, gather_step_storage::GraphStoreError> {
    if chain.steps.is_empty() {
        return Ok(String::new());
    }

    let mut lines = Vec::with_capacity(chain.steps.len() + 1);

    // Emit the anchor node (source of the first step).
    if let Some(first) = chain.steps.first() {
        let name = node_display_name(store, first.from)?;
        lines.push(wrap_inline_code(&name));
    }

    for step in &chain.steps {
        let edge_label = edge_kind_label(step.edge_kind);
        let target_name = node_display_name(store, step.to)?;
        lines.push(format!(
            " → {edge_label} {}",
            wrap_inline_code(&target_name)
        ));
    }

    Ok(lines.join("\n"))
}

/// Resolve a human-readable display name for a node.
///
/// Preference order: `file_path`, then `name`, then hex fallback.
fn node_display_name<S: GraphStore>(
    store: &S,
    id: gather_step_core::NodeId,
) -> Result<String, gather_step_storage::GraphStoreError> {
    let Some(node) = store.get_node(id)? else {
        return Ok(format!("<{}>", hex_id(id)));
    };
    let display = if node.file_path.is_empty() {
        node.name.clone()
    } else {
        node.file_path.clone()
    };
    Ok(display)
}

/// Convert a [`NodeId`] to a short hex string for fallback display.
fn hex_id(id: gather_step_core::NodeId) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(8);
    for b in id.as_bytes().iter().take(4) {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Human-readable label for an edge kind (kept intentionally concise).
fn edge_kind_label(kind: EdgeKind) -> &'static str {
    match kind {
        EdgeKind::Defines => "Defines",
        EdgeKind::Calls => "Calls",
        EdgeKind::Imports => "Imports",
        EdgeKind::Exports => "Exports",
        EdgeKind::Extends => "Extends",
        EdgeKind::Implements => "Implements",
        EdgeKind::References => "References",
        EdgeKind::DependsOn => "DependsOn",
        EdgeKind::UsesDecorator => "UsesDecorator",
        EdgeKind::Publishes => "Publishes",
        EdgeKind::Consumes => "Consumes",
        EdgeKind::Triggers => "Triggers",
        EdgeKind::Serves => "Serves",
        EdgeKind::PersistsTo => "PersistsTo",
        EdgeKind::UsesShared => "UsesShared",
        EdgeKind::UsesTypeFrom => "UsesTypeFrom",
        EdgeKind::UsesEventFrom => "UsesEventFrom",
        EdgeKind::UsesGuardFrom => "UsesGuardFrom",
        EdgeKind::ConsumesApiFrom => "ConsumesApiFrom",
        EdgeKind::ProducesEventFor => "ProducesEventFor",
        EdgeKind::ImplementsContractFrom => "ImplementsContractFrom",
        EdgeKind::ChangedIn => "ChangedIn",
        EdgeKind::IntroducedBy => "IntroducedBy",
        EdgeKind::AuthoredBy => "AuthoredBy",
        EdgeKind::ReviewedBy => "ReviewedBy",
        EdgeKind::MergedAs => "MergedAs",
        EdgeKind::CommentedOn => "CommentedOn",
        EdgeKind::Resolves => "Resolves",
        EdgeKind::RelatesTo => "RelatesTo",
        EdgeKind::PartOf => "PartOf",
        EdgeKind::BreaksIfChanged => "BreaksIfChanged",
        EdgeKind::CoChangesWith => "CoChangesWith",
        EdgeKind::OwnedBy => "OwnedBy",
        EdgeKind::CrossRepoDepends => "CrossRepoDepends",
        EdgeKind::PropagatesEvent => "PropagatesEvent",
        EdgeKind::DriftsFrom => "DriftsFrom",
        EdgeKind::ContractOn => "ContractOn",
        // EdgeKind is #[non_exhaustive]; new variants fall back to debug name.
        _ => "UnknownEdge",
    }
}
