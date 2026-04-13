use gather_step_core::{EdgeKind, NodeKind};

pub fn node_kind_label(kind: NodeKind) -> &'static str {
    match kind {
        NodeKind::File => "file",
        NodeKind::Function => "function",
        NodeKind::Class => "class",
        NodeKind::Type => "type",
        NodeKind::Module => "module",
        NodeKind::Import => "import",
        NodeKind::Decorator => "decorator",
        NodeKind::Entity => "entity",
        NodeKind::Route => "route",
        NodeKind::Topic => "topic",
        NodeKind::Queue => "queue",
        NodeKind::Subject => "subject",
        NodeKind::Stream => "stream",
        NodeKind::Event => "event",
        NodeKind::SharedSymbol => "shared_symbol",
        NodeKind::PayloadContract => "payload_contract",
        NodeKind::Repo => "repo",
        NodeKind::Convention => "convention",
        NodeKind::Service => "service",
        NodeKind::Commit => "commit",
        NodeKind::PR => "pr",
        NodeKind::Review => "review",
        NodeKind::Comment => "comment",
        NodeKind::Author => "author",
        NodeKind::Ticket => "ticket",
        _ => "unknown",
    }
}

pub fn edge_kind_label(kind: EdgeKind) -> &'static str {
    match kind {
        EdgeKind::Defines => "defines",
        EdgeKind::Calls => "calls",
        EdgeKind::Imports => "imports",
        EdgeKind::Exports => "exports",
        EdgeKind::Extends => "extends",
        EdgeKind::Implements => "implements",
        EdgeKind::References => "references",
        EdgeKind::DependsOn => "depends_on",
        EdgeKind::UsesDecorator => "uses_decorator",
        EdgeKind::Publishes => "publishes",
        EdgeKind::Consumes => "consumes",
        EdgeKind::Triggers => "triggers",
        EdgeKind::Serves => "serves",
        EdgeKind::PersistsTo => "persists_to",
        EdgeKind::UsesShared => "uses_shared",
        EdgeKind::UsesTypeFrom => "uses_type_from",
        EdgeKind::UsesEventFrom => "uses_event_from",
        EdgeKind::UsesGuardFrom => "uses_guard_from",
        EdgeKind::ConsumesApiFrom => "consumes_api_from",
        EdgeKind::ProducesEventFor => "produces_event_for",
        EdgeKind::ImplementsContractFrom => "implements_contract_from",
        EdgeKind::ChangedIn => "changed_in",
        EdgeKind::IntroducedBy => "introduced_by",
        EdgeKind::AuthoredBy => "authored_by",
        EdgeKind::ReviewedBy => "reviewed_by",
        EdgeKind::MergedAs => "merged_as",
        EdgeKind::CommentedOn => "commented_on",
        EdgeKind::Resolves => "resolves",
        EdgeKind::RelatesTo => "relates_to",
        EdgeKind::PartOf => "part_of",
        EdgeKind::BreaksIfChanged => "breaks_if_changed",
        EdgeKind::CoChangesWith => "co_changes_with",
        EdgeKind::OwnedBy => "owned_by",
        EdgeKind::CrossRepoDepends => "cross_repo_depends",
        EdgeKind::PropagatesEvent => "propagates_event",
        EdgeKind::DriftsFrom => "drifts_from",
        EdgeKind::ContractOn => "contract_on",
        _ => "unknown",
    }
}

pub fn evidence_kind_label(resolver: Option<&str>) -> &'static str {
    match resolver {
        Some("frontend_literal") => "literal",
        Some("frontend_constant") => "imported_constant",
        Some("frontend_hint") => "hint",
        Some("import_map" | "same_module" | "unique" | "suffix" | "fuzzy_name" | "fallback") => {
            "symbol_resolution"
        }
        Some("nestjs_route") => "framework_route",
        Some(_) => "derived",
        None => "unknown",
    }
}
