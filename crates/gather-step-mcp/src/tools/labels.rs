use gather_step_core::{EdgeKind, NodeKind};

pub fn node_kind_label(kind: NodeKind) -> &'static str {
    kind.label()
}

pub fn edge_kind_label(kind: EdgeKind) -> &'static str {
    kind.label()
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
