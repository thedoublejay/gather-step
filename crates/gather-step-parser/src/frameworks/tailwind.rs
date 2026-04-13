use std::sync::OnceLock;

use aho_corasick::AhoCorasick;
use gather_step_core::{EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, ref_node_id};

use crate::tree_sitter::ParsedFile;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TailwindAugmentation {
    pub nodes: Vec<NodeData>,
    pub edges: Vec<EdgeData>,
}

pub fn augment(parsed: &ParsedFile) -> TailwindAugmentation {
    let source = &*parsed.source;

    let mut augmentation = TailwindAugmentation::default();
    let file_path = parsed.file_node.file_path.clone();
    add_tailwind_config_nodes(parsed, &file_path, source, &mut augmentation);
    add_tailwind_usage_nodes(parsed, &file_path, source, &mut augmentation);
    augmentation
}

/// Needles for the Tailwind config theme-token scan.
///
/// Index order must stay in sync with `CONFIG_TOKENS`.
static CONFIG_AC: OnceLock<AhoCorasick> = OnceLock::new();

const CONFIG_TOKENS: [(&str, &str); 4] = [
    ("colors", "colors"),
    ("fontFamily", "fontFamily"),
    ("spacing", "spacing"),
    ("screens", "screens"),
];

fn config_ac() -> &'static AhoCorasick {
    CONFIG_AC.get_or_init(|| {
        AhoCorasick::new(CONFIG_TOKENS.map(|(_, needle)| needle))
            .expect("CONFIG_AC patterns are valid")
    })
}

/// Needles for detecting Tailwind usage in a source file.
///
/// Used by `looks_like_tailwind_usage` for a fast boolean gate.
static USAGE_GATE_AC: OnceLock<AhoCorasick> = OnceLock::new();

fn usage_gate_ac() -> &'static AhoCorasick {
    USAGE_GATE_AC.get_or_init(|| {
        AhoCorasick::new([
            "className=",
            "class=",
            "tw`",
            "@apply",
            "bg-",
            "text-",
            "grid ",
            "flex ",
        ])
        .expect("USAGE_GATE_AC patterns are valid")
    })
}

/// Signal needles for the per-file Tailwind usage scan.
///
/// Index order must stay in sync with `USAGE_SIGNALS`.
static USAGE_SIGNAL_AC: OnceLock<AhoCorasick> = OnceLock::new();

const USAGE_SIGNALS: [(&str, &str); 9] = [
    ("responsive", "sm:"),
    ("responsive", "md:"),
    ("responsive", "lg:"),
    ("state", "hover:"),
    ("state", "focus:"),
    ("state", "active:"),
    ("dark", "dark:"),
    ("arbitrary", "["),
    ("apply", "@apply"),
];

fn usage_signal_ac() -> &'static AhoCorasick {
    USAGE_SIGNAL_AC.get_or_init(|| {
        AhoCorasick::new(USAGE_SIGNALS.map(|(_, needle)| needle))
            .expect("USAGE_SIGNAL_AC patterns are valid")
    })
}

fn add_tailwind_config_nodes(
    parsed: &ParsedFile,
    file_path: &str,
    source: &str,
    augmentation: &mut TailwindAugmentation,
) {
    if !is_tailwind_config(file_path) {
        return;
    }

    let config_node = virtual_node(
        parsed,
        NodeKind::Convention,
        "__tailwind__config",
        "tailwind_config",
        Some("config".to_owned()),
    );
    augmentation.nodes.push(config_node.clone());
    augmentation
        .edges
        .push(file_edge(parsed, config_node.id, EdgeKind::Defines));

    // Walk all matches; track which pattern indices already emitted a node to
    // avoid duplicate nodes when the same needle matches multiple times.
    let mut emitted = [false; 4];
    for mat in config_ac().find_iter(source.as_bytes()) {
        let idx = mat.pattern().as_usize();
        if emitted[idx] {
            continue;
        }
        emitted[idx] = true;
        let (token, _) = CONFIG_TOKENS[idx];
        let qn = format!("__tailwind__theme__{token}");
        let token_node = virtual_node(
            parsed,
            NodeKind::Convention,
            &qn,
            token,
            Some("theme_token".to_owned()),
        );
        augmentation.nodes.push(token_node.clone());
        augmentation.edges.push(EdgeData {
            source: config_node.id,
            target: token_node.id,
            kind: EdgeKind::Defines,
            metadata: EdgeMetadata::default(),
            owner_file: parsed.file_node.id,
            is_cross_file: false,
        });
    }
}

fn add_tailwind_usage_nodes(
    parsed: &ParsedFile,
    file_path: &str,
    source: &str,
    augmentation: &mut TailwindAugmentation,
) {
    if !looks_like_tailwind_usage(source) {
        return;
    }

    let usage_qn = format!("__tailwind__usage__{file_path}");
    let usage_node = virtual_node(
        parsed,
        NodeKind::Convention,
        &usage_qn,
        "tailwind_usage",
        Some("utility_usage".to_owned()),
    );
    augmentation.nodes.push(usage_node.clone());
    augmentation
        .edges
        .push(file_edge(parsed, usage_node.id, EdgeKind::References));

    // Walk all signal matches; track which `kind` strings have already produced
    // a signal node (multiple needles can map to the same kind, e.g.
    // "responsive" → "sm:", "md:", "lg:").
    let mut emitted_kinds = rustc_hash::FxHashSet::default();
    for mat in usage_signal_ac().find_iter(source.as_bytes()) {
        let idx = mat.pattern().as_usize();
        let (kind, _) = USAGE_SIGNALS[idx];
        if !emitted_kinds.insert(kind) {
            continue;
        }
        let qn = format!("__tailwind__signal__{kind}");
        let signal_node = virtual_node(
            parsed,
            NodeKind::Convention,
            &qn,
            kind,
            Some("tailwind_signal".to_owned()),
        );
        augmentation.nodes.push(signal_node.clone());
        augmentation.edges.push(EdgeData {
            source: usage_node.id,
            target: signal_node.id,
            kind: EdgeKind::References,
            metadata: EdgeMetadata::default(),
            owner_file: parsed.file_node.id,
            is_cross_file: false,
        });
    }
}

fn is_tailwind_config(file_path: &str) -> bool {
    matches!(
        file_path,
        "tailwind.config.js" | "tailwind.config.ts" | "tailwind.config.mjs" | "tailwind.config.cjs"
    )
}

/// Returns `true` when the source file contains at least one Tailwind marker.
///
/// Uses a prebuilt `AhoCorasick` automaton over 8 needles so a single linear
/// pass replaces 8 individual `str::contains` calls.  Short-circuits at the
/// first match via `is_match`.
fn looks_like_tailwind_usage(source: &str) -> bool {
    usage_gate_ac().is_match(source.as_bytes())
}

fn virtual_node(
    parsed: &ParsedFile,
    kind: NodeKind,
    qualified_name: &str,
    name: &str,
    signature: Option<String>,
) -> NodeData {
    NodeData {
        id: ref_node_id(kind, qualified_name),
        kind,
        repo: parsed.file_node.repo.clone(),
        file_path: parsed.file_node.file_path.clone(),
        name: name.to_owned(),
        qualified_name: Some(qualified_name.to_owned()),
        external_id: Some(qualified_name.to_owned()),
        signature,
        visibility: None,
        span: parsed.file_node.span.clone(),
        is_virtual: true,
    }
}

fn file_edge(parsed: &ParsedFile, target: gather_step_core::NodeId, kind: EdgeKind) -> EdgeData {
    EdgeData {
        source: parsed.file_node.id,
        target,
        kind,
        metadata: EdgeMetadata::default(),
        owner_file: parsed.file_node.id,
        is_cross_file: false,
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::needless_raw_string_hashes)]

    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::NodeKind;

    use crate::{
        FileEntry, Language, frameworks::Framework, tree_sitter::parse_file_with_frameworks,
    };

    use super::augment;

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> Self {
            let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-tailwind-{name}-{}-{counter}",
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

    fn parse(repo_root: &Path, relative: &str) -> crate::ParsedFile {
        parse_file_with_frameworks(
            "sample-web",
            repo_root,
            &FileEntry {
                path: relative.into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &[Framework::Tailwind],
        )
        .expect("fixture should parse")
    }

    #[test]
    fn config_file_emits_theme_tokens() {
        let dir = TestDir::new("config");
        fs::write(
            dir.path().join("tailwind.config.ts"),
            r#"
export default {
  theme: {
    extend: {
      colors: { brand: '#000' },
      fontFamily: { display: ['Georgia'] },
      spacing: { 18: '4.5rem' },
    },
  },
};
"#,
        )
        .expect("config should write");

        let parsed = parse(dir.path(), "tailwind.config.ts");
        let augmentation = augment(&parsed);

        assert!(augmentation.nodes.iter().any(|node| {
            node.kind == NodeKind::Convention
                && node.external_id.as_deref() == Some("__tailwind__config")
        }));
        assert!(
            augmentation
                .nodes
                .iter()
                .any(|node| { node.external_id.as_deref() == Some("__tailwind__theme__colors") })
        );
    }

    #[test]
    fn component_file_emits_usage_signals() {
        let dir = TestDir::new("component");
        fs::write(
            dir.path().join("Card.tsx"),
            r#"
export function Card() {
  return <div className="grid bg-slate-900 text-white sm:grid-cols-2 hover:bg-slate-800 dark:bg-black" />;
}
"#,
        )
        .expect("component should write");

        let parsed = parse(dir.path(), "Card.tsx");
        let augmentation = augment(&parsed);

        assert!(
            augmentation
                .nodes
                .iter()
                .any(|node| { node.external_id.as_deref() == Some("__tailwind__usage__Card.tsx") })
        );
        assert!(
            augmentation.nodes.iter().any(|node| {
                node.external_id.as_deref() == Some("__tailwind__signal__responsive")
            })
        );
        assert!(
            augmentation
                .nodes
                .iter()
                .any(|node| { node.external_id.as_deref() == Some("__tailwind__signal__dark") })
        );
    }
}
