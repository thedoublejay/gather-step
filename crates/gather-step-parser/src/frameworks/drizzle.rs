use std::sync::OnceLock;

use gather_step_core::{EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, ref_node_id};
use memchr::memmem;

use crate::tree_sitter::ParsedFile;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DrizzleAugmentation {
    pub nodes: Vec<NodeData>,
    pub edges: Vec<EdgeData>,
}

pub fn augment(parsed: &ParsedFile) -> DrizzleAugmentation {
    let source = &*parsed.source;

    let mut augmentation = DrizzleAugmentation::default();
    add_table_nodes(parsed, source, &mut augmentation);
    add_client_and_query_nodes(parsed, source, &mut augmentation);
    augmentation
}

fn add_table_nodes(parsed: &ParsedFile, source: &str, augmentation: &mut DrizzleAugmentation) {
    for line in source.lines() {
        let trimmed = line.trim();
        let Some((symbol, table_name)) = extract_table_declaration(trimmed) else {
            continue;
        };
        let effective = if table_name.is_empty() {
            symbol
        } else {
            table_name
        };
        let qn = format!("__drizzle_table__{effective}");
        let node = virtual_node(
            parsed,
            NodeKind::Entity,
            &qn,
            effective,
            Some("drizzle_table".to_owned()),
        );
        augmentation.nodes.push(node.clone());
        augmentation
            .edges
            .push(file_edge(parsed, node.id, EdgeKind::Defines));
    }
}

/// SIMD-accelerated single-needle finder for the `drizzle(` client instantiation marker.
static DRIZZLE_CLIENT_FINDER: OnceLock<memmem::Finder<'static>> = OnceLock::new();

fn drizzle_client_finder() -> &'static memmem::Finder<'static> {
    DRIZZLE_CLIENT_FINDER.get_or_init(|| memmem::Finder::new("drizzle(").into_owned())
}

fn add_client_and_query_nodes(
    parsed: &ParsedFile,
    source: &str,
    augmentation: &mut DrizzleAugmentation,
) {
    if drizzle_client_finder().find(source.as_bytes()).is_some() {
        let client = virtual_node(
            parsed,
            NodeKind::Service,
            "__drizzle_client__default",
            "drizzle",
            Some("client".to_owned()),
        );
        augmentation.nodes.push(client.clone());
        augmentation
            .edges
            .push(file_edge(parsed, client.id, EdgeKind::Defines));
    }

    for (line_number, line) in source.lines().enumerate() {
        let Some(table) = extract_drizzle_query_target(line) else {
            continue;
        };
        let qn = format!("__drizzle_table__{table}");
        let node = virtual_node(
            parsed,
            NodeKind::Entity,
            &qn,
            table,
            Some("query_target".to_owned()),
        );
        augmentation.nodes.push(node.clone());
        augmentation.edges.push(EdgeData {
            source: owner_for_line(parsed, line_number + 1),
            target: node.id,
            kind: EdgeKind::References,
            metadata: EdgeMetadata::default(),
            owner_file: parsed.file_node.id,
            is_cross_file: false,
        });
    }
}

fn extract_table_declaration(line: &str) -> Option<(&str, &str)> {
    const BUILDERS: [&str; 4] = [
        "pgTable(",
        "mysqlTable(",
        "sqliteTable(",
        "singlestoreTable(",
    ];

    let symbol = line
        .strip_prefix("export const ")
        .or_else(|| line.strip_prefix("const "))?
        .split('=')
        .next()?
        .trim();

    for builder in BUILDERS {
        let Some(index) = line.find(builder) else {
            continue;
        };
        let rest = &line[index + builder.len()..];
        let table_name = rest
            .trim()
            .trim_start_matches('"')
            .split('"')
            .next()
            .unwrap_or(symbol);
        return Some((symbol, table_name));
    }
    None
}

fn extract_drizzle_query_target(line: &str) -> Option<&str> {
    for prefix in [".from(", ".insert(", ".update(", ".delete("] {
        let index = line.find(prefix)?;
        let rest = &line[index + prefix.len()..];
        let target = rest
            .split([')', ',', ' '])
            .next()
            .unwrap_or_default()
            .trim();
        if !target.is_empty() {
            return Some(target);
        }
    }
    None
}

fn owner_for_line(parsed: &ParsedFile, line: usize) -> gather_step_core::NodeId {
    parsed
        .symbols
        .iter()
        .find(|symbol| {
            symbol.node.kind == NodeKind::Function
                && symbol.node.span.as_ref().is_some_and(|span| {
                    (span.line_start as usize) <= line && line <= (span.line_end() as usize)
                })
        })
        .map_or(parsed.file_node.id, |symbol| symbol.node.id)
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

    use gather_step_core::{EdgeKind, NodeKind};

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
                "gather-step-drizzle-{name}-{}-{counter}",
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
            "sample-app",
            repo_root,
            &FileEntry {
                path: relative.into(),
                language: Language::TypeScript,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &[Framework::Drizzle],
        )
        .expect("fixture should parse")
    }

    #[test]
    fn table_declaration_emits_entity_node() {
        let dir = TestDir::new("table");
        fs::write(
            dir.path().join("schema.ts"),
            r#"
export const users = pgTable("users", {
  id: serial("id"),
});
"#,
        )
        .expect("schema should write");

        let parsed = parse(dir.path(), "schema.ts");
        let augmentation = augment(&parsed);
        assert!(augmentation.nodes.iter().any(|node| {
            node.kind == NodeKind::Entity
                && node.external_id.as_deref() == Some("__drizzle_table__users")
        }));
    }

    #[test]
    fn query_usage_emits_reference_edge() {
        let dir = TestDir::new("query");
        fs::write(
            dir.path().join("repo.ts"),
            r#"
const db = drizzle(pool);

export async function listUsers() {
  return db.select().from(users);
}
"#,
        )
        .expect("repo should write");

        let parsed = parse(dir.path(), "repo.ts");
        let augmentation = augment(&parsed);
        let users = augmentation
            .nodes
            .iter()
            .find(|node| node.external_id.as_deref() == Some("__drizzle_table__users"))
            .expect("users node should exist");
        assert!(
            augmentation
                .edges
                .iter()
                .any(|edge| { edge.kind == EdgeKind::References && edge.target == users.id })
        );
    }
}
