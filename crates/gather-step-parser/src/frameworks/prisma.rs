use std::sync::OnceLock;

use gather_step_core::{EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, ref_node_id};
use memchr::memmem;

use crate::tree_sitter::ParsedFile;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PrismaAugmentation {
    pub nodes: Vec<NodeData>,
    pub edges: Vec<EdgeData>,
}

pub fn augment(parsed: &ParsedFile) -> PrismaAugmentation {
    let source = &*parsed.source;

    let mut augmentation = PrismaAugmentation::default();
    let file_path = parsed.file_node.file_path.clone();
    if file_path.ends_with(".prisma") {
        add_schema_nodes(parsed, source, &mut augmentation);
    }
    add_client_and_query_nodes(parsed, source, &mut augmentation);
    augmentation
}

fn add_schema_nodes(parsed: &ParsedFile, source: &str, augmentation: &mut PrismaAugmentation) {
    for line in source.lines() {
        let trimmed = line.trim();
        if let Some(provider) = extract_assignment_value(trimmed, "provider") {
            let qn = format!("__prisma_provider__{provider}");
            let node = virtual_node(
                parsed,
                NodeKind::Service,
                &qn,
                provider,
                Some("provider".to_owned()),
            );
            augmentation.nodes.push(node.clone());
            augmentation
                .edges
                .push(file_edge(parsed, node.id, EdgeKind::Defines));
        }
        if let Some(model) = trimmed.strip_prefix("model ").and_then(first_word) {
            let qn = format!("__prisma_model__{model}");
            let node = virtual_node(
                parsed,
                NodeKind::Entity,
                &qn,
                model,
                Some("prisma_model".to_owned()),
            );
            augmentation.nodes.push(node.clone());
            augmentation
                .edges
                .push(file_edge(parsed, node.id, EdgeKind::Defines));
        }
    }
}

/// SIMD-accelerated single-needle finder for the `PrismaClient` import marker.
static PRISMA_CLIENT_FINDER: OnceLock<memmem::Finder<'static>> = OnceLock::new();

fn prisma_client_finder() -> &'static memmem::Finder<'static> {
    PRISMA_CLIENT_FINDER.get_or_init(|| memmem::Finder::new("PrismaClient").into_owned())
}

fn add_client_and_query_nodes(
    parsed: &ParsedFile,
    source: &str,
    augmentation: &mut PrismaAugmentation,
) {
    if prisma_client_finder().find(source.as_bytes()).is_some() {
        let client_node = virtual_node(
            parsed,
            NodeKind::Service,
            "__prisma_client__default",
            "PrismaClient",
            Some("client".to_owned()),
        );
        augmentation.nodes.push(client_node.clone());
        augmentation
            .edges
            .push(file_edge(parsed, client_node.id, EdgeKind::Defines));
    }

    for (line_number, line) in source.lines().enumerate() {
        let Some((model, op)) = extract_prisma_query(line) else {
            continue;
        };
        let model_name = pascal_case(model);
        let qn = format!("__prisma_model__{model_name}");
        let node = virtual_node(
            parsed,
            NodeKind::Entity,
            &qn,
            &model_name,
            Some(format!("query:{op}")),
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

fn extract_prisma_query(line: &str) -> Option<(&str, &str)> {
    const OPS: [&str; 8] = [
        "findMany",
        "findUnique",
        "findFirst",
        "create",
        "update",
        "delete",
        "upsert",
        "count",
    ];

    for op in OPS {
        let needle = format!(".{op}(");
        let index = line.find(&needle)?;
        let prefix = &line[..index];
        let model = prefix.rsplit('.').next()?.trim();
        if model.is_empty() {
            continue;
        }
        return Some((model, op));
    }
    None
}

fn extract_assignment_value<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    let (_, rhs) = line.split_once('=')?;
    let lhs = line[..line.find('=')?].trim();
    if !lhs.ends_with(key) {
        return None;
    }
    Some(rhs.trim().trim_matches('"'))
}

fn first_word(value: &str) -> Option<&str> {
    value.split_whitespace().next()
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

fn pascal_case(value: &str) -> String {
    let mut out = String::new();
    let mut uppercase = true;
    for ch in value.chars() {
        if matches!(ch, '_' | '-') {
            uppercase = true;
            continue;
        }
        if uppercase {
            out.extend(ch.to_uppercase());
            uppercase = false;
        } else {
            out.push(ch);
        }
    }
    out
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
                "gather-step-prisma-{name}-{}-{counter}",
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

    fn parse(repo_root: &Path, relative: &str, language: Language) -> crate::ParsedFile {
        parse_file_with_frameworks(
            "sample-app",
            repo_root,
            &FileEntry {
                path: relative.into(),
                language,
                size_bytes: 0,
                content_hash: [0; 32],
                source_bytes: None,
            },
            &[Framework::Prisma],
        )
        .expect("fixture should parse")
    }

    #[test]
    fn schema_file_emits_provider_and_model_nodes() {
        let dir = TestDir::new("schema");
        fs::create_dir_all(dir.path().join("prisma")).expect("prisma dir");
        fs::write(
            dir.path().join("prisma/schema.prisma"),
            r#"
datasource db {
  provider = "postgresql"
}

model User {
  id Int @id
}
"#,
        )
        .expect("schema should write");

        let parsed = parse(dir.path(), "prisma/schema.prisma", Language::TypeScript);
        let augmentation = augment(&parsed);

        assert!(augmentation.nodes.iter().any(|node| {
            node.kind == NodeKind::Service
                && node.external_id.as_deref() == Some("__prisma_provider__postgresql")
        }));
        assert!(augmentation.nodes.iter().any(|node| {
            node.kind == NodeKind::Entity
                && node.external_id.as_deref() == Some("__prisma_model__User")
        }));
    }

    #[test]
    fn client_query_emits_entity_reference() {
        let dir = TestDir::new("query");
        fs::write(
            dir.path().join("service.ts"),
            r#"
import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

export async function listUsers() {
  return prisma.user.findMany();
}
"#,
        )
        .expect("service should write");

        let parsed = parse(dir.path(), "service.ts", Language::TypeScript);
        let augmentation = augment(&parsed);

        let user = augmentation
            .nodes
            .iter()
            .find(|node| node.external_id.as_deref() == Some("__prisma_model__User"))
            .expect("user model should exist");
        assert!(
            augmentation
                .edges
                .iter()
                .any(|edge| { edge.kind == EdgeKind::References && edge.target == user.id })
        );
    }
}
