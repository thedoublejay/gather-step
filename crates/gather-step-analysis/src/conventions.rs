use std::collections::BTreeMap;

use gather_step_core::NodeKind;
use gather_step_storage::GraphStore;
use thiserror::Error;

fn virtual_kind_label(kind: NodeKind) -> &'static str {
    match kind {
        NodeKind::Route => "route",
        NodeKind::Topic => "topic",
        NodeKind::Queue => "queue",
        NodeKind::Subject => "subject",
        NodeKind::Stream => "stream",
        NodeKind::Event => "event",
        _ => "unknown",
    }
}

#[derive(Debug, Error)]
pub enum ConventionError {
    #[error(transparent)]
    Store(#[from] gather_step_storage::GraphStoreError),
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConventionFinding {
    pub repo: String,
    pub pattern: String,
    pub description: String,
    pub confidence: f64,
    pub examples: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConventionReport {
    pub findings: Vec<ConventionFinding>,
}

pub fn detect_conventions<S: GraphStore>(
    store: &S,
    repo: &str,
) -> Result<ConventionReport, ConventionError> {
    let nodes = store.nodes_by_repo(repo)?;
    let mut top_dir_counts = BTreeMap::<String, Vec<String>>::new();
    let mut decorator_counts = BTreeMap::<String, Vec<String>>::new();
    let mut virtual_kind_counts = BTreeMap::<String, Vec<String>>::new();

    for node in nodes {
        if node.kind == NodeKind::File
            && let Some((top_dir, _)) = node.file_path.split_once('/')
        {
            top_dir_counts
                .entry(top_dir.to_owned())
                .or_default()
                .push(node.file_path.clone());
        }
        if node.kind == NodeKind::Decorator {
            decorator_counts
                .entry(node.name.clone())
                .or_default()
                .push(node.file_path.clone());
        }
        if matches!(
            node.kind,
            NodeKind::Route
                | NodeKind::Topic
                | NodeKind::Queue
                | NodeKind::Subject
                | NodeKind::Stream
                | NodeKind::Event
        ) {
            virtual_kind_counts
                .entry(virtual_kind_label(node.kind).to_owned())
                .or_default()
                .push(node.file_path.clone());
        }
    }

    let mut findings = Vec::new();
    for (top_dir, examples) in top_dir_counts {
        if examples.len() >= 2 {
            findings.push(ConventionFinding {
                repo: repo.to_owned(),
                pattern: format!("files_under:{top_dir}"),
                description: format!("Code is commonly organized under `{top_dir}/...`."),
                confidence: 0.7,
                examples: examples.into_iter().take(3).collect(),
            });
        }
    }
    for (decorator, examples) in decorator_counts {
        if examples.len() >= 2 {
            findings.push(ConventionFinding {
                repo: repo.to_owned(),
                pattern: format!("decorator:{decorator}"),
                description: format!("Decorator `{decorator}` appears repeatedly across the repo."),
                confidence: 0.8,
                examples: examples.into_iter().take(3).collect(),
            });
        }
    }
    for (virtual_kind, examples) in virtual_kind_counts {
        if examples.len() >= 2 {
            findings.push(ConventionFinding {
                repo: repo.to_owned(),
                pattern: format!("virtual_surface:{virtual_kind}"),
                description: format!("The repo consistently models `{virtual_kind}` surfaces."),
                confidence: 0.75,
                examples: examples.into_iter().take(3).collect(),
            });
        }
    }
    findings.sort_by(|left, right| left.pattern.cmp(&right.pattern));

    Ok(ConventionReport { findings })
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::{NodeData, NodeKind, node_id};
    use gather_step_storage::{GraphStore, GraphStoreDb};

    use super::detect_conventions;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new(name: &str) -> Self {
            let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-conventions-{name}-{}-{id}.redb",
                process::id()
            ));
            Self { path }
        }
        fn path(&self) -> &Path {
            &self.path
        }
    }
    impl Drop for TempDb {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
        }
    }

    #[test]
    fn detects_repeated_directory_and_decorator_patterns() {
        let temp_db = TempDb::new("conventions");
        let store = GraphStoreDb::open(temp_db.path()).expect("open graph");
        let nodes = vec![
            node(
                "service-a",
                "src/routes/a.ts",
                NodeKind::File,
                "src/routes/a.ts",
            ),
            node(
                "service-a",
                "src/routes/b.ts",
                NodeKind::File,
                "src/routes/b.ts",
            ),
            node(
                "service-a",
                "src/services/a.ts",
                NodeKind::File,
                "src/services/a.ts",
            ),
            node(
                "service-a",
                "src/routes/a.ts",
                NodeKind::Decorator,
                "Controller",
            ),
            node(
                "service-a",
                "src/routes/b.ts",
                NodeKind::Decorator,
                "Controller",
            ),
        ];
        store.bulk_insert(&nodes, &[]).expect("write graph");

        let report = detect_conventions(&store, "service-a").expect("report");
        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.pattern == "decorator:Controller")
        );
        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.pattern == "files_under:src")
        );
    }

    fn node(repo: &str, file_path: &str, kind: NodeKind, name: &str) -> NodeData {
        NodeData {
            id: node_id(repo, file_path, kind, name),
            kind,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: name.to_owned(),
            qualified_name: None,
            external_id: None,
            signature: None,
            visibility: None,
            span: None,
            is_virtual: false,
        }
    }
}
