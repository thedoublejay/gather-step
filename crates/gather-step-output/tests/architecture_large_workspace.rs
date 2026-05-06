//! Regression test: a 32-repo workspace must keep every repo visible in
//! `gather-step-architecture.md` and avoid hitting the truncation marker.
//!
//! Reproduces the symptom seen on a 32-repo monorepo where the
//! architecture rule renders the repo map fully and then runs out of byte
//! budget partway through `## Cross-Repo Dependencies`.

use std::{
    env, fs,
    path::PathBuf,
    process,
    sync::atomic::{AtomicU64, Ordering},
};

use gather_step_core::{DepthLevel, RegistryStore, RepoIndexMetadata};
use gather_step_output::{
    ARCHITECTURE_MAX_BUDGET, ClaudeMdOptions, architecture_budget, generate_rule_files,
};
use gather_step_storage::{GraphStore, GraphStoreDb};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(name: &str) -> Self {
        let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!(
            "gather-step-output-large-{name}-{}-{id}",
            process::id()
        ));
        fs::create_dir_all(&path).expect("temp dir");
        Self { path }
    }

    fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[test]
fn architecture_rule_keeps_all_repos_for_large_workspace() {
    let root = TempDir::new("32-repos");
    let graph = GraphStoreDb::open(root.path().join("graph.redb")).expect("graph open");
    graph.bulk_insert(&[], &[]).expect("empty insert");

    let registry_path = root.path().join("registry.json");
    let mut registry = RegistryStore::open(&registry_path).expect("registry");

    let repo_count = 32_usize;
    let repo_names: Vec<String> = (0..repo_count).map(|i| format!("service-{i:02}")).collect();
    for name in &repo_names {
        registry
            .register_repo(name.clone(), root.path().join(name), Some(DepthLevel::Full))
            .expect("register repo");
        registry
            .update_repo_metadata(
                name,
                RepoIndexMetadata {
                    last_indexed_at: Some("1".to_owned()),
                    file_count: 1,
                    symbol_count: 5,
                    frameworks: vec!["nestjs".to_owned()],
                    depth_level: DepthLevel::Full,
                },
            )
            .expect("metadata");
    }

    let files = generate_rule_files(
        &graph,
        None,
        registry.registry(),
        &ClaudeMdOptions::default(),
    )
    .expect("rules should render");

    let architecture = files
        .iter()
        .find(|f| f.relative_path.ends_with("architecture.md"))
        .expect("architecture rule should be generated");

    // Every repo must appear in the repo map — no row is silently dropped.
    for name in &repo_names {
        assert!(
            architecture.content.contains(name),
            "architecture rule must include repo `{name}`. content:\n{}",
            architecture.content
        );
    }

    // The truncation marker must not appear at the default budget for a
    // 32-repo workspace. If it does, the architecture budget is no longer
    // enough for typical multi-repo monorepos.
    assert!(
        !architecture.content.contains("<!-- Truncated:"),
        "architecture rule unexpectedly truncated for {repo_count} repos:\n{}",
        architecture.content
    );

    // Architecture content must fit inside the scaled architecture budget.
    let budget = architecture_budget(repo_count);
    assert!(
        architecture.content.len() <= budget,
        "architecture rule should fit within architecture_budget({repo_count}) = {budget} bytes; got {}",
        architecture.content.len()
    );

    // The scaled budget must stay within the documented hard ceiling so
    // generated docs never balloon beyond the agreed maximum.
    assert!(
        budget <= ARCHITECTURE_MAX_BUDGET,
        "scaled budget {budget} exceeds ARCHITECTURE_MAX_BUDGET {ARCHITECTURE_MAX_BUDGET}"
    );
}

#[test]
fn architecture_budget_scales_predictably() {
    use gather_step_output::{
        ARCHITECTURE_BASE_BUDGET, ARCHITECTURE_PER_REPO_BUDGET, architecture_budget,
    };

    assert_eq!(architecture_budget(0), ARCHITECTURE_BASE_BUDGET);
    assert!(architecture_budget(10) > ARCHITECTURE_BASE_BUDGET);
    // Linear growth in the unsaturated range.
    let small = architecture_budget(8);
    let medium = architecture_budget(16);
    assert_eq!(medium - small, 8 * ARCHITECTURE_PER_REPO_BUDGET);
    // Hits the ceiling for very large workspaces.
    assert_eq!(architecture_budget(10_000), ARCHITECTURE_MAX_BUDGET);
}
