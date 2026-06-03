use std::path::{Path, PathBuf};

use gather_step_core::GatherStepConfig;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReviewRepoSpec {
    pub repo_name: String,
    /// Normalized config-relative prefix (root repo → empty) for mapping
    /// repo-local git diffs to workspace-relative paths.
    pub repo_path: String,
    pub git_repo_root: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReviewTarget {
    pub index_workspace_root: PathBuf,
    pub repos: Vec<ReviewRepoSpec>,
}

impl ReviewTarget {
    /// Assumes a validated config (the loaders enforce relative, non-escaping
    /// paths; FS-level containment is checked at index time).
    pub fn from_config(index_workspace_root: &Path, config: &GatherStepConfig) -> Self {
        let repos = config
            .repos
            .iter()
            .map(|repo| ReviewRepoSpec {
                repo_name: repo.name.clone(),
                repo_path: repo.normalized_rel_path(),
                git_repo_root: repo.resolve_root(index_workspace_root),
            })
            .collect();
        Self {
            index_workspace_root: index_workspace_root.to_path_buf(),
            repos,
        }
    }

    pub fn is_polyrepo(&self) -> bool {
        self.repos.len() > 1
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use gather_step_core::GatherStepConfig;

    use super::ReviewTarget;

    #[test]
    fn derives_one_spec_per_repo_with_workspace_joined_git_roots() {
        let config = GatherStepConfig::from_yaml_str(
            "repos:\n  - name: backend\n    path: services/backend\n  - name: web\n    path: apps/web\n",
        )
        .expect("config should parse");

        let target = ReviewTarget::from_config(Path::new("/ws"), &config);

        assert!(target.is_polyrepo());
        assert_eq!(target.repos.len(), 2);
        assert_eq!(target.repos[0].repo_name, "backend");
        assert_eq!(target.repos[0].repo_path, "services/backend");
        assert_eq!(
            target.repos[0].git_repo_root,
            PathBuf::from("/ws/services/backend")
        );
        assert_eq!(target.repos[1].repo_path, "apps/web");
        assert_eq!(target.repos[1].git_repo_root, PathBuf::from("/ws/apps/web"));
        assert_eq!(target.index_workspace_root, PathBuf::from("/ws"));
    }

    #[test]
    fn root_repo_has_empty_prefix_and_workspace_git_root() {
        let config = GatherStepConfig::from_yaml_str("repos:\n  - name: myrepo\n    path: .\n")
            .expect("config should parse");

        let target = ReviewTarget::from_config(Path::new("/ws"), &config);

        assert!(!target.is_polyrepo());
        assert_eq!(target.repos[0].repo_path, "");
        assert_eq!(target.repos[0].git_repo_root, PathBuf::from("/ws"));
    }

    #[test]
    fn normalizes_dot_slash_prefixes() {
        // A root repo (".") cannot coexist with child repos — the config
        // validator rejects them as overlapping — so this covers `./`-prefixed
        // children, the realistic polyrepo shape.
        let config = GatherStepConfig::from_yaml_str(
            "repos:\n  - name: a\n    path: ./repos/service-a\n  - name: b\n    path: ./services/service-b\n",
        )
        .expect("config should parse");

        let target = ReviewTarget::from_config(Path::new("/ws"), &config);

        assert_eq!(target.repos[0].repo_path, "repos/service-a");
        assert_eq!(
            target.repos[0].git_repo_root,
            PathBuf::from("/ws/repos/service-a")
        );
        assert_eq!(target.repos[1].repo_path, "services/service-b");
        assert_eq!(
            target.repos[1].git_repo_root,
            PathBuf::from("/ws/services/service-b")
        );
    }

    #[test]
    fn loader_rejects_unsafe_paths_so_specs_are_always_contained() {
        // The safety contract `from_config` relies on: absolute and escaping
        // paths never reach it because the loader validates first.
        assert!(GatherStepConfig::from_yaml_str("repos:\n  - name: x\n    path: /etc\n").is_err());
        assert!(
            GatherStepConfig::from_yaml_str("repos:\n  - name: x\n    path: ../escape\n").is_err()
        );
    }
}
