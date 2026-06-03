use std::path::{Path, PathBuf};

use gather_step_core::GatherStepConfig;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReviewRepoSpec {
    pub repo_name: String,
    pub git_repo_root: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReviewTarget {
    pub index_workspace_root: PathBuf,
    pub repos: Vec<ReviewRepoSpec>,
}

impl ReviewTarget {
    #[must_use]
    pub fn from_config(index_workspace_root: &Path, config: &GatherStepConfig) -> Self {
        let repos = config
            .repos
            .iter()
            .map(|repo| {
                let git_repo_root = if repo.path.is_empty() || repo.path == "." {
                    index_workspace_root.to_path_buf()
                } else {
                    index_workspace_root.join(&repo.path)
                };
                ReviewRepoSpec {
                    repo_name: repo.name.clone(),
                    git_repo_root,
                }
            })
            .collect();
        Self {
            index_workspace_root: index_workspace_root.to_path_buf(),
            repos,
        }
    }

    #[must_use]
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
        assert_eq!(
            target.repos[0].git_repo_root,
            PathBuf::from("/ws/services/backend")
        );
        assert_eq!(target.repos[1].repo_name, "web");
        assert_eq!(target.repos[1].git_repo_root, PathBuf::from("/ws/apps/web"));
        assert_eq!(target.index_workspace_root, PathBuf::from("/ws"));
    }

    #[test]
    fn single_repo_at_root_has_git_root_equal_to_workspace() {
        let config = GatherStepConfig::from_yaml_str("repos:\n  - name: myrepo\n    path: .\n")
            .expect("config should parse");

        let target = ReviewTarget::from_config(Path::new("/ws"), &config);

        assert!(!target.is_polyrepo());
        assert_eq!(target.repos.len(), 1);
        assert_eq!(target.repos[0].git_repo_root, PathBuf::from("/ws"));
    }
}
