use std::io::Write;

use anyhow::Result;
use clap::CommandFactory;

use crate::{
    app::AppContext,
    commands::{Cli, init, status},
};

pub fn run(app: &AppContext) -> Result<()> {
    if !app.is_interactive() {
        Cli::command().print_help()?;
        std::io::stdout().write_all(b"\n")?;
        return Ok(());
    }

    if app.workspace_paths().config_path.exists() {
        status::run_default(app)
    } else {
        init::run(app, init::InitArgs::default())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use indicatif::MultiProgress;

    use super::*;

    #[test]
    fn configured_unindexed_workspace_renders_status_summary() {
        let temp = tempfile::tempdir().expect("temp dir");
        fs::write(temp.path().join("gather-step.config.yaml"), "repos: []\n").expect("config");

        let app = AppContext {
            workspace_path: temp.path().to_path_buf(),
            repo_filter: None,
            json_output: false,
            no_interactive: false,
            stdin_is_tty: true,
            stdout_is_tty: true,
            ci_env_set: false,
            show_banner: false,
            multi_progress: MultiProgress::new(),
        };

        run(&app).expect("status summary should render");
    }
}
