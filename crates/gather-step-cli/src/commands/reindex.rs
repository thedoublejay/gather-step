use anyhow::Result;
use clap::Args;

use crate::{
    app::AppContext,
    commands::{clean, index},
    path_safety,
};

#[derive(Debug, Args, PartialEq, Eq)]
pub struct ReindexArgs {
    #[command(flatten)]
    pub index: index::IndexArgs,
}

pub fn run(app: &AppContext, args: ReindexArgs) -> Result<()> {
    let defaults = app.workspace_paths();
    let registry_path = args
        .index
        .registry
        .clone()
        .unwrap_or(defaults.registry_path);
    let storage_root = args.index.storage.clone().unwrap_or(defaults.storage_root);

    // Canonicalize the workspace root (already canonical in AppContext, but we
    // re-derive from the field to make the safety dependency explicit).
    let canonical_root = path_safety::canonical_workspace_root(&app.workspace_path)?;

    // Validate every cleanup target before any removal.  If any target escapes
    // the workspace root the whole operation is rejected.
    clean::validate_and_clean_generated_paths(
        &[storage_root.clone(), registry_path.clone()],
        &canonical_root,
    )?;

    index::run(app, args.index)
}
