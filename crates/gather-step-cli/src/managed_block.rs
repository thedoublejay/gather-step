//! Idempotent managed-block writer for `CLAUDE.md` and `AGENTS.md`.
//!
//! `gather-step generate claude-md --target=summary` and
//! `gather-step generate agents-md` produce sidecar files
//! (`CLAUDE.gather.md` / `AGENTS.gather.md`) that nothing auto-loads.
//! This module appends a sentinel-fenced block to the workspace-root
//! `CLAUDE.md` / `AGENTS.md` so Claude Code and Codex actually pick up
//! the generated context.
//!
//! The block is bounded by `gather-step:start` / `gather-step:end`
//! sentinels, so re-running init (or `--install-include`) replaces the
//! contents between fences without disturbing user-authored content.

use std::{fs, path::Path};

use anyhow::{Context, Result};

use crate::commands::generate::GeneratedFileOutput;

/// Which managed file to install the include block into.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManagedBlockTarget {
    /// `CLAUDE.md` at the workspace root, importing `CLAUDE.gather.md`.
    Claude,
    /// `AGENTS.md` at the workspace root, importing `AGENTS.gather.md`.
    Agents,
}

impl ManagedBlockTarget {
    fn target_filename(self) -> &'static str {
        match self {
            ManagedBlockTarget::Claude => "CLAUDE.md",
            ManagedBlockTarget::Agents => "AGENTS.md",
        }
    }

    fn import_filename(self) -> &'static str {
        match self {
            ManagedBlockTarget::Claude => "CLAUDE.gather.md",
            ManagedBlockTarget::Agents => "AGENTS.gather.md",
        }
    }

    fn audience(self) -> &'static str {
        match self {
            ManagedBlockTarget::Claude => "Claude Code",
            ManagedBlockTarget::Agents => "Codex",
        }
    }
}

const MANAGED_BLOCK_START: &str = "<!-- gather-step:start -->";
const MANAGED_BLOCK_END: &str = "<!-- gather-step:end -->";

/// Append (or refresh) the managed include block in `target`.
///
/// Returns `Ok(Some(...))` describing the file that was written, or
/// `Ok(None)` when the file already had an identical block (no-op for
/// re-runs).
///
/// # Errors
/// Returns an error if the file system rejects the read or write — never
/// truncates user content outside the fence.
pub fn install_managed_block_for_target(
    workspace_root: &Path,
    target: ManagedBlockTarget,
    version: &str,
) -> Result<Option<GeneratedFileOutput>> {
    let path = workspace_root.join(target.target_filename());
    let block = render_managed_block(target, version);

    let existing = match fs::read_to_string(&path) {
        Ok(contents) => Some(contents),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(err) => {
            return Err(err).with_context(|| format!("reading {}", path.display()));
        }
    };

    let new_contents = match existing {
        None => block.clone(),
        Some(existing) => match splice_managed_block(&existing, &block) {
            Splice::Updated(updated) => updated,
            Splice::Identical => return Ok(None),
        },
    };

    fs::write(&path, &new_contents).with_context(|| format!("writing {}", path.display()))?;

    Ok(Some(GeneratedFileOutput {
        path: path.display().to_string(),
        bytes: new_contents.len(),
    }))
}

fn render_managed_block(target: ManagedBlockTarget, version: &str) -> String {
    let import_file = target.import_filename();
    let audience = target.audience();
    format!(
        "{MANAGED_BLOCK_START}\n\
         <!-- Managed by `gather-step` v{version}. Re-run `gather-step generate \
         {generate_command} --install-include` to refresh.\n\
              Edit `{import_file}` (regenerated) or content outside this fence \
         (preserved). -->\n\
         \n\
         @{import_file}\n\
         \n\
         ## How to use gather-step ({audience})\n\
         - Reach for `gather-step` MCP tools (or `gather-step pack ...`) before \
         broad file searches when planning, tracing, or reviewing changes.\n\
         - Cite verified findings with file paths and repos.\n\
         - When a finding looks wrong, open an issue at \
         <https://github.com/thedoublejay/gather-step/issues>.\n\
         {MANAGED_BLOCK_END}\n",
        generate_command = match target {
            ManagedBlockTarget::Claude => "claude-md --target=summary",
            ManagedBlockTarget::Agents => "agents-md",
        },
    )
}

enum Splice {
    Updated(String),
    Identical,
}

fn splice_managed_block(existing: &str, block: &str) -> Splice {
    if let Some(start) = existing.find(MANAGED_BLOCK_START)
        && let Some(end_rel) = existing[start..].find(MANAGED_BLOCK_END)
    {
        let end = start + end_rel + MANAGED_BLOCK_END.len();
        // The block already ends with `\n`. The suffix begins with the
        // first byte after the end sentinel (typically `\n` plus user
        // content), so concatenation reads "block\n[suffix...]" and stays
        // idempotent across re-runs.
        let new_contents = format!(
            "{prefix}{block}{suffix}",
            prefix = &existing[..start],
            block = block,
            suffix = strip_leading_newline(&existing[end..]),
        );
        if new_contents == existing {
            Splice::Identical
        } else {
            Splice::Updated(new_contents)
        }
    } else {
        // No existing block — append after a blank-line separator so the
        // managed block reads as its own paragraph.
        let separator = if existing.is_empty() || existing.ends_with("\n\n") {
            ""
        } else if existing.ends_with('\n') {
            "\n"
        } else {
            "\n\n"
        };
        Splice::Updated(format!("{existing}{separator}{block}"))
    }
}

/// Drop a single leading `\n` so splicing does not introduce double
/// newlines when the existing file had `<!-- gather-step:end -->\n...`.
fn strip_leading_newline(s: &str) -> &str {
    s.strip_prefix('\n').unwrap_or(s)
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::{ManagedBlockTarget, install_managed_block_for_target};

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> Self {
            let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-managed-block-{name}-{}-{id}",
                process::id()
            ));
            fs::create_dir_all(&path).expect("test dir should exist");
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

    #[test]
    fn creates_claude_md_when_missing() {
        let temp = TestDir::new("create-claude");
        let written =
            install_managed_block_for_target(temp.path(), ManagedBlockTarget::Claude, "9.9.9")
                .expect("install ok")
                .expect("file written");

        let body = fs::read_to_string(&written.path).expect("read claude md");
        assert!(body.contains("<!-- gather-step:start -->"));
        assert!(body.contains("<!-- gather-step:end -->"));
        assert!(body.contains("@CLAUDE.gather.md"));
        assert!(body.contains("https://github.com/thedoublejay/gather-step/issues"));
    }

    #[test]
    fn preserves_existing_user_content_above_fence() {
        let temp = TestDir::new("preserve-existing");
        let claude_md = temp.path().join("CLAUDE.md");
        fs::write(&claude_md, "# My Project\n\nUser-authored guidance.\n").expect("seed");

        install_managed_block_for_target(temp.path(), ManagedBlockTarget::Claude, "9.9.9")
            .expect("install ok");

        let body = fs::read_to_string(&claude_md).expect("read claude md");
        assert!(body.starts_with("# My Project\n\nUser-authored guidance."));
        assert!(body.contains("<!-- gather-step:start -->"));
    }

    #[test]
    fn refreshing_is_idempotent() {
        let temp = TestDir::new("idempotent");
        install_managed_block_for_target(temp.path(), ManagedBlockTarget::Agents, "9.9.9")
            .expect("first install");
        let after_first =
            fs::read_to_string(temp.path().join("AGENTS.md")).expect("read after first");

        let second =
            install_managed_block_for_target(temp.path(), ManagedBlockTarget::Agents, "9.9.9")
                .expect("second install");

        let after_second =
            fs::read_to_string(temp.path().join("AGENTS.md")).expect("read after second");
        assert_eq!(after_first, after_second);
        assert!(second.is_none(), "second install should be a no-op");
    }

    #[test]
    fn refresh_replaces_old_managed_block_in_place() {
        let temp = TestDir::new("refresh-version");
        install_managed_block_for_target(temp.path(), ManagedBlockTarget::Claude, "1.0.0")
            .expect("first");

        install_managed_block_for_target(temp.path(), ManagedBlockTarget::Claude, "2.0.0")
            .expect("second");

        let body = fs::read_to_string(temp.path().join("CLAUDE.md")).expect("read after refresh");
        assert!(body.contains("v2.0.0"));
        assert!(!body.contains("v1.0.0"));
        // Single managed block.
        let starts = body.matches("<!-- gather-step:start -->").count();
        assert_eq!(starts, 1);
    }

    #[test]
    fn agents_target_writes_agents_md() {
        let temp = TestDir::new("agents");
        let written =
            install_managed_block_for_target(temp.path(), ManagedBlockTarget::Agents, "9.9.9")
                .expect("install ok")
                .expect("file written");
        assert!(written.path.ends_with("AGENTS.md"));
        let body = fs::read_to_string(&written.path).expect("read agents md");
        assert!(body.contains("@AGENTS.gather.md"));
    }
}
