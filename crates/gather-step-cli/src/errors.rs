use std::io::ErrorKind;

use anyhow::Error;
use gather_step_core::ConfigError;
use gather_step_storage::{GraphStoreError, MetadataStoreError, SearchStoreError};

const SCHEMA_VERSION_MISMATCH_MESSAGE: &str = "Index schema version mismatch — built by a different gather-step release. Next step: run `gather-step index --auto-recover` to rebuild, or `gather-step clean && gather-step index`.";

#[must_use]
pub fn format_operator_error(error: &Error) -> String {
    let full = error_chain_text(error);

    for cause in error.chain() {
        if let Some(config_error) = cause.downcast_ref::<ConfigError>() {
            return format_config_error(config_error);
        }
        if let Some(graph_error) = cause.downcast_ref::<GraphStoreError>() {
            match graph_error {
                GraphStoreError::StorageHeld { .. }
                | GraphStoreError::StorageHeldByDaemon { .. } => {
                    return "Another gather-step process is using this workspace. Stop `gather-step watch` or `gather-step serve --watch`, then retry.".to_owned();
                }
                GraphStoreError::Corrupt { .. } => {
                    return "Your index is corrupt or incomplete. Run `gather-step index --auto-recover` to rebuild generated state, or run `gather-step clean && gather-step index`.".to_owned();
                }
                GraphStoreError::SchemaVersionMismatch { .. } => {
                    return SCHEMA_VERSION_MISMATCH_MESSAGE.to_owned();
                }
                _ => {}
            }
        }
        if let Some(search_error) = cause.downcast_ref::<SearchStoreError>()
            && matches!(search_error, SearchStoreError::SchemaVersionMismatch { .. })
        {
            return SCHEMA_VERSION_MISMATCH_MESSAGE.to_owned();
        }
        if let Some(metadata_error) = cause.downcast_ref::<MetadataStoreError>()
            && matches!(
                metadata_error,
                MetadataStoreError::SchemaVersionMismatch { .. }
            )
        {
            return SCHEMA_VERSION_MISMATCH_MESSAGE.to_owned();
        }
    }

    if contains_ascii_case_insensitive(&full, "workspace is not a git repository") {
        return "Workspace is not a git repository. Next step: run from a git checkout or omit `--release-gate` for an unsealed run.".to_owned();
    }
    if contains_ascii_case_insensitive(&full, ".gather-step")
        && contains_ascii_case_insensitive(&full, "permission denied")
    {
        return "Cannot write `.gather-step` generated state. Next step: fix permissions on `.gather-step` or pass writable `--storage`/`--registry` paths.".to_owned();
    }
    if contains_ascii_case_insensitive(&full, "database already open")
        || contains_ascii_case_insensitive(&full, "already locked by another gather-step process")
        || contains_ascii_case_insensitive(&full, "locked by gather-step pid")
    {
        return "Another gather-step process is using this workspace. Stop `gather-step watch` or `gather-step serve --watch`, then retry.".to_owned();
    }
    if contains_ascii_case_insensitive(&full, "db corrupted")
        || contains_ascii_case_insensitive(&full, "corrupt")
        || contains_ascii_case_insensitive(&full, "repair aborted")
    {
        return "Your index is corrupt or incomplete. Run `gather-step index --auto-recover` to rebuild generated state, or run `gather-step clean && gather-step index`.".to_owned();
    }
    if contains_ascii_case_insensitive(&full, "schema version mismatch")
        || contains_ascii_case_insensitive(&full, "manual upgrade required")
    {
        return SCHEMA_VERSION_MISMATCH_MESSAGE.to_owned();
    }

    full
}

fn format_config_error(error: &ConfigError) -> String {
    match error {
        ConfigError::Read { path, source } if source.kind() == ErrorKind::NotFound => {
            format!(
                "Config not found: {path}. Next step: run `gather-step init` or pass `--config <path>`."
            )
        }
        ConfigError::Read { path, source } if source.kind() == ErrorKind::PermissionDenied => {
            format!(
                "Cannot read config: {path}. Next step: fix file permissions or pass `--config <path>`."
            )
        }
        ConfigError::Read { path, .. } => {
            format!(
                "Cannot read config: {path}. Next step: fix the path or pass `--config <path>`."
            )
        }
        ConfigError::Parse { path, .. } => {
            format!("Config YAML is malformed: {path}. Next step: fix the YAML syntax and rerun.")
        }
        ConfigError::Validation { reason, .. } if reason.contains("path does not exist") => {
            format!(
                "Configured repo path does not exist: {reason}. Next step: create the repo directory or fix the repo path in the config."
            )
        }
        ConfigError::Validation { reason, .. } => {
            format!("Config is invalid: {reason}. Next step: fix the config and rerun.")
        }
    }
}

fn error_chain_text(error: &Error) -> String {
    one_line(
        error
            .chain()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(": "),
    )
}

fn one_line(message: impl AsRef<str>) -> String {
    message
        .as_ref()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn contains_ascii_case_insensitive(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle.as_bytes()))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{SCHEMA_VERSION_MISMATCH_MESSAGE, format_operator_error};

    #[test]
    fn graph_store_schema_mismatch_maps_to_friendly_message() {
        let raw = gather_step_storage::GraphStoreError::SchemaVersionMismatch {
            path: PathBuf::from("/tmp/graph.redb"),
            stored: 99,
            expected: 0,
        };
        let err: anyhow::Error = anyhow::Error::new(raw);
        assert_eq!(format_operator_error(&err), SCHEMA_VERSION_MISMATCH_MESSAGE);
    }

    #[test]
    fn search_store_schema_mismatch_maps_to_friendly_message() {
        let raw = gather_step_storage::SearchStoreError::SchemaVersionMismatch {
            stored: "99".to_owned(),
            expected: 1,
        };
        let err: anyhow::Error = anyhow::Error::new(raw);
        assert_eq!(format_operator_error(&err), SCHEMA_VERSION_MISMATCH_MESSAGE);
    }

    #[test]
    fn metadata_store_schema_mismatch_maps_to_friendly_message() {
        let raw = gather_step_storage::MetadataStoreError::SchemaVersionMismatch {
            stored: 99,
            expected: 0,
        };
        let err: anyhow::Error = anyhow::Error::new(raw);
        assert_eq!(format_operator_error(&err), SCHEMA_VERSION_MISMATCH_MESSAGE);
    }

    #[test]
    fn unrelated_io_error_is_not_remapped_to_schema_message() {
        let err: anyhow::Error = anyhow::Error::msg("read /tmp/foo: permission denied");
        let msg = format_operator_error(&err);
        assert!(
            !msg.contains("schema version mismatch"),
            "permission-denied error must not be remapped to schema-mismatch message: {msg}"
        );
    }

    #[test]
    fn unhandled_error_preserves_full_cause_chain() {
        // Wrap an inner error with anyhow::Context so the chain has two links.
        // The fallback path must surface both, not just the outermost message.
        use anyhow::Context;
        let inner: anyhow::Error = anyhow::Error::msg("config not found at worktree root");
        let wrapped: anyhow::Result<()> =
            Err::<(), _>(inner).context("review engine materialize failed");
        let err = wrapped.unwrap_err();
        let msg = format_operator_error(&err);
        assert!(
            msg.contains("review engine materialize failed"),
            "outer context lost: {msg}"
        );
        assert!(
            msg.contains("config not found at worktree root"),
            "inner cause swallowed by formatter — chain not surfaced: {msg}"
        );
    }
}
