use std::io::ErrorKind;

use anyhow::Error;
use gather_step_core::ConfigError;
use gather_step_storage::{GraphStoreError, MetadataStoreError, SearchStoreError};

const UNSUPPORTED_SCHEMA_MESSAGE: &str = "your local index uses an unsupported schema; run `gather-step clean && gather-step index` to rebuild";

#[must_use]
pub fn format_operator_error(error: &Error) -> String {
    let full = error_chain_text(error);

    for cause in error.chain() {
        if let Some(config_error) = cause.downcast_ref::<ConfigError>() {
            return format_config_error(config_error);
        }
        if cause
            .downcast_ref::<MetadataStoreError>()
            .is_some_and(|err| matches!(err, MetadataStoreError::SchemaVersionMismatch { .. }))
            || cause
                .downcast_ref::<SearchStoreError>()
                .is_some_and(|err| matches!(err, SearchStoreError::VersionMismatch { .. }))
        {
            return UNSUPPORTED_SCHEMA_MESSAGE.to_owned();
        }
        if let Some(graph_error) = cause.downcast_ref::<GraphStoreError>() {
            match graph_error {
                GraphStoreError::StorageHeld { .. }
                | GraphStoreError::StorageHeldByDaemon { .. } => {
                    return "another gather-step process is using this workspace; stop `gather-step watch` or `gather-step serve --watch`, then retry".to_owned();
                }
                GraphStoreError::Corrupt { .. } | GraphStoreError::SchemaVersionMismatch { .. } => {
                    return "your index is corrupt or incomplete; run `gather-step index --auto-recover` to rebuild generated state, or run `gather-step clean && gather-step index`".to_owned();
                }
                _ => {}
            }
        }
    }

    if contains_ascii_case_insensitive(&full, "workspace is not a git repository") {
        return "workspace is not a git repository. Next step: run from a git checkout or omit `--release-gate` for an unsealed run".to_owned();
    }
    if contains_ascii_case_insensitive(&full, ".gather-step")
        && contains_ascii_case_insensitive(&full, "permission denied")
    {
        return "cannot write `.gather-step` generated state. Next step: fix permissions on `.gather-step` or pass writable `--storage`/`--registry` paths".to_owned();
    }
    if contains_ascii_case_insensitive(&full, "database already open")
        || contains_ascii_case_insensitive(&full, "already locked by another gather-step process")
        || contains_ascii_case_insensitive(&full, "locked by gather-step pid")
    {
        return "another gather-step process is using this workspace; stop `gather-step watch` or `gather-step serve --watch`, then retry".to_owned();
    }
    if contains_ascii_case_insensitive(&full, "db corrupted")
        || contains_ascii_case_insensitive(&full, "corrupt")
        || contains_ascii_case_insensitive(&full, "repair aborted")
        || contains_ascii_case_insensitive(&full, "manual upgrade required")
    {
        return "your index is corrupt or incomplete; run `gather-step index --auto-recover` to rebuild generated state, or run `gather-step clean && gather-step index`".to_owned();
    }

    one_line(error.to_string())
}

fn format_config_error(error: &ConfigError) -> String {
    match error {
        ConfigError::Read { path, source } if source.kind() == ErrorKind::NotFound => {
            format!(
                "config not found: {path}. Next step: run `gather-step init` or pass `--config <path>`"
            )
        }
        ConfigError::Read { path, source } if source.kind() == ErrorKind::PermissionDenied => {
            format!(
                "cannot read config: {path}. Next step: fix file permissions or pass `--config <path>`"
            )
        }
        ConfigError::Read { path, .. } => {
            format!("cannot read config: {path}. Next step: fix the path or pass `--config <path>`")
        }
        ConfigError::Parse { path, .. } => {
            format!("config YAML is malformed: {path}. Next step: fix the YAML syntax and rerun")
        }
        ConfigError::Validation { reason, .. } if reason.contains("path does not exist") => {
            format!(
                "configured repo path does not exist: {reason}. Next step: create the repo directory or fix the repo path in the config"
            )
        }
        ConfigError::Validation { reason, .. } => {
            format!("config is invalid: {reason}. Next step: fix the config and rerun")
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
    use gather_step_storage::GraphStoreError;

    use super::format_operator_error;

    #[test]
    fn graph_schema_mismatch_reports_auto_recover() {
        let error = anyhow::Error::new(GraphStoreError::SchemaVersionMismatch {
            stored: 0,
            expected: 1,
        })
        .context("opening storage at /tmp/workspace/.gather-step/storage");

        let message = format_operator_error(&error);

        assert!(message.contains("index is corrupt or incomplete"));
        assert!(message.contains("gather-step index --auto-recover"));
    }
}
