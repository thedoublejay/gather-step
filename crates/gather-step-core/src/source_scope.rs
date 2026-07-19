use std::path::Path;

/// Source provenance inferred conservatively from a repository-relative path.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum SourceScope {
    Production,
    Test,
    Fixture,
    Generated,
    #[default]
    Unknown,
}

impl SourceScope {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Production => "production",
            Self::Test => "test",
            Self::Fixture => "fixture",
            Self::Generated => "generated",
            Self::Unknown => "unknown",
        }
    }

    #[must_use]
    pub const fn contributes_runtime_dependency(self) -> bool {
        matches!(self, Self::Production | Self::Unknown)
    }
}

#[must_use]
pub fn classify_source_scope(path: &str) -> SourceScope {
    if path.is_empty() || path == "__virtual__" {
        return SourceScope::Unknown;
    }
    let normalized = path.replace('\\', "/").to_ascii_lowercase();
    let file_name = Path::new(&normalized)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(normalized.as_str());
    let components = normalized.split('/').collect::<Vec<_>>();

    if components.iter().any(|component| {
        matches!(
            *component,
            "generated" | "dist" | "build" | "target" | ".next" | "coverage"
        )
    }) || file_name.contains(".generated.")
    {
        return SourceScope::Generated;
    }
    if components.iter().any(|component| {
        matches!(
            *component,
            "fixture" | "fixtures" | "testdata" | "snapshots" | "__snapshots__"
        )
    }) {
        return SourceScope::Fixture;
    }
    if components
        .iter()
        .any(|component| matches!(*component, "test" | "tests" | "__tests__"))
        || (file_name.starts_with("test_") && file_name.ends_with(".py"))
        || file_name.ends_with("_test.py")
        || [
            ".test.ts",
            ".test.tsx",
            ".test.js",
            ".test.jsx",
            ".spec.ts",
            ".spec.tsx",
        ]
        .iter()
        .any(|suffix| file_name.ends_with(suffix))
        || file_name.ends_with("_test.rs")
    {
        return SourceScope::Test;
    }
    SourceScope::Production
}

#[cfg(test)]
mod tests {
    use super::{SourceScope, classify_source_scope};

    #[test]
    fn classifies_common_cross_language_source_scopes() {
        assert_eq!(
            classify_source_scope("src/service.py"),
            SourceScope::Production
        );
        assert_eq!(
            classify_source_scope("tests/test_service.py"),
            SourceScope::Test
        );
        assert_eq!(
            classify_source_scope("src/service.spec.ts"),
            SourceScope::Test
        );
        assert_eq!(
            classify_source_scope("fixtures/sample.py"),
            SourceScope::Fixture
        );
        assert_eq!(
            classify_source_scope("src/generated/client.ts"),
            SourceScope::Generated
        );
        assert_eq!(classify_source_scope(""), SourceScope::Unknown);
    }
}
