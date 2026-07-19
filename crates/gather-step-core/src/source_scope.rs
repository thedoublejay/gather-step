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
    let mut normalized = path.replace('\\', "/");
    normalized.make_ascii_lowercase();
    let file_name = Path::new(&normalized)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(normalized.as_str());
    let file_path = Path::new(file_name);
    let extension = file_path
        .extension()
        .and_then(|extension| extension.to_str());
    let file_stem = file_path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or(file_name);
    let test_marker = Path::new(file_stem)
        .extension()
        .and_then(|marker| marker.to_str());
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
        || (extension == Some("py")
            && (file_name.starts_with("test_")
                || file_stem.ends_with("_test")
                || file_stem == "conftest"))
        || (matches!(extension, Some("ts" | "tsx" | "mts" | "cts"))
            && matches!(test_marker, Some("test" | "spec")))
        || (matches!(extension, Some("js" | "jsx" | "mjs" | "cjs"))
            && matches!(test_marker, Some("test" | "spec")))
        || (matches!(extension, Some("rs" | "go")) && file_stem.ends_with("_test"))
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

    #[test]
    fn classifies_language_specific_test_file_conventions() {
        for (path, expected) in [
            ("pkg/orders/service_test.go", SourceScope::Test),
            ("pkg/orders/service.go", SourceScope::Production),
            ("src/orders/service.spec.js", SourceScope::Test),
            ("src/orders/service.spec.jsx", SourceScope::Test),
            ("src/orders/service.spec.mjs", SourceScope::Test),
            ("src/orders/service.spec.cjs", SourceScope::Test),
            ("src/orders/service.test.mjs", SourceScope::Test),
            ("src/orders/service.spec.mts", SourceScope::Test),
            ("src/orders/service.test.cts", SourceScope::Test),
            ("src/orders/service.js", SourceScope::Production),
            ("conftest.py", SourceScope::Test),
            ("app/conftest.py", SourceScope::Test),
            ("app/service.py", SourceScope::Production),
        ] {
            assert_eq!(classify_source_scope(path), expected, "path: {path}");
        }
    }
}
