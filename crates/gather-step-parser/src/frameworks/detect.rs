//! Repo-level framework detection.
//!
//! Each supported framework-specific extractor (`NestJS`, `FastAPI`, `Spring`, ...)
//! has real runtime cost per parsed file. Running every extractor against
//! every repo wastes cycles on repos that don't use that framework — and also
//! risks false positives when an extractor's heuristics happen to match
//! non-framework code. `detect_frameworks` scans the repo root once and
//! returns the set of frameworks whose extractors are worth running. The
//! orchestrator caches this per repo.

use std::{fs, path::Path};

use rustc_hash::FxHashSet;

/// A framework whose extractor pack is gated on per-repo detection.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Framework {
    NestJs,
    Mongoose,
    NextJs,
    Tailwind,
    Prisma,
    Drizzle,
    TypeOrm,
    React,
    ReactRouter,
    ReactHookForm,
    Storybook,
    Azure,
    Redux,
    Zustand,
    LaunchDarkly,
    /// Detection-only Python web API pack.
    FastApi,
    /// Always-active pack for detecting cross-package frontend hook boundary
    /// edges.  Does not require a per-repo detection predicate.
    FrontendHooks,
}

/// Inspect the repo root and return the set of frameworks whose extractor
/// packs should run for files in this repo.
///
/// Detection is intentionally conservative — it only looks at well-known
/// manifest files (`package.json`, `pyproject.toml`, and `requirements.txt`)
/// and never parses source files. A missing or malformed manifest returns an
/// empty set rather than erroring, because framework detection is a
/// performance optimisation, not a hard correctness requirement.
#[must_use]
pub fn detect_frameworks(repo_root: &Path) -> FxHashSet<Framework> {
    let mut frameworks = FxHashSet::default();
    let manifest = read_manifest_json(repo_root);
    if has_dependency_in_manifest(manifest.as_ref(), &["@nestjs/core"])
        || (has_dependency_in_manifest(manifest.as_ref(), &["@nestjs/cli"])
            && (has_any_file(repo_root, &["nest-cli.json"])
                || manifest_script_contains_value(
                    manifest.as_ref(),
                    &["nest build", "nest start"],
                )))
    {
        frameworks.insert(Framework::NestJs);
    }
    if has_dependency_in_manifest(manifest.as_ref(), &["@nestjs/mongoose", "mongoose"]) {
        frameworks.insert(Framework::Mongoose);
    }
    if has_dependency_in_manifest(manifest.as_ref(), &["next"]) {
        frameworks.insert(Framework::NextJs);
    }
    if has_dependency_in_manifest(manifest.as_ref(), &["tailwindcss"])
        || has_any_file(
            repo_root,
            &[
                "tailwind.config.js",
                "tailwind.config.ts",
                "tailwind.config.mjs",
                "tailwind.config.cjs",
            ],
        )
    {
        frameworks.insert(Framework::Tailwind);
    }
    if has_dependency_in_manifest(manifest.as_ref(), &["prisma", "@prisma/client"])
        || has_any_file(repo_root, &["prisma/schema.prisma", "schema.prisma"])
    {
        frameworks.insert(Framework::Prisma);
    }
    if has_dependency_in_manifest(manifest.as_ref(), &["drizzle-orm", "drizzle-kit"])
        || has_any_file(
            repo_root,
            &[
                "drizzle.config.ts",
                "drizzle.config.js",
                "drizzle.config.mjs",
                "drizzle.config.cjs",
            ],
        )
    {
        frameworks.insert(Framework::Drizzle);
    }
    if has_dependency_in_manifest(manifest.as_ref(), &["typeorm"]) {
        frameworks.insert(Framework::TypeOrm);
    }
    if has_dependency_in_manifest(manifest.as_ref(), &["react"]) {
        frameworks.insert(Framework::React);
    }
    if has_dependency_in_manifest(manifest.as_ref(), &["react-router", "react-router-dom"]) {
        frameworks.insert(Framework::ReactRouter);
    }
    if has_dependency_in_manifest(manifest.as_ref(), &["react-hook-form"]) {
        frameworks.insert(Framework::ReactHookForm);
    }
    if has_dependency_in_manifest(manifest.as_ref(), &["@storybook/react", "storybook"]) {
        frameworks.insert(Framework::Storybook);
    }
    if has_dependency_in_manifest(
        manifest.as_ref(),
        &[
            "@azure/service-bus",
            "@azure/web-pubsub",
            "@azure/web-pubsub-client",
        ],
    ) {
        frameworks.insert(Framework::Azure);
    }
    if has_dependency_in_manifest(
        manifest.as_ref(),
        &["redux", "@reduxjs/toolkit", "redux-saga"],
    ) {
        frameworks.insert(Framework::Redux);
    }
    if has_dependency_in_manifest(manifest.as_ref(), &["zustand"]) {
        frameworks.insert(Framework::Zustand);
    }
    if has_dependency_in_manifest(
        manifest.as_ref(),
        &[
            "launchdarkly-js-client-sdk",
            "launchdarkly-react-client-sdk",
            "launchdarkly-node-server-sdk",
            "@launchdarkly/node-server-sdk",
        ],
    ) {
        frameworks.insert(Framework::LaunchDarkly);
    }
    if has_any_python_dependency(repo_root, &["fastapi"]) {
        frameworks.insert(Framework::FastApi);
    }
    // FrontendHooks detection is always active for any repo: cross-package hook
    // imports can appear in any TypeScript/JavaScript codebase regardless of
    // which framework it uses.
    frameworks.insert(Framework::FrontendHooks);
    frameworks
}

/// Returns `true` when `repo_root/package.json` lists `@nestjs/core` in any
/// dependency section. The presence of `@nestjs/common` or `@nestjs/microservices`
/// alone is NOT sufficient: those can appear in libraries that expose types
/// without being `NestJS` applications themselves. `@nestjs/core` is the
/// runtime framework marker.
#[must_use]
pub fn is_nestjs(repo_root: &Path) -> bool {
    if has_any_dependency(repo_root, &["@nestjs/core"]) {
        return true;
    }

    if !has_any_dependency(repo_root, &["@nestjs/cli"]) {
        return false;
    }

    has_any_file(repo_root, &["nest-cli.json"])
        || manifest_script_contains(repo_root, &["nest build", "nest start"])
}

/// Returns `true` when `repo_root/package.json` lists `@nestjs/mongoose` or
/// `mongoose` in any dependency section. The presence of either indicates
/// Mongoose ODM usage, triggering the Mongoose extractor pack for schema,
/// model, and repository pattern extraction.
#[must_use]
pub fn is_mongoose(repo_root: &Path) -> bool {
    has_any_dependency(repo_root, &["@nestjs/mongoose", "mongoose"])
}

/// Returns `true` when `repo_root/package.json` lists `next` in any dependency
/// section. This gates the `Next.js` extractor pack for file-based routes,
/// route handlers, layouts, and middleware.
#[must_use]
pub fn is_nextjs(repo_root: &Path) -> bool {
    has_any_dependency(repo_root, &["next"])
}

/// Returns `true` when the repo uses Tailwind by dependency or config file.
#[must_use]
pub fn is_tailwind(repo_root: &Path) -> bool {
    has_any_dependency(repo_root, &["tailwindcss"])
        || has_any_file(
            repo_root,
            &[
                "tailwind.config.js",
                "tailwind.config.ts",
                "tailwind.config.mjs",
                "tailwind.config.cjs",
            ],
        )
}

/// Returns `true` when Prisma is present by dependency or schema file.
#[must_use]
pub fn is_prisma(repo_root: &Path) -> bool {
    has_any_dependency(repo_root, &["prisma", "@prisma/client"])
        || has_any_file(repo_root, &["prisma/schema.prisma", "schema.prisma"])
}

/// Returns `true` when Drizzle is present by dependency or config file.
#[must_use]
pub fn is_drizzle(repo_root: &Path) -> bool {
    has_any_dependency(repo_root, &["drizzle-orm", "drizzle-kit"])
        || has_any_file(
            repo_root,
            &[
                "drizzle.config.ts",
                "drizzle.config.js",
                "drizzle.config.mjs",
                "drizzle.config.cjs",
            ],
        )
}

/// Returns `true` when TypeORM is present by dependency.
#[must_use]
pub fn is_typeorm(repo_root: &Path) -> bool {
    has_any_dependency(repo_root, &["typeorm"])
}

/// Returns `true` when `repo_root/package.json` lists `react` in any
/// dependency section. This gates the React extractor pack for hooks
/// (useQuery, useMutation), service wrappers, and config-driven endpoints.
#[must_use]
pub fn is_react(repo_root: &Path) -> bool {
    has_any_dependency(repo_root, &["react"])
}

/// Returns `true` when `react-router` or `react-router-dom` is present.
#[must_use]
pub fn is_react_router(repo_root: &Path) -> bool {
    has_any_dependency(repo_root, &["react-router", "react-router-dom"])
}

/// Returns `true` when `react-hook-form` is present.
#[must_use]
pub fn is_react_hook_form(repo_root: &Path) -> bool {
    has_any_dependency(repo_root, &["react-hook-form"])
}

/// Returns `true` when `@storybook/react` or `storybook` is present.
#[must_use]
pub fn is_storybook(repo_root: &Path) -> bool {
    has_any_dependency(repo_root, &["@storybook/react", "storybook"])
}

/// Returns `true` when any Azure messaging SDK is present.
#[must_use]
pub fn is_azure(repo_root: &Path) -> bool {
    has_any_dependency(
        repo_root,
        &[
            "@azure/service-bus",
            "@azure/web-pubsub",
            "@azure/web-pubsub-client",
        ],
    )
}

/// Returns `true` when `redux` or `@reduxjs/toolkit` is present.
#[must_use]
pub fn is_redux(repo_root: &Path) -> bool {
    has_any_dependency(repo_root, &["redux", "@reduxjs/toolkit", "redux-saga"])
}

/// Returns `true` when `zustand` is present.
#[must_use]
pub fn is_zustand(repo_root: &Path) -> bool {
    has_any_dependency(repo_root, &["zustand"])
}

/// Returns `true` when `LaunchDarkly` SDK is present.
#[must_use]
pub fn is_launchdarkly(repo_root: &Path) -> bool {
    has_any_dependency(
        repo_root,
        &[
            "launchdarkly-js-client-sdk",
            "launchdarkly-react-client-sdk",
            "launchdarkly-node-server-sdk",
            "@launchdarkly/node-server-sdk",
        ],
    )
}

/// Returns `true` when `FastAPI` is present in Python dependency metadata.
#[must_use]
pub fn is_fastapi(repo_root: &Path) -> bool {
    has_any_python_dependency(repo_root, &["fastapi"])
}

/// Returns `true` when the repo has a `src/serviceConfigs` directory, which
/// indicates a proxy-gateway repo with config-driven route definitions.
#[must_use]
pub fn is_gateway_proxy(repo_root: &Path) -> bool {
    repo_root.join("src").join("serviceConfigs").is_dir()
}

fn has_any_dependency(repo_root: &Path, packages: &[&str]) -> bool {
    let Some(manifest) = read_manifest_json(repo_root) else {
        return false;
    };
    has_dependency_in_manifest(Some(&manifest), packages)
}

fn has_dependency_in_manifest(manifest: Option<&serde_json::Value>, packages: &[&str]) -> bool {
    let Some(manifest) = manifest else {
        return false;
    };
    for section in [
        "dependencies",
        "devDependencies",
        "peerDependencies",
        "optionalDependencies",
    ] {
        if let Some(deps) = manifest.get(section).and_then(|value| value.as_object()) {
            for package in packages {
                if deps.contains_key(*package) {
                    return true;
                }
            }
        }
    }
    false
}

fn read_manifest(repo_root: &Path) -> Option<String> {
    let manifest_path = repo_root.join("package.json");
    let metadata = fs::symlink_metadata(&manifest_path).ok()?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return None;
    }
    fs::read_to_string(manifest_path).ok()
}

fn read_manifest_json(repo_root: &Path) -> Option<serde_json::Value> {
    let raw = read_manifest(repo_root)?;
    serde_json::from_str::<serde_json::Value>(&raw).ok()
}

fn has_any_python_dependency(repo_root: &Path, packages: &[&str]) -> bool {
    let pyproject_match = read_pyproject(repo_root)
        .as_ref()
        .is_some_and(|manifest| has_dependency_in_pyproject(manifest, packages));
    pyproject_match || requirements_contains_dependency(repo_root, packages)
}

fn read_pyproject(repo_root: &Path) -> Option<toml::Value> {
    let pyproject_path = repo_root.join("pyproject.toml");
    let metadata = fs::symlink_metadata(&pyproject_path).ok()?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return None;
    }
    let raw = fs::read_to_string(pyproject_path).ok()?;
    toml::from_str::<toml::Value>(&raw).ok()
}

fn has_dependency_in_pyproject(manifest: &toml::Value, packages: &[&str]) -> bool {
    let project = manifest.get("project");
    let project_dependencies = project
        .and_then(|project| project.get("dependencies"))
        .and_then(toml::Value::as_array)
        .is_some_and(|dependencies| dependency_array_contains(dependencies, packages));
    if project_dependencies {
        return true;
    }

    let optional_dependencies = project
        .and_then(|project| project.get("optional-dependencies"))
        .and_then(toml::Value::as_table)
        .is_some_and(|groups| {
            groups.values().any(|dependencies| {
                dependencies
                    .as_array()
                    .is_some_and(|dependencies| dependency_array_contains(dependencies, packages))
            })
        });
    if optional_dependencies {
        return true;
    }

    manifest
        .get("tool")
        .and_then(|tool| tool.get("poetry"))
        .and_then(|poetry| poetry.get("dependencies"))
        .and_then(toml::Value::as_table)
        .is_some_and(|dependencies| {
            dependencies
                .keys()
                .any(|dependency| package_name_matches(dependency, packages))
        })
}

fn dependency_array_contains(dependencies: &[toml::Value], packages: &[&str]) -> bool {
    dependencies
        .iter()
        .filter_map(toml::Value::as_str)
        .any(|dependency| package_name_matches(dependency_name(dependency), packages))
}

fn requirements_contains_dependency(repo_root: &Path, packages: &[&str]) -> bool {
    let requirements_path = repo_root.join("requirements.txt");
    let Ok(metadata) = fs::symlink_metadata(&requirements_path) else {
        return false;
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return false;
    }
    let Ok(raw) = fs::read_to_string(requirements_path) else {
        return false;
    };
    raw.lines().any(|line| {
        let line = line.trim();
        !line.is_empty()
            && !line.starts_with('#')
            && package_name_matches(dependency_name(line), packages)
    })
}

fn dependency_name(spec: &str) -> &str {
    spec.trim()
        .split(['[', '<', '>', '=', '!', '~', ';', ' ', '\t'])
        .next()
        .unwrap_or("")
        .trim()
}

fn package_name_matches(name: &str, packages: &[&str]) -> bool {
    packages
        .iter()
        .any(|package| name.eq_ignore_ascii_case(package))
}

fn manifest_script_contains(repo_root: &Path, needles: &[&str]) -> bool {
    let Some(manifest) = read_manifest_json(repo_root) else {
        return false;
    };
    manifest_script_contains_value(Some(&manifest), needles)
}

fn manifest_script_contains_value(manifest: Option<&serde_json::Value>, needles: &[&str]) -> bool {
    let Some(manifest) = manifest else {
        return false;
    };
    let Some(scripts) = manifest
        .get("scripts")
        .and_then(serde_json::Value::as_object)
    else {
        return false;
    };
    scripts
        .values()
        .filter_map(serde_json::Value::as_str)
        .any(|script| needles.iter().any(|needle| script.contains(needle)))
}

fn has_any_file(repo_root: &Path, paths: &[&str]) -> bool {
    paths.iter().any(|path| repo_root.join(path).exists())
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::PathBuf,
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    use pretty_assertions::assert_eq;

    use super::{
        Framework, detect_frameworks, is_drizzle, is_fastapi, is_mongoose, is_nestjs, is_nextjs,
        is_prisma, is_react, is_tailwind, is_typeorm,
    };

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(name: &str) -> Self {
            let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-framework-detect-{name}-{}-{counter}",
                process::id()
            ));
            fs::create_dir_all(&path).expect("temp dir should create");
            Self { path }
        }

        fn write(&self, relative: &str, contents: &str) {
            let full = self.path.join(relative);
            if let Some(parent) = full.parent() {
                fs::create_dir_all(parent).expect("parent dir should create");
            }
            fs::write(full, contents).expect("fixture should write");
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn is_nestjs_detects_core_in_dependencies() {
        let dir = TempDir::new("nest-deps");
        dir.write(
            "package.json",
            r#"{ "name": "service-a", "dependencies": { "@nestjs/core": "^11.0.0" } }"#,
        );
        assert!(is_nestjs(&dir.path));
    }

    #[test]
    fn is_nestjs_detects_core_in_dev_dependencies() {
        let dir = TempDir::new("nest-devdeps");
        dir.write(
            "package.json",
            r#"{ "name": "sample", "devDependencies": { "@nestjs/core": "^11.0.0" } }"#,
        );
        assert!(is_nestjs(&dir.path));
    }

    #[test]
    fn is_nestjs_returns_false_when_only_common_is_present() {
        // A type-only library may depend on @nestjs/common without being a
        // NestJS application; @nestjs/core is the runtime marker.
        let dir = TempDir::new("nest-common-only");
        dir.write(
            "package.json",
            r#"{ "name": "types-lib", "dependencies": { "@nestjs/common": "^11.0.0" } }"#,
        );
        assert!(!is_nestjs(&dir.path));
    }

    #[test]
    fn is_nestjs_detects_cli_plus_nest_scripts() {
        let dir = TempDir::new("nest-cli-scripts");
        dir.write(
            "package.json",
            r#"
{
  "name": "service-a",
  "devDependencies": { "@nestjs/cli": "^11.0.0" },
  "scripts": {
    "build": "nest build",
    "start": "nest start"
  }
}
"#,
        );
        assert!(is_nestjs(&dir.path));
    }

    #[test]
    fn is_nestjs_detects_cli_plus_nest_cli_json() {
        let dir = TempDir::new("nest-cli-json");
        dir.write(
            "package.json",
            r#"{ "name": "service-a", "devDependencies": { "@nestjs/cli": "^11.0.0" } }"#,
        );
        dir.write(
            "nest-cli.json",
            "{ \"collection\": \"@nestjs/schematics\" }\n",
        );
        assert!(is_nestjs(&dir.path));
    }

    #[test]
    fn is_nestjs_returns_false_for_cli_without_nest_app_markers() {
        let dir = TempDir::new("nest-cli-only");
        dir.write(
            "package.json",
            r#"{ "name": "tooling-lib", "devDependencies": { "@nestjs/cli": "^11.0.0" } }"#,
        );
        assert!(!is_nestjs(&dir.path));
    }

    #[test]
    fn is_nestjs_returns_false_without_package_json() {
        let dir = TempDir::new("no-manifest");
        assert!(!is_nestjs(&dir.path));
    }

    #[test]
    fn is_nestjs_returns_false_on_malformed_package_json() {
        let dir = TempDir::new("malformed");
        dir.write("package.json", "{ this is not valid json");
        assert!(!is_nestjs(&dir.path));
    }

    #[test]
    fn detect_frameworks_returns_nestjs_set_when_detected() {
        let dir = TempDir::new("detect-nest");
        dir.write(
            "package.json",
            r#"{ "dependencies": { "@nestjs/core": "^11.0.0" } }"#,
        );
        let detected = detect_frameworks(&dir.path);
        assert!(detected.contains(&Framework::NestJs));
        assert!(detected.contains(&Framework::FrontendHooks));
        // FrontendHooks is always-on, so total is NestJs + FrontendHooks.
        assert_eq!(detected.len(), 2);
    }

    #[test]
    fn detect_frameworks_returns_only_frontend_hooks_for_plain_repo() {
        // A plain repo with no recognised framework still gets FrontendHooks.
        let dir = TempDir::new("detect-plain");
        dir.write(
            "package.json",
            r#"{ "dependencies": { "express": "^4.0.0" } }"#,
        );
        let detected = detect_frameworks(&dir.path);
        assert!(detected.contains(&Framework::FrontendHooks));
        assert_eq!(detected.len(), 1);
    }

    #[test]
    #[cfg(unix)]
    fn detect_frameworks_ignores_symlinked_package_json() {
        let dir = TempDir::new("detect-symlink-manifest");
        dir.write(
            "external.json",
            r#"{ "dependencies": { "@nestjs/core": "^11.0.0" } }"#,
        );
        symlink(
            dir.path.join("external.json"),
            dir.path.join("package.json"),
        )
        .expect("manifest symlink");

        assert!(!is_nestjs(&dir.path));
        // FrontendHooks is always-on, so the set is not empty even when NestJS
        // detection correctly rejects the symlinked manifest.
        let detected = detect_frameworks(&dir.path);
        assert!(!detected.contains(&Framework::NestJs));
        assert!(detected.contains(&Framework::FrontendHooks));
    }

    #[test]
    fn is_mongoose_detects_nestjs_mongoose_package() {
        let dir = TempDir::new("mongoose-nestjs");
        dir.write(
            "package.json",
            r#"{ "dependencies": { "@nestjs/mongoose": "^10.0.0" } }"#,
        );
        assert!(is_mongoose(&dir.path));
    }

    #[test]
    fn is_mongoose_detects_standalone_mongoose_package() {
        let dir = TempDir::new("mongoose-standalone");
        dir.write(
            "package.json",
            r#"{ "dependencies": { "mongoose": "^8.0.0" } }"#,
        );
        assert!(is_mongoose(&dir.path));
    }

    #[test]
    fn is_mongoose_returns_false_without_mongoose() {
        let dir = TempDir::new("no-mongoose");
        dir.write(
            "package.json",
            r#"{ "dependencies": { "@nestjs/core": "^11.0.0" } }"#,
        );
        assert!(!is_mongoose(&dir.path));
    }

    #[test]
    fn is_react_detects_react_in_dependencies() {
        let dir = TempDir::new("react-deps");
        dir.write(
            "package.json",
            r#"{ "dependencies": { "react": "^19.0.0" } }"#,
        );
        assert!(is_react(&dir.path));
    }

    #[test]
    fn is_react_returns_false_without_react() {
        let dir = TempDir::new("no-react");
        dir.write("package.json", r#"{ "dependencies": { "vue": "^3.0.0" } }"#);
        assert!(!is_react(&dir.path));
    }

    #[test]
    fn nextjs_tailwind_prisma_drizzle_and_typeorm_are_detected() {
        let dir = TempDir::new("modern-web");
        dir.write(
            "package.json",
            r#"
{
  "dependencies": {
    "next": "^15.0.0",
    "tailwindcss": "^4.0.0",
    "@prisma/client": "^6.0.0",
    "drizzle-orm": "^0.40.0",
    "typeorm": "^0.3.24"
  },
  "devDependencies": {
    "prisma": "^6.0.0",
    "drizzle-kit": "^0.30.0"
  }
}
"#,
        );

        assert!(is_nextjs(&dir.path));
        assert!(is_tailwind(&dir.path));
        assert!(is_prisma(&dir.path));
        assert!(is_drizzle(&dir.path));
        assert!(is_typeorm(&dir.path));

        let detected = detect_frameworks(&dir.path);
        assert!(detected.contains(&Framework::NextJs));
        assert!(detected.contains(&Framework::Tailwind));
        assert!(detected.contains(&Framework::Prisma));
        assert!(detected.contains(&Framework::Drizzle));
        assert!(detected.contains(&Framework::TypeOrm));
    }

    #[test]
    fn fastapi_is_detected_from_python_dependency_metadata() {
        let pyproject_dir = TempDir::new("fastapi-pyproject");
        pyproject_dir.write(
            "pyproject.toml",
            r#"
[project]
dependencies = [
  "fastapi>=0.115",
]
"#,
        );
        assert!(is_fastapi(&pyproject_dir.path));

        let requirements_dir = TempDir::new("fastapi-requirements");
        requirements_dir.write("requirements.txt", "fastapi==0.115.0\nuvicorn\n");
        assert!(is_fastapi(&requirements_dir.path));

        let detected = detect_frameworks(&pyproject_dir.path);
        assert!(detected.contains(&Framework::FastApi));
        assert!(detected.contains(&Framework::FrontendHooks));
    }

    #[test]
    fn config_files_can_trigger_tailwind_prisma_and_drizzle_detection() {
        let dir = TempDir::new("config-detect");
        dir.write("package.json", r#"{ "dependencies": {} }"#);
        dir.write("tailwind.config.ts", "export default {};\n");
        dir.write("prisma/schema.prisma", "model User { id Int @id }\n");
        dir.write("drizzle.config.ts", "export default {};\n");

        assert!(is_tailwind(&dir.path));
        assert!(is_prisma(&dir.path));
        assert!(is_drizzle(&dir.path));
    }

    #[test]
    fn detect_frameworks_returns_multiple_when_applicable() {
        let dir = TempDir::new("detect-multi");
        dir.write(
            "package.json",
            r#"{ "dependencies": { "@nestjs/core": "^11.0.0", "@nestjs/mongoose": "^10.0.0" } }"#,
        );
        let detected = detect_frameworks(&dir.path);
        assert!(detected.contains(&Framework::NestJs));
        assert!(detected.contains(&Framework::Mongoose));
        assert!(detected.contains(&Framework::FrontendHooks));
        // NestJs + Mongoose + FrontendHooks (always-on).
        assert_eq!(detected.len(), 3);
    }
}
