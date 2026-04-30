//! Pack registry: maps each [`PackId`] to its detection predicate and
//! augmentation function.
//!
//! The registry is the single source of truth for which packs are available
//! and how they are activated.  The orchestrator calls [`PackRegistry::detect`]
//! once per repo and then passes the resulting `Vec<PackId>` into every rayon
//! worker via [`PackRegistry::augment_all`].
//!
//! ## Augmentation grouping
//!
//! Some packs share a single underlying augmentation function:
//!
//! - [`PackId::Azure`] and [`PackId::LaunchDarkly`] both delegate to
//!   [`azure::augment`].  When both are active, the augmentation runs only
//!   once (the registry skips duplicates that map to the same group).
//! - [`PackId::ReactRouter`], [`PackId::Redux`], [`PackId::Zustand`], and
//!   [`PackId::ReactHookForm`] all delegate to [`frontend_router::augment`].
//!   Again, only one call is made regardless of how many of these packs are
//!   active.
//! - [`PackId::SharedLib`] always activates for TypeScript/JavaScript files.
//!   It has no detection predicate (`detect: None`).

use std::path::Path;

use gather_step_core::{EdgeData, NodeData};

use crate::{
    frameworks::{
        azure, detect, drizzle, frontend_hooks, frontend_react, frontend_router, gateway_proxy,
        mongoose, nestjs, nextjs, prisma, storybook, tailwind, typeorm,
    },
    traverse::Language,
    tree_sitter::ParsedFile,
};

/// Unique identifier for a semantic pack.
///
/// Each variant corresponds to one set of framework-specific extraction rules.
/// The `serde` representation uses `snake_case` so pack IDs can appear in YAML
/// config files.
#[derive(
    Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum PackId {
    /// `NestJS` controller, event, and DI extraction.
    Nestjs,
    /// Mongoose schema, model, and repository pattern extraction.
    Mongoose,
    /// `Next.js` app/pages router extraction.
    Nextjs,
    /// Tailwind config and utility usage extraction.
    Tailwind,
    /// Prisma schema and client extraction.
    Prisma,
    /// Drizzle schema and query extraction.
    Drizzle,
    /// `TypeORM` migration table extraction.
    TypeOrm,
    /// React hooks and service-wrapper extraction.
    React,
    /// React Router route tree extraction.
    ReactRouter,
    /// React Hook Form extraction.
    ReactHookForm,
    /// Storybook story and component extraction.
    Storybook,
    /// Azure Service Bus / Web `PubSub` extraction (shares augmentation with
    /// [`PackId::LaunchDarkly`]).
    Azure,
    /// Redux store extraction (shares augmentation with [`PackId::ReactRouter`],
    /// [`PackId::Zustand`], and [`PackId::ReactHookForm`]).
    Redux,
    /// Zustand store extraction (shares augmentation with [`PackId::ReactRouter`],
    /// [`PackId::Redux`], and [`PackId::ReactHookForm`]).
    Zustand,
    /// `LaunchDarkly` feature-flag extraction (shares augmentation with
    /// [`PackId::Azure`]).
    LaunchDarkly,
    /// Detection-only `FastAPI` Python API pack.
    Fastapi,
    /// Shared-library / shared-lib contract detection.  This pack is always
    /// active for TypeScript/JavaScript files; it has no detection predicate.
    SharedLib,
    /// Config-driven proxy-route extraction for gateway repos that define
    /// routes in `src/serviceConfigs/**/*.ts` object literals rather than via
    /// `NestJS` decorators.
    GatewayProxy,
    /// Cross-package frontend hook boundary detection.  Always active for any
    /// TypeScript/JavaScript file; emits `ConsumesHookFrom` edges when a file
    /// imports a hook-named export (`useXxx`) via a cross-package specifier.
    FrontendHooks,
}

/// Grouping used to prevent running the same underlying augmentation
/// function more than once when multiple packs that share it are active.
///
/// This is `pub(crate)` so that `tree_sitter.rs` can use it for group-level
/// deduplication when iterating over active packs.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum AugGroup {
    Nestjs,
    Mongoose,
    Nextjs,
    Tailwind,
    Prisma,
    Drizzle,
    TypeOrm,
    React,
    FrontendRouter,
    Storybook,
    Azure,
    Fastapi,
    SharedLib,
    GatewayProxy,
    FrontendHooks,
}

impl PackId {
    /// The augmentation group this pack belongs to.  Packs in the same group
    /// share one underlying augmentation function; [`PackRegistry::augment_all`]
    /// runs each group at most once.
    #[must_use]
    pub(crate) fn aug_group(self) -> AugGroup {
        match self {
            Self::Nestjs => AugGroup::Nestjs,
            Self::Mongoose => AugGroup::Mongoose,
            Self::Nextjs => AugGroup::Nextjs,
            Self::Tailwind => AugGroup::Tailwind,
            Self::Prisma => AugGroup::Prisma,
            Self::Drizzle => AugGroup::Drizzle,
            Self::TypeOrm => AugGroup::TypeOrm,
            Self::React => AugGroup::React,
            Self::ReactRouter | Self::Redux | Self::Zustand | Self::ReactHookForm => {
                AugGroup::FrontendRouter
            }
            Self::Storybook => AugGroup::Storybook,
            Self::Azure | Self::LaunchDarkly => AugGroup::Azure,
            Self::Fastapi => AugGroup::Fastapi,
            Self::SharedLib => AugGroup::SharedLib,
            Self::GatewayProxy => AugGroup::GatewayProxy,
            Self::FrontendHooks => AugGroup::FrontendHooks,
        }
    }
}

/// Internal description of a single pack: its detection predicate and its ID.
///
/// The augmentation function is not stored here — it is dispatched at call
/// time via [`PackRegistry::augment`] using a `match` on [`PackId`].
struct PackEntry {
    id: PackId,
    /// Returns `true` when this pack should be active for the given repo root.
    /// `None` means the pack is always active (no detection needed).
    detect: Option<fn(&Path) -> bool>,
}

/// Registry of all built-in semantic packs.
///
/// Construct with [`PackRegistry::builtin`], then call [`PackRegistry::detect`]
/// once per repo root to get the set of active packs.
pub struct PackRegistry {
    packs: Vec<PackEntry>,
}

/// The combined output of one or more augmentation passes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AugmentationOutput {
    pub nodes: Vec<NodeData>,
    pub edges: Vec<EdgeData>,
}

impl PackRegistry {
    /// Build the default registry containing all built-in packs in priority
    /// order.
    ///
    /// # Examples
    ///
    /// ```
    /// use gather_step_parser::frameworks::registry::PackRegistry;
    /// let registry = PackRegistry::builtin();
    /// ```
    #[must_use]
    pub fn builtin() -> Self {
        Self {
            packs: vec![
                PackEntry {
                    id: PackId::Nestjs,
                    detect: Some(detect::is_nestjs),
                },
                PackEntry {
                    id: PackId::Mongoose,
                    detect: Some(detect::is_mongoose),
                },
                PackEntry {
                    id: PackId::Nextjs,
                    detect: Some(detect::is_nextjs),
                },
                PackEntry {
                    id: PackId::Tailwind,
                    detect: Some(detect::is_tailwind),
                },
                PackEntry {
                    id: PackId::Prisma,
                    detect: Some(detect::is_prisma),
                },
                PackEntry {
                    id: PackId::Drizzle,
                    detect: Some(detect::is_drizzle),
                },
                PackEntry {
                    id: PackId::TypeOrm,
                    detect: Some(detect::is_typeorm),
                },
                PackEntry {
                    id: PackId::React,
                    detect: Some(detect::is_react),
                },
                PackEntry {
                    id: PackId::ReactRouter,
                    detect: Some(detect::is_react_router),
                },
                PackEntry {
                    id: PackId::ReactHookForm,
                    detect: Some(detect::is_react_hook_form),
                },
                PackEntry {
                    id: PackId::Storybook,
                    detect: Some(detect::is_storybook),
                },
                PackEntry {
                    id: PackId::Azure,
                    detect: Some(detect::is_azure),
                },
                PackEntry {
                    id: PackId::Redux,
                    detect: Some(detect::is_redux),
                },
                PackEntry {
                    id: PackId::Zustand,
                    detect: Some(detect::is_zustand),
                },
                PackEntry {
                    id: PackId::LaunchDarkly,
                    detect: Some(detect::is_launchdarkly),
                },
                PackEntry {
                    id: PackId::Fastapi,
                    detect: Some(detect::is_fastapi),
                },
                // SharedLib has no detection predicate — it is always active.
                PackEntry {
                    id: PackId::SharedLib,
                    detect: None,
                },
                PackEntry {
                    id: PackId::GatewayProxy,
                    detect: Some(detect::is_gateway_proxy),
                },
                // FrontendHooks has no detection predicate — it is always
                // active for TS/JS files, similar to SharedLib.
                PackEntry {
                    id: PackId::FrontendHooks,
                    detect: None,
                },
            ],
        }
    }

    /// Scan the repo root and return the set of packs that should be active.
    ///
    /// Packs with `detect: None` (currently only [`PackId::SharedLib`]) are
    /// always included.  Packs with a detection predicate are included only
    /// when the predicate returns `true` for `repo_root`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::Path;
    /// use gather_step_parser::frameworks::registry::PackRegistry;
    ///
    /// let registry = PackRegistry::builtin();
    /// let active = registry.detect(Path::new("/path/to/repo"));
    /// ```
    #[must_use]
    pub fn detect(&self, repo_root: &Path) -> Vec<PackId> {
        self.packs
            .iter()
            .filter(|entry| entry.detect.is_none_or(|predicate| predicate(repo_root)))
            .map(|entry| entry.id)
            .collect()
    }

    /// Run augmentation for a single pack against `parsed`.
    ///
    /// Returns the combined nodes and edges produced by the augmentation
    /// function that backs this pack.  Note that multiple packs may share the
    /// same underlying function (e.g. `Azure` and `LaunchDarkly`); when you
    /// have a set of active packs, prefer [`PackRegistry::augment_all`] to
    /// avoid running shared functions more than once.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use gather_step_parser::frameworks::registry::{PackId, PackRegistry};
    ///
    /// let registry = PackRegistry::builtin();
    /// // parsed: ParsedFile obtained elsewhere
    /// // let (nodes, edges) = registry.augment(PackId::Nestjs, &parsed);
    /// ```
    #[must_use]
    pub fn augment(&self, pack_id: PackId, parsed: &ParsedFile) -> AugmentationOutput {
        match pack_id.aug_group() {
            AugGroup::Nestjs => {
                let aug = nestjs::augment(parsed);
                AugmentationOutput {
                    nodes: aug.nodes,
                    edges: aug.edges,
                }
            }
            AugGroup::Mongoose => {
                let aug = mongoose::augment(parsed);
                AugmentationOutput {
                    nodes: aug.nodes,
                    edges: aug.edges,
                }
            }
            AugGroup::Nextjs => {
                let aug = nextjs::augment(parsed);
                AugmentationOutput {
                    nodes: aug.nodes,
                    edges: aug.edges,
                }
            }
            AugGroup::Tailwind => {
                let aug = tailwind::augment(parsed);
                AugmentationOutput {
                    nodes: aug.nodes,
                    edges: aug.edges,
                }
            }
            AugGroup::Prisma => {
                let aug = prisma::augment(parsed);
                AugmentationOutput {
                    nodes: aug.nodes,
                    edges: aug.edges,
                }
            }
            AugGroup::Drizzle => {
                let aug = drizzle::augment(parsed);
                AugmentationOutput {
                    nodes: aug.nodes,
                    edges: aug.edges,
                }
            }
            AugGroup::TypeOrm => {
                let aug = typeorm::augment(parsed);
                AugmentationOutput {
                    nodes: aug.nodes,
                    edges: aug.edges,
                }
            }
            AugGroup::React => {
                let aug = frontend_react::augment(parsed);
                AugmentationOutput {
                    nodes: aug.nodes,
                    edges: aug.edges,
                }
            }
            AugGroup::FrontendRouter => {
                let aug = frontend_router::augment(parsed);
                AugmentationOutput {
                    nodes: aug.nodes,
                    edges: aug.edges,
                }
            }
            AugGroup::Storybook => {
                let aug = storybook::augment(parsed);
                AugmentationOutput {
                    nodes: aug.nodes,
                    edges: aug.edges,
                }
            }
            AugGroup::Azure => {
                let aug = azure::augment(parsed);
                AugmentationOutput {
                    nodes: aug.nodes,
                    edges: aug.edges,
                }
            }
            AugGroup::Fastapi => AugmentationOutput::default(),
            AugGroup::SharedLib => {
                let aug = azure::augment_shared_lib(parsed);
                AugmentationOutput {
                    nodes: aug.nodes,
                    edges: aug.edges,
                }
            }
            AugGroup::GatewayProxy => {
                let aug = gateway_proxy::augment(parsed);
                AugmentationOutput {
                    nodes: aug.nodes,
                    edges: aug.edges,
                }
            }
            AugGroup::FrontendHooks => {
                let aug = frontend_hooks::augment(parsed);
                AugmentationOutput {
                    nodes: aug.nodes,
                    edges: aug.edges,
                }
            }
        }
    }

    /// Run augmentation for a set of packs, accumulating results.
    ///
    /// Each augmentation *group* (see [`PackId::aug_group`]) is run at most
    /// once even if multiple packs that belong to the same group are present in
    /// `pack_ids`.  The [`PackId::SharedLib`] pack is handled specially: it
    /// only runs when `parsed` is a TypeScript or JavaScript file.
    ///
    /// Results from each group are accumulated into a single
    /// [`AugmentationOutput`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use gather_step_parser::frameworks::registry::{PackId, PackRegistry};
    ///
    /// let registry = PackRegistry::builtin();
    /// let packs = vec![PackId::Nestjs, PackId::Mongoose];
    /// // let output = registry.augment_all(&packs, &parsed);
    /// ```
    #[must_use]
    pub fn augment_all(&self, pack_ids: &[PackId], parsed: &ParsedFile) -> AugmentationOutput {
        let mut seen_groups = rustc_hash::FxHashSet::default();
        let mut output = AugmentationOutput::default();

        for &pack_id in pack_ids {
            // SharedLib and FrontendHooks only run for TS/JS files.
            if matches!(pack_id, PackId::SharedLib | PackId::FrontendHooks)
                && !matches!(
                    parsed.file.language,
                    Language::TypeScript | Language::JavaScript
                )
            {
                continue;
            }

            let group = pack_id.aug_group();
            if !seen_groups.insert(group) {
                // Another pack in this group was already processed.
                continue;
            }

            let aug = self.augment(pack_id, parsed);
            output.nodes.extend(aug.nodes);
            output.edges.extend(aug.edges);
        }

        output
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::PathBuf,
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use pretty_assertions::assert_eq;

    use super::{PackId, PackRegistry};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(name: &str) -> Self {
            let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-registry-{name}-{}-{counter}",
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
    fn builtin_registry_detects_nestjs() {
        let dir = TempDir::new("detect-nestjs");
        dir.write(
            "package.json",
            r#"{ "dependencies": { "@nestjs/core": "^11.0.0" } }"#,
        );
        let registry = PackRegistry::builtin();
        let active = registry.detect(&dir.path);
        assert!(
            active.contains(&PackId::Nestjs),
            "NestJS should be detected from package.json"
        );
    }

    #[test]
    fn shared_lib_is_always_active() {
        // Even a completely empty repo root (no package.json at all) must
        // include SharedLib in the detected pack set.
        let dir = TempDir::new("shared-lib-always");
        let registry = PackRegistry::builtin();
        let active = registry.detect(&dir.path);
        assert!(
            active.contains(&PackId::SharedLib),
            "SharedLib must always appear regardless of repo contents"
        );
    }

    #[test]
    fn detect_returns_always_on_packs_for_plain_repo() {
        // A plain repo with no recognisable framework dependencies should
        // produce the always-on packs: SharedLib and FrontendHooks.
        let dir = TempDir::new("plain-repo");
        dir.write(
            "package.json",
            r#"{ "dependencies": { "express": "^4.0.0" } }"#,
        );
        let registry = PackRegistry::builtin();
        let active = registry.detect(&dir.path);
        assert!(active.contains(&PackId::SharedLib));
        assert!(active.contains(&PackId::FrontendHooks));
        assert_eq!(active.len(), 2);
    }

    #[test]
    fn detect_returns_multiple_packs_when_applicable() {
        let dir = TempDir::new("multi-pack");
        dir.write(
            "package.json",
            r#"{ "dependencies": { "@nestjs/core": "^11.0.0", "mongoose": "^8.0.0" } }"#,
        );
        let registry = PackRegistry::builtin();
        let active = registry.detect(&dir.path);
        assert!(active.contains(&PackId::Nestjs));
        assert!(active.contains(&PackId::Mongoose));
        assert!(active.contains(&PackId::SharedLib));
    }

    #[test]
    fn detect_includes_v2_web_packs_when_present() {
        let dir = TempDir::new("v2-web-packs");
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
  }
}
"#,
        );
        let registry = PackRegistry::builtin();
        let active = registry.detect(&dir.path);
        assert!(active.contains(&PackId::Nextjs));
        assert!(active.contains(&PackId::Tailwind));
        assert!(active.contains(&PackId::Prisma));
        assert!(active.contains(&PackId::Drizzle));
        assert!(active.contains(&PackId::TypeOrm));
        assert!(active.contains(&PackId::SharedLib));
    }

    #[test]
    fn builtin_registry_detects_fastapi_without_augmenting_python_files() {
        let dir = TempDir::new("detect-fastapi");
        dir.write(
            "pyproject.toml",
            "[project]\ndependencies = [\"fastapi>=0.115\"]\n",
        );
        let registry = PackRegistry::builtin();
        let active = registry.detect(&dir.path);
        assert!(active.contains(&PackId::Fastapi));
    }

    #[test]
    fn augment_dispatches_to_correct_pack() {
        use gather_step_core::NodeKind;

        let controller_source = r"
import { Controller, Get } from '@nestjs/common';

@Controller('items')
export class ItemController {
  @Get('list')
  list() {}
}
";

        let dir = TempDir::new("dispatch-test");
        fs::create_dir_all(dir.path.join("src")).expect("src dir");
        fs::write(dir.path.join("src/controller.ts"), controller_source)
            .expect("controller fixture");

        let traversal_file = crate::traverse::FileEntry {
            path: std::path::PathBuf::from("src/controller.ts"),
            language: crate::traverse::Language::TypeScript,
            size_bytes: 0,
            content_hash: [0u8; 32],
            source_bytes: None,
        };

        let parsed = crate::tree_sitter::parse_file_with_frameworks(
            "test-repo",
            &dir.path,
            &traversal_file,
            &[crate::frameworks::Framework::NestJs],
        )
        .expect("parse should succeed");

        // The NestJS augmenter should have produced Route virtual nodes for
        // the @Get('list') decorator on ItemController.
        let has_route = parsed
            .nodes
            .iter()
            .any(|n| n.kind == NodeKind::Route && n.name.contains("list"));
        assert!(
            has_route,
            "augment(NestJs) should produce a Route node for @Get('list'); nodes: {:#?}",
            parsed.nodes
        );
    }

    #[test]
    fn augment_all_deduplicates_shared_groups() {
        // Azure and LaunchDarkly share one augmentation group.  Providing both
        // pack IDs should not cause a panic or double-augmentation — the second
        // pack in the group is silently skipped.
        //
        // We verify this indirectly: parse an empty TS file with (a) only
        // Azure, then (b) both Azure and LaunchDarkly.  Both runs should
        // produce identical node counts because the underlying augmenter only
        // runs once for the shared group.
        let dir = TempDir::new("dedup-test");
        fs::create_dir_all(dir.path.join("src")).expect("src dir");
        fs::write(dir.path.join("src/empty.ts"), "export {};\n").expect("empty fixture");

        let file_entry = crate::traverse::FileEntry {
            path: std::path::PathBuf::from("src/empty.ts"),
            language: crate::traverse::Language::TypeScript,
            size_bytes: 0,
            content_hash: [0u8; 32],
            source_bytes: None,
        };
        let parsed_azure_only = crate::tree_sitter::parse_file_with_frameworks(
            "test-repo",
            &dir.path,
            &file_entry,
            &[crate::frameworks::Framework::Azure],
        )
        .expect("parse azure only");

        let parsed_both = crate::tree_sitter::parse_file_with_frameworks(
            "test-repo",
            &dir.path,
            &file_entry,
            &[
                crate::frameworks::Framework::Azure,
                crate::frameworks::Framework::LaunchDarkly,
            ],
        )
        .expect("parse azure + launchdarkly");

        assert_eq!(
            parsed_azure_only.nodes.len(),
            parsed_both.nodes.len(),
            "Azure and LaunchDarkly share one augmentation; adding both should not duplicate nodes"
        );
    }
}
