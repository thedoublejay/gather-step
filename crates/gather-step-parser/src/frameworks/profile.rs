//! Profile system: named bundles of [`PackId`]s that can be applied to a repo.
//!
//! A profile declares which packs it activates and can optionally extend one
//! or more other profiles.  [`resolve_profile`] flattens the `extends` chain
//! and returns a deduplicated, sorted list of resolved packs with any per-pack
//! options preserved.
//!
//! # Example YAML
//!
//! ```yaml
//! profiles:
//!   - name: backend_base
//!     packs:
//!       - nestjs
//!       - mongoose
//!       - shared_lib
//!
//!   - name: backend_standard
//!     extends:
//!       - backend_base
//!     packs:
//!       - azure
//! ```

use std::collections::{BTreeMap, BTreeSet};

use super::registry::PackId;

/// A named bundle of packs that can be applied to a repo.
///
/// Profiles are serialised as YAML objects inside
/// `.gather-step.local.yaml`.  The `extends` field lists other profile
/// names whose packs should be merged in before the local `packs` list
/// is applied.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Profile {
    /// Unique name for this profile, e.g. `"backend_standard"`.
    pub name: String,
    /// Names of profiles whose packs should be inherited.  Resolved
    /// recursively; cycles are silently ignored (a profile already in the
    /// visited set is skipped).
    #[serde(default)]
    pub extends: Vec<String>,
    /// Packs directly declared by this profile.
    #[serde(default)]
    pub packs: Vec<PackRef>,
}

/// Reference to a pack within a profile, optionally carrying per-pack options.
///
/// In YAML the simple form is just a bare pack ID string:
///
/// ```yaml
/// packs:
///   - nestjs
///   - mongoose
/// ```
///
/// The extended form with options is an object:
///
/// ```yaml
/// packs:
///   - id: nestjs
///     options:
///       strict: true
/// ```
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum PackRef {
    /// Simple reference: just the pack ID string.
    Simple(PackId),
    /// Reference with additional options.
    WithOptions {
        /// The pack to activate.
        id: PackId,
        /// Arbitrary per-pack options.  Currently unused by built-in packs
        /// but preserved for forward compatibility.
        #[serde(default)]
        options: serde_norway::Value,
    },
}

/// Fully-resolved pack activation, including any profile-supplied options.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ResolvedPack {
    pub id: PackId,
    #[serde(default)]
    pub options: serde_norway::Value,
}

impl PackRef {
    /// Extract the [`PackId`] regardless of variant.
    ///
    /// # Examples
    ///
    /// ```
    /// use gather_step_parser::frameworks::{profile::PackRef, registry::PackId};
    ///
    /// assert_eq!(PackRef::Simple(PackId::Nestjs).pack_id(), PackId::Nestjs);
    /// ```
    #[must_use]
    pub fn pack_id(&self) -> PackId {
        match self {
            Self::Simple(id) | Self::WithOptions { id, .. } => *id,
        }
    }

    #[must_use]
    pub fn to_resolved(&self) -> ResolvedPack {
        match self {
            Self::Simple(id) => ResolvedPack {
                id: *id,
                options: serde_norway::Value::Null,
            },
            Self::WithOptions { id, options } => ResolvedPack {
                id: *id,
                options: options.clone(),
            },
        }
    }
}

/// Resolve a profile name to a flat, deduplicated list of resolved packs.
///
/// The `extends` chain is walked depth-first.  If a cycle is encountered
/// (profile A extends profile B which extends profile A), the duplicate
/// entry is simply skipped — no error is returned.
///
/// Returns an empty [`BTreeSet`] when `name` is not found in `profiles`.
///
/// # Examples
///
/// ```
/// use gather_step_parser::frameworks::{
///     profile::{PackRef, Profile, resolve_profile},
///     registry::PackId,
/// };
///
/// let profiles = vec![
///     Profile {
///         name: "base".to_owned(),
///         extends: vec![],
///         packs: vec![PackRef::Simple(PackId::Nestjs)],
///     },
///     Profile {
///         name: "extended".to_owned(),
///         extends: vec!["base".to_owned()],
///         packs: vec![PackRef::Simple(PackId::Mongoose)],
///     },
/// ];
///
/// let resolved = resolve_profile("extended", &profiles);
/// assert!(resolved.iter().any(|pack| pack.id == PackId::Nestjs));
/// assert!(resolved.iter().any(|pack| pack.id == PackId::Mongoose));
/// ```
#[must_use]
pub fn resolve_profile(name: &str, profiles: &[Profile]) -> Vec<ResolvedPack> {
    let mut result = BTreeMap::new();
    let mut visited = BTreeSet::new();
    resolve_recursive(name, profiles, &mut result, &mut visited);
    result.into_values().collect()
}

fn resolve_recursive(
    name: &str,
    profiles: &[Profile],
    result: &mut BTreeMap<PackId, ResolvedPack>,
    visited: &mut BTreeSet<String>,
) {
    if !visited.insert(name.to_owned()) {
        // Already visited — cycle or duplicate; skip.
        return;
    }

    let Some(profile) = profiles.iter().find(|p| p.name == name) else {
        return;
    };

    // Recurse into extended profiles first so derived profiles can override
    // pack options declared by a base profile for the same PackId.
    for base_name in &profile.extends {
        resolve_recursive(base_name, profiles, result, visited);
    }

    for pack_ref in &profile.packs {
        let pack = pack_ref.to_resolved();
        result.insert(pack.id, pack);
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::{PackRef, Profile, ResolvedPack, resolve_profile};
    use crate::frameworks::registry::PackId;

    #[test]
    fn resolve_profile_with_extends() {
        let profiles = vec![
            Profile {
                name: "base".to_owned(),
                extends: vec![],
                packs: vec![PackRef::Simple(PackId::Nestjs)],
            },
            Profile {
                name: "extended".to_owned(),
                extends: vec!["base".to_owned()],
                packs: vec![PackRef::Simple(PackId::Mongoose)],
            },
        ];

        let resolved = resolve_profile("extended", &profiles);
        assert!(
            resolved.iter().any(|pack| pack.id == PackId::Nestjs),
            "extended profile should inherit Nestjs from base"
        );
        assert!(
            resolved.iter().any(|pack| pack.id == PackId::Mongoose),
            "extended profile should include its own Mongoose pack"
        );
        assert_eq!(resolved.len(), 2);
    }

    #[test]
    fn simple_pack_ref_extracts_id() {
        assert_eq!(
            PackRef::Simple(PackId::Nestjs).pack_id(),
            PackId::Nestjs,
            "Simple(Nestjs).pack_id() should return Nestjs"
        );
    }

    #[test]
    fn with_options_pack_ref_extracts_id() {
        let pack_ref = PackRef::WithOptions {
            id: PackId::React,
            options: serde_norway::Value::Null,
        };
        assert_eq!(
            pack_ref.pack_id(),
            PackId::React,
            "WithOptions{{ id: React }}.pack_id() should return React"
        );
    }

    #[test]
    fn resolve_profile_unknown_name_returns_empty() {
        let profiles: Vec<Profile> = vec![];
        let resolved = resolve_profile("nonexistent", &profiles);
        assert!(resolved.is_empty());
    }

    #[test]
    fn resolve_profile_cycle_does_not_panic() {
        // A → B → A should terminate without infinite recursion.
        let profiles = vec![
            Profile {
                name: "a".to_owned(),
                extends: vec!["b".to_owned()],
                packs: vec![PackRef::Simple(PackId::Nestjs)],
            },
            Profile {
                name: "b".to_owned(),
                extends: vec!["a".to_owned()],
                packs: vec![PackRef::Simple(PackId::Mongoose)],
            },
        ];
        let resolved = resolve_profile("a", &profiles);
        // Both packs should be present regardless of cycle direction.
        assert!(resolved.iter().any(|pack| pack.id == PackId::Nestjs));
        assert!(resolved.iter().any(|pack| pack.id == PackId::Mongoose));
    }

    #[test]
    fn resolve_profile_deduplicates_packs() {
        // Two profiles that both declare the same pack — resolution should
        // yield only one entry.
        let profiles = vec![
            Profile {
                name: "base".to_owned(),
                extends: vec![],
                packs: vec![PackRef::Simple(PackId::Nestjs)],
            },
            Profile {
                name: "child".to_owned(),
                extends: vec!["base".to_owned()],
                packs: vec![
                    PackRef::Simple(PackId::Nestjs), // duplicate
                    PackRef::Simple(PackId::Mongoose),
                ],
            },
        ];
        let resolved = resolve_profile("child", &profiles);
        assert_eq!(
            resolved,
            vec![
                ResolvedPack {
                    id: PackId::Nestjs,
                    options: serde_norway::Value::Null,
                },
                ResolvedPack {
                    id: PackId::Mongoose,
                    options: serde_norway::Value::Null,
                },
            ],
            "duplicate pack IDs should be deduplicated during resolution"
        );
    }

    #[test]
    fn resolve_profile_deep_extends_chain() {
        // A → B → C
        let profiles = vec![
            Profile {
                name: "a".to_owned(),
                extends: vec!["b".to_owned()],
                packs: vec![PackRef::Simple(PackId::React)],
            },
            Profile {
                name: "b".to_owned(),
                extends: vec!["c".to_owned()],
                packs: vec![PackRef::Simple(PackId::Mongoose)],
            },
            Profile {
                name: "c".to_owned(),
                extends: vec![],
                packs: vec![PackRef::Simple(PackId::Nestjs)],
            },
        ];
        let resolved = resolve_profile("a", &profiles);
        assert_eq!(
            resolved,
            vec![
                ResolvedPack {
                    id: PackId::Nestjs,
                    options: serde_norway::Value::Null,
                },
                ResolvedPack {
                    id: PackId::Mongoose,
                    options: serde_norway::Value::Null,
                },
                ResolvedPack {
                    id: PackId::React,
                    options: serde_norway::Value::Null,
                },
            ]
        );
    }

    #[test]
    fn resolve_profile_preserves_pack_options() {
        let profiles = vec![
            Profile {
                name: "base".to_owned(),
                extends: vec![],
                packs: vec![PackRef::WithOptions {
                    id: PackId::React,
                    options: serde_norway::from_str::<serde_norway::Value>("strict: true")
                        .expect("valid yaml"),
                }],
            },
            Profile {
                name: "child".to_owned(),
                extends: vec!["base".to_owned()],
                packs: vec![PackRef::Simple(PackId::Nestjs)],
            },
        ];

        let resolved = resolve_profile("child", &profiles);
        let react_pack = resolved
            .iter()
            .find(|pack| pack.id == PackId::React)
            .expect("react pack should resolve");
        assert_eq!(
            react_pack.options,
            serde_norway::from_str::<serde_norway::Value>("strict: true").expect("valid yaml")
        );
    }

    #[test]
    fn resolve_profile_child_overrides_base_pack_options() {
        let profiles = vec![
            Profile {
                name: "base".to_owned(),
                extends: vec![],
                packs: vec![PackRef::WithOptions {
                    id: PackId::React,
                    options: serde_norway::from_str::<serde_norway::Value>("strict: false")
                        .expect("valid yaml"),
                }],
            },
            Profile {
                name: "child".to_owned(),
                extends: vec!["base".to_owned()],
                packs: vec![PackRef::WithOptions {
                    id: PackId::React,
                    options: serde_norway::from_str::<serde_norway::Value>("strict: true")
                        .expect("valid yaml"),
                }],
            },
        ];

        let resolved = resolve_profile("child", &profiles);
        assert_eq!(
            resolved,
            vec![ResolvedPack {
                id: PackId::React,
                options: serde_norway::from_str::<serde_norway::Value>("strict: true")
                    .expect("valid yaml"),
            }]
        );
    }
}
