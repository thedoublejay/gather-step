//! Cache identity helpers for coordinated PR-set reviews.

use serde::{Deserialize, Serialize};

use crate::pr_review::artifact_root::{CacheKey, MARKER_SCHEMA_VERSION};

/// Resolved cache identity for one PR-set entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedPrCacheEntry {
    pub id: String,
    pub repo: String,
    pub workspace_hash: String,
    pub base_sha: String,
    pub head_sha: String,
    pub config_hash: String,
    pub gather_step_version: String,
}

impl ResolvedPrCacheEntry {
    #[must_use]
    pub fn cache_key(&self) -> CacheKey {
        CacheKey {
            workspace_hash: self.workspace_hash.clone(),
            base_sha: self.base_sha.clone(),
            head_sha: self.head_sha.clone(),
            config_hash: self.config_hash.clone(),
            schema_version: MARKER_SCHEMA_VERSION,
            gather_step_version: self.gather_step_version.clone(),
        }
    }
}

/// Stable cache identity for a full PR set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrSetCacheKey {
    pub manifest_id: String,
    pub manifest_version: u32,
    pub entries: Vec<ResolvedPrCacheEntry>,
}

impl PrSetCacheKey {
    /// Stable fingerprint for the complete set. Entry order in the manifest is
    /// not semantically meaningful for cache reuse, so entries are sorted before
    /// hashing.
    #[must_use]
    pub fn fingerprint(&self) -> String {
        set_fingerprint(self)
    }
}

/// Return the same fingerprint a single-PR run would use for this entry.
#[must_use]
pub fn entry_fingerprint(entry: &ResolvedPrCacheEntry) -> String {
    entry.cache_key().fingerprint()
}

/// Stable fingerprint for the full set.
#[must_use]
pub fn set_fingerprint(key: &PrSetCacheKey) -> String {
    let mut entries = key.entries.clone();
    entries.sort_by(|a, b| {
        (&a.repo, &a.base_sha, &a.head_sha, &a.id).cmp(&(&b.repo, &b.base_sha, &b.head_sha, &b.id))
    });
    let entry_json = serde_json::to_string(&entries).unwrap_or_default();
    let canonical = format!(
        r#"{{"entries":{},"manifest_id":{},"manifest_version":{}}}"#,
        entry_json,
        serde_json::to_string(&key.manifest_id).unwrap_or_default(),
        key.manifest_version
    );
    let hash = blake3::hash(canonical.as_bytes());
    let hex = hash.to_hex();
    hex[..16].to_owned()
}

#[cfg(test)]
mod tests {
    use super::{PrSetCacheKey, ResolvedPrCacheEntry, entry_fingerprint};
    use crate::pr_review::artifact_root::{CacheKey, MARKER_SCHEMA_VERSION};

    fn entry(id: &str, repo: &str) -> ResolvedPrCacheEntry {
        ResolvedPrCacheEntry {
            id: id.to_owned(),
            repo: repo.to_owned(),
            workspace_hash: "workspacehash123".to_owned(),
            base_sha: "1111111111111111111111111111111111111111".to_owned(),
            head_sha: "2222222222222222222222222222222222222222".to_owned(),
            config_hash: "confighash123456".to_owned(),
            gather_step_version: "4.1.0".to_owned(),
        }
    }

    #[test]
    fn entry_fingerprint_matches_single_pr_cache_key() {
        let entry = entry("api", "web-api");
        let single = CacheKey {
            workspace_hash: entry.workspace_hash.clone(),
            base_sha: entry.base_sha.clone(),
            head_sha: entry.head_sha.clone(),
            config_hash: entry.config_hash.clone(),
            schema_version: MARKER_SCHEMA_VERSION,
            gather_step_version: entry.gather_step_version.clone(),
        };

        assert_eq!(entry_fingerprint(&entry), single.fingerprint());
    }

    #[test]
    fn set_fingerprint_is_manifest_order_independent() {
        let a = PrSetCacheKey {
            manifest_id: "checkout-refresh".to_owned(),
            manifest_version: 0,
            entries: vec![entry("api", "api"), entry("web", "web")],
        };
        let b = PrSetCacheKey {
            manifest_id: "checkout-refresh".to_owned(),
            manifest_version: 0,
            entries: vec![entry("web", "web"), entry("api", "api")],
        };

        assert_eq!(a.fingerprint(), b.fingerprint());
    }

    #[test]
    fn set_fingerprint_changes_when_manifest_id_changes() {
        let a = PrSetCacheKey {
            manifest_id: "checkout-refresh".to_owned(),
            manifest_version: 0,
            entries: vec![entry("api", "api")],
        };
        let b = PrSetCacheKey {
            manifest_id: "checkout-refresh-v2".to_owned(),
            manifest_version: 0,
            entries: vec![entry("api", "api")],
        };

        assert_ne!(a.fingerprint(), b.fingerprint());
    }
}
