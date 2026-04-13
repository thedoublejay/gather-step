use std::{collections::BTreeSet, sync::OnceLock};

use gather_step_storage::{
    CommitFileChangeKind, CommitFileDeltaRecord, CommitRecord, FileAnalytics, MetadataStore,
    MetadataStoreError,
};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use tracing::debug;

const DEFAULT_TOUCH_WEIGHT: f64 = 1.0;
const DEFAULT_BUS_FACTOR_THRESHOLD: f64 = 0.8;

/// Module-level fallback key used when no per-instance redact key has been
/// installed via [`set_redact_key`].  Even the fallback is a keyed hash (not
/// plain BLAKE3), so rainbow-table attacks require knowledge of this key in
/// addition to the email address.  The fallback is intentionally distinct from
/// the all-zeros key to prevent accidental collision with the trivial case.
const FALLBACK_REDACT_KEY: &[u8; 32] = b"gather-step-redact-key-v1-stable";

/// Per-instance redact key storage.  Initialized once at startup by
/// [`set_redact_key`]; falls back to [`FALLBACK_REDACT_KEY`] if unset.
static REDACT_KEY: OnceLock<[u8; 32]> = OnceLock::new();

/// Install the per-instance redact key used by [`redact_email`].
///
/// Should be called once, early in process startup (e.g. during
/// `McpContext::from_workspace_stores`).  Subsequent calls are silently
/// ignored — the first caller wins, which is the correct semantics for a
/// one-time init pattern.
///
/// Without a call to this function `redact_email` uses [`FALLBACK_REDACT_KEY`],
/// which is still keyed BLAKE3 (preimage-hard), but not per-instance.
pub fn set_redact_key(key: [u8; 32]) {
    let _ = REDACT_KEY.set(key);
}

/// Redact a raw author email address into a stable opaque identifier.
///
/// The identifier is the first 16 hex characters of a keyed BLAKE3 digest of
/// the email bytes, suffixed with `"@redacted"`.  This is:
///
/// - **Stable**: the same email always produces the same identifier within a
///   process (or across processes sharing the same redact key).
/// - **Opaque**: preimage recovery is infeasible without the per-instance
///   redact key.  The key is installed at startup via [`set_redact_key`]; if
///   not set, a module-level constant fallback key is used instead.
/// - **Recognizable**: two entries with the same identifier are from the same
///   author, enabling aggregation without exposing PII.
///
/// BLAKE3 is used throughout the workspace for content hashing and node IDs;
/// this function aligns with that choice rather than pulling in a second hash
/// family.
///
/// # Examples
///
/// ```
/// use gather_step_git::redact_email;
///
/// let id = redact_email("alice@example.com");
/// assert!(id.ends_with("@redacted"));
/// assert_eq!(id.len(), "0000000000000000@redacted".len());
/// ```
#[must_use]
pub fn redact_email(email: &str) -> String {
    let key = REDACT_KEY.get().unwrap_or(FALLBACK_REDACT_KEY);
    let digest = blake3::keyed_hash(key, email.as_bytes());
    let prefix: String = digest
        .as_bytes()
        .iter()
        .flat_map(|byte| {
            [
                char::from_digit(u32::from(byte >> 4), 16).unwrap_or('0'),
                char::from_digit(u32::from(byte & 0x0f), 16).unwrap_or('0'),
            ]
        })
        .take(16)
        .collect();
    format!("{prefix}@redacted")
}

#[derive(Clone, Debug)]
pub struct OwnershipOptions {
    pub touch_weight: f64,
    pub bus_factor_threshold: f64,
}

impl Default for OwnershipOptions {
    fn default() -> Self {
        Self {
            touch_weight: DEFAULT_TOUCH_WEIGHT,
            bus_factor_threshold: DEFAULT_BUS_FACTOR_THRESHOLD,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OwnershipContribution {
    pub author_email: String,
    pub contribution_score: f64,
    pub ownership_pct: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OwnershipSummary {
    pub repo: String,
    pub file_path: String,
    pub total_contribution_score: f64,
    pub contributions: Vec<OwnershipContribution>,
    pub top_owner_email: Option<String>,
    pub top_owner_pct: f64,
    pub bus_factor: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BusFactorRisk {
    pub repo: String,
    pub file_path: String,
    pub bus_factor: u32,
    pub top_owner_email: Option<String>,
    pub top_owner_pct: f64,
    pub is_risky: bool,
}

pub fn analyze_ownership_from_store<S: MetadataStore>(
    store: &S,
    repo: &str,
    options: &OwnershipOptions,
) -> Result<Vec<OwnershipSummary>, MetadataStoreError> {
    let commits = store.get_commits_by_repo(repo, i64::MIN, i64::MAX)?;
    let deltas = store.get_commit_file_deltas_for_repo(repo)?;
    Ok(analyze_ownership(&commits, &deltas, options))
}

/// Single-file variant of [`analyze_ownership_from_store`]. Used by per-file
/// query paths (e.g. the MCP `who_owns` tool) to answer ownership questions
/// for one file without scanning the entire repo's commit history.
///
/// Resolves the file's rename chain inside SQL via
/// [`MetadataStore::get_history_for_file_with_renames`] and returns `None`
/// when the file has no recorded history.
pub fn analyze_ownership_for_file<S: MetadataStore>(
    store: &S,
    repo: &str,
    file_path: &str,
    options: &OwnershipOptions,
) -> Result<Option<OwnershipSummary>, MetadataStoreError> {
    let (commits, deltas) = store.get_history_for_file_with_renames(repo, file_path)?;
    if commits.is_empty() && deltas.is_empty() {
        return Ok(None);
    }
    // `analyze_ownership` returns one summary per file path it observes.
    // Rename canonicalisation rolls earlier paths into the latest one, so
    // the requested `file_path` is the right post-canonicalisation key.
    Ok(analyze_ownership(&commits, &deltas, options)
        .into_iter()
        .find(|summary| summary.file_path == file_path))
}

pub fn persist_ownership_into_file_analytics<S: MetadataStore>(
    store: &S,
    repo: &str,
    ownership: &[OwnershipSummary],
) -> Result<(), MetadataStoreError> {
    let mut analytics_by_path = store
        .list_file_analytics_for_repo(repo)?
        .into_iter()
        .map(|record| (record.file_path.clone(), record))
        .collect::<FxHashMap<_, _>>();

    for summary in ownership {
        let mut record = analytics_by_path
            .remove(&summary.file_path)
            .unwrap_or(FileAnalytics {
                repo: summary.repo.clone(),
                file_path: summary.file_path.clone(),
                total_commits: 0,
                commits_90d: 0,
                commits_180d: 0,
                commits_365d: 0,
                hotspot_score: 0.0,
                bus_factor: 0,
                top_owner_email: None,
                top_owner_pct: 0.0,
                complexity_trend: None,
                last_modified: 0,
                computed_at: 0,
            });
        record.top_owner_email.clone_from(&summary.top_owner_email);
        record.top_owner_pct = summary.top_owner_pct;
        record.bus_factor = i64::from(summary.bus_factor);
        analytics_by_path.insert(summary.file_path.clone(), record);
    }

    let mut merged = analytics_by_path.into_values().collect::<Vec<_>>();
    merged.sort_by(|left, right| left.file_path.cmp(&right.file_path));
    store.replace_file_analytics_for_repo(repo, &merged)
}

pub fn bus_factor_risks(
    ownership: &[OwnershipSummary],
    options: &OwnershipOptions,
) -> Vec<BusFactorRisk> {
    ownership
        .iter()
        .map(|summary| BusFactorRisk {
            repo: summary.repo.clone(),
            file_path: summary.file_path.clone(),
            bus_factor: summary.bus_factor,
            top_owner_email: summary.top_owner_email.clone(),
            top_owner_pct: summary.top_owner_pct,
            is_risky: summary.top_owner_pct >= options.bus_factor_threshold,
        })
        .collect()
}

pub fn analyze_ownership(
    commits: &[CommitRecord],
    deltas: &[CommitFileDeltaRecord],
    options: &OwnershipOptions,
) -> Vec<OwnershipSummary> {
    let commit_by_sha = commits
        .iter()
        .map(|commit| (commit.sha.clone(), commit.author_email.clone()))
        .collect::<FxHashMap<_, _>>();
    debug_assert!(
        commits.windows(2).all(|pair| pair[0].repo == pair[1].repo),
        "analyze_ownership was called with commits from multiple repos",
    );
    debug_assert!(
        deltas.windows(2).all(|pair| pair[0].repo == pair[1].repo),
        "analyze_ownership was called with deltas from multiple repos",
    );
    let rename_successors = build_rename_successors(commits, deltas);
    let repo = commits.first().map_or_else(
        || {
            deltas
                .first()
                .map(|delta| delta.repo.clone())
                .unwrap_or_default()
        },
        |commit| commit.repo.clone(),
    );

    let mut contribution_by_file = FxHashMap::<String, FxHashMap<String, f64>>::default();
    for delta in deltas {
        if matches!(
            delta.change_kind,
            CommitFileChangeKind::TypeChanged | CommitFileChangeKind::Deleted
        ) {
            continue;
        }
        let Some(author_email) = commit_by_sha.get(&delta.sha) else {
            continue;
        };
        let canonical_path = canonicalize_path(&rename_successors, &delta.file_path);
        let contribution =
            f64::from(u32::try_from(delta.insertions.unwrap_or(0)).unwrap_or(u32::MAX))
                + f64::from(u32::try_from(delta.deletions.unwrap_or(0)).unwrap_or(u32::MAX))
                + options.touch_weight;
        let per_author = contribution_by_file.entry(canonical_path).or_default();
        *per_author.entry(author_email.clone()).or_default() += contribution;
    }

    let mut summaries = contribution_by_file
        .into_iter()
        .map(|(file_path, contributions)| {
            let total_contribution_score = contributions.values().sum::<f64>();
            let mut ranked = contributions
                .into_iter()
                .map(|(author_email, contribution_score)| OwnershipContribution {
                    ownership_pct: if total_contribution_score == 0.0 {
                        0.0
                    } else {
                        contribution_score / total_contribution_score
                    },
                    author_email,
                    contribution_score,
                })
                .collect::<Vec<_>>();
            ranked.sort_by(|left, right| {
                right
                    .ownership_pct
                    .total_cmp(&left.ownership_pct)
                    .then_with(|| left.author_email.cmp(&right.author_email))
            });

            let top_owner_email = ranked.first().map(|entry| entry.author_email.clone());
            let top_owner_pct = ranked.first().map_or(0.0, |entry| entry.ownership_pct);
            let bus_factor = compute_bus_factor(&ranked, options.bus_factor_threshold);

            OwnershipSummary {
                repo: repo.clone(),
                file_path,
                total_contribution_score,
                contributions: ranked,
                top_owner_email,
                top_owner_pct,
                bus_factor,
            }
        })
        .collect::<Vec<_>>();
    summaries.sort_by(|left, right| left.file_path.cmp(&right.file_path));
    summaries
}

fn compute_bus_factor(contributions: &[OwnershipContribution], threshold: f64) -> u32 {
    if contributions.is_empty() {
        return 0;
    }
    let mut running = 0.0;
    for (index, contribution) in contributions.iter().enumerate() {
        running += contribution.ownership_pct;
        if running >= threshold {
            return u32::try_from(index + 1).unwrap_or(u32::MAX);
        }
    }
    u32::try_from(contributions.len()).unwrap_or(u32::MAX)
}

fn build_rename_successors(
    commits: &[CommitRecord],
    deltas: &[CommitFileDeltaRecord],
) -> FxHashMap<String, String> {
    // Resolve renames in commit chronological order so a later move wins
    // over an earlier one. Sorting deltas by SHA (a content hash) here
    // would be effectively random and could pick the wrong successor when
    // the same path is renamed twice, splitting ownership across paths.
    let date_by_sha = commits
        .iter()
        .map(|commit| (commit.sha.as_str(), commit.date))
        .collect::<FxHashMap<_, _>>();
    let mut ordered = deltas.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| {
        let left_date = date_by_sha.get(left.sha.as_str()).copied().unwrap_or(0);
        let right_date = date_by_sha.get(right.sha.as_str()).copied().unwrap_or(0);
        left_date
            .cmp(&right_date)
            .then_with(|| left.sha.cmp(&right.sha))
            .then_with(|| left.file_path.cmp(&right.file_path))
    });

    let mut successors = FxHashMap::default();
    for delta in ordered {
        if delta.change_kind == CommitFileChangeKind::Renamed
            && let Some(old_path) = delta.old_path.as_ref()
        {
            let current = canonicalize_path(&successors, &delta.file_path);
            successors.insert(old_path.clone(), current);
        }
    }
    successors
}

fn canonicalize_path(successors: &FxHashMap<String, String>, path: &str) -> String {
    let mut current = path;
    let mut seen = BTreeSet::new();
    while let Some(next) = successors.get(current) {
        if !seen.insert(current.to_owned()) {
            // A rename cycle (e.g. A→B followed later by B→A) is expected
            // operational noise in repos that revert file moves. The cycle
            // detection logic is correct — best-effort canonical path is
            // returned — but this is not an actionable warning so it is
            // demoted to `debug!` to avoid flooding logs.
            debug!(
                path = path,
                cycle_at = current,
                "rename successor cycle detected; returning best-effort canonical path",
            );
            break;
        }
        current = next;
    }
    current.to_owned()
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::{OwnershipOptions, analyze_ownership, bus_factor_risks, compute_bus_factor};
    use gather_step_storage::{CommitFileChangeKind, CommitFileDeltaRecord, CommitRecord};

    #[test]
    fn author_with_dominant_share_is_flagged_as_bus_factor_risk() {
        let summaries = analyze_ownership(&commits(), &deltas(), &OwnershipOptions::default());
        let risks = bus_factor_risks(&summaries, &OwnershipOptions::default());

        assert!(risks.iter().any(|risk| {
            risk.file_path == "src/lib.rs"
                && risk.is_risky
                && risk.top_owner_email.as_deref() == Some("alice@example.com")
        }));
    }

    #[test]
    fn ownership_percentages_are_ranked_and_normalized() {
        let summaries = analyze_ownership(&commits(), &deltas(), &OwnershipOptions::default());
        let lib = summaries
            .iter()
            .find(|summary| summary.file_path == "src/lib.rs")
            .expect("lib summary");

        assert_eq!(lib.top_owner_email.as_deref(), Some("alice@example.com"));
        assert!(lib.top_owner_pct >= 0.8);
        assert_eq!(lib.bus_factor, 1);
        assert!(
            (lib.contributions
                .iter()
                .map(|entry| entry.ownership_pct)
                .sum::<f64>()
                - 1.0)
                .abs()
                < 0.0001
        );
    }

    #[test]
    fn rename_history_rolls_old_path_into_latest_path() {
        let summaries = analyze_ownership(&commits(), &deltas(), &OwnershipOptions::default());
        assert!(
            summaries
                .iter()
                .any(|summary| summary.file_path == "src/lib.rs")
        );
        assert!(
            summaries
                .iter()
                .all(|summary| summary.file_path != "src/old.rs")
        );
    }

    #[test]
    fn bus_factor_counts_authors_until_threshold() {
        assert_eq!(
            compute_bus_factor(
                &[
                    contribution("a", 0.5),
                    contribution("b", 0.3),
                    contribution("c", 0.2),
                ],
                0.8,
            ),
            2
        );
    }

    #[test]
    fn deleted_files_are_ignored_for_ownership() {
        let summaries = analyze_ownership(
            &commits(),
            &[CommitFileDeltaRecord {
                repo: "service-a".to_owned(),
                sha: "a".to_owned(),
                file_path: "src/deleted.rs".to_owned(),
                change_kind: CommitFileChangeKind::Deleted,
                insertions: Some(1),
                deletions: Some(1),
                old_path: None,
            }],
            &OwnershipOptions::default(),
        );
        assert!(summaries.is_empty());
    }

    fn contribution(author_email: &str, ownership_pct: f64) -> super::OwnershipContribution {
        super::OwnershipContribution {
            author_email: author_email.to_owned(),
            contribution_score: ownership_pct,
            ownership_pct,
        }
    }

    fn commits() -> Vec<CommitRecord> {
        vec![
            CommitRecord {
                sha: "a".to_owned(),
                repo: "service-a".to_owned(),
                author_email: "alice@example.com".to_owned(),
                date: 1,
                message: "feat: a".to_owned(),
                classification: Some("feat".to_owned()),
                files_changed: 1,
                insertions: 10,
                deletions: 0,
                has_decision_signal: false,
                pr_number: None,
            },
            CommitRecord {
                sha: "b".to_owned(),
                repo: "service-a".to_owned(),
                author_email: "alice@example.com".to_owned(),
                date: 2,
                message: "refactor: b".to_owned(),
                classification: Some("refactor".to_owned()),
                files_changed: 1,
                insertions: 8,
                deletions: 1,
                has_decision_signal: false,
                pr_number: None,
            },
            CommitRecord {
                sha: "c".to_owned(),
                repo: "service-a".to_owned(),
                author_email: "bob@example.com".to_owned(),
                date: 3,
                message: "fix: c".to_owned(),
                classification: Some("fix".to_owned()),
                files_changed: 1,
                insertions: 1,
                deletions: 1,
                has_decision_signal: false,
                pr_number: None,
            },
        ]
    }

    fn deltas() -> Vec<CommitFileDeltaRecord> {
        vec![
            CommitFileDeltaRecord {
                repo: "service-a".to_owned(),
                sha: "a".to_owned(),
                file_path: "src/old.rs".to_owned(),
                change_kind: CommitFileChangeKind::Modified,
                insertions: Some(10),
                deletions: Some(0),
                old_path: None,
            },
            CommitFileDeltaRecord {
                repo: "service-a".to_owned(),
                sha: "b".to_owned(),
                file_path: "src/lib.rs".to_owned(),
                change_kind: CommitFileChangeKind::Renamed,
                insertions: Some(0),
                deletions: Some(0),
                old_path: Some("src/old.rs".to_owned()),
            },
            CommitFileDeltaRecord {
                repo: "service-a".to_owned(),
                sha: "c".to_owned(),
                file_path: "src/lib.rs".to_owned(),
                change_kind: CommitFileChangeKind::Modified,
                insertions: Some(1),
                deletions: Some(1),
                old_path: None,
            },
        ]
    }
}
