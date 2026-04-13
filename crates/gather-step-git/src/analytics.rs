use std::collections::{BTreeMap, BTreeSet};

use gather_step_storage::{CoChangePairRecord, FileAnalytics, MetadataStore, MetadataStoreError};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{CommitFact, CommitFileChangeKind};

const SECONDS_PER_DAY: f64 = 86_400.0;
/// Tolerance for harmless clock skew between the indexing host and
/// commit-author timestamps. Events more than this far into the future
/// are treated as anomalous and excluded from recency-weighted analytics.
const CLOCK_SKEW_GRACE_SECONDS: i64 = 5 * 60;

#[derive(Clone, Debug)]
pub struct AnalyticsOptions {
    pub hotspot_tau_days: f64,
    pub co_change_tau_days: f64,
    pub touch_weight: f64,
    pub max_files_per_commit_for_co_change: usize,
    /// Global cap on distinct co-change pairs accumulated during one
    /// [`analyze_history`] call. Without this cap, a hostile or
    /// pathologically wide history can grow `co_change_by_pair` to
    /// hundreds of millions of entries (per-commit cap × commit count).
    /// When the cap is reached, occurrences for already-seen pairs still
    /// increment, but no new pairs are added.
    pub max_co_change_pairs: usize,
    pub complexity_by_file: BTreeMap<String, f64>,
    pub excluded_co_change_pairs: BTreeSet<(String, String)>,
    pub min_co_change_occurrences: u32,
    pub min_co_change_strength: f64,
}

impl Default for AnalyticsOptions {
    fn default() -> Self {
        Self {
            hotspot_tau_days: 90.0,
            co_change_tau_days: 180.0,
            touch_weight: 1.0,
            max_files_per_commit_for_co_change: 128,
            max_co_change_pairs: 500_000,
            complexity_by_file: BTreeMap::new(),
            excluded_co_change_pairs: BTreeSet::new(),
            min_co_change_occurrences: 2,
            min_co_change_strength: 0.0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HotspotRecord {
    pub repo: String,
    pub file_path: String,
    pub total_commits: u32,
    pub commits_90d: u32,
    pub commits_180d: u32,
    pub commits_365d: u32,
    pub weighted_churn: f64,
    pub complexity_factor: f64,
    pub hotspot_score: f64,
    pub last_modified_unix: i64,
    pub computed_at_unix: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CoChangeRecord {
    pub repo: String,
    pub file_a: String,
    pub file_b: String,
    pub strength: f64,
    pub occurrences: u32,
    pub last_seen_unix: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AnalyticsReport {
    pub hotspots: Vec<HotspotRecord>,
    pub co_changes: Vec<CoChangeRecord>,
}

impl AnalyticsReport {
    pub fn persist<S: MetadataStore>(
        &self,
        store: &S,
        repo: &str,
    ) -> Result<(), MetadataStoreError> {
        let hotspot_rows = self
            .hotspots
            .iter()
            .map(|record| FileAnalytics {
                repo: record.repo.clone(),
                file_path: record.file_path.clone(),
                total_commits: i64::from(record.total_commits),
                commits_90d: i64::from(record.commits_90d),
                commits_180d: i64::from(record.commits_180d),
                commits_365d: i64::from(record.commits_365d),
                hotspot_score: record.hotspot_score,
                bus_factor: 0,
                top_owner_email: None,
                top_owner_pct: 0.0,
                complexity_trend: None,
                last_modified: record.last_modified_unix,
                computed_at: record.computed_at_unix,
            })
            .collect::<Vec<_>>();
        let co_change_rows = self
            .co_changes
            .iter()
            .map(|record| CoChangePairRecord {
                repo: record.repo.clone(),
                file_a: record.file_a.clone(),
                file_b: record.file_b.clone(),
                strength: record.strength,
                occurrences: i64::from(record.occurrences),
                last_seen: record.last_seen_unix,
            })
            .collect::<Vec<_>>();
        store.replace_file_analytics_for_repo(repo, &hotspot_rows)?;
        store.replace_co_change_pairs_for_repo(repo, &co_change_rows)?;
        Ok(())
    }
}

pub fn analyze_history(
    facts: &[CommitFact],
    computed_at_unix: i64,
    options: &AnalyticsOptions,
) -> AnalyticsReport {
    // The first fact's repo is treated as authoritative for the entire batch.
    // Callers must pass single-repo input; mixing repos here would silently
    // attribute every fact to whichever repo happens to come first. The
    // `gather-step-git` ingestion layer always runs per-repo so this
    // invariant is upheld in practice; future callers should preserve it.
    let Some(repo) = facts.first().map(|fact| fact.repo.clone()) else {
        return AnalyticsReport::default();
    };
    debug_assert!(
        facts.iter().all(|fact| fact.repo == repo),
        "analyze_history was called with facts from multiple repos: {repo} != ...",
    );

    let rename_successors = build_rename_successors(facts);
    let mut hotspot_by_file = FxHashMap::<String, MutableHotspot>::default();
    let mut co_change_by_pair = FxHashMap::<(String, String), MutableCoChange>::default();

    for fact in facts {
        let Some(age_days) = age_days(computed_at_unix, fact.author_date_unix) else {
            warn!(
                repo = %repo,
                sha = %fact.sha,
                author_date_unix = fact.author_date_unix,
                computed_at_unix,
                "skipping future-dated commit from analytics: anomalous clock skew",
            );
            continue;
        };
        let hotspot_decay = decay(age_days, options.hotspot_tau_days);
        let co_change_decay = decay(age_days, options.co_change_tau_days);

        let mut touched_paths = BTreeSet::new();
        for delta in &fact.file_deltas {
            if matches!(
                delta.change_kind,
                CommitFileChangeKind::TypeChanged | CommitFileChangeKind::Deleted
            ) {
                continue;
            }

            let canonical_path = canonicalize_path(&rename_successors, &delta.file_path);
            let complexity_factor = options
                .complexity_by_file
                .get(&canonical_path)
                .copied()
                .unwrap_or(1.0);
            let churn = f64::from(u32::try_from(delta.insertions.unwrap_or(0)).unwrap_or(u32::MAX))
                + f64::from(u32::try_from(delta.deletions.unwrap_or(0)).unwrap_or(u32::MAX))
                + options.touch_weight;
            let entry = hotspot_by_file.entry(canonical_path.clone()).or_default();
            // saturating_add: an extreme repo (millions of commits) cannot
            // wrap u32 silently and produce wrong rankings.
            entry.total_commits = entry.total_commits.saturating_add(1);
            if age_days <= 90.0 {
                entry.commits_90d = entry.commits_90d.saturating_add(1);
            }
            if age_days <= 180.0 {
                entry.commits_180d = entry.commits_180d.saturating_add(1);
            }
            if age_days <= 365.0 {
                entry.commits_365d = entry.commits_365d.saturating_add(1);
            }
            entry.weighted_churn += hotspot_decay * churn;
            entry.last_modified_unix = entry.last_modified_unix.max(fact.author_date_unix);
            entry.complexity_factor = complexity_factor;
            touched_paths.insert(canonical_path);
        }

        let touched_paths = touched_paths.into_iter().collect::<Vec<_>>();
        if touched_paths.len() <= options.max_files_per_commit_for_co_change {
            for left_index in 0..touched_paths.len() {
                for right_index in (left_index + 1)..touched_paths.len() {
                    let pair = canonicalize_pair(
                        touched_paths[left_index].clone(),
                        touched_paths[right_index].clone(),
                    );
                    if options.excluded_co_change_pairs.contains(&pair) {
                        continue;
                    }
                    // Cap the global pair count: existing pairs still
                    // accumulate occurrences/strength so popular pairs
                    // remain accurate, but new pairs are dropped once the
                    // limit is hit. Without this cap a hostile or wide
                    // history can grow `co_change_by_pair` unboundedly.
                    let entry = if let Some(entry) = co_change_by_pair.get_mut(&pair) {
                        entry
                    } else {
                        if co_change_by_pair.len() >= options.max_co_change_pairs {
                            continue;
                        }
                        co_change_by_pair.entry(pair).or_default()
                    };
                    entry.occurrences = entry.occurrences.saturating_add(1);
                    entry.strength += co_change_decay;
                    entry.last_seen_unix = entry.last_seen_unix.max(fact.author_date_unix);
                }
            }
        }
    }

    let mut hotspots = hotspot_by_file
        .into_iter()
        .map(|(file_path, state)| HotspotRecord {
            repo: repo.clone(),
            file_path,
            total_commits: state.total_commits,
            commits_90d: state.commits_90d,
            commits_180d: state.commits_180d,
            commits_365d: state.commits_365d,
            weighted_churn: state.weighted_churn,
            complexity_factor: state.complexity_factor,
            hotspot_score: state.weighted_churn * state.complexity_factor,
            last_modified_unix: state.last_modified_unix,
            computed_at_unix: computed_at_unix.max(0),
        })
        .collect::<Vec<_>>();
    hotspots.sort_by(|left, right| {
        right
            .hotspot_score
            .total_cmp(&left.hotspot_score)
            .then_with(|| left.file_path.cmp(&right.file_path))
    });

    let mut co_changes = co_change_by_pair
        .into_iter()
        .filter_map(|((file_a, file_b), state)| {
            if state.occurrences < options.min_co_change_occurrences
                || state.strength < options.min_co_change_strength
            {
                return None;
            }
            Some(CoChangeRecord {
                repo: repo.clone(),
                file_a,
                file_b,
                strength: state.strength,
                occurrences: state.occurrences,
                last_seen_unix: state.last_seen_unix,
            })
        })
        .collect::<Vec<_>>();
    co_changes.sort_by(|left, right| {
        right
            .strength
            .total_cmp(&left.strength)
            .then_with(|| left.file_a.cmp(&right.file_a))
            .then_with(|| left.file_b.cmp(&right.file_b))
    });

    AnalyticsReport {
        hotspots,
        co_changes,
    }
}

#[derive(Default)]
struct MutableHotspot {
    total_commits: u32,
    commits_90d: u32,
    commits_180d: u32,
    commits_365d: u32,
    weighted_churn: f64,
    complexity_factor: f64,
    last_modified_unix: i64,
}

#[derive(Default)]
struct MutableCoChange {
    strength: f64,
    occurrences: u32,
    last_seen_unix: i64,
}

fn build_rename_successors(facts: &[CommitFact]) -> FxHashMap<String, String> {
    let mut ordered = facts.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|fact| fact.author_date_unix);

    let mut successors = FxHashMap::default();
    for fact in ordered {
        for delta in &fact.file_deltas {
            if delta.change_kind == CommitFileChangeKind::Renamed
                && let Some(old_path) = delta.old_path.as_ref()
            {
                let current = canonicalize_path(&successors, &delta.file_path);
                successors.insert(old_path.clone(), current);
            }
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

fn canonicalize_pair(left: String, right: String) -> (String, String) {
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}

/// Returns the number of days between `computed_at_unix` and `event_unix`,
/// or `None` when the event is more than [`CLOCK_SKEW_GRACE_SECONDS`] in the
/// future. Future-dated commits would otherwise round to age=0 and silently
/// receive maximum recency weight, which lets a hostile author dominate
/// hotspot/co-change scoring.
fn age_days(computed_at_unix: i64, event_unix: i64) -> Option<f64> {
    let delta = computed_at_unix.saturating_sub(event_unix);
    if delta < -CLOCK_SKEW_GRACE_SECONDS {
        return None;
    }
    let clamped = u32::try_from(delta.max(0)).unwrap_or(u32::MAX);
    Some(f64::from(clamped) / SECONDS_PER_DAY)
}

fn decay(age_days: f64, tau_days: f64) -> f64 {
    if tau_days <= 0.0 {
        return 1.0;
    }
    (-age_days / tau_days).exp()
}

#[cfg(test)]
mod tests {
    use std::iter;

    use pretty_assertions::assert_eq;

    use super::{AnalyticsOptions, analyze_history};
    use crate::{CommitFact, CommitFileChangeKind, CommitFileDelta};

    #[test]
    fn hotspot_scoring_favors_frequent_recent_complex_files() {
        let computed_at = 200 * 86_400;
        let mut options = AnalyticsOptions {
            min_co_change_occurrences: 1,
            ..AnalyticsOptions::default()
        };
        options
            .complexity_by_file
            .insert("src/big.rs".to_owned(), 4.0);
        options
            .complexity_by_file
            .insert("src/small.rs".to_owned(), 1.0);

        let big_file_facts = (0..50).map(|offset| {
            commit_fact(
                200 * 86_400 - i64::from(offset) * 86_400,
                vec![delta(
                    "src/big.rs",
                    CommitFileChangeKind::Modified,
                    Some(10),
                    Some(5),
                    None,
                )],
            )
        });
        let small_file_facts = (0..5).map(|offset| {
            commit_fact(
                200 * 86_400 - i64::from(offset) * 86_400,
                vec![delta(
                    "src/small.rs",
                    CommitFileChangeKind::Modified,
                    Some(2),
                    Some(1),
                    None,
                )],
            )
        });
        let facts = big_file_facts.chain(small_file_facts).collect::<Vec<_>>();

        let report = analyze_history(&facts, computed_at, &options);
        assert_eq!(report.hotspots[0].file_path, "src/big.rs");
        assert!(report.hotspots[0].hotspot_score > report.hotspots[1].hotspot_score);
    }

    #[test]
    fn co_change_pairs_accumulate_occurrences_and_canonicalize_paths() {
        let options = AnalyticsOptions {
            min_co_change_occurrences: 1,
            ..AnalyticsOptions::default()
        };

        let facts = iter::repeat_with(|| {
            commit_fact(
                100,
                vec![
                    delta(
                        "src/z.rs",
                        CommitFileChangeKind::Modified,
                        Some(1),
                        Some(0),
                        None,
                    ),
                    delta(
                        "src/a.rs",
                        CommitFileChangeKind::Modified,
                        Some(1),
                        Some(0),
                        None,
                    ),
                ],
            )
        })
        .take(10)
        .collect::<Vec<_>>();

        let report = analyze_history(&facts, 100, &options);
        assert_eq!(report.co_changes.len(), 1);
        assert_eq!(report.co_changes[0].file_a, "src/a.rs");
        assert_eq!(report.co_changes[0].file_b, "src/z.rs");
        assert_eq!(report.co_changes[0].occurrences, 10);
    }

    #[test]
    fn temporal_decay_reduces_old_co_change_strength() {
        let options = AnalyticsOptions {
            min_co_change_occurrences: 1,
            ..AnalyticsOptions::default()
        };

        let recent = analyze_history(
            &[commit_fact(
                180 * 86_400,
                vec![
                    delta(
                        "src/a.rs",
                        CommitFileChangeKind::Modified,
                        Some(1),
                        Some(1),
                        None,
                    ),
                    delta(
                        "src/b.rs",
                        CommitFileChangeKind::Modified,
                        Some(1),
                        Some(1),
                        None,
                    ),
                ],
            )],
            180 * 86_400,
            &options,
        );
        let old = analyze_history(
            &[commit_fact(
                0,
                vec![
                    delta(
                        "src/a.rs",
                        CommitFileChangeKind::Modified,
                        Some(1),
                        Some(1),
                        None,
                    ),
                    delta(
                        "src/b.rs",
                        CommitFileChangeKind::Modified,
                        Some(1),
                        Some(1),
                        None,
                    ),
                ],
            )],
            180 * 86_400,
            &options,
        );

        assert!(recent.co_changes[0].strength > old.co_changes[0].strength);
    }

    #[test]
    fn rename_history_normalizes_old_paths_to_latest_path() {
        let options = AnalyticsOptions {
            min_co_change_occurrences: 1,
            ..AnalyticsOptions::default()
        };

        let facts = vec![
            commit_fact(
                10,
                vec![
                    delta(
                        "src/old.rs",
                        CommitFileChangeKind::Modified,
                        Some(1),
                        Some(0),
                        None,
                    ),
                    delta(
                        "src/helper.rs",
                        CommitFileChangeKind::Modified,
                        Some(1),
                        Some(0),
                        None,
                    ),
                ],
            ),
            commit_fact(
                20,
                vec![delta(
                    "src/new.rs",
                    CommitFileChangeKind::Renamed,
                    Some(0),
                    Some(0),
                    Some("src/old.rs"),
                )],
            ),
            commit_fact(
                30,
                vec![
                    delta(
                        "src/new.rs",
                        CommitFileChangeKind::Modified,
                        Some(1),
                        Some(0),
                        None,
                    ),
                    delta(
                        "src/helper.rs",
                        CommitFileChangeKind::Modified,
                        Some(1),
                        Some(0),
                        None,
                    ),
                ],
            ),
        ];

        let report = analyze_history(&facts, 30, &options);
        assert!(
            report
                .hotspots
                .iter()
                .all(|record| record.file_path != "src/old.rs")
        );
        assert!(
            report
                .hotspots
                .iter()
                .any(|record| record.file_path == "src/new.rs")
        );
        assert_eq!(report.co_changes[0].file_a, "src/helper.rs");
        assert_eq!(report.co_changes[0].file_b, "src/new.rs");
        assert_eq!(report.co_changes[0].occurrences, 2);
    }

    #[test]
    fn deleted_files_do_not_produce_hotspots_or_co_change_pairs() {
        let options = AnalyticsOptions {
            min_co_change_occurrences: 1,
            ..AnalyticsOptions::default()
        };
        let report = analyze_history(
            &[commit_fact(
                10,
                vec![
                    delta(
                        "src/deleted.rs",
                        CommitFileChangeKind::Deleted,
                        Some(4),
                        Some(4),
                        None,
                    ),
                    delta(
                        "src/live.rs",
                        CommitFileChangeKind::Modified,
                        Some(2),
                        Some(1),
                        None,
                    ),
                ],
            )],
            10,
            &options,
        );

        assert_eq!(report.hotspots.len(), 1);
        assert_eq!(report.hotspots[0].file_path, "src/live.rs");
        assert!(report.co_changes.is_empty());
    }

    fn commit_fact(author_date_unix: i64, file_deltas: Vec<CommitFileDelta>) -> CommitFact {
        CommitFact {
            repo: "service-a".to_owned(),
            sha: format!("sha-{author_date_unix}"),
            author_email: "alice@example.com".to_owned(),
            author_date_unix,
            message: "feat: x".to_owned(),
            classification: Some("feat".to_owned()),
            pr_number: None,
            has_decision_signal: false,
            parent_count: 1,
            file_deltas,
        }
    }

    fn delta(
        file_path: &str,
        change_kind: CommitFileChangeKind,
        insertions: Option<u64>,
        deletions: Option<u64>,
        old_path: Option<&str>,
    ) -> CommitFileDelta {
        CommitFileDelta {
            file_path: file_path.to_owned(),
            change_kind,
            insertions,
            deletions,
            old_path: old_path.map(str::to_owned),
        }
    }
}
