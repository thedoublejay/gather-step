//! Shared diff machinery for the pr-review delta extractors.
//!
//! Every surface extractor (`routes`, `symbols`, `events`, `decorators`,
//! `payload_contracts`, `ai_contracts`) builds a baseline map and a review map
//! keyed by a stable identity, then partitions the keys into added (review
//! only), removed (baseline only), and common (both). [`three_way_diff`]
//! performs that partition once so each extractor only supplies its own
//! change-detection logic for the common keys.

use std::hash::Hash;

use rustc_hash::FxHashMap;

/// Result of partitioning a baseline map and a review map by key.
///
/// - `added` — values whose key is present only in the review map.
/// - `removed` — values whose key is present only in the baseline map.
/// - `common` — `(key, baseline_value, review_value)` for keys in both maps.
///   The caller decides whether a common pair counts as "changed".
pub struct ThreeWayDiff<K, T> {
    pub added: Vec<T>,
    pub removed: Vec<T>,
    pub common: Vec<(K, T, T)>,
}

/// Partition `baseline` and `review` into added / removed / common entries.
///
/// Iteration order is unspecified (the inputs are hash maps); callers sort the
/// resulting lists to obtain deterministic output.
pub fn three_way_diff<K, T>(
    baseline: FxHashMap<K, T>,
    mut review: FxHashMap<K, T>,
) -> ThreeWayDiff<K, T>
where
    K: Eq + Hash + Clone,
{
    let mut added: Vec<T> = Vec::new();
    let mut removed: Vec<T> = Vec::new();
    let mut common: Vec<(K, T, T)> = Vec::new();

    for (key, baseline_value) in baseline {
        match review.remove(&key) {
            Some(review_value) => common.push((key, baseline_value, review_value)),
            None => removed.push(baseline_value),
        }
    }

    // Whatever remains in `review` had no baseline counterpart.
    added.extend(review.into_values());

    ThreeWayDiff {
        added,
        removed,
        common,
    }
}
