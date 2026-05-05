#![forbid(unsafe_code)]

use std::path::Path;

use serde::{Deserialize, Serialize};

/// Top-level thresholds loaded from `benchmark/thresholds.yaml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Thresholds {
    pub parsing_correctness: ParsingThresholds,
    pub graph_quality: GraphQualityThresholds,
    pub link_quality: LinkQualityThresholds,
    pub planning_oracle: PlanningOracleThresholds,
    pub latency: LatencyThresholds,
    pub memory: MemoryThresholds,
    pub storage: StorageThresholds,
}

/// Thresholds for parsing correctness.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsingThresholds {
    /// Minimum fraction of fixtures that must parse without error.  `1.0` means 100 %.
    pub pass_rate_min: f64,
}

/// Thresholds for graph node and edge recall/precision.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQualityThresholds {
    /// Minimum recall for extracted nodes versus the expected set.
    pub nodes_recall_min: f64,
    /// Minimum precision for extracted nodes versus the expected set.
    pub nodes_precision_min: f64,
    /// Minimum recall for extracted edges versus the expected set.
    pub edges_recall_min: f64,
    /// Minimum precision for extracted edges versus the expected set.
    pub edges_precision_min: f64,
}

/// Thresholds for cross-boundary link quality.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkQualityThresholds {
    /// Maximum number of expected repos that may be missing from results.
    pub missed_repos_max: usize,
    /// Maximum number of expected files that may be missing from results.
    pub missed_files_max: usize,
    /// Maximum number of result entries that are false positives.
    pub false_positives_max: usize,
    /// Minimum precision for edges that cross repo boundaries.
    pub cross_boundary_precision_min: f64,
}

/// Thresholds for planning-oracle aggregate metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanningOracleThresholds {
    /// Minimum fraction of scenarios that must pass.
    pub coverage_min: f64,
    /// Minimum fraction of scenarios whose top result is correct.
    pub top1_accuracy_min: f64,
    /// Minimum fraction of scenarios whose expected result appears in the top 3.
    pub top3_accuracy_min: f64,
    /// Minimum mean reciprocal rank across scenarios.
    pub mrr_min: f64,
    /// Minimum expected-file recall across scenarios.
    pub expected_file_recall_min: f64,
    /// Minimum expected-repo recall across scenarios when that metric is applicable.
    pub expected_repo_recall_min: f64,
    /// Maximum fraction of scenarios that may include forbidden files.
    pub forbidden_hit_rate_max: f64,
    /// Maximum fraction of scenarios that may return no items.
    pub empty_result_rate_max: f64,
    /// Maximum fraction of scenarios that may report unresolved gaps.
    pub unresolved_gap_rate_max: f64,
    /// Minimum event target resolution success rate when event scenarios are present.
    pub event_resolution_success_rate_min: f64,
    /// Minimum Kendall tau stability across repeated runs.
    pub stability_kendall_tau_min: f64,
}

/// API latency thresholds derived from criterion measurements.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyThresholds {
    /// Maximum allowed p50 latency in milliseconds.
    pub p50_ms_max: u64,
    /// Maximum allowed p95 latency in milliseconds.
    pub p95_ms_max: u64,
    /// Maximum allowed p99 latency in milliseconds.
    pub p99_ms_max: u64,
}

/// Memory / RSS regression thresholds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryThresholds {
    /// Maximum allowed RSS growth as a fraction of baseline (e.g. `0.10` = 10 %).
    pub rss_growth_max_fraction: f64,
    /// Maximum allowed absolute RSS in bytes (default 1 GiB = 1 073 741 824).
    pub rss_absolute_max_bytes: u64,
}

/// On-disk storage regression thresholds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageThresholds {
    /// Maximum allowed graph store size in bytes.
    pub graph_bytes_max: u64,
    /// Maximum allowed `SQLite` metadata file size in bytes.
    pub metadata_bytes_max: u64,
    /// Maximum allowed Tantivy search index size in bytes.
    pub search_bytes_max: u64,
    /// Maximum allowed total generated storage size in bytes.
    pub total_bytes_max: u64,
}

impl Thresholds {
    /// Load thresholds from a YAML file at `path`.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read or when the YAML is
    /// malformed.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)?;
        let parsed = serde_norway::from_str(&raw)?;
        Ok(parsed)
    }

    /// Return the default thresholds matching the values in
    /// `benchmark/thresholds.yaml`.
    #[must_use]
    pub fn default_thresholds() -> Self {
        Self {
            parsing_correctness: ParsingThresholds { pass_rate_min: 1.0 },
            graph_quality: GraphQualityThresholds {
                nodes_recall_min: 0.95,
                nodes_precision_min: 0.85,
                edges_recall_min: 0.95,
                edges_precision_min: 0.85,
            },
            link_quality: LinkQualityThresholds {
                missed_repos_max: 1,
                missed_files_max: 3,
                false_positives_max: 5,
                cross_boundary_precision_min: 0.85,
            },
            planning_oracle: PlanningOracleThresholds {
                coverage_min: 1.0,
                top1_accuracy_min: 1.0,
                top3_accuracy_min: 1.0,
                mrr_min: 1.0,
                expected_file_recall_min: 1.0,
                expected_repo_recall_min: 1.0,
                forbidden_hit_rate_max: 0.0,
                empty_result_rate_max: 0.0,
                unresolved_gap_rate_max: 0.30,
                event_resolution_success_rate_min: 1.0,
                stability_kendall_tau_min: 0.999,
            },
            latency: LatencyThresholds {
                p50_ms_max: 50,
                p95_ms_max: 300,
                p99_ms_max: 1000,
            },
            memory: MemoryThresholds {
                rss_growth_max_fraction: 0.10,
                rss_absolute_max_bytes: 1_073_741_824,
            },
            storage: StorageThresholds {
                graph_bytes_max: 800_000,
                metadata_bytes_max: 1_500_000,
                search_bytes_max: 50_000,
                total_bytes_max: 3_500_000,
            },
        }
    }
}
