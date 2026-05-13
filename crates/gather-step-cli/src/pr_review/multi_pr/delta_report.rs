//! Output model for coordinated PR-set reviews.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    path::PathBuf,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    cache_key::{PrSetCacheKey, ResolvedPrCacheEntry},
    manifest::PrSetManifest,
};

pub const MULTI_PR_DELTA_REPORT_SCHEMA_VERSION: u32 = 0;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiPrDeltaReport {
    pub schema_version: u32,
    pub metadata: MultiPrMetadata,
    pub prs: Vec<PerPrDeltaReport>,
    pub errors: Vec<ErroredPrReview>,
    pub cross_pr: CrossPrFindings,
    pub threshold_exceeded: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiPrMetadata {
    pub set_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub manifest_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_path: Option<PathBuf>,
    pub total_prs: usize,
    pub completed_prs: usize,
    pub failed_prs: usize,
    pub skipped_prs: usize,
    pub set_fingerprint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerPrDeltaReport {
    pub id: String,
    pub repo: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pr: Option<u64>,
    pub base: String,
    pub head: String,
    pub threshold_exceeded: bool,
    pub delta_report: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErroredPrReview {
    pub id: String,
    pub repo: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pr: Option<u64>,
    pub base: String,
    pub head: String,
    pub status: PrReviewSetEntryStatus,
    pub message: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PrReviewSetEntryStatus {
    Failed,
    Skipped,
}

impl PrReviewSetEntryStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Failed => "failed",
            Self::Skipped => "skipped",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CrossPrFindings {
    pub contract_drifts: Vec<CrossPrContractDrift>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrossPrContractDrift {
    pub identity: String,
    pub producer_prs: Vec<String>,
    pub consumer_prs: Vec<String>,
    pub severity: CrossPrSeverity,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum CrossPrSeverity {
    Low,
    Medium,
    High,
}

impl CrossPrSeverity {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

impl MultiPrDeltaReport {
    #[must_use]
    pub fn from_parts(
        manifest: &PrSetManifest,
        manifest_path: Option<PathBuf>,
        prs: Vec<PerPrDeltaReport>,
        errors: Vec<ErroredPrReview>,
        threshold_exceeded: bool,
    ) -> Self {
        let set_fingerprint = set_fingerprint_for_report(manifest, &prs);
        let skipped_prs = errors
            .iter()
            .filter(|error| error.status == PrReviewSetEntryStatus::Skipped)
            .count();
        let failed_prs = errors.len().saturating_sub(skipped_prs);
        let cross_pr = CrossPrFindings::from_prs(&prs);

        Self {
            schema_version: MULTI_PR_DELTA_REPORT_SCHEMA_VERSION,
            metadata: MultiPrMetadata {
                set_id: manifest.id.clone(),
                title: manifest.title.clone(),
                manifest_version: manifest.version,
                manifest_path,
                total_prs: manifest.prs.len(),
                completed_prs: prs.len(),
                failed_prs,
                skipped_prs,
                set_fingerprint,
            },
            prs,
            errors,
            cross_pr,
            threshold_exceeded,
        }
    }

    pub fn render_json(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    #[must_use]
    pub fn render_markdown(&self) -> String {
        let mut buf = String::new();
        let title = self
            .metadata
            .title
            .as_deref()
            .unwrap_or(self.metadata.set_id.as_str());
        let _ = writeln!(buf, "# Gather Step PR-set review: {title}\n");
        let _ = writeln!(buf, "- Set id: `{}`", self.metadata.set_id);
        let _ = writeln!(
            buf,
            "- PRs: {} completed, {} failed, {} skipped, {} total",
            self.metadata.completed_prs,
            self.metadata.failed_prs,
            self.metadata.skipped_prs,
            self.metadata.total_prs
        );
        let _ = writeln!(
            buf,
            "- Set fingerprint: `{}`",
            self.metadata.set_fingerprint
        );
        if self.threshold_exceeded {
            let _ = writeln!(buf, "- Severity threshold: exceeded");
        } else {
            let _ = writeln!(buf, "- Severity threshold: not exceeded");
        }

        self.render_cross_pr_section(&mut buf);
        self.render_errors_section(&mut buf);
        self.render_pr_summaries(&mut buf);

        buf
    }

    #[must_use]
    pub fn render_github_comment(&self, limit: usize) -> String {
        truncate_to_limit(self.render_markdown(), limit)
    }

    #[must_use]
    pub fn render_braingent(&self) -> String {
        let mut buf = String::new();
        let _ = writeln!(buf, "---");
        let _ = writeln!(buf, "type: pr-review-set");
        let _ = writeln!(buf, "set_id: {}", self.metadata.set_id);
        let _ = writeln!(buf, "set_fingerprint: {}", self.metadata.set_fingerprint);
        let _ = writeln!(buf, "---\n");
        buf.push_str(&self.render_markdown());
        buf
    }

    fn render_cross_pr_section(&self, buf: &mut String) {
        let _ = writeln!(buf, "\n## Cross-PR findings\n");
        if self.cross_pr.contract_drifts.is_empty() {
            let _ = writeln!(buf, "No cross-PR payload contract drift found.");
            return;
        }

        let _ = writeln!(
            buf,
            "| Severity | Contract | Producer PRs | Consumer PRs | Reason |"
        );
        let _ = writeln!(buf, "|---|---|---|---|---|");
        for drift in &self.cross_pr.contract_drifts {
            let consumer_prs = if drift.consumer_prs.is_empty() {
                "none".to_owned()
            } else {
                drift.consumer_prs.join(", ")
            };
            let _ = writeln!(
                buf,
                "| {} | `{}` | {} | {} | {} |",
                drift.severity.as_str(),
                drift.identity,
                drift.producer_prs.join(", "),
                consumer_prs,
                drift.reason
            );
        }
    }

    fn render_errors_section(&self, buf: &mut String) {
        if self.errors.is_empty() {
            return;
        }

        let _ = writeln!(buf, "\n## Failed or skipped PRs\n");
        let _ = writeln!(buf, "| Status | PR | Repo | Range | Message |");
        let _ = writeln!(buf, "|---|---|---|---|---|");
        for error in &self.errors {
            let pr = error
                .pr
                .map_or_else(|| error.id.clone(), |pr| format!("#{pr}"));
            let _ = writeln!(
                buf,
                "| {} | {} | `{}` | `{}`..`{}` | {} |",
                error.status.as_str(),
                pr,
                error.repo,
                error.base,
                error.head,
                error.message
            );
        }
    }

    fn render_pr_summaries(&self, buf: &mut String) {
        let _ = writeln!(buf, "\n## Per-PR summaries\n");
        if self.prs.is_empty() {
            let _ = writeln!(buf, "No PRs completed successfully.");
            return;
        }

        let _ = writeln!(
            buf,
            "| PR | Repo | Range | Changed files | Routes | Symbols | Payload contracts | Events | Deployment |"
        );
        let _ = writeln!(buf, "|---|---|---|---:|---:|---:|---:|---:|---:|");
        for pr in &self.prs {
            let label = pr
                .pr
                .map_or_else(|| pr.id.clone(), |number| format!("#{number}"));
            let counts = ReportCounts::from_report(&pr.delta_report);
            let _ = writeln!(
                buf,
                "| {} | `{}` | `{}`..`{}` | {} | {} | {} | {} | {} | {} |",
                label,
                pr.repo,
                pr.base,
                pr.head,
                counts.changed_files,
                counts.routes,
                counts.symbols,
                counts.payload_contracts,
                counts.events,
                counts.deployment
            );
        }
    }
}

impl CrossPrFindings {
    #[must_use]
    pub fn from_prs(prs: &[PerPrDeltaReport]) -> Self {
        let mut producer_mentions: BTreeMap<String, ContractMentionAccumulator> = BTreeMap::new();
        let mut consumer_mentions: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

        for pr in prs {
            for mention in contract_mentions_for_pr(pr) {
                if mention.side == ContractSide::Producer {
                    let entry = producer_mentions
                        .entry(mention.identity.clone())
                        .or_default();
                    entry.pr_ids.insert(pr.id.clone());
                    entry.severity = entry.severity.max(mention.severity);
                } else {
                    consumer_mentions
                        .entry(mention.identity)
                        .or_default()
                        .insert(pr.id.clone());
                }
            }
        }

        let mut contract_drifts = Vec::new();
        for (identity, producer) in producer_mentions {
            let consumer_prs: BTreeSet<String> = consumer_mentions
                .get(&identity)
                .cloned()
                .unwrap_or_default();
            let covered_elsewhere = producer.pr_ids.iter().any(|producer_pr| {
                consumer_prs
                    .iter()
                    .any(|consumer_pr| consumer_pr != producer_pr)
            });
            if covered_elsewhere {
                continue;
            }

            contract_drifts.push(CrossPrContractDrift {
                identity: identity.clone(),
                producer_prs: producer.pr_ids.into_iter().collect(),
                consumer_prs: consumer_prs.into_iter().collect(),
                severity: producer.severity,
                reason: if producer.severity >= CrossPrSeverity::High {
                    "Producer payload contract changed in a breaking direction without a matching consumer PR in this set.".to_owned()
                } else {
                    "Producer payload contract changed without a matching consumer PR in this set.".to_owned()
                },
            });
        }

        Self { contract_drifts }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContractSide {
    Producer,
    Consumer,
}

#[derive(Debug, Clone)]
struct ContractMention {
    identity: String,
    side: ContractSide,
    severity: CrossPrSeverity,
}

#[derive(Debug, Clone)]
struct ContractMentionAccumulator {
    pr_ids: BTreeSet<String>,
    severity: CrossPrSeverity,
}

impl Default for ContractMentionAccumulator {
    fn default() -> Self {
        Self {
            pr_ids: BTreeSet::new(),
            severity: CrossPrSeverity::Low,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ReportCounts {
    changed_files: usize,
    routes: usize,
    symbols: usize,
    payload_contracts: usize,
    events: usize,
    deployment: usize,
}

impl ReportCounts {
    fn from_report(report: &Value) -> Self {
        Self {
            changed_files: value_array(report, &["changed_files"]).len(),
            routes: surface_count(report, "routes"),
            symbols: surface_count(report, "symbols"),
            payload_contracts: surface_count(report, "payload_contracts"),
            events: surface_count(report, "events"),
            deployment: deployment_count(report),
        }
    }
}

fn contract_mentions_for_pr(pr: &PerPrDeltaReport) -> Vec<ContractMention> {
    let mut mentions = Vec::new();
    for bucket in ["added", "removed"] {
        for item in value_array(&pr.delta_report, &["payload_contracts", bucket]) {
            if let Some(mention) = contract_mention_from_value(item, bucket) {
                mentions.push(mention);
            }
        }
    }
    for item in value_array(&pr.delta_report, &["payload_contracts", "changed"]) {
        if let Some(mention) = contract_mention_from_value(item, "changed") {
            mentions.push(mention);
        }
    }
    mentions
}

fn contract_mention_from_value(item: &Value, bucket: &str) -> Option<ContractMention> {
    let identity = item.get("target_qualified_name")?.as_str()?.to_owned();
    let side = match item.get("side").and_then(Value::as_str) {
        Some("producer") => ContractSide::Producer,
        Some("consumer") => ContractSide::Consumer,
        _ => return None,
    };
    let severity = match side {
        ContractSide::Consumer => CrossPrSeverity::Low,
        ContractSide::Producer => contract_change_severity(item, bucket),
    };
    Some(ContractMention {
        identity,
        side,
        severity,
    })
}

fn contract_change_severity(item: &Value, bucket: &str) -> CrossPrSeverity {
    match bucket {
        "removed" => CrossPrSeverity::High,
        "added" => CrossPrSeverity::Medium,
        "changed" => {
            let breaking = !value_array(item, &["fields_removed"]).is_empty()
                || !value_array(item, &["fields_optional_to_required"]).is_empty()
                || !value_array(item, &["fields_type_changed"]).is_empty();
            if breaking {
                CrossPrSeverity::High
            } else if !value_array(item, &["fields_added"]).is_empty()
                || !value_array(item, &["fields_required_to_optional"]).is_empty()
            {
                CrossPrSeverity::Medium
            } else {
                CrossPrSeverity::Low
            }
        }
        _ => CrossPrSeverity::Low,
    }
}

fn surface_count(report: &Value, surface: &str) -> usize {
    ["added", "removed", "changed"]
        .into_iter()
        .map(|bucket| value_array(report, &[surface, bucket]).len())
        .sum()
}

fn deployment_count(report: &Value) -> usize {
    let Some(object) = report.get("deployment").and_then(Value::as_object) else {
        return 0;
    };
    object
        .iter()
        .filter(|(key, _)| key.as_str() != "unavailable")
        .map(|(_, value)| value.as_array().map_or(0, Vec::len))
        .sum()
}

fn value_array<'a>(value: &'a Value, path: &[&str]) -> Vec<&'a Value> {
    let mut cursor = value;
    for segment in path {
        let Some(next) = cursor.get(*segment) else {
            return Vec::new();
        };
        cursor = next;
    }
    cursor
        .as_array()
        .map(|items| items.iter().collect())
        .unwrap_or_default()
}

fn set_fingerprint_for_report(manifest: &PrSetManifest, prs: &[PerPrDeltaReport]) -> String {
    let entries = prs
        .iter()
        .map(|pr| ResolvedPrCacheEntry {
            id: pr.id.clone(),
            repo: pr.repo.clone(),
            workspace_hash: pr
                .delta_report
                .get("safety")
                .and_then(|safety| safety.get("cache_key"))
                .and_then(Value::as_str)
                .and_then(|cache_key| cache_key.split(':').next())
                .unwrap_or_default()
                .to_owned(),
            base_sha: pr
                .delta_report
                .get("metadata")
                .and_then(|metadata| metadata.get("base_sha"))
                .and_then(Value::as_str)
                .unwrap_or(pr.base.as_str())
                .to_owned(),
            head_sha: pr
                .delta_report
                .get("metadata")
                .and_then(|metadata| metadata.get("head_sha"))
                .and_then(Value::as_str)
                .unwrap_or(pr.head.as_str())
                .to_owned(),
            config_hash: pr
                .delta_report
                .get("safety")
                .and_then(|safety| safety.get("config_hash"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            gather_step_version: env!("CARGO_PKG_VERSION").to_owned(),
        })
        .collect();

    PrSetCacheKey {
        manifest_id: manifest.id.clone(),
        manifest_version: manifest.version,
        entries,
    }
    .fingerprint()
}

fn truncate_to_limit(mut value: String, limit: usize) -> String {
    if value.len() <= limit {
        return value;
    }
    let suffix = "\n\n_Report truncated to fit the GitHub comment limit._";
    if limit <= suffix.len() {
        value.truncate(limit);
        return value;
    }
    let mut target = limit - suffix.len();
    while target > 0 && !value.is_char_boundary(target) {
        target -= 1;
    }
    format!("{}{}", &value[..target], suffix)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        CrossPrFindings, CrossPrSeverity, MultiPrDeltaReport, PerPrDeltaReport,
        PrReviewSetEntryStatus,
    };
    use crate::pr_review::multi_pr::manifest::{PrEntry, PrSetManifest};

    fn pr(id: &str, side: &str) -> PerPrDeltaReport {
        pr_with_config_hash(id, side, "cfg")
    }

    fn pr_with_config_hash(id: &str, side: &str, config_hash: &str) -> PerPrDeltaReport {
        PerPrDeltaReport {
            id: id.to_owned(),
            repo: id.to_owned(),
            pr: None,
            base: "main".to_owned(),
            head: format!("feature/{id}"),
            threshold_exceeded: false,
            delta_report: json!({
                "metadata": {
                    "base_sha": "1111111111111111111111111111111111111111",
                    "head_sha": "2222222222222222222222222222222222222222"
                },
                "safety": {
                    "cache_key": "workspace:base:head",
                    "config_hash": config_hash
                },
                "changed_files": ["src/app.ts"],
                "routes": { "added": [], "removed": [], "changed": [] },
                "symbols": { "added": [], "removed": [], "changed": [] },
                "payload_contracts": {
                    "added": [],
                    "removed": [],
                    "changed": [{
                        "repo": id,
                        "file": "src/payload.ts",
                        "target_qualified_name": "UpdateLabelProject",
                        "side": side,
                        "fields_added": [],
                        "fields_removed": [{ "name": "name", "type_name": "string", "optional": false }],
                        "fields_optional_to_required": [],
                        "fields_required_to_optional": [],
                        "fields_type_changed": []
                    }]
                },
                "events": { "added": [], "removed": [], "changed": [] },
                "deployment": {}
            }),
        }
    }

    #[test]
    fn cross_pr_drift_flags_producer_without_consumer_pr() {
        let findings = CrossPrFindings::from_prs(&[pr("api", "producer")]);

        assert_eq!(findings.contract_drifts.len(), 1);
        assert_eq!(findings.contract_drifts[0].severity, CrossPrSeverity::High);
    }

    #[test]
    fn cross_pr_drift_is_covered_by_other_consumer_pr() {
        let findings = CrossPrFindings::from_prs(&[pr("api", "producer"), pr("web", "consumer")]);

        assert!(findings.contract_drifts.is_empty());
    }

    #[test]
    fn multi_pr_report_tracks_failed_and_skipped_counts() {
        let manifest = PrSetManifest {
            version: 0,
            id: "checkout-refresh".to_owned(),
            title: None,
            prs: vec![
                PrEntry {
                    id: "api".to_owned(),
                    repo: "api".to_owned(),
                    base: "main".to_owned(),
                    head: "feature/api".to_owned(),
                    pr: Some(1),
                    depends_on: vec![],
                },
                PrEntry {
                    id: "web".to_owned(),
                    repo: "web".to_owned(),
                    base: "main".to_owned(),
                    head: "feature/web".to_owned(),
                    pr: Some(2),
                    depends_on: vec!["api".to_owned()],
                },
            ],
        };
        let report = MultiPrDeltaReport::from_parts(
            &manifest,
            None,
            vec![pr("api", "producer")],
            vec![super::ErroredPrReview {
                id: "web".to_owned(),
                repo: "web".to_owned(),
                pr: Some(2),
                base: "main".to_owned(),
                head: "feature/web".to_owned(),
                status: PrReviewSetEntryStatus::Skipped,
                message: "dependency failed".to_owned(),
            }],
            false,
        );

        assert_eq!(report.metadata.completed_prs, 1);
        assert_eq!(report.metadata.failed_prs, 0);
        assert_eq!(report.metadata.skipped_prs, 1);
        let markdown = report.render_markdown();
        assert!(markdown.contains("Cross-PR findings"));
        assert!(
            markdown.contains("| skipped | #2 |"),
            "markdown should use the stable serialized status label: {markdown}"
        );
    }

    #[test]
    fn set_fingerprint_includes_child_config_hash() {
        let manifest = PrSetManifest {
            version: 0,
            id: "checkout-refresh".to_owned(),
            title: None,
            prs: vec![PrEntry {
                id: "api".to_owned(),
                repo: "api".to_owned(),
                base: "main".to_owned(),
                head: "feature/api".to_owned(),
                pr: Some(1),
                depends_on: vec![],
            }],
        };

        let first = super::set_fingerprint_for_report(
            &manifest,
            &[pr_with_config_hash("api", "producer", "cfg-a")],
        );
        let second = super::set_fingerprint_for_report(
            &manifest,
            &[pr_with_config_hash("api", "producer", "cfg-b")],
        );

        assert_ne!(
            first, second,
            "set fingerprint must change when a child review config hash changes"
        );
    }
}
