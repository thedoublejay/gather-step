#![expect(clippy::print_stdout, reason = "diagnostic example emits TSV output")]
#![expect(
    clippy::print_stderr,
    reason = "diagnostic example emits progress output"
)]

/// Exhaustive full-graph diagnostic: counts every cross-repo edge by bucket
/// and `EdgeKind`.
///
/// Buckets:
///   `true_cross_repo`   — both endpoints in real (non-virtual) repos, neither
///                        node is virtual.
///   `virtual_ownership` — target.repo == "__virtual__" && target.kind == Author.
///   `virtual_other`     — any other __virtual__-terminating edge.
///
/// Usage:
///   cargo run --release -p gather-step-storage \
///     --example `count_cross_repo_edges_by_kind` \
///     --features gather-step-storage/test-support \
///     -- --storage <path-to-storage-dir>
use std::{collections::BTreeMap, path::PathBuf};

use gather_step_core::{EdgeKind, NodeKind};
use gather_step_storage::GraphStoreDb;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const VIRTUAL_NODE_REPO: &str = "__virtual__";

fn main() -> Result<(), BoxError> {
    // --- Argument parsing (no clap; match audit_cross_repo_edges style) ---
    let mut storage_dir: Option<PathBuf> = None;
    let mut args = std::env::args().skip(1).peekable();
    while let Some(arg) = args.next() {
        if arg == "--storage" {
            storage_dir = args.next().map(PathBuf::from);
        }
    }
    let storage_dir = storage_dir.unwrap_or_else(|| PathBuf::from("."));
    let graph_path = storage_dir.join("graph.redb");
    eprintln!("Opening {}", graph_path.display());

    let graph = GraphStoreDb::open(&graph_path)?;

    // Baseline from production counter for reconciliation.
    let production_count = graph.count_cross_repo_edges()?;
    eprintln!("count_cross_repo_edges() = {production_count}");

    // New split-metric production methods.
    let prod_true = graph.count_true_cross_repo_edges()?;
    let prod_own = graph.count_history_ownership_edges()?;
    let prod_voth = graph.count_virtual_other_cross_repo_edges()?;
    eprintln!("count_true_cross_repo_edges()         = {prod_true}");
    eprintln!("count_history_ownership_edges()        = {prod_own}");
    eprintln!("count_virtual_other_cross_repo_edges() = {prod_voth}");
    eprintln!(
        "split sum vs production: {} vs {} ({})",
        prod_true + prod_own + prod_voth,
        production_count,
        if u64::try_from(prod_true + prod_own + prod_voth).ok() == Some(production_count) {
            "matches"
        } else {
            "MISMATCH"
        }
    );

    // Full-graph scan via test-support helper.
    let all_edges = graph.all_edges_attributed()?;
    eprintln!("Total edges in graph: {}", all_edges.len());

    // Counters: BTreeMap for stable output ordering.
    let mut true_cross_repo: BTreeMap<EdgeKind, u64> = BTreeMap::new();
    let mut virtual_ownership: BTreeMap<EdgeKind, u64> = BTreeMap::new();
    let mut virtual_other: BTreeMap<(EdgeKind, NodeKind), u64> = BTreeMap::new();
    let mut skipped_same_repo: u64 = 0;

    for (edge, src_repo, _src_virt, _src_kind, tgt_repo, tgt_virt, tgt_kind) in &all_edges {
        if src_repo == tgt_repo {
            skipped_same_repo += 1;
            continue;
        }
        if tgt_repo == VIRTUAL_NODE_REPO {
            if *tgt_kind == NodeKind::Author {
                *virtual_ownership.entry(edge.kind).or_default() += 1;
            } else {
                *virtual_other.entry((edge.kind, *tgt_kind)).or_default() += 1;
            }
        } else if !tgt_virt {
            // Both repos are real and target is not virtual.
            *true_cross_repo.entry(edge.kind).or_default() += 1;
        } else {
            // tgt_repo != __virtual__ but tgt_is_virtual == true:
            // treat as true_cross_repo (still a real repo pair).
            *true_cross_repo.entry(edge.kind).or_default() += 1;
        }
    }

    eprintln!("Skipped same-repo edges: {skipped_same_repo}");

    // --- TSV output ---
    println!("bucket\tedge_kind\tcount");
    for (kind, count) in &true_cross_repo {
        println!("true_cross_repo\t{kind:?}\t{count}");
    }
    for (kind, count) in &virtual_ownership {
        println!("virtual_ownership\t{kind:?}\t{count}");
    }
    for ((kind, tgt_kind), count) in &virtual_other {
        println!("virtual_other[tgt={tgt_kind:?}]\t{kind:?}\t{count}");
    }

    let total_true: u64 = true_cross_repo.values().sum();
    let total_vown: u64 = virtual_ownership.values().sum();
    let total_voth: u64 = virtual_other.values().sum();
    let grand_total = total_true + total_vown + total_voth;

    println!();
    println!("TOTAL cross_repo_edges: {grand_total}");
    println!();

    eprintln!("\n--- Totals ---");
    eprintln!("  true_cross_repo   = {total_true}");
    eprintln!("  virtual_ownership = {total_vown}");
    eprintln!("  virtual_other     = {total_voth}");
    eprintln!("  ---");
    eprintln!("  sum               = {grand_total}");
    eprintln!(
        "  count_cross_repo_edges = {production_count}  (reconciliation: {})",
        if grand_total == production_count {
            "matches".to_owned()
        } else {
            let diff = i128::from(grand_total) - i128::from(production_count);
            format!("off by {diff:+}")
        }
    );

    Ok(())
}
