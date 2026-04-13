#![expect(clippy::print_stdout, reason = "diagnostic example emits sampled rows")]
#![expect(
    clippy::print_stderr,
    reason = "diagnostic example emits audit progress"
)]

/// One-shot audit helper: opens an existing graph.redb and dumps sampled
/// cross-repo edges with enough detail for ground-truth spot-checking.
///
/// Usage:
///   cargo run -p gather-step-storage \
///     --example `audit_cross_repo_edges` -- <path-to-storage-dir> [sample-size]
///
/// Gated so it can never be imported as a library surface.
use std::path::PathBuf;

use gather_step_core::{EdgeKind, NodeId};
use gather_step_storage::GraphStoreDb;
use rustc_hash::FxHashMap;

use gather_step_storage::GraphStore;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

fn percent(numerator: usize, denominator: usize) -> f64 {
    let numerator = u32::try_from(numerator).unwrap_or(u32::MAX);
    let denominator = u32::try_from(denominator.max(1)).unwrap_or(u32::MAX);
    100.0 * f64::from(numerator) / f64::from(denominator)
}

fn main() -> Result<(), BoxError> {
    let mut args = std::env::args().skip(1);
    let storage_dir = args
        .next()
        .map_or_else(|| PathBuf::from("."), PathBuf::from);
    let sample_size: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(30);

    let graph_path = storage_dir.join("graph.redb");
    eprintln!("Opening {}", graph_path.display());
    let graph = GraphStoreDb::open(&graph_path)?;

    let total = graph.count_cross_repo_edges()?;
    eprintln!("Total cross-repo edges: {total}");

    // We need to iterate all edges ourselves to decode source/target repos.
    // Re-use the same EDGES table approach as count_cross_repo_edges.
    // We access this via the public API: get_node after collecting edge IDs
    // via the raw scan. Since there is no public "iter_all_edges", we replicate
    // the logic using the redb file directly — but that would require internal
    // access. Instead we use the trait API: for every NodeKind::File node across
    // all repos, pull outgoing edges.
    //
    // The full graph has ~270K nodes. We want a representative sample without
    // full traversal. Strategy: collect up to sample_size * 10 file nodes from
    // each repo (capped), then scan their outgoing edges for cross-repo ones.

    // Collect all nodes (using nodes_by_repo is repo-specific; use the public
    // GraphStore trait methods available). We call nodes_by_repo("__virtual__")
    // to also list virtual nodes for completeness.

    // Build a node cache: NodeId -> (repo, file_path, kind)
    let mut node_cache: FxHashMap<NodeId, (String, String, gather_step_core::NodeKind)> =
        FxHashMap::default();

    // We need all repos. Inspect registry.json if present in the same dir.
    let registry_path = storage_dir
        .parent()
        .unwrap_or(&storage_dir)
        .join("registry.json");
    let registry_path2 = storage_dir.join("../registry.json");

    let registry_text = std::fs::read_to_string(&registry_path)
        .or_else(|_| std::fs::read_to_string(&registry_path2))
        .unwrap_or_default();

    let mut repo_names: Vec<String> = if registry_text.is_empty() {
        vec![]
    } else {
        let v: serde_json::Value = serde_json::from_str(&registry_text)?;
        // Registry "repos" can be either an object (name -> info) or an array.
        if let Some(obj) = v["repos"].as_object() {
            obj.keys().cloned().collect()
        } else if let Some(arr) = v["repos"].as_array() {
            arr.iter()
                .filter_map(|r| r["name"].as_str().map(str::to_owned))
                .collect()
        } else {
            vec![]
        }
    };
    repo_names.push("__virtual__".to_owned());
    eprintln!("Repos found in registry: {}", repo_names.len());

    for repo in &repo_names {
        let nodes = graph.nodes_by_repo(repo)?;
        for node in nodes {
            node_cache.insert(
                node.id,
                (node.repo.clone(), node.file_path.clone(), node.kind),
            );
        }
    }
    eprintln!("Node cache populated: {} entries", node_cache.len());

    // Collect cross-repo edges by scanning outgoing edges for every file node.
    let mut cross_edges: Vec<(String, String, String, String, EdgeKind, Option<String>)> = vec![];

    for (node_id, (src_repo, src_path, _kind)) in &node_cache {
        if src_repo == "__virtual__" {
            continue;
        }
        let outgoing = graph.get_outgoing(*node_id)?;
        for edge in outgoing {
            if edge.kind == EdgeKind::CrossRepoDepends {
                let tgt = node_cache.get(&edge.target).map_or_else(
                    || ("?".into(), "?".into()),
                    |(r, p, _)| (r.clone(), p.clone()),
                );
                cross_edges.push((
                    src_repo.clone(),
                    src_path.clone(),
                    tgt.0,
                    tgt.1,
                    edge.kind,
                    edge.metadata.resolver.clone(),
                ));
            } else if let Some((tgt_repo, tgt_path, _)) = node_cache.get(&edge.target)
                && tgt_repo != src_repo
            {
                cross_edges.push((
                    src_repo.clone(),
                    src_path.clone(),
                    tgt_repo.clone(),
                    tgt_path.clone(),
                    edge.kind,
                    edge.metadata.resolver.clone(),
                ));
            }
        }
        if cross_edges.len() >= sample_size * 2000 {
            // collected enough to sample from
            break;
        }
    }

    eprintln!(
        "Cross-repo edges collected for sampling: {}",
        cross_edges.len()
    );

    // Sample evenly across the collected set.
    let step = if cross_edges.len() > sample_size {
        cross_edges.len() / sample_size
    } else {
        1
    };

    println!("src_repo\tsrc_file\ttgt_repo\ttgt_file\tedge_kind\tresolver");
    let mut printed = 0;
    for (i, (sr, sf, tr, tf, ek, resolver)) in cross_edges.iter().enumerate() {
        if i % step == 0 && printed < sample_size {
            println!(
                "{sr}\t{sf}\t{tr}\t{tf}\t{ek}\t{}",
                resolver.as_deref().unwrap_or("")
            );
            printed += 1;
        }
    }

    // Print edge-kind breakdown of the full cross-repo set.
    eprintln!(
        "\n--- Edge-kind breakdown of sampled cross-repo set ({} edges) ---",
        cross_edges.len()
    );
    let mut kind_counts: FxHashMap<String, usize> = FxHashMap::default();
    for (_, _, _, _, ek, _) in &cross_edges {
        *kind_counts.entry(ek.to_string()).or_default() += 1;
    }
    let mut kinds: Vec<_> = kind_counts.into_iter().collect();
    kinds.sort_by(|a, b| b.1.cmp(&a.1));
    for (k, c) in &kinds {
        eprintln!("  {k}: {c}");
    }

    // Print analytics attribution.
    let history_analytics_count = cross_edges
        .iter()
        .filter(|(_, _, _, _, _, r)| {
            r.as_deref()
                .is_some_and(|resolver| matches!(resolver, "history_ownership" | "co_change"))
        })
        .count();
    let total_sampled = cross_edges.len();
    eprintln!(
        "\nhistory-analytics-derived: {history_analytics_count}/{total_sampled} ({:.1}%)",
        percent(history_analytics_count, total_sampled)
    );
    eprintln!(
        "Static/other:   {}/{total_sampled} ({:.1}%)",
        total_sampled - history_analytics_count,
        percent(total_sampled - history_analytics_count, total_sampled)
    );

    Ok(())
}
