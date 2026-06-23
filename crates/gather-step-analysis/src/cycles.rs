use gather_step_core::{EdgeKind, NodeKind};
use gather_step_storage::{GraphStore, GraphStoreError};
use rustc_hash::FxHashMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CycleError {
    #[error(transparent)]
    Store(#[from] GraphStoreError),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cycle {
    pub nodes: Vec<String>,
    pub repos: Vec<String>,
    pub cross_repo: bool,
}

pub fn find_cycles<S: GraphStore>(
    store: &S,
    edge_kinds: Option<&[EdgeKind]>,
) -> Result<Vec<Cycle>, CycleError> {
    let mut index_of: FxHashMap<[u8; 16], usize> = FxHashMap::default();
    let mut labels: Vec<String> = Vec::new();
    let mut repos: Vec<String> = Vec::new();
    let mut node_ids = Vec::new();
    for kind in NodeKind::all() {
        for node in store.nodes_by_type(*kind)? {
            let key = node.id.as_bytes();
            if index_of.contains_key(&key) {
                continue;
            }
            index_of.insert(key, labels.len());
            labels.push(
                node.qualified_name
                    .clone()
                    .unwrap_or_else(|| format!("{}::{}", node.repo, node.name)),
            );
            repos.push(node.repo.clone());
            node_ids.push(node.id);
        }
    }

    let count = node_ids.len();
    let mut adjacency: Vec<Vec<usize>> = vec![Vec::new(); count];
    let mut self_loop = vec![false; count];
    for (source_index, node_id) in node_ids.iter().enumerate() {
        for edge in store.get_outgoing(*node_id)? {
            if let Some(kinds) = edge_kinds
                && !kinds.contains(&edge.kind)
            {
                continue;
            }
            let Some(&target_index) = index_of.get(&edge.target.as_bytes()) else {
                continue;
            };
            if target_index == source_index {
                self_loop[source_index] = true;
            }
            adjacency[source_index].push(target_index);
        }
    }

    let sccs = tarjan(&adjacency);

    let mut cycles = Vec::new();
    for scc in sccs {
        let is_cycle = scc.len() > 1 || (scc.len() == 1 && self_loop[scc[0]]);
        if !is_cycle {
            continue;
        }
        let mut nodes: Vec<String> = scc.iter().map(|&i| labels[i].clone()).collect();
        nodes.sort();
        let mut cycle_repos: Vec<String> = scc.iter().map(|&i| repos[i].clone()).collect();
        cycle_repos.sort();
        cycle_repos.dedup();
        let cross_repo = cycle_repos.len() > 1;
        cycles.push(Cycle {
            nodes,
            repos: cycle_repos,
            cross_repo,
        });
    }
    cycles.sort_by(|left, right| left.nodes.cmp(&right.nodes));
    Ok(cycles)
}

fn tarjan(adjacency: &[Vec<usize>]) -> Vec<Vec<usize>> {
    let count = adjacency.len();
    let mut indices = vec![usize::MAX; count];
    let mut lowlink = vec![0_usize; count];
    let mut on_stack = vec![false; count];
    let mut stack: Vec<usize> = Vec::new();
    let mut sccs: Vec<Vec<usize>> = Vec::new();
    let mut next_index = 0_usize;

    for root in 0..count {
        if indices[root] != usize::MAX {
            continue;
        }
        let mut call_stack: Vec<(usize, usize)> = vec![(root, 0)];
        while let Some(&(node, child)) = call_stack.last() {
            if child == 0 {
                indices[node] = next_index;
                lowlink[node] = next_index;
                next_index += 1;
                stack.push(node);
                on_stack[node] = true;
            }

            if child < adjacency[node].len() {
                let top = call_stack.len() - 1;
                call_stack[top].1 += 1;
                let target = adjacency[node][child];
                if indices[target] == usize::MAX {
                    call_stack.push((target, 0));
                } else if on_stack[target] {
                    lowlink[node] = lowlink[node].min(indices[target]);
                }
            } else {
                if lowlink[node] == indices[node] {
                    let mut scc = Vec::new();
                    while let Some(member) = stack.pop() {
                        on_stack[member] = false;
                        scc.push(member);
                        if member == node {
                            break;
                        }
                    }
                    sccs.push(scc);
                }
                call_stack.pop();
                if let Some(&(parent, _)) = call_stack.last() {
                    lowlink[parent] = lowlink[parent].min(lowlink[node]);
                }
            }
        }
    }
    sccs
}

#[cfg(test)]
mod tests {
    use gather_step_core::{EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeId, NodeKind, node_id};
    use gather_step_storage::{GraphStore, GraphStoreDb};

    use super::find_cycles;
    use crate::test_utils::TempDb;

    fn func(repo: &str, name: &str) -> NodeData {
        NodeData {
            id: node_id(repo, "src/a.ts", NodeKind::Function, name),
            kind: NodeKind::Function,
            repo: repo.to_owned(),
            file_path: "src/a.ts".to_owned(),
            name: name.to_owned(),
            qualified_name: Some(format!("{repo}::{name}")),
            external_id: None,
            signature: None,
            visibility: None,
            span: None,
            is_virtual: false,
            ai_role: None,
        }
    }

    fn calls(source: NodeId, target: NodeId, owner: NodeId) -> EdgeData {
        EdgeData {
            source,
            target,
            kind: EdgeKind::Calls,
            metadata: EdgeMetadata::default(),
            owner_file: owner,
            is_cross_file: false,
        }
    }

    #[test]
    fn detects_a_simple_cycle_and_ignores_acyclic_chains() {
        let temp = TempDb::new("cycles", "simple");
        let store = GraphStoreDb::open(temp.path()).expect("store");
        let file = NodeData {
            kind: NodeKind::File,
            ..func("web", "src/a.ts")
        };
        let a = func("web", "a");
        let b = func("web", "b");
        let c = func("web", "c");
        let d = func("web", "d");
        store
            .bulk_insert(
                &[file.clone(), a.clone(), b.clone(), c.clone(), d.clone()],
                &[
                    calls(a.id, b.id, file.id),
                    calls(b.id, c.id, file.id),
                    calls(c.id, a.id, file.id),
                    calls(c.id, d.id, file.id),
                ],
            )
            .expect("write");

        let cycles = find_cycles(&store, Some(&[EdgeKind::Calls])).expect("cycles");
        assert_eq!(cycles.len(), 1, "expected one cycle, got {cycles:?}");
        assert_eq!(cycles[0].nodes, vec!["web::a", "web::b", "web::c"]);
        assert!(!cycles[0].cross_repo);
    }

    #[test]
    fn acyclic_graph_has_no_cycles() {
        let temp = TempDb::new("cycles", "acyclic");
        let store = GraphStoreDb::open(temp.path()).expect("store");
        let file = NodeData {
            kind: NodeKind::File,
            ..func("web", "src/a.ts")
        };
        let a = func("web", "a");
        let b = func("web", "b");
        store
            .bulk_insert(
                &[file.clone(), a.clone(), b.clone()],
                &[calls(a.id, b.id, file.id)],
            )
            .expect("write");

        assert!(
            find_cycles(&store, Some(&[EdgeKind::Calls]))
                .expect("cycles")
                .is_empty()
        );
    }

    #[test]
    fn flags_cross_repo_cycle() {
        let temp = TempDb::new("cycles", "cross-repo");
        let store = GraphStoreDb::open(temp.path()).expect("store");
        let file = NodeData {
            kind: NodeKind::File,
            ..func("web", "src/a.ts")
        };
        let a = func("web", "a");
        let b = func("api", "b");
        store
            .bulk_insert(
                &[file.clone(), a.clone(), b.clone()],
                &[calls(a.id, b.id, file.id), calls(b.id, a.id, file.id)],
            )
            .expect("write");

        let cycles = find_cycles(&store, Some(&[EdgeKind::Calls])).expect("cycles");
        assert_eq!(cycles.len(), 1);
        assert!(cycles[0].cross_repo);
        assert_eq!(cycles[0].repos, vec!["api", "web"]);
    }
}
