// use log::{info};
use petgraph::graphmap::DiGraphMap;
use petgraph::algo::kosaraju_scc;
use smallbank::SmallBankTransactionHandler;
use std::collections::{HashMap, HashSet, VecDeque};
use log::info;

type Transaction = Vec<u8>;
type Node = u64;

// =============================================================================
// LocalOrderGraph — UNCHANGED
// =============================================================================

#[derive(Clone)]
pub struct LocalOrderGraph{
    // local order
    local_order: Vec<(Node, Transaction)>,
    sb_handler: SmallBankTransactionHandler,
    dependencies: HashMap<Node, (char, Vec<u32>)>,
}

impl LocalOrderGraph{
    pub fn new(local_order: Vec<(Node, Transaction)>, sb_handler: SmallBankTransactionHandler) -> Self {
        let mut dependencies: HashMap<Node, (char, Vec<u32>)> = HashMap::new();
        for order in &local_order{
            let id  = (*order).0;
            let dep = sb_handler.get_transaction_dependency((*order).1.clone().into());
            dependencies.insert(id, dep);
        }

        LocalOrderGraph{
            local_order: local_order,
            sb_handler: sb_handler,
            dependencies: dependencies,
        }
    }

    pub fn get_dag(&self) -> DiGraphMap<Node, u8>{
        // (1) Create an empty graph G=(V,E)
        let mut dag: DiGraphMap<Node, u8> = DiGraphMap::new();

        // (2) Add all the nodes into the empty graph, as per local order received
        for order in &self.local_order{
            let _node = dag.add_node((*order).0);
        }

        // Better time complexity with some space
        let mut seen_deps: HashMap<u32, Vec<(Node, char)>> = HashMap::new();
        for (curr_digest, _tx) in &self.local_order{
            let curr_deps: &(char, Vec<u32>) = &self.dependencies[curr_digest];
            for curr_dep in &curr_deps.1{
                seen_deps.entry(*curr_dep).or_insert_with(Vec::new);
                for prev_tx_info in &seen_deps[&curr_dep]{
                    if curr_deps.0=='r' && prev_tx_info.1=='r'{
                        continue;
                    }
                    dag.add_edge(prev_tx_info.0, *curr_digest, 1);
                }
                seen_deps.entry(*curr_dep).or_insert_with(Vec::new).push((*curr_digest,curr_deps.0));
            }
        }
        return dag;
    }

    pub fn get_dag_serialized(&self) -> Vec<Vec<u8>>{
        let dag: DiGraphMap<Node, u8> = self.get_dag();
        let mut dag_vec: Vec<Vec<u8>> = Vec::new();

        for node in dag.nodes(){
            let mut node_vec: Vec<Node> = Vec::new();
            node_vec.push(node);
            for neighbor in dag.neighbors(node){
                node_vec.push(neighbor);
            }
            dag_vec.push(bincode::serialize(&node_vec).expect("Failed to serialize local order dag"));
        }

        return dag_vec;
    }

    pub fn get_dag_deserialized(serialized_dag: Vec<Vec<u8>>) -> DiGraphMap<Node, u8>{
        let mut dag: DiGraphMap<Node, u8> = DiGraphMap::new();
        for serialized_node_info in serialized_dag{
            match bincode::deserialize::<Vec<Node>>(&serialized_node_info).unwrap() {
                node_info => {
                    let node: Node = node_info[0];
                    dag.add_node(node);
                    for neighbor_idx in 1..node_info.len(){
                        dag.add_edge(node, node_info[neighbor_idx], 1);
                    }
                },
            }
        }
        return dag;
    }
}

// =============================================================================
// GlobalDependencyGraph — UNCHANGED
// (Builds global graph from local orders; may contain cycles)
// =============================================================================

#[derive(Clone)]
pub struct GlobalDependencyGraph{
    dag: DiGraphMap<Node, u8>,
    fixed_transactions: HashSet<Node>,
    missed_edges: HashMap<(Node, Node), u16>,
}

impl GlobalDependencyGraph{
    pub fn new(local_order_graphs: Vec<DiGraphMap<Node, u8>>, fixed_tx_threshold: f32, pending_tx_threshold: f32) -> Self {
        info!("GlobalDependencyGraph::new fixed_tx_threshold = {:?}, pending_tx_threshold = {:?}", fixed_tx_threshold, pending_tx_threshold);
        let edge_threshold = f32::min(fixed_tx_threshold, pending_tx_threshold) - 1.0;
        // (1) Create an empty graph G=(V,E)
        let mut dag: DiGraphMap<Node, u8> = DiGraphMap::new();

        // (2) Find transactions' counts
        let mut transaction_counts: HashMap<Node, Node> = HashMap::new();
        let mut edge_counts: HashMap<(Node,Node), u16> = HashMap::new();
        let mut loop_count:u64 = 0;
        for local_order_graph in &local_order_graphs{
            for node in local_order_graph.nodes(){
                let count = *transaction_counts.entry(node).or_insert(0);
                transaction_counts.insert(node, count+1);
                loop_count = loop_count + 1;
            }
            for (from, to, _weight) in local_order_graph.all_edges(){
                edge_counts.entry((from, to)).or_insert(0);
                edge_counts.entry((to, from)).or_insert(0);
                edge_counts.insert((from, to), edge_counts[&(from, to)]+1);
            }
        }

        // (3) Find fixed and pending transactions and add them into the graph
        let mut fixed_transactions: HashSet<Node> = HashSet::new();
        for (&tx, &count) in &transaction_counts{
            if count as f32 >= fixed_tx_threshold || count as f32 >= pending_tx_threshold{
                dag.add_node(tx);
            }
            if count as f32 >= fixed_tx_threshold{
                fixed_transactions.insert(tx);
            }
        }

        // (4) Find edges to add into the graph
        // (5) Find missing edges in this graph
        let mut missed_edges: HashMap<(Node, Node), u16> = HashMap::new();
        for (&(from, to), &count) in &edge_counts{
            if ((count as f32) >= fixed_tx_threshold || (count as f32) >= pending_tx_threshold) && count > edge_counts[&(to, from)]{
                dag.add_edge(from, to, 1);
            }
            else if dag.contains_node(from) && dag.contains_node(to) && (count as f32) < edge_threshold && (count as f32) > 0.0{
                // edge between 'from' and 'to' is missing
                missed_edges.insert((from,to), count);
                missed_edges.insert((to, from), edge_counts[&(to, from)]);
            }
        }

        GlobalDependencyGraph{
            dag: dag,
            fixed_transactions: fixed_transactions,
            missed_edges: missed_edges,
        }
    }

    pub fn get_dag(&self) -> &DiGraphMap<Node, u8>{
        return &self.dag;
    }

    pub fn get_fixed_transactions(&self) -> &HashSet<Node>{
        return &self.fixed_transactions;
    }

    pub fn get_missed_edges(&self) -> &HashMap<(Node, Node), u16>{
        return &self.missed_edges;
    }
}

// =============================================================================
// NEW: Condensation — Algorithm 2, Line 23
//
// Detects SCCs using Kosaraju's algorithm. Each SCC with >1 node (i.e., a
// cycle) is linearized into a deterministic chain sorted by tx_uid. Inter-SCC
// edges are rewired: tail(source_SCC) → head(target_SCC). The resulting graph
// is guaranteed to be a DAG.
//
// Why sorted chains instead of opaque super-nodes:
//   - The existing serialization format (each node = one u64) is preserved.
//   - The execution threadpool, Kahn's scheduler, and store lookups all work
//     unchanged — every node is still a single transaction.
//   - Sorting by tx_uid is deterministic across all replicas.
//   - The tradeoff (sequential intra-SCC execution) is acceptable because
//     cyclically-dependent transactions have no meaningful parallel order.
// =============================================================================

/// Result of the condensation step. Carries the linearized DAG plus metadata
/// needed by the pruning step to reason about SCCs.
struct CondensationResult {
    /// The linearized DAG — all cycles replaced with sorted chains.
    dag: DiGraphMap<Node, u8>,
    /// Sorted member list for each SCC (index = SCC id).
    scc_members: Vec<Vec<Node>>,
    /// Maps every node to the SCC it belongs to.
    node_to_scc: HashMap<Node, usize>,
    /// Whether each SCC contains at least one fixed transaction.
    scc_is_fixed: Vec<bool>,
}

fn condense_graph(
    graph: &DiGraphMap<Node, u8>,
    fixed_transactions: &HashSet<Node>,
) -> CondensationResult {
    let sccs = kosaraju_scc(graph);

    let mut node_to_scc: HashMap<Node, usize> = HashMap::new();
    let mut scc_members: Vec<Vec<Node>> = Vec::new();
    let mut scc_is_fixed: Vec<bool> = Vec::new();

    for (i, scc) in sccs.iter().enumerate() {
        // Sort members deterministically by tx_uid so all replicas agree
        let mut sorted = scc.clone();
        sorted.sort();

        let is_fixed = sorted.iter().any(|n| fixed_transactions.contains(n));

        for &node in &sorted {
            node_to_scc.insert(node, i);
        }

        scc_members.push(sorted);
        scc_is_fixed.push(is_fixed);
    }

    // ---- Build the linearized DAG ----
    let mut dag: DiGraphMap<Node, u8> = DiGraphMap::new();

    // Add every node (preserve the full vertex set)
    for node in graph.nodes() {
        dag.add_node(node);
    }

    // Intra-SCC: replace cycle with a sorted chain
    for members in &scc_members {
        if members.len() > 1 {
            info!(
                "CONDENSE: linearizing SCC of size {} — chain: {:?}",
                members.len(),
                members
            );
            for i in 0..members.len() - 1 {
                dag.add_edge(members[i], members[i + 1], 1);
            }
        }
    }

    // Inter-SCC: tail(source) → head(target), deduplicated at the SCC-pair level
    let mut added_scc_pairs: HashSet<(usize, usize)> = HashSet::new();
    for (from, to, _) in graph.all_edges() {
        let from_scc = node_to_scc[&from];
        let to_scc = node_to_scc[&to];
        if from_scc != to_scc && added_scc_pairs.insert((from_scc, to_scc)) {
            let from_tail = *scc_members[from_scc].last().unwrap();
            let to_head = scc_members[to_scc][0];
            dag.add_edge(from_tail, to_head, 1);
        }
    }

    CondensationResult {
        dag,
        scc_members,
        node_to_scc,
        scc_is_fixed,
    }
}

// =============================================================================
// UPDATED: Pruning — Algorithm 2, Lines 24-30
//
// The paper says: for every *pending* vertex u in the condensation graph, if
// there is no *fixed* vertex v such that (u, v) ∈ E^C, remove u and all its
// edges. A vertex (SCC) is "pending" if it contains zero fixed transactions.
//
// The old implementation only removed trailing all-pending SCCs from the end
// of the topological order and stopped at the first fixed SCC, missing pending
// SCCs elsewhere in the graph.
// =============================================================================

#[derive(Clone)]
pub struct PrunedGraph {
    pruned_graph: DiGraphMap<Node, u8>,
}

impl PrunedGraph {
    /// Prune pending SCCs that have no direct outgoing edge to a fixed SCC in
    /// the condensation graph (Algorithm 2, lines 24-27).
    pub fn new(condensation: &CondensationResult) -> Self {
        let mut pruned_graph = condensation.dag.clone();
        let num_sccs = condensation.scc_members.len();

        // Build SCC-level successor map from the linearized DAG edges
        let mut scc_successors: HashMap<usize, HashSet<usize>> = HashMap::new();
        for (from, to, _) in condensation.dag.all_edges() {
            let from_scc = condensation.node_to_scc[&from];
            let to_scc = condensation.node_to_scc[&to];
            if from_scc != to_scc {
                scc_successors.entry(from_scc).or_default().insert(to_scc);
            }
        }

        // For each pending SCC, check for a direct outgoing edge to a fixed SCC
        for scc_idx in 0..num_sccs {
            if condensation.scc_is_fixed[scc_idx] {
                continue; // Fixed SCCs are always kept
            }

            let has_fixed_successor = scc_successors
                .get(&scc_idx)
                .map(|succs| succs.iter().any(|&s| condensation.scc_is_fixed[s]))
                .unwrap_or(false);

            if !has_fixed_successor {
                info!(
                    "PRUNE: removing pending SCC {} with {} members: {:?}",
                    scc_idx,
                    condensation.scc_members[scc_idx].len(),
                    condensation.scc_members[scc_idx]
                );
                for &node in &condensation.scc_members[scc_idx] {
                    pruned_graph.remove_node(node);
                }
            }
        }

        PrunedGraph { pruned_graph }
    }

    pub fn get_dag(&self) -> DiGraphMap<Node, u8> {
        return self.pruned_graph.clone();
    }
}

// =============================================================================
// NEW: Transitive Reduction — Algorithm 2, Line 31
//
// Given a DAG G^C, produce the minimal subgraph G^T that preserves all
// reachability relationships. An edge (u, v) is redundant iff v is reachable
// from u through some other path of length ≥ 2.
//
// Complexity: O(|E| · (|V| + |E|)) — for each edge, one BFS. This is
// acceptable for the per-round transaction graph sizes in this system.
// For very large graphs, an O(|V|^2) matrix-based approach could be used.
// =============================================================================

fn transitive_reduction(dag: &DiGraphMap<Node, u8>) -> DiGraphMap<Node, u8> {
    // Collect all edges up front; we check reachability against the ORIGINAL
    // graph so that removing one edge doesn't affect checks for others.
    let edges: Vec<(Node, Node)> = dag.all_edges().map(|(a, b, _)| (a, b)).collect();

    let mut result = dag.clone();

    for &(u, v) in &edges {
        // Is v reachable from u through some *other* neighbor in the original graph?
        // If yes, the direct edge (u, v) is redundant.
        let mut redundant = false;
        for neighbor in dag.neighbors(u) {
            if neighbor != v && bfs_reachable(dag, neighbor, v) {
                redundant = true;
                break;
            }
        }
        if redundant {
            result.remove_edge(u, v);
        }
    }

    result
}

/// BFS reachability check: returns true if `target` is reachable from `start`.
fn bfs_reachable(dag: &DiGraphMap<Node, u8>, start: Node, target: Node) -> bool {
    if start == target {
        return true;
    }
    let mut visited: HashSet<Node> = HashSet::new();
    let mut queue: VecDeque<Node> = VecDeque::new();
    queue.push_back(start);
    visited.insert(start);

    while let Some(current) = queue.pop_front() {
        for neighbor in dag.neighbors(current) {
            if neighbor == target {
                return true;
            }
            if visited.insert(neighbor) {
                queue.push_back(neighbor);
            }
        }
    }
    false
}

// =============================================================================
// NEW (optional): Missing-edge recomputation after condensation + pruning
//
// Algorithm 2, Lines 28-30: "for every pair of data-dependent transactions
// t and t' with no edge: if t and t' are in two different vertices of V^C:
// Add (t, t') to M."
//
// This updates the missing-edge set to reflect the condensed/pruned graph.
// Pairs within the same SCC do NOT need tracking (their order is resolved).
// =============================================================================

/// Recompute missing edge pairs relative to the condensed+pruned graph.
/// Only keeps pairs where both nodes survive pruning and belong to different
/// SCCs. Pairs within the same SCC are dropped (their order is already
/// determined by the intra-SCC chain).
fn recompute_missed_edges(
    original_missed: &HashMap<(Node, Node), u16>,
    pruned_dag: &DiGraphMap<Node, u8>,
    node_to_scc: &HashMap<Node, usize>,
) -> HashMap<(Node, Node), u16> {
    let mut result: HashMap<(Node, Node), u16> = HashMap::new();

    for (&(from, to), &count) in original_missed {
        // Both nodes must still exist in the pruned graph
        if !pruned_dag.contains_node(from) || !pruned_dag.contains_node(to) {
            continue;
        }
        // Pairs within the same SCC have their order determined by the chain
        if let (Some(&scc_from), Some(&scc_to)) = (node_to_scc.get(&from), node_to_scc.get(&to)) {
            if scc_from == scc_to {
                continue; // Same SCC — order is resolved
            }
        }
        result.insert((from, to), count);
    }

    result
}

// =============================================================================
// UPDATED: GlobalOrderGraph
//
// Full pipeline per Algorithm 2:
//   1. GlobalDependencyGraph  — build graph from local orders (may have cycles)
//   2. condense_graph         — SCC detection + cycle linearization (Line 23)
//   3. PrunedGraph::new       — remove pending SCCs with no fixed successor (Lines 24-30)
//   4. recompute_missed_edges — drop intra-SCC pairs, keep only cross-SCC (Lines 28-30)
//   5. transitive_reduction   — remove redundant edges (Line 31)
// =============================================================================

#[derive(Clone)]
pub struct GlobalOrderGraph {
    global_order_graph: DiGraphMap<Node, u8>,
    missed_edges: HashMap<(Node, Node), u16>,
}

impl GlobalOrderGraph {
    pub fn new(
        local_order_graphs: Vec<DiGraphMap<Node, u8>>,
        fixed_tx_threshold: f32,
        pending_tx_threshold: f32,
    ) -> Self {
        // Step 1: Build global dependency graph (may contain cycles)
        let global_dep = GlobalDependencyGraph::new(
            local_order_graphs,
            fixed_tx_threshold,
            pending_tx_threshold,
        );

        info!(
            "GlobalOrderGraph: dependency graph built — nodes={} edges={} missed_edge_pairs={}",
            global_dep.get_dag().node_count(),
            global_dep.get_dag().edge_count(),
            global_dep.get_missed_edges().len(),
        );

        // Step 2: Condensation — detect SCCs, linearize cycles into sorted chains
        let condensation = condense_graph(
            global_dep.get_dag(),
            global_dep.get_fixed_transactions(),
        );

        info!(
            "GlobalOrderGraph: condensation done — {} SCCs, linearized DAG nodes={} edges={}",
            condensation.scc_members.len(),
            condensation.dag.node_count(),
            condensation.dag.edge_count(),
        );

        // Step 3: Pruning — remove pending SCCs with no direct fixed successor
        let pruned = PrunedGraph::new(&condensation);
        let pruned_dag = pruned.get_dag();

        info!(
            "GlobalOrderGraph: pruning done — nodes={} edges={}",
            pruned_dag.node_count(),
            pruned_dag.edge_count(),
        );

        // Step 4: Recompute missed edges (drop intra-SCC pairs)
        let missed_edges = recompute_missed_edges(
            global_dep.get_missed_edges(),
            &pruned_dag,
            &condensation.node_to_scc,
        );

        info!(
            "GlobalOrderGraph: missed edges after recomputation = {}",
            missed_edges.len(),
        );

        // Step 5: Transitive reduction — remove redundant edges
        let reduced_dag = transitive_reduction(&pruned_dag);

        info!(
            "GlobalOrderGraph: transitive reduction done — nodes={} edges={} (removed {} redundant edges)",
            reduced_dag.node_count(),
            reduced_dag.edge_count(),
            pruned_dag.edge_count() - reduced_dag.edge_count(),
        );

        GlobalOrderGraph {
            global_order_graph: reduced_dag,
            missed_edges,
        }
    }

    pub fn get_dag(&self) -> DiGraphMap<Node, u8> {
        return self.global_order_graph.clone();
    }

    pub fn get_dag_serialized(&self) -> Vec<Vec<u8>> {
        let dag: DiGraphMap<Node, u8> = self.get_dag();
        let mut dag_vec: Vec<Vec<u8>> = Vec::new();

        for node in dag.nodes() {
            let mut node_vec: Vec<Node> = Vec::new();
            node_vec.push(node);
            for neighbor in dag.neighbors(node) {
                node_vec.push(neighbor);
            }
            dag_vec.push(
                bincode::serialize(&node_vec)
                    .expect("Failed to serialize global order graph"),
            );
        }

        return dag_vec;
    }

    pub fn get_dag_deserialized(serialized_dag: Vec<Vec<u8>>) -> DiGraphMap<Node, u8> {
        let mut dag: DiGraphMap<Node, u8> = DiGraphMap::new();
        for serialized_node_info in serialized_dag {
            match bincode::deserialize::<Vec<Node>>(&serialized_node_info).unwrap() {
                node_info => {
                    let node: Node = node_info[0];
                    dag.add_node(node);
                    for neighbor_idx in 1..node_info.len() {
                        dag.add_edge(node, node_info[neighbor_idx], 1);
                    }
                },
            }
        }
        return dag;
    }

    pub fn get_missed_edges(&self) -> HashMap<(Node, Node), u16> {
        return self.missed_edges.clone();
    }
}


pub fn add(left: usize, right: usize) -> usize {
    left + right
}