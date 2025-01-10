// use log::{info};
use petgraph::graphmap::DiGraphMap;
use petgraph::algo::kosaraju_scc;
// use petgraph::algo::condensation;
// use bytes::BufMut as _;
// use bytes::BytesMut;
// use bytes::Bytes;
// use crypto::Digest;
use smallbank::SmallBankTransactionHandler;
use std::collections::{HashMap, HashSet};
use log::info;
// use debugtimer::DebugTimer;


type Transaction = Vec<u8>;
type Node = u64;

#[derive(Clone)]
pub struct LocalOrderGraph{
    // local order
    local_order: Vec<(Node, Transaction)>,
    sb_handler: SmallBankTransactionHandler,
    dependencies: HashMap<Node, (char, Vec<u32>)>,
}

impl LocalOrderGraph{
    pub fn new(local_order: Vec<(Node, Transaction)>, sb_handler: SmallBankTransactionHandler) -> Self {
        // info!("local order is received = {:?}", local_order);
        let mut dependencies: HashMap<Node, (char, Vec<u32>)> = HashMap::new();
        for order in &local_order{
            let id  = (*order).0;
            let dep = sb_handler.get_transaction_dependency((*order).1.clone().into());
            dependencies.insert(id, dep);
        }
        // info!("size of input local order = {:?}", local_order.len());

        LocalOrderGraph{
            local_order: local_order,
            sb_handler: sb_handler,
            dependencies: dependencies,
        }
    }

    pub fn get_dag(&self) -> DiGraphMap<Node, u8>{
        // info!("local order DAG creation start");

        // (1) Create an empty graph G=(V,E)
        let mut dag: DiGraphMap<Node, u8> = DiGraphMap::new();

        // (2) Add all the nodes into the empty graph, as per local order received
        for order in &self.local_order{
            let _node = dag.add_node((*order).0);
            // info!("node added {:?} :: {:?}", (*order).0, node);
        }

        // // Better time complexity with some space
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
                // _ => panic!("Unexpected message"),
            }
        }
        return dag;
    }
}

#[derive(Clone)]
pub struct GlobalDependencyGraph{
    dag: DiGraphMap<Node, u8>,
    fixed_transactions: HashSet<Node>,
    missed_edges: HashMap<(Node, Node), u16>,
}

impl GlobalDependencyGraph{
    pub fn new(local_order_graphs: Vec<DiGraphMap<Node, u8>>, fixed_tx_threshold: f32, pending_tx_threshold: f32) -> Self {
        info!("In new global dependency graph fixed_tx_threshold = {:?}, pending_tx_threshold = {:?}", fixed_tx_threshold, pending_tx_threshold);
        info!("Local order graphs:");
        // for (i, graph) in local_order_graphs.iter().enumerate() {
        //     info!("Graph {}: {} nodes, {} edges", i, graph.node_count(), graph.edge_count());
        //     info!("Nodes: {:?}", graph.nodes().collect::<Vec<_>>());
        //     info!("Edges: {:?}", graph.all_edges().collect::<Vec<_>>());
        // }
        
        // (1) Create an empty graph G=(V,E)
        let mut dag: DiGraphMap<Node, u8> = DiGraphMap::new();

        // (2) Find transactions' counts
        let mut transaction_counts: HashMap<Node, Node> = HashMap::new();
        let mut edge_counts: HashMap<(Node,Node), u16> = HashMap::new();
        let mut loop_count:u64 = 0;
        for local_order_graph in &local_order_graphs{
            for node in local_order_graph.nodes(){
                // info!("global_dependency_graph::new : tx = {:?}", node);
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
        info!("Loop count = {:?}", loop_count);
        info!("Transaction count = {:?}", transaction_counts.len());

        // (3) Find fixed and pending transactions and add them into the graph
        let mut fixed_transactions: HashSet<Node> = HashSet::new();
        for (&tx, &count) in &transaction_counts{
            info!("global_dependency_graph::new : tx = {:?}, count = {:?}", tx, count);
            if count as f32 >= fixed_tx_threshold || count as f32 >= pending_tx_threshold{
                dag.add_node(tx);
            }
            if count as f32 >= pending_tx_threshold{
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
            else if !((edge_counts[&(to, from)] as f32) >= fixed_tx_threshold || (edge_counts[&(to, from)] as f32) >= pending_tx_threshold){
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

#[derive(Clone)]
pub struct PrunedGraph{
    pruned_graph:  DiGraphMap<Node, u8>,
}

impl PrunedGraph{
    pub fn new(global_dependency_graph: &DiGraphMap<Node, u8>, fixed_transactions: &HashSet<Node>) -> Self {
        let strongely_connected_components = kosaraju_scc(global_dependency_graph);
        let mut pruned_graph:  DiGraphMap<Node, u8> = global_dependency_graph.clone();
        let mut idx: usize = strongely_connected_components.len()-1;
        let mut count = 0;

        while strongely_connected_components.len() > 0 && count<strongely_connected_components.len(){
            let mut is_fixed: bool = false;
            for node in &strongely_connected_components[idx]{
                if fixed_transactions.contains(node){
                    is_fixed = true;
                    break;
                }
            }
            if is_fixed{
                break;
            }
            // All pending transactions are found in this scc : remove these
            for &node in &strongely_connected_components[idx]{
                pruned_graph.remove_node(node);
            }
            idx -= 1;
            count += 1;
        } 
        

        PrunedGraph{
            pruned_graph: pruned_graph,
        }
    }

    pub fn get_dag(&self) -> DiGraphMap<Node, u8>{
        return self.pruned_graph.clone();
    }
}

#[derive(Clone)]
pub struct GlobalOrderGraph{
    global_order_graph:  DiGraphMap<Node, u8>,
    missed_edges: HashMap<(Node, Node), u16>,
}

impl GlobalOrderGraph{
    pub fn new(local_order_graphs: Vec<DiGraphMap<Node, u8>>, fixed_tx_threshold: f32, pending_tx_threshold: f32) -> Self {
        info!("In new global order graph");
        let global_dependency_graph: GlobalDependencyGraph = GlobalDependencyGraph::new(local_order_graphs, fixed_tx_threshold, pending_tx_threshold);
        let pruned_graph: PrunedGraph = PrunedGraph::new(global_dependency_graph.get_dag(), global_dependency_graph.get_fixed_transactions());
        let global_order_graph: DiGraphMap<Node, u8> = pruned_graph.get_dag();
        let missed_edges: HashMap<(Node, Node), u16> = global_dependency_graph.get_missed_edges().clone();

        GlobalOrderGraph{
            global_order_graph: global_order_graph,
            missed_edges: missed_edges,
        }
    }

    pub fn get_dag(&self) -> DiGraphMap<Node, u8>{
        return self.global_order_graph.clone();
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
            dag_vec.push(bincode::serialize(&node_vec).expect("Failed to serialize global order graph"));
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
                // _ => panic!("Unexpected message"),
            }
        }
        return dag;
    }

    pub fn get_missed_edges(&self) -> HashMap<(Node, Node), u16>{
        return self.missed_edges.clone();
    }
}


pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
