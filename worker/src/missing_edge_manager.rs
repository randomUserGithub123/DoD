// Copyright(C) Heena Nagda.
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use store::Store;
use config::Committee;
use log::{error, info};

type Node = u64;

#[derive(Clone, Debug, Serialize, Deserialize)]
enum EdgeManagerFormat {
    MissingEdgeFormat(Vec<Node>),
}

#[derive(Clone)]
pub struct MissingEdgeManager {
    store: Store,
    committee: Committee,  // if (self.local_order_dags.len() as u32) < self.committee.quorum_threshold(){

    // in-mem
    missing_pairs: HashMap<Node, HashSet<Node>>, // Reverse index for O(1) lookup
    missing_edges: HashMap<(Node, Node), u16>,
    finalized_edges: HashSet<(Node, Node)>,
    threshold: u16,
}

impl MissingEdgeManager {
    pub fn new(store: Store, committee: Committee) -> MissingEdgeManager {
        MissingEdgeManager{
            store,
            committee,
            missing_pairs: HashMap::new(),
            missing_edges: HashMap::new(),
            finalized_edges: HashSet::new(),
            threshold: 2,
        }
    }
    // self.round.to_le_bytes()
    // let batch_round = u64::from_le_bytes(batch_round_arr);
    pub async fn add_missing_edge(&mut self, v1: Node, v2: Node) {
        // // info!("add_missing_edge = {:?} -> {:?}", v1, v2);
        // // info!("add_missing_edge = {:?} -> {:?}", v2, v1);
        // let message_fwd = EdgeManagerFormat::MissingEdgeFormat(vec![v1, v2]);
        // let message_rev = EdgeManagerFormat::MissingEdgeFormat(vec![v2, v1]);

        // let serialized_fwd = bincode::serialize(&message_fwd).expect("Failed to serialize missing edge (fwd) while adding to the store");
        // let serialized_rev = bincode::serialize(&message_rev).expect("Failed to serialize missing edge (rev) while adding to the store");


        // // check if this edge is already exists
        // match self.store.read(serialized_fwd.to_vec()).await {
        //     Ok(Some(_count_arr)) => (),
        //     Ok(None) => {
        //         let count: u64 = 0;
        //         self.store.write(serialized_fwd, count.to_le_bytes().to_vec()).await;
        //         self.store.write(serialized_rev, count.to_le_bytes().to_vec()).await;
        //     },
        //     Err(e) => error!("Error while storing missing edge for the first time = {}", e),
        // }

        self.missing_pairs.entry(v1).or_insert_with(HashSet::new).insert(v2);
        self.missing_pairs.entry(v2).or_insert_with(HashSet::new).insert(v1);
    }

    pub async fn is_missing_edge(&mut self, from: Node, to: Node) -> bool {
        let message = EdgeManagerFormat::MissingEdgeFormat(vec![from, to]);
        let serialized = bincode::serialize(&message).expect("Failed to serialize missing edge while checking into the store about missing_edge");
        
        match self.store.read(serialized).await {
            Ok(Some(_count_vec)) => return true,
            Ok(None) => return false,
            Err(e) => error!("Error while checking if there is a missing edge = {}", e),
        }
        return false;
    }

    pub async fn add_updated_edge(&mut self, from: Node, to: Node, new_count: u16) -> bool{
        // let message = EdgeManagerFormat::MissingEdgeFormat(vec![from, to]);
        // let serialized = bincode::serialize(&message).expect("Failed to serialize updated edge while adding into the store");

        // match self.store.read(serialized.clone()).await {
        //     Ok(Some(count_vec)) => {
        //         let mut count_arr: [u8; 8] = [Default::default(); 8];
        //         count_arr[..8].copy_from_slice(&count_vec);
        //         let mut count = u64::from_le_bytes(count_arr);
        //         count += new_count as u64;
        //         self.store.write(serialized, count.to_le_bytes().to_vec()).await;
        //         return count >= self.committee.quorum_threshold() as u64;
        //     },
        //     Ok(None) => (),
        //     Err(e) => error!("Error while checking if there is a missing edge = {}", e),
        // }
        // return false;

        let edge = (from, to);

        let count = self.missing_edges.entry(edge).or_insert(0);
        *count += new_count;

        if *count >= self.threshold {
            self.finalize_edge(edge).await;
        }

        return true;
    }

    /// Process a node and update connected edges
    pub async fn process_node(&mut self, node: u64) {
        if let Some(neighbors) = self.missing_pairs.get(&node) {
            // info!("process_node: Processing node = {:?}", node);
            let mut to_finalize = Vec::new();

            for &neighbor in neighbors.iter() {
                let edge = (neighbor, node);

                if let Some(count) = self.missing_edges.get_mut(&edge) {
                    *count += 1;
                    // info!("process_node: edge count = {:?}, threshold = {:?}", *count, self.threshold);
                    if *count >= self.threshold {
                        // info!("process_node:edge to_finalize= {:?}", edge);
                        to_finalize.push(edge);
                    }
                }
            }

            // Move finalized edges and remove from missing_edges & missing_pairs
            for edge in to_finalize {
                // info!("process_node:edge to_finalize => finalize_edge = {:?}", edge);
                self.finalize_edge(edge).await;
            }
        }
    }

    /// Finalize an edge and remove from `missing_edges` & `missing_pairs`
    pub async fn finalize_edge(&mut self, edge: (u64, u64)) {
        // info!("finalize_edge: edge = {:?}", edge);
        self.finalized_edges.insert(edge);
        self.missing_edges.remove(&edge);

        // Remove from missing_pairs
        if let Some(neighbors) = self.missing_pairs.get_mut(&edge.0) {
            neighbors.remove(&edge.1);
            if neighbors.is_empty() {
                self.missing_pairs.remove(&edge.0);
            }
        }
        if let Some(neighbors) = self.missing_pairs.get_mut(&edge.1) {
            neighbors.remove(&edge.0);
            if neighbors.is_empty() {
                self.missing_pairs.remove(&edge.1);
            }
        }
    }

    /// Check if an edge is finalized and return its direction
    pub async fn is_finalized(&self, from: u64, to: u64) -> Option<(u64, u64)> {
        if self.finalized_edges.contains(&(from, to)) {
            Some((from, to))
        } else if self.finalized_edges.contains(&(to, from)) {
            Some((to, from))
        } else {
            None
        }
    }

    /// Check if a node exists in `missing_pairs`
    pub async fn is_missing(&self, node: u64) -> bool {
        self.missing_pairs.contains_key(&node)
    }

    pub async fn is_missing_edge_updated(&mut self, from: Node, to: Node) -> Option<(u64, u64)> {
        // // info!("is_missing_edge_updated = {:?} -> {:?}", from, to);
        // let message = EdgeManagerFormat::MissingEdgeFormat(vec![from, to]);
        // let serialized = bincode::serialize(&message).expect("Failed to serialize missing edge while checking into the store about missed_edge_updated");
        
        // match self.store.read(serialized).await {
        //     Ok(Some(count_vec)) => {
        //         let mut count_arr: [u8; 8] = [Default::default(); 8];
        //         count_arr[..8].copy_from_slice(&count_vec);
        //         let count = u64::from_le_bytes(count_arr);
        //         return count >= (self.committee.quorum_threshold() as u64);
        //     },
        //     Ok(None) => error!("missed pair not found in the store while checking is_missing_edge_updated"),
        //     Err(e) => error!("Error while checking if there is a missing_edge_updated = {}", e),
        // }
        // return false;
       
        // if let Some(edge) = self.is_finalized(from, to).await {
        //     return true;
        // } 
        // return false;
        return self.is_finalized(from, to).await;
    }

    async fn _u64_to_vec8(self, num: u64) -> Vec<u8>{
        return num.to_le_bytes().to_vec().clone();
    }

    async fn _vec8_to_u64(self, num_vec: Vec<u8>) -> u64{
        let mut num_arr: [u8; 8] = [Default::default(); 8];
        num_arr[..8].copy_from_slice(&num_vec);
        let num = u64::from_le_bytes(num_arr);
        return num;
    }
}