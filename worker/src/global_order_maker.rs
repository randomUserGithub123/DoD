// Copyright(C) Heena Nagda.
use crate::global_order_quorum_waiter::GlobalOrderQuorumWaiterMessage;
use crate::worker::{Round, WorkerMessage};
use crate::batch_buffer::BatchBufferRoundDoneMessage;
// use crate::worker::SerializedBatchDigestMessage;
use crate::missing_edge_manager::MissingEdgeManager;
use config::{WorkerId, Committee};
use crypto::Digest;
use crypto::PublicKey;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use petgraph::algo::k_shortest_path;
use std::convert::TryInto;
use std::net::SocketAddr;
use std::sync::{Arc};
use futures::lock::Mutex;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use log::{info};
use graph::{LocalOrderGraph, GlobalOrderGraph};
use petgraph::prelude::DiGraphMap;
use network::ReliableSender;
use bytes::Bytes;
use std::collections::HashSet;
use std::collections::HashMap;

/// Indicates a serialized `WorkerMessage::Batch` message.
pub type SerializedBatchMessage = Vec<u8>;
pub type Transaction = Vec<u8>;
pub type GlobalOrder = Vec<Transaction>;
type Node = u64;
pub type MissedEdgePairs = HashSet<(Node, Node)>;

#[derive(Debug)]
pub struct GlobalOrderMakerMessage {
    /// A serialized `WorkerMessage::Batch` message.
    pub batch: SerializedBatchMessage,
    /// Whether we are processing our own batches or the batches of other nodes.
    pub own_digest: bool,
}

pub struct LocalOrderDags {
    pub local_order_dags: Vec<DiGraphMap<Node, u8>>,
    pub sent: bool,
}

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct GlobalOrderMaker{
    /// The committee information.
    committee: Committee,
    /// Our worker's id.
    id: WorkerId,
    /// The persistent storage.
    store: Store,
    /// Object of missing_edge_manager
    missed_edge_manager: Arc<Mutex<MissingEdgeManager>>,
    /// Current round.
    current_round: Round,
    /// Rashnu round
    rashnu_round: u64,
    /// Local orders
    local_order_dags: Vec<DiGraphMap<Node, u8>>,
    /// Input channel to receive updated current round.
    rx_round: Receiver<Round>,
    /// Input channel to receive batches.
    rx_batch: Receiver<GlobalOrderMakerMessage>,
    // /// Output channel to send out Global Ordered batches' digests.
    // tx_digest: Sender<SerializedBatchDigestMessage>,
    /// Output channel to deliver sealed Global Order to the `GlobalOrderQuorumWaiter`.
    tx_message: Sender<GlobalOrderQuorumWaiterMessage>,
    /// The network addresses of the other workers that share our worker id.
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// A network sender to broadcast the batches to the other workers.
    network: ReliableSender,
    // store a map from rashnu round to a struct that contains the local order dags and whether this was sent
    rashnu_round_to_local_order_dags: HashMap<u64, LocalOrderDags>,
    /// Output channel to send an update to the batch buffer saying that we're done with this round
    tx_batch_buffer: Sender<BatchBufferRoundDoneMessage>,
    // Fairness factor
    gamma: f64,
}


impl GlobalOrderMaker {
    /// Spawn a new GlobalOrderMaker.
    pub fn spawn(
        committee: Committee,
        id: WorkerId,
        store: Store,
        missed_edge_manager: Arc<Mutex<MissingEdgeManager>>,
        rx_round: Receiver<Round>,
        rx_batch: Receiver<GlobalOrderMakerMessage>,
        // tx_digest: Sender<SerializedBatchDigestMessage>,
        tx_batch_buffer: Sender<BatchBufferRoundDoneMessage>,
        tx_message: Sender<GlobalOrderQuorumWaiterMessage>,
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
        gamma: f64,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                id,
                store,
                missed_edge_manager,
                current_round: 1,
                rashnu_round: 1,  
                local_order_dags: Vec::new(),
                rx_round,
                rx_batch,
                // tx_digest,
                tx_message,
                workers_addresses,
                network: ReliableSender::new(),
                rashnu_round_to_local_order_dags: HashMap::new(),
                tx_batch_buffer,
                gamma,
            }
            .run()
            .await;
        });
    }

    /// Main loop.
    async fn run(&mut self) {
        while let Some(GlobalOrderMakerMessage { batch, own_digest }) = self.rx_batch.recv().await {
            // use std::time::Instant;
            // let start = Instant::now();
            // Get the new round number if advanced (non blocking)
            // match self.rx_round.try_recv(){
            //     Ok(round) => {
            //         info!("Update round received : {}", round);
            //         self.current_round = round;
            //         self.local_order_dags.clear();
            //     },
            //     _ => (),
            // }

            // info!("current_round = {:?}", self.current_round);
            // info!("rashnu_round = {:?}", self.rashnu_round);

            let mut send_order: bool = false;
            let mut batch_rashnu_round: u64 = 0;
            
            // get the digest of the batch here
            let debug_batch_digest = Digest(Sha512::digest(&batch).as_slice()[..32].try_into().unwrap());
            // get the round ID
            match bincode::deserialize(&batch).unwrap() {
                WorkerMessage::Batch(mut batch) => {
                    match batch.pop() {
                        Some(batch_round_vec) => {
                            let batch_round_arr: [u8; 8] = batch_round_vec.try_into().unwrap_or_else(|batch_round_vec: Vec<u8>| panic!("Expected a Vec of length {} but it was {}", 8, batch_round_vec.len()));
                            batch_rashnu_round = u64::from_le_bytes(batch_round_arr);
                            // info!("global_order_maker::run : batch_rashnu_round = {:?}", batch_rashnu_round);

                            // info!("global_order_maker::run : batch_rashnu_round = {:?} digest = {:?} own_digest = {:?}", batch_rashnu_round, debug_batch_digest, own_digest);
                            
                            if !self.rashnu_round_to_local_order_dags.contains_key(&batch_rashnu_round){
                                self.rashnu_round_to_local_order_dags.insert(batch_rashnu_round, LocalOrderDags { local_order_dags: Vec::new(), sent: false });
                            }
                            let dag = LocalOrderGraph::get_dag_deserialized(batch);
                            self.update_missed_edges(dag.clone()).await;
                            let local_order_dags = self.rashnu_round_to_local_order_dags.get_mut(&batch_rashnu_round).unwrap();
                            if !local_order_dags.sent{
                                // let dag = LocalOrderGraph::get_dag_deserialized(batch);
                                // self.update_missed_edges(dag.clone()).await;
                                // info!("global_order_maker::run : adding local order dag to local_order_dags, new length = {:?}", local_order_dags.local_order_dags.len() + 1);
                                local_order_dags.local_order_dags.push(dag);
                            }
                            if (local_order_dags.local_order_dags.len() as u32) >= self.committee.quorum_threshold() && !local_order_dags.sent {
                                // info!("global_order_maker::run : send_order = true, batch_digest = {:?}", debug_batch_digest);
                                local_order_dags.sent = true;
                                send_order = true;
                            }
                        }
                        _ => panic!("Unexpected batch round found"),
                    }
                }
                _ => panic!("Unexpected message"),
            }

            // creating a Global Order
            // if (local_order_dags.local_order_dags.len() as u32) < self.committee.quorum_threshold(){
            //     match bincode::deserialize(&batch).unwrap() {
            //         WorkerMessage::Batch(mut batch) => {
            //             match batch.pop() {
            //                 Some(batch_round_vec) => {
            //                     let batch_round_arr = batch_round_vec.try_into().unwrap_or_else(|batch_round_vec: Vec<u8>| panic!("Expected a Vec of length {} but it was {}", 8, batch_round_vec.len()));
            //                     let batch_round = u64::from_le_bytes(batch_round_arr);
            //                     // 
            //                     if batch_round == self.rashnu_round {
            //                         let dag = LocalOrderGraph::get_dag_deserialized(batch);
            //                         self.update_missed_edges(dag.clone()).await;
            //                         self.local_order_dags.push(dag);
            //                         if (self.local_order_dags.len() as u32) >= self.committee.quorum_threshold(){
            //                             send_order = true;
            //                         }
            //                     }
            //                 }
            //                 _ => panic!("Unexpected batch round found"),
            //             }
            //         },
            //         _ => panic!("Unexpected message"),
            //     }
            // }

            if send_order{
                // TODO: Pending and fixed transaction threshold
                // create a Global Order based on n-f received local orders 
                // let global_order_start = Instant::now();
                let local_order_dags = self.rashnu_round_to_local_order_dags.get_mut(&batch_rashnu_round).unwrap();
                //let global_order_graph_obj: GlobalOrderGraph = GlobalOrderGraph::new(local_order_dags.local_order_dags.clone(), 0.0, 0.0); // 3.0, 2.5
                let fixed_tx_threshold = self.committee.fixed_tx_threshold();
                let pending_tx_threshold = self.committee.pending_tx_threshold(self.gamma);
                // info!("global_order_maker::run : fixed_tx_threshold = {:?}, pending_tx_threshold = {:?}", fixed_tx_threshold as f32, pending_tx_threshold as f32);
                let global_order_graph_obj: GlobalOrderGraph = GlobalOrderGraph::new(local_order_dags.local_order_dags.clone(), fixed_tx_threshold as f32, pending_tx_threshold as f32); // 3.0, 2.5
                let dag_obj = global_order_graph_obj.get_dag();
                let global_order_sent_nodes = dag_obj.nodes();
                let global_order_graph = global_order_graph_obj.get_dag_serialized();
                let missed_edges = global_order_graph_obj.get_missed_edges();
                let mut missed_pairs: HashSet<(Node, Node)> = HashSet::new();
                // let global_order_end = Instant::now();
                // let global_order_duration = global_order_end.duration_since(global_order_start);
                // info!("global_order_maker::run : global_order_duration = {:?}, round = {:?}", global_order_duration, batch_rashnu_round);

                // info!("global_order_maker::run : digest = {:?}, num of nodes = {:?}", debug_batch_digest, global_order_graph_obj.get_dag().node_count());
                
                for ((from, to), count) in &missed_edges{
                    {
                        let mut missed_edge_manager_lock = self.missed_edge_manager.lock().await;
                        missed_edge_manager_lock.add_missing_edge(*from, *to).await;
                        missed_edge_manager_lock.add_updated_edge(*from, *to, *count).await;
                    }

                    if !missed_pairs.contains(&(*to, *from)){
                        missed_pairs.insert((*from, *to));
                    }
                }

                info!("missed_pairs count = {:?}", missed_pairs.len());
                // info!("global_order_maker::run : global_order_graph size = {:?}", global_order_graph.len());
                let global_order_len = global_order_graph.len();
                let message = WorkerMessage::GlobalOrderInfo(global_order_graph, missed_pairs.clone());
                let serialized = bincode::serialize(&message).expect("Failed to serialize global order graph");

                // for (from, to) in &missed_pairs{
                //     info!("Missed pair = {:?} -> {:?} added to the serialized graph = {:?}", *from, *to, serialized);
                // }

                #[cfg(feature = "benchmark")]
                {
                    // NOTE: This is one extra hash that is only needed to print the following log entries.
                    let digest = Digest(
                        Sha512::digest(&serialized).as_slice()[..32]
                            .try_into()
                            .unwrap(),
                    );

                    // for id in 1..=4 {
                    //     // NOTE: This log entry is used to compute performance.
                    //     info!(
                    //         "Batch {:?} contains sample tx {}",
                    //         digest,
                    //         id
                    //     );
                    // }

                    // NOTE: This log entry is used to compute performance.
                    info!("Batch {:?} contains {} B", digest, global_order_len*512);
                    // info!("Batch: {:?} maps to {:?}", digest, debug_batch_digest);
                }

                // Broadcast the batch through the network.
                let (names, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
                let bytes = Bytes::from(serialized.clone());
                let handlers = self.network.broadcast(addresses, bytes).await;
                
                // Send the batch through the deliver channel for further processing.
                // info!("Send Global order to Quorum Waiter");
                self.tx_message
                .send(GlobalOrderQuorumWaiterMessage {
                    global_order_info: serialized,
                    handlers: names.into_iter().zip(handlers.into_iter()).collect(),
                })
                .await
                .expect("Failed to deliver global order");

                // send an update to the batch buffer saying that we're done with this round
                let mut sent_nodes = Vec::new();
                for node in global_order_sent_nodes{
                    // tx uid of the transaction
                    // info!("global_order_maker::run : sending node = {:?}", node);
                    sent_nodes.push(node);
                }

                let batch_buffer_message = BatchBufferRoundDoneMessage {
                    sent_nodes,
                    rashnu_round: batch_rashnu_round,
                };
                self.tx_batch_buffer.send(batch_buffer_message).await.expect("Failed to send batch buffer message");

                // let end = Instant::now();
                // let duration = end.duration_since(start);
                // info!("global_order_maker::run : duration = {:?}, round = {:?}", duration, batch_rashnu_round);
            }
        }
    }

    /// Update the edges those were missed in previous global order, and now found in new set of transactions
    async fn update_missed_edges(&mut self, dag: DiGraphMap<Node, u8>){
        for (from, to, _weight) in dag.all_edges(){
            let mut missed_edge_manager_lock = self.missed_edge_manager.lock().await;
            if missed_edge_manager_lock.is_missing_edge(from, to).await {
                missed_edge_manager_lock.add_updated_edge(from, to, 1).await;
            }
        }
    }
}
