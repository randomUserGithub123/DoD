// Copyright(C) Heena Nagda.
use crate::global_order_quorum_waiter::GlobalOrderQuorumWaiterMessage;
use crate::worker::{Round, WorkerMessage};
// use crate::worker::SerializedBatchDigestMessage;
use crate::missing_edge_manager::MissingEdgeManager;
use config::{WorkerId, Committee};
use crypto::Digest;
use crypto::PublicKey;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
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
                local_order_dags: Vec::new(),
                rx_round,
                rx_batch,
                // tx_digest,
                tx_message,
                workers_addresses,
                network: ReliableSender::new(),
                gamma,
            }
            .run()
            .await;
        });
    }

    /// Main loop.
    async fn run(&mut self) {
        while let Some(GlobalOrderMakerMessage { batch, own_digest }) = self.rx_batch.recv().await {
            // Get the new round number if advanced (non blocking)
            match self.rx_round.try_recv(){
                Ok(round) => {
                    info!("Update round received : {}", round);
                    self.current_round = round;
                    self.local_order_dags.clear();
                },
                _ => (),
            }

            info!("current_round = {:?}", self.current_round);

            let mut send_order: bool = false;
            // creating a Global Order
            if (self.local_order_dags.len() as u32) < self.committee.quorum_threshold(){
                match bincode::deserialize(&batch).unwrap() {
                    WorkerMessage::Batch(mut batch) => {
                        match batch.pop() {
                            Some(batch_round_vec) => {
                                let batch_round_arr = batch_round_vec.try_into().unwrap_or_else(|batch_round_vec: Vec<u8>| panic!("Expected a Vec of length {} but it was {}", 8, batch_round_vec.len()));
                                let batch_round = u64::from_le_bytes(batch_round_arr);
                                // 
                                if batch_round == self.current_round {
                                    let dag = LocalOrderGraph::get_dag_deserialized(batch);
                                    self.update_missed_edges(dag.clone()).await;
                                    self.local_order_dags.push(dag);
                                    if (self.local_order_dags.len() as u32) >= self.committee.quorum_threshold(){
                                        send_order = true;
                                    }
                                }
                            }
                            _ => panic!("Unexpected batch round found"),
                        }
                    },
                    _ => panic!("Unexpected message"),
                }
            }

            if send_order{
                // TODO: Pending and fixed transaction threshold
                // create a Global Order based on n-f received local orders 
                let fixed_tx_threshold = self.committee.fixed_tx_threshold();
                let pending_tx_threshold = self.committee.pending_tx_threshold(self.gamma);
                let global_order_graph_obj: GlobalOrderGraph = GlobalOrderGraph::new(self.local_order_dags.clone(), fixed_tx_threshold, pending_tx_threshold); // 3.0, 2.5
                let global_order_graph = global_order_graph_obj.get_dag_serialized();
                let missed_edges = global_order_graph_obj.get_missed_edges();
                let mut missed_pairs: HashSet<(Node, Node)> = HashSet::new();
                
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

                    for id in 1..=4 {
                        // NOTE: This log entry is used to compute performance.
                        info!(
                            "Batch {:?} contains sample tx {}",
                            digest,
                            id
                        );
                    }

                    // NOTE: This log entry is used to compute performance.
                    info!("Batch {:?} contains {} B", digest, 512*4);
                }

                // Broadcast the batch through the network.
                let (names, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
                let bytes = Bytes::from(serialized.clone());
                let handlers = self.network.broadcast(addresses, bytes).await;
                
                // Send the batch through the deliver channel for further processing.
                info!("Send Global order to Quorum Waiter");
                self.tx_message
                .send(GlobalOrderQuorumWaiterMessage {
                    global_order_info: serialized,
                    handlers: names.into_iter().zip(handlers.into_iter()).collect(),
                })
                .await
                .expect("Failed to deliver global order");
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
