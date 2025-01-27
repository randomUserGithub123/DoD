use std::{collections::VecDeque, env::current_exe};

use crate::quorum_waiter::QuorumWaiterMessage;
use crate::processor::SerializedBatchMessage;
use crate::worker::WorkerMessage;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use network::ReliableSender;
use log::info;
use crypto::PublicKey;
use std::net::SocketAddr;
use graph::LocalOrderGraph;
use ed25519_dalek::{Digest as _, Sha512};
use crypto::Digest;
use core::convert::TryInto;
use bytes::Bytes;
use smallbank::SmallBankTransactionHandler;
use crate::batch_maker::Transaction;
use std::collections::HashMap;

// The batch buffer is responsible for storing batches received from clients
// When a new global order graph is created, the next batch buffer is moved down the pipeline
type Node = u64;
type TransactionBuffer = Vec<(Node, Transaction)>;

#[derive(Debug)]
pub struct BatchBufferMessage {
    pub batch: TransactionBuffer,
    pub rashnu_round: u64,
}

pub struct BatchBufferRoundDoneMessage {
    pub sent_nodes: Vec<Node>,
    pub rashnu_round: u64,
}

pub struct NodeInfo {
    pub node: Node,
    pub tx: Transaction,
    pub sent: bool,
    pub included_in_global_order: bool,
}

pub struct BatchBuffer {
    // Receive time sorted store of batches
    batch_store: VecDeque<BatchBufferMessage>,
    // Input channel to receive batches
    rx_message: Receiver<BatchBufferMessage>,
    // Input channel to receive transactions without enough votes from the previous Rashnu round
    rx_round_done: Receiver<BatchBufferRoundDoneMessage>,
    // Output channel to send batches to the next stage (quorum waiter)
    tx_message: Sender<QuorumWaiterMessage>,
    // workers addresses for the broadcast
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    // network sender to broadcast the batches to the other workers
    network: ReliableSender,
    // smallbank handler to get the dependency of each transaction
    sb_handler: SmallBankTransactionHandler,
    // Hashmap to store unsent nodes
    node_info_map: HashMap<Node, NodeInfo>,
    // flag to check whether we are in a round or not
    in_round: bool,
}

impl BatchBuffer {
    pub fn spawn(
        rx_message: Receiver<BatchBufferMessage>,
        rx_round_done: Receiver<BatchBufferRoundDoneMessage>,
        tx_message: Sender<QuorumWaiterMessage>,
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
        sb_handler: SmallBankTransactionHandler,
    ) {
        tokio::spawn(async move {
            Self { 
                batch_store: VecDeque::new(), 
                rx_message, 
                rx_round_done, 
                tx_message, 
                workers_addresses, 
                network: ReliableSender::new(),
                sb_handler,
                node_info_map: HashMap::new(),
                in_round: false,
            }.run().await;
        });
    }

    async fn run(&mut self) {
        // select on the two channels
        loop {
            tokio::select! {
                Some(batch) = self.rx_message.recv() => {
                    // info!("batch_buffer::run: received batch");
                    // info!("batch_buffer::run: sealing round no active round");
                    self.seal_round_no_active_round(batch).await;
                    // info!("batch_buffer::run: done sealing round no active round");
                    // if !self.in_round {
                    //     info!("batch_buffer::run: starting new round");
                    //     self.in_round = true;
                    //     // send this to the next stage (quorum waiter)
                    //     info!("batch_buffer::run: sealing round no active round");
                    //     self.seal_round_no_active_round(batch).await;
                    //     info!("batch_buffer::run: done sealing round no active round");
                    // } else {
                    //     info!("batch_buffer::run: adding batch to store, length = {:?}", self.batch_store.len());
                    //     self.batch_store.push_back(batch);
                    //     info!("batch_buffer::run: done adding batch to store, length = {:?}", self.batch_store.len());
                    // }
                }
                Some(round_done_message) = self.rx_round_done.recv() => {
                    // info!("batch_buffer::run: received round_done_message for round = {:?}", round_done_message.rashnu_round);
                    // if (self.batch_store.len() == 0) {
                    //     info!("batch_buffer::run: no batches in store, ending round");
                    //     self.in_round = false;
                    // } else {
                    //     info!("batch_buffer::run: sealing round");
                    //     self.seal_round(round_done_message).await;
                    //     info!("batch_buffer::run: done sealing round");
                    // }

                    // handle the case where the current Rashnu round is done
                    // let current_batch_msg = self.batch_store.pop_front().unwrap();
                    // let current_batch = current_batch_msg.batch;
                    // let current_rashnu_round = current_batch_msg.rashnu_round;
                    // assert!(current_batch_msg.rashnu_round == batch_buffer_message.rashnu_round + 1); // TODO: check this
                    // let mut final_batch = Vec::new();
                    // for (tx_uid, tx) in batch_buffer_message.batch {
                    //     final_batch.push((tx_uid, tx));
                    // }
                    // for (tx_uid, tx) in current_batch {
                    //     final_batch.push((tx_uid, tx));
                    // }
                    // let local_order_len = final_batch.len();
                    // let local_order_graph_obj: LocalOrderGraph = LocalOrderGraph::new(final_batch, self.sb_handler.clone());
                    // let mut batch = local_order_graph_obj.get_dag_serialized();
                    // // Adding current Rashnu round number with this batch
                    // batch.push(current_rashnu_round.to_le_bytes().to_vec());
                    // let message = WorkerMessage::Batch(batch);
                    // let serialized = bincode::serialize(&message).expect("Failed to serialize the batch");

                    // // compute digest
                    // let digest = Digest(Sha512::digest(&serialized).as_slice()[..32].try_into().unwrap());
                    // info!("batch_buffer::run: local_order_len = {:?}, digest = {:?}", local_order_len, digest);

                    // #[cfg(feature = "benchmark")]
                    // {
                    //     // NOTE: This is one extra hash that is only needed to print the following log entries.
                    //     let digest = Digest(
                    //         Sha512::digest(&serialized).as_slice()[..32]
                    //             .try_into()
                    //             .unwrap(),
                    //     );
                    //     // NOTE: This log entry is used to compute performance.
                    //     info!("Batch {:?} ------ {} B", digest, local_order_len);
                    // }

                    // // and send the updated batch to the next stage (quorum waiter)

                    // let (names, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
                    // let bytes = Bytes::from(serialized.clone());
                    // let handlers = self.network.broadcast(addresses, bytes).await;

                    // self.tx_message
                    //     .send(QuorumWaiterMessage { 
                    //         batch: serialized, 
                    //         handlers: names.into_iter().zip(handlers.into_iter()).collect() 
                    //     })
                    //     .await
                    //     .expect("Failed to send the batch to the next stage");
                }
            }
            tokio::task::yield_now().await;
        }
    }

    async fn seal_round_no_active_round(&mut self, current_batch_msg: BatchBufferMessage) {
        // send this to the next stage (quorum waiter)
        let local_order_len = current_batch_msg.batch.len();
        let local_order_graph_obj: LocalOrderGraph = LocalOrderGraph::new(current_batch_msg.batch, self.sb_handler.clone());
        let mut batch = local_order_graph_obj.get_dag_serialized();
        batch.push(current_batch_msg.rashnu_round.to_le_bytes().to_vec());
        let message = WorkerMessage::Batch(batch);
        let serialized = bincode::serialize(&message).expect("Failed to serialize the batch");

        let digest = Digest(Sha512::digest(&serialized).as_slice()[..32].try_into().unwrap()); // compute digest
        // info!("batch_buffer::run: local_order_len = {:?}, digest = {:?}", local_order_len, digest);

        #[cfg(feature = "benchmark")]
        {
            // NOTE: This is one extra hash that is only needed to print the following log entries.
            let digest = Digest(
                Sha512::digest(&serialized).as_slice()[..32]
                    .try_into()
                    .unwrap(),
            );
            // NOTE: This log entry is used to compute performance.
            info!("Batch {:?} ------ {} B", digest, local_order_len);
        }

        let (names, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        let handlers = self.network.broadcast(addresses, bytes).await;

        self.tx_message
            .send(QuorumWaiterMessage { 
                batch: serialized, 
                handlers: names.into_iter().zip(handlers.into_iter()).collect() 
            })
            .await.expect("Failed to send the batch to the next stage");
        // info!("batch_buffer::seal_round_no_active_round: done sending batch to the next stage");
    }

    async fn seal_round(&mut self, round_done_message: BatchBufferRoundDoneMessage) {
        // info!("batch_buffer::seal_round: sealing round = {:?}", round_done_message.rashnu_round);
        let mut final_batch = Vec::new();
        let current_batch_msg = self.batch_store.pop_front().unwrap();
        let current_batch = current_batch_msg.batch;
        let current_rashnu_round = current_batch_msg.rashnu_round;
        // check that the Rashnu round number is reasonable
        // Step 1 : add all unsent txs from previous rounds that have not appeared in leader proposal
        for node in &round_done_message.sent_nodes {
            if let Some(node_info) = self.node_info_map.get_mut(&node) {
                if !node_info.included_in_global_order {
                    node_info.included_in_global_order = true;
                }
            }
        }
        for node in round_done_message.sent_nodes {
            if let Some(node_info) = self.node_info_map.get_mut(&node) {
                if !node_info.included_in_global_order {
                    // add this node to the batch
                    // info!("batch_buffer::seal_round: adding tx to final batch, node = {:?}", node);
                    final_batch.push((node, node_info.tx.clone()));
                }
            }
        }
        // Step 2 : add all txs received in the current round
        for (node, tx) in current_batch {
            // info!("batch_buffer::seal_round: adding tx to final batch, node = {:?}", node);
            final_batch.push((node, tx.clone()));
            self.node_info_map.insert(node, NodeInfo { node, tx, sent: true, included_in_global_order: false });
        }
        // Step 3 : construct the local order DAG (do we need to add missing edges here?)
        let local_order_len = final_batch.len();
        let local_order_graph_obj: LocalOrderGraph = LocalOrderGraph::new(final_batch, self.sb_handler.clone());
        let mut batch = local_order_graph_obj.get_dag_serialized();
    
        batch.push(current_rashnu_round.to_le_bytes().to_vec()); // Adding current Rashnu round number with this batch
        let message = WorkerMessage::Batch(batch);
        let serialized = bincode::serialize(&message).expect("Failed to serialize the batch");

        let digest = Digest(Sha512::digest(&serialized).as_slice()[..32].try_into().unwrap()); // compute digest
        // info!("batch_buffer::run: local_order_len = {:?}, digest = {:?}", local_order_len, digest);

        #[cfg(feature = "benchmark")]
        {
            // NOTE: This is one extra hash that is only needed to print the following log entries.
            let digest = Digest(
                Sha512::digest(&serialized).as_slice()[..32]
                    .try_into()
                    .unwrap(),
            );
            // NOTE: This log entry is used to compute performance.
            info!("Batch {:?} ------ {} B", digest, local_order_len);
        }

        // and send the updated batch to the next stage (quorum waiter)

        let (names, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        let handlers = self.network.broadcast(addresses, bytes).await;

        // Step 4: send the batch to the next stage (quorum waiter)
        self.tx_message
            .send(QuorumWaiterMessage { 
                batch: serialized, 
                handlers: names.into_iter().zip(handlers.into_iter()).collect() 
            })
            .await
            .expect("Failed to send the batch to the next stage");

    }
}
