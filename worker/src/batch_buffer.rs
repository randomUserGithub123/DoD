use std::collections::VecDeque;

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

// The batch buffer is responsible for storing batches received from clients
// When a new global order graph is created, the next batch buffer is moved down the pipeline
type Node = u64;
type TransactionBuffer = Vec<(Node, Transaction)>;

#[derive(Debug)]
pub struct BatchBufferMessage {
    pub batch: TransactionBuffer,
    pub rashnu_round: u64,
}

pub struct BatchBuffer {
    // Receive time sorted store of batches
    batch_store: VecDeque<BatchBufferMessage>,
    // Input channel to receive batches
    rx_message: Receiver<BatchBufferMessage>,
    // Input channel to receive transactions without enough votes from the previous Rashnu round
    rx_transaction: Receiver<TransactionBuffer>,   // TODO: fix to the correct type
    // Output channel to send batches to the next stage (quorum waiter)
    tx_message: Sender<QuorumWaiterMessage>,
    // workers addresses for the broadcast
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    // network sender to broadcast the batches to the other workers
    network: ReliableSender,
    // smallbank handler to get the dependency of each transaction
    sb_handler: SmallBankTransactionHandler,
}

impl BatchBuffer {
    pub fn spawn(
        rx_message: Receiver<BatchBufferMessage>,
        rx_transaction: Receiver<TransactionBuffer>,
        tx_message: Sender<QuorumWaiterMessage>,
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
        sb_handler: SmallBankTransactionHandler,
    ) {
        tokio::spawn(async move {
            Self { 
                batch_store: VecDeque::new(), 
                rx_message, 
                rx_transaction, 
                tx_message, 
                workers_addresses, 
                network: ReliableSender::new(),
                sb_handler,
            }.run().await;
        });
    }

    async fn run(&mut self) {
        // TODO: Implement the batch buffer
        // select on the two channels
        loop {
            tokio::select! {
                Some(batch) = self.rx_message.recv() => {
                    self.batch_store.push_back(batch);
                }
                Some(transaction) = self.rx_transaction.recv() => {
                    let current_batch_msg = self.batch_store.pop_front().unwrap();
                    let current_batch = current_batch_msg.batch;
                    let current_rashnu_round = current_batch_msg.rashnu_round;
                    let mut final_batch = Vec::new();
                    for (tx_uid, tx) in transaction {
                        final_batch.push((tx_uid, tx));
                    }
                    for (tx_uid, tx) in current_batch {
                        final_batch.push((tx_uid, tx));
                    }
                    let local_order_len = final_batch.len();
                    let local_order_graph_obj: LocalOrderGraph = LocalOrderGraph::new(final_batch, self.sb_handler.clone());
                    let mut batch = local_order_graph_obj.get_dag_serialized();
                    // Adding current Rashnu round number with this batch
                    batch.push(current_rashnu_round.to_le_bytes().to_vec());
                    let message = WorkerMessage::Batch(batch);
                    let serialized = bincode::serialize(&message).expect("Failed to serialize the batch");

                    // compute digest
                    let digest = Digest(Sha512::digest(&serialized).as_slice()[..32].try_into().unwrap());
                    info!("batch_buffer::run: local_order_len = {:?}, digest = {:?}", local_order_len, digest);

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

                    self.tx_message
                        .send(QuorumWaiterMessage { 
                            batch: serialized, 
                            handlers: names.into_iter().zip(handlers.into_iter()).collect() 
                        })
                        .await
                        .expect("Failed to send the batch to the next stage");
                }
            }
            tokio::task::yield_now().await;
        }
    }
}
