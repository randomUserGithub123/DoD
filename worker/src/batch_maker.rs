// Copyright(C) Facebook, Inc. and its affiliates.
use crate::quorum_waiter::QuorumWaiterMessage;
use crate::worker::{WorkerMessage, Round};
use crate::writer_store::WriterStore;
use crate::batch_buffer::BatchBufferMessage;
use bytes::Bytes;
#[cfg(feature = "benchmark")]
use crypto::Digest;
use crypto::PublicKey;
#[cfg(feature = "benchmark")]
use ed25519_dalek::{Digest as _, Sha512};
#[cfg(feature = "benchmark")]
use log::info;
use network::{ReliableSender, Writer};
#[cfg(feature = "benchmark")]
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use core::convert::TryInto;
// use tokio::macros::support::Poll;
use tokio::time::{sleep, Duration, Instant};
use graph::LocalOrderGraph;
use smallbank::SmallBankTransactionHandler;
// use debugtimer::DebugTimer;
use std::sync::{Arc};
use futures::lock::Mutex;
use store::{Store};

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

pub type Transaction = Vec<u8>;
pub type Batch = Vec<Transaction>;
type Node = u64;

/// The default channel capacity for channel of the writer.
// pub const CHANNEL_CAPACITY: usize = 1_000;

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    // Writer handle (socket to send an ack to the corresponding client)
    writer_store: Arc<Mutex<WriterStore>>,
    // The persistent storage.
    store: Store,
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<(Transaction, Arc<Mutex<Writer>>)>,
    /// Output channel to deliver sealed batches to the `QuorumWaiter`.
    tx_message: Sender<BatchBufferMessage>,
    /// Input channel to receive new round number when advanced
    rx_batch_round: Receiver<Round>,
    /// Current round
    current_round: Round,
    /// The network addresses of the other workers that share our worker id.
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// smallbank handler to get the dependency of each transaction
    sb_handler: SmallBankTransactionHandler,
    /// Holds the current batch.
    current_batch: Batch,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    /// A network sender to broadcast the batches to the other workers.
    network: ReliableSender,
    /// The current Rashnu round number
    rashnu_round: u64,
}

impl BatchMaker {
    pub fn spawn(
        writer_store: Arc<Mutex<WriterStore>>,
        store: Store,
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<(Transaction, Arc<Mutex<Writer>>)>,
        tx_message: Sender<BatchBufferMessage>,
        rx_batch_round: Receiver<Round>,
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
        sb_handler: SmallBankTransactionHandler,
    ) {
        tokio::spawn(async move {
            Self {
                writer_store,
                store,
                batch_size,
                max_batch_delay,
                rx_transaction,
                tx_message,
                rx_batch_round,
                current_round: 1,
                workers_addresses,
                sb_handler,
                current_batch: Batch::with_capacity(batch_size * 2),
                current_batch_size: 0,
                network: ReliableSender::new(),
                rashnu_round: 1,
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);

        loop {
            // Get the new round number if advanced (non blocking)
            // match self.rx_batch_round.try_recv(){
            //     Ok(round) => {
            //         info!("Update round received : {}", round);
            //         self.current_round = round;
            //     },
            //     _ => (),
            // }
            
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some((transaction, writer)) = self.rx_transaction.recv() => {
                    self.current_batch_size += transaction.len();
                    self.current_batch.push(transaction.clone());
                    let tx_uid: u64 = self.sb_handler.get_transaction_uid(Bytes::from(transaction));

                    // Add writer to the in-memory store
                    {
                        let mut writer_store_lock = self.writer_store.lock().await;
                        writer_store_lock.add_writer(tx_uid, writer);
                    }

                    if self.current_batch_size >= self.batch_size {
                        self.seal().await;
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        self.seal().await;
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self) {
        #[cfg(feature = "benchmark")]
        let size = self.current_batch_size;

        // Look for sample txs (they all start with 0) and gather their txs id (the next 8 bytes).
        #[cfg(feature = "benchmark")]
        let tx_ids: Vec<[u8; 8]> = self
            .current_batch
            .iter()
            .filter(|tx| tx[0] == 0u8 && tx.len() > 8)
            .filter_map(|tx| tx[1..9].try_into().ok())
            .collect::<Vec<_>>();

        // TODO: Graphs
        // info!("size of current batch = {:?}", self.current_batch.len());
        // TODO: Take non-sampled transactions
        let mut local_order: Vec<(Node, Transaction)> = Vec::new();
        self.current_batch_size = 0;

        let tx_uids: Vec<_> = self
            .current_batch
            .iter()
            .filter_map(|tx| tx[1..9].try_into().ok())
            .collect();

        let drained_batch: Vec<_> = self.current_batch.drain(..).collect();

        for i in 0..tx_uids.len(){
            // info!("tx_uid Received = {:?}", tx_uid);
            let tx_uid = u64::from_be_bytes(tx_uids[i]);
            let tx = &drained_batch[i];

            info!("batch_maker::seal : tx_uid to store = {:?}", tx_uid);

            // Add transaction against tx_uid in the store for later execution
            self.store.write(tx_uid.to_be_bytes().to_vec(), tx.clone()).await;

            // Add transaction to create a local order
            local_order.push((tx_uid, tx.clone()));
        }
        info!("batch_maker::seal : local_order size number of nodes = {:?}", local_order.len());
        let batch_buffer_message = BatchBufferMessage {
            batch: local_order,
            rashnu_round: self.rashnu_round,
        };
        self.rashnu_round += 1;
        self.tx_message.send(batch_buffer_message).await.expect("Failed to send batch buffer message");

        // let drained_batch: Vec<_> = self.current_batch.drain(..).collect();
        // let mut idx: Node = 0;
        // for tx in &drained_batch{
        //     local_order.push((idx, tx.clone()));
        //     // local_order.push((self.sb_handler.get_transaction_uid(Bytes::from(<Vec<u8> as TryInto<Vec<u8>>>::try_into(tx.clone()).unwrap())), tx.clone()));
        //     idx += 1;
        // };
        // let local_order_len = local_order.len();
        // let local_order_graph_obj: LocalOrderGraph = LocalOrderGraph::new(local_order, self.sb_handler.clone());
        // let mut batch = local_order_graph_obj.get_dag_serialized();
        // // Adding current Rashnu round number with this batch
        // batch.push(self.rashnu_round.to_le_bytes().to_vec());
        // self.rashnu_round += 1;
        
        // // Serialize the batch.
        // // self.current_batch_size = 0;
        // // let batch: Vec<_> = self.current_batch.drain(..).collect();
        // let message = WorkerMessage::Batch(batch);
        // let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");
        
        // // compute digest
        // let digest = Digest(
        //     Sha512::digest(&serialized).as_slice()[..32]
        //         .try_into()
        //         .unwrap(),
        // );
        // info!("batch_maker::seal : local_order_len = {:?}, digest = {:?}", local_order_len, digest);

        // #[cfg(feature = "benchmark")]
        // {
        //     // NOTE: This is one extra hash that is only needed to print the following log entries.
        //     let digest = Digest(
        //         Sha512::digest(&serialized).as_slice()[..32]
        //             .try_into()
        //             .unwrap(),
        //     );

        //     for id in tx_ids {
        //         // NOTE: This log entry is used to compute performance.
        //         info!(
        //             "Batch {:?} ----- {}",
        //             digest,
        //             u64::from_be_bytes(id)
        //         );
        //     }

        //     // NOTE: This log entry is used to compute performance.
        //     info!("Batch {:?} ------ {} B", digest, size);
        // }

        // // Broadcast the batch through the network.
        // let (names, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
        // let bytes = Bytes::from(serialized.clone());
        // let handlers = self.network.broadcast(addresses, bytes).await;

        // // Send the batch through the deliver channel for further processing.
        // self.tx_message
        //     .send(QuorumWaiterMessage {
        //         batch: serialized,
        //         handlers: names.into_iter().zip(handlers.into_iter()).collect(),
        //     })
        //     .await
        //     .expect("Failed to deliver batch");
    }
}
