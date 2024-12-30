// Copyright(C) Facebook, Inc. and its affiliates.
use crate::batch_maker::{Batch, BatchMaker, Transaction};
use crate::helper::Helper;
use crate::primary_connector::PrimaryConnector;
use crate::processor::{Processor, SerializedBatchMessage};
use crate::global_order_processor::{GlobalOrderProcessor, SerializedGlobalOrderMessage};
use crate::quorum_waiter::QuorumWaiter;
use crate::synchronizer::Synchronizer;
use crate::batch_buffer::{BatchBuffer, BatchBufferRoundDoneMessage};
use crate::global_order_maker::{GlobalOrder, MissedEdgePairs, GlobalOrderMaker, GlobalOrderMakerMessage};
use crate::global_order_quorum_waiter::GlobalOrderQuorumWaiter;
use crate::missing_edge_manager::MissingEdgeManager;
use crate::execution_queue::ExecutionQueue;
use crate::writer_store::WriterStore;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, Parameters, WorkerId};
use crypto::{Digest, PublicKey};
use futures::sink::SinkExt as _;
use log::{error, info, warn};
use network::{MessageHandler, Receiver, Writer};
use primary::PrimaryWorkerMessage;
use serde::{Deserialize, Serialize};
use std::error::Error;
use store::Store;
use tokio::sync::mpsc::{channel, Sender};
use smallbank::SmallBankTransactionHandler;
use std::sync::Arc;
use futures::lock::Mutex;
use futures::stream::SplitSink;
use std::clone::Clone;

#[cfg(test)]
#[path = "tests/worker_tests.rs"]
pub mod worker_tests;

/// The default channel capacity for each channel of the worker.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// The primary round number.
// TODO: Move to the primary.
pub type Round = u64;

/// Indicates a serialized `WorkerPrimaryMessage` message.
pub type SerializedBatchDigestMessage = Vec<u8>;

/// The message exchanged between workers.
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerMessage {
    Batch(Batch),
    BatchRequest(Vec<Digest>, /* origin */ PublicKey),
    GlobalOrderInfo(GlobalOrder, MissedEdgePairs),
    TxRequest(Vec<u8>, PublicKey),
    TxResponse(Vec<u8>, Transaction),
}

pub struct Worker {
    /// The public key of this authority.
    name: PublicKey,
    /// The id of this worker.
    id: WorkerId,
    /// Shard responsibility for this worker
    shard: Vec<u64>,
    /// The committee information.
    committee: Committee,
    /// The configuration parameters.
    parameters: Parameters,
    /// The persistent storage.
    store: Store,
    /// small-bank handler to execute transacrtions
    sb_handler: SmallBankTransactionHandler,
    /// Object of missing edge manager
    missed_edge_manager: Arc<Mutex<MissingEdgeManager>>,
    /// Keeping track of the elements in the Execution Queue (on worker)
    exe_queue: ExecutionQueue,
    /// Writer handle (socket to send an ack to the corresponding client)
    writer_store: Arc<Mutex<WriterStore>>,
}

impl Worker {
    pub fn spawn(
        name: PublicKey,
        id: WorkerId,
        shard: Vec<u64>,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        sb_handler: SmallBankTransactionHandler,
    ) {
        // Define a worker instance.
        let missed_edge_manager = Arc::new(Mutex::new(MissingEdgeManager::new(store.clone(), committee.clone())));
        let writer_store = Arc::new(Mutex::new(WriterStore::new()));

        let worker = Self {
            name,
            id,
            shard,
            committee: committee.clone(),
            parameters: parameters.clone(),
            store: store.clone(),
            sb_handler: sb_handler.clone(),
            missed_edge_manager: missed_edge_manager.clone(),
            exe_queue: ExecutionQueue::new(store.clone(), writer_store.clone(), sb_handler.clone(), missed_edge_manager.clone()),
            writer_store,
        };

        // Spawn all worker tasks.
        let (tx_primary, rx_primary) = channel(CHANNEL_CAPACITY);
        let (tx_batch_round, rx_batch_round) = channel(CHANNEL_CAPACITY);
        let (tx_global_order_round, rx_global_order_round) = channel(CHANNEL_CAPACITY);
        let (tx_global_order_batch, rx_global_order_batch) = channel(CHANNEL_CAPACITY);
        let (tx_batch_buffer_round_done, rx_batch_buffer_round_done) = channel(CHANNEL_CAPACITY);
        let (tx_global_order_quorum_waiter, rx_global_order_quorum_waiter) = channel(CHANNEL_CAPACITY);
        let (tx_global_order_processor, rx_global_order_processor) = channel(CHANNEL_CAPACITY);
        worker.handle_primary_messages(tx_batch_round, tx_global_order_round);
        worker.handle_clients_transactions(rx_batch_round, tx_global_order_batch.clone(), rx_batch_buffer_round_done);
        worker.handle_workers_messages(tx_global_order_batch, tx_primary.clone());

        // The `GlobalOrderMaker` create Global order DAG based on n-f local order DAGs. 
        // Then it hashes and stores the DAG. It then forwards the digest to the `PrimaryConnector`
        // that will send it to our primary machine.
        GlobalOrderMaker::spawn(
            committee.clone(),
            id,
            store.clone(),
            missed_edge_manager.clone(),
            /* rx_round */ rx_global_order_round,
            /* rx_batch */ rx_global_order_batch,
            // /* tx_digest */ tx_primary,
            tx_batch_buffer_round_done, 
            /* tx_message */ tx_global_order_quorum_waiter,
            /* workers_addresses */
            committee
                .others_workers(&name, &id)
                .iter()
                .map(|(name, addresses)| (*name, addresses.worker_to_worker))
                .collect(),
            parameters.gamma,
        );

        // The `QuorumWaiter` waits for 2f authorities to acknowledge reception of the batch. It then forwards
        // the batch to the `Processor`.
        GlobalOrderQuorumWaiter::spawn(
            committee.clone(),
            /* stake */ committee.stake(&name),
            /* rx_message */ rx_global_order_quorum_waiter,
            /* tx_order */ tx_global_order_processor,
        );

        // The `GlobalOrderProcessor` hashes and stores the global order. It then forwards the batch's digest to the `PrimaryConnector`
        // that will send it to our primary machine.
        GlobalOrderProcessor::spawn(
            name,
            id,
            store,
            missed_edge_manager.clone(),
            /* rx_global_order */ rx_global_order_processor,
            /* tx_digest */ tx_primary,
            /* own_batch */ true,
            /* workers_addresses */ committee
                .others_workers(&name, &id)
                .iter()
                .map(|(name, addresses)| (*name, addresses.worker_to_worker))
                .collect(),
        );

        // The `PrimaryConnector` allows the worker to send messages to its primary.
        PrimaryConnector::spawn(
            worker
                .committee.clone()
                .primary(&worker.name)
                .expect("Our public key is not in the committee")
                .worker_to_primary,
            rx_primary,
        );

        // NOTE: This log entry is used to compute performance.
        info!(
            "Worker {} successfully booted on {}",
            id,
            worker
                .committee
                .worker(&worker.name, &worker.id)
                .expect("Our public key or worker id is not in the committee")
                .transactions
                .ip()
        );
    }

    /// Spawn all tasks responsible to handle messages from our primary.
    fn handle_primary_messages(&self, tx_batch_round: Sender<Round>, tx_global_order_round: Sender<Round>) {
        let (tx_synchronizer, rx_synchronizer) = channel(CHANNEL_CAPACITY);

        // Receive incoming messages from our primary.
        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .primary_to_worker;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn(
            address,
            /* handler */
            PrimaryReceiverHandler { tx_synchronizer },
        );

        // The `Synchronizer` is responsible to keep the worker in sync with the others. It handles the commands
        // it receives from the primary (which are mainly notifications that we are out of sync).
        Synchronizer::spawn(
            self.name,
            self.id,
            self.committee.clone(),
            self.store.clone(),
            self.writer_store.clone(),
            self.sb_handler.clone(),
            self.parameters.gc_depth,
            self.parameters.sync_retry_delay,
            self.parameters.sync_retry_nodes,
            self.exe_queue.clone(),
            /* rx_message */ rx_synchronizer,
            /* tx_batch_round */ tx_batch_round,
            /* tx_global_order_round */ tx_global_order_round,
        );

        info!(
            "Worker {} listening to primary messages on {}",
            self.id, address
        );
    }

    /// Spawn all tasks responsible to handle clients transactions.
    fn handle_clients_transactions(&self, 
        rx_batch_round: tokio::sync::mpsc::Receiver<Round>, 
        tx_global_order_batch: Sender<GlobalOrderMakerMessage>,
        rx_batch_buffer_round_done: tokio::sync::mpsc::Receiver<BatchBufferRoundDoneMessage>
    ) {
        let (tx_batch_maker, rx_batch_maker) = channel(CHANNEL_CAPACITY);
        let (tx_batch_buffer, rx_batch_buffer) = channel(CHANNEL_CAPACITY);
        let (tx_quorum_waiter, rx_quorum_waiter) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

        // We first receive clients' transactions from the network.
        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .transactions;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn(
            address,
            /* handler */ TxReceiverHandler { tx_batch_maker },
        );

        // The transactions are sent to the `BatchMaker` that assembles them into batches. It then broadcasts
        // (in a reliable manner) the batches to all other workers that share the same `id` as us. Finally, it
        // gathers the 'cancel handlers' of the messages and send them to the `QuorumWaiter`.
        BatchMaker::spawn(
            self.writer_store.clone(),
            self.store.clone(),
            self.parameters.batch_size,
            self.parameters.max_batch_delay,
            /* rx_transaction */ rx_batch_maker,
            /* tx_message */ tx_batch_buffer,
            /* rx_batch_round */ rx_batch_round,
            /* workers_addresses */
            self.committee
                .others_workers(&self.name, &self.id)
                .iter()
                .map(|(name, addresses)| (*name, addresses.worker_to_worker))
                .collect(),
            self.sb_handler.clone(),
        );

        BatchBuffer::spawn(
            rx_batch_buffer,
            rx_batch_buffer_round_done,
            tx_quorum_waiter,
            self.committee
                .others_workers(&self.name, &self.id)
                .iter()
                .map(|(name, addresses)| (*name, addresses.worker_to_worker))
                .collect(),
            self.sb_handler.clone(),
        );

        // The `QuorumWaiter` waits for 2f authorities to acknowledge reception of the batch. It then forwards
        // the batch to the `Processor`.
        QuorumWaiter::spawn(
            self.committee.clone(),
            /* stake */ self.committee.stake(&self.name),
            /* rx_message */ rx_quorum_waiter,
            /* tx_batch */ tx_processor,
        );

        // The `Processor` hashes and stores the batch. It then forwards the batch's digest to the `PrimaryConnector`
        // that will send it to our primary machine.
        Processor::spawn(
            self.id,
            self.store.clone(),
            /* rx_batch */ rx_processor,
            /* tx_global_order_batch */ tx_global_order_batch,
            /* own_batch */ true,
        );

        info!(
            "Worker {} listening to client transactions on {}",
            self.id, address
        );
    }

    /// Spawn all tasks responsible to handle messages from other workers.
    fn handle_workers_messages(&self, 
        tx_global_order_batch: Sender<GlobalOrderMakerMessage>, 
        tx_primary: Sender<SerializedBatchDigestMessage>
    ) {
        let (tx_helper, rx_helper) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);
        let (tx_global_order_processor, rx_global_order_processor) = channel(CHANNEL_CAPACITY);
        let (tx_transaction_helper, rx_transaction_helper) = channel(CHANNEL_CAPACITY);

        // Receive incoming messages from other workers.
        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .worker_to_worker;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn(
            address,
            /* handler */
            WorkerReceiverHandler {
                tx_helper,
                tx_processor,
                tx_global_order_processor,
                tx_transaction_helper,
            },
        );

        // The `Helper` is dedicated to reply to batch requests from other workers.
        Helper::spawn(
            self.id,
            self.committee.clone(),
            self.store.clone(),
            /* rx_request */ rx_helper,
        );

        // This `Processor` hashes and stores the batches we receive from the other workers. It then forwards the
        // batch's digest to the `PrimaryConnector` that will send it to our primary.
        Processor::spawn(
            self.id,
            self.store.clone(),
            /* rx_batch */ rx_processor,
            /* tx_global_order_batch */ tx_global_order_batch,
            /* own_batch */ false,
        );

        // The `GlobalOrderProcessor` hashes and stores the batches we receive from the other workers. It then forwards the
        // batch's digest to the `PrimaryConnector` that will send it to our primary.
        GlobalOrderProcessor::spawn(
            self.name,
            self.id,
            self.store.clone(),
            self.missed_edge_manager.clone(),
            /* rx_global_order */ rx_global_order_processor,
            /* tx_digest */ tx_primary,
            /* own_batch */ false,
            /* workers_addresses */
            self.committee
                .others_workers(&self.name, &self.id)
                .iter()
                .map(|(name, addresses)| (*name, addresses.worker_to_worker))
                .collect(),
        );

        TransactionHelper::spawn(
            self.id,
            self.committee.clone(),
            self.store.clone(),
            /* rx_request */ rx_transaction_helper,
        );

        info!(
            "Worker {} listening to worker messages on {}",
            self.id, address
        );
    }
}

/// Defines how the network receiver handles incoming transactions.
#[derive(Clone)]
struct TxReceiverHandler {
    tx_batch_maker: Sender<(Transaction, Arc<Mutex<Writer>>)>,
}

#[async_trait]
impl MessageHandler for TxReceiverHandler {
    async fn dispatch(&self, writer: Arc<Mutex<Writer>>, message: Bytes) -> Result<(), Box<dyn Error>> {
        // Send the transaction to the batch maker.
        self.tx_batch_maker
            .send((message.to_vec(), writer))
            .await
            .expect("Failed to send transaction");

        // Give the change to schedule other tasks.
        tokio::task::yield_now().await;
        Ok(())
    }
}

/// Defines how the network receiver handles incoming workers messages.
#[derive(Clone)]
struct WorkerReceiverHandler {
    tx_helper: Sender<(Vec<Digest>, PublicKey)>,
    tx_processor: Sender<SerializedBatchMessage>,
    tx_global_order_processor: Sender<SerializedGlobalOrderMessage>,
    tx_transaction_helper: Sender<Vec<u8>>,
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    async fn dispatch(&self, writer: Arc<Mutex<Writer>>, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // Reply with an ACK.
        // let _ = writer.send(Bytes::from("Ack")).await;
        {
            let mut shareable_writer = writer.lock().await;
            let _ = shareable_writer.send(Bytes::from("Ack")).await;
        }

        // Deserialize and parse the message.
        match bincode::deserialize(&serialized) {
            Ok(WorkerMessage::Batch(..)) => self
                .tx_processor
                .send(serialized.to_vec())
                .await
                .expect("Failed to send batch"),
            Ok(WorkerMessage::BatchRequest(missing, requestor)) => self
                .tx_helper
                .send((missing, requestor))
                .await
                .expect("Failed to send batch request"),
            Ok(WorkerMessage::GlobalOrderInfo(..)) => self
                .tx_global_order_processor
                .send(serialized.to_vec())
                .await
                .expect("Failed to send other workers' global order to the global order processor"),
            Ok(WorkerMessage::TxRequest(tx_id_vec, requestor)) => {
                self.tx_transaction_helper
                .send((tx_id_vec, requestor))
                .await
                .expect("Failed to send tx request");
            }
            Ok(WorkerMessage::TxResponse(tx_id_vec, tx)) => {
                // send this to a storage helper
            }

            Err(e) => warn!("Serialization error: {}", e),
        }
        Ok(())
    }
}

/// Defines how the network receiver handles incoming primary messages.
#[derive(Clone)]
struct PrimaryReceiverHandler {
    tx_synchronizer: Sender<PrimaryWorkerMessage>,
}

#[async_trait]
impl MessageHandler for PrimaryReceiverHandler {
    async fn dispatch(
        &self,
        _writer: Arc<Mutex<Writer>>,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        // Deserialize the message and send it to the synchronizer.
        match bincode::deserialize(&serialized) {
            Err(e) => error!("Failed to deserialize primary message: {}", e),
            Ok(message) => self
                .tx_synchronizer
                .send(message)
                .await
                .expect("Failed to send transaction"),
        }
        Ok(())
    }
}
