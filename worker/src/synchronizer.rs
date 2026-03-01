// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::{Round, WorkerMessage};
use crate::execution_queue::ExecutionQueue;
use crate::writer_store::WriterStore;
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
// use futures::SinkExt;
use std::sync::{Arc};
use futures::lock::Mutex;
use log::{debug, error, info, warn};
use network::{SimpleSender};
use primary::PrimaryWorkerMessage;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use store::{Store, StoreError};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};
use smallbank::SmallBankTransactionHandler;

#[cfg(test)]
#[path = "tests/synchronizer_tests.rs"]
pub mod synchronizer_tests;

/// Resolution of the timer managing retrials of sync requests (in ms).
const TIMER_RESOLUTION: u64 = 1_000;

// The `Synchronizer` is responsible to keep the worker in sync with the others.
pub struct Synchronizer {
    /// The public key of this authority.
    name: PublicKey,
    /// The id of this worker.
    id: WorkerId,
    /// The committee information.
    committee: Committee,
    // The persistent storage.
    store: Store,
    // Writer handle (socket to send an ack to the corresponding client)
    writer_store: Arc<Mutex<WriterStore>>,
    // small-bank handler to execute transacrtions
    sb_handler: SmallBankTransactionHandler,
    /// The depth of the garbage collection.
    gc_depth: Round,
    /// The delay to wait before re-trying to send sync requests.
    sync_retry_delay: u64,
    /// Determine with how many nodes to sync when re-trying to send sync-requests. These nodes
    /// are picked at random from the committee.
    sync_retry_nodes: usize,
    /// Input channel to receive the commands from the primary.
    rx_message: Receiver<PrimaryWorkerMessage>,
    /// Output channel to send the new round for a newly created batch when advanced
    tx_batch_round: Sender<Round>,
    /// Output channel to send the new round for a global order creation when advanced
    tx_global_order_round: Sender<Round>,
    /// A network sender to send requests to the other workers.
    network: SimpleSender,
    /// Loosely keep track of the's round number (only used for cleanup).
    round: Round,
    /// Keeps the digests (of batches) that are waiting to be processed by the primary. Their
    /// processing will resume when we get the missing batches in the store or we no longer need them.
    /// It also keeps the round number and a timestamp (`u128`) of each request we sent.
    pending: HashMap<Digest, (Round, Sender<()>, u128)>,
    /// Keeping track of the elements in the Execution Queue (on worker)
    exe_queue: ExecutionQueue,
    /// Counter for tracking how many Execute messages processed
    execute_count: u64,
}

impl Synchronizer {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        id: WorkerId,
        committee: Committee,
        store: Store,
        writer_store: Arc<Mutex<WriterStore>>,
        sb_handler: SmallBankTransactionHandler,
        gc_depth: Round,
        sync_retry_delay: u64,
        sync_retry_nodes: usize,
        exe_queue: ExecutionQueue,
        rx_message: Receiver<PrimaryWorkerMessage>,
        tx_batch_round: Sender<Round>,
        tx_global_order_round: Sender<Round>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                id,
                committee,
                store,
                writer_store,
                sb_handler,
                gc_depth,
                sync_retry_delay,
                sync_retry_nodes,
                rx_message,
                tx_batch_round,
                tx_global_order_round,
                network: SimpleSender::new(),
                round: Round::default(),
                pending: HashMap::new(),
                exe_queue,
                execute_count: 0,
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for a batch to become available in the storage
    /// and then delivers its digest.
    async fn waiter(
        missing: Digest,
        mut store: Store,
        deliver: Digest,
        mut handler: Receiver<()>,
    ) -> Result<Option<Digest>, StoreError> {
        tokio::select! {
            result = store.notify_read(missing.to_vec()) => {
                result.map(|_| Some(deliver))
            }
            _ = handler.recv() => Ok(None),
        }
    }

    /// Main loop listening to the primary's messages.
    async fn run(&mut self) {
        let mut waiting = FuturesUnordered::new();

        let timer = sleep(Duration::from_millis(TIMER_RESOLUTION));
        tokio::pin!(timer);

        let run_start = Instant::now();
        info!("TRACE_SYNC: synchronizer run loop starting");

        let mut last_heartbeat = Instant::now();
        let mut msg_count: u64 = 0;

        loop {
            // Heartbeat every 5 seconds
            if last_heartbeat.elapsed().as_secs() >= 5 {
                info!("TRACE_SYNC: HEARTBEAT uptime={:?} msg_count={} execute_count={} pending={} round={}", 
                    run_start.elapsed(), msg_count, self.execute_count, self.pending.len(), self.round);
                last_heartbeat = Instant::now();
            }

            tokio::select! {
                // Handle primary's messages.
                Some(message) = self.rx_message.recv() => {
                    msg_count += 1;
                    match message {
                    PrimaryWorkerMessage::Synchronize(digests, target) => {
                        info!("TRACE_SYNC: Synchronize received num_digests={} target={:?}", digests.len(), target);
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Failed to measure time")
                            .as_millis();

                        let mut missing = Vec::new();
                        for digest in digests {
                            // Ensure we do not send twice the same sync request.
                            if self.pending.contains_key(&digest) {
                                continue;
                            }

                            // Check if we received the batch in the meantime.
                            match self.store.read(digest.to_vec()).await {
                                Ok(None) => {
                                    missing.push(digest.clone());
                                    debug!("Requesting sync for batch {}", digest);
                                },
                                Ok(Some(_)) => {
                                    // The batch arrived in the meantime: no need to request it.
                                },
                                Err(e) => {
                                    error!("{}", e);
                                    continue;
                                }
                            }

                            // Add the digest to the waiter.
                            let deliver = digest.clone();
                            let (tx_cancel, rx_cancel) = channel(1_000);
                            let fut = Self::waiter(digest.clone(), self.store.clone(), deliver, rx_cancel);
                            waiting.push(fut);
                            self.pending.insert(digest, (self.round, tx_cancel, now));
                        }

                        let address = match self.committee.worker(&target, &self.id) {
                            Ok(address) => address.worker_to_worker,
                            Err(e) => {
                                error!("The primary asked us to sync with an unknown node: {}", e);
                                continue;
                            }
                        };
                        let message = WorkerMessage::BatchRequest(missing.clone(), self.name);
                        let serialized = bincode::serialize(&message).expect("Failed to serialize our own message");
                        info!("TRACE_SYNC: Synchronize sending {} missing batch requests", missing.len());
                        self.network.send(address, Bytes::from(serialized)).await;
                    },

                    PrimaryWorkerMessage::Execute(certificate) => {
                        self.execute_count += 1;
                        let execute_start = Instant::now();
                        let round = certificate.round();
                        let num_digests = certificate.header.payload.len();
                        info!("TRACE_SYNC: Execute START #{} round={} num_digests={} (queue backlog pending={})", 
                            self.execute_count, round, num_digests, self.pending.len());

                        for (i, digest) in certificate.header.payload.keys().enumerate() {
                            info!("TRACE_SYNC: Execute digest {}/{} digest={:?} round={} exec_count={}", 
                                i+1, num_digests, digest, round, self.execute_count);
                            let digest_start = Instant::now();
                            self.exe_queue.execute(*digest).await;
                            let digest_time = digest_start.elapsed();
                            if digest_time.as_secs() >= 5 {
                                warn!("TRACE_SYNC: Execute digest {}/{} VERY SLOW {:?} round={}", i+1, num_digests, digest_time, round);
                            }
                            info!("TRACE_SYNC: Execute digest {}/{} DONE in {:?} round={}", i+1, num_digests, digest_time, round);
                        }

                        let total_time = execute_start.elapsed();
                        if total_time.as_secs() >= 10 {
                            warn!("TRACE_SYNC: Execute ALL DONE SLOW for round={} total_time={:?} exec_count={}", round, total_time, self.execute_count);
                        }
                        info!("TRACE_SYNC: Execute ALL DONE for round={} total_time={:?} exec_count={}", round, total_time, self.execute_count);
                    },
                    
                    PrimaryWorkerMessage::Cleanup(round) => {
                        info!("TRACE_SYNC: Cleanup received round={} (prev_round={}, pending={})", round, self.round, self.pending.len());
                        self.round = round;

                        if self.round < self.gc_depth {
                            continue;
                        }

                        let mut gc_round = self.round - self.gc_depth;
                        let mut gc_count = 0;
                        for (r, handler, _) in self.pending.values() {
                            if r <= &gc_round {
                                let _ = handler.send(()).await;
                                gc_count += 1;
                            }
                        }
                        let before = self.pending.len();
                        self.pending.retain(|_, (r, _, _)| r > &mut gc_round);
                        info!("TRACE_SYNC: Cleanup gc_round={} cancelled={} removed={}", gc_round, gc_count, before - self.pending.len());
                    }

                    PrimaryWorkerMessage::AdvanceRound(round) => {
                        info!("TRACE_SYNC: AdvanceRound received round={} (prev_round={})", round, self.round);
                        self.tx_batch_round
                            .send(round)
                            .await
                            .expect("Failed to deliver new batch round");
                        self.tx_global_order_round
                            .send(round)
                            .await
                            .expect("Failed to deliver new global order round");   
                        info!("TRACE_SYNC: AdvanceRound delivered round={}", round);
                    }
                }},

                Some(result) = waiting.next() => match result {
                    Ok(Some(digest)) => {
                        info!("TRACE_SYNC: waiter resolved digest={}", digest);
                        self.pending.remove(&digest);
                    },
                    Ok(None) => {
                    },
                    Err(e) => error!("{}", e)
                },

                () = &mut timer => {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Failed to measure time")
                        .as_millis();

                    let mut retry = Vec::new();
                    for (digest, (_, _, timestamp)) in &self.pending {
                        if timestamp + (self.sync_retry_delay as u128) < now {
                            debug!("Requesting sync for batch {} (retry)", digest);
                            retry.push(digest.clone());
                        }
                    }
                    if !retry.is_empty() {
                        info!("TRACE_SYNC: timer retry sending {} sync requests (pending={})", retry.len(), self.pending.len());
                        let addresses = self.committee
                            .others_workers(&self.name, &self.id)
                            .iter().map(|(_, address)| address.worker_to_worker)
                            .collect();
                        let message = WorkerMessage::BatchRequest(retry, self.name);
                        let serialized = bincode::serialize(&message).expect("Failed to serialize our own message");
                        self.network
                            .lucky_broadcast(addresses, Bytes::from(serialized), self.sync_retry_nodes)
                            .await;
                    }

                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                },
            }
        }
    }
}