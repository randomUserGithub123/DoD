use crate::writer_store::WriterStore;
use tokio::sync::{mpsc, Mutex};
use tokio::task::{self, JoinHandle};
use std::sync::Arc;
use network::Writer;
use bytes::Bytes;
use futures::SinkExt;
use smallbank::SmallBankTransactionHandler;
use store::Store;
use log::{error, info, warn};

/// ThreadWorker struct that represents an async worker.
/// Only stores the id and handle — all other state is moved into the spawned task.
struct ThreadWorker {
    id: usize,
    handle: JoinHandle<()>,
}

impl ThreadWorker {
    /// Starts a worker that listens for messages from the channel
    fn new(
        id: usize,
        receiver: Arc<Mutex<mpsc::Receiver<u64>>>,
        store: Store,
        writer_store: Arc<futures::lock::Mutex<WriterStore>>,
        sb_handler: SmallBankTransactionHandler,
        tx_done: tokio::sync::mpsc::UnboundedSender<u64>,
    ) -> ThreadWorker {
        let mut store_clone = store;
        let mut sb_handler_clone = sb_handler;
        let writer_store_clone = Arc::clone(&writer_store);
        let tx_done_clone = tx_done;
        let worker_id = id;

        let handle = task::spawn(async move {
            let mut processed_count: u64 = 0;

            loop {
                let msg = {
                    let acquire_start = std::time::Instant::now();
                    let mut rx = receiver.lock().await;
                    let acquire_ms = acquire_start.elapsed().as_millis();
                    if acquire_ms > 1000 {
                        warn!(
                            "ThreadWorker {}: waited {}ms to acquire receiver lock",
                            worker_id, acquire_ms
                        );
                    }
                    rx.recv().await
                };

                match msg {
                    Some(tx_uid) => {
                        processed_count += 1;
                        let tx_id_vec = tx_uid.to_be_bytes().to_vec();

                        // Step 1: Read the transaction from the store
                        let read_start = std::time::Instant::now();
                        let tx_result = store_clone.read(tx_id_vec.clone()).await;
                        let read_ms = read_start.elapsed().as_millis();
                        if read_ms > 1000 {
                            warn!(
                                "ThreadWorker {}: store.read took {}ms for tx_uid={}",
                                worker_id, read_ms, tx_uid
                            );
                        }

                        // Step 2: Execute the transaction
                        match &tx_result {
                            Ok(Some(tx)) => {
                                let exec_start = std::time::Instant::now();
                                sb_handler_clone.execute_transaction(Bytes::from(tx.clone()));
                                let exec_ms = exec_start.elapsed().as_millis();
                                if exec_ms > 1000 {
                                    warn!(
                                        "ThreadWorker {}: execute_transaction took {}ms for tx_uid={}",
                                        worker_id, exec_ms, tx_uid
                                    );
                                }
                            }
                            Ok(None) => {
                                error!("ThreadWorker {} :: Cannot find tx_uid = {:?} in the store", worker_id, tx_uid);
                            }
                            Err(e) => {
                                error!("ThreadWorker {} :: Store read error for tx_uid = {:?}: {}", worker_id, tx_uid, e);
                            }
                        }

                        // Step 3: Notify completion IMMEDIATELY — before any client IO.
                        let _ = tx_done_clone.send(tx_uid);

                        // Step 4: Log TX_FINALIZED for every successfully executed tx.
                        // This is used by the log parser to compute TPS, latency, etc.
                        if tx_result.is_ok() && tx_result.as_ref().unwrap().is_some() {
                            info!("TX_FINALIZED: tx_uid={}", tx_uid);
                        }

                        // Periodic throughput log
                        if processed_count % 500 == 0 {
                            info!(
                                "ThreadWorker {}: processed {} txs so far",
                                worker_id, processed_count
                            );
                        }
                    }
                    None => {
                        info!(
                            "ThreadWorker {}: shutting down after processing {} txs",
                            worker_id, processed_count
                        );
                        break;
                    }
                }
            }
        });

        ThreadWorker { id, handle }
    }
}

/// ExecutionThreadPool struct that manages worker threads
pub struct ExecutionThreadPool {
    sender: mpsc::Sender<u64>,
    thread_workers: Vec<ThreadWorker>,
}

impl ExecutionThreadPool {
    /// Creates a new thread pool with `size` thread_workers
    pub fn new(
        size: usize,
        store: Store,
        writer_store: Arc<futures::lock::Mutex<WriterStore>>,
        sb_handler: SmallBankTransactionHandler,
        tx_done: mpsc::UnboundedSender<u64>,
    ) -> ExecutionThreadPool {
        let (sender, receiver) = mpsc::channel::<u64>(100);
        let receiver = Arc::new(Mutex::new(receiver));
        let mut thread_workers = Vec::with_capacity(size);

        for id in 0..size {
            let thread_worker = ThreadWorker::new(
                id,
                Arc::clone(&receiver),
                store.clone(),
                writer_store.clone(),
                sb_handler.clone(),
                tx_done.clone(),
            );
            thread_workers.push(thread_worker);
        }

        info!("ExecutionThreadPool: created with {} workers", size);
        ExecutionThreadPool { sender, thread_workers }
    }

    /// Sends a message to the worker pool
    pub async fn send_message(&self, message: u64) {
        if let Err(e) = self.sender.send(message).await {
            info!("Failed to send message to worker: {}", e);
        }
    }

    /// Graceful shutdown: Wait for all thread_workers to finish
    pub async fn shutdown(self) {
        drop(self.sender); // Close the channel so workers exit their loops
        for thread_worker in self.thread_workers {
            if let Err(e) = thread_worker.handle.await {
                info!("Worker {} encountered an error: {:?}", thread_worker.id, e);
            }
        }
    }
}