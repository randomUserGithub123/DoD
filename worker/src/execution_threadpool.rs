use crate::writer_store::WriterStore;
use tokio::sync::{mpsc, Mutex};
use tokio::task::{self, JoinHandle};
use std::sync::Arc;
use network::Writer;
use bytes::Bytes;
use futures::SinkExt;
use smallbank::SmallBankTransactionHandler;
use store::Store;
use log::{error, info};

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

        let handle = task::spawn(async move {
            loop {
                let msg = {
                    let mut rx = receiver.lock().await;
                    rx.recv().await
                };

                match msg {
                    Some(tx_uid) => {
                        let tx_id_vec = tx_uid.to_be_bytes().to_vec();

                        // Step 1: Read the transaction from the store
                        let tx_result = store_clone.read(tx_id_vec.clone()).await;

                        // Step 2: Execute the transaction
                        match &tx_result {
                            Ok(Some(tx)) => {
                                sb_handler_clone.execute_transaction(Bytes::from(tx.clone()));
                            }
                            Ok(None) => {
                                error!("ThreadWorker :: Cannot find tx_uid = {:?} in the store", tx_uid);
                            }
                            Err(e) => {
                                error!("ThreadWorker :: Store read error for tx_uid = {:?}: {}", tx_uid, e);
                            }
                        }

                        // Step 3: Notify completion IMMEDIATELY — before any client IO.
                        // This is critical: if the client writer hangs (slow/disconnected client),
                        // it must NEVER block the execution pipeline.
                        let _ = tx_done_clone.send(tx_uid);

                        // Step 4: Send response to client in a fire-and-forget task.
                        // This runs independently — a stuck TCP connection cannot block the worker.
                        if tx_result.is_ok() && tx_result.as_ref().unwrap().is_some() {
                            let writer_store_for_response = Arc::clone(&writer_store_clone);
                            let tx_id_vec_for_response = tx_id_vec;
                            tokio::spawn(async move {
                                let writer_opt = {
                                    let mut writer_store_lock = writer_store_for_response.lock().await;
                                    if writer_store_lock.writer_exists(tx_uid) {
                                        let writer = writer_store_lock.get_writer(tx_uid);
                                        writer_store_lock.delete_writer(tx_uid);
                                        Some(writer)
                                    } else {
                                        None
                                    }
                                };

                                if let Some(writer) = writer_opt {
                                    let mut writer_lock = writer.lock().await;
                                    let _ = writer_lock.send(Bytes::from(tx_id_vec_for_response)).await;
                                    log::info!("TX_FINALIZED: tx_uid={}", tx_uid);
                                }
                            });
                        }
                    }
                    None => {
                        break; // Graceful shutdown when channel closes
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