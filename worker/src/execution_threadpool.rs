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
use tokio::time::Instant;
use std::sync::atomic::{AtomicU64, Ordering};

// Global counters for cross-worker visibility
static POOL_ACTIVE_TASKS: AtomicU64 = AtomicU64::new(0);
static POOL_COMPLETED_TASKS: AtomicU64 = AtomicU64::new(0);

/// ThreadWorker struct that represents an async worker
struct ThreadWorker {
    id: usize,
    handle: JoinHandle<()>,
    store: Store,
    writer_store: Arc<futures::lock::Mutex<WriterStore>>,
    sb_handler: SmallBankTransactionHandler,
    tx_done: tokio::sync::mpsc::UnboundedSender<u64>,
}

impl ThreadWorker {
    /// Starts a worker that listens for messages from the channel
    fn new(
        id: usize, 
        receiver: Arc<Mutex<mpsc::Receiver<u64>>>, 
        mut store: Store,
        writer_store: Arc<futures::lock::Mutex<WriterStore>>, 
        mut sb_handler: SmallBankTransactionHandler, 
        tx_done: tokio::sync::mpsc::UnboundedSender<u64>
    ) -> ThreadWorker {
        
        let mut store_clone = store.clone();
        let mut sb_handler_clone = sb_handler.clone();
        let writer_store_clone = Arc::clone(&writer_store);
        let tx_done_clone = tx_done.clone();
        
        let handle = task::spawn(async move {
            info!("TRACE_TW: ThreadWorker {} started", id);
            let mut processed_count: u64 = 0;

            loop {
                let recv_start = Instant::now();
                let msg = {
                    let mut rx = receiver.lock().await;
                    let lock_time = recv_start.elapsed();
                    if lock_time.as_millis() > 100 {
                        warn!("TRACE_TW: worker={} receiver lock took {:?} (SLOW)", id, lock_time);
                    }
                    rx.recv().await
                };

                match msg {
                    Some(tx_uid) => {
                        processed_count += 1;
                        POOL_ACTIVE_TASKS.fetch_add(1, Ordering::Relaxed);
                        let task_start = Instant::now();
                        info!("TRACE_TW: worker={} tx_uid={} START (recv_wait={:?}, active_tasks={}, completed_global={})", 
                            id, tx_uid, recv_start.elapsed(),
                            POOL_ACTIVE_TASKS.load(Ordering::Relaxed),
                            POOL_COMPLETED_TASKS.load(Ordering::Relaxed));

                        let tx_id_vec = tx_uid.to_be_bytes().to_vec();

                        // ---- POTENTIAL HANG POINT 1: writer_store lock ----
                        info!("TRACE_TW: worker={} tx_uid={} LOCKING writer_store", id, tx_uid);
                        let ws_lock_start = Instant::now();
                        let mut writer_store_lock = writer_store_clone.lock().await;
                        let ws_lock_time = ws_lock_start.elapsed();
                        if ws_lock_time.as_millis() > 50 {
                            warn!("TRACE_TW: worker={} tx_uid={} writer_store lock took {:?} (SLOW)", id, tx_uid, ws_lock_time);
                        } else {
                            info!("TRACE_TW: worker={} tx_uid={} got writer_store lock in {:?}", id, tx_uid, ws_lock_time);
                        }

                        if writer_store_lock.writer_exists(tx_uid) {
                            // ---- POTENTIAL HANG POINT 2: store read while holding writer_store lock ----
                            info!("TRACE_TW: worker={} tx_uid={} reading store (HOLDING writer_store lock!)", id, tx_uid);
                            let store_read_start = Instant::now();
                            match store_clone.read(tx_id_vec.clone()).await {
                                Ok(Some(tx)) => {
                                    let store_read_time = store_read_start.elapsed();
                                    if store_read_time.as_millis() > 50 {
                                        warn!("TRACE_TW: worker={} tx_uid={} store.read SLOW {:?}", id, tx_uid, store_read_time);
                                    } else {
                                        info!("TRACE_TW: worker={} tx_uid={} store.read OK in {:?}", id, tx_uid, store_read_time);
                                    }
                                    
                                    // ---- POTENTIAL HANG POINT 3: transaction execution ----
                                    let exec_start = Instant::now();
                                    sb_handler_clone.execute_transaction(Bytes::from(tx));
                                    let exec_time = exec_start.elapsed();
                                    if exec_time.as_millis() > 50 {
                                        warn!("TRACE_TW: worker={} tx_uid={} execute_transaction SLOW {:?}", id, tx_uid, exec_time);
                                    } else {
                                        info!("TRACE_TW: worker={} tx_uid={} execute_transaction in {:?}", id, tx_uid, exec_time);
                                    }

                                    {
                                        let writer = writer_store_lock.get_writer(tx_uid);
                                        let writer_clone = Arc::clone(&writer);
                                        drop(writer_store_lock); // Release WriterStore lock early
                                        info!("TRACE_TW: worker={} tx_uid={} dropped writer_store lock, now LOCKING writer", id, tx_uid);

                                        // ---- POTENTIAL HANG POINT 4: writer lock ----
                                        let writer_lock_start = Instant::now();
                                        let mut writer_lock = writer_clone.lock().await;
                                        let writer_lock_time = writer_lock_start.elapsed();
                                        if writer_lock_time.as_millis() > 50 {
                                            warn!("TRACE_TW: worker={} tx_uid={} writer lock SLOW {:?}", id, tx_uid, writer_lock_time);
                                        } else {
                                            info!("TRACE_TW: worker={} tx_uid={} got writer lock in {:?}", id, tx_uid, writer_lock_time);
                                        }

                                        // ---- POTENTIAL HANG POINT 5: writer send (TCP) ----
                                        let send_start = Instant::now();
                                        let send_result = writer_lock.send(Bytes::from(tx_id_vec)).await;
                                        let send_time = send_start.elapsed();
                                        if send_time.as_millis() > 100 {
                                            warn!("TRACE_TW: worker={} tx_uid={} writer.send SLOW {:?} result={:?}", id, tx_uid, send_time, send_result.is_ok());
                                        } else {
                                            info!("TRACE_TW: worker={} tx_uid={} writer.send in {:?}", id, tx_uid, send_time);
                                        }

                                        log::info!(
                                            "TX_FINALIZED: tx_uid={}",
                                            tx_uid
                                        );
                                        
                                        // ---- POTENTIAL HANG POINT 6: re-acquiring writer_store lock for delete ----
                                        info!("TRACE_TW: worker={} tx_uid={} RE-LOCKING writer_store for delete", id, tx_uid);
                                        let ws_relock_start = Instant::now();
                                        let mut writer_store_lock = writer_store_clone.lock().await;
                                        let ws_relock_time = ws_relock_start.elapsed();
                                        if ws_relock_time.as_millis() > 50 {
                                            warn!("TRACE_TW: worker={} tx_uid={} re-lock writer_store SLOW {:?}", id, tx_uid, ws_relock_time);
                                        } else {
                                            info!("TRACE_TW: worker={} tx_uid={} re-locked writer_store in {:?}", id, tx_uid, ws_relock_time);
                                        }
                                        writer_store_lock.delete_writer(tx_uid);
                                    }                        
                                }
                                Ok(None) => {
                                    drop(writer_store_lock);
                                    error!("TRACE_TW: worker={} tx_uid={} NOT FOUND in store (store.read returned None)", id, tx_uid);
                                },
                                Err(e) => {
                                    drop(writer_store_lock);
                                    error!("TRACE_TW: worker={} tx_uid={} store error: {}", id, tx_uid, e);
                                },
                            }
                        } else {
                            drop(writer_store_lock);
                            log::warn!(
                                "TRACE_TW: worker={} TX_SKIPPED tx_uid={} (not in writer_store)",
                                id, tx_uid
                            );
                        }

                        // ---- POTENTIAL HANG POINT 7: tx_done send ----
                        info!("TRACE_TW: worker={} tx_uid={} sending tx_done total_task_time={:?}", id, tx_uid, task_start.elapsed());
                        let done_result = tx_done_clone.send(tx_uid);
                        if done_result.is_err() {
                            error!("TRACE_TW: worker={} tx_uid={} FAILED to send tx_done!", id, tx_uid);
                        }
                        POOL_ACTIVE_TASKS.fetch_sub(1, Ordering::Relaxed);
                        POOL_COMPLETED_TASKS.fetch_add(1, Ordering::Relaxed);
                        info!("TRACE_TW: worker={} tx_uid={} DONE processed_so_far={} total_task_time={:?}", id, tx_uid, processed_count, task_start.elapsed());
                    }
                    None => {
                        info!("TRACE_TW: ThreadWorker {} shutting down after processing {} tasks", id, processed_count);
                        break;
                    }
                }
            }
        });

        ThreadWorker { 
            id, 
            handle,
            store,
            writer_store,
            sb_handler,
            tx_done,
        }
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
        tx_done: mpsc::UnboundedSender<u64>
    ) -> ExecutionThreadPool {
        let (sender, receiver) = mpsc::channel::<u64>(100);
        let receiver = Arc::new(Mutex::new(receiver));
        let mut thread_workers = Vec::with_capacity(size);

        // Reset global counters
        POOL_ACTIVE_TASKS.store(0, Ordering::Relaxed);
        POOL_COMPLETED_TASKS.store(0, Ordering::Relaxed);

        info!("TRACE_TP: creating ExecutionThreadPool size={}", size);
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
        info!("TRACE_TP: ExecutionThreadPool created with {} workers", size);

        ExecutionThreadPool { sender, thread_workers }
    }

    /// Sends a message to the worker pool
    pub async fn send_message(&self, message: u64) {
        info!("TRACE_TP: send_message tx_uid={}", message);
        let send_start = Instant::now();
        match self.sender.send(message).await {
            Err(e) => {
                error!("TRACE_TP: FAILED to send tx_uid={} to worker: {} (elapsed={:?})", message, e, send_start.elapsed());
            }
            Ok(()) => {
                let send_time = send_start.elapsed();
                if send_time.as_millis() > 50 {
                    warn!("TRACE_TP: send_message tx_uid={} SLOW send {:?} (channel may be full)", message, send_time);
                } else {
                    info!("TRACE_TP: send_message tx_uid={} sent OK in {:?}", message, send_start.elapsed());
                }
            }
        }
    }

    /// Graceful shutdown: Wait for all thread_workers to finish
    pub async fn shutdown(self) {
        info!("TRACE_TP: shutdown START — dropping sender (active={}, completed={})", 
            POOL_ACTIVE_TASKS.load(Ordering::Relaxed),
            POOL_COMPLETED_TASKS.load(Ordering::Relaxed));
        drop(self.sender);
        for thread_worker in self.thread_workers {
            info!("TRACE_TP: shutdown waiting for worker {}", thread_worker.id);
            let wait_start = Instant::now();
            match thread_worker.handle.await {
                Err(e) => {
                    error!("TRACE_TP: Worker {} error: {:?} (waited {:?})", thread_worker.id, e, wait_start.elapsed());
                }
                Ok(()) => {
                    info!("TRACE_TP: Worker {} joined OK in {:?}", thread_worker.id, wait_start.elapsed());
                }
            }
        }
        info!("TRACE_TP: shutdown COMPLETE (total_completed={})", POOL_COMPLETED_TASKS.load(Ordering::Relaxed));
    }
}