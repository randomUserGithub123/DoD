use crate::worker::WorkerMessage;
use crate::global_order_maker::GlobalOrderMakerMessage;
use petgraph::graphmap::DiGraphMap;
use config::WorkerId;
use graph::LocalOrderGraph;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

/// Indicates a serialized `WorkerMessage::Batch` message.
pub type SerializedTxResponseMessage = Vec<u8>;
type Node = u64;

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct StorageHelper;

impl StorageHelper {
    pub fn spawn(
        // Our worker's id.
        _id: WorkerId,
        // The persistent storage.
        _store: Store,
        // Input channel to receive batches.
        mut rx_batch: Receiver<SerializedTxResponseMessage>,
    ) {
        tokio::spawn(async move {
            while let Some(batch) = rx_batch.recv().await {
                // store each transaction into the store
                match bincode::deserialize(&batch).unwrap() {
                    WorkerMessage::TxResponse(tx_uid, tx) => {
                        store.write(tx_uid, tx).await;
                    },
                    _ => panic!("StorageHelper::spawn : Unexpected batch"),
                }
            }
        });
    }
}
