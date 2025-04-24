// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::WorkerMessage;
// use crate::worker::SerializedBatchDigestMessage;
use crate::global_order_maker::GlobalOrderMakerMessage;
use petgraph::graphmap::DiGraphMap;
use config::WorkerId;
use graph::LocalOrderGraph;
use log::info;
// use crypto::Digest;
// use ed25519_dalek::Digest as _;
// use ed25519_dalek::Sha512;
// use primary::WorkerPrimaryMessage;
// use std::convert::TryInto;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/processor_tests.rs"]
pub mod processor_tests;

/// Indicates a serialized `WorkerMessage::Batch` message.
pub type SerializedBatchMessage = Vec<u8>;
type Node = u64;

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct Processor;

impl Processor {
    pub fn spawn(
        // Our worker's id.
        _id: WorkerId,
        // The persistent storage.
        _store: Store,
        // Input channel to receive batches.
        mut rx_batch: Receiver<SerializedBatchMessage>,
        /*// Output channel to send out batches' digests.
        tx_digest: Sender<SerializedBatchDigestMessage>,*/
        // Output channel to send out batch to global order maker
        tx_global_order_batch: Sender<GlobalOrderMakerMessage>,
        // Whether we are processing our own batches or the batches of other nodes.
        own_digest: bool,
    ) {
        tokio::spawn(async move {
            while let Some(batch) = rx_batch.recv().await {
                // store each transaction into the store
                match bincode::deserialize(&batch).unwrap() {
                    WorkerMessage::Batch(mut serialized_local_order_graph_obj) => {
                        let _ = serialized_local_order_graph_obj.pop();
                        let dag: DiGraphMap<Node, u8> = LocalOrderGraph::get_dag_deserialized(serialized_local_order_graph_obj);
                        for node in dag.nodes(){
                            // TODO : Tx id 
                            // info!("Processor::spawn : tx_uid = {:?}", node);
                        }
                    },
                    _ => panic!("Processor::spawn : Unexpected batch"),
                }
                // info!("Processor::spawn : done processing batch");

                // send batch to global order
                let message1 = GlobalOrderMakerMessage {batch:batch.clone(), own_digest:own_digest}; 
                tx_global_order_batch
                .send(message1)
                .await
                .expect("Failed to send batch to `global order maker`");

                // // Hash the batch.
                // let digest = Digest(Sha512::digest(&batch).as_slice()[..32].try_into().unwrap());

                // // Store the batch.
                // store.write(digest.to_vec(), batch).await;

                // // Deliver the batch's digest.
                // let message = match own_digest {
                //     true => WorkerPrimaryMessage::OurBatch(digest, id),
                //     false => WorkerPrimaryMessage::OthersBatch(digest, id),
                // };
                // let message = bincode::serialize(&message)
                //     .expect("Failed to serialize our own worker-primary message");
                // tx_digest
                //     .send(message)
                //     .await
                //     .expect("Failed to send digest");
            }
        });
    }
}
