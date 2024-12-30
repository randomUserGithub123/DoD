// Copyright(C) Heena Nagda.
use crate::worker::{SerializedBatchDigestMessage, WorkerMessage};
use crate::missing_edge_manager::MissingEdgeManager;
use petgraph::graphmap::DiGraphMap;
use config::WorkerId;
use crypto::Digest;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use primary::WorkerPrimaryMessage;
use std::convert::TryInto;
use std::sync::{Arc};
use futures::lock::Mutex;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use graph::GlobalOrderGraph;
use log::{info, error};
use network::ReliableSender;
use std::net::SocketAddr;
use crypto::PublicKey;
use bytes::Bytes;


/// Indicates a serialized `WorkerMessage::GlobalOrderInfo` message.
pub type SerializedGlobalOrderMessage = Vec<u8>;
type Node = u64;

#[derive(Debug)]
/// Hashes and stores batches, it then outputs the batch's digest.
pub struct GlobalOrderProcessor;

impl GlobalOrderProcessor {
    pub fn spawn(
        // Our worker's name
        name: PublicKey,
        // Our worker's id.
        id: WorkerId,
        // The persistent storage.
        mut store: Store,
        // Object of missing_edge_manager
        missed_edge_manager: Arc<Mutex<MissingEdgeManager>>,
        // Input channel to receive batches.
        mut rx_global_order: Receiver<SerializedGlobalOrderMessage>,
        // Output channel to send out batches' digests.
        tx_digest: Sender<SerializedBatchDigestMessage>,
        // Whether we are processing our own batches or the batches of other nodes.
        own_digest: bool,
        // workers addresses for the broadcast
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
    ) {
        tokio::spawn(async move {
            // TODO: It is GlobalOrderInfo(GlobalOrder, MissedEdgePairs) NOT just GlobalOrder
            let mut network = ReliableSender::new();
            while let Some(global_order) = rx_global_order.recv().await {
                info!("Received Global order to process further. own_digest = {:?}", own_digest);

                    match bincode::deserialize(&global_order).unwrap() {
                        WorkerMessage::GlobalOrderInfo(global_order_graph_serialized, missed_pairs) => {
                            let dag: DiGraphMap<Node, u8> = GlobalOrderGraph::get_dag_deserialized(global_order_graph_serialized);
                            for tx_uid in dag.nodes(){
                                let tx_id_vec = tx_uid.to_be_bytes().to_vec();
                                match store.read(tx_id_vec.clone()).await {
                                    Ok(Some(_data)) => (),
                                    Ok(None) => {
                                        // let mut network = ReliableSender::new();
                                        // TODO : ask other worker about the full transaction against this Tx id
                                        let (names, addresses): (Vec<_>, _) = workers_addresses.iter().cloned().unzip();
                                        let tx_request = WorkerMessage::TxRequest(tx_id_vec, name);
                                        let serialized_tx_request = bincode::serialize(&tx_request).expect("Failed to serialize tx request");
                                        let bytes = Bytes::from(serialized_tx_request.clone());
                                        // let _ = network.broadcast(addresses, bytes).await;
                                    },
                                    Err(e) => error!("{}", e),
                                }
                            }

                            if !own_digest {
                                for (from, to) in &missed_pairs{
                                    let mut missed_edge_manager_lock = missed_edge_manager.lock().await;
                                    missed_edge_manager_lock.add_missing_edge(*from, *to).await;
                                    missed_edge_manager_lock.add_updated_edge(*from, *to, 1).await;
                                }
                            }
                        },
                        _ => panic!("GlobalOrderProcessor::spawn : Unexpected OthersBatch"),
                    }

                // Hash the batch.
                let digest = Digest(Sha512::digest(&global_order).as_slice()[..32].try_into().unwrap());

                // Store the batch.
                store.write(digest.to_vec(), global_order).await;

                // Deliver the batch's digest.
                let message = match own_digest {
                    true => WorkerPrimaryMessage::OurBatch(digest, id),
                    false => WorkerPrimaryMessage::OthersBatch(digest, id),
                };
                info!("Sending digest to primary connector. own_digest = {:?}", own_digest);
                let message = bincode::serialize(&message)
                    .expect("Failed to serialize our own worker-primary message");
                tx_digest
                    .send(message)
                    .await
                    .expect("Failed to send digest");
            }
        });
    }
}
