// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::Certificate;
use crate::primary::PrimaryWorkerMessage;
use bytes::Bytes;
use config::Committee;
use crypto::PublicKey;
use network::{ReliableSender, SimpleSender};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use log::{info};

/// Receives the highest round reached by consensus and update it for all tasks.
pub struct GarbageCollector {
    /// The current consensus round (used for cleanup).
    consensus_round: Arc<AtomicU64>,
    /// Receives the ordered certificates from consensus.
    rx_consensus: Receiver<Certificate>,
    /// The network addresses of our workers.
    addresses: Vec<SocketAddr>,
    /// A network sender to notify our workers of cleanup events.
    // network: SimpleSender,
    network: ReliableSender,
}

impl GarbageCollector {
    pub fn spawn(
        name: &PublicKey,
        committee: &Committee,
        consensus_round: Arc<AtomicU64>,
        rx_consensus: Receiver<Certificate>,
    ) {
        let addresses = committee
            .our_workers(name)
            .expect("Our public key or worker id is not in the committee")
            .iter()
            .map(|x| x.primary_to_worker)
            .collect();

        tokio::spawn(async move {
            Self {
                consensus_round,
                rx_consensus,
                addresses,
                // network: SimpleSender::new(),
                network: ReliableSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        let mut last_committed_round = 0;
        while let Some(certificate) = self.rx_consensus.recv().await {
            // TODO [issue #9]: Re-include batch digests that have not been sequenced into our next block.

            // for digest in certificate.header.payload.keys() {
            //     info!("sequenced digest = {:?} for execution", *digest);
            // }

            // Channel ordering towards workers
            let execution_bytes = bincode::serialize(&PrimaryWorkerMessage::Execute(certificate.clone()))
                .expect("Failed to serialize execution message");
            for address in &self.addresses {
                self.network
                    .send(*address, Bytes::from(execution_bytes.clone()))
                    .await;
            }
            // self.network
            //     .broadcast(self.addresses.clone(), Bytes::from(execution_bytes))
            //     .await;
            log::info!("GC Broadcast Execute");

            let round = certificate.round();
            if round > last_committed_round {
                last_committed_round = round;

                // Trigger cleanup on the primary.
                self.consensus_round.store(round, Ordering::Relaxed);

                // Trigger cleanup on the workers..
                let bytes = bincode::serialize(&PrimaryWorkerMessage::Cleanup(round))
                    .expect("Failed to serialize our own message");
                for address in &self.addresses {
                    self.network
                        .send(*address, Bytes::from(bytes.clone()))
                        .await;
                }
                // self.network
                //     .broadcast(self.addresses.clone(), Bytes::from(bytes))
                //     .await;
            }
        }
    }
}
