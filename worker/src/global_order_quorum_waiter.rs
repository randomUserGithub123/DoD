// Copyright(C) Heena Nagda.
use crate::global_order_processor::SerializedGlobalOrderMessage;
use config::{Committee, Stake};
use crypto::PublicKey;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use network::CancelHandler;
use tokio::sync::mpsc::{Receiver, Sender};
use log::info;

#[derive(Debug)]
pub struct GlobalOrderQuorumWaiterMessage {
    /// A serialized `WorkerMessage::GlobalOrderInfo` message.
    pub global_order_info: SerializedGlobalOrderMessage,
    /// The cancel handlers to receive the acknowledgements of our broadcast.
    pub handlers: Vec<(PublicKey, CancelHandler)>,
}

/// The QuorumWaiter waits for 2f authorities to acknowledge reception of a batch.
pub struct GlobalOrderQuorumWaiter {
    /// The committee information.
    committee: Committee,
    /// The stake of this authority.
    stake: Stake,
    /// Input Channel to receive commands.
    rx_message: Receiver<GlobalOrderQuorumWaiterMessage>,
    /// Channel to deliver batches for which we have enough acknowledgements.
    tx_order: Sender<SerializedGlobalOrderMessage>,
}

impl GlobalOrderQuorumWaiter {
    /// Spawn a new GlobalOrderQuorumWaiter.
    pub fn spawn(
        committee: Committee,
        stake: Stake,
        rx_message: Receiver<GlobalOrderQuorumWaiterMessage>,
        tx_order: Sender<Vec<u8>>,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                stake,
                rx_message,
                tx_order,
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for a future to complete and then delivers a value.
    async fn waiter(wait_for: CancelHandler, deliver: Stake) -> Stake {
        let _ = wait_for.await;
        deliver
    }

    /// Main loop.
    async fn run(&mut self) {
        while let Some(GlobalOrderQuorumWaiterMessage { global_order_info, handlers }) = self.rx_message.recv().await {
            let mut wait_for_quorum: FuturesUnordered<_> = handlers
                .into_iter()
                .map(|(name, handler)| {
                    let stake = self.committee.stake(&name);
                    Self::waiter(handler, stake)
                })
                .collect();

            // Wait for the first 2f nodes to send back an Ack. Then we consider the batch
            // delivered and we send its digest to the primary (that will include it into
            // the dag). This should reduce the amount of synching.
            let mut total_stake = self.stake;
            while let Some(stake) = wait_for_quorum.next().await {
                total_stake += stake;
                // info!("global_order_quorum_waiter::run: total_stake = {:?}", total_stake);
                if total_stake >= self.committee.quorum_threshold() {
                    // info!("Received 2f acks, Sending Global order to the global order processor");
                    self.tx_order
                        .send(global_order_info)
                        .await
                        .expect("Failed to deliver global order to the global order processor");
                    break;
                }
            }
            // info!("global_order_quorum_waiter::run: done delivering global order");
        }
    }
}
