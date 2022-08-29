use crate::config::{Committee, Parameters};
use crate::core::ConsensusMessage;
use crate::error::ConsensusResult;
use crate::filter::Filter;
use crate::mempool::ConsensusMempoolMessage;
use crate::messages::Block;
use crate::Abraxas;
use crypto::{PublicKey, SignatureService};
use log::info;
use network::{NetReceiver, NetSender};
use store::Store;
use threshold_crypto::PublicKeySet;
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;

pub struct Consensus;

impl Consensus {
    #[allow(clippy::too_many_arguments)]
    pub async fn run(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        signature_service: SignatureService,
        pk_set: PublicKeySet, // The set of tss public keys
        tx_core: Sender<ConsensusMessage>,
        rx_core: Receiver<ConsensusMessage>,
        tx_consensus_mempool: Sender<ConsensusMempoolMessage>,
        tx_commit: Sender<Block>,
    ) -> ConsensusResult<()> {
        info!(
            "Consensus timeout delay set to {} ms",
            parameters.timeout_delay
        );
        info!(
            "Consensus synchronizer retry delay set to {} ms",
            parameters.sync_retry_delay
        );
        info!(
            "Consensus max payload size set to {} B",
            parameters.max_payload_size
        );
        info!(
            "Consensus min block delay set to {} ms",
            parameters.min_block_delay
        );
        info!("ddos {}", parameters.ddos);
        info!("random ddos {}", parameters.random_ddos);

        let (tx_network, rx_network) = channel(10000);
        let (tx_filter, rx_filter) = channel(10000);

        // Make the network sender and receiver.
        let address = committee.address(&name).map(|mut x| {
            x.set_ip("0.0.0.0".parse().unwrap());
            x
        })?;
        let network_receiver = NetReceiver::new(address, tx_core.clone());
        tokio::spawn(async move {
            network_receiver.run().await;
        });

        let mut network_sender = NetSender::new(rx_network);
        tokio::spawn(async move {
            network_sender.run().await;
        });

        // Custom filter to arbitrary delay network messages.
        Filter::run(rx_filter, tx_network, parameters.clone());

        let mut abraxas = Abraxas::new(
            name,
            committee,
            parameters,
            signature_service,
            pk_set,
            store,
            rx_core,
            tx_core.clone(),
            tx_filter,
            tx_commit,
            tx_consensus_mempool,
        )
        .await;

        abraxas.run().await;

        Ok(())
    }
}
