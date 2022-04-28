use crate::config::{Committee, Parameters, Protocol};
use crate::core::ConsensusMessage;
use crate::error::ConsensusResult;
use crate::filter::Filter;
use crate::mempool::ConsensusMempoolMessage;
use crate::messages::Block;
use crate::OptimisticCompiler;
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
        _protocol: Protocol,
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

        // Make the synchronizer. This instance runs in a background thread
        // and asks other nodes for any block that we may be missing.
        // let synchronizer = Synchronizer::new(
        //     name,
        //     committee.clone(),
        //     store.clone(),
        //     /* network_filter */ tx_filter.clone(),
        //     /* core_channel */ tx_core,
        //     parameters.sync_retry_delay,
        // )
        // .await;

        let mut optimistic_compiler = OptimisticCompiler::new(
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

        optimistic_compiler.run().await;

        // match protocol {
        //     Protocol::HotStuff => {
        //         // Run HotStuff
        //         let mut core = Core::new(
        //             name,
        //             committee,
        //             parameters,
        //             signature_service,
        //             store,
        //             leader_elector,
        //             mempool_driver,
        //             synchronizer,
        //             /* core_channel */ rx_core,
        //             /* network_filter */ tx_filter,
        //             /* commit_channel */ tx_commit,
        //         );
        //         tokio::spawn(async move {
        //             core.run().await;
        //         });
        //     }
        //     Protocol::AsyncHotStuff => {
        //         // Run AsyncHotStuff
        //         let mut hotstuff_with_fallback = Fallback::new(
        //             name,
        //             committee,
        //             parameters,
        //             signature_service,
        //             pk_set,
        //             store,
        //             leader_elector,
        //             mempool_driver,
        //             synchronizer,
        //             /* core_channel */ rx_core,
        //             /* network_filter */ tx_filter,
        //             /* commit_channel */ tx_commit,
        //             false,
        //         );
        //         tokio::spawn(async move {
        //             hotstuff_with_fallback.run().await;
        //         });
        //     }
        //     Protocol::TwoChainVABA => {
        //         // Run TwoChainVABA, which is just fallback with timeout=0, i.e., immediately send timeout after exiting a fallback
        //         let mut vaba = Fallback::new(
        //             name,
        //             committee,
        //             parameters,
        //             signature_service,
        //             pk_set,
        //             store,
        //             leader_elector,
        //             mempool_driver,
        //             synchronizer,
        //             /* core_channel */ rx_core,
        //             /* network_filter */ tx_filter,
        //             /* commit_channel */ tx_commit,
        //             true, // running vaba
        //         );
        //         tokio::spawn(async move {
        //             vaba.run().await;
        //         });
        //     }
        //     _ => {
        //         return Ok(());
        //     }
        // }

        Ok(())
    }
}
