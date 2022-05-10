use crate::aggregator::Aggregator;
use crate::config::{Committee, Parameters};
use crate::core::{ConsensusMessage, Core};
use crate::error::ConsensusResult;
use crate::fallback::Fallback;
use crate::filter::FilterInput;
use crate::leader::LeaderElector;
use crate::mempool::{ConsensusMempoolMessage, MempoolDriver};
use crate::messages::{Block, RecoveryVote};
use crate::synchronizer::Synchronizer;
use crate::{MempoolWrapper, SeqNumber};
use async_recursion::async_recursion;
use crypto::{Digest, PublicKey, SignatureService};
use log::{debug, info, warn};
use std::collections::VecDeque;
use std::convert::TryInto;
use store::Store;
use threshold_crypto::PublicKeySet;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[derive(Debug)]
enum State {
    Steady,
    Recovery,
}

#[derive(Debug)]
pub enum SubProto {
    Jolteon,
    Vaba,
}

#[derive(Debug)]
pub enum Event {
    Vote,
    Lock,
    Advance,
    VabaOut(Block),
}

pub struct OptimisticCompiler {
    name: PublicKey,
    committee: Committee,
    era: SeqNumber,
    loopback: usize,
    l: usize,
    k_voted: usize,
    state: State,
    aggregator: Aggregator,
    signature_service: SignatureService,
    network_filter: Sender<FilterInput>,
    main_chain: Vec<Block>,
    vaba_chain: Vec<Block>,
    rx_main: Receiver<ConsensusMessage>, // Incoming consensus messages
    rx_event: Receiver<Event>,           // Events from sub protocols
    rx_blocks: Receiver<VecDeque<Block>>, // Blocks to add to the main chain from sub protocols
    tx_jolteon: Sender<ConsensusMessage>, // Channel for forwarding messages to sub protocols
    tx_vaba: Sender<ConsensusMessage>,   // Channel for forwarding messages to sub protocols
    tx_cert: Sender<Digest>, // Used to send recovery certificates to the mempool wrapper
    tx_remove: Sender<Vec<Digest>>, // Used to send signal the mempool wrapper which transactions can be removed
    tx_stop_start: Sender<()>,      // Used to stop and start jolteon
}

impl OptimisticCompiler {
    pub async fn new(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        signature_service: SignatureService,
        pk_set: PublicKeySet,
        store: Store,
        rx_main: Receiver<ConsensusMessage>,
        tx_main: Sender<ConsensusMessage>,
        tx_filter: Sender<FilterInput>,
        tx_commit: Sender<Block>,
        tx_consensus_mempool: Sender<ConsensusMempoolMessage>,
    ) -> Self {
        // Channel for receiving and sending events for the sub protocols.
        let (tx_event, rx_event) = channel(1_000);

        // Channel to stop and start jolteon
        let (tx_stop_start, rx_stop_start) = channel(100);

        // Channel for sending blocks from the sub protocols to the main protocol.
        let (tx_blocks, rx_blocks) = channel(1_000);

        // MempoolWrapper which acts as a buffer, such that both sub protocols receive the
        // same transactions.
        let mempool_driver = MempoolDriver::new(tx_consensus_mempool.clone());
        let (tx_wrapper, rx_wrapper) = channel(1_000);
        let max_payload_size = parameters.clone().max_payload_size;
        let (tx_cert, rx_cert) = channel(1_000);
        let (tx_remove, rx_remove) = channel(1_000);
        let mut mempool_wrapper = MempoolWrapper::new(
            max_payload_size,
            mempool_driver,
            rx_wrapper,
            rx_cert,
            rx_remove,
        );
        tokio::spawn(async move {
            mempool_wrapper.run().await;
        });

        // Channels for forwarding messages to the correct subprotocol.
        let (tx_jolteon, rx_jolteon) = channel(10_000);
        let (tx_vaba, rx_vaba) = channel(10_000);

        // Create synchronizer for jolteon
        let sync_retry_delay = parameters.clone().sync_retry_delay;
        let synchronizer_jolteon = Synchronizer::new(
            name.clone(),
            committee.clone(),
            store.clone(),
            /* network_filter */ tx_filter.clone(),
            /* core_channel */ tx_main.clone(),
            sync_retry_delay.clone(),
            SubProto::Jolteon,
        )
        .await;

        // Create synchronizer for vaba
        let sync_retry_delay = parameters.clone().sync_retry_delay;
        let synchronizer_vaba = Synchronizer::new(
            name.clone(),
            committee.clone(),
            store.clone(),
            /* network_filter */ tx_filter.clone(),
            /* core_channel */ tx_main.clone(),
            sync_retry_delay.clone(),
            SubProto::Vaba,
        )
        .await;

        // Create one jolteon instance
        let mut jolteon = Core::new(
            name.clone(),
            committee.clone(),
            parameters.clone(),
            signature_service.clone(),
            store.clone(),
            LeaderElector::new(committee.clone()),
            MempoolDriver::new(tx_consensus_mempool.clone()),
            synchronizer_jolteon,
            /* core_channel */ rx_jolteon,
            /* network_filter */ tx_filter.clone(),
            /* commit_channel */ tx_commit.clone(),
            tx_event.clone(),
            tx_wrapper.clone(),
            tx_blocks.clone(),
            rx_stop_start,
        );

        // Create one vaba instance
        let mut vaba = Fallback::new(
            name.clone(),
            committee.clone(),
            parameters.clone(),
            signature_service.clone(),
            pk_set.clone(),
            store.clone(),
            LeaderElector::new(committee.clone()),
            MempoolDriver::new(tx_consensus_mempool.clone()),
            synchronizer_vaba,
            /* core_channel */ rx_vaba,
            /* network_filter */ tx_filter.clone(),
            /* commit_channel */ tx_commit.clone(),
            true, // running vaba
            tx_event.clone(),
            tx_wrapper.clone(),
        );

        // Run vaba
        tokio::spawn(async move {
            vaba.run().await;
        });

        // Run jolteon
        tokio::spawn(async move {
            jolteon.run().await;
        });

        Self {
            name,
            committee: committee.clone(),
            era: 0,
            loopback: 10,
            l: 0,
            k_voted: 0,
            state: State::Steady,
            aggregator: Aggregator::new(committee.clone()),
            signature_service: signature_service.clone(),
            network_filter: tx_filter.clone(),
            main_chain: Vec::new(),
            vaba_chain: Vec::new(),
            rx_main,
            rx_event,
            rx_blocks,
            tx_jolteon,
            tx_vaba,
            tx_cert,
            tx_remove,
            tx_stop_start,
        }
    }

    async fn handle_message(&mut self, message: ConsensusMessage) {
        match message {
            ConsensusMessage::Recovery(rv) => self.handle_recovery_vote(&rv).await,
            _ => {
                self.forward_message(message)
                    .await
                    .expect("Failed to forward message to sub protocol");
            }
        }
    }

    async fn handle_blocks(&mut self, mut blocks: VecDeque<Block>) {
        // Received block(s) that can be appended to the main chain.
        debug!(
            "Received block(s): {:?}. Len main {} Len vaba {}",
            blocks,
            self.main_chain.len(),
            self.vaba_chain.len()
        );
        while let Some(block) = blocks.pop_back() {
            if !block.payload.is_empty() {
                self.tx_remove
                    .send(block.payload.clone())
                    .await
                    .expect("Failed to send transactions to mempool wrapper");
                info!("Committed {}", block);

                #[cfg(feature = "benchmark")]
                for x in &block.payload {
                    if OptimisticCompiler::is_certificate(x.to_vec()) {
                        continue;
                    }
                    // NOTE: This log entry is used to compute performance.
                    info!("Committed TX({})", base64::encode(x));
                }
            }
            self.main_chain.push(block);
        }

        self.ss_try_resolve().await;
    }

    async fn handle_event(&mut self, event: Event) {
        // Received an event notification by one of the two sub protocols.
        match self.state {
            State::Steady => {
                match event {
                    Event::VabaOut(block) => {
                        debug!(
                            "Vaba out: Len main {} Len vaba {}",
                            self.main_chain.len(),
                            self.vaba_chain.len()
                        );
                        self.vaba_chain.push(block);
                        self.ss_try_resolve().await;
                    }
                    _ => {
                        // Vote, Lock, Advance
                        self.ss_try_resolve().await;
                    }
                }
            }
            State::Recovery => match event {
                Event::VabaOut(block) => {
                    debug!(
                        "Vaba out: Len main {} Len vaba {}",
                        self.main_chain.len(),
                        self.vaba_chain.len()
                    );
                    self.vaba_chain.push(block);
                    // We received a qc, so we need to call rs_try_vote
                    self.rs_try_vote().await;
                    self.rs_try_resolve().await;
                }
                _ => {}
            },
        }
    }

    async fn handle_recovery_vote(&mut self, rv: &RecoveryVote) {
        //debug!("Received recovery vote {:?}", rv);
        if let Ok(_) = rv.verify() {
            let res = self.aggregator.add_recovery_vote(rv.clone());
            if res {
                debug!(
                    "Received enough recovery votes for era {}, index {}, inputting cert",
                    rv.era, rv.index
                );
                let cert = self.make_recovery_cert(rv.era, rv.index);
                self.tx_cert
                    .send(cert)
                    .await
                    .expect("Failed to send certificate to mempool wrapper");
            }
        }
    }

    fn make_recovery_cert(&mut self, era: SeqNumber, index: SeqNumber) -> Digest {
        // TODO: use real threshold certificates
        let cert_prefix: &[u8] = [2; 16].as_slice();
        let cert = [
            cert_prefix,
            era.to_be_bytes().as_slice(),
            index.to_be_bytes().as_slice(),
        ]
        .concat();
        Digest(cert.as_slice()[..32].try_into().unwrap())
        // debug!(
        //     "Created certificate for e: {}, i: {} -> {:?}",
        //     era, index, ret
        // );
    }

    #[async_recursion]
    async fn switch_to_steady(&mut self) {
        debug!("Entering steady state. Era {}", self.era + 1);
        self.era += 1;
        self.state = State::Steady;
        self.ss_try_resolve().await;
        self.tx_stop_start
            .send(())
            .await
            .expect("Failed to start jolteon");
    }

    #[async_recursion]
    async fn switch_to_recovery(&mut self) {
        debug!("Entering recovery state");
        self.state = State::Recovery;
        self.tx_stop_start
            .send(())
            .await
            .expect("Failed to stop jolteon");
        self.rs_try_vote().await;
        self.rs_try_resolve().await;
    }

    /* Steady state functions */

    async fn ss_try_resolve(&mut self) {
        if self._ss_try_resolve() {
            self.switch_to_recovery().await;
        }
    }

    fn _ss_try_resolve(&mut self) -> bool {
        if self.vaba_chain.len() < self.l + self.loopback {
            return false;
        } else {
            if self.vaba_chain[self.l].payload.is_empty() {
                self.l += 1;
                return self._ss_try_resolve();
            }
            for tx in &self.vaba_chain[self.l].payload {
                // Ignore recovery certificates
                if OptimisticCompiler::is_certificate(tx.to_vec()) {
                    continue;
                }
                let x = tx.clone();
                if !self.certified_on_time(x.clone()) {
                    // debug!(
                    //     "Tx {} not certified in time in block {:?}",
                    //     &x, &self.vaba_chain[self.l]
                    // );
                    // If there is one tx not certified on time switch to recovery state
                    self.l += 1;
                    return true;
                } else {
                    self.l += 1;
                    return self._ss_try_resolve();
                }
            }
            false
        }
    }

    fn certified_on_time(&mut self, tx: Digest) -> bool {
        for b in &self.main_chain {
            if b.payload.contains(&tx) {
                return true;
            }
        }
        debug!(
            "Tx {} not yet in main chain. len main: {}. len vaba: {}",
            tx,
            self.main_chain.len(),
            self.vaba_chain.len()
        );
        false
    }

    /* Recovery state functions */

    async fn rs_try_resolve(&mut self) {
        if self._rs_try_resolve().await {
            self.switch_to_steady().await;
        }
    }

    #[async_recursion]
    async fn _rs_try_resolve(&mut self) -> bool {
        if self.vaba_chain.len() <= self.l {
            return false;
        }
        for tx in self.vaba_chain[self.l].clone().payload {
            // Check if tx is a certificate
            if OptimisticCompiler::is_certificate(tx.to_vec()) {
                // TODO: check of qc necessary? This should be a guarantee of vaba
                let era = u64::from_be_bytes(tx.to_vec()[16..24].try_into().unwrap());
                let index = u64::from_be_bytes(tx.to_vec()[24..32].try_into().unwrap());
                if era != self.era {
                    debug!(
                        "Got a cert with wrong data e: {}, i: {}, cert: {:?}",
                        era,
                        index,
                        tx.to_vec()
                    );
                    self.l += 1;
                    return self._rs_try_resolve().await;
                }
                debug!(
                    "Got a cert with e: {}, i: {}, cert: {:?}",
                    era,
                    index,
                    tx.to_vec()
                );
                // Set blocks, increment l and return true
                if self.main_chain.len() < self.vaba_chain.len() {
                    let mut queue = VecDeque::new();
                    for i in self.main_chain.len()..self.vaba_chain.len() {
                        queue.push_back(self.vaba_chain[i].clone());
                    }
                    debug!(
                        "Bulk transaction. Adding {} blocks. main {} vaba {}",
                        queue.len(),
                        self.main_chain.len(),
                        self.vaba_chain.len()
                    );
                    self.handle_blocks(queue).await;
                }
                self.l += 1;
                return true;
            }
        }
        self.l += 1;
        return self._rs_try_resolve().await;
    }

    async fn rs_try_vote(&mut self) {
        debug!(
            "rs_try_vote: k_voted: {} len vaba: {}",
            self.k_voted,
            self.vaba_chain.len()
        );
        let mut recovery_votes = Vec::new();
        for i in self.k_voted..self.vaba_chain.len() {
            self.k_voted = i - 1;
            if self.vaba_chain[i].qc.view > self.era {
                break;
            }
            if self.vaba_chain[i].qc.view == self.era {
                debug!(
                    "Matching qc found! era: {} index: {}",
                    self.vaba_chain[i].qc.view, self.vaba_chain[i].qc.round
                );
                let rv = RecoveryVote::new(
                    self.era,
                    self.vaba_chain[i].round,
                    self.signature_service.clone(),
                    self.vaba_chain[i].qc.clone(),
                    self.name,
                )
                .await;
                recovery_votes.push(rv);
            }
        }

        // Send recovery votes
        for rv in recovery_votes {
            match Synchronizer::transmit(
                ConsensusMessage::Recovery(rv.clone()),
                &self.name,
                None,
                &self.network_filter,
                &self.committee,
            )
            .await
            {
                Ok(_) => (),
                Err(e) => warn!("{}", e),
            };
            self.handle_recovery_vote(&rv).await;
        }
    }

    fn is_certificate(digest: Vec<u8>) -> bool {
        for i in 0..16 {
            if digest[i] != 2 {
                return false;
            }
        }
        true
    }

    async fn forward_message(
        &mut self,
        message: ConsensusMessage,
    ) -> Result<(), SendError<ConsensusMessage>> {
        match message {
            // Messages used by jolteon
            ConsensusMessage::ProposeJolteon(_) => self.tx_jolteon.send(message).await,
            ConsensusMessage::VoteJolteon(_) => self.tx_jolteon.send(message).await,
            ConsensusMessage::TimeoutJolteon(_) => self.tx_jolteon.send(message).await,
            ConsensusMessage::TCJolteon(_) => self.tx_jolteon.send(message).await,
            ConsensusMessage::SignedQCJolteon(_) => self.tx_jolteon.send(message).await,
            ConsensusMessage::RandomnessShareJolteon(_) => self.tx_jolteon.send(message).await,
            ConsensusMessage::RandomCoinJolteon(_) => self.tx_jolteon.send(message).await,
            ConsensusMessage::SyncRequestJolteon(_, _) => self.tx_jolteon.send(message).await,
            ConsensusMessage::SyncReplyJolteon(_) => self.tx_jolteon.send(message).await,

            // TODO: This currently gets ignored
            ConsensusMessage::LoopBack(_) => {
                //tx_jolteon.send(message).await.unwrap()
                //tx_vaba.send(message).await.unwrap()
                Ok(())
            }

            // Messages used by vaba
            ConsensusMessage::ProposeVaba(_) => self.tx_vaba.send(message).await,
            ConsensusMessage::VoteVaba(_) => self.tx_vaba.send(message).await,
            ConsensusMessage::TimeoutVaba(_) => self.tx_vaba.send(message).await,
            ConsensusMessage::TCVaba(_) => self.tx_vaba.send(message).await,
            ConsensusMessage::SignedQCVaba(_) => self.tx_vaba.send(message).await,
            ConsensusMessage::RandomnessShareVaba(_) => self.tx_vaba.send(message).await,
            ConsensusMessage::RandomCoinVaba(_) => self.tx_vaba.send(message).await,
            ConsensusMessage::SyncRequestVaba(_, _) => self.tx_vaba.send(message).await,
            ConsensusMessage::SyncReplyVaba(_) => self.tx_vaba.send(message).await,

            _ => {
                warn!("Wrong message type {:?}", message);
                Ok(())
            }
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(message) = self.rx_main.recv() => self.handle_message(message).await,
                Some(blocks) = self.rx_blocks.recv() => self.handle_blocks(blocks).await,
                Some(event) = self.rx_event.recv() => self.handle_event(event).await
            }
        }
    }
}
