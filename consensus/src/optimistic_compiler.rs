use crate::config::{Committee, Parameters};
use crate::core::{ConsensusMessage, Core};
use crate::fallback::Fallback;
use crate::filter::FilterInput;
use crate::leader::LeaderElector;
use crate::mempool::{ConsensusMempoolMessage, MempoolDriver};
use crate::messages::{Block, RecoveryVote};
use crate::synchronizer::Synchronizer;
use crate::{SeqNumber, MempoolWrapper};
use crypto::{Digest, PublicKey, SignatureService};
use log::{debug, info};
use std::collections::VecDeque;
use std::convert::TryInto;
use store::Store;
use threshold_crypto::PublicKeySet;
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
    era: SeqNumber,
    loopback: usize,
    l: usize,
    k: usize,
    k_voted: usize,
    state: State,
    signature_service: SignatureService,
    main_chain: Vec<Block>,
    vaba_chain: Vec<Block>,
    rx_main: Receiver<ConsensusMessage>, // Incoming consensus messages
    rx_event: Receiver<Event>,           // Events from sub protocols
    rx_blocks: Receiver<VecDeque<Block>>, // Blocks to add to the main chain from sub protocols
    tx_jolteon: Sender<ConsensusMessage>, // Channel for forwarding messages to sub protocols
    tx_vaba: Sender<ConsensusMessage>,   // Channel for forwarding messages to sub protocols
    tx_cert: Sender<Digest>, // Used to send recovery certificates to the mempool wrapper
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

        // Channel for sending blocks from the sub protocols to the main protocol.
        let (tx_blocks, rx_blocks) = channel(1_000);

        // MempoolWrapper which acts as a buffer, such that both sub protocols receive the
        // same transactions.
        let mempool_driver = MempoolDriver::new(tx_consensus_mempool.clone());
        let (tx_wrapper, rx_wrapper) = channel(1_000);
        let max_payload_size = parameters.clone().max_payload_size;
        let (tx_cert, rx_cert) = channel(1_000);
        let mut mempool_wrapper =
            MempoolWrapper::new(max_payload_size, mempool_driver, rx_wrapper, rx_cert);
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
            debug!("Starting vaba");
            vaba.run().await;
        });

        // Run jolteon
        tokio::spawn(async move {
            debug!("Starting jolteon");
            jolteon.run().await;
        });

        Self {
            name,
            era: 1,
            loopback: 10,
            l: 0,
            k: 0,
            k_voted: 0,
            state: State::Steady,
            signature_service: signature_service.clone(),
            main_chain: Vec::new(),
            vaba_chain: Vec::new(),
            rx_main,
            rx_event,
            rx_blocks,
            tx_jolteon,
            tx_vaba,
            tx_cert,
        }
    }

    async fn handle_message(&mut self, message: ConsensusMessage) {
        match message {
            ConsensusMessage::Recovery(rv) => self.handle_recovery_vote(rv),
            _ => self.forward_message(message).await,
        }
    }

    fn handle_blocks(&mut self, mut blocks: VecDeque<Block>) {
        // Received block(s) from jolteon that can be appended to the
        // main chain.
        debug!("Received block(s) from jolteon {:?}", blocks);
        while let Some(block) = blocks.pop_back() {
            if !block.payload.is_empty() {
                info!("Committed {}", block);

                #[cfg(feature = "benchmark")]
                for x in &block.payload {
                    // NOTE: This log entry is used to compute performance.
                    info!("Committed TX({})", base64::encode(x));
                }
            }
            self.main_chain.push(block);
        }

        self.ss_try_resolve();
    }

    async fn handle_event(&mut self, event: Event) {
        // Received an event notification by one of the two sub protocols.
        debug!("Received event! {:?}", event);
        match self.state {
            State::Steady => {
                match event {
                    Event::VabaOut(block) => {
                        // debug!("Testing Recovery Vote!");
                        // let b = block.clone();
                        // let recovery_vote = RecoveryVote::new(12, 2, self.signature_service.clone(), b.qc, self.name).await;
                        // debug!("Recovery vote: {:?}", recovery_vote);
                        // let res = recovery_vote.verify();
                        // debug!("Result of verification: {:?}", res);
                        self.vaba_chain.push(block);
                        self.ss_try_resolve();
                        self.rs_try_vote().await;
                    }
                    _ => {
                        // Vote, Lock, Advance
                        self.ss_try_resolve();
                    }
                }
            }
            State::Recovery => match event {
                Event::VabaOut(block) => {
                    self.vaba_chain.push(block);
                    // We received a qc, so we need to call rs_try_vote
                    self.rs_try_vote().await;
                    self.rs_try_resolve().await;
                }
                _ => {}
            },
        }
    }

    fn handle_recovery_vote(&mut self, rv: RecoveryVote) {
        debug!("Received recovery vote {:?}", rv);
        // TODO: implement me
        // TODO: check signature
        // TODO: add to aggregator
        // TODO: if we have enough recovery votes for era e input to vaba
    }

    fn switch_to_steady(&mut self) {
        // TODO: implement me
        self.era += 1;
        self.state = State::Steady;
        self.ss_try_resolve();
        // TODO: begin running jolteon
    }

    async fn switch_to_recovery(&mut self) {
        // TODO: implement me
        self.state = State::Recovery;
        self.rs_try_vote().await;
        self.rs_try_resolve().await;
    }

    /* Steady state functions */

    fn ss_try_resolve(&mut self) {
        if self._ss_try_resolve() {
            // TODO: switch to recovery
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
                let x = tx.clone();
                if !self.certified_on_time(x) {
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
        // TODO: implement me
        // TODO: check if tx is already in fast chain. If no -> false
        for b in &self.main_chain {
            if b.payload.contains(&tx) {
                debug!("Main chain contains {}", tx);
                return true;
            }
        }
        debug!(
            "Tx {} not yet in main chain. len main: {}. len vaba: {}",
            tx,
            self.main_chain.len(),
            self.vaba_chain.len()
        );
        true
    }

    /* Recovery state functions */

    async fn rs_try_resolve(&mut self) {
        if self._rs_try_resolve().await {}
    }

    async fn _rs_try_resolve(&mut self) -> bool {
        // TODO: implement me
        if self.vaba_chain.len() < self.l {
            return false;
        }
        // if there is no rc in vaba[l]:
        //      l++
        //      return rsTryResolve(l)
        // if there is no qc for every k <= rc.index:
        //      return false
        // if we are here set blocks to main chain, l++ and return true
        false
    }

    async fn rs_try_vote(&mut self) {
        // TODO: implement me
        let k = self.main_chain.len();
        debug!("rsTryVote k_voted: {} k: {}", self.k_voted, k);
        for i in self.k_voted..k {
            for b in &self.main_chain {
                if b.qc.round == (i as u64) {
                    //debug!("IT'S A MATCH! index: {} era: {}", i, b.qc.view);
                }
            }
        }
        //self.k_voted = k;
    }

    fn handle_certificate(&mut self, tx: &Digest) {
        let p = tx.to_vec();
        if p[0] == 2 && p[1] == 2 && p[2] == 2 && p[3] == 2 {
            debug!("Recoverycert! {:?}", tx);
        }
    }

    async fn forward_message(&mut self, message: ConsensusMessage) {
        // TODO: replace unwrap with expect
        match message {
            // Messages used by jolteon
            ConsensusMessage::ProposeJolteon(_) => self.tx_jolteon.send(message).await.unwrap(),
            ConsensusMessage::VoteJolteon(_) => self.tx_jolteon.send(message).await.unwrap(),
            ConsensusMessage::TimeoutJolteon(_) => self.tx_jolteon.send(message).await.unwrap(),
            ConsensusMessage::TCJolteon(_) => self.tx_jolteon.send(message).await.unwrap(),
            ConsensusMessage::SignedQCJolteon(_) => self.tx_jolteon.send(message).await.unwrap(),
            ConsensusMessage::RandomnessShareJolteon(_) => {
                self.tx_jolteon.send(message).await.unwrap()
            }
            ConsensusMessage::RandomCoinJolteon(_) => self.tx_jolteon.send(message).await.unwrap(),
            ConsensusMessage::SyncRequestJolteon(_, _) => {
                self.tx_jolteon.send(message).await.unwrap()
            }
            ConsensusMessage::SyncReplyJolteon(_) => self.tx_jolteon.send(message).await.unwrap(),

            // TODO: THIS CURRENTLY GETS IGNORED
            ConsensusMessage::LoopBack(_) => {
                //tx_jolteon.send(message).await.unwrap()
                //tx_vaba.send(message).await.unwrap()
            }

            // Messages used by vaba
            ConsensusMessage::ProposeVaba(_) => self.tx_vaba.send(message).await.unwrap(),
            ConsensusMessage::VoteVaba(_) => self.tx_vaba.send(message).await.unwrap(),
            ConsensusMessage::TimeoutVaba(_) => self.tx_vaba.send(message).await.unwrap(),
            ConsensusMessage::TCVaba(_) => self.tx_vaba.send(message).await.unwrap(),
            ConsensusMessage::SignedQCVaba(_) => self.tx_vaba.send(message).await.unwrap(),
            ConsensusMessage::RandomnessShareVaba(_) => self.tx_vaba.send(message).await.unwrap(),
            ConsensusMessage::RandomCoinVaba(_) => self.tx_vaba.send(message).await.unwrap(),
            ConsensusMessage::SyncRequestVaba(_, _) => self.tx_vaba.send(message).await.unwrap(),
            ConsensusMessage::SyncReplyVaba(_) => self.tx_vaba.send(message).await.unwrap(),

            _ => debug!("Wrong message type {:?}", message),
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(message) = self.rx_main.recv() => self.handle_message(message).await,
                Some(blocks) = self.rx_blocks.recv() => self.handle_blocks(blocks),
                Some(event) = self.rx_event.recv() => self.handle_event(event).await
            }
        }
    }
}
