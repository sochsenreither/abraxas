use crate::aggregator::Aggregator;
use crate::config::{Committee, Parameters};
use crate::core::{ConsensusMessage, Core};
use crate::error::ConsensusResult;
use crate::fallback::Fallback;
use crate::filter::FilterInput;
use crate::leader::LeaderElector;
use crate::mempool::{ConsensusMempoolMessage, MempoolDriver};
use crate::mempool_wrapper::MempoolCmd;
use crate::messages::{Block, RecoveryVote, RC};
use crate::synchronizer::Synchronizer;
use crate::{MempoolWrapper, MessageHandler, SeqNumber};
use async_recursion::async_recursion;
use crypto::{Digest, Hash, PublicKey, SignatureService};
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet, VecDeque};
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
    VabaOut(VecDeque<Block>),
    JolteonOut(VecDeque<Block>),
}

pub struct Abraxas {
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
    main_chain_len: usize,
    vaba_chain_len: usize,
    vaba_chain: Vec<Vec<u8>>,
    last_synchronized: usize, // Last Index of vaba chain that got added to the main chain.
    main_txs: HashSet<Digest>, // Contains transactions in the main chain that are not yet certified.
    rx_rec: Receiver<ConsensusMessage>, // Incoming recovery votes/certificates.
    rx_event: Receiver<Event>, // Events from sub protocols.
    tx_mempool_cmd: Sender<MempoolCmd>, // Channel for communication with the mempool wrapper.
    tx_stop_start: Sender<()>, // Used to stop and start jolteon.
    recovery_certificates: HashMap<SeqNumber, VecDeque<RC>>, // Maps era -> recovery certificate.
    rc_inputted: bool,         // True if a recovery certificate was sent to the mempool wrapper.
    rc_received: HashSet<SeqNumber>, // Indices of already received recovery certificates.
    tx_application_layer: Sender<Block>, // Channel to send blocks to the application layer.
    store: Store,              // Storage.
}

impl Abraxas {
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

        // MempoolWrapper which acts as a buffer, such that both sub protocols receive the
        // same transactions.
        let mempool_driver = MempoolDriver::new(tx_consensus_mempool.clone());
        let max_payload_size = parameters.clone().max_payload_size;
        let (tx_mempool_wrapper_cmd, rx_mempool_wrapper_cmd) = channel(1_000);
        let mut mempool_wrapper =
            MempoolWrapper::new(max_payload_size, mempool_driver, rx_mempool_wrapper_cmd);
        tokio::spawn(async move {
            mempool_wrapper.run().await;
        });

        // Channels for forwarding messages to the correct subprotocol.
        let (tx_jolteon, rx_jolteon) = channel(1_000);
        let (tx_vaba, rx_vaba) = channel(1_000);

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

        // Create a message handler
        let (tx_rec, rx_rec) = channel(1_000);
        let mut message_handler = MessageHandler::new(rx_main, tx_rec.clone(), tx_jolteon, tx_vaba);
        tokio::spawn(async move {
            message_handler.run().await;
        });

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
            tx_event.clone(),
            rx_stop_start,
            tx_mempool_wrapper_cmd.clone(),
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
            true, // running vaba
            tx_event.clone(),
            tx_mempool_wrapper_cmd.clone(),
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
            loopback: parameters.loopback,
            l: 0,
            k_voted: 0,
            state: State::Steady,
            aggregator: Aggregator::new(committee.clone()),
            signature_service: signature_service.clone(),
            network_filter: tx_filter.clone(),
            main_chain_len: 0,
            vaba_chain_len: 0,
            vaba_chain: Vec::new(),
            last_synchronized: 0,
            main_txs: HashSet::new(),
            rx_rec,
            rx_event,
            tx_mempool_cmd: tx_mempool_wrapper_cmd,
            tx_stop_start,
            recovery_certificates: HashMap::new(),
            rc_inputted: false,
            rc_received: HashSet::new(),
            tx_application_layer: tx_commit.clone(),
            store: store.clone(),
        }
    }

    async fn store_vaba_block(&mut self, block: &Block) {
        let key = block.digest().to_vec();
        self.vaba_chain.push(key.clone());
        self.vaba_chain_len += 1;
        // We panic if we get an error, because this means the store is broken. We don't want to
        // proceed any further with a broken store.
        let value = bincode::serialize(block).expect("Failed to serialize block");
        self.store.write(key, value).await;
    }

    async fn get_vaba_block(&mut self, index: usize) -> Block {
        let key = self.vaba_chain[index].clone();
        // We panic if we get an error, because this means the store is broken. We don't want to
        // proceed any further with a broken store.
        self.store
            .read(key)
            .await
            .unwrap()
            .and_then(|bytes| bincode::deserialize(&bytes).ok())
            .unwrap()
    }

    async fn store_block(&mut self, block: &Block) {
        let key = block.digest().to_vec();
        self.main_chain_len += 1;
        // We panic if we get an error, because this means the store is broken. We don't want to
        // proceed any further with a broken store.
        let value = bincode::serialize(block).expect("Failed to serialize block");
        self.store.write(key, value).await;
    }

    async fn handle_message(&mut self, message: ConsensusMessage) {
        match message {
            ConsensusMessage::RecoveryVote(rv) => {
                if let Err(e) = self.handle_recovery_vote(&rv).await {
                    debug!("{}", e);
                }
            }
            ConsensusMessage::RecoveryCertificate(rc) => {
                self.handle_recovery_certificate(&rc).await
            }
            _ => debug!("Wrong message type!"),
        }
    }

    async fn handle_blocks(&mut self, mut blocks: VecDeque<Block>) {
        // Received block(s) that can be appended to the main chain.
        // debug!(
        //     "Received block(s): {:?}. Len main {} Len vaba {}",
        //     blocks, self.main_chain_len, self.vaba_chain_len
        // );
        while let Some(block) = blocks.pop_back() {
            if !block.payload.is_empty() {
                // Remove transactions from the mempool wrapper
                self.tx_mempool_cmd
                    .send(MempoolCmd::Remove(block.payload.clone()))
                    .await
                    .expect("Failed to send transactions to mempool wrapper");

                info!("Committed {:?}", block);

                for x in &block.payload {
                    self.main_txs.insert(x.clone());

                    #[cfg(feature = "benchmark")]
                    // NOTE: This log entry is used to compute performance.
                    info!("Committed TX({})", base64::encode(x));
                }
            }
            self.store_block(&block).await;
            // Send all the newly committed blocks to the node's application layer.
            if let Err(e) = self.tx_application_layer.send(block).await {
                warn!("Failed to send block through the commit channel: {}", e);
            }
        }
    }

    async fn handle_event(&mut self, event: Event) {
        // Received an event notification by one of the two sub protocols.
        match self.state {
            State::Steady => {
                match event {
                    Event::VabaOut(mut blocks) => {
                        while let Some(block) = blocks.pop_back() {
                            debug!(
                                "VabaOut. Main len: {} Vaba len: {}, {:?}",
                                self.main_chain_len,
                                self.vaba_chain_len,
                                block.clone()
                            );
                            self.store_vaba_block(&block).await;
                        }
                        self.ss_try_resolve().await;
                    }
                    Event::JolteonOut(blocks) => {
                        info!(
                            "JolteonOut {}. Main len: {} Vaba len: {}",
                            blocks.len(),
                            self.main_chain_len,
                            self.vaba_chain_len
                        );
                        self.handle_blocks(blocks).await
                    }
                    _ => {
                        // Vote, Lock, Advance
                        self.ss_try_resolve().await;
                    }
                }
            }
            State::Recovery => match event {
                Event::VabaOut(mut blocks) => {
                    while let Some(block) = blocks.pop_back() {
                        debug!(
                            "VabaOut. Main len: {} Vaba len: {}, {:?}",
                            self.main_chain_len,
                            self.vaba_chain_len,
                            block.clone()
                        );
                        self.store_vaba_block(&block).await;
                    }
                    // We received a qc, so we need to call rs_try_vote
                    self.rs_try_vote().await;
                    self.rs_try_resolve().await;
                }
                Event::Vote => {
                    // We received a qc, so we need to call rs_try_vote
                    self.rs_try_vote().await;
                }
                _ => {}
            },
        }
    }

    async fn handle_recovery_certificate(&mut self, rc: &RC) {
        if self.rc_received.contains(&rc.index) {
            return;
        }
        // Multicast rc.
        // debug!("Multicasting rc {:?}", rc.clone());
        // if let Err(e) = Synchronizer::transmit(
        //     ConsensusMessage::RecoveryCertificate(rc.clone()),
        //     &self.name,
        //     None,
        //     &self.network_filter,
        //     &self.committee,
        // )
        // .await
        // {
        //     warn!("{}", e);
        // }
        self.recovery_certificates
            .entry(rc.era)
            .or_insert_with(VecDeque::new)
            .push_back(rc.clone());
        self.send_recovery_certificate().await;
    }

    async fn handle_recovery_vote(&mut self, rv: &RecoveryVote) -> ConsensusResult<()> {
        //debug!("Received recovery vote {:?}", rv);
        rv.verify()?;
        if let Some(rc) = self.aggregator.add_recovery_vote(rv.clone())? {
            self.handle_recovery_certificate(&rc).await;
            debug!(
                "Received enough recovery votes for era {}, index {}, inputting cert",
                rv.era, rv.index
            );
        }
        Ok(())
    }

    async fn send_recovery_certificate(&mut self) {
        if self.rc_inputted {
            return;
        }
        let m = self
            .recovery_certificates
            .entry(self.era)
            .or_insert_with(VecDeque::new);
        if let Some(rc) = m.pop_front() {
            debug!("Sending RC to mempool wrapper {:?}", rc);
            self.tx_mempool_cmd
                .send(MempoolCmd::AddCert(rc.clone()))
                .await
                .expect("Failed to send recovery certificate to mempool wrapper");
            self.rc_inputted = true;
            if m.is_empty() {
                m.push_back(rc);
            }
        }
    }

    #[async_recursion]
    async fn switch_to_steady(&mut self) {
        self.era += 1;
        self.state = State::Steady;
        info!("Entering steady state. Era {}", self.era);
        // Remove recovery certificates of the previous era.
        for k in 0..self.era {
            if let Some(l) = self.recovery_certificates.get_mut(&k) {
                l.clear();
            }
        }

        self.rc_inputted = false;
        // Clean up aggregator
        self.aggregator.cleanup_recovery_votes(&self.era);

        self.tx_stop_start
            .send(())
            .await
            .expect("Failed to start jolteon");
        self.ss_try_resolve().await;
    }

    #[async_recursion]
    async fn switch_to_recovery(&mut self) {
        info!("Entering recovery state");
        self.state = State::Recovery;
        self.tx_stop_start
            .send(())
            .await
            .expect("Failed to stop jolteon");
        self.rs_try_vote().await;
        self.rs_try_resolve().await;
    }

    /* Steady state functions */

    #[async_recursion]
    async fn ss_try_resolve(&mut self) {
        if self._ss_try_resolve().await {
            self.switch_to_recovery().await;
        }
    }

    #[async_recursion]
    async fn _ss_try_resolve(&mut self) -> bool {
        if self.vaba_chain_len < self.l + self.loopback {
            return false;
        }
        let block = self.get_vaba_block(self.l).await;
        if block.payload.is_empty() {
            self.l += 1;
            return self._ss_try_resolve().await;
        }
        for tx in block.payload {
            let x = tx.clone();
            if !self.certified_on_time(x.clone()) {
                self.l += 1;
                return true;
            }
        }
        self.l += 1;
        return self._ss_try_resolve().await;
    }

    fn certified_on_time(&mut self, tx: Digest) -> bool {
        if self.main_txs.contains(&tx) {
            // Transaction is certified. There is no need to store it any longer
            self.main_txs.remove(&tx);
            return true;
        }
        debug!(
            "Tx {} not yet in main chain. len main: {}. len vaba: {} l: {} loopback: {}",
            tx, self.main_chain_len, self.vaba_chain_len, self.l, self.loopback
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
        if self.vaba_chain_len <= self.l {
            return false;
        }
        // Check if the block contains a recovery certificate.
        let block = self.get_vaba_block(self.l).await;
        if let Some(rc) = block.rc {
            if let Err(e) = rc.verify(&self.committee) {
                // We got an invalid recovery certificate. Input the next recovery certificate to the mempool wrapper.
                warn!("{}", e);
                self.rc_inputted = false;
                self.send_recovery_certificate().await;
                self.l += 1;
                return self._rs_try_resolve().await;
            }
            // We got a valid recovery certificate. Set blocks, increment l and return true

            // TODO: check if any blocks from the fast chain can be submitted to the main chain
            // Look up newest block in fast chain and check if it is in the main chain. If so we can
            // skip the rest of the blocks. Else recursively append?

            debug!("Valid RC received {:?}", rc);
            if self.main_chain_len < self.vaba_chain_len {
                let mut queue = VecDeque::new();
                for i in self.last_synchronized..self.vaba_chain_len {
                    let block = self.get_vaba_block(i).await;
                    queue.push_back(block);
                }
                self.last_synchronized = self.vaba_chain_len - 1;
                debug!(
                    "Bulk transaction. Adding {} blocks. main {} vaba {}",
                    queue.len(),
                    self.main_chain_len,
                    self.vaba_chain_len
                );
                self.handle_blocks(queue).await;
            }
            self.l += 1;
            return true;
        } else {
            // There wasn't any recovery certificate in the block. We send a certificate to the mempool wrapper, so that
            // the next vaba block will include a certificate.
            debug!("NO RC in block {:?}", block);
            self.rc_inputted = false;
            self.send_recovery_certificate().await;
            self.l += 1;
            return self._rs_try_resolve().await;
        }
    }

    async fn rs_try_vote(&mut self) {
        debug!(
            "rs_try_vote: k_voted: {} len vaba: {}",
            self.k_voted, self.vaba_chain_len
        );
        let mut recovery_votes = Vec::new();
        for i in self.k_voted..self.vaba_chain_len {
            let block = self.get_vaba_block(i).await;
            self.k_voted = i - 1;
            if block.qc.view > self.era {
                break;
            }
            if block.qc.view == self.era {
                debug!(
                    "Matching qc found! era: {} index: {}",
                    block.qc.view, block.qc.round
                );
                let rv = RecoveryVote::new(
                    self.era,
                    block.round,
                    self.signature_service.clone(),
                    block.qc.clone(),
                    self.name,
                )
                .await;
                recovery_votes.push(rv);
            }
        }

        // Send recovery votes
        for rv in recovery_votes {
            if let Err(e) = Synchronizer::transmit(
                ConsensusMessage::RecoveryVote(rv.clone()),
                &self.name,
                None,
                &self.network_filter,
                &self.committee,
            )
            .await
            {
                warn!("{}", e);
            };
            if let Err(e) = self.handle_recovery_vote(&rv).await {
                debug!("{}", e);
            }
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(message) = self.rx_rec.recv() => self.handle_message(message).await,
                Some(event) = self.rx_event.recv() => self.handle_event(event).await
            }
        }
    }
}
