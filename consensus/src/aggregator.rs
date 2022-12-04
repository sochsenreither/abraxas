use crate::config::{Committee, Stake};
use crate::core::SeqNumber;
use crate::error::{ConsensusError, ConsensusResult};
use crate::messages::{RecoveryVote, Timeout, Vote, QC, RC, TC};
use crypto::Hash as _;
use crypto::{Digest, PublicKey, Signature};
use std::collections::{HashMap, HashSet};

#[cfg(test)]
#[path = "tests/aggregator_tests.rs"]
pub mod aggregator_tests;

// In HotStuff, votes/timeouts aggregated by round
// In VABA and async fallback, votes aggregated by round, timeouts/coin_share aggregated by view
pub struct Aggregator {
    committee: Committee,
    votes_aggregators: HashMap<SeqNumber, HashMap<Digest, Box<QCMaker>>>,
    timeouts_aggregators: HashMap<SeqNumber, Box<TCMaker>>,
    recovery_votes_aggregators: HashMap<SeqNumber, HashMap<SeqNumber, Box<RCMaker>>>, // era -> index -> rv
}

impl Aggregator {
    pub fn new(committee: Committee) -> Self {
        Self {
            committee,
            votes_aggregators: HashMap::new(),
            timeouts_aggregators: HashMap::new(),
            recovery_votes_aggregators: HashMap::new(),
        }
    }

    pub fn add_recovery_vote(&mut self, rv: RecoveryVote) -> ConsensusResult<Option<RC>> {
        // Add the new recovery vote to our aggregator and see if we have a Recovery certificate.
        self.recovery_votes_aggregators
            .entry(rv.era)
            .or_insert_with(HashMap::new)
            .entry(rv.index)
            .or_insert_with(|| Box::new(RCMaker::new()))
            .append(rv, &self.committee)
    }

    pub fn add_vote(&mut self, vote: Vote) -> ConsensusResult<Option<QC>> {
        // TODO [issue #7]: A bad node may make us run out of memory by sending many votes
        // with different round numbers or different digests.

        // Add the new vote to our aggregator and see if we have a QC.
        self.votes_aggregators
            .entry(vote.round)
            .or_insert_with(HashMap::new)
            .entry(vote.digest())
            .or_insert_with(|| Box::new(QCMaker::new()))
            .append(vote, &self.committee)
    }

    pub fn add_timeout(&mut self, timeout: Timeout) -> ConsensusResult<Option<TC>> {
        // TODO: A bad node may make us run out of memory by sending many timeouts
        // with different round numbers.

        // Add the new timeout to our aggregator and see if we have a TC.
        self.timeouts_aggregators
            .entry(timeout.seq)
            .or_insert_with(|| Box::new(TCMaker::new()))
            .append(timeout, &self.committee)
    }

    // used in HotStuff
    pub fn cleanup(&mut self, round: &SeqNumber) {
        self.votes_aggregators.retain(|k, _| k >= round);
        self.timeouts_aggregators.retain(|k, _| k >= round);
    }
    // used in VABA and async fallback
    pub fn cleanup_async(&mut self, view: &SeqNumber, round: &SeqNumber) {
        self.votes_aggregators.retain(|k, _| k >= round);
        self.timeouts_aggregators.retain(|k, _| k >= view);
    }
    // used in abraxas
    pub fn cleanup_recovery_votes(&mut self, era: &SeqNumber) {
        self.recovery_votes_aggregators.retain(|k, _| k >= era);
    }
}

struct RCMaker {
    weight: Stake,
    votes: Vec<RecoveryVote>,
    used: HashSet<PublicKey>,
}

impl RCMaker {
    pub fn new() -> Self {
        Self {
            weight: 0,
            votes: Vec::new(),
            used: HashSet::new(),
        }
    }

    /// Try to append a recovery vote to a (partial) quorum.
    pub fn append(
        &mut self,
        vote: RecoveryVote,
        committee: &Committee,
    ) -> ConsensusResult<Option<RC>> {
        let author = vote.author;
        // Ensure it is the first time this authority votes.
        ensure!(
            self.used.insert(author),
            ConsensusError::AuthorityReuseinRC(author)
        );
        self.votes.push(vote.clone());
        self.weight += committee.stake(&author);
        if self.weight >= committee.quorum_threshold() {
            self.weight = 0;
            return Ok(Some(RC {
                era: vote.era,
                index: vote.index,
                recovery_votes: self.votes.clone(),
            }));
        }
        Ok(None)
    }
}

struct QCMaker {
    weight: Stake,
    votes: Vec<(PublicKey, Signature)>,
    used: HashSet<PublicKey>,
}

impl QCMaker {
    pub fn new() -> Self {
        Self {
            weight: 0,
            votes: Vec::new(),
            used: HashSet::new(),
        }
    }

    /// Try to append a signature to a (partial) quorum.
    pub fn append(&mut self, vote: Vote, committee: &Committee) -> ConsensusResult<Option<QC>> {
        let author = vote.author;
        // Ensure it is the first time this authority votes.
        ensure!(
            self.used.insert(author),
            ConsensusError::AuthorityReuseinQC(author)
        );
        self.votes.push((author, vote.signature));
        self.weight += committee.stake(&author);
        if self.weight >= committee.quorum_threshold() {
            self.weight = 0; // Ensures QC is only made once.
            return Ok(Some(QC {
                hash: vote.hash.clone(),
                view: vote.view,
                round: vote.round,
                height: vote.height,
                fallback: vote.fallback,
                proposer: vote.proposer,
                acceptor: vote.proposer,
                votes: self.votes.clone(),
            }));
        }
        Ok(None)
    }
}

struct TCMaker {
    weight: Stake,
    votes: Vec<(PublicKey, Signature, SeqNumber)>,
    used: HashSet<PublicKey>,
}

impl TCMaker {
    pub fn new() -> Self {
        Self {
            weight: 0,
            votes: Vec::new(),
            used: HashSet::new(),
        }
    }

    /// Try to append a signature to a (partial) quorum.
    pub fn append(
        &mut self,
        timeout: Timeout,
        committee: &Committee,
    ) -> ConsensusResult<Option<TC>> {
        let author = timeout.author;

        if self.used.contains(&author) {
            return Ok(None);
        }
        self.used.insert(author);

        // // Ensure it is the first time this authority votes.
        // ensure!(
        //     self.used.insert(author),
        //     ConsensusError::AuthorityReuseinTC(author)
        // );

        // Add the timeout to the accumulator.
        self.votes
            .push((author, timeout.signature, timeout.high_qc.round));
        self.weight += committee.stake(&author);
        let mut threshold = committee.quorum_threshold();
        if timeout.seq == 0 {
            threshold = committee.large_threshold();
        }
        if self.weight >= threshold {
            self.weight = 0; // Ensures TC is only created once.
            return Ok(Some(TC {
                seq: timeout.seq,
                votes: self.votes.clone(),
            }));
        }
        Ok(None)
    }
}
