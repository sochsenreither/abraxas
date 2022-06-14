use super::*;
use crate::{
    common::{chain, committee, keys, MockMempool},
    MempoolWrapper,
};
use crypto::SecretKey;
use std::fs;
use tokio::sync::mpsc::channel;

async fn core(
    name: PublicKey,
    secret: SecretKey,
    store_path: &str,
) -> (Sender<ConsensusMessage>, Receiver<FilterInput>) {
    let (tx_core, rx_core) = channel(1);
    let (tx_network, rx_network) = channel(3);
    let (tx_consensus_mempool, rx_consensus_mempool) = channel(1);

    let parameters = Parameters {
        timeout_delay: 100,
        ..Parameters::default()
    };
    let signature_service = SignatureService::new(secret, None);
    let _ = fs::remove_dir_all(store_path);
    let store = Store::new(store_path).unwrap();
    let leader_elector = LeaderElector::new(committee());
    MockMempool::run(rx_consensus_mempool);
    let mempool_driver = MempoolDriver::new(tx_consensus_mempool.clone());
    let synchronizer = Synchronizer::new(
        name,
        committee(),
        store.clone(),
        /* network_channel */ tx_network.clone(),
        /* core_channel */ tx_core.clone(),
        parameters.sync_retry_delay,
        SubProto::Jolteon,
    )
    .await;
    let (tx_event, mut rx_event) = channel(1);
    let (_, rx_stop_start) = channel(1);
    let (tx_mempool_wrapper_cmd, rx_mempool_wrapper_cmd) = channel(1);
    let mut mempool_wrapper = MempoolWrapper::new(100, mempool_driver, rx_mempool_wrapper_cmd);
    let mut core = Core::new(
        name,
        committee(),
        parameters,
        signature_service,
        store,
        leader_elector,
        MempoolDriver::new(tx_consensus_mempool),
        synchronizer,
        /* core_channel */ rx_core,
        /* network_channel */ tx_network,
        tx_event,
        rx_stop_start,
        tx_mempool_wrapper_cmd,
    );
    tokio::spawn(async move {
        core.run().await;
    });
    tokio::spawn(async move {
        mempool_wrapper.run().await;
    });
    tokio::spawn(async move { while let Some(_e) = rx_event.recv().await {} });
    (tx_core, rx_network)
}

fn leader_keys(round: SeqNumber) -> (PublicKey, SecretKey) {
    let leader_elector = LeaderElector::new(committee());
    let leader = leader_elector.get_leader(round);
    keys()
        .into_iter()
        .find(|(public_key, _)| *public_key == leader)
        .unwrap()
}

#[tokio::test]
async fn handle_proposal() {
    // Make a block and the vote we expect to receive.
    let block = chain(vec![leader_keys(1)]).pop().unwrap();
    let (public_key, secret_key) = keys().pop().unwrap();
    let vote = Vote::new_from_key(
        block.digest(),
        block.view,
        block.round,
        block.height,
        block.fallback,
        block.author,
        public_key,
        &secret_key,
    );

    // Run a core instance.
    let store_path = ".db_test_handle_proposal";
    let (tx_core, mut rx_network) = core(public_key, secret_key, store_path).await;

    // Send a block to the core.
    let message = ConsensusMessage::ProposeJolteon(block.clone());
    tx_core.send(message).await.unwrap();

    // Ensure we get a vote back.
    match rx_network.recv().await {
        Some((message, recipient)) => {
            match message {
                ConsensusMessage::VoteJolteon(v) => assert_eq!(v, vote),
                _ => assert!(false),
            }
            let (next_leader, _) = leader_keys(2);
            let address = committee().address(&next_leader).unwrap();
            assert_eq!(recipient, vec![address]);
        }
        _ => assert!(false),
    }
}

#[tokio::test]
async fn generate_proposal() {
    // Get the keys of the leaders of this round and the next.
    let (leader, leader_key) = leader_keys(1);
    let (next_leader, next_leader_key) = leader_keys(2);

    // Make a block, votes, and QC.
    let block = Block::new_from_key(QC::genesis(), leader, 1, 1, 0, 0, Vec::new(), &leader_key);
    let hash = block.digest();
    let votes: Vec<_> = keys()
        .iter()
        .map(|(public_key, secret_key)| {
            Vote::new_from_key(
                hash.clone(),
                block.view,
                block.round,
                block.height,
                block.fallback,
                block.author,
                *public_key,
                &secret_key,
            )
        })
        .collect();
    let qc = QC {
        hash,
        view: block.view,
        round: block.round,
        height: block.height,
        fallback: block.fallback,
        proposer: block.author,
        acceptor: block.author,
        votes: votes
            .iter()
            .cloned()
            .map(|x| (x.author, x.signature))
            .collect(),
    };

    // Run a core instance.
    let store_path = ".db_test_generate_proposal";
    let (tx_core, mut rx_network) = core(next_leader, next_leader_key, store_path).await;

    // Send all votes to the core.
    for vote in votes.clone() {
        let message = ConsensusMessage::VoteJolteon(vote);
        tx_core.send(message).await.unwrap();
    }

    // Ensure the core sends a new block.
    match rx_network.recv().await {
        Some((message, mut recipients)) => {
            match message {
                ConsensusMessage::ProposeJolteon(b) => {
                    assert_eq!(b.round, 2);
                    assert_eq!(b.qc, qc);
                }
                _ => assert!(false),
            }
            let mut addresses = committee().broadcast_addresses(&next_leader);
            addresses.sort();
            recipients.sort();
            assert_eq!(recipients, addresses);
        }
        _ => assert!(false),
    }
}

#[tokio::test]
async fn commit_block() {
    // Get enough distinct leaders to form a quorum.
    let leaders = vec![leader_keys(1), leader_keys(2), leader_keys(3)];
    let chain = chain(leaders);

    // Run a core instance.
    let store_path = ".db_test_commit_block";
    let (public_key, secret_key) = keys().pop().unwrap();
    let (tx_core, _rx_network) = core(public_key, secret_key, store_path).await;

    // Send a the blocks to the core.
    let _committed = chain[0].clone();
    for block in chain {
        let message = ConsensusMessage::ProposeJolteon(block);
        tx_core.send(message).await.unwrap();
    }
}

#[tokio::test]
async fn local_timeout_round() {
    // Make the timeout vote we expect.
    let (public_key, secret_key) = leader_keys(3);
    let timeout = Timeout::new_from_key(QC::genesis(), 1, public_key, &secret_key);

    // Run a core instance.
    let store_path = ".db_test_local_timeout_round";
    let (_tx_core, mut rx_network) = core(public_key, secret_key, store_path).await;

    // Ensure the following operation happen in the right order.
    match rx_network.recv().await {
        Some((message, mut recipients)) => {
            match message {
                ConsensusMessage::TimeoutJolteon(t) => assert_eq!(t, timeout),
                _ => assert!(false),
            }
            let mut addresses = committee().broadcast_addresses(&public_key);
            addresses.sort();
            recipients.sort();
            assert_eq!(recipients, addresses);
        }
        _ => assert!(false),
    }
}
