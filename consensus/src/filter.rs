use crate::config::Parameters;
use crate::core::ConsensusMessage;
use bytes::Bytes;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::debug;
use network::NetMessage;
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration};
use rand::Rng;

pub type FilterInput = (ConsensusMessage, Vec<SocketAddr>);

pub struct Filter;

impl Filter {
    pub fn run(
        mut core: Receiver<FilterInput>,
        network: Sender<NetMessage>,
        parameters: Parameters,
    ) {
        tokio::spawn(async move {
            let mut pending = FuturesUnordered::new();
            loop {
                tokio::select! {
                    Some(input) = core.recv() => pending.push(Self::delay(input, parameters.clone())),
                    Some(input) = pending.next() => Self::transmit(input, &network).await,
                    else => break
                }
            }
        });
    }

    async fn transmit(input: FilterInput, network: &Sender<NetMessage>) {
        let (message, addresses) = input;
        let bytes = bincode::serialize(&message).expect("Failed to serialize core message");
        let net_message = NetMessage(Bytes::from(bytes), addresses);
        if let Err(e) = network.send(net_message).await {
            panic!("Failed to send block through network channel: {}", e);
        }
    }

    async fn delay(input: FilterInput, parameters: Parameters) -> FilterInput {
        let (message, _) = &input;
        // Only add network delay for non-fallback block proposals
        if let ConsensusMessage::ProposeJolteon(block) = message {
            if parameters.random_ddos && block.fallback == 0 {
                // Add delay with chance 1/10
                if rand::thread_rng().gen_bool(1.0/10.0) {
                    sleep(Duration::from_millis(parameters.network_delay)).await;
                }
                return input;
            } else if parameters.ddos && block.fallback == 0 {
                sleep(Duration::from_millis(parameters.network_delay)).await;
                return input;
            }
        }
        input
    }
}
