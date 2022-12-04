use crate::ConsensusMessage;
use log::debug;
use tokio::sync::mpsc::{Receiver, Sender};

pub struct MessageHandler {
    rx_main: Receiver<ConsensusMessage>,   // Incoming network data
    tx_recovery: Sender<ConsensusMessage>, // Channel to Abraxas
    tx_jolteon: Sender<ConsensusMessage>,  // Channel to Jolteon
    tx_vaba: Sender<ConsensusMessage>,     // Channel to Vaba
}

impl MessageHandler {
    pub fn new(
        rx_main: Receiver<ConsensusMessage>,   // Incoming network data
        tx_recovery: Sender<ConsensusMessage>, // Channel to Abraxas
        tx_jolteon: Sender<ConsensusMessage>,  // Channel to Jolteon
        tx_vaba: Sender<ConsensusMessage>,     // Channel to Vaba
    ) -> Self {
        Self {
            rx_main,
            tx_recovery,
            tx_jolteon,
            tx_vaba,
        }
    }

    pub async fn run(&mut self) {
        loop {
            if let Some(message) = self.rx_main.recv().await {
                let res = match message {
                    // Messages for jolteon
                    ConsensusMessage::ProposeJolteon(_) => self.tx_jolteon.send(message).await,
                    ConsensusMessage::VoteJolteon(_) => self.tx_jolteon.send(message).await,
                    ConsensusMessage::TimeoutJolteon(_) => self.tx_jolteon.send(message).await,
                    ConsensusMessage::TCJolteon(_) => self.tx_jolteon.send(message).await,
                    ConsensusMessage::SignedQCJolteon(_) => self.tx_jolteon.send(message).await,
                    ConsensusMessage::RandomnessShareJolteon(_) => self.tx_jolteon.send(message).await,
                    ConsensusMessage::RandomCoinJolteon(_) => self.tx_jolteon.send(message).await,
                    ConsensusMessage::SyncRequestJolteon(_, _) => self.tx_jolteon.send(message).await,
                    ConsensusMessage::SyncReplyJolteon(_) => self.tx_jolteon.send(message).await,

                    ConsensusMessage::LoopBack(block) => {
                        match block.fallback {
                            0 => self.tx_jolteon
                                .send(ConsensusMessage::LoopBack(block.clone()))
                                .await
                                .unwrap(),
                            _ => self.tx_vaba
                                .send(ConsensusMessage::LoopBack(block.clone()))
                                .await
                                .unwrap(),
                        }
                        Ok(())
                    }

                    // Messages for vaba
                    ConsensusMessage::ProposeVaba(_) => self.tx_vaba.send(message).await,
                    ConsensusMessage::VoteVaba(_) => self.tx_vaba.send(message).await,
                    ConsensusMessage::TimeoutVaba(_) => self.tx_vaba.send(message).await,
                    ConsensusMessage::TCVaba(_) => self.tx_vaba.send(message).await,
                    ConsensusMessage::SignedQCVaba(_) => self.tx_vaba.send(message).await,
                    ConsensusMessage::RandomnessShareVaba(_) => self.tx_vaba.send(message).await,
                    ConsensusMessage::RandomCoinVaba(_) => self.tx_vaba.send(message).await,
                    ConsensusMessage::SyncRequestVaba(_, _) => self.tx_vaba.send(message).await,
                    ConsensusMessage::SyncReplyVaba(_) => self.tx_vaba.send(message).await,

                    // Recovery vote and certificate
                    _ => self.tx_recovery.send(message).await,
                };
                match res {
                    Ok(_) => (),
                    Err(e) => debug!("{}", e),
                }
            }
        }
    }
}
