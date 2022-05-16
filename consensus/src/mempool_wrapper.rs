use crate::mempool::MempoolDriver;
use crate::messages::RC;
use crate::optimistic_compiler::SubProto;
use crypto::Digest;
use log::{debug, info};
use std::collections::HashSet;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;

pub struct MempoolWrapper {
    buffer_jolteon: HashSet<Digest>,
    buffer_vaba: HashSet<Digest>,
    max: usize,
    digest_len: usize,
    payload_amount: usize,
    mempool_driver: MempoolDriver,
    rc: Option<RC>, // Currently hold recovery certificate. Note: there is only one to prevent the mempool wrapper from spamming vaba with recovery certificates
    rx_command: Receiver<MempoolCmd>,
}

#[derive(Debug)]
pub enum MempoolCmd {
    Request((oneshot::Sender<(Vec<Digest>, Option<RC>)>, SubProto)),
    Remove(Vec<Digest>),
    AddCert(RC),
}

impl MempoolWrapper {
    pub fn new(
        max: usize,
        mempool_driver: MempoolDriver,
        rx_command: Receiver<MempoolCmd>,
    ) -> Self {
        Self {
            buffer_jolteon: HashSet::new(),
            buffer_vaba: HashSet::new(),
            max,
            digest_len: Digest::default().size(),
            payload_amount: max / Digest::default().size(),
            mempool_driver,
            rc: None,
            rx_command,
        }
    }

    // Logs incoming transactions
    fn log_data(&mut self, data: &Vec<Digest>, proto: SubProto) {
        for d in data {
            // NOTE: This log entry is used to compute performance.
            info!("Incoming TX({})", base64::encode(d.clone()));
            self.buffer_jolteon.insert(d.clone());
            self.buffer_vaba.insert(d.clone());
        }
        debug!(
            "Got request from {:?}. Len jolteon buf: {} Len vaba buf: {}",
            proto,
            self.buffer_jolteon.len(),
            self.buffer_vaba.len(),
        );
    }

    // Handles transaction requests from the sub protocols. If there aren't any transactions already in the buffers
    // the mempool will be asked for transactions.
    async fn handle_request(
        &mut self,
        answer: oneshot::Sender<(Vec<Digest>, Option<RC>)>,
        proto: SubProto,
    ) {
        let (digests, rc) = match proto {
            SubProto::Jolteon => {
                if self.buffer_jolteon.len() < self.payload_amount {
                    let payload_to_get = self.max - (self.buffer_jolteon.len() * self.digest_len);
                    let data = self.mempool_driver.get(payload_to_get).await;
                    self.log_data(&data, proto);
                }
                let digests = self
                    .buffer_jolteon
                    .iter()
                    .take(self.payload_amount)
                    .cloned()
                    .collect();
                for x in &digests {
                    self.buffer_jolteon.remove(x);
                }
                (digests, None)
            }
            SubProto::Vaba => {
                if self.buffer_vaba.len() < self.payload_amount {
                    let payload_to_get = self.max - (self.buffer_vaba.len() * self.digest_len);
                    let data = self.mempool_driver.get(payload_to_get).await;
                    self.log_data(&data, proto);
                }
                let digests = self
                    .buffer_vaba
                    .iter()
                    .take(self.payload_amount)
                    .cloned()
                    .collect();
                for x in &digests {
                    self.buffer_vaba.remove(x);
                }
                (digests, self.rc.clone())
            }
        };
        if let Some(_) = rc {
            self.rc = None;
        }
        answer.send((digests, rc)).expect("Failed to send");
    }

    fn remove_tx(&mut self, txs: Vec<Digest>) {
        debug!("Removing txs {:?}", &txs);
        for tx in txs {
            self.buffer_jolteon.remove(&tx);
            self.buffer_vaba.remove(&tx);
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(cmd) = self.rx_command.recv() => {
                    match cmd {
                        MempoolCmd::Request((answer, proto)) => self.handle_request(answer, proto).await,
                        MempoolCmd::Remove(txs) => self.remove_tx(txs),
                        MempoolCmd::AddCert(rc) => self.rc = Some(rc),
                    }
                }
            }
        }
    }
}
