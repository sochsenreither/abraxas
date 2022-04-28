use crate::mempool::MempoolDriver;
use crate::optimistic_compiler::SubProto;
use crypto::Digest;
use log::{debug, info};
use std::collections::HashSet;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;

pub struct MempoolWrapper {
    buffer_jolteon: HashSet<Digest>,
    buffer_vaba: HashSet<Digest>,
    buffer_rc: HashSet<Digest>,
    max: usize,
    digest_len: usize,
    payload_amount: usize,
    mempool_driver: MempoolDriver,
    rx_request: Receiver<(oneshot::Sender<Vec<Digest>>, SubProto)>,
    rx_cert: Receiver<Digest>,
}

impl MempoolWrapper {
    pub fn new(
        max: usize,
        mempool_driver: MempoolDriver,
        rx_request: Receiver<(oneshot::Sender<Vec<Digest>>, SubProto)>,
        rx_cert: Receiver<Digest>,
    ) -> Self {
        Self {
            buffer_jolteon: HashSet::new(),
            buffer_vaba: HashSet::new(),
            buffer_rc: HashSet::new(),
            max,
            digest_len: Digest::default().size(),
            payload_amount: max / Digest::default().size(),
            mempool_driver,
            rx_request,
            rx_cert,
        }
    }

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

    async fn handle_request(&mut self, answer: oneshot::Sender<Vec<Digest>>, proto: SubProto) {
        let digests = match proto {
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
                digests
            }
            SubProto::Vaba => {
                // If there are recovery certificates in the buffer use them as input
                if self.buffer_vaba.len() + self.buffer_rc.len() < self.payload_amount {
                    let payload_to_get = self.max
                        - (self.buffer_vaba.len() * self.digest_len)
                        - (self.buffer_rc.len() * self.digest_len);
                    let data = self.mempool_driver.get(payload_to_get).await;
                    self.log_data(&data, proto);
                }
                let mut certs: Vec<Digest> = self
                    .buffer_rc
                    .iter()
                    .take(self.payload_amount)
                    .cloned()
                    .collect();
                let mut digests: Vec<Digest> = self
                    .buffer_vaba
                    .iter()
                    .take(self.payload_amount - certs.len())
                    .cloned()
                    .collect();
                for x in &digests {
                    self.buffer_vaba.remove(x);
                }
                for x in &certs {
                    self.buffer_rc.remove(x);
                }
                certs.append(&mut digests);
                certs
            }
        };
        answer.send(digests).expect("Failed to send");
    }

    pub async fn run(&mut self) {
        // let blub: [u8; 32] = [
        //     2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1,
        //     1, 1, 1,
        // ];
        // let test = Digest(blub.as_slice()[..32].try_into().unwrap());
        loop {
            tokio::select! {
                Some(rc) = self.rx_cert.recv() => {
                    self.buffer_rc.insert(rc.clone());
                    info!("Incoming TX({})", base64::encode(rc));
                },
                Some((answer, proto)) = self.rx_request.recv() => self.handle_request(answer, proto).await
            }
        }
    }
}
