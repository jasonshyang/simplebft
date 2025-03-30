use sha2::{Digest as ShaDigest, Sha512};

use super::{message::{Block, Hashable}, peers::Peers, message::Stage};
use crate::common::crypto::{Digest, Signature};

/*
    A Quorum Certificate (QC) over a tuple
    ⟨type, viewNumber, node⟩ is a data type that combines a collection
    of signatures for the same tuple signed by (n − f ) replicas. Given
    a QC qc, we use qc.type, qc.viewNumber , qc.node to refer to the
    matching fields of the original tuple.

    TODO: Currently the QC validation is not yet completed and should expect to be buggy
*/

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ConsensusPayload {
    pub view_num: u64,
    pub stage: Stage,
    pub block: Block,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct QuorumCertificate {
    pub payload: ConsensusPayload,
    pub signatures: Vec<Signature>,
}

impl QuorumCertificate {
    pub fn new(view_num: u64, stage: Stage, block: Block) -> Self {
        let payload = ConsensusPayload {
            view_num,
            stage,
            block,
        };
        QuorumCertificate {
            payload,
            signatures: Vec::new(),
        }
    }

    pub fn genesis() -> Self {
        QuorumCertificate {
            payload: ConsensusPayload {
                view_num: 0,
                stage: Stage::Prepare,
                block: Block::genesis(),
            },
            signatures: Vec::new(),
        }
    }

    pub fn reset(&mut self, payload: ConsensusPayload) {
        self.payload = payload;
        self.signatures.clear();
    }

    pub fn add_signature(&mut self, sig: Signature) {
        self.signatures.push(sig);
    }

    pub fn is_genesis(&self) -> bool {
        self.payload.view_num == 0
    }

    pub fn is_complete(&self, n: usize, f: usize) -> bool {
        self.signatures.len() >= (n - f)
    }

    pub fn validate(&self, peers: &Peers, n: usize, f: usize) -> bool {
        if self.is_genesis() {
            println!("QC validation bypassed: Genesis QC");
            return true;
        }

        if self.is_complete(n, f) == false {
            println!("QC failed: QC is not complete");
            return false;
        }

        // Check the signatures are from distinct replicas from peers
        let mut valid_signatures = 0;
        for sig in &self.signatures {
            if sig.verify(&self.payload.hash()) == false  {
                println!("QC failed: Signature verification failed");
                println!("QC: {:?}", self);
                return false;
            }

            if peers.members.contains(&sig.signer) {
                valid_signatures += 1;
            }
        }

        if valid_signatures < (n - f) {
            println!("QC failed: Not enough valid signatures");
            return false;
        }

        println!("QC validation passed");
        
        true
    }
}

impl Hashable for ConsensusPayload {
    fn hash(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.view_num.to_be_bytes());
        hasher.update(self.stage.as_ref());
        hasher.update(&self.block.hash());
        let result = hasher.finalize();
        let mut digest = [0u8; 64];
        digest.copy_from_slice(&result[..]);
        digest
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::crypto::Keypair;

    #[test]
    fn test_validate_qc() {
        let keypair1 = Keypair::new_pair();
        let keypair2 = Keypair::new_pair();
        let keypair3 = Keypair::new_pair();
        let peers = Peers::new(vec![keypair1.pubkey(), keypair2.pubkey(), keypair3.pubkey()]);
        let n = 3;
        let f = 1;

        let block = Block::new(1, [0u8; 64], [0u8; 1024]);
        let payload = ConsensusPayload {
            view_num: 1,
            stage: Stage::Prepare,
            block,
        };
        let sig1 = keypair1.sign(&payload.hash());
        let sig2 = keypair2.sign(&payload.hash());
        let sig3 = keypair3.sign(&payload.hash());
        let qc = QuorumCertificate {
            payload,
            signatures: vec![sig1, sig2, sig3],
        };

        assert_eq!(qc.validate(&peers, n, f), true);
    }
}