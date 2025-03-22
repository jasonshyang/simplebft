use sha2::{Digest as ShaDigest, Sha512};

use super::{message::{Block, Hashable, MessageType}, peers::Peers};
use crate::common::crypto::{Digest, Signature};

/*
    A Quorum Certificate (QC) over a tuple
    ⟨type, viewNumber, node⟩ is a data type that combines a collection
    of signatures for the same tuple signed by (n − f ) replicas. Given
    a QC qc, we use qc.type, qc.viewNumber , qc.node to refer to the
    matching fields of the original tuple.
*/

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ConsensusPayload {
    pub view_num: u64,
    pub message_type: MessageType,
    pub block: Block,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct QuorumCertificate {
    pub payload: ConsensusPayload,
    pub signatures: Vec<Signature>,
}

impl QuorumCertificate {
    pub fn new(view_num: u64, message_type: MessageType, block: Block) -> Self {
        let payload = ConsensusPayload {
            view_num,
            message_type,
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
                message_type: MessageType::NewView,
                block: Block::genesis(),
            },
            signatures: Vec::new(),
        }
    }

    pub fn add_signature(&mut self, sig: Signature) {
        self.signatures.push(sig);
    }

    pub fn validate(&self, peers: &Peers, n: usize, f: usize) -> bool {
        if self.signatures.len() < (n - f) {
            return false;
        }

        // Check the signatures are from distinct replicas from peers
        let mut valid_signatures = 0;
        for sig in &self.signatures {
            if sig.verify(&self.payload.hash()) == false  {
                return false;
            }

            if peers.members.contains(&sig.signer) {
                valid_signatures += 1;
            }
        }

        if valid_signatures < (n - f) {
            return false;
        }
        
        true
    }
}

impl Hashable for ConsensusPayload {
    fn hash(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.view_num.to_be_bytes());
        hasher.update(self.message_type.as_ref());
        hasher.update(&self.block.hash());
        let result = hasher.finalize();
        let mut digest = [0u8; 64];
        digest.copy_from_slice(&result[..]);
        digest
    }
}
