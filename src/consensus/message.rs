use sha2::{Digest as ShaDigest, Sha512};

use super::qc::QuorumCertificate;
use crate::common::crypto::{Digest, Signature};

/*
    A message m ...
    is automatically stamped with curView, the sender’s current view
    number. Each message has a type m.type ∈ {new-view, prepare,
    pre-commit, commit, decide}. m.node contains a proposed node
    (the leaf node of a proposed branch). There is an optional field
    m.justify. The leader always uses this field to carry the QC for the
    different phases. Replicas use it in new-view messages to carry the
    highest prepareQC . Each message sent in a replica role contains a
    partial signaturem.partialSig by the sender over the tuple ⟨m.type,
    m.viewNumber ,m.node⟩, which is added in the voteMsg() utility.
*/

pub const MAX_BLOCK_SIZE: usize = 1024;

pub type BlockData = [u8; MAX_BLOCK_SIZE];

pub trait Hashable {
    fn hash(&self) -> Digest;
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Stage {
    Prepare,
    PreCommit,
    Commit,
    Decide,
}

#[derive(Debug, Clone)]
pub enum Message {
    NewView(NewView),
    Proposal(Proposal),
    Vote(Vote),
}

#[derive(Clone, Debug)]
pub struct Proposal {
    pub view_num: u64,
    pub stage: Stage,
    pub block: Block,
    pub qc: QuorumCertificate,
    pub sig: Signature,
}

#[derive(Clone, Debug)]
pub struct Vote {
    pub view_num: u64,
    pub stage: Stage,
    pub hash: Digest,
    pub sig: Signature,
}

#[derive(Clone, Debug)]
pub struct NewView {
    pub view_num: u64,
    pub qc: QuorumCertificate,
    pub sig: Signature,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Block {
    pub view_num: u64,
    pub parent: Digest,
    pub data: BlockData,
}

impl AsRef<[u8]> for Stage {
    fn as_ref(&self) -> &[u8] {
        match self {
            Stage::Prepare => &[1u8],
            Stage::PreCommit => &[2u8],
            Stage::Commit => &[3u8],
            Stage::Decide => &[4u8],
        }
    }
}

impl Stage {
    pub fn next(&self) -> Self {
        match self {
            Stage::Prepare => Stage::PreCommit,
            Stage::PreCommit => Stage::Commit,
            Stage::Commit => Stage::Decide,
            Stage::Decide => Stage::Prepare,
        }
    }
}

impl Block {
    pub fn new(view_num: u64, parent: Digest, data: BlockData) -> Self {
        Block {
            view_num,
            parent,
            data,
        }
    }

    pub fn genesis() -> Self {
        Block {
            view_num: 0,
            parent: [0u8; 64],
            data: [0u8; MAX_BLOCK_SIZE],
        }
    }
}

impl Hashable for Block {
    fn hash(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.view_num.to_be_bytes());
        hasher.update(&self.parent);
        hasher.update(self.data.as_ref());
        let result = hasher.finalize();
        let mut digest = [0u8; 64];
        digest.copy_from_slice(&result[..]);
        digest
    }
}
