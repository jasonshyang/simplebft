use tokio::sync::mpsc::{Receiver, Sender};

use crate::common::crypto::Keypair;
use super::{message::{Hashable, Message, Proposal, Vote}, peers::Peers, qc::QuorumCertificate};

pub struct ConsensusProcessor {
    pub keypair: Keypair,
    pub stage: Stage,
    pub role: ConsensusRole,
    pub state: ConsensusState,
    pub msg_rx: Receiver<Message>,
    pub msg_tx: Sender<Message>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Stage {
    NewView,
    Prepare,
    PreCommit,
    Commit,
    Decide,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ConsensusRole {
    Leader,
    Replica,
}

pub struct ConsensusState {
    pub current_view: u64,
    pub high_qc: QuorumCertificate,
    pub next_qc: QuorumCertificate,
    pub peers: Peers,
}

impl ConsensusProcessor {
    pub async fn run(&mut self) {
        println!("Running Consensus Processor");
        loop {
            let message = self.msg_rx.recv().await.unwrap();
            match message {
                Message::Proposal(proposal) => {
                    if self.role == ConsensusRole::Replica {
                        self.handle_proposal(proposal).await;
                    } else {
                        println!("Invalid role, expected Replica");
                        // Do nothing
                    }
                }
                Message::Vote(vote) => {
                    if self.role == ConsensusRole::Leader {
                        self.handle_vote(vote).await;
                    } else {
                        println!("Invalid role, expected Leader");
                        // Do nothing
                    }
                }
            }
        }
    }

    async fn handle_proposal(&mut self, proposal: Proposal) {
        println!("Handling Proposal");
        // Check if proposal is from leader
        if proposal.sig.signer != self.state.peers.get_leader(self.state.current_view) {
            println!("Proposal not from leader");
            return;
        }

        // Verify QC
        if proposal.qc.validate(&self.state.peers, self.state.peers.members.len(), self.state.peers.members.len() / 3) == false {
            println!("QC validation failed");
            return;
        }

        // Liveness rule - The liveness rule is the replica will accept m if m.justify has a higher view than the current lockedQC
        if proposal.qc.payload.view_num > self.state.high_qc.payload.view_num {
            println!("Liveness rule failed");
            return;
        }

        // Safety rule - The safety rule to accept a proposal is the branch of m.node extends from the currently locked node lockedQC .node
        if proposal.qc.payload.block.parent == self.state.high_qc.payload.block.hash() {
            println!("Safety rule failed");
            return;
        }

        // Build vote
        let block_hash = proposal.block.hash();
        let sig = self.keypair.sign(&block_hash);
        let vote = Vote {
            view_num: self.state.current_view,
            stage: proposal.stage,
            block_hash,
            sig,
        };
        println!("Built Vote");

        // Send vote
        self.msg_tx.send(Message::Vote(vote)).await.unwrap();
        println!("Sent Vote");

    }

    async fn handle_vote(&mut self, vote: Vote) {
        println!("Handling Vote");
        // Check if vote is signed by peers
        if self.state.peers.is_member(&vote.sig.signer) == false {
            println!("Vote not from peers");
            return;
        }

        // Check if vote is valid
        if vote.sig.verify(&vote.block_hash) == false {
            println!("Vote signature verification failed");
            return;
        }

        // Check if vote is for current view
        if vote.view_num != self.state.current_view {
            println!("Vote not for current view");
            return;
        }

        // Check if vote is for current stage
        if vote.stage != self.stage {
            println!("Vote not for current stage");
            return;
        }

        // Add vote to QC
        self.state.next_qc.add_signature(vote.sig);
        println!("Added vote to QC");

        // Check if next QC is complete
        if self.state.next_qc.is_complete(self.state.peers.members.len(), self.state.peers.members.len() / 3) {
            println!("Next QC is complete");
            // Update high QC
            self.state.high_qc = self.state.next_qc.clone();

            // Advance stage
            self.stage = self.stage.next();

            // Broadcast QC
            self.msg_tx.send(Message::Proposal(Proposal {
                view_num: self.state.current_view,
                stage: self.stage.clone(),
                block: self.state.high_qc.payload.block.clone(),
                qc: self.state.high_qc.clone(),
                sig: self.keypair.sign(&self.state.high_qc.payload.hash()),
            })).await.unwrap();
            println!("Broadcasted QC");
        }
    }

}

impl AsRef<[u8]> for Stage {
    fn as_ref(&self) -> &[u8] {
        match self {
            Stage::NewView => &[0u8],
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
            Stage::NewView => Stage::Prepare,
            Stage::Prepare => Stage::PreCommit,
            Stage::PreCommit => Stage::Commit,
            Stage::Commit => Stage::Decide,
            Stage::Decide => Stage::NewView,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{common::crypto::{Pubkey, Secretkey}, consensus::message::Block};

    #[tokio::test]
    async fn test_handle_proposal() {
        println!("Running test_handle_proposal");
        let proposer_key = Pubkey { key: [2u8; 32] };
        let proposer_secret = Secretkey { key: [2u8; 64] };
        let proposer_keypair = Keypair { pubkey: proposer_key.clone(), secret: proposer_secret };

        let replica_key = Pubkey { key: [1u8; 32] };
        let replica_secret = Secretkey { key: [2u8; 64] };
        let replica_keypair = Keypair { pubkey: replica_key.clone(), secret: replica_secret };

        let peers = Peers::new(vec![proposer_key, replica_key]);

        let proposal = Proposal {
            view_num: 0,
            stage: Stage::NewView,
            block: Block::genesis(),
            qc: QuorumCertificate::genesis(),
            sig: proposer_keypair.sign(&[0u8; 64]),
        };

        let message = Message::Proposal(proposal);

        let (incoming_tx, incoming_rx) = tokio::sync::mpsc::channel(100);
        let (outgoing_tx, mut outgoing_rx) = tokio::sync::mpsc::channel(100);

        let mut processor = ConsensusProcessor {
            keypair: replica_keypair,
            stage: Stage::NewView,
            role: ConsensusRole::Replica,
            state: ConsensusState {
                current_view: 0,
                high_qc: QuorumCertificate::genesis(),
                next_qc: QuorumCertificate::genesis(),
                peers,
            },
            msg_rx: incoming_rx,
            msg_tx: outgoing_tx,
        };

        incoming_tx.send(message).await.unwrap();

        tokio::spawn(async move {
            processor.run().await;
        });

        let message = outgoing_rx.recv().await.unwrap();
        
        println!("{:?}", message);
    }

    #[tokio::test]
    async fn test_handle_vote() {
        let proposer_key = Pubkey { key: [1u8; 32] };
        let proposer_secret = Secretkey { key: [2u8; 64] };
        let proposer_keypair = Keypair { pubkey: proposer_key.clone(), secret: proposer_secret };

        let replica1_key = Pubkey { key: [2u8; 32] };
        let replica1_secret = Secretkey { key: [2u8; 64] };
        let replica1_keypair = Keypair { pubkey: replica1_key.clone(), secret: replica1_secret };

        let replica2_key = Pubkey { key: [3u8; 32] };
        let replica2_secret = Secretkey { key: [2u8; 64] };
        let replica2_keypair = Keypair { pubkey: replica2_key.clone(), secret: replica2_secret };

        let replica3_key = Pubkey { key: [4u8; 32] };
        let replica3_secret = Secretkey { key: [2u8; 64] };
        let replica3_keypair = Keypair { pubkey: replica3_key.clone(), secret: replica3_secret };
        
        let peers = Peers::new(vec![proposer_key, replica1_key, replica2_key, replica3_key]);

        let vote1 = Vote {
            view_num: 0,
            stage: Stage::NewView,
            block_hash: [0u8; 64],
            sig: replica1_keypair.sign(&[0u8; 64]),
        };

        let vote2 = Vote {
            view_num: 0,
            stage: Stage::NewView,
            block_hash: [0u8; 64],
            sig: replica2_keypair.sign(&[0u8; 64]),
        };

        let vote3 = Vote {
            view_num: 0,
            stage: Stage::NewView,
            block_hash: [0u8; 64],
            sig: replica3_keypair.sign(&[0u8; 64]),
        };

        let message1 = Message::Vote(vote1);
        let message2 = Message::Vote(vote2);
        let message3 = Message::Vote(vote3);

        let (incoming_tx, incoming_rx) = tokio::sync::mpsc::channel(100);
        let (outgoing_tx, mut outgoing_rx) = tokio::sync::mpsc::channel(100);

        let mut processor = ConsensusProcessor {
            keypair: proposer_keypair,
            stage: Stage::NewView,
            role: ConsensusRole::Leader,
            state: ConsensusState {
                current_view: 0,
                high_qc: QuorumCertificate::genesis(),
                next_qc: QuorumCertificate::genesis(),
                peers,
            },
            msg_rx: incoming_rx,
            msg_tx: outgoing_tx,
        };

        incoming_tx.send(message1).await.unwrap();
        incoming_tx.send(message2).await.unwrap();
        incoming_tx.send(message3).await.unwrap();

        tokio::spawn(async move {
            processor.run().await;
        });

        let message = outgoing_rx.recv().await.unwrap();

        println!("{:?}", message);
    }
}