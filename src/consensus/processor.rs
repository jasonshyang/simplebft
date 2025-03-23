use tokio::sync::mpsc::{Receiver, Sender};

use crate::common::crypto::Keypair;
use super::{message::{Hashable, Message, NewView, Proposal, Vote}, peers::Peers, qc::QuorumCertificate};

/*
    TODO: Add node parent check
    TODO: Add timeout
    TODO: Handle interruption
*/

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
    pub vote_count: u64,
    pub peers: Peers,
}

impl ConsensusProcessor {
    pub async fn run(&mut self) {
        println!("Running Consensus Processor");
        loop {
            let message = self.msg_rx.recv().await.unwrap();
            match message {
                Message::NewView(new_view) => {
                    if self.role == ConsensusRole::Leader {
                        self.handle_new_view(new_view).await;
                    } else {
                        println!("Invalid role, expected Leader");
                        // Do nothing
                    }
                }
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

    async fn handle_new_view(&mut self, new_view: NewView) {
        println!("Handling New View");
        // Check if new_view is signed by peers
        if self.state.peers.is_member(&new_view.sig.signer) == false {
            println!("Vote not from peers");
            return;
        }

        // Check if new_view qc is valid
        if new_view.qc.validate(&self.state.peers, self.state.peers.members.len(), self.state.peers.members.len() / 3) == false {
            println!("New View QC validation failed");
            return;
        }

        // Update high QC and view number if new_view qc has higher view number
        if new_view.qc.payload.view_num > self.state.high_qc.payload.view_num {
            self.state.high_qc = new_view.qc.clone();
            self.state.current_view = new_view.qc.payload.view_num;
        }

        // Update vote count
        self.increment_vote_count();

        // Check if vote count is quorum
        if self.is_vote_count_quorum() {
            println!("Vote count quorum reached");
            // Start Prepare stage
            self.stage = Stage::Prepare;

            // Send Prepare Proposal
            self.msg_tx.send(Message::Proposal(Proposal {
                view_num: self.state.current_view,
                stage: Stage::Prepare,
                block: self.state.high_qc.payload.block.clone(),
                qc: self.state.high_qc.clone(),
                sig: self.keypair.sign(&self.state.high_qc.payload.hash()),
            })).await.unwrap();
            println!("Broadcasted new Proposal");

            // Reset vote count
            self.reset_vote_count();
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
        self.increment_vote_count();
        println!("Added vote to QC");

        // Check if next QC is complete
        if self.is_vote_count_quorum() {
            println!("Vote count quorum reached");
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

            // Reset vote count
            self.reset_vote_count();
        }
    }

    fn increment_vote_count(&mut self) {
        self.state.vote_count += 1;
    }

    fn reset_vote_count(&mut self) {
        self.state.vote_count = 0;
    }

    fn is_vote_count_quorum(&self) -> bool {
        self.state.vote_count >= (self.state.peers.members.len() - self.state.peers.members.len() / 3).try_into().unwrap()
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{common::crypto::{Pubkey, Secretkey}, consensus::{message::Block, qc::ConsensusPayload}};

    #[tokio::test]
    async fn test_handle_new_view() {
        println!("Running test_handle_new_view");
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

        let last_block = Block {
            view_num: 0,
            parent: [0u8; 64],
            data: [1u8; 1024],
        };

        let last_qc = QuorumCertificate {
            payload: ConsensusPayload {
                view_num: 0,
                stage: Stage::Commit,
                block: last_block.clone(),
            },
            signatures: vec![
                proposer_keypair.sign(&last_block.hash()),
                replica1_keypair.sign(&last_block.hash()),
                replica2_keypair.sign(&last_block.hash()),
                replica3_keypair.sign(&last_block.hash()),
            ],
        };

        let new_view1 = NewView {
            view_num: 0,
            qc: last_qc.clone(),
            sig: replica1_keypair.sign(&last_block.hash()),
        };

        let new_view2 = NewView {
            view_num: 0,
            qc: last_qc.clone(),
            sig: replica2_keypair.sign(&last_block.hash()),
        };

        let new_view3 = NewView {
            view_num: 0,
            qc: last_qc.clone(),
            sig: replica3_keypair.sign(&last_block.hash()),
        };

        let message1 = Message::NewView(new_view1);
        let message2 = Message::NewView(new_view2);
        let message3 = Message::NewView(new_view3);

        let (incoming_tx, incoming_rx) = tokio::sync::mpsc::channel(100);
        let (outgoing_tx, mut outgoing_rx) = tokio::sync::mpsc::channel(100);

        let mut processor = ConsensusProcessor {
            keypair: proposer_keypair,
            stage: Stage::Prepare,
            role: ConsensusRole::Leader,
            state: ConsensusState {
                current_view: 1,
                high_qc: QuorumCertificate::genesis(),
                next_qc: QuorumCertificate::genesis(),
                vote_count: 0,
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

        let last_block = Block {
            view_num: 0,
            parent: [0u8; 64],
            data: [1u8; 1024],
        };

        let block = Block {
            view_num: 0,
            parent: last_block.hash(),
            data: [0u8; 1024],
        };

        let proposal = Proposal {
            view_num: 0,
            stage: Stage::Prepare,
            block: block.clone(),
            qc: QuorumCertificate::genesis(),
            sig: proposer_keypair.sign(&block.hash()),
        };

        let message = Message::Proposal(proposal);

        let (incoming_tx, incoming_rx) = tokio::sync::mpsc::channel(100);
        let (outgoing_tx, mut outgoing_rx) = tokio::sync::mpsc::channel(100);

        let mut processor = ConsensusProcessor {
            keypair: replica_keypair,
            stage: Stage::Prepare,
            role: ConsensusRole::Replica,
            state: ConsensusState {
                current_view: 0,
                high_qc: QuorumCertificate::genesis(),
                next_qc: QuorumCertificate::genesis(),
                vote_count: 0,
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
            stage: Stage::Prepare,
            block_hash: [0u8; 64],
            sig: replica1_keypair.sign(&[0u8; 64]),
        };

        let vote2 = Vote {
            view_num: 0,
            stage: Stage::Prepare,
            block_hash: [0u8; 64],
            sig: replica2_keypair.sign(&[0u8; 64]),
        };

        let vote3 = Vote {
            view_num: 0,
            stage: Stage::Prepare,
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
            stage: Stage::Prepare,
            role: ConsensusRole::Leader,
            state: ConsensusState {
                current_view: 0,
                high_qc: QuorumCertificate::genesis(),
                next_qc: QuorumCertificate::genesis(),
                vote_count: 0,
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