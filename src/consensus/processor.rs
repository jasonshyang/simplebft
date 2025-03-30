use tokio::sync::mpsc::{Receiver, Sender};

use crate::common::crypto::Keypair;
use super::{message::{Hashable, Message, NewView, Proposal, Stage, Vote}, peers::Peers, qc::{ConsensusPayload, QuorumCertificate}};

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
            match self.msg_rx.recv().await {
                Some(message) => {
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
                },
                None => {
                    println!("Channel closed");
                    break;
                },
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
            println!("Broadcasted new Proposal, stage: {:?}", self.stage);

            // Reset vote count
            self.reset_vote_count();

            // Update next QC
            self.reset_qc(new_view.qc.payload.clone());
        }


    }

    async fn handle_proposal(&mut self, proposal: Proposal) {
        println!("Handling Proposal");
        // Check if proposal is from leader
        if proposal.sig.signer != self.state.peers.get_leader(self.state.current_view) {
            println!("Proposal not from leader, signed by {:?}", proposal.sig.signer);
            println!("Leader is {:?}", self.state.peers.get_leader(self.state.current_view));
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
        let hash = proposal.qc.payload.hash();
        let sig = self.keypair.sign(&hash);
        let vote = Vote {
            view_num: self.state.current_view,
            stage: proposal.stage.clone(),
            hash,
            sig,
        };
        println!("Built Vote, stage: {:?}", vote.stage);

        // Send vote
        self.msg_tx.send(Message::Vote(vote)).await.unwrap();
        println!("Sent Vote");

        // Update stage
        self.stage = proposal.stage.clone();

    }

    async fn handle_vote(&mut self, vote: Vote) {
        println!("Handling Vote");
        // Check if vote is signed by peers
        if self.state.peers.is_member(&vote.sig.signer) == false {
            println!("Vote not from peers");
            return;
        }

        // Check if vote is valid
        if vote.sig.verify(&vote.hash) == false {
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
            println!("Expected: {:?}, Actual: {:?}", self.stage, vote.stage);
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

            // Broadcast QC
            self.msg_tx.send(Message::Proposal(Proposal {
                view_num: self.state.current_view,
                stage: self.stage.next(),
                block: self.state.high_qc.payload.block.clone(),
                qc: self.state.high_qc.clone(),
                sig: self.keypair.sign(&self.state.high_qc.payload.hash()),
            })).await.unwrap();
            println!("Broadcasted QC, stage: {:?}", self.stage);

            // Advance stage
            self.stage = self.stage.next();
            println!("Stage advanced to {:?}", self.stage);

            // Reset vote count
            self.reset_vote_count();

            // Update next QC
            self.reset_qc(self.state.high_qc.payload.clone());
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

    fn reset_qc(&mut self, payload: ConsensusPayload) {
        self.state.next_qc.reset(payload);
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{common::crypto::Keypair, consensus::{message::Block, qc::ConsensusPayload}};

    #[tokio::test]
    async fn test_end_to_end() {
        // Arrange: Create channels for proposer and replicas
        let (proposor_incoming_tx, proposor_incoming_rx) = tokio::sync::mpsc::channel(100);
        let (proposor_outgoing_tx, mut proposor_outgoing_rx) = tokio::sync::mpsc::channel(100);

        let (replica1_incoming_tx, replica1_incoming_rx) = tokio::sync::mpsc::channel(100);
        let (replica1_outgoing_tx, mut replica1_outgoing_rx) = tokio::sync::mpsc::channel(100);

        let (replica2_incoming_tx, replica2_incoming_rx) = tokio::sync::mpsc::channel(100);
        let (replica2_outgoing_tx, mut replica2_outgoing_rx) = tokio::sync::mpsc::channel(100);

        let (replica3_incoming_tx, replica3_incoming_rx) = tokio::sync::mpsc::channel(100);
        let (replica3_outgoing_tx, mut replica3_outgoing_rx) = tokio::sync::mpsc::channel(100);

        let replica_senders = vec![replica1_incoming_tx, replica2_incoming_tx, replica3_incoming_tx];

        // Arrange: Create keypairs for proposer and replicas
        let proposer = Keypair::new_pair();
        let replica1 = Keypair::new_pair();
        let replica2 = Keypair::new_pair();
        let replica3 = Keypair::new_pair();
        let peers = Peers::new(vec![replica1.pubkey(), proposer.pubkey(), replica2.pubkey(), replica3.pubkey()]);

        // Arrange: Create a genesis block and a genesis QC
        let last_block = Block::genesis();
        let last_qc = QuorumCertificate::genesis();

        let new_block = Block {
            view_num: 1,
            parent: last_block.hash(),
            data: [1u8; 1024],
        };

        let next_qc = QuorumCertificate {
            payload: ConsensusPayload {
                view_num: 1,
                stage: Stage::Decide,
                block: new_block.clone(),
            },
            signatures: Vec::new(),
        };

        // Arrange: Create new view message for each replica
        let new_view1 = NewView {
            view_num: 1,
            qc: last_qc.clone(),
            sig: replica1.sign(&last_block.hash()),
        };

        let new_view2 = NewView {
            view_num: 1,
            qc: last_qc.clone(),
            sig: replica2.sign(&last_block.hash()),
        };

        let new_view3 = NewView {
            view_num: 1,
            qc: last_qc.clone(),
            sig: replica3.sign(&last_block.hash()),
        };

        let message1 = Message::NewView(new_view1);
        let message2 = Message::NewView(new_view2);
        let message3 = Message::NewView(new_view3);

        // Arrange: Instantiate processers for proposer and replicas
        let mut proposer_processor = ConsensusProcessor {
            keypair: proposer,
            stage: Stage::Prepare,
            role: ConsensusRole::Leader,
            state: ConsensusState {
                current_view: 1,
                high_qc: QuorumCertificate::genesis(),
                next_qc,
                vote_count: 0,
                peers: peers.clone(),
            },
            msg_rx: proposor_incoming_rx,
            msg_tx: proposor_outgoing_tx,
        };

        let mut replica1_processor = ConsensusProcessor {
            keypair: replica1,
            stage: Stage::Prepare,
            role: ConsensusRole::Replica,
            state: ConsensusState {
                current_view: 1,
                high_qc: QuorumCertificate::genesis(),
                next_qc: QuorumCertificate::genesis(),
                vote_count: 0,
                peers: peers.clone(),
            },
            msg_rx: replica1_incoming_rx,
            msg_tx: replica1_outgoing_tx,
        };

        let mut replica2_processor = ConsensusProcessor {
            keypair: replica2,
            stage: Stage::Prepare,
            role: ConsensusRole::Replica,
            state: ConsensusState {
                current_view: 1,
                high_qc: QuorumCertificate::genesis(),
                next_qc: QuorumCertificate::genesis(),
                vote_count: 0,
                peers: peers.clone(),
            },
            msg_rx: replica2_incoming_rx,
            msg_tx: replica2_outgoing_tx,
        };

        let mut replica3_processor = ConsensusProcessor {
            keypair: replica3,
            stage: Stage::Prepare,
            role: ConsensusRole::Replica,
            state: ConsensusState {
                current_view: 1,
                high_qc: QuorumCertificate::genesis(),
                next_qc: QuorumCertificate::genesis(),
                vote_count: 0,
                peers: peers.clone(),
            },
            msg_rx: replica3_incoming_rx,
            msg_tx: replica3_outgoing_tx,
        };

        tokio::spawn(async move {
            proposer_processor.run().await;
        });

        tokio::spawn(async move {
            replica1_processor.run().await;
        });

        tokio::spawn(async move {
            replica2_processor.run().await;
        });

        tokio::spawn(async move {
            replica3_processor.run().await;
        });

        // ------------------------------------
        // 1. PREPARE PHASE
        // ------------------------------------

        // Act: Send new view messages to proposer
        proposor_incoming_tx.send(message1).await.unwrap();
        proposor_incoming_tx.send(message2).await.unwrap();
        proposor_incoming_tx.send(message3).await.unwrap();

        // Act: Processor handles new view messages and sends proposal messages
        let prepare_msg = proposor_outgoing_rx.recv().await.unwrap();

        // Act: Proposal messages broadcasted to replicas
        // This currently uses a helper function, will be replaced by a sender struct bound to the node
        broadcast(prepare_msg, replica_senders.clone()).await;

        // Act: Replicas receive proposal messages and send votes
        let vote_msg1 = replica1_outgoing_rx.recv().await.unwrap();
        let vote_msg2 = replica2_outgoing_rx.recv().await.unwrap();
        let vote_msg3 = replica3_outgoing_rx.recv().await.unwrap();

        proposor_incoming_tx.send(vote_msg1).await.unwrap();
        proposor_incoming_tx.send(vote_msg2).await.unwrap();
        proposor_incoming_tx.send(vote_msg3).await.unwrap();

        // ------------------------------------
        // 2. PRE-COMMIT PHASE
        // ------------------------------------

        // Act: Proposer receives votes and sends pre-commit messages
        let pre_commit_msg = proposor_outgoing_rx.recv().await.unwrap();

        // Act: Pre-commit messages broadcasted to replicas
        broadcast(pre_commit_msg, replica_senders.clone()).await;

        // Act: Replicas receive pre-commit messages and send votes
        let vote_msg1 = replica1_outgoing_rx.recv().await.unwrap();
        let vote_msg2 = replica2_outgoing_rx.recv().await.unwrap();
        let vote_msg3 = replica3_outgoing_rx.recv().await.unwrap();

        proposor_incoming_tx.send(vote_msg1).await.unwrap();
        proposor_incoming_tx.send(vote_msg2).await.unwrap();
        proposor_incoming_tx.send(vote_msg3).await.unwrap();

        // ------------------------------------
        // 3. COMMIT PHASE
        // ------------------------------------

        // Act: Proposer receives votes and sends commit messages
        let commit_msg = proposor_outgoing_rx.recv().await.unwrap();

        // Act: Commit messages broadcasted to replicas
        broadcast(commit_msg, replica_senders.clone()).await;

        // Act: Replicas receive commit messages and send votes
        let vote_msg1 = replica1_outgoing_rx.recv().await.unwrap();
        let vote_msg2 = replica2_outgoing_rx.recv().await.unwrap();
        let vote_msg3 = replica3_outgoing_rx.recv().await.unwrap();

        proposor_incoming_tx.send(vote_msg1).await.unwrap();
        proposor_incoming_tx.send(vote_msg2).await.unwrap();
        proposor_incoming_tx.send(vote_msg3).await.unwrap();

        // ------------------------------------
        // 4. DECIDE PHASE
        // ------------------------------------

        // Act: Proposer receives votes and sends decide messages
        proposor_outgoing_rx.recv().await.unwrap();

    }

    async fn broadcast(message: Message, senders: Vec<Sender<Message>>) {
        for sender in senders {
            sender.send(message.clone()).await.unwrap();
        }
    }
}