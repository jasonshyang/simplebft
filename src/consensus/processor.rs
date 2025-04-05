use tokio::sync::mpsc::{Receiver, Sender};

use crate::common::crypto::Keypair;
use super::{message::{Block, BlockData, BlockDataExt, Hashable, Message, NewView, Proposal, Stage, Vote}, peers::Peers, qc::{ConsensusPayload, QuorumCertificate}, store::Store};

/*
    TODO: Add node parent check
    TODO: Add timeout
    TODO: Handle interruption
    TODO: right now it's not tolerant to network lag where there are missed commit block
*/

pub struct ConsensusProcessor {
    pub id: u64,
    pub keypair: Keypair,
    pub stage: Stage,
    pub role: ConsensusRole,
    pub state: ConsensusState,
    pub store: Store,
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
    pub next_qc: Option<QuorumCertificate>,
    pub vote_count: u64,
    pub peers: Peers,
}

impl ConsensusProcessor {
    pub async fn run(&mut self) {
        println!("Node {}: Running Consensus Processor", self.id);
        loop {
            match self.msg_rx.recv().await {
                Some(message) => {
                    match message {
                        Message::NewView(new_view) => {
                            if self.role == ConsensusRole::Leader {
                                self.handle_new_view(new_view).await;
                            } else {
                                println!("Node {}: Invalid role, expected Leader", self.id);
                                // Do nothing
                            }
                        }
                        Message::Proposal(proposal) => {
                            if self.role == ConsensusRole::Replica {
                                self.handle_proposal(proposal).await;
                            } else {
                                println!("Node {}: Invalid role, expected Replica", self.id);
                                // Do nothing
                            }
                        }
                        Message::Vote(vote) => {
                            if self.role == ConsensusRole::Leader {
                                self.handle_vote(vote).await;
                            } else {
                                println!("Node {}: Invalid role, expected Leader", self.id);
                                // Do nothing
                            }
                        }
                    }
                },
                None => {
                    println!("Node {}: Channel closed", self.id);
                    break;
                },
            }
        }
    }

    async fn handle_new_view(&mut self, new_view: NewView) {
        println!("Node {}: Handling New View", self.id);
        // Check if new_view is signed by peers
        if self.state.peers.is_member(&new_view.sig.signer) == false {
            println!("Node {}: New View not from peers", self.id);
            return;
        }

        // Check if new_view qc is valid
        if new_view.qc.validate(&self.state.peers, self.state.peers.members.len(), self.state.peers.members.len() / 3) == false {
            println!("Node {}: New View QC validation failed", self.id);
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
            println!("Node {}: Vote count quorum reached", self.id);
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
            println!("Node {}: Broadcasted new Proposal, stage: {:?}", self.id, self.stage);

            // Reset vote count
            self.reset_vote_count();

            // Update next QC
            self.reset_next_qc(new_view.qc.payload.clone());
        }


    }

    async fn handle_proposal(&mut self, proposal: Proposal) {
        println!("Node {}: Handling Proposal", self.id);
        // Validate proposal
        if !self.validate_proposal(&proposal) {
            println!("Node {}: Proposal validation failed", self.id);
            return;
        }

        // Update stage
        self.stage = proposal.stage.clone();

        match self.stage {
            Stage::Prepare => {
                println!("Node {}: Stage is Prepare, Voting", self.id);
                // Vote for proposal
                self.vote(&proposal).await;
            }
            Stage::PreCommit => {
                println!("Node {}: Stage is PreCommit, Voting", self.id);
                // Vote for proposal
                self.vote(&proposal).await;
            }
            Stage::Commit => {
                println!("Node {}: Stage is Commit, Voting and Committing", self.id);
                // Vote for proposal
                self.vote(&proposal).await;
                self.commit(proposal.qc.clone());
            }
            Stage::Decide => {
                println!("Node {}: Stage is Decide, Executing", self.id);
                self.execute();
                println!("Node {}: Executed block, preparing new view", self.id);
                self.next_view().await;
                println!("Node {}: New view prepared and sent to leader", self.id);
            }
        }
    }

    async fn handle_vote(&mut self, vote: Vote) {
        println!("Node {}: Handling Vote", self.id);
        // Validate vote
        if !self.validate_vote(&vote) {
            println!("Node {}: Vote validation failed", self.id);
            return;
        }

        // Add vote to QC
        if let Some(next_qc) = &mut self.state.next_qc{
            next_qc.add_signature(vote.sig)
        } else {
            panic!("Node {}: next_qc is None", self.id);
        };
        
        self.increment_vote_count();
        println!("Node {}: Incremented vote count", self.id);

        // Check if next QC is complete
        if self.is_vote_count_quorum() {
            println!("Node {}: Vote count quorum reached", self.id);
            // Update high QC
            self.state.high_qc = self.state.next_qc.clone().unwrap();

            // Broadcast QC
            self.msg_tx.send(Message::Proposal(Proposal {
                view_num: self.state.current_view,
                stage: self.stage.next(),
                block: self.state.high_qc.payload.block.clone(),
                qc: self.state.high_qc.clone(),
                sig: self.keypair.sign(&self.state.high_qc.payload.hash()),
            })).await.unwrap();
            println!("Node {}: Broadcasted new Proposal, stage: {:?}", self.id, self.stage);

            // Advance stage
            self.stage = self.stage.next();
            println!("Node {}: Advanced stage to {:?}", self.id, self.stage);

            // Reset vote count
            self.reset_vote_count();

            // Update next QC
            self.reset_next_qc(self.state.high_qc.payload.clone());
        }
    }

    fn validate_proposal(&self, proposal: &Proposal) -> bool {
        // Check if proposal is from leader
        if proposal.sig.signer != self.state.peers.get_leader(self.state.current_view) {
            println!("Node {}: Proposal not from leader, signed by {:?}", self.id, proposal.sig.signer);
            println!("Node {}: Leader is {:?}", self.id, self.state.peers.get_leader(self.state.current_view));
            return false;
        }

        // Verify QC
        if proposal.qc.validate(&self.state.peers, self.state.peers.members.len(), self.state.peers.members.len() / 3) == false {
            println!("Node {}: Proposal QC validation failed", self.id);
            return false;
        }

        // Liveness rule - The liveness rule is the replica will accept m if m.justify has a higher view than the current lockedQC
        if proposal.qc.payload.view_num > self.state.high_qc.payload.view_num {
            println!("Node {}: Liveness rule failed", self.id);
            return false;
        }

        // Safety rule - The safety rule to accept a proposal is the branch of m.node extends from the currently locked node lockedQC .node
        if proposal.qc.payload.block.parent == self.state.high_qc.payload.block.hash() {
            println!("Node {}: Safety rule failed", self.id);
            return false;
        }

        true
    }

    fn validate_vote(&self, vote: &Vote) -> bool {
        if self.state.peers.is_member(&vote.sig.signer) == false {
            println!("Node {}: Vote not from peers", self.id);
            return false;
        }

        // Check if vote is valid
        if vote.sig.verify(&vote.hash) == false {
            println!("Node {}: Vote signature verification failed", self.id);
            return false;
        }

        // Check if vote is for current view
        if vote.view_num != self.state.current_view {
            println!("Node {}: Vote not for current view", self.id);
            return false;
        }

        // Check if vote is for current stage
        if vote.stage != self.stage {
            println!("Node {}: Vote not for current stage", self.id);
            println!("Node {}: Expected stage: {:?}", self.id, self.stage);
            return false;
        }

        true
    }

    async fn vote(&self, proposal: &Proposal) {
        let hash = proposal.qc.payload.hash();
        let sig = self.keypair.sign(&hash);
        let vote = Vote {
            view_num: self.state.current_view,
            stage: proposal.stage.clone(),
            hash,
            sig,
        };
        println!("Node {}: Built Vote, stage: {:?}", self.id, vote.stage);

        // Send vote
        self.msg_tx.send(Message::Vote(vote)).await.unwrap();
        println!("Node {}: Sent Vote", self.id);
    }

    async fn next_view(&mut self) {
        // Increment view number
        self.state.current_view += 1;

        // Reset vote count
        self.reset_vote_count();

        // Right now we dont rotate roles
        // if self.state.peers.get_leader(self.state.current_view) == self.keypair.pubkey() {
        //     self.role = ConsensusRole::Leader;
        // } else {
        //     self.role = ConsensusRole::Replica;
        // }

        // Take action based on role
        if self.role == ConsensusRole::Leader {
            self.state.next_qc = Some(QuorumCertificate::new(self.state.current_view, Stage::Prepare, self.new_block()));
        } else {
            self.state.next_qc = None;

            let new_view = NewView {
                view_num: self.state.current_view,
                qc: self.state.high_qc.clone(),
                sig: self.keypair.sign(&self.state.high_qc.payload.hash()),
            };
            // Send new view message to leader
            self.msg_tx.send(Message::NewView(new_view)).await.unwrap();
        }
    }

    fn commit(&mut self, qc: QuorumCertificate) {
        self.state.high_qc = qc;
    }

    fn execute(&mut self) {
        // Execute the block
        self.store.add_block(self.state.high_qc.payload.block.clone());
    }

    fn new_block(&self) -> Block {
        // Create a new block
        Block {
            view_num: self.state.current_view,
            parent: self.state.high_qc.payload.block.hash(),
            // TODO: Replace with actual data, currently simplified to just view number *1024
            data: BlockData::new(self.state.current_view * 1024),
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

    fn reset_next_qc(&mut self, payload: ConsensusPayload) {
        if let Some(next_qc) = &mut self.state.next_qc {
            next_qc.reset(payload)
        } else {
            self.state.next_qc = Some(QuorumCertificate::new(payload.view_num, payload.stage, payload.block));
        };
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{common::crypto::Keypair, consensus::{message::Block, qc::ConsensusPayload}};

    #[tokio::test]
    async fn test_end_to_end() {
        let last_view = 1;
        let current_view = 2;
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
        let peers = Peers::new(vec![replica1.pubkey(), replica2.pubkey(), proposer.pubkey(), replica3.pubkey()]);

        // Arrange: Create a genesis block and a genesis QC
        let block = Block {
            view_num: current_view,
            parent: Block::genesis().hash(),
            data: BlockData::new(0),
        };

        let last_qc_payload = ConsensusPayload {
            view_num: last_view,
            stage: Stage::Decide,
            block: block.clone(),
        };

        let last_qc = QuorumCertificate{
            payload: last_qc_payload.clone(),
            signatures: vec![
                replica1.sign(&last_qc_payload.hash()),
                replica2.sign(&last_qc_payload.hash()),
                replica3.sign(&last_qc_payload.hash()),
            ],
        };

        // Arrange: Create new view message for each replica
        let new_view1 = NewView {
            view_num: current_view,
            qc: last_qc.clone(),
            sig: replica1.sign(&block.hash()),
        };

        let new_view2 = NewView {
            view_num: current_view,
            qc: last_qc.clone(),
            sig: replica2.sign(&block.hash()),
        };

        let new_view3 = NewView {
            view_num: current_view,
            qc: last_qc.clone(),
            sig: replica3.sign(&block.hash()),
        };

        let message1 = Message::NewView(new_view1);
        let message2 = Message::NewView(new_view2);
        let message3 = Message::NewView(new_view3);

        // Arrange: Instantiate processers for proposer and replicas
        let mut proposer_processor = ConsensusProcessor {
            id: 0,
            keypair: proposer,
            stage: Stage::Prepare,
            role: ConsensusRole::Leader,
            state: ConsensusState {
                current_view: current_view,
                high_qc: last_qc.clone(),
                next_qc: None,
                vote_count: 0,
                peers: peers.clone(),
            },
            store: Store::new(),
            msg_rx: proposor_incoming_rx,
            msg_tx: proposor_outgoing_tx,
        };

        let mut replica1_processor = ConsensusProcessor {
            id: 1,
            keypair: replica1,
            stage: Stage::Prepare,
            role: ConsensusRole::Replica,
            state: ConsensusState {
                current_view: current_view,
                high_qc: last_qc.clone(),
                next_qc: None,
                vote_count: 0,
                peers: peers.clone(),
            },
            store: Store::new(),
            msg_rx: replica1_incoming_rx,
            msg_tx: replica1_outgoing_tx,
        };

        let mut replica2_processor = ConsensusProcessor {
            id: 2,
            keypair: replica2,
            stage: Stage::Prepare,
            role: ConsensusRole::Replica,
            state: ConsensusState {
                current_view: current_view,
                high_qc: last_qc.clone(),
                next_qc: None,
                vote_count: 0,
                peers: peers.clone(),
            },
            store: Store::new(),
            msg_rx: replica2_incoming_rx,
            msg_tx: replica2_outgoing_tx,
        };

        let mut replica3_processor = ConsensusProcessor {
            id: 3,
            keypair: replica3,
            stage: Stage::Prepare,
            role: ConsensusRole::Replica,
            state: ConsensusState {
                current_view: current_view,
                high_qc: last_qc.clone(),
                next_qc: None,
                vote_count: 0,
                peers: peers.clone(),
            },
            store: Store::new(),
            msg_rx: replica3_incoming_rx,
            msg_tx: replica3_outgoing_tx,
        };

        println!("Node {}: Starting processors", proposer_processor.id);

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
        let decide_msg = proposor_outgoing_rx.recv().await.unwrap();
        // Act: Decide messages broadcasted to replicas
        broadcast(decide_msg, replica_senders.clone()).await;

        // Assert: Check if the commit was successful
        let expect_block = block.clone();
        let expect_view_num = current_view + 1;
        if let Message::NewView(new_view1) = replica1_outgoing_rx.recv().await.unwrap() {
            assert_eq!(new_view1.qc.payload.block, expect_block);
            assert_eq!(new_view1.view_num, expect_view_num);
        } else {
            panic!("Wrong Message Type");
        };

        if let Message::NewView(new_view2) = replica2_outgoing_rx.recv().await.unwrap() {
            assert_eq!(new_view2.qc.payload.block, expect_block);
            assert_eq!(new_view2.view_num, expect_view_num);
        } else {
            panic!("Wrong Message Type");
        };

        if let Message::NewView(new_view3) = replica3_outgoing_rx.recv().await.unwrap() {
            assert_eq!(new_view3.qc.payload.block, expect_block);
            assert_eq!(new_view3.view_num, expect_view_num);
        } else {
            panic!("Wrong Message Type");
        };
    }

    async fn broadcast(message: Message, senders: Vec<Sender<Message>>) {
        for sender in senders {
            sender.send(message.clone()).await.unwrap();
        }
    }
}