// use std::collections::HashMap;

// use crate::common::crypto::{Digest, Keypair, Pubkey, Signature};
// use crate::consensus::message::BlockData;
// use crate::consensus::{
//     message::{Block, Hashable, Message, MessageType},
//     peers::Peers,
//     qc::QuorumCertificate,
// };

// pub struct Node {
//     pub keypair: Keypair,
//     pub current_view: u64,
//     pub locked_qc: QuorumCertificate,
//     pub peers: Peers,
//     pub store: HashMap<Digest, BlockData>, // TODO: Implement DB to persist data, for now, we use a simple HM in memory
// }

// impl Node {
//     pub fn new(keypair: Keypair, peers: Peers) -> Self {
//         Node {
//             keypair,
//             current_view: 0,
//             locked_qc: QuorumCertificate::genesis(),
//             peers,
//             store: HashMap::new(),
//         }
//     }

//     pub fn sign(&self, digest: &Digest) -> Signature {
//         self.keypair.sign(digest)
//     }

//     pub fn make_propose(&self, qc: QuorumCertificate, data: BlockData) -> Message {
//         let parent = self.locked_qc.payload.block.hash();
//         let block = Block::new(self.current_view, parent, data);

//         let sig = self.keypair.sign(&block.hash());

//         Message {
//             author: self.keypair.pubkey.clone(),
//             view_num: self.current_view,
//             message_type: MessageType::Prepare,
//             block,
//             qc,
//             sig,
//         }
//     }

//     pub fn check_proposal(&self, message: &Message) -> bool {
//         if message.author != self.get_leader() {
//             return false;
//         }

//         // Verify QC
//         if message.qc.validate(&self.peers, self.peers.members.len(), self.peers.members.len() / 3) == false {
//             return false;
//         }

//         // Liveness rule - The liveness rule is the replica will accept m if m.justify has a higher view than the current lockedQC
//         if message.qc.payload.view_num > self.locked_qc.payload.view_num {
//             return true;
//         }

//         // Safety rule - The safety rule to accept a proposal is the branch of m.node extends from the currently locked node lockedQC .node
//         if message.qc.payload.block.parent == self.locked_qc.payload.block.hash() {
//             return true;
//         }
        
//         false
//     }

//     pub fn vote(&self, message: Message) -> Message {
//         let sig = self.keypair.sign(&message.block.hash());

//         Message {
//             author: self.keypair.pubkey.clone(),
//             view_num: self.current_view,
//             message_type: message.message_type,
//             block: message.block.clone(),
//             qc: message.qc.clone(),
//             sig,
//         }
//     }

//     pub fn commit(&mut self, message: Message) {
//         self.store.insert(message.block.hash(), message.block.data.clone());
//         self.current_view += 1;
//     }

//     pub fn create_qc(&self, message_type: MessageType, block: Block) -> QuorumCertificate {
//         QuorumCertificate::new(
//             self.current_view,
//             message_type,
//             block,
//         )
//     }

//     pub fn on_receive(&mut self, message: Message) {
//         match message.message_type {
//             MessageType::NewView => {
//                 // NewView message is sent by a replica as it transitions into viewNumber (including the first view) and carries the highest prepareQC that the replica received (⊥ if none)
//                 // The leader processes these messages in order to select a branch that has the highest preceding view in which a prepareQC was formed
//                 // The leader selects the prepareQC with the highest view, denoted highQC, among the new-view messages
//                 // Because highQC is the highest among (n − f ) replicas, no higher view could have reached a commit decision
//                 // The branch led by highQC.node is therefore safe
//                 // The leader uses the createLeaf method to extend the tail of highQC.node with a new proposal
//                 // The method creates a new leaf node as a child and embeds a digest of the parent in the child node
//                 // The leader then sends the new node in a prepare message to all other replicas
//                 // The proposal carries highQC for safety justification
//                 // Upon receiving the prepare message for the current view from the leader, replica r uses the safeNode predicate to determine whether to accept it
//                 // If it is accepted, the replica sends a prepare vote with a partial signature (produced by tsignr) for the proposal to the leader

                
//             },
//             MessageType::Prepare => {
//                 // When the leader receives (n − f ) prepare votes for the current proposal curProposal, it combines them into a prepareQC
//                 // The leader broadcasts prepareQC in pre-commit messages
//                 // A replica responds to the leader with pre-commit vote having a signed digest of the proposal
//             },
//             MessageType::PreCommit => {
//                 // The pre-commit phase is similar to the prepare phase
//                 // When the leader receives (n − f ) pre-commit votes, it combines them into a precommitQC and broadcasts it in commit messages
//                 // Replicas respond to it with a commit vote
//                 // Importantly, a replica becomes locked on the precommitQC at this point by setting its lockedQC to precommitQC
//                 // This is crucial to guard the safety of the proposal in case it becomes a consensus decision
//             },
//             MessageType::Commit => {
//                 // The commit phase is
//                 // When the leader receives (n − f ) commit votes, it combines them into a commitQC
//                 // Once the leader has assembled a commitQC, it sends it in a decide message to all other replicas
//                 // Upon receiving a decide message, a replica considers the proposal embodied in the commitQC a committed decision, and executes the commands in the committed branch
//                 // The replica increments viewNumber and starts the next view
//             },
//             MessageType::Decide => {
//                 // The decide phase is
//                 // When the leader receives (n−f ) commit votes, it combines them into a commitQC
//                 // Once the leader has assembled a commitQC, it sends it in a decide message to all other replicas
//                 // Upon receiving a decide message, a replica considers the proposal embodied in the commitQC a committed decision, and executes the commands in the committed branch
//                 // The replica increments viewNumber and starts the next view
//             },
//         }
//     }

//     fn get_leader(&self) -> Pubkey {
//         self.peers.get_leader(self.current_view)
//     }
// }
