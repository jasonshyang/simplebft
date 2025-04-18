pub mod message;
pub mod peers;
pub mod qc;
pub mod processor;
pub mod store;

pub use message::*;
pub use peers::*;
pub use qc::*;
pub use processor::*;
pub use store::*;

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc::Sender;

    use crate::common::crypto::Keypair;
    use super::*;

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

        // Arrange: Create blocks
        let last_block = Block {
            view_num: last_view,
            parent: Block::genesis().hash(),
            data: BlockData::new(1),
        };

        let block = Block {
            view_num: current_view,
            parent: last_block.hash(),
            data: BlockData::new(2),
        };

        let last_qc_payload = ConsensusPayload {
            view_num: last_view,
            stage: Stage::Decide,
            block: last_block.clone(),
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
            sig: replica1.sign(&last_qc_payload.hash()),
        };

        let new_view2 = NewView {
            view_num: current_view,
            qc: last_qc.clone(),
            sig: replica2.sign(&last_qc_payload.hash()),
        };

        let new_view3 = NewView {
            view_num: current_view,
            qc: last_qc.clone(),
            sig: replica3.sign(&last_qc_payload.hash()),
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
            next_block: Some(block.clone()),
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
            next_block: None,
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
            next_block: None,
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
            next_block: None,
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
