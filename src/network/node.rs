use crate::{common::crypto::Keypair, consensus::peers::Peers};

pub struct Node {
    pub keypair: Keypair,
    pub peers: Peers,
}