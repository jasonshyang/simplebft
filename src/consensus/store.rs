use std::collections::HashMap;

use crate::common::crypto::Digest;
use super::message::{Block, Hashable};

pub struct Store {
    pub blocks: HashMap<Digest, Block>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            blocks: HashMap::new(),
        }
    }

    pub fn add_block(&mut self, block: Block) {
        self.blocks.insert(block.hash(), block);
    }
}