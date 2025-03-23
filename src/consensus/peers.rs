use crate::common::crypto::Pubkey;

#[derive(Debug, Clone)]
pub struct Peers {
    pub members: Vec<Pubkey>,
}

impl Peers {
    pub fn new(members: Vec<Pubkey>) -> Self {
        Peers { members }
    }

    pub fn get_leader(&self, view_num: u64) -> Pubkey {
        self.members[view_num as usize % self.members.len()].clone()
    }

    pub fn is_member(&self, pubkey: &Pubkey) -> bool {
        self.members.contains(pubkey)
    }
}
