/*
    HotStuff solves the State Machine Replication (SMR) problem. At the core of SMR is a protocol for deciding on a growing log of command requests by clients. A group of state-machine replicas apply commands in sequence order consistently. A client sends a command request to all replicas, and waits for responses from (f + 1) of them.

    The protocol works in a succession of views numbered with monotonically increasing view numbers. Each viewNumber has a unique dedicated leader known to all. Each replica stores a tree of pending commands as its local data structure. Each tree node contains a proposed command (or a batch of them), metadata associated with the protocol, and a parent link. The branch led by a given node is the path from the node all the way to the tree root by visiting parent links. During the protocol, a monotonically growing branch becomes committed. To become committed, the leader of a particular view proposing the branch must collect votes from a quorum of (n − f ) replicas in three phases, prepare, pre-commit, and commit.

    A key ingredient in the protocol is a collection of (n − f ) votes over a leader proposal, referred to as a quorum certificate (or “QC” in short). The QC is associated with a particular node and a view number. The tcombine utility employs a threshold signature scheme to generate a representation of (n − f ) signed votes as a single authenticator
*/

fn main() {
    println!("Hello, world!");
}