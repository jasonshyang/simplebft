pub mod node;

/*
    Network communication is point-to-point, authenticated and
    reliable: one correct replica receives a message from another correct
    replica if and only if the latter sent that message to the former. When
    we refer to a “broadcast”, it involves the broadcaster, if correct, sending the same point-to-point messages to all replicas, including itself.
    We adopt the partial synchrony model of Dwork et al. [25], where
    there is a known bound ∆ and an unknown Global Stabilization
    Time (GST), such that after GST, all transmissions between two correct replicas arrive within time ∆. Our protocol will ensure safety
    always, and will guarantee progress within a bounded duration
    after GST. (Guaranteeing progress before GST is impossible [27].)
    In practice, our protocol will guarantee progress if the system remains stable (i.e., if messages arrive within ∆ time) for sufficiently
    long after GST, though assuming that it does so forever simplifies
    discussion.
*/