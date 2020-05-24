pub const ELECTION_TIMEOUT: u64 = 800;
pub const OUTGOING_MSGS_PERIOD: u64 = 1;
pub const CAPACITY: usize = 100000;   // capacity of number of messages in parallel. Set to max batch size in experiment test space

pub mod paxos {
    pub const GET_DECIDED_PERIOD: u64 = 20;
    pub const TRANSFER_TIMEOUT: u64 = 300;
}

pub mod raft {
    pub const TICK_PERIOD: u64 = 100;   // one tick = +1 in logical clock
    pub const LEADER_HEARTBEAT_PERIOD: u64 = 100;
    pub const RANDOM_DELTA: u64 = 200;
    pub const MAX_MSG_SIZE: u64 = 64000;    // determines how much batching. Set to max TCP msg size
}

pub mod client {
    pub const PROPOSAL_TIMEOUT: u64 = 800;
    pub const MASTER_PID: u64 = 1;
}