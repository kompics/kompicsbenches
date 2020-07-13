pub const ELECTION_TIMEOUT: u64 = 5000;
pub const OUTGOING_MSGS_PERIOD: u64 = 1;
pub const MAX_INFLIGHT: usize = 1000000;   // capacity of number of messages in parallel. Set to max batch size in experiment test space
pub const META_RESULTS_DIR: &str = "../meta_results/meta2"; // change for each benchmark

pub mod paxos {
    pub const TRANSFER_TIMEOUT: u64 = 300;
    pub const BLE_DELTA: u64 = 20;
    pub const INITIAL_ELECTION_TIMEOUT: u64 = 200;
    pub const BATCH_DECIDE: bool = false;
}

pub mod raft {
    pub const TICK_PERIOD: u64 = 100;   // one tick = +1 in logical clock
    pub const LEADER_HEARTBEAT_PERIOD: u64 = 100;
    pub const RANDOM_DELTA: u64 = 200;
    pub const MAX_BATCH_SIZE: u64 = u64::max_value();
}

pub mod client {
    pub const PROPOSAL_TIMEOUT: u64 = 10000;
}
