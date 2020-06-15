pub const ELECTION_TIMEOUT: u64 = 1000;
pub const OUTGOING_MSGS_PERIOD: u64 = 1;
pub const MAX_INFLIGHT: usize = 100000;   // capacity of number of messages in parallel. Set to max batch size in experiment test space
pub const LATENCY_DIR: &str = "../latency_results";
pub const LATENCY_FILE: &str = "latency.out";

pub mod paxos {
    pub const GET_DECIDED_PERIOD: u64 = 1;
    pub const TRANSFER_TIMEOUT: u64 = 300;
    pub const BLE_DELTA: u64 = 20;
}

pub mod raft {
    pub const TICK_PERIOD: u64 = 100;   // one tick = +1 in logical clock
    pub const LEADER_HEARTBEAT_PERIOD: u64 = 100;
    pub const RANDOM_DELTA: u64 = 200;
    pub const MAX_BATCH_SIZE: u64 = u64::max_value();
}

pub mod client {
    pub const PROPOSAL_TIMEOUT: u64 = 800;
}
