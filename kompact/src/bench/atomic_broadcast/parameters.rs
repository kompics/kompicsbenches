pub const ELECTION_TIMEOUT: u64 = 5000;
pub const OUTGOING_MSGS_PERIOD: u64 = 1;
pub const MAX_INFLIGHT: usize = 10000000;   // capacity of number of messages in parallel. Set to max batch size in experiment test space
pub const INITIAL_ELECTION_FACTOR: u64 = 10;   // shorter first election: ELECTION_TIMEOUT/INITIAL_ELECTION_FACTOR
pub const META_RESULTS_DIR: &str = "../meta_results/meta-arcon1-reconfiguration"; // change for each benchmark

pub mod paxos {
    pub const GET_DECIDED_PERIOD: u64 = 1;
    pub const TRANSFER_TIMEOUT: u64 = 300;
    pub const BLE_DELTA: u64 = 100;
    pub const PRIO_START_ROUND: u64 = 10;
}

pub mod raft {
    pub const TICK_PERIOD: u64 = 100;   // one tick = +1 in logical clock
    pub const LEADER_HEARTBEAT_PERIOD: u64 = 1000;
    pub const MAX_BATCH_SIZE: u64 = u64::max_value();
}

pub mod client {
    pub const PROPOSAL_TIMEOUT: u64 = 20000;
}
