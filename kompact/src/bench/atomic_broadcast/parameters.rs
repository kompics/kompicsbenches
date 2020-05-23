pub const ELECTION_TIMEOUT: u64 = 300;
pub const OUTGOING_MSGS_PERIOD: u64 = 1;

pub mod paxos {
    pub const GET_DECIDED_PERIOD: u64 = 50;
    pub const TRANSFER_TIMEOUT: u64 = 800;
}

pub mod raft {
    pub const TICK_PERIOD: u64 = 100;   // one tick = +1 in logical clock
    pub const LEADER_HEARTBEAT_PERIOD: u64 = 100;
    pub const RANDOM_DELTA: u64 = 200;
}

pub mod client {
    pub const PROPOSAL_TIMEOUT: u64 = 800;
    pub const MASTER_PID: u64 = 1;
}