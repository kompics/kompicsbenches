use kompact::prelude::SerId;

pub const PING_ID: SerId = 50;
pub const PONG_ID: SerId = 51;
pub const STATIC_PING_ID: SerId = 52;
pub const STATIC_PONG_ID: SerId = 53;

/* serids for Partitioning Actor messages */
pub const PARTITIONING_INIT_MSG: SerId = 44;
pub const PARTITIONING_INIT_ACK_MSG: SerId = PARTITIONING_INIT_MSG;
pub const PARTITIONING_RUN_MSG: SerId = 45;
pub const PARTITIONING_DONE_MSG: SerId = PARTITIONING_RUN_MSG;

pub const ATOMICREG_ID: SerId = 46;
