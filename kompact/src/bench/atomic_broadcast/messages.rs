extern crate raft as tikv_raft;

use kompact::prelude::*;
use tikv_raft::prelude::Message as TikvRaftMsg;
use crate::serialiser_ids;
use protobuf::{Message, parse_from_bytes};

use self::raft::*;

pub mod raft {
    extern crate raft as tikv_raft;
    use tikv_raft::prelude::Message as TikvRaftMsg;

    #[derive(Clone, Debug)]
    pub struct RaftMsg {
        pub iteration_id: u32,
        pub payload: TikvRaftMsg
    }

    #[derive(Clone, Debug)]
    pub struct CreateTikvRaft {
        pub node_id: u64,
        pub iteration_id: u32
    }
}

pub mod paxos {
    // TODO
    pub mod ballot_leader_election {
        use super::super::*;

        #[derive(Clone, Copy, PartialEq, Debug, PartialOrd)]
        pub struct Ballot {
            pub n: u64,
            pub pid: u64
        }

        impl Ballot {
            pub fn with(n: u64, pid: u64) -> Ballot {
                Ballot{n, pid}
            }
        }

        #[derive(Copy, Clone, Debug)]
        pub struct Leader {
            pub pid: u64,
            pub ballot: Ballot
        }

        impl Leader {
            pub fn with(pid: u64, ballot: Ballot) -> Leader {
                Leader{pid, ballot}
            }
        }

        #[derive(Clone, Debug)]
        pub enum HeartbeatMsg {
            Request(HeartbeatRequest),
            Reply(HeartbeatReply)
        }

        #[derive(Clone, Debug)]
        pub struct HeartbeatRequest {
            pub round: u64,
            pub max_ballot: Ballot,
        }

        impl HeartbeatRequest {
            pub fn with(round: u64, max_ballot: Ballot) -> HeartbeatRequest {
                HeartbeatRequest {round, max_ballot}
            }
        }

        #[derive(Clone, Debug)]
        pub struct HeartbeatReply {
            pub sender_pid: u64,
            pub round: u64,
            pub max_ballot: Ballot
        }

        impl HeartbeatReply {
            pub fn with(sender_pid: u64, round: u64, max_ballot: Ballot) -> HeartbeatReply {
                HeartbeatReply { sender_pid, round, max_ballot}
            }
        }

        pub struct BallotLeaderSer;

        const HB_REQ_ID: u8 = 0;
        const HB_REP_ID: u8 = 1;

        impl Serialiser<HeartbeatMsg> for BallotLeaderSer {
            fn ser_id(&self) -> u64 {
                serialiser_ids::BLE_ID
            }

            fn size_hint(&self) -> Option<usize> {
                Some(30)
            }

            fn serialise(&self, v: &HeartbeatMsg, buf: &mut dyn BufMut) -> Result<(), SerError> {
                match v {
                    HeartbeatMsg::Request(req) => {
                        buf.put_u8(HB_REQ_ID);
                        buf.put_u64_be(req.round);
                        buf.put_u64_be(req.max_ballot.n);
                        buf.put_u64_be(req.max_ballot.pid);
                    }
                    HeartbeatMsg::Reply(rep) => {
                        buf.put_u8(HB_REP_ID);
                        buf.put_u64_be(rep.sender_pid);
                        buf.put_u64_be(rep.round);
                        buf.put_u64_be(rep.max_ballot.n);
                        buf.put_u64_be(rep.max_ballot.pid);
                    }
                }
                Ok(())
            }
        }

        impl Deserialiser<HeartbeatMsg> for BallotLeaderSer {
            const SER_ID: u64 = serialiser_ids::BLE_ID;

            fn deserialise(buf: &mut dyn Buf) -> Result<HeartbeatMsg, SerError> {
                match buf.get_u8() {
                    HB_REQ_ID => {
                        let round = buf.get_u64_be();
                        let n = buf.get_u64_be();
                        let pid = buf.get_u64_be();
                        let max_ballot = Ballot::with(n, pid);
                        let hb_req = HeartbeatRequest::with(round, max_ballot);
                        Ok(HeartbeatMsg::Request(hb_req))
                    }
                    HB_REP_ID => {
                        let sender_pid = buf.get_u64_be();
                        let round = buf.get_u64_be();
                        let n = buf.get_u64_be();
                        let pid = buf.get_u64_be();
                        let max_ballot = Ballot::with(n, pid);
                        let hb_rep = HeartbeatReply::with(sender_pid, round, max_ballot);
                        Ok(HeartbeatMsg::Reply(hb_rep))
                    }
                    _ => {
                        Err(SerError::InvalidType(
                            "Found unkown id but expected HeartbeatMessage".into(),
                        ))
                    }
                }
            }
        }
    }
}

/*** Shared Messages***/
#[derive(Clone, Debug)]
pub struct Run;

#[derive(Clone, Debug)]
pub struct Proposal {
    pub id: u64,
    pub client: ActorPath,
    pub reconfig: Option<(Vec<u64>, Vec<u64>)>,
}

impl Proposal {
    pub fn reconfiguration(id: u64, client: ActorPath, reconfig: (Vec<u64>, Vec<u64>)) -> Proposal {
        let proposal = Proposal {
            id,
            client,
            reconfig: Some(reconfig),
        };
        proposal
    }

    pub fn normal(id: u64, client: ActorPath) -> Proposal {
        let proposal = Proposal {
            id,
            client,
            reconfig: None,
        };
        proposal
    }

    pub fn serialize_normal(&self) -> Result<Vec<u8>, SerError> {   // serialize to use with tikv raft
        let mut buf = vec![];
        buf.put_u64_be(self.id);
        self.client.serialise(&mut buf)?;
        Ok(buf.clone())
    }

    pub fn deserialize_normal(bytes: &Vec<u8>) -> Proposal {  // deserialize from tikv raft
        let mut buf = bytes.into_buf();
        let id = buf.get_u64_be();
        let client = ActorPath::deserialise(&mut buf).expect("No client actorpath in proposal");
        Proposal::normal(id, client)
    }
}

#[derive(Clone, Debug)]
pub struct ProposalResp {
    pub id: u64,
    pub client: Option<ActorPath>,  // client don't need it when receiving it
    pub succeeded: bool,
    pub current_config: Option<(Vec<u64>, Vec<u64>)>,
}

impl ProposalResp {
    pub fn failed(proposal: Proposal) -> ProposalResp {
        let pr = ProposalResp {
            id: proposal.id,
            client: Some(proposal.client),
            succeeded: false,
            current_config: proposal.reconfig
        };
        pr
    }

    pub fn succeeded_normal(proposal: Proposal) -> ProposalResp {
        ProposalResp {
            id: proposal.id,
            client: Some(proposal.client),
            succeeded: true,
            current_config: None
        }
    }

    pub fn succeeded_reconfiguration(client: ActorPath, current_config: (Vec<u64>, Vec<u64>)) -> ProposalResp {
        ProposalResp {
            id: RECONFIG_ID,
            client: Some(client),
            succeeded: true,
            current_config: Some(current_config)
        }
    }
}

#[derive(Clone, Debug)]
pub struct ProposalForward {
    pub leader_id: u64,
    pub proposal: Proposal
}

impl ProposalForward {
    pub fn with(leader_id: u64, proposal: Proposal) -> ProposalForward {
        ProposalForward {
            leader_id,
            proposal
        }
    }
}

#[derive(Clone, Debug)]
pub struct SequenceReq;

#[derive(Clone, Debug)]
pub struct SequenceResp {
    pub node_id: u64,
    pub sequence: Vec<u64>
}

#[derive(Clone, Debug)]
pub enum CommunicatorMsg {
    // TODO: Add PaxosMsg and reuse Communicator and AtomicBroadcastSer in Paxos?
    RaftMsg(RaftMsg),
    Proposal(Proposal),
    ProposalResp(ProposalResp),
    ProposalForward(ProposalForward),
    SequenceReq(SequenceReq),
    SequenceResp(SequenceResp),
    InitAck(u32)
}

const PROPOSAL_ID: u8 = 0;
const PROPOSALRESP_ID: u8 = 1;
const RAFT_MSG_ID: u8 = 2;
const SEQREQ_ID: u8 = 3;
const SEQRESP_ID: u8 = 4;

const PROPOSAL_FAILED: u8 = 0;
const PROPOSAL_SUCCESS: u8 = 1;

pub const RECONFIG_ID: u64 = 0;

pub struct AtomicBroadcastSer;

impl Serialiser<CommunicatorMsg> for AtomicBroadcastSer {
    fn ser_id(&self) -> SerId {
        serialiser_ids::RAFT_ID
    }

    fn size_hint(&self) -> Option<usize> {
        Some(50000)
    }

    fn serialise(&self, enm: &CommunicatorMsg, buf: &mut dyn BufMut) -> Result<(), SerError> {
        match enm {
            CommunicatorMsg::RaftMsg(rm) => {
                buf.put_u8(RAFT_MSG_ID);
                buf.put_u32_be(rm.iteration_id);
                let bytes: Vec<u8> = rm.payload.write_to_bytes().expect("Protobuf failed to serialise TikvRaftMsg");
                buf.put_slice(&bytes);
                Ok(())
            },
            CommunicatorMsg::Proposal(p) => {
                buf.put_u8(PROPOSAL_ID);
                buf.put_u64_be(p.id);
                match &p.reconfig {
                    Some((voters, followers)) => {
                        let voters_len: u32 = voters.len() as u32;
                        buf.put_u32_be(voters_len);
                        for voter in voters.to_owned() {
                            buf.put_u64_be(voter);
                        }
                        let followers_len: u32 = followers.len() as u32;
                        buf.put_u32_be(followers_len);
                        for follower in followers.to_owned() {
                            buf.put_u64_be(follower);
                        }
                    },
                    None => {
                        buf.put_u32_be(0);
                        buf.put_u32_be(0);
                    }
                }
                p.client.serialise(buf).expect("Failed to serialise actorpath");
                Ok(())
            },
            CommunicatorMsg::ProposalResp(pr) => {
                buf.put_u8(PROPOSALRESP_ID);
                buf.put_u64_be(pr.id);
                if pr.succeeded {
                    buf.put_u8(PROPOSAL_SUCCESS);
                } else {
                    buf.put_u8(PROPOSAL_FAILED);
                }
                match &pr.current_config {
                    Some((voters, followers)) => {
                        let voters_len: u32 = voters.len() as u32;
                        buf.put_u32_be(voters_len);
                        for voter in voters.to_owned() {
                            buf.put_u64_be(voter);
                        }
                        let followers_len: u32 = followers.len() as u32;
                        buf.put_u32_be(followers_len);
                        for follower in followers.to_owned() {
                            buf.put_u64_be(follower);
                        }
                    },
                    None => {
                        buf.put_u32_be(0);
                        buf.put_u32_be(0);
                    }
                }
                Ok(())
            },
            CommunicatorMsg::SequenceReq(_) => {
                buf.put_u8(SEQREQ_ID);
                Ok(())
            },
            CommunicatorMsg::SequenceResp(sr) => {
                buf.put_u8(SEQRESP_ID);
                buf.put_u64_be(sr.node_id);
                let seq_len = sr.sequence.len() as u32;
                buf.put_u32_be(seq_len);
                for i in &sr.sequence {
                    buf.put_u64_be(i.clone());
                }
                Ok(())
            },
            _ => {
                Err(SerError::InvalidType("Tried to serialise unknown type".into()))
            }
        }
    }
}

impl Deserialiser<CommunicatorMsg> for AtomicBroadcastSer {
    const SER_ID: u64 = serialiser_ids::RAFT_ID;

    fn deserialise(buf: &mut dyn Buf) -> Result<CommunicatorMsg, SerError> {
        match buf.get_u8(){
            RAFT_MSG_ID => {
                let iteration_id = buf.get_u32_be();
                let bytes = buf.bytes();
                let payload: TikvRaftMsg = parse_from_bytes::<TikvRaftMsg>(bytes).expect("Protobuf failed to deserialise TikvRaftMsg");
                let rm = RaftMsg { iteration_id, payload};
                Ok(CommunicatorMsg::RaftMsg(rm))
            },
            PROPOSAL_ID => {
                let id = buf.get_u64_be();
                let voters_len = buf.get_u32_be();
                let mut voters = vec![];
                for _ in 0..voters_len { voters.push(buf.get_u64_be()); }
                let followers_len = buf.get_u32_be();
                let mut followers = vec![];
                for _ in 0..followers_len { followers.push(buf.get_u64_be()); }
                let reconfig =
                    if voters_len == 0 && followers_len == 0 {
                        None
                    } else {
                        Some((voters, followers))
                    };
                let client = ActorPath::deserialise(buf).expect("Failed to deserialise actorpath");
                let proposal = Proposal {
                    id,
                    client,
                    reconfig
                };
                Ok(CommunicatorMsg::Proposal(proposal))
            },
            PROPOSALRESP_ID => {
                let id = buf.get_u64_be();
                let succeeded: bool = match buf.get_u8() {
                    0 => false,
                    _ => true,
                };
                let voters_len = buf.get_u32_be();
                let mut voters = vec![];
                for _ in 0..voters_len {
                    voters.push(buf.get_u64_be());
                }
                let followers_len = buf.get_u32_be();
                let mut followers = vec![];
                for _ in 0..followers_len {
                    followers.push(buf.get_u64_be());
                }
                let reconfig =
                    if voters_len == 0 && followers_len == 0 {
                        None
                    } else {
                        Some((voters, followers))
                    };
                let pr = ProposalResp {
                    id,
                    succeeded,
                    client: None,
                    current_config: reconfig,
                };
                Ok(CommunicatorMsg::ProposalResp(pr))
            },
            SEQREQ_ID => Ok(CommunicatorMsg::SequenceReq(SequenceReq)),
            SEQRESP_ID => {
                let node_id = buf.get_u64_be();
                let sequence_len = buf.get_u32_be();
                let mut sequence: Vec<u64> = Vec::new();
                for _ in 0..sequence_len {
                    sequence.push(buf.get_u64_be());
                }
                let sr = SequenceResp{ node_id, sequence};
                Ok(CommunicatorMsg::SequenceResp(sr))
            }
            _ => {
                Err(SerError::InvalidType(
                    "Found unkown id but expected RaftMsg, Proposal or ProposalResp".into(),
                ))
            }
        }
    }
}