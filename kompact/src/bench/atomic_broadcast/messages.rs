extern crate raft as tikv_raft;

use kompact::prelude::*;
use crate::serialiser_ids;
use protobuf::{Message, parse_from_bytes};

use self::raft::*;

pub mod raft {
    extern crate raft as tikv_raft;
    use tikv_raft::prelude::Message as TikvRaftMsg;
    use kompact::prelude::{Serialiser, SerError, BufMut, Deserialiser, Buf};
    use super::*;

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

    pub struct RaftSer;

    impl Serialiser<RaftMsg> for RaftSer {
        fn ser_id(&self) -> u64 {
            serialiser_ids::RAFT_ID
        }

        fn size_hint(&self) -> Option<usize> {
            Some(500)
        }

        fn serialise(&self, rm: &RaftMsg, buf: &mut dyn BufMut) -> Result<(), SerError> {
            buf.put_u32_be(rm.iteration_id);
            let bytes: Vec<u8> = rm.payload.write_to_bytes().expect("Protobuf failed to serialise TikvRaftMsg");
            buf.put_slice(&bytes);
            Ok(())
        }
    }

    impl Deserialiser<RaftMsg> for RaftSer {
        const SER_ID: u64 = serialiser_ids::RAFT_ID;

        fn deserialise(buf: &mut dyn Buf) -> Result<RaftMsg, SerError> {
            let iteration_id = buf.get_u32_be();
            let bytes = buf.bytes();
            let payload: TikvRaftMsg = parse_from_bytes::<TikvRaftMsg>(bytes).expect("Protobuf failed to deserialise TikvRaftMsg");
            let rm = RaftMsg { iteration_id, payload };
            Ok(rm)
        }
    }
}

pub mod paxos {
    use ballot_leader_election::Ballot;
    use super::super::paxos::raw_paxos::Entry;
    use std::fmt::Debug;
    use kompact::prelude::{Serialiser, SerError, BufMut, Deserialiser, Buf};
    use crate::serialiser_ids;
    use crate::bench::atomic_broadcast::paxos::raw_paxos::StopSign;

    #[derive(Clone, Debug)]
    pub struct Prepare {
        pub n: Ballot,
        pub ld: u64,
        pub n_accepted: Ballot,
    }

    impl Prepare {
        pub fn with(n: Ballot, ld: u64, n_accepted: Ballot) -> Prepare {
            Prepare{ n, ld, n_accepted }
        }
    }

    #[derive(Clone, Debug)]
    pub struct Promise {
        pub n: Ballot,
        pub n_accepted: Ballot,
        pub sfx: Vec<Entry>,
        pub ld: u64,
    }

    impl Promise {
        pub fn with(n: Ballot, n_accepted: Ballot, sfx: Vec<Entry>, ld: u64) -> Promise {
            Promise { n, n_accepted, sfx, ld }
        }
    }

    #[derive(Clone, Debug)]
    pub struct AcceptSync {
        pub n: Ballot,
        pub sfx: Vec<Entry>,
        pub ld: u64
    }

    impl AcceptSync {
        pub fn with(n: Ballot, sfx: Vec<Entry>, ld: u64) -> AcceptSync {
            AcceptSync { n, sfx, ld }
        }
    }

    #[derive(Clone, Debug)]
    pub struct Accept {
        pub n: Ballot,
        pub entry: Entry,
    }

    impl Accept {
        pub fn with(n: Ballot, entry: Entry) -> Accept {
            Accept{ n, entry }
        }
    }

    #[derive(Clone, Debug)]
    pub struct Accepted {
        pub n: Ballot,
        pub la: u64,
    }

    impl Accepted {
        pub fn with(n: Ballot, la: u64) -> Accepted {
            Accepted{ n, la }
        }
    }

    #[derive(Clone, Debug)]
    pub struct Decide {
        pub ld: u64,
        pub n: Ballot,
    }

    impl Decide {
        pub fn with(ld: u64, n: Ballot) -> Decide {
            Decide{ ld, n }
        }
    }

    #[derive(Clone, Debug)]
    pub enum PaxosMsg {
        Prepare(Prepare),
        Promise(Promise),
        AcceptSync(AcceptSync),
        Accept(Accept),
        Accepted(Accepted),
        Decide(Decide)
    }

    #[derive(Clone, Debug)]
    pub struct Message {
        pub from: u64,
        pub to: u64,
        pub msg: PaxosMsg
    }

    impl Message {
        pub fn with(from: u64, to: u64, msg: PaxosMsg) -> Message {
            Message{ from, to, msg }
        }
    }

    const PREPARE_ID: u8 = 1;
    const PROMISE_ID: u8 = 2;
    const ACCEPTSYNC_ID: u8 = 3;
    const ACCEPT_ID: u8 = 4;
    const ACCEPTED_ID: u8 = 5;
    const DECIDE_ID: u8 = 6;

    const NORMAL_ENTRY_ID: u8 = 7;
    const SS_ENTRY_ID: u8 = 8;

    pub struct PaxosSer;

    impl PaxosSer {
        fn serialise_ballot(ballot: &Ballot, buf: &mut dyn BufMut) {
            buf.put_u64_be(ballot.n);
            buf.put_u64_be(ballot.pid);
        }

        fn serialise_entry(e: &Entry, buf: &mut dyn BufMut) {
            match e {
                Entry::Normal(data) => {
                    buf.put_u8(NORMAL_ENTRY_ID);
                    buf.put_u32_be(data.len() as u32);
                    buf.put_slice(data);
                },
                Entry::StopSign(ss) => {
                    buf.put_u8(SS_ENTRY_ID);
                    buf.put_u32_be(ss.config_id);
                    buf.put_u32_be(ss.nodes.len() as u32);
                    ss.nodes.iter().for_each(|pid| buf.put_u64_be(*pid));
                }
            }
        }

        fn serialise_entries(ents: &Vec<Entry>, buf: &mut dyn BufMut) {
            buf.put_u32_be(ents.len() as u32);
            for e in ents {
                Self::serialise_entry(e, buf);
            }
        }

        fn deserialise_ballot(buf: &mut dyn Buf) -> Ballot {
            let n = buf.get_u64_be();
            let pid = buf.get_u64_be();
            Ballot::with(n, pid)
        }

        fn deserialise_entry(buf: &mut dyn Buf) -> Entry {
            match buf.get_u8() {
                NORMAL_ENTRY_ID => {
                    let data_len = buf.get_u32_be();
                    let mut data = Vec::with_capacity(data_len as usize);
                    for _ in 0..data_len {
                        data.push(buf.get_u8());
                    }
                    Entry::Normal(data)
                },
                SS_ENTRY_ID => {
                    let config_id = buf.get_u32_be();
                    let nodes_len = buf.get_u32_be();
                    let mut nodes = vec![];
                    for _ in 0..nodes_len {
                        nodes.push(buf.get_u64_be());
                    }
                    let ss = StopSign::with(config_id, nodes);
                    Entry::StopSign(ss)
                },
                _ => unimplemented!()
            }
        }

        fn deserialise_entries(buf: &mut dyn Buf) -> Vec<Entry> {
            let mut ents = vec![];
            let len = buf.get_u32_be();
            for _ in 0..len {
                ents.push(Self::deserialise_entry(buf));
            }
            ents
        }
    }

    impl Serialiser<Message> for PaxosSer {
        fn ser_id(&self) -> u64 {
            serialiser_ids::PAXOS_ID
        }

        fn size_hint(&self) -> Option<usize> {
            Some(220000)   // TODO?
        }

        fn serialise(&self, m: &Message, buf: &mut dyn BufMut) -> Result<(), SerError> {
            buf.put_u64_be(m.from);
            buf.put_u64_be(m.to);
            match &m.msg {
                PaxosMsg::Prepare(p) => {
                    buf.put_u8(PREPARE_ID);
                    Self::serialise_ballot(&p.n, buf);
                    Self::serialise_ballot(&p.n_accepted, buf);
                    buf.put_u64_be(p.ld);
                },
                PaxosMsg::Promise(p) => {
                    buf.put_u8(PROMISE_ID);
                    Self::serialise_ballot(&p.n, buf);
                    Self::serialise_ballot(&p.n_accepted, buf);
                    buf.put_u64_be(p.ld);
                    Self::serialise_entries(&p.sfx, buf);
                },
                PaxosMsg::AcceptSync(acc_sync) => {
                    buf.put_u8(ACCEPTSYNC_ID);
                    Self::serialise_ballot(&acc_sync.n, buf);
                    Self::serialise_entries(&acc_sync.sfx, buf);
                    buf.put_u64_be(acc_sync.ld);
                }
                PaxosMsg::Accept(a) => {
                    buf.put_u8(ACCEPT_ID);
                    Self::serialise_ballot(&a.n, buf);
                    buf.put_u32_be(1);  // len
                    Self::serialise_entry(&a.entry, buf);
                },
                PaxosMsg::Accepted(acc) => {
                    buf.put_u8(ACCEPTED_ID);
                    Self::serialise_ballot(&acc.n, buf);
                    buf.put_u64_be(acc.la);
                },
                PaxosMsg::Decide(d) => {
                    buf.put_u8(DECIDE_ID);
                    Self::serialise_ballot(&d.n, buf);
                    buf.put_u64_be(d.ld);
                }
            }
            Ok(())
        }
    }

    impl Deserialiser<Message> for PaxosSer {
        const SER_ID: u64 = serialiser_ids::PAXOS_ID;

        fn deserialise(buf: &mut dyn Buf) -> Result<Message, SerError> {
            let from = buf.get_u64_be();
            let to = buf.get_u64_be();
            match buf.get_u8() {
                PREPARE_ID => {
                    let n = Self::deserialise_ballot(buf);
                    let n_accepted = Self::deserialise_ballot(buf);
                    let ld = buf.get_u64_be();
                    let p = Prepare::with(n, ld, n_accepted);
                    let msg = Message::with(from, to, PaxosMsg::Prepare(p));
                    Ok(msg)
                },
                PROMISE_ID => {
                    let n = Self::deserialise_ballot(buf);
                    let n_accepted = Self::deserialise_ballot(buf);
                    let ld = buf.get_u64_be();
                    let sfx = Self::deserialise_entries(buf);
                    let prom = Promise::with(n, n_accepted, sfx, ld);
                    let msg = Message::with(from, to, PaxosMsg::Promise(prom));
                    Ok(msg)
                },
                ACCEPTSYNC_ID => {
                    let n = Self::deserialise_ballot(buf);
                    let sfx = Self::deserialise_entries(buf);
                    let ld = buf.get_u64_be();
                    let acc_sync = AcceptSync::with(n, sfx, ld);
                    let msg = Message::with(from, to, PaxosMsg::AcceptSync(acc_sync));
                    Ok(msg)
                },
                ACCEPT_ID => {
                    let n = Self::deserialise_ballot(buf);
                    let len = buf.get_u32_be();
                    if len != 1 {
                        Err(SerError::InvalidData(
                            "Should only be 1 entry in Accept".into(),
                        ))
                    } else {
                        let entry = Self::deserialise_entry(buf);
                        let a = Accept::with(n, entry);
                        let msg = Message::with(from, to, PaxosMsg::Accept(a));
                        Ok(msg)
                    }
                },
                ACCEPTED_ID => {
                    let n = Self::deserialise_ballot(buf);
                    let ld = buf.get_u64_be();
                    let acc = Accepted::with(n, ld);
                    let msg = Message::with(from, to, PaxosMsg::Accepted(acc));
                    Ok(msg)
                },
                DECIDE_ID => {
                    let n = Self::deserialise_ballot(buf);
                    let ld = buf.get_u64_be();
                    let d = Decide::with(ld, n);
                    let msg = Message::with(from, to, PaxosMsg::Decide(d));
                    Ok(msg)
                },
                _ => {
                    Err(SerError::InvalidType(
                        "Found unkown id but expected PaxosMsg".into(),
                    ))
                }
            }
        }
    }

    #[derive(Clone, Debug)]
    pub struct SequenceMetaData {
        next_config_id: u32,
        len: u64
    }

    impl SequenceMetaData {
        pub fn with(next_config_id: u32, len: u64) -> SequenceMetaData {
            SequenceMetaData{ next_config_id, len }
        }
    }

    #[derive(Clone, Debug)]
    pub struct SequenceTransfer {
        pub config_id: u32,
        pub tag: u32,
        pub seq_len: u32,
        pub seq_ser: Vec<u8>,
        pub metadata: SequenceMetaData
    }

    impl SequenceTransfer {
        pub fn with(
            config_id: u32,
            tag: u32,
            seq_len: u32,
            seq_ser: Vec<u8>,
            metadata: SequenceMetaData
        ) -> SequenceTransfer {
            SequenceTransfer{ config_id, tag, seq_len, seq_ser, metadata }
        }
    }

    #[derive(Clone, Debug)]
    pub struct SequenceRequest {
        pub config_id: u32,
        pub tag: u32,   // keep track of which segment of the sequence this is
        pub from_idx: u64,
        pub to_idx: u64
    }

    impl SequenceRequest {
        pub fn with(config_id: u32, tag: u32, from_idx: u64, to_idx: u64) -> SequenceRequest {
            SequenceRequest{ config_id, tag, from_idx, to_idx }
        }
    }

    #[derive(Clone, Debug)]
    pub struct SequenceResponse {
        pub config_id: u32,
        pub from_idx: u64,
        pub to_idx: u64
    }

    #[derive(Clone, Debug)]
    pub struct ReconfigInit {
        pub config_id: u32,
        pub nodes: Vec<u64>,
    }

    impl ReconfigInit {
        pub fn with(config_id: u32, nodes: Vec<u64>) -> ReconfigInit {
            ReconfigInit{ config_id, nodes }
        }
    }

    pub struct ReplicaSer;

    impl Serialiser<ReconfigInit> for ReplicaSer {
        fn ser_id(&self) -> u64 {
            serialiser_ids::REPLICA_ID
        }

        fn size_hint(&self) -> Option<usize> {
            Some(50) // TODO?
        }

        fn serialise(&self, r: &ReconfigInit, buf: &mut dyn BufMut) -> Result<(), SerError> {
            buf.put_u32_be(r.config_id);
            buf.put_u32_be(r.nodes.len() as u32);
            r.nodes.iter().for_each(|pid| buf.put_u64_be(*pid));
            Ok(())
        }
    }

    impl Deserialiser<ReconfigInit> for ReplicaSer {
        const SER_ID: u64 = serialiser_ids::REPLICA_ID;

        fn deserialise(buf: &mut dyn Buf) -> Result<ReconfigInit, SerError> {
            let config_id = buf.get_u32_be();
            let mut nodes = vec![];
            let nodes_len = buf.get_u32_be();
            for _ in 0..nodes_len {
                nodes.push(buf.get_u64_be());
            }
            let r = ReconfigInit::with(config_id, nodes);
            Ok(r)
        }
    }

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
                Some(50)
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
    pub succeeded: bool,
    pub current_config: Option<(Vec<u64>, Vec<u64>)>,
}

impl ProposalResp {
    pub fn succeeded_normal(id: u64) -> ProposalResp {
        ProposalResp{ id, succeeded: true, current_config: None }
    }

    pub fn succeeded_reconfiguration(current_config: (Vec<u64>, Vec<u64>)) -> ProposalResp {
        ProposalResp {
            id: RECONFIG_ID,
            succeeded: true,
            current_config: Some(current_config)
        }
    }

    pub fn failed(id: u64) -> ProposalResp {
        ProposalResp{ id, succeeded: false, current_config: None }
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
pub struct SequenceResp {
    pub node_id: u64,
    pub sequence: Vec<u64>
}

impl SequenceResp {
    pub fn with(node_id: u64, sequence: Vec<u64>) -> SequenceResp {
        SequenceResp{ node_id, sequence }
    }
}

#[derive(Clone, Debug)]
pub enum CommunicatorMsg {
    RaftMsg(RaftMsg),
    ProposalResp(ActorPath, ProposalResp),
    ProposalForward(ProposalForward),
    SequenceResp(SequenceResp),
    InitAck(u32)
}

#[derive(Clone, Debug)]
pub enum AtomicBroadcastMsg {
    Proposal(Proposal),
    ProposalResp(ProposalResp),
    SequenceReq,
    SequenceResp(SequenceResp)
}

const PROPOSAL_ID: u8 = 0;
const PROPOSALRESP_ID: u8 = 1;
const SEQREQ_ID: u8 = 3;
const SEQRESP_ID: u8 = 4;

const PROPOSAL_FAILED: u8 = 0;
const PROPOSAL_SUCCESS: u8 = 1;

pub const RECONFIG_ID: u64 = 0;

pub struct AtomicBroadcastSer;

impl Serialiser<AtomicBroadcastMsg> for AtomicBroadcastSer {
    fn ser_id(&self) -> SerId {
        serialiser_ids::ATOMICBCAST_ID
    }

    fn size_hint(&self) -> Option<usize> {
        Some(50000)
    }

    fn serialise(&self, enm: &AtomicBroadcastMsg, buf: &mut dyn BufMut) -> Result<(), SerError> {
        match enm {
            AtomicBroadcastMsg::Proposal(p) => {
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
            AtomicBroadcastMsg::ProposalResp(pr) => {
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
            AtomicBroadcastMsg::SequenceReq => {
                buf.put_u8(SEQREQ_ID);
                Ok(())
            },
            AtomicBroadcastMsg::SequenceResp(sr) => {
                buf.put_u8(SEQRESP_ID);
                buf.put_u64_be(sr.node_id);
                let seq_len = sr.sequence.len() as u32;
                buf.put_u32_be(seq_len);
                for i in &sr.sequence {
                    buf.put_u64_be(i.clone());
                }
                Ok(())
            }
        }
    }
}

impl Deserialiser<AtomicBroadcastMsg> for AtomicBroadcastSer {
    const SER_ID: u64 = serialiser_ids::ATOMICBCAST_ID;

    fn deserialise(buf: &mut dyn Buf) -> Result<AtomicBroadcastMsg, SerError> {
        match buf.get_u8(){
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
                Ok(AtomicBroadcastMsg::Proposal(proposal))
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
                    current_config: reconfig,
                };
                Ok(AtomicBroadcastMsg::ProposalResp(pr))
            },
            SEQREQ_ID => Ok(AtomicBroadcastMsg::SequenceReq),
            SEQRESP_ID => {
                let node_id = buf.get_u64_be();
                let sequence_len = buf.get_u32_be();
                let mut sequence: Vec<u64> = Vec::new();
                for _ in 0..sequence_len {
                    sequence.push(buf.get_u64_be());
                }
                let sr = SequenceResp{ node_id, sequence};
                Ok(AtomicBroadcastMsg::SequenceResp(sr))
            }
            _ => {
                Err(SerError::InvalidType(
                    "Found unkown id but expected RaftMsg, Proposal or ProposalResp".into(),
                ))
            }
        }
    }
}