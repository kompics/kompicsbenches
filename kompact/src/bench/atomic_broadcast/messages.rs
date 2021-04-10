extern crate raft as tikv_raft;

use crate::{bench::atomic_broadcast::atomic_broadcast::ReconfigurationPolicy, serialiser_ids};
use kompact::prelude::*;
use protobuf::{parse_from_bytes, Message};
use rand::Rng;

pub const DATA_SIZE_HINT: usize = 8; // TODO

pub mod raft {
    extern crate raft as tikv_raft;
    use super::*;
    use kompact::prelude::{Buf, BufMut, Deserialiser, SerError};
    use tikv_raft::prelude::Message as TikvRaftMsg;

    pub struct RawRaftSer;

    #[derive(Debug)]
    pub struct RaftMsg(pub TikvRaftMsg); // wrapper to implement eager serialisation

    impl Serialisable for RaftMsg {
        fn ser_id(&self) -> u64 {
            serialiser_ids::RAFT_ID
        }

        fn size_hint(&self) -> Option<usize> {
            let num_entries = self.0.entries.len();
            Some(60 + num_entries * DATA_SIZE_HINT) // TODO
        }

        fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
            let bytes = self
                .0
                .write_to_bytes()
                .expect("Protobuf failed to serialise TikvRaftMsg");
            buf.put_slice(&bytes);
            Ok(())
        }

        fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
            Ok(self)
        }
    }

    impl Deserialiser<TikvRaftMsg> for RawRaftSer {
        const SER_ID: u64 = serialiser_ids::RAFT_ID;

        fn deserialise(buf: &mut dyn Buf) -> Result<TikvRaftMsg, SerError> {
            let bytes = buf.chunk();
            let remaining = buf.remaining();
            let rm: TikvRaftMsg = if bytes.len() < remaining {
                let mut dst = Vec::with_capacity(remaining);
                buf.copy_to_slice(dst.as_mut_slice());
                parse_from_bytes::<TikvRaftMsg>(dst.as_slice())
                    .expect("Protobuf failed to deserialise TikvRaftMsg")
            } else {
                parse_from_bytes::<TikvRaftMsg>(bytes)
                    .expect("Protobuf failed to deserialise TikvRaftMsg")
            };
            Ok(rm)
        }
    }
}

pub mod paxos {
    use crate::{
        bench::atomic_broadcast::{
            messages::DATA_SIZE_HINT, paxos::ballot_leader_election::Ballot,
        },
        serialiser_ids,
    };
    use kompact::prelude::{Any, Buf, BufMut, Deserialiser, SerError, Serialisable};
    use leaderpaxos::{
        leader_election::Leader,
        messages::*,
        storage::{Entry, StopSign},
    };
    use std::{fmt::Debug, ops::Deref};

    const PREPARE_ID: u8 = 1;
    const PROMISE_ID: u8 = 2;
    const ACCEPTSYNC_ID: u8 = 3;
    const ACCEPTDECIDE_ID: u8 = 4;
    const ACCEPTED_ID: u8 = 5;
    const DECIDE_ID: u8 = 6;
    const PROPOSALFORWARD_ID: u8 = 7;
    const FIRSTACCEPT_ID: u8 = 8;
    const PREPAREREQ_ID: u8 = 9;

    const NORMAL_ENTRY_ID: u8 = 1;
    const SS_ENTRY_ID: u8 = 2;

    // const PAXOS_MSG_OVERHEAD: usize = 17;
    // const BALLOT_OVERHEAD: usize = 16;
    // const DATA_SIZE: usize = 8;
    // const ENTRY_OVERHEAD: usize = 21 + DATA_SIZE;

    pub struct PaxosSer;

    impl PaxosSer {
        fn serialise_ballot(ballot: &Ballot, buf: &mut dyn BufMut) {
            buf.put_u32(ballot.n);
            buf.put_u64(ballot.pid);
        }

        fn serialise_entry(e: &Entry<Ballot>, buf: &mut dyn BufMut) {
            match e {
                Entry::Normal(d) => {
                    buf.put_u8(NORMAL_ENTRY_ID);
                    let data = d.as_slice();
                    buf.put_u32(data.len() as u32);
                    buf.put_slice(data);
                }
                Entry::StopSign(ss) => {
                    buf.put_u8(SS_ENTRY_ID);
                    buf.put_u32(ss.config_id);
                    buf.put_u32(ss.nodes.len() as u32);
                    ss.nodes.iter().for_each(|pid| buf.put_u64(*pid));
                    match ss.skip_prepare_use_leader {
                        Some(l) => {
                            buf.put_u8(1);
                            buf.put_u64(l.pid);
                            Self::serialise_ballot(&l.round, buf);
                        }
                        _ => buf.put_u8(0),
                    }
                }
            }
        }

        pub(crate) fn serialise_entries(ents: &[Entry<Ballot>], buf: &mut dyn BufMut) {
            buf.put_u32(ents.len() as u32);
            for e in ents {
                Self::serialise_entry(e, buf);
            }
        }

        fn deserialise_ballot(buf: &mut dyn Buf) -> Ballot {
            let n = buf.get_u32();
            let pid = buf.get_u64();
            Ballot::with(n, pid)
        }

        fn deserialise_entry(buf: &mut dyn Buf) -> Entry<Ballot> {
            match buf.get_u8() {
                NORMAL_ENTRY_ID => {
                    let data_len = buf.get_u32() as usize;
                    let mut data = vec![0; data_len];
                    buf.copy_to_slice(&mut data);
                    Entry::Normal(data)
                }
                SS_ENTRY_ID => {
                    let config_id = buf.get_u32();
                    let nodes_len = buf.get_u32() as usize;
                    let mut nodes = Vec::with_capacity(nodes_len);
                    for _ in 0..nodes_len {
                        nodes.push(buf.get_u64());
                    }
                    let skip_prepare_use_leader = match buf.get_u8() {
                        1 => {
                            let pid = buf.get_u64();
                            let ballot = Self::deserialise_ballot(buf);
                            Some(Leader::with(pid, ballot))
                        }
                        _ => None,
                    };
                    let ss = StopSign::with(config_id, nodes, skip_prepare_use_leader);
                    Entry::StopSign(ss)
                }
                error_id => panic!("Got unexpected id in deserialise_entry: {}", error_id),
            }
        }

        pub fn deserialise_entries(buf: &mut dyn Buf) -> Vec<Entry<Ballot>> {
            let len = buf.get_u32();
            let mut ents = Vec::with_capacity(len as usize);
            for _ in 0..len {
                ents.push(Self::deserialise_entry(buf));
            }
            ents
        }
    }

    #[derive(Clone, Debug)]
    pub struct PaxosMsgWrapper(pub Message<Ballot>);

    impl Deref for PaxosMsgWrapper {
        type Target = Message<Ballot>;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl Serialisable for PaxosMsgWrapper {
        fn ser_id(&self) -> u64 {
            serialiser_ids::PAXOS_ID
        }

        fn size_hint(&self) -> Option<usize> {
            let overhead = 16;
            let msg_size = match &self.msg {
                PaxosMsg::Prepare(_) => 41,
                PaxosMsg::Promise(p) => 41 + p.sfx.len() * DATA_SIZE_HINT,
                PaxosMsg::AcceptSync(a) => 26 + a.entries.len() * DATA_SIZE_HINT,
                PaxosMsg::FirstAccept(_) => 17 + DATA_SIZE_HINT,
                PaxosMsg::AcceptDecide(a) => 25 + a.entries.len() * DATA_SIZE_HINT,
                PaxosMsg::ProposalForward(pf) => 1 + pf.len() * DATA_SIZE_HINT,
                _ => 25,
            };
            Some(overhead + msg_size)
        }

        fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
            buf.put_u64(self.from);
            buf.put_u64(self.to);
            match &self.msg {
                PaxosMsg::PrepareReq => {
                    buf.put_u8(PREPAREREQ_ID);
                }
                PaxosMsg::Prepare(p) => {
                    buf.put_u8(PREPARE_ID);
                    PaxosSer::serialise_ballot(&p.n, buf);
                    PaxosSer::serialise_ballot(&p.n_accepted, buf);
                    buf.put_u64(p.ld);
                    buf.put_u64(p.sfx_len);
                }
                PaxosMsg::Promise(p) => {
                    buf.put_u8(PROMISE_ID);
                    PaxosSer::serialise_ballot(&p.n, buf);
                    PaxosSer::serialise_ballot(&p.n_accepted, buf);
                    buf.put_u64(p.ld);
                    PaxosSer::serialise_entries(&p.sfx, buf);
                }
                PaxosMsg::AcceptSync(acc_sync) => {
                    buf.put_u8(ACCEPTSYNC_ID);
                    buf.put_u64(acc_sync.ld);
                    let sync: u8 = if acc_sync.sync { 1 } else { 0 };
                    buf.put_u8(sync);
                    PaxosSer::serialise_ballot(&acc_sync.n, buf);
                    PaxosSer::serialise_entries(&acc_sync.entries, buf);
                }
                PaxosMsg::FirstAccept(f) => {
                    buf.put_u8(FIRSTACCEPT_ID);
                    PaxosSer::serialise_ballot(&f.n, buf);
                    PaxosSer::serialise_entries(&f.entries, buf);
                }
                PaxosMsg::AcceptDecide(a) => {
                    buf.put_u8(ACCEPTDECIDE_ID);
                    buf.put_u64(a.ld);
                    PaxosSer::serialise_ballot(&a.n, buf);
                    PaxosSer::serialise_entries(&a.entries, buf);
                }
                PaxosMsg::Accepted(acc) => {
                    buf.put_u8(ACCEPTED_ID);
                    PaxosSer::serialise_ballot(&acc.n, buf);
                    buf.put_u64(acc.la);
                }
                PaxosMsg::Decide(d) => {
                    buf.put_u8(DECIDE_ID);
                    PaxosSer::serialise_ballot(&d.n, buf);
                    buf.put_u64(d.ld);
                }
                PaxosMsg::ProposalForward(entries) => {
                    buf.put_u8(PROPOSALFORWARD_ID);
                    PaxosSer::serialise_entries(entries, buf);
                }
            }
            Ok(())
        }

        fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
            Ok(self)
        }
    }

    impl Deserialiser<Message<Ballot>> for PaxosSer {
        const SER_ID: u64 = serialiser_ids::PAXOS_ID;

        fn deserialise(buf: &mut dyn Buf) -> Result<Message<Ballot>, SerError> {
            let from = buf.get_u64();
            let to = buf.get_u64();
            let msg = match buf.get_u8() {
                PREPAREREQ_ID => Message::with(from, to, PaxosMsg::PrepareReq),
                PREPARE_ID => {
                    let n = Self::deserialise_ballot(buf);
                    let n_accepted = Self::deserialise_ballot(buf);
                    let ld = buf.get_u64();
                    let sfx_len = buf.get_u64();
                    let p = Prepare::with(n, ld, n_accepted, sfx_len);
                    Message::with(from, to, PaxosMsg::Prepare(p))
                }
                PROMISE_ID => {
                    let n = Self::deserialise_ballot(buf);
                    let n_accepted = Self::deserialise_ballot(buf);
                    let ld = buf.get_u64();
                    let sfx = Self::deserialise_entries(buf);
                    let prom = Promise::with(n, n_accepted, sfx, ld);
                    Message::with(from, to, PaxosMsg::Promise(prom))
                }
                ACCEPTSYNC_ID => {
                    let ld = buf.get_u64();
                    let sync = match buf.get_u8() {
                        1 => true,
                        0 => false,
                        _ => panic!("Unexpected sync flag when deserialising acceptsync"),
                    };
                    let n = Self::deserialise_ballot(buf);
                    let sfx = Self::deserialise_entries(buf);
                    let acc_sync = AcceptSync::with(n, sfx, ld, sync);
                    Message::with(from, to, PaxosMsg::AcceptSync(acc_sync))
                }
                ACCEPTDECIDE_ID => {
                    let ld = buf.get_u64();
                    let n = Self::deserialise_ballot(buf);
                    let entries = Self::deserialise_entries(buf);
                    let a = AcceptDecide::with(n, ld, entries);
                    Message::with(from, to, PaxosMsg::AcceptDecide(a))
                }
                ACCEPTED_ID => {
                    let n = Self::deserialise_ballot(buf);
                    let ld = buf.get_u64();
                    let acc = Accepted::with(n, ld);
                    Message::with(from, to, PaxosMsg::Accepted(acc))
                }
                DECIDE_ID => {
                    let n = Self::deserialise_ballot(buf);
                    let ld = buf.get_u64();
                    let d = Decide::with(ld, n);
                    Message::with(from, to, PaxosMsg::Decide(d))
                }
                PROPOSALFORWARD_ID => {
                    let entries = Self::deserialise_entries(buf);
                    let pf = PaxosMsg::ProposalForward(entries);
                    Message::with(from, to, pf)
                }
                FIRSTACCEPT_ID => {
                    let n = Self::deserialise_ballot(buf);
                    let entries = Self::deserialise_entries(buf);
                    let f = FirstAccept::with(n, entries);
                    Message::with(from, to, PaxosMsg::FirstAccept(f))
                }
                _ => {
                    return Err(SerError::InvalidType(
                        "Found unkown id but expected PaxosMsg".into(),
                    ));
                }
            };
            Ok(msg)
        }
    }

    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub struct SegmentIndex {
        pub from: u64,
        pub to: u64,
    }

    impl SegmentIndex {
        pub fn with(from: u64, to: u64) -> Self {
            SegmentIndex { from, to }
        }
    }

    #[derive(Clone, Debug)]
    pub struct SequenceSegment {
        idx: SegmentIndex,
        pub entries: Vec<Entry<Ballot>>,
    }

    impl SequenceSegment {
        pub fn with(idx: SegmentIndex, entries: Vec<Entry<Ballot>>) -> SequenceSegment {
            SequenceSegment { idx, entries }
        }

        pub fn get_index(&self) -> SegmentIndex {
            self.idx
        }

        pub fn get_from_idx(&self) -> u64 {
            self.idx.from
        }

        pub fn get_to_idx(&self) -> u64 {
            self.idx.to
        }
    }

    #[derive(Clone, Debug)]
    pub struct SequenceMetaData {
        pub config_id: u32,
        pub len: u64,
    }

    impl SequenceMetaData {
        pub fn with(config_id: u32, len: u64) -> SequenceMetaData {
            SequenceMetaData { config_id, len }
        }
    }

    #[derive(Clone, Debug)]
    pub struct SegmentTransfer {
        pub config_id: u32,
        pub succeeded: bool,
        pub metadata: SequenceMetaData,
        pub segment: SequenceSegment,
    }

    impl SegmentTransfer {
        pub fn with(
            config_id: u32,
            succeeded: bool,
            metadata: SequenceMetaData,
            segment: SequenceSegment,
        ) -> SegmentTransfer {
            SegmentTransfer {
                config_id,
                succeeded,
                metadata,
                segment,
            }
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct SegmentRequest {
        pub config_id: u32,
        pub idx: SegmentIndex,
        pub requestor_pid: u64,
    }

    impl SegmentRequest {
        pub fn with(config_id: u32, idx: SegmentIndex, requestor_pid: u64) -> SegmentRequest {
            SegmentRequest {
                config_id,
                idx,
                requestor_pid,
            }
        }
    }

    #[derive(Clone, Debug)]
    pub struct Reconfig {
        pub continued_nodes: Vec<u64>,
        pub new_nodes: Vec<u64>,
    }

    impl Reconfig {
        pub fn with(continued_nodes: Vec<u64>, new_nodes: Vec<u64>) -> Reconfig {
            Reconfig {
                continued_nodes,
                new_nodes,
            }
        }

        pub fn len(&self) -> usize {
            self.continued_nodes.len() + self.new_nodes.len()
        }

        pub fn get_peers_of(&self, pid: u64) -> Vec<u64> {
            self.continued_nodes
                .iter()
                .chain(self.new_nodes.iter())
                .filter(|p| p != &&pid)
                .copied()
                .collect()
        }
    }

    #[derive(Clone, Debug)]
    pub enum ReconfigurationMsg {
        Init(ReconfigInit),
        SegmentRequest(SegmentRequest),
        SegmentTransfer(SegmentTransfer),
    }

    #[derive(Clone, Debug)]
    pub struct ReconfigInit {
        pub config_id: u32,
        pub nodes: Reconfig,
        pub seq_metadata: SequenceMetaData,
        pub from: u64,
        pub segment: Option<SequenceSegment>,
        pub skip_prepare_use_leader: Option<Leader<Ballot>>,
    }

    impl ReconfigInit {
        pub fn with(
            config_id: u32,
            nodes: Reconfig,
            seq_metadata: SequenceMetaData,
            from: u64,
            segment: Option<SequenceSegment>,
            skip_prepare_use_leader: Option<Leader<Ballot>>,
        ) -> ReconfigInit {
            ReconfigInit {
                config_id,
                nodes,
                seq_metadata,
                from,
                segment,
                skip_prepare_use_leader,
            }
        }
    }

    const RECONFIG_INIT_ID: u8 = 1;
    const SEQ_REQ_ID: u8 = 2;
    const SEQ_TRANSFER_ID: u8 = 3;

    pub struct ReconfigSer;

    impl Serialisable for ReconfigurationMsg {
        fn ser_id(&self) -> u64 {
            serialiser_ids::RECONFIG_ID
        }

        fn size_hint(&self) -> Option<usize> {
            match self {
                ReconfigurationMsg::Init(_) => Some(64),
                ReconfigurationMsg::SegmentRequest(_) => Some(25),
                ReconfigurationMsg::SegmentTransfer(_) => Some(1000),
            }
        }

        fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
            match self {
                ReconfigurationMsg::Init(r) => {
                    buf.put_u8(RECONFIG_INIT_ID);
                    buf.put_u32(r.config_id);
                    buf.put_u64(r.from);
                    buf.put_u32(r.seq_metadata.config_id);
                    buf.put_u64(r.seq_metadata.len);
                    buf.put_u32(r.nodes.continued_nodes.len() as u32);
                    r.nodes
                        .continued_nodes
                        .iter()
                        .for_each(|pid| buf.put_u64(*pid));
                    buf.put_u32(r.nodes.new_nodes.len() as u32);
                    r.nodes.new_nodes.iter().for_each(|pid| buf.put_u64(*pid));
                    match &r.segment {
                        Some(segment) => {
                            buf.put_u8(1);
                            buf.put_u64(segment.get_from_idx());
                            buf.put_u64(segment.get_to_idx());
                            PaxosSer::serialise_entries(segment.entries.as_slice(), buf);
                        }
                        None => buf.put_u8(0),
                    }
                    match &r.skip_prepare_use_leader {
                        Some(l) => {
                            buf.put_u8(1);
                            buf.put_u64(l.pid);
                            PaxosSer::serialise_ballot(&l.round, buf);
                        }
                        None => buf.put_u8(0),
                    }
                }
                ReconfigurationMsg::SegmentRequest(sr) => {
                    buf.put_u8(SEQ_REQ_ID);
                    buf.put_u32(sr.config_id);
                    buf.put_u64(sr.idx.from);
                    buf.put_u64(sr.idx.to);
                    buf.put_u64(sr.requestor_pid);
                }
                ReconfigurationMsg::SegmentTransfer(st) => {
                    buf.put_u8(SEQ_TRANSFER_ID);
                    buf.put_u32(st.config_id);
                    let succeeded: u8 = if st.succeeded { 1 } else { 0 };
                    buf.put_u8(succeeded);
                    buf.put_u64(st.segment.get_from_idx());
                    buf.put_u64(st.segment.get_to_idx());
                    buf.put_u32(st.metadata.config_id);
                    buf.put_u64(st.metadata.len);
                    PaxosSer::serialise_entries(st.segment.entries.as_slice(), buf);
                }
            }
            Ok(())
        }

        fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
            Ok(self)
        }
    }
    impl Deserialiser<ReconfigurationMsg> for ReconfigSer {
        const SER_ID: u64 = serialiser_ids::RECONFIG_ID;

        fn deserialise(buf: &mut dyn Buf) -> Result<ReconfigurationMsg, SerError> {
            match buf.get_u8() {
                RECONFIG_INIT_ID => {
                    let config_id = buf.get_u32();
                    let from = buf.get_u64();
                    let seq_metadata_config_id = buf.get_u32();
                    let seq_metadata_len = buf.get_u64();
                    let continued_nodes_len = buf.get_u32();
                    let mut continued_nodes = Vec::with_capacity(continued_nodes_len as usize);
                    for _ in 0..continued_nodes_len {
                        continued_nodes.push(buf.get_u64());
                    }
                    let new_nodes_len = buf.get_u32();
                    let mut new_nodes = Vec::with_capacity(new_nodes_len as usize);
                    for _ in 0..new_nodes_len {
                        new_nodes.push(buf.get_u64());
                    }
                    let segment = match buf.get_u8() {
                        1 => {
                            let from_idx = buf.get_u64();
                            let to_idx = buf.get_u64();
                            let idx = SegmentIndex::with(from_idx, to_idx);
                            let entries = PaxosSer::deserialise_entries(buf);
                            Some(SequenceSegment::with(idx, entries))
                        }
                        _ => None,
                    };
                    let skip_prepare_use_leader = match buf.get_u8() {
                        1 => {
                            let leader_pid = buf.get_u64();
                            let ballot = PaxosSer::deserialise_ballot(buf);
                            Some(Leader::with(leader_pid, ballot))
                        }
                        _ => None,
                    };
                    let seq_metadata =
                        SequenceMetaData::with(seq_metadata_config_id, seq_metadata_len);
                    let nodes = Reconfig::with(continued_nodes, new_nodes);
                    let r = ReconfigInit::with(
                        config_id,
                        nodes,
                        seq_metadata,
                        from,
                        segment,
                        skip_prepare_use_leader,
                    );
                    Ok(ReconfigurationMsg::Init(r))
                }
                SEQ_REQ_ID => {
                    let config_id = buf.get_u32();
                    let from_idx = buf.get_u64();
                    let to_idx = buf.get_u64();
                    let idx = SegmentIndex::with(from_idx, to_idx);
                    let requestor_pid = buf.get_u64();
                    let sr = SegmentRequest::with(config_id, idx, requestor_pid);
                    Ok(ReconfigurationMsg::SegmentRequest(sr))
                }
                SEQ_TRANSFER_ID => {
                    let config_id = buf.get_u32();
                    let succeeded = buf.get_u8() == 1;
                    let from_idx = buf.get_u64();
                    let to_idx = buf.get_u64();
                    let idx = SegmentIndex::with(from_idx, to_idx);
                    let metadata_config_id = buf.get_u32();
                    let metadata_seq_len = buf.get_u64();
                    let entries = PaxosSer::deserialise_entries(buf);
                    let metadata = SequenceMetaData::with(metadata_config_id, metadata_seq_len);
                    let segment = SequenceSegment::with(idx, entries);
                    let st = SegmentTransfer::with(config_id, succeeded, metadata, segment);
                    Ok(ReconfigurationMsg::SegmentTransfer(st))
                }
                _ => Err(SerError::InvalidType(
                    "Found unkown id but expected ReconfigurationMsg".into(),
                )),
            }
        }
    }

    pub mod ballot_leader_election {
        use super::super::*;
        use crate::bench::atomic_broadcast::paxos::ballot_leader_election::Ballot;

        #[derive(Clone, Debug)]
        pub enum HeartbeatMsg {
            Request(HeartbeatRequest),
            Reply(HeartbeatReply),
        }

        #[derive(Clone, Debug)]
        pub struct HeartbeatRequest {
            pub round: u32,
            pub max_ballot: Ballot,
        }

        impl HeartbeatRequest {
            pub fn with(round: u32, max_ballot: Ballot) -> HeartbeatRequest {
                HeartbeatRequest { round, max_ballot }
            }
        }

        #[derive(Clone, Debug)]
        pub struct HeartbeatReply {
            pub sender_pid: u64,
            pub round: u32,
            pub max_ballot: Ballot,
        }

        impl HeartbeatReply {
            pub fn with(sender_pid: u64, round: u32, max_ballot: Ballot) -> HeartbeatReply {
                HeartbeatReply {
                    sender_pid,
                    round,
                    max_ballot,
                }
            }
        }

        pub struct BallotLeaderSer;

        const HB_REQ_ID: u8 = 1;
        const HB_REP_ID: u8 = 2;

        impl Serialisable for HeartbeatMsg {
            fn ser_id(&self) -> u64 {
                serialiser_ids::BLE_ID
            }

            fn size_hint(&self) -> Option<usize> {
                Some(55)
            }

            fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
                match self {
                    HeartbeatMsg::Request(req) => {
                        buf.put_u8(HB_REQ_ID);
                        buf.put_u32(req.round);
                        buf.put_u32(req.max_ballot.n);
                        buf.put_u64(req.max_ballot.pid);
                    }
                    HeartbeatMsg::Reply(rep) => {
                        buf.put_u8(HB_REP_ID);
                        buf.put_u64(rep.sender_pid);
                        buf.put_u32(rep.round);
                        buf.put_u32(rep.max_ballot.n);
                        buf.put_u64(rep.max_ballot.pid);
                    }
                }
                Ok(())
            }

            fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
                Ok(self)
            }
        }

        impl Deserialiser<HeartbeatMsg> for BallotLeaderSer {
            const SER_ID: u64 = serialiser_ids::BLE_ID;

            fn deserialise(buf: &mut dyn Buf) -> Result<HeartbeatMsg, SerError> {
                match buf.get_u8() {
                    HB_REQ_ID => {
                        let round = buf.get_u32();
                        let n = buf.get_u32();
                        let pid = buf.get_u64();
                        let max_ballot = Ballot::with(n, pid);
                        let hb_req = HeartbeatRequest::with(round, max_ballot);
                        Ok(HeartbeatMsg::Request(hb_req))
                    }
                    HB_REP_ID => {
                        let sender_pid = buf.get_u64();
                        let round = buf.get_u32();
                        let n = buf.get_u32();
                        let pid = buf.get_u64();
                        let max_ballot = Ballot::with(n, pid);
                        let hb_rep = HeartbeatReply::with(sender_pid, round, max_ballot);
                        Ok(HeartbeatMsg::Reply(hb_rep))
                    }
                    _ => Err(SerError::InvalidType(
                        "Found unkown id but expected HeartbeatMessage".into(),
                    )),
                }
            }
        }
    }
}

/*** Shared Messages***/
#[derive(Clone, Debug)]
pub struct Run;

pub const RECONFIG_ID: u64 = 0;

#[derive(Clone, Debug)]
pub struct Proposal {
    pub data: Vec<u8>,
}

impl Proposal {
    pub fn with(data: Vec<u8>) -> Self {
        Proposal { data }
    }
}

#[derive(Clone, Debug)]
pub struct ReconfigurationProposal {
    pub policy: ReconfigurationPolicy,
    pub new_nodes: Vec<u64>,
}

impl ReconfigurationProposal {
    pub fn with(policy: ReconfigurationPolicy, new_nodes: Vec<u64>) -> Self {
        ReconfigurationProposal { policy, new_nodes }
    }

    pub fn get_new_configuration(
        &self,
        leader_pid: u64,
        current_configuration: Vec<u64>,
    ) -> Vec<u64> {
        let mut nodes: Vec<u64> = current_configuration
            .iter()
            .filter(|pid| pid != &&leader_pid)
            .copied()
            .collect(); // get current followers
        let num_remove = match self.policy {
            ReconfigurationPolicy::ReplaceFollower => self.new_nodes.len(),
            ReconfigurationPolicy::ReplaceLeader => self.new_nodes.len() - 1, // -1 as we will remove leader
        };
        // choose randomly which nodes to remove
        let mut rng = rand::thread_rng();
        for _ in 0..num_remove {
            let num_current_followers = nodes.len();
            let rnd = rng.gen_range(0, num_current_followers);
            nodes.remove(rnd);
        }
        nodes.append(&mut self.new_nodes.clone()); // insert new nodes
        if let ReconfigurationPolicy::ReplaceFollower = self.policy {
            nodes.push(leader_pid);
        }
        nodes
    }
}

#[derive(Clone, Debug)]
pub struct ProposalResp {
    pub data: Vec<u8>,
    pub latest_leader: u64,
}

impl ProposalResp {
    pub fn with(data: Vec<u8>, latest_leader: u64) -> ProposalResp {
        ProposalResp {
            data,
            latest_leader,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReconfigurationResp {
    pub latest_leader: u64,
    pub current_configuration: Vec<u64>,
}

impl ReconfigurationResp {
    pub fn with(latest_leader: u64, current_configuration: Vec<u64>) -> Self {
        ReconfigurationResp {
            latest_leader,
            current_configuration,
        }
    }
}

#[derive(Clone, Debug)]
pub enum AtomicBroadcastMsg {
    Proposal(Proposal),
    ReconfigurationProposal(ReconfigurationProposal),
    ProposalResp(ProposalResp),
    ReconfigurationResp(ReconfigurationResp),
    FirstLeader(u64),
}

const PROPOSAL_ID: u8 = 1;
const PROPOSALRESP_ID: u8 = 2;
const FIRSTLEADER_ID: u8 = 3;
const RECONFIGPROP_ID: u8 = 4;
const RECONFIGRESP_ID: u8 = 5;

/// serialisation ids for ReconfigurationPolicy
const REPLACELEADER_ID: u8 = 1;
const REPLACEFOLLOWER_ID: u8 = 2;

impl Serialisable for AtomicBroadcastMsg {
    fn ser_id(&self) -> u64 {
        serialiser_ids::ATOMICBCAST_ID
    }

    fn size_hint(&self) -> Option<usize> {
        /*let msg_size = match &self {
            AtomicBroadcastMsg::Proposal(p) => {
                let reconfig_len = match p.reconfig.as_ref() {
                    Some(r) => r.len(),
                    _ => 0,
                };
                13 + DATA_SIZE_HINT + reconfig_len * 8
            }
            AtomicBroadcastMsg::ProposalResp(_) => 13 + DATA_SIZE_HINT,
            AtomicBroadcastMsg::PendingReconfiguration(_) => 5 + DATA_SIZE_HINT,
            AtomicBroadcastMsg::FirstLeader(_) => 9,
        };
        Some(msg_size)*/
        None
    }

    fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
        match self {
            AtomicBroadcastMsg::Proposal(p) => {
                buf.put_u8(PROPOSAL_ID);
                let data = p.data.as_slice();
                let data_len = data.len() as u32;
                buf.put_u32(data_len);
                buf.put_slice(data);
            }
            AtomicBroadcastMsg::ReconfigurationProposal(rp) => {
                buf.put_u8(RECONFIGPROP_ID);
                match rp.policy {
                    ReconfigurationPolicy::ReplaceFollower => buf.put_u8(REPLACEFOLLOWER_ID),
                    ReconfigurationPolicy::ReplaceLeader => buf.put_u8(REPLACELEADER_ID),
                }
                buf.put_u32(rp.new_nodes.len() as u32);
                for node in &rp.new_nodes {
                    buf.put_u64(*node);
                }
            }
            AtomicBroadcastMsg::ProposalResp(pr) => {
                buf.put_u8(PROPOSALRESP_ID);
                buf.put_u64(pr.latest_leader);
                let data = pr.data.as_slice();
                let data_len = data.len() as u32;
                buf.put_u32(data_len);
                buf.put_slice(data);
            }
            AtomicBroadcastMsg::ReconfigurationResp(rr) => {
                buf.put_u8(RECONFIGRESP_ID);
                buf.put_u64(rr.latest_leader);
                let config_len: u32 = rr.current_configuration.len() as u32;
                buf.put_u32(config_len);
                for node in &rr.current_configuration {
                    buf.put_u64(*node);
                }
            }
            AtomicBroadcastMsg::FirstLeader(pid) => {
                buf.put_u8(FIRSTLEADER_ID);
                buf.put_u64(*pid);
            }
        }
        Ok(())
    }

    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        Ok(self)
    }
}

pub struct AtomicBroadcastDeser;

impl Deserialiser<AtomicBroadcastMsg> for AtomicBroadcastDeser {
    const SER_ID: u64 = serialiser_ids::ATOMICBCAST_ID;

    fn deserialise(buf: &mut dyn Buf) -> Result<AtomicBroadcastMsg, SerError> {
        match buf.get_u8() {
            PROPOSAL_ID => {
                let data_len = buf.get_u32() as usize;
                let mut data = vec![0; data_len];
                buf.copy_to_slice(&mut data);
                let proposal = Proposal::with(data);
                Ok(AtomicBroadcastMsg::Proposal(proposal))
            }
            RECONFIGPROP_ID => {
                let policy = match buf.get_u8() {
                    REPLACEFOLLOWER_ID => ReconfigurationPolicy::ReplaceFollower,
                    REPLACELEADER_ID => ReconfigurationPolicy::ReplaceLeader,
                    e => {
                        return Err(SerError::InvalidType(format!(
                            "Found unkown ReconfigurationPolicy id: {}, but expected {} or {}",
                            e, REPLACELEADER_ID, REPLACELEADER_ID
                        )))
                    }
                };
                let new_nodes_len = buf.get_u32() as usize;
                let mut new_nodes = Vec::with_capacity(new_nodes_len);
                for _ in 0..new_nodes_len {
                    new_nodes.push(buf.get_u64());
                }
                let rp = ReconfigurationProposal::with(policy, new_nodes);
                Ok(AtomicBroadcastMsg::ReconfigurationProposal(rp))
            }
            PROPOSALRESP_ID => {
                let latest_leader = buf.get_u64();
                let data_len = buf.get_u32() as usize;
                // println!("latest_leader: {}, data_len: {}, buf remaining: {}", latest_leader, data_len, buf.remaining());
                let mut data = vec![0; data_len];
                buf.copy_to_slice(&mut data);
                let pr = ProposalResp {
                    data,
                    latest_leader,
                };
                // print!(" deser ok");
                Ok(AtomicBroadcastMsg::ProposalResp(pr))
            }
            FIRSTLEADER_ID => {
                let pid = buf.get_u64();
                Ok(AtomicBroadcastMsg::FirstLeader(pid))
            }
            RECONFIGRESP_ID => {
                let latest_leader = buf.get_u64();
                let config_len = buf.get_u32() as usize;
                let mut current_config = Vec::with_capacity(config_len);
                for _ in 0..config_len {
                    current_config.push(buf.get_u64());
                }
                let rr = ReconfigurationResp::with(latest_leader, current_config);
                Ok(AtomicBroadcastMsg::ReconfigurationResp(rr))
            }
            _ => Err(SerError::InvalidType(
                "Found unkown id but expected RaftMsg, Proposal or ProposalResp".into(),
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub enum StopMsg {
    Peer(u64),
    Client,
}

const PEER_STOP_ID: u8 = 1;
const CLIENT_STOP_ID: u8 = 2;

impl Serialisable for StopMsg {
    fn ser_id(&self) -> u64 {
        serialiser_ids::STOP_ID
    }

    fn size_hint(&self) -> Option<usize> {
        Some(9)
    }

    fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
        match self {
            StopMsg::Peer(pid) => {
                buf.put_u8(PEER_STOP_ID);
                buf.put_u64(*pid);
            }
            StopMsg::Client => buf.put_u8(CLIENT_STOP_ID),
        }
        Ok(())
    }

    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        Ok(self)
    }
}

pub struct StopMsgDeser;

impl Deserialiser<StopMsg> for StopMsgDeser {
    const SER_ID: u64 = serialiser_ids::STOP_ID;

    fn deserialise(buf: &mut dyn Buf) -> Result<StopMsg, SerError> {
        match buf.get_u8() {
            PEER_STOP_ID => {
                let pid = buf.get_u64();
                Ok(StopMsg::Peer(pid))
            }
            CLIENT_STOP_ID => Ok(StopMsg::Client),
            _ => Err(SerError::InvalidType(
                "Found unkown id but expected Peer stop or client stop".into(),
            )),
        }
    }
}
