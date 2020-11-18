extern crate raft as tikv_raft;

use crate::serialiser_ids;
use kompact::prelude::*;
use protobuf::{parse_from_bytes, Message};

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
            let bytes = buf.bytes();
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
    use super::super::paxos::raw_paxos::Entry;
    use crate::bench::atomic_broadcast::messages::DATA_SIZE_HINT;
    use crate::bench::atomic_broadcast::paxos::raw_paxos::StopSign;
    use crate::serialiser_ids;
    use ballot_leader_election::Ballot;
    use kompact::prelude::{Any, Buf, BufMut, Deserialiser, SerError, Serialisable};
    use std::fmt::Debug;

    #[derive(Clone, Debug)]
    pub struct Prepare {
        pub n: Ballot,
        pub ld: u64,
        pub n_accepted: Ballot,
    }

    impl Prepare {
        pub fn with(n: Ballot, ld: u64, n_accepted: Ballot) -> Prepare {
            Prepare { n, ld, n_accepted }
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
            Promise {
                n,
                n_accepted,
                sfx,
                ld,
            }
        }
    }

    #[derive(Clone, Debug)]
    pub struct AcceptSync {
        pub n: Ballot,
        pub entries: Vec<Entry>,
        pub ld: u64,
        pub sync: bool, // true -> append on prefix(ld), false -> append
    }

    impl AcceptSync {
        pub fn with(n: Ballot, sfx: Vec<Entry>, ld: u64, sync: bool) -> AcceptSync {
            AcceptSync {
                n,
                entries: sfx,
                ld,
                sync,
            }
        }
    }

    #[derive(Clone, Debug)]
    pub struct FirstAccept {
        pub n: Ballot,
        pub entries: Vec<Entry>,
    }

    impl FirstAccept {
        pub fn with(n: Ballot, entries: Vec<Entry>) -> FirstAccept {
            FirstAccept { n, entries }
        }
    }

    #[derive(Clone, Debug)]
    pub struct AcceptDecide {
        pub n: Ballot,
        pub ld: u64,
        pub entries: Vec<Entry>,
    }

    impl AcceptDecide {
        pub fn with(n: Ballot, ld: u64, entries: Vec<Entry>) -> AcceptDecide {
            AcceptDecide { n, ld, entries }
        }
    }

    #[derive(Clone, Debug)]
    pub struct Accepted {
        pub n: Ballot,
        pub la: u64,
    }

    impl Accepted {
        pub fn with(n: Ballot, la: u64) -> Accepted {
            Accepted { n, la }
        }
    }

    #[derive(Clone, Debug)]
    pub struct Decide {
        pub ld: u64,
        pub n: Ballot,
    }

    impl Decide {
        pub fn with(ld: u64, n: Ballot) -> Decide {
            Decide { ld, n }
        }
    }

    #[derive(Clone, Debug)]
    pub enum PaxosMsg {
        Prepare(Prepare),
        Promise(Promise),
        AcceptSync(AcceptSync),
        FirstAcceptReq,
        FirstAccept(FirstAccept),
        AcceptDecide(AcceptDecide),
        Accepted(Accepted),
        Decide(Decide),
        ProposalForward(Vec<Entry>),
    }

    #[derive(Clone, Debug)]
    pub struct Message {
        pub from: u64,
        pub to: u64,
        pub msg: PaxosMsg,
    }

    impl Message {
        pub fn with(from: u64, to: u64, msg: PaxosMsg) -> Message {
            Message { from, to, msg }
        }
    }

    const PREPARE_ID: u8 = 1;
    const PROMISE_ID: u8 = 2;
    const ACCEPTSYNC_ID: u8 = 3;
    const ACCEPTDECIDE_ID: u8 = 4;
    const ACCEPTED_ID: u8 = 5;
    const DECIDE_ID: u8 = 6;
    const PROPOSALFORWARD_ID: u8 = 7;
    const FIRSTACCEPTREQ_ID: u8 = 8;
    const FIRSTACCEPT_ID: u8 = 9;

    const NORMAL_ENTRY_ID: u8 = 1;
    const SS_ENTRY_ID: u8 = 2;

    // const PAXOS_MSG_OVERHEAD: usize = 17;
    // const BALLOT_OVERHEAD: usize = 16;
    // const DATA_SIZE: usize = 8;
    // const ENTRY_OVERHEAD: usize = 21 + DATA_SIZE;

    pub struct PaxosSer;

    impl PaxosSer {
        fn serialise_ballot(ballot: &Ballot, buf: &mut dyn BufMut) {
            buf.put_u64(ballot.n);
            buf.put_u64(ballot.pid);
        }

        fn serialise_entry(e: &Entry, buf: &mut dyn BufMut) {
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
                    match ss.skip_prepare_n {
                        Some(n) => {
                            buf.put_u8(1);
                            Self::serialise_ballot(&n, buf);
                        }
                        _ => buf.put_u8(0),
                    }
                }
            }
        }

        pub(crate) fn serialise_entries(ents: &[Entry], buf: &mut dyn BufMut) {
            buf.put_u32(ents.len() as u32);
            for e in ents {
                Self::serialise_entry(e, buf);
            }
        }

        fn deserialise_ballot(buf: &mut dyn Buf) -> Ballot {
            let n = buf.get_u64();
            let pid = buf.get_u64();
            Ballot::with(n, pid)
        }

        fn deserialise_entry(buf: &mut dyn Buf) -> Entry {
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
                    let skip_prepare_n = match buf.get_u8() {
                        1 => Some(Self::deserialise_ballot(buf)),
                        _ => None,
                    };
                    let ss = StopSign::with(config_id, nodes, skip_prepare_n);
                    Entry::StopSign(ss)
                }
                error_id => panic!(format!(
                    "Got unexpected id in deserialise_entry: {}",
                    error_id
                )),
            }
        }

        pub fn deserialise_entries(buf: &mut dyn Buf) -> Vec<Entry> {
            let len = buf.get_u32();
            let mut ents = Vec::with_capacity(len as usize);
            for _ in 0..len {
                ents.push(Self::deserialise_entry(buf));
            }
            ents
        }
    }

    impl Serialisable for Message {
        fn ser_id(&self) -> u64 {
            serialiser_ids::PAXOS_ID
        }

        fn size_hint(&self) -> Option<usize> {
            let overhead = 16;
            let msg_size = match &self.msg {
                PaxosMsg::Prepare(_) => 41,
                PaxosMsg::Promise(p) => 41 + p.sfx.len() * DATA_SIZE_HINT,
                PaxosMsg::FirstAcceptReq => 1,
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
                PaxosMsg::Prepare(p) => {
                    buf.put_u8(PREPARE_ID);
                    PaxosSer::serialise_ballot(&p.n, buf);
                    PaxosSer::serialise_ballot(&p.n_accepted, buf);
                    buf.put_u64(p.ld);
                }
                PaxosMsg::Promise(p) => {
                    buf.put_u8(PROMISE_ID);
                    PaxosSer::serialise_ballot(&p.n, buf);
                    PaxosSer::serialise_ballot(&p.n_accepted, buf);
                    buf.put_u64(p.ld);
                    PaxosSer::serialise_entries(&p.sfx, buf);
                }
                PaxosMsg::FirstAcceptReq => {
                    buf.put_u8(FIRSTACCEPTREQ_ID);
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
    impl Deserialiser<Message> for PaxosSer {
        const SER_ID: u64 = serialiser_ids::PAXOS_ID;

        fn deserialise(buf: &mut dyn Buf) -> Result<Message, SerError> {
            let from = buf.get_u64();
            let to = buf.get_u64();
            match buf.get_u8() {
                PREPARE_ID => {
                    let n = Self::deserialise_ballot(buf);
                    let n_accepted = Self::deserialise_ballot(buf);
                    let ld = buf.get_u64();
                    let p = Prepare::with(n, ld, n_accepted);
                    let msg = Message::with(from, to, PaxosMsg::Prepare(p));
                    Ok(msg)
                }
                PROMISE_ID => {
                    let n = Self::deserialise_ballot(buf);
                    let n_accepted = Self::deserialise_ballot(buf);
                    let ld = buf.get_u64();
                    let sfx = Self::deserialise_entries(buf);
                    let prom = Promise::with(n, n_accepted, sfx, ld);
                    let msg = Message::with(from, to, PaxosMsg::Promise(prom));
                    Ok(msg)
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
                    let msg = Message::with(from, to, PaxosMsg::AcceptSync(acc_sync));
                    Ok(msg)
                }
                ACCEPTDECIDE_ID => {
                    let ld = buf.get_u64();
                    let n = Self::deserialise_ballot(buf);
                    let entries = Self::deserialise_entries(buf);
                    let a = AcceptDecide::with(n, ld, entries);
                    let msg = Message::with(from, to, PaxosMsg::AcceptDecide(a));
                    Ok(msg)
                }
                ACCEPTED_ID => {
                    let n = Self::deserialise_ballot(buf);
                    let ld = buf.get_u64();
                    let acc = Accepted::with(n, ld);
                    let msg = Message::with(from, to, PaxosMsg::Accepted(acc));
                    Ok(msg)
                }
                DECIDE_ID => {
                    let n = Self::deserialise_ballot(buf);
                    let ld = buf.get_u64();
                    let d = Decide::with(ld, n);
                    let msg = Message::with(from, to, PaxosMsg::Decide(d));
                    Ok(msg)
                }
                PROPOSALFORWARD_ID => {
                    let entries = Self::deserialise_entries(buf);
                    let pf = PaxosMsg::ProposalForward(entries);
                    let msg = Message::with(from, to, pf);
                    Ok(msg)
                }
                FIRSTACCEPTREQ_ID => {
                    let accsync_req = PaxosMsg::FirstAcceptReq;
                    let msg = Message::with(from, to, accsync_req);
                    Ok(msg)
                }
                FIRSTACCEPT_ID => {
                    let n = Self::deserialise_ballot(buf);
                    let entries = Self::deserialise_entries(buf);
                    let f = FirstAccept::with(n, entries);
                    let msg = Message::with(from, to, PaxosMsg::FirstAccept(f));
                    Ok(msg)
                }
                _ => Err(SerError::InvalidType(
                    "Found unkown id but expected PaxosMsg".into(),
                )),
            }
        }
    }

    #[derive(Clone, Debug)]
    pub struct SequenceSegment {
        pub from_idx: u64,
        pub to_idx: u64,
        pub entries: Vec<Entry>,
    }

    impl SequenceSegment {
        pub fn with(from_idx: u64, to_idx: u64, entries: Vec<Entry>) -> SequenceSegment {
            SequenceSegment {
                from_idx,
                to_idx,
                entries,
            }
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
    pub struct SequenceTransfer {
        pub config_id: u32,
        pub tag: u32,
        pub succeeded: bool,
        pub metadata: SequenceMetaData,
        pub segment: SequenceSegment,
    }

    impl SequenceTransfer {
        pub fn with(
            config_id: u32,
            tag: u32,
            succeeded: bool,
            metadata: SequenceMetaData,
            segment: SequenceSegment,
        ) -> SequenceTransfer {
            SequenceTransfer {
                config_id,
                tag,
                succeeded,
                metadata,
                segment,
            }
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    pub struct SequenceRequest {
        pub config_id: u32,
        pub tag: u32, // keep track of which segment of the sequence this is
        pub from_idx: u64,
        pub to_idx: u64,
        pub requestor_pid: u64,
    }

    impl SequenceRequest {
        pub fn with(
            config_id: u32,
            tag: u32,
            from_idx: u64,
            to_idx: u64,
            requestor_pid: u64,
        ) -> SequenceRequest {
            SequenceRequest {
                config_id,
                tag,
                from_idx,
                to_idx,
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
    }

    #[derive(Clone, Debug)]
    pub enum ReconfigurationMsg {
        Init(ReconfigInit),
        SequenceRequest(SequenceRequest),
        SequenceTransfer(SequenceTransfer),
    }

    #[derive(Clone, Debug)]
    pub struct ReconfigInit {
        pub config_id: u32,
        pub nodes: Reconfig,
        pub seq_metadata: SequenceMetaData,
        pub from: u64,
        pub segment: Option<SequenceSegment>,
        pub skip_prepare_n: Option<Ballot>,
    }

    impl ReconfigInit {
        pub fn with(
            config_id: u32,
            nodes: Reconfig,
            seq_metadata: SequenceMetaData,
            from: u64,
            segment: Option<SequenceSegment>,
            skip_prepare_n: Option<Ballot>,
        ) -> ReconfigInit {
            ReconfigInit {
                config_id,
                nodes,
                seq_metadata,
                from,
                segment,
                skip_prepare_n,
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
                ReconfigurationMsg::SequenceRequest(_) => Some(25),
                ReconfigurationMsg::SequenceTransfer(_) => Some(1000),
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
                            buf.put_u64(segment.from_idx);
                            buf.put_u64(segment.to_idx);
                            PaxosSer::serialise_entries(segment.entries.as_slice(), buf);
                        }
                        None => buf.put_u8(0),
                    }
                    match &r.skip_prepare_n {
                        Some(n) => {
                            buf.put_u8(1);
                            PaxosSer::serialise_ballot(n, buf);
                        }
                        None => buf.put_u8(0),
                    }
                }
                ReconfigurationMsg::SequenceRequest(sr) => {
                    buf.put_u8(SEQ_REQ_ID);
                    buf.put_u32(sr.config_id);
                    buf.put_u32(sr.tag);
                    buf.put_u64(sr.from_idx);
                    buf.put_u64(sr.to_idx);
                    buf.put_u64(sr.requestor_pid);
                }
                ReconfigurationMsg::SequenceTransfer(st) => {
                    buf.put_u8(SEQ_TRANSFER_ID);
                    buf.put_u32(st.config_id);
                    buf.put_u32(st.tag);
                    let succeeded: u8 = if st.succeeded { 1 } else { 0 };
                    buf.put_u8(succeeded);
                    buf.put_u64(st.segment.from_idx);
                    buf.put_u64(st.segment.to_idx);
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
                            let entries = PaxosSer::deserialise_entries(buf);
                            Some(SequenceSegment::with(from_idx, to_idx, entries))
                        }
                        _ => None,
                    };
                    let skip_prepare_n = match buf.get_u8() {
                        1 => Some(PaxosSer::deserialise_ballot(buf)),
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
                        skip_prepare_n,
                    );
                    Ok(ReconfigurationMsg::Init(r))
                }
                SEQ_REQ_ID => {
                    let config_id = buf.get_u32();
                    let tag = buf.get_u32();
                    let from_idx = buf.get_u64();
                    let to_idx = buf.get_u64();
                    let requestor_pid = buf.get_u64();
                    let sr = SequenceRequest::with(config_id, tag, from_idx, to_idx, requestor_pid);
                    Ok(ReconfigurationMsg::SequenceRequest(sr))
                }
                SEQ_TRANSFER_ID => {
                    let config_id = buf.get_u32();
                    let tag = buf.get_u32();
                    let succeeded = buf.get_u8() == 1;
                    let from_idx = buf.get_u64();
                    let to_idx = buf.get_u64();
                    let metadata_config_id = buf.get_u32();
                    let metadata_seq_len = buf.get_u64();
                    let entries = PaxosSer::deserialise_entries(buf);
                    let metadata = SequenceMetaData::with(metadata_config_id, metadata_seq_len);
                    let segment = SequenceSegment::with(from_idx, to_idx, entries);
                    let st = SequenceTransfer::with(config_id, tag, succeeded, metadata, segment);
                    Ok(ReconfigurationMsg::SequenceTransfer(st))
                }
                _ => Err(SerError::InvalidType(
                    "Found unkown id but expected ReconfigurationMsg".into(),
                )),
            }
        }
    }

    pub mod ballot_leader_election {
        use super::super::*;

        #[derive(Clone, Copy, Eq, Debug, Ord, PartialOrd, PartialEq)]
        pub struct Ballot {
            pub n: u64,
            pub pid: u64,
        }

        impl Ballot {
            pub fn with(n: u64, pid: u64) -> Ballot {
                Ballot { n, pid }
            }
        }

        #[derive(Copy, Clone, Debug)]
        pub struct Leader {
            pub pid: u64,
            pub ballot: Ballot,
        }

        impl Leader {
            pub fn with(pid: u64, ballot: Ballot) -> Leader {
                Leader { pid, ballot }
            }
        }

        #[derive(Clone, Debug)]
        pub enum HeartbeatMsg {
            Request(HeartbeatRequest),
            Reply(HeartbeatReply),
        }

        #[derive(Clone, Debug)]
        pub struct HeartbeatRequest {
            pub round: u64,
            pub max_ballot: Ballot,
        }

        impl HeartbeatRequest {
            pub fn with(round: u64, max_ballot: Ballot) -> HeartbeatRequest {
                HeartbeatRequest { round, max_ballot }
            }
        }

        #[derive(Clone, Debug)]
        pub struct HeartbeatReply {
            pub sender_pid: u64,
            pub round: u64,
            pub max_ballot: Ballot,
        }

        impl HeartbeatReply {
            pub fn with(sender_pid: u64, round: u64, max_ballot: Ballot) -> HeartbeatReply {
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
                        buf.put_u64(req.round);
                        buf.put_u64(req.max_ballot.n);
                        buf.put_u64(req.max_ballot.pid);
                    }
                    HeartbeatMsg::Reply(rep) => {
                        buf.put_u8(HB_REP_ID);
                        buf.put_u64(rep.sender_pid);
                        buf.put_u64(rep.round);
                        buf.put_u64(rep.max_ballot.n);
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
                        let round = buf.get_u64();
                        let n = buf.get_u64();
                        let pid = buf.get_u64();
                        let max_ballot = Ballot::with(n, pid);
                        let hb_req = HeartbeatRequest::with(round, max_ballot);
                        Ok(HeartbeatMsg::Request(hb_req))
                    }
                    HB_REP_ID => {
                        let sender_pid = buf.get_u64();
                        let round = buf.get_u64();
                        let n = buf.get_u64();
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
    pub reconfig: Option<(Vec<u64>, Vec<u64>)>,
}

impl Proposal {
    pub fn reconfiguration(data: Vec<u8>, reconfig: (Vec<u64>, Vec<u64>)) -> Proposal {
        Proposal {
            data,
            reconfig: Some(reconfig),
        }
    }

    pub fn normal(data: Vec<u8>) -> Proposal {
        Proposal {
            data,
            reconfig: None,
        }
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
pub enum AtomicBroadcastMsg {
    Proposal(Proposal),
    ProposalResp(ProposalResp),
    FirstLeader(u64),
    PendingReconfiguration(Vec<u8>),
}

const PROPOSAL_ID: u8 = 1;
const PROPOSALRESP_ID: u8 = 2;
const FIRSTLEADER_ID: u8 = 3;
const PENDINGRECONFIG_ID: u8 = 4;

impl Serialisable for AtomicBroadcastMsg {
    fn ser_id(&self) -> u64 {
        serialiser_ids::ATOMICBCAST_ID
    }

    fn size_hint(&self) -> Option<usize> {
        let msg_size = match &self {
            AtomicBroadcastMsg::Proposal(p) => {
                let reconfig_len = match p.reconfig.as_ref() {
                    Some((v, f)) => v.len() + f.len(),
                    _ => 0,
                };
                13 + DATA_SIZE_HINT + reconfig_len * 8
            }
            AtomicBroadcastMsg::ProposalResp(_) => 13 + DATA_SIZE_HINT,
            AtomicBroadcastMsg::PendingReconfiguration(_) => 5 + DATA_SIZE_HINT,
            AtomicBroadcastMsg::FirstLeader(_) => 9,
        };
        Some(msg_size)
    }

    fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
        match self {
            AtomicBroadcastMsg::Proposal(p) => {
                buf.put_u8(PROPOSAL_ID);
                let data = p.data.as_slice();
                let data_len = data.len() as u32;
                buf.put_u32(data_len);
                buf.put_slice(data);
                match &p.reconfig {
                    Some((voters, followers)) => {
                        let voters_len: u32 = voters.len() as u32;
                        buf.put_u32(voters_len);
                        for voter in voters.to_owned() {
                            buf.put_u64(voter);
                        }
                        let followers_len: u32 = followers.len() as u32;
                        buf.put_u32(followers_len);
                        for follower in followers.to_owned() {
                            buf.put_u64(follower);
                        }
                    }
                    None => {
                        buf.put_u32(0); // 0 voters len
                        buf.put_u32(0); // 0 followers len
                    }
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
            AtomicBroadcastMsg::FirstLeader(pid) => {
                buf.put_u8(FIRSTLEADER_ID);
                buf.put_u64(*pid);
            }
            AtomicBroadcastMsg::PendingReconfiguration(data) => {
                buf.put_u8(PENDINGRECONFIG_ID);
                let d = data.as_slice();
                buf.put_u32(d.len() as u32);
                buf.put_slice(d);
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
                let voters_len = buf.get_u32() as usize;
                let mut voters = Vec::with_capacity(voters_len);
                for _ in 0..voters_len {
                    voters.push(buf.get_u64());
                }
                let followers_len = buf.get_u32() as usize;
                let mut followers = Vec::with_capacity(followers_len);
                for _ in 0..followers_len {
                    followers.push(buf.get_u64());
                }
                let reconfig = if voters_len == 0 && followers_len == 0 {
                    None
                } else {
                    Some((voters, followers))
                };
                let proposal = Proposal { data, reconfig };
                Ok(AtomicBroadcastMsg::Proposal(proposal))
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
            PENDINGRECONFIG_ID => {
                let data_len = buf.get_u32() as usize;
                // println!("latest_leader: {}, data_len: {}, buf remaining: {}", latest_leader, data_len, buf.remaining());
                let mut data = vec![0; data_len];
                buf.copy_to_slice(&mut data);
                Ok(AtomicBroadcastMsg::PendingReconfiguration(data))
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

#[derive(Clone, Debug)]
pub struct SequenceResp {
    pub node_id: u64,
    pub sequence: Vec<u64>,
}

impl SequenceResp {
    pub fn with(node_id: u64, sequence: Vec<u64>) -> SequenceResp {
        SequenceResp { node_id, sequence }
    }
}

#[derive(Clone, Debug)]
pub enum TestMessage {
    SequenceReq,
    SequenceResp(SequenceResp),
}

#[derive(Clone)]
pub struct TestMessageSer;

const SEQREQ_ID: u8 = 0;
const SEQRESP_ID: u8 = 1;

impl Serialiser<TestMessage> for TestMessageSer {
    fn ser_id(&self) -> u64 {
        serialiser_ids::TEST_SEQ_ID
    }

    fn size_hint(&self) -> Option<usize> {
        Some(50000)
    }

    fn serialise(&self, msg: &TestMessage, buf: &mut dyn BufMut) -> Result<(), SerError> {
        match msg {
            TestMessage::SequenceReq => {
                buf.put_u8(SEQREQ_ID);
                Ok(())
            }
            TestMessage::SequenceResp(sr) => {
                buf.put_u8(SEQRESP_ID);
                buf.put_u64(sr.node_id);
                let seq_len = sr.sequence.len() as u32;
                buf.put_u32(seq_len);
                for i in &sr.sequence {
                    buf.put_u64(*i);
                }
                Ok(())
            }
        }
    }
}

impl Deserialiser<TestMessage> for TestMessageSer {
    const SER_ID: u64 = serialiser_ids::TEST_SEQ_ID;

    fn deserialise(buf: &mut dyn Buf) -> Result<TestMessage, SerError> {
        match buf.get_u8() {
            SEQREQ_ID => Ok(TestMessage::SequenceReq),
            SEQRESP_ID => {
                let node_id = buf.get_u64();
                let sequence_len = buf.get_u32();
                let mut sequence: Vec<u64> = Vec::new();
                for _ in 0..sequence_len {
                    sequence.push(buf.get_u64());
                }
                let sr = SequenceResp{ node_id, sequence};
                Ok(TestMessage::SequenceResp(sr))
            },
            _ => {
                Err(SerError::InvalidType(
                    "Found unkown id when deserialising TestMessage. Expected SequenceReq or SequenceResp".into(),
                ))
            }
        }
    }
}
