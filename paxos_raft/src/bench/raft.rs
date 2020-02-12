extern crate raft as tikv_raft;

use kompact::prelude::*;
//use kompact_benchmarks::partitioning_actor::*;
use std::collections::HashMap;
use tikv_raft::eraftpb::ConfState;
use tikv_raft::storage::MemStorage;
use tikv_raft::{prelude::*, StateRole, prelude::Message as TikvRaftMsg};
use protobuf::{Message as PbMessage, parse_from_bytes};
use super::super::serialiser_ids as serialiser_ids;
use std::time::Duration;
use uuid::Uuid;

const CONFICCHANGE_ID: u64 = 0; // use 0 for configuration change

pub mod raft {
    // TODO add parameter in protoc to use memory or disk
    use super::*;

    struct MessagingPort;

    impl Port for MessagingPort {
        type Indication = RaftCompMsg;
        type Request = CommunicatorMsg;
    }

    #[derive(ComponentDefinition)]
    struct Communicator {
        ctx: ComponentContext<Communicator>,
        raft_port: ProvidedPort<MessagingPort, Communicator>,
        peers: HashMap<u64, ActorPath>, // tikv raft node id -> actorpath
    }

    impl Communicator {
        fn new() -> Communicator {
            Communicator {
                ctx: ComponentContext::new(),
                raft_port: ProvidedPort::new(),
                peers: HashMap::new()
            }
        }
    }

    impl Provide<ControlPort> for Communicator {
        fn handle(&mut self, _event: <ControlPort as Port>::Request) -> () {
            // ignore
        }
    }

    impl Provide<MessagingPort> for Communicator {
        fn handle(&mut self, msg: CommunicatorMsg) {
            let receiver: &ActorPath = match &msg {
                CommunicatorMsg::TikvRaftMsg(rm) => self.peers.get(&rm.get_to()).expect(format!("Could not find actorpath for id={}", &rm.get_to()).as_ref()),
                CommunicatorMsg::Proposal(p) => &p.client,
                CommunicatorMsg::ProposalResp(pr) => pr.client.as_ref().expect("No actorpath provided in ProposalResp")
            };
            receiver.tell((msg.clone(), RaftSer), self);
        }
    }

    impl Actor for Communicator {
        type Message = ();

        fn receive_local(&mut self, _msg: Self::Message) -> () {
            unimplemented!()
        }

        fn receive_network(&mut self, m: NetMessage) -> () {
            match_deser! {m; {
                comm_msg: CommunicatorMsg [RaftSer] => {
                    match comm_msg {
                        CommunicatorMsg::TikvRaftMsg(rm) => self.raft_port.trigger(RaftCompMsg::TikvRaftMsg(rm)),
                        CommunicatorMsg::Proposal(p) => self.raft_port.trigger(RaftCompMsg::Proposal(p)),
                        _ => error!(self.ctx.log(), "Got unexpected msg: {:?}", comm_msg),
                    }
                },
                !Err(e) => error!(self.ctx.log(), "Error deserialising msg: {:?}", e),
            }
            }
        }
    }

    #[derive(ComponentDefinition)]
    struct RaftComp {
        ctx: ComponentContext<RaftComp>,
        raft_node: Option<RawNode<MemStorage>>,
        communication_port: RequiredPort<MessagingPort, RaftComp>,
        reconfig_client: Option<ActorPath>,
    }

    impl Provide<ControlPort> for RaftComp {
        fn handle(&mut self, _event: ControlEvent) -> () {
            // ignore
        }
    }

    impl Require<MessagingPort> for RaftComp {
        fn handle(&mut self, msg: RaftCompMsg) -> () {
            match msg {
                RaftCompMsg::TikvRaftMsg(rm) => {
                    self.step(rm);
                }
                RaftCompMsg::Proposal(p) => {
                    let raft_node = self.raft_node.as_mut().expect("TikvRaftNode not initialized in RaftComp");
                    if raft_node.raft.state == StateRole::Leader{
                        self.propose(p);
                    }
                    // TODO check if TIKV handles it or if we have to do forward to leader ourselves
                    /*else {
                        match self.peers.get(raft.leader_id) {
                            Some(leader) => {
                                let m =
                                self.communication_port.trigger()
                            }
                        }
                    }*/
                }
            }
        }
    }

    impl RaftComp {
        fn with() -> RaftComp { // TODO add experiment params
            RaftComp {
                ctx: ComponentContext::new(),
                raft_node: None,
                communication_port: RequiredPort::new(),
                reconfig_client: None,
            }
        }

        fn start_timers(&mut self){
            let delay = Duration::from_millis(0);
            let on_ready_period = Duration::from_millis(1);
            let tick_period = Duration::from_millis(100);
            let on_ready_uuid = uuid::Uuid::new_v4();
            let tick_uuid = uuid::Uuid::new_v4();
            // give new reference to self to make compiler happy
            self.schedule_periodic(delay, on_ready_period, move |c, on_ready_uuid| c.on_ready());
            self.schedule_periodic(delay, tick_period, move |rc, tick_uuid| rc.tick());
        }

        fn tick(&mut self) {
            self.raft_node.as_mut()
                .expect("TikvRaftNode not initialized in RaftComp")
                .tick();
        }

        // Step a raft message, initialize the raft if need.
        fn step(&mut self, msg: TikvRaftMsg) {
            let _ = self.raft_node.as_mut().expect("TikvRaftNode not initialized in RaftComp").step(msg);
        }

        fn propose(&mut self, proposal: Proposal) {
            let raft_node = self.raft_node.as_mut().expect("TikvRaftNode not initialized in RaftComp");
            let last_index1 = raft_node.raft.raft_log.last_index() + 1;
            match &proposal.conf_change {
                Some(cc) => {
                    self.reconfig_client = Some(proposal.client.clone());
                    let _ = raft_node.propose_conf_change(vec![], cc.clone());
                }
                None => {   // i.e normal operation
                    let ser_data = proposal.serialize_normal();
                    match ser_data {
                        Ok(data) => {
                            let _ = raft_node.propose(vec![], data);
                        },
                        _ => {
                            eprintln!("Cannot propose due to tikv serialization failure...");
                            // Propose failed, don't forget to respond to the client.
                            let pr = ProposalResp::failed(&proposal);
                            self.communication_port.trigger(CommunicatorMsg::ProposalResp(pr));
                        }
                    }

                }
            }
            let last_index2 = raft_node.raft.raft_log.last_index() + 1;
            if last_index2 == last_index1 {
                // Propose failed, don't forget to respond to the client.
                let pr = ProposalResp::failed(&proposal);
                self.communication_port.trigger(CommunicatorMsg::ProposalResp(pr));
            }
        }

        fn on_ready(&mut self) {
            let raft_node = self.raft_node.as_mut().expect("TikvRaftNode not initialized in RaftComp");
            if !raft_node.has_ready() {
                return;
            }
            let store = raft_node.raft.raft_log.store.clone();

            // Get the `Ready` with `RawNode::ready` interface.
            let mut ready = raft_node.ready();

            // Persistent raft logs. It's necessary because in `RawNode::advance` we stabilize
            // raft logs to the latest position.
            if let Err(e) = store.wl().append(ready.entries()) {
                eprintln!("persist raft log fail: {:?}, need to retry or panic", e);
                return;
            }

            // Apply the snapshot. It's necessary because in `RawNode::advance` we stabilize the snapshot.
            if *ready.snapshot() != Snapshot::default() {
                let s = ready.snapshot().clone();
                if let Err(e) = store.wl().apply_snapshot(s) {
                    eprintln!("apply snapshot fail: {:?}, need to retry or panic", e);
                    return;
                }
            }

            // Send out the messages come from the node.
            for msg in ready.messages.drain(..) {
                self.communication_port.trigger(CommunicatorMsg::TikvRaftMsg(msg));
            }

            // Apply all committed proposals.
            if let Some(committed_entries) = ready.committed_entries.take() {
                for entry in &committed_entries {
                    if entry.data.is_empty() {
                        // From new elected leaders.
                        continue;
                    }
                    if let EntryType::EntryConfChange = entry.get_entry_type() {
                        // For conf change messages, make them effective.
                        let mut cc = ConfChange::default();
                        cc.merge_from_bytes(&entry.data).unwrap();
                        let node_id = cc.node_id;
                        match cc.get_change_type() {
                            ConfChangeType::AddNode => raft_node.raft.add_node(node_id).unwrap(),
                            ConfChangeType::RemoveNode => raft_node.raft.remove_node(node_id).unwrap(),
                            ConfChangeType::AddLearnerNode => raft_node.raft.add_learner(node_id).unwrap(),
                            ConfChangeType::BeginMembershipChange
                            | ConfChangeType::FinalizeMembershipChange => unimplemented!(),
                        }
                        let cs = ConfState::from(raft_node.raft.prs().configuration().clone());
                        store.wl().set_conf_state(cs, None);
                        if raft_node.raft.state == StateRole::Leader && self.reconfig_client.is_some() {
                            // TODO what if proposed by one node and that node is no longer the leader?
                            let client = self.reconfig_client.clone().expect("Should have cached Reconfiguration client");
                            let pr = ProposalResp::succeeded_configchange(client);
                            self.communication_port.trigger(CommunicatorMsg::ProposalResp(pr));
                        }
                    } else {
                        // For normal proposals, extract the key-value pair and then
                        // insert them into the kv engine.
                        let data = Proposal::deserialize_normal(&entry.data);
                        // TODO write to disk for LogCabin comparison?
                        if raft_node.raft.state == StateRole::Leader {
                            let pr = ProposalResp::succeeded_normal(&data, entry.get_term(), entry.get_index());
                            self.communication_port.trigger(CommunicatorMsg::ProposalResp(pr));
                        }
                    }

                }
                if let Some(last_committed) = committed_entries.last() {
                    let mut s = store.wl();
                    s.mut_hard_state().commit = last_committed.index;
                    s.mut_hard_state().term = last_committed.term;
                }
            }
            // Call `RawNode::advance` interface to update position flags in the raft.
            raft_node.advance(ready);
        }
    }

    struct Init {
        id: u64,
//        peers: HashMap<u64, ActorPath>,
//        config: Config
    }

    impl Actor for RaftComp {
        type Message = ();

        fn receive_local(&mut self, _msg: Self::Message) -> () {
            unimplemented!()
        }

        fn receive_network(&mut self, _msg: NetMessage) -> () {
            unimplemented!()
        }
    }

    /*** All messages and their serializers ***/
    #[derive(Clone, Debug)]
    enum RaftCompMsg {
        TikvRaftMsg(TikvRaftMsg),
        Proposal(Proposal)
    }

    #[derive(Clone, Debug)]
    enum CommunicatorMsg {
        TikvRaftMsg(TikvRaftMsg),
        Proposal(Proposal),
        ProposalResp(ProposalResp)
    }

    #[derive(Clone, Debug)]
    struct Proposal {
        id: u64,
        client: ActorPath,
        conf_change: Option<ConfChange>, // conf change.
    }

    impl Proposal {
        fn conf_change(id: u64, client: &ActorPath, cc: &ConfChange) -> Proposal {
            let proposal = Proposal {
                id,
                client: client.clone(),
                conf_change: Some(cc.clone()),
            };
            proposal
        }

        fn normal(id: u64, client: &ActorPath) -> Proposal {
            let proposal = Proposal {
                id,
                client: client.clone(),
                conf_change: None,
            };
            proposal
        }

        fn serialize_normal(&self) -> Result<Vec<u8>, SerError> {   // serialize to use with tikv raft
            let mut buf = vec![];
            buf.put_u64_be(self.id);
            self.client.serialise(&mut buf)?;
            Ok(buf.clone())
        }

        fn deserialize_normal(bytes: &Vec<u8>) -> Proposal {  // deserialize from tikv raft
            let mut buf = bytes.into_buf();
            let id = buf.get_u64_be();
            let client = ActorPath::deserialise(&mut buf).expect("No client actorpath in proposal");
            Proposal::normal(id, &client)
        }
    }

    #[derive(Clone, Debug)]
    struct ProposalResp {
        id: u64,
        client: Option<ActorPath>,  // client don't need it when receiving it
        succeeded: bool,
        // do we actually need these?
        term_index: Option<(u64, u64)>
    }

    impl ProposalResp {
        fn failed(proposal: &Proposal) -> ProposalResp {
            ProposalResp {
                id: proposal.id,
                client: Some(proposal.client.clone()),
                succeeded: false,
                term_index: None
            }
        }

        fn succeeded_normal(proposal: &Proposal, term: u64, index: u64) -> ProposalResp {
            ProposalResp {
                id: proposal.id,
                client: Some(proposal.client.clone()),
                succeeded: true,
                term_index: Some((term, index))
            }
        }

        fn succeeded_configchange(client: ActorPath) -> ProposalResp {
            ProposalResp {
                id: CONFICCHANGE_ID,
                client: Some(client),
                succeeded: true,
                term_index: None
            }
        }
    }

    const PROPOSAL_ID: u8 = 0;
    const PROPOSALRESP_ID: u8 = 1;
    const RAFT_MSG_ID: u8 = 2;

    const PROPOSAL_FAILED: u8 = 0;
    const PROPOSAL_SUCCESS: u8 = 1;

    struct RaftSer;

    impl Serialiser<CommunicatorMsg> for RaftSer {
        fn ser_id(&self) -> SerId {
            serialiser_ids::RAFT_ID
        }

        fn serialise(&self, enm: &CommunicatorMsg, buf: &mut dyn BufMut) -> Result<(), SerError> {
            match enm {
                CommunicatorMsg::TikvRaftMsg(rm) => {
                    buf.put_u8(RAFT_MSG_ID);
                    let bytes: Vec<u8> = rm.write_to_bytes().expect("Protobuf failed to serialise TikvRaftMsg");
                    buf.put_slice(&bytes);
                },
                CommunicatorMsg::Proposal(p) => {
                    buf.put_u8(PROPOSAL_ID);
                    buf.put_u64_be(p.id);
                    match &p.conf_change {
                        Some(cf) => {
                            let cf_bytes: Vec<u8> = cf.write_to_bytes().expect("Protobuf failed to serialise ConfigChange");
                            let cf_length = cf_bytes.len() as u64;
                            buf.put_u64_be(cf_length);
                            buf.put_slice(&cf_bytes);
                        },
                        None => {
                            buf.put_u64_be(0);
                        }
                    }
                    p.client.serialise(buf)?;
                },
                CommunicatorMsg::ProposalResp(pr) => {
                    buf.put_u8(PROPOSALRESP_ID);
                    buf.put_u64_be(pr.id);
                    if pr.succeeded {
                        buf.put_u8(PROPOSAL_SUCCESS);
                        let (term, index) = pr.term_index.unwrap();
                        buf.put_u64_be(term);
                        buf.put_u64_be(index);
                    } else {
                        buf.put_u8(PROPOSAL_FAILED);
                    }
                }
            }
            Ok(())
        }
    }

    impl Deserialiser<CommunicatorMsg> for RaftSer {
        const SER_ID: u64 = serialiser_ids::RAFT_ID;

        fn deserialise(buf: &mut dyn Buf) -> Result<CommunicatorMsg, SerError> {
            match buf.get_u8(){
                RAFT_MSG_ID => {
                    let bytes = buf.bytes();
                    let msg: TikvRaftMsg = parse_from_bytes::<TikvRaftMsg>(bytes).expect("Protobuf failed to deserialise TikvRaftMsg");
                    Ok(CommunicatorMsg::TikvRaftMsg(msg))
                },
                PROPOSAL_ID => {
                    let id = buf.get_u64_be();
                    let n = buf.get_u64_be();
                    let conf_change = if n > 0 {
                        let mut cf_bytes: Vec<u8> = vec![0; n as usize];
                        buf.copy_to_slice(&mut cf_bytes);
                        let cc = parse_from_bytes::<ConfChange>(&cf_bytes).expect("Protobuf failed to serialise ConfigChange");
                        Some(cc)
                    } else {
                        None
                    };
                    let client = ActorPath::deserialise(buf)?;
                    let proposal = Proposal {
                        id,
                        client,
                        conf_change
                    };
                    Ok(CommunicatorMsg::Proposal(proposal))
                },
                PROPOSALRESP_ID => {
                    let id = buf.get_u64_be();
                    if buf.get_u8() > 0 {
                        let succeeded = true;
                        let term = buf.get_u64_be();
                        let index = buf.get_u64_be();
                        let pr = ProposalResp {
                            id,
                            succeeded,
                            client: None,
                            term_index: Some((term, index))
                        };
                        Ok(CommunicatorMsg::ProposalResp(pr))
                    } else {
                        let succeeded = false;
                        let pr = ProposalResp {
                            id,
                            succeeded,
                            client: None,
                            term_index: None
                        };
                        Ok(CommunicatorMsg::ProposalResp(pr))
                    }
                },
                _ => {
                    Err(SerError::InvalidType(
                        "Found unkown id but expected RaftMsg, Proposal or ProposalResp".into(),
                    ))
                }
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use std::str::FromStr;

        #[test]
        fn raft_ser_test() {
            use super::*;

            use tikv_raft::prelude::{MessageType, Entry, EntryType, ConfChange, ConfChangeType, Message as TikvRaftMsg};
            use protobuf::RepeatedField;
            /*** RaftMsg ***/
            let from: u64 = 1;
            let to: u64 = 2;
            let term: u64 = 3;
            let index:u64 = 4;
            let msg_type: MessageType = MessageType::MsgPropose;
            let mut entry = Entry::new();
            entry.set_term(term);
            entry.set_index(index);
            entry.set_entry_type(EntryType::EntryNormal);
            let entries: RepeatedField<Entry> = RepeatedField::from_vec(vec![entry]);

            let mut m = TikvRaftMsg::new();
            m.set_from(from);
            m.set_to(to);
            m.set_msg_type(msg_type);
            m.set_entries(entries);

            let mut bytes: Vec<u8> = vec![];
            if RaftSer.serialise(&CommunicatorMsg::TikvRaftMsg(m.clone()), &mut bytes).is_err(){panic!("Failed to serialise TikvRaftMsg")};
            let mut buf = bytes.into_buf();
            match RaftSer::deserialise(&mut buf) {
                Ok(des) => {
                    match des {
                        CommunicatorMsg::TikvRaftMsg(payload) => {
                            let des_from = payload.get_from();
                            let des_to = payload.get_to();
                            let des_msg_type = payload.get_msg_type();
                            let des_entries = payload.get_entries();
                            assert_eq!(from, des_from);
                            assert_eq!(to, des_to);
                            assert_eq!(msg_type, des_msg_type);
                            assert_eq!(m.get_entries(), des_entries);
                            assert_eq!(m, payload);
                            println!("Ser/Des RaftMsg passed");
                        },
                        _ => panic!("Deserialised message should be RaftMsg")
                    }
                },
                _ => panic!("Failed to deserialise RaftMsg")
            }
            /*** Proposal ***/
            let client = ActorPath::from_str("local://127.0.0.1:0/test_actor").expect("Failed to create test actorpath");
            let mut b: Vec<u8> = vec![];
            let id: u64 = 12;
            let configchange_id: u64 = 31;
            let start_index: u64 = 47;
            let change_type = ConfChangeType::AddNode;
            let mut cc = ConfChange::new();
            cc.set_id(configchange_id);
            cc.set_change_type(change_type);
            cc.set_start_index(start_index);
            let p = Proposal::conf_change(id, &client, &cc);
            if RaftSer.serialise(&CommunicatorMsg::Proposal(p), &mut b).is_err() {panic!("Failed to serialise Proposal")};
            match RaftSer::deserialise(&mut b.into_buf()){
                Ok(c) => {
                    match c {
                        CommunicatorMsg::Proposal(p) => {
                            let des_id = p.id;
                            let des_client = p.client;
                            match p.conf_change {
                                Some(cc) => {
                                    let cc_id = cc.get_id();
                                    let cc_change_type = cc.get_change_type();
                                    let cc_start_index = cc.get_start_index();
                                    assert_eq!(id, des_id);
                                    assert_eq!(client, des_client);
                                    assert_eq!(configchange_id, cc_id);
                                    assert_eq!(start_index, cc_start_index);
                                    assert_eq!(change_type, cc_change_type);
                                    println!("Ser/Des Proposal passed");
                                }
                                _ => panic!("ConfChange should not be None")
                            }
                        }
                        _ => panic!("Deserialised message should be Proposal")
                    }
                }
                _ => panic!("Failed to deserialise Proposal")
            }
            /*** ProposalResp ***/
            let succeeded = true;
            let pr = ProposalResp {
                id,
                client: None,
                succeeded,
                term_index: Some((term, index))
            };
            let mut b1: Vec<u8> = vec![];
            if RaftSer.serialise(&CommunicatorMsg::ProposalResp(pr), &mut b1).is_err(){panic!("Failed to serailise ProposalResp")};
            match RaftSer::deserialise(&mut b1.into_buf()){
                Ok(cm) => {
                    match cm {
                        CommunicatorMsg::ProposalResp(pr) => {
                            let des_id = pr.id;
                            let des_succeeded = pr.succeeded;
                            let des_term_index = pr.term_index;
                            let des_client = pr.client;
                            assert_eq!(id, des_id);
                            assert_eq!(None, des_client);
                            assert_eq!(succeeded, des_succeeded);
                            assert_eq!(Some((term, index)), des_term_index);
                            println!("Ser/Des ProposalResp passed");
                        }
                        _ => panic!("Deserialised message should be ProposalResp")
                    }
                }
                _ => panic!("Failed to deserialise ProposalResp")
            }
        }
    }
}

