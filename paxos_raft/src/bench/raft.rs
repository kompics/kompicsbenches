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
use std::sync::Arc;
use synchronoise::CountdownEvent;

const CONFICCHANGE_ID: u64 = 0; // use 0 for configuration change

trait RaftStorage: tikv_raft::storage::Storage {
    // TODO
//    fn read(&self) -> [tikv_raft::eraftpb::Entry];
//    fn write(&self, entries: &[tikv_raft::eraftpb::Entry]) -> bool;
    fn append_log(&self, entries: &[tikv_raft::eraftpb::Entry]) -> Result<(), tikv_raft::Error>;

}

impl RaftStorage for MemStorage {
    fn append_log(&self, entries: &[tikv_raft::eraftpb::Entry]) -> Result<(), tikv_raft::Error> {
        self.wl().append(entries)
    }
}

pub mod raft {
    // TODO add parameter in protoc to use memory or disk
    use super::*;
    use protobuf::reflect::ProtobufValue;

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
        fn handle(&mut self, event: <ControlPort as Port>::Request) -> () {
            match event {
                ControlEvent::Start => println!("Hello from Communicator"),
                _ => {},
            }
        }
    }

    impl Provide<MessagingPort> for Communicator {
        fn handle(&mut self, msg: CommunicatorMsg) {
            match &msg {
                CommunicatorMsg::TikvRaftMsg(rm) => {
                    let receiver = self.peers.get(&rm.get_to()).expect(&format!("Could not find actorpath for id={}", &rm.get_to()));
                    receiver.tell((msg.to_owned(), RaftSer), self);
                },
                CommunicatorMsg::ProposalResp(pr) => {
                    let receiver = pr.client.as_ref().expect("No client actorpath provided in ProposalResp");
                    receiver.tell((msg.to_owned(), RaftSer), self);
                },
                CommunicatorMsg::ProposalForward(pf) => {
                    let receiver = self.peers.get(&pf.leader_id).expect("Could not find actorpath to leader in ProposalForward");
                    receiver.tell((CommunicatorMsg::Proposal(pf.proposal.to_owned()), RaftSer), self);
                }
                _ => debug!(self.ctx.log(), "Communicator should not receive proposal from RaftComp...")
            }
        }
    }

    impl Actor for Communicator {
        type Message = Init;

        fn receive_local(&mut self, msg: Init) -> () {
            self.peers = msg.peers;
            let ctr = match &msg.id {
                1 => {
                    let num_nodes = self.peers.len() as u64;
                    CreateTikvRaft{ id: msg.id, num_nodes: Some(num_nodes)}
                }

                _ => CreateTikvRaft{ id: msg.id, num_nodes: None}
            };
            self.raft_port.trigger(RaftCompMsg::CreateTikvRaft(ctr));
        }

        fn receive_network(&mut self, m: NetMessage) -> () {
            let client = m.sender().clone();
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
    struct RaftComp<S: RaftStorage + std::marker::Send + std::clone::Clone + 'static> where S: std::marker::Send{
        ctx: ComponentContext<Self>,
        raft_node: Option<RawNode<S>>,
        communication_port: RequiredPort<MessagingPort, Self>,
        reconfig_client: Option<ActorPath>,
        config: Config,
        storage: S,
    }

    impl<S: RaftStorage + std::marker::Send + std::clone::Clone + 'static> Provide<ControlPort> for RaftComp<S>{
        fn handle(&mut self, event: ControlEvent) -> () {
            // ignore
        }
    }

    impl<S: RaftStorage + std::marker::Send + std::clone::Clone + 'static> Require<MessagingPort> for RaftComp<S> {
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
                    else {
                        let leader_id = raft_node.raft.leader_id;
                        if leader_id > 0 {
                            debug!(self.ctx.log(), "{}", format!("Forwarding proposal to leader_id: {}", &leader_id));
                            let pf = ProposalForward::with(leader_id, p);
                            self.communication_port.trigger(CommunicatorMsg::ProposalForward(pf));
                        } else {
                            // no leader... let client node config change failed
                            let pr = ProposalResp::failed(p);
                            self.communication_port.trigger(CommunicatorMsg::ProposalResp(pr));
                        }
                    }
                }
                RaftCompMsg::CreateTikvRaft(ctr) => {
                    self.config.id = ctr.id;
                    self.raft_node = Some(RawNode::new(&self.config, self.storage.clone()).expect("Failed to create TikvRaftNode"));
//                    info!(self.ctx.log(), "Created RawNode {}", &ctr.id);
                    if self.config.id == 1 {    // leader
                        let raft_node = self.raft_node.as_mut().unwrap();
                        raft_node.raft.become_candidate();
                        raft_node.raft.become_leader();
//                        self.add_all_nodes(ctr.num_nodes.expect("No num_nodes given to leader"));
                    }
                    self.start_timers();
                }
            }
        }
    }

    impl<S: RaftStorage + std::marker::Send + std::clone::Clone + 'static> RaftComp<S> {
        fn with(config: Config, storage: S) -> RaftComp<S> { // TODO add experiment params
            RaftComp {
                ctx: ComponentContext::new(),
                raft_node: None,
                communication_port: RequiredPort::new(),
                reconfig_client: None,
                config,
                storage
            }
        }

        fn add_all_nodes(&mut self, num_nodes: u64){
            let raft_node = self.raft_node.as_mut().unwrap();
            for i in 1..num_nodes+1 {
                raft_node.raft.add_node(i).expect(&format!("Failed to add node {}", i));
            }

        }

        fn start_timers(&mut self){
            let delay = Duration::from_millis(0);
            let on_ready_uuid = uuid::Uuid::new_v4();
            let tick_uuid = uuid::Uuid::new_v4();
            // give new reference to self to make compiler happy
            self.schedule_periodic(delay, Duration::from_millis(1), move |c, on_ready_uuid| c.on_ready());
            self.schedule_periodic(delay, Duration::from_millis(100), move |rc, tick_uuid| rc.tick());
        }

        fn tick(&mut self) {
            self.raft_node.as_mut()
                .expect("TikvRaftNode not initialized in RaftComp")
                .tick();
        }

        // Step a raft message, initialize the raft if need.
        fn step(&mut self, msg: TikvRaftMsg) {
//            info!(self.ctx.log(), "RaftComp stepping msg {:?}", &msg.get_msg_type());
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
                            error!(self.ctx.log(), "Cannot propose due to tikv serialization failure...");
                            // Propose failed, don't forget to respond to the client.
                            let pr = ProposalResp::failed(proposal.clone());
                            self.communication_port.trigger(CommunicatorMsg::ProposalResp(pr));
                        }
                    }

                }
            }
            let last_index2 = raft_node.raft.raft_log.last_index() + 1;
            if last_index2 == last_index1 {
                // Propose failed, don't forget to respond to the client.
                let pr = ProposalResp::failed(proposal);
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
            if let Err(e) = store.append_log(ready.entries()) {
                eprintln!("persist raft log fail: {:?}, need to retry or panic", e);
                return;
            }

            // TODO Apply the snapshot. It's necessary because in `RawNode::advance` we stabilize the snapshot.
            /* if *ready.snapshot() != Snapshot::default() {
                let s = ready.snapshot().clone();
                /*if let Err(e) = store.wl().apply_snapshot(s) {
                    eprintln!("apply snapshot fail: {:?}, need to retry or panic", e);
                    return;
                }*/
            }*/

            // Send out the messages come from the node.
            for msg in ready.messages.drain(..) {
//                info!(self.ctx.log(), "Communicator sending some message...");
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
                        // TODO
                        //store.wl().set_conf_state(cs, None);
                        if raft_node.raft.state == StateRole::Leader && self.reconfig_client.is_some() {
                            let client = self.reconfig_client.clone().expect("Should have cached Reconfiguration client");
                            let pr = ProposalResp::succeeded_configchange(client);
                            self.communication_port.trigger(CommunicatorMsg::ProposalResp(pr));
                        }
                    } else {
                        // For normal proposals, extract the key-value pair and then
                        // insert them into the kv engine.
                        let des_proposal = Proposal::deserialize_normal(&entry.data);
                        // TODO write to disk for LogCabin comparison?
                        if raft_node.raft.state == StateRole::Leader {
                            let pr = ProposalResp::succeeded_normal(des_proposal, entry.get_term(), entry.get_index());
                            self.communication_port.trigger(CommunicatorMsg::ProposalResp(pr));
                        }
                    }

                }
                if let Some(last_committed) = committed_entries.last() {
                    //TODO
                    /*let mut s = store.wl();
                    s.mut_hard_state().commit = last_committed.index;
                    s.mut_hard_state().term = last_committed.term;*/
                }
            }
            // Call `RawNode::advance` interface to update position flags in the raft.
            raft_node.advance(ready);
        }
    }


    impl<S: RaftStorage + std::marker::Send + std::clone::Clone + 'static> Actor for RaftComp<S> {
        type Message = ();

        fn receive_local(&mut self, _msg: Self::Message) -> () {
            unimplemented!()
        }

        fn receive_network(&mut self, _msg: NetMessage) -> () {
            unimplemented!()
        }
    }

    #[derive(ComponentDefinition)]
    struct Client {
        ctx: ComponentContext<Self>,
        finished_promise: KPromise<Vec<u64>>,
        num_proposals: u64,
        raft_nodes: HashMap<u64, ActorPath>,
        decided_sequence: Vec<u64>,
    }

    impl Client {
        fn with(finished_promise: KPromise<Vec<u64>>, num_proposals: u64, raft_nodes: HashMap<u64, ActorPath>) -> Client {
            Client {
                ctx: ComponentContext::new(),
                finished_promise,
                num_proposals,
                raft_nodes,
                decided_sequence: Vec::with_capacity(num_proposals as usize)
            }
        }
    }

    impl Provide<ControlPort> for Client {
        fn handle(&mut self, event: <ControlPort as Port>::Request) -> () {
            match event {
                ControlEvent::Start => {
                    for i in 0..self.num_proposals {
                        let ap = self.ctx.actor_path();
                        let p = Proposal::normal(i, ap);
                        let raft_node = self.raft_nodes.get(&2).expect("Could not find actorpath to raft node!");
//                        let ap = self.ctx.actor_path();
                        raft_node.tell((CommunicatorMsg::Proposal(p), RaftSer), self);
                    }
                }
                _ => {}, //ignore
            }
        }
    }

    impl Actor for Client {
        type Message = ();

        fn receive_local(&mut self, msg: Self::Message) -> () {
            // ignore
        }

        fn receive_network(&mut self, msg: NetMessage) -> () {
            match_deser!{msg; {
                cm: CommunicatorMsg [RaftSer] => {
                    match cm {
                        CommunicatorMsg::ProposalResp(pr) => {
                            if pr.succeeded {
                                info!(self.ctx.log(), "Succesful proposal!");
                                self.decided_sequence.push(pr.id);
                                if (self.decided_sequence.len() as u64) == self.num_proposals {
                                    info!(self.ctx.log(), "Got all results!");
                                    info!(self.ctx.log(), "{:?}", self.decided_sequence);
                                    self.finished_promise
                                        .to_owned()
                                        .fulfill(self.decided_sequence.clone())
                                        .expect("Failed to fulfill finished promise");
                                }
                            }
                            else {
                                error!(self.ctx.log(), "Client proposal failed...")
                            }
                        },
                        _ => error!(self.ctx.log(), "Client received unexpected msg"),
                    }
                },

                !Err(e) => error!(self.ctx.log(), "Failed to deserialise msg in Client"),
            }
            }
        }
    }

    /*** All messages and their serializers ***/
    #[derive(Clone, Debug)]
    struct Init {   // TODO: CommunicatorMsg::Init
    id: u64,
        peers: HashMap<u64, ActorPath>,
    }

    #[derive(Clone, Debug)]
    struct CreateTikvRaft {
        id: u64,
        num_nodes: Option<u64>
    }

    #[derive(Clone, Debug)]
    enum RaftCompMsg {
        CreateTikvRaft(CreateTikvRaft),
        TikvRaftMsg(TikvRaftMsg),
        Proposal(Proposal)
    }

    #[derive(Clone, Debug)]
    enum CommunicatorMsg {
        TikvRaftMsg(TikvRaftMsg),
        Proposal(Proposal),
        ProposalResp(ProposalResp),
        ProposalForward(ProposalForward)
    }

    #[derive(Clone, Debug)]
    struct Proposal {
        id: u64,
        client: ActorPath,
        conf_change: Option<ConfChange>, // conf change.
    }

    impl Proposal {
        fn conf_change(id: u64, client: ActorPath, cc: &ConfChange) -> Proposal {
            let proposal = Proposal {
                id,
                client,
                conf_change: Some(cc.clone()),
            };
            proposal
        }

        fn normal(id: u64, client: ActorPath) -> Proposal {
            let proposal = Proposal {
                id,
                client,
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
            Proposal::normal(id, client)
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
        fn failed(proposal: Proposal) -> ProposalResp {
            ProposalResp {
                id: proposal.id,
                client: Some(proposal.client),
                succeeded: false,
                term_index: None
            }
        }

        fn succeeded_normal(proposal: Proposal, term: u64, index: u64) -> ProposalResp {
            ProposalResp {
                id: proposal.id,
                client: Some(proposal.client),
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

    #[derive(Clone, Debug)]
    struct ProposalForward {
        leader_id: u64,
        proposal: Proposal
    }

    impl ProposalForward {
        fn with(leader_id: u64, proposal: Proposal) -> ProposalForward {
            ProposalForward {
                leader_id,
                proposal
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

        fn size_hint(&self) -> Option<usize> {
            Some(100) // TODO: Set it dynamically? 33 is for the largest message(Value)
        }

        fn serialise(&self, enm: &CommunicatorMsg, buf: &mut dyn BufMut) -> Result<(), SerError> {
            match enm {
                CommunicatorMsg::TikvRaftMsg(rm) => {
                    buf.put_u8(RAFT_MSG_ID);
                    let bytes: Vec<u8> = rm.write_to_bytes().expect("Protobuf failed to serialise TikvRaftMsg");
                    buf.put_slice(&bytes);
                    Ok(())
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
                    p.client.serialise(buf).expect("Failed to serialise actorpath");
                    Ok(())
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
                    Ok(())
                },
                _ => {
                    Err(SerError::InvalidType("Tried to serialise unknown type".into()))
                }
            }
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
                    let client = ActorPath::deserialise(buf).expect("Failed to deserialise actorpath");
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
        use super::*;

        fn example_config() -> Config {
            Config {
                election_tick: 10,
                heartbeat_tick: 3,
                ..Default::default()
            }
        }

        #[test]
        fn kompact_raft_ser_test() {
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
            let p = Proposal::conf_change(id, client.clone(), &cc);
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

        #[test]
        fn tikv_raft_ser_test() {
            let client = ActorPath::from_str("local://127.0.0.1:0/test_actor").expect("Failed to create test actorpath");
            let id = 346;
            let proposal = Proposal::normal(id, client);
            let bytes = proposal.serialize_normal().expect("Failed to tikv serialize proposal");
            let des_proposal = Proposal::deserialize_normal(&bytes);
            assert_eq!(proposal.id, des_proposal.id);
            assert_eq!(proposal.client, des_proposal.client);
            assert_eq!(proposal.conf_change, des_proposal.conf_change);
        }

        #[test]
        fn kompact_raft_test(){
            use super::*;
            use std::{thread, time};

            let mut systems: Vec<KompactSystem> = Vec::new();
            let mut peers: HashMap<u64, ActorPath> = HashMap::new();
            let mut actor_refs: Vec<ActorRef<Init>> = Vec::new();
            let n: u64 = 3;
            let num_proposals = 10;

            for i in 1..n+1 {
                let system =
                    kompact_benchmarks::kompact_system_provider::global().new_remote_system_with_threads(format!("raft{}", i), 4);

                /*** Setup RaftComp ***/
                let (raft_comp, unique_reg_f) = system.create_and_register(|| {
                    RaftComp::with(example_config(), MemStorage::new_with_conf_state((vec![1,2,3], vec![])))
                });
                let raft_comp_f = system.start_notify(&raft_comp);
                raft_comp_f
                    .wait_timeout(Duration::from_millis(1000))
                    .expect("RaftComp never started!");
                unique_reg_f.wait_expect(
                    Duration::from_millis(1000),
                    "RaftComp failed to register!",
                );

                /*** Setup communicator ***/
                let (communicator, unique_reg_f) =
                    system.create_and_register(|| { Communicator::new() });

                let named_reg_f = system.register_by_alias(
                    &communicator,
                    format!("communicator{}", i),
                );

                unique_reg_f
                    .wait_timeout(Duration::from_millis(1000))
                    .expect("Communicator never registered!")
                    .expect("Communicator to register!");

                named_reg_f.wait_expect(
                    Duration::from_millis(1000),
                    "Communicator failed to register!",
                );

                let communicator_f = system.start_notify(&communicator);
                communicator_f
                    .wait_timeout(Duration::from_millis(1000))
                    .expect("Communicator never started!");

                biconnect_components::<MessagingPort, _, _>(&communicator, &raft_comp)
                    .expect("Could not connect components!");

                /*** Add self to peers map ***/
                let self_path = ActorPath::Named(NamedPath::with_system(
                    system.system_path(),
                    vec![format!("communicator{}", i).into()],
                ));
                systems.push(system);
                peers.insert(i, self_path);
                actor_refs.push(communicator.actor_ref());
            }
            for (id, actor_ref) in actor_refs.iter().enumerate() {
                actor_ref.tell(Init{ id: (id+1) as u64, peers: peers.clone()});
            }
            thread::sleep(Duration::from_secs(1));
            let (p, f) = kpromise::<Vec<u64>>();
            let (client, unique_reg_f) = systems[0].create_and_register( || {
                Client::with(p, num_proposals, peers.clone())
            });
            unique_reg_f.wait_expect(
                Duration::from_millis(1000),
                "Client failed to register!"
            );
            let client_f = systems[0].start_notify(&client);
            client_f
                .wait_timeout(Duration::from_millis(1000))
                .expect("Client never started!");
            let decided_sequence = f.wait_timeout(Duration::from_secs(10)).expect("Failed to get results");
            for system in systems {
                system
                    .shutdown()
                    .expect("Kompact didn't shut down properly");
            }
            assert_eq!(num_proposals, decided_sequence.len() as u64);
//            println!("{:?}", decided_sequence);
            for i in 0..num_proposals {
                let mut iter = decided_sequence.iter();
                let found = iter.find(|&&x| x == i).is_some();
                assert_eq!(true, found);
            }
        }
    }
}

