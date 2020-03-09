extern crate raft as tikv_raft;

use kompact::prelude::*;
//use kompact_benchmarks::partitioning_actor::*;
use std::collections::HashMap;
use tikv_raft::{prelude::*, StateRole, prelude::Message as TikvRaftMsg, storage::MemStorage};
use protobuf::{Message as PbMessage, parse_from_bytes};
use super::super::serialiser_ids as serialiser_ids;
use std::{time::Duration, path::PathBuf, fs::OpenOptions, io::Write, ops::Range, convert::TryInto, fs::File, mem::size_of, sync::Arc};
use uuid::Uuid;
use self::tikv_raft::Error;
use memmap::MmapMut;
use super::super::raft_storage::{RaftStorage, DiskStorage};

const CONFIGCHANGE_ID: u64 = 0; // use 0 for configuration change

pub mod raft {
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
        cached_client: Option<ActorPath>    // cached client to send SequenceResp to
    }

    impl Communicator {
        fn new() -> Communicator {
            Communicator {
                ctx: ComponentContext::new(),
                raft_port: ProvidedPort::new(),
                peers: HashMap::new(),
                cached_client: None
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
                },
                CommunicatorMsg::SequenceResp(_) => {
                    let receiver = self.cached_client.as_ref().expect("No cached client found for SequenceResp");
                    receiver.tell((msg.to_owned(), RaftSer), self);
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
                    CreateTikvRaft{ id: msg.id }
                }

                _ => CreateTikvRaft{ id: msg.id }
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
                        CommunicatorMsg::SequenceReq(sr) => {
                            self.cached_client = Some(client);
                            self.raft_port.trigger(RaftCompMsg::SequenceReq(sr));
                        }
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
        fn handle(&mut self, _event: ControlEvent) -> () {
            // ignore
        }
    }

    impl<S: RaftStorage + std::marker::Send + std::clone::Clone + 'static> Require<MessagingPort> for RaftComp<S> {
        fn handle(&mut self, msg: RaftCompMsg) -> () {
            match msg {
                RaftCompMsg::CreateTikvRaft(ctr) => {
                    self.config.id = ctr.id;
                    self.raft_node = Some(RawNode::new(&self.config, self.storage.clone()).expect("Failed to create TikvRaftNode"));
                    if self.config.id == 1 {    // leader
                        let raft_node = self.raft_node.as_mut().unwrap();
                        raft_node.raft.become_candidate();
                        raft_node.raft.become_leader();
                    }
                    self.start_timers();
                }
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
                            let pf = ProposalForward::with(leader_id, p);
                            self.communication_port.trigger(CommunicatorMsg::ProposalForward(pf));
                        } else {
                            // no leader... let client node proposal failed
                            let pr = ProposalResp::failed(p);
                            self.communication_port.trigger(CommunicatorMsg::ProposalResp(pr));
                        }
                    }
                }
                RaftCompMsg::SequenceReq(_) => {
                    let raft_node = self.raft_node.as_mut().expect("TikvRaftNode not initialized in RaftComp");
                    let raft_entries: Vec<Entry> = raft_node.raft.raft_log.all_entries();
                    let mut sequence: Vec<u64> = Vec::new();
                    for entry in raft_entries {
                        if entry.get_entry_type() == EntryType::EntryNormal && (&entry.data.len() > &0) {
                            let value = Proposal::deserialize_normal(&entry.data);
                            sequence.push(value.id)
                        }
                    }
                    let sr = SequenceResp{ node_id: self.config.id, sequence };
                    self.communication_port.trigger(CommunicatorMsg::SequenceResp(sr));
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
                storage,
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
            let mut store = raft_node.raft.raft_log.store.clone();

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
                        let change_type = cc.get_change_type();
                        match &change_type {
                            ConfChangeType::AddNode => raft_node.raft.add_node(node_id.clone()).unwrap(),
                            ConfChangeType::RemoveNode => raft_node.raft.remove_node(node_id.clone()).unwrap(),
                            ConfChangeType::AddLearnerNode => raft_node.raft.add_learner(node_id.clone()).unwrap(),
                            ConfChangeType::BeginMembershipChange
                            | ConfChangeType::FinalizeMembershipChange => unimplemented!(),
                        }
                        // TODO
                        /*let cs = ConfState::from(raft_node.raft.prs().configuration().clone());
                        store.wl().set_conf_state(cs, None);*/
                        if raft_node.raft.state == StateRole::Leader && self.reconfig_client.is_some() {
                            let client = self.reconfig_client.clone().unwrap();
                            let pr = ProposalResp::succeeded_configchange(client, node_id, change_type, entry.get_term(), entry.get_index());
                            self.communication_port.trigger(CommunicatorMsg::ProposalResp(pr));
                        }
                    } else {
                        // For normal proposals, reply to client
                        let des_proposal = Proposal::deserialize_normal(&entry.data);
                        if raft_node.raft.state == StateRole::Leader {
                            let pr = ProposalResp::succeeded_normal(des_proposal, entry.get_term(), entry.get_index());
                            self.communication_port.trigger(CommunicatorMsg::ProposalResp(pr));
                        }
                    }

                }
                if let Some(last_committed) = committed_entries.last() {
                    store.set_hard_state(last_committed.index, last_committed.term).expect("Failed to set hardstate");
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
        finished_promise: KPromise<HashMap<u64, Vec<u64>>>,
        num_proposals: u64,
        raft_nodes: HashMap<u64, ActorPath>,
        test_results: HashMap<u64, Vec<u64>>,
        test_reconfig: Option<(Vec<u64>, Vec<u64>)>,
        query_node: u64
    }

    impl Client {
        fn with(finished_promise: KPromise<HashMap<u64, Vec<u64>>>,
                num_proposals: u64,
                raft_nodes: HashMap<u64, ActorPath>,
                test_reconfig: Option<(Vec<u64>, Vec<u64>)>,
                query_node: u64
        ) -> Client {
            Client {
                ctx: ComponentContext::new(),
                finished_promise,
                num_proposals,
                raft_nodes,
                test_results: HashMap::new(),
                test_reconfig,
                query_node
            }
        }

        fn propose_remove_node(&self, remove_node: u64) -> () {
            let mut conf_change = ConfChange::default();
            conf_change.node_id = remove_node;
            conf_change.set_change_type(ConfChangeType::RemoveNode);
            let p = Proposal::conf_change(remove_node, self.ctx.actor_path(), &conf_change);
            let raft_node = self.raft_nodes.get(&remove_node).expect("Could not find actorpath to raft node!");
            raft_node.tell((CommunicatorMsg::Proposal(p), RaftSer), self);
        }

        fn propose_add_node(&self, add_node: u64) {
            let mut conf_change = ConfChange::default();
            conf_change.node_id = add_node;
            conf_change.set_change_type(ConfChangeType::AddNode);
            let p = Proposal::conf_change(add_node, self.ctx.actor_path(), &conf_change);
            let raft_node = self.raft_nodes.get(&self.query_node).expect("Could not find actorpath to raft node!");
            raft_node.tell((CommunicatorMsg::Proposal(p), RaftSer), self);
        }

    }

    impl Provide<ControlPort> for Client {
        fn handle(&mut self, event: <ControlPort as Port>::Request) -> () {
            match event {
                ControlEvent::Start => {
                    self.test_results.insert(0, Vec::new());
                    for i in 0..self.num_proposals {
                        let ap = self.ctx.actor_path();
                        let p = Proposal::normal(i, ap);
                        let raft_node = self.raft_nodes.get(&self.query_node).expect("Could not find actorpath to raft node!");
                        raft_node.tell((CommunicatorMsg::Proposal(p), RaftSer), self);
                    }
                }
                _ => {}, //ignore
            }
        }
    }

    impl Actor for Client {
        type Message = ();

        fn receive_local(&mut self, _msg: Self::Message) -> () {
            // ignore
        }

        fn receive_network(&mut self, msg: NetMessage) -> () {
            use std::thread;

            match_deser!{msg; {
                cm: CommunicatorMsg [RaftSer] => {
                    match cm {
                        CommunicatorMsg::ProposalResp(pr) => {
                            match &pr.conf_change {
                                Some((node_id, change_type)) => {
                                    match change_type {
                                        ConfChangeType::AddNode => {
                                            info!(self.ctx.log(), "{}", format!("Successfully added node {}", node_id));
                                            let test_reconfig = self.test_reconfig.as_mut().expect("No reconfiguration found");
                                            match test_reconfig.1.pop() {
                                                Some(remove_node) => self.propose_remove_node(remove_node),
                                                None => { // Reconfiguration completed, try proposing normally again
                                                    thread::sleep(Duration::from_secs(2));
                                                    let ap = self.ctx.actor_path();
                                                    let p = Proposal::normal(self.num_proposals, ap);
                                                    let raft_node = self.raft_nodes.get(&self.query_node).expect("Could not find actorpath to raft node!");
                                                    raft_node.tell((CommunicatorMsg::Proposal(p), RaftSer), self);
                                                }
                                            }
                                        }
                                        ConfChangeType::RemoveNode => {
                                            info!(self.ctx.log(), "{}", format!("Successfully removed node {}", node_id));
                                            let add_node = self.test_reconfig
                                                                  .as_mut()
                                                                  .expect("No reconfiguration found")
                                                                  .0
                                                                  .pop()
                                                                  .expect("Got no node when popping add nodes");
                                            self.propose_add_node(add_node);
                                        }
                                        ConfChangeType::AddLearnerNode => {
                                            error!(self.ctx.log(), "Got unexpected AddLearnerNode response");
                                        }
                                        _ => {
                                            error!(self.ctx.log(), "Got unexpected ConfChangeType")
                                        }
                                    }
                                },
                                None => {   // normal ProposalResp
                                    let decided_sequence = self.test_results.get_mut(&0).unwrap();
                                    decided_sequence.push(pr.id);
                                    if (decided_sequence.len() as u64) == self.num_proposals {
                                        match &self.test_reconfig {
                                            Some(_) => {    // done with all proposals, now start reconfiguration
                                               info!(self.ctx.log(), "Done with all proposals, starting reconfiguration now...");
                                               let remove_node = self.test_reconfig
                                                         .as_mut()
                                                         .expect("No reconfiguration found")
                                                         .1
                                                         .pop()
                                                         .expect("Got no node when popping remove nodes");
                                                self.propose_remove_node(remove_node);
                                            }
                                            None => {
                                                self.finished_promise
                                                    .to_owned()
                                                    .fulfill(self.test_results.clone())
                                                    .expect("Failed to fulfill finished promise");
                                            }
                                        }
                                    } else if (decided_sequence.len() as u64) > self.num_proposals { // normal proposal after reconfig worked
                                        info!(self.ctx.log(), "Normal propose after reconfiguration worked!");
                                        for (_, actorpath) in &self.raft_nodes {  // get sequence of ALL (past or present) nodes
                                            let req = SequenceReq;
                                            actorpath.tell((CommunicatorMsg::SequenceReq(req), RaftSer), self);
                                        }
                                    }
                                }
                            }
                        },
                        CommunicatorMsg::SequenceResp(sr) => {
                            self.test_results.insert(sr.node_id, sr.sequence);
                            if (self.test_results.len() - 1) == self.raft_nodes.len() {    // got all sequences from everybody, we're done
                                self.finished_promise
                                .to_owned()
                                .fulfill(self.test_results.clone())
                                .expect("Failed to fulfill finished promise after getting all sequences");
                            }
                        }
                        _ => error!(self.ctx.log(), "Client received unexpected msg"),
                    }
                },

                !Err(e) => error!(self.ctx.log(), "{}", &format!("Client failed to deserialise msg: {:?}", e)),
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
    }

    #[derive(Clone, Debug)]
    struct SequenceReq;

    #[derive(Clone, Debug)]
    struct SequenceResp {
        node_id: u64,
        sequence: Vec<u64>
    }

    #[derive(Clone, Debug)]
    enum RaftCompMsg {
        CreateTikvRaft(CreateTikvRaft),
        TikvRaftMsg(TikvRaftMsg),
        Proposal(Proposal),
        SequenceReq(SequenceReq),
    }

    #[derive(Clone, Debug)]
    enum CommunicatorMsg {
        TikvRaftMsg(TikvRaftMsg),
        Proposal(Proposal),
        ProposalResp(ProposalResp),
        ProposalForward(ProposalForward),
        SequenceReq(SequenceReq),
        SequenceResp(SequenceResp)
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
        conf_change: Option<(u64, ConfChangeType)>,
        // do we actually need these?
        term_index: Option<(u64, u64)>
    }

    impl ProposalResp {
        fn failed(proposal: Proposal) -> ProposalResp {
            let mut pr = ProposalResp {
                id: proposal.id,
                client: Some(proposal.client),
                succeeded: false,
                term_index: None,
                conf_change: None
            };
            match proposal.conf_change {
                Some(cf) => {
                    let node_id = cf.get_node_id();
                    let change_type = cf.get_change_type();
                    pr.conf_change = Some((node_id, change_type));
                }
                None => {}
            }
            pr
        }

        fn succeeded_normal(proposal: Proposal, term: u64, index: u64) -> ProposalResp {
            ProposalResp {
                id: proposal.id,
                client: Some(proposal.client),
                succeeded: true,
                term_index: Some((term, index)),
                conf_change: None
            }
        }

        fn succeeded_configchange(client: ActorPath, node_id: u64, change_type: ConfChangeType, term: u64, index: u64) -> ProposalResp {
            ProposalResp {
                id: CONFIGCHANGE_ID,
                client: Some(client),
                succeeded: true,
                term_index: Some((term, index)),
                conf_change: Some((node_id, change_type))
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
    const SEQREQ_ID: u8 = 3;
    const SEQRESP_ID: u8 = 4;

    const PROPOSAL_FAILED: u8 = 0;
    const PROPOSAL_SUCCESS: u8 = 1;

    const CHANGETYPE_ADD_NODE: u8 = 0;
    const CHANGETYPE_REMOVE_NODE: u8 = 1;
    const CHANGETYPE_ADD_LEARNER: u8 = 2;

    struct RaftSer;

    impl Serialiser<CommunicatorMsg> for RaftSer {
        fn ser_id(&self) -> SerId {
            serialiser_ids::RAFT_ID
        }

        fn size_hint(&self) -> Option<usize> {
            Some(200) // TODO: Set it dynamically?
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
                    match &pr.conf_change {
                        Some((node_id, change_type)) => {
                            buf.put_u64_be(*node_id);
                            match change_type {
                                ConfChangeType::AddNode => buf.put_u8(CHANGETYPE_ADD_NODE),
                                ConfChangeType::RemoveNode => buf.put_u8(CHANGETYPE_REMOVE_NODE),
                                ConfChangeType::AddLearnerNode => buf.put_u8(CHANGETYPE_ADD_LEARNER),
                                _ => return Err(SerError::InvalidType("Tried to serialise unknown ConfChangeType".into()))
                            }
                        }
                        None => { buf.put_u64_be(0) }
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
                        buf.put_u64_be(*i);
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
                    let succeeded: bool = match buf.get_u8() {
                        0 => false,
                        _ => true,
                    };
                    let term_index = match &succeeded {
                        true => {
                            let term = buf.get_u64_be();
                            let index = buf.get_u64_be();
                            Some((term, index))
                        }
                        false => None
                    };
                    let mut pr = ProposalResp {
                        id,
                        succeeded,
                        client: None,
                        term_index,
                        conf_change: None,
                    };
                    let node_id = buf.get_u64_be();
                    match &node_id {
                        0 => {},
                        _ => {
                            match buf.get_u8() {
                                CHANGETYPE_ADD_NODE => {
                                    pr.conf_change = Some((node_id, ConfChangeType::AddNode));
                                },
                                CHANGETYPE_REMOVE_NODE => {
                                    pr.conf_change = Some((node_id, ConfChangeType::RemoveNode));
                                },
                                CHANGETYPE_ADD_LEARNER => {
                                    pr.conf_change = Some((node_id, ConfChangeType::AddLearnerNode));
                                },
                                _ => {},
                            };
                        }
                    }
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

    #[cfg(test)]
    mod tests {
        use std::str::FromStr;
        use super::*;
        use std::fs::remove_dir_all;

        fn example_config() -> Config {
            Config {
                election_tick: 10,
                heartbeat_tick: 3,
                ..Default::default()
            }
        }

        fn create_raft_nodes(n: u64,
                             systems: &mut Vec<KompactSystem>,
                             peers: &mut HashMap<u64, ActorPath>,
                             communicators: &mut Vec<ActorRef<Init>>,
                             configuration: Vec<u64>,
                             dir: &str)
        {
            for i in 1..n+1 {
                let system =
                    kompact_benchmarks::kompact_system_provider::global().new_remote_system_with_threads(format!("raft{}", i), 4);
                let storage = DiskStorage::new_with_conf_state(&format!("{}{}", dir, i), (configuration.clone(), vec![]));
//                let storage =  MemStorage::new_with_conf_state((configuration.clone(), vec![]));
                /*** Setup RaftComp ***/
                let (raft_comp, unique_reg_f) = system.create_and_register(|| {
                    RaftComp::with(example_config(), storage)
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
                communicators.push(communicator.actor_ref());
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
            let change_type = ConfChangeType::AddNode;
            let pr = ProposalResp {
                id,
                client: None,
                succeeded,
                term_index: Some((term, index)),
                conf_change: Some((id, change_type))
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
                            let des_conf_change = pr.conf_change;
                            assert_eq!(id, des_id);
                            assert_eq!(None, des_client);
                            assert_eq!(succeeded, des_succeeded);
                            assert_eq!(Some((term, index)), des_term_index);
                            assert_eq!(Some((id, change_type)), des_conf_change);
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
        fn kompact_raft_normal_proposal_test(){
            use super::*;
            use std::{thread, time, time::Instant};

            let mut systems: Vec<KompactSystem> = Vec::new();
            let mut peers: HashMap<u64, ActorPath> = HashMap::new();
            let mut communicators: Vec<ActorRef<Init>> = Vec::new();
            let n: u64 = 3;
            let num_proposals = 1000;
            let mut configuration: Vec<u64> = Vec::new();
            for i in 1..n+1 { configuration.push(i) }
            let dir = "normal_test";
            create_raft_nodes(n, &mut systems, &mut peers,&mut communicators, configuration, &format!("{}/{}", dir, "storage_node"));

            for (id, actor_ref) in communicators.iter().enumerate() {
                actor_ref.tell(Init{ id: (id+1) as u64, peers: peers.clone()});
            }
            thread::sleep(Duration::from_secs(1));
            let (p, f) = kpromise::<HashMap<u64, Vec<u64>>>();
            let (client, unique_reg_f) = systems[0].create_and_register( || {
                Client::with(p, num_proposals, peers.clone(), None, 1)
            });
            unique_reg_f.wait_expect(
                Duration::from_millis(1000),
                "Client failed to register!"
            );
//            let now = Instant::now();   // for testing with logcabin
            let client_f = systems[0].start_notify(&client);
            client_f.wait_timeout(Duration::from_millis(1000))
                    .expect("Client never started!");

            let decided_sequence = f.wait_timeout(Duration::from_secs(10))
                                               .expect("Failed to get results")
                                               .get(&0)
                                               .expect("Client's sequence should be in 0...")
                                               .to_owned();

//            let exec_time = now.elapsed().as_millis();
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
            remove_dir_all(dir).expect("Failed to remove test storage files");
            /*let mut f = File::create("results.out").expect("Failed to create results file");
            let s = format!("{} nodes, {} proposals took {} ms", n, num_proposals, exec_time);
            f.write_all(s.as_bytes()).unwrap();
            f.sync_all().unwrap();
            for i in 1..n+1 {
                remove_dir_all(format!("{}{}", &dir, i)).expect("Failed to remove test storage files");
            }*/
        }

        #[test]
        fn kompact_raft_reconfig_test(){
            use super::*;
            use std::{thread, time, time::Instant};

            let mut systems: Vec<KompactSystem> = Vec::new();
            let mut peers: HashMap<u64, ActorPath> = HashMap::new();
            let mut communicators: Vec<ActorRef<Init>> = Vec::new();
            let n: u64 = 5; // use n nodes in total (includes nodes to be added and removed)
            let active_n: u64 = 3;
            let quorum = active_n/2 + 1;
            let num_proposals = 20;
            let add_nodes: Vec<u64> = vec![4, 5];
            let remove_nodes: Vec<u64> = vec![2, 3];
            let reconfig = Some((add_nodes, remove_nodes));
            let dir = "reconfig_test";

            create_raft_nodes(n, &mut systems, &mut peers,&mut communicators, vec![1,2,3], &format!("{}/{}", dir, "storage_node"));

            for (id, actor_ref) in communicators.iter().enumerate() {
                actor_ref.tell(Init{ id: (id+1) as u64, peers: peers.clone()});
            }
            thread::sleep(Duration::from_secs(1));
            let (p, f) = kpromise::<HashMap<u64, Vec<u64>>>();
            let (client, unique_reg_f) = systems[0].create_and_register( || {
                Client::with(p, num_proposals - 1, peers.clone(), reconfig, 1)
            });
            unique_reg_f.wait_expect(
                Duration::from_millis(1000),
                "Client failed to register!"
            );
            let client_f = systems[0].start_notify(&client);
            client_f.wait_timeout(Duration::from_millis(1000))
                .expect("Client never started!");

            let all_sequences = f.wait_timeout(Duration::from_secs(60)).expect("Failed to get results");

            let client_sequence = all_sequences.get(&0).expect("Client's sequence should be in 0...").to_owned();
            for system in systems {
                system
                    .shutdown()
                    .expect("Kompact didn't shut down properly");
            }
            let client_seq_len = client_sequence.len();
            assert_eq!(num_proposals, client_seq_len as u64);
            let mut counter = 0;
            for i in 1..n+1 {
                let sequence = all_sequences.get(&i).unwrap();
//                println!("Node {}: {:?}", i, sequence);
                assert!(client_sequence.starts_with(sequence));
                if sequence.starts_with(&client_sequence) {
                    counter += 1;
                }
            }
            if counter < quorum {
                panic!("Majority should have decided sequence: counter: {}, quorum: {}", counter, quorum);
            }
            remove_dir_all(dir).expect("Failed to remove test storage files");
        }
    }
}