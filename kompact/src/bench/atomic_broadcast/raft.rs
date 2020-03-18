extern crate raft as tikv_raft;

use kompact::prelude::*;
use tikv_raft::{prelude::*, StateRole, prelude::Message as TikvRaftMsg, prelude::Entry};
use protobuf::{Message as PbMessage};
use std::{time::Duration, collections::HashMap};
use crate::partitioning_actor::{Init, InitAck, PartitioningActorSer};
use super::messages::{*, raft::*};
use super::storage::raft::*;

#[derive(Clone, Debug)]
pub enum RaftCompMsg {
    CreateTikvRaft(CreateTikvRaft),
    TikvRaftMsg(RaftMsg),
    Proposal(Proposal),
    SequenceReq(SequenceReq),
}

pub struct MessagingPort;

impl Port for MessagingPort {
    type Indication = RaftCompMsg;
    type Request = CommunicatorMsg;
}

#[derive(ComponentDefinition)]
pub struct Communicator {
    ctx: ComponentContext<Communicator>,
    raft_port: ProvidedPort<MessagingPort, Communicator>,
    peers: HashMap<u64, ActorPath>, // tikv raft node id -> actorpath
    cached_next_iter_metadata: Option<(ActorPath, HashMap<u64, ActorPath>)>,   // (partitioning_actor, peers) for next init_ack
    cached_client: Option<ActorPath>    // cached client to send SequenceResp to
}

impl Communicator {
    pub fn new() -> Communicator {
        Communicator {
            ctx: ComponentContext::new(),
            raft_port: ProvidedPort::new(),
            peers: HashMap::new(),
            cached_next_iter_metadata: None,
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
            CommunicatorMsg::RaftMsg(rm) => {
                let receiver = self.peers.get(&rm.payload.get_to()).expect(&format!("Could not find actorpath for id={}", &rm.payload.get_to()));
                receiver.tell((msg.to_owned(), AtomicBroadcastSer), self);
            },
            CommunicatorMsg::ProposalResp(pr) => {
                let receiver = pr.client.as_ref().expect("No client actorpath provided in ProposalResp");
                receiver.tell((msg.to_owned(), AtomicBroadcastSer), self);
            },
            CommunicatorMsg::ProposalForward(pf) => {
                let receiver = self.peers.get(&pf.leader_id).expect("Could not find actorpath to leader in ProposalForward");
                receiver.tell((CommunicatorMsg::Proposal(pf.proposal.to_owned()), AtomicBroadcastSer), self);
            },
            CommunicatorMsg::SequenceResp(_) => {
                let receiver = self.cached_client.as_ref().expect("No cached client found for SequenceResp");
                receiver.tell((msg.to_owned(), AtomicBroadcastSer), self);
            },
            CommunicatorMsg::InitAck(init_ack) => {
                let meta =  self.cached_next_iter_metadata.take().expect("No next iteration metadata cached");
                let receiver = meta.0;
                let peers = meta.1;
                self.peers = peers;
                receiver.tell((init_ack.to_owned(), PartitioningActorSer), self);
            }
            _ => debug!(self.ctx.log(), "Communicator should not receive proposal from RaftComp...")
        }
    }
}

impl Actor for Communicator {
    type Message = ();

    fn receive_local(&mut self, _msg: Self::Message) -> () {
        unimplemented!();
    }

    fn receive_network(&mut self, m: NetMessage) -> () {
        let sender = m.sender().clone();
        match_deser! {m; {
                init: Init [PartitioningActorSer] => {
                    let iteration_id = init.init_id;
                    let node_id = init.rank as u64;
                    let mut peers = HashMap::new();
                    for (id, actorpath) in init.nodes.into_iter().enumerate() {
                        peers.insert(id as u64 + 1, actorpath);
                    }
                    self.cached_next_iter_metadata = Some((sender, peers));
                    let ctr = CreateTikvRaft{ node_id, iteration_id };
                    self.raft_port.trigger(RaftCompMsg::CreateTikvRaft(ctr));
                },

                comm_msg: CommunicatorMsg [AtomicBroadcastSer] => {
                    match comm_msg {
                        CommunicatorMsg::RaftMsg(rm) => self.raft_port.trigger(RaftCompMsg::TikvRaftMsg(rm)),
                        CommunicatorMsg::Proposal(p) => self.raft_port.trigger(RaftCompMsg::Proposal(p)),
                        CommunicatorMsg::SequenceReq(sr) => {
                            self.cached_client = Some(sender);
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
pub struct RaftComp<S: RaftStorage + std::marker::Send + std::clone::Clone + 'static> where S: std::marker::Send{
    ctx: ComponentContext<Self>,
    raft_node: Option<RawNode<S>>,
    communication_port: RequiredPort<MessagingPort, Self>,
    reconfig_client: Option<ActorPath>,
    config: Config,
    conf_state: (Vec<u64>, Vec<u64>),
    iteration_id: u32,
    timers: Option<(ScheduledTimer, ScheduledTimer)>,
    has_reconfigured: bool
}

impl<S: RaftStorage + std::marker::Send + std::clone::Clone + 'static> Provide<ControlPort> for RaftComp<S>{
    fn handle(&mut self, event: ControlEvent) -> () {
        match event {
            ControlEvent::Start => {},
            _ => {
                self.stop_timers();
                match self.raft_node.take() {
                    Some(mut raft_node) => raft_node.mut_store().clear().expect("Failed to clear storage!"),
                    _ => {}
                }
            }
        }
    }
}

impl<S: RaftStorage + std::marker::Send + std::clone::Clone + 'static> Require<MessagingPort> for RaftComp<S> {
    fn handle(&mut self, msg: RaftCompMsg) -> () {
        match msg {
            RaftCompMsg::CreateTikvRaft(ctr) => {
                info!(self.ctx.log(), "{}", format!("Creating raft node, iteration: {}, id: {}", ctr.iteration_id, ctr.node_id));
                self.iteration_id = ctr.iteration_id;
                self.config.id = ctr.node_id;
                self.has_reconfigured = false;
                let dir = &format!("./diskstorage_node{}", ctr.node_id);
                match self.raft_node.take() {
                    Some(mut raft_node) => raft_node.mut_store().clear().expect("Failed to clear storage!"),
                    _ => {}
                }
                let store = S::new_with_conf_state(Some(dir), self.conf_state.clone());
                self.raft_node = Some(RawNode::new(&self.config, store).expect("Failed to create TikvRaftNode"));
                if self.config.id == 1 {    // leader
                    let raft_node = self.raft_node.as_mut().unwrap();
                    raft_node.raft.become_candidate();
                    raft_node.raft.become_leader();
                }
                if self.timers.is_none() {
                    self.start_timers();
                }
                self.communication_port.trigger(CommunicatorMsg::InitAck(InitAck(self.iteration_id)));
            }
            RaftCompMsg::TikvRaftMsg(rm) => {
                if rm.iteration_id == self.iteration_id {
                    self.step(rm.payload)
                }
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
                        error!(self.ctx.log(), "Failed proposal: No leader");
                        let pr = ProposalResp::failed(p);
                        self.communication_port.trigger(CommunicatorMsg::ProposalResp(pr));
                    }
                }
            }
            RaftCompMsg::SequenceReq(_) => {
                let raft_node = self.raft_node.as_mut().expect("TikvRaftNode not initialized in RaftComp");
                let raft_entries: Vec<Entry> = raft_node.raft.raft_log.all_entries();
                let mut sequence: Vec<u64> = Vec::new();
                info!(self.ctx.log(), "Got SequenceReq");
                for entry in raft_entries {
                    if entry.get_entry_type() == EntryType::EntryNormal && !&entry.data.is_empty() {
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
    pub fn with(config: Config, conf_state: (Vec<u64>, Vec<u64>)) -> RaftComp<S> {
        RaftComp {
            ctx: ComponentContext::new(),
            raft_node: None,
            communication_port: RequiredPort::new(),
            reconfig_client: None,
            config,
            conf_state,
            iteration_id: 0,
            timers: None,
            has_reconfigured: false
        }
    }

    fn start_timers(&mut self){
        let delay = Duration::from_millis(0);
        let on_ready_uuid = uuid::Uuid::new_v4();
        let tick_uuid = uuid::Uuid::new_v4();
        // give new reference to self to make compiler happy
        let ready_timer = self.schedule_periodic(delay, Duration::from_millis(1), move |c, on_ready_uuid| c.on_ready());
        let tick_timer = self.schedule_periodic(delay, Duration::from_millis(100), move |rc, tick_uuid| rc.tick());
        self.timers = Some((ready_timer, tick_timer));
    }

    fn stop_timers(&mut self) {
        let timers = self.timers.take().unwrap();
        self.cancel_timer(timers.0);
        self.cancel_timer(timers.1);
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

        match &proposal.reconfig {
            Some(reconfig) => {
                self.reconfig_client = Some(proposal.client.clone());
                let _ = raft_node.raft.propose_membership_change(reconfig.to_owned()).unwrap();
            }
            None => {   // i.e normal operation
                let ser_data = proposal.serialize_normal();
                match ser_data {
                    Ok(data) => {
                        raft_node.propose(vec![], data).expect("Failed to propose in TikvRaft");
                    },
                    _ => {
                        error!(self.ctx.log(), "Failed proposal: tikv serialization failure...");
                        // Propose failed, don't forget to respond to the client.
                        let pr = ProposalResp::failed(proposal.clone());
                        self.communication_port.trigger(CommunicatorMsg::ProposalResp(pr));
                        return;
                    }
                }
            }
        }
        let last_index2 = raft_node.raft.raft_log.last_index() + 1;
        if last_index2 == last_index1 {
            // Propose failed, don't forget to respond to the client.
            error!(self.ctx.log(), "Failed proposal: Failed to append to storage?");
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
            error!(self.ctx.log(), "{}", format!("persist raft log fail: {:?}, need to retry or panic", e));
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
            let rm = RaftMsg { iteration_id: self.iteration_id, payload: msg};
            self.communication_port.trigger(CommunicatorMsg::RaftMsg(rm));
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
                    let change_type = cc.get_change_type();
                    match &change_type {
                        ConfChangeType::BeginMembershipChange => {
                            let reconfig = cc.get_configuration();
                            let start_index = cc.get_start_index();
                            info!(self.ctx.log(), "{}", format!("Beginning reconfiguration to: {:?}, start_index: {}", reconfig, start_index));
                            raft_node
                                .raft
                                .begin_membership_change(&cc)
                                .expect("Failed to begin reconfiguration");

                            assert!(raft_node.raft.is_in_membership_change());
                            let cs = ConfState::from(raft_node.raft.prs().configuration().clone());
                            store.set_conf_state(cs, Some((reconfig.clone(), start_index)));
                        }
                        ConfChangeType::FinalizeMembershipChange => {
                            if !self.has_reconfigured {
                                info!(self.ctx.log(), "{}", format!("Finalizing reconfiguration: {:?}", raft_node.raft.prs().next_configuration()));
                                raft_node
                                    .raft
                                    .finalize_membership_change(&cc)
                                    .expect("Failed to finalize reconfiguration");
                                self.has_reconfigured = true;
                                let cs = ConfState::from(raft_node.raft.prs().configuration().clone());
                                if raft_node.raft.state == StateRole::Leader && self.reconfig_client.is_some() {
                                    let client = self.reconfig_client.as_ref().unwrap().clone();
                                    let current_config = (cs.nodes.clone(), cs.learners.clone());
                                    let pr = ProposalResp::succeeded_reconfiguration(client, current_config);
                                    self.communication_port.trigger(CommunicatorMsg::ProposalResp(pr));
                                }
                                store.set_conf_state(cs, None);
                            }
                            /*else {
                                info!(self.ctx.log(), "{}", format!("Got unexpected finalize: id: {}, node_id: {}", cc.id, cc.node_id));
                            }*/
                        }
                        _ => unimplemented!(),
                    }
                } else {
                    // For normal proposals, reply to client
                    let des_proposal = Proposal::deserialize_normal(&entry.data);
                    if raft_node.raft.state == StateRole::Leader {
                        let pr = ProposalResp::succeeded_normal(des_proposal);
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use super::*;
    use super::super::client::tests::TestClient;
    use crate::partitioning_actor::PartitioningActor;
    use synchronoise::CountdownEvent;
    use std::sync::Arc;
    use tikv_raft::storage::MemStorage;

    fn example_config() -> Config {
        Config {
            id: 1,
            election_tick: 10,
            heartbeat_tick: 3,
            ..Default::default()
        }
    }

    fn create_raft_nodes<T: RaftStorage + std::marker::Send + std::clone::Clone + 'static>(
        n: u64,
        systems: &mut Vec<KompactSystem>,
        peers: &mut HashMap<u64, ActorPath>,
        conf_state: (Vec<u64>, Vec<u64>),
    ) {
        for i in 1..=n {
            let system =
                kompact_benchmarks::kompact_system_provider::global().new_remote_system_with_threads(format!("raft{}", i), 4);
            let (raft_comp, unique_reg_f) = system.create_and_register(|| {
                RaftComp::<T>::with(example_config(), conf_state.clone())
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
        }
    }

    #[test]
    fn kompact_raft_ser_test() {
        use super::*;

        use tikv_raft::prelude::{MessageType, Entry, EntryType, Message as TikvRaftMsg};
        use protobuf::RepeatedField;
        /*** RaftMsg ***/
        let from: u64 = 1;
        let to: u64 = 2;
        let term: u64 = 3;
        let index: u64 = 4;
        let iteration_id: u32 = 5;

        let msg_type: MessageType = MessageType::MsgPropose;
        let mut entry = Entry::new();
        entry.set_term(term);
        entry.set_index(index);
        entry.set_entry_type(EntryType::EntryNormal);
        let entries: RepeatedField<Entry> = RepeatedField::from_vec(vec![entry]);

        let mut payload = TikvRaftMsg::new();
        payload.set_from(from);
        payload.set_to(to);
        payload.set_msg_type(msg_type);
        payload.set_entries(entries);
        let rm = RaftMsg { iteration_id, payload: payload.clone() };

        let mut bytes: Vec<u8> = vec![];
        if AtomicBroadcastSer.serialise(&CommunicatorMsg::RaftMsg(rm), &mut bytes).is_err(){panic!("Failed to serialise TikvRaftMsg")};
        let mut buf = bytes.into_buf();
        match AtomicBroadcastSer::deserialise(&mut buf) {
            Ok(des) => {
                match des {
                    CommunicatorMsg::RaftMsg(rm) => {
                        let des_iteration_id = rm.iteration_id;
                        let des_payload = rm.payload;
                        let des_from = des_payload.get_from();
                        let des_to = des_payload.get_to();
                        let des_msg_type = des_payload.get_msg_type();
                        let des_entries = des_payload.get_entries();
                        assert_eq!(des_iteration_id, iteration_id);
                        assert_eq!(from, des_from);
                        assert_eq!(to, des_to);
                        assert_eq!(msg_type, des_msg_type);
                        assert_eq!(des_payload.get_entries(), des_entries);
                        assert_eq!(des_payload, payload);
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
        let voters: Vec<u64> = vec![1,2,3];
        let followers: Vec<u64> = vec![4,5,6];
        let reconfig = (voters.clone(), followers.clone());
        let p = Proposal::reconfiguration(id, client.clone(), reconfig.clone());
        if AtomicBroadcastSer.serialise(&CommunicatorMsg::Proposal(p), &mut b).is_err() {panic!("Failed to serialise Proposal")};
        match AtomicBroadcastSer::deserialise(&mut b.into_buf()){
            Ok(c) => {
                match c {
                    CommunicatorMsg::Proposal(p) => {
                        let des_id = p.id;
                        let des_client = p.client;
                        let des_reconfig = p.reconfig;
                        assert_eq!(id, des_id);
                        assert_eq!(client, des_client);
                        assert_eq!(Some(reconfig.clone()), des_reconfig);
                        println!("Ser/Des Proposal passed");
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
            current_config: Some((voters, followers))
        };
        let mut b1: Vec<u8> = vec![];
        if AtomicBroadcastSer.serialise(&CommunicatorMsg::ProposalResp(pr), &mut b1).is_err(){panic!("Failed to serailise ProposalResp")};
        match AtomicBroadcastSer::deserialise(&mut b1.into_buf()){
            Ok(cm) => {
                match cm {
                    CommunicatorMsg::ProposalResp(pr) => {
                        let des_id = pr.id;
                        let des_succeeded = pr.succeeded;
                        let des_client = pr.client;
                        let des_reconfig = pr.current_config;
                        assert_eq!(id, des_id);
                        assert_eq!(None, des_client);
                        assert_eq!(succeeded, des_succeeded);
                        assert_eq!(Some(reconfig), des_reconfig);
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
        assert_eq!(proposal.reconfig, des_proposal.reconfig);
    }

    #[test]
    fn raft_test() {
        let n: u64 = 5;
        let active_n: u64 = 3;
        let quorum = active_n/2 + 1;
        let num_proposals = 1500;
        let config = (vec![1,2,3], vec![]);
        let reconfig = Some((vec![1,4,5], vec![]));

        type Storage = DiskStorage;

        let mut systems: Vec<KompactSystem> = Vec::new();
        let mut peers: HashMap<u64, ActorPath> = HashMap::new();
        create_raft_nodes::<Storage>(n , &mut systems, &mut peers, config);
        let mut nodes = vec![];
        for i in 1..=n {
            nodes.push(peers.get(&i).unwrap().clone());
        }
        /*** Setup partitioning actor ***/
        let prepare_latch = Arc::new(CountdownEvent::new(1));
        let (partitioning_actor, unique_reg_f) = systems[0].create_and_register(|| {
            PartitioningActor::with(
                prepare_latch.clone(),
                None,
                1,
                nodes,
                1,
                None,
            )
        });
        unique_reg_f.wait_expect(
            Duration::from_millis(1000),
            "PartitioningComp failed to register!",
        );

        let partitioning_actor_f = systems[0].start_notify(&partitioning_actor);
        partitioning_actor_f
            .wait_timeout(Duration::from_millis(1000))
            .expect("PartitioningComp never started!");

        prepare_latch.wait();
        /*** Setup client ***/
        let (p, f) = kpromise::<HashMap<u64, Vec<u64>>>();
        let (client, unique_reg_f) = systems[0].create_and_register( || {
            TestClient::with(
                num_proposals,
                peers,
                reconfig,
                p,
            )
        });
        unique_reg_f.wait_expect(
            Duration::from_millis(1000),
            "Client failed to register!",
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

        assert_eq!(num_proposals, client_sequence.len() as u64);
        for i in 1..=num_proposals {
            let mut iter = client_sequence.iter();
            let found = iter.find(|&&x| x == i).is_some();
            assert_eq!(true, found);
        }
        let mut counter = 0;
        for i in 1..=n {
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
    }
}



