extern crate raft as tikv_raft;

use kompact::prelude::*;
use tikv_raft::{prelude::*, StateRole, prelude::Message as TikvRaftMsg, prelude::Entry};
use protobuf::{Message as PbMessage};
use std::{time::Duration, collections::HashMap, marker::Send, clone::Clone};
use crate::partitioning_actor::{PartitioningActorMsg, PartitioningActorSer};
use super::messages::{*};
use super::storage::raft::*;
use crate::bench::atomic_broadcast::communicator::{Communicator, CommunicationPort, CommunicatorMsg, AtomicBroadcastCompMsg};
use std::sync::Arc;
use uuid::Uuid;
use std::collections::HashSet;

const COMMUNICATOR: &str = "communicator";
const DELAY: Duration = Duration::from_millis(0);
const READY_PERIOD: Duration = Duration::from_millis(1);
const TICK_PERIOD: Duration = Duration::from_millis(100);

#[derive(Debug)]
pub enum RaftReplicaMsg {
    ProposalForward(ProposalForward),
    RegResp(RegistrationResponse)
}

impl From<RegistrationResponse> for RaftReplicaMsg {
    fn from(rr: RegistrationResponse) -> Self {
        RaftReplicaMsg::RegResp(rr)
    }
}

#[derive(ComponentDefinition)]
pub struct RaftReplica<S> where S: RaftStorage + Send + Clone + 'static {
    ctx: ComponentContext<Self>,
    pid: u64,
    initial_config: Vec<u64>,
    raft_comp: Option<Arc<Component<RaftComp<S>>>>,
    communicator: Option<Arc<Component<Communicator>>>,
    nodes: HashMap<u64, ActorPath>,
    pending_registration: Option<Uuid>,
    iteration_id: u32,
    stopped: bool,
    partitioning_actor: Option<ActorPath>,
}

impl<S> RaftReplica<S>  where S: RaftStorage + Send + Clone + 'static {
    pub fn with(initial_config: Vec<u64>) -> Self {
        RaftReplica {
            ctx: ComponentContext::new(),
            pid: 0,
            initial_config,
            raft_comp: None,
            communicator: None,
            nodes: HashMap::new(),
            pending_registration: None,
            iteration_id: 0,
            stopped: false,
            partitioning_actor: None,
        }
    }

    fn create_rawraft_config(&self) -> Config {
        let c = Config {
            id: self.pid,
            election_tick: 5,  // number of ticks without HB before starting election
            heartbeat_tick: 1,  // leader sends HB every heartbeat_tick
            max_inflight_msgs: 10000,    // TODO: max_inflight_msgs * msg_size = BUFFER_SIZE in Kompact?
            max_size_per_msg: 6400, //std::u64::MAX
            ..Default::default()
        };
        assert_eq!(c.validate().is_ok(), true);
        c
    }

    fn create_components(&mut self) {
        let mut communicator_peers: HashMap<u64, ActorPath> = HashMap::with_capacity(self.nodes.len());
        for (pid, ap) in &self.nodes {
            if pid != &self.pid {
                match ap {
                    ActorPath::Named(n) => {
                        let sys_path = n.system();
                        let protocol = sys_path.protocol();
                        let port = sys_path.port();
                        let addr = sys_path.address();
                        let named_communicator = NamedPath::new(
                            protocol,
                            addr.clone(),
                            port,
                            vec![format!("{}{}-{}", COMMUNICATOR, pid, self.iteration_id).into()]
                        );
                        communicator_peers.insert(*pid, ActorPath::Named(named_communicator));
                    },
                    _ => unimplemented!(),
                }
            }
        }

        let system = self.ctx.system();
        let dir = &format!("./diskstorage_node{}", self.pid);
        let conf_state: (Vec<u64>, Vec<u64>) = (self.initial_config.clone(), vec![]);
        let store = S::new_with_conf_state(Some(dir), conf_state);
        let raw_raft = RawNode::new(&self.create_rawraft_config(), store).expect("Failed to create tikv Raft");
        let raft_comp = system.create( || {
            RaftComp::with(raw_raft, self.actor_ref())
        });
        system.register_without_response(&raft_comp);
        let communicator = system.create(|| {
            Communicator::with(communicator_peers)
        });
        system.register_without_response(&communicator);
        let communicator_alias = format!("{}{}-{}", COMMUNICATOR, self.pid, self.iteration_id);
        let r = system.register_by_alias(&communicator, communicator_alias, self);
        self.pending_registration = Some(r.0);
        biconnect_components::<CommunicationPort, _, _>(&communicator, &raft_comp)
            .expect("Could not connect components!");
        self.raft_comp = Some(raft_comp);
        self.communicator = Some(communicator);
    }

    fn start_components(&self) {
        let raft = self.raft_comp.as_ref().expect("No raft comp to start!");
        let communicator = self.communicator.as_ref().expect("No communicator to start!");
        self.ctx.system().start(raft);
        self.ctx.system().start(communicator);
    }

    fn kill_components(&mut self) {
        let system = self.ctx.system();
        if let Some(raft) = self.raft_comp.take() {
            system.kill(raft);
        }
        if let Some(communicator) = self.communicator.take() {
            system.kill(communicator);
        }
    }
}

impl<S> Provide<ControlPort> for RaftReplica<S> where S: RaftStorage + Send + Clone + 'static {
    fn handle(&mut self, _: <ControlPort as Port>::Request) -> () {
        // ignore
    }
}

impl<S> Actor for RaftReplica<S> where S: RaftStorage + Send + Clone + 'static {
    type Message = RaftReplicaMsg;

    fn receive_local(&mut self, msg: Self::Message) -> () {
        match msg {
            RaftReplicaMsg::ProposalForward(pf) => {
                if let Some(ap) = self.nodes.get(&pf.leader_id) {
                    ap.tell( (AtomicBroadcastMsg::Proposal(pf.proposal), AtomicBroadcastSer), self);
                }
            },
            RaftReplicaMsg::RegResp(rr) => {
                if let Some(id) = self.pending_registration {
                    if id == rr.id.0 {
                        self.pending_registration = None;
                        self.partitioning_actor
                            .as_ref()
                            .expect("No partitioning actor found!")
                            .tell_serialised(PartitioningActorMsg::InitAck(self.iteration_id), self)
                            .expect("Should serialise");
                    }
                }
            }
        }
    }

    fn receive_network(&mut self, m: NetMessage) -> () {
        let NetMessage{sender, receiver: _, data} = m;
        match_deser! {data; {
                p: PartitioningActorMsg [PartitioningActorSer] => {
                    match p {
                        PartitioningActorMsg::Init(init) => {
                            self.iteration_id = init.init_id;
                            self.pid = init.pid as u64;
                            info!(self.ctx.log(), "Got init! My pid: {}", self.pid);
                            for (id, actorpath) in init.nodes.into_iter().enumerate() {
                                self.nodes.insert(id as u64 + 1, actorpath);
                            }
                            self.partitioning_actor = Some(sender);
                            self.stopped = false;
                            self.create_components();
                        },
                        PartitioningActorMsg::Run => {
                            self.start_components();
                        },
                        PartitioningActorMsg::Stop => {
                            self.kill_components();
                            self.stopped = true;
                            sender.tell_serialised(PartitioningActorMsg::StopAck, self).expect("Should serialise");
                        },
                        _ => unimplemented!(),
                    }
                },
                am: AtomicBroadcastMsg [AtomicBroadcastSer] => {
                    match am {
                        AtomicBroadcastMsg::Proposal(p) => {
                            if let Some(raft_comp) = self.raft_comp.as_ref() {
                                raft_comp.actor_ref().tell(RaftCompMsg::Propose(p));
                            }
                        },
                        _ => error!(self.ctx.log(), "Got unexpected AtomicBroadcastMsg: {:?}", am),
                    }
                },
                tm: TestMessage [TestMessageSer] => {
                    match tm {
                        TestMessage::SequenceReq => {
                            if let Some(raft_comp) = self.raft_comp.as_ref() {
                                let seq = raft_comp.actor_ref().ask(|promise| RaftCompMsg::SequenceReq(Ask::new(promise, ()))).wait();
                                let sr = SequenceResp::with(self.pid, seq);
                                sender.tell((TestMessage::SequenceResp(sr), TestMessageSer), self);
                            }
                        },
                        _ => error!(self.ctx.log(), "Got unexpected TestMessage: {:?}", tm),
                    }
                },
                !Err(e) => error!(self.ctx.log(), "Error deserialising msg: {:?}", e),
            }
            }
    }
}

#[derive(Debug)]
pub enum RaftCompMsg {
    Propose(Proposal),
    SequenceReq(Ask<(), Vec<u64>>)
}

#[derive(ComponentDefinition)]
pub struct RaftComp<S> where S: RaftStorage + Send + Clone + 'static {
    ctx: ComponentContext<Self>,
    replica: ActorRef<RaftReplicaMsg>,
    raw_raft: RawNode<S>,
    communication_port: RequiredPort<CommunicationPort, Self>,
    timers: Option<(ScheduledTimer, ScheduledTimer)>,
    has_reconfigured: bool,
    cached_client: bool,
    stopped: bool,
}

impl<S> Provide<ControlPort> for RaftComp<S> where
    S: RaftStorage + Send + Clone + 'static {
        fn handle(&mut self, event: ControlEvent) -> () {
            match event {
                ControlEvent::Start => {
                    self.start_timers();
                },
                _ => {
                    self.stop_timers();
                    self.raw_raft.mut_store().clear().expect("Failed to clear storage!");
                }
            }
        }
}

impl<S> Actor for RaftComp<S> where
    S: RaftStorage + Send + Clone + 'static{
    type Message = RaftCompMsg;

    fn receive_local(&mut self, msg: Self::Message) -> () {
        match msg {
            RaftCompMsg::Propose(p) => {
                if !self.stopped {
                    if self.raw_raft.raft.state == StateRole::Leader{
                        self.propose(p);    // TODO propose no matter what state if TikV forwards automatically
                    }
                    else {
                        let leader_id = self.raw_raft.raft.leader_id;
                        if leader_id > 0 {
                            let pf = ProposalForward::with(leader_id, p);
                            self.replica.tell(RaftReplicaMsg::ProposalForward(pf));
                        } /*else {    // no leader
                            // info!(self.ctx.log(), "Got proposal but no leader...");
                            if !self.cached_client {
                                self.communication_port.trigger(CommunicatorMsg::CacheClient(p.client));
                                self.cached_client = true;
                            }
                            let pr = ProposalResp::failed(p.id);
                            self.communication_port.trigger(CommunicatorMsg::ProposalResponse(pr));
                        }*/
                    }
                }
            },
            RaftCompMsg::SequenceReq(sr) => {
                let raft_entries: Vec<Entry> = self.raw_raft.raft.raft_log.all_entries();
                let mut sequence: Vec<u64> = Vec::with_capacity(raft_entries.len());
                let mut unique = HashSet::new();
                for entry in raft_entries {
                    if entry.get_entry_type() == EntryType::EntryNormal && !&entry.data.is_empty() {
                        let id = entry.data.as_slice().get_u64();
                        if id != 0 {
                            sequence.push(id);
                        }
                        unique.insert(id);
                    }
                }
                info!(self.ctx.log(), "Got SequenceReq: my seq_len={}. Unique={}", sequence.len(), unique.len());
                sr.reply(sequence).expect("Failed to respond SequenceReq ask");
            }
        }
    }

    fn receive_network(&mut self, _msg: NetMessage) -> () {
        unimplemented!()
    }
}

impl<S> Require<CommunicationPort> for RaftComp<S> where
    S: RaftStorage + Send + Clone + 'static {
        fn handle(&mut self, msg: AtomicBroadcastCompMsg) -> () {
            if let AtomicBroadcastCompMsg::RawRaftMsg(rm) = msg {
                if !self.stopped {
                    self.step(rm);
                }
            }
        }
}

impl<S> RaftComp<S> where S: RaftStorage + Send + Clone + 'static {
    pub fn with(raw_raft: RawNode<S>, replica: ActorRef<RaftReplicaMsg>) -> RaftComp<S> {
        RaftComp {
            ctx: ComponentContext::new(),
            replica,
            raw_raft,
            communication_port: RequiredPort::new(),
            timers: None,
            has_reconfigured: false,
            cached_client: false,
            stopped: false
        }
    }

    fn start_timers(&mut self){
        let ready_timer = self.schedule_periodic(DELAY, READY_PERIOD, move |c, _| c.on_ready());
        let tick_timer = self.schedule_periodic(DELAY, TICK_PERIOD, move |rc, _| rc.tick() );
        self.timers = Some((ready_timer, tick_timer));
    }

    fn stop_timers(&mut self) {
        if let Some(timers) = self.timers.take() {
            self.cancel_timer(timers.0);
            self.cancel_timer(timers.1);
        }
    }

    fn tick(&mut self) {
        self.raw_raft.tick();
    }

    fn step(&mut self, msg: TikvRaftMsg) {
        let _ = self.raw_raft.step(msg);
    }

    fn propose(&mut self, proposal: Proposal) {
        let last_index1 = self.raw_raft.raft.raft_log.last_index() + 1;
        let id = proposal.id;
        if id % 2000 == 0 {
            info!(self.ctx.log(), "Proposing {}", id);
        }
        if !self.cached_client {
            self.communication_port.trigger(CommunicatorMsg::CacheClient(proposal.client));
            self.cached_client = true;
        }
        match proposal.reconfig {
            Some(reconfig) => {
                let _ = self.raw_raft.raft.propose_membership_change(reconfig).unwrap();
            }
            None => {   // i.e normal operation
                let mut data: Vec<u8> = Vec::with_capacity(8);
                data.put_u64(id);
                self.raw_raft.propose(vec![], data).expect("Failed to propose in TikvRaft");
            }
        }
        let last_index2 = self.raw_raft.raft.raft_log.last_index() + 1;
        if last_index2 == last_index1 {
            // Propose failed, don't forget to respond to the client.
            error!(self.ctx.log(), "Failed proposal: Failed to append to storage?");
            let pr = ProposalResp::failed(id);
            self.communication_port.trigger(CommunicatorMsg::ProposalResponse(pr));
        }
    }

    fn on_ready(&mut self) {
        if !self.raw_raft.has_ready() {
            return;
        }
        let mut store = self.raw_raft.raft.raft_log.store.clone();

        // Get the `Ready` with `RawNode::ready` interface.
        let mut ready = self.raw_raft.ready();

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
            self.communication_port.trigger(CommunicatorMsg::RawRaftMsg(msg));
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
                            debug!(self.ctx.log(), "{}", format!("Beginning reconfiguration to: {:?}, start_index: {}", reconfig, start_index));
                            self.raw_raft
                                .raft
                                .begin_membership_change(&cc)
                                .expect("Failed to begin reconfiguration");

                            assert!(self.raw_raft.raft.is_in_membership_change());  // TODO remove?
                            let cs = ConfState::from(self.raw_raft.raft.prs().configuration().clone());
                            store.set_conf_state(cs, Some((reconfig.clone(), start_index)));
                        }
                        ConfChangeType::FinalizeMembershipChange => {
                            if !self.has_reconfigured {
                                self.raw_raft
                                    .raft
                                    .finalize_membership_change(&cc)
                                    .expect("Failed to finalize reconfiguration");

                                self.has_reconfigured = true;
                                let cs = ConfState::from(self.raw_raft.raft.prs().configuration().clone());
                                if self.cached_client {
                                    let current_config = (cs.nodes.clone(), cs.learners.clone());
                                    let pr = ProposalResp::succeeded_reconfiguration(current_config);
                                    self.communication_port.trigger(CommunicatorMsg::ProposalResponse(pr));
                                }
                                store.set_conf_state(cs, None);
                            }
                        }
                        _ => unimplemented!(),
                    }
                } else {
                    // normal proposals
                    if self.cached_client {
                        let id = entry.data.as_slice().get_u64();
                        let pr = ProposalResp::succeeded_normal(id);
                        self.communication_port.trigger(CommunicatorMsg::ProposalResponse(pr));
                    }
                }
            }
            if let Some(last_committed) = committed_entries.last() {
                store.set_hard_state(last_committed.index, last_committed.term).expect("Failed to set hardstate");
            }
        }
        // Call `RawNode::advance` interface to update position flags in the raft.
        self.raw_raft.advance(ready);
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::client::tests::TestClient;
    use crate::partitioning_actor::{PartitioningActor, IterationControlMsg};
    use synchronoise::CountdownEvent;
    use std::sync::Arc;
    #[allow(unused_imports)]
    use tikv_raft::storage::MemStorage;

    fn create_raft_nodes<T: RaftStorage + std::marker::Send + std::clone::Clone + 'static>(
        n: u64,
        systems: &mut Vec<KompactSystem>,
        peers: &mut HashMap<u64, ActorPath>,
        conf_state: (Vec<u64>, Vec<u64>),
    ) {
        for i in 1..=n {
            let system =
                kompact_benchmarks::kompact_system_provider::global().new_remote_system_with_threads(format!("raft{}", i), 4);
            let (raft_replica, unique_reg_f) = system.create_and_register(|| {
                RaftReplica::<T>::with(conf_state.0.clone())
            });
            let raft_replica_f = system.start_notify(&raft_replica);
            raft_replica_f
                .wait_timeout(Duration::from_millis(1000))
                .expect("RaftComp never started!");
            unique_reg_f.wait_expect(
                Duration::from_millis(1000),
                "RaftComp failed to register!",
            );

            let named_reg_f = system.register_by_alias(
                &raft_replica,
                format!("raft_replica{}", i),
            );

            named_reg_f.wait_expect(
                Duration::from_millis(1000),
                "Communicator failed to register!",
            );

            /*** Add self to peers map ***/
            let self_path = ActorPath::Named(NamedPath::with_system(
                system.system_path(),
                vec![format!("raft_replica{}", i).into()],
            ));
            systems.push(system);
            peers.insert(i, self_path);
        }
    }
/*
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
        RaftSer.serialise(&rm, &mut bytes).expect("Failed to serialise RaftMsg");
        let mut buf = bytes.as_slice();
        match RaftSer::deserialise(&mut buf) {
            Ok(rm) => {
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
        AtomicBroadcastSer.serialise(&AtomicBroadcastMsg::Proposal(p), &mut b).expect("Failed to serialise Proposal");
        match AtomicBroadcastSer::deserialise(&mut b.as_slice()){
            Ok(c) => {
                match c {
                    AtomicBroadcastMsg::Proposal(p) => {
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
            succeeded,
            current_config: Some((voters, followers))
        };
        let mut b1: Vec<u8> = vec![];
        AtomicBroadcastSer.serialise(&AtomicBroadcastMsg::ProposalResp(pr), &mut b1).expect("Failed to serailise ProposalResp");
        match AtomicBroadcastSer::deserialise(&mut b1.as_slice()){
            Ok(cm) => {
                match cm {
                    AtomicBroadcastMsg::ProposalResp(pr) => {
                        let des_id = pr.id;
                        let des_succeeded = pr.succeeded;
                        let des_reconfig = pr.current_config;
                        assert_eq!(id, des_id);
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
*/
    #[test]
    fn raft_test() {
        let n: u64 = 3;
        let quorum = n/2 + 1;
        let num_proposals = 1000;
        let batch_size = 1000;
        let config = (vec![1,2,3], vec![]);
        let reconfig = None;
       // let reconfig = Some((vec![1,4,5], vec![]));
        let check_sequences = false;

        type Storage = MemStorage;

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
        partitioning_actor.actor_ref().tell(IterationControlMsg::Prepare(None));
        prepare_latch.wait();
        partitioning_actor.actor_ref().tell(IterationControlMsg::Run);
        /*** Setup client ***/
        let (p, f) = kpromise::<HashMap<u64, Vec<u64>>>();
        let (client, unique_reg_f) = systems[0].create_and_register( || {
            TestClient::with(
                num_proposals,
                batch_size,
                peers,
                reconfig.clone(),
                p,
                check_sequences
            )
        });
        unique_reg_f.wait_expect(
            Duration::from_millis(1000),
            "Client failed to register!",
        );
        let client_f = systems[0].start_notify(&client);
        client_f.wait_timeout(Duration::from_millis(1000))
            .expect("Client never started!");
        client.actor_ref().tell(Run);
        let all_sequences = f.wait();
        let client_sequence = all_sequences.get(&0).expect("Client's sequence should be in 0...").to_owned();
        for system in systems {
            let _ = system.shutdown();
                // .expect("Kompact didn't shut down properly");
        }

        assert_eq!(num_proposals, client_sequence.len() as u64);
        for i in 1..=num_proposals {
            let mut iter = client_sequence.iter();
            let found = iter.find(|&&x| x == i).is_some();
            assert_eq!(true, found);
        }
        let mut counter = 0;
        if check_sequences {
            for i in 1..=n {
                let sequence = all_sequences.get(&i).expect(&format!("Did not get sequence for node {}", i));
                // println!("Node {}: {:?}", i, sequence.len());
                // assert!(client_sequence.starts_with(sequence));
                for id in &client_sequence {
                    if !sequence.contains(&id) {
                        counter += 1;
                        break;
                    }
                }
            }
            if counter >= quorum {
                panic!("Majority DOES NOT have all client elements: counter: {}, quorum: {}", counter, quorum);
            }
        }
    }
}



