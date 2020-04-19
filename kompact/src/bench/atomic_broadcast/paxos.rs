use kompact::prelude::*;
use super::storage::paxos::*;
use std::fmt::{Debug, Formatter, Error};
use ballot_leader_election::{BallotLeaderComp, BallotLeaderElection};
use raw_paxos::{Entry, Paxos};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use super::messages::paxos::{Message as RawPaxosMsg, ballot_leader_election::Leader};
use crate::bench::atomic_broadcast::messages::paxos::{PaxosSer, ReconfigInit, ReconfigSer, SequenceResponse, SequenceRequest, SequenceMetaData, Reconfig, ReconfigurationMsg};
use crate::bench::atomic_broadcast::messages::{Proposal, ProposalResp, AtomicBroadcastMsg, AtomicBroadcastSer, SequenceResp, RECONFIG_ID};
use crate::partitioning_actor::{PartitioningActorSer, PartitioningActorMsg, Init};
use synchronoise::CountdownEvent;
use uuid::Uuid;
use kompact::prelude::Buf;
use crate::bench::atomic_broadcast::raft::RaftCompMsg::SequenceReq;
use rand::Rng;

const BLE: &str = "ble";
const PAXOS: &str = "paxos";

const DELTA: u64 = 300;

pub trait SequenceTraits: Sequence + Debug + Send + Sync + 'static {}
pub trait PaxosStateTraits: PaxosState + Send + 'static {}

#[derive(Debug)]
pub struct FinalMsg<S> where
    S: SequenceTraits,
{
    pub config_id: u32,
    pub nodes: Reconfig,
    pub final_sequence: Arc<S>,
}

impl<S> FinalMsg<S> where
    S: SequenceTraits
{
    pub fn with(config_id: u32, nodes: Reconfig, final_sequence: Arc<S>) -> FinalMsg<S> {
        FinalMsg { config_id, nodes, final_sequence }
    }
}

#[derive(Debug)]
pub enum PaxosCompMsg {
    Propose(Proposal),
    SequenceReq(Ask<(u64, u64), Option<Vec<u8>>>),
    Stop(Stop),
}

#[derive(Clone)]
pub struct Stop {
    cd: Arc<CountdownEvent>
}

impl Debug for Stop {
    fn fmt(&self, _f: &mut Formatter<'_>) -> Result<(), Error> {
        Ok(())
    }
}

impl Stop {
    fn with(cd: Arc<CountdownEvent>) -> Stop {
        Stop{ cd }
    }
}

pub enum TransferPolicy {
    Eager,
    Pull
}

#[derive(ComponentDefinition)]
pub struct ReplicaComp<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    ctx: ComponentContext<Self>,
    pid: u64,
    initial_config: Vec<u64>,
    replicas: HashMap<u32, (Arc<Component<BallotLeaderComp>>, Arc<Component<PaxosComp<S, P>>>)>,
    active_config_id: u32,
    nodes: HashMap<u64, ActorPath>, // derive actorpaths of peers' ble and paxos replicas from these
    prev_sequences: HashMap<u32, Arc<S>>,
    stopped: bool,
    iteration_id: u32,
    partitioning_actor: Option<ActorPath>,
    pending_registrations: HashSet<Uuid>,
    policy: TransferPolicy,
    pending_seq_transfers: HashMap<u32, (u32, HashMap<u32, Vec<Entry>>)>,   // <config_id, (num_segments, <segment_id, entries>)
    pending_seq_timers: HashMap<(u32, u32), ScheduledTimer>, // <(config_id, segment_id), retry_timer>
    complete_sequences: HashSet<u32>,
    active_peers: (HashSet<u64>, HashSet<u64>)
}

impl<S, P> ReplicaComp<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    pub fn with(initial_config: Vec<u64>, policy: TransferPolicy) -> ReplicaComp<S, P> {
        ReplicaComp {
            ctx: ComponentContext::new(),
            pid: 0,
            initial_config,
            replicas: HashMap::new(),
            active_config_id: 0,
            nodes: HashMap::new(),
            prev_sequences: HashMap::new(),
            stopped: false,
            iteration_id: 0,
            partitioning_actor: None,
            pending_registrations: HashSet::new(),
            policy,
            pending_seq_transfers: HashMap::new(),
            pending_seq_timers: HashMap::new(),
            complete_sequences: HashSet::new(),
            active_peers: (HashSet::new(), HashSet::new())
        }
    }

    fn create_replica(&mut self, config_id: u32, nodes: Vec<u64>, register_with_response: bool) {
        let mut paxos_peers = HashMap::new();
        let mut ble_peers = vec![];
        for pid in nodes {
            if pid != self.pid {
                let actorpath = self.nodes.get(&pid).expect("No actorpath found");
                match actorpath {
                    ActorPath::Named(n) => {
                        // derive paxos and ble actorpath of peers from replica actorpath
                        let sys_path = n.system();
                        let protocol = sys_path.protocol();
                        let port = sys_path.port();
                        let addr = sys_path.address();
                        let named_paxos = NamedPath::new(
                            protocol,
                            addr.clone(),
                            port,
                            vec![format!("{}{},{}-{}", PAXOS, pid, config_id, self.iteration_id).into()]
                            // vec![format!("{}{},{}", PAXOS, pid, config_id)]
                        );
                        let named_ble = NamedPath::new(
                            protocol,
                            addr.clone(),
                            port,
                            vec![format!("{}{},{}-{}", BLE, pid, config_id, self.iteration_id).into()]
                            // vec![format!("{}{},{}", BLE, pid, config_id).into()]
                        );
                        paxos_peers.insert(pid, ActorPath::Named(named_paxos));
                        ble_peers.push(ActorPath::Named(named_ble));
                    },
                    _ => error!(self.ctx.log(), "{}", format!("Actorpath is not named for node {}", pid)),
                }
            }
        }
        // info!(self.ctx.log(), "Derived all actorpaths");
        let system = self.ctx.system();
        /*** create and register Paxos ***/
        let paxos_comp = system.create(|| {
            PaxosComp::with(self.ctx.actor_ref(), paxos_peers, config_id, self.pid)
        });
        system.register_without_response(&paxos_comp);
        /*** create and register BLE ***/
        let ble_comp = system.create( || {
            BallotLeaderComp::with(ble_peers, self.pid, DELTA)
        });
        system.register_without_response(&ble_comp);
        let paxos_alias = format!("{}{},{}-{}", PAXOS, self.pid, config_id, self.iteration_id);
        let ble_alias = format!("{}{},{}-{}", BLE, self.pid, config_id, self.iteration_id);
        // let paxos_alias = format!("{}{},{}", PAXOS, self.pid, config_id);
        // let ble_alias = format!("{}{},{}", BLE, self.pid, config_id);
        if register_with_response {
            /*let (paxos_id, ble_id) = if self.iteration_id > 1 {
                let p = system.update_alias_registration(&paxos_comp, paxos_alias, self);
                let b = system.update_alias_registration(&ble_comp, ble_alias, self);
                (p, b)
            } else {
                let p = system.register_by_alias(&paxos_comp, paxos_alias, self);
                let b = system.register_by_alias(&ble_comp, ble_alias, self);
                (p, b)
            };
            self.pending_registrations.insert(paxos_id.0);
            self.pending_registrations.insert(ble_id.0);*/
            let paxos_id = system.register_by_alias(&paxos_comp, paxos_alias, self);
            self.pending_registrations.insert(paxos_id.0);

            let ble_id = system.register_by_alias(&ble_comp, ble_alias, self);
            self.pending_registrations.insert(ble_id.0);
        } else {
            /*if self.iteration_id > 1 {
                system.update_alias_registration_without_response(&paxos_comp, paxos_alias);
                system.update_alias_registration_without_response(&ble_comp, ble_alias);
            } else {
                system.register_by_alias_without_response(&paxos_comp, paxos_alias);
                system.register_by_alias_without_response(&ble_comp, ble_alias);
            }*/
            system.register_by_alias_without_response(&paxos_comp, paxos_alias);
            system.register_by_alias_without_response(&ble_comp, ble_alias);
        }

        biconnect_components::<BallotLeaderElection, _, _>(&ble_comp, &paxos_comp)
            .expect("Could not connect components!");
        self.replicas.insert(config_id, (ble_comp, paxos_comp));
        self.active_config_id = config_id;
    }

    fn start_replica(&mut self, config_id: u32) {
        if let Some((ble, paxos)) = self.replicas.get(&config_id) {
            self.ctx.system().start(paxos);
            self.ctx.system().start(ble);
        }
    }

    fn kill_all_replicas(&mut self) {
        for (_, (ble, paxos)) in self.replicas.drain() {
            self.ctx.system().kill(ble);
            self.ctx.system().kill(paxos);
        }
    }

    fn new_iteration(&mut self, init: Init) {
        let nodes = init.nodes;
        self.kill_all_replicas();
        self.active_config_id = 0;
        self.stopped = false;
        self.prev_sequences = HashMap::new();
        self.pid = init.pid as u64;
        self.iteration_id = init.init_id;
        for (id, actorpath) in nodes.into_iter().enumerate() {
            self.nodes.insert(id as u64 + 1, actorpath);
        }
        if self.initial_config.contains(&self.pid){
            self.active_config_id += 1;
            self.create_replica(self.active_config_id, self.initial_config.clone(), true);
        } else {
            let resp = PartitioningActorMsg::InitAck(self.iteration_id);
            let ap = self.partitioning_actor.take().expect("PartitioningActor not found!");
            ap.tell_serialised(resp, self).expect("Should serialise");
        }
    }

    fn pull_previous_sequence(&mut self, config_id: u32, seq_len: u64) {
        let num_continued_nodes = self.active_peers.0.len() + self.active_peers.1.len();
        let mut continued_nodes = self.active_peers.0.clone();
        for pid in &self.active_peers.1 {
            continued_nodes.insert(*pid);
        }
        let offset = seq_len/ num_continued_nodes as u64;
        if num_continued_nodes < 1 {
            panic!("No continued nodes to pull sequence from: {:?}", self.active_peers);
        }
        info!(self.ctx.log(), "Pulling sequence {} from {:?}", config_id, &self.active_peers);
        self.pending_seq_transfers.insert(config_id, (num_continued_nodes as u32, HashMap::with_capacity(num_continued_nodes)));
        for (i, pid) in continued_nodes.iter().enumerate() {
            let from_idx = i as u64 * offset;
            let to_idx = if from_idx as u64 + offset > seq_len{
                seq_len
            } else {
                from_idx + offset
            };
            let tag = i + 1;
            self.request_sequence(*pid, config_id, from_idx, to_idx, tag as u32);
            /*let timer = self.schedule_once(Duration::from_millis(300), move |c, _| c.retry_request_sequence()); // TODO
            self.pending_seq_timers.insert(
                (config_id, tag),
                timer
            );*/
        }
    }

    fn request_sequence(&self, pid: u64, config_id: u32, from_idx: u64, to_idx: u64, tag: u32) {
        let sr = SequenceRequest::with(config_id, tag, from_idx, to_idx);
        self.nodes
            .get(&pid)
            .expect(&format!("Failed to get Actorpath of node {}", pid))
            .tell((ReconfigurationMsg::SequenceRequest(sr), ReconfigSer), self);
    }

    fn retry_request_sequence(&mut self) {
        unimplemented!();
    }

    fn transfer_sequence(&self, sr: SequenceRequest, requestor: ActorPath) {
        let (succeeded, ser_entries) = match self.prev_sequences.get(&sr.config_id) {
            Some(seq) => {
                info!(self.ctx.log(), "Transfer seq: {:?}", &sr);
                let entries = seq.get_ser_entries(sr.from_idx, sr.to_idx).expect("Should have all entries in final sequence!");
                (true, entries)
            },
            None => {
                if self.active_config_id == sr.config_id {
                    let (_, paxos) = self.replicas.get(&sr.config_id).unwrap();
                    let ser_entries_f = paxos
                        .actor_ref()
                        .ask(|promise| PaxosCompMsg::SequenceReq(Ask::new(promise, (sr.from_idx, sr.to_idx))))
                        .wait();
                    match ser_entries_f {
                        Some(ser_entries) => (true, ser_entries),
                        None => (false, vec![]),
                    }
                } else {
                    (false, vec![])
                }
            }
        };
        let prev_seq_len = match self.prev_sequences.get(&(sr.config_id-1)) {
            Some(prev_seq) => {
                prev_seq.get_sequence_len()
            },
            None => 0,
        };
        let metadata = SequenceMetaData::with(sr.config_id-1, prev_seq_len);
        let st = SequenceResponse::with(sr.config_id, sr.tag, succeeded, sr.from_idx, sr.to_idx, ser_entries, metadata);
        requestor.tell((ReconfigurationMsg::SequenceResponse(st), ReconfigSer), self);
    }

    fn handle_sequence_transfer(&mut self, st: SequenceResponse) {
        info!(self.ctx.log(), "Handling SequenceTransfer config_id: {}, tag: {}", st.config_id, st.tag);
        let prev_config_id = st.metadata.config_id;
        let prev_seq_len = st.metadata.len;
        // pull previous sequence if exists and not already started
        if prev_config_id != 0 && !self.complete_sequences.contains(&prev_config_id) && !self.pending_seq_transfers.contains_key(&prev_config_id) {
            self.pull_previous_sequence( prev_config_id, prev_seq_len);
        }
        if st.succeeded {
            match self.pending_seq_transfers.get_mut(&st.config_id) {
                Some(segments) => {
                    let seq = PaxosSer::deserialise_entries(&mut st.ser_entries.as_slice());
                    segments.1.insert(st.tag, seq);
                    if segments.1.len() as u32 == segments.0 {  // got all segments, i.e. complete sequence
                        info!(self.ctx.log(), "Got all segments for sequence {}", st.config_id);
                        let mut complete_seq = segments.1.remove(&1).unwrap();
                        for tag in 2..=segments.0 {
                            let mut segment = segments.1.remove(&tag).unwrap();
                            complete_seq.append(&mut segment);
                        }
                        let final_sequence = S::new_with_sequence(complete_seq);
                        self.prev_sequences.insert(st.config_id, Arc::new(final_sequence));
                        self.complete_sequences.insert(st.config_id);
                        self.pending_seq_transfers.remove(&st.config_id);
                        if self.pending_seq_transfers.is_empty() {  // got all previous sequences
                            self.complete_sequences.clear();
                            self.start_replica(self.active_config_id);
                        }
                    }
                },
                None => panic!("{}", &format!("Got unexpected sequence transfer config_id: {}, tag: {}, index: {}-{}", st.config_id, st.tag, st.from_idx, st.to_idx)),
            }
        } else {
            let config_id = st.config_id;
            let tag = st.tag;
            let from_idx = st.from_idx;
            let to_idx = st.to_idx;
            let num_active = self.active_peers.0.len();
            let mut rng = rand::thread_rng();
            let rnd = rng.gen_range(1, num_active);
            let pid = self.active_peers.0.iter().nth(rnd).expect("Failed to get random active pid");
            info!(self.ctx.log(), "Got failed SequenceResponse for config_id: {}, tag: {}. Requesting node {} instead", config_id, tag, pid);
            self.request_sequence(*pid, config_id, from_idx, to_idx, tag);
        }
    }
}

impl<S, P> Provide<ControlPort> for ReplicaComp<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    fn handle(&mut self, event: <ControlPort as Port>::Request) -> () {
        // ignore
        match event {
            ControlEvent::Start => info!(self.ctx.log(), "Started ReplicaComp!"),
            _ => {},
        }
    }
}

#[derive(Debug)]
pub enum ReplicaCompMsg<S> where S: SequenceTraits{
    Reconfig(FinalMsg<S>),
    RegResp(RegistrationResponse)
}

impl<S> From<RegistrationResponse> for ReplicaCompMsg<S> where S: SequenceTraits {
    fn from(rr: RegistrationResponse) -> Self {
        ReplicaCompMsg::RegResp(rr)
    }
}

impl<S, P> Actor for ReplicaComp<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    type Message = ReplicaCompMsg<S>;

    fn receive_local(&mut self, msg: Self::Message) -> () {
        match msg {
            ReplicaCompMsg::Reconfig(r) => {
                info!(self.ctx.log(), "Got reconfig");
                let prev_config_id = self.active_config_id;
                let final_seq_len: u64 = r.final_sequence.get_sequence_len();
                let seq_metadata = SequenceMetaData::with(prev_config_id, final_seq_len);
                info!(self.ctx.log(), "Inserting final sequence {}", prev_config_id);
                self.prev_sequences.insert(prev_config_id, r.final_sequence);
                let r_init = ReconfigurationMsg::Init(ReconfigInit::with(r.config_id, r.nodes.clone(), seq_metadata, self.pid));
                for pid in &r.nodes.new_nodes {
                    if pid != &self.pid {
                        info!(self.ctx.log(), "Sending ReconfigInit to node {}", pid);
                        let actorpath = self.nodes.get(pid).expect("No actorpath found for new node");
                        actorpath.tell((r_init.clone(), ReconfigSer), self);
                        if let TransferPolicy::Eager = self.policy {
                            unimplemented!();
                        }
                    }
                }
                let mut nodes = r.nodes.continued_nodes;
                let mut new_nodes = r.nodes.new_nodes;
                if nodes.contains(&self.pid) {
                    nodes.append(&mut new_nodes);
                    self.create_replica(r.config_id, nodes, false);
                    self.start_replica(r.config_id);
                }
            },
            ReplicaCompMsg::RegResp(rr) => {
                self.pending_registrations.remove(&rr.id.0);
                if self.pending_registrations.is_empty() {
                    info!(self.ctx.log(), "Registered all components");
                    let resp = PartitioningActorMsg::InitAck(self.iteration_id);
                    let ap = self.partitioning_actor.take().expect("PartitioningActor not found!");
                    ap.tell_serialised(resp, self).expect("Should serialise");
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
                        info!(self.ctx.log(), "{}", format!("Got init! My pid: {}", init.pid));
                        self.partitioning_actor = Some(sender);
                        self.new_iteration(init);
                    },
                    PartitioningActorMsg::Run => {
                        self.start_replica(self.active_config_id);
                    },
                    PartitioningActorMsg::Stop => {
                        info!(self.ctx.log(), "Stopping ble and paxos...");
                        let num_comps = self.replicas.len() * 2;
                        if num_comps > 0 {
                            let cd = Arc::new(CountdownEvent::new(num_comps));
                            let s = Stop::with(cd.clone());
                            for (ble, paxos) in self.replicas.values() {
                                ble.actor_ref().tell(s.clone());
                                paxos.actor_ref().tell(PaxosCompMsg::Stop(s.clone()));
                            }
                            cd.wait_timeout(Duration::from_secs(5));
                        }
                        info!(self.ctx.log(), "Stopped all child components");
                        self.stopped = true;
                        sender
                            .tell_serialised(PartitioningActorMsg::StopAck, self)
                            .expect("Should serialise");
                    },
                    _ => unimplemented!()
                }
            },
            rm: ReconfigurationMsg [ReconfigSer] => {
                match rm {
                    ReconfigurationMsg::Init(r) => {
                        if self.active_config_id < r.config_id && !self.stopped {
                            info!(self.ctx.log(), "Got ReconfigInit!");
                            for pid in &r.nodes.continued_nodes {
                                if pid == &r.from {
                                    self.active_peers.0.insert(*pid);
                                } else {
                                    self.active_peers.1.insert(*pid);
                                }
                            }
                            if let TransferPolicy::Pull = self.policy {
                                std::thread::sleep(Duration::from_secs(1));
                                self.pull_previous_sequence(r.seq_metadata.config_id, r.seq_metadata.len);
                            }
                            let mut nodes = r.nodes.continued_nodes;
                            let mut new_nodes = r.nodes.new_nodes;
                            nodes.append(&mut new_nodes);
                            self.create_replica(r.config_id, nodes, false);
                        } else if self.active_config_id == r.config_id {
                            self.active_peers.1.remove(&r.from);
                            self.active_peers.0.insert(r.from);
                        }
                    },
                    ReconfigurationMsg::SequenceRequest(sr) => {
                        self.transfer_sequence(sr, sender);
                    },
                    ReconfigurationMsg::SequenceResponse(st) => {
                        self.handle_sequence_transfer(st);
                    }
                }
            },
            am: AtomicBroadcastMsg [AtomicBroadcastSer] => {
                match am {
                    AtomicBroadcastMsg::Proposal(p) => {
                        if !self.stopped {
                            let active_paxos = &self.replicas.get(&self.active_config_id).expect("No active paxos replica").1;
                            active_paxos.actor_ref().tell(PaxosCompMsg::Propose(p));
                        }
                    },
                    /*AtomicBroadcastMsg::SequenceReq => {
                        if self.replicas.len() > 1 {
                            unimplemented!();
                        } else {
                            let active_paxos = &self.replicas.get(&self.active_config_id).expect("No active paxos replica").1;
                            active_paxos.actor_ref().tell(PaxosCompMsg::SequenceReq(sender));
                        }
                    },*/
                    _ => unimplemented!(),
                }
            },
            !Err(e) => error!(self.ctx.log(), "Error deserialising msg: {:?}", e),
            }
        }
    }
}

#[derive(ComponentDefinition)]
struct PaxosComp<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    ctx: ComponentContext<Self>,
    supervisor: ActorRef<ReplicaCompMsg<S>>,
    ble_port: RequiredPort<BallotLeaderElection, Self>,
    peers: HashMap<u64, ActorPath>,
    paxos: Paxos<S, P>,
    pid: u64,
    current_leader: u64,
    stopped: bool,
    timers: Option<(ScheduledTimer, ScheduledTimer, ScheduledTimer)>,
    pending_proposals: HashSet<u64>,
    cached_client: Option<ActorPath>,
}

impl<S, P> PaxosComp<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    fn with(
        supervisor: ActorRef<ReplicaCompMsg<S>>,
        peers: HashMap<u64, ActorPath>,
        config_id: u32,
        pid: u64
    ) -> PaxosComp<S, P>
    {
        let seq = S::new();
        let paxos_state = P::new();
        let storage = Storage::with(seq, paxos_state);
        let mut raw_peers = vec![];
        for pid in peers.keys() {
            raw_peers.push(pid.clone());
        }
        let paxos = Paxos::with(config_id, pid, raw_peers, storage);
        PaxosComp {
            ctx: ComponentContext::new(),
            supervisor,
            ble_port: RequiredPort::new(),
            peers,
            paxos,
            pid,
            current_leader: 0,
            stopped: false,
            timers: None,
            pending_proposals: HashSet::new(),
            cached_client: None,
        }
    }

    fn start_timers(&mut self) {
        let decided_timer = self.schedule_periodic(
            Duration::from_millis(0),
            Duration::from_millis(100),
            move |c, _| c.get_decided()
        );
        let outgoing_timer = self.schedule_periodic(
            Duration::from_millis(0),
            Duration::from_millis(1),
            move |p, _| p.get_outgoing()
        );
        let print_timer = self.schedule_periodic(
            Duration::from_secs(5),
            Duration::from_secs(60),
            move |p, _| info!(p.ctx.log(), "{}", p.paxos.get_ld_la())
        );
        self.timers = Some((decided_timer, outgoing_timer, print_timer));
    }

    fn stop_timers(&mut self) {
        if let Some(timers) = self.timers.take() {
            self.cancel_timer(timers.0);
            self.cancel_timer(timers.1);
            self.cancel_timer(timers.2);
        }
    }
    
    fn get_outgoing(&mut self) {
        for out_msg in self.paxos.get_outgoing_messages() {
            if out_msg.to == self.pid {
                self.paxos.handle(out_msg);
            } else {
                let receiver = self.peers.get(&out_msg.to).expect(&format!("Actorpath for node id: {} not found", &out_msg.to));
                receiver.tell((out_msg, PaxosSer), self);   // TODO trigger to replica and let replica respond to client instead?
            }
        }
    }

    fn get_decided(&mut self) {
        if self.pending_proposals.is_empty() {
           return;
        }
        for decided in self.paxos.get_decided_entries() {
            match decided {
                Entry::Normal(n) => {
                    let id = n.data.as_slice().get_u64();
                    if self.pending_proposals.remove(&id) {
                        let pr = ProposalResp::succeeded_normal(id);
                        self.cached_client.as_ref().expect("No cached client").tell((AtomicBroadcastMsg::ProposalResp(pr), AtomicBroadcastSer), self);
                    } else {
                        panic!("Got duplicate or not proposed response: {}", id);
                    }
                },
                Entry::StopSign(ss) => {
                    /*if self.responses.remove(&RECONFIG_ID) {
                        unimplemented!();
                    }*/
                    let final_seq = self.paxos.stop_and_get_sequence();
                    let (continued_nodes, new_nodes) = ss.nodes.iter().partition(
                        |&pid| pid == &self.pid || self.peers.contains_key(pid)
                    );
                    info!(self.ctx.log(), "Decided StopSign! Continued: {:?}, new: {:?}", &continued_nodes, &new_nodes);
                    let nodes = Reconfig::with(continued_nodes, new_nodes);
                    let r = FinalMsg::with(ss.config_id, nodes, final_seq);
                    self.supervisor.tell(ReplicaCompMsg::Reconfig(r));
                    let pr = ProposalResp::succeeded_normal(RECONFIG_ID);
                    self.cached_client.as_ref().expect("No cached client").tell((AtomicBroadcastMsg::ProposalResp(pr), AtomicBroadcastSer), self);
                }
            }
        }
    }

    fn propose(&mut self, p: Proposal) {
        if let None = self.cached_client {
            self.cached_client = Some(p.client);
        }
        match p.reconfig {
            Some((reconfig, _)) => {
                info!(self.ctx.log(), "Proposing reconfiguration: {:?}", reconfig);
                self.paxos.propose_reconfiguration(reconfig);
                self.pending_proposals.insert(RECONFIG_ID);
            }
            None => {
                let mut data: Vec<u8> = Vec::with_capacity(8);
                data.put_u64(p.id);
                self.paxos.propose_normal(data);
                self.pending_proposals.insert(p.id);
            }
        }
    }
}

impl<S, P> Actor for PaxosComp<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    type Message = PaxosCompMsg;

    fn receive_local(&mut self, msg: PaxosCompMsg) -> () {  // TODO change to AtomicBroadcastMsg to support SequenceReq
        match msg {
            PaxosCompMsg::Propose(p) => {
                self.propose(p);
            },
            PaxosCompMsg::SequenceReq(a) => {
                info!(self.ctx.log(), "Got SequenceReq");
                let (from_idx, to_idx) = a.request();
                let ser_entries = self.paxos.get_ser_entries(*from_idx, *to_idx);
                a.reply(ser_entries).expect("Failed to reply ReplicaComp's Ask with serialised entries");
            },
            PaxosCompMsg::Stop(s) => {
                self.stopped = true;
                self.stop_timers();
                s.cd.decrement().expect("Failed to countdown stop");
            }
        }
    }

    fn receive_network(&mut self, m: NetMessage) -> () {
        if self.stopped { return; }
        let NetMessage{sender: _, receiver: _, data} = m;
        match_deser!{data; {
            pm: RawPaxosMsg [PaxosSer] => {
                self.paxos.handle(pm);
            },
            !Err(e) => error!(self.ctx.log(), "Error deserialising msg: {:?}", e),
        }
        }
    }
}

impl<S, P> Provide<ControlPort> for PaxosComp<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    fn handle(&mut self, event: <ControlPort as Port>::Request) -> () {
        match event {
            ControlEvent::Start => {
                info!(self.ctx.log(), "PaxosComp started!");
                self.start_timers();
            },
            _ => {
                self.stop_timers();
            }
        }
    }
}

impl<S, P> Require<BallotLeaderElection> for PaxosComp<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    fn handle(&mut self, l: Leader) -> () {
        if !self.stopped {
            info!(self.ctx.log(), "{}", format!("Node {} became leader. Ballot: {:?}", l.pid, l.ballot));
            self.current_leader = l.pid;
            self.paxos.handle_leader(l);
        }
    }
}

pub mod raw_paxos{
    use super::super::messages::paxos::{*};
    use super::super::messages::paxos::ballot_leader_election::{Ballot, Leader};
    use super::super::storage::paxos::Storage;
    use super::{SequenceTraits, PaxosStateTraits};
    use std::fmt::Debug;
    use std::collections::HashMap;
    use std::mem;
    use std::sync::Arc;
    use crate::bench::atomic_broadcast::messages::{ProposalForward, Proposal};
    use kompact::prelude::Buf;

    pub struct Paxos<S, P> where
        S: SequenceTraits,
        P: PaxosStateTraits
    {
        storage: Storage<S, P>,
        config_id: u32,
        pub pid: u64,
        majority: usize,
        peers: Vec<u64>,    // excluding self pid
        state: (Role, Phase),
        leader: u64,
        n_leader: Ballot,
        promises: Vec<ReceivedPromise>,
        las: HashMap<u64, u64>,
        lds: HashMap<u64, u64>,
        proposals: Vec<Entry>,
        lc: u64,    // length of longest chosen seq
        decided: Vec<Entry>,
        outgoing: Vec<Message>,
        hb_forward: Vec<Entry>,
        num_proposed: u64,
    }

    impl<S, P> Paxos<S, P> where
        S: SequenceTraits,
        P: PaxosStateTraits
    {
        /*** User functions ***/
        pub fn with(
            config_id: u32,
            pid: u64,
            peers: Vec<u64>,
            storage: Storage<S, P>
        ) -> Paxos<S, P> {
            let num_nodes = &peers.len() + 1;
            let majority = num_nodes/2 + 1;
            let n_leader = Ballot::with(0, 0);
            Paxos {
                storage,
                pid,
                config_id,
                majority,
                peers,
                state: (Role::Follower, Phase::None),
                leader: 0,
                n_leader,
                promises: Vec::with_capacity(num_nodes),
                las: HashMap::with_capacity(num_nodes),
                lds: HashMap::with_capacity(num_nodes),
                proposals: vec![],
                lc: 0,
                decided: vec![],
                outgoing: vec![],
                hb_forward: vec![],
                num_proposed: 0,
            }
        }

        pub fn get_decided_entries(&mut self) -> Vec<Entry> {
            let decided_entries = mem::replace(&mut self.decided, vec![]);
            decided_entries
        }

        pub fn get_outgoing_messages(&mut self) -> Vec<Message> {
            let outgoing_msgs = mem::replace(&mut self.outgoing, vec![]);
            outgoing_msgs
        }

        pub fn handle(&mut self, m: Message) {
            match m.msg {
                PaxosMsg::Prepare(prep) => self.handle_prepare(prep, m.from),
                PaxosMsg::Promise(prom) => {
                    match &self.state {
                        (Role::Leader, Phase::Prepare) => self.handle_promise_prepare(prom, m.from),
                        (Role::Leader, Phase::Accept) => self.handle_promise_accept(prom, m.from),
                        _ => {
                            if !self.proposals.is_empty() {
                                let proposals = mem::replace(&mut self.proposals, vec![]);
                                // println!("Got promise but not leader... Forwarding proposals to {}", self.leader);
                                self.forward_proposals(proposals);
                            }
                        }
                    }
                },
                PaxosMsg::AcceptSync(acc_sync) => self.handle_accept_sync(acc_sync, m.from),
                PaxosMsg::Accept(acc) => self.handle_accept(acc, m.from),
                PaxosMsg::Accepted(accepted) => self.handle_accepted(accepted, m.from),
                PaxosMsg::Decide(d) => self.handle_decide(d),
                PaxosMsg::ProposalForward(proposals) => self.handle_forwarded_proposal(proposals),
            }
        }

        pub fn propose_normal(&mut self, data: Vec<u8>) {
            let normal_entry = Entry::Normal(self.create_normal_entry(data));
            match self.state {
                (Role::Leader, Phase::Prepare) => {
                    self.proposals.push(normal_entry);
                },
                (Role::Leader, Phase::Accept) => {
                    self.send_accept(normal_entry);
                },
                _ => {
                    self.forward_proposals(vec![normal_entry]);
                }
            }
        }

        pub fn propose_reconfiguration(&mut self, nodes: Vec<u64>) {
            let ss = StopSign::with(self.config_id + 1, nodes);
            let entry = Entry::StopSign(ss);
            match self.state {
                (Role::Leader, Phase::Prepare) => {
                    self.proposals.push(entry);
                },
                (Role::Leader, Phase::Accept) => {
                    self.send_accept(entry);
                },
                _ => {
                    self.forward_proposals(vec![entry])
                }
            }
        }

        pub fn get_sequence(&self) -> Vec<Entry> {
            self.storage.get_sequence()
        }

        pub fn get_ser_entries(&self, from_idx: u64, to_idx: u64) -> Option<Vec<u8>> {
            self.storage.get_ser_entries(from_idx, to_idx)
        }

        pub(crate) fn stop_and_get_sequence(&mut self) -> Arc<S> {
            self.storage.stop_and_get_sequence()
        }

        pub fn get_ld_la(&self) -> String { // TODO REMOVE
            let ld = self.storage.get_decided_len();
            let la = self.storage.get_sequence_len();
            format!("ld: {}, la: {}", ld, la)
        }

        fn create_normal_entry(&mut self, data: Vec<u8>) -> Normal {
            self.num_proposed += 1;
            let metadata = EntryMetaData::with(self.pid, self.num_proposed);
            Normal{ metadata, data }
        }

        fn clear_state(&mut self) {
            self.las.clear();
            self.promises.clear();
            self.lds.clear();
        }

        /*** Leader ***/
        pub fn handle_leader(&mut self, l: Leader) {
            let n = l.ballot;
            if n <= self.n_leader {
                return;
            }
            self.clear_state();
            if self.pid == l.pid {
                self.n_leader = n.clone();
                self.leader = n.pid;
                self.storage.set_promise(n.clone());
                /* insert my promise */
                let na = self.storage.get_accepted_ballot();
                let sfx = self.storage.get_decided_suffix();
                let rp = ReceivedPromise::with( na, sfx);
                self.promises.push(rp);
                /* insert my longest decided sequnce */
                let ld = self.storage.get_decided_len();
                self.lds.insert(self.pid, ld);
                /* initialise longest chosen sequence and update state */
                self.lc = 0;
                self.state = (Role::Leader, Phase::Prepare);
                if !self.hb_forward.is_empty(){
                    println!("Appending hb proposals, len: {}", self.hb_forward.len());
                    self.proposals.append(&mut self.hb_forward);
                }
                /* send prepare */
                for pid in &self.peers {
                    let prep = Prepare::with(n.clone(), ld, self.storage.get_accepted_ballot());
                    self.outgoing.push(Message::with(self.pid, *pid, PaxosMsg::Prepare(prep)));
                }
            } else {
                self.state.0 = Role::Follower;
                self.leader = n.pid;
                if !self.proposals.is_empty() {
                    let proposals = mem::replace(&mut self.proposals, vec![]);
                    // println!("Lost leadership: Forwarding proposals to {}", self.leader);
                    self.forward_proposals(proposals);
                }
                if !self.hb_forward.is_empty() {
                    let hb_proposals = mem::replace(&mut self.hb_forward, vec![]);
                    // println!("Lost leadership: Forwarding hb-proposals to {}", self.leader);
                    self.forward_proposals(hb_proposals);
                }
            }
        }

        fn forward_proposals(&mut self, proposals: Vec<Entry>) {
            if self.leader == 0 {
                let mut p = proposals;
                self.hb_forward.append(&mut p);
            } else {
                let pf = PaxosMsg::ProposalForward(proposals);
                let msg = Message::with(self.pid, self.leader, pf);
                self.outgoing.push(msg);
            }
        }

        fn handle_forwarded_proposal(&mut self, proposals: Vec<Entry>) {
            if !self.storage.stopped() {
                match self.state {
                    (Role::Leader, Phase::Prepare) => {
                        let mut p = proposals;
                        // println!("Appending forwarded proposals: len: {}, la: {}", p.len(), self.storage.get_sequence_len());
                        self.proposals.append(&mut p)
                    },
                    (Role::Leader, Phase::Accept) => {
                        // println!("Sending accept forwarded proposals: len: {}, la: {}", proposals.len(), self.storage.get_sequence_len());
                        for p in proposals {
                            self.send_accept(p);
                        }
                    }
                    _ => {
                        // println!("Not leader when receiving forwarded proposal... leader: {}", self.leader);
                        self.forward_proposals(proposals);
                    },
                }
            }
        }

        fn send_accept(&mut self, entry: Entry) {
            if !self.storage.stopped() {
                self.storage.append_entry(entry.clone(), true);
                self.las.insert(self.pid, self.storage.get_sequence_len());
                for pid in self.lds.keys() {
                    if pid != &self.pid {
                        let acc = Accept::with(self.n_leader.clone(), entry.clone());
                        self.outgoing.push(Message::with(self.pid, *pid, PaxosMsg::Accept(acc)));
                    }
                }
            }
        }

        fn handle_promise_prepare(&mut self, prom: Promise, from: u64) {
            if prom.n == self.n_leader {
                let rp = ReceivedPromise::with(prom.n_accepted, prom.sfx);
                self.promises.push(rp);
                self.lds.insert(from, prom.ld);
                if self.promises.len() >= self.majority {
                    let suffix = Self::max_value(&self.promises);
                    let last_is_stop = match suffix.last() {
                        Some(e) => e.is_stopsign(),
                        None => false
                    };
                    // println!("Prepare completed: suffix len={}, ld: {}, la before: {}", suffix.len(), self.storage.get_decided_len(), self.storage.get_sequence_len());
                    self.storage.append_on_decided_prefix(suffix);
                    if last_is_stop {
                        self.proposals = vec![];    // will never be decided
                    } else {
                        /*if !self.proposals.is_empty() {
                            // TODO remove
                            let first = if let Entry::Normal(first) = self.proposals.first().unwrap() {
                                first.data.as_slice().get_u64()
                            } else { 0 };
                            let last = if let Entry::Normal(last) = self.proposals.first().unwrap() {
                                last.data.as_slice().get_u64()
                            } else { 0 };
                            println!("Appending proposals id: {}-{}", first, last);
                        }*/
                        Self::drop_after_stopsign(&mut self.proposals); // drop after ss, if ss exists
                        let seq = mem::replace(&mut self.proposals, vec![]);
                        self.storage.append_sequence(seq);
                        self.las.insert(self.pid, self.storage.get_sequence_len());
                        self.state = (Role::Leader, Phase::Accept);
                    }
                    // println!("Sending AcceptSync la: {}", self.storage.get_sequence_len());
                    for (pid, lds) in self.lds.iter() {
//                        if *lds != va_len {
                        if pid != &self.pid {
                            let sfx = self.storage.get_suffix(*lds);
                            let acc_sync = AcceptSync::with(self.n_leader.clone(), sfx, *lds);
                            self.outgoing.push(Message::with(self.pid, *pid, PaxosMsg::AcceptSync(acc_sync)));
                        }
                    }
                }
            }
        }

        fn handle_promise_accept(&mut self, prom: Promise, from: u64) {
            if prom.n == self.n_leader {
                self.lds.insert(from, prom.ld);
                if prom.ld > self.storage.get_sequence_len() {
                    panic!(
                        "promise_accept from node {}. ld: {}, my seq_len: {}, my ld: {}",
                        from,
                        prom.ld,
                        self.storage.get_sequence_len(),
                        self.storage.get_decided_len()
                    );
                }
                let sfx = self.storage.get_suffix(prom.ld);
                let acc_sync = AcceptSync::with(self.n_leader.clone(), sfx, prom.ld);
                self.outgoing.push(Message::with(self.pid, from, PaxosMsg::AcceptSync(acc_sync)));
                if self.lc != 0 {
                    // inform what got decided already
                    let d = Decide::with(self.lc, self.n_leader.clone());
                    self.outgoing.push(Message::with(self.pid, from, PaxosMsg::Decide(d)));
                }
            }
        }

        fn handle_accepted(&mut self, accepted: Accepted, from: u64) {
            if accepted.n == self.n_leader && self.state == (Role::Leader, Phase::Accept) {
                self.las.insert(from, accepted.la);
                if &accepted.la <= &self.lc {
                    return;
                }
                let mut counter = 0;
                for (_pid, la) in &self.las {
                    if la >= &accepted.la {
                        counter += 1;
                        if counter == self.majority { break; }
                    }
                }
                if counter >= self.majority {
                    self.lc = accepted.la;
                    self.storage.set_pending_chosen_offset(self.lc);
                    let d = Decide::with(self.lc, self.n_leader.clone());
                    for pid in self.lds.keys() {
                        self.outgoing.push(Message::with(self.pid, *pid, PaxosMsg::Decide(d.clone())));
                    }
                }
            }
        }

        /*** Follower ***/
        fn handle_prepare(&mut self, prep: Prepare, from: u64) {
            if &self.storage.get_promise() < &prep.n {
                self.storage.set_promise(prep.n.clone());
                self.state = (Role::Follower, Phase:: Prepare);
                let na = self.storage.get_accepted_ballot();
                let suffix = if &na >= &prep.n_accepted {
                    if prep.ld > self.storage.get_sequence_len() {
                        panic!(
                            "ld in Prepare is longer than my accepted seq: ld: {}, la: {}, ballot: {:?}, my accepted ballot: {:?}",
                            prep.ld,
                            self.storage.get_sequence_len(),
                            prep.n_accepted,
                            na
                        )
                    }
                    self.storage.get_suffix(prep.ld)
                } else {
                    vec![]
                };
                let p = Promise::with(prep.n, na, suffix, self.storage.get_decided_len());
                self.outgoing.push(Message::with(self.pid, from, PaxosMsg::Promise(p)));
            }
        }

        fn handle_accept_sync(&mut self, acc_sync: AcceptSync, from: u64) {
            if self.state == (Role::Follower, Phase::Prepare) {
                if self.storage.get_promise() == acc_sync.n {
                    self.storage.set_accepted_ballot(acc_sync.n.clone());
                    let mut sfx = acc_sync.sfx;
                    let discarded_entries = self.storage.append_on_prefix(acc_sync.ld, &mut sfx);
                    self.state = (Role::Follower, Phase::Accept);
                    let accepted = Accepted::with(acc_sync.n, self.storage.get_sequence_len());
                    self.outgoing.push(Message::with(self.pid, from, PaxosMsg::Accepted(accepted)));
                    if !discarded_entries.is_empty() {  // forward the proposals that I failed to decide
                        let start = match discarded_entries.first().unwrap() {
                            Entry::Normal(n) => {
                                n.data.as_slice().get_u64()
                            },
                            _ => 0,
                        };
                        let end = match discarded_entries.last().unwrap() {
                            Entry::Normal(n) => {
                                n.data.as_slice().get_u64()
                            },
                            _ => 0,
                        };
                        println!("UNEMPTY DISCARDED ENTRIES: {}: {}-{}. Forwarding them to {}", discarded_entries.len(), start, end, self.leader);
                        self.forward_proposals(discarded_entries);
                    }
                }
            }
        }

        fn handle_accept(&mut self, acc: Accept, from: u64) {
            if self.state == (Role::Follower, Phase::Accept) {
                if self.storage.get_promise() == acc.n {
                    self.storage.append_entry(acc.entry, false);
                    let accepted = Accepted::with(acc.n, self.storage.get_sequence_len());
                    self.outgoing.push(Message::with(self.pid, from, PaxosMsg::Accepted(accepted)));
                }
            }
        }

        fn handle_decide(&mut self, dec: Decide) {
            if self.storage.get_promise() == dec.n {
                let prev_ld = self.storage.set_decided_len(dec.ld);
                // println!("Deciding: prev_ld: {}, ld: {}", prev_ld, dec.ld);
                if prev_ld < dec.ld {
                    let mut decided_entries = self.storage.get_entries(prev_ld, dec.ld);
                    self.decided.append(&mut decided_entries);
                }
            }
        }

        /*** algorithm specific functions ***/
        fn max_value(promises: &Vec<ReceivedPromise>) -> Vec<Entry> {
            let mut max_n: &Ballot = &promises[0].n_accepted;
            let mut max_sfx: &Vec<Entry> = &promises[0].sfx;
            for p in promises {
                if &p.n_accepted > max_n {
                    max_n = &p.n_accepted;
                    max_sfx = &p.sfx;
                } else if &p.n_accepted == max_n && &p.sfx.len() > &max_sfx.len() {
                    max_sfx = &p.sfx;
                }
            }
            max_sfx.clone()
        }

        fn drop_after_stopsign(entries: &mut Vec<Entry>) {   // drop all entries ordered after stopsign (if any)
            for (idx, e) in entries.iter().enumerate() {
                if e.is_stopsign() {
                    entries.truncate(idx + 1);
                    return;
                }
            }
        }
    }

    #[derive(PartialEq, Debug)]
    enum Phase {
        Prepare,
        Accept,
        None
    }

    #[derive(PartialEq, Debug)]
    enum Role {
        Follower,
        Leader
    }

    struct ReceivedPromise {
        n_accepted: Ballot,
        sfx: Vec<Entry>
    }

    impl ReceivedPromise {
        fn with(n_accepted: Ballot, sfx: Vec<Entry>) -> ReceivedPromise {
            ReceivedPromise { n_accepted, sfx }
        }
    }

    #[derive(Clone, Debug)]
    pub struct StopSign {
        pub config_id: u32,
        pub nodes: Vec<u64>,
    }

    impl StopSign {
        pub fn with(config_id: u32, nodes: Vec<u64>) -> StopSign {
            StopSign{ config_id, nodes }
        }
    }

    impl PartialEq for StopSign {
        fn eq(&self, other: &Self) -> bool {
            unimplemented!()
        }

        fn ne(&self, other: &Self) -> bool {
            unimplemented!()
        }
    }

    #[derive(Clone, Debug)]
    pub struct Normal {
        pub metadata: EntryMetaData,
        pub data: Vec<u8>,
    }

    impl PartialEq for Normal {
        fn eq(&self, other: &Self) -> bool { self.metadata == other.metadata }

        fn ne(&self, other: &Self) -> bool { self.metadata != other.metadata }
    }

    impl Normal {
        pub fn with(metadata: EntryMetaData, data: Vec<u8>) -> Normal {
            Normal{ metadata, data }
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    pub struct EntryMetaData {
        pub proposed_by: u64,
        pub n: u64,
    }

    impl EntryMetaData {
        pub fn with(proposed_by: u64, n: u64) -> EntryMetaData {
            EntryMetaData{ proposed_by, n }
        }
    }


    #[derive(Clone, Debug, PartialEq)]
    pub enum Entry {
        Normal(Normal),
        StopSign(StopSign)
    }

    impl Entry {
        pub(crate) fn is_stopsign(&self) -> bool {
            match self {
                Entry::StopSign(_) => true,
                _ => false
            }
        }
    }
}

mod ballot_leader_election {
    use super::*;
    use super::super::messages::{paxos::ballot_leader_election::*};
    use std::time::Duration;

    pub struct BallotLeaderElection;

    impl Port for BallotLeaderElection {
        type Indication = Leader;
        type Request = ();
    }

    #[derive(ComponentDefinition)]
    pub struct BallotLeaderComp {   // TODO decouple from kompact, similar style to tikv_raft with tick() replacing timers
        ctx: ComponentContext<Self>,
        ble_port: ProvidedPort<BallotLeaderElection, Self>,
        pid: u64,
        peers: Vec<ActorPath>,
        round: u64,
        ballots: Vec<(u64, Ballot)>,
        current_ballot: Ballot,  // (round, pid)
        leader: Option<(u64, Ballot)>,
        max_ballot: Ballot,
        hb_delay: u64,
        delta: u64,
        majority: usize,
        stopped: bool,
        timer: Option<ScheduledTimer>
    }

    impl BallotLeaderComp {
        pub fn with(peers: Vec<ActorPath>, pid: u64, delta: u64) -> BallotLeaderComp {
            let n = &peers.len() + 1;
            BallotLeaderComp {
                ctx: ComponentContext::new(),
                ble_port: ProvidedPort::new(),
                pid,
                majority: n/2 + 1, // +1 because peers is exclusive ourselves
                peers,
                round: 0,
                ballots: Vec::with_capacity(n),
                current_ballot: Ballot::with(0, pid),
                leader: None,
                max_ballot: Ballot::with(0, pid),
                hb_delay: delta,
                delta,
                stopped: false,
                timer: None
            }
        }

        fn max_by_ballot(ballots: Vec<(u64, Ballot)>) -> (u64, Ballot) {
            let mut top = ballots[0];
            for ballot in ballots {
                if ballot.1 > top.1 {
                    top = ballot;
                } else if ballot.1 == top.1 && ballot.0 > top.0 {   // use pid to tiebreak
                    top = ballot;
                }
            }
            top
        }

        fn check_leader(&mut self) {
            self.ballots.push((self.pid, self.current_ballot));
            let ballots = std::mem::replace(&mut self.ballots, Vec::with_capacity(self.majority * 2));
            let (top_pid, top_ballot) = Self::max_by_ballot(ballots);
            if top_ballot < self.max_ballot {
                self.current_ballot.n = self.max_ballot.n + 1;
                self.leader = None;
            } else {
                if self.leader != Some((top_pid, top_ballot)) {
                    self.max_ballot = top_ballot;
                    self.leader = Some((top_pid, top_ballot));
                    self.ble_port.trigger(Leader::with(top_pid, top_ballot));
                }
            }
        }

        fn hb_timeout(&mut self) {
            if self.ballots.len() + 1 >= self.majority {
                self.check_leader();
            }
            self.round += 1;
            for peer in &self.peers {
                let hb_request = HeartbeatRequest::with(self.round, self.max_ballot);
                peer.tell((HeartbeatMsg::Request(hb_request), BallotLeaderSer), self);
            }
        }

        fn start_timer(&mut self) {
            let timer = self.schedule_periodic(
                Duration::from_millis(0),
                Duration::from_millis(self.hb_delay),
                move |c, _| c.hb_timeout()
            );
            self.timer = Some(timer);
        }

        fn stop_timer(&mut self) {
            if let Some(timer) = self.timer.take() {
                self.cancel_timer(timer);
            }
        }
    }

    impl Provide<ControlPort> for BallotLeaderComp {
        fn handle(&mut self, event: <ControlPort as Port>::Request) -> () {
            // ignore
            match event {
                ControlEvent::Start => {
                    info!(self.ctx.log(), "BLE started!");
                    self.start_timer();
                },
                _ => self.stop_timer(),
            }
        }
    }

    impl Provide<BallotLeaderElection> for BallotLeaderComp {
        fn handle(&mut self, _: <BallotLeaderElection as Port>::Request) -> () {
            unimplemented!()
        }
    }

    impl Actor for BallotLeaderComp {
        type Message = Stop;

        fn receive_local(&mut self, msg: Stop) -> () {
            msg.cd.decrement().expect("Failed to countdown stop latch");
        }

        fn receive_network(&mut self, m: NetMessage) -> () {
            if self.stopped { return; }
            let NetMessage{sender, receiver: _, data} = m;
            match_deser!{data; {
                hb: HeartbeatMsg [BallotLeaderSer] => {
                    match hb {
                        HeartbeatMsg::Request(req) => {
                            if req.max_ballot > self.max_ballot {
                                self.max_ballot = req.max_ballot;
                            }
                            let hb_reply = HeartbeatReply::with(self.pid, req.round, self.current_ballot);
                            sender.tell((HeartbeatMsg::Reply(hb_reply), BallotLeaderSer), self);
                        },
                        HeartbeatMsg::Reply(rep) => {
                            if rep.round == self.round {
                                self.ballots.push((rep.sender_pid, rep.max_ballot));
                            } else {
                                self.stop_timer();
                                self.hb_delay += self.delta;
                                self.start_timer();
                            }
                        }
                    }
                },
                !Err(e) => error!(self.ctx.log(), "Error deserialising msg: {:?}", e),
            }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::client::{Client, tests::TestClient};
    use crate::partitioning_actor::{PartitioningActor, IterationControlMsg};
    use synchronoise::CountdownEvent;
    use std::sync::Arc;
    use super::super::messages::Run;
    use crate::bench::atomic_broadcast::paxos::raw_paxos::{EntryMetaData, Normal};

    fn create_replica_nodes(n: u64, initial_conf: Vec<u64>) -> (Vec<KompactSystem>, HashMap<u64, ActorPath>) {
        let mut systems = vec![];
        let mut nodes = HashMap::new();
        for i in 1..=n {
            let system =
                kompact_benchmarks::kompact_system_provider::global().new_remote_system_with_threads(format!("paxos_replica{}", i), 4);
            let (replica_comp, unique_reg_f) = system.create_and_register(|| {
                ReplicaComp::<MemorySequence, MemoryState>::with(initial_conf.clone(), TransferPolicy::Pull)
            });
            unique_reg_f.wait_expect(
                Duration::from_millis(1000),
                "ReplicaComp failed to register!",
            );
            let replica_comp_f = system.start_notify(&replica_comp);
            replica_comp_f
                .wait_timeout(Duration::from_millis(1000))
                .expect("ReplicaComp never started!");

            let named_reg_f = system.register_by_alias(
                &replica_comp,
                format!("replica{}", i),
            );
            let self_path = ActorPath::Named(NamedPath::with_system(
                system.system_path(),
                vec![format!("replica{}", i).into()],
            ));
            systems.push(system);
            nodes.insert(i, self_path);

        }
        (systems, nodes)
    }

    #[test]
    fn paxos_test() {
        let n: u64 = 3;
        let active_n: u64 = 3;
        let quorum = active_n/2 + 1;
        let num_proposals = 4000;
        let batch_size = 1000;
        let config = vec![1,2,3];
        let reconfig = None;
        // let reconfig = Some((vec![1,4,5], vec![]));
        let check_sequences = false;

        let (systems, nodes) = create_replica_nodes(n, config);
        let mut actorpaths = vec![];
        nodes.iter().for_each(|x| actorpaths.push(x.1.clone()));
        /*** Setup partitioning actor ***/
        let prepare_latch = Arc::new(CountdownEvent::new(1));
        let (partitioning_actor, unique_reg_f) = systems[0].create_and_register(|| {
            PartitioningActor::with(
                prepare_latch.clone(),
                None,
                1,
                actorpaths,
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
        /*** Setup client ***/
        let (p, f) = kpromise::<HashMap<u64, Vec<u64>>>();
        let (client, unique_reg_f) = systems[0].create_and_register( || {
            TestClient::with(
                num_proposals,
                batch_size,
                nodes,
                reconfig,
                p,
                check_sequences,
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
            if !found {
                panic!("Did not find value {} in client's sequence", i);
            }
        }

        if check_sequences {
            let mut counter = 0;
            for i in 1..=n {
                let sequence = all_sequences.get(&i).expect(&format!("Did not get sequence for node {}", i));
                println!("Node {}: {:?}", i, sequence.len());
                assert!(client_sequence.starts_with(sequence));
                if sequence.starts_with(&client_sequence) {
                    counter += 1;
                }
            }
            if counter < quorum {
                panic!("Majority should have decided sequence: counter: {}, quorum: {}", counter, quorum);
            }
        }
        println!("PASSED!!!");
    }

    #[test]
    fn reconfig_ser_des_test(){
        let config_id = 1;
        let from_idx = 2;
        let to_idx = 3;
        let tag = 4;
        let continued_nodes = vec![5,6,7];
        let new_nodes = vec![8,9,10];
        let reconfig = Reconfig::with(continued_nodes.clone(), new_nodes.clone());
        let len = 8;
        let metadata = SequenceMetaData::with(config_id, len);
        let req_init = ReconfigInit::with(config_id, reconfig.clone(), metadata.clone());
        let mut bytes: Vec<u8> = vec![];
        ReconfigSer.serialise(&ReconfigurationMsg::Init(req_init), &mut bytes).expect("Failed to serialise ReconfigInit");
        let mut buf = bytes.as_slice();
        if let Ok(ReconfigurationMsg::Init(des)) = ReconfigSer::deserialise(&mut buf) {
            assert_eq!(des.config_id, config_id);
            assert_eq!(des.nodes.continued_nodes, continued_nodes);
            assert_eq!(des.nodes.new_nodes, new_nodes);
            assert_eq!(des.seq_metadata.config_id, metadata.config_id);
            assert_eq!(des.seq_metadata.len, metadata.len);
        }
        let seq_ser = vec![1,2,3];
        let st = SequenceResponse::with(config_id, tag, from_idx, to_idx, seq_ser.clone(), metadata.clone());
        bytes.clear();
        ReconfigSer.serialise(&ReconfigurationMsg::SequenceResponse(st), &mut bytes).expect("Failed to serialise SequenceTransfer");
        buf = bytes.as_slice();
        if let Ok(ReconfigurationMsg::SequenceResponse(des)) = ReconfigSer::deserialise(&mut buf) {
            assert_eq!(des.config_id, config_id);
            assert_eq!(des.tag, tag);
            assert_eq!(des.ser_entries, seq_ser);
            assert_eq!(des.metadata.config_id, metadata.config_id);
            assert_eq!(des.metadata.len, metadata.len);
        }
    }
}
