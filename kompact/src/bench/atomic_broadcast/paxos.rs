use kompact::prelude::*;
use super::storage::paxos::*;
use std::fmt::{Debug};
use ballot_leader_election::{BallotLeaderComp, BallotLeaderElection};
use raw_paxos::{Entry, Paxos};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use super::messages::{TestMessage, TestMessageSer, SequenceResp, paxos::ballot_leader_election::Leader};
use crate::bench::atomic_broadcast::messages::paxos::{PaxosSer, ReconfigInit, ReconfigSer, SequenceTransfer, SequenceRequest, SequenceMetaData, Reconfig, ReconfigurationMsg};
use crate::bench::atomic_broadcast::messages::{Proposal, ProposalResp, AtomicBroadcastMsg, AtomicBroadcastSer, RECONFIG_ID};
use crate::partitioning_actor::{PartitioningActorSer, PartitioningActorMsg, Init};
use uuid::Uuid;
use kompact::prelude::Buf;
use rand::Rng;
use crate::bench::atomic_broadcast::communicator::{CommunicationPort, AtomicBroadcastCompMsg, CommunicatorMsg, Communicator};

const BLE: &str = "ble";
const COMMUNICATOR: &str = "communicator";

const DELTA: u64 = 300;
const TRANSFER_TIMEOUT: Duration = Duration::from_millis(200);
const INITIAL_CAPACITY: usize = 1000;

pub trait SequenceTraits: Sequence + Debug + Send + Sync + 'static {}
pub trait PaxosStateTraits: PaxosState + Send + 'static {}

#[derive(Debug)]
pub struct FinalMsg<S> where S: SequenceTraits {
    pub config_id: u32,
    pub nodes: Reconfig,
    pub final_sequence: Arc<S>,
}

impl<S> FinalMsg<S> where S: SequenceTraits {
    pub fn with(config_id: u32, nodes: Reconfig, final_sequence: Arc<S>) -> FinalMsg<S> {
        FinalMsg { config_id, nodes, final_sequence }
    }
}

#[derive(Debug)]
pub enum PaxosCompMsg {
    Propose(Proposal),
    SequenceReq(Ask<(u64, u64), Option<Vec<u8>>>),
    GetAllEntries(Ask<(), Vec<Entry>>)
}

#[derive(Clone, Debug)]
pub enum TransferPolicy {
    Eager,
    Pull
}

#[derive(ComponentDefinition)]
pub struct PaxosReplica<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    ctx: ComponentContext<Self>,
    pid: u64,
    initial_config: Vec<u64>,
    paxos_comps: HashMap<u32, Arc<Component<PaxosComp<S, P>>>>,
    ble_communicator_comps: HashMap<u32, (Arc<Component<BallotLeaderComp>>, Arc<Component<Communicator>>)>,
    active_config_id: u32,
    nodes: HashMap<u64, ActorPath>, // derive actorpaths of peers' ble and paxos replicas from these
    prev_sequences: HashMap<u32, Arc<S>>,
    stopped: bool,
    iteration_id: u32,
    partitioning_actor: Option<ActorPath>,
    alias_registrations: HashSet<Uuid>,
    policy: TransferPolicy,
    next_config_id: u32,
    pending_seq_transfers: HashMap<u32, (u32, HashMap<u32, Vec<Entry>>)>,   // <config_id, (num_segments, <segment_id, entries>)
    complete_sequences: HashSet<u32>,
    active_peers: (HashSet<u64>, HashSet<u64>), // (ready, not_ready)
    forward_discarded: bool
}

impl<S, P> PaxosReplica<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    pub fn with(initial_config: Vec<u64>, policy: TransferPolicy, forward_discarded: bool) -> PaxosReplica<S, P> {
        PaxosReplica {
            ctx: ComponentContext::new(),
            pid: 0,
            initial_config,
            paxos_comps: HashMap::new(),
            ble_communicator_comps: HashMap::new(),
            active_config_id: 0,
            nodes: HashMap::new(),
            prev_sequences: HashMap::new(),
            stopped: false,
            iteration_id: 0,
            partitioning_actor: None,
            alias_registrations: HashSet::new(),
            policy,
            next_config_id: 0,
            pending_seq_transfers: HashMap::new(),
            complete_sequences: HashSet::new(),
            active_peers: (HashSet::new(), HashSet::new()),
            forward_discarded,
        }
    }

    fn create_replica(&mut self, config_id: u32, nodes: Vec<u64>, register_alias_with_response: bool) {
        let num_peers = nodes.len() - 1;
        let mut paxos_peers = HashSet::with_capacity(num_peers);
        let mut communicator_peers = HashMap::with_capacity(num_peers);
        let mut ble_peers = HashSet::with_capacity(num_peers);
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
                        let named_communicator = NamedPath::new(
                            protocol,
                            addr.clone(),
                            port,
                            vec![format!("{}{},{}-{}", COMMUNICATOR, pid, config_id, self.iteration_id).into()]
                        );
                        let named_ble = NamedPath::new(
                            protocol,
                            addr.clone(),
                            port,
                            vec![format!("{}{},{}-{}", BLE, pid, config_id, self.iteration_id).into()]
                        );
                        paxos_peers.insert(pid);
                        communicator_peers.insert(pid, ActorPath::Named(named_communicator));
                        ble_peers.insert(ActorPath::Named(named_ble));
                    },
                    _ => error!(self.ctx.log(), "{}", format!("Actorpath is not named for node {}", pid)),
                }
            }
        }
        let system = self.ctx.system();
        /*** create and register Paxos ***/
        let paxos_comp = system.create(|| {
            PaxosComp::with(self.ctx.actor_ref(), paxos_peers, config_id, self.pid, self.forward_discarded)
        });
        system.register_without_response(&paxos_comp);
        /*** create and register Communicator ***/
        let communicator = system.create( || {
            Communicator::with(communicator_peers)
        });
        system.register_without_response(&communicator);
        /*** create and register BLE ***/
        let ble_comp = system.create( || {
            BallotLeaderComp::with(ble_peers, self.pid, DELTA)
        });
        system.register_without_response(&ble_comp);
        let communicator_alias = format!("{}{},{}-{}", COMMUNICATOR, self.pid, config_id, self.iteration_id);
        let ble_alias = format!("{}{},{}-{}", BLE, self.pid, config_id, self.iteration_id);
        if register_alias_with_response {
            let comm_alias_id = system.register_by_alias(&communicator, communicator_alias, self);
            let ble_alias_id = system.register_by_alias(&ble_comp, ble_alias, self);
            self.alias_registrations.insert(comm_alias_id.0);
            self.alias_registrations.insert(ble_alias_id.0);
        } else {
            system.register_by_alias_without_response(&communicator, communicator_alias);
            system.register_by_alias_without_response(&ble_comp, ble_alias);
        }
        /*** connect components ***/
        biconnect_components::<CommunicationPort, _, _>(&communicator, &paxos_comp)
            .expect("Could not connect Communicator and PaxosComp!");

        biconnect_components::<BallotLeaderElection, _, _>(&ble_comp, &paxos_comp)
            .expect("Could not connect BLE and PaxosComp!");

        self.paxos_comps.insert(config_id, paxos_comp);
        self.ble_communicator_comps.insert(config_id, (ble_comp, communicator));
    }

    fn start_replica(&mut self, config_id: u32) {
        debug!(self.ctx.log(), "Starting replica pid: {}, config_id: {}", self.pid, self.next_config_id);
        self.active_config_id = config_id;
        if let Some(paxos) = self.paxos_comps.get(&config_id) {
            self.ctx.system().start(paxos);
        }
        if let Some((ble, communicator)) = self.ble_communicator_comps.get(&config_id) {
            self.ctx.system().start(communicator);
            self.ctx.system().start(ble);
        }
    }

    fn kill_all_replicas(&mut self) {
        for (_, (ble, communicator)) in self.ble_communicator_comps.drain() {
            self.ctx.system().kill(ble);
            self.ctx.system().kill(communicator);
        }
        for (_, paxos) in self.paxos_comps.drain() {
            self.ctx.system().kill(paxos);
        }
    }

    fn new_iteration(&mut self, init: Init) {
        let nodes = init.nodes;
        self.active_config_id = 0;
        self.next_config_id = 0;
        self.stopped = false;
        self.prev_sequences = HashMap::new();
        self.pid = init.pid as u64;
        self.iteration_id = init.init_id;
        for (id, actorpath) in nodes.into_iter().enumerate() {
            self.nodes.insert(id as u64 + 1, actorpath);
        }
        if self.initial_config.contains(&self.pid){
            self.next_config_id = 1;
            self.create_replica(self.next_config_id, self.initial_config.clone(), true);
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
        }
        self.schedule_once(TRANSFER_TIMEOUT, move |c, _| c.retry_request_sequence(config_id, seq_len));
    }

    fn request_sequence(&self, pid: u64, config_id: u32, from_idx: u64, to_idx: u64, tag: u32) {
        let sr = SequenceRequest::with(config_id, tag, from_idx, to_idx);
        self.nodes
            .get(&pid)
            .expect(&format!("Failed to get Actorpath of node {}", pid))
            .tell((ReconfigurationMsg::SequenceRequest(sr), ReconfigSer), self);
    }

    fn retry_request_sequence(&mut self, config_id: u32, seq_len: u64) {
        if self.active_config_id >= config_id {
            return;
        }
        let transfer = self.pending_seq_transfers.get(&config_id).expect("Replica should have prepared for receiving eager transfers");
        let num_segments = transfer.0;
        let received_segments = &transfer.1;
        let offset = seq_len/num_segments as u64;
        let num_ready_peers = self.active_peers.0.len();
        for i in 0..num_segments {
            let tag = i + 1;
            if !received_segments.contains_key(&tag) {    // missing segment
                let from_idx = i as u64 * offset;
                let to_idx = from_idx + offset;
                let pid = self.active_peers.0.iter().nth(i as usize % num_ready_peers).unwrap();
                self.request_sequence(*pid, config_id, from_idx, to_idx, tag);
            }
        }
        self.schedule_once(TRANSFER_TIMEOUT, move |c, _| c.retry_request_sequence(config_id, seq_len));
    }

    fn get_sequence_metadata(&self, config_id: u32) -> SequenceMetaData {
        let seq_len = match self.prev_sequences.get(&config_id) {
            Some(prev_seq) => {
                prev_seq.get_sequence_len()
            },
            None => 0,
        };
        SequenceMetaData::with(config_id, seq_len)
    }

    fn create_eager_sequence_transfer(&self, continued_nodes: &Vec<u64>, config_id: u32) -> SequenceTransfer {
        let index = continued_nodes.iter().position(|pid| pid == &self.pid).expect("Could not find my pid in continued_nodes");
        let tag = index as u32 + 1;
        let n_continued = continued_nodes.len();
        let final_seq = self.prev_sequences.get(&config_id).expect("Should have final sequence");
        let seq_len = final_seq.get_sequence_len();
        let offset = seq_len/n_continued as u64;
        let from_idx = index as u64 * offset;
        let to_idx = from_idx + offset;
        let ser_entries = final_seq.get_ser_entries(from_idx, to_idx).expect("Should have entries of final sequence");
        let prev_seq_metadata = self.get_sequence_metadata(config_id-1);
        let st = SequenceTransfer::with(config_id, tag, true, from_idx, to_idx, ser_entries, prev_seq_metadata);
        st
    }

    fn handle_sequence_request(&self, sr: SequenceRequest, requestor: ActorPath) {
        let (succeeded, ser_entries) = match self.prev_sequences.get(&sr.config_id) {
            Some(seq) => {
                let entries = seq.get_ser_entries(sr.from_idx, sr.to_idx).expect("Should have all entries in final sequence!");
                (true, entries)
            },
            None => {
                if self.active_config_id == sr.config_id {
                    let paxos = self.paxos_comps.get(&sr.config_id).unwrap();
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
        let prev_seq_metadata = self.get_sequence_metadata(sr.config_id-1);
        let st = SequenceTransfer::with(sr.config_id, sr.tag, succeeded, sr.from_idx, sr.to_idx, ser_entries, prev_seq_metadata);
        requestor.tell((ReconfigurationMsg::SequenceTransfer(st), ReconfigSer), self);
    }

    fn handle_sequence_transfer(&mut self, st: SequenceTransfer) {
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
                            self.start_replica(self.next_config_id);
                        }
                    }
                },
                None => panic!("{}", &format!("Got unexpected sequence transfer config_id: {}, tag: {}, index: {}-{}", st.config_id, st.tag, st.from_idx, st.to_idx)),
            }
        } else {    // failed sequence transfer i.e. not reached final seq yet
            let config_id = st.config_id;
            let tag = st.tag;
            let from_idx = st.from_idx;
            let to_idx = st.to_idx;
            // query randomly someone we know have reached final seq
            let num_active = self.active_peers.0.len();
            let mut rng = rand::thread_rng();
            let rnd = rng.gen_range(1, num_active);
            let pid = self.active_peers.0.iter().nth(rnd).expect("Failed to get random active pid");
            self.request_sequence(*pid, config_id, from_idx, to_idx, tag);
        }
    }
}

impl<S, P> Provide<ControlPort> for PaxosReplica<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    fn handle(&mut self, _: <ControlPort as Port>::Request) -> () {
        // ignore
    }
}

#[derive(Debug)]
pub enum PaxosReplicaMsg<S> where S: SequenceTraits{
    Reconfig(FinalMsg<S>),
    RegResp(RegistrationResponse)
}

impl<S> From<RegistrationResponse> for PaxosReplicaMsg<S> where S: SequenceTraits {
    fn from(rr: RegistrationResponse) -> Self {
        PaxosReplicaMsg::RegResp(rr)
    }
}

impl<S, P> Actor for PaxosReplica<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    type Message = PaxosReplicaMsg<S>;

    fn receive_local(&mut self, msg: Self::Message) -> () {
        match msg {
            PaxosReplicaMsg::Reconfig(r) => {
                let prev_config_id = self.active_config_id;
                // if r.nodes.continued_nodes.contains(&self.pid) {
                //     self.next_config_id = r.config_id;
                // }
                let final_seq_len: u64 = r.final_sequence.get_sequence_len();
                let seq_metadata = SequenceMetaData::with(prev_config_id, final_seq_len);
                self.prev_sequences.insert(prev_config_id, r.final_sequence);
                let r_init = ReconfigurationMsg::Init(ReconfigInit::with(r.config_id, r.nodes.clone(), seq_metadata, self.pid));
                for pid in &r.nodes.new_nodes {
                    if pid != &self.pid {
                        info!(self.ctx.log(), "Sending ReconfigInit to node {}", pid);
                        let actorpath = self.nodes.get(pid).expect("No actorpath found for new node");
                        actorpath.tell((r_init.clone(), ReconfigSer), self);
                    }
                }
                if let TransferPolicy::Eager = self.policy {
                    let st = self.create_eager_sequence_transfer(&r.nodes.continued_nodes, prev_config_id);
                    for pid in &r.nodes.new_nodes {
                        let actorpath = self.nodes.get(pid).expect("No actorpath found for new node");
                        actorpath.tell((ReconfigurationMsg::SequenceTransfer(st.clone()), ReconfigSer), self);
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
            PaxosReplicaMsg::RegResp(rr) => {
                self.alias_registrations.remove(&rr.id.0);
                if self.alias_registrations.is_empty() {
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
                        debug!(self.ctx.log(), "{}", format!("Got init! My pid: {}", init.pid));
                        self.partitioning_actor = Some(sender);
                        self.new_iteration(init);
                    },
                    PartitioningActorMsg::Run => {
                        self.start_replica(self.next_config_id);
                    },
                    PartitioningActorMsg::Stop => {
                        self.kill_all_replicas();
                        debug!(self.ctx.log(), "Stopped all child components");
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
                            debug!(self.ctx.log(), "Got ReconfigInit for config_id: {} from node {}", r.config_id, r.from);
                            self.next_config_id = r.config_id;
                            for pid in &r.nodes.continued_nodes {
                                if pid == &r.from {
                                    self.active_peers.0.insert(*pid);
                                } else {
                                    self.active_peers.1.insert(*pid);
                                }
                            }
                            match self.policy {
                                TransferPolicy::Pull => self.pull_previous_sequence(r.seq_metadata.config_id, r.seq_metadata.len),
                                TransferPolicy::Eager => {
                                    let n = r.nodes.continued_nodes.len();
                                    let config_id = r.seq_metadata.config_id;
                                    self.pending_seq_transfers.insert(r.seq_metadata.config_id, (n as u32, HashMap::with_capacity(n)));
                                    let seq_len = r.seq_metadata.len;
                                    self.schedule_once(TRANSFER_TIMEOUT, move |c, _| c.retry_request_sequence(config_id, seq_len));
                                }
                            }
                            let mut nodes = r.nodes.continued_nodes;
                            let mut new_nodes = r.nodes.new_nodes;
                            nodes.append(&mut new_nodes);
                            self.create_replica(r.config_id, nodes, false);
                        } else if self.next_config_id == r.config_id {
                            if r.nodes.continued_nodes.contains(&r.from) {
                                // update who we know already decided final seq
                                self.active_peers.1.remove(&r.from);
                                self.active_peers.0.insert(r.from);
                            }
                        }
                    },
                    ReconfigurationMsg::SequenceRequest(sr) => {
                        self.handle_sequence_request(sr, sender);
                    },
                    ReconfigurationMsg::SequenceTransfer(st) => {
                        self.handle_sequence_transfer(st);
                    }
                }
            },
            am: AtomicBroadcastMsg [AtomicBroadcastSer] => {
                match am {
                    AtomicBroadcastMsg::Proposal(p) => {
                        if !self.stopped {
                            if let Some(active_paxos) = &self.paxos_comps.get(&self.active_config_id) {
                                active_paxos.actor_ref().tell(PaxosCompMsg::Propose(p));
                            }
                        }
                    },
                    _ => unimplemented!(),
                }
            },
            tm: TestMessage [TestMessageSer] => {
                match tm {
                    TestMessage::SequenceReq => {
                        let mut all_entries = vec![];
                        let mut unique = HashSet::new();
                        for i in 1..self.active_config_id {
                            if let Some(seq) = self.prev_sequences.get(&i) {
                                let sequence = seq.get_sequence();
                                for entry in sequence {
                                    if let Entry::Normal(n) = entry {
                                        let id = n.data.as_slice().get_u64();
                                        all_entries.push(id);
                                        unique.insert(id);
                                    }
                                }
                            }
                        }
                        if let Some(active_paxos) = self.paxos_comps.get(&self.active_config_id) {
                            let sequence = active_paxos.actor_ref().ask(|promise| PaxosCompMsg::GetAllEntries(Ask::new(promise, ()))).wait();
                            for entry in sequence {
                                if let Entry::Normal(n) = entry {
                                    let id = n.data.as_slice().get_u64();
                                    all_entries.push(id);
                                    unique.insert(id);
                            }
                        }
                        info!(self.ctx.log(), "Got SequenceReq: my seq_len: {}, unique: {}", all_entries.len(), unique.len());
                        let sr = SequenceResp::with(self.pid, all_entries);
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

#[derive(ComponentDefinition)]
struct PaxosComp<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    ctx: ComponentContext<Self>,
    supervisor: ActorRef<PaxosReplicaMsg<S>>,
    communication_port: RequiredPort<CommunicationPort, Self>,
    ble_port: RequiredPort<BallotLeaderElection, Self>,
    peers: HashSet<u64>,
    paxos: Paxos<S, P>,
    pid: u64,
    current_leader: u64,
    stopped: bool,
    timers: Option<(ScheduledTimer, ScheduledTimer)>,
    cached_client: bool,
}

impl<S, P> PaxosComp<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    fn with(
        supervisor: ActorRef<PaxosReplicaMsg<S>>,
        peers: HashSet<u64>,
        config_id: u32,
        pid: u64,
        forward_discarded: bool
    ) -> PaxosComp<S, P>
    {
        let seq = S::new();
        let paxos_state = P::new();
        let storage = Storage::with(seq, paxos_state);
        let paxos = Paxos::with(config_id, pid, peers.clone(), storage, forward_discarded);
        PaxosComp {
            ctx: ComponentContext::new(),
            supervisor,
            communication_port: RequiredPort::new(),
            ble_port: RequiredPort::new(),
            peers,
            paxos,
            pid,
            current_leader: 0,
            stopped: false,
            timers: None,
            cached_client: false,
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
            move |p, _| p.send_outgoing()
        );
        self.timers = Some((decided_timer, outgoing_timer));
    }

    fn stop_timers(&mut self) {
        if let Some(timers) = self.timers.take() {
            self.cancel_timer(timers.0);
            self.cancel_timer(timers.1);
        }
    }
    
    fn send_outgoing(&mut self) {
        for out_msg in self.paxos.get_outgoing_messages() {
            if out_msg.to == self.pid {
                self.paxos.handle(out_msg);
            } else {
                self.communication_port.trigger(CommunicatorMsg::RawPaxosMsg(out_msg));
            }
        }
    }

    fn get_decided(&mut self) {
        if !self.cached_client {
           return;
        }
        for decided in self.paxos.get_decided_entries() {
            match decided {
                Entry::Normal(n) => {
                    let id = n.data.as_slice().get_u64();
                    let pr = ProposalResp::succeeded_normal(id);
                    self.communication_port.trigger(CommunicatorMsg::ProposalResponse(pr));
                },
                Entry::StopSign(ss) => {
                    let final_seq = self.paxos.stop_and_get_sequence();
                    let (continued_nodes, new_nodes) = ss.nodes.iter().partition(
                        |&pid| pid == &self.pid || self.peers.contains(pid)
                    );
                    debug!(self.ctx.log(), "Decided StopSign! Continued: {:?}, new: {:?}", &continued_nodes, &new_nodes);
                    let nodes = Reconfig::with(continued_nodes, new_nodes);
                    let r = FinalMsg::with(ss.config_id, nodes, final_seq);
                    self.supervisor.tell(PaxosReplicaMsg::Reconfig(r));
                    let pr = ProposalResp::succeeded_normal(RECONFIG_ID);
                    self.communication_port.trigger(CommunicatorMsg::ProposalResponse(pr));
                }
            }
        }
    }

    fn propose(&mut self, p: Proposal) {
        if !self.cached_client {
            self.communication_port.trigger(CommunicatorMsg::CacheClient(p.client));
            self.cached_client = true;
        }
        match p.reconfig {
            Some((reconfig, _)) => {
                self.paxos.propose_reconfiguration(reconfig);
            }
            None => {
                let mut data: Vec<u8> = Vec::with_capacity(8);
                data.put_u64(p.id);
                self.paxos.propose_normal(data);
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
                let (from_idx, to_idx) = a.request();
                let ser_entries = self.paxos.get_ser_entries(*from_idx, *to_idx);
                a.reply(ser_entries).expect("Failed to reply ReplicaComp's Ask with serialised entries");
            },
            PaxosCompMsg::GetAllEntries(a) => { // for testing only
                let seq = self.paxos.get_sequence();
                a.reply(seq).expect("Failed to reply to GetAllEntries");
            }
        }
    }

    fn receive_network(&mut self, _: NetMessage) -> () {
        // ignore
    }
}

impl<S, P> Provide<ControlPort> for PaxosComp<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    fn handle(&mut self, event: <ControlPort as Port>::Request) -> () {
        match event {
            ControlEvent::Start => {
                self.start_timers();
            },
            _ => {
                self.stop_timers();
            }
        }
    }
}

impl<S, P> Require<CommunicationPort> for PaxosComp<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    fn handle(&mut self, msg: <CommunicationPort as Port>::Indication) -> () {
        if let AtomicBroadcastCompMsg::RawPaxosMsg(pm) = msg {
            if !self.stopped {
                self.paxos.handle(pm);
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
            debug!(self.ctx.log(), "{}", format!("Node {} became leader. Ballot: {:?}", l.pid, l.ballot));
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
    use std::collections::{HashMap, HashSet};
    use std::mem;
    use std::sync::Arc;
    use crate::bench::atomic_broadcast::paxos::INITIAL_CAPACITY;

    pub struct Paxos<S, P> where
        S: SequenceTraits,
        P: PaxosStateTraits
    {
        storage: Storage<S, P>,
        config_id: u32,
        pid: u64,
        majority: usize,
        peers: HashSet<u64>,    // excluding self pid
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
        forward_discarded: bool
    }

    impl<S, P> Paxos<S, P> where
        S: SequenceTraits,
        P: PaxosStateTraits
    {
        /*** User functions ***/
        pub fn with(
            config_id: u32,
            pid: u64,
            peers: HashSet<u64>,
            storage: Storage<S, P>,
            forward_discarded: bool
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
                proposals: Vec::with_capacity(INITIAL_CAPACITY),
                lc: 0,
                decided: Vec::with_capacity(INITIAL_CAPACITY),
                outgoing: Vec::with_capacity(INITIAL_CAPACITY),
                hb_forward: Vec::with_capacity(INITIAL_CAPACITY),
                num_proposed: 0,
                forward_discarded
            }
        }

        pub fn get_decided_entries(&mut self) -> Vec<Entry> {
            let decided_entries = mem::replace(&mut self.decided, Vec::with_capacity(INITIAL_CAPACITY));
            decided_entries
        }

        pub fn get_outgoing_messages(&mut self) -> Vec<Message> {
            let outgoing_msgs = mem::replace(&mut self.outgoing, Vec::with_capacity(INITIAL_CAPACITY));
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
                                let proposals = mem::replace(&mut self.proposals, Vec::with_capacity(INITIAL_CAPACITY));
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
                if self.forward_discarded {
                    if !self.proposals.is_empty() {
                        let proposals = mem::replace(&mut self.proposals, Vec::with_capacity(INITIAL_CAPACITY));
                        // println!("Lost leadership: Forwarding proposals to {}", self.leader);
                        self.forward_proposals(proposals);
                    }
                    if !self.hb_forward.is_empty() {
                        let hb_proposals = mem::replace(&mut self.hb_forward, Vec::with_capacity(INITIAL_CAPACITY));
                        // println!("Lost leadership: Forwarding hb-proposals to {}", self.leader);
                        self.forward_proposals(hb_proposals);
                    }
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
                // println!("Forwarding to node {}", self.leader);
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
                self.storage.append_entry(entry.clone(), self.forward_discarded);
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
                        Self::drop_after_stopsign(&mut self.proposals); // drop after ss, if ss exists
                        let seq = mem::replace(&mut self.proposals, Vec::with_capacity(INITIAL_CAPACITY));
                        self.storage.append_sequence(seq, self.forward_discarded);
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
            self.config_id == other.config_id && self.nodes == other.nodes
        }

        fn ne(&self, other: &Self) -> bool {
            self.config_id != other.config_id || self.nodes != other.nodes
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
        peers: HashSet<ActorPath>,
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
        pub fn with(peers: HashSet<ActorPath>, pid: u64, delta: u64) -> BallotLeaderComp {
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
            match event {
                ControlEvent::Start => {
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
        type Message = ();

        fn receive_local(&mut self, _: Self::Message) -> () {
            // ignore
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
    use super::super::client::tests::TestClient;
    use crate::partitioning_actor::{PartitioningActor, IterationControlMsg};
    use synchronoise::CountdownEvent;
    use std::sync::Arc;
    use super::super::messages::Run;

    fn create_replica_nodes(n: u64, initial_conf: Vec<u64>, policy: TransferPolicy, forward_discarded: bool) -> (Vec<KompactSystem>, HashMap<u64, ActorPath>, Vec<ActorPath>) {
        let mut systems = Vec::with_capacity(n as usize);
        let mut nodes = HashMap::with_capacity(n as usize);
        let mut actorpaths = Vec::with_capacity(n as usize);
        for i in 1..=n {
            let system =
                kompact_benchmarks::kompact_system_provider::global().new_remote_system_with_threads(format!("paxos_replica{}", i), 4);
            let (replica_comp, unique_reg_f) = system.create_and_register(|| {
                PaxosReplica::<MemorySequence, MemoryState>::with(initial_conf.clone(), policy.clone(), forward_discarded)
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
            named_reg_f.wait_expect(Duration::from_secs(1), "ReplicaComp failed to register alias");
            let self_path = ActorPath::Named(NamedPath::with_system(
                system.system_path(),
                vec![format!("replica{}", i).into()],
            ));
            systems.push(system);
            nodes.insert(i, self_path.clone());
            actorpaths.push(self_path);
        }
        (systems, nodes, actorpaths)
    }

    #[test]
    fn paxos_test() {
        let n: u64 = 5;
        let quorum = n/2 + 1;
        let num_proposals = 50;
        let batch_size = 10;
        let config = vec![1,2,3];
        // let reconfig = None;
        let reconfig = Some((vec![1,4,5], vec![]));
        let check_sequences = true;
        let policy = TransferPolicy::Eager;
        let forward_discarded = true;

        let (systems, nodes, actorpaths) = create_replica_nodes(n, config, policy, forward_discarded);
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
        partitioning_actor.actor_ref().tell(IterationControlMsg::Run);
        /*** Setup client ***/
        let (p, f) = kpromise::<HashMap<u64, Vec<u64>>>();
        let (client, unique_reg_f) = systems[0].create_and_register( || {
            TestClient::with(
                num_proposals,
                batch_size,
                nodes,
                reconfig.clone(),
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
            assert_eq!(true, found);
        }
        let mut counter = 0;
        for i in 1..=n {
            let sequence = all_sequences.get(&i).expect(&format!("Did not get sequence for node {}", i));
            // println!("Node {}: {:?}", i, sequence.len());
            // assert!(client_sequence.starts_with(sequence));
            for id in &client_sequence {
                if !sequence.contains(&id) {
                    println!("Node {} did not have id: {} in sequence", i, id);
                    counter += 1;
                    break;
                }
            }
            if let Some(r) = &reconfig {
                if r.0.contains(&i) {
                    println!("New node {} len: {}", i, sequence.len());
                }
            }
        }
        if counter >= quorum {
            panic!("Majority DOES NOT have all client elements: counter: {}, quorum: {}", counter, quorum);
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
        let len = 11;
        let metadata = SequenceMetaData::with(config_id, len);
        let pid = 12;
        let req_init = ReconfigInit::with(config_id, reconfig.clone(), metadata.clone(), pid);
        let mut bytes: Vec<u8> = vec![];
        ReconfigSer.serialise(&ReconfigurationMsg::Init(req_init), &mut bytes).expect("Failed to serialise ReconfigInit");
        let mut buf = bytes.as_slice();
        if let Ok(ReconfigurationMsg::Init(des)) = ReconfigSer::deserialise(&mut buf) {
            assert_eq!(des.config_id, config_id);
            assert_eq!(des.nodes.continued_nodes, continued_nodes);
            assert_eq!(des.nodes.new_nodes, new_nodes);
            assert_eq!(des.seq_metadata.config_id, metadata.config_id);
            assert_eq!(des.seq_metadata.len, metadata.len);
            assert_eq!(des.from, pid);
        }
        let seq_ser = vec![1,2,3];
        let st = SequenceTransfer::with(config_id, tag, true, from_idx, to_idx, seq_ser.clone(), metadata.clone());
        bytes.clear();
        ReconfigSer.serialise(&ReconfigurationMsg::SequenceTransfer(st), &mut bytes).expect("Failed to serialise SequenceTransfer");
        buf = bytes.as_slice();
        if let Ok(ReconfigurationMsg::SequenceTransfer(des)) = ReconfigSer::deserialise(&mut buf) {
            assert_eq!(des.config_id, config_id);
            assert_eq!(des.tag, tag);
            assert_eq!(des.ser_entries, seq_ser);
            assert_eq!(des.metadata.config_id, metadata.config_id);
            assert_eq!(des.metadata.len, metadata.len);
            assert_eq!(des.succeeded, true);
        }
    }
}
