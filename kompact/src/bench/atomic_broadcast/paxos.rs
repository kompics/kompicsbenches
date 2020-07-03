use kompact::prelude::*;
use super::storage::paxos::*;
use std::fmt::{Debug};
use ballot_leader_election::{BallotLeaderComp, BallotLeaderElection};
use raw_paxos::{Entry, Paxos};
use std::sync::Arc;
use std::time::Duration;
use super::messages::*;
use super::messages::paxos::ballot_leader_election::Leader;
use super::messages::paxos::{PaxosSer, ReconfigInit, ReconfigSer, SequenceTransfer, SequenceRequest, SequenceMetaData, Reconfig, ReconfigurationMsg};
// use crate::bench::atomic_broadcast::messages::{Proposal, ProposalResp, AtomicBroadcastMsg, AtomicBroadcastDeser, RECONFIG_ID};
use crate::partitioning_actor::{PartitioningActorSer, PartitioningActorMsg, Init};
use uuid::Uuid;
use kompact::prelude::Buf;
use rand::Rng;
use super::communicator::{CommunicationPort, AtomicBroadcastCompMsg, CommunicatorMsg, Communicator};
use super::parameters::{*, paxos::*};
use crate::serialiser_ids::ATOMICBCAST_ID;
use hashbrown::{HashMap, HashSet};

const BLE: &str = "ble";
const COMMUNICATOR: &str = "communicator";

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
    // SequenceReq(SequenceRequest),
    GetAllEntries(Ask<(), Vec<Entry>>)
}

#[derive(Clone, Debug)]
pub enum ReconfigurationPolicy {
    Eager,
    Pull,
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
    ble_comps: HashMap<u32, Arc<Component<BallotLeaderComp>>>,
    communicator_comps: HashMap<u32, Arc<Component<Communicator>>>,
    active_config_id: u32,
    leader_in_active_config: u64,
    nodes: Vec<ActorPath>, // derive actorpaths of peers' ble and paxos replicas from these
    prev_sequences: HashMap<u32, Arc<S>>,
    stopped: bool,
    iteration_id: u32,
    partitioning_actor: Option<ActorPath>,
    alias_registrations: HashSet<Uuid>,
    policy: ReconfigurationPolicy,
    next_config_id: Option<u32>,
    pending_seq_transfers: HashMap<u32, (u32, HashMap<u32, Vec<Entry>>)>,   // <config_id, (num_segments, <segment_id, entries>)
    complete_sequences: HashSet<u32>,
    active_peers: (Vec<u64>, Vec<u64>), // (ready, not_ready)
    retry_transfer_timers: HashMap<u32, ScheduledTimer>,
    cached_client: Option<ActorPath>,
    // pending_local_seq_requests: HashMap<SequenceRequest, ActorPath>,
    pending_kill_comps: usize,
    batch_accept: bool
}

impl<S, P> PaxosReplica<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    pub fn with(initial_config: Vec<u64>, policy: ReconfigurationPolicy, batch_accept: bool) -> PaxosReplica<S, P> {
        PaxosReplica {
            ctx: ComponentContext::new(),
            pid: 0,
            initial_config,
            paxos_comps: HashMap::new(),
            ble_comps: HashMap::new(),
            communicator_comps: HashMap::new(),
            active_config_id: 0,
            leader_in_active_config: 0,
            nodes: vec![],
            prev_sequences: HashMap::new(),
            stopped: false,
            iteration_id: 0,
            partitioning_actor: None,
            alias_registrations: HashSet::new(),
            policy,
            next_config_id: None,
            pending_seq_transfers: HashMap::new(),
            complete_sequences: HashSet::new(),
            active_peers: (Vec::new(), Vec::new()),
            retry_transfer_timers: HashMap::new(),
            cached_client: None,
            // pending_local_seq_requests: HashMap::new(),
            pending_kill_comps: 0,
            batch_accept
        }
    }

    fn create_replica(&mut self, config_id: u32, nodes: Vec<u64>, register_alias_with_response: bool) {
        let num_peers = nodes.len() - 1;
        let mut communicator_peers = HashMap::with_capacity(num_peers);
        let mut ble_peers = Vec::with_capacity(num_peers);
        let mut peers = nodes;
        peers.retain(|pid| pid != &self.pid);
        for pid in &peers {
            let idx = *pid as usize - 1;
            let actorpath = self.nodes.get(idx).expect("No actorpath found");
            match actorpath {
                ActorPath::Named(n) => {
                    // derive paxos and ble actorpath of peers from replica actorpath
                    let sys_path = n.system();
                    let protocol = sys_path.protocol();
                    let port = sys_path.port();
                    let addr = sys_path.address();
                    let named_communicator = NamedPath::new(
                        protocol,
                        *addr,
                        port,
                        vec![format!("{}{},{}-{}", COMMUNICATOR, pid, config_id, self.iteration_id)]
                    );
                    let named_ble = NamedPath::new(
                        protocol,
                        *addr,
                        port,
                        vec![format!("{}{},{}-{}", BLE, pid, config_id, self.iteration_id)]
                    );
                    communicator_peers.insert(*pid, ActorPath::Named(named_communicator));
                    ble_peers.push(ActorPath::Named(named_ble));
                },
                _ => error!(self.ctx.log(), "{}", format!("Actorpath is not named for node {}", pid)),
            }
        }
        let system = self.ctx.system();
        let kill_recipient: Recipient<KillResponse> = self.ctx.actor_ref().recipient();
        /*** create and register Paxos ***/
        let paxos_comp = system.create(|| {
            PaxosComp::with(self.ctx.actor_ref(), peers, config_id, self.pid, self.batch_accept)
        });
        system.register_without_response(&paxos_comp);
        /*** create and register Communicator ***/
        let communicator = system.create( || {
            Communicator::with(
                communicator_peers,
                self.cached_client.as_ref().expect("No cached client!").clone(),
                kill_recipient.clone()
            )
        });
        system.register_without_response(&communicator);
        /*** create and register BLE ***/
        let ble_comp = system.create( || {
            BallotLeaderComp::with(ble_peers, self.pid, ELECTION_TIMEOUT, BLE_DELTA, kill_recipient)
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
        self.ble_comps.insert(config_id, ble_comp);
        self.communicator_comps.insert(config_id, communicator);
        self.next_config_id = Some(config_id);
    }

    fn start_replica(&mut self) {
        if let Some(config_id) = self.next_config_id {
            info!(self.ctx.log(), "Starting replica pid: {}, config_id: {}", self.pid, config_id);
            self.active_config_id = config_id;
            self.leader_in_active_config = 0;
            let paxos = self.paxos_comps
                .get(&config_id)
                .expect(&format!("Could not find PaxosComp with config_id: {}", config_id));
            let ble = self.ble_comps
                .get(&config_id)
                .expect(&format!("Could not find BLE config_id: {}", config_id));
            let communicator = self.communicator_comps
                .get(&config_id)
                .expect(&format!("Could not find Communicator with config_id: {}", config_id));
            self.ctx.system().start(paxos);
            self.ctx.system().start(ble);
            self.ctx.system().start(communicator);
            self.next_config_id = None;
        }
    }

    fn kill_all_replicas(&mut self) {
        self.pending_kill_comps = self.ble_comps.len() + self.paxos_comps.len() + self.communicator_comps.len();
        debug!(self.ctx.log(), "Killing {} child components...", self.pending_kill_comps);
        if self.pending_kill_comps == 0 {
            debug!(self.ctx.log(), "Stopped all child components");
            self.partitioning_actor
                .as_ref()
                .unwrap()
                .tell_serialised(PartitioningActorMsg::StopAck, self)
                .expect("Should serialise");
        } else {
            for (_, ble) in self.ble_comps.drain() {
                self.ctx.system().kill(ble);
            }
            for (_, paxos) in self.paxos_comps.drain() {
                self.ctx.system().kill(paxos);
            }
            for (_, communicator) in self.communicator_comps.drain() {
                self.ctx.system().kill(communicator);
            }
        }
    }

    fn new_iteration(&mut self, init: Init) {
        self.stopped = false;
        self.nodes = init.nodes;
        self.pid = init.pid as u64;
        self.iteration_id = init.init_id;
        let ser_client = init.init_data.expect("Init should include ClientComp's actorpath");
        let client = ActorPath::deserialise(&mut ser_client.as_slice()).expect("Failed to deserialise Client's actorpath");
        self.cached_client = Some(client);
        if self.initial_config.contains(&self.pid){
            self.next_config_id = Some(1);
            self.create_replica(1, self.initial_config.clone(), true);
        } else {
            let resp = PartitioningActorMsg::InitAck(self.iteration_id);
            let ap = self.partitioning_actor.take().expect("PartitioningActor not found!");
            ap.tell_serialised(resp, self).expect("Should serialise");
        }
    }

    fn pull_sequence(&mut self, config_id: u32, seq_len: u64) {
        let num_ready_peers = self.active_peers.0.len();
        let num_unready_peers = self.active_peers.1.len();
        let num_continued_nodes = num_ready_peers + num_unready_peers;
        self.pending_seq_transfers.insert(config_id, (num_continued_nodes as u32, HashMap::with_capacity(num_continued_nodes)));
        let offset = seq_len/num_continued_nodes as u64;
        // get segment from unready nodes (probably have early segments of final sequence)
        for (i, pid) in self.active_peers.1.iter().enumerate() {
            let from_idx = i as u64 * offset;
            let to_idx = if from_idx as u64 + offset > seq_len{
                seq_len
            } else {
                from_idx + offset
            };
            let tag = i + 1;
            debug!(self.ctx.log(), "Requesting segment from {}, config_id: {}, tag: {}, idx: {}-{}", pid, config_id, tag, from_idx, to_idx-1);
            self.request_sequence(*pid, config_id, from_idx, to_idx, tag as u32);
        }
        // get segment from ready nodes (definitely has final sequence)
        for (i, pid) in self.active_peers.0.iter().enumerate() {
            let from_idx = (num_unready_peers + i) as u64 * offset;
            let to_idx = if from_idx as u64 + offset > seq_len{
                seq_len
            } else {
                from_idx + offset
            };
            let tag = num_unready_peers + i + 1;
            debug!(self.ctx.log(), "Requesting segment from {}, config_id: {}, tag: {}, idx: {}-{}", pid, config_id, tag, from_idx, to_idx-1);
            self.request_sequence(*pid, config_id, from_idx, to_idx, tag as u32);
        }
        let timer = self.schedule_once(Duration::from_millis(TRANSFER_TIMEOUT), move |c, _| c.retry_request_sequence(config_id, seq_len));
        self.retry_transfer_timers.insert(config_id, timer);
    }

    fn request_sequence(&self, pid: u64, config_id: u32, from_idx: u64, to_idx: u64, tag: u32) {
        let sr = SequenceRequest::with(config_id, tag, from_idx, to_idx, self.pid);
        let idx = pid as usize - 1;
        self.nodes
            .get(idx)
            .expect(&format!("Failed to get Actorpath of node {}", pid))
            .tell_serialised(ReconfigurationMsg::SequenceRequest(sr), self)
            .expect("Should serialise!");
    }

    fn retry_request_sequence(&mut self, config_id: u32, seq_len: u64) {
        if let Some(transfer) = self.pending_seq_transfers.get(&config_id) {
            let num_segments = transfer.0;
            let received_segments = &transfer.1;
            let offset = seq_len/num_segments as u64;
            let num_active = self.active_peers.0.len();
            if num_active > 0 {
                for i in 0..num_segments {
                    let tag = i + 1;
                    if !received_segments.contains_key(&tag) {    // missing segment, retry from a replica we know have the final seq
                        let from_idx = i as u64 * offset;
                        let to_idx = from_idx + offset;
                        // info!(self.ctx.log(), "Retrying timed out seq transfer: tag: {}, idx: {}-{}, policy: {:?}", tag, from_idx, to_idx, self.policy);
                        let pid = self.active_peers.0.get(i as usize % num_active).expect(&format!("Failed to get active pid. idx: {}, len: {}", i, self.active_peers.0.len()));
                        self.request_sequence(*pid, config_id, from_idx, to_idx, tag);
                    }
                }
            }
            let timer = self.schedule_once(Duration::from_millis(TRANSFER_TIMEOUT), move |c, _| c.retry_request_sequence(config_id, seq_len));
            self.retry_transfer_timers.insert(config_id, timer);
        }
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

    fn create_eager_sequence_transfer(&self, continued_nodes: &[u64], config_id: u32) -> SequenceTransfer {
        let index = continued_nodes.iter().position(|pid| pid == &self.pid).expect("Could not find my pid in continued_nodes");
        let tag = index as u32 + 1;
        let n_continued = continued_nodes.len();
        let final_seq = self.prev_sequences.get(&config_id).expect("Should have final sequence");
        let seq_len = final_seq.get_sequence_len();
        let offset = seq_len/n_continued as u64;
        let from_idx = index as u64 * offset;
        let to_idx = from_idx + offset;
        // info!(self.ctx.log(), "Creating eager sequence transfer. Tag: {}, idx: {}-{}, continued_nodes: {:?}", tag, from_idx, to_idx, continued_nodes);
        let ser_entries = final_seq.get_ser_entries(from_idx, to_idx).expect("Should have entries of final sequence");
        let prev_seq_metadata = self.get_sequence_metadata(config_id-1);
        let st = SequenceTransfer::with(config_id, tag, true, from_idx, to_idx, ser_entries, prev_seq_metadata);
        st
    }

    fn handle_sequence_request(&mut self, sr: SequenceRequest, requestor: ActorPath) {
        if self.leader_in_active_config == sr.requestor_pid { return; }
        let (succeeded, ser_entries) = match self.prev_sequences.get(&sr.config_id) {
            Some(seq) => {
                if let Some(entries) = seq.get_ser_entries(sr.from_idx, sr.to_idx) {
                    (true, entries)
                } else {
                    warn!(
                        self.ctx.log(),
                        "Previous sequence did not have requested entries: active_config: {}, requested_config: {}, requested_idx: {}-{}, sequence len: {}",
                        self.active_config_id,
                        sr.config_id,
                        sr.from_idx,
                        sr.to_idx,
                        seq.get_sequence_len()
                    );
                    (false, vec![])
                }
            },
            None => {
                (false, vec![])
                /*if self.active_config_id == sr.config_id {  // we have not reached final sequence, but might still have requested elements
                    let paxos = self.paxos_comps.get(&sr.config_id).expect(&format!("No paxos comp with config_id: {} when handling SequenceRequest. My config_ids: {:?}", sr.config_id, self.paxos_comps.keys()));
                    paxos.actor_ref().tell(PaxosCompMsg::SequenceReq(sr.clone()));
                    self.pending_local_seq_requests.insert(sr, requestor);
                    return;
                } else {
                    (false, vec![])
                }*/
            }
        };
        let prev_seq_metadata = self.get_sequence_metadata(sr.config_id-1);
        let st = SequenceTransfer::with(sr.config_id, sr.tag, succeeded, sr.from_idx, sr.to_idx, ser_entries, prev_seq_metadata);
        // info!(self.ctx.log(), "Replying seq transfer: tag: {}, idx: {}-{}", st.tag, st.from_idx, st.to_idx);
        requestor.tell_serialised(ReconfigurationMsg::SequenceTransfer(st), self).expect("Should serialise!");
    }

    fn handle_sequence_transfer(&mut self, st: SequenceTransfer) {
        if self.active_config_id > st.config_id || self.complete_sequences.contains(&st.config_id) || self.next_config_id.unwrap_or(0) <= st.config_id {
            return; // ignore late sequence transfers
        }
        let prev_config_id = st.metadata.config_id;
        let prev_seq_len = st.metadata.len;
        // pull previous sequence if exists and not already started
        if prev_config_id != 0 && !self.complete_sequences.contains(&prev_config_id) && !self.pending_seq_transfers.contains_key(&prev_config_id) {
            self.pull_sequence(prev_config_id, prev_seq_len);
        }
        if st.succeeded {
            let segments = self.pending_seq_transfers.get_mut(&st.config_id).expect(&format!("Got unexpected sequence transfer config_id: {}, tag: {}, index: {}-{}. active config: {}, Stopped: {}", st.config_id, st.tag, st.from_idx, st.to_idx, self.active_config_id, self.stopped));
            // info!(self.ctx.log(), "Got segment config_id: {}, tag: {}", st.config_id, st.tag);
            let seq = PaxosSer::deserialise_entries(&mut st.ser_entries.as_slice());
            segments.1.insert(st.tag, seq);
            if segments.1.len() as u32 == segments.0 {  // got all segments, i.e. complete sequence
                let mut complete_seq = segments.1.remove(&1).expect(&format!("Failed to get first segment. Received tags: {:?}", segments.1.keys()));
                for tag in 2..=segments.0 {
                    let mut segment = segments.1.remove(&tag).expect(&format!("Failed to get segment of tag {}. Received tags: {:?}", tag, segments.1.keys()));
                    complete_seq.append(&mut segment);
                }
                let final_sequence = S::new_with_sequence(complete_seq);
                self.prev_sequences.insert(st.config_id, Arc::new(final_sequence));
                self.complete_sequences.insert(st.config_id);
                self.pending_seq_transfers.remove(&st.config_id);
                if let Some(timer) = self.retry_transfer_timers.remove(&st.config_id){
                    self.cancel_timer(timer);
                }
                if self.pending_seq_transfers.is_empty() {  // got all sequence transfers
                    self.complete_sequences.clear();
                    debug!(self.ctx.log(), "Got all previous sequences!");
                    self.start_replica();
                }
            }
        } else {    // failed sequence transfer i.e. not reached final seq yet
            let config_id = st.config_id;
            let tag = st.tag;
            let from_idx = st.from_idx;
            let to_idx = st.to_idx;
            // info!(self.ctx.log(), "Got failed seq transfer: tag: {}, idx: {}-{}", tag, from_idx, to_idx);
            // query someone we know have reached final seq
            let num_active = self.active_peers.0.len();
            if num_active > 0 {
                // choose randomly
                let mut rng = rand::thread_rng();
                let rnd = rng.gen_range(0, num_active);
                let pid = self.active_peers.0[rnd];
                self.request_sequence(pid, config_id, from_idx, to_idx, tag);
            } // else let timeout handle it to retry
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
    Leader(u32, u64),
    Reconfig(FinalMsg<S>),
    RegResp(RegistrationResponse),
    // SequenceResp(SequenceRequest, Option<Vec<u8>>),
    KillResp
}

impl<S> From<RegistrationResponse> for PaxosReplicaMsg<S> where S: SequenceTraits {
    fn from(rr: RegistrationResponse) -> Self {
        PaxosReplicaMsg::RegResp(rr)
    }
}

impl<S> From<KillResponse> for PaxosReplicaMsg<S> where S: SequenceTraits{
    fn from(_: KillResponse) -> Self {
        PaxosReplicaMsg::KillResp
    }
}

impl<S, P> Actor for PaxosReplica<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    type Message = PaxosReplicaMsg<S>;

    fn receive_local(&mut self, msg: Self::Message) -> () {
        if self.stopped {
            if let PaxosReplicaMsg::KillResp = msg {
                self.pending_kill_comps -= 1;
                if self.pending_kill_comps == 0 {
                    debug!(self.ctx.log(), "Stopped all child components");
                    self.partitioning_actor
                        .as_ref()
                        .unwrap()
                        .tell_serialised(PartitioningActorMsg::StopAck, self)
                        .expect("Should serialise");
                }
            }
            return;
        }
        match msg {
            PaxosReplicaMsg::Leader(config_id, pid) => {
                if self.active_config_id == config_id {
                    if self.leader_in_active_config == 0 && pid == self.pid {
                        self.cached_client
                            .as_ref()
                            .expect("No cached client!")
                            .tell_serialised(AtomicBroadcastMsg::FirstLeader(pid), self)
                            .expect("Should serialise FirstLeader");
                    }
                    self.leader_in_active_config = pid;
                }
            },
            PaxosReplicaMsg::Reconfig(r) => {
                let prev_config_id = self.active_config_id;
                let final_seq_len: u64 = r.final_sequence.get_sequence_len();
                debug!(self.ctx.log(), "RECONFIG: Next config_id: {}, prev_config: {}, len: {}", r.config_id, prev_config_id, final_seq_len);
                let seq_metadata = SequenceMetaData::with(prev_config_id, final_seq_len);
                self.prev_sequences.insert(prev_config_id, r.final_sequence);
                let r_init = ReconfigurationMsg::Init(ReconfigInit::with(r.config_id, r.nodes.clone(), seq_metadata, self.pid));
                for pid in &r.nodes.new_nodes {
                    if pid != &self.pid {
                        let idx = *pid as usize - 1;
                        let actorpath = self.nodes.get(idx).expect(&format!("No actorpath found for new node {}", pid));
                        actorpath.tell_serialised(r_init.clone(), self).expect("Should serialise!");
                    }
                }
                let mut nodes = r.nodes.continued_nodes;
                let mut new_nodes = r.nodes.new_nodes;
                if nodes.contains(&self.pid) {
                    if let ReconfigurationPolicy::Eager = self.policy {
                        let st = self.create_eager_sequence_transfer(nodes.as_slice(), prev_config_id);
                        for pid in &new_nodes {
                            let idx = *pid as usize - 1;
                            let actorpath = self.nodes.get(idx).expect(&format!("No actorpath found for new node {}", pid));
                            actorpath.tell_serialised(ReconfigurationMsg::SequenceTransfer(st.clone()), self).expect("Should serialise!");
                        }
                    }
                    nodes.append(&mut new_nodes);
                    self.create_replica(r.config_id, nodes, false);
                    self.start_replica();
                }
            },
            PaxosReplicaMsg::RegResp(rr) => {
                self.alias_registrations.remove(&rr.id.0);
                if self.alias_registrations.is_empty() {
                    let resp = PartitioningActorMsg::InitAck(self.iteration_id);
                    let ap = self.partitioning_actor.take().expect("PartitioningActor not found!");
                    ap.tell_serialised(resp, self).expect("Should serialise");
                }
            },
            /*
            PaxosReplicaMsg::SequenceResp(sr, ser_entries) => {
                if let Some(requestor) = self.pending_local_seq_requests.get(&sr) {
                    let (succeeded, serialised) = match ser_entries {
                        Some(sr) => (true, sr),
                        None => (false, vec![]),
                    };
                    let prev_seq_metadata = self.get_sequence_metadata(sr.config_id-1);
                    let st = SequenceTransfer::with(sr.config_id, sr.tag, succeeded, sr.from_idx, sr.to_idx, serialised, prev_seq_metadata);
                    // info!(self.ctx.log(), "Replying async seq transfer: tag: {}", st.tag);
                    requestor.tell_serialised(ReconfigurationMsg::SequenceTransfer(st), self).expect("Should serialise!");
                }
                self.pending_local_seq_requests.remove(&sr);
            },
            */
            _ => {}
        }
    }

    fn receive_network(&mut self, m: NetMessage) -> () {
        match m.data.ser_id {
            ATOMICBCAST_ID => {
                if !self.stopped {
                    if self.leader_in_active_config == self.pid {
                        if let AtomicBroadcastMsg::Proposal(p) = m.try_deserialise_unchecked::<AtomicBroadcastMsg, AtomicBroadcastDeser>().expect("Should be AtomicBroadcastMsg!") {
                            if p.reconfig.is_some() && self.active_config_id > 1 {   // TODO make proposal enum and check reconfig id
                                warn!(self.ctx.log(), "Duplicate reconfig proposal? Active config: {}", self.active_config_id);
                                return;
                            }
                            else {
                                let active_paxos = &self.paxos_comps.get(&self.active_config_id).expect("Could not get PaxosComp actor ref despite being leader");
                                active_paxos.actor_ref().tell(PaxosCompMsg::Propose(p));
                            }
                        }
                    } else if self.leader_in_active_config > 0 {
                        let idx = self.leader_in_active_config as usize - 1;
                        let leader = self.nodes.get(idx).unwrap_or_else(|| panic!("Could not get leader's actorpath. Pid: {}", self.leader_in_active_config));
                        leader.forward_with_original_sender(m, self);
                    }
                    // else no leader... just drop
                }
            },
            _ => {
                let NetMessage{sender, data, ..} = m;
                match_deser! {data; {
                    p: PartitioningActorMsg [PartitioningActorSer] => {
                        match p {
                            PartitioningActorMsg::Init(init) => {
                                self.partitioning_actor = Some(sender);
                                    self.new_iteration(init);
                            },
                            PartitioningActorMsg::Run => {
                                self.start_replica();
                            },
                            PartitioningActorMsg::Stop => {
                                self.partitioning_actor = Some(sender);
                                self.kill_all_replicas();
                                let retry_timers = std::mem::take(&mut self.retry_transfer_timers);
                                for (_, timer) in retry_timers {
                                    self.cancel_timer(timer);
                                }
                                self.active_config_id = 0;
                                self.leader_in_active_config = 0;
                                self.next_config_id = None;
                                self.prev_sequences.clear();
                                self.active_peers.0.clear();
                                self.active_peers.1.clear();
                                self.complete_sequences.clear();
                                self.stopped = true;
                            },
                            _ => unimplemented!()
                        }
                    },
                    rm: ReconfigurationMsg [ReconfigSer] => {
                        match rm {
                            ReconfigurationMsg::Init(r) => {
                                if self.stopped || self.active_config_id >= r.config_id {
                                    return;
                                }
                                match self.next_config_id {
                                    None => {
                                        debug!(self.ctx.log(), "Got ReconfigInit for config_id: {} from node {}", r.config_id, r.from);
                                        for pid in &r.nodes.continued_nodes {
                                            if pid == &r.from {
                                                self.active_peers.0.push(*pid);
                                            } else {
                                                self.active_peers.1.push(*pid);
                                            }
                                        }
                                        let num_expected_transfers = r.nodes.continued_nodes.len();
                                        let mut nodes = r.nodes.continued_nodes;
                                        let mut new_nodes = r.nodes.new_nodes;
                                        nodes.append(&mut new_nodes);
                                        if r.seq_metadata.len == 1 && r.seq_metadata.config_id == 1 {
                                            // only SS in final sequence and no other prev sequences -> start directly
                                            let final_sequence = S::new_with_sequence(vec![]);
                                            self.prev_sequences.insert(r.seq_metadata.config_id, Arc::new(final_sequence));
                                            self.create_replica(r.config_id, nodes, false);
                                            self.start_replica();
                                        } else {
                                            match self.policy {
                                                ReconfigurationPolicy::Pull => self.pull_sequence(r.seq_metadata.config_id, r.seq_metadata.len),
                                                ReconfigurationPolicy::Eager => {
                                                    let config_id = r.seq_metadata.config_id;
                                                    self.pending_seq_transfers.insert(r.seq_metadata.config_id, (num_expected_transfers as u32, HashMap::with_capacity(num_expected_transfers)));
                                                    let seq_len = r.seq_metadata.len;
                                                    let timer = self.schedule_once(Duration::from_millis(TRANSFER_TIMEOUT/2), move |c, _| c.retry_request_sequence(config_id, seq_len));
                                                    self.retry_transfer_timers.insert(config_id, timer);
                                                },
                                            }
                                            self.create_replica(r.config_id, nodes, false);
                                        }
                                    },
                                    Some(next_config_id) => {
                                        if next_config_id == r.config_id {
                                            if r.nodes.continued_nodes.contains(&r.from) {
                                                // update who we know already decided final seq
                                                self.active_peers.1.retain(|x| x == &r.from);
                                                self.active_peers.0.push(r.from);
                                            }
                                        }
                                    }
                                }
                            },
                            ReconfigurationMsg::SequenceRequest(sr) => {
                                if !self.stopped {
                                    self.handle_sequence_request(sr, sender);
                                }
                            },
                            ReconfigurationMsg::SequenceTransfer(st) => {
                                if !self.stopped {
                                    self.handle_sequence_transfer(st);
                                }
                            }
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
                                                let id = n.as_slice().get_u64();
                                                all_entries.push(id);
                                                unique.insert(id);
                                            }
                                        }
                                    }
                                }
                                if self.active_config_id > 0 {
                                    let active_paxos = self.paxos_comps.get(&self.active_config_id).unwrap();
                                    let sequence = active_paxos.actor_ref().ask(|promise| PaxosCompMsg::GetAllEntries(Ask::new(promise, ()))).wait();
                                    for entry in sequence {
                                        if let Entry::Normal(n) = entry {
                                            let id = n.as_slice().get_u64();
                                            all_entries.push(id);
                                            unique.insert(id);
                                         }
                                    }
                                    let min = unique.iter().min();
                                    let max = unique.iter().max();
                                    debug!(self.ctx.log(), "Got SequenceReq: my seq_len: {}, unique: {}, min: {:?}, max: {:?}", all_entries.len(), unique.len(), min, max);
                                } else {
                                    warn!(self.ctx.log(), "Got SequenceReq but no active paxos: {}", self.active_config_id);
                                }
                                let sr = SequenceResp::with(self.pid, all_entries);
                                sender.tell((TestMessage::SequenceResp(sr), TestMessageSer), self);
                            },
                            _ => error!(self.ctx.log(), "Got unexpected TestMessage: {:?}", tm),
                        }
                    },
                    !Err(e) => error!(self.ctx.log(), "Error deserialising msg: {:?}", e),
                    }
                }
            },
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
    peers: Vec<u64>,
    paxos: Paxos<S, P>,
    config_id: u32,
    pid: u64,
    current_leader: u64,
    timers: Option<(ScheduledTimer, ScheduledTimer)>,
}

impl<S, P> PaxosComp<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    fn with(
        supervisor: ActorRef<PaxosReplicaMsg<S>>,
        peers: Vec<u64>,
        config_id: u32,
        pid: u64,
        batch_accept: bool
    ) -> PaxosComp<S, P>
    {
        let seq = S::new();
        let paxos_state = P::new();
        let storage = Storage::with(seq, paxos_state);
        let paxos = Paxos::with(config_id, pid, peers.clone(), storage, batch_accept);
        PaxosComp {
            ctx: ComponentContext::new(),
            supervisor,
            communication_port: RequiredPort::new(),
            ble_port: RequiredPort::new(),
            peers,
            paxos,
            config_id,
            pid,
            current_leader: 0,
            timers: None,
        }
    }

    fn start_timers(&mut self) {
        let decided_timer = self.schedule_periodic(
            Duration::from_millis(1),
            Duration::from_millis(GET_DECIDED_PERIOD),
            move |c, _| c.get_decided()
        );
        let outgoing_timer = self.schedule_periodic(
            Duration::from_millis(0),
            Duration::from_millis(OUTGOING_MSGS_PERIOD),
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
        for out_msg in self.paxos.get_outgoing_msgs() {
            self.communication_port.trigger(CommunicatorMsg::RawPaxosMsg(out_msg));
        }
    }

    fn get_decided(&mut self) {
        for decided in self.paxos.get_decided_entries() {
            match decided {
                Entry::Normal(data) => {
                    if self.current_leader == self.pid {
                        let pr = ProposalResp::with(data, self.current_leader);
                        self.communication_port.trigger(CommunicatorMsg::ProposalResponse(pr));
                    }
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
                    let leader = 0; // we don't know who will become leader in new config
                    let mut data: Vec<u8> = Vec::with_capacity(8);
                    data.put_u64(RECONFIG_ID);
                    let pr = ProposalResp::with(data, leader);
                    self.communication_port.trigger(CommunicatorMsg::ProposalResponse(pr));
                }
            }
        }
    }

    fn propose(&mut self, p: Proposal) {
        match p.reconfig {
            Some((reconfig, _)) => {
                self.paxos.propose_reconfiguration(reconfig);
            },
            None => {
                self.paxos.propose_normal(p.data);
            }
        }
    }
}

impl<S, P> Actor for PaxosComp<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    type Message = PaxosCompMsg;

    fn receive_local(&mut self, msg: PaxosCompMsg) -> () {
        match msg {
            PaxosCompMsg::Propose(p) => {
                self.propose(p);
            },
            /*
            PaxosCompMsg::SequenceReq(seq_req) => {
                let ser_entries = self.paxos.get_chosen_ser_entries(seq_req.from_idx, seq_req.to_idx);
                self.supervisor.tell(PaxosReplicaMsg::SequenceResp(seq_req, ser_entries));
            },
            */
            PaxosCompMsg::GetAllEntries(a) => { // for testing only
                let seq = self.paxos.get_sequence();
                a.reply(seq).expect("Failed to reply to GetAllEntries");
            },
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
            ControlEvent::Kill => {
                self.stop_timers();
                self.supervisor.tell(PaxosReplicaMsg::KillResp);
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
            self.paxos.handle(pm);
        }
    }
}

impl<S, P> Require<BallotLeaderElection> for PaxosComp<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    fn handle(&mut self, l: Leader) -> () {
        debug!(self.ctx.log(), "{}", format!("Node {} became leader in config {}. Ballot: {:?}",  l.pid, self.config_id, l.ballot));
        self.paxos.handle_leader(l);
        if self.current_leader != l.pid && !self.paxos.stopped() {
            self.current_leader = l.pid;
            self.supervisor.tell(PaxosReplicaMsg::Leader(self.config_id, l.pid));
        }
    }
}

pub mod raw_paxos{
    use super::super::messages::paxos::{*};
    use super::super::messages::paxos::ballot_leader_election::{Ballot, Leader};
    use super::super::storage::paxos::Storage;
    use super::{SequenceTraits, PaxosStateTraits};
    use std::fmt::Debug;
    use hashbrown::HashMap;
    use std::mem;
    use std::sync::Arc;
    use crate::bench::atomic_broadcast::parameters::MAX_INFLIGHT;
    use indexmap::map::IndexMap;

    pub struct Paxos<S, P> where
        S: SequenceTraits,
        P: PaxosStateTraits
    {
        storage: Storage<S, P>,
        config_id: u32,
        pid: u64,
        majority: usize,
        peers: Vec<u64>,    // excluding self pid
        state: (Role, Phase),
        leader: u64,
        n_leader: Ballot,
        promises_meta: HashMap<u64, (Ballot, usize)>,
        las: IndexMap<u64, u64>,
        lds: IndexMap<u64, u64>,
        proposals: Vec<Entry>,
        lc: u64,    // length of longest chosen seq
        prev_ld: u64,
        acc_sync_ld: u64,
        max_promise_meta: (Ballot, usize, u64),  // ballot, sfx len, pid
        max_promise_sfx: Vec<Entry>,
        batch_accept: bool,
        batch_accept_meta: Vec<Option<(Ballot, usize)>>,    //  ballot, index in outgoing
        outgoing: Vec<Message>,
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
            storage: Storage<S, P>,
            batch_accept: bool,
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
                promises_meta: HashMap::with_capacity(num_nodes),
                las: IndexMap::with_capacity(num_nodes),
                lds: IndexMap::with_capacity(num_nodes),
                proposals: vec![],
                lc: 0,
                prev_ld: 0,
                acc_sync_ld: 0,
                max_promise_meta: (Ballot::with(0, 0), 0, 0),
                max_promise_sfx: vec![],
                batch_accept,
                batch_accept_meta: vec![None; num_nodes],
                outgoing: Vec::with_capacity(MAX_INFLIGHT),
            }
        }

        pub fn get_outgoing_msgs(&mut self) -> Vec<Message> {
            let mut outgoing = Vec::with_capacity(MAX_INFLIGHT);
            std::mem::swap(&mut self.outgoing, &mut outgoing);
            if self.batch_accept {
                self.batch_accept_meta = vec![None; self.majority * 2 - 1];
            }
            outgoing
        }

        pub fn get_decided_entries(&mut self) -> Vec<Entry> {
            let ld = self.storage.get_decided_len();
            if self.prev_ld < ld {
                let decided = self.storage.get_entries(self.prev_ld, ld);
                self.prev_ld = ld;
                decided
            } else {
                vec![]
            }
        }

        pub fn handle(&mut self, m: Message) {
            match m.msg {
                PaxosMsg::Prepare(prep) => self.handle_prepare(prep, m.from),
                PaxosMsg::Promise(prom) => {
                    match &self.state {
                        (Role::Leader, Phase::Prepare) => self.handle_promise_prepare(prom, m.from),
                        (Role::Leader, Phase::Accept) => self.handle_promise_accept(prom, m.from),
                        _ => {}
                    }
                },
                PaxosMsg::AcceptSync(acc_sync) => self.handle_accept_sync(acc_sync, m.from),
                PaxosMsg::Accept(acc) => self.handle_accept(acc, m.from),
                PaxosMsg::Accepted(accepted) => self.handle_accepted(accepted, m.from),
                PaxosMsg::Decide(d) => self.handle_decide(d),
                PaxosMsg::ProposalForward(proposals) => self.handle_forwarded_proposal(proposals),
            }
        }

        pub fn stopped(&self) -> bool { self.storage.stopped() }

        pub fn propose_normal(&mut self, data: Vec<u8>) {
            if self.stopped(){ return; }
            let normal_entry = Entry::Normal(data);
            match self.state {
                (Role::Leader, Phase::Prepare) => {
                    self.proposals.push(normal_entry);
                },
                (Role::Leader, Phase::Accept) => {
                    self.send_accept(normal_entry);
                },
                _ => {
                    self.forward_proposals(normal_entry);
                }
            }
        }

        pub fn propose_reconfiguration(&mut self, nodes: Vec<u64>) {
            if self.stopped(){ return; }
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
                    self.forward_proposals(entry)
                }
            }
        }

        /*
        pub fn get_chosen_ser_entries(&self, from_idx: u64, to_idx: u64) -> Option<Vec<u8>> {
            let ld = self.storage.get_decided_len();
            let max_idx = std::cmp::max(ld, self.lc);
            if to_idx > max_idx {
                None
            } else {
                self.storage.get_ser_entries(from_idx, to_idx)
            }
        }
        */

        pub(crate) fn stop_and_get_sequence(&mut self) -> Arc<S> {
            self.storage.stop_and_get_sequence()
        }

        pub fn get_sequence_len(&self) -> u64 {
            self.storage.get_sequence_len()
        }

        fn clear_peers_state(&mut self) {
            self.las.clear();
            self.promises_meta.clear();
            self.lds.clear();
        }

        /*** Leader ***/
        pub fn handle_leader(&mut self, l: Leader) {
            let n = l.ballot;
            if n <= self.n_leader || n <= self.storage.get_promise() {
                return;
            }
            self.clear_peers_state();
            if self.stopped() {
                self.proposals.clear();
            }
            if self.pid == l.pid {
                self.n_leader = n;
                self.leader = n.pid;
                self.storage.set_promise(n);
                /* insert my promise */
                let na = self.storage.get_accepted_ballot();
                let ld = self.storage.get_decided_len();
                let sfx = self.storage.get_suffix(ld);
                let sfx_len = sfx.len();
                self.max_promise_meta = (na, sfx_len, self.pid);
                self.promises_meta.insert(self.pid, (na, sfx_len));
                self.max_promise_sfx = sfx;
                /* insert my longest decided sequnce */
                self.acc_sync_ld = ld;
                /* initialise longest chosen sequence and update state */
                self.lc = 0;
                self.state = (Role::Leader, Phase::Prepare);
                /* send prepare */
                for pid in &self.peers {
                    let prep = Prepare::with(n, ld, self.storage.get_accepted_ballot());
                    self.outgoing.push(Message::with(self.pid, *pid, PaxosMsg::Prepare(prep)));
                }
            } else {
                self.state.0 = Role::Follower;
                self.leader = n.pid;
            }
        }

        fn forward_proposals(&mut self, entry: Entry) {
            if self.leader > 0 {
                let pf = PaxosMsg::ProposalForward(entry);
                let msg = Message::with(self.pid, self.leader, pf);
                // println!("Forwarding to node {}", self.leader);
                self.outgoing.push(msg);
            }
        }

        fn handle_forwarded_proposal(&mut self, entry: Entry) {
            if !self.stopped() {
                match self.state {
                    (Role::Leader, Phase::Prepare) => {
                        // println!("Appending forwarded proposals: len: {}, la: {}", p.len(), self.storage.get_sequence_len());
                        self.proposals.push(entry)
                    },
                    (Role::Leader, Phase::Accept) => {
                        // println!("Sending accept forwarded proposals: len: {}, la: {}", proposals.len(), self.storage.get_sequence_len());
                        self.send_accept(entry);

                    },
                    _ => {
                        // println!("Not leader when receiving forwarded proposal... leader: {}", self.leader);
                        self.forward_proposals(entry);
                    },
                }
            }
        }

        fn send_accept(&mut self, entry: Entry) {
            if !self.stopped() {
                for pid in self.lds.keys() {
                    if self.batch_accept {
                        let pid_idx = *pid as usize - 1;
                        match self.batch_accept_meta.get_mut(pid_idx).unwrap() {
                            Some((ballot, idx)) if ballot == &self.n_leader => {
                                let outgoing_len = self.outgoing.len(); // TODO remove
                                let Message{msg, ..} = self.outgoing.get_mut(*idx).expect(&format!("No message in outgoing for node {}. Outgoing len: {}, cached idx: {}", pid, outgoing_len, idx));
                                match msg {
                                    PaxosMsg::Accept(a) => {
                                        a.entries.push(entry.clone());
                                    },
                                    PaxosMsg::AcceptSync(acc) => {
                                        acc.entries.push(entry.clone());
                                    },
                                    _ => panic!("Not Accept or AcceptSync when batching"),
                                }
                            },
                            _ => {
                                let acc = Accept::with(self.n_leader, vec![entry.clone()]);
                                let cache_idx = self.outgoing.len();
                                self.outgoing.push(Message::with(self.pid, *pid, PaxosMsg::Accept(acc)));
                                self.batch_accept_meta[pid_idx] = Some((self.n_leader, cache_idx));
                            }
                        }
                    } else {
                        let acc = Accept::with(self.n_leader, vec![entry.clone()]);
                        self.outgoing.push(Message::with(self.pid, *pid, PaxosMsg::Accept(acc)));
                    }
                }
                self.storage.append_entry(entry);
                self.las.insert(self.pid, self.storage.get_sequence_len());
            }
        }

        fn handle_promise_prepare(&mut self, prom: Promise, from: u64) {
            if prom.n == self.n_leader {
                let sfx_len = prom.sfx.len();
                let promise_meta = &(prom.n_accepted, sfx_len, from);
                if promise_meta > &self.max_promise_meta {
                    self.max_promise_meta = promise_meta.clone();
                    self.max_promise_sfx = prom.sfx;
                }
                self.promises_meta.insert(from, (prom.n_accepted, sfx_len));
                self.lds.insert(from, prom.ld);
                if self.promises_meta.len() >= self.majority {
                    let (max_promise_n, max_sfx_len, max_pid) = self.max_promise_meta;
                    let last_is_stop = match self.max_promise_sfx.last() {
                        Some(e) => e.is_stopsign(),
                        None => false
                    };
                    if max_pid != self.pid {    // sync self with max pid's sequence
                        let my_promise = self.promises_meta.get(&self.pid).unwrap();
                        if my_promise != &(max_promise_n, max_sfx_len) {
                            self.storage.append_on_decided_prefix(mem::take(&mut self.max_promise_sfx));
                        }
                    }
                    if last_is_stop {
                        self.proposals.clear();    // will never be decided
                    } else {
                        Self::drop_after_stopsign(&mut self.proposals); // drop after ss, if ss exists
                    }
                    // create accept_sync with only new proposals for all pids with max_promise
                    let mut new_entries = mem::take(&mut self.proposals);
                    let max_promise_acc_sync = AcceptSync::with(self.n_leader, new_entries.clone(), *(self.lds.get(&max_pid).unwrap_or(&self.acc_sync_ld)), false);
                    // append new proposals in my sequence
                    self.storage.append_sequence(&mut new_entries);
                    self.las.insert(self.pid, self.storage.get_sequence_len());
                    self.state = (Role::Leader, Phase::Accept);
                    // send accept_sync to followers
                    for (pid, lds) in self.lds.iter().filter(|(pid, _)| *pid != &max_pid) {
                        let promise_meta = self.promises_meta.get(&pid).expect(&format!("No promise from {}. Max pid: {}. Promises received from: {:?}", pid, max_pid, self.promises_meta.keys()));
                        if promise_meta == &(max_promise_n, max_sfx_len) {
                            let msg = Message::with(self.pid, *pid, PaxosMsg::AcceptSync(max_promise_acc_sync.clone()));
                            self.outgoing.push(msg);
                        } else {
                            let sfx = self.storage.get_suffix(*lds);
                            let acc_sync = AcceptSync::with(self.n_leader, sfx, *lds, true);
                            let msg = Message::with(self.pid, *pid, PaxosMsg::AcceptSync(acc_sync));
                            self.outgoing.push(msg);
                        }
                        if self.batch_accept {
                            self.batch_accept_meta[*pid as usize - 1] = Some((self.n_leader, self.outgoing.len() - 1));
                        }
                    }
                    if max_pid != self.pid {
                        // send acceptsync to max_pid
                        let msg = Message::with(self.pid, max_pid, PaxosMsg::AcceptSync(max_promise_acc_sync));
                        self.outgoing.push(msg);
                        if self.batch_accept {
                            self.batch_accept_meta[max_pid as usize - 1] = Some((self.n_leader, self.outgoing.len() - 1));
                        }
                    }
                }
            }
        }

        fn handle_promise_accept(&mut self, prom: Promise, from: u64) {
            if prom.n == self.n_leader {
                self.lds.insert(from, prom.ld);
                let sfx = self.storage.get_suffix(prom.ld);
                let acc_sync = AcceptSync::with(self.n_leader, sfx, prom.ld, true);
                self.outgoing.push(Message::with(self.pid, from, PaxosMsg::AcceptSync(acc_sync)));
                // inform what got decided already
                let idx = if self.lc > 0 {
                    self.lc
                } else {
                    self.storage.get_decided_len()
                };
                if idx > prom.ld {
                    let d = Decide::with(idx, self.n_leader);
                    self.outgoing.push(Message::with(self.pid, from, PaxosMsg::Decide(d)));
                }
            }
        }

        fn handle_accepted(&mut self, accepted: Accepted, from: u64) {
            if accepted.n == self.n_leader && self.state == (Role::Leader, Phase::Accept) {
                self.las.insert(from, accepted.la);
                if accepted.la > self.lc {
                    let chosen = self.las.values().filter(|la| *la >= &accepted.la).count() >= self.majority;
                    if chosen {
                        self.lc = accepted.la;
                        let d = Decide::with(self.lc, self.n_leader);
                        for pid in self.lds.keys() {
                            self.outgoing.push(Message::with(self.pid, *pid, PaxosMsg::Decide(d.clone())));
                        }
                        self.handle_decide(d);
                    }
                }
            }
        }

        /*** Follower ***/
        fn handle_prepare(&mut self, prep: Prepare, from: u64) {
            if self.storage.get_promise() < prep.n {
                self.leader = from;
                self.storage.set_promise(prep.n.clone());
                self.state = (Role::Follower, Phase:: Prepare);
                let na = self.storage.get_accepted_ballot();
                let sfx = if na >= prep.n_accepted {
                    self.storage.get_suffix(prep.ld)
                } else {
                    vec![]
                };
                let p = Promise::with(prep.n, na, sfx, self.storage.get_decided_len());
                self.outgoing.push(Message::with(self.pid, from, PaxosMsg::Promise(p)));
            }
        }

        fn handle_accept_sync(&mut self, acc_sync: AcceptSync, from: u64) {
            if self.state == (Role::Follower, Phase::Prepare) {
                if self.storage.get_promise() == acc_sync.n {
                    self.storage.set_accepted_ballot(acc_sync.n.clone());
                    let mut entries = acc_sync.entries;
                    if acc_sync.sync {
                        self.storage.append_on_prefix(acc_sync.ld, &mut entries);
                    } else {
                        self.storage.append_sequence(&mut entries);
                    }
                    self.state = (Role::Follower, Phase::Accept);
                    let accepted = Accepted::with(acc_sync.n, self.storage.get_sequence_len());
                    self.outgoing.push(Message::with(self.pid, from, PaxosMsg::Accepted(accepted)));
                }
            }
        }

        fn handle_accept(&mut self, acc: Accept, from: u64) {
            if self.state == (Role::Follower, Phase::Accept) {
                if self.storage.get_promise() == acc.n {
                    let mut entries = acc.entries;
                    self.storage.append_sequence(&mut entries);
                    let accepted = Accepted::with(acc.n, self.storage.get_sequence_len());
                    self.outgoing.push(Message::with(self.pid, from, PaxosMsg::Accepted(accepted)));
                }
            }
        }

        fn handle_decide(&mut self, dec: Decide) {
            if self.storage.get_promise() == dec.n {
                self.storage.set_decided_len(dec.ld);
            }
        }

        /*** algorithm specific functions ***/

        fn drop_after_stopsign(entries: &mut Vec<Entry>) {   // drop all entries ordered after stopsign (if any)
            let ss_idx = entries.iter().position(|e| e.is_stopsign());
            if let Some(idx) = ss_idx {
                entries.truncate(idx + 1);
            };
        }

        pub fn get_sequence(&self) -> Vec<Entry> {
            self.storage.get_sequence()
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
    }

    #[derive(Clone, Debug, PartialEq)]
    pub enum Entry {
        Normal(Vec<u8>),
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
        ballots: Vec<(Ballot, u64)>,
        current_ballot: Ballot,  // (round, pid)
        leader: Option<(Ballot, u64)>,
        max_ballot: Ballot,
        hb_delay: u64,
        delta: u64,
        majority: usize,
        timer: Option<ScheduledTimer>,
        supervisor: Recipient<KillResponse>,
    }

    impl BallotLeaderComp {
        pub fn with(peers: Vec<ActorPath>, pid: u64, hb_delay: u64, delta: u64, supervisor: Recipient<KillResponse>) -> BallotLeaderComp {
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
                hb_delay,
                delta,
                timer: None,
                supervisor
            }
        }

        fn check_leader(&mut self) {
            self.ballots.push((self.current_ballot, self.pid));
            let mut ballots = Vec::with_capacity(self.peers.len());
            std::mem::swap(&mut self.ballots, &mut ballots);
            let (top_ballot, top_pid) = ballots.into_iter().max().unwrap();
            if top_ballot < self.max_ballot {
                self.current_ballot.n = self.max_ballot.n + 1;
                self.leader = None;
            } else {
                if self.leader != Some((top_ballot, top_pid)) {
                    self.max_ballot = top_ballot;
                    self.leader = Some((top_ballot, top_pid));
                    self.ble_port.trigger(Leader::with(top_pid, top_ballot));
                }
            }
        }

        fn hb_timeout(&mut self) {
            if self.ballots.len() + 1 >= self.majority {
                self.check_leader();
            } else {
                self.ballots.clear();
            }
            self.round += 1;
            for peer in &self.peers {
                let hb_request = HeartbeatRequest::with(self.round, self.max_ballot);
                peer.tell_serialised(HeartbeatMsg::Request(hb_request),self).expect("HBRequest should serialise!");
            }
            self.start_timer();
        }

        fn start_timer(&mut self) {
            let timer = self.schedule_once(
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
                    for peer in &self.peers {
                        let hb_request = HeartbeatRequest::with(self.round, self.max_ballot);
                        peer.tell_serialised(HeartbeatMsg::Request(hb_request),self).expect("HBRequest should serialise!");
                    }
                    self.start_timer();
                },
                ControlEvent::Kill => {
                    self.stop_timer();
                    self.supervisor.tell(KillResponse);
                },
                _ => {}
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
            let NetMessage{sender, data, ..} = m;
            match_deser!{data; {
                hb: HeartbeatMsg [BallotLeaderSer] => {
                    match hb {
                        HeartbeatMsg::Request(req) => {
                            if req.max_ballot > self.max_ballot {
                                self.max_ballot = req.max_ballot;
                            }
                            let hb_reply = HeartbeatReply::with(self.pid, req.round, self.current_ballot);
                            sender.tell_serialised(HeartbeatMsg::Reply(hb_reply), self).expect("HBReply should serialise!");
                        },
                        HeartbeatMsg::Reply(rep) => {
                            if rep.round == self.round {
                                self.ballots.push((rep.max_ballot, rep.sender_pid));
                            } else {
                                debug!(self.ctx.log(), "Got late hb reply. HB delay: {}", self.hb_delay);
                                self.hb_delay += self.delta;
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
    use crate::bench::atomic_broadcast::paxos::raw_paxos::Entry::Normal;
    use super::super::messages::paxos::ballot_leader_election::Ballot;
    use crate::bench::atomic_broadcast::messages::paxos::{Message, PaxosMsg};

    fn create_replica_nodes(n: u64, initial_conf: Vec<u64>, policy: ReconfigurationPolicy) -> (Vec<KompactSystem>, HashMap<u64, ActorPath>, Vec<ActorPath>) {
        let mut systems = Vec::with_capacity(n as usize);
        let mut nodes = HashMap::with_capacity(n as usize);
        let mut actorpaths = Vec::with_capacity(n as usize);
        for i in 1..=n {
            let system =
                kompact_benchmarks::kompact_system_provider::global().new_remote_system_with_threads(format!("paxos_replica{}", i), 4);
            let (replica_comp, unique_reg_f) = system.create_and_register(|| {
                PaxosReplica::<MemorySequence, MemoryState>::with(initial_conf.clone(), policy.clone())
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
        let num_proposals = 4000;
        let batch_size = 2000;
        let config = vec![1,2,3];
        let reconfig: Option<(Vec<u64>, Vec<u64>)> = None;
        // let reconfig = Some((vec![1,2,6,7,8], vec![]));
        let n: u64 = match reconfig {
            None => config.len() as u64,
            Some(ref r) => *(r.0.last().unwrap()),
        };
        let check_sequences = true;
        let policy = ReconfigurationPolicy::Pull;
        let active_n = config.len() as u64;
        let quorum = active_n/2 + 1;

        let (systems, nodes, actorpaths) = create_replica_nodes(n, config, policy);
        /*** Setup client ***/
        let (p, f) = kpromise::<HashMap<u64, Vec<u64>>>();
        let (client_comp, unique_reg_f) = systems[0].create_and_register( || {
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
        let system = systems.first().unwrap();
        let client_comp_f = system.start_notify(&client_comp);
        client_comp_f
            .wait_timeout(Duration::from_secs(2), )
            .expect("ClientComp never started!");
        let named_reg_f = system.register_by_alias(
            &client_comp,
            "client",
        );
        named_reg_f.wait_expect(
            Duration::from_secs(2),
            "Failed to register alias for ClientComp"
        );
        let client_path = ActorPath::Named(NamedPath::with_system(
            system.system_path(),
            vec![String::from("client")],
        ));
        let mut ser_client = Vec::<u8>::new();
        client_path.serialise(&mut ser_client).expect("Failed to serialise ClientComp actorpath");
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
        partitioning_actor.actor_ref().tell(IterationControlMsg::Prepare(Some(ser_client)));
        prepare_latch.wait();
        partitioning_actor.actor_ref().tell(IterationControlMsg::Run);
        client_comp.actor_ref().tell(Run);
        let all_sequences = f.wait_timeout(Duration::from_secs(60)).expect("Failed to get results");
        let client_sequence = all_sequences.get(&0).expect("Client's sequence should be in 0...").to_owned();
        for system in systems {
            system.shutdown().expect("Kompact didn't shut down properly");
        }

        assert_eq!(num_proposals, client_sequence.len() as u64);
        for i in 1..=num_proposals {
            let mut iter = client_sequence.iter();
            let found = iter.find(|&&x| x == i).is_some();
            assert_eq!(true, found);
        }
        if check_sequences {
            let mut counter = 0;
            for i in 1..=n {
                let sequence = all_sequences.get(&i).expect(&format!("Did not get sequence for node {}", i));
                // println!("Node {}: {:?}", i, sequence.len());
                // assert!(client_sequence.starts_with(sequence));
                if let Some(r) = &reconfig {
                    if r.0.contains(&i) {
                        for id in &client_sequence {
                            if !sequence.contains(&id) {
                                println!("Node {} did not have id: {} in sequence", i, id);
                                counter += 1;
                                break;
                            }
                        }
                    }
                }
            }
            if counter >= quorum {
                panic!("Majority of new configuration DOES NOT have all client elements: counter: {}, quorum: {}", counter, quorum);
            }
        }
        println!("PASSED!!!");
    }
}
