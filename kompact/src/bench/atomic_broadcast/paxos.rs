use kompact::prelude::*;
use super::storage::paxos::*;
use std::fmt::{Debug};
use ballot_leader_election::{BallotLeaderComp, BallotLeaderElection, Stop as BLEStop};
use raw_paxos::{Entry, Paxos};
use std::sync::Arc;
use std::time::Duration;
use super::messages::{*, StopMsg as NetStopMsg};
use super::messages::paxos::ballot_leader_election::Leader;
use super::messages::paxos::{ReconfigInit, ReconfigSer, SequenceTransfer, SequenceRequest, SequenceMetaData, Reconfig, ReconfigurationMsg};
// use crate::bench::atomic_broadcast::messages::{Proposal, ProposalResp, AtomicBroadcastMsg, AtomicBroadcastDeser, RECONFIG_ID};
use crate::partitioning_actor::{PartitioningActorSer, PartitioningActorMsg, Init};
use uuid::Uuid;
use kompact::prelude::Buf;
use rand::Rng;
use super::communicator::{CommunicationPort, Communicator};
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
    LocalSequenceReq(ActorPath, SequenceRequest, SequenceMetaData),
    GetAllEntries(Ask<(), Vec<Entry>>),
    Stop
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
    paxos_comps: Vec<Arc<Component<PaxosComp<S, P>>>>,
    ble_comps: Vec<Arc<Component<BallotLeaderComp>>>,
    communicator_comps: Vec<Arc<Component<Communicator>>>,
    active_config: (u32, usize),    // (config_id, idx)
    leader_in_active_config: u64,
    nodes: Vec<ActorPath>, // derive actorpaths of peers' ble and paxos replicas from these
    prev_sequences: HashMap<u32, Arc<S>>,   // TODO vec
    stopped: bool,
    client_stopped: bool,
    iteration_id: u32,
    partitioning_actor: Option<ActorPath>,
    alias_registrations: HashSet<Uuid>,
    policy: ReconfigurationPolicy,
    next_config_id: Option<u32>,
    pending_seq_transfers: Vec<(Vec<u32>, Vec<Entry>)>,   // (remaining_segments, entries)
    complete_sequences: Vec<u32>,
    active_peers: (Vec<u64>, Vec<u64>), // (ready, not_ready)
    retry_transfer_timers: HashMap<u32, ScheduledTimer>,
    cached_client: Option<ActorPath>,
    pending_stop_comps: usize,
    pending_kill_comps: usize,
    cleanup_latch: Option<Ask<(), ()>>,
    hb_proposals: Vec<NetMessage>,
}

impl<S, P> PaxosReplica<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    pub fn with(initial_config: Vec<u64>, policy: ReconfigurationPolicy) -> PaxosReplica<S, P> {
        PaxosReplica {
            ctx: ComponentContext::new(),
            pid: 0,
            initial_config,
            paxos_comps: vec![],
            ble_comps: vec![],
            communicator_comps: vec![],
            active_config: (0, 0),
            leader_in_active_config: 0,
            nodes: vec![],
            prev_sequences: HashMap::new(),
            stopped: false,
            client_stopped: false,
            iteration_id: 0,
            partitioning_actor: None,
            alias_registrations: HashSet::new(),
            policy,
            next_config_id: None,
            pending_seq_transfers: vec![],
            complete_sequences: vec![],
            active_peers: (vec![], vec![]),
            retry_transfer_timers: HashMap::new(),
            cached_client: None,
            pending_stop_comps: 0,
            pending_kill_comps: 0,
            cleanup_latch: None,
            hb_proposals: vec![]
        }
    }

    fn create_replica(&mut self, config_id: u32, nodes: Vec<u64>, register_alias_with_response: bool, start: bool, ble_prio: bool, quick_start: bool) {
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
        let stopkill_recipient: Recipient<StopKillResponse> = self.ctx.actor_ref().recipient();
        /*** create and register Paxos ***/
        let paxos_comp = system.create(|| {
            PaxosComp::with(self.ctx.actor_ref(), peers, config_id, self.pid)
        });
        system.register_without_response(&paxos_comp);
        /*** create and register Communicator ***/
        let kill_recipient: Recipient<KillResponse> = self.ctx.actor_ref().recipient();
        let communicator = system.create( || {
            Communicator::with(
                communicator_peers,
                self.cached_client.as_ref().expect("No cached client!").clone(),
                kill_recipient
            )
        });
        system.register_without_response(&communicator);
        /*** create and register BLE ***/
        let ble_comp = system.create( || {
            BallotLeaderComp::with(ble_peers, self.pid, ELECTION_TIMEOUT, BLE_DELTA, stopkill_recipient, ble_prio, quick_start)
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

        if start {
            info!(self.ctx.log(), "Starting replica pid: {}, config_id: {}", self.pid, config_id);
            self.active_config = (config_id, self.paxos_comps.len());
            self.leader_in_active_config = 0;
            self.next_config_id = None;
            system.start(&paxos_comp);
            system.start(&ble_comp);
            system.start(&communicator);
        } else {
            self.next_config_id = Some(config_id);
        }

        self.paxos_comps.push(paxos_comp);
        self.ble_comps.push(ble_comp);
        self.communicator_comps.push(communicator);
    }

    fn start_replica(&mut self) {
        if let Some(config_id) = self.next_config_id {
            info!(self.ctx.log(), "Starting replica pid: {}, config_id: {}", self.pid, config_id);
            let idx = self.paxos_comps.len() - 1;
            self.active_config = (config_id, idx);
            self.leader_in_active_config = 0;
            let paxos = self.paxos_comps
                .get(idx)
                .expect(&format!("Could not find PaxosComp with config_id: {}", config_id));
            let ble = self.ble_comps
                .get(idx)
                .expect(&format!("Could not find BLE config_id: {}", config_id));
            let communicator = self.communicator_comps
                .get(idx)
                .expect(&format!("Could not find Communicator with config_id: {}", config_id));
            self.ctx.system().start(paxos);
            self.ctx.system().start(ble);
            self.ctx.system().start(communicator);
            self.next_config_id = None;
        }
    }

    fn stop_all_replicas(&mut self) {
        self.pending_stop_comps = self.ble_comps.len() + self.paxos_comps.len();
        if self.pending_stop_comps == 0 && self.client_stopped {
            self.send_stop_ack();
            if self.cleanup_latch.is_some() {
                self.kill_all_replicas();
            }
        } else {
            for ble in &self.ble_comps {
                ble.actor_ref().tell(BLEStop(self.pid));
            }
            for paxos in &self.paxos_comps {
                paxos.actor_ref().tell(PaxosCompMsg::Stop);
            }
        }
    }

    fn kill_all_replicas(&mut self) {
        assert!(self.stopped, "Tried to kill replicas but not stopped");
        assert_eq!(self.pending_stop_comps, 0, "Tried to kill replicas but all replicas not stopped");
        self.pending_kill_comps = self.ble_comps.len() + self.paxos_comps.len() + self.communicator_comps.len();
        debug!(self.ctx.log(), "Killing {} child components...", self.pending_kill_comps);
        if self.pending_kill_comps == 0 && self.client_stopped {
            // info!(self.ctx.log(), "Killed all components. Decrementing cleanup latch");
            self.cleanup_latch.take().expect("No cleanup latch").reply(()).expect("Failed to reply clean up latch");
        } else {
            let system = self.ctx.system();
            for ble in self.ble_comps.drain(..) {
                system.kill(ble);
            }
            for paxos in self.paxos_comps.drain(..) {
                system.kill(paxos);
            }
            for communicator in self.communicator_comps.drain(..) {
                system.kill(communicator);
            }
        }
    }

    fn new_iteration(&mut self, init: Init) {
        self.stopped = false;
        self.client_stopped = false;
        self.nodes = init.nodes;
        self.pid = init.pid as u64;
        self.iteration_id = init.init_id;
        let ser_client = init.init_data.expect("Init should include ClientComp's actorpath");
        let client = ActorPath::deserialise(&mut ser_client.as_slice()).expect("Failed to deserialise Client's actorpath");
        self.cached_client = Some(client);
        if self.initial_config.contains(&self.pid){
            self.next_config_id = Some(1);
            self.create_replica(1, self.initial_config.clone(), true, false, false, true);
        } else {
            let resp = PartitioningActorMsg::InitAck(self.iteration_id);
            let ap = self.partitioning_actor.take().expect("PartitioningActor not found!");
            ap.tell_serialised(resp, self).expect("Should serialise");
        }
    }

    fn deserialise_and_propose(&self, m: NetMessage) {
        if let AtomicBroadcastMsg::Proposal(p) = m.try_deserialise_unchecked::<AtomicBroadcastMsg, AtomicBroadcastDeser>().expect("Should be AtomicBroadcastMsg!") {
            let active_paxos = &self.paxos_comps.get(self.active_config.1).expect("Could not get PaxosComp actor ref despite being leader");
            active_paxos.actor_ref().tell(PaxosCompMsg::Propose(p));
        }
    }

    fn send_stop_ack(&self) {
        // info!(self.ctx.log(), "Sending StopAck");
        self.partitioning_actor
            .as_ref()
            .unwrap()
            .tell_serialised(PartitioningActorMsg::StopAck, self)
            .expect("Should serialise");
    }

    fn pull_sequence(&mut self, config_id: u32, seq_len: u64) {
        let num_ready_peers = self.active_peers.0.len();
        let num_unready_peers = self.active_peers.1.len();
        let num_continued_nodes = num_ready_peers + num_unready_peers;
        let idx = config_id as usize - 1;
        let rem_segments: Vec<_> =  (1..=num_continued_nodes).map(|x| x as u32).collect();
        self.pending_seq_transfers[idx] = (rem_segments, vec![Entry::Normal(vec![]); seq_len as usize]);
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
        let timer = self.schedule_once(Duration::from_millis(TRANSFER_TIMEOUT), move |c, _| c.retry_request_sequence(config_id, seq_len, num_continued_nodes as u64));
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

    fn retry_request_sequence(&mut self, config_id: u32, seq_len: u64, total_segments: u64) {
        if let Some((rem_segments, _)) = self.pending_seq_transfers.get(config_id as usize) {
            let offset = seq_len/total_segments;
            let num_active = self.active_peers.0.len();
            if num_active > 0 {
                for tag in rem_segments {
                    let i = tag - 1;
                    let from_idx = i as u64 * offset;
                    let to_idx = from_idx + offset;
                    // info!(self.ctx.log(), "Retrying timed out seq transfer: tag: {}, idx: {}-{}, policy: {:?}", tag, from_idx, to_idx, self.policy);
                    let pid = self.active_peers.0.get(i as usize % num_active).expect(&format!("Failed to get active pid. idx: {}, len: {}", i, self.active_peers.0.len()));
                    self.request_sequence(*pid, config_id, from_idx, to_idx, *tag);
                }
            }
            let timer = self.schedule_once(Duration::from_millis(TRANSFER_TIMEOUT), move |c, _| c.retry_request_sequence(config_id, seq_len, total_segments));
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

    fn append_transferred_segment(&mut self, st: SequenceTransfer) {
        let (rem_transfers, sequence) = self.pending_seq_transfers.get_mut(st.config_id as usize - 1).expect("Should have initialised pending sequence");
        let tag = st.tag;
        let offset = st.from_idx as usize;
        for (i, entry) in st.entries.into_iter().enumerate() {
            sequence[offset + i] = entry;
        }
        // PaxosSer::deserialise_entries_into(&mut st.entries.as_slice(), sequence, offset);
        rem_transfers.retain(|t| t != &tag);
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
        // info!(self.ctx.log(), "Creating eager sequence transfer. Tag: {}, idx: {}-{}, continued_nodes: {:?}", tag, from_idx, to_idx, continued_nodes);
        let entries = final_seq.get_entries(from_idx, to_idx).to_vec();
        let prev_seq_metadata = self.get_sequence_metadata(config_id-1);
        let st = SequenceTransfer::with(config_id, tag, true, from_idx, to_idx, entries, prev_seq_metadata);
        st
    }

    fn handle_sequence_request(&mut self, sr: SequenceRequest, requestor: ActorPath) {
        if self.leader_in_active_config == sr.requestor_pid { return; }
        let (succeeded, entries) = match self.prev_sequences.get(&sr.config_id) {
            Some(seq) => {
                let ents = seq.get_entries(sr.from_idx, sr.to_idx).to_vec();
                (true, ents)
            },
            None => {
                // (false, vec![])
                if self.active_config.0 == sr.config_id {  // we have not reached final sequence, but might still have requested elements. Outsource request to corresponding PaxosComp
                    let paxos = self.paxos_comps.get(self.active_config.1).expect(&format!("No paxos comp with config_id: {} when handling SequenceRequest. Len of PaxosComps: {}", sr.config_id, self.paxos_comps.len()));
                    let prev_seq_metadata = self.get_sequence_metadata(sr.config_id-1);
                    paxos.actor_ref().tell(PaxosCompMsg::LocalSequenceReq(requestor, sr, prev_seq_metadata));
                    return;
                } else {
                    (false, vec![])
                }
            }
        };
        let prev_seq_metadata = self.get_sequence_metadata(sr.config_id-1);
        let st = SequenceTransfer::with(sr.config_id, sr.tag, succeeded, sr.from_idx, sr.to_idx, entries, prev_seq_metadata);
        // info!(self.ctx.log(), "Replying seq transfer: tag: {}, idx: {}-{}", st.tag, st.from_idx, st.to_idx);
        requestor.tell_serialised(ReconfigurationMsg::SequenceTransfer(st), self).expect("Should serialise!");
    }

    fn handle_sequence_transfer(&mut self, st: SequenceTransfer) {
        if self.active_config.0 > st.config_id || self.complete_sequences.contains(&st.config_id) || self.next_config_id.unwrap_or(0) <= st.config_id {
            return; // ignore late sequence transfers
        }
        let prev_config_id = st.metadata.config_id;
        let prev_seq_len = st.metadata.len;
        // pull previous sequence if exists and not already started
        if prev_config_id != 0 && !self.complete_sequences.contains(&prev_config_id){
            let idx = prev_config_id as usize - 1;
            if let Some((rem_segments, _)) = self.pending_seq_transfers.get(idx) {
                if rem_segments.is_empty() {
                    self.pull_sequence(prev_config_id, prev_seq_len);
                }
            }
        }
        let succeeded = st.succeeded;
        let config_id = st.config_id;
        if succeeded {
            let idx = config_id as usize - 1;
            self.append_transferred_segment(st);
            let got_all_segments = self.pending_seq_transfers[idx].0.is_empty();
            if got_all_segments {
                self.complete_sequences.push(config_id);
                let mut c = (vec![], vec![]);
                std::mem::swap(&mut c, &mut self.pending_seq_transfers[idx]);
                self.prev_sequences.insert(config_id, Arc::new(S::new_with_sequence(c.1)));
                if let Some(timer) = self.retry_transfer_timers.remove(&config_id){
                    self.cancel_timer(timer);
                }
                if self.complete_sequences.len() + 1 == self.next_config_id.unwrap() as usize{  // got all sequence transfers
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
    StopResp,
    KillResp,
    CleanupIteration(Ask<(), ()>)
}

#[derive(Debug)]
pub enum StopKillResponse {
    StopResp,
    KillResp
}

impl<S> From<StopKillResponse> for PaxosReplicaMsg<S> where S: SequenceTraits{
    fn from(s: StopKillResponse) -> Self {
        match s {
            StopKillResponse::StopResp => PaxosReplicaMsg::StopResp,
            StopKillResponse::KillResp => PaxosReplicaMsg::KillResp,
        }
    }
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
        match msg {
            PaxosReplicaMsg::Leader(config_id, pid) => {
                if self.active_config.0 == config_id {
                    if self.leader_in_active_config == 0 {
                        let hb_proposals = std::mem::take(&mut self.hb_proposals);
                        if pid == self.pid {  // notify client if no leader before
                            self.cached_client
                                .as_ref()
                                .expect("No cached client!")
                                .tell_serialised(AtomicBroadcastMsg::FirstLeader(pid), self)
                                .expect("Should serialise FirstLeader");
                            for net_msg in hb_proposals {
                                self.deserialise_and_propose(net_msg);
                            }
                        } else if pid != self.pid && !hb_proposals.is_empty() {
                            let idx = pid as usize - 1;
                            let leader = self.nodes.get(idx).unwrap_or_else(|| panic!("Could not get leader's actorpath. Pid: {}", self.leader_in_active_config));
                            for m in hb_proposals {
                                leader.forward_with_original_sender(m, self);
                            }
                        }
                    }
                    self.leader_in_active_config = pid;
                }
            },
            PaxosReplicaMsg::Reconfig(r) => {
                let prev_config_id = self.active_config.0;
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
                        let st = self.create_eager_sequence_transfer(&nodes, prev_config_id);
                        for pid in &new_nodes {
                            let idx = *pid as usize - 1;
                            let actorpath = self.nodes.get(idx).expect(&format!("No actorpath found for new node {}", pid));
                            actorpath.tell_serialised(ReconfigurationMsg::SequenceTransfer(st.clone()), self).expect("Should serialise!");
                        }
                    }
                    nodes.append(&mut new_nodes);
                    let ble_prio = self.leader_in_active_config == self.pid && cfg!(feature = "headstart_ble");
                    self.create_replica(r.config_id, nodes, false, true, ble_prio, ble_prio);
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
            PaxosReplicaMsg::StopResp => {
                assert!(self.stopped, "Got StopResp when not stopped");
                assert!(self.pending_stop_comps > 0, "Got unexpected StopResp when no pending stop comps");
                self.pending_stop_comps -= 1;
                if self.pending_stop_comps == 0 && self.client_stopped {
                    self.send_stop_ack();
                    if self.cleanup_latch.is_some() {
                        self.kill_all_replicas();
                    }
                }
            },
            PaxosReplicaMsg::KillResp => {
                assert!(self.stopped, "Got KillResp when not stopped");
                assert!(self.pending_kill_comps > 0, "Got unexpected KillResp when no pending kill comps");
                self.pending_kill_comps -= 1;
                // info!(self.ctx.log(), "Got kill response. Remaining: {}", self.pending_kill_comps);
                if self.pending_kill_comps == 0 && self.client_stopped {
                    // info!(self.ctx.log(), "Killed all components. Decrementing cleanup latch");
                    self.cleanup_latch.take().expect("No cleanup latch").reply(()).expect("Failed to reply clean up latch");
                }
            },
            PaxosReplicaMsg::CleanupIteration(a) => {
                self.cleanup_latch = Some(a);
                if self.stopped && self.pending_stop_comps == 0 {
                    self.kill_all_replicas();
                }
            },
        }
    }

    fn receive_network(&mut self, m: NetMessage) -> () {
        trace!(self.ctx.log(), "PaxosReplica got {:?}", m);
        match m.data.ser_id {
            ATOMICBCAST_ID => {
                if !self.stopped {
                    match self.leader_in_active_config {
                        my_pid if my_pid == self.pid => self.deserialise_and_propose(m),
                        other_pid if other_pid > 0 => {
                            let idx = self.leader_in_active_config as usize - 1;
                            let leader = self.nodes.get(idx).unwrap_or_else(|| panic!("Could not get leader's actorpath. Pid: {}", self.leader_in_active_config));
                            leader.forward_with_original_sender(m, self);
                        },
                        _ => {
                            self.hb_proposals.push(m);
                        },
                    }
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
                                self.stopped = true;
                                self.partitioning_actor = Some(sender);
                                self.stop_all_replicas();
                                let retry_timers = std::mem::take(&mut self.retry_transfer_timers);
                                for (_, timer) in retry_timers {
                                    self.cancel_timer(timer);
                                }
                                self.active_config.0 = 0;
                                self.leader_in_active_config = 0;
                                self.next_config_id = None;
                                self.prev_sequences.clear();
                                self.active_peers.0.clear();
                                self.active_peers.1.clear();
                                self.complete_sequences.clear();
                            },
                            _ => unimplemented!()
                        }
                    },
                    rm: ReconfigurationMsg [ReconfigSer] => {
                        match rm {
                            ReconfigurationMsg::Init(r) => {
                                if self.stopped || self.active_config.0 >= r.config_id {
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
                                            self.create_replica(r.config_id, nodes, false, true, false, false);
                                        } else {
                                            self.pending_seq_transfers = vec![(vec![], vec![]); r.config_id as usize];
                                            match self.policy {
                                                ReconfigurationPolicy::Pull => self.pull_sequence(r.seq_metadata.config_id, r.seq_metadata.len),
                                                ReconfigurationPolicy::Eager => {
                                                    let config_id = r.seq_metadata.config_id;
                                                    let seq_len = r.seq_metadata.len;
                                                    let idx = config_id as usize - 1;
                                                    let rem_segments: Vec<_> = (1..=num_expected_transfers).map(|x| x as u32).collect();
                                                    self.pending_seq_transfers[idx] = (rem_segments, vec![Entry::Normal(vec![]); seq_len as usize]);
                                                    let timer = self.schedule_once(Duration::from_millis(TRANSFER_TIMEOUT/2), move |c, _| c.retry_request_sequence(config_id, seq_len, num_expected_transfers as u64));
                                                    self.retry_transfer_timers.insert(config_id, timer);
                                                },
                                            }
                                            self.create_replica(r.config_id, nodes, false, false, false, false);
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
                    client_stop: NetStopMsg [StopMsgDeser] => {
                        if let NetStopMsg::Client = client_stop {
                            // info!(self.ctx.log(), "Got client stop");
                            self.client_stopped = true;
                            if self.stopped && self.pending_stop_comps == 0 {
                                self.send_stop_ack();
                                if self.cleanup_latch.is_some() {
                                    self.kill_all_replicas();
                                }
                            }
                        }
                    },
                    tm: TestMessage [TestMessageSer] => {
                        match tm {
                            TestMessage::SequenceReq => {
                                let mut all_entries = vec![];
                                let mut unique = HashSet::new();
                                for i in 1..self.active_config.0 {
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
                                if self.active_config.0 > 0 {
                                    let active_paxos = self.paxos_comps.get(self.active_config.1).unwrap();
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
                                    warn!(self.ctx.log(), "Got SequenceReq but no active paxos: {}", self.active_config.0);
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
    pending_reconfig: bool,
    stopped: bool,
    stopped_peers: HashSet<u64>
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
    ) -> PaxosComp<S, P>
    {
        let seq = S::new();
        let paxos_state = P::new();
        let storage = Storage::with(seq, paxos_state);
        let paxos = Paxos::with(config_id, pid, peers.clone(), storage);
        PaxosComp {
            ctx: ComponentContext::new(),
            supervisor,
            communication_port: RequiredPort::new(),
            ble_port: RequiredPort::new(),
            stopped_peers: HashSet::with_capacity(peers.len()),
            peers,
            paxos,
            config_id,
            pid,
            current_leader: 0,
            timers: None,
            pending_reconfig: false,
            stopped: false,
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
        if self.current_leader != self.paxos.leader {
            self.current_leader = self.paxos.leader;
            self.supervisor.tell(PaxosReplicaMsg::Leader(self.config_id, self.current_leader));
        }
        let decided_entries = self.paxos.get_decided_entries();
        let stopsign = match decided_entries.last() {
            Some(Entry::StopSign(ss)) => Some(ss.clone()),
            _ => None
        };
        if self.current_leader == self.pid {
            for decided in decided_entries.to_vec() {
                if let Entry::Normal(data) = decided {
                    let pr = ProposalResp::with(data, self.current_leader);
                    self.communication_port.trigger(CommunicatorMsg::ProposalResponse(pr));
                }
            }
        }
        if let Some(ss) = stopsign {
            let final_seq = self.paxos.stop_and_get_sequence();
            let new_config_len = ss.nodes.len();
            let mut data: Vec<u8> = Vec::with_capacity( 8 + 4 + 8 * new_config_len);
            data.put_u64(RECONFIG_ID);
            data.put_u32(new_config_len as u32);
            for pid in &ss.nodes {
                data.put_u64(*pid);
            }
            let (continued_nodes, new_nodes) = ss.nodes.iter().partition(
                |&pid| pid == &self.pid || self.peers.contains(pid)
            );
            debug!(self.ctx.log(), "Decided StopSign! Continued: {:?}, new: {:?}", &continued_nodes, &new_nodes);
            let nodes = Reconfig::with(continued_nodes, new_nodes);
            let r = FinalMsg::with(ss.config_id, nodes, final_seq);
            self.supervisor.tell(PaxosReplicaMsg::Reconfig(r));
            let leader = 0; // we don't know who will become leader in new config
            let pr = ProposalResp::with(data, leader);
            self.communication_port.trigger(CommunicatorMsg::ProposalResponse(pr));
        }
    }

    fn propose(&mut self, p: Proposal) -> bool {
        match p.reconfig {
            Some((reconfig, _)) => {
                self.paxos.propose_reconfiguration(reconfig)
            },
            None => {
                self.paxos.propose_normal(p.data)
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
                let succeeded = self.propose(p);
                if !succeeded && !self.pending_reconfig {
                    self.pending_reconfig = true;
                    self.communication_port.trigger(CommunicatorMsg::PendingReconfiguration);
                }
            },
            PaxosCompMsg::LocalSequenceReq(requestor, seq_req, prev_seq_metadata) => {
                let (succeeded, entries) = self.paxos.get_chosen_entries(seq_req.from_idx, seq_req.to_idx);
                let st = SequenceTransfer::with(seq_req.config_id, seq_req.tag, succeeded, seq_req.from_idx, seq_req.to_idx, entries, prev_seq_metadata);
                requestor.tell_serialised(ReconfigurationMsg::SequenceTransfer(st), self).expect("Should serialise!");
            },
            PaxosCompMsg::GetAllEntries(a) => { // for testing only
                let seq = self.paxos.get_sequence();
                a.reply(seq).expect("Failed to reply to GetAllEntries");
            },
            PaxosCompMsg::Stop => {
                self.communication_port.trigger(CommunicatorMsg::SendStop(self.pid));
                self.stop_timers();
                self.stopped = true;
                if self.stopped_peers.len() == self.peers.len() {
                    // info!(self.ctx.log(), "Got stopped from all Paxos peers");
                    self.supervisor.tell(PaxosReplicaMsg::StopResp);
                }
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
        match msg {
            AtomicBroadcastCompMsg::RawPaxosMsg(pm) if !self.stopped => {
                self.paxos.handle(pm);
            },
            AtomicBroadcastCompMsg::StopMsg(pid) => {
                assert!(self.stopped_peers.insert(pid), "Got duplicate stop from peer {}", pid);
                // info!(self.ctx.log(), "Got stopped from Paxos peer {}", pid);
                if self.stopped && self.stopped_peers.len() == self.peers.len() {
                    // info!(self.ctx.log(), "Got stopped from all peers");
                    self.supervisor.tell(PaxosReplicaMsg::StopResp);
                }
            },
            _ => {}
        }
    }
}

impl<S, P> Require<BallotLeaderElection> for PaxosComp<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    fn handle(&mut self, l: Leader) -> () {
        // info!(self.ctx.log(), "{}", format!("Node {} became leader in config {}. Ballot: {:?}",  l.pid, self.config_id, l.ballot));
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
    use std::mem;
    use std::sync::Arc;
    use crate::bench::atomic_broadcast::parameters::MAX_INFLIGHT;

    #[derive(ComponentDefinition)]
    pub struct Paxos<S, P> where
        S: SequenceTraits,
        P: PaxosStateTraits
    {
        ctx: ComponentContext<Self>,
        supervisor: ActorRef<PaxosReplicaMsg<S>>,
        communication_port: RequiredPort<CommunicationPort, Self>,
        ble_port: RequiredPort<BallotLeaderElection, Self>,
        storage: Storage<S, P>,
        config_id: u32,
        pid: u64,
        majority: usize,
        peers: Vec<u64>,    // excluding self pid
        state: (Role, Phase),
        pub leader: u64,
        n_leader: Ballot,
        promises_meta: Vec<Option<(Ballot, usize)>>,
        las: Vec<u64>,
        lds: Vec<Option<u64>>,
        proposals: Vec<Entry>,
        lc: u64,    // length of longest chosen seq
        prev_ld: u64,
        acc_sync_ld: u64,
        max_promise_meta: (Ballot, usize, u64),  // ballot, sfx len, pid
        max_promise_sfx: Vec<Entry>,
        batch_accept_meta: Vec<Option<(Ballot, usize)>>,    //  ballot, index in outgoing
        latest_decide_meta: Vec<Option<(Ballot, usize)>>,
        latest_accepted_meta: Option<(Ballot, usize)>,
        outgoing: Vec<Message>,
        num_nodes: usize,
    }

    impl<S, P> Paxos<S, P> where
        S: SequenceTraits,
        P: PaxosStateTraits
    {
        /*** User functions ***/
        pub fn with(
            supervisor: ActorRef<PaxosReplicaMsg<S>>,
            config_id: u32,
            pid: u64,
            peers: Vec<u64>,
            storage: Storage<S, P>,
        ) -> Paxos<S, P> {
            let num_nodes = &peers.len() + 1;
            let majority = num_nodes/2 + 1;
            let n_leader = Ballot::with(0, 0);
            let max_peer_pid = peers.iter().max().unwrap();
            let max_pid = std::cmp::max(max_peer_pid, &pid);
            let num_nodes = *max_pid as usize;
            Paxos {
                ctx: ComponentContext::new(),
                supervisor,
                communication_port: RequiredPort::new(),
                ble_port: RequiredPort::new(),
                storage,
                pid,
                config_id,
                majority,
                peers,
                state: (Role::Follower, Phase::None),
                leader: 0,
                n_leader,
                promises_meta: vec![None; num_nodes],
                las: vec![0; num_nodes],
                lds: vec![None; num_nodes],
                proposals: Vec::with_capacity(MAX_INFLIGHT),
                lc: 0,
                prev_ld: 0,
                acc_sync_ld: 0,
                max_promise_meta: (Ballot::with(0, 0), 0, 0),
                max_promise_sfx: vec![],
                batch_accept_meta: vec![None; num_nodes],
                latest_decide_meta: vec![None; num_nodes],
                latest_accepted_meta: None,
                outgoing: Vec::with_capacity(MAX_INFLIGHT),
                num_nodes,
            }
        }

        pub fn get_outgoing_msgs(&mut self) -> Vec<Message> {
            let mut outgoing = Vec::with_capacity(MAX_INFLIGHT);
            std::mem::swap(&mut self.outgoing, &mut outgoing);
            if cfg!(feature = "batch_accept") {
                self.batch_accept_meta = vec![None; self.num_nodes];
            }
            if cfg!(feature = "latest_decide") {
                self.latest_decide_meta = vec![None; self.num_nodes];
            }
            if cfg!(feature = "latest_accepted") {
                self.latest_accepted_meta = None;
            }
            outgoing
        }

        pub fn get_decided_entries(&mut self) -> &[Entry] {
            let ld = self.storage.get_decided_len();
            if self.prev_ld < ld {
                let decided = self.storage.get_entries(self.prev_ld, ld);
                self.prev_ld = ld;
                decided
            } else {
                &[]
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
                PaxosMsg::AcceptDecide(acc) => self.handle_acceptdecide(acc, m.from),
                PaxosMsg::Accepted(accepted) => self.handle_accepted(accepted, m.from),
                PaxosMsg::Decide(d) => self.handle_decide(d),
                PaxosMsg::ProposalForward(proposals) => self.handle_forwarded_proposal(proposals),
            }
        }

        pub fn stopped(&self) -> bool { self.storage.stopped() }

        pub fn propose_normal(&mut self, data: Vec<u8>) -> bool {
            if self.stopped(){
                false
            } else {
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
                true
            }
        }

        pub fn propose_reconfiguration(&mut self, nodes: Vec<u64>) -> bool {
            if self.stopped(){
                false
            } else {
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
                true
            }
        }

        pub fn get_chosen_entries(&self, from_idx: u64, to_idx: u64) -> (bool, Vec<Entry>) {
            let ld = self.storage.get_decided_len();
            let max_idx = std::cmp::max(ld, self.lc);
            if to_idx > max_idx {
                (false, vec![])
            } else {
                (true, self.storage.get_entries(from_idx, to_idx).to_vec())
            }
        }

        pub(crate) fn stop_and_get_sequence(&mut self) -> Arc<S> {
            self.storage.stop_and_get_sequence()
        }

        fn clear_peers_state(&mut self) {
            self.las = vec![0; self.num_nodes];
            self.promises_meta = vec![None; self.num_nodes];
            self.lds = vec![None; self.num_nodes];
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
                self.promises_meta[self.pid as usize - 1] = Some((na, sfx_len));
                self.max_promise_sfx = sfx;
                /* insert my longest decided sequnce */
                self.acc_sync_ld = ld;
                /* initialise longest chosen sequence and update state */
                self.lc = 0;
                self.state = (Role::Leader, Phase::Prepare);
                /* send prepare */
                for pid in &self.peers {
                    let prep = Prepare::with(n, ld, self.storage.get_accepted_ballot());
                    let msg = Message::with(self.pid, *pid, PaxosMsg::Prepare(prep));
                    self.communication_port.trigger(CommunicatorMsg::RawPaxosMsg(msg));
                }
            } else {
                self.state.0 = Role::Follower;
                self.leader = n.pid;
                let proposals = mem::take(&mut self.proposals);
                if !proposals.is_empty() {
                    for p in proposals {
                        self.forward_proposals(p);
                    }
                }
            }
        }

        fn forward_proposals(&mut self, entry: Entry) {
            if self.leader > 0 {
                let pf = PaxosMsg::ProposalForward(entry);
                let msg = Message::with(self.pid, self.leader, pf);
                // println!("Forwarding to node {}", self.leader);
                self.outgoing.push(msg);
            } else {
                self.proposals.push(entry);
            }
        }

        fn handle_forwarded_proposal(&mut self, entry: Entry) {
            if !self.stopped() {
                match self.state {
                    (Role::Leader, Phase::Prepare) => {
                        self.proposals.push(entry)
                    },
                    (Role::Leader, Phase::Accept) => {
                        self.send_accept(entry);

                    },
                    _ => {
                        self.forward_proposals(entry);
                    },
                }
            }
        }

        fn send_accept(&mut self, entry: Entry) {
            let promised_idx = self.lds.iter().enumerate().filter(|(_, x)| x.is_some()).map(|(idx, _)| idx);
            for idx in promised_idx {
                if cfg!(feature = "batch_accept") {
                    match self.batch_accept_meta.get_mut(idx) {
                        Some(Some((ballot, outgoing_idx))) if ballot == &self.n_leader => {
                            let Message{msg, ..} = self.outgoing.get_mut(*outgoing_idx).unwrap();
                            match msg {
                                PaxosMsg::AcceptDecide(a) => {
                                    a.entries.push(entry.clone());
                                },
                                PaxosMsg::AcceptSync(acc) => {
                                    acc.entries.push(entry.clone());
                                },
                                _ => panic!("Not Accept or AcceptSync when batching"),
                            }
                        },
                        _ => {
                            let acc = AcceptDecide::with(self.n_leader, self.lc, vec![entry.clone()]);
                            let cache_idx = self.outgoing.len();
                            let pid = idx as u64 + 1;
                            self.outgoing.push(Message::with(self.pid, pid, PaxosMsg::AcceptDecide(acc)));
                            self.batch_accept_meta[idx] = Some((self.n_leader, cache_idx));
                        }
                    }
                } else {
                    let pid = idx as u64 + 1;
                    let acc = AcceptDecide::with(self.n_leader, self.lc, vec![entry.clone()]);
                    self.outgoing.push(Message::with(self.pid, pid, PaxosMsg::AcceptDecide(acc)));
                }
            }
            self.storage.append_entry(entry);
            self.las[self.pid as usize - 1] = self.storage.get_sequence_len();
        }

        fn handle_promise_prepare(&mut self, prom: Promise, from: u64) {
            if prom.n == self.n_leader {
                let sfx_len = prom.sfx.len();
                let promise_meta = &(prom.n_accepted, sfx_len, from);
                if promise_meta > &self.max_promise_meta {
                    if sfx_len > 0 || (sfx_len == 0 && prom.ld >= self.acc_sync_ld){
                        self.max_promise_meta = promise_meta.clone();
                        self.max_promise_sfx = prom.sfx;
                    }
                }
                let idx = from as usize - 1;
                self.promises_meta[idx] = Some((prom.n_accepted, sfx_len));
                self.lds[idx] = Some(prom.ld);
                let num_promised =  self.promises_meta.iter().filter(|x| x.is_some()).count();
                if num_promised >= self.majority {
                    let (max_promise_n, max_sfx_len, max_pid) = self.max_promise_meta;
                    let last_is_stop = match self.max_promise_sfx.last() {
                        Some(e) => e.is_stopsign(),
                        None => false
                    };
                    let max_sfx_is_empty = self.max_promise_sfx.is_empty();
                    if max_pid != self.pid {    // sync self with max pid's sequence
                        let my_promise = &self.promises_meta[self.pid as usize - 1].unwrap();
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
                    let max_ld = self.lds[max_pid as usize - 1].unwrap_or(self.acc_sync_ld);    // unwrap_or: if we are max_pid then unwrap will be none
                    let max_promise_acc_sync = AcceptSync::with(self.n_leader, new_entries.clone(), max_ld, false);
                    // append new proposals in my sequence
                    self.storage.append_sequence(&mut new_entries);
                    self.las[self.pid as usize - 1] = self.storage.get_sequence_len();
                    self.state = (Role::Leader, Phase::Accept);
                    // send accept_sync to followers
                    let my_idx = self.pid as usize - 1;
                    let promised = self.lds.iter().enumerate().filter(|(idx, ld)| idx != &my_idx && ld.is_some());
                    for (idx, l) in promised {
                        let pid = idx as u64 + 1;
                        let ld = l.unwrap();
                        let promise_meta = &self.promises_meta[idx].expect(&format!("No promise from {}. Max pid: {}", pid, max_pid));
                        if cfg!(feature = "max_accsync") {
                            if promise_meta == &(max_promise_n, max_sfx_len) {
                                if !max_sfx_is_empty || (max_sfx_is_empty && ld >= self.acc_sync_ld) {
                                    let msg = Message::with(self.pid, pid, PaxosMsg::AcceptSync(max_promise_acc_sync.clone()));
                                    self.outgoing.push(msg);
                                }
                            } else {
                                let sfx = self.storage.get_suffix(ld);
                                let acc_sync = AcceptSync::with(self.n_leader, sfx, ld, true);
                                let msg = Message::with(self.pid, pid, PaxosMsg::AcceptSync(acc_sync));
                                self.outgoing.push(msg);
                            }
                        } else {
                            let sfx = self.storage.get_suffix(ld);
                            let acc_sync = AcceptSync::with(self.n_leader, sfx, ld, true);
                            let msg = Message::with(self.pid, pid, PaxosMsg::AcceptSync(acc_sync));
                            self.outgoing.push(msg);
                        }
                        if cfg!(feature = "batch_accept") {
                            self.batch_accept_meta[idx]= Some((self.n_leader, self.outgoing.len() - 1));
                        }
                    }
                }
            }
        }

        fn handle_promise_accept(&mut self, prom: Promise, from: u64) {
            if prom.n == self.n_leader {
                let idx = from as usize - 1;
                self.lds[idx] = Some(prom.ld);
                let sfx_len = prom.sfx.len();
                let promise_meta = &(prom.n_accepted, sfx_len);
                let (max_ballot, max_sfx_len, _) = self.max_promise_meta;
                let (sync, sfx_start) = if promise_meta == &(max_ballot, max_sfx_len) && cfg!(feature = "max_accsync") {
                    match max_sfx_len == 0 {
                        false => (false, self.acc_sync_ld + sfx_len as u64),
                        true if prom.ld >= self.acc_sync_ld => (false, self.acc_sync_ld + sfx_len as u64),
                        _ => (true, prom.ld),
                    }
                } else {
                    (true, prom.ld)
                };
                let sfx = self.storage.get_suffix(sfx_start);
                // println!("Handle promise from {} in Accept phase: {:?}, sfx len: {}", from, (sync, sfx_start), sfx.len());
                let acc_sync = AcceptSync::with(self.n_leader, sfx, prom.ld, sync);
                self.outgoing.push(Message::with(self.pid, from, PaxosMsg::AcceptSync(acc_sync)));
                // inform what got decided already
                let ld = if self.lc > 0 {
                    self.lc
                } else {
                    self.storage.get_decided_len()
                };
                if ld > prom.ld {
                    let d = Decide::with(ld, self.n_leader);
                    self.outgoing.push(Message::with(self.pid, from, PaxosMsg::Decide(d)));
                }
            }
        }

        fn handle_accepted(&mut self, accepted: Accepted, from: u64) {
            if accepted.n == self.n_leader && self.state == (Role::Leader, Phase::Accept) {
                self.las[from as usize - 1] = accepted.la;
                if accepted.la > self.lc {
                    let mut sorted_las = self.las.clone();
                    sorted_las.sort();
                    let chosen_idx = sorted_las[self.majority - 1];
                    if chosen_idx > self.lc {
                        self.lc = chosen_idx;
                        let d = Decide::with(self.lc, self.n_leader);
                        if cfg!(feature = "latest_decide") {
                            let promised_idx = self.lds.iter().enumerate().filter(|(_, ld)| ld.is_some());
                            for (idx, _) in promised_idx {
                                #[cfg(feature = "batch_accept")] {
                                    match self.batch_accept_meta.get_mut(idx) {
                                        Some(Some((ballot, outgoing_idx))) if ballot == &self.n_leader => {
                                            let Message{msg, ..} = self.outgoing.get_mut(*outgoing_idx).unwrap();
                                            match msg {
                                                PaxosMsg::AcceptDecide(a) => {
                                                    a.ld = self.lc;
                                                },
                                                PaxosMsg::AcceptSync(acc) => {
                                                    acc.ld = self.lc;
                                                },
                                                _ => panic!("Not AcceptDecide or AcceptSync when sending latest decide"),
                                            }
                                            continue;
                                        },
                                        _ => {}
                                    }
                                }
                                match self.latest_decide_meta.get_mut(idx) {
                                    Some(Some((ballot, outgoing_dec_idx))) if ballot == &self.n_leader => {
                                        let Message{msg, ..} = self.outgoing.get_mut(*outgoing_dec_idx).unwrap();
                                        match msg {
                                            PaxosMsg::Decide(d) => {
                                                d.ld = self.lc;
                                            },
                                            _ => panic!("Cached message in outgoing was not Decide"),
                                        }
                                    },
                                    _ => {
                                        let cache_dec_idx = self.outgoing.len();
                                        self.latest_decide_meta[idx] = Some((self.n_leader, cache_dec_idx));
                                        let pid = idx as u64 + 1;
                                        self.outgoing.push(Message::with(self.pid, pid, PaxosMsg::Decide(d.clone())));
                                    }
                                }
                            }
                        } else {
                            let promised_pids = self.lds.iter().enumerate().filter(|(_, ld)| ld.is_some()).map( |(idx, _)| idx as u64 + 1);
                            for pid in promised_pids {
                                self.outgoing.push(Message::with(self.pid, pid, PaxosMsg::Decide(d.clone())));
                            }
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
                let msg = Message::with(self.pid, from, PaxosMsg::Promise(p));
                self.communication_port.trigger(CommunicatorMsg::RawPaxosMsg(msg));
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
                    if cfg!(feature = "latest_accepted") {
                        let cached_idx = self.outgoing.len();
                        self.latest_accepted_meta = Some((acc_sync.n, cached_idx));
                    }
                    self.outgoing.push(Message::with(self.pid, from, PaxosMsg::Accepted(accepted)));
                }
            }
        }

        fn handle_acceptdecide(&mut self, acc: AcceptDecide, from: u64) {
            if self.state == (Role::Follower, Phase::Accept) {
                if self.storage.get_promise() == acc.n {
                    let mut entries = acc.entries;
                    self.storage.append_sequence(&mut entries);
                    if cfg!(feature = "latest_accepted") {
                        match self.latest_accepted_meta {
                            Some((ballot, outgoing_idx)) if ballot == acc.n => {
                                let Message{msg, ..} = self.outgoing.get_mut(outgoing_idx).unwrap();
                                match msg {
                                    PaxosMsg::Accepted(a) => {
                                        a.la = self.storage.get_sequence_len();
                                    },
                                    _ => panic!("Cached idx is not an Accepted message!")
                                }
                            },
                            _ => {
                                let accepted = Accepted::with(acc.n, self.storage.get_sequence_len());
                                let cached_idx = self.outgoing.len();
                                self.latest_accepted_meta = Some((acc.n, cached_idx));
                                self.outgoing.push(Message::with(self.pid, from, PaxosMsg::Accepted(accepted)));
                            }
                        }
                    } else {
                        let accepted = Accepted::with(acc.n, self.storage.get_sequence_len());
                        self.outgoing.push(Message::with(self.pid, from, PaxosMsg::Accepted(accepted)));
                    }
                    // handle decide
                    if acc.ld > self.storage.get_decided_len() {
                        self.storage.set_decided_len(acc.ld);
                    }
                }
            }
        }

        fn handle_decide(&mut self, dec: Decide) {
            if self.storage.get_promise() == dec.n {
                if self.prev_ld >= dec.ld {
                    return;
                }
                let entry = self.storage.decide(dec.ld);
                self.prev_ld = dec.ld;
                match entry {
                    Entry::Normal(data) => {
                        if self.state.0 == Role::Leader {
                            let pr = ProposalResp::with(data, self.pid);
                            self.communication_port.trigger(CommunicatorMsg::ProposalResponse(pr));
                        }
                    }
                    Entry::StopSign(ss) => {
                        let final_seq = self.stop_and_get_sequence();
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

    impl<S, P> Actor for Paxos<S, P> where
        S: SequenceTraits,
        P: PaxosStateTraits
    {
        type Message = PaxosCompMsg;

        fn receive_local(&mut self, msg: PaxosCompMsg) -> () {
            match msg {
                PaxosCompMsg::Propose(p) => {
                    match p.reconfig {
                        Some(r) => self.propose_reconfiguration(r.0),
                        None => {
                            self.propose_normal(p.data);
                        }
                    }
                },
                /*PaxosCompMsg::SequenceReq(seq_req) => {
                    let ser_entries = self.get_chosen_ser_entries(seq_req.from_idx, seq_req.to_idx);
                    self.supervisor.tell(PaxosReplicaMsg::SequenceResp(seq_req, ser_entries));
                },*/
                PaxosCompMsg::GetAllEntries(a) => { // for testing only
                    let seq = self.get_sequence();
                    a.reply(seq).expect("Failed to reply to GetAllEntries");
                },
            }
        }

        fn receive_network(&mut self, _: NetMessage) -> () {
            // ignore
        }
    }

    impl<S, P> Provide<ControlPort> for Paxos<S, P> where
        S: SequenceTraits,
        P: PaxosStateTraits
    {
        fn handle(&mut self, event: <ControlPort as Port>::Request) -> () {
            if let ControlEvent::Kill = event {
                self.supervisor.tell(PaxosReplicaMsg::KillResp);
            }
        }
    }

    impl<S, P> Require<CommunicationPort> for Paxos<S, P> where
        S: SequenceTraits,
        P: PaxosStateTraits
    {
        fn handle(&mut self, msg: <CommunicationPort as Port>::Indication) -> () {
            if let AtomicBroadcastCompMsg::RawPaxosMsg(pm) = msg {
            	trace!(self.ctx.log(), "handling {:?}", pm);
                self.handle(pm)
            }
        }
    }

    impl<S, P> Require<BallotLeaderElection> for Paxos<S, P> where
        S: SequenceTraits,
        P: PaxosStateTraits
    {
        fn handle(&mut self, l: Leader) -> () {
            debug!(self.ctx.log(), "{}", format!("Node {} became leader in config {}. Ballot: {:?}",  l.pid, self.config_id, l.ballot));
            if self.leader != l.pid && !self.stopped() {
                self.supervisor.tell(PaxosReplicaMsg::Leader(self.config_id, l.pid));
            }
            self.leader = l.pid;
            self.handle_leader(l);
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
    use super::super::messages::{paxos::ballot_leader_election::*, StopMsg as NetStopMsg, StopMsgDeser};
    use std::time::Duration;

    #[derive(Clone, Debug)]
    pub struct Stop(pub u64);

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
        supervisor: Recipient<StopKillResponse>,
        stopped: bool,
        stopped_peers: HashSet<u64>,
        has_had_leader: bool,
        quick_start: bool
    }

    impl BallotLeaderComp {
        pub fn with(peers: Vec<ActorPath>, pid: u64, hb_delay: u64, delta: u64, supervisor: Recipient<StopKillResponse>, prio_start: bool, quick_start: bool) -> BallotLeaderComp {
            let n = &peers.len() + 1;
            let initial_round = if prio_start { PRIO_START_ROUND } else { 0 };
            let initial_ballot = Ballot::with(initial_round, pid);
            BallotLeaderComp {
                ctx: ComponentContext::new(),
                ble_port: ProvidedPort::new(),
                pid,
                majority: n/2 + 1, // +1 because peers is exclusive ourselves
                peers,
                round: initial_round,
                ballots: Vec::with_capacity(n),
                current_ballot: initial_ballot.clone(),
                leader: None,
                max_ballot: initial_ballot,
                hb_delay,
                delta,
                timer: None,
                supervisor,
                stopped: false,
                stopped_peers: HashSet::with_capacity(n),
                has_had_leader: !prio_start,
                quick_start
            }
        }

        fn check_leader(&mut self) {
            let mut ballots = Vec::with_capacity(self.peers.len());
            std::mem::swap(&mut self.ballots, &mut ballots);
            let (top_ballot, top_pid) = ballots.into_iter().max().unwrap();
            if top_ballot < self.max_ballot {   // did not get HB from leader
                self.current_ballot.n = self.max_ballot.n + 1;
                self.leader = None;
            } else {
                if self.leader != Some((top_ballot, top_pid)) { // got a new leader with greater ballot
                    self.has_had_leader = true;
                    self.max_ballot = top_ballot;
                    self.leader = Some((top_ballot, top_pid));
                    self.ble_port.trigger(Leader::with(top_pid, top_ballot));
                }
            }
        }

        fn hb_timeout(&mut self) {
            if self.ballots.len() + 1 >= self.majority {
                self.ballots.push((self.current_ballot, self.pid));
                self.check_leader();
            } else {
                self.ballots.clear();
            }
            let delay = if !self.has_had_leader{    // use short timeout if still no first leader
                ELECTION_TIMEOUT/INITIAL_ELECTION_FACTOR
            } else {
                self.hb_delay
            };
            self.round += 1;
            for peer in &self.peers {
                let hb_request = HeartbeatRequest::with(self.round, self.max_ballot);
                peer.tell_serialised(HeartbeatMsg::Request(hb_request),self).expect("HBRequest should serialise!");
            }
            self.start_timer(delay);
        }

        fn start_timer(&mut self, t: u64) {
            let timer = self.schedule_once(
                Duration::from_millis(t),
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
                    let delay = if self.quick_start{    // use short timeout if still no first leader
                        ELECTION_TIMEOUT/INITIAL_ELECTION_FACTOR
                    } else {
                        self.hb_delay
                    };
                    self.start_timer(delay);
                },
                ControlEvent::Kill => {
                    self.stop_timer();
                    self.supervisor.tell(StopKillResponse::KillResp);
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
        type Message = Stop;

        fn receive_local(&mut self, stop: Stop) -> () {
            self.stop_timer();
            for peer in &self.peers {
                peer.tell_serialised(NetStopMsg::Peer(stop.0), self).expect("NetStopMsg should serialise!");
            }
            self.stopped = true;
            if self.stopped_peers.len() == self.peers.len() {
                // info!(self.ctx.log(), "Got stopped from all peers");
                self.supervisor.tell(StopKillResponse::StopResp);
            }
        }

        fn receive_network(&mut self, m: NetMessage) -> () {
            let NetMessage{sender, data, ..} = m;
            match_deser!{data; {
                hb: HeartbeatMsg [BallotLeaderSer] => {
                    match hb {
                        HeartbeatMsg::Request(req) if !self.stopped => {
                            if req.max_ballot > self.max_ballot {
                                self.max_ballot = req.max_ballot;
                            }
                            let hb_reply = HeartbeatReply::with(self.pid, req.round, self.current_ballot);
                            sender.tell_serialised(HeartbeatMsg::Reply(hb_reply), self).expect("HBReply should serialise!");
                        },
                        HeartbeatMsg::Reply(rep) if !self.stopped => {
                            if rep.round == self.round {
                                self.ballots.push((rep.max_ballot, rep.sender_pid));
                            } else {
                                debug!(self.ctx.log(), "Got late hb reply. HB delay: {}", self.hb_delay);
                                self.hb_delay += self.delta;
                            }
                        },
                        _ => {},
                    }
                },
                stop: NetStopMsg [StopMsgDeser] => {
                    if let NetStopMsg::Peer(pid) = stop {
                        assert!(self.stopped_peers.insert(pid), "Got duplicate stop from peer {}", pid);
                        // info!(self.ctx.log(), "Got stopped from BLE peer {}", pid);
                        if self.stopped && self.stopped_peers.len() == self.peers.len() {
                            // info!(self.ctx.log(), "Got stopped from all peers");
                            self.supervisor.tell(StopKillResponse::StopResp);
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
