use super::atomic_broadcast::ExperimentConfig;
use super::communicator::{
    AtomicBroadcastCompMsg, CommunicationPort, Communicator, CommunicatorMsg,
};
use super::messages::paxos::ballot_leader_election::{Ballot, Leader};
use super::messages::paxos::{
    Reconfig, ReconfigInit, ReconfigSer, ReconfigurationMsg, SequenceMetaData, SequenceRequest,
    SequenceSegment, SequenceTransfer,
};
use super::messages::{StopMsg as NetStopMsg, *};
use super::storage::paxos::*;
use crate::bench::atomic_broadcast::atomic_broadcast::Done;
use crate::bench::atomic_broadcast::paxos::raw_paxos::StopSign;
use crate::partitioning_actor::{Init, PartitioningActorMsg, PartitioningActorSer};
use crate::serialiser_ids::ATOMICBCAST_ID;
use ballot_leader_election::{BallotLeaderComp, BallotLeaderElection, Stop as BLEStop};
use hashbrown::{HashMap, HashSet};
use kompact::prelude::*;
use kompact::KompactLogger;
use rand::Rng;
use raw_paxos::{Entry, Paxos};
use std::{borrow::Borrow, fmt::Debug, ops::DerefMut, sync::Arc, time::Duration};

const BLE: &str = "ble";
const COMMUNICATOR: &str = "communicator";

pub trait SequenceTraits: Sequence + Debug + Send + Sync + 'static {}
pub trait PaxosStateTraits: PaxosState + Send + 'static {}

#[derive(Debug)]
pub struct FinalMsg<S>
where
    S: SequenceTraits,
{
    pub config_id: u32,
    pub nodes: Reconfig,
    pub final_sequence: Arc<S>,
    pub skip_prepare_n: Option<Ballot>,
}

impl<S> FinalMsg<S>
where
    S: SequenceTraits,
{
    pub fn with(
        config_id: u32,
        nodes: Reconfig,
        final_sequence: Arc<S>,
        skip_prepare_n: Option<Ballot>,
    ) -> FinalMsg<S> {
        FinalMsg {
            config_id,
            nodes,
            final_sequence,
            skip_prepare_n,
        }
    }
}

#[derive(Debug)]
pub enum PaxosReplicaMsg {
    Propose(Proposal),
    LocalSequenceReq(ActorPath, SequenceRequest, SequenceMetaData),
    GetAllEntries(Ask<(), Vec<Entry>>),
    Stop(Ask<(bool, bool), ()>), // (ack_client, late)
}

#[derive(Clone, Debug)]
pub enum ReconfigurationPolicy {
    Eager,
    Pull,
}

#[derive(Clone, Debug)]
struct ConfigMeta {
    id: u32,
    leader: u64,
    pending_reconfig: bool,
}

impl ConfigMeta {
    fn new(id: u32) -> Self {
        ConfigMeta {
            id,
            leader: 0,
            pending_reconfig: false,
        }
    }
}

#[derive(ComponentDefinition)]
pub struct PaxosComp<S, P>
where
    S: SequenceTraits,
    P: PaxosStateTraits,
{
    ctx: ComponentContext<Self>,
    pid: u64,
    initial_config: Vec<u64>,
    paxos_replicas: Vec<Arc<Component<PaxosReplica<S, P>>>>,
    ble_comps: Vec<Arc<Component<BallotLeaderComp>>>,
    communicator_comps: Vec<Arc<Component<Communicator>>>,
    active_config: ConfigMeta,
    nodes: Vec<ActorPath>, // derive actorpaths of peers' ble and paxos replicas from these
    prev_sequences: HashMap<u32, Arc<S>>, // TODO vec
    stopped: bool,
    iteration_id: u32,
    partitioning_actor: Option<ActorPath>,
    policy: ReconfigurationPolicy,
    next_config_id: Option<u32>,
    pending_seq_transfers: Vec<(Vec<u32>, Vec<Entry>)>, // (remaining_segments, entries)
    complete_sequences: Vec<u32>,
    active_peers: (Vec<u64>, Vec<u64>), // (ready, not_ready)
    retry_transfer_timers: HashMap<u32, ScheduledTimer>,
    cached_client: Option<ActorPath>,
    hb_proposals: Vec<NetMessage>,
    experiment_config: ExperimentConfig,
}

impl<S, P> PaxosComp<S, P>
where
    S: SequenceTraits,
    P: PaxosStateTraits,
{
    pub fn with(
        initial_config: Vec<u64>,
        policy: ReconfigurationPolicy,
        experiment_config: ExperimentConfig,
    ) -> PaxosComp<S, P> {
        PaxosComp {
            ctx: ComponentContext::uninitialised(),
            pid: 0,
            initial_config,
            paxos_replicas: vec![],
            ble_comps: vec![],
            communicator_comps: vec![],
            active_config: ConfigMeta::new(0),
            nodes: vec![],
            prev_sequences: HashMap::new(),
            stopped: false,
            iteration_id: 0,
            partitioning_actor: None,
            policy,
            next_config_id: None,
            pending_seq_transfers: vec![],
            complete_sequences: vec![],
            active_peers: (vec![], vec![]),
            retry_transfer_timers: HashMap::new(),
            cached_client: None,
            hb_proposals: vec![],
            experiment_config,
        }
    }

    fn derive_actorpaths(
        &self,
        config_id: u32,
        peers: &Vec<u64>,
    ) -> (Vec<ActorPath>, HashMap<u64, ActorPath>) {
        let num_peers = peers.len();
        let mut communicator_peers = HashMap::with_capacity(num_peers);
        let mut ble_peers = Vec::with_capacity(num_peers);
        for pid in peers {
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
                        vec![format!(
                            "{}{},{}-{}",
                            COMMUNICATOR, pid, config_id, self.iteration_id
                        )],
                    );
                    let named_ble = NamedPath::new(
                        protocol,
                        *addr,
                        port,
                        vec![format!(
                            "{}{},{}-{}",
                            BLE, pid, config_id, self.iteration_id
                        )],
                    );
                    communicator_peers.insert(*pid, ActorPath::Named(named_communicator));
                    ble_peers.push(ActorPath::Named(named_ble));
                }
                _ => error!(
                    self.ctx.log(),
                    "{}",
                    format!("Actorpath is not named for node {}", pid)
                ),
            }
        }
        (ble_peers, communicator_peers)
    }

    fn create_replica(
        &mut self,
        config_id: u32,
        nodes: Vec<u64>,
        send_ack: bool,
        start: bool,
        ble_quick_start: bool,
        skip_prepare_n: Option<Ballot>,
    ) -> Handled {
        let mut peers = nodes;
        peers.retain(|pid| pid != &self.pid);
        let (ble_peers, communicator_peers) = self.derive_actorpaths(config_id, &peers);
        let system = self.ctx.system();
        /*** create and register Paxos ***/
        let log: KompactLogger = self.ctx.log().new(o!("raw_paxos" => self.pid));
        let max_inflight = self.experiment_config.max_inflight;
        let (paxos, paxos_f) = system.create_and_register(|| {
            PaxosReplica::with(
                self.ctx.actor_ref(),
                peers,
                config_id,
                self.pid,
                log,
                skip_prepare_n,
                max_inflight,
            )
        });
        /*** create and register Communicator ***/
        let (communicator, comm_f) = system.create_and_register(|| {
            Communicator::with(
                communicator_peers,
                self.cached_client
                    .as_ref()
                    .expect("No cached client!")
                    .clone(),
            )
        });
        /*** create and register BLE ***/
        let election_timeout = self.experiment_config.election_timeout;
        let config = self.ctx.config();
        let ble_delta = config["paxos"]["ble_delta"]
            .as_i64()
            .expect("Failed to load get_decided_period");
        let initial_election_factor = self.experiment_config.initial_election_factor;
        let (ble_comp, ble_f) = system.create_and_register(|| {
            BallotLeaderComp::with(
                ble_peers,
                self.pid,
                election_timeout as u64,
                ble_delta as u64,
                ble_quick_start,
                skip_prepare_n.clone(),
                initial_election_factor,
            )
        });
        let communicator_alias = format!(
            "{}{},{}-{}",
            COMMUNICATOR, self.pid, config_id, self.iteration_id
        );
        let ble_alias = format!("{}{},{}-{}", BLE, self.pid, config_id, self.iteration_id);
        let comm_alias_f = system.register_by_alias(&communicator, communicator_alias);
        let ble_alias_f = system.register_by_alias(&ble_comp, ble_alias);
        /*** connect components ***/
        biconnect_components::<CommunicationPort, _, _>(&communicator, &paxos)
            .expect("Could not connect Communicator and PaxosComp!");

        biconnect_components::<BallotLeaderElection, _, _>(&ble_comp, &paxos)
            .expect("Could not connect BLE and PaxosComp!");

        Handled::block_on(self, move |mut async_self| async move {
            paxos_f.await.unwrap().expect("Failed to register paxos");
            ble_f.await.unwrap().expect("Failed to register ble");
            comm_f
                .await
                .unwrap()
                .expect("Failed to register communicator");
            comm_alias_f
                .await
                .unwrap()
                .expect("Failed to register comm_alias");
            ble_alias_f
                .await
                .unwrap()
                .expect("Failed to register ble_alias");

            if start {
                info!(
                    async_self.ctx.log(),
                    "Starting replica pid: {}, config_id: {}", async_self.pid, config_id
                );
                async_self.active_config = ConfigMeta::new(config_id);
                async_self.next_config_id = None;
                system.start(&paxos);
                system.start(&ble_comp);
                system.start(&communicator);

                match skip_prepare_n {
                    Some(n) => async_self
                        .ctx
                        .actor_ref()
                        .tell(PaxosCompMsg::Leader(config_id, n.pid)),
                    _ => {}
                }
            } else {
                async_self.next_config_id = Some(config_id);
            }

            async_self.paxos_replicas.push(paxos);
            async_self.ble_comps.push(ble_comp);
            async_self.communicator_comps.push(communicator);

            if send_ack {
                let resp = PartitioningActorMsg::InitAck(async_self.iteration_id);
                let ap = async_self
                    .partitioning_actor
                    .take()
                    .expect("PartitioningActor not found!");
                ap.tell_serialised(resp, async_self.deref_mut())
                    .expect("Should serialise");
            }
        })
    }

    fn start_replica(&mut self) {
        if let Some(config_id) = self.next_config_id {
            info!(
                self.ctx.log(),
                "Starting replica pid: {}, config_id: {}", self.pid, config_id
            );
            self.active_config = ConfigMeta::new(config_id);
            let paxos = self.paxos_replicas.last().unwrap_or_else(|| {
                panic!("Could not find PaxosComp with config_id: {}", config_id)
            });
            let ble = self
                .ble_comps
                .last()
                .unwrap_or_else(|| panic!("Could not find BLE config_id: {}", config_id));
            let communicator = self.communicator_comps.last().unwrap_or_else(|| {
                panic!("Could not find Communicator with config_id: {}", config_id)
            });
            self.ctx.system().start(paxos);
            self.ctx.system().start(ble);
            self.ctx.system().start(communicator);
            self.next_config_id = None;
        }
    }

    fn stop_components(&mut self) -> Handled {
        self.stopped = true;
        let retry_timers = std::mem::take(&mut self.retry_transfer_timers);
        for (_, timer) in retry_timers {
            self.cancel_timer(timer);
        }
        self.active_config.id = 0;
        self.active_config.leader = 0;
        self.prev_sequences.clear();
        self.active_peers.0.clear();
        self.active_peers.1.clear();
        self.complete_sequences.clear();
        let num_comps =
            self.ble_comps.len() + self.paxos_replicas.len() + self.communicator_comps.len();
        assert!(
            num_comps > 0,
            "Should not get client stop if no child components"
        );
        debug!(self.ctx.log(), "Killing {} child components...", num_comps);
        let mut stop_futures = Vec::with_capacity(num_comps);
        let late_stop = self.next_config_id.is_some();
        let (ble_last, rest_ble) = self.ble_comps.split_last().expect("No ble comps!");
        for ble in rest_ble {
            stop_futures.push(
                ble.actor_ref()
                    .ask(|p| BLEStop(Ask::new(p, (self.pid, false)))),
            );
        }
        stop_futures.push(
            ble_last
                .actor_ref()
                .ask(|p| BLEStop(Ask::new(p, (self.pid, late_stop)))),
        );

        let (paxos_last, rest) = self
            .paxos_replicas
            .split_last()
            .expect("No paxos replicas!");
        for paxos_replica in rest {
            stop_futures.push(
                paxos_replica
                    .actor_ref()
                    .ask(|p| PaxosReplicaMsg::Stop(Ask::new(p, (false, false)))),
            );
        }
        stop_futures.push(
            paxos_last
                .actor_ref()
                .ask(|p| PaxosReplicaMsg::Stop(Ask::new(p, (true, late_stop)))),
        );

        if late_stop {
            self.start_replica();
        }

        Handled::block_on(self, move |_| async move {
            for stop_f in stop_futures {
                stop_f.await.expect("Failed to stop child components!");
            }
        })
    }

    fn kill_components(&mut self, ask: Ask<(), Done>) -> Handled {
        let system = self.ctx.system();
        let mut kill_futures = vec![];
        for ble in self.ble_comps.drain(..) {
            let ble_f = system.kill_notify(ble);
            kill_futures.push(ble_f);
        }
        for paxos in self.paxos_replicas.drain(..) {
            let paxos_f = system.kill_notify(paxos);
            kill_futures.push(paxos_f);
        }
        for communicator in self.communicator_comps.drain(..) {
            let comm_f = system.kill_notify(communicator);
            kill_futures.push(comm_f);
        }
        Handled::block_on(self, move |_| async move {
            for f in kill_futures {
                f.await.expect("Failed to kill child components");
            }
            ask.reply(Done).unwrap();
        })
    }

    fn new_iteration(&mut self, init: Init) -> Handled {
        self.stopped = false;
        self.nodes = init.nodes;
        self.pid = init.pid as u64;
        self.iteration_id = init.init_id;
        self.active_config = ConfigMeta::new(0);
        let ser_client = init
            .init_data
            .expect("Init should include ClientComp's actorpath");
        let client = ActorPath::deserialise(&mut ser_client.as_slice())
            .expect("Failed to deserialise Client's actorpath");
        self.cached_client = Some(client);
        if self.initial_config.contains(&self.pid) {
            self.next_config_id = Some(1);
            self.create_replica(1, self.initial_config.clone(), true, false, true, None)
        } else {
            let resp = PartitioningActorMsg::InitAck(self.iteration_id);
            let ap = self
                .partitioning_actor
                .take()
                .expect("PartitioningActor not found!");
            ap.tell_serialised(resp, self).expect("Should serialise");
            Handled::Ok
        }
    }

    fn deserialise_and_propose(&self, m: NetMessage) {
        if let AtomicBroadcastMsg::Proposal(p) = m
            .try_deserialise_unchecked::<AtomicBroadcastMsg, AtomicBroadcastDeser>()
            .expect("Should be AtomicBroadcastMsg!")
        {
            let active_paxos = &self
                .paxos_replicas
                .last()
                .expect("Could not get PaxosComp actor ref despite being leader");
            active_paxos.actor_ref().tell(PaxosReplicaMsg::Propose(p));
        }
    }

    fn pull_sequence(&mut self, config_id: u32, seq_len: u64, skip_tag: Option<usize>) {
        let num_ready_peers = self.active_peers.0.len();
        let num_unready_peers = self.active_peers.1.len();
        let num_continued_nodes = num_ready_peers + num_unready_peers;
        let idx = config_id as usize - 1;
        let rem_segments: Vec<_> = (1..=num_continued_nodes).map(|x| x as u32).collect();
        self.pending_seq_transfers[idx] =
            (rem_segments, vec![Entry::Normal(vec![]); seq_len as usize]);
        let offset = seq_len / num_continued_nodes as u64;
        // get segment from unready nodes (probably have early segments of final sequence)
        let skip = skip_tag.unwrap_or(0);
        for (i, pid) in self.active_peers.1.iter().enumerate() {
            let from_idx = i as u64 * offset;
            let to_idx = if from_idx as u64 + offset > seq_len {
                seq_len
            } else {
                from_idx + offset
            };
            let tag = i + 1;
            if tag != skip {
                debug!(
                    self.ctx.log(),
                    "Requesting segment from {}, config_id: {}, tag: {}, idx: {}-{}",
                    pid,
                    config_id,
                    tag,
                    from_idx,
                    to_idx - 1
                );
                self.request_sequence(*pid, config_id, from_idx, to_idx, tag as u32);
            }
        }
        // get segment from ready nodes (definitely has final sequence)
        for (i, pid) in self.active_peers.0.iter().enumerate() {
            let from_idx = (num_unready_peers + i) as u64 * offset;
            let to_idx = if from_idx as u64 + offset > seq_len {
                seq_len
            } else {
                from_idx + offset
            };
            let tag = num_unready_peers + i + 1;
            if tag != skip {
                debug!(
                    self.ctx.log(),
                    "Requesting segment from {}, config_id: {}, tag: {}, idx: {}-{}",
                    pid,
                    config_id,
                    tag,
                    from_idx,
                    to_idx - 1
                );
                self.request_sequence(*pid, config_id, from_idx, to_idx, tag as u32);
            }
        }
        let transfer_timeout = self.ctx.config()["paxos"]["transfer_timeout"]
            .as_duration()
            .expect("Failed to load get_decided_period");
        let timer = self.schedule_once(transfer_timeout, move |c, _| {
            c.retry_request_sequence(config_id, seq_len, num_continued_nodes as u64)
        });
        self.retry_transfer_timers.insert(config_id, timer);
    }

    fn request_sequence(&self, pid: u64, config_id: u32, from_idx: u64, to_idx: u64, tag: u32) {
        let sr = SequenceRequest::with(config_id, tag, from_idx, to_idx, self.pid);
        let idx = pid as usize - 1;
        self.nodes
            .get(idx)
            .unwrap_or_else(|| panic!("Failed to get Actorpath of node {}", pid))
            .tell_serialised(ReconfigurationMsg::SequenceRequest(sr), self)
            .expect("Should serialise!");
    }

    fn retry_request_sequence(
        &mut self,
        config_id: u32,
        seq_len: u64,
        total_segments: u64,
    ) -> Handled {
        if let Some((rem_segments, _)) = self.pending_seq_transfers.get(config_id as usize) {
            let offset = seq_len / total_segments;
            let num_active = self.active_peers.0.len();
            if num_active > 0 {
                for tag in rem_segments {
                    let i = tag - 1;
                    let from_idx = i as u64 * offset;
                    let to_idx = from_idx + offset;
                    // info!(self.ctx.log(), "Retrying timed out seq transfer: tag: {}, idx: {}-{}, policy: {:?}", tag, from_idx, to_idx, self.policy);
                    let pid = self
                        .active_peers
                        .0
                        .get(i as usize % num_active)
                        .unwrap_or_else(|| {
                            panic!(
                                "Failed to get active pid. idx: {}, len: {}",
                                i,
                                self.active_peers.0.len()
                            )
                        });
                    self.request_sequence(*pid, config_id, from_idx, to_idx, *tag);
                }
            }
            let transfer_timeout = self.ctx.config()["paxos"]["transfer_timeout"]
                .as_duration()
                .expect("Failed to load get_decided_period");
            let timer = self.schedule_once(transfer_timeout, move |c, _| {
                c.retry_request_sequence(config_id, seq_len, total_segments)
            });
            self.retry_transfer_timers.insert(config_id, timer);
        }
        Handled::Ok
    }

    fn get_sequence_metadata(&self, config_id: u32) -> SequenceMetaData {
        let seq_len = match self.prev_sequences.get(&config_id) {
            Some(prev_seq) => prev_seq.get_sequence_len(),
            None => 0,
        };
        SequenceMetaData::with(config_id, seq_len)
    }

    fn append_transferred_segment(&mut self, st: SequenceTransfer) {
        let (rem_transfers, sequence) = self
            .pending_seq_transfers
            .get_mut(st.config_id as usize - 1)
            .expect("Should have initialised pending sequence");
        let tag = st.tag;
        let segment = st.segment;
        let offset = segment.from_idx as usize;
        for (i, entry) in segment.entries.into_iter().enumerate() {
            sequence[offset + i] = entry;
        }
        // PaxosSer::deserialise_entries_into(&mut st.entries.as_slice(), sequence, offset);
        rem_transfers.retain(|t| t != &tag);
    }

    fn get_continued_idx(&self, continued_nodes: &[u64]) -> usize {
        continued_nodes
            .iter()
            .position(|pid| pid == &self.pid)
            .expect("Could not find my pid in continued_nodes")
    }

    fn create_segment(&self, continued_nodes: &[u64], config_id: u32) -> SequenceSegment {
        let index = self.get_continued_idx(continued_nodes);
        let n_continued = continued_nodes.len();
        let final_seq = self
            .prev_sequences
            .get(&config_id)
            .expect("Should have final sequence");
        let seq_len = final_seq.get_sequence_len();
        let offset = seq_len / n_continued as u64;
        let from_idx = index as u64 * offset;
        let to_idx = from_idx + offset;
        let entries = final_seq.get_entries(from_idx, to_idx).to_vec();
        SequenceSegment::with(from_idx, to_idx, entries)
    }

    fn create_eager_sequence_transfer(
        &self,
        continued_nodes: &[u64],
        config_id: u32,
    ) -> SequenceTransfer {
        let index = self.get_continued_idx(continued_nodes);
        let tag = index as u32 + 1;
        let segment = self.create_segment(continued_nodes, config_id);
        let prev_seq_metadata = self.get_sequence_metadata(config_id - 1);
        SequenceTransfer::with(config_id, tag, true, prev_seq_metadata, segment)
    }

    fn handle_sequence_request(&mut self, sr: SequenceRequest, requestor: ActorPath) {
        if self.active_config.leader == sr.requestor_pid {
            return;
        }
        let (succeeded, entries) = match self.prev_sequences.get(&sr.config_id) {
            Some(seq) => {
                let ents = seq.get_entries(sr.from_idx, sr.to_idx).to_vec();
                (true, ents)
            }
            None => {
                // (false, vec![])
                if self.active_config.id == sr.config_id {
                    // we have not reached final sequence, but might still have requested elements. Outsource request to corresponding PaxosComp
                    let paxos = self.paxos_replicas.last().unwrap_or_else(|| panic!("No paxos comp with config_id: {} when handling SequenceRequest. Len of PaxosComps: {}", sr.config_id, self.paxos_replicas.len()));
                    let prev_seq_metadata = self.get_sequence_metadata(sr.config_id - 1);
                    paxos.actor_ref().tell(PaxosReplicaMsg::LocalSequenceReq(
                        requestor,
                        sr,
                        prev_seq_metadata,
                    ));
                    return;
                } else {
                    (false, vec![])
                }
            }
        };
        let prev_seq_metadata = self.get_sequence_metadata(sr.config_id - 1);
        let segment = SequenceSegment::with(sr.from_idx, sr.to_idx, entries);
        let st =
            SequenceTransfer::with(sr.config_id, sr.tag, succeeded, prev_seq_metadata, segment);
        // info!(self.ctx.log(), "Replying seq transfer: tag: {}, idx: {}-{}", st.tag, st.from_idx, st.to_idx);
        requestor
            .tell_serialised(ReconfigurationMsg::SequenceTransfer(st), self)
            .expect("Should serialise!");
    }

    fn handle_sequence_transfer(&mut self, st: SequenceTransfer) {
        if self.active_config.id > st.config_id
            || self.complete_sequences.contains(&st.config_id)
            || self.next_config_id.unwrap_or(0) <= st.config_id
        {
            return; // ignore late sequence transfers
        }
        let prev_config_id = st.metadata.config_id;
        let prev_seq_len = st.metadata.len;
        // pull previous sequence if exists and not already started
        if prev_config_id != 0 && !self.complete_sequences.contains(&prev_config_id) {
            let idx = prev_config_id as usize - 1;
            if let Some((rem_segments, _)) = self.pending_seq_transfers.get(idx) {
                if rem_segments.is_empty() {
                    self.pull_sequence(prev_config_id, prev_seq_len, None);
                }
            }
        }
        if st.succeeded {
            let idx = st.config_id as usize - 1;
            let config_id = st.config_id;
            self.append_transferred_segment(st);
            let got_all_segments = self.pending_seq_transfers[idx].0.is_empty();
            if got_all_segments {
                self.complete_sequences.push(config_id);
                let mut c = (vec![], vec![]);
                std::mem::swap(&mut c, &mut self.pending_seq_transfers[idx]);
                self.prev_sequences
                    .insert(config_id, Arc::new(S::new_with_sequence(c.1)));
                if let Some(timer) = self.retry_transfer_timers.remove(&config_id) {
                    self.cancel_timer(timer);
                }
                if self.complete_sequences.len() + 1 == self.next_config_id.unwrap() as usize {
                    // got all sequence transfers
                    self.complete_sequences.clear();
                    debug!(self.ctx.log(), "Got all previous sequences!");
                    self.start_replica();
                }
            }
        } else {
            // failed sequence transfer i.e. not reached final seq yet
            let config_id = st.config_id;
            let tag = st.tag;
            let from_idx = st.segment.from_idx;
            let to_idx = st.segment.to_idx;
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

#[derive(Debug)]
pub enum PaxosCompMsg<S>
where
    S: SequenceTraits,
{
    Leader(u32, u64),
    PendingReconfig(Vec<u8>),
    Reconfig(FinalMsg<S>),
    KillComponents(Ask<(), Done>),
}

impl<S, P> ComponentLifecycle for PaxosComp<S, P>
where
    S: SequenceTraits,
    P: PaxosStateTraits,
{
}

impl<S, P> Actor for PaxosComp<S, P>
where
    S: SequenceTraits,
    P: PaxosStateTraits,
{
    type Message = PaxosCompMsg<S>;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        match msg {
            PaxosCompMsg::Leader(config_id, pid) => {
                if self.active_config.id == config_id {
                    if self.active_config.leader == 0 {
                        let hb_proposals = std::mem::take(&mut self.hb_proposals);
                        if pid == self.pid {
                            // notify client if no leader before
                            self.cached_client
                                .as_ref()
                                .expect("No cached client!")
                                .tell_serialised(AtomicBroadcastMsg::FirstLeader(pid), self)
                                .expect("Should serialise FirstLeader");
                            for net_msg in hb_proposals {
                                self.deserialise_and_propose(net_msg);
                            }
                        } else if !hb_proposals.is_empty() {
                            let idx = pid as usize - 1;
                            let leader = self.nodes.get(idx).unwrap_or_else(|| {
                                panic!(
                                    "Could not get leader's actorpath. Pid: {}",
                                    self.active_config.leader
                                )
                            });
                            for m in hb_proposals {
                                leader.forward_with_original_sender(m, self);
                            }
                        }
                    }
                    self.active_config.leader = pid;
                }
            }
            PaxosCompMsg::PendingReconfig(data) => {
                self.active_config.pending_reconfig = true;
                self.cached_client
                    .as_ref()
                    .expect("No cached client!")
                    .tell_serialised(AtomicBroadcastMsg::PendingReconfiguration(data), self)
                    .expect("Should serialise FirstLeader");
            }
            PaxosCompMsg::Reconfig(r) => {
                /*** ReconfigResponse to client ***/
                let new_config_len = r.nodes.len();
                let mut data: Vec<u8> = Vec::with_capacity(8 + 4 + 8 * new_config_len);
                data.put_u64(RECONFIG_ID);
                data.put_u32(new_config_len as u32);
                let config = r
                    .nodes
                    .continued_nodes
                    .iter()
                    .chain(r.nodes.new_nodes.iter());
                for pid in config {
                    data.put_u64(*pid);
                }
                let pr = ProposalResp::with(data, 0); // let new leader notify client itself when it's ready
                self.cached_client
                    .as_ref()
                    .expect("No cached client!")
                    .tell_serialised(AtomicBroadcastMsg::ProposalResp(pr), self)
                    .expect("Should serialise ReconfigResponse");
                /*** handle final sequence and notify new nodes ***/
                let prev_config_id = self.active_config.id;
                let final_seq_len: u64 = r.final_sequence.get_sequence_len();
                debug!(
                    self.ctx.log(),
                    "RECONFIG: Next config_id: {}, prev_config: {}, len: {}",
                    r.config_id,
                    prev_config_id,
                    final_seq_len
                );
                let seq_metadata = SequenceMetaData::with(prev_config_id, final_seq_len);
                self.prev_sequences.insert(prev_config_id, r.final_sequence);
                let segment = if self.active_config.leader == self.pid {
                    Some(self.create_segment(&r.nodes.continued_nodes, r.config_id - 1))
                } else {
                    None
                };
                let r_init = ReconfigurationMsg::Init(ReconfigInit::with(
                    r.config_id,
                    r.nodes.clone(),
                    seq_metadata,
                    self.pid,
                    segment,
                    r.skip_prepare_n,
                ));
                for pid in &r.nodes.new_nodes {
                    let idx = *pid as usize - 1;
                    let actorpath = self
                        .nodes
                        .get(idx)
                        .unwrap_or_else(|| panic!("No actorpath found for new node {}", pid));
                    actorpath
                        .tell_serialised(r_init.clone(), self)
                        .expect("Should serialise!");
                }
                /*** Start new replica if continued ***/
                let mut nodes = r.nodes.continued_nodes;
                let mut new_nodes = r.nodes.new_nodes;
                if nodes.contains(&self.pid) {
                    if let ReconfigurationPolicy::Eager = self.policy {
                        let st = self.create_eager_sequence_transfer(&nodes, prev_config_id);
                        for pid in &new_nodes {
                            let idx = *pid as usize - 1;
                            let actorpath = self.nodes.get(idx).unwrap_or_else(|| {
                                panic!("No actorpath found for new node {}", pid)
                            });
                            actorpath
                                .tell_serialised(
                                    ReconfigurationMsg::SequenceTransfer(st.clone()),
                                    self,
                                )
                                .expect("Should serialise!");
                        }
                    }
                    nodes.append(&mut new_nodes);
                    let ble_quick_start = r.skip_prepare_n.is_none();
                    let handled = self.create_replica(
                        r.config_id,
                        nodes,
                        false,
                        true,
                        ble_quick_start,
                        r.skip_prepare_n,
                    );
                    return handled;
                }
            }
            PaxosCompMsg::KillComponents(ask) => {
                let handled = self.kill_components(ask);
                return handled;
            }
        }
        Handled::Ok
    }

    fn receive_network(&mut self, m: NetMessage) -> Handled {
        match m.data.ser_id {
            ATOMICBCAST_ID => {
                if !self.stopped && !self.active_config.pending_reconfig {
                    match self.active_config.leader {
                        my_pid if my_pid == self.pid => self.deserialise_and_propose(m),
                        other_pid if other_pid > 0 => {
                            let idx = self.active_config.leader as usize - 1;
                            let leader = self.nodes.get(idx).unwrap_or_else(|| {
                                panic!(
                                    "Could not get leader's actorpath. Pid: {}",
                                    self.active_config.leader
                                )
                            });
                            leader.forward_with_original_sender(m, self);
                        }
                        _ => {
                            self.hb_proposals.push(m);
                        }
                    }
                }
            }
            _ => {
                let NetMessage { sender, data, .. } = m;
                match_deser! {data; {
                    p: PartitioningActorMsg [PartitioningActorSer] => {
                        match p {
                            PartitioningActorMsg::Init(init) => {
                                self.partitioning_actor = Some(sender);
                                    let handled = self.new_iteration(init);
                                    return handled;
                            },
                            PartitioningActorMsg::Run => {
                                self.start_replica();
                            },
                            _ => unimplemented!()
                        }
                    },
                    rm: ReconfigurationMsg [ReconfigSer] => {
                        match rm {
                            ReconfigurationMsg::Init(r) => {
                                if self.stopped {
                                    let mut peers = r.nodes.continued_nodes;
                                    let mut new_nodes = r.nodes.new_nodes;
                                    peers.append(&mut new_nodes);
                                    let (ble_peers, communicator_peers) = self.derive_actorpaths(r.config_id, &peers);
                                    for ble_peer in ble_peers {
                                        ble_peer.tell_serialised(NetStopMsg::Peer(self.pid), self)
                                                .expect("NetStopMsg should serialise!");
                                    }
                                    for (_, comm_peer) in communicator_peers {
                                        comm_peer.tell_serialised(NetStopMsg::Peer(self.pid), self)
                                                 .expect("NetStopMsg should serialise!");
                                    }
                                    return Handled::Ok;
                                } else if self.active_config.id >= r.config_id {
                                    return Handled::Ok;
                                }
                                match self.next_config_id {
                                    None => {
                                        // info!(self.ctx.log(), "Got ReconfigInit for config_id: {} from node {}", r.config_id, r.from);
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
                                        let from = r.from;
                                        nodes.append(&mut new_nodes);
                                        let ble_quick_start = r.skip_prepare_n.is_none();
                                        if r.seq_metadata.len == 1 && r.seq_metadata.config_id == 1 {
                                            // only SS in final sequence and no other prev sequences -> start directly
                                            let final_sequence = S::new_with_sequence(vec![]);
                                            self.prev_sequences.insert(r.seq_metadata.config_id, Arc::new(final_sequence));
                                            let handled = self.create_replica(r.config_id, nodes, false, true, ble_quick_start, r.skip_prepare_n);
                                            return handled;
                                        } else {
                                            self.pending_seq_transfers = vec![(vec![], vec![]); r.config_id as usize];
                                            let skip_tag = if r.segment.is_some() {
                                                // check which tag this segment corresponds to
                                                let index = self.active_peers.0.iter().position(|x| x == &from).unwrap();
                                                let num_unready_peers = self.active_peers.1.len();
                                                let tag = num_unready_peers + index + 1;
                                                Some(tag)
                                            } else {
                                                None
                                            };
                                            match self.policy {
                                                ReconfigurationPolicy::Pull => {
                                                    self.pull_sequence(r.seq_metadata.config_id, r.seq_metadata.len, skip_tag);
                                                },
                                                ReconfigurationPolicy::Eager => {
                                                    let config_id = r.seq_metadata.config_id;
                                                    let seq_len = r.seq_metadata.len;
                                                    let idx = config_id as usize - 1;
                                                    let rem_segments: Vec<_> = (1..=num_expected_transfers).map(|x| x as u32).collect();
                                                    self.pending_seq_transfers[idx] = (rem_segments, vec![Entry::Normal(vec![]); seq_len as usize]);
                                                    let transfer_timeout = self.ctx.config()["paxos"]["transfer_timeout"].as_duration().expect("Failed to load get_decided_period");
                                                    let timer = self.schedule_once(transfer_timeout, move |c, _| c.retry_request_sequence(config_id, seq_len, num_expected_transfers as u64));
                                                    self.retry_transfer_timers.insert(config_id, timer);
                                                },
                                            }
                                            if let Some(tag) = skip_tag {
                                                let st = SequenceTransfer::with(r.seq_metadata.config_id, tag as u32, true, r.seq_metadata, r.segment.unwrap());
                                                self.handle_sequence_transfer(st);
                                            }
                                            let handled = self.create_replica(r.config_id, nodes, false, false, ble_quick_start, r.skip_prepare_n);
                                            return handled;
                                        }
                                    },
                                    Some(next_config_id) => {
                                        if next_config_id == r.config_id && r.nodes.continued_nodes.contains(&r.from) {
                                            // update who we know already decided final seq
                                            self.active_peers.1.retain(|x| x == &r.from);
                                            self.active_peers.0.push(r.from);
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
                            return self.stop_components();
                        }
                    },
                    tm: TestMessage [TestMessageSer] => {
                        match tm {
                            TestMessage::SequenceReq => {
                                let mut all_entries = vec![];
                                let mut unique = HashSet::new();
                                for i in 1..self.active_config.id {
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
                                if self.active_config.id > 0 {
                                    let active_paxos = self.paxos_replicas.last().unwrap();
                                    let sequence = active_paxos.actor_ref().ask(|promise| PaxosReplicaMsg::GetAllEntries(Ask::new(promise, ()))).wait();
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
                                    warn!(self.ctx.log(), "Got SequenceReq but no active paxos: {}", self.active_config.id);
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
            }
        }
        Handled::Ok
    }
}

#[derive(ComponentDefinition)]
struct PaxosReplica<S, P>
where
    S: SequenceTraits,
    P: PaxosStateTraits,
{
    ctx: ComponentContext<Self>,
    supervisor: ActorRef<PaxosCompMsg<S>>,
    communication_port: RequiredPort<CommunicationPort>,
    ble_port: RequiredPort<BallotLeaderElection>,
    peers: Vec<u64>,
    paxos: Paxos<S, P>,
    config_id: u32,
    pid: u64,
    current_leader: u64,
    timers: Option<(ScheduledTimer, ScheduledTimer)>,
    skipped_prepare: bool,
    pending_reconfig: bool,
    stopped: bool,
    stopped_peers: HashSet<u64>,
    stop_ask: Option<Ask<(bool, bool), ()>>,
}

impl<S, P> PaxosReplica<S, P>
where
    S: SequenceTraits,
    P: PaxosStateTraits,
{
    fn with(
        supervisor: ActorRef<PaxosCompMsg<S>>,
        peers: Vec<u64>,
        config_id: u32,
        pid: u64,
        raw_paxos_log: KompactLogger,
        skipped_prepare_ballot: Option<Ballot>,
        max_inflight: usize,
    ) -> PaxosReplica<S, P> {
        let seq = S::new_with_sequence(Vec::with_capacity(max_inflight));
        let paxos_state = P::new();
        let storage = Storage::with(seq, paxos_state);
        let skipped_prepare = match skipped_prepare_ballot {
            Some(b) if b.pid != pid => true,
            _ => false,
        };
        let paxos = Paxos::with(
            config_id,
            pid,
            peers.clone(),
            storage,
            raw_paxos_log,
            skipped_prepare_ballot,
            Some(max_inflight),
        );
        PaxosReplica {
            ctx: ComponentContext::uninitialised(),
            supervisor,
            communication_port: RequiredPort::uninitialised(),
            ble_port: RequiredPort::uninitialised(),
            stopped_peers: HashSet::with_capacity(peers.len()),
            peers,
            paxos,
            config_id,
            pid,
            current_leader: 0,
            timers: None,
            skipped_prepare,
            pending_reconfig: false,
            stopped: false,
            stop_ask: None,
        }
    }

    fn start_timers(&mut self) {
        let config = self.ctx.config();
        let get_decided_period = config["paxos"]["get_decided_period"]
            .as_duration()
            .expect("Failed to load get_decided_period");
        let outgoing_period = config["experiment"]["outgoing_period"]
            .as_duration()
            .expect("Failed to load outgoing_period");
        let request_acceptsync_timer = config["paxos"]["request_acceptsync_timer"]
            .as_duration()
            .expect("Failed to load request_acceptsync_timer");
        let decided_timer =
            self.schedule_periodic(Duration::from_millis(1), get_decided_period, move |c, _| {
                c.get_decided()
            });
        let outgoing_timer =
            self.schedule_periodic(Duration::from_millis(0), outgoing_period, move |p, _| {
                p.send_outgoing()
            });
        if self.skipped_prepare {
            let _ = self.schedule_periodic(
                Duration::from_millis(0),
                request_acceptsync_timer,
                move |c, _| {
                    c.paxos.request_firstaccept_if_not_started();
                    Handled::Ok
                },
            );
        }
        self.timers = Some((decided_timer, outgoing_timer));
    }

    fn stop_timers(&mut self) {
        if let Some(timers) = self.timers.take() {
            self.cancel_timer(timers.0);
            self.cancel_timer(timers.1);
        }
    }

    fn stop_if_pending(&mut self) -> Handled {
        if self.stopped_peers.len() != self.peers.len() {
            self.stop_ask
                .take()
                .expect("No stop ask")
                .reply(())
                .expect("Failed to reply to late stop ask");
        }
        Handled::Ok
    }

    fn send_outgoing(&mut self) -> Handled {
        for out_msg in self.paxos.get_outgoing_msgs() {
            self.communication_port
                .trigger(CommunicatorMsg::RawPaxosMsg(out_msg));
        }
        Handled::Ok
    }

    fn handle_stopsign(&mut self, ss: &StopSign) {
        let final_seq = self.paxos.stop_and_get_sequence();
        let new_config_len = ss.nodes.len();
        let mut data: Vec<u8> = Vec::with_capacity(8 + 4 + 8 * new_config_len);
        data.put_u64(RECONFIG_ID);
        data.put_u32(new_config_len as u32);
        for pid in &ss.nodes {
            data.put_u64(*pid);
        }
        let (continued_nodes, new_nodes) = ss
            .nodes
            .iter()
            .partition(|&pid| pid == &self.pid || self.peers.contains(pid));
        debug!(
            self.ctx.log(),
            "Decided StopSign! Continued: {:?}, new: {:?}", &continued_nodes, &new_nodes
        );
        let nodes = Reconfig::with(continued_nodes, new_nodes);
        let r = FinalMsg::with(ss.config_id, nodes, final_seq, ss.skip_prepare_n);
        self.supervisor.tell(PaxosCompMsg::Reconfig(r));
    }

    fn get_decided(&mut self) -> Handled {
        let leader = self.paxos.get_current_leader();
        if self.current_leader != leader {
            self.current_leader = leader;
            self.supervisor
                .tell(PaxosCompMsg::Leader(self.config_id, self.current_leader));
        }
        if self.current_leader == self.pid {
            // leader: check reconfiguration and send responses to client
            let decided_entries = self.paxos.get_decided_entries().to_vec();
            let last = decided_entries.last();
            if let Some(Entry::StopSign(ss)) = last {
                self.handle_stopsign(&ss);
            }
            let latest_leader = if self.paxos.stopped() {
                0 // if we are/have reconfigured don't call ourselves leader
            } else {
                self.pid
            };
            for decided in decided_entries {
                if let Entry::Normal(data) = decided {
                    let pr = ProposalResp::with(data, latest_leader);
                    self.communication_port
                        .trigger(CommunicatorMsg::ProposalResponse(pr));
                }
            }
        } else {
            // follower: just handle a possible reconfiguration
            if let Some(Entry::StopSign(ss)) = self.paxos.get_decided_entries().last().cloned() {
                self.handle_stopsign(&ss);
            }
        }
        Handled::Ok
    }

    fn propose(&mut self, p: Proposal) -> Result<(), Vec<u8>> {
        match p.reconfig {
            Some((reconfig, _)) => {
                let prio_start_round = self.ctx.config()["paxos"]["prio_start_round"]
                    .as_i64()
                    .map(|x| x as u64);
                self.paxos
                    .propose_reconfiguration(reconfig, prio_start_round)
            }
            None => self.paxos.propose_normal(p.data),
        }
    }
}

impl<S, P> Actor for PaxosReplica<S, P>
where
    S: SequenceTraits,
    P: PaxosStateTraits,
{
    type Message = PaxosReplicaMsg;

    fn receive_local(&mut self, msg: PaxosReplicaMsg) -> Handled {
        match msg {
            PaxosReplicaMsg::Propose(p) => {
                if !self.pending_reconfig {
                    if let Err(data) = self.propose(p) {
                        self.pending_reconfig = true;
                        self.supervisor.tell(PaxosCompMsg::PendingReconfig(data));
                    }
                }
            }
            PaxosReplicaMsg::LocalSequenceReq(requestor, seq_req, prev_seq_metadata) => {
                let (succeeded, entries) = self
                    .paxos
                    .get_chosen_entries(seq_req.from_idx, seq_req.to_idx);
                let segment = SequenceSegment::with(seq_req.from_idx, seq_req.to_idx, entries);
                let st = SequenceTransfer::with(
                    seq_req.config_id,
                    seq_req.tag,
                    succeeded,
                    prev_seq_metadata,
                    segment,
                );
                requestor
                    .tell_serialised(ReconfigurationMsg::SequenceTransfer(st), self)
                    .expect("Should serialise!");
            }
            PaxosReplicaMsg::GetAllEntries(a) => {
                // for testing only
                let seq = self.paxos.get_sequence();
                a.reply(seq).expect("Failed to reply to GetAllEntries");
            }
            PaxosReplicaMsg::Stop(ask) => {
                let (ack_client, late_stop) = *ask.request();
                self.communication_port
                    .trigger(CommunicatorMsg::SendStop(self.pid, ack_client));
                self.stop_timers();
                self.stopped = true;
                if self.stopped_peers.len() == self.peers.len() {
                    ask.reply(()).expect("Failed to reply stop ask");
                } else {
                    if late_stop {
                        self.schedule_once(Duration::from_secs(3), move |c, _| c.stop_if_pending());
                    }
                    self.stop_ask = Some(ask);
                }
            }
        }
        Handled::Ok
    }

    fn receive_network(&mut self, _: NetMessage) -> Handled {
        // ignore
        Handled::Ok
    }
}

impl<S, P> ComponentLifecycle for PaxosReplica<S, P>
where
    S: SequenceTraits,
    P: PaxosStateTraits,
{
    fn on_start(&mut self) -> Handled {
        let bc = BufferConfig::default();
        self.ctx.borrow().init_buffers(Some(bc), None);
        self.start_timers();
        Handled::Ok
    }

    fn on_kill(&mut self) -> Handled {
        self.stop_timers();
        Handled::Ok
    }
}

impl<S, P> Require<CommunicationPort> for PaxosReplica<S, P>
where
    S: SequenceTraits,
    P: PaxosStateTraits,
{
    fn handle(&mut self, msg: <CommunicationPort as Port>::Indication) -> Handled {
        match msg {
            AtomicBroadcastCompMsg::RawPaxosMsg(pm) if !self.stopped => {
                self.paxos.handle(pm);
            }
            AtomicBroadcastCompMsg::StopMsg(pid) => {
                assert!(
                    self.stopped_peers.insert(pid),
                    "Got duplicate stop from peer {}",
                    pid
                );
                if self.stopped && self.stopped_peers.len() == self.peers.len() {
                    self.stop_ask
                        .take()
                        .expect("No stop ask!")
                        .reply(())
                        .expect("Failed to reply stop ask!");
                }
            }
            _ => {}
        }
        Handled::Ok
    }
}

impl<S, P> Require<BallotLeaderElection> for PaxosReplica<S, P>
where
    S: SequenceTraits,
    P: PaxosStateTraits,
{
    fn handle(&mut self, l: Leader) -> Handled {
        // info!(self.ctx.log(), "Node {} became leader in config {}. Ballot: {:?}",  l.pid, self.config_id, l.ballot);
        self.paxos.handle_leader(l);
        if self.current_leader != l.pid && !self.paxos.stopped() {
            self.current_leader = l.pid;
            self.supervisor
                .tell(PaxosCompMsg::Leader(self.config_id, l.pid));
        }
        Handled::Ok
    }
}

pub mod raw_paxos {
    use super::super::messages::paxos::ballot_leader_election::{Ballot, Leader};
    use super::super::messages::paxos::*;
    use super::super::storage::paxos::Storage;
    use super::{PaxosStateTraits, SequenceTraits};
    use crate::serialiser_ids::RECONFIG_ID;
    use kompact::prelude::BufMut;
    use kompact::KompactLogger;
    use std::fmt::Debug;
    use std::mem;
    use std::sync::Arc;

    pub struct Paxos<S, P>
    where
        S: SequenceTraits,
        P: PaxosStateTraits,
    {
        storage: Storage<S, P>,
        config_id: u32,
        pid: u64,
        majority: usize,
        peers: Vec<u64>, // excluding self pid
        state: (Role, Phase),
        leader: u64,
        n_leader: Ballot,
        promises_meta: Vec<Option<(Ballot, usize)>>,
        las: Vec<u64>,
        lds: Vec<Option<u64>>,
        proposals: Vec<Entry>,
        lc: u64, // length of longest chosen seq
        prev_ld: u64,
        acc_sync_ld: u64,
        max_promise_meta: (Ballot, usize, u64), // ballot, sfx len, pid
        max_promise_sfx: Vec<Entry>,
        batch_accept_meta: Vec<Option<(Ballot, usize)>>, //  ballot, index in outgoing
        latest_decide_meta: Vec<Option<(Ballot, usize)>>,
        latest_accepted_meta: Option<(Ballot, usize)>,
        outgoing: Vec<Message>,
        num_nodes: usize,
        log: KompactLogger,
        max_inflight: usize,
        requested_firstaccept: bool,
    }

    impl<S, P> Paxos<S, P>
    where
        S: SequenceTraits,
        P: PaxosStateTraits,
    {
        /*** User functions ***/
        pub fn with(
            config_id: u32,
            pid: u64,
            peers: Vec<u64>,
            storage: Storage<S, P>,
            log: KompactLogger,
            skipped_prepare: Option<Ballot>,
            max_inflight: Option<usize>,
        ) -> Paxos<S, P> {
            let num_nodes = &peers.len() + 1;
            let majority = num_nodes / 2 + 1;
            let max_peer_pid = peers.iter().max().unwrap();
            let max_pid = std::cmp::max(max_peer_pid, &pid);
            let num_nodes = *max_pid as usize;
            let (state, lds) = match skipped_prepare {
                Some(n) => {
                    let (role, lds) = if n.pid == pid {
                        let mut v = vec![None; num_nodes];
                        for peer in &peers {
                            let idx = *peer as usize - 1;
                            v[idx] = Some(0);
                        }
                        (Role::Leader, v)
                    } else {
                        (Role::Follower, vec![None; num_nodes])
                    };
                    let state = (role, Phase::FirstAccept);
                    (state, lds)
                }
                _ => {
                    let state = (Role::Follower, Phase::None);
                    let lds = vec![None; num_nodes];
                    (state, lds)
                }
            };
            let n_leader = skipped_prepare.unwrap_or_else(|| Ballot::with(0, 0));
            // info!(log, "Start raw paxos pid: {}, state: {:?}, n_leader: {:?}", pid, state, n_leader);
            let max_inflight = max_inflight.unwrap_or(100000);
            let mut paxos = Paxos {
                storage,
                pid,
                config_id,
                majority,
                peers,
                state,
                leader: n_leader.pid,
                n_leader,
                promises_meta: vec![None; num_nodes],
                las: vec![0; num_nodes],
                lds,
                proposals: Vec::with_capacity(max_inflight),
                lc: 0,
                prev_ld: 0,
                acc_sync_ld: 0,
                max_promise_meta: (Ballot::with(0, 0), 0, 0),
                max_promise_sfx: vec![],
                batch_accept_meta: vec![None; num_nodes],
                latest_decide_meta: vec![None; num_nodes],
                latest_accepted_meta: None,
                outgoing: Vec::with_capacity(max_inflight),
                num_nodes,
                log,
                max_inflight,
                requested_firstaccept: false,
            };
            paxos.storage.set_promise(n_leader);
            paxos
        }

        pub fn get_current_leader(&self) -> u64 {
            self.leader
        }

        pub fn get_outgoing_msgs(&mut self) -> Vec<Message> {
            let mut outgoing = Vec::with_capacity(self.max_inflight);
            std::mem::swap(&mut self.outgoing, &mut outgoing);
            #[cfg(feature = "batch_accept")]
            {
                self.batch_accept_meta = vec![None; self.num_nodes];
            }
            #[cfg(feature = "latest_decide")]
            {
                self.latest_decide_meta = vec![None; self.num_nodes];
            }
            #[cfg(feature = "latest_accepted")]
            {
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
                PaxosMsg::Promise(prom) => match &self.state {
                    (Role::Leader, Phase::Prepare) => self.handle_promise_prepare(prom, m.from),
                    (Role::Leader, Phase::Accept) => self.handle_promise_accept(prom, m.from),
                    _ => {}
                },
                PaxosMsg::FirstAcceptReq => self.handle_firstacceptreq(m.from),
                PaxosMsg::AcceptSync(acc_sync) => self.handle_accept_sync(acc_sync, m.from),
                PaxosMsg::FirstAccept(f) => self.handle_firstaccept(f),
                PaxosMsg::AcceptDecide(acc) => self.handle_acceptdecide(acc, m.from),
                PaxosMsg::Accepted(accepted) => self.handle_accepted(accepted, m.from),
                PaxosMsg::Decide(d) => self.handle_decide(d),
                PaxosMsg::ProposalForward(proposals) => self.handle_forwarded_proposal(proposals),
            }
        }

        pub fn stopped(&self) -> bool {
            self.storage.stopped()
        }

        pub fn propose_normal(&mut self, data: Vec<u8>) -> Result<(), Vec<u8>> {
            if self.stopped() {
                Err(data)
            } else {
                let entry = Entry::Normal(data);
                match self.state {
                    (Role::Leader, Phase::Prepare) => self.proposals.push(entry),
                    (Role::Leader, Phase::Accept) => self.send_accept(entry),
                    (Role::Leader, Phase::FirstAccept) => self.send_first_accept(entry),
                    _ => self.forward_proposals(vec![entry]),
                }
                Ok(())
            }
        }

        pub fn propose_reconfiguration(
            &mut self,
            nodes: Vec<u64>,
            prio_start_round: Option<u64>,
        ) -> Result<(), Vec<u8>> {
            if self.stopped() {
                let mut data: Vec<u8> = Vec::with_capacity(8);
                data.put_u64(RECONFIG_ID);
                Err(data)
            } else {
                let continued_nodes: Vec<&u64> = nodes
                    .iter()
                    .filter(|&pid| pid == &self.pid || self.peers.contains(pid))
                    .collect();
                let skip_prepare_n = if !continued_nodes.is_empty() {
                    let my_idx = self.pid as usize - 1;
                    let max_idx = self
                        .las
                        .iter()
                        .enumerate()
                        .filter(|(idx, _)| {
                            idx != &my_idx && continued_nodes.contains(&&(*idx as u64 + 1))
                        })
                        .max_by(|(_, la), (_, other_la)| la.cmp(other_la));
                    let max_pid = match max_idx {
                        Some((other_idx, _)) => other_idx as u64 + 1, // give leadership of new config to most up-to-date peer
                        None => self.pid,
                    };
                    Some(Ballot::with(prio_start_round.unwrap_or(0), max_pid))
                } else {
                    None
                };
                let ss = StopSign::with(self.config_id + 1, nodes, skip_prepare_n);
                let entry = Entry::StopSign(ss);
                match self.state {
                    (Role::Leader, Phase::Prepare) => self.proposals.push(entry),
                    (Role::Leader, Phase::Accept) => self.send_accept(entry),
                    (Role::Leader, Phase::FirstAccept) => self.send_first_accept(entry),
                    _ => self.forward_proposals(vec![entry]),
                }
                Ok(())
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

        /// Allows user to request FirstAccept if still not started after skipping prepare phase in reconfiguration.
        /// Breaks the deadlock of when the leader has sent all its accept messages and waits for accepted but
        /// followers never received those due to they hadn't started yet.
        pub fn request_firstaccept_if_not_started(&mut self) {
            if self.state == (Role::Follower, Phase::FirstAccept) && !self.requested_firstaccept {
                self.requested_firstaccept = true;
                self.outgoing.push(Message::with(
                    self.pid,
                    self.leader,
                    PaxosMsg::FirstAcceptReq,
                ));
            } // else: we have already started in new config, don't care about this call
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
                /* insert my longest decided sequence */
                self.acc_sync_ld = ld;
                /* initialise longest chosen sequence and update state */
                self.lc = 0;
                self.state = (Role::Leader, Phase::Prepare);
                /* send prepare */
                for pid in &self.peers {
                    let prep = Prepare::with(n, ld, self.storage.get_accepted_ballot());
                    self.outgoing
                        .push(Message::with(self.pid, *pid, PaxosMsg::Prepare(prep)));
                }
            } else {
                self.state.0 = Role::Follower;
            }
        }

        fn forward_proposals(&mut self, mut entries: Vec<Entry>) {
            if self.leader > 0 && self.leader != self.pid {
                let pf = PaxosMsg::ProposalForward(entries);
                let msg = Message::with(self.pid, self.leader, pf);
                // println!("Forwarding to node {}", self.leader);
                self.outgoing.push(msg);
            } else {
                self.proposals.append(&mut entries);
            }
        }

        fn handle_forwarded_proposal(&mut self, mut entries: Vec<Entry>) {
            if !self.stopped() {
                match self.state {
                    (Role::Leader, Phase::Prepare) => self.proposals.append(&mut entries),
                    (Role::Leader, Phase::Accept) => self.send_batch_accept(entries),
                    (Role::Leader, Phase::FirstAccept) => {
                        let rest = entries.split_off(1);
                        self.send_first_accept(entries.pop().unwrap());
                        self.send_batch_accept(rest);
                    }
                    _ => self.forward_proposals(entries),
                }
            }
        }

        fn send_first_accept(&mut self, entry: Entry) {
            // info!(self.log, "Sending first accept");
            let promised_pids = self
                .lds
                .iter()
                .enumerate()
                .filter(|(_, x)| x.is_some())
                .map(|(idx, _)| idx as u64 + 1);
            for pid in promised_pids {
                let f = FirstAccept::with(self.n_leader, vec![entry.clone()]);
                self.outgoing
                    .push(Message::with(self.pid, pid, PaxosMsg::FirstAccept(f)));
            }
            let la = self.storage.append_entry(entry);
            self.las[self.pid as usize - 1] = la;
            self.state.1 = Phase::Accept;
        }

        fn send_accept(&mut self, entry: Entry) {
            let promised_idx = self
                .lds
                .iter()
                .enumerate()
                .filter(|(_, x)| x.is_some())
                .map(|(idx, _)| idx);
            for idx in promised_idx {
                if cfg!(feature = "batch_accept") {
                    match self.batch_accept_meta.get_mut(idx).unwrap() {
                        Some((ballot, outgoing_idx)) if ballot == &self.n_leader => {
                            let Message { msg, .. } = self.outgoing.get_mut(*outgoing_idx).unwrap();
                            match msg {
                                PaxosMsg::AcceptDecide(a) => a.entries.push(entry.clone()),
                                PaxosMsg::AcceptSync(acc) => acc.entries.push(entry.clone()),
                                PaxosMsg::FirstAccept(f) => f.entries.push(entry.clone()),
                                _ => panic!("Not Accept or AcceptSync when batching"),
                            }
                        }
                        _ => {
                            let acc =
                                AcceptDecide::with(self.n_leader, self.lc, vec![entry.clone()]);
                            let cache_idx = self.outgoing.len();
                            let pid = idx as u64 + 1;
                            self.outgoing.push(Message::with(
                                self.pid,
                                pid,
                                PaxosMsg::AcceptDecide(acc),
                            ));
                            self.batch_accept_meta[idx] = Some((self.n_leader, cache_idx));
                            #[cfg(feature = "latest_decide")]
                            {
                                self.latest_decide_meta[idx] = Some((self.n_leader, cache_idx));
                            }
                        }
                    }
                } else {
                    let pid = idx as u64 + 1;
                    let acc = AcceptDecide::with(self.n_leader, self.lc, vec![entry.clone()]);
                    self.outgoing
                        .push(Message::with(self.pid, pid, PaxosMsg::AcceptDecide(acc)));
                }
            }
            let la = self.storage.append_entry(entry);
            self.las[self.pid as usize - 1] = la;
        }

        fn send_batch_accept(&mut self, mut entries: Vec<Entry>) {
            let promised_idx = self
                .lds
                .iter()
                .enumerate()
                .filter(|(_, x)| x.is_some())
                .map(|(idx, _)| idx);
            for idx in promised_idx {
                if cfg!(feature = "batch_accept") {
                    match self.batch_accept_meta.get_mut(idx).unwrap() {
                        Some((ballot, outgoing_idx)) if ballot == &self.n_leader => {
                            let Message { msg, .. } = self.outgoing.get_mut(*outgoing_idx).unwrap();
                            match msg {
                                PaxosMsg::AcceptDecide(a) => {
                                    a.entries.append(entries.clone().as_mut())
                                }
                                PaxosMsg::AcceptSync(acc) => {
                                    acc.entries.append(entries.clone().as_mut())
                                }
                                PaxosMsg::FirstAccept(f) => {
                                    f.entries.append(entries.clone().as_mut())
                                }
                                _ => panic!("Not Accept or AcceptSync when batching"),
                            }
                        }
                        _ => {
                            let acc = AcceptDecide::with(self.n_leader, self.lc, entries.clone());
                            let cache_idx = self.outgoing.len();
                            let pid = idx as u64 + 1;
                            self.outgoing.push(Message::with(
                                self.pid,
                                pid,
                                PaxosMsg::AcceptDecide(acc),
                            ));
                            self.batch_accept_meta[idx] = Some((self.n_leader, cache_idx));
                            #[cfg(feature = "latest_decide")]
                            {
                                self.latest_decide_meta[idx] = Some((self.n_leader, cache_idx));
                            }
                        }
                    }
                } else {
                    let pid = idx as u64 + 1;
                    let acc = AcceptDecide::with(self.n_leader, self.lc, entries.clone());
                    self.outgoing
                        .push(Message::with(self.pid, pid, PaxosMsg::AcceptDecide(acc)));
                }
            }
            let la = self.storage.append_sequence(&mut entries);
            self.las[self.pid as usize - 1] = la;
        }

        fn handle_promise_prepare(&mut self, prom: Promise, from: u64) {
            if prom.n == self.n_leader {
                let sfx_len = prom.sfx.len();
                let promise_meta = &(prom.n_accepted, sfx_len, from);
                if promise_meta > &self.max_promise_meta && sfx_len > 0
                    || (sfx_len == 0 && prom.ld >= self.acc_sync_ld)
                {
                    self.max_promise_meta = *promise_meta;
                    self.max_promise_sfx = prom.sfx;
                }
                let idx = from as usize - 1;
                self.promises_meta[idx] = Some((prom.n_accepted, sfx_len));
                self.lds[idx] = Some(prom.ld);
                let num_promised = self.promises_meta.iter().filter(|x| x.is_some()).count();
                if num_promised >= self.majority {
                    let (max_promise_n, max_sfx_len, max_pid) = self.max_promise_meta;
                    let last_is_stop = match self.max_promise_sfx.last() {
                        Some(e) => e.is_stopsign(),
                        None => false,
                    };
                    let max_sfx_is_empty = self.max_promise_sfx.is_empty();
                    if max_pid != self.pid {
                        // sync self with max pid's sequence
                        let my_promise = &self.promises_meta[self.pid as usize - 1].unwrap();
                        if my_promise != &(max_promise_n, max_sfx_len) {
                            self.storage
                                .append_on_decided_prefix(mem::take(&mut self.max_promise_sfx));
                        }
                    }
                    if last_is_stop {
                        self.proposals.clear(); // will never be decided
                    } else {
                        Self::drop_after_stopsign(&mut self.proposals); // drop after ss, if ss exists
                    }
                    // create accept_sync with only new proposals for all pids with max_promise
                    let mut new_entries = mem::take(&mut self.proposals);
                    let max_ld = self.lds[max_pid as usize - 1].unwrap_or(self.acc_sync_ld); // unwrap_or: if we are max_pid then unwrap will be none
                    let max_promise_acc_sync =
                        AcceptSync::with(self.n_leader, new_entries.clone(), max_ld, false);
                    // append new proposals in my sequence
                    let la = self.storage.append_sequence(&mut new_entries);
                    self.las[self.pid as usize - 1] = la;
                    self.state = (Role::Leader, Phase::Accept);
                    // send accept_sync to followers
                    let my_idx = self.pid as usize - 1;
                    let promised = self
                        .lds
                        .iter()
                        .enumerate()
                        .filter(|(idx, ld)| idx != &my_idx && ld.is_some());
                    for (idx, l) in promised {
                        let pid = idx as u64 + 1;
                        let ld = l.unwrap();
                        let promise_meta = &self.promises_meta[idx].unwrap_or_else(|| {
                            panic!("No promise from {}. Max pid: {}", pid, max_pid)
                        });
                        if cfg!(feature = "max_accsync") {
                            if promise_meta == &(max_promise_n, max_sfx_len) {
                                if !max_sfx_is_empty || ld >= self.acc_sync_ld {
                                    let msg = Message::with(
                                        self.pid,
                                        pid,
                                        PaxosMsg::AcceptSync(max_promise_acc_sync.clone()),
                                    );
                                    self.outgoing.push(msg);
                                }
                            } else {
                                let sfx = self.storage.get_suffix(ld);
                                let acc_sync = AcceptSync::with(self.n_leader, sfx, ld, true);
                                let msg =
                                    Message::with(self.pid, pid, PaxosMsg::AcceptSync(acc_sync));
                                self.outgoing.push(msg);
                            }
                        } else {
                            let sfx = self.storage.get_suffix(ld);
                            let acc_sync = AcceptSync::with(self.n_leader, sfx, ld, true);
                            let msg = Message::with(self.pid, pid, PaxosMsg::AcceptSync(acc_sync));
                            self.outgoing.push(msg);
                        }
                        #[cfg(feature = "batch_accept")]
                        {
                            self.batch_accept_meta[idx] =
                                Some((self.n_leader, self.outgoing.len() - 1));
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
                let (sync, sfx_start) = if promise_meta == &(max_ballot, max_sfx_len)
                    && cfg!(feature = "max_accsync")
                {
                    match max_sfx_len == 0 {
                        false => (false, self.acc_sync_ld + sfx_len as u64),
                        true if prom.ld >= self.acc_sync_ld => {
                            (false, self.acc_sync_ld + sfx_len as u64)
                        }
                        _ => (true, prom.ld),
                    }
                } else {
                    (true, prom.ld)
                };
                let sfx = self.storage.get_suffix(sfx_start);
                // println!("Handle promise from {} in Accept phase: {:?}, sfx len: {}", from, (sync, sfx_start), sfx.len());
                let acc_sync = AcceptSync::with(self.n_leader, sfx, prom.ld, sync);
                self.outgoing.push(Message::with(
                    self.pid,
                    from,
                    PaxosMsg::AcceptSync(acc_sync),
                ));
                // inform what got decided already
                let ld = if self.lc > 0 {
                    self.lc
                } else {
                    self.storage.get_decided_len()
                };
                if ld > prom.ld {
                    let d = Decide::with(ld, self.n_leader);
                    self.outgoing
                        .push(Message::with(self.pid, from, PaxosMsg::Decide(d)));
                    #[cfg(feature = "latest_decide")]
                    {
                        let idx = from as usize - 1;
                        let cached_idx = self.outgoing.len() - 1;
                        self.latest_decide_meta[idx] = Some((self.n_leader, cached_idx));
                    }
                }
            }
        }

        fn handle_firstacceptreq(&mut self, from: u64) {
            if self.state.0 != Role::Leader || self.state.1 == Phase::FirstAccept {
                return;
            }
            let entries = self.storage.get_sequence();
            let f = FirstAccept::with(self.n_leader, entries);
            let pm = PaxosMsg::FirstAccept(f);
            if cfg!(feature = "batch_accept") {
                let idx = from as usize - 1;
                /*** replace any cached msg with the FirstAccept (as receiver will discard the original msg anyway) ***/
                let cache_idx = if let Some((_, cached_accept_idx)) =
                    self.batch_accept_meta.get_mut(idx).unwrap()
                {
                    let Message { msg, .. } = self.outgoing.get_mut(*cached_accept_idx).unwrap();
                    *msg = pm;
                    *cached_accept_idx
                } else if let Some((_, cached_decide_idx)) =
                    self.latest_decide_meta.get_mut(idx).unwrap()
                {
                    let Message { msg, .. } = self.outgoing.get_mut(*cached_decide_idx).unwrap();
                    *msg = pm;
                    *cached_decide_idx
                } else {
                    self.outgoing.push(Message::with(self.pid, from, pm));
                    self.outgoing.len() - 1
                };
                self.batch_accept_meta[idx] = Some((self.n_leader, cache_idx));
                #[cfg(feature = "latest_decide")]
                {
                    self.latest_decide_meta[idx] = None;
                }
            } else {
                self.outgoing.push(Message::with(self.pid, from, pm));
            }
        }

        fn handle_accepted(&mut self, accepted: Accepted, from: u64) {
            if accepted.n == self.n_leader && self.state == (Role::Leader, Phase::Accept) {
                self.las[from as usize - 1] = accepted.la;
                if accepted.la > self.lc {
                    let chosen =
                        self.las.iter().filter(|la| *la >= &accepted.la).count() >= self.majority;
                    if chosen {
                        self.lc = accepted.la;
                        let d = Decide::with(self.lc, self.n_leader);
                        if cfg!(feature = "latest_decide") {
                            let promised_idx =
                                self.lds.iter().enumerate().filter(|(_, ld)| ld.is_some());
                            for (idx, _) in promised_idx {
                                match self.latest_decide_meta.get_mut(idx).unwrap() {
                                    Some((ballot, outgoing_dec_idx))
                                        if ballot == &self.n_leader =>
                                    {
                                        let Message { msg, .. } =
                                            self.outgoing.get_mut(*outgoing_dec_idx).unwrap();
                                        match msg {
                                            PaxosMsg::AcceptDecide(a) => a.ld = self.lc,
                                            PaxosMsg::Decide(d) => d.ld = self.lc,
                                            _ => {
                                                panic!("Cached message in outgoing was not Decide")
                                            }
                                        }
                                    }
                                    _ => {
                                        let cache_dec_idx = self.outgoing.len();
                                        self.latest_decide_meta[idx] =
                                            Some((self.n_leader, cache_dec_idx));
                                        let pid = idx as u64 + 1;
                                        self.outgoing.push(Message::with(
                                            self.pid,
                                            pid,
                                            PaxosMsg::Decide(d.clone()),
                                        ));
                                    }
                                }
                            }
                        } else {
                            let promised_pids = self
                                .lds
                                .iter()
                                .enumerate()
                                .filter(|(_, ld)| ld.is_some())
                                .map(|(idx, _)| idx as u64 + 1);
                            for pid in promised_pids {
                                self.outgoing.push(Message::with(
                                    self.pid,
                                    pid,
                                    PaxosMsg::Decide(d.clone()),
                                ));
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
                self.storage.set_promise(prep.n);
                self.state = (Role::Follower, Phase::Prepare);
                let na = self.storage.get_accepted_ballot();
                let sfx = if na >= prep.n_accepted {
                    self.storage.get_suffix(prep.ld)
                } else {
                    vec![]
                };
                let p = Promise::with(prep.n, na, sfx, self.storage.get_decided_len());
                self.outgoing
                    .push(Message::with(self.pid, from, PaxosMsg::Promise(p)));
            }
        }

        fn handle_accept_sync(&mut self, acc_sync: AcceptSync, from: u64) {
            if self.state == (Role::Follower, Phase::Prepare)
                && self.storage.get_promise() == acc_sync.n
            {
                self.storage.set_accepted_ballot(acc_sync.n);
                let mut entries = acc_sync.entries;
                let la = if acc_sync.sync {
                    self.storage.append_on_prefix(acc_sync.ld, &mut entries)
                } else {
                    self.storage.append_sequence(&mut entries)
                };
                self.state = (Role::Follower, Phase::Accept);
                let accepted = Accepted::with(acc_sync.n, la);
                #[cfg(feature = "latest_accepted")]
                {
                    let cached_idx = self.outgoing.len();
                    self.latest_accepted_meta = Some((acc_sync.n, cached_idx));
                }
                self.outgoing
                    .push(Message::with(self.pid, from, PaxosMsg::Accepted(accepted)));
                /*** Forward proposals ***/
                let proposals = mem::take(&mut self.proposals);
                if !proposals.is_empty() {
                    self.forward_proposals(proposals);
                }
            }
        }

        fn handle_firstaccept(&mut self, f: FirstAccept) {
            if self.storage.get_promise() == f.n {
                match self.state {
                    (Role::Follower, Phase::FirstAccept) => {
                        let mut entries = f.entries;
                        self.storage.set_accepted_ballot(f.n);
                        self.accept_entries(f.n, &mut entries);
                        self.state.1 = Phase::Accept;
                        /*** Forward proposals ***/
                        let proposals = mem::take(&mut self.proposals);
                        if !proposals.is_empty() {
                            self.forward_proposals(proposals);
                        }
                    }
                    (Role::Follower, Phase::Accept) => {
                        if f.entries.len() as u64 > self.storage.get_sequence_len() {
                            let mut entries = f.entries;
                            self.storage.append_on_prefix(0, &mut entries);
                        }
                    }
                    _ => {}
                }
            }
        }

        fn handle_acceptdecide(&mut self, acc: AcceptDecide, from: u64) {
            if self.storage.get_promise() == acc.n {
                match self.state {
                    (Role::Follower, Phase::Accept) => {
                        let mut entries = acc.entries;
                        self.accept_entries(acc.n, &mut entries);
                        // handle decide
                        if acc.ld > self.storage.get_decided_len() {
                            self.storage.set_decided_len(acc.ld);
                        }
                    }
                    (Role::Follower, Phase::FirstAccept) if !self.requested_firstaccept => {
                        self.requested_firstaccept = true;
                        self.outgoing
                            .push(Message::with(self.pid, from, PaxosMsg::FirstAcceptReq));
                    }
                    _ => {}
                }
            }
        }

        fn handle_decide(&mut self, dec: Decide) {
            if self.storage.get_promise() == dec.n {
                match self.state.1 {
                    Phase::FirstAccept => {
                        if !self.requested_firstaccept {
                            self.requested_firstaccept = true;
                            self.outgoing.push(Message::with(
                                self.pid,
                                self.leader,
                                PaxosMsg::FirstAcceptReq,
                            ));
                        }
                    }
                    _ => {
                        self.storage.set_decided_len(dec.ld);
                        /*if dec.ld == self.storage.get_sequence_len() && self.stopped() && self.leader == self.pid{
                            info!(self.log, "Decided StopSign: ld={}, las: {:?}", dec.ld, self.las);
                        }*/
                    }
                }
            }
        }

        /*** algorithm specific functions ***/
        fn drop_after_stopsign(entries: &mut Vec<Entry>) {
            // drop all entries ordered after stopsign (if any)
            let ss_idx = entries.iter().position(|e| e.is_stopsign());
            if let Some(idx) = ss_idx {
                entries.truncate(idx + 1);
            };
        }

        pub fn get_sequence(&self) -> Vec<Entry> {
            self.storage.get_sequence()
        }

        fn accept_entries(&mut self, n: Ballot, entries: &mut Vec<Entry>) {
            let la = self.storage.append_sequence(entries);
            if cfg!(feature = "latest_accepted") {
                match self.latest_accepted_meta {
                    Some((ballot, outgoing_idx)) if ballot == n => {
                        let Message { msg, .. } = self.outgoing.get_mut(outgoing_idx).unwrap();
                        match msg {
                            PaxosMsg::Accepted(a) => a.la = la,
                            _ => panic!("Cached idx is not an Accepted message!"),
                        }
                    }
                    _ => {
                        let accepted = Accepted::with(n, la);
                        let cached_idx = self.outgoing.len();
                        self.latest_accepted_meta = Some((n, cached_idx));
                        self.outgoing.push(Message::with(
                            self.pid,
                            self.leader,
                            PaxosMsg::Accepted(accepted),
                        ));
                    }
                }
            } else {
                let accepted = Accepted::with(n, la);
                self.outgoing.push(Message::with(
                    self.pid,
                    self.leader,
                    PaxosMsg::Accepted(accepted),
                ));
            }
        }
    }

    #[derive(PartialEq, Debug)]
    enum Phase {
        Prepare,
        FirstAccept,
        Accept,
        None,
    }

    #[derive(PartialEq, Debug)]
    enum Role {
        Follower,
        Leader,
    }

    #[derive(Clone, Debug)]
    pub struct StopSign {
        pub config_id: u32,
        pub nodes: Vec<u64>,
        pub skip_prepare_n: Option<Ballot>,
    }

    impl StopSign {
        pub fn with(config_id: u32, nodes: Vec<u64>, skip_prepare_n: Option<Ballot>) -> StopSign {
            StopSign {
                config_id,
                nodes,
                skip_prepare_n,
            }
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
        StopSign(StopSign),
    }

    impl Entry {
        pub(crate) fn is_stopsign(&self) -> bool {
            match self {
                Entry::StopSign(_) => true,
                _ => false,
            }
        }
    }
}

mod ballot_leader_election {
    use super::super::messages::{
        paxos::ballot_leader_election::*, StopMsg as NetStopMsg, StopMsgDeser,
    };
    use super::*;
    use std::time::Duration;

    #[derive(Debug)]
    pub struct Stop(pub Ask<(u64, bool), ()>);

    pub struct BallotLeaderElection;

    impl Port for BallotLeaderElection {
        type Indication = Leader;
        type Request = ();
    }

    #[derive(ComponentDefinition)]
    pub struct BallotLeaderComp {
        // TODO decouple from kompact, similar style to tikv_raft with tick() replacing timers
        ctx: ComponentContext<Self>,
        ble_port: ProvidedPort<BallotLeaderElection>,
        pid: u64,
        peers: Vec<ActorPath>,
        round: u64,
        ballots: Vec<(Ballot, u64)>,
        current_ballot: Ballot, // (round, pid)
        leader: Option<(Ballot, u64)>,
        max_ballot: Ballot,
        hb_delay: u64,
        delta: u64,
        majority: usize,
        timer: Option<ScheduledTimer>,
        stopped: bool,
        stopped_peers: HashSet<u64>,
        stop_ask: Option<Ask<(u64, bool), ()>>,
        quick_timeout: bool,
        initial_election_factor: u64,
    }

    impl BallotLeaderComp {
        pub fn with(
            peers: Vec<ActorPath>,
            pid: u64,
            hb_delay: u64,
            delta: u64,
            quick_timeout: bool,
            initial_max_ballot: Option<Ballot>,
            initial_election_factor: u64,
        ) -> BallotLeaderComp {
            let n = &peers.len() + 1;
            let initial_round = match initial_max_ballot {
                Some(ballot) if ballot.pid == pid => ballot.n,
                _ => 0,
            };
            let initial_ballot = Ballot::with(initial_round, pid);
            BallotLeaderComp {
                ctx: ComponentContext::uninitialised(),
                ble_port: ProvidedPort::uninitialised(),
                pid,
                majority: n / 2 + 1, // +1 because peers is exclusive ourselves
                peers,
                round: initial_round,
                ballots: Vec::with_capacity(n),
                current_ballot: initial_ballot,
                leader: None,
                max_ballot: initial_max_ballot.unwrap_or(initial_ballot),
                hb_delay,
                delta,
                timer: None,
                stopped: false,
                stopped_peers: HashSet::with_capacity(n),
                stop_ask: None,
                quick_timeout,
                initial_election_factor,
            }
        }

        fn check_leader(&mut self) {
            let mut ballots = Vec::with_capacity(self.peers.len());
            std::mem::swap(&mut self.ballots, &mut ballots);
            let (top_ballot, top_pid) = ballots.into_iter().max().unwrap();
            if top_ballot < self.max_ballot {
                // did not get HB from leader
                self.current_ballot.n = self.max_ballot.n + 1;
                self.leader = None;
            } else if self.leader != Some((top_ballot, top_pid)) {
                // got a new leader with greater ballot
                self.quick_timeout = false;
                self.max_ballot = top_ballot;
                self.leader = Some((top_ballot, top_pid));
                self.ble_port.trigger(Leader::with(top_pid, top_ballot));
            }
        }

        fn hb_timeout(&mut self) -> Handled {
            if self.ballots.len() + 1 >= self.majority {
                self.ballots.push((self.current_ballot, self.pid));
                self.check_leader();
            } else {
                self.ballots.clear();
            }
            let delay = if self.quick_timeout {
                // use short timeout if still no first leader
                self.hb_delay / self.initial_election_factor
            } else {
                self.hb_delay
            };
            self.round += 1;
            for peer in &self.peers {
                let hb_request = HeartbeatRequest::with(self.round, self.max_ballot);
                peer.tell_serialised(HeartbeatMsg::Request(hb_request), self)
                    .expect("HBRequest should serialise!");
            }
            self.start_timer(delay);
            Handled::Ok
        }

        fn start_timer(&mut self, t: u64) {
            let timer = self.schedule_once(Duration::from_millis(t), move |c, _| c.hb_timeout());
            self.timer = Some(timer);
        }

        fn stop_timer(&mut self) {
            if let Some(timer) = self.timer.take() {
                self.cancel_timer(timer);
            }
        }

        fn stop_if_pending(&mut self) -> Handled {
            if self.stopped_peers.len() != self.peers.len() {
                self.stop_ask
                    .take()
                    .expect("No stop ask!")
                    .reply(())
                    .expect("Failed to reply to late stop ask");
            }
            Handled::Ok
        }
    }

    impl ComponentLifecycle for BallotLeaderComp {
        fn on_start(&mut self) -> Handled {
            // info!(self.ctx.log(), "Started BLE with params: current_ballot: {:?}, quick timeout: {}, round: {}, max_ballot: {:?}", self.current_ballot, self.quick_timeout, self.round, self.max_ballot);
            let bc = BufferConfig::default();
            self.ctx.borrow().init_buffers(Some(bc), None);
            for peer in &self.peers {
                let hb_request = HeartbeatRequest::with(self.round, self.max_ballot);
                peer.tell_serialised(HeartbeatMsg::Request(hb_request), self)
                    .expect("HBRequest should serialise!");
            }
            let delay = if self.quick_timeout {
                // use short timeout if still no first leader
                self.hb_delay / self.initial_election_factor
            } else {
                self.hb_delay
            };
            self.start_timer(delay);
            Handled::Ok
        }

        fn on_kill(&mut self) -> Handled {
            self.stop_timer();
            Handled::Ok
        }
    }

    impl Provide<BallotLeaderElection> for BallotLeaderComp {
        fn handle(&mut self, _: <BallotLeaderElection as Port>::Request) -> Handled {
            unimplemented!()
        }
    }

    impl Actor for BallotLeaderComp {
        type Message = Stop;

        fn receive_local(&mut self, stop: Stop) -> Handled {
            let (pid, late_stop) = *stop.0.request();
            self.stop_timer();
            for peer in &self.peers {
                peer.tell_serialised(NetStopMsg::Peer(pid), self)
                    .expect("NetStopMsg should serialise!");
            }
            self.stopped = true;
            if self.stopped_peers.len() == self.peers.len() {
                stop.0.reply(()).expect("Failed to reply to stop ask!");
            } else {
                self.stop_ask = Some(stop.0);
                if late_stop {
                    self.schedule_once(Duration::from_secs(3), move |c, _| c.stop_if_pending());
                }
            }
            Handled::Ok
        }

        fn receive_network(&mut self, m: NetMessage) -> Handled {
            let NetMessage { sender, data, .. } = m;
            match_deser! {data; {
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
                        if self.stopped && self.stopped_peers.len() == self.peers.len() {
                            self.stop_ask.take().expect("No stop ask!").reply(()).expect("Failed to reply ask");
                        }
                    }

                },
                !Err(e) => error!(self.ctx.log(), "Error deserialising msg: {:?}", e),
            }
            }
            Handled::Ok
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::client::tests::TestClient;
    use super::super::messages::paxos::ballot_leader_election::Ballot;
    use super::super::messages::Run;
    use super::*;
    use crate::bench::atomic_broadcast::messages::paxos::{Message, PaxosMsg};
    use crate::bench::atomic_broadcast::paxos::raw_paxos::Entry::Normal;
    use crate::partitioning_actor::{IterationControlMsg, PartitioningActor};
    use std::sync::Arc;
    use synchronoise::CountdownEvent;

    fn create_replica_nodes(
        n: u64,
        initial_conf: Vec<u64>,
        policy: ReconfigurationPolicy,
    ) -> (Vec<KompactSystem>, HashMap<u64, ActorPath>, Vec<ActorPath>) {
        let mut systems = Vec::with_capacity(n as usize);
        let mut nodes = HashMap::with_capacity(n as usize);
        let mut actorpaths = Vec::with_capacity(n as usize);
        for i in 1..=n {
            let system = kompact_benchmarks::kompact_system_provider::global()
                .new_remote_system_with_threads(format!("paxos_replica{}", i), 4);
            let (replica_comp, unique_reg_f) = system.create_and_register(|| {
                PaxosComp::<MemorySequence, MemoryState>::with(initial_conf.clone(), policy.clone())
            });
            unique_reg_f.wait_expect(
                Duration::from_millis(1000),
                "ReplicaComp failed to register!",
            );
            let replica_comp_f = system.start_notify(&replica_comp);
            replica_comp_f
                .wait_timeout(Duration::from_millis(1000))
                .expect("ReplicaComp never started!");

            let named_reg_f = system.register_by_alias(&replica_comp, format!("replica{}", i));
            let self_path = named_reg_f.wait_expect(
                Duration::from_secs(1),
                "ReplicaComp failed to register alias",
            );
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
        let config = vec![1, 2, 3];
        let reconfig: Option<(Vec<u64>, Vec<u64>)> = None;
        // let reconfig = Some((vec![1,2,6,7,8], vec![]));
        let n: u64 = match reconfig {
            None => config.len() as u64,
            Some(ref r) => *(r.0.last().unwrap()),
        };
        let check_sequences = true;
        let policy = ReconfigurationPolicy::Pull;
        let active_n = config.len() as u64;
        let quorum = active_n / 2 + 1;

        let (systems, nodes, actorpaths) = create_replica_nodes(n, config, policy);
        /*** Setup client ***/
        let (p, f) = kpromise::<HashMap<u64, Vec<u64>>>();
        let (client_comp, unique_reg_f) = systems[0].create_and_register(|| {
            TestClient::with(
                num_proposals,
                batch_size,
                nodes,
                reconfig.clone(),
                p,
                check_sequences,
            )
        });
        unique_reg_f.wait_expect(Duration::from_millis(1000), "Client failed to register!");
        let system = systems.first().unwrap();
        let client_comp_f = system.start_notify(&client_comp);
        client_comp_f
            .wait_timeout(Duration::from_secs(2))
            .expect("ClientComp never started!");
        let named_reg_f = system.register_by_alias(&client_comp, "client");
        let client_path = named_reg_f.wait_expect(
            Duration::from_secs(2),
            "Failed to register alias for ClientComp",
        );
        let mut ser_client = Vec::<u8>::new();
        client_path
            .serialise(&mut ser_client)
            .expect("Failed to serialise ClientComp actorpath");
        /*** Setup partitioning actor ***/
        let prepare_latch = Arc::new(CountdownEvent::new(1));
        let (partitioning_actor, unique_reg_f) = systems[0].create_and_register(|| {
            PartitioningActor::with(prepare_latch.clone(), None, 1, actorpaths, None)
        });
        unique_reg_f.wait_expect(
            Duration::from_millis(1000),
            "PartitioningComp failed to register!",
        );

        let partitioning_actor_f = systems[0].start_notify(&partitioning_actor);
        partitioning_actor_f
            .wait_timeout(Duration::from_millis(1000))
            .expect("PartitioningComp never started!");
        partitioning_actor
            .actor_ref()
            .tell(IterationControlMsg::Prepare(Some(ser_client)));
        prepare_latch.wait();
        partitioning_actor
            .actor_ref()
            .tell(IterationControlMsg::Run);
        client_comp.actor_ref().tell(Run);
        let all_sequences = f
            .wait_timeout(Duration::from_secs(60))
            .expect("Failed to get results");
        let client_sequence = all_sequences
            .get(&0)
            .expect("Client's sequence should be in 0...")
            .to_owned();
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
        if check_sequences {
            let mut counter = 0;
            for i in 1..=n {
                let sequence = all_sequences
                    .get(&i)
                    .unwrap_or_else(|| panic!("Did not get sequence for node {}", i));
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
