use super::{
    atomic_broadcast::ExperimentParams,
    communicator::{AtomicBroadcastCompMsg, CommunicationPort, Communicator, CommunicatorMsg},
    messages::{
        paxos::{
            Reconfig, ReconfigInit, ReconfigSer, ReconfigurationMsg, SequenceMetaData,
            SequenceRequest, SequenceSegment, SequenceTransfer,
        },
        StopMsg as NetStopMsg, *,
    },
};
#[cfg(test)]
use crate::bench::atomic_broadcast::atomic_broadcast::tests::SequenceResp;
use crate::{
    bench::atomic_broadcast::{atomic_broadcast::Done, paxos::ballot_leader_election::Ballot},
    partitioning_actor::{PartitioningActorMsg, PartitioningActorSer},
    serialiser_ids::ATOMICBCAST_ID,
};
use ballot_leader_election::{BallotLeaderComp, BallotLeaderElection, Stop as BLEStop};
use hashbrown::{HashMap, HashSet};
use kompact::prelude::*;
// use kompact::KompactLogger;
use rand::Rng;
use std::{borrow::Borrow, fmt::Debug, ops::DerefMut, sync::Arc, time::Duration};

use leaderpaxos::{leader_election::*, paxos::*, storage::*};

const BLE: &str = "ble";
const COMMUNICATOR: &str = "communicator";

#[derive(Debug)]
pub struct FinalMsg<S>
where
    S: SequenceTraits<Ballot>,
{
    pub config_id: u32,
    pub nodes: Reconfig,
    pub final_sequence: Arc<S>,
    pub skip_prepare_use_leader: Option<Leader<Ballot>>,
}

impl<S> FinalMsg<S>
where
    S: SequenceTraits<Ballot>,
{
    pub fn with(
        config_id: u32,
        nodes: Reconfig,
        final_sequence: Arc<S>,
        skip_prepare_use_leader: Option<Leader<Ballot>>,
    ) -> FinalMsg<S> {
        FinalMsg {
            config_id,
            nodes,
            final_sequence,
            skip_prepare_use_leader,
        }
    }
}

#[derive(Debug)]
pub enum PaxosReplicaMsg {
    Propose(Proposal),
    LocalSequenceReq(ActorPath, SequenceRequest, SequenceMetaData),
    Stop(Ask<bool, ()>), // ack_client
    #[cfg(test)]
    SequenceReq(Ask<(), Vec<Entry<Ballot>>>),
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
    S: SequenceTraits<Ballot>,
    P: StateTraits<Ballot>,
{
    ctx: ComponentContext<Self>,
    pid: u64,
    experiment_configurations: (Vec<u64>, Vec<u64>),
    paxos_replicas: Vec<Arc<Component<PaxosReplica<S, P>>>>,
    ble_comps: Vec<Arc<Component<BallotLeaderComp>>>,
    communicator_comps: Vec<Arc<Component<Communicator>>>,
    active_config: ConfigMeta,
    nodes: Vec<ActorPath>, // derive actorpaths of peers' ble and paxos replicas from these
    prev_sequences: HashMap<u32, Arc<S>>, // TODO vec
    stopped: bool,
    iteration_id: u32,
    policy: ReconfigurationPolicy,
    next_config_id: Option<u32>,
    pending_seq_transfers: Vec<(Vec<u32>, Vec<Entry<Ballot>>)>, // (remaining_segments, entries)
    complete_sequences: Vec<u32>,
    active_peers: (Vec<u64>, Vec<u64>), // (ready, not_ready)
    retry_transfer_timers: HashMap<u32, ScheduledTimer>,
    cached_client: Option<ActorPath>,
    hb_proposals: Vec<NetMessage>,
    experiment_params: ExperimentParams,
    first_config_id: u32, // used to keep track of which idx in paxos_replicas corresponds to which config_id
}

impl<S, P> PaxosComp<S, P>
where
    S: SequenceTraits<Ballot>,
    P: StateTraits<Ballot>,
{
    pub fn with(
        experiment_configurations: (Vec<u64>, Vec<u64>),
        policy: ReconfigurationPolicy,
        experiment_params: ExperimentParams,
    ) -> PaxosComp<S, P> {
        PaxosComp {
            ctx: ComponentContext::uninitialised(),
            pid: 0,
            experiment_configurations,
            paxos_replicas: vec![],
            ble_comps: vec![],
            communicator_comps: vec![],
            active_config: ConfigMeta::new(0),
            nodes: vec![],
            prev_sequences: HashMap::new(),
            stopped: false,
            iteration_id: 0,
            policy,
            next_config_id: None,
            pending_seq_transfers: vec![],
            complete_sequences: vec![],
            active_peers: (vec![], vec![]),
            retry_transfer_timers: HashMap::new(),
            cached_client: None,
            hb_proposals: vec![],
            experiment_params,
            first_config_id: 0,
        }
    }

    fn derive_actorpaths(
        &self,
        config_id: u32,
        peers: &[u64],
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
        ble_quick_start: bool,
        skip_prepare_n: Option<Leader<Ballot>>,
    ) -> Vec<KFuture<RegistrationResult>> {
        let mut peers = nodes;
        peers.retain(|pid| pid != &self.pid);
        let (ble_peers, communicator_peers) = self.derive_actorpaths(config_id, &peers);
        let system = self.ctx.system();
        /*** create and register Paxos ***/
        // let log: KompactLogger = self.ctx.log().new(o!("raw_paxos" => self.pid));
        let max_inflight = self.experiment_params.max_inflight;
        let (paxos, paxos_f) = system.create_and_register(|| {
            PaxosReplica::with(
                self.ctx.actor_ref(),
                peers,
                config_id,
                self.pid,
                // log,
                skip_prepare_n.clone(),
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
        let election_timeout = self.experiment_params.election_timeout;
        let config = self.ctx.config();
        let ble_delta = config["paxos"]["ble_delta"]
            .as_i64()
            .expect("Failed to load get_decided_period");
        let initial_election_factor = self.experiment_params.initial_election_factor;
        let (ble_comp, ble_f) = system.create_and_register(|| {
            BallotLeaderComp::with(
                ble_peers,
                self.pid,
                election_timeout as u64,
                ble_delta as u64,
                ble_quick_start,
                skip_prepare_n,
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

        self.paxos_replicas.push(paxos);
        self.ble_comps.push(ble_comp);
        self.communicator_comps.push(communicator);

        vec![paxos_f, ble_f, comm_f, comm_alias_f, ble_alias_f]
    }

    fn set_initial_replica_state_after_reconfig(
        &mut self,
        config_id: u32,
        skip_prepare_use_leader: Option<Leader<Ballot>>,
    ) {
        let idx = (config_id - self.first_config_id) as usize;
        let paxos = self.paxos_replicas.get(idx).unwrap_or_else(|| {
            panic!(
                "No replica to set with idx: {}, replicas len: {}",
                idx,
                self.paxos_replicas.len()
            )
        });
        paxos.on_definition(|p| {
            let peers = p.peers.clone();
            let paxos_state = P::new();
            // let raw_paxos_log: KompactLogger = self.ctx.log().new(o!("raw_paxos" => self.pid));
            let max_inflight = self.experiment_params.max_inflight;
            let seq = S::new_with_sequence(Vec::with_capacity(max_inflight));
            let storage = Storage::with(seq, paxos_state);
            p.paxos = Paxos::with(
                config_id,
                self.pid,
                peers,
                storage,
                // raw_paxos_log,
                skip_prepare_use_leader,
                Some(max_inflight),
            );
        });
        if let Some(n) = skip_prepare_use_leader {
            let ble = self
                .ble_comps
                .get(idx)
                .unwrap_or_else(|| panic!("Could not find BLE config_id: {}", config_id));
            ble.on_definition(|ble| {
                ble.set_initial_leader(n); // TODO Only new leader should call this
            });
        }
    }

    fn start_replica(&mut self, config_id: u32) {
        let idx = (config_id - self.first_config_id) as usize;
        let paxos = self.paxos_replicas.get(idx).unwrap_or_else(|| {
            panic!(
                "No replica to start with idx: {}, replicas len: {}",
                idx,
                self.paxos_replicas.len()
            )
        });
        info!(
            self.ctx.log(),
            "Starting replica pid: {}, config_id: {}", self.pid, config_id
        );
        self.active_config = ConfigMeta::new(config_id);
        let ble = self
            .ble_comps
            .get(idx)
            .unwrap_or_else(|| panic!("Could not find BLE config_id: {}", config_id));
        let communicator = self
            .communicator_comps
            .get(idx)
            .unwrap_or_else(|| panic!("Could not find Communicator with config_id: {}", config_id));
        self.ctx.system().start(paxos);
        self.ctx.system().start(ble);
        self.ctx.system().start(communicator);
        self.next_config_id = None;
    }

    fn stop_components(&mut self) -> Handled {
        self.stopped = true;
        let retry_timers = std::mem::take(&mut self.retry_transfer_timers);
        for (_, timer) in retry_timers {
            self.cancel_timer(timer);
        }
        let num_configs = self.paxos_replicas.len() as u32;
        let stopping_before_started = self.active_config.id < num_configs;
        let num_comps =
            self.ble_comps.len() + self.paxos_replicas.len() + self.communicator_comps.len();
        assert!(
            num_comps > 0,
            "Should not get client stop if no child components"
        );
        let mut stop_futures = Vec::with_capacity(num_comps);
        debug!(
            self.ctx.log(),
            "Stopping {} child components... next_config: {:?}, late_stop: {}",
            num_comps,
            self.next_config_id,
            stopping_before_started
        );
        let (ble_last, rest_ble) = self.ble_comps.split_last().expect("No ble comps!");
        for ble in rest_ble {
            stop_futures.push(ble.actor_ref().ask_with(|p| BLEStop(Ask::new(p, self.pid))));
        }
        stop_futures.push(
            ble_last
                .actor_ref()
                .ask_with(|p| BLEStop(Ask::new(p, self.pid))),
        );

        let (paxos_last, rest) = self
            .paxos_replicas
            .split_last()
            .expect("No paxos replicas!");
        for paxos_replica in rest {
            stop_futures.push(
                paxos_replica
                    .actor_ref()
                    .ask_with(|p| PaxosReplicaMsg::Stop(Ask::new(p, false))),
            );
        }
        stop_futures.push(
            paxos_last
                .actor_ref()
                .ask_with(|p| PaxosReplicaMsg::Stop(Ask::new(p, true))), // last replica should respond to client
        );

        if stopping_before_started {
            // experiment was finished before replica even started
            let unstarted_replica = if self.active_config.id == 0 {
                self.first_config_id
            } else {
                num_configs
            };
            self.start_replica(unstarted_replica);
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

    fn deserialise_and_propose(&self, m: NetMessage) {
        if let AtomicBroadcastMsg::Proposal(p) = m
            .try_deserialise_unchecked::<AtomicBroadcastMsg, AtomicBroadcastDeser>()
            .expect("Should be AtomicBroadcastMsg!")
        {
            let idx = (self.active_config.id - self.first_config_id) as usize;
            let active_paxos = self
                .paxos_replicas
                .get(idx)
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
                    debug!(
                        self.ctx.log(),
                        "Retrying timed out seq transfer: tag: {}, idx: {}-{}, policy: {:?}",
                        tag,
                        from_idx,
                        to_idx,
                        self.policy
                    );
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
                    let idx = (self.active_config.id - self.first_config_id) as usize;
                    let paxos = self.paxos_replicas.get(idx).unwrap_or_else(|| panic!("No paxos replica with idx: {} when handling SequenceRequest. Len of PaxosReplicas: {}", idx, self.paxos_replicas.len()));
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
        debug!(
            self.ctx.log(),
            "Replying seq transfer: tag: {}, idx: {}-{}",
            st.tag,
            st.segment.from_idx,
            st.segment.to_idx
        );
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
                let config_id = self
                    .next_config_id
                    .expect("Got all sequence transfer but no next config id!");
                if self.complete_sequences.len() + 1 == config_id as usize {
                    // got all sequence transfers
                    self.complete_sequences.clear();
                    debug!(self.ctx.log(), "Got all previous sequences!");
                    self.start_replica(config_id);
                }
            }
        } else {
            // failed sequence transfer i.e. not reached final seq yet
            let config_id = st.config_id;
            let tag = st.tag;
            let from_idx = st.segment.from_idx;
            let to_idx = st.segment.to_idx;
            debug!(
                self.ctx.log(),
                "Got failed seq transfer: tag: {}, idx: {}-{}", tag, from_idx, to_idx
            );
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

    fn reset_state(&mut self) {
        self.stopped = false;
        self.active_config = ConfigMeta::new(0);
        self.active_peers = (vec![], vec![]);
        self.complete_sequences.clear();
        self.first_config_id = 0;
        self.pending_seq_transfers.clear();
        self.prev_sequences.clear();
        self.hb_proposals.clear();
        self.next_config_id = None;
    }
}

#[derive(Debug)]
pub enum PaxosCompMsg<S>
where
    S: SequenceTraits<Ballot>,
{
    Leader(u32, u64),
    PendingReconfig(Vec<u8>),
    Reconfig(FinalMsg<S>),
    KillComponents(Ask<(), Done>),
    #[cfg(test)]
    GetSequence(Ask<(), SequenceResp>),
}

impl<S, P> ComponentLifecycle for PaxosComp<S, P>
where
    S: SequenceTraits<Ballot>,
    P: StateTraits<Ballot>,
{
}

impl<S, P> Actor for PaxosComp<S, P>
where
    S: SequenceTraits<Ballot>,
    P: StateTraits<Ballot>,
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
                    r.skip_prepare_use_leader,
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
                let continued_nodes = r.nodes.continued_nodes;
                let new_nodes = r.nodes.new_nodes;
                if continued_nodes.contains(&self.pid) {
                    if let ReconfigurationPolicy::Eager = self.policy {
                        let st =
                            self.create_eager_sequence_transfer(&continued_nodes, prev_config_id);
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
                    self.set_initial_replica_state_after_reconfig(
                        r.config_id,
                        r.skip_prepare_use_leader,
                    );
                    self.start_replica(r.config_id);
                }
            }
            PaxosCompMsg::KillComponents(ask) => {
                let handled = self.kill_components(ask);
                return handled;
            }
            #[cfg(test)]
            PaxosCompMsg::GetSequence(ask) => {
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
                    let sequence = active_paxos
                        .actor_ref()
                        .ask_with(|promise| PaxosReplicaMsg::SequenceReq(Ask::new(promise, ())))
                        .wait();
                    for entry in sequence {
                        if let Entry::Normal(n) = entry {
                            let id = n.as_slice().get_u64();
                            all_entries.push(id);
                            unique.insert(id);
                        }
                    }
                    let min = unique.iter().min();
                    let max = unique.iter().max();
                    debug!(
                        self.ctx.log(),
                        "Got SequenceReq: my seq_len: {}, unique: {}, min: {:?}, max: {:?}",
                        all_entries.len(),
                        unique.len(),
                        min,
                        max
                    );
                } else {
                    warn!(
                        self.ctx.log(),
                        "Got SequenceReq but no active paxos: {}", self.active_config.id
                    );
                }
                let sr = SequenceResp::with(self.pid, all_entries);
                ask.reply(sr).expect("Failed to reply SequenceResp");
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
                match_deser! {data {
                    msg(p): PartitioningActorMsg [using PartitioningActorSer] => {
                        match p {
                            PartitioningActorMsg::Init(init) => {
                                self.reset_state();
                                self.nodes = init.nodes;
                                self.pid = init.pid as u64;
                                self.iteration_id = init.init_id;
                                let ser_client = init
                                    .init_data
                                    .expect("Init should include ClientComp's actorpath");
                                let client = ActorPath::deserialise(&mut ser_client.as_slice())
                                    .expect("Failed to deserialise Client's actorpath");
                                self.cached_client = Some(client);

                                let (initial_configuration, reconfiguration) = self.experiment_configurations.clone();
                                let handled = Handled::block_on(self, move |mut async_self| async move {
                                    if initial_configuration.contains(&async_self.pid) {
                                        async_self.next_config_id = Some(1);
                                        let futures = async_self.create_replica(1, initial_configuration, true, None);
                                        for f in futures {
                                            f.await.unwrap().expect("Failed to register when creating replica 1");
                                        }
                                        async_self.first_config_id = 1;
                                    }
                                    if reconfiguration.contains(&async_self.pid) {
                                        let futures = async_self.create_replica(2, reconfiguration, false, None);
                                        for f in futures {
                                            f.await.unwrap().expect("Failed to register when creating replica 2");
                                        }
                                        if async_self.first_config_id != 1 {    // not in initial configuration
                                            async_self.first_config_id = 2;
                                        }
                                    }
                                    let resp = PartitioningActorMsg::InitAck(async_self.iteration_id);
                                    sender.tell_serialised(resp, async_self.deref_mut())
                                        .expect("Should serialise");
                                });
                                return handled;
                            },
                            PartitioningActorMsg::Run => {
                                if let Some(config_id) = self.next_config_id {
                                    self.start_replica(config_id);
                                }
                            },
                            _ => unimplemented!()
                        }
                    },
                    msg(rm): ReconfigurationMsg [using ReconfigSer] => {
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
                                        debug!(self.ctx.log(), "Got ReconfigInit for config_id: {} from node {}", r.config_id, r.from);
                                        self.next_config_id = Some(r.config_id);
                                        self.set_initial_replica_state_after_reconfig(r.config_id, r.skip_prepare_use_leader);
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
                                        if r.seq_metadata.len == 1 && r.seq_metadata.config_id == 1 {
                                            // only SS in final sequence and no other prev sequences -> start directly
                                            let final_sequence = S::new_with_sequence(vec![]);
                                            self.prev_sequences.insert(r.seq_metadata.config_id, Arc::new(final_sequence));
                                            self.start_replica(r.config_id);
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
                                                    let idx = (config_id - 1) as usize;
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
                    msg(client_stop): NetStopMsg [using StopMsgDeser] => {
                        if let NetStopMsg::Client = client_stop {
                            return self.stop_components();
                        }
                    },
                    err(e) => error!(self.ctx.log(), "Error deserialising msg: {:?}", e),
                    default(_) => unimplemented!("Expected either PartitioningActorMsg, ReconfigurationMsg or NetStopMsg!"),
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
    S: SequenceTraits<Ballot>,
    P: StateTraits<Ballot>,
{
    ctx: ComponentContext<Self>,
    supervisor: ActorRef<PaxosCompMsg<S>>,
    communication_port: RequiredPort<CommunicationPort>,
    ble_port: RequiredPort<BallotLeaderElection>,
    peers: Vec<u64>,
    paxos: Paxos<Ballot, S, P>,
    config_id: u32,
    pid: u64,
    current_leader: u64,
    timers: Option<(ScheduledTimer, ScheduledTimer)>,
    pending_reconfig: bool,
    stopped: bool,
    stopped_peers: HashSet<u64>,
    stop_ask: Option<Ask<bool, ()>>,
}

impl<S, P> PaxosReplica<S, P>
where
    S: SequenceTraits<Ballot>,
    P: StateTraits<Ballot>,
{
    fn with(
        supervisor: ActorRef<PaxosCompMsg<S>>,
        peers: Vec<u64>,
        config_id: u32,
        pid: u64,
        // raw_paxos_log: KompactLogger,
        skip_prepare_use_leader: Option<Leader<Ballot>>,
        max_inflight: usize,
    ) -> PaxosReplica<S, P> {
        let seq = S::new_with_sequence(Vec::with_capacity(max_inflight));
        let paxos_state = P::new();
        let storage = Storage::with(seq, paxos_state);
        let paxos = Paxos::with(
            config_id,
            pid,
            peers.clone(),
            storage,
            // raw_paxos_log,
            skip_prepare_use_leader,
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
        let decided_timer =
            self.schedule_periodic(Duration::from_millis(1), get_decided_period, move |c, _| {
                c.get_decided()
            });
        let outgoing_timer =
            self.schedule_periodic(Duration::from_millis(0), outgoing_period, move |p, _| {
                p.send_outgoing()
            });
        self.timers = Some((decided_timer, outgoing_timer));
    }

    fn stop_timers(&mut self) {
        if let Some(timers) = self.timers.take() {
            self.cancel_timer(timers.0);
            self.cancel_timer(timers.1);
        }
    }

    fn send_outgoing(&mut self) -> Handled {
        for out_msg in self.paxos.get_outgoing_msgs() {
            self.communication_port
                .trigger(CommunicatorMsg::RawPaxosMsg(out_msg));
        }
        Handled::Ok
    }

    fn handle_stopsign(&mut self, ss: &StopSign<Ballot>) {
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
        let r = FinalMsg::with(ss.config_id, nodes, final_seq, ss.skip_prepare_use_leader);
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
            // Only leader responds to client. This is fine for benchmarking. In real-life, each replica probably caches clients and responds to them.
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

    fn propose(&mut self, p: Proposal) -> Result<(), ProposeErr> {
        match p.reconfig {
            Some((reconfig, _)) => {
                let n = self.ctx.config()["paxos"]["prio_start_round"]
                    .as_i64()
                    .expect("No prio start round in config!") as u64;
                let prio_start_round = Ballot::with(n, 0);
                self.paxos
                    .propose_reconfiguration(reconfig, Some(prio_start_round))
            }
            None => self.paxos.propose_normal(p.data),
        }
    }
}

impl<S, P> Actor for PaxosReplica<S, P>
where
    S: SequenceTraits<Ballot>,
    P: StateTraits<Ballot>,
{
    type Message = PaxosReplicaMsg;

    fn receive_local(&mut self, msg: PaxosReplicaMsg) -> Handled {
        match msg {
            PaxosReplicaMsg::Propose(p) => {
                if !self.pending_reconfig {
                    if let Err(propose_err) = self.propose(p) {
                        match propose_err {
                            ProposeErr::Normal(data) => {
                                self.pending_reconfig = true;
                                self.supervisor.tell(PaxosCompMsg::PendingReconfig(data))
                            }
                            ProposeErr::Reconfiguration(new_config) => {
                                panic!("Failed to propose new config: {:?}", new_config)
                            }
                        }
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
            PaxosReplicaMsg::Stop(ask) => {
                let ack_client = *ask.request();
                self.communication_port
                    .trigger(CommunicatorMsg::SendStop(self.pid, ack_client));
                self.stop_timers();
                self.stopped = true;
                if self.stopped_peers.len() == self.peers.len() {
                    ask.reply(()).expect("Failed to reply stop ask");
                } else {
                    // have not got stop from all peers yet
                    self.stop_ask = Some(ask);
                }
            }
            #[cfg(test)]
            PaxosReplicaMsg::SequenceReq(a) => {
                // for testing only
                let seq = self.paxos.get_sequence();
                a.reply(seq).expect("Failed to reply to GetAllEntries");
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
    S: SequenceTraits<Ballot>,
    P: StateTraits<Ballot>,
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
    S: SequenceTraits<Ballot>,
    P: StateTraits<Ballot>,
{
    fn handle(&mut self, msg: <CommunicationPort as Port>::Indication) -> Handled {
        match msg {
            AtomicBroadcastCompMsg::RawPaxosMsg(pm) if !self.stopped => {
                self.paxos.handle(pm);
            }
            AtomicBroadcastCompMsg::StopMsg(pid) => {
                assert!(
                    self.stopped_peers.insert(pid),
                    "Paxos replica {} got duplicate stop from peer {}",
                    self.config_id,
                    pid
                );
                debug!(
                    self.ctx.log(),
                    "PaxosReplica {} got stopped from peer {}", self.config_id, pid
                );
                if self.stopped && self.stopped_peers.len() == self.peers.len() {
                    debug!(
                        self.ctx.log(),
                        "PaxosReplica {} got stopped from all peers", self.config_id
                    );
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
    S: SequenceTraits<Ballot>,
    P: StateTraits<Ballot>,
{
    fn handle(&mut self, l: Leader<Ballot>) -> Handled {
        debug!(
            self.ctx.log(),
            "Node {} became leader in config {}. Ballot: {:?}", l.pid, self.config_id, l.round
        );
        self.paxos.handle_leader(l);
        if self.current_leader != l.pid && !self.paxos.stopped() {
            self.current_leader = l.pid;
            self.supervisor
                .tell(PaxosCompMsg::Leader(self.config_id, l.pid));
        }
        Handled::Ok
    }
}

pub(crate) mod ballot_leader_election {
    use super::{
        super::messages::{paxos::ballot_leader_election::*, StopMsg as NetStopMsg, StopMsgDeser},
        *,
    };
    use leaderpaxos::leader_election::Round;
    use std::time::Duration;

    #[derive(Clone, Copy, Eq, Debug, Default, Ord, PartialOrd, PartialEq)]
    pub struct Ballot {
        pub n: u64,
        pub pid: u64,
    }

    impl Ballot {
        pub fn with(n: u64, pid: u64) -> Ballot {
            Ballot { n, pid }
        }
    }

    impl Round for Ballot {}

    #[derive(Debug)]
    pub struct Stop(pub Ask<u64, ()>); // pid

    pub struct BallotLeaderElection;

    impl Port for BallotLeaderElection {
        type Indication = Leader<Ballot>;
        type Request = ();
    }

    #[derive(ComponentDefinition)]
    pub struct BallotLeaderComp {
        // TODO decouple from kompact, similar style to tikv_raft with tick() replacing timers
        ctx: ComponentContext<Self>,
        ble_port: ProvidedPort<BallotLeaderElection>,
        pid: u64,
        peers: Vec<ActorPath>,
        hb_round: u64,
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
        stop_ask: Option<Ask<u64, ()>>,
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
            initial_leader: Option<Leader<Ballot>>,
            initial_election_factor: u64,
        ) -> BallotLeaderComp {
            let n = &peers.len() + 1;
            let (leader, initial_ballot, max_ballot) = match initial_leader {
                Some(l) => {
                    let leader = Some((l.round, l.pid));
                    let initial_ballot = if l.pid == pid {
                        l.round
                    } else {
                        Ballot::with(0, pid)
                    };
                    (leader, initial_ballot, l.round)
                }
                None => {
                    let initial_ballot = Ballot::with(0, pid);
                    (None, initial_ballot, initial_ballot)
                }
            };
            BallotLeaderComp {
                ctx: ComponentContext::uninitialised(),
                ble_port: ProvidedPort::uninitialised(),
                pid,
                majority: n / 2 + 1, // +1 because peers is exclusive ourselves
                peers,
                hb_round: 0,
                ballots: Vec::with_capacity(n),
                current_ballot: initial_ballot,
                leader,
                max_ballot,
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

        /// Sets initial state after creation. Should only be used before being started.
        pub fn set_initial_leader(&mut self, l: Leader<Ballot>) {
            // TODO make sure not already started
            self.leader = Some((l.round, l.pid));
            self.current_ballot = if l.pid == self.pid {
                l.round
            } else {
                Ballot::with(0, self.pid)
            };
            self.max_ballot = l.round;
            self.quick_timeout = false;
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
            self.hb_round += 1;
            for peer in &self.peers {
                let hb_request = HeartbeatRequest::with(self.hb_round, self.max_ballot);
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
    }

    impl ComponentLifecycle for BallotLeaderComp {
        fn on_start(&mut self) -> Handled {
            debug!(self.ctx.log(), "Started BLE with params: current_ballot: {:?}, quick timeout: {}, hb_round: {}, max_ballot: {:?}", self.current_ballot, self.quick_timeout, self.hb_round, self.max_ballot);
            let bc = BufferConfig::default();
            self.ctx.borrow().init_buffers(Some(bc), None);
            for peer in &self.peers {
                let hb_request = HeartbeatRequest::with(self.hb_round, self.max_ballot);
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
            let pid = *stop.0.request();
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
            }
            Handled::Ok
        }

        fn receive_network(&mut self, m: NetMessage) -> Handled {
            let NetMessage { sender, data, .. } = m;
            match_deser! {data {
                msg(hb): HeartbeatMsg [using BallotLeaderSer] => {
                    match hb {
                        HeartbeatMsg::Request(req) if !self.stopped => {
                            if req.max_ballot > self.max_ballot {
                                self.max_ballot = req.max_ballot;
                            }
                            let hb_reply = HeartbeatReply::with(self.pid, req.round, self.current_ballot);
                            sender.tell_serialised(HeartbeatMsg::Reply(hb_reply), self).expect("HBReply should serialise!");
                        },
                        HeartbeatMsg::Reply(rep) if !self.stopped => {
                            if rep.round == self.hb_round {
                                self.ballots.push((rep.max_ballot, rep.sender_pid));
                            } else {
                                trace!(self.ctx.log(), "Got late hb reply. HB delay: {}", self.hb_delay);
                                self.hb_delay += self.delta;
                            }
                        },
                        _ => {},
                    }
                },
                msg(stop): NetStopMsg [using StopMsgDeser] => {
                    if let NetStopMsg::Peer(pid) = stop {
                        assert!(self.stopped_peers.insert(pid), "BLE got duplicate stop from peer {}", pid);
                        debug!(self.ctx.log(), "BLE got stopped from peer {}", pid);
                        if self.stopped && self.stopped_peers.len() == self.peers.len() {
                            debug!(self.ctx.log(), "BLE got stopped from all peers");
                            self.stop_ask
                                .take()
                                .expect("No stop ask!")
                                .reply(())
                                .expect("Failed to reply ask");
                        }
                    }

                },
                err(e) => error!(self.ctx.log(), "Error deserialising msg: {:?}", e),
                default(_) => unimplemented!("Should be either HeartbeatMsg or NetStopMsg!"),
            }
            }
            Handled::Ok
        }
    }
}
