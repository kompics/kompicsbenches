use super::{
    atomic_broadcast::ExperimentParams,
    communicator::{AtomicBroadcastCompMsg, CommunicationPort, Communicator, CommunicatorMsg},
    messages::{
        paxos::{
            Reconfig, ReconfigInit, ReconfigSer, ReconfigurationMsg, SegmentRequest,
            SegmentTransfer, SequenceMetaData, SequenceSegment,
        },
        StopMsg as NetStopMsg, *,
    },
};
#[cfg(test)]
use crate::bench::atomic_broadcast::atomic_broadcast::tests::SequenceResp;
use crate::{
    bench::atomic_broadcast::{
        atomic_broadcast::Done,
        ble::{Ballot, BallotLeaderComp, BallotLeaderElection, Stop as BLEStop},
    },
    partitioning_actor::{PartitioningActorMsg, PartitioningActorSer},
    serialiser_ids::ATOMICBCAST_ID,
};
use hashbrown::{HashMap, HashSet};
use kompact::prelude::*;
// use kompact::KompactLogger;
use rand::Rng;
use std::{fmt::Debug, ops::DerefMut, sync::Arc, time::Duration};

use crate::bench::atomic_broadcast::messages::paxos::SegmentIndex;
use leaderpaxos::{leader_election::*, paxos::*, storage::*};

use crate::bench::atomic_broadcast::client::create_raw_proposal;
#[cfg(feature = "measure_io")]
use crate::bench::atomic_broadcast::{util::exp_params::*, util::io_metadata::IOMetaData};
#[cfg(feature = "measure_io")]
use quanta::{Clock, Instant};

#[cfg(feature = "periodic_replica_logging")]
use crate::bench::atomic_broadcast::util::exp_params::WINDOW_DURATION;
#[cfg(feature = "measure_io")]
use std::io::Write;
#[cfg(feature = "measure_io")]
use std::sync::Mutex;

use crate::bench::atomic_broadcast::util::io_metadata::LogIOMetaData;

const BLE: &str = "ble";
const COMMUNICATOR: &str = "communicator";

type ConfigId = u32;

#[derive(Debug)]
pub struct FinalMsg<S>
where
    S: SequenceTraits<Ballot>,
{
    pub config_id: ConfigId,
    pub nodes: Reconfig,
    pub final_sequence: Arc<S>,
    pub skip_prepare_use_leader: Option<Leader<Ballot>>,
}

impl<S> FinalMsg<S>
where
    S: SequenceTraits<Ballot>,
{
    pub fn with(
        config_id: ConfigId,
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
    ProposeReconfiguration(ReconfigurationProposal),
    LocalSegmentReq(ActorPath, SegmentRequest, SequenceMetaData),
    Stop(Ask<bool, ()>), // ack_client
    #[cfg(test)]
    SequenceReq(Ask<(), Vec<Entry<Ballot>>>),
}

#[derive(Clone, Debug)]
struct ConfigMeta {
    id: ConfigId,
    leader: u64,
    pending_reconfig: bool,
}

impl ConfigMeta {
    fn new(id: ConfigId) -> Self {
        ConfigMeta {
            id,
            leader: 0,
            pending_reconfig: false,
        }
    }
}

#[derive(Default)]
struct HoldBackProposals {
    pub deserialised: Vec<Proposal>,
    pub serialised: Vec<NetMessage>,
}

impl HoldBackProposals {
    fn is_empty(&self) -> bool {
        self.serialised.is_empty() && self.deserialised.is_empty()
    }

    fn clear(&mut self) {
        self.serialised.clear();
        self.deserialised.clear();
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
    initial_configuration: Vec<u64>,
    is_reconfig_exp: bool,
    paxos_replicas: Vec<Arc<Component<PaxosReplica<S, P>>>>,
    ble_comps: Vec<Arc<Component<BallotLeaderComp>>>,
    communicator_comps: Vec<Arc<Component<Communicator>>>,
    active_config: ConfigMeta,
    nodes: Vec<ActorPath>, // derive actorpaths of peers' ble and paxos replicas from these
    prev_sequences: HashMap<ConfigId, Arc<S>>, // TODO vec
    stopped: bool,
    iteration_id: u32,
    next_config_id: Option<ConfigId>,
    pending_segments: HashMap<ConfigId, Vec<SegmentIndex>>,
    received_segments: HashMap<ConfigId, Vec<SequenceSegment>>,
    active_config_ready_peers: Vec<u64>,
    handled_seq_requests: Vec<SegmentRequest>,
    cached_client: Option<ActorPath>,
    hb_proposals: HoldBackProposals,
    experiment_params: ExperimentParams,
    first_config_id: ConfigId, // used to keep track of which idx in paxos_replicas corresponds to which config_id
    removed: bool,
    #[cfg(feature = "measure_io")]
    clock: Clock,
    #[cfg(feature = "measure_io")]
    io_metadata: IOMetaData,
    #[cfg(feature = "measure_io")]
    io_timer: Option<ScheduledTimer>,
    #[cfg(feature = "measure_io")]
    io_windows: Vec<(Instant, IOMetaData)>,
}

impl<S, P> PaxosComp<S, P>
where
    S: SequenceTraits<Ballot>,
    P: StateTraits<Ballot>,
{
    pub fn with(
        initial_configuration: Vec<u64>,
        is_reconfig_exp: bool,
        experiment_params: ExperimentParams,
    ) -> PaxosComp<S, P> {
        PaxosComp {
            ctx: ComponentContext::uninitialised(),
            pid: 0,
            initial_configuration,
            is_reconfig_exp,
            paxos_replicas: vec![],
            ble_comps: vec![],
            communicator_comps: vec![],
            active_config: ConfigMeta::new(0),
            nodes: vec![],
            prev_sequences: HashMap::new(),
            stopped: false,
            iteration_id: 0,
            next_config_id: None,
            pending_segments: HashMap::new(),
            received_segments: HashMap::new(),
            active_config_ready_peers: vec![],
            handled_seq_requests: vec![],
            cached_client: None,
            hb_proposals: HoldBackProposals::default(),
            experiment_params,
            first_config_id: 0,
            removed: false,
            #[cfg(feature = "measure_io")]
            clock: Clock::new(),
            #[cfg(feature = "measure_io")]
            io_metadata: IOMetaData::default(),
            #[cfg(feature = "measure_io")]
            io_timer: None,
            #[cfg(feature = "measure_io")]
            io_windows: vec![],
        }
    }

    fn get_actorpath(&self, pid: u64) -> &ActorPath {
        let idx = pid as usize - 1;
        self.nodes
            .get(idx)
            .unwrap_or_else(|| panic!("Could not get actorpath of pid: {}", pid))
    }

    fn derive_actorpaths(
        &self,
        config_id: ConfigId,
        peers: &[u64],
    ) -> (Vec<ActorPath>, HashMap<u64, ActorPath>) {
        let num_peers = peers.len();
        let mut communicator_peers = HashMap::with_capacity(num_peers);
        let mut ble_peers = Vec::with_capacity(num_peers);
        for pid in peers {
            let actorpath = self.get_actorpath(*pid);
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

    fn create_raw_paxos(
        &self,
        config_id: ConfigId,
        raw_peers: Option<Vec<u64>>,
        skip_prepare_use_leader: Option<Leader<Ballot>>,
    ) -> Paxos<Ballot, S, P> {
        let reconfig_replica = raw_peers.is_none(); // replica to be used after reconfiguration
        let max_inflight = self.experiment_params.max_inflight;
        let initial_log = if cfg!(feature = "preloaded_log") && !reconfig_replica {
            let size: u64 = self.experiment_params.preloaded_log_size;
            let mut preloaded_log = Vec::with_capacity(size as usize);
            for id in 1..=size {
                let data = create_raw_proposal(id);
                let entry = Entry::Normal(data);
                preloaded_log.push(entry);
            }
            preloaded_log
        } else {
            Vec::with_capacity(max_inflight)
        };
        let seq = S::new_with_sequence(initial_log);
        let paxos_state = P::new();
        let storage = Storage::with(seq, paxos_state);
        Paxos::with(
            config_id,
            self.pid,
            raw_peers.unwrap_or_else(|| vec![1]), // if peers is empty then we will set it later upon reconfiguration, hence initialise with vec![1] for now
            storage,
            skip_prepare_use_leader,
            Some(max_inflight),
        )
    }

    fn create_replica(
        &mut self,
        config_id: ConfigId,
        initial_nodes: Option<Vec<u64>>,
        ble_quick_start: bool,
        skip_prepare_n: Option<Leader<Ballot>>,
    ) -> Vec<KFuture<RegistrationResult>> {
        let peers = match initial_nodes {
            Some(nodes) => {
                let peers: Vec<u64> = nodes
                    .iter()
                    .filter(|pid| pid != &&self.pid)
                    .copied()
                    .collect();
                peers
            }
            _ => vec![],
        };
        let (ble_peers, communicator_peers) = self.derive_actorpaths(config_id, &peers);
        let raw_paxos_peers = if peers.is_empty() { None } else { Some(peers) };
        let raw_paxos = self.create_raw_paxos(config_id, raw_paxos_peers.clone(), skip_prepare_n);
        let system = self.ctx.system();
        /*** create and register Paxos ***/
        let (paxos, paxos_f) = system.create_and_register(|| {
            PaxosReplica::with(
                self.ctx.actor_ref(),
                raw_paxos_peers,
                config_id,
                self.pid,
                raw_paxos,
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
        config_id: ConfigId,
        skip_prepare_use_leader: Option<Leader<Ballot>>,
        peers: Vec<u64>,
    ) {
        let idx = (config_id - self.first_config_id) as usize;
        let paxos = self.paxos_replicas.get(idx).unwrap_or_else(|| {
            panic!(
                "No replica to set with idx: {}, replicas len: {}",
                idx,
                self.paxos_replicas.len()
            )
        });
        let (ble_peers, communicator_peers) = self.derive_actorpaths(config_id, &peers);
        paxos.on_definition(|p| {
            p.peers = peers.clone();
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
        let ble = self
            .ble_comps
            .get(idx)
            .unwrap_or_else(|| panic!("Could not find BLE config_id: {}", config_id));
        ble.on_definition(|ble| {
            ble.majority = ble_peers.len() / 2 + 1;
            ble.peers = ble_peers;
            if let Some(n) = skip_prepare_use_leader {
                ble.set_initial_leader(n);
            }
        });
        let communicator = self
            .communicator_comps
            .get(idx)
            .unwrap_or_else(|| panic!("Could not find BLE config_id: {}", config_id));
        communicator.on_definition(|comm| {
            comm.peers = communicator_peers;
        })
    }

    fn start_replica(&mut self, config_id: ConfigId) {
        let idx = (config_id - self.first_config_id) as usize;
        let paxos = self.paxos_replicas.get(idx).unwrap_or_else(|| {
            panic!(
                "No replica to start with idx: {}, replicas len: {}",
                idx,
                self.paxos_replicas.len()
            )
        });
        self.active_config = ConfigMeta::new(config_id);
        let ble = self
            .ble_comps
            .get(idx)
            .unwrap_or_else(|| panic!("Could not find BLE config_id: {}", config_id));
        let communicator = self
            .communicator_comps
            .get(idx)
            .unwrap_or_else(|| panic!("Could not find Communicator with config_id: {}", config_id));
        info!(
            self.ctx.log(),
            "Starting replica pid: {}, config_id: {}. PaxosReplica: {:?}, Communicator: {:?}, BLE: {:?}",
            self.pid, config_id, paxos.id(), communicator.id(), ble.id()
        );
        self.ctx.system().start(paxos);
        self.ctx.system().start(ble);
        self.ctx.system().start(communicator);
        self.next_config_id = None;
    }

    fn stop_ble(&mut self, log_io: Option<LogIOMetaData>, stop_futures: &mut Vec<KFuture<()>>) {
        let (ble_last, rest_ble) = self.ble_comps.split_last().expect("No ble comps!");
        for ble in rest_ble {
            stop_futures.push(
                ble.actor_ref()
                    .ask_with(|p| BLEStop(Ask::new(p, (self.pid, log_io.clone())))),
            );
        }
        stop_futures.push(
            ble_last
                .actor_ref()
                .ask_with(|p| BLEStop(Ask::new(p, (self.pid, log_io)))),
        );
    }

    fn stop_components(&mut self) -> Handled {
        self.stopped = true;
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
        #[cfg(feature = "measure_io")]
        {
            if let Some(timer) = self.io_timer.take() {
                self.cancel_timer(timer);
            }
            let mut file = self.experiment_params.get_io_meta_results_file();
            writeln!(
                file,
                "\n---------- IO usage node {} in iteration: {} ----------",
                self.pid, self.iteration_id
            )
                .expect("Failed to write IO file");
            if !self.io_windows.is_empty() || self.io_metadata != IOMetaData::default() {
                self.io_windows.push((self.clock.now(), self.io_metadata));
                self.io_metadata.reset();
                let mut str = String::new();
                let total =
                    self.io_windows
                        .iter()
                        .fold(IOMetaData::default(), |sum, (ts, io_meta)| {
                            str.push_str(&format!("{}, {:?}\n", ts.as_u64(), io_meta));
                            sum + (*io_meta)
                        });
                writeln!(file, "Total PaxosComp IO: {:?}\n{}", total, str)
                    .expect("Failed to write IO file");
                file.flush().expect("Failed to flush IO file");
            }
            let m = Mutex::new(file);
            let l = LogIOMetaData::with(Arc::new(m));
            for c in &self.communicator_comps {
                c.actor_ref().tell(l.clone());
            }
            self.stop_ble(Some(l), &mut stop_futures);
        }

        #[cfg(not(feature = "measure_io"))]
        {
            self.stop_ble(None, &mut stop_futures);
        }
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

    fn propose(&self, p: Proposal) {
        let idx = (self.active_config.id - self.first_config_id) as usize;
        let active_paxos = self
            .paxos_replicas
            .get(idx)
            .expect("Could not get PaxosComp actor ref despite being leader");
        active_paxos.actor_ref().tell(PaxosReplicaMsg::Propose(p));
    }

    fn propose_reconfiguration(&self, rp: ReconfigurationProposal) {
        let idx = (self.active_config.id - self.first_config_id) as usize;
        let active_paxos = self
            .paxos_replicas
            .get(idx)
            .expect("Could not get PaxosComp actor ref despite being leader");
        active_paxos
            .actor_ref()
            .tell(PaxosReplicaMsg::ProposeReconfiguration(rp));
    }

    fn deserialise_and_propose(&self, m: NetMessage) {
        match_deser! {m {
            msg(am): AtomicBroadcastMsg [using AtomicBroadcastDeser] => {
                match am {
                    AtomicBroadcastMsg::Proposal(p) => self.propose(p),
                    AtomicBroadcastMsg::ReconfigurationProposal(rp) => self.propose_reconfiguration(rp),
                    _ => {}
                }
            }
        }}
    }

    fn propose_hb_proposals(&mut self) {
        let hb_proposals = std::mem::take(&mut self.hb_proposals);
        let HoldBackProposals {
            serialised: ser_hb,
            deserialised: deser_hb,
        } = hb_proposals;
        for net_msg in ser_hb {
            self.deserialise_and_propose(net_msg);
        }
        for p in deser_hb {
            self.propose(p);
        }
    }

    fn forward_hb_proposals(&mut self, pid: u64) {
        let hb_proposals = std::mem::take(&mut self.hb_proposals);
        let HoldBackProposals {
            serialised: ser_hb,
            deserialised: deser_hb,
        } = hb_proposals;
        let receiver = self.get_actorpath(pid);
        for net_msg in ser_hb {
            receiver.forward_with_original_sender(net_msg, self);
        }
        for p in deser_hb {
            receiver
                .tell_serialised(AtomicBroadcastMsg::Proposal(p), self)
                .expect("Should serialise!");
        }
    }

    /// Calculates the `SegmentIndex` of each node
    fn get_node_segment_idx(&self, seq_len: u64, from_nodes: &[u64]) -> Vec<(u64, SegmentIndex)> {
        let offset = seq_len / from_nodes.len() as u64;
        from_nodes.iter().enumerate().map(|(i, pid)| {
            let from_idx = i as u64 * offset;
            let to_idx = if from_idx as u64 + offset > seq_len {
                seq_len
            } else {
                from_idx + offset
            };
            let idx = SegmentIndex::with(from_idx, to_idx);
            (*pid, idx)
        }).collect()
    }

    fn pull_sequence(&mut self, config_id: ConfigId, seq_len: u64, skip_idx: Option<SegmentIndex>, from_nodes: &[u64]) {
        debug!(self.ctx.log(), "Pull Sequence skip: {:?}", skip_idx);
        let skip = skip_idx.unwrap_or_else(|| SegmentIndex::with(0, 0));
        let indices: Vec<(u64, SegmentIndex)> = self.get_node_segment_idx(seq_len, from_nodes);
        for (pid, segment_idx) in &indices {
            if segment_idx != &skip {
                info!(
                    self.ctx.log(),
                    "Pull Sequence: Requesting segment from {}, config_id: {}, idx: {:?}",
                    pid,
                    config_id,
                    segment_idx
                );

                let sr = SegmentRequest::with(config_id, *segment_idx, self.pid);
                #[cfg(feature = "measure_io")]
                {
                    self.io_metadata.update_sent(&sr);
                }
                let receiver = self.get_actorpath(*pid);
                receiver
                    .tell_serialised(ReconfigurationMsg::SegmentRequest(sr), self)
                    .expect("Should serialise!");
            }
        }
        let transfer_timeout = self.ctx.config()["paxos"]["transfer_timeout"]
            .as_duration()
            .expect("Failed to load get_decided_period");
        let _ = self.schedule_once(transfer_timeout, move |c, _| {
            c.retry_pending_segments(config_id)
        });
        let pending_segments: Vec<SegmentIndex> = indices
            .iter()
            .filter(|(_, segment_idx)| segment_idx != &skip)
            .map(|(_, segment_idx)| *segment_idx)
            .collect();
        self.pending_segments.insert(config_id, pending_segments);
    }

    fn retry_pending_segments(&mut self, config_id: ConfigId) -> Handled {
        if let Some(remaining) = self.pending_segments.get(&config_id) {
            let active_config_ready_peers = &self.active_config_ready_peers;
            let num_active_peers = active_config_ready_peers.len();
            for (i, segment_idx) in remaining.iter().enumerate() {
                let idx = if i < num_active_peers {
                    num_active_peers - 1 - i
                } else {
                    num_active_peers - 1 - (i % num_active_peers)
                };
                let pid = active_config_ready_peers[idx];
                let sr = SegmentRequest::with(config_id, *segment_idx, self.pid);
                info!(self.ctx.log(), "Retry SegmentRequest: pid: {}, active_peers: {:?}, {:?}", pid, self.active_config_ready_peers, sr);
                #[cfg(feature = "measure_io")]
                {
                    self.io_metadata.update_sent(&sr);
                }
                let receiver = self.get_actorpath(pid);
                receiver
                    .tell_serialised(ReconfigurationMsg::SegmentRequest(sr), self)
                    .expect("Should serialise!");
            }
        }
        let transfer_timeout = self.ctx.config()["paxos"]["transfer_timeout"]
            .as_duration()
            .expect("Failed to load get_decided_period");
        let _ = self.schedule_once(transfer_timeout, move |c, _| {
            c.retry_pending_segments(config_id)
        });
        Handled::Ok
    }

    fn get_sequence_metadata(&self, config_id: ConfigId) -> SequenceMetaData {
        let seq_len = match self.prev_sequences.get(&config_id) {
            Some(prev_seq) => prev_seq.get_sequence_len(),
            None => 0,
        };
        SequenceMetaData::with(config_id, seq_len)
    }

    fn handle_segment(&mut self, config_id: ConfigId, s: SequenceSegment) {
        let config_id = config_id;
        let first_received = !self.received_segments.contains_key(&config_id);
        if first_received {
            self.received_segments.insert(config_id, vec![]);
        }
        let segment_idx = s.get_index();
        let segments = self
            .received_segments
            .get_mut(&config_id)
            .expect("No received segments");
        segments.push(s);
        self.pending_segments
            .get_mut(&config_id)
            .expect("No entry in pending segments with config_id")
            .retain(|idx| idx != &segment_idx);
    }

    fn handle_completed_sequence_transfer(&mut self, config_id: ConfigId) {
        let mut all_segments = self
            .received_segments
            .remove(&config_id)
            .expect("Should have all segments!");
        all_segments.sort_by_key(|s1| s1.get_from_idx());
        let len = all_segments.last().unwrap().get_to_idx() as usize;
        info!(self.ctx.log(), "Got complete sequence of config_id: {}, len: {}", config_id, len);
        let mut sequence = Vec::with_capacity(len);
        for mut segment in all_segments {
            sequence.append(&mut segment.entries);
        }
        self.prev_sequences
            .insert(config_id, Arc::new(S::new_with_sequence(sequence)));
        self.pending_segments.remove(&config_id);

        let next_config_id = self
            .next_config_id
            .expect("Got all sequence transfer but no next config id!");
        if self.prev_sequences.len() + 1 == next_config_id as usize {
            // got all sequence transfers
            info!(self.ctx.log(), "Got all previous sequences!");
            self.start_replica(next_config_id);
        }
    }

    fn get_continued_idx(&self, continued_nodes: &[u64]) -> usize {
        continued_nodes
            .iter()
            .position(|pid| pid == &self.pid)
            .expect("Could not find my pid in continued_nodes")
    }

    fn create_segment(&self, continued_nodes: &[u64], config_id: ConfigId) -> SequenceSegment {
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
        let idx = SegmentIndex::with(from_idx, to_idx);
        let entries = final_seq.get_entries(from_idx, to_idx).to_vec();
        SequenceSegment::with(idx, entries)
    }

    fn handle_segment_request(&mut self, sr: SegmentRequest, requestor: ActorPath) {
        if self.active_config.leader == sr.requestor_pid || self.handled_seq_requests.contains(&sr)
        {
            return;
        }
        let (succeeded, entries) = match self.prev_sequences.get(&sr.config_id) {
            Some(seq) => {
                let ents = seq.get_entries(sr.idx.from, sr.idx.to).to_vec();
                (true, ents)
            }
            None => {
                if self.active_config.id == sr.config_id {
                    // we have not reached final sequence, but might still have requested elements. Outsource request to corresponding PaxosComp
                    let idx = (self.active_config.id - self.first_config_id) as usize;
                    let paxos = self.paxos_replicas.get(idx).unwrap_or_else(|| panic!("No paxos replica with idx: {} when handling SequenceRequest. Len of PaxosReplicas: {}", idx, self.paxos_replicas.len()));
                    let prev_seq_metadata = self.get_sequence_metadata(sr.config_id - 1);
                    paxos.actor_ref().tell(PaxosReplicaMsg::LocalSegmentReq(
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
        let segment = SequenceSegment::with(sr.idx, entries);
        let st = SegmentTransfer::with(sr.config_id, succeeded, prev_seq_metadata, segment);
        #[cfg(feature = "measure_io")]
        {
            let est_segment_size = Self::estimate_segment_size(&st.segment);
            let est_size = std::mem::size_of_val(&st) + est_segment_size;
            self.io_metadata.update_sent_with_size(est_size);
        }
        info!(
            self.ctx.log(),
            "Replying to pid: {} with {} segment request: idx: {:?}",
            sr.requestor_pid,
            succeeded,
            sr.idx
        );
        requestor
            .tell_serialised(ReconfigurationMsg::SegmentTransfer(st), self)
            .expect("Should serialise!");
        if succeeded {
            self.handled_seq_requests.push(sr);
        }
    }

    fn has_handled_segment(&self, config_id: ConfigId, idx: SegmentIndex) -> bool {
        let already_handled = match self.received_segments.get(&config_id) {
            Some(segments) => segments.iter().any(|s| s.get_index() == idx),
            None => false,
        };
        already_handled
            || self.active_config.id >= config_id
            || self.prev_sequences.contains_key(&config_id)
    }

    fn handle_segment_transfer(&mut self, st: SegmentTransfer) {
        if self.has_handled_segment(st.config_id, st.segment.get_index()) {
            return;
        }
        let prev_config_id = st.metadata.config_id;
        let prev_seq_len = st.metadata.len;
        // pull previous sequence if exists and not already started
        if prev_config_id != 0
            && !self.prev_sequences.contains_key(&prev_config_id)
            && !self.pending_segments.contains_key(&prev_config_id)
        {
            let ready_peers = self.active_config_ready_peers.clone();
            self.pull_sequence(prev_config_id, prev_seq_len, None, &ready_peers);
        }
        if st.succeeded {
            debug!(
                self.ctx.log(),
                "Got successful segment {:?}",
                st.segment.get_index()
            );
            let config_id = st.config_id;
            self.handle_segment(config_id, st.segment);
            let got_all_segments = self
                .pending_segments
                .get(&config_id)
                .unwrap()
                .is_empty();
            if got_all_segments {
                self.handle_completed_sequence_transfer(config_id);
            }
        } else {
            // failed sequence transfer i.e. not reached final seq yet
            let config_id = st.config_id;
            warn!(
                self.ctx.log(),
                "Got failed segment transfer: idx: {:?}",
                st.segment.get_index()
            );
            // query someone we know have reached final seq
            let num_active = self.active_config_ready_peers.len();
            if num_active > 0 {
                // choose randomly
                let mut rng = rand::thread_rng();
                let rnd = rng.gen_range(0, num_active);
                let pid = self.active_config_ready_peers[rnd];
                let sr = SegmentRequest::with(config_id, st.segment.get_index(), self.pid);
                #[cfg(feature = "measure_io")]
                {
                    self.io_metadata.update_sent(&sr);
                }
                let receiver = self.get_actorpath(pid);
                receiver
                    .tell_serialised(ReconfigurationMsg::SegmentRequest(sr), self)
                    .expect("Should serialise!");
            } // else let timeout handle it to retry
        }
    }

    fn reset_state(&mut self) {
        self.pid = 0;
        self.active_config = ConfigMeta::new(0);
        self.nodes.clear();
        self.prev_sequences.clear();
        self.stopped = false;
        self.iteration_id = 0;
        self.next_config_id = None;
        self.pending_segments.clear();
        self.received_segments.clear();
        self.handled_seq_requests.clear();
        self.cached_client = None;
        self.hb_proposals.clear();
        self.active_config_ready_peers.clear();
        self.first_config_id = 0;
        self.removed = false;

        self.paxos_replicas.clear();
        self.ble_comps.clear();
        self.communicator_comps.clear();
        #[cfg(feature = "measure_io")]
        {
            self.io_metadata = IOMetaData::default();
            self.io_windows.clear();
        }
    }

    #[cfg(feature = "measure_io")]
    fn estimate_segment_size(s: &SequenceSegment) -> usize {
        let num_entries = s.entries.len();
        num_entries * DATA_SIZE
    }
}

#[derive(Debug)]
pub enum PaxosCompMsg<S>
where
    S: SequenceTraits<Ballot>,
{
    Leader(ConfigId, u64),
    PendingReconfig(Vec<u8>),
    Reconfig(FinalMsg<S>),
    KillComponents(Ask<(), Done>),
    #[cfg(test)]
    GetSequence(Ask<(), SequenceResp>),
    #[cfg(feature = "measure_io")]
    LocalSegmentTransferMeta(usize),
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
                    let prev_leader = self.active_config.leader;
                    self.active_config.leader = pid;
                    if pid == self.pid {
                        if prev_leader == 0 {
                            // notify client if no leader before
                            self.cached_client
                                .as_ref()
                                .expect("No cached client!")
                                .tell_serialised(AtomicBroadcastMsg::Leader(pid), self)
                                .expect("Should serialise FirstLeader");
                        }
                        self.propose_hb_proposals();
                    } else if !self.hb_proposals.is_empty() {
                        self.forward_hb_proposals(self.active_config.leader);
                    }
                }
            }
            PaxosCompMsg::PendingReconfig(data) => {
                self.active_config.pending_reconfig = true;
                self.hb_proposals.deserialised.push(Proposal::with(data));
            }
            PaxosCompMsg::Reconfig(r) => {
                /*** handle final sequence and notify new nodes ***/
                let prev_config_id = self.active_config.id;
                let prev_leader = self.active_config.leader;
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
                let continued = r.nodes.continued_nodes.contains(&self.pid);
                let prev_config_id = r.config_id - 1;
                /*** Start new replica if continued ***/
                if continued {
                    let peers = r.nodes.get_peers_of(self.pid);
                    self.set_initial_replica_state_after_reconfig(
                        r.config_id,
                        r.skip_prepare_use_leader,
                        peers,
                    );
                    self.start_replica(r.config_id);
                    // transfer sequence to new nodes
                    let continued_nodes = &r.nodes.continued_nodes;
                    let eager_transfer = if prev_leader == self.pid {
                        Some(self.create_segment(continued_nodes, prev_config_id))
                    } else {
                        None
                    };
                    for pid in &r.nodes.new_nodes {
                        let segment = if !self.handled_seq_requests.iter().any(|sr| {
                            sr.config_id == prev_config_id && sr.requestor_pid == *pid
                        }) {
                            // the new node hasn't already requested us
                            if let Some(s) = &eager_transfer {
                                self.handled_seq_requests.push(SegmentRequest::with(prev_config_id, s.get_index(), *pid));
                            }
                            eager_transfer.clone()
                        } else {
                            None
                        };
                        let r = ReconfigInit::with(
                            r.config_id,
                            r.nodes.clone(),
                            seq_metadata.clone(),
                            self.pid,
                            segment,
                            r.skip_prepare_use_leader,
                        );
                        #[cfg(feature = "measure_io")]
                        {
                            let est_segment_size = match &r.segment {
                                Some(s) => Self::estimate_segment_size(s),
                                None => 0,
                            };
                            let est_size = std::mem::size_of_val(&r) + est_segment_size;
                            self.io_metadata.update_sent_with_size(est_size);
                        }
                        let r_init = ReconfigurationMsg::Init(r);
                        let actorpath = self.get_actorpath(*pid);
                        actorpath
                            .tell_serialised(r_init, self)
                            .expect("Should serialise!");
                    }
                } else {
                    self.removed = true;
                }
                if !self.hb_proposals.is_empty() {
                    let receiver = match r.skip_prepare_use_leader {
                        Some(Leader { pid, .. }) => pid,
                        None => match r.nodes.continued_nodes.first() {
                            Some(pid) => *pid,
                            None => *r
                                .nodes
                                .new_nodes
                                .first()
                                .expect("No nodes in continued or new nodes!?"),
                        },
                    };
                    self.forward_hb_proposals(receiver);
                }
            }
            PaxosCompMsg::KillComponents(ask) => {
                let handled = self.kill_components(ask);
                return handled;
            }
            #[cfg(feature = "measure_io")]
            PaxosCompMsg::LocalSegmentTransferMeta(size) => {
                self.io_metadata.update_sent_with_size(size);
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
                if !self.stopped {
                    if self.removed {
                        self.cached_client
                            .as_ref()
                            .expect("No cached client!")
                            .forward_with_original_sender(m, self);
                    } else if self.active_config.pending_reconfig {
                        self.hb_proposals.serialised.push(m);
                    } else {
                        match self.active_config.leader {
                            0 => {
                                // active config has no leader yet
                                self.hb_proposals.serialised.push(m);
                            }
                            my_pid if my_pid == self.pid => self.deserialise_and_propose(m),
                            other => {
                                let leader = self.get_actorpath(other);
                                leader.forward_with_original_sender(m, self);
                            }
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
                                #[cfg(feature = "measure_io")] {
                                    let timer = self.schedule_periodic(WINDOW_DURATION, WINDOW_DURATION, move |c, _| {
                                        if !c.io_windows.is_empty() || c.io_metadata != IOMetaData::default() {
                                            c.io_windows.push((c.clock.now(), c.io_metadata));
                                            c.io_metadata.reset();
                                        }
                                        Handled::Ok
                                    });
                                    self.io_timer = Some(timer);
                                }
                                let initial_configuration= self.initial_configuration.clone();
                                let handled = Handled::block_on(self, move |mut async_self| async move {
                                    if initial_configuration.contains(&async_self.pid) {
                                        async_self.next_config_id = Some(1);
                                        let futures = async_self.create_replica(1, Some(initial_configuration), true, None);
                                        for f in futures {
                                            f.await.unwrap().expect("Failed to register when creating replica 1");
                                        }
                                        async_self.first_config_id = 1;
                                    }
                                    if async_self.is_reconfig_exp {
                                        let futures = async_self.create_replica(2, None, false, None);
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
                                } else {
                                    #[cfg(feature = "measure_io")] {
                                        let est_segment_size = match &r.segment {
                                            Some(s) => Self::estimate_segment_size(s),
                                            None => 0
                                        };
                                        let est_size = std::mem::size_of_val(&r) + est_segment_size;
                                        self.io_metadata.update_received_with_size(est_size);
                                    }
                                    if self.active_config.id >= r.config_id {
                                        return Handled::Ok;
                                    }
                                    match self.next_config_id {
                                        None => {
                                            debug!(self.ctx.log(), "Got ReconfigInit for config_id: {} from node {}", r.config_id, r.from);
                                            self.next_config_id = Some(r.config_id);
                                            let peers = r.nodes.get_peers_of(self.pid);
                                            self.set_initial_replica_state_after_reconfig(r.config_id, r.skip_prepare_use_leader, peers);
                                            if r.nodes.continued_nodes.contains(&r.from) {
                                                self.active_config_ready_peers.push(r.from);
                                            }
                                            if r.seq_metadata.len == 1 && r.seq_metadata.config_id == 1 {
                                                // only SS in final sequence and no other prev sequences -> start directly
                                                let final_sequence = S::new_with_sequence(vec![]);
                                                self.prev_sequences.insert(r.seq_metadata.config_id, Arc::new(final_sequence));
                                                self.start_replica(r.config_id);
                                            } else {
                                                let skip_idx = r.segment.as_ref().map(|segment| segment.get_index());
                                                // pull sequence from continued nodes
                                                self.pull_sequence(r.seq_metadata.config_id, r.seq_metadata.len, skip_idx, &r.nodes.continued_nodes);
                                                if skip_idx.is_some() {
                                                    let st = SegmentTransfer::with(r.seq_metadata.config_id, true, r.seq_metadata, r.segment.unwrap());
                                                    self.handle_segment_transfer(st);
                                                }
                                            }
                                        },
                                        Some(next_config_id) => {
                                            if next_config_id == r.config_id && r.nodes.continued_nodes.contains(&r.from) {
                                                // update who we know already decided final seq
                                                if r.nodes.continued_nodes.contains(&r.from) {
                                                    self.active_config_ready_peers.push(r.from);
                                                }
                                                let eager_segment_transfer = r.segment.is_some();
                                                if eager_segment_transfer {
                                                    let st = SegmentTransfer::with(r.seq_metadata.config_id, true, r.seq_metadata, r.segment.unwrap());
                                                    self.handle_segment_transfer(st);
                                                }
                                            }
                                        }
                                    }
                                }
                            },
                            ReconfigurationMsg::SegmentRequest(sr) => {
                                if !self.stopped {
                                    #[cfg(feature = "measure_io")] {
                                        self.io_metadata.update_received(&sr);
                                    }
                                    self.handle_segment_request(sr, sender);
                                }
                            },
                            ReconfigurationMsg::SegmentTransfer(st) => {
                                if !self.stopped {
                                    #[cfg(feature = "measure_io")] {
                                        let est_segment_size = Self::estimate_segment_size(&st.segment);
                                        let est_size = std::mem::size_of_val(&st) + est_segment_size;
                                        self.io_metadata.update_received_with_size(est_size);
                                    }
                                    self.handle_segment_transfer(st);
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
    config_id: ConfigId,
    pid: u64,
    current_leader: u64,
    timer: Option<ScheduledTimer>,
    stopped: bool,
    stopped_peers: HashSet<u64>,
    stop_ask: Option<Ask<bool, ()>>,
    #[cfg(feature = "periodic_replica_logging")]
    num_decided: usize,
}

impl<S, P> PaxosReplica<S, P>
where
    S: SequenceTraits<Ballot>,
    P: StateTraits<Ballot>,
{
    fn with(
        supervisor: ActorRef<PaxosCompMsg<S>>,
        peers: Option<Vec<u64>>,
        config_id: ConfigId,
        pid: u64,
        paxos: Paxos<Ballot, S, P>,
    ) -> PaxosReplica<S, P> {
        PaxosReplica {
            ctx: ComponentContext::uninitialised(),
            supervisor,
            communication_port: RequiredPort::uninitialised(),
            ble_port: RequiredPort::uninitialised(),
            stopped_peers: HashSet::new(),
            peers: peers.unwrap_or_default(),
            paxos,
            config_id,
            pid,
            current_leader: 0,
            timer: None,
            stopped: false,
            stop_ask: None,
            #[cfg(feature = "periodic_replica_logging")]
            num_decided: 0,
        }
    }

    fn start_timer(&mut self) {
        let config = self.ctx.config();
        let outgoing_period = config["experiment"]["outgoing_period"]
            .as_duration()
            .expect("Failed to load outgoing_period");
        let timer =
            self.schedule_periodic(Duration::from_millis(0), outgoing_period, move |c, _| {
                c.get_decided();
                c.send_outgoing();
                Handled::Ok
            });
        self.timer = Some(timer);
        #[cfg(feature = "periodic_replica_logging")]
        {
            self.schedule_periodic(WINDOW_DURATION, WINDOW_DURATION, move |c, _| {
                info!(
                    c.ctx.log(),
                    "Decided: {} in config_id: {}", c.num_decided, c.config_id
                );
                Handled::Ok
            });
        }
    }

    fn stop_timer(&mut self) {
        if let Some(timer) = self.timer.take() {
            self.cancel_timer(timer);
        }
        #[cfg(feature = "periodic_replica_logging")]
        {
            info!(
                self.ctx.log(),
                "Stopped timers. Decided: {} in config_id: {}", self.num_decided, self.config_id
            );
        }
    }

    fn send_outgoing(&mut self) {
        for out_msg in self.paxos.get_outgoing_msgs() {
            self.communication_port
                .trigger(CommunicatorMsg::RawPaxosMsg(out_msg));
        }
    }

    fn handle_stopsign(&mut self, ss: StopSign<Ballot>) {
        let final_seq = self.paxos.stop_and_get_sequence();
        let (continued_nodes, new_nodes) = ss
            .nodes
            .iter()
            .partition(|&pid| pid == &self.pid || self.peers.contains(pid));
        debug!(
            self.ctx.log(),
            "Decided StopSign! Continued: {:?}, new: {:?}", &continued_nodes, &new_nodes
        );
        let leader = match ss.skip_prepare_use_leader {
            Some(l) => l.pid,
            None => 0,
        };
        let nodes = Reconfig::with(continued_nodes, new_nodes);
        let r = FinalMsg::with(ss.config_id, nodes, final_seq, ss.skip_prepare_use_leader);
        self.supervisor.tell(PaxosCompMsg::Reconfig(r));
        // respond client
        let rr = ReconfigurationResp::with(leader, ss.nodes);
        self.communication_port
            .trigger(CommunicatorMsg::ReconfigurationResponse(rr));
    }

    fn get_decided(&mut self) {
        let leader = self.paxos.get_current_leader();
        if self.current_leader != leader {
            self.current_leader = leader;
            self.supervisor
                .tell(PaxosCompMsg::Leader(self.config_id, self.current_leader));
        }
        let decided_entries = self.paxos.get_decided_entries();
        #[cfg(feature = "periodic_replica_logging")]
        {
            self.num_decided += decided_entries.len();
        }
        if self.current_leader == self.pid {
            for decided in decided_entries.to_vec() {
                match decided {
                    Entry::Normal(data) => {
                        let pr = ProposalResp::with(data, self.pid);
                        self.communication_port
                            .trigger(CommunicatorMsg::ProposalResponse(pr));
                    }
                    Entry::StopSign(ss) => {
                        self.handle_stopsign(ss);
                    }
                }
            }
        } else {
            let last = decided_entries.last().cloned();
            if let Some(Entry::StopSign(ss)) = last {
                self.handle_stopsign(ss);
            }
        }
    }

    fn propose(&mut self, p: Proposal) -> Result<(), ProposeErr> {
        self.paxos.propose_normal(p.data)
    }

    fn propose_reconfiguration(&mut self, reconfig: Vec<u64>) -> Result<(), ProposeErr> {
        let n = self.ctx.config()["paxos"]["prio_start_round"]
            .as_i64()
            .expect("No prio start round in config!") as u32;
        let prio_start_round = Ballot::with(n, 0);
        self.paxos
            .propose_reconfiguration(reconfig, Some(prio_start_round))
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
                if let Err(propose_err) = self.propose(p) {
                    match propose_err {
                        ProposeErr::Normal(data) => {
                            self.supervisor.tell(PaxosCompMsg::PendingReconfig(data))
                        }
                        ProposeErr::Reconfiguration(_) => {
                            unreachable!()
                        }
                    }
                }
            }
            PaxosReplicaMsg::ProposeReconfiguration(rp) => {
                let mut current_config = self.peers.clone();
                current_config.push(self.pid);
                let new_config = rp.get_new_configuration(self.current_leader, current_config);
                self.propose_reconfiguration(new_config)
                    .expect("Failed to propose reconfiguration")
            }
            PaxosReplicaMsg::LocalSegmentReq(requestor, seq_req, prev_seq_metadata) => {
                let segment_idx = seq_req.idx;
                let (succeeded, entries) = self
                    .paxos
                    .get_chosen_entries(segment_idx.from, segment_idx.to);
                let segment = SequenceSegment::with(segment_idx, entries);
                let st =
                    SegmentTransfer::with(seq_req.config_id, succeeded, prev_seq_metadata, segment);
                #[cfg(feature = "measure_io")]
                {
                    let size = std::mem::size_of_val(&st);
                    self.supervisor
                        .tell(PaxosCompMsg::LocalSegmentTransferMeta(size));
                }
                requestor
                    .tell_serialised(ReconfigurationMsg::SegmentTransfer(st), self)
                    .expect("Should serialise!");
            }
            PaxosReplicaMsg::Stop(ask) => {
                let ack_client = *ask.request();
                self.communication_port
                    .trigger(CommunicatorMsg::SendStop(self.pid, ack_client));
                self.stop_timer();
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
        self.ctx.init_buffers(Some(bc), None);
        if !self.peers.is_empty() {
            self.start_timer();
        }
        Handled::Ok
    }

    fn on_kill(&mut self) -> Handled {
        self.stop_timer();
        #[cfg(feature = "measure_io")]
        {
            if !self.peers.is_empty() {
                let final_seq = self.paxos.get_sequence();
                info!(
                    self.ctx.log(),
                    "Final Sequence in config_id: {}, len: {}",
                    self.config_id,
                    final_seq.len()
                );
            }
        }
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
