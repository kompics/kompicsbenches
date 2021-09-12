use super::messages::{
    AtomicBroadcastDeser, AtomicBroadcastMsg, Proposal, StopMsg as NetStopMsg, StopMsgDeser,
    RECONFIG_ID,
};
#[cfg(feature = "simulate_partition")]
use crate::bench::atomic_broadcast::messages::PartitioningExpMsg;
use crate::bench::atomic_broadcast::{
    atomic_broadcast::ReconfigurationPolicy,
    messages::{ReconfigurationProposal, ReconfigurationResp},
    util::exp_params::*,
};
use hashbrown::HashMap;
use kompact::prelude::*;
use quanta::{Clock, Instant};
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};
use synchronoise::{event::CountdownError, CountdownEvent};

const STOP_TIMEOUT: Duration = Duration::from_secs(30);
const PROPOSAL_ID_SIZE: usize = 8; // size of u64
const PAYLOAD: [u8; DATA_SIZE - PROPOSAL_ID_SIZE] = [0; DATA_SIZE - PROPOSAL_ID_SIZE];
#[derive(Debug, PartialEq)]
enum ExperimentState {
    Setup,
    Running,
    Finished,
}

#[derive(Debug)]
pub enum LocalClientMessage {
    Run,
    Stop(Ask<(), MetaResults>), // (num_timed_out, latency)
}

#[derive(Debug)]
struct ProposalMetaData {
    start_time: Option<Instant>,
    timer: ScheduledTimer,
}

impl ProposalMetaData {
    fn with(start_time: Option<Instant>, timer: ScheduledTimer) -> ProposalMetaData {
        ProposalMetaData { start_time, timer }
    }

    fn set_timer(&mut self, timer: ScheduledTimer) {
        self.timer = timer;
    }
}

#[derive(Debug)]
pub struct MetaResults {
    pub num_timed_out: u64,
    pub num_retried: u64,
    pub latencies: Vec<Duration>,
    pub leader_changes: Vec<(SystemTime, (u64, u64))>,
    pub windowed_results: Vec<usize>,
    pub reconfig_ts: Option<(SystemTime, SystemTime)>,
    pub timestamps: Vec<Instant>,
}

impl MetaResults {
    pub fn with(
        num_timed_out: u64,
        num_retried: u64,
        latencies: Vec<Duration>,
        leader_changes: Vec<(SystemTime, (u64, u64))>,
        windowed_results: Vec<usize>,
        reconfig_ts: Option<(SystemTime, SystemTime)>,
        timestamps: Vec<Instant>,
    ) -> Self {
        MetaResults {
            num_timed_out,
            num_retried,
            latencies,
            leader_changes,
            windowed_results,
            reconfig_ts,
            timestamps,
        }
    }
}

#[derive(ComponentDefinition)]
pub struct Client {
    ctx: ComponentContext<Self>,
    num_proposals: u64,
    num_concurrent_proposals: u64,
    nodes: HashMap<u64, ActorPath>,
    reconfig: Option<(ReconfigurationPolicy, Vec<u64>)>,
    leader_election_latch: Arc<CountdownEvent>,
    finished_latch: Arc<CountdownEvent>,
    latest_proposal_id: u64,
    max_proposal_id: u64,
    responses: HashMap<u64, Option<Duration>>,
    pending_proposals: HashMap<u64, ProposalMetaData>,
    timeout: Duration,
    current_leader: u64,
    leader_round: u64,
    state: ExperimentState,
    current_config: Vec<u64>,
    num_timed_out: u64,
    num_retried: usize,
    leader_changes: Vec<(SystemTime, (u64, u64))>,
    stop_ask: Option<Ask<(), MetaResults>>,
    window_timer: Option<ScheduledTimer>,
    /// timestamp, number of proposals completed
    windows: Vec<usize>,
    clock: Clock,
    reconfig_start_ts: Option<SystemTime>,
    reconfig_end_ts: Option<SystemTime>,
    #[cfg(feature = "track_timeouts")]
    timeouts: Vec<u64>,
    #[cfg(feature = "track_timeouts")]
    late_responses: Vec<u64>,
    #[cfg(feature = "track_timestamps")]
    timestamps: HashMap<u64, Instant>,
    #[cfg(feature = "preloaded_log")]
    num_preloaded_proposals: u64,
    #[cfg(feature = "preloaded_log")]
    rem_preloaded_proposals: u64,
    #[cfg(feature = "simulate_partition")]
    periodic_partition_timer: Option<ScheduledTimer>,
    #[cfg(feature = "simulate_partition")]
    recover_periodic_partition: bool,
}

impl Client {
    pub fn with(
        initial_config: Vec<u64>,
        num_proposals: u64,
        num_concurrent_proposals: u64,
        nodes: HashMap<u64, ActorPath>,
        reconfig: Option<(ReconfigurationPolicy, Vec<u64>)>,
        timeout: Duration,
        preloaded_log_size: u64,
        leader_election_latch: Arc<CountdownEvent>,
        finished_latch: Arc<CountdownEvent>,
    ) -> Client {
        let clock = Clock::new();
        Client {
            ctx: ComponentContext::uninitialised(),
            num_proposals,
            num_concurrent_proposals,
            nodes,
            reconfig,
            leader_election_latch,
            finished_latch,
            latest_proposal_id: preloaded_log_size,
            max_proposal_id: num_proposals + preloaded_log_size,
            responses: HashMap::with_capacity(num_proposals as usize),
            pending_proposals: HashMap::with_capacity(num_concurrent_proposals as usize),
            timeout,
            current_leader: 0,
            leader_round: 0,
            state: ExperimentState::Setup,
            current_config: initial_config,
            num_timed_out: 0,
            num_retried: 0,
            leader_changes: vec![],
            stop_ask: None,
            window_timer: None,
            windows: vec![],
            clock,
            reconfig_start_ts: None,
            reconfig_end_ts: None,
            #[cfg(feature = "track_timeouts")]
            timeouts: vec![],
            #[cfg(feature = "track_timestamps")]
            timestamps: HashMap::with_capacity(num_proposals as usize),
            #[cfg(feature = "track_timeouts")]
            late_responses: vec![],
            #[cfg(feature = "preloaded_log")]
            num_preloaded_proposals: preloaded_log_size,
            #[cfg(feature = "preloaded_log")]
            rem_preloaded_proposals: preloaded_log_size,
            #[cfg(feature = "simulate_partition")]
            periodic_partition_timer: None,
            #[cfg(feature = "simulate_partition")]
            recover_periodic_partition: false,
        }
    }

    fn propose_normal(&self, id: u64) {
        let leader = self.nodes.get(&self.current_leader).unwrap();
        let data = create_raw_proposal(id);
        let p = Proposal::with(data);
        leader
            .tell_serialised(AtomicBroadcastMsg::Proposal(p), self)
            .expect("Should serialise Proposal");
    }

    fn propose_reconfiguration(&self, node: &ActorPath) {
        let (policy, reconfig) = self.reconfig.as_ref().unwrap();
        info!(
            self.ctx.log(),
            "{}",
            format!(
                "Proposing reconfiguration: policy: {:?}, new nodes: {:?}",
                policy, reconfig
            )
        );
        let rp = ReconfigurationProposal::with(*policy, reconfig.clone());
        node.tell_serialised(AtomicBroadcastMsg::ReconfigurationProposal(rp), self)
            .expect("Should serialise reconfig Proposal");
        #[cfg(feature = "track_timeouts")]
        {
            info!(self.ctx.log(), "Proposed reconfiguration. latest_proposal_id: {}, timed_out: {}, pending proposals: {}, min: {:?}, max: {:?}",
                self.latest_proposal_id, self.num_timed_out, self.pending_proposals.len(), self.pending_proposals.keys().min(), self.pending_proposals.keys().max());
        }
    }

    fn send_concurrent_proposals(&mut self) {
        let num_inflight = self.pending_proposals.len() as u64;
        if num_inflight == self.num_concurrent_proposals || self.current_leader == 0 {
            return;
        }
        let available_n = self.num_concurrent_proposals - num_inflight;
        let from = self.latest_proposal_id + 1;
        let i = self.latest_proposal_id + available_n;
        let to = if i > self.max_proposal_id {
            self.max_proposal_id
        } else {
            i
        };
        if from > to {
            return;
        }
        let cache_start_time =
            self.num_concurrent_proposals == 1 || cfg!(feature = "track_latency");
        for id in from..=to {
            let current_time = match cache_start_time {
                true => Some(self.clock.now()),
                _ => None,
            };
            self.propose_normal(id);
            let timer = self.schedule_once(self.timeout, move |c, _| c.proposal_timeout(id));
            let proposal_meta = ProposalMetaData::with(current_time, timer);
            self.pending_proposals.insert(id, proposal_meta);
        }
        self.latest_proposal_id = to;
    }

    fn handle_normal_response(&mut self, id: u64, latency_res: Option<Duration>) {
        #[cfg(feature = "track_timestamps")]
        {
            let timestamp = self.clock.now();
            self.timestamps.insert(id, timestamp);
        }
        self.responses.insert(id, latency_res);
        let received_count = self.responses.len() as u64;
        if received_count == self.num_proposals {
            if self.reconfig.is_none() {
                if let Some(timer) = self.window_timer.take() {
                    self.cancel_timer(timer);
                }
                #[cfg(feature = "simulate_partition")] {
                    if let Some(timer) = self.periodic_partition_timer.take() {
                        self.cancel_timer(timer);
                    }
                }
                self.state = ExperimentState::Finished;
                self.finished_latch
                    .decrement()
                    .expect("Failed to countdown finished latch");
                let leader_changes: Vec<_> =
                    self.leader_changes.iter().map(|(_ts, lc)| lc).collect();
                if self.num_timed_out > 0 || self.num_retried > 0 {
                    info!(self.ctx.log(), "Got all responses with {} timeouts and {} retries. Number of leader changes: {}, {:?}, Last leader was: {}, ballot/term: {}.", self.num_timed_out, self.num_retried, self.leader_changes.len(), leader_changes, self.current_leader, self.leader_round);
                    #[cfg(feature = "track_timeouts")]
                    {
                        let min = self.timeouts.iter().min();
                        let max = self.timeouts.iter().max();
                        let late_min = self.late_responses.iter().min();
                        let late_max = self.late_responses.iter().max();
                        info!(
                                self.ctx.log(),
                                "Timed out: Min: {:?}, Max: {:?}. Late responses: {}, min: {:?}, max: {:?}",
                                min,
                                max,
                                self.late_responses.len(),
                                late_min,
                                late_max
                            );
                    }
                } else {
                    info!(
                        self.ctx.log(),
                        "Got all responses. Number of leader changes: {}, {:?}, Last leader was: {}, ballot/term: {}.",
                        self.leader_changes.len(),
                        leader_changes,
                        self.current_leader,
                        self.leader_round
                    );
                }
            } else {
                warn!(
                    self.ctx.log(),
                    "Got all normal responses but still pending reconfiguration"
                );
            }
        } else if received_count == self.num_proposals / 2 && self.reconfig.is_some() {
            let leader = self
                .nodes
                .get(&self.current_leader)
                .expect("No leader to propose reconfiguration to!");
            self.propose_reconfiguration(&leader);
            self.reconfig_start_ts = Some(SystemTime::now());
            let timer = self.schedule_once(self.timeout, move |c, _| c.reconfig_timeout());
            let proposal_meta = ProposalMetaData::with(None, timer);
            self.pending_proposals.insert(RECONFIG_ID, proposal_meta);
        }
    }

    fn handle_reconfig_response(&mut self, rr: ReconfigurationResp) {
        if let Some(proposal_meta) = self.pending_proposals.remove(&RECONFIG_ID) {
            self.reconfig_end_ts = Some(SystemTime::now());
            let new_config = rr.current_configuration;
            self.cancel_timer(proposal_meta.timer);
            if self.responses.len() as u64 == self.num_proposals {
                self.state = ExperimentState::Finished;
                self.finished_latch
                    .decrement()
                    .expect("Failed to countdown finished latch");
                info!(self.ctx.log(), "Got reconfig at last. {} proposals timed out. Leader changes: {}, {:?}, Last leader was: {}, ballot/term: {}", self.num_timed_out, self.leader_changes.len(), self.leader_changes, self.current_leader, self.leader_round);
            } else {
                self.reconfig = None;
                self.current_config = new_config;
                info!(
                    self.ctx.log(),
                    "Reconfig OK, leader: {}, old: {}, current_config: {:?}",
                    rr.latest_leader,
                    self.current_leader,
                    self.current_config
                );
                if rr.latest_leader > 0
                    && self.current_leader != rr.latest_leader
                    && rr.leader_round > self.leader_round
                {
                    self.current_leader = rr.latest_leader;
                    self.leader_round = rr.leader_round;
                    self.leader_changes
                        .push((SystemTime::now(), (self.current_leader, self.leader_round)));
                }
                self.send_concurrent_proposals();
            }
        }
    }

    fn proposal_timeout(&mut self, id: u64) -> Handled {
        if self.responses.contains_key(&id) {
            return Handled::Ok;
        }
        // info!(self.ctx.log(), "Timed out proposal {}", id);
        self.num_timed_out += 1;
        self.propose_normal(id);
        let _ = self.schedule_once(self.timeout, move |c, _| c.proposal_timeout(id));
        #[cfg(feature = "track_timeouts")]
        {
            self.timeouts.push(id);
        }
        Handled::Ok
    }

    fn reconfig_timeout(&mut self) -> Handled {
        let leader = self
            .nodes
            .get(&self.current_leader)
            .expect("No leader to propose reconfiguration to!");
        self.propose_reconfiguration(leader);
        let timer = self.schedule_once(self.timeout, move |c, _| c.reconfig_timeout());
        let proposal_meta = self
            .pending_proposals
            .get_mut(&RECONFIG_ID)
            .expect("Could not find MetaData for Reconfiguration in pending_proposals");
        proposal_meta.set_timer(timer);
        Handled::Ok
    }

    fn send_stop(&mut self) {
        for ap in self.nodes.values() {
            ap.tell_serialised(NetStopMsg::Client, self)
                .expect("Failed to send Client stop");
        }
        let _ = self.schedule_once(STOP_TIMEOUT, move |c, _| {
            warn!(c.ctx.log(), "Client timed out stopping... Returning to BenchmarkMaster anyway who will force kill.");
            c.reply_stop_ask();
            Handled::Ok
        });
    }

    fn reply_stop_ask(&mut self) {
        if let Some(stop_ask) = self.stop_ask.take() {
            let l = std::mem::take(&mut self.responses);
            let mut v: Vec<_> = l
                .into_iter()
                .filter(|(_, latency)| latency.is_some())
                .collect();
            v.sort();
            let latencies: Vec<Duration> =
                v.into_iter().map(|(_, latency)| latency.unwrap()).collect();

            let reconfig_ts = self
                .reconfig_start_ts
                .map(|start_ts| (start_ts, self.reconfig_end_ts.unwrap()));
            #[allow(unused_mut)] // TODO remove
            let mut meta_results = MetaResults::with(
                self.num_timed_out,
                self.num_retried as u64,
                latencies,
                std::mem::take(&mut self.leader_changes),
                std::mem::take(&mut self.windows),
                reconfig_ts,
                vec![],
            );
            #[cfg(feature = "track_timestamps")]
            {
                let mut timestamps: Vec<_> =
                    std::mem::take(&mut self.timestamps).into_iter().collect();
                timestamps.sort();
                meta_results.timestamps = timestamps.into_iter().map(|(_pid, ts)| ts).collect();
            }
            stop_ask
                .reply(meta_results)
                .expect("Failed to reply StopAsk!");
        }
    }

    #[cfg(feature = "simulate_partition")]
    fn create_partition(&mut self) {
        assert_ne!(self.current_leader, 0);
        let leader_ap = self.nodes.get(&self.current_leader).expect("No leader");
        let followers: Vec<_> = self
            .nodes
            .iter()
            .filter_map(|(pid, _)| {
                if pid != &self.current_leader {
                    Some(pid)
                } else {
                    None
                }
            })
            .copied()
            .collect();
        if self.nodes.len() == 5 {
            // Deadlock scenario
            let lagging_follower = *followers.first().unwrap(); // first follower to be partitioned from the leader
            info!(self.ctx.log(), "Creating partition. leader: {}, term: {}, lagging follower connected to majority: {}, num_responses: {}", self.current_leader, self.leader_round, lagging_follower, self.responses.len());
            for pid in &followers {
                let disconnect_peers: Vec<_> = if pid == &lagging_follower {
                    vec![self.current_leader]
                } else {
                    self.nodes
                        .keys()
                        .filter(|p| *p != &lagging_follower && *p != pid)
                        .copied()
                        .collect()
                };
                info!(
                    self.ctx.log(),
                    "Node {} getting disconnected from {:?}", pid, disconnect_peers
                );
                let ap = self.nodes.get(&pid).expect("No follower ap");
                ap.tell_serialised(
                    PartitioningExpMsg::DisconnectPeers(disconnect_peers, None),
                    self,
                )
                .expect("Should serialise");
            }
            let non_lagging: Vec<u64> = followers
                .iter()
                .filter(|pid| *pid != &lagging_follower)
                .copied()
                .collect();
            leader_ap
                .tell_serialised(
                    PartitioningExpMsg::DisconnectPeers(non_lagging, Some(lagging_follower)),
                    self,
                )
                .expect("Should serialise");
        } else if self.nodes.len() == 3 {
            // Chained scenario
            let disconnected_follower = followers.first().unwrap();
            let ap = self.nodes.get(disconnected_follower).unwrap();
            ap.tell_serialised(
                PartitioningExpMsg::DisconnectPeers(vec![self.current_leader], None),
                self,
            )
            .expect("Should serialise!");
            leader_ap
                .tell_serialised(
                    PartitioningExpMsg::DisconnectPeers(vec![*disconnected_follower], None),
                    self,
                )
                .expect("Should serialise!");
            info!(
                self.ctx.log(),
                "Creating partition. Disconnecting leader: {} from follower: {}, num_responses: {}",
                self.current_leader,
                disconnected_follower,
                self.responses.len()
            );
            /*
            // Periodic full scenario
            let disconnected_follower = *followers.first().unwrap();
            let intermediate_duration = self.ctx.config()["partition_experiment"]
                ["intermediate_duration"]
                .as_duration()
                .expect("No intermediate duration!");
            let timer = self.schedule_periodic(
                Duration::from_millis(0),
                intermediate_duration,
                move |c, _| {
                    if c.periodic_partition_timer.is_some() {
                        c.periodic_partition(disconnected_follower);
                    }
                    Handled::Ok
                },
            );
            self.periodic_partition_timer = Some(timer);
            return; // end of periodic partition
             */
        } else {
            unimplemented!()
        }
        let partition_duration = self.ctx.config()["partition_experiment"]["partition_duration"]
            .as_duration()
            .expect("No partition duration!");
        self.schedule_once(partition_duration, move |c, _| {
            if let Some(timer) = c.periodic_partition_timer.take() {
                c.cancel_timer(timer);
            }
            c.recover_partition();
            Handled::Ok
        });
    }

    #[cfg(feature = "simulate_partition")]
    fn recover_partition(&mut self) {
        info!(self.ctx.log(), "Recovering from network partition. Leader: {}, ballot/term: {}", self.current_leader, self.leader_round);
        for (_pid, ap) in &self.nodes {
            ap.tell_serialised(PartitioningExpMsg::RecoverPeers, self)
                .expect("Should serialise");
        }
    }

    #[cfg(feature = "simulate_partition")]
    fn periodic_partition(&mut self, disconnected_follower: u64) {
        if self.recover_periodic_partition {
            self.recover_partition()
        } else {
            info!(self.ctx.log(), "Partitioning {}. Leader: {}, ballot/term: {}", disconnected_follower, self.current_leader, self.leader_round);
            let rest: Vec<u64> = self
                .nodes
                .keys()
                .filter(|pid| **pid != disconnected_follower)
                .copied()
                .collect();
            for (pid, ap) in self.nodes.iter() {
                if pid == &disconnected_follower {
                    ap.tell_serialised(
                        PartitioningExpMsg::DisconnectPeers(rest.clone(), None),
                        self,
                    )
                    .expect("Should serialise!");
                } else {
                    ap.tell_serialised(
                        PartitioningExpMsg::DisconnectPeers(vec![disconnected_follower], None),
                        self,
                    )
                    .expect("Should serialise!");
                }
            }
        }
        self.recover_periodic_partition = !self.recover_periodic_partition;
    }
}

ignore_lifecycle!(Client);

impl Actor for Client {
    type Message = LocalClientMessage;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        match msg {
            LocalClientMessage::Run => {
                self.state = ExperimentState::Running;
                assert_ne!(self.current_leader, 0);
                #[cfg(feature = "track_timestamps")]
                {
                    self.leader_changes.push((SystemTime::now(), (self.current_leader, self.leader_round)));

                }
                #[cfg(feature = "periodic_client_logging")]
                {
                    self.schedule_periodic(WINDOW_DURATION, WINDOW_DURATION, move |c, _| {
                        info!(
                            c.ctx.log(),
                            "Num responses: {}, leader: {}, ballot/term: {}",
                            c.responses.len(),
                            c.current_leader,
                            c.leader_round
                        );
                        Handled::Ok
                    });
                }
                let timer =
                    self.schedule_periodic(WINDOW_DURATION, WINDOW_DURATION, move |c, _| {
                        c.windows.push(c.responses.len());
                        Handled::Ok
                    });
                self.window_timer = Some(timer);
                self.send_concurrent_proposals();
                #[cfg(feature = "simulate_partition")]
                {
                    let partition_ts = self.ctx.config()["partition_experiment"]["partition_ts"]
                        .as_duration()
                        .expect("No partition ts!");
                    self.schedule_once(partition_ts, move |c, _| {
                        c.create_partition();
                        Handled::Ok
                    });
                }
            }
            LocalClientMessage::Stop(a) => {
                let pending_proposals = std::mem::take(&mut self.pending_proposals);
                for proposal_meta in pending_proposals {
                    self.cancel_timer(proposal_meta.1.timer);
                }
                self.send_stop();
                self.stop_ask = Some(a);
            }
        }
        Handled::Ok
    }

    fn receive_network(&mut self, m: NetMessage) -> Handled {
        let NetMessage {
            sender: _,
            receiver: _,
            data,
        } = m;
        match_deser! {data {
            msg(am): AtomicBroadcastMsg [using AtomicBroadcastDeser] => {
                // info!(self.ctx.log(), "Handling {:?}", am);
                match am {
                    AtomicBroadcastMsg::Leader(pid, round) if self.state == ExperimentState::Setup => {
                        assert!(pid > 0);
                        self.current_leader = pid;
                        self.leader_round = round;
                        if cfg!(feature = "preloaded_log") {
                            return Handled::Ok; // wait until all preloaded responses before decrementing leader latch
                        } else {
                            match self.leader_election_latch.decrement() {
                                Ok(_) => info!(self.ctx.log(), "Got first leader: {}, ballot/term: {}. Current config: {:?}. Payload size: {:?}", pid, self.leader_round, self.current_config, DATA_SIZE),
                                Err(e) => if e != CountdownError::AlreadySet {
                                    panic!("Failed to decrement election latch: {:?}", e);
                                }
                            }
                        }
                    }
                    AtomicBroadcastMsg::Leader(pid, round) if self.state == ExperimentState::Running && round > self.leader_round => {
                        self.leader_round = round;
                        if self.current_leader != pid {
                            self.current_leader = pid;
                            self.leader_changes.push((SystemTime::now(), (self.current_leader, self.leader_round)));
                        }
                    },
                    AtomicBroadcastMsg::ReconfigurationResp(rr) => {
                        self.handle_reconfig_response(rr);
                    }
                    AtomicBroadcastMsg::ProposalResp(pr) => {
                        if self.state == ExperimentState::Finished { return Handled::Ok; }
                        let data = pr.data;
                        let id = data.as_slice().get_u64();
                        if let Some(proposal_meta) = self.pending_proposals.remove(&id) {
                            let now = self.clock.now();
                            let latency = proposal_meta.start_time.map(|start_time| now.duration_since(start_time));
                            self.cancel_timer(proposal_meta.timer);
                            if self.current_config.contains(&pr.latest_leader) && self.leader_round < pr.leader_round{
                                // info!(self.ctx.log(), "Got leader in normal response: {}. old: {}", pr.latest_leader, self.current_leader);
                                self.leader_round = pr.leader_round;
                                if self.current_leader != pr.latest_leader {
                                    self.current_leader = pr.latest_leader;
                                    self.leader_changes.push((SystemTime::now(), (self.current_leader, self.leader_round)));
                                }
                            }
                            self.handle_normal_response(id, latency);
                            self.send_concurrent_proposals();
                        } else {
                            #[cfg(feature = "preloaded_log")] {
                                if id <= self.num_preloaded_proposals && self.state == ExperimentState::Setup {
                                    self.current_leader = pr.latest_leader;
                                    self.leader_round = pr.leader_round;
                                    self.rem_preloaded_proposals -= 1;
                                    if self.rem_preloaded_proposals == 0 {
                                        match self.leader_election_latch.decrement() {
                                            Ok(_) => info!(self.ctx.log(), "Got all preloaded responses. Decrementing leader latch. leader: {}. Current config: {:?}. Payload size: {:?}", self.current_leader, self.current_config, DATA_SIZE),
                                            Err(e) => if e != CountdownError::AlreadySet {
                                                panic!("Failed to decrement election latch: {:?}", e);
                                            }
                                        }
                                    }
                                }
                            }
                            #[cfg(feature = "track_timeouts")] {
                                if self.timeouts.contains(&id) {
                                    /*if self.late_responses.is_empty() {
                                        info!(self.ctx.log(), "Got first late response: {}", id);
                                    }*/
                                    self.late_responses.push(id);
                                }
                            }
                        }
                    },
                    AtomicBroadcastMsg::Proposal(p) => {    // node piggybacked proposal i.e. proposal failed
                        let id = p.data.as_slice().get_u64();
                        if !self.responses.contains_key(&id) {
                            self.num_retried += 1;
                            self.propose_normal(id);
                        }
                    }
                    _ => {},
                }
            },
            msg(stop): NetStopMsg [using StopMsgDeser] => {
                if let NetStopMsg::Peer(pid) = stop {
                    self.nodes.remove(&pid).unwrap_or_else(|| panic!("Got stop from unknown pid {}", pid));
                    if self.nodes.is_empty() {
                        self.reply_stop_ask();
                    }
                }
            },
            err(e) => error!(self.ctx.log(), "{}", &format!("Client failed to deserialise msg: {:?}", e)),
            default(_) => unimplemented!("Should be either AtomicBroadcastMsg or NetStopMsg"),
        }
        }
        Handled::Ok
    }
}

pub fn create_raw_proposal(id: u64) -> Vec<u8> {
    let mut data: Vec<u8> = Vec::with_capacity(DATA_SIZE - PROPOSAL_ID_SIZE);
    data.put_u64(id);
    data.put_slice(&PAYLOAD);
    data
}
