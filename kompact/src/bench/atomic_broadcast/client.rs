use super::messages::{
    AtomicBroadcastDeser, AtomicBroadcastMsg, Proposal, StopMsg as NetStopMsg, StopMsgDeser,
    RECONFIG_ID,
};
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
    start_time: Option<SystemTime>,
    timer: ScheduledTimer,
}

impl ProposalMetaData {
    fn with(start_time: Option<SystemTime>, timer: ScheduledTimer) -> ProposalMetaData {
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
    pub leader_changes: Vec<(Instant, u64)>,
    pub windowed_results: Vec<usize>,
    pub reconfig_ts: Option<(Instant, Instant)>,
    pub timestamps: Vec<Instant>,
}

impl MetaResults {
    pub fn with(
        num_timed_out: u64,
        num_retried: u64,
        latencies: Vec<Duration>,
        leader_changes: Vec<(Instant, u64)>,
        windowed_results: Vec<usize>,
        reconfig_ts: Option<(Instant, Instant)>,
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
    state: ExperimentState,
    current_config: Vec<u64>,
    num_timed_out: u64,
    num_retried: usize,
    leader_changes: Vec<(Instant, u64)>,
    stop_ask: Option<Ask<(), MetaResults>>,
    window_timer: Option<ScheduledTimer>,
    /// timestamp, number of proposals completed
    windows: Vec<usize>,
    clock: Clock,
    start_ts: Instant,
    reconfig_start_ts: Option<Instant>,
    reconfig_end_ts: Option<Instant>,
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
        let start_ts = clock.now();
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
            state: ExperimentState::Setup,
            current_config: initial_config,
            num_timed_out: 0,
            num_retried: 0,
            leader_changes: vec![],
            stop_ask: None,
            window_timer: None,
            windows: vec![],
            clock,
            start_ts,
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
                true => Some(SystemTime::now()),
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
                self.state = ExperimentState::Finished;
                self.finished_latch
                    .decrement()
                    .expect("Failed to countdown finished latch");
                let leader_changes_pid: Vec<&u64> =
                    self.leader_changes.iter().map(|(_ts, pid)| pid).collect();
                if self.num_timed_out > 0 || self.num_retried > 0 {
                    info!(self.ctx.log(), "Got all responses with {} timeouts and {} retries. Number of leader changes: {}, {:?}, Last leader was: {}. start_ts: {}", self.num_timed_out, self.num_retried, self.leader_changes.len(), leader_changes_pid, self.current_leader, self.start_ts.as_u64());
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
                        "Got all responses. Number of leader changes: {}, {:?}, Last leader was: {}. start_ts: {}",
                        self.leader_changes.len(),
                        leader_changes_pid,
                        self.current_leader,
                        self.start_ts.as_u64()
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
            self.reconfig_start_ts = Some(self.clock.now());
            let timer = self.schedule_once(self.timeout, move |c, _| c.reconfig_timeout());
            let proposal_meta = ProposalMetaData::with(None, timer);
            self.pending_proposals.insert(RECONFIG_ID, proposal_meta);
        }
    }

    fn handle_reconfig_response(&mut self, rr: ReconfigurationResp) {
        if let Some(proposal_meta) = self.pending_proposals.remove(&RECONFIG_ID) {
            self.reconfig_end_ts = Some(self.clock.now());
            let new_config = rr.current_configuration;
            self.cancel_timer(proposal_meta.timer);
            if self.responses.len() as u64 == self.num_proposals {
                self.state = ExperimentState::Finished;
                self.finished_latch
                    .decrement()
                    .expect("Failed to countdown finished latch");
                info!(self.ctx.log(), "Got reconfig at last. {} proposals timed out. Leader changes: {}, {:?}, Last leader was: {}", self.num_timed_out, self.leader_changes.len(), self.leader_changes, self.current_leader);
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
                if rr.latest_leader > 0 && self.current_leader != rr.latest_leader {
                    self.current_leader = rr.latest_leader;
                    self.leader_changes
                        .push((self.clock.now(), rr.latest_leader));
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
}

ignore_lifecycle!(Client);

impl Actor for Client {
    type Message = LocalClientMessage;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        match msg {
            LocalClientMessage::Run => {
                self.state = ExperimentState::Running;
                assert_ne!(self.current_leader, 0);
                let now = self.clock.now();
                self.start_ts = now;
                #[cfg(feature = "track_timestamps")]
                {
                    self.leader_changes.push((now, self.current_leader));
                }
                #[cfg(feature = "periodic_client_logging")]
                {
                    self.schedule_periodic(WINDOW_DURATION, WINDOW_DURATION, move |c, _| {
                        info!(c.ctx.log(), "Num responses: {}", c.responses.len());
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
                    AtomicBroadcastMsg::Leader(pid) => {
                        assert!(pid > 0);
                        self.current_leader = pid;
                        #[cfg(feature = "preloaded_log")] {
                            if self.state == ExperimentState::Setup { // wait until all preloaded responses before decrementing leader latch
                                return Handled::Ok;
                            }
                        }
                        match self.leader_election_latch.decrement() {
                            Ok(_) => info!(self.ctx.log(), "Got first leader: {}. Current config: {:?}. Payload size: {:?}", pid, self.current_config, DATA_SIZE),
                            Err(e) => if e != CountdownError::AlreadySet {
                                panic!("Failed to decrement election latch: {:?}", e);
                            }
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
                            let latency = proposal_meta.start_time.map(|start_time| start_time.elapsed().expect("Failed to get elapsed duration"));
                            self.cancel_timer(proposal_meta.timer);
                            if self.current_config.contains(&pr.latest_leader) && self.current_leader != pr.latest_leader {
                                // info!(self.ctx.log(), "Got leader in normal response: {}. old: {}", pr.latest_leader, self.current_leader);
                                self.current_leader = pr.latest_leader;
                                self.leader_changes.push((self.clock.now(), pr.latest_leader));
                            }
                            self.handle_normal_response(id, latency);
                            self.send_concurrent_proposals();
                        } else {
                            #[cfg(feature = "preloaded_log")] {
                                if id <= self.num_preloaded_proposals && self.state == ExperimentState::Setup{
                                    self.current_leader = pr.latest_leader;
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
                    e => error!(self.ctx.log(), "Client received unexpected msg: {:?}", e),
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
