use super::messages::{
    AtomicBroadcastDeser, AtomicBroadcastMsg, Proposal, StopMsg as NetStopMsg, StopMsgDeser,
    RECONFIG_ID,
};
use crate::bench::atomic_broadcast::{
    atomic_broadcast::ReconfigurationPolicy,
    exp_params::*,
    messages::{ReconfigurationProposal, ReconfigurationResp},
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
    pub latencies: Vec<Duration>,
    pub reconfig_latency: Option<Duration>,
    pub timestamps_leader_changes: Option<(Vec<Duration>, Vec<(u64, Duration)>)>,
    pub timestamps_reconfig: Option<(Duration, Duration)>,
}

impl MetaResults {
    pub fn with(
        num_timed_out: u64,
        latencies: Vec<Duration>,
        reconfig_latency: Option<Duration>,
        timestamps_leader_changes: Option<(Vec<Duration>, Vec<(u64, Duration)>)>,
        timestamps_reconfig: Option<(Duration, Duration)>,
    ) -> Self {
        MetaResults {
            num_timed_out,
            latencies,
            reconfig_latency,
            timestamps_leader_changes,
            timestamps_reconfig,
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
    reconfig_latency: Option<Duration>,
    leader_election_latch: Arc<CountdownEvent>,
    finished_latch: Arc<CountdownEvent>,
    latest_proposal_id: u64,
    max_proposal_id: u64,
    responses: HashMap<u64, Option<Duration>>,
    pending_proposals: HashMap<u64, ProposalMetaData>,
    retry_proposals: Vec<u64>,
    timeout: Duration,
    current_leader: u64,
    state: ExperimentState,
    current_config: Vec<u64>,
    num_timed_out: u64,
    leader_changes: Vec<u64>,
    stop_ask: Option<Ask<(), MetaResults>>,
    window_timer: Option<ScheduledTimer>,
    /// timestamp, number of proposals completed
    windows: Vec<(Instant, usize)>,
    clock: Clock,
    leader_changes_ts: Vec<Instant>,
    reconfig_start_ts: Option<Instant>,
    reconfig_end_ts: Option<Instant>,
    #[cfg(feature = "track_timeouts")]
    timeouts: Vec<u64>,
    #[cfg(feature = "track_timeouts")]
    late_responses: Vec<u64>,
    #[cfg(feature = "track_timestamps")]
    start: Instant,
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
        Client {
            ctx: ComponentContext::uninitialised(),
            num_proposals,
            num_concurrent_proposals,
            nodes,
            reconfig,
            reconfig_latency: None,
            leader_election_latch,
            finished_latch,
            latest_proposal_id: preloaded_log_size,
            max_proposal_id: num_proposals + preloaded_log_size,
            responses: HashMap::with_capacity(num_proposals as usize),
            pending_proposals: HashMap::with_capacity(num_concurrent_proposals as usize),
            retry_proposals: Vec::with_capacity(num_concurrent_proposals as usize),
            timeout,
            current_leader: 0,
            state: ExperimentState::Setup,
            current_config: initial_config,
            num_timed_out: 0,
            leader_changes: vec![],
            stop_ask: None,
            window_timer: None,
            windows: vec![],
            #[cfg(feature = "track_timestamps")]
            start: clock.now(),
            clock,
            leader_changes_ts: vec![],
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

    fn send_retry_proposals(&mut self) {
        let retry_proposals: Vec<u64> = std::mem::take(&mut self.retry_proposals);
        for id in retry_proposals {
            self.propose_normal(id);
        }
    }

    fn send_concurrent_proposals(&mut self) {
        let num_inflight = self.pending_proposals.len() as u64;
        assert!(num_inflight <= self.num_concurrent_proposals);
        if !self.retry_proposals.is_empty() {
            self.send_retry_proposals();
        }
        let available_n = self.num_concurrent_proposals - num_inflight;
        if num_inflight == self.num_concurrent_proposals || self.current_leader == 0 {
            return;
        }
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
                    self.windows
                        .push((self.clock.now(), received_count as usize));
                }
                self.state = ExperimentState::Finished;
                self.finished_latch
                    .decrement()
                    .expect("Failed to countdown finished latch");
                if self.num_timed_out > 0 {
                    info!(self.ctx.log(), "Got all responses with {} timeouts, Number of leader changes: {}, {:?}, Last leader was: {}", self.num_timed_out, self.leader_changes.len(), self.leader_changes, self.current_leader);
                    self.log_timed_results();
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
                        "Got all responses. Number of leader changes: {}, {:?}, Last leader was: {}",
                        self.leader_changes.len(),
                        self.leader_changes,
                        self.current_leader
                    );
                    self.log_timed_results();
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
            let start_time = Some(SystemTime::now());
            self.reconfig_start_ts = Some(self.clock.now());
            let timer = self.schedule_once(self.timeout, move |c, _| c.reconfig_timeout());
            let proposal_meta = ProposalMetaData::with(start_time, timer);
            self.pending_proposals.insert(RECONFIG_ID, proposal_meta);
        }
    }

    fn log_timed_results(&self) {
        let mut str = if let Some(start_ts) = self.reconfig_start_ts {
            String::from(&format!(
                "Reconfig ts: {},{}\n",
                start_ts.as_u64(),
                self.reconfig_end_ts.unwrap().as_u64()
            ))
        } else {
            String::new()
        };
        if !self.leader_changes.is_empty() {
            str.push_str("Leader changes: ");
            self.leader_changes
                .iter()
                .zip(self.leader_changes_ts.iter())
                .for_each(|(pid, ts)| str.push_str(&format!("{},{} ", ts.as_u64(), pid)));
            str.push_str("\n");
        }
        str.push_str("Decisions: ");
        let mut prev_n = 0;
        for (ts, n) in &self.windows {
            str.push_str(&format!("{},{} ", ts.as_u64(), n - prev_n));
            prev_n = *n;
        }
        info!(
            self.ctx.log(),
            "{:?} windowed results:\n{}", WINDOW_DURATION, str
        );
    }

    fn handle_reconfig_response(&mut self, rr: ReconfigurationResp) {
        if let Some(proposal_meta) = self.pending_proposals.remove(&RECONFIG_ID) {
            let latency = proposal_meta
                .start_time
                .expect("No reconfiguration start time")
                .elapsed()
                .expect("Could not get reconfiguration duration!");
            self.reconfig_end_ts = Some(self.clock.now());
            self.reconfig_latency = Some(latency);
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
                if rr.latest_leader > 0 && self.current_leader != rr.latest_leader {
                    self.current_leader = rr.latest_leader;
                    self.leader_changes.push(rr.latest_leader);
                    self.leader_changes_ts.push(self.clock.now());
                }
                info!(
                    self.ctx.log(),
                    "Reconfig OK, leader: {}, old: {}, current_config: {:?}",
                    rr.latest_leader,
                    self.current_leader,
                    self.current_config
                );
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
        let proposal_meta = self
            .pending_proposals
            .remove(&id)
            .expect("Timed out on proposal not in pending proposals");
        let latency = proposal_meta.start_time.map(|start_time| {
            start_time
                .elapsed()
                .expect("Failed to get elapsed duration")
        });
        self.handle_normal_response(id, latency);
        self.send_concurrent_proposals();
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
            #[allow(unused_mut)] // TODO remove
            let mut meta_results = MetaResults::with(
                self.num_timed_out,
                latencies,
                self.reconfig_latency.take(),
                None,
                None,
            );
            #[cfg(feature = "track_timestamps")]
            {
                let mut ts: Vec<_> = std::mem::take(&mut self.timestamps).into_iter().collect();
                ts.sort();
                let timestamps = ts
                    .into_iter()
                    .map(|(_, timestamp)| timestamp.duration_since(self.start))
                    .collect();
                let leader_changes_ts = std::mem::take(&mut self.leader_changes_ts);
                let pid_ts = self
                    .leader_changes
                    .iter()
                    .zip(leader_changes_ts)
                    .map(|(pid, ts)| (*pid, ts.duration_since(self.start)))
                    .collect();
                meta_results.timestamps_leader_changes = Some((timestamps, pid_ts));

                if let Some(rs) = self.reconfig_start_ts.take() {
                    let reconfig_start = rs.duration_since(self.start);
                    let reconfig_end = self
                        .reconfig_end_ts
                        .expect("No reconfig end!")
                        .duration_since(self.start);
                    meta_results.timestamps_reconfig = Some((reconfig_start, reconfig_end));
                }
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
                #[cfg(feature = "track_timestamps")]
                {
                    let now = self.clock.now();
                    self.start = now;
                    self.leader_changes.push(self.current_leader);
                    self.leader_changes_ts.push(now);
                }
                let timer =
                    self.schedule_periodic(WINDOW_DURATION, WINDOW_DURATION, move |c, _| {
                        c.windows.push((c.clock.now(), c.responses.len()));
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
                        if cfg!(feature = "preloaded_log") && self.state == ExperimentState::Setup {   // wait until all preloaded responses before decrementing leader latch
                            return Handled::Ok;
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
                        // let response = Self::deserialise_response(&mut data.as_slice());
                        let id = data.as_slice().get_u64();
                        if let Some(proposal_meta) = self.pending_proposals.remove(&id) {
                            let latency = proposal_meta.start_time.map(|start_time| start_time.elapsed().expect("Failed to get elapsed duration"));
                            self.cancel_timer(proposal_meta.timer);
                            if self.current_config.contains(&pr.latest_leader) && self.current_leader != pr.latest_leader {
                                // info!(self.ctx.log(), "Got leader in normal response: {}. old: {}", pr.latest_leader, self.current_leader);
                                self.current_leader = pr.latest_leader;
                                self.leader_changes.push(pr.latest_leader);
                                self.leader_changes_ts.push(self.clock.now());
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
                                            Ok(_) => info!(self.ctx.log(), "Got all preloaded responses. Decrementing leader latch. pid: {}. Current config: {:?}. Payload size: {:?}", self.current_leader, self.current_config, DATA_SIZE),
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
                        self.retry_proposals.push(id);
                        self.send_concurrent_proposals();
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
