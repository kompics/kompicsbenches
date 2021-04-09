use super::messages::{
    AtomicBroadcastDeser, AtomicBroadcastMsg, Proposal, StopMsg as NetStopMsg, StopMsgDeser,
    RECONFIG_ID,
};
use crate::bench::atomic_broadcast::{
    atomic_broadcast::ReconfigurationPolicy,
    messages::{ReconfigurationProposal, ReconfigurationResp},
};
use hashbrown::HashMap;
use kompact::prelude::*;
#[cfg(feature = "track_timestamps")]
use quanta::{Clock, Instant};
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};
use synchronoise::{event::CountdownError, CountdownEvent};

const STOP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq)]
enum ExperimentState {
    LeaderElection,
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
    #[cfg(feature = "track_timeouts")]
    timeouts: Vec<u64>,
    #[cfg(feature = "track_timeouts")]
    late_responses: Vec<u64>,
    #[cfg(feature = "track_timestamps")]
    clock: Clock,
    #[cfg(feature = "track_timestamps")]
    start: Option<Instant>,
    #[cfg(feature = "track_timestamps")]
    timestamps: HashMap<u64, Instant>,
    #[cfg(feature = "track_timestamps")]
    leader_changes_t: Vec<Instant>,
    #[cfg(feature = "track_timestamps")]
    reconfig_start_ts: Option<Instant>,
    #[cfg(feature = "track_timestamps")]
    reconfig_end_ts: Option<Instant>,
}

impl Client {
    pub fn with(
        initial_config: Vec<u64>,
        num_proposals: u64,
        num_concurrent_proposals: u64,
        nodes: HashMap<u64, ActorPath>,
        reconfig: Option<(ReconfigurationPolicy, Vec<u64>)>,
        timeout: Duration,
        leader_election_latch: Arc<CountdownEvent>,
        finished_latch: Arc<CountdownEvent>,
    ) -> Client {
        Client {
            ctx: ComponentContext::uninitialised(),
            num_proposals,
            num_concurrent_proposals,
            nodes,
            reconfig,
            reconfig_latency: None,
            leader_election_latch,
            finished_latch,
            latest_proposal_id: 0,
            responses: HashMap::with_capacity(num_proposals as usize),
            pending_proposals: HashMap::with_capacity(num_concurrent_proposals as usize),
            retry_proposals: Vec::with_capacity(num_concurrent_proposals as usize),
            timeout,
            current_leader: 0,
            state: ExperimentState::LeaderElection,
            current_config: initial_config,
            num_timed_out: 0,
            leader_changes: vec![],
            stop_ask: None,
            #[cfg(feature = "track_timeouts")]
            timeouts: vec![],
            #[cfg(feature = "track_timeouts")]
            late_responses: vec![],
            #[cfg(feature = "track_timestamps")]
            clock: Clock::new(),
            #[cfg(feature = "track_timestamps")]
            start: None,
            #[cfg(feature = "track_timestamps")]
            timestamps: HashMap::with_capacity(num_proposals as usize),
            #[cfg(feature = "track_timestamps")]
            leader_changes_t: vec![],
            #[cfg(feature = "track_timestamps")]
            reconfig_start_ts: None,
            #[cfg(feature = "track_timestamps")]
            reconfig_end_ts: None,
        }
    }

    fn propose_normal(&self, id: u64) {
        let leader = self.nodes.get(&self.current_leader).unwrap();
        let mut data: Vec<u8> = Vec::with_capacity(8);
        data.put_u64(id);
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
        let to = if i > self.num_proposals {
            self.num_proposals
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
                self.state = ExperimentState::Finished;
                self.finished_latch
                    .decrement()
                    .expect("Failed to countdown finished latch");
                if self.num_timed_out > 0 {
                    info!(self.ctx.log(), "Got all responses with {} timeouts, Number of leader changes: {}, {:?}, Last leader was: {}", self.num_timed_out, self.leader_changes.len(), self.leader_changes, self.current_leader);
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
            #[cfg(feature = "track_timestamps")]
            {
                self.reconfig_start_ts = Some(self.clock.now());
            }
            let timer = self.schedule_once(self.timeout, move |c, _| c.reconfig_timeout());
            let proposal_meta = ProposalMetaData::with(start_time, timer);
            self.pending_proposals.insert(RECONFIG_ID, proposal_meta);
        }
    }

    fn handle_reconfig_response(&mut self, rr: ReconfigurationResp) {
        if let Some(proposal_meta) = self.pending_proposals.remove(&RECONFIG_ID) {
            let latency = proposal_meta
                .start_time
                .expect("No reconfiguration start time")
                .elapsed()
                .expect("Could not get reconfiguration duration!");
            #[cfg(feature = "track_timestamps")]
            {
                self.reconfig_end_ts = Some(self.clock.now());
            }
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
                    #[cfg(feature = "track_timestamps")]
                    {
                        self.leader_changes_t.push(self.clock.now());
                    }
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
        let latency = match proposal_meta.start_time {
            Some(start_time) => Some(
                start_time
                    .elapsed()
                    .expect("Failed to get elapsed duration"),
            ),
            _ => None,
        };
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
                let start = self.start.expect("No cached start point");
                let timestamps = ts
                    .into_iter()
                    .map(|(_, timestamp)| timestamp.duration_since(start))
                    .collect();
                let leader_changes_t = std::mem::take(&mut self.leader_changes_t);
                let pid_ts = self
                    .leader_changes
                    .iter()
                    .zip(leader_changes_t)
                    .map(|(pid, ts)| (*pid, ts.duration_since(start)))
                    .collect();
                meta_results.timestamps_leader_changes = Some((timestamps, pid_ts));

                if let Some(rs) = self.reconfig_start_ts.take() {
                    let reconfig_start = rs.duration_since(start);
                    let reconfig_end = self
                        .reconfig_end_ts
                        .expect("No reconfig end!")
                        .duration_since(start);
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
                    self.start = Some(now);
                    self.leader_changes.push(self.current_leader);
                    self.leader_changes_t.push(now);
                }
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
                    AtomicBroadcastMsg::FirstLeader(pid) => {
                        if !self.current_config.contains(&pid) { return Handled::Ok; }
                        match self.state {
                            ExperimentState::LeaderElection => {
                                assert!(pid > 0);
                                self.current_leader = pid;
                                match self.leader_election_latch.decrement() {
                                    Ok(_) => info!(self.ctx.log(), "Got first leader: {}. Current config: {:?}", pid, self.current_config),
                                    Err(e) => if e != CountdownError::AlreadySet {
                                        panic!("Failed to decrement election latch: {:?}", e);
                                    }
                                }
                            },
                            _ => {},
                        }
                    },
                    AtomicBroadcastMsg::ReconfigurationResp(rr) => {
                        self.handle_reconfig_response(rr);
                    }
                    AtomicBroadcastMsg::ProposalResp(pr) => {
                        if self.state == ExperimentState::Finished || self.state == ExperimentState::LeaderElection { return Handled::Ok; }
                        let data = pr.data;
                        // let response = Self::deserialise_response(&mut data.as_slice());
                        let id = data.as_slice().get_u64();
                        if let Some(proposal_meta) = self.pending_proposals.remove(&id) {
                            let latency = match proposal_meta.start_time {
                                Some(start_time) => Some(start_time.elapsed().expect("Failed to get elapsed duration")),
                                _ => None,
                            };
                            self.cancel_timer(proposal_meta.timer);
                            if self.current_config.contains(&pr.latest_leader) && self.current_leader != pr.latest_leader {
                                // info!(self.ctx.log(), "Got leader in normal response: {}. old: {}", pr.latest_leader, self.current_leader);
                                self.current_leader = pr.latest_leader;
                                self.leader_changes.push(pr.latest_leader);
                                #[cfg(feature = "track_timestamps")] {
                                    self.leader_changes_t.push(self.clock.now());
                                }
                            }
                            self.handle_normal_response(id, latency);
                            self.send_concurrent_proposals();
                        }
                        #[cfg(feature = "track_timeouts")] {
                            if self.timeouts.contains(&id) {
                                /*if self.late_responses.is_empty() {
                                    info!(self.ctx.log(), "Got first late response: {}", id);
                                }*/
                                self.late_responses.push(id);
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
