use super::messages::{
    AtomicBroadcastDeser, AtomicBroadcastMsg, Proposal, StopMsg as NetStopMsg, StopMsgDeser,
    RECONFIG_ID,
};
use hashbrown::HashMap;
use kompact::prelude::*;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use synchronoise::event::CountdownError;
use synchronoise::CountdownEvent;

#[derive(PartialEq)]
enum ExperimentState {
    LeaderElection,
    Running,
    ProposedReconfiguration,
    ReconfigurationElection,
    Finished,
}

#[derive(Debug)]
pub enum LocalClientMessage {
    Run,
    Stop(Ask<(), MetaResults>), // (num_timed_out, latency)
}

enum Response {
    Normal(u64),
    Reconfiguration(Vec<u64>),
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
    pub latencies: Vec<(u64, Duration)>,
}

impl MetaResults {
    pub fn with(num_timed_out: u64, latencies: Vec<(u64, Duration)>) -> Self {
        MetaResults {
            num_timed_out,
            latencies,
        }
    }
}

#[derive(ComponentDefinition)]
pub struct Client {
    ctx: ComponentContext<Self>,
    num_proposals: u64,
    num_concurrent_proposals: u64,
    nodes: HashMap<u64, ActorPath>,
    reconfig: Option<(Vec<u64>, Vec<u64>)>,
    leader_election_latch: Arc<CountdownEvent>,
    finished_latch: Arc<CountdownEvent>,
    latest_proposal_id: u64,
    responses: HashMap<u64, Option<Duration>>,
    pending_proposals: HashMap<u64, ProposalMetaData>,
    timeout: Duration,
    current_leader: u64,
    state: ExperimentState,
    current_config: Vec<u64>,
    num_timed_out: u64,
    leader_changes: Vec<u64>,
    first_proposal_after_reconfig: Option<u64>,
    retry_proposals: Vec<(u64, Option<SystemTime>)>,
    stop_ask: Option<Ask<(), MetaResults>>,
    #[cfg(feature = "track_timeouts")]
    timeouts: Vec<u64>,
    #[cfg(feature = "track_timeouts")]
    late_responses: Vec<u64>,
}

impl Client {
    pub fn with(
        initial_config: Vec<u64>,
        num_proposals: u64,
        num_concurrent_proposals: u64,
        nodes: HashMap<u64, ActorPath>,
        reconfig: Option<(Vec<u64>, Vec<u64>)>,
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
            leader_election_latch,
            finished_latch,
            latest_proposal_id: 0,
            responses: HashMap::with_capacity(num_proposals as usize),
            pending_proposals: HashMap::with_capacity(num_concurrent_proposals as usize),
            timeout,
            current_leader: 0,
            state: ExperimentState::LeaderElection,
            current_config: initial_config,
            num_timed_out: 0,
            leader_changes: vec![],
            first_proposal_after_reconfig: None,
            retry_proposals: Vec::with_capacity(num_concurrent_proposals as usize),
            stop_ask: None,
            #[cfg(feature = "track_timeouts")]
            timeouts: vec![],
            #[cfg(feature = "track_timeouts")]
            late_responses: vec![],
        }
    }

    fn propose_normal(&self, id: u64, node: &ActorPath) {
        let mut data: Vec<u8> = Vec::with_capacity(8);
        data.put_u64(id);
        let p = Proposal::normal(data);
        node.tell_serialised(AtomicBroadcastMsg::Proposal(p), self)
            .expect("Should serialise Proposal");
    }

    fn propose_reconfiguration(&self, node: &ActorPath) {
        let reconfig = self.reconfig.as_ref().unwrap();
        debug!(
            self.ctx.log(),
            "{}",
            format!("Sending reconfiguration: {:?}", reconfig)
        );
        let mut data: Vec<u8> = Vec::with_capacity(8);
        data.put_u64(RECONFIG_ID);
        let p = Proposal::reconfiguration(data, reconfig.clone());
        node.tell_serialised(AtomicBroadcastMsg::Proposal(p), self)
            .expect("Should serialise reconfig Proposal");
        #[cfg(feature = "track_timeouts")]
        {
            info!(self.ctx.log(), "Proposed reconfiguration. latest_proposal_id: {}, timed_out: {}, pending proposals: {}, min: {:?}, max: {:?}",
                self.latest_proposal_id, self.num_timed_out, self.pending_proposals.len(), self.pending_proposals.keys().min(), self.pending_proposals.keys().max());
        }
    }

    fn send_concurrent_proposals(&mut self) {
        let num_inflight = self.pending_proposals.len() as u64;
        assert!(num_inflight <= self.num_concurrent_proposals);
        let available_n = self.num_concurrent_proposals - num_inflight;
        if num_inflight == self.num_concurrent_proposals || self.current_leader == 0 {
            return;
        }
        let leader = self.nodes.get(&self.current_leader).unwrap().clone();
        if self.retry_proposals.is_empty() {
            // normal case
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
            let cache_start_time = self.num_concurrent_proposals == 1
                || self.state == ExperimentState::ProposedReconfiguration;
            for id in from..=to {
                let current_time = match cache_start_time {
                    true => Some(SystemTime::now()),
                    _ => None,
                };
                self.propose_normal(id, &leader);
                let timer = self.schedule_once(self.timeout, move |c, _| c.proposal_timeout(id));
                let proposal_meta = ProposalMetaData::with(current_time, timer);
                self.pending_proposals.insert(id, proposal_meta);
            }
            self.latest_proposal_id = to;
        } else {
            let num_retry_proposals = self.retry_proposals.len();
            let n = if num_retry_proposals > available_n as usize {
                available_n as usize
            } else {
                num_retry_proposals
            };
            let retry_proposals: Vec<_> = self.retry_proposals.drain(0..n).collect();
            #[cfg(feature = "track_timeouts")]
            {
                let min = retry_proposals.iter().min();
                let max = retry_proposals.iter().max();
                let count = retry_proposals.len();
                let num_pending = self.pending_proposals.len();
                info!(self.ctx.log(), "Retrying proposals to node {}. Count: {}, min: {:?}, max: {:?}, num_pending: {}", self.current_leader, count, min, max, num_pending);
            }
            for (id, start_time) in retry_proposals {
                self.propose_normal(id, &leader);
                let timer = self.schedule_once(self.timeout, move |c, _| c.proposal_timeout(id));
                let meta = ProposalMetaData::with(start_time, timer);
                self.pending_proposals.insert(id, meta);
            }
        }
    }

    fn handle_normal_response(&mut self, id: u64, latency_res: Option<Duration>) {
        self.responses.insert(id, latency_res);
        let received_count = self.responses.len() as u64;
        if received_count == self.num_proposals && self.reconfig.is_none() {
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
        } else if received_count == self.num_proposals / 2 && self.reconfig.is_some() {
            if let Some(leader) = self.nodes.get(&self.current_leader) {
                self.propose_reconfiguration(&leader);
                #[cfg(feature = "track_reconfig_latency")]
                {
                    self.state = ExperimentState::ProposedReconfiguration;
                }
            }
            let timer =
                self.schedule_once(self.timeout, move |c, _| c.proposal_timeout(RECONFIG_ID));
            let proposal_meta = ProposalMetaData::with(None, timer);
            self.pending_proposals.insert(RECONFIG_ID, proposal_meta);
        }
    }

    fn proposal_timeout(&mut self, id: u64) -> Handled {
        if self.responses.contains_key(&id)
            || self.state == ExperimentState::ReconfigurationElection
        {
            return Handled::Ok;
        }
        // info!(self.ctx.log(), "Timed out proposal {}", id);
        if id == RECONFIG_ID {
            if let Some(leader) = self.nodes.get(&self.current_leader) {
                self.propose_reconfiguration(leader);
                #[cfg(feature = "track_reconfig_latency")]
                {
                    self.state = ExperimentState::ProposedReconfiguration;
                }
            }
            let timer = self.schedule_once(self.timeout, move |c, _| c.proposal_timeout(id));
            let proposal_meta = self.pending_proposals.get_mut(&id).expect(&format!(
                "Could not find pending proposal id {}, latest_proposal_id: {}",
                id, self.latest_proposal_id
            ));
            proposal_meta.set_timer(timer);
        } else {
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
        }
        Handled::Ok
    }

    fn hold_back_proposals(&mut self, from: u64, to: u64) {
        for i in from..=to {
            let ProposalMetaData { start_time, timer } =
                self.pending_proposals.remove(&i).expect(&format!(
                    "No proposal with id {} in pending_proposals to hold back",
                    i
                ));
            self.cancel_timer(timer);
            self.retry_proposals.push((i, start_time));
        }
    }

    fn deserialise_response(data: &mut dyn Buf) -> Response {
        match data.get_u64() {
            RECONFIG_ID => {
                let len = data.get_u32();
                let mut config = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    config.push(data.get_u64());
                }
                Response::Reconfiguration(config)
            }
            n => Response::Normal(n),
        }
    }

    fn send_stop(&self) {
        for ap in self.nodes.values() {
            ap.tell_serialised(NetStopMsg::Client, self)
                .expect("Failed to send Client stop");
        }
    }

    fn reply_stop_ask(&mut self) {
        let l = std::mem::take(&mut self.responses);
        let mut v: Vec<_> = l
            .into_iter()
            .filter(|(_, latency)| latency.is_some())
            .collect();
        v.sort();
        let latencies: Vec<(u64, Duration)> = v
            .into_iter()
            .map(|(id, latency)| (id, latency.unwrap()))
            .collect();
        let meta_results = MetaResults::with(self.num_timed_out, latencies);
        self.stop_ask
            .take()
            .expect("No stop promise!")
            .reply(meta_results)
            .expect("Failed to reply write latency file!");
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
        match_deser! {data; {
            am: AtomicBroadcastMsg [AtomicBroadcastDeser] => {
                // info!(self.ctx.log(), "Handling {:?}", am);
                match am {
                    AtomicBroadcastMsg::FirstLeader(pid) => {
                        if !self.current_config.contains(&pid) { return Handled::Ok; }
                        match self.state {
                            ExperimentState::LeaderElection => {
                                self.current_leader = pid;
                                match self.leader_election_latch.decrement() {
                                    Ok(_) => info!(self.ctx.log(), "Got first leader: {}", pid),
                                    Err(e) => if e != CountdownError::AlreadySet {
                                        panic!("Failed to decrement election latch: {:?}", e);
                                    }
                                }
                            },
                            ExperimentState::ReconfigurationElection => {
                                if self.current_leader != pid {
                                    // info!(self.ctx.log(), "Got leader in ReconfigElection: {}. old: {}", pid, self.current_leader);
                                    self.current_leader = pid;
                                    self.leader_changes.push(pid);
                                }
                                self.state = ExperimentState::Running;
                                if self.retry_proposals.is_empty() {
                                    self.first_proposal_after_reconfig = Some(self.latest_proposal_id);
                                }
                                self.send_concurrent_proposals();
                            },
                            _ => {},
                        }
                    },
                    AtomicBroadcastMsg::ProposalResp(pr) => {
                        if self.state == ExperimentState::Finished || self.state == ExperimentState::LeaderElection { return Handled::Ok; }
                        let data = pr.data;
                        let response = Self::deserialise_response(&mut data.as_slice());
                        match response {
                            Response::Normal(id) => {
                                if let Some(proposal_meta) = self.pending_proposals.remove(&id) {
                                    let latency = match proposal_meta.start_time {
                                        Some(start_time) => Some(start_time.elapsed().expect("Failed to get elapsed duration")),
                                        _ => None,
                                    };
                                    self.cancel_timer(proposal_meta.timer);
                                    if self.current_config.contains(&pr.latest_leader) && self.current_leader != pr.latest_leader && self.state != ExperimentState::ReconfigurationElection {
                                        // info!(self.ctx.log(), "Got leader in normal response: {}. old: {}", pr.latest_leader, self.current_leader);
                                        self.current_leader = pr.latest_leader;
                                        self.leader_changes.push(pr.latest_leader);
                                    }
                                    self.handle_normal_response(id, latency);
                                    if self.state != ExperimentState::ReconfigurationElection {
                                        self.send_concurrent_proposals();
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
                            Response::Reconfiguration(new_config) => {
                                if let Some(proposal_meta) = self.pending_proposals.remove(&RECONFIG_ID) {
                                    self.cancel_timer(proposal_meta.timer);
                                    if self.responses.len() as u64 == self.num_proposals {
                                        self.state = ExperimentState::Finished;
                                        self.finished_latch.decrement().expect("Failed to countdown finished latch");
                                        info!(self.ctx.log(), "Got reconfig at last. {} proposals timed out. Leader changes: {}, {:?}, Last leader was: {}", self.num_timed_out, self.leader_changes.len(), self.leader_changes, self.current_leader);
                                    } else {
                                        self.reconfig = None;
                                        self.current_config = new_config;
                                        let leader_changed = self.current_leader != pr.latest_leader;
                                        info!(self.ctx.log(), "Reconfig OK, leader: {}, old: {}, current_config: {:?}", pr.latest_leader, self.current_leader, self.current_config);
                                        self.current_leader = pr.latest_leader;
                                        if self.current_leader == 0 {   // Paxos or Raft-remove-leader: wait for leader in new config
                                            self.state = ExperimentState::ReconfigurationElection;
                                        } else {    // Raft: continue if there is a leader
                                            self.state = ExperimentState::Running;
                                            self.send_concurrent_proposals();
                                            if leader_changed {
                                                self.leader_changes.push(pr.latest_leader);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    },
                    AtomicBroadcastMsg::PendingReconfiguration(data) => {
                        let dropped_proposal = data.as_slice().get_u64();
                        match self.state {
                            ExperimentState::Running => {
                                if self.reconfig.is_some() {    // still running in old config
                                    self.hold_back_proposals(dropped_proposal, self.latest_proposal_id);
                                    self.state = ExperimentState::ReconfigurationElection;  // wait for FirstLeader in new configuration before proposing more
                                } else {    // already running in new configuration
                                    match self.first_proposal_after_reconfig {
                                        Some(first_id) => {
                                            self.hold_back_proposals(dropped_proposal, first_id);
                                            self.send_concurrent_proposals();
                                        }
                                        _ => unreachable!("Running in new configuration but never cached first proposal in new config!?"),
                                    }
                                }
                            }
                            ExperimentState::ReconfigurationElection => {
                                self.hold_back_proposals(dropped_proposal, self.latest_proposal_id);
                            }
                            _ => {}
                        }
                    }
                    _ => error!(self.ctx.log(), "Client received unexpected msg"),
                }
            },
            stop: NetStopMsg [StopMsgDeser] => {
                if let NetStopMsg::Peer(pid) = stop {
                    self.nodes.remove(&pid).expect(&format!("Got stop from unknown pid {}", pid));
                    if self.nodes.is_empty() {
                        self.reply_stop_ask();
                    }
                }
            },
            !Err(e) => error!(self.ctx.log(), "{}", &format!("Client failed to deserialise msg: {:?}", e)),
        }
        }
        Handled::Ok
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::bench::atomic_broadcast::messages::{TestMessage, TestMessageSer};

    #[derive(ComponentDefinition)]
    pub struct TestClient {
        ctx: ComponentContext<Self>,
        num_proposals: u64,
        concurrent_proposals: u64,
        nodes: HashMap<u64, ActorPath>,
        reconfig: Option<(Vec<u64>, Vec<u64>)>,
        test_results: HashMap<u64, Vec<u64>>,
        finished_promise: KPromise<HashMap<u64, Vec<u64>>>,
        check_sequences: bool,
        responses: HashSet<u64>,
        proposal_timeouts: HashMap<u64, ScheduledTimer>,
    }

    impl TestClient {
        pub fn with(
            num_proposals: u64,
            concurrent_proposals: u64,
            nodes: HashMap<u64, ActorPath>,
            reconfig: Option<(Vec<u64>, Vec<u64>)>,
            finished_promise: KPromise<HashMap<u64, Vec<u64>>>,
            check_sequences: bool,
        ) -> TestClient {
            let mut tr = HashMap::new();
            tr.insert(0, Vec::with_capacity(num_proposals as usize));
            TestClient {
                ctx: ComponentContext::uninitialised(),
                num_proposals,
                concurrent_proposals,
                nodes,
                reconfig,
                test_results: tr,
                finished_promise,
                check_sequences,
                responses: HashSet::with_capacity(num_proposals as usize),
                proposal_timeouts: HashMap::with_capacity(concurrent_proposals as usize),
            }
        }

        fn propose_normal(&mut self, id: u64) {
            let p = Proposal::normal(id);
            let node = self
                .nodes
                .get(&1)
                .expect("Could not find actorpath to raft node!");
            node.tell_serialised(AtomicBroadcastMsg::Proposal(p), self)
                .expect("Should serialise proposal");
            let timer = self.schedule_once(timeout, move |c, _| c.retry_proposal(id));
            self.proposal_timeouts.insert(id, timer);
        }

        fn send_normal_proposals<T>(&mut self, r: T)
        where
            T: IntoIterator<Item = u64>,
        {
            for id in r {
                self.propose_normal(id);
            }
        }

        fn send_batch(&mut self) {
            let received_count = self.responses.len() as u64;
            let from = received_count + 1;
            let i = received_count + self.concurrent_proposals;
            let to = if i > self.num_proposals {
                self.num_proposals
            } else {
                i
            };
            self.send_normal_proposals(from..=to);
        }

        fn propose_reconfiguration(&mut self) {
            let reconfig = self.reconfig.take().unwrap();
            info!(
                self.ctx.log(),
                "{}",
                format!("Sending reconfiguration: {:?}", reconfig)
            );
            let p = Proposal::reconfiguration(0, reconfig);
            let raft_node = self
                .nodes
                .get(&1)
                .expect("Could not find actorpath to raft node!");
            raft_node
                .tell_serialised(AtomicBroadcastMsg::Proposal(p), self)
                .expect("Should serialise reconfig proposal");
        }

        fn retry_proposal(&mut self, id: u64) {
            if !self.responses.contains(&id) {
                self.propose_normal(id);
            }
        }
    }

    impl Provide<ControlPort> for TestClient {
        fn handle(&mut self, event: <ControlPort as Port>::Request) -> Handled {
            match event {
                ControlEvent::Start => info!(self.ctx.log(), "Started bcast client"),
                _ => {} //ignore
            }
            Handled::Ok
        }
    }

    impl Actor for TestClient {
        type Message = Run;

        fn receive_local(&mut self, _msg: Self::Message) -> Handled {
            // info!(self.ctx.log(), "CLIENT ACTORPATH={:?}", self.ctx.actor_path());
            self.send_batch();
            self.schedule_periodic(
                Duration::from_secs(5),
                Duration::from_secs(30),
                move |c, _| {
                    info!(
                        c.ctx.log(),
                        "Client: received: {}/{}",
                        c.responses.len(),
                        c.num_proposals
                    )
                },
            );
            Handled::Ok
        }

        fn receive_network(&mut self, msg: NetMessage) -> Handled {
            let NetMessage {
                sender: _,
                receiver: _,
                data,
            } = msg;
            match_deser! {data; {
                    am: AtomicBroadcastMsg [AtomicBroadcastDeser] => {
                        match am {
                            AtomicBroadcastMsg::ProposalResp(pr) => {
                                    match pr.id {
                                        RECONFIG_ID => {
                                            info!(self.ctx.log(), "reconfiguration succeeded?");
                                            if self.responses.len() as u64 == self.num_proposals {
                                                if self.check_sequences {
                                                    info!(self.ctx.log(), "TestClient requesting sequences");
                                                    for (_, actorpath) in &self.nodes {  // get sequence of ALL (past or present) nodes
                                                        actorpath.tell((TestMessage::SequenceReq, TestMessageSer), self);
                                                    }
                                                } else {
                                                    self.finished_promise
                                                        .to_owned()
                                                        .fulfil(self.test_results.clone())
                                                        .expect("Failed to fulfill finished promise after successful reconfiguration");
                                                }
                                            } /*else {
                                                self.send_batch();
                                            }*/
                                        },
                                        _ => {
                                            if self.responses.insert(pr.id) {
                                                if pr.id % 100 == 0 {
                                                    info!(self.ctx.log(), "Got succeeded proposal {}", pr.id);
                                                }
                                                if let Some(timer) = self.proposal_timeouts.remove(&pr.id) {
                                                    self.cancel_timer(timer);
                                                }
                                                let decided_sequence = self.test_results.get_mut(&0).unwrap();
                                                decided_sequence.push(pr.id);
                                                let received_count = self.responses.len() as u64;
            //                                    info!(self.ctx.log(), "Got proposal response id: {}", &pr.id);
                                                if received_count == self.num_proposals {
                                                    if self.check_sequences {
                                                        info!(self.ctx.log(), "TestClient requesting sequences. num_responses: {}", self.responses.len());
                                                        for (_, actorpath) in &self.nodes {  // get sequence of ALL (past or present) nodes
                                                            actorpath.tell((TestMessage::SequenceReq, TestMessageSer), self);
                                                        }
                                                    } else {
                                                        self.finished_promise
                                                            .to_owned()
                                                            .fulfil(self.test_results.clone())
                                                            .expect("Failed to fulfill finished promise after getting all sequences");
                                                    }
                                                } else if received_count % self.concurrent_proposals == 0 {
                                                    if received_count >= self.num_proposals/2 && self.reconfig.is_some() {
                                                        self.propose_reconfiguration();
                                                    }
                                                    self.send_batch();
                                                }
                                            }
                                        }
                                    }
                            },
                            e => error!(self.ctx.log(), "Client received unexpected msg {:?}", e),
                        }
                        Handled::Ok
                    },
                    tm: TestMessage [TestMessageSer] => {
                        match tm {
                            TestMessage::SequenceResp(sr) => {
                                self.test_results.insert(sr.node_id, sr.sequence);
                                if (self.test_results.len()) == self.nodes.len() + 1 {    // got sequences from everybody, we're done
                                    info!(self.ctx.log(), "Got all sequences");
                                    self.finished_promise
                                        .to_owned()
                                        .fulfil(self.test_results.clone())
                                        .expect("Failed to fulfill finished promise after getting all sequences");
                                }
                            },
                            _ => error!(self.ctx.log(), "Received unexpected TestMessage {:?}", tm),
                        }
                    },
                    !Err(e) => error!(self.ctx.log(), "{}", &format!("Client failed to deserialise msg: {:?}", e)),
                }
                }
        }
    }
}
