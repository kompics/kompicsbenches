use kompact::prelude::*;
use std::sync::Arc;
use synchronoise::CountdownEvent;
use hashbrown::HashMap;
use super::messages::{Proposal, AtomicBroadcastMsg, AtomicBroadcastDeser, RECONFIG_ID, StopMsg as NetStopMsg};
use std::time::{Duration, SystemTime};
use synchronoise::event::CountdownError;

#[derive(PartialEq)]
enum ExperimentState {
    LeaderElection,
    Running,
    ReconfigurationElection,
    Finished
}

#[derive(Debug)]
pub enum LocalClientMessage {
    Run,
    Stop,
    WriteLatencyFile(Ask<(), Vec<(u64, Duration)>>)
}

enum Response {
    Normal(u64),
    Reconfiguration(Vec<u64>)
}

#[derive(Debug)]
struct ProposalMetaData {
    start_time: Option<SystemTime>,
    timer: ScheduledTimer
}

impl ProposalMetaData {
    fn with(start_time: Option<SystemTime>, timer: ScheduledTimer) -> ProposalMetaData {
        ProposalMetaData{ start_time, timer }
    }

    fn set_timer(&mut self, timer: ScheduledTimer) { self.timer = timer; }
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
    retry_after_reconfig: bool,
    current_config: Vec<u64>
}

impl Client {
    pub fn with(
        initial_config: Vec<u64>,
        num_proposals: u64,
        num_concurrent_proposals: u64,
        nodes: HashMap<u64, ActorPath>,
        reconfig: Option<(Vec<u64>, Vec<u64>)>,
        timeout: u64,
        leader_election_latch: Arc<CountdownEvent>,
        finished_latch: Arc<CountdownEvent>,
        retry_after_reconfig: bool,
    ) -> Client {
        Client {
            ctx: ComponentContext::new(),
            num_proposals,
            num_concurrent_proposals,
            nodes,
            reconfig,
            leader_election_latch,
            finished_latch,
            latest_proposal_id: 0,
            responses: HashMap::with_capacity(num_proposals as usize),
            pending_proposals: HashMap::with_capacity(num_concurrent_proposals as usize),
            timeout: Duration::from_millis(timeout),
            current_leader: 0,
            state: ExperimentState::LeaderElection,
            retry_after_reconfig,
            current_config: initial_config
        }
    }

    fn propose_normal(&self, id: u64, node: &ActorPath) {
        let mut data: Vec<u8> = Vec::with_capacity(8);
        data.put_u64(id);
        let p = Proposal::normal(data);
        node.tell_serialised(AtomicBroadcastMsg::Proposal(p), self).expect("Should serialise Proposal");
    }

    fn propose_reconfiguration(&self, node: &ActorPath) {
        let reconfig = self.reconfig.as_ref().unwrap();
        debug!(self.ctx.log(), "{}", format!("Sending reconfiguration: {:?}", reconfig));
        let mut data: Vec<u8> = Vec::with_capacity(8);
        data.put_u64(RECONFIG_ID);
        let p = Proposal::reconfiguration(data, reconfig.clone());
        node.tell_serialised(AtomicBroadcastMsg::Proposal(p), self).expect("Should serialise reconfig Proposal");
    }

    fn send_concurrent_proposals(&mut self) {
        let num_inflight = self.pending_proposals.len() as u64;
        assert!(num_inflight <= self.num_concurrent_proposals);
        if self.latest_proposal_id == self.num_proposals ||  num_inflight == self.num_concurrent_proposals || self.current_leader == 0 {
            return;
        }
        let from = self.latest_proposal_id + 1;
        let i = self.latest_proposal_id + self.num_concurrent_proposals - num_inflight;
        let to = if i > self.num_proposals {
            self.num_proposals
        } else {
            i
        };
        for id in from ..= to {
            let current_time = match self.num_concurrent_proposals {
                1 => Some(SystemTime::now()),
                _ => None,
            };
            let leader = self.nodes.get(&self.current_leader).unwrap();
            self.propose_normal(id, leader);
            let timer = self.schedule_once(self.timeout, move |c, _| c.retry_proposal(id));
            let proposal_meta = ProposalMetaData::with(current_time, timer);
            self.pending_proposals.insert(id, proposal_meta);
        }
        self.state = ExperimentState::Running;
        self.latest_proposal_id = to;
    }

    fn retry_proposal(&mut self, id: u64) {
        trace!(self.ctx.log(), "Retry proposal {}?", id);
        if self.responses.contains_key(&id) { panic!("Failed to cancel timer?"); }
        if let Some(leader) = self.nodes.get(&self.current_leader) {
            match id {
                RECONFIG_ID => self.propose_reconfiguration(leader),
                _ => self.propose_normal(id, leader)
            }
            self.state = ExperimentState::Running;
        }
        let timer = self.schedule_once(self.timeout, move |c, _| c.retry_proposal(id));
        let proposal_meta = self.pending_proposals.get_mut(&id)
            .expect(&format!("Could not find pending proposal id {}, latest_proposal_id: {}", id, self.latest_proposal_id));
        proposal_meta.set_timer(timer);
    }

    fn retry_after_reconfig(&mut self, pending_proposals: &mut HashMap<u64, ProposalMetaData>) {
        let leader = self.nodes.get(&self.current_leader).unwrap().clone();
        for (id, meta) in pending_proposals {
            self.propose_normal(*id, &leader);
            let i = id.clone();
            let timer = self.schedule_once(self.timeout, move |c, _| c.retry_proposal(i));
            let old_timer = std::mem::replace(&mut meta.timer, timer);
            self.cancel_timer(old_timer);
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
            },
            n => Response::Normal(n),
        }
    }

    fn send_stop(&self) {
        for ap in self.nodes.values() {
            ap.tell_serialised(NetStopMsg::Client, self).expect("Failed to send Client stop");
        }
    }
}

impl Provide<ControlPort> for Client {
    fn handle(&mut self, event: <ControlPort as Port>::Request) -> () {
        if let ControlEvent::Kill = event {
            let pending_proposals = std::mem::take(&mut self.pending_proposals);
            for proposal_meta in pending_proposals {
                self.cancel_timer(proposal_meta.1.timer);
            }
        }
    }
}

impl Actor for Client {
    type Message = LocalClientMessage;

    fn receive_local(&mut self, msg: Self::Message) -> () {
        match msg {
            LocalClientMessage::Run => {
                self.state = ExperimentState::Running;
                assert_ne!(self.current_leader, 0);
                self.send_concurrent_proposals();
            },
            LocalClientMessage::Stop => {
                self.send_stop();
            },
            LocalClientMessage::WriteLatencyFile(ask) => {
                let l = std::mem::take(&mut self.responses);
                let mut v: Vec<_> = l.into_iter().collect();
                v.sort();
                let latencies: Vec<(u64, Duration)> = v.into_iter().map(|(id, latency)| (id, latency.unwrap()) ).collect();
                ask.reply(latencies).expect("Failed to reply write latency file!");
            }
        }
    }

    fn receive_network(&mut self, m: NetMessage) -> () {
        let NetMessage{sender: _, receiver: _, data} = m;
        match_deser!{data; {
            am: AtomicBroadcastMsg [AtomicBroadcastDeser] => {
                trace!(self.ctx.log(), "Handling {:?}", am);
                match am {
                    AtomicBroadcastMsg::FirstLeader(pid) => {
                        if !self.current_config.contains(&pid) { return; }
                        self.current_leader = pid;
                        match self.state {
                            ExperimentState::LeaderElection => {
                                info!(self.ctx.log(), "Got first leader: {}. Decrementing election latch", pid);
                                match self.leader_election_latch.decrement() {
                                    Ok(_) => {},
                                    Err(e) => if e != CountdownError::AlreadySet {
                                        panic!("Failed to decrement election latch: {:?}", e);
                                    }
                                }
                            },
                            ExperimentState::ReconfigurationElection => {
                                if self.current_leader > 0 && self.retry_after_reconfig && !self.pending_proposals.is_empty() {
                                    let mut pending_proposals = std::mem::take(&mut self.pending_proposals);
                                    self.retry_after_reconfig(&mut pending_proposals);
                                    self.pending_proposals = pending_proposals;
                                }
                                self.send_concurrent_proposals();
                            },
                            _ => {},
                        }
                    },
                    AtomicBroadcastMsg::ProposalResp(pr) => {
                        if self.state == ExperimentState::Finished || self.state == ExperimentState::LeaderElection { return; }
                        let data = pr.data;
                        let response = Self::deserialise_response(&mut data.as_slice());
                        match response {
                            Response::Normal(id) => {
                                if let Some(proposal_meta) = self.pending_proposals.remove(&id) {
                                    let latency = match self.num_concurrent_proposals {
                                        1 => {
                                            let start_time = proposal_meta.start_time.expect("No start time found!");
                                            Some(start_time.elapsed().expect("Failed to get elapsed duration"))
                                        },
                                        _ => None,
                                    };
                                    self.cancel_timer(proposal_meta.timer);
                                    self.responses.insert(id, latency);
                                    let received_count = self.responses.len() as u64;
                                    if received_count == self.num_proposals && self.reconfig.is_none() {
                                        info!(self.ctx.log(), "Got all responses");
                                        self.state = ExperimentState::Finished;
                                        self.finished_latch.decrement().expect("Failed to countdown finished latch");
                                    } else {
                                        if received_count == self.num_proposals/2 && self.reconfig.is_some(){
                                            if let Some(leader) = self.nodes.get(&self.current_leader) {
                                                self.propose_reconfiguration(&leader);
                                            }
                                            let timer = self.schedule_once(self.timeout, move |c, _| c.retry_proposal(RECONFIG_ID));
                                            let proposal_meta = ProposalMetaData::with(None, timer);
                                            self.pending_proposals.insert(RECONFIG_ID, proposal_meta);
                                        }
                                        if self.state != ExperimentState::ReconfigurationElection && self.current_config.contains(&pr.latest_leader) {
                                            self.current_leader = pr.latest_leader;
                                            self.send_concurrent_proposals();
                                        }
                                    }
                                }
                            }
                            Response::Reconfiguration(new_config) => {
                                if let Some(proposal_meta) = self.pending_proposals.remove(&RECONFIG_ID) {
                                    self.cancel_timer(proposal_meta.timer);
                                    if self.responses.len() as u64 == self.num_proposals {
                                        info!(self.ctx.log(), "Got reconfig at last");
                                        self.state = ExperimentState::Finished;
                                        self.finished_latch.decrement().expect("Failed to countdown finished latch");
                                    } else {
                                        self.reconfig = None;
                                        self.current_leader = pr.latest_leader;
                                        // info!(self.ctx.log(), "Reconfig OK, leader: {}", self.current_leader);
                                        if self.current_leader == 0 {   // Paxos or Raft-remove-leader: wait for leader in new config
                                            self.state = ExperimentState::ReconfigurationElection;
                                        } else {    // Raft: continue if there is a leader
                                            self.send_concurrent_proposals();
                                        }
                                    }
                                    // info!(self.ctx.log(), "Reconfiguration succeeded: {:?}", new_config);
                                    self.current_config = new_config;
                                }
                            }
                        }
                    }
                    _ => error!(self.ctx.log(), "Client received unexpected msg"),
                }
            },
            !Err(e) => error!(self.ctx.log(), "{}", &format!("Client failed to deserialise msg: {:?}", e)),
        }
        }
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
                ctx: ComponentContext::new(),
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
            let node = self.nodes.get(&1).expect("Could not find actorpath to raft node!");
            node.tell_serialised(AtomicBroadcastMsg::Proposal(p), self).expect("Should serialise proposal");
            let timer = self.schedule_once(timeout, move |c, _| c.retry_proposal(id));
            self.proposal_timeouts.insert(id, timer);
        }

        fn send_normal_proposals<T>(&mut self, r: T) where T: IntoIterator<Item = u64> {
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
            info!(self.ctx.log(), "{}", format!("Sending reconfiguration: {:?}", reconfig));
            let p = Proposal::reconfiguration(0, reconfig);
            let raft_node = self.nodes.get(&1).expect("Could not find actorpath to raft node!");
            raft_node.tell_serialised(AtomicBroadcastMsg::Proposal(p), self).expect("Should serialise reconfig proposal");
        }

        fn retry_proposal(&mut self, id: u64) {
            if !self.responses.contains(&id) {
                self.propose_normal(id);
            }
        }
    }

    impl Provide<ControlPort> for TestClient {
        fn handle(&mut self, event: <ControlPort as Port>::Request) -> () {
            match event {
                ControlEvent::Start => info!(self.ctx.log(), "Started bcast client"),
                _ => {}, //ignore
            }
        }
    }

    impl Actor for TestClient {
        type Message = Run;

        fn receive_local(&mut self, _msg: Self::Message) -> () {
            // info!(self.ctx.log(), "CLIENT ACTORPATH={:?}", self.ctx.actor_path());
            self.send_batch();
            self.schedule_periodic(
                Duration::from_secs(5),
                Duration::from_secs(30),
                move |c, _| info!(c.ctx.log(), "Client: received: {}/{}", c.responses.len(), c.num_proposals)
            );
        }

        fn receive_network(&mut self, msg: NetMessage) -> () {
            let NetMessage{sender: _, receiver: _, data} = msg;
            match_deser!{data; {
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
