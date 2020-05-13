use kompact::prelude::*;
use std::sync::Arc;
use synchronoise::CountdownEvent;
use std::collections::{HashMap, HashSet};
use super::messages::{Proposal, AtomicBroadcastMsg, AtomicBroadcastSer, Run, RECONFIG_ID};
use std::time::Duration;

const PROPOSAL_TIMEOUT: u64 = 800;
const DELTA: u64 = 20;

#[derive(ComponentDefinition)]
pub struct Client {
    ctx: ComponentContext<Self>,
    num_proposals: u64,
    batch_size: u64,
    nodes: HashMap<u64, ActorPath>,
    reconfig: Option<(Vec<u64>, Vec<u64>)>,
    finished_latch: Arc<CountdownEvent>,
    responses: HashSet<u64>,
    proposal_timeouts: HashMap<u64, ScheduledTimer>,
    timeout: u64,
}

impl Client {
    pub fn with(
        num_proposals: u64,
        batch_size: u64,
        nodes: HashMap<u64, ActorPath>,
        reconfig: Option<(Vec<u64>, Vec<u64>)>,
        finished_latch: Arc<CountdownEvent>,
    ) -> Client {
        Client {
            ctx: ComponentContext::new(),
            num_proposals,
            batch_size,
            nodes,
            reconfig,
            finished_latch,
            responses: HashSet::with_capacity(num_proposals as usize),
            proposal_timeouts: HashMap::with_capacity(batch_size as usize),
            timeout: PROPOSAL_TIMEOUT
        }
    }

    fn propose_normal(&mut self, id: u64) {
        let ap = self.ctx.actor_path();
        let p = Proposal::normal(id, ap);
        let node = self.nodes.get(&1).expect("Could not find actorpath to raft node!");
        node.tell((AtomicBroadcastMsg::Proposal(p), AtomicBroadcastSer), self);
        let timeout = Duration::from_millis(self.timeout);
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
        let i = received_count + self.batch_size;
        let to = if i > self.num_proposals {
            self.num_proposals
        } else {
            i
        };
        debug!(self.ctx.log(), "Sending: {}-{}", from, to);
        self.send_normal_proposals(from..=to);
    }

    fn propose_reconfiguration(&mut self) {
        let ap = self.ctx.actor_path();
        let reconfig = self.reconfig.take().unwrap();
        debug!(self.ctx.log(), "{}", format!("Sending reconfiguration: {:?}", reconfig));
        let p = Proposal::reconfiguration(RECONFIG_ID, ap, reconfig.clone());
        // TODO send proposal to who?
        let raft_node = self.nodes.get(&1).expect("Could not find actorpath to raft node!");
        raft_node.tell((AtomicBroadcastMsg::Proposal(p), AtomicBroadcastSer), self);
        let timeout = Duration::from_millis(self.timeout);
        let timer = self.schedule_once(timeout, move |c, _| c.retry_reconfig(reconfig));
        self.proposal_timeouts.insert(RECONFIG_ID, timer);
    }

    fn retry_proposal(&mut self, id: u64) {
        if !self.responses.contains(&id) {
            self.propose_normal(id);
        }
    }

    fn retry_reconfig(&mut self, reconfig: (Vec<u64>, Vec<u64>)) {
        if let Some(_) = self.proposal_timeouts.remove(&RECONFIG_ID) {
            debug!(self.ctx.log(), "TIMEOUT RECONFIGURING");
            let p = Proposal::reconfiguration(RECONFIG_ID, self.ctx.actor_path(), reconfig.clone());
            let raft_node = self.nodes.get(&1).expect("Could not find actorpath to raft node!");
            raft_node.tell((AtomicBroadcastMsg::Proposal(p), AtomicBroadcastSer), self);
            let timeout = Duration::from_millis(self.timeout);
            let timer = self.schedule_once(timeout, move |c, _| c.retry_reconfig(reconfig));
            self.proposal_timeouts.insert(RECONFIG_ID, timer);
        }
    }
}

impl Provide<ControlPort> for Client {
    fn handle(&mut self, _event: <ControlPort as Port>::Request) -> () {
        // ignore
    }
}

impl Actor for Client {
    type Message = Run;

    fn receive_local(&mut self, _msg: Self::Message) -> () {
        self.send_batch();
    }

    fn receive_network(&mut self, m: NetMessage) -> () {
        let NetMessage{sender: _, receiver: _, data} = m;
        match_deser!{data; {
            am: AtomicBroadcastMsg [AtomicBroadcastSer] => {
                match am {
                    AtomicBroadcastMsg::ProposalResp(pr) => {
                        if pr.succeeded {
                            match pr.id {
                                RECONFIG_ID => {
                                    if let Some(timer) = self.proposal_timeouts.remove(&RECONFIG_ID) {
                                        self.cancel_timer(timer);
                                    }
                                    info!(self.ctx.log(), "reconfiguration succeeded?");
                                },
                                _ => {
                                    if self.responses.insert(pr.id) {
                                        if pr.id % 2000 == 0 {  // TODO REMOVE
                                            debug!(self.ctx.log(), "ProposalResp: {}", pr.id);
                                        }
                                        if let Some(timer) = self.proposal_timeouts.remove(&pr.id) {
                                            self.cancel_timer(timer);
                                        }
                                        let received_count = self.responses.len() as u64;
                                        if received_count == self.num_proposals {
                                            self.finished_latch.decrement().expect("Failed to countdown finished latch");
                                        } else {
                                            if received_count == self.num_proposals/2 && self.reconfig.is_some(){
                                                self.propose_reconfiguration();
                                            }
                                            if received_count % self.batch_size == 0 {
                                                self.send_batch();
                                            }
                                        }
                                    } else {    // duplicate response
                                        self.timeout = self.timeout + self.timeout/DELTA;
                                    }
                                }
                            }
                        } else {    // failed proposal
                            debug!(self.ctx.log(), "{}", format!("Received failed proposal response: {}", pr.id));
                            let id = pr.id;
                            if let Some(timer) = self.proposal_timeouts.remove(&id) {
                                self.cancel_timer(timer);
                            }
                            let timeout = Duration::from_millis(self.timeout);
                            let timer = self.schedule_once(timeout, move |c, _| c.retry_proposal(id));
                            self.proposal_timeouts.insert(pr.id, timer);
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
        batch_size: u64,
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
            batch_size: u64,
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
                batch_size,
                nodes,
                reconfig,
                test_results: tr,
                finished_promise,
                check_sequences,
                responses: HashSet::with_capacity(num_proposals as usize),
                proposal_timeouts: HashMap::with_capacity(batch_size as usize),
            }
        }

        fn propose_normal(&mut self, id: u64) {
            let ap = self.ctx.actor_path();
            let p = Proposal::normal(id, ap);
            let node = self.nodes.get(&1).expect("Could not find actorpath to raft node!");
            node.tell((AtomicBroadcastMsg::Proposal(p), AtomicBroadcastSer), self);
            let timer = self.schedule_once(PROPOSAL_TIMEOUT, move |c, _| c.retry_proposal(id));
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
            let i = received_count + self.batch_size;
            let to = if i > self.num_proposals {
                self.num_proposals
            } else {
                i
            };
            info!(self.ctx.log(), "Sending: {}-{}", from, to);
            self.send_normal_proposals(from..=to);
        }

        fn propose_reconfiguration(&mut self) {
            let ap = self.ctx.actor_path();
            let reconfig = self.reconfig.take().unwrap();
            info!(self.ctx.log(), "{}", format!("Sending reconfiguration: {:?}", reconfig));
            let p = Proposal::reconfiguration(0, ap, reconfig);
            let raft_node = self.nodes.get(&1).expect("Could not find actorpath to raft node!");
            raft_node.tell((AtomicBroadcastMsg::Proposal(p), AtomicBroadcastSer), self);
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
            am: AtomicBroadcastMsg [AtomicBroadcastSer] => {
                match am {
                    AtomicBroadcastMsg::ProposalResp(pr) => {
                        if pr.succeeded {
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
                                        } else if received_count % self.batch_size == 0 {
                                            if received_count >= self.num_proposals/2 && self.reconfig.is_some() {
                                                self.propose_reconfiguration();
                                            }
                                            self.send_batch();
                                        }
                                    }
                                }
                            }
                        } else {    // failed proposal
                            // info!(self.ctx.log(), "{}", format!("Received failed proposal response: {}", pr.id));
                            let id = pr.id;
                            if let Some(timer) = self.proposal_timeouts.remove(&id) {
                                self.cancel_timer(timer);
                            }
                            let timer = self.schedule_once(PROPOSAL_TIMEOUT, move |c, _| c.retry_proposal(id));
                            self.proposal_timeouts.insert(pr.id, timer);
                        }
                    },
                    _ => error!(self.ctx.log(), "Client received unexpected msg"),
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