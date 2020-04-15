use kompact::prelude::*;
use std::sync::Arc;
use synchronoise::CountdownEvent;
use std::collections::HashMap;
use super::messages::{Proposal, AtomicBroadcastMsg, AtomicBroadcastSer, Run, RECONFIG_ID};
use std::time::Duration;

const MAX_RETRIES: u32 = 50;
const RETRY_DELAY: Duration = Duration::from_millis(100);

#[derive(ComponentDefinition)]
pub struct Client {
    ctx: ComponentContext<Self>,
    num_proposals: u64,
    batch_size: u64,
    nodes: HashMap<u64, ActorPath>,
    reconfig: Option<(Vec<u64>, Vec<u64>)>,
    finished_latch: Arc<CountdownEvent>,
    received_count: u64,
    retries: HashMap<u64, u32>,
    iteration_id: u32,
}

impl Client {
    pub fn with(
        num_proposals: u64,
        batch_size: u64,
        nodes: HashMap<u64, ActorPath>,
        reconfig: Option<(Vec<u64>, Vec<u64>)>,
        finished_latch: Arc<CountdownEvent>,
        iteration_id: u32,
    ) -> Client {
        Client {
            ctx: ComponentContext::new(),
            num_proposals,
            batch_size,
            nodes,
            reconfig,
            finished_latch,
            received_count: 0,
            retries: HashMap::new(),
            iteration_id,
        }
    }

    fn propose_normal(&self, id: u64) {
        let ap = self.ctx.actor_path();
        let p = Proposal::normal(id, ap);
        let node = self.nodes.get(&1).expect("Could not find actorpath to raft node!");
        node.tell((AtomicBroadcastMsg::Proposal(p), AtomicBroadcastSer), self);
    }

    fn send_normal_proposals<T>(&self, r: T) where T: IntoIterator<Item = u64> {
        for id in r {
            self.propose_normal(id);
        }
    }

    fn propose_reconfiguration(&mut self) {
        let ap = self.ctx.actor_path();
        let reconfig = self.reconfig.take().unwrap();
        info!(self.ctx.log(), "{}", format!("Sending reconfiguration: {:?}", reconfig.clone()));
        let p = Proposal::reconfiguration(RECONFIG_ID, ap, reconfig);
        // TODO send proposal to who?
        let raft_node = self.nodes.get(&1).expect("Could not find actorpath to raft node!");
        raft_node.tell((AtomicBroadcastMsg::Proposal(p), AtomicBroadcastSer), self);
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
        if self.reconfig.is_some() {
            self.send_normal_proposals(1..self.num_proposals/2);
            std::thread::sleep(Duration::from_secs(2));
            self.propose_reconfiguration();
            // self.send_normal_proposals(self.num_proposals/2..=self.num_proposals);
        } else {
            info!(self.ctx.log(), "Sending: {}-{}", 1, self.batch_size);
            self.send_normal_proposals(1..=self.batch_size);
        }
        self.schedule_periodic(
            Duration::from_secs(5),
            Duration::from_secs(30),
            move |c, _| info!(c.ctx.log(), "Iteration {}: received: {}/{}", c.iteration_id, c.received_count, c.num_proposals)
        );
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
                                    info!(self.ctx.log(), "reconfiguration succeeded?");
                                }
                                _ => {
                                    self.received_count += 1;
//                                    info!(self.ctx.log(), "Got proposal response id: {}", &pr.id);
                                    if self.received_count == self.num_proposals {
                                        self.finished_latch.decrement().expect("Failed to countdown finished latch");
                                    } else if self.received_count % self.batch_size == 0 {
                                        let from = self.received_count + 1;
                                        let i = self.received_count + self.batch_size;
                                        let to = if i > self.num_proposals {
                                            self.num_proposals
                                        } else {
                                            i
                                        };
                                        info!(self.ctx.log(), "Sending: {}-{}", from, to);
                                        self.send_normal_proposals(from..=to);
                                    }
                                }
                            }
                        } else {
//                            info!(self.ctx.log(), "{}", format!("Received failed proposal response: {}", pr.id));
                            match self.retries.get_mut(&pr.id) {
                                Some(num_retries) => {
                                    *num_retries += 1;
                                    if *num_retries < MAX_RETRIES {
                                        let id = pr.id;
                                        self.schedule_once(RETRY_DELAY, move |c, _| c.propose_normal(id));
                                    } else {
                                        error!(self.ctx.log(), "Proposal {} reached maximum number of retries without success", pr.id);
                                    }
                                },
                                None => {
                                    let id = pr.id;
                                    self.schedule_once(RETRY_DELAY, move |c, _| c.propose_normal(id));
                                    self.retries.insert(pr.id, 1);
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

    #[derive(ComponentDefinition)]
    pub struct TestClient {
        ctx: ComponentContext<Self>,
        num_proposals: u64,
        batch_size: u64,
        nodes: HashMap<u64, ActorPath>,
        reconfig: Option<(Vec<u64>, Vec<u64>)>,
        test_results: HashMap<u64, Vec<u64>>,
        finished_promise: KPromise<HashMap<u64, Vec<u64>>>,
        received_count: u64,
        reconfig_count: u32,
        retries: HashMap<u64, u32>,
        check_sequences: bool,
        responses: HashMap<u64, ActorPath>,
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
            TestClient {
                ctx: ComponentContext::new(),
                num_proposals,
                batch_size,
                nodes,
                reconfig,
                test_results: HashMap::new(),
                finished_promise,
                received_count: 0,
                reconfig_count: 0,
                retries: HashMap::new(),
                check_sequences,
                responses: HashMap::new()
            }
        }

        fn propose_normal(&self, id: u64) {
            let ap = self.ctx.actor_path();
            let p = Proposal::normal(id, ap);
            let node = self.nodes.get(&1).expect("Could not find actorpath to raft node!");
            node.tell((AtomicBroadcastMsg::Proposal(p), AtomicBroadcastSer), self);
        }

        fn send_normal_proposals<T>(&self, r: T) where T: IntoIterator<Item = u64> {
            for id in r {
                self.propose_normal(id);
            }
        }

        fn propose_reconfiguration(&mut self) {
            let ap = self.ctx.actor_path();
            let reconfig = self.reconfig.as_ref().unwrap().clone();
            info!(self.ctx.log(), "{}", format!("Sending reconfiguration: {:?}", reconfig.clone()));
            let p = Proposal::reconfiguration(0, ap, reconfig);
            let raft_node = self.nodes.get(&1).expect("Could not find actorpath to raft node!");
            raft_node.tell((AtomicBroadcastMsg::Proposal(p), AtomicBroadcastSer), self);
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
            self.test_results.insert(0, Vec::new());
            if self.reconfig.is_some() {
                self.send_normal_proposals(1..self.num_proposals/2);
                self.propose_reconfiguration();
                std::thread::sleep(Duration::from_secs(2));
                self.send_normal_proposals(self.num_proposals/2..=self.num_proposals);
            } else {
                info!(self.ctx.log(), "Sending: {}-{}", 1, self.batch_size);
                self.send_normal_proposals(1..=self.batch_size);
            }
        }

        fn receive_network(&mut self, msg: NetMessage) -> () {
            let NetMessage{sender, receiver: _, data} = msg;
            match_deser!{data; {
            am: AtomicBroadcastMsg [AtomicBroadcastSer] => {
                match am {
                    AtomicBroadcastMsg::ProposalResp(pr) => {
                        if pr.succeeded {
                            match pr.id {
                                RECONFIG_ID => {
                                    info!(self.ctx.log(), "reconfiguration succeeded?");
                                }
                                _ => {
                                    // info!(self.ctx.log(), "Got succeeded proposal {}", pr.id);
                                    match self.responses.insert(pr.id, sender.clone()) {
                                        Some(node) if node == sender => {
                                            panic!("Got duplicate response id: {} from same node: {:?}, {:?}", pr.id, node, sender);
                                        },
                                        Some(other) if other != sender => {
                                            panic!("Got duplicate response id: {} from different nodes: {:?}, {:?}", pr.id, other, sender);
                                        },
                                        _ => {},
                                    }

                                    let decided_sequence = self.test_results.get_mut(&0).unwrap();
                                    decided_sequence.push(pr.id);
                                    self.received_count += 1;
                                    if self.received_count == self.num_proposals {
                                        if self.check_sequences {
                                            for (_, actorpath) in &self.nodes {  // get sequence of ALL (past or present) nodes
                                                actorpath.tell((AtomicBroadcastMsg::SequenceReq, AtomicBroadcastSer), self);
                                            }
                                        } else {
                                            self.finished_promise
                                                .to_owned()
                                                .fulfil(self.test_results.clone())
                                                .expect("Failed to fulfill finished promise after getting all sequences");
                                        }
                                    } else if self.received_count % self.batch_size == 0 {
                                        let from = self.received_count + 1;
                                        let i = self.received_count + self.batch_size;
                                        let to = if i > self.num_proposals {
                                            self.num_proposals
                                        } else {
                                            i
                                        };
                                        info!(self.ctx.log(), "Sending: {}-{}", from, to);
                                        self.send_normal_proposals(from..=to);
                                    }
                                }
                            }
                        } else {
//                            info!(self.ctx.log(), "{}", format!("Received failed proposal response: {}", pr.id));
                            match self.retries.get_mut(&pr.id) {
                                Some(num_retries) => {
                                    *num_retries += 1;
                                    if *num_retries < MAX_RETRIES {
                                        let id = pr.id;
                                        self.schedule_once(RETRY_DELAY, move |c, _| c.propose_normal(id));
                                    } else {
                                        error!(self.ctx.log(), "Proposal {} reached maximum number of retries without success", pr.id);
                                    }
                                },
                                None => {
                                    let id = pr.id;
                                    self.schedule_once(RETRY_DELAY, move |c, _| c.propose_normal(id));
                                    self.retries.insert(pr.id, 1);
                                }
                            }
                        }
                    },
                    AtomicBroadcastMsg::SequenceResp(sr) => {
                        self.test_results.insert(sr.node_id, sr.sequence);
                        if (self.test_results.len() - 1) == self.nodes.len() {    // got all sequences from everybody, we're done
                            self.finished_promise
                                .to_owned()
                                .fulfil(self.test_results.clone())
                                .expect("Failed to fulfill finished promise after getting all sequences");
                        }
                    },
                    _ => error!(self.ctx.log(), "Client received unexpected msg"),
                }
            },
            !Err(e) => error!(self.ctx.log(), "{}", &format!("Client failed to deserialise msg: {:?}", e)),
        }
        }
        }
    }
}