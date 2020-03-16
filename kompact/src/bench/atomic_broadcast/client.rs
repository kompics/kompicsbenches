use kompact::prelude::*;
use std::sync::Arc;
use synchronoise::CountdownEvent;
use std::collections::HashMap;
use super::messages::{Proposal, CommunicatorMsg, AtomicBroadcastSer, RECONFIG_ID};

#[derive(Clone, Debug)]
pub struct Run;

#[derive(ComponentDefinition)]
pub struct Client {
    ctx: ComponentContext<Self>,
    num_proposals: u64,
    nodes: HashMap<u64, ActorPath>,
    reconfig: Option<(Vec<u64>, Vec<u64>)>,
    finished_latch: Arc<CountdownEvent>,
    sent_count: u64,
    received_count: u64,
}

impl Client {
    pub fn with(
        num_proposals: u64,
        nodes: HashMap<u64, ActorPath>,
        reconfig: Option<(Vec<u64>, Vec<u64>)>,
        finished_latch: Arc<CountdownEvent>
    ) -> Client {
        Client {
            ctx: ComponentContext::new(), num_proposals, nodes, reconfig, finished_latch, sent_count: 0, received_count: 0
        }
    }

    fn propose_normal(&mut self, n: u64) {
        let num_sent = self.sent_count;
        for i in 1..=n {
            let ap = self.ctx.actor_path();
            let p = Proposal::normal(i + num_sent, ap);
            let raft_node = self.nodes.get(&1).expect("Could not find actorpath to raft node!");
            raft_node.tell((CommunicatorMsg::Proposal(p), AtomicBroadcastSer), self);
            self.sent_count += 1;
        }
    }

    fn propose_reconfiguration(&mut self) {
        let ap = self.ctx.actor_path();
        let reconfig = self.reconfig.take().unwrap();
        info!(self.ctx.log(), "{}", format!("Sending reconfiguration: {:?}", reconfig.clone()));
        let p = Proposal::reconfiguration(0, ap, reconfig);
        // TODO send proposal to who?
        let raft_node = self.nodes.get(&1).expect("Could not find actorpath to raft node!");
        raft_node.tell((CommunicatorMsg::Proposal(p), AtomicBroadcastSer), self);
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
            self.propose_normal(self.num_proposals/2);
        } else {
            self.propose_normal(self.num_proposals);
        }
    }

    fn receive_network(&mut self, msg: NetMessage) -> () {
        match_deser!{msg; {
            cm: CommunicatorMsg [AtomicBroadcastSer] => {
                match cm {
                    CommunicatorMsg::ProposalResp(pr) => {
                        if pr.succeeded {
                            match pr.id {
                                RECONFIG_ID => {
                                    let rest = self.num_proposals - self.sent_count;
                                    info!(self.ctx.log(), "{}", format!("Reconfiguration succeeded! Sending rest of the proposals: {}", rest));
//                                    assert_eq!(self.reconfig.take().unwrap(), r);
                                    self.propose_normal(rest);    // propose rest after succesful reconfig
                                }
                                _ => {
                                    self.received_count += 1;
                                    if self.received_count == self.num_proposals {
                                        self.finished_latch.decrement().expect("Failed to countdown finished latch");
                                    } else if self.received_count == self.num_proposals/2 && self.reconfig.is_some() {
                                        self.propose_reconfiguration();
                                    }
                                }
                            }
                        } /*else {
                            error!(self.ctx.log(), "{}", format!("Received failed proposal response: {}", pr.id));
                        }*/
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
    use crate::bench::atomic_broadcast::messages::SequenceReq;

    #[derive(ComponentDefinition)]
    pub struct TestClient {
        ctx: ComponentContext<Self>,
        num_proposals: u64,
        nodes: HashMap<u64, ActorPath>,
        reconfig: Option<(Vec<u64>, Vec<u64>)>,
        test_results: HashMap<u64, Vec<u64>>,
        finished_promise: KPromise<HashMap<u64, Vec<u64>>>,
        sent_count: u64,
        received_count: u64,
    }

    impl TestClient {
        pub fn with(
            num_proposals: u64,
            nodes: HashMap<u64, ActorPath>,
            reconfig: Option<(Vec<u64>, Vec<u64>)>,
            finished_promise: KPromise<HashMap<u64, Vec<u64>>>,
        ) -> TestClient {
            TestClient {
                ctx: ComponentContext::new(),
                num_proposals,
                nodes,
                reconfig,
                test_results: HashMap::new(),
                finished_promise,
                sent_count: 0,
                received_count: 0
            }
        }

        fn propose_normal(&mut self, n: u64) {
            let num_sent = self.sent_count;
            for i in 1..=n {
                let ap = self.ctx.actor_path();
                let p = Proposal::normal(i + num_sent, ap);
                let raft_node = self.nodes.get(&1).expect("Could not find actorpath to raft node!");
                raft_node.tell((CommunicatorMsg::Proposal(p), AtomicBroadcastSer), self);
                self.sent_count += 1;
            }
            info!(self.ctx.log(), "Sent all proposals");
        }

        fn propose_reconfiguration(&mut self) {
            let ap = self.ctx.actor_path();
            let reconfig = self.reconfig.as_ref().unwrap().clone();
            info!(self.ctx.log(), "{}", format!("Sending reconfiguration: {:?}", reconfig.clone()));
            let p = Proposal::reconfiguration(0, ap, reconfig);
            // TODO send proposal to who?
            let raft_node = self.nodes.get(&1).expect("Could not find actorpath to raft node!");
            raft_node.tell((CommunicatorMsg::Proposal(p), AtomicBroadcastSer), self);
        }
    }

    impl Provide<ControlPort> for TestClient {
        fn handle(&mut self, event: <ControlPort as Port>::Request) -> () {
            match event {
                ControlEvent::Start => {
                    info!(self.ctx.log(), "Started bcast client");
                    self.test_results.insert(0, Vec::new());
                    if self.reconfig.is_some() {
                        self.propose_normal(self.num_proposals/2);
                    } else {
                        self.propose_normal(self.num_proposals);
                    }
                }
                _ => {}, //ignore
            }
        }
    }

    impl Actor for TestClient {
        type Message = ();

        fn receive_local(&mut self, _msg: Self::Message) -> () {
            // ignore
        }

        fn receive_network(&mut self, msg: NetMessage) -> () {
            match_deser!{msg; {
            cm: CommunicatorMsg [AtomicBroadcastSer] => {
                match cm {
                    CommunicatorMsg::ProposalResp(pr) => {
                        if pr.succeeded {
                            match pr.id {
                                RECONFIG_ID => {
                                    let rest = self.num_proposals - self.sent_count;
                                    info!(self.ctx.log(), "{}", format!("Reconfiguration succeeded! Sending rest of the proposals: {}", rest));
                                    self.propose_normal(rest);    // propose rest after succesful reconfig
                                }
                                _ => {
                                    let decided_sequence = self.test_results.get_mut(&0).unwrap();
                                    decided_sequence.push(pr.id);
                                    self.received_count += 1;
                                    if self.received_count == self.num_proposals {
                                        if self.reconfig.is_some() {
                                            for (_, actorpath) in &self.nodes {  // get sequence of ALL (past or present) nodes
                                                let req = SequenceReq;
                                                actorpath.tell((CommunicatorMsg::SequenceReq(req), AtomicBroadcastSer), self);
                                            }
                                        } else {
                                            self.finished_promise
                                                .to_owned()
                                                .fulfill(self.test_results.clone())
                                                .expect("Failed to fulfill finished promise");
                                        }
                                    } else if self.received_count == self.num_proposals/2 && self.reconfig.is_some() {
                                        self.propose_reconfiguration();
                                    }
                                }
                            }
                        } else {
                            error!(self.ctx.log(), "{}", format!("Received failed proposal response: {}", pr.id));
                        }
                    },
                    CommunicatorMsg::SequenceResp(sr) => {
                        self.test_results.insert(sr.node_id, sr.sequence);
                        if (self.test_results.len() - 1) == self.nodes.len() {    // got all sequences from everybody, we're done
                            self.finished_promise
                                .to_owned()
                                .fulfill(self.test_results.clone())
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