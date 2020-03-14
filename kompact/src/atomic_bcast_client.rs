use super::*;
use kompact::prelude::*;
use std::sync::Arc;
use synchronoise::CountdownEvent;
use std::collections::HashMap;
use bench::atomic_broadcast::raft::{Proposal, ProposalResp, CommunicatorMsg, RaftSer};

#[derive(ComponentDefinition)]
pub struct AtomicBcastClient {
    ctx: ComponentContext<Self>,
    num_proposals: u32,
    nodes: HashMap<u64, ActorPath>,
    reconfig: Option<(Vec<u64>, Vec<u64>)>,
    finished_latch: Arc<CountdownEvent>,
    sent_count: u64,
    received_count: u64,
}

impl AtomicBcastClient {
    pub fn with(
        num_proposals: u64,
        nodes: HashMap<u64, ActorPath>,
        reconfig: Option<(Vec<u64>, Vec<u64>)>,
        finished_latch: Arc<CountdownEvent>
    ) -> AtomicBcastClient {
        AtomicBcastClient {
            ctx: ComponentContext::new(), num_proposals, nodes, reconfig, finished_latch, sent_count: 0, received_count: 0
        }
    }

    fn propose_normal(&mut self, n: u64) {
        let num_sent = self.sent_count;
        for i in 1..=n {
            let ap = self.ctx.actor_path();
            let p = Proposal::normal(i + num_sent, ap);
            let raft_node = self.nodes.get(&1).expect("Could not find actorpath to raft node!");
            raft_node.tell((CommunicatorMsg::Proposal(p), RaftSer), self);
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
        raft_node.tell((CommunicatorMsg::Proposal(p), RaftSer), self);
    }
}

impl Provide<ControlPort> for AtomicBcastClient {
    fn handle(&mut self, event: <ControlPort as Port>::Request) -> () {
        match event {
            ControlEvent::Start => {
                info!(self.ctx.log(), "Started bcast client");
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

impl Actor for AtomicBcastClient {
    type Message = ();

    fn receive_local(&mut self, _msg: Self::Message) -> () {
        // ignore
    }

    fn receive_network(&mut self, msg: NetMessage) -> () {
        match_deser!{msg; {
            cm: CommunicatorMsg [RaftSer] => {
                match cm {
                    CommunicatorMsg::ProposalResp(pr) => {
                        if pr.succeeded {
                            match pr.id {
                                0 => {
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
                        } else {
                            error!(self.ctx.log(), "{}", format!("Received failed proposal response: {}", pr.id));
                        }
                    }
                    _ => unimplemented!()
                }
            },
            !Err(e) => error!(self.ctx.log(), "{}", &format!("Client failed to deserialise msg: {:?}", e)),
        }
        }
    }
}