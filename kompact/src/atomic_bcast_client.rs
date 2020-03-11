use super::*;
use kompact::prelude::*;
use std::sync::Arc;
use synchronoise::CountdownEvent;
use std::collections::HashMap;
use bench::atomic_broadcast::raft::{Proposal, ProposalResp, CommunicatorMsg, RaftSer};
use bench::atomic_broadcast::Reconfiguration;

#[derive(ComponentDefinition)]
pub struct AtomicBcastClient {
    ctx: ComponentContext<Self>,
    num_proposals: u64,
    nodes: HashMap<u64, ActorPath>,
    reconfig: Option<Reconfiguration>,
    finished_latch: Arc<CountdownEvent>
}

impl AtomicBcastClient {
    pub fn with(
        num_proposals: u64,
        nodes: HashMap<u64, ActorPath>,
        reconfig: Option<Reconfiguration>,
        finished_latch: Arc<CountdownEvent>
    ) -> AtomicBcastClient {
        AtomicBcastClient {
            ctx: ComponentContext::new(), num_proposals, nodes, reconfig, finished_latch
        }
    }
}

impl Provide<ControlPort> for AtomicBcastClient {
    fn handle(&mut self, event: <ControlPort as Port>::Request) -> () {
        match event {
            ControlEvent::Start => {
                info!(self.ctx.log(), "Started bcast client");
                for i in 0..self.num_proposals {
                    let ap = self.ctx.actor_path();
                    let p = Proposal::normal(i, ap);
                    // TODO send proposal to who?
                    let raft_node = self.nodes.get(&1).expect("Could not find actorpath to raft node!");
                    raft_node.tell((CommunicatorMsg::Proposal(p), RaftSer), self);
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
//                            info!(self.ctx.log(), "{}", format!("Successful proposal: {}", pr.id));
                            self.num_proposals -= 1;
                            if self.num_proposals == 0 {
                                self.finished_latch.decrement().expect("Failed to countdown finished latch");
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