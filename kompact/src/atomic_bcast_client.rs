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
    finished_latch: Arc<CountdownEvent>
}

impl AtomicBcastClient {
    pub fn with(
        num_proposals: u32,
        nodes: HashMap<u64, ActorPath>,
        finished_latch: Arc<CountdownEvent>
    ) -> AtomicBcastClient {
        AtomicBcastClient {
            ctx: ComponentContext::new(), num_proposals, nodes, finished_latch
        }
    }
}

impl Provide<ControlPort> for AtomicBcastClient {
    fn handle(&mut self, event: <ControlPort as Port>::Request) -> () {
        match event {
            ControlEvent::Start => {
                info!(self.ctx.log(), "Started bcast client");
                for i in 1..=self.num_proposals {
                    let ap = self.ctx.actor_path();
                    let p = Proposal::normal(i as u64, ap);
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
                            self.num_proposals -= 1;
                            if self.num_proposals == 0 {
                                self.finished_latch.decrement().expect("Failed to countdown finished latch");
                            }
                        } else {
                            error!(self.ctx.log(), "{}", format!("Got failed proposal: {}", pr.id));
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