extern crate raft as tikv_raft;

use kompact::prelude::*;
use super::messages::paxos::Message as RawPaxosMsg;
use tikv_raft::prelude::Message as RawRaftMsg;
use crate::bench::atomic_broadcast::messages::{ProposalResp, AtomicBroadcastMsg, AtomicBroadcastSer};
use std::collections::HashMap;
use crate::bench::atomic_broadcast::messages::{raft::RawRaftSer, paxos::PaxosSer, KillResponse};
use crate::bench::atomic_broadcast::messages::raft::RaftMsg;

#[derive(Clone, Debug)]
pub enum AtomicBroadcastCompMsg {
    RawRaftMsg(RawRaftMsg),
    RawPaxosMsg(RawPaxosMsg),
}

#[derive(Clone, Debug)]
pub enum CommunicatorMsg {
    RawRaftMsg(RawRaftMsg),
    RawPaxosMsg(RawPaxosMsg),
    ProposalResponse(ProposalResp),
}

pub struct CommunicationPort;

impl Port for CommunicationPort {
    type Indication = AtomicBroadcastCompMsg;
    type Request = CommunicatorMsg;
}

#[derive(ComponentDefinition)]
pub struct Communicator {
    ctx: ComponentContext<Communicator>,
    atomic_broadcast_port: ProvidedPort<CommunicationPort, Communicator>,
    peers: HashMap<u64, ActorPath>, // tikv raft node id -> actorpath
    client: ActorPath,    // cached client to send SequenceResp to
    supervisor: Recipient<KillResponse>
}

impl Communicator {
    pub fn with(peers: HashMap<u64, ActorPath>, client: ActorPath, supervisor: Recipient<KillResponse>) -> Communicator {
        Communicator {
            ctx: ComponentContext::new(),
            atomic_broadcast_port: ProvidedPort::new(),
            peers,
            client,
            supervisor,
        }
    }
}

impl Provide<ControlPort> for Communicator {
    fn handle(&mut self, event: <ControlPort as Port>::Request) -> () {
        if let ControlEvent::Kill = event {
            self.supervisor.tell(KillResponse);
        }
    }
}

impl Provide<CommunicationPort> for Communicator {
    fn handle(&mut self, msg: CommunicatorMsg) {
        match msg {
            CommunicatorMsg::RawRaftMsg(rm) => {
                let receiver = self.peers.get(&rm.get_to()).unwrap_or_else(|| panic!("Could not find actorpath for id={}. Known peers: {:?}. RaftMsg: {:?}", &rm.get_to(), self.peers.keys(), rm));
                receiver.tell_serialised(RaftMsg(rm), self).expect("Should serialise RaftMsg");
            },
            CommunicatorMsg::RawPaxosMsg(pm) => {
                let receiver = self.peers.get(&pm.to).unwrap_or_else(|| panic!("RawPaxosMsg: Could not find actorpath for id={}. Known peers: {:?}. PaxosMsg: {:?}", &pm.to, self.peers.keys(), pm));
                receiver.tell_serialised(pm, self).expect("Should serialise RawPaxosMsg");
            },
            CommunicatorMsg::ProposalResponse(pr) => {
                let am = AtomicBroadcastMsg::ProposalResp(pr);
                self.client.tell((am, AtomicBroadcastSer), self);
            },
        }
    }
}

impl Actor for Communicator {
    type Message = ();

    fn receive_local(&mut self, _msg: Self::Message) -> () {
        // ignore
    }

    fn receive_network(&mut self, m: NetMessage) -> () {
        let NetMessage{sender: _, receiver: _, data} = m;
        match_deser! {data; {
                r: RawRaftMsg [RawRaftSer] => {
                    self.atomic_broadcast_port.trigger(AtomicBroadcastCompMsg::RawRaftMsg(r));
                },
                p: RawPaxosMsg [PaxosSer] => {
                    self.atomic_broadcast_port.trigger(AtomicBroadcastCompMsg::RawPaxosMsg(p));
                },
                !Err(e) => error!(self.ctx.log(), "Error deserialising msg: {:?}", e),
            }
            }
    }
}