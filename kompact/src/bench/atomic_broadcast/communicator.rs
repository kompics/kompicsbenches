extern crate raft as tikv_raft;

use kompact::prelude::*;
use super::messages::paxos::Message as RawPaxosMsg;
use tikv_raft::prelude::Message as RawRaftMsg;
use crate::bench::atomic_broadcast::messages::{ProposalResp, AtomicBroadcastMsg, AtomicBroadcastSer};
use std::collections::HashMap;
use crate::bench::atomic_broadcast::messages::{raft::RawRaftSer, paxos::PaxosSer};
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
    CacheClient(ActorPath)
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
    cached_client: Option<ActorPath>,    // cached client to send SequenceResp to
    stopped: bool
}

impl Communicator {
    pub fn with(peers: HashMap<u64, ActorPath>) -> Communicator {
        Communicator {
            ctx: ComponentContext::new(),
            atomic_broadcast_port: ProvidedPort::new(),
            peers,
            cached_client: None,
            stopped: false
        }
    }
}

impl Provide<ControlPort> for Communicator {
    fn handle(&mut self, _: <ControlPort as Port>::Request) -> () {
        // ignore
    }
}

impl Provide<CommunicationPort> for Communicator {
    fn handle(&mut self, msg: CommunicatorMsg) {
        if !self.stopped {
            match msg {
                CommunicatorMsg::CacheClient(client) => self.cached_client = Some(client),
                CommunicatorMsg::RawRaftMsg(rm) => {
                    let receiver = self.peers.get(&rm.get_to()).expect(&format!("Could not find actorpath for id={}. Known peers: {:?}", &rm.get_to(), self.peers.keys()));
                    receiver.tell_serialised(RaftMsg(rm), self).expect("Should serialise RaftMsg");
                },
                CommunicatorMsg::RawPaxosMsg(pm) => {
                    let receiver = self.peers.get(&pm.to).expect(&format!("RawPaxosMsg: Could not find actorpath for id={}. Known peers: {:?}", &pm.to, self.peers.keys()));
                    // receiver.tell((pm, PaxosSer), self);
                    receiver.tell_serialised(pm, self).expect("Should serialise RawPaxosMsg");
                },
                CommunicatorMsg::ProposalResponse(pr) => {
                    let client = self.cached_client.as_ref().expect("No cached client");
                    let am = AtomicBroadcastMsg::ProposalResp(pr);
                    client.tell((am, AtomicBroadcastSer), self);
                },
            }
        }
    }
}

impl Actor for Communicator {
    type Message = ();

    fn receive_local(&mut self, _msg: Self::Message) -> () {
        // self.stopped = true;
        // let _ = msg.0.reply(());
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