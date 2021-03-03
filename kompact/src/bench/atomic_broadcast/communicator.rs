extern crate raft as tikv_raft;

use crate::bench::atomic_broadcast::{
    messages::{
        paxos::{PaxosMsgWrapper, PaxosSer},
        raft::{RaftMsg, RawRaftSer},
        AtomicBroadcastMsg, ProposalResp, StopMsg as NetStopMsg, StopMsgDeser,
    },
    paxos::ballot_leader_election::Ballot,
};
use hashbrown::HashMap;
use kompact::prelude::*;
use leaderpaxos::messages::Message as PaxosMsg;
use tikv_raft::prelude::Message as RawRaftMsg;

#[derive(Clone, Debug)]
pub enum AtomicBroadcastCompMsg {
    RawRaftMsg(RawRaftMsg),
    RawPaxosMsg(PaxosMsg<Ballot>),
    StopMsg(u64),
}

#[derive(Clone, Debug)]
pub enum CommunicatorMsg {
    RawRaftMsg(RawRaftMsg),
    RawPaxosMsg(PaxosMsg<Ballot>),
    ProposalResponse(ProposalResp),
    SendStop(u64, bool),
}

pub struct CommunicationPort;

impl Port for CommunicationPort {
    type Indication = AtomicBroadcastCompMsg;
    type Request = CommunicatorMsg;
}

#[derive(ComponentDefinition)]
pub struct Communicator {
    ctx: ComponentContext<Communicator>,
    atomic_broadcast_port: ProvidedPort<CommunicationPort>,
    peers: HashMap<u64, ActorPath>, // node id -> actorpath
    client: ActorPath,              // cached client to send SequenceResp to
}

impl Communicator {
    pub fn with(peers: HashMap<u64, ActorPath>, client: ActorPath) -> Communicator {
        Communicator {
            ctx: ComponentContext::uninitialised(),
            atomic_broadcast_port: ProvidedPort::uninitialised(),
            peers,
            client,
        }
    }
}

ignore_lifecycle!(Communicator);

impl Provide<CommunicationPort> for Communicator {
    fn handle(&mut self, msg: CommunicatorMsg) -> Handled {
        match msg {
            CommunicatorMsg::RawRaftMsg(rm) => {
                let receiver = self.peers.get(&rm.get_to()).unwrap_or_else(|| {
                    panic!(
                        "Could not find actorpath for id={}. Known peers: {:?}. RaftMsg: {:?}",
                        &rm.get_to(),
                        self.peers.keys(),
                        rm
                    )
                });
                receiver
                    .tell_serialised(RaftMsg(rm), self)
                    .expect("Should serialise RaftMsg");
            }
            CommunicatorMsg::RawPaxosMsg(pm) => {
                trace!(self.ctx.log(), "sending {:?}", pm);
                let receiver = self.peers.get(&pm.to).unwrap_or_else(|| panic!("RawPaxosMsg: Could not find actorpath for id={}. Known peers: {:?}. PaxosMsg: {:?}", &pm.to, self.peers.keys(), pm));
                receiver
                    .tell_serialised(PaxosMsgWrapper(pm), self)
                    .expect("Should serialise RawPaxosMsg");
            }
            CommunicatorMsg::ProposalResponse(pr) => {
                trace!(self.ctx.log(), "ProposalResp: {:?}", pr);
                let am = AtomicBroadcastMsg::ProposalResp(pr);
                self.client
                    .tell_serialised(am, self)
                    .expect("Should serialise ProposalResp");
            }
            CommunicatorMsg::SendStop(my_pid, ack_client) => {
                debug!(self.ctx.log(), "Sending stop to {:?}", self.peers.keys());
                for ap in self.peers.values() {
                    ap.tell_serialised(NetStopMsg::Peer(my_pid), self)
                        .expect("Should serialise StopMsg")
                }
                if ack_client {
                    self.client
                        .tell_serialised(NetStopMsg::Peer(my_pid), self)
                        .expect("Should serialise StopMsg")
                }
            }
        }
        Handled::Ok
    }
}

impl Actor for Communicator {
    type Message = ();

    fn receive_local(&mut self, _msg: Self::Message) -> Handled {
        // ignore
        Handled::Ok
    }

    fn receive_network(&mut self, m: NetMessage) -> Handled {
        let NetMessage { data, .. } = m;
        match_deser! {data {
            msg(r): RawRaftMsg [using RawRaftSer] => {
                self.atomic_broadcast_port.trigger(AtomicBroadcastCompMsg::RawRaftMsg(r));
            },
            msg(p): PaxosMsg<Ballot> [using PaxosSer] => {
                self.atomic_broadcast_port.trigger(AtomicBroadcastCompMsg::RawPaxosMsg(p));
            },
            msg(stop): NetStopMsg [using StopMsgDeser] => {
                if let NetStopMsg::Peer(pid) = stop {
                    self.atomic_broadcast_port.trigger(AtomicBroadcastCompMsg::StopMsg(pid));
                }
            },
            err(e) => error!(self.ctx.log(), "Error deserialising msg: {:?}", e),
            default(_) => unimplemented!("Should be either RawRaftMsg, PaxosMsg or NetStopMsg!")
        }
        }
        Handled::Ok
    }
}
