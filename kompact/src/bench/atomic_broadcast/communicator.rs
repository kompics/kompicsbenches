extern crate raft as tikv_raft;

use crate::bench::atomic_broadcast::{
    messages::{
        paxos::{PaxosMsgWrapper, PaxosSer},
        raft::{RaftMsg, RawRaftSer},
        AtomicBroadcastMsg, ProposalResp, ReconfigurationResp, StopMsg as NetStopMsg, StopMsgDeser,
    },
    paxos::ballot_leader_election::Ballot,
};
use hashbrown::HashMap;
use kompact::prelude::*;
use leaderpaxos::messages::Message as PaxosMsg;
use tikv_raft::prelude::Message as RawRaftMsg;
#[cfg(feature = "measure_io")]
use kompact::messaging::NetData;
#[cfg(feature = "measure_io")]
use crate::bench::atomic_broadcast::atomic_broadcast::IOMetaData;

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
    ReconfigurationResponse(ReconfigurationResp),
    SendStop(u64, bool),
    #[cfg(feature = "measure_io")]
    StartReconfigMeasurement,
    #[cfg(feature = "measure_io")]
    StopReconfigMeasurement
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
    pub(crate) peers: HashMap<u64, ActorPath>, // node id -> actorpath
    client: ActorPath,                         // cached client to send SequenceResp to
    #[cfg(feature = "measure_io")]
    io_metadata: IOMetaData,
    #[cfg(feature = "measure_io")]
    measure_reconfig_io: bool,
    #[cfg(feature = "measure_io")]
    reconfig_io_metadata: IOMetaData
}

impl Communicator {
    pub fn with(peers: HashMap<u64, ActorPath>, client: ActorPath) -> Communicator {
        Communicator {
            ctx: ComponentContext::uninitialised(),
            atomic_broadcast_port: ProvidedPort::uninitialised(),
            peers,
            client,
            #[cfg(feature = "measure_io")]
            io_metadata: IOMetaData::default(),
            #[cfg(feature = "measure_io")]
            measure_reconfig_io: false,
            #[cfg(feature = "measure_io")]
            reconfig_io_metadata: IOMetaData::default(),
        }
    }

    fn get_actorpath(&self, id: u64) -> &ActorPath {
        self.peers.get(&id).unwrap_or_else(|| {
            panic!(
                "Could not find actorpath for id={}. Known peers: {:?}",
                id,
                self.peers.keys(),
            )
        })
    }

    #[cfg(feature = "measure_io")]
    fn update_received_io_metadata(&mut self, msg: &NetData) {
        if self.measure_reconfig_io {
            self.reconfig_io_metadata.update_received(msg);
        } else {
            self.io_metadata.update_received(msg);
        }
    }

    #[cfg(feature = "measure_io")]
    fn update_sent_io_metadata(&mut self, msg: &CommunicatorMsg) {
        match msg {
            CommunicatorMsg::RawRaftMsg(rm) => {
                if self.measure_reconfig_io {
                    self.reconfig_io_metadata.update_sent(rm);
                } else {
                    self.io_metadata.update_sent(rm);
                }
            }
            CommunicatorMsg::RawPaxosMsg(pm) => {
                if self.measure_reconfig_io {
                    self.reconfig_io_metadata.update_sent(pm);
                } else {
                    self.io_metadata.update_sent(pm);
                }
            }
            _ => {}
        }
    }
}

impl ComponentLifecycle for Communicator {
    fn on_kill(&mut self) -> Handled {
        #[cfg(feature = "measure_io")] {
            let d = IOMetaData::default();
            if self.io_metadata != d {
                info!(self.ctx.log(), "{:?}", self.io_metadata);
            }
            if self.reconfig_io_metadata != d {
                info!(self.ctx.log(), "Reconfiguration {:?}", self.reconfig_io_metadata);
            }
        }
        Handled::Ok
    }
}

impl Provide<CommunicationPort> for Communicator {
    fn handle(&mut self, msg: CommunicatorMsg) -> Handled {
        #[cfg(feature = "measure_io")]
            self.update_sent_io_metadata(&msg);
        match msg {
            CommunicatorMsg::RawRaftMsg(rm) => {
                let receiver = self.get_actorpath(rm.get_to());
                receiver
                    .tell_serialised(RaftMsg(rm), self)
                    .expect("Should serialise RaftMsg");
            }
            CommunicatorMsg::RawPaxosMsg(pm) => {
                trace!(self.ctx.log(), "sending {:?}", pm);
                let receiver = self.get_actorpath(pm.to);
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
            CommunicatorMsg::ReconfigurationResponse(rr) => {
                trace!(self.ctx.log(), "ReconfigurationResp: {:?}", rr);
                let am = AtomicBroadcastMsg::ReconfigurationResp(rr);
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
            #[cfg(feature = "measure_io")]
            CommunicatorMsg::StartReconfigMeasurement => self.measure_reconfig_io = true,
            #[cfg(feature = "measure_io")]
            CommunicatorMsg::StopReconfigMeasurement => self.measure_reconfig_io = false,
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
        #[cfg(feature = "measure_io")]
            self.update_received_io_metadata(&data);
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