extern crate raft as tikv_raft;

#[cfg(feature = "measure_io")]
use crate::bench::atomic_broadcast::atomic_broadcast::IOMetaData;
#[cfg(feature = "measure_io")]
use crate::bench::atomic_broadcast::exp_params::*;
use crate::bench::atomic_broadcast::{
    ble::Ballot,
    messages::{
        paxos::{PaxosMsgWrapper, PaxosSer},
        raft::{RaftMsg, RawRaftSer},
        AtomicBroadcastMsg, ProposalResp, ReconfigurationResp, StopMsg as NetStopMsg, StopMsgDeser,
    },
};
use hashbrown::HashMap;
use kompact::prelude::*;
use leaderpaxos::messages::Message as RawPaxosMsg;
#[cfg(feature = "measure_io")]
use leaderpaxos::messages::PaxosMsg;
#[cfg(feature = "measure_io")]
use std::time::Duration;
use tikv_raft::prelude::Message as RawRaftMsg;

#[derive(Clone, Debug)]
pub enum AtomicBroadcastCompMsg {
    RawRaftMsg(RawRaftMsg),
    RawPaxosMsg(RawPaxosMsg<Ballot>),
    StopMsg(u64),
}

#[derive(Clone, Debug)]
pub enum CommunicatorMsg {
    RawRaftMsg(RawRaftMsg),
    RawPaxosMsg(RawPaxosMsg<Ballot>),
    ProposalResponse(ProposalResp),
    ReconfigurationResponse(ReconfigurationResp),
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
    pub(crate) peers: HashMap<u64, ActorPath>, // node id -> actorpath
    client: ActorPath,                         // cached client to send SequenceResp to
    #[cfg(feature = "measure_io")]
    io_metadata: IOMetaData,
    #[cfg(feature = "measure_io")]
    log_io_timer: Option<ScheduledTimer>,
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
            log_io_timer: None,
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
    fn update_sent_io_metadata(&mut self, msg: &CommunicatorMsg) {
        match msg {
            CommunicatorMsg::RawRaftMsg(rm) => {
                let est_size = Self::estimate_raft_msg_size(rm);
                self.io_metadata.update_sent_with_size(est_size);
            }
            CommunicatorMsg::RawPaxosMsg(pm) => {
                let est_size = Self::estimate_paxos_msg_size(pm);
                self.io_metadata.update_sent_with_size(est_size);
            }
            _ => {}
        }
    }

    #[cfg(feature = "measure_io")]
    fn estimate_paxos_msg_size(pm: &RawPaxosMsg<Ballot>) -> usize {
        let num_entries = match &pm.msg {
            PaxosMsg::Promise(p) => p.sfx.len(),
            PaxosMsg::AcceptSync(acc_sync) => acc_sync.entries.len(),
            PaxosMsg::FirstAccept(f) => f.entries.len(),
            PaxosMsg::AcceptDecide(a) => a.entries.len(),
            _ => 0, // rest of the messages doesn't send entries
        };
        num_entries * DATA_SIZE + std::mem::size_of_val(pm)
    }

    #[cfg(feature = "measure_io")]
    fn estimate_raft_msg_size(rm: &RawRaftMsg) -> usize {
        let num_entries = rm.entries.len();
        num_entries * DATA_SIZE + std::mem::size_of_val(rm)
    }
}

impl ComponentLifecycle for Communicator {
    fn on_start(&mut self) -> Handled {
        #[cfg(feature = "measure_io")]
        {
            let timer = self.schedule_periodic(LOG_IO_PERIOD, LOG_IO_PERIOD, move |c, _| {
                info!(c.ctx.log(), "{:?}", c.io_metadata);
                Handled::Ok
            });
            self.log_io_timer = Some(timer);
        }
        Handled::Ok
    }

    fn on_kill(&mut self) -> Handled {
        #[cfg(feature = "measure_io")]
        {
            if let Some(timer) = self.log_io_timer.take() {
                self.cancel_timer(timer);
            }
            if self.io_metadata != IOMetaData::default() {
                info!(self.ctx.log(), "{:?}", self.io_metadata);
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
                #[cfg(feature = "measure_io")] {
                    let est_size = Self::estimate_raft_msg_size(&r);
                    self.io_metadata.update_received_with_size(est_size);
                }
                self.atomic_broadcast_port.trigger(AtomicBroadcastCompMsg::RawRaftMsg(r));
            },
            msg(p): RawPaxosMsg<Ballot> [using PaxosSer] => {
                #[cfg(feature = "measure_io")] {
                    let est_size = Self::estimate_paxos_msg_size(&p);
                    self.io_metadata.update_received_with_size(est_size);
                }
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
