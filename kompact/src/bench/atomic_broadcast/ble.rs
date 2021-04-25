use crate::bench::atomic_broadcast::messages::{
    paxos::ballot_leader_election::*, StopMsg as NetStopMsg, StopMsgDeser,
};
#[cfg(feature = "measure_io")]
use crate::bench::atomic_broadcast::util::io_metadata::IOMetaData;
use hashbrown::HashSet;
use kompact::prelude::*;
use leaderpaxos::leader_election::{Leader, Round};
use std::time::Duration;

#[derive(Clone, Copy, Eq, Debug, Default, Ord, PartialOrd, PartialEq)]
pub struct Ballot {
    pub n: u32,
    pub pid: u64,
}

impl Ballot {
    pub fn with(n: u32, pid: u64) -> Ballot {
        Ballot { n, pid }
    }
}

impl Round for Ballot {}

#[derive(Debug)]
pub struct Stop(pub Ask<u64, ()>); // pid

pub struct BallotLeaderElection;

impl Port for BallotLeaderElection {
    type Indication = Leader<Ballot>;
    type Request = ();
}

#[derive(ComponentDefinition)]
pub struct BallotLeaderComp {
    // TODO decouple from kompact, similar style to tikv_raft with tick() replacing timers
    ctx: ComponentContext<Self>,
    ble_port: ProvidedPort<BallotLeaderElection>,
    pid: u64,
    pub(crate) peers: Vec<ActorPath>,
    hb_round: u32,
    ballots: Vec<Ballot>,
    current_ballot: Ballot, // (round, pid)
    leader: Option<Ballot>,
    hb_delay: u64,
    delta: u64,
    pub majority: usize,
    timer: Option<ScheduledTimer>,
    stopped: bool,
    stopped_peers: HashSet<u64>,
    stop_ask: Option<Ask<u64, ()>>,
    quick_timeout: bool,
    initial_election_factor: u64,
    #[cfg(feature = "measure_io")]
    io_metadata: IOMetaData,
}

impl BallotLeaderComp {
    pub fn with(
        peers: Vec<ActorPath>,
        pid: u64,
        hb_delay: u64,
        delta: u64,
        quick_timeout: bool,
        initial_leader: Option<Leader<Ballot>>,
        initial_election_factor: u64,
    ) -> BallotLeaderComp {
        let n = &peers.len() + 1;
        let (leader, initial_ballot) = match initial_leader {
            Some(l) => {
                let leader_ballot = Ballot::with(l.round.n, l.pid);
                let initial_ballot = if l.pid == pid {
                    leader_ballot
                } else {
                    Ballot::with(0, pid)
                };
                (Some(leader_ballot), initial_ballot)
            }
            None => {
                let initial_ballot = Ballot::with(0, pid);
                (None, initial_ballot)
            }
        };
        BallotLeaderComp {
            ctx: ComponentContext::uninitialised(),
            ble_port: ProvidedPort::uninitialised(),
            pid,
            majority: n / 2 + 1, // +1 because peers is exclusive ourselves
            peers,
            hb_round: 0,
            ballots: Vec::with_capacity(n),
            current_ballot: initial_ballot,
            leader,
            hb_delay,
            delta,
            timer: None,
            stopped: false,
            stopped_peers: HashSet::with_capacity(n),
            stop_ask: None,
            quick_timeout,
            initial_election_factor,
            #[cfg(feature = "measure_io")]
            io_metadata: IOMetaData::default(),
        }
    }

    /// Sets initial state after creation. Should only be used before being started.
    pub fn set_initial_leader(&mut self, l: Leader<Ballot>) {
        assert!(self.leader.is_none());
        let leader_ballot = Ballot::with(l.round.n, l.pid);
        self.leader = Some(leader_ballot);
        self.current_ballot = if l.pid == self.pid {
            leader_ballot
        } else {
            Ballot::with(0, self.pid)
        };
        self.quick_timeout = false;
        self.ble_port.trigger(Leader::with(l.pid, leader_ballot));
    }

    fn check_leader(&mut self) {
        let mut ballots = Vec::with_capacity(self.peers.len());
        std::mem::swap(&mut self.ballots, &mut ballots);
        debug!(self.ctx.log(), "check leader ballots: {:?}", ballots);
        let top_ballot = ballots.into_iter().max().unwrap();
        if top_ballot < self.leader.unwrap_or_default() {
            // did not get HB from leader
            debug!(
                self.ctx.log(),
                "Did not get hb from leader. top: {:?}, leader: {:?}", top_ballot, self.leader
            );
            self.current_ballot.n = self.leader.unwrap_or_default().n + 1;
            self.leader = None;
        } else if self.leader != Some(top_ballot) {
            // got a new leader with greater ballot
            self.quick_timeout = false;
            self.leader = Some(top_ballot);
            let top_pid = top_ballot.pid;
            self.ble_port.trigger(Leader::with(top_pid, top_ballot));
        }
    }

    fn hb_timeout(&mut self) -> Handled {
        if self.ballots.len() + 1 >= self.majority {
            self.ballots.push(self.current_ballot);
            self.check_leader();
        } else {
            self.ballots.clear();
        }
        let delay = if self.quick_timeout {
            // use short timeout if still no first leader
            self.hb_delay / self.initial_election_factor
        } else {
            self.hb_delay
        };
        self.hb_round += 1;
        for peer in &self.peers {
            let hb_request = HeartbeatRequest::with(self.hb_round);
            #[cfg(feature = "measure_io")]
            {
                self.io_metadata.update_sent(&hb_request);
            }
            peer.tell_serialised(HeartbeatMsg::Request(hb_request), self)
                .expect("HBRequest should serialise!");
        }
        self.start_timer(delay);
        Handled::Ok
    }

    fn start_timer(&mut self, t: u64) {
        let timer = self.schedule_once(Duration::from_millis(t), move |c, _| c.hb_timeout());
        self.timer = Some(timer);
    }

    fn stop_timer(&mut self) {
        if let Some(timer) = self.timer.take() {
            self.cancel_timer(timer);
        }
    }
}

impl ComponentLifecycle for BallotLeaderComp {
    fn on_start(&mut self) -> Handled {
        debug!(self.ctx.log(), "Started BLE with params: current_ballot: {:?}, quick timeout: {}, hb_round: {}, leader: {:?}", self.current_ballot, self.quick_timeout, self.hb_round, self.leader);
        let bc = BufferConfig::default();
        self.ctx.init_buffers(Some(bc), None);
        self.hb_timeout()
    }

    fn on_kill(&mut self) -> Handled {
        self.stop_timer();
        #[cfg(feature = "measure_io")]
        {
            if self.io_metadata != IOMetaData::default() {
                info!(self.ctx.log(), "BLE IO: {:?}", self.io_metadata);
            }
        }
        Handled::Ok
    }
}

impl Provide<BallotLeaderElection> for BallotLeaderComp {
    fn handle(&mut self, _: <BallotLeaderElection as Port>::Request) -> Handled {
        unimplemented!()
    }
}

impl Actor for BallotLeaderComp {
    type Message = Stop;

    fn receive_local(&mut self, stop: Stop) -> Handled {
        let pid = *stop.0.request();
        self.stop_timer();
        for peer in &self.peers {
            peer.tell_serialised(NetStopMsg::Peer(pid), self)
                .expect("NetStopMsg should serialise!");
        }
        self.stopped = true;
        if self.stopped_peers.len() == self.peers.len() {
            stop.0.reply(()).expect("Failed to reply to stop ask!");
        } else {
            self.stop_ask = Some(stop.0);
        }
        Handled::Ok
    }

    fn receive_network(&mut self, m: NetMessage) -> Handled {
        let NetMessage { sender, data, .. } = m;
        match_deser! {data {
            msg(hb): HeartbeatMsg [using BallotLeaderSer] => {
                match hb {
                    HeartbeatMsg::Request(req) if !self.stopped => {
                        #[cfg(feature = "measure_io")] {
                            self.io_metadata.update_received(&req);
                        }
                        let hb_reply = HeartbeatReply::with(req.round, self.current_ballot);
                        #[cfg(feature = "measure_io")] {
                            self.io_metadata.update_sent(&hb_reply);
                        }
                        sender.tell_serialised(HeartbeatMsg::Reply(hb_reply), self).expect("HBReply should serialise!");
                    },
                    HeartbeatMsg::Reply(rep) if !self.stopped => {
                        #[cfg(feature = "measure_io")] {
                            self.io_metadata.update_received(&rep);
                        }
                        if rep.round == self.hb_round {
                            self.ballots.push(rep.ballot);
                        } else {
                            trace!(self.ctx.log(), "Got late hb reply. HB delay: {}", self.hb_delay);
                            self.hb_delay += self.delta;
                        }
                    },
                    _ => {},
                }
            },
            msg(stop): NetStopMsg [using StopMsgDeser] => {
                if let NetStopMsg::Peer(pid) = stop {
                    assert!(self.stopped_peers.insert(pid), "BLE got duplicate stop from peer {}", pid);
                    debug!(self.ctx.log(), "BLE got stopped from peer {}", pid);
                    if self.stopped && self.stopped_peers.len() == self.peers.len() {
                        debug!(self.ctx.log(), "BLE got stopped from all peers");
                        self.stop_ask
                            .take()
                            .expect("No stop ask!")
                            .reply(())
                            .expect("Failed to reply ask");
                    }
                }

            },
            err(e) => error!(self.ctx.log(), "Error deserialising msg: {:?}", e),
            default(_) => unimplemented!("Should be either HeartbeatMsg or NetStopMsg!"),
        }
        }
        Handled::Ok
    }
}