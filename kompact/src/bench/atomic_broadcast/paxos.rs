use kompact::prelude::*;
use super::storage::paxos::PaxosStorage;
use super::messages::paxos::{ballot_leader_election::Ballot, *};
use std::fmt::Debug;
use std::collections::HashMap;

enum Phase {
    Prepare,
    Accept,
    Recover,
    None
}

enum Role {
    Follower,
    Leader
}

struct ReceivedPromise<T> {
    pid: u64,
    n: Round,
    sfx: Vec<T>
}

pub struct Message {
    from: u64,
    to: u64,
    msg: PaxosMsg
}

pub struct Paxos<S, T> where
    S: PaxosStorage,
    T: Clone + Debug + Serialisable {
        storage: S,
        pid: u64,
        nodes: Vec<u64>,
        state: (Role, Phase),
        n_leader: Round, // (config_id, ballot)
        promises: Vec<ReceivedPromise<T>>,
        prev_final_seq: Vec<T>,
        las: HashMap<u64, u64>,
        lds: HashMap<u64, u64>,
        prop_cmds: Vec<T>,
        lc: u64,    // length of longest chosen seq
        outgoing: Vec<Message> // TODO optimize, maybe SPMC?
}

impl<S, T> Paxos<S, T> where
    S: PaxosStorage,
    T: Clone + Debug + Serialisable {

    pub fn step(&mut self, m: Message) {
        match &m.msg {
            PaxosMsg::Leader(l) => unimplemented!(),
            PaxosMsg::Prepare(prep) => unimplemented!(),
            PaxosMsg::Promise(prom) => unimplemented!(),
            PaxosMsg::AcceptSync(acc_sync) => unimplemented!(),
            PaxosMsg::Accept(acc) => unimplemented!(),
            PaxosMsg::Accepted(accepted) => unimplemented!(),
            PaxosMsg::Decide(d) => unimplemented!(),
        }
    }

}

mod ballot_leader_election {
    use super::*;
    use super::super::messages::{paxos::ballot_leader_election::*, Run};
    use std::time::Duration;

    pub struct BallotLeaderElection;

    impl Port for BallotLeaderElection {
        type Indication = Leader;
        type Request = ();
    }

    #[derive(ComponentDefinition)]
    pub struct BallotLeaderComp {   // TODO decouple from kompact, similar style to tikv_raft with tick() replacing timers
        ctx: ComponentContext<Self>,
        ble_port: ProvidedPort<BallotLeaderElection, BallotLeaderComp>,
        pid: u64,
        peers: Vec<ActorPath>,
        round: u64,
        ballots: Vec<(u64, Ballot)>,
        current_ballot: Ballot,  // (round, pid)
        leader: Option<(u64, Ballot)>,
        max_ballot: Ballot,
        hb_delay: u64,
        delta: u64,
        majority: usize,
        timer: Option<ScheduledTimer>
    }

    impl BallotLeaderComp {
        fn new(peers: Vec<ActorPath>, pid: u64, delta: u64) -> BallotLeaderComp {
            BallotLeaderComp {
                ctx: ComponentContext::new(),
                ble_port: ProvidedPort::new(),
                pid,
                majority: (&peers.len() + 1)/2 + 1, // +1 because peers is all other nodes, exclusive ourselves
                peers,
                round: 0,
                ballots: vec![],
                current_ballot: Ballot::with(0, pid),
                leader: None,
                max_ballot: Ballot::with(0, pid),
                hb_delay: delta,
                delta,
                timer: None
            }
        }

        fn max_by_ballot(ballots: Vec<(u64, Ballot)>) -> (u64, Ballot) {
            let mut top = ballots[0];
            for ballot in ballots {
                if ballot.1 > top.1 {
                    top = ballot;
                } else if ballot.1 == top.1 && ballot.0 > top.0 {   // use pid to tiebreak
                    top = ballot;
                }
            }
            top
        }

        fn check_leader(&mut self) {
            self.ballots.push((self.pid, self.current_ballot));
            let ballots: Vec<(u64, Ballot)> = self.ballots.drain(..).collect();
            let (top_pid, top_ballot) = Self::max_by_ballot(ballots);
            if top_ballot < self.max_ballot {
                self.current_ballot.n = self.max_ballot.n + 1;
                self.leader = None;
            } else {
                if self.leader.is_some() {
                    if self.leader.unwrap() == (top_pid, top_ballot) {
                        return;
                    }
                }
                self.max_ballot = top_ballot;
                self.leader = Some((top_pid, top_ballot));
                self.ble_port.trigger(Leader::with(top_pid, top_ballot));
            }
        }

        fn hb_timeout(&mut self) {
            if self.ballots.len() + 1 >= self.majority {
                self.check_leader();
            }
            self.round += 1;
            for peer in &self.peers {
                let hb_request = HeartbeatRequest::with(self.round, self.max_ballot);
                peer.tell((HeartbeatMsg::Request(hb_request), BallotLeaderSer), self);
            }
        }

        fn start_timer(&mut self) {
            let delay = Duration::from_millis(0);
//            let uuid = uuid::Uuid::new_v4();
            let timer = self.schedule_periodic(
                delay,
                Duration::from_millis(self.hb_delay),
                move |c, _| c.hb_timeout()
            );
            self.timer = Some(timer);
        }

        fn stop_timer(&mut self) {
            let timer = self.timer.take().unwrap();
            self.cancel_timer(timer);
        }
    }

    impl Provide<ControlPort> for BallotLeaderComp {
        fn handle(&mut self, _: <ControlPort as Port>::Request) -> () {
            // ignore
        }
    }

    impl Provide<BallotLeaderElection> for BallotLeaderComp {
        fn handle(&mut self, _: <BallotLeaderElection as Port>::Request) -> () {
            unimplemented!()
        }
    }

    impl Actor for BallotLeaderComp {
        type Message = Run;

        fn receive_local(&mut self, _msg: Self::Message) -> () {
            // TODO implement STOP
            self.start_timer();
        }

        fn receive_network(&mut self, m: NetMessage) -> () {
            let sender = &m.sender().clone();
            match_deser!{m; {
                hb: HeartbeatMsg [BallotLeaderSer] => {
                    match hb {
                        HeartbeatMsg::Request(req) => {
                            if req.max_ballot > self.max_ballot {
                                self.max_ballot = req.max_ballot;
                            }
                            let hb_reply = HeartbeatReply::with(self.pid, req.round, self.current_ballot);
                            sender.tell((HeartbeatMsg::Reply(hb_reply), BallotLeaderSer), self);
                        },
                        HeartbeatMsg::Reply(rep) => {
                            if rep.round == self.round {
                                self.ballots.push((rep.sender_pid, rep.max_ballot));
                            } else {    // TODO deal with HB from previous iterations?
                                self.stop_timer();
                                self.hb_delay += self.delta;
                                self.start_timer();
                            }
                        }
                    }
                },

                !Err(e) => error!(self.ctx.log(), "Error deserialising msg: {:?}", e),
            }
            }
        }
    }

    #[test]
    fn paxos_test() {
        let a = Ballot::with(1, 4);
        let b = Ballot::with(1, 4);

        assert!(a == b);
    }

}

