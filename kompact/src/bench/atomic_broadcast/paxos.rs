use kompact::prelude::*;
use super::storage::paxos::PaxosStorage;
use super::messages::paxos::{*};
use super::messages::paxos::ballot_leader_election::{Ballot, Leader};
use std::fmt::Debug;
use std::collections::HashMap;
use crate::bench::atomic_broadcast::paxos::Role::Follower;
use std::mem;

#[derive(PartialEq, Debug)]
enum Phase {
    Prepare,
    Accept,
    Recover,
    None
}

#[derive(PartialEq, Debug)]
enum Role {
    Follower,
    Leader
}

struct ReceivedPromise<T> where T: Clone + Debug {
    pid: u64,
    n_accepted: Round,
    sfx: Vec<Entry<T>>
}

impl<T> ReceivedPromise<T> where T: Clone + Debug {
    fn from(pid: u64, n_accepted: Round, sfx: Vec<Entry<T>>) -> ReceivedPromise<T> {
        ReceivedPromise { pid, n_accepted, sfx }
    }
}

#[derive(Clone, Debug)]
pub struct StopSign {
    pub config_id: u32,
    pub nodes: Vec<u64>,
}

impl StopSign {
    pub fn with(config_id: u32, nodes: Vec<u64>) -> StopSign {
        StopSign{ config_id, nodes }
    }
}

#[derive(Clone, Debug)]
pub enum Entry<T> where T: Clone + Debug {
    Normal(T),
    StopSign(StopSign)
}

impl<T> Entry<T> where T: Clone + Debug {
    fn is_stopsign(&self) -> bool {
        match self {
            Entry::StopSign(_) => true,
            _ => false
        }
    }
}

pub struct Message<T> where T: Clone + Debug {
    from: u64,
    to: u64,
    msg: PaxosMsg<T>
}

impl<T> Message<T> where T: Clone + Debug {
    pub fn with(from: u64, to: u64, msg: PaxosMsg<T>) -> Message<T> {
        Message{ from, to, msg }
    }
}

pub struct Paxos<S, T> where
    S: PaxosStorage<T>,
    T: Clone + Debug {
        storage: S,
        pid: u64,
        config_id: u32,
        majority: usize,
        nodes: Vec<u64>,
        state: (Role, Phase),
        n_leader: Round, // (config_id, ballot)
        promises: Vec<ReceivedPromise<T>>,
        prev_final_seq: Vec<T>,
        las: HashMap<u64, u64>,
        lds: HashMap<u64, u64>,
        proposals: Vec<Entry<T>>,
        lc: u64,    // length of longest chosen seq
        decided: Vec<Entry<T>>, // TODO don't expose entry to client?
        outgoing: Vec<Message<T>>,
}

impl<S, T> Paxos<S, T> where
    S: PaxosStorage<T>,
    T: Clone + Debug {

    /*** User functions ***/
    pub fn get_decided_entries(&mut self) -> Vec<Entry<T>> {
        let decided_entries = mem::replace(&mut self.decided, vec![]);
        decided_entries
    }

    pub fn get_outgoing_messages(&mut self) -> Vec<Message<T>> {
        let outgoing_msgs = mem::replace(&mut self.outgoing, vec![]);
        outgoing_msgs
    }

    pub fn handle(&mut self, m: Message<T>) {
        match m.msg {
            PaxosMsg::Leader(l) => self.handle_leader(l),
            PaxosMsg::Prepare(prep) => self.handle_prepare(prep, m.from),
            PaxosMsg::Promise(prom) => {
                match &self.state {
                    (Role::Leader, Phase::Prepare) => self.handle_promise_prepare(prom, m.from),
                    (Role::Leader, Phase::Accept) => self.handle_promise_accept(prom, m.from),
                    _ => panic!("{}", format!("Got promise msg in wrong state: {:?}", self.state)),
                }
            },
            PaxosMsg::AcceptSync(acc_sync) => self.handle_accept_sync(acc_sync, m.from),
            PaxosMsg::Accept(acc) => self.handle_accept(acc, m.from),
            PaxosMsg::Accepted(accepted) => self.handle_accepted(accepted, m.from),
            PaxosMsg::Decide(d) => self.handle_decide(d, m.from),
        }
    }

    pub fn propose_normal(&mut self, proposal: T) {
        match self.state {
            (Role::Leader, Phase::Prepare) => {
                self.proposals.push(Entry::Normal(proposal));
            },
            (Role::Leader, Phase::Accept) => {
                let entry = Entry::Normal(proposal);
                self.propose(entry);
            },
            _ => {
                // TODO forward to leader?
                unimplemented!();
            }
        }
    }

    pub fn propose_reconfiguration(&mut self, nodes: Vec<u64>) {
        match self.state {
            (Role::Leader, Phase::Prepare) => {
                let ss = StopSign::with(self.config_id + 1, nodes);
                self.proposals.push(Entry::StopSign(ss));

            },
            (Role::Leader, Phase::Accept) => {
                let ss = StopSign::with(self.config_id + 1, nodes);
                let entry = Entry::StopSign(ss);
                self.propose(entry);
            },
            _ => {
                // TODO forward to leader?
                unimplemented!();
            }
        }
    }

    /*** Leader ***/
    pub fn handle_leader(&mut self, l: Leader) {
        let n = Round::with(self.config_id, l.ballot);
        if self.pid == l.pid && n > self.n_leader {
            self.n_leader = n.clone();
            self.storage.set_promise(n);
            let prev_seq_len: u64 = self.prev_final_seq.len() as u64;
            self.las.insert(self.pid, prev_seq_len);
            self.lds = HashMap::new();
            self.lds.insert(self.pid, self.storage.get_decided_len());
            self.lc = prev_seq_len;
            self.state = (Role::Leader, Phase::Prepare);
        } else {
            if self.state.1 == Phase::Recover {
                // TODO send PREPAREREQ
                unimplemented!();
            } else {
                self.state.0 = Role::Follower;
            }
        }
    }

    fn propose(&mut self, entry: Entry<T>) {
        if !self.storage.stopped() {
            self.storage.append_entry(entry.clone());
            self.las.insert(self.pid, self.storage.get_sequence_len());
            for pid in &self.nodes {
                if pid != &self.pid {
                    if self.lds.get(&pid).is_some() {
                        let acc = Accept::with(self.n_leader.clone(), entry.clone(), self.storage.get_decided_len());
                        // TODO SEND ACCEPT
                        self.outgoing.push(Message::with(self.pid, *pid, PaxosMsg::Accept(acc)));
                    }
                }
            }
        }
    }

    fn handle_promise_prepare(&mut self, prom: Promise<T>, from: u64) {
        if prom.n == self.n_leader {
            let rp = ReceivedPromise::from(from, prom.n_accepted, prom.sfx);
            self.promises.push(rp);
            self.lds.insert(from, prom.ld);
            if self.promises.len() > self.majority {
                let mut suffix = Self::max_value(&self.promises);
                let last_is_stop = &suffix.last().unwrap().is_stopsign();
                self.storage.append_sequence(&mut suffix);
                if *last_is_stop {
                    self.proposals = vec![];    // will never be decided
                } else {
                    Self::drop_after_stopsign(&mut self.proposals);
                    let mut proposals = mem::replace(&mut self.proposals, vec![]);  // consume proposals
                    self.storage.append_sequence(&mut proposals);
                    self.las.insert(self.pid, self.storage.get_sequence_len());
                    self.state = (Role::Leader, Phase::Accept);
                }
                let va_len = self.storage.get_sequence_len();
                for (pid, lds) in self.lds.iter() {
                    if lds != &va_len {
                        let sfx = self.storage.get_suffix(*lds);
                        let acc_sync = AcceptSync::with(self.n_leader.clone(), sfx, *lds);
                        self.outgoing.push(Message::with(self.pid, *pid, PaxosMsg::AcceptSync(acc_sync)));
                    }
                }
            }
        }
    }

    fn handle_promise_accept(&mut self, prom: Promise<T>, from: u64) {
        if prom.n == self.n_leader {
            self.lds.insert(from, prom.ld);
            let sfx = self.storage.get_suffix(prom.ld);
            let acc_sync = AcceptSync::with(self.n_leader.clone(), sfx, prom.ld);
            self.outgoing.push(Message::with(self.pid, from, PaxosMsg::AcceptSync(acc_sync)));
            if self.lc != self.prev_final_seq.len() as u64 {
                // inform what got decided already
                let d = Decide::with(self.lc, self.n_leader.clone());
                self.outgoing.push(Message::with(self.pid, from, PaxosMsg::Decide(d)));
            }
        }
    }

    fn handle_accepted(&mut self, accepted: Accepted, from: u64) {
        if accepted.n == self.n_leader && self.state == (Role::Leader, Phase::Accept) {
            self.las.insert(from, accepted.la);
            let mut counter = 0;
            for (pid, la) in self.las.iter() {
                if la >= &accepted.la { counter += 1; }
            }
            if accepted.la > self.lc && counter >= self.majority {
                self.lc = accepted.la;
                for pid in &self.nodes {
                    let d = Decide::with(self.lc, self.n_leader.clone());
                    self.outgoing.push(Message::with(self.pid, *pid, PaxosMsg::Decide(d)));
                }
            }
        }
    }

    /*** Follower ***/
    fn handle_prepare(&mut self, prep: Prepare, from: u64) {
        if &self.storage.get_promise() < &prep.n {
            self.storage.set_promise(prep.n.clone());
            self.state = (Role::Follower, Phase:: Prepare);
            let na = self.storage.get_accepted_round();
            let suffix = if &na >= &prep.n_accepted {
                self.storage.get_decided_suffix()
            } else {
                vec![]
            };
            let p = Promise::with(prep.n, na, suffix, self.storage.get_decided_len());
            self.outgoing.push(Message::with(self.pid, from, PaxosMsg::Promise(p)));
        }
    }

    fn handle_accept_sync(&mut self, acc_sync: AcceptSync<T>, from: u64) {
        if self.state == (Role::Follower, Phase::Prepare) {
            if self.storage.get_promise() == acc_sync.n {
                self.storage.set_accepted_round(acc_sync.n.clone());
                let mut sfx = acc_sync.sfx;
                self.storage.append_on_prefix(acc_sync.ld, &mut sfx);
                self.state = (Role::Follower, Phase::Accept);
                let accepted = Accepted::with(acc_sync.n, self.storage.get_sequence_len());
                self.outgoing.push(Message::with(self.pid, from, PaxosMsg::Accepted(accepted)));
            }
        }
    }

    fn handle_accept(&mut self, acc: Accept<T>, from: u64) {
        if self.state == (Role::Follower, Phase::Accept) {
            if self.storage.get_promise() == acc.n {
                self.storage.append_entry(acc.entry);
                let accepted = Accepted::with(acc.n, self.storage.get_sequence_len());
                self.outgoing.push(Message::with(self.pid, from, PaxosMsg::Accepted(accepted)));
            }
        }
    }

    fn handle_decide(&mut self, dec: Decide, from: u64) {
        if self.storage.get_promise() == dec.n {
            let mut decided_entries = self.storage.decide_entries(dec.ld);
            self.decided.append(&mut decided_entries);
        }
    }

    /*** algorithm specific functions ***/
    fn max_value(promises: &Vec<ReceivedPromise<T>>) -> Vec<Entry<T>> {
        let mut max_n: &Round = &promises[0].n_accepted;
        let mut max_sfx: &Vec<Entry<T>> = &promises[0].sfx;
        for p in promises {
            if &p.n_accepted > max_n {
                max_n = &p.n_accepted;
                max_sfx = &p.sfx;
            }
        }
        max_sfx.clone()
    }

    fn drop_after_stopsign(entries: &mut Vec<Entry<T>>) {   // drop all entries ordered after stopsign (if any)
        for (idx, e) in entries.iter().enumerate() {
            if e.is_stopsign() {
                entries.truncate(idx + 1);
                return;
            }
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

mod communicator {

}
