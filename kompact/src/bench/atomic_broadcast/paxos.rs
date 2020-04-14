use kompact::prelude::*;
use super::storage::paxos::*;
use std::fmt::{Debug, Formatter, Error};
use ballot_leader_election::{BallotLeaderComp, BallotLeaderElection};
use raw_paxos::{Entry, Paxos};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use super::messages::paxos::{Message as RawPaxosMsg, ballot_leader_election::Leader};
use crate::bench::atomic_broadcast::messages::paxos::{PaxosSer, ReconfigInit, ReplicaSer, SequenceTransfer, SequenceRequest};
use crate::bench::atomic_broadcast::messages::{Proposal, ProposalResp, AtomicBroadcastMsg, AtomicBroadcastSer, SequenceResp, RECONFIG_ID};
use crate::partitioning_actor::{PartitioningActorSer, PartitioningActorMsg, Init};
use synchronoise::CountdownEvent;
use uuid::Uuid;
use kompact::prelude::Buf;

const BLE: &str = "ble";
const PAXOS: &str = "paxos";

const DELTA: u64 = 300;
const PF_MAX_LEN: usize = 100;

pub trait SequenceTraits: Sequence + Debug + Send + Sync + 'static {}
pub trait PaxosStateTraits: PaxosState + Send + 'static {}

#[derive(Debug)]
pub struct Reconfiguration<S> where
    S: SequenceTraits,
{
    pub config_id: u32,
    pub config: HashMap<u64, bool>,    // nodes not part of last config
    pub final_sequence: Arc<S>,
}

impl<S> Reconfiguration<S> where
    S: SequenceTraits
{
    pub fn with(config_id: u32, config: HashMap<u64, bool>, final_sequence: Arc<S>) -> Reconfiguration<S> {
        Reconfiguration{ config_id, config, final_sequence }
    }
}

#[derive(Debug)]
pub enum PaxosCompMsg {
    Propose(Proposal),
    SequenceReq(ActorPath),
    Stop(Stop),
}

#[derive(Clone)]
pub struct Stop {
    cd: Arc<CountdownEvent>
}

impl Debug for Stop {
    fn fmt(&self, _f: &mut Formatter<'_>) -> Result<(), Error> {
        Ok(())
    }
}

impl Stop {
    fn with(cd: Arc<CountdownEvent>) -> Stop {
        Stop{ cd }
    }
}

pub enum TransferPolicy {
    Eager,
    Passive
}

#[derive(ComponentDefinition)]
pub struct ReplicaComp<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    ctx: ComponentContext<Self>,
    pid: u64,
    initial_config: Vec<u64>,
    replicas: HashMap<u32, (Arc<Component<BallotLeaderComp>>, Arc<Component<PaxosComp<S, P>>>)>,
    active_config_id: u32,
    nodes: HashMap<u64, ActorPath>, // derive actorpaths of peers' ble and paxos replicas from these
    prev_sequences: HashMap<u32, Arc<S>>,
    stopped: bool,
    iteration_id: u32,
    partitioning_actor: Option<ActorPath>,
    pending_registrations: HashSet<Uuid>,
    policy: TransferPolicy,
    pending_seq_transfers: HashMap<(u32, u32), Vec<Entry>>,
    pending_seq_len: HashMap<u32, u64>,
}

impl<S, P> ReplicaComp<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    pub fn with(initial_config: Vec<u64>, policy: TransferPolicy) -> ReplicaComp<S, P> {
        ReplicaComp {
            ctx: ComponentContext::new(),
            pid: 0,
            initial_config,
            replicas: HashMap::new(),
            active_config_id: 0,
            nodes: HashMap::new(),
            prev_sequences: HashMap::new(),
            stopped: false,
            iteration_id: 0,
            partitioning_actor: None,
            pending_registrations: HashSet::new(),
            policy,
            pending_seq_transfers: HashMap::new(),
            pending_seq_len: HashMap::new()
        }
    }

    fn create_replica(&mut self, config_id: u32, nodes: Vec<u64>) {
        let mut paxos_peers = HashMap::new();
        let mut ble_peers = vec![];
        for pid in nodes {
            if pid != self.pid {
                let actorpath = self.nodes.get(&pid).expect("No actorpath found");
                match actorpath {
                    ActorPath::Named(n) => {
                        // derive paxos and ble actorpath of peers from replica actorpath
                        let sys_path = n.system();
                        let protocol = sys_path.protocol();
                        let port = sys_path.port();
                        let addr = sys_path.address();
                        let named_paxos = NamedPath::new(
                            protocol,
                            addr.clone(),
                            port,
                            vec![format!("{}{},{}-{}", PAXOS, pid, config_id, self.iteration_id).into()]
                        );
                        let named_ble = NamedPath::new(
                            protocol,
                            addr.clone(),
                            port,
                            vec![format!("{}{},{}-{}", BLE, pid, config_id, self.iteration_id).into()]
                        );
                        paxos_peers.insert(pid, ActorPath::Named(named_paxos));
                        ble_peers.push(ActorPath::Named(named_ble));
                    },
                    _ => error!(self.ctx.log(), "{}", format!("Actorpath is not named for node {}", pid)),
                }
            }
        }
        // info!(self.ctx.log(), "Derived all actorpaths");
        let system = self.ctx.system();
        /*** create and register Paxos ***/
        let paxos_comp = system.create(|| {
            PaxosComp::with(self.ctx.actor_ref(), paxos_peers, config_id, self.pid)
        });
        system.register_without_response(&paxos_comp);
        let paxos_id = system.register_by_alias(&paxos_comp, format!("{}{},{}-{}", PAXOS, self.pid, config_id, self.iteration_id), self);
        self.pending_registrations.insert(paxos_id.0);
        /*** create and register BLE ***/
        let ble_comp = system.create( || {
            BallotLeaderComp::with(ble_peers, self.pid, DELTA)
        });
        system.register_without_response(&ble_comp);
        let ble_id = system.register_by_alias(&ble_comp, format!("{}{},{}-{}", BLE, self.pid, config_id, self.iteration_id), self);
        self.pending_registrations.insert(ble_id.0);
        // let _ = system.register_by_alias(&paxos_comp, format!("{}{},{}-{}", PAXOS, self.pid, config_id, self.iteration_id));
        // let _ = system.register_by_alias(&ble_comp, format!("{}{},{}-{}", BLE, self.pid, config_id, self.iteration_id));
        biconnect_components::<BallotLeaderElection, _, _>(&ble_comp, &paxos_comp)
            .expect("Could not connect components!");
        self.replicas.insert(config_id, (ble_comp, paxos_comp));
        self.active_config_id = config_id;
    }

    fn kill_all_replicas(&mut self) {
        for (_, (ble, paxos)) in self.replicas.drain() {
            self.ctx.system().kill(ble);
            self.ctx.system().kill(paxos);
        }
    }

    fn new_iteration(&mut self, init: Init) {
        let nodes = init.nodes;
        self.kill_all_replicas();
        self.active_config_id = 0;
        self.stopped = false;
        self.prev_sequences = HashMap::new();
        self.pid = init.pid as u64;
        self.iteration_id = init.init_id;
        for (id, actorpath) in nodes.into_iter().enumerate() {
            self.nodes.insert(id as u64 + 1, actorpath);
        }
        if self.initial_config.contains(&self.pid){
            self.active_config_id += 1;
            self.create_replica(self.active_config_id, self.initial_config.clone());
        }
    }

    /*fn request_sequence(&mut self, continued_peers: &Vec<u64>, prev_seq_len: u64, config_id: u32) {
        /*let num_continued = continued_peers.len();
        let n = prev_seq_len/num_continued as u64;
        for i in 0..num_continued {
            let from_idx = i as u64 * n;
            let to_idx = from_idx + n;
            let actorpath = self.nodes.get(&continued_peers[i]).unwrap();
            let sr = SequenceRequest::with(config_id, from_idx, to_idx);
            // TODO send to actorpath
            self.pending_seq_transfers += 1;
        }*/
        unimplemented!();
    }

    fn transfer_sequence(&self, config_id: u32, requestor: ActorPath, from_idx: u64, to_idx: u64) {
        /*if config_id == self.active_config_id {

        } else {
            let prev_seq = self.prev_sequences.get(&config_id).expect("Previous sequence not found!");
            let seq = prev_seq.get_entries(from_idx, to_idx);
        }*/
        unimplemented!();
    }

    fn handle_sequence_resp(&mut self, config_id: u32, msg_id: u32, seq: Vec<Entry>) {

    }*/
}

impl<S, P> Provide<ControlPort> for ReplicaComp<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    fn handle(&mut self, event: <ControlPort as Port>::Request) -> () {
        // ignore
        match event {
            ControlEvent::Start => info!(self.ctx.log(), "Started ReplicaComp!"),
            _ => {},
        }
    }
}

#[derive(Debug)]
pub enum ReplicaCompMsg<S> where S: SequenceTraits{
    Reconfig(Reconfiguration<S>),
    RegResp(RegistrationResponse)
}

impl<S> From<RegistrationResponse> for ReplicaCompMsg<S> where S: SequenceTraits {
    fn from(rr: RegistrationResponse) -> Self {
        ReplicaCompMsg::RegResp(rr)
    }
}

impl<S, P> Actor for ReplicaComp<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    type Message = ReplicaCompMsg<S>;

    fn receive_local(&mut self, msg: Self::Message) -> () {
        match msg {
            ReplicaCompMsg::Reconfig(r) => {
                info!(self.ctx.log(), "Got reconfig");
                self.prev_sequences.insert(self.active_config_id, r.final_sequence);
                let nodes: Vec<u64> = r.config.keys().map(|x| *x).collect();
                // let old_config_id = self.active_config_id;
                if r.config.contains_key(&self.pid) {
                    self.create_replica(r.config_id, nodes.clone());
                }
                for (pid, new_node) in r.config {
                    if new_node && pid != self.pid {
                        info!(self.ctx.log(), "Sending ReconfigInit to node {}", pid);
                        let actorpath = self.nodes.get(&pid).expect("No actorpath found for new node");
                        let r_init = ReconfigInit::with(r.config_id, nodes.clone());
                        actorpath.tell((r_init, ReplicaSer), self);
                        if let TransferPolicy::Eager = self.policy {
                            unimplemented!();
                        }
                    }
                }
            }
            ReplicaCompMsg::RegResp(rr) => {
                self.pending_registrations.remove(&rr.id.0);
                if self.pending_registrations.is_empty() {
                    let (ble, paxos) = self.replicas.get(&self.active_config_id).expect("BLE and Paxos component not found!");
                    self.ctx.system().start(paxos);
                    self.ctx.system().start(ble);
                    let resp = PartitioningActorMsg::InitAck(self.iteration_id);
                    let ap = self.partitioning_actor.take().expect("PartitioningActor not found!");
                    ap.tell_serialised(resp, self).expect("Should serialise");
                }
            }
        }


    }

    fn receive_network(&mut self, m: NetMessage) -> () {
        let NetMessage{sender, receiver, data} = m;
        match_deser! {data; {
            p: PartitioningActorMsg [PartitioningActorSer] => {
                match p {
                    PartitioningActorMsg::Init(init) => {
                        info!(self.ctx.log(), "{}", format!("Got init! My pid: {}", init.pid));
                        self.partitioning_actor = Some(sender);
                        self.new_iteration(init);
                    },
                    PartitioningActorMsg::Stop => {
                        info!(self.ctx.log(), "Stopping ble and paxos...");
                        let num_comps = self.replicas.len() * 2;
                        if num_comps > 0 {
                            let cd = Arc::new(CountdownEvent::new(num_comps));
                            let s = Stop::with(cd.clone());
                            for (ble, paxos) in self.replicas.values() {
                                ble.actor_ref().tell(s.clone());
                                paxos.actor_ref().tell(PaxosCompMsg::Stop(s.clone()));
                            }
                            cd.wait_timeout(Duration::from_secs(5));
                        }
                        info!(self.ctx.log(), "Stopped all child components");
                        self.stopped = true;
                        sender
                            .tell_serialised(PartitioningActorMsg::StopAck, self)
                            .expect("Should serialise");
                    },
                    _ => unimplemented!()
                }
            },
            r: ReconfigInit [ReplicaSer] => {
                if self.active_config_id < r.config_id && !self.stopped {
                    info!(self.ctx.log(), "Got ReconfigInit!");
                    if r.nodes.contains(&self.pid) {
                    // TODO pull previous sequence from the nodes in r.nodes
                        self.create_replica(r.config_id, r.nodes);
                        self.active_config_id = r.config_id;
                    }
                }
            },
            am: AtomicBroadcastMsg [AtomicBroadcastSer] => {
                match am {
                    AtomicBroadcastMsg::Proposal(p) => {
                        if !self.stopped {
                            let active_paxos = &self.replicas.get(&self.active_config_id).expect("No active paxos replica").1;
                            active_paxos.actor_ref().tell(PaxosCompMsg::Propose(p));
                        }
                    },
                    AtomicBroadcastMsg::SequenceReq => {
                        if self.replicas.len() > 1 {
                            unimplemented!();
                        } else {
                            let active_paxos = &self.replicas.get(&self.active_config_id).expect("No active paxos replica").1;
                            active_paxos.actor_ref().tell(PaxosCompMsg::SequenceReq(sender));
                        }
                    },
                    _ => unimplemented!(),
                }
            },
            !Err(e) => error!(self.ctx.log(), "Error deserialising msg: {:?}", e),
            }
        }
    }
}

#[derive(ComponentDefinition)]
struct PaxosComp<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    ctx: ComponentContext<Self>,
    supervisor: ActorRef<ReplicaCompMsg<S>>,
    ble_port: RequiredPort<BallotLeaderElection, Self>,
    peers: HashMap<u64, ActorPath>,
    paxos: Paxos<S, P>,
    pid: u64,
    current_leader: u64,
    stopped: bool,
    timers: Option<(ScheduledTimer, ScheduledTimer, ScheduledTimer)>,
    responses: HashMap<u64, u32>,
    cached_client: Option<ActorPath>,
    max_response: u64,
}

impl<S, P> PaxosComp<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    fn with(
        supervisor: ActorRef<ReplicaCompMsg<S>>,
        peers: HashMap<u64, ActorPath>,
        config_id: u32,
        pid: u64
    ) -> PaxosComp<S, P>
    {
        let seq = S::new();
        let paxos_state = P::new();
        let storage = Storage::with(seq, paxos_state);
        let mut raw_peers = vec![];
        for pid in peers.keys() {
            raw_peers.push(pid.clone());
        }
        let paxos = Paxos::with(config_id, pid, raw_peers, storage);
        PaxosComp {
            ctx: ComponentContext::new(),
            supervisor,
            ble_port: RequiredPort::new(),
            peers,
            paxos,
            pid,
            current_leader: 0,
            stopped: false,
            timers: None,
            responses: HashMap::new(),
            cached_client: None,
            max_response: 0
        }
    }

    fn start_timers(&mut self) {
        let decided_timer = self.schedule_periodic(
            Duration::from_millis(0),
            Duration::from_millis(100),
            move |c, _| c.get_decided()
        );
        let outgoing_timer = self.schedule_periodic(
            Duration::from_millis(0),
            Duration::from_millis(1),
            move |p, _| p.get_outgoing()
        );
        let print_timer = self.schedule_periodic(
            Duration::from_secs(5),
            Duration::from_secs(60),
            move |p, _| info!(p.ctx.log(), "{}", p.paxos.get_ld_la())
        );
        self.timers = Some((decided_timer, outgoing_timer, print_timer));
    }

    fn stop_timers(&mut self) {
        if let Some(timers) = self.timers.take() {
            self.cancel_timer(timers.0);
            self.cancel_timer(timers.1);
            self.cancel_timer(timers.2);
        }
    }
    
    fn get_outgoing(&mut self) {
        for out_msg in self.paxos.get_outgoing_messages() {
            if out_msg.to == self.pid {    // TODO handle msgs to self earlier in rawpaxos?
                self.paxos.handle(out_msg);
            } else {
                let receiver = self.peers.get(&out_msg.to).expect(&format!("Actorpath for node id: {} not found", &out_msg.to));
                receiver.tell((out_msg, PaxosSer), self);   // TODO trigger to replica and let replica respond to client instead?
            }
        }
    }

    fn get_decided(&mut self) {
        if self.responses.is_empty() {
           return;
        }
        for decided in self.paxos.get_decided_entries() {
            match decided {
                Entry::Normal(n) => {
                    let id = n.data.as_slice().get_u64();
                    if id > self.max_response {
                        self.max_response = id;
                    }
                    // let p = Proposal::deserialize_normal(&n.data);
                    match self.responses.get_mut(&id) {
                        Some(count) => {
                            *count += 1;
                            if *count == 1 {
                                let pr = ProposalResp::succeeded_normal(id);
                                self.cached_client.as_ref().expect("No cached client").tell((AtomicBroadcastMsg::ProposalResp(pr), AtomicBroadcastSer), self);
                            }
                            else if *count > 1 {
                                panic!("Duplicate response for id: {}, count: {}, max_id: {}", id, count, self.max_response);
                            }
                        },
                        None => unimplemented!(),
                    }
                },
                Entry::StopSign(ss) => {
                    /*if self.responses.remove(&RECONFIG_ID) {
                        unimplemented!();
                    }*/
                    info!(self.ctx.log(), "Decided StopSign!");
                    let final_seq = self.paxos.stop_and_get_sequence();
                    let mut config = HashMap::new();
                    for pid in &ss.nodes {
                        if self.peers.contains_key(pid) {
                            config.insert(*pid, false);
                        } else { // i.e. new node
                            config.insert(*pid, true);
                        }
                    }
                    let r = Reconfiguration::with(ss.config_id, config,final_seq);
                    self.supervisor.tell(ReplicaCompMsg::Reconfig(r));
                }
            }
        }
    }

    fn propose(&mut self, p: Proposal) {
        if let None = self.cached_client {
            self.cached_client = Some(p.client);
        }
        match p.reconfig {
            Some((reconfig, _)) => {
                info!(self.ctx.log(), "Proposing reconfiguration: {:?}", reconfig);
                self.paxos.propose_reconfiguration(reconfig);
                self.responses.insert(RECONFIG_ID, 0);
            }
            None => {
                let mut data: Vec<u8> = vec![];
                data.put_u64(p.id);
                // let data = p.serialize_normal().expect("Failed to serialise proposal");
                self.paxos.propose_normal(data);
                self.responses.insert(p.id, 0);
            }
        }
    }
}

impl<S, P> Actor for PaxosComp<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    type Message = PaxosCompMsg;

    fn receive_local(&mut self, msg: PaxosCompMsg) -> () {  // TODO change to AtomicBroadcastMsg to support SequenceReq
        match msg {
            PaxosCompMsg::Propose(p) => {
                self.propose(p);
                /*if self.current_leader == self.pid {
                    if p.id % 500 == 0 {
                        info!(self.ctx.log(), "Proposing {}", p.id);
                    }
                    self.propose(p);
                } else {
                    if p.id % 500 == 0 {
                        info!(self.ctx.log(), "Forwarding proposal {} to node {}", p.id, self.current_leader);
                    }
                    self.forward_leader(p);
                }*/
            },
            PaxosCompMsg::SequenceReq(requestor) => {
                info!(self.ctx.log(), "Got SequenceReq");
                let mut seq = vec![];
                let entries = self.paxos.get_sequence();
                for e in entries {
                    if let Entry::Normal(n) = e {
                        let p = Proposal::deserialize_normal(&n.data);
                        seq.push(p.id);
                    }
                }
                info!(self.ctx.log(), "Sending SequenceResp");
                let sr = SequenceResp::with(self.pid, seq);
                let am = AtomicBroadcastMsg::SequenceResp(sr);
                requestor.tell((am, AtomicBroadcastSer), self);
            },
            PaxosCompMsg::Stop(s) => {
                self.stopped = true;
                self.stop_timers();
                s.cd.decrement().expect("Failed to countdown stop");
            }
        }
    }

    fn receive_network(&mut self, m: NetMessage) -> () {
        if self.stopped { return; }
        let NetMessage{sender, receiver: _, data} = m;
        match_deser!{data; {
            pm: RawPaxosMsg [PaxosSer] => {
                self.paxos.handle(pm);
            },
            !Err(e) => error!(self.ctx.log(), "Error deserialising msg: {:?}", e),
        }
        }
    }
}

impl<S, P> Provide<ControlPort> for PaxosComp<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    fn handle(&mut self, event: <ControlPort as Port>::Request) -> () {
        match event {
            ControlEvent::Start => {
                info!(self.ctx.log(), "PaxosComp started!");
                self.start_timers();
            },
            _ => {
                self.stop_timers();
            }
        }
    }
}

impl<S, P> Require<BallotLeaderElection> for PaxosComp<S, P> where
    S: SequenceTraits,
    P: PaxosStateTraits
{
    fn handle(&mut self, l: Leader) -> () {
        if !self.stopped {
            info!(self.ctx.log(), "{}", format!("Node {} became leader. Ballot: {:?}", l.pid, l.ballot));
            self.current_leader = l.pid;
            self.paxos.handle_leader(l);
        }
    }
}

pub mod raw_paxos{
    use super::super::messages::paxos::{*};
    use super::super::messages::paxos::ballot_leader_election::{Ballot, Leader};
    use super::super::storage::paxos::Storage;
    use super::{SequenceTraits, PaxosStateTraits};
    use std::fmt::Debug;
    use std::collections::HashMap;
    use std::mem;
    use std::sync::Arc;
    use crate::bench::atomic_broadcast::messages::{ProposalForward, Proposal};
    use crate::bench::atomic_broadcast::paxos::PF_MAX_LEN;
    use kompact::prelude::Buf;

    pub struct Paxos<S, P> where
        S: SequenceTraits,
        P: PaxosStateTraits
    {
        storage: Storage<S, P>,
        config_id: u32,
        pub pid: u64,
        majority: usize,
        peers: Vec<u64>,    // excluding self pid
        state: (Role, Phase),
        leader: u64,
        n_leader: Ballot,
        promises: Vec<ReceivedPromise>,
        las: HashMap<u64, u64>,
        lds: HashMap<u64, u64>,
        proposals: Vec<Entry>,
        lc: u64,    // length of longest chosen seq
        decided: Vec<Entry>, // TODO don't expose entry to client?
        outgoing: Vec<Message>,
        hb_forward: Vec<Entry>,
        num_proposed: u64,
    }

    impl<S, P> Paxos<S, P> where
        S: SequenceTraits,
        P: PaxosStateTraits
    {
        /*** User functions ***/
        pub fn with(
            config_id: u32,
            pid: u64,
            peers: Vec<u64>,
            storage: Storage<S, P>
        ) -> Paxos<S, P> {
            let majority = (&peers.len() + 1)/2 + 1;
            let n_leader = Ballot::with(0, 0);
            Paxos {
                storage,
                pid,
                config_id,
                majority,
                peers,
                state: (Role::Follower, Phase::None),
                leader: 0,  // TODO remove leader and use n_leader.pid instead
                n_leader,
                promises: vec![],
                las: HashMap::new(),
                lds: HashMap::new(),
                proposals: vec![],
                lc: 0,
                decided: vec![],
                outgoing: vec![],
                hb_forward: vec![],
                num_proposed: 0,
            }
        }

        pub fn get_decided_entries(&mut self) -> Vec<Entry> {
            let decided_entries = mem::replace(&mut self.decided, vec![]);
            decided_entries
        }

        pub fn get_outgoing_messages(&mut self) -> Vec<Message> {
            let outgoing_msgs = mem::replace(&mut self.outgoing, vec![]);
            outgoing_msgs
        }

        pub fn handle(&mut self, m: Message) {
            match m.msg {
                PaxosMsg::Prepare(prep) => self.handle_prepare(prep, m.from),
                PaxosMsg::Promise(prom) => {
                    match &self.state {
                        (Role::Leader, Phase::Prepare) => self.handle_promise_prepare(prom, m.from),
                        (Role::Leader, Phase::Accept) => self.handle_promise_accept(prom, m.from),
                        _ => {
                            if !self.proposals.is_empty() {
                                let proposals = mem::replace(&mut self.proposals, vec![]);
                                // println!("Got promise but not leader... Forwarding proposals to {}", self.leader);
                                self.forward_proposals(proposals);
                            }
                        }
                    }
                },
                PaxosMsg::AcceptSync(acc_sync) => self.handle_accept_sync(acc_sync, m.from),
                PaxosMsg::Accept(acc) => self.handle_accept(acc, m.from),
                PaxosMsg::Accepted(accepted) => self.handle_accepted(accepted, m.from),
                PaxosMsg::Decide(d) => self.handle_decide(d),
                PaxosMsg::ProposalForward(proposals) => self.handle_proposal_forward(proposals),
            }
        }

        pub fn propose_normal(&mut self, data: Vec<u8>) {
            let normal_entry = Entry::Normal(self.create_normal_entry(data));
            match self.state {
                (Role::Leader, Phase::Prepare) => {
                    self.proposals.push(normal_entry);
                },
                (Role::Leader, Phase::Accept) => {
                    self.send_accept(normal_entry);
                },
                _ => {  // TODO forward without metadata
                    self.forward_proposals(vec![normal_entry]);
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
                    self.send_accept(entry);
                },
                _ => {  // TODO change to forward
                    panic!("Got propose when not being leader: State: {:?}", self.state);
                }
            }
        }

        pub fn get_sequence(&self) -> Vec<Entry> {
            self.storage.get_sequence()
        }

        pub(crate) fn stop_and_get_sequence(&mut self) -> Arc<S> {
            self.storage.stop_and_get_sequence()
        }

        pub fn get_ld_la(&self) -> String { // TODO REMOVE
            let ld = self.storage.get_decided_len();
            let la = self.storage.get_sequence_len();
            format!("ld: {}, la: {}", ld, la)
        }

        fn create_normal_entry(&mut self, data: Vec<u8>) -> Normal {
            self.num_proposed += 1;
            let metadata = EntryMetaData::with(self.pid, self.num_proposed);
            Normal{ metadata, data }
        }

        fn clear_state(&mut self) {
            self.las = HashMap::new();
            self.promises = vec![];
            self.lds = HashMap::new();
        }

        /*** Leader ***/
        pub fn handle_leader(&mut self, l: Leader) {
            let n = l.ballot;
            if n <= self.n_leader {
                return;
            }
            self.clear_state();
            if self.pid == l.pid && n > self.n_leader {
                self.n_leader = n.clone();
                self.leader = n.pid;
                self.storage.set_promise(n.clone());
                /* insert my promise */
                let na = self.storage.get_accepted_ballot();
                let sfx = self.storage.get_decided_suffix();    // TODO get serialised instead
                let rp = ReceivedPromise::with( na, sfx);
                self.promises.push(rp);
                /* insert my longest decided sequnce */
                let ld = self.storage.get_decided_len();
                self.lds.insert(self.pid, ld);
                /* initialise longest chosen sequence and update state */
                self.lc = 0;
                self.state = (Role::Leader, Phase::Prepare);
                /*if !self.hb_forward.is_empty(){
                    println!("Appending hb proposals, len: {}", self.hb_forward.len());
                    self.proposals.append(self.hb_forward.as_mut());
                }*/
                /* send prepare */
                for pid in &self.peers {
                    let prep = Prepare::with(n.clone(), ld, self.storage.get_accepted_ballot());
                    self.outgoing.push(Message::with(self.pid, *pid, PaxosMsg::Prepare(prep)));
                }
            } else {
                self.state.0 = Role::Follower;
                self.leader = n.pid;
                if !self.proposals.is_empty() {
                    let proposals = mem::replace(&mut self.proposals, vec![]);
                    // println!("Lost leadership: Forwarding proposals to {}", self.leader);
                    self.forward_proposals(proposals);
                }
                if !self.hb_forward.is_empty() {
                    let hb_proposals = mem::replace(&mut self.hb_forward, vec![]);
                    // println!("Lost leadership: Forwarding hb-proposals to {}", self.leader);
                    self.forward_proposals(hb_proposals);
                }
            }
        }

        fn forward_proposals(&mut self, proposals: Vec<Entry>) {
            if self.leader == 0 {
                let mut p = proposals;
                self.hb_forward.append(&mut p);
            } else {
                /*for chunk in proposals.chunks(PF_MAX_LEN) {
                    let pf = PaxosMsg::ProposalForward(chunk.to_vec());
                    let msg = Message::with(self.pid, self.leader, pf);
                    self.outgoing.push(msg);
                }*/
                let pf = PaxosMsg::ProposalForward(proposals);
                let msg = Message::with(self.pid, self.leader, pf);
                self.outgoing.push(msg);
            }
        }

        fn handle_proposal_forward(&mut self, proposals: Vec<Entry>) {
            if !self.storage.stopped() {
                match self.state {
                    (Role::Leader, Phase::Prepare) => {
                        let mut p = proposals;
                        // println!("Appending forwarded proposals: len: {}, la: {}", p.len(), self.storage.get_sequence_len());
                        self.proposals.append(&mut p)
                    },
                    (Role::Leader, Phase::Accept) => {
                        // println!("Sending accept forwarded proposals: len: {}, la: {}", proposals.len(), self.storage.get_sequence_len());
                        for p in proposals {
                            self.send_accept(p);
                        }
                    }
                    _ => {
                        // println!("Not leader when receiving forwarded proposal... leader: {}", self.leader);
                        self.forward_proposals(proposals);
                    },
                }
            }
        }

        fn send_accept(&mut self, entry: Entry) {
            if !self.storage.stopped() {
                self.storage.append_entry(entry.clone(), true);
                self.las.insert(self.pid, self.storage.get_sequence_len());
                // println!("Sending accept la: {}, {:?}", self.storage.get_sequence_len(), self.n_leader);
                for pid in self.lds.keys() {
                    if pid != &self.pid {
                        let acc = Accept::with(self.n_leader.clone(), entry.clone());
                        self.outgoing.push(Message::with(self.pid, *pid, PaxosMsg::Accept(acc)));
                    }
                }
            }
        }

        fn handle_promise_prepare(&mut self, prom: Promise, from: u64) {
            if prom.n == self.n_leader {
                let rp = ReceivedPromise::with(prom.n_accepted, prom.sfx);
                self.promises.push(rp);
                self.lds.insert(from, prom.ld);
                if self.promises.len() >= self.majority {
                    let suffix = Self::max_value(&self.promises);
                    let last_is_stop = match suffix.last() {
                        Some(e) => e.is_stopsign(),
                        None => false
                    };
                    // println!("Prepare completed: suffix len={}, ld: {}, la before: {}", suffix.len(), self.storage.get_decided_len(), self.storage.get_sequence_len());
                    self.storage.append_on_decided_prefix(suffix);
                    if last_is_stop {
                        self.proposals = vec![];    // will never be decided
                    } else {
                        /*if !self.proposals.is_empty() {
                            Self::drop_after_stopsign(&mut self.proposals); // drop after ss, if ss exists
                            // TODO remove
                            let first = if let Entry::Normal(first) = self.proposals.first().unwrap() {
                                first.data.as_slice().get_u64()
                            } else { 0 };
                            let last = if let Entry::Normal(last) = self.proposals.first().unwrap() {
                                last.data.as_slice().get_u64()
                            } else { 0 };
                            println!("Appending proposals id: {}-{}", first, last);
                        }*/
                        self.storage.append_sequence(&mut self.proposals);
                        self.las.insert(self.pid, self.storage.get_sequence_len());
                        self.state = (Role::Leader, Phase::Accept);
                    }
                    // println!("Leader Prepare completed");
                    assert!(self.lds.len() >= self.majority);
                    // println!("Sending AcceptSync la: {}", self.storage.get_sequence_len());
                    for (pid, lds) in self.lds.iter() {
//                        if *lds != va_len {
                        if *pid != self.pid {
                            /*if *lds > self.storage.get_sequence_len() {
                                panic!(
                                    "promise_prepare from node {}. ld: {}, my seq_len: {}. Suffix len: {}, my ld: {}",
                                    pid,
                                    lds,
                                    self.storage.get_sequence_len(),
                                    suffix.len(),
                                    self.storage.get_decided_len()
                                );
                            }*/
                            let sfx = self.storage.get_suffix(*lds);
                            let acc_sync = AcceptSync::with(self.n_leader.clone(), sfx, *lds);
                            self.outgoing.push(Message::with(self.pid, *pid, PaxosMsg::AcceptSync(acc_sync)));
                        }
                    }
                }
            } /*else {
                println!("Got old promise in prepare: {:?}, leader: {:?}", prom.n, self.n_leader);
            }*/
        }

        fn handle_promise_accept(&mut self, prom: Promise, from: u64) {
            if prom.n == self.n_leader {
                self.lds.insert(from, prom.ld);
                if prom.ld > self.storage.get_sequence_len() {
                    panic!(
                        "promise_accept from node {}. ld: {}, my seq_len: {}, my ld: {}",
                        from,
                        prom.ld,
                        self.storage.get_sequence_len(),
                        self.storage.get_decided_len()
                    );
                }
                let sfx = self.storage.get_suffix(prom.ld);
                let acc_sync = AcceptSync::with(self.n_leader.clone(), sfx, prom.ld);
                self.outgoing.push(Message::with(self.pid, from, PaxosMsg::AcceptSync(acc_sync)));
                if self.lc != 0 {
                    // inform what got decided already
                    let d = Decide::with(self.lc, self.n_leader.clone());
                    self.outgoing.push(Message::with(self.pid, from, PaxosMsg::Decide(d)));
                }
            } /*else {
                println!("Got promise for {:?} but leader ballot: {:?}", prom.n, self.n_leader);
            }*/
        }

        fn handle_accepted(&mut self, accepted: Accepted, from: u64) {
            if accepted.n == self.n_leader && self.state == (Role::Leader, Phase::Accept) {
                self.las.insert(from, accepted.la);
                if &accepted.la <= &self.lc {
                    return;
                }
                let mut counter = 0;
                for (_pid, la) in &self.las {
                    if la >= &accepted.la {
                        counter += 1;   // TODO Break when counter == majority
                    }
                }
                if counter >= self.majority {
                    self.lc = accepted.la;
                    self.storage.set_pending_chosen_offset(self.lc);
                    let d = Decide::with(self.lc, self.n_leader.clone());
                    for pid in self.lds.keys() {
                        self.outgoing.push(Message::with(self.pid, *pid, PaxosMsg::Decide(d.clone())));
                    }
                }
            }
        }

        /*** Follower ***/
        fn handle_prepare(&mut self, prep: Prepare, from: u64) {
            if &self.storage.get_promise() < &prep.n {
                self.storage.set_promise(prep.n.clone());
                self.state = (Role::Follower, Phase:: Prepare);
                let na = self.storage.get_accepted_ballot();
                let suffix = if &na >= &prep.n_accepted {
                    if prep.ld > self.storage.get_sequence_len() {
                        panic!(
                            "ld in Prepare is longer than my accepted seq: ld: {}, la: {}, ballot: {:?}, my accepted ballot: {:?}",
                            prep.ld,
                            self.storage.get_sequence_len(),
                            prep.n_accepted,
                            na
                        )
                    }
                    self.storage.get_suffix(prep.ld)
                } else {
                    vec![]
                };
                let p = Promise::with(prep.n, na, suffix, self.storage.get_decided_len());
                self.outgoing.push(Message::with(self.pid, from, PaxosMsg::Promise(p)));
            } /*else {
                println!("Old prepare: {:?}, promised: {:?}", prep.n, self.storage.get_promise());
            }*/
        }

        fn handle_accept_sync(&mut self, acc_sync: AcceptSync, from: u64) {
            if self.state == (Role::Follower, Phase::Prepare) {
                if self.storage.get_promise() == acc_sync.n {
                    self.storage.set_accepted_ballot(acc_sync.n.clone());
                    let mut sfx = acc_sync.sfx;
                    let discarded_entries = self.storage.append_on_prefix(acc_sync.ld, &mut sfx);
                    self.state = (Role::Follower, Phase::Accept);
                    let accepted = Accepted::with(acc_sync.n, self.storage.get_sequence_len());
                    self.outgoing.push(Message::with(self.pid, from, PaxosMsg::Accepted(accepted)));
                    if !discarded_entries.is_empty() {  // forward the proposals that I failed to decide
                        let start = match discarded_entries.first().unwrap() {
                            Entry::Normal(n) => {
                                n.data.as_slice().get_u64()
                            },
                            _ => 0,
                        };
                        let end = match discarded_entries.last().unwrap() {
                            Entry::Normal(n) => {
                                n.data.as_slice().get_u64()
                            },
                            _ => 0,
                        };
                        println!("UNEMPTY DISCARDED ENTRIES: {}: {}-{}. Forwarding them to {}", discarded_entries.len(), start, end, self.leader);
                        self.forward_proposals(discarded_entries);
                    }
                }
            }
        }

        fn handle_accept(&mut self, acc: Accept, from: u64) {
            if self.state == (Role::Follower, Phase::Accept) {
                if self.storage.get_promise() == acc.n {
                    self.storage.append_entry(acc.entry, false);
                    let accepted = Accepted::with(acc.n, self.storage.get_sequence_len());
                    self.outgoing.push(Message::with(self.pid, from, PaxosMsg::Accepted(accepted)));
                }
            }
        }

        fn handle_decide(&mut self, dec: Decide) {
            if self.storage.get_promise() == dec.n {
                let prev_ld = self.storage.set_decided_len(dec.ld);
                // println!("Deciding: prev_ld: {}, ld: {}", prev_ld, dec.ld);
                if prev_ld < dec.ld {
                    let mut decided_entries = self.storage.get_entries(prev_ld, dec.ld);
                    self.decided.append(&mut decided_entries);
                }
            }
        }

        /*** algorithm specific functions ***/
        fn max_value(promises: &Vec<ReceivedPromise>) -> Vec<Entry> {
            let mut max_n: &Ballot = &promises[0].n_accepted;
            let mut max_sfx: &Vec<Entry> = &promises[0].sfx;
            for p in promises {
                if &p.n_accepted > max_n {
                    max_n = &p.n_accepted;
                    max_sfx = &p.sfx;
                } else if &p.n_accepted == max_n && &p.sfx.len() > &max_sfx.len() {
                    max_sfx = &p.sfx;
                }
            }
            max_sfx.clone()
        }

        fn drop_after_stopsign(entries: &mut Vec<Entry>) {   // drop all entries ordered after stopsign (if any)
            for (idx, e) in entries.iter().enumerate() {
                if e.is_stopsign() {
                    entries.truncate(idx + 1);
                    return;
                }
            }
        }
    }

    #[derive(PartialEq, Debug)]
    enum Phase {
        Prepare,
        Accept,
        None
    }

    #[derive(PartialEq, Debug)]
    enum Role {
        Follower,
        Leader
    }

    struct ReceivedPromise {
        n_accepted: Ballot,
        sfx: Vec<Entry>
    }

    impl ReceivedPromise {
        fn with(n_accepted: Ballot, sfx: Vec<Entry>) -> ReceivedPromise {
            ReceivedPromise { n_accepted, sfx }
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

    impl PartialEq for StopSign {
        fn eq(&self, other: &Self) -> bool {
            unimplemented!()
        }

        fn ne(&self, other: &Self) -> bool {
            unimplemented!()
        }
    }

    #[derive(Clone, Debug)]
    pub struct Normal {
        pub metadata: EntryMetaData,
        pub data: Vec<u8>,
    }

    impl PartialEq for Normal {
        fn eq(&self, other: &Self) -> bool { self.metadata == other.metadata }

        fn ne(&self, other: &Self) -> bool { self.metadata != other.metadata }
    }

    impl Normal {
        pub fn with(metadata: EntryMetaData, data: Vec<u8>) -> Normal {
            Normal{ metadata, data }
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    pub struct EntryMetaData {
        pub proposed_by: u64,
        pub n: u64,
    }

    impl EntryMetaData {
        pub fn with(proposed_by: u64, n: u64) -> EntryMetaData {
            EntryMetaData{ proposed_by, n }
        }
    }


    #[derive(Clone, Debug, PartialEq)]
    pub enum Entry {
        Normal(Normal),
        StopSign(StopSign)
    }

    impl Entry {
        pub(crate) fn is_stopsign(&self) -> bool {
            match self {
                Entry::StopSign(_) => true,
                _ => false
            }
        }
    }

    /*impl PartialEq for Entry {
        fn eq(&self, other: &Self) -> bool {

        }

        fn ne(&self, other: &Self) -> bool {
            unimplemented!()
        }
    }*/
}

mod ballot_leader_election {
    use super::*;
    use super::super::messages::{paxos::ballot_leader_election::*};
    use std::time::Duration;

    pub struct BallotLeaderElection;

    impl Port for BallotLeaderElection {
        type Indication = Leader;
        type Request = ();
    }

    #[derive(ComponentDefinition)]
    pub struct BallotLeaderComp {   // TODO decouple from kompact, similar style to tikv_raft with tick() replacing timers
        ctx: ComponentContext<Self>,
        ble_port: ProvidedPort<BallotLeaderElection, Self>,
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
        stopped: bool,
        timer: Option<ScheduledTimer>
    }

    impl BallotLeaderComp {
        pub fn with(peers: Vec<ActorPath>, pid: u64, delta: u64) -> BallotLeaderComp {
            BallotLeaderComp {
                ctx: ComponentContext::new(),
                ble_port: ProvidedPort::new(),
                pid,
                majority: (&peers.len() + 1)/2 + 1, // +1 because peers is exclusive ourselves
                peers,
                round: 0,
                ballots: vec![],
                current_ballot: Ballot::with(0, pid),
                leader: None,
                max_ballot: Ballot::with(0, pid),
                hb_delay: delta,
                delta,
                stopped: false,
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
                if self.leader != Some((top_pid, top_ballot)) {
                    self.max_ballot = top_ballot;
                    self.leader = Some((top_pid, top_ballot));
                    self.ble_port.trigger(Leader::with(top_pid, top_ballot));
                }
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
            let timer = self.schedule_periodic(
                Duration::from_millis(0),
                Duration::from_millis(self.hb_delay),
                move |c, _| c.hb_timeout()
            );
            self.timer = Some(timer);
        }

        fn stop_timer(&mut self) {
            if let Some(timer) = self.timer.take() {
                self.cancel_timer(timer);
            }
        }
    }

    impl Provide<ControlPort> for BallotLeaderComp {
        fn handle(&mut self, event: <ControlPort as Port>::Request) -> () {
            // ignore
            match event {
                ControlEvent::Start => {
                    info!(self.ctx.log(), "BLE started!");
                    self.start_timer();
                },
                _ => self.stop_timer(),
            }
        }
    }

    impl Provide<BallotLeaderElection> for BallotLeaderComp {
        fn handle(&mut self, _: <BallotLeaderElection as Port>::Request) -> () {
            unimplemented!()
        }
    }

    impl Actor for BallotLeaderComp {
        type Message = Stop;

        fn receive_local(&mut self, msg: Stop) -> () {
            msg.cd.decrement().expect("Failed to countdown stop latch");
        }

        fn receive_network(&mut self, m: NetMessage) -> () {
            if self.stopped { return; }
            let NetMessage{sender, receiver, data} = m;
            match_deser!{data; {
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
                            } else {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::client::{Client, tests::TestClient};
    use crate::partitioning_actor::{PartitioningActor, IterationControlMsg};
    use synchronoise::CountdownEvent;
    use std::sync::Arc;
    use super::super::messages::Run;
    use crate::bench::atomic_broadcast::paxos::raw_paxos::{EntryMetaData, Normal};

    fn create_replica_nodes(n: u64, initial_conf: Vec<u64>) -> (Vec<KompactSystem>, HashMap<u64, ActorPath>) {
        let mut systems = vec![];
        let mut nodes = HashMap::new();
        for i in 1..=n {
            let system =
                kompact_benchmarks::kompact_system_provider::global().new_remote_system_with_threads(format!("paxos_replica{}", i), 4);
            let (replica_comp, unique_reg_f) = system.create_and_register(|| {
                ReplicaComp::<MemorySequence, MemoryState>::with(initial_conf.clone(), TransferPolicy::Passive)
            });
            unique_reg_f.wait_expect(
                Duration::from_millis(1000),
                "ReplicaComp failed to register!",
            );
            let replica_comp_f = system.start_notify(&replica_comp);
            replica_comp_f
                .wait_timeout(Duration::from_millis(1000))
                .expect("ReplicaComp never started!");

            let named_reg_f = system.register_by_alias(
                &replica_comp,
                format!("replica{}", i),
            );
            let self_path = ActorPath::Named(NamedPath::with_system(
                system.system_path(),
                vec![format!("replica{}", i).into()],
            ));
            systems.push(system);
            nodes.insert(i, self_path);

        }
        (systems, nodes)
    }

    #[test]
    fn paxos_test() {
        let n: u64 = 3;
        let active_n: u64 = 3;
        let quorum = active_n/2 + 1;
        let num_proposals = 4000;
        let batch_size = 1000;
        let config = vec![1,2,3];
        let reconfig = None;
        // let reconfig = Some((vec![1,4,5], vec![]));
        let check_sequences = false;

        let (systems, nodes) = create_replica_nodes(n, config);
        let mut actorpaths = vec![];
        nodes.iter().for_each(|x| actorpaths.push(x.1.clone()));
        /*** Setup partitioning actor ***/
        let prepare_latch = Arc::new(CountdownEvent::new(1));
        let (partitioning_actor, unique_reg_f) = systems[0].create_and_register(|| {
            PartitioningActor::with(
                prepare_latch.clone(),
                None,
                1,
                actorpaths,
                None,
            )
        });
        unique_reg_f.wait_expect(
            Duration::from_millis(1000),
            "PartitioningComp failed to register!",
        );

        let partitioning_actor_f = systems[0].start_notify(&partitioning_actor);
        partitioning_actor_f
            .wait_timeout(Duration::from_millis(1000))
            .expect("PartitioningComp never started!");
        partitioning_actor.actor_ref().tell(IterationControlMsg::Prepare(None));
        prepare_latch.wait();
        /*** Setup client ***/
        let (p, f) = kpromise::<HashMap<u64, Vec<u64>>>();
        let (client, unique_reg_f) = systems[0].create_and_register( || {
            TestClient::with(
                num_proposals,
                batch_size,
                nodes,
                reconfig,
                p,
                check_sequences,
            )
        });
        unique_reg_f.wait_expect(
            Duration::from_millis(1000),
            "Client failed to register!",
        );
        let client_f = systems[0].start_notify(&client);
        client_f.wait_timeout(Duration::from_millis(1000))
            .expect("Client never started!");
        client.actor_ref().tell(Run);
        let all_sequences = f.wait_timeout(Duration::from_secs(60)).expect("Failed to get results");
        let client_sequence = all_sequences.get(&0).expect("Client's sequence should be in 0...").to_owned();
        for system in systems {
            system
                .shutdown()
                .expect("Kompact didn't shut down properly");
        }

        assert_eq!(num_proposals, client_sequence.len() as u64);
        for i in 1..=num_proposals {
            let mut iter = client_sequence.iter();
            let found = iter.find(|&&x| x == i).is_some();
            if !found {
                panic!("Did not find value {} in client's sequence", i);
            }
        }

        if check_sequences {
            let mut counter = 0;
            for i in 1..=n {
                let sequence = all_sequences.get(&i).expect(&format!("Did not get sequence for node {}", i));
                println!("Node {}: {:?}", i, sequence.len());
                assert!(client_sequence.starts_with(sequence));
                if sequence.starts_with(&client_sequence) {
                    counter += 1;
                }
            }
            if counter < quorum {
                panic!("Majority should have decided sequence: counter: {}, quorum: {}", counter, quorum);
            }
        }
        println!("PASSED!!!");
    }

    #[test]
    fn paxos_entry_test() {
        let metadata = EntryMetaData::with(1,2);
        let data = vec![1,2,3];
        let data2 = vec![3,2,1];
        let entry = Entry::Normal(Normal::with(metadata.clone(), data));
        let entry2 = Entry::Normal(Normal::with(metadata, data2));
        let mut ents = vec![entry.clone()];
        if !ents.contains(&entry) {
            panic!("Should have entry!");
        }
        if !ents.contains(&entry2) {
            panic!("Should have entry2!");
        }

    }
}
