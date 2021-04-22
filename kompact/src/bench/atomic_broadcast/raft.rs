extern crate raft as tikv_raft;

use self::tikv_raft::Configuration;
use super::{
    messages::{StopMsg as NetStopMsg, StopMsgDeser, *},
    storage::raft::*,
};
#[cfg(test)]
use crate::bench::atomic_broadcast::atomic_broadcast::tests::SequenceResp;
use crate::{
    bench::atomic_broadcast::{
        atomic_broadcast::Done,
        client::create_raw_proposal,
        communicator::{AtomicBroadcastCompMsg, CommunicationPort, Communicator, CommunicatorMsg},
    },
    partitioning_actor::{PartitioningActorMsg, PartitioningActorSer},
    serialiser_ids::ATOMICBCAST_ID,
};
use hashbrown::{HashMap, HashSet};
use kompact::prelude::*;
use protobuf::Message as PbMessage;
use rand::Rng;
use std::{borrow::Borrow, clone::Clone, marker::Send, ops::DerefMut, sync::Arc, time::Duration};
use tikv_raft::{
    prelude::{Entry, Message as TikvRaftMsg, *},
    StateRole,
};

const COMMUNICATOR: &str = "communicator";
const DELAY: Duration = Duration::from_millis(0);

#[derive(Debug)]
pub enum RaftCompMsg {
    Leader(bool, u64),
    Removed,
    ForwardReconfig(u64, ReconfigurationProposal),
    KillComponents(Ask<(), Done>),
    #[cfg(test)]
    GetSequence(Ask<(), SequenceResp>),
}

#[derive(ComponentDefinition)]
pub struct RaftComp<S>
where
    S: RaftStorage + Send + Clone + 'static,
{
    ctx: ComponentContext<Self>,
    pid: u64,
    initial_config: Vec<u64>,
    raft_replica: Option<Arc<Component<RaftReplica<S>>>>,
    communicator: Option<Arc<Component<Communicator>>>,
    peers: HashMap<u64, ActorPath>,
    removed: bool,
    iteration_id: u32,
    stopped: bool,
    partitioning_actor: Option<ActorPath>,
    cached_client: Option<ActorPath>,
    current_leader: u64,
}

impl<S> RaftComp<S>
where
    S: RaftStorage + Send + Clone + 'static,
{
    pub fn with(initial_config: Vec<u64>) -> Self {
        RaftComp {
            ctx: ComponentContext::uninitialised(),
            pid: 0,
            initial_config,
            raft_replica: None,
            communicator: None,
            peers: HashMap::new(),
            removed: false,
            iteration_id: 0,
            stopped: false,
            partitioning_actor: None,
            cached_client: None,
            current_leader: 0,
        }
    }

    fn create_rawraft_config(&self) -> Config {
        let config = self.ctx.config();
        let max_inflight_msgs = config["experiment"]["max_inflight"]
            .as_i64()
            .expect("Failed to load max_inflight") as usize;
        let election_timeout = config["experiment"]["election_timeout"]
            .as_i64()
            .expect("Failed to load election_timeout") as usize;
        let tick_period = config["raft"]["tick_period"]
            .as_i64()
            .expect("Failed to load tick_period") as usize;
        let leader_hb_period = config["raft"]["leader_hb_period"]
            .as_i64()
            .expect("Failed to load leader_hb_period") as usize;
        let max_batch_size = config["raft"]["max_batch_size"]
            .as_i64()
            .expect("Failed to load max_batch_size") as u64;
        let pre_vote = config["raft"]["pre_vote"]
            .as_bool()
            .expect("Failed to load pre_vote");
        let check_quorum = config["raft"]["check_quorum"]
            .as_bool()
            .expect("Failed to load check_quorum");
        // convert from ms to logical clock ticks
        let election_tick = election_timeout / tick_period;
        let heartbeat_tick = leader_hb_period / tick_period;
        // info!(self.ctx.log(), "RawRaft config: election_tick={}, heartbeat_tick={}", election_tick, heartbeat_tick);
        let max_size_per_msg = max_batch_size;
        let c = Config {
            id: self.pid,
            election_tick,  // number of ticks without HB before starting election
            heartbeat_tick, // leader sends HB every heartbeat_tick
            max_inflight_msgs,
            max_size_per_msg,
            batch_append: true,
            pre_vote,
            check_quorum,
            ..Default::default()
        };
        assert!(c.validate().is_ok(), "Invalid RawRaft config");
        c
    }

    fn reset_state(&mut self) {
        self.pid = 0;
        self.peers.clear();
        self.removed = false;
        self.iteration_id = 0;
        self.stopped = false;
        self.current_leader = 0;
        self.cached_client = None;
        self.partitioning_actor = None;

        self.raft_replica = None;
        self.communicator = None;
    }

    fn create_components(&mut self) -> Handled {
        let mut communicator_peers: HashMap<u64, ActorPath> =
            HashMap::with_capacity(self.peers.len());
        for (pid, ap) in &self.peers {
            match ap {
                ActorPath::Named(n) => {
                    let sys_path = n.system();
                    let protocol = sys_path.protocol();
                    let port = sys_path.port();
                    let addr = sys_path.address();
                    let named_communicator = NamedPath::new(
                        protocol,
                        *addr,
                        port,
                        vec![format!("{}{}-{}", COMMUNICATOR, pid, self.iteration_id)],
                    );
                    communicator_peers.insert(*pid, ActorPath::Named(named_communicator));
                }
                _ => unimplemented!(),
            }
        }

        let system = self.ctx.system();
        let conf_state: (Vec<u64>, Vec<u64>) = (self.initial_config.clone(), vec![]);
        let store = if cfg!(feature = "preloaded_log") && self.initial_config.contains(&self.pid) {
            let size = self.ctx.config()["experiment"]["preloaded_log_size"]
                .as_i64()
                .expect("Failed to load preloaded_log_size") as u64;
            let mut preloaded_log = Vec::with_capacity(size as usize);
            for id in 1..=size {
                let mut entry = Entry::default();
                let data = create_raw_proposal(id);
                entry.data = data;
                entry.index = id + 1;
                preloaded_log.push(entry);
            }
            S::new_with_entries_and_conf_state(None, preloaded_log.as_slice(), conf_state)
        } else {
            S::new_with_conf_state(None, conf_state)
        };
        let raw_raft =
            RawNode::new(&self.create_rawraft_config(), store).expect("Failed to create tikv Raft");
        let max_inflight = self.ctx.config()["experiment"]["max_inflight"]
            .as_i64()
            .expect("Failed to load max_inflight") as usize;
        let (raft_replica, raft_f) = system.create_and_register(|| {
            RaftReplica::with(raw_raft, self.actor_ref(), self.peers.len(), max_inflight)
        });
        let (communicator, comm_f) = system.create_and_register(|| {
            Communicator::with(
                communicator_peers,
                self.cached_client
                    .as_ref()
                    .expect("No cached client")
                    .clone(),
            )
        });
        let communicator_alias = format!("{}{}-{}", COMMUNICATOR, self.pid, self.iteration_id);
        let comm_alias_f = system.register_by_alias(&communicator, communicator_alias);
        biconnect_components::<CommunicationPort, _, _>(&communicator, &raft_replica)
            .expect("Could not connect components!");
        self.raft_replica = Some(raft_replica);
        self.communicator = Some(communicator);
        Handled::block_on(self, move |mut async_self| async move {
            raft_f
                .await
                .unwrap()
                .expect("Timed out registering raft component");
            comm_f
                .await
                .unwrap()
                .expect("Timed out registering communicator");
            comm_alias_f
                .await
                .unwrap()
                .expect("Timed out registering communicator alias");
            async_self
                .partitioning_actor
                .take()
                .expect("No partitioning actor found!")
                .tell_serialised(
                    PartitioningActorMsg::InitAck(async_self.iteration_id),
                    async_self.deref_mut(),
                )
                .expect("Should serialise InitAck");
        })
    }

    fn start_components(&self) {
        let raft = self.raft_replica.as_ref().expect("No raft comp to start!");
        let communicator = self
            .communicator
            .as_ref()
            .expect("No communicator to start!");
        self.ctx.system().start(raft);
        self.ctx.system().start(communicator);
    }

    fn stop_components(&mut self) -> Handled {
        self.stopped = true;
        // info!(self.ctx.log(), "Stopping components");
        let raft = self
            .raft_replica
            .as_ref()
            .expect("Got stop but no Raft replica");
        let stop_f = raft
            .actor_ref()
            .ask_with(|p| RaftReplicaMsg::Stop(Ask::new(p, ())));
        Handled::block_on(self, move |_| async move {
            stop_f.await.expect("Failed to stop RaftReplica");
        })
    }

    fn kill_components(&mut self, ask: Ask<(), Done>) -> Handled {
        let system = self.ctx.system();
        let mut kill_futures = Vec::with_capacity(2);

        if let Some(raft) = self.raft_replica.take() {
            let kill_raft = system.kill_notify(raft);
            kill_futures.push(kill_raft);
        }
        if let Some(communicator) = self.communicator.take() {
            let kill_comm = system.kill_notify(communicator);
            kill_futures.push(kill_comm);
        }

        Handled::block_on(self, move |_| async move {
            for f in kill_futures {
                f.await.expect("Failed to kill");
            }
            ask.reply(Done).expect("Failed to reply done");
        })
    }
}

impl<S> ComponentLifecycle for RaftComp<S> where S: RaftStorage + Send + Clone + 'static {}

impl<S> Actor for RaftComp<S>
where
    S: RaftStorage + Send + Clone + 'static,
{
    type Message = RaftCompMsg;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        match msg {
            RaftCompMsg::Leader(notify_client, pid) => {
                debug!(self.ctx.log(), "Node {} became leader", pid);
                if notify_client {
                    self.cached_client
                        .as_ref()
                        .expect("No cached client!")
                        .tell_serialised(AtomicBroadcastMsg::Leader(pid), self)
                        .expect("Should serialise FirstLeader");
                }
                self.current_leader = pid
            }
            RaftCompMsg::Removed => {
                self.removed = true;
            }
            RaftCompMsg::ForwardReconfig(leader_pid, rp) => {
                self.current_leader = leader_pid;
                self.peers
                    .get(&leader_pid)
                    .unwrap_or_else(|| {
                        panic!("No actorpath to current leader: {}", self.current_leader)
                    })
                    .tell_serialised(AtomicBroadcastMsg::ReconfigurationProposal(rp), self)
                    .expect("Should serialise")
            }
            RaftCompMsg::KillComponents(ask) => {
                return self.kill_components(ask);
            }
            #[cfg(test)]
            RaftCompMsg::GetSequence(ask) => {
                let raft_replica = self.raft_replica.as_ref().expect("No raft replica");
                let seq = raft_replica
                    .actor_ref()
                    .ask_with(|promise| RaftReplicaMsg::SequenceReq(Ask::new(promise, ())))
                    .wait();
                let sr = SequenceResp::with(self.pid, seq);
                ask.reply(sr).expect("Failed to reply SequenceResp");
            }
        }
        Handled::Ok
    }

    fn receive_network(&mut self, m: NetMessage) -> Handled {
        match m.data.ser_id {
            ATOMICBCAST_ID => {
                if !self.stopped {
                    if self.removed {
                        self.cached_client
                            .as_ref()
                            .expect("No cached client!")
                            .forward_with_original_sender(m, self);
                    } else if self.current_leader == self.pid || self.current_leader == 0 {
                        // if no leader, raftcomp will hold back
                        match_deser! {m {
                            msg(am): AtomicBroadcastMsg [using AtomicBroadcastDeser] => {
                                match am {
                                    AtomicBroadcastMsg::Proposal(p) => {
                                        self.raft_replica
                                            .as_ref()
                                            .expect("No active RaftComp")
                                            .actor_ref()
                                            .tell(RaftReplicaMsg::Propose(p));
                                    }
                                    AtomicBroadcastMsg::ReconfigurationProposal(rp) => {
                                        self.raft_replica
                                            .as_ref()
                                            .expect("No active RaftComp")
                                            .actor_ref()
                                            .tell(RaftReplicaMsg::ProposeReconfiguration(rp));
                                    }
                                    _ => {}
                                }
                            }
                        }}
                    } else if self.current_leader > 0 {
                        let leader = self.peers.get(&self.current_leader).unwrap_or_else(|| {
                            panic!(
                                "Could not get leader's actorpath. Pid: {}",
                                self.current_leader
                            )
                        });
                        leader.forward_with_original_sender(m, self);
                    }
                }
            }
            _ => {
                let NetMessage { sender, data, .. } = m;
                match_deser! {data {
                    msg(p): PartitioningActorMsg [using PartitioningActorSer] => {
                        match p {
                            PartitioningActorMsg::Init(init) => {
                                self.reset_state();
                                info!(self.ctx.log(), "Raft got init, pid: {}", init.pid);
                                self.iteration_id = init.init_id;
                                let my_pid = init.pid as u64;
                                let ser_client = init.init_data.expect("Init should include ClientComp's actorpath");
                                let client = ActorPath::deserialise(&mut ser_client.as_slice()).expect("Failed to deserialise Client's actorpath");
                                self.cached_client = Some(client);
                                self.peers = init.nodes.into_iter().enumerate().map(|(idx, ap)| (idx as u64 + 1, ap)).filter(|(pid, _)| pid != &my_pid).collect();
                                self.pid = my_pid;
                                self.partitioning_actor = Some(sender);
                                self.stopped = false;
                                let handled = self.create_components();
                                return handled;
                            },
                            PartitioningActorMsg::Run => {
                                self.start_components();
                            },
                            _ => {},
                        }
                    },
                    msg(client_stop): NetStopMsg [using StopMsgDeser] => {
                        if let NetStopMsg::Client = client_stop {
                            // info!(self.ctx.log(), "Got client stop");
                            assert!(!self.stopped);
                            return self.stop_components();
                        }
                    },
                    err(e) => error!(self.ctx.log(), "Error deserialising msg: {:?}", e),
                    default(_) => unimplemented!("Expected either PartitioningActorMsg or NetStopMsg!"),
                }
                }
            }
        }
        Handled::Ok
    }
}

#[derive(Debug)]
pub enum RaftReplicaMsg {
    Propose(Proposal),
    ProposeReconfiguration(ReconfigurationProposal),
    Stop(Ask<(), ()>),
    #[cfg(test)]
    SequenceReq(Ask<(), Vec<u64>>),
}

#[derive(Debug, PartialEq)]
enum ReconfigurationState {
    None,
    Pending,
    Finished,
    Removed,
}

#[derive(Debug, PartialEq)]
enum State {
    Election,
    Running,
}

#[derive(ComponentDefinition)]
pub struct RaftReplica<S>
where
    S: RaftStorage + Send + Clone + 'static,
{
    ctx: ComponentContext<Self>,
    supervisor: ActorRef<RaftCompMsg>,
    state: State,
    raw_raft: RawNode<S>,
    communication_port: RequiredPort<CommunicationPort>,
    timers: Option<(ScheduledTimer, ScheduledTimer)>,
    reconfig_state: ReconfigurationState,
    current_leader: u64,
    num_peers: usize,
    stopped: bool,
    stopped_peers: HashSet<u64>,
    hb_proposals: Vec<Proposal>,
    hb_reconfig: Option<ReconfigurationProposal>,
    max_inflight: usize,
    stop_ask: Option<Ask<(), ()>>,
}

impl<S> ComponentLifecycle for RaftReplica<S>
where
    S: RaftStorage + Send + Clone + 'static,
{
    fn on_start(&mut self) -> Handled {
        let bc = BufferConfig::default();
        self.ctx.borrow().init_buffers(Some(bc), None);
        self.start_timers();
        Handled::Ok
    }

    fn on_kill(&mut self) -> Handled {
        // info!(self.ctx.log(), "Got kill! RaftLog commited: {}", self.raw_raft.raft.raft_log.committed);
        self.stop_timers();
        self.raw_raft
            .mut_store()
            .clear()
            .expect("Failed to clear storage!");
        Handled::Ok
    }
}

impl<S> Actor for RaftReplica<S>
where
    S: RaftStorage + Send + Clone + 'static,
{
    type Message = RaftReplicaMsg;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        match msg {
            RaftReplicaMsg::Propose(p) => {
                self.propose(p);
            }
            RaftReplicaMsg::ProposeReconfiguration(pr) => {
                self.propose_reconfiguration(pr);
            }
            RaftReplicaMsg::Stop(ask) => {
                self.communication_port
                    .trigger(CommunicatorMsg::SendStop(self.raw_raft.raft.id, true));
                self.stop_timers();
                self.stopped = true;
                if self.stopped_peers.len() == self.num_peers {
                    ask.reply(()).expect("Failed to reply Stop ask");
                } else {
                    self.stop_ask = Some(ask);
                }
            }
            #[cfg(test)]
            RaftReplicaMsg::SequenceReq(sr) => {
                let raft_entries: Vec<Entry> = self.raw_raft.raft.raft_log.all_entries();
                let mut sequence: Vec<u64> = Vec::with_capacity(raft_entries.len());
                let mut unique = HashSet::new();
                for entry in raft_entries {
                    if entry.get_entry_type() == EntryType::EntryNormal && !&entry.data.is_empty() {
                        let id = entry.data.as_slice().get_u64();
                        if id != 0 {
                            sequence.push(id);
                        }
                        unique.insert(id);
                    }
                }
                info!(
                    self.ctx.log(),
                    "Got SequenceReq: my seq_len={}. Unique={}",
                    sequence.len(),
                    unique.len()
                );
                sr.reply(sequence)
                    .expect("Failed to respond SequenceReq ask");
            }
        }
        Handled::Ok
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        // ignore
        Handled::Ok
    }
}

impl<S> Require<CommunicationPort> for RaftReplica<S>
where
    S: RaftStorage + Send + Clone + 'static,
{
    fn handle(&mut self, msg: AtomicBroadcastCompMsg) -> Handled {
        match msg {
            AtomicBroadcastCompMsg::RawRaftMsg(rm)
                if !self.stopped && self.reconfig_state != ReconfigurationState::Removed =>
            {
                self.step(rm);
            }
            AtomicBroadcastCompMsg::StopMsg(from_pid) => {
                assert!(
                    self.stopped_peers.insert(from_pid),
                    "Got duplicate stop from {}",
                    from_pid
                );
                // info!(self.ctx.log(), "Got stop from {}. received: {}, num_peers: {}", from_pid, self.stopped_peers.len(), self.num_peers);
                if self.stopped_peers.len() == self.num_peers && self.stopped {
                    self.stop_ask
                        .take()
                        .expect("No stop ask")
                        .reply(())
                        .expect("Failed to reply Stop ask");
                }
            }
            _ => {}
        }
        Handled::Ok
    }
}

impl<S> RaftReplica<S>
where
    S: RaftStorage + Send + Clone + 'static,
{
    pub fn with(
        raw_raft: RawNode<S>,
        replica: ActorRef<RaftCompMsg>,
        num_peers: usize,
        max_inflight: usize,
    ) -> RaftReplica<S> {
        RaftReplica {
            ctx: ComponentContext::uninitialised(),
            supervisor: replica,
            state: State::Election,
            raw_raft,
            communication_port: RequiredPort::uninitialised(),
            timers: None,
            reconfig_state: ReconfigurationState::None,
            current_leader: 0,
            stopped: false,
            stopped_peers: HashSet::new(),
            num_peers,
            hb_proposals: vec![],
            hb_reconfig: None,
            max_inflight,
            stop_ask: None,
        }
    }

    fn start_timers(&mut self) {
        let config = self.ctx.config();
        let outgoing_period = config["experiment"]["outgoing_period"]
            .as_duration()
            .expect("Failed to load outgoing_period");
        let tick_period = config["raft"]["tick_period"]
            .as_i64()
            .expect("Failed to load tick_period") as u64;
        let ready_timer = self.schedule_periodic(DELAY, outgoing_period, move |c, _| c.on_ready());
        let tick_timer =
            self.schedule_periodic(DELAY, Duration::from_millis(tick_period), move |rc, _| {
                rc.tick()
            });
        self.timers = Some((ready_timer, tick_timer));
    }

    fn stop_timers(&mut self) {
        if let Some(timers) = self.timers.take() {
            self.cancel_timer(timers.0);
            self.cancel_timer(timers.1);
        }
    }

    fn try_campaign_leader(&mut self) -> Handled {
        // start campaign to become leader if none has been elected yet
        let leader = self.raw_raft.raft.leader_id;
        if leader == 0 && self.state == State::Election {
            let _ = self.raw_raft.campaign();
        }
        Handled::Ok
    }

    fn tick(&mut self) -> Handled {
        self.raw_raft.tick();
        let leader = self.raw_raft.raft.leader_id;
        if leader != 0 {
            if let Some(rp) = self.hb_reconfig.take() {
                self.propose_reconfiguration(rp);
            }
            if !self.hb_proposals.is_empty() {
                let proposals = std::mem::take(&mut self.hb_proposals);
                for proposal in proposals {
                    self.propose(proposal);
                }
            }
            if leader != self.current_leader {
                // info!(self.ctx.log(), "New leader: {}, old: {}", leader, self.current_leader);
                self.current_leader = leader;
                let notify_client = if self.state == State::Election {
                    self.state = State::Running;
                    true
                } else {
                    false
                };
                self.supervisor
                    .tell(RaftCompMsg::Leader(notify_client, leader));
            }
        }
        Handled::Ok
    }

    fn step(&mut self, msg: TikvRaftMsg) {
        let _ = self.raw_raft.step(msg);
    }

    fn propose(&mut self, proposal: Proposal) {
        if self.raw_raft.raft.leader_id == 0 {
            self.hb_proposals.push(proposal);
        } else {
            let data = proposal.data;
            self.raw_raft.propose(vec![], data).unwrap_or_else(|_| {
                panic!(
                    "Failed to propose. leader: {}, lead_transferee: {:?}",
                    self.raw_raft.raft.leader_id, self.raw_raft.raft.lead_transferee
                )
            });
        }
    }

    fn propose_reconfiguration(&mut self, rp: ReconfigurationProposal) {
        if let ReconfigurationState::None = self.reconfig_state {
            let leader_pid = self.raw_raft.raft.leader_id;
            match leader_pid {
                0 => {
                    self.hb_reconfig = Some(rp);
                }
                my_pid if my_pid == self.raw_raft.raft.id => {
                    let current_config: Vec<u64> = self
                        .raw_raft
                        .raft
                        .prs()
                        .configuration()
                        .voters()
                        .iter()
                        .copied()
                        .collect();
                    let new_voters = rp.get_new_configuration(leader_pid, current_config);
                    let new_config = Configuration::new(new_voters, vec![]);
                    self.raw_raft
                        .raft
                        .propose_membership_change(new_config)
                        .expect("Failed to propose joint consensus reconfiguration");
                    self.reconfig_state = ReconfigurationState::Pending;
                }
                _ => {
                    self.supervisor
                        .tell(RaftCompMsg::ForwardReconfig(leader_pid, rp));
                }
            }
        }
    }

    fn on_ready(&mut self) -> Handled {
        if !self.raw_raft.has_ready() {
            return Handled::Ok;
        }
        let mut store = self.raw_raft.raft.raft_log.store.clone();

        // Get the `Ready` with `RawNode::ready` interface.
        let mut ready = self.raw_raft.ready();

        // Send out the messages come from the node.
        let mut ready_msgs = Vec::with_capacity(self.max_inflight);
        std::mem::swap(&mut ready.messages, &mut ready_msgs);
        for msg in ready_msgs {
            self.communication_port
                .trigger(CommunicatorMsg::RawRaftMsg(msg));
        }

        // Persistent raft logs. It's necessary because in `RawNode::advance` we stabilize
        // raft logs to the latest position.
        if let Err(e) = store.append_log(ready.entries()) {
            error!(
                self.ctx.log(),
                "{}",
                format!("persist raft log fail: {:?}, need to retry or panic", e)
            );
            return Handled::Ok;
        }

        // let mut next_conf_change: Option<ConfChangeType> = None;
        // Apply all committed proposals.
        if let Some(committed_entries) = ready.committed_entries.take() {
            for entry in &committed_entries {
                if entry.data.is_empty() {
                    // From new elected leaders.
                    continue;
                }
                if let EntryType::EntryConfChange = entry.get_entry_type() {
                    if self.reconfig_state == ReconfigurationState::Removed
                        || self.reconfig_state == ReconfigurationState::Finished
                    {
                        continue;
                    }
                    // For conf change messages, make them effective.
                    let mut cc = ConfChange::default();
                    cc.merge_from_bytes(&entry.data).unwrap();
                    let change_type = cc.get_change_type();
                    match &change_type {
                        ConfChangeType::BeginMembershipChange => {
                            let reconfig = cc.get_configuration();
                            let start_index = cc.get_start_index();
                            debug!(
                                self.ctx.log(),
                                "{}",
                                format!(
                                    "Beginning reconfiguration to: {:?}, start_index: {}",
                                    reconfig, start_index
                                )
                            );
                            self.raw_raft
                                .raft
                                .begin_membership_change(&cc)
                                .expect("Failed to begin reconfiguration");
                            assert!(self.raw_raft.raft.is_in_membership_change());
                        }
                        ConfChangeType::FinalizeMembershipChange => {
                            self.raw_raft
                                .raft
                                .finalize_membership_change(&cc)
                                .expect("Failed to finalize reconfiguration");

                            let current_conf = self.raw_raft.raft.prs().configuration().clone();
                            let current_voters = current_conf.voters();
                            if !current_voters.contains(&self.raw_raft.raft.id) {
                                self.reconfig_state = ReconfigurationState::Removed;
                                self.stop_timers();
                                self.supervisor.tell(RaftCompMsg::Removed);
                            } else {
                                self.reconfig_state = ReconfigurationState::Finished;
                            }
                            let leader = self.raw_raft.raft.leader_id;
                            if leader == 0 {
                                // leader was removed
                                self.state = State::Election; // reset leader so it can notify client when new leader emerges
                                if self.reconfig_state != ReconfigurationState::Removed {
                                    // campaign later if we are not removed
                                    let mut rng = rand::thread_rng();
                                    let config = self.ctx.config();
                                    let tick_period = config["raft"]["tick_period"]
                                        .as_i64()
                                        .expect("Failed to load tick_period")
                                        as usize;
                                    let election_timeout = config["experiment"]["election_timeout"]
                                        .as_i64()
                                        .expect("Failed to load election_timeout")
                                        as usize;
                                    let initial_election_factor = config["experiment"]
                                        ["initial_election_factor"]
                                        .as_i64()
                                        .expect("Failed to load initial_election_factor")
                                        as usize;
                                    // randomize with ticks to ensure at least one tick difference in timeout
                                    let intial_timeout_ticks =
                                        (election_timeout / initial_election_factor) / tick_period;
                                    let rnd = rng
                                        .gen_range(intial_timeout_ticks, 2 * intial_timeout_ticks);
                                    let timeout = rnd * tick_period;
                                    self.schedule_once(
                                        Duration::from_millis(timeout as u64),
                                        move |c, _| c.try_campaign_leader(),
                                    );
                                }
                            }
                            let current_configuration =
                                current_voters.iter().copied().collect::<Vec<u64>>();
                            let cs = ConfState::from(current_conf);
                            store.set_conf_state(cs, None);
                            let rr = ReconfigurationResp::with(leader, current_configuration);
                            self.communication_port
                                .trigger(CommunicatorMsg::ReconfigurationResponse(rr));
                        }
                        _ => unimplemented!(),
                    }
                } else {
                    // normal proposals
                    if self.raw_raft.raft.state == StateRole::Leader {
                        let pr =
                            ProposalResp::with(entry.get_data().to_vec(), self.raw_raft.raft.id);
                        self.communication_port
                            .trigger(CommunicatorMsg::ProposalResponse(pr));
                    }
                }
            }
            if let Some(last_committed) = committed_entries.last() {
                store
                    .set_hard_state(last_committed.index, last_committed.term)
                    .expect("Failed to set hardstate");
            }
        }
        // Call `RawNode::advance` interface to update position flags in the raft.
        self.raw_raft.advance(ready);
        Handled::Ok
    }
}
