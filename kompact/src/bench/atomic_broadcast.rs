extern crate raft as tikv_raft;

use super::*;

use benchmark_suite_shared::kompics_benchmarks::benchmarks::{AtomicBroadcastRequest};
use kompact::prelude::*;
use protobuf::Message;
use synchronoise::CountdownEvent;
use std::sync::Arc;
use std::str::FromStr;
use self::raft::{RaftComp, Communicator, MessagingPort, storage::*};
use tikv_raft::{Config, storage::MemStorage};
use partitioning_actor::{Init, InitAck, PartitioningActor, PartitioningActorSer};
use self::atomic_bcast_client::AtomicBcastClient as Client;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub struct ClientParams {
    algorithm: Algorithm,
    last_node_id: u32,
}

#[derive(Default)]
pub struct AtomicBroadcast;

impl DistributedBenchmark for AtomicBroadcast {
    type MasterConf = AtomicBroadcastRequest;
    type ClientConf = ClientParams;
    type ClientData = ActorPath;
    type Master = AtomicBroadcastMaster;
    type Client = AtomicBroadcastClient;
    const LABEL: &'static str = "AtomicBroadcast";

    fn new_master() -> Self::Master { AtomicBroadcastMaster::new() }

    fn msg_to_master_conf(msg: Box<dyn (::protobuf::Message)>) -> Result<Self::MasterConf, BenchmarkError> {
        downcast_msg!(msg; AtomicBroadcastRequest)
    }

    fn new_client() -> Self::Client {
        AtomicBroadcastClient::new()
    }

    fn str_to_client_conf(s: String) -> Result<Self::ClientConf, BenchmarkError> {
        let split: Vec<_> = s.split(",").collect();
        if split.len() != 2 {
            Err(BenchmarkError::InvalidMessage(format!(
                "String '{}' does not represent a client conf! Split length should be 2",
                s
            )))
        } else {
            let algorithm = match split[0].to_lowercase().as_ref() {
                "paxos" => Algorithm::Paxos,
                "raft" => Algorithm::Raft,
                _ => {
                    return Err(BenchmarkError::InvalidMessage(format!(
                        "String to ClientConf error: '{}' does not represent an algorithm",
                        s
                    )));
                }
            };
            let last_node_id = split[1].parse::<u32>().map_err(|e| {
                BenchmarkError::InvalidMessage(format!(
                    "String to ClientConf error: '{}' does not represent a node id: {:?}",
                    split[1], e
                ))
            })?;
            Ok(ClientParams{algorithm, last_node_id})
        }
    }

    fn str_to_client_data(str: String) -> Result<Self::ClientData, BenchmarkError> {
        let res = ActorPath::from_str(&str);
        res.map_err(|e| {
            BenchmarkError::InvalidMessage(format!("Could not read client data: {}", e))
        })
    }

    fn client_conf_to_str(c: Self::ClientConf) -> String {
        let a = match c.algorithm {
            Algorithm::Paxos => "paxos",
            Algorithm::Raft => "raft",
            _ => ""
        };
        let s = format!("{},{}", a, c.last_node_id);
//        println!("ClientConf string: {}", &s);
        s
    }

    fn client_data_to_str(d: Self::ClientData) -> String { d.to_string() }
}

fn get_initial_conf(last_node_id: u64) -> (Vec<u64>, Vec<u64>) {
    let mut conf: Vec<u64> = vec![];
    for id in 1..=last_node_id {
        conf.push(id);
    }
    let initial_conf: (Vec<u64>, Vec<u64>) = (conf, vec![]);
    initial_conf
}

#[derive(Clone, Debug, PartialEq)]
enum Algorithm {
    Paxos,
    Raft,
}

type Storage = DiskStorage;

const OFF: u32 = 0;
const ONE: u32 = 1;
const MAJORITY: u32 = 2;

type Storage = DiskStorage;

pub struct AtomicBroadcastMaster {
    algorithm: Option<Algorithm>,
    num_nodes: Option<u32>,
    num_proposals: Option<u32>,
    num_parallel_proposals: Option<u32>,
    reconfiguration: Option<u32>,
    system: Option<KompactSystem>,
    finished_latch: Option<Arc<CountdownEvent>>,
    iteration_id: u32,
//    paxos_comp: Option<Arc<Component<LeaderPaxosComp>>>,
    raft_components: Option<(Arc<Component<RaftComp<Storage>>>, Arc<Component<Communicator>>)>,
    client_comp: Option<Arc<Component<Client>>>
}

impl AtomicBroadcastMaster {
    fn new() -> AtomicBroadcastMaster {
        AtomicBroadcastMaster {
            algorithm: None,
            num_nodes: None,
            num_proposals: None,
            num_parallel_proposals: None,
            reconfiguration: None,
            system: None,
            finished_latch: None,
            iteration_id: 0,
//            paxos_comp: None,
            raft_components: None,
            client_comp: None
        }
    }
}

impl DistributedBenchmarkMaster for AtomicBroadcastMaster {
    type MasterConf = AtomicBroadcastRequest;
    type ClientConf = ClientParams;
    type ClientData = ActorPath;

    fn setup(&mut self, c: Self::MasterConf, m: &DeploymentMetaData) -> Result<Self::ClientConf, BenchmarkError> {
        println!("Setting up Atomic Broadcast (Master)");
        let additional_nodes: u32 = match c.reconfiguration.to_lowercase().as_ref() {
            "off" => {
                self.reconfiguration = None;
                0
            }
            "one" => {
                self.reconfiguration = Some(ONE);
                1
            }
            "majority" => {
                self.reconfiguration = Some(MAJORITY);
                c.number_of_nodes/2 + 1
            }
            _ => return Err(BenchmarkError::InvalidMessage(String::from("Got unknown reconfiguration parameter")))
        };
        let n = c.number_of_nodes + additional_nodes;
        if m.number_of_clients() + 1 < n {
            return Err(BenchmarkError::InvalidTest(format!(
                "Not enough clients: {}, Required: {}",
                &m.number_of_clients(),
               &n
            )));
        }
        self.num_nodes = Some(c.number_of_nodes);
        self.algorithm = match c.algorithm.to_lowercase().as_ref() {
            "paxos" => Some(Algorithm::Paxos),
            "raft" => Some(Algorithm::Raft),
            _ => {
                return Err(BenchmarkError::InvalidTest(format!(
                    "Unimplemented algoritm: {}",
                    &c.algorithm
                )));
            }
        };
        self.num_proposals = Some(c.number_of_proposals);
        self.num_parallel_proposals = Some(c.proposals_in_parallel);
        let system =
            crate::kompact_system_provider::global().new_remote_system_with_threads("atomicbroadcast", 4);
        self.system = Some(system);
        let params = ClientParams { algorithm: self.algorithm.as_ref().unwrap().clone(), last_node_id: c.number_of_nodes };
        Ok(params)
    }

    fn prepare_iteration(&mut self, d: Vec<Self::ClientData>) -> () {
        println!("Preparing iteration");
        match self.system {
            Some(ref system) => {
                let prepare_latch = Arc::new(CountdownEvent::new(1));
                let finished_latch = Arc::new(CountdownEvent::new(1));
                match self.algorithm {
                    Some(Algorithm::Paxos) => {
                        unimplemented!()
                    }
                    Some(Algorithm::Raft) => {
                        let conf_state = get_initial_conf(self.num_nodes.unwrap() as u64);
//                        let storage = MemStorage::new_with_conf_state(conf_state);
//                        let dir = "./diskstorage";
//                        let storage = DiskStorage::new_with_conf_state(dir, conf_state);
                        let config = Config {
                            election_tick: 10,
                            heartbeat_tick: 3,
                            ..Default::default()
                        };
                        /*** Setup RaftComp ***/
                        let (raft_comp, unique_reg_f) = system.create_and_register(|| {
                            RaftComp::with(config, conf_state)
                        });
                        let raft_comp_f = system.start_notify(&raft_comp);
                        raft_comp_f
                            .wait_timeout(Duration::from_millis(1000))
                            .expect("RaftComp never started!");
                        unique_reg_f.wait_expect(
                            Duration::from_millis(1000),
                            "RaftComp failed to register!",
                        );
                        /*** Setup communicator ***/
                        let (communicator, unique_reg_f) =
                            system.create_and_register(|| { Communicator::new() });
                        let named_reg_f = system.register_by_alias(
                            &communicator,
                            format!("communicator{}", &self.iteration_id),
                        );
                        unique_reg_f
                            .wait_timeout(Duration::from_millis(1000))
                            .expect("Communicator never registered!")
                            .expect("Communicator to register!");
                        named_reg_f.wait_expect(
                            Duration::from_millis(1000),
                            "Communicator failed to register!",
                        );
                        let communicator_f = system.start_notify(&communicator);
                        communicator_f
                            .wait_timeout(Duration::from_millis(1000))
                            .expect("Communicator never started!");

                        biconnect_components::<MessagingPort, _, _>(&communicator, &raft_comp)
                            .expect("Could not connect components!");

                        let self_path = ActorPath::Named(NamedPath::with_system(
                            system.system_path(),
                            vec![format!("communicator{}", &self.iteration_id).into()],
                        ));

                        let mut nodes = d;
                        nodes.insert(0, self_path);
                        let mut nodes_id: HashMap<u64, ActorPath> = HashMap::new();
                        for (id, actorpath) in nodes.iter().enumerate() {
                            nodes_id.insert(id as u64 + 1, actorpath.clone());
                        }
                        /*** Setup partitioning actor ***/
                        let (partitioning_actor, unique_reg_f) = system.create_and_register(|| {
                            PartitioningActor::with(
                                prepare_latch.clone(),
                                None,
                                self.iteration_id,
                                nodes,
                                0,
                                None,
                            )
                        });
                        unique_reg_f.wait_expect(
                            Duration::from_millis(1000),
                            "PartitioningComp failed to register!",
                        );

                        let partitioning_actor_f = system.start_notify(&partitioning_actor);
                        partitioning_actor_f
                            .wait_timeout(Duration::from_millis(1000))
                            .expect("PartitioningComp never started!");
                        /*** Setup client ***/
                        let (client_comp, unique_reg_f) = system.create_and_register( || {
                            Client::with(
                                self.num_proposals.unwrap(),
                                nodes_id,
                                finished_latch.clone(),
                            )
                        });
                        unique_reg_f.wait_expect(
                            Duration::from_millis(1000),
                            "Client failed to register!",
                        );

                        self.iteration_id += 1;
                        self.raft_components = Some((raft_comp, communicator));
                        self.client_comp = Some(client_comp);
                        self.finished_latch = Some(finished_latch);
                        prepare_latch.wait();
                        /*** Partitioning actor not needed anymore, kill it ***/
                        let kill_pactor_f = system.kill_notify(partitioning_actor);
                        kill_pactor_f
                            .wait_timeout(Duration::from_millis(1000))
                            .expect("Partitioning Actor never died!");
                    }
                    _ => unimplemented!(),
                }
            }
            None => unimplemented!()
        }
    }

    fn run_iteration(&mut self) -> () {
        println!("Running Atomic Broadcast experiment!");
        match self.system {
            Some(ref system) => {
                let client_comp_f = system.start_notify(self.client_comp.as_ref().expect("No client component initialised"));
                client_comp_f
                    .wait_timeout(Duration::from_millis(1000))
                    .expect("ClientComp never started!");
                let finished_latch = self.finished_latch.take().unwrap();
                finished_latch.wait();
            }
            _ => unimplemented!()
        }
    }

    fn cleanup_iteration(&mut self, last_iteration: bool, _exec_time_millis: f64) -> () {
        println!("Cleaning up Atomic Broadcast (master) side");
        let system = self.system.take().unwrap();

        if self.raft_components.is_some() {
            let (raft_comp, communicator) = self.raft_components.take().unwrap();
            let kill_raft_f = system.kill_notify(raft_comp);
            kill_raft_f
                .wait_timeout(Duration::from_millis(1000))
                .expect("RaftComp never died!");

            let kill_communicator_f = system.kill_notify(communicator);
            kill_communicator_f
                .wait_timeout(Duration::from_millis(1000))
                .expect("Communicator never died");
        }

        if last_iteration {
            println!("Cleaning up last iteration");
            system
                .shutdown()
                .expect("Kompact didn't shut down properly");
            self.num_proposals = None;
            self.algorithm = None;
            self.num_parallel_proposals = None;
            self.num_nodes = None;
        } else {
            self.system = Some(system);
        }
    }
}

pub struct AtomicBroadcastClient {
    system: Option<KompactSystem>,
    //    paxos_comp: Option<Arc<Component<LeaderPaxosComp>>>,
    raft_components: Option<(Arc<Component<RaftComp<Storage>>>, Arc<Component<Communicator>>)>,
}

impl AtomicBroadcastClient {
    fn new() -> AtomicBroadcastClient {
        AtomicBroadcastClient {
            system: None,
            raft_comp: None,
            raft_communicator: None,
            conf_state: None,
        }
    }
}

impl DistributedBenchmarkClient for AtomicBroadcastClient {
    type ClientConf = ClientParams;
    type ClientData = ActorPath;

    fn setup(&mut self, c: Self::ClientConf) -> Self::ClientData {
        println!("Setting up Atomic Broadcast (client)");
        let system =
            crate::kompact_system_provider::global().new_remote_system_with_threads("atomicbroadcast", 4);
        let named_path = match c.algorithm {
            Algorithm::Paxos => {
                unimplemented!()
            }
            Algorithm::Raft => {
                let conf_state = get_initial_conf(c.last_node_id as u64);
//                let storage = MemStorage::new_with_conf_state(conf_state);
//                let dir = "./diskstorage";
//                let storage = DiskStorage::new_with_conf_state(dir, conf_state);
                let config = Config {
                    election_tick: 10,
                    heartbeat_tick: 3,
                    ..Default::default()
                };
                /*** Setup RaftComp ***/
                let (raft_comp, unique_reg_f) = system.create_and_register(|| {
                    RaftComp::with(config, conf_state)
                });
                let raft_comp_f = system.start_notify(&raft_comp);
                raft_comp_f
                    .wait_timeout(Duration::from_millis(1000))
                    .expect("RaftComp never started!");
                unique_reg_f.wait_expect(
                    Duration::from_millis(1000),
                    "RaftComp failed to register!",
                );
                /*** Setup communicator ***/
                let (communicator, unique_reg_f) =
                    system.create_and_register(|| { Communicator::new() });
                let named_reg_f = system.register_by_alias(
                    &communicator,
                    "communicator",
                );
                unique_reg_f
                    .wait_timeout(Duration::from_millis(1000))
                    .expect("Communicator never registered!")
                    .expect("Communicator to register!");
                named_reg_f.wait_expect(
                    Duration::from_millis(1000),
                    "Communicator failed to register!",
                );
                let communicator_f = system.start_notify(&communicator);
                communicator_f
                    .wait_timeout(Duration::from_millis(1000))
                    .expect("Communicator never started!");

                let self_path = ActorPath::Named(NamedPath::with_system(
                    system.system_path(),
                    vec!["communicator".into()],
                ));

                self.raft_communicator = Some(communicator);
                self_path
            }
            _ => unimplemented!()
        };
        self.system = Some(system);
        println!("Got path for Atomic Broadcast actor: {}", named_path);
        named_path
    }

    fn prepare_iteration(&mut self) -> () {
        println!("Preparing Atomic Broadcast (client)");
        if self.raft_communicator.is_some() {
            let dir = "./diskstorage";
            let conf_state = self.conf_state.as_ref().unwrap().clone();
            let storage = DiskStorage::new_with_conf_state(dir, conf_state);
            let config = Config {
                election_tick: 10,
                heartbeat_tick: 3,
                ..Default::default()
            };
            let system = self.system.as_mut().expect("Kompact System not found");
            /*** Setup RaftComp ***/
            let (raft_comp, unique_reg_f) = system.create_and_register(|| {
                RaftComp::with(config, storage)
            });
            let raft_comp_f = system.start_notify(&raft_comp);
            raft_comp_f
                .wait_timeout(Duration::from_millis(1000))
                .expect("RaftComp never started!");
            unique_reg_f.wait_expect(
                Duration::from_millis(1000),
                "RaftComp failed to register!",
            );

            biconnect_components::<MessagingPort, _, _>(self.raft_communicator.as_ref().unwrap(), &raft_comp)
                .expect("Could not connect components!");

            self.raft_comp = Some(raft_comp);
        }
    }

    fn cleanup_iteration(&mut self, last_iteration: bool) -> () {
        println!("Cleaning up Atomic Broadcast (client)");
        if self.raft_comp.is_some() {
            let system = self.system.as_mut().unwrap();
            let kill_raft_f = system.kill_notify(self.raft_comp.take().unwrap());
            kill_raft_f
                .wait_timeout(Duration::from_millis(1000))
                .expect("Atomic Broadcast never died!");

            if last_iteration {
                let system = self.system.take().unwrap();
                let kill_communicator_f = system.kill_notify(self.raft_communicator.take().unwrap());
                kill_communicator_f
                    .wait_timeout(Duration::from_millis(1000))
                    .expect("Communicator never died!");

                system
                    .shutdown()
                    .expect("Kompact didn't shut down properly");
            }
        }
    }
}

pub mod raft {
    extern crate raft as tikv_raft;

    use kompact::prelude::*;
    use tikv_raft::{prelude::*, StateRole, prelude::Message as TikvRaftMsg, storage::MemStorage};
    use protobuf::{Message as PbMessage, parse_from_bytes};
    use super::super::serialiser_ids as serialiser_ids;
    use std::{time::Duration, path::PathBuf, fs::OpenOptions, io::Write, ops::Range, convert::TryInto, fs::File, mem::size_of, sync::Arc, collections::HashMap};
    use uuid::Uuid;
    use self::tikv_raft::Error;
    use storage::RaftStorage;
    use super::partitioning_actor::{Init, InitAck, PartitioningActorSer};
    use crate::bench::atomic_broadcast::raft::storage::DiskStorage;

    const CONFIGCHANGE_ID: u64 = 0;

    pub struct MessagingPort;

    impl Port for MessagingPort {
        type Indication = RaftCompMsg;
        type Request = CommunicatorMsg;
    }

    #[derive(ComponentDefinition)]
    pub struct Communicator {
        ctx: ComponentContext<Communicator>,
        raft_port: ProvidedPort<MessagingPort, Communicator>,
        peers: HashMap<u64, ActorPath>, // tikv raft node id -> actorpath
        cached_next_iter_metadata: Option<(ActorPath, HashMap<u64, ActorPath>)>,   // (partitioning_actor, peers) for next init_ack
        cached_client: Option<ActorPath>    // cached client to send SequenceResp to
    }

    impl Communicator {
        pub fn new() -> Communicator {
            Communicator {
                ctx: ComponentContext::new(),
                raft_port: ProvidedPort::new(),
                peers: HashMap::new(),
                cached_next_iter_metadata: None,
                cached_client: None
            }
        }
    }

    impl Provide<ControlPort> for Communicator {
        fn handle(&mut self, _event: <ControlPort as Port>::Request) -> () {
            // ignore
        }
    }

    impl Provide<MessagingPort> for Communicator {
        fn handle(&mut self, msg: CommunicatorMsg) {
            match &msg {
                CommunicatorMsg::TikvRaftMsg(rm) => {
                    let receiver = self.peers.get(&rm.payload.get_to()).expect(&format!("Could not find actorpath for id={}", &rm.payload.get_to()));
                    receiver.tell((msg.to_owned(), RaftSer), self);
                },
                CommunicatorMsg::ProposalResp(pr) => {
                    let receiver = pr.client.as_ref().expect("No client actorpath provided in ProposalResp");
                    receiver.tell((msg.to_owned(), RaftSer), self);
                },
                CommunicatorMsg::ProposalForward(pf) => {
                    let receiver = self.peers.get(&pf.leader_id).expect("Could not find actorpath to leader in ProposalForward");
                    receiver.tell((CommunicatorMsg::Proposal(pf.proposal.to_owned()), RaftSer), self);
                },
                CommunicatorMsg::SequenceResp(_) => {
                    let receiver = self.cached_client.as_ref().expect("No cached client found for SequenceResp");
                    receiver.tell((msg.to_owned(), RaftSer), self);
                },
                CommunicatorMsg::InitAck(init_ack) => {
                    let meta =  self.cached_next_iter_metadata.take().expect("No next iteration metadata cached");
                    let receiver = meta.0;
                    let peers = meta.1;
                    self.peers = peers;
                    receiver.tell((init_ack.to_owned(), PartitioningActorSer), self);
                }
                _ => debug!(self.ctx.log(), "{}", format!("Communicator got unexpected msg: {:?}", msg))
            }
        }
    }

    impl Actor for Communicator {
        type Message = ();

        fn receive_local(&mut self, _msg: Self::Message) -> () {
            unimplemented!();
        }

        fn receive_network(&mut self, m: NetMessage) -> () {
            let sender = m.sender().clone();
            match_deser! {m; {
                init: Init [PartitioningActorSer] => {
                    let iteration_id = init.init_id;
                    let node_id = init.rank as u64;
                    let mut peers = HashMap::new();
                    for (id, actorpath) in init.nodes.into_iter().enumerate() {
                        peers.insert(id as u64 + 1, actorpath);
                    }
                    self.cached_next_iter_metadata = Some((sender, peers));
                    let ctr = CreateTikvRaft{ node_id, iteration_id };
                    self.raft_port.trigger(RaftCompMsg::CreateTikvRaft(ctr));
                },

                comm_msg: CommunicatorMsg [RaftSer] => {
                    match comm_msg {
                        CommunicatorMsg::TikvRaftMsg(rm) => self.raft_port.trigger(RaftCompMsg::TikvRaftMsg(rm)),
                        CommunicatorMsg::Proposal(p) => self.raft_port.trigger(RaftCompMsg::Proposal(p)),
                        CommunicatorMsg::SequenceReq(sr) => {
                            self.cached_client = Some(sender);
                            self.raft_port.trigger(RaftCompMsg::SequenceReq(sr));
                        }
                        _ => error!(self.ctx.log(), "Got unexpected msg: {:?}", comm_msg),
                    }
                },
                !Err(e) => error!(self.ctx.log(), "Error deserialising msg: {:?}", e),
            }
            }
        }
    }

    #[derive(ComponentDefinition)]
    pub struct RaftComp<S: RaftStorage + std::marker::Send + std::clone::Clone + 'static> where S: std::marker::Send{
        ctx: ComponentContext<Self>,
        raft_node: Option<RawNode<S>>,
        communication_port: RequiredPort<MessagingPort, Self>,
        reconfig_client: Option<ActorPath>,
        config: Config,
        conf_state: (Vec<u64>, Vec<u64>),
        iteration_id: u32,
        timers: Option<(ScheduledTimer, ScheduledTimer)>
    }

    impl<S: RaftStorage + std::marker::Send + std::clone::Clone + 'static> Provide<ControlPort> for RaftComp<S>{
        fn handle(&mut self, event: ControlEvent) -> () {
            match event {
                ControlEvent::Start => {},
                _ => self.stop_timers()
            }
        }
    }

    impl<S: RaftStorage + std::marker::Send + std::clone::Clone + 'static> Require<MessagingPort> for RaftComp<S> {
        fn handle(&mut self, msg: RaftCompMsg) -> () {
            match msg {
                RaftCompMsg::CreateTikvRaft(ctr) => {
                    info!(self.ctx.log(), "{}", format!("Creating raft node, iteration: {}, id: {}", ctr.iteration_id, ctr.node_id));
                    self.iteration_id = ctr.iteration_id;
                    self.config.id = ctr.node_id;
                    let store = S::new_with_conf_state(self.conf_state.clone());
                    self.raft_node = Some(RawNode::new(&self.config, store).expect("Failed to create TikvRaftNode"));
                    if self.config.id == 1 {    // leader
                        let raft_node = self.raft_node.as_mut().unwrap();
                        raft_node.raft.become_candidate();
                        raft_node.raft.become_leader();
                    }
                    if self.timers.is_none() {
                        self.start_timers();
                    }
                    self.communication_port.trigger(CommunicatorMsg::InitAck(InitAck(self.iteration_id)));
                }
                RaftCompMsg::TikvRaftMsg(rm) => {
                    if rm.iteration_id == self.iteration_id {
                        self.step(rm.payload)
                    }
                }
                RaftCompMsg::Proposal(p) => {
                    let raft_node = self.raft_node.as_mut().expect("TikvRaftNode not initialized in RaftComp");
                    if raft_node.raft.state == StateRole::Leader{
                        self.propose(p);
                    }
                    else {
                        let leader_id = raft_node.raft.leader_id;
                        if leader_id > 0 {
                            let pf = ProposalForward::with(leader_id, p);
                            self.communication_port.trigger(CommunicatorMsg::ProposalForward(pf));
                        } else {
                            // no leader... let client node proposal failed
                            let pr = ProposalResp::failed(p);
                            self.communication_port.trigger(CommunicatorMsg::ProposalResp(pr));
                        }
                    }
                }
                RaftCompMsg::SequenceReq(_) => {
                    let raft_node = self.raft_node.as_mut().expect("TikvRaftNode not initialized in RaftComp");
                    let raft_entries: Vec<Entry> = raft_node.raft.raft_log.all_entries();
                    let mut sequence: Vec<u64> = Vec::new();
                    for entry in raft_entries {
                        if entry.get_entry_type() == EntryType::EntryNormal && !&entry.data.is_empty() {
                            let value = Proposal::deserialize_normal(&entry.data);
                            sequence.push(value.id)
                        }
                    }
                    let sr = SequenceResp{ node_id: self.config.id, sequence };
                    self.communication_port.trigger(CommunicatorMsg::SequenceResp(sr));
                }
            }
        }
    }

    impl<S: RaftStorage + std::marker::Send + std::clone::Clone + 'static> RaftComp<S> {
        pub fn with(config: Config, conf_state: (Vec<u64>, Vec<u64>)) -> RaftComp<S> {
            RaftComp {
                ctx: ComponentContext::new(),
                raft_node: None,
                communication_port: RequiredPort::new(),
                reconfig_client: None,
                config,
                conf_state,
                iteration_id: 0,
                timers: None,
            }
        }

        fn start_timers(&mut self){
            let delay = Duration::from_millis(0);
            let on_ready_uuid = uuid::Uuid::new_v4();
            let tick_uuid = uuid::Uuid::new_v4();
            // give new reference to self to make compiler happy
            let ready_timer = self.schedule_periodic(delay, Duration::from_millis(1), move |c, on_ready_uuid| c.on_ready());
            let tick_timer = self.schedule_periodic(delay, Duration::from_millis(100), move |rc, tick_uuid| rc.tick());
            self.timers = Some((ready_timer, tick_timer));
        }

        fn stop_timers(&mut self) {
            match self.timers.take() {
                Some((ready_timer, tick_timer)) => {
                    self.cancel_timer(ready_timer);
                    self.cancel_timer(tick_timer);
                }
                _ => {}
            }
        }

        fn tick(&mut self) {
            self.raft_node.as_mut()
                .expect("TikvRaftNode not initialized in RaftComp")
                .tick();
        }

        fn step(&mut self, msg: TikvRaftMsg) {
            let _ = self.raft_node.as_mut().expect("TikvRaftNode not initialized in RaftComp").step(msg);
        }

        fn propose(&mut self, proposal: Proposal) {
            let raft_node = self.raft_node.as_mut().expect("TikvRaftNode not initialized in RaftComp");
            let last_index1 = raft_node.raft.raft_log.last_index() + 1;

            match &proposal.conf_change {
                Some(cc) => {
                    self.reconfig_client = Some(proposal.client.clone());
                    let _ = raft_node.propose_conf_change(vec![], cc.clone());
                }
                None => {   // i.e normal operation
                    let ser_data = proposal.serialize_normal();
                    match ser_data {
                        Ok(data) => {
                            let _ = raft_node.propose(vec![], data);
                        },
                        _ => {
                            error!(self.ctx.log(), "Cannot propose due to tikv serialization failure...");
                            // Propose failed, don't forget to respond to the client.
                            let pr = ProposalResp::failed(proposal.clone());
                            self.communication_port.trigger(CommunicatorMsg::ProposalResp(pr));
                        }
                    }

                }
            }
            let last_index2 = raft_node.raft.raft_log.last_index() + 1;
            if last_index2 == last_index1 {
                // Propose failed, don't forget to respond to the client.
                let pr = ProposalResp::failed(proposal);
                self.communication_port.trigger(CommunicatorMsg::ProposalResp(pr));
            }
        }

        fn on_ready(&mut self) {
            let raft_node = self.raft_node.as_mut().expect("TikvRaftNode not initialized in RaftComp");
            if !raft_node.has_ready() {
                return;
            }
            let mut store = raft_node.raft.raft_log.store.clone();

            // Get the `Ready` with `RawNode::ready` interface.
            let mut ready = raft_node.ready();

            // Persistent raft logs. It's necessary because in `RawNode::advance` we stabilize
            // raft logs to the latest position.
            if let Err(e) = store.append_log(ready.entries()) {
                eprintln!("persist raft log fail: {:?}, need to retry or panic", e);
                return;
            }

            // TODO Apply the snapshot. It's necessary because in `RawNode::advance` we stabilize the snapshot.
            /* if *ready.snapshot() != Snapshot::default() {
                let s = ready.snapshot().clone();
                /*if let Err(e) = store.wl().apply_snapshot(s) {
                    eprintln!("apply snapshot fail: {:?}, need to retry or panic", e);
                    return;
                }*/
            }*/

            // Send out the messages come from the node.
            for msg in ready.messages.drain(..) {
                let rm = RaftMsg { iteration_id: self.iteration_id, payload: msg};
                self.communication_port.trigger(CommunicatorMsg::TikvRaftMsg(rm));
            }

            // Apply all committed proposals.
            if let Some(committed_entries) = ready.committed_entries.take() {
                for entry in &committed_entries {
                    if entry.data.is_empty() {
                        // From new elected leaders.
                        continue;
                    }
                    if let EntryType::EntryConfChange = entry.get_entry_type() {
                        // For conf change messages, make them effective.
                        let mut cc = ConfChange::default();
                        cc.merge_from_bytes(&entry.data).unwrap();
                        let node_id = cc.node_id;
                        let change_type = cc.get_change_type();
                        match &change_type {
                            ConfChangeType::AddNode => raft_node.raft.add_node(node_id.clone()).unwrap(),
                            ConfChangeType::RemoveNode => raft_node.raft.remove_node(node_id.clone()).unwrap(),
                            ConfChangeType::AddLearnerNode => raft_node.raft.add_learner(node_id.clone()).unwrap(),
                            ConfChangeType::BeginMembershipChange
                            | ConfChangeType::FinalizeMembershipChange => unimplemented!(),
                        }
                        // TODO
                        /*let cs = ConfState::from(raft_node.raft.prs().configuration().clone());
                        store.wl().set_conf_state(cs, None);*/
                        if raft_node.raft.state == StateRole::Leader && self.reconfig_client.is_some() {
                            let client = self.reconfig_client.clone().unwrap();
                            let pr = ProposalResp::succeeded_configchange(client, node_id, change_type);
                            self.communication_port.trigger(CommunicatorMsg::ProposalResp(pr));
                        }
                    } else {
                        // For normal proposals, reply to client
                        let des_proposal = Proposal::deserialize_normal(&entry.data);
                        if raft_node.raft.state == StateRole::Leader {
                            let pr = ProposalResp::succeeded_normal(des_proposal);
                            self.communication_port.trigger(CommunicatorMsg::ProposalResp(pr));
                        }
                    }

                }
                if let Some(last_committed) = committed_entries.last() {
                    store.set_hard_state(last_committed.index, last_committed.term).expect("Failed to set hardstate");
                }
            }
            // Call `RawNode::advance` interface to update position flags in the raft.
            raft_node.advance(ready);
        }

    }


    impl<S: RaftStorage + std::marker::Send + std::clone::Clone + 'static> Actor for RaftComp<S> {
        type Message = ();

        fn receive_local(&mut self, _msg: Self::Message) -> () {
            unimplemented!()
        }

        fn receive_network(&mut self, _msg: NetMessage) -> () {
            unimplemented!()
        }
    }

    /*** All messages and their serializers ***/
    #[derive(Clone, Debug)]
    pub struct CreateTikvRaft {
        node_id: u64,
        iteration_id: u32
    }

    #[derive(Clone, Debug)]
    pub struct RaftMsg {
        iteration_id: u32,
        payload: TikvRaftMsg
    }

    #[derive(Clone, Debug)]
    pub struct SequenceReq;

    #[derive(Clone, Debug)]
    pub struct SequenceResp {
        node_id: u64,
        sequence: Vec<u64>
    }

    #[derive(Clone, Debug)]
    pub enum RaftCompMsg {
        CreateTikvRaft(CreateTikvRaft),
        TikvRaftMsg(RaftMsg),
        Proposal(Proposal),
        SequenceReq(SequenceReq),
    }

    #[derive(Clone, Debug)]
    pub enum CommunicatorMsg {
        TikvRaftMsg(RaftMsg),
        Proposal(Proposal),
        ProposalResp(ProposalResp),
        ProposalForward(ProposalForward),
        SequenceReq(SequenceReq),
        SequenceResp(SequenceResp),
        InitAck(InitAck)
    }

    #[derive(Clone, Debug)]
    pub struct Proposal {
        id: u64,
        client: ActorPath,
        conf_change: Option<ConfChange>,
    }

    impl Proposal {
        fn conf_change(id: u64, client: ActorPath, cc: &ConfChange) -> Proposal {
            let proposal = Proposal {
                id,
                client,
                conf_change: Some(cc.clone()),
            };
            proposal
        }

        pub(crate) fn normal(id: u64, client: ActorPath) -> Proposal {
            let proposal = Proposal {
                id,
                client,
                conf_change: None,
            };
            proposal
        }

        fn serialize_normal(&self) -> Result<Vec<u8>, SerError> {   // serialize to use with tikv raft
            let mut buf = vec![];
            buf.put_u64_be(self.id);
            self.client.serialise(&mut buf)?;
            Ok(buf.clone())
        }

        fn deserialize_normal(bytes: &Vec<u8>) -> Proposal {  // deserialize from tikv raft
            let mut buf = bytes.into_buf();
            let id = buf.get_u64_be();
            let client = ActorPath::deserialise(&mut buf).expect("No client actorpath in proposal");
            Proposal::normal(id, client)
        }
    }

    #[derive(Clone, Debug)]
    pub struct ProposalResp {
        pub id: u64,
        client: Option<ActorPath>,  // client don't need it when receiving it
        pub succeeded: bool,
        conf_change: Option<(u64, ConfChangeType)>,
    }

    impl ProposalResp {
        fn failed(proposal: Proposal) -> ProposalResp {
            let mut pr = ProposalResp {
                id: proposal.id,
                client: Some(proposal.client),
                succeeded: false,
                conf_change: None
            };
            match proposal.conf_change {
                Some(cf) => {
                    let node_id = cf.get_node_id();
                    let change_type = cf.get_change_type();
                    pr.conf_change = Some((node_id, change_type));
                }
                None => {}
            }
            pr
        }

        fn succeeded_normal(proposal: Proposal) -> ProposalResp {
            ProposalResp {
                id: proposal.id,
                client: Some(proposal.client),
                succeeded: true,
                conf_change: None
            }
        }

        fn succeeded_configchange(client: ActorPath, node_id: u64, change_type: ConfChangeType) -> ProposalResp {
            ProposalResp {
                id: CONFIGCHANGE_ID,
                client: Some(client),
                succeeded: true,
                conf_change: Some((node_id, change_type))
            }
        }
    }

    #[derive(Clone, Debug)]
    pub struct ProposalForward {
        leader_id: u64,
        proposal: Proposal
    }

    impl ProposalForward {
        fn with(leader_id: u64, proposal: Proposal) -> ProposalForward {
            ProposalForward {
                leader_id,
                proposal
            }
        }
    }

    const PROPOSAL_ID: u8 = 0;
    const PROPOSALRESP_ID: u8 = 1;
    const RAFT_MSG_ID: u8 = 2;
    const SEQREQ_ID: u8 = 3;
    const SEQRESP_ID: u8 = 4;

    const PROPOSAL_FAILED: u8 = 0;
    const PROPOSAL_SUCCESS: u8 = 1;

    const CHANGETYPE_ADD_NODE: u8 = 0;
    const CHANGETYPE_REMOVE_NODE: u8 = 1;
    const CHANGETYPE_ADD_LEARNER: u8 = 2;

    pub struct RaftSer;

    impl Serialiser<CommunicatorMsg> for RaftSer {
        fn ser_id(&self) -> SerId {
            serialiser_ids::RAFT_ID
        }

        fn size_hint(&self) -> Option<usize> {
            Some(200) // TODO: Set it dynamically?
        }

        fn serialise(&self, enm: &CommunicatorMsg, buf: &mut dyn BufMut) -> Result<(), SerError> {
            match enm {
                CommunicatorMsg::TikvRaftMsg(rm) => {
                    buf.put_u8(RAFT_MSG_ID);
                    buf.put_u32_be(rm.iteration_id);
                    let bytes: Vec<u8> = rm.payload.write_to_bytes().expect("Protobuf failed to serialise TikvRaftMsg");
                    buf.put_slice(&bytes);
                    Ok(())
                },
                CommunicatorMsg::Proposal(p) => {
                    buf.put_u8(PROPOSAL_ID);
                    buf.put_u64_be(p.id);
                    match &p.conf_change {
                        Some(cf) => {
                            let cf_bytes: Vec<u8> = cf.write_to_bytes().expect("Protobuf failed to serialise ConfigChange");
                            let cf_length = cf_bytes.len() as u64;
                            buf.put_u64_be(cf_length);
                            buf.put_slice(&cf_bytes);
                        },
                        None => {
                            buf.put_u64_be(0);
                        }
                    }
                    p.client.serialise(buf).expect("Failed to serialise actorpath");
                    Ok(())
                },
                CommunicatorMsg::ProposalResp(pr) => {
                    buf.put_u8(PROPOSALRESP_ID);
                    buf.put_u64_be(pr.id);
                    if pr.succeeded {
                        buf.put_u8(PROPOSAL_SUCCESS);
                    } else {
                        buf.put_u8(PROPOSAL_FAILED);
                    }
                    match &pr.conf_change {
                        Some((node_id, change_type)) => {
                            buf.put_u64_be(*node_id);
                            match change_type {
                                ConfChangeType::AddNode => buf.put_u8(CHANGETYPE_ADD_NODE),
                                ConfChangeType::RemoveNode => buf.put_u8(CHANGETYPE_REMOVE_NODE),
                                ConfChangeType::AddLearnerNode => buf.put_u8(CHANGETYPE_ADD_LEARNER),
                                _ => return Err(SerError::InvalidType("Tried to serialise unknown ConfChangeType".into()))
                            }
                        }
                        None => { buf.put_u64_be(0) }
                    }
                    Ok(())
                },
                CommunicatorMsg::SequenceReq(_) => {
                    buf.put_u8(SEQREQ_ID);
                    Ok(())
                },
                CommunicatorMsg::SequenceResp(sr) => {
                    buf.put_u8(SEQRESP_ID);
                    buf.put_u64_be(sr.node_id);
                    let seq_len = sr.sequence.len() as u32;
                    buf.put_u32_be(seq_len);
                    for i in &sr.sequence {
                        buf.put_u64_be(*i);
                    }
                    Ok(())
                },
                _ => {
                    Err(SerError::InvalidType("Tried to serialise unknown type".into()))
                }
            }
        }
    }

    impl Deserialiser<CommunicatorMsg> for RaftSer {
        const SER_ID: u64 = serialiser_ids::RAFT_ID;

        fn deserialise(buf: &mut dyn Buf) -> Result<CommunicatorMsg, SerError> {
            match buf.get_u8(){
                RAFT_MSG_ID => {
                    let iteration_id = buf.get_u32_be();
                    let bytes = buf.bytes();
                    let payload: TikvRaftMsg = parse_from_bytes::<TikvRaftMsg>(bytes).expect("Protobuf failed to deserialise TikvRaftMsg");
                    let rm = RaftMsg { iteration_id, payload};
                    Ok(CommunicatorMsg::TikvRaftMsg(rm))
                },
                PROPOSAL_ID => {
                    let id = buf.get_u64_be();
                    let n = buf.get_u64_be();
                    let conf_change = if n > 0 {
                        let mut cf_bytes: Vec<u8> = vec![0; n as usize];
                        buf.copy_to_slice(&mut cf_bytes);
                        let cc = parse_from_bytes::<ConfChange>(&cf_bytes).expect("Protobuf failed to serialise ConfigChange");
                        Some(cc)
                    } else {
                        None
                    };
                    let client = ActorPath::deserialise(buf).expect("Failed to deserialise actorpath");
                    let proposal = Proposal {
                        id,
                        client,
                        conf_change
                    };
                    Ok(CommunicatorMsg::Proposal(proposal))
                },
                PROPOSALRESP_ID => {
                    let id = buf.get_u64_be();
                    let succeeded: bool = match buf.get_u8() {
                        0 => false,
                        _ => true,
                    };
                    let mut pr = ProposalResp {
                        id,
                        succeeded,
                        client: None,
                        conf_change: None,
                    };
                    let node_id = buf.get_u64_be();
                    match &node_id {
                        0 => {},
                        _ => {
                            match buf.get_u8() {
                                CHANGETYPE_ADD_NODE => {
                                    pr.conf_change = Some((node_id, ConfChangeType::AddNode));
                                },
                                CHANGETYPE_REMOVE_NODE => {
                                    pr.conf_change = Some((node_id, ConfChangeType::RemoveNode));
                                },
                                CHANGETYPE_ADD_LEARNER => {
                                    pr.conf_change = Some((node_id, ConfChangeType::AddLearnerNode));
                                },
                                _ => {},
                            };
                        }
                    }
                    Ok(CommunicatorMsg::ProposalResp(pr))
                },
                SEQREQ_ID => Ok(CommunicatorMsg::SequenceReq(SequenceReq)),
                SEQRESP_ID => {
                    let node_id = buf.get_u64_be();
                    let sequence_len = buf.get_u32_be();
                    let mut sequence: Vec<u64> = Vec::new();
                    for _ in 0..sequence_len {
                        sequence.push(buf.get_u64_be());
                    }
                    let sr = SequenceResp{ node_id, sequence};
                    Ok(CommunicatorMsg::SequenceResp(sr))
                }
                _ => {
                    Err(SerError::InvalidType(
                        "Found unkown id but expected RaftMsg, Proposal or ProposalResp".into(),
                    ))
                }
            }
        }
    }

    pub(super) mod storage {
        use super::*;
        use tikv_raft::{storage::MemStorage, Error, StorageError, util::limit_size};
        use memmap::MmapMut;
        use std::{path::PathBuf, fs::OpenOptions, io::Write, ops::Range, convert::TryInto, fs::File, mem::size_of, fs::create_dir_all, io::SeekFrom};
        use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
        use std::io::prelude::*;
        use std::path::Prefix::Disk;

        pub trait RaftStorage: tikv_raft::storage::Storage {
            fn append_log(&mut self, entries: &[tikv_raft::eraftpb::Entry]) -> Result<(), tikv_raft::Error>;
            fn set_hard_state(&mut self, commit: u64, term: u64) -> Result<(), Error>;
            fn new_with_conf_state(conf_state: (Vec<u64>, Vec<u64>)) -> Self;
        }

        impl RaftStorage for MemStorage {
            fn append_log(&mut self, entries: &[tikv_raft::eraftpb::Entry]) -> Result<(), tikv_raft::Error> {
                self.wl().append(entries)
            }

            fn set_hard_state(&mut self, commit: u64, term: u64) -> Result<(), Error> {
                self.wl().mut_hard_state().commit = commit;
                self.wl().mut_hard_state().term = term;
                Ok(())
            }

            fn new_with_conf_state(conf_state: (Vec<u64>, Vec<u64>)) -> Self {
                MemStorage::new_with_conf_state(conf_state)
            }
        }

        struct FileMmap{file: File, mem_map: MmapMut}

        impl FileMmap {
            fn new(file: File, mem_map: MmapMut) -> FileMmap{
                FileMmap{ file, mem_map}
            }
        }

        #[derive(Clone)]
        pub struct DiskStorage {
            core: Arc<RwLock<DiskStorageCore>>
        }

        impl DiskStorage {
            // Opens up a read lock on the storage and returns a guard handle. Use this
            // with functions that don't require mutation.
            fn rl(&self) -> RwLockReadGuard<'_, DiskStorageCore> {
                self.core.read().unwrap()
            }

            // Opens up a write lock on the storage and returns guard handle. Use this
            // with functions that take a mutable reference to self.
            fn wl(&self) -> RwLockWriteGuard<'_, DiskStorageCore> {
                self.core.write().unwrap()
            }

            pub fn new(dir: &str) -> DiskStorage {
                let core = Arc::new(RwLock::new(DiskStorageCore::new(dir)));
                DiskStorage{ core }
            }

            pub fn new_with_conf_state<T>(dir: &str, conf_state: T) -> DiskStorage
                where
                    ConfState: From<T>
            {
                let core = Arc::new(RwLock::new(DiskStorageCore::new_with_conf_state(dir, conf_state)));
                DiskStorage{ core }
            }
        }

        impl RaftStorage for DiskStorage {
            fn append_log(&mut self, entries: &[Entry]) -> Result<(), Error> {
                self.wl().append_log(entries)
            }

            fn set_hard_state(&mut self, commit: u64, term: u64) -> Result<(), Error> {
                self.wl().set_hard_state(commit, term)
            }

            fn new_with_conf_state(conf_state: (Vec<u64>, Vec<u64>)) -> Self {
                DiskStorage::new_with_conf_state("./diskstorage", conf_state)
            }
        }

        impl Storage for DiskStorage {
            fn initial_state(&self) -> Result<RaftState, Error> {
                self.rl().initial_state()
            }

            fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>, Error> {
                self.wl().entries(low, high, max_size)
            }

            fn term(&self, idx: u64) -> Result<u64, Error> {
                self.rl().term(idx)
            }

            fn first_index(&self) -> Result<u64, Error> {
                self.rl().first_index()
            }

            fn last_index(&self) -> Result<u64, Error> {
                self.rl().last_index()
            }

            fn snapshot(&self, _request_index: u64) -> Result<Snapshot, Error> {
                unimplemented!()
            }
        }

        struct DiskStorageCore {
            conf_state: ConfState,
            hard_state: MmapMut,
            log: FileMmap,
            offset: FileMmap,   // file that maps from index to byte offset
            raft_metadata: MmapMut, // memory map with metadata of raft index of first and last entry in log
            // TODO: Persist these as well?
            num_entries: u64,
            snapshot_metadata: SnapshotMetadata,
        }

        impl DiskStorageCore {
            const TERM_INDEX: Range<usize> = 0..8;
            const VOTE_INDEX: Range<usize> = 8..16;
            const COMMIT_INDEX: Range<usize> = 16..24;

            const FIRST_INDEX_IS_SET: Range<usize> = 0..1;    // 1 if first_index is set
            const FIRST_INDEX: Range<usize> = 1..9;
            const LAST_INDEX_IS_SET: Range<usize> = 9..10;    // 1 if last_index is set
            const LAST_INDEX: Range<usize> = 10..18;

            const FILE_SIZE: u64 = 8000000;

            fn new(dir: &str) -> DiskStorageCore {
                create_dir_all(dir).expect(&format!("Failed to create given directory: {}", dir));

                let log_path = PathBuf::from(format!("{}/log", dir));
                let log_file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&log_path).expect("Failed to create/open log file");
                log_file.set_len(DiskStorageCore::FILE_SIZE).expect("Failed to set file length of log");  // LogCabin also uses 8MB files...
                let log_mmap = unsafe { MmapMut::map_mut(&log_file).expect("Failed to create memory map for log") };
                let log = FileMmap::new(log_file, log_mmap);

                let hs_path = PathBuf::from(format!("{}/hard_state", dir));
                let hs_file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&hs_path).expect("Failed to create/open hard_state file");
                hs_file.set_len(3*size_of::<u64>() as u64).expect("Failed to set file length of hard_state");    // we only need 3 u64: term, vote and commit
                let hard_state = unsafe { MmapMut::map_mut(&hs_file).expect("Failed to create memory map for hard_state") };

                let offset_path = PathBuf::from(format!("{}/offset", dir));
                let offset_file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&offset_path).expect("Failed to create/open offset file");
                offset_file.set_len(DiskStorageCore::FILE_SIZE).expect("Failed to set file length of offset");
                let offset_mmap = unsafe { MmapMut::map_mut(&offset_file).expect("Failed to create memory map for offset") };
                let offset = FileMmap::new(offset_file, offset_mmap);

                let raft_metadata_path = PathBuf::from(format!("{}/raft_metadata", dir));
                let raft_metadata_file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&raft_metadata_path).expect("Failed to create/open raft_metadata file");
                raft_metadata_file.set_len((2*size_of::<u64>() + 2) as u64).expect("Failed to set file length of raft_file");    // we need 2 u64: first and last index and 2 bytes to check if they are set
                let raft_metadata = unsafe { MmapMut::map_mut(&raft_metadata_file).expect("Failed to create memory map for raft_metadata") };

                let conf_state = ConfState::new();
                let snapshot_metadata: SnapshotMetadata = Default::default();

                DiskStorageCore { conf_state, hard_state, log, offset, raft_metadata, num_entries: 0, snapshot_metadata }
            }

            fn new_with_conf_state<T>(dir: &str, conf_state: T) -> DiskStorageCore
                where
                    ConfState: From<T>
            {
                let mut store = DiskStorageCore::new(dir);
                store.conf_state = ConfState::from(conf_state);
                store.snapshot_metadata.index = 1;
                store.snapshot_metadata.term = 1;
                (&mut store.hard_state[DiskStorageCore::TERM_INDEX]).write(&1u64.to_be_bytes()).expect("Failed to write hard state term");
                (&mut store.hard_state[DiskStorageCore::COMMIT_INDEX]).write(&1u64.to_be_bytes()).expect("Failed to write hard state commit");
                store
            }

            fn read_hard_state_field(&self, field: Range<usize>) -> u64 {
                let bytes = self.hard_state.get(field).expect("Failed to read bytes from hard_state");
                u64::from_be_bytes(bytes.try_into().expect("Failed to deserialise to u64"))
            }

            fn get_hard_state(&self) -> HardState {
                let term = self.read_hard_state_field(DiskStorageCore::TERM_INDEX);
                let commit = self.read_hard_state_field(DiskStorageCore::COMMIT_INDEX);
                let vote = self.read_hard_state_field(DiskStorageCore::VOTE_INDEX);
                let mut hs = HardState::new();
                hs.set_term(term);
                hs.set_commit(commit);
                hs.set_vote(vote);
                hs
            }

            fn append_entries(&mut self, entries: &[Entry], from_log_index: u64) -> Result<(), Error> {    // appends all entries starting from a given log index
                let from = if from_log_index >= self.num_entries {
                    SeekFrom::Current(0)
                } else {
                    SeekFrom::Start(self.get_log_offset(from_log_index) as u64)
                };
                self.log.file.seek(from)?;
                let num_added_entries = entries.len() as u64;
                let new_first_index = entries[0].index;
                let new_last_index = entries.last().unwrap().index;
                for (i, e) in entries.into_iter().enumerate() {
                    let current_offset = self.log.file.seek(SeekFrom::Current(0)).expect("Failed to get current seeked offset");    // byte offset in log file
                    let ser_entry = e.write_to_bytes().expect("Protobuf failed to serialise Entry");
                    let ser_entry_len = ser_entry.len() as u64;
                    let ser_len = ser_entry_len.to_be_bytes();
                    self.log.file.write(&ser_len)?;  // write len of serialised entry
                    match self.log.file.write(&ser_entry) { // write entry
                        Ok(_) => {
                            // write to offset file
                            let start = (from_log_index as usize + i) * size_of::<u64>();
                            let stop = start + size_of::<u64>();
                            (&mut self.offset.mem_map[start..stop]).write(&current_offset.to_be_bytes()).expect("Failed to write to offset");
                        }
                        _ => panic!("Failed to write to log")
                    }
                }
                if from_log_index == 0 {
                    self.set_raft_metadata(DiskStorageCore::FIRST_INDEX, new_first_index).expect("Failed to set first index metadata");
                }
                let num_removed_entries = self.num_entries - from_log_index;
                self.num_entries = self.num_entries + num_added_entries - num_removed_entries;
                self.set_raft_metadata(DiskStorageCore::LAST_INDEX, new_last_index).expect("Failed to set last index metadata");
                self.log.file.flush()?;
                self.offset.file.flush()?;
//        println!("N: {}, last_index: {}", self.num_entries, new_last_index);
                Ok(())
            }

            // returns byte offset in log from log_index
            fn get_log_offset(&self, log_index: u64) -> usize {
                let s = size_of::<u64>();
                let start = (log_index as usize) * s;
                let stop = start + s;
                let bytes = self.offset.mem_map.get(start..stop);
                let offset = u64::from_be_bytes(bytes.unwrap().try_into().unwrap());
                offset as usize
            }

            fn get_entry(&self, index: u64) -> Entry {
                let start = self.get_log_offset(index);
                let stop = start + size_of::<u64>();
                let entry_len = self.log.mem_map.get(start..stop);
                let des_entry_len = u64::from_be_bytes(entry_len.unwrap().try_into().unwrap());
                let r = stop..(stop + des_entry_len as usize);
                let entry = self.log.mem_map.get(r).expect(&format!("Failed to get serialised entry in range {}..{}", stop, stop + des_entry_len as usize));
                let des_entry = parse_from_bytes::<Entry>(entry).expect("Protobuf failed to deserialise entry");
                des_entry
            }

            fn set_raft_metadata(&mut self, field: Range<usize>, value: u64) -> Result<(), Error> {
                assert!(field == DiskStorageCore::FIRST_INDEX || field == DiskStorageCore::LAST_INDEX);
                match field {
                    DiskStorageCore::FIRST_INDEX => {
                        (&mut self.raft_metadata[DiskStorageCore::FIRST_INDEX_IS_SET]).write(&[1u8]).expect("Failed to set first_index bit");
                    },
                    DiskStorageCore::LAST_INDEX => {
                        (&mut self.raft_metadata[DiskStorageCore::LAST_INDEX_IS_SET]).write(&[1u8]).expect("Failed to set last_index bit");
                    },
                    _ => panic!("Unexpected field"),
                }
                (&mut self.raft_metadata[field]).write(&value.to_be_bytes()).expect("Failed to write raft metadata");
                self.raft_metadata.flush()?;
                Ok(())
            }

            fn get_raft_metadata(&self, field: Range<usize>) -> Option<u64> {
                match field {
                    DiskStorageCore::FIRST_INDEX => {
                        if self.raft_metadata.get(DiskStorageCore::FIRST_INDEX_IS_SET).unwrap()[0] == 0 {
                            return None;
                        } else {
                            let bytes = self.raft_metadata.get(field).unwrap();
                            return Some(u64::from_be_bytes(bytes.try_into().expect("Could not deserialise to u64")));
                        }
                    },
                    DiskStorageCore::LAST_INDEX => {
                        if self.raft_metadata.get(DiskStorageCore::LAST_INDEX_IS_SET).unwrap()[0] == 0 {
                            return None;
                        } else {
                            let bytes = self.raft_metadata.get(field).unwrap();
                            return Some(u64::from_be_bytes(bytes.try_into().expect("Could not deserialise to u64")));
                        }
                    },
                    _ => panic!("Got unexpected field in get_raft_metadata"),
                }
            }

            fn append_log(&mut self, entries: &[Entry]) -> Result<(), Error> {
                if entries.is_empty() { return Ok(()); }
                let first_index = self.first_index().expect("Failed to get first index");
                if first_index > entries[0].index {
                    panic!(
                        "overwrite compacted raft logs, compacted: {}, append: {}",
                        first_index - 1,
                        entries[0].index,
                    );
                }
                let last_index = self.last_index().expect("Failed to get last index");
                if last_index + 1 < entries[0].index {
                    panic!(
                        "raft logs should be continuous, last index: {}, new appended: {}",
                        last_index,
                        entries[0].index,
                    );
                }
                let diff = entries[0].index - first_index;
                self.append_entries(entries, diff)
            }

            fn set_hard_state(&mut self, commit: u64, term: u64) -> Result<(), Error> {
                (&mut self.hard_state[DiskStorageCore::TERM_INDEX]).write(&term.to_be_bytes())?;
                (&mut self.hard_state[DiskStorageCore::COMMIT_INDEX]).write(&commit.to_be_bytes())?;
                Ok(())
            }

            fn new_with_conf_state(conf_state: (Vec<u64>, Vec<u64>)) -> Self {
                DiskStorageCore::new_with_conf_state("./diskstorage", conf_state)
            }
        }

        impl Storage for DiskStorageCore {
            fn initial_state(&self) -> Result<RaftState, Error> {
                let hard_state = self.get_hard_state();
                let rs = RaftState::new(hard_state, self.conf_state.clone());
                Ok(rs)
            }

            fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>, Error> {
                let first_index = self.first_index()?;
                if low < first_index {
                    return Err(Error::Store(StorageError::Compacted));
                }
                let last_index = self.last_index()?;
                if high > last_index + 1 {
                    panic!(
                        "index out of bound (last: {}, high: {})",
                        last_index + 1,
                        high
                    );
                }
                let offset = first_index;
                let lo = low - offset;
                let hi = high - offset;
                let mut ents: Vec<Entry> = Vec::new();
                for i in lo..hi {
                    ents.push(self.get_entry(i))
                }
                let max_size = max_size.into();
                limit_size(&mut ents, max_size);
                Ok(ents)
            }

            fn term(&self, idx: u64) -> Result<u64, Error> {
                if idx == self.snapshot_metadata.index {
                    return Ok(self.snapshot_metadata.term);
                }
                let offset = self.first_index()?;
                if idx < offset {
                    return Err(Error::Store(StorageError::Compacted));
                }
                let log_index = idx - offset;
                if log_index >= self.num_entries {
                    println!("{}", format!("log_index: {}, num_entries: {}", log_index, self.num_entries));
                    return Err(Error::Store(StorageError::Unavailable));
                }
                Ok(self.get_entry(log_index).term)
            }

            fn first_index(&self) -> Result<u64, Error> {
                match self.get_raft_metadata(DiskStorageCore::FIRST_INDEX) {
                    Some(index) => Ok(index),
                    None => Ok(self.snapshot_metadata.index + 1)
                }
            }

            fn last_index(&self) -> Result<u64, Error> {
                match self.get_raft_metadata(DiskStorageCore::LAST_INDEX) {
                    Some(index) => Ok(index),
                    None => Ok(self.snapshot_metadata.index)
                }
            }

            fn snapshot(&self, _request_index: u64) -> Result<Snapshot, Error> {
                unimplemented!();
            }
        }

        #[cfg(test)]
        mod test {
            use super::*;
            use std::fs::remove_dir_all;

            fn new_entry(index: u64, term: u64) -> Entry {
                let mut e = Entry::default();
                e.term = term;
                e.index = index;
                e
            }

            fn size_of<T: PbMessage>(m: &T) -> u32 {
                m.compute_size() as u32
            }

            #[test]
            fn diskstorage_term_test() {
                let ents = vec![new_entry(2, 2), new_entry(3, 3)];
                let mut tests = vec![
                    (0, Err(Error::Store(StorageError::Compacted))),
                    (2, Ok(2)),
                    (3, Ok(3)),
                    (6, Err(Error::Store(StorageError::Unavailable))),
                ];

                let mut store = DiskStorage::new_with_conf_state("term_test", (vec![1,2,3], vec![]));
                store.append_log(&ents).expect("Failed to append logs");

                for (i, (idx, wterm)) in tests.drain(..).enumerate() {
                    let t = store.term(idx);
                    if t != wterm {
                        panic!("#{}: expect res {:?}, got {:?}", i, wterm, t);
                    }
                }
                remove_dir_all("term_test").expect("Failed to remove test storage files");
            }

            #[test]
            fn diskstorage_entries_test(){
                let ents = vec![
                    new_entry(2, 2),
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 6),
                ];
                let max_u64 = u64::max_value();
                let mut tests = vec![
                    (0, 6, max_u64, Err(Error::Store(StorageError::Compacted))),
                    (3, 4, max_u64, Ok(vec![new_entry(3, 3)])),
                    (4, 5, max_u64, Ok(vec![new_entry(4, 4)])),
                    (4, 6, max_u64, Ok(vec![new_entry(4, 4), new_entry(5, 5)])),
                    (4, 7, max_u64, Ok(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)])),
                    // even if maxsize is zero, the first entry should be returned
                    (4, 7, 0, Ok(vec![new_entry(4, 4)])),
                    // limit to 2
                    (4, 7, u64::from(size_of(&ents[1]) + size_of(&ents[2])), Ok(vec![new_entry(4, 4), new_entry(5, 5)])),
                    (4, 7, u64::from(size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3]) / 2), Ok(vec![new_entry(4, 4), new_entry(5, 5)])),
                    (4, 7, u64::from(size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3]) - 1), Ok(vec![new_entry(4, 4), new_entry(5, 5)])),
                    // all
                    (4, 7, u64::from(size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3])), Ok(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)])),
                ];
                let mut storage = DiskStorage::new_with_conf_state("entries_test", (vec![1,2,3], vec![]));
                storage.append_log(&ents).expect("Failed to append logs");
                for (i, (lo, hi, maxsize, wentries)) in tests.drain(..).enumerate() {
                    let e = storage.entries(lo, hi, maxsize);
                    if e != wentries {
                        panic!("#{}: expect entries {:?}, got {:?}", i, wentries, e);
                    }
                }
                remove_dir_all("entries_test").expect("Failed to remove test storage files");
            }

            #[test]
            fn diskstorage_overwrite_entries_test() {
                let ents = vec![
                    new_entry(2, 2),
                    new_entry(3, 3),
                    new_entry(4, 3),
                    new_entry(5, 3),
                    new_entry(6, 3),
                ];
                let mut storage = DiskStorage::new_with_conf_state("overwrite_entries_test", (vec![1,2,3], vec![]));
                storage.append_log(&ents).expect("Failed to append logs");
                let overwrite_ents = vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                ];
                storage.append_log(&overwrite_ents).expect("Failed to append logs");
                let tests = vec![(3, Ok(3)), (4, Ok(4)), (5, Ok(5)), (6, Err(Error::Store(StorageError::Unavailable)))];
                for (idx, exp) in tests {
                    let res = storage.term(idx as u64);
                    assert_eq!(res, exp);
                }
                remove_dir_all("overwrite_entries_test").expect("Failed to remove test storage files");
            }

            #[test]
            fn diskstorage_metadata_test() {
                let ents = vec![
                    new_entry(2, 2),
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 6),
                ];
                let mut storage = DiskStorage::new_with_conf_state("metadata_test", (vec![1,2,3], vec![]));
                storage.append_log(&ents).expect("Failed to append logs");
                assert_eq!(storage.first_index(), Ok(2));
                assert_eq!(storage.last_index(), Ok(6));
                let overwrite_ents = vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                ];
                storage.append_log(&overwrite_ents).expect("Failed to append logs");
                assert_eq!(storage.first_index(), Ok(2));
                assert_eq!(storage.last_index(), Ok(5));
                remove_dir_all("metadata_test").expect("Failed to remove test storage files");
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use std::str::FromStr;
        use super::*;
        use std::fs::remove_dir_all;

        fn example_config() -> Config {
            Config {
                election_tick: 10,
                heartbeat_tick: 3,
                ..Default::default()
            }
        }

        #[test]
        fn kompact_raft_ser_test() {
            use super::*;

            use tikv_raft::prelude::{MessageType, Entry, EntryType, ConfChange, ConfChangeType, Message as TikvRaftMsg};
            use protobuf::RepeatedField;
            /*** RaftMsg ***/
            let from: u64 = 1;
            let to: u64 = 2;
            let term: u64 = 3;
            let index: u64 = 4;
            let iteration_id: u32 = 5;

            let msg_type: MessageType = MessageType::MsgPropose;
            let mut entry = Entry::new();
            entry.set_term(term);
            entry.set_index(index);
            entry.set_entry_type(EntryType::EntryNormal);
            let entries: RepeatedField<Entry> = RepeatedField::from_vec(vec![entry]);

            let mut payload = TikvRaftMsg::new();
            payload.set_from(from);
            payload.set_to(to);
            payload.set_msg_type(msg_type);
            payload.set_entries(entries);
            let rm = RaftMsg { iteration_id, payload: payload.clone() };

            let mut bytes: Vec<u8> = vec![];
            if RaftSer.serialise(&CommunicatorMsg::TikvRaftMsg(rm), &mut bytes).is_err(){panic!("Failed to serialise TikvRaftMsg")};
            let mut buf = bytes.into_buf();
            match RaftSer::deserialise(&mut buf) {
                Ok(des) => {
                    match des {
                        CommunicatorMsg::TikvRaftMsg(rm) => {
                            let des_iteration_id = rm.iteration_id;
                            let des_payload = rm.payload;
                            let des_from = des_payload.get_from();
                            let des_to = des_payload.get_to();
                            let des_msg_type = des_payload.get_msg_type();
                            let des_entries = des_payload.get_entries();
                            assert_eq!(des_iteration_id, iteration_id);
                            assert_eq!(from, des_from);
                            assert_eq!(to, des_to);
                            assert_eq!(msg_type, des_msg_type);
                            assert_eq!(des_payload.get_entries(), des_entries);
                            assert_eq!(des_payload, payload);
                            println!("Ser/Des RaftMsg passed");
                        },
                        _ => panic!("Deserialised message should be RaftMsg")
                    }
                },
                _ => panic!("Failed to deserialise RaftMsg")
            }
            /*** Proposal ***/
            let client = ActorPath::from_str("local://127.0.0.1:0/test_actor").expect("Failed to create test actorpath");
            let mut b: Vec<u8> = vec![];
            let id: u64 = 12;
            let configchange_id: u64 = 31;
            let start_index: u64 = 47;
            let change_type = ConfChangeType::AddNode;
            let mut cc = ConfChange::new();
            cc.set_id(configchange_id);
            cc.set_change_type(change_type);
            cc.set_start_index(start_index);
            let p = Proposal::conf_change(id, client.clone(), &cc);
            if RaftSer.serialise(&CommunicatorMsg::Proposal(p), &mut b).is_err() {panic!("Failed to serialise Proposal")};
            match RaftSer::deserialise(&mut b.into_buf()){
                Ok(c) => {
                    match c {
                        CommunicatorMsg::Proposal(p) => {
                            let des_id = p.id;
                            let des_client = p.client;
                            match p.conf_change {
                                Some(cc) => {
                                    let cc_id = cc.get_id();
                                    let cc_change_type = cc.get_change_type();
                                    let cc_start_index = cc.get_start_index();
                                    assert_eq!(id, des_id);
                                    assert_eq!(client, des_client);
                                    assert_eq!(configchange_id, cc_id);
                                    assert_eq!(start_index, cc_start_index);
                                    assert_eq!(change_type, cc_change_type);
                                    println!("Ser/Des Proposal passed");
                                }
                                _ => panic!("ConfChange should not be None")
                            }
                        }
                        _ => panic!("Deserialised message should be Proposal")
                    }
                }
                _ => panic!("Failed to deserialise Proposal")
            }
            /*** ProposalResp ***/
            let succeeded = true;
            let change_type = ConfChangeType::AddNode;
            let pr = ProposalResp {
                id,
                client: None,
                succeeded,
                conf_change: Some((id, change_type))
            };
            let mut b1: Vec<u8> = vec![];
            if RaftSer.serialise(&CommunicatorMsg::ProposalResp(pr), &mut b1).is_err(){panic!("Failed to serailise ProposalResp")};
            match RaftSer::deserialise(&mut b1.into_buf()){
                Ok(cm) => {
                    match cm {
                        CommunicatorMsg::ProposalResp(pr) => {
                            let des_id = pr.id;
                            let des_succeeded = pr.succeeded;
                            let des_client = pr.client;
                            let des_conf_change = pr.conf_change;
                            assert_eq!(id, des_id);
                            assert_eq!(None, des_client);
                            assert_eq!(succeeded, des_succeeded);
                            assert_eq!(Some((id, change_type)), des_conf_change);
                            println!("Ser/Des ProposalResp passed");
                        }
                        _ => panic!("Deserialised message should be ProposalResp")
                    }
                }
                _ => panic!("Failed to deserialise ProposalResp")
            }
        }

        #[test]
        fn tikv_raft_ser_test() {
            let client = ActorPath::from_str("local://127.0.0.1:0/test_actor").expect("Failed to create test actorpath");
            let id = 346;
            let proposal = Proposal::normal(id, client);
            let bytes = proposal.serialize_normal().expect("Failed to tikv serialize proposal");
            let des_proposal = Proposal::deserialize_normal(&bytes);
            assert_eq!(proposal.id, des_proposal.id);
            assert_eq!(proposal.client, des_proposal.client);
            assert_eq!(proposal.conf_change, des_proposal.conf_change);
        }
    }
}