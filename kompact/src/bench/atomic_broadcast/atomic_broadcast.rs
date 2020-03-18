extern crate raft as tikv_raft;

use super::super::*;
use benchmark_suite_shared::kompics_benchmarks::benchmarks::{AtomicBroadcastRequest};
use kompact::prelude::*;
use synchronoise::CountdownEvent;
use std::sync::Arc;
use std::str::FromStr;
use super::raft::{RaftComp, Communicator, MessagingPort};
use tikv_raft::{Config, storage::MemStorage};
use partitioning_actor::PartitioningActor;
use super::client::{Client};
use super::messages::Run;
use std::collections::HashMap;
use super::storage::raft::*;

#[derive(Debug, Clone, PartialEq)]
pub struct ClientParams {
    algorithm: Algorithm,
    last_node_id: u64,
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
            let last_node_id = split[1].parse::<u64>().map_err(|e| {
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

fn get_reconfig_data(s: String, n: u64) -> Result<(u64, Option<(Vec<u64>, Vec<u64>)>), BenchmarkError> {
    match s.to_lowercase().as_ref() {
        "off" => {
            Ok((0, None))
        },
        "single" => {
            let mut new_voters = vec![];
            let new_followers: Vec<u64> = vec![];
            for i in 1..n {
                new_voters.push(i);   // all but last_node_id
            }
            let replacing_node_id = n + 1;
            new_voters.push(replacing_node_id);    // i.e. will replace last_node_id after reconfiguration
            assert_eq!(n, new_voters.len() as u64);
            let reconfiguration = Some((new_voters, new_followers));
            Ok((1, reconfiguration))
        },
        "majority" => {
            let mut new_voters = vec![];
            let new_followers: Vec<u64> = vec![];
            let majority = n/2 + 1;
            for i in 1..majority {
                new_voters.push(i);   // push minority ids
            }
            let first_new_node_id = n + 1;
            let last_new_node_id = n + n - 1;
            for i in first_new_node_id..=last_new_node_id {
                new_voters.push(i);   // push all new majority ids
            }
            assert_eq!(n, new_voters.len() as u64);
            let reconfiguration = Some((new_voters, new_followers));
            Ok((majority, reconfiguration))
        },
        _ => Err(BenchmarkError::InvalidMessage(String::from("Got unknown reconfiguration parameter")))
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Algorithm {
    Paxos,
    Raft,
}

type Storage = MemStorage;

pub struct AtomicBroadcastMaster {
    algorithm: Option<Algorithm>,
    num_nodes: Option<u64>,
    num_proposals: Option<u64>,
    num_parallel_proposals: Option<u64>,
    reconfiguration: Option<(Vec<u64>, Vec<u64>)>,
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
        self.num_nodes = Some(c.number_of_nodes);
        match get_reconfig_data(c.reconfiguration, c.number_of_nodes) {
            Ok((additional_n, reconfig)) => {
                let n = c.number_of_nodes + additional_n;
                if m.number_of_clients() as u64 + 1 < n {
                    return Err(BenchmarkError::InvalidTest(format!(
                        "Not enough clients: {}, Required: {}",
                        &m.number_of_clients(),
                        &(n-1)
                    )));
                }
                self.reconfiguration = reconfig;
            },
            Err(e) => return Err(e)
        }
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
                self.iteration_id += 1;
                match self.algorithm {
                    Some(Algorithm::Paxos) => {
                        unimplemented!()
                    }
                    Some(Algorithm::Raft) => {
                        let conf_state = get_initial_conf(self.num_nodes.unwrap() as u64);
                        let config = Config {
                            election_tick: 10,
                            heartbeat_tick: 3,
                            max_inflight_msgs: self.num_proposals.unwrap() as usize,
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
                        // TODO: create partitioning actor in setup and just trigger an event w. iteration_id on it locally (change atomic register as well)
                        /*** Setup partitioning actor ***/
                        let (partitioning_actor, unique_reg_f) = system.create_and_register(|| {
                            PartitioningActor::with(
                                prepare_latch.clone(),
                                None,
                                self.iteration_id,
                                nodes,
                                1,
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
                                self.reconfiguration.clone(),
                                finished_latch.clone(),
                            )
                        });
                        unique_reg_f.wait_expect(
                            Duration::from_millis(1000),
                            "Client failed to register!",
                        );

                        let client_comp_f = system.start_notify(&client_comp);
                        client_comp_f
                            .wait_timeout(Duration::from_millis(1000))
                            .expect("ClientComp never started!");

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
        match self.client_comp {
            Some(ref client_comp) => {
                client_comp.actor_ref().tell(Run);
                let finished_latch = self.finished_latch.take().unwrap();
                finished_latch.wait();
            }
            _ => unimplemented!()
        }
    }

    fn cleanup_iteration(&mut self, last_iteration: bool, _exec_time_millis: f64) -> () {
        println!("Cleaning up Atomic Broadcast (master) side");
        let system = self.system.take().unwrap();
        let client = self.client_comp.take().unwrap();
        let kill_client_f = system.kill_notify(client);
        kill_client_f
            .wait_timeout(Duration::from_millis(1000))
            .expect("Client never died");

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
            raft_components: None
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
//                let conf_state = get_initial_conf(3);
                let conf_state = get_initial_conf(c.last_node_id);
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

                biconnect_components::<MessagingPort, _, _>(&communicator, &raft_comp)
                    .expect("Could not connect components!");

                let self_path = ActorPath::Named(NamedPath::with_system(
                    system.system_path(),
                    vec!["communicator".into()],
                ));

                self.raft_components = Some((raft_comp, communicator));
                self_path
            }
        };
        self.system = Some(system);
        println!("Got path for Atomic Broadcast actor: {}", named_path);
        named_path
    }

    fn prepare_iteration(&mut self) -> () {
        println!("Preparing Atomic Broadcast (client)");
    }

    fn cleanup_iteration(&mut self, last_iteration: bool) -> () {
        println!("Cleaning up Atomic Broadcast (client)");
        if last_iteration {
            let system = self.system.take().unwrap();
//            if self.paxos_comp.is_some(){
//
//            }
            if self.raft_components.is_some() {
                let (raft_comp, communicator) = self.raft_components.take().unwrap();
                let kill_raft_f = system.kill_notify(raft_comp);
                kill_raft_f
                    .wait_timeout(Duration::from_millis(1000))
                    .expect("Atomic Broadcast never died!");

                let kill_communicator_f = system.kill_notify(communicator);
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