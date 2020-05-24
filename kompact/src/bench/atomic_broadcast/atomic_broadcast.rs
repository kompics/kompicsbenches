extern crate raft as tikv_raft;

use super::super::*;
use benchmark_suite_shared::kompics_benchmarks::benchmarks::{AtomicBroadcastRequest};
use kompact::prelude::*;
use synchronoise::CountdownEvent;
use std::sync::Arc;
use std::str::FromStr;
use super::raft::{RaftReplica};
use super::paxos::{PaxosReplica, TransferPolicy};
use super::storage::paxos::{MemoryState, MemorySequence};
use partitioning_actor::PartitioningActor;
use super::client::{Client};
use super::messages::Run;
use std::collections::HashMap;
use crate::partitioning_actor::IterationControlMsg;

#[allow(unused_imports)]
use super::storage::raft::DiskStorage;
use tikv_raft::{storage::MemStorage};

const PAXOS_PATH: &'static str = "paxos_replica";
const RAFT_PATH: &'static str = "raft_replica";

#[derive(Debug, Clone)]
pub struct ClientParams {
    algorithm: Algorithm,
    last_node_id: u64,
    transfer_policy: Option<TransferPolicy>,
    forward_discarded: bool
}

impl ClientParams {
    fn with(algorithm: Algorithm, last_node_id: u64, transfer_policy: Option<TransferPolicy>, forward_discarded: bool) -> ClientParams {
        ClientParams{ algorithm, last_node_id, transfer_policy, forward_discarded }
    }
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
        if split.len() != 4 {
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
                        split[0]
                    )));
                }
            };
            let last_node_id = split[1].parse::<u64>().map_err(|e| {
                BenchmarkError::InvalidMessage(format!(
                    "String to ClientConf error: '{}' does not represent a node id: {:?}",
                    split[1], e
                ))
            })?;
            let transfer_policy = match split[2].to_lowercase().as_ref() {
                "eager" => Some(TransferPolicy::Eager),
                "pull" => Some(TransferPolicy::Pull),
                "none" => None,
                _ => {
                    return Err(BenchmarkError::InvalidMessage(format!(
                        "String to ClientConf error: '{}' does not represent a transfer policy",
                        split[2]
                    )));
                }
            };
            let forward_discarded = split[3].parse::<bool>().map_err(|e| {
                BenchmarkError::InvalidMessage(format!(
                    "String to ClientConf error: '{}' does not represent a bool: {:?}",
                    split[3], e
                ))
            })?;
            Ok(ClientParams::with(algorithm, last_node_id, transfer_policy, forward_discarded))
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
        let tp = match c.transfer_policy {
            Some(TransferPolicy::Eager) => "eager",
            Some(TransferPolicy::Pull) => "pull",
            None => "none"
        };
        let s = format!("{},{},{},{}", a, c.last_node_id, tp, c.forward_discarded);
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

fn get_reconfig_data(s: &str, n: u64) -> Result<(u64, Option<(Vec<u64>, Vec<u64>)>), BenchmarkError> {
    match s.to_lowercase().as_ref() {
        "off" => {
            Ok((0, None))
        },
        "single" => {
            let mut reconfig: Vec<u64> = (1..n).collect();
            reconfig.push(n+1);
            let new_followers: Vec<u64> = vec![];
            let reconfiguration = Some((reconfig, new_followers));
            Ok((1, reconfiguration))
        },
        "majority" => {
            let majority = n/2 + 1;
            let mut reconfig: Vec<u64> = (1..majority).collect();   // minority i.e. continued node ids
            for i in 1..=majority {
                let new_node_id = n + i;
                reconfig.push(new_node_id);
            }
            assert_eq!(n, reconfig.len() as u64);
            let new_followers: Vec<u64> = vec![];
            let reconfiguration = Some((reconfig, new_followers));
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
    batch_size: Option<u64>,
    reconfiguration: Option<(Vec<u64>, Vec<u64>)>,
    paxos_transfer_policy: Option<TransferPolicy>,
    forward_discarded: bool,
    system: Option<KompactSystem>,
    finished_latch: Option<Arc<CountdownEvent>>,
    iteration_id: u32,
    paxos_replica: Option<Arc<Component<PaxosReplica<MemorySequence, MemoryState>>>>,
    raft_replica: Option<Arc<Component<RaftReplica<Storage>>>>,
    client_comp: Option<Arc<Component<Client>>>,
    partitioning_actor: Option<Arc<Component<PartitioningActor>>>,
}

impl AtomicBroadcastMaster {
    fn new() -> AtomicBroadcastMaster {
        AtomicBroadcastMaster {
            algorithm: None,
            num_nodes: None,
            num_proposals: None,
            batch_size: None,
            reconfiguration: None,
            paxos_transfer_policy: Some(TransferPolicy::Pull),
            forward_discarded: false,
            system: None,
            finished_latch: None,
            iteration_id: 0,
            paxos_replica: None,
            raft_replica: None,
            client_comp: None,
            partitioning_actor: None,
        }
    }

    fn initialise_iteration(&self, nodes: Vec<ActorPath>, client: ActorPath) -> Arc<Component<PartitioningActor>> {
        let system = self.system.as_ref().unwrap();
        let prepare_latch = Arc::new(CountdownEvent::new(1));
        /*** Setup partitioning actor ***/
        let (partitioning_actor, unique_reg_f) = system.create_and_register(|| {
            PartitioningActor::with(
                prepare_latch.clone(),
                None,
                self.iteration_id,
                nodes,
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
        let mut ser_client = Vec::<u8>::new();
        client.serialise(&mut ser_client).expect("Failed to serialise ClientComp actorpath");
        partitioning_actor.actor_ref().tell(IterationControlMsg::Prepare(Some(ser_client)));
        prepare_latch.wait();
        partitioning_actor
    }

    fn create_client(
        &self,
        nodes_id: HashMap<u64, ActorPath>,
        reconfig: Option<(Vec<u64>, Vec<u64>)>
    ) -> (Arc<Component<Client>>, ActorPath) {
        let system = self.system.as_ref().unwrap();
        let finished_latch = self.finished_latch.clone().unwrap();
        /*** Setup client ***/
        let (client_comp, unique_reg_f) = system.create_and_register( || {
            Client::with(
                self.num_proposals.unwrap(),
                self.batch_size.unwrap(),
                nodes_id,
                reconfig,
                finished_latch,
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
        let named_reg_f = system.register_by_alias(
            &client_comp,
            format!("client{}", &self.iteration_id),
        );
        named_reg_f.wait_expect(
            Duration::from_millis(1000),
            "Failed to register alias for ClientComp"
        );
        let client_path = ActorPath::Named(NamedPath::with_system(
            system.system_path(),
            vec![format!("client{}", &self.iteration_id).into()],
        ));
        (client_comp, client_path)
    }

    fn validate_and_set_experiment_args(&mut self, c: &AtomicBroadcastRequest, num_clients: u32) -> Result<(), BenchmarkError> {  // TODO reconfiguration
        self.num_nodes = Some(c.number_of_nodes);
        self.num_proposals = Some(c.number_of_proposals);
        self.batch_size = Some(c.batch_size);
        if c.batch_size > c.number_of_proposals {
            return Err(BenchmarkError::InvalidTest(
                format!("Batch size: {} should be less or equal to number of proposals: {}", c.batch_size, c.number_of_proposals)
            ));
        }
        match c.algorithm.to_lowercase().as_ref() {
            "paxos" => {
                self.algorithm = Some(Algorithm::Paxos);
                self.forward_discarded = c.forward_discarded;
                match c.reconfiguration.to_lowercase().as_ref() {
                    "off" => {
                        if c.transfer_policy.to_lowercase() != "none" {
                            return Err(BenchmarkError::InvalidTest(
                                format!("Reconfiguration is off, transfer policy should be none, but found: {}", &c.transfer_policy)
                            ));
                        }
                    },
                    s if s == "single" || s == "majority" => {
                        self.paxos_transfer_policy = match c.transfer_policy.to_lowercase().as_ref() {
                            "eager" => Some(TransferPolicy::Eager),
                            "pull" => Some(TransferPolicy::Pull),
                            _ => {
                                return Err(BenchmarkError::InvalidTest(
                                    format!("Unimplemented Paxos transfer policy: {}", &c.transfer_policy)
                                ));
                            }
                        }
                    },
                    _ => {}
                }
            },
            "raft" => {
                self.algorithm = Some(Algorithm::Raft);
                if c.forward_discarded {
                    return Err(BenchmarkError::InvalidTest(
                        format!("Unimplemented forward discarded for: {}", &c.algorithm)
                    ));
                }
                self.paxos_transfer_policy = match c.transfer_policy.to_lowercase().as_ref() {
                    "none" => None,
                    _ => {
                        return Err(BenchmarkError::InvalidTest(
                            format!("Unimplemented Raft transfer policy: {}", &c.transfer_policy)
                        ));
                    }
                };
            },
            _ => {
                return Err(BenchmarkError::InvalidTest(
                    format!("Unimplemented atomic broadcast algorithm: {}", &c.algorithm)
                ));
            }
        };
        match get_reconfig_data(&c.reconfiguration, c.number_of_nodes) {
            Ok((additional_n, reconfig)) => {
                let n = c.number_of_nodes + additional_n;
                if num_clients as u64 + 1 < n {
                    return Err(BenchmarkError::InvalidTest(format!(
                        "Not enough clients: {}, Required: {}",
                        &num_clients,
                        &(n-1)
                    )));
                }
                self.reconfiguration = reconfig;
            },
            Err(e) => return Err(e)
        }
        Ok(())
    }
}

impl DistributedBenchmarkMaster for AtomicBroadcastMaster {
    type MasterConf = AtomicBroadcastRequest;
    type ClientConf = ClientParams;
    type ClientData = ActorPath;

    fn setup(&mut self, c: Self::MasterConf, m: &DeploymentMetaData) -> Result<Self::ClientConf, BenchmarkError> {
        println!("Setting up Atomic Broadcast (Master)");
        self.validate_and_set_experiment_args(&c, m.number_of_clients())?;
        let system = crate::kompact_system_provider::global().new_remote_system_with_threads("atomicbroadcast", 4);
        self.system = Some(system);
        let params = ClientParams::with(
            self.algorithm.clone().unwrap(),
            c.number_of_nodes,
            self.paxos_transfer_policy.clone(),
            c.forward_discarded
        );
        Ok(params)
    }

    fn prepare_iteration(&mut self, d: Vec<Self::ClientData>) -> () {
        println!("Preparing iteration");
        match self.system {
            Some(ref system) => {
                let finished_latch = Arc::new(CountdownEvent::new(1));
                self.finished_latch = Some(finished_latch);
                self.iteration_id += 1;
                let self_path = match self.algorithm {
                    Some(Algorithm::Paxos) => {
                        let initial_config = get_initial_conf(self.num_nodes.unwrap()).0;
                        let (paxos_replica, unique_reg_f) = system.create_and_register(|| {
                            PaxosReplica::with(initial_config, self.paxos_transfer_policy.clone().unwrap(), self.forward_discarded)
                        });
                        unique_reg_f.wait_expect(
                            Duration::from_millis(1000),
                            "ReplicaComp failed to register!",
                        );
                        let named_reg_f = system.register_by_alias(
                            &paxos_replica,
                            format!("{}{}", PAXOS_PATH, &self.iteration_id),
                        );
                        named_reg_f.wait_expect(
                            Duration::from_millis(1000),
                            "Failed to register alias for ReplicaComp"
                        );
                        let paxos_replica_f = system.start_notify(&paxos_replica);
                        paxos_replica_f
                            .wait_timeout(Duration::from_millis(1000))
                            .expect("ReplicaComp never started!");
                        let self_path = ActorPath::Named(NamedPath::with_system(
                            system.system_path(),
                            vec![format!("{}{}", PAXOS_PATH, &self.iteration_id).into()],
                        ));

                        self.paxos_replica = Some(paxos_replica);
                        self_path
                    }
                    Some(Algorithm::Raft) => {
                        let conf_state = get_initial_conf(self.num_nodes.unwrap() as u64);
                        let (raft_replica, unique_reg_f) = system.create_and_register(|| {
                            RaftReplica::<Storage>::with(conf_state.0)
                        });
                        unique_reg_f.wait_expect(
                            Duration::from_millis(1000),
                            "RaftComp failed to register!",
                        );
                        let named_reg_f = system.register_by_alias(
                            &raft_replica,
                            format!("{}{}", RAFT_PATH, &self.iteration_id),
                        );
                        named_reg_f.wait_expect(
                            Duration::from_millis(1000),
                            "RaftReplica failed to register!",
                        );
                        let raft_replica_f = system.start_notify(&raft_replica);
                        raft_replica_f
                            .wait_timeout(Duration::from_millis(1000))
                            .expect("RaftComp never started!");
                        let self_path = ActorPath::Named(NamedPath::with_system(
                            system.system_path(),
                            vec![format!("{}{}", RAFT_PATH, &self.iteration_id).into()],
                        ));

                        self.raft_replica = Some(raft_replica);
                        self_path
                    }
                    _ => unimplemented!(),
                };
                println!("Master replica path: {:?}", self_path);
                let mut nodes = d;
                nodes.insert(0, self_path);
                let mut nodes_id: HashMap<u64, ActorPath> = HashMap::new();
                for (id, actorpath) in nodes.iter().enumerate() {
                    nodes_id.insert(id as u64 + 1, actorpath.clone());
                }
                let (client_comp, client_path) = self.create_client(nodes_id, self.reconfiguration.clone());
                self.partitioning_actor = Some(self.initialise_iteration(nodes, client_path));
                self.client_comp = Some(client_comp);
            }
            None => unimplemented!()
        }
    }

    fn run_iteration(&mut self) -> () {
        println!("Running Atomic Broadcast experiment!");
        match self.partitioning_actor {
            Some(ref p_actor) => {
                p_actor.actor_ref().tell(IterationControlMsg::Run);
            },
            _ => panic!("No partitioning actor found!"),
        }
        match self.client_comp {
            Some(ref client_comp) => {
                client_comp.actor_ref().tell(Run);
                let finished_latch = self.finished_latch.take().unwrap();
                finished_latch.wait();
            }
            _ => panic!("No client found!"),
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

        if let Some(partitioning_actor) = self.partitioning_actor.take() {
            let stop_f =
                partitioning_actor
                .actor_ref()
                .ask( |promise| IterationControlMsg::Stop(Ask::new(promise, ())));

            stop_f.wait();
            // stop_f.wait_timeout(STOP_TIMEOUT).expect("Timed out while stopping iteration");

            let kill_pactor_f = system.kill_notify(partitioning_actor);
            kill_pactor_f
                .wait_timeout(Duration::from_millis(1000))
                .expect("Partitioning Actor never died!");
        }

        if let Some(paxos_replica_comp) = self.paxos_replica.take() {
            let kill_replica_f = system.kill_notify(paxos_replica_comp);
            kill_replica_f
                .wait_timeout(Duration::from_millis(1000))
                .expect("Paxos Replica never died!");
        }

        if let Some(raft_replica) = self.raft_replica.take() {
            let kill_raft_f = system.kill_notify(raft_replica);
            kill_raft_f
                .wait_timeout(Duration::from_millis(1000))
                .expect("RaftComp never died!");
        }

        if last_iteration {
            println!("Cleaning up last iteration");
            system
                .shutdown()
                .expect("Kompact didn't shut down properly");
            self.num_proposals = None;
            self.algorithm = None;
            self.batch_size = None;
            self.num_nodes = None;
        } else {
            self.system = Some(system);
        }
    }
}

pub struct AtomicBroadcastClient {
    system: Option<KompactSystem>,
    paxos_replica: Option<Arc<Component<PaxosReplica<MemorySequence, MemoryState>>>>,
    raft_replica: Option<Arc<Component<RaftReplica<Storage>>>>,
}

impl AtomicBroadcastClient {
    fn new() -> AtomicBroadcastClient {
        AtomicBroadcastClient {
            system: None,
            paxos_replica: None,
            raft_replica: None
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
                let initial_config = get_initial_conf(c.last_node_id).0;
                let (paxos_replica, unique_reg_f) = system.create_and_register(|| {
                    PaxosReplica::with(initial_config, c.transfer_policy.unwrap(), c.forward_discarded)
                });
                unique_reg_f.wait_expect(
                    Duration::from_millis(1000),
                    "ReplicaComp failed to register!",
                );
                let named_reg_f = system.register_by_alias(
                    &paxos_replica,
                    PAXOS_PATH,
                );
                named_reg_f.wait_expect(
                    Duration::from_millis(1000),
                    "Failed to register alias for ReplicaComp"
                );
                let paxos_replica_f = system.start_notify(&paxos_replica);
                paxos_replica_f
                    .wait_timeout(Duration::from_millis(1000))
                    .expect("ReplicaComp never started!");
                let self_path = ActorPath::Named(NamedPath::with_system(
                    system.system_path(),
                    vec![PAXOS_PATH.into()],
                ));

                self.paxos_replica = Some(paxos_replica);
                self_path
            }
            Algorithm::Raft => {
                let conf_state = get_initial_conf(c.last_node_id);
//                let storage = MemStorage::new_with_conf_state(conf_state);
//                let dir = "./diskstorage";
//                let storage = DiskStorage::new_with_conf_state(dir, conf_state);
                /*** Setup RaftComp ***/
                let (raft_replica, unique_reg_f) = system.create_and_register(|| {
                    RaftReplica::<Storage>::with(conf_state.0)
                });
                unique_reg_f.wait_expect(
                    Duration::from_millis(1000),
                    "RaftComp failed to register!",
                );
                let named_reg_f = system.register_by_alias(
                    &raft_replica,
                    RAFT_PATH,
                );
                named_reg_f.wait_expect(
                    Duration::from_millis(1000),
                    "Communicator failed to register!",
                );
                let raft_replica_f = system.start_notify(&raft_replica);
                raft_replica_f
                    .wait_timeout(Duration::from_millis(1000))
                    .expect("RaftComp never started!");
                let self_path = ActorPath::Named(NamedPath::with_system(
                    system.system_path(),
                    vec![RAFT_PATH.into()],
                ));

                self.raft_replica = Some(raft_replica);
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
            if let Some(replica) = self.paxos_replica.take() {
                let kill_replica_f = system.kill_notify(replica);
                kill_replica_f
                    .wait_timeout(Duration::from_secs(1))
                    .expect("Paxos Replica never died!");
            }
            if let Some(raft_replica) = self.raft_replica.take() {
                let kill_raft_f = system.kill_notify(raft_replica);
                kill_raft_f
                    .wait_timeout(Duration::from_millis(1000))
                    .expect("Atomic Broadcast never died!");
            }
            system
                .shutdown()
                .expect("Kompact didn't shut down properly");
        }
    }
}