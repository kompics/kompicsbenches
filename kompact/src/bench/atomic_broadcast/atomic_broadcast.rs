extern crate raft as tikv_raft;

use super::super::*;
use benchmark_suite_shared::kompics_benchmarks::benchmarks::{AtomicBroadcastRequest};
use kompact::prelude::*;
use synchronoise::CountdownEvent;
use std::sync::Arc;
use std::str::FromStr;
use super::raft::{RaftReplica, ReconfigurationPolicy as RaftReconfigurationPolicy};
use super::paxos::{PaxosReplica, ReconfigurationPolicy as PaxosReconfigurationPolicy};
use super::storage::paxos::{MemoryState, MemorySequence};
use partitioning_actor::PartitioningActor;
use super::client::{Client, LocalClientMessage};
use super::messages::Run;
use hashbrown::HashMap;
use crate::partitioning_actor::IterationControlMsg;
use hdrhistogram::Histogram;

#[allow(unused_imports)]
use super::storage::raft::DiskStorage;
use tikv_raft::{storage::MemStorage};
use crate::bench::atomic_broadcast::parameters::{LATENCY_DIR, LATENCY_FILE};
use std::fs::{File, create_dir_all, OpenOptions};
use std::error::Error;
use std::io::Write;
use crate::bench::atomic_broadcast::parameters::client::PROPOSAL_TIMEOUT;
use std::time::{SystemTime, UNIX_EPOCH};
use std::io;

const PAXOS_PATH: &'static str = "paxos_replica";
const RAFT_PATH: &'static str = "raft_replica";
const REGISTER_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct ClientParams {
    algorithm: String,
    last_node_id: u64,
    reconfig_policy: String,
}

impl ClientParams {
    fn with(algorithm: String, last_node_id: u64, reconfig_policy: String) -> ClientParams {
        ClientParams{ algorithm, last_node_id, reconfig_policy }
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
        if split.len() != 3 {
            Err(BenchmarkError::InvalidMessage(format!(
                "String '{}' does not represent a client conf! Split length should be 3",
                s
            )))
        } else {
            let algorithm = split[0].to_lowercase();
            let last_node_id = split[1].parse::<u64>().map_err(|e| {
                BenchmarkError::InvalidMessage(format!(
                    "String to ClientConf error: '{}' does not represent a node id: {:?}",
                    split[1], e
                ))
            })?;
            let reconfig_policy = split[2].to_lowercase();
            Ok(ClientParams::with(algorithm, last_node_id, reconfig_policy))
        }
    }

    fn str_to_client_data(str: String) -> Result<Self::ClientData, BenchmarkError> {
        let res = ActorPath::from_str(&str);
        res.map_err(|e| {
            BenchmarkError::InvalidMessage(format!("Could not read client data: {}", e))
        })
    }

    fn client_conf_to_str(c: Self::ClientConf) -> String {
        let s = format!("{},{},{}", c.algorithm, c.last_node_id, c.reconfig_policy);
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
            let reconfig: Vec<u64> = (2..=n+1).collect();
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
type Storage = MemStorage;

pub struct AtomicBroadcastMaster {
    algorithm: Option<String>,
    num_nodes: Option<u64>,
    num_proposals: Option<u64>,
    concurrent_proposals: Option<u64>,
    reconfiguration: Option<(Vec<u64>, Vec<u64>)>,
    system: Option<KompactSystem>,
    finished_latch: Option<Arc<CountdownEvent>>,
    iteration_id: u32,
    client_comp: Option<Arc<Component<Client>>>,
    partitioning_actor: Option<Arc<Component<PartitioningActor>>>,
    num_written_latency: usize,   // used to calculate avg of median latency
    latency_hist: Option<Histogram<u64>>,
    run_id: u64,
}

impl AtomicBroadcastMaster {
    fn new() -> AtomicBroadcastMaster {
        AtomicBroadcastMaster {
            algorithm: None,
            num_nodes: None,
            num_proposals: None,
            concurrent_proposals: None,
            reconfiguration: None,
            system: None,
            finished_latch: None,
            iteration_id: 0,
            client_comp: None,
            partitioning_actor: None,
            num_written_latency: 0,
            latency_hist: None,
            run_id: 0,
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
        reconfig: Option<(Vec<u64>, Vec<u64>)>,
        leader_election_latch: Arc<CountdownEvent>,
    ) -> (Arc<Component<Client>>, ActorPath) {
        let system = self.system.as_ref().unwrap();
        let finished_latch = self.finished_latch.clone().unwrap();
        let retry_after_reconfig = match self.algorithm {
            Some(ref p) if p == "paxos" => true,
            _ => false,
        };
        /*** Setup client ***/
        let (client_comp, unique_reg_f) = system.create_and_register( || {
            Client::with(
                self.num_proposals.unwrap(),
                self.concurrent_proposals.unwrap(),
                nodes_id,
                reconfig,
                PROPOSAL_TIMEOUT,
                leader_election_latch,
                finished_latch,
                retry_after_reconfig
            )
        });
        unique_reg_f.wait_expect(
            REGISTER_TIMEOUT,
            "Client failed to register!",
        );
        let client_comp_f = system.start_notify(&client_comp);
        client_comp_f
            .wait_timeout(REGISTER_TIMEOUT)
            .expect("ClientComp never started!");
        let named_reg_f = system.register_by_alias(
            &client_comp,
            format!("client{}", &self.iteration_id),
        );
        named_reg_f.wait_expect(
            REGISTER_TIMEOUT,
            "Failed to register alias for ClientComp"
        );
        let client_path = ActorPath::Named(NamedPath::with_system(
            system.system_path(),
            vec![format!("client{}", &self.iteration_id).into()],
        ));
        (client_comp, client_path)
    }

    fn validate_experiment_params(&mut self, c: &AtomicBroadcastRequest, num_clients: u32) -> Result<(), BenchmarkError> {  // TODO reconfiguration
        if c.concurrent_proposals > c.number_of_proposals {
            return Err(BenchmarkError::InvalidTest(
                format!("Concurrent proposals: {} should be less or equal to number of proposals: {}", c.concurrent_proposals, c.number_of_proposals)
            ));
        }
        match c.algorithm.to_lowercase().as_ref() {
            "paxos" => {
                match c.reconfiguration.to_lowercase().as_ref() {
                    "off" => {
                        if c.reconfig_policy.to_lowercase() != "none" {
                            return Err(BenchmarkError::InvalidTest(
                                format!("Reconfiguration is off, transfer policy should be none, but found: {}", &c.reconfig_policy)
                            ));
                        }
                    },
                    s if s == "single" || s == "majority" => {
                        let reconfig_policy: &str = &c.reconfig_policy.to_lowercase();
                        if reconfig_policy != "eager" && reconfig_policy != "pull" {
                            return Err(BenchmarkError::InvalidTest(
                                format!("Unimplemented Paxos transfer policy: {}", &c.reconfig_policy)
                            ));
                        }
                    },
                    _ => {
                        return Err(BenchmarkError::InvalidTest(
                            format!("Unimplemented Paxos reconfiguration: {}", &c.reconfiguration)
                        ));
                    }
                }
            },
            raft if raft == "raft-nobatch" || raft == "raft-batch" => {
                match c.reconfiguration.to_lowercase().as_ref() {
                    "off" => {
                        if c.reconfig_policy.to_lowercase() != "none" {
                            return Err(BenchmarkError::InvalidTest(
                                format!("Reconfiguration is off, transfer policy should be none, but found: {}", &c.reconfig_policy)
                            ));
                        }
                    },
                    s if s == "single" || s == "majority" => {
                        let reconfig_policy: &str = &c.reconfig_policy.to_lowercase();
                        if reconfig_policy != "remove-leader" && reconfig_policy != "remove-follower" && reconfig_policy != "joint-consensus-remove-leader" && reconfig_policy != "joint-consensus-remove-follower" {
                            return Err(BenchmarkError::InvalidTest(
                                format!("Unimplemented Raft transfer policy: {}", &c.reconfig_policy)
                            ));
                        }
                    },
                    _ => {
                        return Err(BenchmarkError::InvalidTest(
                            format!("Unimplemented Raft reconfiguration: {}", &c.reconfiguration)
                        ));
                    }
                }
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
                if (num_clients as u64) < n {
                    return Err(BenchmarkError::InvalidTest(format!(
                        "Not enough clients: {}, Required: {}",
                        num_clients,
                        n
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
        self.validate_experiment_params(&c, m.number_of_clients())?;
        self.algorithm = Some(c.algorithm.clone());
        self.num_nodes = Some(c.number_of_nodes);
        self.num_proposals = Some(c.number_of_proposals);
        self.concurrent_proposals = Some(c.concurrent_proposals);
        if c.concurrent_proposals == 1 {
            self.latency_hist = Some(Histogram::<u64>::new(4).expect("Failed to create latency histogram"));
            self.run_id = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
        }
        let system = crate::kompact_system_provider::global().new_remote_system_with_threads("atomicbroadcast", 4);
        self.system = Some(system);
        let params = ClientParams::with(
            c.algorithm,
            c.number_of_nodes,
            c.reconfig_policy,
        );
        Ok(params)
    }

    fn prepare_iteration(&mut self, d: Vec<Self::ClientData>) -> () {
        println!("Preparing iteration");
        if self.system.is_none() { panic!("No KompactSystem found!") }
        let finished_latch = Arc::new(CountdownEvent::new(1));
        self.finished_latch = Some(finished_latch);
        self.iteration_id += 1;
        let mut nodes_id: HashMap<u64, ActorPath> = HashMap::new();
        for (id, actorpath) in d.iter().enumerate() {
            nodes_id.insert(id as u64 + 1, actorpath.clone());
        }
        let leader_election_latch = Arc::new(CountdownEvent::new(1));
        let (client_comp, client_path) = self.create_client(nodes_id, self.reconfiguration.clone(), leader_election_latch.clone());
        let partitioning_actor = self.initialise_iteration(d, client_path);
        partitioning_actor.actor_ref().tell(IterationControlMsg::Run);
        leader_election_latch.wait();   // wait until leader is established
        self.partitioning_actor = Some(partitioning_actor);
        self.client_comp = Some(client_comp);
    }

    fn run_iteration(&mut self) -> () {
        println!("Running Atomic Broadcast experiment!");
        match self.client_comp {
            Some(ref client_comp) => {
                client_comp.actor_ref().tell(LocalClientMessage::Run);
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
        if let Some(1) = self.concurrent_proposals  {
            let dir = format!("{}/run-{}/", LATENCY_DIR, self.run_id);
            create_dir_all(&dir).unwrap_or_else(|_| panic!("Failed to create given directory: {}", &dir));
            let mut file = OpenOptions::new().create(true).append(true).open(format!("{}raw_{}_{}.data", &dir, self.algorithm.as_ref().unwrap(), self.num_proposals.as_ref().unwrap())).expect("Failed to open latency file");
            let latencies = client
                .actor_ref()
                .ask( |promise| LocalClientMessage::WriteLatencyFile(Ask::new(promise, ())))
                .wait();
            let histo = self.latency_hist.as_mut().unwrap();
            for (_, l) in latencies {
                let latency = l.as_micros() as u64;
                writeln!(file, "{}", latency).expect("Failed to write raw latency");
                histo.record(latency).expect("Failed to record histogram");
            }
            file.flush().expect("Failed to flush raw latency file");
        }

        let kill_client_f = system.kill_notify(client);
        kill_client_f
            .wait_timeout(REGISTER_TIMEOUT)
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
                .wait_timeout(REGISTER_TIMEOUT)
                .expect("Partitioning Actor never died!");
        }

        if last_iteration {
            println!("Cleaning up last iteration");
            if let Some(1) = self.concurrent_proposals {
                let dir = format!("{}/run-{}/", LATENCY_DIR, self.run_id);
                create_dir_all(&dir).unwrap_or_else(|_| panic!("Failed to create given directory: {}", dir));
                let mut file = OpenOptions::new().create(true).append(true).open(format!("{}summary_{}_{}.out", &dir, self.algorithm.as_ref().unwrap(), self.num_proposals.as_ref().unwrap())).expect("Failed to open latency file");
                let hist = std::mem::take(&mut self.latency_hist).unwrap();
                let quantiles = [0.001, 0.01, 0.005, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999];
                for q in &quantiles {
                    writeln!(file, "Value at quantile {}: {} micro s", q, hist.value_at_quantile(*q)).expect("Failed to write latency file");
                }
                let max = hist.max();
                writeln!(
                    file,
                    "Min: {} micro s, Max: {} micro s, Average: {} micro s",
                    hist.min(),
                    max,
                    hist.mean()
                ).expect("Failed to write histogram summary");
                writeln!(file, "Total elements: {}", hist.len()).expect("Failed to write histogram summary");
                file.flush().expect("Failed to flush histogram file");
            }
            self.algorithm = None;
            self.num_nodes = None;
            self.reconfiguration = None;
            self.concurrent_proposals = None;
            self.num_proposals = None;
            self.num_written_latency = 0;
            self.iteration_id = 0;
            system
                .shutdown()
                .expect("Kompact didn't shut down properly");
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
        let named_path = match c.algorithm.as_ref() {
            "paxos" => {
                let initial_config = get_initial_conf(c.last_node_id).0;
                let reconfig_policy = match c.reconfig_policy.as_ref() {
                    "none" => None,
                    "eager" => Some(PaxosReconfigurationPolicy::Eager),
                    "pull" => Some(PaxosReconfigurationPolicy::Pull),
                    unknown => panic!("Got unknown Paxos transfer policy: {}", unknown),
                };
                let (paxos_replica, unique_reg_f) = system.create_and_register(|| {
                    PaxosReplica::with(initial_config, reconfig_policy.unwrap_or(PaxosReconfigurationPolicy::Pull))
                });
                unique_reg_f.wait_expect(
                    REGISTER_TIMEOUT,
                    "ReplicaComp failed to register!",
                );
                let named_reg_f = system.register_by_alias(
                    &paxos_replica,
                    PAXOS_PATH,
                );
                named_reg_f.wait_expect(
                    REGISTER_TIMEOUT,
                    "Failed to register alias for ReplicaComp"
                );
                let paxos_replica_f = system.start_notify(&paxos_replica);
                paxos_replica_f
                    .wait_timeout(REGISTER_TIMEOUT)
                    .expect("ReplicaComp never started!");
                let self_path = ActorPath::Named(NamedPath::with_system(
                    system.system_path(),
                    vec![PAXOS_PATH.into()],
                ));

                self.paxos_replica = Some(paxos_replica);
                self_path
            }
            raft if raft == "raft-nobatch" || raft == "raft-batch" => {
                let batch = if raft == "raft-batch" { true } else { false };
                let conf_state = get_initial_conf(c.last_node_id);
                let reconfig_policy = match c.reconfig_policy.as_ref() {
                    "none" => None,
                    "joint-consensus-remove-leader" => Some(RaftReconfigurationPolicy::JointConsensusRemoveLeader),
                    "joint-consensus-remove-follower" => Some(RaftReconfigurationPolicy::JointConsensusRemoveFollower),
                    "remove-leader" => Some(RaftReconfigurationPolicy::RemoveLeader),
                    "remove-follower" => Some(RaftReconfigurationPolicy::RemoveFollower),
                    unknown => panic!("Got unknown Raft transfer policy: {}", unknown),
                };
//                let storage = MemStorage::new_with_conf_state(conf_state);
//                let dir = "./diskstorage";
//                let storage = DiskStorage::new_with_conf_state(dir, conf_state);
                /*** Setup RaftComp ***/
                let (raft_replica, unique_reg_f) = system.create_and_register(|| {
                    RaftReplica::<Storage>::with(
                        conf_state.0,
                        reconfig_policy.unwrap_or(RaftReconfigurationPolicy::RemoveFollower),
                        batch
                    )
                });
                unique_reg_f.wait_expect(
                    REGISTER_TIMEOUT,
                    "RaftComp failed to register!",
                );
                let named_reg_f = system.register_by_alias(
                    &raft_replica,
                    RAFT_PATH,
                );
                named_reg_f.wait_expect(
                    REGISTER_TIMEOUT,
                    "Communicator failed to register!",
                );
                let raft_replica_f = system.start_notify(&raft_replica);
                raft_replica_f
                    .wait_timeout(REGISTER_TIMEOUT)
                    .expect("RaftComp never started!");
                let self_path = ActorPath::Named(NamedPath::with_system(
                    system.system_path(),
                    vec![RAFT_PATH.into()],
                ));

                self.raft_replica = Some(raft_replica);
                self_path
            },
            unknown => panic!("Got unknown algorithm: {}", unknown),
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
                    .wait_timeout(REGISTER_TIMEOUT)
                    .expect("Paxos Replica never died!");
            }
            if let Some(raft_replica) = self.raft_replica.take() {
                let kill_raft_f = system.kill_notify(raft_replica);
                kill_raft_f
                    .wait_timeout(REGISTER_TIMEOUT)
                    .expect("Raft Replica never died!");
            }
            system
                .shutdown()
                .expect("Kompact didn't shut down properly");
        }
    }
}
