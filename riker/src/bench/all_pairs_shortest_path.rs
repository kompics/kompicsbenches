use super::*;

use crate::riker_system_provider::*;
use benchmark_suite_shared::helpers::graphs::*;
use benchmark_suite_shared::kompics_benchmarks::benchmarks::APSPRequest;
use log::{debug, info};
use riker::actors::*;
use std::collections::BTreeMap;
use std::convert::TryInto;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;
use synchronoise::CountdownEvent;

#[derive(Default)]
pub struct AllPairsShortestPath;

impl Benchmark for AllPairsShortestPath {
    type Conf = APSPRequest;
    type Instance = AllPairsShortestPathI;

    fn msg_to_conf(msg: Box<dyn (::protobuf::Message)>) -> Result<Self::Conf, BenchmarkError> {
        downcast_msg!(msg; APSPRequest)
    }

    fn new_instance() -> Self::Instance {
        AllPairsShortestPathI::new()
    }

    const LABEL: &'static str = "AllPairsShortestPath";
}

pub struct AllPairsShortestPathI {
    num_nodes: Option<usize>,
    block_size: Option<usize>,
    system: Option<RikerSystem>,
    manager: Option<ActorRef<ManagerMsg>>,
    graph: Option<Arc<Graph<f64>>>,
    latch: Option<Arc<CountdownEvent>>,
}

impl AllPairsShortestPathI {
    fn new() -> AllPairsShortestPathI {
        AllPairsShortestPathI {
            num_nodes: None,
            block_size: None,
            system: None,
            manager: None,
            graph: None,
            latch: None,
        }
    }
}

impl BenchmarkInstance for AllPairsShortestPathI {
    type Conf = APSPRequest;

    fn setup(&mut self, c: &Self::Conf) -> () {
        let num_nodes: usize = c
            .number_of_nodes
            .try_into()
            .expect("Nodes should fit into usize");
        self.num_nodes = Some(num_nodes);
        let block_size: usize = c
            .block_size
            .try_into()
            .expect("Block Size should fit into usize");
        self.block_size = Some(block_size);
        let system = RikerSystem::new("apsp", num_cpus::get()).expect("System");
        let manager = system
            .start(ManagerActor::props(block_size), "apsp-manager")
            .expect("ManagerActor never started!");
        self.manager = Some(manager);
        self.system = Some(system);
        let graph = generate_graph(num_nodes);
        self.graph = Some(Arc::new(graph));
    }

    fn prepare_iteration(&mut self) -> () {
        self.latch = Some(Arc::new(CountdownEvent::new(1)));
    }

    fn run_iteration(&mut self) -> () {
        if let Some(ref manager) = self.manager {
            if let Some(ref graph) = self.graph {
                let latch = self.latch.take().unwrap();
                manager.tell(
                    ManagerMsg::ComputeFW {
                        graph: graph.clone(),
                        latch: latch.clone(),
                    },
                    None,
                );
                latch.wait();
            } else {
                unimplemented!();
            }
        } else {
            unimplemented!();
        }
    }

    fn cleanup_iteration(&mut self, last_iteration: bool, _exec_time_millis: f64) -> () {
        let system = self.system.take().unwrap();

        if last_iteration {
            if let Some(manager) = self.manager.take() {
                system.stop(manager);
            }
            self.graph = None;

            system
                .shutdown()
                .wait()
                .expect("Riker didn't shut down properly");
            self.num_nodes = None;
            self.block_size = None;
        } else {
            self.system = Some(system);
        }
    }
}

#[derive(Clone)]
enum ManagerMsg {
    ComputeFW {
        graph: Arc<Graph<f64>>,
        latch: Arc<CountdownEvent>,
    },
    BlockResult {
        block: Arc<Block<f64>>,
    },
}
impl fmt::Debug for ManagerMsg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ManagerMsg::ComputeFW { .. } => write!(f, "ComputeFW{{..}}"),
            ManagerMsg::BlockResult { .. } => write!(f, "BlockResult{{..}}"),
        }
    }
}

// used to break trait bound resolution cycle of Message
#[derive(Debug, Clone)]
struct Neighbours(Vec<ActorRef<BlockMsg>>);
// definitely safe, the compiler just endlessly recurses while trying to figure this out
unsafe impl Send for Neighbours {}
impl Deref for Neighbours {
    type Target = Vec<ActorRef<BlockMsg>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl From<Vec<ActorRef<BlockMsg>>> for Neighbours {
    fn from(actors: Vec<ActorRef<BlockMsg>>) -> Self {
        Neighbours(actors)
    }
}

#[derive(Clone)]
enum BlockMsg {
    Neighbours(Neighbours),
    PhaseResult { k: Phase, data: Arc<Block<f64>> },
}
impl fmt::Debug for BlockMsg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BlockMsg::Neighbours(_) => write!(f, "Neighbours(..)"),
            BlockMsg::PhaseResult { k, .. } => write!(f, "PhaseResult{{k={},..}}", k),
        }
    }
}

struct ManagerActor {
    block_size: usize,
    block_workers: Vec<ActorRef<BlockMsg>>,
    latch: Option<Arc<CountdownEvent>>,
    assembly: Option<Vec<Vec<Arc<Block<f64>>>>>,
    missing_blocks: Option<usize>,
    #[cfg(test)]
    result: Option<Graph<f64>>,
}

impl ManagerActor {
    fn with(block_size: usize) -> ManagerActor {
        ManagerActor {
            block_size,
            block_workers: Vec::new(),
            latch: None,
            assembly: None,
            missing_blocks: None,
            #[cfg(test)]
            result: None,
        }
    }

    fn props(block_size: usize) -> BoxActorProd<ManagerActor> {
        Props::new_from(move || ManagerActor::with(block_size))
    }

    fn distribute_graph(&mut self, graph: &Graph<f64>, ctx: &Context<ManagerMsg>) {
        let mut blocks_owned = graph.break_into_blocks(self.block_size);
        let blocks: Vec<Vec<Arc<Block<f64>>>> = blocks_owned
            .drain(..)
            .map(|mut row| row.drain(..).map(|block| Arc::new(block)).collect())
            .collect();
        let num_nodes = graph.num_nodes();
        let self_ref = ctx.myself();
        let mut block_actors: Vec<Vec<ActorRef<BlockMsg>>> = blocks
            .iter()
            .map(|row| {
                row.iter()
                    .map(|block| {
                        ctx.actor_of_props(
                            &format!("block-actor-{}", block.block_id()),
                            BlockActor::props(block.clone(), num_nodes, self_ref.clone()),
                        )
                        .expect("BlockActor should have started")
                    })
                    .collect()
            })
            .collect();
        let num_neighbours = blocks.len() * 2 - 2;
        for bi in 0usize..blocks.len() {
            for bj in 0usize..blocks.len() {
                let mut neighbours: Vec<ActorRef<BlockMsg>> = Vec::with_capacity(num_neighbours);
                // add neighbours in the same row
                for r in 0usize..blocks.len() {
                    if r != bi {
                        neighbours.push(block_actors[r][bj].clone());
                    }
                }
                // add neighbours in the same column
                for c in 0usize..blocks.len() {
                    if c != bj {
                        neighbours.push(block_actors[bi][c].clone());
                    }
                }
                block_actors[bi][bj].tell(BlockMsg::Neighbours(neighbours.into()), None);
            }
        }
        self.block_workers = block_actors.drain(..).flatten().collect();
        self.missing_blocks = Some(self.block_workers.len());
        self.assembly = Some(blocks); // initialise with unfinished blocks
    }
}

impl Actor for ManagerActor {
    type Msg = ManagerMsg;

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, _sender: Sender) {
        match msg {
            ManagerMsg::ComputeFW { graph, latch } => {
                debug!("Got ComputeFW!");

                #[cfg(test)]
                {
                    let mut my_graph = (*graph).clone();
                    my_graph.compute_floyd_warshall();
                    self.result = Some(my_graph);
                }
                self.distribute_graph(graph.as_ref(), ctx);
                self.latch = Some(latch);
            }
            ManagerMsg::BlockResult { block } => {
                if let Some(ref mut missing_blocks) = self.missing_blocks {
                    if let Some(ref mut blocks) = self.assembly {
                        let (i, j) = block.block_position();
                        blocks[i][j] = block;
                        *missing_blocks -= 1usize;
                    } else {
                        unimplemented!();
                    }
                    if *missing_blocks == 0usize {
                        let mut blocks = self.assembly.take().unwrap();
                        let blocks_owned: Vec<Vec<Block<f64>>> = blocks
                            .drain(..)
                            .map(|mut row| {
                                row.drain(..)
                                    .map(|block| {
                                        match Arc::try_unwrap(block) {
                                            Ok(block_owned) => block_owned,
                                            Err(block_shared) => (*block_shared).clone(), // still in use :(
                                        }
                                    })
                                    .collect()
                            })
                            .collect();
                        #[allow(unused_variables)] // used during test
                        let result = Graph::assemble_from_blocks(blocks_owned);
                        self.latch
                            .take()
                            .unwrap()
                            .decrement()
                            .expect("Should decrement!");
                        info!("Done!");
                        // cleanup
                        self.block_workers.clear(); // they shutdown themselves
                        self.missing_blocks = None;
                        #[cfg(test)]
                        assert_eq!(self.result.take().unwrap(), result, "Wrong APSP result!");
                    } else {
                        debug!("Got another {} blocks outstanding.", missing_blocks);
                    }
                } else {
                    unimplemented!();
                }
            }
        }
    }
}

struct BlockActor {
    num_nodes: usize,
    manager: ActorRef<ManagerMsg>,
    ready: bool,
    neighbours: Vec<ActorRef<BlockMsg>>,
    k: Phase,
    neighbour_data: BTreeMap<usize, Arc<Block<f64>>>,
    next_neighbour_data: BTreeMap<usize, Arc<Block<f64>>>,
    current_data: Arc<Block<f64>>,
    done: bool,
}

impl BlockActor {
    fn with(
        initial_block: Arc<Block<f64>>,
        num_nodes: usize,
        manager: ActorRef<ManagerMsg>,
    ) -> BlockActor {
        BlockActor {
            num_nodes,
            manager,
            ready: false,
            neighbours: Vec::new(),
            k: Phase::default(),
            neighbour_data: BTreeMap::new(),
            next_neighbour_data: BTreeMap::new(),
            current_data: initial_block,
            done: false,
        }
    }

    fn props(
        initial_block: Arc<Block<f64>>,
        num_nodes: usize,
        manager: ActorRef<ManagerMsg>,
    ) -> BoxActorProd<BlockActor> {
        Props::new_from(move || BlockActor::with(initial_block.clone(), num_nodes, manager.clone()))
    }

    fn notify_neighbours(&self) -> () {
        assert!(self.ready);
        let msg = BlockMsg::PhaseResult {
            k: self.k,
            data: self.current_data.clone(),
        };
        for neighbour in self.neighbours.iter() {
            neighbour.tell(msg.clone(), None);
        }
    }

    fn perform_computation(&mut self) -> () {
        let mut neighbour_data = BTreeMap::new();
        std::mem::swap(&mut self.neighbour_data, &mut neighbour_data);
        let lookup_fun = |block_id: usize| {
            neighbour_data
                .get(&block_id)
                .expect("Block id should have been in neighbour data")
                .as_ref()
        };
        if let Some(ref mut current_data) = Arc::get_mut(&mut self.current_data) {
            current_data.compute_floyd_warshall_inner(&lookup_fun, self.k.k());
        } else {
            // our data is still in use somewhere...gotta clone :(
            let mut current_data = (*self.current_data).clone();
            current_data.compute_floyd_warshall_inner(&lookup_fun, self.k.k());
            self.current_data = Arc::new(current_data);
        }
    }
}

impl Actor for BlockActor {
    type Msg = BlockMsg;

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, _sender: Sender) {
        match msg {
            BlockMsg::Neighbours(nodes) => {
                debug!(
                    "Got {} neighbours at block_id={}",
                    nodes.len(),
                    self.current_data.block_id()
                );
                self.neighbours = nodes.0;
                self.ready = true;
                self.notify_neighbours();
            }
            res @ BlockMsg::PhaseResult { .. } if !self.ready => {
                ctx.myself().tell(res, None); // queue msg until ready
            }
            BlockMsg::PhaseResult { k: other_k, data } => {
                if !self.done {
                    if other_k == self.k {
                        debug!(
                            "Got PhaseResult(k={}) at block_id={}",
                            other_k,
                            self.current_data.block_id()
                        );

                        self.neighbour_data.insert(data.block_id(), data);
                        while self.neighbour_data.len() == self.neighbours.len() {
                            self.k.inc();
                            self.perform_computation();
                            self.notify_neighbours();
                            std::mem::swap(&mut self.neighbour_data, &mut self.next_neighbour_data);
                            // self.next_neighbour_data.clear(); has been emptied in perform_computation
                            if self.k == self.num_nodes - 1usize {
                                self.manager.tell(
                                    ManagerMsg::BlockResult {
                                        block: self.current_data.clone(),
                                    },
                                    None,
                                );
                                ctx.stop(ctx.myself());
                                self.done = true;
                                return; // don't run the whole loop again
                            }
                        }
                    } else if other_k == self.k.incremented() {
                        self.next_neighbour_data.insert(data.block_id(), data);
                    } else {
                        unimplemented!("Got k={}, but I'm in phase k={}", other_k, self.k);
                    }
                }
            }
        }
    }
}
