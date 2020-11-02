use actix::*;
use actix_system_provider::{ActixSystem, PoisonPill};
use benchmark_suite_shared::helpers::graphs::*;
use benchmark_suite_shared::kompics_benchmarks::benchmarks::APSPRequest;
use std::collections::BTreeMap;
use std::convert::TryInto;
use std::sync::Arc;
use synchronoise::CountdownEvent;

use super::*;

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
    system: Option<ActixSystem>,
    manager: Option<Addr<ManagerActor>>,
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
        let mut system = crate::actix_system_provider::new_system("apsp");
        let manager = ManagerActor::with(block_size).start();
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
                manager.do_send(ComputeFW {
                    graph: graph.clone(),
                    latch: latch.clone(),
                });
                latch.wait();
            } else {
                unimplemented!();
            }
        } else {
            unimplemented!();
        }
    }

    fn cleanup_iteration(&mut self, last_iteration: bool, _exec_time_millis: f64) -> () {
        let mut system = self.system.take().unwrap();

        if last_iteration {
            if let Some(manager) = self.manager.take() {
                system.stop(manager);
            };
            self.graph = None;

            system.shutdown().expect("Actix didn't shut down properly");
            self.num_nodes = None;
            self.block_size = None;
        } else {
            self.system = Some(system);
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct ComputeFW {
    graph: Arc<Graph<f64>>,
    latch: Arc<CountdownEvent>,
}
#[derive(Message)]
#[rtype(result = "()")]
struct BlockResult {
    block: Arc<Block<f64>>,
}

// impl fmt::Debug for ManagerMsg {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         match self {
//             ManagerMsg::ComputeFW { .. } => write!(f, "ComputeFW{{..}}"),
//             ManagerMsg::BlockResult { .. } => write!(f, "BlockResult{{..}}"),
//         }
//     }
// }

#[derive(Message)]
#[rtype(result = "()")]
struct Neighbours(Vec<Recipient<PhaseResult>>);

#[derive(Message, Clone)]
#[rtype(result = "()")]
struct PhaseResult {
    k: Phase,
    data: Arc<Block<f64>>,
}

// impl fmt::Debug for BlockMsg {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         match self {
//             BlockMsg::Neighbours(_) => write!(f, "Neighbours(..)"),
//             BlockMsg::PhaseResult { k, .. } => write!(f, "PhaseResult{{k={},..}}", k),
//         }
//     }
// }

struct ManagerActor {
    block_size: usize,
    block_workers: Vec<Addr<BlockActor>>,
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

    fn distribute_graph(&mut self, graph: &Graph<f64>, self_addr: Addr<Self>) {
        let mut blocks_owned = graph.break_into_blocks(self.block_size);
        let blocks: Vec<Vec<Arc<Block<f64>>>> = blocks_owned
            .drain(..)
            .map(|mut row| row.drain(..).map(|block| Arc::new(block)).collect())
            .collect();
        let num_nodes = graph.num_nodes();
        let self_rec: Recipient<BlockResult> = self_addr.recipient();
        let mut block_actors: Vec<Vec<Addr<BlockActor>>> = blocks
            .iter()
            .map(|row| {
                row.iter()
                    .map(|block| {
                        BlockActor::with(block.clone(), num_nodes, self_rec.clone()).start()
                    })
                    .collect()
            })
            .collect();
        let num_neighbours = blocks.len() * 2 - 2;
        for bi in 0usize..blocks.len() {
            for bj in 0usize..blocks.len() {
                let mut neighbours: Vec<Recipient<PhaseResult>> =
                    Vec::with_capacity(num_neighbours);
                // add neighbours in the same row
                for r in 0usize..blocks.len() {
                    if r != bi {
                        neighbours.push(block_actors[r][bj].clone().recipient());
                    }
                }
                // add neighbours in the same column
                for c in 0usize..blocks.len() {
                    if c != bj {
                        neighbours.push(block_actors[bi][c].clone().recipient());
                    }
                }
                block_actors[bi][bj].do_send(Neighbours(neighbours));
            }
        }
        self.block_workers = block_actors.drain(..).flatten().collect();
        self.missing_blocks = Some(self.block_workers.len());
        self.assembly = Some(blocks); // initialise with unfinished blocks
    }
}

impl Actor for ManagerActor {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        // nothing
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        // nothing
    }
}

impl Handler<PoisonPill> for ManagerActor {
    type Result = ();

    fn handle(&mut self, _msg: PoisonPill, ctx: &mut Context<Self>) -> Self::Result {
        //println!("PoisonPill received, shutting down.");
        ctx.stop();
    }
}

impl Handler<ComputeFW> for ManagerActor {
    type Result = ();

    fn handle(&mut self, msg: ComputeFW, ctx: &mut Context<Self>) -> Self::Result {
        let ComputeFW { graph, latch } = msg;
        println!("Got ComputeFW!");

        #[cfg(test)]
        {
            let mut my_graph = (*graph).clone();
            my_graph.compute_floyd_warshall();
            self.result = Some(my_graph);
        }
        self.distribute_graph(graph.as_ref(), ctx.address());
        self.latch = Some(latch);
    }
}

impl Handler<BlockResult> for ManagerActor {
    type Result = ();

    fn handle(&mut self, msg: BlockResult, _ctx: &mut Context<Self>) -> Self::Result {
        let block = msg.block;
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
                println!("APSP Manager Done!");
                // cleanup
                self.block_workers.clear(); // they shutdown themselves
                self.missing_blocks = None;
                #[cfg(test)]
                assert_eq!(self.result.take().unwrap(), result, "Wrong APSP result!");
            } else {
                // debug!(
                //     self.ctx.log(),
                //     "Got another {} blocks outstanding.", missing_blocks
                // );
            }
        } else {
            unimplemented!();
        }
    }
}

struct BlockActor {
    num_nodes: usize,
    manager: Recipient<BlockResult>,
    ready: bool,
    neighbours: Vec<Recipient<PhaseResult>>,
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
        manager: Recipient<BlockResult>,
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

    fn notify_neighbours(&self) -> () {
        assert!(self.ready);
        let msg = PhaseResult {
            k: self.k,
            data: self.current_data.clone(),
        };
        for neighbour in self.neighbours.iter() {
            let res = neighbour.do_send(msg.clone());
            if res.is_err() { // this can happen during the last phase
                #[cfg(test)]
                eprintln!("Could not update neighbour!");
            }
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
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        // nothing
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        // nothing
    }
}

impl Handler<Neighbours> for BlockActor {
    type Result = ();

    fn handle(&mut self, msg: Neighbours, _ctx: &mut Context<Self>) -> Self::Result {
        let nodes = msg.0;
        // debug!(
        //             self.ctx.log(),
        //             "Got {} neighbours at block_id={}",
        //             nodes.len(),
        //             self.current_data.block_id()
        //         );
        self.neighbours = nodes;
        self.ready = true;
        self.notify_neighbours();
    }
}

impl Handler<PhaseResult> for BlockActor {
    type Result = ();

    fn handle(&mut self, msg: PhaseResult, ctx: &mut Context<Self>) -> Self::Result {
        if !self.ready {
            ctx.notify(msg); // queue msg until ready
        } else {
            let PhaseResult { k: other_k, data } = msg;
            if !self.done {
                if other_k == self.k {
                    // debug!(
                    //     self.ctx.log(),
                    //     "Got PhaseResult(k={}) at block_id={}",
                    //     other_k,
                    //     self.current_data.block_id()
                    // );

                    self.neighbour_data.insert(data.block_id(), data);
                    while self.neighbour_data.len() == self.neighbours.len() {
                        self.k.inc();
                        self.perform_computation();
                        self.notify_neighbours();
                        std::mem::swap(&mut self.neighbour_data, &mut self.next_neighbour_data);
                        // self.next_neighbour_data.clear(); has been emptied in perform_computation
                        if self.k == self.num_nodes - 1usize {
                            self.manager
                                .do_send(BlockResult {
                                    block: self.current_data.clone(),
                                })
                                .expect("Should send");
                            ctx.stop();
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
