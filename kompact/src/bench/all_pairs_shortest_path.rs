use super::*;

use benchmark_suite_shared::helpers::graphs::*;
use benchmark_suite_shared::kompics_benchmarks::benchmarks::APSPRequest;
use kompact::prelude::*;
use std::collections::BTreeMap;
use std::convert::TryInto;
use std::fmt;
use std::sync::{Arc, Weak};
use synchronoise::CountdownEvent;

pub mod actor_apsp {
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
        system: Option<KompactSystem>,
        manager: Option<Arc<Component<ManagerActor>>>,
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
            let system = crate::kompact_system_provider::global().new_system("apsp");
            let manager = system.create(|| ManagerActor::with(block_size));
            let manager_f = system.start_notify(&manager);
            manager_f
                .wait_timeout(Duration::from_millis(1000))
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
                    manager.actor_ref().tell(ManagerMsg::ComputeFW {
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
            let system = self.system.take().unwrap();

            if last_iteration {
                if let Some(manager) = self.manager.take() {
                    let f = system.kill_notify(manager);
                    f.wait_timeout(Duration::from_millis(1000))
                        .expect("Manager never died!");
                }
                self.graph = None;

                system
                    .shutdown()
                    .expect("Kompics didn't shut down properly");
                self.num_nodes = None;
                self.block_size = None;
            } else {
                self.system = Some(system);
            }
        }
    }

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
    #[derive(Clone)]
    enum BlockMsg {
        Neighbours(Vec<ActorRef<BlockMsg>>),
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

    #[derive(ComponentDefinition)]
    struct ManagerActor {
        ctx: ComponentContext<Self>,
        block_size: usize,
        block_workers: Vec<Weak<Component<BlockActor>>>,
        latch: Option<Arc<CountdownEvent>>,
        assembly: Option<Vec<Vec<Arc<Block<f64>>>>>,
        missing_blocks: Option<usize>,
        #[cfg(test)]
        result: Option<Graph<f64>>,
    }

    impl ManagerActor {
        fn with(block_size: usize) -> ManagerActor {
            ManagerActor {
                ctx: ComponentContext::new(),
                block_size,
                block_workers: Vec::new(),
                latch: None,
                assembly: None,
                missing_blocks: None,
                #[cfg(test)]
                result: None,
            }
        }

        fn distribute_graph(&mut self, graph: &Graph<f64>) {
            let mut blocks_owned = graph.break_into_blocks(self.block_size);
            let blocks: Vec<Vec<Arc<Block<f64>>>> = blocks_owned
                .drain(..)
                .map(|mut row| row.drain(..).map(|block| Arc::new(block)).collect())
                .collect();
            let num_nodes = graph.num_nodes();
            let system = self.ctx.system();
            let self_ref = self.ctx.actor_ref().hold().expect("Live ref");
            let mut block_actors: Vec<Vec<Arc<Component<BlockActor>>>> = blocks
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|block| {
                            let block_actor = system.create(|| {
                                BlockActor::with(block.clone(), num_nodes, self_ref.clone())
                            });
                            system.start(&block_actor);
                            block_actor
                        })
                        .collect()
                })
                .collect();
            let num_neighbours = blocks.len() * 2 - 2;
            for bi in 0usize..blocks.len() {
                for bj in 0usize..blocks.len() {
                    let mut neighbours: Vec<ActorRef<BlockMsg>> =
                        Vec::with_capacity(num_neighbours);
                    // add neighbours in the same row
                    for r in 0usize..blocks.len() {
                        if r != bi {
                            neighbours.push(block_actors[r][bj].actor_ref());
                        }
                    }
                    // add neighbours in the same column
                    for c in 0usize..blocks.len() {
                        if c != bj {
                            neighbours.push(block_actors[bi][c].actor_ref());
                        }
                    }
                    block_actors[bi][bj]
                        .actor_ref()
                        .tell(BlockMsg::Neighbours(neighbours));
                }
            }
            self.block_workers = block_actors
                .drain(..)
                .flatten()
                .map(|ba| Arc::downgrade(&ba))
                .collect(); // don't prevent them from deallocation when they are done
            self.missing_blocks = Some(self.block_workers.len());
            self.assembly = Some(blocks); // initialise with unfinished blocks
        }
    }

    ignore_control!(ManagerActor);

    impl Actor for ManagerActor {
        type Message = ManagerMsg;

        fn receive_local(&mut self, msg: Self::Message) -> () {
            match msg {
                ManagerMsg::ComputeFW { graph, latch } => {
                    debug!(self.ctx.log(), "Got ComputeFW!");

                    #[cfg(test)]
                    {
                        let mut my_graph = (*graph).clone();
                        my_graph.compute_floyd_warshall();
                        self.result = Some(my_graph);
                    }
                    self.distribute_graph(graph.as_ref());
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
                            info!(self.ctx.log(), "Done!");
                            // cleanup
                            self.block_workers.clear(); // they shutdown themselves
                            self.missing_blocks = None;
                            #[cfg(test)]
                            assert_eq!(self.result.take().unwrap(), result, "Wrong APSP result!");
                        } else {
                            debug!(
                                self.ctx.log(),
                                "Got another {} blocks outstanding.", missing_blocks
                            );
                        }
                    } else {
                        unimplemented!();
                    }
                }
            }
        }

        fn receive_network(&mut self, _msg: NetMessage) -> () {
            unimplemented!();
        }
    }

    #[derive(ComponentDefinition)]
    struct BlockActor {
        ctx: ComponentContext<Self>,
        num_nodes: usize,
        manager: ActorRefStrong<ManagerMsg>,
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
            manager: ActorRefStrong<ManagerMsg>,
        ) -> BlockActor {
            BlockActor {
                ctx: ComponentContext::new(),
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
            let msg = BlockMsg::PhaseResult {
                k: self.k,
                data: self.current_data.clone(),
            };
            for neighbour in self.neighbours.iter() {
                neighbour.tell(msg.clone());
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

    ignore_control!(BlockActor);

    impl Actor for BlockActor {
        type Message = BlockMsg;

        fn receive_local(&mut self, msg: Self::Message) -> () {
            match msg {
                BlockMsg::Neighbours(nodes) => {
                    debug!(
                        self.ctx.log(),
                        "Got {} neighbours at block_id={}",
                        nodes.len(),
                        self.current_data.block_id()
                    );
                    self.neighbours = nodes;
                    self.ready = true;
                    self.notify_neighbours();
                }
                res @ BlockMsg::PhaseResult { .. } if !self.ready => {
                    self.ctx.actor_ref().tell(res); // queue msg until ready
                }
                BlockMsg::PhaseResult { k: other_k, data } => {
                    if !self.done {
                        if other_k == self.k {
                            debug!(
                                self.ctx.log(),
                                "Got PhaseResult(k={}) at block_id={}",
                                other_k,
                                self.current_data.block_id()
                            );

                            self.neighbour_data.insert(data.block_id(), data);
                            while self.neighbour_data.len() == self.neighbours.len() {
                                self.k.inc();
                                self.perform_computation();
                                self.notify_neighbours();
                                std::mem::swap(
                                    &mut self.neighbour_data,
                                    &mut self.next_neighbour_data,
                                );
                                // self.next_neighbour_data.clear(); has been emptied in perform_computation
                                if self.k == self.num_nodes - 1usize {
                                    self.manager.tell(ManagerMsg::BlockResult {
                                        block: self.current_data.clone(),
                                    });
                                    self.ctx.suicide();
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

        fn receive_network(&mut self, _msg: NetMessage) -> () {
            unimplemented!();
        }
    }
}

pub mod component_apsp {
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
        system: Option<KompactSystem>,
        manager: Option<Arc<Component<ManagerActor>>>,
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
            let system = crate::kompact_system_provider::global().new_system("apsp");
            let manager = system.create(|| ManagerActor::with(block_size));
            let manager_f = system.start_notify(&manager);
            manager_f
                .wait_timeout(Duration::from_millis(1000))
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
            if let Some(ref system) = self.system {
                if let Some(ref manager) = self.manager {
                    if let Some(ref graph) = self.graph {
                        let latch = self.latch.take().unwrap();
                        let msg = ManagerMsg::ComputeFW {
                            graph: graph.clone(),
                            latch: latch.clone(),
                        };
                        let manager_port: ProvidedRef<ManagerPort> = manager.provided_ref();
                        system.trigger_r(msg, &manager_port);
                        latch.wait();
                    } else {
                        unimplemented!();
                    }
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
                    let f = system.kill_notify(manager);
                    f.wait_timeout(Duration::from_millis(1000))
                        .expect("Manager never died!");
                }
                self.graph = None;

                system
                    .shutdown()
                    .expect("Kompics didn't shut down properly");
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
    struct ManagerPort;
    impl Port for ManagerPort {
        type Request = ManagerMsg;
        type Indication = Never;
    }
    #[derive(Clone)]
    struct PhaseResult {
        k: Phase,
        data: Arc<Block<f64>>,
    }
    impl fmt::Debug for PhaseResult {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "PhaseResult{{k={},block_id={}}}",
                self.k,
                self.data.block_id()
            )
        }
    }
    struct BlockPort;
    impl Port for BlockPort {
        type Indication = PhaseResult;
        type Request = Never;
    }

    #[derive(ComponentDefinition, Actor)]
    struct ManagerActor {
        ctx: ComponentContext<Self>,
        manager_port: ProvidedPort<ManagerPort, Self>,
        block_size: usize,
        block_workers: Vec<Weak<Component<BlockActor>>>,
        latch: Option<Arc<CountdownEvent>>,
        assembly: Option<Vec<Vec<Arc<Block<f64>>>>>,
        missing_blocks: Option<usize>,
        #[cfg(test)]
        result: Option<Graph<f64>>,
    }

    impl ManagerActor {
        fn with(block_size: usize) -> ManagerActor {
            ManagerActor {
                ctx: ComponentContext::new(),
                manager_port: ProvidedPort::new(),
                block_size,
                block_workers: Vec::new(),
                latch: None,
                assembly: None,
                missing_blocks: None,
                #[cfg(test)]
                result: None,
            }
        }

        fn distribute_graph(&mut self, graph: &Graph<f64>) {
            let mut blocks_owned = graph.break_into_blocks(self.block_size);
            let blocks: Vec<Vec<Arc<Block<f64>>>> = blocks_owned
                .drain(..)
                .map(|mut row| row.drain(..).map(|block| Arc::new(block)).collect())
                .collect();
            let num_nodes = graph.num_nodes();
            let system = self.ctx.system();
            let num_neighbours = blocks.len() * 2 - 2;
            let mut block_actors: Vec<Vec<Arc<Component<BlockActor>>>> = blocks
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|block| {
                            system.create(|| {
                                BlockActor::with(block.clone(), num_nodes, num_neighbours)
                            })
                        })
                        .collect()
                })
                .collect();
            let manager_port = self.manager_port.share();
            for bi in 0usize..blocks.len() {
                for bj in 0usize..blocks.len() {
                    let current = &block_actors[bi][bj];
                    let current_block_port: RequiredRef<BlockPort> = current.required_ref();
                    current.connect_to_provided(manager_port.clone());
                    // add neighbours in the same row
                    for r in 0usize..blocks.len() {
                        if r != bi {
                            let neighbour = &block_actors[r][bj];
                            neighbour.connect_to_required(current_block_port.clone());
                        }
                    }
                    // add neighbours in the same column
                    for c in 0usize..blocks.len() {
                        if c != bj {
                            let neighbour = &block_actors[bi][c];
                            neighbour.connect_to_required(current_block_port.clone());
                        }
                    }
                }
            }
            let block_workers: Vec<Arc<Component<BlockActor>>> =
                block_actors.drain(..).flatten().collect();
            for worker in block_workers.iter() {
                system.start(worker);
            }
            self.block_workers = block_workers.iter().map(|ba| Arc::downgrade(ba)).collect(); // don't prevent them from deallocation when they are done
            self.missing_blocks = Some(self.block_workers.len());
            self.assembly = Some(blocks); // initialise with unfinished blocks
        }
    }

    ignore_control!(ManagerActor);

    impl Provide<ManagerPort> for ManagerActor {
        fn handle(&mut self, event: ManagerMsg) -> () {
            match event {
                ManagerMsg::ComputeFW { graph, latch } => {
                    debug!(self.ctx.log(), "Got ComputeFW!");

                    #[cfg(test)]
                    {
                        let mut my_graph = (*graph).clone();
                        my_graph.compute_floyd_warshall();
                        self.result = Some(my_graph);
                    }
                    self.distribute_graph(graph.as_ref());
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
                            info!(self.ctx.log(), "Done!");
                            // cleanup
                            self.block_workers.clear(); // they shutdown themselves
                            self.missing_blocks = None;
                            #[cfg(test)]
                            assert_eq!(self.result.take().unwrap(), result, "Wrong APSP result!");
                        } else {
                            debug!(
                                self.ctx.log(),
                                "Got another {} blocks outstanding.", missing_blocks
                            );
                        }
                    } else {
                        unimplemented!();
                    }
                }
            }
        }
    }

    #[derive(ComponentDefinition, Actor)]
    struct BlockActor {
        ctx: ComponentContext<Self>,
        manager_port: RequiredPort<ManagerPort, Self>,
        block_port_prov: ProvidedPort<BlockPort, Self>,
        block_port_req: RequiredPort<BlockPort, Self>,
        num_nodes: usize,
        num_neighbours: usize,
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
            num_neighbours: usize,
        ) -> BlockActor {
            BlockActor {
                ctx: ComponentContext::new(),
                manager_port: RequiredPort::new(),
                block_port_prov: ProvidedPort::new(),
                block_port_req: RequiredPort::new(),
                num_nodes,
                num_neighbours,
                k: Phase::default(),
                neighbour_data: BTreeMap::new(),
                next_neighbour_data: BTreeMap::new(),
                current_data: initial_block,
                done: false,
            }
        }

        fn notify_neighbours(&mut self) -> () {
            let msg = PhaseResult {
                k: self.k,
                data: self.current_data.clone(),
            };
            self.block_port_prov.trigger(msg);
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

    impl Provide<ControlPort> for BlockActor {
        fn handle(&mut self, event: ControlEvent) -> () {
            match event {
                ControlEvent::Start => {
                    debug!(
                        self.ctx.log(),
                        "Got Start at block_id={}.",
                        self.current_data.block_id()
                    );
                    self.notify_neighbours();
                }
                _ => (), // ignore
            }
        }
    }

    impl Require<ManagerPort> for BlockActor {
        fn handle(&mut self, _event: Never) -> () {
            // ignore
        }
    }
    impl Provide<BlockPort> for BlockActor {
        fn handle(&mut self, _event: Never) -> () {
            // ignore
        }
    }
    impl Require<BlockPort> for BlockActor {
        fn handle(&mut self, event: PhaseResult) -> () {
            let PhaseResult { k: other_k, data } = event;
            if !self.done {
                if other_k == self.k {
                    debug!(
                        self.ctx.log(),
                        "Got PhaseResult(k={}) at block_id={}",
                        other_k,
                        self.current_data.block_id()
                    );

                    self.neighbour_data.insert(data.block_id(), data);
                    while self.neighbour_data.len() == self.num_neighbours {
                        self.k.inc();
                        self.perform_computation();
                        self.notify_neighbours();
                        std::mem::swap(&mut self.neighbour_data, &mut self.next_neighbour_data);
                        // self.next_neighbour_data.clear(); has been emptied in perform_computation
                        if self.k == self.num_nodes - 1usize {
                            self.manager_port.trigger(ManagerMsg::BlockResult {
                                block: self.current_data.clone(),
                            });
                            self.ctx.suicide();
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
