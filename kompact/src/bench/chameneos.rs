use super::*;

use benchmark_suite_shared::helpers::chameneos::ChameneosColour;
use benchmark_suite_shared::kompics_benchmarks::benchmarks::ChameneosRequest;
use kompact::prelude::*;
use std::convert::TryInto;
use std::sync::Arc;
use synchronoise::CountdownEvent;

pub mod actor_chameneos {
    use super::*;

    #[derive(Default)]
    pub struct Chameneos;

    impl Benchmark for Chameneos {
        type Conf = ChameneosRequest;
        type Instance = ChameneosI;

        fn msg_to_conf(msg: Box<dyn (::protobuf::Message)>) -> Result<Self::Conf, BenchmarkError> {
            downcast_msg!(msg; ChameneosRequest)
        }

        fn new_instance() -> Self::Instance {
            ChameneosI::new()
        }

        const LABEL: &'static str = "Chameneos";
    }

    pub struct ChameneosI {
        num_chameneos: Option<usize>,
        num_meetings: Option<u64>,
        system: Option<KompactSystem>,
        mall: Option<Arc<Component<ChameneosMallActor>>>,
        chameneos: Vec<Arc<Component<ChameneoActor>>>,
        latch: Option<Arc<CountdownEvent>>,
    }

    impl ChameneosI {
        fn new() -> ChameneosI {
            ChameneosI {
                num_chameneos: None,
                num_meetings: None,
                system: None,
                mall: None,
                chameneos: Vec::new(),
                latch: None,
            }
        }
    }

    impl BenchmarkInstance for ChameneosI {
        type Conf = ChameneosRequest;

        fn setup(&mut self, c: &Self::Conf) -> () {
            self.num_chameneos = Some(c.number_of_chameneos.try_into().unwrap());
            self.num_meetings = Some(c.number_of_meetings);
            let system = crate::kompact_system_provider::global().new_system("chameneos");
            self.system = Some(system);
        }

        fn prepare_iteration(&mut self) -> () {
            if let Some(ref system) = self.system {
                if let Some(num_chameneos) = self.num_chameneos {
                    if let Some(num_meetings) = self.num_meetings {
                        let latch = Arc::new(CountdownEvent::new(1));
                        let mall = system.create(|| {
                            ChameneosMallActor::with(num_meetings, num_chameneos, latch.clone())
                        });
                        self.latch = Some(latch);
                        let mall_f = system.start_notify(&mall);
                        mall_f
                            .wait_timeout(Duration::from_millis(1000))
                            .expect("ChameneosMallActor never started!");
                        let mall_ref = mall.actor_ref().hold().expect("Live ref");
                        self.mall = Some(mall);

                        for i in 0usize..num_chameneos {
                            let initial_colour = ChameneosColour::for_id(i);
                            let chameneo = system
                                .create(|| ChameneoActor::with(mall_ref.clone(), initial_colour));
                            self.chameneos.push(chameneo);
                        }
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

        fn run_iteration(&mut self) -> () {
            if let Some(ref system) = self.system {
                let latch = self.latch.take().unwrap();
                for chameneo in self.chameneos.drain(..) {
                    system.start(&chameneo);
                }
                latch.wait();
            } else {
                unimplemented!();
            }
        }

        fn cleanup_iteration(&mut self, last_iteration: bool, _exec_time_millis: f64) -> () {
            let system = self.system.take().unwrap();
            // self.chameneos.clear(); // they stop themselves and got drained when run

            if let Some(mall) = self.mall.take() {
                let f = system.kill_notify(mall);
                f.wait_timeout(Duration::from_millis(1000))
                    .expect("Mall never died!");
            }

            if last_iteration {
                system
                    .shutdown()
                    .expect("Kompics didn't shut down properly");
                self.num_chameneos = None;
                self.num_meetings = None;
            } else {
                self.system = Some(system);
            }
        }
    }

    #[derive(Debug)]
    enum MallMsg {
        MeetingCount(u64),
        Meet {
            colour: ChameneosColour,
            chameneo: ActorRefStrong<ChameneoMsg>,
        },
    }
    impl MallMsg {
        fn count(meeting_count: u64) -> MallMsg {
            MallMsg::MeetingCount(meeting_count)
        }
        fn meet(colour: ChameneosColour, chameneo: ActorRefStrong<ChameneoMsg>) -> MallMsg {
            MallMsg::Meet { colour, chameneo }
        }
    }

    #[derive(Debug)]
    enum ChameneoMsg {
        Meet {
            colour: ChameneosColour,
            chameneo: ActorRefStrong<ChameneoMsg>,
        },
        Change {
            colour: ChameneosColour,
        },
        Exit,
    }
    impl ChameneoMsg {
        fn meet(colour: ChameneosColour, chameneo: ActorRefStrong<ChameneoMsg>) -> ChameneoMsg {
            ChameneoMsg::Meet { colour, chameneo }
        }
        fn change(colour: ChameneosColour) -> ChameneoMsg {
            ChameneoMsg::Change { colour }
        }
    }

    #[derive(ComponentDefinition)]
    struct ChameneosMallActor {
        ctx: ComponentContext<Self>,
        num_meetings: u64,
        num_chameneos: usize,
        latch: Arc<CountdownEvent>,
        waiting_chameneo: Option<ActorRefStrong<ChameneoMsg>>,
        sum_meetings: u64,
        meetings_count: u64,
        num_faded: usize,
    }

    impl ChameneosMallActor {
        fn with(
            num_meetings: u64,
            num_chameneos: usize,
            latch: Arc<CountdownEvent>,
        ) -> ChameneosMallActor {
            ChameneosMallActor {
                ctx: ComponentContext::new(),
                num_meetings,
                num_chameneos,
                latch,
                waiting_chameneo: None,
                sum_meetings: 0u64,
                meetings_count: 0u64,
                num_faded: 0usize,
            }
        }
    }

    ignore_control!(ChameneosMallActor);

    impl Actor for ChameneosMallActor {
        type Message = MallMsg;

        fn receive_local(&mut self, msg: Self::Message) -> () {
            match msg {
                MallMsg::MeetingCount(count) => {
                    self.num_faded += 1usize;
                    self.sum_meetings += count;
                    if self.num_faded == self.num_chameneos {
                        self.latch.decrement().expect("Should count down");
                        info!(self.ctx.log(), "Done!");
                    }
                }
                MallMsg::Meet { colour, chameneo } => {
                    if self.meetings_count < self.num_meetings {
                        match self.waiting_chameneo.take() {
                            Some(other) => {
                                self.meetings_count += 1u64;
                                other.tell(ChameneoMsg::meet(colour, chameneo));
                            }
                            None => {
                                self.waiting_chameneo = Some(chameneo);
                            }
                        }
                    } else {
                        chameneo.tell(ChameneoMsg::Exit);
                    }
                }
            }
        }

        fn receive_network(&mut self, _msg: NetMessage) -> () {
            unimplemented!();
        }
    }

    #[derive(ComponentDefinition)]
    struct ChameneoActor {
        ctx: ComponentContext<Self>,
        mall: ActorRefStrong<MallMsg>,
        colour: ChameneosColour,
        meetings: u64,
    }
    impl ChameneoActor {
        fn with(mall: ActorRefStrong<MallMsg>, initial_colour: ChameneosColour) -> ChameneoActor {
            ChameneoActor {
                ctx: ComponentContext::new(),
                mall,
                colour: initial_colour,
                meetings: 0u64,
            }
        }

        fn self_ref(&self) -> ActorRefStrong<ChameneoMsg> {
            self.ctx.actor_ref().hold().expect("Live ref")
        }
    }

    impl Provide<ControlPort> for ChameneoActor {
        fn handle(&mut self, msg: ControlEvent) {
            match msg {
                ControlEvent::Start => {
                    self.mall.tell(MallMsg::meet(self.colour, self.self_ref()));
                }
                _ => (), // ignore
            }
        }
    }

    impl Actor for ChameneoActor {
        type Message = ChameneoMsg;

        fn receive_local(&mut self, msg: Self::Message) -> () {
            match msg {
                ChameneoMsg::Meet {
                    colour: other_colour,
                    chameneo,
                } => {
                    self.colour = self.colour.complement(other_colour);
                    self.meetings += 1u64;
                    chameneo.tell(ChameneoMsg::change(self.colour));
                    self.mall.tell(MallMsg::meet(self.colour, self.self_ref()));
                }
                ChameneoMsg::Change { colour: new_colour } => {
                    self.colour = new_colour;
                    self.meetings += 1u64;
                    self.mall.tell(MallMsg::meet(self.colour, self.self_ref()));
                }
                ChameneoMsg::Exit => {
                    self.colour = ChameneosColour::Faded;
                    self.mall.tell(MallMsg::count(self.meetings));
                    self.ctx.suicide();
                }
            }
        }

        fn receive_network(&mut self, _msg: NetMessage) -> () {
            unimplemented!();
        }
    }
}

pub mod mixed_chameneos {
    use super::*;

    #[derive(Default)]
    pub struct Chameneos;

    impl Benchmark for Chameneos {
        type Conf = ChameneosRequest;
        type Instance = ChameneosI;

        fn msg_to_conf(msg: Box<dyn (::protobuf::Message)>) -> Result<Self::Conf, BenchmarkError> {
            downcast_msg!(msg; ChameneosRequest)
        }

        fn new_instance() -> Self::Instance {
            ChameneosI::new()
        }

        const LABEL: &'static str = "Chameneos";
    }

    pub struct ChameneosI {
        num_chameneos: Option<usize>,
        num_meetings: Option<u64>,
        system: Option<KompactSystem>,
        mall: Option<Arc<Component<ChameneosMallActor>>>,
        chameneos: Vec<Arc<Component<ChameneoActor>>>,
        latch: Option<Arc<CountdownEvent>>,
    }

    impl ChameneosI {
        fn new() -> ChameneosI {
            ChameneosI {
                num_chameneos: None,
                num_meetings: None,
                system: None,
                mall: None,
                chameneos: Vec::new(),
                latch: None,
            }
        }
    }

    impl BenchmarkInstance for ChameneosI {
        type Conf = ChameneosRequest;

        fn setup(&mut self, c: &Self::Conf) -> () {
            self.num_chameneos = Some(c.number_of_chameneos.try_into().unwrap());
            self.num_meetings = Some(c.number_of_meetings);
            let system = crate::kompact_system_provider::global().new_system("chameneos");
            self.system = Some(system);
        }

        fn prepare_iteration(&mut self) -> () {
            if let Some(ref system) = self.system {
                if let Some(num_chameneos) = self.num_chameneos {
                    if let Some(num_meetings) = self.num_meetings {
                        let latch = Arc::new(CountdownEvent::new(1));
                        let mall = system.create(|| {
                            ChameneosMallActor::with(num_meetings, num_chameneos, latch.clone())
                        });
                        self.latch = Some(latch);
                        let mall_f = system.start_notify(&mall);
                        mall_f
                            .wait_timeout(Duration::from_millis(1000))
                            .expect("ChameneosMallActor never started!");
                        let mall_port: ProvidedRef<MallPort> = mall.provided_ref();

                        for i in 0usize..num_chameneos {
                            let initial_colour = ChameneosColour::for_id(i);
                            let chameneo = system.create(|| ChameneoActor::with(initial_colour));
                            chameneo.connect_to_provided(mall_port.clone());
                            mall.connect_to_required(chameneo.required_ref());
                            self.chameneos.push(chameneo);
                        }
                        self.mall = Some(mall);
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

        fn run_iteration(&mut self) -> () {
            if let Some(ref system) = self.system {
                let latch = self.latch.take().unwrap();
                for chameneo in self.chameneos.drain(..) {
                    system.start(&chameneo);
                }
                latch.wait();
            } else {
                unimplemented!();
            }
        }

        fn cleanup_iteration(&mut self, last_iteration: bool, _exec_time_millis: f64) -> () {
            let system = self.system.take().unwrap();
            // self.chameneos.clear(); // they stop themselves and got drained when run

            if let Some(mall) = self.mall.take() {
                let f = system.kill_notify(mall);
                f.wait_timeout(Duration::from_millis(1000))
                    .expect("Mall never died!");
            }

            if last_iteration {
                system
                    .shutdown()
                    .expect("Kompics didn't shut down properly");
                self.num_chameneos = None;
                self.num_meetings = None;
            } else {
                self.system = Some(system);
            }
        }
    }

    #[derive(Debug, Clone)]
    enum MallMsg {
        MeetingCount(u64),
        Meet {
            colour: ChameneosColour,
            chameneo: ActorRefStrong<ChameneoMsg>,
        },
    }
    impl MallMsg {
        fn count(meeting_count: u64) -> MallMsg {
            MallMsg::MeetingCount(meeting_count)
        }
        fn meet(colour: ChameneosColour, chameneo: ActorRefStrong<ChameneoMsg>) -> MallMsg {
            MallMsg::Meet { colour, chameneo }
        }
    }

    #[derive(Debug, Clone)]
    struct Exit;

    struct MallPort;
    impl Port for MallPort {
        type Indication = Exit;
        type Request = MallMsg;
    }

    #[derive(Debug)]
    enum ChameneoMsg {
        Meet {
            colour: ChameneosColour,
            chameneo: ActorRefStrong<ChameneoMsg>,
        },
        Change {
            colour: ChameneosColour,
        },
    }
    impl ChameneoMsg {
        fn meet(colour: ChameneosColour, chameneo: ActorRefStrong<ChameneoMsg>) -> ChameneoMsg {
            ChameneoMsg::Meet { colour, chameneo }
        }
        fn change(colour: ChameneosColour) -> ChameneoMsg {
            ChameneoMsg::Change { colour }
        }
    }

    #[derive(ComponentDefinition, Actor)]
    struct ChameneosMallActor {
        ctx: ComponentContext<Self>,
        mall_port: ProvidedPort<MallPort, Self>,
        num_meetings: u64,
        num_chameneos: usize,
        latch: Arc<CountdownEvent>,
        waiting_chameneo: Option<ActorRefStrong<ChameneoMsg>>,
        sum_meetings: u64,
        meetings_count: u64,
        num_faded: usize,
        sent_exit: bool,
    }

    impl ChameneosMallActor {
        fn with(
            num_meetings: u64,
            num_chameneos: usize,
            latch: Arc<CountdownEvent>,
        ) -> ChameneosMallActor {
            ChameneosMallActor {
                ctx: ComponentContext::new(),
                mall_port: ProvidedPort::new(),
                num_meetings,
                num_chameneos,
                latch,
                waiting_chameneo: None,
                sum_meetings: 0u64,
                meetings_count: 0u64,
                num_faded: 0usize,
                sent_exit: false,
            }
        }
    }

    ignore_control!(ChameneosMallActor);

    impl Provide<MallPort> for ChameneosMallActor {
        fn handle(&mut self, msg: MallMsg) -> () {
            match msg {
                MallMsg::MeetingCount(count) => {
                    self.num_faded += 1usize;
                    self.sum_meetings += count;
                    if self.num_faded == self.num_chameneos {
                        self.latch.decrement().expect("Should count down");
                        info!(self.ctx.log(), "Done!");
                    }
                }
                MallMsg::Meet { colour, chameneo } => {
                    if self.meetings_count < self.num_meetings {
                        match self.waiting_chameneo.take() {
                            Some(other) => {
                                self.meetings_count += 1u64;
                                other.tell(ChameneoMsg::meet(colour, chameneo));
                            }
                            None => {
                                self.waiting_chameneo = Some(chameneo);
                            }
                        }
                    } else if !self.sent_exit {
                        self.mall_port.trigger(Exit);
                        self.sent_exit = true;
                    } // else just drop, since we already sent exit
                }
            }
        }
    }

    #[derive(ComponentDefinition)]
    struct ChameneoActor {
        ctx: ComponentContext<Self>,
        mall_port: RequiredPort<MallPort, Self>,
        colour: ChameneosColour,
        meetings: u64,
    }
    impl ChameneoActor {
        fn with(initial_colour: ChameneosColour) -> ChameneoActor {
            ChameneoActor {
                ctx: ComponentContext::new(),
                mall_port: RequiredPort::new(),
                colour: initial_colour,
                meetings: 0u64,
            }
        }

        fn self_ref(&self) -> ActorRefStrong<ChameneoMsg> {
            self.ctx.actor_ref().hold().expect("Live ref")
        }
    }

    impl Provide<ControlPort> for ChameneoActor {
        fn handle(&mut self, msg: ControlEvent) {
            match msg {
                ControlEvent::Start => {
                    self.mall_port
                        .trigger(MallMsg::meet(self.colour, self.self_ref()));
                }
                _ => (), // ignore
            }
        }
    }

    impl Require<MallPort> for ChameneoActor {
        fn handle(&mut self, _msg: Exit) -> () {
            self.colour = ChameneosColour::Faded;
            self.mall_port.trigger(MallMsg::count(self.meetings));
            self.ctx.suicide();
        }
    }

    impl Actor for ChameneoActor {
        type Message = ChameneoMsg;

        fn receive_local(&mut self, msg: Self::Message) -> () {
            match msg {
                ChameneoMsg::Meet {
                    colour: other_colour,
                    chameneo,
                } => {
                    self.colour = self.colour.complement(other_colour);
                    self.meetings += 1u64;
                    chameneo.tell(ChameneoMsg::change(self.colour));
                    self.mall_port
                        .trigger(MallMsg::meet(self.colour, self.self_ref()));
                }
                ChameneoMsg::Change { colour: new_colour } => {
                    self.colour = new_colour;
                    self.meetings += 1u64;
                    self.mall_port
                        .trigger(MallMsg::meet(self.colour, self.self_ref()));
                }
            }
        }

        fn receive_network(&mut self, _msg: NetMessage) -> () {
            unimplemented!();
        }
    }
}
