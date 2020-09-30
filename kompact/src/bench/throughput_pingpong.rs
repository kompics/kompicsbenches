use super::*;

use benchmark_suite_shared::kompics_benchmarks::benchmarks::ThroughputPingPongRequest;
use kompact::prelude::*;
use messages::{
    Ping, PingWithSender, PingerMessage, Pong, Run, StaticPing, StaticPingWithSender, StaticPong,
    RUN, STATIC_PING, STATIC_PONG,
};
use std::sync::Arc;
use synchronoise::CountdownEvent;

pub struct Params {
    pub num_msgs: u64,
    pub num_pairs: u32,
    pub pipeline: u64,
    pub static_only: bool,
}

impl Params {
    pub fn from_req(r: &ThroughputPingPongRequest) -> Params {
        Params {
            num_msgs: r.messages_per_pair,
            num_pairs: r.parallelism,
            pipeline: r.pipeline_size,
            static_only: r.static_only,
        }
    }
}

pub enum EitherComponents<S, D>
where
    S: 'static + ComponentDefinition,
    D: 'static + ComponentDefinition,
{
    StaticOnly(Vec<Arc<Component<S>>>),
    NonStatic(Vec<Arc<Component<D>>>),
    Empty,
}

impl<S, D> EitherComponents<S, D>
where
    S: 'static + ComponentDefinition,
    D: 'static + ComponentDefinition,
{
    #[inline]
    pub fn take(&mut self) -> EitherComponents<S, D> {
        std::mem::replace(self, EitherComponents::Empty)
    }

    pub fn start_all(&self, system: &KompactSystem) -> Result<(), String> {
        match self {
            EitherComponents::StaticOnly(components) => {
                let futures: Vec<_> = components.iter().map(|c| system.start_notify(c)).collect();
                for f in futures {
                    f.wait_timeout(Duration::from_millis(1000))
                        .map_err(|_| "Component never started!")?;
                }
                Ok(())
            }
            EitherComponents::NonStatic(components) => {
                let futures: Vec<_> = components.iter().map(|c| system.start_notify(c)).collect();
                for f in futures {
                    f.wait_timeout(Duration::from_millis(1000))
                        .map_err(|_| "Component never started!")?;
                }
                Ok(())
            }
            EitherComponents::Empty => Err("EMPTY!".to_string()),
        }
    }

    pub fn kill_all(self, system: &KompactSystem) -> Result<(), String> {
        match self {
            EitherComponents::StaticOnly(mut components) => {
                let futures: Vec<_> = components
                    .drain(..)
                    .map(|c| system.kill_notify(c))
                    .collect();
                for f in futures {
                    f.wait_timeout(Duration::from_millis(1000))
                        .map_err(|_| "Component never died!")?;
                }
                Ok(())
            }
            EitherComponents::NonStatic(mut components) => {
                let futures: Vec<_> = components
                    .drain(..)
                    .map(|c| system.kill_notify(c))
                    .collect();
                for f in futures {
                    f.wait_timeout(Duration::from_millis(1000))
                        .map_err(|_| "Component never died!")?;
                }
                Ok(())
            }
            EitherComponents::Empty => Err("EMPTY!".to_string()),
        }
    }

    // pub fn actor_ref_for_each<F>(&self, mut f: F) -> ()
    // where
    //     F: FnMut(ActorRef),
    // {
    //     match self {
    //         EitherComponents::StaticOnly(components) => {
    //             components.iter().map(|c| c.actor_ref()).for_each(|r| f(r))
    //         }
    //         EitherComponents::NonStatic(components) => {
    //             components.iter().map(|c| c.actor_ref()).for_each(|r| f(r))
    //         }
    //         EitherComponents::Empty => (), // nothing to do
    //     }
    // }

    // pub fn hold_all(&self) -> Vec<Recipient<Run>> {
    //     match self {
    //         EitherComponents::StaticOnly(components) => components
    //             .iter()
    //             .map(|c| c.actor_ref().recipient())
    //             .collect(),
    //         EitherComponents::NonStatic(components) => components
    //             .iter()
    //             .map(|c| c.actor_ref().recipient())
    //             .collect(),
    //         EitherComponents::Empty => Vec::new(), // nothing to do
    //     }
    // }
}

pub mod actor_pingpong {
    use super::*;

    #[derive(Default)]
    pub struct PingPong;

    impl Benchmark for PingPong {
        type Conf = ThroughputPingPongRequest;
        type Instance = PingPongI;

        fn msg_to_conf(msg: Box<dyn (::protobuf::Message)>) -> Result<Self::Conf, BenchmarkError> {
            downcast_msg!(msg; ThroughputPingPongRequest)
        }

        fn new_instance() -> Self::Instance {
            PingPongI::new()
        }

        const LABEL: &'static str = "ThroughputPingPong";
    }

    pub struct PingPongI {
        params: Option<Params>,
        system: Option<KompactSystem>,
        pingers: EitherComponents<StaticPinger, Pinger>,
        pinger_refs: Vec<Recipient<&'static Run>>,
        pongers: EitherComponents<StaticPonger, Ponger>,
        latch: Option<Arc<CountdownEvent>>,
    }

    impl PingPongI {
        fn new() -> PingPongI {
            PingPongI {
                params: None,
                system: None,
                pingers: EitherComponents::Empty,
                pinger_refs: Vec::new(),
                pongers: EitherComponents::Empty,
                latch: None,
            }
        }
    }

    impl BenchmarkInstance for PingPongI {
        type Conf = ThroughputPingPongRequest;

        fn setup(&mut self, c: &Self::Conf) -> () {
            self.params = Some(Params::from_req(c));
            let system = crate::kompact_system_provider::global().new_system("throughputpingpong");
            self.system = Some(system);
        }

        fn prepare_iteration(&mut self) -> () {
            match self.params {
                Some(ref params) => match self.system {
                    Some(ref system) => {
                        let latch = Arc::new(CountdownEvent::new(params.num_pairs as usize));
                        let (pingers, pongers) = if params.static_only {
                            let mut vpi = Vec::with_capacity(params.num_pairs as usize);
                            let mut vpo = Vec::with_capacity(params.num_pairs as usize);
                            for _ in 1..=params.num_pairs {
                                let ponger = system.create(StaticPonger::new);
                                let ponger_ref = ponger.actor_ref();
                                vpo.push(ponger);
                                let pinger = system.create(|| {
                                    StaticPinger::with(
                                        params.num_msgs,
                                        params.pipeline,
                                        latch.clone(),
                                        ponger_ref,
                                    )
                                });
                                self.pinger_refs.push(pinger.actor_ref().recipient());
                                vpi.push(pinger);
                            }
                            (
                                EitherComponents::StaticOnly(vpi),
                                EitherComponents::StaticOnly(vpo),
                            )
                        } else {
                            let mut vpi = Vec::with_capacity(params.num_pairs as usize);
                            let mut vpo = Vec::with_capacity(params.num_pairs as usize);
                            for _ in 1..=params.num_pairs {
                                let ponger = system.create(Ponger::new);
                                let ponger_ref = ponger.actor_ref();
                                vpo.push(ponger);
                                let pinger = system.create(|| {
                                    Pinger::with(
                                        params.num_msgs,
                                        params.pipeline,
                                        latch.clone(),
                                        ponger_ref,
                                    )
                                });
                                self.pinger_refs.push(pinger.actor_ref().recipient());
                                vpi.push(pinger);
                            }
                            (
                                EitherComponents::NonStatic(vpi),
                                EitherComponents::NonStatic(vpo),
                            )
                        };

                        pongers
                            .start_all(system)
                            .expect("Pongers did not start correctly!");
                        pingers
                            .start_all(system)
                            .expect("Pingers did not start correctly!");

                        self.pongers = pongers;
                        self.pingers = pingers;
                        self.latch = Some(latch);
                    }
                    None => unimplemented!(),
                },
                None => unimplemented!(),
            }
        }

        fn run_iteration(&mut self) -> () {
            assert!(self.system.is_some());
            let latch = self.latch.take().unwrap();
            self.pinger_refs.iter().for_each(|pinger_ref| {
                pinger_ref.tell(&RUN);
            });
            latch.wait();
        }

        fn cleanup_iteration(&mut self, last_iteration: bool, _exec_time_millis: f64) -> () {
            let system = self.system.take().unwrap();
            self.pinger_refs.clear();
            self.pingers
                .take()
                .kill_all(&system)
                .expect("Pingers did not shut down correctly!");
            self.pongers
                .take()
                .kill_all(&system)
                .expect("Pongers did not shut down correctly!");

            if last_iteration {
                system
                    .shutdown()
                    .expect("Kompics didn't shut down properly");
                self.params = None;
            } else {
                self.system = Some(system);
            }
        }
    }

    /*****************
     * Static Pinger *
     *****************/

    #[derive(ComponentDefinition)]
    struct StaticPinger {
        ctx: ComponentContext<StaticPinger>,
        latch: Arc<CountdownEvent>,
        ponger: ActorRefStrong<StaticPingWithSender>,
        count: u64,
        pipeline: u64,
        sent_count: u64,
        recv_count: u64,
    }

    impl StaticPinger {
        fn with(
            count: u64,
            pipeline: u64,
            latch: Arc<CountdownEvent>,
            ponger: ActorRef<StaticPingWithSender>,
        ) -> StaticPinger {
            StaticPinger {
                ctx: ComponentContext::uninitialised(),
                latch,
                ponger: ponger.hold().expect("Live ref"),
                count,
                pipeline,
                sent_count: 0,
                recv_count: 0,
            }
        }
    }

    ignore_lifecycle!(StaticPinger);

    impl Actor for StaticPinger {
        type Message = PingerMessage<&'static StaticPong>;

        fn receive_local(&mut self, msg: Self::Message) -> Handled {
            match msg {
                PingerMessage::Run => {
                    let mut pipelined: u64 = 0;
                    while (pipelined < self.pipeline) && (self.sent_count < self.count) {
                        self.ponger.tell(WithSenderStrong::from(&STATIC_PING, self));
                        self.sent_count += 1;
                        pipelined += 1;
                    }
                }
                PingerMessage::Pong(_) => {
                    self.recv_count += 1;
                    if self.recv_count < self.count {
                        if self.sent_count < self.count {
                            self.ponger.tell(WithSenderStrong::from(&STATIC_PING, self));
                            self.sent_count += 1;
                        }
                    } else {
                        self.latch.decrement().expect("Should decrement!");
                    }
                }
            }
            Handled::Ok
        }
        fn receive_network(&mut self, msg: NetMessage) -> Handled {
            crit!(self.ctx.log(), "Got unexpected message: {:?}", msg);
            unimplemented!(); // shouldn't happen during the test
        }
    }

    /*****************
     * Static Ponger *
     *****************/

    #[derive(ComponentDefinition)]
    struct StaticPonger {
        ctx: ComponentContext<StaticPonger>,
    }

    impl StaticPonger {
        fn new() -> StaticPonger {
            StaticPonger {
                ctx: ComponentContext::uninitialised(),
            }
        }
    }

    ignore_lifecycle!(StaticPonger);

    impl Actor for StaticPonger {
        type Message = StaticPingWithSender;

        fn receive_local(&mut self, msg: Self::Message) -> Handled {
            msg.reply(PingerMessage::Pong(&STATIC_PONG));
            Handled::Ok
        }
        fn receive_network(&mut self, msg: NetMessage) -> Handled {
            crit!(self.ctx.log(), "Got unexpected message: {:?}", msg);
            unimplemented!(); // shouldn't happen during the test
        }
    }

    /*********************
     * Non-Static Pinger *
     *********************/

    #[derive(ComponentDefinition)]
    struct Pinger {
        ctx: ComponentContext<Pinger>,
        latch: Arc<CountdownEvent>,
        ponger: ActorRefStrong<PingWithSender>,
        count: u64,
        pipeline: u64,
        sent_count: u64,
        recv_count: u64,
    }

    impl Pinger {
        fn with(
            count: u64,
            pipeline: u64,
            latch: Arc<CountdownEvent>,
            ponger: ActorRef<PingWithSender>,
        ) -> Pinger {
            Pinger {
                ctx: ComponentContext::uninitialised(),
                latch,
                ponger: ponger.hold().expect("Live ref"),
                count,
                pipeline,
                sent_count: 0,
                recv_count: 0,
            }
        }
    }

    ignore_lifecycle!(Pinger);

    impl Actor for Pinger {
        type Message = PingerMessage<Pong>;

        fn receive_local(&mut self, msg: Self::Message) -> Handled {
            match msg {
                PingerMessage::Run => {
                    let mut pipelined: u64 = 0;
                    while (pipelined < self.pipeline) && (self.sent_count < self.count) {
                        self.ponger
                            .tell(WithSenderStrong::from(Ping::new(self.sent_count), self));
                        self.sent_count += 1;
                        pipelined += 1;
                    }
                }
                PingerMessage::Pong(_pong) => {
                    self.recv_count += 1;
                    if self.recv_count < self.count {
                        if self.sent_count < self.count {
                            self.ponger
                                .tell(WithSenderStrong::from(Ping::new(self.sent_count), self));
                            self.sent_count += 1;
                        }
                    } else {
                        self.latch.decrement().expect("Should decrement!");
                    }
                }
            }
            Handled::Ok
        }
        fn receive_network(&mut self, msg: NetMessage) -> Handled {
            crit!(self.ctx.log(), "Got unexpected message: {:?}", msg);
            unimplemented!(); // shouldn't happen during the test
        }
    }

    /*********************
     * Non-Static Ponger *
     *********************/

    #[derive(ComponentDefinition)]
    struct Ponger {
        ctx: ComponentContext<Ponger>,
    }

    impl Ponger {
        fn new() -> Ponger {
            Ponger {
                ctx: ComponentContext::uninitialised(),
            }
        }
    }

    ignore_lifecycle!(Ponger);

    impl Actor for Ponger {
        type Message = PingWithSender;

        fn receive_local(&mut self, msg: Self::Message) -> Handled {
            msg.reply(PingerMessage::Pong(Pong::new(msg.index)));
            Handled::Ok
        }
        fn receive_network(&mut self, msg: NetMessage) -> Handled {
            crit!(self.ctx.log(), "Got unexpected message: {:?}", msg);
            unimplemented!(); // shouldn't happen during the test
        }
    }
}

pub mod component_pingpong {
    use super::*;

    struct StaticPingPongPort;

    impl Port for StaticPingPongPort {
        type Indication = StaticPong;
        type Request = StaticPing;
    }

    #[derive(Default)]
    pub struct PingPong;

    impl Benchmark for PingPong {
        type Conf = ThroughputPingPongRequest;
        type Instance = PingPongI;

        fn msg_to_conf(msg: Box<dyn (::protobuf::Message)>) -> Result<Self::Conf, BenchmarkError> {
            downcast_msg!(msg; ThroughputPingPongRequest)
        }

        fn new_instance() -> Self::Instance {
            PingPongI::new()
        }

        const LABEL: &'static str = "ThroughputPingPong";
    }

    pub struct PingPongI {
        params: Option<Params>,
        system: Option<KompactSystem>,
        pingers: EitherComponents<StaticPinger, Pinger>,
        pongers: EitherComponents<StaticPonger, Ponger>,
        latch: Option<Arc<CountdownEvent>>,
    }

    impl PingPongI {
        fn new() -> PingPongI {
            PingPongI {
                params: None,
                system: None,
                pingers: EitherComponents::Empty,
                pongers: EitherComponents::Empty,
                latch: None,
            }
        }
    }

    impl BenchmarkInstance for PingPongI {
        type Conf = ThroughputPingPongRequest;

        fn setup(&mut self, c: &Self::Conf) -> () {
            self.params = Some(Params::from_req(c));
            let system = crate::kompact_system_provider::global().new_system("throughputpingpong");
            self.system = Some(system);
        }

        fn prepare_iteration(&mut self) -> () {
            match self.params {
                Some(ref params) => match self.system {
                    Some(ref system) => {
                        let latch = Arc::new(CountdownEvent::new(params.num_pairs as usize));
                        let (pingers, pongers) = if params.static_only {
                            let mut vpi = Vec::with_capacity(params.num_pairs as usize);
                            let mut vpo = Vec::with_capacity(params.num_pairs as usize);
                            for _ in 1..=params.num_pairs {
                                let ponger = system.create(StaticPonger::new);
                                let pinger = system.create(|| {
                                    StaticPinger::with(
                                        params.num_msgs,
                                        params.pipeline,
                                        latch.clone(),
                                    )
                                });
                                // on_dual_definition(&pinger, &ponger, |pinger_def, ponger_def| {
                                //     biconnect(&mut ponger_def.ppp, &mut pinger_def.ppp);
                                // })
                                biconnect_components::<StaticPingPongPort, _, _>(&ponger, &pinger)
                                    .expect("Could not connect components!");
                                vpo.push(ponger);
                                vpi.push(pinger);
                            }
                            (
                                EitherComponents::StaticOnly(vpi),
                                EitherComponents::StaticOnly(vpo),
                            )
                        } else {
                            let mut vpi = Vec::with_capacity(params.num_pairs as usize);
                            let mut vpo = Vec::with_capacity(params.num_pairs as usize);
                            for _ in 1..=params.num_pairs {
                                let ponger = system.create(Ponger::new);

                                let pinger = system.create(|| {
                                    Pinger::with(params.num_msgs, params.pipeline, latch.clone())
                                });
                                // on_dual_definition(&pinger, &ponger, |pinger_def, ponger_def| {
                                //     biconnect(&mut ponger_def.ppp, &mut pinger_def.ppp);
                                // })
                                biconnect_components::<PingPongPort, _, _>(&ponger, &pinger)
                                    .expect("Could not connect components!");
                                vpo.push(ponger);
                                vpi.push(pinger);
                            }
                            (
                                EitherComponents::NonStatic(vpi),
                                EitherComponents::NonStatic(vpo),
                            )
                        };

                        pongers
                            .start_all(system)
                            .expect("Pongers did not start correctly!");

                        self.pongers = pongers;
                        self.pingers = pingers;
                        self.latch = Some(latch);
                    }
                    None => unimplemented!(),
                },
                None => unimplemented!(),
            }
        }

        fn run_iteration(&mut self) -> () {
            match self.system {
                Some(ref system) => {
                    let latch = self.latch.take().unwrap();

                    self.pingers
                        .start_all(system)
                        .expect("Pingers did not start correctly!");

                    latch.wait();
                }
                None => unimplemented!(),
            }
        }

        fn cleanup_iteration(&mut self, last_iteration: bool, _exec_time_millis: f64) -> () {
            let system = self.system.take().unwrap();
            self.pingers
                .take()
                .kill_all(&system)
                .expect("Pingers did not shut down correctly!");
            self.pongers
                .take()
                .kill_all(&system)
                .expect("Pongers did not shut down correctly!");

            if last_iteration {
                system
                    .shutdown()
                    .expect("Kompics didn't shut down properly");
                self.params = None;
            } else {
                self.system = Some(system);
            }
        }
    }

    /*****************
     * Static Pinger *
     *****************/

    #[derive(ComponentDefinition, Actor)]
    struct StaticPinger {
        ctx: ComponentContext<StaticPinger>,
        ppp: RequiredPort<StaticPingPongPort>,
        latch: Arc<CountdownEvent>,
        count: u64,
        pipeline: u64,
        sent_count: u64,
        recv_count: u64,
    }

    impl StaticPinger {
        fn with(count: u64, pipeline: u64, latch: Arc<CountdownEvent>) -> StaticPinger {
            StaticPinger {
                ctx: ComponentContext::uninitialised(),
                ppp: RequiredPort::uninitialised(),
                latch,
                count,
                pipeline,
                sent_count: 0,
                recv_count: 0,
            }
        }
    }

    impl ComponentLifecycle for StaticPinger {
        fn on_start(&mut self) -> Handled {
            let mut pipelined: u64 = 0;
            while (pipelined < self.pipeline) && (self.sent_count < self.count) {
                self.ppp.trigger(StaticPing);
                self.sent_count += 1;
                pipelined += 1;
            }
            Handled::Ok
        }
    }

    impl Require<StaticPingPongPort> for StaticPinger {
        fn handle(&mut self, _event: StaticPong) -> Handled {
            self.recv_count += 1;
            if self.recv_count < self.count {
                if self.sent_count < self.count {
                    self.ppp.trigger(StaticPing);
                    self.sent_count += 1;
                }
            } else {
                self.latch.decrement().expect("Should decrement!");
            }
            Handled::Ok
        }
    }

    /*****************
     * Static Ponger *
     *****************/

    #[derive(ComponentDefinition, Actor)]
    struct StaticPonger {
        ctx: ComponentContext<StaticPonger>,
        ppp: ProvidedPort<StaticPingPongPort>,
    }

    impl StaticPonger {
        fn new() -> StaticPonger {
            StaticPonger {
                ctx: ComponentContext::uninitialised(),
                ppp: ProvidedPort::uninitialised(),
            }
        }
    }

    ignore_lifecycle!(StaticPonger);

    impl Provide<StaticPingPongPort> for StaticPonger {
        fn handle(&mut self, _event: StaticPing) -> Handled {
            self.ppp.trigger(StaticPong);
            Handled::Ok
        }
    }

    /*********************
     * Non-Static Pinger *
     *********************/

    struct PingPongPort;

    impl Port for PingPongPort {
        type Indication = Pong;
        type Request = Ping;
    }

    #[derive(ComponentDefinition, Actor)]
    struct Pinger {
        ctx: ComponentContext<Pinger>,
        ppp: RequiredPort<PingPongPort>,
        latch: Arc<CountdownEvent>,
        count: u64,
        pipeline: u64,
        sent_count: u64,
        recv_count: u64,
    }

    impl Pinger {
        fn with(count: u64, pipeline: u64, latch: Arc<CountdownEvent>) -> Pinger {
            Pinger {
                ctx: ComponentContext::uninitialised(),
                ppp: RequiredPort::uninitialised(),
                latch,
                count,
                pipeline,
                sent_count: 0,
                recv_count: 0,
            }
        }
    }

    impl ComponentLifecycle for Pinger {
        fn on_start(&mut self) -> Handled {
            let mut pipelined: u64 = 0;
            while (pipelined < self.pipeline) && (self.sent_count < self.count) {
                self.ppp.trigger(Ping::new(self.sent_count));
                self.sent_count += 1;
                pipelined += 1;
            }
            Handled::Ok
        }
    }

    impl Require<PingPongPort> for Pinger {
        fn handle(&mut self, _event: Pong) -> Handled {
            self.recv_count += 1;
            if self.recv_count < self.count {
                if self.sent_count < self.count {
                    self.ppp.trigger(Ping::new(self.sent_count));
                    self.sent_count += 1;
                }
            } else {
                self.latch.decrement().expect("Should decrement!");
            }
            Handled::Ok
        }
    }

    /*********************
     * Non-Static Ponger *
     *********************/

    #[derive(ComponentDefinition, Actor)]
    struct Ponger {
        ctx: ComponentContext<Ponger>,
        ppp: ProvidedPort<PingPongPort>,
    }

    impl Ponger {
        fn new() -> Ponger {
            Ponger {
                ctx: ComponentContext::uninitialised(),
                ppp: ProvidedPort::uninitialised(),
            }
        }
    }

    ignore_lifecycle!(Ponger);

    impl Provide<PingPongPort> for Ponger {
        fn handle(&mut self, event: Ping) -> Handled {
            self.ppp.trigger(Pong::new(event.index));
            Handled::Ok
        }
    }
}
