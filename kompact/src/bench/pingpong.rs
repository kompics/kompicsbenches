use super::*;

use benchmark_suite_shared::kompics_benchmarks::benchmarks::PingPongRequest;
use kompact::prelude::*;
use messages::{
    PingerMessage, StaticPing, StaticPingWithSender, StaticPong, RUN, STATIC_PING, STATIC_PONG,
};
use std::sync::Arc;
use synchronoise::CountdownEvent;

pub mod actor_pingpong {
    use super::*;

    #[derive(Default)]
    pub struct PingPong;

    impl Benchmark for PingPong {
        type Conf = PingPongRequest;
        type Instance = PingPongI;

        fn msg_to_conf(msg: Box<dyn (::protobuf::Message)>) -> Result<Self::Conf, BenchmarkError> {
            downcast_msg!(msg; PingPongRequest)
        }

        fn new_instance() -> Self::Instance {
            PingPongI::new()
        }

        const LABEL: &'static str = "PingPong";
    }

    pub struct PingPongI {
        num: Option<u64>,
        system: Option<KompactSystem>,
        pinger: Option<Arc<Component<Pinger>>>,
        ponger: Option<Arc<Component<Ponger>>>,
        latch: Option<Arc<CountdownEvent>>,
    }

    impl PingPongI {
        fn new() -> PingPongI {
            PingPongI {
                num: None,
                system: None,
                pinger: None,
                ponger: None,
                latch: None,
            }
        }
    }

    impl BenchmarkInstance for PingPongI {
        type Conf = PingPongRequest;

        fn setup(&mut self, c: &Self::Conf) -> () {
            self.num = Some(c.number_of_messages);
            let system =
                crate::kompact_system_provider::global().new_system_with_threads("pingpong", 2);
            self.system = Some(system);
        }

        fn prepare_iteration(&mut self) -> () {
            match self.num {
                Some(num) => match self.system {
                    Some(ref system) => {
                        let ponger = system.create(Ponger::new);
                        let latch = Arc::new(CountdownEvent::new(1));
                        let pinger =
                            system.create(|| Pinger::with(num, latch.clone(), ponger.actor_ref()));

                        let ponger_f = system.start_notify(&ponger);
                        let pinger_f = system.start_notify(&pinger);

                        ponger_f
                            .wait_timeout(Duration::from_millis(1000))
                            .expect("Ponger never started!");
                        pinger_f
                            .wait_timeout(Duration::from_millis(1000))
                            .expect("Pinger never started!");

                        self.ponger = Some(ponger);
                        self.pinger = Some(pinger);
                        self.latch = Some(latch);
                    }
                    None => unimplemented!(),
                },
                None => unimplemented!(),
            }
        }

        fn run_iteration(&mut self) -> () {
            match self.system {
                Some(ref _system) => match self.pinger {
                    Some(ref pinger) => {
                        let latch = self.latch.take().unwrap();
                        let pinger_ref = pinger.actor_ref();

                        pinger_ref.tell(&RUN);
                        latch.wait();
                    }
                    None => unimplemented!(),
                },
                None => unimplemented!(),
            }
        }

        fn cleanup_iteration(&mut self, last_iteration: bool, _exec_time_millis: f64) -> () {
            let system = self.system.take().unwrap();
            let pinger = self.pinger.take().unwrap();
            let f = system.kill_notify(pinger);

            f.wait_timeout(Duration::from_millis(1000))
                .expect("Pinger never died!");

            let ponger = self.ponger.take().unwrap();
            let f = system.kill_notify(ponger);

            f.wait_timeout(Duration::from_millis(1000))
                .expect("Ponger never died!");

            if last_iteration {
                system
                    .shutdown()
                    .expect("Kompics didn't shut down properly");
                self.num = None;
            } else {
                self.system = Some(system);
            }
        }
    }

    #[derive(ComponentDefinition)]
    struct Pinger {
        ctx: ComponentContext<Pinger>,
        latch: Arc<CountdownEvent>,
        ponger: ActorRefStrong<StaticPingWithSender>,
        count_down: u64,
    }

    impl Pinger {
        fn with(
            count: u64,
            latch: Arc<CountdownEvent>,
            ponger: ActorRef<StaticPingWithSender>,
        ) -> Pinger {
            Pinger {
                ctx: ComponentContext::new(),
                latch,
                ponger: ponger.hold().expect("Live ref"),
                count_down: count,
            }
        }
    }

    ignore_control!(Pinger);

    impl Actor for Pinger {
        type Message = PingerMessage<&'static StaticPong>;

        fn receive_local(&mut self, msg: Self::Message) -> () {
            match msg {
                PingerMessage::Run => {
                    self.ponger.tell(WithSenderStrong::from(&STATIC_PING, self));
                }
                PingerMessage::Pong(_) => {
                    if self.count_down > 0 {
                        self.count_down -= 1;
                        self.ponger.tell(WithSenderStrong::from(&STATIC_PING, self));
                    } else {
                        self.latch.decrement().expect("Should decrement!");
                    }
                }
            }
        }
        fn receive_network(&mut self, msg: NetMessage) -> () {
            crit!(self.ctx.log(), "Got unexpected message: {:?}", msg);
            unimplemented!(); // shouldn't happen during the test
        }
    }

    #[derive(ComponentDefinition)]
    struct Ponger {
        ctx: ComponentContext<Ponger>,
    }

    impl Ponger {
        fn new() -> Ponger {
            Ponger {
                ctx: ComponentContext::new(),
            }
        }
    }

    ignore_control!(Ponger);

    impl Actor for Ponger {
        type Message = StaticPingWithSender;

        fn receive_local(&mut self, msg: Self::Message) -> () {
            msg.reply(PingerMessage::Pong(&STATIC_PONG));
        }
        fn receive_network(&mut self, msg: NetMessage) -> () {
            crit!(self.ctx.log(), "Got unexpected message: {:?}", msg);
            unimplemented!(); // shouldn't happen during the test
        }
    }
}

pub mod component_pingpong {
    use super::*;

    struct PingPongPort;

    impl Port for PingPongPort {
        type Indication = &'static StaticPong;
        type Request = &'static StaticPing;
    }

    #[derive(Default)]
    pub struct PingPong;

    impl Benchmark for PingPong {
        type Conf = PingPongRequest;
        type Instance = PingPongI;

        fn msg_to_conf(msg: Box<dyn (::protobuf::Message)>) -> Result<Self::Conf, BenchmarkError> {
            downcast_msg!(msg; PingPongRequest)
        }

        fn new_instance() -> Self::Instance {
            PingPongI::new()
        }

        const LABEL: &'static str = "PingPong";
    }

    pub struct PingPongI {
        num: Option<u64>,
        system: Option<KompactSystem>,
        pinger: Option<Arc<Component<Pinger>>>,
        ponger: Option<Arc<Component<Ponger>>>,
        latch: Option<Arc<CountdownEvent>>,
    }

    impl PingPongI {
        fn new() -> PingPongI {
            PingPongI {
                num: None,
                system: None,
                pinger: None,
                ponger: None,
                latch: None,
            }
        }
    }

    impl BenchmarkInstance for PingPongI {
        type Conf = PingPongRequest;

        fn setup(&mut self, c: &Self::Conf) -> () {
            self.num = Some(c.number_of_messages);
            let system =
                crate::kompact_system_provider::global().new_system_with_threads("pingpong", 2);
            self.system = Some(system);
        }

        fn prepare_iteration(&mut self) -> () {
            match self.num {
                Some(num) => match self.system {
                    Some(ref system) => {
                        let ponger = system.create(Ponger::new);
                        let latch = Arc::new(CountdownEvent::new(1));
                        let pinger = system.create(|| Pinger::with(num, latch.clone()));

                        // on_dual_definition(&pinger, &ponger, |pinger_def, ponger_def| {
                        //     biconnect(&mut ponger_def.ppp, &mut pinger_def.ppp);
                        // })
                        biconnect_components::<PingPongPort, _, _>(&ponger, &pinger)
                            .expect("Could not connect components!");

                        let ponger_f = system.start_notify(&ponger);

                        ponger_f
                            .wait_timeout(Duration::from_millis(1000))
                            .expect("Ponger never started!");

                        self.ponger = Some(ponger);
                        self.pinger = Some(pinger);
                        self.latch = Some(latch);
                    }
                    None => unimplemented!(),
                },
                None => unimplemented!(),
            }
        }

        fn run_iteration(&mut self) -> () {
            match self.system {
                Some(ref system) => match self.pinger {
                    Some(ref pinger) => {
                        let latch = self.latch.take().unwrap();

                        system.start(pinger);

                        latch.wait();
                    }
                    None => unimplemented!(),
                },
                None => unimplemented!(),
            }
        }

        fn cleanup_iteration(&mut self, last_iteration: bool, _exec_time_millis: f64) -> () {
            let system = self.system.take().unwrap();
            let pinger = self.pinger.take().unwrap();
            let f = system.kill_notify(pinger);

            f.wait_timeout(Duration::from_millis(1000))
                .expect("Pinger never died!");

            let ponger = self.ponger.take().unwrap();
            let f = system.kill_notify(ponger);

            f.wait_timeout(Duration::from_millis(1000))
                .expect("Ponger never died!");

            if last_iteration {
                system
                    .shutdown()
                    .expect("Kompics didn't shut down properly");
                self.num = None;
            } else {
                self.system = Some(system);
            }
        }
    }

    #[derive(ComponentDefinition, Actor)]
    struct Pinger {
        ctx: ComponentContext<Pinger>,
        ppp: RequiredPort<PingPongPort, Pinger>,
        latch: Arc<CountdownEvent>,
        count_down: u64,
    }

    impl Pinger {
        fn with(count: u64, latch: Arc<CountdownEvent>) -> Pinger {
            Pinger {
                ctx: ComponentContext::new(),
                ppp: RequiredPort::new(),
                latch,
                count_down: count,
            }
        }
    }

    impl Provide<ControlPort> for Pinger {
        fn handle(&mut self, event: ControlEvent) -> () {
            match event {
                ControlEvent::Start => self.ppp.trigger(&STATIC_PING),
                _ => (), // ignore
            }
        }
    }

    impl Require<PingPongPort> for Pinger {
        fn handle(&mut self, _event: &StaticPong) -> () {
            if self.count_down > 0 {
                self.count_down -= 1;
                self.ppp.trigger(&STATIC_PING);
            } else {
                self.latch.decrement().expect("Should decrement!");
            }
        }
    }

    #[derive(ComponentDefinition, Actor)]
    struct Ponger {
        ctx: ComponentContext<Ponger>,
        ppp: ProvidedPort<PingPongPort, Ponger>,
    }

    impl Ponger {
        fn new() -> Ponger {
            Ponger {
                ctx: ComponentContext::new(),
                ppp: ProvidedPort::new(),
            }
        }
    }

    ignore_control!(Ponger);

    impl Provide<PingPongPort> for Ponger {
        fn handle(&mut self, _event: &StaticPing) -> () {
            self.ppp.trigger(&STATIC_PONG);
        }
    }
}
