use super::*;

use kompics::*;
use std::sync::Arc;
use synchronoise::CountdownEvent;

#[derive(Clone, Debug)]
struct Start;
#[derive(Clone, Debug)]
struct Ping;
#[derive(Clone, Debug)]
struct Pong;

pub mod actor_pingpong {
    use super::*;

    pub struct PingPong {
        num: Option<u64>,
        system: Option<KompicsSystem>,
        pinger: Option<Arc<Component<Pinger>>>,
        ponger: Option<Arc<Component<Ponger>>>,
        latch: Option<Arc<CountdownEvent>>,
    }

    impl PingPong {
        pub fn new() -> PingPong {
            PingPong {
                num: None,
                system: None,
                pinger: None,
                ponger: None,
                latch: None,
            }
        }
    }

    impl Benchmark for PingPong {
        type Conf = benchmarks::PingPongRequest;

        fn setup(&mut self, c: &Self::Conf) -> () {
            self.num = Some(c.number_of_messages);
            let mut conf = KompicsConfig::new();
            conf.label("pingpong".to_string());
            conf.threads(2);
            let system = KompicsSystem::new(conf);
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

                        system.start(&ponger);
                        system.start(&pinger);
                        let ponger_f = system.start_notify(&ponger);
                        let pinger_f = system.start_notify(&pinger);

                        ponger_f
                            .await_timeout(Duration::from_millis(1000))
                            .expect("Ponger never started!");
                        pinger_f
                            .await_timeout(Duration::from_millis(1000))
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
                Some(ref system) => match self.pinger {
                    Some(ref pinger) => {
                        let latch = self.latch.take().unwrap();
                        let pinger_ref = pinger.actor_ref();

                        pinger_ref.tell(Box::new(Start), system);
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

            f.await_timeout(Duration::from_millis(1000))
                .expect("Pinger never died!");

            let ponger = self.ponger.take().unwrap();
            let f = system.kill_notify(ponger);

            f.await_timeout(Duration::from_millis(1000))
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
        ponger: ActorRef,
        count_down: u64,
    }

    impl Pinger {
        fn with(count: u64, latch: Arc<CountdownEvent>, ponger: ActorRef) -> Pinger {
            Pinger {
                ctx: ComponentContext::new(),
                latch,
                ponger,
                count_down: count,
            }
        }
    }

    impl Provide<ControlPort> for Pinger {
        fn handle(&mut self, _event: ControlEvent) -> () {
            // ignore
        }
    }

    impl Actor for Pinger {
        fn receive_local(&mut self, _sender: ActorRef, msg: Box<Any>) -> () {
            if msg.is::<Start>() {
                self.ponger.tell(Box::new(Ping), &self.ctx);
            } else if msg.is::<Pong>() {
                if self.count_down > 0 {
                    self.count_down -= 1;
                    self.ponger.tell(Box::new(Ping), &self.ctx);
                } else {
                    let _ = self.latch.decrement();
                }
            } else {
                crit!(self.ctx.log(), "Got unexpected local msg {:?}", msg);
                unimplemented!(); // shouldn't happen during the test
            }
        }
        fn receive_message(&mut self, sender: ActorPath, _ser_id: u64, _buf: &mut Buf) -> () {
            crit!(self.ctx.log(), "Got unexpected message from {}", sender);
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

    impl Provide<ControlPort> for Ponger {
        fn handle(&mut self, _event: ControlEvent) -> () {
            // ignore
        }
    }

    impl Actor for Ponger {
        fn receive_local(&mut self, sender: ActorRef, msg: Box<Any>) -> () {
            if msg.is::<Ping>() {
                sender.tell(Box::new(Pong), &self.ctx);
            } else {
                crit!(self.ctx.log(), "Got unexpected local msg {:?}", msg);
                unimplemented!(); // shouldn't happen during the test
            }
        }
        fn receive_message(&mut self, sender: ActorPath, _ser_id: u64, _buf: &mut Buf) -> () {
            crit!(self.ctx.log(), "Got unexpected message from {}", sender);
            unimplemented!(); // shouldn't happen during the test
        }
    }
}

mod component_pingpong {
    use super::*;

    struct PingPongPort;

    impl Port for PingPongPort {
        type Indication = Pong;
        type Request = Ping;
    }
}
