use super::*;

use crate::riker_system_provider::*;
use benchmark_suite_shared::kompics_benchmarks::benchmarks::PingPongRequest;
use riker::actors::*;
use std::sync::Arc;
use synchronoise::CountdownEvent;

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
    system: Option<RikerSystem>,
    pinger: Option<ActorRef<PingerMsg>>,
    ponger: Option<ActorRef<Ping>>,
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
        let system = RikerSystem::new("pingpong", 2).expect("System");
        self.system = Some(system);
    }

    fn prepare_iteration(&mut self) -> () {
        match self.num {
            Some(num) => match self.system {
                Some(ref mut system) => {
                    let ponger = system
                        .start(Ponger::props(), "ponger")
                        .expect("Should start Ponger!");
                    let latch = Arc::new(CountdownEvent::new(1));
                    let ponger_rec = ponger.clone();
                    let platch = latch.clone();
                    let pinger = system
                        .start(Pinger::props(num, platch, ponger_rec), "pinger")
                        .expect("Should start Pinger");

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
        match self.pinger {
            Some(ref pinger) => {
                let latch = self.latch.take().unwrap();
                pinger.tell(PingerMsg::Start, None);
                latch.wait();
            }
            None => unimplemented!(),
        }
    }

    fn cleanup_iteration(&mut self, last_iteration: bool, _exec_time_millis: f64) -> () {
        let system = self.system.take().unwrap();
        let pinger = self.pinger.take().unwrap();
        let ponger = self.ponger.take().unwrap();
        system.stop(pinger);
        system.stop(ponger);

        if last_iteration {
            system
                .shutdown()
                .wait()
                .expect("Riker didn't shut down properly");
            self.num = None;
        } else {
            self.system = Some(system);
        }
    }
}

#[derive(Clone, Debug)]
enum PingerMsg {
    Start,
    Pong,
}

#[derive(Clone, Debug)]
struct Ping {
    sender: ActorRef<PingerMsg>,
}

struct Pinger {
    latch: Arc<CountdownEvent>,
    ponger: ActorRef<Ping>,
    count_down: u64,
}

impl Pinger {
    fn with(count: u64, latch: Arc<CountdownEvent>, ponger: ActorRef<Ping>) -> Pinger {
        Pinger {
            latch,
            ponger,
            count_down: count,
        }
    }
    fn props(
        count: u64,
        latch: Arc<CountdownEvent>,
        ponger: ActorRef<Ping>,
    ) -> BoxActorProd<Pinger> {
        Props::new_from(move || Pinger::with(count, latch.clone(), ponger.clone()))
    }
}

impl Actor for Pinger {
    type Msg = PingerMsg;

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, _sender: Sender) {
        match msg {
            PingerMsg::Start => {
                self.ponger.tell(
                    Ping {
                        sender: ctx.myself(),
                    },
                    None,
                );
            }
            PingerMsg::Pong => {
                if self.count_down > 0 {
                    self.count_down -= 1;
                    self.ponger.tell(
                        Ping {
                            sender: ctx.myself(),
                        },
                        None,
                    );
                } else {
                    let _ = self.latch.decrement();
                }
            }
        }
    }
}

struct Ponger;

impl Ponger {
    fn new() -> Ponger {
        Ponger
    }
    fn props() -> BoxActorProd<Ponger> {
        Props::new_from(Ponger::new)
    }
}

impl Actor for Ponger {
    type Msg = Ping;

    fn recv(&mut self, _ctx: &Context<Self::Msg>, msg: Self::Msg, _sender: Sender) {
        msg.sender.tell(PingerMsg::Pong, None);
    }
}
