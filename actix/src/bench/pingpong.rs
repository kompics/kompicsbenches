use super::*;

use actix::*;
use actix_system_provider::{ActixSystem, PoisonPill};
use benchmark_suite_shared::kompics_benchmarks::benchmarks::PingPongRequest;
use futures::Future;
use std::fmt;
use std::sync::Arc;
use synchronoise::CountdownEvent;

#[derive(Default)]
pub struct PingPong;

impl Benchmark for PingPong {
    type Conf = PingPongRequest;
    type Instance = PingPongI;

    fn msg_to_conf(msg: Box<::protobuf::Message>) -> Result<Self::Conf, BenchmarkError> {
        downcast_msg!(msg; PingPongRequest)
    }

    fn new_instance() -> Self::Instance {
        PingPongI::new()
    }

    const LABEL: &'static str = "PingPong";
}

pub struct PingPongI {
    num: Option<u64>,
    system: Option<ActixSystem>,
    pinger: Option<Addr<Pinger>>,
    ponger: Option<Addr<Ponger>>,
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
        let system = crate::actix_system_provider::new_system("pingpong");
        self.system = Some(system);
    }

    fn prepare_iteration(&mut self) -> () {
        match self.num {
            Some(num) => match self.system {
                Some(ref mut system) => {
                    let ponger = system.start(Ponger::new).expect("Should start Ponger!");
                    let latch = Arc::new(CountdownEvent::new(1));
                    let ponger_rec = ponger.clone();
                    let platch = latch.clone();
                    let pinger = system
                        .start(move || Pinger::with(num, platch, ponger_rec))
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
                let pinger_start_f = pinger.send(Start);
                pinger_start_f.wait().expect("Should have started!");
                latch.wait();
            }
            None => unimplemented!(),
        }
    }

    fn cleanup_iteration(&mut self, last_iteration: bool, _exec_time_millis: f64) -> () {
        let mut system = self.system.take().unwrap();
        let pinger = self.pinger.take().unwrap();
        let ponger = self.ponger.take().unwrap();
        system.stop(pinger).expect("Pinger didn't stop!");
        system.stop(ponger).expect("Ponger didn't stop!");

        if last_iteration {
            system.shutdown().expect("Actix didn't shut down properly");
            self.num = None;
        } else {
            self.system = Some(system);
        }
    }
}

#[derive(Clone, Debug)]
struct Start;
impl Message for Start {
    type Result = ();
}

#[derive(Clone)]
struct CacheRecipient(Recipient<Pong>);
impl Message for CacheRecipient {
    type Result = ();
}
impl fmt::Debug for CacheRecipient {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CacheRecipient(<pinger>)")
    }
}

#[derive(Clone, Debug)]
struct Ping;
impl Message for Ping {
    type Result = ();
}

#[derive(Clone, Debug)]
struct Pong;
impl Message for Pong {
    type Result = ();
}

struct Pinger {
    latch: Arc<CountdownEvent>,
    ponger: Addr<Ponger>,
    count_down: u64,
}

impl Pinger {
    fn with(count: u64, latch: Arc<CountdownEvent>, ponger: Addr<Ponger>) -> Pinger {
        Pinger {
            latch,
            ponger,
            count_down: count,
        }
    }
}

impl Actor for Pinger {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        println!("Pinger is alive");
        let self_rec = ctx.address().recipient();
        self.ponger.do_send(CacheRecipient(self_rec));
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        println!("Pinger is stopped");
    }
}

impl Handler<Start> for Pinger {
    type Result = ();

    fn handle(&mut self, _msg: Start, _ctx: &mut Context<Self>) -> Self::Result {
        self.ponger.do_send(Ping);
    }
}

impl Handler<Pong> for Pinger {
    type Result = ();

    fn handle(&mut self, _msg: Pong, _ctx: &mut Context<Self>) -> Self::Result {
        if self.count_down > 0 {
            self.count_down -= 1;
            self.ponger.do_send(Ping);
        } else {
            let _ = self.latch.decrement();
        }
    }
}
impl Handler<PoisonPill> for Pinger {
    type Result = ();

    fn handle(&mut self, _msg: PoisonPill, ctx: &mut Context<Self>) -> Self::Result {
        println!("PoisonPill received, shutting down.");
        ctx.stop();
    }
}

struct Ponger {
    pinger: Option<Recipient<Pong>>,
}

impl Ponger {
    fn new() -> Ponger {
        Ponger { pinger: None }
    }
}

impl Actor for Ponger {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        println!("Ponger is alive");
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        println!("Ponger is stopped");
    }
}

impl Handler<CacheRecipient> for Ponger {
    type Result = ();

    fn handle(&mut self, msg: CacheRecipient, _ctx: &mut Context<Self>) -> Self::Result {
        println!("Ponger has Pinger cached");
        self.pinger = Some(msg.0);
    }
}

impl Handler<Ping> for Ponger {
    type Result = ();

    fn handle(&mut self, _msg: Ping, _ctx: &mut Context<Self>) -> Self::Result {
        if let Some(ref pinger) = self.pinger {
            pinger.do_send(Pong).expect("Should bloody work!");
        } else {
            panic!("Recipient should have been cached already!");
        }
    }
}

impl Handler<PoisonPill> for Ponger {
    type Result = ();

    fn handle(&mut self, _msg: PoisonPill, ctx: &mut Context<Self>) -> Self::Result {
        println!("PoisonPill received, shutting down.");
        ctx.stop();
    }
}
