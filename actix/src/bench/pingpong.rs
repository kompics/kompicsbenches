use super::*;

use actix::*;
use actix_system_provider::{ActixSystem, PoisonPill};
use benchmark_suite_shared::kompics_benchmarks::benchmarks::PingPongRequest;
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
                futures::executor::block_on(pinger_start_f);
                latch.wait();
            }
            None => unimplemented!(),
        }
    }

    fn cleanup_iteration(&mut self, last_iteration: bool, _exec_time_millis: f64) -> () {
        let mut system = self.system.take().unwrap();
        let pinger = self.pinger.take().unwrap();
        let ponger = self.ponger.take().unwrap();
        system.stop(pinger);
        system.stop(ponger);

        if last_iteration {
            system.shutdown().expect("Actix didn't shut down properly");
            self.num = None;
        } else {
            self.system = Some(system);
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct Start;

#[derive(Message)]
#[rtype(result = "()")]
struct Ping(Recipient<Pong>);
// impl fmt::Debug for Ping {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(f, "Ping(<pinger>)")
//     }
// }

#[derive(Message)]
#[rtype(result = "()")]
struct Pong;

struct Pinger {
    latch: Arc<CountdownEvent>,
    ponger: Addr<Ponger>,
    count_down: u64,
    self_rec: Option<Recipient<Pong>>,
}

impl Pinger {
    fn with(count: u64, latch: Arc<CountdownEvent>, ponger: Addr<Ponger>) -> Pinger {
        Pinger {
            latch,
            ponger,
            count_down: count,
            self_rec: None,
        }
    }
    fn self_rec(&self) -> Recipient<Pong> {
        self.self_rec.clone().unwrap()
    }
}

impl Actor for Pinger {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        //println!("Pinger is alive");
        self.self_rec = Some(ctx.address().recipient());
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        //println!("Pinger is stopped");
        self.self_rec = None;
    }
}

impl Handler<Start> for Pinger {
    type Result = ();

    fn handle(&mut self, _msg: Start, _ctx: &mut Context<Self>) -> Self::Result {
        self.ponger.do_send(Ping(self.self_rec()));
    }
}

impl Handler<Pong> for Pinger {
    type Result = ();

    fn handle(&mut self, _msg: Pong, _ctx: &mut Context<Self>) -> Self::Result {
        if self.count_down > 0 {
            self.count_down -= 1;
            self.ponger.do_send(Ping(self.self_rec()));
        } else {
            let _ = self.latch.decrement();
        }
    }
}
impl Handler<PoisonPill> for Pinger {
    type Result = ();

    fn handle(&mut self, _msg: PoisonPill, ctx: &mut Context<Self>) -> Self::Result {
        //println!("PoisonPill received, shutting down.");
        ctx.stop();
    }
}

struct Ponger;

impl Ponger {
    fn new() -> Ponger {
        Ponger
    }
}

impl Actor for Ponger {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        //println!("Ponger is alive");
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        //println!("Ponger is stopped");
    }
}

impl Handler<Ping> for Ponger {
    type Result = ();

    fn handle(&mut self, msg: Ping, _ctx: &mut Context<Self>) -> Self::Result {
        let pinger = msg.0;
        pinger.do_send(Pong).expect("Pong didn't send.");
    }
}

impl Handler<PoisonPill> for Ponger {
    type Result = ();

    fn handle(&mut self, _msg: PoisonPill, ctx: &mut Context<Self>) -> Self::Result {
        //println!("PoisonPill received, shutting down.");
        ctx.stop();
    }
}
