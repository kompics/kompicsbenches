use super::*;

use actix::*;
use actix_system_provider::{ActixSystem, PoisonPill};
use benchmark_suite_shared::kompics_benchmarks::benchmarks::ThroughputPingPongRequest;
use futures::Future;
use std::fmt;
use std::sync::Arc;
use synchronoise::CountdownEvent;

#[derive(Clone, Debug)]
pub struct Start;
impl Message for Start {
    type Result = ();
}

#[derive(Clone)]
struct StaticCacheRecipient(Recipient<StaticPong>);
impl Message for StaticCacheRecipient {
    type Result = ();
}
impl fmt::Debug for StaticCacheRecipient {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StaticCacheRecipient(<pinger>)")
    }
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
struct StaticPing;
impl Message for StaticPing {
    type Result = ();
}

#[derive(Clone, Debug)]
struct StaticPong;
impl Message for StaticPong {
    type Result = ();
}
#[derive(Clone, Debug)]
pub struct Ping {
    pub index: u64,
}
impl Ping {
    pub fn new(index: u64) -> Ping {
        Ping { index }
    }
}
impl Message for Ping {
    type Result = ();
}
#[derive(Clone, Debug)]
pub struct Pong {
    pub index: u64,
}
impl Pong {
    pub fn new(index: u64) -> Pong {
        Pong { index }
    }
}
impl Message for Pong {
    type Result = ();
}

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
    S: 'static + Actor<Context = Context<S>> + Handler<PoisonPill>,
    D: 'static + Actor<Context = Context<D>> + Handler<PoisonPill>,
{
    StaticOnly(Vec<Addr<S>>),
    NonStatic(Vec<Addr<D>>),
    Empty,
}

impl<S, D> EitherComponents<S, D>
where
    S: 'static + Actor<Context = Context<S>> + Handler<PoisonPill>,
    D: 'static + Actor<Context = Context<D>> + Handler<PoisonPill>,
{
    #[inline]
    pub fn take(&mut self) -> EitherComponents<S, D> {
        std::mem::replace(self, EitherComponents::Empty)
    }

    pub fn kill_all(self, system: &mut ActixSystem) -> Result<(), String> {
        match self {
            EitherComponents::StaticOnly(mut components) => {
                components
                    .drain(..)
                    .for_each(|c| system.stop(c).expect("Could not stop"));
                Ok(())
            }
            EitherComponents::NonStatic(mut components) => {
                components
                    .drain(..)
                    .for_each(|c| system.stop(c).expect("Could not stop"));
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
}

impl<S, D> EitherComponents<S, D>
where
    S: 'static + Actor<Context = Context<S>> + Handler<PoisonPill> + Handler<Start>,
    D: 'static + Actor<Context = Context<D>> + Handler<PoisonPill> + Handler<Start>,
{
    pub fn start_all(&mut self) -> Result<(), String> {
        match self {
            EitherComponents::StaticOnly(ref mut components) => {
                let futures: Vec<_> = components.iter().map(|c| c.send(Start)).collect();
                for f in futures {
                    f.wait().map_err(|_| "Component never started!")?;
                }
                Ok(())
            }
            EitherComponents::NonStatic(ref mut components) => {
                let futures: Vec<_> = components.iter().map(|c| c.send(Start)).collect();
                for f in futures {
                    f.wait().map_err(|_| "Component never started!")?;
                }
                Ok(())
            }
            EitherComponents::Empty => Err("EMPTY!".to_string()),
        }
    }
}

#[derive(Default)]
pub struct PingPong;

impl Benchmark for PingPong {
    type Conf = ThroughputPingPongRequest;
    type Instance = PingPongI;

    fn msg_to_conf(msg: Box<::protobuf::Message>) -> Result<Self::Conf, BenchmarkError> {
        downcast_msg!(msg; ThroughputPingPongRequest)
    }

    fn new_instance() -> Self::Instance {
        PingPongI::new()
    }

    const LABEL: &'static str = "ThroughputPingPong";
}

pub struct PingPongI {
    params: Option<Params>,
    system: Option<ActixSystem>,
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
        let system = crate::actix_system_provider::new_system("throughputpingpong");
        self.system = Some(system);
    }

    fn prepare_iteration(&mut self) -> () {
        match self.params {
            Some(ref params) => match self.system {
                Some(ref mut system) => {
                    let latch = Arc::new(CountdownEvent::new(params.num_pairs as usize));
                    let num_msgs = params.num_msgs;
                    let pipeline = params.pipeline;
                    let (pingers, pongers) = if params.static_only {
                        let mut vpi = Vec::with_capacity(params.num_pairs as usize);
                        let mut vpo = Vec::with_capacity(params.num_pairs as usize);
                        for _ in 1..=params.num_pairs {
                            let ponger = system
                                .start(StaticPonger::new)
                                .expect("Couldn't create StaticPonger");
                            let ponger_rec = ponger.clone();
                            let platch = latch.clone();
                            vpo.push(ponger);
                            let pinger = system
                                .start(move || {
                                    StaticPinger::with(num_msgs, pipeline, platch, ponger_rec)
                                })
                                .expect("Couldn't create StaticPinger");
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
                            let ponger = system.start(Ponger::new).expect("Couldn't create Ponger");
                            let ponger_rec = ponger.clone();
                            let platch = latch.clone();
                            vpo.push(ponger);
                            let pinger = system
                                .start(move || Pinger::with(num_msgs, pipeline, platch, ponger_rec))
                                .expect("Couldn't create Pinger");
                            vpi.push(pinger);
                        }
                        (
                            EitherComponents::NonStatic(vpi),
                            EitherComponents::NonStatic(vpo),
                        )
                    };

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
        let latch = self.latch.take().unwrap();
        self.pingers.start_all().expect("Couldn't start all pingers");
        latch.wait();
    }

    fn cleanup_iteration(&mut self, last_iteration: bool, _exec_time_millis: f64) -> () {
        let mut system = self.system.take().unwrap();
        self.pingers
            .take()
            .kill_all(&mut system)
            .expect("Pingers did not shut down correctly!");;
        self.pongers
            .take()
            .kill_all(&mut system)
            .expect("Pongers did not shut down correctly!");;

        if last_iteration {
            system.shutdown().expect("Actix didn't shut down properly");
            self.params = None;
        } else {
            self.system = Some(system);
        }
    }
}

/*****************
 * Static Pinger *
 *****************/
struct StaticPinger {
    latch: Arc<CountdownEvent>,
    ponger: Addr<StaticPonger>,
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
        ponger: Addr<StaticPonger>,
    ) -> StaticPinger {
        StaticPinger {
            latch,
            ponger,
            count,
            pipeline,
            sent_count: 0,
            recv_count: 0,
        }
    }
}

impl Actor for StaticPinger {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        println!("Pinger is alive");
        let self_rec = ctx.address().recipient();
        self.ponger.do_send(StaticCacheRecipient(self_rec));
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        println!("Pinger is stopped");
    }
}

impl Handler<Start> for StaticPinger {
    type Result = ();

    fn handle(&mut self, _msg: Start, _ctx: &mut Context<Self>) -> Self::Result {
        let mut pipelined: u64 = 0;
        while (pipelined < self.pipeline) && (self.sent_count < self.count) {
            self.ponger.do_send(StaticPing);
            self.sent_count += 1;
            pipelined += 1;
        }
    }
}

impl Handler<StaticPong> for StaticPinger {
    type Result = ();

    fn handle(&mut self, _msg: StaticPong, _ctx: &mut Context<Self>) -> Self::Result {
        self.recv_count += 1;
        if self.recv_count < self.count {
            if self.sent_count < self.count {
                self.ponger.do_send(StaticPing);
                self.sent_count += 1;
            }
        } else {
            let _ = self.latch.decrement();
        }
    }
}
impl Handler<PoisonPill> for StaticPinger {
    type Result = ();

    fn handle(&mut self, _msg: PoisonPill, ctx: &mut Context<Self>) -> Self::Result {
        println!("PoisonPill received, shutting down.");
        ctx.stop();
    }
}

/*****************
 * Static Ponger *
 *****************/
struct StaticPonger {
    pinger: Option<Recipient<StaticPong>>,
}

impl StaticPonger {
    fn new() -> StaticPonger {
        StaticPonger { pinger: None }
    }
}
impl Actor for StaticPonger {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        println!("Ponger is alive");
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        println!("Ponger is stopped");
    }
}

impl Handler<StaticCacheRecipient> for StaticPonger {
    type Result = ();

    fn handle(&mut self, msg: StaticCacheRecipient, _ctx: &mut Context<Self>) -> Self::Result {
        println!("Ponger has Pinger cached");
        self.pinger = Some(msg.0);
    }
}

impl Handler<StaticPing> for StaticPonger {
    type Result = ();

    fn handle(&mut self, _msg: StaticPing, _ctx: &mut Context<Self>) -> Self::Result {
        if let Some(ref pinger) = self.pinger {
            pinger.do_send(StaticPong).expect("Should bloody work!");
        } else {
            panic!("Recipient should have been cached already!");
        }
    }
}

impl Handler<PoisonPill> for StaticPonger {
    type Result = ();

    fn handle(&mut self, _msg: PoisonPill, ctx: &mut Context<Self>) -> Self::Result {
        println!("PoisonPill received, shutting down.");
        ctx.stop();
    }
}

/*********************
 * Non-Static Pinger *
 *********************/
struct Pinger {
    latch: Arc<CountdownEvent>,
    ponger: Addr<Ponger>,
    count: u64,
    pipeline: u64,
    sent_count: u64,
    recv_count: u64,
}

impl Pinger {
    fn with(count: u64, pipeline: u64, latch: Arc<CountdownEvent>, ponger: Addr<Ponger>) -> Pinger {
        Pinger {
            latch,
            ponger,
            count,
            pipeline,
            sent_count: 0,
            recv_count: 0,
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
        let mut pipelined: u64 = 0;
        while (pipelined < self.pipeline) && (self.sent_count < self.count) {
            self.ponger.do_send(Ping::new(self.sent_count));
            self.sent_count += 1;
            pipelined += 1;
        }
    }
}

impl Handler<Pong> for Pinger {
    type Result = ();

    fn handle(&mut self, _msg: Pong, _ctx: &mut Context<Self>) -> Self::Result {
        self.recv_count += 1;
        if self.recv_count < self.count {
            if self.sent_count < self.count {
                self.ponger.do_send(Ping::new(self.sent_count));
                self.sent_count += 1;
            }
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

/*********************
 * Non-Static Ponger *
 *********************/
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

    fn handle(&mut self, msg: Ping, _ctx: &mut Context<Self>) -> Self::Result {
        if let Some(ref pinger) = self.pinger {
            pinger
                .do_send(Pong::new(msg.index))
                .expect("Should bloody work!");
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
