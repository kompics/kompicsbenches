use super::*;

use crate::riker_system_provider::*;
use benchmark_suite_shared::kompics_benchmarks::benchmarks::ThroughputPingPongRequest;
use riker::actors::*;
use std::sync::Arc;
use synchronoise::CountdownEvent;

pub trait StartVariant {
    type Wrapper: Message;
    fn start() -> Self::Wrapper;
}

#[derive(Clone, Debug)]
pub enum StaticPingerMsg {
    Start,
    Pong,
}
impl StartVariant for StaticPingerMsg {
    type Wrapper = StaticPingerMsg;
    fn start() -> Self::Wrapper {
        StaticPingerMsg::Start
    }
}

#[derive(Clone, Debug)]
struct StaticPing {
    sender: ActorRef<StaticPingerMsg>,
}

#[derive(Clone, Debug)]
pub enum PingerMsg {
    Start,
    Pong(u64),
}
impl StartVariant for PingerMsg {
    type Wrapper = PingerMsg;
    fn start() -> Self::Wrapper {
        PingerMsg::Start
    }
}

#[derive(Clone, Debug)]
pub struct Ping {
    index: u64,
    sender: ActorRef<PingerMsg>,
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
    S: Message,
    D: Message,
{
    StaticOnly(Vec<ActorRef<S>>),
    NonStatic(Vec<ActorRef<D>>),
    Empty,
}

impl<S, D> EitherComponents<S, D>
where
    S: Message,
    D: Message,
{
    #[inline]
    pub fn take(&mut self) -> EitherComponents<S, D> {
        std::mem::replace(self, EitherComponents::Empty)
    }

    pub fn kill_all(self, system: &mut RikerSystem) -> Result<(), String> {
        match self {
            EitherComponents::StaticOnly(mut components) => {
                components.drain(..).for_each(|c| system.stop(c));
                Ok(())
            }
            EitherComponents::NonStatic(mut components) => {
                components.drain(..).for_each(|c| system.stop(c));
                Ok(())
            }
            EitherComponents::Empty => Err("EMPTY!".to_string()),
        }
    }
}

impl<S, D> EitherComponents<S, D>
where
    S: Message + StartVariant<Wrapper = S>,
    D: Message + StartVariant<Wrapper = D>,
{
    pub fn start_all(&mut self) -> Result<(), String> {
        match self {
            EitherComponents::StaticOnly(ref mut components) => {
                components
                    .iter()
                    .for_each(|c| c.tell(<S as StartVariant>::start(), None));
                Ok(())
            }
            EitherComponents::NonStatic(ref mut components) => {
                components
                    .iter()
                    .for_each(|c| c.tell(<D as StartVariant>::start(), None));
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
    system: Option<RikerSystem>,
    pingers: EitherComponents<StaticPingerMsg, PingerMsg>,
    pongers: EitherComponents<StaticPing, Ping>,
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
        let system = new_system("throughputpingpong");
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
                                .start(StaticPonger::props(), "static-ponger")
                                .expect("Couldn't create StaticPonger");
                            let ponger_rec = ponger.clone();
                            let platch = latch.clone();
                            vpo.push(ponger);
                            let pinger = system
                                .start(
                                    StaticPinger::props(num_msgs, pipeline, platch, ponger_rec),
                                    "static-pinger",
                                )
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
                            let ponger = system
                                .start(Ponger::props(), "ponger")
                                .expect("Couldn't create Ponger");
                            let ponger_rec = ponger.clone();
                            let platch = latch.clone();
                            vpo.push(ponger);
                            let pinger = system
                                .start(
                                    Pinger::props(num_msgs, pipeline, platch, ponger_rec),
                                    "pinger",
                                )
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
        self.pingers
            .start_all()
            .expect("Couldn't start all pingers");
        latch.wait();
    }

    fn cleanup_iteration(&mut self, last_iteration: bool, _exec_time_millis: f64) -> () {
        let mut system = self.system.take().unwrap();
        self.pingers
            .take()
            .kill_all(&mut system)
            .expect("Pingers did not shut down correctly!");
        self.pongers
            .take()
            .kill_all(&mut system)
            .expect("Pongers did not shut down correctly!");

        if last_iteration {
            system
                .shutdown()
                .wait()
                .expect("Riker didn't shut down properly");
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
    ponger: ActorRef<StaticPing>,
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
        ponger: ActorRef<StaticPing>,
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

    fn props(
        count: u64,
        pipeline: u64,
        latch: Arc<CountdownEvent>,
        ponger: ActorRef<StaticPing>,
    ) -> BoxActorProd<StaticPinger> {
        Props::new_from(move || StaticPinger::with(count, pipeline, latch.clone(), ponger.clone()))
    }
}

impl Actor for StaticPinger {
    type Msg = StaticPingerMsg;

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, _sender: Sender) {
        match msg {
            StaticPingerMsg::Start => {
                let mut pipelined: u64 = 0;
                while (pipelined < self.pipeline) && (self.sent_count < self.count) {
                    self.ponger.tell(
                        StaticPing {
                            sender: ctx.myself(),
                        },
                        None,
                    );
                    self.sent_count += 1;
                    pipelined += 1;
                }
            }
            StaticPingerMsg::Pong => {
                self.recv_count += 1;
                if self.recv_count < self.count {
                    if self.sent_count < self.count {
                        self.ponger.tell(
                            StaticPing {
                                sender: ctx.myself(),
                            },
                            ctx.myself().into(),
                        );
                        self.sent_count += 1;
                    }
                } else {
                    let _ = self.latch.decrement();
                }
            }
        }
    }
}

/*****************
 * Static Ponger *
 *****************/
struct StaticPonger;

impl StaticPonger {
    fn new() -> StaticPonger {
        StaticPonger
    }
    fn props() -> BoxActorProd<StaticPonger> {
        Props::new_from(StaticPonger::new)
    }
}
impl Actor for StaticPonger {
    type Msg = StaticPing;

    fn recv(&mut self, _ctx: &Context<Self::Msg>, msg: Self::Msg, _sender: Sender) {
        msg.sender.tell(StaticPingerMsg::Pong, None);
    }
}

/*********************
 * Non-Static Pinger *
 *********************/
struct Pinger {
    latch: Arc<CountdownEvent>,
    ponger: ActorRef<Ping>,
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
        ponger: ActorRef<Ping>,
    ) -> Pinger {
        Pinger {
            latch,
            ponger,
            count,
            pipeline,
            sent_count: 0,
            recv_count: 0,
        }
    }
    fn props(
        count: u64,
        pipeline: u64,
        latch: Arc<CountdownEvent>,
        ponger: ActorRef<Ping>,
    ) -> BoxActorProd<Pinger> {
        Props::new_from(move || Pinger::with(count, pipeline, latch.clone(), ponger.clone()))
    }
}

impl Actor for Pinger {
    type Msg = PingerMsg;

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, _sender: Sender) {
        match msg {
            PingerMsg::Start => {
                let mut pipelined: u64 = 0;
                while (pipelined < self.pipeline) && (self.sent_count < self.count) {
                    self.ponger.tell(
                        Ping {
                            index: self.sent_count,
                            sender: ctx.myself(),
                        },
                        None,
                    );
                    self.sent_count += 1;
                    pipelined += 1;
                }
            }
            PingerMsg::Pong(_) => {
                self.recv_count += 1;
                if self.recv_count < self.count {
                    if self.sent_count < self.count {
                        self.ponger.tell(
                            Ping {
                                index: self.sent_count,
                                sender: ctx.myself(),
                            },
                            None,
                        );
                        self.sent_count += 1;
                    }
                } else {
                    let _ = self.latch.decrement();
                }
            }
        }
    }
}

/*********************
 * Non-Static Ponger *
 *********************/
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
        msg.sender.tell(PingerMsg::Pong(msg.index), None);
    }
}
