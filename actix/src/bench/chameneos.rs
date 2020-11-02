use super::*;

use actix::*;
use actix_system_provider::{ActixSystem, PoisonPill};
use benchmark_suite_shared::helpers::chameneos::ChameneosColour;
use benchmark_suite_shared::kompics_benchmarks::benchmarks::ChameneosRequest;
use std::convert::TryInto;
use std::sync::Arc;
use synchronoise::CountdownEvent;

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
    system: Option<ActixSystem>,
    mall: Option<Addr<ChameneosMallActor>>,
    chameneos: Vec<Addr<ChameneoActor>>,
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
        let system = crate::actix_system_provider::new_system("chameneos");
        self.system = Some(system);
    }

    fn prepare_iteration(&mut self) -> () {
        if let Some(ref mut system) = self.system {
            if let Some(num_chameneos) = self.num_chameneos {
                if let Some(num_meetings) = self.num_meetings {
                    let latch = Arc::new(CountdownEvent::new(1));
                    let mall_latch = latch.clone();
                    let mall = ChameneosMallActor::with(num_meetings, num_chameneos, mall_latch).start();
                    self.latch = Some(latch);

                    for i in 0usize..num_chameneos {
                        let initial_colour = ChameneosColour::for_id(i);
                        let mall_ref = mall.clone();
                        let chameneo = ChameneoActor::with(mall_ref, initial_colour)
                            .start();
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
        let latch = self.latch.take().unwrap();
        for chameneo in self.chameneos.drain(..) {
            chameneo.do_send(ChameneoMsg::Start);
        }
        latch.wait();
    }

    fn cleanup_iteration(&mut self, last_iteration: bool, _exec_time_millis: f64) -> () {
        let mut system = self.system.take().unwrap();
        // self.chameneos.clear(); // they stop themselves and got drained when run

        if let Some(mall) = self.mall.take() {
            system.stop(mall);
        }

        if last_iteration {
            system.shutdown().expect("Actix didn't shut down properly");
            self.num_chameneos = None;
            self.num_meetings = None;
        } else {
            self.system = Some(system);
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
enum MallMsg {
    MeetingCount(u64),
    Meet {
        colour: ChameneosColour,
        chameneo: Addr<ChameneoActor>,
    },
}
impl MallMsg {
    fn count(meeting_count: u64) -> MallMsg {
        MallMsg::MeetingCount(meeting_count)
    }
    fn meet(colour: ChameneosColour, chameneo: Addr<ChameneoActor>) -> MallMsg {
        MallMsg::Meet { colour, chameneo }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
enum ChameneoMsg {
    Start,
    Meet {
        colour: ChameneosColour,
        chameneo: Addr<ChameneoActor>,
    },
    Change {
        colour: ChameneosColour,
    },
    Exit,
}
impl ChameneoMsg {
    fn meet(colour: ChameneosColour, chameneo: Addr<ChameneoActor>) -> ChameneoMsg {
        ChameneoMsg::Meet { colour, chameneo }
    }
    fn change(colour: ChameneosColour) -> ChameneoMsg {
        ChameneoMsg::Change { colour }
    }
}

struct ChameneosMallActor {
    num_meetings: u64,
    num_chameneos: usize,
    latch: Arc<CountdownEvent>,
    waiting_chameneo: Option<Addr<ChameneoActor>>,
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

impl Actor for ChameneosMallActor {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        // nothing
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        // nothing
    }
}

impl Handler<PoisonPill> for ChameneosMallActor {
    type Result = ();

    fn handle(&mut self, _msg: PoisonPill, ctx: &mut Context<Self>) -> Self::Result {
        //println!("PoisonPill received, shutting down.");
        ctx.stop();
    }
}

impl Handler<MallMsg> for ChameneosMallActor {
    type Result = ();

    fn handle(&mut self, msg: MallMsg, _ctx: &mut Context<Self>) -> Self::Result {
        match msg {
            MallMsg::MeetingCount(count) => {
                self.num_faded += 1usize;
                self.sum_meetings += count;
                if self.num_faded == self.num_chameneos {
                    self.latch.decrement().expect("Should count down");
                    println!("Chameneos Mall is Done!");
                }
            }
            MallMsg::Meet { colour, chameneo } => {
                if self.meetings_count < self.num_meetings {
                    match self.waiting_chameneo.take() {
                        Some(other) => {
                            self.meetings_count += 1u64;
                            other.do_send(ChameneoMsg::meet(colour, chameneo));
                        }
                        None => {
                            self.waiting_chameneo = Some(chameneo);
                        }
                    }
                } else {
                    chameneo.do_send(ChameneoMsg::Exit);
                }
            }
        }
    }
}

struct ChameneoActor {
    mall: Addr<ChameneosMallActor>,
    colour: ChameneosColour,
    meetings: u64,
    self_addr: Option<Addr<Self>>,
}
impl ChameneoActor {
    fn with(mall: Addr<ChameneosMallActor>, initial_colour: ChameneosColour) -> ChameneoActor {
        ChameneoActor {
            mall,
            colour: initial_colour,
            meetings: 0u64,
            self_addr: None,
        }
    }
}

impl Actor for ChameneoActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        self.self_addr = Some(ctx.address());
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        self.self_addr = None;
    }
}

impl Handler<ChameneoMsg> for ChameneoActor {
    type Result = ();

    fn handle(&mut self, msg: ChameneoMsg, ctx: &mut Context<Self>) -> Self::Result {
        if let Some(ref self_addr) = self.self_addr {
            match msg {
                ChameneoMsg::Start => {
                    self.mall
                        .do_send(MallMsg::meet(self.colour, self_addr.clone()));
                }
                ChameneoMsg::Meet {
                    colour: other_colour,
                    chameneo,
                } => {
                    self.colour = self.colour.complement(other_colour);
                    self.meetings += 1u64;
                    chameneo.do_send(ChameneoMsg::change(self.colour));
                    self.mall
                        .do_send(MallMsg::meet(self.colour, self_addr.clone()));
                }
                ChameneoMsg::Change { colour: new_colour } => {
                    self.colour = new_colour;
                    self.meetings += 1u64;
                    self.mall
                        .do_send(MallMsg::meet(self.colour, self_addr.clone()));
                }
                ChameneoMsg::Exit => {
                    self.colour = ChameneosColour::Faded;
                    self.mall.do_send(MallMsg::count(self.meetings));
                    ctx.stop();
                }
            }
        }
    }
}
