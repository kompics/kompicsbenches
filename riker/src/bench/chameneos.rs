use super::*;

use crate::riker_system_provider::*;
use benchmark_suite_shared::helpers::chameneos::ChameneosColour;
use benchmark_suite_shared::kompics_benchmarks::benchmarks::ChameneosRequest;
use log::info;
use riker::actors::*;
use std::convert::TryInto;
use std::ops::Deref;
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
    system: Option<RikerSystem>,
    mall: Option<ActorRef<MallMsg>>,
    chameneos: Vec<ActorRef<ChameneoMsg>>,
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
        let system = RikerSystem::new("chameneos", num_cpus::get()).expect("System");
        self.system = Some(system);
    }

    fn prepare_iteration(&mut self) -> () {
        if let Some(ref system) = self.system {
            if let Some(num_chameneos) = self.num_chameneos {
                if let Some(num_meetings) = self.num_meetings {
                    let latch = Arc::new(CountdownEvent::new(1));
                    let mall = system
                        .start(
                            ChameneosMallActor::props(num_meetings, num_chameneos, latch.clone()),
                            "chameneos-mall",
                        )
                        .expect("ChameneosMallActor never started!");
                    self.latch = Some(latch);

                    for i in 0usize..num_chameneos {
                        let initial_colour = ChameneosColour::for_id(i);
                        let chameneo = system
                            .start(
                                ChameneoActor::props(mall.clone(), initial_colour),
                                "chameneo",
                            )
                            .expect("Chameneo never started");
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
            chameneo.tell(ChameneoMsg::Start, None)
        }
        latch.wait();
    }

    fn cleanup_iteration(&mut self, last_iteration: bool, _exec_time_millis: f64) -> () {
        let system = self.system.take().unwrap();
        self.chameneos.clear(); // they stop themselves so just drop all

        if let Some(mall) = self.mall.take() {
            system.stop(mall);
        }

        if last_iteration {
            system
                .shutdown()
                .wait()
                .expect("Riker didn't shut down properly");
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
        chameneo: ChameneoRef,
    },
}
impl MallMsg {
    fn count(meeting_count: u64) -> MallMsg {
        MallMsg::MeetingCount(meeting_count)
    }
    fn meet(colour: ChameneosColour, chameneo: ChameneoRef) -> MallMsg {
        MallMsg::Meet { colour, chameneo }
    }
}

// used to break trait bound resolution cycle of Message
#[derive(Debug, Clone)]
struct ChameneoRef(ActorRef<ChameneoMsg>);
//impl Message for ChameneoRef {}
// definitely safe, the compiler just endlessly recurses while trying to figure this out
unsafe impl Send for ChameneoRef {}
impl Deref for ChameneoRef {
    type Target = ActorRef<ChameneoMsg>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl From<ActorRef<ChameneoMsg>> for ChameneoRef {
    fn from(actor: ActorRef<ChameneoMsg>) -> Self {
        ChameneoRef(actor)
    }
}

#[derive(Debug, Clone)]
enum ChameneoMsg {
    Start,
    Meet {
        colour: ChameneosColour,
        chameneo: ChameneoRef,
    },
    Change {
        colour: ChameneosColour,
    },
    Exit,
}
impl ChameneoMsg {
    fn meet(colour: ChameneosColour, chameneo: ChameneoRef) -> ChameneoMsg {
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
    waiting_chameneo: Option<ChameneoRef>,
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

    fn props(
        num_meetings: u64,
        num_chameneos: usize,
        latch: Arc<CountdownEvent>,
    ) -> BoxActorProd<ChameneosMallActor> {
        Props::new_from(move || ChameneosMallActor::with(num_meetings, num_chameneos, latch.clone()))
    }
}

impl Actor for ChameneosMallActor {
    type Msg = MallMsg;

    fn recv(&mut self, _ctx: &Context<Self::Msg>, msg: Self::Msg, _sender: Sender) {
        match msg {
            MallMsg::MeetingCount(count) => {
                self.num_faded += 1usize;
                self.sum_meetings += count;
                if self.num_faded == self.num_chameneos {
                    self.latch.decrement().expect("Should count down");
                    info!("Done!");
                }
            }
            MallMsg::Meet { colour, chameneo } => {
                if self.meetings_count < self.num_meetings {
                    match self.waiting_chameneo.take() {
                        Some(other) => {
                            self.meetings_count += 1u64;
                            other.tell(ChameneoMsg::meet(colour, chameneo), None);
                        }
                        None => {
                            self.waiting_chameneo = Some(chameneo);
                        }
                    }
                } else {
                    chameneo.tell(ChameneoMsg::Exit, None);
                }
            }
        }
    }
}

struct ChameneoActor {
    mall: ActorRef<MallMsg>,
    colour: ChameneosColour,
    meetings: u64,
}
impl ChameneoActor {
    fn with(mall: ActorRef<MallMsg>, initial_colour: ChameneosColour) -> ChameneoActor {
        ChameneoActor {
            mall,
            colour: initial_colour,
            meetings: 0u64,
        }
    }

    fn props(
        mall: ActorRef<MallMsg>,
        initial_colour: ChameneosColour,
    ) -> BoxActorProd<ChameneoActor> {
        Props::new_from(move || ChameneoActor::with(mall.clone(), initial_colour))
    }
}

impl Actor for ChameneoActor {
    type Msg = ChameneoMsg;

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, _sender: Sender) {
        match msg {
            ChameneoMsg::Start => {
                self.mall
                    .tell(MallMsg::meet(self.colour, ctx.myself().into()), None);
            }
            ChameneoMsg::Meet {
                colour: other_colour,
                chameneo,
            } => {
                self.colour = self.colour.complement(other_colour);
                self.meetings += 1u64;
                chameneo.tell(ChameneoMsg::change(self.colour), None);
                self.mall
                    .tell(MallMsg::meet(self.colour, ctx.myself().into()), None);
            }
            ChameneoMsg::Change { colour: new_colour } => {
                self.colour = new_colour;
                self.meetings += 1u64;
                self.mall
                    .tell(MallMsg::meet(self.colour, ctx.myself().into()), None);
            }
            ChameneoMsg::Exit => {
                self.colour = ChameneosColour::Faded;
                self.mall.tell(MallMsg::count(self.meetings), None);
                ctx.stop(ctx.myself());
            }
        }
    }
}
