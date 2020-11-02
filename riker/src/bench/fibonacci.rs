use super::*;

use crate::riker_system_provider::*;
use benchmark_suite_shared::kompics_benchmarks::benchmarks::FibonacciRequest;
use log::debug;
use riker::actors::*;
use std::sync::Arc;
use synchronoise::CountdownEvent;

#[derive(Default)]
pub struct Fibonacci;

impl Benchmark for Fibonacci {
    type Conf = FibonacciRequest;
    type Instance = FibonacciI;

    fn msg_to_conf(msg: Box<dyn (::protobuf::Message)>) -> Result<Self::Conf, BenchmarkError> {
        downcast_msg!(msg; FibonacciRequest)
    }

    fn new_instance() -> Self::Instance {
        FibonacciI::new()
    }

    const LABEL: &'static str = "Fibonacci";
}

pub struct FibonacciI {
    fib_number: Option<u32>,
    system: Option<RikerSystem>,
    fib: Option<ActorRef<FibonacciMsg>>,
    latch: Option<Arc<CountdownEvent>>,
}

impl FibonacciI {
    fn new() -> FibonacciI {
        FibonacciI {
            fib_number: None,
            system: None,
            fib: None,
            latch: None,
        }
    }
}

impl BenchmarkInstance for FibonacciI {
    type Conf = FibonacciRequest;

    fn setup(&mut self, c: &Self::Conf) -> () {
        self.fib_number = Some(c.fib_number);
        let system = RikerSystem::new("fibonacci", num_cpus::get()).expect("System");
        self.system = Some(system);
    }

    fn prepare_iteration(&mut self) -> () {
        match self.system {
            Some(ref system) => {
                let latch = Arc::new(CountdownEvent::new(1));
                let fib = system
                    .start(
                        FibonacciActor::props(ResultTarget::Latch(latch.clone())),
                        "fib",
                    )
                    .expect("Should start FibonacciActor!");

                self.fib = Some(fib);
                self.latch = Some(latch);
            }
            None => unimplemented!(),
        }
    }

    fn run_iteration(&mut self) -> () {
        match self.fib_number {
            Some(fib_number) => match self.fib {
                Some(ref fib) => {
                    let latch = self.latch.take().unwrap();

                    fib.tell(FibonacciMsg::request(fib_number), None);
                    latch.wait();
                }
                None => unimplemented!(),
            },
            None => unimplemented!(),
        }
    }

    fn cleanup_iteration(&mut self, last_iteration: bool, _exec_time_millis: f64) -> () {
        let _ = self.fib.take(); // kills itself, so just drop

        if last_iteration {
            let system = self.system.take().unwrap();
            system
                .shutdown()
                .wait()
                .expect("Rikerdidn't shut down properly");
            self.fib_number = None;
        }
    }
}

#[derive(Clone)]
enum ResultTarget {
    Latch(Arc<CountdownEvent>),
    Parent(ActorRef<FibonacciMsg>),
}

#[derive(Debug, Clone)]
enum FibonacciMsg {
    Request { n: u32 },
    Response { value: u64 },
}
impl FibonacciMsg {
    fn request(n: u32) -> FibonacciMsg {
        FibonacciMsg::Request { n }
    }

    fn response(value: u64) -> FibonacciMsg {
        FibonacciMsg::Response { value }
    }

    const RESPONSE_ONE: FibonacciMsg = FibonacciMsg::Response { value: 1u64 };
}

struct FibonacciActor {
    report_to: ResultTarget,
    result: u64,
    num_responses: u8,
}
impl FibonacciActor {
    fn with(report_to: ResultTarget) -> FibonacciActor {
        FibonacciActor {
            report_to,
            result: 0u64,
            num_responses: 0u8,
        }
    }

    fn props(report_to: ResultTarget) -> BoxActorProd<FibonacciActor> {
        Props::new_from(move || FibonacciActor::with(report_to.clone()))
    }

    fn send_result(&mut self, response: FibonacciMsg, ctx: &Context<FibonacciMsg>) {
        match self.report_to {
            ResultTarget::Parent(ref parent_ref) => {
                parent_ref.tell(response, None);
            }
            ResultTarget::Latch(ref latch) => {
                latch.decrement().expect("Should decrement!");
            }
        }
        ctx.stop(ctx.myself());
    }
}

impl Actor for FibonacciActor {
    type Msg = FibonacciMsg;

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, _sender: Sender) {
        match msg {
            FibonacciMsg::Request { n } => {
                debug!("Got Request n={}", n);
                if n <= 2u32 {
                    self.send_result(FibonacciMsg::RESPONSE_ONE, ctx);
                } else {
                    let self_ref = ctx.myself();
                    let f1 = ctx
                        .actor_of_props(
                            "fib1",
                            FibonacciActor::props(ResultTarget::Parent(self_ref.clone())),
                        )
                        .expect("Fibonacci Child");
                    f1.tell(FibonacciMsg::request(n - 1u32), None);
                    let f2 = ctx
                        .actor_of_props(
                            "fib2",
                            FibonacciActor::props(ResultTarget::Parent(self_ref.clone())),
                        )
                        .expect("Fibonacci Child");
                    f2.tell(FibonacciMsg::request(n - 2u32), None);
                }
            }
            FibonacciMsg::Response { value } => {
                debug!("Got Response value={}", value);
                self.num_responses += 1u8;
                self.result += value;

                if self.num_responses == 2u8 {
                    self.send_result(FibonacciMsg::response(self.result), ctx);
                }
            }
        }
    }
}
