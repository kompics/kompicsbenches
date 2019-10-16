use super::*;

use benchmark_suite_shared::kompics_benchmarks::benchmarks::FibonacciRequest;
use kompact::prelude::*;
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
    system: Option<KompactSystem>,
    fib: Option<Arc<Component<FibonacciActor>>>,
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
        let system = crate::kompact_system_provider::global().new_system("fibonacci");
        self.system = Some(system);
    }

    fn prepare_iteration(&mut self) -> () {
        match self.system {
            Some(ref system) => {
                let latch = Arc::new(CountdownEvent::new(1));
                let fib =
                    system.create(|| FibonacciActor::with(ResultTarget::Latch(latch.clone())));

                let fib_f = system.start_notify(&fib);

                fib_f
                    .wait_timeout(Duration::from_millis(30000)) // wait longer to be sure the system is cleaned out
                    .expect("FibonacciActor never started!");

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
                    let fib_ref = fib.actor_ref();

                    fib_ref.tell(FibonacciMsg::request(fib_number));
                    latch.wait();
                }
                None => unimplemented!(),
            },
            None => unimplemented!(),
        }
    }

    fn cleanup_iteration(&mut self, last_iteration: bool, _exec_time_millis: f64) -> () {
        let _ = self.fib.take(); // kills itself, so just drop
        std::thread::sleep(Duration::from_millis(500)); // reduce bleed over effect from still dying actors into next test
        if last_iteration {
            let system = self.system.take().unwrap();
            system
                .shutdown()
                .expect("Kompics didn't shut down properly");
            self.fib_number = None;
        }
    }
}

enum ResultTarget {
    Latch(Arc<CountdownEvent>),
    Parent(ActorRefStrong<FibonacciMsg>),
}

#[derive(Debug)]
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

#[derive(ComponentDefinition)]
struct FibonacciActor {
    ctx: ComponentContext<Self>,
    report_to: ResultTarget,
    result: u64,
    num_responses: u8,
}
impl FibonacciActor {
    fn with(report_to: ResultTarget) -> FibonacciActor {
        FibonacciActor {
            ctx: ComponentContext::new(),
            report_to,
            result: 0u64,
            num_responses: 0u8,
        }
    }

    fn send_result(&mut self, response: FibonacciMsg) {
        match self.report_to {
            ResultTarget::Parent(ref parent_ref) => {
                parent_ref.tell(response);
            }
            ResultTarget::Latch(ref latch) => {
                latch.decrement().expect("Should decrement!");
            }
        }
        self.ctx.suicide();
    }
}

ignore_control!(FibonacciActor);

impl Actor for FibonacciActor {
    type Message = FibonacciMsg;

    fn receive_local(&mut self, msg: Self::Message) -> () {
        match msg {
            FibonacciMsg::Request { n } => {
                debug!(self.ctx.log(), "Got Request n={}", n);
                if n <= 2u32 {
                    self.send_result(FibonacciMsg::RESPONSE_ONE);
                } else {
                    let self_ref = self.actor_ref().hold().expect("Live ref");
                    let f1 = self
                        .ctx
                        .system()
                        .create(|| FibonacciActor::with(ResultTarget::Parent(self_ref.clone())));
                    self.ctx.system().start(&f1); // don't use create_and_start to avoid unnecessary registration
                    f1.actor_ref().tell(FibonacciMsg::request(n - 1u32));
                    let f2 = self
                        .ctx
                        .system()
                        .create(|| FibonacciActor::with(ResultTarget::Parent(self_ref.clone())));
                    self.ctx.system().start(&f2); // don't use create_and_start to avoid unnecessary registration
                    f2.actor_ref().tell(FibonacciMsg::request(n - 2u32));
                }
            }
            FibonacciMsg::Response { value } => {
                debug!(self.ctx.log(), "Got Response value={}", value);
                self.num_responses += 1u8;
                self.result += value;

                if self.num_responses == 2u8 {
                    self.send_result(FibonacciMsg::response(self.result));
                }
            }
        }
    }

    fn receive_network(&mut self, _msg: NetMessage) -> () {
        unimplemented!();
    }
}
