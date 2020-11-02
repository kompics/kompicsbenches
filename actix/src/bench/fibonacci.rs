use super::*;

use actix::*;
use actix_system_provider::ActixSystem;
use benchmark_suite_shared::kompics_benchmarks::benchmarks::FibonacciRequest;
use std::sync::Arc;
use synchronoise::CountdownEvent;
use futures::Sink;

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
    system: Option<ActixSystem>,
    fib: Option<Addr<FibonacciActor>>,
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
        let system = crate::actix_system_provider::new_system("fibonacci");
        self.system = Some(system);
    }

    fn prepare_iteration(&mut self) -> () {
        match self.system {
            Some(ref mut system) => {
                let latch = Arc::new(CountdownEvent::new(1));
                let latch2 = latch.clone();
                let fib = system
                    .start(move || FibonacciActor::with(ResultTarget::Latch(latch2)))
                    .expect("Create FibonacciActor");

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

                    fib.do_send(FibonacciMsg::request(fib_number));
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
            system.shutdown().expect("Actix didn't shut down properly");
            self.fib_number = None;
        }
    }
}

enum ResultTarget {
    Latch(Arc<CountdownEvent>),
    Parent(Recipient<FibResponse>),
}

#[derive(Message)]
#[rtype(result = "()")]
struct FibRequest {
    n: u32,
}

#[derive(Message)]
#[rtype(result = "()")]
struct FibResponse {
    value: u64,
}

#[derive(Debug)]
struct FibonacciMsg;
impl FibonacciMsg {
    fn request(n: u32) -> FibRequest {
        FibRequest { n }
    }

    fn response(value: u64) -> FibResponse {
        FibResponse { value }
    }

    const RESPONSE_ONE: FibResponse = FibResponse { value: 1u64 };
}

struct FibonacciActor {
    report_to: ResultTarget,
    result: u64,
    num_responses: u8,
    self_rec: Option<Recipient<FibResponse>>,
}
impl FibonacciActor {
    fn with(report_to: ResultTarget) -> FibonacciActor {
        FibonacciActor {
            report_to,
            result: 0u64,
            num_responses: 0u8,
            self_rec: None,
        }
    }

    fn send_result(&mut self, response: FibResponse, ctx: &mut Context<Self>) {
        match self.report_to {
            ResultTarget::Parent(ref parent_ref) => {
                parent_ref
                    .do_send(response)
                    .expect("Send response to parent");
            }
            ResultTarget::Latch(ref latch) => {
                latch.decrement().expect("Should decrement!");
            }
        }
        ctx.stop();
    }
}

impl Actor for FibonacciActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        self.self_rec = Some(ctx.address().recipient());
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        self.self_rec = None;
    }
}

impl Handler<FibRequest> for FibonacciActor {
    type Result = ();

    fn handle(&mut self, msg: FibRequest, ctx: &mut Context<Self>) -> Self::Result {
        if let Some(ref self_rec) = self.self_rec {
            let n = msg.n;
            if n <= 2u32 {
                self.send_result(FibonacciMsg::RESPONSE_ONE, ctx);
            } else {
                let f1 = FibonacciActor::with(ResultTarget::Parent(self_rec.clone())).start();
                f1.do_send(FibonacciMsg::request(n - 1u32));
                let f2 = FibonacciActor::with(ResultTarget::Parent(self_rec.clone())).start();
                f2.do_send(FibonacciMsg::request(n - 2u32));
            }
        } else {
            unimplemented!();
        }
    }
}

impl Handler<FibResponse> for FibonacciActor {
    type Result = ();

    fn handle(&mut self, msg: FibResponse, ctx: &mut Context<Self>) -> Self::Result {
        self.num_responses += 1u8;
        self.result += msg.value;

        if self.num_responses == 2u8 {
            self.send_result(FibonacciMsg::response(self.result), ctx);
        }
    }
}
