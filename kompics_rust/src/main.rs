extern crate protobuf;
extern crate grpc;
extern crate futures;
extern crate futures_cpupool;
extern crate tokio_core;
extern crate tls_api;
extern crate kompics;
extern crate synchronoise;
#[macro_use]
extern crate slog;
extern crate time;

use kompics_benchmark::messages;
use kompics_benchmark::benchmarks;
use kompics_benchmark::benchmarks_grpc;
use std::thread;
use std::env;
use futures::future;
use futures::future::Future;
//use tokio_core::reactor::Core;

mod kompics_benchmark;
mod benchmark;
mod bench;

fn main() {
	let args: Vec<String> = env::args().collect();
	let addr: String = args.get(1).map(|s| s.clone()).unwrap_or("127.0.0.1:45678".to_string());
    let mut serverb = grpc::ServerBuilder::new_plain();
    serverb.http.set_addr(addr.clone()).expect(&format!("Could not use address: {}.", addr));
    serverb.add_service(benchmarks_grpc::BenchmarkRunnerServer::new_service_def(BenchmarkRunnerImpl::new()));
    let _server = serverb.build().expect("server");

    loop {
        thread::park();
    }
}

struct BenchmarkRunnerImpl {
    //core: Core,
}

impl BenchmarkRunnerImpl {
    fn new() -> BenchmarkRunnerImpl {
        BenchmarkRunnerImpl {
            //core: Core::new(),
        }
    }
    fn run<F>(&self, f: F) -> impl Future<Item=messages::TestResult, Error=grpc::Error>
    where F: FnOnce() -> messages::TestResult + std::panic::UnwindSafe  {
        let lf = future::lazy(|| {
            let r = f();
            future::ok::<messages::TestResult, ()>(r)
        });
        let fcu = lf.catch_unwind();
        let fe = fcu.map_err(|_| grpc::Error::Panic("Benchmark panicked!".to_string()));
        let f = fe.map(|r: Result<_, _>| match r {
            Ok(tm) => tm,
            Err(_) => {
                let msg = "Something went wrong...".to_string();
                let mut tf = messages::TestFailure::new();
                tf.set_reason(msg);
                let mut rm = messages::TestResult::new();
                rm.set_failure(tf);
                rm
            }
        });
        f
    }
}

impl benchmarks_grpc::BenchmarkRunner for BenchmarkRunnerImpl {
    fn ping_pong(&self, _o: grpc::RequestOptions, p: benchmarks::PingPongRequest) -> grpc::SingleResponse<messages::TestResult> {
    	println!("Got ping_ping req: {}", p.number_of_messages);
    	let f = self.run(move || {
            let mut b = bench::pingpong::actor_pingpong::PingPong::new();
            benchmark::run(&mut b, &p).into()
        });
    	grpc::SingleResponse::no_metadata(f)
    }
}