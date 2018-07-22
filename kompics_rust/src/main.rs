extern crate futures;
extern crate futures_cpupool;
extern crate grpc;
extern crate kompics;
extern crate protobuf;
extern crate synchronoise;
extern crate tls_api;
extern crate tokio_core;
#[macro_use]
extern crate slog;
extern crate time;

use futures::future;
use futures::future::Future;
use kompics_benchmark::benchmarks;
use kompics_benchmark::benchmarks_grpc;
use kompics_benchmark::messages;
use std::env;
use std::thread;
//use tokio_core::reactor::Core;

mod bench;
mod benchmark;
mod benchmark_runner;
mod kompics_benchmark;

fn main() {
    let args: Vec<String> = env::args().collect();
    let mode: BenchMode = args
        .get(1)
        .map(|s| {
            let lows = s.to_lowercase();
            if lows == "actor" {
                BenchMode::ACTOR
            } else if lows == "component" {
                BenchMode::COMPONENT
            } else {
                panic!("Unkown bench mode {}", lows);
            }
        })
        .expect("No bench mode was provided!");
    let addr: String = args
        .get(2)
        .map(|s| s.clone())
        .unwrap_or("127.0.0.1:45678".to_string());
    let mut serverb = grpc::ServerBuilder::new_plain();
    serverb
        .http
        .set_addr(addr.clone())
        .expect(&format!("Could not use address: {}.", addr));
    match mode {
        BenchMode::ACTOR => {
            serverb.add_service(benchmarks_grpc::BenchmarkRunnerServer::new_service_def(
                benchmark_runner::BenchmarkRunnerActorImpl::new(),
            ))
        }
        BenchMode::COMPONENT => {
            serverb.add_service(benchmarks_grpc::BenchmarkRunnerServer::new_service_def(
                benchmark_runner::BenchmarkRunnerComponentImpl::new(),
            ))
        }
    }

    let _server = serverb.build().expect("server");

    loop {
        thread::park();
    }
}

enum BenchMode {
    ACTOR,
    COMPONENT,
}
