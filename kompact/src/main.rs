extern crate futures;
extern crate futures_cpupool;
extern crate grpc;
extern crate kompact;
extern crate protobuf;
extern crate synchronoise;
extern crate tls_api;
extern crate tokio_core;
#[macro_use]
extern crate slog;
extern crate time;

use futures::future;
use futures::future::Future;
use kompact_benchmark::benchmarks;
use kompact_benchmark::benchmarks_grpc;
use kompact_benchmark::messages;
use std::env;
use std::thread;
//use tokio_core::reactor::Core;

mod bench;
mod benchmark;
mod benchmark_runner;
mod kompact_benchmark;

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
        }).expect("No bench mode was provided!");
    if args.len() <= 3 {
        // local mode
        let bench_runner_addr: String = args
            .get(2)
            .map(|s| s.clone())
            .unwrap_or("127.0.0.1:45678".to_string());
        
        let mut serverb = grpc::ServerBuilder::new_plain();
        serverb
            .http
            .set_addr(bench_runner_addr.clone())
            .expect(&format!("Could not use address: {}.", bench_runner_addr));
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

        let server = serverb.build().expect("server");

        loop {
            println!("Running in local mode with runner={}, isAlive? {}", server.local_addr(), server.is_alive());
            thread::park();
        }
    } else if args.len() == 4 {
        // client mode
        let master_addr: String = args[2].clone();
        let client_addr: String = args[3].clone();
        println!(
            "Running in client mode with master={}, client={}",
            master_addr, client_addr
        );
    } else if args.len() == 5 {
        // master mode
        let bench_runner_addr: String = args[2].clone();
        let master_addr: String = args[3].clone();
        let num_clients: usize = args[4]
            .parse()
            .expect("Could not convert arg to unsigned integer number of clients.");
        println!(
            "Running in master mode with runner={}, master={}, #clients={}",
            bench_runner_addr, master_addr, num_clients
        );
    } else {
        panic!("Too many args={} provided!", args.len());
    }
    // let mut serverb = grpc::ServerBuilder::new_plain();
    // serverb
    //     .http
    //     .set_addr(addr.clone())
    //     .expect(&format!("Could not use address: {}.", addr));
    // match mode {
    //     BenchMode::ACTOR => {
    //         serverb.add_service(benchmarks_grpc::BenchmarkRunnerServer::new_service_def(
    //             benchmark_runner::BenchmarkRunnerActorImpl::new(),
    //         ))
    //     }
    //     BenchMode::COMPONENT => {
    //         serverb.add_service(benchmarks_grpc::BenchmarkRunnerServer::new_service_def(
    //             benchmark_runner::BenchmarkRunnerComponentImpl::new(),
    //         ))
    //     }
    // }

    // let _server = serverb.build().expect("server");

    loop {
        thread::park();
    }
}

enum BenchMode {
    ACTOR,
    COMPONENT,
}
