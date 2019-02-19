#![feature(fnbox)]
#![feature(impl_trait_in_bindings)]
pub mod benchmark;
pub mod benchmark_client;
pub mod benchmark_master;
pub mod benchmark_runner;
pub mod kompics_benchmarks;

pub use self::benchmark::*;
use self::kompics_benchmarks::*;
use std::{net::SocketAddrV4, thread};

pub struct BenchmarkMain;
impl BenchmarkMain {
    pub fn run_with<H: benchmarks_grpc::BenchmarkRunner + 'static + Sync + Send + 'static>(
        args: Vec<String>,
        runner: H,
        benchmarks: Box<BenchmarkFactory>,
    ) -> ()
    {
        if args.len() <= 2 {
            // local mode
            let bench_runner_addr: String =
                args.get(1).map(|s| s.clone()).unwrap_or("127.0.0.1:45678".to_string());

            let mut serverb = grpc::ServerBuilder::new_plain();
            serverb
                .http
                .set_addr(bench_runner_addr.clone())
                .expect(&format!("Could not use address: {}.", bench_runner_addr));
            serverb.add_service(benchmarks_grpc::BenchmarkRunnerServer::new_service_def(runner));

            let server = serverb.build().expect("server");
            println!("Running in local mode with runner={}", server.local_addr());
            loop {
                thread::park();
                if server.is_alive() {
                    println!("Still running {}", server.local_addr());
                } else {
                    println!("Server died.");
                    std::process::exit(1);
                }
            }
        } else if args.len() == 3 {
            // client mode
            let master_addr: SocketAddrV4 =
                args[1].parse().expect("Could not convert arg to socket address!");
            let client_addr: SocketAddrV4 =
                args[2].parse().expect("Could not convert arg to socket address!");
            println!("Running in client mode with master={}, client={}", master_addr, client_addr);
            benchmark_client::run(
                *client_addr.ip(),
                client_addr.port(),
                *master_addr.ip(),
                master_addr.port(),
                benchmarks,
            );
        } else if args.len() == 4 {
            // master mode
            let bench_runner_addr: SocketAddrV4 =
                args[1].parse().expect("Could not convert arg to socket address!");
            let master_addr: SocketAddrV4 =
                args[2].parse().expect("Could not convert arg to socket address!");
            let num_clients: usize = args[3]
                .parse()
                .expect("Could not convert arg to unsigned integer number of clients.");
            println!(
                "Running in master mode with runner={}, master={}, #clients={}",
                bench_runner_addr, master_addr, num_clients
            );
            benchmark_master::run(
                bench_runner_addr.port(),
                master_addr.port(),
                num_clients,
                benchmarks,
            );
        } else {
            panic!("Too many args={} provided!", args.len());
        };
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
