//#[macro_use]
//extern crate benchmark_suite_shared;
//extern crate kompact_benchmarks;

//use benchmark_suite_shared::BenchmarkMain;

#[allow(unused_imports)]
use slog::{crit, debug, error, info, warn};
//use std::env;

mod bench;
//mod benchmark_runner;
mod raft_test;
pub mod serialiser_ids;

fn main() {
//    let mut args: Vec<String> = env::args().collect();
//    BenchmarkMain::run_with(
//        args,
//        benchmark_runner::BenchmarkRunnerImpl::new(),
//        bench::mixed(),
//        kompact_system_provider::set_global_public_if,  // TODO: add kompact (from this project) in Cargo
//        )
    raft_test::main();
}
