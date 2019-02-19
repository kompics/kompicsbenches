#[macro_use]
extern crate slog;
#[macro_use]
extern crate benchmark_suite_shared;

use benchmark_suite_shared::BenchmarkMain;
use grpc;
use std::env;

mod bench;
mod benchmark_runner;

fn main() {
    let mut args: Vec<String> = env::args().collect();
    let mode: BenchMode = args
        .get(1)
        .map(|s| {
            let lows = s.to_lowercase();
            if lows == "actor" {
                BenchMode::ACTOR
            } else if lows == "component" {
                BenchMode::COMPONENT
            } else if lows == "mixed" {
                BenchMode::MIXED
            } else {
                panic!("Unkown bench mode {}", lows);
            }
        })
        .expect("No bench mode was provided!");

    args.remove(1);
    match mode {
        BenchMode::ACTOR => {
            BenchmarkMain::run_with(args, benchmark_runner::BenchmarkRunnerActorImpl::new(), bench::actor());
        }
        BenchMode::COMPONENT => {
            BenchmarkMain::run_with(args, benchmark_runner::BenchmarkRunnerComponentImpl::new(), bench::component());
        }
        BenchMode::MIXED => {
            BenchmarkMain::run_with(args, benchmark_runner::BenchmarkRunnerComponentImpl::new(), bench::mixed());
        }
    }
}

enum BenchMode {
    ACTOR,
    COMPONENT,
    MIXED,
}
