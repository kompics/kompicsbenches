#[macro_use]
extern crate benchmark_suite_shared;

use benchmark_suite_shared::BenchmarkMain;
use grpc;
#[allow(unused_imports)]
use slog::{crit, debug, error, info, warn};
use std::env;

mod bench;
mod benchmark_runner;
pub mod kompact_system_provider;
pub mod partitioning_actor;
pub mod serialiser_ids;

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
            BenchmarkMain::run_with(
                args,
                benchmark_runner::BenchmarkRunnerActorImpl::new(),
                bench::actor(),
                kompact_system_provider::set_global_public_if,
            );
        }
        BenchMode::COMPONENT => {
            BenchmarkMain::run_with(
                args,
                benchmark_runner::BenchmarkRunnerComponentImpl::new(),
                bench::component(),
                kompact_system_provider::set_global_public_if,
            );
        }
        BenchMode::MIXED => {
            BenchmarkMain::run_with(
                args,
                benchmark_runner::BenchmarkRunnerMixedImpl::new(),
                bench::mixed(),
                kompact_system_provider::set_global_public_if,
            );
        }
    }
}

enum BenchMode {
    ACTOR,
    COMPONENT,
    MIXED,
}

#[cfg(test)]
mod tests {
    use super::*;
    use benchmark_suite_shared::test_utils::test_implementation;

    //#[ignore]
    #[test]
    fn test_actor() {
        let benchmarks = Box::new(bench::ActorFactory {});
        test_implementation(benchmarks);
    }

    #[test]
    fn test_component() {
        let benchmarks = Box::new(bench::ComponentFactory {});
        test_implementation(benchmarks);
    }

    //#[ignore]
    #[test]
    fn test_mixed() {
        let benchmarks = Box::new(bench::MixedFactory {});
        test_implementation(benchmarks);
    }
}
