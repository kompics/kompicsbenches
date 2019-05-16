extern crate benchmark_suite_shared;

use benchmark_suite_shared::downcast_msg;
use benchmark_suite_shared::BenchmarkMain;
use grpc;
#[allow(unused_imports)]
use slog::{crit, debug, error, info, warn};
use std::env;

pub mod actix_system_provider;
mod bench;
mod benchmark_runner;

fn main() {
    let args: Vec<String> = env::args().collect();
    BenchmarkMain::run_with(
        args,
        benchmark_runner::BenchmarkRunnerImpl::new(),
        bench::factory(),
        |_| {},
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use benchmark_suite_shared::test_utils::test_implementation;

    #[test]
    fn test_mixed() {
        let benchmarks = Box::new(bench::Factory {});
        test_implementation(benchmarks);
    }
}
