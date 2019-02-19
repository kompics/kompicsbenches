use super::*;
use benchmark_suite_shared::benchmark_runner::{not_implemented, run, run_async};
use benchmark_suite_shared::kompics_benchmarks::{benchmarks, benchmarks_grpc, messages};

pub struct BenchmarkRunnerActorImpl;

impl BenchmarkRunnerActorImpl {
    pub fn new() -> BenchmarkRunnerActorImpl {
        BenchmarkRunnerActorImpl {
            //core: Core::new(),
        }
    }
}

impl benchmarks_grpc::BenchmarkRunner for BenchmarkRunnerActorImpl {
    fn ping_pong(
        &self,
        _o: grpc::RequestOptions,
        p: benchmarks::PingPongRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        println!("Got ping_ping req: {}", p.number_of_messages);
        let f = run_async(move || {
            let b = bench::pingpong::actor_pingpong::PingPong::default();
            run(&b, &p).into()
        });
        grpc::SingleResponse::no_metadata(f)
    }

    fn net_ping_pong(
        &self,
        _o: grpc::RequestOptions,
        p: benchmarks::PingPongRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        grpc::SingleResponse::completed(not_implemented())
    }
}

pub struct BenchmarkRunnerComponentImpl;

impl BenchmarkRunnerComponentImpl {
    pub fn new() -> BenchmarkRunnerComponentImpl {
        BenchmarkRunnerComponentImpl {
            //core: Core::new(),
        }
    }
}

impl benchmarks_grpc::BenchmarkRunner for BenchmarkRunnerComponentImpl {
    fn ping_pong(
        &self,
        _o: grpc::RequestOptions,
        p: benchmarks::PingPongRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        println!("Got ping_ping req: {}", p.number_of_messages);
        let f = run_async(move || {
            let b = bench::pingpong::component_pingpong::PingPong::default();
            run(&b, &p).into()
        });
        grpc::SingleResponse::no_metadata(f)
    }

    fn net_ping_pong(
        &self,
        _o: grpc::RequestOptions,
        p: benchmarks::PingPongRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        grpc::SingleResponse::completed(not_implemented())
    }
}
