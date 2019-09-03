use super::*;
use benchmark_suite_shared::benchmark_runner::{not_implemented, run, run_async};
use benchmark_suite_shared::kompics_benchmarks::{benchmarks, benchmarks_grpc, messages};
use futures::future::Future;

#[derive(Clone)]
pub struct BenchmarkRunnerImpl;

impl BenchmarkRunnerImpl {
    pub fn new() -> BenchmarkRunnerImpl {
        BenchmarkRunnerImpl {}
    }
}

impl benchmarks_grpc::BenchmarkRunner for BenchmarkRunnerImpl {
    fn ready(
        &self,
        _o: grpc::RequestOptions,
        _p: messages::ReadyRequest,
    ) -> grpc::SingleResponse<messages::ReadyResponse> {
        println!("Got ready? req.");
        let mut msg = messages::ReadyResponse::new();
        msg.set_status(true);
        grpc::SingleResponse::completed(msg)
    }

    fn shutdown(
        &self,
        _o: grpc::RequestOptions,
        _p: messages::ShutdownRequest,
    ) -> ::grpc::SingleResponse<messages::ShutdownAck> {
        unimplemented!();
    }

    fn ping_pong(
        &self,
        _o: grpc::RequestOptions,
        p: benchmarks::PingPongRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        println!("Got ping_pong req: {}", p.number_of_messages);
        let f = run_async(move || {
            let b = bench::pingpong::PingPong::default();
            run(&b, &p).into()
        })
        .map_err(|e| {
            println!("Converting benchmark error into grpc error: {:?}", e);
            e.into()
        });
        grpc::SingleResponse::no_metadata(f)
    }

    fn net_ping_pong(
        &self,
        _o: grpc::RequestOptions,
        _p: benchmarks::PingPongRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        grpc::SingleResponse::completed(not_implemented())
    }

    fn throughput_ping_pong(
        &self,
        _o: grpc::RequestOptions,
        p: benchmarks::ThroughputPingPongRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        println!("Got req: {:?}", p);
        let f = run_async(move || {
            let b = bench::throughput_pingpong::PingPong::default();
            run(&b, &p).into()
        })
        .map_err(|e| {
            println!("Converting benchmark error into grpc error: {:?}", e);
            e.into()
        });
        grpc::SingleResponse::no_metadata(f)
    }

    fn net_throughput_ping_pong(
        &self,
        _o: grpc::RequestOptions,
        _p: benchmarks::ThroughputPingPongRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        grpc::SingleResponse::completed(not_implemented())
    }

    fn atomic_register(
        &self,
        _o: grpc::RequestOptions,
        _p: benchmarks::AtomicRegisterRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        unimplemented!();
    }
}
