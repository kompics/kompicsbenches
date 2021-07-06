use super::*;
use benchmark_suite_shared::{
    benchmark_runner::{not_implemented, run, run_async},
    kompics_benchmarks::{benchmarks, benchmarks_grpc, messages},
};
use futures::future::Future;

#[derive(Clone)]
pub struct BenchmarkRunnerActorImpl;

impl BenchmarkRunnerActorImpl {
    pub fn new() -> BenchmarkRunnerActorImpl {
        BenchmarkRunnerActorImpl {
            //core: Core::new(),
        }
    }
}

impl benchmarks_grpc::BenchmarkRunner for BenchmarkRunnerActorImpl {
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
            let b = bench::pingpong::actor_pingpong::PingPong::default();
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
            let b = bench::throughput_pingpong::actor_pingpong::PingPong::default();
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
        grpc::SingleResponse::completed(not_implemented())
    }

    fn streaming_windows(
        &self,
        _o: grpc::RequestOptions,
        _p: benchmarks::StreamingWindowsRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        grpc::SingleResponse::completed(not_implemented())
    }

    fn fibonacci(
        &self,
        _o: grpc::RequestOptions,
        p: benchmarks::FibonacciRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        println!("Got fibonacci req: {:?}", p);
        let f = run_async(move || {
            let b = bench::fibonacci::Fibonacci::default();
            run(&b, &p).into()
        })
        .map_err(|e| {
            println!("Converting benchmark error into grpc error: {:?}", e);
            e.into()
        });
        grpc::SingleResponse::no_metadata(f)
    }

    fn chameneos(
        &self,
        _o: grpc::RequestOptions,
        p: benchmarks::ChameneosRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        println!("Got chameneos req: {:?}", p);
        let f = run_async(move || {
            let b = bench::chameneos::actor_chameneos::Chameneos::default();
            run(&b, &p).into()
        })
        .map_err(|e| {
            println!("Converting benchmark error into grpc error: {:?}", e);
            e.into()
        });
        grpc::SingleResponse::no_metadata(f)
    }

    fn all_pairs_shortest_path(
        &self,
        _o: grpc::RequestOptions,
        p: benchmarks::APSPRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        println!("Got APSP req: {:?}", p);
        let f = run_async(move || {
            let b = bench::all_pairs_shortest_path::actor_apsp::AllPairsShortestPath::default();
            run(&b, &p).into()
        })
        .map_err(|e| {
            println!("Converting benchmark error into grpc error: {:?}", e);
            e.into()
        });
        grpc::SingleResponse::no_metadata(f)
    }

    fn atomic_broadcast(
        &self,
        _o: grpc::RequestOptions,
        _p: benchmarks::AtomicBroadcastRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        grpc::SingleResponse::completed(not_implemented())
    }

    fn sized_throughput(
        &self,
        _o: grpc::RequestOptions,
        p: benchmarks::SizedThroughputRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        println!("Got STP req: {:?}", p);
        grpc::SingleResponse::completed(not_implemented())
    }
}

#[derive(Clone)]
pub struct BenchmarkRunnerComponentImpl;

impl BenchmarkRunnerComponentImpl {
    pub fn new() -> BenchmarkRunnerComponentImpl {
        BenchmarkRunnerComponentImpl {
            //core: Core::new(),
        }
    }
}

impl benchmarks_grpc::BenchmarkRunner for BenchmarkRunnerComponentImpl {
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
        println!("Got req: {:?}", p);
        let f = run_async(move || {
            let b = bench::pingpong::component_pingpong::PingPong::default();
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
            let b = bench::throughput_pingpong::component_pingpong::PingPong::default();
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
        grpc::SingleResponse::completed(not_implemented())
    }
    fn streaming_windows(
        &self,
        _o: grpc::RequestOptions,
        _p: benchmarks::StreamingWindowsRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        grpc::SingleResponse::completed(not_implemented())
    }

    fn fibonacci(
        &self,
        _o: grpc::RequestOptions,
        p: benchmarks::FibonacciRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        println!("Got fibonacci req: {:?}", p);
        grpc::SingleResponse::completed(not_implemented())
    }

    fn chameneos(
        &self,
        _o: grpc::RequestOptions,
        p: benchmarks::ChameneosRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        println!("Got chameneos req: {:?}", p);
        grpc::SingleResponse::completed(not_implemented())
    }

    fn all_pairs_shortest_path(
        &self,
        _o: grpc::RequestOptions,
        p: benchmarks::APSPRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        println!("Got APSP req: {:?}", p);
        let f = run_async(move || {
            let b = bench::all_pairs_shortest_path::component_apsp::AllPairsShortestPath::default();
            run(&b, &p).into()
        })
        .map_err(|e| {
            println!("Converting benchmark error into grpc error: {:?}", e);
            e.into()
        });
        grpc::SingleResponse::no_metadata(f)
    }

    fn atomic_broadcast(
        &self,
        _o: grpc::RequestOptions,
        _p: benchmarks::AtomicBroadcastRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        grpc::SingleResponse::completed(not_implemented())
    }

    fn sized_throughput(
        &self,
        _o: grpc::RequestOptions,
        _p: benchmarks::SizedThroughputRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        grpc::SingleResponse::completed(not_implemented())
    }
}

#[derive(Clone)]
pub struct BenchmarkRunnerMixedImpl;

impl BenchmarkRunnerMixedImpl {
    pub fn new() -> BenchmarkRunnerMixedImpl {
        BenchmarkRunnerMixedImpl {}
    }
}

impl benchmarks_grpc::BenchmarkRunner for BenchmarkRunnerMixedImpl {
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
        _p: benchmarks::PingPongRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        grpc::SingleResponse::completed(not_implemented())
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
        _p: benchmarks::ThroughputPingPongRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        grpc::SingleResponse::completed(not_implemented())
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
        grpc::SingleResponse::completed(not_implemented())
    }

    fn streaming_windows(
        &self,
        _o: grpc::RequestOptions,
        _p: benchmarks::StreamingWindowsRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        grpc::SingleResponse::completed(not_implemented())
    }

    fn fibonacci(
        &self,
        _o: grpc::RequestOptions,
        p: benchmarks::FibonacciRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        println!("Got fibonacci req: {:?}", p);
        grpc::SingleResponse::completed(not_implemented())
    }

    fn chameneos(
        &self,
        _o: grpc::RequestOptions,
        p: benchmarks::ChameneosRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        println!("Got chameneos req: {:?}", p);
        let f = run_async(move || {
            let b = bench::chameneos::mixed_chameneos::Chameneos::default();
            run(&b, &p).into()
        })
        .map_err(|e| {
            println!("Converting benchmark error into grpc error: {:?}", e);
            e.into()
        });
        grpc::SingleResponse::no_metadata(f)
    }

    fn all_pairs_shortest_path(
        &self,
        _o: grpc::RequestOptions,
        p: benchmarks::APSPRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        println!("Got APSP req: {:?}", p);
        grpc::SingleResponse::completed(not_implemented())
    }

    fn atomic_broadcast(
        &self,
        _o: grpc::RequestOptions,
        _p: benchmarks::AtomicBroadcastRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        grpc::SingleResponse::completed(not_implemented())
    }

    fn sized_throughput(
        &self,
        _o: grpc::RequestOptions,
        _p: benchmarks::SizedThroughputRequest,
    ) -> grpc::SingleResponse<messages::TestResult> {
        grpc::SingleResponse::completed(not_implemented())
    }
}
