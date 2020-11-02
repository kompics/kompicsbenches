#![feature(array_map)]
#![feature(unsized_locals)]
#![feature(impl_trait_in_bindings)]
pub mod benchmark;
pub mod benchmark_client;
pub mod benchmark_master;
pub mod benchmark_runner;
pub mod helpers;
pub mod kompics_benchmarks;

pub use self::benchmark::*;
use self::kompics_benchmarks::*;
#[allow(unused_imports)]
use slog::{crit, debug, error, info, o, warn, Drain, Logger};
use slog_scope;
use slog_stdlog;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    thread,
    time::Duration,
};
//pub(crate) type BenchLogger = Logger;
pub struct BenchmarkMain;
impl BenchmarkMain {
    pub fn run_with<H, F>(
        args: Vec<String>,
        runner: H,
        benchmarks: Box<dyn BenchmarkFactory>,
        set_public_if: F,
    ) -> ()
    where
        H: benchmarks_grpc::BenchmarkRunner + Clone + Sync + Send + 'static,
        F: FnOnce(IpAddr),
    {
        let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
        let logger = Logger::root(slog_term::FullFormat::new(plain).build().fuse(), o!());

        info!(logger, "The root logger works!");

        let _scope_guard = slog_scope::set_global_logger(logger.clone());
        let _log_guard = slog_stdlog::init().unwrap();

        if args.len() <= 2 {
            // local mode
            let bench_runner_addr: String =
                args.get(1).map(|s| s.clone()).unwrap_or("127.0.0.1:45678".to_string());

            benchmark_runner::run_server(runner, bench_runner_addr, None)
        } else if args.len() == 3 {
            // client mode
            let master_addr: SocketAddr =
                args[1].parse().expect("Could not convert arg to socket address!");
            let client_addr: SocketAddr =
                args[2].parse().expect("Could not convert arg to socket address!");
            println!("Running in client mode with master={}, client={}", master_addr, client_addr);
            set_public_if(client_addr.ip());
            benchmark_client::run(
                client_addr.ip(),
                client_addr.port(),
                master_addr.ip(),
                master_addr.port(),
                benchmarks,
                logger.new(o!("ty" => "benchmark_client::run")),
            );
            unreachable!("This should not return!");
        } else if args.len() == 4 {
            // master mode
            let bench_runner_addr: SocketAddr =
                args[1].parse().expect("Could not convert arg to socket address!");
            let master_addr: SocketAddr =
                args[2].parse().expect("Could not convert arg to socket address!");
            let num_clients: usize = args[3]
                .parse()
                .expect("Could not convert arg to unsigned integer number of clients.");
            println!(
                "Running in master mode with runner={}, master={}, #clients={}",
                bench_runner_addr, master_addr, num_clients
            );
            set_public_if(master_addr.ip());
            benchmark_master::run(
                bench_runner_addr.port(),
                master_addr.port(),
                num_clients,
                benchmarks,
                logger.new(o!("ty" => "benchmark_master::run")),
            );
            unreachable!("This should not return!");
        } else {
            panic!("Too many args={} provided!", args.len());
        };
    }
}

fn force_shutdown() {
    std::thread::spawn(|| {
        thread::sleep(Duration::from_millis(500));
        std::process::exit(0);
    });
}

pub mod test_utils {
    use super::*;
    use benchmarks_grpc::BenchmarkRunner;
    use futures::future::Future;
    use grpc::ClientStubExt;
    use itertools::Itertools;
    use std::{
        clone::Clone,
        net::SocketAddr,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
    };

    pub fn test_implementation<F>(benchmarks: Box<F>)
    where F: BenchmarkFactory + Clone + 'static {
        let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
        let logger = Logger::root(slog_term::FullFormat::new(plain).build().fuse(), o!());

        info!(logger, "The root logger works!");

        let scope_guard = slog_scope::set_global_logger(logger.clone());
        scope_guard.cancel_reset(); // prevent one test removing the other's logger when running in parallel
        let _ = slog_stdlog::init(); // ignore the error if the other implementation already set the logger

        let runner_addr: SocketAddr = "127.0.0.1:45678".parse().expect("runner address");
        let master_addr: SocketAddr = "127.0.0.1:45679".parse().expect("master address");
        let client_ports: [u16; 4] = [45680, 45681, 45682, 45683];
        let client_addrs: [SocketAddr; 4] =
            client_ports.map(|port| SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port));

        let mut implemented: Vec<String> = Vec::new();
        let mut not_implemented: Vec<String> = Vec::new();

        let mut check_result = |label: &str, tr: messages::TestResult| {
            if tr.has_success() {
                let s = tr.get_success();
                assert_eq!(s.run_results.len(), s.number_of_runs as usize);
                implemented.push(label.into());
            } else if tr.has_failure() {
                let f = tr.get_failure();
                assert!(
                    f.reason.contains("RSE"),
                    "Tests are only allowed to fail because of RSE requirements!"
                ); // since tests are short they may not meet RSE requirements
                implemented.push(label.into());
            } else if tr.has_not_implemented() {
                warn!(logger, "Test {} was not implemented", label);
                not_implemented.push(label.into());
            } else {
                panic!("Unexpected test result: {:?}", tr);
            }
        };

        let num_clients = 4;

        let benchmarks1 = benchmarks.clone();

        let master_logger = logger.clone();

        let master_handle = std::thread::Builder::new()
            .name("benchmark_master".to_string())
            .spawn(move || {
                info!(master_logger, "Starting master");
                benchmark_master::run(
                    runner_addr.port(),
                    master_addr.port(),
                    num_clients,
                    benchmarks1,
                    master_logger.new(o!("ty" => "benchmark_master::run")),
                );
                info!(master_logger, "Finished master");
            })
            .expect("master thread");

        let client_handles = client_addrs.map(|client_addr_ref| {
            let client_addr = client_addr_ref.clone();
            let bench = benchmarks.clone();
            let client_logger = logger.clone();
            let thread_name = format!("benchmark_client-{}", client_addr.port());
            let client_handle = std::thread::Builder::new()
                .name(thread_name)
                .spawn(move || {
                    info!(client_logger, "Starting client {}", client_addr);
                    benchmark_client::run(
                        client_addr.ip(),
                        client_addr.port(),
                        master_addr.ip(),
                        master_addr.port(),
                        bench,
                        client_logger
                            .new(o!("ty" => "benchmark_client::run", "addr" => client_addr)),
                    );
                    info!(client_logger, "Finished client {}", client_addr);
                })
                .expect("client thread");
            client_handle
        });

        let bench_stub = benchmarks_grpc::BenchmarkRunnerClient::new_plain(
            &runner_addr.ip().to_string(),
            runner_addr.port(),
            Default::default(),
        )
        .expect("bench stub");

        let mut attempts = 0;
        while attempts < 20 {
            attempts += 1;
            info!(logger, "Checking if ready, attempt #{}", attempts);
            let ready_f =
                bench_stub.ready(grpc::RequestOptions::default(), messages::ReadyRequest::new());
            match ready_f.drop_metadata().wait() {
                Ok(res) => {
                    if res.status {
                        info!(logger, "Was ready.");
                        break;
                    } else {
                        info!(logger, "Wasn't ready, yet.");
                        std::thread::sleep(Duration::from_millis(500));
                    }
                },
                Err(e) => {
                    info!(logger, "Couldn't connect, yet: {}", e);
                    std::thread::sleep(Duration::from_millis(500));
                },
            }
        }

        /*
         * Ping Pong
         */
        let mut ppr = benchmarks::PingPongRequest::new();
        ppr.set_number_of_messages(100);
        let ppres_f =
            bench_stub.ping_pong(grpc::RequestOptions::default(), ppr.clone()).drop_metadata();
        let ppres = ppres_f.wait().expect("pp result");
        //assert!(ppres.has_success(), "PingPong TestResult should have been a success!");
        check_result("PingPong", ppres);

        let nppres_f =
            bench_stub.net_ping_pong(grpc::RequestOptions::default(), ppr.clone()).drop_metadata();
        let nppres = nppres_f.wait().expect("npp result");
        //assert!(nppres.has_success(), "NetPingPong TestResult should have been a success!");
        check_result("NetPingPong", nppres);

        /*
         * (Net) Throughput Ping Pong
        */
        let mut tppr = benchmarks::ThroughputPingPongRequest::new();
        tppr.set_messages_per_pair(100);
        tppr.set_pipeline_size(10);
        tppr.set_parallelism(2);
        tppr.set_static_only(true);

        let tppres_f = bench_stub
            .throughput_ping_pong(grpc::RequestOptions::default(), tppr.clone())
            .drop_metadata();
        let tppres = tppres_f.wait().expect("tpp result");
        // assert!(
        //     tppres.has_success(),
        //     "ThroughputPingPong (Static) TestResult should have been a success!"
        // );
        check_result("ThroughputPingPong (Static)", tppres);

        let ntppres_f = bench_stub
            .net_throughput_ping_pong(grpc::RequestOptions::default(), tppr.clone())
            .drop_metadata();
        let ntppres = ntppres_f.wait().expect("ntpp result");
        // assert!(
        //     ntppres.has_success(),
        //     "NetThroughputPingPong (Static) TestResult should have been a success!"
        // );
        check_result("NetThroughputPingPong (Static)", ntppres);

        tppr.set_static_only(false);
        let tppres2_f = bench_stub
            .throughput_ping_pong(grpc::RequestOptions::default(), tppr.clone())
            .drop_metadata();
        let tppres2 = tppres2_f.wait().expect("tpp result");
        // assert!(
        //     tppres2.has_success(),
        //     "ThroughputPingPong (GC) TestResult should have been a success!"
        // );
        check_result("ThroughputPingPong (GC)", tppres2);

        let ntppres2_f = bench_stub
            .net_throughput_ping_pong(grpc::RequestOptions::default(), tppr.clone())
            .drop_metadata();
        let ntppres2 = ntppres2_f.wait().expect("ntpp result");
        // assert!(
        //     ntppres2.has_success(),
        //     "NetThroughputPingPong (GC) TestResult should have been a success!"
        // );
        check_result("NetThroughputPingPong (GC)", ntppres2);
        /*
         * Atomic Register
         */
        let mut nnar_request = benchmarks::AtomicRegisterRequest::new();
        nnar_request.set_read_workload(0.5);
        nnar_request.set_write_workload(0.5);
        nnar_request.set_partition_size(3);
        nnar_request.set_number_of_keys(500);

        let nnares_f = bench_stub
            .atomic_register(grpc::RequestOptions::default(), nnar_request)
            .drop_metadata();
        let nnares = nnares_f.wait().expect("nnar result");
        check_result("Atomic Register", nnares);

        /*
         * Streaming Windows
         */
        let mut sw_request = benchmarks::StreamingWindowsRequest::new();
        sw_request.set_batch_size(10);
        sw_request.set_number_of_partitions(2);
        sw_request.set_number_of_windows(2);
        sw_request.set_window_size("10ms".to_string());
        sw_request.set_window_size_amplification(1000);

        let swres_f = bench_stub
            .streaming_windows(grpc::RequestOptions::default(), sw_request)
            .drop_metadata();
        let swres = swres_f.wait().expect("sw result");
        check_result("Streaming Windows", swres);

        /*
         * Fibonacci
         */
        let mut fibr = benchmarks::FibonacciRequest::new();
        fibr.set_fib_number(15);
        let fibres_f = bench_stub.fibonacci(grpc::RequestOptions::default(), fibr).drop_metadata();
        let fibres = fibres_f.wait().expect("fib result");
        check_result("Fibonacci", fibres);

        /*
         * Chameneos
         */
        let mut chamr = benchmarks::ChameneosRequest::new();
        chamr.set_number_of_chameneos(10);
        chamr.set_number_of_meetings(100);
        let chamres_f =
            bench_stub.chameneos(grpc::RequestOptions::default(), chamr).drop_metadata();
        let chamres = chamres_f.wait().expect("cham result");
        check_result("Chameneos", chamres);

        /*
         * All-Pairs Shortest Path
         */
        let mut apspr = benchmarks::APSPRequest::new();
        apspr.set_number_of_nodes(12);
        apspr.set_block_size(4);
        let apspres_f = bench_stub
            .all_pairs_shortest_path(grpc::RequestOptions::default(), apspr)
            .drop_metadata();
        let apspres = apspres_f.wait().expect("apsp result");
        check_result("AllPairsShortestPath", apspres);

        /*
         * Sized Throughput
         */
        let mut stp = benchmarks::SizedThroughputRequest::new();
        stp.set_message_size(100);
        stp.set_batch_size(10);
        stp.set_number_of_batches(2);
        stp.set_number_of_pairs(2);
        let stpres_f = bench_stub
            .sized_throughput(grpc::RequestOptions::default(), stp.clone())
            .drop_metadata();
        let stpres = stpres_f.wait().expect("tpp result");
        assert!(
            stpres.has_success(),
            "ThroughputPingPong (Static) TestResult should have been a success!"
        );
        check_result("SizedThroughput", stpres);


        info!(logger, "Sending shutdown request to master");
        let mut sreq = messages::ShutdownRequest::new();
        sreq.set_force(false);
        let _shutdownres_f =
            bench_stub.shutdown(grpc::RequestOptions::default(), sreq).drop_metadata();

        info!(logger, "Waiting for master to finish...");
        master_handle.join().expect("Master panicked!");
        info!(logger, "Master is done.");
        info!(logger, "Waiting for all clients to finish...");
        match client_handles {
            [ch0, ch1, ch2, ch3] => {
                ch0.join().expect("Client panicked!");
                ch1.join().expect("Client panicked!");
                ch2.join().expect("Client panicked!");
                ch3.join().expect("Client panicked!");
            },
        }
        info!(logger, "All clients are done.");

        info!(
            logger,
            "
%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% MASTER-CLIENT SUMMARY %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%
{} tests implemented: {:?}
{} tests not implemented: {:?}
",
            implemented.len(),
            implemented,
            not_implemented.len(),
            not_implemented
        );
    }

    pub fn test_local_implementation<H>(runner: H)
    where H: benchmarks_grpc::BenchmarkRunner + Clone + Sync + Send + 'static {
        let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
        let logger = Logger::root(slog_term::FullFormat::new(plain).build().fuse(), o!());

        info!(logger, "The root logger works!");

        let scope_guard = slog_scope::set_global_logger(logger.clone());
        scope_guard.cancel_reset(); // prevent one test removing the other's logger when running in parallel
        let _ = slog_stdlog::init(); // ignore the error if the other implementation already set the logger

        let runner_addr: SocketAddr = "127.0.0.1:45677".parse().expect("runner address");

        let mut implemented: Vec<String> = Vec::new();
        let mut not_implemented: Vec<String> = Vec::new();

        let mut check_result = |label: &str, tr: messages::TestResult| {
            if tr.has_success() {
                let s = tr.get_success();
                assert_eq!(s.run_results.len(), s.number_of_runs as usize);
                implemented.push(label.into());
            } else if tr.has_failure() {
                let f = tr.get_failure();
                assert!(
                    f.reason.contains("RSE"),
                    "Tests are only allowed to fail because of RSE requirements!"
                ); // since tests are short they may not meet RSE requirements
                implemented.push(label.into());
            } else if tr.has_not_implemented() {
                warn!(logger, "Test {} was not implemented", label);
                not_implemented.push(label.into());
            } else {
                panic!("Unexpected test result: {:?}", tr);
            }
        };

        let runner_logger = logger.clone();
        let runner_shutdown = Arc::new(AtomicBool::new(false));
        let runner_shutdown2 = runner_shutdown.clone();

        let runner_handle = std::thread::Builder::new()
            .name("benchmark_runner".to_string())
            .spawn(move || {
                info!(runner_logger, "Starting runner");
                benchmark_runner::run_server(
                    runner,
                    runner_addr.to_string(),
                    Some(runner_shutdown2),
                );
                info!(runner_logger, "Finished runner");
            })
            .expect("runner thread");

        let bench_stub = benchmarks_grpc::BenchmarkRunnerClient::new_plain(
            &runner_addr.ip().to_string(),
            runner_addr.port(),
            Default::default(),
        )
        .expect("bench stub");

        let mut attempts = 0;
        while attempts < 20 {
            attempts += 1;
            info!(logger, "Checking if ready, attempt #{}", attempts);
            let ready_f =
                bench_stub.ready(grpc::RequestOptions::default(), messages::ReadyRequest::new());
            match ready_f.drop_metadata().wait() {
                Ok(res) => {
                    if res.status {
                        info!(logger, "Was ready.");
                        break;
                    } else {
                        info!(logger, "Wasn't ready, yet.");
                        std::thread::sleep(Duration::from_millis(500));
                    }
                },
                Err(e) => {
                    info!(logger, "Couldn't connect, yet: {}", e);
                    std::thread::sleep(Duration::from_millis(500));
                },
            }
        }

        /*
         * Ping Pong
         */
        let mut ppr = benchmarks::PingPongRequest::new();
        ppr.set_number_of_messages(100);
        let ppres_f =
            bench_stub.ping_pong(grpc::RequestOptions::default(), ppr.clone()).drop_metadata();
        let ppres = ppres_f.wait().expect("pp result");
        check_result("PingPong", ppres);

        /*
         * Throughput Ping Pong
         */
        let mut tppr = benchmarks::ThroughputPingPongRequest::new();
        tppr.set_messages_per_pair(100);
        tppr.set_pipeline_size(10);
        tppr.set_parallelism(2);
        tppr.set_static_only(true);
        let tppres_f = bench_stub
            .throughput_ping_pong(grpc::RequestOptions::default(), tppr.clone())
            .drop_metadata();
        let tppres = tppres_f.wait().expect("tpp result");
        check_result("ThroughputPingPong (Static)", tppres);

        tppr.set_static_only(false);
        let tppres2_f = bench_stub
            .throughput_ping_pong(grpc::RequestOptions::default(), tppr.clone())
            .drop_metadata();
        let tppres2 = tppres2_f.wait().expect("tpp result");
        // assert!(
        //     tppres2.has_success(),
        //     "ThroughputPingPong (GC) TestResult should have been a success!"
        // );
        check_result("ThroughputPingPong (GC)", tppres2);

        /*
         * Fibonacci
         */
        let mut fibr = benchmarks::FibonacciRequest::new();
        fibr.set_fib_number(15);
        let fibres_f = bench_stub.fibonacci(grpc::RequestOptions::default(), fibr).drop_metadata();
        let fibres = fibres_f.wait().expect("fib result");
        check_result("Fibonacci", fibres);

        /*
         * Chameneos
         */
        let mut chamr = benchmarks::ChameneosRequest::new();
        chamr.set_number_of_chameneos(10);
        chamr.set_number_of_meetings(100);
        let chamres_f =
            bench_stub.chameneos(grpc::RequestOptions::default(), chamr).drop_metadata();
        let chamres = chamres_f.wait().expect("cham result");
        check_result("Chameneos", chamres);

        /*
         * All-Pairs Shortest Path
         */
        let mut apspr = benchmarks::APSPRequest::new();
        apspr.set_number_of_nodes(12);
        apspr.set_block_size(4);
        let apspres_f = bench_stub
            .all_pairs_shortest_path(grpc::RequestOptions::default(), apspr)
            .drop_metadata();
        let apspres = apspres_f.wait().expect("apsp result");
        check_result("AllPairsShortestPath", apspres);

        info!(logger, "Sending shutdown request to runner");
        runner_shutdown.store(true, Ordering::Relaxed);
        runner_handle.thread().unpark();

        info!(logger, "Waiting for runner to finish...");
        runner_handle.join().expect("Runner panicked!");
        info!(logger, "Runner is done.");

        info!(
            logger,
            "
%%%%%%%%%%%%%%%%%%%%%%%%
%% LOCAL TEST SUMMARY %%
%%%%%%%%%%%%%%%%%%%%%%%%
{} tests implemented: {:?}
{} tests not implemented: {:?}
",
            implemented.len(),
            implemented,
            not_implemented.len(),
            not_implemented
        );
    }

    #[derive(Clone, Copy, Debug, PartialEq)]
    pub enum KVOperation {
        ReadInvokation,
        ReadResponse,
        WriteInvokation,
        WriteResponse,
    }

    #[derive(Clone, Copy, Debug)]
    pub struct KVTimestamp {
        pub key:       u64,
        pub operation: KVOperation,
        pub value:     Option<u32>,
        pub time:      i64,
        pub sender:    u32,
    }

    pub fn all_linearizable(timestamps: &Vec<KVTimestamp>) -> bool {
        for (_key, mut trace) in timestamps.into_iter().map(|x| (x.key, *x)).into_group_map() {
            trace.sort_by_key(|x| x.time);
            let mut s: Vec<u32> = vec![0];
            if !is_linearizable(&trace, s.as_mut()) {
                return false;
            }
        }
        true
    }

    fn is_linearizable(h: &Vec<KVTimestamp>, s: &mut Vec<u32>) -> bool {
        match h.is_empty() {
            true => true,
            false => {
                let minimal_ops = get_minimal_ops(h);
                for op in minimal_ops {
                    let response_val = get_response_value(op, h).unwrap();
                    match op.operation {
                        KVOperation::WriteInvokation | KVOperation::WriteResponse => {
                            //                            println!("current value={}, write {}", s.last().unwrap(), response_val);
                            let removed_op_trace = remove_op(op, h);
                            s.push(response_val);
                            if is_linearizable(&removed_op_trace, s) {
                                return true;
                            } else {
                                s.pop();
                            }
                        },
                        KVOperation::ReadInvokation | KVOperation::ReadResponse => {
                            //                            println!("current value={}, read {}", s.last().unwrap(), response_val);
                            let removed_op_trace = remove_op(op, h);
                            if s.last().unwrap() == &response_val
                                && is_linearizable(&removed_op_trace, s)
                            {
                                return true;
                            }
                        },
                    }
                }
                false
            },
        }
    }

    fn get_minimal_ops(trace: &Vec<KVTimestamp>) -> Vec<&KVTimestamp> {
        let mut minimal_ops = Vec::new();
        for entry in trace {
            match entry.operation {
                KVOperation::ReadInvokation | KVOperation::WriteInvokation => {
                    minimal_ops.push(entry)
                },
                _ => break,
            }
        }
        minimal_ops.clone()
    }

    fn get_response_value(invokation: &KVTimestamp, trace: &Vec<KVTimestamp>) -> Option<u32> {
        match invokation.operation {
            KVOperation::ReadInvokation => {
                let f = trace.iter().find(|&&x| {
                    x.operation == KVOperation::ReadResponse && x.sender == invokation.sender
                });
                match f {
                    Some(ts) => ts.value,
                    _ => None,
                }
            },
            _ => invokation.value,
        }
    }

    fn remove_op(entry: &KVTimestamp, trace: &Vec<KVTimestamp>) -> Vec<KVTimestamp> {
        let mut cloned: Vec<KVTimestamp> = trace.clone();
        cloned.retain(|&x| x.sender != entry.sender);
        assert!(cloned.len() < trace.len());
        cloned
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::Future;
    use test_utils::{test_implementation, test_local_implementation};

    #[derive(Default)]
    struct TestLocalBench;
    impl Benchmark for TestLocalBench {
        type Conf = ();
        type Instance = TestLocalBenchI;

        const LABEL: &'static str = "TestBench";

        fn msg_to_conf(_msg: Box<dyn (::protobuf::Message)>) -> Result<Self::Conf, BenchmarkError> {
            Ok(())
        }

        fn new_instance() -> Self::Instance { TestLocalBenchI {} }
    }
    struct TestLocalBenchI;
    impl BenchmarkInstance for TestLocalBenchI {
        type Conf = ();

        fn setup(&mut self, _c: &Self::Conf) -> () {
            println!("Test got setup.");
        }

        fn prepare_iteration(&mut self) -> () {
            println!("Preparing iteration");
        }

        fn run_iteration(&mut self) -> () {
            println!("Running iteration");
        }

        fn cleanup_iteration(&mut self, last_iteration: bool, _exec_time_millis: f64) -> () {
            let last = if last_iteration { "last" } else { "" };
            println!("Cleaning up after {} iteration", last);
        }
    }

    struct TestDistributedBench {
        //_data: std::marker::PhantomData<M>,
    }
    //impl<M> Send for TestDistributedBench<M> {}
    //impl<M> Sync for TestDistributedBench<M> {}
    struct TestDistributedBenchMaster;
    struct TestDistributedBenchClient;

    impl TestDistributedBench {
        fn new() -> TestDistributedBench {
            TestDistributedBench {
                //_data: std::marker::PhantomData
            }
        }
    }

    impl DistributedBenchmark for TestDistributedBench {
        type Client = TestDistributedBenchClient;
        type ClientConf = ();
        type ClientData = ();
        type Master = TestDistributedBenchMaster;
        type MasterConf = ();

        const LABEL: &'static str = "DistTestBench";

        fn new_master() -> Self::Master { TestDistributedBenchMaster {} }

        fn msg_to_master_conf(
            _msg: Box<dyn (::protobuf::Message)>,
        ) -> Result<Self::MasterConf, BenchmarkError> {
            //downcast_msg!(msg; benchmarks::PingPongRequest; |_ppr| ())
            //downcast_msg!(msg; M; |_ppr| ())
            Ok(())
        }

        fn new_client() -> Self::Client { TestDistributedBenchClient {} }

        fn str_to_client_conf(_str: String) -> Result<Self::ClientConf, BenchmarkError> { Ok(()) }

        fn str_to_client_data(_str: String) -> Result<Self::ClientData, BenchmarkError> { Ok(()) }

        fn client_conf_to_str(_c: Self::ClientConf) -> String { "".to_string() }

        fn client_data_to_str(_d: Self::ClientData) -> String { "".to_string() }
    }

    impl DistributedBenchmarkMaster for TestDistributedBenchMaster {
        type ClientConf = ();
        type ClientData = ();
        type MasterConf = ();

        fn setup(
            &mut self,
            _c: Self::MasterConf,
            _m: &DeploymentMetaData,
        ) -> Result<Self::ClientConf, BenchmarkError> {
            println!("Master setting up");
            Ok(())
        }

        fn prepare_iteration(&mut self, _d: Vec<Self::ClientData>) -> () {
            println!("Master preparing iteration");
        }

        fn run_iteration(&mut self) -> () {
            println!("Master running iteration");
        }

        fn cleanup_iteration(&mut self, last_iteration: bool, _exec_time_millis: f64) -> () {
            let last = if last_iteration { "last" } else { "" };
            println!("Master cleaning up {} iteration", last);
        }
    }

    impl DistributedBenchmarkClient for TestDistributedBenchClient {
        type ClientConf = ();
        type ClientData = ();

        fn setup(&mut self, _c: Self::ClientConf) -> Self::ClientData {
            println!("Client setting up");
        }

        fn prepare_iteration(&mut self) -> () {
            println!("Client preparing iteration");
        }

        fn cleanup_iteration(&mut self, last_iteration: bool) -> () {
            let last = if last_iteration { "last" } else { "" };
            println!("Client cleaning up {} iteration", last);
        }
    }

    #[derive(Clone, Debug)]
    struct TestFactory;

    impl BenchmarkFactory for TestFactory {
        fn by_label(&self, label: &str) -> Result<AbstractBench, NotImplementedError> {
            match label {
                TestLocalBench::LABEL => self.ping_pong().map_into(),
                TestDistributedBench::LABEL => self.net_ping_pong().map_into(),
                _ => Err(NotImplementedError::NotFound),
            }
        }

        fn box_clone(&self) -> Box<dyn BenchmarkFactory> { Box::new(TestFactory {}) }

        fn ping_pong(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
            Ok(TestLocalBench {}.into())
        }

        fn net_ping_pong(
            &self,
        ) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
            Ok(TestDistributedBench::new().into())
        }

        fn throughput_ping_pong(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
            Ok(TestLocalBench {}.into())
        }

        fn net_throughput_ping_pong(
            &self,
        ) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
            Ok(TestDistributedBench::new().into())
        }

        fn atomic_register(
            &self,
        ) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
            Ok(TestDistributedBench::new().into())
        }

        fn streaming_windows(
            &self,
        ) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
            Ok(TestDistributedBench::new().into())
        }

        fn fibonacci(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
            Ok(TestLocalBench {}.into())
        }

        fn chameneos(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
            Ok(TestLocalBench {}.into())
        }

        fn all_pairs_shortest_path(
            &self,
        ) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
            Ok(TestLocalBench {}.into())
        }

        fn atomic_broadcast(&self) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
            Ok(TestDistributedBench::new().into())
        }

        fn sized_throughput(
            &self,
        ) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
            Ok(TestDistributedBench::new().into())
        }
    }

    impl benchmarks_grpc::BenchmarkRunner for TestFactory {
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
            let bench = benchmark::BenchmarkFactory::ping_pong(self);
            let f = benchmark_runner::run_async(move || match bench {
                Ok(b) => b.run(Box::new(p)).into(),
                Err(e) => Err(BenchmarkError::NotImplemented(e)).into(),
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
            grpc::SingleResponse::completed(benchmark_runner::not_implemented())
        }

        fn throughput_ping_pong(
            &self,
            _o: grpc::RequestOptions,
            p: benchmarks::ThroughputPingPongRequest,
        ) -> grpc::SingleResponse<messages::TestResult> {
            println!("Got req: {:?}", p);
            let bench = benchmark::BenchmarkFactory::throughput_ping_pong(self);
            let f = benchmark_runner::run_async(move || match bench {
                Ok(b) => b.run(Box::new(p)).into(),
                Err(e) => Err(BenchmarkError::NotImplemented(e)).into(),
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
            grpc::SingleResponse::completed(benchmark_runner::not_implemented())
        }

        fn atomic_register(
            &self,
            _o: grpc::RequestOptions,
            _p: benchmarks::AtomicRegisterRequest,
        ) -> grpc::SingleResponse<messages::TestResult> {
            grpc::SingleResponse::completed(benchmark_runner::not_implemented())
        }

        fn streaming_windows(
            &self,
            _o: grpc::RequestOptions,
            _p: benchmarks::StreamingWindowsRequest,
        ) -> grpc::SingleResponse<messages::TestResult> {
            grpc::SingleResponse::completed(benchmark_runner::not_implemented())
        }

        fn fibonacci(
            &self,
            _o: grpc::RequestOptions,
            p: benchmarks::FibonacciRequest,
        ) -> grpc::SingleResponse<messages::TestResult> {
            println!("Got fibonacci req: {:?}", p);
            let bench = benchmark::BenchmarkFactory::fibonacci(self);
            let f = benchmark_runner::run_async(move || match bench {
                Ok(b) => b.run(Box::new(p)).into(),
                Err(e) => Err(BenchmarkError::NotImplemented(e)).into(),
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
            let bench = benchmark::BenchmarkFactory::chameneos(self);
            let f = benchmark_runner::run_async(move || match bench {
                Ok(b) => b.run(Box::new(p)).into(),
                Err(e) => Err(BenchmarkError::NotImplemented(e)).into(),
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
            let bench = benchmark::BenchmarkFactory::all_pairs_shortest_path(self);
            let f = benchmark_runner::run_async(move || match bench {
                Ok(b) => b.run(Box::new(p)).into(),
                Err(e) => Err(BenchmarkError::NotImplemented(e)).into(),
            })
            .map_err(|e| {
                println!("Converting benchmark error into grpc error: {:?}", e);
                e.into()
            });
            grpc::SingleResponse::no_metadata(f)
        }

        fn sized_throughput(
            &self,
            _o: grpc::RequestOptions,
            _p: benchmarks::SizedThroughputRequest,
        ) -> grpc::SingleResponse<messages::TestResult>
        {
            grpc::SingleResponse::completed(benchmark_runner::not_implemented())
        }

        fn atomic_broadcast(
            &self,
            _o: grpc::RequestOptions,
            _p: benchmarks::AtomicBroadcastRequest,
        ) -> grpc::SingleResponse<messages::TestResult>
        {
            grpc::SingleResponse::completed(benchmark_runner::not_implemented())
        }

    }

    #[test]
    fn test_client_master() {
        let benchmarks = Box::new(TestFactory {});
        test_implementation(benchmarks);
    }

    #[test]
    fn test_local() {
        let runner = TestFactory {};
        test_local_implementation(runner);
    }
}
