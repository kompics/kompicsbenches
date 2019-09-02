#![feature(unsized_locals)]
#![feature(impl_trait_in_bindings)]
pub mod benchmark;
pub mod benchmark_client;
pub mod benchmark_master;
pub mod benchmark_runner;
pub mod kompics_benchmarks;

pub use self::benchmark::*;
use self::kompics_benchmarks::*;
#[allow(unused_imports)]
use slog::{crit, debug, error, info, o, warn, Drain, Logger};
use slog_scope;
use slog_stdlog;
use std::{
    net::{IpAddr, SocketAddr},
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
        H: benchmarks_grpc::BenchmarkRunner + 'static + Sync + Send + 'static,
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
    use std::{clone::Clone, net::SocketAddr};

    pub fn test_implementation<F>(benchmarks: Box<F>)
    where F: BenchmarkFactory + Clone + 'static {
        let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
        let logger = Logger::root(slog_term::FullFormat::new(plain).build().fuse(), o!());

        info!(logger, "The root logger works!");

        let _scope_guard = slog_scope::set_global_logger(logger.clone());
        let _log_guard = slog_stdlog::init().unwrap();

        let runner_addr: SocketAddr = "127.0.0.1:45678".parse().expect("runner address");
        let master_addr: SocketAddr = "127.0.0.1:45679".parse().expect("master address");
        let client_addr: SocketAddr = "127.0.0.1:45680".parse().expect("client address");

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

        let num_clients = 1;

        let benchmarks1 = benchmarks.clone();
        let benchmarks2 = benchmarks.clone();

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

        let client_logger = logger.clone();

        let client_handle = std::thread::Builder::new()
            .name("benchmark_client".to_string())
            .spawn(move || {
                info!(client_logger, "Starting client");
                benchmark_client::run(
                    client_addr.ip(),
                    client_addr.port(),
                    master_addr.ip(),
                    master_addr.port(),
                    benchmarks2,
                    client_logger.new(o!("ty" => "benchmark_client::run")),
                );
                info!(client_logger, "Finished client");
            })
            .expect("client thread");

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
            let res = ready_f.drop_metadata().wait().expect("ready result");
            if res.status {
                info!(logger, "Was ready.");
                break;
            } else {
                info!(logger, "Wasn't ready, yet.");
                std::thread::sleep(Duration::from_millis(500));
            }
        }

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

        info!(logger, "Sending shutdown request to master");
        let mut sreq = messages::ShutdownRequest::new();
        sreq.set_force(false);
        let _shutdownres_f =
            bench_stub.shutdown(grpc::RequestOptions::default(), sreq).drop_metadata();

        // // No way to shut these down...just drop
        // drop(master_handle);
        // drop(client_handle);

        info!(logger, "Waiting for master to finish...");
        master_handle.join().expect("Master panicked!");
        info!(logger, "Master is done.");
        info!(logger, "Waiting for client to finish...");
        client_handle.join().expect("Client panicked!");
        info!(logger, "Client is done.");

        info!(
            logger,
            "
%%%%%%%%%%%%%
%% SUMMARY %%
%%%%%%%%%%%%%
{} tests implemented: {:?}
{} tests not implemented: {:?}
",
            implemented.len(),
            implemented,
            not_implemented.len(),
            not_implemented
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_utils::test_implementation;

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
        ) -> Result<Self::ClientConf, BenchmarkError>
        {
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
            Err(NotImplementedError::FutureWork)
        }
    }

    #[test]
    fn test_client_master() {
        let benchmarks = Box::new(TestFactory {});
        test_implementation(benchmarks);
    }
}
