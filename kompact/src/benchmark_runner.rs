use super::*;

fn run<F>(f: F) -> impl Future<Item = messages::TestResult, Error = grpc::Error>
where
    F: FnOnce() -> messages::TestResult + std::panic::UnwindSafe,
{
    let lf = future::lazy(|| {
        let r = f();
        future::ok::<messages::TestResult, ()>(r)
    });
    let fcu = lf.catch_unwind();
    let fe = fcu.map_err(|_| grpc::Error::Panic("Benchmark panicked!".to_string()));
    let f = fe.map(|r: Result<_, _>| match r {
        Ok(tm) => tm,
        Err(_) => {
            let msg = "Something went wrong...".to_string();
            let mut tf = messages::TestFailure::new();
            tf.set_reason(msg);
            let mut rm = messages::TestResult::new();
            rm.set_failure(tf);
            rm
        }
    });
    f
}

fn not_implemented() -> messages::TestResult {
    let ni = messages::NotImplemented::new();
    let mut rm = messages::TestResult::new();
    rm.set_not_implemented(ni);
    rm
}

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
        let f = run(move || {
            let mut b = bench::pingpong::actor_pingpong::PingPong::new();
            benchmark::run(&mut b, &p).into()
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
        let f = run(move || {
            let mut b = bench::pingpong::component_pingpong::PingPong::new();
            benchmark::run(&mut b, &p).into()
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
