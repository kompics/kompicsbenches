use crate::{
    benchmark::{Benchmark, BenchmarkError, BenchmarkInstance, *},
    benchmark_master::ClientEntry,
    kompics_benchmarks::{messages},
};
use futures::future::{self, Future};
//use slog::{crit, debug, error, info, o, warn, Drain, Logger};
use time;

pub fn run_async<F>(f: F) -> impl Future<Item = messages::TestResult, Error = BenchmarkError>
where F: FnOnce() -> messages::TestResult + std::panic::UnwindSafe {
    let lf = future::lazy(|| {
        let r = f();
        future::ok::<messages::TestResult, ()>(r)
    });
    let fcu = lf.catch_unwind();
    let fe = fcu.map_err(|_| BenchmarkError::Panic);
    let f = fe.map(|r: Result<_, _>| match r {
        Ok(tm) => tm,
        Err(_) => {
            let msg = "Something went wrong...".to_string();
            let mut tf = messages::TestFailure::new();
            tf.set_reason(msg);
            let mut rm = messages::TestResult::new();
            rm.set_failure(tf);
            rm
        },
    });
    f
}

pub fn not_implemented() -> messages::TestResult {
    let ni = messages::NotImplemented::new();
    let mut rm = messages::TestResult::new();
    rm.set_not_implemented(ni);
    rm
}

pub(crate) const MIN_RUNS: usize = 20;
pub(crate) const MAX_RUNS: usize = 100;
pub(crate) const RSE_TARGET: f64 = 0.1; // 10% RSE
pub(crate) const NS_TO_MS: f64 = 1.0 / (1000.0 * 1000.0);

pub fn run<B: Benchmark>(_b: &B, c: &B::Conf) -> Result<Vec<f64>, BenchmarkError> {
    let mut bi = B::new_instance();
    bi.setup(c);
    let mut results = Vec::with_capacity(MIN_RUNS);
    let mut n_runs = 0;
    bi.prepare_iteration();
    results.push(measure(|| bi.run_iteration()));
    n_runs += 1;
    // run at least 20 to be able to calculate RSE
    while n_runs < MIN_RUNS {
        bi.cleanup_iteration(false, *results.last().unwrap());
        bi.prepare_iteration();
        results.push(measure(|| bi.run_iteration()));
        n_runs += 1;
    }
    while (n_runs < MAX_RUNS) && (rse(&results) > RSE_TARGET) {
        bi.cleanup_iteration(false, *results.last().unwrap());
        bi.prepare_iteration();
        results.push(measure(|| bi.run_iteration()));
        n_runs += 1;
    }
    bi.cleanup_iteration(true, *results.last().unwrap());
    let result_rse = rse(&results);
    if result_rse > RSE_TARGET {
        let msg = format!(
            "RSE target of {}% was not met by value {}% after {} runs!",
            RSE_TARGET * 100.0,
            result_rse * 100.0,
            n_runs
        );
        eprintln!("{}", msg);
        Err(BenchmarkError::RSETargetNotMet(msg))
    } else {
        Ok(results)
    }
    // });
    // match res {
    //     Ok(Ok(r)) => Ok(r),
    //     Ok(Err(b)) => Err(b),
    //     Err(_) => Err(BenchmarkError::Panic),
    // }
}

impl From<Result<Vec<f64>, BenchmarkError>> for messages::TestResult {
    fn from(res: Result<Vec<f64>, BenchmarkError>) -> Self {
        match res {
            Ok(data) => {
                let len = data.len();
                let mut ts = messages::TestSuccess::new();
                ts.set_number_of_runs(len as u32);
                ts.set_run_results(data);
                let mut rm = messages::TestResult::new();
                rm.set_success(ts);
                rm
            },
            Err(e) => {
                let msg = format!("{:?}", e);
                let mut tf = messages::TestFailure::new();
                tf.set_reason(msg);
                let mut rm = messages::TestResult::new();
                rm.set_failure(tf);
                rm
            },
        }
    }
}

pub(crate) fn measure<F>(f: F) -> f64
where F: FnOnce() -> () {
    let start = time::precise_time_ns();
    f();
    let end = time::precise_time_ns();
    let diff = (end - start) as f64;
    let diff_millis = diff * NS_TO_MS;
    diff_millis
}

pub(crate) fn rse(l: &Vec<f64>) -> f64 { l.relative_error_mean() }

trait Stats {
    fn sample_size(&self) -> f64;
    fn sum(&self) -> f64;
    fn sample_mean(&self) -> f64 { self.sum() / self.sample_size() }
    fn sample_variance(&self) -> f64;
    fn sample_standard_deviation(&self) -> f64 { self.sample_variance().sqrt() }
    fn standard_error_mean(&self) -> f64 {
        let sample_size = self.sample_size();
        let ssd = self.sample_standard_deviation();
        ssd / sample_size.sqrt()
    }
    fn relative_error_mean(&self) -> f64 {
        let sem = self.standard_error_mean();
        let mean = self.sample_mean();
        sem / mean
    }
}

impl<'a> Stats for &'a Vec<f64> {
    fn sample_size(&self) -> f64 { self.len() as f64 }

    fn sum(&self) -> f64 { self.iter().fold(0.0, |acc, v| acc + v) }

    fn sample_variance(&self) -> f64 {
        let sample_mean = self.sample_mean();
        let sum = self.iter().fold(0.0, |acc, sample| {
            let err = sample - sample_mean;
            acc + (err * err)
        });
        sum / (self.sample_size() - 1.0)
    }
}

pub(crate) struct DistributedIteration {
    master:        Box<dyn AbstractBenchmarkMaster>,
    client_data_l: Vec<(ClientEntry, ClientDataHolder)>,
    n_runs:        usize,
    results:       Vec<f64>,
}

impl DistributedIteration {
    pub(crate) fn new(
        master: Box<dyn AbstractBenchmarkMaster>,
        client_data_l: Vec<(ClientEntry, ClientDataHolder)>,
    ) -> DistributedIteration
    {
        DistributedIteration { master, client_data_l, n_runs: 0, results: Vec::new() }
    }

    pub(crate) fn n_runs(&self) -> usize { self.n_runs }

    pub(crate) fn results(self) -> Vec<f64> { self.results }

    pub fn prepare(mut self) -> Self {
        self.master
            .prepare_iteration(self.client_data_l.iter().map(|(_, d)| d.clone()).collect())
            .expect("prepare failed!");
        self
    }

    pub fn run(mut self) -> Self {
        let res = measure(|| self.master.run_iteration());
        self.results.push(res);
        self.n_runs += 1;
        self
    }

    pub fn cleanup(mut self) -> impl Future<Item = (Self, bool), Error = grpc::Error> {
        let clients: Vec<_> = self.client_data_l.iter().map(|(c, _)| c.clone()).collect();
        let is_final: bool = if (self.n_runs < MIN_RUNS)
            || (self.n_runs < MAX_RUNS) && (rse(&self.results) > RSE_TARGET)
        {
            self.master.cleanup_iteration(false, *self.results.last().unwrap());
            false
        } else {
            self.master.cleanup_iteration(true, *self.results.last().unwrap());
            true
        };
        let fl = clients.into_iter().map(move |c| c.cleanup(is_final));
        let f = future::join_all(fl);
        f.map(move |_| (self, is_final))
    }
}
