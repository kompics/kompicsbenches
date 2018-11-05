use super::*;

//use std::panic::catch_unwind;
use time;

pub trait Benchmark {
    type Conf;
    fn setup(&mut self, c: &Self::Conf) -> ();
    fn prepare_iteration(&mut self) -> () {}
    fn run_iteration(&mut self) -> ();
    fn cleanup_iteration(&mut self, _last_iteration: bool, _exec_time_millis: f64) -> () {}
}

const MIN_RUNS: usize = 20;
const MAX_RUNS: usize = 100;
const RSE_TARGET: f64 = 0.1; // 10% RSE
const NS_TO_MS: f64 = 1.0 / (1000.0 * 1000.0);

pub fn run<B: Benchmark>(b: &mut B, c: &B::Conf) -> Result<Vec<f64>, BenchmarkError> {
    //    let res = catch_unwind(move || {
    b.setup(c);
    let mut results = Vec::with_capacity(MIN_RUNS);
    let mut n_runs = 0;
    b.prepare_iteration();
    results.push(measure(|| b.run_iteration()));
    n_runs += 1;
    // run at least 20 to be able to calculate RSE
    while n_runs < MIN_RUNS {
        b.cleanup_iteration(false, *results.last().unwrap());
        b.prepare_iteration();
        results.push(measure(|| b.run_iteration()));
        n_runs += 1;
    }
    while (n_runs < MAX_RUNS) && (rse(&results) > RSE_TARGET) {
        b.cleanup_iteration(false, *results.last().unwrap());
        b.prepare_iteration();
        results.push(measure(|| b.run_iteration()));
        n_runs += 1;
    }
    b.cleanup_iteration(true, *results.last().unwrap());
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
            }
            Err(e) => {
                let msg = format!("{:?}", e);
                let mut tf = messages::TestFailure::new();
                tf.set_reason(msg);
                let mut rm = messages::TestResult::new();
                rm.set_failure(tf);
                rm
            }
        }
    }
}

fn measure<F>(f: F) -> f64
where
    F: FnOnce() -> (),
{
    let start = time::precise_time_ns();
    f();
    let end = time::precise_time_ns();
    let diff = (end - start) as f64;
    let diff_millis = diff * NS_TO_MS;
    diff_millis
}

fn rse(l: &Vec<f64>) -> f64 {
    l.relative_error_mean()
}

#[derive(Debug)]
pub enum BenchmarkError {
    RSETargetNotMet(String),
    Panic,
}

trait Stats {
    fn sample_size(&self) -> f64;
    fn sum(&self) -> f64;
    fn sample_mean(&self) -> f64 {
        self.sum() / self.sample_size()
    }
    fn sample_variance(&self) -> f64;
    fn sample_standard_deviation(&self) -> f64 {
        self.sample_variance().sqrt()
    }
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
    fn sample_size(&self) -> f64 {
        self.len() as f64
    }
    fn sum(&self) -> f64 {
        self.iter().fold(0.0, |acc, v| acc + v)
    }
    fn sample_variance(&self) -> f64 {
        let sample_mean = self.sample_mean();
        let sum = self.iter().fold(0.0, |acc, sample| {
            let err = sample - sample_mean;
            acc + (err * err)
        });
        sum / (self.sample_size() - 1.0)
    }
}
