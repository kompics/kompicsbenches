use super::*;
use benchmark_suite_shared::benchmark::*;

pub mod pingpong;
pub mod throughput_pingpong;

pub fn factory() -> Box<dyn BenchmarkFactory> {
    Box::new(Factory {})
}

#[derive(Clone, Debug, PartialEq)]
pub struct Factory;
impl BenchmarkFactory for Factory {
    fn by_label(&self, label: &str) -> Result<AbstractBench, NotImplementedError> {
        match label {
            pingpong::PingPong::LABEL => self.ping_pong().map_into(),
            throughput_pingpong::PingPong::LABEL => self.throughput_ping_pong().map_into(),
            _ => Err(NotImplementedError::NotFound),
        }
    }

    fn box_clone(&self) -> Box<dyn BenchmarkFactory> {
        Box::new(Factory {})
    }

    fn ping_pong(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
        Ok(pingpong::PingPong.into())
    }
    fn net_ping_pong(&self) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
        Err(NotImplementedError::NotImplementable)
    }

    fn throughput_ping_pong(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
        Ok(throughput_pingpong::PingPong.into())
    }
    fn net_throughput_ping_pong(
        &self,
    ) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
        Err(NotImplementedError::NotImplementable)
    }

    fn atomic_register(
        &self,
    ) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
        Err(NotImplementedError::NotImplementable)
    }

    fn streaming_windows(
        &self,
    ) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
        Err(NotImplementedError::NotImplementable)
    }
}
