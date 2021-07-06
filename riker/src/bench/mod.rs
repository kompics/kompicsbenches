use super::*;
use benchmark_suite_shared::benchmark::*;

pub mod all_pairs_shortest_path;
pub mod chameneos;
pub mod fibonacci;
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

    fn fibonacci(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
        Ok(fibonacci::Fibonacci {}.into())
    }
    fn chameneos(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
        Ok(chameneos::Chameneos {}.into())
    }

    fn all_pairs_shortest_path(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
        Ok(all_pairs_shortest_path::AllPairsShortestPath {}.into())
    }

    fn atomic_broadcast(
        &self,
    ) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
        Err(NotImplementedError::FutureWork)
    }


    fn sized_throughput(
        &self,
    ) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
        Err(NotImplementedError::NotImplementable)
    }
}
