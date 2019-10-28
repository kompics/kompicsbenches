use super::*;
use benchmark_suite_shared::benchmark::*;
use std::time::Duration;

pub mod all_pairs_shortest_path;
pub mod atomicregister;
pub mod chameneos;
pub mod fibonacci;
mod messages;
pub mod net_throughput_pingpong;
pub mod netpingpong;
pub mod pingpong;
pub mod streaming_windows;
pub mod throughput_pingpong;

pub trait ReceiveRun {
    fn recipient(&self) -> kompact::prelude::Recipient<&'static messages::Run>;
}

pub fn component() -> Box<dyn BenchmarkFactory> {
    Box::new(ComponentFactory {})
}
#[derive(Clone, Debug, PartialEq)]
pub struct ComponentFactory;
impl BenchmarkFactory for ComponentFactory {
    fn by_label(&self, label: &str) -> Result<AbstractBench, NotImplementedError> {
        match label {
            pingpong::component_pingpong::PingPong::LABEL => self.ping_pong().map_into(),
            throughput_pingpong::component_pingpong::PingPong::LABEL => {
                self.throughput_ping_pong().map_into()
            }
            _ => Err(NotImplementedError::NotFound),
        }
    }

    fn box_clone(&self) -> Box<dyn BenchmarkFactory> {
        Box::new(ComponentFactory {})
    }

    fn ping_pong(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
        Ok(pingpong::component_pingpong::PingPong {}.into())
    }
    fn net_ping_pong(&self) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
        Err(NotImplementedError::NotImplementable)
    }

    fn throughput_ping_pong(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
        Ok(throughput_pingpong::component_pingpong::PingPong {}.into())
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
        Err(NotImplementedError::NotImplementable)
    }
    fn chameneos(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
        Err(NotImplementedError::NotImplementable)
    }
    fn all_pairs_shortest_path(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
        Ok(all_pairs_shortest_path::component_apsp::AllPairsShortestPath {}.into())
    }
}

pub fn actor() -> Box<dyn BenchmarkFactory> {
    Box::new(ActorFactory {})
}
#[derive(Clone, Debug, PartialEq)]
pub struct ActorFactory;
impl BenchmarkFactory for ActorFactory {
    fn by_label(&self, label: &str) -> Result<AbstractBench, NotImplementedError> {
        match label {
            pingpong::actor_pingpong::PingPong::LABEL => self.ping_pong().map_into(),
            netpingpong::PingPong::LABEL => self.net_ping_pong().map_into(),
            throughput_pingpong::actor_pingpong::PingPong::LABEL => {
                self.throughput_ping_pong().map_into()
            }
            net_throughput_pingpong::PingPong::LABEL => self.net_throughput_ping_pong().map_into(),
            atomicregister::actor_atomicregister::AtomicRegister::LABEL => {
                self.atomic_register().map_into()
            }
            streaming_windows::StreamingWindows::LABEL => self.streaming_windows().map_into(),
            _ => Err(NotImplementedError::NotFound),
        }
    }

    fn box_clone(&self) -> Box<dyn BenchmarkFactory> {
        Box::new(ActorFactory {})
    }

    fn ping_pong(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
        Ok(pingpong::actor_pingpong::PingPong {}.into())
    }
    fn net_ping_pong(&self) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
        Ok(netpingpong::PingPong {}.into())
    }

    fn throughput_ping_pong(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
        Ok(throughput_pingpong::actor_pingpong::PingPong {}.into())
    }
    fn net_throughput_ping_pong(
        &self,
    ) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
        Ok(net_throughput_pingpong::PingPong {}.into())
    }

    fn atomic_register(
        &self,
    ) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
        Ok(atomicregister::actor_atomicregister::AtomicRegister {}.into())
    }
    fn streaming_windows(
        &self,
    ) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
        Ok(streaming_windows::StreamingWindows {}.into())
    }
    fn fibonacci(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
        Ok(fibonacci::Fibonacci {}.into())
    }
    fn chameneos(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
        Ok(chameneos::actor_chameneos::Chameneos {}.into())
    }

    fn all_pairs_shortest_path(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
        Ok(all_pairs_shortest_path::actor_apsp::AllPairsShortestPath {}.into())
    }
}
pub fn mixed() -> Box<dyn BenchmarkFactory> {
    Box::new(MixedFactory {})
}
#[derive(Clone, Debug, PartialEq)]
pub struct MixedFactory;
impl BenchmarkFactory for MixedFactory {
    fn by_label(&self, label: &str) -> Result<AbstractBench, NotImplementedError> {
        match label {
            atomicregister::mixed_atomicregister::AtomicRegister::LABEL => {
                self.atomic_register().map_into()
            }
            _ => Err(NotImplementedError::NotFound),
        }
    }

    fn box_clone(&self) -> Box<dyn BenchmarkFactory> {
        Box::new(MixedFactory {})
    }

    fn ping_pong(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
        Err(NotImplementedError::NotImplementable)
    }
    fn net_ping_pong(&self) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
        Err(NotImplementedError::NotImplementable)
    }

    fn throughput_ping_pong(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
        Err(NotImplementedError::NotImplementable)
    }
    fn net_throughput_ping_pong(
        &self,
    ) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
        Err(NotImplementedError::NotImplementable)
    }

    fn atomic_register(
        &self,
    ) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
        Ok(atomicregister::mixed_atomicregister::AtomicRegister {}.into())
    }
    fn streaming_windows(
        &self,
    ) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
        Err(NotImplementedError::NotImplementable)
    }
    fn fibonacci(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
        Err(NotImplementedError::NotImplementable)
    }

    fn chameneos(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
        Ok(chameneos::mixed_chameneos::Chameneos {}.into())
    }
    fn all_pairs_shortest_path(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
        Err(NotImplementedError::FutureWork)
    }
}
