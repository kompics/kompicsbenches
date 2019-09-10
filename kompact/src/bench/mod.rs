use super::*;
use benchmark_suite_shared::benchmark::*;
use std::time::Duration;

pub mod atomicregister;
pub mod net_throughput_pingpong;
pub mod netpingpong;
pub mod pingpong;
pub mod throughput_pingpong;

#[derive(Clone, Debug)]
pub struct Start;
pub const START: Start = Start;

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
}
