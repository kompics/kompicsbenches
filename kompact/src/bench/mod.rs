use super::*;
use benchmark_suite_shared::benchmark::*;
use std::time::Duration;

pub mod netpingpong;
pub mod pingpong;

pub fn component() -> Box<BenchmarkFactory> {
    Box::new(ComponentFactory {})
}
#[derive(Clone, Debug, PartialEq)]
pub struct ComponentFactory;
impl BenchmarkFactory for ComponentFactory {
    fn by_label(&self, label: &str) -> Result<AbstractBench, NotImplementedError> {
        match label {
            pingpong::component_pingpong::PingPong::LABEL => self.pingpong().map_into(),
            _ => Err(NotImplementedError::NotFound),
        }
    }

    fn pingpong(&self) -> Result<Box<AbstractBenchmark>, NotImplementedError> {
        Ok(pingpong::component_pingpong::PingPong {}.into())
    }
    fn netpingpong(&self) -> Result<Box<AbstractDistributedBenchmark>, NotImplementedError> {
        Err(NotImplementedError::NotImplementable)
    }
}

pub fn actor() -> Box<BenchmarkFactory> {
    Box::new(ActorFactory {})
}
#[derive(Clone, Debug, PartialEq)]
pub struct ActorFactory;
impl BenchmarkFactory for ActorFactory {
    fn by_label(&self, label: &str) -> Result<AbstractBench, NotImplementedError> {
        match label {
            pingpong::actor_pingpong::PingPong::LABEL => self.pingpong().map_into(),
            netpingpong::PingPong::LABEL => self.netpingpong().map_into(),
            _ => Err(NotImplementedError::NotFound),
        }
    }

    fn pingpong(&self) -> Result<Box<AbstractBenchmark>, NotImplementedError> {
        Ok(pingpong::actor_pingpong::PingPong {}.into())
    }
    fn netpingpong(&self) -> Result<Box<AbstractDistributedBenchmark>, NotImplementedError> {
        Ok(netpingpong::PingPong {}.into())
    }
}
pub fn mixed() -> Box<BenchmarkFactory> {
    Box::new(MixedFactory {})
}
#[derive(Clone, Debug, PartialEq)]
pub struct MixedFactory;
impl BenchmarkFactory for MixedFactory {
    fn by_label(&self, label: &str) -> Result<AbstractBench, NotImplementedError> {
        match label {
            pingpong::component_pingpong::PingPong::LABEL => self.pingpong().map_into(),
            netpingpong::PingPong::LABEL => self.netpingpong().map_into(),
            _ => Err(NotImplementedError::NotFound),
        }
    }

    fn pingpong(&self) -> Result<Box<AbstractBenchmark>, NotImplementedError> {
        Ok(pingpong::component_pingpong::PingPong {}.into())
    }
    fn netpingpong(&self) -> Result<Box<AbstractDistributedBenchmark>, NotImplementedError> {
        Ok(netpingpong::PingPong {}.into())
    }
}
