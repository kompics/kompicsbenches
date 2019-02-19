use super::*;
use benchmark_suite_shared::benchmark::*;
use std::time::Duration;

pub mod pingpong;

pub fn component() -> Box<BenchmarkFactory> {
	Box::new(ComponentFactory {})
}
pub struct ComponentFactory;
impl BenchmarkFactory for ComponentFactory {
    fn by_label(&self, label: &str) -> Option<AbstractBench> {
    	match label {
    		pingpong::component_pingpong::PingPong::LABEL => Some(self.pingpong().into()),
    		_ => None,
    	}
    }

    fn pingpong(&self) -> Box<AbstractBenchmark> {
    	pingpong::component_pingpong::PingPong {}.into()
    }
    fn netpingpong(&self) -> Box<AbstractDistributedBenchmark> {
    	unimplemented!();
    }
}

pub fn actor() -> Box<BenchmarkFactory> {
	Box::new(ActorFactory {})
}
pub struct ActorFactory;
impl BenchmarkFactory for ActorFactory {
    fn by_label(&self, label: &str) -> Option<AbstractBench> {
    	match label {
    		pingpong::actor_pingpong::PingPong::LABEL => Some(self.pingpong().into()),
    		_ => None,
    	}
    }

    fn pingpong(&self) -> Box<AbstractBenchmark> {
    	pingpong::actor_pingpong::PingPong {}.into()
    }
    fn netpingpong(&self) -> Box<AbstractDistributedBenchmark> {
    	unimplemented!();
    }
}
pub fn mixed() -> Box<BenchmarkFactory> {
	Box::new(MixedFactory {})
}
pub struct MixedFactory;
impl BenchmarkFactory for MixedFactory {
    fn by_label(&self, label: &str) -> Option<AbstractBench> {
    	match label {
    		pingpong::component_pingpong::PingPong::LABEL => Some(self.pingpong().into()),
    		_ => None,
    	}
    }

    fn pingpong(&self) -> Box<AbstractBenchmark> {
    	pingpong::component_pingpong::PingPong {}.into()
    }
    fn netpingpong(&self) -> Box<AbstractDistributedBenchmark> {
    	unimplemented!();
    }
}
