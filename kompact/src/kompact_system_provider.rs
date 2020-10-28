//use super::*;

use kompact::executors::*;
use kompact::net::buffers::BufferConfig;
use kompact::prelude::*;
use num_cpus;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

// Would be nicer to be able to declare the default value const -.-
static mut GLOBAL: KompactSystemProvider = KompactSystemProvider {
    public_if: IpAddr::V4(Ipv4Addr::LOCALHOST),
};

pub(crate) fn set_global_public_if(addr: IpAddr) {
    unsafe {
        GLOBAL.set_public_if(addr);
    }
}
pub fn global() -> &'static KompactSystemProvider {
    unsafe { &GLOBAL }
}

pub struct KompactSystemProvider {
    public_if: IpAddr,
}

mod consts {
    pub const NUM_WORKERS_DEFAULT: usize = 4;
    pub const NUM_WORKERS_MAX: usize = 64;
}

impl KompactSystemProvider {
    pub fn set_public_if<I>(&mut self, s: I)
    where
        I: Into<IpAddr>,
    {
        self.public_if = s.into();
    }

    pub fn new_system<I: Into<String>>(&self, name: I) -> KompactSystem {
        self.new_system_with_threads(name, self.get_num_workers())
    }

    pub fn new_system_with_threads<I: Into<String>>(
        &self,
        name: I,
        threads: usize,
    ) -> KompactSystem {
        let s = name.into();
        let mut conf = KompactConfig::default();
        conf.label(s);
        conf.threads(threads);
        Self::set_executor_for_threads(threads, &mut conf);
        conf.throughput(50);
        let system = conf.build().expect("KompactSystem");
        system
    }

    pub fn new_remote_system<I: Into<String>>(&self, name: I) -> KompactSystem {
        self.new_remote_system_with_threads(name, self.get_num_workers())
    }

    pub fn new_remote_system_with_threads<I: Into<String>>(
        &self,
        name: I,
        threads: usize,
    ) -> KompactSystem {
        let s = name.into();
        let addr = SocketAddr::new(self.get_public_if(), 0);
        let mut conf = KompactConfig::default();
        conf.label(s);
        conf.threads(threads);
        Self::set_executor_for_threads(threads, &mut conf);
        conf.throughput(50);
        conf.system_components(DeadletterBox::new, NetworkConfig::new(addr).build());
        let system = conf.build().expect("KompactSystem");
        system
    }

    pub fn new_remote_system_with_threads_config<I: Into<String>>(
        &self,
        name: I,
        threads: usize,
        mut conf: KompactConfig,
        buf_conf: BufferConfig,
        tcp_no_delay: bool,
    ) -> KompactSystem {
        let s = name.into();
        let addr = SocketAddr::new(self.get_public_if(), 0);
        conf.label(s);
        conf.threads(threads);
        Self::set_executor_for_threads(threads, &mut conf);
        conf.throughput(50);
        let mut nc = NetworkConfig::with_buffer_config(addr, buf_conf);
        nc.set_tcp_nodelay(tcp_no_delay);
        conf.system_components(DeadletterBox::new, nc.build());
        let system = conf.build().expect("KompactSystem");
        system
    }

    pub fn get_num_workers(&self) -> usize {
        let n = num_cpus::get();
        if (n >= consts::NUM_WORKERS_DEFAULT) && (n <= consts::NUM_WORKERS_MAX) {
            n
        } else if n >= consts::NUM_WORKERS_DEFAULT {
            consts::NUM_WORKERS_MAX
        } else {
            consts::NUM_WORKERS_DEFAULT
        }
    }

    pub fn get_public_if(&self) -> IpAddr {
        self.public_if
    }

    fn set_executor_for_threads(threads: usize, conf: &mut KompactConfig) -> () {
        if threads <= 32 {
            conf.executor(|t| crossbeam_workstealing_pool::small_pool(t))
        } else if threads <= 64 {
            conf.executor(|t| crossbeam_workstealing_pool::large_pool(t))
        } else {
            panic!("DynPool doesn't work atm!");
            //conf.scheduler(|t| crossbeam_workstealing_pool::dyn_pool(t))
        };
    }
}

impl Default for KompactSystemProvider {
    fn default() -> Self {
        KompactSystemProvider {
            public_if: "127.0.0.1".parse().unwrap(),
        }
    }
}
