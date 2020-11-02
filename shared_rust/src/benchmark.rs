use std::{convert::From, marker::PhantomData, panic::UnwindSafe};

pub use self::{distributed_benchmark::*, local_benchmark::*};

mod local_benchmark {
    use super::*;

    pub trait Benchmark: Send + Sync + UnwindSafe {
        type Conf;
        type Instance: BenchmarkInstance<Conf = Self::Conf>;

        fn msg_to_conf(msg: Box<dyn (::protobuf::Message)>) -> Result<Self::Conf, BenchmarkError>;
        fn new_instance() -> Self::Instance;

        const LABEL: &'static str;
    }

    pub trait BenchmarkInstance {
        type Conf;

        fn setup(&mut self, c: &Self::Conf) -> ();
        fn prepare_iteration(&mut self) -> () {}
        fn run_iteration(&mut self) -> ();
        fn cleanup_iteration(&mut self, _last_iteration: bool, _exec_time_millis: f64) -> () {}
    }

    pub trait AbstractBenchmark: Send + Sync + UnwindSafe {
        fn new_instance(&self) -> Box<dyn AbstractBenchmarkInstance>;
        fn run(&self, msg: Box<dyn (::protobuf::Message)>) -> Result<Vec<f64>, BenchmarkError>;
        fn label(&self) -> &'static str;
    }
    pub trait AbstractBenchmarkInstance {
        fn setup(&mut self, msg: Box<dyn (::protobuf::Message)>) -> Result<(), BenchmarkError>;
        fn prepare_iteration(&mut self) -> () {}
        fn run_iteration(&mut self) -> ();
        fn cleanup_iteration(&mut self, _last_iteration: bool, _exec_time_millis: f64) -> () {}
    }

    struct BenchmarkObject<B>
    where B: Benchmark + Default + 'static
    {
        _marker: PhantomData<B>,
    }

    impl<B: Benchmark + Default + UnwindSafe> BenchmarkObject<B> {
        #[allow(dead_code)]
        fn new() -> BenchmarkObject<B> { BenchmarkObject { _marker: PhantomData } }

        // Just using B to guide type inference
        fn with_benchmark(_b: B) -> BenchmarkObject<B> { BenchmarkObject { _marker: PhantomData } }
    }

    impl<B: Benchmark + Default + UnwindSafe> AbstractBenchmark for BenchmarkObject<B> {
        fn new_instance(&self) -> Box<dyn AbstractBenchmarkInstance> {
            let bi = B::new_instance();
            let bio: BenchmarkInstanceObject<B> = BenchmarkInstanceObject::new(bi);
            Box::new(bio)
        }

        fn run(&self, msg: Box<dyn (::protobuf::Message)>) -> Result<Vec<f64>, BenchmarkError> {
            let conf_res = B::msg_to_conf(msg);
            let b = B::default();
            let res = conf_res.and_then(|conf| crate::benchmark_runner::run(&b, &conf).into());
            res
        }

        fn label(&self) -> &'static str { B::LABEL }
    }

    impl<B: Benchmark + Default> From<B> for BenchmarkObject<B> {
        fn from(item: B) -> Self { BenchmarkObject::with_benchmark(item) }
    }

    impl<B: Benchmark + Default + 'static> From<B> for Box<dyn AbstractBenchmark> {
        fn from(item: B) -> Self {
            Box::new(BenchmarkObject::with_benchmark(item)) as Box<dyn AbstractBenchmark>
        }
    }

    struct BenchmarkInstanceObject<B>
    where B: Benchmark + 'static
    {
        bi: B::Instance,
    }

    impl<B: Benchmark> BenchmarkInstanceObject<B> {
        fn new(bi: B::Instance) -> BenchmarkInstanceObject<B> { BenchmarkInstanceObject { bi } }
    }

    impl<B: Benchmark> AbstractBenchmarkInstance for BenchmarkInstanceObject<B> {
        fn setup(&mut self, msg: Box<dyn (::protobuf::Message)>) -> Result<(), BenchmarkError> {
            self.bi = B::new_instance();
            B::msg_to_conf(msg).map(|c| self.bi.setup(&c))
        }

        fn prepare_iteration(&mut self) -> () { self.bi.prepare_iteration() }

        fn run_iteration(&mut self) -> () { self.bi.run_iteration() }

        fn cleanup_iteration(&mut self, last_iteration: bool, exec_time_millis: f64) -> () {
            self.bi.cleanup_iteration(last_iteration, exec_time_millis)
        }
    }
}

mod distributed_benchmark {
    use super::*;

    #[derive(Debug, Clone)]
    pub struct DeploymentMetaData {
        number_of_clients: u32,
    }
    impl DeploymentMetaData {
        pub fn new(number_of_clients: u32) -> DeploymentMetaData {
            DeploymentMetaData { number_of_clients }
        }

        pub fn number_of_clients(&self) -> u32 { self.number_of_clients }
    }

    pub trait DistributedBenchmark: Send + Sync {
        type MasterConf;
        type ClientConf;
        type ClientData;
        type Master: DistributedBenchmarkMaster<
            MasterConf = Self::MasterConf,
            ClientConf = Self::ClientConf,
            ClientData = Self::ClientData,
        >;
        type Client: DistributedBenchmarkClient<
            ClientConf = Self::ClientConf,
            ClientData = Self::ClientData,
        >;

        const LABEL: &'static str;

        fn new_master() -> Self::Master;
        fn msg_to_master_conf(
            msg: Box<dyn (::protobuf::Message)>,
        ) -> Result<Self::MasterConf, BenchmarkError>;

        fn new_client() -> Self::Client;
        fn str_to_client_conf(str: String) -> Result<Self::ClientConf, BenchmarkError>;
        fn str_to_client_data(str: String) -> Result<Self::ClientData, BenchmarkError>;

        fn client_conf_to_str(c: Self::ClientConf) -> String;
        fn client_data_to_str(d: Self::ClientData) -> String;
    }

    pub trait DistributedBenchmarkMaster {
        type MasterConf;
        type ClientConf;
        type ClientData;

        fn setup(
            &mut self,
            c: Self::MasterConf,
            meta: &DeploymentMetaData,
        ) -> Result<Self::ClientConf, BenchmarkError>;
        fn prepare_iteration(&mut self, _d: Vec<Self::ClientData>) -> () {}
        fn run_iteration(&mut self) -> ();
        fn cleanup_iteration(&mut self, _last_iteration: bool, _exec_time_millis: f64) -> () {}
    }

    pub trait DistributedBenchmarkClient {
        type ClientConf;
        type ClientData;

        fn setup(&mut self, c: Self::ClientConf) -> Self::ClientData;
        fn prepare_iteration(&mut self) -> () {}
        fn cleanup_iteration(&mut self, _last_iteration: bool) -> () {}
    }

    pub trait AbstractDistributedBenchmark: Send + Sync {
        fn new_master(&self) -> Box<dyn AbstractBenchmarkMaster>;
        fn new_client(&self) -> Box<dyn AbstractBenchmarkClient>;
        fn label(&self) -> &'static str;
    }

    #[derive(Clone)]
    pub struct ClientConfHolder(String);
    impl From<ClientConfHolder> for String {
        fn from(cch: ClientConfHolder) -> Self { cch.0 }
    }
    impl From<String> for ClientConfHolder {
        fn from(s: String) -> Self { ClientConfHolder(s) }
    }

    #[derive(Clone)]
    pub struct ClientDataHolder(String);
    impl From<ClientDataHolder> for String {
        fn from(cdh: ClientDataHolder) -> Self { cdh.0 }
    }
    impl From<String> for ClientDataHolder {
        fn from(s: String) -> Self { ClientDataHolder(s) }
    }

    pub trait AbstractBenchmarkMaster {
        fn setup(
            &mut self,
            msg: Box<dyn (::protobuf::Message)>,
            meta: &DeploymentMetaData,
        ) -> Result<ClientConfHolder, BenchmarkError>;
        fn prepare_iteration(&mut self, _d: Vec<ClientDataHolder>) -> Result<(), BenchmarkError> {
            Ok(())
        }
        fn run_iteration(&mut self) -> ();
        fn cleanup_iteration(&mut self, _last_iteration: bool, _exec_time_millis: f64) -> () {}
    }
    pub trait AbstractBenchmarkClient {
        fn setup(&mut self, c: ClientConfHolder) -> Result<ClientDataHolder, BenchmarkError>;
        fn prepare_iteration(&mut self) -> () {}
        fn cleanup_iteration(&mut self, _last_iteration: bool) -> () {}
    }

    struct DistributedBenchmarkObject<B>
    where B: DistributedBenchmark + 'static
    {
        _marker: PhantomData<B>,
    }

    impl<B: DistributedBenchmark + 'static> DistributedBenchmarkObject<B> {
        #[allow(dead_code)]
        fn new() -> DistributedBenchmarkObject<B> {
            DistributedBenchmarkObject { _marker: PhantomData }
        }

        // Just using B to guide type inference
        fn with_benchmark(_b: B) -> DistributedBenchmarkObject<B> {
            DistributedBenchmarkObject { _marker: PhantomData }
        }
    }

    impl<B: DistributedBenchmark> AbstractDistributedBenchmark for DistributedBenchmarkObject<B> {
        fn new_master(&self) -> Box<dyn AbstractBenchmarkMaster> {
            let bm = B::new_master();
            let bmo: BenchmarkMasterObject<B> = BenchmarkMasterObject::new(bm);
            Box::new(bmo)
        }

        fn new_client(&self) -> Box<dyn AbstractBenchmarkClient> {
            let bc = B::new_client();
            let bco: BenchmarkClientObject<B> = BenchmarkClientObject::new(bc);
            Box::new(bco)
        }

        fn label(&self) -> &'static str { B::LABEL }
    }

    impl<B: DistributedBenchmark> From<B> for DistributedBenchmarkObject<B> {
        fn from(item: B) -> Self { DistributedBenchmarkObject::with_benchmark(item) }
    }

    impl<B: DistributedBenchmark + 'static> From<B> for Box<dyn AbstractDistributedBenchmark> {
        fn from(item: B) -> Self {
            Box::new(DistributedBenchmarkObject::with_benchmark(item))
                as Box<dyn AbstractDistributedBenchmark>
        }
    }

    struct BenchmarkMasterObject<B>
    where B: DistributedBenchmark + 'static
    {
        bm: B::Master,
    }

    impl<B: DistributedBenchmark + 'static> BenchmarkMasterObject<B> {
        fn new(bm: B::Master) -> BenchmarkMasterObject<B> { BenchmarkMasterObject { bm } }
    }

    impl<B: DistributedBenchmark + 'static> AbstractBenchmarkMaster for BenchmarkMasterObject<B> {
        fn setup(
            &mut self,
            msg: Box<dyn (::protobuf::Message)>,
            meta: &DeploymentMetaData,
        ) -> Result<ClientConfHolder, BenchmarkError> {
            let res = B::msg_to_master_conf(msg);
            res.and_then(|c| {
                self.bm.setup(c, meta).map(|cconf| {
                    let cconf_ser = B::client_conf_to_str(cconf);
                    ClientConfHolder(cconf_ser)
                })
            })
        }

        fn prepare_iteration(&mut self, d: Vec<ClientDataHolder>) -> Result<(), BenchmarkError> {
            let res: Result<Vec<B::ClientData>, BenchmarkError> =
                d.into_iter().map(|holder| B::str_to_client_data(holder.0)).collect();
            res.map(|d_deser| self.bm.prepare_iteration(d_deser))
        }

        fn run_iteration(&mut self) -> () { self.bm.run_iteration(); }

        fn cleanup_iteration(&mut self, last_iteration: bool, exec_time_millis: f64) -> () {
            self.bm.cleanup_iteration(last_iteration, exec_time_millis);
        }
    }

    struct BenchmarkClientObject<B>
    where B: DistributedBenchmark + 'static
    {
        bc: B::Client,
    }

    impl<B: DistributedBenchmark + 'static> BenchmarkClientObject<B> {
        fn new(bc: B::Client) -> BenchmarkClientObject<B> { BenchmarkClientObject { bc } }
    }

    impl<B: DistributedBenchmark + 'static> AbstractBenchmarkClient for BenchmarkClientObject<B> {
        fn setup(&mut self, c: ClientConfHolder) -> Result<ClientDataHolder, BenchmarkError> {
            let res = B::str_to_client_conf(c.0);
            res.map(|cconf| {
                let cdata = self.bc.setup(cconf);
                let cdata_ser = B::client_data_to_str(cdata);
                ClientDataHolder(cdata_ser)
            })
        }

        fn prepare_iteration(&mut self) -> () { self.bc.prepare_iteration(); }

        fn cleanup_iteration(&mut self, last_iteration: bool) -> () {
            self.bc.cleanup_iteration(last_iteration);
        }
    }
}

pub enum AbstractBench {
    Local(Box<dyn AbstractBenchmark>),
    Distributed(Box<dyn AbstractDistributedBenchmark>),
}

impl From<Box<dyn AbstractBenchmark>> for AbstractBench {
    fn from(item: Box<dyn AbstractBenchmark>) -> Self { AbstractBench::Local(item) }
}

impl From<Box<dyn AbstractDistributedBenchmark>> for AbstractBench {
    fn from(item: Box<dyn AbstractDistributedBenchmark>) -> Self {
        AbstractBench::Distributed(item)
    }
}

pub trait ResultInto<O, E> {
    fn map_into(self) -> Result<O, E>;
}

impl<I, O, E> ResultInto<O, E> for Result<I, E>
where O: From<I>
{
    fn map_into(self) -> Result<O, E> { self.map(|i| i.into()) }
}

// impl From<Result<Box<AbstractBenchmark>, NotImplementedError>> for Result<AbstractBench, NotImplementedError> {
//     fn from(item: Result<Box<AbstractBenchmark>, NotImplementedError>) -> Self {
//         item.map(|i| i.into())
//     }
// }

#[derive(Debug)]
pub enum NotImplementedError {
    FutureWork,
    NotImplementable,
    NotFound,
}

pub trait BenchmarkFactory: Send + Sync {
    fn by_label(&self, label: &str) -> Result<AbstractBench, NotImplementedError>;
    fn box_clone(&self) -> Box<dyn BenchmarkFactory>;

    fn ping_pong(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError>;
    fn net_ping_pong(&self) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError>;
    fn throughput_ping_pong(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError>;
    fn net_throughput_ping_pong(
        &self,
    ) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError>;
    fn atomic_register(&self)
        -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError>;
    fn streaming_windows(
        &self,
    ) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError>;
    fn fibonacci(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError>;
    fn chameneos(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError>;
    fn all_pairs_shortest_path(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError>;
    fn atomic_broadcast(
        &self,
    ) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError>;
    fn sized_throughput(&self) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError>;
}

impl Clone for Box<dyn BenchmarkFactory> {
    fn clone(&self) -> Box<dyn BenchmarkFactory> { self.box_clone() }
}

#[macro_export]
macro_rules! downcast_msg {
    ($m:expr; $M:ty) => {{
        let descr = $m.descriptor();
        let res = $m.into_any().downcast::<$M>();
        res.map(|b| *b).map_err(|_e| {
            BenchmarkError::InvalidMessage(format!(
                "Got {}, but expected {}",
                descr.name(),
                stringify!($M)
            ))
        })
    }};
    ($m:expr; $M:ty; $f:expr) => {{
        let descr = $m.descriptor();
        let res = $m.into_any().downcast::<$M>();
        res.map(|b| $f(*b)).map_err(|_e| {
            BenchmarkError::InvalidMessage(format!(
                "Got {}, but expected {}",
                descr.name(),
                stringify!($M)
            ))
        })
    }};
}

#[derive(Debug)]
pub enum BenchmarkError {
    RSETargetNotMet(String),
    Panic,
    InvalidMessage(String),
    RPCError(grpc::Error),
    InvalidTest(String),
    NotImplemented(NotImplementedError),
    InvalidDeployment(String),
}

impl From<grpc::Error> for BenchmarkError {
    fn from(error: grpc::Error) -> Self { BenchmarkError::RPCError(error) }
}

impl From<BenchmarkError> for grpc::Error {
    fn from(error: BenchmarkError) -> Self {
        match error {
            BenchmarkError::RPCError(e) => e,
            _ => grpc::Error::Panic(format!("Converted BenchmarkError: {:?}", error)),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::benchmarks::PingPongRequest;

    struct TestB;
    struct TestBI;

    impl Benchmark for TestB {
        type Conf = PingPongRequest;
        type Instance = TestBI;

        const LABEL: &'static str = "TestB";

        fn msg_to_conf(msg: Box<dyn (::protobuf::Message)>) -> Result<Self::Conf, BenchmarkError> {
            downcast_msg!(msg; PingPongRequest)
        }

        fn new_instance() -> Self::Instance { TestBI {} }
    }

    impl BenchmarkInstance for TestBI {
        type Conf = PingPongRequest;

        fn setup(&mut self, _c: &Self::Conf) -> () {}

        fn run_iteration(&mut self) -> () {}
    }

    #[test]
    fn instantiate_local_benchmark() -> () {
        let mut bi = TestB::new_instance();
        let msg = PingPongRequest::new();
        let c = TestB::msg_to_conf(Box::new(msg)).unwrap();
        bi.setup(&c);
    }

    #[derive(Default)]
    struct Test2B;
    struct Test2BI;
    struct Test2Conf;

    impl Benchmark for Test2B {
        type Conf = Test2Conf;
        type Instance = Test2BI;

        const LABEL: &'static str = "Test2B";

        fn msg_to_conf(msg: Box<dyn (::protobuf::Message)>) -> Result<Self::Conf, BenchmarkError> {
            downcast_msg!(msg; PingPongRequest; |_ppr| Test2Conf{})
        }

        fn new_instance() -> Self::Instance { Test2BI {} }
    }

    impl BenchmarkInstance for Test2BI {
        type Conf = Test2Conf;

        fn setup(&mut self, _c: &Self::Conf) -> () {}

        fn run_iteration(&mut self) -> () {}
    }

    #[test]
    fn instantiate_local_benchmark_with_mapped_conf() -> () {
        let mut bi = Test2B::new_instance();
        let msg = PingPongRequest::new();
        let c = Test2B::msg_to_conf(Box::new(msg)).unwrap();
        bi.setup(&c);
    }

    struct Test3B;
    struct Test3Conf;
    struct Test3BM;
    struct Test3BC;

    impl DistributedBenchmark for Test3B {
        type Client = Test3BC;
        type ClientConf = String;
        type ClientData = String;
        type Master = Test3BM;
        type MasterConf = Test3Conf;

        const LABEL: &'static str = "Test3B";

        fn new_master() -> Self::Master { Test3BM {} }

        fn msg_to_master_conf(
            msg: Box<dyn (::protobuf::Message)>,
        ) -> Result<Self::MasterConf, BenchmarkError> {
            downcast_msg!(msg; PingPongRequest; |_ppr| Test3Conf{})
        }

        fn new_client() -> Self::Client { Test3BC {} }

        fn str_to_client_conf(str: String) -> Result<Self::ClientConf, BenchmarkError> { Ok(str) }

        fn str_to_client_data(str: String) -> Result<Self::ClientData, BenchmarkError> { Ok(str) }

        fn client_conf_to_str(c: Self::ClientConf) -> String { c }

        fn client_data_to_str(d: Self::ClientData) -> String { d }
    }

    impl DistributedBenchmarkMaster for Test3BM {
        type ClientConf = String;
        type ClientData = String;
        type MasterConf = Test3Conf;

        fn setup(
            &mut self,
            _c: Self::MasterConf,
            _m: &DeploymentMetaData,
        ) -> Result<Self::ClientConf, BenchmarkError> {
            Ok("ok".into())
        }

        fn prepare_iteration(&mut self, _d: Vec<Self::ClientData>) -> () {}

        fn run_iteration(&mut self) -> () {}

        fn cleanup_iteration(&mut self, _last_iteration: bool, _exec_time_millis: f64) -> () {}
    }

    impl DistributedBenchmarkClient for Test3BC {
        type ClientConf = String;
        type ClientData = String;

        fn setup(&mut self, c: Self::ClientConf) -> Self::ClientData { c }

        fn prepare_iteration(&mut self) -> () {}

        fn cleanup_iteration(&mut self, _last_iteration: bool) -> () {}
    }

    #[test]
    fn instantiate_distributed_benchmark() -> () {
        let mut master = Test3B::new_master();
        let msg = PingPongRequest::new();
        let mconf =
            Test3B::msg_to_master_conf(Box::new(msg)).expect("Could not create master conf!");
        let cconf =
            master.setup(mconf, &DeploymentMetaData::new(1)).expect("Could not setup master!");
        let cconf_ser = Test3B::client_conf_to_str(cconf);
        let mut client = Test3B::new_client();
        let cconf_deser =
            Test3B::str_to_client_conf(cconf_ser).expect("Could not create client conf");
        let cdata = client.setup(cconf_deser);
        let cdata_ser = Test3B::client_data_to_str(cdata);
        client.prepare_iteration();
        let cdata_deser =
            Test3B::str_to_client_data(cdata_ser).expect("Could not create client data");
        let all_cdata = vec![cdata_deser];
        master.prepare_iteration(all_cdata);
        master.run_iteration();
        master.cleanup_iteration(false, 0.0);
        client.cleanup_iteration(false);
    }

    pub(crate) struct TestFactory;

    impl TestFactory {
        pub(crate) fn boxed() -> Box<dyn BenchmarkFactory> {
            let tf = TestFactory {};
            Box::new(tf)
        }
    }

    impl BenchmarkFactory for TestFactory {
        fn box_clone(&self) -> Box<dyn BenchmarkFactory> { TestFactory::boxed() }

        fn by_label(&self, label: &str) -> Result<AbstractBench, NotImplementedError> {
            match label {
                Test2B::LABEL => self.ping_pong().map_into(),
                Test3B::LABEL => self.net_ping_pong().map_into(),
                _ => Err(NotImplementedError::NotFound),
            }
        }

        fn ping_pong(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
            Ok(Test2B {}.into())
        }

        fn net_ping_pong(
            &self,
        ) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
            Ok(Test3B {}.into())
        }

        fn throughput_ping_pong(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
            Ok(Test2B {}.into())
        }

        fn net_throughput_ping_pong(
            &self,
        ) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
            Ok(Test3B {}.into())
        }

        fn atomic_register(
            &self,
        ) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
            Ok(Test3B {}.into())
        }

        fn streaming_windows(
            &self,
        ) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
            Ok(Test3B {}.into())
        }

        fn fibonacci(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
            Ok(Test2B {}.into())
        }

        fn chameneos(&self) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
            Ok(Test2B {}.into())
        }

        fn all_pairs_shortest_path(
            &self,
        ) -> Result<Box<dyn AbstractBenchmark>, NotImplementedError> {
            Ok(Test2B {}.into())
        }

        fn atomic_broadcast(
            &self,
        ) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
            Ok(Test3B {}.into())
        }

        fn sized_throughput(
            &self,
        ) -> Result<Box<dyn AbstractDistributedBenchmark>, NotImplementedError> {
            Ok(Test3B {}.into())
        }
    }

    #[test]
    fn instantiate_abstract_local_benchmark() -> () {
        let factory = TestFactory {};
        let b = factory.ping_pong().unwrap();
        let mut bi = b.new_instance();
        let msg = PingPongRequest::new();
        bi.setup(Box::new(msg)).unwrap();
        bi.prepare_iteration();
        bi.run_iteration();
        bi.cleanup_iteration(false, 0.0);
    }

    #[test]
    fn instantiate_abstract_distributed_benchmark() -> () {
        let factory = TestFactory {};
        let b = factory.net_ping_pong().unwrap();
        let mut bm = b.new_master();
        let msg = PingPongRequest::new();
        let cconf = bm.setup(Box::new(msg), &DeploymentMetaData::new(1)).unwrap();
        let mut bc = b.new_client();
        let cdata = bc.setup(cconf).unwrap();
        bc.prepare_iteration();
        let all_cdata = vec![cdata];
        bm.prepare_iteration(all_cdata).expect("prepare failed");
        bm.run_iteration();
        bm.cleanup_iteration(false, 0.0);
        bc.cleanup_iteration(false);
    }
}
