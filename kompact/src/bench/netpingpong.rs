use super::*;

use benchmark_suite_shared::kompics_benchmarks::benchmarks::PingPongRequest;
use kompact::prelude::*;
use kompact::*;
use std::str::FromStr;
use std::sync::Arc;
use synchronoise::CountdownEvent;

#[derive(Clone, Debug)]
struct Start;
#[derive(Clone, Debug)]
struct Ping;
#[derive(Clone, Debug)]
struct Pong;

#[derive(Default)]
pub struct PingPong;

impl DistributedBenchmark for PingPong {
    type MasterConf = PingPongRequest;
    type ClientConf = ();
    type ClientData = ActorPath;
    type Master = PingPongMaster;
    type Client = PingPongClient;

    const LABEL: &'static str = "NetPingPong";

    fn new_master() -> Self::Master {
        PingPongMaster::new()
    }
    fn msg_to_master_conf(
        msg: Box<::protobuf::Message>,
    ) -> Result<Self::MasterConf, BenchmarkError> {
        downcast_msg!(msg; PingPongRequest)
    }

    fn new_client() -> Self::Client {
        PingPongClient::new()
    }
    fn str_to_client_conf(_str: String) -> Result<Self::ClientConf, BenchmarkError> {
        Ok(())
    }
    fn str_to_client_data(str: String) -> Result<Self::ClientData, BenchmarkError> {
        let res = ActorPath::from_str(&str);
        res.map_err(|e| {
            BenchmarkError::InvalidMessage(format!("Could not read client data: {}", e))
        })
    }

    fn client_conf_to_str(_c: Self::ClientConf) -> String {
        String::new()
    }
    fn client_data_to_str(d: Self::ClientData) -> String {
        d.to_string()
    }
}

pub struct PingPongMaster {
    num: Option<u64>,
    system: Option<KompactSystem>,
    pinger: Option<Arc<Component<Pinger>>>,
    ponger: Option<ActorPath>,
    latch: Option<Arc<CountdownEvent>>,
}

impl PingPongMaster {
    fn new() -> PingPongMaster {
        PingPongMaster {
            num: None,
            system: None,
            pinger: None,
            ponger: None,
            latch: None,
        }
    }
}

impl DistributedBenchmarkMaster for PingPongMaster {
    type MasterConf = PingPongRequest;
    type ClientConf = ();
    type ClientData = ActorPath;

    fn setup(&mut self, c: Self::MasterConf) -> Self::ClientConf {
        self.num = Some(c.number_of_messages);
        let system = crate::kompact_system_provider::global().new_remote_system("pingpong", 1);
        self.system = Some(system);
        ()
    }
    fn prepare_iteration(&mut self, d: Vec<Self::ClientData>) -> () {
        let ponger_ref = match self.ponger {
            Some(ref p) => p.clone(),
            None => {
                let ponger_ref = d[0].clone();
                println!("Resolved path to ponger: {}", &ponger_ref);
                self.ponger = Some(ponger_ref.clone());
                ponger_ref
            }
        };
        match self.num {
            Some(num) => match self.system {
                Some(ref system) => {
                    let latch = Arc::new(CountdownEvent::new(1));
                    let (pinger, unique_reg_f) =
                        system.create_and_register(|| Pinger::with(num, latch.clone(), ponger_ref));

                    unique_reg_f
                        .wait_timeout(Duration::from_millis(1000))
                        .expect("Ponger never registered!")
                        .expect("Ponger failed to register!");

                    let pinger_f = system.start_notify(&pinger);

                    pinger_f
                        .wait_timeout(Duration::from_millis(1000))
                        .expect("Pinger never started!");

                    self.pinger = Some(pinger);
                    self.latch = Some(latch);
                }
                None => unimplemented!(),
            },
            None => unimplemented!(),
        }
    }
    fn run_iteration(&mut self) -> () {
        match self.system {
            Some(ref system) => {
                let latch = self.latch.take().unwrap();
                if let Some(pinger) = self.pinger.take() {
                    let pinger_ref = pinger.actor_ref();
                    pinger_ref.tell(Box::new(Start), system);
                    latch.wait();
                    self.pinger = Some(pinger);
                } else {
                    unimplemented!()
                }
            }
            None => unimplemented!(),
        }
    }
    fn cleanup_iteration(&mut self, last_iteration: bool, _exec_time_millis: f64) -> () {
        println!("Cleaning up pinger side");
        let system = self.system.take().unwrap();
        let pinger = self.pinger.take().unwrap();
        let f = system.kill_notify(pinger);

        f.wait_timeout(Duration::from_millis(1000))
            .expect("Pinger never died!");

        if last_iteration {
            system
                .shutdown()
                .expect("Kompact didn't shut down properly");
            self.num = None;
        } else {
            self.system = Some(system);
        }
    }
}

pub struct PingPongClient {
    system: Option<KompactSystem>,
    ponger: Option<Arc<Component<Ponger>>>,
}

impl PingPongClient {
    fn new() -> PingPongClient {
        PingPongClient {
            system: None,
            ponger: None,
        }
    }
}

impl DistributedBenchmarkClient for PingPongClient {
    type ClientConf = ();
    type ClientData = ActorPath;

    fn setup(&mut self, _c: Self::ClientConf) -> Self::ClientData {
        println!("Setting up ponger.");

        let system = crate::kompact_system_provider::global().new_remote_system("pingpong", 1);
        let (ponger, unique_reg_f) = system.create_and_register(|| Ponger::new());
        let named_reg_f = system.register_by_alias(&ponger, "ponger");
        unique_reg_f
            .wait_timeout(Duration::from_millis(1000))
            .expect("Ponger never registered!")
            .expect("Ponger failed to register!");
        named_reg_f
            .wait_timeout(Duration::from_millis(1000))
            .expect("Ponger never registered!")
            .expect("Ponger failed to register!");
        let start_f = system.start_notify(&ponger);
        start_f
            .wait_timeout(Duration::from_millis(1000))
            .expect("Ponger never started!");

        let named_path = ActorPath::Named(NamedPath::with_system(
            system.system_path(),
            vec!["ponger".into()],
        ));

        println!("Got path for ponger: {}", named_path);

        self.system = Some(system);
        self.ponger = Some(ponger);

        named_path
    }

    fn prepare_iteration(&mut self) -> () {
        // nothing to do
        println!("Preparing ponger iteration");
    }

    fn cleanup_iteration(&mut self, last_iteration: bool) -> () {
        println!("Cleaning up ponger side");
        if last_iteration {
            let system = self.system.take().unwrap();
            let ponger = self.ponger.take().unwrap();
            let stop_f = system.kill_notify(ponger);

            stop_f
                .wait_timeout(Duration::from_millis(1000))
                .expect("Ponger never died!");

            system
                .shutdown()
                .expect("Kompact didn't shut down properly");
        }
    }
}

#[derive(Debug, Clone)]
struct PingMsg;

#[derive(Debug, Clone)]
struct PongMsg;

struct PingPongSer;
const PING_PONG_SER: PingPongSer = PingPongSer {};
const PING_ID: i8 = 1;
const PONG_ID: i8 = 2;
impl Serialiser<PingMsg> for PingPongSer {
    fn serid(&self) -> u64 {
        42 // because why not^^
    }
    fn size_hint(&self) -> Option<usize> {
        Some(9)
    }
    fn serialise(&self, _v: &PingMsg, buf: &mut BufMut) -> Result<(), SerError> {
        buf.put_i8(PING_ID);
        Result::Ok(())
    }
}

impl Serialiser<PongMsg> for PingPongSer {
    fn serid(&self) -> u64 {
        42 // because why not^^
    }
    fn size_hint(&self) -> Option<usize> {
        Some(9)
    }
    fn serialise(&self, _v: &PongMsg, buf: &mut BufMut) -> Result<(), SerError> {
        buf.put_i8(PONG_ID);
        Result::Ok(())
    }
}
impl Deserialiser<PingMsg> for PingPongSer {
    fn deserialise(buf: &mut Buf) -> Result<PingMsg, SerError> {
        if buf.remaining() < 1 {
            return Err(SerError::InvalidData(format!(
                "Serialised typed has 9bytes but only {}bytes remain in buffer.",
                buf.remaining()
            )));
        }
        match buf.get_i8() {
            PING_ID => Ok(PingMsg),
            PONG_ID => Err(SerError::InvalidType(
                "Found PongMsg, but expected PingMsg.".into(),
            )),
            _ => Err(SerError::InvalidType(
                "Found unkown id, but expected PingMsg.".into(),
            )),
        }
    }
}
impl Deserialiser<PongMsg> for PingPongSer {
    fn deserialise(buf: &mut Buf) -> Result<PongMsg, SerError> {
        if buf.remaining() < 1 {
            return Err(SerError::InvalidData(format!(
                "Serialised typed has 9bytes but only {}bytes remain in buffer.",
                buf.remaining()
            )));
        }
        match buf.get_i8() {
            PONG_ID => Ok(PongMsg),
            PING_ID => Err(SerError::InvalidType(
                "Found PingMsg, but expected PongMsg.".into(),
            )),
            _ => Err(SerError::InvalidType(
                "Found unkown id, but expected PongMsg.".into(),
            )),
        }
    }
}

#[derive(ComponentDefinition)]
struct Pinger {
    ctx: ComponentContext<Pinger>,
    latch: Arc<CountdownEvent>,
    ponger: ActorPath,
    count_down: u64,
}

impl Pinger {
    fn with(count: u64, latch: Arc<CountdownEvent>, ponger: ActorPath) -> Pinger {
        Pinger {
            ctx: ComponentContext::new(),
            latch,
            ponger,
            count_down: count,
        }
    }
}

impl Provide<ControlPort> for Pinger {
    fn handle(&mut self, _event: ControlEvent) -> () {
        // ignore
    }
}

impl Actor for Pinger {
    fn receive_local(&mut self, sender: ActorRef, msg: Box<Any>) -> () {
        if msg.is::<Start>() {
            self.ponger.tell((PingMsg, PING_PONG_SER), self);
        } else {
            crit!(self.ctx.log(), "Got unexpected message from {}", sender);
            unimplemented!(); // shouldn't happen during the test
        }
    }
    fn receive_message(&mut self, sender: ActorPath, ser_id: u64, buf: &mut Buf) -> () {
        if ser_id == Serialiser::<PongMsg>::serid(&PING_PONG_SER) {
            let r: Result<PongMsg, SerError> = PingPongSer::deserialise(buf);
            match r {
                Ok(_pong) => {
                    // TODO remove for test
                    info!(self.ctx.log(), "Got msg Pong from {}", sender);
                    self.count_down -= 1;
                    if self.count_down > 0 {
                        self.ponger.tell((PingMsg, PING_PONG_SER), self);
                    } else {
                        let _ = self.latch.decrement();
                    }
                }
                Err(e) => error!(self.ctx.log(), "Error deserialising PongMsg: {:?}", e),
            }
        } else {
            crit!(
                self.ctx.log(),
                "Got message with unexpected serialiser {} from {}",
                ser_id,
                sender
            );
            unimplemented!(); // shouldn't happen during the test
        }
    }
}

#[derive(ComponentDefinition)]
struct Ponger {
    ctx: ComponentContext<Ponger>,
}

impl Ponger {
    fn new() -> Ponger {
        Ponger {
            ctx: ComponentContext::new(),
        }
    }
}

impl Provide<ControlPort> for Ponger {
    fn handle(&mut self, _event: ControlEvent) -> () {
        // ignore
    }
}

impl Actor for Ponger {
    fn receive_local(&mut self, _sender: ActorRef, msg: Box<Any>) -> () {
        crit!(self.ctx.log(), "Got unexpected local msg {:?}", msg);
        unimplemented!(); // shouldn't happen during the test
    }
    fn receive_message(&mut self, sender: ActorPath, ser_id: u64, buf: &mut Buf) -> () {
        if ser_id == Serialiser::<PingMsg>::serid(&PING_PONG_SER) {
            let r: Result<PingMsg, SerError> = PingPongSer::deserialise(buf);
            match r {
                Ok(_ping) => {
                    // TODO remove for test
                    info!(self.ctx.log(), "Got msg Ping from {}", sender);
                    sender.tell((PongMsg, PING_PONG_SER), self);
                }
                Err(e) => error!(self.ctx.log(), "Error deserialising PingMsg: {:?}", e),
            }
        } else {
            crit!(
                self.ctx.log(),
                "Got message with unexpected serialiser {} from {}",
                ser_id,
                sender
            );
            unimplemented!(); // shouldn't happen during the test
        }
    }
}
