use super::*;

use benchmark_suite_shared::kompics_benchmarks::benchmarks::ThroughputPingPongRequest;
use kompact::prelude::*;
use kompact::*;
use std::str::FromStr;
use std::sync::Arc;
use synchronoise::CountdownEvent;

use throughput_pingpong::{EitherComponents, Params, Ping, Pong, StaticPing, StaticPong};

#[derive(Debug, Clone, PartialEq)]
pub struct ClientParams {
    num_pongers: u32,
    static_only: bool,
}
impl ClientParams {
    fn new(num_pongers: u32, static_only: bool) -> ClientParams {
        ClientParams {
            num_pongers,
            static_only,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClientRefs(Vec<ActorPath>);

#[derive(Default)]
pub struct PingPong;

impl DistributedBenchmark for PingPong {
    type MasterConf = ThroughputPingPongRequest;
    type ClientConf = ClientParams;
    type ClientData = ClientRefs;
    type Master = PingPongMaster;
    type Client = PingPongClient;

    const LABEL: &'static str = "NetThroughputPingPong";

    fn new_master() -> Self::Master {
        PingPongMaster::new()
    }
    fn msg_to_master_conf(
        msg: Box<::protobuf::Message>,
    ) -> Result<Self::MasterConf, BenchmarkError> {
        downcast_msg!(msg; ThroughputPingPongRequest)
    }

    fn new_client() -> Self::Client {
        PingPongClient::new()
    }
    fn str_to_client_conf(str: String) -> Result<Self::ClientConf, BenchmarkError> {
        let split: Vec<_> = str.split(',').collect();
        if split.len() != 2 {
            Err(BenchmarkError::InvalidMessage(format!(
                "String '{}' does not represent a client conf!",
                str
            )))
        } else {
            let num_str = split[0];
            let num = num_str.parse::<u32>().map_err(|e| {
                BenchmarkError::InvalidMessage(format!(
                    "String '{}' does not represent a client conf: {:?}",
                    str, e
                ))
            })?;
            let static_str = split[1];
            let static_only = static_str.parse::<bool>().map_err(|e| {
                BenchmarkError::InvalidMessage(format!(
                    "String '{}' does not represent a client conf: {:?}",
                    str, e
                ))
            })?;
            Ok(ClientParams::new(num, static_only))
        }
    }
    fn str_to_client_data(str: String) -> Result<Self::ClientData, BenchmarkError> {
        let res: Result<Vec<_>, _> = str.split(',').map(|s| ActorPath::from_str(s)).collect();
        res.map(|paths| ClientRefs(paths)).map_err(|e| {
            BenchmarkError::InvalidMessage(format!("Could not read client data: {}", e))
        })
    }

    fn client_conf_to_str(c: Self::ClientConf) -> String {
        format!("{},{}", c.num_pongers, c.static_only)
    }
    fn client_data_to_str(d: Self::ClientData) -> String {
        d.0.into_iter()
            .map(|path| path.to_string())
            .collect::<Vec<String>>()
            .join(",")
    }
}

pub struct PingPongMaster {
    params: Option<Params>,
    system: Option<KompactSystem>,
    pingers: EitherComponents<StaticPinger, Pinger>,
    pongers: Vec<ActorPath>,
    latch: Option<Arc<CountdownEvent>>,
}

impl PingPongMaster {
    fn new() -> PingPongMaster {
        PingPongMaster {
            params: None,
            system: None,
            pingers: EitherComponents::Empty,
            pongers: Vec::new(),
            latch: None,
        }
    }
}

impl DistributedBenchmarkMaster for PingPongMaster {
    type MasterConf = ThroughputPingPongRequest;
    type ClientConf = ClientParams;
    type ClientData = ClientRefs;

    fn setup(&mut self, c: Self::MasterConf) -> Self::ClientConf {
        let params = Params::from_req(&c);
        let system = crate::kompact_system_provider::global()
            .new_remote_system("throughputpingpong", num_cpus::get());
        self.system = Some(system);
        let client_conf = ClientParams::new(params.num_pairs, params.static_only);
        self.params = Some(params);
        client_conf
    }
    fn prepare_iteration(&mut self, d: Vec<Self::ClientData>) -> () {
        self.pongers = d[0].0.clone();
        match self.params {
            Some(ref params) => match self.system {
                Some(ref system) => {
                    assert_eq!(self.pongers.len(), params.num_pairs as usize);
                    let latch = Arc::new(CountdownEvent::new(params.num_pairs as usize));
                    let pingers = if params.static_only {
                        let mut vpi = Vec::with_capacity(params.num_pairs as usize);
                        for ponger_ref in self.pongers.iter() {
                            let (pinger, unique_reg_f) = system.create_and_register(|| {
                                StaticPinger::with(
                                    params.num_msgs,
                                    params.pipeline,
                                    latch.clone(),
                                    ponger_ref.clone(),
                                )
                            });
                            unique_reg_f
                                .wait_timeout(Duration::from_millis(1000))
                                .expect("Pinger never registered!")
                                .expect("Pinger failed to register!");
                            vpi.push(pinger);
                        }
                        EitherComponents::StaticOnly(vpi)
                    } else {
                        let mut vpi = Vec::with_capacity(params.num_pairs as usize);
                        for ponger_ref in self.pongers.iter() {
                            let (pinger, unique_reg_f) = system.create_and_register(|| {
                                Pinger::with(
                                    params.num_msgs,
                                    params.pipeline,
                                    latch.clone(),
                                    ponger_ref.clone(),
                                )
                            });
                            unique_reg_f
                                .wait_timeout(Duration::from_millis(1000))
                                .expect("Pinger never registered!")
                                .expect("Pinger failed to register!");
                            vpi.push(pinger);
                        }
                        EitherComponents::NonStatic(vpi)
                    };
                    pingers
                        .start_all(system)
                        .expect("Pingers did not start correctly!");

                    self.pingers = pingers;
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
                self.pingers.actor_ref_for_each(|pinger_ref| {
                    pinger_ref.tell(&START, system);
                });
                latch.wait();
            }
            None => unimplemented!(),
        }
    }
    fn cleanup_iteration(&mut self, last_iteration: bool, _exec_time_millis: f64) -> () {
        let system = self.system.take().unwrap();
        self.pingers
            .take()
            .kill_all(&system)
            .expect("Pingers did not shut down correctly!");

        if last_iteration {
            system
                .shutdown()
                .expect("Kompics didn't shut down properly");
            self.params = None;
        } else {
            self.system = Some(system);
        }
    }
}

pub struct PingPongClient {
    system: Option<KompactSystem>,
    pongers: EitherComponents<StaticPonger, Ponger>,
}

impl PingPongClient {
    fn new() -> PingPongClient {
        PingPongClient {
            system: None,
            pongers: EitherComponents::Empty,
        }
    }
}

impl DistributedBenchmarkClient for PingPongClient {
    type ClientConf = ClientParams;
    type ClientData = ClientRefs;

    fn setup(&mut self, c: Self::ClientConf) -> Self::ClientData {
        println!("Setting up ponger.");

        let system = crate::kompact_system_provider::global()
            .new_remote_system("throughputpingpong", num_cpus::get());
        let (pongers, ponger_refs) = if c.static_only {
            let mut vpo = Vec::with_capacity(c.num_pongers as usize);
            let mut vpor = Vec::with_capacity(c.num_pongers as usize);
            for _ in 1..=c.num_pongers {
                let (ponger, unique_reg_f) = system.create_and_register(StaticPonger::new);
                unique_reg_f
                    .wait_timeout(Duration::from_millis(1000))
                    .expect("Ponger never registered!")
                    .expect("Ponger failed to register!");
                let path: ActorPath = (system.system_path(), ponger.id().clone()).into();
                vpor.push(path);
                vpo.push(ponger);
            }
            (EitherComponents::StaticOnly(vpo), vpor)
        } else {
            let mut vpo = Vec::with_capacity(c.num_pongers as usize);
            let mut vpor = Vec::with_capacity(c.num_pongers as usize);
            for _ in 1..=c.num_pongers {
                let (ponger, unique_reg_f) = system.create_and_register(Ponger::new);
                unique_reg_f
                    .wait_timeout(Duration::from_millis(1000))
                    .expect("Ponger never registered!")
                    .expect("Ponger failed to register!");
                let path: ActorPath = (system.system_path(), ponger.id().clone()).into();
                vpor.push(path);
                vpo.push(ponger);
            }
            (EitherComponents::NonStatic(vpo), vpor)
        };
        pongers
            .start_all(&system)
            .expect("Pongers did not start correctly!");

        self.system = Some(system);
        self.pongers = pongers;

        ClientRefs(ponger_refs)
    }

    fn prepare_iteration(&mut self) -> () {
        // nothing to do
        println!("Preparing ponger iteration");
    }

    fn cleanup_iteration(&mut self, last_iteration: bool) -> () {
        println!("Cleaning up ponger side");
        if last_iteration {
            let system = self.system.take().unwrap();
            self.pongers
                .take()
                .kill_all(&system)
                .expect("Pongers did not shut down correctly!");

            system
                .shutdown()
                .expect("Kompact didn't shut down properly");
        }
    }
}

struct PingPongSer;
const PING_PONG_SER: PingPongSer = PingPongSer {};
const STATIC_PING_ID: u8 = 1;
const STATIC_PONG_ID: u8 = 2;
const PING_ID: u8 = 3;
const PONG_ID: u8 = 4;
const SER_ID: u64 = 43;
impl Serialiser<StaticPing> for PingPongSer {
    fn serid(&self) -> u64 {
        SER_ID
    }
    fn size_hint(&self) -> Option<usize> {
        Some(1)
    }
    fn serialise(&self, _v: &StaticPing, buf: &mut BufMut) -> Result<(), SerError> {
        buf.put_u8(STATIC_PING_ID);
        Result::Ok(())
    }
}

impl Serialiser<StaticPong> for PingPongSer {
    fn serid(&self) -> u64 {
        SER_ID
    }
    fn size_hint(&self) -> Option<usize> {
        Some(1)
    }
    fn serialise(&self, _v: &StaticPong, buf: &mut BufMut) -> Result<(), SerError> {
        buf.put_u8(STATIC_PONG_ID);
        Result::Ok(())
    }
}
impl Serialiser<Ping> for PingPongSer {
    fn serid(&self) -> u64 {
        SER_ID
    }
    fn size_hint(&self) -> Option<usize> {
        Some(9)
    }
    fn serialise(&self, v: &Ping, buf: &mut BufMut) -> Result<(), SerError> {
        buf.put_u8(PING_ID);
        buf.put_u64_be(v.index);
        Result::Ok(())
    }
}

impl Serialiser<Pong> for PingPongSer {
    fn serid(&self) -> u64 {
        SER_ID
    }
    fn size_hint(&self) -> Option<usize> {
        Some(9)
    }
    fn serialise(&self, v: &Pong, buf: &mut BufMut) -> Result<(), SerError> {
        buf.put_u8(PONG_ID);
        buf.put_u64_be(v.index);
        Result::Ok(())
    }
}
impl Deserialiser<StaticPing> for PingPongSer {
    fn deserialise(buf: &mut Buf) -> Result<StaticPing, SerError> {
        if buf.remaining() < 1 {
            return Err(SerError::InvalidData(format!(
                "Serialised typed has 1byte but only {}bytes remain in buffer.",
                buf.remaining()
            )));
        }
        match buf.get_u8() {
            STATIC_PING_ID => Ok(StaticPing),
            x => Err(SerError::InvalidType(format!("Found unexpected id {}.", x))),
        }
    }
}
impl Deserialiser<StaticPong> for PingPongSer {
    fn deserialise(buf: &mut Buf) -> Result<StaticPong, SerError> {
        if buf.remaining() < 1 {
            return Err(SerError::InvalidData(format!(
                "Serialised typed has 1byte but only {}bytes remain in buffer.",
                buf.remaining()
            )));
        }
        match buf.get_u8() {
            STATIC_PONG_ID => Ok(StaticPong),
            x => Err(SerError::InvalidType(format!("Found unexpected id {}.", x))),
        }
    }
}
impl Deserialiser<Ping> for PingPongSer {
    fn deserialise(buf: &mut Buf) -> Result<Ping, SerError> {
        if buf.remaining() < 9 {
            return Err(SerError::InvalidData(format!(
                "Serialised typed has 9byte but only {}bytes remain in buffer.",
                buf.remaining()
            )));
        }
        match buf.get_u8() {
            PING_ID => {
                let index = buf.get_u64_be();
                Ok(Ping::new(index))
            }
            x => Err(SerError::InvalidType(format!("Found unexpected id {}.", x))),
        }
    }
}
impl Deserialiser<Pong> for PingPongSer {
    fn deserialise(buf: &mut Buf) -> Result<Pong, SerError> {
        if buf.remaining() < 9 {
            return Err(SerError::InvalidData(format!(
                "Serialised typed has 9byte but only {}bytes remain in buffer.",
                buf.remaining()
            )));
        }
        match buf.get_u8() {
            PONG_ID => {
                let index = buf.get_u64_be();
                Ok(Pong::new(index))
            }
            x => Err(SerError::InvalidType(format!("Found unexpected id {}.", x))),
        }
    }
}

/*****************
 * Static Pinger *
 *****************/

#[derive(ComponentDefinition)]
struct StaticPinger {
    ctx: ComponentContext<StaticPinger>,
    latch: Arc<CountdownEvent>,
    ponger: ActorPath,
    count: u64,
    pipeline: u64,
    sent_count: u64,
    recv_count: u64,
}

impl StaticPinger {
    fn with(
        count: u64,
        pipeline: u64,
        latch: Arc<CountdownEvent>,
        ponger: ActorPath,
    ) -> StaticPinger {
        StaticPinger {
            ctx: ComponentContext::new(),
            latch,
            ponger,
            count,
            pipeline,
            sent_count: 0,
            recv_count: 0,
        }
    }
}

impl Provide<ControlPort> for StaticPinger {
    fn handle(&mut self, _event: ControlEvent) -> () {
        // ignore
    }
}

impl Actor for StaticPinger {
    fn receive_local(&mut self, _sender: ActorRef, msg: &Any) -> () {
        if msg.is::<Start>() {
            let mut pipelined: u64 = 0;
            while (pipelined < self.pipeline) && (self.sent_count < self.count) {
                self.ponger.tell((StaticPing, PING_PONG_SER), self);
                self.sent_count += 1;
                pipelined += 1;
            }
        } else {
            crit!(
                self.ctx.log(),
                "Got unexpected local msg {:?} (tid: {:?})",
                msg,
                msg.type_id(),
            );
            unimplemented!(); // shouldn't happen during the test
        }
    }
    fn receive_message(&mut self, sender: ActorPath, ser_id: u64, buf: &mut Buf) -> () {
        if ser_id == Serialiser::<StaticPong>::serid(&PING_PONG_SER) {
            let r: Result<StaticPong, SerError> = PingPongSer::deserialise(buf);
            match r {
                Ok(_pong) => {
                    self.recv_count += 1;
                    if self.recv_count < self.count {
                        if self.sent_count < self.count {
                            self.ponger.tell((StaticPing, PING_PONG_SER), self);
                            self.sent_count += 1;
                        }
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

/*****************
 * Static Ponger *
 *****************/

#[derive(ComponentDefinition)]
struct StaticPonger {
    ctx: ComponentContext<StaticPonger>,
}

impl StaticPonger {
    fn new() -> StaticPonger {
        StaticPonger {
            ctx: ComponentContext::new(),
        }
    }
}

impl Provide<ControlPort> for StaticPonger {
    fn handle(&mut self, _event: ControlEvent) -> () {
        // ignore
    }
}

impl Actor for StaticPonger {
    fn receive_local(&mut self, _sender: ActorRef, msg: &Any) -> () {
        crit!(
            self.ctx.log(),
            "Got unexpected local msg {:?} (tid: {:?})",
            msg,
            msg.type_id(),
        );
        unimplemented!(); // shouldn't happen during the test
    }
    fn receive_message(&mut self, sender: ActorPath, ser_id: u64, buf: &mut Buf) -> () {
        if ser_id == Serialiser::<StaticPing>::serid(&PING_PONG_SER) {
            let r: Result<StaticPing, SerError> = PingPongSer::deserialise(buf);
            match r {
                Ok(_ping) => {
                    sender.tell((StaticPong, PING_PONG_SER), self);
                }
                Err(e) => error!(self.ctx.log(), "Error deserialising StaticPing: {:?}", e),
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

/*********************
 * Non-Static Pinger *
 *********************/

#[derive(ComponentDefinition)]
struct Pinger {
    ctx: ComponentContext<Pinger>,
    latch: Arc<CountdownEvent>,
    ponger: ActorPath,
    count: u64,
    pipeline: u64,
    sent_count: u64,
    recv_count: u64,
}

impl Pinger {
    fn with(count: u64, pipeline: u64, latch: Arc<CountdownEvent>, ponger: ActorPath) -> Pinger {
        Pinger {
            ctx: ComponentContext::new(),
            latch,
            ponger,
            count,
            pipeline,
            sent_count: 0,
            recv_count: 0,
        }
    }
}

impl Provide<ControlPort> for Pinger {
    fn handle(&mut self, _event: ControlEvent) -> () {
        // ignore
    }
}

impl Actor for Pinger {
    fn receive_local(&mut self, _sender: ActorRef, msg: &Any) -> () {
        if msg.is::<Start>() {
            let mut pipelined: u64 = 0;
            while (pipelined < self.pipeline) && (self.sent_count < self.count) {
                self.ponger
                    .tell((Ping::new(self.sent_count), PING_PONG_SER), self);
                self.sent_count += 1;
                pipelined += 1;
            }
        } else {
            crit!(
                self.ctx.log(),
                "Got unexpected local msg {:?} (tid: {:?})",
                msg,
                msg.type_id()
            );
            unimplemented!(); // shouldn't happen during the test
        }
    }
    fn receive_message(&mut self, sender: ActorPath, ser_id: u64, buf: &mut Buf) -> () {
        if ser_id == Serialiser::<Pong>::serid(&PING_PONG_SER) {
            let r: Result<Pong, SerError> = PingPongSer::deserialise(buf);
            match r {
                Ok(_pong) => {
                    self.recv_count += 1;
                    if self.recv_count < self.count {
                        if self.sent_count < self.count {
                            self.ponger
                                .tell((Ping::new(self.sent_count), PING_PONG_SER), self);
                            self.sent_count += 1;
                        }
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

/*********************
 * Non-Static Ponger *
 *********************/

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
    fn receive_local(&mut self, _sender: ActorRef, msg: &Any) -> () {
        crit!(
            self.ctx.log(),
            "Got unexpected local msg {:?} (tid: {:?})",
            msg,
            msg.type_id()
        );
        unimplemented!(); // shouldn't happen during the test
    }
    fn receive_message(&mut self, sender: ActorPath, ser_id: u64, buf: &mut Buf) -> () {
        if ser_id == Serialiser::<Ping>::serid(&PING_PONG_SER) {
            let r: Result<Ping, SerError> = PingPongSer::deserialise(buf);
            match r {
                Ok(ping) => {
                    sender.tell((Pong::new(ping.index), PING_PONG_SER), self);
                }
                Err(e) => error!(self.ctx.log(), "Error deserialising StaticPing: {:?}", e),
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

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_client_params() {
        let params = ClientParams::new(42, true);
        let param_string = PingPong::client_conf_to_str(params.clone());
        let params_deser = PingPong::str_to_client_conf(param_string).unwrap();
        assert_eq!(params, params_deser);

        let params2 = ClientParams::new(42, false);
        let param_string2 = PingPong::client_conf_to_str(params2.clone());
        let params_deser2 = PingPong::str_to_client_conf(param_string2).unwrap();
        assert_eq!(params2, params_deser2);
    }

    #[test]
    fn test_client_data() {
        let ref1 = ActorPath::Unique(UniquePath::new(
            Transport::LOCAL,
            "127.0.0.1".parse().expect("hardcoded IP"),
            8080,
            Uuid::new_v4(),
        ));
        let ref1_string = ref1.to_string();
        let ref1_deser = ActorPath::from_str(&ref1_string).unwrap();
        assert_eq!(ref1, ref1_deser);
        let ref2 = ActorPath::Unique(UniquePath::new(
            Transport::LOCAL,
            "127.0.0.1".parse().expect("hardcoded IP"),
            8080,
            Uuid::new_v4(),
        ));
        let ref2_string = ref2.to_string();
        let ref2_deser = ActorPath::from_str(&ref2_string).unwrap();
        assert_eq!(ref2, ref2_deser);
        let data = ClientRefs(vec![ref1, ref2]);
        let data_string = PingPong::client_data_to_str(data.clone());
        println!("Serialised data: {}", data_string);
        let data_deser = PingPong::str_to_client_data(data_string).unwrap();
        assert_eq!(data.0, data_deser.0);
    }
}
