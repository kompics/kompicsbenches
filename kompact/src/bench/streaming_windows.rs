use super::*;

use benchmark_suite_shared::kompics_benchmarks::benchmarks::StreamingWindowsRequest;
use kompact::prelude::*;
use parse_duration;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::convert::TryInto;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use synchronoise::CountdownEvent;

pub struct WindowerConfig {
    window_size: Duration,
    batch_size: u64,
    amplification: u64,
    upstream_actor_paths: Vec<ActorPath>,
}
impl WindowerConfig {
    fn new(
        window_size: Duration,
        batch_size: u64,
        amplification: u64,
        upstream_actor_paths: Vec<ActorPath>,
    ) -> WindowerConfig {
        WindowerConfig {
            window_size,
            batch_size,
            amplification,
            upstream_actor_paths,
        }
    }
}
pub struct WindowerRefs(Vec<ActorPath>);

#[derive(Default)]
pub struct StreamingWindows;

impl DistributedBenchmark for StreamingWindows {
    type MasterConf = StreamingWindowsRequest;
    type ClientConf = WindowerConfig;
    type ClientData = WindowerRefs;

    type Master = StreamingWindowsMaster;
    type Client = StreamingWindowsClient;

    const LABEL: &'static str = "StreamingWindows";

    fn new_master() -> Self::Master {
        StreamingWindowsMaster::new()
    }

    fn msg_to_master_conf(
        msg: Box<dyn (::protobuf::Message)>,
    ) -> Result<Self::MasterConf, BenchmarkError> {
        downcast_msg!(msg; StreamingWindowsRequest)
    }

    fn new_client() -> Self::Client {
        StreamingWindowsClient::new()
    }
    fn str_to_client_conf(str: String) -> Result<Self::ClientConf, BenchmarkError> {
        let split: Vec<_> = str.split(',').collect();
        if split.len() != 4 {
            Err(BenchmarkError::InvalidMessage(format!(
                "String '{}' does not represent a client conf!",
                str
            )))
        } else {
            let window_size_str = split[0];
            // Note: The `as_millis` produces a u128, but `from_millis` takes a u64.
            // Should not be an issue, though, since JVM implementations can't deal
            // with u128-sized Durations anyway.
            let window_size = window_size_str
                .parse::<u64>()
                .map(Duration::from_millis)
                .map_err(|e| {
                    BenchmarkError::InvalidMessage(format!(
                        "String '{}' does not represent a client conf: {:?}",
                        str, e
                    ))
                })?;
            let batch_size_str = split[1];
            let batch_size = batch_size_str.parse::<u64>().map_err(|e| {
                BenchmarkError::InvalidMessage(format!(
                    "String '{}' does not represent a client conf: {:?}",
                    str, e
                ))
            })?;
            let amplification_str = split[2];
            let amplification = amplification_str.parse::<u64>().map_err(|e| {
                BenchmarkError::InvalidMessage(format!(
                    "String '{}' does not represent a client conf: {:?}",
                    str, e
                ))
            })?;
            let paths = split[3];
            let actor_paths_res: Result<Vec<ActorPath>, _> =
                paths.split(';').map(|s| ActorPath::from_str(s)).collect();
            let actor_paths = actor_paths_res.map_err(|e| {
                BenchmarkError::InvalidMessage(format!("Could not read client conf: {}", e))
            })?;
            Ok(WindowerConfig::new(
                window_size,
                batch_size,
                amplification,
                actor_paths,
            ))
        }
    }

    fn str_to_client_data(str: String) -> Result<Self::ClientData, BenchmarkError> {
        let res: Result<Vec<_>, _> = str.split(',').map(|s| ActorPath::from_str(s)).collect();
        res.map(|paths| WindowerRefs(paths)).map_err(|e| {
            BenchmarkError::InvalidMessage(format!("Could not read client data: {}", e))
        })
    }

    fn client_conf_to_str(c: Self::ClientConf) -> String {
        let paths = c
            .upstream_actor_paths
            .into_iter()
            .map(|path| path.to_string())
            .collect::<Vec<String>>()
            .join(";");
        format!(
            "{},{},{},{}",
            c.window_size.as_millis(),
            c.batch_size,
            c.amplification,
            paths
        )
    }
    fn client_data_to_str(d: Self::ClientData) -> String {
        d.0.into_iter()
            .map(|path| path.to_string())
            .collect::<Vec<String>>()
            .join(",")
    }
}

struct Params {
    number_of_partitions: u32,
    batch_size: u64,
    window_size: Duration,
    number_of_windows: u64,
    window_size_amplification: u64,
}
impl Params {
    fn from_req(msg: &StreamingWindowsRequest) -> Result<Params, BenchmarkError> {
        let window_size = parse_duration::parse(&msg.window_size).map_err(|e| {
            BenchmarkError::InvalidMessage(format!(
                "Could not read window_size {} into Duration instance! Error was: {}",
                msg.window_size, e
            ))
        })?;
        Ok(Params {
            number_of_partitions: msg.number_of_partitions,
            batch_size: msg.batch_size,
            window_size,
            number_of_windows: msg.number_of_windows,
            window_size_amplification: msg.window_size_amplification,
        })
    }
}

const REG_TIMEOUT: Duration = Duration::from_secs(6);
const FLUSH_TIMEOUT: Duration = Duration::from_secs(60);

pub struct StreamingWindowsMaster {
    params: Option<Params>,
    system: Option<KompactSystem>,
    latch: Option<Arc<CountdownEvent>>,
    sources: Vec<(u32, Arc<Component<StreamSource>>)>,
    sinks: Vec<Arc<Component<StreamSink>>>,
    sink_refs: Vec<ActorRefStrong<SinkMsg>>,
}

impl StreamingWindowsMaster {
    fn new() -> StreamingWindowsMaster {
        StreamingWindowsMaster {
            params: None,
            system: None,
            latch: None,
            sources: Vec::new(),
            sinks: Vec::new(),
            sink_refs: Vec::new(),
        }
    }
}

impl DistributedBenchmarkMaster for StreamingWindowsMaster {
    type MasterConf = StreamingWindowsRequest;
    type ClientConf = WindowerConfig;
    type ClientData = WindowerRefs;

    fn setup(
        &mut self,
        c: Self::MasterConf,
        _m: &DeploymentMetaData,
    ) -> Result<Self::ClientConf, BenchmarkError> {
        let params = Params::from_req(&c)?;
        let system = crate::kompact_system_provider::global()
            .new_remote_system("streamingwindows");

        let mut sources: Vec<ActorPath> = Vec::new();
        for pid in 0..params.number_of_partitions {
            let (source, req_f) = system.create_and_register(|| StreamSource::with(pid));
            let source_path = req_f.wait_expect(REG_TIMEOUT, "Source failed to register!");
            system
                .start_notify(&source)
                .wait_timeout(REG_TIMEOUT)
                .expect("Source failed to start!");
            self.sources.push((pid, source));
            sources.push(source_path);
        }
        self.system = Some(system);
        let client_conf = WindowerConfig::new(
            params.window_size,
            params.batch_size,
            params.window_size_amplification,
            sources,
        );
        self.params = Some(params);
        Ok(client_conf)
    }

    fn prepare_iteration(&mut self, d: Vec<Self::ClientData>) -> () {
        let mut windowers = d[0].0.clone();
        if let Some(ref params) = self.params {
            if let Some(ref system) = self.system {
                assert_eq!(windowers.len(), params.number_of_partitions as usize);
                let latch = Arc::new(CountdownEvent::new(params.number_of_partitions as usize));
                for windower in windowers.drain(..) {
                    let (sink, req_f) = system.create_and_register(|| {
                        StreamSink::with(latch.clone(), params.number_of_windows, windower)
                    });
                    let _sink_path = req_f.wait_expect(REG_TIMEOUT, "Sink failed to register!");

                    system
                        .start_notify(&sink)
                        .wait_timeout(REG_TIMEOUT)
                        .expect("Sink failed to start!");
                    self.sink_refs
                        .push(sink.actor_ref().hold().expect("Live ref"));
                    self.sinks.push(sink);
                }
                self.latch = Some(latch);
            } else {
                unimplemented!();
            }
        } else {
            unimplemented!();
        }
    }
    fn run_iteration(&mut self) -> () {
        if let Some(ref _system) = self.system {
            let latch = self.latch.take().unwrap();
            self.sink_refs.iter().for_each(|sink_ref| {
                sink_ref.tell(SinkMsg::Run);
            });
            latch.wait();
        } else {
            unimplemented!()
        }
    }
    fn cleanup_iteration(&mut self, last_iteration: bool, _exec_time_millis: f64) -> () {
        let system = self.system.take().expect("System during cleanup");
        let reset_futures: Vec<KFuture<()>> = self
            .sources
            .iter()
            .map(|source| {
                let source_ref = source.1.actor_ref();
                source_ref.ask(|promise| SourceMsg::Reset(Ask::new(promise, ())))
            })
            .collect();
        self.sink_refs.clear();
        let stop_futures: Vec<KFuture<()>> = self
            .sinks
            .drain(..)
            .map(|sink| system.kill_notify(sink))
            .collect();
        for f in stop_futures {
            f.wait_timeout(REG_TIMEOUT)
                .expect("A sink didn't shut down in time!");
        }
        for f in reset_futures {
            f.wait_timeout(FLUSH_TIMEOUT)
                .expect("A source failed to flush in time!");
        }
        if last_iteration {
            let stop_futures: Vec<KFuture<()>> = self
                .sources
                .drain(..)
                .map(|source| system.kill_notify(source.1))
                .collect();
            for f in stop_futures {
                f.wait_timeout(REG_TIMEOUT)
                    .expect("A source didn't shut down in time!");
            }
            system
                .shutdown()
                .expect("Kompact didn't shut down properly");
            self.params = None;
        } else {
            self.system = Some(system);
        }
    }
}

pub struct StreamingWindowsClient {
    system: Option<KompactSystem>,
    windowers: Vec<Arc<Component<Windower>>>,
}
impl StreamingWindowsClient {
    fn new() -> StreamingWindowsClient {
        StreamingWindowsClient {
            system: None,
            windowers: Vec::new(),
        }
    }
}

impl DistributedBenchmarkClient for StreamingWindowsClient {
    type ClientConf = WindowerConfig;
    type ClientData = WindowerRefs;

    fn setup(&mut self, mut c: Self::ClientConf) -> Self::ClientData {
        println!("Setting up windowers.");

        let system = crate::kompact_system_provider::global()
            .new_remote_system("streamingwindows");

        let window_size = c.window_size;
        let batch_size = c.batch_size;
        let amplification = c.amplification;

        let mut windowers: Vec<ActorPath> = Vec::new();
        for source in c.upstream_actor_paths.drain(..) {
            let (windower, reg_f) = system.create_and_register(|| {
                Windower::with(window_size, batch_size, amplification, source)
            });
            let windower_path = reg_f.wait_expect(REG_TIMEOUT, "Windower failed to register!");

            system
                .start_notify(&windower)
                .wait_timeout(REG_TIMEOUT)
                .expect("Windower failed to start!");

            self.windowers.push(windower);
            windowers.push(windower_path);
        }
        self.system = Some(system);
        WindowerRefs(windowers)
    }

    fn prepare_iteration(&mut self) -> () {
        // nothing to do
        println!("Preparing windower iteration");
    }

    fn cleanup_iteration(&mut self, last_iteration: bool) -> () {
        println!("Cleaning up windower side");
        if last_iteration {
            let system = self.system.take().unwrap();
            let stop_futures: Vec<KFuture<()>> = self
                .windowers
                .drain(..)
                .map(|windower| system.kill_notify(windower))
                .collect();
            for f in stop_futures {
                f.wait_timeout(REG_TIMEOUT)
                    .expect("A windower didn't shut down in time!");
            }

            system
                .shutdown()
                .expect("Kompact didn't shut down properly");
        }
    }
}

#[derive(Debug)]
enum WindowerMsg {
    Start,
    Stop,
    Event {
        ts: u64,
        partition_id: u32,
        value: i64,
    },
    Flush,
}
impl WindowerMsg {
    const SERID: SerId = serialiser_ids::SW_WINDOWER_ID;

    const START_FLAG: u8 = 1u8;
    const STOP_FLAG: u8 = 2u8;
    const EVENT_FLAG: u8 = 3u8;
    const FLUSH_FLAG: u8 = 4u8;

    pub fn event(ts: u64, partition_id: u32, value: i64) -> WindowerMsg {
        WindowerMsg::Event {
            ts,
            partition_id,
            value,
        }
    }
}
impl Serialisable for WindowerMsg {
    fn ser_id(&self) -> SerId {
        Self::SERID
    }
    fn size_hint(&self) -> Option<usize> {
        match self {
            WindowerMsg::Start => Some(1),
            WindowerMsg::Stop => Some(1),
            WindowerMsg::Event { .. } => Some(21),
            WindowerMsg::Flush => Some(1),
        }
    }
    fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
        match self {
            WindowerMsg::Start => buf.put_u8(Self::START_FLAG),
            WindowerMsg::Stop => buf.put_u8(Self::STOP_FLAG),
            WindowerMsg::Event {
                ts,
                partition_id,
                value,
            } => {
                buf.put_u8(Self::EVENT_FLAG);
                buf.put_u64_be(*ts);
                buf.put_u32_be(*partition_id);
                buf.put_i64_be(*value);
            }
            WindowerMsg::Flush => buf.put_u8(Self::FLUSH_FLAG),
        }
        Ok(())
    }

    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        Ok(self)
    }
}
impl Deserialiser<WindowerMsg> for WindowerMsg {
    const SER_ID: SerId = Self::SERID;
    fn deserialise(buf: &mut dyn Buf) -> Result<WindowerMsg, SerError> {
        let flag = buf.get_u8();
        match flag {
            Self::START_FLAG => Ok(WindowerMsg::Start),
            Self::STOP_FLAG => Ok(WindowerMsg::Stop),
            Self::EVENT_FLAG => {
                let ts = buf.get_u64_be();
                let pid = buf.get_u32_be();
                let value = buf.get_i64_be();
                Ok(WindowerMsg::event(ts, pid, value))
            }
            Self::FLUSH_FLAG => Ok(WindowerMsg::Flush),
            _ => Err(SerError::InvalidData(format!("Unknown flag: {}", flag))),
        }
    }
}

#[derive(ComponentDefinition)]
struct Windower {
    ctx: ComponentContext<Self>,
    window_size_ms: u64,
    batch_size: u64,
    amplification: u64,
    upstream: ActorPath,
    ready_mark: u64,
    downstream: Option<ActorPath>,
    current_window: Vec<i64>,
    window_start_ts: u64,
    received_since_ready: u64,
    running: bool,
}
impl Windower {
    fn with(
        window_size: Duration,
        batch_size: u64,
        amplification: u64,
        upstream: ActorPath,
    ) -> Windower {
        let window_size_ms = window_size.as_millis() as u64; // see above for cast explanation
        let ready_mark = batch_size / 2;
        Windower {
            ctx: ComponentContext::new(),
            window_size_ms,
            batch_size,
            amplification,
            upstream,
            ready_mark,
            downstream: None,
            current_window: Vec::new(),
            window_start_ts: 0u64,
            received_since_ready: 0u64,
            running: false,
        }
    }

    #[inline(always)]
    fn window_end(&self) -> u64 {
        self.window_start_ts + self.window_size_ms
    }

    fn trigger_window(&mut self, partition_id: u32) {
        debug!(
            self.ctx.log(),
            "Triggering window with {} messages.",
            self.current_window.len()
        );
        if self.current_window.is_empty() {
            warn!(
                self.ctx.log(),
                "Windows should not be empty in the benchmark!"
            );
            return;
        }
        self.current_window.sort_unstable();
        let len = self.current_window.len();
        let median = if len % 2 == 0 {
            let upper_middle = len / 2;
            let lower_middle = upper_middle - 1;
            let um_value = self.current_window[upper_middle];
            let lm_value = self.current_window[lower_middle];
            match lm_value.checked_add(um_value) {
                Some(sum) => (sum as f64) / 2.0,
                None => {
                    let um_double = (um_value as f64) / 2.0;
                    let lm_double = (lm_value as f64) / 2.0;
                    lm_double + um_double
                }
            }
        } else {
            let middle = len / 2;
            self.current_window[middle] as f64
        };
        self.current_window.clear();
        if let Some(ref downstream) = self.downstream {
            let msg = SinkMsg::WindowAggregate {
                ts: self.window_start_ts,
                partition_id,
                value: median,
            };
            downstream
                .tell_ser(msg.serialised(), self)
                .expect("Should serialise");
        } else {
            unimplemented!();
        }
    }

    fn manage_ready(&mut self) {
        self.received_since_ready += 1u64;
        if self.received_since_ready > self.ready_mark {
            debug!(self.ctx.log(), "Sending Ready");
            let msg = SourceMsg::Ready {
                batch_size: self.received_since_ready,
            };
            self.upstream
                .tell_ser(msg.serialised(), self)
                .expect("Should serialise");
            self.received_since_ready = 0u64;
        }
    }
}

ignore_control!(Windower);

impl NetworkActor for Windower {
    type Message = WindowerMsg;
    type Deserialiser = WindowerMsg;

    fn receive(&mut self, sender: Option<ActorPath>, msg: Self::Message) -> () {
        match msg {
            WindowerMsg::Start if !self.running => {
                debug!(self.ctx.log(), "Got Start");
                self.downstream = sender;
                self.running = true;
                self.received_since_ready = 0u64;
                let msg = SourceMsg::Ready {
                    batch_size: self.batch_size,
                };
                self.upstream
                    .tell_ser(msg.serialised(), self)
                    .expect("Should serialise!");
            }
            WindowerMsg::Stop if self.running => {
                debug!(self.ctx.log(), "Got Stop");
                self.running = false;
                self.current_window.clear();
                self.downstream = None;
                self.window_start_ts = 0u64;
            }
            WindowerMsg::Event {
                ts,
                partition_id: pid,
                value,
            } if self.running => {
                if ts >= self.window_end() {
                    self.trigger_window(pid);
                    self.window_start_ts = ts;
                }
                let reserve: usize = self.amplification.try_into().unwrap_or(std::usize::MAX);
                self.current_window.reserve(reserve);
                for _i in 0..self.amplification {
                    self.current_window.push(value);
                }
                self.manage_ready();
            }
            WindowerMsg::Event { .. } if !self.running => {
                debug!(self.ctx.log(), "Dropping event, since not running");
            }
            WindowerMsg::Flush => {
                self.upstream
                    .tell_ser(SourceMsg::Flushed.serialised(), self)
                    .expect("Should serialise");
            }
            _ => unimplemented!("Unexpected message {:?} when running={}", msg, self.running),
        }
    }
}

#[derive(Debug)]
pub enum SourceMsg {
    Reset(Ask<(), ()>),
    Next,
    Ready { batch_size: u64 },
    Flushed,
}
impl SourceMsg {
    const SERID: SerId = serialiser_ids::SW_SOURCE_ID;

    const READY_FLAG: u8 = 1u8;
    const FLUSHED_FLAG: u8 = 2u8;
}
impl Serialisable for SourceMsg {
    fn ser_id(&self) -> SerId {
        Self::SERID
    }
    fn size_hint(&self) -> Option<usize> {
        match self {
            SourceMsg::Reset(_) => None, // don't serialise
            SourceMsg::Next => None,     // don't serialise
            SourceMsg::Ready { .. } => Some(9),
            SourceMsg::Flushed => Some(1),
        }
    }
    fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
        match self {
            SourceMsg::Reset(_) => unimplemented!("Reset shouldn't be sent over network!"),
            SourceMsg::Next => unimplemented!("Next shouldn't be sent over network!"),
            SourceMsg::Ready { batch_size: bs } => {
                buf.put_u8(Self::READY_FLAG);
                buf.put_u64_be(*bs);
            }
            SourceMsg::Flushed => buf.put_u8(Self::FLUSHED_FLAG),
        }
        Ok(())
    }

    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        Ok(self)
    }
}
impl Deserialiser<SourceMsg> for SourceMsg {
    const SER_ID: SerId = Self::SERID;
    fn deserialise(buf: &mut dyn Buf) -> Result<SourceMsg, SerError> {
        let flag = buf.get_u8();
        match flag {
            Self::READY_FLAG => {
                let bs = buf.get_u64_be();
                Ok(SourceMsg::Ready { batch_size: bs })
            }
            Self::FLUSHED_FLAG => Ok(SourceMsg::Flushed),
            _ => Err(SerError::InvalidData(format!("Unknown flag: {}", flag))),
        }
    }
}

#[derive(ComponentDefinition)]
pub struct StreamSource {
    ctx: ComponentContext<Self>,
    partition_id: u32,
    random: SmallRng,
    downstream: Option<ActorPath>,
    remaining: u64,
    current_ts: u64,
    flushing: bool,
    reply_on_flushed: Option<Ask<(), ()>>,
}
impl StreamSource {
    pub fn with(partition_id: u32) -> StreamSource {
        let random = SmallRng::seed_from_u64(partition_id as u64);
        StreamSource {
            ctx: ComponentContext::new(),
            partition_id,
            random,
            downstream: None,
            remaining: 0u64,
            current_ts: 0u64,
            flushing: false,
            reply_on_flushed: None,
        }
    }

    fn send(&mut self) {
        if self.remaining > 0u64 {
            if let Some(ref downstream) = self.downstream {
                let msg = WindowerMsg::event(self.current_ts, self.partition_id, self.random.gen());
                downstream
                    .tell_ser(msg.serialised(), self)
                    .expect("Should serialise!");
            } else {
                unimplemented!();
            }
            self.current_ts += 1u64;
            self.remaining -= 1u64;
            if self.remaining > 0u64 {
                self.ctx().actor_ref().tell(SourceMsg::Next);
            } else {
                debug!(self.ctx().log(), "Waiting for Ready...");
            }
        }
    }
}
ignore_control!(StreamSource);
impl NetworkActor for StreamSource {
    type Message = SourceMsg;
    type Deserialiser = SourceMsg;

    fn receive(&mut self, sender: Option<ActorPath>, msg: Self::Message) -> () {
        match msg {
            SourceMsg::Ready { batch_size } if !self.flushing => {
                debug!(self.ctx().log(), "Got Ready!");
                self.remaining += batch_size;
                self.downstream = sender;
                self.send();
            }
            SourceMsg::Ready { .. } if self.flushing => (), // drop
            SourceMsg::Next if !self.flushing => self.send(),
            SourceMsg::Next if self.flushing => (), // drop
            SourceMsg::Reset(ask) if !self.flushing => {
                debug!(self.ctx().log(), "Got Reset");
                self.flushing = true;
                self.reply_on_flushed = Some(ask);
                if let Some(downstream) = self.downstream.take() {
                    downstream
                        .tell_ser(WindowerMsg::Flush.serialised(), self)
                        .expect("serialise!");
                } else {
                    unimplemented!();
                }
                self.current_ts = 0u64;
                self.remaining = 0u64;
            }
            SourceMsg::Flushed if self.flushing => {
                debug!(self.ctx().log(), "Got Flushed");
                if let Some(ask) = self.reply_on_flushed.take() {
                    ask.reply(()).expect("Sent");
                } else {
                    unimplemented!();
                }
                self.flushing = false;
            }
            _ => unimplemented!(
                "Unexpected message {:?} when flshing={}",
                msg,
                self.flushing
            ),
        }
    }
}

#[derive(Debug)]
pub enum SinkMsg {
    Run,
    WindowAggregate {
        ts: u64,
        partition_id: u32,
        value: f64,
    },
}
impl SinkMsg {
    const SERID: SerId = serialiser_ids::SW_SINK_ID;

    pub fn window(ts: u64, pid: u32, value: f64) -> SinkMsg {
        SinkMsg::WindowAggregate {
            ts,
            partition_id: pid,
            value,
        }
    }
}
impl Serialisable for SinkMsg {
    fn ser_id(&self) -> SerId {
        Self::SERID
    }
    fn size_hint(&self) -> Option<usize> {
        match self {
            SinkMsg::Run => None, // don't serialise
            SinkMsg::WindowAggregate { .. } => Some(20),
        }
    }
    fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
        match self {
            SinkMsg::Run => unimplemented!("Run shouldn't be sent over network!"),
            SinkMsg::WindowAggregate {
                ts,
                partition_id,
                value,
            } => {
                buf.put_u64_be(*ts);
                buf.put_u32_be(*partition_id);
                buf.put_f64_be(*value);
            }
        }
        Ok(())
    }

    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        Ok(self)
    }
}
impl Deserialiser<SinkMsg> for SinkMsg {
    const SER_ID: SerId = Self::SERID;
    fn deserialise(buf: &mut dyn Buf) -> Result<SinkMsg, SerError> {
        if buf.remaining() < 20 {
            Err(SerError::InvalidData(format!(
                "Need {} bytes, but only {} remain in buffer!",
                20,
                buf.remaining()
            )))
        } else {
            let ts = buf.get_u64_be();
            let pid = buf.get_u32_be();
            let value = buf.get_f64_be();
            Ok(SinkMsg::window(ts, pid, value))
        }
    }
}

#[derive(ComponentDefinition)]
pub struct StreamSink {
    ctx: ComponentContext<Self>,
    latch: Arc<CountdownEvent>,
    number_of_windows: u64,
    upstream: ActorPath,
    window_count: u64,
}
impl StreamSink {
    pub fn with(
        latch: Arc<CountdownEvent>,
        number_of_windows: u64,
        upstream: ActorPath,
    ) -> StreamSink {
        StreamSink {
            ctx: ComponentContext::new(),
            latch,
            number_of_windows,
            upstream,
            window_count: 0u64,
        }
    }
}
ignore_control!(StreamSink);
impl NetworkActor for StreamSink {
    type Message = SinkMsg;
    type Deserialiser = SinkMsg;

    fn receive(&mut self, _sender: Option<ActorPath>, msg: Self::Message) {
        match msg {
            SinkMsg::Run => {
                debug!(self.ctx().log(), "Got Run");
                self.upstream
                    .tell_ser(WindowerMsg::Start.serialised(), self)
                    .expect("Should serialise");
            }
            SinkMsg::WindowAggregate { value: median, .. } => {
                debug!(self.ctx().log(), "Got window with median={}", median);
                self.window_count += 1u64;
                if self.window_count == self.number_of_windows {
                    self.latch.decrement().expect("Latch should decrement");
                    self.upstream
                        .tell_ser(WindowerMsg::Stop.serialised(), self)
                        .expect("Should serialise");
                    debug!(self.ctx().log(), "Done!");
                } else {
                    debug!(
                        self.ctx().log(),
                        "Got {}/{} windows.", self.window_count, self.number_of_windows
                    );
                }
            }
        }
    }
}
