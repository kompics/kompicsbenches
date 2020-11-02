use super::*;

use crate::bench::messages::SizedThroughputMessage;
use benchmark_suite_shared::kompics_benchmarks::benchmarks::SizedThroughputRequest;
use kompact::prelude::{ser_helpers::preserialise_msg, *};
use std::{borrow::BorrowMut, fmt::Debug, ops::Deref, str::FromStr, sync::Arc, time::Duration};
use synchronoise::CountdownEvent;

pub struct SizedRefs(Vec<ActorPath>);

#[derive(Default)]
pub struct SizedThroughputBenchmark;

impl DistributedBenchmark for SizedThroughputBenchmark {
    type MasterConf = SizedThroughputRequest;
    type ClientConf = SizedThroughputRequest;
    type ClientData = SizedRefs;

    type Master = SizedThroughputMaster;
    type Client = SizedThroughputClient;

    const LABEL: &'static str = "SizedThroughput";

    fn new_master() -> Self::Master {
        SizedThroughputMaster::new()
    }

    fn msg_to_master_conf(
        msg: Box<dyn (::protobuf::Message)>,
    ) -> Result<Self::MasterConf, BenchmarkError> {
        downcast_msg!(msg; SizedThroughputRequest)
    }

    fn new_client() -> Self::Client {
        SizedThroughputClient::new()
    }
    fn str_to_client_conf(str: String) -> Result<Self::ClientConf, BenchmarkError> {
        let split: Vec<_> = str.split(',').collect();
        if split.len() != 4 {
            Err(BenchmarkError::InvalidMessage(format!(
                "String '{}' does not represent a client conf!",
                str
            )))
        } else {
            let message_size = split[0];
            let message_size = message_size.parse::<u32>().map_err(|e| {
                BenchmarkError::InvalidMessage(format!(
                    "String '{}' does not represent a client conf: {:?}",
                    str, e
                ))
            })?;
            let batch_size_str = split[1];
            let batch_size = batch_size_str.parse::<u32>().map_err(|e| {
                BenchmarkError::InvalidMessage(format!(
                    "String '{}' does not represent a client conf: {:?}",
                    str, e
                ))
            })?;
            let number_of_batches_str = split[2];
            let number_of_batches = number_of_batches_str.parse::<u32>().map_err(|e| {
                BenchmarkError::InvalidMessage(format!(
                    "String '{}' does not represent a client conf: {:?}",
                    str, e
                ))
            })?;
            let number_of_pairs = split[3];
            let number_of_pairs = number_of_pairs.parse::<u32>().map_err(|e| {
                BenchmarkError::InvalidMessage(format!(
                    "String '{}' does not represent a client conf: {:?}",
                    str, e
                ))
            })?;
            let mut client_conf = SizedThroughputRequest::new();
            client_conf.set_message_size(message_size);
            client_conf.set_batch_size(batch_size);
            client_conf.set_number_of_batches(number_of_batches);
            client_conf.set_number_of_pairs(number_of_pairs);

            Ok(client_conf)
        }
    }

    fn str_to_client_data(str: String) -> Result<Self::ClientData, BenchmarkError> {
        let res: Result<Vec<_>, _> = str.split(',').map(|s| ActorPath::from_str(s)).collect();
        res.map(|paths| SizedRefs(paths)).map_err(|e| {
            BenchmarkError::InvalidMessage(format!("Could not read client data: {}", e))
        })
    }

    fn client_conf_to_str(c: Self::ClientConf) -> String {
        format!(
            "{},{},{},{}",
            c.message_size, c.batch_size, c.number_of_batches, c.number_of_pairs,
        )
    }
    fn client_data_to_str(d: Self::ClientData) -> String {
        d.0.into_iter()
            .map(|path| path.to_string())
            .collect::<Vec<String>>()
            .join(",")
    }
}

const REG_TIMEOUT: Duration = Duration::from_secs(6);
const FLUSH_TIMEOUT: Duration = Duration::from_secs(60);

pub struct SizedThroughputMaster {
    params: Option<SizedThroughputRequest>,
    system: Option<KompactSystem>,
    sources: Vec<(Arc<Component<SizedThroughputSource>>)>,
    //sinks: Vec<Arc<Component<SizedThroughputSink>>>,
    source_refs: Vec<ActorRef<SourceMsg>>,
}

impl SizedThroughputMaster {
    fn new() -> SizedThroughputMaster {
        SizedThroughputMaster {
            params: None,
            system: None,
            sources: Vec::new(),
            // sinks: Vec::new(),
            source_refs: Vec::new(),
        }
    }
}

impl DistributedBenchmarkMaster for SizedThroughputMaster {
    type MasterConf = SizedThroughputRequest;
    type ClientConf = SizedThroughputRequest;
    type ClientData = SizedRefs;

    fn setup(
        &mut self,
        c: Self::MasterConf,
        _m: &DeploymentMetaData,
    ) -> Result<Self::ClientConf, BenchmarkError> {
        let system = crate::kompact_system_provider::global().new_remote_system("SizedThroughput");
        let client_conf = c.clone();
        let params = c.clone();
        println!(
            "Set up Master pairs: {}, batches: {} msg_size: {}, batch_size: {}",
            c.number_of_pairs, c.number_of_batches, c.message_size, c.batch_size
        );

        self.system = Some(system);
        self.params = Some(params);
        Ok(client_conf)
    }

    fn prepare_iteration(&mut self, mut d: Vec<Self::ClientData>) -> () {
        if self.sources.len() == 0 {
            println!("Preparing master for first iteration");
            let sinks = &mut d[0].0;
            let params = self.params.take().unwrap();
            assert_eq!(
                params.number_of_pairs,
                sinks.len() as u32,
                "num sinks should be num_pairs"
            );
            if let Some(system) = &mut self.system {
                for sink in sinks {
                    let (source, req_f) = system.create_and_register(|| {
                        SizedThroughputSource::with(
                            params.get_message_size(),
                            params.get_batch_size(),
                            params.get_number_of_batches(),
                            sink.clone(),
                        )
                    });
                    let _ = req_f.wait_expect(REG_TIMEOUT, "Source failed to register!");
                    system
                        .start_notify(&source)
                        .wait_timeout(REG_TIMEOUT)
                        .expect("Source failed to start!");
                    self.source_refs.push(source.actor_ref().clone());
                    self.sources.push(source);
                }
            } else {
                panic!("faulty set-up");
            }
            self.params = Some(params);
        } else {
            println!("Preparing master iteration");
        }
    }

    fn run_iteration(&mut self) -> () {
        if let Some(ref _system) = self.system {
            let latch = Arc::new(CountdownEvent::new(self.source_refs.len()));
            self.source_refs.iter().for_each(|source_ref| {
                source_ref.tell(SourceMsg::Run(Some(latch.clone())));
            });
            latch.wait();
        } else {
            unimplemented!()
        }
    }
    fn cleanup_iteration(&mut self, last_iteration: bool, _exec_time_millis: f64) -> () {
        if last_iteration {
            println!("Cleaning up sources for SizedThroughput, last iteration");
            if let Some(system) = self.system.take() {
                for (source) in self.sources.drain(..) {
                    system.kill(source);
                }
                system
                    .shutdown()
                    .expect("Kompact didn't shut down properly");
            }
        } else {
            //println!("Cleaning up sources for SizedThroughput iteration, doing nothing");
        }
    }
}

pub struct SizedThroughputClient {
    system: Option<KompactSystem>,
    sinks: Vec<Arc<Component<SizedThroughputSink>>>,
}
impl SizedThroughputClient {
    fn new() -> SizedThroughputClient {
        SizedThroughputClient {
            system: None,
            sinks: Vec::new(),
        }
    }
}

impl DistributedBenchmarkClient for SizedThroughputClient {
    type ClientConf = SizedThroughputRequest;
    type ClientData = SizedRefs;

    fn setup(&mut self, c: Self::ClientConf) -> Self::ClientData {
        let system = crate::kompact_system_provider::global().new_remote_system("SizedThroughput");

        let mut sinks: Vec<ActorPath> = Vec::new();
        for _ in 0..c.number_of_pairs {
            let (sink, reg_f) =
                system.create_and_register(|| SizedThroughputSink::new(c.batch_size));
            let sink_path = reg_f.wait_expect(REG_TIMEOUT, "Sink failed to register!");

            system
                .start_notify(&sink)
                .wait_timeout(REG_TIMEOUT)
                .expect("Sink failed to start!");

            self.sinks.push(sink);
            sinks.push(sink_path);
        }
        println!("Set up Client");
        self.system = Some(system);
        SizedRefs(sinks)
    }

    fn prepare_iteration(&mut self) -> () {
        // nothing to do
        println!("Preparing sinks for SizedThroughput");
    }

    fn cleanup_iteration(&mut self, last_iteration: bool) -> () {
        if last_iteration {
            println!("Cleaning up sinks for SizedThroughput, last iteration");
            if let Some(system) = self.system.take() {
                let mut kill_futures = Vec::new();
                for sink in self.sinks.drain(..) {
                    let kf = system.kill_notify(sink);
                    kill_futures.push(kf);
                }
                for kf in kill_futures {
                    kf.wait_timeout(Duration::from_millis(1000))
                        .expect("Sink Actor never died!");
                }
                system
                    .shutdown()
                    .expect("Kompact didn't shut down properly");
            }
        } else {
            println!("Cleaning up sinks for SizedThroughput iteration, doing nothing");
        }
    }
}

#[derive(ComponentDefinition)]
pub struct SizedThroughputSource {
    ctx: ComponentContext<Self>,
    latch: Option<Arc<CountdownEvent>>,
    downstream: ActorPath,
    message_size: u32,
    message: SizedThroughputMessage,
    batch_size: u32,
    number_of_batches: u32,
    sent_batches: u32,
    acked_batches: u32,
}

impl SizedThroughputSource {
    pub fn with(
        message_size: u32,
        batch_size: u32,
        number_of_batches: u32,
        downstream: ActorPath,
    ) -> SizedThroughputSource {
        SizedThroughputSource {
            ctx: ComponentContext::uninitialised(),
            latch: None,
            downstream,
            message_size,
            batch_size,
            message: SizedThroughputMessage::new(message_size as usize),
            number_of_batches,
            sent_batches: 0,
            acked_batches: 0,
        }
    }

    fn send(&mut self) {
        self.sent_batches += 1;
        for _ in 0..self.batch_size {
            // Allows serialising by reference while still serialising on each send.
            let preserialised = self.ctx.preserialise(&self.message).expect("preserialise");
            self.downstream
                .tell_preserialised(preserialised, self)
                .expect("tell preserialised");
        }
    }
}

impl ComponentLifecycle for SizedThroughputSource {
    fn on_start(&mut self) -> Handled {
        Handled::Ok
    }
}
//ignore_lifecycle!(SizedThroughputSource);

impl NetworkActor for SizedThroughputSource {
    type Message = SourceMsg;
    type Deserialiser = SourceMsg;

    fn receive(&mut self, _: Option<ActorPath>, msg: Self::Message) -> Handled {
        match msg {
            SourceMsg::Ack => {
                self.acked_batches += 1;
                if self.sent_batches < self.number_of_batches {
                    // Send the next batch
                    self.send();
                } else if self.acked_batches == self.number_of_batches {
                    // Finished
                    self.latch
                        .take()
                        .expect("Should have a latch")
                        .decrement()
                        .expect("Should decrement");
                }
            }
            SourceMsg::Run(latch) => {
                self.latch = latch;
                // We start the experiment, set current_batch to 0
                // Keep two batches in flight throughout the experiment:
                self.sent_batches = 0;
                self.acked_batches = 0;
                self.send();
                self.send();
            }
        }
        Handled::Ok
    }
}

#[derive(ComponentDefinition)]
pub struct SizedThroughputSink {
    ctx: ComponentContext<Self>,
    batch_size: u32,
    received: u32,
}
impl SizedThroughputSink {
    pub fn new(batch_size: u32) -> SizedThroughputSink {
        SizedThroughputSink {
            ctx: ComponentContext::uninitialised(),
            batch_size,
            received: 0,
        }
    }
}

ignore_lifecycle!(SizedThroughputSink);

impl NetworkActor for SizedThroughputSink {
    type Message = SizedThroughputMessage;
    type Deserialiser = SizedThroughputMessage;

    fn receive(&mut self, sender: Option<ActorPath>, msg: Self::Message) -> Handled {
        self.received += msg.aux as u32;
        if self.received == self.batch_size {
            if let Some(source) = sender {
                // Ack the batch and reset the received counter
                source.tell(SourceMsg::Ack, self);
                self.received = 0;
            } else {
                panic!("No source for the Ack message!")
            }
        }
        Handled::Ok
    }
}

#[derive(Clone)]
pub enum SourceMsg {
    Run(Option<Arc<CountdownEvent>>),
    Ack,
}

impl SourceMsg {
    const SERID: SerId = serialiser_ids::STP_SOURCE_ID;
    const RUN_FLAG: u8 = 1u8;
    const ACK_FLAG: u8 = 2u8;
}

impl ::std::fmt::Debug for SourceMsg {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.write_str("SourceMsg::");
        match self {
            Self::Run(_) => f.write_str("Run"),
            Self::Ack => f.write_str("Ack"),
        }
    }
}
impl Serialisable for SourceMsg {
    fn ser_id(&self) -> SerId {
        Self::SERID
    }
    fn size_hint(&self) -> Option<usize> {
        match self {
            SourceMsg::Run(_) => None, // don't serialise
            SourceMsg::Ack => None,
        }
    }
    fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
        match self {
            SourceMsg::Run(_) => {
                buf.put_u8(Self::RUN_FLAG);
            }
            SourceMsg::Ack => {
                buf.put_u8(Self::ACK_FLAG);
            }
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
        match buf.get_u8() {
            Self::ACK_FLAG => Ok(Self::Ack),
            Self::RUN_FLAG => Ok(Self::Run(None)),
            _ => unimplemented!(),
        }
    }
}
