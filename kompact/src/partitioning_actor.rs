use super::*;
use benchmark_suite_shared::test_utils::{KVOperation, KVTimestamp};
use kompact::prelude::*;
use std::sync::Arc;
use synchronoise::CountdownEvent;

#[derive(ComponentDefinition)]
pub struct PartitioningActor {
    // TODO generalize for different applications by using serialised data as Init msg
    ctx: ComponentContext<PartitioningActor>,
    prepare_latch: Arc<CountdownEvent>,
    finished_latch: Option<Arc<CountdownEvent>>,
    reply_stop: Option<Ask<(), ()>>,
    stop_ack_count: u32,
    init_id: u32,
    n: u32,
    nodes: Vec<ActorPath>,
    init_ack_count: u32,
    done_count: u32,
    test_promise: Option<KPromise<Vec<KVTimestamp>>>,
    test_results: Vec<KVTimestamp>,
}

impl PartitioningActor {
    pub fn with(
        prepare_latch: Arc<CountdownEvent>,
        finished_latch: Option<Arc<CountdownEvent>>,
        init_id: u32,
        nodes: Vec<ActorPath>,
        test_promise: Option<KPromise<Vec<KVTimestamp>>>,
    ) -> PartitioningActor {
        PartitioningActor {
            ctx: ComponentContext::uninitialised(),
            prepare_latch,
            finished_latch,
            reply_stop: None,
            stop_ack_count: 0,
            init_id,
            n: 0,
            nodes,
            init_ack_count: 0,
            done_count: 0,
            test_promise,
            test_results: Vec::new(),
        }
    }
}

ignore_lifecycle!(PartitioningActor);

impl Actor for PartitioningActor {
    type Message = IterationControlMsg;
    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        match msg {
            IterationControlMsg::Prepare(init_data) => {
                self.n = self.nodes.len() as u32;
                for (r, node) in (&self.nodes).iter().enumerate() {
                    let pid = r as u32 + 1;
                    let init = Init {
                        pid,
                        init_id: self.init_id,
                        nodes: self.nodes.clone(),
                        init_data: init_data.clone(),
                    };
                    node.tell_serialised(PartitioningActorMsg::Init(init), self)
                        .expect("Should serialise");
                }
            }
            IterationControlMsg::Run => {
                for node in &self.nodes {
                    node.tell_serialised(PartitioningActorMsg::Run, self)
                        .expect("Should serialise");
                }
            }
            IterationControlMsg::Stop(stop_latch) => {
                self.reply_stop = Some(stop_latch);
                info!(self.ctx.log(), "Stopping iteration {}", self.init_id);
                for node in &self.nodes {
                    node.tell_serialised(PartitioningActorMsg::Stop, self)
                        .expect("Should serialise");
                }
            }
        }
        Handled::Ok
    }

    fn receive_network(&mut self, msg: NetMessage) -> Handled {
        match_deser! {msg {
            msg(p): PartitioningActorMsg [using PartitioningActorSer] => {
                match p {
                    PartitioningActorMsg::InitAck(_) => {
                        self.init_ack_count += 1;
                        debug!(self.ctx.log(), "Got init ack {}/{}", &self.init_ack_count, &self.n);
                        if self.init_ack_count == self.n {
                            self.prepare_latch
                                .decrement()
                                .expect("Latch didn't decrement!");
                        }
                    },
                    PartitioningActorMsg::Done => {
                        self.done_count += 1;
                        if self.done_count == self.n {
                            debug!(self.ctx.log(), "Everybody is done");
                            self.finished_latch
                                .as_ref()
                                .unwrap()
                                .decrement()
                                .expect("Latch didn't decrement!");
                        }
                    },
                    PartitioningActorMsg::TestDone(td) => {
                        self.done_count += 1;
                        self.test_results.extend(td);
                        if self.done_count == self.n {
                            self.test_promise
                                .take()
                                .unwrap()
                                .fulfil(self.test_results.clone())
                                .expect("Could not fulfill promise with test results");
                        }
                    },
                    PartitioningActorMsg::StopAck => {
                        self.stop_ack_count += 1;
                        debug!(self.ctx.log(), "{}", format!("Got StopAck {}/{}", self.stop_ack_count, self.n));
                        if self.stop_ack_count == self.n {
                            self.reply_stop.take().unwrap().reply(()).expect("Stopped iteration");
                        }
                    }
                    e => error!(self.ctx.log(), "Got unexpected msg: {:?}", e),
                }
            },
            err(e) => error!(self.ctx.log(), "Error deserialising msg: {:?}", e),
            default(_) => unimplemented!(),
        }}
        Handled::Ok
    }
}

#[derive(Debug, Clone)]
pub struct Init {
    pub pid: u32,
    pub init_id: u32,
    pub nodes: Vec<ActorPath>,
    pub init_data: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
pub struct TestDone(pub Vec<KVTimestamp>);

#[derive(Debug, Clone)]
pub enum PartitioningActorMsg {
    Init(Init),
    InitAck(u32),
    Run,
    Done,
    TestDone(Vec<KVTimestamp>),
    Stop,
    StopAck,
}

#[derive(Debug)]
pub enum IterationControlMsg {
    Prepare(Option<Vec<u8>>),
    Run,
    Stop(Ask<(), ()>),
}

pub struct PartitioningActorSer;
pub const PARTITIONING_ACTOR_SER: PartitioningActorSer = PartitioningActorSer {};
const INIT_ID: u8 = 1;
const INITACK_ID: u8 = 2;
const RUN_ID: u8 = 3;
const DONE_ID: u8 = 4;
const TESTDONE_ID: u8 = 5;
const STOP_ID: u8 = 6;
const STOP_ACK_ID: u8 = 13;
/* bytes to differentiate KVOperations*/
const READ_INV: u8 = 8;
const READ_RESP: u8 = 9;
const WRITE_INV: u8 = 10;
const WRITE_RESP: u8 = 11;

impl Serialisable for PartitioningActorMsg {
    fn ser_id(&self) -> u64 {
        serialiser_ids::PARTITIONING_ID
    }

    fn size_hint(&self) -> Option<usize> {
        match self {
            PartitioningActorMsg::Init(_) => Some(1000),
            PartitioningActorMsg::InitAck(_) => Some(5),
            PartitioningActorMsg::Run => Some(1),
            PartitioningActorMsg::Done => Some(1),
            PartitioningActorMsg::Stop => Some(1),
            PartitioningActorMsg::StopAck => Some(1),
            PartitioningActorMsg::TestDone(_) => Some(100000),
        }
    }

    fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
        match self {
            PartitioningActorMsg::Init(i) => {
                buf.put_u8(INIT_ID);
                buf.put_u32(i.pid);
                buf.put_u32(i.init_id);
                match &i.init_data {
                    Some(data) => {
                        buf.put_u64(data.len() as u64);
                        for byte in data {
                            buf.put_u8(*byte);
                        }
                    }
                    None => buf.put_u64(0),
                }
                buf.put_u32(i.nodes.len() as u32);
                for node in i.nodes.iter() {
                    node.serialise(buf)?;
                }
            }
            PartitioningActorMsg::InitAck(id) => {
                buf.put_u8(INITACK_ID);
                buf.put_u32(*id);
            }
            PartitioningActorMsg::Run => buf.put_u8(RUN_ID),
            PartitioningActorMsg::Done => buf.put_u8(DONE_ID),
            PartitioningActorMsg::TestDone(timestamps) => {
                buf.put_u8(TESTDONE_ID);
                buf.put_u32(timestamps.len() as u32);
                for ts in timestamps {
                    buf.put_u64(ts.key);
                    match ts.operation {
                        KVOperation::ReadInvokation => buf.put_u8(READ_INV),
                        KVOperation::ReadResponse => {
                            buf.put_u8(READ_RESP);
                            buf.put_u32(ts.value.unwrap());
                        }
                        KVOperation::WriteInvokation => {
                            buf.put_u8(WRITE_INV);
                            buf.put_u32(ts.value.unwrap());
                        }
                        KVOperation::WriteResponse => {
                            buf.put_u8(WRITE_RESP);
                            buf.put_u32(ts.value.unwrap());
                        }
                    }
                    buf.put_i64(ts.time);
                    buf.put_u32(ts.sender);
                }
            }
            PartitioningActorMsg::Stop => buf.put_u8(STOP_ID),
            PartitioningActorMsg::StopAck => buf.put_u8(STOP_ACK_ID),
        }
        Ok(())
    }

    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        Ok(self)
    }
}

impl Deserialiser<PartitioningActorMsg> for PartitioningActorSer {
    const SER_ID: u64 = serialiser_ids::PARTITIONING_ID;

    fn deserialise(buf: &mut dyn Buf) -> Result<PartitioningActorMsg, SerError> {
        match buf.get_u8() {
            INIT_ID => {
                let pid: u32 = buf.get_u32();
                let init_id: u32 = buf.get_u32();
                let data_len: u64 = buf.get_u64();
                let init_data = match data_len {
                    0 => None,
                    _ => {
                        let mut data = vec![];
                        for _ in 0..data_len {
                            data.push(buf.get_u8());
                        }
                        Some(data)
                    }
                };
                let nodes_len: u32 = buf.get_u32();
                let mut nodes: Vec<ActorPath> = Vec::new();
                for _ in 0..nodes_len {
                    let actorpath = ActorPath::deserialise(buf)?;
                    nodes.push(actorpath);
                }
                let init = Init {
                    pid,
                    init_id,
                    nodes,
                    init_data,
                };
                Ok(PartitioningActorMsg::Init(init))
            }
            INITACK_ID => {
                let init_id = buf.get_u32();
                Ok(PartitioningActorMsg::InitAck(init_id))
            }
            RUN_ID => Ok(PartitioningActorMsg::Run),
            DONE_ID => Ok(PartitioningActorMsg::Done),
            TESTDONE_ID => {
                let n: u32 = buf.get_u32();
                let mut timestamps: Vec<KVTimestamp> = Vec::new();
                for _ in 0..n {
                    let key = buf.get_u64();
                    let (operation, value) = match buf.get_u8() {
                        READ_INV => (KVOperation::ReadInvokation, None),
                        READ_RESP => (KVOperation::ReadResponse, Some(buf.get_u32())),
                        WRITE_INV => (KVOperation::WriteInvokation, Some(buf.get_u32())),
                        WRITE_RESP => (KVOperation::WriteResponse, Some(buf.get_u32())),
                        _ => panic!("Found unknown KVOperation id"),
                    };
                    let time = buf.get_i64();
                    let sender = buf.get_u32();
                    let ts = KVTimestamp {
                        key,
                        operation,
                        value,
                        time,
                        sender,
                    };
                    timestamps.push(ts);
                }
                Ok(PartitioningActorMsg::TestDone(timestamps))
            }
            STOP_ID => Ok(PartitioningActorMsg::Stop),
            STOP_ACK_ID => Ok(PartitioningActorMsg::StopAck),
            _ => Err(SerError::InvalidType(
                "Found unkown id, but expected PartitioningActorMsg.".into(),
            )),
        }
    }
}
