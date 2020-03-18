use super::*;
use kompact::prelude::*;
use std::sync::Arc;
use synchronoise::CountdownEvent;
use benchmark_suite_shared::test_utils::{KVTimestamp, KVOperation};

#[derive(ComponentDefinition)]
pub struct PartitioningActor {  // TODO generalize for different applications by using serialised data as Init msg
    ctx: ComponentContext<PartitioningActor>,
    prepare_latch: Arc<CountdownEvent>,
    finished_latch: Option<Arc<CountdownEvent>>,
    reply_stop: Option<Ask<(), ()>>,
    stop_ack_count: u32,
    init_id: u32,
    n: u32,
    nodes: Vec<ActorPath>,
    num_keys: u64,
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
        num_keys: u64,
        test_promise: Option<KPromise<Vec<KVTimestamp>>>,
    ) -> PartitioningActor {
        PartitioningActor {
            ctx: ComponentContext::new(),
            prepare_latch,
            finished_latch,
            reply_stop: None,
            stop_ack_count: 0,
            init_id,
            n: nodes.len() as u32,
            nodes,
            num_keys,
            init_ack_count: 0,
            done_count: 0,
            test_promise,
            test_results: Vec::new(),
        }
    }
}

impl Provide<ControlPort> for PartitioningActor {
    fn handle(&mut self, event: ControlEvent) -> () {
        match event {
            ControlEvent::Start => {
                let min_key: u64 = 0;
                let max_key = self.num_keys - 1;
                info!(self.ctx.log(), "{}", format!("Sending init to {} nodes", self.n));
                for (r, node) in (&self.nodes).iter().enumerate() {
                    let rank = r as u32 + 1;
                    let init = Init {
                        rank,
                        init_id: self.init_id,
                        nodes: self.nodes.clone(),
                        min_key,
                        max_key,
                    };
                    node.tell_ser(PartitioningActorMsg::Init(init).serialised(), self).expect("Should serialise");
                }
            }
            _ => {} // ignore
        }
    }
}

impl Actor for PartitioningActor {
    type Message = IterationControlMsg;

    fn receive_local(&mut self, msg: Self::Message) -> () {
        match msg {
            IterationControlMsg::Run => {
                for node in &self.nodes {
                    node.tell_ser(PartitioningActorMsg::Run.serialised(), self)
                        .expect("Should serialise");
                }
            },
            IterationControlMsg::StopIteration(stop_latch) => {
                self.reply_stop = Some(stop_latch);
                for node in &self.nodes {
                    node.tell_ser(PartitioningActorMsg::Stop.serialised(), self)
                        .expect("Should serialise");
                }
            }
        }
    }

    fn receive_network(&mut self, msg: NetMessage) -> () {
        match_deser! {msg; {
            p: PartitioningActorMsg [PartitioningActorSer] => {
                match p {
                    PartitioningActorMsg::InitAck(_) => {
                        self.init_ack_count += 1;
                        //                    info!(self.ctx.log(), "Got init ack {}/{} from {}", &self.init_ack_count, &self.n, sender);
                        if self.init_ack_count == self.n {
                            info!(self.ctx.log(), "Got init_ack from everybody!");
                            self.prepare_latch
                                .decrement()
                                .expect("Latch didn't decrement!");
                        }
                    },
                    PartitioningActorMsg::Done => {
                        self.done_count += 1;
                        if self.done_count == self.n {
                            info!(self.ctx.log(), "Everybody is done");
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
                                .fulfill(self.test_results.clone())
                                .expect("Could not fulfill promise with test results");
                        }
                    },
                    e => error!(self.ctx.log(), "Got unexpected msg: {:?}", e),
                }
            },
            !Err(e) => error!(self.ctx.log(), "Error deserialising msg: {:?}", e),
        }}
    }
}

#[derive(Debug, Clone)]
pub struct Init {
    pub rank: u32,
    pub init_id: u32,
    pub nodes: Vec<ActorPath>,
    pub min_key: u64,
    pub max_key: u64,
}

#[derive(Clone, Debug)]
pub struct TestDone(pub Vec<KVTimestamp>);

#[derive(Debug, Clone)]
pub enum PartitioningActorMsg{
    Init(Init),
    InitAck(u32),
    Run,
    Done,
    TestDone(Vec<KVTimestamp>),
    Stop,
    StopAck
}

#[derive(Debug)]
pub enum IterationControlMsg {
    Run,
    StopIteration(Ask<(), ()>)
}

pub struct PartitioningActorSer;
pub const PARTITIONING_ACTOR_SER: PartitioningActorSer = PartitioningActorSer {};
const INIT_ID: i8 = 1;
const INITACK_ID: i8 = 2;
const RUN_ID: i8 = 3;
const DONE_ID: i8 = 4;
const TESTDONE_ID: i8 = 5;
const STOP_ID: i8 = 6;
const STOP_ACK_ID: i8 = 7;
/* bytes to differentiate KVOperations*/
const READ_INV: i8 = 6;
const READ_RESP: i8 = 7;
const WRITE_INV: i8 = 8;
const WRITE_RESP: i8 = 9;


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
                buf.put_i8(INIT_ID);
                buf.put_u32_be(i.rank);
                buf.put_u32_be(i.init_id);
                buf.put_u64_be(i.min_key);
                buf.put_u64_be(i.max_key);
                buf.put_u32_be(i.nodes.len() as u32);
                for node in i.nodes.iter() {
                    node.serialise(buf)?;
                }
            },
            PartitioningActorMsg::InitAck(id) => {
                buf.put_i8(INITACK_ID);
                buf.put_u32_be(id.to_owned());
            },
            PartitioningActorMsg::Run => {
                buf.put_i8(RUN_ID);
            },
            PartitioningActorMsg::Done => {
                buf.put_i8(DONE_ID);
            },
            PartitioningActorMsg::TestDone(timestamps) => {
                buf.put_i8(TESTDONE_ID);
                buf.put_u32_be(timestamps.len() as u32);
                for ts in timestamps{
                    buf.put_u64_be(ts.key);
                    match ts.operation {
                        KVOperation::ReadInvokation => buf.put_i8(READ_INV),
                        KVOperation::ReadResponse => {
                            buf.put_i8(READ_RESP);
                            buf.put_u32_be(ts.value.unwrap());
                        },
                        KVOperation::WriteInvokation => {
                            buf.put_i8(WRITE_INV);
                            buf.put_u32_be(ts.value.unwrap());
                        },
                        KVOperation::WriteResponse => {
                            buf.put_i8(WRITE_RESP);
                            buf.put_u32_be(ts.value.unwrap());
                        },
                    }
                    buf.put_i64_be(ts.time);
                    buf.put_u32_be(ts.sender);
                }
            },
            PartitioningActorMsg::Stop => {
                buf.put_i8(STOP_ID);
            },
            PartitioningActorMsg::StopAck => {
                buf.put_i8(STOP_ACK_ID);
            }
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
        match buf.get_i8() {
            INIT_ID => {
                let rank: u32 = buf.get_u32_be();
                let init_id: u32 = buf.get_u32_be();
                let min_key: u64 = buf.get_u64_be();
                let max_key: u64 = buf.get_u64_be();
                let nodes_len: u32 = buf.get_u32_be();
                let mut nodes: Vec<ActorPath> = Vec::new();
                for _ in 0..nodes_len {
                    let actorpath = ActorPath::deserialise(buf)?;
                    nodes.push(actorpath);
                }
                let init = Init {
                    rank,
                    init_id,
                    nodes,
                    min_key,
                    max_key,
                };
                Ok(PartitioningActorMsg::Init(init))
            },
            INITACK_ID => {
                let init_id = buf.get_u32_be();
                Ok(PartitioningActorMsg::InitAck(init_id))
            },
            RUN_ID => Ok(PartitioningActorMsg::Run),
            DONE_ID => Ok(PartitioningActorMsg::Done),
            TESTDONE_ID => {
                let n: u32 = buf.get_u32_be();
                let mut timestamps: Vec<KVTimestamp> = Vec::new();
                for _ in 0..n {
                    let key = buf.get_u64_be();
                    let (operation, value) = match buf.get_i8() {
                        READ_INV => (KVOperation::ReadInvokation, None),
                        READ_RESP => (KVOperation::ReadResponse, Some(buf.get_u32_be())),
                        WRITE_INV => (KVOperation::WriteInvokation, Some(buf.get_u32_be())),
                        WRITE_RESP => (KVOperation::WriteResponse, Some(buf.get_u32_be())),
                        _ => panic!("Found unknown KVOperation id"),
                    };
                    let time = buf.get_i64_be();
                    let sender = buf.get_u32_be();
                    let ts = KVTimestamp{key, operation, value, time, sender};
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