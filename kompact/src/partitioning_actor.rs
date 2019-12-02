use super::*;
use kompact::prelude::*;
use std::sync::Arc;
use synchronoise::CountdownEvent;
use benchmark_suite_shared::test_utils::{KVTimestamp, KVOperation};

#[derive(ComponentDefinition)]
pub struct PartitioningActor {
    ctx: ComponentContext<PartitioningActor>,
    prepare_latch: Arc<CountdownEvent>,
    finished_latch: Option<Arc<CountdownEvent>>,
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
                //                info!(self.ctx.log(), "Sending init to nodes");
                for (r, node) in (&self.nodes).iter().enumerate() {
                    let rank = r as u32;
                    let init = Init {
                        rank,
                        init_id: self.init_id,
                        nodes: self.nodes.clone(),
                        min_key,
                        max_key,
                    };
                    node.tell((init, PARTITIONING_ACTOR_SER), self);
                }
            }
            _ => {} // ignore
        }
    }
}

impl Actor for PartitioningActor {
    type Message = Run;

    fn receive_local(&mut self, _msg: Self::Message) -> () {
        for node in &self.nodes {
            node.tell((Run, PARTITIONING_ACTOR_SER), self);
        }
    }

    fn receive_network(&mut self, msg: NetMessage) -> () {
        match_deser! {msg; {
            _init_ack: InitAck [PartitioningActorSer] => {
                self.init_ack_count += 1;
                    //                    info!(self.ctx.log(), "Got init ack {}/{} from {}", &self.init_ack_count, &self.n, sender);
                    if self.init_ack_count == self.n {
                        info!(self.ctx.log(), "Got init_ack from everybody!");
                        self.prepare_latch
                            .decrement()
                            .expect("Latch didn't decrement!");
                    }
            },
            _done: Done [PartitioningActorSer] => {
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
            td: TestDone [PartitioningActorSer] => {
                self.done_count += 1;
                self.test_results.extend(td.0);
                if self.done_count == self.n {
                    self.test_promise.take()
                                     .unwrap()
                                     .fulfill(self.test_results.clone())
                                     .expect("Could not fulfill promise with test results");
                }
            },
            !Err(e) => error!(self.ctx.log(), "Error deserialising msg: {:?}", e),
        }}
    }
}

#[derive(Clone, Debug)]
struct Start;
#[derive(Debug, Clone)]
pub struct Init {
    pub rank: u32,
    pub init_id: u32,
    pub nodes: Vec<ActorPath>,
    pub min_key: u64,
    pub max_key: u64,
}
#[derive(Debug, Clone)]
pub struct InitAck(pub u32);
#[derive(Clone, Debug)]
pub struct Run;
#[derive(Clone, Debug)]
pub struct Done;
#[derive(Clone, Debug)]
pub struct TestDone(pub Vec<KVTimestamp>);

pub struct PartitioningActorSer;
pub const PARTITIONING_ACTOR_SER: PartitioningActorSer = PartitioningActorSer {};
const INIT_ID: i8 = 1;
const INITACK_ID: i8 = 2;
const RUN_ID: i8 = 3;
const DONE_ID: i8 = 4;
const TESTDONE_ID: i8 = 5;
/* bytes to differentiate KVOperations*/
const READ_INV: i8 = 6;
const READ_RESP: i8 = 7;
const WRITE_INV: i8 = 8;
const WRITE_RESP: i8 = 9;


impl Serialiser<Init> for PartitioningActorSer {
    fn ser_id(&self) -> SerId {
        serialiser_ids::PARTITIONING_INIT_MSG
    }
    fn size_hint(&self) -> Option<usize> {
        Some(1000)
    } // TODO dynamic buffer

    fn serialise(&self, i: &Init, buf: &mut dyn BufMut) -> Result<(), SerError> {
        buf.put_i8(INIT_ID);
        buf.put_u32_be(i.rank);
        buf.put_u32_be(i.init_id);
        buf.put_u64_be(i.min_key);
        buf.put_u64_be(i.max_key);
        buf.put_u32_be(i.nodes.len() as u32);
        for node in i.nodes.iter() {
            node.serialise(buf)?;
        }
        Ok(())
    }
}
impl Deserialiser<Init> for PartitioningActorSer {
    const SER_ID: SerId = serialiser_ids::PARTITIONING_INIT_MSG;

    fn deserialise(buf: &mut dyn Buf) -> Result<Init, SerError> {
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
                Ok(init)
            }
            _ => Err(SerError::InvalidType(
                "Found unkown id, but expected Init.".into(),
            )),
        }
    }
}

impl Serialiser<InitAck> for PartitioningActorSer {
    fn ser_id(&self) -> SerId {
        serialiser_ids::PARTITIONING_INIT_ACK_MSG
    }

    fn size_hint(&self) -> Option<usize> {
        Some(5)
    }

    fn serialise(&self, init_ack: &InitAck, buf: &mut dyn BufMut) -> Result<(), SerError> {
        buf.put_i8(INITACK_ID);
        buf.put_u32_be(init_ack.0);
        Ok(())
    }
}
impl Deserialiser<InitAck> for PartitioningActorSer {
    const SER_ID: SerId = serialiser_ids::PARTITIONING_INIT_ACK_MSG;

    fn deserialise(buf: &mut dyn Buf) -> Result<InitAck, SerError> {
        match buf.get_i8() {
            INITACK_ID => {
                let init_id = buf.get_u32_be();
                Ok(InitAck(init_id))
            }
            _ => Err(SerError::InvalidType(
                "Found unkown id, but expected InitAck.".into(),
            )),
        }
    }
}

impl Serialiser<Run> for PartitioningActorSer {
    fn ser_id(&self) -> SerId {
        serialiser_ids::PARTITIONING_RUN_MSG
    }
    fn size_hint(&self) -> Option<usize> {
        Some(1)
    }
    fn serialise(&self, _v: &Run, buf: &mut dyn BufMut) -> Result<(), SerError> {
        buf.put_i8(RUN_ID);
        Ok(())
    }
}

impl Deserialiser<Run> for PartitioningActorSer {
    const SER_ID: SerId = serialiser_ids::PARTITIONING_RUN_MSG;

    fn deserialise(buf: &mut dyn Buf) -> Result<Run, SerError> {
        match buf.get_i8() {
            RUN_ID => Ok(Run),
            _ => Err(SerError::InvalidType(
                "Found unkown id, but expected Run.".into(),
            )),
        }
    }
}

impl Serialiser<Done> for PartitioningActorSer {
    fn ser_id(&self) -> SerId {
        serialiser_ids::PARTITIONING_DONE_MSG
    }
    fn size_hint(&self) -> Option<usize> {
        Some(1)
    }

    fn serialise(&self, _v: &Done, buf: &mut dyn BufMut) -> Result<(), SerError> {
        buf.put_i8(DONE_ID);
        Ok(())
    }
}

impl Deserialiser<Done> for PartitioningActorSer {
    const SER_ID: SerId = serialiser_ids::PARTITIONING_DONE_MSG;

    fn deserialise(buf: &mut dyn Buf) -> Result<Done, SerError> {
        match buf.get_i8() {
            DONE_ID => Ok(Done),
            _ => Err(SerError::InvalidType(
                "Found unkown id, but expected Run.".into(),
            )),
        }
    }
}

impl Serialiser<TestDone> for PartitioningActorSer {
    fn ser_id(&self) -> SerId {
        serialiser_ids::PARTITIONING_TESTDONE_MSG
    }
    fn size_hint(&self) -> Option<usize> {
        Some(100000)
    }

    fn serialise(&self, td: &TestDone, buf: &mut dyn BufMut) -> Result<(), SerError> {
        let timestamps = &td.0;
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

        Ok(())
    }
}

impl Deserialiser<TestDone> for PartitioningActorSer {
    const SER_ID: SerId = serialiser_ids::PARTITIONING_TESTDONE_MSG;

    fn deserialise(buf: &mut dyn Buf) -> Result<TestDone, SerError> {
        match buf.get_i8() {
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
                let test_done = TestDone{ 0: timestamps };
                Ok(test_done)
            }
            _ => Err(SerError::InvalidType(
                "Found unkown id, but expected Init.".into(),
            )),
        }
    }
}


