use super::*;
use kompact::prelude::*;
use kompact::*;
use std::sync::Arc;
use synchronoise::CountdownEvent;
use std::io::Read;
use protobuf::descriptor::FieldOptions_CType::STRING;
use kompact::helpers::deserialise_actor_path;

#[derive(ComponentDefinition)]
pub struct PartitioningActor {
    ctx: ComponentContext<PartitioningActor>,
    prepare_latch: Arc<CountdownEvent>,
    finished_latch: Arc<CountdownEvent>,
    init_id: u32,
    n: u32,
    nodes: Vec<ActorPath>,
    num_keys: u64,
    partition_size: u32,
    init_ack_count: u32,
    done_count: u32,
}

impl PartitioningActor {
    pub fn with(prepare_latch: Arc<CountdownEvent>, finished_latch: Arc<CountdownEvent>, init_id: u32, nodes: Vec<ActorPath>, num_keys: u64, partition_size: u32) -> PartitioningActor {
        PartitioningActor{
            ctx: ComponentContext::new(),
            prepare_latch,
            finished_latch,
            init_id,
            n: nodes.len() as u32,
            nodes,
            num_keys,
            partition_size,
            init_ack_count: 0,
            done_count: 0,
        }
    }
}

impl Provide<ControlPort> for PartitioningActor {
    fn handle(&mut self, event: ControlEvent) -> () {
        match event{
            ControlEvent::Start => {
                let min_key: u64 = 0;
                let max_key = self.num_keys - 1;
                for (r, node) in (&self.nodes).iter().enumerate(){
                    let rank = r as u32;
                    let init = Init{
                        rank,
                        init_id: self.init_id,
                        nodes: self.nodes.clone(),
                        min_key,
                        max_key,
                    };
                    node.tell((init, PARTITIONING_ACTOR_SER), self);
                }
//                info!(self.ctx.log(), "sent init to all nodes");
            }
            _ => {} // ignore
        }
    }
}

impl Actor for PartitioningActor{
    fn receive_local(&mut self, sender: ActorRef, msg: Box<Any>) -> () {
        if msg.is::<Run>() {
            info!(self.ctx.log(), "Telling nodes to run");
            for node in &self.nodes{
                node.tell((Run, PARTITIONING_ACTOR_SER), self);
            }
        }
    }

    fn receive_message(&mut self, sender: ActorPath, ser_id: u64, buf: &mut Buf) -> () {
        if ser_id == Serialiser::<InitAck>::serid(&PARTITIONING_ACTOR_SER){
            let r: Result<InitAck, SerError> = PartitioningActorSer::deserialise(buf);
            match r {
                Ok(init_ack) => {
                    self.init_ack_count += 1;
//                    info!(self.ctx.log(), "Got init ack {}/{} from {}", &self.init_ack_count, &self.n, sender);
                    if self.init_ack_count == self.n{
                        info!(self.ctx.log(), "Got init_ack from everybody!");
                        self.prepare_latch.decrement();
                    }
                }
                Err(e) => error!(self.ctx.log(), "Error deserialising InitAck: {:?}", e),
            }
        }
        else if ser_id == Serialiser::<Done>::serid(&PARTITIONING_ACTOR_SER){
            let r: Result<Done, SerError> = PartitioningActorSer::deserialise(buf);
            match r {
                Ok(_done) => {
                    self.done_count += 1;
                    if self.done_count == self.n {
                        info!(self.ctx.log(), "Everybody is done");
                        self.finished_latch.decrement();
                    }
                }
                Err(e) => error!(self.ctx.log(), "Error deserialising Done: {:?}", e),
            }
        }
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

pub struct PartitioningActorSer;
pub const PARTITIONING_ACTOR_SER: PartitioningActorSer = PartitioningActorSer {};
const INIT_ID: i8 = 1;
const INITACK_ID: i8 = 2;
const RUN_ID: i8 = 3;
const DONE_ID: i8 = 4;

impl Serialiser<Init> for PartitioningActorSer {
    fn serid(&self) -> u64 {
        serializer_ids::PARTITIONING_INIT_MSG
    }
    fn size_hint(&self) -> Option<usize> {
        Some(1000)
    }   // TODO dynamic buffer

    fn serialise(&self, i: &Init, buf: &mut BufMut) -> Result<(), SerError> {
        buf.put_i8(INIT_ID);
        buf.put_u32_be(i.rank);
        buf.put_u32_be(i.init_id);
        buf.put_u64_be(i.min_key);
        buf.put_u64_be(i.max_key);
        buf.put_u32_be(i.nodes.len() as u32);
        for node in i.nodes.iter(){
            node.serialise(buf);
        }
        Ok(())
    }
}
impl Deserialiser<Init> for PartitioningActorSer {
    fn deserialise(buf: &mut Buf) -> Result<Init, SerError> {
        match buf.get_i8() {
            INIT_ID => {
                let rank: u32 = buf.get_u32_be();
                let init_id: u32 = buf.get_u32_be();
                let min_key: u64 = buf.get_u64_be();
                let max_key: u64 = buf.get_u64_be();
                let nodes_len: u32 = buf.get_u32_be();
                let mut nodes: Vec<ActorPath> = Vec::new();
                let mut buffer = buf;
                for _ in 0..nodes_len{
                    if let Ok((b, actorpath)) = deserialise_actor_path(buffer){
//                        println!("actorpath={}", &actorpath);
                        nodes.push(actorpath);
                        buffer = b;
                    } else {
                        return Err(SerError::InvalidType(
                            "Could not deserialise data to ActorPath".into(),
                        ));
                    }
                }
                let init = Init{ rank, init_id, nodes, min_key, max_key };
                Ok(init)
            }
            _ => Err(SerError::InvalidType(
                "Found unkown id, but expected Init.".into(),
            )),
        }
    }
}

impl Serialiser<InitAck> for PartitioningActorSer {
    fn serid(&self) -> u64 {
        serializer_ids::PARTITIONING_INIT_ACK_MSG
    }

    fn size_hint(&self) -> Option<usize> {
        Some(5)
    }

    fn serialise(&self, init_ack: &InitAck, buf: &mut BufMut) -> Result<(), SerError> {
        buf.put_i8(INITACK_ID);
        buf.put_u32_be(init_ack.0);
        Ok(())
    }
}
impl Deserialiser<InitAck> for PartitioningActorSer {
    fn deserialise(buf: &mut Buf) -> Result<InitAck, SerError> {
        match buf.get_i8(){
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

impl Serialiser<Run> for PartitioningActorSer{
    fn serid(&self) -> u64 {
        serializer_ids::PARTITIONING_RUN_MSG
    }
    fn size_hint(&self) -> Option<usize> { Some(1) }
    fn serialise(&self, v: &Run, buf: &mut BufMut) -> Result<(), SerError> {
        buf.put_i8(RUN_ID);
        Ok(())
    }
}

impl Deserialiser<Run> for PartitioningActorSer{
    fn deserialise(buf: &mut Buf) -> Result<Run, SerError> {
       match buf.get_i8(){
           RUN_ID => { Ok(Run) }
           _ => Err(SerError::InvalidType(
               "Found unkown id, but expected Run.".into(),
           )),
       }
    }
}

impl Serialiser<Done> for PartitioningActorSer{
    fn serid(&self) -> u64 { serializer_ids::PARTITIONING_DONE_MSG }
    fn size_hint(&self) -> Option<usize> { Some(1) }

    fn serialise(&self, v: &Done, buf: &mut BufMut) -> Result<(), SerError> {
        buf.put_i8(DONE_ID);
        Ok(())
    }
}

impl Deserialiser<Done> for PartitioningActorSer{
    fn deserialise(buf: &mut Buf) -> Result<Done, SerError> {
        match buf.get_i8(){
            DONE_ID => { Ok(Done) }
            _ => Err(SerError::InvalidType(
                "Found unkown id, but expected Run.".into(),
            )),
        }
    }
}
