use crate::serialiser_ids;
use kompact::prelude::*;
use rand::Rng;
use std::fmt::{Debug, Formatter};

#[derive(Debug)]
pub struct Run;
pub const RUN: Run = Run;

#[derive(Debug, Clone)]
pub enum PingerMessage<P> {
    Run,
    Pong(P),
}
impl<P> From<&Run> for PingerMessage<P> {
    fn from(_run: &Run) -> Self {
        PingerMessage::Run
    }
}
// impl<P> From<P> for PingerMessage<P> {
//     fn from(pong: P) -> Self {
//         PingerMessage::Pong(pong)
//     }
// }

pub type StaticPingWithSender =
    WithSenderStrong<&'static StaticPing, PingerMessage<&'static StaticPong>>;

pub type PingWithSender = WithSenderStrong<Ping, PingerMessage<Pong>>;

#[derive(Clone, Debug)]
pub struct StaticPing;
pub const STATIC_PING: StaticPing = StaticPing;

impl StaticPing {
    pub const SERID: SerId = serialiser_ids::STATIC_PING_ID;
}

impl Serialisable for StaticPing {
    #[inline(always)]
    fn ser_id(&self) -> SerId {
        StaticPing::SERID
    }

    fn size_hint(&self) -> Option<usize> {
        Some(0)
    }

    fn serialise(&self, _buf: &mut dyn BufMut) -> Result<(), SerError> {
        Ok(())
    }

    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        Ok(self)
    }
}
impl Deserialiser<StaticPing> for StaticPing {
    const SER_ID: SerId = StaticPing::SERID;

    fn deserialise(_buf: &mut dyn Buf) -> Result<StaticPing, SerError> {
        Ok(STATIC_PING)
    }
}

#[derive(Clone, Debug)]
pub struct StaticPong;
pub const STATIC_PONG: StaticPong = StaticPong;

impl StaticPong {
    pub const SERID: SerId = serialiser_ids::STATIC_PONG_ID;
}

impl Serialisable for StaticPong {
    #[inline(always)]
    fn ser_id(&self) -> SerId {
        StaticPong::SERID
    }

    fn size_hint(&self) -> Option<usize> {
        Some(0)
    }

    fn serialise(&self, _buf: &mut dyn BufMut) -> Result<(), SerError> {
        Ok(())
    }

    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        Ok(self)
    }
}
impl Deserialiser<StaticPong> for StaticPong {
    const SER_ID: SerId = StaticPong::SERID;

    fn deserialise(_buf: &mut dyn Buf) -> Result<StaticPong, SerError> {
        Ok(STATIC_PONG)
    }
}

#[derive(Clone, Debug)]
pub struct Ping {
    pub index: u64,
}
impl Ping {
    pub const SERID: SerId = serialiser_ids::PING_ID;

    pub fn new(index: u64) -> Ping {
        Ping { index }
    }
}
impl Serialisable for Ping {
    #[inline(always)]
    fn ser_id(&self) -> SerId {
        Self::SERID
    }

    fn size_hint(&self) -> Option<usize> {
        Some(8)
    }

    fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
        buf.put_u64(self.index);
        Ok(())
    }

    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        Ok(self)
    }
}
impl Deserialiser<Ping> for Ping {
    const SER_ID: SerId = Ping::SERID;

    fn deserialise(buf: &mut dyn Buf) -> Result<Ping, SerError> {
        let index = buf.get_u64();
        Ok(Ping::new(index))
    }
}

#[derive(Clone, Debug)]
pub struct Pong {
    pub index: u64,
}
impl Pong {
    pub const SERID: SerId = serialiser_ids::PONG_ID;

    pub fn new(index: u64) -> Pong {
        Pong { index }
    }
}
impl Serialisable for Pong {
    #[inline(always)]
    fn ser_id(&self) -> SerId {
        Self::SERID
    }

    fn size_hint(&self) -> Option<usize> {
        Some(8)
    }

    fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
        buf.put_u64(self.index);
        Ok(())
    }

    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        Ok(self)
    }
}
impl Deserialiser<Pong> for Pong {
    const SER_ID: SerId = Pong::SERID;

    fn deserialise(buf: &mut dyn Buf) -> Result<Pong, SerError> {
        let index = buf.get_u64();
        Ok(Pong::new(index))
    }
}

#[derive(Clone)]
pub struct SizedThroughputMessage {
    data: Box<[u8]>,
    pub aux: u8,
    //checksum: u64,
}

impl SizedThroughputMessage {
    const SERID: SerId = serialiser_ids::STP_MESSAGE_ID;

    pub fn new(size: usize) -> Self {
        let mut rng = rand::thread_rng();
        let data_vec: Vec<u8> = (0..size).map(|v| rng.gen_range(u8::MIN, u8::MAX)).collect();
        let data = data_vec.into_boxed_slice();
        /*let mut checksum: u64 = 0;
        for d in &data {
            checksum += *d as u64;
        }*/
        SizedThroughputMessage { data, aux: 1 }
    }
}

impl Debug for SizedThroughputMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BigPingMsg")
            .field("data length", &(self.data.len()))
            .finish()
    }
}

impl Serialisable for SizedThroughputMessage {
    fn ser_id(&self) -> SerId {
        Self::SERID
    }
    fn size_hint(&self) -> Option<usize> {
        Some(self.data.len() + 4)
    }
    fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
        buf.put_u32(self.data.len() as u32);
        buf.put_u8(self.aux);
        //buf.put_u64(self.checksum);
        buf.put_slice(&*self.data);
        Ok(())
    }

    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        Ok(self)
    }
}

impl Deserialiser<SizedThroughputMessage> for SizedThroughputMessage {
    const SER_ID: SerId = Self::SERID;
    fn deserialise(buf: &mut dyn Buf) -> Result<SizedThroughputMessage, SerError> {
        let data_len = buf.get_u32() as usize;
        let aux = buf.get_u8();
        /* unsafe {
            data.set_len(data_len);
        } */
        // let checksum = buf.get_u64();
        unsafe {
            let mut data = Box::<[u8]>::new_uninit_slice(data_len).assume_init();
            buf.copy_to_slice(&mut *data);
            Ok(Self { data, aux })
        }
        /*
        let mut sum: u64 = 0;
        for d in &data {
            sum += *d as u64;
        }
        assert_eq!(sum, checksum);
        */
    }
}
