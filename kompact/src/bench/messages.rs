use crate::serialiser_ids;
use kompact::prelude::*;

#[derive(Clone, Debug)]
pub struct StaticPing;
pub const STATIC_PING: StaticPing = StaticPing;

impl StaticPing {
    pub const SERID: u64 = serialiser_ids::STATIC_PING_ID;
}

impl Serialisable for StaticPing {
    #[inline(always)]
    fn serid(&self) -> u64 {
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
    fn deserialise(_buf: &mut dyn Buf) -> Result<StaticPing, SerError> {
        Ok(STATIC_PING)
    }
}

#[derive(Clone, Debug)]
pub struct StaticPong;
pub const STATIC_PONG: StaticPong = StaticPong;

impl StaticPong {
    pub const SERID: u64 = serialiser_ids::STATIC_PONG_ID;
}

impl Serialisable for StaticPong {
    #[inline(always)]
    fn serid(&self) -> u64 {
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
    fn deserialise(_buf: &mut dyn Buf) -> Result<StaticPong, SerError> {
        Ok(STATIC_PONG)
    }
}

#[derive(Clone, Debug)]
pub struct Ping {
    pub index: u64,
}
impl Ping {
    pub const SERID: u64 = serialiser_ids::PING_ID;

    pub fn new(index: u64) -> Ping {
        Ping { index }
    }
}
impl Serialisable for Ping {
    #[inline(always)]
    fn serid(&self) -> u64 {
        Self::SERID
    }

    fn size_hint(&self) -> Option<usize> {
        Some(8)
    }

    fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
        buf.put_u64_be(self.index);
        Ok(())
    }

    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        Ok(self)
    }
}
impl Deserialiser<Ping> for Ping {
    fn deserialise(buf: &mut dyn Buf) -> Result<Ping, SerError> {
        let index = buf.get_u64_be();
        Ok(Ping::new(index))
    }
}

#[derive(Clone, Debug)]
pub struct Pong {
    pub index: u64,
}
impl Pong {
    pub const SERID: u64 = serialiser_ids::PONG_ID;

    pub fn new(index: u64) -> Pong {
        Pong { index }
    }
}
impl Serialisable for Pong {
    #[inline(always)]
    fn serid(&self) -> u64 {
        Self::SERID
    }

    fn size_hint(&self) -> Option<usize> {
        Some(8)
    }

    fn serialise(&self, buf: &mut dyn BufMut) -> Result<(), SerError> {
        buf.put_u64_be(self.index);
        Ok(())
    }

    fn local(self: Box<Self>) -> Result<Box<dyn Any + Send>, Box<dyn Serialisable>> {
        Ok(self)
    }
}
impl Deserialiser<Pong> for Pong {
    fn deserialise(buf: &mut dyn Buf) -> Result<Pong, SerError> {
        let index = buf.get_u64_be();
        Ok(Pong::new(index))
    }
}
