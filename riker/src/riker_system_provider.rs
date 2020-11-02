use riker::actors::*;
use futures_executor::ThreadPool;
use futures_preview::executor::block_on;
use futures_preview::prelude::*;
use riker_patterns::ask;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
//use futures_preview::future::RemoteHandle;
use futures::future::RemoteHandle;

use std::ops::Deref;
use std::sync::Arc;

pub fn new_system<I: Into<String>>(name: I) -> RikerSystem {
    RikerSystem::new(&name.into(), num_cpus::get()).expect("Riker ActorSystem")
}

pub trait Awaitable {
    type Output;

    fn wait(self) -> Self::Output;
}
impl<F> Awaitable for F
where
    F: Future,
{
    type Output = <F as Future>::Output;

    fn wait(self) -> Self::Output {
        block_on(self)
    }
}

#[derive(Debug)]
pub enum RikerSystemError {
    ThreadPool(std::io::Error),
    Riker(riker::system::SystemError),
}

pub struct RikerSystem {
    id_tracker: Arc<AtomicU64>,
    system: Arc<ActorSystem>,
}

impl RikerSystem {
    pub fn new(label: &str, num_threads: usize) -> Result<RikerSystem, RikerSystemError> {
        let exec = ThreadPool::builder()
            .pool_size(num_threads)
            .name_prefix(format!("riker-{}-pool", label))
            .create()
            .map_err(|e| RikerSystemError::ThreadPool(e))?;
        let sys = SystemBuilder::new()
            .name(label)
            .exec(exec)
            .create()
            .map_err(|e| RikerSystemError::Riker(e))?;
        let wrap = RikerSystem {
            id_tracker: Arc::new(AtomicU64::new(0u64)),
            system: Arc::new(sys),
        };
        Ok(wrap)
    }

    /// Same as `ActorRefFactory::actor_of`, but always generating a unique name within the system.
    pub fn start<A>(
        &self,
        props: BoxActorProd<A>,
        name_prefix: &str,
    ) -> Result<ActorRef<A::Msg>, CreateError>
    where
        A: Actor,
    {
        let uid = self.id_tracker.fetch_add(1u64, Ordering::SeqCst);
        let name = format!("{}-{}", name_prefix, uid);
        self.system.actor_of_props(
            &name,
            props,
        )
    }

    pub fn ask<T, A, R>(&self, actor: &A, value: T) -> RemoteHandle<R>
    where
        T: Message,
        A: Tell<T>,
        R: Message,
    {
        ask::ask(self.system.as_ref(), actor, value)
    }
}
impl Deref for RikerSystem {
    type Target = ActorSystem;

    fn deref(&self) -> &Self::Target {
        &self.system
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone)]
    struct Ping(usize);

    #[derive(Debug)]
    struct MyActor {
        count: usize,
    }

    impl MyActor {
        fn new() -> MyActor {
            MyActor { count: 0 }
        }

        fn props() -> BoxActorProd<MyActor> {
            Props::new_from(MyActor::new)
        }
    }

    // Provide Actor implementation for our actor
    impl Actor for MyActor {
        type Msg = Ping;

        fn recv(&mut self, _ctx: &Context<Self::Msg>, msg: Self::Msg, sender: Sender) {
            self.count += msg.0;

            println!("received ping {:?}", msg);

            if let Some(s) = sender.as_ref() {
                s.try_tell(self.count, None).expect("Should have replied");
            }
        }
    }

    #[test]
    fn test_system_creation() {
        let sys = new_system("TestSystem");
        println!("Sending stuff!");
        //sys.run().expect("Should work maybe.");
        let addr = sys
            .start(MyActor::props(), "my_actor")
            .expect("Actor should start");
        println!("Got address! {:?}", addr);
        addr.tell(Ping(10), None);

        let res_f: RemoteHandle<usize> = sys.ask(&addr, Ping(1000));
        let res = res_f.wait();
        println!("Got result: {}", res);
        assert_eq!(res, 1010);
        sys.stop(addr);
        println!("Shutting down...");
        let shutdown_f = sys.shutdown();
        shutdown_f.wait().expect("Should have shut down!");
        println!("Finished!");
    }
}
