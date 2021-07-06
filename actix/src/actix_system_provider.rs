//use super::*;

use actix::*;
//use actix::prelude::*;
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::channel::oneshot::channel as promise;
use futures::{StreamExt, Future};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::thread;
//pub struct ActixSystemProvider;

//impl ActixSystemProvider {
pub fn new_system<I: Into<String>>(name: I) -> ActixSystem {
    ActixSystem::new(name.into())
}
//}

pub struct ActixSystem {
    system_ref: Sender<SystemCommand>,
    core: Arc<Mutex<ActixSystemCore>>,
}

impl ActixSystem {
    fn new(label: String) -> ActixSystem {
        let (tx, rx) = channel(32);
        let core = ActixSystemCore {
            label,
            receiver: Some(rx),
        };
        let core = Arc::new(Mutex::new(core));
        let core2 = core.clone();

        thread::spawn(move || {
            let mut guard = core.lock().unwrap();
            guard.run();
        });

        ActixSystem {
            system_ref: tx,
            core: core2,
        }
    }

    // pub fn run(&mut self) -> Result<(), SystemError> {
    //     self.system_ref
    //         .try_send(SystemCommand::Run)
    //         .map_err(|_| SystemError::SendError)
    // }

    pub fn shutdown(mut self) -> Result<(), SystemError> {
        self.system_ref
            .try_send(SystemCommand::Shutdown)
            .map_err(|_| SystemError::SendError)
            .and_then(|_| {
                // once the lock can be acquired the thread must have finished
                self.core
                    .lock()
                    .map(|g| drop(g))
                    .map_err(|_| SystemError::SendError)
            })
    }

    pub fn start<A, F>(&mut self, actor_fn: F) -> Result<Addr<A>, SystemError>
    where
        A: Actor<Context = Context<A>>,
        F: FnOnce() -> A + 'static + Send,
    {
        let (tx, rx) = promise::<Addr<A>>();
        let fun = || {
            let addr = actor_fn().start();
            let _ = tx.send(addr); // ignore if no one is listening
        };
        let boxed = Box::new(fun);
        self.system_ref
            .try_send(SystemCommand::Start(boxed))
            .map_err(|_| SystemError::SendError);
        futures::executor::block_on(rx).map_err(|_| SystemError::SendError)
            //.and_then(|_| futures::executor::block_on(rx).map_err(|_| SystemError::SendError))

    }

    pub fn stop<A>(&mut self, actor: Addr<A>) -> Result<(), SystemError>
    where
        A: Actor<Context = Context<A>> + Handler<PoisonPill>,
    {
        futures::executor::block_on(actor
            .send(PoisonPill))
            .map_err(|_| SystemError::SendError)
    }
}

//pub trait StoppableActor {}
pub struct PoisonPill;
impl Message for PoisonPill {
    type Result = ();
}

// impl<A> Handler<PoisonPill> for A
// where
//     A: Actor<Context = Context<A>> + StoppableActor,
// {
//     type Result = ();

//     fn handle(&mut self, msg: PoisonPill, ctx: &mut Context<Self>) -> Self::Result {
//         println!("PoisonPill received, shutting down.");
//         ctx.stop();
//     }
// }

#[derive(Debug, PartialEq)]
pub enum SystemError {
    SendError,
}

enum SystemCommand {
    Start(Box<dyn FnOnce() + Send>),
    //Stop,
    //Run,
    Shutdown,
}
impl fmt::Debug for SystemCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SystemCommand::Start(_) => write!(f, "SystemCommand::Start(<fun>)"),
            //SystemCommand::Stop => write!(f, "SystemCommand::Stop"),
            //SystemCommand::Run => write!(f, "SystemCommand::Run"),
            SystemCommand::Shutdown => write!(f, "SystemCommand::Shutdown"),
        }
    }
}

struct ActixSystemCore {
    label: String,
    receiver: Option<Receiver<SystemCommand>>,
}

impl ActixSystemCore {
    fn run(&mut self) -> () {
        let system = System::new();
            //(self.label.clone());

        let command_receiver = Box::new(self.receiver.take().unwrap());
        let manager_future = command_receiver.for_each(|cmd| {
            match cmd {
                SystemCommand::Start(f) => {
                    //println!("Starting something!");
                    f();
                }
                //SystemCommand::Stop => println!("Stopping something!"),
                //SystemCommand::Run => println!("Running something!"),
                SystemCommand::Shutdown => {
                    println!("Shutting down");
                    System::current().stop();
                }
            }
            futures::future::ready(())
        });

        System::current().arbiter().spawn(manager_future);

        println!("Running system...");
        system.run().expect("Should be running.");
        println!("System stopped!");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    struct Ping(usize);

    impl Message for Ping {
        type Result = usize;
    }

    #[derive(Debug)]
    struct MyActor {
        count: usize,
    }

    impl MyActor {
        fn new() -> MyActor {
            MyActor { count: 0 }
        }
    }

    // Provide Actor implementation for our actor
    impl Actor for MyActor {
        type Context = Context<Self>;

        fn started(&mut self, _ctx: &mut Context<Self>) {
            println!("Actor is alive");
        }

        fn stopped(&mut self, _ctx: &mut Context<Self>) {
            println!("Actor is stopped");
        }
    }

    impl Handler<Ping> for MyActor {
        type Result = usize;

        fn handle(&mut self, msg: Ping, _ctx: &mut Context<Self>) -> Self::Result {
            self.count += msg.0;

            self.count
        }
    }

    impl Handler<PoisonPill> for MyActor {
        type Result = ();

        fn handle(&mut self, _msg: PoisonPill, ctx: &mut Context<Self>) -> Self::Result {
            println!("PoisonPill received, shutting down.");
            ctx.stop();
        }
    }

    /*
    #[test]
    fn test_system_creation() {
        let mut sys = new_system("TestSystem");
        println!("Sending stuff!");
        //sys.run().expect("Should work maybe.");
        let addr = sys
            .start(move || MyActor::new())
            .expect("Actor should start");
        println!("Got address! {:?}", addr);
        let res_f = addr.send(Ping(10));
        let res = res_f.wait().expect("Result");
        println!("Got result: {}", res);
        println!("Waiting for things to happen...");
        thread::sleep(Duration::from_millis(1000));
        sys.stop(addr).expect("Actor should have shut down");
        println!("Shutting down...");
        sys.shutdown().expect("Should work maybe.");
        println!("Finished!");
    }

     */
}
