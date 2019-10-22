Implementation Issues
=====================


Actix
-----
https://actix.rs/

- No proper threadpools (router-style single actor type only)
- No dispatching (low-level tokio direct networking only)

Akka
-----
- Atomic Register: The atomic register actor cannot find the actor refs of other nodes using `resolveOne()`. Hence, the actor has been implemented to send and `IDENTIFY` message to the actor paths of the other nodes in order for the receivers to retrieve the actorref using `sender()`. Each node will send an `INIT_ACK` message to the iteration actor when it has received #partition_size number of `IDENTIFY` messages.

Kompics Java
-----
- Atomic Register: The iteration component(`JIterationComponent`) is written in Scala due to Kompics Java does not allow to trigger the `RUN` event on the control port (from `KompicsSystemProvider`). Although written in Scala, it has to communicate with the atomic register component, which is written in Java. Hence, the iteration component will trigger Java NetMessages.

Riker
-----
https://github.com/riker-rs/riker/

- Not networking implementation
