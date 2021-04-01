Implementation Issues
=====================


Actix
-----
https://actix.rs/

- No proper threadpools (router-style single actor type only)
- No dispatching (low-level tokio direct networking only)

Akka
----
- Atomic Register: The atomic register actor cannot find the actor refs of other nodes using `resolveOne()`. Hence, the actor has been implemented to send and `IDENTIFY` message to the actor paths of the other nodes in order for the receivers to retrieve the actorref using `sender()`. Each node will send an `INIT_ACK` message to the iteration actor when it has received #partition_size number of `IDENTIFY` messages.

Kompics Java
------------
- Atomic Register: The iteration component(`JIterationComponent`) is written in Scala due to Kompics Java does not allow to trigger the `RUN` event on the control port (from `KompicsSystemProvider`). Although written in Scala, it has to communicate with the atomic register component, which is written in Java. Hence, the iteration component will trigger Java NetMessages.

Kompact Mixed
-------------
- Atomic Broadcast: The different designs of reconfiguration in Paxos and Raft results in different client behaviour. The first proposal that arrives after the StopSign has been added to the sequence will be replied with a `PendingReconfiguration(id)` message, where `id` was the proposal. This allows for the client to wait until the new configuration is active and retry the proposals that are still pending up to `id`. The client cannot behave the same for Raft as it is not separated as clear when configurations start and stop. The experiments in Raft can thus result in dropped proposals. For instance, a Raft follower might forward proposals to the old leader. However, that server might already have been removed in the new configuration. The client does not know whether the proposal is simply late or has been dropped.

Riker
-----
https://github.com/riker-rs/riker/

- No networking implementation

Erlang
------
- APSP uses `gb_trees` instead of `array`, because multidimensional arrays in erlang have terrible ergonomics. Trees have worse lookup/update performance, though.
