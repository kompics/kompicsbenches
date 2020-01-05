package se.kth.benchmarks.akka

import akka.actor.typed.ActorRefResolver
import org.scalatest._

import scala.util.{Failure, Random, Success, Try}
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import kompics.benchmarks.distributed._
import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}
import se.kth.benchmarks.akka.TypedPartitioningActor.{Done, InitAck}
import se.kth.benchmarks.akka.typed_bench.AtomicRegister.{Init, Run}
import se.kth.benchmarks.test.{DistributedTest => DTest}

class DistributedTest extends FunSuite with Matchers {
  test("Master-Client communication (untyped)") {
    val dtest = new DTest(se.kth.benchmarks.akka.bench.Factory);
    dtest.test();
  }

  test("Master-Client communication (typed)") {
    val dtest = new DTest(se.kth.benchmarks.akka.typed_bench.Factory);
    dtest.test();
  }

  test("Atomic Register Linearizability Test (untyped)"){
    import scala.collection.mutable.ListBuffer
    import scala.collection.immutable.List
    import se.kth.benchmarks.akka.PartitioningEvents.{Init, InitAck, Run, Identify, TestDone, Start}
    import se.kth.benchmarks.akka.bench.AtomicRegister.{Ack, AtomicRegisterActor, AtomicRegisterSerializer, ClientRef, Read, Value, Write}
    import se.kth.benchmarks.test.KVTestUtil.{KVTimestamp, isLinearizable}
    import java.util.concurrent.CountDownLatch
    import akka.actor.Props

    val serializers = SerializerBindings
      .empty()
      .addSerializer[AtomicRegisterSerializer](AtomicRegisterSerializer.NAME)
      .addBinding[Read](AtomicRegisterSerializer.NAME)
      .addBinding[Value](AtomicRegisterSerializer.NAME)
      .addBinding[Write](AtomicRegisterSerializer.NAME)
      .addBinding[Ack](AtomicRegisterSerializer.NAME)
      .addSerializer[PartitioningActorSerializer](PartitioningActorSerializer.NAME)
      .addBinding[Init](PartitioningActorSerializer.NAME)
      .addBinding[InitAck](PartitioningActorSerializer.NAME)
      .addBinding[Run.type](PartitioningActorSerializer.NAME)
      .addBinding[Done.type](PartitioningActorSerializer.NAME)
      .addBinding[Identify.type](PartitioningActorSerializer.NAME)
      .addBinding[TestDone](PartitioningActorSerializer.NAME)

    val workloads = List((0.5f, 0.5f), (0.95f, 0.05f))
    val r = Random
    for ((read_workload, write_workload) <- workloads){
      val num_keys: Long = r.nextInt(1000).toLong + 100L
      val partition_size: Int = {
        val i = r.nextInt(6) + 3
        if (i % 2 == 0) i + 1 else i  // uneven partition size
      }
      println(s"Atomic Register Linearizability Test (untyped) with partition size: $partition_size, number of keys: $num_keys, r: $read_workload, w: $write_workload")
      var nodes = ListBuffer[ClientRef]()
      val systems = for (i <- 0 until partition_size) yield {
        val system = ActorSystemProvider.newRemoteActorSystem(name = s"atomicregister$i", threads = 4, serialization = serializers)
        val atomicReg = system.actorOf(Props(new AtomicRegisterActor(read_workload, write_workload, true)), s"atomicreg$i")
        nodes += ClientRef(ActorSystemProvider.actorPathForRef(atomicReg, system))
        system
      }
      val prepare_latch = new CountDownLatch(1)
      val result_promise = Promise[List[KVTimestamp]]
      val resultF = result_promise.future
      val p_actor = systems.head.actorOf(Props(new PartitioningActor(prepare_latch, None, 1, nodes.toList, num_keys, partition_size, Some(result_promise))), "partitioningactor")
      p_actor ! Start
      prepare_latch.await();
      p_actor ! Run
      val results: List[KVTimestamp] = Await.result(resultF, 30 seconds)
      systems.foreach(_.terminate())  // TODO: Check future?
      val timestamps: Map[Long, List[KVTimestamp]] = results.groupBy(x => x.key)
//      timestamps.values.foreach(trace => isLinearizable(trace.sortBy(x => x.time), ListBuffer(0)) shouldBe true)
      timestamps.values.foreach{
        case trace =>
          val sorted = trace.sortBy(x => x.time)
//          println()
//          println(sorted)
          isLinearizable(sorted, ListBuffer(0)) shouldBe true
      }
    }
  }

  test("Atomic Register Linearizability Test (typed)"){
    import scala.collection.mutable.ListBuffer
    import scala.collection.immutable.List
    import se.kth.benchmarks.akka.typed_bench.AtomicRegister.{Ack, AtomicRegisterActor, AtomicRegisterSerializer, AtomicRegisterMessage, ClientRef, Init, Read, Run, Value, Write, SystemSupervisor}
    import se.kth.benchmarks.akka.typed_bench.AtomicRegister.SystemSupervisor._
    import se.kth.benchmarks.akka.TypedPartitioningActor.{Done, InitAck, TestDone}
    import se.kth.benchmarks.test.KVTestUtil.{KVTimestamp, isLinearizable}
    import java.util.concurrent.CountDownLatch

    val serializers = SerializerBindings
      .empty()
      .addSerializer[AtomicRegisterSerializer](AtomicRegisterSerializer.NAME)
      .addBinding[Run.type](AtomicRegisterSerializer.NAME)
      .addBinding[Read](AtomicRegisterSerializer.NAME)
      .addBinding[Write](AtomicRegisterSerializer.NAME)
      .addBinding[Value](AtomicRegisterSerializer.NAME)
      .addBinding[Ack](AtomicRegisterSerializer.NAME)
      .addSerializer[TypedPartitioningActorSerializer](TypedPartitioningActorSerializer.NAME)
      .addBinding[Done.type](TypedPartitioningActorSerializer.NAME)
      .addBinding[TestDone](TypedPartitioningActorSerializer.NAME)
      .addBinding[Init](TypedPartitioningActorSerializer.NAME)
      .addBinding[InitAck](TypedPartitioningActorSerializer.NAME)

    val workloads = List((0.5f, 0.5f), (0.95f, 0.05f))
    val r = Random
    for ((read_workload, write_workload) <- workloads){
      val num_keys: Long = r.nextInt(1000).toLong + 100L
      val partition_size: Int = {
        val i = r.nextInt(6) + 3
        if (i % 2 == 0) i + 1 else i  // uneven partition size
      }
      println(s"Atomic Register Linearizability Test (typed) with partition size: $partition_size, number of keys: $num_keys, r: $read_workload, w: $write_workload")
      var nodes = ListBuffer[ClientRef]()
      val client_systems = for (i <- 0 until partition_size - 1) yield {
        val system = ActorSystemProvider.newRemoteTypedActorSystem[AtomicRegisterMessage](
          AtomicRegisterActor(read_workload, write_workload, true),
          name = s"atomicreg_client$i" ,
          threads = 4,
          serialization = serializers
        )
        val resolver = ActorRefResolver(system)
        nodes += ClientRef(resolver.toSerializationFormat(system))
        system
      }
      val master_system = ActorSystemProvider.newRemoteTypedActorSystem[SystemSupervisor.SystemMessage](
        SystemSupervisor(),
        name = "atomicreg_supervisor",
        threads = 4,
        serialization = serializers
      )
      master_system ! StartAtomicRegister(read_workload, write_workload, true, 1)
      val prepare_latch = new CountDownLatch(1)
      val result_promise = Promise[List[KVTimestamp]]
      val resultF = result_promise.future
      master_system ! StartPartitioningActor(prepare_latch, None, 1, nodes.toList, num_keys, partition_size, Some(result_promise))
      prepare_latch.await();
      master_system ! RunIteration
      val results: List[KVTimestamp] = Await.result(resultF, 30 seconds)
      client_systems.foreach(_.terminate())  // TODO: Check future?
      master_system.terminate()
      val timestamps: Map[Long, List[KVTimestamp]] = results.groupBy(x => x.key)
      timestamps.values.foreach(trace => isLinearizable(trace.sortBy(x => x.time), ListBuffer(0)) shouldBe true)
    }
  }
}
