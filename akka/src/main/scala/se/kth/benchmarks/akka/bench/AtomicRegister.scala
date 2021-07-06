package se.kth.benchmarks.akka.bench

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.serialization.Serializer
import akka.util.ByteString
import akka.event.Logging
import se.kth.benchmarks.akka._
import se.kth.benchmarks._

import scala.util.{Failure, Success, Try}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import java.util.concurrent.{CountDownLatch, TimeUnit, TimeoutException}

import se.kth.benchmarks.akka.PartitioningActor
import se.kth.benchmarks.akka.PartitioningEvents._
import kompics.benchmarks.benchmarks.AtomicRegisterRequest

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Try
import java.util.UUID.randomUUID

import scala.math.Ordering.Implicits._

import com.typesafe.scalalogging.StrictLogging
import se.kth.benchmarks.test.KVTestUtil.{KVTimestamp, ReadInvokation, ReadResponse, WriteInvokation, WriteResponse}

object AtomicRegister extends DistributedBenchmark with StrictLogging {

  case class ClientRef(actorPath: String)
  case class ClientParams(read_workload: Float, write_workload: Float)
  class FailedPreparationException(cause: String) extends Exception

  override type MasterConf = AtomicRegisterRequest
  override type ClientConf = ClientParams
  override type ClientData = ClientRef

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

  class MasterImpl extends Master {
    private var read_workload = 0.0f;
    private var write_workload = 0.0f;
    private var partition_size: Int = -1;
    private var num_keys: Long = -1L;
    private var system: ActorSystem = null;
    private var atomicRegister: ActorRef = null;
    private var partitioningActor: ActorRef = null;
    private var prepare_latch: CountDownLatch = null;
    private var finished_latch: CountDownLatch = null;
    private var init_id: Int = -1;

    override def setup(c: MasterConf, meta: DeploymentMetaData): Try[ClientConf] = Try {
      logger.info("Atomic Register(Master) Setup!");

      this.read_workload = c.readWorkload;
      this.write_workload = c.writeWorkload;
      this.partition_size = c.partitionSize;
      this.num_keys = c.numberOfKeys;
      val numNodes = meta.numberOfClients + 1;
      assert(partition_size <= numNodes, s"Invalid partition size $partition_size > $numNodes (number of nodes).");
      assert(partition_size > 0, s"Invalid partition size $partition_size <= 0.");
      assert((1.0 - (read_workload + write_workload)) < 0.00001,
             s"Invalid workload sum ${read_workload + write_workload} != 1.0");
      this.system =
        ActorSystemProvider.newRemoteActorSystem(name = "atomicregister", threads = 4, serialization = serializers);
      ClientParams(read_workload, write_workload)
    };

    override def prepareIteration(d: List[ClientData]): Unit = {
      val testing = false
      atomicRegister =
        system.actorOf(Props(new AtomicRegisterActor(read_workload, write_workload, testing)), s"atomicreg$init_id");
      val atomicRegPath = ActorSystemProvider.actorPathForRef(atomicRegister, system);
      logger.debug(s"Atomic Register(Master) path is $atomicRegPath")
      val nodes = ClientRef(atomicRegPath) :: d;
      init_id += 1;
      prepare_latch = new CountDownLatch(1);
      finished_latch = new CountDownLatch(1);
      partitioningActor = system.actorOf(
        Props(new PartitioningActor(prepare_latch, Some(finished_latch), init_id, nodes, num_keys, partition_size, None)),
        s"partitioningactor$init_id"
      );
      partitioningActor ! Start;
      prepare_latch.await();
    }

    override def runIteration(): Unit = {
      partitioningActor ! Run;
      finished_latch.await();
    };

    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      logger.debug("Cleaning up Atomic Register(Master) side");
      if (prepare_latch != null) prepare_latch = null;
      if (finished_latch != null) finished_latch = null;
      if (atomicRegister != null) {
        system.stop(atomicRegister);
        atomicRegister = null;
      }
      if (partitioningActor != null) {
        system.stop(partitioningActor);
        partitioningActor = null;
      }
      if (lastIteration) {
        logger.debug("Cleaning up Last iteration");
        try {
          val f = system.terminate();
          Await.ready(f, 11.second);
          system = null;
          logger.info("Last cleanup completed!");
        } catch {
          case ex: Exception => logger.error("Failed to terminate ActorSystem", ex)
        }
      }
    }
  }

  class ClientImpl extends Client with StrictLogging {
    private var read_workload = 0.0f;
    private var write_workload = 0.0f;
    private var system: ActorSystem = null;
    private var atomicRegister: ActorRef = null;

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
      .addBinding[Identify.type](PartitioningActorSerializer.NAME);

    override def setup(c: ClientConf): ClientData = {
      system =
        ActorSystemProvider.newRemoteActorSystem(name = "atomicregister", threads = 4, serialization = serializers);
      this.read_workload = c.read_workload;
      this.write_workload = c.write_workload;
      val testing = false
      atomicRegister =
        system.actorOf(Props(new AtomicRegisterActor(read_workload, write_workload, testing)), s"atomicreg${randomUUID()}");
      val path = ActorSystemProvider.actorPathForRef(atomicRegister, system);
      logger.debug(s"Atomic Register Path is $path");
      ClientRef(path)
    }

    override def prepareIteration(): Unit = {
      logger.debug("Preparing Atomic Register(Client) iteration");
    }

    override def cleanupIteration(lastIteration: Boolean): Unit = {
      logger.debug("Cleaning up Atomic Register(Client) side")
      if (lastIteration) {
        logger.debug("Cleaning up Last iteration")
        atomicRegister = null
        try {
          val f = system.terminate();
          Await.ready(f, 11.second);
          system = null;
          logger.info("Last cleanup completed!")
        } catch {
          case ex: Exception => logger.error("Failed to terminate ActorSystem!", ex)
        }
      }
    }
  }

  override def newMaster(): Master = new MasterImpl();

  override def msgToMasterConf(msg: scalapb.GeneratedMessage): Try[MasterConf] = Try {
    msg.asInstanceOf[AtomicRegisterRequest]
  };

  override def newClient(): Client = new ClientImpl();

  override def strToClientConf(str: String): Try[ClientConf] = Try {
    val split = str.split(":");
    assert(split.length == 2);
    ClientParams(split(0).toFloat, split(1).toFloat)
  }

  override def strToClientData(str: String): Try[ClientData] = Success(ClientRef(str));

  override def clientConfToString(c: ClientConf): String = s"${c.read_workload}:${c.write_workload}";

  override def clientDataToString(d: ClientData): String = d.actorPath;

  class AtomicRegisterState {
    var (ts, wr) = (0, 0)
    var value = 0
    var acks = 0
    var readval = 0
    var writeval = 0
    var rid = 0
    var reading = false
    var first_received_ts = 0
    var skip_impose = true
  }

  class AtomicRegisterActor(read_workload: Float, write_workload: Float, testing: Boolean) extends Actor {
    //implicit def addComparators[A](x: A)(implicit o: math.Ordering[A]): o.Ops =
    //  o.mkOrderingOps(x); // for tuple comparison

    val logger = Logging(context.system, this)

    var nodes: List[ActorRef] = _
    var nodes_listBuffer = new ListBuffer[ActorRef]
    var n = 0
    var selfRank: Int = -1
    var register_state: mutable.Map[Long, AtomicRegisterState] = mutable.Map.empty // (key, state)
    var register_readlist: mutable.Map[Long, mutable.Map[ActorRef, (Int, Int, Int)]] = mutable.Map.empty

    var min_key: Long = -1
    var max_key: Long = -1

    /* Experiment variables */
    var read_count: Long = 0
    var write_count: Long = 0
    var master: ActorRef = _
    var current_run_id: Int = -1

    /* Linearizability test variables */
    var timestamps = ListBuffer[KVTimestamp]()

    private def bcast(receivers: List[ActorRef], msg: AtomicRegisterMessage): Unit = {
      for (node <- receivers) node ! msg
    }

    private def newIteration(i: Init): Unit = {
      current_run_id = i.init_id
      for (c: ClientRef <- i.nodes) {
        val f = context.system.actorSelection(c.actorPath)
        f ! Identify
      }
      n = i.nodes.size
      selfRank = i.rank
      min_key = i.min
      max_key = i.max

      /* Reset KV and states */
      register_state.clear()
      register_readlist.clear()
      for (i <- min_key to max_key) {
        register_state += (i -> new AtomicRegisterState)
        register_readlist += (i -> mutable.Map.empty[ActorRef, (Int, Int, Int)])
      }
    }

    private def invokeRead(key: Long): Unit = {
      val register = register_state(key)
      register.rid += 1;
      register.acks = 0
      register_readlist(key).clear()
      register.reading = true
      if (testing) timestamps += KVTimestamp(key, ReadInvokation, None, System.currentTimeMillis(), selfRank)
      bcast(nodes, Read(current_run_id, key, register.rid))
    }

    private def invokeWrite(key: Long): Unit = {
      val wval = selfRank
      val register = register_state(key)
      register.rid += 1
      register.writeval = wval
      register.acks = 0
      register.reading = false
      register_readlist(key).clear()
      if (testing) timestamps += KVTimestamp(key, WriteInvokation, Some(selfRank), System.currentTimeMillis(), selfRank)
      bcast(nodes, Read(current_run_id, key, register.rid))
    }

    private def invokeOperations(): Unit = {
      val num_keys = max_key - min_key + 1
      val num_reads = (num_keys * read_workload).toLong
      val num_writes = (num_keys * write_workload).toLong

      read_count = num_reads
      write_count = num_writes

      if (selfRank % 2 == 0) {
        for (i <- 0L until num_reads) invokeRead(min_key + i)
        for (i <- 0L until num_writes) invokeWrite(min_key + num_reads + i)
      } else {
        for (i <- 0L until num_writes) invokeWrite(min_key + i)
        for (i <- 0L until num_reads) invokeRead(min_key + num_writes + i)
      }
    }

    private def sendDone(): Unit = {
      logger.debug("Sending Done!")
      if (!testing) master ! Done
      else master ! TestDone(timestamps.toList)
    }

    private def readResponse(key: Long, read_value: Int): Unit = {
      read_count -= 1
      if (testing) timestamps += KVTimestamp(key, ReadResponse, Some(read_value), System.currentTimeMillis(), selfRank)
      if (read_count == 0 && write_count == 0) sendDone()
    }

    private def writeResponse(key: Long): Unit = {
      write_count -= 1
      if (testing){
        val write_value = selfRank  // we always write with our rank
        timestamps += KVTimestamp(key, WriteResponse, Some(write_value), System.currentTimeMillis(), selfRank)
      }
      if (read_count == 0 && write_count == 0) sendDone()
    }

    override def receive = {

      case Identify => {
        nodes_listBuffer += sender()
        if (nodes_listBuffer.size == n) {
          master ! InitAck(current_run_id)
          nodes = nodes_listBuffer.toList
          nodes_listBuffer.clear()
        }
      }

      case i: Init => {
        newIteration(i)
        master = sender()
      }

      case Run => {
        invokeOperations()
      }

      case Read(current_run_id, key, readId) => {
        val current_state: AtomicRegisterState = register_state(key)
        sender() ! Value(current_run_id, key, readId, current_state.ts, current_state.wr, current_state.value)
      }

      case v: Value => {
        if (v.run_id == current_run_id) {
          val current_register = register_state(v.key)
          if (v.rid == current_register.rid) {
            var readlist = register_readlist(v.key)
            if (current_register.reading) {
              if (readlist.isEmpty) {
                current_register.first_received_ts = v.ts
                current_register.readval = v.value
              } else if (current_register.skip_impose) {
                if (current_register.first_received_ts != v.ts) current_register.skip_impose = false
              }
            }
            val src = sender()
            readlist(src) = (v.ts, v.wr, v.value)
            if (readlist.size > n / 2) {
              if (current_register.reading && current_register.skip_impose) {
                current_register.value = current_register.readval
                register_readlist(v.key).clear()
                readResponse(v.key, current_register.readval)
              } else {
                var (maxts, rr, readvalue) = readlist.values.maxBy(_._1)
                current_register.readval = readvalue
                register_readlist(v.key).clear()
                var bcastvalue = readvalue
                if (!current_register.reading) {
                  rr = selfRank
                  maxts += 1
                  bcastvalue = current_register.writeval
                }
                bcast(nodes, Write(v.run_id, v.key, v.rid, maxts, rr, bcastvalue))
              }
            }
          }
        }

      }

      case w: Write => {
        if (w.run_id == current_run_id) {
          val current_state = register_state(w.key)
          if ((w.ts, w.wr) > (current_state.ts, current_state.wr)) {
            current_state.ts = w.ts
            current_state.wr = w.wr
            current_state.value = w.value
          }
        }
        sender() ! Ack(w.run_id, w.key, w.rid)
      }

      case a: Ack => {
        if (a.run_id == current_run_id) {
          val current_register = register_state(a.key)
          if (a.rid == current_register.rid) {
            current_register.acks += 1
            if (current_register.acks > n / 2) {
              register_state(a.key).acks = 0
              if (current_register.reading) {
                readResponse(a.key, current_register.readval)
              } else {
                writeResponse(a.key)
              }
            }
          }
        }
      }
    }
  }

  sealed trait AtomicRegisterMessage
  case class ResolvedActors(actorRefs: List[ActorRef])
  case class Read(run_id: Int, key: Long, rid: Int) extends AtomicRegisterMessage
  case class Ack(run_id: Int, key: Long, rid: Int) extends AtomicRegisterMessage
  case class Value(run_id: Int, key: Long, rid: Int, ts: Int, wr: Int, value: Int) extends AtomicRegisterMessage
  case class Write(run_id: Int, key: Long, rid: Int, ts: Int, wr: Int, value: Int) extends AtomicRegisterMessage

  object AtomicRegisterSerializer {
    val NAME = "atomicregister"

    private val READ_FLAG: Byte = 1
    private val WRITE_FLAG: Byte = 2
    private val ACK_FLAG: Byte = 3
    private val VALUE_FLAG: Byte = 4
  }

  class AtomicRegisterSerializer extends Serializer {
    import AtomicRegisterSerializer._
    import java.nio.{ByteBuffer, ByteOrder}

    implicit val order = ByteOrder.BIG_ENDIAN;

    override def identifier: Int = SerializerIds.ATOMICREG
    override def includeManifest: Boolean = false

    override def toBinary(o: AnyRef): Array[Byte] = {
      o match {
        case r: Read =>
          ByteString.createBuilder.putByte(READ_FLAG).putInt(r.run_id).putLong(r.key).putInt(r.rid).result().toArray
        case w: Write =>
          ByteString.createBuilder
            .putByte(WRITE_FLAG)
            .putInt(w.run_id)
            .putLong(w.key)
            .putInt(w.rid)
            .putInt(w.ts)
            .putInt(w.wr)
            .putInt(w.value)
            .result()
            .toArray
        case v: Value =>
          ByteString.createBuilder
            .putByte(VALUE_FLAG)
            .putInt(v.run_id)
            .putLong(v.key)
            .putInt(v.rid)
            .putInt(v.ts)
            .putInt(v.wr)
            .putInt(v.value)
            .result()
            .toArray
        case a: Ack =>
          ByteString.createBuilder.putByte(ACK_FLAG).putInt(a.run_id).putLong(a.key).putInt(a.rid).result().toArray
      }
    }

    override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
      val buf = ByteBuffer.wrap(bytes).order(order);
      val flag = buf.get;
      flag match {
        case READ_FLAG => {
          val run_id = buf.getInt
          val key = buf.getLong
          val rid = buf.getInt
          Read(run_id, key, rid)
        }
        case ACK_FLAG => {
          val run_id = buf.getInt
          val key = buf.getLong
          val rid = buf.getInt
          Ack(run_id, key, rid)
        }
        case WRITE_FLAG => {
          val run_id = buf.getInt
          val key = buf.getLong
          val rid = buf.getInt
          val ts = buf.getInt
          val wr = buf.getInt
          val value = buf.getInt
          Write(run_id, key, rid, ts, wr, value)
        }
        case VALUE_FLAG => {
          val run_id = buf.getInt
          val key = buf.getLong
          val rid = buf.getInt
          val ts = buf.getInt
          val wr = buf.getInt
          val value = buf.getInt
          Value(run_id, key, rid, ts, wr, value)
        }
      }
    }
  }
}
