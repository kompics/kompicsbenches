package se.kth.benchmarks.akka.bench

import akka.actor._
import akka.serialization.Serializer
import akka.util.ByteString
import akka.event.Logging
import se.kth.benchmarks.akka._
import se.kth.benchmarks._

import scala.util.{Failure, Success, Try}
import scala.concurrent.Await
import scala.concurrent.duration._
import java.util.concurrent.{CountDownLatch, TimeUnit}

import IterationActor._
import kompics.benchmarks.benchmarks.AtomicRegisterRequest

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Try

object AtomicRegister extends DistributedBenchmark {

  case class ClientRef(actorPath: String)
  case class ClientParams(read_workload: Float, write_workload: Float)
  class FailedPreparationException(cause: String) extends Exception

  override type MasterConf = AtomicRegisterRequest
  override type ClientConf = ClientParams
  override type ClientData = ClientRef

  val serializers = SerializerBindings
    .empty()
    .addSerializer[AtomicRegisterSerializer](AtomicRegisterSerializer.NAME)
    .addBinding[DONE.type](AtomicRegisterSerializer.NAME)
    .addBinding[READ](AtomicRegisterSerializer.NAME)
    .addBinding[VALUE](AtomicRegisterSerializer.NAME)
    .addBinding[WRITE](AtomicRegisterSerializer.NAME)
    .addBinding[ACK](AtomicRegisterSerializer.NAME)
    .addBinding[IDENTIFY.type](AtomicRegisterSerializer.NAME)
    .addSerializer[IterationActorSerializer](IterationActorSerializer.NAME)
    .addBinding[INIT](IterationActorSerializer.NAME)
    .addBinding[INIT_ACK](IterationActorSerializer.NAME)
    .addBinding[RUN.type](IterationActorSerializer.NAME)

  class MasterImpl extends Master {
    private var read_workload = 0.0F;
    private var write_workload = 0.0F;
    private var partition_size: Int = -1;
    private var num_keys: Long = -1l;
    private var system: ActorSystem = null;
    private var atomicRegister: ActorRef = null;
    private var iterationActor: ActorRef = null;
    private var prepare_latch: CountDownLatch = null;
    private var finished_latch: CountDownLatch = null;
    private var init_id: Int = -1;

    override def setup(c: MasterConf): ClientConf = {
      println("Atomic Register(Master) Setup!")
      system = ActorSystemProvider.newRemoteActorSystem(
        name = "atomicregister",
        threads = 1,
        serialization = serializers);

      this.read_workload = c.readWorkload;
      this.write_workload = c.writeWorkload;
      this.partition_size = c.partitionSize;
      this.num_keys = c.numberOfKeys;
      ClientParams(read_workload, write_workload)
    };

    override def prepareIteration(d: List[ClientData]): Unit = {
      atomicRegister = system.actorOf(Props(new AtomicRegisterActor(read_workload, write_workload)), "atomicreg")
      val atomicRegPath = ActorSystemProvider.actorPathForRef(atomicRegister, system)
      println(s"Atomic Register(Master) path is $atomicRegPath")
      val nodes = ClientRef(atomicRegPath) :: d
      val num_nodes = nodes.size
      assert(partition_size <= num_nodes && partition_size > 0 && read_workload + write_workload == 1)
      init_id += 1
      prepare_latch = new CountDownLatch(1)
      finished_latch = new CountDownLatch(1)
      iterationActor = system.actorOf(Props(new IterationActor(prepare_latch, finished_latch, init_id, nodes, num_keys, partition_size)), "itactor")
      iterationActor ! START
      val timeout = 100
      val timeunit = TimeUnit.SECONDS
      val successful_prep = prepare_latch.await(timeout, timeunit)
      if (!successful_prep) {
        println("Timeout in prepareIteration for INIT_ACK")
        throw new FailedPreparationException("Timeout waiting for INIT ACK from all nodes")
      }
    }

    override def runIteration(): Unit = {
      iterationActor ! RUN
      finished_latch.await()
    };

    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      println("Cleaning up Atomic Register(Master) side");
      if (prepare_latch != null) prepare_latch = null
      if (finished_latch != null) finished_latch = null
      if (atomicRegister != null) {
        system.stop(atomicRegister)
        atomicRegister = null
        system.stop(iterationActor)
        iterationActor = null
      }
      if (lastIteration) {
        println("Cleaning up Last iteration")
        system.terminate();
        Await.ready(system.whenTerminated, 5.second);
        system = null;
        println("Last cleanup completed!")
      }
    }
  }

  class ClientImpl extends Client {
    private var read_workload = 0.0F;
    private var write_workload = 0.0F;
    private var system: ActorSystem = null;
    private var atomicRegister: ActorRef = null;

    val serializers = SerializerBindings
      .empty()
      .addSerializer[AtomicRegisterSerializer](AtomicRegisterSerializer.NAME)
      .addBinding[DONE.type](AtomicRegisterSerializer.NAME)
      .addBinding[READ](AtomicRegisterSerializer.NAME)
      .addBinding[VALUE](AtomicRegisterSerializer.NAME)
      .addBinding[WRITE](AtomicRegisterSerializer.NAME)
      .addBinding[ACK](AtomicRegisterSerializer.NAME)
      .addBinding[IDENTIFY.type](AtomicRegisterSerializer.NAME)
      .addSerializer[IterationActorSerializer](IterationActorSerializer.NAME)
      .addBinding[INIT](IterationActorSerializer.NAME)
      .addBinding[INIT_ACK](IterationActorSerializer.NAME)
      .addBinding[RUN.type](IterationActorSerializer.NAME)

    override def setup(c: ClientConf): ClientData = {
      println("Atomic Register(Client) Setup!")
      system = ActorSystemProvider.newRemoteActorSystem(
        name = "atomicregister",
        threads = 1,
        serialization = serializers);
      this.read_workload = c.read_workload
      this.write_workload = c.write_workload
      atomicRegister = system.actorOf(Props(new AtomicRegisterActor(read_workload, write_workload)), "atomicreg")
      val path = ActorSystemProvider.actorPathForRef(atomicRegister, system);
      println(s"Atomic Register Path is $path");
      ClientRef(path)
    }

    override def prepareIteration(): Unit = {
      println("Preparing Atomic Register(Client) iteration")
    }

    override def cleanupIteration(lastIteration: Boolean): Unit = {
      println("Cleaning up Atomic Register(Client) side")
      if (lastIteration) {
        println("Cleaning up Last iteration")
        if (atomicRegister != null){
          system.stop(atomicRegister)
          atomicRegister = null;
        }
        system.terminate();
        Await.ready(system.whenTerminated, 5.second);
        system = null;
        println("Last cleanup completed!")
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

  class AtomicRegisterActor(read_workload: Float, write_workload: Float) extends Actor {
    implicit def addComparators[A](x: A)(implicit o: math.Ordering[A]): o.Ops = o.mkOrderingOps(x); // for tuple comparison

    val logger = Logging(context.system, this)

    var nodes: List[ActorRef] =  _
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

    private def bcast(receivers: List[ActorRef], msg: AtomicRegisterMessage): Unit = {
      for (node <- receivers) node ! msg
    }

    private def newIteration(i: INIT): Unit = {
      current_run_id = i.init_id
//      nodes =
        for (c: ClientRef <- i.nodes) yield {
          val f = context.system.actorSelection(c.actorPath)
          f ! IDENTIFY
          /*  TODO: resolveOne doesn't work?
          val f = context.system.actorSelection(c.actorPath).resolveOne(10 seconds)
          val a_ref: ActorRef = Await.result(f, 30 seconds)
          logger.info(s"RESOLVED PATH=$a_ref")
          a_ref*/
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
      bcast(nodes, READ(current_run_id, key, register.rid))
    }

    private def invokeWrite(key: Long): Unit = {
      val wval = selfRank
      val register = register_state(key)
      register.rid += 1
      register.writeval = wval
      register.acks = 0
      register.reading = false
      register_readlist(key).clear()
      bcast(nodes, READ(current_run_id, key, register.rid))
    }

    private def invokeOperations(): Unit = {
      val num_keys = max_key - min_key + 1
      val num_reads = (num_keys * read_workload).toLong
      val num_writes = (num_keys * write_workload).toLong

      read_count = num_reads
      write_count = num_writes

      if (selfRank % 2 == 0) {
        for (i <- 0l until num_reads) invokeRead(min_key + i)
        for (i <- 0l until num_writes) invokeWrite(min_key + num_reads + i)
      } else {
        for (i <- 0l until num_writes) invokeWrite(min_key + i)
        for (i <- 0l until num_reads) invokeRead(min_key + num_writes + i)
      }
    }

    private def readResponse(key: Long, read_value: Int): Unit = {
      read_count -= 1
      if (read_count == 0 && write_count == 0) master ! DONE
    }

    private def writeResponse(key: Long): Unit = {
      write_count -= 1
      if (read_count == 0 && write_count == 0) master ! DONE
    }

    override def receive = {

      case IDENTIFY => {
        nodes_listBuffer += sender()
        if (nodes_listBuffer.size == n) {
          master ! INIT_ACK(current_run_id)
          nodes = nodes_listBuffer.toList
          nodes_listBuffer.clear()
        }
      }

      case i: INIT => {
        newIteration(i)
        master = sender()
      }

      case RUN => {
        invokeOperations()
      }

      case READ(current_run_id, key, readId) => {
        val current_state: AtomicRegisterState = register_state(key)
        sender() ! VALUE(current_run_id, key, readId, current_state.ts, current_state.wr, current_state.value)
      }

      case v: VALUE => {
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
                bcast(nodes, WRITE(v.run_id, v.key, v.rid, maxts, rr, bcastvalue))
              }
            }
          }
        }

      }

      case w: WRITE => {
        if (w.run_id == current_run_id) {
          val current_state = register_state(w.key)
          if ((w.ts, w.wr) > (current_state.ts, current_state.wr)) {
            current_state.ts = w.ts
            current_state.wr = w.wr
            current_state.value = w.value
          }
        }
        sender() ! ACK(w.run_id, w.key, w.rid)
      }

      case a: ACK => {
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

  trait AtomicRegisterMessage
  case object DONE extends AtomicRegisterMessage
  case object IDENTIFY extends AtomicRegisterMessage
  case class READ(run_id: Int, key: Long, rid: Int) extends AtomicRegisterMessage
  case class ACK(run_id: Int, key: Long, rid: Int) extends AtomicRegisterMessage
  case class VALUE(run_id: Int, key: Long, rid: Int, ts: Int, wr: Int, value: Int) extends AtomicRegisterMessage
  case class WRITE(run_id: Int, key: Long, rid: Int, ts: Int, wr: Int, value: Int) extends AtomicRegisterMessage

  object AtomicRegisterSerializer {
    val NAME = "atomicregister"

    private val READ_FLAG: Byte = 1
    private val WRITE_FLAG: Byte = 2
    private val ACK_FLAG: Byte = 3
    private val VALUE_FLAG: Byte = 4
    private val DONE_FLAG: Byte = 5
    private val IDENTIFY_FLAG: Byte = 6
  }

  class AtomicRegisterSerializer extends Serializer {
    import AtomicRegisterSerializer._
    import java.nio.{ ByteBuffer, ByteOrder }

    implicit val order = ByteOrder.BIG_ENDIAN;

    override def identifier: Int = SerializerIds.ATOMICREG
    override def includeManifest: Boolean = false

    override def toBinary(o: AnyRef): Array[Byte] = {
      o match {
        case DONE => Array(DONE_FLAG)
        case IDENTIFY => Array(IDENTIFY_FLAG)
        case r: READ => ByteString.createBuilder.putByte(READ_FLAG).putInt(r.run_id).putLong(r.key).putInt(r.rid).result().toArray
        case w: WRITE => ByteString.createBuilder.putByte(WRITE_FLAG).putInt(w.run_id).putLong(w.key).putInt(w.rid).putInt(w.ts).putInt(w.wr).putInt(w.value).result().toArray
        case v: VALUE => ByteString.createBuilder.putByte(VALUE_FLAG).putInt(v.run_id).putLong(v.key).putInt(v.rid).putInt(v.ts).putInt(v.wr).putInt(v.value).result().toArray
        case a: ACK => ByteString.createBuilder.putByte(ACK_FLAG).putInt(a.run_id).putLong(a.key).putInt(a.rid).result().toArray
      }
    }

    override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
      val buf = ByteBuffer.wrap(bytes).order(order);
      val flag = buf.get;
      flag match {
        case DONE_FLAG => DONE
        case IDENTIFY_FLAG => IDENTIFY
        case READ_FLAG => {
          val run_id = buf.getInt
          val key = buf.getLong
          val rid = buf.getInt
          READ(run_id, key, rid)
        }
        case ACK_FLAG => {
          val run_id = buf.getInt
          val key = buf.getLong
          val rid = buf.getInt
          ACK(run_id, key, rid)
        }
        case WRITE_FLAG => {
          val run_id = buf.getInt
          val key = buf.getLong
          val rid = buf.getInt
          val ts = buf.getInt
          val wr = buf.getInt
          val value = buf.getInt
          WRITE(run_id, key, rid, ts, wr, value)
        }
        case VALUE_FLAG => {
          val run_id = buf.getInt
          val key = buf.getLong
          val rid = buf.getInt
          val ts = buf.getInt
          val wr = buf.getInt
          val value = buf.getInt
          VALUE(run_id, key, rid, ts, wr, value)
        }
      }
    }
  }
}

