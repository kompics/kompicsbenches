package se.kth.benchmarks.kompicsscala.bench

import java.util.{ NoSuchElementException, Optional, UUID }
import java.util.concurrent.{ CountDownLatch, TimeUnit }

import io.netty.buffer.ByteBuf
import kompics.benchmarks.benchmarks.AtomicRegisterRequest
import scalapb.GeneratedMessage
import se.kth.benchmarks.{ DistributedBenchmark, kompicsscala }

import scala.concurrent.duration._
import se.kth.benchmarks.kompicsscala._
import se.sics.kompics.network.Network
import se.sics.kompics.network.netty.serialization.{ Serializer, Serializers }
import se.sics.kompics.sl._

import scala.collection.mutable
import scala.concurrent.Await
import scala.util.{ Random, Success, Try }

object AtomicRegister extends DistributedBenchmark {

  case class ClientParams(read_workload: Float, write_workload: Float)
  class FailedPreparationException(cause: String) extends Exception

  override type MasterConf = AtomicRegisterRequest
  override type ClientConf = ClientParams
  override type ClientData = NetAddress

  class MasterImpl extends Master {
    private var read_workload = 0.0F;
    private var write_workload = 0.0F;
    private var partition_size: Int = -1;
    private var num_keys: Long = -1l;
    private var system: KompicsSystem = null;
    private var atomicRegister: UUID = null;
    private var beb: UUID = null;
    private var iterationComp: UUID = null;
    private var prepare_latch: CountDownLatch = null;
    private var finished_latch: CountDownLatch = null;
    private var init_id: Int = -1;

    override def setup(c: MasterConf): ClientConf = {
      system = KompicsSystemProvider.newRemoteKompicsSystem(1);
      AtomicRegisterSerializer.register();
      IterationCompSerializer.register();
      this.read_workload = c.readWorkload;
      this.write_workload = c.writeWorkload;
      this.partition_size = c.partitionSize;
      this.num_keys = c.numberOfKeys;
      ClientParams(read_workload, write_workload)
    };
    override def prepareIteration(d: List[ClientData]): Unit = {
      assert(system != null);
      val addr = system.networkAddress.get;
      println(s"Atomic Register(Master) Path is $addr");
      val nodes = addr :: d
      val num_nodes = nodes.size
      assert(partition_size <= num_nodes && partition_size > 0 && read_workload + write_workload == 1)
      /*if (num_nodes < partition_size || partition_size == 0 || num_nodes % partition_size != 0) {
        throw new FailedPreparationException(s"Bad partition arguments: N=$num_nodes, partition size=$partition_size")
      }
      if (read_workload + write_workload != 1) throw new FailedPreparationException(s"Sum of workload must be 1.0: read=$read_workload, write=$write_workload")
      */
      val atomicRegisterIdF = system.createNotify[AtomicRegisterComp](Init(read_workload, write_workload)) // TODO parallelism
      atomicRegister = Await.result(atomicRegisterIdF, 5.second)
      /* connect network */
      val connF = system.connectNetwork(atomicRegister);
      Await.result(connF, 5.seconds);
      /* connect best effort broadcast */
      val bebF = system.createNotify[BEBComp](Init(addr))
      beb = Await.result(bebF, 5.second)
      val beb_net_connF = system.connectNetwork(beb)
      Await.result(beb_net_connF, 5.second)
      val beb_ar_connF = system.connectComponents[BestEffortBroadcast](atomicRegister, beb)
      Await.result(beb_ar_connF, 5.seconds)
      /* connect Iteration prepare component */
      init_id += 1
      prepare_latch = new CountDownLatch(1)
      finished_latch = new CountDownLatch(1)
      val iterationCompF = system.createNotify[IterationComp](Init(prepare_latch, finished_latch, init_id, nodes, num_keys, partition_size)) // only wait for INIT_ACK from clients
      iterationComp = Await.result(iterationCompF, 5.second)
      val iterationComp_net_connF = system.connectNetwork(iterationComp)
      Await.result(iterationComp_net_connF, 5.seconds)
      assert(beb != null && iterationComp != null && atomicRegister != null);
      system.startNotify(beb)
      system.startNotify(atomicRegister)
      system.startNotify(iterationComp)
      val timeout = 100
      val timeunit = TimeUnit.SECONDS
      val successful_prep = prepare_latch.await(timeout, timeunit)
      if (!successful_prep) {
        println("Timeout on INIT_ACK in prepareIteration")
        throw new FailedPreparationException("Timeout waiting for INIT ACK from all nodes")
      }
    }
    override def runIteration(): Unit = {
      val timeout = 5
      val timeunit = TimeUnit.MINUTES
      system.triggerComponent(RUN, iterationComp)
      val successful_run = finished_latch.await(timeout, timeunit)
      //      finished_latch.await()
      if (!successful_run) println(s"Timeout in runIteration: num_keys=$num_keys, read=$read_workload, write=$write_workload (timeout=$timeout $timeunit)")
    };
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      println("Cleaning up Atomic Register(Master) side");
      assert(system != null);
      if (prepare_latch != null) prepare_latch = null
      if (finished_latch != null) finished_latch = null
      if (atomicRegister != null) {
        val killF = system.killNotify(atomicRegister)
        Await.ready(killF, 5.seconds)
        val killBebF = system.killNotify(beb)
        Await.ready(killBebF, 5.seconds)
        val killiterationCompF = system.killNotify(iterationComp)
        Await.ready(killiterationCompF, 5.seconds)
        atomicRegister = null
        beb = null
        iterationComp = null
      }
      if (lastIteration) {
        println("Cleaning up Last iteration")
        val f = system.terminate();
        system = null;
      }
    }
  }

  class ClientImpl extends Client {
    private var system: KompicsSystem = null;
    private var atomicRegister: UUID = null;
    private var beb: UUID = null
    private var read_workload = 0.0F
    private var write_workload = 0.0F

    override def setup(c: ClientConf): ClientData = {
      system = KompicsSystemProvider.newRemoteKompicsSystem(1);
      AtomicRegisterSerializer.register();
      IterationCompSerializer.register();
      val addr = system.networkAddress.get;
      println(s"Atomic Register(Client) Path is $addr");
      this.read_workload = c.read_workload;
      this.write_workload = c.write_workload;
      val atomicRegisterF = system.createNotify[AtomicRegisterComp](Init(read_workload, write_workload))
      atomicRegister = Await.result(atomicRegisterF, 5.seconds)
      /* connect network */
      val connF = system.connectNetwork(atomicRegister);
      Await.result(connF, 5.seconds);
      /* connect best effort broadcast */
      val bebF = system.createNotify[BEBComp](Init(addr))
      beb = Await.result(bebF, 5.second)
      val beb_net_connF = system.connectNetwork(beb)
      Await.result(beb_net_connF, 5.second)
      val beb_ar_connF = system.connectComponents[BestEffortBroadcast](atomicRegister, beb)
      Await.result(beb_ar_connF, 5.seconds);
      system.startNotify(beb)
      system.startNotify(atomicRegister)
      addr
    }
    override def prepareIteration(): Unit = {
      // nothing
      println("Preparing Atomic Register(Client) iteration");
      assert(system != null);
    }
    override def cleanupIteration(lastIteration: Boolean): Unit = {
      println("Cleaning up Atomic Register(Client) side");
      assert(system != null);
      if (lastIteration) {
        println("Cleaning up Last iteration")
        atomicRegister = null; // will be stopped when system is shut down
        beb = null
        system.terminate();
        system = null;
      }
    }
  }

  override def newMaster(): kompicsscala.bench.AtomicRegister.Master = new MasterImpl();

  override def msgToMasterConf(msg: GeneratedMessage): Try[MasterConf] = Try {
    msg.asInstanceOf[AtomicRegisterRequest]
  };

  override def newClient(): kompicsscala.bench.AtomicRegister.Client = new ClientImpl();

  override def strToClientConf(str: String): Try[ClientConf] = Try {
    val split = str.split(":");
    assert(split.length == 2);
    ClientParams(split(0).toFloat, split(1).toFloat)
  };

  override def strToClientData(str: String): Try[ClientData] = Try {
    val split = str.split(":");
    assert(split.length == 2);
    val ipStr = split(0); //.replaceAll("""/""", "");
    val portStr = split(1);
    val port = portStr.toInt;
    NetAddress.from(ipStr, port)
  }.flatten;

  override def clientConfToString(c: ClientConf): String = s"${c.read_workload}:${c.write_workload}";

  override def clientDataToString(d: ClientData): String = {
    s"${d.isa.getHostString()}:${d.getPort()}"
  }

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

  class AtomicRegisterComp(init: Init[AtomicRegisterComp]) extends ComponentDefinition {
    implicit def addComparators[A](x: A)(implicit o: math.Ordering[A]): o.Ops = o.mkOrderingOps(x); // for tuple comparison

    val net = requires[Network]
    val beb = requires[BestEffortBroadcast]

    val Init(read_workload: Float, write_workload: Float) = init
    var nodes: List[NetAddress] = _
    var n = 0
    val selfAddr = cfg.getValue[NetAddress](KompicsSystemProvider.SELF_ADDR_KEY)
    var selfRank: Int = -1

    var min_key: Long = -1
    var max_key: Long = -1
    var register_state: mutable.Map[Long, AtomicRegisterState] = mutable.Map.empty // (key, state)
    // keep readlist in separate map as it's used not as often. readlist values=(ts, processID, value)
    var register_readlist: mutable.Map[Long, mutable.Map[NetAddress, (Int, Int, Int)]] = mutable.Map.empty

    /* Experiment variables */
    var read_count: Long = 0
    var write_count: Long = 0
    var master: NetAddress = _
    var current_run_id: Int = -1

    private def newIteration(i: INIT): Unit = {
      current_run_id = i.init_id
      nodes = i.nodes
      n = nodes.size
      selfRank = i.rank
      min_key = i.min
      max_key = i.max
      /* Reset KV and states */
      register_state.clear()
      register_readlist.clear()
      for (i <- min_key to max_key) {
        register_state += (i -> new AtomicRegisterState)
        register_readlist += (i -> mutable.Map.empty[NetAddress, (Int, Int, Int)])
      }
    }

    private def invokeRead(key: Long): Unit = {
      val register = register_state(key)
      register.rid += 1;
      register.acks = 0
      register_readlist(key).clear()
      register.reading = true
      //      logger.info(s"Invoking READ key=$key")
      trigger(BEBRequest(nodes, READ(current_run_id, key, register.rid)) -> beb)
    }

    private def invokeWrite(key: Long): Unit = {
      val wval = selfRank
      val register = register_state(key)
      register.rid += 1
      register.writeval = wval
      register.acks = 0
      register.reading = false
      register_readlist(key).clear()
      //      logger.info(s"Invoking WRITE key=$key")
      trigger(BEBRequest(nodes, READ(current_run_id, key, register.rid)) -> beb)
    }

    private def invokeOperations(): Unit = {
      val num_keys = max_key - min_key + 1
      val num_reads = (num_keys * read_workload).toLong
      val num_writes = (num_keys * write_workload).toLong

      logger.info(s"Invoking operations: $num_reads reads and $num_writes writes. Keys: $min_key - $max_key. n=$n")
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
      logger.info(s"Read response key=$key read_count=$read_count")
      if (read_count == 0 && write_count == 0) runFinished()
    }

    private def writeResponse(key: Long): Unit = {
      write_count -= 1
      logger.info(s"Write response key=$key, write_count=$write_count")
      if (read_count == 0 && write_count == 0) runFinished()
    }

    private def runFinished(): Unit = {
      logger.info(s"Atomic register $selfAddr is done!")
      trigger(NetMessage.viaTCP(selfAddr, master)(DONE) -> net)
    }

    ctrl uponEvent {
      case _: Start => handle {
        assert(selfAddr != null)
        //        logger.info(s"Atomic Register $selfAddr component has started ")
      }
    }

    beb uponEvent {
      case BEBDeliver(READ(current_run_id, key, readId), src) => handle {
        //        logger.info(s"Responding to READ key=$key from $src")
        val current_state: AtomicRegisterState = register_state(key)
        trigger(NetMessage.viaTCP(selfAddr, src)(VALUE(current_run_id, key, readId, current_state.ts, current_state.wr, current_state.value)) -> net)
      };

      case BEBDeliver(w: WRITE, src) => handle {
        if (w.run_id == current_run_id) {
          //          logger.info(s"Responding to WRITE key=${w.key} from $src")
          val current_state = register_state(w.key)
          if ((w.ts, w.wr) > (current_state.ts, current_state.wr)) {
            current_state.ts = w.ts
            current_state.wr = w.wr
            current_state.value = w.value
          }
        }
        trigger(NetMessage.viaTCP(selfAddr, src)(ACK(w.run_id, w.key, w.rid)) -> net)
      };
    }

    net uponEvent {
      case context @ NetMessage(header, i: INIT) => handle{
        newIteration(i)
        master = header.getSource()
        trigger(context.reply(selfAddr)(INIT_ACK(i.init_id)) -> net)
      }

      case NetMessage(header, v: VALUE) => handle {
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
            val src = header.getSource()
            readlist(src) = (v.ts, v.wr, v.value)
            //            logger.info("Got VALUE key=" + v.key + " from " + src + "readlist size=" + readlist.size)
            if (readlist.size > n / 2) {
              if (current_register.reading && current_register.skip_impose) {
                //                logger.info(s"Skipped impose: key=${v.key}, ts=${v.ts}")
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
                //                logger.info("Sending WRITE key=" + v.key)
                trigger(BEBRequest(nodes, WRITE(v.run_id, v.key, v.rid, maxts, rr, bcastvalue)) -> beb)
              }
            }
          }
        }
      }

      case NetMessage(header, a: ACK) => handle {
        if (a.run_id == current_run_id) {
          val current_register = register_state(a.key)
          if (a.rid == current_register.rid) {
            current_register.acks += 1
            //            logger.info("Got ack key=" + a.key + " from " + header.getSource() + ", count=" + current_register.acks)
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

      case NetMessage(_, RUN) => handle{
        invokeOperations()
      }
    }
  }

  case object DONE extends KompicsEvent;
  case class READ(run_id: Int, key: Long, rid: Int) extends KompicsEvent;
  case class ACK(run_id: Int, key: Long, rid: Int) extends KompicsEvent;
  case class VALUE(run_id: Int, key: Long, rid: Int, ts: Int, wr: Int, value: Int) extends KompicsEvent;
  case class WRITE(run_id: Int, key: Long, rid: Int, ts: Int, wr: Int, value: Int) extends KompicsEvent;

  object AtomicRegisterSerializer extends Serializer {
    private val READ_FLAG: Byte = 1
    private val WRITE_FLAG: Byte = 2
    private val ACK_FLAG: Byte = 3
    private val VALUE_FLAG: Byte = 4
    private val DONE_FLAG: Byte = 5

    override def identifier(): Int = se.kth.benchmarks.kompics.SerializerIds.S_ATOMIC_REG

    def register(): Unit = {
      Serializers.register(this, "atomicregister");
      Serializers.register(classOf[READ], "atomicregister");
      Serializers.register(classOf[ACK], "atomicregister");
      Serializers.register(classOf[WRITE], "atomicregister");
      Serializers.register(classOf[VALUE], "atomicregister");
      Serializers.register(DONE.getClass, "atomicregister")
    }

    override def toBinary(o: Any, buf: ByteBuf): Unit = {
      o match {
        case DONE => {
          buf.writeByte(DONE_FLAG)
        }
        case r: READ => {
          buf.writeByte(READ_FLAG)
          buf.writeInt(r.run_id)
          buf.writeLong(r.key)
          buf.writeInt(r.rid)
        }
        case w: WRITE => {
          buf.writeByte(WRITE_FLAG)
          buf.writeInt(w.run_id)
          buf.writeLong(w.key)
          buf.writeInt(w.rid)
          buf.writeInt(w.ts)
          buf.writeInt(w.wr)
          buf.writeInt(w.value)
        }
        case a: ACK => {
          buf.writeByte(ACK_FLAG)
          buf.writeInt(a.run_id)
          buf.writeLong(a.key)
          buf.writeInt(a.rid)
        }
        case v: VALUE => {
          buf.writeByte(VALUE_FLAG)
          buf.writeInt(v.run_id)
          buf.writeLong(v.key)
          buf.writeInt(v.rid)
          buf.writeInt(v.ts)
          buf.writeInt(v.wr)
          buf.writeInt(v.value)
        }
      }
    }

    override def fromBinary(buf: ByteBuf, optional: Optional[AnyRef]): AnyRef = {
      val flag = buf.readByte()
      flag match {
        case DONE_FLAG => DONE
        case READ_FLAG => {
          val run_id = buf.readInt()
          val key = buf.readLong()
          val rid = buf.readInt()
          READ(run_id, key, rid)
        }
        case ACK_FLAG => {
          val run_id = buf.readInt()
          val key = buf.readLong()
          val rid = buf.readInt()
          ACK(run_id, key, rid)
        }
        case WRITE_FLAG => {
          val run_id = buf.readInt()
          val key = buf.readLong()
          val rid = buf.readInt()
          val ts = buf.readInt()
          val wr = buf.readInt()
          val value = buf.readInt()
          WRITE(run_id, key, rid, ts, wr, value)
        }
        case VALUE_FLAG => {
          val run_id = buf.readInt()
          val key = buf.readLong()
          val rid = buf.readInt()
          val ts = buf.readInt()
          val wr = buf.readInt()
          val value = buf.readInt()
          VALUE(run_id, key, rid, ts, wr, value)
        }
        case _ => {
          Console.err.print(s"Got invalid ser flag: $flag");
          null
        }
      }

    }
  }
}

