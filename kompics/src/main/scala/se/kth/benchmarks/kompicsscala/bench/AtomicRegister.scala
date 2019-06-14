package se.kth.benchmarks.kompicsscala.bench

import java.net.{ InetAddress, InetSocketAddress }
import java.util.{ Optional, UUID }
import java.util.concurrent.CountDownLatch

import io.netty.buffer.ByteBuf
import kompics.benchmarks.benchmarks.AtomicRegisterRequest
import scalapb.GeneratedMessage
import se.kth.benchmarks.{ DistributedBenchmark, kompicsscala }

import scala.concurrent.duration._
import se.kth.benchmarks.kompicsscala._
import se.sics.kompics.network.Network
import se.sics.kompics.network.netty.serialization.{ Serializer, Serializers }
import se.sics.kompics.sl._
import se.sics.kompics.timer.{ Timer, SchedulePeriodicTimeout, Timeout, CancelPeriodicTimeout }
import se.sics.kompics.timer.java.JavaTimer

import scala.collection.mutable
import scala.concurrent.Await
import scala.util.{ Success, Try }

object AtomicRegister extends DistributedBenchmark {
  override type MasterConf = AtomicRegisterRequest
  override type ClientConf = AtomicRegisterRequest
  override type ClientData = NetAddress

  class MasterImpl extends Master {
    private var num_reads = -1l;
    private var num_writes = -1l;
    private var system: KompicsSystem = null;
    private var atomicRegister: UUID = null;
    private var beb: UUID = null;
    private var timer: UUID = null;
    private var latch: CountDownLatch = null;

    override def setup(c: MasterConf): ClientConf = {
      system = KompicsSystemProvider.newRemoteKompicsSystem(1);
      AtomicRegisterSerializer.register()
      this.num_reads = c.numberOfReads;
      this.num_writes = c.numberOfWrites;
      println(s"Atomic Register(Master) setup: numReads=$num_reads numWrites=$num_writes")
      c
    };
    override def prepareIteration(d: List[ClientData]): Unit = {
      println("Atomic Register(Master) prepare iteration!")
      assert(system != null);
      latch = new CountDownLatch(1);
      val addr = system.networkAddress.get;
      println(s"Atomic Register(Master) Path is $addr");
      var nodes = d.toSet
      nodes += addr
      val atomicRegisterIdF = system.createNotify[AtomicRegisterComp](Init(latch, Some(nodes), num_reads, num_writes))
      atomicRegister = Await.result(atomicRegisterIdF, 5.second)
      /* connect network */
      val connF = system.connectNetwork(atomicRegister);
      Await.result(connF, 5.seconds);
      /* connect best effort broadcast */
      val bebF = system.createNotify[BEBComp](Init(addr)) // TODO: use config addr instead, same
      beb = Await.result(bebF, 5.second)
      val beb_netw_connF = system.connectNetwork(beb)
      Await.result(beb_netw_connF, 5.second)
      val beb_ar_connF = system.connectComponents[BestEffortBroadcast](atomicRegister, beb)
      Await.result(beb_ar_connF, 5.seconds);

      val timerF = system.createNotify[JavaTimer](Init.none[JavaTimer])
      timer = Await.result(timerF, 5.second)
      val timer_connF = system.connectComponents[Timer](atomicRegister, timer)
      Await.result(timer_connF, 5.seconds);

    }
    override def runIteration(): Unit = {
      assert(system != null);
      assert(atomicRegister != null);
      assert(beb != null)
      println("Atomic Register(Master) run iteration!")
      system.startNotify(timer)
      system.startNotify(beb)
      val startF = system.startNotify(atomicRegister);
      //startF.failed.foreach(e => eprintln(s"Could not start pinger: $e"));
      latch.await();
    };
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      println("Cleaning up Atomic Register(Master) side");
      assert(system != null);
      if (latch != null) {
        latch = null;
      }
      if (atomicRegister != null) {
        val killF = system.killNotify(atomicRegister)
        Await.ready(killF, 5.seconds)
        val killBebF = system.killNotify(beb)
        Await.ready(killBebF, 5.seconds)
        val killTimerF = system.killNotify(timer)
        Await.ready(killTimerF, 5.seconds)
        atomicRegister = null
        beb = null
        timer = null
      }
      if (lastIteration) {
        val f = system.terminate();
        system = null;
      }
    }
  }

  class ClientImpl extends Client {
    private var system: KompicsSystem = null;
    private var atomicRegister: UUID = null;
    private var beb: UUID = null
    private var num_reads = -1l
    private var num_writes = -1l
    private var latch: CountDownLatch = null

    override def setup(c: ClientConf): ClientData = {
      system = KompicsSystemProvider.newRemoteKompicsSystem(1);
      latch = new CountDownLatch(1);
      AtomicRegisterSerializer.register();
      val addr = system.networkAddress.get;
      println(s"Atomic Register(Client) Path is $addr");
      this.num_reads = c.numberOfReads;
      this.num_writes = c.numberOfWrites;
      val atomicRegisterF = system.createNotify[AtomicRegisterComp](Init(latch, None, num_reads, num_writes))
      atomicRegister = Await.result(atomicRegisterF, 5.seconds)
      /* connect network */
      val connF = system.connectNetwork(atomicRegister);
      Await.result(connF, 5.seconds);
      /* connect best effort broadcast */
      val bebF = system.createNotify[BEBComp](Init(addr)) // TODO: is addr correct?
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
    AtomicRegisterRequest({ split(0).toInt }, { split(1).toInt })
    //    AtomicRegisterRequest().withNumberOfReads(split(0).toInt).withNumberOfWrites(split(1).toInt)
  };

  override def strToClientData(str: String): Try[ClientData] = Try {
    val split = str.split(":");
    assert(split.length == 2);
    val ipStr = split(0); //.replaceAll("""/""", "");
    val portStr = split(1);
    val port = portStr.toInt;
    NetAddress.from(ipStr, port)
  }.flatten;

  override def clientConfToString(c: ClientConf): String = s"${c.numberOfReads}:${c.numberOfWrites}";

  override def clientDataToString(d: ClientData): String = {
    s"${d.isa.getHostString()}:${d.getPort()}"
  }
  class AtomicRegisterComp(init: Init[AtomicRegisterComp]) extends ComponentDefinition {
    implicit def addComparators[A](x: A)(implicit o: math.Ordering[A]): o.Ops = o.mkOrderingOps(x); // for tuple comparison

    val net = requires[Network]
    val beb = requires[BestEffortBroadcast]
    val timer = requires[Timer]

    val Init(latch: CountDownLatch, option_nodes: Option[Set[NetAddress]], num_read: Long, num_write: Long) = init
    var nodes = option_nodes.getOrElse(Set[NetAddress]()) // master will receive the set, clients have to wait for master msg
    var n = nodes.size
    val selfAddr = cfg.getValue[NetAddress](KompicsSystemProvider.SELF_ADDR_KEY)
    var selfRank: Int = selfAddr.hashCode()
    var (ts, wr) = (0, 0)
    var value: Option[Int] = None
    var acks = 0
    var readval: Option[Int] = None
    var writeval: Option[Int] = None
    var rid = 0
    var readlist: mutable.Map[NetAddress, (Int, Int, Option[Int])] = mutable.Map.empty // (ts, processID, value)
    var reading = false

    /* Experiment variables */
    var read_count = num_read
    var write_count = num_write
    var init_ack_count = n
    var timerId: Option[UUID] = None

    object AtomicRegisterComp { // TODO: put in config file?
      val INIT_ACK = -1
    }

    def invokeRead(): Unit = {
      rid = rid + 1;
      acks = 0
      readlist = mutable.Map.empty
      reading = true
      trigger(BEBRequest(nodes, READ(rid)) -> beb)
    }

    def invokeWrite(wval: Int): Unit = {
      rid = rid + 1
      writeval = Some(wval)
      acks = 0
      readlist = mutable.Map.empty
      trigger(BEBRequest(nodes, READ(rid)) -> beb)
    }

    private def responseRead(read_value: Option[Int]): Unit = {
      read_count -= 1
      if (read_count == 0 && write_count == 0) latch.countDown()
      else invokeWrite(write_count.toInt)
    }

    private def responseWrite(): Unit = {
      write_count -= 1
      if (read_count == 0 && write_count == 0) latch.countDown()
      else invokeRead()
    }

    ctrl uponEvent {
      case _: Start => handle {
        assert(selfAddr != null)
        logger.info(s"Atomic Register Component $selfAddr has started!")
        if (n > 0) {
          logger.info("Sending init to: ")
          for (node <- nodes) println(node)
          init_ack_count = n
          val spt = new SchedulePeriodicTimeout(5, 20000) // todo: second argument(period)
          val timeout = InitTimeout(spt)
          spt.setTimeoutEvent(timeout)
          trigger(spt -> timer)
          timerId = Some(timeout.getTimeoutId)
          trigger(BEBRequest(nodes, INIT(nodes)) -> beb)
        }
      }
    }

    timer uponEvent {
      case InitTimeout(_) => handle {
        logger.error("Time out waiting for INIT ACKS")
        latch.countDown()
      }
    }

    beb uponEvent {
      case BEBDeliver(INIT(received_nodes: Set[NetAddress]), src) => handle {
        logger.info("Got INIT! Sending ack")
        nodes = received_nodes
        n = nodes.size
        trigger(NetMessage.viaTCP(selfAddr, src)(ACK(AtomicRegisterComp.INIT_ACK)) -> net)
        // TODO: should client invoke reads or writes as well?
      }

      case BEBDeliver(READ(readID), src) => handle {
        trigger(NetMessage.viaTCP(selfAddr, src)(VALUE(readID, ts, wr, value)) -> net)
      };

      case BEBDeliver(w: WRITE, src) => handle {
        val rid_received = w.rid
        val ts_received = w.ts
        val wr_received = w.wr
        val writeVal_received = w.value
        if ((ts_received, wr_received) > (ts, wr)) {
          ts = ts_received
          wr = wr_received
          value = writeVal_received
        }
        trigger(NetMessage.viaTCP(selfAddr, src)(ACK(rid_received)) -> net)
      };
    }

    net uponEvent {
      case NetMessage(header, ACK(AtomicRegisterComp.INIT_ACK)) => handle{
        val src = header.getSource()
        logger.debug(s"Got init ack from $src")
        init_ack_count -= 1
        if (init_ack_count == 0) {
          trigger(new CancelPeriodicTimeout(timerId.get) -> timer)
          logger.info("Got init ack from everybody! Starting experiment")
          invokeWrite(write_count.toInt)
        }
      }

      case NetMessage(header, v: VALUE) => handle {
        val src = header.getSource()
        if (v.rid == rid) {
          readlist(src) = (v.ts, v.wr, v.value)
          if (readlist.size > n / 2) {
            var (maxts, rr, readvalue1) = readlist.values.maxBy(_._1)
            readval = readvalue1
            readlist = mutable.Map.empty
            var bcastvalue: Option[Int] = None
            if (reading) {
              bcastvalue = readval
            } else {
              rr = selfRank
              maxts += 1
              bcastvalue = writeval
            }
            trigger(BEBRequest(nodes, WRITE(rid, maxts, rr, bcastvalue)) -> beb)
          }
        }
      }
      case NetMessage(_, a: ACK) => handle {
        if (a.rid == rid) {
          acks = acks + 1
          if (acks > n / 2) {
            acks = 0
            if (reading) {
              reading = false
              responseRead(readval)
            } else {
              responseWrite()
            }
          }
        }
      }
    }
  }

  case class InitTimeout(spt: SchedulePeriodicTimeout) extends Timeout(spt);
  case class INIT(nodes: Set[NetAddress]) extends KompicsEvent;
  case class READ(rid: Int) extends KompicsEvent;
  case class ACK(rid: Int) extends KompicsEvent;
  case class VALUE(rid: Int, ts: Int, wr: Int, value: Option[Int]) extends KompicsEvent;
  case class WRITE(rid: Int, ts: Int, wr: Int, value: Option[Int]) extends KompicsEvent;

  object AtomicRegisterSerializer extends Serializer {
    private val READ_FLAG: Byte = 1
    private val WRITE_FLAG: Byte = 2
    private val ACK_FLAG: Byte = 3
    private val VALUE_FLAG: Byte = 4
    private val INIT_FLAG: Byte = 5
    private val NONE_FLAG: Byte = -1

    override def identifier(): Int = se.kth.benchmarks.kompics.SerializerIds.S_ATOMIC_REG

    def register(): Unit = {
      /* weird work around for using serializers with case class */
      val r = READ(0)
      val a = ACK(0)
      val w = WRITE(0, 0, 0, Some(0))
      val v = VALUE(0, 0, 0, Some(0))
      val i = INIT(Set[NetAddress]())

      Serializers.register(this, "atomicregister");
      Serializers.register(i.getClass, "atomicregister");
      Serializers.register(r.getClass, "atomicregister");
      Serializers.register(a.getClass, "atomicregister");
      Serializers.register(w.getClass, "atomicregister");
      Serializers.register(v.getClass, "atomicregister");
    }

    override def toBinary(o: Any, buf: ByteBuf): Unit = {
      o match {
        case i: INIT => {
          buf.writeByte(INIT_FLAG)
          buf.writeInt(i.nodes.size)
          for (node <- i.nodes) {
            val ip = node.getIp().getAddress
            assert(ip.length == 4)
            buf.writeBytes(ip);
            buf.writeShort(node.getPort());
          }
        }
        case r: READ => {
          buf.writeByte(READ_FLAG)
          buf.writeInt(r.rid)
        }
        case w: WRITE => {
          buf.writeByte(WRITE_FLAG)
          buf.writeInt(w.rid)
          buf.writeInt(w.ts)
          buf.writeInt(w.wr)
          buf.writeInt(w.value.get)
        }
        case a: ACK => {
          buf.writeByte(ACK_FLAG)
          buf.writeInt(a.rid)
        }
        case v: VALUE => {
          buf.writeByte(VALUE_FLAG)
          buf.writeInt(v.rid)
          buf.writeInt(v.ts)
          buf.writeInt(v.wr)
          buf.writeInt(v.value.getOrElse(NONE_FLAG))
        }
      }
    }

    override def fromBinary(buf: ByteBuf, optional: Optional[AnyRef]): AnyRef = {
      val flag = buf.readByte()
      flag match {
        case READ_FLAG => READ(buf.readInt())
        case ACK_FLAG  => ACK(buf.readInt())
        case WRITE_FLAG => {
          val rid = buf.readInt
          val ts = buf.readInt
          val wr = buf.readInt
          val value = buf.readInt
          WRITE(rid, ts, wr, Option(value))
        }
        case VALUE_FLAG => {
          val rid = buf.readInt
          val ts = buf.readInt
          val wr = buf.readInt
          var value = Option(buf.readInt)
          if (value.get == NONE_FLAG) value = None;
          VALUE(rid, ts, wr, value)
        }
        case INIT_FLAG => {
          val n = buf.readInt()
          var nodes = Set[NetAddress]()
          for (_ <- 0 until n) {
            val ipBytes = new Array[Byte](4)
            buf.readBytes(ipBytes)
            val addr = InetAddress.getByAddress(ipBytes)
            val port: Int = buf.readUnsignedShort()
            nodes += NetAddress(new InetSocketAddress(addr, port))
          }
          INIT(nodes)
        }
        case _ => {
          Console.err.print(s"Got invalid ser flag: $flag");
          null
        }
      }

    }
  }
}

