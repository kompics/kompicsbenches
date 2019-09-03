package se.kth.benchmarks.kompicsscala.bench

import se.kth.benchmarks._
import se.kth.benchmarks.kompicsscala._
import _root_.kompics.benchmarks.benchmarks.ThroughputPingPongRequest
import se.sics.kompics.network.Network
import se.sics.kompics.sl._
import scala.util.{Failure, Success, Try}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import java.util.concurrent.CountDownLatch
import java.util.UUID
import se.sics.kompics.network.netty.serialization.{Serializer, Serializers}
import java.util.Optional
import io.netty.buffer.ByteBuf

object NetThroughputPingPong extends DistributedBenchmark {

  implicit val ec = scala.concurrent.ExecutionContext.global;

  case class ClientParams(numPongers: Int, staticOnly: Boolean)

  override type MasterConf = ThroughputPingPongRequest;
  override type ClientConf = ClientParams;
  override type ClientData = NetAddress;

  class MasterImpl extends Master {
    private var numMsgs = -1L;
    private var numPairs = -1;
    private var pipeline = -1L;
    private var staticOnly = true;
    private var system: KompicsSystem = null;
    private var pingers: List[UUID] = List.empty;
    private var latch: CountDownLatch = null;

    override def setup(c: MasterConf): ClientConf = {
      this.numMsgs = c.messagesPerPair;
      this.numPairs = c.parallelism;
      this.pipeline = c.pipelineSize;
      this.staticOnly = c.staticOnly;
      system = KompicsSystemProvider.newRemoteKompicsSystem(Runtime.getRuntime.availableProcessors());
      NetPingPongSerializer.register();
      ClientParams(numPairs, staticOnly)
    };
    override def prepareIteration(d: List[ClientData]): Unit = {
      val ponger = d.head;
      println(s"Resolved path to ${ponger}");
      latch = new CountDownLatch(numPairs);
      val pingersLF = (1 to numPairs).map { index =>
        for {
          pingerId <- if (staticOnly) {
            system.createNotify[StaticPinger](Init[StaticPinger](index, latch, numMsgs, pipeline, ponger))
          } else {
            system.createNotify[Pinger](Init[Pinger](index, latch, numMsgs, pipeline, ponger))
          };
          _ <- system.connectNetwork(pingerId)
        } yield {
          pingerId
        }
      }.toList;
      val pingersFL = Future.sequence(pingersLF);
      pingers = Await.result(pingersFL, 5.seconds);
    }
    override def runIteration(): Unit = {
      val startLF = pingers.map(pinger => system.startNotify(pinger));
      val startFL = Future.sequence(startLF);
      Await.result(startFL, Duration.Inf);
      latch.await();
    };
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      println("Cleaning up pinger side");
      assert(system != null);
      if (latch != null) {
        latch = null;
      }
      val killPingersLF = pingers.map(pinger => system.killNotify(pinger));
      val killPingersFL = Future.sequence(killPingersLF);
      pingers = List.empty;
      Await.result(killPingersFL, Duration.Inf);
      if (lastIteration) {
        val f = system.terminate();
        system = null;
      }
    }
  }

  class ClientImpl extends Client {
    private var system: KompicsSystem = null;
    private var pongers: List[UUID] = null;

    override def setup(c: ClientConf): ClientData = {
      system = KompicsSystemProvider.newRemoteKompicsSystem(1);
      NetPingPongSerializer.register();

      val lf = (0 to c.numPongers).map { index =>
        for {
          pongerId <- if (c.staticOnly) {
            system.createNotify[StaticPonger](Init[StaticPonger](index))
          } else {
            system.createNotify[Ponger](Init[Ponger](index))
          };
          _ <- system.connectNetwork(pongerId);
          _ <- system.startNotify(pongerId)
        } yield {
          pongerId
        }
      }.toList;
      val fl = Future.sequence(lf);
      val l = Await.result(fl, Duration.Inf);
      system.networkAddress.get
    }
    override def prepareIteration(): Unit = {
      // nothing
      println("Preparing ponger iteration");
      assert(system != null);
    }
    override def cleanupIteration(lastIteration: Boolean): Unit = {
      println("Cleaning up ponger side");
      assert(system != null);
      if (lastIteration) {
        pongers = List.empty; // will be stopped when system is shut down
        system.terminate();
        system = null;
      }
    }
  }

  override def newMaster(): Master = new MasterImpl();
  override def msgToMasterConf(msg: scalapb.GeneratedMessage): Try[MasterConf] = Try {
    msg.asInstanceOf[ThroughputPingPongRequest]
  };

  override def newClient(): Client = new ClientImpl();
  override def strToClientConf(str: String): Try[ClientConf] = Try {
    val split = str.split(",");
    val num = split(0).toInt;
    val staticOnly = split(1) match {
      case "true"  => true
      case "false" => false
    };
    ClientParams(num, staticOnly)
  };
  override def strToClientData(str: String): Try[ClientData] = NetAddress.fromString(str);

  override def clientConfToString(c: ClientConf): String = s"${c.numPongers},${c.staticOnly}";
  override def clientDataToString(d: ClientData): String = d.asString;

  case class StaticPing(id: Int) extends KompicsEvent;
  case class StaticPong(id: Int) extends KompicsEvent;
  case class Ping(index: Long, id: Int) extends KompicsEvent;
  case class Pong(index: Long, id: Int) extends KompicsEvent;

  object NetPingPongSerializer extends Serializer {

    val NAME: String = "tpnetpingpong";

    def register(): Unit = {
      Serializers.register(this, NAME);
      Serializers.register(classOf[StaticPing], NAME);
      Serializers.register(classOf[StaticPong], NAME);
      Serializers.register(classOf[Ping], NAME);
      Serializers.register(classOf[Pong], NAME);
    }

    val NO_HINT: Optional[Object] = Optional.empty();

    private val STATIC_PING_FLAG: Byte = 1;
    private val STATIC_PONG_FLAG: Byte = 2;
    private val PING_FLAG: Byte = 3;
    private val PONG_FLAG: Byte = 4;

    override def identifier(): Int = se.kth.benchmarks.kompics.SerializerIds.S_NETTPPP;

    override def toBinary(o: Any, buf: ByteBuf): Unit = {
      o match {
        case StaticPing(id)  => buf.writeByte(STATIC_PING_FLAG).writeInt(id)
        case StaticPong(id)  => buf.writeByte(STATIC_PONG_FLAG).writeInt(id)
        case Ping(index, id) => buf.writeByte(PING_FLAG).writeLong(index).writeInt(id)
        case Pong(index, id) => buf.writeByte(PONG_FLAG).writeLong(index).writeInt(id)
      }
    }

    override def fromBinary(buf: ByteBuf, hint: Optional[Object]): Object = {
      val flag = buf.readByte();
      flag match {
        case STATIC_PING_FLAG => {
          val id = buf.readInt();
          StaticPing(id)
        }
        case STATIC_PONG_FLAG => {
          val id = buf.readInt();
          StaticPong(id)
        }
        case PING_FLAG => {
          val index = buf.readLong();
          val id = buf.readInt();
          Ping(index, id)
        }
        case PONG_FLAG => {
          val index = buf.readLong();
          val id = buf.readInt();
          Pong(index, id)
        }
        case _ => {
          Console.err.print(s"Got invalid ser flag: $flag");
          null
        }
      }
    }
  }

  class StaticPinger(init: Init[StaticPinger]) extends ComponentDefinition {

    val Init(selfId: Int, latch: CountDownLatch, count: Long, pipeline: Long, ponger: NetAddress) = init;

    val net = requires[Network];

    lazy val selfAddr = cfg.getValue[NetAddress](KompicsSystemProvider.SELF_ADDR_KEY);

    private var sentCount = 0L;
    private var recvCount = 0L;

    ctrl uponEvent {
      case _: Start =>
        handle {
          assert(selfAddr != null);
          var pipelined = 0L;
          while (pipelined < pipeline && sentCount < count) {
            trigger(NetMessage.viaTCP(selfAddr, ponger)(StaticPing(selfId)) -> net);
            pipelined += 1L;
            sentCount += 1L;
          }
        }
    }

    net uponEvent {
      case context @ NetMessage(_, StaticPong(this.selfId)) =>
        handle {
          recvCount += 1L;
          if (recvCount < count) {
            if (sentCount < count) {
              trigger(NetMessage.viaTCP(selfAddr, ponger)(StaticPing(selfId)) -> net);
              sentCount += 1L;
            }
          } else {
            latch.countDown();
          }
        }
    }
  }

  class StaticPonger(init: Init[StaticPonger]) extends ComponentDefinition {

    val Init(selfId: Int) = init;

    val net = requires[Network];

    lazy val selfAddr = cfg.getValue[NetAddress](KompicsSystemProvider.SELF_ADDR_KEY);

    ctrl uponEvent {
      case _: Start =>
        handle {
          assert(selfAddr != null);
        }
    }

    net uponEvent {
      case context @ NetMessage(_, StaticPing(this.selfId)) =>
        handle {
          trigger(context.reply(selfAddr)(StaticPong(selfId)) -> net);
        }
    }
  }

  class Pinger(init: Init[Pinger]) extends ComponentDefinition {

    val Init(selfId: Int, latch: CountDownLatch, count: Long, pipeline: Long, ponger: NetAddress) = init;

    val net = requires[Network];

    lazy val selfAddr = cfg.getValue[NetAddress](KompicsSystemProvider.SELF_ADDR_KEY);

    private var sentCount = 0L;
    private var recvCount = 0L;

    ctrl uponEvent {
      case _: Start =>
        handle {
          var pipelined = 0L;
          while (pipelined < pipeline && sentCount < count) {
            trigger(NetMessage.viaTCP(selfAddr, ponger)(Ping(sentCount, selfId)) -> net);
            pipelined += 1L;
            sentCount += 1L;
          }
        }
    }

    net uponEvent {
      case context @ NetMessage(_, Pong(_, this.selfId)) =>
        handle {
          recvCount += 1L;
          if (recvCount < count) {
            if (sentCount < count) {
              trigger(NetMessage.viaTCP(selfAddr, ponger)(Ping(sentCount, selfId)) -> net);
              sentCount += 1L;
            }
          } else {
            latch.countDown();
          }
        }
    }
  }

  class Ponger(init: Init[Ponger]) extends ComponentDefinition {

    val Init(selfId: Int) = init;

    val net = requires[Network];

    lazy val selfAddr = cfg.getValue[NetAddress](KompicsSystemProvider.SELF_ADDR_KEY);

    ctrl uponEvent {
      case _: Start =>
        handle {
          assert(selfAddr != null);
        }
    }

    net uponEvent {
      case context @ NetMessage(_, Ping(index, this.selfId)) =>
        handle {
          trigger(context.reply(selfAddr)(Pong(index, selfId)) -> net);
        }
    }
  }
}
