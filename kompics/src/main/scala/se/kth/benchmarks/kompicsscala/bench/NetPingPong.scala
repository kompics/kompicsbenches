package se.kth.benchmarks.kompicsscala.bench

import se.kth.benchmarks._
import se.kth.benchmarks.kompicsscala._
import kompics.benchmarks.benchmarks.PingPongRequest
import se.sics.kompics.sl._
import se.sics.kompics.{ KompicsEvent, Start }
import se.sics.kompics.network.Network
import scala.util.{ Try, Success, Failure }
import scala.concurrent.Await
import scala.concurrent.duration._
import java.util.concurrent.CountDownLatch
import java.util.UUID
import se.sics.kompics.network.netty.serialization.{ Serializer, Serializers }
import com.google.common.base.Optional
import io.netty.buffer.ByteBuf

object NetPingPong extends DistributedBenchmark {

  case class ClientRef(actorPath: String)

  override type MasterConf = PingPongRequest;
  override type ClientConf = Unit;
  override type ClientData = NetAddress;

  class MasterImpl extends Master {
    private var num = -1l;
    private var system: KompicsSystem = null;
    private var pinger: UUID = null;
    private var latch: CountDownLatch = null;

    override def setup(c: MasterConf): ClientConf = {
      this.num = c.numberOfMessages;
      system = KompicsSystemProvider.newRemoteKompicsSystem(1);
      NetPingPongSerializer.register();
    };
    override def prepareIteration(d: List[ClientData]): Unit = {
      assert(system != null);
      val pongerAddr = d.head;
      latch = new CountDownLatch(1);
      val pingerIdF = system.createNotify[Pinger](Init(latch, num, pongerAddr));
      pinger = Await.result(pingerIdF, 5.second);
      val connF = system.connectNetwork(pinger);
      Await.result(connF, 5.seconds);
      val addr = system.networkAddress.get;
      println(s"Pinger Path is $addr");
    }
    override def runIteration(): Unit = {
      assert(system != null);
      assert(pinger != null);
      val startF = system.startNotify(pinger);
      //startF.failed.foreach(e => eprintln(s"Could not start pinger: $e"));
      latch.await();
    };
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      println("Cleaning up pinger side");
      assert(system != null);
      if (latch != null) {
        latch = null;
      }
      if (pinger != null) {
        val killF = system.killNotify(pinger);
        Await.ready(killF, 5.seconds);
        pinger = null;
      }
      if (lastIteration) {
        val f = system.terminate();
        system = null;
      }
    }
  }

  class ClientImpl extends Client {
    private var system: KompicsSystem = null;
    private var ponger: UUID = null;

    override def setup(c: ClientConf): ClientData = {
      system = KompicsSystemProvider.newRemoteKompicsSystem(1);
      NetPingPongSerializer.register();

      val pongerF = system.createNotify[Ponger](Init());
      ponger = Await.result(pongerF, 5.seconds);
      val connF = system.connectNetwork(ponger);
      Await.result(connF, 5.seconds);
      val addr = system.networkAddress.get;
      println(s"Ponger Path is $addr");
      val pongerStartF = system.startNotify(ponger);
      Await.result(pongerStartF, 5.seconds);
      addr
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
        ponger = null; // will be stopped when system is shut down
        system.terminate();
        system = null;
      }
    }
  }

  override def newMaster(): Master = new MasterImpl();
  override def msgToMasterConf(msg: scalapb.GeneratedMessage): Try[MasterConf] = Try {
    msg.asInstanceOf[PingPongRequest]
  };

  override def newClient(): Client = new ClientImpl();
  override def strToClientConf(str: String): Try[ClientConf] = Success(());
  override def strToClientData(str: String): Try[ClientData] = Try {
    val split = str.split(":");
    assert(split.length == 2);
    val ipStr = split(0); //.replaceAll("""/""", "");
    val portStr = split(1);
    val port = portStr.toInt;
    NetAddress.from(ipStr, port)
  }.flatten;

  override def clientConfToString(c: ClientConf): String = "";
  override def clientDataToString(d: ClientData): String = {
    s"${d.isa.getHostString()}:${d.getPort()}"
  }

  case object Ping extends KompicsEvent;
  case object Pong extends KompicsEvent;

  object NetPingPongSerializer extends Serializer {

    def register(): Unit = {
      Serializers.register(this, "netpingpong");
      Serializers.register(Ping.getClass, "netpingpong");
      Serializers.register(Pong.getClass, "netpingpong");
    }

    val NO_HINT: Optional[Object] = Optional.absent();

    private val PING_FLAG: Byte = 1;
    private val PONG_FLAG: Byte = 2;

    override def identifier(): Int = 101;

    override def toBinary(o: Any, buf: ByteBuf): Unit = {
      o match {
        case Ping => buf.writeByte(PING_FLAG)
        case Pong => buf.writeByte(PONG_FLAG)
      }
    }

    override def fromBinary(buf: ByteBuf, hint: Optional[Object]): Object = {
      val flag = buf.readByte();
      flag match {
        case PING_FLAG => Ping
        case PONG_FLAG => Pong
        case _ => {
          Console.err.print(s"Got invalid ser flag: $flag");
          null
        }
      }
    }
  }

  class Pinger(init: Init[Pinger]) extends ComponentDefinition {

    val net = requires[Network];

    val Init(latch: CountDownLatch, count: Long, ponger: NetAddress) = init;

    lazy val selfAddr = cfg.getValue[NetAddress](KompicsSystemProvider.SELF_ADDR_KEY);

    var countDown = count;

    ctrl uponEvent {
      case _: Start => handle {
        assert(selfAddr != null);
        trigger(NetMessage.viaTCP(selfAddr, ponger)(Ping) -> net);
      }
    }
    net uponEvent {
      case context @ NetMessage(_, Pong) => handle {
        if (countDown > 0) {
          countDown -= 1;
          trigger(NetMessage.viaTCP(selfAddr, ponger)(Ping) -> net);
        } else {
          latch.countDown();
        }
      }
    }
  }

  class Ponger(_init: Init[Ponger]) extends ComponentDefinition {

    val net = requires[Network];

    lazy val selfAddr = cfg.getValue[NetAddress](KompicsSystemProvider.SELF_ADDR_KEY);

    ctrl uponEvent {
      case _: Start => handle {
        assert(selfAddr != null);
      }
    }

    net uponEvent {
      case context @ NetMessage(_, Ping) => handle {
        trigger(context.reply(selfAddr)(Pong) -> net);
      }
    }
  }
}
