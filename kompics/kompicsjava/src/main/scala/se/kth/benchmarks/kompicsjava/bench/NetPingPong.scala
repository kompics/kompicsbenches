package se.kth.benchmarks.kompicsjava.bench

import se.kth.benchmarks._
import se.kth.benchmarks.kompicsscala.{KompicsSystem, KompicsSystemProvider, NetAddress}
import _root_.kompics.benchmarks.benchmarks.PingPongRequest
import se.kth.benchmarks.kompicsjava.bench.netpingpong._
import se.sics.kompics.network.Network
import se.sics.kompics.sl.Init
import scala.util.{Failure, Success, Try}
import scala.concurrent.Await
import scala.concurrent.duration._
import java.util.concurrent.CountDownLatch
import java.util.UUID
import se.sics.kompics.network.netty.serialization.{Serializer, Serializers}
import java.util.Optional
import io.netty.buffer.ByteBuf
import com.typesafe.scalalogging.StrictLogging

object NetPingPong extends DistributedBenchmark {

  override type MasterConf = PingPongRequest;
  override type ClientConf = Unit;
  override type ClientData = NetAddress;

  NetPingPongSerializer.register();

  class MasterImpl extends Master with StrictLogging {
    private var num = -1L;
    private var system: KompicsSystem = null;
    private var pinger: UUID = null;
    private var latch: CountDownLatch = null;

    override def setup(c: MasterConf, _meta: DeploymentMetaData): Try[ClientConf] = Try {
      logger.info("Setting up Master");
      this.num = c.numberOfMessages;
      this.system = KompicsSystemProvider.newRemoteKompicsSystem(1);
    };
    override def prepareIteration(d: List[ClientData]): Unit = {
      logger.debug("Preparing iteration");
      assert(system != null);
      val pongerAddr = d.head;
      latch = new CountDownLatch(1);
      val pingerIdF = system.createNotify[Pinger](new Pinger.Init(latch, num, pongerAddr.asJava));
      pinger = Await.result(pingerIdF, 5.second);
      val connF = system.connectNetwork(pinger);
      Await.result(connF, 5.seconds);
      val addr = system.networkAddress.get;
      logger.trace(s"Pinger Path is $addr");
    }
    override def runIteration(): Unit = {
      assert(system != null);
      assert(pinger != null);
      val startF = system.startNotify(pinger);
      latch.await();
    };
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      logger.debug("Cleaning up pinger side");
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
        logger.info("Cleaned up Master");
      }
    }
  }

  class ClientImpl extends Client with StrictLogging {
    private var system: KompicsSystem = null;
    private var ponger: UUID = null;

    override def setup(c: ClientConf): ClientData = {
      logger.info("Setting up Client");
      system = KompicsSystemProvider.newRemoteKompicsSystem(1);
      NetPingPongSerializer.register();

      val pongerF = system.createNotify[Ponger](Init.none[Ponger]);
      ponger = Await.result(pongerF, 5.seconds);
      val connF = system.connectNetwork(ponger);
      Await.result(connF, 5.seconds);
      val addr = system.networkAddress.get;
      logger.trace(s"Ponger Path is $addr");
      val pongerStartF = system.startNotify(ponger);
      Await.result(pongerStartF, 5.seconds);
      addr
    }
    override def prepareIteration(): Unit = {
      logger.debug("Preparing ponger iteration");
      assert(system != null);
    }
    override def cleanupIteration(lastIteration: Boolean): Unit = {
      logger.debug("Cleaning up ponger side");
      assert(system != null);
      if (lastIteration) {
        ponger = null; // will be stopped when system is shut down
        system.terminate();
        system = null;
        logger.info("Cleaned up Client");
      }
    }
  }

  override def newMaster(): Master = new MasterImpl();
  override def msgToMasterConf(msg: scalapb.GeneratedMessage): Try[MasterConf] = Try {
    msg.asInstanceOf[PingPongRequest]
  };

  override def newClient(): Client = new ClientImpl();
  override def strToClientConf(str: String): Try[ClientConf] = Success(());
  override def strToClientData(str: String): Try[ClientData] = NetAddress.fromString(str);

  override def clientConfToString(c: ClientConf): String = "";
  override def clientDataToString(d: ClientData): String = d.asString;
}
