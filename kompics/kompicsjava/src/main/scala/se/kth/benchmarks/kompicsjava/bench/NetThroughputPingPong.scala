package se.kth.benchmarks.kompicsjava.bench

import se.kth.benchmarks._
import se.kth.benchmarks.kompicsscala.{KompicsSystem, KompicsSystemProvider, NetAddress}
import _root_.kompics.benchmarks.benchmarks.ThroughputPingPongRequest
import se.kth.benchmarks.kompicsjava.bench.netthroughputpingpong._;
import scala.util.{Failure, Success, Try}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import java.util.concurrent.CountDownLatch
import java.util.UUID
import se.sics.kompics.network.netty.serialization.Serializers
import com.typesafe.scalalogging.StrictLogging

object NetThroughputPingPong extends DistributedBenchmark {

  implicit val ec = scala.concurrent.ExecutionContext.global;

  case class ClientParams(numPongers: Int, staticOnly: Boolean)

  override type MasterConf = ThroughputPingPongRequest;
  override type ClientConf = ClientParams;
  override type ClientData = NetAddress;

  class MasterImpl extends Master with StrictLogging {
    private var numMsgs = -1L;
    private var numPairs = -1;
    private var pipeline = -1L;
    private var staticOnly = true;
    private var system: KompicsSystem = null;
    private var pingers: List[UUID] = List.empty;
    private var latch: CountDownLatch = null;

    override def setup(c: MasterConf, _meta: DeploymentMetaData): Try[ClientConf] = Try {
      logger.info("Setting up Master");
      NetPingPongSerializer.register();

      this.numMsgs = c.messagesPerPair;
      this.numPairs = c.parallelism;
      this.pipeline = c.pipelineSize;
      this.staticOnly = c.staticOnly;
      this.system = KompicsSystemProvider.newRemoteKompicsSystem(Runtime.getRuntime.availableProcessors());

      ClientParams(numPairs, staticOnly)
    };
    override def prepareIteration(d: List[ClientData]): Unit = {
      logger.debug("Preparing iteration");
      val ponger = d.head;
      logger.trace(s"Resolved path to ${ponger}");
      latch = new CountDownLatch(numPairs);
      val pingersLF = (1 to numPairs).map { index =>
        for {
          pingerId <- if (staticOnly) {
            system.createNotify[StaticPinger](new StaticPinger.Init(index, latch, numMsgs, pipeline, ponger.asJava))
          } else {
            system.createNotify[Pinger](new Pinger.Init(index, latch, numMsgs, pipeline, ponger.asJava))
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
      logger.debug("Cleaning up pinger side");
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
        logger.info("Cleaned up Master.");
      }
    }
  }

  class ClientImpl extends Client with StrictLogging {
    private var system: KompicsSystem = null;
    private var pongers: List[UUID] = null;

    override def setup(c: ClientConf): ClientData = {
      logger.info("Setting up Client");
      system = KompicsSystemProvider.newRemoteKompicsSystem(1);
      NetPingPongSerializer.register();

      val lf = (0 to c.numPongers).map { index =>
        for {
          pongerId <- if (c.staticOnly) {
            system.createNotify[StaticPonger](new StaticPonger.Init(index))
          } else {
            system.createNotify[Ponger](new Ponger.Init(index))
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
      logger.debug("Preparing ponger iteration");
      assert(system != null);
    }
    override def cleanupIteration(lastIteration: Boolean): Unit = {
      logger.debug("Cleaning up ponger side");
      assert(system != null);
      if (lastIteration) {
        pongers = List.empty; // will be stopped when system is shut down
        system.terminate();
        system = null;
        logger.info("Cleaned up Client.");
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
}
