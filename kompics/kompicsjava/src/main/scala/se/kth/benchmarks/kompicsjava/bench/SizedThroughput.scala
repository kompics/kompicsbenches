package se.kth.benchmarks.kompicsjava.bench

import java.util.UUID
import java.util.concurrent.CountDownLatch

import _root_.kompics.benchmarks.benchmarks.SizedThroughputRequest
import com.typesafe.scalalogging.StrictLogging
import se.kth.benchmarks._
import se.kth.benchmarks.kompicsjava.bench.sizedthroughput.{SizedThroughputSerializer, SizedThroughputSink, SizedThroughputSource}
import se.kth.benchmarks.kompicsscala.{KompicsSystem, KompicsSystemProvider, NetAddress}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

object SizedThroughput extends DistributedBenchmark {

  implicit val ec = scala.concurrent.ExecutionContext.global;

  case class ClientParams(numPairs: Int, batchSize: Int)

  override type MasterConf = SizedThroughputRequest;
  override type ClientConf = ClientParams;
  override type ClientData = NetAddress;

  class MasterImpl extends Master with StrictLogging {
    private var params: MasterConf = null;
    private var system: KompicsSystem = null;
    private var sources: List[UUID] = List.empty;
    private var latch: CountDownLatch = null;

    override def setup(c: MasterConf, _meta: DeploymentMetaData): Try[ClientConf] = Try {
      logger.info("Setting up Master, numPairs: " ++ c.numberOfPairs.toString
        ++ ", message size: " ++ c.messageSize.toString
        ++ " batchSize: " ++ c.batchSize.toString
        ++  ", batchCount: " ++ c.numberOfBatches.toString);
      SizedThroughputSerializer.register();
      SizedThroughputSerializer.register();

      this.params = c;
      this.system = KompicsSystemProvider.newRemoteKompicsSystem(Runtime.getRuntime.availableProcessors());
      ClientParams(c.numberOfPairs, c.batchSize)
    };

    override def prepareIteration(d: List[ClientData]): Unit = {
      logger.debug("Preparing iteration");
      val sink = d.head;
      logger.trace(s"Resolved path to ${sink}");
      val latch = new CountDownLatch(this.params.numberOfPairs.toInt);

      val lf = (0 to params.numberOfPairs).map { index =>
        for {
          sourceId <- system.createNotify(
            new SizedThroughputSource.Init(params.messageSize, params.batchSize, params.numberOfBatches, index, sink.asJava, latch)
          )
          _ <- system.connectNetwork(sourceId)
        } yield {
          sourceId
        }
      }.toList;
      val fl = Future.sequence(lf);
      val l = Await.result(fl, Duration.Inf);
      sources = l;
      this.latch = latch;
    }
    override def runIteration(): Unit = {
      val startLF = sources.map(source => system.startNotify(source));
      val startFL = Future.sequence(startLF);
      Await.result(startFL, Duration.Inf);
      latch.await();
    };
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      logger.debug("Cleaning up source side");
      assert(system != null);
      if (latch != null) {
        latch = null;
      }
      val killSourcesLF = sources.map(source => system.killNotify(source));
      val killSourcesFL = Future.sequence(killSourcesLF);
      sources = List.empty;
      Await.result(killSourcesFL, Duration.Inf);

      if (lastIteration) {
        sources = List.empty;
        val f = system.terminate();
        system = null;
        logger.info("Cleaned up Master.");
      }
    }
  }

  class ClientImpl extends Client with StrictLogging {
    private var system: KompicsSystem = null;
    private var sinks: List[UUID] = null;

    override def setup(c: ClientConf): ClientData = {
      logger.info("Setting up Client, numPairs: " ++ c.numPairs.toString ++ " batchSize: " ++ c.batchSize.toString);

      this.system = KompicsSystemProvider.newRemoteKompicsSystem(1);
      SizedThroughputSerializer.register();

      val lf = (0 to c.numPairs).map { index =>
        for {
          sinkId <- system.createNotify(new SizedThroughputSink.Init(index, c.batchSize))
          _ <- system.connectNetwork(sinkId);
          _ <- system.startNotify(sinkId)
        } yield {
          sinkId
        }
      }.toList;
      val fl = Future.sequence(lf);
      val l = Await.result(fl, Duration.Inf);
      sinks = l;
      system.networkAddress.get
    }
    override def prepareIteration(): Unit = {
      // nothing
      logger.debug("Preparing sink iteration");
      assert(system != null);
    }
    override def cleanupIteration(lastIteration: Boolean): Unit = {
      logger.debug("Cleaning up sink side");
      assert(system != null);
      if (lastIteration) {
        sinks = List.empty; // will be stopped when system is shut down
        system.terminate();
        system = null;
        logger.info("Cleaned up Client.");
      }
    }
  }

  override def newMaster(): Master = new MasterImpl();
  override def msgToMasterConf(msg: scalapb.GeneratedMessage): Try[MasterConf] = Try {
    msg.asInstanceOf[SizedThroughputRequest]
  };

  override def newClient(): Client = new ClientImpl();
  override def strToClientConf(str: String): Try[ClientConf] = Try {
    val split = str.split(",");
    val numPairs = split(0).toInt;
    val batchSize = split(1).toInt;
    ClientParams(numPairs, batchSize)
  };
  override def strToClientData(str: String): Try[ClientData] = NetAddress.fromString(str);

  override def clientConfToString(c: ClientConf): String = s"${c.numPairs},${c.batchSize}";
  override def clientDataToString(d: ClientData): String = d.asString;
}
