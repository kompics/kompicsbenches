package se.kth.benchmarks.kompicsjava.bench

import se.kth.benchmarks._
import se.kth.benchmarks.kompicsscala.{KompicsSystem, KompicsSystemProvider, NetAddress}
import se.kth.benchmarks.kompicsjava._
import se.kth.benchmarks.kompicsjava.bench.streamingwindows._

import java.util.UUID
import se.sics.kompics.network.netty.serialization.Serializers

import scala.util.{Failure, Success, Try}
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import java.util.concurrent.{CountDownLatch, TimeUnit, TimeoutException}

import _root_.kompics.benchmarks.benchmarks.StreamingWindowsRequest

import scala.collection.mutable
import scala.util.Try
import java.util.UUID.randomUUID

import com.typesafe.scalalogging.StrictLogging

object StreamingWindows extends DistributedBenchmark {

  case class WindowerConfig(numberOfPartitions: Int,
                            windowSize: FiniteDuration,
                            batchSize: Long,
                            amplification: Long,
                            addr: NetAddress)
  case class WindowerAddr(addr: NetAddress)

  override type MasterConf = StreamingWindowsRequest;
  override type ClientConf = WindowerConfig;
  override type ClientData = WindowerAddr;

  val RESOLVE_TIMEOUT: FiniteDuration = 6.seconds;
  val FLUSH_TIMEOUT: FiniteDuration = 1.minute;
  implicit val ec = scala.concurrent.ExecutionContext.global;

  override def newMaster(): Master = new MasterImpl();
  override def msgToMasterConf(msg: scalapb.GeneratedMessage): Try[MasterConf] =
    Try(msg.asInstanceOf[StreamingWindowsRequest]);

  override def newClient(): Client = new ClientImpl();
  override def strToClientConf(str: String): Try[ClientConf] = Try {
    val parts = str.split(",");
    assert(parts.length == 5, "ClientConf consists of 5 comma-separated parts!");
    val numberOfPartitions = parts(0).toInt;
    val windowSizeMS = parts(1).toLong;
    val windowSize = Duration.apply(windowSizeMS, TimeUnit.MILLISECONDS);
    val batchSize = parts(2).toLong;
    val amplification = parts(3).toLong;
    val addr = NetAddress.fromString(parts(4)).get;
    WindowerConfig(numberOfPartitions, windowSize, batchSize, amplification, addr)
  }
  override def strToClientData(str: String): Try[ClientData] = {
    NetAddress.fromString(str).map(addr => WindowerAddr(addr))
  }

  override def clientConfToString(c: ClientConf): String = {
    s"${c.numberOfPartitions},${c.windowSize.toMillis},${c.batchSize},${c.amplification},${c.addr.asString}"
  }
  override def clientDataToString(d: ClientData): String = d.addr.asString;

  StreamingWindowsSerializer.register();

  class MasterImpl extends Master with StrictLogging {

    private var numberOfPartitions: Int = -1;
    private var batchSize: Long = -1L;
    private var windowSize: FiniteDuration = Duration.Zero;
    private var numberOfWindows: Long = -1L;
    private var windowSizeAmplification: Long = -1L;

    private var system: KompicsSystem = null;
    private var sources: List[(Int, UUID)] = Nil;
    private var sinks: List[(Int, UUID)] = Nil;
    private var latch: CountDownLatch = null;

    override def setup(c: MasterConf, _meta: DeploymentMetaData): Try[ClientConf] = Try {
      logger.info(s"Setting up master: $c");
      this.numberOfPartitions = c.numberOfPartitions;
      this.batchSize = c.batchSize;
      this.windowSizeAmplification = c.windowSizeAmplification;
      this.windowSize = Duration(c.windowSize).asInstanceOf[FiniteDuration];
      this.numberOfWindows = c.numberOfWindows;
      this.system = KompicsSystemProvider.newRemoteKompicsSystem(Runtime.getRuntime.availableProcessors());
      val sourcesLF = (for (pid <- (0 until numberOfPartitions)) yield {
        for {
          source <- system.createNotify[StreamSource](new StreamSource.Init(pid));
          _ <- system.connectNetwork(source);
          _ <- system.startNotify(source)
        } yield (pid, source)
      }).toList;
      val sourcesFL = Future.sequence(sourcesLF);
      this.sources = Await.result(sourcesFL, RESOLVE_TIMEOUT);
      val netAddr = system.networkAddress.get;
      WindowerConfig(numberOfPartitions, windowSize, batchSize, windowSizeAmplification, netAddr)
    }
    override def prepareIteration(d: List[ClientData]): Unit = {
      logger.trace("Preparing iteration");

      val client = d.head;
      val windowerAddr = client.addr;
      this.latch = new CountDownLatch(this.numberOfPartitions);

      val sinksLF = (for (pid <- (0 until numberOfPartitions)) yield {
        for {
          sink <- system
            .createNotify[StreamSink](new StreamSink.Init(pid, this.latch, this.numberOfWindows, windowerAddr.asJava));
          _ <- system.connectNetwork(sink)
        } yield (pid, sink)
      }).toList;
      val sinksFL = Future.sequence(sinksLF);
      this.sinks = Await.result(sinksFL, RESOLVE_TIMEOUT);
    }
    override def runIteration(): Unit = {
      val startLF = this.sinks.map { case (_, sink) => system.startNotify(sink) };
      val startFL = Future.sequence(startLF);
      Await.result(startFL, Duration.Inf);
      this.latch.await();
    }
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      logger.trace("Cleaning up iteration");
      if (this.system != null) {
        val resetFutures = this.sources.map {
          case (_, source) =>
            this.system.runOnComponent(source) { component =>
              val cd = component.getComponent().asInstanceOf[StreamSource];
              scala.compat.java8.FutureConverters.toScala(cd.reset())
            }
        };
        logger.trace("Flushing remaining messages on the channels");
        // Await.ready(Future.sequence(resetFutures), FLUSH_TIMEOUT);
        for (f <- resetFutures) {
          Await.result(Await.result(f, FLUSH_TIMEOUT), FLUSH_TIMEOUT)
        }

        val stopFutures = this.sinks.map { case (_, sink) => this.system.killNotify(sink) };
        Await.ready(Future.sequence(stopFutures), RESOLVE_TIMEOUT);
        this.sinks = Nil;

        logger.trace("Remaining messages are flushed out.");
      }
      if (lastIteration) {
        if (this.system != null) {
          val stopFutures = this.sources.map { case (_, source) => this.system.killNotify(source) };
          Await.ready(Future.sequence(stopFutures), RESOLVE_TIMEOUT);
          this.sources = Nil;

          system.terminate();
          this.system = null;
        }
        logger.info("Completed final cleanup.");
      }
    }
  }

  class ClientImpl extends Client with StrictLogging {

    private var windowSize: FiniteDuration = Duration.Zero;
    private var batchSize: Long = -1L;
    private var amplification: Long = -1L;

    private var system: KompicsSystem = null;
    private var sourceAddress: Option[NetAddress] = None;
    private var windowers: List[(Int, UUID)] = Nil;

    override def setup(c: ClientConf): ClientData = {
      logger.info(s"Setting up client: $c");

      this.windowSize = c.windowSize;
      this.batchSize = c.batchSize;
      this.amplification = c.amplification;
      this.sourceAddress = Some(c.addr);

      this.system = KompicsSystemProvider.newRemoteKompicsSystem(Runtime.getRuntime.availableProcessors());
      val javaWindowSize = scala.compat.java8.DurationConverters.toJava(this.windowSize);
      val windowersLF = (for (pid <- (0 until c.numberOfPartitions)) yield {
        for {
          windower <- system
            .createNotify[Windower](
              new Windower.Init(pid, javaWindowSize, this.batchSize, this.amplification, this.sourceAddress.get.asJava)
            );
          _ <- system.connectNetwork(windower);
          _ <- system.startNotify(windower)
        } yield (pid, windower)
      }).toList;
      val windowersFL = Future.sequence(windowersLF);
      this.windowers = Await.result(windowersFL, RESOLVE_TIMEOUT);
      val netAddr = system.networkAddress.get;
      WindowerAddr(netAddr)
    }
    override def prepareIteration(): Unit = {
      // nothing
      logger.debug("Preparing iteration.");
    }
    override def cleanupIteration(lastIteration: Boolean): Unit = {
      logger.debug("Cleaning iteration.");
      if (lastIteration) {
        if (this.system != null) {
          val stopFutures = this.windowers.map { case (_, windower) => this.system.killNotify(windower) };
          Await.ready(Future.sequence(stopFutures), RESOLVE_TIMEOUT);
          this.windowers = Nil;

          system.terminate();
          this.system = null;
        }
        logger.info("Final cleanup complete!");
      }
    }
  }
}
