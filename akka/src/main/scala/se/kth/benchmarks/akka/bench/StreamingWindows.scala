package se.kth.benchmarks.akka.bench

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.serialization.Serializer
import akka.util.{ByteString, Timeout}
import akka.event.Logging
import akka.pattern.ask
import se.kth.benchmarks.akka._
import se.kth.benchmarks._

import scala.util.{Failure, Success, Try}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import java.util.concurrent.{CountDownLatch, TimeUnit, TimeoutException}

import kompics.benchmarks.benchmarks.StreamingWindowsRequest

import scala.collection.mutable
import scala.util.Try
import java.util.UUID.randomUUID

import com.typesafe.scalalogging.StrictLogging

object StreamingWindows extends DistributedBenchmark {

  case class WindowerConfig(windowSize: Duration,
                            batchSize: Long,
                            amplification: Long,
                            upstreamActorPaths: List[String])
  case class WindowerRefs(actorPaths: List[String])

  override type MasterConf = StreamingWindowsRequest;
  override type ClientConf = WindowerConfig;
  override type ClientData = WindowerRefs;

  val RESOLVE_TIMEOUT: FiniteDuration = 6.seconds;
  val FLUSH_TIMEOUT: FiniteDuration = 1.minute;

  val serializers = SerializerBindings
    .empty()
    .addSerializer[StreamingWindowsSerializer](StreamingWindowsSerializer.NAME)
    .addBinding[WindowerMessage.Start.type](StreamingWindowsSerializer.NAME)
    .addBinding[WindowerMessage.Stop.type](StreamingWindowsSerializer.NAME)
    .addBinding[WindowerMessage.Event](StreamingWindowsSerializer.NAME)
    .addBinding[WindowerMessage.Ready](StreamingWindowsSerializer.NAME)
    .addBinding[WindowerMessage.WindowAggregate](StreamingWindowsSerializer.NAME)
    .addBinding[WindowerMessage.Flush.type](StreamingWindowsSerializer.NAME)
    .addBinding[WindowerMessage.Flushed.type](StreamingWindowsSerializer.NAME);

  override def newMaster(): Master = new MasterImpl();
  override def msgToMasterConf(msg: scalapb.GeneratedMessage): Try[MasterConf] =
    Try(msg.asInstanceOf[StreamingWindowsRequest]);

  override def newClient(): Client = new ClientImpl();
  override def strToClientConf(str: String): Try[ClientConf] = Try {
    val parts = str.split(",");
    assert(parts.length == 4, "ClientConf consists of 4 comma-separated parts!");
    val windowSizeMS = parts(0).toLong;
    val windowSize = Duration.apply(windowSizeMS, TimeUnit.MILLISECONDS);
    val batchSize = parts(1).toLong;
    val amplification = parts(2).toLong;
    val upstream = parts(3).split(";").toList;
    WindowerConfig(windowSize, batchSize, amplification, upstream)
  }
  override def strToClientData(str: String): Try[ClientData] = Success(WindowerRefs(str.split(";").toList));

  override def clientConfToString(c: ClientConf): String = {
    s"${c.windowSize.toMillis},${c.batchSize},${c.amplification},${c.upstreamActorPaths.mkString(";")}"
  }
  override def clientDataToString(d: ClientData): String = d.actorPaths.mkString(";");

  class MasterImpl extends Master with StrictLogging {

    private var numberOfPartitions: Int = -1;
    private var batchSize: Long = -1L;
    private var windowSize: Duration = Duration.Zero;
    private var numberOfWindows: Long = -1L;
    private var windowSizeAmplification: Long = -1L;

    private var system: ActorSystem = null;
    private var sources: List[(Int, ActorRef)] = Nil;
    private var sinks: List[ActorRef] = Nil;
    private var latch: CountDownLatch = null;

    override def setup(c: MasterConf, _meta: DeploymentMetaData): Try[ClientConf] = Try {
      logger.info(s"Setting up master: $c");
      this.numberOfPartitions = c.numberOfPartitions;
      this.batchSize = c.batchSize;
      this.windowSizeAmplification = c.windowSizeAmplification;
      this.windowSize = Duration(c.windowSize);
      this.numberOfWindows = c.numberOfWindows;
      this.system = ActorSystemProvider.newRemoteActorSystem(name = "streamingwindows",
                                                             threads = Runtime.getRuntime.availableProcessors(),
                                                             serialization = serializers);
      this.sources = (for (pid <- (0 until numberOfPartitions)) yield {
        val source = system.actorOf(Props(new StreamSource(pid)));
        (pid, source)
      }).toList;
      val sourcesStringified: List[String] = this.sources.map {
        case (_, ref) => ActorSystemProvider.actorPathForRef(ref, system)
      };
      WindowerConfig(windowSize, batchSize, windowSizeAmplification, sourcesStringified)
    }
    override def prepareIteration(d: List[ClientData]): Unit = {
      implicit val ec = scala.concurrent.ExecutionContext.global;

      logger.trace("Preparing iteration");

      val client = d.head;
      val windowersFs = client.actorPaths.map(path => system.actorSelection(path).resolveOne(RESOLVE_TIMEOUT));
      val windowersF = Future.sequence(windowersFs);
      val windowers = Await.result(windowersF, RESOLVE_TIMEOUT);
      this.latch = new CountDownLatch(this.numberOfPartitions);
      this.sinks = (for (windower <- windowers) yield {
        val source = system.actorOf(Props(new StreamSink(this.latch, this.numberOfWindows, windower)));
        source
      }).toList;
    }
    override def runIteration(): Unit = {
      for (sink <- this.sinks) {
        sink ! StreamSink.Start;
      }
      this.latch.await();
    }
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      implicit val ec = scala.concurrent.ExecutionContext.global;
      implicit val timeout = Timeout(FLUSH_TIMEOUT);

      logger.trace("Cleaning up iteration");
      val resetFutures = this.sources.map { case (_, source) => source ? StreamSource.Reset };
      this.sinks.foreach(sink => this.system.stop(sink));
      this.sinks = Nil;
      logger.trace("Flushing remaining messages on the channels");
      Await.ready(Future.sequence(resetFutures), FLUSH_TIMEOUT);
      logger.trace("Remaining messages are flushed out.");
      if (lastIteration) {
        if (this.system != null) {
          this.sources.foreach { case (_, source) => this.system.stop(source) };
          this.sources = Nil;

          val f = system.terminate();
          Await.ready(f, RESOLVE_TIMEOUT);
          this.system = null;
        }
        logger.info("Completed final cleanup.");
      }
    }
  }

  class ClientImpl extends Client with StrictLogging {

    private var windowSize: Duration = Duration.Zero;
    private var batchSize: Long = -1L;
    private var amplification: Long = -1L;

    private var system: ActorSystem = null;
    private var sources: List[ActorRef] = Nil;
    private var windowers: List[ActorRef] = Nil;

    override def setup(c: ClientConf): ClientData = {
      logger.info(s"Setting up client: $c");
      implicit val ec = scala.concurrent.ExecutionContext.global;

      this.windowSize = c.windowSize;
      this.batchSize = c.batchSize;
      this.amplification = c.amplification;
      this.system = ActorSystemProvider.newRemoteActorSystem(name = "streamingwindows",
                                                             threads = Runtime.getRuntime.availableProcessors(),
                                                             serialization = serializers);
      val sourcesFs = c.upstreamActorPaths.map(path => system.actorSelection(path).resolveOne(RESOLVE_TIMEOUT));
      val sourcesF = Future.sequence(sourcesFs);
      this.sources = Await.result(sourcesF, RESOLVE_TIMEOUT);
      this.windowers = for (source <- sources) yield {
        system.actorOf(Props(new Windower(this.windowSize, this.batchSize, this.amplification, source)))
      };
      val windowerPaths = this.windowers.map(windower => ActorSystemProvider.actorPathForRef(windower, system));
      WindowerRefs(windowerPaths)
    }
    override def prepareIteration(): Unit = {
      // nothing
      logger.debug("Preparing iteration.");
    }
    override def cleanupIteration(lastIteration: Boolean): Unit = {
      logger.debug("Cleaning iteration.");
      if (lastIteration) {
        if (this.system != null) {
          this.windowers.foreach(windower => this.system.stop(windower));
          this.windowers = Nil;
          val f = system.terminate();
          Await.ready(f, RESOLVE_TIMEOUT);
          this.system = null;
        }
        logger.info("Final cleanup complete!");
      }
    }
  }

  sealed trait WindowerMessage;
  object WindowerMessage {
    // receive
    case object Start extends WindowerMessage;
    case object Stop extends WindowerMessage;
    case class Event(ts: Long, partitionId: Int, value: Long) extends WindowerMessage;
    case object Flush extends WindowerMessage;
    // send
    case class Ready(numMessages: Long) extends WindowerMessage;
    case class WindowAggregate(ts: Long, partitionId: Int, value: Double) extends WindowerMessage;
    case object Flushed extends WindowerMessage;
  }

  class Windower(windowSize: Duration, val batchSize: Long, val amplification: Long, val upstream: ActorRef)
      extends Actor
      with ActorLogging {
    import WindowerMessage._;

    val windowSizeMS = windowSize.toMillis;
    require(windowSizeMS > 0L);
    val readyMark = batchSize / 2;

    private var downstream: Option[ActorRef] = None;

    private val currentWindow = mutable.ArrayBuffer.empty[Long];
    private var windowStartTS: Long = 0L;

    private var receivedSinceReady: Long = 0L;
    private var running: Boolean = false;

    override def receive = {
      case Start if !running => {
        log.debug("Got Start");
        downstream = Some(sender());
        running = true;
        receivedSinceReady = 0L;
        upstream ! Ready(batchSize);
      }
      case Start if running => ??? // da fuq?
      case Stop if running => {
        log.debug("Got Stop");
        running = false;
        currentWindow.clear();
        downstream = None;
        windowStartTS = 0L;
      }
      case Stop if !running => ??? // da fuq?
      case Event(ts, id, value) if running => {
        if (ts < (windowStartTS + windowSizeMS)) {
          //log.debug(s"Window not yet ready ($ts < ${(windowStartTS + windowSizeMS)})");
          for (_ <- 0L until this.amplification) {
            currentWindow += value;
          }
        } else {
          triggerWindow(id);
          windowStartTS = ts;
          for (_ <- 0L until this.amplification) {
            currentWindow += value;
          }
        }
        manageReady();
      }
      case e: Event if !running => {
        log.debug(s"Dropping message $e, since not running!");
      }
      case Flush => {
        sender() ! Flushed;
      }
    }

    private def triggerWindow(partitionId: Int): Unit = {
      log.debug(s"Triggering window with ${currentWindow.size} messages.");
      if (currentWindow.isEmpty) {
        log.warning("Windows should not be empty in the benchmark!");
        return;
      }
      val median = Statistics.medianFromUnsorted(currentWindow);
      currentWindow.clear();
      downstream.get ! WindowAggregate(windowStartTS, partitionId, median);
    }
    private def manageReady(): Unit = {
      receivedSinceReady += 1L;
      if (receivedSinceReady > readyMark) {
        log.debug("Sending Ready");
        upstream ! Ready(receivedSinceReady);
        receivedSinceReady = 0L;
      }
    }
  }

  class StreamSource(val partitionId: Int) extends Actor with ActorLogging {
    import WindowerMessage._;

    val random = new java.util.Random(partitionId);

    private var downstream: Option[ActorRef] = None;

    private var remaining: Long = 0L;
    private var currentTS: Long = 0L;

    private var flushing: Boolean = false;

    private var replyOnFlushed: Option[ActorRef] = None;

    override def receive = {
      case Ready(batchSize) if !flushing => {
        log.debug("Got ready!");
        this.remaining += batchSize;
        this.downstream = Some(sender());
        send()
      }
      case Ready(_) if flushing           => () // drop
      case StreamSource.Next if !flushing => send()
      case StreamSource.Next if flushing  => () // drop
      case StreamSource.Reset if !flushing => {
        log.debug("Got reset");
        this.flushing = true;
        this.replyOnFlushed = Some(sender());
        this.downstream.get ! Flush;
        this.currentTS = 0L;
        this.remaining = 0L;
        this.downstream = None;
      }
      case Flushed if flushing => {
        log.debug("Got flushed");
        this.replyOnFlushed.get ! Flushed;
        this.replyOnFlushed = None;
        this.flushing = false;
      }
    }

    private def send(): Unit = {
      if (remaining > 0L) {
        downstream.get ! Event(currentTS, partitionId, random.nextLong());
        currentTS += 1L;
        remaining -= 1L;
        if (remaining > 0L) {
          self ! StreamSource.Next;
        } else {
          log.debug("Waiting for Ready...");
        }
      }
    }
  }
  object StreamSource {
    case object Next;
    case object Reset;
  }

  class StreamSink(val latch: CountDownLatch, val numberOfWindows: Long, val upstream: ActorRef)
      extends Actor
      with ActorLogging {

    private var windowCount: Long = 0L;

    override def receive = {
      case StreamSink.Start => {
        log.debug("Got start!");
        upstream ! WindowerMessage.Start;
      }
      case WindowerMessage.WindowAggregate(_, _, median) => {
        log.debug(s"Got window with median=$median");
        windowCount += 1L;
        if (windowCount == numberOfWindows) {
          latch.countDown();
          upstream ! WindowerMessage.Stop;
          log.debug("Done!");
        } else {
          log.debug(s"Got ${windowCount}/${numberOfWindows} windows.");
        }
      }
    }
  }
  object StreamSink {
    case object Start;
  }

  object StreamingWindowsSerializer {

    val NAME = "streamingwindows";

    private val START_FLAG: Byte = 1;
    private val STOP_FLAG: Byte = 2;
    private val EVENT_FLAG: Byte = 3;
    private val READY_FLAG: Byte = 4;
    private val WINDOW_FLAG: Byte = 5;
    private val FLUSH_FLAG: Byte = 6;
    private val FLUSHED_FLAG: Byte = 7;
  }

  class StreamingWindowsSerializer extends Serializer {
    import StreamingWindowsSerializer._;
    import WindowerMessage._;
    import java.nio.{ByteBuffer, ByteOrder};
    import _root_.akka.util.ByteString;

    implicit val order = ByteOrder.BIG_ENDIAN;

    override def identifier: Int = SerializerIds.STREAMINGWINDOWS;
    override def includeManifest: Boolean = false;
    override def toBinary(o: AnyRef): Array[Byte] = {
      o match {
        case Start => Array(START_FLAG)
        case Stop  => Array(STOP_FLAG)
        case Event(ts, pid, value) => {
          val bs = ByteString.createBuilder;
          bs.putByte(EVENT_FLAG);
          bs.putLong(ts);
          bs.putInt(pid);
          bs.putLong(value);
          bs.result().toArray
        }
        case Ready(quota) => {
          val bs = ByteString.createBuilder;
          bs.putByte(READY_FLAG);
          bs.putLong(quota);
          bs.result().toArray
        }
        case WindowAggregate(ts, pid, value) => {
          val bs = ByteString.createBuilder;
          bs.putByte(WINDOW_FLAG);
          bs.putLong(ts);
          bs.putInt(pid);
          bs.putDouble(value);
          bs.result().toArray
        }
        case Flush   => Array(FLUSH_FLAG)
        case Flushed => Array(FLUSHED_FLAG)
      }
    }

    override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
      val buf = ByteBuffer.wrap(bytes).order(order);
      val flag = buf.get;

      flag match {
        case START_FLAG => Start
        case STOP_FLAG  => Stop
        case EVENT_FLAG => {
          val ts = buf.getLong();
          val pid = buf.getInt();
          val value = buf.getLong();
          Event(ts, pid, value)
        }
        case READY_FLAG => {
          val quota = buf.getLong();
          Ready(quota)
        }
        case WINDOW_FLAG => {
          val ts = buf.getLong();
          val pid = buf.getInt();
          val value = buf.getDouble();
          WindowAggregate(ts, pid, value)
        }
        case FLUSH_FLAG   => Flush
        case FLUSHED_FLAG => Flushed
      }
    }
  }
}
