package se.kth.benchmarks.kompicsscala.bench

import se.kth.benchmarks._
import se.kth.benchmarks.kompicsscala._

import se.sics.kompics.network.Network
import se.sics.kompics.sl._
import java.util.UUID
import se.sics.kompics.network.netty.serialization.{Serializer, Serializers}
import java.util.Optional
import io.netty.buffer.ByteBuf

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
                            windowSize: Duration,
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
    private var windowSize: Duration = Duration.Zero;
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
      this.windowSize = Duration(c.windowSize);
      this.numberOfWindows = c.numberOfWindows;
      this.system = KompicsSystemProvider.newRemoteKompicsSystem(Runtime.getRuntime.availableProcessors());
      val sourcesLF = (for (pid <- (0 until numberOfPartitions)) yield {
        for {
          source <- system.createNotify[StreamSource](Init[StreamSource](pid));
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
      logger.info("Preparing iteration");

      val client = d.head;
      val windowerAddr = client.addr;
      this.latch = new CountDownLatch(this.numberOfPartitions);

      val sinksLF = (for (pid <- (0 until numberOfPartitions)) yield {
        for {
          sink <- system
            .createNotify[StreamSink](Init[StreamSink](pid, this.latch, this.numberOfWindows, windowerAddr));
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
              logger.trace("Resetting source");
              cd.reset()
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

    private var windowSize: Duration = Duration.Zero;
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
      val windowersLF = (for (pid <- (0 until c.numberOfPartitions)) yield {
        for {
          windower <- system
            .createNotify[Windower](
              WindowerInit(pid, this.windowSize, this.batchSize, this.amplification, this.sourceAddress.get)
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

  sealed trait WindowerMessage extends KompicsEvent;
  object WindowerMessage {
    // receive
    case class Start(partitionId: Int) extends WindowerMessage;
    case class Stop(partitionId: Int) extends WindowerMessage;
    case class Event(ts: Long, partitionId: Int, value: Long) extends WindowerMessage;
    case class Flush(partitionId: Int) extends WindowerMessage;
    // send
    case class Ready(partitionId: Int, numMessages: Long) extends WindowerMessage;
    case class WindowAggregate(ts: Long, partitionId: Int, value: Double) extends WindowerMessage;
    case class Flushed(partitionId: Int) extends WindowerMessage;
  }

  case class WindowerInit(partitionId: Int,
                          windowSize: Duration,
                          batchSize: Long,
                          amplification: Long,
                          upstream: NetAddress)
      extends se.sics.kompics.Init[Windower];

  class Windower(init: WindowerInit) extends ComponentDefinition {
    import WindowerMessage._;

    val WindowerInit(partitionId: Int,
                     windowSize: Duration,
                     batchSize: Long,
                     amplification: Long,
                     upstream: NetAddress) =
      init;

    private val net = requires[Network];

    lazy val selfAddr = cfg.getValue[NetAddress](KompicsSystemProvider.SELF_ADDR_KEY);

    val windowSizeMS = windowSize.toMillis;
    require(windowSizeMS > 0L);
    val readyMark = batchSize / 2;

    private var downstream: Option[NetAddress] = None;

    private val currentWindow = mutable.ArrayBuffer.empty[Long];
    private var windowStartTS: Long = 0L;

    private var receivedSinceReady: Long = 0L;
    private var running: Boolean = false;
    private var waitingOnFlushed = false;

    net uponEvent {
      case context @ NetMessage(_, Start(this.partitionId)) =>
        handle {
          if (!running) {
            log.debug("Got Start");
            downstream = Some(context.getSource);
            running = true;
            receivedSinceReady = 0L;
            sendUpstream(Ready(partitionId, batchSize));
          } else {
            ??? // nope!
          }
        }
      case context @ NetMessage(_, Stop(this.partitionId)) =>
        handle {
          if (running) {
            log.debug("Got Stop");
            running = false;
            currentWindow.clear();
            downstream = None;
            windowStartTS = 0L;
            if (waitingOnFlushed) {
              sendUpstream(Flushed(this.partitionId));
              waitingOnFlushed = false;
            }
          } else {
            ??? // nope!
          }
        }
      case context @ NetMessage(_, Event(ts, this.partitionId, value)) =>
        handle {
          if (running) {
            if (ts < (windowStartTS + windowSizeMS)) {
              //log.debug(s"Window not yet ready ($ts < ${(windowStartTS + windowSizeMS)})");
              for (_ <- 0L until this.amplification) {
                currentWindow += value;
              }
            } else {
              triggerWindow(this.partitionId);
              windowStartTS = ts;
              for (_ <- 0L until this.amplification) {
                currentWindow += value;
              }
            }
            manageReady();
          } else {
            log.debug("Dropping message Event, since not running!");
          }
        }
      case context @ NetMessage(_, Flush(this.partitionId)) =>
        handle {
          if (!running) { // delay flushing response until not running
            sendUpstream(Flushed(this.partitionId));
          } else {
            waitingOnFlushed = true;
          }
        }
    }

    private def sendUpstream(event: KompicsEvent): Unit = {
      val msg = NetMessage.viaTCP(selfAddr, upstream)(event);
      trigger(msg, net);
    }
    private def sendDownstream(event: KompicsEvent): Unit = {
      val msg = NetMessage.viaTCP(selfAddr, downstream.get)(event);
      trigger(msg, net);
    }

    private def triggerWindow(partitionId: Int): Unit = {
      log.debug(s"Triggering window with ${currentWindow.size} messages.");
      if (currentWindow.isEmpty) {
        log.warn("Windows should not be empty in the benchmark!");
        return;
      }
      val median = Statistics.medianFromUnsorted(currentWindow);
      currentWindow.clear();
      sendDownstream(WindowAggregate(windowStartTS, partitionId, median));
    }
    private def manageReady(): Unit = {
      receivedSinceReady += 1L;
      if (receivedSinceReady > readyMark) {
        log.debug("Sending Ready");
        sendUpstream(Ready(this.partitionId, receivedSinceReady));
        receivedSinceReady = 0L;
      }
    }
  }

  object StreamSource {
    case object Next extends KompicsEvent;
    case class Reset(replyWith: Promise[Unit]) extends KompicsEvent;
  }

  class StreamSource(init: Init[StreamSource]) extends ComponentDefinition {
    import WindowerMessage.{Event, Flush, Flushed, Ready};
    import StreamSource._;

    val Init(partitionId: Int) = init;

    private val net = requires[Network];

    val random = new java.util.Random(partitionId);

    lazy val selfAddr = cfg.getValue[NetAddress](KompicsSystemProvider.SELF_ADDR_KEY);
    private var downstream: Option[NetAddress] = None;

    private var remaining: Long = 0L;
    private var currentTS: Long = 0L;

    private var flushing: Boolean = false;

    private var replyOnFlushed: Option[Promise[Unit]] = None;

    net uponEvent {
      case context @ NetMessage(_, Ready(this.partitionId, batchSize)) =>
        handle {
          if (!flushing) {
            log.debug("Got ready!");
            this.remaining += batchSize;
            this.downstream = Some(context.getSource());
            send()
          } // else drop
        }
      case context @ NetMessage(_, Flushed(this.partitionId)) =>
        handle {
          if (flushing) {
            log.debug("Got Flushed");
            this.replyOnFlushed.get.success(());
            this.replyOnFlushed = None;
            this.flushing = false;
          } else {
            ??? // shouldn't happen!
          }
        }
    }

    loopbck uponEvent {
      case Reset(replyTo) =>
        handle {
          log.debug(s"Got Reset replyOnFlushed = ${replyTo.isCompleted}");
          this.flushing = true;
          this.replyOnFlushed = Some(replyTo);
          sendDownstream(Flush(this.partitionId));
          this.currentTS = 0L;
          this.remaining = 0L;
          this.downstream = None;
        }
      case Next =>
        handle {
          if (!flushing) {
            send();
          } // else drop
        }
    }

    private def send(): Unit = {
      if (remaining > 0L) {
        sendDownstream(Event(currentTS, partitionId, random.nextLong()));
        currentTS += 1L;
        remaining -= 1L;
        if (remaining > 0L) {
          trigger(Next, onSelf);
        } else {
          log.debug("Waiting for Ready...");
        }
      }
    }

    private def sendDownstream(event: KompicsEvent): Unit = {
      val to = downstream.get;
      val msg = NetMessage.viaTCP(selfAddr, to)(event);
      trigger(msg, net);
    }

    def reset(): Future[Unit] = {
      val p: Promise[Unit] = Promise();
      trigger(Reset(p), onSelf);
      p.future
    }
  }

  class StreamSink(init: Init[StreamSink]) extends ComponentDefinition {

    val Init(partitionId: Int, latch: CountDownLatch, numberOfWindows: Long, upstream: NetAddress) = init;

    private val net = requires[Network];

    lazy val selfAddr = cfg.getValue[NetAddress](KompicsSystemProvider.SELF_ADDR_KEY);

    private var windowCount: Long = 0L;

    ctrl uponEvent {
      case _: Start =>
        handle {
          log.debug("Got Start!");
          sendUpstream(WindowerMessage.Start(partitionId));
        }
    }

    net uponEvent {
      case context @ NetMessage(_, WindowerMessage.WindowAggregate(_, this.partitionId, median)) =>
        handle {
          log.debug(s"Got window with median=$median");
          windowCount += 1L;
          if (windowCount == numberOfWindows) {
            latch.countDown();
            sendUpstream(WindowerMessage.Stop(partitionId));
            log.debug("Done!");
          } else {
            log.debug(s"Got ${windowCount}/${numberOfWindows} windows.");
          }
        }
    }

    private def sendUpstream(event: KompicsEvent): Unit = {
      val msg = NetMessage.viaTCP(selfAddr, upstream)(event);
      trigger(msg, net);
    }
  }

  object StreamingWindowsSerializer extends Serializer {
    import WindowerMessage._;

    val NAME = "streamingwindows";

    private val START_FLAG: Byte = 1;
    private val STOP_FLAG: Byte = 2;
    private val EVENT_FLAG: Byte = 3;
    private val READY_FLAG: Byte = 4;
    private val WINDOW_FLAG: Byte = 5;
    private val FLUSH_FLAG: Byte = 6;
    private val FLUSHED_FLAG: Byte = 7;

    def register(): Unit = {
      Serializers.register(this, NAME);
      Serializers.register(classOf[WindowerMessage.Start], NAME);
      Serializers.register(classOf[WindowerMessage.Stop], NAME);
      Serializers.register(classOf[WindowerMessage.Event], NAME);
      Serializers.register(classOf[WindowerMessage.Ready], NAME);
      Serializers.register(classOf[WindowerMessage.WindowAggregate], NAME);
      Serializers.register(classOf[WindowerMessage.Flush], NAME);
      Serializers.register(classOf[WindowerMessage.Flushed], NAME);
    }

    override def identifier(): Int = se.kth.benchmarks.kompics.SerializerIds.S_STREAMINGWINDOWS;

    override def toBinary(o: Any, buf: ByteBuf): Unit = {
      o match {
        case Start(pid) => {
          buf.writeByte(START_FLAG);
          buf.writeInt(pid);
        }
        case Stop(pid) => {
          buf.writeByte(STOP_FLAG);
          buf.writeInt(pid);
        }
        case Event(ts, pid, value) => {
          buf.writeByte(EVENT_FLAG);
          buf.writeLong(ts);
          buf.writeInt(pid);
          buf.writeLong(value);
        }
        case Ready(pid, quota) => {
          buf.writeByte(READY_FLAG);
          buf.writeInt(pid);
          buf.writeLong(quota);
        }
        case WindowAggregate(ts, pid, value) => {
          buf.writeByte(WINDOW_FLAG);
          buf.writeLong(ts);
          buf.writeInt(pid);
          buf.writeDouble(value);
        }
        case Flush(pid) => {
          buf.writeByte(FLUSH_FLAG);
          buf.writeInt(pid);
        }
        case Flushed(pid) => {
          buf.writeByte(FLUSHED_FLAG);
          buf.writeInt(pid);
        }
      }
    }

    override def fromBinary(buf: ByteBuf, hint: Optional[Object]): Object = {
      val flag = buf.readByte();

      flag match {
        case START_FLAG => {
          val pid = buf.readInt();
          Start(pid)
        }
        case STOP_FLAG => {
          val pid = buf.readInt();
          Stop(pid)
        }
        case EVENT_FLAG => {
          val ts = buf.readLong();
          val pid = buf.readInt();
          val value = buf.readLong();
          Event(ts, pid, value)
        }
        case READY_FLAG => {
          val pid = buf.readInt();
          val quota = buf.readLong();
          Ready(pid, quota)
        }
        case WINDOW_FLAG => {
          val ts = buf.readLong();
          val pid = buf.readInt();
          val value = buf.readDouble();
          WindowAggregate(ts, pid, value)
        }
        case FLUSH_FLAG => {
          val pid = buf.readInt();
          Flush(pid)
        }
        case FLUSHED_FLAG => {
          val pid = buf.readInt();
          Flushed(pid)
        }
      }
    }
  }
}
