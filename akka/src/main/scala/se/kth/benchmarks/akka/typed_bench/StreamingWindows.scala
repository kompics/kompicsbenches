package se.kth.benchmarks.akka.typed_bench

import akka.actor.typed.{ActorRef, ActorRefResolver, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.scaladsl.AskPattern._
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
import se.kth.benchmarks.akka.typed_bench.NetThroughputPingPong.ActorReference
import _root_.akka.NotUsed

object StreamingWindows extends DistributedBenchmark {

  case class WindowerConfig(windowSize: Duration,
                            batchSize: Long,
                            amplification: Long,
                            upstreamActorPaths: List[ActorReference])
  case class WindowerRefs(actorPaths: List[ActorReference])

  override type MasterConf = StreamingWindowsRequest;
  override type ClientConf = WindowerConfig;
  override type ClientData = WindowerRefs;

  val RESOLVE_TIMEOUT: FiniteDuration = 6.seconds;
  val FLUSH_TIMEOUT: FiniteDuration = 1.minute;

  val serializers = SerializerBindings
    .empty()
    .addSerializer[StreamingWindowsSerializer](StreamingWindowsSerializer.NAME)
    .addBinding[Windower.Start](StreamingWindowsSerializer.NAME)
    .addBinding[Windower.Stop.type](StreamingWindowsSerializer.NAME)
    .addBinding[Windower.Event](StreamingWindowsSerializer.NAME)
    .addBinding[StreamSource.Ready](StreamingWindowsSerializer.NAME)
    .addBinding[StreamSink.WindowAggregate](StreamingWindowsSerializer.NAME)
    .addBinding[Windower.Flush.type](StreamingWindowsSerializer.NAME)
    .addBinding[StreamSource.Flushed.type](StreamingWindowsSerializer.NAME);

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
    val upstream = parts(3).split(";").map(ActorReference).toList;
    WindowerConfig(windowSize, batchSize, amplification, upstream)
  }
  override def strToClientData(str: String): Try[ClientData] =
    Success(WindowerRefs(str.split(";").map(ActorReference).toList));

  override def clientConfToString(c: ClientConf): String = {
    s"${c.windowSize.toMillis},${c.batchSize},${c.amplification},${c.upstreamActorPaths.map(_.actorPath).mkString(";")}"
  }
  override def clientDataToString(d: ClientData): String = d.actorPaths.map(_.actorPath).mkString(";");

  private def matchingPath(path: String): String = {
    val last = path.split("/").last;
    val legal = last.split("#").head;
    legal
  }

  class MasterImpl extends Master with StrictLogging {
    import MasterSupervisor._;

    private var numberOfPartitions: Int = -1;
    private var batchSize: Long = -1L;
    private var windowSize: Duration = Duration.Zero;
    private var numberOfWindows: Long = -1L;
    private var windowSizeAmplification: Long = -1L;

    private var system: ActorSystem[SystemMessage] = null;
    private var latch: CountDownLatch = null;

    override def setup(c: MasterConf, _meta: DeploymentMetaData): Try[ClientConf] = Try {

      logger.info(s"Setting up master: $c");
      this.numberOfPartitions = c.numberOfPartitions;
      this.batchSize = c.batchSize;
      this.windowSizeAmplification = c.windowSizeAmplification;
      this.windowSize = Duration(c.windowSize);
      this.numberOfWindows = c.numberOfWindows;
      this.system = ActorSystemProvider
        .newRemoteTypedActorSystem[SystemMessage](MasterSupervisor(),
                                                  "streamingwindows_master_supervisor",
                                                  Runtime.getRuntime.availableProcessors(),
                                                  serializers);

      val f: Future[List[ActorReference]] = {
        implicit val timeout = Timeout(RESOLVE_TIMEOUT);
        implicit val scheduler = system.scheduler; // don't leak the scheudler
        this.system.ask(replyTo => StartSources(replyTo, numberOfPartitions));
      };
      val sourcesStringified = Await.result(f, RESOLVE_TIMEOUT);
      WindowerConfig(windowSize, batchSize, windowSizeAmplification, sourcesStringified)
    }
    override def prepareIteration(d: List[ClientData]): Unit = {
      implicit val ec = scala.concurrent.ExecutionContext.global;

      logger.trace("Preparing iteration");

      val client = d.head;
      val windowers = client.actorPaths;
      this.latch = new CountDownLatch(this.numberOfPartitions);
      val f: Future[OperationSucceeded.type] = {
        implicit val timeout = Timeout(RESOLVE_TIMEOUT);
        implicit val scheduler = system.scheduler; // don't leak the scheudler
        this.system.ask(replyTo => StartSinks(replyTo, this.latch, windowers, this.numberOfWindows));
      };
      Await.result(f, RESOLVE_TIMEOUT);
    }
    override def runIteration(): Unit = {
      this.system ! RunIteration;
      this.latch.await();
    }
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      //   implicit val ec = scala.concurrent.ExecutionContext.global;
      //   implicit val timeout = Timeout(FLUSH_TIMEOUT);

      logger.trace("Cleaning up iteration");
      val resetFuture: Future[OperationSucceeded.type] = {
        implicit val timeout = Timeout(FLUSH_TIMEOUT);
        implicit val scheduler = system.scheduler; // don't leak the scheudler
        this.system.ask(replyTo => ResetSources(replyTo));
      };
      val stopFuture: Future[OperationSucceeded.type] = {
        implicit val timeout = Timeout(FLUSH_TIMEOUT);
        implicit val scheduler = system.scheduler; // don't leak the scheudler
        this.system.ask(replyTo => StopSinks(replyTo));
      };
      Await.result(stopFuture, RESOLVE_TIMEOUT);
      logger.trace("Flushing remaining messages on the channels");
      Await.result(resetFuture, FLUSH_TIMEOUT);

      logger.trace("Remaining messages are flushed out.");
      if (lastIteration) {
        if (this.system != null) {
          val stopFuture: Future[OperationSucceeded.type] = {
            implicit val timeout = Timeout(FLUSH_TIMEOUT);
            implicit val scheduler = system.scheduler; // don't leak the scheudler
            this.system.ask(replyTo => StopSources(replyTo));
          };
          Await.result(stopFuture, RESOLVE_TIMEOUT);
          this.system.terminate();
          Await.result(this.system.whenTerminated, RESOLVE_TIMEOUT);
          this.system = null;
        }
        logger.info("Completed final cleanup.");
      }
    }
  }

  object MasterSupervisor {
    sealed trait SystemMessage;
    case class StartSources(sources: ActorRef[List[ActorReference]], numberOfPartitions: Int) extends SystemMessage;
    case class StartSinks(replyTo: ActorRef[OperationSucceeded.type],
                          latch: CountDownLatch,
                          windowers: List[ActorReference],
                          numberOfWindows: Long)
        extends SystemMessage;
    case object RunIteration extends SystemMessage;
    case class ResetSources(replyTo: ActorRef[OperationSucceeded.type]) extends SystemMessage;
    case class SourceReset(success: Boolean, source: ActorRef[Nothing]) extends SystemMessage;
    case class StopSources(replyTo: ActorRef[OperationSucceeded.type]) extends SystemMessage;
    case class StopSinks(replyTo: ActorRef[OperationSucceeded.type]) extends SystemMessage;
    case object OperationSucceeded;

    def apply(): Behavior[SystemMessage] = Behaviors.setup(context => new MasterSupervisor(context))
  }

  class MasterSupervisor(context: ActorContext[MasterSupervisor.SystemMessage])
      extends AbstractBehavior[MasterSupervisor.SystemMessage](context) {
    import MasterSupervisor._;

    context.setLoggerName(this.getClass);
    val log = context.log;
    val resolver = ActorRefResolver(context.system);

    private var sources: List[(Int, ActorRef[StreamSource.SourceMsg])] = Nil;
    private var sinks: List[ActorRef[StreamSink.SinkMsg]] = Nil;
    private var resetResponses: List[ActorRef[Nothing]] = Nil;
    private var resetReplyTo: Option[ActorRef[OperationSucceeded.type]] = None;

    private def getWindowerRef(a: ActorReference): ActorRef[Windower.WindowerMsg] = {
      resolver.resolveActorRef(a.actorPath)
    }

    override def onMessage(msg: SystemMessage): Behavior[SystemMessage] = {
      msg match {
        case StartSources(replyTo, numberOfPartitions) => {
          this.sources = (for (pid <- (0 until numberOfPartitions)) yield {
            val source = context.spawn(StreamSource(pid), s"source$pid");
            (pid, source)
          }).toList;
          val sourcesStringified = this.sources.map {
            case (_, ref) => ActorReference(resolver.toSerializationFormat(ref))
          };
          replyTo ! sourcesStringified;
        }
        case StartSinks(replyTo, latch, windowers, numberOfWindows) => {
          this.sinks = (for (windower <- windowers) yield {
            val last = matchingPath(windower.actorPath);
            val windowerRef = getWindowerRef(windower);
            val source = context.spawn(StreamSink(latch, numberOfWindows, windowerRef), s"sink-for-$last");
            source
          }).toList;
          replyTo ! OperationSucceeded;
        }
        case RunIteration => {
          this.sinks.foreach(sink => sink ! StreamSink.Start);
        }
        case ResetSources(replyTo) => {
          implicit val timeout: Timeout = Timeout(FLUSH_TIMEOUT);
          this.resetReplyTo = Some(replyTo);
          this.sources.map {
            case (_, source) =>
              context.ask(source, StreamSource.Reset) {
                case Success(_) => SourceReset(true, source.narrow[Nothing])
                case Failure(ex) => {
                  log.error("StreamSource did not reply with Flushed!", ex);
                  SourceReset(false, source.narrow[Nothing])
                }
              }
          }
        }
        case SourceReset(success, source) if success => {
          this.resetResponses ::= source;
          if (this.resetResponses.size == this.sources.size) {
            this.resetReplyTo.get ! OperationSucceeded;
            this.resetReplyTo = None;
            this.resetResponses = Nil;
          }
        }
        case SourceReset(success, source) if !success => () // ignore
        case StopSources(replyTo) => {
          this.sources.foreach { case (_, source) => context.stop(source) };
          this.sources = Nil;
          replyTo ! OperationSucceeded;
        }
        case StopSinks(replyTo) => {
          this.sinks.foreach(sink => context.stop(sink));
          this.sinks = Nil;
          replyTo ! OperationSucceeded;
        }
      }
      this
    }
  }

  class ClientImpl extends Client with StrictLogging {
    import ClientSupervisor._;

    private var windowSize: Duration = Duration.Zero;
    private var batchSize: Long = -1L;
    private var amplification: Long = -1L;

    private var system: ActorSystem[SystemMessage] = null;

    override def setup(c: ClientConf): ClientData = {
      logger.info(s"Setting up client: $c");
      implicit val ec = scala.concurrent.ExecutionContext.global;

      this.windowSize = c.windowSize;
      this.batchSize = c.batchSize;
      this.amplification = c.amplification;
      this.system = ActorSystemProvider
        .newRemoteTypedActorSystem[SystemMessage](ClientSupervisor(),
                                                  "streamingwindows_client_supervisor",
                                                  Runtime.getRuntime.availableProcessors(),
                                                  serializers);

      val f: Future[WindowerRefs] = {
        implicit val timeout = Timeout(RESOLVE_TIMEOUT);
        implicit val scheduler = system.scheduler; // don't leak the scheduler
        this.system.ask(replyTo => StartWindowers(replyTo, c));
      };
      Await.result(f, RESOLVE_TIMEOUT)
    }
    override def prepareIteration(): Unit = {
      // nothing
      logger.debug("Preparing iteration.");
    }
    override def cleanupIteration(lastIteration: Boolean): Unit = {
      logger.debug("Cleaning iteration.");
      if (lastIteration) {
        if (this.system != null) {
          val stopFuture: Future[OperationSucceeded.type] = {
            implicit val timeout = Timeout(FLUSH_TIMEOUT);
            implicit val scheduler = system.scheduler; // don't leak the scheudler
            this.system.ask(replyTo => StopWindowers(replyTo));
          };
          Await.result(stopFuture, RESOLVE_TIMEOUT);
          this.system.terminate();
          Await.result(this.system.whenTerminated, RESOLVE_TIMEOUT);
          this.system = null;
        }
        logger.info("Final cleanup complete!");
      }
    }
  }

  object ClientSupervisor {
    sealed trait SystemMessage;
    case class StartWindowers(replyTo: ActorRef[WindowerRefs], conf: WindowerConfig) extends SystemMessage;
    case class StopWindowers(replyTo: ActorRef[OperationSucceeded.type]) extends SystemMessage;

    case object OperationSucceeded;

    def apply(): Behavior[SystemMessage] = Behaviors.setup(context => new ClientSupervisor(context))
  }

  class ClientSupervisor(context: ActorContext[ClientSupervisor.SystemMessage])
      extends AbstractBehavior[ClientSupervisor.SystemMessage](context) {
    import ClientSupervisor._;

    context.setLoggerName(this.getClass);
    val log = context.log;
    val resolver = ActorRefResolver(context.system);

    private var sources: List[ActorRef[StreamSource.SourceMsg]] = Nil;
    private var windowers: List[ActorRef[Windower.WindowerMsg]] = Nil;

    private def getSourceRef(a: ActorReference): ActorRef[StreamSource.SourceMsg] = {
      resolver.resolveActorRef(a.actorPath)
    }

    override def onMessage(msg: SystemMessage): Behavior[SystemMessage] = {
      msg match {
        case StartWindowers(replyTo, conf) => {
          this.sources = conf.upstreamActorPaths.map(getSourceRef);
          this.windowers = for ((source, sourcePath) <- this.sources.zip(conf.upstreamActorPaths)) yield {
            val last = matchingPath(sourcePath.actorPath);
            context.spawn(Windower(conf.windowSize, conf.batchSize, conf.amplification, source), s"windower-for-$last")
          };
          val windowerPaths = this.windowers.map {
            case ref => ActorReference(resolver.toSerializationFormat(ref))
          };
          replyTo ! WindowerRefs(windowerPaths);
        }
        case StopWindowers(replyTo) => {
          this.windowers.foreach { case windower => context.stop(windower) };
          this.windowers = Nil;
          replyTo ! OperationSucceeded;
        }
      }
      this
    }
  }

  object Windower {
    sealed trait WindowerMsg;
    case class Start(downstreamRef: ActorReference) extends WindowerMsg;
    case object Stop extends WindowerMsg;
    case class Event(ts: Long, partitionId: Int, value: Long) extends WindowerMsg;
    case object Flush extends WindowerMsg;

    def apply(windowSize: Duration,
              batchSize: Long,
              amplification: Long,
              upstream: ActorRef[StreamSource.SourceMsg]): Behavior[WindowerMsg] =
      Behaviors.setup(ctx => new Windower(ctx, windowSize, batchSize, amplification, upstream));
  }

  class Windower(context: ActorContext[Windower.WindowerMsg],
                 windowSize: Duration,
                 val batchSize: Long,
                 val amplification: Long,
                 val upstream: ActorRef[StreamSource.SourceMsg])
      extends AbstractBehavior[Windower.WindowerMsg](context) {
    import Windower._;

    context.setLoggerName(this.getClass);
    val log = context.log;
    val resolver = ActorRefResolver(context.system);
    val selfRef = ActorReference(resolver.toSerializationFormat(context.self));

    val windowSizeMS = windowSize.toMillis;
    require(windowSizeMS > 0L);
    val readyMark = batchSize / 2;

    private var downstream: Option[ActorRef[StreamSink.SinkMsg]] = None;

    private val currentWindow = mutable.ArrayBuffer.empty[Long];
    private var windowStartTS: Long = 0L;

    private var receivedSinceReady: Long = 0L;
    private var running: Boolean = false;

    private def getSinkRef(a: ActorReference): ActorRef[StreamSink.SinkMsg] = {
      resolver.resolveActorRef(a.actorPath)
    }

    override def onMessage(msg: WindowerMsg): Behavior[WindowerMsg] = {
      msg match {
        case Start(downstreamRef) if !running => {
          log.debug("Got Start");
          downstream = Some(getSinkRef(downstreamRef));
          running = true;
          receivedSinceReady = 0L;
          upstream ! StreamSource.Ready(batchSize, selfRef);
        }
        case Start(_) if running => ??? // da fuq?
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
          upstream ! StreamSource.Flushed;
        }
      }
      this
    }

    private def triggerWindow(partitionId: Int): Unit = {
      log.debug(s"Triggering window with ${currentWindow.size} messages.");
      if (currentWindow.isEmpty) {
        log.warn("Windows should not be empty in the benchmark!");
        return;
      }
      val median = Statistics.medianFromUnsorted(currentWindow);
      currentWindow.clear();
      downstream.get ! StreamSink.WindowAggregate(windowStartTS, partitionId, median);
    }
    private def manageReady(): Unit = {
      receivedSinceReady += 1L;
      if (receivedSinceReady > readyMark) {
        log.debug("Sending Ready");
        upstream ! StreamSource.Ready(receivedSinceReady, selfRef);
        receivedSinceReady = 0L;
      }
    }
  }

  object StreamSource {
    sealed trait SourceMsg;
    case object Next extends SourceMsg;
    case class Reset(replyTo: ActorRef[Flushed.type]) extends SourceMsg;
    case class Ready(numMessages: Long, sendTo: ActorReference) extends SourceMsg;
    case object Flushed extends SourceMsg;

    def apply(pid: Int): Behavior[SourceMsg] = Behaviors.setup(ctx => new StreamSource(ctx, pid));
  }
  class StreamSource(context: ActorContext[StreamSource.SourceMsg], val partitionId: Int)
      extends AbstractBehavior[StreamSource.SourceMsg](context) {
    import StreamSource._;

    context.setLoggerName(this.getClass);
    val log = context.log;
    val resolver = ActorRefResolver(context.system);

    val random = new java.util.Random(partitionId);

    private var downstream: Option[ActorRef[Windower.WindowerMsg]] = None;

    private var remaining: Long = 0L;
    private var currentTS: Long = 0L;

    private var flushing: Boolean = false;

    private var replyOnFlushed: Option[ActorRef[Flushed.type]] = None;

    private def getWindowerRef(a: ActorReference): ActorRef[Windower.WindowerMsg] = {
      resolver.resolveActorRef(a.actorPath)
    }

    override def onMessage(msg: SourceMsg): Behavior[SourceMsg] = {
      msg match {
        case Ready(batchSize, sendTo) if !flushing => {
          log.debug("Got Ready!");
          this.remaining += batchSize;
          this.downstream = Some(getWindowerRef(sendTo));
          send();
        }
        case Ready(_, _) if flushing        => () // drop
        case StreamSource.Next if !flushing => send()
        case StreamSource.Next if flushing  => () // drop
        case StreamSource.Reset(replyTo) if !flushing => {
          log.debug("Got Reset");
          this.flushing = true;
          this.replyOnFlushed = Some(replyTo);
          this.downstream.get ! Windower.Flush;
          this.currentTS = 0L;
          this.remaining = 0L;
          this.downstream = None;
        }
        case StreamSource.Reset(_) if flushing => ??? // shouldn't happen!
        case Flushed if flushing => {
          log.debug("Got Flushed");
          this.replyOnFlushed.get ! Flushed;
          this.replyOnFlushed = None;
          this.flushing = false;
        }
        case Flushed if !flushing => ??? // shouldn't happen!
      }
      this
    }

    private def send(): Unit = {
      if (remaining > 0L) {
        downstream.get ! Windower.Event(currentTS, partitionId, random.nextLong());
        currentTS += 1L;
        remaining -= 1L;
        if (remaining > 0L) {
          context.self ! StreamSource.Next;
        } else {
          log.debug("Waiting for Ready...");
        }
      }
    }
  }

  object StreamSink {
    sealed trait SinkMsg;
    case object Start extends SinkMsg;
    case class WindowAggregate(ts: Long, partitionId: Int, value: Double) extends SinkMsg;

    def apply(latch: CountDownLatch,
              numberOfWindows: Long,
              upstream: ActorRef[Windower.WindowerMsg]): Behavior[SinkMsg] =
      Behaviors.setup(ctx => new StreamSink(ctx, latch, numberOfWindows, upstream));
  }
  class StreamSink(context: ActorContext[StreamSink.SinkMsg],
                   val latch: CountDownLatch,
                   val numberOfWindows: Long,
                   val upstream: ActorRef[Windower.WindowerMsg])
      extends AbstractBehavior[StreamSink.SinkMsg](context) {
    import StreamSink._;

    context.setLoggerName(this.getClass);
    val log = context.log;
    val resolver = ActorRefResolver(context.system);
    val selfRef = ActorReference(resolver.toSerializationFormat(context.self));

    private var windowCount: Long = 0L;

    override def onMessage(msg: SinkMsg): Behavior[SinkMsg] = {
      msg match {
        case Start => {
          log.debug("Got Start!");
          upstream ! Windower.Start(selfRef);
        }
        case WindowAggregate(_, _, median) => {
          log.debug(s"Got window with median=$median");
          windowCount += 1L;
          if (windowCount == numberOfWindows) {
            latch.countDown();
            upstream ! Windower.Stop;
            log.debug("Done!");
          } else {
            log.debug(s"Got ${windowCount}/${numberOfWindows} windows.");
          }
        }
      }
      this
    }
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
    import Windower.{Event, Flush, Start, Stop};
    import StreamSource.{Flushed, Ready};
    import StreamSink.WindowAggregate;
    import java.nio.{ByteBuffer, ByteOrder};
    import _root_.akka.util.ByteString;

    implicit val order = ByteOrder.BIG_ENDIAN;

    override def identifier: Int = SerializerIds.STREAMINGWINDOWS;
    override def includeManifest: Boolean = false;
    override def toBinary(o: AnyRef): Array[Byte] = {
      o match {
        case Start(downstreamRef) => {
          val bs = ByteString.createBuilder;
          bs.putByte(START_FLAG);
          SerUtils.stringIntoByteString(bs, downstreamRef.actorPath);
          bs.result().toArray
        }
        case Stop => Array(STOP_FLAG)
        case Event(ts, pid, value) => {
          val bs = ByteString.createBuilder;
          bs.putByte(EVENT_FLAG);
          bs.putLong(ts);
          bs.putInt(pid);
          bs.putLong(value);
          bs.result().toArray
        }
        case Ready(quota, downstreamRef) => {
          val bs = ByteString.createBuilder;
          bs.putByte(READY_FLAG);
          bs.putLong(quota);
          SerUtils.stringIntoByteString(bs, downstreamRef.actorPath);
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
        case START_FLAG => {
          val src = ActorReference(SerUtils.stringFromByteBuffer(buf));
          Start(src)
        }
        case STOP_FLAG => Stop
        case EVENT_FLAG => {
          val ts = buf.getLong();
          val pid = buf.getInt();
          val value = buf.getLong();
          Event(ts, pid, value)
        }
        case READY_FLAG => {
          val quota = buf.getLong();
          val src = ActorReference(SerUtils.stringFromByteBuffer(buf));
          Ready(quota, src)
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
