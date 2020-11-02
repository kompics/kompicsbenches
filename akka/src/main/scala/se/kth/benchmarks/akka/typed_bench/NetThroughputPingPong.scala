package se.kth.benchmarks.akka.typed_bench

import java.util.concurrent.CountDownLatch
import akka.actor.typed.{ActorRef, ActorRefResolver, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.scaladsl.AskPattern._
import akka.serialization.Serializer
import akka.util.{ByteString, Timeout}
import kompics.benchmarks.benchmarks.ThroughputPingPongRequest
import scalapb.GeneratedMessage
import se.kth.benchmarks.{DeploymentMetaData, DistributedBenchmark}
import se.kth.benchmarks.akka.{ActorSystemProvider, SerializerBindings, SerializerIds}
import se.kth.benchmarks.akka.typed_bench.NetThroughputPingPong.ClientSystemSupervisor.StartPongers
import se.kth.benchmarks.akka.typed_bench.NetThroughputPingPong.SystemSupervisor.{OperationSucceeded, RunIteration, StartPingers, StopPingers, SystemMessage}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}
import com.typesafe.scalalogging.StrictLogging

import scala.language.postfixOps

object NetThroughputPingPong extends DistributedBenchmark {
  case class ActorReference(actorPath: String)
  case class ClientRefs(actorPaths: List[String])
  case class ClientParams(numPongers: Int, staticOnly: Boolean)

  override type MasterConf = ThroughputPingPongRequest;
  override type ClientConf = ClientParams;
  override type ClientData = ClientRefs;

  val serializers = SerializerBindings
    .empty()
    .addSerializer[PingPongSerializer](PingPongSerializer.NAME)
    .addBinding[StaticPing](PingPongSerializer.NAME)
    .addBinding[StaticPong.type](PingPongSerializer.NAME)
    .addBinding[Ping](PingPongSerializer.NAME)
    .addBinding[Pong](PingPongSerializer.NAME);

  class MasterImpl extends Master with StrictLogging {
    private var numMsgs = -1L;
    private var numPairs = -1;
    private var pipeline = -1L;
    private var staticOnly = true;
    private var system: ActorSystem[SystemMessage] = null
    private var latch: CountDownLatch = null;

    override def setup(c: MasterConf, _meta: DeploymentMetaData): Try[ClientConf] = Try {
      logger.info("Setting up Master");
      this.system =
        ActorSystemProvider.newRemoteTypedActorSystem[SystemMessage](SystemSupervisor(),
                                                                     "nettpingpong_supervisor",
                                                                     Runtime.getRuntime.availableProcessors(),
                                                                     serializers);
      this.numMsgs = c.messagesPerPair;
      this.numPairs = c.parallelism;
      this.pipeline = c.pipelineSize;
      this.staticOnly = c.staticOnly;
      ClientParams(numPairs, staticOnly)
    }

    override def prepareIteration(d: List[ClientData]): Unit = {
      logger.debug("Preparing iteration");
      latch = new CountDownLatch(numPairs);
      implicit val timeout: Timeout = 3.seconds;
      implicit val scheduler = system.scheduler;
      val f: Future[OperationSucceeded.type] =
        system.ask(ref => StartPingers(ref, latch, numMsgs, pipeline, staticOnly, d.head));
      implicit val ec = scala.concurrent.ExecutionContext.global;
      Await.result(f, 3 seconds);
    }

    override def runIteration(): Unit = {
      system ! RunIteration;
      latch.await();
    }

    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      logger.debug("Cleaning up pinger side");
      if (latch != null) {
        latch = null;
      }
      implicit val timeout: Timeout = 3.seconds;
      implicit val scheduler = system.scheduler;
      val f: Future[OperationSucceeded.type] = system.ask(ref => StopPingers(ref));
      implicit val ec = scala.concurrent.ExecutionContext.global;
      Await.result(f, 3 seconds);
      if (lastIteration) {
        system.terminate();
        Await.ready(system.whenTerminated, 5.seconds);
        system = null;
        logger.info("Cleaned up Master");
      }

    }
  }

  class ClientImpl extends Client with StrictLogging {
    private var system: ActorSystem[StartPongers] = null;

    override def setup(c: ClientParams): ClientRefs = {
      logger.info("Setting up Client");
      system = ActorSystemProvider.newRemoteTypedActorSystem[StartPongers](ClientSystemSupervisor(),
                                                                           "nettpingpong_clientsupervisor",
                                                                           1,
                                                                           serializers);
      implicit val timeout: Timeout = 3.seconds;
      implicit val scheduler = system.scheduler;
      val f: Future[ClientRefs] = system.ask(ref => StartPongers(ref, c.staticOnly, c.numPongers));
      implicit val ec = scala.concurrent.ExecutionContext.global;
      val ready = Await.ready(f, 5.seconds);
      ready.value.get match {
        case Success(pongerPaths) => {
          logger.trace(s"Ponger Paths are${pongerPaths.actorPaths.mkString}");
          pongerPaths
        }
        case Failure(e) => ClientRefs(List.empty)
      }
    }
    override def prepareIteration(): Unit = {
      // nothing
      logger.debug("Preparing ponger iteration");
    }
    override def cleanupIteration(lastIteration: Boolean): Unit = {
      logger.debug("Cleaning up ponger side");
      if (lastIteration) {
        system.terminate();
        Await.ready(system.whenTerminated, 5.second);
        system = null;
        logger.info("Cleaned up Client");
      }
    }
  }

  override def newMaster(): Master = new MasterImpl()

  override def msgToMasterConf(msg: scalapb.GeneratedMessage): Try[MasterConf] = Try {
    msg.asInstanceOf[ThroughputPingPongRequest]
  };

  override def newClient(): NetThroughputPingPong.Client = new ClientImpl()

  override def strToClientConf(str: String): Try[ClientConf] = Try {
    val split = str.split(",");
    val num = split(0).toInt;
    val staticOnly = split(1) match {
      case "true"  => true
      case "false" => false
    };
    ClientParams(num, staticOnly)
  };
  override def strToClientData(str: String): Try[ClientData] = Try {
    val l = str.split(",").toList;
    ClientRefs(l)
  };

  override def clientConfToString(c: ClientConf): String = s"${c.numPongers},${c.staticOnly}";

  override def clientDataToString(d: ClientData): String = d.actorPaths.mkString(",");

  object ClientSystemSupervisor {
    case class StartPongers(replyTo: ActorRef[ClientRefs], staticOnly: Boolean, numPongers: Int)
    def apply(): Behavior[StartPongers] = Behaviors.setup(context => new ClientSystemSupervisor(context))
  }

  class ClientSystemSupervisor(context: ActorContext[StartPongers]) extends AbstractBehavior[StartPongers](context) {
    val resolver = ActorRefResolver(context.system)

    private def getPongerPaths[T](refs: List[ActorRef[T]]): List[String] = {
      for (pongerRef <- refs) yield resolver.toSerializationFormat(pongerRef)
    }

    override def onMessage(msg: StartPongers): Behavior[StartPongers] = {
      if (msg.staticOnly) {
        val static_pongers = (1 to msg.numPongers).map(i => context.spawn(StaticPonger(), s"typed_ponger$i")).toList
        msg.replyTo ! ClientRefs(getPongerPaths[StaticPing](static_pongers))
      } else {
        val pongers = (1 to msg.numPongers).map(i => context.spawn(Ponger(), s"typed_ponger$i")).toList
        msg.replyTo ! ClientRefs(getPongerPaths[Ping](pongers))
      }
      this
    }
  }

  object SystemSupervisor {
    sealed trait SystemMessage
    case class StartPingers(replyTo: ActorRef[OperationSucceeded.type],
                            latch: CountDownLatch,
                            numMsgs: Long,
                            pipeline: Long,
                            staticOnly: Boolean,
                            pongers: ClientRefs)
        extends SystemMessage
    case object RunIteration extends SystemMessage
    case class StopPingers(replyTo: ActorRef[OperationSucceeded.type]) extends SystemMessage
    case object OperationSucceeded

    def apply(): Behavior[SystemMessage] = Behaviors.setup(context => new SystemSupervisor(context))
  }

  class SystemSupervisor(context: ActorContext[SystemMessage]) extends AbstractBehavior[SystemMessage](context) {
    val resolver = ActorRefResolver(context.system);
    var pingers: List[ActorRef[MsgForPinger]] = null;
    var static_pingers: List[ActorRef[MsgForStaticPinger]] = null;
    var run_id: Int = -1;
    var staticOnly = true;

    override def onMessage(msg: SystemMessage): Behavior[SystemMessage] = {
      msg match {
        case s: StartPingers => {
          this.staticOnly = s.staticOnly
          if (staticOnly) {
            static_pingers = s.pongers.actorPaths.zipWithIndex.map {
              case (static_ponger, i) =>
                context.spawn(StaticPinger(s.latch, s.numMsgs, s.pipeline, static_ponger),
                              s"typed_staticpinger${run_id}_$i")
            }
          } else {
            pingers = s.pongers.actorPaths.zipWithIndex.map {
              case (ponger, i) =>
                context.spawn(Pinger(s.latch, s.numMsgs, s.pipeline, ponger), s"typed_pinger${run_id}_$i")
            }
          }
          s.replyTo ! OperationSucceeded
          this
        }
        case RunIteration => {
          if (staticOnly) static_pingers.foreach(static_pinger => static_pinger ! RunStaticPinger)
          else pingers.foreach(pinger => pinger ! RunPinger)
          this
        }
        case StopPingers(replyTo) => {
          if (staticOnly) {
            if (static_pingers.nonEmpty) {
              static_pingers.foreach(static_pinger => context.stop(static_pinger))
              static_pingers = List.empty
            }
          } else {
            if (pingers.nonEmpty) {
              pingers.foreach(pinger => context.stop(pinger))
              pingers = List.empty
            }
          }
          replyTo ! OperationSucceeded
          this
        }
      }
    }
  }

  sealed trait MsgForPinger
  case class Ping(src: ActorReference, index: Long)
  case class Pong(index: Long) extends MsgForPinger
  case object RunPinger extends MsgForPinger

  object Pinger {
    def apply(latch: CountDownLatch, count: Long, pipeline: Long, ponger: String): Behavior[MsgForPinger] =
      Behaviors.setup(context => new Pinger(context, latch, count, pipeline, ponger))
  }

  class Pinger(context: ActorContext[MsgForPinger],
               latch: CountDownLatch,
               count: Long,
               pipeline: Long,
               pongerPath: String)
      extends AbstractBehavior[MsgForPinger](context) {
    val resolver = ActorRefResolver(context.system)
    val selfRef = ActorReference(resolver.toSerializationFormat(context.self))
    val ponger: ActorRef[Ping] = resolver.resolveActorRef(pongerPath)

    var sentCount = 0L;
    var recvCount = 0L;

    override def onMessage(msg: MsgForPinger): Behavior[MsgForPinger] = {
      msg match {
        case RunPinger => {
          var pipelined = 0L;
          while (pipelined < pipeline && sentCount < count) {
            ponger ! Ping(selfRef, sentCount)
            pipelined += 1L;
            sentCount += 1L;
          }
        }
        case Pong(_) => {
          recvCount += 1L;
          if (recvCount < count) {
            if (sentCount < count) {
              ponger ! Ping(selfRef, sentCount);
              sentCount += 1L;
            }
          } else {
            latch.countDown();
          }
        }
      }
      this
    }
  }

  object Ponger {
    def apply(): Behavior[Ping] = Behaviors.setup(context => new Ponger(context))
  }

  class Ponger(context: ActorContext[Ping]) extends AbstractBehavior[Ping](context) {
    val resolver = ActorRefResolver(context.system)

    private def getPingerRef(a: ActorReference): ActorRef[MsgForPinger] = {
      resolver.resolveActorRef(a.actorPath)
    }

    override def onMessage(msg: Ping): Behavior[Ping] = {
      getPingerRef(msg.src) ! Pong(msg.index)
      this
    }
  }

  sealed trait MsgForStaticPinger
  case class StaticPing(src: ActorReference)
  case object StaticPong extends MsgForStaticPinger
  case object RunStaticPinger extends MsgForStaticPinger

  object StaticPinger {
    def apply(latch: CountDownLatch, count: Long, pipeline: Long, ponger: String): Behavior[MsgForStaticPinger] =
      Behaviors.setup(context => new StaticPinger(context, latch, count, pipeline, ponger))
  }

  class StaticPinger(context: ActorContext[MsgForStaticPinger],
                     latch: CountDownLatch,
                     count: Long,
                     pipeline: Long,
                     pongerPath: String)
      extends AbstractBehavior[MsgForStaticPinger](context) {
    val resolver = ActorRefResolver(context.system);
    val selfRef = ActorReference(resolver.toSerializationFormat(context.self));
    val ponger: ActorRef[StaticPing] = resolver.resolveActorRef(pongerPath);

    var sentCount = 0L;
    var recvCount = 0L;

    override def onMessage(msg: MsgForStaticPinger): Behavior[MsgForStaticPinger] = {
      msg match {
        case RunStaticPinger => {
          var pipelined = 0L;
          while (pipelined < pipeline && sentCount < count) {
            ponger ! StaticPing(selfRef);
            pipelined += 1L;
            sentCount += 1L;
          }
        }
        case StaticPong => {
          recvCount += 1L;
          if (recvCount < count) {
            if (sentCount < count) {
              ponger ! StaticPing(selfRef);
              sentCount += 1L;
            }
          } else {
            latch.countDown();
          }
        }
      }
      this
    }
  }

  object StaticPonger {
    def apply(): Behavior[StaticPing] = Behaviors.setup(context => new StaticPonger(context))
  }

  class StaticPonger(context: ActorContext[StaticPing]) extends AbstractBehavior[StaticPing](context) {
    val resolver = ActorRefResolver(context.system)

    private def getPingerRef(a: ActorReference): ActorRef[MsgForStaticPinger] = {
      resolver.resolveActorRef(a.actorPath)
    }

    override def onMessage(msg: StaticPing): Behavior[StaticPing] = {
      getPingerRef(msg.src) ! StaticPong
      this
    }
  }

  object PingPongSerializer {
    val NAME = "tpnetpingpong";

    private val STATIC_PING_FLAG: Byte = 1;
    private val STATIC_PONG_FLAG: Byte = 2;
    private val PING_FLAG: Byte = 3;
    private val PONG_FLAG: Byte = 4;
  }

  class PingPongSerializer extends Serializer {
    import se.kth.benchmarks.akka.SerUtils;
    import PingPongSerializer._
    import java.nio.{ByteBuffer, ByteOrder}

    implicit val order = ByteOrder.BIG_ENDIAN;

    override def identifier: Int = SerializerIds.TYPEDNETPPP
    override def includeManifest: Boolean = false

    override def toBinary(o: AnyRef): Array[Byte] = {
      o match {
        case Ping(src, index) => {
          val br = ByteString.createBuilder.putByte(PING_FLAG)
          br.putLong(index)
          SerUtils.stringIntoByteString(br, src.actorPath);
          br.result().toArray
        }
        case StaticPing(src) => {
          val br = ByteString.createBuilder.putByte(STATIC_PING_FLAG)
          SerUtils.stringIntoByteString(br, src.actorPath);
          br.result().toArray
        }
        case Pong(index) => ByteString.createBuilder.putByte(PONG_FLAG).putLong(index).result().toArray
        case StaticPong  => Array(STATIC_PONG_FLAG)
      }
    }

    override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
      val buf = ByteBuffer.wrap(bytes).order(order);
      val flag = buf.get;
      flag match {
        case PING_FLAG => {
          val index = buf.getLong;
          val src = ActorReference(SerUtils.stringFromByteBuffer(buf));
          Ping(src, index)
        }
        case STATIC_PING_FLAG => {
          val src = ActorReference(SerUtils.stringFromByteBuffer(buf));
          StaticPing(src)
        }
        case PONG_FLAG        => Pong(buf.getLong)
        case STATIC_PONG_FLAG => StaticPong
      }
    }
  }

}
