package se.kth.benchmarks.akka.typed_bench

import java.nio.ByteOrder
import java.util.concurrent.CountDownLatch
import akka.actor.typed.{ActorRef, ActorRefResolver, ActorSystem, Behavior}
import kompics.benchmarks.benchmarks.PingPongRequest
import se.kth.benchmarks.{DeploymentMetaData, DistributedBenchmark}
import se.kth.benchmarks.akka.{ActorSystemProvider, SerializerBindings, SerializerIds}
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.serialization.Serializer
import akka.util.{ByteString, Timeout}
import se.kth.benchmarks.akka.typed_bench.NetPingPong.SystemSupervisor.{OperationSucceeded, RunPinger, StartPinger, StopPinger, SystemMessage}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Success, Try}
import com.typesafe.scalalogging.StrictLogging

import scala.language.postfixOps

object NetPingPong extends DistributedBenchmark {

  case class ClientRef(actorPath: String)

  override type MasterConf = PingPongRequest
  override type ClientConf = Unit
  override type ClientData = ClientRef

  val serializers = SerializerBindings
    .empty()
    .addSerializer[PingPongSerializer](PingPongSerializer.NAME)
    .addBinding[Ping](PingPongSerializer.NAME)
    .addBinding[Pong.type](PingPongSerializer.NAME);

  class MasterImpl extends Master with StrictLogging {
    private var num = -1L;
    private var system: ActorSystem[SystemMessage] = null
    private var latch: CountDownLatch = null;

    override def setup(c: MasterConf, _meta: DeploymentMetaData): Try[ClientConf] = Try {
      logger.info("Setting up Master");
      this.num = c.numberOfMessages
      this.system = ActorSystemProvider
        .newRemoteTypedActorSystem[SystemMessage](SystemSupervisor(), "netpingpong_supervisor", 1, serializers)
    }

    override def prepareIteration(d: List[ClientRef]): Unit = {
      logger.debug("Preparing iteration");
      val ponger = d.head
      logger.trace(s"Resolving ponger path ${ponger}");
      latch = new CountDownLatch(1);
      implicit val timeout: Timeout = 3.seconds
      implicit val scheduler = system.scheduler
      val f: Future[OperationSucceeded.type] = system.ask(ref => StartPinger(ref, latch, num, ponger))
      implicit val ec = scala.concurrent.ExecutionContext.global;
      Await.result(f, 3 seconds)
    }

    override def runIteration(): Unit = {
      system ! RunPinger
      latch.await()
    }

    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      logger.debug("Cleaning up pinger side");
      if (latch != null) {
        latch = null;
      }
      implicit val timeout: Timeout = 3.seconds
      implicit val scheduler = system.scheduler
      val f: Future[OperationSucceeded.type] = system.ask(ref => StopPinger(ref))
      implicit val ec = scala.concurrent.ExecutionContext.global;
      Await.result(f, 3 seconds)
//      system ! SystemSupervisor.StopPinger
      if (lastIteration) {
        system.terminate()
//        implicit val ec = scala.concurrent.ExecutionContext.global;
        Await.ready(system.whenTerminated, 5 seconds)
        system = null
        logger.info("Cleanup up Master.");
      }
    }
  }

  class ClientImpl extends Client with StrictLogging {
    private var ponger: ActorSystem[Ping] = null

    override def setup(c: ClientConf): ClientRef = {
      logger.info("Setting up Client");
      ponger = ActorSystemProvider.newRemoteTypedActorSystem[Ping](Ponger(), "netpingpong", 1, serializers)
      val resolver = ActorRefResolver(ponger)
      val path = resolver.toSerializationFormat(ponger)
      logger.trace(s"Ponger path is $path")
      ClientRef(path)
    }

    override def prepareIteration(): Unit = {
      logger.debug("Preparing ponger iteration")
    }

    override def cleanupIteration(lastIteration: Boolean): Unit = {
      logger.debug("Cleaning up ponger side")
      if (lastIteration) {
        ponger.terminate()
        Await.ready(ponger.whenTerminated, 5 seconds)
        ponger = null
        logger.info("Cleaned up client.");
      }
    }
  }

  object SystemSupervisor {
    def apply(): Behavior[SystemMessage] = Behaviors.setup(context => new SystemSupervisor(context))

    sealed trait SystemMessage
    case class StartPinger(ref: ActorRef[OperationSucceeded.type], latch: CountDownLatch, num: Long, ponger: ClientRef)
        extends SystemMessage
    case object RunPinger extends SystemMessage
    case class StopPinger(ref: ActorRef[OperationSucceeded.type]) extends SystemMessage
    case object OperationSucceeded
  }

  class SystemSupervisor(context: ActorContext[SystemMessage]) extends AbstractBehavior[SystemMessage](context) {
    var pinger: ActorRef[MsgForPinger] = null
    var id: Int = -1

    override def onMessage(msg: SystemMessage): Behavior[SystemMessage] = {
      msg match {
        case start: StartPinger => {
          id += 1
          pinger = context
            .spawn[MsgForPinger](Pinger(start.latch, start.num, start.ponger), "typed_pinger".concat(id.toString))
          start.ref ! OperationSucceeded
          this
        }
        case stop: StopPinger => {
          if (pinger != null) {
            context.stop(pinger)
            pinger = null
          }
          stop.ref ! OperationSucceeded
          this
        }
        case RunPinger => {
          pinger ! Run
          this
        }
      }
    }
  }

  override def newMaster(): Master = new MasterImpl()

  override def msgToMasterConf(msg: scalapb.GeneratedMessage): Try[MasterConf] = Try {
    msg.asInstanceOf[PingPongRequest]
  };

  override def newClient(): Client = new ClientImpl()

  override def strToClientConf(str: String): Try[ClientConf] = Success(())

  override def strToClientData(str: String): Try[ClientData] = Success(ClientRef(str))

  override def clientConfToString(c: ClientConf): String = ""

  override def clientDataToString(d: ClientData): String = d.actorPath

  sealed trait MsgForPinger

  case class Ping(src: ClientRef);
  case object Pong extends MsgForPinger
  case object Run extends MsgForPinger

  object Pinger {
    def apply(latch: CountDownLatch, count: Long, ponger: ClientRef): Behavior[MsgForPinger] =
      Behaviors.setup(context => new Pinger(context, latch, count, ponger))
  }

  class Pinger(context: ActorContext[MsgForPinger], latch: CountDownLatch, count: Long, pongerRef: ClientRef)
      extends AbstractBehavior[MsgForPinger](context) {
    var countDown = count;
    val resolver = ActorRefResolver(context.system)
    val ponger: ActorRef[Ping] = resolver.resolveActorRef(pongerRef.actorPath)
    val selfPath: ClientRef = ClientRef(resolver.toSerializationFormat(context.self))

    override def onMessage(msg: MsgForPinger): Behavior[MsgForPinger] = {
      msg match {
        case Run => {
          ponger ! Ping(selfPath)
        }
        case Pong => {
          if (countDown > 0) {
            countDown -= 1
            ponger ! Ping(selfPath)
          } else latch.countDown()
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

    private def getPingerRef(c: ClientRef): ActorRef[MsgForPinger] = {
      resolver.resolveActorRef(c.actorPath)
    }

    override def onMessage(msg: Ping): Behavior[Ping] = {
      getPingerRef(msg.src) ! Pong
      this
    }
  }

  object PingPongSerializer {

    val NAME = "typednetpingpong";
    implicit val order = ByteOrder.BIG_ENDIAN;

    private val PING_FLAG: Byte = 1;
    private val PONG_FLAG: Byte = 2;
  }

  class PingPongSerializer extends Serializer {
    import PingPongSerializer._
    import java.nio.{ByteBuffer, ByteOrder}

    override def identifier: Int = SerializerIds.TYPEDNETPP
    override def includeManifest: Boolean = false

    override def toBinary(o: AnyRef): Array[Byte] = {
      o match {
        case Ping(sender) => {
          val br = ByteString.createBuilder.putByte(PING_FLAG)
          val src_bytes = sender.actorPath.getBytes
          br.putShort(src_bytes.size)
          br.putBytes(src_bytes)
          br.result().toArray
        }
        case Pong => Array(PONG_FLAG)
      }
    }

    override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
      val buf = ByteBuffer.wrap(bytes).order(order);
      val flag = buf.get;
      flag match {
        case PING_FLAG => {
          val src_length: Int = buf.getShort
          val src_bytes = new Array[Byte](src_length)
          buf.get(src_bytes)
          val src = ClientRef(src_bytes.map(_.toChar).mkString)
          Ping(src)
        }
        case PONG_FLAG => Pong
      }
    }
  }
}
