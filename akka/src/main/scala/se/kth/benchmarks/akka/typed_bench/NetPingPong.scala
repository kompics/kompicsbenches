package se.kth.benchmarks.akka.typed_bench

import java.nio.ByteOrder
import java.util.concurrent.CountDownLatch

import akka.actor.typed.{ActorRef, ActorRefResolver, ActorSystem, Behavior}
import kompics.benchmarks.benchmarks.PingPongRequest
import se.kth.benchmarks.DistributedBenchmark
import se.kth.benchmarks.akka.{ActorSystemProvider, SerializerBindings, SerializerIds}
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.serialization.Serializer
import akka.util.{ByteString, Timeout}
import se.kth.benchmarks.akka.typed_bench.NetPingPong.SystemSupervisor.{GracefulShutdown, OperationSucceeded, RunPinger, StartPinger, StopPinger, SystemMessage}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Success, Try}

object NetPingPong extends DistributedBenchmark{

  case class ClientRef(actorPath: String)

  override type MasterConf = PingPongRequest
  override type ClientConf = Unit
  override type ClientData = ClientRef

  val serializers = SerializerBindings
    .empty()
    .addSerializer[PingPongSerializer](PingPongSerializer.NAME)
    .addBinding[Ping](PingPongSerializer.NAME)
    .addBinding[Pong.type](PingPongSerializer.NAME);

  class MasterImpl extends Master {
    private var num = -1l;
    private var system: ActorSystem[SystemMessage] = null
    private var latch: CountDownLatch = null;

    override def setup(c: MasterConf): ClientConf = {
      this.num = c.numberOfMessages
      system = ActorSystemProvider.newRemoteTypedActorSystem[SystemMessage](SystemSupervisor(), "netpingpong_supervisor", 1, serializers)
    }

    override def prepareIteration(d: List[ClientRef]): Unit = {
      val ponger = d.head
      println(s"Resolving ponger path ${ponger}");
      latch = new CountDownLatch(1);
      implicit val timeout: Timeout = 3.seconds
      implicit val scheduler = system.scheduler
      val f: Future[OperationSucceeded.type] = system.ask(ref => StartPinger(ref, latch, num, ponger))
      implicit val ec = system.executionContext
      Await.result(f, 3 seconds)
    }

    override def runIteration(): Unit = {
      system ! RunPinger
      latch.await()
    }

    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      println("Cleaning up pinger side");
      if (latch != null) {
        latch = null;
      }
      implicit val timeout: Timeout = 3.seconds
      implicit val scheduler = system.scheduler
      val f: Future[OperationSucceeded.type] = system.ask(ref => StopPinger(ref))
      implicit val ec = system.executionContext
      Await.result(f, 3 seconds)
//      system ! SystemSupervisor.StopPinger
      if (lastIteration){
        println("Cleaning up last iteration...")
        system ! GracefulShutdown
//        system.terminate()
        Await.ready(system.whenTerminated, 5 seconds)
        system = null
        println("Last cleanup completed")
      }
    }
  }

  class ClientImpl extends Client {

    private var ponger: ActorSystem[MsgForPonger] = null

    override def setup(c: ClientConf): ClientRef = {
      ponger = ActorSystemProvider.newRemoteTypedActorSystem[MsgForPonger](Ponger(), "netpingpong", 1, serializers)
      val resolver = ActorRefResolver(ponger)
      val path = resolver.toSerializationFormat(ponger)
      println(s"Ponger path is $path")
      ClientRef(path)
    }

    override def prepareIteration(): Unit = {
      println("Preparing ponger iteration")
    }

    override def cleanupIteration(lastIteration: Boolean): Unit = {
      println("Cleaning up ponger side")
      if (lastIteration){
        println("Cleaning up last iteration")
        ponger.terminate()
        Await.ready(ponger.whenTerminated, 5 seconds)
        ponger = null
        println("Last cleanup completed")
      }
    }
  }

  object SystemSupervisor {
    def apply(): Behavior[SystemMessage] = Behaviors.setup(context => new SystemSupervisor(context))

    sealed trait SystemMessage
    case class StartPinger(ref: ActorRef[OperationSucceeded.type], latch: CountDownLatch, num: Long, ponger: ClientRef) extends SystemMessage
    case object RunPinger extends SystemMessage
    case class StopPinger(ref: ActorRef[OperationSucceeded.type]) extends SystemMessage
    case object GracefulShutdown extends SystemMessage
    case object OperationSucceeded
  }

  class SystemSupervisor(context: ActorContext[SystemMessage]) extends AbstractBehavior[SystemMessage]{
    var pinger: ActorRef[MsgForPinger] = null
    var id: Int = -1

    override def onMessage(msg: SystemMessage): Behavior[SystemMessage] = {
      msg match {
        case start: StartPinger => {
          id += 1
          pinger = context.spawn[MsgForPinger](Pinger(start.latch, start.num, start.ponger), "typed_pinger".concat(id.toString))
          start.ref ! OperationSucceeded
          this
        }
        case stop: StopPinger => {
          if (pinger != null){
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
        case GracefulShutdown => {
          Behaviors.stopped
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
  sealed trait MsgForPonger

  case object Stop extends MsgForPonger
  case class Ping(src: ClientRef) extends MsgForPonger;
  case object Pong extends MsgForPinger
  case object Run extends MsgForPinger

  object Pinger {
    def apply(latch: CountDownLatch, count: Long, ponger: ClientRef): Behavior[MsgForPinger] = Behaviors.setup(context => new Pinger(context, latch, count, ponger))
  }

  class Pinger(context: ActorContext[MsgForPinger], latch: CountDownLatch, count: Long, pongerRef: ClientRef) extends AbstractBehavior[MsgForPinger]{
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
          }
          else latch.countDown()
        }
      }
      this
    }
  }

  object Ponger{
    def apply(): Behavior[MsgForPonger] = Behaviors.setup(context => new Ponger(context))
  }

  class Ponger(context: ActorContext[MsgForPonger]) extends AbstractBehavior[MsgForPonger]{
    val resolver = ActorRefResolver(context.system)

    private def getPingerRef(c: ClientRef): ActorRef[MsgForPinger] = {
      resolver.resolveActorRef(c.actorPath)
    }

    override def onMessage(msg: MsgForPonger): Behavior[MsgForPonger] = {
      msg match {
        case Ping(src) => {
          getPingerRef(src) ! Pong
          this
        }
        case Stop => {
          Behaviors.stopped
        }
      }
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
    import java.nio.{ ByteBuffer, ByteOrder }


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
