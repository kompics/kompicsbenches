package se.kth.benchmarks.akka.typed_bench

import java.util.concurrent.CountDownLatch

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.scaladsl.AskPattern._

import kompics.benchmarks.benchmarks.PingPongRequest
import scalapb.GeneratedMessage
import se.kth.benchmarks.Benchmark
import se.kth.benchmarks.akka.ActorSystemProvider
import se.kth.benchmarks.akka.typed_bench.PingPong.SystemSupervisor.{RunIteration, StartActors, StopActors, OperationSucceeded, SystemMessage}
import akka.util.Timeout

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object PingPong extends Benchmark {
  override type Conf = PingPongRequest

  override def msgToConf(msg: scalapb.GeneratedMessage): Try[Conf] = {
    Try(msg.asInstanceOf[PingPongRequest])
  };
  override def newInstance(): PingPong.Instance = new PingPongI

  class PingPongI extends Instance {

    private var num = -1l;
    private var system: ActorSystem[SystemMessage] = null
    private var latch: CountDownLatch = null;

    override def setup(c: Conf): Unit = {
      println("Atomic Register(Master) setup!")
      this.num = c.numberOfMessages
      system = ActorSystemProvider.newTypedActorSystem[SystemMessage](SystemSupervisor(), "typed_pingpong", 2)
    }

    override def prepareIteration(): Unit = {
      assert(system != null);
      assert(num > 0);
      println("Preparing iteration")
      latch = new CountDownLatch(1);
      implicit val timeout: Timeout = 3.seconds
      implicit val scheduler = system.scheduler
      val f: Future[OperationSucceeded.type] = system.ask(ref => StartActors(ref, latch, num))
      implicit val ec = system.executionContext
      Await.result(f, 3 seconds)
    }

    override def runIteration(): Unit = {
      println("Run iteration")
      system ! RunIteration
      latch.await()
    }

    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      println("Cleaning up iteration")
      if (latch != null) {
        latch = null;
      }
      implicit val timeout: Timeout = 3.seconds
      implicit val scheduler = system.scheduler
      val result: Future[SystemSupervisor.OperationSucceeded.type] = system.ask(ref => SystemSupervisor.StopActors(ref))
      implicit val ec = system.executionContext
      Await.result(result, 3 seconds)
      if (lastIteration){
        system.terminate();
        Await.ready(system.whenTerminated, 5.second);
        system = null;
      }

    }
  }

  object SystemSupervisor {
    sealed trait SystemMessage
    case class StartActors(replyTo: ActorRef[OperationSucceeded.type], latch: CountDownLatch, num: Long) extends SystemMessage
    case object RunIteration extends SystemMessage
    case class StopActors(replyTo: ActorRef[OperationSucceeded.type]) extends SystemMessage
    case object OperationSucceeded

    def apply(): Behavior[SystemMessage] = Behaviors.setup(context => new SystemSupervisor(context))
  }

  class SystemSupervisor(context: ActorContext[SystemMessage]) extends AbstractBehavior[SystemMessage]{

    var pinger: ActorRef[MsgForPinger] = null
    var ponger: ActorRef[Ping] = null
    var run_id: Int = -1

    override def onMessage(msg: SystemMessage): Behavior[SystemMessage] = {
      msg match {
        case s: StartActors => {
          run_id += 1
          ponger = context.spawn(Ponger(), s"typed_ponger$run_id")
          pinger = context.spawn(Pinger(s.latch, s.num, ponger), s"typed_pinger$run_id")
          s.replyTo ! OperationSucceeded
        }
        case RunIteration => pinger ! Run
        case StopActors(replyTo: ActorRef[OperationSucceeded.type]) => {
          if (pinger != null) {
            context.stop(pinger)
            pinger = null
          }
          if (ponger != null) {
            context.stop(ponger)
            ponger = null
          }
          replyTo ! OperationSucceeded
        }
      }
      this
    }
  }

  sealed trait MsgForPinger
  case class Ping(src: ActorRef[MsgForPinger])
  case object Pong extends MsgForPinger
  case object Run extends MsgForPinger

  object Pinger{
    def apply(latch: CountDownLatch, count: Long, ponger: ActorRef[Ping]): Behavior[MsgForPinger] = Behaviors.setup(context => new Pinger(context, latch, count, ponger))
  }

  class Pinger(context: ActorContext[MsgForPinger], latch: CountDownLatch, count: Long, ponger: ActorRef[Ping]) extends AbstractBehavior[MsgForPinger]{
    var countDown = count;
    val selfRef = context.self

    override def onMessage(msg: MsgForPinger): Behavior[MsgForPinger] = {
      msg match {
        case Run => ponger ! Ping(selfRef)
        case Pong => {
          if (countDown > 0){
            countDown -= 1
            ponger ! Ping(selfRef)
          } else latch.countDown()
        }
      }
      this
    }
  }

  object Ponger{
    def apply(): Behavior[Ping] = Behaviors.setup(context => new Ponger(context))
  }

  class Ponger(context: ActorContext[Ping]) extends AbstractBehavior[Ping]{
    override def onMessage(msg: Ping): Behavior[Ping] = {
      msg.src ! Pong
      this
    }
  }
}
