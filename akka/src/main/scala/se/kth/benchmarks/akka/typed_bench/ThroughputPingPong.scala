package se.kth.benchmarks.akka.typed_bench

import java.util.concurrent.CountDownLatch
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout
import kompics.benchmarks.benchmarks.ThroughputPingPongRequest
import scalapb.GeneratedMessage
import se.kth.benchmarks.Benchmark
import se.kth.benchmarks.akka.ActorSystemProvider
import se.kth.benchmarks.akka.bench.ThroughputPingPong.{Conf, Ping}
import se.kth.benchmarks.akka.typed_bench.ThroughputPingPong.SystemSupervisor.{GracefulShutdown, OperationSucceeded, RunIteration, StartActors, StopActors, SystemMesssage}

import scala.util.Try
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import com.typesafe.scalalogging.StrictLogging

import scala.language.postfixOps

object ThroughputPingPong extends Benchmark {
  override type Conf = ThroughputPingPongRequest

  override def msgToConf(msg: scalapb.GeneratedMessage): Try[Conf] = {
    Try(msg.asInstanceOf[ThroughputPingPongRequest])
  };

  override def newInstance(): ThroughputPingPong.Instance = new PingPongI

  class PingPongI extends Instance with StrictLogging {
    private var numMsgs = -1L;
    private var numPairs = -1;
    private var pipeline = -1L;
    private var staticOnly = true
    private var system: ActorSystem[SystemMesssage] = null
    private var latch: CountDownLatch = null

    override def setup(c: Conf): Unit = {
      logger.info("Setting up Instance");
      this.numMsgs = c.messagesPerPair;
      this.numPairs = c.parallelism;
      this.pipeline = c.pipelineSize;
      this.staticOnly = c.staticOnly;
      this.system = ActorSystemProvider.newTypedActorSystem[SystemMesssage](SystemSupervisor(),
                                                                            "typed_tppingpong",
                                                                            Runtime.getRuntime.availableProcessors())
    }

    override def prepareIteration(): Unit = {
      logger.debug("Preparing iteration");
      assert(system != null);
      latch = new CountDownLatch(numPairs);
      implicit val timeout: Timeout = 3.seconds;
      implicit val scheduler = system.scheduler;
      val f: Future[OperationSucceeded.type] =
        system.ask(ref => StartActors(ref, latch, numMsgs, numPairs, pipeline, staticOnly));
      implicit val ec = system.executionContext;
      Await.result(f, 5 seconds);
    }

    override def runIteration(): Unit = {
      system ! RunIteration;
      latch.await();
    }

    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      logger.debug("Cleaning up iteration");
      if (latch != null) {
        latch = null;
      }
      implicit val timeout: Timeout = 3.seconds;
      implicit val scheduler = system.scheduler;
      val f: Future[OperationSucceeded.type] = system.ask(ref => StopActors(ref));
      implicit val ec = system.executionContext;
      Await.result(f, 3 seconds);
      if (lastIteration) {
        if (system != null) {
          system ! GracefulShutdown;
          Await.ready(system.whenTerminated, 5.second);
          system = null;
        }
        logger.info("Cleaned up Instance");
      }
    }

  }

  object SystemSupervisor {
    sealed trait SystemMesssage
    case class StartActors(replyTo: ActorRef[OperationSucceeded.type],
                           latch: CountDownLatch,
                           numMsgs: Long,
                           numPairs: Int,
                           pipeline: Long,
                           staticOnly: Boolean)
        extends SystemMesssage
    case object RunIteration extends SystemMesssage
    case class StopActors(replyTo: ActorRef[OperationSucceeded.type]) extends SystemMesssage
    case object GracefulShutdown extends SystemMesssage
    case object OperationSucceeded

    def apply(): Behavior[SystemMesssage] = Behaviors.setup(context => new SystemSupervisor(context))
  }

  class SystemSupervisor(context: ActorContext[SystemMesssage]) extends AbstractBehavior[SystemMesssage](context) {

    var pingers: List[ActorRef[MsgForPinger]] = null
    var pongers: List[ActorRef[Ping]] = null
    var static_pingers: List[ActorRef[MsgForStaticPinger]] = null
    var static_pongers: List[ActorRef[StaticPing]] = null
    var run_id: Int = -1
    var staticOnly = true

    override def onMessage(msg: SystemMesssage): Behavior[SystemMesssage] = {
      msg match {
        case s: StartActors => {
          context.log.info(s"Got startactors")
          run_id += 1
          this.staticOnly = s.staticOnly
          val indexes = (1 to s.numPairs).toList;
          if (staticOnly) {
            static_pongers = indexes.map(i => context.spawn(StaticPonger(), s"typed_staticponger${run_id}_$i"))
          } else {
            pongers = indexes.map(i => context.spawn(Ponger(), s"typed_ponger${run_id}_$i"))
          }
          if (staticOnly) {
            static_pingers = static_pongers.zipWithIndex.map {
              case (static_ponger, i) =>
                context.spawn(StaticPinger(s.latch, s.numMsgs, s.pipeline, static_ponger),
                              s"typed_staticpinger${run_id}_$i")
            }
          } else {
            pingers = pongers.zipWithIndex.map {
              case (ponger, i) =>
                context.spawn(Pinger(s.latch, s.numMsgs, s.pipeline, ponger), s"typed_pinger${run_id}_$i")
            }
          }
          s.replyTo ! OperationSucceeded
          context.log.info("StartActors completed")
          this
        }
        case RunIteration => {
          if (staticOnly) static_pingers.foreach(static_pinger => static_pinger ! RunStaticPinger)
          else pingers.foreach(pinger => pinger ! RunPinger)
          this
        }
        case StopActors(replyTo: ActorRef[OperationSucceeded.type]) => {
          if (staticOnly) {
            if (static_pongers.nonEmpty) {
              static_pongers.foreach(static_ponger => context.stop(static_ponger))
              static_pongers = List.empty
            }
            if (static_pingers.nonEmpty) {
              static_pingers.foreach(static_pinger => context.stop(static_pinger))
              static_pingers = List.empty
            }
          } else {
            if (pongers.nonEmpty) {
              pongers.foreach(ponger => context.stop(ponger))
              pongers = List.empty
            }
            if (pingers.nonEmpty) {
              pingers.foreach(pinger => context.stop(pinger))
              pingers = List.empty
            }
          }
          replyTo ! OperationSucceeded
          this
        }
        case GracefulShutdown => {
          Behaviors.stopped
        }
      }
    }
  }

  sealed trait MsgForPinger
  case class Ping(src: ActorRef[MsgForPinger], index: Long)
  case class Pong(index: Long) extends MsgForPinger
  case object RunPinger extends MsgForPinger

  object Pinger {
    def apply(latch: CountDownLatch, count: Long, pipeline: Long, ponger: ActorRef[Ping]): Behavior[MsgForPinger] =
      Behaviors.setup(context => new Pinger(context, latch, count, pipeline, ponger))
  }

  class Pinger(context: ActorContext[MsgForPinger],
               latch: CountDownLatch,
               count: Long,
               pipeline: Long,
               ponger: ActorRef[Ping])
      extends AbstractBehavior[MsgForPinger](context) {
    var sentCount = 0L;
    var recvCount = 0L;
    val selfRef = context.self

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
    override def onMessage(msg: Ping): Behavior[Ping] = {
      msg.src ! Pong(msg.index)
      this
    }

  }

  sealed trait MsgForStaticPinger
  case class StaticPing(src: ActorRef[MsgForStaticPinger])
  case object StaticPong extends MsgForStaticPinger
  case object RunStaticPinger extends MsgForStaticPinger

  object StaticPinger {
    def apply(latch: CountDownLatch,
              count: Long,
              pipeline: Long,
              ponger: ActorRef[StaticPing]): Behavior[MsgForStaticPinger] =
      Behaviors.setup(context => new StaticPinger(context, latch, count, pipeline, ponger))
  }

  class StaticPinger(context: ActorContext[MsgForStaticPinger],
                     latch: CountDownLatch,
                     count: Long,
                     pipeline: Long,
                     ponger: ActorRef[StaticPing])
      extends AbstractBehavior[MsgForStaticPinger](context) {
    var sentCount = 0L;
    var recvCount = 0L;
    val selfRef = context.self

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
    override def onMessage(msg: StaticPing): Behavior[StaticPing] = {
      msg.src ! StaticPong
      this
    }
  }

}
