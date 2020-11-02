package se.kth.benchmarks.akka.typed_bench

import java.util.concurrent.{CountDownLatch, TimeUnit}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.scaladsl.AskPattern._
import kompics.benchmarks.benchmarks.FibonacciRequest
import scalapb.GeneratedMessage
import se.kth.benchmarks.Benchmark
import se.kth.benchmarks.akka.ActorSystemProvider
import akka.util.Timeout

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import com.typesafe.scalalogging.StrictLogging

import scala.language.postfixOps

object Fibonacci extends Benchmark {
  override type Conf = FibonacciRequest;

  override def msgToConf(msg: scalapb.GeneratedMessage): Try[Conf] = {
    Try(msg.asInstanceOf[FibonacciRequest])
  };
  override def newInstance(): Instance = new FibonacciI;

  class FibonacciI extends Instance with StrictLogging {
    import Fibonacci.SystemSupervisor._;

    private var fibNumber = -1;
    private var system: ActorSystem[SystemMessage] = null;
    private var latch: CountDownLatch = null;

    override def setup(c: Conf): Unit = {
      logger.info(s"Setting up Instance with config: $c");
      this.fibNumber = c.fibNumber;
      this.system = ActorSystemProvider.newTypedActorSystem[SystemMessage](SystemSupervisor(), "typed_fibonacci");
    }
    override def prepareIteration(): Unit = {
      assert(this.system != null);
      logger.debug("Preparing iteration");
      implicit val timeout: Timeout = 3.seconds;
      implicit val scheduler = system.scheduler;
      this.latch = new CountDownLatch(1);
      val f: Future[OperationSucceeded.type] = system.ask(ref => StartActor(ref, latch));
      implicit val ec = system.executionContext;
      Await.result(f, 3 seconds);
    }
    override def runIteration(): Unit = {
      assert(this.fibNumber > 0);
      assert(this.latch != null);
      assert(this.system != null);
      this.system ! RunIteration(this.fibNumber);
      latch.await();
    }
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      logger.debug("Cleaning up iteration");
      if (this.latch != null) {
        this.latch = null;
      }
      if (lastIteration) {
        system ! GracefulShutdown
        Await.ready(system.whenTerminated, 5.second);
        system = null;
        logger.info("Cleaned up Instance");
      }
    }
  }

  object SystemSupervisor {
    sealed trait SystemMessage;
    case class StartActor(replyTo: ActorRef[OperationSucceeded.type], latch: CountDownLatch) extends SystemMessage;
    case class RunIteration(n: Int) extends SystemMessage;
    case object GracefulShutdown extends SystemMessage;
    case object OperationSucceeded;

    def apply(): Behavior[SystemMessage] = Behaviors.setup(context => new SystemSupervisor(context));
  }

  class SystemSupervisor(context: ActorContext[SystemSupervisor.SystemMessage])
      extends AbstractBehavior[SystemSupervisor.SystemMessage](context) {
    import SystemSupervisor._;

    private var fib: ActorRef[FibonacciMsg] = null;
    var run_id: Int = -1

    override def onMessage(msg: SystemMessage): Behavior[SystemMessage] = {
      msg match {
        case s: StartActor => {
          run_id += 1;
          fib = context.spawn(FibonacciActor(s.latch), s"typed_fib$run_id");
          s.replyTo ! OperationSucceeded;
          this
        }
        case RunIteration(n) => {
          this.fib ! FibonacciMsg.Request(n);
          this
        }
        case GracefulShutdown => {
          Behaviors.stopped
        }
      }
    }
  }

  sealed trait FibonacciMsg;
  object FibonacciMsg {
    final case class Request(n: Int) extends FibonacciMsg;
    final case class Response(value: Long) extends FibonacciMsg;
  }

  object FibonacciActor {
    def apply(latch: CountDownLatch): Behavior[FibonacciMsg] =
      Behaviors.setup(context => new FibonacciActor(context, Right(latch)));

    def apply(parent: ActorRef[FibonacciMsg]): Behavior[FibonacciMsg] =
      Behaviors.setup(context => new FibonacciActor(context, Left(parent)));
  }

  class FibonacciActor(context: ActorContext[FibonacciMsg],
                       val reportTo: Either[ActorRef[FibonacciMsg], CountDownLatch])
      extends AbstractBehavior[FibonacciMsg](context) {
    import FibonacciMsg._;

    context.setLoggerName(this.getClass);
    val log = context.log;
    val selfRef = context.self

    private var result = 0L;
    private var numResponses = 0;

    override def onMessage(msg: FibonacciMsg): Behavior[FibonacciMsg] = {
      msg match {
        case Request(n) => {
          log.debug(s"Got Request with n=$n");
          if (n <= 2) {
            sendResult(1L)
          } else {
            val f1 = context.spawn(FibonacciActor(selfRef), s"typed_fib_child1");
            f1 ! Request(n - 1);
            val f2 = context.spawn(FibonacciActor(selfRef), s"typed_fib_child2");
            f2 ! Request(n - 2);
            this
          }
        }
        case Response(value) => {
          log.debug(s"Got Response with value=$value");
          this.numResponses += 1;
          this.result += value;

          if (this.numResponses == 2) {
            sendResult(this.result)
          } else {
            this
          }
        }
      }
    }

    private def sendResult(value: Long): Behavior[FibonacciMsg] = {
      reportTo match {
        case Left(ref) => ref ! Response(value)
        case Right(latch) => {
          latch.countDown();
          log.info(s"Final value was $value");
        }
      }
      Behaviors.stopped
    }
  }
}
