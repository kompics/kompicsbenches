package se.kth.benchmarks.kompicsscala.bench

import se.kth.benchmarks.kompicsscala.{KompicsSystem, KompicsSystemProvider}
import se.kth.benchmarks.Benchmark
import kompics.benchmarks.benchmarks.FibonacciRequest
import se.sics.kompics.{Component, Fault, Kill, Killed, Kompics, KompicsEvent, Start, Started}
import se.sics.kompics.sl._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try
import java.util.concurrent.CountDownLatch
import java.util.UUID
import com.typesafe.scalalogging.StrictLogging

object Fibonacci extends Benchmark {
  override type Conf = FibonacciRequest;

  override def msgToConf(msg: scalapb.GeneratedMessage): Try[Conf] = {
    Try(msg.asInstanceOf[FibonacciRequest])
  };
  override def newInstance(): Instance = new FibonacciI;

  class FibonacciI extends Instance with StrictLogging {
    import scala.concurrent.ExecutionContext.Implicits.global;

    private var fibNumber = -1;
    private var system: KompicsSystem = null;
    private var fib: UUID = null;
    private var latch: CountDownLatch = null;

    override def setup(c: Conf): Unit = {
      logger.info(s"Setting up Instance with config: $c");
      this.fibNumber = c.fibNumber;
      this.system = KompicsSystemProvider.newKompicsSystem();
    }
    override def prepareIteration(): Unit = {
      assert(this.system != null);
      logger.debug("Preparing iteration");
      this.latch = new CountDownLatch(1);
      val f = for {
        fibId <- system.createNotify[FibonacciComponent](Init[FibonacciComponent](Right(latch)));
        _ <- system.startNotify(fibId)
      } yield fibId;
      this.fib = Await.result(f, Duration.Inf);
    }
    override def runIteration(): Unit = {
      assert(this.fibNumber > 0);
      assert(this.latch != null);
      assert(this.fib != null);
      assert(this.system != null);
      system.runOnComponent(this.fib) { c =>
        `!trigger`(FibonacciMsg.Request(this.fibNumber, this.fib) -> c.provided(FibonacciPort.getClass()))(
          c.getComponent
        );
      }
      latch.await();
    }
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      logger.debug("Cleaning up iteration");
      if (this.latch != null) {
        this.latch = null;
      }
      if (this.fib != null) {
        // system.stop(this.fib); kills itself
        this.fib = null;
      }
      if (lastIteration) {
        system.terminate();
        system = null;
        logger.info("Cleaned up Instance");
      }
    }
  }

  sealed trait FibonacciMsg extends KompicsEvent;
  object FibonacciMsg {
    final case class Request(n: Int, id: UUID) extends FibonacciMsg;
    final case class Response(value: Long) extends FibonacciMsg;
  }
  object FibonacciPort extends Port {
    request[FibonacciMsg.Request];
    indication[FibonacciMsg.Response];
  }
  object Parent;

  class FibonacciComponent(init: Init[FibonacciComponent]) extends ComponentDefinition {
    import FibonacciMsg._;

    val parentPort = provides(FibonacciPort);
    val childPort = requires(FibonacciPort);

    val myId: UUID = this.id();

    val Init(reportTo: Either[Parent.type, CountDownLatch] @unchecked) = init;

    private var result = 0L;
    private var numResponses = 0;
    private var child1: Option[Component] = None;
    private var child2: Option[Component] = None;

    ctrl uponEvent {
      case event: Killed =>
        handle {
          (child1, child2) match {
            case (Some(f1), _) if event.component.id() == f1.id() => {
              child1 = None;
            }
            case (_, Some(f2)) if event.component.id() == f2.id() => {
              child2 = None;
            }
            case _ => () // ignore
          }
          maybeSuicide();
        }
    }

    parentPort uponEvent {
      case Request(n, this.myId) =>
        handle {
          log.debug(s"Got Request with n=$n");
          if (n <= 2) {
            sendResult(1L);
            suicide();
          } else {
            val f1 = this.create[FibonacciComponent](Init[FibonacciComponent](Left(Parent)));
            val f2 = this.create[FibonacciComponent](Init[FibonacciComponent](Left(Parent)));
            val childPortOuter = this.childPort.dualNegative;
            connect(f1 -> childPortOuter);
            connect(f2 -> childPortOuter);
            trigger(Start.event -> f1.control());
            trigger(Start.event -> f2.control());
            trigger(Request(n - 1, f1.id()) -> childPort);
            trigger(Request(n - 2, f2.id()) -> childPort);
            child1 = Some(f1);
            child2 = Some(f2);
          }
        }
    }

    childPort uponEvent {
      case Response(value) =>
        handle {
          log.debug(s"Got Response with value=$value");
          this.numResponses += 1;
          this.result += value;

          if (this.numResponses == 2) {
            sendResult(this.result);
            maybeSuicide();
          }
        }
    }

    private def sendResult(value: Long): Unit = {
      reportTo match {
        case Left(_parent) => trigger(Response(value) -> parentPort)
        case Right(latch) => {
          latch.countDown();
          log.info(s"Final value was $value");
        }
      }
    }

    private def maybeSuicide(): Unit = {
      if ((numResponses == 2) && child1.isEmpty && child2.isEmpty) {
        logger.debug("Both children died, shutting down");
        suicide();
      }
    }
  }
}
