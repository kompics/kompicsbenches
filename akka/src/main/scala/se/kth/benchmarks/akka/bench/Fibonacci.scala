package se.kth.benchmarks.akka.bench

import se.kth.benchmarks.akka.ActorSystemProvider
import se.kth.benchmarks.Benchmark
import kompics.benchmarks.benchmarks.FibonacciRequest
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, PoisonPill, Props}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try
import java.util.concurrent.CountDownLatch
import com.typesafe.scalalogging.StrictLogging

object Fibonacci extends Benchmark {
  override type Conf = FibonacciRequest;

  override def msgToConf(msg: scalapb.GeneratedMessage): Try[Conf] = {
    Try(msg.asInstanceOf[FibonacciRequest])
  };
  override def newInstance(): Instance = new FibonacciI;

  class FibonacciI extends Instance with StrictLogging {

    private var fibNumber = -1;
    private var system: ActorSystem = null;
    private var fib: ActorRef = null;
    private var latch: CountDownLatch = null;

    override def setup(c: Conf): Unit = {
      logger.info("Setting up Instance");
      this.fibNumber = c.fibNumber;
      this.system = ActorSystemProvider.newActorSystem(name = "fibonacci");
    }
    override def prepareIteration(): Unit = {
      assert(this.system != null);
      logger.debug("Preparing iteration");
      this.latch = new CountDownLatch(1);
      this.fib = system.actorOf(Props(new FibonacciActor(Right(latch))));
    }
    override def runIteration(): Unit = {
      assert(this.fibNumber > 0);
      assert(this.latch != null);
      assert(this.fib != null);
      this.fib ! FibonacciMsg.Request(this.fibNumber);
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
        val f = system.terminate();
        Await.ready(f, 5.second);
        system = null;
        logger.info("Cleaned up Instance");
      }
    }
  }

  sealed trait FibonacciMsg;
  object FibonacciMsg {
    final case class Request(n: Int) extends FibonacciMsg;
    final case class Response(value: Long) extends FibonacciMsg;
  }

  class FibonacciActor(val reportTo: Either[ActorRef, CountDownLatch]) extends Actor with ActorLogging {
    import FibonacciMsg._;

    private var result = 0L;
    private var numResponses = 0;

    override def receive = {
      case Request(n) => {
        log.debug(s"Got Request with n=$n");
        if (n <= 2) {
          sendResult(1L);
        } else {
          val f1 = context.actorOf(Props(new FibonacciActor(Left(self))));
          f1 ! Request(n - 1);
          val f2 = context.actorOf(Props(new FibonacciActor(Left(self))));
          f2 ! Request(n - 2);
        }
      }
      case Response(value) => {
        log.debug(s"Got Response with value=$value");
        this.numResponses += 1;
        this.result += value;

        if (this.numResponses == 2) {
          sendResult(this.result)
        }
      }
    }

    private def sendResult(value: Long): Unit = {
      reportTo match {
        case Left(ref) => ref ! Response(value)
        case Right(latch) => {
          latch.countDown();
          log.info(s"Final value was $value");
        }
      }
      context.stop(self);
    }
  }
}
