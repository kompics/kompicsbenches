package se.kth.benchmarks.kompicsjava.bench

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
import se.kth.benchmarks.kompicsjava.bench.fibonacci._
import java.{util => ju}

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
        fibId <- system.createNotify[FibonacciComponent](new FibonacciComponent.Init(ju.Optional.of(latch)));
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
        `!trigger`(new Messages.FibRequest(this.fibNumber, this.fib) -> c.getPositive(classOf[FibonacciPort]))(
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
}
