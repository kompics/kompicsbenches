package se.kth.benchmarks.kompicsjava.bench

import se.kth.benchmarks.Benchmark
import se.kth.benchmarks.kompicsscala.{KompicsSystem, KompicsSystemProvider}
import kompics.benchmarks.benchmarks.PingPongRequest
import se.kth.benchmarks.kompicsjava.bench.pingpong._;
import se.sics.kompics.sl.Init;
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Try
import java.util.concurrent.CountDownLatch
import java.util.UUID

object PingPong extends Benchmark {
  import scala.concurrent.ExecutionContext.Implicits.global;

  override type Conf = PingPongRequest;

  override def msgToConf(msg: scalapb.GeneratedMessage): Try[Conf] = {
    Try(msg.asInstanceOf[PingPongRequest])
  };
  override def newInstance(): Instance = new PingPongI;

  class PingPongI extends Instance {
    private var num = -1L;
    private var system: KompicsSystem = null;
    private var pinger: UUID = null;
    private var ponger: UUID = null;
    private var latch: CountDownLatch = null;

    override def setup(c: Conf): Unit = {
      this.num = c.numberOfMessages;
      this.system = KompicsSystemProvider.newKompicsSystem(threads = 2);
    }
    override def prepareIteration(): Unit = {
      assert(system != null);
      assert(num > 0);
      latch = new CountDownLatch(1);
      val f = for {
        pongerId <- system.createNotify[Ponger](Init.none[Ponger]);
        pingerId <- system.createNotify[Pinger](new Pinger.Init(latch, num));
        _ <- system.connectComponents[PingPongPort](pingerId, pongerId);
        _ <- system.startNotify(pongerId)
      } yield {
        ponger = pongerId;
        pinger = pingerId;
      };
      Await.result(f, Duration.Inf);
    }
    override def runIteration(): Unit = {
      val startF = system.startNotify(pinger);
      Await.result(startF, Duration.Inf);
      latch.await();
    }
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      if (latch != null) {
        latch = null;
      }
      val kill1F = if (pinger != null) {
        val killF = system.killNotify(pinger);
        pinger = null;
        killF
      } else {
        Future.successful()
      }
      val kill2F = if (ponger != null) {
        val killF = system.killNotify(ponger);
        ponger = null;
        killF
      } else {
        Future.successful()
      }
      Await.result(kill1F, Duration.Inf);
      Await.result(kill2F, Duration.Inf);
      if (lastIteration) {
        system.terminate();
        system = null;
      }
    }
  }
}
