package se.kth.benchmarks.kompicsscala.bench

import se.kth.benchmarks.Benchmark
import se.kth.benchmarks.kompicsscala.{KompicsSystem, KompicsSystemProvider}
import kompics.benchmarks.benchmarks.PingPongRequest
import se.sics.kompics.{Component, Kill, Killed, Kompics, KompicsEvent, Start, Started}
import se.sics.kompics.sl._
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
      //    val pongerF = system.createNotify[Ponger](Init.none[Ponger]);
      latch = new CountDownLatch(1);
      //    val pingerF = system.createNotify[Pinger](Init(latch, num));
      //    ponger = Await.result(pongerF, Duration.Inf);
      //    pinger = Await.result(pingerF, Duration.Inf);
      //    val connF = system.connectComponents(pinger, ponger);
      //    Await.result(connF, Duration.Inf);
      //    val startF = system.startNotify(ponger);
      //    Await.result(startF, Duration.Inf);
      val f = for {
        pongerId <- system.createNotify[Ponger](Init.none[Ponger]);
        pingerId <- system.createNotify[Pinger](Init(latch, num));
        _ <- system.connectComponents[PingPongPort.type](pingerId, pongerId);
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

  case object Ping extends KompicsEvent;
  case object Pong extends KompicsEvent;

  object PingPongPort extends Port {
    request(Ping);
    indication(Pong);
  }

  class Pinger(init: Init[Pinger]) extends ComponentDefinition {

    val Init(latch: CountDownLatch, count: Long) = init;

    val ppp = requires(PingPongPort);

    var countDown = count;

    ctrl uponEvent {
      case _: Start => {
        trigger(Ping -> ppp);
      }
    }

    ppp uponEvent {
      case Pong => {
        if (countDown > 0) {
          countDown -= 1;
          trigger(Ping -> ppp);
        } else {
          latch.countDown();
        }
      }
    }
  }

  class Ponger extends ComponentDefinition {

    val ppp = provides(PingPongPort);

    ppp uponEvent {
      case Ping => {
        trigger(Pong -> ppp);
      }
    }
  }
}
