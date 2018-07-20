package se.kth.benchmarks.bench

import se.kth.benchmarks.Benchmark
import kompics.benchmarks.benchmarks.PingPongRequest
import akka.actor.{ ActorSystem, Actor, ActorRef, Props, PoisonPill }
import scala.concurrent.Await
import scala.concurrent.duration._
import java.util.concurrent.CountDownLatch

class PingPong extends Benchmark {
  override type Conf = PingPongRequest;

  private var num = -1l;
  private var system: ActorSystem = null;
  private var pinger: ActorRef = null;
  private var ponger: ActorRef = null;
  private var latch: CountDownLatch = null;

  override def setup(c: Conf): Unit = {
    this.num = c.numberOfMessages;
    system = ActorSystem("pingpong");
  }
  override def prepareIteration(): Unit = {
    assert(system != null);
    assert(num > 0);
    ponger = system.actorOf(Props(new Ponger));
    latch = new CountDownLatch(1);
    pinger = system.actorOf(Props(new Pinger(latch, num, ponger)));
  }
  override def runIteration(): Unit = {
    pinger ! Start;
    latch.await();
  }
  override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
    if (latch != null) {
      latch = null;
    }
    if (pinger != null) {
      system.stop(pinger);
      pinger = null;
    }
    if (ponger != null) {
      system.stop(ponger);
      ponger = null;
    }
    if (lastIteration) {
      val f = system.terminate();
      Await.ready(f, 5.second);
      system = null;
    }
  }
}

case object Start
case object Ping
case object Pong

class Pinger(latch: CountDownLatch, count: Long, ponger: ActorRef) extends Actor {
  var countDown = count;

  override def receive = {
    case Start => {
      ponger ! Ping;
    }
    case Pong => {
      if (countDown > 0) {
        countDown -= 1;
        ponger ! Ping;
      } else {
        latch.countDown();
      }
    }
  }
}

class Ponger extends Actor {
  def receive = {
    case Ping => {
      sender() ! Pong;
    }
  }
}
