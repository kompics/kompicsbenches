package se.kth.benchmarks.akka.bench

import se.kth.benchmarks.akka.ActorSystemProvider
import se.kth.benchmarks.Benchmark
import kompics.benchmarks.benchmarks.ThroughputPingPongRequest
import akka.actor.{ ActorSystem, Actor, ActorRef, Props, PoisonPill }
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try
import java.util.concurrent.CountDownLatch

object ThroughputPingPong extends Benchmark {
  override type Conf = ThroughputPingPongRequest;

  override def msgToConf(msg: scalapb.GeneratedMessage): Try[Conf] = {
    Try(msg.asInstanceOf[ThroughputPingPongRequest])
  };
  override def newInstance(): Instance = new PingPongI;

  class PingPongI extends Instance {

    private var numMsgs = -1l;
    private var numPairs = -1;
    private var pipeline = -1l;
    private var staticOnly = true;
    private var system: ActorSystem = null;
    private var pingers: List[ActorRef] = List.empty;
    private var pongers: List[ActorRef] = List.empty;
    private var latch: CountDownLatch = null;

    override def setup(c: Conf): Unit = {
      this.numMsgs = c.messagesPerPair;
      this.numPairs = c.parallelism;
      this.pipeline = c.pipelineSize;
      this.staticOnly = c.staticOnly;
      system = ActorSystemProvider.newActorSystem(
        name = "tppingpong", threads =
        Runtime.getRuntime.availableProcessors());
    }
    override def prepareIteration(): Unit = {
      assert(system != null);
      val indexes = (1 to numPairs).toList;
      if (staticOnly) {
        pongers = indexes.map(i => system.actorOf(Props(new StaticPonger)));
      } else {
        pongers = indexes.map(i => system.actorOf(Props(new Ponger)));
      }
      latch = new CountDownLatch(numPairs);
      if (staticOnly) {
        pingers = pongers.map(ponger => system.actorOf(Props(new StaticPinger(latch, numMsgs, pipeline, ponger))));
      } else {
        pingers = pongers.map(ponger => system.actorOf(Props(new Pinger(latch, numMsgs, pipeline, ponger))));
      }

    }
    override def runIteration(): Unit = {
      pingers.foreach(pinger => pinger ! Start);
      latch.await();
    }
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      if (latch != null) {
        latch = null;
      }
      if (!pingers.isEmpty) {
        pingers.foreach(pinger => system.stop(pinger));
        pingers = List.empty;
      }
      if (!pongers.isEmpty) {
        pongers.foreach(ponger => system.stop(ponger));
        pongers = List.empty;
      }
      if (lastIteration) {
        val f = system.terminate();
        Await.ready(f, 5.second);
        system = null;
      }
    }
  }

  case object Start
  case object StaticPing;
  case object StaticPong;
  case class Ping(index: Long);
  case class Pong(index: Long);

  class StaticPinger(latch: CountDownLatch, count: Long, pipeline: Long, ponger: ActorRef) extends Actor {
    var sentCount = 0l;
    var recvCount = 0l;

    override def receive = {
      case Start => {
        var pipelined = 0l;
        while (pipelined < pipeline && sentCount < count) {
          ponger ! StaticPing;
          pipelined += 1l;
          sentCount += 1l;
        }
      }
      case StaticPong => {
        recvCount += 1l;
        if (recvCount < count) {
          if (sentCount < count) {
            ponger ! StaticPing;
            sentCount += 1l;
          }
        } else {
          latch.countDown();
        }
      }
    }
  }

  class StaticPonger extends Actor {
    def receive = {
      case StaticPing => {
        sender() ! StaticPong;
      }
    }
  }

  class Pinger(latch: CountDownLatch, count: Long, pipeline: Long, ponger: ActorRef) extends Actor {
    var sentCount = 0l;
    var recvCount = 0l;

    override def receive = {
      case Start => {
        var pipelined = 0l;
        while (pipelined < pipeline && sentCount < count) {
          ponger ! Ping(sentCount);
          pipelined += 1l;
          sentCount += 1l;
        }
      }
      case Pong(_) => {
        recvCount += 1l;
        if (recvCount < count) {
          if (sentCount < count) {
            ponger ! Ping(sentCount);
            sentCount += 1l;
          }
        } else {
          latch.countDown();
        }
      }
    }
  }

  class Ponger extends Actor {
    override def receive = {
      case Ping(i) => {
        sender() ! Pong(i);
      }
    }
  }
}
