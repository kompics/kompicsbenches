package se.kth.benchmarks.akka.bench

import akka.actor._
import se.kth.benchmarks.akka.ActorSystemProvider
import se.kth.benchmarks._
import kompics.benchmarks.benchmarks.PingPongRequest
import scala.util.{ Try, Success, Failure }
import scala.concurrent.Await
import scala.concurrent.duration._
import java.util.concurrent.CountDownLatch

object NetPingPong extends DistributedBenchmark {

  case class ClientRef(actorPath: String)

  override type MasterConf = PingPongRequest;
  override type ClientConf = Unit;
  override type ClientData = ClientRef;

  class MasterImpl extends Master {
    private var num = -1l;
    private var system: ActorSystem = null;
    private var pinger: ActorRef = null;
    private var latch: CountDownLatch = null;

    override def setup(c: MasterConf): ClientConf = {
      this.num = c.numberOfMessages;
      system = ActorSystemProvider.newRemoteActorSystem(name = "pingpong", threads = 1);
      ()
    };
    override def prepareIteration(d: List[ClientData]): Unit = {
      val pongerPath = d.head.actorPath;
      println(s"Resolving path ${pongerPath}");
      val pongerF = system.actorSelection(pongerPath).resolveOne(5 seconds);
      val ponger = Await.result(pongerF, 5 seconds);
      println(s"Resolved path to $ponger");
      latch = new CountDownLatch(1);
      pinger = system.actorOf(Props(new Pinger(latch, num, ponger)), "pinger");
    }
    override def runIteration(): Unit = {
      pinger ! Start;
      latch.await();
    };
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      println("Cleaning up pinger side");
      if (latch != null) {
        latch = null;
      }
      if (pinger != null) {
        system.stop(pinger);
        pinger = null;
      }
      if (lastIteration) {
        val f = system.terminate();
        Await.ready(f, 5 seconds);
        system = null;
      }
    }
  }

  class ClientImpl extends Client {
    private var system: ActorSystem = null;
    private var ponger: ActorRef = null;

    override def setup(c: ClientConf): ClientData = {
      system = ActorSystemProvider.newRemoteActorSystem(name = "pingpong", threads = 1);
      ponger = system.actorOf(Props(new Ponger), "ponger");
      val path = ActorSystemProvider.actorPathForRef(ponger, system);
      println(s"Ponger Path is $path");
      ClientRef(path)
    }
    override def prepareIteration(): Unit = {
      // nothing
      println("Preparing ponger iteration");
    }
    override def cleanupIteration(lastIteration: Boolean): Unit = {
      println("Cleaning up ponger side");
      if (lastIteration) {
        val f = system.terminate();
        Await.ready(f, 5.second);
        system = null;
      }
    }
  }

  override def newMaster(): Master = new MasterImpl();
  override def msgToMasterConf(msg: scalapb.GeneratedMessage): Try[MasterConf] = Try {
    msg.asInstanceOf[PingPongRequest]
  };

  override def newClient(): Client = new ClientImpl();
  override def strToClientConf(str: String): Try[ClientConf] = Success(());
  override def strToClientData(str: String): Try[ClientData] = Success(ClientRef(str));

  override def clientConfToString(c: ClientConf): String = "";
  override def clientDataToString(d: ClientData): String = d.actorPath;

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
}
