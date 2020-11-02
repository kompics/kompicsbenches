package se.kth.benchmarks.akka.bench

import akka.actor._
import akka.serialization.Serializer
import akka.util.ByteString
import se.kth.benchmarks.akka.{ActorSystemProvider, SerializerBindings, SerializerIds}
import se.kth.benchmarks._
import kompics.benchmarks.benchmarks.ThroughputPingPongRequest

import scala.util.{Failure, Success, Try}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import java.util.concurrent.CountDownLatch
import com.typesafe.scalalogging.StrictLogging

import scala.language.postfixOps

object NetThroughputPingPong extends DistributedBenchmark {

  case class ClientRefs(actorPaths: List[String])

  case class ClientParams(numPongers: Int, staticOnly: Boolean)

  override type MasterConf = ThroughputPingPongRequest;
  override type ClientConf = ClientParams;
  override type ClientData = ClientRefs;

  val serializers = SerializerBindings
    .empty()
    .addSerializer[PingPongSerializer](PingPongSerializer.NAME)
    .addBinding[StaticPing.type](PingPongSerializer.NAME)
    .addBinding[StaticPong.type](PingPongSerializer.NAME)
    .addBinding[Ping](PingPongSerializer.NAME)
    .addBinding[Pong](PingPongSerializer.NAME);

  class MasterImpl extends Master with StrictLogging {

    implicit val ec = scala.concurrent.ExecutionContext.global;

    private var numMsgs = -1L;
    private var numPairs = -1;
    private var pipeline = -1L;
    private var staticOnly = true;
    private var system: ActorSystem = null;
    private var pingers: List[ActorRef] = List.empty;
    private var latch: CountDownLatch = null;
    private var run_id = -1

    override def setup(c: MasterConf, _meta: DeploymentMetaData): Try[ClientConf] = Try {
      logger.info("Setting up Master");
      this.numMsgs = c.messagesPerPair;
      this.numPairs = c.parallelism;
      this.pipeline = c.pipelineSize;
      this.staticOnly = c.staticOnly;
      this.system = ActorSystemProvider.newRemoteActorSystem(name = "tppingpong",
                                                             threads = Runtime.getRuntime.availableProcessors(),
                                                             serialization = serializers);
      ClientParams(numPairs, staticOnly)
    };
    override def prepareIteration(d: List[ClientData]): Unit = {
      logger.debug("Preparing iteration");
      val pongersF =
        Future.sequence(d.head.actorPaths.map(pongerPath => system.actorSelection(pongerPath).resolveOne(5 seconds)));
      val pongers = Await.result(pongersF, 5 seconds);
      logger.trace(s"Resolved paths to ${pongers.mkString}");
      latch = new CountDownLatch(numPairs);
      run_id += 1
      if (staticOnly) {
        pingers = pongers.zipWithIndex.map {
          case (ponger, i) =>
            system.actorOf(Props(new StaticPinger(latch, numMsgs, pipeline, ponger)), s"pinger${run_id}_$i")
        };
      } else {
        pingers = pongers.zipWithIndex.map {
          case (ponger, i) => system.actorOf(Props(new Pinger(latch, numMsgs, pipeline, ponger)), s"pinger${run_id}_$i")
        };
      }
    }
    override def runIteration(): Unit = {
      pingers.foreach(pinger => pinger ! Start);
      latch.await();
    };
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      logger.debug("Cleaning up pinger side");
      if (latch != null) {
        latch = null;
      }
      if (!pingers.isEmpty) {
        pingers.foreach(pinger => system.stop(pinger));
        pingers = List.empty;
      }
      if (lastIteration) {
        val f = system.terminate();
        Await.ready(f, 5 seconds);
        system = null;
        logger.info("Cleaned up Master");
      }
    }
  }

  class ClientImpl extends Client with StrictLogging {
    private var system: ActorSystem = null;
    private var pongers: List[ActorRef] = List.empty;

    override def setup(c: ClientConf): ClientData = {
      logger.info("Setting up Client");
      system = ActorSystemProvider.newRemoteActorSystem(name = "pingpong", threads = 1, serialization = serializers);
      if (c.staticOnly) {
        pongers = (1 to c.numPongers).map(i => system.actorOf(Props(new StaticPonger), s"ponger$i")).toList;
      } else {
        pongers = (1 to c.numPongers).map(i => system.actorOf(Props(new Ponger), s"ponger$i")).toList;
      }
      val paths = pongers.map(ponger => ActorSystemProvider.actorPathForRef(ponger, system));
      logger.trace(s"Ponger Paths are ${paths.mkString}");
      ClientRefs(paths)
    }
    override def prepareIteration(): Unit = {
      // nothing
      logger.debug("Preparing ponger iteration");
    }
    override def cleanupIteration(lastIteration: Boolean): Unit = {
      logger.debug("Cleaning up ponger side");
      if (lastIteration) {
        val f = system.terminate();
        Await.ready(f, 5.second);
        system = null;
        logger.info("Cleaning up Client");
      }
    }
  }

  override def newMaster(): Master = new MasterImpl();
  override def msgToMasterConf(msg: scalapb.GeneratedMessage): Try[MasterConf] = Try {
    msg.asInstanceOf[ThroughputPingPongRequest]
  };

  override def newClient(): Client = new ClientImpl();
  override def strToClientConf(str: String): Try[ClientConf] = Try {
    val split = str.split(",");
    val num = split(0).toInt;
    val staticOnly = split(1) match {
      case "true"  => true
      case "false" => false
    };
    ClientParams(num, staticOnly)
  };
  override def strToClientData(str: String): Try[ClientData] = Try {
    val l = str.split(",").toList;
    ClientRefs(l)
  };

  override def clientConfToString(c: ClientConf): String = s"${c.numPongers},${c.staticOnly}";
  override def clientDataToString(d: ClientData): String = d.actorPaths.mkString(",");

  case object Start;
  case object StaticPing;
  case object StaticPong;
  case class Ping(index: Long);
  case class Pong(index: Long);

  object PingPongSerializer {

    val NAME = "tpnetpingpong";

    private val STATIC_PING_FLAG: Byte = 1;
    private val STATIC_PONG_FLAG: Byte = 2;
    private val PING_FLAG: Byte = 3;
    private val PONG_FLAG: Byte = 4;
  }

  class PingPongSerializer extends Serializer {
    import PingPongSerializer._;
    import java.nio.{ByteBuffer, ByteOrder}

    implicit val order = ByteOrder.BIG_ENDIAN;

    override def identifier: Int = SerializerIds.NETTPPP;
    override def includeManifest: Boolean = false;
    override def toBinary(o: AnyRef): Array[Byte] = {
      o match {
        case StaticPing => Array(STATIC_PING_FLAG)
        case StaticPong => Array(STATIC_PONG_FLAG)
        case Ping(i)    => ByteString.createBuilder.putByte(PING_FLAG).putLong(i).result().toArray
        case Pong(i)    => ByteString.createBuilder.putByte(PONG_FLAG).putLong(i).result().toArray
      }
    }

    override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
      val buf = ByteBuffer.wrap(bytes).order(order);
      val flag = buf.get;
      flag match {
        case STATIC_PING_FLAG => StaticPing
        case STATIC_PONG_FLAG => StaticPong
        case PING_FLAG => {
          val i = buf.getLong;
          Ping(i)
        }
        case PONG_FLAG => {
          val i = buf.getLong;
          Pong(i)
        }
      }
    }
  }

  class StaticPinger(latch: CountDownLatch, count: Long, pipeline: Long, ponger: ActorRef) extends Actor {
    var sentCount = 0L;
    var recvCount = 0L;

    override def receive = {
      case Start => {
        var pipelined = 0L;
        while (pipelined < pipeline && sentCount < count) {
          ponger ! StaticPing;
          pipelined += 1L;
          sentCount += 1L;
        }
      }
      case StaticPong => {
        recvCount += 1;
        if (recvCount < count) {
          if (sentCount < count) {
            ponger ! StaticPing;
            sentCount += 1;
          }
        } else {
          latch.countDown();
        }
      }
    }
  }

  class StaticPonger extends Actor {
    override def receive = {
      case StaticPing => {
        sender() ! StaticPong;
      }
    }
  }

  class Pinger(latch: CountDownLatch, count: Long, pipeline: Long, ponger: ActorRef) extends Actor {
    var sentCount = 0L;
    var recvCount = 0L;

    override def receive = {
      case Start => {
        var pipelined = 0L;
        while (pipelined < pipeline && sentCount < count) {
          ponger ! Ping(sentCount);
          pipelined += 1L;
          sentCount += 1L;
        }
      }
      case Pong(_) => {
        recvCount += 1;
        if (recvCount < count) {
          if (sentCount < count) {
            ponger ! Ping(sentCount);
            sentCount += 1;
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
