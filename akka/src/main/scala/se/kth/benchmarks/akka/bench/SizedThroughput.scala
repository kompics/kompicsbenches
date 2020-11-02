package se.kth.benchmarks.akka.bench

import java.util.concurrent.CountDownLatch
import akka.actor._
import akka.serialization.Serializer
import akka.util.ByteString
import com.typesafe.scalalogging.StrictLogging
import kompics.benchmarks.benchmarks.SizedThroughputRequest
import se.kth.benchmarks._
import se.kth.benchmarks.akka.{ActorSystemProvider, SerializerBindings, SerializerIds}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.Try

object SizedThroughput extends DistributedBenchmark {

  case class ClientRefs(actorPaths: List[String])

  case class ClientParams(numPairs: Int, batchSize: Int)

  override type MasterConf = SizedThroughputRequest;
  override type ClientConf = ClientParams;
  override type ClientData = ClientRefs;

  val serializers = SerializerBindings
    .empty()
    .addSerializer[SizedThroughputSerializer](SizedThroughputSerializer.NAME)
    .addBinding[SizedThroughputMessage](SizedThroughputSerializer.NAME)
    .addBinding[Ack.type](SizedThroughputSerializer.NAME);

  class MasterImpl extends Master with StrictLogging {

    implicit val ec = scala.concurrent.ExecutionContext.global;

    private var numPairs = -1;
    private var messageSize = -1;
    private var batchSize = -1;
    private var batchCount = -1;
    private var system: ActorSystem = null;
    private var sources: List[ActorRef] = List.empty;
    private var latch: CountDownLatch = null;
    private var run_id = -1
    private var setting_up = true;

    override def setup(c: MasterConf, _meta: DeploymentMetaData): Try[ClientConf] = Try {
      this.batchCount = c.numberOfBatches;
      this.numPairs = c.numberOfPairs;
      this.batchSize = c.batchSize;
      this.messageSize = c.messageSize;
      logger.info("Setting up Master, numPairs: " ++ numPairs.toString
        ++ ", message size: " ++ messageSize.toString
        ++ " batchSize: " ++ batchSize.toString
        ++  ", batchCount: " ++ batchCount.toString);
      this.system = ActorSystemProvider.newRemoteActorSystem(name = "sizedthroughput_supervisor",
                                                             threads = Runtime.getRuntime.availableProcessors(),
                                                             serialization = serializers);
      ClientParams(numPairs, batchSize)
    };
    override def prepareIteration(d: List[ClientData]): Unit = {
      if (setting_up) {
        // Only create sources the first time
        logger.debug("Preparing first iteration");
        val sinksF =
          Future.sequence(d.head.actorPaths.map(sinkPath => system.actorSelection(sinkPath).resolveOne(5 seconds)));
        val sinks = Await.result(sinksF, 5 seconds);
        logger.trace(s"Resolved paths to ${sinks.mkString}");
        run_id += 1
        sources = sinks.zipWithIndex.map {
          case (sink, i) => system.actorOf(Props(new Source(messageSize, batchSize, batchCount, sink)),
            s"source${run_id}_$i")
        };
        setting_up = false;
      } else {
        logger.debug("Preparing iteration");
      }
    }
    override def runIteration(): Unit = {
      latch = new CountDownLatch(numPairs);
      sources.foreach(source => source ! Start(latch));
      latch.await();
    };
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      logger.debug("Cleaning up Source side");
      if (latch != null) {
        latch = null;
      }
      if (lastIteration) {
        if (!sources.isEmpty) {
          sources.foreach(source => system.stop(source));
          sources = List.empty;
        }
        val f = system.terminate();
        Await.ready(f, 5 seconds);
        system = null;
        logger.info("Cleaned up Master");
      }
    }
  }

  class ClientImpl extends Client with StrictLogging {
    private var system: ActorSystem = null;
    private var sinks: List[ActorRef] = List.empty;

    override def setup(c: ClientConf): ClientData = {
      logger.info("Setting up Client, numPairs: " ++ c.numPairs.toString ++ " batchSize: " ++ c.batchSize.toString);
      system = ActorSystemProvider.newRemoteActorSystem(name = "sizedthroughput-clientsupervisor", threads = 1, serialization = serializers);
      sinks = (1 to c.numPairs).map(i => system.actorOf(Props(new Sink(c.batchSize)), s"sink$i")).toList;

      val paths = sinks.map(sink => ActorSystemProvider.actorPathForRef(sink, system));
      logger.trace(s"Sink Paths are ${paths.mkString}");
      ClientRefs(paths)
    }
    override def prepareIteration(): Unit = {
      // nothing
      logger.debug("Preparing Sink iteration");
    }
    override def cleanupIteration(lastIteration: Boolean): Unit = {
      logger.debug("Cleaning up Sink side");
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
    msg.asInstanceOf[SizedThroughputRequest]
  };

  override def newClient(): Client = new ClientImpl();
  override def strToClientConf(str: String): Try[ClientConf] = Try {
    val split = str.split(",");
    val numPairs = split(0).toInt;
    val batchSize = split(1).toInt;
    ClientParams(numPairs, batchSize)
  };
  override def strToClientData(str: String): Try[ClientData] = Try {
    val l = str.split(",").toList;
    ClientRefs(l)
  };

  override def clientConfToString(c: ClientConf): String = s"${c.numPairs},${c.batchSize}";
  override def clientDataToString(d: ClientData): String = d.actorPaths.mkString(",");

  case class Start(latch: CountDownLatch);
  case class SizedThroughputMessage(aux: Int, data: Array[Byte]);
  object Ack;

  object SizedThroughputSerializer {
    val NAME = "sizedthroughput";
    private val SIZEDTHROUGHPUTMESSAGE_FLAG: Byte = 1;
    private val ACK_FLAG: Byte = 2;
  }

  class SizedThroughputSerializer extends Serializer {
    import java.nio.{ByteBuffer, ByteOrder}

    import SizedThroughputSerializer._

    implicit val order = ByteOrder.BIG_ENDIAN;

    override def identifier: Int = SerializerIds.SIZEDTHROUGHPUT;
    override def includeManifest: Boolean = false;
    override def toBinary(o: AnyRef): Array[Byte] = {
      o match {
        case SizedThroughputMessage(aux, data) => ByteString.createBuilder
          .putByte(SIZEDTHROUGHPUTMESSAGE_FLAG).putInt(aux).putInt(data.length).putBytes(data).result().toArray;
        case Ack => Array(ACK_FLAG)
      }
    }

    override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
      val buf = ByteBuffer.wrap(bytes).order(order);
      val flag = buf.get;
      flag match {
        case ACK_FLAG => Ack
        case SIZEDTHROUGHPUTMESSAGE_FLAG => {
          val aux = buf.getInt;
          val length = buf.getInt;
          val data = new Array[Byte](length);
          buf.get(data, 0, length);
          SizedThroughputMessage(aux, data)
        }
      }
    }
  }

  class Source(messageSize: Int, batchSize: Int, batchCount: Int, sink: ActorRef) extends Actor {
    var latch: CountDownLatch = null;
    var sentBatches = 0;
    var ackedBatches = 0;
    var msg: SizedThroughputMessage = SizedThroughputMessage(1, {
      val data = new Array[Byte](messageSize)
      scala.util.Random.nextBytes(data);
      data
    })

    def send(): Unit = {
      sentBatches += 1;
      for (x <- 0 to batchSize) {
        sink ! msg;
      }
    }

    override def receive = {
      case Start(new_latch) => {
        sentBatches = 0;
        ackedBatches = 0;
        latch = new_latch;
        send();
        send();
      }
      case Ack => {
        ackedBatches += 1;
        if (sentBatches < batchCount) {
          send();
        } else if (ackedBatches == batchCount) {
          // Done
          latch.countDown();
        }
      }
    }
  }

  class Sink(batchSize: Int) extends Actor {
    var received = 0;
    override def receive = {
      case SizedThroughputMessage(aux, data) => {
        received += aux.toInt;
        if (received == batchSize) {
          received = 0;
          sender() ! Ack;
        }
      }
    }
  }
}
