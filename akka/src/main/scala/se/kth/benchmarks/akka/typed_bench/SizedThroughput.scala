package se.kth.benchmarks.akka.typed_bench

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, ActorRefResolver, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.AskPattern._
import akka.serialization.Serializer
import akka.util.{ByteString, Timeout}
import com.typesafe.scalalogging.StrictLogging
import kompics.benchmarks.benchmarks.SizedThroughputRequest
import se.kth.benchmarks.akka.typed_bench.NetThroughputPingPong.ActorReference
import se.kth.benchmarks.akka.typed_bench.SizedThroughput.ClientSystemSupervisor.StartSinks
import se.kth.benchmarks.akka.typed_bench.SizedThroughput.SystemSupervisor.{OperationSucceeded, RunIteration, StartSources, StopSources, SystemMessage}
import se.kth.benchmarks.akka.typed_bench.SizedThroughput.{Sink, Source, Ack, MsgForStaticSource}
import se.kth.benchmarks.akka.{ActorSystemProvider, SerUtils, SerializerBindings, SerializerIds}
import se.kth.benchmarks.{DeploymentMetaData, DistributedBenchmark}

import java.util.concurrent.CountDownLatch
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

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
    .addBinding[MsgForStaticSource](SizedThroughputSerializer.NAME);

  class MasterImpl extends Master with StrictLogging {

    implicit val ec = scala.concurrent.ExecutionContext.global;

    private var numPairs = -1;
    private var messageSize = -1;
    private var batchSize = -1;
    private var batchCount = -1;
    private var system: ActorSystem[SystemMessage] = null;
    private var sources: List[ActorRef[SizedThroughputMessage]] = List.empty;
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
        ++ ", batchCount: " ++ batchCount.toString);
      this.system = ActorSystemProvider.newRemoteTypedActorSystem[SystemMessage](SystemSupervisor(), "sizedthroughput_supervisor",
        Runtime.getRuntime.availableProcessors(),
        serializers);
      ClientParams(numPairs, batchSize)
    };

    override def prepareIteration(d: List[ClientData]): Unit = {
      if (setting_up) {
        // Only create sources the first time
        logger.info("Preparing first iteration");
        implicit val timeout: Timeout = 3.seconds;
        implicit val scheduler = system.scheduler;
        val f: Future[OperationSucceeded.type] =
          system.ask(ref => StartSources(ref, latch, messageSize, batchSize, batchCount, d.head));
        // val sinksF =
        //  Future.sequence(d.head.actorPaths.map(sinkPath => system.actorSelection(sinkPath).resolveOne(5 seconds)));
        // val sinks = Await.result(sinksF, 5 seconds);
        Await.result(f, 3 seconds);
        // logger.trace(s"Resolved paths to ${sinks.mkString}");
        run_id += 1
        /* sources = sinks.zipWithIndex.map {
          case (sink, i) => system.actorOf(Props(new Source(messageSize, batchSize, batchCount, sink)),
            s"source${run_id}_$i")
        }; */
        setting_up = false;
      } else {
        logger.info("Preparing iteration");
      }
    }

    override def runIteration(): Unit = {
      logger.info("running iteration");
      latch = new CountDownLatch(numPairs);
      system ! RunIteration(latch);
      latch.await();
    };

    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      logger.debug("Cleaning up Source side");
      if (latch != null) {
        latch = null;
      }
      if (lastIteration) {
        // system ! StopSources(selfRef);
        val f = system.terminate();
        Await.ready(system.whenTerminated, 5.seconds);
        system = null;
        logger.info("Cleaned up Master");
      }
    }
  }

  class ClientImpl extends Client with StrictLogging {
    private var system: ActorSystem[StartSinks] = null;
    private var sinks: List[ActorRef[SizedThroughputMessage]] = List.empty;

    override def setup(c: ClientConf): ClientData = {
      logger.info("Setting up Client, numPairs: " ++ c.numPairs.toString ++ " batchSize: " ++ c.batchSize.toString);
      system = ActorSystemProvider.newRemoteTypedActorSystem[StartSinks](ClientSystemSupervisor(), name = "sizedthroughput-clientsupervisor", threads = 1, serialization = serializers);
      implicit val timeout: Timeout = 3.seconds;
      implicit val scheduler = system.scheduler;
      val f: Future[ClientRefs] = system.ask(ref => StartSinks(ref, c.numPairs, c.batchSize));
      implicit val ec = scala.concurrent.ExecutionContext.global;

      //sinks = (1 to c.numPairs).map(i => system.actorOf(Props(new Sink(c.batchSize)), s"sink$i")).toList;
      //val paths = sinks.map(sink => ActorSystemProvider.actorPathForRef(sink, system));
      val ready = Await.ready(f, 5.seconds);
      ready.value.get match {
        case Success(paths) => {
          logger.info(s"Sink Paths are ${paths.actorPaths.mkString}");
          paths
        }
        case Failure(e) => ClientRefs(List.empty)
      }
    }

    override def prepareIteration(): Unit = {
      // nothing
      logger.info("Preparing Sink iteration");
    }

    override def cleanupIteration(lastIteration: Boolean): Unit = {
      logger.info("Cleaning up Sink side");
      if (lastIteration) {
        system.terminate();
        Await.ready(system.whenTerminated, 5.seconds);
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

  object ClientSystemSupervisor {
    case class StartSinks(replyTo: ActorRef[ClientRefs], numPairs: Long, batchSize: Long)

    def apply(): Behavior[StartSinks] = Behaviors.setup(context => new ClientSystemSupervisor(context))
  }

  class ClientSystemSupervisor(context: ActorContext[StartSinks]) extends AbstractBehavior[StartSinks](context) {
    val resolver = ActorRefResolver(context.system)

    private def getSinkPaths[T](refs: List[ActorRef[T]]): List[String] = {
      for (sinkRef <- refs) yield resolver.toSerializationFormat(sinkRef)
    }

    override def onMessage(msg: StartSinks): Behavior[StartSinks] = {
      val sinks = (1 to msg.numPairs.toInt).map(i => context.spawn(Sink(msg.batchSize), s"sink$i")).toList
      msg.replyTo ! ClientRefs(getSinkPaths[SizedThroughputMessage](sinks))
      this
    }
  }

  object SystemSupervisor {
    sealed trait SystemMessage

    case class StartSources(replyTo: ActorRef[OperationSucceeded.type],
                            latch: CountDownLatch,
                            messageSize: Long,
                            batchSize: Long,
                            batchCount: Long,
                            sinks: ClientRefs)
      extends SystemMessage

    case class RunIteration(latch: CountDownLatch) extends SystemMessage

    case class StopSources(replyTo: ActorRef[OperationSucceeded.type]) extends SystemMessage

    case object OperationSucceeded

    def apply(): Behavior[SystemMessage] = Behaviors.setup(context => new SystemSupervisor(context))
  }

  class SystemSupervisor(context: ActorContext[SystemMessage]) extends AbstractBehavior[SystemMessage](context) {
    val resolver = ActorRefResolver(context.system);
    var sources: List[ActorRef[MsgForStaticSource]] = null;
    var run_id: Int = -1;

    override def onMessage(msg: SystemMessage): Behavior[SystemMessage] = {
      msg match {
        case s: StartSources => {
          sources = s.sinks.actorPaths.zipWithIndex.map {
            case (sink, i) =>
              context.spawn(Source(s.messageSize, s.batchSize, s.batchCount, sink),
                s"typed_source${run_id}_$i")
          }
          s.replyTo ! OperationSucceeded
          this
        }
        case RunIteration(latch) => {
          context.log.info(s"Running iteration with ${sources.length} sources");
          sources.foreach(source => source ! Start(latch))
          this
        }
        case StopSources(replyTo) => {
          if (sources.nonEmpty) {
            sources.foreach(source => context.stop(source))
            sources = List.empty
          }
          replyTo ! OperationSucceeded
          this
        }
      }
    }
  }

  sealed trait MsgForStaticSource
  case class Start(latch: CountDownLatch) extends MsgForStaticSource
  case object Ack extends MsgForStaticSource

  case class SizedThroughputMessage(aux: Int, data: Array[Byte], src: ActorReference);


  object SizedThroughputSerializer {
    val NAME = "sizedthroughput";
    private val SIZEDTHROUGHPUTMESSAGE_FLAG: Byte = 1;
    private val ACK_FLAG: Byte = 2;
  }

  class SizedThroughputSerializer extends Serializer {

    import SizedThroughputSerializer._

    import java.nio.{ByteBuffer, ByteOrder}

    implicit val order = ByteOrder.BIG_ENDIAN;

    override def identifier: Int = SerializerIds.SIZEDTHROUGHPUT;

    override def includeManifest: Boolean = false;

    override def toBinary(o: AnyRef): Array[Byte] = {
      o match {
        case SizedThroughputMessage(aux, data, src) => {
          val br = ByteString.createBuilder.putByte(SIZEDTHROUGHPUTMESSAGE_FLAG).putInt(aux).putInt(data.length).putBytes(data);
          SerUtils.stringIntoByteString(br, src.actorPath);
          br.result().toArray
        };
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
          val src = ActorReference(SerUtils.stringFromByteBuffer(buf));
          SizedThroughputMessage(aux, data, src)
        }
      }
    }
  }

  object Source {
    def apply(messageSize: Long, batchSize: Long, batchCount: Long, sinkPath: String): Behavior[MsgForStaticSource] =
      Behaviors.setup(context => new Source(context, messageSize, batchSize, batchCount, sinkPath))
  }

  class Source(context: ActorContext[MsgForStaticSource], messageSize: Long, batchSize: Long, batchCount: Long, sinkPath: String) extends AbstractBehavior[MsgForStaticSource](context) {
    val resolver = ActorRefResolver(context.system);
    val sink: ActorRef[SizedThroughputMessage] = resolver.resolveActorRef(sinkPath);
    var latch: CountDownLatch = null;
    var sentBatches = 0;
    var ackedBatches = 0;
    val selfRef = ActorReference(resolver.toSerializationFormat(context.self));

    var msg: SizedThroughputMessage = SizedThroughputMessage(1, {
      val data = new Array[Byte](messageSize.toInt)
      scala.util.Random.nextBytes(data);
      data
    },
      selfRef
    )

    def send(): Unit = {
      sentBatches += 1;
      for (x <- 0L to batchSize) {
        sink ! msg;
      }
    }

    override def onMessage(msg: MsgForStaticSource): Behavior[MsgForStaticSource] = {
      msg match {
        case Start(new_latch) => {
          context.log.info("Source starting");
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
            context.log.info("Source done");
            latch.countDown();
          }
        }
      }
      this
    }
  }

  object Sink {
    def apply(batchSize: Long): Behavior[SizedThroughputMessage] =
      Behaviors.setup(context => new Sink(context, batchSize))
  }

  class Sink(context: ActorContext[SizedThroughputMessage], batchSize: Long) extends AbstractBehavior[SizedThroughputMessage](context) {
    var received = 0;
    val resolver = ActorRefResolver(context.system)

    private def getSourceRef(a: ActorReference): ActorRef[MsgForStaticSource] = {
      resolver.resolveActorRef(a.actorPath)
    }

    override def onMessage(msg: SizedThroughputMessage): Behavior[SizedThroughputMessage] = {
      msg match {
        case SizedThroughputMessage(aux, data, sender) => {
          received += aux.toInt;
          if (received == batchSize) {
            received = 0;
            getSourceRef(sender) ! Ack;
          }
          this
        }
      }
    }
  }
}
