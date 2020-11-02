package se.kth.benchmarks.kompicsscala.bench

import java.util.{Optional, UUID}
import java.util.concurrent.CountDownLatch

import _root_.kompics.benchmarks.benchmarks.SizedThroughputRequest
import com.typesafe.scalalogging.StrictLogging
import io.netty.buffer.ByteBuf
import se.kth.benchmarks._
import se.kth.benchmarks.kompicsscala._
import se.sics.kompics.network.Network
import se.sics.kompics.network.netty.serialization.{Serializer, Serializers}
import se.sics.kompics.sl._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.Try

object SizedThroughput extends DistributedBenchmark {

  implicit val ec: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global;

  case class ClientParams(numPairs: Int, batchSize: Int)

  override type MasterConf = SizedThroughputRequest;
  override type ClientConf = ClientParams;
  override type ClientData = NetAddress;

  class MasterImpl extends Master with StrictLogging {
    private var params: MasterConf = null;
    private var system: KompicsSystem = null;
    private var sources: List[UUID] = List.empty;
    private var latch: CountDownLatch = null;

    override def setup(c: MasterConf, _meta: DeploymentMetaData): Try[ClientConf] = Try {
      logger.info("Setting up Master, numPairs: " ++ c.numberOfPairs.toString
        ++ ", message size: " ++ c.messageSize.toString
        ++ " batchSize: " ++ c.batchSize.toString
        ++  ", batchCount: " ++ c.numberOfBatches.toString);
      SizedThroughputSerializer.register();

      this.params = c;
      this.system = KompicsSystemProvider.newRemoteKompicsSystem(Runtime.getRuntime.availableProcessors());
      ClientParams(c.numberOfPairs, c.batchSize)
    };

    override def prepareIteration(d: List[ClientData]): Unit = {
      logger.debug("Preparing iteration");
      val sink = d.head;
      logger.trace(s"Resolved path to ${sink}");
      val latch = new CountDownLatch(this.params.numberOfPairs.toInt);

      val lf = (0 to params.numberOfPairs).map { index =>
        for {
          sourceId <- system.createNotify[Source](Init[Source](params.messageSize,
                                  params.batchSize,
                                  params.numberOfBatches,
                                  index,
                                  sink,
                                  latch)
          );
          _ <- system.connectNetwork(sourceId)
        } yield {
          sourceId
        }
      }.toList;
      val fl = Future.sequence(lf);
      val l = Await.result(fl, Duration.Inf);
      sources = l;
      this.latch = latch;
    }
    override def runIteration(): Unit = {
      val startLF = sources.map(source => system.startNotify(source));
      val startFL = Future.sequence(startLF);
      Await.result(startFL, Duration.Inf);
      latch.await();
    };
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      logger.debug("Cleaning up source side");
      assert(system != null);
      if (latch != null) {
        latch = null;
      }
      val killSourcesLF = sources.map(source => system.killNotify(source));
      val killSourcesFL = Future.sequence(killSourcesLF);
      sources = List.empty;
      Await.result(killSourcesFL, Duration.Inf);

      if (lastIteration) {
        sources = List.empty;
        val f = system.terminate();
        system = null;
        logger.info("Cleaned up Master.");
      }
    }
  }

  class ClientImpl extends Client with StrictLogging {
    private var system: KompicsSystem = null;
    private var sinks: List[UUID] = null;

    override def setup(c: ClientConf): ClientData = {
      logger.info("Setting up Client, numPairs: " ++ c.numPairs.toString ++ " batchSize: " ++ c.batchSize.toString);

      this.system = KompicsSystemProvider.newRemoteKompicsSystem(1);
      SizedThroughputSerializer.register();

      val lf = (0 to c.numPairs).map { index =>
        for {
          sinkId <- system.createNotify[Sink](Init[Sink](index, c.batchSize));
          _ <- system.connectNetwork(sinkId);
          _ <- system.startNotify(sinkId)
        } yield {
          sinkId
        }
      }.toList;
      val fl = Future.sequence(lf);
      val l = Await.result(fl, Duration.Inf);
      sinks = l;
      system.networkAddress.get
    }
    override def prepareIteration(): Unit = {
      // nothing
      logger.debug("Preparing sink iteration");
      assert(system != null);
    }
    override def cleanupIteration(lastIteration: Boolean): Unit = {
      logger.debug("Cleaning up sink side");
      assert(system != null);
      if (lastIteration) {
        sinks = List.empty; // will be stopped when system is shut down
        system.terminate();
        system = null;
        logger.info("Cleaned up Client.");
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
  override def strToClientData(str: String): Try[ClientData] = NetAddress.fromString(str);

  override def clientConfToString(c: ClientConf): String = s"${c.numPairs},${c.batchSize}";
  override def clientDataToString(d: ClientData): String = d.asString;

  case class SizedThroughputMessage(id: Int, aux: Int, data: Array[Byte]) extends KompicsEvent;
  case class Ack(id: Int) extends KompicsEvent;

  object SizedThroughputSerializer extends Serializer {

    val NAME: String = "sizedthroughput";

    def register(): Unit = {
      Serializers.register(this, NAME);
      Serializers.register(classOf[SizedThroughputMessage], NAME);
      Serializers.register(classOf[Ack], NAME);
    }

    val NO_HINT: Optional[Object] = Optional.empty();

    private val SIZEDTHROUGHPUTMESSAGE_FLAG: Byte = 1;
    private val ACK_FLAG: Byte = 2;

    override def identifier(): Int = se.kth.benchmarks.kompics.SerializerIds.S_NETTPPP;

    override def toBinary(o: Any, buf: ByteBuf): Unit = {
      o match {
        case SizedThroughputMessage(id, aux, data)  => buf.writeByte(SIZEDTHROUGHPUTMESSAGE_FLAG)
          .writeInt(id).writeByte(aux).writeInt(data.length).writeBytes(data)
        case Ack(id)  => buf.writeByte(ACK_FLAG).writeInt(id)
      }
    }

    override def fromBinary(buf: ByteBuf, hint: Optional[Object]): Object = {
      val flag = buf.readByte();
      flag match {
        case SIZEDTHROUGHPUTMESSAGE_FLAG => {
          val id = buf.readInt();
          val aux = buf.readByte();
          val length = buf.readInt();
          val data = new Array[Byte](length);
          buf.readBytes(data, 0, length);
          SizedThroughputMessage(id, aux, data)
        }
        case ACK_FLAG => {
          val id = buf.readInt();
          Ack(id)
        }
        case _ => {
          Console.err.print(s"Got invalid ser flag: $flag");
          null
        }
      }
    }
  }

  class Source(init: Init[Source]) extends ComponentDefinition {

    val Init(messageSize: Int,
             batchSize: Int,
             batchCount: Int,
             selfId: Int,
             sink: NetAddress,
             latch: CountDownLatch) =
      init;

    val net = requires[Network];

    lazy val selfAddr = cfg.getValue[NetAddress](KompicsSystemProvider.SELF_ADDR_KEY);

    private var sentBatches = 0;
    private var ackedBatches = 0;
    private var msg = SizedThroughputMessage(selfId, 1, {
        val data = new Array[Byte](messageSize)
        scala.util.Random.nextBytes(data);
        data
      })

    def send(): Unit = {
      sentBatches += 1;
      for (x <- 0 to batchSize) {
        trigger((NetMessage.viaTCP(selfAddr, sink)(msg)), net);
      }
    }

    ctrl uponEvent {
      case _: Start => handle {
        // Send two batches
        send();
        send();
      }
    }

    net uponEvent {
      case context @ NetMessage(_, Ack(this.selfId)) =>
        handle {
          ackedBatches += 1;
          if (sentBatches < batchCount) {
            send()
          } else if (ackedBatches == batchCount) {
            // Done
            latch.countDown();
          }
        }
    }
  }

  class Sink(init: Init[Sink]) extends ComponentDefinition {

    val Init(selfId: Int, batchSize: Int) = init;

    val net = requires[Network];
    var received = 0;

    lazy val selfAddr = cfg.getValue[NetAddress](KompicsSystemProvider.SELF_ADDR_KEY);

    ctrl uponEvent {
      case _: Start =>
        handle {
          assert(selfAddr != null);
        }
    }

    net uponEvent {
      case context @ NetMessage(_, SizedThroughputMessage(this.selfId, aux, data)) =>
        handle {
          received += aux.toInt;
          if (received == batchSize) {
            received = 0;
            trigger(context.reply(selfAddr)(Ack(selfId)) -> net);
          }
        }
    }
  }
}
