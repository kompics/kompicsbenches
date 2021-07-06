package se.kth.benchmarks.akka

import java.util.concurrent.CountDownLatch
import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.serialization.Serializer
import akka.util.ByteString
import se.kth.benchmarks.akka.bench.AtomicRegister.ClientRef

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import PartitioningEvents._

import scala.util.{Failure, Success}
import akka.actor.ActorLogging
import se.kth.benchmarks.test.KVTestUtil.{KVTimestamp, ReadInvokation, ReadResponse, WriteInvokation, WriteResponse}

import scala.collection.immutable.List
import scala.language.postfixOps

class PartitioningActor(prepare_latch: CountDownLatch,
                        finished_latch: Option[CountDownLatch],
                        init_id: Int,
                        nodes: List[ClientRef],
                        num_keys: Long,
                        partition_size: Int,
                        test_promise: Option[Promise[List[KVTimestamp]]] @unchecked)
    extends Actor
    with ActorLogging {
  implicit val ec = scala.concurrent.ExecutionContext.global;

  val active_nodes = if (partition_size < nodes.size) nodes.slice(0, partition_size) else nodes
  var resolved_active_nodes: List[ActorRef] = null
  val n = active_nodes.size
  var init_ack_count: Int = 0
  var done_count = 0
  var test_results = ListBuffer[KVTimestamp]()

  override def receive: Receive = {
    case Start => {
      log.debug("Received Start")
      val actorRefsF =
        Future.sequence(active_nodes.map(node => context.actorSelection(node.actorPath).resolveOne(6 seconds)))
      actorRefsF.onComplete {
        case Success(actorRefs) => {
          self ! ResolvedNodes(actorRefs);
        }
        case Failure(ex) => {
          log.error("Failed to resolve ActorPaths!", ex);
          System.exit(1); // prevent deadlock
        }
      }
      // TODO: The following should work but sometimes fails to solve actorpath :(
      /*for ((node, rank) <- active_nodes.zipWithIndex){
        try {
          val f = context.actorSelection(node.actorPath).resolveOne(5 seconds)
          val a_ref: ActorRef = Await.result(f, 5 seconds)
          a_ref ! INIT(rank, init_id, active_nodes, min_key, max_key)
        } catch {
          case _ => {
            val a_path = context.actorSelection(node.actorPath)
            log.info(s"Could not resolve ActorRef. Sending INIT to ActorPath instead: $a_path")
            a_path ! INIT(rank, init_id, active_nodes, min_key, max_key)
          }
        }*/
    }

    case ResolvedNodes(resolvedRefs) => {
      log.debug("Resolved Nodes")
      val min_key: Long = 0L
      val max_key: Long = num_keys - 1
      resolved_active_nodes = resolvedRefs
      for ((node, rank) <- resolvedRefs.zipWithIndex) {
        node ! Init(rank, init_id, active_nodes, min_key, max_key)
      }
    }

    case InitAck(init_id) => {
      init_ack_count += 1
      if (init_ack_count == n) {
        prepare_latch.countDown()
      }
    }

    case Run => {
      resolved_active_nodes.foreach(ref => ref ! Run)
    }
    case Done => {
      log.debug("Got Done!")
      done_count += 1
      if (done_count == n) {
        log.debug("Done!!!")
        finished_latch.get.countDown()
      }
    }
    case TestDone(timestamps) => {
      done_count += 1
      test_results ++= timestamps
      if (done_count == n) test_promise.get.success(test_results.toList)
    }

  }
}

object PartitioningEvents {
  case object Start
  case class ResolvedNodes(actorRefs: List[ActorRef])
  case class Init(rank: Int, init_id: Int, nodes: List[ClientRef], min: Long, max: Long)
  case class InitAck(init_id: Int)
  case object Run
  case object Done
  case class TestDone(timestamps: List[KVTimestamp])
  case object Identify
}

object PartitioningActorSerializer {
  val NAME = "partitioningactor"

  val INIT_FLAG: Byte = 1
  val INIT_ACK_FLAG: Byte = 2
  val RUN_FLAG: Byte = 3
  val DONE_FLAG: Byte = 4
  val TESTDONE_FLAG: Byte = 5
  val IDENTIFY_FLAG: Byte = 6
  /* Bytes to represent test read and write operations of a KVTimestamp*/
  val WRITEINVOKATION_FLAG: Byte = 6
  val READINVOKATION_FLAG: Byte = 7
  val WRITERESPONSE_FLAG: Byte = 8
  val READRESPONSE_FLAG: Byte = 9
}

class PartitioningActorSerializer extends Serializer {
  import PartitioningActorSerializer._
  import java.nio.{ByteBuffer, ByteOrder}

  implicit val order = ByteOrder.BIG_ENDIAN;

  override def identifier: Int = SerializerIds.PARTITACTOR
  override def includeManifest: Boolean = false

  override def toBinary(o: AnyRef): Array[Byte] = {
    o match {
      case i: Init => {
        val bs = ByteString.createBuilder.putByte(INIT_FLAG)
        bs.putInt(i.rank)
        bs.putInt(i.init_id)
        bs.putLong(i.min)
        bs.putLong(i.max)
        bs.putInt(i.nodes.size)
        for (cRef <- i.nodes) {
          SerUtils.stringIntoByteString(bs, cRef.actorPath);
        }
        bs.result().toArray
      }
      case ack: InitAck => {
        ByteString.createBuilder.putByte(INIT_ACK_FLAG).putInt(ack.init_id).result().toArray
      }
      case td: TestDone => {
        val bs = ByteString.createBuilder.putByte(TESTDONE_FLAG)
        bs.putInt(td.timestamps.size)
        for (ts <- td.timestamps){
          bs.putLong(ts.key)
          ts.operation match {
            case ReadInvokation => bs.putByte(READINVOKATION_FLAG)
            case ReadResponse => {
              bs.putByte(READRESPONSE_FLAG)
              bs.putInt(ts.value.get)
            }
            case WriteInvokation => {
              bs.putByte(WRITEINVOKATION_FLAG)
              bs.putInt(ts.value.get)
            }
            case WriteResponse => {
              bs.putByte(WRITERESPONSE_FLAG)
              bs.putInt(ts.value.get)
            }
          }
          bs.putLong(ts.time)
          bs.putInt(ts.sender)
        }
        bs.result().toArray
      }
      case Run      => Array(RUN_FLAG)
      case Done     => Array(DONE_FLAG)
      case Identify => Array(IDENTIFY_FLAG)
    }

  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val buf = ByteBuffer.wrap(bytes).order(order);
    val flag = buf.get;
    flag match {
      case INIT_FLAG => {
        val rank = buf.getInt
        val initId = buf.getInt
        val min = buf.getLong
        val max = buf.getLong
        val n = buf.getInt
        var nodes = new ListBuffer[ClientRef]
        for (_ <- 0 until n) {
          val cRef = SerUtils.stringFromByteBuffer(buf);
          nodes += ClientRef(cRef)
        }
        Init(rank, initId, nodes.toList, min, max)
      }
      case TESTDONE_FLAG => {
        var results = new ListBuffer[KVTimestamp]()
        val n = buf.getInt
        for (_ <- 0 until n){
          val key = buf.getLong
          val operation = buf.get
          operation match {
            case READINVOKATION_FLAG => {
              val time = buf.getLong
              val sender = buf.getInt
              results += KVTimestamp(key, ReadInvokation, None, time, sender)
            }
            case _ => {
              val op = operation match {
                case READRESPONSE_FLAG => ReadResponse
                case WRITEINVOKATION_FLAG => WriteInvokation
                case WRITERESPONSE_FLAG => WriteResponse
              }
              val value = buf.getInt
              val time = buf.getLong
              val sender = buf.getInt
              results += KVTimestamp(key, op, Some(value), time, sender)
            }
          }
        }
        TestDone(results.toList)
      }
      case INIT_ACK_FLAG => InitAck(buf.getInt)
      case RUN_FLAG      => Run
      case DONE_FLAG     => Done
      case IDENTIFY_FLAG => Identify
    }
  }
}
