package se.kth.benchmarks.akka

import java.util.concurrent.CountDownLatch

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.serialization.Serializer
import akka.util.ByteString
import se.kth.benchmarks.akka.bench.AtomicRegister.{ClientRef}

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import PartitioningActor.{Start, Done, ResolvedNodes, _}

import scala.util.{Failure, Success}

class PartitioningActor(prepare_latch: CountDownLatch, finished_latch: CountDownLatch, init_id: Int, nodes: List[ClientRef], num_keys: Long, partition_size: Int) extends Actor{
  val logger = Logging(context.system, this)
  implicit val ec = scala.concurrent.ExecutionContext.global;

  val active_nodes = if (partition_size < nodes.size) nodes.slice(0, partition_size) else nodes
  var resolved_active_nodes: List[ActorRef] = null
  val n = active_nodes.size
  var init_ack_count: Int = 0
  var done_count = 0

  override def receive: Receive = {
    case Start => {
      val actorRefsF = Future.sequence(active_nodes.map(node => context.actorSelection(node.actorPath).resolveOne(6 seconds)))
      actorRefsF.onComplete{
        case Success(actorRefs) => {
          self ! ResolvedNodes(actorRefs)
        }
        case Failure(ex) => {
          logger.info(s"Failed to resolve ActorPaths: $ex")
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
            logger.info(s"Could not resolve ActorRef. Sending INIT to ActorPath instead: $a_path")
            a_path ! INIT(rank, init_id, active_nodes, min_key, max_key)
          }
        }*/
    }

    case ResolvedNodes(resolvedRefs) => {
      val min_key: Long = 0l
      val max_key: Long = num_keys - 1
      resolved_active_nodes = resolvedRefs
      for ((node, rank) <- resolvedRefs.zipWithIndex){
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
      done_count += 1
      if (done_count == n) {
        finished_latch.countDown()
      }
    }

  }
}

object PartitioningActor{
  case object Start
  case class ResolvedNodes(actorRefs: List[ActorRef])
  case class Init(rank: Int, init_id: Int, nodes: List[ClientRef], min: Long, max: Long)
  case class InitAck(init_id: Int)
  case object Run
  case object Done
  case object Identify
}


object PartitioningActorSerializer {
  val NAME = "partitioningactor"

  private val INIT_FLAG: Byte = 1
  private val INIT_ACK_FLAG: Byte = 2
  private val RUN_FLAG: Byte = 3
  private val DONE_FLAG: Byte = 4
  private val IDENTIFY_FLAG: Byte = 5
}

class PartitioningActorSerializer extends Serializer {
  import PartitioningActorSerializer._
  import java.nio.{ ByteBuffer, ByteOrder }

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
        for (c_ref <- i.nodes){
          val bytes = c_ref.actorPath.getBytes
          bs.putShort(bytes.size)
          bs.putBytes(bytes)
        }
        bs.result().toArray
      }
      case ack: InitAck => {
        ByteString.createBuilder.putByte(INIT_ACK_FLAG).putInt(ack.init_id).result().toArray
      }
      case Run => Array(RUN_FLAG)
      case Done => Array(DONE_FLAG)
      case Identify => Array(IDENTIFY_FLAG)
    }

  }


  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val buf = ByteBuffer.wrap(bytes).order(order);
    val flag = buf.get;
    flag match {
      case INIT_FLAG => {
        val rank = buf.getInt
        val init_id = buf.getInt
        val min = buf.getLong
        val max = buf.getLong
        val n = buf.getInt
        var nodes = new ListBuffer[ClientRef]
        for (_ <- 0 until n){
          val string_length: Int = buf.getShort
          val bytes = new Array[Byte](string_length)
          buf.get(bytes)
          val c_ref = ClientRef(bytes.map(_.toChar).mkString)
          nodes += c_ref
        }
        Init(rank, init_id, nodes.toList, min, max)
      }
      case INIT_ACK_FLAG => InitAck(buf.getInt)
      case RUN_FLAG => Run
      case DONE_FLAG => Done
      case IDENTIFY_FLAG => Identify
    }
  }
}

