package se.kth.benchmarks.akka

import java.util.concurrent.CountDownLatch

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.serialization.Serializer
import akka.util.ByteString
import se.kth.benchmarks.akka.bench.AtomicRegister.{ClientRef, DONE}

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import PartitioningActor.{ResolvedNodes, _}

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
    case START => {
      val actorRefsF = Future.sequence(active_nodes.map(node => context.actorSelection(node.actorPath).resolveOne(6 seconds)))
      actorRefsF.onComplete{
        case Success(actorRefs) => {
          /*for ((node, rank) <- actorRefs.zipWithIndex){
            node ! INIT(rank, init_id, active_nodes, min_key, max_key)
          }*/
          self ! ResolvedNodes(actorRefs)
        }
        case Failure(ex) => {
          logger.info(s"Failed to resolve ActorPaths: $ex")
        }
      }
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
        node ! INIT(rank, init_id, active_nodes, min_key, max_key)
      }
    }

    case INIT_ACK(init_id) => {
      init_ack_count += 1
      if (init_ack_count == n) {
        prepare_latch.countDown()
      }
    }

    case RUN => {
      resolved_active_nodes.foreach(ref => ref ! RUN)
      /*for (node <- active_nodes){
        val f = context.actorSelection(node.actorPath).resolveOne(5 seconds)
        val a_ref = Await.result(f, 5 seconds)
        a_ref ! RUN
      }*/
    }
    case DONE => {
      done_count += 1
      if (done_count == n) {
        finished_latch.countDown()
      }
    }

  }
}

object PartitioningActor{
  case object START
  case class ResolvedNodes(actorRefs: List[ActorRef])
  case class INIT(rank: Int, init_id: Int, nodes: List[ClientRef], min: Long, max: Long)
  case class INIT_ACK(init_id: Int)
  case object RUN
}


object PartitioningActorSerializer {
  val NAME = "partitioningactor"

  private val INIT_FLAG: Byte = 1
  private val INIT_ACK_FLAG: Byte = 2
  private val RUN_FLAG: Byte = 3
}

class PartitioningActorSerializer extends Serializer {
  import PartitioningActorSerializer._
  import java.nio.{ ByteBuffer, ByteOrder }

  implicit val order = ByteOrder.BIG_ENDIAN;

  override def identifier: Int = SerializerIds.PARTITACTOR
  override def includeManifest: Boolean = false

  override def toBinary(o: AnyRef): Array[Byte] = {
    o match {
      case i: INIT => {
        val bs = ByteString.createBuilder.putByte(INIT_FLAG)
        bs.putInt(i.rank)
        bs.putInt(i.init_id)
        bs.putLong(i.min)
        bs.putLong(i.max)
        bs.putInt(i.nodes.size)
        for (c_ref <- i.nodes){
//          println(s"Serializing path ${c_ref.actorPath}")
          val bytes = c_ref.actorPath.getBytes
          bs.putShort(bytes.size)
          bs.putBytes(bytes)
        }
        bs.result().toArray
      }
      case ack: INIT_ACK => {
        ByteString.createBuilder.putByte(INIT_ACK_FLAG).putInt(ack.init_id).result().toArray
      }
      case RUN => Array(RUN_FLAG)
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
        INIT(rank, init_id, nodes.toList, min, max)
      }
      case INIT_ACK_FLAG => INIT_ACK(buf.getInt)
      case RUN_FLAG => RUN
    }
  }
}

