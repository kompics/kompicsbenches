package se.kth.benchmarks.akka

import java.util.concurrent.CountDownLatch

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.serialization.Serializer
import akka.util.ByteString
import se.kth.benchmarks.akka.bench.AtomicRegister.{ClientRef, DONE}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration._
import IterationActor._

import scala.util.{Failure, Success}

class IterationActor(prepare_latch: CountDownLatch, finished_latch: CountDownLatch, init_id: Int, nodes: List[ClientRef], num_keys: Long, partition_size: Int) extends Actor{
  val logger = Logging(context.system, this)

  var active_nodes = nodes
  var n = nodes.size
  var init_ack_count: Int = 0
  var done_count = 0
  val min_key: Long = 0l
  val max_key: Long = num_keys - 1

  override def receive: Receive = {
    case START => {
      if (partition_size < n) {
        active_nodes = nodes.slice(0, partition_size)
        n = active_nodes.size
      }
      for ((node, rank) <- active_nodes.zipWithIndex){
        val f = context.actorSelection(node.actorPath).resolveOne(5 seconds)
        val ready = Await.ready(f, 5 seconds)
        ready.value.get match {
          case Success(a_ref) => a_ref ! INIT(rank, init_id, active_nodes, min_key, max_key)
          case Failure(_) => {
            val a_path = context.actorSelection(node.actorPath)
            logger.info(s"Could not resolve ActorRef. Sending INIT to ActorPath instead: $a_path")
            a_path ! INIT(rank, init_id, active_nodes, min_key, max_key)
          }
        }
        /*Await.result(f, 5 seconds)
        val a_ref: ActorRef = Await.result(f, 5 seconds)
        a_ref ! INIT(rank, init_id, active_nodes, min_key, max_key)*/
      }
    }

    case INIT_ACK(init_id) => {
      init_ack_count += 1
      if (init_ack_count == n) {
        prepare_latch.countDown()
      }
    }

    case RUN => {
      for (node <- active_nodes){
        val f = context.actorSelection(node.actorPath).resolveOne(5 seconds)
        val a_ref = Await.result(f, 5 seconds)
        a_ref ! RUN
      }
    }
    case DONE => {
      done_count += 1
      if (done_count == n) {
        finished_latch.countDown()
      }
    }

  }
}

object IterationActor{
  case object START
  case class INIT(rank: Int, init_id: Int, nodes: List[ClientRef], min: Long, max: Long)
  case class INIT_ACK(init_id: Int)
  case object RUN
}


object IterationActorSerializer {
  val NAME = "iterationactor"

  private val INIT_FLAG: Byte = 1
  private val INIT_ACK_FLAG: Byte = 2
  private val RUN_FLAG: Byte = 3
}

class IterationActorSerializer extends Serializer {
  import IterationActorSerializer._
  import java.nio.{ ByteBuffer, ByteOrder }

  implicit val order = ByteOrder.BIG_ENDIAN;

  override def identifier: Int = SerializerIds.ITACTOR
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

