package se.kth.benchmarks.akka

import java.util.concurrent.CountDownLatch

import akka.actor.typed.{ActorRef, ActorRefResolver, Behavior, Signal}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import TypedIterationActor._
import akka.actor.TypedActor.PreStart
import akka.serialization.Serializer
import akka.util.ByteString
import se.kth.benchmarks.akka.typed_bench.AtomicRegister.{AtomicRegisterMessage, ClientRef, INIT, RUN => ATOMICREG_RUN}

import scala.collection.mutable.ListBuffer

object TypedIterationActor {
  def apply(prepare_latch: CountDownLatch, finished_latch: CountDownLatch, init_id: Int, nodes: List[ClientRef], num_keys: Long, partition_size: Int): Behavior[IterationMessage] =
    Behaviors.setup(context => new TypedIterationActor(context, prepare_latch, finished_latch, init_id, nodes, num_keys, partition_size))

  trait IterationMessage

  case object START extends IterationMessage
  case object RUN extends IterationMessage
  case object DONE extends IterationMessage
  case class INIT_ACK(init_id: Int) extends IterationMessage
}


class TypedIterationActor(context: ActorContext[IterationMessage],
                          prepare_latch: CountDownLatch,
                          finished_latch: CountDownLatch,
                          init_id: Int,
                          nodes: List[ClientRef],
                          num_keys: Long,
                          partition_size: Int) extends AbstractBehavior[IterationMessage] {

//  val logger = context.log
  var active_nodes = nodes
  var n = nodes.size
  var init_ack_count: Int = 0
  var done_count = 0
  val min_key: Long = 0l
  val max_key: Long = num_keys - 1
  val resolver = ActorRefResolver(context.system)
  val selfRef = ClientRef(resolver.toSerializationFormat(context.self))

  private def getActorRef(c: ClientRef): ActorRef[AtomicRegisterMessage] = {
    resolver.resolveActorRef(c.ref)
  }

  override def onMessage(msg: IterationMessage): Behavior[IterationMessage] = {
    msg match {
      case START => {
        if (partition_size < n) {
          active_nodes = nodes.slice(0, partition_size)
          n = active_nodes.size
        }

        for ((node, rank) <- active_nodes.zipWithIndex){
          val actorRef = getActorRef(node)
          actorRef ! INIT(selfRef, rank, init_id, active_nodes, min_key, max_key)
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
          val actorRef = getActorRef(node)
          actorRef ! ATOMICREG_RUN
        }
      }

      case DONE => {
        done_count += 1
        if (done_count == n) {
          finished_latch.countDown()
        }
      }
    }
    this
  }
}

object TypedIterationActorSerializer {
  val NAME = "typed_iterationactor"

  private val INIT_FLAG: Byte = 1
  private val INIT_ACK_FLAG: Byte = 2
  private val RUN_FLAG: Byte = 3
  private val DONE_FLAG: Byte = 4
}

class TypedIterationActorSerializer extends Serializer {
  import TypedIterationActorSerializer._
  import java.nio.{ ByteBuffer, ByteOrder }

  implicit val order = ByteOrder.BIG_ENDIAN;

  override def identifier: Int = SerializerIds.TYPEDITACTOR
  override def includeManifest: Boolean = false


  override def toBinary(o: AnyRef): Array[Byte] = {
    o match {
      case i: INIT => {
        val bs = ByteString.createBuilder.putByte(INIT_FLAG)
        val src_bytes = i.src.ref.getBytes
        bs.putShort(src_bytes.size)
        bs.putBytes(src_bytes)
        bs.putInt(i.rank)
        bs.putInt(i.init_id)
        bs.putLong(i.min)
        bs.putLong(i.max)
        bs.putInt(i.nodes.size)
        for (ClientRef(node: String) <- i.nodes){
          val node_bytes = node.getBytes
          bs.putShort(node_bytes.size)
          bs.putBytes(node_bytes)
        }
        bs.result().toArray
      }
      case ack: INIT_ACK => {
        ByteString.createBuilder.putByte(INIT_ACK_FLAG).putInt(ack.init_id).result().toArray
      }
      case RUN => Array(RUN_FLAG)
      case DONE => Array(DONE_FLAG)
    }
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val buf = ByteBuffer.wrap(bytes).order(order);
    val flag = buf.get;
    flag match {
      case INIT_FLAG => {
        val src_length: Int = buf.getShort
        val bytes = new Array[Byte](src_length)
        buf.get(bytes)
        val src = ClientRef(bytes.map(_.toChar).mkString)
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
          val clientRef = ClientRef(bytes.map(_.toChar).mkString)
          nodes += clientRef

        }
        INIT(src, rank, init_id, nodes.toList, min, max)
      }
      case INIT_ACK_FLAG => INIT_ACK(buf.getInt)
      case RUN_FLAG => RUN
      case DONE_FLAG => DONE
    }
  }
}

