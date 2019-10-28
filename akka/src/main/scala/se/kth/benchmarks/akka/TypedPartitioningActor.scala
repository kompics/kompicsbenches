package se.kth.benchmarks.akka

import java.util.concurrent.CountDownLatch

import akka.actor.typed.{ActorRef, ActorRefResolver, Behavior, Signal}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import TypedPartitioningActor._
import akka.actor.TypedActor.PreStart
import akka.serialization.Serializer
import akka.util.ByteString
import se.kth.benchmarks.akka.typed_bench.AtomicRegister.{AtomicRegisterMessage, ClientRef, Init, Run => ATOMICREG_RUN}

import scala.collection.mutable.ListBuffer

object TypedPartitioningActor {
  def apply(prepare_latch: CountDownLatch,
            finished_latch: CountDownLatch,
            init_id: Int,
            nodes: List[ClientRef],
            num_keys: Long,
            partition_size: Int): Behavior[PartitioningMessage] =
    Behaviors.setup(
      context =>
        new TypedPartitioningActor(context, prepare_latch, finished_latch, init_id, nodes, num_keys, partition_size)
    )

  trait PartitioningMessage

  case object Start extends PartitioningMessage
  case object Run extends PartitioningMessage
  case object Done extends PartitioningMessage
  case class InitAck(init_id: Int) extends PartitioningMessage
}

class TypedPartitioningActor(context: ActorContext[PartitioningMessage],
                             prepare_latch: CountDownLatch,
                             finished_latch: CountDownLatch,
                             init_id: Int,
                             nodes: List[ClientRef],
                             num_keys: Long,
                             partition_size: Int)
    extends AbstractBehavior[PartitioningMessage] {

//  val logger = context.log

  val active_nodes = if (partition_size < nodes.size) nodes.slice(0, partition_size) else nodes
  val n = active_nodes.size
  var init_ack_count: Int = 0
  var done_count = 0
  val resolver = ActorRefResolver(context.system)
  val selfRef = ClientRef(resolver.toSerializationFormat(context.self))

  private def getActorRef(c: ClientRef): ActorRef[AtomicRegisterMessage] = {
    resolver.resolveActorRef(c.ref)
  }

  override def onMessage(msg: PartitioningMessage): Behavior[PartitioningMessage] = {
    msg match {
      case Start => {
        val min_key: Long = 0L
        val max_key: Long = num_keys - 1
        for ((node, rank) <- active_nodes.zipWithIndex) {
          val actorRef = getActorRef(node)
          actorRef ! Init(selfRef, rank, init_id, active_nodes, min_key, max_key)
        }
      }

      case InitAck(init_id) => {
        init_ack_count += 1
        if (init_ack_count == n) {
          prepare_latch.countDown()
        }
      }

      case Run => {
        for (node <- active_nodes) {
          val actorRef = getActorRef(node)
          actorRef ! ATOMICREG_RUN
        }
      }

      case Done => {
        done_count += 1
        if (done_count == n) {
          finished_latch.countDown()
        }
      }
    }
    this
  }
}

object TypedPartitioningActorSerializer {
  val NAME = "typed_partitioningactor"

  private val INIT_FLAG: Byte = 1
  private val INIT_ACK_FLAG: Byte = 2
  private val RUN_FLAG: Byte = 3
  private val DONE_FLAG: Byte = 4
}

class TypedPartitioningActorSerializer extends Serializer {
  import TypedPartitioningActorSerializer._
  import java.nio.{ByteBuffer, ByteOrder}

  implicit val order = ByteOrder.BIG_ENDIAN;

  override def identifier: Int = SerializerIds.TYPEDPARTITACTOR
  override def includeManifest: Boolean = false

  override def toBinary(o: AnyRef): Array[Byte] = {
    o match {
      case i: Init => {
        val bs = ByteString.createBuilder.putByte(INIT_FLAG)
        val src_bytes = i.src.ref.getBytes
        bs.putShort(src_bytes.size)
        bs.putBytes(src_bytes)
        bs.putInt(i.rank)
        bs.putInt(i.init_id)
        bs.putLong(i.min)
        bs.putLong(i.max)
        bs.putInt(i.nodes.size)
        for (ClientRef(node: String) <- i.nodes) {
          val node_bytes = node.getBytes
          bs.putShort(node_bytes.size)
          bs.putBytes(node_bytes)
        }
        bs.result().toArray
      }
      case ack: InitAck => {
        ByteString.createBuilder.putByte(INIT_ACK_FLAG).putInt(ack.init_id).result().toArray
      }
      case Run  => Array(RUN_FLAG)
      case Done => Array(DONE_FLAG)
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
        for (_ <- 0 until n) {
          val string_length: Int = buf.getShort
          val bytes = new Array[Byte](string_length)
          buf.get(bytes)
          val clientRef = ClientRef(bytes.map(_.toChar).mkString)
          nodes += clientRef

        }
        Init(src, rank, init_id, nodes.toList, min, max)
      }
      case INIT_ACK_FLAG => InitAck(buf.getInt)
      case RUN_FLAG      => Run
      case DONE_FLAG     => Done
    }
  }
}
