package se.kth.benchmarks.kompicsscala

import java.net.{ InetAddress, InetSocketAddress }
import java.util.Optional
import java.util.concurrent.CountDownLatch

import io.netty.buffer.ByteBuf
import se.kth.benchmarks.kompicsscala.bench.AtomicRegister.DONE
import se.sics.kompics.network.Network
import se.sics.kompics.network.netty.serialization.{ Serializer, Serializers }
import se.sics.kompics.sl.{ ComponentDefinition, Init, KompicsEvent, Start, handle }

import scala.collection.mutable.ListBuffer

// used to send topology and rank to nodes in atomic register
class PartitioningComp(init: Init[PartitioningComp]) extends ComponentDefinition {
  val net = requires[Network]

  val Init(prepare_latch: CountDownLatch, finished_latch: CountDownLatch, init_id: Int, nodes: List[NetAddress], num_keys: Long, partition_size: Int) = init
  val active_nodes = if (partition_size < nodes.size) nodes.slice(0, partition_size) else nodes
  val n = active_nodes.size
  var init_ack_count: Int = 0
  var done_count = 0
  lazy val selfAddr = cfg.getValue[NetAddress](KompicsSystemProvider.SELF_ADDR_KEY);

  ctrl uponEvent {
    case _: Start => handle {
      assert(selfAddr != null)
      val min_key: Long = 0l
      val max_key: Long = num_keys - 1
      for ((node, rank) <- active_nodes.zipWithIndex) {
        trigger(NetMessage.viaTCP(selfAddr, node)(INIT(rank, init_id, active_nodes, min_key, max_key)) -> net)
      }
    }
    case RUN => handle {
      for (node <- active_nodes) {
        trigger(NetMessage.viaTCP(selfAddr, node)(RUN) -> net)
      }
    }
  }

  net uponEvent {
    case NetMessage(_, INIT_ACK(init_id)) => handle {
      init_ack_count += 1
      if (init_ack_count == n) {
        prepare_latch.countDown()
      }
    }
    case NetMessage(header, DONE) => handle{
      done_count += 1
      if (done_count == n) {
        logger.info("Everybody is done")
        finished_latch.countDown()
      }
    }
  }
}

case class INIT(rank: Int, init_id: Int, nodes: List[NetAddress], min: Long, max: Long) extends KompicsEvent;
case class INIT_ACK(init_id: Int) extends KompicsEvent;
case object RUN extends KompicsEvent;

object PartitioningCompSerializer extends Serializer {
  private val INIT_FLAG: Byte = 1
  private val INIT_ACK_FLAG: Byte = 2
  private val RUN_FLAG: Byte = 3

  def register(): Unit = {
    Serializers.register(this, "partitioningcomp")
    Serializers.register(classOf[INIT], "partitioningcomp")
    Serializers.register(classOf[INIT_ACK], "partitioningcomp")
    Serializers.register(RUN.getClass, "partitioningcomp")
  }

  override def identifier(): Int = se.kth.benchmarks.kompics.SerializerIds.S_PART_COMP

  override def toBinary(o: Any, buf: ByteBuf): Unit = {
    o match {
      case i: INIT => {
        buf.writeByte(INIT_FLAG)
        buf.writeInt(i.rank)
        buf.writeInt(i.init_id)
        buf.writeLong(i.min)
        buf.writeLong(i.max)
        buf.writeInt(i.nodes.size)
        for (node <- i.nodes) {
          val ip = node.getIp().getAddress
          assert(ip.length == 4)
          buf.writeBytes(ip);
          buf.writeShort(node.getPort());
        }
      }
      case ack: INIT_ACK => {
        buf.writeByte(INIT_ACK_FLAG)
        buf.writeInt(ack.init_id)
      }
      case RUN => buf.writeByte(RUN_FLAG)
    }
  }

  override def fromBinary(buf: ByteBuf, optional: Optional[AnyRef]): AnyRef = {
    val flag = buf.readByte()
    flag match {
      case INIT_FLAG => {
        val rank = buf.readInt()
        val init_id = buf.readInt()
        val min = buf.readLong()
        val max = buf.readLong()
        val n = buf.readInt()
        var nodes = new ListBuffer[NetAddress]
        for (_ <- 0 until n) {
          val ipBytes = new Array[Byte](4)
          buf.readBytes(ipBytes)
          val addr = InetAddress.getByAddress(ipBytes)
          val port: Int = buf.readUnsignedShort()
          nodes += NetAddress(new InetSocketAddress(addr, port))
        }
        INIT(rank, init_id, nodes.toList, min, max)
      }
      case INIT_ACK_FLAG => INIT_ACK(buf.readInt())
      case RUN_FLAG      => RUN
      case _ => {
        Console.err.print(s"Got invalid ser flag: $flag");
        null
      }
    }
  }
}