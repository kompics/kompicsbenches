package se.kth.benchmarks.kompicsscala

import java.net.{InetAddress, InetSocketAddress}
import java.util.Optional
import java.util.concurrent.CountDownLatch

import io.netty.buffer.ByteBuf
import se.sics.kompics.network.Network
import se.sics.kompics.network.netty.serialization.{Serializer, Serializers}
import se.sics.kompics.sl.{ComponentDefinition, Init => KompicsInit, KompicsEvent, Start, handle}
import PartitioningComp.{Done, Init, InitAck, Run}

import scala.collection.mutable.ListBuffer

class PartitioningComp(init: KompicsInit[PartitioningComp]) extends ComponentDefinition {
  val net = requires[Network];

  val KompicsInit(prepare_latch: CountDownLatch,
                  finished_latch: CountDownLatch,
                  init_id: Int,
                  nodes: List[NetAddress] @unchecked,
                  num_keys: Long,
                  partition_size: Int) = init;
  val active_nodes = if (partition_size < nodes.size) nodes.slice(0, partition_size) else nodes;
  val n = active_nodes.size;
  var init_ack_count: Int = 0;
  var done_count = 0;
  lazy val selfAddr = cfg.getValue[NetAddress](KompicsSystemProvider.SELF_ADDR_KEY);

  ctrl uponEvent {
    case _: Start => {
      assert(selfAddr != null)
      val min_key: Long = 0L
      val max_key: Long = num_keys - 1
      for ((node, rank) <- active_nodes.zipWithIndex) {
        trigger(NetMessage.viaTCP(selfAddr, node)(Init(rank, init_id, active_nodes, min_key, max_key)) -> net)
      }
    }
    case Run => {
      for (node <- active_nodes) {
        trigger(NetMessage.viaTCP(selfAddr, node)(Run) -> net)
      }
    }
  }

  net uponEvent {
    case NetMessage(_, InitAck(init_id)) => {
      init_ack_count += 1
      if (init_ack_count == n) {
        prepare_latch.countDown()
      }
    }
    case NetMessage(header, Done) => {
      done_count += 1
      if (done_count == n) {
        logger.info("Everybody is done")
        finished_latch.countDown()
      }
    }
  }
}

object PartitioningComp {
  case class Init(rank: Int, init_id: Int, nodes: List[NetAddress], min: Long, max: Long) extends KompicsEvent;
  case class InitAck(init_id: Int) extends KompicsEvent;
  case object Run extends KompicsEvent;
  case object Done extends KompicsEvent;
}

object PartitioningCompSerializer extends Serializer {
  private val INIT_FLAG: Byte = 1
  private val INIT_ACK_FLAG: Byte = 2
  private val RUN_FLAG: Byte = 3
  private val DONE_FLAG: Byte = 4

  def register(): Unit = {
    Serializers.register(this, "partitioningcomp")
    Serializers.register(classOf[Init], "partitioningcomp")
    Serializers.register(classOf[InitAck], "partitioningcomp")
    Serializers.register(Run.getClass, "partitioningcomp")
    Serializers.register(Done.getClass, "partitioningcomp")
  }

  override def identifier(): Int = se.kth.benchmarks.kompics.SerializerIds.S_PART_COMP

  override def toBinary(o: Any, buf: ByteBuf): Unit = {
    o match {
      case i: Init => {
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
      case ack: InitAck => {
        buf.writeByte(INIT_ACK_FLAG)
        buf.writeInt(ack.init_id)
      }
      case Run  => buf.writeByte(RUN_FLAG)
      case Done => buf.writeByte(DONE_FLAG)
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
        Init(rank, init_id, nodes.toList, min, max)
      }
      case INIT_ACK_FLAG => InitAck(buf.readInt())
      case RUN_FLAG      => Run
      case DONE_FLAG     => Done

      case _ => {
        Console.err.print(s"Got invalid ser flag: $flag");
        null
      }
    }
  }
}
