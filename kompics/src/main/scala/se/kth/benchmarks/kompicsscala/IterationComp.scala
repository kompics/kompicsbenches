package se.kth.benchmarks.kompicsscala

import java.net.{ InetAddress, InetSocketAddress }
import java.util.Optional
import java.util.concurrent.CountDownLatch

import io.netty.buffer.ByteBuf
import se.kth.benchmarks.kompicsscala.bench.AtomicRegister.DONE
import se.sics.kompics.network.Network
import se.sics.kompics.network.netty.serialization.{ Serializer, Serializers }
import se.sics.kompics.sl.{ ComponentDefinition, Init, KompicsEvent, Start, handle }

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

// used to send topology and rank to nodes in atomic register
class IterationComp(init: Init[IterationComp]) extends ComponentDefinition {
  val net = requires[Network]

  val Init(prepare_latch: CountDownLatch, finished_latch: CountDownLatch, init_id: Int, nodes: List[NetAddress], num_keys: Long, partition_size: Int) = init
  val n = nodes.size
  var init_ack_count: Int = n
  var done_count = n
  var partitions = mutable.Map[Long, List[NetAddress]]()
  lazy val selfAddr = cfg.getValue[NetAddress](KompicsSystemProvider.SELF_ADDR_KEY);

  ctrl uponEvent {
    case _: Start => handle {
      assert(selfAddr != null)
      val num_partitions: Long = n / partition_size
      val offset = num_keys / num_partitions
      var rank = 0
      var j = 0
      logger.info(s"Start partitioning: #keys=$num_keys, #partitions=$num_partitions, #offset=$offset")
      for (i <- 0l until num_partitions - 1) {
        val min = i * offset
        val max = min + offset - 1
        val partition = nodes.slice(j, j + partition_size)
        val num_nodes = partition.size
        logger.info(s"Partition $i, #nodes=$num_nodes, keys: $min - $max")
        for (node <- partition) {
          trigger(NetMessage.viaTCP(selfAddr, node)(INIT(rank, init_id, partition, min, max)) -> net)
          rank += 1
        }
        partitions += (i -> partition)
        j += partition_size
      }
      // rest of the keys in the last partition
      val last_min = (num_partitions - 1) * offset
      val last_max = num_keys - 1
      val partition = nodes.slice(j, j + partition_size)
      val num_nodes = partition.size
      logger.info(s"Last Partition, #nodes=$num_nodes, keys: $last_min - $last_max")
      for (node <- partition) {
        trigger(NetMessage.viaTCP(selfAddr, node)(INIT(rank, init_id, partition, last_min, last_max)) -> net)
        rank += 1
      }
      partitions += (num_partitions - 1 -> partition)

    }
    case RUN => handle {
      logger.info("Sending RUN to everybody...")
      for (node <- nodes) {
        trigger(NetMessage.viaTCP(selfAddr, node)(RUN) -> net)
      }
    }
  }

  net uponEvent {
    case NetMessage(_, INIT_ACK(init_id)) => handle {
      init_ack_count -= 1
      if (init_ack_count == 0) {
        logger.info("Got INIT_ACK from everybody")
        prepare_latch.countDown()
      }
    }
    case NetMessage(header, DONE) => handle{
      logger.info("Got done from " + header.getSource())
      done_count -= 1
      if (done_count == 0) {
        logger.info("Everybody is done")
        finished_latch.countDown()
      }
    }
  }

}

case class INIT(rank: Int, init_id: Int, nodes: List[NetAddress], min: Long, max: Long) extends KompicsEvent;
case class INIT_ACK(init_id: Int) extends KompicsEvent;
case object RUN extends KompicsEvent;

object IterationCompSerializer extends Serializer {
  private val INIT_FLAG: Byte = 1
  private val INIT_ACK_FLAG: Byte = 2
  private val RUN_FLAG: Byte = 3

  def register(): Unit = {
    Serializers.register(this, "iterationcomp")
    Serializers.register(classOf[INIT], "iterationcomp")
    Serializers.register(classOf[INIT_ACK], "iterationcomp")
    Serializers.register(RUN.getClass, "iterationcomp")
  }

  override def identifier(): Int = se.kth.benchmarks.kompics.SerializerIds.S_IT_COMP

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
