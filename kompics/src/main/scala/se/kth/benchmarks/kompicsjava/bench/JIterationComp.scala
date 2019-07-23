package se.kth.benchmarks.kompicsjava.bench

import java.util
import java.util.concurrent.CountDownLatch

import se.kth.benchmarks.kompicsscala._
import se.kth.benchmarks.kompicsjava.net.{ NetAddress => JNetAddress, NetMessage => JNetMessage }
import se.kth.benchmarks.kompicsjava.bench.atomicregister.events.{ INIT => JINIT, INIT_ACK => JINIT_ACK, RUN => JRUN, DONE => JDONE }
import se.sics.kompics.network.Network
import se.sics.kompics.sl.{ ComponentDefinition, Init, Start, handle }

import scala.collection.mutable

class JIterationComp(init: Init[JIterationComp]) extends ComponentDefinition {
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
      logger.info(s"Start partitioning: #nodes=$n #keys=$num_keys, #partitions=$num_partitions, #offset=$offset")
      for (i <- 0l until num_partitions - 1) {
        val min = i * offset
        val max = min + offset - 1
        val partition = nodes.slice(j, j + partition_size)
        val num_nodes = partition.size
        val jpartition = new util.LinkedList[JNetAddress]()
        for (node <- partition) jpartition.add(node.asJava)
        logger.info(s"Partition $i, #nodes=$num_nodes, keys: $min - $max")
        for (node <- partition) {
          trigger(JNetMessage.viaTCP(selfAddr.asJava, node.asJava, new JINIT(rank, init_id, jpartition, min, max)), net)
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
      val jpartition = new util.LinkedList[JNetAddress]()
      for (node <- partition) jpartition.add(node.asJava)
      logger.info(s"Last Partition, #nodes=$num_nodes, keys: $last_min - $last_max")
      for (node <- partition) {
        trigger(JNetMessage.viaTCP(selfAddr.asJava, node.asJava, new JINIT(rank, init_id, jpartition, last_min, last_max)), net)
        //        trigger(NetMessage.viaTCP(selfAddr, node)(new JINIT(rank, init_id, jpartition, last_min, last_max)) -> net)
        rank += 1
      }
      partitions += (num_partitions - 1 -> partition)

    }
    case RUN => handle {
      logger.info("Sending RUN to everybody...")
      for (node <- nodes) {
        trigger(JNetMessage.viaTCP(selfAddr.asJava, node.asJava, JRUN.event), net)
      }
    }

  }

  net uponEvent {
    /*case j: JNetMessage => handle{
      //      logger.info("GOT JAVA MESSAGE: extractValue=" + j.extractValue() + ", pattern=" + j.extractPattern())
      j.extractValue() match {
        case init_ack: JINIT_ACK =>
          if (init_ack.init_id == init_id) {
            //            logger.info("Got INIT_ACK from " + j.getSource().asString)
            init_ack_count -= 1
            if (init_ack_count == 0) {
              logger.info("Got INIT_ACK from everybody")
              prepare_latch.countDown()
            }
          }
        case _: JDONE =>
          logger.info("Got done from " + j.getSource().asString())
          done_count -= 1
          if (done_count == 0) {
            logger.info("Everybody is done")
            finished_latch.countDown()
          }
        case otherjavamsg => // other java msg
      }
    }*/

    case NetMessage(header, init_ack: JINIT_ACK) => handle {
      if (init_ack.init_id == init_id) {
        logger.info("Got INIT_ACK from " + header.getSource().asString)
        init_ack_count -= 1
        if (init_ack_count == 0) {
          //          logger.info("Got INIT_ACK from everybody")
          prepare_latch.countDown()
        }
      }
    }

    case NetMessage(header, _: JDONE) => handle{
      //      logger.info("Got done from " + header.getSource().asString)
      done_count -= 1
      if (done_count == 0) {
        logger.info("Everybody is done")
        finished_latch.countDown()
      }
    }

  }
}
