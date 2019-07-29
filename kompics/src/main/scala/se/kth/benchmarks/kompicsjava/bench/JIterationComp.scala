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
  val active_nodes = new util.LinkedList[JNetAddress]()
  var n = nodes.size
  var init_ack_count: Int = 0
  var done_count = 0
  var partitions = mutable.Map[Long, List[NetAddress]]()
  val min_key: Long = 0l
  val max_key: Long = num_keys - 1
  lazy val selfAddr = cfg.getValue[NetAddress](KompicsSystemProvider.SELF_ADDR_KEY);

  ctrl uponEvent {
    case _: Start => handle {
      assert(selfAddr != null)
      for (i <- 0 until partition_size) {
        active_nodes.add(nodes(i).asJava)
      }
      n = active_nodes.size()
      logger.info(s"Active nodes: $n/${nodes.size}")
      for (i <- 0 until partition_size) {
        trigger(JNetMessage.viaTCP(selfAddr.asJava, active_nodes.get(i), new JINIT(i, init_id, active_nodes, min_key, max_key)), net)
      }
    }
    case RUN => handle {
      logger.info("Sending RUN to everybody...")
      val it = active_nodes.iterator()
      while (it.hasNext()) {
        trigger(JNetMessage.viaTCP(selfAddr.asJava, it.next(), JRUN.event), net)
      }
      /*
      for (i <- 0 until n) {
        trigger(JNetMessage.viaTCP(selfAddr.asJava, active_nodes.get(i), JRUN.event), net)
      }*/
    }

  }

  net uponEvent {
    case NetMessage(header, init_ack: JINIT_ACK) => handle {
      if (init_ack.init_id == init_id) {
        //        logger.info("Got INIT_ACK from " + header.getSource().asString)
        init_ack_count += 1
        if (init_ack_count == n) {
          //          logger.info("Got INIT_ACK from everybody")
          prepare_latch.countDown()
        }
      }
    }

    case NetMessage(header, _: JDONE) => handle{
      //      logger.info("Got done from " + header.getSource().asString)
      done_count += 1
      if (done_count == n) {
        logger.info("Everybody is done")
        finished_latch.countDown()
      }
    }

  }
}
