package se.kth.benchmarks.kompicsjava.bench

import java.util
import java.util.concurrent.CountDownLatch

import se.kth.benchmarks.kompicsscala._
import se.kth.benchmarks.kompicsjava.net.{NetAddress => JNetAddress, NetMessage => JNetMessage}
import se.kth.benchmarks.kompicsjava.partitioningcomponent.events.{Done, InitAck, Init => JInit, Run => JRun}
import se.sics.kompics.network.Network
import se.sics.kompics.sl.{ComponentDefinition, Init, KompicsEvent, Start, handle}
import JPartitioningComp.Run

import scala.collection.mutable

class JPartitioningComp(init: Init[JPartitioningComp]) extends ComponentDefinition {
  val net = requires[Network]

  val Init(prepare_latch: CountDownLatch,
           finished_latch: CountDownLatch,
           init_id: Int,
           nodes: List[NetAddress] @unchecked,
           num_keys: Long,
           partition_size: Int) = init;
  val active_nodes = new util.LinkedList[JNetAddress]();
  for (i <- 0 until partition_size) {
    active_nodes.add(nodes(i).asJava)
  }
  val n = active_nodes.size();
  var init_ack_count: Int = 0;
  var done_count = 0;
  var partitions = mutable.Map[Long, List[NetAddress]]();
  lazy val selfAddr = cfg.getValue[NetAddress](KompicsSystemProvider.SELF_ADDR_KEY);

  ctrl uponEvent {
    case _: Start =>
      handle {
        assert(selfAddr != null)

        val min_key: Long = 0L
        val max_key: Long = num_keys - 1
        for (i <- 0 until partition_size) {
          trigger(JNetMessage.viaTCP(selfAddr.asJava,
                                     active_nodes.get(i),
                                     new JInit(i, init_id, active_nodes, min_key, max_key)),
                  net)
        }
      }
    case Run =>
      handle {
        val it = active_nodes.iterator()
        while (it.hasNext) {
          trigger(JNetMessage.viaTCP(selfAddr.asJava, it.next(), JRun.event), net)
        }
      }

  }

  net uponEvent {
    case NetMessage(header, init_ack: InitAck) =>
      handle {
        if (init_ack.init_id == init_id) {
          init_ack_count += 1
          if (init_ack_count == n) {
            prepare_latch.countDown()
          }
        }
      }

    case NetMessage(header, _: Done) =>
      handle {
        done_count += 1
        if (done_count == n) {
          finished_latch.countDown()
        }
      }

  }
}

object JPartitioningComp {
  case object Run extends KompicsEvent;
}
