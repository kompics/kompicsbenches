package se.kth.benchmarks.kompicsjava.bench

import java.util
import java.util.concurrent.CountDownLatch

import se.kth.benchmarks.kompicsscala._
import se.kth.benchmarks.kompicsjava.net.{NetAddress => JNetAddress, NetMessage => JNetMessage}
import se.kth.benchmarks.kompicsjava.partitioningcomponent.events.{Done, InitAck, Init => JInit, Run => JRun, TestDone => JTestDone}
import se.sics.kompics.network.Network
import se.sics.kompics.sl.{ComponentDefinition, Init, KompicsEvent, Start, handle}
import JPartitioningComp.Run
import se.kth.benchmarks.test.KVTestUtil.KVTimestamp

import scala.collection.JavaConverters._
import scala.collection.immutable.List
import scala.collection.mutable
import scala.concurrent.Promise

class JPartitioningComp(init: Init[JPartitioningComp]) extends ComponentDefinition {
  val net = requires[Network]

  val Init(prepare_latch: CountDownLatch,
           finished_latch: Option[CountDownLatch] @unchecked,
           init_id: Int,
           nodes: List[NetAddress] @unchecked,
           num_keys: Long,
           partition_size: Int,
           test_promise: Option[Promise[List[KVTimestamp]]] @unchecked) = init;
  val active_nodes = new util.LinkedList[JNetAddress]();
  for (i <- 0 until partition_size) {
    active_nodes.add(nodes(i).asJava)
  }
  val n = active_nodes.size();
  var init_ack_count: Int = 0;
  var done_count = 0;
  var partitions = mutable.Map[Long, List[NetAddress]]();
  var test_results = mutable.Buffer[KVTimestamp]()
  val selfAddr = cfg.getValue[NetAddress](KompicsSystemProvider.SELF_ADDR_KEY);

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
    case NetMessage(_, init_ack: InitAck) =>
      handle {
        if (init_ack.init_id == init_id) {
          init_ack_count += 1
          if (init_ack_count == n) {
            prepare_latch.countDown()
          }
        }
      }

    case NetMessage(_, _: Done) =>
      handle {
        done_count += 1
        if (done_count == n) {
          finished_latch.get.countDown()
        }
      }

    case NetMessage(_, test_done: JTestDone) =>
      handle {
        done_count += 1
        test_results ++= test_done.timestamps.asScala
        if (done_count == n) {
          logger.info("Test is done")
          test_promise.get.success(test_results.toList)
        }
      }

  }
}

object JPartitioningComp {
  case object Run extends KompicsEvent;
}
