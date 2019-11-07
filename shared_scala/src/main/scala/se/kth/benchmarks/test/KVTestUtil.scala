package se.kth.benchmarks.test

import scala.collection.immutable.List
;

object KVTestUtil {

  sealed trait KVOperation
  case object Read extends KVOperation
  case object Write extends KVOperation

  case class KVTimestamp(key: Long, operation: KVOperation, value: Int, time: Long)

  def isLinearizable(trace: List[KVTimestamp]): Boolean = {
    val sorted_trace = trace.sortBy(x => x.time)
    var latestValue: Int = 0
    sorted_trace.foreach{
      ts =>
        if (ts.operation == Read && ts.value != latestValue){
          false
        }
        else {
          latestValue = ts.value
        }
    }
    true
  }
}
