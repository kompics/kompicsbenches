package se.kth.benchmarks.test;

object KVTestUtil {

  sealed trait KVOperation
  case object Read extends KVOperation
  case object Write extends KVOperation

  case class KVTimestamp(key: Long, operation: KVOperation, value: Int, time: Long)
}
