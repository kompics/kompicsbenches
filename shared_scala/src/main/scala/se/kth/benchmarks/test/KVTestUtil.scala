package se.kth.benchmarks.test

import scala.collection.immutable.List
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

object KVTestUtil {

  sealed trait KVOperation
  case object ReadInvokation extends KVOperation
  case object WriteInvokation extends KVOperation
  case object ReadResponse extends KVOperation
  case object WriteResponse extends KVOperation

  case class KVTimestamp(key: Long, operation: KVOperation, value: Option[Int], time: Long, sender: Int)

  def isLinearizable(h: List[KVTimestamp], s: ListBuffer[Int]): Boolean = {
    if (h.isEmpty) true
    else {
      val minimalOps = getMinimalOperations(h)
      for (op <- minimalOps){
        val responseValue = getResponseValue(op, h)
        op.operation match {
          case WriteInvokation | WriteResponse => {
            if (isLinearizable(removeOperation(op, h), s += responseValue)) return true
            else s.remove(s.size - 1) // undo operation
          }
          case ReadInvokation | ReadResponse => {
            if (s.last == responseValue && isLinearizable(removeOperation(op, h), s)) return true
          }
        }
      }
      false
    }
  }

  private def getMinimalOperations(trace: List[KVTimestamp]): List[KVTimestamp] = {
    var minimalOps = ListBuffer[KVTimestamp]()
    breakable {
      for (entry <- trace) {
        if (entry.operation == ReadInvokation || entry.operation == WriteInvokation)
          minimalOps += entry
        else break()
      }
    }
    minimalOps.toList
  }

  private def getResponseValue(invokation: KVTimestamp, trace: List[KVTimestamp]): Int = {
    invokation.operation match {
      case ReadInvokation =>
        val response = trace.find(entry => entry.operation == ReadResponse && entry.sender == invokation.sender)
        response.get.value.get
      case _ => invokation.value.get
    }
  }

  private def removeOperation(entry: KVTimestamp, trace: List[KVTimestamp]): List[KVTimestamp] = {
    val res = trace.filterNot(x => x.sender == entry.sender)
    res
  }
}
