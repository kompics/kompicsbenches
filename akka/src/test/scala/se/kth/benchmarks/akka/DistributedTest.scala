package se.kth.benchmarks.akka

import org.scalatest._
import scala.util.{ Success, Failure, Try }
import scala.concurrent.{ Future, Await }
import scala.concurrent.duration._
import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import kompics.benchmarks.distributed._
import io.grpc.{ Server, ServerBuilder, ManagedChannelBuilder }
import se.kth.benchmarks.test.{ DistributedTest => DTest }
import se.kth.benchmarks.akka.bench.Factory

class DistributedTest extends FunSuite with Matchers {
  test("Master-Client communication") {
    val dtest = new DTest(Factory);
    dtest.test();
  }
}
