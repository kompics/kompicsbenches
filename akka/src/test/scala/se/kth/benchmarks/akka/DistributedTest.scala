package se.kth.benchmarks.akka

import org.scalatest._
import scala.util.{Failure, Success, Try}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import kompics.benchmarks.distributed._
import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}
import se.kth.benchmarks.test.{DistributedTest => DTest}

class DistributedTest extends FunSuite with Matchers {
  test("Master-Client communication (untyped)") {
    val dtest = new DTest(se.kth.benchmarks.akka.bench.Factory);
    dtest.test();
  }

  test("Master-Client communication (typed)") {
    val dtest = new DTest(se.kth.benchmarks.akka.typed_bench.Factory);
    dtest.test();
  }
}
