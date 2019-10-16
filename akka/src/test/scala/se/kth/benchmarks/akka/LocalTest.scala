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
import se.kth.benchmarks.akka.bench.Factory

class LocalTest extends FunSuite with Matchers {
  ignore("Local communication (untyped)") {
    val ltest = new se.kth.benchmarks.test.LocalTest(new BenchmarkRunnerImpl());
    ltest.test();
  }

  test("Local communication (typed)") {
    val ltest = new se.kth.benchmarks.test.LocalTest(new TypedBenchmarkRunnerImpl());
    ltest.test();
  }
}
