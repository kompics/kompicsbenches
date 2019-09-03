package se.kth.benchmarks

import org.scalatest._
import scala.util.{Failure, Success, Try}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import kompics.benchmarks.distributed._
import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}
import java.util.concurrent.Executors

class LocalTest extends FunSuite with Matchers {
  test("Local communication") {
    val ltest = new se.kth.benchmarks.test.LocalTest(TestRunner);
    ltest.test();
  }
}

object TestRunner extends BenchmarkRunnerGrpc.BenchmarkRunner {
  implicit val futurePool = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor());

  override def ready(request: ReadyRequest): Future[ReadyResponse] = {
    Future.successful(ReadyResponse(true))
  }
  override def shutdown(request: ShutdownRequest): Future[ShutdownAck] = {
    ???
  }

  override def pingPong(request: PingPongRequest): Future[TestResult] = {
    Future {
      val res = BenchmarkRunner.run(TestLocalBench)();
      val msg = BenchmarkRunner.resultToTestResult(res);
      msg
    }
  }

  override def netPingPong(request: PingPongRequest): Future[TestResult] = {
    Future.successful(NotImplemented())
  }

  override def throughputPingPong(request: ThroughputPingPongRequest): Future[TestResult] = {
    Future {
      val res = BenchmarkRunner.run(TestLocalBench)();
      val msg = BenchmarkRunner.resultToTestResult(res);
      msg
    }
  }

  override def netThroughputPingPong(request: ThroughputPingPongRequest): Future[TestResult] = {
    Future.successful(NotImplemented())
  }

  override def atomicRegister(request: AtomicRegisterRequest): Future[TestResult] = {
    Future.successful(NotImplemented())
  }
}
