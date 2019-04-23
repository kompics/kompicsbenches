package se.kth.benchmarks.kompicsscala

import se.kth.benchmarks.BenchmarkRunner
import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._

import java.util.logging.Logger
import java.util.concurrent.Executors

class BenchmarkRunnerImpl extends BenchmarkRunnerGrpc.BenchmarkRunner {
  implicit val futurePool = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor());

  override def ready(request: ReadyRequest): Future[ReadyResponse] = {
    Future.successful(ReadyResponse(true))
  }
  override def shutdown(request: ShutdownRequest): Future[ShutdownAck] = {
    ???
  }

  override def pingPong(request: PingPongRequest): Future[TestResult] = {
    Future {
      val res = BenchmarkRunner.run(bench.PingPong)(request);
      val msg = BenchmarkRunner.resultToTestResult(res);
      msg
    }
  }

  override def netPingPong(request: PingPongRequest): Future[TestResult] = {
    Future.successful(NotImplemented())
  }
}
