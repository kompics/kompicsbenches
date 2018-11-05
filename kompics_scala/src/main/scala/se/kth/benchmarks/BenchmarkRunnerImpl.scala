package se.kth.benchmarks

import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._

import java.util.logging.Logger
import java.util.concurrent.Executors

class BenchmarkRunnerImpl extends BenchmarkRunnerGrpc.BenchmarkRunner {
  implicit val futurePool = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor());

  override def pingPong(request: PingPongRequest): Future[TestResult] = {
    Future {
      val ppb = new bench.PingPong;
      val res = BenchmarkRunner.run(ppb)(request);
      val msg = BenchmarkRunner.resultToTestResult(res);
      msg
    }
  }
}
