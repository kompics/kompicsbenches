package se.kth.benchmarks.kompicsjava

import se.kth.benchmarks.BenchmarkRunner
import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import scala.concurrent.{ExecutionContext, Future}
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

  override def netPingPong(request: PingPongRequest): Future[TestResult] = Future.successful(NotImplemented());

  override def throughputPingPong(request: ThroughputPingPongRequest): Future[TestResult] = {
    Future {
      val res = BenchmarkRunner.run(bench.ThroughputPingPong)(request);
      val msg = BenchmarkRunner.resultToTestResult(res);
      msg
    }
  }

  override def netThroughputPingPong(request: ThroughputPingPongRequest): Future[TestResult] =
    Future.successful(NotImplemented())

  override def atomicRegister(request: AtomicRegisterRequest): Future[TestResult] = Future.successful(NotImplemented());

  override def streamingWindows(request: StreamingWindowsRequest): Future[TestResult] =
    Future.successful(NotImplemented());

  override def allPairsShortestPath(request: APSPRequest): Future[TestResult] = {
    Future {
      val res = BenchmarkRunner.run(bench.AllPairsShortestPath)(request);
      val msg = BenchmarkRunner.resultToTestResult(res);
      msg
    }
  }

  override def chameneos(request: ChameneosRequest): Future[TestResult] = {
    Future {
      val res = BenchmarkRunner.run(bench.Chameneos)(request);
      val msg = BenchmarkRunner.resultToTestResult(res);
      msg
    }
  }

  override def fibonacci(request: FibonacciRequest): Future[TestResult] = {
    Future {
      val res = BenchmarkRunner.run(bench.Fibonacci)(request);
      val msg = BenchmarkRunner.resultToTestResult(res);
      msg
    }
  }

  override def atomicBroadcast(request: AtomicBroadcastRequest): Future[TestResult] = Future.successful(NotImplemented());

  override def sizedThroughput(request: SizedThroughputRequest): Future[TestResult] =
    Future.successful(NotImplemented())
}
