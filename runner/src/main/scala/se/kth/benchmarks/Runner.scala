package se.kth.benchmarks

import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import scala.concurrent.{ Future, ExecutionContext, Await }
import scala.concurrent.duration.Duration
import scala.util.{ Try, Success, Failure }
import com.lkroll.common.macros.Macros
import com.typesafe.scalalogging.LazyLogging

class Runner(stub: Runner.Stub) extends LazyLogging {
  import TestResultMessage.SealedValue;

  def runAll(): Unit = {
    Benchmarks.benchmarks.foreach(runOne)
  }

  def runOne(b: Benchmark): Unit = {
    logger.info(s"Running ${b.name}");
    b.withStub(stub) { f =>
      logger.info(s"Awaiting run result...");
      val result = Await.ready(f, Duration.Inf).value.get;
      result match {
        case Success(r) => {
          r.sealedValue match {
            case SealedValue.Empty             => logger.warn(s"Benchmark ${b.name} invocation was empty.")
            case SealedValue.Failure(msg)      => logger.warn(s"Benchmark ${b.name} invocation failed: ${msg.reason}")
            case SealedValue.NotImplemented(_) => logger.info(s"Benchmark ${b.name} is not implemented.")
            case SealedValue.Success(TestSuccess(nRuns, data)) => {
              logger.info(s"Benchmark ${b.name} finished successfully with ${nRuns} runs.");
              val stats = new Statistics(data);
              logger.info(s"${b.symbol} ${stats.sampleMean}ms");
              // TODO write to disk
            }
          }
          logger.info(s"Benchmark ${b.name} finished successfully.");
        }
        case Failure(e) => {
          logger.warn(s"Benchmark ${b.name} invocation failed.", e);
        }
      }
    }
  }
}

object Runner {
  type Stub = BenchmarkRunnerGrpc.BenchmarkRunnerStub;
}
