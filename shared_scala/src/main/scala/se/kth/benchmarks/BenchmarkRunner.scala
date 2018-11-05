package se.kth.benchmarks

import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import scala.util.{ Try, Success, Failure }
import io.grpc.{ Server, ServerBuilder }

import java.util.logging.Logger
import java.util.concurrent.Executors

class BenchmarkRunnerServer(port: Int, executionContext: ExecutionContext, runner: BenchmarkRunnerGrpc.BenchmarkRunner) { self =>
  val serverPool = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor());

  private[this] var server: Server = null;

  private[benchmarks] def start(): Unit = {
    server = ServerBuilder.forPort(port).addService(
      BenchmarkRunnerGrpc.bindService(runner, serverPool)).
      build.start;

    BenchmarkRunnerServer.logger.info("Server started, listening on " + port)
    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
    }
  }

  private[benchmarks] def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  private[benchmarks] def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

}

object BenchmarkRunnerServer {
  private[benchmarks] val logger = Logger.getLogger(classOf[BenchmarkRunnerServer].getName);

  val DEFAULT_PORT = 45678;

  def runWith(args: Array[String], runner: BenchmarkRunnerGrpc.BenchmarkRunner): Unit = {
    val server = new BenchmarkRunnerServer(DEFAULT_PORT, ExecutionContext.global, runner);
    server.start();
    server.blockUntilShutdown();
  }
}

object BenchmarkRunner {
  val MIN_RUNS = 20;
  val MAX_RUNS = 100;
  val RSE_TARGET = 0.1; // 10% RSE

  def run[B <: Benchmark](b: B)(c: b.Conf): Try[List[Double]] = {
    Try {
      b.setup(c);
      var results = List.empty[Double];
      var nRuns = 0;
      // first run
      b.prepareIteration();
      results ::= measure(b.runIteration);
      nRuns += 1;
      // run at least 20 to be able to calculate RSE
      while (nRuns < 20) {
        b.cleanupIteration(false, results.head);
        b.prepareIteration();
        results ::= measure(b.runIteration);
        nRuns += 1;
      }
      // run until RSE target is met
      while ((nRuns < MAX_RUNS) && (rse(results) > RSE_TARGET)) {
        b.cleanupIteration(false, results.head);
        b.prepareIteration();
        results ::= measure(b.runIteration);
        nRuns += 1;
      }
      b.cleanupIteration(true, results.head);
      val resultRSE = rse(results);
      if (resultRSE > RSE_TARGET) {
        val msg = s"RSE target of ${RSE_TARGET * 100.0}% was not met by value ${resultRSE * 100.0}% after ${nRuns} runs!";
        BenchmarkRunnerServer.logger.warning(msg);
        throw new BenchmarkException(msg);
      } else {
        results
      }
    }
  }

  def measure(f: () => Unit): Double = {
    val start = System.nanoTime();
    f();
    val stop = System.nanoTime();
    val diff = stop - start;
    val diffMillis = Duration(diff.toDouble, NANOSECONDS).toUnit(MILLISECONDS);
    diffMillis
  }

  def resultToTestResult(r: Try[List[Double]]): TestResult = {
    r match {
      case Success(l) => TestSuccess(l.length, l)
      case Failure(f) => {
        BenchmarkRunnerServer.logger.warning(s"Test Failure: ${f.getMessage}");
        f.printStackTrace();
        val msg = s"${f.getClass.getName}: ${f.getMessage}";
        TestFailure(msg)
      }
    }
  }

  def rse(l: List[Double]): Double = new Statistics(l).relativeErrorOfTheMean;
}

class BenchmarkException(message: String) extends Exception(message) {

  def this(message: String, cause: Throwable) {
    this(message)
    initCause(cause)
  }

  def this(cause: Throwable) {
    this(Option(cause).map(_.toString).orNull, cause)
  }

  def this() {
    this(null: String)
  }
}
