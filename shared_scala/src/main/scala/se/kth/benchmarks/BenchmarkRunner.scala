package se.kth.benchmarks

import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import io.grpc.{Server, ServerBuilder}

import com.typesafe.scalalogging.StrictLogging
import java.util.concurrent.Executors

class BenchmarkRunnerServer(port: Int, executionContext: ExecutionContext, runner: BenchmarkRunnerGrpc.BenchmarkRunner)
    extends StrictLogging { self =>

  private[this] var server: Server = null;

  private[benchmarks] def start(): Unit = {
    import util.retry.blocking.{Failure, Retry, RetryStrategy, Success}

    implicit val retryStrategy = RetryStrategy.fixedBackOff(retryDuration = 500.milliseconds, maxAttempts = 10);

    server = Retry {
      ServerBuilder
        .forPort(port)
        .addService(BenchmarkRunnerGrpc.bindService(runner, executionContext))
        .build()
        .start();
    } match {
      case Success(server) => {
        logger.info("Server started, listening on " + port);
        server
      }
      case Failure(ex) => {
        logger.error(s"Could not start BenchmarkRunnerServer: $ex");
        throw ex;
      }
    };
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

  val DEFAULT_PORT = 45678;
  val serverPool = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor());

  def runWith(args: Array[String], runner: BenchmarkRunnerGrpc.BenchmarkRunner): Unit = {
    val runnerAddr = Util.argToAddr(args(0)).get;
    run(runnerAddr, runner, Promise());
  }

  def run(runnerAddr: GrpcAddress,
          runner: BenchmarkRunnerGrpc.BenchmarkRunner,
          p: Promise[BenchmarkRunnerServer]): Unit = {
    val server = new BenchmarkRunnerServer(runnerAddr.port, serverPool, runner);
    server.start();
    p.success(server);
    server.blockUntilShutdown();
  }
}

object BenchmarkRunner extends StrictLogging {

  val MIN_RUNS = 30;
  val MAX_RUNS = 100;
  val RSE_TARGET = 0.1; // 10% RSE

  def run[B <: Benchmark](b: B)(c: b.Conf): Try[List[Double]] = {
    Try {
      val bi = b.newInstance();
      bi.setup(c);
      var results = List.empty[Double];
      var nRuns = 0;
      // first run
      bi.prepareIteration();
      results ::= measure(bi.runIteration);
      nRuns += 1;
      // run at least MIN_RUNS to be able to calculate RSE using normal distribution
      while (nRuns < MIN_RUNS) {
        bi.cleanupIteration(false, results.head);
        bi.prepareIteration();
        results ::= measure(bi.runIteration);
        nRuns += 1;
      }
      // run until RSE target is met
      while ((nRuns < MAX_RUNS) && (rse(results) > RSE_TARGET)) {
        bi.cleanupIteration(false, results.head);
        bi.prepareIteration();
        results ::= measure(bi.runIteration);
        nRuns += 1;
      }
      bi.cleanupIteration(true, results.head);
      val resultRSE = rse(results);
      if (resultRSE > RSE_TARGET) {
        val msg =
          s"RSE target of ${RSE_TARGET * 100.0}% was not met by value ${resultRSE * 100.0}% after ${nRuns} runs!";
        logger.warn(msg);
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
        logger.warn(s"Test Failure: ${f.getMessage}");
        f.printStackTrace();
        val msg = s"${f.getClass.getName}: ${f.getMessage}";
        TestFailure(msg)
      }
    }
  }
  def failureToTestResult(f: Failure[_], stage: Option[String] = None): TestResult = {
    val e = f.exception;
    logger.warn(s"Test Failure at stage $stage: ${e.getMessage}", e);
    val msg = s"${e.getClass.getName}: ${e.getMessage}";
    TestFailure(msg)
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
