package se.kth.benchmarks.test;

import org.scalatest._
import scala.util.{Failure, Success, Try}
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import kompics.benchmarks.distributed._
import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}
import se.kth.benchmarks._
import com.typesafe.scalalogging.StrictLogging

class LocalTest(val runnerImpl: BenchmarkRunnerGrpc.BenchmarkRunner) extends Matchers with StrictLogging {

  private var implemented: List[String] = Nil;
  private var notImplemented: List[String] = Nil;

  val timeout = 60.seconds;

  def test(): Unit = {
    val runnerPort = 45677;
    val runnerAddrS = s"127.0.0.1:$runnerPort";
    val runnerAddr = Util.argToAddr(runnerAddrS).get;

    /*
     * Setup
     */
    val serverPromise: Promise[BenchmarkRunnerServer] = Promise();
    val runnerThread = new Thread("BenchmarkRunner") {
      override def run(): Unit = {
        logger.debug("Starting runner");
        BenchmarkRunnerServer.run(runnerAddr, runnerImpl, serverPromise);
        logger.debug("Finished runner");
      }
    };
    runnerThread.start();

    val server = Await.result(serverPromise.future, 1.second);

    var benchChannel = ManagedChannelBuilder.forAddress(runnerAddr.addr, runnerAddr.port).usePlaintext().build();
    var benchStub = BenchmarkRunnerGrpc.stub(benchChannel);
    try {

      var attempts = 0;
      var ready = false;
      while (!ready && attempts < 20) {
        attempts += 1;
        try {
          logger.info(s"Checking if runner is ready, attempt #${attempts}");
          val readyF = benchStub.ready(ReadyRequest());
          val res = Await.result(readyF, 500.milliseconds);
          if (res.status) {
            logger.info("Runner is ready!");
            ready = true
          } else {
            logger.info("Runner wasn't ready, yet.");
            Thread.sleep(500);
          }
        } catch {
          case e: Throwable => {
            logger.error("Couldn't connect to runner server. Redoing setup and retrying...", e);
            TestUtil.shutdownChannel(benchChannel);
            benchChannel = ManagedChannelBuilder.forAddress(runnerAddr.addr, runnerAddr.port).usePlaintext().build();
            benchStub = BenchmarkRunnerGrpc.stub(benchChannel);
            Thread.sleep(500);
          }
        }
      }
      ready should be(true);

      /*
       * Ping Pong
       */
      logger.info("Starting test PingPong");
      val ppr = PingPongRequest().withNumberOfMessages(100);
      val pprResF = benchStub.pingPong(ppr);
      checkResult("PingPong", pprResF);
      logger.info("Finished test PingPong");

      /*
       * Throughput Ping Pong
       */
      logger.info("Starting test ThroughputPingPong (static)");
      val tppr =
        ThroughputPingPongRequest()
          .withMessagesPerPair(100)
          .withParallelism(2)
          .withPipelineSize(20)
          .withStaticOnly(true);
      val tpprResF = benchStub.throughputPingPong(tppr);
      checkResult("ThroughputPingPong (static)", tpprResF);
      logger.info("Finished Starting test ThroughputPingPong (static)");

      logger.info("Starting test ThroughputPingPong (gc)");
      val tppr2 = ThroughputPingPongRequest()
        .withMessagesPerPair(100)
        .withParallelism(2)
        .withPipelineSize(20)
        .withStaticOnly(false);
      val tpprResF2 = benchStub.throughputPingPong(tppr2);
      checkResult("ThroughputPingPong (gc)", tpprResF2);
      logger.info("Finished test ThroughputPingPong (gc)");

      /*
       * Clean Up
       */
      logger.debug("Sending shutdown request to runner");
      server.stop();
      logger.debug("Waiting for runner to finish...");
      runnerThread.join();
      logger.debug("Runner is done.");

    } catch {
      case e: org.scalatest.exceptions.TestFailedException => throw e // let these pass through
      case e: Throwable => {
        logger.error("Error during test", e);
        Console.err.println("Thrown error:");
        e.printStackTrace(Console.err);
        Console.err.println("Caused by:");
        e.getCause().printStackTrace(Console.err);
        fail(e);
      }
    } finally {
      benchStub = null;
      if (benchChannel != null) {
        TestUtil.shutdownChannel(benchChannel);
        benchChannel = null;
      }
    }

    logger.info(s"""
%%%%%%%%%%%%%%%%%%%
%% LOCAL SUMMARY %%
%%%%%%%%%%%%%%%%%%%
${implemented.size} tests implemented: ${implemented.mkString(",")}
${notImplemented.size} tests not implemented: ${notImplemented.mkString(",")}
""")
  }

  private def checkResult(label: String, trf: Future[TestResult]): Unit = {
    val trfReady = Await.ready(trf, timeout);
    trfReady.value.get match {
      case Success(tr) => {
        tr match {
          case s: TestSuccess => {
            s.runResults.size should equal(s.numberOfRuns);
            implemented ::= label;
          }
          case f: TestFailure => {
            f.reason should include("RSE"); // since tests are short they may not meet RSE requirements
            implemented ::= label;
          }
          case n: NotImplemented => {
            logger.warn(s"Test $label was not implemented");
            notImplemented ::= label;
          }
          case x => fail(s"Unexpected test result: $x")
        }
      }
      case Failure(e) => {
        logger.error(s"Test $label failed in promise/future!", e);
        e.printStackTrace(Console.err);
        fail(s"Test failed due to gRPC communication: ${e.getMessage()}")
      }
    }
  }
}
