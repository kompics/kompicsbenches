package se.kth.benchmarks.test

import org.scalatest._
import scala.util.{Failure, Success, Try}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import kompics.benchmarks.distributed._
import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}
import se.kth.benchmarks._
import com.typesafe.scalalogging.StrictLogging

class DistributedTest(val benchFactory: BenchmarkFactory) extends Matchers with StrictLogging {

  private var implemented: List[String] = Nil;
  private var notImplemented: List[String] = Nil;

  val timeout = 60.seconds;

  def test(): Unit = {
    val runnerPort = 45678;
    val runnerAddrS = s"127.0.0.1:$runnerPort";
    val masterPort = 45679;
    val masterAddrS = s"127.0.0.1:$masterPort";
    val clientPorts = Array(45680, 45681, 45682, 45683);
    val clientAddrs = clientPorts.map(clientPort => s"127.0.0.1:$clientPort");

    val numClients = 4;

    /*
     * Setup
     */

    val masterThread = new Thread("BenchmarkMaster") {
      override def run(): Unit = {
        logger.debug("Starting master");
        val runnerAddr = Util.argToAddr(runnerAddrS).get;
        val masterAddr = Util.argToAddr(masterAddrS).get;
        BenchmarkMaster.run(numClients, masterAddr.port, runnerAddr.port, benchFactory);
        logger.debug("Finished master");
      }
    };
    masterThread.start();

    val clientThreads = clientAddrs.map { clientAddrS =>
      val clientThread = new Thread(s"BenchmarkClient-$clientAddrS") {
        override def run(): Unit = {
          logger.debug(s"Starting client $clientAddrS");
          val masterAddr = Util.argToAddr(masterAddrS).get;
          val clientAddr = Util.argToAddr(clientAddrS).get;
          BenchmarkClient.run(clientAddr.addr, masterAddr.addr, masterAddr.port);
          logger.debug(s"Finished client $clientAddrS");
        }
      };
      clientThread.start();
      clientThread
    };

    val runnerAddr = Util.argToAddr(runnerAddrS).get;
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

      logger.info("Starting test NetPingPong");
      val npprResF = benchStub.netPingPong(ppr);
      checkResult("NetPingPong", npprResF);
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
      logger.info("Finished test ThroughputPingPong (static)");

      logger.info("Starting test NetThroughputPingPong (static)");
      val tnpprResF = benchStub.netThroughputPingPong(tppr);
      checkResult("NetThroughputPingPong (static)", tnpprResF);
      logger.info("Finished test NetThroughputPingPong (static)");

      logger.info("Starting test ThroughputPingPong (gc)");
      val tppr2 = ThroughputPingPongRequest()
        .withMessagesPerPair(100)
        .withParallelism(2)
        .withPipelineSize(20)
        .withStaticOnly(false);
      val tpprResF2 = benchStub.throughputPingPong(tppr2);
      checkResult("ThroughputPingPong (gc)", tpprResF2);
      logger.info("Finished test ThroughputPingPong (gc)");

      logger.info("Starting test NetThroughputPingPong (gc)");
      val tnpprResF2 = benchStub.netThroughputPingPong(tppr2);
      checkResult("NetThroughputPingPong (gc)", tnpprResF2);
      logger.info("Finished test NetThroughputPingPong (gc)");

      logger.info("Starting test AtomicRegister");
      val nnarr = AtomicRegisterRequest()
        .withReadWorkload(0.5f)
        .withWriteWorkload(0.5f)
        .withPartitionSize(3)
        .withNumberOfKeys(500);
      val nnarResF = benchStub.atomicRegister(nnarr);
      checkResult("Atomic Register", nnarResF);
      logger.info("Finished test AtomicRegister");

      /*
       * Clean Up
       */
      logger.debug("Sending shutdown request to master");
      val sreq = ShutdownRequest().withForce(false);
      val shutdownResF = benchStub.shutdown(sreq);

      logger.debug("Waiting for master to finish...");
      masterThread.join();
      logger.debug("Master is done.");
      logger.debug("Waiting for all clients to finish...");
      clientThreads.foreach(t => t.join());
      logger.debug("All clients are done.");
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
      }
    }

    logger.info(s"""
%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% MASTER-CLIENT SUMMARY %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%
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
