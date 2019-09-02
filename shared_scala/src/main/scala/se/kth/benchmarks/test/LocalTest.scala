package se.kth.benchmarks.test

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
        println("Starting runner");
        BenchmarkRunnerServer.run(runnerAddr, runnerImpl, serverPromise);
        println("Finished runner");
      }
    };
    runnerThread.start();

    val server = Await.result(serverPromise.future, 1.second);

    val benchStub = {
      val channel = ManagedChannelBuilder.forAddress(runnerAddr.addr, runnerAddr.port).usePlaintext().build;
      val stub = BenchmarkRunnerGrpc.stub(channel);
      stub
    };

    var attempts = 0;
    var ready = false;
    while (!ready && attempts < 20) {
      attempts += 1;
      println(s"Checking if ready, attempt #${attempts}");
      val readyF = benchStub.ready(ReadyRequest());
      val res = Await.result(readyF, 500.milliseconds);
      if (res.status) {
        println("Was ready.");
        ready = true
      } else {
        println("Wasn't ready, yet.");
        Thread.sleep(500);
      }
    }
    ready should be(true);

    /*
     * Ping Pong
     */
    val ppr = PingPongRequest().withNumberOfMessages(100);
    val pprResF = benchStub.pingPong(ppr);
    val pprRes = Await.result(pprResF, 30.seconds);
    checkResult("PingPong", pprRes);

    /*
     * Throughput Ping Pong
     */
    val tppr =
      ThroughputPingPongRequest().withMessagesPerPair(100).withParallelism(2).withPipelineSize(20).withStaticOnly(true);
    val tpprResF = benchStub.throughputPingPong(tppr);
    val tpprRes = Await.result(tpprResF, 30.seconds);
    checkResult("ThroughputPingPong (static)", tpprRes);

    val tppr2 = ThroughputPingPongRequest()
      .withMessagesPerPair(100)
      .withParallelism(2)
      .withPipelineSize(20)
      .withStaticOnly(false);
    val tpprResF2 = benchStub.throughputPingPong(tppr2);
    val tpprRes2 = Await.result(tpprResF2, 30.seconds);
    checkResult("ThroughputPingPong (gc)", tpprRes2);

    /*
     * Clean Up
     */
    println("Sending shutdown request to runner");
    server.stop();

    println("Waiting for runner to finish...");
    runnerThread.join();
    println("Runner is done.");

    println(s"""
%%%%%%%%%%%%%%%%%%%
%% LOCAL SUMMARY %%
%%%%%%%%%%%%%%%%%%%
${implemented.size} tests implemented: ${implemented.mkString(",")}
${notImplemented.size} tests not implemented: ${notImplemented.mkString(",")}
""")
  }

  private def checkResult(label: String, tr: TestResult): Unit = {
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
}
