package se.kth.benchmarks.test

import org.scalatest._
import scala.util.{ Success, Failure, Try }
import scala.concurrent.{ Future, Await }
import scala.concurrent.duration._
import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import kompics.benchmarks.distributed._
import io.grpc.{ Server, ServerBuilder, ManagedChannelBuilder }
import se.kth.benchmarks._
import com.typesafe.scalalogging.StrictLogging

class DistributedTest(val benchFactory: BenchmarkFactory) extends Matchers with StrictLogging {

  private var implemented: List[String] = Nil;
  private var notImplemented: List[String] = Nil;

  def test(): Unit = {
    val runnerPort = 45678;
    val runnerAddrS = s"127.0.0.1:$runnerPort";
    val masterPort = 45679;
    val masterAddrS = s"127.0.0.1:$masterPort";
    val clientPort = 45680;
    val clientAddrS = s"127.0.0.1:$clientPort";

    val numClients = 1;

    val masterThread = new Thread("BenchmarkMaster") {
      override def run(): Unit = {
        println("Starting master");
        val runnerAddr = Util.argToAddr(runnerAddrS).get;
        val masterAddr = Util.argToAddr(masterAddrS).get;
        BenchmarkMaster.run(numClients, masterAddr.port, runnerAddr.port, benchFactory);
        println("Finished master");
      }
    };
    masterThread.start();

    val clientThread = new Thread("BenchmarkClient") {
      override def run(): Unit = {
        println("Starting client");
        val masterAddr = Util.argToAddr(masterAddrS).get;
        val clientAddr = Util.argToAddr(clientAddrS).get;
        BenchmarkClient.run(clientAddr.addr, masterAddr.addr, masterAddr.port);
        println("Finished client");
      }
    };
    clientThread.start();

    val runnerAddr = Util.argToAddr(runnerAddrS).get;
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
    ready should be (true);

    val ppr = PingPongRequest().withNumberOfMessages(100);
    val pprResF = benchStub.pingPong(ppr);
    val pprRes = Await.result(pprResF, 30.seconds);
    checkResult("PingPong", pprRes);

    val npprResF = benchStub.netPingPong(ppr);
    val npprRes = Await.result(npprResF, 30.seconds);
    checkResult("NetPingPong", npprRes);

    println("Sending shutdown request to master");
    val sreq = ShutdownRequest().withForce(false);
    val shutdownResF = benchStub.shutdown(sreq);

    println("Waiting for master to finish...");
    masterThread.join();
    println("Master is done.");
    println("Waiting for client to finish...");
    clientThread.join();
    println("Client is done.");

    println(s"""
%%%%%%%%%%%%%
%% SUMMARY %%
%%%%%%%%%%%%%
${implemented.size} tests implemented: ${implemented.mkString(",")}
${notImplemented.size} tests not implemented: ${notImplemented.mkString(",")}
""")
  }

  private def checkResult(label: String, tr: TestResult): Unit = {
    tr match {
      case s: TestSuccess => {
        s.runResults.size should equal (s.numberOfRuns);
        implemented ::= label;
      }
      case f: TestFailure => {
        f.reason should include ("RSE"); // since tests are short they may not meet RSE requirements
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
