package se.kth.benchmarks

import org.scalatest._
import scala.util.{ Success, Failure, Try }
import scala.concurrent.{ Future, Await }
import scala.concurrent.duration._
import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import kompics.benchmarks.distributed._
import io.grpc.{ Server, ServerBuilder, ManagedChannelBuilder }

class DistributedTest extends FunSuite with Matchers {
  test("Master-Client communication") {
    val runnerPort = 45678;
    val runnerAddrS = s"127.0.0.1:$runnerPort";
    val masterPort = 45679;
    val masterAddrS = s"127.0.0.1:$masterPort";
    val clientPort = 45680;
    val clientAddrS = s"127.0.0.1:$clientPort";

    val numClients = 1;

    val benchFactory = TestFactory;

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
    pprRes shouldBe a[TestSuccess];

    val npprResF = benchStub.netPingPong(ppr);
    val npprRes = Await.result(npprResF, 30.seconds);
    npprRes shouldBe a[TestSuccess];

    println("Sending shutdown request to master");
    val sreq = ShutdownRequest().withForce(false);
    val shutdownResF = benchStub.shutdown(sreq);

    println("Waiting for master to finish...");
    masterThread.join();
    println("Master is done.");
    println("Waiting for client to finish...");
    clientThread.join();
    println("Client is done.");
  }
}

object TestLocalBench extends Benchmark {
  type Conf = Unit;

  class TestLocalBenchI extends Instance {
    override def setup(c: Conf): Unit = {
      println("Test got setup.");
    }
    override def prepareIteration(): Unit = {
      println("Preparing iteration.");
    }
    override def runIteration(): Unit = {
     val start = System.currentTimeMillis();
      println("Running iteration.");
      // make sure results are of consistent length to avoid RSE target violations
      var end = System.currentTimeMillis();
      while ((end - start) < 100) {
        end = System.currentTimeMillis();
      }
    }
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      val last = if (lastIteration) { "last" } else { "" };
      println(s"Cleanup after $last iteration.");
    }
  }

  override def msgToConf(msg: scalapb.GeneratedMessage): Try[Conf] = Success(());
  override def newInstance(): Instance = new TestLocalBenchI;
}

object TestDistributedBench extends DistributedBenchmark {
  type MasterConf = Unit;
  type ClientConf = Unit;
  type ClientData = Unit;

  class TestDistributedBenchMaster extends Master {
    override def setup(c: MasterConf): ClientConf = {
      println("Master setting up.");
    }
    override def prepareIteration(d: List[ClientData]): Unit = {
      println("Master preparing iteration.");
    }
    override def runIteration(): Unit = {
      val start = System.currentTimeMillis();
      println("Master running iteration.");
      // make sure results are of consistent length to avoid RSE target violations
      var end = System.currentTimeMillis();
      while ((end - start) < 100) {
        end = System.currentTimeMillis();
      }
    }
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      val last = if (lastIteration) { "last" } else { "" };
      println(s"Master cleaning up after $last iteration.");
    }
  }

  class TestDistributedBenchClient extends Client {
    override def setup(c: ClientConf): ClientData = {
      println("Client setting up.");
    }
    override def prepareIteration(): Unit = {
      println("Client preparing iteration.");
    }
    override def cleanupIteration(lastIteration: Boolean): Unit = {
      val last = if (lastIteration) { "last" } else { "" };
      println(s"Client cleaning up after $last iteration.");
    }
  }

  override def newMaster(): Master = new TestDistributedBenchMaster;
  override def msgToMasterConf(msg: scalapb.GeneratedMessage): Try[MasterConf] = Success(());

  override def newClient(): Client = new TestDistributedBenchClient;
  override def strToClientConf(str: String): Try[ClientConf] = Success(());
  override def strToClientData(str: String): Try[ClientData] = Success(());

  override def clientConfToString(c: ClientConf): String = "";
  override def clientDataToString(d: ClientData): String = "";
}

object TestFactory extends BenchmarkFactory {

  override def pingpong(): Benchmark = TestLocalBench;
  override def netpingpong(): DistributedBenchmark = TestDistributedBench;
}
