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
    val dtest = new se.kth.benchmarks.test.DistributedTest(TestFactory);
    dtest.test();
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

  override def pingPong(): Benchmark = TestLocalBench;
  override def netPingPong(): DistributedBenchmark = TestDistributedBench;
  override def throughputPingPong(): Benchmark = TestLocalBench;
  override def netThroughputPingPong(): DistributedBenchmark = TestDistributedBench;
  override def atomicRegister(): DistributedBenchmark = TestDistributedBench;
}
