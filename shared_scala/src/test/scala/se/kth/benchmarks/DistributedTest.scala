package se.kth.benchmarks

import org.scalatest._
import scala.util.{Failure, Success, Try}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import kompics.benchmarks.distributed._
import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}
import com.typesafe.scalalogging.StrictLogging

class DistributedTest extends FunSuite with Matchers {
  ignore("Master-Client communication") {
    val dtest = new se.kth.benchmarks.test.DistributedTest(TestFactory);
    dtest.test();
  }

  test("Master-Client failures") {
    for (s <- Stage.list) {
      val masterFailTest = new se.kth.benchmarks.test.DistributedTest(new FailFactory(Some(s), None));
      masterFailTest.testFail();
      val clientFailTest = new se.kth.benchmarks.test.DistributedTest(new FailFactory(None, Some(s)));
      clientFailTest.testFail();
    }
  }
}

object TestLocalBench extends Benchmark {
  type Conf = Unit;

  class TestLocalBenchI extends Instance with StrictLogging {
    override def setup(c: Conf): Unit = {
      logger.trace("Test got setup.");
    }
    override def prepareIteration(): Unit = {
      logger.trace("Preparing iteration.");
    }
    override def runIteration(): Unit = {
      val start = System.currentTimeMillis();
      logger.trace("Running iteration.");
      // make sure results are of consistent length to avoid RSE target violations
      var end = System.currentTimeMillis();
      while ((end - start) < 100) {
        end = System.currentTimeMillis();
      }
    }
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      val last = if (lastIteration) {
        "last"
      } else {
        ""
      };
      logger.trace(s"Cleanup after $last iteration.");
    }
  }

  override def msgToConf(msg: scalapb.GeneratedMessage): Try[Conf] = Success(());
  override def newInstance(): Instance = new TestLocalBenchI;
}

sealed trait Stage {
  def fail(): Unit = {
    throw new BenchTestException(this);
  }
  def failIfEq(s: Stage): Unit = {
    if (s == this) {
      this.fail()
    } else {
      ()
    }
  }
}
object Stage {
  case object Setup extends Stage;
  case object Prepare extends Stage;
  case object Run extends Stage;
  case object Cleanup extends Stage;
  case object LastCleanup extends Stage;

  def fromString(str: String): Stage = {
    str match {
      case "Setup"       => Setup
      case "Prepare"     => Prepare
      case "Run"         => Run
      case "Cleanup"     => Cleanup
      case "LastCleanup" => LastCleanup
      case _             => ???
    }
  }

  val list = List(Setup, Prepare, Run, Cleanup, LastCleanup);
}
class BenchTestException(s: Stage) extends Exception(s"Benchmark was purposefully failed at stage $s.") {}

class FailLocalBench(val failAt: Stage) extends Benchmark {
  type Conf = Unit;

  class FailLocalBenchI extends Instance {

    override def setup(c: Conf): Unit = {
      FailLocalBench.this.failAt.failIfEq(Stage.Setup)
    }
    override def prepareIteration(): Unit = {
      FailLocalBench.this.failAt.failIfEq(Stage.Prepare)
    }
    override def runIteration(): Unit = {
      FailLocalBench.this.failAt.failIfEq(Stage.Prepare)
      val start = System.currentTimeMillis();
      // make sure results are of consistent length to avoid RSE target violations
      var end = System.currentTimeMillis();
      while ((end - start) < 100) {
        end = System.currentTimeMillis();
      }
    }
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      val last = if (lastIteration) {
        FailLocalBench.this.failAt.failIfEq(Stage.LastCleanup)
      } else {
        FailLocalBench.this.failAt.failIfEq(Stage.Cleanup)
      };
    }
  }

  override def msgToConf(msg: scalapb.GeneratedMessage): Try[Conf] = Success(());
  override def newInstance(): Instance = new FailLocalBenchI;
}

class FailDistributedBench(val masterFailAt: Option[Stage], val clientFailAt: Option[Stage])
    extends DistributedBenchmark {
  assert(!(masterFailAt.isDefined && clientFailAt.isDefined), "Only test master NAND client failures!");

  def this() { // for loading at the client side
    this(None, None)
  }

  type MasterConf = Unit;
  type ClientConf = Option[Stage];
  type ClientData = Unit;

  def failIfSome(so: Option[Stage], stage: Stage): Unit = {
    so match {
      case Some(s) => s.failIfEq(stage)
      case None    => ()
    }
  }

  class FailDistributedBenchMaster extends Master {
    override def setup(c: MasterConf, _meta: DeploymentMetaData): Try[ClientConf] = Try {
      failIfSome(FailDistributedBench.this.masterFailAt, Stage.Setup);
      FailDistributedBench.this.clientFailAt
    };
    override def prepareIteration(d: List[ClientData]): Unit = {
      failIfSome(FailDistributedBench.this.masterFailAt, Stage.Prepare);
    }
    override def runIteration(): Unit = {
      failIfSome(FailDistributedBench.this.masterFailAt, Stage.Run);
      val start = System.currentTimeMillis();
      // make sure results are of consistent length to avoid RSE target violations
      var end = System.currentTimeMillis();
      while ((end - start) < 100) {
        end = System.currentTimeMillis();
      }
    }
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      val last = if (lastIteration) {
        failIfSome(FailDistributedBench.this.masterFailAt, Stage.LastCleanup);
      } else {
        failIfSome(FailDistributedBench.this.masterFailAt, Stage.Cleanup);
      };
    }
  }

  class FailDistributedBenchClient extends Client {

    private var failAt: Option[Stage] = None;

    override def setup(c: ClientConf): ClientData = {
      this.failAt = c;
      failIfSome(failAt, Stage.Setup);
    }
    override def prepareIteration(): Unit = {
      failIfSome(failAt, Stage.Prepare);
    }
    override def cleanupIteration(lastIteration: Boolean): Unit = {
      val last = if (lastIteration) {
        failIfSome(failAt, Stage.LastCleanup);
      } else {
        failIfSome(failAt, Stage.Cleanup);
      };
    }
  }

  override def newMaster(): Master = new FailDistributedBenchMaster;
  override def msgToMasterConf(msg: scalapb.GeneratedMessage): Try[MasterConf] = Success(());

  override def newClient(): Client = new FailDistributedBenchClient;
  override def strToClientConf(str: String): Try[ClientConf] = {
    if (str.isEmpty) {
      Success(None)
    } else {
      Try { Stage.fromString(str) }.map(s => Some(s))
    }
  }
  override def strToClientData(str: String): Try[ClientData] = Success(());

  override def clientConfToString(c: ClientConf): String = {
    c match {
      case Some(s) => s.toString()
      case None    => ""
    }
  }
  override def clientDataToString(d: ClientData): String = "";
}

object TestDistributedBench extends DistributedBenchmark {
  type MasterConf = Unit;
  type ClientConf = Unit;
  type ClientData = Unit;

  class TestDistributedBenchMaster extends Master with StrictLogging {
    override def setup(c: MasterConf, _meta: DeploymentMetaData): Try[ClientConf] = {
      logger.trace("Master setting up.");
      Success(())
    }
    override def prepareIteration(d: List[ClientData]): Unit = {
      logger.trace("Master preparing iteration.");
    }
    override def runIteration(): Unit = {
      val start = System.currentTimeMillis();
      logger.trace("Master running iteration.");
      // make sure results are of consistent length to avoid RSE target violations
      var end = System.currentTimeMillis();
      while ((end - start) < 100) {
        end = System.currentTimeMillis();
      }
    }
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      val last = if (lastIteration) {
        "last"
      } else {
        ""
      };
      logger.trace(s"Master cleaning up after $last iteration.");
    }
  }

  class TestDistributedBenchClient extends Client with StrictLogging {
    override def setup(c: ClientConf): ClientData = {
      logger.trace("Client setting up.");
    }
    override def prepareIteration(): Unit = {
      logger.trace("Client preparing iteration.");
    }
    override def cleanupIteration(lastIteration: Boolean): Unit = {
      val last = if (lastIteration) {
        "last"
      } else {
        ""
      };
      logger.trace(s"Client cleaning up after $last iteration.");
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
  override def streamingWindows(): DistributedBenchmark = TestDistributedBench;
  override def allPairsShortestPath(): Benchmark = TestLocalBench;
  override def chameneos(): Benchmark = TestLocalBench;
  override def fibonacci: Benchmark = TestLocalBench;
  override def atomicBroadcast(): DistributedBenchmark = TestDistributedBench;
}

class FailFactory(val masterFailAt: Option[Stage], val clientFailAt: Option[Stage]) extends BenchmarkFactory {

  override def pingPong(): Benchmark = masterFailAt match {
    case Some(s) => new FailLocalBench(s) // fail on first
    case None    => TestLocalBench
  };
  override def netPingPong(): DistributedBenchmark =
    new FailDistributedBench(masterFailAt, clientFailAt); // of each type
  override def throughputPingPong(): Benchmark = TestLocalBench; // run one of each of the others normally
  override def netThroughputPingPong(): DistributedBenchmark = TestDistributedBench;
  override def atomicRegister(): DistributedBenchmark = ???;
  override def streamingWindows(): DistributedBenchmark = ???;
  override def allPairsShortestPath(): Benchmark = ???;
  override def chameneos(): Benchmark = ???;
  override def fibonacci: Benchmark = ???;
  override def atomicBroadcast(): DistributedBenchmark = ???;
}
