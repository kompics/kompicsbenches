package se.kth.benchmarks.kompicsjava.bench

import se.kth.benchmarks.kompicsscala.{KompicsSystem, KompicsSystemProvider}
import se.kth.benchmarks.Benchmark
import se.kth.benchmarks.kompicsjava.bench.allpairsshortestpath.graphs.{DoubleGraph, GraphUtils};
import kompics.benchmarks.benchmarks.APSPRequest
import se.sics.kompics.{Component, Fault, Kill, Killed, Kompics, KompicsEvent, Start, Started}
import se.sics.kompics.sl._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try
import scala.collection.mutable
import java.util.concurrent.CountDownLatch
import com.typesafe.scalalogging.StrictLogging
import java.util.UUID

import se.kth.benchmarks.kompicsjava.bench.allpairsshortestpath._

object AllPairsShortestPath extends Benchmark {

  override type Conf = APSPRequest;

  override def msgToConf(msg: scalapb.GeneratedMessage): Try[Conf] = {
    Try(msg.asInstanceOf[APSPRequest])
  };
  override def newInstance(): Instance = new AllPairsShortestPathI;

  class AllPairsShortestPathI extends Instance with StrictLogging {
    import scala.concurrent.ExecutionContext.Implicits.global;

    private var numNodes = -1;
    private var blockSize = -1;
    private var system: KompicsSystem = null;
    private var manager: UUID = null;
    private var graph: DoubleGraph = null;
    private var latch: CountDownLatch = null;

    override def setup(c: Conf): Unit = {
      logger.info(s"Setting up Instance with config: $c");
      this.numNodes = c.numberOfNodes;
      this.blockSize = c.blockSize;
      this.system = KompicsSystemProvider.newKompicsSystem();
      val f = for {
        manId <- system.createNotify[ManagerComponent](new ManagerComponent.Init(blockSize));
        _ <- system.startNotify(manId)
      } yield manId;
      this.manager = Await.result(f, Duration.Inf);
      this.graph = GraphUtils.generateGraph(numNodes);
    }
    override def prepareIteration(): Unit = {
      logger.debug("Preparing iteration");
      this.latch = new CountDownLatch(1);
    }
    override def runIteration(): Unit = {
      assert(this.system != null);
      assert(this.latch != null);
      assert(this.manager != null);
      assert(this.graph != null);
      system.runOnComponent(this.manager) { c =>
        `!trigger`(new Messages.ComputeFW(graph, latch) -> c.getPositive(classOf[ManagerPort]))(
          c.getComponent
        );
      }
      latch.await();
    }
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      logger.debug("Cleaning up iteration");
      if (this.latch != null) {
        this.latch = null;
      }
      if (lastIteration) {
        if (this.manager != null) {
          val f = system.killNotify(manager);
          Await.result(f, Duration.Inf);
          this.manager = null;
        }
        if (this.graph != null) {
          this.graph = null;
        }
        system.terminate();
        system = null;
        logger.info("Cleaned up Instance");
      }
    }
  }
}
