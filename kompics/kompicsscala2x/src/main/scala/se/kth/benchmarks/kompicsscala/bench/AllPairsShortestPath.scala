package se.kth.benchmarks.kompicsscala.bench

import se.kth.benchmarks.kompicsscala.{KompicsSystem, KompicsSystemProvider}
import se.kth.benchmarks.Benchmark
import se.kth.benchmarks.helpers.{Block, Graph, GraphUtils}
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
    private var graph: Graph[Double] = null;
    private var latch: CountDownLatch = null;

    override def setup(c: Conf): Unit = {
      logger.info(s"Setting up Instance with config: $c");
      this.numNodes = c.numberOfNodes;
      this.blockSize = c.blockSize;
      this.system = KompicsSystemProvider.newKompicsSystem();
      val f = for {
        manId <- system.createNotify[ManagerComponent](Init[ManagerComponent](blockSize));
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
        `!trigger`(Messages.ComputeFW(graph, latch) -> c.provided(ManagerPort.getClass()))(
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

  object Messages {
    case class ComputeFW(graph: Graph[Double], latch: CountDownLatch) extends KompicsEvent;
    case class BlockResult(block: Block[Double]) extends KompicsEvent;

    case class PhaseResult(k: Int, data: Block[Double]) extends KompicsEvent;
  }

  object ManagerPort extends Port {
    import Messages._;
    request[ComputeFW];
    request[BlockResult];
  }
  object BlockPort extends Port {
    import Messages._;
    indication[PhaseResult];
  }

  class ManagerComponent(init: Init[ManagerComponent]) extends ComponentDefinition {
    import Messages._;

    val managerPort = provides(ManagerPort);

    val Init(blockSize: Int) = init;

    private var blockWorkers: List[Component] = Nil;
    private var latch: Option[CountDownLatch] = None;
    private var assembly: Option[Array[Array[Block[Double]]]] = None;
    private var missingBlocks = -1;

    // test only!
    //private var result: Option[Graph[Double]] = None;

    managerPort uponEvent {
      case ComputeFW(graph, latch) => {
        log.debug("Got ComputeFW!");
        distributeGraph(graph);
        this.latch = Some(latch);
        // test only!
        //   val myGraph = graph.copy();
        //   myGraph.computeFloydWarshall();
        //   this.result = Some(myGraph);
      }
      case BlockResult(block) => {
        assembly match {
          case Some(blocks) => {
            val (i, j) = block.blockPosition;
            blocks(i)(j) = block;
            missingBlocks -= 1;
            if (missingBlocks == 0) {
              val result = Graph.assembleFromBlocks(blocks);
              latch.get.countDown();
              log.info(s"Done!");
              //log.info(s"Done, with result: $result");
              // cleanup
              // blockWorkers.foreach { c =>
              //   trigger(Kill.event -> c.control());
              // }
              blockWorkers = Nil;
              latch = None;
              assembly = None;
              missingBlocks = -1;
              // test only!
              //assert(this.result.get == result, "Wrong APSP result!");
            } else {
              log.debug(s"Got another $missingBlocks blocks outstanding.");
            }
          }
          case None => ??? // da fuq?
        }
      }
    }

    override def handleFault(fault: Fault): Fault.ResolveAction = {
      if (fault.getEvent().isInstanceOf[Kill]) {
        logger.info("Ignoring duplicate kill event.");
        return Fault.ResolveAction.DESTROY;
      } else {
        return Fault.ResolveAction.ESCALATE;
      }
    }

    private def distributeGraph(graph: Graph[Double]): Unit = {
      val blocks = graph.breakIntoBlocks(blockSize);
      val numNodes = graph.numNodes;
      val outerPort = managerPort.dualPositive;
      val neighbourCount = blocks.size * 2 - 2; // exclude myself
      val blockComponents =
        blocks.map(_.map(b => create[BlockComponent](Init[BlockComponent](b, numNodes, neighbourCount))));
      for (bi <- 0 until blocks.size; bj <- 0 until blocks.size) {
        val current = blockComponents(bi)(bj);
        connect(outerPort -> current);
        var neighbourCount2 = 0;
        // add neighbours in the same row
        for (r <- 0 until blocks.size) {
          if (r != bi) {
            val neighbour = blockComponents(r)(bj);
            connect(BlockPort)(neighbour -> current);
            neighbourCount2 += 1;
          }
        }
        // add neighbours in the same column
        for (c <- 0 until blocks.size) {
          if (c != bj) {
            val neighbour = blockComponents(bi)(c);
            connect(BlockPort)(neighbour -> current);
            neighbourCount2 += 1;
          }
        }
        assert(neighbourCount == neighbourCount2);
      }
      this.blockWorkers = blockComponents.map(_.toList).toList.flatten;
      this.blockWorkers.foreach { c =>
        trigger(Start.event -> c.control());
      }
      this.missingBlocks = this.blockWorkers.size;
      this.assembly = Some(blocks); // initialise with unfinished blocks
    }

  }

  class BlockComponent(init: Init[BlockComponent]) extends ComponentDefinition {
    import Messages._;

    val managerPort = requires(ManagerPort);
    val blockPortProv = provides(BlockPort);
    val blockPortReq = requires(BlockPort);

    val Init(intialBlock: Block[Double] @unchecked, numNodes: Int, numNeighbours: Int) = init;

    private var k: Int = -1;
    private var neighbourData = Map.empty[Int, Block[Double]];
    private var nextNeighbourData = Map.empty[Int, Block[Double]];
    private var currentData = intialBlock;
    private var done: Boolean = false;

    ctrl uponEvent {
      case _: Start => {
        log.debug(s"Got Start at blockId=${this.currentData.blockId}.");
        notifyNeighbours();
      }
    }

    blockPortReq uponEvent {
      case PhaseResult(otherK, data) => {
        if (!done) {
          if (otherK == this.k) {
            log.debug(s"Got PhaseResult(k=$otherK) at blockId=${this.currentData.blockId}.");

            this.neighbourData += (data.blockId -> data);
            while (this.neighbourData.size == this.numNeighbours) {
              this.k += 1;
              performComputation();
              notifyNeighbours();
              neighbourData = nextNeighbourData;
              nextNeighbourData = Map.empty[Int, Block[Double]];

              if (this.k == numNodes - 1) {
                trigger(BlockResult(currentData) -> managerPort);
                done = true;
                suicide();
              }
            }
          } else if (otherK == this.k + 1) {
            this.nextNeighbourData += (data.blockId -> data);
          } else {
            throw new RuntimeException(s"Got k=$otherK but I'm in phase k=${this.k}!")
          }
        }
      }
    }

    private def notifyNeighbours(): Unit = {
      trigger(PhaseResult(this.k, this.currentData) -> blockPortProv);
    }
    private def performComputation(): Unit = {
      currentData = currentData.copy(); // this would be unnecessary if I could see the reference count
      currentData.computeFloydWarshallInner(this.neighbourData.apply, this.k);
    }
  }
}
