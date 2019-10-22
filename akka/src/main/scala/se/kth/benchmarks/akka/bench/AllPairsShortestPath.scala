package se.kth.benchmarks.akka.bench

import se.kth.benchmarks.akka.ActorSystemProvider
import se.kth.benchmarks.Benchmark
import se.kth.benchmarks.helpers.{Block, Graph, GraphUtils}
import kompics.benchmarks.benchmarks.APSPRequest
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, PoisonPill, Props}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try
import scala.collection.mutable
import java.util.concurrent.CountDownLatch
import com.typesafe.scalalogging.StrictLogging

object AllPairsShortestPath extends Benchmark {

  override type Conf = APSPRequest;

  override def msgToConf(msg: scalapb.GeneratedMessage): Try[Conf] = {
    Try(msg.asInstanceOf[APSPRequest])
  };
  override def newInstance(): Instance = new AllPairsShortestPathI;

  class AllPairsShortestPathI extends Instance with StrictLogging {

    private var numNodes = -1;
    private var blockSize = -1;
    private var system: ActorSystem = null;
    private var manager: ActorRef = null;
    private var graph: Graph[Double] = null;
    private var latch: CountDownLatch = null;

    override def setup(c: Conf): Unit = {
      logger.info(s"Setting up Instance with config: $c");
      this.numNodes = c.numberOfNodes;
      this.blockSize = c.blockSize;
      this.system = ActorSystemProvider.newActorSystem(name = "apsp");
      this.manager = system.actorOf(Props(new ManagerActor(blockSize)));
      this.graph = GraphUtils.generateGraph(numNodes);
    }
    override def prepareIteration(): Unit = {
      logger.debug("Preparing iteration");
      this.latch = new CountDownLatch(1);
    }
    override def runIteration(): Unit = {
      assert(this.latch != null);
      assert(this.manager != null);
      assert(this.graph != null);
      this.manager ! Messages.ComputeFW(graph, latch);
      latch.await();
    }
    override def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      logger.debug("Cleaning up iteration");
      if (this.latch != null) {
        this.latch = null;
      }
      if (lastIteration) {
        if (this.manager != null) {
          system.stop(manager);
          this.manager = null;
        }
        if (this.graph != null) {
          this.graph = null;
        }
        val f = system.terminate();
        Await.ready(f, 5.second);
        system = null;
        logger.info("Cleaned up Instance");
      }
    }
  }

  sealed trait BlockMsg;
  sealed trait ManagerMsg;
  object Messages {
    case class ComputeFW(graph: Graph[Double], latch: CountDownLatch) extends ManagerMsg;
    case class BlockResult(block: Block[Double]) extends ManagerMsg;

    case class Neighbours(nodes: List[ActorRef]) extends BlockMsg;
    case class PhaseResult(k: Int, data: Block[Double]) extends BlockMsg;
  }

  class ManagerActor(val blockSize: Int) extends Actor with ActorLogging {
    import Messages._;

    private var blockWorkers: List[ActorRef] = Nil;
    private var latch: Option[CountDownLatch] = None;
    private var assembly: Option[Array[Array[Block[Double]]]] = None;
    private var missingBlocks = -1;

    // test only!
    // private var result: Option[Graph[Double]] = None;

    override def receive: Receive = {
      case ComputeFW(graph, latch) => {
        log.debug("Got ComputeFW!");
        distributeGraph(graph);
        this.latch = Some(latch);
        // test only!
        // val myGraph = graph.copy();
        // myGraph.computeFloydWarshall();
        // this.result = Some(myGraph);
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
              // log.info(s"Done, with result: $result");
              // cleanup
              blockWorkers = Nil;
              latch = None;
              assembly = None;
              missingBlocks = -1;
              // test only!
              //   assert(this.result.get == result, "Wrong APSP result!");
            } else {
              log.debug(s"Got another $missingBlocks blocks outstanding.");
            }
          }
          case None => ??? // da fuq?
        }
      }
    }

    private def distributeGraph(graph: Graph[Double]): Unit = {
      val blocks = graph.breakIntoBlocks(blockSize);
      val numNodes = graph.numNodes;
      val blockActors = blocks.map(_.map(b => context.actorOf(Props(new BlockActor(b, numNodes, self)))));
      for (bi <- 0 until blocks.size; bj <- 0 until blocks.size) {
        var neighbours: List[ActorRef] = Nil;
        // add neighbours in the same row
        for (r <- 0 until blocks.size) {
          if (r != bi) {
            neighbours ::= blockActors(r)(bj);
          }
        }
        // add neighbours in the same column
        for (c <- 0 until blocks.size) {
          if (c != bj) {
            neighbours ::= blockActors(bi)(c);
          }
        }
        blockActors(bi)(bj) ! Neighbours(neighbours);
      }
      this.blockWorkers = blockActors.map(_.toList).toList.flatten;
      this.missingBlocks = this.blockWorkers.size;
      this.assembly = Some(blocks); // initialise with unfinished blocks
    }
  }

  class BlockActor(intialBlock: Block[Double], val numNodes: Int, val manager: ActorRef)
      extends Actor
      with ActorLogging {
    import Messages._;

    private var ready: Boolean = false;
    private var neighbours: List[ActorRef] = Nil;
    private var k: Int = -1;
    private var neighbourData = Map.empty[Int, Block[Double]];
    private var nextNeighbourData = Map.empty[Int, Block[Double]];
    private var currentData = intialBlock;

    override def receive: Receive = {
      case Neighbours(nodes) => {
        log.debug(s"Got ${nodes.size} neighbours at blockId=${this.currentData.blockId}.");
        this.neighbours = nodes;
        this.ready = true;
        notifyNeighbours();
      }
      case msg: PhaseResult if !ready => {
        self ! msg; // queue msg
      }
      case PhaseResult(k, data) if k == this.k => {
        log.debug(s"Got PhaseResult(k=$k) at blockId=${this.currentData.blockId}.");

        this.neighbourData += (data.blockId -> data);
        while (this.neighbourData.size == neighbours.size) {
          this.k += 1;
          performComputation();
          notifyNeighbours();
          neighbourData = nextNeighbourData;
          nextNeighbourData = Map.empty[Int, Block[Double]];

          if (this.k == numNodes - 1) {
            manager ! BlockResult(currentData);
            context.stop(self);
          }
        }
      }
      case PhaseResult(k, data) if k == this.k + 1 => {
        this.nextNeighbourData += (data.blockId -> data);
      }
      case PhaseResult(k, _) => throw new RuntimeException(s"Got k=$k but I'm in phase k=${this.k}!")
    }

    private def notifyNeighbours(): Unit = {
      assert(this.neighbours != Nil, "Should not be ready!");
      val msg = PhaseResult(this.k, this.currentData);
      neighbours.foreach { neighbour =>
        neighbour ! msg;
      }
    }
    private def performComputation(): Unit = {
      currentData = currentData.copy(); // this would be unnecessary if I could see the reference count
      currentData.computeFloydWarshallInner(this.neighbourData.apply, this.k);
    }
  }
}
