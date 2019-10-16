package se.kth.benchmarks.helpers

import org.scalatest._

class GraphTest extends FunSuite with Matchers {
  test("Graph generation") {
    val g = GraphUtils.generateGraph(5);
    //println(g);
    g should ===(g);
  }

  test("Graph blocking should roundtrip") {
    val g = GraphUtils.generateGraph(10);
    //println(g);
    val blocks = g.breakIntoBlocks(5);
    for (bi <- 0 until blocks.size; bj <- 0 until blocks.size) {
      val (i, j) = blocks(bi)(bj).blockPosition;
      bi should ===(i);
      bj should ===(j);
    }
    //println(blocks.map(_.map(b => s"${b.blockId} -> $b").mkString("\n")).mkString("\n"));
    val g2 = Graph.assembleFromBlocks(blocks);
    g2 should ===(g);
  }

  test("Graph block floyd-warshall") {
    val g = GraphUtils.generateGraph(10);
    //println(g);
    val blocks = g.breakIntoBlocks(5);
    //println(blocks.map(_.map(b => s"${b.blockId} -> $b").mkString("\n")).mkString("\n"));
    val lookupfun: Int => Block[Double] = (blockId: Int) => {
      val i = blockId / blocks.size;
      val j = blockId % blocks.size;
      blocks(i)(j)
    };
    for (k <- 0 until 10) {
      blocks.foreach(_.foreach(block => block.computeFloydWarshallInner(lookupfun, k)));
    }
    //println("=== Blocked APSP ===");
    val gBlocked = Graph.assembleFromBlocks(blocks);
    //println(gBlocked);
    //println("=== Raw APSP ===");
    val g2 = g.copy();
    g2.computeFloydWarshall();
    //println(g2);
    gBlocked should ===(g2);
  }
}
