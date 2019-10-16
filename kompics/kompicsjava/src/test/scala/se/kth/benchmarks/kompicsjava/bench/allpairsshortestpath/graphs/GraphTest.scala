package se.kth.benchmarks.kompicsjava.bench.allpairsshortestpath.graphs

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
      val i = blocks(bi)(bj).blockPositionI();
      val j = blocks(bi)(bj).blockPositionJ();
      bi should ===(i);
      bj should ===(j);
    }
    //println(blocks.map(_.map(b => s"${b.blockId} -> $b").mkString("\n")).mkString("\n"));
    val g2 = DoubleGraph.assembleFromBlocks(blocks);
    g2 should ===(g);
  }

  test("Graph block floyd-warshall") {
    val g = GraphUtils.generateGraph(10);
    //println(g);
    val blocks = g.breakIntoBlocks(5);
    //println(blocks.map(_.map(b => s"${b.blockId} -> $b").mkString("\n")).mkString("\n"));
    val lookupMap: java.util.Map[java.lang.Integer, DoubleBlock] = new java.util.TreeMap[Integer, DoubleBlock];
    blocks.foreach(_.foreach(b => lookupMap.put(b.blockId, b)));
    for (k <- 0 until 10) {
      blocks.foreach(_.foreach(block => block.computeFloydWarshallInner(lookupMap, k)));
    }
    //println("=== Blocked APSP ===");
    val gBlocked = DoubleGraph.assembleFromBlocks(blocks);
    //println(gBlocked);
    //println("=== Raw APSP ===");
    val g2 = g.copy();
    g2.computeFloydWarshall();
    //println(g2);
    gBlocked should ===(g2);
  }
}
