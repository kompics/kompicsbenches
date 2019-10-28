<<<<<<< HEAD
package se.kth.benchmarks.runner

import org.scalatest._
import kompics.benchmarks.benchmarks._

class ParameterTest extends FunSuite with Matchers {
  test("PingPong should CSV rountrip") {
    val bench = Benchmarks.pingPong;
    Benchmarks.benchmarkLookup(bench.symbol) should be theSameInstanceAs bench;
    val pp = PingPongRequest(100);
    val space = bench.space.asInstanceOf[ParameterSpacePB[PingPongRequest]];
    val descr = space.describe(pp);
    val csv = descr.toCSV;
    Console.err.println(s"PingPong CSV: $csv");
    val pp_deser = space.paramsFromCSV(csv);
    pp_deser should equal(pp);
  }

  test("Throughput PingPong should CSV rountrip") {
    val bench = Benchmarks.throughputPingPong;
    Benchmarks.benchmarkLookup(bench.symbol) should be theSameInstanceAs bench;
    val pp =
      ThroughputPingPongRequest(messagesPerPair = 100000000, pipelineSize = 10, parallelism = 2, staticOnly = true);
    val space = bench.space.asInstanceOf[ParameterSpacePB[ThroughputPingPongRequest]];
    val descr = space.describe(pp);
    val csv = descr.toCSV;
    Console.err.println(s"ThroughputPingPong CSV: $csv");
    val pp_deser = space.paramsFromCSV(csv);
    pp_deser should equal(pp);
  }

  test("Streaming Windows should CSV rountrip") {
    val bench = Benchmarks.streamingWindows;
    Benchmarks.benchmarkLookup(bench.symbol) should be theSameInstanceAs bench;
    val pp =
      StreamingWindowsRequest(numberOfPartitions = 2, batchSize = 100, windowSize = "100ms", numberOfWindows = 20);
    val space = bench.space.asInstanceOf[ParameterSpacePB[StreamingWindowsRequest]];
    val descr = space.describe(pp);
    val csv = descr.toCSV;
    Console.err.println(s"StreamingWindows CSV: $csv");
    val pp_deser = space.paramsFromCSV(csv);
    pp_deser should equal(pp);
  }
}
=======
package se.kth.benchmarks.runner

import org.scalatest._
import kompics.benchmarks.benchmarks._

class ParameterTest extends FunSuite with Matchers {
  test("PingPong should CSV rountrip") {
    val bench = Benchmarks.pingPong;
    Benchmarks.benchmarkLookup(bench.symbol) should be theSameInstanceAs bench;
    val pp = PingPongRequest(100);
    val space = bench.space.asInstanceOf[ParameterSpacePB[PingPongRequest]];
    val descr = space.describe(pp);
    val csv = descr.toCSV;
    Console.err.println(s"PingPong CSV: $csv");
    val pp_deser = space.paramsFromCSV(csv);
    pp_deser should equal(pp);
  }

  test("Throughput PingPong should CSV rountrip") {
    val bench = Benchmarks.throughputPingPong;
    Benchmarks.benchmarkLookup(bench.symbol) should be theSameInstanceAs bench;
    val pp =
      ThroughputPingPongRequest(messagesPerPair = 100000000, pipelineSize = 10, parallelism = 2, staticOnly = true);
    val space = bench.space.asInstanceOf[ParameterSpacePB[ThroughputPingPongRequest]];
    val descr = space.describe(pp);
    val csv = descr.toCSV;
    Console.err.println(s"ThroughputPingPong CSV: $csv");
    val pp_deser = space.paramsFromCSV(csv);
    pp_deser should equal(pp);
  }

  test("Streaming Windows should CSV rountrip") {
    val bench = Benchmarks.streamingWindows;
    Benchmarks.benchmarkLookup(bench.symbol) should be theSameInstanceAs bench;
    val pp =
      StreamingWindowsRequest(numberOfPartitions = 2, batchSize = 100, windowSize = "100ms", numberOfWindows = 20);
    val space = bench.space.asInstanceOf[ParameterSpacePB[StreamingWindowsRequest]];
    val descr = space.describe(pp);
    val csv = descr.toCSV;
    Console.err.println(s"StreamingWindows CSV: $csv");
    val pp_deser = space.paramsFromCSV(csv);
    pp_deser should equal(pp);
  }

  test("Fibonaccy should CSV rountrip") {
    val bench = Benchmarks.fibonacci;
    Benchmarks.benchmarkLookup(bench.symbol) should be theSameInstanceAs bench;
    val pp =
      FibonacciRequest(fibNumber = 28);
    val space = bench.space.asInstanceOf[ParameterSpacePB[FibonacciRequest]];
    val descr = space.describe(pp);
    val csv = descr.toCSV;
    Console.err.println(s"Fibonacci CSV: $csv");
    val pp_deser = space.paramsFromCSV(csv);
    pp_deser should equal(pp);
  }

  test("Chameneos should CSV rountrip") {
    val bench = Benchmarks.chameneos;
    Benchmarks.benchmarkLookup(bench.symbol) should be theSameInstanceAs bench;
    val pp =
      ChameneosRequest(numberOfChameneos = 12, numberOfMeetings = 100000L);
    val space = bench.space.asInstanceOf[ParameterSpacePB[ChameneosRequest]];
    val descr = space.describe(pp);
    val csv = descr.toCSV;
    Console.err.println(s"Chameneos CSV: $csv");
    val pp_deser = space.paramsFromCSV(csv);
    pp_deser should equal(pp);
  }

  test("APSP should CSV rountrip") {
    val bench = Benchmarks.allPairsShortestPath;
    Benchmarks.benchmarkLookup(bench.symbol) should be theSameInstanceAs bench;
    val pp =
      APSPRequest(numberOfNodes = 128, blockSize = 8);
    val space = bench.space.asInstanceOf[ParameterSpacePB[APSPRequest]];
    val descr = space.describe(pp);
    val csv = descr.toCSV;
    Console.err.println(s"APSP CSV: $csv");
    val pp_deser = space.paramsFromCSV(csv);
    pp_deser should equal(pp);
  }
}
>>>>>>> c92c44604e519dd0b6cc96f7ef122b9ca6b9cde1
