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
