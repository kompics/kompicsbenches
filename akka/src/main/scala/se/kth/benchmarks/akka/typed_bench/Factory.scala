package se.kth.benchmarks.akka.typed_bench

import se.kth.benchmarks.{Benchmark, BenchmarkFactory, DistributedBenchmark}

object Factory extends BenchmarkFactory {
  override def pingPong(): Benchmark = PingPong;
  override def netPingPong(): DistributedBenchmark = NetPingPong;
  override def throughputPingPong(): Benchmark = ThroughputPingPong;
  override def netThroughputPingPong(): DistributedBenchmark = NetThroughputPingPong;
  override def atomicRegister(): DistributedBenchmark = AtomicRegister;
  override def streamingWindows(): DistributedBenchmark = StreamingWindows;
  override def allPairsShortestPath(): se.kth.benchmarks.Benchmark = AllPairsShortestPath;
  override def chameneos(): se.kth.benchmarks.Benchmark = Chameneos;
  override def fibonacci: se.kth.benchmarks.Benchmark = Fibonacci;
  override def atomicBroadcast(): DistributedBenchmark = ???
  override def sizedThroughput(): DistributedBenchmark = SizedThroughput;
}
