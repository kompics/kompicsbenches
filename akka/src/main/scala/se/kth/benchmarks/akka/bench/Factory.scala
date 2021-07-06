package se.kth.benchmarks.akka.bench

import se.kth.benchmarks._

object Factory extends BenchmarkFactory {
  override def pingPong(): Benchmark = PingPong;
  override def netPingPong(): DistributedBenchmark = NetPingPong;
  override def netThroughputPingPong(): se.kth.benchmarks.DistributedBenchmark = NetThroughputPingPong;
  override def throughputPingPong(): se.kth.benchmarks.Benchmark = ThroughputPingPong;
  override def atomicRegister(): DistributedBenchmark = AtomicRegister;
  override def streamingWindows(): DistributedBenchmark = StreamingWindows;
  override def allPairsShortestPath(): se.kth.benchmarks.Benchmark = AllPairsShortestPath;
  override def chameneos(): se.kth.benchmarks.Benchmark = Chameneos;
  override def fibonacci: se.kth.benchmarks.Benchmark = Fibonacci;
  override def sizedThroughput(): DistributedBenchmark = SizedThroughput;
  override def atomicBroadcast(): DistributedBenchmark = ???
}
