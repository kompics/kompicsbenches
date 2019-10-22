<<<<<<< HEAD
package se.kth.benchmarks.kompicsscala.bench

import se.kth.benchmarks._

object Factory extends BenchmarkFactory {
  override def pingPong(): Benchmark = PingPong;
  override def netPingPong(): DistributedBenchmark = NetPingPong;
  override def throughputPingPong(): Benchmark = ThroughputPingPong;
  override def netThroughputPingPong(): DistributedBenchmark = NetThroughputPingPong;
  override def atomicRegister(): DistributedBenchmark = AtomicRegister;
  override def streamingWindows(): DistributedBenchmark = StreamingWindows;
}
=======
package se.kth.benchmarks.kompicsscala.bench

import se.kth.benchmarks._

object Factory extends BenchmarkFactory {
  override def pingPong(): Benchmark = PingPong;
  override def netPingPong(): DistributedBenchmark = NetPingPong;
  override def throughputPingPong(): Benchmark = ThroughputPingPong;
  override def netThroughputPingPong(): DistributedBenchmark = NetThroughputPingPong;
  override def atomicRegister(): DistributedBenchmark = AtomicRegister;
  override def streamingWindows(): DistributedBenchmark = StreamingWindows;
  override def fibonacci: Benchmark = Fibonacci;
  override def chameneos(): Benchmark = Chameneos;
  override def allPairsShortestPath(): Benchmark = AllPairsShortestPath;
}
>>>>>>> c92c44604e519dd0b6cc96f7ef122b9ca6b9cde1
