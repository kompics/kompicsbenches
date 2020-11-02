package se.kth.benchmarks.kompicsscala.bench

import se.kth.benchmarks._
import se.kth.benchmarks.helpers.ChameneosColour

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
  override def sizedThroughput(): DistributedBenchmark = SizedThroughput;
  override def atomicBroadcast(): DistributedBenchmark = ???;
}
