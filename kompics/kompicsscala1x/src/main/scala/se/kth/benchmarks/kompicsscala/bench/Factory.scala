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
