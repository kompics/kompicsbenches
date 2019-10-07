package se.kth.benchmarks.akka.bench

import se.kth.benchmarks._

object Factory extends BenchmarkFactory {
  override def pingPong(): Benchmark = PingPong;
  override def netPingPong(): DistributedBenchmark = NetPingPong;
  override def netThroughputPingPong(): se.kth.benchmarks.DistributedBenchmark = NetThroughputPingPong;
  override def throughputPingPong(): se.kth.benchmarks.Benchmark = ThroughputPingPong;
  override def atomicRegister(): DistributedBenchmark = AtomicRegister;
  override def streamingWindows(): DistributedBenchmark = StreamingWindows;
}
