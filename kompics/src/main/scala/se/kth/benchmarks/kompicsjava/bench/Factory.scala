package se.kth.benchmarks.kompicsjava.bench

import se.kth.benchmarks._

object Factory extends BenchmarkFactory {
  override def pingpong(): Benchmark = PingPong;
  override def netpingpong(): DistributedBenchmark = NetPingPong;
}
