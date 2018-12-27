package se.kth.benchmarks.akka.bench

import se.kth.benchmarks._

object Factory extends BenchmarkFactory {
  override def pingpong(): Benchmark = PingPong;
  override def netpingpong(): DistributedBenchmark = NetPingPong;
}
