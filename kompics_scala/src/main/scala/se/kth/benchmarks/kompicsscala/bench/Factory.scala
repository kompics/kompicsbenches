package se.kth.benchmarks.kompicsscala.bench

import se.kth.benchmarks._

object Factory extends BenchmarkFactory {
  override def pingpong(): Benchmark = PingPong;
  override def netpingpong(): DistributedBenchmark = ???; // TODO implement
}
