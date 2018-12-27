package se.kth.benchmarks.akka

import se.kth.benchmarks.BenchmarkMain;

object Main {
  def main(args: Array[String]): Unit = {
    BenchmarkMain.runWith(args, bench.Factory, new BenchmarkRunnerImpl(), (publicIf) => {
      ActorSystemProvider.setPublicIf(publicIf)
    });
  }
}
