package se.kth.benchmarks.kompicsscala

import se.kth.benchmarks.BenchmarkMain;

object Main {
  def main(args: Array[String]): Unit = {
    BenchmarkMain.runWith(args, bench.Factory, new BenchmarkRunnerImpl(), (publicIf) => {
      // TODO
      // KompicsSystemProvider.setPublicIf(publicIf)
    });
  }
}
