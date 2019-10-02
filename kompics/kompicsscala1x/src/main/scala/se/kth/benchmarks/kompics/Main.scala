package se.kth.benchmarks.kompics

import se.kth.benchmarks.BenchmarkMain;
import se.kth.benchmarks.kompicsscala.KompicsSystemProvider

object Main {
  def main(args: Array[String]): Unit = {
    import se.kth.benchmarks.kompicsscala.BenchmarkRunnerImpl;
    import se.kth.benchmarks.kompicsscala.bench.Factory;

    BenchmarkMain.runWith(args, Factory, new BenchmarkRunnerImpl(), (publicIf) => {
      KompicsSystemProvider.setPublicIf(publicIf)
    });
  }
}
