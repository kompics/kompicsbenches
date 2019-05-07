package se.kth.benchmarks.kompics

import se.kth.benchmarks.BenchmarkMain;
import se.kth.benchmarks.kompicsscala.KompicsSystemProvider

object Main {
  def main(args: Array[String]): Unit = {
    val benchMode = args(0);
    benchMode match {
      case "java" => {

        import se.kth.benchmarks.kompicsjava.BenchmarkRunnerImpl;
        import se.kth.benchmarks.kompicsjava.bench.Factory;

        BenchmarkMain.runWith(args.tail, Factory, new BenchmarkRunnerImpl(), (publicIf) => {
          KompicsSystemProvider.setPublicIf(publicIf)
        });
      }
      case "scala" => {

        import se.kth.benchmarks.kompicsscala.BenchmarkRunnerImpl;
        import se.kth.benchmarks.kompicsscala.bench.Factory;

        BenchmarkMain.runWith(args.tail, Factory, new BenchmarkRunnerImpl(), (publicIf) => {
          KompicsSystemProvider.setPublicIf(publicIf)
        });
      }
      case s => ???
    }
  }
}
