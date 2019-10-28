package se.kth.benchmarks.akka

import se.kth.benchmarks.BenchmarkMain;

object Main {
  def main(args: Array[String]): Unit = {
    val (benchMode, arguments) = args.splitAt(1)
    benchMode.head match {
      case "untyped" => {
        import se.kth.benchmarks.akka.bench.Factory

//        println("Running untyped with args=" + arguments.mkString(","))
        BenchmarkMain.runWith(arguments, Factory, new BenchmarkRunnerImpl(), (publicIf) => {
          ActorSystemProvider.setPublicIf(publicIf)
        });
      }

      case "typed" => {
        import se.kth.benchmarks.akka.typed_bench.Factory

        BenchmarkMain.runWith(arguments, Factory, new TypedBenchmarkRunnerImpl(), (publicIf) => {
          ActorSystemProvider.setPublicIf(publicIf)
        });
      }
    }

  }
}
