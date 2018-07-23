package se.kth.benchmarks

object Main {
  def main(args: Array[String]): Unit = {
    BenchmarkRunnerServer.runWith(args, new BenchmarkRunnerImpl());
  }
}
