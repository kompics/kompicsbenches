package se.kth.benchmarks

import io.grpc.{ Server, ServerBuilder }
import scala.concurrent.{ ExecutionContext, Future }

object Main {
  def main(args: Array[String]): Unit = {
    val server = new BenchmarkRunnerServer(45678, ExecutionContext.global)
    server.start()
    server.blockUntilShutdown()
  }
}
