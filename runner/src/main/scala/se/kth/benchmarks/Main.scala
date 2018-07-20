package se.kth.benchmarks

import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import scala.concurrent.{ Future, ExecutionContext, Await }
import scala.concurrent.duration.Duration
import scala.util.{ Try, Success, Failure }
import io.grpc.{ StatusRuntimeException, ManagedChannelBuilder, ManagedChannel }

object Main {
  implicit val ec = ExecutionContext.global;

  def main(args: Array[String]): Unit = {
    val addr = if (args.length < 2) "127.0.0.1:45678" else args(1);
    val addrParts = addr.split(":");
    assert(addrParts.length == 2);
    val host = addrParts(0);
    val port = Integer.parseUnsignedInt(addrParts(1));

    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build;
    val request = PingPongRequest(numberOfMessages = 1000000);
    val stub = BenchmarkRunnerGrpc.stub(channel);
    val runner = new Runner(stub);
    runner.runAll();
  }
}
