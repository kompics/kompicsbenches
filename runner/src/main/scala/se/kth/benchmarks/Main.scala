package se.kth.benchmarks

import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import scala.concurrent.{ Future, ExecutionContext, Await }
import scala.concurrent.duration.Duration
import scala.util.{ Try, Success, Failure }
import io.grpc.{ StatusRuntimeException, ManagedChannelBuilder, ManagedChannel }
import org.rogach.scallop._
import java.io.File

object Main {
  implicit val ec = ExecutionContext.global;

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args);
    val (host, port) = conf.server();

    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build;
    val stub = BenchmarkRunnerGrpc.stub(channel);
    val runner = new Runner(conf.prefix(), conf.outputFolder(), stub);
    runner.runAll();
  }
}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {

  val server = opt[String](
    descr = "Address of the benchmark server to connect to.",
    default = Some("127.0.0.1:45678")).map(addr => {
      val addrParts = addr.split(":");
      assert(addrParts.length == 2);
      val host = addrParts(0);
      val port = Integer.parseUnsignedInt(addrParts(1));
      (host, port)
    });

  val prefix = opt[String](
    descr = "Symbol identifying the implementation running the benchmark.",
    required = true);

  val outputFolder = opt[File](
    descr = "Result folder.",
    required = true);

  verify();
}
