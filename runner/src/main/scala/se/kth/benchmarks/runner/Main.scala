package se.kth.benchmarks.runner

import kompics.benchmarks.benchmarks._
import kompics.benchmarks.messages._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import se.kth.benchmarks.BenchmarkRunnerServer;
import io.grpc.ManagedChannelBuilder

import org.rogach.scallop._
import java.io.File

object Main {
  implicit val ec = ExecutionContext.global;

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args);
    val (host, port) = conf.server();

    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build;
    val stub = BenchmarkRunnerGrpc.stub(channel);
    val runner = new Runner(conf, stub);
    if (conf.benchmarks.isSupplied) {
      runner.runOnly(conf.benchmarks());
    } else {
      runner.runAll();
    }
  }
}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {

  val server = opt[String](descr = "Address of the benchmark server to connect to.",
                           default = Some(s"127.0.0.1:${BenchmarkRunnerServer.DEFAULT_PORT}")).map(addr => {
    val addrParts = addr.split(":");
    assert(addrParts.length == 2);
    val host = addrParts(0);
    val port = Integer.parseUnsignedInt(addrParts(1));
    (host, port)
  });

  val prefix = opt[String](descr = "Symbol identifying the implementation running the benchmark.", required = true);

  val outputFolder = opt[File](descr = "Result folder.");

  val console = opt[Boolean](descr = "Output to console instead of result folder");

  val testing = opt[Boolean](
    descr = "Run with testing parameter space instead of the larger benchmark parameter space.",
    default = Some(false)
  );

  val benchmarks = opt[String](
    descr = "Only run benchmarks whose symbols appear in this list.",
    default = None
  );

  requireOne(outputFolder, console);

  verify();
}
