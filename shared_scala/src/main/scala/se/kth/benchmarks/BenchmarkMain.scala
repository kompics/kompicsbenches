package se.kth.benchmarks

import kompics.benchmarks.benchmarks.BenchmarkRunnerGrpc
import scala.util.Try

object BenchmarkMain {
  def runWith(args: Array[String],
              benchFactory: BenchmarkFactory,
              runnerImpl: => BenchmarkRunnerGrpc.BenchmarkRunner,
              globalConfigure: (String) => Unit): Unit = {
    if (args.length == 1) {
      // local mode
      BenchmarkRunnerServer.runWith(args, runnerImpl);
    } else if (args.length == 2) {
      // client mode
      val masterAddr = Util.argToAddr(args(0)).get;
      val clientAddr = Util.argToAddr(args(1)).get;
      globalConfigure(clientAddr.addr);
      BenchmarkClient.run(clientAddr.addr, masterAddr.addr, masterAddr.port);
    } else if (args.length == 3) {
      // master mode
      val runnerAddr = Util.argToAddr(args(0)).get;
      val masterAddr = Util.argToAddr(args(1)).get;
      val numClients = Try(args(2).toInt).get;
      globalConfigure(masterAddr.addr);
      BenchmarkMaster.run(numClients, masterAddr.port, runnerAddr.port, benchFactory);
    }
  }
}
