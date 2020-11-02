package se.kth.benchmarks

import scala.util.Try

trait Benchmark {
  type Conf;

  trait Instance {
    def setup(c: Conf): Unit;
    def prepareIteration(): Unit = {}
    def runIteration(): Unit;
    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {}
  }

  def msgToConf(msg: scalapb.GeneratedMessage): Try[Conf];
  def newInstance(): Instance;
}

case class DeploymentMetaData(numberOfClients: Int)

trait DistributedBenchmark {
  type MasterConf;
  type ClientConf;
  type ClientData;

  trait Master {
    def setup(c: MasterConf, meta: DeploymentMetaData): Try[ClientConf];
    def prepareIteration(d: List[ClientData]): Unit = {}
    def runIteration(): Unit;
    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {}
  }

  trait Client {
    def setup(c: ClientConf): ClientData;
    def prepareIteration(): Unit = {}
    def cleanupIteration(lastIteration: Boolean): Unit = {}
  }

  def newMaster(): Master;
  def msgToMasterConf(msg: scalapb.GeneratedMessage): Try[MasterConf];

  def newClient(): Client;
  def strToClientConf(str: String): Try[ClientConf];
  def strToClientData(str: String): Try[ClientData];

  def clientConfToString(c: ClientConf): String;
  def clientDataToString(d: ClientData): String;
}

//case class BenchmarkEntry[C, B <: Benchmark](b: B)(implicit ev: C =:= B#Conf)

trait BenchmarkFactory {
  //import kompics.benchmarks.benchmarks._;

  def pingPong(): Benchmark;
  def netPingPong(): DistributedBenchmark;
  def throughputPingPong(): Benchmark;
  def netThroughputPingPong(): DistributedBenchmark;
  def atomicRegister(): DistributedBenchmark;
  def streamingWindows(): DistributedBenchmark;
  def allPairsShortestPath(): Benchmark;
  def chameneos(): Benchmark;
  def fibonacci: Benchmark;
  def atomicBroadcast(): DistributedBenchmark;
  def sizedThroughput: DistributedBenchmark;
}
