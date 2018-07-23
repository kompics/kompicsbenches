package se.kth.benchmarks

trait Benchmark {
  type Conf;
  def setup(c: Conf): Unit;
  def prepareIteration(): Unit = {}
  def runIteration(): Unit;
  def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {}
}
