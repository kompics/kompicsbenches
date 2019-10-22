<<<<<<< HEAD
package se.kth.benchmarks.kompicsjava

import org.scalatest._

class DistributedTest extends FunSuite with Matchers {
  test("Local communication") {
    val ltest = new se.kth.benchmarks.test.LocalTest(new BenchmarkRunnerImpl());
    ltest.test();
  }
}
=======
package se.kth.benchmarks.kompicsjava

import org.scalatest._

class LocalTest extends FunSuite with Matchers {
  test("Local communication") {
    val ltest = new se.kth.benchmarks.test.LocalTest(new BenchmarkRunnerImpl());
    ltest.test();
  }
}
>>>>>>> c92c44604e519dd0b6cc96f7ef122b9ca6b9cde1
