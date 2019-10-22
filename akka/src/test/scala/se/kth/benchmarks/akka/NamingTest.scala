package se.kth.benchmarks.akka

import org.scalatest._
import org.scalactic.source.Position.apply

/**
 * These tests make sure that akka will be able to find custom serializers by name.
 */
class NamingTest extends FunSuite with Matchers {
  test("Top level classes should be named properly") {
    val ref = ClassRef.from[TestClass];
    ref.fullName should equal ("se.kth.benchmarks.akka.TestClass");
  }
  test("Top level objects should be named properly") {
    val ref = ClassRef.from[TestObject.type];
    ref.fullName should equal ("se.kth.benchmarks.akka.TestObject$");
  }
  test("Inner classes should be named properly") {
    val ref = ClassRef.from[TestObject.InnerTestClass];
    ref.fullName should equal ("se.kth.benchmarks.akka.TestObject$InnerTestClass");
  }
  test("Inner objects should be named properly") {
    val ref = ClassRef.from[TestObject.InnerTestObject.type];
    ref.fullName should equal ("se.kth.benchmarks.akka.TestObject$InnerTestObject$");
  }
}

class TestClass {

}
object TestObject {
  class InnerTestClass {

  }
  object InnerTestObject {

  }
}
