package se.kth.benchmarks

import org.scalatest._
import java.security.InvalidParameterException

class StatisticsTest extends FunSuite with Matchers {
  import scala.collection.mutable.ArrayBuffer;

  test("Median for empty buffer should fail.") {
    val buf = ArrayBuffer.empty[Long];
    assertThrows[IllegalArgumentException] { Statistics.medianFromUnsorted(buf) };
  }

  test("Median should not fail for arrays of different lengths > 0") {
    val buf = ArrayBuffer.empty[Long];
    for (i <- 0L to 1000L) {
      buf += i;
      //println(s"Testing with buf of size ${buf.size}");
      val median = Statistics.medianFromUnsorted(buf);
      median should be >= 0.0;
    }
  }
  test("Median should produce correct values") {
    {
      val buf = ArrayBuffer(1L);
      val median = Statistics.medianFromUnsorted(buf);
      median shouldBe 1.0 +- 0.0001;
    }
    {
      val buf = ArrayBuffer(1L, 3L);
      val median = Statistics.medianFromUnsorted(buf);
      median shouldBe 2.0 +- 0.0001;
    }
    {
      val buf = ArrayBuffer(1L, 3L, 5L);
      val median = Statistics.medianFromUnsorted(buf);
      median shouldBe 3.0 +- 0.0001;
    }
    {
      val buf = ArrayBuffer(1L, 3L, 5L, 18L);
      val median = Statistics.medianFromUnsorted(buf);
      median shouldBe 4.0 +- 0.0001;
    }
  }
}
