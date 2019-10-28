package se.kth.benchmarks

class Statistics(results: Seq[Double]) {
  lazy val sampleSize = results.size.toDouble;
  lazy val sampleMean = results.sum / sampleSize;
  lazy val sampleVariance = results.foldLeft(0.0) { (acc, sample) =>
    val err = sample - sampleMean;
    acc + (err * err)
  } / (sampleSize - 1.0);
  lazy val sampleStandardDeviation = Math.sqrt(sampleVariance);
  lazy val standardErrorOfTheMean = sampleStandardDeviation / Math.sqrt(sampleSize);
  lazy val relativeErrorOfTheMean = standardErrorOfTheMean / sampleMean;
  lazy val symmetricConfidenceInterval95: (Double, Double) = {
    val cidist = 1.96 * standardErrorOfTheMean;
    (sampleMean - cidist, sampleMean + cidist)
  }
  def render(unit: String): String = {
    s"#${sampleSize} with mean of ${sampleMean}${unit} and error of ${standardErrorOfTheMean}${unit} (${relativeErrorOfTheMean * 100.0}%)"
  }
}

object Statistics {
  import scala.collection.mutable.ArrayBuffer;

  def medianFromUnsorted(buf: ArrayBuffer[Long]): Double = {
    require(!buf.isEmpty, "No medians in empty collection!");
    val sortedBuf = buf.sorted; // this is pretty inefficient, but sortInPlace only appears in Scala 2.13
    val len = sortedBuf.length;
    val median = if (len % 2 == 0) { // is even
      val upperMiddle = len / 2; // zero indexing
      val lowerMiddle = upperMiddle - 1;
      (sortedBuf(lowerMiddle) + sortedBuf(upperMiddle)).toDouble / 2.0
    } else { // is odd
      val middle = len / 2;
      sortedBuf(middle).toDouble
    };
    median
  }
}
