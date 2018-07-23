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
