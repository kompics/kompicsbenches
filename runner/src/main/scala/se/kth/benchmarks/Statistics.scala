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
}
