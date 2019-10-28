package se.kth.benchmarks.runner.utils

import scala.concurrent.duration._;

object Conversions {

  case class SizeToTime(mbPerSec: Double, min: Duration) {

    def timeForMB(size: Double): String = {
      val t = size / mbPerSec;
      val dur = t.seconds;
      assert(dur >= min, s"Duration must be >= ${min}, but was ${dur}");
      dur.toString()
    }
  }
}
