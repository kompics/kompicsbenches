package se.kth.benchmarks.runner.utils

import com.panayotis.gnuplot.{ plot => gplot, _ }
import scala.reflect._
import scala.collection.mutable

object ImplResults {
  def slices[Params, T](
    input:   Map[String, ImplGroupedResult[Params]],
    grouper: Params => T,
    mapper:  Params => Long): Map[T, Map[String, Impl2DResult]] = {
    val builder = mutable.HashMap.empty[T, mutable.HashMap[String, Impl2DResult]];
    val sliced = input.mapValues(_.slices(grouper, mapper));
    sliced.foreach {
      case (impl, slices) =>
        slices.foreach {
          case (key, slice) =>
            val entry = builder.getOrElseUpdate(key, mutable.HashMap.empty);
            entry += (impl -> slice);
        }
    }
    builder.mapValues(_.toMap).toMap
  }

  def dices[Params, T](
    input:   Map[String, ImplGroupedResult[Params]],
    grouper: Params => T,
    mapper:  Params => (Long, Long)): Map[T, Map[String, Impl3DResult]] = {
    val builder = mutable.HashMap.empty[T, mutable.HashMap[String, Impl3DResult]];
    val sliced = input.mapValues(_.dices(grouper, mapper));
    sliced.foreach {
      case (impl, slices) =>
        slices.foreach {
          case (key, slice) =>
            val entry = builder.getOrElseUpdate(key, mutable.HashMap.empty);
            entry += (impl -> slice);
        }
    }
    builder.mapValues(_.toMap).toMap
  }

  def mapData[Params](
    input: Map[String, ImplGroupedResult[Params]],
    f:     (Params, Double) => Double): Map[String, ImplGroupedResult[Params]] = {
    input.mapValues(_.mapMeans(f))
  }
}

case class ImplGroupedResult[Params: ClassTag](implLabel: String, params: List[Params], means: List[Double]) {
  def mapParams[P: ClassTag](f: Params => P): ImplGroupedResult[P] = {
    this.copy(params = this.params.map(f))
  }

  def mapMeans(f: (Params, Double) => Double): ImplGroupedResult[Params] = {
    val newMeans = params.zip(means).map(t => f.tupled(t));
    this.copy(means = newMeans)
  }

  def map2D(f: Params => Long): Impl2DResult = {
    Impl2DResult(implLabel, this.params.map(f).toArray, means.toArray)
  }

  def map3D(f: Params => (Long, Long)): Impl3DResult = {
    val (params1, params2) = this.params.map(f).unzip;
    Impl3DResult(implLabel, params1.toArray, params2.toArray, means.toArray)
  }

  def groupBy[T](grouper: Params => T): Map[T, ImplGroupedResult[Params]] = {
    val indexedParams = params.zipWithIndex;
    val builder = mutable.HashMap.empty[T, (mutable.ListBuffer[Params], mutable.ListBuffer[Double])];
    val meanArray = means.toArray;
    indexedParams.foreach {
      case (p, i) =>
        val key = grouper(p);
        val entry = builder.getOrElseUpdate(key, (mutable.ListBuffer.empty, mutable.ListBuffer.empty));
        val mean = meanArray(i);
        entry._1 += p;
        entry._2 += mean;
    }
    builder.mapValues(t => ImplGroupedResult(implLabel, t._1.toList, t._2.toList)).toMap
  }

  def slices[T](grouper: Params => T, mapper: Params => Long): Map[T, Impl2DResult] = {
    groupBy(grouper).mapValues(_.map2D(mapper))
  }

  def dices[T](grouper: Params => T, mapper: Params => (Long, Long)): Map[T, Impl3DResult] = {
    groupBy(grouper).mapValues(_.map3D(mapper))
  }

  def slice(keep: Params => Boolean, mapper: Params => Long): Impl2DResult = {
    val paramArray = params.toArray;
    val meanArray = means.toArray;
    assert(paramArray.length == meanArray.length);
    val newParams = new mutable.ArrayBuilder.ofLong;
    val newMeans = new mutable.ArrayBuilder.ofDouble;
    for (i <- 0 to paramArray.length) {
      val p = paramArray(i);
      if (keep(p)) {
        newParams += mapper(p);
        newMeans += meanArray(i);
      }
    }
    val newParamsRes = newParams.result();
    val newMeansRes = newMeans.result();
    assert(newParamsRes.length == newMeansRes.length);
    Impl2DResult(implLabel, newParamsRes, newMeansRes)
  }
  def dice(keep: Params => Boolean, mapper: Params => (Long, Long)): Impl3DResult = {
    val paramArray = params.toArray;
    val meanArray = means.toArray;
    assert(paramArray.length == meanArray.length);
    val newParams1 = new mutable.ArrayBuilder.ofLong;
    val newParams2 = new mutable.ArrayBuilder.ofLong;
    val newMeans = new mutable.ArrayBuilder.ofDouble;
    for (i <- 0 to paramArray.length) {
      val p = paramArray(i);
      if (keep(p)) {
        val (p1, p2) = mapper(p);
        newParams1 += p1;
        newParams2 += p2;
        newMeans += meanArray(i);
      }
    }
    val newParams1Res = newParams1.result();
    val newParams2Res = newParams2.result();
    val newMeansRes = newMeans.result();
    assert(newParams1Res.length == newMeansRes.length);
    assert(newParams2Res.length == newMeansRes.length);
    Impl3DResult(implLabel, newParams1Res, newParams2Res, newMeansRes)
  }
}

case class Impl2DResult(implLabel: String, params: Array[Long], means: Array[Double]) extends dataset.DataSet {
  override def getDimensions(): Int = 2;
  override def getPointValue(point: Int, dimension: Int): String = dimension match {
    case 0 => params(point).toString
    case 1 => means(point).toString
    case _ => ???
  };
  override def size(): Int = means.size;
}

case class Impl3DResult(implLabel: String, params1: Array[Long], params2: Array[Long], means: Array[Double]) extends dataset.DataSet {
  override def getDimensions(): Int = 3;
  override def getPointValue(point: Int, dimension: Int): String = dimension match {
    case 0 => params1(point).toString
    case 1 => params2(point).toString
    case 2 => means(point).toString
    case _ => ???
  };
  override def size(): Int = means.size;
}
