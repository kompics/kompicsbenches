package se.kth.benchmarks.visualisation.generator

import com.typesafe.scalalogging.StrictLogging
import java.io.File
import scala.util.{Failure, Success, Try}
import scalatags.Text.all._
import scala.reflect._
import scala.collection.mutable.TreeSet
import com.github.tototoshi.csv._
import se.kth.benchmarks.runner.{Benchmark, Benchmarks}

object Plotter extends StrictLogging {
  def fromSource(source: File): Try[Plotted] = Try {
    logger.debug(s"Plotting $source");
    val sourceName = source.getName();
    val title = sourceName.substring(0, sourceName.length() - 5);
    val fileName = s"${title.toLowerCase()}.html";
    val data = loadData(title, source);
    val plots = plot(data);
    val content = body(
      h1(data.benchmark.name),
      for (plot <- plots)
        yield div(
          div(id := s"container-${plot.id}"),
          script(s"""
      var series = ${toSeriesArray(plot.seriesData)};
      Plotting.plotSeries('${plot.title}', '${plot.xAxisLabel}', ${plot.xAxisCategories.mkJsString},'${plot.yAxisLabel}', series, document.getElementById('container-${plot.id}'));
      """)
        )
    );
    Plotted(title, fileName, Frame.embed(content))
  };

  private def plot(data: BenchmarkData[String]): List[PlotData] = {
    data.benchmark.symbol match {
      case "PINGPONG" | "NETPINGPONG" => plots.PingPong.plot(data)
      case _                          => List.empty // TODO
    }
  }

  private def toSeriesArray(series: List[Series]): String = {
    series
      .map(toSeriesObject)
      .mkString("[", ",\n", "]")
  }
  private def toSeriesObject(series: Series): String = {
    val sb = new StringBuilder();
    sb += '{';
    series.meta.foreach {
      case (key, value) => sb ++= s"${key}:${value.render},"
    }
    val seriesData = series.data
      .map(v =>
        if (v.isNaN()) {
          "null"
        } else {
          v.toString()
        }
      )
      .mkJsRawString;
    sb ++= s"data: ${seriesData}";
    sb += '}';
    sb.toString
  }

  private def loadData(title: String, source: File): BenchmarkData[String] = {
    import se.kth.benchmarks.scripts._;

    logger.debug(s"Reading from ${source}...");
    val reader = CSVReader.open(source);
    val rawData = reader.allWithHeaders();
    reader.close()
    logger.debug(s"Read all data from ${source} (${rawData.size} lines).");
    // IMPL,PARAMS,MEAN,SSD,SEM,RSE,CI95LO,CI95UP
    val stats = rawData.map(m => (m("IMPL"), m("PARAMS"), Statistics.fromRow(m)));
    val statsGrouped = stats.groupBy(_._1).map {
      case (key, entries) =>
        val params = entries.map(_._2);
        val stats = entries.map(_._3);
        (key -> ImplGroupedResult(implementations(key).label, params, stats))
    };
    val benchO = Benchmarks.benchmarkLookup.get(title);
    benchO match {
      case Some(bench) => BenchmarkData(bench, statsGrouped)
      case None => {
        throw new RuntimeException(s"Could not get benchmark entry for symbol '${title}'!");
      }
    }
  }
}
case class Plot(title: String, relativePath: String)
case class Plotted(title: String, fileName: String, text: String)
case class PlotData(id: String,
                    title: String,
                    xAxisLabel: String,
                    xAxisCategories: Array[String],
                    yAxisLabel: String,
                    seriesData: List[Series])
object Statistics {
  def fromRow(row: Map[String, String]): Statistics = {
    Statistics(
      sampleMean = row("MEAN").toDouble,
      sampleStandardDeviation = row("SSD").toDouble,
      standardErrorOfTheMean = row("SEM").toDouble,
      relativeErrorOfTheMean = row("RSE").toDouble,
      symmetricConfidenceInterval95 = (row("CI95LO").toDouble, row("CI95UP").toDouble)
    )
  }
}
case class Statistics(sampleMean: Double,
                      sampleStandardDeviation: Double,
                      standardErrorOfTheMean: Double,
                      relativeErrorOfTheMean: Double,
                      symmetricConfidenceInterval95: (Double, Double));
case class ImplGroupedResult[Params: ClassTag](implLabel: String, params: List[Params], stats: List[Statistics]) {
  def mapParams[P: ClassTag](f: Params => P): ImplGroupedResult[P] = {
    this.copy(params = this.params.map(f))
  }
  def paramSet[T: Ordering: ClassTag](f: Params => T): TreeSet[T] = {
    val mapped = params.map(f);
    TreeSet(mapped: _*)
  }
  def map2D[T: Ordering: ClassTag](f: Params => T, labels: Array[T]): Series = {
    val series = Array.ofDim[Double](labels.length);
    var localOffset = 0;
    for (i <- 0 until labels.length) {
      val expected = labels(i);
      val localPosition = i - localOffset;
      val actual = f(params(localPosition));
      if (actual == expected) {
        series(i) = stats(localPosition).sampleMean;
      } else {
        series(i) = Double.NaN;
        localOffset += 1;
      }
    }
    val meta = Map(
      "name" -> JsString(implLabel)
    );
    Series(meta, series)
  }
}
case class Series(meta: Map[String, JsValue], data: Array[Double]) {
  def addMeta(values: (String, JsValue)*): Series = {
    this.copy(meta = meta ++ values)
  }
}
case class BenchmarkData[Params](benchmark: Benchmark, results: Map[String, ImplGroupedResult[Params]]) {
  def mapParams[P: ClassTag](f: Params => P): BenchmarkData[P] = {
    val mapped: Map[String, ImplGroupedResult[P]] = this.results.mapValues(p => p.mapParams(f));
    this.copy(results = mapped)
  }
  def paramSeries[T: Ordering: ClassTag](f: Params => T): Array[T] = {
    val sets = results.values.map(res => res.paramSet(f));
    val set = sets.foldLeft(TreeSet.empty[T]) { (acc, params) =>
      acc.union(params)
    };
    set.toArray
  }
}

sealed trait JsValue {
  def render: String;
}
case class JsString(s: String) extends JsValue {
  override def render: String = s"'${s}'";
}
case class JsRaw(s: String) extends JsValue {
  override def render: String = s"${s}";
}
