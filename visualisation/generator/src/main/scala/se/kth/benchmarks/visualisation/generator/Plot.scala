package se.kth.benchmarks.visualisation.generator

import com.typesafe.scalalogging.StrictLogging
import java.io.File
import scala.util.{Failure, Success, Try}
import scalatags.Text.all._
import scala.reflect._
import scala.collection.mutable
import scala.collection.immutable
import com.github.tototoshi.csv._
import se.kth.benchmarks.runner.{Benchmark, Benchmarks}
import scalatags.Text

object Plotter extends StrictLogging {
  def fromSource(source: File): Try[Plotted] = Try {
    logger.debug(s"Plotting $source");
    val sourceName = source.getName();
    val title = sourceName.substring(0, sourceName.length() - 5);
    val fileName = s"${title.toLowerCase()}.html";
    val data = loadData(title, source);
    val plots = plot(data);
    val content = div(
      BootstrapStyle.containerFluid,
      h2(data.benchmark.name),
      plots.render
    );
    Plotted(data.benchmark.name, fileName, Frame.embed(content, data.benchmark.name))
  };

  private def plot(data: BenchmarkData[String]): PlotGroup = {
    data.benchmark.symbol match {
      case "PINGPONG" | "NETPINGPONG"     => plots.PingPong.plot(data)
      case "TPPINGPONG" | "NETTPPINGPONG" => plots.ThroughputPingPong.plot(data)
      case "FIBONACCI"                    => plots.Fibonacci.plot(data)
      case "CHAMENEOS"                    => plots.Chameneos.plot(data)
      case "APSP"                         => plots.AllPairsShortestPath.plot(data)
      case "ATOMICREGISTER"               => plots.AtomicRegister.plot(data)
      case "STREAMINGWINDOWS"             => plots.StreamingWindows.plot(data)
      case "ATOMICBROADCAST"              => plots.AtomicBroadcast.plot(data)
      case "SIZEDTP"                      => plots.SizedThroughput.plot(data)
      case _                              => PlotGroup.Empty // TODO
    }
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
                    fixedParams: List[String] = Nil,
                    xAxisLabel: String,
                    xAxisCategories: Array[String],
                    yAxisLabel: String,
                    seriesData: List[Series])
sealed trait PlotGroup {
  def render: Tag;
}
object PlotGroup {
  case object Empty extends PlotGroup {
    override def render: Tag =
      div(BootstrapStyle.alert, BootstrapStyle.alertInfo, role := "alert", "No plots to show.");
  }

  case class Single(plot: PlotData) extends PlotGroup {
    override def render: Tag =
      div(
        div(id := s"container-${plot.id}"),
        Frame.backToTopButton,
        script(s"""
  var series = ${toSeriesArray(plot.seriesData)};
  Plotting.plotSeries('${plot.title}', '${plot.xAxisLabel}', ${plot.xAxisCategories.mkJsString},'${plot.yAxisLabel}', series, document.getElementById('container-${plot.id}'));
  """)
      );
  }
  case class Axes(groups: List[Along]) extends PlotGroup {
    override def render: Tag = div(
      div(
        BootstrapStyle.navbar,
        BootstrapStyle.navbarLight,
        StandardStyle.navBox,
        BootstrapStyle.flexColumn,
        div(
          StandardStyle.navBoxInner,
          h4("Plot Axes"),
          ul(
            BootstrapStyle.nav,
            BootstrapStyle.flexColumn,
            for (group <- groups)
              yield li(BootstrapStyle.navItem,
                       a(StandardStyle.navLink, href := s"#${group.mainAxisId}", group.mainAxisLabel))
          )
        )
      ),
      for (group <- groups)
        yield group.render
    );
  }

  case class Along(mainAxisId: String, mainAxisLabel: String, groups: List[PlotGroup]) extends PlotGroup {
    override def render: Tag = div(
      h3(id := mainAxisId, s"By $mainAxisLabel"),
      div(
        BootstrapStyle.navbar,
        BootstrapStyle.navbarLight,
        StandardStyle.navBox,
        BootstrapStyle.flexColumn,
        div(
          StandardStyle.navBoxInner,
          h4("Plots"),
          ul(
            BootstrapStyle.nav,
            BootstrapStyle.flexColumn,
            for ((group, index) <- groups.zipWithIndex) yield {
              group match {
                case WithParameters(plot) => {
                  li(
                    BootstrapStyle.navItem,
                    a(StandardStyle.navLink, href := s"#${mainAxisId}-plot-$index", s"Plot #${index}"),
                    span(plot.fixedParams.mkString(", "))
                  )
                }
                case CalculatedWithParameters(plot, _) => {
                  li(
                    BootstrapStyle.navItem,
                    a(StandardStyle.navLink, href := s"#${mainAxisId}-plot-$index", s"Plot #${index}"),
                    span(plot.fixedParams.mkString(", "))
                  )
                }
                case _ => {
                  li(BootstrapStyle.navItem,
                     a(StandardStyle.navLink, href := s"#${mainAxisId}-plot-$index", s"Plot #${index}"))
                }
              }
            }
          )
        )
      ),
      for ((group, index) <- groups.zipWithIndex)
        yield div( // force break
                  h4(id := s"${mainAxisId}-plot-$index", s"Plot #${index} by $mainAxisLabel"), // force break
                  group.render)
    );
  }

  case class WithParameters(plot: PlotData) extends PlotGroup {
    override def render: Tag =
      div(
        div(plot.fixedParams.mkString(", ")),
        div(id := s"container-${plot.id}"),
        Frame.backToTopButton,
        script(s"""
  var series = ${toSeriesArray(plot.seriesData)};
  Plotting.plotSeries('${plot.title}', '${plot.xAxisLabel}', ${plot.xAxisCategories.mkJsString},'${plot.yAxisLabel}', series, document.getElementById('container-${plot.id}'));
  """)
      );
  }

  case class CalculatedWithParameters(primary: PlotData, secondary: PlotData) extends PlotGroup {

    val primaryContainerId = s"container-${primary.id}";
    val secondaryContainerId = s"container-${secondary.id}";

    override def render: Tag =
      div(
        div(primary.fixedParams.mkString(", ")),
        div(id := secondaryContainerId),
        div(id := primaryContainerId),
        Frame.backToTopButton,
        script(s"""
  var primarySeries = ${toSeriesArray(primary.seriesData)};
  var secondarySeries = ${toSeriesArray(secondary.seriesData)};
  Plotting.plotSeries('${primary.title}', '${primary.xAxisLabel}', ${primary.xAxisCategories.mkJsString},'${primary.yAxisLabel}', primarySeries, document.getElementById('${primaryContainerId}'));
  Plotting.plotSeries('${secondary.title}', '${secondary.xAxisLabel}', ${secondary.xAxisCategories.mkJsString},'${secondary.yAxisLabel}', secondarySeries, document.getElementById('${secondaryContainerId}'));
  """)
      );
  }

  private def toSeriesArray(series: List[Series]): String = {
    series
      .map(_.render)
      .mkString("[", ",\n", "]")
  }
}

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
  assert(params.length == stats.length, "Parameters must match values!");

  def mapParams[P: ClassTag](f: Params => P): ImplGroupedResult[P] = {
    this.copy(params = this.params.map(f))
  }
  def sortParams(implicit ev: Ordering[Params]): ImplGroupedResult[Params] = {
    val sorted = this.params.zip(this.stats).sortBy(_._1);
    val (newParams, newStats) = sorted.unzip;
    this.copy(params = newParams, stats = newStats)
  }
  def sortParamsBy[T: Ordering](extract: Params => T): ImplGroupedResult[Params] = {
    val sorted = this.params.zip(this.stats).sortBy(t => extract(t._1));
    val (newParams, newStats) = sorted.unzip;
    this.copy(params = newParams, stats = newStats)
  }
  def paramSet[T: Ordering: ClassTag](f: Params => T): mutable.TreeSet[T] = {
    val mapped = params.map(f);
    mutable.TreeSet(mapped: _*)
  }
  def map2D[T: Ordering: ClassTag](f: Params => T, labels: Array[T]): DataSeries = {
    assert(labels.length >= params.length, "There shouldn't be params for no labels.");
    assert(labels.length >= stats.length, "There shouldn't be values for no labels.");
    val sorted = this.mapParams(f).sortParams;
    val series = Array.ofDim[Double](labels.length);
    var localOffset = 0;
    for (i <- 0 until labels.length) {
      val expected = labels(i);
      val localPosition = i - localOffset;
      if (params.length > localPosition) {
        val actual = sorted.params(localPosition);
        if (actual == expected) {
          series(i) = sorted.stats(localPosition).sampleMean;
        } else {
          series(i) = Double.NaN;
          localOffset += 1;
        }
      } else {
        series(i) = Double.NaN;
      }
    }
    val meta = Map(
      "name" -> JsString(implLabel)
    );
    DataSeries(meta, series)
  }
  def map2DWithCalc[T: Ordering: ClassTag, C](f: Params => T,
                                              labels: Array[T],
                                              calc: (Params, Statistics) => Double): DataSeries = {
    assert(labels.length >= params.length, "There shouldn't be params for no labels.");
    assert(labels.length >= stats.length, "There shouldn't be values for no labels.");
    val sorted = this.mapParams(p => (f(p), p)).sortParamsBy(_._1);
    val series = Array.ofDim[Double](labels.length);
    var localOffset = 0;
    for (i <- 0 until labels.length) {
      val expected = labels(i);
      val localPosition = i - localOffset;
      if (params.length > localPosition) {
        val (actual, actualParams) = sorted.params(localPosition);
        if (actual == expected) {
          series(i) = calc(actualParams, sorted.stats(localPosition));
        } else {
          series(i) = Double.NaN;
          localOffset += 1;
        }
      } else {
        series(i) = Double.NaN;
      }
    }
    val meta = Map(
      "name" -> JsString(implLabel)
    );
    DataSeries(meta, series)
  }
  def map2DErrorBars[T: Ordering: ClassTag](f: Params => T, labels: Array[T]): ErrorBarSeries = {
    val series = Array.ofDim[(Double, Double)](labels.length);
    var localOffset = 0;
    for (i <- 0 until labels.length) {
      val expected = labels(i);
      val localPosition = i - localOffset;
      if (params.length > localPosition) {
        val actual = f(params(localPosition));
        if (actual == expected) {
          val stats = this.stats(localPosition);
          val mean = stats.sampleMean;
          series(i) = (mean - stats.standardErrorOfTheMean, mean + stats.standardErrorOfTheMean);
        } else {
          series(i) = (Double.NaN, Double.NaN);
          localOffset += 1;
        }
      } else {
        series(i) = (Double.NaN, Double.NaN);
      }
    }
    val meta = Map(
      "name" -> JsString(s"$implLabel error"),
      "type" -> JsString("errorbar")
    );
    ErrorBarSeries(meta, series)
  }
  def mapCI95Bars[T: Ordering: ClassTag](f: Params => T, labels: Array[T]): ErrorBarSeries = {
    val series = Array.ofDim[(Double, Double)](labels.length);
    var localOffset = 0;
    for (i <- 0 until labels.length) {
      val expected = labels(i);
      val localPosition = i - localOffset;
      if (params.length > localPosition) {
        val actual = f(params(localPosition));
        if (actual == expected) {
          val stats = this.stats(localPosition);
          series(i) = stats.symmetricConfidenceInterval95;
        } else {
          series(i) = (Double.NaN, Double.NaN);
          localOffset += 1;
        }
      } else {
        series(i) = (Double.NaN, Double.NaN);
      }
    }
    val meta = Map(
      "name" -> JsString(s"$implLabel ci95"),
      "type" -> JsString("errorbar")
    );
    ErrorBarSeries(meta, series)
  }

  def groupBy[T: Ordering](grouper: Params => T): immutable.SortedMap[T, ImplGroupedResult[Params]] = {
    val indexedParams = params.zipWithIndex;
    var builder = mutable.TreeMap.empty[T, (mutable.ListBuffer[Params], mutable.ListBuffer[Statistics])];
    val statsArray = stats.toArray;
    indexedParams.foreach {
      case (p, i) =>
        val key = grouper(p);
        val entry = builder.getOrElseUpdate(key, (mutable.ListBuffer.empty, mutable.ListBuffer.empty));
        val stats = statsArray(i);
        entry._1 += p;
        entry._2 += stats;
    }
    val immutableBuilder = immutable.TreeMap.newBuilder[T, ImplGroupedResult[Params]];
    for ((k, t) <- builder) {
      immutableBuilder += (k -> ImplGroupedResult(implLabel, t._1.toList, t._2.toList));
    }
    immutableBuilder.result()
  }

  def slices[T: Ordering, P: ClassTag](grouper: Params => T,
                                       mapper: Params => P): immutable.SortedMap[T, ImplGroupedResult[P]] = {
    this.groupBy(grouper).mapValues(_.mapParams(mapper))
  }
}
sealed trait Series {
  def addMeta(values: (String, JsValue)*): Series;
  def render: String;
  def getName: String;
}
case class DataSeries(meta: Map[String, JsValue], data: Array[Double]) extends Series {
  override def addMeta(values: (String, JsValue)*): Series = {
    this.copy(meta = meta ++ values)
  }
  override def render: String = {
    val sb = new StringBuilder();
    sb += '{';
    meta.foreach {
      case (key, value) => sb ++= s"${key}:${value.render},"
    }
    val seriesData = data
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

  override def getName: String = {
    meta.get("name").get.render
  }
}
case class ErrorBarSeries(meta: Map[String, JsValue], data: Array[(Double, Double)]) extends Series {
  override def addMeta(values: (String, JsValue)*): Series = {
    this.copy(meta = meta ++ values)
  }
  override def render: String = {
    val sb = new StringBuilder();
    sb += '{';
    meta.foreach {
      case (key, value) => sb ++= s"${key}:${value.render},"
    }
    val seriesData = data.map {
      case (l, h) if l.isNaN() || h.isNaN() => "null"
      case (l, h)                           => s"[${l}, ${h}]"
    }.mkJsRawString;
    sb ++= s"data: ${seriesData}";
    sb += '}';
    sb.toString
  }

  override def getName: String = {
    meta.get("name").get.render
  }}
case class BenchmarkData[Params](benchmark: Benchmark, results: Map[String, ImplGroupedResult[Params]]) {
  def mapParams[P: ClassTag](f: Params => P): BenchmarkData[P] = {
    val mapped: Map[String, ImplGroupedResult[P]] = this.results.mapValues(p => p.mapParams(f));
    this.copy(results = mapped)
  }
  def paramSeries[T: Ordering: ClassTag](f: Params => T): Array[T] = {
    val sets = results.values.map(res => res.paramSet(f));
    val set = sets.foldLeft(mutable.TreeSet.empty[T]) { (acc, params) =>
      acc.union(params)
    };
    set.toArray
  }

  def slices[T: Ordering, P: ClassTag](
      grouper: Params => T,
      mapper: Params => P
  ): immutable.SortedMap[T, immutable.SortedMap[String, ImplGroupedResult[P]]] = {
    val builder = mutable.TreeMap.empty[T, mutable.TreeMap[String, ImplGroupedResult[P]]];
    val sliced = results.mapValues(_.slices(grouper, mapper));
    sliced.foreach {
      case (impl, slices) =>
        slices.foreach {
          case (key, slice) =>
            val entry = builder.getOrElseUpdate(key, mutable.TreeMap.empty);
            entry += (impl -> slice);
        }
    }
    val immutableBuilder = immutable.TreeMap.newBuilder[T, immutable.SortedMap[String, ImplGroupedResult[P]]];
    for ((k, t) <- builder) {
      val innerBuilder = immutable.TreeMap.newBuilder[String, ImplGroupedResult[P]];
      for ((kinner, tinner) <- t) {
        innerBuilder += (kinner -> tinner);
      }
      immutableBuilder += (k -> innerBuilder.result());
    }
    immutableBuilder.result()
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
