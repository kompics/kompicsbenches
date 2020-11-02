package se.kth.benchmarks.visualisation.generator.plots

import scala.reflect._
import se.kth.benchmarks.visualisation.generator.{BenchmarkData, DataSeries, ErrorBarSeries, FrameworkPlotStyle, ImplGroupedResult, JsRaw, JsString, PlotData, PlotGroup, Series, Statistics}
import se.kth.benchmarks.runner.{Benchmark, BenchmarkWithSpace, ParameterSpacePB, Parameters}
import kompics.benchmarks.benchmarks._

import scala.collection.immutable
import scala.collection.immutable.TreeMap
import scala.collection.mutable.ListBuffer

abstract class ExperimentPlots[Params <: Parameters.Message[Params]](val bench: BenchmarkWithSpace[Params],
                                                                     val space: ParameterSpacePB[Params],
                                                                     val data: BenchmarkData[Params]) {
  def plotAlong[T: Ordering: ClassTag, G: Ordering, S](mainAxis: Params => T,
                                                       groupings: Params => G,
                                                       plotId: G => String,
                                                       plotParams: G => List[String],
                                                       plotTitle: String,
                                                       xAxisLabel: String,
                                                       xAxisTitle: String,
                                                       xAxisId: String,
                                                       yAxisLabel: String,
                                                       units: String,
                                                       calculateParams: G => S,
                                                       calculateValue: (S, T, Statistics) => Double,
                                                       calculatedTitle: String,
                                                       calculatedYAxisLabel: String,
                                                       calculatedUnits: String): PlotGroup.Along = {
    val sliced = data.slices(groupings, mainAxis);
    val plots: List[PlotGroup] = sliced.toList.map {
      case (params, impls) => {
        val paramSeries = data.paramSeries(mainAxis);
        val groupedSeries = impls.mapValues(_.map2D(identity, paramSeries));
        val groupedErrorSeries = impls.mapValues(_.map2DErrorBars(identity, paramSeries));
        val mergedSeries = (for (key <- groupedSeries.keys) yield {
          (key, groupedSeries(key), groupedErrorSeries(key))
        }).toList;
        val sortedSeries: List[Series] = mergedSeries.sortBy(_._1).map(t => List[Series](t._2, t._3)).flatten;
        val pimpedSeries: List[Series] = sortedSeries.map( series => {
          series.addMeta("tooltip" -> JsRaw(s"{valueDecimals: 2, valueSuffix: '${units}'}"))
          .addMeta(  "color" -> JsRaw(FrameworkPlotStyle.getColor(series.getName)))
            .addMeta(  "marker" -> JsRaw(s"{${FrameworkPlotStyle.getMarker(series.getName)}}"))
            .addMeta("dashStyle" -> JsRaw(FrameworkPlotStyle.getDashStyle(series.getName)))
        }
        );
        val paramsS = paramSeries.map(_.toString);
        val corePlotId = s"${bench.symbol.toLowerCase()}-${plotId(params)}";
        val primaryPlotId = s"${corePlotId}-primary";
        val primaryPlot = PlotData(
          id = primaryPlotId,
          title = s"${bench.name} (${plotTitle})",
          fixedParams = plotParams(params),
          xAxisLabel = xAxisLabel,
          xAxisCategories = paramsS,
          yAxisLabel = yAxisLabel,
          seriesData = pimpedSeries
        );
        val calculatedParams = calculateParams(params);
        val calculatedSeries = impls
          .mapValues(_.map2DWithCalc(identity, paramSeries, (t, stats) => calculateValue(calculatedParams, t, stats)))
          .toList;
        val sortedCalculatedSeries: List[Series] = calculatedSeries.sortBy(_._1).map(t => t._2);
        val pimpedCalculatedSeries: List[Series] = sortedCalculatedSeries.map(series => {
          series.addMeta("tooltip" -> JsRaw(s"{valueDecimals: 2, valueSuffix: '${calculatedUnits}'}"))
          .addMeta(  "color" -> JsRaw(FrameworkPlotStyle.getColor(series.getName)))
            .addMeta(  "marker" -> JsRaw(s"{${FrameworkPlotStyle.getMarker(series.getName)}}"))
            .addMeta("dashStyle" -> JsRaw(FrameworkPlotStyle.getDashStyle(series.getName)))
        }
        );
        val calculatedPlotId = s"${corePlotId}-calculated";
        val calculatedPlot = PlotData(
          id = calculatedPlotId,
          title = s"${bench.name} (${calculatedTitle})",
          fixedParams = plotParams(params),
          xAxisLabel = xAxisLabel,
          xAxisCategories = paramsS,
          yAxisLabel = calculatedYAxisLabel,
          seriesData = pimpedCalculatedSeries
        );
        PlotGroup.CalculatedWithParameters(primaryPlot, calculatedPlot)
      }
    };
    PlotGroup.Along(xAxisId, xAxisTitle, plots)
  }

  def plotAlongNoCalc[T: Ordering: ClassTag, G: Ordering, S](mainAxis: Params => T,
                                                             groupings: Params => G,
                                                             plotId: G => String,
                                                             plotParams: G => List[String],
                                                             plotTitle: String,
                                                             xAxisLabel: String,
                                                             xAxisTitle: String,
                                                             xAxisId: String,
                                                             yAxisLabel: String,
                                                             units: String): PlotGroup.Along = {
    val sliced = data.slices(groupings, mainAxis);
    val plots: List[PlotGroup] = sliced.toList.map {
      case (params, impls) => {
        val paramSeries = data.paramSeries(mainAxis);
        val groupedSeries = impls.mapValues(_.map2D(identity, paramSeries));
        val groupedErrorSeries = impls.mapValues(_.map2DErrorBars(identity, paramSeries));
        val mergedSeries = (for (key <- groupedSeries.keys) yield {
          (key, groupedSeries(key), groupedErrorSeries(key))
        }).toList;
        val sortedSeries: List[Series] = mergedSeries.sortBy(_._1).map(t => List[Series](t._2, t._3)).flatten;
        val pimpedSeries: List[Series] = sortedSeries.map( series => {
          series.addMeta("tooltip" -> JsRaw(s"{valueDecimals: 2, valueSuffix: '${units}'}"))
            .addMeta(  "color" -> JsRaw(FrameworkPlotStyle.getColor(series.getName)))
            .addMeta(  "marker" -> JsRaw(s"{${FrameworkPlotStyle.getMarker(series.getName)}}"))
            .addMeta("dashStyle" -> JsRaw(FrameworkPlotStyle.getDashStyle(series.getName)))
        }
        );
        val paramsS = paramSeries.map(_.toString);
        val corePlotId = s"${bench.symbol.toLowerCase()}-${plotId(params)}";
        val primaryPlotId = s"${corePlotId}-primary";
        val primaryPlot = PlotData(
          id = primaryPlotId,
          title = s"${bench.name} (${plotTitle})",
          fixedParams = plotParams(params),
          xAxisLabel = xAxisLabel,
          xAxisCategories = paramsS,
          yAxisLabel = yAxisLabel,
          seriesData = pimpedSeries
        );
        PlotGroup.WithParameters(primaryPlot)
      }
    };
    PlotGroup.Along(xAxisId, xAxisTitle, plots)

  }

  def plotAlongPreSliced[T: Ordering: ClassTag, K: Ordering, G: Ordering, S](mainAxis: Params => T,
                                                       groupings: Params => G,
                                                       plotId: G => String,
                                                       plotParams: G => List[String],
                                                       plotTitle: String,
                                                       xAxisLabel: String,
                                                       xAxisTitle: String,
                                                       xAxisId: String,
                                                       yAxisLabel: String,
                                                       units: String,
                                                       calculateParams: G => S,
                                                       calculateValue: (S, T, Statistics) => Double,
                                                       calculatedTitle: String,
                                                       calculatedYAxisLabel: String,
                                                       calculatedUnits: String,
                                                       seriesParams: Params => K,
                                                       sliced: scala.collection.SortedMap[G, immutable.SortedMap[K, ImplGroupedResult[T]]]
                                                       ): PlotGroup.Along = {
    val plots: List[PlotGroup] = sliced.toList.map {
      case (params, impls) => {
        val paramSeries = data.paramSeries(mainAxis).filter(x => impls.values.exists(i => i.params.contains(x)))  // keep only x param if a datapoint uses it
        val groupedSeries = impls.mapValues(_.map2D(identity, paramSeries));
//        val groupedErrorSeries = impls.mapValues(_.mapCI95Bars(identity, paramSeries));
        val groupedErrorSeries = impls.mapValues(_.map2DErrorBars(identity, paramSeries));
        var merged = new ListBuffer[(K, DataSeries, ErrorBarSeries)]();
        for (key <- groupedSeries.keys) {
          val m = (key, groupedSeries(key), groupedErrorSeries(key));
          merged += m;
        }
        val mergedSeries = merged.toList;
        val sortedSeries: List[Series] = mergedSeries.map(t => List[Series](t._2, t._3)).flatten; // already sorted preslice
        val pimpedSeries: List[Series] = sortedSeries.map( series => {
          series.addMeta("tooltip" -> JsRaw(s"{valueDecimals: 2, valueSuffix: '${units}'}"))
            .addMeta(  "color" -> JsRaw(FrameworkPlotStyle.getColor(series.getName)))
            .addMeta(  "marker" -> JsRaw(s"{${FrameworkPlotStyle.getMarker(series.getName)}}"))
            .addMeta("dashStyle" -> JsRaw(FrameworkPlotStyle.getDashStyle(series.getName)))
        }
        );
        val paramsS = paramSeries.map(_.toString);
        val corePlotId = s"${bench.symbol.toLowerCase()}-${plotId(params)}";
        val primaryPlotId = s"${corePlotId}-primary";
        val primaryPlot = PlotData(
          id = primaryPlotId,
          title = s"${bench.name} (${plotTitle})",
          fixedParams = plotParams(params),
          xAxisLabel = xAxisLabel,
          xAxisCategories = paramsS,
          yAxisLabel = yAxisLabel,
          seriesData = pimpedSeries
        );
        val calculatedParams = calculateParams(params);
        val calculatedSeries = impls
          .mapValues(_.map2DWithCalc(identity, paramSeries, (t, stats) => calculateValue(calculatedParams, t, stats)))
          .toList;
        val sortedCalculatedSeries: List[Series] = calculatedSeries.map(t => t._2);
        val pimpedCalculatedSeries: List[Series] = sortedCalculatedSeries.map(series => {
          series.addMeta("tooltip" -> JsRaw(s"{valueDecimals: 2, valueSuffix: '${calculatedUnits}'}"))
            .addMeta(  "color" -> JsRaw(FrameworkPlotStyle.getColor(series.getName)))
            .addMeta(  "marker" -> JsRaw(s"{${FrameworkPlotStyle.getMarker(series.getName)}}"))
            .addMeta("dashStyle" -> JsRaw(FrameworkPlotStyle.getDashStyle(series.getName)))
        }
        );
        val calculatedPlotId = s"${corePlotId}-calculated";
        val calculatedPlot = PlotData(
          id = calculatedPlotId,
          title = s"${bench.name} (${calculatedTitle})",
          fixedParams = plotParams(params),
          xAxisLabel = xAxisLabel,
          xAxisCategories = paramsS,
          yAxisLabel = calculatedYAxisLabel,
          seriesData = pimpedCalculatedSeries
        );
        PlotGroup.CalculatedWithParameters(primaryPlot, calculatedPlot)
      }
    };
    PlotGroup.Along(xAxisId, xAxisTitle, plots)
  }
}
