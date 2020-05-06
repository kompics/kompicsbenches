package se.kth.benchmarks.visualisation.generator.plots

import scala.reflect._
import se.kth.benchmarks.visualisation.generator.{BenchmarkData, ImplGroupedResult, JsRaw, JsString, PlotData, PlotGroup, Series, Statistics}
import se.kth.benchmarks.runner.{Benchmark, BenchmarkWithSpace, ParameterSpacePB, Parameters}
import kompics.benchmarks.benchmarks._

import scala.collection.immutable

abstract class ExperimentPlots[Params <: Parameters.Message[Params]](val bench: BenchmarkWithSpace[Params],
                                                                     val space: ParameterSpacePB[Params],
                                                                     val data: BenchmarkData[Params]) {
  def plotAlong[T: Ordering: ClassTag, G: Ordering, S](mainAxis: Params => T, // Ex: num_keys
                                                       groupings: Params => G,  // ex read-write-partition_size
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
    println(s"\nsliced: $sliced");
    val plots: List[PlotGroup] = sliced.toList.map {
      case (params, impls) => { // Ex: (r-50-w-50, partiion_size = 3, kompact) TODO REMOVE
        println("\nparams: " + params + ", impls: " + impls);
        val paramSeries = data.paramSeries(mainAxis);
        println("\nparamSeries: " + paramSeries.mkString(", "));
        val groupedSeries = impls.mapValues(_.map2D(identity, paramSeries));
        val groupedErrorSeries = impls.mapValues(_.map2DErrorBars(identity, paramSeries));
        groupedSeries.foreach {
          case (key, value) => {
            println(s"\nGROUPED_SERIES key: $key, value-data: ${value.data.mkString(", ")}")
          }
        }
//        println("\ngroupedseries: " + groupedSeries);
        val mergedSeries = (for (key <- groupedSeries.keys) yield {
          (key, groupedSeries(key), groupedErrorSeries(key))
        }).toList;
        println("\nmerged series: " + mergedSeries);
        val sortedSeries: List[Series] = mergedSeries.sortBy(_._1).map(t => List[Series](t._2, t._3)).flatten;
        println("\nsorted series: " + sortedSeries);

        val pimpedSeries: List[Series] = sortedSeries.map(
          _.addMeta(
            "tooltip" -> JsRaw(s"{valueDecimals: 2, valueSuffix: '${units}'}")
          )
        );
        val paramsS = paramSeries.map(_.toString);
        paramsS.foreach(x => println("paramsS: " + x));
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
        val pimpedCalculatedSeries: List[Series] = sortedCalculatedSeries.map(
          _.addMeta(
            "tooltip" -> JsRaw(s"{valueDecimals: 2, valueSuffix: '${calculatedUnits}'}")
          )
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
        val pimpedSeries: List[Series] = sortedSeries.map(
          _.addMeta(
            "tooltip" -> JsRaw(s"{valueDecimals: 2, valueSuffix: '${units}'}")
          )
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

  def plotAlongPreSliced[T: Ordering: ClassTag, G: Ordering, S](mainAxis: Params => T, // Ex: num_keys
                                                       groupings: Params => G,  // ex read-write-partition_size
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
                                                       sliced: scala.collection.SortedMap[G, immutable.SortedMap[String, ImplGroupedResult[T]]]
                                                       ): PlotGroup.Along = {
//    val sliced = data.slices(groupings, mainAxis);
    val plots: List[PlotGroup] = sliced.toList.map {
      case (params, impls) => { // Ex: (r-50-w-50, partiion_size = 3, kompact) TODO REMOVE
        println("\nparams: " + params + ", impls: " + impls);
        val paramSeries = data.paramSeries(mainAxis);
        println("\nparamSeries: " + paramSeries.mkString(", "));
        val groupedSeries = impls.mapValues(_.map2D(identity, paramSeries));
        val groupedErrorSeries = impls.mapValues(_.map2DErrorBars(identity, paramSeries));
        groupedSeries.foreach {
          case (key, value) => {
            println(s"\nGROUPED_SERIES key: $key, value-data: ${value.data.mkString(", ")}")
          }
        }
        //        println("\ngroupedseries: " + groupedSeries);
        val mergedSeries = (for (key <- groupedSeries.keys) yield {
          (key, groupedSeries(key), groupedErrorSeries(key))
        }).toList;
        println("\nmerged series: " + mergedSeries);
        val sortedSeries: List[Series] = mergedSeries.sortBy(_._1).map(t => List[Series](t._2, t._3)).flatten;
        println("\nsorted series: " + sortedSeries);

        val pimpedSeries: List[Series] = sortedSeries.map(
          _.addMeta(
            "tooltip" -> JsRaw(s"{valueDecimals: 2, valueSuffix: '${units}'}")
          )
        );
        val paramsS = paramSeries.map(_.toString);
        paramsS.foreach(x => println("paramsS: " + x));
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
        val pimpedCalculatedSeries: List[Series] = sortedCalculatedSeries.map(
          _.addMeta(
            "tooltip" -> JsRaw(s"{valueDecimals: 2, valueSuffix: '${calculatedUnits}'}")
          )
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
